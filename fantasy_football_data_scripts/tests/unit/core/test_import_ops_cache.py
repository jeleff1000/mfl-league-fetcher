"""Imports must hydrate missing weekly rank inputs before enrichment/upload."""

import duckdb
import polars as pl
import pytest

from multi_league.core import date_utils, db_reader, import_utils, ops_cache
from multi_league.transformations.player.modules.nfl_rankings import calculate_nfl_rankings


@pytest.fixture
def ops_sources(tmp_path, monkeypatch):
    cache_path = tmp_path / "ops_cache.duckdb"
    schema = """
        CREATE SCHEMA nfl_historical;
        CREATE TABLE nfl_historical.nfl_player_stats_all (
            NFL_player_id VARCHAR, year INTEGER, week INTEGER, season_type VARCHAR,
            nfl_team VARCHAR, opponent_nfl_team VARCHAR, player_week VARCHAR,
            position VARCHAR, rank_qb_4pt INTEGER, rank_alltime_qb_4pt INTEGER,
            recon_correction_log VARCHAR
        )
    """
    with duckdb.connect(str(cache_path)) as cache:
        cache.execute(schema)
        cache.execute("""
            INSERT INTO nfl_historical.nfl_player_stats_all VALUES
            ('caleb',2025,1,'REG','CHI','GB','caleb_2025_1','QB',7,222,'historical')
        """)
    live = duckdb.connect(":memory:")
    live.execute(schema)
    live.execute("""
        INSERT INTO nfl_historical.nfl_player_stats_all VALUES
        ('caleb',2026,1,'REG','CHI','CAR','caleb_2026_1','QB',1,216,'large-unused-blob'),
        ('caleb',2026,2,'REG','CHI','GB','caleb_2026_2','QB',5,216,'large-unused-blob')
    """)

    class Reader:
        fail = False
        queries = []

        def query_df(self, sql, *, database):
            assert database == "___ops"
            assert "SELECT *" not in sql.upper()
            self.queries.append(sql)
            if self.fail:
                raise RuntimeError("Fly unavailable")
            return live.execute(sql).fetchdf()

        query_df_parquet = query_df

        def query(self, sql, *, database):
            return self.query_df(sql, database=database).to_dict("records")

    reader = Reader()
    monkeypatch.setenv("OPS_CACHE_PATH", str(cache_path))
    monkeypatch.setenv("DATABASE_BACKEND", "fly")
    monkeypatch.setattr(db_reader, "get_reader", lambda: reader)
    monkeypatch.setattr(date_utils, "get_current_nfl_season_year", lambda **_kwargs: 2026)
    monkeypatch.setattr(date_utils, "get_nfl_state", lambda: {
        "season": "2026", "league_season": "2026", "season_type": "regular", "week": 2,
    })
    def forbid_copy(*_args, **_kwargs):
        raise AssertionError("cache hydration must not copy the full database")
    monkeypatch.setattr(ops_cache.shutil, "copy2", forbid_copy)
    yield cache_path, live, reader
    live.close()


def test_stale_cache_import_hydrates_live_ranks_without_replacing_history(ops_sources):
    cache_path, _live, reader = ops_sources
    assert import_utils.run_track_1_verify(2025, 2026) is True
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        current = cache.execute("""
            SELECT position, rank_qb_4pt, rank_alltime_qb_4pt
            FROM nfl_historical.nfl_player_stats_all WHERE year=2026 ORDER BY week
        """).fetchdf()
        assert cache.execute("""
            SELECT rank_alltime_qb_4pt, recon_correction_log
            FROM nfl_historical.nfl_player_stats_all WHERE year=2025
        """).fetchone() == (222, "historical")
    assert len(current) == 2
    ranked = calculate_nfl_rankings(pl.from_pandas(current))
    assert ranked["position_alltime_rank"].to_list() == [216, 216]
    downloads = [sql for sql in reader.queries if "IN (VALUES" in sql]
    assert len(downloads) == 2
    assert all("year = 2026" in sql for sql in downloads)
    assert all('NULL AS "recon_correction_log"' in sql for sql in downloads)


def test_existing_latest_week_receives_corrected_ranks_with_identical_keys(ops_sources):
    cache_path, live, reader = ops_sources
    live.execute("DELETE FROM nfl_historical.nfl_player_stats_all WHERE week=2")
    with duckdb.connect(str(cache_path)) as cache:
        cache.execute("""
            INSERT INTO nfl_historical.nfl_player_stats_all VALUES
            ('caleb',2026,1,'REG','CHI','CAR','caleb_2026_1','QB',1,1,'stale')
        """)

    assert import_utils.run_track_1_verify(2025, 2026) is True

    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("""
            SELECT year, rank_alltime_qb_4pt
            FROM nfl_historical.nfl_player_stats_all ORDER BY year
        """).fetchall() == [(2025, 222), (2026, 216)]
    downloads = [sql for sql in reader.queries if "IN (VALUES" in sql]
    assert len(downloads) == 1
    assert "year = 2026 AND week = 1" in downloads[0]
    assert all("hash(" not in sql.lower() for sql in reader.queries)


def test_existing_cache_refreshes_only_latest_played_week(ops_sources):
    cache_path, live, reader = ops_sources
    current = live.execute("""
        SELECT NFL_player_id, year, week, season_type, nfl_team, opponent_nfl_team,
               player_week, position, rank_qb_4pt, rank_alltime_qb_4pt, recon_correction_log
        FROM nfl_historical.nfl_player_stats_all
    """).fetchdf()
    current["rank_alltime_qb_4pt"] = 1
    with duckdb.connect(str(cache_path)) as cache:
        cache.register("current_rows", current)
        cache.execute("INSERT INTO nfl_historical.nfl_player_stats_all SELECT * FROM current_rows")

    assert import_utils.run_track_1_verify(2025, 2026) is True

    downloads = [sql for sql in reader.queries if "IN (VALUES" in sql]
    assert len(downloads) == 1
    assert "year = 2026 AND week = 2" in downloads[0]
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("""
            SELECT week, rank_alltime_qb_4pt FROM nfl_historical.nfl_player_stats_all
            WHERE year=2026 ORDER BY week
        """).fetchall() == [(1, 1), (2, 216)]


@pytest.mark.parametrize("unavailable", ["offline", "empty"])
def test_missing_active_ops_aborts_instead_of_returning_ignored_false(ops_sources, unavailable):
    cache_path, live, reader = ops_sources
    if unavailable == "offline":
        reader.fail = True
    else:
        live.execute("DELETE FROM nfl_historical.nfl_player_stats_all")
    with pytest.raises(RuntimeError, match="2026"):
        import_utils.run_track_1_verify(2025, 2026)
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all").fetchone()[0] == 1


def test_import_removes_corrected_deleted_and_wholly_removed_week_keys(ops_sources):
    cache_path, _live, reader = ops_sources
    with duckdb.connect(str(cache_path)) as cache:
        cache.execute("""
            INSERT INTO nfl_historical.nfl_player_stats_all VALUES
            ('caleb',2026,1,'REG','CHI','GB','caleb_2026_1','QB',7,999,'old opponent'),
            ('removed',2026,1,'REG','CHI','CAR','removed_2026_1','QB',8,999,'deleted'),
            ('removed',2026,3,'REG','CHI','GB','removed_2026_3','QB',8,999,'removed week')
        """)

    assert import_utils.run_track_1_verify(2025, 2026) is True
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("""
            SELECT NFL_player_id, year, week, opponent_nfl_team
            FROM nfl_historical.nfl_player_stats_all ORDER BY year, week, NFL_player_id
        """).fetchall() == [
            ('caleb', 2025, 1, 'GB'), ('caleb', 2026, 1, 'CAR'), ('caleb', 2026, 2, 'GB'),
        ]
    assert len([sql for sql in reader.queries if "IN (VALUES" in sql]) == 2


@pytest.mark.parametrize("state", [
    {"season": "2026", "league_season": "2026", "season_type": "pre"},
    {"season": "2025", "league_season": "2026", "season_type": "off"},
    {"season": "2026", "league_season": "2026", "season_type": "off"},
])
def test_verified_preseason_allows_an_empty_single_year_shell(ops_sources, monkeypatch, state):
    cache_path, live, reader = ops_sources
    live.execute("DELETE FROM nfl_historical.nfl_player_stats_all")
    monkeypatch.setattr(date_utils, "get_nfl_state", lambda: state)

    assert import_utils.run_track_1_verify(2026, 2026) is True
    assert not any("IN (VALUES" in sql for sql in reader.queries)
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("SELECT year FROM nfl_historical.nfl_player_stats_all").fetchall() == [(2025,)]


@pytest.mark.parametrize("state", [
    None, {"season": "2027", "season_type": "pre"},
    {"league_season": "2026", "season_type": "off"},
])
def test_empty_source_without_matching_preseason_evidence_stays_closed(ops_sources, monkeypatch, state):
    _cache_path, live, _reader = ops_sources
    live.execute("DELETE FROM nfl_historical.nfl_player_stats_all")
    monkeypatch.setattr(date_utils, "get_nfl_state", lambda: state)
    with pytest.raises(RuntimeError, match="2026"):
        import_utils.run_track_1_verify(2026, 2026)


@pytest.mark.parametrize("rank_column", ["rank_qb_4pt", "rank_alltime_qb_4pt"])
def test_weekly_rank_only_revision_refreshes_cache_then_becomes_noop(ops_sources, rank_column):
    from scripts.refresh_yahoo_active_season import _finalized_ops

    cache_path, live, reader = ops_sources
    # Include the old hash inputs too: the old implementation must fail on
    # unchanged revisions, not because the test schema lacks scoring atoms.
    raw_columns = (
        'passing_yards passing_tds passing_interceptions rushing_yards rushing_tds '
        'receptions receiving_yards receiving_tds fantasy_points_ppr def_sacks '
        'def_interceptions def_tackles_solo def_tackles_with_assist fg_made pat_made points_allowed'
    ).split()
    with duckdb.connect(str(cache_path)) as cache:
        for column in raw_columns:
            live.execute(f'ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN "{column}" DOUBLE DEFAULT 0')
            cache.execute(f'ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN "{column}" DOUBLE DEFAULT 0')
        frame = live.execute("SELECT * FROM nfl_historical.nfl_player_stats_all WHERE week=1").fetchdf()
        cache.register('seed', frame)
        cache.execute('INSERT INTO nfl_historical.nfl_player_stats_all SELECT * FROM seed')
    original = _finalized_ops(reader, year=2026, through_week=1)
    live.execute(f'UPDATE nfl_historical.nfl_player_stats_all SET "{rank_column}"=3 WHERE week=1')
    corrected = _finalized_ops(reader, year=2026, through_week=1)
    assert original.source_revision.tolist() != corrected.source_revision.tolist()
    assert corrected.week.tolist() == [1]
    reader.queries.clear()

    for _ in range(2):
        assert ops_cache._patch_research_ops_cache_from_fly(
            reader, corrected, base=cache_path, year=2026, weeks=[1],
            work_dir=cache_path.parent, in_place=True,
        ) == cache_path
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute(
            f'SELECT "{rank_column}" FROM nfl_historical.nfl_player_stats_all WHERE year=2026'
        ).fetchone() == (3,)
    assert len([sql for sql in reader.queries if "IN (VALUES" in sql]) == 1


def test_weekly_rank_delta_preserves_an_unadmitted_game(ops_sources):
    from scripts.refresh_yahoo_active_season import _finalized_ops

    cache_path, _live, reader = ops_sources
    with duckdb.connect(str(cache_path)) as cache:
        cache.execute("""
            INSERT INTO nfl_historical.nfl_player_stats_all VALUES
            ('unplayed',2026,1,'REG','DAL','NYG','unplayed_2026_1','QB',NULL,NULL,'preserve')
        """)
    expected = _finalized_ops(reader, year=2026, through_week=1)
    ops_cache._patch_research_ops_cache_from_fly(
        reader, expected, base=cache_path, year=2026, weeks=[1],
        work_dir=cache_path.parent, in_place=True,
    )
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("""
            SELECT NFL_player_id FROM nfl_historical.nfl_player_stats_all
            WHERE year=2026 ORDER BY NFL_player_id
        """).fetchall() == [('caleb',), ('unplayed',)]


def test_reduced_weekly_cache_avoids_download_for_unrelated_rank_variants(ops_sources):
    from scripts.refresh_yahoo_active_season import _finalized_ops

    cache_path, live, reader = ops_sources
    frame = live.execute("SELECT * FROM nfl_historical.nfl_player_stats_all WHERE week=1").fetchdf()
    with duckdb.connect(str(cache_path)) as cache:
        cache.register('seed', frame)
        cache.execute('INSERT INTO nfl_historical.nfl_player_stats_all SELECT * FROM seed')
    live.execute('ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN rank_qb_6pt INTEGER DEFAULT 9')

    for rank in (1, 3, 3):
        live.execute(f'UPDATE nfl_historical.nfl_player_stats_all SET rank_qb_4pt={rank} WHERE week=1')
        expected = _finalized_ops(reader, year=2026, through_week=1)
        ops_cache._patch_research_ops_cache_from_fly(
            reader, expected, base=cache_path, year=2026, weeks=[1],
            work_dir=cache_path.parent, in_place=True,
        )
    downloads = [sql for sql in reader.queries if 'NULL AS "recon_correction_log"' in sql]
    assert len(downloads) == 1
    assert 'rank_qb_6pt' not in downloads[0]
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute('SELECT rank_qb_4pt FROM nfl_historical.nfl_player_stats_all WHERE year=2026').fetchone() == (3,)


def test_failed_insert_rolls_back_local_identity_removals(ops_sources):
    cache_path, live, reader = ops_sources
    live.execute('ALTER TABLE nfl_historical.nfl_player_stats_all ALTER rank_qb_4pt TYPE VARCHAR')
    live.execute("UPDATE nfl_historical.nfl_player_stats_all SET rank_qb_4pt='invalid' WHERE week=1")
    with duckdb.connect(str(cache_path)) as cache:
        cache.execute("""
            INSERT INTO nfl_historical.nfl_player_stats_all VALUES
            ('caleb',2026,1,'REG','CHI','GB','caleb_2026_1','QB',7,999,'keep until success')
        """)
    with pytest.raises(RuntimeError, match="2026"):
        import_utils.run_track_1_verify(2025, 2026)
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("""
            SELECT opponent_nfl_team, rank_qb_4pt FROM nfl_historical.nfl_player_stats_all
            WHERE year=2026
        """).fetchall() == [('GB', 7)]


def test_preseason_does_not_hide_missing_previously_cached_games(ops_sources, monkeypatch):
    cache_path, live, _reader = ops_sources
    live.execute("DELETE FROM nfl_historical.nfl_player_stats_all")
    monkeypatch.setattr(date_utils, "get_nfl_state", lambda: {
        "season": "2026", "league_season": "2026", "season_type": "pre",
    })
    with duckdb.connect(str(cache_path)) as cache:
        cache.execute("""
            INSERT INTO nfl_historical.nfl_player_stats_all VALUES
            ('caleb',2026,1,'REG','CHI','GB','caleb_2026_1','QB',7,999,'played')
        """)
    with pytest.raises(RuntimeError, match="2026"):
        import_utils.run_track_1_verify(2026, 2026)
