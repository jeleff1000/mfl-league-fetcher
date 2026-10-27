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
        read_kwargs = []

        def query_df(self, sql, *, database, **kwargs):
            assert database == "___ops"
            assert "SELECT *" not in sql.upper()
            self.queries.append(sql)
            self.read_kwargs.append(kwargs)
            if self.fail:
                raise RuntimeError("Fly unavailable")
            return live.execute(sql).fetchdf()

        query_df_parquet = query_df

        def query(self, sql, *, database, **kwargs):
            return self.query_df(sql, database=database, **kwargs).to_dict("records")

    reader = Reader()
    reader.read_kwargs.clear()
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


def test_import_ops_cache_reads_use_the_worker_retry_budget(ops_sources):
    _cache_path, _live, reader = ops_sources

    assert import_utils.run_track_1_verify(2025, 2026) is True

    assert reader.read_kwargs
    assert all(
        kwargs == {"timeout_seconds": 60, "max_retries": 3}
        for kwargs in reader.read_kwargs
    ), reader.read_kwargs


def test_initial_import_accepts_ops_reads_slower_than_ui_timeout(ops_sources, monkeypatch):
    """A healthy scoring revision scan may exceed the live refresh's 8s limit."""
    from io import BytesIO
    import requests
    from multi_league.core.readers.fly_reader import FlyReader

    cache_path, live, _reader = ops_sources
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")
    monkeypatch.delenv("LEAGUE_UPDATE_DEADLINE_EPOCH", raising=False)
    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.sleep", lambda _seconds: None)
    revision_timeouts = []

    def post(url, *, json, **kwargs):
        if json["sql"].startswith("SELECT week,"):
            revision_timeouts.append(kwargs["timeout"])
            if kwargs["timeout"] < 20:
                raise requests.exceptions.ReadTimeout("healthy OPS scan needs 20 seconds")
        frame = live.execute(json["sql"]).fetchdf()
        response = requests.Response()
        response.status_code = 200
        if url.endswith("/query-parquet"):
            payload = BytesIO()
            frame.to_parquet(payload, index=False)
            response._content = payload.getvalue()
        else:
            response._content = frame.to_json(orient="records").encode()
        return response

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    monkeypatch.setattr(db_reader, "get_reader", FlyReader)

    assert import_utils.run_track_1_verify(2025, 2026) is True
    assert revision_timeouts == [60]
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute(
            "SELECT year, week, rank_alltime_qb_4pt FROM "
            "nfl_historical.nfl_player_stats_all ORDER BY year, week"
        ).fetchall() == [(2025, 1, 222), (2026, 1, 216), (2026, 2, 216)]


def test_import_current_rank_revisions_do_not_download_wide_rows(ops_sources):
    cache_path, live, reader = ops_sources
    current = live.execute("SELECT * FROM nfl_historical.nfl_player_stats_all").fetchdf()
    with duckdb.connect(str(cache_path)) as cache:
        cache.register("current_rows", current)
        cache.execute("INSERT INTO nfl_historical.nfl_player_stats_all SELECT * FROM current_rows")
    reader.queries.clear()

    assert import_utils.run_track_1_verify(2025, 2026) is True

    assert not any('NULL AS "recon_correction_log"' in sql for sql in reader.queries)
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("SELECT year, week, rank_alltime_qb_4pt FROM "
                             "nfl_historical.nfl_player_stats_all ORDER BY year, week").fetchall() == [
            (2025, 1, 222), (2026, 1, 216), (2026, 2, 216),
        ]


def test_refresh_rank_inputs_recover_two_timeouts_without_losing_history(
    ops_sources, monkeypatch
):
    """Replay the shared NFL-read failure before patching only admitted rows."""
    import time
    import requests
    from multi_league.core.readers.fly_reader import FlyReader

    cache_path, live, _reader = ops_sources
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", str(time.time() + 120))
    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.sleep", lambda _seconds: None)
    revision_attempts = []

    def post(_url, *, json, **kwargs):
        from io import BytesIO

        if json["sql"].startswith("SELECT week,"):
            revision_attempts.append(kwargs["timeout"])
            if len(revision_attempts) < 3:
                raise requests.exceptions.ReadTimeout("transient NFL revision read stall")
        frame = live.execute(json["sql"]).fetchdf()
        response = requests.Response()
        response.status_code = 200
        if _url.endswith("/query-parquet"):
            payload = BytesIO()
            frame.to_parquet(payload, index=False)
            response._content = payload.getvalue()
        else:
            response._content = frame.to_json(orient="records").encode()
        return response

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    reader = FlyReader()
    finalized = ops_cache._finalized_ops(reader, year=2026, through_week=1)
    ops_cache._patch_research_ops_cache_from_fly(
        reader, finalized, base=cache_path, year=2026, weeks=[1],
        work_dir=cache_path.parent, in_place=True,
    )

    assert revision_attempts == [8, 8, 8]
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute(
            "SELECT year, week, rank_alltime_qb_4pt FROM "
            "nfl_historical.nfl_player_stats_all ORDER BY year, week"
        ).fetchall() == [(2025, 1, 222), (2026, 1, 216)]


def test_import_detects_rank_only_correction_in_an_older_week(ops_sources):
    cache_path, live, reader = ops_sources
    current = live.execute("SELECT * FROM nfl_historical.nfl_player_stats_all").fetchdf()
    with duckdb.connect(str(cache_path)) as cache:
        cache.register("current_rows", current)
        cache.execute("INSERT INTO nfl_historical.nfl_player_stats_all SELECT * FROM current_rows")
    live.execute("UPDATE nfl_historical.nfl_player_stats_all SET rank_alltime_qb_4pt=213 WHERE week=1")
    reader.queries.clear()

    assert import_utils.run_track_1_verify(2025, 2026) is True

    downloads = [sql for sql in reader.queries if 'NULL AS "recon_correction_log"' in sql]
    assert len(downloads) == 1
    assert "week = 1" in downloads[0]
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("SELECT year, week, rank_alltime_qb_4pt FROM "
                             "nfl_historical.nfl_player_stats_all ORDER BY year, week").fetchall() == [
            (2025, 1, 222), (2026, 1, 213), (2026, 2, 216),
        ]


@pytest.mark.parametrize("column,kind,old,new", [
    ("player_week", "VARCHAR", "caleb_2026_1", "corrected-player-week"),
    ("position", "VARCHAR", "QB", "RB"),
    ("nfl_position", "VARCHAR", "QB", "RB"),
    ("fpts_ppr", "DOUBLE", 10.0, 24.0),
    ("ppg_season_4pt_ppr", "DOUBLE", 10.0, 24.0),
    ("ppg_alltime_4pt_ppr", "DOUBLE", 10.0, 24.0),
])
def test_import_revision_covers_every_copied_enrichment_input(ops_sources, column, kind, old, new):
    cache_path, live, reader = ops_sources
    with duckdb.connect(str(cache_path)) as cache:
        if column not in {"player_week", "position"}:
            cache.execute(f'ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN "{column}" {kind}')
            live.execute(f'ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN "{column}" {kind}')
        live.execute(f'UPDATE nfl_historical.nfl_player_stats_all SET "{column}"=? WHERE week=1', [old])
        current = live.execute("SELECT * FROM nfl_historical.nfl_player_stats_all").fetchdf()
        cache.register("current_rows", current)
        cache.execute("INSERT INTO nfl_historical.nfl_player_stats_all SELECT * FROM current_rows")
    live.execute(f'UPDATE nfl_historical.nfl_player_stats_all SET "{column}"=? WHERE week=1', [new])
    assert import_utils.run_track_1_verify(2025, 2026) is True
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute(f'SELECT "{column}" FROM nfl_historical.nfl_player_stats_all '
                             'WHERE year=2026 AND week=1').fetchone() == (new,)


def test_cache_revision_covers_the_existing_precomputed_ppg_registry():
    from multi_league.transformations.player.modules.ppg_precompute import (
        PPG_VARIANTS, get_ppg_columns_for_scoring,
    )

    metrics = {column.rsplit("_", 2)[0] for column in get_ppg_columns_for_scoring().values()}
    columns = {f"{metric}_{td}_{ppr}" for metric in metrics for td, ppr in PPG_VARIANTS}
    assert columns <= set(ops_cache._ops_revision_columns(columns))
    assert "recon_correction_log" not in ops_cache._ops_revision_columns(["recon_correction_log"])


@pytest.mark.parametrize("fault", ["timeout", "disconnect", "exhausted", "incomplete"])
def test_shared_import_http_faults_preserve_history_and_authoritative_ranks(ops_sources, monkeypatch, fault):
    """Exercise the real reader/retries, DuckDB patch and ranking consumer together."""
    from io import BytesIO
    import requests
    from multi_league.core.readers.fly_reader import FlyReader

    cache_path, live, _reader = ops_sources
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-token")
    monkeypatch.delenv("LEAGUE_UPDATE_DEADLINE_EPOCH", raising=False)
    monkeypatch.setattr(db_reader, "get_reader", FlyReader)
    monkeypatch.setattr("multi_league.core.readers.fly_reader.time.sleep", lambda _seconds: None)
    attempts = []

    def post(url, *, json, **kwargs):
        frame = live.execute(json["sql"]).fetchdf()
        response = requests.Response()
        response.status_code = 200
        if url.endswith("/query-parquet"):
            attempts.append(json["sql"])
            if fault == "exhausted" or (len(attempts) == 1 and fault == "timeout"):
                raise requests.exceptions.ReadTimeout("injected timeout")
            if len(attempts) == 1 and fault == "disconnect":
                raise requests.exceptions.ChunkedEncodingError("injected connection loss")
            if fault == "incomplete":
                frame = frame.iloc[:0]
            payload = BytesIO()
            frame.to_parquet(payload, index=False)
            response._content = payload.getvalue()
        else:
            response._content = frame.to_json(orient="records").encode()
        return response

    monkeypatch.setattr("multi_league.core.readers.fly_reader.requests.post", post)
    if fault in {"exhausted", "incomplete"}:
        with pytest.raises(RuntimeError, match="2026"):
            import_utils.run_track_1_verify(2025, 2026)
    else:
        assert import_utils.run_track_1_verify(2025, 2026) is True
    with duckdb.connect(str(cache_path), read_only=True) as cache:
        assert cache.execute("SELECT rank_alltime_qb_4pt, recon_correction_log FROM "
                             "nfl_historical.nfl_player_stats_all WHERE year=2025").fetchone() == (222, "historical")
        current = cache.execute("SELECT position, rank_qb_4pt, rank_alltime_qb_4pt FROM "
                                "nfl_historical.nfl_player_stats_all WHERE year=2026 ORDER BY week").fetchdf()
    if fault in {"exhausted", "incomplete"}:
        assert current.empty
    else:
        assert calculate_nfl_rankings(pl.from_pandas(current))["position_alltime_rank"].to_list() == [216, 216]
        assert len(attempts) == 3  # one failed attempt, then each of the two admitted weeks
    if fault == "exhausted":
        assert len(attempts) == 3


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


def test_existing_cache_refreshes_only_changed_rank_rows(ops_sources):
    cache_path, live, reader = ops_sources
    current = live.execute("""
        SELECT NFL_player_id, year, week, season_type, nfl_team, opponent_nfl_team,
               player_week, position, rank_qb_4pt, rank_alltime_qb_4pt, recon_correction_log
        FROM nfl_historical.nfl_player_stats_all
    """).fetchdf()
    current.loc[current["week"] == 2, "rank_alltime_qb_4pt"] = 1
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
        """).fetchall() == [(1, 216), (2, 216)]


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
