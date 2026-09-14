import pytest
import duckdb
from pathlib import Path
import local_reader as local_reader_module

from local_reader import (
    LocalReader,
    configured_league_bucket,
    configured_source_mode,
    source_fingerprint,
)


def test_configured_league_bucket_is_zero_indexed(monkeypatch):
    monkeypatch.setenv("RESEARCH_LEAGUE_BUCKETS", "4")
    monkeypatch.setenv("RESEARCH_LEAGUE_BUCKET", "2")

    assert configured_league_bucket() == (2, 4)


def test_configured_league_bucket_fails_closed_on_incomplete_or_invalid_config(monkeypatch):
    monkeypatch.setenv("RESEARCH_LEAGUE_BUCKETS", "4")
    monkeypatch.delenv("RESEARCH_LEAGUE_BUCKET", raising=False)
    with pytest.raises(ValueError, match="must be set together"):
        configured_league_bucket()

    monkeypatch.setenv("RESEARCH_LEAGUE_BUCKET", "4")
    with pytest.raises(ValueError, match="0 <= bucket < buckets"):
        configured_league_bucket()


def test_configured_source_mode_defaults_to_full_and_rejects_unknown(monkeypatch):
    monkeypatch.delenv("RESEARCH_SOURCE_MODE", raising=False)
    assert configured_source_mode() == "full"

    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "real_delta")
    assert configured_source_mode() == "real_delta"

    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "surprise")
    with pytest.raises(ValueError, match="RESEARCH_SOURCE_MODE"):
        configured_source_mode()


def test_weighted_partition_restricts_population_gate_before_full_scan():
    """Weighted shards must filter the source before _lg_has_data is computed."""
    source = Path(local_reader_module.__file__).read_text(encoding="utf-8")
    partition = source.index("source_partition_guard =")
    population_gate = source.index("CREATE TEMP TABLE _lg_has_data")
    assert partition < population_gate
    assert "WHERE {source_partition_guard}" in source


def test_source_fingerprint_changes_with_mode_and_source_version(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.duckdb"
    corpus = tmp_path / "corpus.duckdb"
    ops = tmp_path / "ops.duckdb"
    for path in (snapshot, corpus, ops):
        path.write_bytes(b"v1")

    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "full")
    full_v1 = source_fingerprint(snapshot, corpus, ops)
    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "real_delta")
    delta_v1 = source_fingerprint(snapshot, corpus, ops)
    assert delta_v1 != full_v1

    snapshot.write_bytes(b"a newer snapshot")
    assert source_fingerprint(snapshot, corpus, ops) != delta_v1


def _write_source(path, leagues):
    con = duckdb.connect(str(path))
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, platform VARCHAR, league_key VARCHAR
    )""")
    con.execute("""CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, is_rostered INTEGER,
        NFL_player_id VARCHAR, fantasy_points DOUBLE
    )""")
    for name, platform, league_key in leagues:
        con.execute(
            "INSERT INTO public.league_settings VALUES (?, 2024, ?, ?)",
            [name, platform, league_key],
        )
        con.execute("""INSERT INTO public.player_fantasy
            SELECT ?, 2024, 1, 'p' || CAST(i AS VARCHAR), 10.0
            FROM range(100) t(i)""", [name])
    con.close()


def test_real_delta_exposes_only_private_nonoverlapping_leagues(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.duckdb"
    corpus = tmp_path / "corpus.duckdb"
    _write_source(snapshot, [
        ("real_overlap", "sleeper", "shared-key"),
        ("real_private", "sleeper", "private-key"),
        ("real_yahoo", "yahoo", "shared-key"),
    ])
    _write_source(corpus, [("public_copy", "sleeper", "shared-key")])
    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "real_delta")
    monkeypatch.delenv("RESEARCH_SAMPLE_LEAGUES", raising=False)
    monkeypatch.delenv("RESEARCH_LEAGUE_BUCKETS", raising=False)
    monkeypatch.delenv("RESEARCH_LEAGUE_BUCKET", raising=False)

    reader = LocalReader(snapshot=snapshot, corpus=corpus, ops_cache=None)
    leagues = {row["db_name"] for row in reader.query(
        "SELECT db_name FROM public.league_settings"
    )}
    player_sources = {row["db_name"] for row in reader.query(
        "SELECT DISTINCT db_name FROM public.player_fantasy"
    )}
    reader.close()

    assert leagues == {"real_private", "real_yahoo"}
    assert player_sources == {"real_private", "real_yahoo"}


def test_real_delta_requires_corpus_for_overlap_protection(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.duckdb"
    _write_source(snapshot, [("real_private", "sleeper", "private-key")])
    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "real_delta")

    with pytest.raises(RuntimeError, match="requires a corpus snapshot"):
        LocalReader(snapshot=snapshot, corpus=None, ops_cache=None)


def test_install_league_filter_exposes_only_requested_league_years(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.duckdb"
    _write_source(snapshot, [
        ("included", "sleeper", "included-key"),
        ("excluded", "sleeper", "excluded-key"),
    ])
    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "full")
    monkeypatch.delenv("RESEARCH_SAMPLE_LEAGUES", raising=False)
    monkeypatch.delenv("RESEARCH_LEAGUE_BUCKETS", raising=False)
    monkeypatch.delenv("RESEARCH_LEAGUE_BUCKET", raising=False)
    reader = LocalReader(snapshot=snapshot, corpus=None, ops_cache=None)

    reader.install_league_filter([("included", 2024)])

    assert reader.query(
        "SELECT DISTINCT db_name, year FROM public.filtered_player_fantasy"
    ) == [{"db_name": "included", "year": 2024}]
    reader.close()


def test_install_league_filter_rejects_empty_and_duplicate_inputs(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.duckdb"
    _write_source(snapshot, [("included", "sleeper", "included-key")])
    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "full")
    reader = LocalReader(snapshot=snapshot, corpus=None, ops_cache=None)

    with pytest.raises(ValueError, match="cannot be empty"):
        reader.install_league_filter([])
    with pytest.raises(ValueError, match="duplicate"):
        reader.install_league_filter([("included", 2024), ("included", 2024)])
    reader.close()


def test_league_buckets_are_disjoint_and_exhaustive(tmp_path, monkeypatch):
    snapshot = tmp_path / "snapshot.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, platform VARCHAR, league_key VARCHAR
    )""")
    con.execute("""CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, is_rostered INTEGER,
        NFL_player_id VARCHAR, fantasy_points DOUBLE
    )""")
    expected = {f"league_{index}" for index in range(12)}
    for name in expected:
        con.execute("INSERT INTO public.league_settings VALUES (?, 2024, 'sleeper', ?)", [name, name])
        con.execute("""INSERT INTO public.player_fantasy
            SELECT ?, 2024, 1, 'p' || CAST(i AS VARCHAR), 10.0
            FROM range(100) t(i)""", [name])
    con.close()

    monkeypatch.delenv("RESEARCH_SAMPLE_LEAGUES", raising=False)
    buckets = []
    for bucket in range(4):
        monkeypatch.setenv("RESEARCH_LEAGUE_BUCKETS", "4")
        monkeypatch.setenv("RESEARCH_LEAGUE_BUCKET", str(bucket))
        reader = LocalReader(snapshot=snapshot, corpus=None, ops_cache=None)
        buckets.append({row["db_name"] for row in reader.query(
            "SELECT db_name FROM public.league_settings"
        )})
        reader.close()

    assert set().union(*buckets) == expected
    assert sum(len(bucket) for bucket in buckets) == len(expected)


@pytest.mark.parametrize("native_col", ["mfl_player_id", "fleaflicker_player_id"])
def test_native_platform_id_map_repairs_null_canonical_ids(tmp_path, monkeypatch, native_col):
    """A surviving native ID must beat the weaker name-only fallback."""
    snapshot = tmp_path / f"{native_col}.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, platform VARCHAR, league_key VARCHAR
    )""")
    con.execute(f"""CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, is_rostered INTEGER,
        NFL_player_id VARCHAR, player VARCHAR, position VARCHAR,
        {native_col} VARCHAR, fantasy_points DOUBLE
    )""")
    con.execute("INSERT INTO public.league_settings VALUES ('native', 2024, 'mfl', 'native')")
    con.execute(f"""INSERT INTO public.player_fantasy
        SELECT 'native', 2024, i + 1, 1,
               CASE WHEN i = 0 THEN 'nfl-1' ELSE NULL END,
               'Same Player', 'RB', 'native-1', 10.0
        FROM range(100) t(i)""")
    con.close()

    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "full")
    reader = LocalReader(snapshot=snapshot, corpus=None, ops_cache=None)
    row = reader.query("""
        SELECT COUNT(*) AS rows,
               COUNT(NFL_player_id) AS identified,
               COUNT(*) FILTER (NFL_player_id='nfl-1') AS repaired
        FROM public.player_fantasy
    """)[0]
    reader.close()

    assert row == {"rows": 100, "identified": 100, "repaired": 100}


def test_external_native_crosswalk_repairs_when_source_has_no_seed_id(tmp_path, monkeypatch):
    """A compact GH crosswalk can repair a fully-null historical source."""
    snapshot = tmp_path / "external.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, platform VARCHAR, league_key VARCHAR
    )""")
    con.execute("""CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, is_rostered INTEGER,
        NFL_player_id VARCHAR, player VARCHAR, position VARCHAR,
        mfl_player_id VARCHAR, fantasy_points DOUBLE
    )""")
    con.execute("INSERT INTO public.league_settings VALUES ('native', 2015, 'mfl', 'native')")
    con.execute("""INSERT INTO public.player_fantasy
        SELECT 'native', 2015, i + 1, 1, NULL,
               'Player Name', 'RB', '12294', 10.0
        FROM range(100) t(i)""")
    con.close()

    crosswalk = tmp_path / "native_id_crosswalk.parquet"
    map_con = duckdb.connect()
    map_con.execute("""CREATE TABLE map(
        platform VARCHAR, year INTEGER, native_id VARCHAR, NFL_player_id VARCHAR,
        name_norm VARCHAR, pos_family VARCHAR
    )""")
    map_con.execute("INSERT INTO map VALUES ('mfl', 2015, '12294', 'nfl-12294', 'player name', 'RB')")
    map_con.execute("COPY map TO ? (FORMAT PARQUET)", [str(crosswalk)])
    map_con.close()

    monkeypatch.setattr(local_reader_module, "NATIVE_ID_CROSSWALK", crosswalk)
    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "full")
    reader = LocalReader(snapshot=snapshot, corpus=None, ops_cache=None)
    row = reader.query("""
        SELECT COUNT(*) AS rows,
               COUNT(NFL_player_id) AS identified,
               COUNT(*) FILTER (NFL_player_id='nfl-12294') AS repaired
        FROM public.player_fantasy
    """)[0]
    reader.close()

    assert row == {"rows": 100, "identified": 100, "repaired": 100}


def test_external_mfl_name_position_crosswalk_repairs_legacy_rows(tmp_path, monkeypatch):
    snapshot = tmp_path / "external_name.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings(
        db_name VARCHAR, year INTEGER, platform VARCHAR, league_key VARCHAR
    )""")
    con.execute("""CREATE TABLE public.player_fantasy(
        db_name VARCHAR, year INTEGER, week INTEGER, is_rostered INTEGER,
        NFL_player_id VARCHAR, player VARCHAR, position VARCHAR, fantasy_points DOUBLE
    )""")
    con.execute("INSERT INTO public.league_settings VALUES ('legacy', 2015, 'mfl', 'legacy')")
    con.execute("""INSERT INTO public.player_fantasy
        SELECT 'legacy', 2015, i + 1, 1, NULL, 'Player Name', 'RB', 10.0
        FROM range(100) t(i)""")
    con.close()

    crosswalk = tmp_path / "name_crosswalk.parquet"
    map_con = duckdb.connect()
    map_con.execute("""CREATE TABLE map(
        platform VARCHAR, year INTEGER, native_id VARCHAR, NFL_player_id VARCHAR,
        name_norm VARCHAR, pos_family VARCHAR
    )""")
    map_con.execute("INSERT INTO map VALUES ('mfl', 2015, '12294', 'nfl-name', 'player name', 'RB')")
    map_con.execute("COPY map TO ? (FORMAT PARQUET)", [str(crosswalk)])
    map_con.close()

    monkeypatch.setattr(local_reader_module, "NATIVE_ID_CROSSWALK", crosswalk)
    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "full")
    reader = LocalReader(snapshot=snapshot, corpus=None, ops_cache=None)
    row = reader.query("""
        SELECT COUNT(*) AS rows,
               COUNT(NFL_player_id) AS identified,
               COUNT(*) FILTER (NFL_player_id='nfl-name') AS repaired
        FROM public.player_fantasy
    """)[0]
    reader.close()
    assert row == {"rows": 100, "identified": 100, "repaired": 100}


def test_source_team_identity_fills_schedule_when_player_has_no_ops_stats(tmp_path, monkeypatch):
    """A source NFL team can supply the team-game denominator for a stats-less player."""
    snapshot = tmp_path / "source_team.duckdb"
    con = duckdb.connect(str(snapshot))
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings(
        db_name VARCHAR, year INTEGER, platform VARCHAR, league_key VARCHAR
    )""")
    con.execute("""CREATE TABLE public.player_fantasy(
        db_name VARCHAR, year INTEGER, week INTEGER, is_rostered INTEGER,
        NFL_player_id VARCHAR, player VARCHAR, position VARCHAR,
        nfl_team_api VARCHAR, fantasy_points DOUBLE
    )""")
    con.execute("INSERT INTO public.league_settings VALUES ('source-team', 2024, 'sleeper', 'source-team')")
    con.execute("""INSERT INTO public.player_fantasy
        SELECT 'source-team', 2024, ((i % 16) + 1), 1, 'source-player',
               'Source Player', 'RB', 'KC', 10.0
        FROM range(100) t(i)""")
    con.close()

    ops = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(ops))
    con.execute("CREATE SCHEMA nfl_historical")
    con.execute("""CREATE TABLE nfl_historical.nfl_player_stats_all(
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, position VARCHAR,
        nfl_team VARCHAR, nfl_franchise_number INTEGER, season_type VARCHAR, player VARCHAR,
        offense_snaps INTEGER, special_teams_snaps INTEGER,
        defense_snaps INTEGER, fantasy_points_ppr DOUBLE
    )""")
    for teams in ("10t", "12t"):
        for scoring in ("std", "half", "ppr"):
            for td in ("4pt", "6pt"):
                con.execute(f'ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN "lamar_{teams}_flx_{scoring}_{td}" DOUBLE')
    for scoring in ("0ppr", "half", "ppr"):
        for td in ("4pt", "6pt"):
            con.execute(f'ALTER TABLE nfl_historical.nfl_player_stats_all ADD COLUMN "fpts_{td}_{scoring}" DOUBLE')
    # Another player establishes the KC schedule, with week 2 as the bye.
    for week in list(range(1, 17)):
        if week == 2:
            continue
        con.execute("""INSERT INTO nfl_historical.nfl_player_stats_all
            (NFL_player_id, year, week, position, nfl_team, nfl_franchise_number, season_type, player,
             offense_snaps, special_teams_snaps, defense_snaps, fantasy_points_ppr)
            VALUES
            ('schedule-player', 2024, ?, 'RB', 'KC', 12, 'REG', 'Schedule Player', 1, 0, 0, 1.0)""", [week])
    con.execute("CREATE TABLE nfl_historical.player_bio(NFL_player_id VARCHAR, nfl_position VARCHAR)")
    con.execute("INSERT INTO nfl_historical.player_bio VALUES ('source-player', 'RB')")
    con.close()

    monkeypatch.setenv("RESEARCH_SOURCE_MODE", "full")
    monkeypatch.setenv("RESEARCH_YEAR_START", "2024")
    monkeypatch.setenv("RESEARCH_YEAR_END", "2024")
    monkeypatch.delenv("RESEARCH_SAMPLE_LEAGUES", raising=False)
    reader = LocalReader(snapshot=snapshot, corpus=None, ops_cache=ops)
    weeks = [r["week"] for r in reader.query("""
        SELECT week FROM public.player_team_game_week
        WHERE NFL_player_id='source-player' AND year=2024 ORDER BY week
    """)]
    source_team = reader.query("""
        SELECT DISTINCT team, nfl_team FROM public.player_team_game_week
        WHERE NFL_player_id='source-player' AND year=2024
    """)
    reader.close()
    assert weeks == [1, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16]
    assert source_team == [{"team": "F:12", "nfl_team": "F:12"}]
