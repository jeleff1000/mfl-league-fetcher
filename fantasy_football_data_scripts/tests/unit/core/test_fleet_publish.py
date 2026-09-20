"""Unit tests for the fleet partition publish contract (client side)."""

import json
import tarfile

import duckdb
import pytest

from multi_league.core.delta_publish import (
    CADENCE_ACTIVE_SEASON,
    CADENCE_CLASSES,
    CADENCE_LEAGUE_ROLLUP,
    canonical_table_registry,
)
from multi_league.core.fleet_publish import (
    FLEET_DB_SENTINEL,
    FLEET_SCHEMA_VERSION,
    FleetScopeError,
    build_fleet_partition_bundle,
)

ACTIVE_YEAR = 2026


def test_registry_declares_cadence_for_every_table():
    registry = canonical_table_registry()
    for table, spec in registry.items():
        assert spec["cadence_class"] in CADENCE_CLASSES, table
        assert spec["weekly_merge_mode"] == "replace_scope", table
        if spec["cadence_class"] == CADENCE_ACTIVE_SEASON:
            assert "year" in spec["columns"], f"{table} is active_season without a year column"
            assert spec["weekly_scope_keys"] == ["db_name", "year"], table
        if spec["cadence_class"] == CADENCE_LEAGUE_ROLLUP:
            assert spec["weekly_scope_keys"] == ["db_name"], table
        # The full-import repair lane keeps whole-league semantics.
        assert spec["merge_mode"] == "replace_league", table


def test_registry_cadence_expectations_for_core_weekly_tables():
    registry = canonical_table_registry()
    for table in ("matchup", "player_fantasy", "schedule", "transactions", "draft"):
        assert registry[table]["cadence_class"] == CADENCE_ACTIVE_SEASON, table
    # Identity resolution history is whole-league despite having year/week.
    assert registry["franchise_identity_audit"]["cadence_class"] == CADENCE_LEAGUE_ROLLUP
    for table in ("matchup_career", "homepage_league_summary", "league_context"):
        assert registry[table]["cadence_class"] == CADENCE_LEAGUE_ROLLUP, table


def test_homepage_league_summary_has_a_single_row_league_identity():
    """The fleet worker needs a key to replace the one-row homepage payload."""
    registry = canonical_table_registry()

    assert registry["homepage_league_summary"]["identity_keys"] == ["db_name"]


def _staged_conn(*, extra_year=None, duplicate_identity=False, null_identity=False, blank_db=False):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager_week VARCHAR, manager VARCHAR
        )
        """
    )
    for league in ("league_a", "league_b"):
        for week in (1, 2):
            conn.execute(
                "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?)",
                [league, ACTIVE_YEAR, week, f"alice_{ACTIVE_YEAR}_{week}", "alice"],
            )
    if extra_year:
        conn.execute(
            "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?)",
            ["league_a", extra_year, 1, f"alice_{extra_year}_1", "alice"],
        )
    if duplicate_identity:
        conn.execute(
            "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?)",
            ["league_a", ACTIVE_YEAR, 1, f"alice_{ACTIVE_YEAR}_1", "alice"],
        )
    if null_identity:
        conn.execute(
            "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?)",
            ["league_a", ACTIVE_YEAR, 3, None, "alice"],
        )
    if blank_db:
        conn.execute(
            "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?)",
            ["", ACTIVE_YEAR, 3, f"blank_{ACTIVE_YEAR}_3", "alice"],
        )
    return conn


def test_build_bundle_manifest_shape(tmp_path):
    conn = _staged_conn()
    try:
        bundle = build_fleet_partition_bundle(
            conn,
            active_year=ACTIVE_YEAR,
            league_generations={"league_a": 0, "league_b": 0},
            tables=["matchup"],
            output_dir=tmp_path,
            import_run_id="123",
            publish_sequence=1,
        )
    finally:
        conn.close()

    manifest = bundle.manifest
    assert manifest["schema_version"] == FLEET_SCHEMA_VERSION
    assert manifest["db_name"] == FLEET_DB_SENTINEL
    assert manifest["mode"] == "weekly"
    assert manifest["active_year"] == ACTIVE_YEAR
    assert manifest["db_names"] == ["league_a", "league_b"]
    assert manifest["db_name_count"] == 2
    assert manifest["league_generations"] == {"league_a": 0, "league_b": 0}

    [entry] = manifest["tables"]
    assert entry["table"] == "matchup"
    assert entry["merge_mode"] == "replace_scope"
    assert entry["cadence_class"] == CADENCE_ACTIVE_SEASON
    assert entry["scope"] == {"year": ACTIVE_YEAR}
    assert entry["row_count"] == 4
    assert entry["db_name_count"] == 2
    assert entry["db_names_hash"]
    assert entry["primary_keys"] == ["db_name", "manager_week"]

    # Archive holds exactly the manifest + one parquet.
    with tarfile.open(bundle.path, "r:gz") as tar:
        names = sorted(m.name for m in tar.getmembers() if m.isfile())
    assert names == ["manifest.json", "tables/matchup.parquet"]

    # Manifest on disk round-trips.
    with tarfile.open(bundle.path, "r:gz") as tar:
        loaded = json.loads(tar.extractfile("manifest.json").read().decode("utf-8"))
    assert loaded["bundle_id"] == bundle.bundle_id
    assert loaded["bundle_hash"] == bundle.bundle_hash


def test_identical_fleet_content_and_base_generation_reuse_one_bundle_identity(tmp_path):
    conn = _staged_conn()
    try:
        first = build_fleet_partition_bundle(
            conn, active_year=ACTIVE_YEAR,
            league_generations={"league_a": 3, "league_b": 3},
            tables=["matchup"], output_dir=tmp_path / "first",
            import_run_id="123", publish_sequence=1,
        )
        replay = build_fleet_partition_bundle(
            conn, active_year=ACTIVE_YEAR,
            league_generations={"league_a": 3, "league_b": 3},
            tables=["matchup"], output_dir=tmp_path / "replay",
            import_run_id="456", publish_sequence=2,
        )
        newer_base = build_fleet_partition_bundle(
            conn, active_year=ACTIVE_YEAR,
            league_generations={"league_a": 4, "league_b": 3},
            tables=["matchup"], output_dir=tmp_path / "newer-base",
            import_run_id="456", publish_sequence=2,
        )
        conn.execute("UPDATE public.matchup SET manager = 'corrected' WHERE db_name = 'league_a' AND week = 1")
        correction = build_fleet_partition_bundle(
            conn, active_year=ACTIVE_YEAR,
            league_generations={"league_a": 3, "league_b": 3},
            tables=["matchup"], output_dir=tmp_path / "correction",
            import_run_id="456", publish_sequence=2,
        )
    finally:
        conn.close()
    assert first.bundle_id == replay.bundle_id
    assert first.bundle_hash == replay.bundle_hash
    assert first.manifest["import_run_id"] != replay.manifest["import_run_id"]
    assert first.bundle_id != newer_base.bundle_id
    assert first.bundle_id != correction.bundle_id


def test_recomputed_aggregate_timestamps_do_not_change_replay_identity(tmp_path):
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute(
            "CREATE TABLE public.homepage_league_summary "
            "(db_name VARCHAR, last_updated TIMESTAMP, data_year INTEGER, "
            "data_week INTEGER, highest_score_points DOUBLE)"
        )
        conn.execute(
            "INSERT INTO public.homepage_league_summary VALUES "
            "('league_a', '2026-09-15 12:00:00', 2026, 1, 152.66)"
        )

        def bundle(label):
            return build_fleet_partition_bundle(
                conn, active_year=ACTIVE_YEAR,
                league_generations={"league_a": 2},
                tables=["homepage_league_summary"],
                output_dir=tmp_path / label,
            )

        first = bundle("first")
        conn.execute(
            "UPDATE public.homepage_league_summary "
            "SET last_updated = '2026-09-15 13:00:00'"
        )
        replay = bundle("replay")
        assert first.bundle_id == replay.bundle_id
        assert first.bundle_hash == replay.bundle_hash

        conn.execute(
            "UPDATE public.homepage_league_summary SET highest_score_points = 153.66"
        )
        correction = bundle("correction")
        assert correction.bundle_id != first.bundle_id
    finally:
        conn.close()


def test_build_bundle_rejects_out_of_scope_year(tmp_path):
    conn = _staged_conn(extra_year=2025)
    try:
        with pytest.raises(FleetScopeError, match="outside active year"):
            build_fleet_partition_bundle(conn, active_year=ACTIVE_YEAR, league_generations={}, tables=["matchup"], output_dir=tmp_path)
    finally:
        conn.close()


def test_build_bundle_rejects_duplicate_identity(tmp_path):
    conn = _staged_conn(duplicate_identity=True)
    try:
        with pytest.raises(FleetScopeError, match="duplicate identity"):
            build_fleet_partition_bundle(conn, active_year=ACTIVE_YEAR, league_generations={}, tables=["matchup"], output_dir=tmp_path)
    finally:
        conn.close()


def test_build_bundle_rejects_null_identity(tmp_path):
    conn = _staged_conn(null_identity=True)
    try:
        with pytest.raises(FleetScopeError, match="null identity"):
            build_fleet_partition_bundle(conn, active_year=ACTIVE_YEAR, league_generations={}, tables=["matchup"], output_dir=tmp_path)
    finally:
        conn.close()


def test_build_bundle_rejects_blank_db_name(tmp_path):
    conn = _staged_conn(blank_db=True)
    try:
        with pytest.raises(FleetScopeError, match="blank db_name"):
            build_fleet_partition_bundle(conn, active_year=ACTIVE_YEAR, league_generations={}, tables=["matchup"], output_dir=tmp_path)
    finally:
        conn.close()


def test_build_bundle_rejects_missing_identity_columns(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    # matchup without manager_week (part of the registry identity)
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER)")
    conn.execute("INSERT INTO public.matchup VALUES ('league_a', ?, 1)", [ACTIVE_YEAR])
    try:
        with pytest.raises(FleetScopeError, match="missing identity columns"):
            build_fleet_partition_bundle(conn, active_year=ACTIVE_YEAR, league_generations={}, tables=["matchup"], output_dir=tmp_path)
    finally:
        conn.close()


def test_build_bundle_omits_empty_and_absent_tables(tmp_path):
    conn = _staged_conn()
    conn.execute("CREATE TABLE public.schedule (db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR)")
    try:
        bundle = build_fleet_partition_bundle(
            conn,
            active_year=ACTIVE_YEAR,
            league_generations={"league_a": 0, "league_b": 0},
            tables=["matchup", "schedule", "transactions"],
            output_dir=tmp_path,
        )
    finally:
        conn.close()

    included = {t["table"] for t in bundle.manifest["tables"]}
    omitted = {t["table"]: t["reason"] for t in bundle.manifest["omitted_tables"]}
    assert included == {"matchup"}
    assert omitted["schedule"] == "no_rows_in_scope"
    assert omitted["transactions"] == "table_not_present_locally"


def test_build_bundle_can_explicitly_empty_one_active_partition(tmp_path):
    conn = _staged_conn()
    conn.execute(
        "CREATE TABLE public.draft ("
        "db_name VARCHAR, year INTEGER, draft_id VARCHAR, round INTEGER, pick INTEGER)"
    )
    try:
        bundle = build_fleet_partition_bundle(
            conn,
            active_year=ACTIVE_YEAR,
            league_generations={"league_a": 7},
            tables=["draft"],
            empty_active_partitions={"draft"},
            output_dir=tmp_path,
        )
    finally:
        conn.close()

    entries = {entry["table"]: entry for entry in bundle.manifest["tables"]}
    assert entries["draft"]["row_count"] == 0
    assert entries["draft"]["empty_reason"] == "explicit_empty_active_partition"
    assert entries["draft"]["scope"] == {"year": ACTIVE_YEAR}
    assert bundle.manifest["db_names"] == ["league_a"]
    with tarfile.open(bundle.path, "r:gz") as tar:
        assert "tables/draft.parquet" in {member.name for member in tar.getmembers()}


def test_build_bundle_rejects_unsafe_empty_partition_requests(tmp_path):
    conn = _staged_conn()
    conn.execute(
        "CREATE TABLE public.draft ("
        "db_name VARCHAR, year INTEGER, draft_id VARCHAR, round INTEGER, pick INTEGER)"
    )
    try:
        with pytest.raises(FleetScopeError, match="exactly one generation-fenced league"):
            build_fleet_partition_bundle(
                conn,
                active_year=ACTIVE_YEAR,
                league_generations={"league_a": 0, "league_b": 0},
                tables=["draft"],
                empty_active_partitions={"draft"},
                output_dir=tmp_path / "many",
            )
        with pytest.raises(FleetScopeError, match="not enabled"):
            build_fleet_partition_bundle(
                conn,
                active_year=ACTIVE_YEAR,
                league_generations={"league_a": 0},
                tables=["matchup", "league_context"],
                empty_active_partitions={"league_context"},
                output_dir=tmp_path / "rollup",
            )
        empty_matchup = duckdb.connect(":memory:")
        empty_matchup.execute("CREATE SCHEMA public")
        empty_matchup.execute(
            "CREATE TABLE public.matchup ("
            "db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR)"
        )
        try:
            with pytest.raises(FleetScopeError, match="not enabled"):
                build_fleet_partition_bundle(
                    empty_matchup,
                    active_year=ACTIVE_YEAR,
                    league_generations={"league_a": 0},
                    tables=["matchup"],
                    empty_active_partitions={"matchup"},
                    output_dir=tmp_path / "core-fact",
                )
        finally:
            empty_matchup.close()
    finally:
        conn.close()


def test_build_bundle_requires_publishable_table(tmp_path):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    try:
        with pytest.raises(FleetScopeError, match="no publishable tables"):
            build_fleet_partition_bundle(conn, active_year=ACTIVE_YEAR, league_generations={}, tables=["matchup"], output_dir=tmp_path)
    finally:
        conn.close()


def test_build_bundle_rejects_unknown_table(tmp_path):
    conn = duckdb.connect(":memory:")
    try:
        with pytest.raises(ValueError, match="Unknown canonical tables"):
            build_fleet_partition_bundle(conn, active_year=ACTIVE_YEAR, league_generations={}, tables=["not_a_table"], output_dir=tmp_path)
    finally:
        conn.close()


def _prior_baseline(active_year=ACTIVE_YEAR):
    """Shape of scripts/publish_baseline.py capture output (light level)."""
    sep = chr(31)
    return {
        "tables": {
            "matchup": {
                "scope_keys": ["db_name", "year"],
                "scopes": {
                    f"league_a{sep}{active_year}": {"rows": 2},
                    f"league_b{sep}{active_year}": {"rows": 2},
                },
            },
            "player_fantasy": {
                "scope_keys": ["db_name", "year"],
                "scopes": {
                    f"league_a{sep}{active_year}": {"rows": 100},
                    f"league_b{sep}{active_year}": {"rows": 100},
                },
            },
        }
    }


def _staged_for_quarantine(*, league_b_player_rows=110):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        "CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR)"
    )
    conn.execute("CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, player_week VARCHAR)")
    conn.execute("CREATE TABLE public.schedule (db_name VARCHAR, year INTEGER, week INTEGER, manager_week VARCHAR)")
    for league, player_rows in (("league_a", 110), ("league_b", league_b_player_rows), ("league_new", 40)):
        for week in (1, 2, 3):
            conn.execute(
                "INSERT INTO public.matchup VALUES (?, ?, ?, ?)",
                [league, ACTIVE_YEAR, week, f"{league}_m_{week}"],
            )
            conn.execute(
                "INSERT INTO public.schedule VALUES (?, ?, ?, ?)",
                [league, ACTIVE_YEAR, week, f"{league}_s_{week}"],
            )
        for i in range(player_rows):
            conn.execute(
                "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?)",
                [league, ACTIVE_YEAR, 1 + i % 3, f"{league}_p_{i}"],
            )
    return conn


def test_completeness_gate_passes_healthy_and_new_leagues():
    from multi_league.core.fleet_publish import quarantine_incomplete_leagues

    conn = _staged_for_quarantine()
    try:
        report = quarantine_incomplete_leagues(
            conn, active_year=ACTIVE_YEAR, prior_baseline=_prior_baseline()
        )
        assert report["quarantined"] == {}
        assert report["checked"] == 3
        assert report["passed"] == 3
    finally:
        conn.close()


def test_completeness_gate_quarantines_row_count_collapse():
    """A league whose fetch half-succeeded (player rows collapsed vs baseline)
    is removed from EVERY staged table so it cannot be published fresh-but-wrong."""
    from multi_league.core.fleet_publish import quarantine_incomplete_leagues

    conn = _staged_for_quarantine(league_b_player_rows=10)  # baseline had 100
    try:
        report = quarantine_incomplete_leagues(
            conn, active_year=ACTIVE_YEAR, prior_baseline=_prior_baseline()
        )
        assert list(report["quarantined"]) == ["league_b"]
        assert "row-count collapse" in report["quarantined"]["league_b"][0]
        # Quarantined league removed from all staged tables; others intact.
        for table in ("matchup", "player_fantasy", "schedule"):
            remaining = {
                row[0]
                for row in conn.execute(f"SELECT DISTINCT db_name FROM public.{table}").fetchall()
            }
            assert remaining == {"league_a", "league_new"}, table
    finally:
        conn.close()


def test_completeness_gate_quarantines_missing_core_table_rows():
    from multi_league.core.fleet_publish import quarantine_incomplete_leagues

    conn = _staged_for_quarantine()
    conn.execute("DELETE FROM public.matchup WHERE db_name = 'league_a'")
    try:
        report = quarantine_incomplete_leagues(
            conn, active_year=ACTIVE_YEAR, prior_baseline=_prior_baseline()
        )
        assert list(report["quarantined"]) == ["league_a"]
        assert "staged has none" in report["quarantined"]["league_a"][0]
    finally:
        conn.close()
