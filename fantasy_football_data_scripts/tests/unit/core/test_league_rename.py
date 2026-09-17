from __future__ import annotations

import duckdb

from multi_league.core.league_rename import (
    consolidate_canonical_league,
    retarget_league_control_plane,
    validate_control_plane_rename,
)


def _registry() -> dict[str, dict]:
    return {
        "matchup": {
            "kind": "core",
            "columns": {"db_name": "VARCHAR", "year": "INTEGER", "manager_week": "VARCHAR", "points": "DOUBLE"},
            "identity_keys": ["db_name", "manager_week"],
        },
        "draft": {
            "kind": "core",
            "columns": {"db_name": "VARCHAR", "year": "INTEGER", "round": "INTEGER", "pick": "INTEGER", "player": "VARCHAR"},
            "identity_keys": ["db_name", "year", "round", "pick"],
        },
        "league_context": {
            "kind": "core",
            "columns": {"db_name": "VARCHAR", "league_name": "VARCHAR", "platform": "VARCHAR", "league_id": "VARCHAR"},
            "identity_keys": ["db_name"],
        },
        "homepage_manager_rankings": {
            "kind": "aggregate",
            "columns": {"db_name": "VARCHAR", "manager": "VARCHAR", "wins": "INTEGER"},
            "identity_keys": ["db_name", "manager"],
        },
    }


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.matchup(db_name VARCHAR, year INTEGER, manager_week VARCHAR, points DOUBLE)")
    conn.execute("CREATE TABLE public.draft(db_name VARCHAR, year INTEGER, round INTEGER, pick INTEGER, player VARCHAR)")
    conn.execute("CREATE TABLE public.league_context(db_name VARCHAR, league_name VARCHAR, platform VARCHAR, league_id VARCHAR)")
    conn.execute("CREATE TABLE public.homepage_manager_rankings(db_name VARCHAR, manager VARCHAR, wins INTEGER)")
    return conn


def test_partial_rename_consolidates_history_without_losing_target_only_rows() -> None:
    conn = _connection()
    conn.execute(
        "INSERT INTO public.matchup VALUES "
        "('agustafantasyleague', 2009, '2009_1_a', 91), "
        "('agustafantasyleague', 2025, '2025_1_a', 101), "
        "('agusta_fantasy_league', 2026, '2026_1_a', 111)"
    )
    conn.execute(
        "INSERT INTO public.draft VALUES "
        "('agusta_fantasy_league', 2009, 1, 1, 'Old Pick'), "
        "('agusta_fantasy_league', 2026, 1, 1, 'New Pick')"
    )
    conn.execute(
        "INSERT INTO public.league_context VALUES "
        "('agustafantasyleague', 'Agusta Fantasy League', 'multi-platform', 'legacy'), "
        "('agusta_fantasy_league', 'Agusta Fantasy League', 'multi-platform', 'current')"
    )
    conn.execute(
        "INSERT INTO public.homepage_manager_rankings VALUES "
        "('agustafantasyleague', 'Ashley', 100), "
        "('agusta_fantasy_league', 'Ashley', 1), "
        "('agusta_fantasy_league', 'Bob', 2)"
    )

    result = consolidate_canonical_league(
        conn,
        source_db="agustafantasyleague",
        target_db="agusta_fantasy_league",
        display_name="Agusta Fantasy League",
        operation_id="rename-agusta",
        registry=_registry(),
    )

    assert conn.execute(
        "SELECT year, manager_week FROM public.matchup WHERE db_name = 'agusta_fantasy_league' ORDER BY year"
    ).fetchall() == [(2009, "2009_1_a"), (2025, "2025_1_a"), (2026, "2026_1_a")]
    assert conn.execute(
        "SELECT year, player FROM public.draft WHERE db_name = 'agusta_fantasy_league' ORDER BY year"
    ).fetchall() == [(2009, "Old Pick"), (2026, "New Pick")]
    assert conn.execute(
        "SELECT league_name, league_id FROM public.league_context WHERE db_name = 'agusta_fantasy_league'"
    ).fetchall() == [("Agusta Fantasy League", "current")]
    assert conn.execute(
        "SELECT manager, wins FROM public.homepage_manager_rankings "
        "WHERE db_name = 'agusta_fantasy_league' ORDER BY manager"
    ).fetchall() == [("Ashley", 100), ("Bob", 2)]
    assert result["source_rows_remaining"] == 0
    assert result["target_years"] == [2009, 2025, 2026]
    assert result["aggregate_tables_retargeted"] == ["homepage_manager_rankings"]


def test_partial_rename_keeps_target_version_of_an_overlapping_identity() -> None:
    conn = _connection()
    conn.execute(
        "INSERT INTO public.matchup VALUES "
        "('agustafantasyleague', 2026, '2026_1_a', 110), "
        "('agustafantasyleague', 2026, '2026_1_b', 90), "
        "('agusta_fantasy_league', 2026, '2026_1_a', 111)"
    )

    consolidate_canonical_league(
        conn,
        source_db="agustafantasyleague",
        target_db="agusta_fantasy_league",
        display_name="Agusta Fantasy League",
        operation_id="rename-agusta-overlap",
        registry=_registry(),
    )

    assert conn.execute(
        "SELECT manager_week, points FROM public.matchup WHERE db_name = 'agusta_fantasy_league' ORDER BY manager_week"
    ).fetchall() == [("2026_1_a", 111.0), ("2026_1_b", 90.0)]
    assert conn.execute(
        "SELECT COUNT(*) FROM public.matchup WHERE db_name = 'agustafantasyleague'"
    ).fetchone()[0] == 0


def test_completed_rename_is_idempotent() -> None:
    conn = _connection()
    conn.execute("INSERT INTO public.matchup VALUES ('old_league', 2025, '2025_1_a', 91)")

    first = consolidate_canonical_league(
        conn,
        source_db="old_league",
        target_db="new_league",
        display_name="New League",
        operation_id="rename-new-league",
        registry=_registry(),
    )
    second = consolidate_canonical_league(
        conn,
        source_db="old_league",
        target_db="new_league",
        display_name="New League",
        operation_id="rename-new-league",
        registry=_registry(),
    )

    assert first["status"] == "CONSOLIDATED"
    assert second["status"] == "ALREADY_CONSOLIDATED"
    assert conn.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0] == 1


def test_rename_never_touches_tables_outside_the_registry() -> None:
    conn = _connection()
    conn.execute("CREATE TABLE public.__corrupt_recovery_matchup(db_name VARCHAR, year INTEGER)")
    conn.execute("INSERT INTO public.__corrupt_recovery_matchup VALUES ('old_league', 2001)")
    conn.execute("INSERT INTO public.matchup VALUES ('old_league', 2025, '2025_1_a', 91)")

    consolidate_canonical_league(
        conn,
        source_db="old_league",
        target_db="new_league",
        display_name="New League",
        operation_id="rename-allowlist",
        registry=_registry(),
    )

    assert conn.execute("SELECT * FROM public.__corrupt_recovery_matchup").fetchall() == [
        ("old_league", 2001)
    ]


def test_data_rename_retargets_publish_generation_and_receipts() -> None:
    conn = _connection()
    conn.execute("CREATE SCHEMA merge_admin")
    conn.execute(
        "CREATE TABLE merge_admin.league_publish_generations(db_name VARCHAR, generation BIGINT, lane VARCHAR, run_id VARCHAR, updated_at TIMESTAMP)"
    )
    conn.execute(
        "CREATE TABLE merge_admin.league_delta_merge_state(db_name VARCHAR, bundle_id VARCHAR, status VARCHAR)"
    )
    conn.execute(
        "INSERT INTO merge_admin.league_publish_generations VALUES "
        "('old_league', 7, 'weekly', 'old-run', '2026-09-16'), "
        "('new_league', 3, 'quick', 'new-run', '2026-09-17')"
    )
    conn.execute(
        "INSERT INTO merge_admin.league_delta_merge_state VALUES ('old_league', 'bundle-1', 'COMMITTED')"
    )
    conn.execute("INSERT INTO public.matchup VALUES ('old_league', 2025, '2025_1_a', 91)")

    consolidate_canonical_league(
        conn,
        source_db="old_league",
        target_db="new_league",
        display_name="New League",
        operation_id="rename-generation",
        registry=_registry(),
    )

    assert conn.execute(
        "SELECT db_name, generation FROM merge_admin.league_publish_generations"
    ).fetchall() == [("new_league", 7)]
    assert conn.execute(
        "SELECT db_name, bundle_id FROM merge_admin.league_delta_merge_state"
    ).fetchall() == [("new_league", "bundle-1")]


def _ops_connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA accounts")
    conn.execute("CREATE SCHEMA IF NOT EXISTS main")
    conn.execute(
        """
        CREATE TABLE accounts.league_inventory(
            database_name VARCHAR,
            league_db VARCHAR,
            platform VARCHAR,
            league_id VARCHAR,
            league_name VARCHAR,
            tier VARCHAR,
            entitled_mode VARCHAR,
            has_credentials BOOLEAN,
            updated_at TIMESTAMP
        )
        """
    )
    conn.execute(
        "CREATE TABLE main.league_credentials(league_id VARCHAR, league_name VARCHAR, database_name VARCHAR, encrypted_refresh_token VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE main.sleeper_leagues(sleeper_league_id VARCHAR, league_name VARCHAR, database_name VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE main.espn_leagues(espn_league_id BIGINT, league_name VARCHAR, database_name VARCHAR, encrypted_espn_s2 VARCHAR, encrypted_swid VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE main.import_jobs(job_id VARCHAR, database_name VARCHAR, status VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE accounts.league_update_manifests(database_name VARCHAR, published_manifest_digest VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE accounts.league_update_dispatches(database_name VARCHAR, status VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE accounts.pending_paid_imports(database_name VARCHAR, league_name VARCHAR, import_payload_json VARCHAR)"
    )
    return conn


def test_control_plane_retarget_preserves_credentials_and_paid_entitlement() -> None:
    conn = _ops_connection()
    conn.execute(
        "INSERT INTO accounts.league_inventory VALUES "
        "('agustafantasyleague', 'agusta_fantasy_league', 'multi-platform', 'current', 'Agusta Fantasy League', 'paid', 'full', true, '2026-09-16'), "
        "('agusta_fantasy_league', 'agusta_fantasy_league', 'multi-platform', 'current', 'Agusta Fantasy League', 'free', 'quick', false, '2026-09-17')"
    )
    conn.execute(
        "INSERT INTO main.league_credentials VALUES ('yahoo-current', 'Agusta Fantasy League', 'agustafantasyleague', 'ciphertext')"
    )
    conn.execute(
        "INSERT INTO main.sleeper_leagues VALUES ('sleeper-current', 'Agusta Fantasy League', 'agusta_fantasy_league')"
    )
    conn.execute("INSERT INTO main.import_jobs VALUES ('job-1', 'agustafantasyleague', 'complete')")
    conn.execute(
        "INSERT INTO accounts.league_update_manifests VALUES "
        "('agustafantasyleague', 'old-digest'), "
        "('agusta_fantasy_league', 'current-digest')"
    )
    conn.execute(
        "INSERT INTO accounts.league_update_dispatches VALUES "
        "('agustafantasyleague', 'committed'), "
        "('agusta_fantasy_league', 'running')"
    )
    conn.execute(
        "INSERT INTO accounts.pending_paid_imports VALUES "
        "('agustafantasyleague', 'Old Name', '{\"db_name\":\"agustafantasyleague\"}')"
    )

    result = retarget_league_control_plane(
        conn,
        source_db="agustafantasyleague",
        target_db="agusta_fantasy_league",
        display_name="Agusta Fantasy League",
        operation_id="rename-agusta",
    )

    assert conn.execute(
        "SELECT database_name, league_db, tier, entitled_mode, has_credentials FROM accounts.league_inventory"
    ).fetchall() == [("agusta_fantasy_league", "agusta_fantasy_league", "paid", "full", True)]
    assert conn.execute(
        "SELECT database_name, encrypted_refresh_token FROM main.league_credentials"
    ).fetchall() == [("agusta_fantasy_league", "ciphertext")]
    assert conn.execute("SELECT database_name FROM main.sleeper_leagues").fetchall() == [
        ("agusta_fantasy_league",)
    ]
    assert conn.execute("SELECT database_name FROM main.import_jobs").fetchall() == [
        ("agusta_fantasy_league",)
    ]
    assert conn.execute(
        "SELECT database_name, published_manifest_digest FROM accounts.league_update_manifests"
    ).fetchall() == [("agusta_fantasy_league", "current-digest")]
    assert conn.execute(
        "SELECT database_name, status FROM accounts.league_update_dispatches"
    ).fetchall() == [("agusta_fantasy_league", "running")]
    assert conn.execute(
        "SELECT database_name, league_name, import_payload_json FROM accounts.pending_paid_imports"
    ).fetchall() == [
        (
            "agusta_fantasy_league",
            "Agusta Fantasy League",
            '{"db_name":"agusta_fantasy_league"}',
        )
    ]
    assert conn.execute(
        "SELECT old_db_name, new_db_name FROM accounts.league_url_aliases"
    ).fetchall() == [("agustafantasyleague", "agusta_fantasy_league")]
    assert result["status"] == "COMMITTED"
    assert "ciphertext" not in str(result)


def test_control_plane_retarget_rejects_an_unrelated_existing_target() -> None:
    conn = _ops_connection()
    conn.execute(
        "INSERT INTO accounts.league_inventory VALUES "
        "('source_league', 'source_league', 'sleeper', 'source-id', 'Source', 'paid', 'full', true, now()), "
        "('target_league', 'target_league', 'sleeper', 'other-id', 'Target', 'paid', 'full', true, now())"
    )

    try:
        retarget_league_control_plane(
            conn,
            source_db="source_league",
            target_db="target_league",
            display_name="Renamed League",
            operation_id="rename-conflict",
        )
    except ValueError as exc:
        assert "unrelated league" in str(exc)
    else:  # pragma: no cover - makes the failure message explicit
        raise AssertionError("expected unrelated target collision")

    assert conn.execute(
        "SELECT database_name FROM accounts.league_inventory ORDER BY database_name"
    ).fetchall() == [("source_league",), ("target_league",)]


def test_control_plane_preflight_detects_collision_without_mutation() -> None:
    conn = _ops_connection()
    conn.execute(
        "INSERT INTO accounts.league_inventory VALUES "
        "('source_league', 'source_league', 'sleeper', 'source-id', 'Source', 'paid', 'full', true, now()), "
        "('target_league', 'target_league', 'sleeper', 'other-id', 'Target', 'paid', 'full', true, now())"
    )

    with __import__("pytest").raises(ValueError, match="unrelated league"):
        validate_control_plane_rename(
            conn,
            source_db="source_league",
            target_db="target_league",
        )

    assert conn.execute("SELECT COUNT(*) FROM accounts.league_inventory").fetchone()[0] == 2
