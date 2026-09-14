from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import build_batch_sql, run_batch
from multi_league.validation_v2.models import Manifest


def test_no_playoff_league_settings_do_not_trip_playoff_range_checks():
    from multi_league.validation_v2.checks.league_settings import CHECKS

    checks = [
        next(c for c in CHECKS if c.name == "settings_playoff_start_week"),
        next(c for c in CHECKS if c.name == "settings_playoff_teams_range"),
    ]

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings AS
        SELECT * FROM (VALUES
            ('no_playoffs', 2025, 0, 18, 19),
            ('bad_start', 2025, 6, 18, 19),
            ('bad_teams', 2025, 1, 18, 15)
        ) AS t(db_name, year, playoff_teams, end_week, playoff_start_week)
        """
    )

    manifest = Manifest(all_leagues=["no_playoffs", "bad_start", "bad_teams"])
    sql = build_batch_sql(checks, "league_settings", manifest, None, {}, table_prefix="public.")
    results = run_batch(conn, sql, checks, manifest, {})
    result_map = {(r.db_name, r.check.name): r for r in results}

    assert result_map[("no_playoffs", "settings_playoff_start_week")].passed is True
    assert result_map[("no_playoffs", "settings_playoff_teams_range")].passed is True
    assert result_map[("bad_start", "settings_playoff_start_week")].passed is False
    assert result_map[("bad_teams", "settings_playoff_teams_range")].passed is False

    conn.close()
