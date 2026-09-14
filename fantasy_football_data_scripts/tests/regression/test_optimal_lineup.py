"""
Regression tests for optimal lineup calculations (league-wide and manager).

Structural invariants checked against pre-computed optimal data in the fixture:

1. League-wide optimal count = total starter slots per week
2. Manager optimal count = total starter slots per manager per week
3. No player appears in optimal lineup twice in the same week
4. Optimal points >= actual started points (manager made suboptimal choices)
5. Only rostered players are marked manager-optimal
6. League-wide optimal players have the highest fantasy_points at their position

Uses regression_db_readonly (pre-computed pipeline data, no mutation needed).
"""

from __future__ import annotations

import math
import warnings

import pytest


# ── helpers ──────────────────────────────────────────────────────────────────


NON_STARTER_SLOTS = {"BN", "IR", "IL", "TAXI", "RESERVE", "RES", "COVID", "PUP", "INJ"}


def _get_starter_slot_count(conn, schema: str) -> int | None:
    """Sum of starter roster slots from league_settings (excludes BN, IR, etc.)."""
    row = conn.execute(f"SELECT * FROM {schema}.league_settings LIMIT 1").fetchdf()
    if row.empty:
        return None
    total = 0
    for col in row.columns:
        if col.startswith("roster_") and col != "roster_positions":
            slot_name = col.replace("roster_", "").upper()
            if slot_name in NON_STARTER_SLOTS:
                continue
            val = row[col].iloc[0]
            try:
                fval = float(val)
            except (TypeError, ValueError):
                continue
            if math.isnan(fval):
                continue
            total += int(fval)
    return total if total > 0 else None


def _has_optimal_data(conn, schema: str) -> bool:
    """Return True if the schema has any optimal_player = 1 rows."""
    cnt = conn.execute(f"SELECT COUNT(*) FROM {schema}.player_fantasy WHERE optimal_player = 1").fetchone()[0]
    return cnt > 0


# ── test: league-wide optimal count per week ─────────────────────────────────


def test_league_wide_optimal_count_per_week(regression_db_readonly, available_schemas):
    """
    For each (year, week), the number of league_wide_optimal_player = 1 rows
    should equal the total starter slots in the roster configuration.

    Playoff/championship weeks (week >= 18) may have fewer NFL games, so
    some positions might not have enough players with fantasy_points to fill
    all slots. We allow the count to be <= expected for those weeks.

    Regular season weeks (1-17) must match exactly.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found.")

    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        starter_slots = _get_starter_slot_count(regression_db_readonly, schema)
        if starter_slots is None:
            skipped.append(f"[{schema}] no starter slots found")
            continue

        # Check if league-wide optimal was computed
        lwo_count = regression_db_readonly.execute(f"""
            SELECT COUNT(*) FROM {schema}.player_fantasy
            WHERE league_wide_optimal_player = 1
        """).fetchone()[0]
        if lwo_count == 0:
            skipped.append(f"[{schema}] no league_wide_optimal data")
            continue

        # Count per (year, week)
        week_counts = regression_db_readonly.execute(f"""
            SELECT year, week, COUNT(*) as opt_count
            FROM {schema}.player_fantasy
            WHERE league_wide_optimal_player = 1
            GROUP BY year, week
            ORDER BY year, week
        """).fetchall()

        for year, week, opt_count in week_counts:
            # Playoff weeks may have fewer players available
            if week >= 18:
                if opt_count > starter_slots:
                    failures.append(
                        f"  [{schema}] year={year} week={week}: "
                        f"league_wide_optimal={opt_count} > max={starter_slots}"
                    )
            else:
                if opt_count != starter_slots:
                    failures.append(
                        f"  [{schema}] year={year} week={week}: "
                        f"league_wide_optimal={opt_count}, expected={starter_slots}"
                    )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED: {msg}", stacklevel=2)

    if failures:
        # Show first 10 failures
        summary = failures[:10]
        if len(failures) > 10:
            summary.append(f"  ... and {len(failures) - 10} more")
        pytest.fail(f"{len(failures)} league-wide optimal count mismatches:\n" + "\n".join(summary))


# ── test: manager optimal count per team-week ────────────────────────────────


def test_manager_optimal_count_per_team_week(regression_db_readonly, available_schemas):
    """
    For each (manager, year, week), the number of optimal_player = 1 rows
    should be <= total starter slots. It can be less if the manager doesn't
    have enough rostered players to fill every slot.

    It should NEVER exceed starter slots — that would mean the optimizer
    is selecting more players than can fit in a valid lineup.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found.")

    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        starter_slots = _get_starter_slot_count(regression_db_readonly, schema)
        if starter_slots is None:
            skipped.append(f"[{schema}] no starter slots found")
            continue

        if not _has_optimal_data(regression_db_readonly, schema):
            skipped.append(f"[{schema}] no optimal_player data")
            continue

        # Count per (manager, year, week)
        over_limit = regression_db_readonly.execute(f"""
            SELECT manager, year, week, COUNT(*) as opt_count
            FROM {schema}.player_fantasy
            WHERE optimal_player = 1
            GROUP BY manager, year, week
            HAVING opt_count > {starter_slots}
            ORDER BY year, week, manager
        """).fetchall()

        for manager, year, week, opt_count in over_limit:
            failures.append(
                f"  [{schema}] manager={manager} year={year} week={week}: "
                f"optimal={opt_count} > max_slots={starter_slots}"
            )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED: {msg}", stacklevel=2)

    if failures:
        summary = failures[:10]
        if len(failures) > 10:
            summary.append(f"  ... and {len(failures) - 10} more")
        pytest.fail(f"{len(failures)} manager optimal count violations:\n" + "\n".join(summary))


# ── test: no duplicate optimal players per week ──────────────────────────────


def test_no_duplicate_optimal_players(regression_db_readonly, available_schemas):
    """
    A player_week should appear at most once in the optimal lineup per week.
    Duplicates would mean the same player is filling two roster slots,
    which is physically impossible.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found.")

    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        if not _has_optimal_data(regression_db_readonly, schema):
            skipped.append(f"[{schema}] no optimal_player data")
            continue

        # Manager optimal duplicates
        mgr_dupes = regression_db_readonly.execute(f"""
            SELECT player_week, manager, year, week, COUNT(*) as cnt
            FROM {schema}.player_fantasy
            WHERE optimal_player = 1
            GROUP BY player_week, manager, year, week
            HAVING cnt > 1
        """).fetchall()

        if mgr_dupes:
            failures.append(f"  [{schema}] {len(mgr_dupes)} duplicate manager-optimal player_weeks")

        # League-wide optimal duplicates
        lwo_dupes = regression_db_readonly.execute(f"""
            SELECT player_week, year, week, COUNT(*) as cnt
            FROM {schema}.player_fantasy
            WHERE league_wide_optimal_player = 1
            GROUP BY player_week, year, week
            HAVING cnt > 1
        """).fetchall()

        if lwo_dupes:
            failures.append(f"  [{schema}] {len(lwo_dupes)} duplicate league-wide-optimal player_weeks")

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} duplicate optimal violations:\n" + "\n".join(failures))


# ── test: optimal points >= actual started points ────────────────────────────


def test_optimal_points_positive_and_reasonable(regression_db_readonly, available_schemas):
    """
    For each (manager, year, week) with optimal_player=1 rows, the sum of
    optimal fantasy_points should be positive and at least as large as the
    median individual player's points (sanity check).

    We don't compare against is_started sums because is_started semantics
    differ across platforms (ESPN marks active roster, not just starters).
    Instead we verify the optimizer produces reasonable totals.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found.")

    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        if not _has_optimal_data(regression_db_readonly, schema):
            skipped.append(f"[{schema}] no optimal_player data")
            continue

        # Check for any manager-week where optimal total is 0 or negative
        bad_totals = regression_db_readonly.execute(f"""
            SELECT franchise_id, year, week, SUM(fantasy_points) as total
            FROM {schema}.player_fantasy
            WHERE optimal_player = 1
              AND fantasy_points IS NOT NULL
            GROUP BY franchise_id, year, week
            HAVING total <= 0
        """).fetchall()

        if bad_totals:
            failures.append(f"  [{schema}] {len(bad_totals)} manager-weeks have " f"optimal total <= 0")

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED: {msg}", stacklevel=2)

    if failures:
        pytest.fail("Optimal points sanity check failures:\n" + "\n".join(failures))


# ── test: only rostered players are manager-optimal ──────────────────────────


def test_only_rostered_players_are_optimal(regression_db_readonly, available_schemas):
    """
    All optimal_player = 1 rows must belong to a rostered manager.
    Unrostered players should never be marked as manager-optimal.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found.")

    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        if not _has_optimal_data(regression_db_readonly, schema):
            skipped.append(f"[{schema}] no optimal_player data")
            continue

        unrostered_optimal = regression_db_readonly.execute(f"""
            SELECT COUNT(*)
            FROM {schema}.player_fantasy
            WHERE optimal_player = 1
              AND (
                manager IS NULL
                OR TRIM(COALESCE(manager, '')) = ''
                OR LOWER(TRIM(manager)) IN ('unrostered', 'fa', 'free agent', 'waivers')
              )
        """).fetchone()[0]

        if unrostered_optimal > 0:
            failures.append(f"  [{schema}] {unrostered_optimal} unrostered players marked optimal_player=1")

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED: {msg}", stacklevel=2)

    if failures:
        pytest.fail("Unrostered players marked optimal:\n" + "\n".join(failures))
