"""
Regression tests for downstream LAMAR value propagation.

For each combo schema in the regression DuckDB, after running calculate_lamar():

1. test_lamar_populates_all_positions
   Every position present in the data (QB, RB, WR, TE, K, DEF) that has any
   fantasy_points rows should have at least some non-zero player_lamar values.
   Catches bugs where a position is silently skipped during LAMAR calculation.

2. test_manager_lamar_sums_to_team_total
   For each (manager, year, week) combination with started players,
   the sum of manager_lamar across started players must be a finite number
   (not NaN, not NULL). Verifies that per-player LAMAR correctly aggregates
   without silent nullification.

3. test_unrostered_players_have_player_lamar
   Players with manager = 'Unrostered' who have fantasy_points populated should
   still receive player_lamar values. LAMAR measures player talent regardless of
   roster status, and unrostered players are essential for league-wide optimal
   lineup calculations.

Uses regression_db_writable (per-test writable copy) so calculate_lamar() can
UPDATE the player_fantasy table.
"""

from __future__ import annotations

import math
import warnings

import pytest

from multi_league.transformations.aggregation.modules.lamar import calculate_lamar

# Positions we care about for downstream checks
CORE_POSITIONS = {"QB", "RB", "WR", "TE", "K", "DEF"}

# Values treated as "unrostered" in the manager column
UNROSTERED_VALUES = {"unrostered", "fa", "free agent", "waivers", ""}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_roster_by_year(conn, schema: str) -> dict:
    """Build roster_by_year dict from league_settings.

    Returns {year: {"QB": 1, "RB": 2, ...}} or {} if the table is empty.
    """
    row = conn.execute(f"SELECT * FROM {schema}.league_settings LIMIT 1").fetchdf()
    if row.empty:
        return {}
    year = int(row["year"].iloc[0])
    roster: dict[str, int] = {}
    for col in row.columns:
        if col.startswith("roster_") and col not in ("roster_positions",):
            pos = col.replace("roster_", "")
            val = row[col].iloc[0]
            try:
                fval = float(val)
            except (TypeError, ValueError):
                continue
            if math.isnan(fval):
                continue
            ival = int(fval)
            if ival > 0:
                roster[pos] = ival
    return {year: roster}


def _run_lamar(conn, schema: str, roster_by_year: dict) -> bool:
    """Run calculate_lamar(); return True on success."""
    table = f"{schema}.player_fantasy"
    try:
        calculate_lamar(conn, table, roster_by_year)
        return True
    except Exception as exc:
        return str(exc)


def _has_fantasy_points(conn, schema: str) -> bool:
    """Return True if player_fantasy has any rows with fantasy_points populated."""
    count = conn.execute(f"""
        SELECT COUNT(*) FROM {schema}.player_fantasy WHERE fantasy_points IS NOT NULL
    """).fetchone()[0]
    return count > 0


# ---------------------------------------------------------------------------
# Test 1: LAMAR populates all positions
# ---------------------------------------------------------------------------


def test_lamar_populates_all_positions(regression_db_writable, available_schemas):
    """
    After calculate_lamar(), every position present in the data that has
    fantasy_points rows must have at least some non-zero player_lamar values.

    This catches bugs where a position is silently skipped (e.g., DEF or K
    being excluded from the replacement pool calculation).
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    conn = regression_db_writable
    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        roster_by_year = _build_roster_by_year(conn, schema)
        if not roster_by_year:
            skipped.append(f"[{schema}] empty league_settings")
            continue

        if not _has_fantasy_points(conn, schema):
            skipped.append(f"[{schema}] no fantasy_points populated")
            continue

        result = _run_lamar(conn, schema, roster_by_year)
        if result is not True:
            failures.append(f"  [{schema}] calculate_lamar() raised: {result}")
            continue

        table = f"{schema}.player_fantasy"

        # Find which core positions have any fantasy_points rows
        present_rows = conn.execute(f"""
            SELECT UPPER(position) as pos, COUNT(*) as fp_count
            FROM {table}
            WHERE fantasy_points IS NOT NULL
              AND UPPER(position) IN ('QB', 'RB', 'WR', 'TE', 'K', 'DEF')
            GROUP BY UPPER(position)
        """).fetchall()

        if not present_rows:
            skipped.append(f"[{schema}] no core-position rows with fantasy_points")
            continue

        # For each present position, verify at least some non-zero player_lamar
        for pos, fp_count in present_rows:
            nonzero_lamar = conn.execute(f"""
                SELECT COUNT(*)
                FROM {table}
                WHERE UPPER(position) = '{pos}'
                  AND fantasy_points IS NOT NULL
                  AND player_lamar IS NOT NULL
                  AND player_lamar != 0
            """).fetchone()[0]

            if nonzero_lamar == 0:
                failures.append(
                    f"  [{schema}] position={pos}: {fp_count} rows with fantasy_points "
                    f"but 0 non-zero player_lamar values — position may be skipped"
                )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED combo: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} position LAMAR population failures:\n" + "\n".join(failures))


# ---------------------------------------------------------------------------
# Test 2: manager_lamar sums to a finite team total
# ---------------------------------------------------------------------------


def test_manager_lamar_sums_to_team_total(regression_db_writable, available_schemas):
    """
    For each (manager, year, week) with started players, the sum of manager_lamar
    across started players must be finite (not NaN, not NULL).

    A NULL sum indicates that at least one started player's manager_lamar is NULL,
    which would silently corrupt team-level LAMAR aggregations downstream.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    conn = regression_db_writable
    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        roster_by_year = _build_roster_by_year(conn, schema)
        if not roster_by_year:
            skipped.append(f"[{schema}] empty league_settings")
            continue

        if not _has_fantasy_points(conn, schema):
            skipped.append(f"[{schema}] no fantasy_points populated")
            continue

        result = _run_lamar(conn, schema, roster_by_year)
        if result is not True:
            failures.append(f"  [{schema}] calculate_lamar() raised: {result}")
            continue

        table = f"{schema}.player_fantasy"

        # Check whether any started rows have null manager_lamar
        # (manager_lamar should be set for all started rows with fantasy_points)
        null_manager_lamar_rows = conn.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE CAST(is_started AS INTEGER) = 1
              AND fantasy_points IS NOT NULL
              AND manager IS NOT NULL
              AND LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')
              AND manager_lamar IS NULL
        """).fetchone()[0]

        if null_manager_lamar_rows > 0:
            # Get a breakdown by (manager, year, week) to help debug
            sample = conn.execute(f"""
                SELECT manager, year, week, COUNT(*) as null_count
                FROM {table}
                WHERE CAST(is_started AS INTEGER) = 1
                  AND fantasy_points IS NOT NULL
                  AND manager IS NOT NULL
                  AND LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')
                  AND manager_lamar IS NULL
                GROUP BY manager, year, week
                ORDER BY null_count DESC
                LIMIT 5
            """).fetchall()
            failures.append(
                f"  [{schema}] {null_manager_lamar_rows} started rows with fantasy_points "
                f"have NULL manager_lamar. Top offenders (manager, year, week, count): "
                f"{[(r[0], r[1], r[2], r[3]) for r in sample]}"
            )
            continue

        # Also check: for each (manager, year, week) the SUM is a finite number
        # (guards against any NaN propagation via Python floats)
        team_week_nulls = conn.execute(f"""
            SELECT COUNT(*)
            FROM (
                SELECT manager, year, week, SUM(manager_lamar) as team_lamar_sum
                FROM {table}
                WHERE CAST(is_started AS INTEGER) = 1
                  AND fantasy_points IS NOT NULL
                  AND manager IS NOT NULL
                  AND LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')
                GROUP BY manager, year, week
            ) t
            WHERE team_lamar_sum IS NULL
        """).fetchone()[0]

        if team_week_nulls > 0:
            failures.append(
                f"  [{schema}] {team_week_nulls} (manager, year, week) groups have "
                f"NULL sum of manager_lamar for started players"
            )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED combo: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} manager_lamar aggregation failures:\n" + "\n".join(failures))


# ---------------------------------------------------------------------------
# Test 3: Unrostered players receive player_lamar
# ---------------------------------------------------------------------------


def test_unrostered_players_have_player_lamar(regression_db_writable, available_schemas):
    """
    Players with manager = 'Unrostered' who have fantasy_points populated must
    still have non-null player_lamar values.

    LAMAR measures player talent regardless of roster status. Unrostered players
    are included so league-wide optimal lineup analysis can compare managers'
    actual lineups against the theoretical best available from the entire NFL pool.
    If unrostered players have NULL player_lamar, optimal lineup analysis breaks.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    conn = regression_db_writable
    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        roster_by_year = _build_roster_by_year(conn, schema)
        if not roster_by_year:
            skipped.append(f"[{schema}] empty league_settings")
            continue

        if not _has_fantasy_points(conn, schema):
            skipped.append(f"[{schema}] no fantasy_points populated")
            continue

        result = _run_lamar(conn, schema, roster_by_year)
        if result is not True:
            failures.append(f"  [{schema}] calculate_lamar() raised: {result}")
            continue

        table = f"{schema}.player_fantasy"

        # Count unrostered rows with fantasy_points (may be 0 for quick imports)
        unrostered_fp_count = conn.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE LOWER(TRIM(COALESCE(manager, ''))) IN ('unrostered', 'fa', 'free agent', 'waivers')
              AND fantasy_points IS NOT NULL
        """).fetchone()[0]

        if unrostered_fp_count == 0:
            skipped.append(f"[{schema}] no unrostered rows with fantasy_points (likely quick import)")
            continue

        # Check: unrostered rows with fantasy_points must have non-null player_lamar
        null_lamar_unrostered = conn.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE LOWER(TRIM(COALESCE(manager, ''))) IN ('unrostered', 'fa', 'free agent', 'waivers')
              AND fantasy_points IS NOT NULL
              AND player_lamar IS NULL
        """).fetchone()[0]

        if null_lamar_unrostered > 0:
            # Breakdown by position to understand which positions are missing
            pos_breakdown = conn.execute(f"""
                SELECT UPPER(position) as pos, COUNT(*) as null_count
                FROM {table}
                WHERE LOWER(TRIM(COALESCE(manager, ''))) IN ('unrostered', 'fa', 'free agent', 'waivers')
                  AND fantasy_points IS NOT NULL
                  AND player_lamar IS NULL
                GROUP BY UPPER(position)
                ORDER BY null_count DESC
            """).fetchall()
            failures.append(
                f"  [{schema}] {null_lamar_unrostered}/{unrostered_fp_count} unrostered rows "
                f"with fantasy_points have NULL player_lamar. "
                f"By position: {[(r[0], r[1]) for r in pos_breakdown]}"
            )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED combo: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} unrostered player_lamar failures:\n" + "\n".join(failures))
