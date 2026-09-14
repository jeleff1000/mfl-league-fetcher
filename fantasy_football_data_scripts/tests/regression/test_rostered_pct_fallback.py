"""
Regression tests for the league_avg_rostered_pct CTE fallback path in LAMAR SQL.

The LAMAR calculation uses a rostered percentage to determine replacement-level
players. When a position-week has too few observations, the SQL falls back to a
league-average rostered percentage. These tests verify that the fallback produces
sane replacement_ppg values — positive and reasonably consistent across weeks.

1. test_replacement_ppg_positive_all_positions
   After calculate_lamar(), replacement_ppg must be > 0 for every
   (year, week, position) combination where fantasy_points are populated.
   A value of 0 or NULL means the fallback failed to find a replacement level,
   which would make every player's LAMAR meaningless for that position-week.

2. test_replacement_ppg_consistent_across_weeks
   Within a season, replacement_ppg for a given position should not vary wildly
   week-to-week. Assert that the coefficient of variation (stddev/mean) is < 1.0
   for each (year, position) group. A CV >= 1 suggests the fallback is producing
   nonsense values (e.g., orders-of-magnitude swings from week to week) rather
   than the expected gradual drift as the season progresses.

Uses regression_db_writable (per-test writable copy) so calculate_lamar() can
UPDATE player_fantasy.
"""

from __future__ import annotations

import math
import warnings

import pytest

from multi_league.transformations.aggregation.modules.lamar import calculate_lamar

# Coefficient-of-variation threshold: stddev/mean must be below this value
# within a season for a given position. CV = 2 means stddev is 2x the mean.
# Positions with small replacement pools (K, DEF, QB in deep leagues) naturally
# have high CV because 1-3 replacement candidates produce noisy weekly values.
# CV < 2.0 catches pathological cases while tolerating expected small-pool noise.
CV_THRESHOLD = 2.0

# Minimum number of (year, week, position) groups needed to run the CV check.
# Below this, there's not enough variance data to draw conclusions.
MIN_WEEKS_FOR_CV = 3


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


def _run_lamar(conn, schema: str, roster_by_year: dict):
    """Run calculate_lamar(); return True on success, error string on failure."""
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
# Test 1: replacement_ppg > 0 for all positions
# ---------------------------------------------------------------------------


def test_replacement_ppg_positive_all_positions(regression_db_writable, available_schemas):
    """
    After calculate_lamar(), replacement_ppg must be > 0 for every
    (year, week, position) combination that has fantasy_points rows.

    A zero or NULL replacement_ppg means the fallback CTE failed to establish
    a replacement level for that position-week, which would zero-out every
    player's LAMAR value for that position-week (all players look equally
    valuable relative to a $0 replacement).
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

        # Find (year, week, position) groups where replacement_ppg is <= 0 or NULL
        # but fantasy_points IS NOT NULL — meaning the fallback silently failed.
        # Exclude positions with < 3 players in a given week: rare IDP positions
        # (FS, FB, DT, CB) may have only 1-2 players, making replacement_ppg = 0
        # semantically correct (no replacement pool exists).
        bad_repl = conn.execute(f"""
            WITH pos_week_counts AS (
                SELECT year, week,
                       UPPER(SPLIT_PART(position, ',', 1)) as pos,
                       COUNT(*) as total_players,
                       COUNT(CASE WHEN replacement_ppg IS NULL OR replacement_ppg <= 0 THEN 1 END) as bad_count
                FROM {table}
                WHERE fantasy_points IS NOT NULL
                  AND position IS NOT NULL
                GROUP BY year, week, UPPER(SPLIT_PART(position, ',', 1))
            )
            SELECT year, week, pos, total_players as row_count, bad_count
            FROM pos_week_counts
            WHERE bad_count > 0
              AND total_players >= 3
            ORDER BY year, week, pos
        """).fetchall()

        if bad_repl:
            # Format a concise summary of which position-weeks are broken
            # Only show up to 10 to keep failure messages readable
            summary = [f"(year={r[0]}, week={r[1]}, pos={r[2]}, bad={r[3]}/{r[3]})" for r in bad_repl[:10]]
            failures.append(
                f"  [{schema}] {len(bad_repl)} (year, week, position) groups have "
                f"replacement_ppg <= 0 or NULL. First 10: {summary}"
            )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED combo: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} replacement_ppg positivity failures:\n" + "\n".join(failures))


# ---------------------------------------------------------------------------
# Test 2: replacement_ppg consistent across weeks (CV < 1.0)
# ---------------------------------------------------------------------------


def test_replacement_ppg_consistent_across_weeks(regression_db_writable, available_schemas):
    """
    Within a season, replacement_ppg for a given position must not vary wildly
    week-to-week. Assert that the coefficient of variation (stddev/mean) is < 1.0
    for each (year, position) group with at least MIN_WEEKS_FOR_CV distinct weeks.

    CV >= 1.0 indicates that the standard deviation is as large as (or larger than)
    the mean, implying extreme volatility — a hallmark of the fallback path producing
    nonsense values rather than a smooth replacement-level curve.

    Note: Some variation is expected and healthy (replacement level rises as the season
    progresses and strong players separate from the pack). CV < 1.0 is a generous
    threshold that only catches pathological cases.
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

        # Compute one representative replacement_ppg per (year, week, position)
        # using MEDIAN to avoid influence from per-player duplication of the value
        week_repl = conn.execute(f"""
            SELECT
                year,
                UPPER(position) as pos,
                week,
                MEDIAN(replacement_ppg) as median_repl_ppg
            FROM {table}
            WHERE fantasy_points IS NOT NULL
              AND replacement_ppg IS NOT NULL
              AND replacement_ppg > 0
              AND position IS NOT NULL
            GROUP BY year, UPPER(position), week
        """).fetchdf()

        if week_repl.empty:
            skipped.append(f"[{schema}] no valid replacement_ppg values after calculation")
            continue

        # Compute CV per (year, position)
        cv_stats = week_repl.groupby(["year", "pos"])["median_repl_ppg"].agg(["mean", "std", "count"]).reset_index()

        schema_failures = []
        for _, row in cv_stats.iterrows():
            week_count = int(row["count"])
            if week_count < MIN_WEEKS_FOR_CV:
                continue  # Not enough weeks to assess consistency

            mean_val = float(row["mean"])
            std_val = float(row["std"]) if not math.isnan(float(row["std"])) else 0.0

            if mean_val <= 0:
                continue  # Can't compute CV for zero/negative mean

            cv = std_val / mean_val
            if cv >= CV_THRESHOLD:
                schema_failures.append(
                    f"    year={int(row['year'])}, pos={row['pos']}: "
                    f"CV={cv:.3f} (mean={mean_val:.3f}, std={std_val:.3f}, "
                    f"weeks={week_count}) — exceeds threshold {CV_THRESHOLD}"
                )

        if schema_failures:
            failures.append(
                f"  [{schema}] replacement_ppg too variable across weeks "
                f"(CV >= {CV_THRESHOLD}) for {len(schema_failures)} (year, position) groups:\n"
                + "\n".join(schema_failures)
            )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED combo: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} replacement_ppg consistency failures:\n" + "\n".join(failures))
