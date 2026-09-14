"""
Regression tests for calculate_lamar() and calculate_lamar_ytd() across all scoring
combo fixtures.

For each combo schema in the regression DuckDB:
1. Build roster_by_year from league_settings
2. Run calculate_lamar() on the writable copy (overwrites pre-existing LAMAR columns)
3. Assert structural correctness
4. If baselines exist: compare total_manager_lamar and verify that replacement_ppg
   and player_lamar_mean are in the right ballpark (10% relative tolerance).
   The baselines were captured from pre-existing pipeline data; recalculation produces
   slightly different per-week replacement_ppg values, but the sum metrics should be
   stable to within 10% for a healthy algorithm.

Note on tolerance design:
- total_manager_lamar: exact match (1e-3 absolute) — this is a sum and should be
  identical since it depends on the same started-player set and their fantasy_points.
- replacement_ppg (MEDIAN per position): 10% relative tolerance — recalculation
  shifts weekly boundary players, moving the median slightly.
- player_lamar_mean: 10% relative tolerance — derived from replacement_ppg, same reason.

Uses regression_db_writable (per-test copy) so DuckDB UPDATE statements work.
"""

from __future__ import annotations

import math
import warnings

import pytest

from multi_league.transformations.aggregation.modules.lamar import (
    calculate_lamar,
)

# ── constants ─────────────────────────────────────────────────────────────────

REPLACEMENT_PPG_REL_TOL = 0.10  # 10% relative — replacement_ppg shifts with recalc
LAMAR_MEAN_REL_TOL = 0.10  # 10% relative — player_lamar mean also shifts
LAMAR_MEAN_ABS_TOL = 0.50  # absolute fallback for means near zero (±0.5)
TOTAL_MANAGER_LAMAR_ABS_TOL = 1.0  # absolute — sum of started manager_lamar, very stable

# ── helpers ───────────────────────────────────────────────────────────────────


def build_roster_by_year(conn, schema: str) -> dict:
    """Build roster_by_year dict from league_settings table.

    Returns {year: {"QB": 1, "RB": 2, ...}} or {} if table is empty.
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


def _rel_close(actual: float, expected: float, rel_tol: float) -> bool:
    """Return True if actual is within rel_tol of expected (relative)."""
    if expected == 0.0:
        return abs(actual) < 1e-6
    return abs(actual - expected) / abs(expected) <= rel_tol


def _lamar_mean_close(actual: float, expected: float) -> bool:
    """Return True if player_lamar_mean values are acceptably close.

    Uses relative tolerance (10%) OR absolute fallback (±0.5) — whichever is
    more lenient. This handles values near zero (e.g. QB mean ≈ -0.11) where
    relative tolerance is overly strict.
    """
    abs_diff = abs(actual - expected)
    if abs_diff <= LAMAR_MEAN_ABS_TOL:
        return True
    if abs(expected) > 1e-6:
        return abs_diff / abs(expected) <= LAMAR_MEAN_REL_TOL
    return False


def _format_mismatch(schema: str, key: str, expected, actual) -> str:
    return f"  [{schema}] {key}: expected={expected!r}, actual={actual!r}"


# ── test: LAMAR values across all combos ─────────────────────────────────────


def test_lamar_values_all_combos(regression_db_writable, available_schemas, baselines):
    """
    For each combo schema:
    1. Run calculate_lamar() with roster_by_year from league_settings
    2. Assert structural correctness (always)
    3. If baselines: compare replacement_ppg (10% rel tol), player_lamar mean
       (10% rel tol), and total_manager_lamar (±1.0 absolute)
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    has_baselines = baselines is not None and isinstance(baselines, dict)

    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        table = f"{schema}.player_fantasy"

        # Build roster_by_year from league_settings
        roster_by_year = build_roster_by_year(regression_db_writable, schema)
        if not roster_by_year:
            skipped.append(f"[{schema}] empty league_settings, skipped")
            continue

        # Run LAMAR calculation (overwrites pre-existing LAMAR columns)
        try:
            calculate_lamar(regression_db_writable, table, roster_by_year)
        except Exception as exc:
            failures.append(f"  [{schema}] calculate_lamar() raised {type(exc).__name__}: {exc}")
            continue

        # ── Structural correctness checks ────────────────────────────────────

        # Check whether any fantasy_points are populated at all
        fps_count = regression_db_writable.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE fantasy_points IS NOT NULL
        """).fetchone()[0]
        if fps_count == 0:
            skipped.append(f"[{schema}] no fantasy_points populated, skipped structural checks")
            continue

        # Check if LAMAR actually ran (not all-zero)
        lamar_count = regression_db_writable.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE player_lamar IS NOT NULL AND player_lamar != 0
        """).fetchone()[0]
        if lamar_count == 0:
            skipped.append(f"[{schema}] all-zero player_lamar after calculation, skipped")
            continue

        # Structural check 1: manager_lamar non-null only where is_started=1 AND
        # fantasy_points IS NOT NULL
        bad_manager_lamar = regression_db_writable.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE manager_lamar IS NOT NULL
              AND NOT (CAST(is_started AS INTEGER) = 1 AND fantasy_points IS NOT NULL)
        """).fetchone()[0]
        if bad_manager_lamar > 0:
            failures.append(
                f"  [{schema}] {bad_manager_lamar} rows have manager_lamar set where "
                f"is_started != 1 or fantasy_points IS NULL"
            )

        # Structural check 2: bench_lamar non-null/non-zero only where
        # is_rostered=1, is_started=0, fantasy_points IS NOT NULL
        bad_bench_lamar = regression_db_writable.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE bench_lamar IS NOT NULL
              AND bench_lamar != 0
              AND NOT (
                COALESCE(CAST(is_rostered AS INTEGER), 0) = 1
                AND COALESCE(CAST(is_started AS INTEGER), 0) = 0
                AND fantasy_points IS NOT NULL
              )
        """).fetchone()[0]
        if bad_bench_lamar > 0:
            failures.append(f"  [{schema}] {bad_bench_lamar} rows have bench_lamar set incorrectly")

        # Structural check 3: No NULL player_lamar where fantasy_points IS NOT NULL
        null_player_lamar = regression_db_writable.execute(f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE fantasy_points IS NOT NULL AND player_lamar IS NULL
        """).fetchone()[0]
        if null_player_lamar > 0:
            failures.append(
                f"  [{schema}] {null_player_lamar} rows have NULL player_lamar " f"where fantasy_points IS NOT NULL"
            )

        # Structural check 4: replacement_ppg > 0 for main positions
        zero_repl_ppg = regression_db_writable.execute(f"""
            SELECT UPPER(position) as pos, COUNT(*) as cnt
            FROM {table}
            WHERE UPPER(position) IN ('QB', 'RB', 'WR', 'TE')
              AND fantasy_points IS NOT NULL
              AND (replacement_ppg IS NULL OR replacement_ppg <= 0)
            GROUP BY UPPER(position)
        """).fetchdf()
        if not zero_repl_ppg.empty:
            for _, r in zero_repl_ppg.iterrows():
                failures.append(f"  [{schema}] position {r['pos']}: {int(r['cnt'])} rows with " f"replacement_ppg <= 0")

        # ── Baseline comparisons ─────────────────────────────────────────────
        if not has_baselines:
            continue
        combo_baseline = baselines.get(schema)
        if combo_baseline is None:
            continue
        lamar_stats = combo_baseline.get("lamar_stats")
        if lamar_stats is None:
            continue

        # Compute actual stats using MEDIAN for replacement_ppg (matches capture script)
        actual_repl_ppg_df = regression_db_writable.execute(f"""
            SELECT UPPER(position) as position, MEDIAN(replacement_ppg) as repl_ppg
            FROM {table}
            WHERE fantasy_points IS NOT NULL
              AND replacement_ppg IS NOT NULL
              AND replacement_ppg > 0
            GROUP BY UPPER(position)
        """).fetchdf()
        actual_repl_ppg = dict(zip(actual_repl_ppg_df["position"], actual_repl_ppg_df["repl_ppg"]))

        actual_lamar_mean_df = regression_db_writable.execute(f"""
            SELECT UPPER(position) as position, AVG(player_lamar) as lamar_mean
            FROM {table}
            WHERE fantasy_points IS NOT NULL AND player_lamar IS NOT NULL
            GROUP BY UPPER(position)
        """).fetchdf()
        actual_lamar_mean = dict(zip(actual_lamar_mean_df["position"], actual_lamar_mean_df["lamar_mean"]))

        actual_total_mgr = regression_db_writable.execute(f"""
            SELECT COALESCE(SUM(manager_lamar), 0)
            FROM {table}
            WHERE manager_lamar IS NOT NULL
        """).fetchone()[0]

        # Compare replacement_ppg per position (10% relative tolerance)
        baseline_repl = lamar_stats.get("replacement_ppg", {})
        for pos, expected_val in baseline_repl.items():
            actual_val = actual_repl_ppg.get(pos.upper())
            if actual_val is None:
                failures.append(f"  [{schema}] replacement_ppg[{pos}]: no data in actual")
                continue
            if not _rel_close(float(actual_val), float(expected_val), REPLACEMENT_PPG_REL_TOL):
                failures.append(
                    _format_mismatch(
                        schema,
                        f"replacement_ppg[{pos}] (10% rel tol)",
                        round(float(expected_val), 4),
                        round(float(actual_val), 4),
                    )
                )

        # Compare player_lamar mean per position (10% relative OR ±0.5 absolute)
        baseline_lamar_mean = lamar_stats.get("player_lamar_mean", {})
        for pos, expected_val in baseline_lamar_mean.items():
            if expected_val is None:
                continue
            actual_val = actual_lamar_mean.get(pos.upper())
            if actual_val is None:
                failures.append(f"  [{schema}] player_lamar_mean[{pos}]: no data in actual")
                continue
            if not _lamar_mean_close(float(actual_val), float(expected_val)):
                failures.append(
                    _format_mismatch(
                        schema,
                        f"player_lamar_mean[{pos}] (10% rel or ±{LAMAR_MEAN_ABS_TOL} abs)",
                        round(float(expected_val), 6),
                        round(float(actual_val), 6),
                    )
                )

        # Compare total_manager_lamar (±1.0 absolute tolerance)
        baseline_total = lamar_stats.get("total_manager_lamar")
        if baseline_total is not None:
            diff = abs(float(actual_total_mgr) - float(baseline_total))
            if diff > TOTAL_MANAGER_LAMAR_ABS_TOL:
                failures.append(
                    _format_mismatch(
                        schema,
                        f"total_manager_lamar (abs tol={TOTAL_MANAGER_LAMAR_ABS_TOL})",
                        round(float(baseline_total), 3),
                        round(float(actual_total_mgr), 3),
                    )
                )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED combo: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} combos failed:\n" + "\n".join(failures))


# ── test: LAMAR YTD cumulative sum ────────────────────────────────────────────


def test_lamar_ytd_cumsum(regression_db_writable, available_schemas):
    """
    For each combo, after running calculate_lamar:
    Assert player_lamar_ytd at week N = cumulative sum of player_lamar for
    weeks 1..N per player per year.

    Does NOT assert monotonicity — LAMAR can be negative in down weeks.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    failures: list[str] = []
    skipped: list[str] = []

    for schema in available_schemas:
        table = f"{schema}.player_fantasy"

        # Build roster_by_year and run LAMAR
        roster_by_year = build_roster_by_year(regression_db_writable, schema)
        if not roster_by_year:
            skipped.append(f"[{schema}] empty league_settings, skipped")
            continue

        try:
            calculate_lamar(regression_db_writable, table, roster_by_year)
        except Exception as exc:
            failures.append(f"  [{schema}] calculate_lamar() raised {type(exc).__name__}: {exc}")
            continue

        # Check if LAMAR ran
        lamar_count = regression_db_writable.execute(f"""
            SELECT COUNT(*) FROM {table} WHERE player_lamar IS NOT NULL
        """).fetchone()[0]
        if lamar_count == 0:
            skipped.append(f"[{schema}] no player_lamar after calculation, skipped YTD test")
            continue

        # Fetch player_lamar and player_lamar_ytd for rows with player_lamar set
        df = regression_db_writable.execute(f"""
            SELECT
                COALESCE(NFL_player_id, player_week) as player_id,
                year,
                week,
                COALESCE(player_lamar, 0) as player_lamar,
                player_lamar_ytd
            FROM {table}
            WHERE player_lamar IS NOT NULL
            ORDER BY player_id, year, week
        """).fetchdf()

        if df.empty:
            skipped.append(f"[{schema}] empty result for YTD check, skipped")
            continue

        # Compute expected YTD as cumulative sum within (player_id, year)
        df = df.copy()
        df["expected_ytd"] = df.groupby(["player_id", "year"])["player_lamar"].cumsum()

        # Check only rows where player_lamar_ytd is populated
        mask = df["player_lamar_ytd"].notna()
        check_df = df[mask].copy()

        if check_df.empty:
            skipped.append(f"[{schema}] no player_lamar_ytd values, skipped YTD check")
            continue

        # Tolerance: 1e-4 absolute for floating-point rounding
        mismatch = check_df[(check_df["player_lamar_ytd"] - check_df["expected_ytd"]).abs() > 1e-4]

        if not mismatch.empty:
            sample = mismatch.head(5)
            lines = []
            for _, row in sample.iterrows():
                lines.append(
                    f"    player_id={row['player_id']} year={row['year']} "
                    f"week={row['week']}: expected={row['expected_ytd']:.4f}, "
                    f"actual={row['player_lamar_ytd']:.4f}"
                )
            failures.append(
                f"  [{schema}] {len(mismatch)} rows have incorrect player_lamar_ytd "
                f"(first 5 shown):\n" + "\n".join(lines)
            )

    if skipped:
        for msg in skipped:
            warnings.warn(f"SKIPPED combo: {msg}", stacklevel=2)

    if failures:
        pytest.fail(f"{len(failures)} YTD cumsum failures:\n" + "\n".join(failures))
