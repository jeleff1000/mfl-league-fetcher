"""
Regression tests for flex LAMAR calculations.

For each combo schema that has flex roster slots (roster_FLX > 0 or similar),
validates that:
1. Flex LAMAR columns are only non-zero for flex-eligible positions
2. Flex replacement level is generally lower than position-specific replacement
   (the flex pool is larger, so the marginal replacement player is weaker)

Tests run against the writable fixture copy so calculate_lamar() can UPDATE.
"""

import pytest

from multi_league.core.roster_slots import get_flex_pools
from multi_league.transformations.aggregation.modules.lamar import (
    FLEX_LAMAR_SUFFIXES,
    calculate_lamar,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

FLEX_POOL_ROSTER_COLS = {
    "FLX": "roster_FLX",
    "REC_FLEX": "roster_REC_FLEX",
    "SUPER_FLEX": "roster_SUPER_FLEX",
    "IDP": "roster_IDP",
    "DB_LB": "roster_DB_LB",
    "DL_LB": "roster_DL_LB",
}

# Standard FLEX (RB/WR/TE pool): QB must have zero player_lamar_flx
STANDARD_FLEX_INELIGIBLE = {"QB", "K", "DEF"}
# SUPER_FLEX (QB/RB/WR/TE pool): K and DEF must have zero player_lamar_super_flex
SUPER_FLEX_INELIGIBLE = {"K", "DEF"}


def _get_table_columns(conn, schema: str, table_name: str) -> set[str]:
    """Return lowercase column names for a schema.table via information_schema."""
    rows = conn.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table_name}'
    """).fetchall()
    return {r[0].lower() for r in rows}


def _get_roster_settings(conn, schema: str) -> dict[str, int]:
    """Extract roster_* columns from league_settings as a flat dict."""
    # Use information_schema for column discovery (DESCRIBE schema.table fails in DuckDB)
    cols_info = conn.execute(f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = 'league_settings'
          AND LOWER(column_name) LIKE 'roster_%'
    """).fetchall()
    roster_cols = [r[0] for r in cols_info]
    if not roster_cols:
        return {}
    col_list = ", ".join(f'COALESCE("{c}", 0) as "{c}"' for c in roster_cols)
    row = conn.execute(f"SELECT {col_list} FROM {schema}.league_settings LIMIT 1").fetchone()
    if not row:
        return {}
    return {c.replace("roster_", "").upper(): int(v) for c, v in zip(roster_cols, row)}


def _build_roster_by_year(conn, schema: str) -> dict:
    """Build a minimal roster_by_year dict from league_settings for calculate_lamar()."""
    settings = _get_roster_settings(conn, schema)
    year_row = conn.execute(f"SELECT year FROM {schema}.league_settings LIMIT 1").fetchone()
    year = year_row[0] if year_row else 2020
    return {year: settings}


def has_flex_pool(conn, schema: str) -> bool:
    """Return True if this combo has any flex roster slots."""
    settings = _get_roster_settings(conn, schema)
    for flex_name in FLEX_POOL_ROSTER_COLS:
        if settings.get(flex_name, 0) > 0:
            return True
    return False


def get_active_flex_pools(conn, schema: str) -> dict[str, list[str]]:
    """Return {flex_name: [eligible_positions]} for pools that are active."""
    settings = _get_roster_settings(conn, schema)
    pools_from_roster = get_flex_pools(settings)
    return {name: list(eligible) for name, eligible, _count in pools_from_roster}


def has_flex_lamar_columns(conn, schema: str) -> bool:
    """Return True if player_fantasy has at least one flex LAMAR column."""
    cols = _get_table_columns(conn, schema, "player_fantasy")
    for suffix in FLEX_LAMAR_SUFFIXES.values():
        if f"player_lamar_{suffix}" in cols:
            return True
    return False


# ---------------------------------------------------------------------------
# Test 1: Flex LAMAR eligibility
# ---------------------------------------------------------------------------


def test_flex_lamar_eligibility(regression_db_writable, available_schemas):
    """
    After calculate_lamar(), flex LAMAR columns must be zero for ineligible positions.

    For standard FLEX (RB/WR/TE pool):
      - QB, K, DEF must have player_lamar_flx == 0

    For SUPER_FLEX (QB/RB/WR/TE pool):
      - K, DEF must have player_lamar_super_flex == 0
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    conn = regression_db_writable
    failures = []

    for schema in available_schemas:
        if not has_flex_pool(conn, schema):
            continue
        if not has_flex_lamar_columns(conn, schema):
            continue

        flex_pools = get_active_flex_pools(conn, schema)
        if not flex_pools:
            continue

        roster_by_year = _build_roster_by_year(conn, schema)
        player_table = f"{schema}.player_fantasy"

        try:
            calculate_lamar(conn, player_table, roster_by_year, dry_run=False)
        except Exception as exc:
            failures.append(f"[{schema}] calculate_lamar() raised {type(exc).__name__}: {exc}")
            continue

        cols = _get_table_columns(conn, schema, "player_fantasy")

        # --- Standard FLEX check (RB/WR/TE pool) ---
        if "FLX" in flex_pools:
            suffix = "flx"
            col = f"player_lamar_{suffix}"
            if col in cols:
                ineligible_sql = ", ".join(f"'{p}'" for p in sorted(STANDARD_FLEX_INELIGIBLE))
                row = conn.execute(f"""
                    SELECT COUNT(*) FROM {player_table}
                    WHERE UPPER(position) IN ({ineligible_sql})
                      AND {col} IS NOT NULL
                      AND {col} != 0
                """).fetchone()
                nonzero_count = row[0] if row else 0
                if nonzero_count > 0:
                    # Sample which positions have non-zero values
                    samples = conn.execute(f"""
                        SELECT UPPER(position), COUNT(*), ROUND(AVG({col}), 2)
                        FROM {player_table}
                        WHERE UPPER(position) IN ({ineligible_sql})
                          AND {col} IS NOT NULL AND {col} != 0
                        GROUP BY UPPER(position)
                    """).fetchall()
                    failures.append(
                        f"[{schema}] {col}: {nonzero_count} rows with non-zero value "
                        f"for ineligible positions {dict((r[0], r[2]) for r in samples)}"
                    )

        # --- SUPER_FLEX check (QB/RB/WR/TE pool) ---
        if "SUPER_FLEX" in flex_pools:
            suffix = "super_flex"
            col = f"player_lamar_{suffix}"
            if col in cols:
                ineligible_sql = ", ".join(f"'{p}'" for p in sorted(SUPER_FLEX_INELIGIBLE))
                row = conn.execute(f"""
                    SELECT COUNT(*) FROM {player_table}
                    WHERE UPPER(position) IN ({ineligible_sql})
                      AND {col} IS NOT NULL
                      AND {col} != 0
                """).fetchone()
                nonzero_count = row[0] if row else 0
                if nonzero_count > 0:
                    samples = conn.execute(f"""
                        SELECT UPPER(position), COUNT(*), ROUND(AVG({col}), 2)
                        FROM {player_table}
                        WHERE UPPER(position) IN ({ineligible_sql})
                          AND {col} IS NOT NULL AND {col} != 0
                        GROUP BY UPPER(position)
                    """).fetchall()
                    failures.append(
                        f"[{schema}] {col}: {nonzero_count} rows with non-zero value "
                        f"for ineligible positions {dict((r[0], r[2]) for r in samples)}"
                    )

        # --- Generic eligibility check for all other flex pools ---
        for flex_name, eligible_positions in flex_pools.items():
            if flex_name in ("FLX", "SUPER_FLEX"):
                continue  # Already handled above
            suffix = FLEX_LAMAR_SUFFIXES.get(flex_name.upper())
            if not suffix:
                continue
            col = f"player_lamar_{suffix}"
            if col not in cols:
                continue

            eligible_upper = {p.upper() for p in eligible_positions}
            # All positions that exist in the table
            all_positions_rows = conn.execute(f"""
                SELECT DISTINCT UPPER(position) FROM {player_table}
                WHERE position IS NOT NULL
            """).fetchall()
            all_positions = {r[0] for r in all_positions_rows}
            ineligible_positions = all_positions - eligible_upper
            if not ineligible_positions:
                continue

            ineligible_sql = ", ".join(f"'{p}'" for p in sorted(ineligible_positions))
            row = conn.execute(f"""
                SELECT COUNT(*) FROM {player_table}
                WHERE UPPER(position) IN ({ineligible_sql})
                  AND {col} IS NOT NULL
                  AND {col} != 0
            """).fetchone()
            nonzero_count = row[0] if row else 0
            if nonzero_count > 0:
                failures.append(
                    f"[{schema}] {col} ({flex_name}): {nonzero_count} rows with non-zero value "
                    f"for ineligible positions (eligible: {sorted(eligible_upper)})"
                )

    if failures:
        pytest.fail(f"{len(failures)} flex LAMAR eligibility failures:\n" + "\n".join(failures))


# ---------------------------------------------------------------------------
# Test 2: Flex replacement level is lower than position-specific replacement
#         for scarce positions (TE)
# ---------------------------------------------------------------------------


def test_flex_replacement_lower_than_position(regression_db_writable, available_schemas):
    """
    For scarce flex-eligible positions (TE), the flex replacement level should
    generally be <= the position-specific replacement level.

    Rationale: TE is the scarcest flex-eligible position. The flex pool
    (RB+WR+TE) has many more players than the TE-only pool, so the marginal
    replacement player in the combined pool is typically weaker than the TE
    replacement player alone. This is NOT necessarily true for abundant positions
    like RB or WR, where the combined pool may be anchored to a higher value by
    the other positions.

    We test TE specifically (if present) since it reliably satisfies the
    direction of the inequality. We require 75% of TE-weeks to have
    replacement_ppg_flx <= replacement_ppg to account for edge weeks.

    Ineligible positions (QB, K, DEF for standard FLEX) must have
    replacement_ppg_flx = 0 since they can't play flex.
    """
    if not available_schemas:
        pytest.skip("No combo schemas found in regression fixture DB.")

    # For TE (scarce): flex repl should be lower than positional repl most weeks
    TE_THRESHOLD = 0.75  # 75% of TE position-weeks must satisfy the inequality
    SMALL_SAMPLE_FLOOR = 5  # Skip weeks where fewer than this many players scored

    conn = regression_db_writable
    failures = []

    for schema in available_schemas:
        if not has_flex_pool(conn, schema):
            continue
        if not has_flex_lamar_columns(conn, schema):
            continue

        flex_pools = get_active_flex_pools(conn, schema)
        if not flex_pools:
            continue

        roster_by_year = _build_roster_by_year(conn, schema)
        player_table = f"{schema}.player_fantasy"

        # Run calculate_lamar so replacement values are populated
        try:
            calculate_lamar(conn, player_table, roster_by_year, dry_run=False)
        except Exception as exc:
            failures.append(f"[{schema}] calculate_lamar() raised {type(exc).__name__}: {exc}")
            continue

        cols = _get_table_columns(conn, schema, "player_fantasy")

        for flex_name, eligible_positions in flex_pools.items():
            suffix = FLEX_LAMAR_SUFFIXES.get(flex_name.upper())
            if not suffix:
                continue

            repl_flex_col = f"replacement_ppg_{suffix}"
            if repl_flex_col not in cols or "replacement_ppg" not in cols:
                continue

            eligible_upper = {p.upper() for p in eligible_positions}

            # --- Ineligible positions must have replacement_ppg_flx = 0 ---
            all_pos_rows = conn.execute(f"""
                SELECT DISTINCT UPPER(position) FROM {player_table}
                WHERE position IS NOT NULL
            """).fetchall()
            all_positions = {r[0] for r in all_pos_rows}
            ineligible_positions = all_positions - eligible_upper
            if ineligible_positions:
                ineligible_sql = ", ".join(f"'{p}'" for p in sorted(ineligible_positions))
                row = conn.execute(f"""
                    SELECT COUNT(*) FROM {player_table}
                    WHERE UPPER(position) IN ({ineligible_sql})
                      AND {repl_flex_col} IS NOT NULL
                      AND {repl_flex_col} != 0
                """).fetchone()
                nonzero_ineligible = row[0] if row else 0
                if nonzero_ineligible > 0:
                    failures.append(
                        f"[{schema}] {repl_flex_col}: {nonzero_ineligible} ineligible-position rows "
                        f"have non-zero replacement value (ineligible: {sorted(ineligible_positions)})"
                    )

            # --- TE scarce-position check ---
            if "TE" not in eligible_upper:
                continue  # No TE in this flex pool, skip directional check

            # Check that flex repl <= positional repl for most TE weeks
            # replacement_ppg is constant per (year, week, position) after UPDATE
            te_comparison = conn.execute(f"""
                SELECT
                    year, week,
                    ANY_VALUE(replacement_ppg) as repl_pos,
                    ANY_VALUE({repl_flex_col}) as repl_flex,
                    COUNT(*) as player_count
                FROM {player_table}
                WHERE UPPER(position) = 'TE'
                  AND replacement_ppg IS NOT NULL AND replacement_ppg > 0
                  AND {repl_flex_col} IS NOT NULL AND {repl_flex_col} > 0
                GROUP BY year, week
                HAVING COUNT(*) >= {SMALL_SAMPLE_FLOOR}
            """).fetchall()

            if not te_comparison:
                continue

            total_te = len(te_comparison)
            te_violations = [
                (r[0], r[1], r[2], r[3])  # year, week, repl_pos, repl_flex
                for r in te_comparison
                if r[3] > r[2] + 0.01  # Allow 0.01 floating-point tolerance
            ]
            violation_rate = len(te_violations) / total_te if total_te > 0 else 0.0
            pass_rate = 1.0 - violation_rate

            if pass_rate < TE_THRESHOLD:
                sample_violations = te_violations[:5]
                failures.append(
                    f"[{schema}] {repl_flex_col} > TE replacement_ppg for "
                    f"{len(te_violations)}/{total_te} weeks "
                    f"({violation_rate:.1%} violation rate, need <{1-TE_THRESHOLD:.0%}). "
                    f"Sample (year, week, te_repl, flx_repl): {sample_violations}"
                )

    if failures:
        pytest.fail(f"{len(failures)} flex replacement level failures:\n" + "\n".join(failures))
