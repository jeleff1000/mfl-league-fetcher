"""
LAMAR calculation module -- pure functions extracted from AggregationEnrichmentsMixin.

All functions take (conn, player_table, ...) instead of using self.
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Any

from multi_league.core.player_identity import select_platform_player_id_column
from multi_league.core.sql_utils import validate_db_name
from multi_league.core.roster_slots import get_flex_pools
from multi_league.shared.filters import rostered_filter_sql  # noqa: F401 - used in f-strings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FLEX_LAMAR_SUFFIXES: dict[str, str] = {
    "FLX": "flx",
    "REC_FLEX": "rec_flex",
    "SUPER_FLEX": "super_flex",
    "IDP": "idp",
    "DB_LB": "db_lb",
    "DL_LB": "dl_lb",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def flex_lamar_suffix(flex_name: str) -> str | None:
    """Return the canonical LAMAR suffix for a supported flex pool."""
    return FLEX_LAMAR_SUFFIXES.get(str(flex_name).upper())


def _get_table_columns(conn, player_table: str) -> set[str]:
    """Return lowercase column names for *player_table*."""
    return {c[0].lower() for c in conn.execute(f"DESCRIBE {player_table}").fetchall()}


def _db_filter(db_name: str | None, alias: str = "") -> str:
    """Return the centralized db_name filter or a no-op for local mode."""
    if not db_name:
        return "1=1"

    validate_db_name(db_name)
    prefix = f"{alias}." if alias else ""
    return f"{prefix}db_name = '{db_name}'"


# ---------------------------------------------------------------------------
# Flex LAMAR
# ---------------------------------------------------------------------------


def calculate_flex_lamar(
    conn,
    player_table: str,
    flex_pools: dict[str, list[str]],
    col_names: list[str],
    dry_run: bool = False,
    db_name: str | None = None,
) -> int:
    """Calculate flex-pool LAMAR for every supported flex type.

    Parameters
    ----------
    conn : DuckDB connection
    player_table : Fully qualified table name (e.g. ``db.public.player_fantasy``)
    flex_pools : Mapping of flex name -> list of eligible positions.
    col_names : Lowercase column names already present in *player_table*.
    dry_run : If True, log SQL but don't execute.

    Returns
    -------
    Total non-zero flex LAMAR rows across all pools.
    """
    col_set = set(col_names)
    total = 0

    for flex_name, eligible_positions in flex_pools.items():
        suffix = flex_lamar_suffix(flex_name)
        if not suffix:
            logger.info(
                "[calculate_flex_lamar] Skipping %s: no canonical flex LAMAR columns for this pool",
                flex_name,
            )
            continue

        required_cols = {
            f"replacement_ppg_{suffix}",
            f"player_lamar_{suffix}",
            f"manager_lamar_{suffix}",
            f"bench_lamar_{suffix}",
        }
        missing_cols = sorted(required_cols - col_set)
        if missing_cols:
            logger.warning(
                "[calculate_flex_lamar] Skipping %s: missing canonical columns %s",
                flex_name,
                ", ".join(missing_cols),
            )
            continue

        eligible_sql = ", ".join(f"'{p}'" for p in sorted(eligible_positions))

        flex_lamar_sql = f"""
            WITH flex_rostered_pct AS (
                SELECT
                    r.year, r.week,
                    LEAST(0.95, 1.0 * r.rostered_count / NULLIF(t.total_count, 0)) as rostered_pct
                FROM (
                    SELECT year, week, COUNT(*) as rostered_count
                    FROM {player_table}
                    WHERE UPPER(position) IN ({eligible_sql})
                      AND {rostered_filter_sql(col="manager")}
                      AND {_db_filter(db_name)}
                    GROUP BY year, week
                ) r
                JOIN (
                    SELECT year, week, COUNT(*) as total_count
                    FROM {player_table}
                    WHERE UPPER(position) IN ({eligible_sql})
                      AND fantasy_points IS NOT NULL AND fantasy_points > 0
                      AND {_db_filter(db_name)}
                    GROUP BY year, week
                ) t ON r.year = t.year AND r.week = t.week
            ),
            flex_avg_pct AS (
                SELECT AVG(rostered_pct) as avg_rostered_pct
                FROM flex_rostered_pct
            ),
            flex_total AS (
                SELECT year, week, COUNT(*) as total_count
                FROM {player_table}
                WHERE UPPER(position) IN ({eligible_sql})
                  AND fantasy_points IS NOT NULL AND fantasy_points > 0
                  AND {_db_filter(db_name)}
                GROUP BY year, week
            ),
            flex_ranked AS (
                SELECT year, week, player_week, fantasy_points,
                       ROW_NUMBER() OVER (
                           PARTITION BY year, week
                           ORDER BY fantasy_points DESC NULLS LAST
                       ) as pool_rank
                FROM {player_table}
                WHERE UPPER(position) IN ({eligible_sql})
                  AND fantasy_points IS NOT NULL AND fantasy_points > 0
                  AND {_db_filter(db_name)}
            ),
            flex_replacement AS (
                SELECT r.year, r.week,
                       AVG(r.fantasy_points) as flex_repl_ppg
                FROM flex_ranked r
                JOIN flex_total ft ON r.year = ft.year AND r.week = ft.week
                LEFT JOIN flex_rostered_pct wp ON r.year = wp.year AND r.week = wp.week
                CROSS JOIN flex_avg_pct fa
                WHERE r.pool_rank BETWEEN
                    CEILING(ft.total_count * COALESCE(wp.rostered_pct, fa.avg_rostered_pct, 0.5))
                    AND CEILING(ft.total_count * COALESCE(wp.rostered_pct, fa.avg_rostered_pct, 0.5)) + 1
                GROUP BY r.year, r.week
            )
            UPDATE {player_table} p
            SET
                replacement_ppg_{suffix} = CASE
                    WHEN UPPER(p.position) IN ({eligible_sql})
                    THEN COALESCE(fr.flex_repl_ppg, 0)
                    ELSE 0
                END,
                player_lamar_{suffix} = CASE
                    WHEN UPPER(p.position) IN ({eligible_sql})
                    THEN p.fantasy_points - COALESCE(fr.flex_repl_ppg, 0)
                    ELSE 0
                END,
                manager_lamar_{suffix} = CASE
                    WHEN UPPER(p.position) IN ({eligible_sql})
                     AND CAST(p.is_started AS INTEGER) = 1
                    THEN p.fantasy_points - COALESCE(fr.flex_repl_ppg, 0)
                    ELSE 0
                END,
                bench_lamar_{suffix} = CASE
                    WHEN UPPER(p.position) IN ({eligible_sql})
                     AND COALESCE(CAST(p.is_rostered AS INTEGER), 0) = 1
                     AND COALESCE(CAST(p.is_started AS INTEGER), 0) = 0
                     AND p.fantasy_points IS NOT NULL
                    THEN p.fantasy_points - COALESCE(fr.flex_repl_ppg, 0)
                    ELSE 0
                END
            FROM flex_replacement fr
            WHERE p.year = fr.year
              AND p.week = fr.week
              AND p.fantasy_points IS NOT NULL
              AND {_db_filter(db_name, 'p')}
        """

        if not dry_run:
            try:
                conn.execute(flex_lamar_sql)
                count = conn.execute(f"""
                    SELECT COUNT(*) FROM {player_table}
                    WHERE player_lamar_{suffix} IS NOT NULL
                      AND player_lamar_{suffix} != 0
                      AND {_db_filter(db_name)}
                """).fetchone()[0]
                logger.info(f"[calculate_flex_lamar] {flex_name} ({suffix}): {count:,} non-zero rows")
                total += count
            except Exception as e:
                logger.error(f"[calculate_flex_lamar] {flex_name} flex LAMAR failed: {e}")

    return total


# ---------------------------------------------------------------------------
# LAMAR YTD
# ---------------------------------------------------------------------------


def calculate_lamar_ytd(
    conn,
    player_table: str,
    flex_columns: list[str] | None = None,
    dry_run: bool = False,
    db_name: str | None = None,
) -> int:
    """Calculate YTD (year-to-date) cumulative LAMAR via SQL window functions.

    Parameters
    ----------
    conn : DuckDB connection
    player_table : Fully qualified table name.
    flex_columns : Explicit list of ``player_lamar_*`` flex column names
        (without ``_ytd``). If *None*, discovered from the table schema.
    dry_run : If True, log but don't execute.

    Returns
    -------
    Number of rows updated (or -1 if dry run / error).
    """
    cols = conn.execute(f"DESCRIBE {player_table}").fetchall()
    col_names = [c[0].lower() for c in cols]

    # Determine player ID column
    id_col = (
        "nfl_player_id"
        if "nfl_player_id" in col_names
        else select_platform_player_id_column(col_names, platform_hint=None)
    )
    if not id_col:
        logger.warning("[calculate_lamar_ytd] No player ID column found, using player_week")
        id_col = "player_week"

    # --- Core YTD (player_lamar, manager_lamar, bench_lamar) ----------------
    ytd_sql = f"""
        WITH ytd_calcs AS (
            SELECT
                player_week,
                SUM(COALESCE(player_lamar, 0)) OVER (
                    PARTITION BY {id_col}, year
                    ORDER BY week
                    ROWS UNBOUNDED PRECEDING
                ) as player_lamar_ytd_calc,
                SUM(COALESCE(manager_lamar, 0)) OVER (
                    PARTITION BY {id_col}, year
                    ORDER BY week
                    ROWS UNBOUNDED PRECEDING
                ) as manager_lamar_ytd_calc,
                SUM(COALESCE(bench_lamar, 0)) OVER (
                    PARTITION BY {id_col}, year
                    ORDER BY week
                    ROWS UNBOUNDED PRECEDING
                ) as bench_lamar_ytd_calc
            FROM {player_table}
            WHERE player_lamar IS NOT NULL
              AND {_db_filter(db_name)}
        )
        UPDATE {player_table} p
        SET
            player_lamar_ytd = c.player_lamar_ytd_calc,
            manager_lamar_ytd = c.manager_lamar_ytd_calc,
            bench_lamar_ytd = c.bench_lamar_ytd_calc
        FROM ytd_calcs c
        WHERE p.player_week = c.player_week
          AND {_db_filter(db_name, 'p')}
    """

    ytd_count = -1
    if not dry_run:
        try:
            conn.execute(ytd_sql)
            count_result = conn.execute(f"""
                SELECT COUNT(*) FROM {player_table}
                WHERE player_lamar_ytd IS NOT NULL
                  AND {_db_filter(db_name)}
            """).fetchone()
            ytd_count = count_result[0] if count_result else 0
            logger.info(f"[calculate_lamar_ytd] Updated {ytd_count:,} rows with YTD LAMAR")
        except Exception as e:
            logger.error(f"[calculate_lamar_ytd] Failed: {e}")
            return -1
    else:
        logger.info("[DRY RUN] Would calculate LAMAR YTD columns")
        return -1

    # --- Flex LAMAR YTD -----------------------------------------------------
    if flex_columns is None:
        # Discover from schema
        fresh_cols = conn.execute(f"DESCRIBE {player_table}").fetchall()
        col_names = [c[0].lower() for c in fresh_cols]
        flex_columns = [
            c[0]
            for c in fresh_cols
            if c[0].lower().startswith("player_lamar_")
            and not c[0].lower().endswith("_ytd")
            and c[0].lower() not in ("player_lamar", "player_lamar_ytd")
        ]

    col_name_set = set(col_names) if col_names else _get_table_columns(conn, player_table)

    for flex_player_col in flex_columns:
        suffix = flex_player_col.lower().replace("player_lamar_", "")
        manager_col_name = f"manager_lamar_{suffix}"
        bench_col_name = f"bench_lamar_{suffix}"

        if manager_col_name not in col_name_set or bench_col_name not in col_name_set:
            continue

        flex_ytd_sql = f"""
            WITH flex_ytd AS (
                SELECT
                    player_week,
                    SUM(COALESCE({flex_player_col}, 0)) OVER (
                        PARTITION BY {id_col}, year ORDER BY week ROWS UNBOUNDED PRECEDING
                    ) as p_ytd,
                    SUM(COALESCE({manager_col_name}, 0)) OVER (
                        PARTITION BY {id_col}, year ORDER BY week ROWS UNBOUNDED PRECEDING
                    ) as m_ytd,
                    SUM(COALESCE({bench_col_name}, 0)) OVER (
                        PARTITION BY {id_col}, year ORDER BY week ROWS UNBOUNDED PRECEDING
                    ) as b_ytd
                FROM {player_table}
                WHERE {flex_player_col} IS NOT NULL
                  AND {_db_filter(db_name)}
            )
            UPDATE {player_table} p
            SET
                {flex_player_col}_ytd = c.p_ytd,
                {manager_col_name}_ytd = c.m_ytd,
                {bench_col_name}_ytd = c.b_ytd
            FROM flex_ytd c
            WHERE p.player_week = c.player_week
              AND {_db_filter(db_name, 'p')}
        """

        try:
            conn.execute(flex_ytd_sql)
            logger.info(f"[calculate_lamar_ytd] Computed YTD for {suffix}")
        except Exception as e:
            logger.error(f"[calculate_lamar_ytd] {suffix} YTD failed: {e}")

    return ytd_count


# ---------------------------------------------------------------------------
# Main LAMAR entry point
# ---------------------------------------------------------------------------


def calculate_lamar(
    conn,
    player_table: str,
    roster_by_year: dict[Any, dict[str, Any]],
    dry_run: bool = False,
    db_name: str | None = None,
) -> int:
    """Calculate LAMAR for ALL players (rostered and unrostered).

    This is the correct approach:
    1. Rank ALL NFL players at each position by fantasy_points (DESC)
    2. N = number of roster slots for that position across all teams
    3. Replacement level = avg PPG of players ranked N to N+1
    4. player_lamar = fantasy_points - replacement_ppg for ALL players

    Parameters
    ----------
    conn : DuckDB connection
    player_table : Fully qualified table name (e.g. ``db.public.player_fantasy``)
    roster_by_year : Dict mapping year -> roster counts per position.
    dry_run : If True, log SQL but don't execute.

    Returns
    -------
    Number of rows updated.
    """
    if not roster_by_year:
        logger.warning("[calculate_lamar] No roster settings available, skipping")
        return 0

    # Ensure LAMAR columns exist before trying to UPDATE them
    cols = conn.execute(f"DESCRIBE {player_table}").fetchall()
    col_names = [c[0].lower() for c in cols]

    # Get number of teams
    try:
        num_teams_result = conn.execute(f"""
            SELECT COUNT(DISTINCT franchise_id) as num_teams
            FROM {player_table}
            WHERE {rostered_filter_sql(col="manager")}
              AND {_db_filter(db_name)}
        """).fetchone()
        num_teams = num_teams_result[0] if num_teams_result else 10
        logger.info(f"[calculate_lamar] Detected {num_teams} teams in league")
    except Exception as e:
        logger.warning(f"[calculate_lamar] Could not detect num_teams: {e}, defaulting to 10")
        num_teams = 10

    # =======================================================================
    # VECTORIZED LAMAR CALCULATION - Single query for ALL years
    # =======================================================================
    logger.info("[calculate_lamar] Running vectorized LAMAR calculation (single query for all years)...")

    # Dual positions (e.g. 'WR,RB' for Cordarrelle Patterson, 'WR,DB' for Travis
    # Hunter) use the primary (first) position for replacement-pool grouping.
    # Granular IDP positions (CB, FS, DT, etc.) are normalized to parent groups
    # (DB, DL, LB) by the platform normalizers at import time.
    _POS = "UPPER(SPLIT_PART(position, ',', 1))"

    lamar_sql = f"""
        WITH weekly_rostered_pct AS (
            SELECT
                r.year, r.week, r.position,
                1.0 * r.rostered_count / NULLIF(t.total_count, 0) as rostered_pct
            FROM (
                SELECT year, week, {_POS} as position, COUNT(*) as rostered_count
                FROM {player_table}
                WHERE {rostered_filter_sql(col="manager")}
                  AND {_db_filter(db_name)}
                GROUP BY year, week, {_POS}
            ) r
            JOIN (
                SELECT year, week, {_POS} as position, COUNT(*) as total_count
                FROM {player_table}
                WHERE {_db_filter(db_name)}
                GROUP BY year, week, {_POS}
            ) t ON r.year = t.year AND r.week = t.week AND r.position = t.position
        ),
        league_avg_rostered_pct AS (
            SELECT position, AVG(rostered_pct) as avg_rostered_pct
            FROM weekly_rostered_pct
            GROUP BY position
        ),
        total_players AS (
            SELECT year, week, {_POS} as position,
                   COUNT(*) as total_count
            FROM {player_table}
            WHERE fantasy_points IS NOT NULL
              AND fantasy_points > 0
              AND {_db_filter(db_name)}
            GROUP BY year, week, {_POS}
        ),
        ranked_players AS (
            SELECT
                year, week, player_week, {_POS} as position, fantasy_points,
                ROW_NUMBER() OVER (
                    PARTITION BY year, week, {_POS}
                    ORDER BY fantasy_points DESC NULLS LAST
                ) as pos_rank
            FROM {player_table}
            WHERE fantasy_points IS NOT NULL
              AND fantasy_points > 0
              AND {_db_filter(db_name)}
        ),
        replacement_candidates AS (
            SELECT
                r.year, r.week, r.position,
                AVG(r.fantasy_points) as replacement_ppg
            FROM ranked_players r
            JOIN total_players tp ON r.year = tp.year AND r.week = tp.week AND r.position = tp.position
            LEFT JOIN weekly_rostered_pct wp ON r.year = wp.year AND r.week = wp.week AND r.position = wp.position
            LEFT JOIN league_avg_rostered_pct la ON r.position = la.position
            WHERE r.pos_rank BETWEEN
                CEILING(tp.total_count * COALESCE(wp.rostered_pct, la.avg_rostered_pct, 0.5))
                AND CEILING(tp.total_count * COALESCE(wp.rostered_pct, la.avg_rostered_pct, 0.5)) + 1
            GROUP BY r.year, r.week, r.position
        )
        UPDATE {player_table} p
        SET
            replacement_ppg = COALESCE(rc.replacement_ppg, 0),
            player_lamar = p.fantasy_points - COALESCE(rc.replacement_ppg, 0),
            manager_lamar = CASE
                WHEN CAST(p.is_started AS INTEGER) = 1 THEN p.fantasy_points - COALESCE(rc.replacement_ppg, 0)
                ELSE NULL
            END,
            bench_lamar = CASE
                WHEN COALESCE(CAST(p.is_rostered AS INTEGER), 0) = 1
                 AND COALESCE(CAST(p.is_started AS INTEGER), 0) = 0
                 AND p.fantasy_points IS NOT NULL
                THEN p.fantasy_points - COALESCE(rc.replacement_ppg, 0)
                ELSE 0
            END
        FROM replacement_candidates rc
        WHERE p.year = rc.year
          AND p.week = rc.week
          AND UPPER(SPLIT_PART(p.position, ',', 1)) = rc.position
          AND p.fantasy_points IS NOT NULL
          AND {_db_filter(db_name, 'p')}
    """

    total_updated = 0
    if not dry_run:
        try:
            conn.execute(lamar_sql)
            count_result = conn.execute(f"""
                SELECT COUNT(*) FROM {player_table}
                WHERE player_lamar IS NOT NULL
                  AND {_db_filter(db_name)}
            """).fetchone()
            total_updated = count_result[0] if count_result else 0
            logger.info(f"[calculate_lamar] Updated LAMAR for {total_updated:,} rows (single query)")

            # Diagnostic
            diag = conn.execute(f"""
                SELECT
                    COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER) = 1) as started_count,
                COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER) = 1 AND manager_lamar != 0) as started_nonzero_lamar,
                COUNT(*) FILTER (WHERE CAST(is_started AS INTEGER) = 1 AND manager_lamar = 0) as started_zero_lamar
                FROM {player_table}
                WHERE player_lamar IS NOT NULL
                  AND {_db_filter(db_name)}
            """).fetchone()
            if diag:
                logger.info(
                    f"[calculate_lamar] Diagnostics: {diag[0]:,} started, "
                    f"{diag[1]:,} with non-zero manager_lamar, "
                    f"{diag[2]:,} with zero manager_lamar"
                )
        except Exception as e:
            logger.error(f"[calculate_lamar] Failed: {e}")
            raise

    # =======================================================================
    # FLEX LAMAR
    # =======================================================================
    all_flex_pools: dict[str, list[str]] = {}
    for _year_key, year_settings in roster_by_year.items():
        roster_counts = year_settings if isinstance(year_settings, dict) else {}
        if "roster_position_counts" in roster_counts:
            counts = roster_counts["roster_position_counts"]
        elif "roster_positions" in roster_counts:
            rp = roster_counts["roster_positions"]
            if rp and isinstance(rp[0], dict):
                counts = {p["position"]: p.get("count", 0) for p in rp}
            elif rp and isinstance(rp[0], str):
                counts = dict(Counter(rp))
            else:
                counts = {}
        else:
            counts = roster_counts

        pools = get_flex_pools(counts)
        for flex_name, eligible, _count in pools:
            if flex_name not in all_flex_pools:
                all_flex_pools[flex_name] = eligible

    if all_flex_pools:
        logger.info(f"[calculate_lamar] Flex pools found: {list(all_flex_pools.keys())}")
        calculate_flex_lamar(
            conn,
            player_table,
            all_flex_pools,
            col_names,
            dry_run=dry_run,
            db_name=db_name,
        )

    # =======================================================================
    # LAMAR YTD
    # =======================================================================
    logger.info("[calculate_lamar] Starting LAMAR YTD calculation...")
    ytd_count = calculate_lamar_ytd(conn, player_table, dry_run=dry_run, db_name=db_name)
    logger.info(f"[calculate_lamar] LAMAR YTD complete: {ytd_count} rows updated")

    return total_updated
