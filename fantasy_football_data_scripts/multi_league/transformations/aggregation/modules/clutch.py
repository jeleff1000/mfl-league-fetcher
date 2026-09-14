"""
Clutch equity calculation module -- pure function extracted from AggregationEnrichmentsMixin.

All functions take (conn, player_table, matchup_table, ...) instead of using self.
"""

from __future__ import annotations

import logging

from multi_league.core.sql_utils import validate_db_name

logger = logging.getLogger(__name__)


def _db_filter(db_name: str | None, alias: str = "") -> str:
    """Return the centralized db_name filter or a no-op for local mode."""
    if not db_name:
        return "1=1"

    validate_db_name(db_name)
    prefix = f"{alias}." if alias else ""
    return f"{prefix}db_name = '{db_name}'"


def calculate_clutch_equity(
    conn,
    player_table: str,
    matchup_table: str,
    dry_run: bool = False,
    db_name: str | None = None,
) -> int:
    """Calculate clutch equity metrics using SQL window functions.

    Measures how much each player contributed to championship odds changes.
    Requires p_champ column in matchup table (from playoff sims).

    This replaces the heavy pandas operations in clutch_calculator.py:
        baseline = df.groupby(['year', 'week', 'position']).agg({'manager_lamar': 'mean'})
        team_totals = started_df.groupby([manager_col, 'year', 'week']).agg({...})

    Clutch equity measures how much each player contributed to championship odds changes:
    - Players who exceed their position's weekly baseline get credit when odds go up
    - Players who fall below baseline get blamed when odds go down
    - sum(clutch_equity) = actual odds change for each team-week

    Args:
        conn: DuckDB connection
        player_table: Fully qualified name of the player_fantasy table
        matchup_table: Fully qualified name of the matchup table
        dry_run: If True, log SQL but do not execute

    Returns:
        Number of rows updated, 0 if skipped, or -1 if an error occurred
    """
    # Verify player_fantasy table exists
    try:
        conn.execute(f"DESCRIBE {player_table}")
    except Exception:
        logger.warning("[calculate_clutch_equity] player_fantasy table not found")
        return 0

    # Verify matchup table exists
    try:
        conn.execute(f"DESCRIBE {matchup_table}")
    except Exception:
        logger.warning("[calculate_clutch_equity] matchup table not found - skipping clutch")
        return 0

    # Check if matchup has championship odds (p_champ column)
    matchup_cols = {c[0].lower() for c in conn.execute(f"DESCRIBE {matchup_table}").fetchall()}
    if "p_champ" not in matchup_cols:
        logger.warning("[calculate_clutch_equity] p_champ column not in matchup - skipping")
        return 0

    # Step 1: Calculate odds_delta in matchup table
    # odds_delta = current odds - previous week's odds (within same manager+year)
    # For week 1, use 100/num_teams as the starting baseline (not hardcoded 10%)
    odds_delta_sql = f"""
        WITH
        -- Get num_teams per year to calculate dynamic starting odds
        teams_per_year AS (
            SELECT year, COUNT(DISTINCT franchise_id) as num_teams
            FROM {matchup_table}
            WHERE manager IS NOT NULL
              AND {_db_filter(db_name)}
            GROUP BY year
        ),
        odds_with_prev AS (
            SELECT
                m.manager_week,
                m.manager,
                m.year,
                m.week,
                m.p_champ,
                LAG(m.p_champ, 1) OVER (
                    PARTITION BY m.franchise_id, m.year
                    ORDER BY m.week
                ) as prev_odds,
                100.0 / COALESCE(t.num_teams, 10) as starting_odds
            FROM {matchup_table} m
            LEFT JOIN teams_per_year t ON m.year = t.year
            WHERE {_db_filter(db_name, 'm')}
        ),
        deltas AS (
            SELECT
                manager_week,
                p_champ - COALESCE(prev_odds, starting_odds) as odds_delta_calc
            FROM odds_with_prev
        )
        UPDATE {matchup_table} m
        SET odds_delta = d.odds_delta_calc
        FROM deltas d
        WHERE m.manager_week = d.manager_week
          AND {_db_filter(db_name, 'm')}
    """

    if not dry_run:
        try:
            conn.execute(odds_delta_sql)
            logger.info("[calculate_clutch_equity] Calculated odds_delta in matchup")
        except Exception as e:
            logger.error(f"[calculate_clutch_equity] Failed to calculate odds_delta: {e}")
            return -1

    # Step 2: Calculate weekly starter baseline LAMAR by position
    # This is the average manager_lamar of all STARTED players at each position
    # Step 3: Calculate above_baseline and below_baseline for each player
    # Step 4: Calculate team totals and distribute clutch equity proportionally

    clutch_sql = f"""
        WITH
        -- Step 2a: Calculate weekly starter baseline by position (started players only)
        starter_baseline AS (
            SELECT
                year, week, position,
                AVG(manager_lamar) as baseline_lamar
            FROM {player_table}
            WHERE CAST(is_started AS INTEGER) = 1
              AND manager_lamar IS NOT NULL
              AND position IS NOT NULL
              AND {_db_filter(db_name)}
            GROUP BY year, week, position
        ),

        -- Step 2b: Join baseline to all players
        with_baseline AS (
            SELECT
                p.player_week,
                p.manager,
                p.year,
                p.week,
                p.position,
                p.manager_lamar,
                p.is_started,
                COALESCE(b.baseline_lamar, 0) as starter_baseline_lamar,
                -- above_baseline = max(lamar - baseline, 0)
                GREATEST(COALESCE(p.manager_lamar, 0) - COALESCE(b.baseline_lamar, 0), 0) as above_baseline,
                -- below_baseline = max(baseline - lamar, 0)
                GREATEST(COALESCE(b.baseline_lamar, 0) - COALESCE(p.manager_lamar, 0), 0) as below_baseline
            FROM {player_table} p
            LEFT JOIN starter_baseline b
                ON p.year = b.year AND p.week = b.week AND p.position = b.position
            WHERE {_db_filter(db_name, 'p')}
        ),

        -- Step 3: Calculate team totals for started players
        team_totals AS (
            SELECT
                franchise_id as _mgr_key, year, week,
                SUM(above_baseline) as team_above_baseline,
                SUM(below_baseline) as team_below_baseline
            FROM with_baseline
            WHERE CAST(is_started AS INTEGER) = 1
            GROUP BY franchise_id, year, week
        ),

        -- Step 4: Join matchup odds_delta and calculate clutch_equity
        clutch_calc AS (
            SELECT
                w.player_week,
                w.starter_baseline_lamar,
                w.above_baseline,
                w.below_baseline,
                CASE
                    -- Odds went UP: credit above-baseline players proportionally
                    WHEN m.odds_delta > 0 AND t.team_above_baseline > 0 AND CAST(w.is_started AS INTEGER) = 1
                    THEN m.odds_delta * (w.above_baseline / t.team_above_baseline)
                    -- Odds went DOWN: blame below-baseline players proportionally
                    WHEN m.odds_delta < 0 AND t.team_below_baseline > 0 AND CAST(w.is_started AS INTEGER) = 1
                    THEN m.odds_delta * (w.below_baseline / t.team_below_baseline)
                    -- No odds change, or edge cases: clutch = 0
                    ELSE 0
                END as clutch_equity
            FROM with_baseline w
            LEFT JOIN team_totals t
                ON w.franchise_id = t._mgr_key AND w.year = t.year AND w.week = t.week
            LEFT JOIN {matchup_table} m
                ON w.franchise_id = m.franchise_id
               AND w.year = m.year
               AND w.week = m.week
               AND {_db_filter(db_name, 'm')}
        )
        UPDATE {player_table} p
        SET
            starter_baseline_lamar = c.starter_baseline_lamar,
            above_baseline = c.above_baseline,
            below_baseline = c.below_baseline,
            clutch_equity = c.clutch_equity
        FROM clutch_calc c
        WHERE p.player_week = c.player_week
          AND {_db_filter(db_name, 'p')}
    """

    if not dry_run:
        try:
            conn.execute(clutch_sql)
            # Count how many rows were updated
            count_result = conn.execute(f"""
                SELECT COUNT(*) FROM {player_table}
                WHERE clutch_equity IS NOT NULL
                  AND clutch_equity != 0
                  AND {_db_filter(db_name)}
            """).fetchone()
            clutch_count = count_result[0] if count_result else 0
            logger.info(f"[calculate_clutch_equity] Updated {clutch_count:,} rows with clutch metrics")
            return clutch_count
        except Exception as e:
            logger.error(f"[calculate_clutch_equity] Failed: {e}")
            return -1
    else:
        logger.info("[DRY RUN] Would calculate clutch equity")
        return -1
