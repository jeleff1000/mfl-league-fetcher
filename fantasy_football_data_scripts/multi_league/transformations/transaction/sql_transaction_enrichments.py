"""
Transaction Enrichments Mixin

Transaction cross-joins and analytics methods.
"""

import logging

logger = logging.getLogger(__name__)

_ADD_TRANSACTION_TYPES_SQL = "('add', 'pickup', 'claim', 'waiver', 'add/drop')"
_ADD_DROP_TRANSACTION_TYPES_SQL = "('add', 'pickup', 'claim', 'waiver', 'add/drop', 'drop')"


def _trade_direction_clause(alias: str, direction: str) -> str:
    """Return SQL that filters trade rows to a single manager perspective."""
    return f" AND {alias}.trade_direction = '{direction}'"


def _add_like_predicate(alias: str) -> str:
    """Trade receivers behave like adds for roster-period calculations."""
    return (
        f"({alias}.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} "
        f"OR ({alias}.transaction_type IN ('trade', 'trade_pick')"
        f"{_trade_direction_clause(alias, 'received')}))"
    )


def _drop_like_predicate(alias: str) -> str:
    """Trade senders behave like drops for roster-period calculations."""
    return (
        f"({alias}.transaction_type = 'drop' "
        f"OR ({alias}.transaction_type IN ('trade', 'trade_pick')"
        f"{_trade_direction_clause(alias, 'sent')}))"
    )


def _non_trade_add_predicate(alias: str) -> str:
    """Return the predicate for add-style transactions only."""
    return f"{alias}.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL}"


def _faab_acquisition_predicate(alias: str) -> str:
    """Return the predicate for add-like transactions that establish FAAB keeper cost."""
    return f"{alias}.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL}"


def _faab_owner_column(player_cols: set[str], trans_cols: set[str]) -> str:
    """Return the franchise_id column for FAAB carry. Raises if missing on either side."""
    if "franchise_id" not in player_cols or "franchise_id" not in trans_cols:
        raise KeyError("franchise_id is required for manager identity")
    return "franchise_id"


def _owner_presence_predicate(alias: str) -> str:
    """Return SQL filtering to rows with a non-null franchise_id."""
    return f"TRIM(COALESCE(CAST({alias}.franchise_id AS VARCHAR), '')) != ''"


def _trade_received_predicate(alias: str) -> str:
    """Return the predicate for trade rows from the acquiring manager's perspective."""
    return f"{alias}.transaction_type IN ('trade', 'trade_pick'){_trade_direction_clause(alias, 'received')}"


def _trade_asset_key_expr(trans_cols: set[str], alias: str) -> str:
    """Build a stable asset key that matches mirrored sent/received trade rows."""
    key_parts: list[str] = []
    for col in [
        "NFL_player_id",
        "yahoo_player_id",
        "sleeper_player_id",
        "espn_player_id",
        "player",
        "traded_pick_season",
        "traded_pick_round",
        "traded_pick_original_owner",
    ]:
        if col in trans_cols or col.lower() in trans_cols:
            key_parts.append(f"COALESCE(CAST({alias}.{col} AS VARCHAR), '')")
    if not key_parts:
        return "''"
    player_key = f"CONCAT_WS('|', {', '.join(key_parts)})"

    # Sleeper can retain a stale conveyed-player mapping on one perspective of
    # a draft-pick trade.  The pick ID is the asset identity; the player fields
    # are enrichment and must not prevent the sent/received legs from pairing.
    if "transaction_type" in trans_cols and "sleeper_player_id" in trans_cols:
        pick_key = (
            f"COALESCE(NULLIF(CAST({alias}.sleeper_player_id AS VARCHAR), ''), "
            f"{player_key})"
        )
        return (
            f"CASE WHEN {alias}.transaction_type = 'trade_pick' "
            f"THEN CONCAT('pick|', {pick_key}) ELSE CONCAT('player|', {player_key}) END"
        )
    return player_key


class TransactionEnrichmentsMixin:
    """Mixin providing transaction-related SQL enrichments.

    Requires SQLEnrichmentsBase infrastructure (self._execute, self._table_exists, etc.)
    """

    def _managed_roster_periods_sql(self, join_key: str) -> str:
        """Shared acquisition windows; scoring-roster ownership resolves drop legs.

        A provider's transaction leg can include moves after the game. Include
        the drop leg, then require the player's actual franchise for that week.
        Event timestamps distinguish a same-leg drop before an acquisition.
        """
        trans_cols = self._get_table_columns("transactions")
        _faab_owner_column(self._get_table_columns("player_fantasy"), trans_cols)
        event_time = (
            "COALESCE(TRY_CAST(t.timestamp AS DOUBLE), "
            "epoch(TRY_CAST(t.timestamp AS TIMESTAMP)))"
            if "timestamp" in trans_cols else "NULL::DOUBLE"
        )
        return f"""
            league_end_weeks AS (
                SELECT CAST(year AS INTEGER) AS year, COALESCE(end_week, 18) AS end_week
                FROM {self._qualified_name('league_settings')}
                WHERE {self._db_filter()}
            ),
            add_transactions AS (
                SELECT t.transaction_id, t.{join_key}, t.year, t.franchise_id,
                       t.cumulative_week AS add_cw, {event_time} AS event_time
                FROM {self._qualified_name('transactions')} t
                WHERE {_add_like_predicate('t')} AND {self._db_filter('t')}
                  AND t.{join_key} IS NOT NULL AND {_owner_presence_predicate('t')}
            ),
            drop_transactions AS (
                SELECT t.{join_key}, t.year, t.franchise_id,
                       t.cumulative_week AS drop_cw, {event_time} AS event_time
                FROM {self._qualified_name('transactions')} t
                WHERE {_drop_like_predicate('t')} AND {self._db_filter('t')}
                  AND t.{join_key} IS NOT NULL
            ),
            roster_periods AS (
                SELECT a.transaction_id, a.{join_key}, a.year, a.franchise_id, a.add_cw, a.event_time,
                       COALESCE(
                           (SELECT MIN(d.drop_cw) FROM drop_transactions d
                            WHERE d.{join_key} = a.{join_key} AND d.year = a.year
                              AND d.franchise_id = a.franchise_id
                              AND (d.drop_cw > a.add_cw OR (
                                  d.drop_cw = a.add_cw AND d.event_time >= a.event_time))),
                           (SELECT CAST(lew.year AS BIGINT) * 100 + lew.end_week
                            FROM league_end_weeks lew WHERE lew.year = CAST(a.year AS INTEGER))
                       ) AS roster_end_cw
                FROM add_transactions a
            ),
            managed_player_weeks AS (
                SELECT p.*, rp.transaction_id AS _acquisition_id
                FROM roster_periods rp
                INNER JOIN {self._qualified_name('player_fantasy')} p
                    ON p.{join_key} = rp.{join_key} AND p.year = rp.year
                    AND p.franchise_id = rp.franchise_id
                    AND p.cumulative_week >= rp.add_cw
                    AND p.cumulative_week <= rp.roster_end_cw
                LEFT JOIN league_end_weeks lew ON CAST(p.year AS INTEGER) = lew.year
                WHERE {self._db_filter('p')} AND p.week <= COALESCE(lew.end_week, 18)
                -- Never award the same scoring row to two acquisition periods.
                -- At provider weekly grain the latest eligible acquisition owns it.
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY p.rowid
                    ORDER BY rp.add_cw DESC, rp.event_time DESC NULLS LAST, rp.transaction_id DESC
                ) = 1
            )
        """

    def transactions_to_player(self) -> int:
        """Add cumulative FAAB metrics to player_fantasy table.

        Calculates max FAAB bid to date for each player-year-week.
        Join key: (player_id, year, cumulative_week)

        Replaces: transactions_to_player_v2.py
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[transactions_to_player] player_fantasy table not found")
            return 0

        if not self._table_exists("transactions"):
            logger.warning("[transactions_to_player] transactions table not found - skipping")
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        trans_cols = self._get_table_columns("transactions")
        trans_table = self._qualified_name("transactions")
        player_table = self._qualified_name("player_fantasy")

        join_key = "NFL_player_id"
        owner_col = _faab_owner_column(player_cols, trans_cols)

        # Check required columns
        if "faab_bid" not in trans_cols:
            logger.warning("[transactions_to_player] faab_bid column not found in transactions")
            return 0

        if "cumulative_week" not in player_cols or "cumulative_week" not in trans_cols:
            logger.warning("[transactions_to_player] cumulative_week not found in both tables")
            return 0

        # Build SET clauses and aggregations based on columns that exist in BOTH tables
        # DuckDB UPDATE requires the target column to exist (can't add columns via UPDATE)
        agg_parts = []
        set_parts = []
        partition_cols = [f"t.{join_key}", "t.year", f"t.{owner_col}"]
        partition_sql = ", ".join(partition_cols)
        owner_select_t = f", t.{owner_col}"
        owner_select_p = f", p.{owner_col}"
        owner_group_t = f", t.{owner_col}"
        owner_group_p = f", p.{owner_col}"
        owner_join_pf_fc = f"\n              AND p.{owner_col} = fc.{owner_col}"
        owner_join_tp = f"\n                     AND t.{owner_col} = p.{owner_col}"
        owner_join_pf = f"\n                  AND p.{owner_col} = f.{owner_col}"
        owner_filter_t = f"\n                  AND {_owner_presence_predicate('t')}"
        owner_filter_p = f"\n                      AND {_owner_presence_predicate('p')}"

        if "max_faab_bid_to_date" in player_cols:
            agg_parts.append(f"""MAX(faab_bid) OVER (
                        PARTITION BY {partition_sql}
                        ORDER BY t.cumulative_week
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) as max_faab_bid_to_date""")
            set_parts.append("max_faab_bid_to_date = fc.max_faab_bid_to_date")

        if "first_acquisition_week" in player_cols:
            agg_parts.append(f"""MIN(cumulative_week) OVER (
                        PARTITION BY {partition_sql}
                    ) as first_acquisition_week""")
            set_parts.append("first_acquisition_week = fc.first_acquisition_week")

        if "total_acquisitions" in player_cols:
            agg_parts.append(f"""COUNT(*) OVER (
                        PARTITION BY {partition_sql}
                    ) as total_acquisitions""")
            set_parts.append("total_acquisitions = fc.total_acquisitions")

        if not set_parts:
            logger.warning("[transactions_to_player] No target columns found in player_fantasy")
            return 0

        sql = f"""
            WITH faab_cumulative AS (
                SELECT
                    t.{join_key},
                    t.year,
                    t.cumulative_week{owner_select_t},
                    {', '.join(agg_parts)}
                FROM {trans_table} t
                WHERE {_faab_acquisition_predicate('t')}
                  AND {self._db_filter('t')}
                  AND t.faab_bid IS NOT NULL
                  AND t.faab_bid > 0{owner_filter_t}
            )
            UPDATE {player_table} p
            SET {', '.join(set_parts)}
            FROM faab_cumulative fc
            WHERE p.{join_key} = fc.{join_key}
              AND p.year = fc.year
              AND p.cumulative_week = fc.cumulative_week
              {owner_join_pf_fc}
              AND {self._db_filter('p')}
        """

        rows = self._execute(sql, f"transactions_to_player: Add cumulative FAAB (key={join_key})")

        # Carry max_faab_bid_to_date forward to all future weeks within the year.
        # Example: FAAB $30 in week 4, $1 in week 6 -> max stays $30 after week 4.
        if "max_faab_bid_to_date" in player_cols:
            sql_forward = f"""
                WITH trans_weekly AS (
                    SELECT
                        t.{join_key},
                        t.year,
                        t.cumulative_week{owner_select_t},
                        MAX(t.faab_bid) AS faab_bid
                    FROM {trans_table} t
                    WHERE {_faab_acquisition_predicate('t')}
                      AND {self._db_filter('t')}
                      AND t.faab_bid IS NOT NULL
                      AND t.faab_bid > 0{owner_filter_t}
                    GROUP BY t.{join_key}, t.year, t.cumulative_week{owner_group_t}
                ),
                pf_weeks AS (
                    SELECT DISTINCT
                        p.{join_key},
                        p.year,
                        p.cumulative_week{owner_select_p}
                    FROM {player_table} p
                    WHERE p.{join_key} IS NOT NULL
                      AND {self._db_filter('p')}{owner_filter_p}
                ),
                faab_forward AS (
                    SELECT
                        p.{join_key},
                        p.year,
                        p.cumulative_week{owner_select_p},
                        MAX(t.faab_bid) AS max_faab_bid_to_date
                    FROM pf_weeks p
                    LEFT JOIN trans_weekly t
                      ON t.{join_key} = p.{join_key}
                     AND t.year = p.year
                     AND t.cumulative_week <= p.cumulative_week{owner_join_tp}
                    GROUP BY p.{join_key}, p.year, p.cumulative_week{owner_group_p}
                )
                UPDATE {player_table} p
                SET max_faab_bid_to_date = f.max_faab_bid_to_date
                FROM faab_forward f
                WHERE p.{join_key} = f.{join_key}
                  AND p.year = f.year
                  AND p.cumulative_week = f.cumulative_week
                  {owner_join_pf}
                  AND {self._db_filter('p')}
            """
            rows += self._execute(sql_forward, "transactions_to_player: Carry forward max FAAB bids")

        return rows

    # =========================================================================
    # PLAYER TO TRANSACTIONS: Add player stats to transaction table
    # =========================================================================

    def player_to_transactions(self) -> int:
        """Add player performance metrics to transactions table.

        Orchestrates sub-enrichments:
        1. _player_perf_at_transaction — before/after fantasy_points metrics
        2. _player_ros_points — ROS fantasy_points (managed + total + aliases)

        Replaces: player_to_transactions_v2.py
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[player_to_transactions] player_fantasy table not found")
            return 0

        if not self._table_exists("transactions"):
            logger.warning("[player_to_transactions] transactions table not found - skipping")
            return 0

        rows = 0
        rows += self._player_perf_at_transaction()
        rows += self._player_ros_points()
        return rows

    # ------------------------------------------------------------------
    # Sub-method: before/after performance metrics
    # ------------------------------------------------------------------

    def _player_perf_at_transaction(self) -> int:
        """Calculate before/after fantasy_points metrics on transactions.

        Sets: points_at_transaction, ppg_before_transaction, weeks_before,
              ppg_after_transaction, total_points_after_4wks, weeks_after,
              position, lamar_at_transaction.
        """
        player_cols = self._get_table_columns("player_fantasy")
        trans_cols = self._get_table_columns("transactions")
        trans_table = self._qualified_name("transactions")
        player_table = self._qualified_name("player_fantasy")

        join_key = "NFL_player_id"

        has_fp = "fantasy_points" in player_cols

        if not has_fp:
            logger.warning("[_player_perf_at_transaction] fantasy_points not in player_fantasy — skipping")
            return 0

        rows = 0

        # --- Part A: position + lamar_at_transaction (same-week point lookup) ---
        # These use cumulative_week matching (player row for the transaction week)
        set_same_week = []
        if "position" in player_cols and "position" in trans_cols:
            set_same_week.append("position = pat.position")
        if "player_lamar" in player_cols and "lamar_at_transaction" in trans_cols:
            set_same_week.append("lamar_at_transaction = pat.player_lamar")

        if set_same_week and "cumulative_week" in player_cols and "cumulative_week" in trans_cols:
            select_parts = [join_key, "year", "cumulative_week"]
            if "position" in player_cols and "position" in trans_cols:
                select_parts.append("position")
            if "player_lamar" in player_cols and "lamar_at_transaction" in trans_cols:
                select_parts.append("player_lamar")

            sql_same_week = f"""
                WITH player_at_trans AS (
                    SELECT {', '.join(select_parts)}
                    FROM {player_table}
                    WHERE {join_key} IS NOT NULL
                      AND {self._db_filter()}
                )
                UPDATE {trans_table} t
                SET {', '.join(set_same_week)}
                FROM player_at_trans pat
                WHERE t.{join_key} = pat.{join_key}
                  AND t.year = pat.year
                  AND t.cumulative_week = pat.cumulative_week
                  AND {self._db_filter('t')}
            """
            rows += self._execute(sql_same_week, "player_to_transactions: position + lamar_at_transaction")

        # --- Part B: before/after aggregated metrics ---
        # Build SET/SELECT based on which target columns exist in transactions
        set_parts = []
        if "points_at_transaction" in trans_cols:
            set_parts.append("points_at_transaction = tp.points_at_transaction")
        if "ppg_before_transaction" in trans_cols:
            set_parts.append("ppg_before_transaction = tp.ppg_before_transaction")
        if "weeks_before" in trans_cols:
            set_parts.append("weeks_before = tp.weeks_before")
        if "ppg_after_transaction" in trans_cols:
            set_parts.append("ppg_after_transaction = tp.ppg_after_transaction")
        if "total_points_after_4wks" in trans_cols:
            set_parts.append("total_points_after_4wks = tp.total_points_after_4wks")
        if "weeks_after" in trans_cols:
            set_parts.append("weeks_after = tp.weeks_after")

        if not set_parts:
            logger.info("[_player_perf_at_transaction] No before/after target columns in transactions")
            return rows

        sql = f"""
            WITH trans_perf AS (
                SELECT
                    t.transaction_id,
                    t.{join_key},
                    -- Before metrics (strict < transaction week)
                    SUM(CASE WHEN p.week < t.week THEN p.fantasy_points END) AS points_at_transaction,
                    AVG(CASE WHEN p.week < t.week THEN p.fantasy_points END) AS ppg_before_transaction,
                    COUNT(CASE WHEN p.week < t.week THEN 1 END) AS weeks_before,
                    -- After metrics (strict > transaction week)
                    AVG(CASE WHEN p.week > t.week THEN p.fantasy_points END) AS ppg_after_transaction,
                    COUNT(CASE WHEN p.week > t.week THEN 1 END) AS weeks_after,
                    -- Next 4 weeks after transaction
                    SUM(CASE WHEN p.week > t.week AND p.week <= t.week + 4
                             THEN p.fantasy_points END) AS total_points_after_4wks
                FROM {trans_table} t
                INNER JOIN {player_table} p
                    ON p.{join_key} = t.{join_key}
                    AND p.year = t.year
                WHERE t.{join_key} IS NOT NULL
                  AND {self._db_filter('t')}
                  AND {self._db_filter('p')}
                GROUP BY t.transaction_id, t.{join_key}
            )
            UPDATE {trans_table} t
            SET {', '.join(set_parts)}
            FROM trans_perf tp
            WHERE t.transaction_id = tp.transaction_id
              AND t.{join_key} = tp.{join_key}
              AND {self._db_filter('t')}
        """

        rows += self._execute(sql, f"player_to_transactions: before/after metrics (key={join_key})")
        return rows

    # ------------------------------------------------------------------
    # Sub-method: ROS fantasy_points (managed + total + aliases)
    # ------------------------------------------------------------------

    def _player_ros_points(self) -> int:
        """Calculate rest-of-season fantasy_points metrics on transactions.

        Sets: total_points_ros_total, ppg_ros_total, weeks_ros_total,
              replacement_ppg_ros_total,
              total_points_ros_managed, ppg_ros_managed, weeks_ros_managed,
              replacement_ppg_ros_managed.
        """
        player_cols = self._get_table_columns("player_fantasy")
        trans_cols = self._get_table_columns("transactions")
        trans_table = self._qualified_name("transactions")
        player_table = self._qualified_name("player_fantasy")

        join_key = "NFL_player_id"

        if "fantasy_points" not in player_cols:
            logger.warning("[_player_ros_points] fantasy_points not in player_fantasy — skipping")
            return 0

        has_replacement_ppg = "replacement_ppg" in player_cols
        rows = 0

        # ---- Step 1: ROS total (all games regardless of roster) ----
        set_total = []
        if "total_points_ros_total" in trans_cols:
            set_total.append("total_points_ros_total = rt.total_points_ros_total")
        if "ppg_ros_total" in trans_cols:
            set_total.append("ppg_ros_total = rt.ppg_ros_total")
        if "weeks_ros_total" in trans_cols:
            set_total.append("weeks_ros_total = rt.weeks_ros_total")
        # Replacement PPG
        if has_replacement_ppg and "replacement_ppg_ros_total" in trans_cols:
            set_total.append("replacement_ppg_ros_total = rt.replacement_ppg_ros_total")

        if set_total:
            replacement_select = ""
            if has_replacement_ppg and "replacement_ppg_ros_total" in trans_cols:
                replacement_select = ",\n                    AVG(p.replacement_ppg) AS replacement_ppg_ros_total"

            settings_table = self._qualified_name("league_settings")
            sql_ros_total = f"""
                WITH league_end_weeks AS (
                    SELECT
                        CAST(year AS INTEGER) AS year,
                    COALESCE(
                        end_week,
                        18
                    ) AS end_week
                    FROM {settings_table}
                    WHERE {self._db_filter()}
                ),
                -- Deduplicate: unique (player, year, week) from transactions
                unique_trans AS (
                    SELECT DISTINCT
                        {join_key}, year, week
                    FROM {trans_table} t
                    WHERE {join_key} IS NOT NULL
                      AND {self._db_filter('t')}
                ),
                ros_total AS (
                    SELECT
                        ut.{join_key},
                        ut.year,
                        ut.week AS trans_week,
                        SUM(p.fantasy_points) AS total_points_ros_total,
                        AVG(p.fantasy_points) AS ppg_ros_total,
                        COUNT(*) AS weeks_ros_total{replacement_select}
                    FROM unique_trans ut
                    INNER JOIN {player_table} p
                        ON p.{join_key} = ut.{join_key}
                        AND p.year = ut.year
                        AND p.week >= ut.week
                LEFT JOIN league_end_weeks lew
                    ON CAST(p.year AS INTEGER) = lew.year
                WHERE p.{join_key} IS NOT NULL
                  AND {self._db_filter('p')}
                  AND p.week <= COALESCE(lew.end_week, 18)
                GROUP BY ut.{join_key}, ut.year, ut.week
            )
                UPDATE {trans_table} t
                SET {', '.join(set_total)}
                FROM ros_total rt
                WHERE t.{join_key} = rt.{join_key}
                  AND t.year = rt.year
                  AND t.week = rt.trans_week
                  AND {self._db_filter('t')}
            """
            rows += self._execute(sql_ros_total, "player_to_transactions: ROS total points")

        # ---- Step 2: ROS managed (while on THIS manager's roster) ----
        set_managed = []
        if "total_points_ros_managed" in trans_cols:
            set_managed.append("total_points_ros_managed = rm.total_points_ros_managed")
        if "ppg_ros_managed" in trans_cols:
            set_managed.append("ppg_ros_managed = rm.ppg_ros_managed")
        if "weeks_ros_managed" in trans_cols:
            set_managed.append("weeks_ros_managed = rm.weeks_ros_managed")
        if has_replacement_ppg and "replacement_ppg_ros_managed" in trans_cols:
            set_managed.append("replacement_ppg_ros_managed = rm.replacement_ppg_ros_managed")

        if set_managed:
            add_like_predicate = _add_like_predicate("t")
            replacement_managed_select = ""
            if has_replacement_ppg and "replacement_ppg_ros_managed" in trans_cols:
                replacement_managed_select = (
                    ",\n                    AVG(p.replacement_ppg) AS replacement_ppg_ros_managed"
                )

            # Initialize managed columns to 0 for all adds BEFORE the main UPDATE.
            # Taxi-squad / injured / suspended adds where the player has no
            # player_fantasy rows after the transaction week would otherwise stay
            # NULL (INNER JOIN yields nothing). 0 is the correct default — the
            # manager added the player and got 0 points from him.
            init_managed_cols = []
            if "total_points_ros_managed" in trans_cols:
                init_managed_cols.append("total_points_ros_managed = 0")
            if "ppg_ros_managed" in trans_cols:
                init_managed_cols.append("ppg_ros_managed = 0")
            if "weeks_ros_managed" in trans_cols:
                init_managed_cols.append("weeks_ros_managed = 0")
            if init_managed_cols:
                sql_init_ros_managed = f"""
                    UPDATE {trans_table}
                    SET {', '.join(init_managed_cols)}
                    WHERE {self._db_filter()}
                      AND {add_like_predicate.replace('t.', '')}
                """
                rows += self._execute(sql_init_ros_managed, "player_to_transactions: init ROS managed to 0 for adds")

            settings_table = self._qualified_name("league_settings")
            # Points and LAMAR must use the exact same ownership windows.
            sql_ros_managed = f"""
                WITH {self._managed_roster_periods_sql(join_key)},
                managed_calc AS (
                    SELECT
                        p._acquisition_id AS transaction_id,
                        p.{join_key},
                        p.franchise_id,
                        SUM(p.fantasy_points) AS total_points_ros_managed,
                        AVG(p.fantasy_points) AS ppg_ros_managed,
                        COUNT(*) AS weeks_ros_managed{replacement_managed_select}
                    FROM managed_player_weeks p
                    GROUP BY p._acquisition_id, p.{join_key}, p.franchise_id
                )
                UPDATE {trans_table} t
                SET {', '.join(set_managed)}
                FROM managed_calc rm
                WHERE t.transaction_id = rm.transaction_id
                  AND t.{join_key} = rm.{join_key}
                  AND t.franchise_id = rm.franchise_id
                  AND {self._db_filter('t')}
            """
            rows += self._execute(sql_ros_managed, "player_to_transactions: ROS managed points")

            # Zero out managed metrics for drops (player leaving, no managed value)
            drop_set = []
            if "total_points_ros_managed" in trans_cols:
                drop_set.append("total_points_ros_managed = 0")
            if "ppg_ros_managed" in trans_cols:
                drop_set.append("ppg_ros_managed = 0")
            if "weeks_ros_managed" in trans_cols:
                drop_set.append("weeks_ros_managed = 0")
            if has_replacement_ppg and "replacement_ppg_ros_managed" in trans_cols:
                drop_set.append("replacement_ppg_ros_managed = NULL")

            sql_drop_zero = f"""
                UPDATE {trans_table}
                SET {', '.join(drop_set)}
                WHERE {self._db_filter()}
                  AND (
                      transaction_type = 'drop'
                      OR (transaction_type IN ('trade', 'trade_pick') AND trade_direction = 'sent')
                  )
            """
            rows += self._execute(sql_drop_zero, "player_to_transactions: zero managed metrics for drops")

        return rows

    # =========================================================================
    # TRANSACTION LAMAR ROS: Calculate rest-of-season LAMAR for transactions
    # =========================================================================

    def transaction_lamar_ros(self) -> int:
        """Calculate rest-of-season LAMAR metrics for transactions.

        For each transaction, calculates:
        - fa_lamar_ros: Sum of weekly LAMAR from transaction week to end of season
        - player_lamar_ros_total: Total ROS LAMAR regardless of roster
        - manager_lamar_ros_managed: LAMAR while under this manager's control

        These metrics enable transaction report cards and ROI analysis.
        """
        if not self._table_exists("player_fantasy"):
            logger.warning("[transaction_lamar_ros] player_fantasy table not found")
            return 0

        if not self._table_exists("transactions"):
            logger.warning("[transaction_lamar_ros] transactions table not found")
            return 0

        player_cols = self._get_table_columns("player_fantasy")
        trans_table = self._qualified_name("transactions")
        player_table = self._qualified_name("player_fantasy")
        conn = self._get_connection()

        # Check if player_fantasy has LAMAR calculated
        if "manager_lamar" not in player_cols and "player_lamar" not in player_cols:
            logger.warning("[transaction_lamar_ros] No LAMAR columns in player_fantasy - run calculate_lamar first")
            return 0

        # Use different LAMAR sources for total-vs-managed windows:
        # - total_lamar_col: player_lamar captures full player value regardless of roster
        # - managed_lamar_col: manager_lamar captures lineup value while on a manager's team
        #
        # Drops/regret need the total player value after the move, not manager_lamar.
        total_lamar_col = "player_lamar" if "player_lamar" in player_cols else "manager_lamar"
        managed_lamar_col = "manager_lamar" if "manager_lamar" in player_cols else "player_lamar"

        join_key = "NFL_player_id"
        logger.info(f"[transaction_lamar_ros] Using join key: {join_key}")

        rows_updated = 0

        # Log diagnostic info about the join
        try:
            diag = conn.execute(f"""
                SELECT COUNT(*) as trans_count,
                       COUNT(DISTINCT t.{join_key}) as unique_players,
                       SUM(CASE WHEN p.{join_key} IS NOT NULL THEN 1 ELSE 0 END) as matched_count
                FROM {trans_table} t
                LEFT JOIN {player_table} p
                    ON p.{join_key} = t.{join_key}
                    AND p.year = t.year
                    AND {self._db_filter('p')}
                WHERE t.{join_key} IS NOT NULL
                  AND {self._db_filter('t')}
            """).fetchone()
            logger.info(
                f"[transaction_lamar_ros] Diagnostics: {diag[0]} transactions, {diag[1]} unique players, {diag[2]} matched to player_fantasy"
            )
        except Exception as e:
            logger.warning(f"[transaction_lamar_ros] Diagnostic query failed: {e}")

        # Step 1: Calculate fa_lamar_ros (rest-of-season LAMAR from transaction week)
        # This represents what the player could contribute to ANY manager from this point forward
        # NOTE: Uses end_week from league_settings to cap at the fantasy regular season end
        #
        # IMPORTANT: First get unique (player, year, week) combinations from transactions,
        # then calculate LAMAR ROS for each. This avoids multiplication when multiple transactions
        # exist for the same player in the same week (common in guillotine leagues where many
        # managers claim the same dropped player).
        settings_table = self._qualified_name("league_settings")
        add_like_predicate = _add_like_predicate("t")

        # Initialize only missing ROS values before the main UPDATE. New rows
        # with no player window correctly become 0. Hydrated rows keep their
        # prior finalized value until a matching recalculation below replaces
        # it (including a legitimate corrected zero).
        init_ros_cols = []
        if "fa_lamar_ros" in self._get_table_columns("transactions"):
            init_ros_cols.append("fa_lamar_ros = COALESCE(fa_lamar_ros, 0)")
        if "player_lamar_ros_total" in self._get_table_columns("transactions"):
            init_ros_cols.append("player_lamar_ros_total = COALESCE(player_lamar_ros_total, 0)")
        if init_ros_cols:
            sql_init_fa_ros = f"""
                UPDATE {trans_table}
                SET {', '.join(init_ros_cols)}
                WHERE {self._db_filter()}
                  AND {add_like_predicate.replace('t.', '')}
            """
            rows_updated += self._execute(
                sql_init_fa_ros, "transaction_lamar_ros: init fa_lamar_ros/player_lamar_ros_total to 0 for adds"
            )
        sql_ros = f"""
            WITH league_end_weeks AS (
                -- Get end_week for each year from league_settings
                SELECT
                    CAST(year AS INTEGER) as year,
                    COALESCE(
                        end_week,
                        18  -- Fallback to week 18 if not specified
                    ) as end_week
                FROM {settings_table}
                WHERE {self._db_filter()}
            ),
            -- Get unique (player, year, week) combinations from transactions
            -- This deduplicates before the expensive LAMAR calculation
            unique_trans_weeks AS (
                SELECT DISTINCT
                    {join_key},
                    year,
                    cumulative_week
                FROM {trans_table} t
                WHERE {join_key} IS NOT NULL
                  AND {self._db_filter('t')}
            ),
            -- Calculate ROS LAMAR for each unique player/year/week combination
            ros_lamar AS (
                SELECT
                    utw.{join_key},
                    utw.year,
                    utw.cumulative_week as trans_week,
                    SUM(COALESCE(p.{total_lamar_col}, 0)) as total_lamar_ros
                FROM unique_trans_weeks utw
                INNER JOIN {player_table} p
                    ON p.{join_key} = utw.{join_key}
                    AND p.year = utw.year
                    AND p.cumulative_week >= utw.cumulative_week
                LEFT JOIN league_end_weeks lew
                    ON CAST(p.year AS INTEGER) = lew.year
                WHERE p.{join_key} IS NOT NULL
                  AND {self._db_filter('p')}
                  AND p.week <= COALESCE(lew.end_week, 18)  -- Cap at league's end_week
                GROUP BY utw.{join_key}, utw.year, utw.cumulative_week
            )
            UPDATE {trans_table} t
            SET
                fa_lamar_ros = r.total_lamar_ros,
                player_lamar_ros_total = r.total_lamar_ros
            FROM ros_lamar r
            WHERE t.{join_key} = r.{join_key}
              AND t.year = r.year
              AND t.cumulative_week = r.trans_week
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_ros, "transaction_lamar_ros: ROS LAMAR")

        # Step 2: Calculate manager_lamar_ros = LAMAR for weeks player was on THIS manager's roster
        # For adds: Sum LAMAR from add week until next drop/trade by same manager (or end of season)
        # For drops: 0 (player leaving, no value to manager)
        # For trades: 0 for sender, calculated for receiver
        #
        # Initialize only missing managed values. This still gives new
        # same-week add/drop rows a 0 default without erasing a finalized value
        # merely because the current partial-week window has no player row.
        sql_init_managed = f"""
            UPDATE {trans_table}
            SET manager_lamar_ros_managed = COALESCE(manager_lamar_ros_managed, 0)
            WHERE {self._db_filter()}
              AND {add_like_predicate.replace('t.', '')}
        """
        rows_updated += self._execute(
            sql_init_managed, "transaction_lamar_ros: init managed LAMAR to 0 for adds/trades"
        )

        # Use the same acquisition windows and scoring ownership as ROS points.
        sql_manager_lamar = f"""
            WITH {self._managed_roster_periods_sql(join_key)},
            manager_lamar_calc AS (
                -- Sum LAMAR only for weeks the player was on this manager's roster
                -- NOTE: Caps at league's end_week from settings
                -- GROUP BY both transaction_id AND player key: trades have multiple
                -- players sharing one transaction_id, each needs individual LAMAR
                SELECT
                    p._acquisition_id AS transaction_id,
                    p.{join_key},
                    p.franchise_id,
                    SUM(COALESCE(p.{managed_lamar_col}, 0)) as manager_lamar
                FROM managed_player_weeks p
                GROUP BY p._acquisition_id, p.{join_key}, p.franchise_id
            )
            UPDATE {trans_table} t
            SET manager_lamar_ros_managed = COALESCE(m.manager_lamar, 0)
            FROM manager_lamar_calc m
            WHERE t.transaction_id = m.transaction_id
              AND t.{join_key} = m.{join_key}
              AND t.franchise_id = m.franchise_id
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_manager_lamar, "transaction_lamar_ros: manager_lamar for roster period")

        # Step 3: Set manager_lamar_ros = 0 for drops (player leaving, no value to manager)
        sql_drops = f"""
            UPDATE {trans_table}
            SET manager_lamar_ros_managed = 0
            WHERE {self._db_filter()}
              AND (
                  transaction_type = 'drop'
                  OR (transaction_type IN ('trade', 'trade_pick') AND trade_direction = 'sent')
              )
        """
        rows_updated += self._execute(sql_drops, "transaction_lamar_ros: zero manager_lamar for drops")

        return rows_updated

    def transaction_score(self) -> int:
        """Calculate transaction_score as a 100-centered quality index.

        Raw scoring ingredients:
        - Adds: manager_lamar_ros_managed (started LAMAR during managed window)
        - Drops: -fa_lamar_ros (negated regret; positive = good drop)
        - Trades: NULL (trade packages use dedicated trade_* scoring)

        The stored transaction_score is normalized by move family within the
        league-season so 100 is the median add/drop and each 15 points is
        roughly one standard deviation. Raw value accounting remains in the
        existing LAMAR columns and trade_net_lamar.

        Grading (PERCENT_RANK over normalized score, separate curves for adds/drops):
        - >= 85th percentile: A
        - >= 65th percentile: B
        - >= 35th percentile: C
        - >= 15th percentile: D
        - < 15th percentile: F
        """
        if not self._table_exists("transactions"):
            logger.warning("[transaction_score] transactions table not found")
            return 0

        trans_cols = self._get_table_columns("transactions")
        trans_table = self._qualified_name("transactions")

        # managed_col: LAMAR extracted by THIS manager (for scoring adds/trade-receivers)
        managed_col = None
        for col in ["manager_lamar_ros_managed", "fa_lamar_ros"]:
            if col in trans_cols:
                managed_col = col
                break

        # regret_col: Total ROS LAMAR after drop/give-up (for scoring drops/trade-senders)
        regret_col = None
        for col in ["fa_lamar_ros", "player_lamar_ros_total"]:
            if col in trans_cols:
                regret_col = col
                break

        if managed_col is None and regret_col is None:
            logger.warning("[transaction_score] No LAMAR ROS column found - run transaction_lamar_ros first")
            return 0

        if managed_col is None:
            managed_col = regret_col
        if regret_col is None:
            regret_col = managed_col

        rows_updated = 0

        # Check for ESPN estimated trade handling
        _has_estimated = "is_estimated" in trans_cols
        _has_source_dest = "source_type" in trans_cols and "destination" in trans_cols

        # Step 1: Calculate transaction_score as raw LAMAR first. This is a
        # transient value used by retained-value extension and then normalized.
        # Adds: manager_lamar_ros_managed (what the player contributed to your lineup)
        # Drops: -fa_lamar_ros (negated post-drop production = regret)
        # Trades: NULL here; trade packages are scored via dedicated trade_* columns
        sql_score = f"""
            UPDATE {trans_table} t
            SET transaction_score = CASE
                    WHEN t.transaction_type IN ('trade', 'trade_pick') THEN NULL
                    WHEN t.transaction_type = 'drop' THEN -COALESCE(t.{regret_col}, 0)
                    WHEN t.transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN COALESCE(t.{managed_col}, 0)
                    ELSE NULL
                END
            WHERE {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_score, "transaction_score: calculate raw LAMAR scores")

        # Step 1b: Extend scores across years for keeper/dynasty leagues
        rows_updated += self._extend_retained_value("transaction_score", _non_trade_add_predicate("t"))

        # Step 2: Normalize add/drop scores to a comparable 100-centered index.
        rows_updated += self._normalize_transaction_score(trans_table)

        # Step 3: Clear all grades
        sql_clear = f"""
            UPDATE {trans_table}
            SET transaction_grade = NULL, score_percentile = NULL
            WHERE {self._db_filter()}
        """
        rows_updated += self._execute(sql_clear, "transaction_score: clear grades")

        # Step 4: Grade adds by percentile (separate curve)
        rows_updated += self._grade_transactions_by_type(
            trans_table, f"transaction_type IN {_ADD_TRANSACTION_TYPES_SQL}"
        )

        # Step 5: Grade drops by percentile (separate curve)
        # Drop scores are already normalized with higher = better.
        rows_updated += self._grade_transactions_by_type(trans_table, "transaction_type = 'drop'")

        # Step 6: Compute trade package net LAMAR and grade packages
        rows_updated += self._compute_trade_net_lamar()

        return rows_updated

    def _normalize_transaction_score(self, trans_table: str) -> int:
        """Convert raw add/drop LAMAR transaction_score values to a 100 index.

        Cohorts are league-season plus move family, so the median add and
        median drop in each season score 100.
        """
        sql = f"""
            WITH scored AS (
                SELECT
                    rowid as rid,
                    year,
                    CASE
                        WHEN transaction_type IN {_ADD_TRANSACTION_TYPES_SQL} THEN 'add'
                        WHEN transaction_type = 'drop' THEN 'drop'
                        ELSE NULL
                    END as score_family,
                    CAST(transaction_score AS DOUBLE) as raw_score
                FROM {trans_table}
                WHERE transaction_score IS NOT NULL
                  AND transaction_type IN {_ADD_DROP_TRANSACTION_TYPES_SQL}
                  AND {self._db_filter()}
            ),
            cohort_stats AS (
                SELECT
                    year,
                    score_family,
                    MEDIAN(raw_score) as median_score,
                    STDDEV_SAMP(raw_score) as std_score,
                    COUNT(*) as score_count
                FROM scored
                WHERE score_family IS NOT NULL
                GROUP BY year, score_family
            ),
            normalized AS (
                SELECT
                    s.rid,
                    CASE
                        WHEN cs.score_count <= 1 THEN 100.0
                        WHEN COALESCE(cs.std_score, 0) = 0 THEN 100.0
                        ELSE ROUND(
                            100.0 + 15.0 * ((s.raw_score - cs.median_score) / GREATEST(cs.std_score, 1.0)),
                            1
                        )
                    END as normalized_score
                FROM scored s
                JOIN cohort_stats cs ON s.year = cs.year AND s.score_family = cs.score_family
            )
            UPDATE {trans_table} t
            SET transaction_score = n.normalized_score
            FROM normalized n
            WHERE t.rowid = n.rid
              AND {self._db_filter('t')}
        """
        return self._execute(sql, "transaction_score: normalize to 100-centered index")

    def _grade_transactions_by_type(self, trans_table, type_filter_sql, only_ungraded=False):
        """Grade transactions by percentile across all years for a specific type filter."""
        # Minimum pool size guard — skip grading if fewer than 30 qualifying transactions
        count_sql = f"""
            SELECT COUNT(*) FROM {trans_table}
            WHERE transaction_score IS NOT NULL
              AND {self._db_filter()}
              AND {type_filter_sql}
        """
        pool_size = self._get_connection().execute(count_sql).fetchone()[0]
        if pool_size < 30:
            logger.warning(f"[transaction_grade] Only {pool_size} graded transactions for {type_filter_sql}, skipping")
            return 0

        guard = "AND t.transaction_grade IS NULL" if only_ungraded else ""
        sql = f"""
            UPDATE {trans_table} t
            SET
                score_percentile = sub.pctile,
                transaction_grade = CASE
                    WHEN sub.pctile >= 95 THEN 'A+'
                    WHEN sub.pctile >= 85 THEN 'A'
                    WHEN sub.pctile >= 75 THEN 'A-'
                    WHEN sub.pctile >= 65 THEN 'B+'
                    WHEN sub.pctile >= 50 THEN 'B'
                    WHEN sub.pctile >= 35 THEN 'B-'
                    WHEN sub.pctile >= 20 THEN 'C'
                    WHEN sub.pctile >= 10 THEN 'D'
                    ELSE 'F'
                END
            FROM (
                SELECT
                    rowid as rid,
                    PERCENT_RANK() OVER (
                        ORDER BY transaction_score ASC
                    ) * 100 as pctile
                FROM {trans_table}
                WHERE transaction_score IS NOT NULL
                  AND {self._db_filter()}
                  AND {type_filter_sql}
            ) sub
            WHERE t.rowid = sub.rid
              AND {self._db_filter('t')}
            {guard}
        """
        return self._execute(sql, f"transaction_score: grade ({type_filter_sql})")

    def _extend_retained_value(self, target_col: str, qualifying_predicate: str) -> int:
        """Extend a retained-value column by league-specific asset horizon.

        This is used for add/claim/pickup rows (`transaction_score`) and
        trade-received rows (`trade_asset_lamar`). Redraft assets are valued for
        the rest of the season, keeper assets for years explicitly kept, and
        dynasty assets while the same franchise still owns the player.

        Chain break conditions (any one ends the extension):
        1. Transaction departure: player is dropped or traded away by the franchise
        2. Keeper mode: the player is not marked as kept in the future year
        3. No roster presence: player has no rows in player_fantasy for the franchise

        Trade-sent rows and drop rows are not extended here because they are
        scored through a different pathway.
        """
        # Check required tables
        if not self._table_exists("transactions"):
            return 0
        if not self._table_exists("player_fantasy"):
            return 0

        trans_cols = self._get_table_columns("transactions")
        player_cols = self._get_table_columns("player_fantasy")
        trans_table = self._qualified_name("transactions")
        player_table = self._qualified_name("player_fantasy")

        # franchise_id is required for ownership chain tracking
        if "franchise_id" not in trans_cols or "franchise_id" not in player_cols:
            raise KeyError("franchise_id is required for manager identity")

        if target_col not in trans_cols:
            logger.info(f"[_extend_retained_value] {target_col} column missing - skipping")
            return 0

        join_key = "NFL_player_id"
        if "cumulative_week" not in trans_cols or "cumulative_week" not in player_cols:
            logger.info("[_extend_retained_value] cumulative_week missing - future retained value skipped")
            return 0

        # Determine LAMAR column in player_fantasy
        lamar_col = "manager_lamar" if "manager_lamar" in player_cols else "player_lamar"
        if lamar_col not in player_cols:
            logger.warning("[_extend_retained_value] No LAMAR column in player_fantasy")
            return 0

        # Determine keeper column in draft table (if draft exists)
        has_draft = self._table_exists("draft")
        keeper_col = None
        draft_table = None
        if has_draft:
            draft_table = self._qualified_name("draft")
            draft_cols = self._get_table_columns("draft")
            if "is_keeper" in draft_cols:
                keeper_col = "is_keeper"
            elif "is_keeper_status" in draft_cols:
                keeper_col = "is_keeper_status"
            # Join key between draft and transactions — use NFL_player_id
            draft_join_key = (
                "NFL_player_id" if "NFL_player_id" in draft_cols and "NFL_player_id" in trans_cols else None
            )

        logger.info(
            f"[_extend_retained_value] target_col={target_col}, join_key={join_key}, "
            f"lamar_col={lamar_col}, has_draft={has_draft}, keeper_col={keeper_col}"
        )

        settings_table = self._qualified_name("league_settings") if self._table_exists("league_settings") else None
        settings_cols = self._get_table_columns("league_settings") if settings_table else set()
        mode_cases: list[str] = []
        if "draft_type" in settings_cols:
            mode_cases.extend(
                [
                    "WHEN LOWER(COALESCE(CAST(s.draft_type AS VARCHAR), '')) = 'dynasty' THEN 'dynasty'",
                    "WHEN LOWER(COALESCE(CAST(s.draft_type AS VARCHAR), '')) = 'keeper' THEN 'keeper'",
                ]
            )
        if "league_type" in settings_cols:
            mode_cases.extend(
                [
                    "WHEN LOWER(COALESCE(CAST(s.league_type AS VARCHAR), '')) LIKE '%dynasty%' THEN 'dynasty'",
                    "WHEN LOWER(COALESCE(CAST(s.league_type AS VARCHAR), '')) LIKE '%keeper%' THEN 'keeper'",
                ]
            )
        if "max_keepers" in settings_cols:
            mode_cases.append("WHEN COALESCE(TRY_CAST(s.max_keepers AS INTEGER), 0) > 0 THEN 'keeper'")

        if settings_table and mode_cases:
            league_modes_cte = f"""
            league_modes AS (
                SELECT
                    CAST(s.year AS INTEGER) AS year,
                    CASE
                        {' '.join(mode_cases)}
                        ELSE 'redraft'
                    END AS retention_mode
                FROM {settings_table} s
                WHERE {self._db_filter('s')}
            ),"""
        else:
            league_modes_cte = """
            league_modes AS (
                SELECT y AS year, 'redraft' AS retention_mode
                FROM all_years
            ),"""

        keeper_row_parts: list[str] = []
        if "keeper_year" in player_cols:
            keeper_row_parts.append("COALESCE(TRY_CAST(p.keeper_year AS INTEGER), 0) > 0")
        if "is_keeper" in player_cols:
            keeper_row_parts.append("COALESCE(TRY_CAST(p.is_keeper AS INTEGER), 0) > 0")

        keeper_events_cte = ""
        if not keeper_row_parts and has_draft and keeper_col and draft_join_key:
            keeper_events_cte = f"""
            keeper_events AS (
                SELECT
                    d.{draft_join_key} AS player_key,
                    d.franchise_id,
                    CAST(d.year AS INTEGER) AS year
                FROM {draft_table} d
                WHERE COALESCE(TRY_CAST(d.{keeper_col} AS INTEGER), 0) > 0
                  AND {self._db_filter('d')}
                  AND d.{draft_join_key} IS NOT NULL
                  AND d.franchise_id IS NOT NULL
            ),"""
            keeper_row_parts.append(
                """
                EXISTS (
                    SELECT 1
                    FROM keeper_events ke
                    WHERE ke.player_key = qt.player_key
                      AND ke.franchise_id = qt.franchise_id
                      AND ke.year = future_yr.y
                )
                """
            )
        keeper_row_condition = " OR ".join(keeper_row_parts) if keeper_row_parts else "FALSE"
        sent_departure_predicate = (
            "t.transaction_type IN ('trade', 'trade_pick') AND t.trade_direction = 'sent'"
            if "trade_direction" in trans_cols
            else "FALSE"
        )

        extension_seed_predicate = f"ABS(COALESCE(t.{target_col}, 0)) >= 3"
        if target_col == "trade_asset_lamar" and "is_conveyed" in trans_cols:
            # Future picks can have zero same-year value at the time they were
            # traded, then convey into a dynasty asset later. Let those rows
            # enter the future-year extension even when the seed is zero.
            extension_seed_predicate = f"""
                (
                    {extension_seed_predicate}
                    OR (
                        t.transaction_type = 'trade_pick'
                        AND COALESCE(TRY_CAST(t.is_conveyed AS BOOLEAN), FALSE)
                    )
                )
            """

        sql_retained_value = f"""
            WITH all_years AS (
                SELECT DISTINCT CAST(year AS INTEGER) AS y
                FROM {player_table}
                WHERE {self._db_filter()}
            ),
            {league_modes_cte}
            {keeper_events_cte}
            qualifying_transactions AS (
                SELECT
                    t.rowid AS rid,
                    t.{join_key} AS player_key,
                    t.franchise_id,
                    CAST(t.year AS INTEGER) AS trans_year,
                    t.cumulative_week AS trans_week,
                    COALESCE(lm.retention_mode, 'redraft') AS retention_mode
                FROM {trans_table} t
                LEFT JOIN league_modes lm
                    ON lm.year = CAST(t.year AS INTEGER)
                WHERE {qualifying_predicate}
                  AND {self._db_filter('t')}
                  AND t.{join_key} IS NOT NULL
                  AND t.franchise_id IS NOT NULL
                  AND t.cumulative_week IS NOT NULL
                  AND {extension_seed_predicate}
            ),
            departure_events AS (
                SELECT
                    t.{join_key} AS player_key,
                    t.franchise_id,
                    t.cumulative_week
                FROM {trans_table} t
                WHERE (
                    t.transaction_type = 'drop'
                    OR ({sent_departure_predicate})
                )
                  AND {self._db_filter('t')}
                  AND t.{join_key} IS NOT NULL
                  AND t.franchise_id IS NOT NULL
                  AND t.cumulative_week IS NOT NULL
            ),
            first_departure AS (
                SELECT
                    qt.rid,
                    MIN(de.cumulative_week) AS departure_week
                FROM qualifying_transactions qt
                INNER JOIN departure_events de
                    ON de.player_key = qt.player_key
                    AND de.franchise_id = qt.franchise_id
                    AND de.cumulative_week > qt.trans_week
                GROUP BY qt.rid
            ),
            future_year_lamar AS (
                SELECT
                    qt.rid,
                    qt.player_key,
                    qt.franchise_id,
                    qt.trans_year,
                    future_yr.y AS ext_year,
                    SUM(COALESCE(p.{lamar_col}, 0)) AS year_lamar
                FROM qualifying_transactions qt
                CROSS JOIN all_years future_yr
                INNER JOIN {player_table} p
                    ON p.{join_key} = qt.player_key
                    AND CAST(p.year AS INTEGER) = future_yr.y
                    AND p.franchise_id = qt.franchise_id
                LEFT JOIN first_departure fd
                    ON fd.rid = qt.rid
                WHERE future_yr.y > qt.trans_year
                  AND p.cumulative_week > qt.trans_week
                  AND (fd.departure_week IS NULL OR p.cumulative_week < fd.departure_week)
                  AND {self._db_filter('p')}
                  AND (
                      qt.retention_mode = 'dynasty'
                      OR (qt.retention_mode = 'keeper' AND ({keeper_row_condition}))
                  )
                GROUP BY qt.rid, qt.player_key, qt.franchise_id, qt.trans_year, future_yr.y
                HAVING SUM(COALESCE(p.{lamar_col}, 0)) IS NOT NULL
            ),
            first_gap AS (
                SELECT
                    qt.rid,
                    MIN(ay.y) AS gap_year
                FROM qualifying_transactions qt
                CROSS JOIN all_years ay
                WHERE ay.y > qt.trans_year
                  AND NOT EXISTS (
                      SELECT 1
                      FROM future_year_lamar fyl
                      WHERE fyl.rid = qt.rid
                        AND fyl.ext_year = ay.y
                  )
                GROUP BY qt.rid
            ),
            extended_lamar AS (
                SELECT
                    fyl.rid,
                    SUM(fyl.year_lamar) AS extra_lamar
                FROM future_year_lamar fyl
                LEFT JOIN first_gap fg ON fg.rid = fyl.rid
                WHERE fg.gap_year IS NULL
                   OR fyl.ext_year < fg.gap_year
                GROUP BY fyl.rid
            )
            UPDATE {trans_table} t
            SET {target_col} = t.{target_col} + el.extra_lamar
            FROM extended_lamar el
            WHERE t.rowid = el.rid
              AND {self._db_filter('t')}
              AND el.extra_lamar != 0
        """

        return self._execute(
            sql_retained_value,
            f"_extend_retained_value: extend {target_col} by league retention mode",
        )

    def _compute_trade_net_lamar(self) -> int:
        """Compute package-level trade grades (trade_net_lamar, trade_grade, trade_percentile).

        For each trade or trade_pick (identified by transaction_id):
        1. Seed received rows with realized managed value for acquired assets
        2. Extend that value by redraft/keeper/dynasty asset horizon
        3. Mirror the acquired-side value onto the matching sent rows
        4. Sum package net per (transaction_id, franchise_id)
        5. Grade trade packages on an all-time percentile curve

        Trivial packages (|trade_net_lamar| < 3) get NULL grade/percentile.
        """
        if not self._table_exists("transactions"):
            return 0

        trans_cols = self._get_table_columns("transactions")
        trans_table = self._qualified_name("transactions")

        required = {
            "transaction_id",
            "franchise_id",
            "source_franchise_id",
            "trade_asset_lamar",
            "trade_net_lamar",
            "trade_grade",
            "trade_percentile",
        }
        if not required.issubset(trans_cols):
            missing = required - trans_cols
            logger.warning(f"[_compute_trade_net_lamar] Missing columns: {missing}")
            return 0

        rows_updated = 0
        trade_received_predicate = _trade_received_predicate("t")
        asset_key_t = _trade_asset_key_expr(trans_cols, "t")
        asset_key_r = _trade_asset_key_expr(trans_cols, "r")
        managed_trade_col = next(
            (
                col
                for col in ["manager_lamar_ros_managed", "player_lamar_ros_managed", "player_lamar_ros_total"]
                if col in trans_cols
            ),
            None,
        )
        if managed_trade_col is None:
            logger.warning("[_compute_trade_net_lamar] No trade asset LAMAR source column found")
            return 0

        # Step 1: Clear existing trade package columns
        sql_clear = f"""
            UPDATE {trans_table}
            SET trade_asset_lamar = NULL,
                trade_net_lamar = NULL,
                trade_grade = NULL,
                trade_percentile = NULL
            WHERE {self._db_filter()}
        """
        rows_updated += self._execute(sql_clear, "trade_net_lamar: clear existing values")

        sql_received = f"""
            UPDATE {trans_table} t
            SET trade_asset_lamar = COALESCE(t.{managed_trade_col}, 0)
            WHERE {trade_received_predicate}
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_received, "trade_net_lamar: seed received-side trade asset values")

        rows_updated += self._extend_retained_value("trade_asset_lamar", trade_received_predicate)

        sql_sent = f"""
            WITH received_trade_assets AS (
                SELECT
                    r.transaction_id,
                    r.year,
                    r.franchise_id as receiving_franchise_id,
                    r.source_franchise_id as sending_franchise_id,
                    {asset_key_r} as asset_key,
                    MAX(COALESCE(r.trade_asset_lamar, 0)) as asset_lamar
                FROM {trans_table} r
                WHERE {trade_received_predicate.replace("t.", "r.")}
                  AND {self._db_filter('r')}
                  AND r.transaction_id IS NOT NULL
                  AND r.franchise_id IS NOT NULL
                GROUP BY
                    r.transaction_id,
                    r.year,
                    r.franchise_id,
                    r.source_franchise_id,
                    {asset_key_r}
            )
            UPDATE {trans_table} t
            SET trade_asset_lamar = rta.asset_lamar
            FROM received_trade_assets rta
            WHERE t.transaction_type IN ('trade', 'trade_pick')
              AND t.trade_direction = 'sent'
              AND t.transaction_id = rta.transaction_id
              AND t.year = rta.year
              AND t.franchise_id = rta.sending_franchise_id
              AND COALESCE(t.source_franchise_id, '') = COALESCE(rta.receiving_franchise_id, '')
              AND {asset_key_t} = rta.asset_key
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_sent, "trade_net_lamar: mirror received asset values onto sent rows")

        # Step 2: Compute trade_net_lamar per franchise side
        sql_net = f"""
            WITH trade_package_scores AS (
                SELECT
                    transaction_id,
                    franchise_id,
                    SUM(
                        CASE
                            WHEN trade_direction = 'received' THEN COALESCE(trade_asset_lamar, 0)
                            WHEN trade_direction = 'sent' THEN -COALESCE(trade_asset_lamar, 0)
                            ELSE 0
                        END
                    ) AS net_lamar
                FROM {trans_table}
                WHERE transaction_type IN ('trade', 'trade_pick')
                  AND {self._db_filter()}
                  AND transaction_id IS NOT NULL
                  AND franchise_id IS NOT NULL
                GROUP BY transaction_id, franchise_id
            )
            UPDATE {trans_table} t
            SET trade_net_lamar = tps.net_lamar
            FROM trade_package_scores tps
            WHERE t.transaction_id = tps.transaction_id
              AND t.franchise_id = tps.franchise_id
              AND t.transaction_type IN ('trade', 'trade_pick')
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_net, "trade_net_lamar: compute package net LAMAR")

        # Step 6c: Grade trade packages on all-time percentile curve
        # One percentile entry per (transaction_id, franchise_id) to avoid
        # multi-player packages getting extra weight. Then broadcast to all rows.

        # Minimum pool size guard — skip grading if fewer than 30 qualifying trade packages
        trade_pool_sql = f"""
            SELECT COUNT(DISTINCT transaction_id || '|' || franchise_id)
            FROM {trans_table}
            WHERE transaction_type IN ('trade', 'trade_pick')
              AND {self._db_filter()}
              AND trade_net_lamar IS NOT NULL
              AND ABS(trade_net_lamar) >= 3
        """
        trade_pool_size = self._get_connection().execute(trade_pool_sql).fetchone()[0]
        if trade_pool_size < 30:
            logger.warning(f"[transaction_grade] Only {trade_pool_size} graded trade packages, skipping trade grade")
        else:
            sql_grade = f"""
                WITH trade_packages AS (
                    SELECT
                        transaction_id,
                        franchise_id,
                        trade_net_lamar
                    FROM {trans_table}
                    WHERE transaction_type IN ('trade', 'trade_pick')
                      AND {self._db_filter()}
                      AND trade_net_lamar IS NOT NULL
                      AND ABS(trade_net_lamar) >= 3
                    GROUP BY transaction_id, franchise_id, trade_net_lamar
                ),
                package_percentiles AS (
                    SELECT
                        transaction_id,
                        franchise_id,
                        PERCENT_RANK() OVER (ORDER BY trade_net_lamar ASC) * 100 as pctile
                    FROM trade_packages
                )
                UPDATE {trans_table} t
                SET
                    trade_percentile = pp.pctile,
                    trade_grade = CASE
                        WHEN pp.pctile >= 95 THEN 'A+'
                        WHEN pp.pctile >= 85 THEN 'A'
                        WHEN pp.pctile >= 75 THEN 'A-'
                        WHEN pp.pctile >= 65 THEN 'B+'
                        WHEN pp.pctile >= 50 THEN 'B'
                        WHEN pp.pctile >= 35 THEN 'B-'
                        WHEN pp.pctile >= 20 THEN 'C'
                        WHEN pp.pctile >= 10 THEN 'D'
                        ELSE 'F'
                    END
                FROM package_percentiles pp
                WHERE t.transaction_id = pp.transaction_id
                  AND t.franchise_id = pp.franchise_id
                  AND t.transaction_type IN ('trade', 'trade_pick')
                  AND {self._db_filter('t')}
            """
            rows_updated += self._execute(sql_grade, "trade_net_lamar: grade trade packages")

        return rows_updated

    # =========================================================================
    # TRANSACTION ENGAGEMENT METRICS: UI-friendly labels, tiers, percentiles
    # =========================================================================

    # =========================================================================
    # FIX UNKNOWN MANAGERS: Backfill NULL/Unknown manager names from nearest add
    # =========================================================================

    def fix_unknown_managers(self) -> int:
        """Backfill 'Unknown' manager names from nearest add transaction.
        Also backfills NULL positions from player_fantasy.

        Step 1: For transactions where manager IS NULL or 'unknown'/'', find the
        closest transaction (by timestamp or cumulative_week) for the same
        NFL_player_id in the same year where manager IS known. Copy that
        manager/franchise_id/team_name.

        Step 2: Backfill NULL positions from player_fantasy (distinct
        NFL_player_id -> position).

        Raises:
            KeyError: if the transactions table does not contain a franchise_id column.
        """
        if not self._table_exists("transactions"):
            logger.warning("[fix_unknown_managers] transactions table not found")
            return 0

        trans_cols = self._get_table_columns("transactions")
        trans_table = self._qualified_name("transactions")

        if "manager" not in trans_cols:
            logger.warning("[fix_unknown_managers] manager column not found in transactions")
            return 0

        rows_updated = 0

        # --- Step 1: Backfill unknown managers from nearest known transaction ---
        player_key = "NFL_player_id"

        # Determine the timestamp/ordering column for proximity matching
        # Prefer cumulative_week (expected to be populated at ingest),
        # fall back to timestamp if present.
        order_col = None
        if "cumulative_week" in trans_cols:
            order_col = "cumulative_week"
        elif "timestamp" in trans_cols:
            order_col = "timestamp"
        elif "week" in trans_cols:
            order_col = "week"

        if order_col is None:
            logger.warning("[fix_unknown_managers] No ordering column found")
            return 0

        # franchise_id is required — raise loudly if missing rather than silently skipping backfill
        if "franchise_id" not in trans_cols:
            raise KeyError("franchise_id is required for manager identity")
        has_team_name = "team_name" in trans_cols

        set_parts = ["manager = nearest.manager", "franchise_id = nearest.franchise_id"]
        if has_team_name:
            set_parts.append("team_name = nearest.team_name")

        # Select parts for the known-manager CTE
        known_select = [
            f"{player_key}",
            "year",
            f"{order_col}",
            "manager",
            "franchise_id",
        ]
        if has_team_name:
            known_select.append("team_name")

        # Use window function: for each unknown transaction, find the closest known
        # transaction for the same player+year, ordered by absolute distance in order_col.
        sql_backfill_manager = f"""
            WITH unknown_trans AS (
                SELECT rowid as rid, {player_key}, year, {order_col}
                FROM {trans_table} t
                WHERE {player_key} IS NOT NULL
                  AND {self._db_filter('t')}
                  AND (manager IS NULL
                       OR LOWER(TRIM(manager)) IN ('unknown', ''))
                  -- A resolved franchise belongs to a real hidden/private
                  -- owner.  Nearest-player inference must only repair true
                  -- orphans, otherwise it can rewrite one leg of a trade to
                  -- the counterparty and break the mirrored identity.
                  AND (
                      franchise_id IS NULL
                      OR LOWER(TRIM(COALESCE(CAST(franchise_id AS VARCHAR), '')))
                         IN ('', '--', '--hidden--', 'none', 'nan', '<na>', 'n/a', 'null')
                  )
            ),
            known_trans AS (
                SELECT {', '.join(known_select)}
                FROM {trans_table} t
                WHERE {player_key} IS NOT NULL
                  AND {self._db_filter('t')}
                  AND manager IS NOT NULL
                  AND LOWER(TRIM(manager)) NOT IN ('unknown', '')
            ),
            nearest_match AS (
                SELECT
                    u.rid,
                    k.manager,
                    k.franchise_id,
                    {"k.team_name," if has_team_name else ""}
                    ROW_NUMBER() OVER (
                        PARTITION BY u.rid
                        ORDER BY ABS(
                            CAST(k.{order_col} AS BIGINT) - CAST(u.{order_col} AS BIGINT)
                        ) ASC
                    ) as rn
                FROM unknown_trans u
                INNER JOIN known_trans k
                    ON k.{player_key} = u.{player_key}
                    AND k.year = u.year
            )
            UPDATE {trans_table} t
            SET {', '.join(set_parts)}
            FROM nearest_match nearest
            WHERE t.rowid = nearest.rid
              AND nearest.rn = 1
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(
            sql_backfill_manager, "fix_unknown_managers: backfill from nearest known transaction"
        )

        # --- Step 2: Backfill NULL positions from player_fantasy ---
        if "position" in trans_cols and self._table_exists("player_fantasy"):
            player_cols = self._get_table_columns("player_fantasy")

            pf_join_key = "NFL_player_id"
            player_table = self._qualified_name("player_fantasy")

            if "position" in player_cols:
                sql_backfill_pos = f"""
                    WITH player_positions AS (
                        SELECT DISTINCT
                            {pf_join_key},
                            FIRST_VALUE(position) OVER (
                                PARTITION BY {pf_join_key}
                                ORDER BY year DESC, week DESC
                            ) as position
                        FROM {player_table}
                        WHERE {pf_join_key} IS NOT NULL
                          AND {self._db_filter()}
                          AND position IS NOT NULL
                          AND TRIM(position) != ''
                    )
                    UPDATE {trans_table} t
                    SET position = pp.position
                    FROM player_positions pp
                    WHERE t.{pf_join_key} = pp.{pf_join_key}
                      AND (t.position IS NULL OR TRIM(t.position) = '')
                      AND {self._db_filter('t')}
                """
                rows_updated += self._execute(
                    sql_backfill_pos, "fix_unknown_managers: backfill NULL positions from player_fantasy"
                )

        return rows_updated

    # =========================================================================
    # DRAFT PICK CONVEYANCES: Link traded picks to the players actually drafted
    # =========================================================================

    def draft_pick_conveyances(self) -> int:
        """Link traded draft picks to the players actually selected.

        For transaction_type='trade_pick', join to draft table using:
        - transactions.traded_pick_season = draft.year
        - transactions.traded_pick_round = draft.round
        - Sleeper transaction pick roster_id matched to the correct draft slot owner

        Sets: conveyed_player, conveyed_lamar, conveyed_fantasy_points,
              conveyed_year, is_conveyed.

        Also copies conveyed LAMAR into standard LAMAR columns so UI
        aggregations work for draft pick trades, and annotates the pick label
        with the eventual player without replacing the traded asset itself.
        """
        if not self._table_exists("transactions"):
            logger.warning("[draft_pick_conveyances] transactions table not found")
            return 0

        if not self._table_exists("draft"):
            logger.warning("[draft_pick_conveyances] draft table not found - cannot link picks")
            return 0

        trans_cols = self._get_table_columns("transactions")
        draft_cols = self._get_table_columns("draft")
        trans_table = self._qualified_name("transactions")
        draft_table = self._qualified_name("draft")

        # Check for required source columns in transactions
        has_pick_season = "traded_pick_season" in trans_cols
        has_pick_round = "traded_pick_round" in trans_cols
        has_pick_owner = "traded_pick_original_owner" in trans_cols

        if not (has_pick_season and has_pick_round):
            logger.info("[draft_pick_conveyances] traded_pick_season/round not in transactions - skipping")
            return 0

        # Check for required draft columns
        if "year" not in draft_cols or "round" not in draft_cols:
            logger.warning("[draft_pick_conveyances] draft table missing year/round columns")
            return 0

        has_draft_slot = "draft_slot" in draft_cols
        has_draft_slot_roster_id = "draft_slot_roster_id" in draft_cols
        has_draft_category = "draft_category" in draft_cols

        # Determine LAMAR and points columns in draft table
        lamar_col = None
        for col in ["manager_lamar", "lamar", "LAMAR"]:
            if col in draft_cols:
                lamar_col = col
                break

        points_col = None
        for col in ["total_fantasy_points", "fantasy_points", "season_fantasy_points", "points"]:
            if col in draft_cols:
                points_col = col
                break

        player_col = "player" if "player" in draft_cols else None
        has_trans_player = "player" in trans_cols

        # Sleeper encodes original pick roster_id in sleeper_player_id:
        # pick_{season}_{round}_{roster_id}. That is the original roster's id,
        # not its draft-order slot, including drafts labelled startup.
        has_sleeper_id = "sleeper_player_id" in trans_cols

        logger.info(
            f"[draft_pick_conveyances] draft columns: lamar={lamar_col}, "
            f"points={points_col}, player={player_col}, draft_slot={has_draft_slot}, "
            f"draft_slot_roster_id={has_draft_slot_roster_id}, "
            f"draft_category={has_draft_category}, sleeper_roster_id={has_sleeper_id}"
        )

        rows_updated = 0

        # --- Step 1: Initialize conveyance fields for all trade_pick rows ---
        init_set_parts = ["is_conveyed = FALSE"]
        for col in ["conveyed_player", "conveyed_lamar", "conveyed_fantasy_points", "conveyed_year"]:
            if col in trans_cols:
                init_set_parts.append(f"{col} = NULL")
        if player_col and has_trans_player:
            init_set_parts.append("player = REGEXP_REPLACE(player, '\\s*\\(became [^)]*\\)$', '')")

        sql_init = f"""
            UPDATE {trans_table}
            SET {', '.join(init_set_parts)}
            WHERE {self._db_filter()}
              AND transaction_type = 'trade_pick'
        """
        rows_updated += self._execute(sql_init, "draft_pick_conveyances: init conveyance fields")

        # --- Step 2: Match draft picks ---
        # Join logic:
        #   transactions.traded_pick_season = draft.year
        #   transactions.traded_pick_round = draft.round
        #   Sleeper rookie/future picks: draft.draft_slot_roster_id = roster_id
        #       extracted from sleeper_player_id
        #   Sleeper startup picks: draft.draft_slot = extracted roster_id
        #   Fallbacks may only use numeric slot-owner identifiers, never manager names.

        # Build draft match CTE
        has_nfl_id = "NFL_player_id" in draft_cols and "NFL_player_id" in trans_cols
        select_parts = ["d.year AS conveyed_year", "d.round"]
        if player_col:
            select_parts.append(f"d.{player_col} AS conveyed_player")
        if lamar_col:
            select_parts.append(f"d.{lamar_col} AS conveyed_lamar")
        if points_col:
            select_parts.append(f"d.{points_col} AS conveyed_fantasy_points")
        if has_nfl_id:
            select_parts.append("d.NFL_player_id AS conveyed_nfl_id")

        # Build the owner matching condition
        sleeper_pick_roster_expr = "TRY_CAST(SPLIT_PART(t.sleeper_player_id, '_', 4) AS INTEGER)"
        numeric_original_owner_expr = "TRY_CAST(t.traded_pick_original_owner AS INTEGER)"

        if has_sleeper_id and has_draft_slot_roster_id:
            owner_condition = f"""
                AND (
                    d.draft_slot_roster_id = {sleeper_pick_roster_expr}
                    OR d.draft_slot_roster_id = {numeric_original_owner_expr}
                )
            """
            match_preference = f"""
                    CASE WHEN d.draft_slot_roster_id = {sleeper_pick_roster_expr}
                         THEN 0 ELSE 1 END,
            """
        elif has_sleeper_id and has_draft_category and has_draft_slot:
            # Without draft_slot_roster_id, only startup conveyance is
            # identity-safe. Future/rookie picks need the draft-order mapping.
            owner_condition = f"""
                AND LOWER(COALESCE(d.draft_category, '')) = 'startup'
                AND d.draft_slot = {sleeper_pick_roster_expr}
            """
            match_preference = ""
        elif has_sleeper_id and has_draft_slot:
            # Legacy Sleeper data without draft_slot_roster_id cannot safely
            # resolve traded rookie picks through draft_order. Only use the
            # numeric original-owner fallback.
            owner_condition = f"""
                AND d.draft_slot = {numeric_original_owner_expr}
            """
            match_preference = ""
        elif has_pick_owner and has_draft_slot:
            # Non-Sleeper: only numeric draft_slot match is identity-safe
            owner_condition = """
                AND d.draft_slot = TRY_CAST(t.traded_pick_original_owner AS INTEGER)
            """
            match_preference = ""
        elif has_pick_owner:
            # No slot identifier available — match only on season+round and accept ambiguity.
            owner_condition = ""
            match_preference = ""
        else:
            # No original owner info — match on season+round only (may be ambiguous)
            owner_condition = ""
            match_preference = ""

        # SET clause for the update
        set_parts = ["is_conveyed = TRUE", "conveyed_year = dm.conveyed_year"]
        if player_col:
            set_parts.append("conveyed_player = dm.conveyed_player")
        if lamar_col:
            set_parts.append("conveyed_lamar = dm.conveyed_lamar")
        if points_col:
            set_parts.append("conveyed_fantasy_points = dm.conveyed_fantasy_points")
        if has_nfl_id:
            set_parts.append("NFL_player_id = dm.conveyed_nfl_id")

        # Keep the visible asset as the pick, then annotate how it conveyed.
        if player_col and has_trans_player:
            set_parts.append("""
                player = (
                    CASE
                        WHEN NULLIF(TRIM(COALESCE(t.player, '')), '') IS NULL
                            OR t.player = dm.conveyed_player
                        THEN CAST(dm.conveyed_year AS VARCHAR) || ' '
                            || CASE dm.round
                                WHEN 1 THEN '1st'
                                WHEN 2 THEN '2nd'
                                WHEN 3 THEN '3rd'
                                ELSE CAST(dm.round AS VARCHAR) || 'th'
                            END
                            || CASE
                                WHEN NULLIF(TRIM(COALESCE(t.traded_pick_original_owner, '')), '') IS NOT NULL
                                THEN ' (from ' || t.traded_pick_original_owner || ')'
                                ELSE ''
                            END
                        ELSE t.player
                    END
                ) || ' (became ' || dm.conveyed_player || ')'
            """)

        sql_match = f"""
            WITH draft_matches AS (
                SELECT
                    t.rowid AS rid,
                    {', '.join(select_parts)},
                    ROW_NUMBER() OVER (
                        PARTITION BY t.rowid
                        ORDER BY {match_preference}d.year ASC, d.round ASC
                    ) AS rn
                FROM {trans_table} t
                INNER JOIN {draft_table} d
                    ON CAST(t.traded_pick_season AS INTEGER) = CAST(d.year AS INTEGER)
                    AND CAST(t.traded_pick_round AS INTEGER) = CAST(d.round AS INTEGER)
                    {owner_condition}
                WHERE t.transaction_type = 'trade_pick'
                  AND {self._db_filter('t')}
                  AND {self._db_filter('d')}
                  AND t.traded_pick_season IS NOT NULL
                  AND t.traded_pick_round IS NOT NULL
            )
            UPDATE {trans_table} t
            SET {', '.join(set_parts)}
            FROM draft_matches dm
            WHERE t.rowid = dm.rid
              AND dm.rn = 1
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_match, "draft_pick_conveyances: match picks to drafted players")

        # --- Step 3: Copy conveyed LAMAR into standard LAMAR columns ---
        # For draft pick trades, the "value" is the conveyed player's LAMAR.
        # This allows trade grading to include pick value.
        if lamar_col:
            lamar_targets = [
                "manager_lamar_ros_managed",
                "player_lamar_ros_managed",
                "player_lamar_ros_total",
                "fa_lamar_ros",
                "lamar_after_acquisition",
            ]
            lamar_set = [f"{col} = t.conveyed_lamar" for col in lamar_targets if col in trans_cols]

            if lamar_set:
                sql_copy_lamar = f"""
                    UPDATE {trans_table} t
                    SET {', '.join(lamar_set)}
                    WHERE t.transaction_type = 'trade_pick'
                      AND t.is_conveyed = TRUE
                      AND t.conveyed_lamar IS NOT NULL
                      AND {self._db_filter('t')}
                """
                rows_updated += self._execute(
                    sql_copy_lamar, "draft_pick_conveyances: copy conveyed LAMAR to standard columns"
                )

        if points_col:
            points_targets = [
                "total_points_ros_managed",
                "points_after_acquisition",
            ]
            points_set = [f"{col} = t.conveyed_fantasy_points" for col in points_targets if col in trans_cols]

            if points_set:
                sql_copy_pts = f"""
                    UPDATE {trans_table} t
                    SET {', '.join(points_set)}
                    WHERE t.transaction_type = 'trade_pick'
                      AND t.is_conveyed = TRUE
                      AND t.conveyed_fantasy_points IS NOT NULL
                      AND {self._db_filter('t')}
                """
                rows_updated += self._execute(
                    sql_copy_pts, "draft_pick_conveyances: copy conveyed points to standard columns"
                )

        # --- Step 4: Placeholder NFL_player_id for un-conveyed (future) picks ---
        # Future picks don't have a drafted player yet, but they still need a stable
        # unique identifier so they can be joined, deduped, and tracked. Format:
        # PICK_{season}_{round}_{original_owner}. This makes identical picks traded
        # multiple times share the same placeholder (they ARE the same pick).
        if has_nfl_id and has_pick_owner:
            sql_placeholder = f"""
                UPDATE {trans_table}
                SET NFL_player_id = 'PICK_' || CAST(traded_pick_season AS VARCHAR)
                    || '_' || CAST(traded_pick_round AS VARCHAR)
                    || '_' || COALESCE(traded_pick_original_owner, 'unknown')
                WHERE transaction_type = 'trade_pick'
                  AND NFL_player_id IS NULL
                  AND traded_pick_season IS NOT NULL
                  AND traded_pick_round IS NOT NULL
                  AND {self._db_filter()}
            """
            rows_updated += self._execute(
                sql_placeholder, "draft_pick_conveyances: placeholder NFL_player_id for future picks"
            )

        # --- Step 5: Dedup trade_pick rows ---
        # Multi-pick trades can produce duplicate (transaction_id, NFL_player_id,
        # manager, trade_direction) rows. Keep only one per unique combination.
        dedup_cols = ["transaction_id", "NFL_player_id", "manager", "trade_direction"]
        available_dedup = [c for c in dedup_cols if c in trans_cols]
        if len(available_dedup) >= 2:
            group_by = ", ".join(available_dedup)
            sql_dedup = f"""
                DELETE FROM {trans_table}
                WHERE transaction_type = 'trade_pick'
                  AND {self._db_filter()}
                  AND rowid NOT IN (
                      SELECT MIN(rowid)
                      FROM {trans_table}
                      WHERE transaction_type = 'trade_pick'
                        AND {self._db_filter()}
                      GROUP BY {group_by}
                  )
            """
            deleted = self._execute(sql_dedup, "draft_pick_conveyances: dedup trade_pick rows")
            if deleted:
                logger.info(f"[draft_pick_conveyances] removed {deleted} duplicate trade_pick rows")

        return rows_updated

    # =========================================================================
    # TRANSACTION ENGAGEMENT METRICS: UI-friendly labels, tiers, percentiles
    # =========================================================================

    def transaction_engagement_metrics(self) -> int:
        """Add UI-friendly labels and tiers to transactions.

        Adds these columns (labels only, no scoring impact):
        - faab_value_tier: FAAB efficiency tier (Steal/Great Value/Good Value/Fair/Overpay)
        - timing_category: Early Season / Mid Season / Late Season / Playoffs
        - pickup_type: Waiver Claim / Free Agent / Trade / Drop / Pickup / Other

        Must run AFTER transaction_score() (needs manager_lamar_ros_managed for FAAB tier).
        """
        if not self._table_exists("transactions"):
            logger.warning("[transaction_engagement_metrics] transactions table not found")
            return 0

        trans_cols = self._get_table_columns("transactions")
        trans_table = self._qualified_name("transactions")

        # Determine managed LAMAR column for FAAB tier
        managed_lamar = None
        for col in ["manager_lamar_ros_managed", "fa_lamar_ros"]:
            if col in trans_cols:
                managed_lamar = col
                break

        if managed_lamar is None:
            logger.info("[transaction_engagement_metrics] No LAMAR ROS column found - faab_value_tier will be skipped")

        rows_updated = 0
        type_expr = "transaction_type" if "transaction_type" in trans_cols else "type"

        # 1. FAAB VALUE TIER (adds with FAAB > 0 only)
        if "faab_bid" in trans_cols and managed_lamar:
            sql_faab_tier = f"""
                UPDATE {trans_table}
                SET faab_value_tier = CASE
                    WHEN {type_expr} NOT IN {_ADD_TRANSACTION_TYPES_SQL} THEN NULL
                    WHEN faab_bid IS NULL OR faab_bid <= 0 THEN NULL
                    WHEN COALESCE({managed_lamar}, 0) / GREATEST(faab_bid, 1) > 5 THEN 'Steal'
                    WHEN COALESCE({managed_lamar}, 0) / GREATEST(faab_bid, 1) > 3 THEN 'Great Value'
                    WHEN COALESCE({managed_lamar}, 0) / GREATEST(faab_bid, 1) > 1 THEN 'Good Value'
                    WHEN COALESCE({managed_lamar}, 0) / GREATEST(faab_bid, 1) > 0 THEN 'Fair'
                    ELSE 'Overpay'
                END
                WHERE {self._db_filter()}
            """
            rows_updated += self._execute(sql_faab_tier, "engagement: faab_value_tier")

        # 2. TIMING CATEGORY
        sql_timing = f"""
            WITH week_info AS (
                SELECT year, MAX(week) as max_week
                FROM {trans_table}
                WHERE {self._db_filter()}
                GROUP BY year
            )
            UPDATE {trans_table} t
            SET timing_category = CASE
                WHEN t.week IS NULL THEN 'Unknown'
                WHEN t.week <= 4 THEN 'Early Season'
                WHEN t.week <= 10 THEN 'Mid Season'
                WHEN w.max_week >= 10 AND t.week >= w.max_week - 3 THEN 'Playoffs'
                ELSE 'Late Season'
            END
            FROM week_info w WHERE t.year = w.year
              AND {self._db_filter('t')}
        """
        rows_updated += self._execute(sql_timing, "engagement: timing_category")

        # 3. PICKUP TYPE
        if "source_type" in trans_cols:
            sql_pickup = f"""
                UPDATE {trans_table}
                SET pickup_type = CASE
                    WHEN LOWER(COALESCE(source_type, '')) = 'waivers' THEN 'Waiver Claim'
                    WHEN LOWER(COALESCE(source_type, '')) IN ('freeagents', 'free_agents') THEN 'Free Agent'
                    WHEN {type_expr} = 'trade' THEN 'Trade'
                    WHEN {type_expr} = 'drop' THEN 'Drop'
                    WHEN {type_expr} IN {_ADD_TRANSACTION_TYPES_SQL} THEN 'Pickup'
                    ELSE 'Other'
                END
                WHERE {self._db_filter()}
            """
        else:
            sql_pickup = f"""
                UPDATE {trans_table}
                SET pickup_type = CASE
                    WHEN {type_expr} IN {_ADD_TRANSACTION_TYPES_SQL} THEN 'Pickup'
                    WHEN {type_expr} = 'drop' THEN 'Drop'
                    WHEN {type_expr} = 'trade' THEN 'Trade'
                    ELSE 'Other'
                END
                WHERE {self._db_filter()}
            """
        rows_updated += self._execute(sql_pickup, "engagement: pickup_type")

        return rows_updated
