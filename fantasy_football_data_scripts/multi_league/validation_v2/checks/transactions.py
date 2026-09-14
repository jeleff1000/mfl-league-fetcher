"""Transaction checks — 32 checks verifying transactions table integrity,
grades, trade symmetry, and derived ROS metrics.

Checks are a mix of sql_expr (batched SUM/CASE on transactions) and sql_full
(standalone queries that need subqueries or cross-table joins).
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check

_ADD_TYPES = "('add', 'pickup', 'claim', 'waiver', 'add/drop')"
_ADD_DROP_TYPES = "('add', 'pickup', 'claim', 'waiver', 'add/drop', 'drop')"
_VALID_TYPES = (
    "('add', 'pickup', 'claim', 'waiver', 'add/drop', 'drop', 'trade', 'trade_pick', 'commissioner', 'commish')"
)
_VALID_GRADES = "('A+','A','A-','B+','B','B-','C+','C','C-','D+','D','D-','F')"
_VALID_TRADE_DIRS = "('received', 'sent', 'trade_pick_received', 'trade_pick_sent')"

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # ERROR: Core — sql_expr checks (batched)
    # ------------------------------------------------------------------
    # 1. txn_id_not_null
    Check(
        name="txn_id_not_null",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description="transaction_id NULL on transactions row",
        sql_expr="SUM(CASE WHEN transaction_id IS NULL THEN 1 ELSE 0 END)",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 2. txn_valid_types
    Check(
        name="txn_valid_types",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description=f"transaction_type outside valid set {_VALID_TYPES}",
        sql_expr=(
            f"SUM(CASE WHEN transaction_type IS NOT NULL "
            f"AND transaction_type NOT IN {_VALID_TYPES} "
            f"THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 3. txn_all_adds_have_player
    #    Add/claim/waiver rows must reference a player.
    Check(
        name="txn_all_adds_have_player",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description="Add-type transaction row has NULL player",
        sql_expr=(f"SUM(CASE WHEN transaction_type IN {_ADD_TYPES} " f"AND player IS NULL " f"THEN 1 ELSE 0 END)"),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 4. txn_faab_nonneg (faab leagues only)
    Check(
        name="txn_faab_nonneg",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description="faab_bid < 0 on transactions row",
        sql_expr="SUM(CASE WHEN faab_bid IS NOT NULL AND faab_bid < 0 THEN 1 ELSE 0 END)",
        feature="faab",
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 5. txn_percentile_bounds
    Check(
        name="txn_percentile_bounds",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description="score_percentile not between 0 and 100",
        sql_expr=(
            "SUM(CASE WHEN score_percentile IS NOT NULL "
            "AND (score_percentile < 0 OR score_percentile > 100) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 6. txn_trade_direction_populated
    Check(
        name="txn_trade_direction_populated",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description="trade_direction NULL for trade/trade_pick row",
        sql_expr=(
            "SUM(CASE WHEN transaction_type IN ('trade', 'trade_pick') "
            "AND trade_direction IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 7. txn_trade_direction_valid
    Check(
        name="txn_trade_direction_valid",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description=f"trade_direction outside valid set {_VALID_TRADE_DIRS}",
        sql_expr=(
            f"SUM(CASE WHEN trade_direction IS NOT NULL "
            f"AND trade_direction NOT IN {_VALID_TRADE_DIRS} "
            f"THEN 1 ELSE 0 END)"
        ),
        batch_group="core",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # ERROR: Core — sql_full checks (need subqueries)
    # ------------------------------------------------------------------
    # 8. txn_no_duplicates
    #    A row is uniquely identified by (transaction_id, player asset, manager,
    #    trade_direction, transaction_type). Prefer normalized NFL_player_id, but
    #    fall back through platform IDs and finally player name for unresolved
    #    assets so distinct null-ID players in the same transaction do not collapse.
    #    - trade_direction: duplicate_trade_rows() in trade_utils.py creates one
    #      row per manager perspective ('sent' + 'received')
    #    - transaction_type: a single transaction_id can hold both add and drop
    #      events for the same manager (add/drop pair)
    #    - NFL_player_id: when present, this prevents rows like ESPN "Unknown"
    #      players with different normalized IDs from collapsing
    Check(
        name="txn_no_duplicates",
        page="transactions",
        table="transactions",
        severity="ERROR",
        description="Duplicate (transaction_id, player asset, manager, trade_direction, type) rows",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, transaction_id, "
            "         COALESCE(CAST(NFL_player_id AS VARCHAR), CAST(yahoo_player_id AS VARCHAR), "
            "                  CAST(sleeper_player_id AS VARCHAR), CAST(espn_player_id AS VARCHAR), "
            "                  NULLIF(TRIM(CAST(player AS VARCHAR)), ''), '') AS asset_key, "
            "         manager, "
            "         COALESCE(trade_direction, '') AS trade_direction, "
            "         COALESCE(transaction_type, '') AS transaction_type, "
            "         COUNT(*) AS cnt "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND transaction_id IS NOT NULL "
            "  GROUP BY db_name, transaction_id, "
            "           COALESCE(CAST(NFL_player_id AS VARCHAR), CAST(yahoo_player_id AS VARCHAR), "
            "                    CAST(sleeper_player_id AS VARCHAR), CAST(espn_player_id AS VARCHAR), "
            "                    NULLIF(TRIM(CAST(player AS VARCHAR)), ''), ''), manager, "
            "           COALESCE(trade_direction, ''), COALESCE(transaction_type, '') "
            "  HAVING COUNT(*) > 1"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="core",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_full checks
    # ------------------------------------------------------------------
    # 9. txn_trade_symmetry
    #    A trade transaction_id must involve at least 2 distinct managers.
    Check(
        name="txn_trade_symmetry",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="Trade transaction_id has <2 distinct managers (asymmetric trade)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, transaction_id, COUNT(DISTINCT manager) AS party_count "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND transaction_type IN ('trade', 'trade_pick') "
            "    AND transaction_id IS NOT NULL "
            "  GROUP BY db_name, transaction_id "
            "  HAVING COUNT(DISTINCT manager) < 2"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 10. txn_nfl_id_coverage
    #     Player add/drop rows (excluding trade_pick) should have NFL_player_id.
    Check(
        name="txn_nfl_id_coverage",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="<80% of non-trade_pick transactions have NFL_player_id",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN NFL_player_id IS NOT NULL THEN 1 ELSE 0 END) AS has_id, "
            "    COUNT(*) AS total "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND transaction_type != 'trade_pick' "
            "    AND player IS NOT NULL "
            "  GROUP BY db_name "
            "  HAVING total > 0 "
            "    AND (CAST(has_id AS DOUBLE) / total) < 0.8"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 11. txn_add_drop_balance
    #     Adds should not vastly outnumber drops (ratio > 5:1 signals an issue).
    Check(
        name="txn_add_drop_balance",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="Add:drop ratio > 5:1 (likely missing drop records)",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM ("
            "  SELECT db_name, "
            f"    SUM(CASE WHEN transaction_type IN {_ADD_TYPES} "
            f"        THEN 1 ELSE 0 END) AS add_count, "
            "    SUM(CASE WHEN transaction_type = 'drop' THEN 1 ELSE 0 END) AS drop_count "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "  GROUP BY db_name "
            "  HAVING drop_count > 0 "
            "    AND (CAST(add_count AS DOUBLE) / drop_count) > 5"
            ") "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
    ),
    # 12. txn_report_card_exists
    #     transaction_report_card should have data for any league with transactions.
    Check(
        name="txn_report_card_exists",
        page="transactions",
        table="transaction_report_card",
        severity="WARNING",
        description="transaction_report_card has no data for league with transactions",
        sql_full=(
            "SELECT t.db_name, 1 AS fail_count "
            "FROM ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list})"
            ") t "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transaction_report_card"
            ") rc ON t.db_name = rc.db_name "
            "WHERE rc.db_name IS NULL"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_transactions"],
    ),
    # 13. txn_agg_tables_exist
    #     Transaction aggregate tables should have data for any league with transactions.
    Check(
        name="txn_agg_tables_exist",
        page="transactions",
        table="transaction_manager_season",
        severity="WARNING",
        description="Transaction aggregate table missing data for league with transactions",
        sql_full=(
            "WITH transaction_leagues AS ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list})"
            "), presence AS ("
            "  SELECT t.db_name, "
            "    CASE WHEN tms.db_name IS NULL THEN 1 ELSE 0 END + "
            "    CASE WHEN tmc.db_name IS NULL THEN 1 ELSE 0 END + "
            "    CASE WHEN tpc.db_name IS NULL THEN 1 ELSE 0 END + "
            "    CASE WHEN rc.db_name IS NULL THEN 1 ELSE 0 END AS missing_count "
            "  FROM transaction_leagues t "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transaction_manager_season"
            ") tms ON t.db_name = tms.db_name "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transaction_manager_career"
            ") tmc ON t.db_name = tmc.db_name "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transaction_player_career"
            ") tpc ON t.db_name = tpc.db_name "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}transaction_report_card"
            ") rc ON t.db_name = rc.db_name "
            ") "
            "SELECT db_name, missing_count AS fail_count "
            "FROM presence "
            "WHERE missing_count > 0"
        ),
        batch_group="quality",
        cost_tier="cheap",
        depends_on=["completeness_has_transactions"],
    ),
    Check(
        name="txn_manager_season_matches_transactions_summary",
        page="transactions",
        table="transaction_manager_season",
        severity="ERROR",
        description="transaction_manager_season summary fields drift from grouped transactions",
        sql_full=(
            "WITH add_drop AS ("
            "  SELECT db_name, franchise_id, year, "
            f"    SUM(CASE WHEN transaction_type IN {_ADD_TYPES} THEN 1 ELSE 0 END) AS adds, "
            "    SUM(CASE WHEN transaction_type = 'drop' THEN 1 ELSE 0 END) AS drops, "
            f"    SUM(CASE WHEN transaction_type IN {_ADD_DROP_TYPES} THEN 1 ELSE 0 END) AS total_moves, "
            f"    SUM(CASE WHEN transaction_type IN {_ADD_TYPES} THEN COALESCE(faab_bid, 0) ELSE 0 END) AS total_faab_bid, "
            f"    AVG(CASE WHEN transaction_type IN {_ADD_DROP_TYPES} THEN transaction_score END) AS total_transaction_score "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND manager IS NOT NULL AND TRIM(manager) <> '' "
            "    AND franchise_id IS NOT NULL AND TRIM(CAST(franchise_id AS VARCHAR)) <> '' "
            f"    AND transaction_type IN {_ADD_DROP_TYPES} "
            "  GROUP BY db_name, franchise_id, year"
            "), "
            "trade_agg AS ("
            "  SELECT db_name, franchise_id, year, COUNT(DISTINCT transaction_id) AS trades "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND manager IS NOT NULL AND TRIM(manager) <> '' "
            "    AND franchise_id IS NOT NULL AND TRIM(CAST(franchise_id AS VARCHAR)) <> '' "
            "    AND transaction_type IN ('trade', 'trade_pick') "
            "  GROUP BY db_name, franchise_id, year"
            "), "
            "expected AS ("
            "  SELECT ad.db_name, ad.franchise_id, ad.year, ad.adds, ad.drops, ad.total_moves, "
            "    ad.total_faab_bid, ad.total_transaction_score, COALESCE(tr.trades, 0) AS trades "
            "  FROM add_drop ad "
            "  LEFT JOIN trade_agg tr "
            "    ON ad.db_name = tr.db_name AND ad.franchise_id = tr.franchise_id AND ad.year = tr.year"
            "), "
            "actual AS ("
            "  SELECT db_name, franchise_id, year, adds, drops, total_moves, total_faab_bid, total_transaction_score, trades "
            "  FROM {table_prefix}transaction_manager_season "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(e.db_name, a.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.franchise_id = a.franchise_id "
            " AND e.year = a.year "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(e.adds, 0) <> COALESCE(a.adds, 0) "
            "   OR COALESCE(e.drops, 0) <> COALESCE(a.drops, 0) "
            "   OR COALESCE(e.total_moves, 0) <> COALESCE(a.total_moves, 0) "
            "   OR COALESCE(e.trades, 0) <> COALESCE(a.trades, 0) "
            "   OR ABS(COALESCE(e.total_faab_bid, 0) - COALESCE(a.total_faab_bid, 0)) > 0.01 "
            "   OR ABS(COALESCE(e.total_transaction_score, 0) - COALESCE(a.total_transaction_score, 0)) > 0.01 "
            "GROUP BY COALESCE(e.db_name, a.db_name)"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_transactions", "txn_agg_tables_exist"],
        fix_action="reimport",
    ),
    Check(
        name="txn_manager_career_matches_season_rollup",
        page="transactions",
        table="transaction_manager_career",
        severity="ERROR",
        description="transaction_manager_career totals drift from transaction_manager_season",
        sql_full=(
            "WITH expected AS ("
            "  SELECT db_name, franchise_id, COUNT(DISTINCT year) AS seasons, "
            "    SUM(adds) AS total_adds, SUM(drops) AS total_drops, SUM(total_moves) AS total_moves, "
            "    SUM(total_faab_bid) AS total_faab_bid, "
            "    SUM(total_transaction_score * total_moves) / NULLIF(SUM(total_moves), 0) AS total_transaction_score, "
            "    SUM(trades) AS total_trades "
            "  FROM {table_prefix}transaction_manager_season "
            "  WHERE db_name IN ({league_list}) "
            "    AND franchise_id IS NOT NULL AND TRIM(CAST(franchise_id AS VARCHAR)) <> '' "
            "  GROUP BY db_name, franchise_id"
            "), "
            "actual AS ("
            "  SELECT db_name, franchise_id, seasons, total_adds, total_drops, total_moves, "
            "    total_faab_bid, total_transaction_score, total_trades "
            "  FROM {table_prefix}transaction_manager_career "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(e.db_name, a.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.franchise_id = a.franchise_id "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(e.seasons, 0) <> COALESCE(a.seasons, 0) "
            "   OR COALESCE(e.total_adds, 0) <> COALESCE(a.total_adds, 0) "
            "   OR COALESCE(e.total_drops, 0) <> COALESCE(a.total_drops, 0) "
            "   OR COALESCE(e.total_moves, 0) <> COALESCE(a.total_moves, 0) "
            "   OR COALESCE(e.total_trades, 0) <> COALESCE(a.total_trades, 0) "
            "   OR ABS(COALESCE(e.total_faab_bid, 0) - COALESCE(a.total_faab_bid, 0)) > 0.01 "
            "   OR ABS(COALESCE(e.total_transaction_score, 0) - COALESCE(a.total_transaction_score, 0)) > 0.01 "
            "GROUP BY COALESCE(e.db_name, a.db_name)"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_transactions", "txn_agg_tables_exist"],
        fix_action="reimport",
    ),
    Check(
        name="txn_player_career_matches_transactions_rollup",
        page="transactions",
        table="transaction_player_career",
        severity="ERROR",
        description="transaction_player_career totals drift from grouped transactions",
        sql_full=(
            # ----------------------------------------------------------------
            # The pipeline's career aggregation applies two transforms that the
            # validator must account for:
            #
            # 1. CONVEYED PICKS: Unresolved pick rows like "2022 1st (from X)"
            #    get rewritten to "Player Name (2022 1st)".  We exclude the
            #    unresolved originals (PICK position, "YYYY Nth (from" pattern)
            #    from expected, and exclude resolved conveyed entries (player
            #    name with "(YYYY" suffix, conveyed_player IS NOT NULL) from
            #    career so they don't appear as EXTRA.
            #
            # 2. POSITION CHANGES: A player traded as WR in 2020 may appear as
            #    RB in career if their position changed.  We compare player-
            #    level totals (summed across positions) rather than per-position.
            # ----------------------------------------------------------------
            "WITH deduped_trade_rows AS ("
            "  SELECT * EXCLUDE (rn) FROM ("
            "    SELECT t.*, "
            "      ROW_NUMBER() OVER ("
            "        PARTITION BY t.transaction_id, t.transaction_type, COALESCE(t.player, ''), COALESCE(t.position, ''), "
            "          COALESCE(CAST(t.traded_pick_season AS VARCHAR), ''), "
            "          COALESCE(CAST(t.traded_pick_round AS VARCHAR), ''), "
            "          COALESCE(t.traded_pick_original_owner, '') "
            "        ORDER BY t.manager, t.source_manager"
            "      ) AS rn "
            "    FROM {table_prefix}transactions t "
            "    WHERE t.db_name IN ({league_list}) "
            "      AND t.transaction_type IN ('trade', 'trade_pick')"
            "  ) WHERE rn = 1"
            "), "
            "base_transactions AS ("
            "  SELECT * FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND transaction_type NOT IN ('trade', 'trade_pick') "
            "  UNION ALL "
            "  SELECT * FROM deduped_trade_rows"
            "), "
            # Expected: group by player only (not position) to handle position
            # changes.  Exclude unresolved pick rows ("2022 1st (from X)").
            "expected AS ("
            "  SELECT db_name, player, "
            f"    SUM(CASE WHEN transaction_type IN {_ADD_TYPES} THEN 1 ELSE 0 END) AS times_added, "
            "    SUM(CASE WHEN transaction_type = 'drop' THEN 1 ELSE 0 END) AS times_dropped, "
            "    SUM(CASE WHEN transaction_type IN ('trade', 'trade_pick') THEN 1 ELSE 0 END) AS times_traded, "
            f"    SUM(CASE WHEN transaction_type IN {_ADD_TYPES} THEN COALESCE(faab_bid, 0) ELSE 0 END) AS total_faab_spent "
            "  FROM base_transactions "
            "  WHERE player IS NOT NULL AND TRIM(player) <> '' "
            "    AND position IS NOT NULL "
            # Exclude unresolved picks: "2022 1st (from X)"
            "    AND NOT (transaction_type = 'trade_pick' "
            "             AND REGEXP_MATCHES(player, '^[0-9]{{4}} [0-9]+(st|nd|rd|th) \\(from ')) "
            # Exclude resolved conveyed picks: "Player Name (2022 1st)"
            "    AND NOT (transaction_type = 'trade_pick' "
            "             AND REGEXP_MATCHES(player, '\\([0-9]{{4}} [0-9]+(st|nd|rd|th)\\)$')) "
            "  GROUP BY db_name, player"
            "), "
            # Actual: group by player only.  Exclude resolved conveyed pick
            # entries — the pipeline creates career rows like
            # "Drake London (2022 1st)" that don't exist in transactions.
            # These have a "(YYYY Nth)" suffix regardless of position.
            "actual AS ("
            "  SELECT db_name, player, "
            "    SUM(times_added) AS times_added, SUM(times_dropped) AS times_dropped, "
            "    SUM(times_traded) AS times_traded, SUM(total_faab_spent) AS total_faab_spent "
            "  FROM {table_prefix}transaction_player_career "
            "  WHERE db_name IN ({league_list}) "
            # Resolved conveyed: "Player Name (YYYY Nth)" suffix
            "    AND NOT REGEXP_MATCHES(player, '\\([0-9]{{4}} [0-9]+(st|nd|rd|th)\\)$') "
            # Unresolved future picks: "YYYY Nth (from X)" pattern
            "    AND NOT REGEXP_MATCHES(player, '^[0-9]{{4}} [0-9]+(st|nd|rd|th) \\(from ') "
            "  GROUP BY db_name, player"
            ") "
            "SELECT COALESCE(e.db_name, a.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.player = a.player "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(e.times_added, 0) <> COALESCE(a.times_added, 0) "
            "   OR COALESCE(e.times_dropped, 0) <> COALESCE(a.times_dropped, 0) "
            "   OR COALESCE(e.times_traded, 0) <> COALESCE(a.times_traded, 0) "
            "   OR ABS(COALESCE(e.total_faab_spent, 0) - COALESCE(a.total_faab_spent, 0)) > 0.01 "
            "GROUP BY COALESCE(e.db_name, a.db_name)"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_transactions", "txn_agg_tables_exist"],
        fix_action="reimport",
    ),
    Check(
        name="txn_report_card_matches_manager_season",
        page="transactions",
        table="transaction_report_card",
        severity="ERROR",
        description="transaction_report_card summary fields drift from transaction_manager_season",
        sql_full=(
            "WITH expected AS ("
            "  SELECT db_name, franchise_id, year, manager, adds, drops, trades, total_faab_bid, "
            "    net_lamar AS total_lamar, transaction_grade, transaction_gpa "
            "  FROM {table_prefix}transaction_manager_season "
            "  WHERE db_name IN ({league_list})"
            "), "
            "actual AS ("
            "  SELECT db_name, franchise_id, year, manager, adds, drops, trades, total_faab_bid, "
            "    total_lamar, transaction_grade, transaction_gpa "
            "  FROM {table_prefix}transaction_report_card "
            "  WHERE db_name IN ({league_list})"
            ") "
            "SELECT COALESCE(e.db_name, a.db_name) AS db_name, COUNT(*) AS fail_count "
            "FROM expected e "
            "FULL OUTER JOIN actual a "
            "  ON e.db_name = a.db_name "
            " AND e.franchise_id = a.franchise_id "
            " AND e.year = a.year "
            "WHERE a.db_name IS NULL "
            "   OR e.db_name IS NULL "
            "   OR COALESCE(e.manager, '') <> COALESCE(a.manager, '') "
            "   OR COALESCE(e.adds, 0) <> COALESCE(a.adds, 0) "
            "   OR COALESCE(e.drops, 0) <> COALESCE(a.drops, 0) "
            "   OR COALESCE(e.trades, 0) <> COALESCE(a.trades, 0) "
            "   OR ABS(COALESCE(e.total_faab_bid, 0) - COALESCE(a.total_faab_bid, 0)) > 0.01 "
            "   OR ABS(COALESCE(e.total_lamar, 0) - COALESCE(a.total_lamar, 0)) > 0.01 "
            "   OR COALESCE(e.transaction_grade, '') <> COALESCE(a.transaction_grade, '') "
            "   OR ABS(COALESCE(e.transaction_gpa, 0) - COALESCE(a.transaction_gpa, 0)) > 0.0001 "
            "GROUP BY COALESCE(e.db_name, a.db_name)"
        ),
        batch_group="core",
        cost_tier="moderate",
        depends_on=["completeness_has_transactions", "txn_agg_tables_exist", "txn_report_card_exists"],
        fix_action="reimport",
    ),
    # 13. txn_conveyed_picks_valid (dynasty leagues)
    #     is_conveyed = 1 picks must have a conveyed_player populated.
    Check(
        name="txn_conveyed_picks_valid",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="is_conveyed = 1 trade_pick row missing conveyed_player",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}transactions "
            "WHERE db_name IN ({league_list}) "
            "  AND transaction_type = 'trade_pick' "
            "  AND is_conveyed = 1 "
            "  AND conveyed_player IS NULL "
            "GROUP BY db_name"
        ),
        feature="dynasty",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # ------------------------------------------------------------------
    # WARNING: Quality — sql_expr checks (batched)
    # ------------------------------------------------------------------
    # 14. txn_trade_has_partner
    Check(
        name="txn_trade_has_partner",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="source_manager NULL for trade/trade_pick row",
        sql_expr=(
            "SUM(CASE WHEN transaction_type IN ('trade', 'trade_pick') "
            "AND source_manager IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 15. txn_lamar_populated
    #     LAMAR should be populated for add-type moves that resolve to a real NFL player.
    #     NULL is expected for: trade_picks (future picks, no NFL_player_id),
    #     trades (use trade_net_lamar instead), rows without NFL_player_id, commish moves.
    Check(
        name="txn_lamar_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="manager_lamar_ros_managed NULL on resolvable add-type transaction",
        sql_expr=(
            f"SUM(CASE WHEN manager_lamar_ros_managed IS NULL "
            f"AND transaction_type IN {_ADD_TYPES} "
            f"AND NFL_player_id IS NOT NULL "
            f"THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 16. txn_lamar_bounds
    #     Season-total ROS LAMAR can legitimately reach 200-500+ for top picks
    #     acquired early in the year (Jayden Daniels 2024 1st hit 538 as of
    #     2026-04-12). Bounds calibrated for weekly LAMAR would flag real data.
    Check(
        name="txn_lamar_bounds",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="manager_lamar_ros_managed outside sanity bounds (-1000 to 1000)",
        sql_expr=(
            "SUM(CASE WHEN manager_lamar_ros_managed IS NOT NULL "
            "AND (manager_lamar_ros_managed < -1000 OR manager_lamar_ros_managed > 1000) "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 17. txn_engagement_populated
    #     NOTE: engagement_score does not exist in ___leagues DDL.
    #     Check removed — column was never added to centralized schema.
    # 18. txn_engagement_range
    #     NOTE: engagement_score does not exist in ___leagues DDL.
    #     Check removed.
    # 19. txn_trade_grade_valid
    Check(
        name="txn_trade_grade_valid",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="trade_grade value outside valid set (A+–F)",
        sql_expr=(
            f"SUM(CASE WHEN trade_grade IS NOT NULL " f"AND trade_grade NOT IN {_VALID_GRADES} " f"THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
    ),
    # 20. txn_trade_net_lamar_populated
    #     Only actual player trades get net_lamar (trade_picks are future picks).
    Check(
        name="txn_trade_net_lamar_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="trade_net_lamar NULL for player trade row",
        sql_expr=(
            "SUM(CASE WHEN transaction_type = 'trade' "
            "AND trade_net_lamar IS NULL "
            "AND NFL_player_id IS NOT NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 21. txn_trade_grade_populated
    #     Only actual player trades get graded (trade_picks are future draft picks).
    Check(
        name="txn_trade_grade_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="trade_grade NULL for player trade row",
        sql_full=(
            "WITH trade_pools AS ("
            "  SELECT db_name, "
            "    COUNT(DISTINCT transaction_id || '|' || franchise_id) AS pool_size "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND transaction_type IN ('trade', 'trade_pick') "
            "    AND trade_net_lamar IS NOT NULL "
            "    AND ABS(trade_net_lamar) >= 3 "
            "  GROUP BY db_name"
            "), "
            "trade_rows AS ("
            "  SELECT t.db_name, "
            "    SUM(CASE WHEN t.transaction_type = 'trade' "
            "      AND t.NFL_player_id IS NOT NULL "
            "      AND t.trade_grade IS NULL "
            "      AND t.trade_net_lamar IS NOT NULL "
            "      AND ABS(t.trade_net_lamar) >= 3 "
            "      AND COALESCE(tp.pool_size, 0) >= 30 "
            "      THEN 1 ELSE 0 END) AS fail_count "
            "  FROM {table_prefix}transactions t "
            "  LEFT JOIN trade_pools tp ON t.db_name = tp.db_name "
            "  WHERE t.db_name IN ({league_list}) "
            "  GROUP BY t.db_name"
            ") "
            "SELECT db_name, fail_count FROM trade_rows"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 22. txn_trade_percentile_populated
    #     Only actual player trades get percentile ranked.
    Check(
        name="txn_trade_percentile_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="trade_percentile NULL for player trade row",
        sql_full=(
            "WITH trade_pools AS ("
            "  SELECT db_name, "
            "    COUNT(DISTINCT transaction_id || '|' || franchise_id) AS pool_size "
            "  FROM {table_prefix}transactions "
            "  WHERE db_name IN ({league_list}) "
            "    AND transaction_type IN ('trade', 'trade_pick') "
            "    AND trade_net_lamar IS NOT NULL "
            "    AND ABS(trade_net_lamar) >= 3 "
            "  GROUP BY db_name"
            "), "
            "trade_rows AS ("
            "  SELECT t.db_name, "
            "    SUM(CASE WHEN t.transaction_type = 'trade' "
            "      AND t.NFL_player_id IS NOT NULL "
            "      AND t.trade_percentile IS NULL "
            "      AND t.trade_net_lamar IS NOT NULL "
            "      AND ABS(t.trade_net_lamar) >= 3 "
            "      AND COALESCE(tp.pool_size, 0) >= 30 "
            "      THEN 1 ELSE 0 END) AS fail_count "
            "  FROM {table_prefix}transactions t "
            "  LEFT JOIN trade_pools tp ON t.db_name = tp.db_name "
            "  WHERE t.db_name IN ({league_list}) "
            "  GROUP BY t.db_name"
            ") "
            "SELECT db_name, fail_count FROM trade_rows"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 23. txn_score_percentile_populated
    #     Percentile is computed for scored add/drop moves. transaction_score is
    #     a 100-centered quality index, so raw-LAMAR triviality thresholds do
    #     not apply here.
    Check(
        name="txn_score_percentile_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="score_percentile NULL on graded add/drop transaction",
        sql_expr=(
            f"SUM(CASE WHEN score_percentile IS NULL "
            f"AND transaction_type IN {_ADD_TYPES} "
            f"AND transaction_score IS NOT NULL "
            f"THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 24. txn_transaction_grade_populated
    #     Grade is assigned to scored add/drop moves. Trades use trade_grade.
    Check(
        name="txn_transaction_grade_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="transaction_grade NULL on graded add/drop transaction",
        sql_expr=(
            f"SUM(CASE WHEN transaction_grade IS NULL "
            f"AND transaction_type IN {_ADD_TYPES} "
            f"AND transaction_score IS NOT NULL "
            f"THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 25. txn_quality_score_populated
    #     transaction_score is only computed for add/drop moves that resolve to real NFL
    #     players. NULL is expected for trades (use trade_net_lamar), commish moves, and
    #     rows without NFL_player_id.
    Check(
        name="txn_quality_score_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="transaction_score NULL on resolvable add/drop transaction",
        sql_expr=(
            "SUM(CASE WHEN transaction_score IS NULL "
            f"AND transaction_type IN {_ADD_DROP_TYPES} "
            "AND NFL_player_id IS NOT NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 26. txn_source_manager_populated
    Check(
        name="txn_source_manager_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="source_manager NULL for trade row",
        sql_expr=("SUM(CASE WHEN transaction_type = 'trade' " "AND source_manager IS NULL " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 27. txn_source_franchise_id_populated
    Check(
        name="txn_source_franchise_id_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="source_franchise_id NULL for trade row",
        sql_expr=("SUM(CASE WHEN transaction_type = 'trade' " "AND source_franchise_id IS NULL " "THEN 1 ELSE 0 END)"),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 28. txn_ros_points_populated
    #     ROS points should be populated for add-type moves with a resolvable NFL player
    #     who has at least one player_fantasy row AT OR AFTER the transaction week.
    #     NULL is expected for: trade_picks, trades, commish moves, post-season-end
    #     adds (week >= 18), and adds of players who never appeared again that year.
    Check(
        name="txn_ros_points_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description=">25% of resolvable add-type transactions missing total_points_ros_managed",
        sql_full=(
            "SELECT t.db_name, SUM(CASE WHEN t.total_points_ros_managed IS NULL THEN 1 ELSE 0 END) AS fail_count "
            "FROM {table_prefix}transactions t "
            "LEFT JOIN {table_prefix}league_settings ls "
            "  ON t.db_name = ls.db_name AND t.year = ls.year "
            "WHERE t.db_name IN ({league_list}) "
            f"  AND t.transaction_type IN {_ADD_TYPES} "
            "  AND t.NFL_player_id IS NOT NULL "
            "  AND t.NFL_player_id NOT LIKE 'PICK_%' "
            "  AND (t.week IS NULL OR t.week < COALESCE(CAST(ls.end_week AS INTEGER), 17)) "
            "  AND EXISTS ("
            "    SELECT 1 FROM {table_prefix}player_fantasy pf "
            "    WHERE pf.db_name = t.db_name "
            "      AND pf.year = t.year "
            "      AND pf.NFL_player_id = t.NFL_player_id "
            "      AND (t.week IS NULL OR pf.week >= t.week) "
            "      AND pf.manager IS NOT NULL "
            "      AND LOWER(TRIM(pf.manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "  ) "
            "GROUP BY t.db_name "
            "HAVING SUM(CASE WHEN t.total_points_ros_managed IS NULL THEN 1 ELSE 0 END) > "
            "  GREATEST(5, 0.25 * COUNT(*))"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 29. txn_net_points_ros_populated
    #     NOTE: net_points_ros does not exist in ___leagues DDL.
    #     Check removed — column was never added to centralized schema.
    # 30. txn_fa_lamar_populated
    #     FA LAMAR joins transactions.NFL_player_id to player_fantasy. Only populated
    #     when the player has at least one player_fantasy row AT OR AFTER the
    #     transaction week. NULL is expected for trade_picks, commish moves,
    #     post-season-end adds (week >= 18), and adds of players who never
    #     appeared again that year (e.g., injured, cut, suspended).
    Check(
        name="txn_fa_lamar_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="fa_lamar_ros NULL on add-type transaction with resolvable NFL player",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}transactions t "
            "WHERE db_name IN ({league_list}) "
            "  AND fa_lamar_ros IS NULL "
            f"  AND transaction_type IN {_ADD_TYPES} "
            "  AND NFL_player_id IS NOT NULL "
            "  AND (t.week IS NULL OR t.week < 18) "
            "  AND EXISTS ("
            "    SELECT 1 FROM {table_prefix}player_fantasy pf "
            "    WHERE pf.db_name = t.db_name "
            "      AND pf.year = t.year "
            "      AND pf.NFL_player_id = t.NFL_player_id "
            "      AND (t.week IS NULL OR pf.week >= t.week) "
            "  ) "
            "GROUP BY db_name"
        ),
        batch_group="quality",
        cost_tier="moderate",
        fix_action="reimport",
    ),
    # 31. txn_destination_populated
    #     destination_franchise_id is intentionally cleared by duplicate_trade_rows() in
    #     trade_utils.py — each row is already a per-manager perspective where the
    #     manager IS the destination (received) or source_manager IS the destination (sent).
    #     Instead, verify that the counterparty routing is populated via source_franchise_id.
    Check(
        name="txn_destination_populated",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="source_franchise_id NULL for trade row (counterparty not resolved)",
        sql_expr=(
            "SUM(CASE WHEN transaction_type IN ('trade', 'trade_pick') "
            "AND source_franchise_id IS NULL "
            "THEN 1 ELSE 0 END)"
        ),
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 32. txn_traded_pick_columns (dynasty leagues)
    Check(
        name="txn_traded_pick_columns",
        page="transactions",
        table="transactions",
        severity="WARNING",
        description="traded_pick_round NULL for trade_pick row (dynasty league)",
        sql_expr=(
            "SUM(CASE WHEN transaction_type = 'trade_pick' " "AND traded_pick_round IS NULL " "THEN 1 ELSE 0 END)"
        ),
        feature="dynasty",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
]
