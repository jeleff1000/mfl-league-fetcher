"""Keeper checks — 6 checks verifying keeper-league specific columns.

All checks are feature-gated on "keeper" and will only run for leagues
in the manifest's keeper_leagues list.

Checks run against player_fantasy (weekly) or draft tables.
The {table_prefix} and {league_list} placeholders are filled by the executor.
"""

from __future__ import annotations

from multi_league.validation_v2.models import Check
from multi_league.validation_v2.scope_sql import published_league_scope_sql

_PUBLISHED_SCOPE = published_league_scope_sql()

CHECKS: list[Check] = [
    # ------------------------------------------------------------------
    # WARNING: Keeper column presence and population — sql_expr (batched)
    # ------------------------------------------------------------------
    # 1. keepers_keeper_column_exists
    #    is_keeper column should not be all NULL for a keeper league.
    #    Returns leagues where is_keeper is entirely NULL (flag missing).
    Check(
        name="keepers_keeper_column_exists",
        page="keepers",
        table="player_fantasy",
        severity="WARNING",
        description="is_keeper column all NULL for keeper league (flag missing)",
        sql_full=(
            "SELECT db_name, 1 AS fail_count "
            "FROM {table_prefix}player_fantasy "
            "WHERE db_name IN ({league_list}) "
            "GROUP BY db_name "
            "HAVING SUM(CASE WHEN is_keeper IS NOT NULL THEN 1 ELSE 0 END) = 0"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 2. keepers_keeper_year_populated
    #    keeper_year should be populated for rows where is_keeper = 1.
    Check(
        name="keepers_keeper_year_populated",
        page="keepers",
        table="player_fantasy",
        severity="WARNING",
        description="keeper_year NULL where is_keeper = 1",
        sql_expr=("SUM(CASE WHEN is_keeper = 1 " "AND keeper_year IS NULL " "THEN 1 ELSE 0 END)"),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 3: REMOVED — base_keeper_cost is join-derived, populated at query time.
    # 4. keepers_keeper_price_populated
    #    Stage 2 of the keeper-config refactor (2026-05-02) wired backend
    #    apply_keeper_rules into Wave 4 of SQLEnrichments.run_all. Any league
    #    with a configured + enabled keeper_config row should get keeper_price
    #    populated automatically on import. Leagues WITHOUT configured rules
    #    legitimately have NULL keeper_price (most leagues — owners haven't
    #    visited the wizard) and are excluded via the JOIN below.
    #
    #    Promoted INFO → WARNING after QA proved auto-apply works on
    #    nyu_ffl (snake/round_escalation) + tfl_of_extraordinary_gentleman
    #    (auction/compounding) — see handoff for verification.
    Check(
        name="keepers_keeper_price_populated",
        page="keepers",
        table="player_fantasy",
        severity="WARNING",
        description="keeper_price NULL where base_keeper_cost populated AND league has enabled keeper_config",
        sql_full=(
            "SELECT pf.db_name, "
            "  SUM(CASE WHEN pf.base_keeper_cost IS NOT NULL "
            "           AND pf.keeper_price IS NULL "
            "           THEN 1 ELSE 0 END) AS fail_count "
            "FROM {table_prefix}player_fantasy pf "
            "JOIN {table_prefix}keeper_config kc "
            "  ON kc.db_name = pf.db_name AND kc.year = 0 "
            " AND COALESCE(kc.enabled, FALSE) = TRUE "
            "WHERE pf.db_name IN ({league_list}) "
            "GROUP BY pf.db_name "
            "HAVING fail_count > 0"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 5. keepers_max_faab_populated
    #    max_faab_bid_to_date is used to compute keeper cost in redraft
    #    FAAB-keeper leagues. Scope must exclude:
    #      - Dynasty leagues: carry keepers via dynasty rules, not FAAB cost.
    #      - Non-keeper leagues: FAAB exists for waiver auctions, not keeper
    #        cost tracking. These legitimately have NULL max_faab_bid_to_date.
    #        demo_league is the canonical example: FAAB waivers + no keepers.
    #    A league-year qualifies only when is_dynasty=false AND max_keepers>0.
    #
    #    The check flags a (db_name, year, NFL_player_id) combination only
    #    when NO player_fantasy row for that player has a populated
    #    max_faab_bid_to_date. Per-week NULLs are legitimate — the column
    #    carries forward the max bid from the acquisition week, so weeks
    #    BEFORE the FAAB transaction (e.g. a player who was drafted then
    #    later FAAB'd) correctly show NULL. Only a total absence for a
    #    player indicates a pipeline miss.
    Check(
        name="keepers_max_faab_populated",
        page="keepers",
        table="player_fantasy",
        severity="WARNING",
        description="max_faab_bid_to_date NULL for FAAB-acquired rostered player (redraft faab-keeper league)",
        sql_full=(
            "WITH keeper_league_years AS ("
            "  SELECT DISTINCT db_name, year "
            "  FROM {table_prefix}league_settings "
            "  WHERE db_name IN ({league_list}) "
            "    AND COALESCE(is_dynasty, false) = false "
            "    AND COALESCE(max_keepers, 0) > 0 "
            "), "
            # Pull (db, year, player, manager, cumulative_week) from real
            # FAAB acquisitions. Both axes are required to scope correctly:
            #   - Manager: only the manager who actually spent FAAB on the
            #     player should be expected to have max_faab_bid_to_date
            #     populated on their rostered weeks. A separate manager who
            #     added the player via $0 claim earlier in the year and
            #     then dropped him is not in the FAAB scope.
            #   - cumulative_week: the FAAB bid is only valid from the
            #     acquisition week forward. If a manager had the player
            #     rostered for weeks 5-8 (e.g. $0 claim), dropped him,
            #     then re-added via $1 FAAB at week 10, weeks 5-8 have
            #     NULL max_faab_bid_to_date legitimately (pre-FAAB weeks),
            #     and only week-10-onwards rostered rows should count.
            "faab_acquisitions AS ("
            "  SELECT t.db_name, t.year, t.NFL_player_id, t.manager, "
            "         MIN(t.cumulative_week) AS first_faab_cw "
            "  FROM {table_prefix}transactions t "
            "  INNER JOIN keeper_league_years kly "
            "    ON kly.db_name = t.db_name AND kly.year = t.year "
            "  WHERE t.db_name IN ({league_list}) "
            "    AND t.NFL_player_id IS NOT NULL "
            "    AND t.manager IS NOT NULL "
            "    AND t.faab_bid IS NOT NULL "
            "    AND t.faab_bid > 0 "
            "    AND t.transaction_type IN ('add', 'pickup', 'claim', 'waiver') "
            "  GROUP BY t.db_name, t.year, t.NFL_player_id, t.manager "
            "), "
            "eligible_players AS ("
            "  SELECT pf.db_name, pf.year, pf.NFL_player_id, pf.manager, "
            "    MAX(CASE WHEN pf.max_faab_bid_to_date IS NOT NULL THEN 1 ELSE 0 END) AS has_any "
            "  FROM {table_prefix}player_fantasy pf "
            "  INNER JOIN faab_acquisitions fa "
            "    ON fa.db_name = pf.db_name "
            "   AND fa.year = pf.year "
            "   AND fa.NFL_player_id = pf.NFL_player_id "
            "   AND fa.manager = pf.manager "
            "  WHERE pf.db_name IN ({league_list}) "
            "    AND pf.manager IS NOT NULL "
            "    AND TRIM(COALESCE(pf.manager, '')) != '' "
            "    AND LOWER(TRIM(pf.manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers') "
            "    AND pf.cumulative_week >= fa.first_faab_cw "
            "  GROUP BY pf.db_name, pf.year, pf.NFL_player_id, pf.manager "
            "), "
            "faab_counts AS ("
            "  SELECT db_name, "
            "    SUM(CASE WHEN has_any = 0 THEN 1 ELSE 0 END) AS fail_count, "
            "    COUNT(*) AS faab_players "
            "  FROM eligible_players "
            "  GROUP BY db_name"
            ") "
            "SELECT db_name, fail_count "
            "FROM faab_counts "
            "WHERE fail_count > 0 AND faab_players > 0"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
        fix_action="reimport",
    ),
    # 6. keepers_config_valid
    #    keeper_config table should have at least one row for keeper leagues.
    #    INFO severity: keeper_config is populated by the frontend admin UI
    #    (/api/league/[db]/keeper-config), not by the import pipeline. Empty
    #    rows for a keeper league signal "owner has not configured keeper
    #    rules yet" — expected steady state, not a data-quality bug. Same
    #    reasoning as keepers_keeper_price_populated above.
    Check(
        name="keepers_config_valid",
        page="keepers",
        table="keeper_config",
        severity="INFO",
        description="keeper_config table has no rows for keeper league (expected until owner configures rules via admin UI)",
        sql_full=(
            "SELECT ki.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) ki "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}keeper_config"
            ") kc ON ki.db_name = kc.db_name "
            "WHERE kc.db_name IS NULL"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
    ),
    Check(
        name="keepers_config_global_row_exists",
        page="keepers",
        table="keeper_config",
        severity="INFO",
        description="keeper_config missing the global year=0 row (expected until owner configures rules via admin UI)",
        sql_full=(
            "SELECT ki.db_name, 1 AS fail_count "
            f"FROM ({_PUBLISHED_SCOPE}) ki "
            "LEFT JOIN ("
            "  SELECT DISTINCT db_name "
            "  FROM {table_prefix}keeper_config "
            "  WHERE year = 0"
            ") kc ON ki.db_name = kc.db_name "
            "WHERE kc.db_name IS NULL"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
    ),
    # Renamed from keepers_config_rules_json_populated (the rules_json column
    # was dropped in Stage 2 of the keeper-config DDL migration, 2026-05-02).
    # Equivalent semantic on the new flat schema: a year=0 row exists but the
    # required scalars (enabled, draft_type) are NULL — i.e., a corrupt config.
    Check(
        name="keepers_config_required_scalars_populated",
        page="keepers",
        table="keeper_config",
        severity="WARNING",
        description="keeper_config year=0 row has NULL enabled or draft_type",
        sql_full=(
            "SELECT db_name, COUNT(*) AS fail_count "
            "FROM {table_prefix}keeper_config "
            "WHERE db_name IN ({league_list}) "
            "  AND year = 0 "
            "  AND (enabled IS NULL OR draft_type IS NULL) "
            "GROUP BY db_name"
        ),
        feature="keeper",
        batch_group="quality",
        cost_tier="cheap",
    ),
]
