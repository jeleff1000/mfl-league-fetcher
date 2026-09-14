"""Shared SQL feature universe for draft intelligence miners."""

from __future__ import annotations

from multi_league.transformations.aggregation.aggregation_utils import central_table


def quote_sql(value: str) -> str:
    """Quote a SQL string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def draft_table() -> str:
    return central_table("draft")


def bio_table() -> str:
    return "___ops.nfl_historical.player_bio"


def stats_table() -> str:
    return "___ops.nfl_historical.nfl_player_stats_all"


def prior_awards_table() -> str:
    return "___ops.nfl_historical.player_prior_awards_by_year"


def build_draft_feature_ctes(db_name: str, *, include_prior_awards: bool = False) -> str:
    """Return common draft feature CTEs used by discovery miners.

    The feature vocabulary intentionally only uses information knowable at draft
    time: draft capital/context, player bio, and prior-year NFL production.
    """

    db_lit = quote_sql(db_name)
    awards_join = (
        f"LEFT JOIN {prior_awards_table()} pa "
        "ON b.NFL_player_id = pa.NFL_player_id AND b.year = CAST(pa.draft_year AS INTEGER)"
        if include_prior_awards
        else ""
    )
    prior_allpro = "COALESCE(pa.prior_allpro_seasons, 0)" if include_prior_awards else "0"
    prior_probowl = "COALESCE(pa.prior_probowl_seasons, 0)" if include_prior_awards else "0"
    prior_award_wins = "COALESCE(pa.prior_major_award_wins, 0)" if include_prior_awards else "0"
    prior_award_votes = "COALESCE(pa.prior_award_vote_mentions, 0)" if include_prior_awards else "0"
    prev_allpro = "COALESCE(pa.prev_allpro_seasons, 0)" if include_prior_awards else "0"
    prev_probowl = "COALESCE(pa.prev_probowl_seasons, 0)" if include_prior_awards else "0"
    prev_award_wins = "COALESCE(pa.prev_major_award_wins, 0)" if include_prior_awards else "0"
    prev_award_votes = "COALESCE(pa.prev_award_vote_mentions, 0)" if include_prior_awards else "0"
    award_feature_sql = (
        """
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'award_history',
           CASE
               WHEN prev_major_award_wins >= 1 THEN 'prev_major_award'
               WHEN prev_allpro_seasons >= 1 THEN 'prev_allpro'
               WHEN prev_probowl_seasons >= 1 THEN 'prev_probowl'
               WHEN prev_award_vote_mentions >= 1 THEN 'prev_award_mention'
               WHEN prior_major_award_wins >= 1 THEN 'prior_major_award'
               WHEN prior_allpro_seasons >= 2 THEN 'prior_multi_allpro'
               WHEN prior_allpro_seasons >= 1 THEN 'prior_allpro'
               WHEN prior_probowl_seasons >= 1 THEN 'prior_probowl'
               WHEN prior_award_vote_mentions >= 1 THEN 'prior_award_mention'
               ELSE 'no_prior_awards'
           END
    FROM enriched
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'bio_profile',
           CASE
               WHEN (prior_allpro_seasons + prior_probowl_seasons + prior_major_award_wins + prior_award_vote_mentions) >= 2
               THEN 'decorated_veteran'
               ELSE 'undecorated_veteran'
           END
    FROM enriched
    WHERE experience_bucket = 'experience_veteran'
"""
        if include_prior_awards
        else ""
    )
    return f"""
WITH draft_base AS (
    SELECT
        d.db_name,
        d.year,
        COALESCE(d.franchise_id, d.manager) AS manager_key,
        d.manager,
        d.NFL_player_id,
        d.position,
        COALESCE(d.cost, 0) AS cost,
        d.pick,
        d.round,
        d.draft_type,
        d.cost_bucket,
        d.nfl_team,
        d.nfl_team_api,
        d.position_draft_label,
        d.draft_age,
        d.draft_age_grade,
        d.drafted_as_starter,
        d.manager_lamar,
        d.expected_lamar,
        d.pick_score,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper
    FROM {draft_table()} d
    WHERE d.db_name = {db_lit}
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
),
with_order AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.year) AS max_pick,
        ROW_NUMBER() OVER (
            PARTITION BY b.year
            ORDER BY COALESCE(b.pick, b.round, 9999), b.manager_key, b.NFL_player_id
        ) AS pick_order,
        COUNT(*) OVER (PARTITION BY b.year) AS picks_in_year,
        SUM(CASE WHEN COALESCE(b.cost, 0) > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY b.year) AS paid_picks_in_year,
        UPPER(COALESCE(NULLIF(b.position, ''), 'UNK')) AS position_group
    FROM draft_base b
    WHERE b.is_keeper = 0
),
with_capital AS (
    SELECT
        b.*,
        CASE
            WHEN b.cost > 0 THEN GREATEST(b.cost, 1)
            ELSE GREATEST(b.max_pick + 1 - COALESCE(b.pick, b.round, b.max_pick), 1)
        END AS capital_weight,
        CASE
            WHEN LOWER(COALESCE(b.draft_type, '')) IN ('live', 'auction', 'offline')
              OR b.paid_picks_in_year >= b.picks_in_year * 0.25
            THEN 'auction'
            ELSE 'snake'
        END AS draft_format,
        CASE
            WHEN b.cost > 0 THEN
                CASE
                    WHEN b.cost >= 50 THEN 'auction_50_plus'
                    WHEN b.cost >= 30 THEN 'auction_30_49'
                    WHEN b.cost >= 16 THEN 'auction_16_29'
                    WHEN b.cost >= 6 THEN 'auction_6_15'
                    ELSE 'auction_1_5'
                END
            ELSE
                CASE
                    WHEN COALESCE(b.round, 99) <= 2 THEN 'snake_r1_2'
                    WHEN COALESCE(b.round, 99) <= 5 THEN 'snake_r3_5'
                    WHEN COALESCE(b.round, 99) <= 9 THEN 'snake_r6_9'
                    ELSE 'snake_r10_plus'
                END
        END AS capital_bucket,
        CASE
            WHEN b.cost >= 30 OR COALESCE(b.round, 99) <= 2 THEN 'capital_premium'
            WHEN b.cost >= 16 OR COALESCE(b.round, 99) <= 5 THEN 'capital_core'
            WHEN b.cost >= 6 OR COALESCE(b.round, 99) <= 9 THEN 'capital_depth'
            ELSE 'capital_flyer'
        END AS capital_tier,
        CASE
            WHEN b.pick_order <= b.picks_in_year * 0.25 THEN 'phase_early'
            WHEN b.pick_order <= b.picks_in_year * 0.65 THEN 'phase_middle'
            ELSE 'phase_late'
        END AS draft_phase
    FROM with_order b
),
prior_stats AS (
    SELECT
        NFL_player_id,
        CAST(year AS INTEGER) + 1 AS draft_year,
        SUM(COALESCE(rushing_yards, 0)) AS prior_rush_yards,
        SUM(COALESCE(carries, 0)) AS prior_carries,
        SUM(COALESCE(targets, 0)) AS prior_targets,
        SUM(COALESCE(receptions, 0)) AS prior_receptions,
        SUM(COALESCE(receiving_yards, 0)) AS prior_receiving_yards,
        MAX(COALESCE(target_share, 0)) AS prior_target_share,
        MAX(COALESCE(wopr, 0)) AS prior_wopr,
        COUNT(DISTINCT week) AS prior_games
    FROM {stats_table()}
    WHERE NFL_player_id IS NOT NULL
      AND year IS NOT NULL
      AND (season_type IS NULL OR season_type = 'REG')
    GROUP BY NFL_player_id, CAST(year AS INTEGER) + 1
),
enriched_base AS (
    SELECT
        b.*,
        CASE
            WHEN (
                SELECT COUNT(*)
                FROM with_capital prev
                WHERE prev.year = b.year
                  AND prev.pick_order BETWEEN b.pick_order - 5 AND b.pick_order - 1
                  AND prev.position_group = b.position_group
            ) >= 3 THEN 'same_position_run'
            WHEN (
                SELECT COUNT(*)
                FROM with_capital prev
                WHERE prev.year = b.year
                  AND prev.pick_order BETWEEN b.pick_order - 5 AND b.pick_order - 1
                  AND prev.position_group = b.position_group
            ) >= 1 THEN 'same_position_recent'
            ELSE 'no_recent_same_position'
        END AS position_run_state,
        UPPER(NULLIF(NULLIF(b.nfl_team_api, ''), 'N/A')) AS nfl_team_at_draft,
        UPPER(COALESCE(NULLIF(b.nfl_team, ''), NULLIF(b.nfl_team_api, ''), NULLIF(pb.latest_team, ''))) AS current_nfl_team,
        UPPER(NULLIF(pb.nfl_draft_team, '')) AS nfl_draft_team,
        NULLIF(pb.college, '') AS college,
        NULLIF(pb.conference, '') AS conference,
        CASE
            WHEN pb.rookie_year IS NULL THEN NULL
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 0 THEN 'experience_rookie'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) = 1 THEN 'experience_year_2'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 3 THEN 'experience_year_3_4'
            ELSE 'experience_veteran'
        END AS experience_bucket,
        CASE
            WHEN COALESCE(pb.is_undrafted, 0) = 1 THEN 'undrafted'
            WHEN pb.draft_round IS NULL THEN NULL
            WHEN pb.draft_round <= 1 THEN 'nfl_round_1'
            WHEN pb.draft_round <= 3 THEN 'nfl_round_2_3'
            WHEN pb.draft_round <= 7 THEN 'nfl_round_4_7'
            ELSE 'nfl_other'
        END AS nfl_draft_capital,
        CASE
            WHEN pb.ras_score IS NULL THEN NULL
            WHEN pb.ras_score >= 9 THEN 'ras_9_plus'
            WHEN pb.ras_score >= 8 THEN 'ras_8_9'
            WHEN pb.ras_score >= 6 THEN 'ras_6_8'
            ELSE 'ras_under_6'
        END AS ras_bucket,
        CASE
            WHEN b.draft_age IS NULL OR b.draft_age <= 0 THEN NULL
            WHEN b.draft_age <= 23 THEN 'age_23_under'
            WHEN b.draft_age <= 26 THEN 'age_24_26'
            WHEN b.draft_age <= 29 THEN 'age_27_29'
            ELSE 'age_30_plus'
        END AS age_bucket,
        CASE
            WHEN pb.height IS NULL THEN NULL
            WHEN pb.height >= 77 THEN 'height_77_plus'
            WHEN pb.height >= 73 THEN 'height_73_76'
            WHEN pb.height >= 69 THEN 'height_69_72'
            ELSE 'height_under_69'
        END AS height_bucket,
        CASE
            WHEN pb.weight IS NULL THEN NULL
            WHEN pb.weight >= 250 THEN 'weight_250_plus'
            WHEN pb.weight >= 220 THEN 'weight_220_249'
            WHEN pb.weight >= 200 THEN 'weight_200_219'
            ELSE 'weight_under_200'
        END AS weight_bucket,
        COALESCE(ps.prior_rush_yards, 0) AS prior_rush_yards,
        COALESCE(ps.prior_carries, 0) AS prior_carries,
        COALESCE(ps.prior_targets, 0) AS prior_targets,
        COALESCE(ps.prior_receptions, 0) AS prior_receptions,
        COALESCE(ps.prior_receiving_yards, 0) AS prior_receiving_yards,
        COALESCE(ps.prior_target_share, 0) AS prior_target_share,
        COALESCE(ps.prior_wopr, 0) AS prior_wopr,
        COALESCE(ps.prior_games, 0) AS prior_games,
        {prev_allpro} AS prev_allpro_seasons,
        {prev_probowl} AS prev_probowl_seasons,
        {prev_award_wins} AS prev_major_award_wins,
        {prev_award_votes} AS prev_award_vote_mentions,
        {prior_allpro} AS prior_allpro_seasons,
        {prior_probowl} AS prior_probowl_seasons,
        {prior_award_wins} AS prior_major_award_wins,
        {prior_award_votes} AS prior_award_vote_mentions
    FROM with_capital b
    LEFT JOIN {bio_table()} pb ON b.NFL_player_id = pb.NFL_player_id
    LEFT JOIN prior_stats ps ON b.NFL_player_id = ps.NFL_player_id AND b.year = ps.draft_year
    {awards_join}
),
enriched_ranked AS (
    SELECT
        eb.*,
        ROW_NUMBER() OVER (
            PARTITION BY eb.year, eb.manager_key, eb.position_group
            ORDER BY eb.capital_weight DESC, eb.pick_order, eb.NFL_player_id
        ) AS position_slot_index
    FROM enriched_base eb
),
enriched AS (
    SELECT
        er.*,
        CASE
            WHEN er.position_group IN ('QB', 'RB', 'WR', 'TE')
            THEN er.position_group || CAST(CASE WHEN er.position_slot_index >= 5 THEN 5 ELSE er.position_slot_index END AS VARCHAR)
            ELSE NULL
        END AS position_slot_label,
        CASE
            WHEN er.position_group IN ('QB', 'RB', 'WR', 'TE') AND er.position_slot_index = 1 THEN er.position_group || '_anchor'
            WHEN er.position_group IN ('QB', 'RB', 'WR', 'TE') AND er.position_slot_index = 2 THEN er.position_group || '_second'
            WHEN er.position_group IN ('QB', 'RB', 'WR', 'TE') AND er.position_slot_index <= 4 THEN er.position_group || '_depth'
            WHEN er.position_group IN ('QB', 'RB', 'WR', 'TE') THEN er.position_group || '_deep_depth'
            ELSE NULL
        END AS position_slot_tier
    FROM enriched_ranked er
),
features AS (
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'current_nfl_team' AS feature_type, current_nfl_team AS feature_value
    FROM enriched WHERE current_nfl_team IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'nfl_team_at_draft', nfl_team_at_draft
    FROM enriched WHERE nfl_team_at_draft IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'nfl_draft_team', nfl_draft_team
    FROM enriched WHERE nfl_draft_team IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'college', college
    FROM enriched WHERE college IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'conference', conference
    FROM enriched WHERE conference IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'nfl_draft_capital', nfl_draft_capital
    FROM enriched WHERE nfl_draft_capital IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'ras_bucket', ras_bucket
    FROM enriched WHERE ras_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'age_bucket', age_bucket
    FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'height_bucket', height_bucket
    FROM enriched WHERE height_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'weight_bucket', weight_bucket
    FROM enriched WHERE weight_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'experience_bucket', experience_bucket
    FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_age', position_group || '_' || age_bucket
    FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_experience', position_group || '_' || experience_bucket
    FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_height', position_group || '_' || height_bucket
    FROM enriched WHERE height_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_weight', position_group || '_' || weight_bucket
    FROM enriched WHERE weight_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_ras', position_group || '_' || ras_bucket
    FROM enriched WHERE ras_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'draft_phase', draft_phase
    FROM enriched WHERE draft_phase IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'drafted_as_starter', CASE WHEN COALESCE(drafted_as_starter, 0) = 1 THEN 'starter' ELSE 'backup' END
    FROM enriched WHERE drafted_as_starter IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'draft_age_grade', draft_age_grade
    FROM enriched WHERE draft_age_grade IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_draft_label', position_draft_label
    FROM enriched WHERE position_draft_label IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'draft_state', draft_phase || '_' || position_run_state
    FROM enriched
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'QB_mobile'
    FROM enriched WHERE position_group = 'QB' AND (prior_rush_yards >= 350 OR prior_carries >= 60)
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'RB_rookie'
    FROM enriched WHERE position_group = 'RB' AND experience_bucket = 'experience_rookie'
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'RB_pass_catcher'
    FROM enriched WHERE position_group = 'RB' AND (prior_receptions >= 40 OR prior_targets >= 55)
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'RB_workhorse'
    FROM enriched WHERE position_group = 'RB' AND prior_carries >= 200
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'WR_target_earner'
    FROM enriched WHERE position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55)
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'TE_target_earner'
    FROM enriched WHERE position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45)
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'TE_elite_athlete'
    FROM enriched WHERE position_group = 'TE' AND ras_bucket = 'ras_9_plus'
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'player_archetype', 'prior_injury_discount'
    FROM enriched WHERE prior_games BETWEEN 1 AND 8
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'qb_mobility_profile',
           CASE WHEN prior_rush_yards >= 350 OR prior_carries >= 60 THEN 'mobile_qb' ELSE 'pocket_or_low_rush_qb' END
    FROM enriched WHERE position_group = 'QB' AND prior_games >= 4
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'rb_receiving_profile',
           CASE WHEN prior_receptions >= 40 OR prior_targets >= 55 THEN 'pass_catching_rb' ELSE 'non_receiving_rb' END
    FROM enriched WHERE position_group = 'RB' AND prior_games >= 4
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'rb_workload_profile',
           CASE WHEN prior_carries >= 200 THEN 'workhorse_rb' ELSE 'committee_or_low_carry_rb' END
    FROM enriched WHERE position_group = 'RB' AND prior_games >= 4
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'receiving_volume_profile',
           CASE
               WHEN position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55) THEN 'WR_target_earner'
               WHEN position_group = 'WR' THEN 'WR_low_volume_profile'
               WHEN position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45) THEN 'TE_target_earner'
               ELSE 'TE_low_volume_profile'
           END
    FROM enriched WHERE position_group IN ('WR', 'TE') AND prior_games >= 4
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, position_group, capital_bucket, draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'availability_profile',
           CASE
               WHEN prior_games BETWEEN 1 AND 8 THEN 'prior_injury_discount'
               WHEN prior_games BETWEEN 9 AND 13 THEN 'partial_prior_season'
               ELSE 'full_prior_season'
           END
    FROM enriched WHERE prior_games > 0
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'capital_bucket', capital_bucket
    FROM enriched WHERE capital_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'capital_tier', capital_tier
    FROM enriched WHERE capital_tier IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_capital_bucket', position_group || '_' || capital_bucket
    FROM enriched WHERE position_group IS NOT NULL AND capital_bucket IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_capital_tier', position_group || '_' || capital_tier
    FROM enriched WHERE position_group IS NOT NULL AND capital_tier IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_draft_phase', position_group || '_' || draft_phase
    FROM enriched WHERE position_group IS NOT NULL AND draft_phase IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'format_position', draft_format || '_' || position_group
    FROM enriched WHERE draft_format IS NOT NULL AND position_group IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'format_capital_tier', draft_format || '_' || capital_tier
    FROM enriched WHERE draft_format IS NOT NULL AND capital_tier IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_slot', position_slot_label
    FROM enriched WHERE position_slot_label IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_slot_tier', position_slot_tier
    FROM enriched WHERE position_slot_tier IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_slot_capital_tier', position_slot_label || '_' || capital_tier
    FROM enriched WHERE position_slot_label IS NOT NULL AND capital_tier IS NOT NULL
    UNION ALL
    SELECT db_name, year, manager_key, manager, NFL_player_id, 'ALL', 'ALL', draft_phase,
           position_run_state, capital_weight, manager_lamar, expected_lamar, pick_score,
           'position_slot_phase', position_slot_label || '_' || draft_phase
    FROM enriched WHERE position_slot_label IS NOT NULL AND draft_phase IS NOT NULL
    {award_feature_sql}
)
"""
