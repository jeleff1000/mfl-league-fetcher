#!/usr/bin/env python3
"""Learn draft profile archetypes from live Fly data in safe chunks.

This is a preview/learning runner: it reads normalized central tables from Fly,
mines manager draft-profile signals in league batches, then feeds those signals
through the archetype learner. It writes local JSON artifacts only.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timezone, UTC
import json
import os
from pathlib import Path
import re
import statistics
import time
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
DEFAULT_PRIOR_AWARDS_PARQUET = REPO_ROOT / "artifacts" / "player_prior_awards_by_year.parquet"
import sys

for module_path in (REPO_ROOT, SCRIPTS_ROOT):
    path_str = str(module_path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.core.readers.fly_reader import FlyReader
from multi_league.transformations.draft.profile_archetypes import (
    assign_profile_archetypes,
    learn_profile_archetypes,
)
from multi_league.transformations.draft.state_registry import classify_feature


def _load_env() -> None:
    for name in [".env", ".env.local"]:
        path = Path(name)
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _q(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _slug(value: str, max_len: int = 120) -> str:
    return re.sub(r"[^a-z0-9]+", "_", str(value).lower()).strip("_")[:max_len] or "unknown"


def _chunks(values: list[str], size: int):
    for idx in range(0, len(values), size):
        yield values[idx : idx + size]


def fetch_leagues(reader: FlyReader, limit: int | None = None) -> list[dict[str, Any]]:
    sql = """
    SELECT
        db_name,
        COUNT(*) AS picks,
        COUNT(DISTINCT year) AS seasons,
        MIN(year) AS min_year,
        MAX(year) AS max_year,
        SUM(CASE WHEN COALESCE(cost, 0) > 0 THEN 1 ELSE 0 END) AS paid_picks,
        SUM(CASE WHEN COALESCE(is_keeper, 0) = 1 OR LOWER(COALESCE(draft_category, '')) = 'keeper' THEN 1 ELSE 0 END) AS keeper_picks
    FROM public.draft
    WHERE db_name IS NOT NULL
      AND year IS NOT NULL
      AND manager IS NOT NULL
      AND NFL_player_id IS NOT NULL
    GROUP BY db_name
    HAVING COUNT(*) >= 80
    ORDER BY picks DESC
    """
    if limit:
        sql += f"\nLIMIT {int(limit)}"
    return reader.query(sql, database="___leagues")


def build_manager_signal_sql(
    db_names: list[str],
    *,
    min_abs_z: float,
    min_repeatability: float,
    min_picks: int,
    min_expected_capital: float,
    per_batch_limit: int,
    prior_awards_table: str | None = None,
) -> str:
    db_list = ", ".join(_q(name) for name in db_names)
    awards_join = (
        f"LEFT JOIN {prior_awards_table} pa "
        "ON b.NFL_player_id = pa.NFL_player_id AND b.year = CAST(pa.draft_year AS INTEGER)"
        if prior_awards_table
        else ""
    )
    prior_allpro = "COALESCE(pa.prior_allpro_seasons, 0)" if prior_awards_table else "0"
    prior_probowl = "COALESCE(pa.prior_probowl_seasons, 0)" if prior_awards_table else "0"
    prior_award_wins = "COALESCE(pa.prior_major_award_wins, 0)" if prior_awards_table else "0"
    prior_award_votes = "COALESCE(pa.prior_award_vote_mentions, 0)" if prior_awards_table else "0"
    prev_allpro = "COALESCE(pa.prev_allpro_seasons, 0)" if prior_awards_table else "0"
    prev_probowl = "COALESCE(pa.prev_probowl_seasons, 0)" if prior_awards_table else "0"
    prev_award_wins = "COALESCE(pa.prev_major_award_wins, 0)" if prior_awards_table else "0"
    prev_award_votes = "COALESCE(pa.prev_award_vote_mentions, 0)" if prior_awards_table else "0"
    award_feature_sql = (
        """
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'award_history',
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
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'bio_profile',
        CASE
            WHEN (prior_allpro_seasons + prior_probowl_seasons + prior_major_award_wins + prior_award_vote_mentions) >= 2
            THEN 'decorated_veteran'
            ELSE 'undecorated_veteran'
        END
    FROM enriched
    WHERE experience_bucket = 'experience_veteran'
"""
        if prior_awards_table
        else ""
    )
    return f"""
WITH max_year AS (
    SELECT MAX(year) AS max_year FROM public.draft WHERE year IS NOT NULL
),
draft_base AS (
    SELECT
        d.db_name,
        d.year,
        d.db_name || ':' || COALESCE(NULLIF(d.franchise_id, ''), NULLIF(d.manager_guid, ''), NULLIF(d.manager, '')) AS manager_key,
        d.manager,
        d.NFL_player_id,
        UPPER(COALESCE(NULLIF(d.position, ''), 'UNK')) AS position_group,
        COALESCE(d.cost, 0) AS cost,
        d.pick,
        d.round,
        d.nfl_team,
        d.nfl_team_api,
        d.cost_bucket,
        d.position_draft_label,
        d.draft_age,
        d.draft_age_grade,
        d.drafted_as_starter,
        d.pick_score,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper
    FROM public.draft d
    WHERE d.db_name IN ({db_list})
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
),
with_capital AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.db_name, b.year) AS max_pick,
        ROW_NUMBER() OVER (
            PARTITION BY b.db_name, b.year
            ORDER BY COALESCE(b.pick, b.round, 9999), b.manager_key, b.NFL_player_id
        ) AS pick_order,
        COUNT(*) OVER (PARTITION BY b.db_name, b.year) AS picks_in_year
    FROM draft_base b
    WHERE b.is_keeper = 0
),
prior_stats AS (
    SELECT
        NFL_player_id,
        CAST(year AS INTEGER) + 1 AS draft_year,
        SUM(COALESCE(rushing_yards, 0)) AS prior_rush_yards,
        SUM(COALESCE(carries, 0)) AS prior_carries,
        SUM(COALESCE(targets, 0)) AS prior_targets,
        SUM(COALESCE(receptions, 0)) AS prior_receptions,
        MAX(COALESCE(target_share, 0)) AS prior_target_share,
        MAX(COALESCE(wopr, 0)) AS prior_wopr,
        COUNT(DISTINCT week) AS prior_games
    FROM ___ops.nfl_historical.nfl_player_stats_all
    WHERE NFL_player_id IS NOT NULL
      AND year >= 2002
      AND year IS NOT NULL
      AND (season_type IS NULL OR season_type = 'REG')
    GROUP BY NFL_player_id, CAST(year AS INTEGER) + 1
),
enriched AS (
    SELECT
        b.*,
        CASE
            WHEN b.cost > 0 THEN GREATEST(b.cost, 1)
            ELSE GREATEST(b.max_pick + 1 - COALESCE(b.pick, b.round, b.max_pick), 1)
        END AS capital_weight,
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
            WHEN b.pick_order <= b.picks_in_year * 0.25 THEN 'phase_early'
            WHEN b.pick_order <= b.picks_in_year * 0.65 THEN 'phase_middle'
            ELSE 'phase_late'
        END AS draft_phase,
        UPPER(COALESCE(NULLIF(b.nfl_team, ''), NULLIF(NULLIF(b.nfl_team_api, ''), 'N/A'))) AS current_nfl_team,
        UPPER(NULLIF(NULLIF(b.nfl_team_api, ''), 'N/A')) AS nfl_team_at_draft,
        UPPER(NULLIF(pb.nfl_draft_team, '')) AS nfl_draft_team,
        NULLIF(pb.college, '') AS college,
        NULLIF(pb.conference, '') AS conference,
        NULLIF(pb.position_category, '') AS position_category,
        NULLIF(pb.position_side, '') AS position_side,
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
    LEFT JOIN ___ops.nfl_historical.player_bio pb ON b.NFL_player_id = pb.NFL_player_id
    LEFT JOIN prior_stats ps ON b.NFL_player_id = ps.NFL_player_id AND b.year = ps.draft_year
    {awards_join}
),
features AS (
    SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, 'ALL' AS control_position, 'position' AS feature_type, position_group AS feature_value FROM enriched WHERE position_group IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'current_nfl_team', current_nfl_team FROM enriched WHERE current_nfl_team IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'nfl_team_at_draft', nfl_team_at_draft FROM enriched WHERE nfl_team_at_draft IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'nfl_draft_team', nfl_draft_team FROM enriched WHERE nfl_draft_team IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'college', college FROM enriched WHERE college IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'conference', conference FROM enriched WHERE conference IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'nfl_draft_capital', nfl_draft_capital FROM enriched WHERE nfl_draft_capital IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'ras_bucket', ras_bucket FROM enriched WHERE ras_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'age_bucket', age_bucket FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'height_bucket', height_bucket FROM enriched WHERE height_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'weight_bucket', weight_bucket FROM enriched WHERE weight_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'experience_bucket', experience_bucket FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_age', position_group || '_' || age_bucket FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_experience', position_group || '_' || experience_bucket FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'draft_phase', draft_phase FROM enriched WHERE draft_phase IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'drafted_as_starter', CASE WHEN COALESCE(drafted_as_starter, 0) = 1 THEN 'starter' ELSE 'backup' END FROM enriched WHERE drafted_as_starter IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'draft_age_grade', draft_age_grade FROM enriched WHERE draft_age_grade IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_draft_label', position_draft_label FROM enriched WHERE position_draft_label IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'QB_mobile' FROM enriched WHERE position_group = 'QB' AND (prior_rush_yards >= 350 OR prior_carries >= 60)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'RB_rookie' FROM enriched WHERE position_group = 'RB' AND experience_bucket = 'experience_rookie'
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'RB_pass_catcher' FROM enriched WHERE position_group = 'RB' AND (prior_receptions >= 40 OR prior_targets >= 55)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'RB_workhorse' FROM enriched WHERE position_group = 'RB' AND prior_carries >= 200
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'WR_target_earner' FROM enriched WHERE position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'TE_target_earner' FROM enriched WHERE position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'TE_elite_athlete' FROM enriched WHERE position_group = 'TE' AND ras_bucket = 'ras_9_plus'
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'prior_injury_discount' FROM enriched WHERE prior_games BETWEEN 1 AND 8
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'qb_mobility_profile',
        CASE WHEN prior_rush_yards >= 350 OR prior_carries >= 60 THEN 'mobile_qb' ELSE 'pocket_or_low_rush_qb' END
        FROM enriched WHERE position_group = 'QB' AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'rb_receiving_profile',
        CASE WHEN prior_receptions >= 40 OR prior_targets >= 55 THEN 'pass_catching_rb' ELSE 'non_receiving_rb' END
        FROM enriched WHERE position_group = 'RB' AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'rb_workload_profile',
        CASE WHEN prior_carries >= 200 THEN 'workhorse_rb' ELSE 'committee_or_low_carry_rb' END
        FROM enriched WHERE position_group = 'RB' AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'receiving_volume_profile',
        CASE
            WHEN position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55) THEN 'WR_target_earner'
            WHEN position_group = 'WR' THEN 'WR_low_volume_profile'
            WHEN position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45) THEN 'TE_target_earner'
            ELSE 'TE_low_volume_profile'
        END
        FROM enriched WHERE position_group IN ('WR', 'TE') AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'availability_profile',
        CASE
            WHEN prior_games BETWEEN 1 AND 8 THEN 'prior_injury_discount'
            WHEN prior_games BETWEEN 9 AND 13 THEN 'partial_prior_season'
            ELSE 'full_prior_season'
        END
        FROM enriched WHERE prior_games > 0
    {award_feature_sql}
),
manager_strata AS (
    SELECT db_name, year, manager_key, ANY_VALUE(manager) AS manager, feature_type, control_position, capital_bucket,
           SUM(capital_weight) AS manager_stratum_capital,
           SUM(capital_weight * capital_weight) AS manager_stratum_weight_sq
    FROM features
    GROUP BY db_name, year, manager_key, feature_type, control_position, capital_bucket
),
manager_feature AS (
    SELECT db_name, year, manager_key, ANY_VALUE(manager) AS manager, feature_type, feature_value, control_position, capital_bucket,
           COUNT(*) AS picks, SUM(capital_weight) AS observed_capital
    FROM features
    GROUP BY db_name, year, manager_key, feature_type, feature_value, control_position, capital_bucket
),
league_feature_rates AS (
    SELECT db_name, year, feature_type, feature_value, control_position, capital_bucket,
           SUM(capital_weight) AS feature_capital,
           SUM(SUM(capital_weight)) OVER (PARTITION BY db_name, year, feature_type, control_position, capital_bucket) AS total_capital
    FROM features
    GROUP BY db_name, year, feature_type, feature_value, control_position, capital_bucket
),
scored_strata AS (
    SELECT ms.manager_key, ms.manager, ms.db_name, lfr.feature_type, lfr.feature_value, ms.year,
           COALESCE(mf.picks, 0) AS picks,
           COALESCE(mf.observed_capital, 0) AS observed_capital,
           ms.manager_stratum_capital,
           GREATEST((ms.manager_stratum_capital * ms.manager_stratum_capital) / NULLIF(ms.manager_stratum_weight_sq, 0), 1.0) AS effective_n,
           lfr.feature_capital / NULLIF(lfr.total_capital, 0) AS expected_share,
           ms.manager_stratum_capital * (lfr.feature_capital / NULLIF(lfr.total_capital, 0)) AS expected_capital
    FROM manager_strata ms
    JOIN league_feature_rates lfr
      ON ms.db_name = lfr.db_name
     AND ms.year = lfr.year
     AND ms.feature_type = lfr.feature_type
     AND ms.control_position = lfr.control_position
     AND ms.capital_bucket = lfr.capital_bucket
    LEFT JOIN manager_feature mf
      ON mf.db_name = ms.db_name
     AND mf.year = ms.year
     AND mf.manager_key = ms.manager_key
     AND mf.feature_type = ms.feature_type
     AND mf.feature_value = lfr.feature_value
     AND mf.control_position = ms.control_position
     AND mf.capital_bucket = ms.capital_bucket
),
candidate_year AS (
    SELECT manager_key, ANY_VALUE(manager) AS manager, ANY_VALUE(db_name) AS db_name, feature_type, feature_value, year,
           SUM(picks) AS picks,
           SUM(observed_capital) AS observed_capital,
           SUM(expected_capital) AS expected_capital,
           SUM(manager_stratum_capital) AS manager_stratum_capital,
           SUM((observed_capital / NULLIF(manager_stratum_capital, 0)) * effective_n) AS observed_effective,
           SUM(expected_share * effective_n) AS expected_effective,
           SUM(effective_n * expected_share * (1 - expected_share)) AS approx_variance
    FROM scored_strata
    GROUP BY manager_key, feature_type, feature_value, year
),
rollup AS (
    SELECT manager_key, ANY_VALUE(manager) AS manager, ANY_VALUE(db_name) AS db_name, feature_type, feature_value,
           SUM(picks) AS picks,
           COUNT(DISTINCT year) AS years_seen,
           MIN(year) AS earliest_year,
           MAX(year) AS latest_year,
           SUM(observed_capital * POWER(0.5, ((SELECT max_year FROM max_year) - year) / 4.0)) / NULLIF(SUM(observed_capital), 0) AS recency_weight,
           SUM(CASE WHEN observed_capital > expected_capital THEN 1 ELSE 0 END) AS positive_years,
           SUM(CASE WHEN observed_capital < expected_capital THEN 1 ELSE 0 END) AS negative_years,
           SUM(observed_capital) AS observed_capital,
           SUM(expected_capital) AS expected_capital,
           SUM(manager_stratum_capital) AS manager_total_capital,
           SUM(observed_effective) AS observed_effective,
           SUM(expected_effective) AS expected_effective,
           SUM(approx_variance) AS approx_variance
    FROM candidate_year
    GROUP BY manager_key, feature_type, feature_value
),
ranked AS (
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           CASE WHEN observed_capital >= expected_capital THEN 'overweight' ELSE 'underweight' END AS signal_direction,
           CAST(picks AS INTEGER) AS picks,
           CAST(years_seen AS INTEGER) AS years_seen,
           CAST(earliest_year AS INTEGER) AS earliest_year,
           CAST(latest_year AS INTEGER) AS latest_year,
           ROUND(recency_weight, 4) AS recency_weight,
           CAST(positive_years AS INTEGER) AS positive_years,
           CAST(negative_years AS INTEGER) AS negative_years,
           ROUND(CASE WHEN observed_capital >= expected_capital THEN positive_years / NULLIF(years_seen, 0) ELSE negative_years / NULLIF(years_seen, 0) END, 4) AS repeatability,
           ROUND(observed_capital, 3) AS observed_capital,
           ROUND(expected_capital, 3) AS expected_capital,
           ROUND(observed_capital - expected_capital, 3) AS excess_capital,
           ROUND(observed_capital / NULLIF(manager_total_capital, 0), 4) AS observed_share,
           ROUND(expected_capital / NULLIF(manager_total_capital, 0), 4) AS expected_share,
           ROUND(observed_capital / NULLIF(expected_capital, 0), 4) AS lift,
           ROUND((observed_effective - expected_effective) / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0), 3) AS capital_z_score
    FROM rollup
    WHERE (picks >= {int(min_picks)} OR expected_capital >= {float(min_expected_capital) * 1.5})
      AND expected_capital >= {float(min_expected_capital)}
      AND years_seen >= 2
)
SELECT *
FROM ranked
WHERE ABS(capital_z_score) >= {float(min_abs_z)}
  AND repeatability >= {float(min_repeatability)}
ORDER BY ABS(capital_z_score) DESC, observed_capital DESC
LIMIT {int(per_batch_limit)}
"""


def build_manager_signal_sql_v2(
    db_names: list[str],
    *,
    min_abs_z: float,
    min_repeatability: float,
    min_picks: int,
    min_expected_capital: float,
    per_batch_limit: int,
    prior_awards_table: str | None = None,
) -> str:
    """Build the multi-lens fleet discovery query.

    The original preview query only asked one question: does this manager spend
    more/less capital than the room on a feature after controlling for position
    and draft tier? This version keeps that lens, then adds count and complete
    absence lenses, plus allocation features where controlling away draft tier
    would hide the behavior we are trying to discover.
    """
    db_list = ", ".join(_q(name) for name in db_names)
    awards_join = (
        f"LEFT JOIN {prior_awards_table} pa "
        "ON b.NFL_player_id = pa.NFL_player_id AND b.year = CAST(pa.draft_year AS INTEGER)"
        if prior_awards_table
        else ""
    )
    prior_allpro = "COALESCE(pa.prior_allpro_seasons, 0)" if prior_awards_table else "0"
    prior_probowl = "COALESCE(pa.prior_probowl_seasons, 0)" if prior_awards_table else "0"
    prior_award_wins = "COALESCE(pa.prior_major_award_wins, 0)" if prior_awards_table else "0"
    prior_award_votes = "COALESCE(pa.prior_award_vote_mentions, 0)" if prior_awards_table else "0"
    prev_allpro = "COALESCE(pa.prev_allpro_seasons, 0)" if prior_awards_table else "0"
    prev_probowl = "COALESCE(pa.prev_probowl_seasons, 0)" if prior_awards_table else "0"
    prev_award_wins = "COALESCE(pa.prev_major_award_wins, 0)" if prior_awards_table else "0"
    prev_award_votes = "COALESCE(pa.prev_award_vote_mentions, 0)" if prior_awards_table else "0"
    award_feature_sql = (
        """
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'award_history',
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
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'bio_profile',
        CASE
            WHEN (prior_allpro_seasons + prior_probowl_seasons + prior_major_award_wins + prior_award_vote_mentions) >= 2
            THEN 'decorated_veteran'
            ELSE 'undecorated_veteran'
        END
    FROM enriched
    WHERE experience_bucket = 'experience_veteran'
"""
        if prior_awards_table
        else ""
    )
    return f"""
WITH max_year AS (
    SELECT MAX(year) AS max_year FROM public.draft WHERE year IS NOT NULL
),
draft_base AS (
    SELECT
        d.db_name,
        d.year,
        d.db_name || ':' || COALESCE(NULLIF(d.franchise_id, ''), NULLIF(d.manager_guid, ''), NULLIF(d.manager, '')) AS manager_key,
        d.manager,
        d.NFL_player_id,
        UPPER(COALESCE(NULLIF(d.position, ''), 'UNK')) AS position_group,
        COALESCE(d.cost, 0) AS cost,
        d.pick,
        d.round,
        d.draft_type,
        d.nfl_team,
        d.nfl_team_api,
        d.cost_bucket,
        d.position_draft_label,
        d.draft_age,
        d.draft_age_grade,
        d.drafted_as_starter,
        d.pick_score,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper
    FROM public.draft d
    WHERE d.db_name IN ({db_list})
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
),
with_order AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.db_name, b.year) AS max_pick,
        ROW_NUMBER() OVER (
            PARTITION BY b.db_name, b.year
            ORDER BY COALESCE(b.pick, b.round, 9999), b.manager_key, b.NFL_player_id
        ) AS pick_order,
        COUNT(*) OVER (PARTITION BY b.db_name, b.year) AS picks_in_year,
        SUM(CASE WHEN COALESCE(b.cost, 0) > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY b.db_name, b.year) AS paid_picks_in_year
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
        MAX(COALESCE(target_share, 0)) AS prior_target_share,
        MAX(COALESCE(wopr, 0)) AS prior_wopr,
        COUNT(DISTINCT week) AS prior_games
    FROM ___ops.nfl_historical.nfl_player_stats_all
    WHERE NFL_player_id IS NOT NULL
      AND year >= 2002
      AND year IS NOT NULL
      AND (season_type IS NULL OR season_type = 'REG')
    GROUP BY NFL_player_id, CAST(year AS INTEGER) + 1
),
enriched_base AS (
    SELECT
        b.*,
        UPPER(COALESCE(NULLIF(b.nfl_team, ''), NULLIF(NULLIF(b.nfl_team_api, ''), 'N/A'))) AS current_nfl_team,
        UPPER(NULLIF(NULLIF(b.nfl_team_api, ''), 'N/A')) AS nfl_team_at_draft,
        UPPER(NULLIF(pb.nfl_draft_team, '')) AS nfl_draft_team,
        NULLIF(pb.college, '') AS college,
        NULLIF(pb.conference, '') AS conference,
        NULLIF(pb.position_category, '') AS position_category,
        NULLIF(pb.position_side, '') AS position_side,
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
    LEFT JOIN ___ops.nfl_historical.player_bio pb ON b.NFL_player_id = pb.NFL_player_id
    LEFT JOIN prior_stats ps ON b.NFL_player_id = ps.NFL_player_id AND b.year = ps.draft_year
    {awards_join}
),
enriched_ranked AS (
    SELECT
        eb.*,
        ROW_NUMBER() OVER (
            PARTITION BY eb.db_name, eb.year, eb.manager_key, eb.position_group
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
base_features AS (
    SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, 'ALL' AS control_position, 'position' AS feature_type, position_group AS feature_value FROM enriched WHERE position_group IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'current_nfl_team', current_nfl_team FROM enriched WHERE current_nfl_team IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'nfl_team_at_draft', nfl_team_at_draft FROM enriched WHERE nfl_team_at_draft IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'nfl_draft_team', nfl_draft_team FROM enriched WHERE nfl_draft_team IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'college', college FROM enriched WHERE college IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'conference', conference FROM enriched WHERE conference IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'nfl_draft_capital', nfl_draft_capital FROM enriched WHERE nfl_draft_capital IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'ras_bucket', ras_bucket FROM enriched WHERE ras_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'age_bucket', age_bucket FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'height_bucket', height_bucket FROM enriched WHERE height_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'weight_bucket', weight_bucket FROM enriched WHERE weight_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'experience_bucket', experience_bucket FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_age', position_group || '_' || age_bucket FROM enriched WHERE age_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_experience', position_group || '_' || experience_bucket FROM enriched WHERE experience_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_height', position_group || '_' || height_bucket FROM enriched WHERE height_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_weight', position_group || '_' || weight_bucket FROM enriched WHERE weight_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_ras', position_group || '_' || ras_bucket FROM enriched WHERE ras_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'draft_phase', draft_phase FROM enriched WHERE draft_phase IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'drafted_as_starter', CASE WHEN COALESCE(drafted_as_starter, 0) = 1 THEN 'starter' ELSE 'backup' END FROM enriched WHERE drafted_as_starter IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'draft_age_grade', draft_age_grade FROM enriched WHERE draft_age_grade IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'position_draft_label', position_draft_label FROM enriched WHERE position_draft_label IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'QB_mobile' FROM enriched WHERE position_group = 'QB' AND (prior_rush_yards >= 350 OR prior_carries >= 60)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'RB_rookie' FROM enriched WHERE position_group = 'RB' AND experience_bucket = 'experience_rookie'
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'RB_pass_catcher' FROM enriched WHERE position_group = 'RB' AND (prior_receptions >= 40 OR prior_targets >= 55)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'RB_workhorse' FROM enriched WHERE position_group = 'RB' AND prior_carries >= 200
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'WR_target_earner' FROM enriched WHERE position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'TE_target_earner' FROM enriched WHERE position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45)
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'TE_elite_athlete' FROM enriched WHERE position_group = 'TE' AND ras_bucket = 'ras_9_plus'
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'player_archetype', 'prior_injury_discount' FROM enriched WHERE prior_games BETWEEN 1 AND 8
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'qb_mobility_profile',
        CASE WHEN prior_rush_yards >= 350 OR prior_carries >= 60 THEN 'mobile_qb' ELSE 'pocket_or_low_rush_qb' END
        FROM enriched WHERE position_group = 'QB' AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'rb_receiving_profile',
        CASE WHEN prior_receptions >= 40 OR prior_targets >= 55 THEN 'pass_catching_rb' ELSE 'non_receiving_rb' END
        FROM enriched WHERE position_group = 'RB' AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'rb_workload_profile',
        CASE WHEN prior_carries >= 200 THEN 'workhorse_rb' ELSE 'committee_or_low_carry_rb' END
        FROM enriched WHERE position_group = 'RB' AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'receiving_volume_profile',
        CASE
            WHEN position_group = 'WR' AND (prior_targets >= 100 OR prior_target_share >= 0.22 OR prior_wopr >= 0.55) THEN 'WR_target_earner'
            WHEN position_group = 'WR' THEN 'WR_low_volume_profile'
            WHEN position_group = 'TE' AND (prior_targets >= 80 OR prior_target_share >= 0.18 OR prior_wopr >= 0.45) THEN 'TE_target_earner'
            ELSE 'TE_low_volume_profile'
        END
        FROM enriched WHERE position_group IN ('WR', 'TE') AND prior_games >= 4
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, capital_weight, position_group, 'availability_profile',
        CASE
            WHEN prior_games BETWEEN 1 AND 8 THEN 'prior_injury_discount'
            WHEN prior_games BETWEEN 9 AND 13 THEN 'partial_prior_season'
            ELSE 'full_prior_season'
        END
        FROM enriched WHERE prior_games > 0
    {award_feature_sql}
),
features AS (
    SELECT
        db_name, year, manager_key, manager, capital_bucket, capital_bucket AS control_bucket,
        capital_weight, control_position, feature_type, feature_value
    FROM base_features
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'capital_bucket', capital_bucket FROM enriched WHERE capital_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'capital_tier', capital_tier FROM enriched WHERE capital_tier IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'position_capital_bucket', position_group || '_' || capital_bucket FROM enriched WHERE position_group IS NOT NULL AND capital_bucket IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'position_capital_tier', position_group || '_' || capital_tier FROM enriched WHERE position_group IS NOT NULL AND capital_tier IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'position_draft_phase', position_group || '_' || draft_phase FROM enriched WHERE position_group IS NOT NULL AND draft_phase IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'format_position', draft_format || '_' || position_group FROM enriched WHERE draft_format IS NOT NULL AND position_group IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'format_capital_tier', draft_format || '_' || capital_tier FROM enriched WHERE draft_format IS NOT NULL AND capital_tier IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'position_slot', position_slot_label FROM enriched WHERE position_slot_label IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'position_slot_tier', position_slot_tier FROM enriched WHERE position_slot_tier IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'position_slot_capital_tier', position_slot_label || '_' || capital_tier FROM enriched WHERE position_slot_label IS NOT NULL AND capital_tier IS NOT NULL
    UNION ALL SELECT db_name, year, manager_key, manager, capital_bucket, 'ALL', capital_weight, 'ALL', 'position_slot_phase', position_slot_label || '_' || draft_phase FROM enriched WHERE position_slot_label IS NOT NULL AND draft_phase IS NOT NULL
),
manager_strata AS (
    SELECT db_name, year, manager_key, ANY_VALUE(manager) AS manager, feature_type, control_position, control_bucket,
           COUNT(*) AS manager_stratum_picks,
           SUM(capital_weight) AS manager_stratum_capital,
           SUM(capital_weight * capital_weight) AS manager_stratum_weight_sq
    FROM features
    GROUP BY db_name, year, manager_key, feature_type, control_position, control_bucket
),
manager_feature AS (
    SELECT db_name, year, manager_key, ANY_VALUE(manager) AS manager, feature_type, feature_value, control_position, control_bucket,
           COUNT(*) AS picks, SUM(capital_weight) AS observed_capital
    FROM features
    GROUP BY db_name, year, manager_key, feature_type, feature_value, control_position, control_bucket
),
league_feature_rates AS (
    SELECT db_name, year, feature_type, feature_value, control_position, control_bucket,
           COUNT(*) AS league_picks,
           SUM(capital_weight) AS feature_capital,
           SUM(COUNT(*)) OVER (PARTITION BY db_name, year, feature_type, control_position, control_bucket) AS total_picks,
           SUM(SUM(capital_weight)) OVER (PARTITION BY db_name, year, feature_type, control_position, control_bucket) AS total_capital
    FROM features
    GROUP BY db_name, year, feature_type, feature_value, control_position, control_bucket
),
scored_strata AS (
    SELECT ms.manager_key, ms.manager, ms.db_name, lfr.feature_type, lfr.feature_value, ms.year,
           COALESCE(mf.picks, 0) AS picks,
           COALESCE(mf.observed_capital, 0) AS observed_capital,
           ms.manager_stratum_picks,
           ms.manager_stratum_capital,
           GREATEST((ms.manager_stratum_capital * ms.manager_stratum_capital) / NULLIF(ms.manager_stratum_weight_sq, 0), 1.0) AS effective_n,
           lfr.league_picks / NULLIF(lfr.total_picks, 0) AS expected_pick_share,
           ms.manager_stratum_picks * (lfr.league_picks / NULLIF(lfr.total_picks, 0)) AS expected_picks,
           ms.manager_stratum_picks * (lfr.league_picks / NULLIF(lfr.total_picks, 0)) * (1 - (lfr.league_picks / NULLIF(lfr.total_picks, 0))) AS approx_pick_variance,
           lfr.feature_capital / NULLIF(lfr.total_capital, 0) AS expected_share,
           ms.manager_stratum_capital * (lfr.feature_capital / NULLIF(lfr.total_capital, 0)) AS expected_capital
    FROM manager_strata ms
    JOIN league_feature_rates lfr
      ON ms.db_name = lfr.db_name
     AND ms.year = lfr.year
     AND ms.feature_type = lfr.feature_type
     AND ms.control_position = lfr.control_position
     AND ms.control_bucket = lfr.control_bucket
    LEFT JOIN manager_feature mf
      ON mf.db_name = ms.db_name
     AND mf.year = ms.year
     AND mf.manager_key = ms.manager_key
     AND mf.feature_type = ms.feature_type
     AND mf.feature_value = lfr.feature_value
     AND mf.control_position = ms.control_position
     AND mf.control_bucket = ms.control_bucket
),
candidate_year AS (
    SELECT manager_key, ANY_VALUE(manager) AS manager, ANY_VALUE(db_name) AS db_name, feature_type, feature_value, year,
           SUM(picks) AS picks,
           SUM(expected_picks) AS expected_picks,
           SUM(approx_pick_variance) AS approx_pick_variance,
           SUM(observed_capital) AS observed_capital,
           SUM(expected_capital) AS expected_capital,
           SUM(manager_stratum_picks) AS manager_stratum_picks,
           SUM(manager_stratum_capital) AS manager_stratum_capital,
           SUM((observed_capital / NULLIF(manager_stratum_capital, 0)) * effective_n) AS observed_effective,
           SUM(expected_share * effective_n) AS expected_effective,
           SUM(effective_n * expected_share * (1 - expected_share)) AS approx_variance
    FROM scored_strata
    GROUP BY manager_key, feature_type, feature_value, year
),
rollup AS (
    SELECT manager_key, ANY_VALUE(manager) AS manager, ANY_VALUE(db_name) AS db_name, feature_type, feature_value,
           SUM(picks) AS picks,
           SUM(expected_picks) AS expected_picks,
           COUNT(DISTINCT year) AS years_seen,
           MIN(year) AS earliest_year,
           MAX(year) AS latest_year,
           SUM(GREATEST(observed_capital, expected_capital, expected_picks) * POWER(0.5, ((SELECT max_year FROM max_year) - year) / 4.0))
              / NULLIF(SUM(GREATEST(observed_capital, expected_capital, expected_picks)), 0) AS recency_weight,
           SUM(CASE WHEN observed_capital > expected_capital THEN 1 ELSE 0 END) AS positive_years,
           SUM(CASE WHEN observed_capital < expected_capital THEN 1 ELSE 0 END) AS negative_years,
           SUM(CASE WHEN picks > expected_picks THEN 1 ELSE 0 END) AS positive_count_years,
           SUM(CASE WHEN picks < expected_picks THEN 1 ELSE 0 END) AS negative_count_years,
           SUM(CASE WHEN picks = 0 AND expected_picks > 0 THEN 1 ELSE 0 END) AS zero_observed_years,
           SUM(observed_capital) AS observed_capital,
           SUM(expected_capital) AS expected_capital,
           SUM(manager_stratum_capital) AS manager_total_capital,
           SUM(manager_stratum_picks) AS manager_total_picks,
           SUM(observed_effective) AS observed_effective,
           SUM(expected_effective) AS expected_effective,
           SUM(approx_variance) AS approx_variance,
           SUM(approx_pick_variance) AS approx_pick_variance
    FROM candidate_year
    GROUP BY manager_key, feature_type, feature_value
),
ranked_base AS (
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           CAST(picks AS INTEGER) AS picks,
           ROUND(expected_picks, 3) AS expected_count,
           ROUND(picks - expected_picks, 3) AS excess_count,
           CAST(years_seen AS INTEGER) AS years_seen,
           CAST(earliest_year AS INTEGER) AS earliest_year,
           CAST(latest_year AS INTEGER) AS latest_year,
           ROUND(recency_weight, 4) AS recency_weight,
           CAST(positive_years AS INTEGER) AS positive_years,
           CAST(negative_years AS INTEGER) AS negative_years,
           CAST(positive_count_years AS INTEGER) AS positive_count_years,
           CAST(negative_count_years AS INTEGER) AS negative_count_years,
           CAST(zero_observed_years AS INTEGER) AS zero_observed_years,
           ROUND(CASE WHEN observed_capital >= expected_capital THEN positive_years / NULLIF(years_seen, 0) ELSE negative_years / NULLIF(years_seen, 0) END, 4) AS capital_repeatability,
           ROUND(CASE WHEN picks >= expected_picks THEN positive_count_years / NULLIF(years_seen, 0) ELSE negative_count_years / NULLIF(years_seen, 0) END, 4) AS count_repeatability,
           ROUND(zero_observed_years / NULLIF(years_seen, 0), 4) AS absence_repeatability,
           ROUND(observed_capital, 3) AS observed_capital,
           ROUND(expected_capital, 3) AS expected_capital,
           ROUND(observed_capital - expected_capital, 3) AS excess_capital,
           ROUND(observed_capital / NULLIF(manager_total_capital, 0), 4) AS observed_capital_share,
           ROUND(expected_capital / NULLIF(manager_total_capital, 0), 4) AS expected_capital_share,
           ROUND(observed_capital / NULLIF(expected_capital, 0), 4) AS capital_lift,
           ROUND(picks / NULLIF(manager_total_picks, 0), 4) AS observed_pick_share,
           ROUND(expected_picks / NULLIF(manager_total_picks, 0), 4) AS expected_pick_share,
           ROUND(picks / NULLIF(expected_picks, 0), 4) AS pick_lift,
           ROUND((observed_effective - expected_effective) / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0), 3) AS capital_z_score_raw,
           ROUND((picks - expected_picks) / NULLIF(SQRT(GREATEST(approx_pick_variance, 1.0)), 0), 3) AS count_z_score
    FROM rollup
    WHERE expected_capital >= {float(min_expected_capital)}
      AND expected_picks >= 0.75
      AND years_seen >= 2
),
ranked AS (
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           'capital_affinity' AS signal_metric,
           CASE WHEN observed_capital >= expected_capital THEN 'overweight' ELSE 'underweight' END AS signal_direction,
           picks, expected_count, excess_count, years_seen, earliest_year, latest_year, recency_weight,
           positive_years, negative_years, capital_repeatability AS repeatability,
           observed_capital, expected_capital, excess_capital,
           observed_capital_share AS observed_share,
           expected_capital_share AS expected_share,
           capital_lift AS lift,
           capital_z_score_raw AS signal_z_score,
           capital_z_score_raw AS capital_z_score,
           count_z_score
    FROM ranked_base
    WHERE picks > 0
      AND (picks >= {int(min_picks)} OR expected_capital >= {float(min_expected_capital) * 1.5})
      AND ABS(capital_z_score_raw) >= {float(min_abs_z)}
      AND capital_repeatability >= {float(min_repeatability)}
    UNION ALL
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           'pick_affinity' AS signal_metric,
           CASE WHEN picks >= expected_count THEN 'overweight' ELSE 'underweight' END AS signal_direction,
           picks, expected_count, excess_count, years_seen, earliest_year, latest_year, recency_weight,
           positive_count_years AS positive_years, negative_count_years AS negative_years, count_repeatability AS repeatability,
           observed_capital, expected_capital, excess_capital,
           observed_pick_share AS observed_share,
           expected_pick_share AS expected_share,
           pick_lift AS lift,
           count_z_score AS signal_z_score,
           count_z_score AS capital_z_score,
           count_z_score
    FROM ranked_base
    WHERE picks > 0
      AND (picks >= {int(min_picks)} OR expected_count >= {float(min_picks)})
      AND ABS(count_z_score) >= {float(min_abs_z)}
      AND count_repeatability >= {float(min_repeatability)}
    UNION ALL
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           'absence_affinity' AS signal_metric,
           'underweight' AS signal_direction,
           picks, expected_count, excess_count, years_seen, earliest_year, latest_year, recency_weight,
           0 AS positive_years, zero_observed_years AS negative_years, absence_repeatability AS repeatability,
           observed_capital, expected_capital, excess_capital,
           observed_pick_share AS observed_share,
           expected_pick_share AS expected_share,
           pick_lift AS lift,
           count_z_score AS signal_z_score,
           count_z_score AS capital_z_score,
           count_z_score
    FROM ranked_base
    WHERE picks = 0
      AND expected_count >= {float(min_picks)}
      AND years_seen >= 3
      AND ABS(count_z_score) >= {float(min_abs_z)}
      AND absence_repeatability >= {float(min_repeatability)}
)
SELECT *
FROM ranked
ORDER BY ABS(signal_z_score) DESC, ABS(excess_capital) DESC, ABS(excess_count) DESC
LIMIT {int(per_batch_limit)}
"""


def build_prior_award_base_sql(db_names: list[str]) -> str:
    """Return draft rows needed for local prior-award mining."""
    db_list = ", ".join(_q(name) for name in db_names)
    return f"""
WITH draft_base AS (
    SELECT
        d.db_name,
        d.year,
        d.db_name || ':' || COALESCE(NULLIF(d.franchise_id, ''), NULLIF(d.manager_guid, ''), NULLIF(d.manager, '')) AS manager_key,
        d.manager,
        CAST(d.NFL_player_id AS VARCHAR) AS NFL_player_id,
        UPPER(COALESCE(NULLIF(d.position, ''), 'UNK')) AS position_group,
        COALESCE(d.cost, 0) AS cost,
        d.pick,
        d.round,
        d.draft_type,
        CASE
            WHEN COALESCE(d.is_keeper, 0) = 1
              OR LOWER(COALESCE(d.draft_category, '')) = 'keeper'
            THEN 1 ELSE 0
        END AS is_keeper
    FROM public.draft d
    WHERE d.db_name IN ({db_list})
      AND d.year IS NOT NULL
      AND d.manager IS NOT NULL
      AND d.NFL_player_id IS NOT NULL
),
with_order AS (
    SELECT
        b.*,
        MAX(COALESCE(b.pick, b.round, 1)) OVER (PARTITION BY b.db_name, b.year) AS max_pick,
        ROW_NUMBER() OVER (
            PARTITION BY b.db_name, b.year
            ORDER BY COALESCE(b.pick, b.round, 9999), b.manager_key, b.NFL_player_id
        ) AS pick_order,
        COUNT(*) OVER (PARTITION BY b.db_name, b.year) AS picks_in_year,
        SUM(CASE WHEN COALESCE(b.cost, 0) > 0 THEN 1 ELSE 0 END) OVER (PARTITION BY b.db_name, b.year) AS paid_picks_in_year
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
        END AS capital_bucket
    FROM with_order b
),
enriched AS (
    SELECT
        b.db_name,
        CAST(b.year AS INTEGER) AS year,
        b.manager_key,
        b.manager,
        b.NFL_player_id,
        b.position_group,
        b.capital_bucket,
        b.capital_weight,
        CASE
            WHEN pb.rookie_year IS NULL THEN NULL
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 0 THEN 'experience_rookie'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) = 1 THEN 'experience_year_2'
            WHEN b.year - CAST(pb.rookie_year AS INTEGER) <= 3 THEN 'experience_year_3_4'
            ELSE 'experience_veteran'
        END AS experience_bucket
    FROM with_capital b
    LEFT JOIN ___ops.nfl_historical.player_bio pb ON b.NFL_player_id = CAST(pb.NFL_player_id AS VARCHAR)
)
SELECT *
FROM enriched
WHERE capital_bucket IS NOT NULL
  AND position_group IS NOT NULL
"""


def _load_prior_awards_frame(path: Path):
    import pandas as pd

    awards = pd.read_parquet(path).copy()
    required = [
        "prev_allpro_seasons",
        "prev_probowl_seasons",
        "prev_major_award_wins",
        "prev_award_vote_mentions",
        "prior_allpro_seasons",
        "prior_probowl_seasons",
        "prior_major_award_wins",
        "prior_award_vote_mentions",
    ]
    for col in required:
        if col not in awards.columns:
            awards[col] = 0
        awards[col] = pd.to_numeric(awards[col], errors="coerce").fillna(0)
    awards["NFL_player_id"] = awards["NFL_player_id"].astype(str)
    awards["draft_year"] = pd.to_numeric(awards["draft_year"], errors="coerce").astype("Int64")
    return awards[["NFL_player_id", "draft_year", *required]]


def mine_prior_award_signals(
    reader: FlyReader,
    db_names: list[str],
    awards_path: Path,
    *,
    min_abs_z: float,
    min_repeatability: float,
    min_picks: int,
    min_expected_capital: float,
    per_batch_limit: int,
) -> list[dict[str, Any]]:
    """Mine draft-safe prior award signals by joining local awards to Fly draft rows."""
    import duckdb
    import pandas as pd

    base_rows = reader.query(build_prior_award_base_sql(db_names), database="___leagues")
    if not base_rows:
        return []

    base = pd.DataFrame(base_rows)
    if base.empty:
        return []
    base["NFL_player_id"] = base["NFL_player_id"].astype(str)
    awards = _load_prior_awards_frame(awards_path)

    conn = duckdb.connect(":memory:")
    conn.register("base", base)
    conn.register("awards", awards)
    sql = f"""
WITH max_year AS (
    SELECT MAX(year) AS max_year FROM base WHERE year IS NOT NULL
),
enriched AS (
    SELECT
        b.*,
        COALESCE(a.prev_allpro_seasons, 0) AS prev_allpro_seasons,
        COALESCE(a.prev_probowl_seasons, 0) AS prev_probowl_seasons,
        COALESCE(a.prev_major_award_wins, 0) AS prev_major_award_wins,
        COALESCE(a.prev_award_vote_mentions, 0) AS prev_award_vote_mentions,
        COALESCE(a.prior_allpro_seasons, 0) AS prior_allpro_seasons,
        COALESCE(a.prior_probowl_seasons, 0) AS prior_probowl_seasons,
        COALESCE(a.prior_major_award_wins, 0) AS prior_major_award_wins,
        COALESCE(a.prior_award_vote_mentions, 0) AS prior_award_vote_mentions
    FROM base b
    LEFT JOIN awards a
      ON b.NFL_player_id = a.NFL_player_id
     AND b.year = CAST(a.draft_year AS INTEGER)
),
features AS (
    SELECT db_name, year, manager_key, manager, capital_bucket, capital_bucket AS control_bucket,
           capital_weight, position_group AS control_position, 'award_history' AS feature_type,
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
           END AS feature_value
    FROM enriched
    UNION ALL
    SELECT db_name, year, manager_key, manager, capital_bucket, capital_bucket AS control_bucket,
           capital_weight, position_group AS control_position, 'bio_profile' AS feature_type,
           CASE
               WHEN (prior_allpro_seasons + prior_probowl_seasons + prior_major_award_wins + prior_award_vote_mentions) >= 2
               THEN 'decorated_veteran'
               ELSE 'undecorated_veteran'
           END AS feature_value
    FROM enriched
    WHERE experience_bucket = 'experience_veteran'
),
manager_strata AS (
    SELECT db_name, year, manager_key, ANY_VALUE(manager) AS manager, feature_type, control_position, control_bucket,
           COUNT(*) AS manager_stratum_picks,
           SUM(capital_weight) AS manager_stratum_capital,
           SUM(capital_weight * capital_weight) AS manager_stratum_weight_sq
    FROM features
    GROUP BY db_name, year, manager_key, feature_type, control_position, control_bucket
),
manager_feature AS (
    SELECT db_name, year, manager_key, ANY_VALUE(manager) AS manager, feature_type, feature_value, control_position, control_bucket,
           COUNT(*) AS picks, SUM(capital_weight) AS observed_capital
    FROM features
    GROUP BY db_name, year, manager_key, feature_type, feature_value, control_position, control_bucket
),
league_feature_rates AS (
    SELECT db_name, year, feature_type, feature_value, control_position, control_bucket,
           COUNT(*) AS league_picks,
           SUM(capital_weight) AS feature_capital,
           SUM(COUNT(*)) OVER (PARTITION BY db_name, year, feature_type, control_position, control_bucket) AS total_picks,
           SUM(SUM(capital_weight)) OVER (PARTITION BY db_name, year, feature_type, control_position, control_bucket) AS total_capital
    FROM features
    GROUP BY db_name, year, feature_type, feature_value, control_position, control_bucket
),
scored_strata AS (
    SELECT ms.manager_key, ms.manager, ms.db_name, lfr.feature_type, lfr.feature_value, ms.year,
           COALESCE(mf.picks, 0) AS picks,
           COALESCE(mf.observed_capital, 0) AS observed_capital,
           ms.manager_stratum_picks,
           ms.manager_stratum_capital,
           GREATEST((ms.manager_stratum_capital * ms.manager_stratum_capital) / NULLIF(ms.manager_stratum_weight_sq, 0), 1.0) AS effective_n,
           lfr.league_picks / NULLIF(lfr.total_picks, 0) AS expected_pick_share,
           ms.manager_stratum_picks * (lfr.league_picks / NULLIF(lfr.total_picks, 0)) AS expected_picks,
           ms.manager_stratum_picks * (lfr.league_picks / NULLIF(lfr.total_picks, 0)) * (1 - (lfr.league_picks / NULLIF(lfr.total_picks, 0))) AS approx_pick_variance,
           lfr.feature_capital / NULLIF(lfr.total_capital, 0) AS expected_share,
           ms.manager_stratum_capital * (lfr.feature_capital / NULLIF(lfr.total_capital, 0)) AS expected_capital
    FROM manager_strata ms
    JOIN league_feature_rates lfr
      ON ms.db_name = lfr.db_name
     AND ms.year = lfr.year
     AND ms.feature_type = lfr.feature_type
     AND ms.control_position = lfr.control_position
     AND ms.control_bucket = lfr.control_bucket
    LEFT JOIN manager_feature mf
      ON mf.db_name = ms.db_name
     AND mf.year = ms.year
     AND mf.manager_key = ms.manager_key
     AND mf.feature_type = lfr.feature_type
     AND mf.feature_value = lfr.feature_value
     AND mf.control_position = lfr.control_position
     AND mf.control_bucket = lfr.control_bucket
),
candidate_year AS (
    SELECT manager_key, ANY_VALUE(manager) AS manager, ANY_VALUE(db_name) AS db_name, feature_type, feature_value, year,
           SUM(picks) AS picks,
           SUM(expected_picks) AS expected_picks,
           SUM(approx_pick_variance) AS approx_pick_variance,
           SUM(observed_capital) AS observed_capital,
           SUM(expected_capital) AS expected_capital,
           SUM(manager_stratum_picks) AS manager_stratum_picks,
           SUM(manager_stratum_capital) AS manager_stratum_capital,
           SUM((observed_capital / NULLIF(manager_stratum_capital, 0)) * effective_n) AS observed_effective,
           SUM(expected_share * effective_n) AS expected_effective,
           SUM(effective_n * expected_share * (1 - expected_share)) AS approx_variance
    FROM scored_strata
    GROUP BY manager_key, feature_type, feature_value, year
),
rollup AS (
    SELECT manager_key, ANY_VALUE(manager) AS manager, ANY_VALUE(db_name) AS db_name, feature_type, feature_value,
           SUM(picks) AS picks,
           SUM(expected_picks) AS expected_picks,
           COUNT(DISTINCT year) AS years_seen,
           MIN(year) AS earliest_year,
           MAX(year) AS latest_year,
           SUM(GREATEST(observed_capital, expected_capital, expected_picks) * POWER(0.5, ((SELECT max_year FROM max_year) - year) / 4.0))
              / NULLIF(SUM(GREATEST(observed_capital, expected_capital, expected_picks)), 0) AS recency_weight,
           SUM(CASE WHEN observed_capital > expected_capital THEN 1 ELSE 0 END) AS positive_years,
           SUM(CASE WHEN observed_capital < expected_capital THEN 1 ELSE 0 END) AS negative_years,
           SUM(CASE WHEN picks > expected_picks THEN 1 ELSE 0 END) AS positive_count_years,
           SUM(CASE WHEN picks < expected_picks THEN 1 ELSE 0 END) AS negative_count_years,
           SUM(CASE WHEN picks = 0 AND expected_picks > 0 THEN 1 ELSE 0 END) AS zero_observed_years,
           SUM(observed_capital) AS observed_capital,
           SUM(expected_capital) AS expected_capital,
           SUM(manager_stratum_capital) AS manager_total_capital,
           SUM(manager_stratum_picks) AS manager_total_picks,
           SUM(observed_effective) AS observed_effective,
           SUM(expected_effective) AS expected_effective,
           SUM(approx_variance) AS approx_variance,
           SUM(approx_pick_variance) AS approx_pick_variance
    FROM candidate_year
    GROUP BY manager_key, feature_type, feature_value
),
ranked_base AS (
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           CAST(picks AS INTEGER) AS picks,
           ROUND(expected_picks, 3) AS expected_count,
           ROUND(picks - expected_picks, 3) AS excess_count,
           CAST(years_seen AS INTEGER) AS years_seen,
           CAST(earliest_year AS INTEGER) AS earliest_year,
           CAST(latest_year AS INTEGER) AS latest_year,
           ROUND(recency_weight, 4) AS recency_weight,
           CAST(positive_years AS INTEGER) AS positive_years,
           CAST(negative_years AS INTEGER) AS negative_years,
           CAST(positive_count_years AS INTEGER) AS positive_count_years,
           CAST(negative_count_years AS INTEGER) AS negative_count_years,
           CAST(zero_observed_years AS INTEGER) AS zero_observed_years,
           ROUND(CASE WHEN observed_capital >= expected_capital THEN positive_years / NULLIF(years_seen, 0) ELSE negative_years / NULLIF(years_seen, 0) END, 4) AS capital_repeatability,
           ROUND(CASE WHEN picks >= expected_picks THEN positive_count_years / NULLIF(years_seen, 0) ELSE negative_count_years / NULLIF(years_seen, 0) END, 4) AS count_repeatability,
           ROUND(zero_observed_years / NULLIF(years_seen, 0), 4) AS absence_repeatability,
           ROUND(observed_capital, 3) AS observed_capital,
           ROUND(expected_capital, 3) AS expected_capital,
           ROUND(observed_capital - expected_capital, 3) AS excess_capital,
           ROUND(observed_capital / NULLIF(manager_total_capital, 0), 4) AS observed_capital_share,
           ROUND(expected_capital / NULLIF(manager_total_capital, 0), 4) AS expected_capital_share,
           ROUND(observed_capital / NULLIF(expected_capital, 0), 4) AS capital_lift,
           ROUND(picks / NULLIF(manager_total_picks, 0), 4) AS observed_pick_share,
           ROUND(expected_picks / NULLIF(manager_total_picks, 0), 4) AS expected_pick_share,
           ROUND(picks / NULLIF(expected_picks, 0), 4) AS pick_lift,
           ROUND((observed_effective - expected_effective) / NULLIF(SQRT(GREATEST(approx_variance, 1.0)), 0), 3) AS capital_z_score_raw,
           ROUND((picks - expected_picks) / NULLIF(SQRT(GREATEST(approx_pick_variance, 1.0)), 0), 3) AS count_z_score
    FROM rollup
    WHERE expected_capital >= {float(min_expected_capital)}
      AND expected_picks >= 0.75
      AND years_seen >= 2
),
ranked AS (
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           'capital_affinity' AS signal_metric,
           CASE WHEN observed_capital >= expected_capital THEN 'overweight' ELSE 'underweight' END AS signal_direction,
           picks, expected_count, excess_count, years_seen, earliest_year, latest_year, recency_weight,
           positive_years, negative_years, capital_repeatability AS repeatability,
           observed_capital, expected_capital, excess_capital,
           observed_capital_share AS observed_share,
           expected_capital_share AS expected_share,
           capital_lift AS lift,
           capital_z_score_raw AS signal_z_score,
           capital_z_score_raw AS capital_z_score,
           count_z_score
    FROM ranked_base
    WHERE picks > 0
      AND (picks >= {int(min_picks)} OR expected_capital >= {float(min_expected_capital) * 1.5})
      AND ABS(capital_z_score_raw) >= {float(min_abs_z)}
      AND capital_repeatability >= {float(min_repeatability)}
    UNION ALL
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           'pick_affinity' AS signal_metric,
           CASE WHEN picks >= expected_count THEN 'overweight' ELSE 'underweight' END AS signal_direction,
           picks, expected_count, excess_count, years_seen, earliest_year, latest_year, recency_weight,
           positive_count_years AS positive_years, negative_count_years AS negative_years, count_repeatability AS repeatability,
           observed_capital, expected_capital, excess_capital,
           observed_pick_share AS observed_share,
           expected_pick_share AS expected_share,
           pick_lift AS lift,
           count_z_score AS signal_z_score,
           count_z_score AS capital_z_score,
           count_z_score
    FROM ranked_base
    WHERE picks > 0
      AND (picks >= {int(min_picks)} OR expected_count >= {float(min_picks)})
      AND ABS(count_z_score) >= {float(min_abs_z)}
      AND count_repeatability >= {float(min_repeatability)}
    UNION ALL
    SELECT db_name, manager_key, manager, feature_type, feature_value,
           'absence_affinity' AS signal_metric,
           'underweight' AS signal_direction,
           picks, expected_count, excess_count, years_seen, earliest_year, latest_year, recency_weight,
           0 AS positive_years, zero_observed_years AS negative_years, absence_repeatability AS repeatability,
           observed_capital, expected_capital, excess_capital,
           observed_pick_share AS observed_share,
           expected_pick_share AS expected_share,
           pick_lift AS lift,
           count_z_score AS signal_z_score,
           count_z_score AS capital_z_score,
           count_z_score
    FROM ranked_base
    WHERE picks = 0
      AND expected_count >= {float(min_picks)}
      AND years_seen >= 3
      AND ABS(count_z_score) >= {float(min_abs_z)}
      AND absence_repeatability >= {float(min_repeatability)}
)
SELECT *
FROM ranked
ORDER BY ABS(signal_z_score) DESC, ABS(excess_capital) DESC, ABS(excess_count) DESC
LIMIT {int(per_batch_limit)}
"""
    try:
        rows_df = conn.execute(sql).fetchdf()
    finally:
        conn.close()
    if rows_df.empty:
        return []
    return json.loads(rows_df.to_json(orient="records"))


def signal_to_assignment(row: dict[str, Any]) -> dict[str, Any]:
    metric = str(row.get("signal_metric") or "capital_affinity")
    z_score = float(row.get("signal_z_score") or row.get("capital_z_score") or 0)
    abs_z = abs(z_score)
    years = int(row.get("years_seen") or 0)
    picks = int(row.get("picks") or 0)
    expected_count = float(row.get("expected_count") or 0)
    expected_capital = float(row.get("expected_capital") or 0)
    observed_capital = float(row.get("observed_capital") or 0)
    repeatability = max(0.0, min(1.0, float(row.get("repeatability") or 0)))
    recency_weight = max(0.0, min(1.0, float(row.get("recency_weight") or 1.0)))
    evidence_count = max(float(picks), expected_count if metric == "absence_affinity" else 0.0)
    metadata = classify_feature(str(row.get("feature_type") or "unknown"), row.get("feature_value"))
    prior_weight = float(metadata["prior_weight"])
    evidence_weight = max(0.0, observed_capital) + max(0.0, expected_capital)
    capital_reliability = evidence_weight / (evidence_weight + prior_weight) if evidence_weight > 0 else 0.0
    year_reliability = min(1.0, years / max(int(metadata["ready_years"]), 1))
    pick_reliability = min(1.0, evidence_count / max(int(metadata["ready_picks"]), 1))
    shrinkage_alpha = max(0.0, min(1.0, capital_reliability * year_reliability * pick_reliability * recency_weight))
    shrunk_z = z_score * shrinkage_alpha
    ready = (
        years >= int(metadata["ready_years"])
        and evidence_count >= int(metadata["ready_picks"])
        and repeatability >= float(metadata["ready_repeatability"])
        and abs(shrunk_z) >= float(metadata["ready_abs_z"])
    )
    watch = (
        years >= int(metadata["watch_years"])
        and evidence_count >= int(metadata["watch_picks"])
        and repeatability >= float(metadata["watch_repeatability"])
        and abs(shrunk_z) >= float(metadata["watch_abs_z"])
    )
    status = "catalog_ready" if ready else "catalog_watch"
    profile_id = "manager_" + _slug(
        f"{metric}_{row['feature_type']}_{row['feature_value']}_{row['signal_direction']}",
        max_len=140,
    )
    z_component = min(1.0, abs(shrunk_z) / max(float(metadata["ready_abs_z"]), 0.01))
    repeat_component = min(1.0, repeatability / max(float(metadata["ready_repeatability"]), 0.01))
    years_component = min(1.0, years / max(int(metadata["ready_years"]), 1))
    picks_component = min(1.0, evidence_count / max(int(metadata["ready_picks"]), 1))
    score = round(
        100.0
        * (
            0.38 * z_component
            + 0.22 * repeat_component
            + 0.13 * years_component
            + 0.13 * picks_component
            + 0.14 * recency_weight
        ),
        3,
    )
    value_delta = (
        float(row.get("excess_capital") or 0) if metric == "capital_affinity" else float(row.get("excess_count") or 0)
    )
    return {
        "db_name": row["db_name"],
        "scope_type": "manager",
        "scope_key": row["manager_key"],
        "scope_label": row["manager"],
        "profile_id": profile_id,
        "profile_kind": "manager",
        "profile_status": status,
        "assignment_rank": 999,
        "assignment_score": score,
        "feature_type": row["feature_type"],
        "feature_value": row["feature_value"],
        "signal_metric": metric,
        "signal_direction": row["signal_direction"],
        "promotion_level": "state_ready" if ready else "briefing_watch",
        "validation_score": score,
        "shrunk_z_score": round(shrunk_z, 3),
        "value_delta": value_delta,
        "picks": picks,
        "years_seen": years,
        "model_version": "live-preview-v3-state-registry",
    }


def rank_assignments(assignments: list[dict[str, Any]]) -> None:
    by_scope: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for assignment in assignments:
        by_scope.setdefault((assignment["db_name"], assignment["scope_key"]), []).append(assignment)
    for values in by_scope.values():
        values.sort(key=lambda row: (-float(row["assignment_score"]), -int(row["picks"]), row["profile_id"]))
        for rank, row in enumerate(values, start=1):
            row["assignment_rank"] = rank


def summarize(
    assignments: list[dict[str, Any]], archetypes: list[dict[str, Any]], archetype_assignments: list[dict[str, Any]]
) -> dict[str, Any]:
    feature_counts = Counter(
        (row["signal_metric"], row["feature_type"], row["feature_value"], row["signal_direction"])
        for row in assignments
    )
    scope_count = len({(row["db_name"], row["scope_key"]) for row in assignments})
    top_features = []
    for (signal_metric, feature_type, feature_value, direction), count in feature_counts.most_common(40):
        rows = [
            row
            for row in assignments
            if row["signal_metric"] == signal_metric
            and row["feature_type"] == feature_type
            and row["feature_value"] == feature_value
            and row["signal_direction"] == direction
        ]
        top_features.append(
            {
                "signal_metric": signal_metric,
                "feature_type": feature_type,
                "feature_value": feature_value,
                "direction": direction,
                "managers": count,
                "median_score": round(statistics.median(float(row["validation_score"]) for row in rows), 2),
            }
        )
    return {
        "signals": len(assignments),
        "ready_signals": sum(1 for row in assignments if row["profile_status"] == "catalog_ready"),
        "manager_scopes_with_signals": scope_count,
        "archetypes": len(archetypes),
        "archetype_assignments": len(archetype_assignments),
        "top_features": top_features,
        "top_archetypes": [
            {
                "label": row["archetype_label"],
                "summary": row["archetype_summary"],
                "scope_count": row["scope_count"],
                "league_count": row["league_count"],
                "profile_count": row["profile_count"],
                "median_assignment_score": row["median_assignment_score"],
                "status": row["archetype_status"],
                "evidence": json.loads(row["evidence_json"]),
            }
            for row in archetypes[:40]
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Learn draft profile archetypes from live Fly data.")
    parser.add_argument("--league-limit", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--per-batch-limit", type=int, default=500)
    parser.add_argument("--min-abs-z", type=float, default=2.75)
    parser.add_argument("--min-repeatability", type=float, default=0.60)
    parser.add_argument("--min-picks", type=int, default=5)
    parser.add_argument("--min-expected-capital", type=float, default=8.0)
    parser.add_argument("--min-archetype-scopes", type=int, default=8)
    parser.add_argument(
        "--prior-awards-table",
        default=None,
        help="Optional draft-safe prior-awards table, e.g. ___ops.nfl_historical.player_prior_awards_by_year",
    )
    parser.add_argument(
        "--prior-awards-parquet",
        default=str(DEFAULT_PRIOR_AWARDS_PARQUET) if DEFAULT_PRIOR_AWARDS_PARQUET.exists() else None,
        help="Optional local draft-safe prior-awards parquet used when no Fly awards table exists.",
    )
    parser.add_argument("--out-dir", default="artifacts/draft_archetype_learning")
    args = parser.parse_args()

    _load_env()
    reader = FlyReader()
    leagues = fetch_leagues(reader, limit=args.league_limit)
    db_names = [row["db_name"] for row in leagues]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    run_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    run_dir = out_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    prior_awards_parquet = Path(args.prior_awards_parquet).expanduser() if args.prior_awards_parquet else None
    if prior_awards_parquet and not prior_awards_parquet.exists():
        print(json.dumps({"warning": "prior awards parquet not found", "path": str(prior_awards_parquet)}), flush=True)
        prior_awards_parquet = None

    all_rows: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    started = time.time()
    batches = list(_chunks(db_names, args.batch_size))
    batch_log_path = run_dir / "batches.jsonl"
    for index, batch in enumerate(batches, start=1):
        batch_started = time.time()
        sql = build_manager_signal_sql_v2(
            batch,
            min_abs_z=args.min_abs_z,
            min_repeatability=args.min_repeatability,
            min_picks=args.min_picks,
            min_expected_capital=args.min_expected_capital,
            per_batch_limit=args.per_batch_limit,
            prior_awards_table=args.prior_awards_table,
        )
        try:
            rows = reader.query(sql, database="___leagues")
            award_rows: list[dict[str, Any]] = []
            if prior_awards_parquet and not args.prior_awards_table:
                award_rows = mine_prior_award_signals(
                    reader,
                    batch,
                    prior_awards_parquet,
                    min_abs_z=args.min_abs_z,
                    min_repeatability=args.min_repeatability,
                    min_picks=args.min_picks,
                    min_expected_capital=args.min_expected_capital,
                    per_batch_limit=args.per_batch_limit,
                )
            all_rows.extend(rows)
            all_rows.extend(award_rows)
            with batch_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({"batch": index, "leagues": batch, "rows": rows, "award_rows": award_rows}) + "\n")
            print(
                json.dumps(
                    {
                        "batch": index,
                        "batches": len(batches),
                        "leagues": len(batch),
                        "rows": len(rows),
                        "award_rows": len(award_rows),
                        "elapsed_sec": round(time.time() - batch_started, 1),
                        "total_rows": len(all_rows),
                    }
                ),
                flush=True,
            )
        except Exception as exc:
            failures.append({"batch": index, "leagues": batch, "error": str(exc)[:1000]})
            with batch_log_path.open("a", encoding="utf-8") as fh:
                fh.write(json.dumps({"batch": index, "leagues": batch, "error": str(exc)[:1000]}) + "\n")
            print(json.dumps({"batch": index, "error": str(exc)[:240]}), flush=True)

    assignments = [signal_to_assignment(row) for row in all_rows]
    rank_assignments(assignments)
    learning_assignments = [row for row in assignments if row.get("promotion_level") != "explore_only"]
    archetypes = learn_profile_archetypes(
        learning_assignments,
        include_watch=True,
        min_manager_scopes=args.min_archetype_scopes,
        min_league_scopes=4,
        manager_archetype_limit=200,
        league_archetype_limit=50,
        max_profiles_per_scope=5,
    )
    archetype_assignments = assign_profile_archetypes(learning_assignments, archetypes, max_archetypes_per_scope=3)
    summary = summarize(learning_assignments, archetypes, archetype_assignments)
    summary.update(
        {
            "run_id": run_id,
            "elapsed_sec": round(time.time() - started, 1),
            "league_count": len(db_names),
            "failed_batches": len(failures),
            "params": vars(args),
        }
    )

    (run_dir / "manager_signals.json").write_text(json.dumps(all_rows, indent=2), encoding="utf-8")
    (run_dir / "profile_assignments.json").write_text(json.dumps(assignments, indent=2), encoding="utf-8")
    (run_dir / "archetypes.json").write_text(json.dumps(archetypes, indent=2), encoding="utf-8")
    (run_dir / "archetype_assignments.json").write_text(json.dumps(archetype_assignments, indent=2), encoding="utf-8")
    (run_dir / "failures.json").write_text(json.dumps(failures, indent=2), encoding="utf-8")
    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    print(json.dumps({"run_dir": str(run_dir), **summary}, indent=2), flush=True)
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
