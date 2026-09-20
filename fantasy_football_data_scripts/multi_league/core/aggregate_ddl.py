"""Canonical DDL for persistent aggregate rollup tables."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from multi_league.core.canonical_matchup import (
    COLUMN_TYPES as MATCHUP_SOURCE_COLUMN_TYPES,
    SCHEDULE_LUCK_SIM_COLUMNS,
    SOS_LUCK_SIM_COLUMNS,
    PLAYOFF_SIM_COLUMNS,
)


@dataclass(frozen=True)
class AggregateSourceSpec:
    required: tuple[str, ...] = ()
    optional: tuple[str, ...] = ()


@dataclass(frozen=True)
class AggregateTableSpec:
    column_types: dict[str, str]
    primary_key: tuple[str, ...] = ()
    source_tables: dict[str, AggregateSourceSpec] = field(default_factory=dict)
    description: str = ""


def _prepend_db_name(column_types: dict[str, str]) -> dict[str, str]:
    return {"db_name": "VARCHAR", **column_types}


def _prepend_primary_key(primary_key: tuple[str, ...]) -> tuple[str, ...]:
    return ("db_name",) + primary_key if primary_key else primary_key


MATCHUP_SEASON_SNAPSHOT_COLUMNS: tuple[str, ...] = (
    *SCHEDULE_LUCK_SIM_COLUMNS,
    *SOS_LUCK_SIM_COLUMNS,
    *PLAYOFF_SIM_COLUMNS,
    "wins_to_date",
    "losses_to_date",
    "ties_to_date",
    "points_scored_to_date",
    "playoff_seed_to_date",
)

PLAYER_FANTASY_SEASON_COLUMN_TYPES = {
    "NFL_player_id": "VARCHAR",
    "year": "INTEGER",
    "player": "VARCHAR",
    "position": "VARCHAR",
    "nfl_team": "VARCHAR",
    "fantasy_points": "DOUBLE",
    "player_lamar": "DOUBLE",
    "manager_lamar": "DOUBLE",
    "clutch_equity": "DOUBLE",
    "games_started": "INTEGER",
    "games_rostered": "INTEGER",
    "wins": "INTEGER",
    "losses": "INTEGER",
    "managers": "VARCHAR",
    "franchise_id": "VARCHAR",
    "team_points": "DOUBLE",
    "opponent_points": "DOUBLE",
    "playoff_games": "INTEGER",
    "playoff_wins": "INTEGER",
    "playoff_losses": "INTEGER",
    "championships": "INTEGER",
    "optimal_player_count": "INTEGER",
    "league_wide_optimal_count": "INTEGER",
    "fantasy_position": "VARCHAR",
    "last_updated": "TIMESTAMP",
}

PLAYER_FANTASY_CAREER_COLUMN_TYPES = {
    "NFL_player_id": "VARCHAR",
    "first_year": "INTEGER",
    "last_year": "INTEGER",
    "years_active": "INTEGER",
    "player": "VARCHAR",
    "position": "VARCHAR",
    "nfl_team": "VARCHAR",
    "fantasy_points": "DOUBLE",
    "player_lamar": "DOUBLE",
    "manager_lamar": "DOUBLE",
    "clutch_equity": "DOUBLE",
    "games_started": "INTEGER",
    "games_rostered": "INTEGER",
    "wins": "INTEGER",
    "losses": "INTEGER",
    "managers": "VARCHAR",
    "franchise_id": "VARCHAR",
    "team_points": "DOUBLE",
    "opponent_points": "DOUBLE",
    "playoff_games": "INTEGER",
    "playoff_wins": "INTEGER",
    "playoff_losses": "INTEGER",
    "championships": "INTEGER",
    "optimal_player_count": "INTEGER",
    "league_wide_optimal_count": "INTEGER",
    "fantasy_position": "VARCHAR",
    "last_updated": "TIMESTAMP",
}

DRAFT_MANAGER_SEASON_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "year": "INTEGER",
    "franchise_id": "VARCHAR",
    "draft_category": "VARCHAR",
    "picks": "INTEGER",
    "keeper_picks": "INTEGER",
    "total_cost": "DOUBLE",
    "total_manager_lamar": "DOUBLE",
    "avg_manager_lamar": "DOUBLE",
    "total_player_lamar": "DOUBLE",
    "total_fantasy_points": "DOUBLE",
    "avg_season_ppg": "DOUBLE",
    "hits": "INTEGER",
    "hit_rate": "DOUBLE",
    "busts": "INTEGER",
    "breakouts": "INTEGER",
    "avg_pick_quality_zscore": "DOUBLE",
    "starters_drafted": "INTEGER",
    "avg_games_played": "DOUBLE",
    "avg_lamar_per_dollar": "DOUBLE",
    "manager_draft_grade": "VARCHAR",
    "manager_draft_score": "DOUBLE",
    "manager_draft_percentile": "DOUBLE",
    "best_pick_player": "VARCHAR",
    "best_pick_lamar": "DOUBLE",
    "worst_pick_player": "VARCHAR",
    "worst_pick_lamar": "DOUBLE",
    "last_updated": "TIMESTAMP",
}

DRAFT_MANAGER_CAREER_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "franchise_id": "VARCHAR",
    "draft_category": "VARCHAR",
    "years_active": "INTEGER",
    "total_picks": "INTEGER",
    "total_keeper_picks": "INTEGER",
    "total_cost": "DOUBLE",
    "total_manager_lamar": "DOUBLE",
    "avg_manager_lamar": "DOUBLE",
    "total_fantasy_points": "DOUBLE",
    "avg_season_ppg": "DOUBLE",
    "career_hit_rate": "DOUBLE",
    "total_busts": "INTEGER",
    "total_breakouts": "INTEGER",
    "avg_pick_quality_zscore": "DOUBLE",
    "avg_games_played": "DOUBLE",
    "avg_lamar_per_dollar": "DOUBLE",
    "career_gpa": "DOUBLE",
    "career_grade": "VARCHAR",
    "best_pick_player": "VARCHAR",
    "best_pick_lamar": "DOUBLE",
    "worst_pick_player": "VARCHAR",
    "worst_pick_lamar": "DOUBLE",
    "last_updated": "TIMESTAMP",
}

DRAFT_PLAYER_CAREER_COLUMN_TYPES = {
    "player": "VARCHAR",
    "position": "VARCHAR",
    "draft_category": "VARCHAR",
    "times_drafted": "INTEGER",
    "times_kept": "INTEGER",
    "total_cost": "DOUBLE",
    "keeper_cost": "DOUBLE",
    "total_manager_lamar": "DOUBLE",
    "avg_manager_lamar": "DOUBLE",
    "lamar_per_dollar": "DOUBLE",
    "total_fantasy_points": "DOUBLE",
    "avg_season_ppg": "DOUBLE",
    "avg_pick_quality_zscore": "DOUBLE",
    "best_manager": "VARCHAR",
    "managers": "VARCHAR",
    "franchise_ids": "VARCHAR",
    "years": "VARCHAR",
    "last_updated": "TIMESTAMP",
}

TRANSACTION_MANAGER_SEASON_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "year": "INTEGER",
    "franchise_id": "VARCHAR",
    "adds": "INTEGER",
    "drops": "INTEGER",
    "total_moves": "INTEGER",
    "total_faab_bid": "DOUBLE",
    "lamar_added": "DOUBLE",
    "lamar_dropped": "DOUBLE",
    "net_lamar": "DOUBLE",
    "net_points_ros": "DOUBLE",
    "total_transaction_score": "DOUBLE",
    "avg_transaction_score": "DOUBLE",
    "avg_faab_per_add": "DOUBLE",
    "avg_timing_mult": "DOUBLE",
    "avg_ppg_improvement": "DOUBLE",
    "avg_points_per_faab": "DOUBLE",
    "transaction_grade": "VARCHAR",
    "transaction_gpa": "DOUBLE",
    "trades": "INTEGER",
    "trade_net_lamar": "DOUBLE",
    "trade_lamar_per_trade": "DOUBLE",
    "trade_wins": "INTEGER",
    "trade_win_rate": "DOUBLE",
    "trade_net_points": "DOUBLE",
    "best_pickup_player": "VARCHAR",
    "best_pickup_lamar": "DOUBLE",
    "worst_drop_player": "VARCHAR",
    "worst_drop_regret": "DOUBLE",
    "last_updated": "TIMESTAMP",
}

TRANSACTION_MANAGER_CAREER_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "franchise_id": "VARCHAR",
    "seasons": "INTEGER",
    "total_adds": "INTEGER",
    "total_drops": "INTEGER",
    "total_moves": "INTEGER",
    "total_faab_bid": "DOUBLE",
    "net_lamar": "DOUBLE",
    "net_points_ros": "DOUBLE",
    "efficiency": "DOUBLE",
    "lamar_per_season": "DOUBLE",
    "total_transaction_score": "DOUBLE",
    "avg_transaction_score": "DOUBLE",
    "avg_timing_mult": "DOUBLE",
    "transaction_grade": "VARCHAR",
    "transaction_gpa": "DOUBLE",
    "total_trades": "INTEGER",
    "trade_net_lamar": "DOUBLE",
    "trade_avg_net": "DOUBLE",
    "trade_win_rate": "DOUBLE",
    "trade_net_points": "DOUBLE",
    "last_updated": "TIMESTAMP",
}

TRANSACTION_PLAYER_CAREER_COLUMN_TYPES = {
    "player": "VARCHAR",
    "position": "VARCHAR",
    "times_added": "INTEGER",
    "times_dropped": "INTEGER",
    "times_traded": "INTEGER",
    "total_faab_spent": "DOUBLE",
    "avg_faab": "DOUBLE",
    "total_lamar_when_added": "DOUBLE",
    "total_lamar_when_dropped": "DOUBLE",
    "avg_drop_regret": "DOUBLE",
    "managers": "VARCHAR",
    "franchise_ids": "VARCHAR",
    "years_active": "INTEGER",
    "last_updated": "TIMESTAMP",
}

TRANSACTION_REPORT_CARD_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "year": "INTEGER",
    "franchise_id": "VARCHAR",
    "adds": "INTEGER",
    "drops": "INTEGER",
    "trades": "INTEGER",
    "total_faab_bid": "DOUBLE",
    "total_lamar": "DOUBLE",
    "win_rate": "DOUBLE",
    "transaction_grade": "VARCHAR",
    "transaction_gpa": "DOUBLE",
    "grade_counts": "VARCHAR",
    "top_pickups": "VARCHAR",
    "worst_drops": "VARCHAR",
    "best_trade": "VARCHAR",
    "worst_trade": "VARCHAR",
    "last_updated": "TIMESTAMP",
}

MATCHUP_SEASON_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "year": "INTEGER",
    "franchise_id": "VARCHAR",
    "games": "BIGINT",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "win_pct": "DOUBLE",
    "total_team_points": "DOUBLE",
    "total_opponent_points": "DOUBLE",
    "avg_team_points": "DOUBLE",
    "avg_opponent_points": "DOUBLE",
    "avg_margin": "DOUBLE",
    "max_team_points": "DOUBLE",
    "min_team_points": "DOUBLE",
    "std_dev_team_points": "DOUBLE",
    "avg_gpa": "DOUBLE",
    "above_league_median": "BIGINT",
    "below_league_median": "BIGINT",
    "close_games": "BIGINT",
    "close_wins": "BIGINT",
    "close_losses": "BIGINT",
    "close_win_pct": "DOUBLE",
    "blowout_wins": "BIGINT",
    "blowout_losses": "BIGINT",
    "max_win_streak": "BIGINT",
    "max_loss_streak": "BIGINT",
    "optimal_games": "BIGINT",
    "optimal_actual_pts": "DOUBLE",
    "optimal_ceiling_pts": "DOUBLE",
    "optimal_bench_pts": "DOUBLE",
    "optimal_efficiency": "DOUBLE",
    "optimal_wins": "BIGINT",
    "optimal_losses": "BIGINT",
    "optimal_missed_wins": "BIGINT",
    "optimal_lucky_wins": "BIGINT",
    "optimal_wins_actual": "BIGINT",
    "optimal_losses_actual": "BIGINT",
    "optimal_outcome_changes": "BIGINT",
    "optimal_margin": "DOUBLE",
    "proj_games": "BIGINT",
    "proj_wins": "BIGINT",
    "proj_losses": "BIGINT",
    "proj_total_team_points": "DOUBLE",
    "proj_total_opponent_points": "DOUBLE",
    "proj_total_proj": "DOUBLE",
    "proj_opp_proj": "DOUBLE",
    "proj_above_proj": "BIGINT",
    "proj_below_proj": "BIGINT",
    "proj_beat_spread": "BIGINT",
    "proj_margin_total": "DOUBLE",
    "proj_spread_avg": "DOUBLE",
    "proj_favored_pct": "DOUBLE",
    "proj_avg_win_pct": "DOUBLE",
    "proj_expected_wins": "BIGINT",
    "proj_upset_wins": "BIGINT",
    "proj_upset_losses": "BIGINT",
    "proj_total_upsets": "BIGINT",
    "proj_total_error": "DOUBLE",
    "proj_avg_error": "DOUBLE",
    "made_playoffs": "BIGINT",
    "is_champion": "BIGINT",
    "is_sacko": "BIGINT",
    "playoff_result": "VARCHAR",
    **{column: MATCHUP_SOURCE_COLUMN_TYPES[column] for column in MATCHUP_SEASON_SNAPSHOT_COLUMNS},
    "mean_avg_seed": "DOUBLE",
    "mean_p_playoffs": "DOUBLE",
    "mean_p_bye": "DOUBLE",
    "mean_p_semis": "DOUBLE",
    "mean_p_final": "DOUBLE",
    "mean_p_champ": "DOUBLE",
    "mean_exp_final_wins": "DOUBLE",
    "mean_exp_final_pf": "DOUBLE",
}

MATCHUP_CAREER_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "seasons": "BIGINT",
    "games": "BIGINT",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "total_team_points": "DOUBLE",
    "total_opponent_points": "DOUBLE",
    "avg_team_points": "DOUBLE",
    "avg_opponent_points": "DOUBLE",
    "avg_margin": "DOUBLE",
    "win_pct": "DOUBLE",
    "close_win_pct": "DOUBLE",
    "franchise_id": "VARCHAR",
    "above_league_median": "BIGINT",
    "below_league_median": "BIGINT",
    "close_games": "BIGINT",
    "close_wins": "BIGINT",
    "close_losses": "BIGINT",
    "blowout_wins": "BIGINT",
    "blowout_losses": "BIGINT",
    "optimal_games": "BIGINT",
    "optimal_actual_pts": "DOUBLE",
    "optimal_ceiling_pts": "DOUBLE",
    "optimal_bench_pts": "DOUBLE",
    "optimal_efficiency": "DOUBLE",
    "optimal_wins": "BIGINT",
    "optimal_losses": "BIGINT",
    "optimal_missed_wins": "BIGINT",
    "optimal_lucky_wins": "BIGINT",
    "optimal_wins_actual": "BIGINT",
    "optimal_losses_actual": "BIGINT",
    "optimal_outcome_changes": "BIGINT",
    "optimal_margin": "DOUBLE",
    "proj_games": "BIGINT",
    "proj_wins": "BIGINT",
    "proj_losses": "BIGINT",
    "proj_total_team_points": "DOUBLE",
    "proj_total_opponent_points": "DOUBLE",
    "proj_total_proj": "DOUBLE",
    "proj_opp_proj": "DOUBLE",
    "proj_above_proj": "BIGINT",
    "proj_below_proj": "BIGINT",
    "proj_beat_spread": "BIGINT",
    "proj_margin_total": "DOUBLE",
    "proj_upset_wins": "BIGINT",
    "proj_upset_losses": "BIGINT",
    "proj_total_upsets": "BIGINT",
    "proj_total_error": "DOUBLE",
    "proj_expected_wins": "BIGINT",
    "max_win_streak": "BIGINT",
    "max_loss_streak": "BIGINT",
    "max_team_points": "DOUBLE",
    "min_team_points": "DOUBLE",
    "avg_power_rating": "DOUBLE",
    "avg_avg_seed": "DOUBLE",
    "avg_p_playoffs": "DOUBLE",
    "avg_p_bye": "DOUBLE",
    "avg_p_semis": "DOUBLE",
    "avg_p_final": "DOUBLE",
    "avg_p_champ": "DOUBLE",
    "avg_exp_wins": "DOUBLE",
    "playoff_seasons": "BIGINT",
    "champion_seasons": "BIGINT",
    "sacko_seasons": "BIGINT",
    "avg_gpa": "DOUBLE",
}

MATCHUP_H2H_SEASON_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "opponent": "VARCHAR",
    "franchise_id": "VARCHAR",
    "opponent_franchise_id": "VARCHAR",
    "year": "INTEGER",
    "games": "BIGINT",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "total_team_points": "DOUBLE",
    "total_margin": "DOUBLE",
    "max_team_points": "DOUBLE",
    "min_team_points": "DOUBLE",
}

MATCHUP_H2H_CAREER_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "opponent": "VARCHAR",
    "franchise_id": "VARCHAR",
    "opponent_franchise_id": "VARCHAR",
    "games": "BIGINT",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "total_team_points": "DOUBLE",
    "total_margin": "DOUBLE",
    "max_team_points": "DOUBLE",
    "min_team_points": "DOUBLE",
    "recent_win": "BIGINT",
    "recent_team_points": "DOUBLE",
    "recent_year": "INTEGER",
    "recent_week": "INTEGER",
    "results": "INTEGER[]",
}

ALL_PLAY_COLUMN_TYPES = {
    "year": "INTEGER",
    "week": "INTEGER",
    "franchise_id": "VARCHAR",
    "opponent_franchise_id": "VARCHAR",
    "result": "VARCHAR",
    "points": "DOUBLE",
    "opponent_points": "DOUBLE",
}

H2H_SEASON_COLUMN_TYPES = {
    "franchise_id": "VARCHAR",
    "opponent_franchise_id": "VARCHAR",
    "year": "INTEGER",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "games": "BIGINT",
}

SCHEDULE_SWAP_COLUMN_TYPES = {
    "year": "INTEGER",
    "week": "INTEGER",
    "franchise_id": "VARCHAR",
    "schedule_of_franchise_id": "VARCHAR",
    "result": "VARCHAR",
    "my_points": "DOUBLE",
    "their_opponent_points": "DOUBLE",
}

SCHEDULE_SWAP_SEASON_COLUMN_TYPES = {
    "franchise_id": "VARCHAR",
    "schedule_of_franchise_id": "VARCHAR",
    "year": "INTEGER",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "games": "BIGINT",
}

STANDINGS_BY_YEAR_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "year": "INTEGER",
    "team_name": "VARCHAR",
    "wins": "INTEGER",
    "losses": "INTEGER",
    "ties": "INTEGER",
    "points_for": "DOUBLE",
    "points_against": "DOUBLE",
    "win_pct": "DOUBLE",
    "final_playoff_seed": "INTEGER",
    "franchise_id": "VARCHAR",
    "above_median_wins": "INTEGER",
    "above_median_losses": "INTEGER",
    "total_wins": "INTEGER",
    "total_losses": "INTEGER",
    "final_result": "VARCHAR",
    "seed": "INTEGER",
}


def _prefixed_column_types(prefix: str, column_types: dict[str, str]) -> dict[str, str]:
    return {f"{prefix}{column}": dtype for column, dtype in column_types.items()}


HOMEPAGE_LEAGUE_RECORD_COLUMN_TYPES = {
    "highest_score_manager": "VARCHAR",
    "highest_score_points": "DOUBLE",
    "highest_score_year": "INTEGER",
    "highest_score_week": "INTEGER",
    "closest_game_manager": "VARCHAR",
    "closest_game_opponent": "VARCHAR",
    "closest_game_margin": "DOUBLE",
    "closest_game_year": "INTEGER",
    "closest_game_week": "INTEGER",
    "biggest_blowout_manager": "VARCHAR",
    "biggest_blowout_opponent": "VARCHAR",
    "biggest_blowout_margin": "DOUBLE",
    "biggest_blowout_year": "INTEGER",
    "biggest_blowout_week": "INTEGER",
    "most_championships_manager": "VARCHAR",
    "most_championships_count": "INTEGER",
}

HOMEPAGE_PLAYER_LEADER_COLUMN_TYPES = {
    "best_game_lamar_player": "VARCHAR",
    "best_game_lamar_manager": "VARCHAR",
    "best_game_lamar_year": "INTEGER",
    "best_game_lamar_week": "INTEGER",
    "best_game_lamar_value": "DOUBLE",
    "best_game_lamar_headshot": "VARCHAR",
    "best_game_clutch_player": "VARCHAR",
    "best_game_clutch_manager": "VARCHAR",
    "best_game_clutch_year": "INTEGER",
    "best_game_clutch_week": "INTEGER",
    "best_game_clutch_value": "DOUBLE",
    "best_game_clutch_headshot": "VARCHAR",
    "best_season_lamar_player": "VARCHAR",
    "best_season_lamar_manager": "VARCHAR",
    "best_season_lamar_year": "INTEGER",
    "best_season_lamar_value": "DOUBLE",
    "best_season_lamar_headshot": "VARCHAR",
    "best_season_clutch_player": "VARCHAR",
    "best_season_clutch_manager": "VARCHAR",
    "best_season_clutch_year": "INTEGER",
    "best_season_clutch_value": "DOUBLE",
    "best_season_clutch_headshot": "VARCHAR",
    "best_career_lamar_player": "VARCHAR",
    "best_career_lamar_manager": "VARCHAR",
    "best_career_lamar_value": "DOUBLE",
    "best_career_lamar_headshot": "VARCHAR",
    "best_career_clutch_player": "VARCHAR",
    "best_career_clutch_manager": "VARCHAR",
    "best_career_clutch_value": "DOUBLE",
    "best_career_clutch_headshot": "VARCHAR",
}

HOMEPAGE_DRAFT_HIGHLIGHT_COLUMN_TYPES = {
    "best_pick_player": "VARCHAR",
    "best_pick_manager": "VARCHAR",
    "best_pick_position": "VARCHAR",
    "best_pick_round": "INTEGER",
    "best_pick_year": "INTEGER",
    "best_pick_lamar": "DOUBLE",
    "best_pick_headshot": "VARCHAR",
    "best_pick_cost": "DOUBLE",
    "best_pick_pick": "INTEGER",
    "worst_pick_player": "VARCHAR",
    "worst_pick_manager": "VARCHAR",
    "worst_pick_position": "VARCHAR",
    "worst_pick_round": "INTEGER",
    "worst_pick_year": "INTEGER",
    "worst_pick_lamar": "DOUBLE",
    "worst_pick_headshot": "VARCHAR",
    "worst_pick_cost": "DOUBLE",
    "worst_pick_pick": "INTEGER",
}

HOMEPAGE_TRANSACTION_HIGHLIGHT_COLUMN_TYPES = {
    "best_pickup_player": "VARCHAR",
    "best_pickup_manager": "VARCHAR",
    "best_pickup_year": "INTEGER",
    "best_pickup_week": "INTEGER",
    "best_pickup_lamar": "DOUBLE",
    "best_pickup_headshot": "VARCHAR",
    "worst_drop_player": "VARCHAR",
    "worst_drop_manager": "VARCHAR",
    "worst_drop_year": "INTEGER",
    "worst_drop_week": "INTEGER",
    "worst_drop_lamar": "DOUBLE",
    "worst_drop_headshot": "VARCHAR",
}

HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES = {
    "winner": "VARCHAR",
    "winner_players": "VARCHAR",
    "winner_headshots": "VARCHAR",
    "winner_lamar": "DOUBLE",
    "loser": "VARCHAR",
    "loser_players": "VARCHAR",
    "loser_headshots": "VARCHAR",
    "loser_lamar": "DOUBLE",
    "net_lamar": "DOUBLE",
    "year": "INTEGER",
    "week": "INTEGER",
}

HOMEPAGE_LEAGUE_SUMMARY_COLUMN_TYPES = {
    "last_updated": "TIMESTAMP",
    "data_year": "INTEGER",
    "data_week": "INTEGER",
    **HOMEPAGE_LEAGUE_RECORD_COLUMN_TYPES,
    **HOMEPAGE_PLAYER_LEADER_COLUMN_TYPES,
    **_prefixed_column_types("alltime_", HOMEPAGE_DRAFT_HIGHLIGHT_COLUMN_TYPES),
    **_prefixed_column_types("season_", HOMEPAGE_DRAFT_HIGHLIGHT_COLUMN_TYPES),
    **_prefixed_column_types("alltime_", HOMEPAGE_TRANSACTION_HIGHLIGHT_COLUMN_TYPES),
    **_prefixed_column_types("season_", HOMEPAGE_TRANSACTION_HIGHLIGHT_COLUMN_TYPES),
    **_prefixed_column_types("alltime_trade_", HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES),
    **_prefixed_column_types("season_trade_", HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES),
}

HOMEPAGE_MANAGER_RANKINGS_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "franchise_id": "VARCHAR",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "win_pct": "DOUBLE",
    "championships": "BIGINT",
    "playoff_appearances": "BIGINT",
    "seasons": "BIGINT",
    "power_rating": "DOUBLE",
    "first_year": "INTEGER",
    "last_year": "INTEGER",
    "career_rank": "INTEGER",
}

HOMEPAGE_CURRENT_STANDINGS_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "franchise_id": "VARCHAR",
    "team_name": "VARCHAR",
    "wins": "BIGINT",
    "losses": "BIGINT",
    "ties": "BIGINT",
    "points_for": "DOUBLE",
    "win_pct": "DOUBLE",
    "power_rating": "DOUBLE",
    "p_playoffs": "DOUBLE",
    "p_champ": "DOUBLE",
    "standings_rank": "INTEGER",
}

HOMEPAGE_TOP_RIVALRIES_COLUMN_TYPES = {
    "manager1": "VARCHAR",
    "manager2": "VARCHAR",
    "franchise_id_1": "VARCHAR",
    "franchise_id_2": "VARCHAR",
    "total_games": "BIGINT",
    "manager1_wins": "BIGINT",
    "manager2_wins": "BIGINT",
    "ties": "BIGINT",
    "competitiveness_score": "DOUBLE",
    "avg_margin": "DOUBLE",
    "rivalry_rank": "INTEGER",
}

HOMEPAGE_MANAGER_CAREER_COLUMN_TYPES = {
    "total_wins": "BIGINT",
    "total_losses": "BIGINT",
    "total_ties": "BIGINT",
    "total_win_pct": "DOUBLE",
    "reg_wins": "BIGINT",
    "reg_losses": "BIGINT",
    "reg_ties": "BIGINT",
    "reg_win_pct": "DOUBLE",
    "playoff_wins": "BIGINT",
    "playoff_losses": "BIGINT",
    "playoff_ties": "BIGINT",
    "playoff_win_pct": "DOUBLE",
    "championships": "BIGINT",
    "sacko_bowls": "BIGINT",
    "playoff_appearances": "BIGINT",
    "playoff_rate": "DOUBLE",
    "first_year": "INTEGER",
    "last_year": "INTEGER",
    "seasons_played": "BIGINT",
    "current_team_name": "VARCHAR",
    "career_points": "DOUBLE",
}

HOMEPAGE_MANAGER_DRAFT_PICK_COLUMN_TYPES = {
    "best_pick_player": "VARCHAR",
    "best_pick_round": "INTEGER",
    "best_pick_year": "INTEGER",
    "best_pick_lamar": "DOUBLE",
    "best_pick_headshot": "VARCHAR",
    "best_pick_cost": "DOUBLE",
    "best_pick_pick": "INTEGER",
    "worst_pick_player": "VARCHAR",
    "worst_pick_round": "INTEGER",
    "worst_pick_year": "INTEGER",
    "worst_pick_lamar": "DOUBLE",
    "worst_pick_headshot": "VARCHAR",
    "worst_pick_cost": "DOUBLE",
    "worst_pick_pick": "INTEGER",
}

HOMEPAGE_MANAGER_TXN_COLUMN_TYPES = {
    "transaction_avg_quality_score": "DOUBLE",
    "transaction_best_add_player": "VARCHAR",
    "transaction_best_add_lamar": "DOUBLE",
    "transaction_best_add_year": "INTEGER",
    "transaction_best_add_week": "INTEGER",
    "transaction_best_add_headshot": "VARCHAR",
    "transaction_worst_drop_player": "VARCHAR",
    "transaction_worst_drop_lamar": "DOUBLE",
    "transaction_worst_drop_year": "INTEGER",
    "transaction_worst_drop_week": "INTEGER",
    "transaction_worst_drop_headshot": "VARCHAR",
    "transaction_best_trade_player": "VARCHAR",
    "transaction_best_trade_lamar": "DOUBLE",
    "transaction_best_trade_year": "INTEGER",
    "transaction_best_trade_headshot": "VARCHAR",
    "transaction_gpa": "DOUBLE",
    "transaction_overall_grade": "VARCHAR",
    "transaction_quality_metric": "DOUBLE",
}

HOMEPAGE_MANAGER_TRADE_COLUMN_TYPES = {
    "trade_winner": "VARCHAR",
    "trade_winner_players": "VARCHAR",
    "trade_winner_headshots": "VARCHAR",
    "trade_winner_lamar": "DOUBLE",
    "trade_loser": "VARCHAR",
    "trade_loser_players": "VARCHAR",
    "trade_loser_headshots": "VARCHAR",
    "trade_loser_lamar": "DOUBLE",
    "trade_net_lamar": "DOUBLE",
    "trade_year": "INTEGER",
    "trade_week": "INTEGER",
}

HOMEPAGE_MANAGER_RIVALRY_COLUMN_TYPES = {
    "nemesis_opponent": "VARCHAR",
    "nemesis_wins": "INTEGER",
    "nemesis_losses": "INTEGER",
    "nemesis_ties": "INTEGER",
    "victim_opponent": "VARCHAR",
    "victim_wins": "INTEGER",
    "victim_losses": "INTEGER",
    "victim_ties": "INTEGER",
    "closest_rival_opponent": "VARCHAR",
    "closest_rival_record": "VARCHAR",
    "closest_rival_avg_margin": "DOUBLE",
}

HOMEPAGE_MANAGER_PLAYER_LEADER_COLUMN_TYPES = {
    "best_game_lamar_player": "VARCHAR",
    "best_game_lamar_year": "INTEGER",
    "best_game_lamar_week": "INTEGER",
    "best_game_lamar_value": "DOUBLE",
    "best_game_lamar_headshot": "VARCHAR",
    "best_game_clutch_player": "VARCHAR",
    "best_game_clutch_year": "INTEGER",
    "best_game_clutch_week": "INTEGER",
    "best_game_clutch_value": "DOUBLE",
    "best_game_clutch_headshot": "VARCHAR",
    "best_season_lamar_player": "VARCHAR",
    "best_season_lamar_year": "INTEGER",
    "best_season_lamar_value": "DOUBLE",
    "best_season_lamar_headshot": "VARCHAR",
    "best_season_clutch_player": "VARCHAR",
    "best_season_clutch_year": "INTEGER",
    "best_season_clutch_value": "DOUBLE",
    "best_season_clutch_headshot": "VARCHAR",
    "best_career_lamar_player": "VARCHAR",
    "best_career_lamar_value": "DOUBLE",
    "best_career_lamar_headshot": "VARCHAR",
    "best_career_clutch_player": "VARCHAR",
    "best_career_clutch_value": "DOUBLE",
    "best_career_clutch_headshot": "VARCHAR",
}

HOMEPAGE_MANAGER_PROFILES_COLUMN_TYPES = {
    "manager": "VARCHAR",
    "franchise_id": "VARCHAR",
    "current_year": "INTEGER",
    **HOMEPAGE_MANAGER_CAREER_COLUMN_TYPES,
    "badges_list": "VARCHAR",
    **HOMEPAGE_MANAGER_DRAFT_PICK_COLUMN_TYPES,
    "draft_career_grade": "VARCHAR",
    **_prefixed_column_types("season_", HOMEPAGE_MANAGER_DRAFT_PICK_COLUMN_TYPES),
    "season_draft_grade": "VARCHAR",
    **HOMEPAGE_MANAGER_TXN_COLUMN_TYPES,
    **_prefixed_column_types("season_", HOMEPAGE_MANAGER_TXN_COLUMN_TYPES),
    **HOMEPAGE_MANAGER_TRADE_COLUMN_TYPES,
    **_prefixed_column_types("season_", HOMEPAGE_MANAGER_TRADE_COLUMN_TYPES),
    **HOMEPAGE_MANAGER_RIVALRY_COLUMN_TYPES,
    **HOMEPAGE_MANAGER_PLAYER_LEADER_COLUMN_TYPES,
    "timeline_data": "VARCHAR",
}

for _column_types_name in (
    "PLAYER_FANTASY_SEASON_COLUMN_TYPES",
    "PLAYER_FANTASY_CAREER_COLUMN_TYPES",
    "DRAFT_MANAGER_SEASON_COLUMN_TYPES",
    "DRAFT_MANAGER_CAREER_COLUMN_TYPES",
    "DRAFT_PLAYER_CAREER_COLUMN_TYPES",
    "TRANSACTION_MANAGER_SEASON_COLUMN_TYPES",
    "TRANSACTION_MANAGER_CAREER_COLUMN_TYPES",
    "TRANSACTION_PLAYER_CAREER_COLUMN_TYPES",
    "TRANSACTION_REPORT_CARD_COLUMN_TYPES",
    "MATCHUP_SEASON_COLUMN_TYPES",
    "MATCHUP_CAREER_COLUMN_TYPES",
    "MATCHUP_H2H_SEASON_COLUMN_TYPES",
    "MATCHUP_H2H_CAREER_COLUMN_TYPES",
    "ALL_PLAY_COLUMN_TYPES",
    "H2H_SEASON_COLUMN_TYPES",
    "SCHEDULE_SWAP_COLUMN_TYPES",
    "SCHEDULE_SWAP_SEASON_COLUMN_TYPES",
    "STANDINGS_BY_YEAR_COLUMN_TYPES",
    "HOMEPAGE_LEAGUE_RECORD_COLUMN_TYPES",
    "HOMEPAGE_PLAYER_LEADER_COLUMN_TYPES",
    "HOMEPAGE_DRAFT_HIGHLIGHT_COLUMN_TYPES",
    "HOMEPAGE_TRANSACTION_HIGHLIGHT_COLUMN_TYPES",
    "HOMEPAGE_TRADE_HIGHLIGHT_COLUMN_TYPES",
    "HOMEPAGE_LEAGUE_SUMMARY_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_RANKINGS_COLUMN_TYPES",
    "HOMEPAGE_CURRENT_STANDINGS_COLUMN_TYPES",
    "HOMEPAGE_TOP_RIVALRIES_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_CAREER_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_DRAFT_PICK_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_TXN_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_TRADE_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_RIVALRY_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_PLAYER_LEADER_COLUMN_TYPES",
    "HOMEPAGE_MANAGER_PROFILES_COLUMN_TYPES",
):
    globals()[_column_types_name] = _prepend_db_name(globals()[_column_types_name])

PLAYER_FANTASY_OPTIONAL_SOURCE_COLUMNS = (
    "player_lamar",
    "manager_lamar",
    "clutch_equity",
    "win",
    "loss",
    "team_points",
    "opponent_points",
    "is_playoffs",
    "championship",
    "optimal_player",
    "league_wide_optimal_player",
    "fantasy_position",
    "position",
    "franchise_id",
)

DRAFT_OPTIONAL_SOURCE_COLUMNS = (
    "draft_category",
    "manager_lamar",
    "lamar",
    "player_lamar",
    "total_fantasy_points",
    "points",
    "season_ppg",
    "pick_quality_zscore",
    "pick_quality_score",
    "games_played",
    "drafted_as_starter",
    "lamar_per_dollar",
    "is_bust",
    "is_breakout",
    "manager_draft_grade",
    "manager_draft_score",
    "manager_draft_percentile_alltime",
    "manager_hit_rate",
    "is_keeper",
    "is_keeper_status",
    "is_keeper_cost",
    "yahoo_position",
    "position",
)

TRANSACTION_OPTIONAL_SOURCE_COLUMNS = (
    "transaction_id",
    "player",
    "position",
    "manager_lamar_ros_managed",
    "fa_lamar_ros",
    "net_lamar_ros",
    "player_lamar_ros_total",
    "transaction_score",
    "score_timing_mult",
    "faab_bid",
    "points_per_faab_dollar",
    "drop_regret_score",
    "ppg_before_transaction",
    "ppg_after_transaction",
    "net_points_ros",
    "total_points_ros_managed",
    "total_points_rest_of_season",
    "total_points_ros_total",
    "trade_direction",
    "source_manager",
    "trade_asset_lamar",
    "week",
    "traded_pick_season",
    "traded_pick_round",
    "traded_pick_original_owner",
    "transaction_grade",
)

MATCHUP_OPTIONAL_SOURCE_COLUMNS = (
    "franchise_id",
    "franchise_name",
    "tie",
    "gpa",
    "above_league_median",
    "close_margin",
    "margin",
    "win_streak",
    "loss_streak",
    "winning_streak",
    "losing_streak",
    "optimal_points",
    "team_projected_points",
    "opponent_projected_points",
    "expected_spread",
    "expected_odds",
    "underdog_wins",
    "favorite_losses",
    "proj_score_error",
    "abs_proj_score_error",
    "is_playoffs",
    "champion",
    "sacko",
    "playoff_round",
    "consolation_round",
    "is_bye_week",
    "is_bye",
    "is_placeholder",
    *MATCHUP_SEASON_SNAPSHOT_COLUMNS,
)

AGGREGATE_TABLE_SPECS: dict[str, AggregateTableSpec] = {
    "player_fantasy_season": AggregateTableSpec(
        column_types=PLAYER_FANTASY_SEASON_COLUMN_TYPES,
        primary_key=("NFL_player_id", "year"),
        source_tables={
            "player_fantasy": AggregateSourceSpec(
                required=("NFL_player_id", "year", "player", "fantasy_points", "is_started", "manager"),
                optional=PLAYER_FANTASY_OPTIONAL_SOURCE_COLUMNS,
            )
        },
        description="Regular-season player fantasy rollup by player/year.",
    ),
    "player_fantasy_career": AggregateTableSpec(
        column_types=PLAYER_FANTASY_CAREER_COLUMN_TYPES,
        primary_key=("NFL_player_id",),
        source_tables={
            "player_fantasy": AggregateSourceSpec(
                required=("NFL_player_id", "year", "player", "fantasy_points", "is_started", "manager"),
                optional=PLAYER_FANTASY_OPTIONAL_SOURCE_COLUMNS,
            )
        },
        description="Regular-season player fantasy rollup across all years.",
    ),
    "player_fantasy_season_all": AggregateTableSpec(
        column_types=PLAYER_FANTASY_SEASON_COLUMN_TYPES,
        primary_key=("NFL_player_id", "year"),
        source_tables={
            "player_fantasy": AggregateSourceSpec(
                required=("NFL_player_id", "year", "player", "fantasy_points", "is_started", "manager"),
                optional=PLAYER_FANTASY_OPTIONAL_SOURCE_COLUMNS,
            )
        },
        description="All-games player fantasy rollup by player/year.",
    ),
    "player_fantasy_career_all": AggregateTableSpec(
        column_types=PLAYER_FANTASY_CAREER_COLUMN_TYPES,
        primary_key=("NFL_player_id",),
        source_tables={
            "player_fantasy": AggregateSourceSpec(
                required=("NFL_player_id", "year", "player", "fantasy_points", "is_started", "manager"),
                optional=PLAYER_FANTASY_OPTIONAL_SOURCE_COLUMNS,
            )
        },
        description="All-games player fantasy rollup across all years.",
    ),
    "draft_manager_season": AggregateTableSpec(
        column_types=DRAFT_MANAGER_SEASON_COLUMN_TYPES,
        primary_key=("franchise_id", "year", "draft_category"),
        source_tables={
            "draft": AggregateSourceSpec(
                required=("manager", "year", "player", "cost"), optional=DRAFT_OPTIONAL_SOURCE_COLUMNS
            )
        },
        description="Draft rollup by manager/year/category.",
    ),
    "draft_manager_career": AggregateTableSpec(
        column_types=DRAFT_MANAGER_CAREER_COLUMN_TYPES,
        primary_key=("franchise_id", "draft_category"),
        source_tables={
            "draft_manager_season": AggregateSourceSpec(
                required=("manager", "year", "draft_category", "picks", "total_manager_lamar"),
                optional=tuple(
                    column for column in DRAFT_MANAGER_SEASON_COLUMN_TYPES if column not in {"manager", "year"}
                ),
            )
        },
        description="Draft rollup by manager/category across seasons.",
    ),
    "draft_player_career": AggregateTableSpec(
        column_types=DRAFT_PLAYER_CAREER_COLUMN_TYPES,
        primary_key=("player", "position", "draft_category"),
        source_tables={
            "draft": AggregateSourceSpec(
                required=("player", "year", "manager", "cost"), optional=DRAFT_OPTIONAL_SOURCE_COLUMNS
            )
        },
        description="Draft rollup by player/position/category.",
    ),
    "transaction_manager_season": AggregateTableSpec(
        column_types=TRANSACTION_MANAGER_SEASON_COLUMN_TYPES,
        primary_key=("franchise_id", "year"),
        source_tables={
            "transactions": AggregateSourceSpec(
                required=("manager", "year", "transaction_type"),
                optional=TRANSACTION_OPTIONAL_SOURCE_COLUMNS,
            )
        },
        description="Transaction rollup by manager/year.",
    ),
    "transaction_manager_career": AggregateTableSpec(
        column_types=TRANSACTION_MANAGER_CAREER_COLUMN_TYPES,
        primary_key=("franchise_id",),
        source_tables={
            "transaction_manager_season": AggregateSourceSpec(
                required=("manager", "year", "adds", "drops", "total_moves", "net_lamar", "total_transaction_score"),
                optional=tuple(
                    column for column in TRANSACTION_MANAGER_SEASON_COLUMN_TYPES if column not in {"manager", "year"}
                ),
            )
        },
        description="Transaction rollup by manager across seasons.",
    ),
    "transaction_player_career": AggregateTableSpec(
        column_types=TRANSACTION_PLAYER_CAREER_COLUMN_TYPES,
        primary_key=("player", "position"),
        source_tables={
            "transactions": AggregateSourceSpec(
                required=("player", "position", "year", "transaction_type"),
                optional=TRANSACTION_OPTIONAL_SOURCE_COLUMNS,
            )
        },
        description="Transaction rollup by player/position.",
    ),
    "transaction_report_card": AggregateTableSpec(
        column_types=TRANSACTION_REPORT_CARD_COLUMN_TYPES,
        primary_key=("franchise_id", "year"),
        source_tables={
            "transaction_manager_season": AggregateSourceSpec(
                required=(
                    "manager",
                    "year",
                    "adds",
                    "drops",
                    "trades",
                    "net_lamar",
                    "transaction_grade",
                    "transaction_gpa",
                ),
                optional=("trade_wins", "total_faab_bid"),
            ),
            "transactions": AggregateSourceSpec(
                required=("manager", "year", "transaction_type"),
                optional=TRANSACTION_OPTIONAL_SOURCE_COLUMNS,
            ),
        },
        description="Per-manager transaction report card payload.",
    ),
    "matchup_season": AggregateTableSpec(
        column_types=MATCHUP_SEASON_COLUMN_TYPES,
        primary_key=("franchise_id", "year"),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "year", "week", "opponent", "team_points", "opponent_points", "win", "loss"),
                optional=MATCHUP_OPTIONAL_SOURCE_COLUMNS,
            ),
            "league_settings": AggregateSourceSpec(required=("year", "playoff_start_week")),
        },
        description="Regular-season matchup rollup by manager/year with playoff snapshots.",
    ),
    "matchup_career": AggregateTableSpec(
        column_types=MATCHUP_CAREER_COLUMN_TYPES,
        primary_key=("franchise_id",),
        source_tables={
            "matchup_season": AggregateSourceSpec(
                required=(
                    "manager",
                    "year",
                    "games",
                    "wins",
                    "losses",
                    "ties",
                    "total_team_points",
                    "total_opponent_points",
                    "max_team_points",
                    "min_team_points",
                ),
                optional=tuple(column for column in MATCHUP_SEASON_COLUMN_TYPES if column not in {"manager", "year"}),
            )
        },
        description="Career matchup rollup by manager.",
    ),
    "matchup_h2h_season": AggregateTableSpec(
        column_types=MATCHUP_H2H_SEASON_COLUMN_TYPES,
        primary_key=("franchise_id", "opponent_franchise_id", "year"),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "opponent", "year", "week", "team_points", "margin", "win", "loss"),
                optional=("tie", "is_bye_week", "is_bye", "is_placeholder"),
            ),
            "league_settings": AggregateSourceSpec(required=("year", "playoff_start_week")),
        },
        description="Regular-season head-to-head matchup rollup by manager/opponent/year.",
    ),
    "matchup_h2h_career": AggregateTableSpec(
        column_types=MATCHUP_H2H_CAREER_COLUMN_TYPES,
        primary_key=("franchise_id", "opponent_franchise_id"),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "opponent", "year", "week", "team_points", "margin", "win", "loss"),
                optional=("tie", "is_bye_week", "is_bye", "is_placeholder"),
            ),
            "league_settings": AggregateSourceSpec(required=("year", "playoff_start_week")),
        },
        description="Career head-to-head matchup rollup by manager/opponent.",
    ),
    "all_play": AggregateTableSpec(
        column_types=ALL_PLAY_COLUMN_TYPES,
        primary_key=("franchise_id", "opponent_franchise_id", "year", "week"),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("year", "week", "franchise_id", "team_points"),
                optional=("is_playoffs", "is_consolation", "is_bye_week", "is_bye", "is_placeholder"),
            ),
            "league_settings": AggregateSourceSpec(required=("year", "playoff_start_week")),
        },
        description="Weekly all-play matrix: each manager score vs every other manager score.",
    ),
    "h2h_season": AggregateTableSpec(
        column_types=H2H_SEASON_COLUMN_TYPES,
        primary_key=("franchise_id", "opponent_franchise_id", "year"),
        source_tables={
            "all_play": AggregateSourceSpec(
                required=("franchise_id", "opponent_franchise_id", "year", "result"),
            ),
        },
        description="Season rollup of all-play matrix records by manager/opponent/year.",
    ),
    "schedule_swap": AggregateTableSpec(
        column_types=SCHEDULE_SWAP_COLUMN_TYPES,
        primary_key=("franchise_id", "schedule_of_franchise_id", "year", "week"),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("year", "week", "franchise_id", "opponent_franchise_id", "team_points", "opponent_points"),
                optional=("is_playoffs", "is_consolation", "is_bye_week", "is_bye", "is_placeholder"),
            ),
            "league_settings": AggregateSourceSpec(required=("year", "playoff_start_week")),
        },
        description="Weekly schedule-swap matrix: each manager score vs each schedule owner's opponent, including own schedule and excluding impossible self-opponent swaps.",
    ),
    "schedule_swap_season": AggregateTableSpec(
        column_types=SCHEDULE_SWAP_SEASON_COLUMN_TYPES,
        primary_key=("franchise_id", "schedule_of_franchise_id", "year"),
        source_tables={
            "schedule_swap": AggregateSourceSpec(
                required=("franchise_id", "schedule_of_franchise_id", "year", "result"),
            ),
        },
        description="Season rollup of schedule-swap records by manager/schedule owner/year, including own schedule and excluding impossible self-opponent swaps.",
    ),
    "standings_by_year": AggregateTableSpec(
        column_types=STANDINGS_BY_YEAR_COLUMN_TYPES,
        primary_key=("franchise_id", "year"),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "year", "week", "team_name", "team_points", "opponent_points", "win", "loss"),
                optional=(
                    "tie",
                    "is_playoffs",
                    "is_consolation",
                    "champion",
                    "sacko",
                    "consolation_round",
                    "playoff_round",
                    "final_playoff_seed",
                    "franchise_id",
                    "above_league_median",
                    "is_bye_week",
                    "is_bye",
                ),
            ),
            "league_settings": AggregateSourceSpec(required=("year",), optional=("uses_median",)),
        },
        description="Standings snapshot by manager/year for fast standings queries.",
    ),
    "homepage_league_summary": AggregateTableSpec(
        column_types=HOMEPAGE_LEAGUE_SUMMARY_COLUMN_TYPES,
        primary_key=(),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "year", "week", "team_points", "opponent_points"),
                optional=(
                    "opponent",
                    "champion",
                    "is_consolation",
                    "is_playoffs",
                    "power_rating",
                    "p_playoffs",
                    "p_champ",
                    "above_league_median",
                    "below_league_median",
                ),
            ),
            "player_fantasy": AggregateSourceSpec(
                required=("manager", "player", "year"),
                optional=("week", "manager_lamar", "clutch_equity", "is_started"),
            ),
            "draft": AggregateSourceSpec(
                required=("manager", "player", "year"),
                optional=("round", "pick", "cost", "draft_grade", "position"),
            ),
            "transactions": AggregateSourceSpec(
                required=("manager", "player", "year", "transaction_type"),
                optional=(
                    "week",
                    "transaction_grade",
                    "trade_direction",
                    "source_manager",
                    "trade_asset_lamar",
                    "manager_lamar_ros_managed",
                    "player_lamar_ros_total",
                ),
            ),
            "league_settings": AggregateSourceSpec(required=("year",), optional=("uses_median",)),
        },
        description="Single-row homepage overview payload for league-wide records and highlights.",
    ),
    "homepage_manager_rankings": AggregateTableSpec(
        column_types=HOMEPAGE_MANAGER_RANKINGS_COLUMN_TYPES,
        primary_key=("franchise_id",),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "year", "week", "win", "loss"),
                optional=("tie", "champion", "is_playoffs", "is_consolation", "power_rating"),
            ),
            "league_settings": AggregateSourceSpec(required=("year",), optional=("uses_median",)),
        },
        description="Homepage career rankings payload by manager.",
    ),
    "homepage_current_standings": AggregateTableSpec(
        column_types=HOMEPAGE_CURRENT_STANDINGS_COLUMN_TYPES,
        primary_key=("franchise_id",),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "year", "week", "team_name", "team_points", "win", "loss"),
                optional=(
                    "tie",
                    "power_rating",
                    "p_playoffs",
                    "p_champ",
                    "is_consolation",
                    "above_league_median",
                    "below_league_median",
                ),
            ),
            "league_settings": AggregateSourceSpec(required=("year",), optional=("uses_median",)),
        },
        description="Homepage current standings payload by manager.",
    ),
    "homepage_top_rivalries": AggregateTableSpec(
        column_types=HOMEPAGE_TOP_RIVALRIES_COLUMN_TYPES,
        primary_key=("franchise_id_1", "franchise_id_2"),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "opponent", "win", "loss", "team_points", "opponent_points"),
                optional=("tie", "is_consolation"),
            )
        },
        description="Homepage rivalry leaderboard payload.",
    ),
    "homepage_manager_profiles": AggregateTableSpec(
        column_types=HOMEPAGE_MANAGER_PROFILES_COLUMN_TYPES,
        primary_key=("franchise_id",),
        source_tables={
            "matchup": AggregateSourceSpec(
                required=("manager", "year", "week", "team_points", "opponent_points", "win", "loss"),
                optional=(
                    "tie",
                    "team_name",
                    "champion",
                    "sacko",
                    "is_playoffs",
                    "is_consolation",
                    "playoff_round",
                    "consolation_round",
                    "above_league_median",
                    "below_league_median",
                ),
            ),
            "player_fantasy": AggregateSourceSpec(
                required=("manager", "player", "year"),
                optional=("week", "manager_lamar", "clutch_equity", "is_started", "NFL_player_id"),
            ),
            "draft": AggregateSourceSpec(
                required=("manager", "player", "year"),
                optional=("round", "pick", "cost", "draft_grade", "position", "draft_value_zscore"),
            ),
            "transactions": AggregateSourceSpec(
                required=("manager", "player", "year", "transaction_type"),
                optional=(
                    "week",
                    "transaction_grade",
                    "transaction_quality_score",
                    "trade_direction",
                    "source_manager",
                    "trade_asset_lamar",
                    "manager_lamar_ros_managed",
                    "player_lamar_ros_total",
                ),
            ),
            "matchup_season": AggregateSourceSpec(
                required=("manager", "year"),
                optional=("avg_team_points", "power_rating", "total_team_points", "optimal_ceiling_pts"),
            ),
            "player_fantasy_season": AggregateSourceSpec(
                required=("year", "managers"),
                optional=("manager_lamar",),
            ),
        },
        description="Homepage profile payload by manager.",
    ),
}

AGGREGATE_TABLE_SPECS = {
    table_name: AggregateTableSpec(
        column_types=spec.column_types,
        primary_key=_prepend_primary_key(spec.primary_key),
        source_tables=spec.source_tables,
        description=spec.description,
    )
    for table_name, spec in AGGREGATE_TABLE_SPECS.items()
}

AGGREGATE_TABLE_COLUMN_TYPES: dict[str, dict[str, str]] = {
    table: spec.column_types for table, spec in AGGREGATE_TABLE_SPECS.items()
}


def aggregate_table_columns(table_name: str) -> list[str]:
    return list(AGGREGATE_TABLE_SPECS[table_name].column_types.keys())


def aggregate_insert_columns(table_name: str, columns: list[str] | tuple[str, ...]) -> str:
    unknown = [column for column in columns if column not in AGGREGATE_TABLE_SPECS[table_name].column_types]
    if unknown:
        raise ValueError(f"{table_name} insert includes non-canonical aggregate columns: {unknown}")
    return ", ".join(f'"{column}"' for column in columns)


def _create_table_sql(database_name: str, table_name: str) -> str:
    spec = AGGREGATE_TABLE_SPECS[table_name]
    column_lines = [f'    "{column}" {dtype}' for column, dtype in spec.column_types.items()]
    if column_lines:
        column_lines[0] = '    "db_name" VARCHAR NOT NULL'
    if spec.primary_key:
        pk = ", ".join(f'"{column}"' for column in spec.primary_key)
        column_lines.append(f"    PRIMARY KEY ({pk})")
    return f'CREATE TABLE IF NOT EXISTS "{database_name}".public.{table_name} (\n' + ",\n".join(column_lines) + "\n)"


def create_aggregate_table_sql(database_name: str, table_name: str) -> str:
    return _create_table_sql(database_name, table_name)


def _existing_columns(conn, database_name: str, table_name: str) -> set[str]:
    """Return columns for ``database_name.public.<table_name>`` (read-only).

    In the centralized model ``database_name`` is always ``___leagues``; the
    query uses ``current_database()`` so it works even if the connection was
    attached under a different alias.
    """
    _ = database_name  # kept for signature compatibility
    try:
        rows = conn.execute(
            "SELECT column_name "
            "FROM information_schema.columns "
            f"WHERE table_catalog = current_database() AND table_schema = 'public' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        ).fetchall()
        return {row[0] for row in rows}
    except Exception:
        return set()


def _existing_column_types(conn, database_name: str, table_name: str) -> dict[str, str]:
    """Return column → type mapping for ``database_name.public.<table_name>``."""
    _ = database_name  # kept for signature compatibility
    try:
        rows = conn.execute(
            "SELECT column_name, data_type "
            "FROM information_schema.columns "
            f"WHERE table_catalog = current_database() AND table_schema = 'public' AND table_name = '{table_name}' "
            "ORDER BY ordinal_position"
        ).fetchall()
        return {row[0]: row[1] for row in rows}
    except Exception:
        return {}


def _table_exists(conn, database_name: str, table_name: str) -> bool:
    """Return True iff ``database_name.public.<table_name>`` exists."""
    _ = database_name  # kept for signature compatibility
    try:
        row = conn.execute(
            "SELECT COUNT(*) "
            "FROM information_schema.tables "
            f"WHERE table_catalog = current_database() AND table_schema = 'public' AND table_name = '{table_name}'"
        ).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


_TYPE_ALIASES = {
    "INTEGER": {"INTEGER", "INT", "INT32", "SIGNED"},
    "BIGINT": {"BIGINT", "INT64", "LONG", "HUGEINT"},
    "DOUBLE": {"DOUBLE", "DOUBLE PRECISION", "FLOAT", "REAL", "DECIMAL", "NUMERIC"},
    "BOOLEAN": {"BOOLEAN", "BOOL", "LOGICAL"},
    "VARCHAR": {"VARCHAR", "TEXT", "STRING"},
    "TIMESTAMP": {"TIMESTAMP", "DATETIME"},
}


def _canonicalize_duckdb_type(dtype: str) -> str:
    return str(dtype).upper().split("(")[0].strip()


def _types_compatible(actual: str, expected: str) -> bool:
    actual_norm = _canonicalize_duckdb_type(actual)
    expected_norm = _canonicalize_duckdb_type(expected)
    if actual_norm == expected_norm:
        return True
    return actual_norm in _TYPE_ALIASES.get(expected_norm, {expected_norm})


def _validate_aggregate_schema(conn, database_name: str, table_name: str) -> None:
    expected = AGGREGATE_TABLE_SPECS[table_name].column_types
    actual = _existing_column_types(conn, database_name, table_name)
    actual_columns = set(actual.keys())
    expected_columns = set(expected.keys())

    missing = sorted(expected_columns - actual_columns)
    extra = sorted(actual_columns - expected_columns)
    type_mismatches = sorted(
        column
        for column, dtype in expected.items()
        if column in actual and not _types_compatible(actual[column], dtype)
    )

    if not missing and not extra and not type_mismatches:
        return

    problems: list[str] = []
    if missing:
        problems.append(f"missing={missing}")
    if extra:
        problems.append(f"extra={extra}")
    if type_mismatches:
        mismatch_detail = {
            column: {"actual": actual[column], "expected": expected[column]} for column in type_mismatches
        }
        problems.append(f"type_mismatches={mismatch_detail}")
    raise ValueError(f"{table_name} aggregate schema drift detected; rebuild required ({'; '.join(problems)})")


_CENTRALIZED_CATALOG = "___leagues"


def _is_centralized(database_name: str) -> bool:
    """True if the target catalog is the centralized ``___leagues`` database."""
    return str(database_name).strip('"') == _CENTRALIZED_CATALOG


def create_named_aggregate_table_sql(database_name: str, spec_table_name: str, target_table_name: str) -> str:
    spec = AGGREGATE_TABLE_SPECS[spec_table_name]
    column_lines = [f'    "{column}" {dtype}' for column, dtype in spec.column_types.items()]
    if column_lines:
        column_lines[0] = '    "db_name" VARCHAR NOT NULL'
    return f'CREATE TABLE "{database_name}".public.{target_table_name} (\n' + ",\n".join(column_lines) + "\n)"


def _create_temp_staging_sql(spec_table_name: str, staging_table_name: str) -> str:
    """Build a ``CREATE OR REPLACE TEMP TABLE`` for a staging table.

    TEMP tables are session-scoped, so they are safe under parallel
    centralized imports — no two concurrent sessions can collide.
    """
    spec = AGGREGATE_TABLE_SPECS[spec_table_name]
    column_lines = [f'    "{column}" {dtype}' for column, dtype in spec.column_types.items()]
    if column_lines:
        column_lines[0] = '    "db_name" VARCHAR NOT NULL'
    return f"CREATE OR REPLACE TEMP TABLE {staging_table_name} (\n" + ",\n".join(column_lines) + "\n)"


def recreate_aggregate_table_like(conn, database_name: str, spec_table_name: str, target_table_name: str) -> None:
    """Recreate a staging table with the same schema as a canonical aggregate.

    In the centralized model the staging table is a session-scoped
    ``TEMP TABLE`` so concurrent imports cannot clobber each other, and no
    DROP ever touches the shared ``___leagues.public.*`` namespace.

    For non-centralized callers (local DuckDB tests), the old drop+create
    behavior is preserved against ``database_name.public.<target_table_name>``.
    """
    if _is_centralized(database_name):
        conn.execute(_create_temp_staging_sql(spec_table_name, target_table_name))
        return
    conn.execute(f'DROP TABLE IF EXISTS "{database_name}".public.{target_table_name}')
    conn.execute(create_named_aggregate_table_sql(database_name, spec_table_name, target_table_name))


def ensure_aggregate_table(conn, database_name: str, table_name: str) -> None:
    """Ensure an aggregate table exists and matches the canonical schema.

    **Strict DDL compliance** (centralized model): tables in the
    ``___leagues`` catalog must be bootstrapped ahead of time via
    ``scripts/migrate_to_centralized.py``; imports never create them.
    When the target is ``___leagues`` and the table is missing, raise.

    For local DuckDB scratch files (any other ``database_name``), continue
    to auto-create the table because local files are disposable per-import
    artifacts, not a shared contract.
    """
    cache = getattr(conn, "_aggregate_schema_validation_cache", None)
    cache_key = (database_name, table_name)
    if cache is not None and cache_key in cache:
        return
    if not _table_exists(conn, database_name, table_name):
        if _is_centralized(database_name):
            raise RuntimeError(
                f"[STRICT DDL] {database_name}.public.{table_name} does not exist. "
                f"Bootstrap the centralized shell via "
                f"`python scripts/migrate_to_centralized.py` before running imports. "
                f"Imports never create tables in {database_name}."
            )
        conn.execute(_create_table_sql(database_name, table_name))
        if cache is not None:
            cache.add(cache_key)
        return
    _validate_aggregate_schema(conn, database_name, table_name)
    if cache is not None:
        cache.add(cache_key)


def recreate_aggregate_table(conn, database_name: str, table_name: str) -> None:
    """Drop + recreate an aggregate table.

    FORBIDDEN against the centralized ``___leagues`` catalog because it
    wipes every league's rows. Centralized callers should use
    :func:`ensure_aggregate_table` (schema drift is managed via canonical
    DDL migrations) and delete per-league rows with
    ``DELETE FROM ___leagues.public.<table> WHERE db_name = ?``.
    """
    if _is_centralized(database_name):
        raise RuntimeError(
            f"recreate_aggregate_table('___leagues', {table_name!r}) is unsafe: "
            "this would wipe data across every league. Use ensure_aggregate_table() "
            "plus a scoped DELETE/INSERT via replace_scoped_aggregate_table_from_dataframe()."
        )
    conn.execute(f'DROP TABLE IF EXISTS "{database_name}".public.{table_name}')
    conn.execute(_create_table_sql(database_name, table_name))


def _infer_duckdb_type_from_series(series: pd.Series) -> str:
    if pd.api.types.is_bool_dtype(series):
        return "BOOLEAN"
    if pd.api.types.is_integer_dtype(series):
        return "BIGINT"
    if pd.api.types.is_float_dtype(series):
        return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(series):
        return "TIMESTAMP"
    return "VARCHAR"


def _quote_identifier(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _prepare_dataframe_columns(df: pd.DataFrame) -> pd.DataFrame:
    prepared = df.copy()
    prepared.columns = [str(column) for column in prepared.columns]
    if prepared.columns.duplicated().any():
        dupes = prepared.columns[prepared.columns.duplicated()].tolist()
        raise ValueError(f"dataframe has duplicate columns: {dupes}")
    if not list(prepared.columns):
        raise ValueError("dataframe has no columns")
    return prepared


def replace_aggregate_table_from_dataframe(conn, database_name: str, table_name: str, df: pd.DataFrame) -> None:
    """Replace all rows in an aggregate table from a DataFrame.

    FORBIDDEN against the centralized ``___leagues`` catalog because the
    unscoped ``DELETE FROM ___leagues.public.<table>`` wipes every league's
    rows. Centralized callers must use
    :func:`multi_league.transformations.aggregation.aggregation_utils.replace_scoped_aggregate_table_from_dataframe`,
    which scopes the DELETE by ``db_name``.
    """
    if _is_centralized(database_name):
        raise RuntimeError(
            f"replace_aggregate_table_from_dataframe('___leagues', {table_name!r}) is unsafe: "
            "unscoped DELETE would wipe every league's rows. Use "
            "aggregation_utils.replace_scoped_aggregate_table_from_dataframe() instead."
        )

    prepared = _prepare_dataframe_columns(df)
    expected_columns = aggregate_table_columns(table_name)
    expected_types = AGGREGATE_TABLE_SPECS[table_name].column_types

    extra = sorted(set(prepared.columns) - set(expected_columns))
    if extra:
        raise ValueError(f"{table_name} aggregate dataframe has non-canonical columns: {extra}")

    for column in expected_columns:
        if column not in prepared.columns:
            prepared[column] = pd.NA
    prepared = prepared[expected_columns]

    try:
        ensure_aggregate_table(conn, database_name, table_name)
    except ValueError:
        # Schema drift still requires a rebuild, but avoid dropping/recreating
        # the table on every normal replace because newer DuckDB
        # clients can surface transaction conflicts during rapid DDL churn.
        recreate_aggregate_table(conn, database_name, table_name)

    conn.execute(f'DELETE FROM "{database_name}".public.{table_name}')

    register_name = "_aggregate_upload"
    conn.register(register_name, prepared)
    try:
        cast_select = ", ".join(
            f"CAST({_quote_identifier(column)} AS {expected_types[column]}) AS {_quote_identifier(column)}"
            for column in expected_columns
        )
        insert_cols = aggregate_insert_columns(table_name, expected_columns)
        conn.execute(
            f'INSERT INTO "{database_name}".public.{table_name} ({insert_cols}) '
            f"SELECT {cast_select} FROM {register_name}"
        )
    finally:
        conn.unregister(register_name)


def replace_snapshot_table_from_dataframe(conn, database_name: str, table_name: str, df: pd.DataFrame) -> None:
    """Replace a snapshot table (ad-hoc or AGGREGATE_TABLE_SPECS-registered).

    FORBIDDEN against the centralized ``___leagues`` catalog when the table
    is ad-hoc (unscoped DROP). Centralized callers should use the scoped
    helper in :mod:`multi_league.transformations.aggregation.aggregation_utils`.
    """
    if table_name in AGGREGATE_TABLE_SPECS:
        replace_aggregate_table_from_dataframe(conn, database_name, table_name, df)
        return

    if _is_centralized(database_name):
        raise RuntimeError(
            f"replace_snapshot_table_from_dataframe('___leagues', {table_name!r}) is unsafe: "
            "unscoped DROP+CREATE would destroy every league's rows. "
            "Register the table in AGGREGATE_TABLE_SPECS and use the scoped helper."
        )

    prepared = _prepare_dataframe_columns(df)
    df_columns = list(prepared.columns)
    df_types = {column: _infer_duckdb_type_from_series(prepared[column]) for column in df_columns}

    if _table_exists(conn, database_name, table_name):
        existing = _existing_column_types(conn, database_name, table_name)
        existing_columns = list(existing.keys())
        if set(existing_columns) != set(df_columns):
            missing = sorted(set(existing_columns) - set(df_columns))
            extra = sorted(set(df_columns) - set(existing_columns))
            raise ValueError(
                f"{table_name} homepage snapshot schema drift detected; "
                f"missing_from_df={missing}; new_in_df={extra}"
            )
        ordered_columns = existing_columns
        target_types = existing
    else:
        ordered_columns = df_columns
        target_types = df_types

    conn.execute(f'DROP TABLE IF EXISTS "{database_name}".public.{table_name}')
    column_lines = [f"    {_quote_identifier(column)} {target_types[column]}" for column in ordered_columns]
    conn.execute(f'CREATE TABLE "{database_name}".public.{table_name} (\n' + ",\n".join(column_lines) + "\n)")

    register_name = "_snapshot_upload"
    conn.register(register_name, prepared)
    try:
        cast_select = ", ".join(
            f"CAST({_quote_identifier(column)} AS {target_types[column]}) AS {_quote_identifier(column)}"
            for column in ordered_columns
        )
        insert_cols = ", ".join(_quote_identifier(column) for column in ordered_columns)
        conn.execute(
            f'INSERT INTO "{database_name}".public.{table_name} ({insert_cols}) '
            f"SELECT {cast_select} FROM {register_name}"
        )
    finally:
        conn.unregister(register_name)
