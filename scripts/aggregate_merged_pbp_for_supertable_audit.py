#!/usr/bin/env python3
"""
Aggregate merged 1978-2025 play-by-play to player-level rollups for supertable gap audits.

Inputs:
  - nfl_pbp_1978_2025_merged.parquet, built by merge_stathead_nflverse_pbp.py
  - player_bio_stathead_pfr_repaired.parquet when present, else player_bio.parquet

Outputs:
  - pbp_player_week_rollup.parquet
  - pbp_player_nfl_season.parquet
  - pbp_player_nfl_season_all.parquet
  - pbp_player_nfl_career.parquet
  - pbp_player_nfl_career_all.parquet
  - CSV summaries for unmapped players, yearly coverage, and role counts
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

try:
    from scripts.sota_recon.pbp_taxonomy import fumble_mentions_sql, official_play_sql
except ModuleNotFoundError:  # direct ``python scripts/aggregate_...py`` invocation
    from sota_recon.pbp_taxonomy import fumble_mentions_sql, official_play_sql
from typing import Any

import duckdb


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "fantasy_football_data_scripts"))
from multi_league.core.data_lake_paths import (  # noqa: E402
    nfl_ops_historical_root,
    player_bio_path,
    raw_stathead_generated_root,
)

# Canonical D-drive data lake (override via LEAGUE_HISTORY_DATA_ROOT). No C-drive default.
_STATHEAD_ROOT = raw_stathead_generated_root()
DEFAULT_PBP = _STATHEAD_ROOT / "pbp_merged_1978_2025" / "nfl_pbp_1978_2025_merged.parquet"
DEFAULT_OUTPUT_DIR = _STATHEAD_ROOT / "pbp_supertable_audit_1978_2025"
DEFAULT_BIO_REPAIRED = nfl_ops_historical_root() / "player_bio_stathead_pfr_repaired.parquet"
DEFAULT_BIO = player_bio_path()


STAT_COLUMNS = [
    "attempts",
    "completions",
    "passing_yards",
    "passing_tds",
    "passing_interceptions",
    "sacks_suffered",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "targets",
    "receptions",
    "receiving_yards",
    "receiving_tds",
    # Success-rate atoms, standard nflverse/nflfastR definition (EPA > 0 — see
    # epa_success_num()). Matches published rbsdm/nflverse values exactly for the
    # 1999+ era. Numerator = successful plays; denominator = plays with a success
    # flag. Stored as counts so they SUM correctly across weeks/seasons/careers;
    # rate is derived downstream as success / success_plays (like comp / att).
    "pass_success",
    "pass_success_plays",
    "rush_success",
    "rush_success_plays",
    "rec_success",
    "rec_success_plays",
    # Advanced efficiency atoms, validated to hit nflverse stats_player_week 100%
    # (see scripts/validate_pbp_atoms_vs_nflverse.py). Phase-separated, stored as
    # SUM-able values. EPA (qb_epa/epa) is EPA-derived so trustworthy 1999+ only
    # (passing_epa exact 2015+, ~99.5% to 2005 — qb_epa sack/scramble drift pre-2005).
    # air_yards begin ~2006 (charting era). 2pt conversions counts.
    "passing_epa",
    "rushing_epa",
    "receiving_epa",
    # WPA (win probability added) — direct EPA analog from the PBP `wpa` column,
    # phase-separated. Derive-only: nflverse does not publish per-player WPA, so this
    # has no external per-player gate (like EPA it is a PBP passthrough, trustworthy
    # where the win-prob model is, i.e. 1999+). total_wpa = sum of the three downstream.
    "passing_wpa",
    "rushing_wpa",
    "receiving_wpa",
    "passing_air_yards",
    "receiving_air_yards",
    "passing_2pt_conversions",
    "rushing_2pt_conversions",
    "receiving_2pt_conversions",
    # CPOE is a MEAN, stored as SUM-able parts: passing_cpoe = sum / n downstream.
    # cpoe is only populated on thrown passes (2006+), so the filter is cpoe NOT NULL.
    "passing_cpoe_sum",
    "passing_cpoe_n",
    # Model-free situational/explosive atoms (down/distance/yards/field position only,
    # trustworthy back to 1978). No nflverse per-player reference exists for these, so
    # they are correct-by-construction PBP counts, not externally gated. Standard
    # explosive thresholds: pass/rec 20+, rush 10+. Red zone = opponent 1-20 yard line.
    "pass_explosive_20",
    "rush_explosive_10",
    "rec_explosive_20",
    "rz_pass_att",
    "rz_pass_td",
    "rz_carries",
    "rz_rush_td",
    "rz_targets",
    "rz_rec_td",
    "completions_40plus",
    "completions_50plus",
    "passing_tds_40plus",
    "passing_tds_50plus",
    "receptions_0_4",
    "receptions_5_9",
    "receptions_10_19",
    "receptions_20_29",
    "receptions_30_39",
    "receptions_40plus",
    "receiving_tds_40plus",
    "receiving_tds_50plus",
    "fumbles",
    "fumbles_lost",
    "rushing_fumbles",
    "rushing_fumbles_lost",
    "receiving_fumbles",
    "receiving_fumbles_lost",
    "sack_fumbles",
    "sack_fumbles_lost",
    "fum_rec",
    "fum_rec_yds",
    "fumble_recovery_yards_own",
    "fumble_recovery_yards_opp",
    "fumble_recovery_yards",
    "fum_ret_td",
    "fg_att",
    "fg_made",
    "fg_missed",
    "fg_blocked",
    "fg_yards",
    "fg_long",
    "fg_made_0_19",
    "fg_made_20_29",
    "fg_made_30_39",
    "fg_made_40_49",
    "fg_made_50_59",
    "fg_made_60plus",
    "pat_att",
    "pat_made",
    "pat_missed",
    "pat_blocked",
    "punts",
    "punt_yards",
    "punt_long",
    "punts_blocked",
    "kickoff_returns",
    "kickoff_return_yards",
    "kickoff_return_tds",
    "punt_returns",
    "punt_return_yards",
    "punt_return_tds",
    "special_teams_tds",
    "def_sacks",
    "def_interceptions",
    "def_interception_yards",
    "def_int_ret_td",
    "def_fumbles_forced",
    "def_tackles_solo",
    "def_tackle_assists",
    "def_tackles_with_assist",
    "def_tackles_for_loss",
    "def_pass_defended",
    "def_qb_hits",
    "def_safeties",
    "def_blk_kick",
    "special_teams_tackles_solo",
]

MAX_COLUMNS = {"fg_long", "punt_long"}
SUM_COLUMNS = [col for col in STAT_COLUMNS if col not in MAX_COLUMNS]

# Stathead play-finder PBP sometimes uses a stable gamebook/PFR-ish ID that
# differs from the player bio/PFR page ID.  These are confirmed by player name,
# role family, active years, and team context in the 1978-1998 PBP rollup.
KNOWN_PBP_PFR_ALIASES = [
    ("pfr:ThomHe00", "00-0016269", "Henry Thomas", "DL", 1987, 2000),
    ("pfr:WillKe00", "WillKe00", "Kevin Williams", "WR", 1993, 1998),
    ("pfr:SmitKe26", "00-0015238", "Kevin Smith", "DB", 1992, 1999),
    ("pfr:BrowCh04", "00-0001887", "Chad Brown", "LB", 1993, 2007),
    ("pfr:StewJa00", "00-0015698", "James Stewart", "RB", 1995, 2002),
    ("pfr:JoneKe01", "JoneKe01", "Keith Jones", "RB", 1989, 1992),
    ("pfr:SmitPa01", "SMI570720", "Paul Smith", "DT", 1968, 1980),
    ("pfr:McCoMi21", "MCC639485", "Mike McCoy", "DT", 1970, 1980),
    ("pfr:JohnCh20", "JOH111711", "Charles Johnson", "CB", 1979, 1981),
    ("pfr:BrowRo22", "BRO656880", "Ron Brown", "LB", 1987, 1988),
    ("pfr:WillJa03", "WillJa03", "James Williams", "DB", 1991, 1998),
    ("pfr:BrowCh20", "00-0001886", "Chad Brown", "DE", 1993, 1995),
    ("pfr:BrowLa00", "BRO542261", "Larry Brown", "WR", 1971, 1984),
    ("pfr:ThomJ.01", "ThomJ.01", "J.T. Thomas", "DB", 1973, 1982),
    ("pfr:milledan01", "milledan01", "Dan Miller", "K", 1982, 1982),
    ("pfr:ClarSt21", "ClarSt21", "Steve Clark", "DE", 1981, 1981),
    ("pfr:JohnCh27", "JohnCh27", "Chuck Johnson", "DT", 1993, 1993),
    ("pfr:WillMi20", "WillMi20", "Michael Williams", "DB", 1995, 1995),
]


BASE_COLUMNS = [
    "season",
    "week",
    "season_type",
    "game_id",
    "play_id",
    "desc",
    "posteam",
    "defteam",
    "fumbled_1_team",
    "fumbled_2_team",
    "fumble_recovery_1_team",
    "fumble_recovery_2_team",
    "forced_fumble_player_1_team",
    "forced_fumble_player_2_team",
    "solo_tackle_1_team",
    "solo_tackle_2_team",
    "assist_tackle_1_team",
    "assist_tackle_2_team",
    "assist_tackle_3_team",
    "assist_tackle_4_team",
    "tackle_with_assist_1_team",
    "tackle_with_assist_2_team",
    "return_team",
    "play_type",
    "play",
    "special_teams_play",
    "two_point_attempt",
    "two_point_conv_result",
    "pass_attempt",
    "complete_pass",
    "incomplete_pass",
    "passing_yards",
    "pass_touchdown",
    "interception",
    "sack",
    "rush_attempt",
    "rushing_yards",
    "rush_touchdown",
    "receiving_yards",
    "yards_gained",
    "yardline_100",
    "success",
    "epa",
    "qb_epa",
    "wpa",
    "air_yards",
    "cpoe",
    "touchdown",
    "return_touchdown",
    "fumble",
    "fumble_lost",
    "field_goal_attempt",
    "field_goal_result",
    "kick_distance",
    "extra_point_attempt",
    "extra_point_result",
    "punt_attempt",
    "punt_blocked",
    "punt_fair_catch",
    "punt_out_of_bounds",
    "kickoff_attempt",
    "kickoff_fair_catch",
    "touchback",
    "kickoff_returner_player_id",
    "kickoff_returner_player_name",
    "punt_returner_player_id",
    "punt_returner_player_name",
    "return_yards",
    "passer_player_id",
    "passer_player_name",
    "receiver_player_id",
    "receiver_player_name",
    "rusher_player_id",
    "rusher_player_name",
    "kicker_player_id",
    "kicker_player_name",
    "punter_player_id",
    "punter_player_name",
    "interception_player_id",
    "interception_player_name",
    "fumbled_1_player_id",
    "fumbled_1_player_name",
    "fumbled_2_player_id",
    "fumbled_2_player_name",
    "fumble_recovery_1_player_id",
    "fumble_recovery_1_player_name",
    "fumble_recovery_1_yards",
    "fumble_recovery_2_player_id",
    "fumble_recovery_2_player_name",
    "fumble_recovery_2_yards",
    "forced_fumble_player_1_player_id",
    "forced_fumble_player_1_player_name",
    "forced_fumble_player_2_player_id",
    "forced_fumble_player_2_player_name",
    "solo_tackle_1_player_id",
    "solo_tackle_1_player_name",
    "solo_tackle_2_player_id",
    "solo_tackle_2_player_name",
    "assist_tackle_1_player_id",
    "assist_tackle_1_player_name",
    "assist_tackle_2_player_id",
    "assist_tackle_2_player_name",
    "assist_tackle_3_player_id",
    "assist_tackle_3_player_name",
    "assist_tackle_4_player_id",
    "assist_tackle_4_player_name",
    "tackle_with_assist_1_player_id",
    "tackle_with_assist_1_player_name",
    "tackle_with_assist_2_player_id",
    "tackle_with_assist_2_player_name",
    "tackle_for_loss_1_player_id",
    "tackle_for_loss_1_player_name",
    "tackle_for_loss_2_player_id",
    "tackle_for_loss_2_player_name",
    "pass_defense_1_player_id",
    "pass_defense_1_player_name",
    "pass_defense_2_player_id",
    "pass_defense_2_player_name",
    "qb_hit_1_player_id",
    "qb_hit_1_player_name",
    "qb_hit_2_player_id",
    "qb_hit_2_player_name",
    "sack_player_id",
    "sack_player_name",
    "half_sack_1_player_id",
    "half_sack_1_player_name",
    "half_sack_2_player_id",
    "half_sack_2_player_name",
    "safety_player_id",
    "safety_player_name",
    "blocked_player_id",
    "blocked_player_name",
    "pbp_source_system",
    "player_id_namespace",
]


def lit(path: Path) -> str:
    return str(path).replace("'", "''")


def sql_text(value: object) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def sql_int(value: int | None) -> str:
    return "NULL" if value is None else str(int(value))


def q_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def nz(col: str) -> str:
    return f"COALESCE(CAST({col} AS DOUBLE), 0.0)"


def valid_id(col: str) -> str:
    return f"{col} IS NOT NULL " f"AND TRIM(CAST({col} AS VARCHAR)) <> '' " f"AND TRIM(CAST({col} AS VARCHAR)) <> '0'"


def safe_name(col: str) -> str:
    return f"NULLIF(TRIM(CAST({col} AS VARCHAR)), '')"


def safe_team_expr(expr: str) -> str:
    return f"NULLIF(TRIM(CAST(({expr}) AS VARCHAR)), '')"


def opponent_for_team_expr(team_expr: str) -> str:
    team = safe_team_expr(team_expr)
    return "CASE " f"WHEN {team} = posteam THEN defteam " f"WHEN {team} = defteam THEN posteam " "ELSE NULL " "END"


def punt_return_team_expr() -> str:
    return (
        "CASE "
        "WHEN COALESCE(CAST(play_type AS VARCHAR), '') = 'punt' THEN defteam "
        "ELSE COALESCE(return_team, defteam) "
        "END"
    )


def fumbled_team_expr(idx: int) -> str:
    player_col = f"fumbled_{idx}_player_id"
    team_col = f"fumbled_{idx}_team"
    return (
        "CASE "
        f"WHEN {player_col} = punt_returner_player_id "
        "AND COALESCE(CAST(play_type AS VARCHAR), '') = 'punt' THEN defteam "
        f"WHEN {player_col} = kickoff_returner_player_id "
        "AND COALESCE(CAST(play_type AS VARCHAR), '') = 'kickoff' THEN COALESCE(return_team, posteam, defteam) "
        f"ELSE COALESCE({team_col}, posteam) "
        "END"
    )


def tackle_team_expr(role_team_col: str) -> str:
    return (
        "CASE "
        "WHEN COALESCE(CAST(pbp_source_system AS VARCHAR), '') = 'nflverse' "
        "AND COALESCE(CAST(play_type AS VARCHAR), '') IN ('run', 'pass') "
        "AND COALESCE(CAST(interception AS DOUBLE), 0.0) = 0.0 "
        "AND COALESCE(CAST(fumble_lost AS DOUBLE), 0.0) = 0.0 "
        f"THEN defteam ELSE COALESCE({role_team_col}, defteam) "
        "END"
    )


def boolish(col: str) -> str:
    return f"{nz(col)} = 1"


def official_play() -> str:
    # Stathead's reliable negation marker is '(no play)'. A declined penalty can
    # be a real counted play, so do not strip it just because '(declined)' exists.
    # For nflverse rows, play_type='no_play' is the canonical marker; do not use
    # the generic `play` flag here because valid modern punt returns can have
    # play=0.
    return official_play_sql("")


def not_two_point() -> str:
    return f"{nz('two_point_attempt')} = 0"


def official_pass_attempt() -> str:
    return f"{boolish('pass_attempt')} AND NOT {boolish('sack')} AND {not_two_point()}"


def down_distance_success_num() -> str:
    """Count a successful play using official down/distance thresholds.

    The rate is derived separately as pass_success / pass_success_plays.  This
    count contract is model-free and therefore applies to the historical PBP
    era as well as modern seasons.
    """
    return (
        "CASE WHEN down IS NOT NULL AND ydstogo IS NOT NULL "
        "AND yards_gained IS NOT NULL AND ("
        "(CAST(down AS INTEGER) = 1 AND CAST(yards_gained AS DOUBLE) >= 0.4 * CAST(ydstogo AS DOUBLE)) "
        "OR (CAST(down AS INTEGER) = 2 AND CAST(yards_gained AS DOUBLE) >= 0.6 * CAST(ydstogo AS DOUBLE)) "
        "OR (CAST(down AS INTEGER) IN (3, 4) AND CAST(yards_gained AS DOUBLE) >= CAST(ydstogo AS DOUBLE))"
        ") THEN 1.0 ELSE 0.0 END"
    )


def down_distance_success_den() -> str:
    """Count play rows with enough down/distance data to classify."""
    return (
        "CASE WHEN down IS NOT NULL AND ydstogo IS NOT NULL "
        "AND yards_gained IS NOT NULL AND CAST(down AS INTEGER) BETWEEN 1 AND 4 "
        "THEN 1.0 ELSE 0.0 END"
    )


def epa_success_num() -> str:
    """Legacy EPA-sign success for the non-passing families."""
    return f"CASE WHEN {boolish('success')} THEN 1.0 ELSE 0.0 END"


def epa_success_den() -> str:
    """Legacy EPA success denominator for the non-passing families."""
    return "CASE WHEN success IS NOT NULL THEN 1.0 ELSE 0.0 END"


def thrown_pass() -> str:
    """A pass that left the QB's hand (completion, incompletion, or interception).

    This is nflverse's air-yards universe: `passing_air_yards`/`receiving_air_yards`
    validated exactly against stats_player_week when summed over thrown passes (sacks
    and scrambles carry no air_yards). Includes two-point pass attempts, matching
    nflverse.
    """
    return f"({boolish('complete_pass')} OR {boolish('incomplete_pass')} OR {boolish('interception')})"


def two_point_success() -> str:
    """A successfully converted two-point attempt (nflverse two_point_conv_result)."""
    return "LOWER(COALESCE(CAST(two_point_conv_result AS VARCHAR), '')) = 'success'"


def in_red_zone() -> str:
    """Snap inside the opponent's 20 (yardline_100 = yards to opp end zone, 1-20)."""
    return "yardline_100 IS NOT NULL AND CAST(yardline_100 AS DOUBLE) BETWEEN 1 AND 20"


def real_punt_return() -> str:
    return (
        f"{boolish('punt_attempt')} "
        f"AND {nz('punt_fair_catch')} = 0 "
        f"AND {nz('punt_out_of_bounds')} = 0 "
        f"AND {nz('touchback')} = 0"
    )


def real_kickoff_return() -> str:
    return (
        f"{boolish('kickoff_attempt')} "
        f"AND {nz('kickoff_fair_catch')} = 0 "
        f"AND {nz('touchback')} = 0 "
        "AND LOWER(COALESCE(CAST(\"desc\" AS VARCHAR), '')) NOT LIKE '%no return%' "
        "AND LOWER(COALESCE(CAST(\"desc\" AS VARCHAR), '')) NOT LIKE '%out of bounds%'"
    )


def made_fg_bucket(low: int, high: int | None = None) -> str:
    upper = "" if high is None else f" AND {nz('kick_distance')} <= {high}"
    return (
        f"CASE WHEN {boolish('field_goal_attempt')} "
        f"AND LOWER(COALESCE(field_goal_result, '')) = 'made' "
        f"AND {nz('kick_distance')} >= {low}{upper} THEN 1.0 ELSE 0.0 END"
    )


def role_select(
    role: str,
    id_col: str,
    name_col: str,
    team_expr: str,
    condition: str,
    assignments: dict[str, str],
) -> str:
    missing = set(assignments) - set(STAT_COLUMNS)
    if missing:
        raise ValueError(f"Unknown stat columns for {role}: {sorted(missing)}")

    stat_sql = []
    for col in STAT_COLUMNS:
        expr = assignments.get(col, "0.0")
        stat_sql.append(f"    CAST(({expr}) AS DOUBLE) AS {col}")
    stat_columns_sql = ",\n".join(stat_sql)

    return f"""
SELECT
    CAST({id_col} AS VARCHAR) AS pbp_player_id,
    {safe_name(name_col)} AS pbp_player_name,
    CAST(season AS INTEGER) AS year,
    CAST(week AS INTEGER) AS week,
    COALESCE(NULLIF(TRIM(CAST(season_type AS VARCHAR)), ''), 'REG') AS season_type,
    CAST(game_id AS VARCHAR) AS game_id,
    CAST(play_id AS BIGINT) AS play_id,
    {safe_team_expr(team_expr)} AS nfl_team,
    {opponent_for_team_expr(team_expr)} AS opponent_nfl_team,
    CAST(pbp_source_system AS VARCHAR) AS pbp_source_system,
    CAST(player_id_namespace AS VARCHAR) AS player_id_namespace,
    '{role}' AS event_role,
{stat_columns_sql}
FROM pbp_base
WHERE {condition}
"""


def build_event_sql() -> str:
    counted = official_play()
    pass_play = f"({counted}) AND {not_two_point()} AND ({boolish('pass_attempt')} OR {boolish('complete_pass')} OR {boolish('sack')} OR {boolish('interception')} OR {nz('passing_yards')} <> 0)"
    rush_play = (
        f"({counted}) AND ({boolish('rush_attempt')} OR {boolish('rush_touchdown')} OR {nz('rushing_yards')} <> 0)"
    )
    receive_play = f"({counted}) AND {not_two_point()} AND ({boolish('pass_attempt')} OR {boolish('complete_pass')} OR {boolish('pass_touchdown')} OR {nz('receiving_yards')} <> 0)"
    fg_made = "LOWER(COALESCE(field_goal_result, '')) = 'made'"
    fg_missed = "LOWER(COALESCE(field_goal_result, '')) = 'missed'"
    fg_blocked = "LOWER(COALESCE(field_goal_result, '')) = 'blocked'"
    xp_made = "LOWER(COALESCE(extra_point_result, '')) = 'good'"
    xp_failed = "LOWER(COALESCE(extra_point_result, '')) IN ('failed', 'missed')"
    xp_blocked = "LOWER(COALESCE(extra_point_result, '')) = 'blocked'"

    def fumble_assignments(idx: int) -> dict[str, str]:
        fid = f"fumbled_{idx}_player_id"
        is_rush = f"{boolish('rush_attempt')} AND {fid} = rusher_player_id"
        is_rec = f"{boolish('complete_pass')} AND {fid} = receiver_player_id"
        is_sack = f"{boolish('sack')} AND {fid} = passer_player_id"
        return {
            "fumbles": f"CASE WHEN {idx}=1 AND fumbled_2_player_id IS NULL THEN {fumble_mentions_sql('')} ELSE 1.0 END",
            "fumbles_lost": f"CASE WHEN {boolish('fumble_lost')} THEN 1.0 ELSE 0.0 END",
            "rushing_fumbles": f"CASE WHEN {is_rush} THEN 1.0 ELSE 0.0 END",
            "rushing_fumbles_lost": f"CASE WHEN {is_rush} AND {boolish('fumble_lost')} THEN 1.0 ELSE 0.0 END",
            "receiving_fumbles": f"CASE WHEN {is_rec} THEN 1.0 ELSE 0.0 END",
            "receiving_fumbles_lost": f"CASE WHEN {is_rec} AND {boolish('fumble_lost')} THEN 1.0 ELSE 0.0 END",
            "sack_fumbles": f"CASE WHEN {is_sack} THEN 1.0 ELSE 0.0 END",
            "sack_fumbles_lost": f"CASE WHEN {is_sack} AND {boolish('fumble_lost')} THEN 1.0 ELSE 0.0 END",
        }

    roles: list[tuple[str, str, str, str, str, dict[str, str]]] = [
        (
            "passer",
            "passer_player_id",
            "passer_player_name",
            "posteam",
            f"{valid_id('passer_player_id')} AND {pass_play}",
            {
                "attempts": f"CASE WHEN {official_pass_attempt()} THEN 1.0 ELSE 0.0 END",
                "completions": f"CASE WHEN {boolish('complete_pass')} THEN 1.0 ELSE 0.0 END",
                "passing_yards": nz("passing_yards"),
                "passing_tds": f"CASE WHEN {boolish('pass_touchdown')} THEN 1.0 ELSE 0.0 END",
                "passing_interceptions": f"CASE WHEN {boolish('interception')} THEN 1.0 ELSE 0.0 END",
                "sacks_suffered": f"CASE WHEN {boolish('sack')} THEN 1.0 ELSE 0.0 END",
                "completions_40plus": f"CASE WHEN {boolish('complete_pass')} AND {nz('passing_yards')} >= 40 THEN 1.0 ELSE 0.0 END",
                "completions_50plus": f"CASE WHEN {boolish('complete_pass')} AND {nz('passing_yards')} >= 50 THEN 1.0 ELSE 0.0 END",
                "passing_tds_40plus": f"CASE WHEN {boolish('pass_touchdown')} AND {nz('passing_yards')} >= 40 THEN 1.0 ELSE 0.0 END",
                "passing_tds_50plus": f"CASE WHEN {boolish('pass_touchdown')} AND {nz('passing_yards')} >= 50 THEN 1.0 ELSE 0.0 END",
                "pass_success": down_distance_success_num(),
                "pass_success_plays": down_distance_success_den(),
                "pass_explosive_20": f"CASE WHEN {boolish('complete_pass')} AND {nz('passing_yards')} >= 20 THEN 1.0 ELSE 0.0 END",
                "rz_pass_att": f"CASE WHEN {official_pass_attempt()} AND {in_red_zone()} THEN 1.0 ELSE 0.0 END",
                "rz_pass_td": f"CASE WHEN {boolish('pass_touchdown')} AND {in_red_zone()} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "receiver",
            "receiver_player_id",
            "receiver_player_name",
            "posteam",
            f"{valid_id('receiver_player_id')} AND {receive_play}",
            {
                "targets": f"CASE WHEN {official_pass_attempt()} THEN 1.0 ELSE 0.0 END",
                "receptions": f"CASE WHEN {boolish('complete_pass')} THEN 1.0 ELSE 0.0 END",
                "receiving_yards": nz("receiving_yards"),
                "receiving_tds": f"CASE WHEN {boolish('pass_touchdown')} THEN 1.0 ELSE 0.0 END",
                "receptions_0_4": f"CASE WHEN {boolish('complete_pass')} AND {nz('receiving_yards')} BETWEEN 0 AND 4 THEN 1.0 ELSE 0.0 END",
                "receptions_5_9": f"CASE WHEN {boolish('complete_pass')} AND {nz('receiving_yards')} BETWEEN 5 AND 9 THEN 1.0 ELSE 0.0 END",
                "receptions_10_19": f"CASE WHEN {boolish('complete_pass')} AND {nz('receiving_yards')} BETWEEN 10 AND 19 THEN 1.0 ELSE 0.0 END",
                "receptions_20_29": f"CASE WHEN {boolish('complete_pass')} AND {nz('receiving_yards')} BETWEEN 20 AND 29 THEN 1.0 ELSE 0.0 END",
                "receptions_30_39": f"CASE WHEN {boolish('complete_pass')} AND {nz('receiving_yards')} BETWEEN 30 AND 39 THEN 1.0 ELSE 0.0 END",
                "receptions_40plus": f"CASE WHEN {boolish('complete_pass')} AND {nz('receiving_yards')} >= 40 THEN 1.0 ELSE 0.0 END",
                "receiving_tds_40plus": f"CASE WHEN {boolish('pass_touchdown')} AND {nz('receiving_yards')} >= 40 THEN 1.0 ELSE 0.0 END",
                "receiving_tds_50plus": f"CASE WHEN {boolish('pass_touchdown')} AND {nz('receiving_yards')} >= 50 THEN 1.0 ELSE 0.0 END",
                "rec_success": epa_success_num(),
                "rec_success_plays": epa_success_den(),
                "rec_explosive_20": f"CASE WHEN {boolish('complete_pass')} AND {nz('receiving_yards')} >= 20 THEN 1.0 ELSE 0.0 END",
                "rz_targets": f"CASE WHEN {official_pass_attempt()} AND {in_red_zone()} THEN 1.0 ELSE 0.0 END",
                "rz_rec_td": f"CASE WHEN {boolish('pass_touchdown')} AND {in_red_zone()} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "rusher",
            "rusher_player_id",
            "rusher_player_name",
            "posteam",
            # nflverse carries = official stat_id 10:11 (rush attempts, excl. two-point).
            # Equals rush_attempt with two-point attempts removed (verified 100% vs
            # stats_player_week 2023). Also scopes rushing_yards to non-two-point.
            f"{valid_id('rusher_player_id')} AND {rush_play} AND {not_two_point()}",
            {
                "carries": f"CASE WHEN {boolish('rush_attempt')} THEN 1.0 ELSE 0.0 END",
                "rushing_yards": nz("rushing_yards"),
                "rushing_tds": f"CASE WHEN {boolish('rush_touchdown')} THEN 1.0 ELSE 0.0 END",
                "rush_success": epa_success_num(),
                "rush_success_plays": epa_success_den(),
                # Canonical explosive rush = 20+ yards.  The legacy column
                # name is retained for schema compatibility; its definition
                # is now explicitly 20+, matching NFL.com's `20` bucket.
                "rush_explosive_10": f"CASE WHEN {boolish('rush_attempt')} AND {nz('rushing_yards')} >= 20 THEN 1.0 ELSE 0.0 END",
                "rz_carries": f"CASE WHEN {boolish('rush_attempt')} AND {in_red_zone()} THEN 1.0 ELSE 0.0 END",
                "rz_rush_td": f"CASE WHEN {boolish('rush_touchdown')} AND {in_red_zone()} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "fumbler_1",
            "fumbled_1_player_id",
            "fumbled_1_player_name",
            fumbled_team_expr(1),
            f"{valid_id('fumbled_1_player_id')} AND ({counted}) AND {boolish('fumble')}",
            fumble_assignments(1),
        ),
        (
            "fumbler_2",
            "fumbled_2_player_id",
            "fumbled_2_player_name",
            fumbled_team_expr(2),
            f"{valid_id('fumbled_2_player_id')} AND ({counted}) AND {boolish('fumble')}",
            fumble_assignments(2),
        ),
        (
            "kicker",
            "kicker_player_id",
            "kicker_player_name",
            "posteam",
            f"{valid_id('kicker_player_id')} AND ({counted}) AND ({boolish('field_goal_attempt')} OR {boolish('extra_point_attempt')})",
            {
                "fg_att": f"CASE WHEN {boolish('field_goal_attempt')} THEN 1.0 ELSE 0.0 END",
                "fg_made": f"CASE WHEN {boolish('field_goal_attempt')} AND {fg_made} THEN 1.0 ELSE 0.0 END",
                "fg_missed": f"CASE WHEN {boolish('field_goal_attempt')} AND {fg_missed} THEN 1.0 ELSE 0.0 END",
                "fg_blocked": f"CASE WHEN {boolish('field_goal_attempt')} AND {fg_blocked} THEN 1.0 ELSE 0.0 END",
                "fg_yards": f"CASE WHEN {boolish('field_goal_attempt')} AND {fg_made} THEN {nz('kick_distance')} ELSE 0.0 END",
                "fg_long": f"CASE WHEN {boolish('field_goal_attempt')} AND {fg_made} THEN {nz('kick_distance')} ELSE 0.0 END",
                "fg_made_0_19": made_fg_bucket(0, 19),
                "fg_made_20_29": made_fg_bucket(20, 29),
                "fg_made_30_39": made_fg_bucket(30, 39),
                "fg_made_40_49": made_fg_bucket(40, 49),
                "fg_made_50_59": made_fg_bucket(50, 59),
                "fg_made_60plus": made_fg_bucket(60, None),
                "pat_att": f"CASE WHEN {boolish('extra_point_attempt')} THEN 1.0 ELSE 0.0 END",
                "pat_made": f"CASE WHEN {boolish('extra_point_attempt')} AND {xp_made} THEN 1.0 ELSE 0.0 END",
                "pat_missed": f"CASE WHEN {boolish('extra_point_attempt')} AND {xp_failed} THEN 1.0 ELSE 0.0 END",
                "pat_blocked": f"CASE WHEN {boolish('extra_point_attempt')} AND {xp_blocked} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "punter",
            "punter_player_id",
            "punter_player_name",
            "posteam",
            f"{valid_id('punter_player_id')} AND ({counted}) AND {boolish('punt_attempt')}",
            {
                "punts": "1.0",
                "punt_yards": nz("kick_distance"),
                "punt_long": nz("kick_distance"),
                "punts_blocked": f"CASE WHEN {boolish('punt_blocked')} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "kickoff_returner",
            "kickoff_returner_player_id",
            "kickoff_returner_player_name",
            "COALESCE(return_team, defteam)",
            f"{valid_id('kickoff_returner_player_id')} AND ({counted}) AND {real_kickoff_return()}",
            {
                "kickoff_returns": "1.0",
                "kickoff_return_yards": nz("return_yards"),
                "kickoff_return_tds": f"CASE WHEN {boolish('return_touchdown')} THEN 1.0 ELSE 0.0 END",
                "special_teams_tds": f"CASE WHEN {boolish('return_touchdown')} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "punt_returner",
            "punt_returner_player_id",
            "punt_returner_player_name",
            punt_return_team_expr(),
            f"{valid_id('punt_returner_player_id')} AND ({counted}) AND {real_punt_return()}",
            {
                "punt_returns": "1.0",
                "punt_return_yards": nz("return_yards"),
                "punt_return_tds": f"CASE WHEN {boolish('return_touchdown')} THEN 1.0 ELSE 0.0 END",
                "special_teams_tds": f"CASE WHEN {boolish('return_touchdown')} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "interceptor",
            "interception_player_id",
            "interception_player_name",
            "defteam",
            f"{valid_id('interception_player_id')} AND ({counted}) AND {not_two_point()} AND {boolish('interception')}",
            {
                "def_interceptions": "1.0",
                "def_interception_yards": nz("return_yards"),
                "def_int_ret_td": f"CASE WHEN {boolish('return_touchdown')} THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "sack",
            "sack_player_id",
            "sack_player_name",
            "defteam",
            f"{valid_id('sack_player_id')} AND ({counted}) AND {not_two_point()} AND {boolish('sack')}",
            {"def_sacks": "1.0"},
        ),
        (
            "half_sack_1",
            "half_sack_1_player_id",
            "half_sack_1_player_name",
            "defteam",
            f"{valid_id('half_sack_1_player_id')} AND ({counted}) AND {not_two_point()} AND {boolish('sack')}",
            {"def_sacks": "0.5"},
        ),
        (
            "half_sack_2",
            "half_sack_2_player_id",
            "half_sack_2_player_name",
            "defteam",
            f"{valid_id('half_sack_2_player_id')} AND ({counted}) AND {not_two_point()} AND {boolish('sack')}",
            {"def_sacks": "0.5"},
        ),
        (
            "forced_fumble_1",
            "forced_fumble_player_1_player_id",
            "forced_fumble_player_1_player_name",
            "COALESCE(forced_fumble_player_1_team, defteam)",
            f"{valid_id('forced_fumble_player_1_player_id')} AND ({counted}) AND {boolish('fumble')}",
            {"def_fumbles_forced": "1.0"},
        ),
        (
            "forced_fumble_2",
            "forced_fumble_player_2_player_id",
            "forced_fumble_player_2_player_name",
            "COALESCE(forced_fumble_player_2_team, defteam)",
            f"{valid_id('forced_fumble_player_2_player_id')} AND ({counted}) AND {boolish('fumble')}",
            {"def_fumbles_forced": "1.0"},
        ),
        (
            "fumble_recovery_1",
            "fumble_recovery_1_player_id",
            "fumble_recovery_1_player_name",
            "fumble_recovery_1_team",
            f"{valid_id('fumble_recovery_1_player_id')} AND ({counted})",
            {
                "fum_rec": "1.0",
                "fum_rec_yds": nz("fumble_recovery_1_yards"),
                "fumble_recovery_yards": nz("fumble_recovery_1_yards"),
                "fumble_recovery_yards_own": f"CASE WHEN fumble_recovery_1_team = posteam THEN {nz('fumble_recovery_1_yards')} ELSE 0.0 END",
                "fumble_recovery_yards_opp": f"CASE WHEN fumble_recovery_1_team = defteam THEN {nz('fumble_recovery_1_yards')} ELSE 0.0 END",
                "fum_ret_td": f"CASE WHEN {boolish('return_touchdown')} AND {nz('fumble_recovery_1_yards')} > 0 THEN 1.0 ELSE 0.0 END",
            },
        ),
        (
            "fumble_recovery_2",
            "fumble_recovery_2_player_id",
            "fumble_recovery_2_player_name",
            "fumble_recovery_2_team",
            f"{valid_id('fumble_recovery_2_player_id')} AND ({counted})",
            {
                "fum_rec": "1.0",
                "fum_rec_yds": nz("fumble_recovery_2_yards"),
                "fumble_recovery_yards": nz("fumble_recovery_2_yards"),
                "fumble_recovery_yards_own": f"CASE WHEN fumble_recovery_2_team = posteam THEN {nz('fumble_recovery_2_yards')} ELSE 0.0 END",
                "fumble_recovery_yards_opp": f"CASE WHEN fumble_recovery_2_team = defteam THEN {nz('fumble_recovery_2_yards')} ELSE 0.0 END",
                "fum_ret_td": f"CASE WHEN {boolish('return_touchdown')} AND {nz('fumble_recovery_2_yards')} > 0 THEN 1.0 ELSE 0.0 END",
            },
        ),
    ]

    # Advanced efficiency roles. Kept SEPARATE from the counting roles above because
    # each was validated against nflverse with its OWN exact play-set (see
    # validate_pbp_atoms_vs_nflverse.py): passing_epa needs qb_epa over official plays
    # INCLUDING two-point (excluding two-point drops it to ~89%); receiving_epa is a
    # bare valid-receiver sum (no play filter); air_yards is summed over thrown passes.
    # Folding these into the counting roles would change their filters and break the
    # counting atoms, so they get dedicated single-purpose roles.
    thrown = thrown_pass()
    two_pt = f"{boolish('two_point_attempt')} AND {two_point_success()}"
    roles.extend(
        [
            (
                "passer_epa",
                "passer_player_id",
                "passer_player_name",
                "posteam",
                f"{valid_id('passer_player_id')} AND ({counted})",
                {"passing_epa": nz("qb_epa")},
            ),
            (
                "rusher_epa",
                "rusher_player_id",
                "rusher_player_name",
                "posteam",
                f"{valid_id('rusher_player_id')} AND ({rush_play})",
                {"rushing_epa": nz("epa")},
            ),
            (
                "receiver_epa",
                "receiver_player_id",
                "receiver_player_name",
                "posteam",
                f"{valid_id('receiver_player_id')}",
                {"receiving_epa": nz("epa")},
            ),
            (
                "passer_wpa",
                "passer_player_id",
                "passer_player_name",
                "posteam",
                f"{valid_id('passer_player_id')} AND ({counted})",
                {"passing_wpa": nz("wpa")},
            ),
            (
                "rusher_wpa",
                "rusher_player_id",
                "rusher_player_name",
                "posteam",
                f"{valid_id('rusher_player_id')} AND ({rush_play})",
                {"rushing_wpa": nz("wpa")},
            ),
            (
                "receiver_wpa",
                "receiver_player_id",
                "receiver_player_name",
                "posteam",
                f"{valid_id('receiver_player_id')}",
                {"receiving_wpa": nz("wpa")},
            ),
            (
                "passer_air_yards",
                "passer_player_id",
                "passer_player_name",
                "posteam",
                f"{valid_id('passer_player_id')} AND {thrown}",
                {"passing_air_yards": nz("air_yards")},
            ),
            (
                "receiver_air_yards",
                "receiver_player_id",
                "receiver_player_name",
                "posteam",
                f"{valid_id('receiver_player_id')} AND {thrown}",
                {"receiving_air_yards": nz("air_yards")},
            ),
            (
                "passer_2pt",
                "passer_player_id",
                "passer_player_name",
                "posteam",
                f"{valid_id('passer_player_id')} AND {two_pt}",
                {"passing_2pt_conversions": "1.0"},
            ),
            (
                "rusher_2pt",
                "rusher_player_id",
                "rusher_player_name",
                "posteam",
                f"{valid_id('rusher_player_id')} AND {two_pt}",
                {"rushing_2pt_conversions": "1.0"},
            ),
            (
                "receiver_2pt",
                "receiver_player_id",
                "receiver_player_name",
                "posteam",
                f"{valid_id('receiver_player_id')} AND {two_pt}",
                {"receiving_2pt_conversions": "1.0"},
            ),
            (
                "passer_cpoe",
                "passer_player_id",
                "passer_player_name",
                "posteam",
                f"{valid_id('passer_player_id')} AND cpoe IS NOT NULL",
                {"passing_cpoe_sum": nz("cpoe"), "passing_cpoe_n": "1.0"},
            ),
        ]
    )

    for idx in (1, 2):
        roles.append(
            (
                f"solo_tackle_{idx}",
                f"solo_tackle_{idx}_player_id",
                f"solo_tackle_{idx}_player_name",
                tackle_team_expr(f"solo_tackle_{idx}_team"),
                f"{valid_id(f'solo_tackle_{idx}_player_id')} AND ({counted})",
                {
                    "def_tackles_solo": "1.0",
                    "special_teams_tackles_solo": f"CASE WHEN {boolish('special_teams_play')} THEN 1.0 ELSE 0.0 END",
                },
            )
        )

    for idx in (1, 2, 3, 4):
        roles.append(
            (
                f"assist_tackle_{idx}",
                f"assist_tackle_{idx}_player_id",
                f"assist_tackle_{idx}_player_name",
                tackle_team_expr(f"assist_tackle_{idx}_team"),
                f"{valid_id(f'assist_tackle_{idx}_player_id')} AND ({counted})",
                {"def_tackle_assists": "1.0"},
            )
        )

    for idx in (1, 2):
        roles.extend(
            [
                (
                    f"tackle_with_assist_{idx}",
                    f"tackle_with_assist_{idx}_player_id",
                    f"tackle_with_assist_{idx}_player_name",
                    tackle_team_expr(f"tackle_with_assist_{idx}_team"),
                    f"{valid_id(f'tackle_with_assist_{idx}_player_id')} AND ({counted})",
                    {"def_tackles_with_assist": "1.0"},
                ),
                (
                    f"tackle_for_loss_{idx}",
                    f"tackle_for_loss_{idx}_player_id",
                    f"tackle_for_loss_{idx}_player_name",
                    "defteam",
                    f"{valid_id(f'tackle_for_loss_{idx}_player_id')} AND ({counted})",
                    {"def_tackles_for_loss": "1.0"},
                ),
                (
                    f"pass_defense_{idx}",
                    f"pass_defense_{idx}_player_id",
                    f"pass_defense_{idx}_player_name",
                    "defteam",
                    f"{valid_id(f'pass_defense_{idx}_player_id')} AND ({counted})",
                    {"def_pass_defended": "1.0"},
                ),
                (
                    f"qb_hit_{idx}",
                    f"qb_hit_{idx}_player_id",
                    f"qb_hit_{idx}_player_name",
                    "defteam",
                    f"{valid_id(f'qb_hit_{idx}_player_id')} AND ({counted})",
                    {"def_qb_hits": "1.0"},
                ),
            ]
        )

    roles.extend(
        [
            (
                "safety",
                "safety_player_id",
                "safety_player_name",
                "defteam",
                f"{valid_id('safety_player_id')} AND ({counted})",
                {"def_safeties": "1.0"},
            ),
            (
                "blocked_kick",
                "blocked_player_id",
                "blocked_player_name",
                "defteam",
                f"{valid_id('blocked_player_id')} AND ({counted}) AND ({boolish('field_goal_attempt')} OR {boolish('extra_point_attempt')} OR {boolish('punt_attempt')})",
                {"def_blk_kick": "1.0"},
            ),
        ]
    )

    return "\nUNION ALL\n".join(
        role_select(role, id_col, name_col, team_expr, condition, assignments)
        for role, id_col, name_col, team_expr, condition, assignments in roles
    )


def create_bio_lookup(con: duckdb.DuckDBPyConnection, bio_path: Path) -> None:
    manual_alias_sql = "\nUNION ALL\n".join(
        "SELECT "
        f"{sql_text(lookup_id)} AS lookup_id, "
        f"{sql_text(nfl_player_id)} AS NFL_player_id, "
        f"{sql_text(player)} AS player, "
        f"{sql_text(position)} AS nfl_position, "
        f"{sql_int(first_year)} AS first_year, "
        f"{sql_int(last_year)} AS last_year"
        for lookup_id, nfl_player_id, player, position, first_year, last_year in KNOWN_PBP_PFR_ALIASES
    )
    con.execute(
        f"""
        CREATE TEMP TABLE bio_source AS
        SELECT
            CAST(NFL_player_id AS VARCHAR) AS NFL_player_id,
            NULLIF(TRIM(CAST(player AS VARCHAR)), '') AS player,
            NULLIF(TRIM(CAST(nfl_position AS VARCHAR)), '') AS nfl_position,
            NULLIF(TRIM(CAST(pfr_id AS VARCHAR)), '') AS pfr_id,
            TRY_CAST(first_year AS INTEGER) AS first_year,
            TRY_CAST(last_year AS INTEGER) AS last_year
        FROM read_parquet('{lit(bio_path)}')
        WHERE NFL_player_id IS NOT NULL
          AND TRIM(CAST(NFL_player_id AS VARCHAR)) <> ''
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE bio_lookup AS
        WITH manual_aliases AS (
            {manual_alias_sql}
        ),
        candidates AS (
            SELECT
                lookup_id,
                NFL_player_id,
                player,
                nfl_position,
                first_year,
                last_year,
                0 AS priority
            FROM manual_aliases
            UNION ALL
            SELECT
                NFL_player_id AS lookup_id,
                NFL_player_id,
                player,
                nfl_position,
                first_year,
                last_year,
                1 AS priority
            FROM bio_source
            UNION ALL
            SELECT
                pfr_id AS lookup_id,
                NFL_player_id,
                player,
                nfl_position,
                first_year,
                last_year,
                2 AS priority
            FROM bio_source
            WHERE pfr_id IS NOT NULL
            UNION ALL
            SELECT
                'pfr:' || pfr_id AS lookup_id,
                NFL_player_id,
                player,
                nfl_position,
                first_year,
                last_year,
                3 AS priority
            FROM bio_source
            WHERE pfr_id IS NOT NULL
            UNION ALL
            SELECT
                'pfr:' || NFL_player_id AS lookup_id,
                NFL_player_id,
                player,
                nfl_position,
                first_year,
                last_year,
                4 AS priority
            FROM bio_source
            WHERE NFL_player_id IS NOT NULL
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY lookup_id
                    ORDER BY
                        priority,
                        COALESCE(last_year, 9999) DESC,
                        COALESCE(first_year, 0) DESC,
                        NFL_player_id
                ) AS rn,
                COUNT(*) OVER (PARTITION BY lookup_id) AS candidate_count
            FROM candidates
            WHERE lookup_id IS NOT NULL
              AND TRIM(CAST(lookup_id AS VARCHAR)) <> ''
        )
        SELECT
            lookup_id,
            NFL_player_id,
            player,
            nfl_position,
            first_year,
            last_year,
            priority,
            candidate_count
        FROM ranked
        WHERE rn = 1
        """
    )


def create_pbp_base(
    con: duckdb.DuckDBPyConnection,
    pbp_path: Path,
    season_min: int | None,
    season_max: int | None,
) -> None:
    schema_cols = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{lit(pbp_path)}')").fetchall()}
    missing = sorted(set(BASE_COLUMNS) - schema_cols)
    if missing:
        raise RuntimeError(f"PBP file is missing expected columns: {missing}")

    filters = ["season IS NOT NULL", "week IS NOT NULL"]
    if season_min is not None:
        filters.append(f"season >= {int(season_min)}")
    if season_max is not None:
        filters.append(f"season <= {int(season_max)}")

    con.execute(
        f"""
        CREATE TEMP TABLE pbp_base AS
        SELECT
            {", ".join(q_ident(col) for col in BASE_COLUMNS)}
        FROM read_parquet('{lit(pbp_path)}')
        WHERE {" AND ".join(filters)}
        """
    )


def create_weekly_rollup(con: duckdb.DuckDBPyConnection) -> None:
    event_sql = build_event_sql()
    sum_exprs = [f"SUM({col}) AS {col}" for col in SUM_COLUMNS]
    max_exprs = [f"MAX({col}) AS {col}" for col in MAX_COLUMNS]
    nonzero_expr = " + ".join([f"ABS(COALESCE({col}, 0.0))" for col in STAT_COLUMNS])

    con.execute(
        f"""
        CREATE TEMP TABLE pbp_player_events AS
        {event_sql}
        """
    )
    con.execute(
        f"""
        CREATE TEMP TABLE pbp_player_week_rollup AS
        WITH mapped AS (
            SELECT
                e.*,
                bl.NFL_player_id AS mapped_NFL_player_id,
                bl.player AS mapped_player,
                bl.nfl_position AS mapped_position,
                bl.first_year,
                bl.last_year,
                bl.candidate_count,
                regexp_replace(e.pbp_player_id, '^pfr:', '') AS pbp_player_id_clean
            FROM pbp_player_events e
            LEFT JOIN bio_lookup bl
              ON e.pbp_player_id = bl.lookup_id
        ),
        grouped AS (
            SELECT
                COALESCE(mapped_NFL_player_id, CASE WHEN player_id_namespace = 'nflverse' THEN pbp_player_id ELSE pbp_player_id_clean END) AS NFL_player_id,
                pbp_player_id,
                pbp_player_id_clean,
                COALESCE(ANY_VALUE(mapped_player), ANY_VALUE(pbp_player_name)) AS player,
                ANY_VALUE(pbp_player_name) AS pbp_player_name,
                ANY_VALUE(mapped_position) AS position,
                CASE WHEN COUNT(DISTINCT nfl_team) = 1 THEN MIN(nfl_team) ELSE NULL END AS nfl_team,
                CASE WHEN COUNT(DISTINCT opponent_nfl_team) = 1 THEN MIN(opponent_nfl_team) ELSE NULL END AS opponent_nfl_team,
                COUNT(DISTINCT nfl_team) AS nfl_team_context_count,
                COUNT(DISTINCT opponent_nfl_team) AS opponent_context_count,
                year,
                week,
                season_type,
                COUNT(DISTINCT game_id) AS games,
                COUNT(*) AS event_rows,
                STRING_AGG(DISTINCT event_role, ';' ORDER BY event_role) AS event_roles,
                STRING_AGG(DISTINCT pbp_source_system, ';' ORDER BY pbp_source_system) AS pbp_source_systems,
                STRING_AGG(DISTINCT player_id_namespace, ';' ORDER BY player_id_namespace) AS player_id_namespaces,
                CASE WHEN MAX(CASE WHEN mapped_NFL_player_id IS NOT NULL THEN 1 ELSE 0 END) = 1 THEN 1 ELSE 0 END AS mapped_to_player_bio,
                MAX(COALESCE(candidate_count, 0)) AS bio_lookup_candidate_count,
                {", ".join(sum_exprs + max_exprs)}
            FROM mapped
            GROUP BY
                COALESCE(mapped_NFL_player_id, CASE WHEN player_id_namespace = 'nflverse' THEN pbp_player_id ELSE pbp_player_id_clean END),
                pbp_player_id,
                pbp_player_id_clean,
                year,
                week,
                season_type
        )
        SELECT
            NFL_player_id,
            CASE
                WHEN NFL_player_id IS NOT NULL THEN NFL_player_id || '_' || year || '_' || week
                ELSE NULL
            END AS player_week,
            pbp_player_id,
            pbp_player_id_clean,
            player,
            pbp_player_name,
            position,
            nfl_team,
            opponent_nfl_team,
            nfl_team_context_count,
            opponent_context_count,
            year,
            week,
            season_type,
            games,
            event_rows,
            event_roles,
            pbp_source_systems,
            player_id_namespaces,
            mapped_to_player_bio,
            bio_lookup_candidate_count,
            {", ".join(STAT_COLUMNS)}
        FROM grouped
        WHERE ({nonzero_expr}) <> 0.0
        """
    )


def create_aggregate_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    group_cols: list[str],
    regular_only: bool,
) -> None:
    filters = []
    if regular_only:
        filters.append("season_type = 'REG'")
    where = "WHERE " + " AND ".join(filters) if filters else ""
    sum_exprs = [f"SUM({col}) AS {col}" for col in SUM_COLUMNS]
    max_exprs = [f"MAX({col}) AS {col}" for col in MAX_COLUMNS]
    group_sql = ", ".join(group_cols)

    con.execute(
        f"""
        CREATE TEMP TABLE {table_name} AS
        SELECT
            {group_sql},
            ANY_VALUE(player) AS player,
            ANY_VALUE(position) AS position,
            SUM(games) AS games,
            SUM(event_rows) AS event_rows,
            COUNT(*) AS player_week_rows,
            STRING_AGG(DISTINCT event_roles, ';' ORDER BY event_roles) AS event_roles,
            STRING_AGG(DISTINCT pbp_source_systems, ';' ORDER BY pbp_source_systems) AS pbp_source_systems,
            STRING_AGG(DISTINCT player_id_namespaces, ';' ORDER BY player_id_namespaces) AS player_id_namespaces,
            MAX(mapped_to_player_bio) AS mapped_to_player_bio,
            MAX(bio_lookup_candidate_count) AS bio_lookup_candidate_count,
            {", ".join(sum_exprs + max_exprs)}
        FROM pbp_player_week_rollup
        {where}
        GROUP BY {group_sql}
        """
    )


def export_table(con: duckdb.DuckDBPyConnection, table: str, path: Path) -> int:
    con.execute(
        f"""
        COPY (SELECT * FROM {table})
        TO '{lit(path)}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    return int(con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def export_csv(con: duckdb.DuckDBPyConnection, query: str, path: Path) -> int:
    con.execute(
        f"""
        COPY ({query})
        TO '{lit(path)}'
        (HEADER, DELIMITER ',')
        """
    )
    return int(con.execute(f"SELECT COUNT(*) FROM ({query}) q").fetchone()[0])


def df_to_markdown(df: Any) -> str:
    if df.empty:
        return "_No rows._"
    columns = [str(col) for col in df.columns]
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for row in df.itertuples(index=False, name=None):
        values = ["" if value is None else str(value) for value in row]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def write_summary(con: duckdb.DuckDBPyConnection, output_dir: Path, manifest: dict[str, Any]) -> None:
    summary = con.execute(
        """
        SELECT
            year,
            season_type,
            COUNT(*) AS player_week_rows,
            COUNT(DISTINCT NFL_player_id) AS players,
            SUM(CASE WHEN mapped_to_player_bio = 0 THEN 1 ELSE 0 END) AS unmapped_player_week_rows,
            SUM(attempts) AS attempts,
            SUM(carries) AS carries,
            SUM(targets) AS targets,
            SUM(fg_att) AS fg_att,
            SUM(punts) AS punts,
            SUM(def_tackles_solo) AS def_tackles_solo,
            SUM(def_sacks) AS def_sacks,
            SUM(def_interceptions) AS def_interceptions
        FROM pbp_player_week_rollup
        GROUP BY year, season_type
        ORDER BY year, season_type
        """
    ).fetchdf()

    by_source = con.execute(
        """
        SELECT
            pbp_source_systems,
            player_id_namespaces,
            MIN(year) AS min_year,
            MAX(year) AS max_year,
            COUNT(*) AS player_week_rows,
            COUNT(DISTINCT NFL_player_id) AS players,
            SUM(CASE WHEN mapped_to_player_bio = 0 THEN 1 ELSE 0 END) AS unmapped_player_week_rows
        FROM pbp_player_week_rollup
        GROUP BY pbp_source_systems, player_id_namespaces
        ORDER BY min_year, pbp_source_systems
        """
    ).fetchdf()

    lines = [
        "# PBP Supertable Audit Rollups",
        "",
        f"Generated: {manifest['generated_at_utc']}",
        f"Input PBP: `{manifest['input_pbp']}`",
        f"Input bio: `{manifest['input_bio']}`",
        "",
        "## Output Rows",
        "",
    ]
    for name, meta in manifest["outputs"].items():
        lines.append(f"- `{name}`: {meta['rows']:,} rows")
    lines.extend(
        [
            "",
            "## Coverage By Source",
            "",
            df_to_markdown(by_source),
            "",
            "## Yearly Coverage",
            "",
            df_to_markdown(summary),
            "",
        ]
    )
    (output_dir / "PBP_SUPERTABLE_AUDIT_ROLLUPS.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pbp", type=Path, default=DEFAULT_PBP)
    parser.add_argument("--bio", type=Path, default=None)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--season-min", type=int, default=None)
    parser.add_argument("--season-max", type=int, default=None)
    parser.add_argument(
        "--duckdb-temp-dir",
        type=Path,
        default=None,
        help="Optional directory for DuckDB spill/temp files during large PBP aggregation.",
    )
    parser.add_argument(
        "--duckdb-threads",
        type=int,
        default=8,
        help="DuckDB worker threads to use for aggregation.",
    )
    parser.add_argument("--force", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    pbp_path = args.pbp
    if args.bio:
        bio_path = args.bio
    else:
        bio_path = DEFAULT_BIO_REPAIRED if DEFAULT_BIO_REPAIRED.exists() else DEFAULT_BIO

    if not pbp_path.exists():
        raise FileNotFoundError(f"PBP file not found: {pbp_path}")
    if not bio_path.exists():
        raise FileNotFoundError(f"Bio file not found: {bio_path}")

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    expected_outputs = [
        output_dir / "pbp_player_week_rollup.parquet",
        output_dir / "pbp_player_nfl_season.parquet",
        output_dir / "pbp_player_nfl_season_all.parquet",
        output_dir / "pbp_player_nfl_career.parquet",
        output_dir / "pbp_player_nfl_career_all.parquet",
    ]
    if not args.force:
        existing = [path for path in expected_outputs if path.exists()]
        if existing:
            raise FileExistsError(
                "Output files already exist. Use --force to overwrite: " + ", ".join(str(path) for path in existing)
            )

    con = duckdb.connect()
    if args.duckdb_temp_dir:
        args.duckdb_temp_dir.mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{lit(args.duckdb_temp_dir)}'")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"PRAGMA threads={max(1, int(args.duckdb_threads))}")

    create_bio_lookup(con, bio_path)
    create_pbp_base(con, pbp_path, args.season_min, args.season_max)
    create_weekly_rollup(con)
    create_aggregate_table(
        con,
        "pbp_player_nfl_season",
        ["NFL_player_id", "year"],
        regular_only=True,
    )
    create_aggregate_table(
        con,
        "pbp_player_nfl_season_all",
        ["NFL_player_id", "year"],
        regular_only=False,
    )
    create_aggregate_table(
        con,
        "pbp_player_nfl_career",
        ["NFL_player_id"],
        regular_only=True,
    )
    create_aggregate_table(
        con,
        "pbp_player_nfl_career_all",
        ["NFL_player_id"],
        regular_only=False,
    )

    output_specs = {
        "pbp_player_week_rollup.parquet": "pbp_player_week_rollup",
        "pbp_player_nfl_season.parquet": "pbp_player_nfl_season",
        "pbp_player_nfl_season_all.parquet": "pbp_player_nfl_season_all",
        "pbp_player_nfl_career.parquet": "pbp_player_nfl_career",
        "pbp_player_nfl_career_all.parquet": "pbp_player_nfl_career_all",
    }

    manifest: dict[str, Any] = {
        "generated_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "input_pbp": str(pbp_path),
        "input_bio": str(bio_path),
        "season_min": args.season_min,
        "season_max": args.season_max,
        "outputs": {},
    }

    for filename, table in output_specs.items():
        path = output_dir / filename
        rows = export_table(con, table, path)
        manifest["outputs"][filename] = {
            "path": str(path),
            "rows": rows,
            "size_bytes": path.stat().st_size,
        }

    export_csv(
        con,
        """
        SELECT
            pbp_player_id,
            pbp_player_id_clean,
            pbp_player_name,
            player_id_namespaces,
            pbp_source_systems,
            MIN(year) AS min_year,
            MAX(year) AS max_year,
            COUNT(*) AS player_week_rows,
            SUM(event_rows) AS event_rows,
            STRING_AGG(DISTINCT event_roles, ';' ORDER BY event_roles) AS event_roles
        FROM pbp_player_week_rollup
        WHERE mapped_to_player_bio = 0
        GROUP BY
            pbp_player_id,
            pbp_player_id_clean,
            pbp_player_name,
            player_id_namespaces,
            pbp_source_systems
        ORDER BY event_rows DESC, min_year, pbp_player_name
        """,
        output_dir / "unmapped_pbp_players.csv",
    )
    export_csv(
        con,
        """
        SELECT
            year,
            season_type,
            COUNT(*) AS player_week_rows,
            COUNT(DISTINCT NFL_player_id) AS players,
            SUM(CASE WHEN mapped_to_player_bio = 0 THEN 1 ELSE 0 END) AS unmapped_player_week_rows,
            SUM(attempts) AS attempts,
            SUM(carries) AS carries,
            SUM(targets) AS targets,
            SUM(fg_att) AS fg_att,
            SUM(punts) AS punts,
            SUM(def_tackles_solo) AS def_tackles_solo,
            SUM(def_sacks) AS def_sacks,
            SUM(def_interceptions) AS def_interceptions
        FROM pbp_player_week_rollup
        GROUP BY year, season_type
        ORDER BY year, season_type
        """,
        output_dir / "player_week_coverage_by_year.csv",
    )
    export_csv(
        con,
        """
        SELECT
            event_roles,
            COUNT(*) AS player_week_rows,
            MIN(year) AS min_year,
            MAX(year) AS max_year
        FROM pbp_player_week_rollup
        GROUP BY event_roles
        ORDER BY player_week_rows DESC
        """,
        output_dir / "player_week_role_combinations.csv",
    )

    (output_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    write_summary(con, output_dir, manifest)

    print(json.dumps(manifest, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
