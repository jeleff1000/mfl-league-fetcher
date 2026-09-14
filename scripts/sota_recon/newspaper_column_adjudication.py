"""The newspaper LONG stat_name block: 629 open concepts, argued one at a time.

THE METHOD, in Joe's words: "you have a table of passing columns. ok so what passing
columns are in the super table? are these the same thing? yes. move on."

That is the whole procedure and it applies cleanly here for one reason: THESE STRINGS ARE
OURS. `newspaper_player_cells.stat_name` is not a publisher's vocabulary being transcribed
-- it is what our own OCR/extraction pipeline emitted, so the authorship rule governs and
`completed_forward_passes` means completed forward passes. There is no `td`-is-Touchdown-
Percentage trap here, because nobody else wrote these names.

WHAT MAKES THIS BLOCK BIG RATHER THAN HARD. 456 distinct names, and most are not stats:

  NARRATIVE      `long_run_note`, `notable_play`, `defensive_standout_note`,
                 `scoring_drive_setup`, `drive_setup_play`
  STANDINGS      ~30 `division_*` / `*_streak` / `*_record_after_game` -- league context
                 the newspaper reported, not a measurement of the game
  PROVENANCE     `evidence_text`, `confidence_score`, `identity_*`, `pilot_*`, `source_*`,
                 `review_status`, `strict_hold_reason` -- our own extraction bookkeeping
  ONE-OFFS       `boston_first_downs_from_scrimmage`,
                 `passing_yards_on_single_completion_to_karr`,
                 `first_score_against_detroit_lions_1934` -- a single article's phrasing,
                 minted as a stat_name by the extractor

Only the first group below is mapping work; the rest is excluded with a reason, and the
one-offs are excluded as EXTRACTION ARTEFACTS rather than as concepts we declined.

SYNONYM COLLAPSE IS THE POINT. The same measurement arrives under many 1920s-newspaper
spellings -- `completed_passes` / `completed_forward_passes` / `forward_pass_completions` /
`forward_passes_completed` / `pass_completions` / `passes_completed` are one canonical. That
is not sloppiness in the extractor; it is the era's prose, and collapsing it is exactly what
the supertable is for.

Run:  python -m scripts.sota_recon.newspaper_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json
import re

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger

GENERATOR = "scripts.sota_recon.newspaper_column_adjudication"

# ---------------------------------------------------------------------------
# stat_name -> canonical. One line per concept; synonyms share a target.
# ---------------------------------------------------------------------------
MAPPED: dict[str, str] = {
    # ---- passing ---------------------------------------------------------------------
    "completed_passes": "completions",
    "completed_forward_passes": "completions",
    "forward_pass_completions": "completions",
    "forward_passes_completed": "completions",
    "pass_completions": "completions",
    "passes_completed": "completions",
    "forward_pass_attempts": "attempts",
    "forward_passes_attempted": "attempts",
    "pass_attempts": "attempts",
    "passes_attempted": "attempts",
    "team_pass_attempts": "attempts",
    "passing_yards": "passing_yards",
    "forward_passing_yards": "passing_yards",
    "net_yards_from_forward_passes": "passing_yards",
    "yards_gained_from_passes": "passing_yards",
    "pass_completion_yards": "passing_yards",
    "passing_touchdowns": "passing_tds",
    "interceptions_thrown": "passing_interceptions",
    "passes_intercepted": "passing_interceptions",
    # ---- rushing ---------------------------------------------------------------------
    "rushing_yards": "rushing_yards",
    "rushing_net_yards": "rushing_yards",
    "rushing_yards_from_scrimmage": "rushing_yards",
    "net_yards_gained_carrying_ball": "rushing_yards",
    "total_yards_gained_carrying_ball": "rushing_yards",
    "rushing_touchdowns": "rushing_tds",
    "long_rush": "rushing_long",
    "long_run": "rushing_long",
    "long_rushing_gain_yards": "rushing_long",
    # ---- receiving -------------------------------------------------------------------
    # NOT mapped: `season_receptions` / `team_season_pass_receptions_total` are
    # SEASON-TO-DATE totals the article quoted, not this game's -- caught by the
    # standings rule, and the validator refused them when they were listed here too.
    "long_reception": "receiving_long",
    # ---- defense / takeaways ----------------------------------------------------------
    # `interception`, `interceptions` and `defensive_interceptions` are the DEFENSIVE side:
    # the passer's side arrives as `interceptions_thrown` / `passes_intercepted`, which is
    # what makes the split safe here rather than a guess.
    "defensive_interceptions": "def_interceptions",
    "interception": "def_interceptions",
    "interceptions": "def_interceptions",
    "opponent_passes_intercepted": "def_interceptions",
    "interception_return_yards": "def_interception_yards",
    "yards_gained_from_intercepted_passes": "def_interception_yards",
    "interception_return_touchdowns": "def_int_ret_td",
    "safeties": "def_safeties",
    "safeties_scored": "def_safeties",
    # ---- fumbles ----------------------------------------------------------------------
    "fumbles": "fumbles",
    "own_fumble_recoveries": "fumble_recovery_own",
    "opponents_fumbles_recovered": "fumble_recovery_opp",
    "fumble_recovery_touchdowns": "fum_ret_td",
    "fumble_return_touchdowns": "fum_ret_td",
    # ---- kicking ----------------------------------------------------------------------
    "field_goals_made": "fg_made",
    "field_goals": "fg_made",
    "team_field_goals": "fg_made",
    "team_field_goals_made": "fg_made",
    "drop_kick_field_goals": "fg_made",
    "field_goals_missed": "fg_missed",
    "missed_field_goals": "fg_missed",
    "field_goal_misses": "fg_missed",
    "missed_field_goal_attempts": "fg_missed",
    "blocked_field_goal_attempts": "fg_blocked",
    "extra_points_made": "pat_made",
    "extra_points": "pat_made",
    "goals_after_touchdown": "pat_made",
    "goals_from_touchdown": "pat_made",
    "successful_tries_for_point_after": "pat_made",
    "extra_points_missed": "pat_missed",
    "missed_extra_points": "pat_missed",
    "failed_extra_points": "pat_missed",
    "blocked_extra_point": "pat_blocked",
    "blocked_extra_point_attempts": "pat_blocked",
    # ---- punting ----------------------------------------------------------------------
    "punts": "punts",
    "punt_count": "punts",
    "number_of_punts": "punts",
    "punt_yards": "punt_yards",
    "punt_distance_yards": "punt_yards",
    "punt_average": "punt_yards_per_punt",
    "punt_average_yards": "punt_yards_per_punt",
    "average_length_of_punts": "punt_yards_per_punt",
    "team_game_stat.punt_average_yards": "punt_yards_per_punt",
    "blocked_punts": "punts_blocked",
    "blocked_punt": "punts_blocked",
    # ---- returns ----------------------------------------------------------------------
    "punt_returns": "punt_returns",
    "number_of_punt_returns": "punt_returns",
    "punt_return_yards": "punt_return_yards",
    "yardage_of_punt_returns": "punt_return_yards",
    "punt_return_touchdowns": "punt_return_tds",
    "number_of_kickoff_returns": "kickoff_returns",
    "kickoff_return_yards": "kickoff_return_yards",
    "yardage_of_kickoff_returns": "kickoff_return_yards",
    "total_kick_return_yards": "kickoff_return_yards",
    "kickoff_return_touchdowns": "kickoff_return_tds",
    # ---- team / general ----------------------------------------------------------------
    "penalties": "penalties",
    "number_of_penalties_against": "penalties",
    "penalty_yards": "penalty_yards",
    "penalty_yards_lost": "penalty_yards",
    "yards_lost_from_penalties": "penalty_yards",
    "scrimmage_yards": "scrimmage_yards",
    "yards_from_scrimmage": "scrimmage_yards",
    "net_yards_from_scrimmage": "scrimmage_yards",
    "yards_gained_from_scrimmage": "scrimmage_yards",
    "touchdowns": "total_tds_accounted_for",
    "touchdowns_scored": "total_tds_accounted_for",
    "touchdowns_total": "total_tds_accounted_for",
    "touchdowns_in_game": "total_tds_accounted_for",
    "team_touchdowns": "total_tds_accounted_for",
    "points": "total_points_scored",
    "points_scored": "total_points_scored",
    "total_points": "total_points_scored",
    "scoring_points": "total_points_scored",
}

# Real material the supertable has no column for. §24.4 census + demand join.
NEW_CANDIDATES: dict[str, str] = {
    # ---- the team box-score line, 2026-07-29 ---------------------------------------
    # Measured against the live release, not assumed: NO v26 column contains
    # 'total_yards', 'total_offense' or 'yards_gained', and the only 'first_down'
    # columns are the three PLAYER-grain ones (passing/rushing/receiving). The
    # supertable was built from player-grain sources and never took the team box-score
    # line, so these are a real gap rather than a spelling we already carry.
    "total_yards": "TEAM total yards. No v26 column holds it",
    "total_yards_gained": "TEAM total yards, another era spelling of the same cell",
    "total_offense_yards": "TEAM total yards, another era spelling of the same cell",
    "yards_gained": "TEAM total yards, another era spelling of the same cell",
    "net_yards": "TEAM net yards -- gross minus yards lost. No v26 column holds it; "
                 "the two `net_yards` matches in the release are passing RATES",
    "net_yards_from_passes_and_scrimmage": "TEAM net yards, era spelling",
    "total_first_downs": "TEAM first downs. v26 carries passing/rushing/receiving "
                         "first downs at PLAYER grain and no team total",
    "first_downs_total": "TEAM first downs, era spelling",
    "yards_lost_from_scrimmage": "TEAM yards lost. v26 carries sack_yards_lost at "
                                 "player grain and nothing at team grain",
    "yards_lost_after_forward_passes": "TEAM yards lost, era spelling",
    "yards_lost_from_scrimmage_passes_penalties": "TEAM yards lost, era spelling",
    "fumbles_recovered": "TEAM fumble recoveries. v26 splits own/opp at player grain "
                         "and carries no team total",
    "average_length_of_kickoff_returns": "kickoff return AVERAGE. v26 carries no "
                                         "return-average column in any family -- the "
                                         "legacy pass found the same gap and had to "
                                         "confirm y/ret against its operands instead",
    "average_length_of_punt_returns": "punt return AVERAGE, mirroring the kickoff one",

    "first_downs": "team first downs. v26 carries passing_/rushing_/receiving_first_downs "
                   "per player but no TEAM total, and the 1920s box score reports the team "
                   "number as its headline efficiency stat",
    "first_downs_from_scrimmage": "team first downs by rush, the era's own split",
    "first_downs_from_passes": "team first downs by pass, the era's own split",
    "first_downs_from_penalties": "team first downs awarded by penalty -- v26 has no "
                                  "penalty-first-down concept at all",
    "number_of_kickoffs": "kickoffs taken. v26 carries kickoff RETURN columns but nothing "
                          "for the kicking side; proposed independently by nflcom and "
                          "StatsCrew, which is what a real schema gap looks like",
    "yardage_of_kickoffs": "kickoff yards -- the kicking side of the kickoff",
    "scrimmage_plays": "offensive plays run. No v26 column, and it is the denominator "
                       "every per-play rate in the program currently lacks",
    "unrecovered_fumbles": "fumbles that went out of bounds or were not recovered by "
                           "either side. v26 splits own/opp recoveries and has no residual, "
                           "so a fumble total cannot currently be reconciled",
    "forward_passes_incomplete": "incompletions as a REPORTED number. Derivable as "
                                 "attempts - completions where both are present, but the "
                                 "newspaper frequently prints incompletions WITHOUT one of "
                                 "the operands, so it is the only form the value takes on "
                                 "those rows",
    "drop_kick_long_yards": "longest DROP KICK. An era-specific concept v26 has no column "
                            "for; the drop kick was legal and common pre-1934",
    "minutes_played": "minutes played, the era's own participation measure -- v26 has snap "
                      "counts (1999+) and nothing for the sixty-minute era",
    "safeties_forced": "safeties forced BY this team's defense, as distinct from "
                       "def_safeties scored. The newspaper reports both sides",
}

# ---------------------------------------------------------------------------
# EXCLUDED, by kind. Each kind is one argument, applied to every member.
# ---------------------------------------------------------------------------
_NARRATIVE = (
    "NARRATIVE PROSE, not a measurement. The extractor minted a stat_name from a sentence "
    "the article wrote about the game ('a long run in the third'), so the cell holds text "
    "or a one-off number whose definition is the sentence. It witnesses nothing a canonical "
    "column could hold, and mapping it would put a caption under a stat")
_STANDINGS = (
    "LEAGUE CONTEXT the newspaper reported alongside the game -- standings, division race, "
    "streaks, record-after-game. Real material, but it is a property of the SEASON at a "
    "point in time rather than of this game, and the v26 subject is player-week. Its "
    "obligation is the standings/schedule lane, not the stat mapping lane")
_PROVENANCE = (
    "OUR OWN extraction bookkeeping -- confidence, evidence locator, identity resolution "
    "state, pilot keys, review/promotion status. It records how the row was obtained and "
    "witnesses nothing about the game")
_ONEOFF = (
    "AN EXTRACTION ARTEFACT, not a concept. The stat_name embeds a specific team, player or "
    "game ('boston_first_downs_from_scrimmage', 'passing_yards_on_single_completion_to_karr'"
    ", 'first_score_against_detroit_lions_1934'), so it can never recur and cannot be a "
    "column. The MATERIAL is real and reachable -- it is a first-down count or a passing "
    "gain -- but it must be re-keyed to the general concept at extraction, not adjudicated "
    "here under a name that names one afternoon")
_LOCATOR = (
    "locates the row -- team, opponent, player, date, week -- rather than measuring "
    "anything. Its obligation is the crosswalk lane (§19.2)")
_COMPOSITE = (
    "a COMPOSITE cell: the newspaper prints several facts in one string ('7-14-0', a "
    "line score by period, 'completions-attempts-yards'). No single-column disposition "
    "fits, which is the same split-at-admission gap StatsCrew's `results.game` and the "
    "nflcom made-att buckets opened. SETTLED BY: a parse into constituents at admission")

_PLAY_GRAIN = (
    "A SINGLE PLAY's yardage, not a player-week total. The newspaper reports that the "
    "touchdown pass covered 38 yards; v26's subject is a player-week and its "
    "passing_yards is that week's total. Writing 38 into it would replace a total with "
    "one play, and the two are not the same measurement at a different resolution -- "
    "they are different statistics. The material is real and there is no play-grain "
    "surface to hold it. NOT a longest-play column either: a play the article happened "
    "to describe is not necessarily the longest one, so it cannot be folded into "
    "passing_long/rushing_long/receiving_long without asserting something the source "
    "never said. SETTLED BY: a play-grain lane, or a re-key at extraction to an "
    "explicitly-longest concept where the article says so")
_PERIOD_GRAIN = (
    "TEAM-QUARTER grain. A period score, a quarter's touchdown count, a half's first "
    "downs: real, well-defined, and a property of a team in a PERIOD. v26 has no period "
    "axis at all -- no column in the release contains 'quarter' or 'period' -- so this "
    "is a missing GRAIN rather than a missing column, and minting four quarter columns "
    "to hold it would be a schema decision taken inside a mapping pass. Carried as an "
    "exclusion that names its own settling condition, the way _COMPOSITE does, because "
    "the material is genuinely addable and should not be lost. SETTLED BY: Joe ruling "
    "whether the supertable takes a period axis")
_DRIVE_GRAIN = (
    "DRIVE grain -- red-zone trips, scoring chances converted, yards on the drive that "
    "set up a score. A real, countable property of a possession, and v26 has no drive "
    "axis: its subjects are player-week and team-week. Same shape as _PERIOD_GRAIN, one "
    "level up. SETTLED BY: Joe ruling whether the supertable takes a drive axis")
_ERA_VOCABULARY = (
    "1930s newspaper vocabulary whose DEFINITION is not recoverable from the name and "
    "which the extractor carried across verbatim. 'Number kicks' does not say whether it "
    "counts punts, kickoffs or placement attempts, and 'passes grounded' is an era term "
    "for a pass deliberately thrown away that our vocabulary has no equivalent for. "
    "Mapping either would invent a definition, which §19 forbids. SETTLED BY: reading "
    "the surrounding box-score columns in the source articles to fix the denominator")
_WHOSE_FUMBLE = (
    "the same whose-fumble question NFL.com's bare `FR` opened: v26 splits "
    "fumble_recovery_own from fumble_recovery_opp, the newspaper says only that a "
    "fumble was recovered, and guessing picks one of two real canonical columns. One "
    "ruling settles this and the nflcom rows together. SETTLED BY: the open "
    "whose-fumble escalation")

_EXPLICIT_EXCLUDE: dict[str, str] = {
    # ---- the 2026-07-29 tail: 146 stat_names the first pass left unhandled --------
    "blocked_field_goal_attempt_distance_yards": _PLAY_GRAIN,
    "field_goal_distance": _PLAY_GRAIN, "field_goal_distance_yards": _PLAY_GRAIN,
    "field_goal_line_yards": _PLAY_GRAIN, "field_goal_made_distance_yards": _PLAY_GRAIN,
    "field_goal_made_yards": _PLAY_GRAIN, "field_goal_yards": _PLAY_GRAIN,
    "field_goal_yards_made": _PLAY_GRAIN, "field_goals_from_yardline": _PLAY_GRAIN,
    "interception_return_touchdown_distance": _PLAY_GRAIN,
    "interception_return_touchdown_distance_yards": _PLAY_GRAIN,
    "interception_return_touchdown_yards": _PLAY_GRAIN,
    "kickoff_return_touchdown_distance": _PLAY_GRAIN,
    "kickoff_return_yards_opening": _PLAY_GRAIN, "known_receiving_gain_yards": _PLAY_GRAIN,
    "lateral_play_gain_yards": _PLAY_GRAIN, "long_touchdown_run_yards": _PLAY_GRAIN,
    "missed_field_goal_attempt_distance": _PLAY_GRAIN,
    "missed_field_goal_distance": _PLAY_GRAIN,
    "missed_field_goal_distance_yards": _PLAY_GRAIN,
    "partially_blocked_punt_return_yards": _PLAY_GRAIN,
    "passing_touchdown_distance": _PLAY_GRAIN, "passing_touchdown_yards": _PLAY_GRAIN,
    "passing_yards_on_touchdown": _PLAY_GRAIN, "punt_return_touchdown_yards": _PLAY_GRAIN,
    "receiving_touchdown_distance": _PLAY_GRAIN, "receiving_touchdown_yards": _PLAY_GRAIN,
    "receiving_yards_on_touchdown": _PLAY_GRAIN,
    "rushing_gain_yards_captioned_play": _PLAY_GRAIN,
    "rushing_touchdown_distance": _PLAY_GRAIN,
    "rushing_touchdown_distance_yards": _PLAY_GRAIN, "rushing_touchdown_yards": _PLAY_GRAIN,
    "rushing_yards_on_touchdown": _PLAY_GRAIN, "touchdown_distance": _PLAY_GRAIN,
    "touchdown_pass_distance": _PLAY_GRAIN, "touchdown_pass_yards": _PLAY_GRAIN,
    "touchdown_play_yards": _PLAY_GRAIN, "touchdown_reception_distance": _PLAY_GRAIN,
    "touchdown_reception_yards": _PLAY_GRAIN, "touchdown_run_distance": _PLAY_GRAIN,
    "touchdown_run_yards": _PLAY_GRAIN,
    "field_goal_line_of_scrimmage": _LOCATOR, "field_goal_placement_yard_line": _LOCATOR,
    "fumble_recovery_yardline": _LOCATOR, "interception_return_to_yardline": _LOCATOR,
    "kickoff_return_to_yardline": _LOCATOR, "return_end_yardline": _LOCATOR,
    "return_tackle_spot": _LOCATOR, "touchdown_saving_tackle_yardline": _LOCATOR,
    "blocked_punt_recovery_return_to_opponent_10": _ONEOFF,
    "blocked_punt_recovery_return_to_opponent_19": _ONEOFF,
    "extra_points_made_by_named_player": _ONEOFF,
    "field_goals_made_by_named_player": _ONEOFF, "named_leading_win_contributor": _ONEOFF,
    "receiving_yards_on_fourth_td_drive": _ONEOFF,
    "receiving_yards_on_single_completion_from_masterson": _ONEOFF,
    "rushing_yards_visible_photo_caption": _ONEOFF,
    "team_field_goals_made_by_named_player": _ONEOFF,
    "team_pass_attempts_by_named_player": _ONEOFF,
    "team_pass_completions_by_named_player": _ONEOFF,
    "team_points_by_single_player": _ONEOFF,
    "first_period_lead": _NARRATIVE, "first_quarter_setup_run": _NARRATIVE,
    "game_result_without_score": _NARRATIVE, "in_game_lead": _NARRATIVE,
    "late_third_period_lead": _NARRATIVE, "long_run_to_scoring_position": _NARRATIVE,
    "missed_scoring_opportunities": _NARRATIVE, "offensive_style": _NARRATIVE,
    "opponent_crossed_50": _NARRATIVE,
    "opponent_goal_line_crossings_allowed_to_date": _NARRATIVE,
    "opponent_never_inside_20": _NARRATIVE, "opponent_territory_possessions": _NARRATIVE,
    "passes_completed_on_touchdown_drive": _NARRATIVE, "play_result": _NARRATIVE,
    "points_from_touchdowns": _NARRATIVE, "safety_conceded_on_bad_snap": _NARRATIVE, "blocked_punt_safety_against": _NARRATIVE,
    "safety_involved": _NARRATIVE, "score_after_third_quarter": _NARRATIVE,
    "score_entering_final_period": _NARRATIVE,
    "score_state_five_minutes_remaining": _NARRATIVE, "scored_every_period": _NARRATIVE,
    "scored_in_every_period": _NARRATIVE, "scoreless_first_three_quarters": _NARRATIVE,
    "scoreless_tie": _NARRATIVE, "scoring_window": _NARRATIVE, "team_passing": _NARRATIVE,
    "team_points_result_line": _NARRATIVE, "touchdown_involvement": _NARRATIVE,
    "touchdown_lateral_assist": _NARRATIVE, "touchdown_pass_chain": _NARRATIVE,
    "touchdowns_reported": _NARRATIVE,
    "blocked_kicks": _COMPOSITE, "extra_points_blocked_or_missed": _COMPOSITE,
    "failed_placement_kicks": _COMPOSITE,
    "first_period_touchdowns": _PERIOD_GRAIN, "fourth_period_touchdowns": _PERIOD_GRAIN,
    "fourth_quarter_touchdowns": _PERIOD_GRAIN, "period_score_q1": _PERIOD_GRAIN,
    "period_score_q2": _PERIOD_GRAIN, "period_score_q3": _PERIOD_GRAIN,
    "period_score_q4": _PERIOD_GRAIN, "scoring_periods_count": _PERIOD_GRAIN,
    "second_half_first_downs": _PERIOD_GRAIN,
    "second_half_forward_pass_attempts": _PERIOD_GRAIN,
    "second_half_touchdowns": _PERIOD_GRAIN,
    "second_quarter_extra_points_made": _PERIOD_GRAIN,
    "second_quarter_field_goals_made": _PERIOD_GRAIN,
    "second_quarter_points": _PERIOD_GRAIN, "second_quarter_touchdowns": _PERIOD_GRAIN,
    "team_safeties_second_half": _PERIOD_GRAIN,
    "team_touchdowns_second_half": _PERIOD_GRAIN, "third_period_points": _PERIOD_GRAIN,
    "third_quarter_touchdowns": _PERIOD_GRAIN, "touchdowns_in_second_period": _PERIOD_GRAIN,
    "touchdowns_in_third_period": _PERIOD_GRAIN,
    "drive_setup_play": _DRIVE_GRAIN, "failed_drive_yards": _DRIVE_GRAIN,
    "late_drive_yards": _DRIVE_GRAIN,
    "receiving_yards_on_scoring_drive_setup": _DRIVE_GRAIN,
    "receiving_yards_on_scoring_setup": _DRIVE_GRAIN, "red_zone_empty_trips": _DRIVE_GRAIN,
    "scoring_drive_setup": _DRIVE_GRAIN, "scoring_drive_yards": _DRIVE_GRAIN,
    "scoring_opportunities_converted": _DRIVE_GRAIN,
    "scoring_opportunities_lost_to_fumbles": _DRIVE_GRAIN,
    "successive_first_downs": _DRIVE_GRAIN,
    "number_kicks": _ERA_VOCABULARY, "passes_grounded": _ERA_VOCABULARY,
    "fumble_recoveries": _WHOSE_FUMBLE, "fumble_recovery": _WHOSE_FUMBLE,
    "attendance": _STANDINGS, "attendance_lower_bound": _STANDINGS,
    "paid_attendance": _STANDINGS, "venue_city": _LOCATOR,
    "weather": _STANDINGS, "weather_condition": _STANDINGS,
    "weather_field_conditions": _STANDINGS, "field_conditions": _STANDINGS,
    "game_weather": _STANDINGS, "game_lighting_context": _STANDINGS,
    "game_date": _LOCATOR, "nfl_team": _LOCATOR, "opponent_nfl_team": _LOCATOR,
    "opponent_raw": _LOCATOR, "player_raw": _LOCATOR, "team_raw": _LOCATOR,
    "resolved_player": _LOCATOR, "receiver": _LOCATOR, "player_week": _LOCATOR,
    "team_1_raw": _LOCATOR, "team_2_raw": _LOCATOR,
    "team_1_nfl_team": _LOCATOR, "team_2_nfl_team": _LOCATOR,
    "result": _STANDINGS, "final_score": _COMPOSITE, "halftime_score": _COMPOSITE,
    "first_half_score": _COMPOSITE, "score_by_period": _COMPOSITE,
    "score_by_periods": _COMPOSITE, "line_score_by_period": _COMPOSITE,
    "line_score_by_quarter": _COMPOSITE, "points_by_quarter": _COMPOSITE,
    "passing_completions_attempts_yards": _COMPOSITE, "passing_line": _COMPOSITE,
    "forward_passing_line": _COMPOSITE, "rushing_line": _COMPOSITE,
    "team_passing_line": _COMPOSITE, "scoring_summary": _COMPOSITE,
    "team_1_score": _COMPOSITE, "team_2_score": _COMPOSITE,
    "team_1_value": _COMPOSITE, "team_2_value": _COMPOSITE,
    "stat_value": _COMPOSITE, "stat_unit": _COMPOSITE, "stat_context": _NARRATIVE,
    "team_stat": _COMPOSITE, "target_table": _PROVENANCE, "stat_source_shape": _PROVENANCE,
    "lineup_side_raw": _LOCATOR, "participation_type": _LOCATOR,
    "distances_yards": _COMPOSITE, "field_goal_distances": _COMPOSITE,
    "scoring_composition": _COMPOSITE, "scoring_periods": _COMPOSITE,
    "touchdowns_by_period": _COMPOSITE, "period_points": _COMPOSITE,
    "score_progression_summary": _COMPOSITE,
}

_PREFIX_EXCLUDE: tuple[tuple[str, str], ...] = (
    ("division_", _STANDINGS), ("league_", _STANDINGS), ("standing", _STANDINGS),
    ("season_", _STANDINGS), ("consecutive_", _STANDINGS), ("championship_", _STANDINGS),
    ("undefeated_", _STANDINGS), ("postgame_", _STANDINGS), ("record_after", _STANDINGS),
    ("home_record", _STANDINGS), ("home_opener", _STANDINGS), ("team_season", _STANDINGS),
    ("team_postgame", _STANDINGS), ("team_streak", _STANDINGS), ("team_win", _STANDINGS),
    ("win_streak", _STANDINGS), ("winning_streak", _STANDINGS), ("losing_streak", _STANDINGS),
    ("shutout_win", _STANDINGS), ("reported_national", _STANDINGS), ("series_wins", _STANDINGS),
    ("career_points", _STANDINGS), ("goal_line_uncrossed", _STANDINGS),
    ("evidence_", _PROVENANCE), ("confidence_", _PROVENANCE), ("identity_", _PROVENANCE),
    ("pilot_", _PROVENANCE), ("source_", _PROVENANCE), ("promotion_", _PROVENANCE),
    ("review_", _PROVENANCE), ("strict_", _PROVENANCE), ("target_entity", _PROVENANCE),
    ("effective_fields", _PROVENANCE), ("controlled_pilot", _PROVENANCE),
    ("third_v3_", _PROVENANCE), ("stat_fields_json", _PROVENANCE),
    ("boston_", _ONEOFF), ("first_packer", _ONEOFF), ("first_score_against", _ONEOFF),
    ("eastern_division", _ONEOFF),
)

_SUFFIX_EXCLUDE: tuple[tuple[str, str], ...] = (
    ("_context", _NARRATIVE), ("_note", _NARRATIVE), ("_narrative", _NARRATIVE),
    ("_claim", _NARRATIVE), ("_visible", _NARRATIVE), ("_summary", _NARRATIVE),
    ("_json", _PROVENANCE), ("_key", _PROVENANCE), ("_status", _PROVENANCE),
    ("_id", _PROVENANCE), ("_text", _NARRATIVE), ("_method", _PROVENANCE),
)

_SUBSTRING_EXCLUDE: tuple[tuple[str, str], ...] = (
    ("_single_completion_to_", _ONEOFF), ("_to_barnard", _ONEOFF),
    ("notable_", _NARRATIVE), ("_from_narrative", _NARRATIVE),
    ("_from_article", _NARRATIVE), ("_partial", _NARRATIVE),
)


def _excluded(name: str) -> str | None:
    if name in _EXPLICIT_EXCLUDE:
        return _EXPLICIT_EXCLUDE[name]
    for prefix, reason in _PREFIX_EXCLUDE:
        if name.startswith(prefix):
            return reason
    for suffix, reason in _SUFFIX_EXCLUDE:
        if name.endswith(suffix):
            return reason
    for part, reason in _SUBSTRING_EXCLUDE:
        if part in name:
            return reason
    return None


def build_decisions() -> tuple[list[dict], dict]:
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    from .column_dossier import load_decisions

    mine = {k for k, e in load_decisions().items() if e.get("generated_by") == GENERATOR}
    decisions: list[dict] = []
    tally = collections.Counter()
    unhandled: set[str] = set()

    for row in dossier["rows"]:
        if row["lineage"] != "newspaper":
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        name = row["column"]
        evidence = (f"newspaper stat_name {name!r} on {row['source']}. These strings are "
                    "OUR extractor's own vocabulary, not a publisher's, so the authorship "
                    "rule governs -- the name is the definition")
        if name in MAPPED:
            decisions.append({"key": key, "disposition": "MAPPED_TO_CANONICAL",
                              "canonical": MAPPED[name],
                              "reason": f"the era's spelling of {MAPPED[name]!r}",
                              "evidence": evidence})
            tally["mapped"] += 1
        elif name in NEW_CANDIDATES:
            decisions.append({"key": key,
                              "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                              "reason": NEW_CANDIDATES[name], "evidence": evidence})
            tally["new"] += 1
        else:
            reason = _excluded(name)
            if reason is None:
                unhandled.add(name)
                tally["unhandled"] += 1
                continue
            decisions.append({"key": key, "disposition": "EXCLUDED_WITH_REASON",
                              "reason": reason, "evidence": evidence})
            tally["excluded"] += 1
    return decisions, dict(tally) | {"unhandled_names": sorted(unhandled)}


def escalated_row_keys() -> dict[str, str]:
    """Nothing is escalated here: every name is either our vocabulary or our artefact."""
    return {}


def _validate(v26: set[str]) -> list[str]:
    problems = [f"{k}: canonical {c!r} does not exist" for k, c in MAPPED.items()
                if c not in v26]
    problems += [f"{k}: proposed NEW but v26 carries it" for k in NEW_CANDIDATES
                 if k in v26]
    overlap = set(MAPPED) & set(NEW_CANDIDATES)
    if overlap:
        problems.append(f"in two buckets: {sorted(overlap)}")
    for name in list(MAPPED) + list(NEW_CANDIDATES):
        if _excluded(name):
            problems.append(f"{name}: mapped/proposed AND caught by an exclude rule -- the "
                            "rule would never fire but the overlap means one of them is "
                            "wrong")
    return problems


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    problems = _validate(_v26_columns())
    if problems:
        print("CORRESPONDENCE TABLE INVALID:")
        for p in problems:
            print("  -", p)
        return 1
    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    for k, v in tally.items():
        if k != "unhandled_names":
            print(f"  {k:12s} {v}")
    if tally["unhandled_names"]:
        print(f"\nUNHANDLED ({len(tally['unhandled_names'])}) -- left OPEN:")
        for i in range(0, len(tally["unhandled_names"]), 3):
            print("   " + "  ".join(f"{n:38s}" for n in tally["unhandled_names"][i:i + 3]))
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
