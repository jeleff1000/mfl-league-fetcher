"""O.9.3 burn-down: the pbp_merged lineage. ONE grain argument, and its exceptions.

THE ARGUMENT, made once. `pbp_merged_1978_2025` has ONE ROW PER PLAY -- `sources.py`
declares its own join key as `game_id+play`, and its mapping obligation is already recorded
as LANE_WITNESSED: "pbp regex/structured lanes + rollup parent (ROLLUP CARRIES THE
MAPSPECS)". The v26 subject is a player-week. So no column of that table is a player-week
measurement, and the witness path for the material it holds runs through
`pbp_player_week_rollup`, which is a separately registered source with its own dossier rows
and its own adjudication. 371 open rows, one argument, four cohorts for the reason string.

BUT THE LINEAGE IS NOT ONE TABLE, AND THAT IS THE WHOLE POINT OF THIS MODULE.

Joe: "those def_* columns are not derived, they are from pbp." Correct, and measured. Of the
404 open rows in this lineage, 371 are the play-grain corpus and 33 are not:

    pbp_team_defense           19 open   TEAM-WEEK grain -- 16 of them are `def_*`
    pbp_player_week_rollup     13 open   PLAYER-WEEK grain
    ancient_pbp1978_recovery    1 open

Sweeping the lineage on the grain argument would have excluded sixteen columns that are
exact, registered, populated canonicals of family `defense`, at a grain v26 carries -- and
they are in the thinnest part of the supertable. That is precisely the "sample before
sweeping" rule, and this is the largest thing it has caught.

THE AUTHORSHIP MEASUREMENT SPLITS THE LINEAGE, and it is not close:

    pbp_team_defense           20 of 21 dossier columns are exact v26 names   95.2%
    pbp_player_week_rollup    109 of 123                                      88.6%
    pbp_merged_1978_2025        7 of 374                                       1.9%

A fifty-fold gap inside ONE lineage. The two rollups are OURS -- our aggregation code names
those columns out of the same vocabulary the contract registry declares. The merged corpus
carries nflverse's vocabulary and is TRANSCRIBED even though we assembled the file. So the
same lineage gets opposite treatments in one pass, and the measurement is why, not a
judgement about which felt more familiar.

THE SEVEN NAME COLLISIONS ARE STILL EXCLUDED, and they are the reason the authorship rule
is scoped by source rather than applied wherever a name matches. `passing_yards` exists in
the play table AND in v26. They are not the same fact: one is the yardage of ONE PASS, the
other is a player's weekly total. Mapping them because the strings agree is the exact error
the rule was written to prevent, at the one spot where it is most tempting.

Run:  python -m scripts.sota_recon.pbp_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger

GENERATOR = "scripts.sota_recon.pbp_column_adjudication"

SOURCES = {
    "pbp_merged_1978_2025": "the merged play-by-play corpus, one row per PLAY",
    "pbp_team_defense": "our PBP-derived team-defense weekly rollup, one row per "
                        "(team, year, week, season_type)",
    "pbp_player_week_rollup": "our PBP-derived player-week rollup",
    "ancient_pbp1978_recovery": "the pbp-1978/79 recovery stream of the ancient bundle",
}

# ---- THE PLAY-GRAIN COHORTS ---------------------------------------------------------
# Four cohorts, one argument. The cohort decides only WHICH SENTENCE the exclusion is
# written with; the exclusion itself is the same for all 371 and rests on the source's own
# declared join key. The membership is a CLOSED ENUMERATION, not a catch-all: a column the
# table gains upstream falls through to the residual, stays OPEN, and is reported. A
# catch-all fourth cohort would have swept the def_* block without anyone noticing.
GRAIN_COHORTS: dict[str, str] = {
    "GAME_AND_CLOCK_STATE":
        "describes the GAME SITUATION a play occurred in -- down, distance, field position, "
        "clock, score, drive, venue, weather, betting line. It is the state the play "
        "happened in, not a measurement of any player",
    "MODEL_OUTPUT":
        "an nflverse MODEL output evaluated per play (expected points, win probability, "
        "completion probability, expected YAC and the running per-team accumulators of "
        "each). It measures the play against a model, and its player-week form is a "
        "weighted aggregate our rollup computes, never a value this row holds",
    "PARTICIPANT_IDENTITY":
        "names WHO filled a role on one play -- passer, rusher, tackler, returner, penalised "
        "player, coach, team. It locates a participant; its obligation is the crosswalk "
        "lane (§19.2), and at player-week grain the participant IS the subject rather than "
        "a column of it",
    "PLAY_EVENT":
        "a per-play event marker or per-play measure (attempt flags, outcome flags, yardage "
        "on THIS play). Its player-week form is the SUM or COUNT of this column over the "
        "player's plays -- an aggregation carried by pbp_player_week_rollup, which is "
        "registered separately and adjudicated on authorship. This column is that rollup's "
        "INPUT, not the canonical cell",
}

_STATE = frozenset({
    "play_id", "old_game_id", "nfl_api_id", "id", "order_sequence", "drive", "fixed_drive",
    "series", "qtr", "quarter_end", "down", "ydstogo", "ydsnet", "yardline_100", "yrdln",
    "side_of_field", "goal_to_go", "game_date", "game_half", "game_seconds_remaining",
    "half_seconds_remaining", "quarter_seconds_remaining", "time", "time_of_day",
    "start_time", "end_clock_time", "end_yard_line", "play_clock", "season_type", "week",
    "location", "div_game", "roof", "surface", "temp", "wind", "weather", "stadium",
    "stadium_id", "game_stadium", "spread_line", "total_line", "total",
    "home_opening_kickoff", "posteam_type", "home_score", "away_score", "posteam_score",
    "defteam_score", "posteam_score_post", "defteam_score_post", "score_differential",
    "score_differential_post", "total_home_score", "total_away_score",
    "home_timeouts_remaining", "away_timeouts_remaining", "posteam_timeouts_remaining",
    "defteam_timeouts_remaining", "result", "series_result", "fixed_drive_result",
    "timeout", "timeout_team", "play_deleted", "aborted_play", "replay_or_challenge",
    "replay_or_challenge_result", "desc", "play", "play_type", "play_type_nfl",
    "st_play_type", "special", "special_teams_play", "sp", "pbp_source_system",
    "player_id_namespace"})
_MODEL = frozenset({
    "ep", "epa", "wp", "wpa", "cp", "cpoe", "def_wp", "home_wp", "away_wp", "home_wp_post",
    "away_wp_post", "vegas_wp", "vegas_home_wp", "vegas_wpa", "vegas_home_wpa", "air_epa",
    "air_wpa", "comp_air_epa", "comp_air_wpa", "comp_yac_epa", "comp_yac_wpa", "yac_epa",
    "yac_wpa", "qb_epa", "pass_oe", "xpass", "success", "series_success", "no_score_prob",
    "opp_fg_prob", "opp_safety_prob", "opp_td_prob", "fg_prob", "safety_prob", "td_prob",
    "extra_point_prob", "two_point_conversion_prob", "xyac_epa", "xyac_fd",
    "xyac_mean_yardage", "xyac_median_yardage", "xyac_success"})
_PARTICIPANT = frozenset({
    "passer", "rusher", "receiver", "name", "fantasy", "fantasy_id", "posteam", "defteam",
    "home_team", "away_team", "return_team", "home_coach", "away_coach", "jersey_number"})
_PARTICIPANT_SUFFIXES = ("_player_id", "_player_name", "_team", "_id", "_jersey_number")
_PLAY_EVENT = frozenset({
    "air_yards", "assist_tackle", "complete_pass", "defensive_extra_point_attempt",
    "defensive_extra_point_conv", "defensive_two_point_attempt", "defensive_two_point_conv",
    "extra_point_attempt", "extra_point_result", "field_goal_attempt", "field_goal_result",
    "first_down", "first_down_pass", "first_down_penalty", "first_down_rush",
    "fourth_down_converted", "fourth_down_failed", "fumble", "fumble_forced", "fumble_lost",
    "fumble_not_forced", "fumble_out_of_bounds", "fumble_recovery_1_yards",
    "fumble_recovery_2_yards", "incomplete_pass", "interception", "kick_distance",
    "kickoff_attempt", "kickoff_downed", "kickoff_fair_catch", "kickoff_in_endzone",
    "kickoff_inside_twenty", "kickoff_out_of_bounds", "lateral_receiving_yards",
    "lateral_reception", "lateral_recovery", "lateral_return", "lateral_rush",
    "lateral_rushing_yards", "no_huddle", "out_of_bounds", "own_kickoff_recovery",
    "own_kickoff_recovery_td", "pass", "pass_attempt", "pass_length", "pass_location",
    "pass_touchdown", "passing_yards", "penalty", "penalty_type", "penalty_yards",
    "punt_attempt", "punt_blocked", "punt_downed", "punt_fair_catch", "punt_in_endzone",
    "punt_inside_twenty", "punt_out_of_bounds", "qb_dropback", "qb_hit", "qb_kneel",
    "qb_scramble", "qb_spike", "receiving_yards", "return_touchdown", "return_yards",
    "run_gap", "run_location", "rush", "rush_attempt", "rush_touchdown", "rushing_yards",
    "sack", "safety", "shotgun", "solo_tackle", "tackle_with_assist", "tackled_for_loss",
    "third_down_converted", "third_down_failed", "touchback", "touchdown",
    "two_point_attempt", "two_point_conv_result", "yards_after_catch", "yards_gained"})


def cohort(column: str) -> str | None:
    """First match wins, and there is deliberately NO catch-all."""
    if column in _STATE or column.startswith("drive_"):
        return "GAME_AND_CLOCK_STATE"
    if column in _MODEL or column.startswith(("total_home_", "total_away_")):
        return "MODEL_OUTPUT"
    if column in _PARTICIPANT or column.endswith(_PARTICIPANT_SUFFIXES):
        return "PARTICIPANT_IDENTITY"
    if column in _PLAY_EVENT:
        return "PLAY_EVENT"
    return None


# ---- THE COLUMNS THAT ARE NOT PLAY-GRAIN, CHECKED ONE AT A TIME ----------------------
#
# `pbp_team_defense` is a TEAM-WEEK table. Its 16 `def_*` columns are exact, registered
# canonicals of family `defense`, and v26 carries team defence as DEF player rows, so a
# team-week row IS the DEF player-week row. Verified against the file itself: one row per
# (nfl_team, year, week, season_type), values populated from 1978.
#
# STAGE BOUNDARY, and it matters more here than anywhere else in this pass: the source's
# `witness_class` is **derived** -- our computation over the merged corpus. Mapping these
# settles IDENTITY and gives the columns an adjudicated witness in the pbp_merged lineage.
# It does NOT make them cross-examinable: a derived witness re-derives, it does not vote.
# The vote for these cells would come from the primary corpus through this same rollup path
# and is a licensing question, not this one. Reporting the mapping as "0 -> 1 witness" is
# true; reporting it as "0 -> 1 VOTE" would not be.
MAPPED: dict[str, str] = {
    "def_plays": "def_plays",
    "def_epa_allowed": "def_epa_allowed",
    "def_pass_epa_allowed": "def_pass_epa_allowed",
    "def_rush_epa_allowed": "def_rush_epa_allowed",
    "def_wpa_allowed": "def_wpa_allowed",
    "def_success_allowed": "def_success_allowed",
    "def_success_plays": "def_success_plays",
    "def_explosive_pass_allowed": "def_explosive_pass_allowed",
    "def_explosive_rush_allowed": "def_explosive_rush_allowed",
    "def_yards_allowed": "def_yards_allowed",
    "def_third_down_faced": "def_third_down_faced",
    "def_third_down_allowed": "def_third_down_allowed",
    "def_fourth_down_faced": "def_fourth_down_faced",
    "def_fourth_down_allowed": "def_fourth_down_allowed",
    "def_rz_plays_faced": "def_rz_plays_faced",
    "def_rz_td_allowed": "def_rz_td_allowed",
}

# Columns in the two rollups that are NOT canonical material, each with the measurement or
# the argument that settles it. Enumerated rather than swept because they sit beside sixteen
# columns that ARE material and share their table.
NOT_PLAY_GRAIN: dict[str, tuple[str, str]] = {
    "pbp_team_defense|nfl_team": (
        "EXCLUDED_WITH_REASON",
        "LOCATOR: the team whose defence the row measures -- it locates the row in franchise space"),
    "pbp_team_defense|season_type": (
        "EXCLUDED_WITH_REASON",
        "LOCATOR: REG/POST partition of the row, a locator in the season dimension"),
    "pbp_team_defense|team_week": (
        "EXCLUDED_WITH_REASON",
        "LOCATOR: the composite row key ('IND_1981_2'), minted by our own rollup"),
    "pbp_player_week_rollup|games": (
        "EXCLUDED_WITH_REASON",
        "MEASURED: exactly ONE distinct value over all 732,058 rows -- constant 1. It "
        "asserts that this player-week row exists, which the row already asserts, so it "
        "can never disagree with anything and cannot witness. Read the other way it would "
        "be `games_played` at player-game grain; that reading is recorded here so the row "
        "can be reopened, but a constant column is not evidence of a count"),
    "pbp_player_week_rollup|event_rows": (
        "EXCLUDED_WITH_REASON",
        "MEASURED 1-367, mean 15.0: the number of PLAY ROWS aggregated into this "
        "player-week. It counts our own aggregation's inputs, not anything that happened "
        "in a football game"),
    "pbp_player_week_rollup|event_roles": (
        "EXCLUDED_WITH_REASON",
        "which participant roles the player filled across the aggregated plays -- our "
        "rollup's own construction record"),
    "pbp_player_week_rollup|nfl_team_context_count": (
        "EXCLUDED_WITH_REASON",
        "a QA counter our rollup writes: how many distinct team values the aggregated "
        "plays carried. It measures the aggregation, not the player"),
    "pbp_player_week_rollup|opponent_context_count": (
        "EXCLUDED_WITH_REASON",
        "the same QA counter on the opponent axis"),
    "pbp_player_week_rollup|mapped_to_player_bio": (
        "EXCLUDED_WITH_REASON",
        "whether identity resolution found this player in the bio spine -- a crosswalk "
        "outcome flag, and its obligation is the crosswalk lane (§19.2)"),
    "pbp_player_week_rollup|pbp_source_systems": (
        "EXCLUDED_WITH_REASON",
        "PROVENANCE: which upstream pbp systems contributed rows to this aggregate"),
    "pbp_player_week_rollup|player_id_namespaces": (
        "EXCLUDED_WITH_REASON",
        "PROVENANCE: which id namespaces the aggregated play rows used"),
    "pbp_player_week_rollup|pbp_player_id": (
        "EXCLUDED_WITH_REASON",
        "LOCATOR: the upstream player id this row aggregates; its obligation is the crosswalk lane"),
    "pbp_player_week_rollup|pbp_player_id_clean": (
        "EXCLUDED_WITH_REASON",
        "LOCATOR: the normalised form of the same id"),
    "pbp_player_week_rollup|pbp_player_name": (
        "EXCLUDED_WITH_REASON",
        "LOCATOR: the upstream player name; a name is a locator and the twins hazard (§19.2) is "
        "exactly why it may not be a witness"),
    "pbp_player_week_rollup|passing_cpoe_n": (
        "EXCLUDED_WITH_REASON",
        "the COUNT half of a weighted-mean intermediate. `passing_cpoe` is a registered "
        "canonical and a MEAN, so our rollup carries (sum, n) to let the mean recombine "
        "across weeks. The pair is the aggregation's internal state; neither half is the "
        "canonical, and mapping either one would put a partial sum under a mean's name"),
    "pbp_player_week_rollup|passing_cpoe_sum": (
        "EXCLUDED_WITH_REASON",
        "the SUM half of that same weighted-mean intermediate"),
}

# The seven strings that exist in BOTH the play table and v26. Called out individually so
# the exclusion is visibly deliberate at the one place where a name match is most tempting
# and most wrong.
_NAME_COLLISIONS = frozenset({"passing_yards", "rushing_yards", "receiving_yards",
                              "penalty_yards", "game_date", "season_type", "week"})


def escalated_row_keys() -> dict[str, str]:
    """No live refusal remains in this pass.

    The former source_positions refusal was lifted by the settled taxonomy ruling:
    the source's specific, multi-valued strings map to nfl_position. The assembled-bundle
    alias is applied by internal_column_adjudication, which owns that read-side schema.
    """
    return {}


def build_decisions() -> tuple[list[dict], dict]:
    from .column_dossier import load_decisions

    v26 = _v26_columns()
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    existing = load_decisions()
    mine = {key for key, entry in existing.items()
            if entry.get("generated_by") == GENERATOR}
    escalated = set(escalated_row_keys())

    decisions: list[dict] = []
    tally: collections.Counter = collections.Counter()
    residual: list[str] = []
    bad_target: list[str] = []

    for row in dossier["rows"]:
        source = row["source"]
        if source not in SOURCES:
            continue
        key = row_key(source, row["table_key"], row["column"])
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        if key in escalated:
            tally["escalated"] += 1
            continue
        column = row["column"]

        pair = f"{source}|{column}"
        if pair in NOT_PLAY_GRAIN:
            disposition, reason = NOT_PLAY_GRAIN[pair]
            decisions.append({
                "key": key, "disposition": disposition, "reason": reason,
                "evidence": f"{source} is {SOURCES[source]}. Checked against the table "
                            f"itself rather than swept with its neighbours -- this column "
                            f"sits beside {len(MAPPED)} columns that ARE canonical "
                            f"material and shares their table"})
            tally[disposition] += 1
            continue

        if source == "pbp_team_defense" and column in MAPPED:
            canonical = MAPPED[column]
            if canonical not in v26:
                bad_target.append(f"{pair} -> {canonical}")
                continue
            decisions.append({
                "key": key,
                "disposition": "MAPPED_TO_CANONICAL",
                "canonical": canonical,
                "reason": f"our PBP-derived team-defense weekly rollup publishes this at "
                          f"TEAM-WEEK grain, and v26 carries team defence as DEF player "
                          f"rows, so the row IS the canonical cell's subject. Name "
                          f"identical because our own aggregation code wrote it out of the "
                          f"contract registry's vocabulary",
                "evidence": f"AUTHORSHIP, measured on this exact table: 20 of its 21 "
                            f"dossier columns are exact v26 names (95.2%), against 1.9% "
                            f"for pbp_merged_1978_2025 in the SAME lineage -- a fifty-fold "
                            f"gap that is what licenses a name match here and forbids it "
                            f"there. stat_contracts.v1.json carries {canonical!r} in "
                            f"family 'defense' at natural grain 'player_game'. File "
                            f"verified: one row per (nfl_team, year, week, season_type), "
                            f"populated from 1978. STAGE BOUNDARY: this source's "
                            f"witness_class is 'derived', so it re-derives and does not "
                            f"vote -- identity, not licensing",
            })
            tally["MAPPED_TO_CANONICAL"] += 1
            continue

        if source == "pbp_merged_1978_2025":
            name = cohort(column)
            if name is None:
                residual.append(column)
                continue
            collision = ""
            if column in _NAME_COLLISIONS:
                collision = (f" NAME COLLISION, EXCLUDED DELIBERATELY: v26 also has a "
                             f"column called {column!r}. They are not the same fact -- "
                             f"this one is the value on ONE PLAY and that one is a "
                             f"player's weekly total. The authorship rule is scoped by "
                             f"SOURCE precisely so a matching string cannot close a row "
                             f"in a transcribed table.")
            decisions.append({
                "key": key,
                "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"PLAY GRAIN ({name}): {GRAIN_COHORTS[name]}.{collision}",
                "evidence": f"sources.py declares this source's join key as "
                            f"'game_id+play' -- one row per play, 2.1M plays -- while the "
                            f"v26 subject is a player-week. test_mapping_obligation "
                            f"records its obligation as LANE_WITNESSED: 'pbp regex/"
                            f"structured lanes + rollup parent (rollup carries the "
                            f"MapSpecs)', so the material here reaches canonical space "
                            f"through pbp_player_week_rollup, which is registered "
                            f"separately and carries its own dossier rows. AUTHORSHIP: "
                            f"7 of 374 columns are exact v26 names (1.9%) -- this table "
                            f"carries nflverse's vocabulary and is TRANSCRIBED even though "
                            f"we assembled the file",
            })
            tally[f"EXCLUDED_PLAY_GRAIN:{name}"] += 1
            continue

        residual.append(f"{source}|{column}")

    return decisions, {
        "total": len(decisions),
        "by_disposition": dict(sorted(tally.items())),
        "residual_left_open": sorted(residual),
        "canonical_not_in_v26": sorted(bad_target),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    decisions, tally = build_decisions()
    print(f"decisions: {tally['total']:,}")
    for name, value in tally["by_disposition"].items():
        print(f"  {name:44s} {value}")
    if tally["canonical_not_in_v26"]:
        print("\nMAPPED targets that do not exist in v26 -- NOT written:")
        for name in tally["canonical_not_in_v26"]:
            print("   -", name)
        return 1
    if tally["residual_left_open"]:
        print(f"\nRESIDUAL -- no cohort claims these, left OPEN ("
              f"{len(tally['residual_left_open'])}):")
        for name in tally["residual_left_open"][:40]:
            print("   -", name)
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
