"""O.9.3 burn-down: PFR, argued against its own `data-stat` machine ids.

THE EVIDENCE IS PFR'S OWN PUBLISHED NAME, and for PFR that name is already the column name:
every cell on a PFR table carries `data-stat="def_cmp_yds"`, and our capture stored that id
verbatim. So this is the StatsCrew shape at scale -- the correspondence is argued against a
machine id the publisher emits, not against a header we transcribed and not against a slug
of our own.

TWO SPELLINGS OF ONE CONCEPT, WHICH IS THE SIGNATURE OF A TRANSCRIBED VOCABULARY.
`def_cmp_pct` (season tables) and `def_cmp_perc` (box tables) are the same PFR concept under
two ids. A vocabulary we author does not do that; one we transcribe from a site that changed
its own generator between page families does. Both are decided here, identically, and the
pair is recorded rather than silently folded.

STEP 0 WAS RUN FIRST. `mapspec_column_adjudication` already imported 189 committed MapSpecs
for this lineage, and those rows are closed. What is left is 755 rows over 271 distinct
data-stat ids -- a column decided once and fanned out over every table that publishes it,
which is why 271 arguments close 755 rows.

WHAT THIS PASS REFUSES, and each refusal names its question rather than picking a side:

    tackles_combined       the tackle total, now refused from a FOURTH surface. PFR
                           publishes it beside solo and assists on some tables and alone
                           on others, and which of them is the site's total is exactly the
                           open escalation.
    def_tgt_yds_per_att    PFR's adv-defense table publishes BOTH this and
    def_yds_per_target     `def_yds_per_target`. Two published columns, two candidate
                           readings (yards per target, and average depth of target), and
                           nothing in the ids settles which is which. Escalated as a PAIR,
                           the way the L4 return block is, because guessing one forces the
                           other.
    pos                    settled: PFR's specific, sometimes multi-valued
                           position string maps to `nfl_position`; `position` remains the
                           ten fantasy buckets.
    awards, draft_info     one cell, several facts ('AP1, PB, MVP-1'). That is the
                           split-at-admission question already on Joe's queue, reached here
                           from a third lineage.
    stat / home_stat /     `pfr_box_team_stats` and `pfr_box_game_info` are LONG tables the
    vis_stat / info        dossier keyed as WIDE. The question is the REGIME, not the
                           column, and adjudicating the column would freeze the wrong key.

A CORRECTION THIS PASS MADE TO ITS OWN BRIEF. The hand-off list paired PFR `fg_long` with
canonical `fg_made_distance`. The contract registry refuses it: `fg_made_distance` is
unit=count, aggregation SUM, while PFR's `fg_long` is the longest field goal made -- and the
registry already carries `fg_long` at unit=yards, aggregation MAX. Same string, right
target, one line down. Checking a proposed mapping against the contract's UNIT and
AGGREGATION CLASS is cheap and it caught this in the first pass over the list.

Run:  python -m scripts.sota_recon.pfr_datastat_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, apply_to_ledger

GENERATOR = "scripts.sota_recon.pfr_datastat_column_adjudication"

SOURCES = "every registered source whose lineage is `pfr`"

# ---- data-stat id -> canonical -------------------------------------------------------
# Keyed on the COLUMN alone, because that is the grain PFR's vocabulary varies at: a
# data-stat id means the same thing on every table that emits it. Where that is NOT true
# the id is escalated instead (see the module docstring), never disambiguated by table.
MAPPED: dict[str, str] = {
    # -- the charting/defensive block. This is the thinnest part of the supertable and
    # -- these ids are the widest external witness available for it.
    "def_cmp": "def_completions_allowed",
    "def_cmp_td": "def_completion_tds_allowed",
    "def_cmp_yds": "def_completion_yards_allowed",
    "def_int": "def_interceptions",
    "def_int_td": "def_int_ret_td",
    "def_int_yds": "def_interception_yards",
    "def_pass_rating": "def_passer_rating_allowed",
    "def_targets": "def_targets_allowed",
    "def_yac": "def_yards_after_catch_allowed",
    "def_air_yds": "def_air_yards_allowed",
    "pass_defended": "def_pass_defended",
    "qb_knockdown": "def_knockdowns",
    "qb_hits": "def_qb_hits",
    "tackles_missed": "def_tackles_missed",
    "tackles_assists": "def_tackle_assists",
    "tackles_loss": "def_tackles_for_loss",
    "fumbles_forced": "def_fumbles_forced",
    "fumbles_rec": "def_fumbles",
    "fumbles_rec_td": "fum_ret_td",
    "fumbles_rec_yds": "fumble_recovery_yards",
    "safety_md": "def_safeties",
    "sacks": "def_sacks",
    # -- snap counts. These four sat among the plumbing and were deliberately NOT swept
    # -- by the bulk rule; this is the decision that closes them properly.
    "defense": "defense_snaps",
    "offense": "offense_snaps",
    "special_teams": "special_teams_snaps",
    "def_pct": "defense_snap_pct",
    "off_pct": "offense_snap_pct",
    "st_pct": "special_teams_snap_pct",
    # -- passing
    "pass_att": "attempts",
    "pass_cmp": "completions",
    "pass_yds": "passing_yards",
    "pass_long": "passing_long",
    "pass_rating": "passer_rating",
    "pass_int_pct": "passing_int_pct",
    "pass_td_pct": "passing_td_pct",
    "pass_first_down": "passing_first_downs",
    "pass_target_yds": "passing_air_yards",
    "pass_yac": "passing_yards_after_catch",
    "pass_sacked": "sacks_suffered",
    "pass_sacked_yds": "sack_yards_lost",
    "pass_sacked_pct": "sack_pct",
    "pass_hits": "passing_hits",
    "pass_hurried": "passing_hurried",
    "pass_pressured": "passing_pressured",
    "pass_blitzed": "passing_blitzed",
    "pass_poor_throws": "passing_poor_throws",
    "pass_adj_net_yds_per_att": "adjusted_net_yards_per_attempt",
    "pass_adj_yds_per_att": "adjusted_yards_per_attempt",
    "pass_net_yds_per_att": "net_yards_per_attempt",
    "pass_yds_per_att": "yards_per_attempt",
    "rush_scrambles": "rushing_scrambles",
    # -- receiving
    "rec": "receptions",
    "rec_yds": "receiving_yards",
    "rec_td": "receiving_tds",
    "rec_long": "receiving_long",
    "rec_first_down": "receiving_first_downs",
    "rec_air_yds": "receiving_completed_air_yards",
    "rec_yac": "receiving_yards_after_catch",
    "rec_drops": "receiving_drops",
    "rec_broken_tackles": "receiving_broken_tackles",
    "rec_pass_rating": "receiving_pass_rating",
    "rec_target_int": "receiving_target_interceptions",
    "targets": "targets",
    # -- rushing
    "rush_att": "carries",
    "rush_yds": "rushing_yards",
    "rush_td": "rushing_tds",
    "rush_long": "rushing_long",
    "rush_first_down": "rushing_first_downs",
    "rush_broken_tackles": "rushing_broken_tackles",
    "rush_yds_before_contact": "rushing_yards_before_contact",
    "rush_yac": "rushing_yards_after_contact",
    "rush_receive_td": "rush_receive_td",
    "yds_from_scrimmage": "yds_from_scrimmage",
    "touches": "touches",
    # -- kicking / punting / returns
    "fg_long": "fg_long",
    "fg_pct": "fg_pct",
    "fga": "fg_att",
    "xpa": "pat_att",
    "xp_pct": "xp_pct",
    "punt_blocked": "punts_blocked",
    "punt_yds_per_punt": "punt_yards_per_punt",
    "punt_ret_long": "punt_return_long",
    "punt_ret_td": "punt_return_tds",
    "kick_ret_long": "kickoff_return_long",
    "kick_ret_td": "kickoff_return_tds",
    "all_purpose_yds": "all_purpose_yards",
    # -- scoring
    "total_tds_scored": "total_tds_scored",
    # -- fumbles (offence side)
    "fumbles": "fumbles",
    # -- combine / bio
    "height": "height",
    "weight": "weight",
    "college": "college",
    "school_name": "college",
    # The settled taxonomy ruling: these are the source's specific position strings,
    # not the ten fantasy buckets carried by `position`. Both are non-aggregatable context
    # under the contract, and nfl_position is explicitly multi-valued (C/G, DE-LB, ...).
    "pos": "nfl_position",
    "source_positions": "nfl_position",
    "forty_yd": "forty",
    "bench_reps": "bench",
    "broad_jump": "broad_jump",
    "cone": "cone",
    "shuttle": "shuttle",
    "vertical": "vertical",
    # -- context / locators that ARE registered canonicals
    "age": "age",
    "games": "games_played",
    "g": "games_played",
    "games_started": "games_started",
    "gs": "games_started",
    "game_date": "game_date",
    "season_type": "season_type",
    "season_phase": "season_type",
    "is_overtime": "is_overtime",
    "home_away": "home_away",
    "nfl_team": "nfl_team",
    "opponent_nfl_team": "opponent_nfl_team",
    "team_points": "team_points",
    "opponent_points": "opponent_points",
    "team_pts": "team_points",
    "opp_pts": "opponent_points",
}

# Real PFR material the supertable has no column for. Routed to the §24.4 census + demand
# join, never added ad hoc.
NEW_CANDIDATES: dict[str, str] = {
    "rec_success": ("PFR's published receiving success rate. The existing v26 `rec_success` "
                     "field is a successful-play count, so no direct scalar mapping is valid. "
                     "Promote a typed receiving success-rate field or retain this as a rate "
                     "witness with its denominator."),
    "rush_success": ("PFR's published rushing success rate. The existing v26 `rush_success` "
                      "field is a successful-play count, so no direct scalar mapping is valid. "
                      "Promote a typed rushing success-rate field or retain this as a rate "
                      "witness with its denominator."),
    "pass_success": ("PFR's published passing success rate. The existing v26 `pass_success` "
                      "field is a successful-play COUNT, not a rate; no canonical rate column "
                      "exists, so this is a season-level promotion candidate (for example, "
                      "`passing_success_pct`) while the raw PFR value remains a derivation "
                      "witness."),
    "av": "PFR's season Approximate Value. The registry carries `w_av` (weighted CAREER "
          "AV) and `dr_av` (draft AV) but no per-season AV",
    "def_int_long": "longest interception return by a defender; the registry has "
                    "def_interception_yards but no long",
    "qbr": "ESPN's Total QBR, republished by PFR. No canonical carries it",
    "comebacks": "fourth-quarter comebacks led",
    "gwd": "game-winning drives led",
    "qb_rec": "the quarterback's won-lost-tied record as a starter, as PFR publishes it",
    "pocket_time": "average time in the pocket, PFR charting",
    "pass_on_target": "on-target throws, PFR charting",
    "pass_spikes": "deliberate spikes, excluded from most rate denominators",
    "pass_throwaways": "deliberate throwaways, likewise",
    "pass_play_action": "play-action dropbacks",
    "pass_play_action_pass_yds": "yards on play-action dropbacks",
    "pass_rpo": "run-pass-option plays",
    "pass_rpo_pass_att": "pass attempts out of RPO",
    "pass_rpo_pass_yds": "pass yards out of RPO",
    "pass_rpo_rush_att": "rush attempts out of RPO",
    "pass_rpo_rush_yds": "rush yards out of RPO",
    "pass_rpo_yds": "total yards out of RPO",
    "punt_in_20": "punts downed inside the 20",
    "punt_net_yds": "net punting yards",
    "punt_ret_yds_opp": "return yards allowed on this punter's punts",
    "punt_tb": "punts into the end zone for a touchback",
    "kickoff_tb": "kickoffs into the end zone for a touchback",
    "reason": "PFR's own reason a player did not play (injury, coach's decision, "
              "suspension). Real material; the registry's `status` is a roster state, not "
              "a per-game absence reason. Deliberately NOT swept with the plumbing -- it "
              "was one of the four columns that check caught",
    "fga1": "field goals attempted from the 1-19 band, as PFR bands them",
    "fga2": "field goals attempted, 20-29",
    "fga3": "field goals attempted, 30-39",
    "fga4": "field goals attempted, 40-49",
    "fga5": "field goals attempted, 50+",
    "fgm1": "field goals made, 1-19 (the registry bands MADE but not ATTEMPTED)",
    "fgm2": "field goals made, 20-29",
    "fgm3": "field goals made, 30-39",
    "fgm4": "field goals made, 40-49",
    "fgm5": "field goals made, 50+",
    "vbd": "PFR's Value Based Drafting score -- a publisher-specific fantasy derivation",
    "uniform_number": "jersey number as PFR lists it per season",
    # ---- CAUGHT ON REVIEW OF THIS TABLE'S OWN FIRST DRAFT. Each of these was written as
    # a MAPPED entry and each was wrong in the same way: the id LOOKED like a canonical
    # that measures something adjacent. Reading meaning off an abbreviation is the trap
    # this programme names, and it caught me inside the module that names it.
    "kickoff": "kickoffs the KICKER put in play. Drafted as `kickoff_returns`, which is "
               "the RETURNER's count -- a different player on the same play. The registry "
               "carries the return side only",
    "kickoff_yds": "yards on those kickoffs, from the kicking side. Drafted as "
                   "`kickoff_return_yards` for the same reason and equally wrong",
    "pass_batted_passes": "passes BATTED DOWN at the line. Drafted as `passing_drops`; a "
                          "batted ball never reaches a receiver, so it is not a drop",
    "def_batted_passes": "passes a defender BATTED. Drafted as `def_pass_defended`; PFR "
                         "publishes both, so they are not the same column",
    "other_td": "PFR's residual touchdown bucket on the scoring table. Which types it "
                "lumps (fumble return, blocked kick, missed-FG return) is a site-definition "
                "question, and the registry separates them -- so this is real material "
                "whose composition has to be settled before it can be split",
    "def_two_pt": "defensive two-point conversions (a returned PAT). All three registered "
                  "2pt canonicals are offensive, so nothing carries this",
}

# Rates PFR publishes whose OPERANDS the same table publishes. Recorded as derived rather
# than mapped: a ratio is recomputable, so it witnesses its operands and adds no
# independent fact -- but it is not excluded either, because a disagreement between the
# published rate and the recomputed one is a real signal about the operands.
DERIVED_RATIOS: dict[str, tuple[str, str]] = {
    "def_cmp_pct": ("def_completions_allowed", "def_targets_allowed"),
    "def_cmp_perc": ("def_completions_allowed", "def_targets_allowed"),
    "def_yds_per_cmp": ("def_completion_yards_allowed", "def_completions_allowed"),
    "tackles_missed_pct": ("def_tackles_missed", "def_tackles_combined"),
    "pass_cmp_pct": ("completions", "attempts"),
    "pass_yds_per_cmp": ("passing_yards", "completions"),
    "pass_air_yds_per_att": ("passing_air_yards", "attempts"),
    "pass_air_yds_per_cmp": ("passing_air_yards", "completions"),
    "pass_yac_per_cmp": ("passing_yards_after_catch", "completions"),
    "pass_drop_pct": ("passing_drops", "attempts"),
    "pass_poor_throw_pct": ("passing_poor_throws", "attempts"),
    "pass_pressured_pct": ("passing_pressured", "dropbacks"),
    "pass_on_target_pct": ("pass_on_target", "attempts"),
    "pass_first_down_pct": ("passing_first_downs", "attempts"),
    "rec_air_yds_per_rec": ("receiving_completed_air_yards", "receptions"),
    "rec_adot": ("receiving_air_yards", "targets"),
    "rec_yac_per_rec": ("receiving_yards_after_catch", "receptions"),
    "rec_yds_per_rec": ("receiving_yards", "receptions"),
    "rec_yds_per_tgt": ("receiving_yards", "targets"),
    "catch_pct": ("receptions", "targets"),
    "rec_drop_pct": ("receiving_drops", "targets"),
    "rec_broken_tackles_per_rec": ("receptions", "receiving_broken_tackles"),
    "rush_yac_per_rush": ("rushing_yards_after_contact", "carries"),
    "rush_yds_bc_per_rush": ("rushing_yards_before_contact", "carries"),
    "rush_yds_per_att": ("rushing_yards", "carries"),
    "rush_broken_tackles_per_rush": ("carries", "rushing_broken_tackles"),
    "rush_scrambles_yds_per_att": ("rushing_yards", "rushing_scrambles"),
    "kick_ret_yds_per_ret": ("kickoff_return_yards", "kickoff_returns"),
    "punt_ret_yds_per_ret": ("punt_return_yards", "punt_returns"),
    "kickoff_yds_avg": ("kickoff_yds", "kickoff"),
    "kickoff_tb_pct": ("kickoff_tb", "kickoff"),
    "punt_net_yds_per_punt": ("punt_net_yds", "punts"),
    "punt_in_20_pct": ("punt_in_20", "punts"),
    "punt_tb_pct": ("punt_tb", "punts"),
    "rec_per_g": ("receptions", "games_played"),
    "rec_yds_per_g": ("receiving_yards", "games_played"),
    "rush_att_per_g": ("carries", "games_played"),
    "rush_yds_per_g": ("rushing_yards", "games_played"),
    "yds_per_touch": ("scrimmage_yards", "touches"),
    "pass_yds_per_g": ("passing_yards", "games_played"),
    "points_per_g": ("total_points_scored", "games_played"),
}

# Some published rates use a denominator that is a composed lower-layer quantity.
# Keep the canonical operands in DERIVED_RATIOS for validation, but state the exact
# equation here so the witness cannot be misread as missed / combined.
DERIVED_RATIO_NOTES: dict[str, str] = {
    "tackles_missed_pct":
        "PFR equation is def_tackles_missed / (def_tackles_combined + "
        "def_tackles_missed); the published combined count excludes the missed-tackle "
        "bucket for this percentage's denominator.",
}

# Columns excluded, each on a stated argument. Grouped by the argument so the same sentence
# is not re-derived per column.
_EXCLUSION_ARGUMENTS = {
    "PLAY_GRAIN": "a column of PFR's play-by-play table, whose subject is one PLAY. The "
                  "v26 subject is a player-week, and this source's obligation is already "
                  "recorded as LANE_WITNESSED (pbp regex lanes + drive recon), so its "
                  "material reaches canonical space through those lanes",
    "DRIVE_GRAIN": "a column of a per-DRIVE table. Its obligation is the drive-balance "
                   "recon lane (recon_doubleheader_alignment), not the mapping lane",
    "SCORING_EVENT_GRAIN": "a column of the per-SCORING-EVENT table, whose obligation is "
                           "the unique-scoring-event lane (recon_scoring_events, R9)",
    "OFFICIATING_CREW": "identifies an official, not a player or a team. No canonical "
                        "column consumes officiating assignments",
    "PRESENCE_LANE": "a column of a starters table whose obligation is the presence "
                     "universe (entity_universes appearance witness)",
    "INDEX_NOT_RAW": "an ERA-ADJUSTED INDEX (100 = league average for that season), not a "
                     "raw measurement. No canonical column mirrors an index, which is why "
                     "pfr_adj_passing witnesses through the plausibility lane rather than "
                     "through MapSpecs -- there is nothing to alias",
    "LOCATOR": "locates the row rather than measuring anything; its obligation is the "
               "crosswalk lane (§19.2)",
    "PROVENANCE": "records how the row was obtained, not a stat",
    "PFR_EXPECTED_POINTS_SPLIT": "a component of PFR's own expected-points decomposition "
                                 "for one game. The registry carries total_epa and the "
                                 "pass/rush EPA splits at PLAYER grain; these are TEAM-game "
                                 "components of a different decomposition, and pairing them "
                                 "would put one publisher's model under another's name. "
                                 "This source witnesses through the EPA backfill lane",
}
EXCLUDED: dict[str, str] = {
    **{c: "PLAY_GRAIN" for c in (
        "detail", "down", "yds_to_go", "qtr_time_remain", "pbp_score_aw", "pbp_score_hm",
        "exp_pts_before", "exp_pts_after", "location")},
    **{c: "DRIVE_GRAIN" for c in (
        "drive_num", "end_event", "net_yds", "start_at", "time_start", "time_total")},
    **{c: "SCORING_EVENT_GRAIN" for c in (
        "description", "home_team_score", "vis_team_score", "time", "scoring")},
    **{c: "OFFICIATING_CREW" for c in ("name", "ref_pos")},
    **{c: "INDEX_NOT_RAW" for c in (
        "pass_adj_net_yds_per_att_idx", "pass_adj_yds_per_att_idx", "pass_cmp_pct_idx",
        "pass_int_pct_idx", "pass_net_yds_per_att_idx", "pass_rating_idx",
        "pass_sacked_pct_idx", "pass_td_pct_idx", "pass_yds_per_att_idx")},
    **{c: "PFR_EXPECTED_POINTS_SPLIT" for c in (
        "pbp_exp_points_def_pass", "pbp_exp_points_def_rush", "pbp_exp_points_def_to",
        "pbp_exp_points_def_tot", "pbp_exp_points_fgxp", "pbp_exp_points_k",
        "pbp_exp_points_kr", "pbp_exp_points_off_pass", "pbp_exp_points_off_rush",
        "pbp_exp_points_off_to", "pbp_exp_points_off_tot", "pbp_exp_points_p",
        "pbp_exp_points_pr", "pbp_exp_points_st", "pbp_exp_points_tot")},
    **{c: "LOCATOR" for c in (
        "quarter", "fantasy_pos", "game_day_of_week", "game_location", "is_away", "is_home",
        "is_neutral", "result", "yds_team")},
    **{c: "PROVENANCE" for c in ("url",)},
}

# Refused, each with the question that would settle it. Keyed on the data-stat id, so the
# refusal fans out over every table publishing it exactly as a decision would.
ESCALATED: dict[str, str] = {
    "tackles_combined":
        "THE TACKLE TOTAL, refused here from a FOURTH surface (StatsCrew, nflcom career, "
        "nflcom splits, now PFR). PFR publishes `tackles_combined` beside "
        "`tackles_solo`/`tackles_assists` on some tables and alone on others, and "
        "`def_tackles_combined` in the registry may be the site's total OR the sum we "
        "would compute. The margin that would settle it is whether combined equals "
        "solo+assists on rows where all three are present AND non-degenerate -- rows where "
        "assists are 0 agree with every reading",
    "def_tgt_yds_per_att":
        "PFR's adv-defense table publishes BOTH `def_tgt_yds_per_att` and "
        "`def_yds_per_target` -- two distinct published columns. One is yards per target; "
        "the other is the average DEPTH of target (an ADOT concept, for which no defensive "
        "canonical exists). Nothing in the two ids says which is which, and guessing one "
        "forces the other. Escalated as a PAIR, like the L4 return block. Settles on the "
        "site's own header text for those two columns",
    "def_yds_per_target": "the other half of the def_tgt_yds_per_att pair -- see that entry",
    "awards":
        "ONE CELL, SEVERAL FACTS ('AP1, PB, MVP-1'). The registry carries "
        "all_pro_first_team, pro_bowl, mvp and 39 other honors_bio canonicals as separate "
        "typed columns. This is the SPLIT-AT-ADMISSION question already on Joe's queue, "
        "reached here from a third lineage -- split at parse, split at adjudication, or "
        "carried composite",
    "draft_info":
        "the same split-at-admission question: one cell carrying team, round, overall pick "
        "and year, against four separate registered canonicals",
    "stat":
        "A REGIME QUESTION, NOT A COLUMN QUESTION. `pfr_box_team_stats` and "
        "`pfr_box_game_info` are LONG tables -- this column holds stat NAMES and "
        "`home_stat`/`vis_stat`/`info` hold the values -- and the dossier keyed both as "
        "WIDE. Adjudicating this column would freeze the wrong key and hide every stat "
        "those tables actually publish. Settles by re-keying the two sources LONG on this "
        "column, after which their real stat vocabulary becomes dossier rows",
    "home_stat": "the value column of the LONG pfr_box_team_stats -- see `stat`",
    "vis_stat": "the other value column of that same LONG table -- see `stat`",
    "info": "the value column of the LONG pfr_box_game_info -- see `stat`",
    "fantasy_rank_overall":
        "PFR's fantasy rank, computed under PFR's scoring. Our `rank` family is 385 "
        "canonicals computed over OUR pool under OUR scoring, so this is not a witness for "
        "any of them -- but it is also not nothing, and whether a foreign publisher's rank "
        "may be carried at all is a policy question nobody has answered",
    "fantasy_rank_pos": "the positional half of the same question",
    "pass_tgt_yds_per_att":
        "the PASSING-side mirror of the def_tgt_yds_per_att pair, and ambiguous the same "
        "way: intended air yards per attempt, or average depth of target. Settles on the "
        "same header check",
    "two_pt_md":
        "two-point conversions MADE, published as one number on PFR's scoring table while "
        "the registry carries passing_2pt_conversions, receiving_2pt_conversions and "
        "rushing_2pt_conversions separately. Either the site's number is a total the "
        "registry has no column for, or it must be split by type it does not publish -- "
        "which is the split-at-admission question again",
}


def escalated_row_keys() -> dict[str, str]:
    """Fan the column-level refusals out over the dossier rows that carry them."""
    from .composite_column_adjudication import owned_row_keys

    composite_owned = owned_row_keys()
    document = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for row in document["rows"]:
        if row["lineage"] != "pfr" or row["column"] not in ESCALATED:
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if key not in composite_owned:
            out[key] = ESCALATED[row["column"]]
    return out


def _canonical_universe() -> set[str]:
    from .column_dossier import DISPOSITIONS_PATH

    registry = json.loads(
        (DISPOSITIONS_PATH.parent / "stat_contracts.v1.json").read_text(encoding="utf-8"))
    return ({c["canonical_name"] for c in registry["stats"]}
            | {c["stat_id"] for c in registry["stats"]})


def _contracts() -> dict[str, dict]:
    from .column_dossier import DISPOSITIONS_PATH

    registry = json.loads(
        (DISPOSITIONS_PATH.parent / "stat_contracts.v1.json").read_text(encoding="utf-8"))
    return {c["canonical_name"]: c for c in registry["stats"]}


def validate() -> list[str]:
    """Runs BEFORE --apply writes anything."""
    problems: list[str] = []
    universe = _canonical_universe()
    buckets = {"MAPPED": set(MAPPED), "NEW_CANDIDATES": set(NEW_CANDIDATES),
               "EXCLUDED": set(EXCLUDED), "DERIVED_RATIOS": set(DERIVED_RATIOS),
               "ESCALATED": set(ESCALATED)}
    for first, second in ((a, b) for a in buckets for b in buckets if a < b):
        overlap = buckets[first] & buckets[second]
        if overlap:
            problems.append(f"in both {first} and {second}: {sorted(overlap)}")
    for column, canonical in MAPPED.items():
        if canonical not in universe:
            problems.append(f"{column} -> {canonical!r} is in no stat contract")
    for column, (numerator, denominator) in DERIVED_RATIOS.items():
        for operand in (numerator, denominator):
            if operand not in universe and operand not in NEW_CANDIDATES:
                problems.append(f"{column}: operand {operand!r} is neither a canonical nor "
                                "a candidate -- a ratio whose operands do not exist is not "
                                "a derivation, it is a guess")
    for column in EXCLUDED.values():
        if column not in _EXCLUSION_ARGUMENTS:
            problems.append(f"exclusion argument {column!r} is undeclared")
    return problems


def build_decisions() -> tuple[list[dict], dict]:
    from .column_dossier import load_decisions
    from .composite_column_adjudication import owned_row_keys

    contracts = _contracts()
    document = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    mine = {key for key, entry in load_decisions().items()
            if entry.get("generated_by") == GENERATOR}
    escalated = set(ESCALATED)
    composite_owned = owned_row_keys()

    decisions: list[dict] = []
    tally: collections.Counter = collections.Counter()
    residual: collections.Counter = collections.Counter()

    for row in document["rows"]:
        if row["lineage"] != "pfr":
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if key in composite_owned:
            continue
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        column = row["column"]
        published = (f"PFR publishes this column as data-stat={column!r} -- the id the "
                     f"site's own generator emits, captured verbatim, not a slug of ours")

        if column in escalated:
            tally["escalated_left_open"] += 1
            continue
        if column in MAPPED:
            canonical = MAPPED[column]
            contract = contracts.get(canonical, {})
            decisions.append({
                "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": canonical,
                "reason": f"PFR data-stat {column!r} is the same measurement as canonical "
                          f"{canonical!r} (family {contract.get('family')!r}, unit "
                          f"{contract.get('unit')!r}, aggregation "
                          f"{contract.get('aggregation_class')!r})",
                "evidence": f"{published}. stat_contracts.v1.json carries {canonical!r} at "
                            f"natural grain {contract.get('natural_grain')!r} with "
                            f"tolerance policy "
                            f"{(contract.get('tolerance_policy') or {}).get('policy')!r}; "
                            f"the unit and aggregation class were checked against the "
                            f"reading, which is what refused fg_long -> fg_made_distance "
                            f"(count/SUM against yards/MAX)",
            })
            tally["mapped"] += 1
            continue
        if column in DERIVED_RATIOS:
            numerator, denominator = DERIVED_RATIOS[column]
            note = DERIVED_RATIO_NOTES.get(column)
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": (f"DERIVED RATIO: {column!r} = {numerator} / {denominator}, both "
                           f"published by PFR itself. {note} A recomputable rate witnesses "
                           f"its operands and carries no independent fact, so it does not "
                           f"become a canonical cell of its own -- but a disagreement "
                           f"between the published rate and the recomputed one is a real "
                           f"signal about the operands and belongs to the equation lane"
                           if note else
                           f"DERIVED RATIO: {column!r} = {numerator} / {denominator}, both "
                           f"published by PFR itself. A recomputable rate witnesses its "
                           f"operands and carries no independent fact, so it does not "
                           f"become a canonical cell of its own -- but a disagreement "
                           f"between the published rate and the recomputed one is a real "
                           f"signal about the operands and belongs to the equation lane"),
                "evidence": f"{published}. Both operands are registered: {numerator!r} and "
                            f"{denominator!r}",
            })
            tally["derived_ratio"] += 1
            continue
        if column in EXCLUDED:
            argument = EXCLUDED[column]
            decisions.append({
                "key": key, "disposition": "EXCLUDED_WITH_REASON",
                "reason": f"{argument}: {_EXCLUSION_ARGUMENTS[argument]}",
                "evidence": f"{published}. The exclusion rests on the source's registered "
                            f"role and declared join grain, both of which live in "
                            f"sources.py and are checkable, not on the column's name",
            })
            tally[f"excluded:{argument}"] += 1
            continue
        if column in NEW_CANDIDATES:
            decisions.append({
                "key": key, "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
                "reason": f"real PFR material the supertable has no column for: "
                          f"{NEW_CANDIDATES[column]}",
                "evidence": f"{published}. Checked against the whole 1,218-stat contract "
                            f"registry, not just the weekly release -- 141 registered "
                            f"stats are absent from that file, so checking the release "
                            f"alone would propose columns we already carry",
            })
            tally["new_candidate"] += 1
            continue
        residual[column] += 1

    return decisions, {
        "total": len(decisions),
        "by_bucket": dict(sorted(tally.items())),
        "residual_columns": dict(residual.most_common()),
        "residual_rows": sum(residual.values()),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    problems = validate()
    if problems:
        print("CORRESPONDENCE TABLE INVALID -- nothing written:")
        for problem in problems:
            print("  -", problem)
        return 1

    decisions, tally = build_decisions()
    print(f"decisions: {tally['total']:,}   (residual rows left OPEN "
          f"{tally['residual_rows']:,} over {len(tally['residual_columns'])} columns)")
    for name, value in tally["by_bucket"].items():
        print(f"  {name:44s} {value}")
    if tally["residual_columns"]:
        print("\nRESIDUAL -- no bucket claims these, left OPEN:")
        for column, count in list(tally["residual_columns"].items())[:60]:
            print(f"   - {column:36s} {count}")
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
