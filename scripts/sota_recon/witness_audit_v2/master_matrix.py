"""master_matrix.py -- the column x witness master matrix with derivation-aware verdicts.

Merges:
  super_column_census.json      (every column, weekly + season/career artifacts)
  witness_contracts_v2.json     (every witness atom, era, grain -- exhaustive v2 scan)
  LINEAGE RULES                 (encoded from the v26 build-script lineage extraction)

Verdicts per column:
  meta                -- identity/provenance; no witness needed
  direct              -- a witness carries the atom itself (list witnesses, grains, eras)
  derived_witnessed   -- computed; EVERY leaf input atom is direct-witnessed
  derived_partial     -- computed; some leaf inputs witnessed, some not
  derived_unwitnessed -- computed; NO leaf input witnessed
  UNMAPPED            -- no witness, no lineage rule -> manual review required (the residual)

Matching is exact (raw name, ATOM_MAP canonical, or a REVIEWED_ALIASES entry). Fuzzy
candidates are PRINTED for human review, never silently accepted.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, r"d:\yahoo_oauth")
from scripts.sota_recon.witness_contracts import ATOM_MAP  # noqa: E402

SCRATCH = Path(r"D:\league-history-data\nfl\derived\validation\witness_audit_2026_07_16")
CENSUS = json.loads((SCRATCH / "super_column_census.json").read_text())
V2 = json.loads((SCRATCH / "witness_contracts_v2.json").read_text())
OUT = SCRATCH / "master_witness_matrix.json"

CANON_TABLES = ["weekly", "player_nfl_season", "player_nfl_season_all",
                "player_nfl_career", "player_nfl_career_all"]

# ---------------------------------------------------------------- witness atom index
# atom -> list of {witness, grain, era_min, era_max, cls}
ATOM_INDEX: dict[str, list[dict]] = {}
for wname, c in V2["contracts"].items():
    if c.get("error") or c.get("cls") == "subject_history":
        continue
    for atom, info in c.get("atoms", {}).items():
        ATOM_INDEX.setdefault(atom, []).append({
            "witness": wname, "grain": c["grain"], "cls": c["cls"],
            "era_min": info.get("era_min"), "era_max": info.get("era_max"),
            "col": info.get("col")})

# super-col -> witness-atom aliases REVIEWED against the v2 scan output (exact, curated).
# (filled after inspecting the v2 atom names; start with the certain ones)
REVIEWED_ALIASES: dict[str, list[str]] = {
    # NGS ingest columns keep the witness name minus the ngs_ prefix
    **{f"ngs_{a}": [a] for a in (
        "avg_cushion", "avg_separation", "avg_yac", "avg_expected_yac",
        "avg_yac_above_expectation", "pct_share_intended_air_yards", "rush_efficiency",
        "pct_att_gte_8_defenders", "avg_time_to_los", "expected_rush_yards",
        "rush_yards_over_expected", "rush_pct_over_expected", "avg_time_to_throw",
        "aggressiveness", "avg_air_yards_to_sticks", "expected_completion_pct",
        "completion_pct_above_expectation", "avg_air_yards_differential")},
    "team_points": ["team_points"], "opponent_points": ["opponent_points"],
    "points_allowed": ["opponent_points"],
    # PFR advanced box/season witnesses (verified atom names from the v2 scan)
    "passing_completed_air_yards": ["pass_air_yds"],
    "receiving_completed_air_yards": ["rec_air_yds"],
    "passing_blitzed": ["pass_blitzed"], "passing_drops": ["pass_drops"],
    "passing_hits": ["pass_hits"], "passing_hurried": ["pass_hurried"],
    "passing_poor_throws": ["pass_poor_throws"], "passing_pressured": ["pass_pressured"],
    "passing_yards_after_catch": ["pass_yac", "yards_after_catch"],
    "receiving_yards_after_catch": ["rec_yac", "yards_after_catch"],
    "receiving_broken_tackles": ["rec_broken_tackles"], "receiving_drops": ["rec_drops"],
    "receiving_adot": ["rec_adot"], "receiving_target_interceptions": ["rec_target_int"],
    "receiving_pass_rating": ["rec_pass_rating"],
    "rushing_broken_tackles": ["rush_broken_tackles"],
    "rushing_yards_before_contact": ["rush_yds_before_contact"],
    "rushing_yards_after_contact": ["rush_yac"],
    "rushing_scrambles": ["rush_scrambles", "qb_scramble"],
    "def_blitzes": ["blitzes"], "def_pressures": ["pressures"], "def_hurries": ["qb_hurry"],
    "def_knockdowns": ["qb_knockdown"], "def_tackles_missed": ["tackles_missed"],
    "def_targets_allowed": ["def_targets"], "def_completions_allowed": ["def_cmp"],
    "def_completion_yards_allowed": ["def_cmp_yds"], "def_completion_tds_allowed": ["def_cmp_td"],
    "def_yards_after_catch_allowed": ["def_yac"], "def_passer_rating_allowed": ["def_pass_rating"],
    "def_air_yards_allowed": ["def_air_yds"],
    # snap-count witnesses
    "offense_snaps": ["offense"], "defense_snaps": ["defense"], "special_teams_snaps": ["special_teams"],
    # pbp per-play flag/value columns as derivation witnesses
    "passing_cpoe": ["cpoe", "passing_cpoe_sum"],
    "timeouts": ["timeout"],
    # PFR player-page value/rate columns
    "w_av": ["av"], "seasons_started": ["gs"], "games_started": ["gs"],
    "completion_pct": ["pass_cmp_pct"], "passing_int_pct": ["pass_int_pct"],
    "passing_td_pct": ["pass_td_pct"], "sack_pct": ["pass_sacked_pct"],
    "yards_per_attempt": ["pass_yds_per_att"],
    "yards_per_carry": ["rush_yds_per_att"], "rushing_yards_per_carry": ["rush_yds_per_att"],
    "yards_per_reception": ["rec_yds_per_rec"], "yards_per_target": ["rec_yds_per_tgt"],
    "receiving_yards_per_target": ["rec_yds_per_tgt"],
    "comp_pct": ["pass_cmp_pct"], "forty": ["forty_yd"], "bench": ["bench_reps"],
}

# meta / provenance patterns (no witness required)
META_PAT = re.compile(
    r"(_repaired_at_\d+|_populated_at_\d+|_merged_at_\d+|_recomputed_at_\d+)$"
    r"|^(recon_correction_log|data_source|player_week|NFL_player_id|player|year|week|season_type"
    r"|team|opponent|nfl_team|opponent_nfl_team|position|nfl_position|primary_position|starter_position"
    r"|fantasy_position|age|headshot_url|gsis_id|pfr_id|boxscore_id|game_date|home_away"
    r"|nfl_franchise_number|opponent_nfl_franchise_number|opponent_nfl_franchise_number"
    r"|first_name|last_name|full_name|birth_date|college|conference|draft_year|draft_round|draft_pick"
    r"|draft_overall|nfl_draft_team|is_undrafted|rookie_year|high_school|birth_place|position_candidates"
    r"|years_active|years_active_1|hof|games|games_played|managers|years|season_number|is_rookie"
    r"|season_positions|career_positions|dal_dtx_collision_repaired_at_20260503|def_identity_repaired_at_20260504)$"
    r"|_id$|_url$|_name$|_key$|_date$|_slug$")

# ---------------------------------------------------------------- lineage recipe sets
OFF = ["passing_yards", "passing_tds", "passing_interceptions", "rushing_yards", "rushing_tds",
       "receiving_yards", "receiving_tds", "receptions", "fumbles_lost", "rushing_fumbles_lost",
       "sack_fumbles_lost", "receiving_fumbles_lost", "passing_2pt_conversions",
       "rushing_2pt_conversions", "receiving_2pt_conversions", "special_teams_tds", "fum_ret_td",
       "pts_k_std", "rushing_first_downs", "receiving_first_downs"]
KREC = ["fg_made", "fg_att", "fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49",
        "fg_made_50_59", "fg_made_60_", "pat_made", "pat_att"]
DEFREC = ["def_sacks", "def_interceptions", "fum_rec", "def_int_ret_td", "fum_ret_td", "def_tds",
          "def_safeties", "def_blk_kick", "punts_blocked", "dst_points_allowed",
          "special_teams_tds", "kickoff_return_yards", "punt_return_yards",
          "three_out", "fourth_down_stop", "def_tackles_for_loss"]
IDPREC = ["def_tackles_solo", "def_tackle_assists", "def_tackles_with_assist", "def_sacks",
          "def_interceptions", "def_fumbles_forced", "fum_rec", "def_tackles_for_loss",
          "def_pass_defended", "def_qb_hits", "def_safeties", "def_int_ret_td", "fum_ret_td",
          "def_tds", "def_blk_kick", "def_interception_yards", "fumble_recovery_yards_own",
          "fumble_recovery_yards_opp", "fum_rec_yds"]
FPTS = [f"fpts_{t}pt_{p}" for t in (4, 5, 6) for p in ("0ppr", "half", "ppr", "tep", "ppfd")]
RANK_POINTS = FPTS + ["pts_k_std", "pts_k_yds", "pts_def_std", "pts_idp_std", "pts_idp_premium",
                      "pts_idp_tackle_heavy", "pts_idp_big_play"]

PTS_TOKENS = [  # ordered granular pts_* token -> inputs
    (r"^pts_pass_yd", ["passing_yards"]), (r"^pts_pass_td", ["passing_tds"]),
    (r"^pts_pass_int", ["passing_interceptions"]), (r"^pts_pass_cmp", ["completions"]),
    (r"^pts_pass_fd", ["passing_first_downs"]), (r"^pts_pass_2pt", ["passing_2pt_conversions"]),
    (r"^pts_pass_(4|5|6)pt", ["passing_yards", "passing_tds", "passing_interceptions"]),
    (r"^pts_rush_yd", ["rushing_yards"]), (r"^pts_rush_td", ["rushing_tds"]),
    (r"^pts_rush_att", ["carries"]), (r"^pts_rush_fd", ["rushing_first_downs"]),
    (r"^pts_rush_2pt", ["rushing_2pt_conversions"]), (r"^pts_rush$", ["rushing_yards", "rushing_tds"]),
    (r"^pts_rec_yd", ["receiving_yards"]), (r"^pts_rec_td", ["receiving_tds"]),
    (r"^pts_rec_fd", ["receiving_first_downs"]), (r"^pts_rec_2pt", ["receiving_2pt_conversions"]),
    (r"^pts_rec_te_bonus", ["receptions"]), (r"^pts_rec_40plus", ["receptions_40plus"]),
    (r"^pts_rush_40plus", ["rushing_40plus"]),
    (r"^pts_rec_(0ppr|half|ppr|tep|1|p5)", ["receptions", "receiving_yards", "receiving_tds"]),
    (r"^pts_fum", ["fumbles_lost", "rushing_fumbles_lost", "sack_fumbles_lost", "receiving_fumbles_lost", "fum_ret_td"]),
    (r"^pts_pick6", ["pick6"]), (r"^pts_sack_taken", ["sacks_suffered"]),
    (r"^pts_st_td", ["special_teams_tds"]), (r"^pts_kr_yd", ["kickoff_return_yards"]),
    (r"^pts_pr_yd", ["punt_return_yards"]), (r"^pts_ret_yds", ["kickoff_return_yards", "punt_return_yards"]),
    (r"^pts_first_downs", ["rushing_first_downs", "receiving_first_downs"]),
    (r"^pts_misc", ["passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions", "special_teams_tds", "fum_ret_td"]),
]

RULES: list[tuple[str, str, list[str]]] = [
    ("scoring_fpts_ret", r"^fpts_(4|5|6)pt_(0ppr|half|ppr|tep)_ret$", OFF + ["kickoff_return_yards", "punt_return_yards", "pts_def_std"]),
    ("scoring_fpts", r"^fpts_(4|5|6)pt_(0ppr|half|ppr|tep|ppfd)$", OFF + ["pts_def_std"]),
    ("scoring_fpts_idp", r"^fpts_idp$", OFF + ["pts_idp_std"]),
    ("scoring_legacy", r"^fantasy_points_ppr$", OFF),
    ("kicker_pts", r"^pts_k", KREC),
    ("dst_team_pts", r"^pts_def_team", ["team_points", "game_margin"]),
    ("dst_pts", r"^pts_def", DEFREC),
    ("dst_pa_tiers", r"^pts_allow", ["dst_points_allowed"]),
    ("dst_ya_tiers", r"^yds_allow", ["total_yds_allowed"]),
    ("idp_pts", r"^pts_idp", IDPREC),
    ("rank_weekly", r"^rank_(?!season_|alltime_)", RANK_POINTS),
    ("rank_denorm", r"^rank_(season|alltime)_", RANK_POINTS),
    ("research_lamar", r"^lamar_", FPTS + ["pts_k_std", "pts_k_yds", "pts_def_std", "pts_idp_std"]),
    ("ppg_family", r"^(ppg_|rolling_|weighted_ppg_|consistency_|avg_pts_next_year)", FPTS + ["pts_def_std", "pts_k_yds"]),
    ("bonus_flags", r"^bonus_", ["passing_yards", "completions", "rushing_yards", "carries", "receiving_yards", "receptions"]),
    ("allowed_mirror", r"^(passing|rushing|receiving)_(yds|tds)_allowed$", ["passing_yards", "passing_tds", "rushing_yards", "rushing_tds", "receiving_yards", "receiving_tds"]),
    ("allowed_mirror2", r"^(def_completions_allowed|def_completion_yards_allowed|def_completion_tds_allowed|def_sack_yards|total_yds_allowed)$", ["completions", "passing_yards", "passing_tds", "sack_yards_lost", "rushing_yards", "receiving_yards"]),
    ("dst_return", r"^dst_return_yards$", ["kickoff_return_yards", "punt_return_yards"]),
    ("composite", r"^(touches|total_touches)$", ["carries", "receptions"]),
    ("composite", r"^opportunities$", ["carries", "targets"]),
    ("composite", r"^turnovers$", ["passing_interceptions", "fumbles_lost"]),
    ("composite", r"^(scrimmage_yards|yds_from_scrimmage)$", ["rushing_yards", "receiving_yards"]),
    ("composite", r"^(scrimmage_tds|rush_receive_td)$", ["rushing_tds", "receiving_tds"]),
    ("composite", r"^(total_tds_scored|total_tds_accounted_for)$", ["rushing_tds", "receiving_tds", "kickoff_return_tds", "punt_return_tds", "fum_ret_td", "def_int_ret_td"]),
    ("composite", r"^total_return_yards$", ["kickoff_return_yards", "punt_return_yards"]),
    ("composite", r"^all_purpose_yards$", ["rushing_yards", "receiving_yards", "kickoff_return_yards", "punt_return_yards", "def_interception_yards", "fum_rec_yds"]),
    ("composite", r"^total_points_scored$", ["rushing_tds", "receiving_tds", "fg_made", "pat_made", "def_safeties"]),
    ("composite", r"^dropbacks$", ["attempts", "sacks_suffered"]),
    ("rate", r"^yards_per_touch$", ["rushing_yards", "receiving_yards", "carries", "receptions"]),
    ("rate", r"^punt_yards_per_punt$", ["punts", "punt_yards"]),
    ("rate", r"^passing_(adjusted_yards|net_yards|adjusted_net_yards)_per_attempt$", ["passing_yards", "passing_tds", "passing_interceptions", "attempts", "sacks_suffered", "sack_yards_lost"]),
    ("rate", r"^(adjusted_yards|net_yards|adjusted_net_yards)_per_attempt$", ["passing_yards", "passing_tds", "passing_interceptions", "attempts", "sacks_suffered", "sack_yards_lost"]),
    ("rate", r"^passing_yards_per_attempt$", ["passing_yards", "attempts"]),
    ("rate", r"^receiving_yards_per_reception$", ["receiving_yards", "receptions"]),
    ("rate", r"^(passer_rating|rate)$", ["completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions"]),
    ("rate", r"^catch_rate$", ["receptions", "targets"]),
    ("rate", r"^(fg%|xp%)$", ["fg_made", "fg_att", "pat_made", "pat_att"]),
    ("rate", r"^(pacr)$", ["passing_yards", "passing_air_yards"]),
    ("rate", r"^(racr)$", ["receiving_yards", "receiving_air_yards"]),
    ("rate", r"^(wopr)$", ["targets", "receiving_air_yards"]),
    ("rate", r"^(receiving_adot|adot)$", ["receiving_air_yards", "targets"]),
    ("team_norm", r"^air_yards_share$", ["receiving_air_yards", "passing_air_yards"]),
    ("legacy_dup", r"^fgm$", ["fg_made"]),
    ("legacy_dup", r"^sfty$", ["def_safeties"]),
    ("kick_detail", r"^(fg_yards|fg_yards_canonical|fg_yds_over_30|fg_yards_over_30_canonical|fg_made_distance|fg_missed_distance|fg_blocked_distance|fg_made_60plus|fg_made_60_plus_canonical|gwfg_att|gwfg_made|gwfg_missed|gwfg_blocked|gwfg_distance|fg_missed_0_19|fg_missed_20_29|fg_missed_30_39|fg_missed_40_49|fg_missed_50_59|fg_missed_60_|fg_missed|pat_missed|pat_blocked|fg_blocked)$", ["fg_made", "fg_att", "pat_made", "pat_att", "fg_long"]),
    ("context_game", r"^(is_win|game_margin|is_overtime|team_points|opponent_points)$", ["team_points", "opponent_points"]),
    ("career_finish", r"^(best_pos_finish|seasons_pos_top\d+|best_overall_finish_ppr|seasons_overall_top\d+_ppr)$", RANK_POINTS),
    ("season_ctx", r"^wins$", ["team_points", "opponent_points"]),
    ("composite", r"^total_epa$", ["passing_epa", "rushing_epa", "receiving_epa"]),
    ("composite", r"^total_wpa$", ["passing_wpa", "rushing_wpa", "receiving_wpa"]),
    ("composite", r"^2pm$", ["passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"]),
    ("composite", r"^pts$", ["rushing_tds", "receiving_tds", "fg_made", "pat_made", "def_safeties"]),
    ("composite", r"^def_tackles_combined$", ["def_tackles_solo", "def_tackle_assists"]),
    ("dst_pa_recon", r"^dst_points_allowed$", ["points_allowed", "def_tds", "def_safeties"]),
    ("rate", r"^pat_pct$", ["pat_made", "pat_att"]),
    ("rate", r"^rec_td_pct$", ["receiving_tds", "targets"]),
    ("rate", r"^rush_td_pct$", ["rushing_tds", "carries"]),
    ("rate", r"^target_share$", ["targets"]),
    ("rate", r"^(offense|defense|special_teams)_snap_pct$", ["offense_snaps", "defense_snaps", "special_teams_snaps"]),
]

# columns whose values are PARSED from witness text/logs (scoring descriptions, pbp detail,
# drive events) or attributed via witness id-columns -- witnessed by construction, but the
# witness does not carry the atom as a named numeric column. col -> (witness list, note)
TEXT_DERIVED: dict[str, tuple[list[str], str]] = {
    "three_out": (["pfr_box:home_drives", "pfr_box:vis_drives", "pfr_box:pbp"], "drive end_event parse (1998+ drives; 1978-97 pbp reconstruction)"),
    "fourth_down_stop": (["pfr_box:home_drives", "pfr_box:vis_drives", "pfr_box:pbp"], "drive end_event parse"),
    "gwfg_att": (["pfr_box:scoring"], "scoring-log game-winning FG parse"),
    "gwfg_made": (["pfr_box:scoring"], "scoring-log game-winning FG parse"),
    "gwfg_missed": (["pfr_box:scoring"], "scoring-log game-winning FG parse"),
    "gwfg_blocked": (["pfr_box:scoring"], "scoring-log game-winning FG parse"),
    "gwfg_distance": (["pfr_box:scoring"], "scoring-log game-winning FG parse"),
    "penalties": (["pfr_box:pbp", "pfr_box:team_stats"], "pbp detail '(accepted)' parse; team-line Penalties-Yards witness"),
    "penalty_yards": (["pfr_box:pbp", "pfr_box:team_stats"], "pbp detail parse"),
    "pick6": (["pbp_merged"], "interception+return-TD attributed to passer"),
    "special_teams_tackles_solo": (["pbp_merged"], "ST tackle attribution ids"),
    "receptions_0_4": (["pfr_box:pbp"], "completion-yardage bucket parse"),
    "receptions_5_9": (["pfr_box:pbp"], "completion-yardage bucket parse"),
    "receptions_10_19": (["pfr_box:pbp"], "completion-yardage bucket parse"),
    "receptions_20_29": (["pfr_box:pbp"], "completion-yardage bucket parse"),
    "receptions_30_39": (["pfr_box:pbp"], "completion-yardage bucket parse"),
    "receptions_40plus": (["pfr_box:pbp"], "completion-yardage bucket parse"),
    "fg_made_0_19": (["pfr_box:scoring", "pfr_season:kicking"], "FG distance parse; season kicking buckets witness"),
    "fg_made_20_29": (["pfr_box:scoring", "pfr_season:kicking"], "FG distance parse"),
    "fg_made_30_39": (["pfr_box:scoring", "pfr_season:kicking"], "FG distance parse"),
    "fg_made_40_49": (["pfr_box:scoring", "pfr_season:kicking"], "FG distance parse"),
    "fg_made_50_59": (["pfr_box:scoring", "pfr_season:kicking"], "FG distance parse"),
    "fg_made_60_": (["pfr_box:scoring", "pfr_season:kicking"], "FG distance parse"),
    # fumble SPLITS: attributed from PBP plays (play-type + fumbler id; lost via recovery team,
    # flag complete 1999+, partial 1978-98) and PFR pbp text parse 1978-97. Not a named witness atom.
    **{c: (["pbp_merged", "pfr_box:pbp"], "pbp attribution: play-type + fumbled_1_player_id; lost flag 1999+, recovery-team derivation before")
       for c in ("rushing_fumbles", "rushing_fumbles_lost", "receiving_fumbles",
                 "receiving_fumbles_lost", "sack_fumbles", "sack_fumbles_lost")},
    "rushing_40plus": (["pbp_merged"], "rush plays >=40yd count"),
    "rushing_tds_40plus": (["pbp_merged"], "rush TD >=40yd count"),
    "rushing_tds_50plus": (["pbp_merged"], "rush TD >=50yd count"),
    "def_tackles_for_loss_yards": (["pbp_merged"], "TFL yards via tackle attribution ids"),
    "misc_yards": (["pbp_merged"], "misc return yardage (missed-FG/blocked-kick returns etc.); 535 nonzero rows 1999-2025, pbp-derivable"),
    "def_fumbles": (["pfr_box:team_stats"], "opponent team-line Fumbles-Lost (long-format stat rows, 1920+)"),
    "fg_missed_0_19": (["pfr_box:scoring", "pfr_box:kicking"], "missed-FG distance parse"),
    "fg_missed_20_29": (["pfr_box:scoring", "pfr_box:kicking"], "missed-FG distance parse"),
    "fg_missed_30_39": (["pfr_box:scoring", "pfr_box:kicking"], "missed-FG distance parse"),
    "fg_missed_40_49": (["pfr_box:scoring", "pfr_box:kicking"], "missed-FG distance parse"),
    "fg_missed_50_59": (["pfr_box:scoring", "pfr_box:kicking"], "missed-FG distance parse"),
    "fg_missed_60_": (["pfr_box:scoring", "pfr_box:kicking"], "missed-FG distance parse"),
    # identifiability corrections (2026-07-16 review): these columns need EVENT-level or
    # catalog-flag inputs — the earlier formula-rule lineage understated/overclaimed them.
    "is_overtime": (["team_games_all"], "catalog OT flag — NOT derivable from final score"),
    "home_away": (["team_games_all"], "catalog is_home/is_away flags"),
    "fg_yards": (["pfr_box:scoring"], "event-level made-FG distances (scoring-log parse)"),
    "fg_yards_canonical": (["pfr_box:scoring"], "event-level distances"),
    "fg_yds_over_30": (["pfr_box:scoring"], "event-level distances"),
    "fg_yards_over_30_canonical": (["pfr_box:scoring"], "event-level distances"),
    "fg_made_distance": (["pfr_box:scoring"], "event-level distances"),
    "fg_missed_distance": (["pfr_box:scoring", "pfr_box:kicking"], "event-level distances"),
    "fg_blocked_distance": (["pfr_box:scoring"], "event-level distances"),
    "fg_made_60_plus_canonical": (["pfr_box:scoring"], "event COUNT of 60+ makes — fg_long alone proves >=1, never how many"),
    "fg_made_60plus": (["pfr_box:scoring", "pbp_merged"], "event count of 60+ makes"),
    "gwfg_distance": (["pfr_box:scoring"], "event-level distance of the GW kick"),
    "dst_points_allowed": (["team_games_all", "pfr_box:scoring"], "PA minus non-defensive concessions via score-delta event attribution (build_pa_reconciled) — final score + def TDs alone are INSUFFICIENT"),
    "offense_snap_pct": (["pfr_season:snap_counts", "pfr_box:home_snap_counts", "pfr_box:vis_snap_counts"], "witness pct columns are %-strings (v2 scan drops %); share-of-team needs the team snap denominator, NOT the player's phase split"),
    "defense_snap_pct": (["pfr_season:snap_counts", "pfr_box:home_snap_counts", "pfr_box:vis_snap_counts"], "same"),
    "special_teams_snap_pct": (["pfr_season:snap_counts", "pfr_box:home_snap_counts", "pfr_box:vis_snap_counts"], "same"),
    # honors/awards: the selection IS row-membership on the honors/voting pages (not a stat col)
    **{c: (["pfr_season:all_pro", "pfr_context:pro_bowl", "pfr_context:voting_pages"], "row membership on PFR honors/voting pages")
       for c in ("all_pro_first_team", "all_pro_second_team", "pro_bowl", "mvp", "opoy", "dpoy",
                 "oroy", "droy", "cpoy", "career_all_pro_first", "career_all_pro_second",
                 "career_pro_bowls", "career_mvps", "career_opoy", "career_dpoy", "career_oroy",
                 "career_droy", "career_cpoy", "probowls", "allpro")},
}

# columns already covered by build lineage but with no external-witness leaves (internal QA cols etc.)
EXPLICIT: dict[str, tuple[str, list[str]]] = {}


def direct_hits(col: str) -> list[dict]:
    hits = []
    names = {col, ATOM_MAP.get(col, col)}
    names.update(REVIEWED_ALIASES.get(col, []))
    for n in names:
        hits.extend(ATOM_INDEX.get(n, []))
    return hits


_resolving: set[str] = set()
_memo: dict[str, dict] = {}
ALL_COLS: set[str] = set()
for t in CANON_TABLES:
    if t in CENSUS:
        ALL_COLS.update(CENSUS[t]["columns"].keys())


def resolve(col: str) -> dict:
    """Verdict dict for a column (memoized, cycle-safe)."""
    if col in _memo:
        return _memo[col]
    if col in _resolving:  # cycle -> treat as unmatched leaf
        return {"verdict": "UNMAPPED", "witnesses": [], "rule": "cycle"}
    _resolving.add(col)
    try:
        if META_PAT.search(col) or col in ("last_updated", "nfl_team_count", "nfl_teams", "latest_team",
                                           "first_year", "last_year", "age_at_draft"):
            r = {"verdict": "meta", "witnesses": [], "rule": "meta"}
        else:
            hits = direct_hits(col)
            if not hits and col in TEXT_DERIVED:
                wl, note = TEXT_DERIVED[col]
                r = {"verdict": "text_derived", "witnesses": wl, "rule": "text_derived", "note": note,
                     "n_primary": len(wl), "game_era": None, "season_era": None}
                _memo[col] = r
                _resolving.discard(col)
                return r
            if hits:
                prim = [h for h in hits if h["cls"] == "primary"]
                r = {"verdict": "direct",
                     "witnesses": sorted({h["witness"] for h in hits}),
                     "n_primary": len({h["witness"] for h in prim}),
                     "game_era": min((h["era_min"] for h in hits if h["grain"] == "game" and h["era_min"]), default=None),
                     "season_era": min((h["era_min"] for h in hits if h["grain"] in ("season", "season_post") and h["era_min"]), default=None),
                     "rule": "direct"}
            else:
                rule_hit = None
                if col in EXPLICIT:
                    rule_hit = EXPLICIT[col]
                else:
                    for name, pat, inputs in RULES:
                        if re.search(pat, col):
                            rule_hit = (name, inputs)
                            break
                    if rule_hit is None and col.startswith("pts_"):
                        for pat, inputs in PTS_TOKENS:
                            if re.search(pat, col):
                                rule_hit = ("pts_granular", inputs)
                                break
                        if rule_hit is None:
                            rule_hit = ("pts_unclassified", [])
                if rule_hit is None:
                    r = {"verdict": "UNMAPPED", "witnesses": [], "rule": None}
                else:
                    name, inputs = rule_hit
                    leaf_w, leaf_unw = set(), set()
                    for i in inputs:
                        sub = resolve(i)
                        if sub["verdict"] in ("direct", "text_derived"):
                            leaf_w.add(i)
                        elif sub["verdict"] in ("derived_witnessed",):
                            leaf_w.add(i + "*")
                        elif sub["verdict"] in ("derived_partial",):
                            leaf_w.add(i + "~")
                            leaf_unw.add(i + "~")
                        else:
                            leaf_unw.add(i)
                    if inputs and not leaf_unw:
                        v = "derived_witnessed"
                    elif leaf_w:
                        v = "derived_partial"
                    else:
                        v = "derived_unwitnessed"
                    r = {"verdict": v, "rule": name, "inputs": inputs,
                         "witnessed_inputs": sorted(leaf_w), "unwitnessed_inputs": sorted(leaf_unw)}
    finally:
        _resolving.discard(col)
    _memo[col] = r
    return r


def main():
    tables_out = {}
    for t in CANON_TABLES:
        if t not in CENSUS:
            continue
        cols = CENSUS[t]["columns"]
        out = {}
        for col, m in cols.items():
            r = dict(resolve(col))
            r["family"] = m["family"]
            out[col] = r
        tables_out[t] = out

    wk = tables_out["weekly"]
    counts = {}
    for col, r in wk.items():
        counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
    print("WEEKLY verdicts:", json.dumps(counts, indent=1))

    unmapped = sorted(c for c, r in wk.items() if r["verdict"] == "UNMAPPED")
    print(f"\nUNMAPPED weekly cols ({len(unmapped)}):")
    for c in unmapped:
        print("   ", c)
    partial = sorted(c for c, r in wk.items() if r["verdict"] in ("derived_partial", "derived_unwitnessed"))
    print(f"\nderived_partial/unwitnessed weekly ({len(partial)}):")
    for c in partial:
        print("   ", c, "->", wk[c].get("unwitnessed_inputs"))

    # season/career-only columns (not on weekly)
    for t in ("player_nfl_season", "player_nfl_career"):
        extra = sorted(c for c in tables_out[t] if c not in wk)
        bad = [c for c in extra if tables_out[t][c]["verdict"] in ("UNMAPPED", "derived_unwitnessed", "derived_partial")]
        print(f"\n{t}: {len(extra)} cols not on weekly; unresolved: {len(bad)}")
        for c in bad:
            print("   ", c, tables_out[t][c]["verdict"])

    # ---------------- reverse ledger: witnessed atoms with NO super column
    used_atoms = set()
    for t in tables_out.values():
        for col, r in t.items():
            if r["verdict"] == "direct":
                used_atoms.add(col)
                used_atoms.add(ATOM_MAP.get(col, col))
                used_atoms.update(REVIEWED_ALIASES.get(col, []))
    unused = {}
    for atom, hits in ATOM_INDEX.items():
        if atom in used_atoms or atom in ALL_COLS:
            continue
        prim = [h for h in hits if h["cls"] == "primary"]
        if not prim:
            continue
        unused[atom] = {"witnesses": sorted({h['witness'] for h in prim}),
                        "game_era": min((h["era_min"] for h in prim if h["grain"] == "game" and h["era_min"]), default=None),
                        "season_era": min((h["era_min"] for h in prim if h["grain"] in ("season", "season_post") and h["era_min"]), default=None)}
    print(f"\nWITNESSED ATOMS WITH NO SUPER COLUMN: {len(unused)} (candidate adds; includes witness-internal junk to curate)")

    # fuzzy suggestions for UNMAPPED (print-only, for reviewed-alias baking)
    atoms_l = {a.lower(): a for a in ATOM_INDEX}
    print("\nFUZZY SUGGESTIONS (review; never auto-accepted):")
    for c in unmapped:
        cand = set()
        base = c.lower()
        for probe in {base, base.removeprefix("def_"), base.removeprefix("passing_"), base.removeprefix("receiving_"),
                      base.removeprefix("rushing_"), base.replace("def_", ""), base + "s", base.rstrip("s")}:
            if probe in atoms_l:
                cand.add(atoms_l[probe])
        if cand:
            print(f"    {c} ?= {sorted(cand)}")

    OUT.write_text(json.dumps({"tables": tables_out, "unused_witness_atoms": unused,
                               "weekly_verdict_counts": counts}, indent=1, sort_keys=True))
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
