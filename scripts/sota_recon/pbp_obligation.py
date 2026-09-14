"""THE PBP OBLIGATION (Joe, 2026-08-01): pbp witnesses BY DEFAULT.

"From now on, for pbp NOT to witness you need a damn good reason -- after we
extracted all the grammar -- why it CAN'T witness."

Every witnessable weekly column is in exactly one of three states:
  WITNESSED -- a pbp-lineage spec exists in WITNESS_MAP;
  CANNOT    -- a receipted reason from the fixed classes below;
  OWED      -- pbp CAN derive it and a spec is debt. Shrink-only: the queue may
               only get smaller, and every entry names the machinery it waits on.

'Machinery not built yet' is OWED, never CANNOT. The test enforces total cover
and the shrink-only rule; a new column that arrives unclassified fails the gate.

Scope floor 1978 (pbp_merged); 2pt concepts 1994 (the rule's birth year).
"""
from __future__ import annotations

PBP_SOURCES = {"pbp_merged_1978_2025", "pbp_player_week_rollup",
               "pbp_team_defense", "ancient_pbp1978_recovery"}

# ---- CANNOT: the only legal reason classes ------------------------------------
NGS_TRACKING = ("player-tracking measurement (Next Gen Stats); no play-by-play "
                "event carries it")
PFR_CHARTING = ("film-charting judgment (PFR advanced); not in the pbp event "
                "vocabulary")
PARTICIPATION = ("snap/lineup participation; pbp records plays, not who stood "
                 "on the field for them")
# RATE_VIA_COMPONENTS DISSOLVED 2026-08-01 (Joe: 'why is completion percentage
# not mapped to pbp?') -- a ratio of witnessed components is still pbp-computable
# and an ILLEGAL cannot. Five rates specced immediately (pbp_ratio shape,
# passing_cpoe VALIDATED 96.7%); the rest moved to OWED as debt.
LEAGUE_FORMULA = "a fantasy-scoring construct, not a real-world stat; recompute lane"

CANNOT: dict[str, str] = {
    # NGS tracking
    **{c: NGS_TRACKING for c in (
        "ngs_aggressiveness", "ngs_avg_cushion", "ngs_avg_expected_yac",
        "ngs_avg_time_to_los", "ngs_avg_time_to_throw", "ngs_avg_yac",
        "ngs_avg_yac_above_expectation", "ngs_pct_att_gte_8_defenders",
        "ngs_avg_air_yards_differential", "ngs_avg_air_yards_to_sticks",
        "ngs_pct_share_intended_air_yards", "ngs_expected_rush_yards",
        "ngs_rush_efficiency", "ngs_rush_pct_over_expected",
        "ngs_rush_yards_over_expected", "ngs_completion_pct_above_expectation",
        "ngs_expected_completion_pct", "ngs_avg_separation")},
    # PFR charting -- GREAT reasons: these stats DO NOT EXIST anywhere before
    # 2018 charting; there is no 1978 road to refuse. (passing_hits LEFT this
    # class 2026-08-01: pbp qb_hit runs from 1999 -- deeper than charting --
    # and is now witnessed; the careful pass found it after Joe asked.)
    **{c: PFR_CHARTING for c in (
        "passing_drops", "passing_poor_throws",
        "passing_blitzed", "passing_hurried", "passing_pressured",
        "receiving_drops", "receiving_broken_tackles",
        "rushing_broken_tackles",
        "rushing_yards_after_contact", "rushing_yards_before_contact",
        "def_blitzes", "def_hurries", "def_knockdowns", "def_pressures",
        "def_tackles_missed")},
    # participation / lineup
    **{c: PARTICIPATION for c in (
        "offense_snaps", "offense_snap_pct", "defense_snaps",
        "defense_snap_pct", "special_teams_snaps", "special_teams_snap_pct",
        "is_starter", "starter_position", "fantasy_position")},
    "fantasy_points_ppr": LEAGUE_FORMULA,
    # PROVEN ALIASES (identical value paths, 2026-08-01): the keeper holds the
    # licence; a second spec would double-declare the same evidence. The pairs
    # sit on the consolidation slate.
    "receiving_tds_allowed": "ALIAS of passing_tds_allowed (team grain: a "
                             "receiving TD IS a pass TD against the defense)",
    "def_completion_tds_allowed": "ALIAS of passing_tds_allowed (same path)",
    "yds_from_scrimmage": "ALIAS of scrimmage_yards",
    "rush_receive_td": "ALIAS of scrimmage_tds",
    # RULED DROPS (Joe): the column is leaving the vocabulary; witnessing a
    # condemned name would be spec-work on a corpse. Scope closes at the drop.
    "fg_made_60_plus_canonical": "RULED_DROP (drop builder, proven subset)",
    "fg_yards_canonical": "RULED_DROP (3-way twin folds to fg_made_distance)",
    "fg_yards_over_30_canonical": "RULED_DROP (no consumer; deletes without fold)",
    "total_tds_accounted_for": "RULED_DROP (arbitration then drop; total_tds_scored keeps the licence)",
    "touches": "RULED_DROP (alias of total_touches; drop builder ready)",
    # terminal states from the FINAL SWEEP (2026-08-01):
    "nfl_franchise_number": ("IDENTITY_REGISTRY: franchise numbering is OUR "
                             "registry construct; pbp carries team codes, not "
                             "franchise numbers -- the map is the KC lane"),
    "opponent_nfl_franchise_number": "IDENTITY_REGISTRY (same)",
    "wopr": ("COMPOSITE of two witnessed shares (1.5*target + 0.7*air); a "
             "third spec would re-declare both"),
    "def_qb_hits_pbp_extension": ("EXTENSION NOTE, not a column: qb_hit_1/2 "
                                  "hitter ids 1999+ deepen the 2018 charting "
                                  "witness when the union lands"),
    "primary_position": ("roster/taxonomy attribute (pending Joe's taxonomy "
                         "ruling); pbp events carry no position"),
}

# ---- OWED: pbp can derive it; the spec is DEBT. Shrink-only.
# (receiving_tds_allowed / yds_from_scrimmage / rush_receive_td left the queue
# 2026-08-01 as PROVEN ALIASES of passing_tds_allowed / scrimmage_yards /
# scrimmage_tds -- identical value paths; consolidation slate, not debt.) ----
OWED: dict[str, str] = {
    # trivial rollups (pbp_rollup / pbp_team_rollup as-is)
    "fg_made_distance": "kick_distance where field_goal_result='made'",
    "fg_missed_0_19": "kick_distance buckets on missed",
    "fg_missed_20_29": "kick_distance buckets on missed",
    "fg_missed_30_39": "kick_distance buckets on missed",
    "fg_missed_40_49": "kick_distance buckets on missed",
    "fg_missed_50_59": "kick_distance buckets on missed",
    "fg_missed_60_": "kick_distance buckets on missed",
    "fg_blocked_distance": "kick_distance where blocked",
    "fg_yds_over_30": "sum(kick_distance-30) made over 30",
    "fg_yards_canonical": "twin pending consolidation; expr = made distances",
    "fg_yards_over_30_canonical": "DELETING (no consumer) -- leaves scope at drop",
    "fg_made_60_plus_canonical": "DELETING (drop builder) -- leaves scope at drop",
    "pick6": "interception=1 AND return_touchdown; twin of def_int_ret_td "
             "(which is already pbp-witnessed)",
    "def_fumbles": "defensive-player fumbles via fumbled ids x defteam",
    "def_sack_yards": "yards_gained on sack=1 by defteam / sack_player",
    "def_tackles_combined": "SOLE 1978 ROAD: tackle-credit ids run from 1978 "
                            "in pbp; PFR tackles start 1994. solo/assist ids",
    "def_tackles_for_loss_yards": "SOLE 1978 ROAD: tfl flag from 1978 + "
                                  "yards_gained<0 credits",
    "def_qb_hits_pbp_extension": "qb_hit_1/2 hitter ids 1999+ -- deeper than "
                                 "the 2018 charting witness (union shape)",
    "scrimmage_yards": "rush+rec yards union per player",
    "scrimmage_tds": "rush_td+rec_td union per player",
    "total_tds_scored": "td_player_id credits all TD types",
    "total_tds_accounted_for": "DROPPING (Joe ruling) -- arbitration first; leaves scope at drop",
    "total_points_scored": "scoring events per player (TD/FG/XP/2pt/safety)",
    "total_touches": "carries + receptions",
    "touches": "DROPPING (alias) -- leaves scope at drop",
    "opportunities": "carries + targets",
    "all_purpose_yards": "scrimmage + return yards union",
    "misc_yards": "lateral/misc yardage from the desc grammar",
    "turnovers": "int thrown + fumbles lost union per player",
    "timeouts": "timeout events per team",
    "fumble_recovery_own": "fumble_recovery ids where recovering team == fumbling team",
    "fumble_recovery_opp": "fumble_recovery ids where teams differ",
    # window / game-state machinery (gwfg att/made/missed/distance SPECCED
    # 2026-08-01 -- left OWED only:)
    "gwfg_blocked": "blocked in the gwfg window (predicate exists, spec trivial)",
    "three_out": "drive segmentation from pbp drive column",
    "fourth_down_stop": "fourth_down_failed by defteam",
    # team mirrors (2nd roots beside validated witnesses; low priority)
    "points_allowed": "final scores by defteam (catalog already 100%)",
    "team_points": "final scores (catalog already 100%)",
    "opponent_points": "final scores (catalog already 100%)",
    "game_margin": "final scores (catalog already 100%)",
    "is_win": "final scores (catalog already 100%)",
    "is_overtime": "qtr>4 exists (catalog already 100%)",
    "home_away": "posteam vs home_team (catalog authoritative)",
    "passing_yds_allowed": "team rollup (team_stats mirror already 100%)",
    "rushing_yds_allowed": "team rollup (team_stats mirror already 100%)",
    "passing_tds_allowed": "team rollup (team_stats mirror already 100%)",
    "rushing_tds_allowed": "team rollup (team_stats mirror already 100%)",
    "total_yds_allowed": "team rollup (gross; def_yards_allowed=net twin)",
    "dst_points_allowed": "contraction over scoring events (definition gate)",
    "dst_return_yards": "return-team attribution union (punt vs kickoff sides)",
    "misc_yards": ("PINNED 08-02 by measurement (Joe: 'pbp can define it'): "
                   "missed/blocked FG return yardage -- Alford 96 (missed-FG "
                   "return), Whittaker 51 + Owens 23 (blocked-FG recoveries), "
                   "3/3 exact. Machinery: nflverse has no FG-returner id "
                   "column, so the lane is a desc parse or the pfr pbp detail "
                   "links (cleaner: returner is a link)"),
    "total_return_yards": "punt+kick return yards per returner",
    # identity-adjacent context that pbp can still corroborate
    "nfl_franchise_number": "team-code x franchise map corroboration (KC lane primary)",
    "opponent_nfl_franchise_number": "same",
    # rates moved from the dissolved CANNOT class (debt, pbp_ratio shape exists):
    "passing_yards_per_attempt": "pbp_ratio (yds/att); also 2a instrument",
    "passing_adjusted_yards_per_attempt": "pbp_ratio",
    # passing_net_yards_per_attempt + passer_rating: SPECCED 2026-08-01 (the
    # careful pass) -- net-Y/A convicts the summed twin; passer_rating convicts
    # a SECOND defect mechanism (mean-of-weekly ratings).
    "passing_adjusted_net_yards_per_attempt": "pbp_ratio",
    "passing_int_pct": "pbp_ratio (int/att)",
    "passing_td_pct": "pbp_ratio (td/att)",
    "receiving_pass_rating": "pbp_ratio (rating on targets to the receiver)",
    "receiving_yards_per_target": "pbp_ratio",
    "yards_per_touch": "pbp_ratio (scrim yds / touches)",
    "pat_pct": "pbp_ratio (xp made/att)",
    "fg_pct": "pbp_ratio (fg made/att)",
    "air_yards_share": "pbp_ratio w/ team denominator window",
    "target_share": "pbp_ratio w/ team denominator window",
    "pacr": "pbp_ratio (rec yds / air yds)",
    "racr": "pbp_ratio",
    "wopr": "pbp_ratio (1.5*tgt_share + 0.7*ay_share)",
    "receiving_adot": "pbp_ratio (air yds / targets)",
    "punt_yards_per_punt": "pbp_ratio (punt yds/punts)",
    "def_passer_rating_allowed": "pbp_ratio by defteam",
}


# ---- AUTO-CLOSURE (2026-08-01): an OWED entry closes ITSELF the moment a
# pbp-lineage spec exists -- the ledger can never lag the map. The declared
# dict above is the historical debt statement; OWED below is the live queue.
def _witnessed() -> set:
    from .witness_map import WITNESS_MAP
    return {s.v26_col for s in WITNESS_MAP if s.source_key in PBP_SOURCES}

OWED_DECLARED = dict(OWED)
# terminal CANNOT states subtract too -- an entry may not be debt and refusal at once
OWED = {k: v for k, v in OWED.items()
        if k not in _witnessed() and k not in CANNOT}
