"""
witness_gate/build_stat_contracts.py -- O.3: THE statistical contract registry (master plan §6 + §25.7).

Unifies the five fragmented aggregation/definition declarations into ONE versioned registry,
`witness_gate/contracts/stat_contracts.v1.json`, with every cross-source conflict adjudicated
by an explicit rule carrying a receipt (never silently merged):

  1. reconcile_atoms.TABLE_ATOMS            (sum/max + PFR-boxscore witness eras + ceilings/floors)
  2. recon_aggregate exclusion sets          (frozen here as LEGACY_*; recon_aggregate now READS us)
  3. aggregate_nfl_stats_fly declaration sets (frozen here as LEGACY_AGG_*; the builder now READS us)
  4. golden_points.SIMPLE_RATE_NUMDEN        (num/den pairs, locked at season+career)
  5. recon_expectations.COVERAGE_FROM / EVIDENCE_GAPS / BOUNDS (era floors, sourced gaps, ceilings)
  +  era_rescue.PBP / FORMULA               (achievable floors + formula dependency maps)
  +  research concept graph / numeric profiles (docs/research-mode-*.json) -- CANDIDATES ONLY per
     §25.7: live-verified polarity/unit defects (age unit=count, air_yards_share unit=yards,
     fumbles_lost higher-is-better, per-game polarity-flip generator bug) mean the research
     metadata NEVER overrides an adjudicated field; steady state reverses the authority direction
     (contracts generate the graph, not vice versa).

Authority direction (§25.7): this registry is AUTHORITATIVE. Legacy semantic metadata enters as
candidate input; disagreements become `conflicts` rows, each resolved by a rule in ADJUDICATION_RULES
with its receipt. The builder FAILS on any conflict without a rule, any live column without exactly
one contract row, and any parity break between the registry-derived aggregation sets and the frozen
legacy behavior (so switching consumers is provably behavior-neutral).

Consumers (switched this slice):
  - fantasy_football_data_scripts/multi_league/data_fetchers/aggregate_nfl_stats_fly.py
  - scripts/sota_recon/recon_aggregate.py
both read the emitted `aggregation_sets` block via multi_league/core/stat_contracts_loader.py.

    python -m scripts.sota_recon.witness_gate.build_stat_contracts          # build + gate + write
    python -m scripts.sota_recon.witness_gate.build_stat_contracts --check  # rebuild, diff vs committed
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
OUT = Path(__file__).resolve().parent / "contracts" / "stat_contracts.v1.json"
PROFILES_JSON = REPO / "docs" / "research-mode-numeric-profiles.json"
CONCEPTS_JSON = REPO / "docs" / "research-mode-semantic-concept-graph.json"
ERA_SCAN_JSON = Path("D:/league-history-data/nfl/derived/validation/witness_audit_2026_07_16/super_column_era_coverage.json")
BIO_PARQUET = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"

AGG_CLASSES = {"SUM", "MAX", "MIN", "RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE", "DISTINCT_COUNT",
               "SET_UNION", "ANY", "FIRST", "LAST", "EVENT_CARDINALITY", "NON_AGGREGATABLE"}

# ---------------------------------------------------------------------------------------------
# FROZEN LEGACY CANDIDATES for the two modules that become registry READERS this slice.
# These are byte-faithful copies of the pre-switch declarations (aggregate_nfl_stats_fly.py and
# recon_aggregate.py as of commit HEAD 2026-07-25). They exist so the parity gate can prove the
# registry-derived sets reproduce the old behavior exactly. Do NOT edit these to make the gate
# pass -- edit the adjudication and record a conflict.
# ---------------------------------------------------------------------------------------------
LEGACY_AGG_PER_GAME_AVG_COLS: dict[str, str] = {}
LEGACY_AGG_DERIVED_RATE_COLS = {
    "catch_rate", "comp_pct", "fg_pct", "pacr", "passer_rating", "racr",
    "yards_per_attempt", "yards_per_carry", "yards_per_reception",
    "yards_per_target", "rec_td_pct", "rush_td_pct", "yards_per_touch",
    "passing_td_pct", "passing_int_pct", "adot", "receiving_adot", "sack_pct", "xp_pct",
    "target_share", "air_yards_share", "wopr",
}
LEGACY_AGG_SHARE_FAMILY_COLS = {"target_share", "air_yards_share", "wopr"}

# ---- MEASURED RATES THE LEGACY SET MISSED, because it holds the SHORT spellings only ----
#
# `yards_per_attempt` is in LEGACY_AGG_DERIVED_RATE_COLS; `passing_yards_per_attempt` is
# not, so the family-prefixed twin of every one of these fell through to SUM. The registry
# therefore carried BOTH typings of one concept -- `adjusted_yards_per_attempt`
# RECOMPUTE_RATE beside `passing_adjusted_yards_per_attempt` SUM -- and any season or
# career rollup that summed the second produced 7.1 + 6.8 + 8.2 = 22.1 yards per attempt.
#
# NOT ASSUMED FROM THE NAME. Measured on the weekly release, `value == numerator/denominator`
# within display tolerance on rows where the denominator is non-zero:
#
#     passing_yards_per_attempt      100.0%  of  49,938 rows
#     rushing_yards_per_carry        100.0%  of 155,166
#     receiving_yards_per_reception  100.0%  of 216,559
#     receiving_yards_per_target     100.0%  of 188,452
#     punt_yards_per_punt            100.0%  of  33,725
#     pat_pct                        100.0%  of  32,143   (max 1.0 -- a proportion)
#
# The four passing_adjusted_* / net variants share the per-attempt denominator and the same
# morphology; they are included on that basis and the assertion below stops the class from
# silently reopening.
# A count whose NAME looks like a rate. Each needs its reason; the assertion at the end of
# build() fails on anything rate-shaped and SUM-classed that is not listed here.
RATE_MORPHOLOGY_EXEMPT: dict[str, str] = {}

# numerator/denominator for the family-prefixed rates, so `rate.components` is populated
# rather than left pending. Same pairs the short-spelling twins already declare.
MEASURED_RATE_DEPENDENCIES = {
    "passing_yards_per_attempt": ("passing_yards", "attempts"),
    "passing_net_yards_per_attempt": ("passing_yards", "attempts"),
    "passing_adjusted_yards_per_attempt": ("passing_yards", "attempts"),
    "passing_adjusted_net_yards_per_attempt": ("passing_yards", "attempts"),
    "rushing_yards_per_carry": ("rushing_yards", "carries"),
    "receiving_yards_per_reception": ("receiving_yards", "receptions"),
    "receiving_yards_per_target": ("receiving_yards", "targets"),
    "punt_yards_per_punt": ("punt_yards", "punts"),
    "pat_pct": ("pat_made", "pat_att"),
}

MEASURED_RATE_COLS = {
    "passing_yards_per_attempt", "passing_net_yards_per_attempt",
    "passing_adjusted_yards_per_attempt", "passing_adjusted_net_yards_per_attempt",
    "rushing_yards_per_carry", "receiving_yards_per_reception",
    "receiving_yards_per_target", "punt_yards_per_punt", "pat_pct",
}
# NGS publishes these metrics directly at weekly/season grain and does not publish
# per-play numerator/denominator operands. They are valid direct published rates,
# but must never be summed or weighted with generic NFL columns.
NGS_DIRECT_RATE_COLS = {
    "ngs_avg_cushion", "ngs_avg_separation", "ngs_avg_yac", "ngs_avg_expected_yac",
    "ngs_avg_yac_above_expectation", "ngs_pct_share_intended_air_yards",
    "ngs_rush_efficiency", "ngs_pct_att_gte_8_defenders", "ngs_avg_time_to_los",
    "ngs_rush_pct_over_expected", "ngs_avg_time_to_throw", "ngs_aggressiveness",
    "ngs_avg_air_yards_to_sticks", "ngs_expected_completion_pct",
    "ngs_completion_pct_above_expectation", "ngs_avg_air_yards_differential",
}
NGS_CAREER_DENOMINATOR = {
    "ngs_pct_share_intended_air_yards": "team_receiving_air_yards",
    "ngs_avg_air_yards_differential": "passing_air_yards_components",
    "ngs_rush_efficiency": "rushing_yards",
}
LEGACY_AGG_DERIVED_RATE_DEPENDENCIES = {
    "catch_rate": ("receptions", "targets"),
    "comp_pct": ("completions", "attempts"),
    "fg_pct": ("fg_made", "fg_att"),
    "pacr": ("passing_yards", "passing_air_yards"),
    "passer_rating": ("completions", "attempts", "passing_yards", "passing_tds", "passing_interceptions"),
    "racr": ("receiving_yards", "receiving_air_yards"),
    "yards_per_attempt": ("passing_yards", "attempts"),
    "yards_per_carry": ("rushing_yards", "carries"),
    "yards_per_reception": ("receiving_yards", "receptions"),
    "yards_per_target": ("receiving_yards", "targets"),
    "rec_td_pct": ("receiving_tds", "targets"),
    "rush_td_pct": ("rushing_tds", "carries"),
    "yards_per_touch": ("rushing_yards", "receiving_yards", "carries", "receptions"),
    "passing_td_pct": ("passing_tds", "attempts"),
    "passing_int_pct": ("passing_interceptions", "attempts"),
    "adot": ("passing_air_yards", "attempts"),
    "receiving_adot": ("receiving_air_yards", "targets"),
    "sack_pct": ("sacks_suffered", "attempts"),
    "xp_pct": ("pat_made", "pat_att"),
    "target_share": ("targets",),
    "air_yards_share": ("receiving_air_yards",),
    "wopr": ("targets", "receiving_air_yards"),
}
LEGACY_AGG_WEIGHTED_AVG_COLS = {
    "passing_cpoe": "attempts",
    # Joe, 2026-07-29: "should still be based on the number of attempts each week."
    # An NGS MODEL OUTPUT -- expected completion % per play, averaged. It has no
    # numerator/denominator we store, so RECOMPUTE_RATE cannot describe it and the
    # contract test rejects a rate row with no components. But SUM is worse: it adds
    # percentages across weeks. Weighting by the week's attempts is the same shape
    # `passing_cpoe` already uses, and cpoe is literally derived against this expectation.
}
LEGACY_AGG_MAX_COLS = {"fg_long", "passing_long", "receiving_long", "rushing_long"}

LEGACY_RA_NONADD_PREFIX = ("ppg_", "rank_", "lamar_", "consistency_", "weighted_ppg_", "rolling_3_",
                           "rolling_5_", "avg_pts_next_year_", "season_ppg")
LEGACY_RA_NONADD_EXACT = {
    "NFL_player_id", "year", "week", "games", "games_played", "season_type",
    "yahoo_player_id", "pfr_id", "nfl_franchise_number", "nfl_team_count",
    "age", "all_pro_first_team", "all_pro_second_team", "pro_bowl", "mvp",
    "career_all_pro_first", "career_all_pro_second", "career_pro_bowls", "career_mvps",
    "age_at_draft", "height", "weight", "draft_year", "draft_round", "draft_overall",
    "is_undrafted", "rookie_year", "years_active", "seasons_started", "hof", "allpro",
    "probowls", "w_av", "dr_av", "forty", "bench", "vertical", "broad_jump", "cone",
    "shuttle", "ras_score",
}
LEGACY_RA_FACT_ADJUSTED = {
    "attempts", "carries", "completions", "fg_att", "fg_made", "fum_rec", "fum_rec_yds",
    "fum_ret_td", "fumbles", "kickoff_return_tds", "kickoff_return_yards", "kickoff_returns",
    "passing_interceptions", "passing_tds", "passing_yards", "pat_att", "pat_made",
    "punt_return_tds", "punt_return_yards", "punt_returns", "punt_yards", "punts",
    "punts_blocked", "receiving_tds", "receiving_yards", "receptions", "rushing_tds",
    "rushing_yards", "sack_yards_lost", "sacks_suffered",
}

# ---------------------------------------------------------------------------------------------
# ADJUDICATION RULES -- every conflict resolution names one of these; each carries its receipt.
# ---------------------------------------------------------------------------------------------
ADJUDICATION_RULES = {
    "R-AGG-LONG-MAX": (
        "Longs are single-play maxima, never sums. Receipts: reconcile_atoms.TABLE_ATOMS registers "
        "*_long with agg='max'; recon_scoring_events.py declares the sum-of-distances form a defect "
        "class; build_fg_buckets recomputes fg_long=MAX(dist). punt_long/kickoff_return_long/"
        "punt_return_long are weekly-only today (verified 2026-07-25: absent from season/career "
        "schemas), so LEGACY_AGG_MAX_COLS omitting them is an exposure gap, not a class conflict."
    ),
    "R-AGG-RATE-RECOMPUTE": (
        "Rates recompute from summed components at every grain, never averaged and never summed. "
        "Receipts: aggregate_nfl_stats_fly 2026-06-29 note ('recomputed from summed fact-adjusted "
        "components, never averaged'); golden_points locks stored == SUM(num)/SUM(den) at season "
        "AND career for every simple pair."
    ),
    "R-AGG-WEIGHTED": (
        "Per-play averages aggregate as volume-weighted recomputes. Receipt: plain SUM stored "
        "17x-inflated NGS season values (Kupp 2021 ngs_avg_separation 62.50 vs true 3.68; "
        "aggregate_nfl_stats_fly comment 2026-07-09)."
    ),
    "R-SEASON-FACT-ADJUSTED": (
        "30 atoms take their season value from a season-grain authority (fact-adjustment sidecar), "
        "so season != SUM(weekly) BY DESIGN; validated by the recon_season_authority lane, excluded "
        "from recon_aggregate SUM-reconcile. aggregation_class stays SUM (the stat is additive); "
        "season_derivation records the witness-direct authority (§19.5 density law)."
    ),
    "R-SEASON-NGS-PUBLISHED": (
        "ngs_* season values are nflverse's PUBLISHED season totals (direct ingest, not "
        "SUM(weekly per-game averages)); recon_aggregate comment; skip-like-fact-adjusted."
    ),
    "R-POLARITY-ADVERSE": (
        "Adverse outcomes (turnovers, misses, sacks taken, points/yards allowed, drops, penalties) "
        "are lower-is-better regardless of the research profile direction. Receipts (live-verified "
        "2026-07-25 against docs/research-mode-numeric-profiles.json): fumbles_lost=higher_is_better "
        "while total_fumbles_lost=lower_is_better (internally inconsistent); sacks_suffered and "
        "fg_missed=higher_is_better; master plan §25.7 first-adjudication list."
    ),
    "R-POLARITY-VARIANT-INHERIT": (
        "Rate/per-game/per-season variants inherit the polarity of their adverse numerator. Receipt: "
        "profiles' generated variants systematically flip (passing_interceptions_per_game= "
        "higher_is_better in the committed profiles JSON) -- §25.7 generator bug."
    ),
    "R-UNIT-SHARE-RATIO": (
        "Share/percentage metrics are dimensionless ratios. Receipt: profiles carry "
        "air_yards_share unit='yards' -- a share of yards is unitless; §25.7."
    ),
    "R-UNIT-AGE-YEARS": (
        "age is measured in years and is contextual, not higher-is-better. Receipt: profiles carry "
        "unit='count' + direction='higher_is_better' for age; §25.7."
    ),
    "R-FLOOR-COVERAGE-CURATED": (
        "recon_expectations.COVERAGE_FROM floors are curated with data-verified rationales (e.g. "
        "passing_interceptions 1933 with the 1932 zero-evidence note; success/EPA 1978 verified "
        "against raw PBP), so they outrank era_rescue.PBP mechanism floors, which are stored as "
        "achievable_floor instead. passing_epa: era_rescue's 1999 entry predates the wave67 EPA "
        "backfill (live 2026-07-21, passing=raw-epa pre-1999); COVERAGE_FROM 1978 is current truth."
    ),
    "R-FLOOR-WITNESS-SCOPED": (
        "reconcile_atoms.TABLE_ATOMS eras are witness-scoped (the PFR boxscore reconcile window "
        "for that atom), not global coverage floors; they land in eras.witness_floors and never "
        "override first_applicable_year. Example: targets TABLE_ATOMS era 1992 (PFR box) vs "
        "COVERAGE_FROM 1978 (PBP rollup) -- both true, different scopes."
    ),
    "R-NUMDEN-CONSENSUS": (
        "golden_points.SIMPLE_RATE_NUMDEN and aggregate_nfl_stats_fly.DERIVED_RATE_DEPENDENCIES "
        "agree on every shared pair (verified at build); the shared value is adopted."
    ),
    "R-RANK-NONAGG": (
        "rank_* columns are recomputed dense permutations per grain (recon_aggregate "
        "RANK-WELLFORMED asserts dense 1..N, no dups); they never aggregate."
    ),
    "R-DERIVED-OUTPUT-FAMILIES": (
        "ppg_/rolling_/consistency_/weighted_ppg_/avg_pts_next_year/lamar_/season_ppg are engine "
        "outputs recomputed per grain (recon_aggregate _NONADD_PREFIX; PPG-SANITY asserts "
        "ppg_season == AVG(weekly fpts))."
    ),
    "R-AGGPOLICY-HEURISTIC-CANDIDATE": (
        "The research aggregationPolicy is derived by name/type heuristics "
        "(research-semantic-operations liveAggregationPolicy / aggregationPolicyForRelationship) "
        "and enters as CANDIDATE ONLY; it never overrides a count-derived SUM/MAX/RECOMPUTE "
        "adjudication (§25.7 authority reversal: contracts generate the graph, not vice versa)."
    ),
    "R-SCALE-MEASURED": (
        "percentage_scale is measured from the live tables (max stored value > 1.5 => 0-100 scale, "
        "else 0-1), not inferred from names; measurement receipt embedded per stat."
    ),
    "R-GAMES-CARDINALITY": (
        "games/games_played count weekly game rows (recon_aggregate GAMES check: season games == "
        "COUNT(weekly rows)); class EVENT_CARDINALITY, season_derivation count_rows."
    ),
    "R-ENRICH-STATIC": (
        "Per-season-constant enrichment (age) and career-static bio/draft/combine/award-count "
        "columns are FIRST/NON_AGGREGATABLE (recon_aggregate _NONADD_EXACT comment: 'per-season "
        "age is a constant, not a sum; award flags are per-season; career-static bio')."
    ),
    "R-RATE-LEGACY-OMISSION": (
        "Rate columns present in the season/career tables but absent from every legacy exclusion "
        "set were being validated as additive (career == SUM(season) on a RATE). Live receipt "
        "2026-07-25: recon_aggregate career_eq_sum_season failed adjusted_net_yards_per_attempt "
        "1,366 rows / adjusted_yards_per_attempt 1,362 / net_yards_per_attempt 1,331 -- the stored "
        "values are correct recomputes (build_derived_columns_v26), the CHECKER's exclusion sets "
        "were incomplete. Contract class RECOMPUTE_RATE; validation lanes now skip via contract "
        "class. The season-builder aggregation_sets stay legacy-parity (these columns are built "
        "by build_derived_columns_v26, not by the _fly rate machinery)."
    ),
    "R-FLOOR-OBSERVED-EARLIER": (
        "Observed nonzero data BEFORE the declared coverage floor keeps the declared floor (the "
        "curated floor states where the source systematically covers; earlier stragglers may be "
        "partial-tracking noise, e.g. 14 pre-1978 stray target games noted in "
        "aggregate_nfl_stats_fly share gating). The earlier observed year is recorded and routes "
        "to the year-range-drift escalation queue (same class as the O.2 146-canonical "
        "year_min/max drift -- an upstream matrix/coverage-lane question, not silently resolved)."
    ),
}

# ---------------------------------------------------------------------------------------------
# Rulebooks (the adjudicated declarations themselves)
# ---------------------------------------------------------------------------------------------

# Adverse (lower-is-better) stats -- explicit names + patterns; def_ exceptions handled in code.
ADVERSE_EXACT = {
    "passing_interceptions", "fumbles", "fumbles_lost", "rushing_fumbles", "rushing_fumbles_lost",
    "receiving_fumbles", "receiving_fumbles_lost", "sack_fumbles", "sack_fumbles_lost",
    "sacks_suffered", "sack_yards_lost", "fg_missed", "pat_missed", "gwfg_missed", "gwfg_blocked",
    "fg_blocked", "pat_blocked", "punts_blocked", "def_tackles_missed",
    "receiving_target_interceptions", "turnovers", "total_turnovers", "pick6",
    "points_allowed", "dst_points_allowed", "total_yds_allowed", "penalties", "penalty_yards",
    "passing_int_pct", "sack_pct",
}
# combine timing drills: lower is better, sort ascending
TIMING_DRILLS = {"forty", "cone", "shuttle"}
CONTEXTUAL_POLARITY = {"age", "age_at_draft", "draft_round", "draft_overall", "draft_year",
                       "rookie_year", "first_year", "last_year", "height", "weight", "adot",
                       "receiving_adot", "is_undrafted", "years_active", "years_active_1",
                       "seasons_started", "nfl_team_count", "timeouts", "dropbacks"}
SORT_ASC = {"draft_round", "draft_overall", "age_at_draft"} | TIMING_DRILLS

UNIT_EXACT = {
    "age": "age_years", "age_at_draft": "age_years",
    "height": "inches", "vertical": "inches", "broad_jump": "inches", "weight": "pounds",
    "forty": "seconds", "cone": "seconds", "shuttle": "seconds", "bench": "reps",
    "ras_score": "score", "w_av": "score", "dr_av": "score",
    "ngs_avg_cushion": "yards", "ngs_avg_separation": "yards",
    "ngs_avg_yac": "yards", "ngs_avg_expected_yac": "yards",
    "ngs_avg_yac_above_expectation": "yards",
    "ngs_pct_share_intended_air_yards": "ratio",
    "ngs_rush_efficiency": "ratio", "ngs_pct_att_gte_8_defenders": "ratio",
    "ngs_avg_time_to_los": "seconds", "ngs_expected_rush_yards": "yards",
    "ngs_rush_yards_over_expected": "yards", "ngs_rush_pct_over_expected": "ratio",
    "ngs_avg_time_to_throw": "seconds", "ngs_aggressiveness": "ratio",
    "ngs_avg_air_yards_to_sticks": "yards", "ngs_expected_completion_pct": "ratio",
    "ngs_completion_pct_above_expectation": "ratio",
    "ngs_avg_air_yards_differential": "yards",
    "passer_rating": "rating", "draft_round": "draft_slot", "draft_overall": "draft_slot",
    "games": "games", "games_played": "games", "games_started": "games", "career_games": "games",
    "years_active": "seasons", "years_active_1": "seasons", "seasons_started": "seasons",
    "total_epa": "epa", "total_wpa": "win_probability",
}

# structural parents (child <= parent), grain player_game unless noted; receipts inline
STRUCTURAL_PARENTS = {
    "completions": ["attempts"],            # reconcile_atoms.CEILINGS player_offense
    "fg_made": ["fg_att"],                  # reconcile_atoms.CEILINGS kicking
    "pat_made": ["pat_att"],                # reconcile_atoms.CEILINGS kicking
    "receptions": ["targets"],              # witness_gate equations.v1 receptions_lte_targets (1992+)
    "gwfg_made": ["gwfg_att"],
}
# §17.1 tolerance law (wired 2026-07-26): cross-source comparison tolerance is a DECLARED
# per-stat policy, never an ad-hoc constant in lane code. Default = EXACT: any disagreement
# beyond float noise is a CONFLICT (typed, queued), because two sources asserting values
# 1 apart are asserting different facts. A non-EXACT entry requires a basis + receipt:
#   {"policy": "DECLARED", "tolerance": <abs>, "basis": "...", "receipt": "..."}
# Legal bases are unit/parse facts (mm:ss parses, declared scale conversions) -- "yards
# sometimes differ by 1" is a finding, not a policy. The old lane rule (±1.5 yards /
# ±0.5 counts) was ad-hoc and is dead; the 85 ±1-yard modern-core disagreements it
# silently absorbed are receipted in docs/source-battle-v0.json (exact vs tolerance gap).
# EMPTY ON PURPOSE: no receipted exception exists today.
TOLERANCE_POLICIES: dict[str, dict] = {}

# partition parents: bucket family -> parent (fg_made = sum of distance buckets + unknown)
FG_BUCKETS = {"fg_made_0_19", "fg_made_20_29", "fg_made_30_39", "fg_made_40_49",
              "fg_made_50_59", "fg_made_60_", "fg_made_60_plus", "fg_made_60plus"}
RECEPTION_BUCKETS = {"receptions_0_4", "receptions_5_9", "receptions_10_19", "receptions_20_29",
                     "receptions_30_39", "receptions_40plus"}

PROVENANCE_PAT = re.compile(r"(_repaired_at_|_recomputed_at_|_merged_at_|_populated_at_)\d{8}$")
PROVENANCE_EXACT = {"recon_correction_log", "data_source", "last_updated", "primary_team_source"}

IDENTITY_EXACT = {
    "NFL_player_id", "player", "player_week", "yahoo_player_id", "sleeper_player_id", "espn_id",
    "pfr_id", "headshot_url", "primary_franchise_id",
}
CONTEXT_EXACT = {
    "year", "week", "season_type", "game_date", "home_away", "is_overtime", "nfl_team",
    "nfl_franchise_number", "opponent_nfl_team", "opponent_nfl_franchise_number", "position",
    "nfl_position", "fantasy_position", "primary_position", "position_candidates",
    "position_category", "position_side", "starter_position", "is_starter", "team_points",
    "opponent_points", "game_margin", "is_win", "status", "latest_team", "primary_team",
    "nfl_teams", "nfl_draft_team", "college", "conference", "high_school", "birth_date",
    "birth_place", "career_history", "season_positions", "career_positions", "first_year",
    "last_year", "rookie_year", "draft_year", "wins",
}
# award/honor per-season flags -> ANY at season, SUM to career counts handled by career_* columns
AWARD_SEASON_FLAGS = {"all_pro_first_team", "all_pro_second_team", "pro_bowl", "mvp", "dpoy",
                      "opoy", "droy", "oroy", "cpoy", "hof"}
CAREER_STATIC = {"allpro", "probowls", "career_all_pro_first", "career_all_pro_second",
                 "career_pro_bowls", "career_mvps", "career_dpoy", "career_opoy", "career_droy",
                 "career_oroy", "career_cpoy", "career_games", "w_av", "dr_av", "height", "weight",
                 "forty", "bench", "vertical", "broad_jump", "cone", "shuttle", "ras_score",
                 "age_at_draft", "draft_round", "draft_overall", "is_undrafted", "years_active",
                 "years_active_1", "seasons_started", "games_started", "nfl_team_count"}

DERIVED_OUTPUT_PREFIXES = ("fpts_", "pts_", "lamar_", "ppg_", "rank_", "rolling_", "weighted_ppg_",
                           "consistency_", "avg_pts_next_year", "bonus_")

ALIAS_OF = {
    "catch_pct": "catch_rate", "completion_pct": "comp_pct", "total_tds_accounted_for": "total_tds_scored",
    "total_touches": "touches", "yds_from_scrimmage": "scrimmage_yards",
    "fantasy_points_ppr": "fpts_4pt_ppr", "years_active_1": "years_active",
}

# multi-component / non-simple rate formulas beyond the AGG dependency map (era_rescue.FORMULA,
# keys normalized to live column names)
EXTRA_RATE_COMPONENTS = {
    "net_yards_per_attempt": ["passing_yards", "sack_yards_lost", "attempts", "sacks_suffered"],
    "adjusted_yards_per_attempt": ["passing_yards", "passing_tds", "passing_interceptions", "attempts"],
    "adjusted_net_yards_per_attempt": ["passing_yards", "passing_tds", "passing_interceptions",
                                       "sack_yards_lost", "attempts", "sacks_suffered"],
    "catch_pct": ["receptions", "targets"],
    "completion_pct": ["completions", "attempts"],
}
# rates whose denominator is not a stored column (team volume / snap totals) -- typed exception
DIRECT_RATE_NO_COMPONENTS = {"offense_snap_pct", "defense_snap_pct", "special_teams_snap_pct"}


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def _sha256(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def load_legacy_inputs():
    """Import the three still-authoritative declaration modules + research candidate JSONs."""
    from scripts.sota_recon.reconcile_atoms import TABLE_ATOMS, CEILINGS, FLOORS  # noqa: F401
    from scripts.sota_recon.golden_points import SIMPLE_RATE_NUMDEN
    from scripts.sota_recon.recon_expectations import COVERAGE_FROM, EVIDENCE_GAPS, BOUNDS
    from scripts.sota_recon.witness_audit_v2.era_rescue import PBP as ERA_PBP, FORMULA as ERA_FORMULA
    profiles = {p["key"]: p for p in _read_json(PROFILES_JSON)["profiles"]}
    graph = _read_json(CONCEPTS_JSON)
    col_policy: dict[str, set[str]] = {}
    for con in graph["concepts"]:
        pol = con.get("aggregationPolicy")
        if not pol:
            continue
        for col in con.get("columns", []):
            col_policy.setdefault(col, set()).add(pol)
    return {
        "TABLE_ATOMS": TABLE_ATOMS, "CEILINGS": CEILINGS, "FLOORS": FLOORS,
        "SIMPLE_RATE_NUMDEN": SIMPLE_RATE_NUMDEN,
        "COVERAGE_FROM": COVERAGE_FROM, "EVIDENCE_GAPS": EVIDENCE_GAPS, "BOUNDS": BOUNDS,
        "ERA_PBP": ERA_PBP, "ERA_FORMULA": ERA_FORMULA,
        "profiles": profiles, "col_policy": col_policy,
    }


def load_grain_schemas():
    import pyarrow.parquet as pq
    files = sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                   key=lambda p: Path(p).stat().st_mtime, reverse=True)
    weekly = files[0]
    art = Path(weekly).parent / "season_career_v26"
    paths = {
        "weekly": Path(weekly),
        "season": art / "player_nfl_season.parquet",
        "season_all": art / "player_nfl_season_all.parquet",
        "career": art / "player_nfl_career.parquet",
        "career_all": art / "player_nfl_career_all.parquet",
        "bio": Path(BIO_PARQUET),
    }
    schemas = {}
    for grain, p in paths.items():
        s = pq.read_schema(str(p))
        schemas[grain] = {n: str(s.field(n).type) for n in s.names}
    return schemas, {k: str(v) for k, v in paths.items()}


def measure_rate_scales(rate_cols, schemas, weekly_path, season_path):
    """percentage_scale per rate col: measured max > 1.5 => 0-100, else 0-1 (R-SCALE-MEASURED)."""
    import duckdb
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    out = {}
    for col in sorted(rate_cols):
        src = None
        if col in schemas["season"]:
            src = season_path
        elif col in schemas["weekly"]:
            src = weekly_path
        if src is None:
            continue
        mx = con.execute(
            f'SELECT MAX(TRY_CAST("{col}" AS DOUBLE)) FROM read_parquet(?) WHERE "{col}" IS NOT NULL',
            [src]).fetchone()[0]
        if mx is None:
            out[col] = {"scale": None, "measured_max": None}
        else:
            out[col] = {"scale": 100 if float(mx) > 1.5 else 1, "measured_max": round(float(mx), 3)}
    con.close()
    return out


def observed_floors():
    """Earliest year with >=3 nonzero rows per column, from the era_rescue scan (refreshed 2026-07-25)."""
    if not ERA_SCAN_JSON.exists():
        return {}
    cov = _read_json(ERA_SCAN_JSON)
    floors = {}
    for col, ys in cov.items():
        yrs = sorted(int(y) for y, n in ys.items() if n >= 3)
        if yrs:
            floors[col] = yrs[0]
    return floors


# ---------------------------------------------------------------------------------------------
# classification
# ---------------------------------------------------------------------------------------------

def family_of(col: str) -> str:
    if PROVENANCE_PAT.search(col) or col in PROVENANCE_EXACT:
        return "provenance"
    if col in IDENTITY_EXACT:
        return "identity"
    if col in CONTEXT_EXACT:
        return "context"
    if col in AWARD_SEASON_FLAGS or col in CAREER_STATIC or col.startswith("career_"):
        return "honors_bio"
    for p, fam in (("rank_", "rank"), ("lamar_", "lamar"), ("fpts_", "fantasy_points"),
                   ("pts_", "fantasy_points"), ("ppg_", "ppg"), ("rolling_", "ppg"),
                   ("weighted_ppg_", "ppg"), ("consistency_", "ppg"), ("avg_pts_next_year", "ppg"),
                   ("bonus_", "fantasy_points"), ("ngs_", "ngs"), ("rz_", "situational"),
                   ("def_", "defense"), ("fum", "fumbles"), ("sack", "passing"),
                   ("gwfg_", "kicking"), ("fg_", "kicking"), ("pat_", "kicking"),
                   ("punt_ret", "returns"), ("punt", "punting"), ("kickoff_", "returns"),
                   ("passing_", "passing"), ("pass_", "passing"), ("rushing_", "rushing"),
                   ("rush_", "rushing"), ("receiving_", "receiving"), ("receptions", "receiving"),
                   ("rec_", "receiving"), ("special_teams_", "special_teams"),
                   ("tack", "defense")):
        if col.startswith(p):
            return fam
    if col in ("season_ppg",):
        return "ppg"
    if col.endswith("_snap_pct") or col.endswith("_snaps"):
        return "snaps"
    return "general"


def domain_of(family: str) -> str:
    return {
        "identity": "identity", "context": "context", "provenance": "provenance",
        "honors_bio": "bio", "rank": "ranking", "lamar": "value_model",
        "fantasy_points": "fantasy_scoring", "ppg": "fantasy_scoring",
        "defense": "defense", "kicking": "kicking", "punting": "punting", "returns": "returns",
        "passing": "offense", "rushing": "offense", "receiving": "offense", "ngs": "offense",
        "situational": "offense", "snaps": "participation", "special_teams": "special_teams",
        "fumbles": "offense", "general": "general",
    }[family]


def build(check_only: bool = False) -> dict:
    L = load_legacy_inputs()
    schemas, paths = load_grain_schemas()
    grain_names = ["weekly", "season", "season_all", "career", "career_all", "bio"]
    universe = sorted(set().union(*[set(schemas[g]) for g in grain_names]))
    floors_obs = observed_floors()

    conflicts: list[dict] = []

    def conflict(stat, field, candidates, resolution, rule_id, note=""):
        assert rule_id in ADJUDICATION_RULES, f"unadjudicated conflict {stat}:{field} needs a rule"
        conflicts.append({"stat": stat, "field": field, "candidates": candidates,
                          "resolution": resolution, "rule_id": rule_id, "note": note})

    # --- witness floors + witness agg from TABLE_ATOMS -------------------------------------
    witness_atoms: dict[str, dict] = {}
    for table, (_pos, atoms) in L["TABLE_ATOMS"].items():
        for _src, v26col, agg, lo, hi in atoms:
            witness_atoms.setdefault(v26col, {})[f"pfr_{table}"] = {"era": [lo, hi], "agg": agg}

    # --- num/den consensus (golden_points vs AGG dependencies) -----------------------------
    numden: dict[str, tuple] = {}
    for col, (num, den) in L["SIMPLE_RATE_NUMDEN"].items():
        agg_dep = LEGACY_AGG_DERIVED_RATE_DEPENDENCIES.get(col)
        if agg_dep is not None and len(agg_dep) == 2 and tuple(agg_dep) != (num, den):
            conflict(col, "rate_numden",
                     {"golden_points": [num, den], "aggregate_nfl_stats_fly": list(agg_dep)},
                     [num, den], "R-NUMDEN-CONSENSUS",
                     "sources disagreed; golden_points (season+career locked) adopted")
        numden[col] = (num, den)

    # --- MAX class: union of AGG.MAX_COLS and TABLE_ATOMS max atoms -------------------------
    max_class = set(LEGACY_AGG_MAX_COLS)
    for col, ws in witness_atoms.items():
        if any(w["agg"] == "max" for w in ws.values()):
            if col not in max_class:
                conflict(col, "aggregation_class",
                         {"reconcile_atoms": "max", "aggregate_nfl_stats_fly.MAX_COLS": "absent"},
                         "MAX", "R-AGG-LONG-MAX")
            max_class.add(col)

    # --- rate columns the legacy exclusion sets omitted (validated-as-additive defect) ------
    for col in sorted(set(EXTRA_RATE_COMPONENTS) | DIRECT_RATE_NO_COMPONENTS):
        if col not in LEGACY_AGG_DERIVED_RATE_COLS:
            conflict(col, "aggregation_class",
                     {"legacy_exclusion_sets": "absent (validation treated as additive)",
                      "adjudicated": "RECOMPUTE_RATE"},
                     "RECOMPUTE_RATE", "R-RATE-LEGACY-OMISSION")

    # --- profiles-side generator-bug receipts (variant polarity flips) ----------------------
    for key, prof in L["profiles"].items():
        for suffix in ("_per_game", "_per_season"):
            if key.endswith(suffix):
                base = key[: -len(suffix)]
                bp = L["profiles"].get(base)
                if bp and bp["direction"] != prof["direction"]:
                    conflict(base, "polarity_variant",
                             {f"profiles[{key}]": prof["direction"], f"profiles[{base}]": bp["direction"]},
                             "variant inherits base polarity", "R-POLARITY-VARIANT-INHERIT")

    rate_cols_all = (set(LEGACY_AGG_DERIVED_RATE_COLS) | set(EXTRA_RATE_COMPONENTS)
                     | DIRECT_RATE_NO_COMPONENTS | MEASURED_RATE_COLS) & set(universe)
    scales = measure_rate_scales(rate_cols_all, schemas, paths["weekly"], paths["season"])

    stats_rows = []
    for col in universe:
        fam = family_of(col)
        dom = domain_of(fam)
        grains = [g for g in grain_names if col in schemas[g]]
        dtype = next(schemas[g][col] for g in grain_names if col in schemas[g])
        numeric = any(t in dtype for t in ("int", "float", "double", "decimal"))

        if "bio" in grains and not any(g in grains for g in ("weekly", "season", "season_all")):
            natural = "player_static"
        elif "weekly" in grains:
            natural = "player_game"
        elif "season" in grains or "season_all" in grains:
            natural = "player_season"
        else:
            natural = "player_career"

        pending: list[str] = []

        # ---------------- aggregation class ----------------
        research_pols = sorted(L["col_policy"].get(col, ()))
        if fam in ("identity", "context", "provenance"):
            agg_class, season_der = "NON_AGGREGATABLE", "none"
        elif col in ("games", "games_played"):
            agg_class, season_der = "EVENT_CARDINALITY", "count_rows"
        elif col in max_class:
            agg_class, season_der = "MAX", "max_weekly"
        elif col in LEGACY_AGG_WEIGHTED_AVG_COLS or col in NGS_DIRECT_RATE_COLS:
            agg_class, season_der = "WEIGHTED_RECOMPUTE", "recompute"
        elif col in rate_cols_all:
            agg_class, season_der = "RECOMPUTE_RATE", "recompute"
        elif col.startswith("rank_"):
            agg_class, season_der = "NON_AGGREGATABLE", "recompute"
        elif fam in ("ppg",) or col == "season_ppg" or col.startswith("lamar_"):
            agg_class, season_der = "NON_AGGREGATABLE", "recompute"
        elif col == "age":
            agg_class, season_der = "FIRST", "static_per_season"
        elif col in AWARD_SEASON_FLAGS:
            agg_class, season_der = "ANY", "static_per_season"
        elif col in CAREER_STATIC or natural == "player_static":
            agg_class, season_der = "FIRST", "static"
        elif numeric:
            agg_class = "SUM"
            if col in LEGACY_RA_FACT_ADJUSTED:
                season_der = "witness_direct_fact_adjusted"
            elif col.startswith("ngs_"):
                season_der = "published_season_total"
            else:
                season_der = "sum_weekly"
        else:
            agg_class, season_der = "NON_AGGREGATABLE", "none"

        # research candidate disagreement on agg class (recorded, never overriding)
        if research_pols and numeric and fam not in ("identity", "context", "provenance"):
            mapped = {"additive_sum": "SUM", "average_or_weighted_rate": "RECOMPUTE_RATE"}
            res_classes = {mapped.get(p) for p in research_pols if p in mapped}
            res_classes.discard(None)
            if res_classes and agg_class not in res_classes and agg_class in ("SUM", "MAX", "RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE"):
                conflict(col, "aggregation_class",
                         {"research_concept_graph": research_pols, "adjudicated": agg_class},
                         agg_class, "R-AGGPOLICY-HEURISTIC-CANDIDATE")

        # ---------------- rate spec ----------------
        rate = None
        if agg_class in ("RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE"):
            comps = None
            if col in numden:
                comps = list(numden[col])
                rate = {"numerator": numden[col][0], "denominator": numden[col][1]}
            elif col in MEASURED_RATE_DEPENDENCIES:
                comps = list(MEASURED_RATE_DEPENDENCIES[col])
                rate = {}
            elif col in LEGACY_AGG_DERIVED_RATE_DEPENDENCIES:
                comps = list(LEGACY_AGG_DERIVED_RATE_DEPENDENCIES[col])
                rate = {}
            elif col in EXTRA_RATE_COMPONENTS:
                comps = list(EXTRA_RATE_COMPONENTS[col])
                rate = {}
            elif col in DIRECT_RATE_NO_COMPONENTS or col in NGS_DIRECT_RATE_COLS:
                rate = {"direct_rate_no_components": True,
                        "note": "denominator (team/unit snap total) is not a stored column"}
            else:
                rate = {"direct_rate_no_components": True}
                pending.append("rate_components")
            if rate is not None and comps:
                rate["components"] = comps
            if col in NGS_CAREER_DENOMINATOR:
                rate["career_denominator"] = NGS_CAREER_DENOMINATOR[col]
            if col in LEGACY_AGG_WEIGHTED_AVG_COLS:
                rate = rate or {}
                rate["weighted_by"] = LEGACY_AGG_WEIGHTED_AVG_COLS[col]
            if col in LEGACY_AGG_SHARE_FAMILY_COLS:
                rate["share_family"] = True
                rate["note"] = ("denominator = team volume summed directly from the weekly table "
                                "(team_vol CTE); weekly share columns are never read")
            if col in scales:
                rate["percentage_scale"] = scales[col]["scale"]
                rate["measured_max"] = scales[col]["measured_max"]

        # ---------------- polarity + sort ----------------
        prof = L["profiles"].get(col)
        if fam in ("identity", "context", "provenance"):
            polarity, sort_dir = "neutral", None
        elif col in ADVERSE_EXACT or col in TIMING_DRILLS:
            polarity = "negative"
            sort_dir = "asc" if col in SORT_ASC else "desc"
            if prof and prof["direction"] == "higher_is_better":
                conflict(col, "benefit_polarity",
                         {"research_profiles": prof["direction"], "adjudicated": "negative"},
                         "negative", "R-POLARITY-ADVERSE")
        elif agg_class in ("RECOMPUTE_RATE",) and rate and rate.get("numerator") in ADVERSE_EXACT:
            polarity, sort_dir = "negative", "desc"
        elif col in CONTEXTUAL_POLARITY:
            polarity = "contextual"
            sort_dir = "asc" if col in SORT_ASC else "desc"
            if col == "age" and prof and prof["direction"] == "higher_is_better":
                conflict(col, "benefit_polarity",
                         {"research_profiles": "higher_is_better", "adjudicated": "contextual"},
                         "contextual", "R-UNIT-AGE-YEARS")
        elif col.startswith("rank_"):
            polarity, sort_dir = "negative", "asc"
        elif numeric:
            polarity, sort_dir = "positive", "desc"
        else:
            polarity, sort_dir = "neutral", None

        # ---------------- unit ----------------
        if col in UNIT_EXACT:
            unit = UNIT_EXACT[col]
            if col == "age" and prof and prof.get("unit") == "count":
                conflict(col, "unit", {"research_profiles": "count", "adjudicated": "age_years"},
                         "age_years", "R-UNIT-AGE-YEARS")
        elif fam in ("identity", "context", "provenance"):
            unit = None
        elif col in rate_cols_all or col.endswith("_share") or col.endswith("_pct") or col in ("pacr", "racr", "wopr"):
            unit = "ratio"
            if prof and prof.get("unit") not in (None, "percentage") and col.endswith("_share"):
                conflict(col, "unit", {"research_profiles": prof.get("unit"), "adjudicated": "ratio"},
                         "ratio", "R-UNIT-SHARE-RATIO")
        elif col.startswith(("fpts_", "pts_", "bonus_")) or col == "fantasy_points_ppr" or col.startswith("lamar_") or fam == "ppg":
            unit = "fantasy_points"
        elif col.startswith("rank_"):
            unit = "rank"
        elif col.endswith(("_epa",)):
            unit = "epa"
        elif col.endswith(("_wpa",)):
            unit = "win_probability"
        elif col.endswith(("_yards", "_yds", "_long")) or col in ("misc_yards",):
            unit = "yards"
        elif col.endswith(("_td", "_tds")) or col in ("pick6",) or "tds_" in col:
            unit = "touchdowns"
        elif numeric:
            unit = "count"
        else:
            unit = None

        # ---------------- eras ----------------
        eras: dict = {}
        event_like = agg_class in ("SUM", "MAX", "RECOMPUTE_RATE", "WEIGHTED_RECOMPUTE",
                                   "EVENT_CARDINALITY")
        cov = L["COVERAGE_FROM"].get(col)
        obs = floors_obs.get(col)
        if cov:
            eras["first_applicable_year"] = cov[0]
            eras["floor_source"] = "coverage_from"
            eras["position_domain"] = list(cov[1])
            if obs is not None and obs < cov[0]:
                conflict(col, "era_floor_observed_before_declared",
                         {"observed_scan": obs, "recon_expectations.COVERAGE_FROM": cov[0]},
                         cov[0], "R-FLOOR-OBSERVED-EARLIER")
        elif obs is not None and numeric and event_like and fam not in ("identity", "context", "provenance"):
            eras["first_applicable_year"] = obs
            eras["floor_source"] = "observed_scan"
        if obs is not None and event_like:
            eras["observed_floor_year"] = obs
        if col in L["ERA_PBP"]:
            floor, how = L["ERA_PBP"][col]
            eras["achievable_floor_year"] = floor
            eras["achievable_mechanism"] = how
            if cov and floor != cov[0]:
                conflict(col, "era_floor",
                         {"recon_expectations.COVERAGE_FROM": cov[0], "era_rescue.PBP": floor},
                         cov[0], "R-FLOOR-COVERAGE-CURATED")
        if col in witness_atoms:
            eras["witness_floors"] = {w: v["era"] for w, v in sorted(witness_atoms[col].items())}
            if cov and any(v["era"][0] != cov[0] for v in witness_atoms[col].values()):
                conflict(col, "era_floor",
                         {"reconcile_atoms.TABLE_ATOMS": {w: v["era"][0] for w, v in witness_atoms[col].items()},
                          "recon_expectations.COVERAGE_FROM": cov[0]},
                         cov[0], "R-FLOOR-WITNESS-SCOPED")
        gaps = []
        for year, (cols_, reason) in L["EVIDENCE_GAPS"].items():
            if col in cols_:
                gaps.append({"year": year, "reason": reason})
        if gaps:
            eras["evidence_gaps"] = gaps

        # ---------------- bounds ----------------
        bounds: dict = {}
        if col in L["BOUNDS"]:
            bounds["per_game_max"] = L["BOUNDS"][col]
            bounds["per_game_max_scope"] = "positions QB/RB/WR/TE/K/FB/HB (recon_expectations.BOUNDS)"
        if col in STRUCTURAL_PARENTS:
            bounds["structural_parents"] = STRUCTURAL_PARENTS[col]
        if col in FG_BUCKETS:
            bounds["partition_parent"] = "fg_made"
        if col in RECEPTION_BUCKETS:
            bounds["partition_parent"] = "receptions"
        bounds_status = "declared" if bounds else "none_declared_pending"
        if not bounds and numeric and fam not in ("identity", "context", "provenance", "rank"):
            pending.append("bound_set")

        # ---------------- formula deps ----------------
        formula = None
        if col in L["ERA_FORMULA"]:
            comps, note = L["ERA_FORMULA"][col]
            formula = {"components": list(comps)}
            if note:
                formula["note"] = note

        credit = ("identity" if fam == "identity" else
                  "context" if fam in ("context", "provenance") else
                  "derived_output" if (col.startswith(DERIVED_OUTPUT_PREFIXES) or fam in ("ppg", "rank", "lamar", "fantasy_points")) else
                  "player_credit")

        row = {
            "stat_id": col,
            "canonical_name": col,
            "family": fam,
            "domain": dom,
            "credit_type": credit,
            "dtype": dtype,
            "grains": grains,
            "natural_grain": natural,
            "aggregation_class": agg_class,
            "season_derivation": season_der,
            "benefit_polarity": polarity,
            "default_sort_direction": sort_dir,
            "unit": unit,
            "bounds_status": bounds_status,
            "tolerance_policy": TOLERANCE_POLICIES.get(col, {"policy": "EXACT"}),
            "definition_version": 1,
        }
        if rate:
            row["rate"] = rate
        if formula:
            row["formula_dependencies"] = formula
        if eras:
            row["eras"] = eras
        if bounds:
            row["bounds"] = bounds
        if col in ALIAS_OF:
            row["alias_of"] = ALIAS_OF[col]
        if col in ALIAS_OF.values():
            row["alias_children"] = sorted(k for k, v in ALIAS_OF.items() if v == col)
        if unit is None and numeric:
            pending.append("unit")
        if pending:
            row["pending_fields"] = sorted(set(pending))
        stats_rows.append(row)

    # ------------------------------------------------------------------------------------------
    # derive the consumer aggregation_sets FROM the rows, then prove parity with legacy behavior
    # ------------------------------------------------------------------------------------------
    by_id = {r["stat_id"]: r for r in stats_rows}
    derived_rate = sorted(c for c in LEGACY_AGG_DERIVED_RATE_COLS
                          if by_id.get(c, {}).get("aggregation_class") == "RECOMPUTE_RATE")
    weighted = {c: w for c, w in LEGACY_AGG_WEIGHTED_AVG_COLS.items()
                if by_id.get(c, {}).get("aggregation_class") == "WEIGHTED_RECOMPUTE"}
    maxc = sorted(c for c, r in by_id.items() if r["aggregation_class"] == "MAX")
    fact_adj = sorted(c for c, r in by_id.items()
                      if r.get("season_derivation") == "witness_direct_fact_adjusted")
    deps = {}
    for c in derived_rate:
        r = by_id[c].get("rate", {})
        comps = r.get("components")
        if comps:
            deps[c] = list(comps)

    agg_sets = {
        "derived_rate_cols": derived_rate,
        "derived_rate_dependencies": deps,
        "weighted_avg_cols": weighted,
        "max_cols": sorted(LEGACY_AGG_MAX_COLS),  # season-builder scope: longs present at season grain
        "max_cols_all_grains": maxc,
        "per_game_avg_cols": dict(LEGACY_AGG_PER_GAME_AVG_COLS),
        "share_family_cols": sorted(LEGACY_AGG_SHARE_FAMILY_COLS),
        "fact_adjusted_season_cols": fact_adj,
        "non_additive_exact": sorted(LEGACY_RA_NONADD_EXACT),
        "non_additive_prefixes": list(LEGACY_RA_NONADD_PREFIX),
    }

    # parity gates (behavior-neutral switch proof)
    assert set(agg_sets["derived_rate_cols"]) == LEGACY_AGG_DERIVED_RATE_COLS, \
        f"rate parity break: {set(agg_sets['derived_rate_cols']) ^ LEGACY_AGG_DERIVED_RATE_COLS}"
    assert {k: tuple(v) for k, v in agg_sets["derived_rate_dependencies"].items()} == \
        LEGACY_AGG_DERIVED_RATE_DEPENDENCIES, "rate dependency parity break"
    assert agg_sets["weighted_avg_cols"] == LEGACY_AGG_WEIGHTED_AVG_COLS, "weighted parity break"
    assert set(agg_sets["max_cols"]) == LEGACY_AGG_MAX_COLS, "max parity break"
    assert set(agg_sets["fact_adjusted_season_cols"]) == LEGACY_RA_FACT_ADJUSTED, "fact-adjusted parity break"
    assert set(agg_sets["non_additive_exact"]) == LEGACY_RA_NONADD_EXACT
    assert tuple(agg_sets["non_additive_prefixes"]) == LEGACY_RA_NONADD_PREFIX
    # every legacy non-additive exact col must genuinely be non-SUM in the registry
    for c in LEGACY_RA_NONADD_EXACT:
        r = by_id.get(c)
        if r is not None:
            assert r["aggregation_class"] != "SUM" or r["season_derivation"] != "sum_weekly", \
                f"{c} is legacy-non-additive but registry says plain SUM"

    # ---- NO RATE MAY BE TYPED SUM ----------------------------------------------------
    # The defect this catches shipped and survived: 10 canonicals whose NAME is a rate
    # carried aggregation_class=SUM, because the legacy set holds only the short spellings
    # and the family-prefixed twins fell through. `stats_without_tolerance_policy` read
    # clean over all of them -- it checks that a policy EXISTS, never that the aggregation
    # class is coherent with the unit. Summing a per-attempt average across weeks is not a
    # tolerance question, it is a wrong number.
    #
    # A genuine count that merely LOOKS like a rate must be named in RATE_MORPHOLOGY_EXEMPT
    # with its reason, so declining costs one line and missing one fails the build.
    import re as _re
    _RATE_NAME = _re.compile(r"(_per_[a-z]+$|_pct$|_rate$|_avg$|_percentage$)")
    _offenders = sorted(
        r["canonical_name"] for r in stats_rows
        if _RATE_NAME.search(r["canonical_name"])
        and r["aggregation_class"] == "SUM"
        and r["canonical_name"] not in RATE_MORPHOLOGY_EXEMPT)
    assert not _offenders, (
        "canonicals whose name is a rate but whose contract says SUM -- a season rollup "
        f"would add averages together: {_offenders}")

    # coverage gate: exactly one row per live column
    assert len(stats_rows) == len(universe) == len(by_id), "row count / duplicate stat_id break"
    for cf in conflicts:
        assert cf["rule_id"] in ADJUDICATION_RULES and ADJUDICATION_RULES[cf["rule_id"]], \
            f"conflict without receipt: {cf}"

    registry = {
        "contract_version": "1",
        "generated_by": "scripts/sota_recon/witness_gate/build_stat_contracts.py",
        "authority": ("AUTHORITATIVE per master plan §25.7: legacy semantic metadata (research "
                      "concept graph / numeric profiles) is candidate input only; conflicts are "
                      "adjudicated below, never silently merged."),
        "inputs": {
            "research_profiles": {"path": "docs/research-mode-numeric-profiles.json",
                                  "fingerprint": _sha256(PROFILES_JSON)},
            "research_concept_graph": {"path": "docs/research-mode-semantic-concept-graph.json",
                                       "fingerprint": _sha256(CONCEPTS_JSON)},
            "grain_tables": paths,
            "era_scan": str(ERA_SCAN_JSON),
            "legacy_modules": [
                "scripts/sota_recon/reconcile_atoms.py:TABLE_ATOMS/CEILINGS/FLOORS",
                "scripts/sota_recon/golden_points.py:SIMPLE_RATE_NUMDEN",
                "scripts/sota_recon/recon_expectations.py:COVERAGE_FROM/EVIDENCE_GAPS/BOUNDS",
                "scripts/sota_recon/witness_audit_v2/era_rescue.py:PBP/FORMULA",
                "frozen: aggregate_nfl_stats_fly + recon_aggregate declaration sets (see builder)",
            ],
        },
        "counts": {
            "stats": len(stats_rows),
            "conflicts": len(conflicts),
            "by_grain": {g: sum(g in row["grains"] for row in stats_rows) for g in grain_names},
            "pending_fields_rows": sum(1 for r in stats_rows if r.get("pending_fields")),
        },
        "adjudication_rules": ADJUDICATION_RULES,
        "conflicts": sorted(conflicts, key=lambda c: (c["stat"], c["field"])),
        "aggregation_sets": agg_sets,
        "stats": stats_rows,
    }
    return registry


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="rebuild and diff against committed registry")
    a = ap.parse_args()
    reg = build()
    text = json.dumps(reg, indent=1, sort_keys=False, ensure_ascii=True)
    if a.check:
        old = OUT.read_text(encoding="utf-8") if OUT.exists() else ""
        if old == text:
            print("check: committed registry matches regeneration")
        else:
            print("check: DRIFT between committed registry and regeneration")
            raise SystemExit(1)
    else:
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(text, encoding="utf-8")
        c = reg["counts"]
        print(f"wrote {OUT}")
        print(f"stats={c['stats']} conflicts={c['conflicts']} pending_rows={c['pending_fields_rows']}")
        print(f"by_grain={c['by_grain']}")
        from collections import Counter
        print("agg classes:", dict(Counter(r['aggregation_class'] for r in reg['stats']).most_common()))
        print("conflict fields:", dict(Counter(f"{x['field']}" for x in reg['conflicts']).most_common()))


if __name__ == "__main__":
    main()
