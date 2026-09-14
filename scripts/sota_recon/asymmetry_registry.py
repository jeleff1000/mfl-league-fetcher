"""
sota_recon/asymmetry_registry.py

The documented set of LEGITIMATE reasons a reconciliation check can "fail" without
the data being wrong. This is what stops the internal audit from crying wolf at a
70% flag rate: a discrepancy is only a finding if it is NOT explained here.

Two kinds of entries:

  EXPECTED_GAPS  - a (stat family, era) where absence is correct, not a hole.
                   e.g. targets do not exist before 1978; official sacks before 1982.

  TOLERANCES     - numeric slack for cross-side / cross-source equality checks that
                   never tie exactly for real football reasons (lateral yardage,
                   sack-yard attribution, fumble-out-of-bounds, rounding).

Edit this file to tune signal/noise. Every lane imports from here so the rules live
in exactly one place.
"""

from __future__ import annotations

# --- (stat_family, era) absences that are EXPECTED, not errors ---------------------
# era tokens come from recon_common.era_of(): early_box | mid_box | pre_pbp | modern
EXPECTED_GAPS = {
    # targets were not recorded league-wide until play-by-play (1978)
    ("targets", "early_box"), ("targets", "mid_box"), ("targets", "pre_pbp"),
    # official sacks (defensive) begin 1982; sacks_suffered (offense) begin 1969 unofficially
    ("def_sacks", "early_box"), ("def_sacks", "mid_box"), ("def_sacks", "pre_pbp"),
    ("sacks_suffered", "early_box"), ("sacks_suffered", "mid_box"),
    # IDP tackles are not systematically recorded before the modern era
    ("def_tackles_solo", "early_box"), ("def_tackles_solo", "mid_box"), ("def_tackles_solo", "pre_pbp"),
    ("def_tackle_assists", "early_box"), ("def_tackle_assists", "mid_box"), ("def_tackle_assists", "pre_pbp"),
    # receiving yards/receptions sparse but present pre-1933; do not hard-fail early box
    ("receptions", "early_box"),
    # air-yards / pressure advanced stats are modern-only
    ("receiving_adot", "early_box"), ("receiving_adot", "mid_box"), ("receiving_adot", "pre_pbp"),
    ("passing_pressured", "early_box"), ("passing_pressured", "mid_box"), ("passing_pressured", "pre_pbp"),
}


def is_expected_gap(stat_family: str, era: str) -> bool:
    return (stat_family, era) in EXPECTED_GAPS


# --- numeric tolerances for equality checks ----------------------------------------
# These are *team-game* level tolerances unless noted. A residual within tolerance is
# NOT a finding; it is the known accounting slack for that comparison.
TOLERANCES = {
    # cross-side (offense vs opposing defense) within one game
    "int_symmetry": 0,            # INTs thrown should equal INTs caught exactly
    "sack_symmetry": 0,           # team sacks == opp QB sacks-suffered (1982+)
    "fumble_recovery_symmetry": 2,  # fumbles out of bounds / scrum credit fuzz

    # within-team (QB passing vs WR/TE/RB receiving) yards, one game
    "pass_recv_yards": 15,        # scrambles, sack-yard accounting, laterals

    # vertical: sum of player stats vs published team total, one game
    "team_total_pass_yards": 12,
    "team_total_rush_yards": 12,

    # second-source corroboration: per player-week per stat absolute difference
    "oracle_yards": 2,            # rounding / play-attribution between sources
    "oracle_counts": 0,           # tds, receptions, attempts: should match exactly
    "oracle_score": 0,            # team points: exact

    # schedule anchor: DEF score vs schedule score
    "schedule_score": 0,
}


def tol(name: str) -> float:
    return float(TOLERANCES[name])


# --- documented residuals: known, characterized, NOT bugs --------------------------
# Each entry is a finding the harness will keep surfacing but which is understood and
# accepted (sourcing-limited or inherent to the era/data). Recorded here so the audit
# verdict can distinguish "accepted residual" from "new regression".
DOCUMENTED_RESIDUALS = {
    "schedule.ancient_def_score_mismatch": (
        "~34 pre-1939 DEF score disagreements vs schedule, concentrated in FRN/Frankford "
        "doubleheader weeks where a week-level join cannot distinguish the two games a "
        "team played in one week. Ancient-era data ambiguity, not a pipeline error."),
    "internal.offense_completeness_pre_pbp": (
        "LARGELY RESOLVED 2026-06-19 by build_pre1978_backfill (player_offense box score): "
        "82,117 player-weeks updated + 4,675 inserted for 1932-77. Team-game offensive "
        "yardage now reconciles to the box score at 1950-77 100.0%, 1932-49 ~101% (v26 "
        "marginally richer than the box-score lineage on the oldest games -- irreducible "
        "cross-source disagreement, not a hole). PRE-1932 still has no player_offense source "
        "(PFR box scores start 1932) so 1920-31 player-game gaps remain unsourceable."),
    "internal.offense_completeness_modern": (
        "~1,548 modern comp_rec/pass_recv mismatches (~2 receptions/game): minor box-score "
        "incompleteness. Fixable in principle from the PBP oracle but requires adding rows; "
        "deferred as low-value."),
    "oracle.unmatched_zero_activity": (
        "~21,501 of 21,789 oracle-unmatched player-weeks are ZERO-activity roster rows the "
        "PBP rollup does not carry. v26 is MORE complete here, not less. Only ~288 unmatched "
        "rows have real activity."),
    "seam.real_historical_transitions": (
        "8 year-over-year league-mean jumps, all at genuine era boundaries (1932->33 box "
        "completeness, 1946->47 postwar passing, 1959->60 AFL expansion). Real football, "
        "not transform artifacts."),
    "identity.chicago_bears_cardinals_RESOLVED": (
        "RESOLVED: v26 correctly separates the Chicago Bears (code CHI, franchise 5) from "
        "the Chicago Cardinals (code CRD, franchise 13). The earlier '218 disagreements' were "
        "a VALIDATION ARTIFACT: the schedule source overloads the 'CHI' code for the Cardinals "
        "(235 Cardinals games coded CHI/13), so a code-based join mismatched v26-Bears-CHI to "
        "schedule-Cardinals-CHI. Corrections join on franchise NUMBER (5 vs 13, consistent in "
        "both sources) so were never affected. Residual: ~27 single-game 1920-1925 opponent "
        "disagreements are week-numbering offsets between sources (ancient schedule reconstruction), "
        "not franchise conflation."),
    "corrections.def_int_enforced_incomplete_offense": (
        "262 def_interceptions values enforced from opponent offense in games where the "
        "offense box is incomplete (comp != rec). Verified: 84/85 oracle-checkable cases the "
        "offense INT count matched the independent oracle, so the enforcement held; ~177 "
        "pre-1978 cases are unverifiable against PBP. Low risk, flagged for completeness."),
    "corrections.synthetic_floors": (
        "20 logical-minimum values WE wrote (6 targets=receptions, 14 attempts=completions) "
        "to clear physical impossibilities where the true value is unsourceable. Tagged in "
        "recon_correction_log; minimums, not exact, and not independently sourced."),
    "oracle.idp_tackle_subjectivity": (
        "def_tackles_solo agrees with the PBP oracle ~88%, def_tackles_for_loss ~98%. "
        "Tackles are UNOFFICIAL and source-subjective (different sources credit solo vs "
        "assisted differently), so per-source variance is inherent, not a v26 error. The "
        "broader non-tackle IDP set (sacks, QB hits, PD, FF, returns) corroborates 99%+."),
    "oracle.def_tackles_with_assist_name_collision": (
        "v26's def_tackles_with_assist = COMBINED tackles (= its own solo+assist, 100% "
        "internally consistent). The PBP oracle uses the same column NAME for a different "
        "concept, so a direct comparison is apples-to-oranges (excluded from the oracle "
        "lane via _NAME_COLLISION). v26's value is validated by its internal identity."),
    "audit.non_derived_coverage": (
        "Non-derived audit status: 73 stats oracle-corroborated 99.69% / 52.7M cells (1978+) "
        "- core offense, DEF scoring, returns, punting, kicking, fumbles, IDP (ex-tackles), "
        "explosive/reception buckets. Composition identities (bucket sums) pass all eras. "
        "2-pt: era-gate clean (0 pre-1994) + cross-side 99.9%. First downs: cross-side 98% "
        "(era-appropriate coverage, 0 pre-1994). Wave 5 fixed 97 structural impossibilities "
        "(82 recv-bucket overcounts NULLed, 13 punt_long NULLed, 2 pat identity). "
        "ONLY REMAINING UNAUDITED: advanced metrics (23 cols: air yards, YAC, pressures, "
        "broken tackles, ADOT) - modern-only (2006+/2018+), need the dedicated advanced "
        "source (not in the PBP rollup). Everything else non-derived is corroborated."),
    "scoring.defensive_st_double_count": (
        "STRUCTURAL: defensive/special-teams scores (def_tds, fum_ret_td, special_teams_tds, "
        "def_safeties) are recorded on BOTH the team DEF row AND the individual player rows. "
        "Summing those columns across all rows double-counts. The scoring lane counts "
        "non-offensive scoring from the DEF row only. Any analytics summing these columns "
        "team-wide must pick ONE level. Candidate for a future normalization."),
    "scoring.decomposition_completeness_by_era": (
        "DEFINITIVE (v26 vs authoritative PFR scoring_summary, points exact by construction): "
        "v26 FINAL SCORES match 99.73% all-era (modern 99.8%, early_box 96.7%) -> the scores are "
        "right. But v26's per-player scoring DECOMPOSITION reconstructs the score only ~59% "
        "(modern 79%, pre_pbp 24%, mid_box 10%) -> which players scored which TDs/FGs is "
        "incomplete in older eras. The authoritative source to backfill it now exists at "
        "derived/scoring_summary/scoring_summary.parquet (built from scoring_combined, 99.98% "
        "internally exact, 1920-2025). FIX PATH: attribute missing TDs/FGs via the scoring "
        "tables' description_link_ids and backfill v26's scoring atoms (next correction wave)."),
    "teamtotal.mid_box_yardage_incompleteness": (
        "mid_box (1933-49) team-games where summed player yards fall short of the schedule "
        "team total: incomplete individual box scores for that era; no finer source exists."),
    "team_dst.pa_no_scoreboard_witness": (
        "44 DEF rows (ancient games, genuine data gaps) have no matching PFR team-game "
        "scoreboard row, so build_pa_reconciled_v26 cannot anchor their points_allowed / "
        "dst_points_allowed to the opponent's official score; they keep their prior reconstructed "
        "value. EVERY other DEF row now anchors exactly: points_allowed == opponent_points 100%. "
        "The join is the unique game (franchise, game_date), falling back to (year, week, fr, "
        "opponent_fr) only where game_date is null -- this resolves all same-opponent doubleheaders. "
        "Not a pipeline error: no source score exists for these games."),
    "team_dst.points_allowed_is_scoreboard": (
        "DESIGN (post 2026-07 fix): points_allowed = the OFFICIAL scoreboard (opponent_points), NOT a "
        "reconstruction from component TD/FG/PAT counts -- the old `pts_allow = tds*6 + 2pt*2 + fg*3 + "
        "pat*1` reconstruction inflated it ~6/opp-TD for 2014+ (matched scoreboard only ~42%). "
        "dst_points_allowed = points_allowed - 6*(opp int-ret + fum-ret TDs) - 2*(opp safeties) is the "
        "fantasy-eligible value (keeps PATs/2pt/ST-return-TDs). Enforced by the consensus_reconcile "
        "tg_auth witness on points_allowed and the pts_def_std tier now reads dst_points_allowed."),
}



# --- stat families compared by the oracle lane, grouped by tolerance class ---------
# (subject_col, oracle_col, tolerance_key)
ORACLE_STAT_PAIRS = [
    ("attempts",              "attempts",              "oracle_counts"),
    ("completions",           "completions",           "oracle_counts"),
    ("passing_yards",         "passing_yards",         "oracle_yards"),
    ("passing_tds",           "passing_tds",           "oracle_counts"),
    ("passing_interceptions", "passing_interceptions", "oracle_counts"),
    ("carries",               "carries",               "oracle_counts"),
    ("rushing_yards",         "rushing_yards",         "oracle_yards"),
    ("rushing_tds",           "rushing_tds",           "oracle_counts"),
    ("targets",               "targets",               "oracle_counts"),
    ("receptions",            "receptions",            "oracle_counts"),
    ("receiving_yards",       "receiving_yards",       "oracle_yards"),
    ("receiving_tds",         "receiving_tds",         "oracle_counts"),
    ("fg_att",                "fg_att",                "oracle_counts"),
    ("fg_made",               "fg_made",               "oracle_counts"),
]
