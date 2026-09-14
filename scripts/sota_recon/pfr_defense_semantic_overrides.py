"""Apply proven defensive PFR semantic overrides to the column ledger.

The advanced-defense receipt proves that ``def_yds_per_target`` is the recomputable
completion-yards-allowed / targets-allowed witness. It must not point at the offensive
receiving rate column. The paired ``def_tgt_yds_per_att`` remains a candidate because its
publisher denominator is not yet identified.
"""
from __future__ import annotations

import json
from pathlib import Path

from scripts.sota_recon.column_dossier import DISPOSITIONS_PATH


GENERATOR = "scripts.sota_recon.pfr_defense_semantic_overrides"


def escalated_row_keys() -> dict[str, str]:
    """This pass closes a semantic mis-map; it does not create new escalations."""
    return {}
OVERRIDES = {
    # Context, season/bio, and derivable witnesses are audited but are not
    # independent weekly supertable promotions.
    "pfr_all_pro_members|*|experience": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "CONTEXT/DERIVED WITNESS: PFR experience is a per-honor-row NFL-experience value. Canonical layers carry rookie_year, years_active, and seasons_started; do not promote a duplicate weekly statistic.",
        "evidence": "PFR membership audit plus player_bio schema.",
    },
    "pfr_pro_bowl_members|*|experience": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "CONTEXT/DERIVED WITNESS: PFR experience is a per-honor-row NFL-experience value. Canonical layers carry rookie_year, years_active, and seasons_started; do not promote a duplicate weekly statistic.",
        "evidence": "PFR membership audit plus player_bio schema.",
    },
    "pfr_all_pro_members|*|gs": {
        "disposition": "MAPPED_TO_CANONICAL",
        "canonical": "games_started",
        "reason": "SEASON CONTEXT MAPPING: PFR GS is games started and belongs to the season/bio presence lane, not a new weekly supertable column.",
        "evidence": "PFR membership audit; canonical season/bio games-started family.",
    },
    "pfr_pro_bowl_members|*|gs": {
        "disposition": "MAPPED_TO_CANONICAL",
        "canonical": "games_started",
        "reason": "SEASON CONTEXT MAPPING: PFR GS is games started and belongs to the season/bio presence lane, not a new weekly supertable column.",
        "evidence": "PFR membership audit; canonical season/bio games-started family.",
    },
    "pfr_pro_bowl_members|*|conference_id": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "CONTEXT LANE: PFR conference_id means NFL conference (AFC/NFC) at the time of the honor, not college conference. Route it with team-season/membership context.",
        "evidence": "PFR membership audit; conference is time-varying franchise context.",
    },
    "pfr_games_played|*|reason": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "CONTEXT LANE: PFR reason means why a player did not play (injury, coach decision, suspension, etc.). It is not a weekly stat counter.",
        "evidence": "PFR games-played audit.",
    },
    "pfr_snap_counts|*|reason": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "CONTEXT LANE: PFR reason means why a player did not play (injury, coach decision, suspension, etc.). It is not a weekly stat counter.",
        "evidence": "PFR snap-count audit.",
    },
    "pfr_games_played|*|uniform_number": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "BIO/SEASON DESTINATION: jersey number belongs in player_bio and season identity context, not as a weekly statistical promotion.",
        "evidence": "PFR games-played audit.",
    },
    "pfr_snap_counts|*|uniform_number": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "BIO/SEASON DESTINATION: jersey number belongs in player_bio and season identity context, not as a weekly statistical promotion.",
        "evidence": "PFR snap-count audit.",
    },
    "pfr_player_season_passing|*|comebacks": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: fourth-quarter comebacks are derived from weekly game/PBP state transitions; do not independently promote the PFR season scalar.",
        "evidence": "PFR passing audit; weekly/PBP is the lower layer.",
    },
    "pfr_passing_post|*|comebacks": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: fourth-quarter comebacks are derived from weekly game/PBP state transitions; do not independently promote the PFR postseason scalar.",
        "evidence": "PFR passing audit; weekly/PBP is the lower layer.",
    },
    "pfr_player_season_passing|*|gwd": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: game-winning drives are derived from weekly game/PBP state transitions; do not independently promote the PFR season scalar.",
        "evidence": "PFR passing audit; weekly/PBP is the lower layer.",
    },
    "pfr_passing_post|*|gwd": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: game-winning drives are derived from weekly game/PBP state transitions; do not independently promote the PFR postseason scalar.",
        "evidence": "PFR passing audit; weekly/PBP is the lower layer.",
    },
    "pfr_player_season_passing|*|qb_rec": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: quarterback won-lost-tied record is derivable for every player with starter and team-game outcome facts; retain PFR as a witness.",
        "evidence": "PFR passing audit; games-started and team-game outcomes are the lower layers.",
    },
    "pfr_passing_post|*|qb_rec": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: quarterback won-lost-tied record is derivable for every player with starter and team-game outcome facts; retain PFR as a witness.",
        "evidence": "PFR passing audit; games-started and team-game outcomes are the lower layers.",
    },
    # Existing canonical fields are mapping/definition adjudications, not
    # missing-column promotions.
    "pfr_player_scoring|*|total_tds_scored": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical total_tds_scored already exists. PFR total_tds_scored does not equal it on all 2025 rows; resolve the component-definition or mapping gap instead of promoting a duplicate.",
        "evidence": "PFR scoring audit: total_tds_scored mismatches against canonical total_tds_scored.",
    },
    "pfr_scoring_post|*|total_tds_scored": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical total_tds_scored already exists. PFR postseason total_tds_scored has no proven component match; do not promote a duplicate.",
        "evidence": "PFR postseason scoring audit.",
    },
    "pfr_player_scoring|*|scoring": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical scoring/points fields already exist. PFR scoring mismatches indicate a component-definition or mapping problem, not a missing duplicate scalar.",
        "evidence": "PFR scoring audit: 2025 mismatches against canonical scoring totals.",
    },
    "pfr_scoring_post|*|scoring": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical scoring/points fields already exist. Explain the postseason mismatch before accepting a mapping.",
        "evidence": "PFR postseason scoring audit.",
    },
    "pfr_player_scoring|*|xpa": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical pat_att exists. PFR xpa differs where blocked PAT attempts are treated differently; investigate the denominator instead of promoting a duplicate.",
        "evidence": "PFR scoring audit: xpa mismatches while PAT makes agree.",
    },
    "pfr_scoring_post|*|xpa": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical pat_att exists. PFR xpa must be reconciled for blocked-PAT treatment before mapping; it is not a new generic attempt column.",
        "evidence": "PFR postseason scoring audit and regular-season denominator evidence.",
    },
    "pfr_player_kicking|*|xpa": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical pat_att exists. PFR xpa differs where blocked PAT attempts are treated differently; investigate the denominator instead of promoting a duplicate.",
        "evidence": "PFR kicking audit: xpa mismatches while PAT makes agree.",
    },
    "pfr_kicking_post|*|xpa": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical pat_att exists. PFR xpa must be reconciled for blocked-PAT treatment before mapping; it is not a new generic attempt column.",
        "evidence": "PFR postseason kicking audit and regular-season denominator evidence.",
    },
    "pfr_player_fantasy|*|fantasy_points": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "MAPPING ADJUDICATION: canonical fantasy scoring fields exist. PFR fantasy_points is a publisher scoring/rounding witness with unresolved 2025 differences, not automatically a new canonical stat.",
        "evidence": "PFR fantasy audit: 470/549 exact standard-scoring comparisons.",
    },
    "pfr_player_scoring|*|two_pt_md": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED/MAPPING WITNESS: canonical passing, rushing, and receiving two-point components exist. Prove their sum against PFR two_pt_md before adding a total.",
        "evidence": "PFR scoring audit; canonical two-point components are present.",
    },
    "pfr_scoring_post|*|two_pt_md": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED/MAPPING WITNESS: canonical passing, rushing, and receiving two-point components exist. Prove their sum against PFR two_pt_md before adding a total.",
        "evidence": "PFR postseason scoring audit; canonical two-point components are present.",
    },
    # Made-distance bands already exist canonically; attempted bands remain
    # genuine gaps.
    "pfr_player_kicking|*|fgm1": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_0_19", "reason": "DIRECT CANONICAL MAPPING: PFR fgm1 (made, 1-19) maps to fg_made_0_19.", "evidence": "Supertable schema contains fg_made_0_19."},
    "pfr_player_kicking|*|fgm2": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_20_29", "reason": "DIRECT CANONICAL MAPPING: PFR fgm2 (made, 20-29) maps to fg_made_20_29.", "evidence": "Supertable schema contains fg_made_20_29."},
    "pfr_player_kicking|*|fgm3": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_30_39", "reason": "DIRECT CANONICAL MAPPING: PFR fgm3 (made, 30-39) maps to fg_made_30_39.", "evidence": "Supertable schema contains fg_made_30_39."},
    "pfr_player_kicking|*|fgm4": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_40_49", "reason": "DIRECT CANONICAL MAPPING: PFR fgm4 (made, 40-49) maps to fg_made_40_49.", "evidence": "Supertable schema contains fg_made_40_49."},
    "pfr_player_kicking|*|fgm5": {"disposition": "EXCLUDED_WITH_REASON", "reason": "DERIVED WITNESS: PFR fgm5 is made field goals at 50+, while the canonical schema splits 50-59 and 60+; verify the sum rather than inventing a single canonical column.", "evidence": "Supertable schema contains fg_made_50_59 and the 60+ made canonical."},
    "pfr_kicking_post|*|fgm1": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_0_19", "reason": "DIRECT CANONICAL MAPPING: PFR postseason fgm1 maps to fg_made_0_19.", "evidence": "Supertable schema contains fg_made_0_19."},
    "pfr_kicking_post|*|fgm2": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_20_29", "reason": "DIRECT CANONICAL MAPPING: PFR postseason fgm2 maps to fg_made_20_29.", "evidence": "Supertable schema contains fg_made_20_29."},
    "pfr_kicking_post|*|fgm3": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_30_39", "reason": "DIRECT CANONICAL MAPPING: PFR postseason fgm3 maps to fg_made_30_39.", "evidence": "Supertable schema contains fg_made_30_39."},
    "pfr_kicking_post|*|fgm4": {"disposition": "MAPPED_TO_CANONICAL", "canonical": "fg_made_40_49", "reason": "DIRECT CANONICAL MAPPING: PFR postseason fgm4 maps to fg_made_40_49.", "evidence": "Supertable schema contains fg_made_40_49."},
    "pfr_kicking_post|*|fgm5": {"disposition": "EXCLUDED_WITH_REASON", "reason": "DERIVED WITNESS: PFR postseason fgm5 is made field goals at 50+, while the canonical schema splits 50-59 and 60+; verify the sum rather than inventing a single canonical column.", "evidence": "Supertable schema contains fg_made_50_59 and the 60+ made canonical."},
    "pfr_adv_defense|*|def_yds_per_target": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED DEFENSIVE RATE: PFR def_yds_per_target = completion_yards_allowed / targets_allowed. It is not the offensive receiving_yards_per_target column; the lower-layer defensive operands reconcile independently.",
        "evidence": "docs/audits/pfr-pfr_adv_defense-2025.json and docs/audits/pfr-pfr_adv_defense_post-2025.json: PFR numerator/denominator equations pass, while mapping to receiving_yards_per_target crosses defensive/offensive families.",
    },
    "pfr_adv_defense_post|*|def_yds_per_target": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED DEFENSIVE RATE: PFR def_yds_per_target = completion_yards_allowed / targets_allowed. It is not the offensive receiving_yards_per_target column; the lower-layer defensive operands reconcile independently.",
        "evidence": "docs/audits/pfr-pfr_adv_defense-2025.json and docs/audits/pfr-pfr_adv_defense_post-2025.json: PFR numerator/denominator equations pass, while mapping to receiving_yards_per_target crosses defensive/offensive families.",
    },
    "pfr_player_kicking|*|xpa": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR xpa does not equal the current pat_att denominator on 2025 regular-season rows: PFR excludes one or more blocked PAT attempts while the weekly layer includes them. Preserve as a promotion candidate for a separately defined unblocked-PAT-attempts counter; no backfill performed.",
        "evidence": "docs/audits/pfr-pfr_player_kicking-2025.json: 8 denominator mismatches; affected kickers differ by one attempt while made PATs agree.",
    },
    "pfr_kicking_post|*|xpa": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR xpa is retained as a candidate until its blocked-PAT denominator is explicitly aligned with the weekly pat_att definition; no 2025 postseason mismatch was observed, but regular-season evidence shows the surfaces are not safely interchangeable.",
        "evidence": "docs/audits/pfr-pfr_player_kicking-2025.json and docs/audits/pfr-pfr_kicking_post-2025.json.",
    },
    "pfr_player_kicking|*|xp_pct": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: PFR xp_pct = xpm / xpa. Its denominator is the PFR xpa definition, which is not currently the weekly pat_att definition on regular-season rows.",
        "evidence": "PFR xpm/xpa operands are present in the same table; xpa is a locked promotion candidate pending denominator adjudication.",
    },
    "pfr_kicking_post|*|xp_pct": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: PFR xp_pct = xpm / xpa; retain the published rate as a witness to PFR's own operands rather than independently mapping it.",
        "evidence": "PFR xpm/xpa operands are present in the same table; the regular-season denominator audit prevents treating pat_att as interchangeable without a definition decision.",
    },
    "pfr_player_punting|*|punt_yds_per_punt": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: PFR punt_yds_per_punt = punt_yds / punt; both operands are published by PFR and the 2025 equation reconciles against the weekly punt_yards / punts layer.",
        "evidence": "docs/audits/pfr-pfr_player_punting-2025.json: published rate is verified from its numerator and denominator; no independent canonical cell is required.",
    },
    "pfr_punting_post|*|punt_yds_per_punt": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "DERIVED WITNESS: PFR punt_yds_per_punt = punt_yds / punt; both operands are published by PFR and the postseason equation reconciles against the weekly punt_yards / punts layer.",
        "evidence": "docs/audits/pfr-pfr_punting_post-2025.json: published rate is verified from its numerator and denominator; no independent canonical cell is required.",
    },
    "pfr_player_returns|*|all_purpose_yds": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "PFR all_purpose_yds is a composite season total; its exact composition must be witnessed from the weekly rushing, receiving, and return lower layers before it can be treated as the scalar all_purpose_yards mapping.",
        "evidence": "docs/audits/pfr-pfr_player_returns-2025.json; direct return counters reconcile, but this receipt does not yet prove the composite equation across all lower-layer components.",
    },
    "pfr_returns_post|*|all_purpose_yds": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "PFR all_purpose_yds is a composite postseason total; its exact composition must be witnessed from the weekly rushing, receiving, and return lower layers before it can be treated as the scalar all_purpose_yards mapping.",
        "evidence": "docs/audits/pfr-pfr_returns_post-2025.json; direct return counters reconcile, but this receipt does not yet prove the composite equation across all lower-layer components.",
    },
    "pfr_player_scoring|*|scoring": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR scoring is a composite player scoring total whose 2025 regular-season values exceed weekly total_points_scored on rows involving defensive, special-teams, two-point, or other scoring components. Preserve the PFR definition as a promotion candidate until all components are reconciled.",
        "evidence": "docs/audits/pfr-pfr_player_scoring-2025.json: 20 regular-season mismatches among 428 comparable rows; examples include Keon Coleman 26 vs weekly 24 and Markquese Bell 2 vs weekly 0.",
    },
    "pfr_scoring_post|*|scoring": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR scoring is retained as a composite-definition candidate; the postseason receipt has one mismatch (Colston Loveland 2 vs weekly 0), so it is not a 100% direct mapping.",
        "evidence": "docs/audits/pfr-pfr_scoring_post-2025.json: scoring mismatch example is Colston Loveland, PFR 2 versus weekly 0.",
    },
    "pfr_player_scoring|*|total_tds_scored": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR total_tds_scored aggregates touchdown buckets beyond the weekly total_tds_scored field; regular-season examples show PFR touchdowns where weekly total_tds_scored is zero. Preserve as a candidate until the composite touchdown equation is explicitly defined.",
        "evidence": "docs/audits/pfr-pfr_player_scoring-2025.json: 11 total_tds_scored mismatches among 434 comparable rows, including Will McDonald and John Metchie.",
    },
    "pfr_scoring_post|*|total_tds_scored": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR total_tds_scored is a composite touchdown bucket and is not promoted as the weekly total_tds_scored field without proving the postseason component definition.",
        "evidence": "PFR scoring postseason schema includes rush, receiving, return, fumble-recovery, defensive, other, and total touchdown buckets.",
    },
    "pfr_player_scoring|*|xpa": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR scoring-table xpa inherits the blocked-PAT denominator difference: affected 2025 kickers are one attempt below weekly pat_att. Preserve the PFR denominator as a promotion candidate.",
        "evidence": "docs/audits/pfr-pfr_player_scoring-2025.json: 7 regular-season xpa mismatches; made PAT totals agree while attempts differ.",
    },
    "pfr_scoring_post|*|xpa": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR scoring-table xpa remains a candidate until the blocked-PAT denominator definition is explicitly aligned with weekly pat_att.",
        "evidence": "The same PFR xpa field is present on the postseason scoring surface; no independent promotion is assumed from the regular-season denominator evidence.",
    },
    "pfr_player_defense|*|fumbles": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR defense fumbles is a defense-table fumble count that does not reconcile to the weekly general fumbles counter on 30 regular-season rows; preserve the defense-side definition as a candidate rather than conflating offensive and defensive fumble lanes.",
        "evidence": "docs/audits/pfr-pfr_player_defense-2025.json: examples include Da'Shawn Hand and Ke'Shawn Williams with PFR fumbles but weekly general fumbles equal to zero.",
    },
    "pfr_defense_post|*|fumbles": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR postseason defense fumbles is retained as a defense-side candidate because it does not reconcile to the weekly general fumbles counter on the observed postseason rows.",
        "evidence": "docs/audits/pfr-pfr_defense_post-2025.json: Ray Davis, Devin Duvernay, and Xavier Smith show PFR fumbles with weekly general fumbles equal to zero.",
    },
    "pfr_player_defense|*|fumbles_rec_yds": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR fumbles_rec_yds is not the weekly fumble_recovery_yards measurement: PFR publishes negative fumble-yard/loss values (for example Zach Frazier -16 and Josh Allen -17). Preserve it as a separately defined candidate.",
        "evidence": "docs/audits/pfr-pfr_player_defense-2025.json: 32 mismatches against weekly fum_rec_yds, including negative PFR values where weekly recovery yards are zero or less negative.",
    },
    "pfr_defense_post|*|fumbles_rec_yds": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR postseason fumbles_rec_yds remains a separately defined fumble-yard candidate; it is not assumed to equal weekly fum_rec_yds without a source-definition match.",
        "evidence": "PFR defense/post schema publishes fumbles_rec_yds as a distinct fumble bucket; no 100% lower-layer equality has been established.",
    },
    "pfr_player_offense_box|*|fumbles": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "The PFR game-offense fumbles counter does not fully roll into the PFR season fumbles field on eight 2025 regular-season player/team rows. Preserve the game-grain fumble definition as a candidate until the source aggregation discrepancy is resolved.",
        "evidence": "docs/audits/pfr-pfr_player_offense_box-offense-regular-2025.json: eight fumble rollup mismatches, including Olszewski, Isaiah Williams, and Britain Covey.",
    },
    "pfr_player_defense_box|*|fumbles_rec_yds": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "Game-defense fumbles_rec_yds rolls to the PFR season fumble-yard/loss field, not weekly fumble_recovery_yards; retain the publisher-specific game/season quantity as a candidate.",
        "evidence": "docs/audits/pfr-pfr_player_defense_box-defense-regular-2025.json: 19 mismatches, including negative season values such as Josh Allen -17 versus game rollup -8.",
    },
    "pfr_player_fantasy|*|fantasy_points": {
        "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "PFR fantasy_points is not a 100% equality witness for the current fpts_4pt_0ppr output: the independent standard-scoring recomputation matches 470/549 numeric 2025 rows, with remaining publisher scoring/rounding/return-definition gaps. Preserve the PFR surface as a candidate until the exact definition is adjudicated.",
        "evidence": "docs/audits/pfr-pfr_player_fantasy-2025.json: 549 comparable fantasy rows, 470 exact matches, 79 mismatches.",
    },
    "pfr_snap_counts|*|off_pct": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "STRUCTURED SNAP WITNESS: PFR off_pct requires the team offensive-snap denominator; player snap counts alone do not supply that denominator. The numerator offense snaps reconciles exactly.",
        "evidence": "docs/audits/pfr-pfr_snap_counts-2025.json: offense counts reconcile; percentage denominator is not present in the player surface.",
    },
    "pfr_snap_counts|*|def_pct": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "STRUCTURED SNAP WITNESS: PFR def_pct requires the team defensive-snap denominator; the player numerator reconciles, but the denominator lives in the team/game layer.",
        "evidence": "docs/audits/pfr-pfr_snap_counts-2025.json: defense counts reconcile; percentage denominator is not present in the player surface.",
    },
    "pfr_snap_counts|*|st_pct": {
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "STRUCTURED SNAP WITNESS: PFR st_pct requires the team special-teams-snap denominator; the player numerator reconciles, but the denominator lives in the team/game layer.",
        "evidence": "docs/audits/pfr-pfr_snap_counts-2025.json: special-teams counts reconcile; percentage denominator is not present in the player surface.",
    },
}

# These keys also have older defensive entries later in the historical table
# above.  Apply the final adjudication after dictionary construction so the
# resolved disposition cannot be shadowed by an earlier source-defense rule.
FINAL_ADJUDICATION = {
    "pfr_player_scoring|*|total_tds_scored": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR total_tds_scored equals the sum of its published TD buckets, including other_td. The canonical total-TD lane lacks that publisher component; do not promote a duplicate total."),
    "pfr_scoring_post|*|total_tds_scored": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR postseason total_tds_scored is the sum of its published touchdown buckets; retain it while canonical other-TD coverage is aligned."),
    "pfr_player_scoring|*|scoring": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR scoring equals 6*total_tds_scored + 2*two_pt_md + 2*def_two_pt + 3*fgm + xpm + 2*safety_md. Canonical total points omits publisher buckets; do not promote a duplicate."),
    "pfr_scoring_post|*|scoring": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR postseason scoring follows the same published component equation; retain it while canonical two-point/other-TD coverage is aligned."),
    "pfr_player_scoring|*|xpa": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR xpa equals pat_att minus pat_blocked, not raw pat_att. The denominator mapping is resolved; do not promote duplicate xpa."),
    "pfr_scoring_post|*|xpa": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR postseason xpa uses pat_att minus pat_blocked; do not promote duplicate xpa."),
    "pfr_player_kicking|*|xpa": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR xpa equals pat_att minus pat_blocked, not raw pat_att. The denominator mapping is resolved; do not promote duplicate xpa."),
    "pfr_kicking_post|*|xpa": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR postseason xpa uses pat_att minus pat_blocked; do not promote duplicate xpa."),
    "pfr_player_fantasy|*|fantasy_points": ("EXCLUDED_WITH_REASON", "DERIVED WITNESS: PFR FantPt follows its published standard formula. Remaining differences are lower-layer operand coverage/identity gaps, not evidence for a duplicate fantasy column."),
    "pfr_player_scoring|*|two_pt_md": ("NEW_SUPERTABLE_COLUMN_CANDIDATE", "REAL SCHEMA GAP: PFR publishes a total two-point-made scalar, while the canonical layer splits conversion types and does not capture every PFR two-point row. Preserve as a typed total candidate."),
    "pfr_scoring_post|*|two_pt_md": ("NEW_SUPERTABLE_COLUMN_CANDIDATE", "REAL SCHEMA GAP: PFR publishes a postseason total two-point-made scalar, while the canonical layer splits conversion types and does not capture every PFR row. Preserve as a typed total candidate."),
    "pfr_player_scoring|*|def_two_pt": ("NEW_SUPERTABLE_COLUMN_CANDIDATE", "REAL DEFENSIVE SCHEMA GAP: PFR def_two_pt is separate from offensive two_pt_md and must reconcile to a defensive two-point conversion event witness from PBP. It is not included in the offensive component sum."),
    "pfr_scoring_post|*|def_two_pt": ("NEW_SUPERTABLE_COLUMN_CANDIDATE", "REAL DEFENSIVE SCHEMA GAP: PFR postseason def_two_pt is separate from offensive two_pt_md and must reconcile to a defensive two-point conversion event witness from PBP. It is not included in the offensive component sum."),
    "pfr_player_defense|*|fumbles": ("EXCLUDED_WITH_REASON", "MAPPING ADJUDICATION: canonical fumbles exists, but PFR defense fumbles does not equal the weekly general fumbles counter. Resolve whether this is a defensive fumble lane or a source mapping problem before adding a field."),
    "pfr_defense_post|*|fumbles": ("EXCLUDED_WITH_REASON", "MAPPING ADJUDICATION: canonical fumbles exists, but PFR postseason defense fumbles does not yet equal the weekly general fumbles counter. Resolve the source definition before adding a field."),
    "pfr_player_offense_box|*|fumbles": ("EXCLUDED_WITH_REASON", "MAPPING ADJUDICATION: canonical fumbles exists, but the PFR game-offense counter does not fully roll to the PFR season counter. Resolve the source aggregation or join before adding a field."),
    "pfr_player_defense|*|fumbles_rec_yds": ("EXCLUDED_WITH_REASON", "MAPPING ADJUDICATION: canonical fumble_recovery_yards exists, but PFR fumbles_rec_yds uses a different signed/source definition. Resolve the source mapping before adding a field."),
    "pfr_defense_post|*|fumbles_rec_yds": ("EXCLUDED_WITH_REASON", "MAPPING ADJUDICATION: canonical fumble_recovery_yards exists, but PFR postseason fumbles_rec_yds uses a different signed/source definition. Resolve the source mapping before adding a field."),
    "pfr_player_defense_box|*|fumbles_rec_yds": ("EXCLUDED_WITH_REASON", "MAPPING ADJUDICATION: canonical fumble_recovery_yards exists, but PFR game-defense fumbles_rec_yds rolls to a publisher-specific signed quantity. Resolve the source mapping before adding a field."),
}


def main() -> None:
    for key, (disposition, reason) in FINAL_ADJUDICATION.items():
        OVERRIDES[key] = {
            "disposition": disposition,
            "reason": reason,
            "evidence": "Final PFR adjudication based on the 2025 receipt and canonical supertable schema.",
        }
    document = json.loads(DISPOSITIONS_PATH.read_text(encoding="utf-8"))
    entries = {entry["key"]: entry for entry in document["decisions"]}
    for key, override in OVERRIDES.items():
        entries[key] = {
            "key": key,
            **override,
            "generated_by": GENERATOR,
        }
    document["decisions"] = sorted(entries.values(), key=lambda item: item["key"])
    DISPOSITIONS_PATH.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    print({"written": len(OVERRIDES), "keys": list(OVERRIDES), "ledger_total": len(document["decisions"])})


if __name__ == "__main__":
    main()
