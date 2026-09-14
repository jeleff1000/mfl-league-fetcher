"""The MAPPING-OBLIGATION gate (O.7): registering a source is not enough.

Joe's 2026-07-26 challenge: "when we register new sources we're keeping that
discipline?" Measured answer at the time: 25 registered sources had ZERO
witness-map rows and nothing failed — an authority could sit registered and
unmapped forever (fail-closed for voting, but silently unutilized).

This gate forces every registered source into exactly one bucket AT REGISTRATION:
  MAPPED            has >=1 WITNESS_MAP row (its columns alias-tracked + validated)
  LANE_WITNESSED    witnesses through a named non-MapSpec lane (presence, scoring
                    events, K-plane joins, identity crosswalks, ...)
  NON_MAPPING_ROLE  its role class never carries stat values (context, identity,
                    anchor, negative-witness, subject)
  MAPPING_PENDING   value-witness with mappings still owed -- queue note mandatory

A newly registered source (nflcom, statscrew, ...) matching none of these FAILS
the suite until its mapping obligation is declared.

Run:  python -m pytest scripts/sota_recon/test_mapping_obligation.py -q
"""

from __future__ import annotations

from . import witness_map as WM
from .sources import registry

# role classes that never carry stat values through column mappings
NON_MAPPING_ROLES = {"identity", "context", "anchor", "negative", "subject"}

# sources that witness through a named lane instead of MapSpecs
LANE_WITNESSED = {
    "pfr_all_pro_members": "pfr_awards typed-membership composite lane (one PFR lineage root)",
    "pfr_pro_bowl_members": "pfr_awards typed-membership composite lane (one PFR lineage root)",
    "pfr_box_scoring": "recon_scoring_events (unique-scoring-event lane, R9)",
    "pfr_box_pbp": "pbp regex lanes (build_misc_pbp_pre1998 family) + drive recon",
    "pfr_box_team_stats": "R4 vertical team-witness lane (kc plane, O.7 queue)",
    "pfr_box_vis_drives": "drive-balance recon (recon_doubleheader_alignment)",
    "pfr_box_home_drives": "drive-balance recon (recon_doubleheader_alignment)",
    "pfr_box_home_starters": "presence universe (entity_universes appearance witness)",
    "pfr_box_vis_starters": "presence universe (entity_universes appearance witness)",
    "pfr_games_played": "presence universe (entity_universes appearance witness)",
    "pfr_games_played_post": "presence universe (entity_universes appearance witness)",
    "pfr_team_games": "game catalog anchor (nfl_team_games_all joins + schedule recon)",
    "schedule_master": "master-schedule anchor (recon_schedule)",
    "scoring_summary": "internal scoring parity (golden_points decomposition)",
    "newspaper_team_claims": "newspaper sidecar lanes (non-voting until holds clear)",
    "newspaper_team_stats": "newspaper sidecar lanes (non-voting until holds clear)",
    "newspaper_player_cells": "newspaper sidecar witness bundle (non-voting until holds clear)",
    "newspaper_scoring_events": "newspaper sidecar scoring lane (non-voting until holds clear)",
    "newspaper_pbp_events": "newspaper sidecar pbp lane (non-voting until holds clear)",
    "newspaper_lineups": "presence universe candidate (newspaper program)",
    "pbp_merged_1978_2025": "pbp regex/structured lanes + rollup parent (rollup carries the MapSpecs)",
    "pbp_team_defense": "R4/R5 team-defense lanes (team_fid-scoped join = O.4 queue)",
    "legacy_motherduck_supertable": "legacy DENSE diff lens (kc lane; 44,714-key gap ledger)",
    "pfr_box_expected_points": "EPA backfill lanes (build_epa_backfill_1978_v26 family)",
    "pfr_adj_passing": "plausibility lane (era-adjusted passing INDEX columns -- "
                       "index-vs-raw monotonicity; no canonical v26 column mirrors an "
                       "index, so there is no alias to map)",
    # OQ-LR-5 per-stream ancient registrations (2026-07-26): these streams witness
    # through the ancient apply/parity lane -- their rows are the receipted upsert
    # source for v26's ancient cells (parent-copy parity), scoped by each source's
    # definition filter. Only ancient_pfa_gamelog carries MapSpecs (MAPPED bucket).
    "ancient_pfr_recovery": "ancient apply parity lane (pfr-bloodline recovery rows; OQ-LR-5)",
    "ancient_pbp1978_recovery": "ancient apply parity lane (pbp-1978 recovery rows; OQ-LR-5)",
    "ancient_newspaper_ocr": "newspaper program (non-voting until holds clear; OQ-LR-5)",
    # O.8 referee-bench onboarding (2026-07-26): the ff_assets captures are PRESENCE
    # witnesses (roster / per-game participation). They carry no stat values, so they
    # witness through the appearance/presence lane rather than MapSpecs.
    "nflcom_team_season_roster":
        "presence universe (entity_universes appearance witness) + slug->pfr_id "
        "crosswalk seed for the nflcom stat families",
    "statscrew_team_season_roster":
        "presence universe (entity_universes appearance witness); candidate root, "
        "no votes until a shared-ancestor receipt exists",
    "pfa_player_game_participation":
        "presence universe (entity_universes appearance witness) at GAME grain, "
        "1920-2025 -- the widest participation witness we hold",
}

# value-witnesses with mappings still owed (each entry = a queue item, not a pass)
# ---- 2026-07-30: FOUR nflcom families LEFT this bucket, obligation discharged ---------
# nflcom_player_career, _season, _situational and _splits now carry real MapSpecs. The
# blocker was never the crosswalk -- that was receipted 2026-07-29 -- it was that MapSpec
# was keyed (source_key, source_col) with no table selector, and on NFL.com that key is not
# unique to a statistic: `nflcom_player_season.lng` is fg_long under field-goals,
# passing_long under passing and punt_long under punts, SEVEN canonicals behind one column
# name. MapSpec now carries source_table/table_col/row_filter, and the generator fans these
# sources out per table, so 680 table-scoped specs exist where 0 could be written before.
#
# nflcom_player_logs and nflcom_player_logs_targeted STAY, and their blocker is different in
# kind: their position-group block id did not survive capture, so one physical column is a
# union of several statistics. That is a capture defect, not a modelling one.
MAPPING_PENDING = {
    # O.8 (2026-07-26): the original 13 pendings CLEARED -- 12 gained MapSpecs
    # (POST-stratum punting/returns/scoring/rec-rush + the advanced 2018+ season and
    # POST tables) and pfr_adj_passing moved to LANE_WITNESSED because it carries
    # era-adjusted INDEX columns that no canonical v26 column mirrors: there is
    # nothing to alias, so a MapSpec would have been a fiction.
    # ---- O.8 (2026-07-26): nflcom stat families. ONE shared blocker, named exactly.
    # Their key space is `nflcom_slug` (a name slug like 'frank-abruzzino'). Every
    # MapSpec validator resolves witnesses to pfr_id, so no nflcom mapping can be
    # written OR licensed until a receipted slug->pfr_id crosswalk exists. The seed
    # is nflcom_team_season_roster (slug + player + team + season) joined to
    # player_bio/pfr rosters; it must land as a crosswalk_receipts.v1.json entry with
    # measured unmatched rates -- a bare name join is the twins hazard (§19.2).
    # ---- LICENSED 2026-07-29, MAPPING STILL OWED. These six read "BLOCKED on crosswalk
    # nflcom_slug->pfr_id" until Joe approved the promotion and the receipt was written
    # (signoff_ledger.v1.json). That blocker is GONE: crosswalk_receipts.v1.json carries
    # nflcom_slug_pfrid at status PASS licensing all six, 72.0% of slugs and 88.1% of roster
    # rows resolved. They stay in this bucket because the bucket means "mappings still owed"
    # and the MapSpecs genuinely are -- LICENSING AND MAPPING ARE DIFFERENT GATES, and this
    # is the first time in the program the two have visibly come apart on the same source.
    # refusal_preconditions.py re-declared all six on NO_MAPSPEC_FOR_SOURCE the same day.
    # ---- CLEARED 2026-08-01, the way the law demands (by MAPPING): nflcom_player_logs
    # (210 specs), nflcom_player_logs_targeted (140), nflcom_team_stats (130) and
    # statscrew_team_season_stats (67+) all carry live MapSpecs -- the generator waves
    # plus the 2026-08-01 hand waves delivered the mappings these entries were owed.
    # ---- 2026-07-27: the StatsCrew STAT families. Two blockers, named separately,
    # because they are NOT the same blocker and clearing one does not clear the other.
    "statscrew_team_season_results":
        "BLOCKED on team-code canonicalization: statscrew codes (CAN/AKR/CLE) into "
        "team_fid franchise space, the SAME era-alias problem (OTI/HOU, CRD/STL/PHO, "
        "CLT/BAL) the O.7 DEF-row join solved by hand. Needs NO player crosswalk. "
        "Game-calendar + running-record witness 1920-2023, 1,521 team-seasons",
}


def _mapped_sources() -> set[str]:
    return {m.source_key for m in WM.WITNESS_MAP}


def test_every_registered_source_declares_its_mapping_obligation():
    mapped = _mapped_sources()
    undeclared = []
    for key, src in registry(include_subject=True).items():
        if key in mapped or key in LANE_WITNESSED or key in MAPPING_PENDING:
            continue
        if src.role in NON_MAPPING_ROLES:
            continue
        undeclared.append(f"{key} (role={src.role})")
    assert not undeclared, (
        "registered sources with NO declared mapping obligation -- add MapSpecs, "
        f"a LANE_WITNESSED entry, or a MAPPING_PENDING queue note: {undeclared}")


def test_buckets_reference_real_sources():
    reg = registry(include_subject=True)
    for key in (*LANE_WITNESSED, *MAPPING_PENDING):
        assert key in reg, f"{key}: bucket entry for unregistered source"


def test_pending_entries_carry_queue_notes():
    for key, note in MAPPING_PENDING.items():
        assert len(note) > 15, f"{key}: pending needs a real queue note"


def test_lane_witnessed_entries_name_their_lane():
    for key, lane in LANE_WITNESSED.items():
        assert len(lane) > 10, f"{key}: name the witnessing lane"


def test_mapped_sources_do_not_hide_in_pending():
    """A source that gains MapSpecs must leave MAPPING_PENDING -- the queue can
    only shrink by actually mapping, never by lingering."""
    mapped = _mapped_sources()
    stale = sorted(mapped & set(MAPPING_PENDING))
    assert not stale, f"sources now mapped but still listed pending: {stale}"


def test_composite_contract_sources_have_declared_mapping_routes():
    from .composite_witness_lane import load_specs

    registered = registry(include_subject=True)
    mapped = _mapped_sources()
    for spec in load_specs():
        for source in spec.sources:
            assert source in registered, f"{spec.spec_id}: unregistered source {source}"
            assert source in mapped or source in LANE_WITNESSED or source in MAPPING_PENDING, (
                f"{spec.spec_id}: {source} has no declared mapping or composite lane route"
            )
