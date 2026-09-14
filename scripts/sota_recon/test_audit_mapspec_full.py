"""Receipts for the full-stratum collision audit's cell-level classifier."""
from __future__ import annotations

from scripts.sota_recon.audit_mapspec_full import classify_full, enrich_cells


def _row(**kw) -> dict:
    row = dict(n=1000, agree_pct=0.99, verdict="VALIDATED",
               n_we_hold_no_row=0, n_we_hold_null=0, informative_n=900,
               source_exceeds_us=0, we_exceed_source=0,
               source_zero_supertable_nonzero=0, source_nonzero_supertable_zero=0)
    row.update(kw)
    enrich_cells(row)
    return row


def test_high_agreement_with_residue_is_not_named_a_conflict_headline():
    # the pilot defect: passing_yards at 98.4% with 33 zero-direction cells in
    # 8,666 must not be headlined by its smallest lane
    row = _row(n=8666, agree_pct=0.9838, n_we_hold_no_row=124, n_we_hold_null=67,
               source_exceeds_us=73, we_exceed_source=67,
               source_nonzero_supertable_zero=33)
    assert row["value_conflict_cells"] == 140
    assert row["backfill_cells"] == 191
    # backfill outweighs conflict -> the dominant lane names the row
    assert classify_full(row) == "SUPERTABLE_NULL_OR_MISSING"


def test_clean_row_agrees():
    assert classify_full(_row()) == "AGREES"


def test_zero_base_agreement_is_not_positive_evidence():
    assert classify_full(_row(informative_n=0)) == "AGREES_ZERO_BASE"


def test_dominant_stored_zero_names_the_supertable_zero_class():
    row = _row(we_exceed_source=100, source_exceeds_us=80,
               source_nonzero_supertable_zero=95)
    assert classify_full(row) == "SUPERTABLE_ZERO_SOURCE_POSITIVE"


def test_two_sided_conflict_without_zero_dominance():
    row = _row(source_exceeds_us=60, we_exceed_source=50)
    assert classify_full(row) == "TWO_SIDED_VALUE_CONFLICT"


def test_undirected_paths_never_claim_a_direction():
    # week-grain/static rows carry only n + agree_pct; direction must not be invented
    row = dict(n=500, agree_pct=0.9, verdict="VALIDATED")
    enrich_cells(row)
    assert row["conflict_directional"] is False
    assert row["value_conflict_cells"] == 50
    assert classify_full(row) == "VALUE_CONFLICT_UNDIRECTED"


def test_broken_and_no_overlap_precede_everything():
    row = _row(verdict="BROKEN: Binder Error")
    assert classify_full(row) == "MAPPING_DEFECT_SQL"
    row = dict(n=0, verdict="NO-OVERLAP")
    enrich_cells(row)
    assert classify_full(row) == "NO_OVERLAP"


def test_zero_direction_subsets_are_never_added_to_the_disjoint_total():
    # source_nonzero_supertable_zero is a SUBSET of source_exceeds_us; the total
    # must remain the disjoint directional sum, or cells double-count
    row = _row(source_exceeds_us=40, we_exceed_source=10,
               source_nonzero_supertable_zero=35)
    assert row["value_conflict_cells"] == 50


def test_merge_disposition_ranks_lanes_by_cells():
    from scripts.sota_recon.merge_mapspec_full import disposition
    backfill_heavy = _row(n_we_hold_no_row=300, source_exceeds_us=20)
    backfill_heavy["conflict_class"] = classify_full(backfill_heavy)
    assert disposition(backfill_heavy) == "BACKFILL_CANDIDATE"
    conflict_heavy = _row(source_exceeds_us=200, we_exceed_source=150,
                          n_we_hold_null=30)
    conflict_heavy["conflict_class"] = classify_full(conflict_heavy)
    assert disposition(conflict_heavy) == "ADJUDICATE"
    clean = _row()
    clean["conflict_class"] = classify_full(clean)
    assert disposition(clean) == "NO_ACTION"
    adjudicated = _row(source_exceeds_us=5, fault="SUPERTABLE_GAP")
    adjudicated["conflict_class"] = classify_full(adjudicated)
    assert disposition(adjudicated) == "KNOWN_FINDING_SUPERTABLE_FAULT"
