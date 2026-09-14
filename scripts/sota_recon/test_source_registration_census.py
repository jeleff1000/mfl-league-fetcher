"""Contract tests for the disk-first source-registration census (O.7).

The law: the census denominator is the DISK. A harvest directory landing on disk
without a registry row or an explicit disposition FAILS here -- silently dropping
a source is structurally impossible.

Run:  python -m pytest scripts/sota_recon/test_source_registration_census.py -q
"""

from __future__ import annotations

from .source_registration_census import DISPOSITION_VOCAB, DISPOSITIONS, build


def _doc():
    return build()


def test_zero_counter_no_undispositioned_source_dirs():
    doc = _doc()
    offenders = [r["dir"] for r in doc["rows"]
                 if r["disposition"] == "UNREGISTERED_NO_DISPOSITION"]
    assert not offenders, (
        f"source dirs on disk with NO registry row and NO disposition: {offenders} "
        "-- register them in sources.py or add a QUEUED/EXCLUDED disposition "
        "with a receipt-note")


def test_every_row_carries_a_legal_disposition():
    for r in _doc()["rows"]:
        assert r["disposition"] in DISPOSITION_VOCAB, r


def test_registered_rows_name_their_sources():
    for r in _doc()["rows"]:
        if r["disposition"] == "REGISTERED":
            assert r["registered_sources"], r["dir"]


def test_manual_dispositions_carry_notes():
    for key, (disp, note) in DISPOSITIONS.items():
        assert disp in ("QUEUED", "EXCLUDED"), key
        assert len(note) > 20, f"{key}: disposition needs a real receipt-note"


def test_witness_gate_census_stays_joined():
    """Every dataset witness_gate's source_census.v1.json declares must resolve
    into a dispositioned census dir or carry its own dataset disposition -- the
    two censuses can never silently re-split (§18)."""
    doc = _doc()
    outside = [d["dataset_id"] for d in doc.get("contract_datasets", [])
               if d["disposition"] == "DATASET_OUTSIDE_CENSUS"]
    assert not outside, outside
    assert doc["counters"]["contract_datasets_outside_census"] == 0


def test_no_unknown_lake_toplevel_dirs():
    """A NEW top-level directory in the lake (a future harvest landing outside
    the scan roots) fails here until it is added to the scan roots or the
    known-internal set -- with a look at what it is."""
    doc = _doc()
    assert doc["unknown_lake_toplevel"] == [], doc["unknown_lake_toplevel"]


def test_containment_beats_manual_disposition():
    """A dir that gains a registry row shows REGISTERED even if a stale manual
    disposition lingers -- the registry is the stronger fact."""
    for r in _doc()["rows"]:
        if r["key"] in DISPOSITIONS:
            assert r["disposition"] in ("REGISTERED",) + (DISPOSITIONS[r["key"]][0],), r


# ---- the layer beneath the publisher (2026-07-28) -------------------------------------

def test_the_denominator_reaches_dataset_grain():
    """`unregistered_without_disposition = 0` was a statement about PUBLISHERS.
    `ff_assets/statscrew` read REGISTERED because ONE dataset under it had a registry
    row, while `ff_assets/statscrew/team_season_stats` sat on disk holding 137,864 rows
    registered nowhere -- through every gate run for a day. Registration happens per
    (publisher, DATASET), so the walk has to reach the children."""
    doc = _doc()
    counters = doc["counters"]
    assert counters["child_dirs"] > counters["registered_dirs"], (
        "the child walk found no more units than the publisher walk -- it is not "
        "reaching a layer the parent counter missed")
    assert counters["child_unregistered_without_disposition"] == 0, [
        child["key"] for row in doc["rows"] for child in (row.get("children") or [])
        if child["disposition"] == "UNREGISTERED_NO_DISPOSITION"]
    # conservation: every child lands in exactly one bucket
    assert (counters["child_dirs_registered"] + counters["child_dirs_queued"]
            + counters["child_unregistered_without_disposition"]
            + sum(1 for row in doc["rows"] for child in (row.get("children") or [])
                  if child["disposition"] == "EXCLUDED")) == counters["child_dirs"]


def test_a_child_inside_a_registered_source_is_not_reported_as_a_gap():
    """A source may be registered AT a directory and own its whole subtree --
    `newspaper_raw_archives` is. Missing that relation reported its three children as
    unregistered material: a denominator inflated by the check meant to catch a deflated
    one, which is the same defect in mirror image."""
    from .source_registration_census import _covers

    assert _covers("a/b", "a/b")          # registered at it
    assert _covers("a/b/run1", "a/b")     # registered under it
    assert _covers("a/b", "a/b/child")    # registered ABOVE it -- the one that was missed
    assert not _covers("a/bb", "a/b")     # prefix collision is not containment
    assert not _covers("a/c", "a/b")

    doc = _doc()
    newspaper = [row for row in doc["rows"] if row["key"].endswith("newspaper_archives")]
    for row in newspaper:
        for child in (row.get("children") or []):
            assert child["disposition"] == "REGISTERED", child


def test_every_child_disposition_carries_a_receipt_note():
    from .source_registration_census import CHILD_DISPOSITIONS

    for key, (disposition, note) in CHILD_DISPOSITIONS.items():
        assert disposition in ("QUEUED", "EXCLUDED"), key
        assert len(note) > 60, f"{key}: a child disposition needs a real receipt-note"
