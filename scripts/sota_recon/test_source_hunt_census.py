"""Law A (DISCOVERY) gate tests -- O.8.

The failure this guards: a source-hunt work order is raised, worked, and abandoned
with no counter ever noticing. Before this lane the hunt program was a fourth
unjoined key-space and witness_gate's `discoveries` feed was empty.
"""

from __future__ import annotations

from . import source_hunt_census as SHC


def test_every_hunt_candidate_carries_a_terminal_disposition():
    doc = SHC.build()
    bad = doc["undispositioned"]
    assert not bad, (
        "hunt candidates with no terminal disposition -- give each CAPTURED / QUEUED / "
        f"ABANDONED(reason) in source_hunt_census.py: {bad}")
    assert doc["counters"]["hunt_candidates_without_terminal_disposition"] == 0


def test_every_hunt_program_dir_on_disk_is_dispositioned():
    """A NEW curated/source_* work-order directory must fail until dispositioned --
    this is the whole point of walking the disk rather than the ledger list."""
    doc = SHC.build()
    undeclared = [p["program"] for p in doc["hunt_programs"]
                  if p["disposition"] == "UNDISPOSITIONED"]
    assert not undeclared, f"undispositioned hunt program dirs: {undeclared}"


def test_abandoned_candidates_state_a_reason():
    doc = SHC.build()
    for group in ("hunt_programs", "source_lanes", "preferred_sources"):
        for row in doc[group]:
            if row["disposition"] == "ABANDONED":
                reason = row.get("reason") or ""
                assert len(reason) > 20, f"{group}:{row}: ABANDONED needs a real reason"


def test_dispositions_use_the_declared_vocabulary():
    doc = SHC.build()
    for group in ("source_lanes", "preferred_sources"):
        for row in doc[group]:
            assert row["disposition"] in SHC.DISPOSITION_VOCAB, row


def test_image_pdf_review_stays_open_and_owned():
    """The one legitimately-open preferred source is the newspaper image-review lane.
    If it ever flips to ABANDONED, the newspaper root's arbitration path is gone --
    that is an escalation, never a quiet edit."""
    doc = SHC.build()
    row = next(r for r in doc["preferred_sources"]
               if r["preferred_source"] == "image/PDF review")
    assert row["disposition"] == "QUEUED"
    assert "newspaper program" in row["reason"]
