"""The two gates that read the layer BENEATH a clean counter."""
from __future__ import annotations

import json

import pytest

from . import capture_width_gate as CW
from . import identity_coverage_gate as IC


def _receipt(module):
    if not module.RECEIPT.exists():
        pytest.skip(f"{module.RECEIPT.name} not built")
    return json.loads(module.RECEIPT.read_text(encoding="utf-8"))


def test_identity_gate_declares_the_part_of_the_registry_it_could_not_read():
    """'0 missing ids' across sources we never opened is the failure this gate exists to
    prevent, so the sources it could NOT resolve an id column for are counted, not
    dropped."""
    doc = _receipt(IC)
    c = doc["counters"]
    assert "sources_without_a_resolvable_id_column" in c
    assert "sources_unreadable" in c
    assert c["sources_checked"] + c["sources_without_a_resolvable_id_column"] \
        + c["sources_unreadable"] > 0
    assert len(doc["sources_without_a_resolvable_id_column"]) == \
        c["sources_without_a_resolvable_id_column"]


def test_identity_spine_is_a_union_never_bio_alone():
    """bio holds ONE Todd Collins where PFR holds two, so a bio-only membership test
    manufactures false confidence about who exists. The spine must stay a union."""
    doc = _receipt(IC)
    assert set(doc["spine"]["sources"]) == {"player_bio", "pfr_player_index"}
    assert doc["spine"]["distinct_ids"] > 0


def test_every_absent_id_is_enumerated_not_just_counted():
    """A queue that reports a number without names is not a queue."""
    doc = _receipt(IC)
    named = {i for ids in doc["missing_ids"].values() for i in ids}
    assert doc["counters"]["distinct_ids_absent_from_spine"] == len(named)
    assert set(doc["unaccepted"]) <= named


def test_an_absence_may_only_be_accepted_with_a_reason():
    assert all(isinstance(v, str) and v.strip() for v in IC.ACCEPTED_ABSENCES.values())


def test_width_gate_compares_per_layout_not_per_source():
    """The first version compared a page's widest table (19 headers) against the source's
    whole stored column union (53 across 15 layouts) and reported 0 suspects -- a FALSE
    CLEAN from the gate built to catch false cleans, since 19 < 53 can never trip. The
    stored side must be a single layout's width."""
    doc = _receipt(CW)
    for source, info in doc["per_source"].items():
        if "error" in info:
            continue
        assert "widest_censused_layout" in info, source
        assert info["dropped_field_suspects"] == max(
            0, info["widest_published_header"] - info["widest_censused_layout"]), source
        # the union is recorded for context but must NOT be the comparison basis
        if "stored_columns_union_across_layouts" in info:
            assert info["widest_censused_layout"] <= info["stored_columns_union_across_layouts"]


def test_width_gate_sample_is_declared():
    """A convenience sample produces a number nobody can reproduce."""
    doc = _receipt(CW)
    for source, info in doc["per_source"].items():
        if "error" in info:
            continue
        assert info["seed"] == CW.SEED, source
        assert 0 < info["pages_sampled"] <= info["pages_available"], source


def test_width_gate_counts_blank_headers():
    """The column-shift defect existed because a blank header was dropped while its cells
    were kept. A width check that skipped blank <th> would be blind to it."""
    page = b"<table><tr><th>A</th><th></th><th>C</th></tr></table>"
    assert CW.widest_header(page) == 3


def test_width_gate_declares_sources_it_cannot_see():
    doc = _receipt(CW)
    assert doc["counters"]["sources_without_a_page_root"] == \
        len(doc["sources_without_a_page_root"])
    assert doc["counters"]["sources_without_a_page_root"] > 0, (
        "if this ever reads 0, confirm it is real coverage and not a lost denominator")


def test_page_roots_carry_their_evidence():
    assert all(isinstance(v, tuple) and len(v) == 2 and v[1].strip()
               for v in CW.PAGE_ROOTS.values())
