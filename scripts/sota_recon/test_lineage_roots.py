"""Contract tests for lineage roots (O.5): mandatory-explicit lineage, regen-diff gate,
era-split resolution, and vocabulary enforcement.

Run:  python -m pytest scripts/sota_recon/test_lineage_roots.py -q
"""

from __future__ import annotations

import dataclasses

import pytest

from . import lineage_roots
from .sources import KNOWN_LINEAGES, Source, registry


# ---------- §16.2: the lineage="pfr" default is dead ----------

def test_lineage_has_no_default():
    field = {f.name: f for f in dataclasses.fields(Source)}["lineage"]
    assert field.default is dataclasses.MISSING
    with pytest.raises(TypeError):
        Source(key="x", role="t", path="p", year_min=1, year_max=2, join="j", note="n")


def test_unknown_lineage_rejected_at_construction():
    with pytest.raises(ValueError):
        Source(key="x", role="t", path="p", year_min=1, year_max=2,
               join="j", note="n", lineage="not-a-lineage")


def test_every_registered_source_declares_a_known_lineage():
    for sid, src in registry(include_subject=True).items():
        assert src.lineage in KNOWN_LINEAGES, sid


# ---------- contract file ----------

def test_committed_contract_matches_generator():
    """§18 regen-diff law: the committed lineage_roots.v1.json must be exactly what the
    committed generator produces."""
    assert lineage_roots.load() == lineage_roots.generate()


def test_contract_validates_clean():
    assert lineage_roots.validate(lineage_roots.load()) == []


def test_every_source_has_exactly_one_entry():
    doc = lineage_roots.load()
    ids = [e["source_id"] for e in doc["sources"]]
    assert len(ids) == len(set(ids))
    assert set(ids) == set(registry(include_subject=True))


def test_open_questions_are_queued_or_resolved_with_a_receipt():
    """An OQ is either still QUEUED (no lane may resolve it implicitly) or RESOLVED
    with a written resolution receipt. The original form of this test allowed ONLY
    QUEUED, which made recording an adjudication impossible without deleting the
    question -- the failure mode being guarded against is a SILENT resolution, not a
    receipted one."""
    for q in lineage_roots.load()["open_questions"]:
        assert q["status"] in ("QUEUED", "RESOLVED"), q["id"]
        if q["status"] == "RESOLVED":
            assert len(q.get("resolution", "")) > 100, \
                f"{q['id']}: RESOLVED needs a written resolution receipt"
        else:
            assert q.get("blocker"), f"{q['id']}: QUEUED needs a blocker"


def test_shared_ancestors_never_affect_voting():
    for a in lineage_roots.load()["shared_ancestors"]:
        assert a["voting_effect"] == "none"
        assert a["status"] == "DECLARED_UNRECEIPTED"


# ---------- root resolution ----------

def test_pbp_era_split_resolves_by_year():
    for sid in ("pbp_merged_1978_2025", "pbp_player_week_rollup", "pbp_team_defense"):
        assert lineage_roots.root_of(sid, 1998) == "pfr", sid
        assert lineage_roots.root_of(sid, 1999) == "nflverse_pbp", sid


def test_non_split_sources_resolve_to_registry_root():
    assert lineage_roots.root_of("pfr_player_season_passing", 1950) == "pfr"
    assert lineage_roots.root_of("ngs_season_published", 2020) == "ngs"
    assert lineage_roots.root_of("ancient_pfa_gamelog", 1940) == "pfa_loc"
    assert lineage_roots.root_of("newspaper_player_cells", 1930) == "newspaper"
    assert lineage_roots.root_of("v26_release", 2020) == "internal"


def test_oq_lr5_ancient_streams_resolve_to_their_own_roots():
    """OQ-LR-5 (2026-07-26): row-level lineage beats source-level lineage. The ancient
    composite bundle used to resolve every row to pfa_loc; per-stream registration
    gives each stream its true root. A regression here re-corrupts ancient-era root
    diversity in both directions."""
    assert lineage_roots.root_of("ancient_pfa_gamelog", 1940) == "pfa_loc"
    assert lineage_roots.root_of("ancient_pfr_recovery", 1940) == "pfr"
    assert lineage_roots.root_of("ancient_newspaper_ocr", 1930) == "newspaper"
    # pbp-1978 stream rides the era-split contract: 1978-79 is PFR-pbp-rooted
    assert lineage_roots.root_of("ancient_pbp1978_recovery", 1978) == "pfr"


def test_oq_lr5_recorded_resolved_with_receipt():
    oq = {q["id"]: q for q in lineage_roots.load()["open_questions"]}
    assert oq["OQ-LR-5"]["status"] == "RESOLVED"
    assert len(oq["OQ-LR-5"].get("resolution", "")) > 200, "resolution needs its receipt"


def test_out_of_window_years_clamp():
    assert lineage_roots.root_of("pbp_merged_1978_2025", 1920) == "pfr"
    assert lineage_roots.root_of("pbp_merged_1978_2025", 2099) == "nflverse_pbp"
