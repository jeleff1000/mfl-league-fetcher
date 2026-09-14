"""Tests for identity resolution and franchise matching."""

from dataclasses import dataclass
from multi_league.external_ingest.identity_match import (
    ExternalManagerOccurrence,
    resolve_identity,
    build_alias_set,
    suggest_top_n,
    IdentityResolution,
    IdentityCluster,
    cluster_resolutions,
)


@dataclass
class FakeFranchise:
    franchise_id: str
    franchise_name: str
    owner_guid: str
    historical_managers: tuple[str, ...] = ()


@dataclass
class FakeRegistry:
    franchises: tuple[FakeFranchise, ...]
    external_alias_mappings: dict = None

    def __post_init__(self):
        if self.external_alias_mappings is None:
            self.external_alias_mappings = {}


def test_alias_set_includes_current_name_and_history():
    f = FakeFranchise("g_1", "Joe Eleff", "guid_x", ("Joe E", "joe_eleff"))
    aliases = build_alias_set(f, external_alias_mappings={})
    assert "joe_eleff" in aliases
    assert "joe_e" in aliases


def test_alias_set_compounds_via_prior_external_mappings():
    f = FakeFranchise("g_1", "Joe Eleff", "guid_x")
    external_maps = {
        "Joe E": {"franchise_id": "g_1"},
        "JosephE": {"franchise_id": "g_1"},
        "OtherGuy": {"franchise_id": "g_2"},  # different franchise — should NOT compound
    }
    aliases = build_alias_set(f, external_alias_mappings=external_maps)
    assert "joe_e" in aliases
    assert "josephe" in aliases
    assert "otherguy" not in aliases


def test_resolve_bound_via_guid_when_real_guid_present():
    occ = ExternalManagerOccurrence(
        manager="Ezra",
        rows_with_guid={"SAQH...": 14},  # at least one row has guid
        years={2013},
        tables={"matchup"},
    )
    registry = FakeRegistry((FakeFranchise("g_1", "Ezra K", "SAQH...", ("Ezra",)),))
    res = resolve_identity(occ, registry)
    assert res.state == "bound_via_guid"
    assert res.franchise_id == "g_1"


def test_resolve_auto_suggested_when_unique_alias_match():
    occ = ExternalManagerOccurrence(
        manager="Ezra",
        rows_with_guid={},  # no guid
        years={2013},
        tables={"draft"},
    )
    registry = FakeRegistry(
        (
            FakeFranchise("g_1", "Ezra K", "SAQH...", ("Ezra",)),
            FakeFranchise("g_2", "Marc", "FYW...", ()),
        )
    )
    res = resolve_identity(occ, registry)
    assert res.state == "auto_suggested"
    assert res.franchise_id == "g_1"


def test_resolve_unresolved_when_alias_collision():
    occ = ExternalManagerOccurrence(
        manager="Ezra",
        rows_with_guid={},
        years={2013},
        tables={"draft"},
    )
    registry = FakeRegistry(
        (
            FakeFranchise("g_1", "Ezra", "g1", ()),
            FakeFranchise("g_2", "Ezra69420", "g2", ("Ezra",)),  # both alias "Ezra"
        )
    )
    res = resolve_identity(occ, registry)
    assert res.state == "unresolved"
    assert res.franchise_id is None


def test_resolve_unresolved_when_no_match():
    occ = ExternalManagerOccurrence(
        manager="Donny T",
        rows_with_guid={},
        years={2013},
        tables={"draft"},
    )
    registry = FakeRegistry((FakeFranchise("g_1", "Ezra", "g1", ()),))
    res = resolve_identity(occ, registry)
    assert res.state == "unresolved"


def test_alias_compounding_resolves_after_prior_mapping():
    occ = ExternalManagerOccurrence(
        manager="Joe E",
        rows_with_guid={},
        years={2014},
        tables={"draft"},
    )
    registry = FakeRegistry(
        (FakeFranchise("g_1", "Joe Eleff", "g1", ()),),
        external_alias_mappings={"Joe E": {"franchise_id": "g_1"}},
    )
    res = resolve_identity(occ, registry)
    assert res.state == "auto_suggested"
    assert res.franchise_id == "g_1"


def test_suggest_top_n_orders_by_score():
    occ = ExternalManagerOccurrence("Brent M", {}, {2013}, {"draft"})
    franchises = [
        FakeFranchise("g_1", "Brent Marshall", "g1", ()),
        FakeFranchise("g_2", "Brian M", "g2", ()),
        FakeFranchise("g_3", "Donny T", "g3", ()),
    ]
    suggestions = suggest_top_n(occ, franchises, top_n=3, score_floor=0.2)
    # Brent Marshall and Brian M tie within score tolerance — set membership, not order
    top_ids = {s.franchise_id for s in suggestions[:2]}
    assert top_ids == {"g_1", "g_2"}
    assert abs(suggestions[0].score - suggestions[1].score) <= 0.05
    assert all(s.score >= 0.2 for s in suggestions)
    assert not any(s.franchise_id == "g_3" for s in suggestions)


def test_suggest_top_n_filters_by_score_floor():
    occ = ExternalManagerOccurrence("XYZ", {}, {2013}, {"draft"})
    franchises = [FakeFranchise("g_1", "Brent Marshall", "g1", ())]
    suggestions = suggest_top_n(occ, franchises, top_n=3, score_floor=0.5)
    assert suggestions == []


def test_cluster_groups_strings_auto_suggesting_to_same_franchise():
    resolutions = [
        IdentityResolution("Joe E", "auto_suggested", franchise_id="g_1"),
        IdentityResolution("Joe Eleff", "auto_suggested", franchise_id="g_1"),
        IdentityResolution("Marc", "auto_suggested", franchise_id="g_2"),
        IdentityResolution("Donny T", "unresolved"),
    ]
    clusters = cluster_resolutions(resolutions)
    grouped_by_fid = {c.franchise_id: c for c in clusters if isinstance(c, IdentityCluster)}
    assert "g_1" in grouped_by_fid
    assert set(grouped_by_fid["g_1"].external_managers) == {"Joe E", "Joe Eleff"}
    # Single-string and unresolved kept ungrouped
    singletons = [c for c in clusters if not isinstance(c, IdentityCluster)]
    assert any(r.manager == "Marc" for r in singletons)
    assert any(r.manager == "Donny T" for r in singletons)


def test_cluster_does_not_group_bound_via_guid():
    resolutions = [
        IdentityResolution("Joe E", "bound_via_guid", franchise_id="g_1"),
        IdentityResolution("Joseph E", "bound_via_guid", franchise_id="g_1"),
    ]
    clusters = cluster_resolutions(resolutions)
    assert not any(isinstance(c, IdentityCluster) for c in clusters)
    assert len(clusters) == 2
