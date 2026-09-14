from dataclasses import dataclass
import pandas as pd
from multi_league.external_ingest.scan import scan_external_staging


@dataclass
class FakeFranchise:
    franchise_id: str
    franchise_name: str
    owner_guid: str
    historical_managers: tuple = ()


@dataclass
class FakeRegistry:
    franchises: tuple
    external_alias_mappings: dict = None

    def __post_init__(self):
        if self.external_alias_mappings is None:
            self.external_alias_mappings = {}


def test_scan_returns_per_table_schema_status():
    staged = {
        "matchup": pd.DataFrame(
            {
                "year": [2013],
                "week": [1],
                "manager": ["Ezra"],
                "opponent": ["Marc"],
                "team_points": [100.0],
                "opponent_points": [90.0],
            }
        ),
    }
    result = scan_external_staging(
        staged, registry=FakeRegistry(()), saved_source_configs=[], saved_identity_mappings={}
    )
    assert "matchup" in result.tables
    assert result.tables["matchup"].satisfaction_status in ("ok", "satisfied")


def test_scan_enumerates_distinct_managers_across_tables():
    staged = {
        "matchup": pd.DataFrame(
            {
                "year": [2013, 2013],
                "week": [1, 1],
                "manager": ["Ezra", "Marc"],
                "opponent": ["Marc", "Ezra"],
                "team_points": [100.0, 90.0],
                "opponent_points": [90.0, 100.0],
            }
        ),
        "draft": pd.DataFrame(
            {
                "year": [2013, 2013],
                "manager": ["Ezra", "Marc"],
                "player": ["Brees", "Brady"],
                "round": [1, 1],
                "pick": [1, 2],
            }
        ),
    }
    result = scan_external_staging(
        staged, registry=FakeRegistry(()), saved_source_configs=[], saved_identity_mappings={}
    )
    managers = {o.manager for o in result.identity_occurrences}
    assert managers == {"Ezra", "Marc"}


def test_scan_resolves_via_guid_when_present():
    staged = {
        "matchup": pd.DataFrame(
            {
                "year": [2013],
                "week": [1],
                "manager": ["Ezra"],
                "opponent": ["Marc"],
                "team_points": [100.0],
                "opponent_points": [90.0],
                "manager_guid": ["SAQH"],
            }
        ),
    }
    registry = FakeRegistry((FakeFranchise("g_1", "Ezra", "SAQH"),))
    result = scan_external_staging(staged, registry=registry, saved_source_configs=[], saved_identity_mappings={})
    res_by_mgr = {r.manager: r for r in result.resolutions if hasattr(r, "manager")}
    assert res_by_mgr["Ezra"].state == "bound_via_guid"
    assert res_by_mgr["Ezra"].franchise_id == "g_1"


def test_scan_applies_saved_identity_mappings_first():
    from multi_league.external_ingest.config_io import IdentityMapping

    staged = {
        "draft": pd.DataFrame({"year": [2013], "manager": ["Ezra"], "player": ["Brees"], "round": [1], "pick": [1]}),
    }
    registry = FakeRegistry((FakeFranchise("g_1", "Different Name", "guid_1"),))
    saved = {"Ezra": IdentityMapping(franchise_id="g_1", source="manual_wizard")}
    result = scan_external_staging(staged, registry=registry, saved_source_configs=[], saved_identity_mappings=saved)
    res_by_mgr = {r.manager: r for r in result.resolutions if hasattr(r, "manager")}
    # Ezra should be pre-resolved from saved mapping (not unresolved despite different canonical name)
    assert res_by_mgr["Ezra"].state in ("auto_suggested", "saved")
    assert res_by_mgr["Ezra"].franchise_id == "g_1"
