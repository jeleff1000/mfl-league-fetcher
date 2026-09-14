# tests/integration/test_round_trip.py
"""End-to-end test: scan → user-decides → apply_mappings → persist → re-scan = zero prompts."""

from dataclasses import dataclass
import json
import pandas as pd
from multi_league.external_ingest.scan import scan_external_staging
from multi_league.external_ingest.apply_mappings import apply_mappings, ColumnMap, IdentityDecision
from multi_league.external_ingest.config_io import (
    SourceMapping,
    IdentityMapping,
    write_external_source_config,
    read_external_source_config,
    write_external_alias_mappings,
    read_external_alias_mappings,
)
from multi_league.external_ingest.auto_map import column_set_hash


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


def test_full_round_trip(tmp_path):
    # KMFFL-like: matchup with guid (auto-resolves), draft without guid (auto-suggests by name)
    staged = {
        "matchup": pd.DataFrame(
            {
                "year": [2013, 2013],
                "week": [1, 1],
                "manager": ["Ezra", "Marc"],
                "opponent": ["Marc", "Ezra"],
                "team_points": [100.0, 90.0],
                "opponent_points": [90.0, 100.0],
                "manager_guid": ["SAQH", "FYW5"],
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
    registry = FakeRegistry(
        (
            FakeFranchise("g_1", "Ezra", "SAQH"),
            FakeFranchise("g_2", "Marc", "FYW5"),
        )
    )

    # First scan: empty saved configs
    scan = scan_external_staging(staged, registry, saved_source_configs=[], saved_identity_mappings={})
    assert scan.tables["matchup"].satisfaction_status == "satisfied"
    assert scan.tables["draft"].satisfaction_status == "satisfied"

    # Resolution states: matchup managers via guid, draft managers via name auto-suggest
    by_mgr = {}
    for r in scan.resolutions:
        if hasattr(r, "manager"):
            by_mgr[r.manager] = r
    assert by_mgr["Ezra"].state in ("bound_via_guid", "auto_suggested")
    assert by_mgr["Marc"].state in ("bound_via_guid", "auto_suggested")

    # User confirms: build payload from scan
    column_maps = [ColumnMap(table=t, column_map=ts.column_map) for t, ts in scan.tables.items()]
    identity_decisions = {
        "Ezra": IdentityDecision(franchise_id="g_1", owner_guid="SAQH"),
        "Marc": IdentityDecision(franchise_id="g_2", owner_guid="FYW5"),
    }

    # Apply
    result = apply_mappings(staged, column_maps, identity_decisions, league_db="kmffl")
    assert result.unmapped_managers_count == 0
    # Draft rows now have manager_guid
    draft_out = result.dfs["draft"]
    assert "manager_guid" in draft_out.columns
    assert set(draft_out["manager_guid"].dropna()) == {"SAQH", "FYW5"}

    # Persist configs
    src_path = tmp_path / "external_source_config.json"
    fc_path = tmp_path / "franchise_config.json"
    fc_path.write_text(json.dumps({"version": "1.1", "franchises": []}))
    src_mappings = [
        SourceMapping(
            source_signature=column_set_hash(list(staged[t].columns)),
            filename_glob=f"{t}_*",
            table=t,
            column_map=ts.column_map,
        )
        for t, ts in scan.tables.items()
    ]
    write_external_source_config(src_path, src_mappings)
    write_external_alias_mappings(
        fc_path, {m: IdentityMapping(franchise_id=d.franchise_id) for m, d in identity_decisions.items()}
    )

    # Second scan: load persisted configs
    saved_src = read_external_source_config(src_path)
    saved_id = read_external_alias_mappings(fc_path)
    scan2 = scan_external_staging(staged, registry, saved_source_configs=saved_src, saved_identity_mappings=saved_id)

    # All resolutions should be pre-bound (no unresolved)
    for r in scan2.resolutions:
        if hasattr(r, "state"):
            assert r.state != "unresolved", f"{r.manager} is unresolved on re-import"
