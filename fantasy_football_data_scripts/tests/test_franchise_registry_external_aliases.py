# tests/test_franchise_registry_external_aliases.py
import json
import pandas as pd
from multi_league.core.franchise_registry import FranchiseRegistry


def test_registry_loads_external_alias_mappings(tmp_path):
    fc_path = tmp_path / "franchise_config.json"
    fc_path.write_text(
        json.dumps(
            {
                "version": "1.2",
                "franchises": [],
                "hidden_guid_merges": {},
                "guid_merges": {},
                "external_alias_mappings": {"Ezra": {"franchise_id": "g_1"}},
            }
        )
    )
    matchup = pd.DataFrame(
        {
            "year": [2013],
            "week": [1],
            "manager_guid": ["g"],
            "manager": ["X"],
            "team_name": ["T"],
            "team_key": ["k.t.1"],
        }
    )
    registry = FranchiseRegistry.from_data(matchup_df=matchup, existing_config=fc_path)
    assert registry.external_alias_mappings == {"Ezra": {"franchise_id": "g_1"}}


def test_merge_franchises_repoints_external_alias_mappings(tmp_path):
    # Regression: merging franchise A→B must rewrite alias entries that used to
    # point at A, otherwise persisted aliases become orphans and identity_match
    # silently stops resolving them.
    matchup = pd.DataFrame(
        {
            "year": [2013, 2013],
            "week": [1, 1],
            "manager_guid": ["g_source", "g_target"],
            "manager": ["SourceMgr", "TargetMgr"],
            "team_name": ["S", "T"],
            "team_key": ["k.t.1", "k.t.2"],
        }
    )
    registry = FranchiseRegistry.from_data(matchup_df=matchup)
    # Pick the actual franchise_ids the registry minted from the input rows.
    fids = list(registry.franchises.keys())
    assert len(fids) == 2, fids
    source_id, target_id = fids[0], fids[1]
    registry.external_alias_mappings = {
        "Ezra T.": {"franchise_id": source_id},
        "Marc": {"franchise_id": target_id},
    }
    registry.merge_franchises(source_id, target_id)
    assert source_id not in registry.franchises
    # Alias previously bound to source must now point at target — not orphaned.
    assert registry.external_alias_mappings["Ezra T."]["franchise_id"] == target_id
    assert registry.external_alias_mappings["Marc"]["franchise_id"] == target_id


def test_registry_saves_external_alias_mappings(tmp_path):
    fc_path = tmp_path / "franchise_config.json"
    matchup = pd.DataFrame(
        {
            "year": [2013],
            "week": [1],
            "manager_guid": ["g"],
            "manager": ["X"],
            "team_name": ["T"],
            "team_key": ["k.t.1"],
        }
    )
    registry = FranchiseRegistry.from_data(matchup_df=matchup)
    registry.external_alias_mappings = {"Ezra": {"franchise_id": "g_1"}}
    registry.save(fc_path)
    payload = json.loads(fc_path.read_text())
    assert payload["external_alias_mappings"] == {"Ezra": {"franchise_id": "g_1"}}
    assert payload["version"] in ("1.2",)
