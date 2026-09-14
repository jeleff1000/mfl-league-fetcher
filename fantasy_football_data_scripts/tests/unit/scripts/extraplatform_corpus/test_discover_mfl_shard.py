import importlib.util
from pathlib import Path


SCRIPT = Path(__file__).parents[5] / "scripts" / "extraplatform_corpus" / "discover_mfl_shard.py"
SPEC = importlib.util.spec_from_file_location("discover_mfl_shard", SCRIPT)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(MODULE)


def test_shard_ids_partition_each_band_without_overlap():
    all_ids = set()
    for shard in range(4):
        ids = set(MODULE.shard_ids([(10, 21)], shard_index=shard, shard_count=4))
        assert not all_ids.intersection(ids)
        all_ids.update(ids)
    assert all_ids == set(range(10, 22))


def test_history_links_preserve_seed_and_linked_season_ids():
    payload = {
        "history": {
            "league": [
                {"year": "2024", "url": "https://www1.myfantasyleague.com/2024/home/123"},
                {"year": "2003", "url": "https://www1.myfantasyleague.com/2003/home/77"},
            ]
        }
    }
    rows = MODULE.history_records(2024, "123", payload, "Test League")
    assert rows == [
        {"year": 2003, "id": "77", "lineage_seed": "2024:123", "link_type": "history"},
        {"year": 2024, "id": "123", "lineage_seed": "2024:123", "link_type": "seed"},
    ]
