from __future__ import annotations

import importlib.util
from pathlib import Path


def load_import_dispatch():
    path = Path(__file__).resolve().parents[1] / "scripts" / "import_dispatch.py"
    spec = importlib.util.spec_from_file_location("import_dispatch_for_test", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_parse_merge_years_accepts_ranges_and_commas():
    module = load_import_dispatch()

    assert module.parse_merge_years("2020,2018-2019,2017") == [2017, 2018, 2019, 2020]


def test_build_merge_source_from_args():
    module = load_import_dispatch()

    class Args:
        merge_from = "fantasy_elite_4e58"
        merge_years = "2014-2020"
        manager_maps = ["Joey=Joseph", " Same = Same "]

    assert module.build_merge_source_from_args(Args()) == {
        "source_db": "fantasy_elite_4e58",
        "manager_mapping": {"Joey": "Joseph"},
        "merge_years": [2014, 2015, 2016, 2017, 2018, 2019, 2020],
    }


def test_sleeper_dispatch_attaches_merge_source(monkeypatch):
    module = load_import_dispatch()
    captured = {}

    def fake_dispatch(platform, league_data, dry_run=False):
        captured["platform"] = platform
        captured["league_data"] = league_data
        captured["dry_run"] = dry_run
        return True

    monkeypatch.setattr(module, "dispatch_workflow", fake_dispatch)

    ok = module.sleeper_dispatch(
        {
            "latest_id": "1312124522580672512",
            "name": "Fantasy Elite",
            "end_year": 2026,
            "start_year": 2021,
            "num_teams": 12,
            "league_ids": {"2021": "old", "2026": "new"},
        },
        database_name="fantasy_elite",
        dry_run=True,
        merge_source={
            "source_db": "fantasy_elite_4e58",
            "manager_mapping": {},
            "merge_years": [2014, 2015, 2016, 2017, 2018, 2019, 2020],
        },
    )

    assert ok is True
    assert captured["platform"] == "sleeper"
    assert captured["dry_run"] is True
    assert captured["league_data"]["database_name"] == "fantasy_elite"
    assert captured["league_data"]["has_external_data"] is True
    assert captured["league_data"]["merge_source"]["source_db"] == "fantasy_elite_4e58"
    assert captured["league_data"]["merge_sources"] == [captured["league_data"]["merge_source"]]
    assert captured["league_data"]["merge_source"]["merge_years"] == [
        2014,
        2015,
        2016,
        2017,
        2018,
        2019,
        2020,
    ]
