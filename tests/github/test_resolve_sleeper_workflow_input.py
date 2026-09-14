from __future__ import annotations

import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / ".github" / "scripts" / "resolve_sleeper_workflow_input.py"


def load_module():
    spec = importlib.util.spec_from_file_location("resolve_sleeper_workflow_input_under_test", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_lookup_registered_sleeper_league_id_returns_latest_mapping(monkeypatch):
    module = load_module()
    monkeypatch.setattr(
        module,
        "fly_query",
        lambda sql, database="___ops": [{"sleeper_league_id": "1312406898149957632"}],
    )

    resolved = module.lookup_registered_sleeper_league_id("los_pollos_hermanos")

    assert resolved == "1312406898149957632"


def test_registered_mapping_can_promote_historical_payload_id(monkeypatch, capsys):
    module = load_module()
    league_data = {
        "league_name": "Los Pollos Hermanos",
        "database_name": "los_pollos_hermanos",
        "league_ids": {
            "2021": "649392774260535296",
            "2022": "784371022462857216",
            "2023": "918025601696030720",
            "2024": "1049811016416509952",
            "2025": "1180275670181478400",
        },
    }

    canonical_id, start_year, end_year = module.canonical_sleeper_league_id(league_data)
    assert canonical_id == "1180275670181478400"
    assert start_year == 2021
    assert end_year == 2025

    monkeypatch.setenv("LEAGUE_DATA_RAW", module.json.dumps(league_data))
    monkeypatch.setenv("PRE_RESOLVED_DATABASE_NAME", "los_pollos_hermanos")
    monkeypatch.setenv("IMPORT_MODE", "full")
    monkeypatch.setattr(module, "lookup_registered_sleeper_league_id", lambda db_name: "1312406898149957632")
    monkeypatch.setattr(
        module, "resolve_db_name", lambda league_id, league_name, pre_resolved_db_name: pre_resolved_db_name
    )

    module.main()
    captured = capsys.readouterr()
    output = module.json.loads(captured.out)

    assert output["outputs"]["sleeper_league_id"] == "1312406898149957632"
    assert output["league_data"]["sleeper_league_id"] == "1312406898149957632"
    assert output["outputs"]["database_name"] == "los_pollos_hermanos"
