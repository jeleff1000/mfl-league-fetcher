import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = ROOT / ".github" / "scripts"


def load_script(name: str):
    path = SCRIPTS_DIR / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_resolve_uses_mapping_when_inventory_row_is_provisional(monkeypatch):
    mod = load_script("resolve_db_name")

    monkeypatch.setenv("DATABASE_SERVER_URL", "https://db.example")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "token")
    monkeypatch.setattr(mod, "lookup_inventory_identity", lambda *args: None)
    monkeypatch.setattr(
        mod,
        "lookup_inventory_owner",
        lambda db: {
            "database_name": "where_did_paul_go",
            "platform": "sleeper",
            "league_id": None,
            "league_name": "Where did Paul go?",
        }
        if db == "where_did_paul_go"
        else None,
    )
    monkeypatch.setattr(mod, "lookup_mapping_table", lambda league_id, platform: "where_did_paul_go")
    monkeypatch.setattr(mod, "check_registry_collision", lambda *args: False)
    monkeypatch.setattr(mod, "check_db_exists", lambda *args: False)

    resolved = mod.resolve("1312934875560448000", "Where did Paul go?", "sleeper")

    assert resolved == "where_did_paul_go"


def test_resolve_hashes_when_inventory_owner_has_different_identity(monkeypatch):
    mod = load_script("resolve_db_name")

    monkeypatch.setenv("DATABASE_SERVER_URL", "https://db.example")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "token")
    monkeypatch.setattr(mod, "lookup_inventory_identity", lambda *args: None)

    def owner(db_name):
        if db_name == "where_did_paul_go":
            return {
                "database_name": "where_did_paul_go",
                "platform": "sleeper",
                "league_id": "999",
                "league_name": "Other League",
            }
        return None

    monkeypatch.setattr(mod, "lookup_inventory_owner", owner)
    monkeypatch.setattr(mod, "lookup_mapping_table", lambda league_id, platform: None)

    resolved = mod.resolve("1312934875560448000", "Where did Paul go?", "sleeper")

    assert resolved.startswith("where_did_paul_go_")
    assert resolved != "where_did_paul_go"


def test_resolve_prefers_explicit_free_slug_over_temporary_mapping(monkeypatch):
    """An explicit unclaimed target must not inherit a temporary credential slug."""
    mod = load_script("resolve_db_name")

    monkeypatch.setenv("DATABASE_SERVER_URL", "https://db.example")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "token")
    monkeypatch.setattr(mod, "lookup_inventory_identity", lambda *args: None)
    monkeypatch.setattr(mod, "lookup_inventory_owner", lambda *args: None)
    monkeypatch.setattr(mod, "lookup_mapping_table", lambda *args: "if_you_re_not_first_youre_last_5c48")
    monkeypatch.setattr(mod, "check_registry_collision", lambda *args: False)
    monkeypatch.setattr(mod, "check_db_exists", lambda *args: False)

    resolved = mod.resolve(
        "470.l.951993",
        "If You're Not First Youre Last",
        "yahoo",
        "if_you_re_not_first_youre_last",
    )

    assert resolved == "if_you_re_not_first_youre_last"


def test_sleeper_input_prefers_payload_db_when_registered_to_current_id(monkeypatch, tmp_path, capsys):
    mod = load_script("resolve_sleeper_workflow_input")
    sleeper_id = "1312934875560448000"

    monkeypatch.setenv("IMPORT_MODE", "full")
    monkeypatch.setenv("PRE_RESOLVED_DATABASE_NAME", "where_did_paul_go_41b5")
    monkeypatch.setenv("OUTPUT_JSON_PATH", str(tmp_path / "league_data_input.json"))
    monkeypatch.delenv("GITHUB_OUTPUT", raising=False)
    monkeypatch.setattr(
        mod,
        "decode_league_data",
        lambda: {
            "league_name": "Where did Paul go?",
            "sleeper_league_id": sleeper_id,
            "league_id": sleeper_id,
            "database_name": "where_did_paul_go",
            "start_year": 2025,
            "end_year": 2025,
        },
    )
    monkeypatch.setattr(
        mod,
        "lookup_registered_sleeper_league_id",
        lambda db_name: sleeper_id if db_name == "where_did_paul_go" else "",
    )
    monkeypatch.setattr(mod, "resolve_db_name", lambda league_id, league_name, pre_resolved: pre_resolved)

    mod.main()

    payload = json.loads(capsys.readouterr().out)
    assert payload["outputs"]["database_name"] == "where_did_paul_go"
