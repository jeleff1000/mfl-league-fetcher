from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path


SCRIPT_PATH = Path(__file__).resolve().parents[2] / ".github" / "scripts" / "resolve_db_name.py"


def load_module():
    spec = importlib.util.spec_from_file_location("resolve_db_name_under_test", SCRIPT_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


def test_mapping_table_wins_over_precomputed(monkeypatch):
    module = load_module()
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://example.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "token")
    monkeypatch.setattr(module, "lookup_inventory_identity", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "lookup_inventory_owner", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "lookup_mapping_table", lambda league_id, platform: "yk_jff_vilde_hatzooleh_league")
    monkeypatch.setattr(module, "check_registry_collision", lambda *args, **kwargs: False)
    monkeypatch.setattr(module, "check_db_exists", lambda *args, **kwargs: False)

    resolved = module.resolve(
        league_id="461.l.724709",
        league_name="'Make FF great again'",
        platform="yahoo",
        pre_computed_db="make_ff_great_again",
    )

    assert resolved == "yk_jff_vilde_hatzooleh_league"


def test_precomputed_name_is_trusted_without_catalog_when_no_collision(monkeypatch):
    module = load_module()
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://example.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "token")
    monkeypatch.setattr(module, "lookup_inventory_identity", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "lookup_inventory_owner", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "lookup_mapping_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "check_registry_collision", lambda *args, **kwargs: False)
    monkeypatch.setattr(module, "check_db_exists", lambda *args, **kwargs: False)

    resolved = module.resolve(
        league_id="461.l.724709",
        league_name="'Make FF great again'",
        platform="yahoo",
        pre_computed_db="yk_jff_vilde_hatzooleh_league",
    )

    assert resolved == "yk_jff_vilde_hatzooleh_league"


def test_precomputed_collision_still_hashes_to_safe_name(monkeypatch):
    module = load_module()
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://example.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "token")
    monkeypatch.setattr(module, "lookup_inventory_identity", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "lookup_inventory_owner", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "lookup_mapping_table", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "slugify", lambda name: "make_ff_great_again")
    monkeypatch.setattr(module, "check_db_exists", lambda *args, **kwargs: False)

    def fake_collision(name, league_id, platform):
        return name in {"legacy_claimed_name", "make_ff_great_again"}

    monkeypatch.setattr(module, "check_registry_collision", fake_collision)

    league_id = "461.l.724709"
    resolved = module.resolve(
        league_id=league_id,
        league_name="'Make FF great again'",
        platform="yahoo",
        pre_computed_db="legacy_claimed_name",
    )

    expected = f"make_ff_great_again_{hashlib.md5(f'yahoo:{league_id}'.encode()).hexdigest()[:4]}"
    assert resolved == expected


def test_inventory_existing_identity_wins_over_stale_precomputed(monkeypatch):
    module = load_module()
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://example.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "token")
    monkeypatch.setattr(
        module,
        "lookup_inventory_identity",
        lambda *args, **kwargs: {
            "database_name": "fantasy_elite_4e58",
            "platform": "yahoo",
            "league_id": "414.l.381184",
            "league_name": "Fantasy Elite",
        },
    )
    monkeypatch.setattr(module, "lookup_inventory_owner", lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "lookup_mapping_table", lambda *args, **kwargs: "fantasy_elite")

    resolved = module.resolve(
        league_id="414.l.381184",
        league_name="Fantasy Elite",
        platform="yahoo",
        pre_computed_db="fantasy_elite",
    )

    assert resolved == "fantasy_elite_4e58"
