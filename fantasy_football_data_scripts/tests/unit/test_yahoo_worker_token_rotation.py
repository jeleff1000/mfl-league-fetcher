import sys
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
DATA_SCRIPTS = ROOT / "fantasy_football_data_scripts"
for path in (ROOT, DATA_SCRIPTS):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

from multi_league.core import fly_writer
from multi_league.utils import credential_store
from scripts import refresh_yahoo_active_season


def test_exact_yahoo_rotation_updates_only_the_selected_owner_tuple(monkeypatch):
    statements: list[str] = []

    class Writer:
        def execute(self, sql, **kwargs):
            statements.append(sql)
            assert kwargs == {
                "database": "___ops",
                "timeout_seconds": 3,
                "server_timeout_seconds": 1,
                "max_retries": 1,
            }
            return [{"league_id": "461.l.90939"}]

    monkeypatch.setattr(credential_store, "CRYPTO_AVAILABLE", True)
    monkeypatch.setattr(credential_store, "get_encryption_key", lambda: "key")
    monkeypatch.setattr(credential_store, "encrypt_token", lambda token, key: f"enc:{token}:{key}")
    monkeypatch.setattr(fly_writer, "FlyWriter", Writer)

    credential_store.persist_rotated_league_refresh_token(
        league_id="461.l.90939",
        database_name="kmffl",
        refresh_token="replacement",
    )

    sql = statements[0]
    assert "WHERE league_id = '461.l.90939'" in sql
    assert "AND database_name = 'kmffl'" in sql
    assert " OR database_name" not in sql
    assert "encrypted_refresh_token = 'enc:replacement:key'" in sql


def test_worker_persists_a_replacement_token_on_the_credential_owner(monkeypatch):
    calls: list[dict[str, str]] = []
    monkeypatch.setattr(
        credential_store,
        "persist_rotated_league_refresh_token",
        lambda **kwargs: calls.append(kwargs),
    )

    changed = refresh_yahoo_active_season._persist_rotated_oauth_refresh_token(
        original_refresh_token="original",
        oauth=SimpleNamespace(refresh_token="replacement"),
        credential_database_name="kmffl",
        credential_league_id="461.l.90939",
    )

    assert changed is True
    assert calls == [{
        "league_id": "461.l.90939",
        "database_name": "kmffl",
        "refresh_token": "replacement",
    }]


def test_worker_skips_an_unchanged_refresh_token(monkeypatch):
    calls: list[dict[str, str]] = []
    monkeypatch.setattr(
        credential_store,
        "persist_rotated_league_refresh_token",
        lambda **kwargs: calls.append(kwargs),
    )

    changed = refresh_yahoo_active_season._persist_rotated_oauth_refresh_token(
        original_refresh_token="same",
        oauth=SimpleNamespace(refresh_token="same"),
        credential_database_name="kmffl",
        credential_league_id="461.l.90939",
    )

    assert changed is False
    assert calls == []
