"""Tests for multi_league.utils.credential_store."""

from __future__ import annotations

import sys
from pathlib import Path


_test_dir = Path(__file__).resolve().parent
_tests_dir = _test_dir.parent.parent
_scripts_dir = _tests_dir.parent
sys.path.insert(0, str(_scripts_dir))

from multi_league.utils import credential_store


class DummyConn:
    def __init__(self, existing_row=None):
        self.existing_row = existing_row
        self.executed: list[tuple[str, list]] = []
        self.closed = False

    def execute(self, sql: str, params=None):
        self.executed.append((sql, list(params or [])))
        return self

    def fetchone(self):
        return self.existing_row

    def close(self):
        self.closed = True


class DummyFlyWriter:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def execute(self, sql: str, database: str = "___leagues"):
        self.calls.append((sql, database))
        return []


class DummyReader:
    def __init__(self, rows):
        self.rows = rows
        self.queries: list[tuple[str, str]] = []

    def query(self, sql: str, database: str):
        self.queries.append((sql, database))
        return self.rows


def test_store_league_credentials_writes_yahoo_credentials_to_fly(monkeypatch):
    writer = DummyFlyWriter()

    from multi_league.core import fly_writer

    monkeypatch.setattr(credential_store, "_is_fly_backend", lambda: True)
    monkeypatch.setattr(credential_store, "CRYPTO_AVAILABLE", True)
    monkeypatch.setattr(credential_store, "encrypt_token", lambda token, key: "enc'token")
    monkeypatch.setattr(fly_writer, "FlyWriter", lambda: writer)

    ok = credential_store.store_league_credentials(
        league_id="470.l.13655",
        league_name="TUTVFL",
        refresh_token="refresh-token",
        database_name="tutvfl",
        encryption_key="test-key",
    )

    assert ok is True
    assert len(writer.calls) == 1
    sql, database = writer.calls[0]
    assert database == "___ops"
    assert "main.league_credentials" in sql
    assert "accounts.league_inventory" in sql
    assert "'470.l.13655'" in sql
    assert "'tutvfl'" in sql
    assert "'enc''token'" in sql


def test_decrypt_token_accepts_unpadded_frontend_tokens():
    if not credential_store.CRYPTO_AVAILABLE:
        return

    key = credential_store.Fernet.generate_key().decode()
    plaintext = "refresh-token-from-frontend"
    encrypted = credential_store.encrypt_token(plaintext, key)

    assert credential_store.decrypt_token(encrypted.rstrip("="), key.rstrip("=")) == plaintext


def test_store_espn_league_preserves_existing_cookies_when_none_are_provided(monkeypatch):
    conn = DummyConn(existing_row=("enc-s2-existing", "enc-swid-existing"))
    # store_espn_league early-returns True on Fly backend (the production
    # default). These tests cover the legacy MotherDuck write path, so force
    # _is_fly_backend() False before exercising it.
    monkeypatch.setattr(credential_store, "_is_fly_backend", lambda: False)
    monkeypatch.setattr(credential_store.duckdb, "connect", lambda target: conn)

    ok = credential_store.store_espn_league(
        espn_league_id=123,
        league_name="Public Reimport",
        database_name="public_reimport",
        motherduck_token="tok123",
    )

    assert ok is True
    # Find the espn_leagues UPDATE (not the league_inventory one appended after)
    espn_update = [(sql, params) for sql, params in conn.executed if "UPDATE ___ops.espn_leagues" in sql]
    assert len(espn_update) == 1
    update_sql, update_params = espn_update[0]
    assert update_params == [
        "Public Reimport",
        "public_reimport",
        "enc-s2-existing",
        "enc-swid-existing",
        123,
    ]
    assert conn.closed is True


def test_store_espn_league_updates_existing_cookies_when_new_values_are_provided(monkeypatch):
    conn = DummyConn(existing_row=("enc-s2-existing", "enc-swid-existing"))
    # See sibling test for why we override _is_fly_backend.
    monkeypatch.setattr(credential_store, "_is_fly_backend", lambda: False)
    monkeypatch.setattr(credential_store.duckdb, "connect", lambda target: conn)
    monkeypatch.setattr(credential_store, "encrypt_token", lambda value, key: f"enc::{value}")

    ok = credential_store.store_espn_league(
        espn_league_id=456,
        league_name="Private Reimport",
        database_name="private_reimport",
        espn_s2="new-s2",
        swid="new-swid",
        encryption_key="test-key",
        motherduck_token="tok123",
    )

    assert ok is True
    # Find the espn_leagues UPDATE (not the league_inventory one appended after)
    espn_update = [(sql, params) for sql, params in conn.executed if "UPDATE ___ops.espn_leagues" in sql]
    assert len(espn_update) == 1
    update_sql, update_params = espn_update[0]
    assert update_params == [
        "Private Reimport",
        "private_reimport",
        "enc::new-s2",
        "enc::new-swid",
        456,
    ]
    assert conn.closed is True


def test_retrieve_espn_credentials_by_league_id_decrypts_cookie_row(monkeypatch):
    if not credential_store.CRYPTO_AVAILABLE:
        return

    key = credential_store.Fernet.generate_key().decode()
    reader = DummyReader(
        [
            {
                "espn_league_id": 134179,
                "league_name": "Private ESPN",
                "database_name": "private_espn",
                "encrypted_espn_s2": credential_store.encrypt_token("s2-cookie", key),
                "encrypted_swid": credential_store.encrypt_token("{SWID}", key),
            }
        ]
    )

    from multi_league.core import db_reader

    monkeypatch.setattr(db_reader, "get_reader", lambda: reader)

    creds = credential_store.retrieve_espn_credentials_by_league_id(134179, encryption_key=key)

    assert creds == {
        "league_id": 134179,
        "league_name": "Private ESPN",
        "database_name": "private_espn",
        "espn_s2": "s2-cookie",
        "swid": "{SWID}",
    }
    assert reader.queries[0][1] == "___ops"
    assert "espn_league_id = 134179" in reader.queries[0][0]


def test_retrieve_espn_credentials_reuses_a_supplied_fly_reader():
    """Active ESPN refreshes must reuse their existing Fly reader for credentials."""
    if not credential_store.CRYPTO_AVAILABLE:
        return

    key = credential_store.Fernet.generate_key().decode()
    reader = DummyReader(
        [
            {
                "espn_league_id": 134179,
                "league_name": "Private ESPN",
                "database_name": "private_espn",
                "encrypted_espn_s2": credential_store.encrypt_token("s2-cookie", key),
                "encrypted_swid": credential_store.encrypt_token("{SWID}", key),
            }
        ]
    )

    creds = credential_store.retrieve_espn_credentials(
        "private_espn",
        encryption_key=key,
        reader=reader,
    )

    assert creds == {
        "league_id": 134179,
        "league_name": "Private ESPN",
        "database_name": "private_espn",
        "espn_s2": "s2-cookie",
        "swid": "{SWID}",
    }
    assert len(reader.queries) == 1
    assert "database_name = 'private_espn'" in reader.queries[0][0]


def test_retrieve_espn_credentials_returns_none_when_cookie_row_is_public(monkeypatch):
    reader = DummyReader(
        [
            {
                "espn_league_id": 134179,
                "league_name": "Public ESPN",
                "database_name": "public_espn",
                "encrypted_espn_s2": None,
                "encrypted_swid": None,
            }
        ]
    )

    from multi_league.core import db_reader

    monkeypatch.setattr(db_reader, "get_reader", lambda: reader)

    assert credential_store.retrieve_espn_credentials_by_league_id(134179, encryption_key="unused") is None
