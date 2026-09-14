from __future__ import annotations

import json

import pytest

from multi_league.utils.yahoo_auth_mode import (
    YahooAuthMode,
    resolve_yahoo_auth_mode,
    validate_cookie_jar_payload,
)
from multi_league.utils.credential_store import encrypt_token, retrieve_yahoo_cookie_credentials


def test_default_mode_is_oauth():
    assert resolve_yahoo_auth_mode(None, None) is YahooAuthMode.OAUTH


def test_explicit_mode_wins_over_stored_mode():
    assert resolve_yahoo_auth_mode("cookie", "oauth") is YahooAuthMode.COOKIE
    assert resolve_yahoo_auth_mode("oauth", "cookie") is YahooAuthMode.OAUTH


def test_stored_mode_is_used_when_explicit_mode_is_absent():
    assert resolve_yahoo_auth_mode(None, "cookie") is YahooAuthMode.COOKIE


@pytest.mark.parametrize("value", [None, "", "not-a-cookie-jar", [], {"cookies": []}])
def test_cookie_payload_requires_a_cookie_list(value):
    with pytest.raises(ValueError):
        validate_cookie_jar_payload(value)


def test_cookie_payload_accepts_netscape_rows_without_exposing_values():
    payload = {
        "format": "netscape",
        "cookies": [
            {
                "domain": ".yahoo.com",
                "name": "session",
                "value": "secret",
                "path": "/",
            }
        ],
    }
    result = validate_cookie_jar_payload(payload)
    assert result["format"] == "netscape"
    assert result["cookie_count"] == 1
    assert "secret" not in str(result)


def test_cookie_credential_read_decrypts_only_at_worker_boundary(monkeypatch):
    from cryptography.fernet import Fernet

    key = Fernet.generate_key().decode()
    payload = {"format": "netscape", "cookies": [{"domain": ".yahoo.com", "name": "session", "value": "secret"}]}
    encrypted = encrypt_token(__import__("json").dumps(payload), key)

    class Reader:
        def query(self, sql, database):
            assert database == "___ops"
            return [{
                "league_id": "nfl.l.1",
                "league_name": "Test",
                "database_name": "test",
                "encrypted_cookie_jar": encrypted,
                "cookie_format": "netscape",
                "captured_at": None,
                "expires_at": None,
                "status": "active",
            }]

    monkeypatch.setenv("CREDENTIAL_ENCRYPTION_KEY", key)
    result = retrieve_yahoo_cookie_credentials(Reader(), "test")
    assert result["cookie_payload"] == payload


def test_cookie_context_returns_a_yahoo_api_compatible_session(tmp_path):
    """Cookie mode must feed the shared Yahoo fetchers, not an HTML runner."""
    from multi_league.core.league_context import LeagueContext

    cookie_path = tmp_path / "cookies.json"
    cookie_path.write_text(
        json.dumps({"cookies": [{"domain": ".yahoo.com", "name": "T", "value": "secret"}]}),
        encoding="utf-8",
    )
    ctx = LeagueContext(
        league_id="461.l.90939",
        league_name="KMFFL",
        require_oauth=False,
        yahoo_auth_mode="cookie",
        cookie_jar_path=str(cookie_path),
    )

    auth = ctx.get_oauth_session()

    assert auth.token_is_valid() is True
    assert auth.session.__class__.__name__ == "CookieYahooApiSession"


def test_settings_phase_uses_the_cookie_context_session():
    """Settings fetches are part of the shared path and must not require OAuth files."""
    from initial_import_v3 import _get_shared_yahoo_session

    sentinel = object()

    class CookieContext:
        yahoo_auth_mode = "cookie"

        def get_oauth_session(self):
            return sentinel

    assert _get_shared_yahoo_session(CookieContext(), oauth_file=None) is sentinel
