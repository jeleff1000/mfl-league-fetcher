import pytest
from requests import Response

from scripts.quick_import_kmffl_2025_web import _authenticated_response, fetch_cached_page


def test_rejects_yahoo_login_shell_that_keeps_the_fantasy_host():
    """Yahoo can host its sign-in shell at football.fantasysports.yahoo.com."""
    response = Response()
    response.status_code = 200
    response.url = (
        "https://football.fantasysports.yahoo.com/?.src=Fantasy"
        "&specId=usernameRegWithName"
    )
    response._content = b"<html><title>Login - Sign in to Yahoo</title></html>"

    with pytest.raises(PermissionError, match="refresh the local cookie jar"):
        _authenticated_response(response)


def test_rejects_yahoo_error_shell_before_metadata_parsing():
    """Signed-in users without league access get Yahoo's generic error shell."""
    response = Response()
    response.status_code = 200
    response.url = "https://football.fantasysports.yahoo.com/2026/f1/71778/settings"
    response._content = b"<html><title>There was a problem | Fantasy Football | Yahoo! Sports</title></html>"

    with pytest.raises(PermissionError, match="could not access this league"):
        _authenticated_response(response)


def test_replaces_cached_yahoo_login_shell_before_parsing(tmp_path):
    """A failed session must not poison the cache after cookies are refreshed."""
    cached_page = tmp_path / "settings.html"
    cached_page.write_bytes(
        b"<html><title>Login - Sign in to Yahoo</title>" + b"x" * 2_000
    )

    fresh_response = Response()
    fresh_response.status_code = 200
    fresh_response.url = "https://football.fantasysports.yahoo.com/2026/f1/71778/settings"
    fresh_response._content = b"<html><title>League Settings</title>fresh page</html>"

    class FreshSession:
        def get(self, *_args, **_kwargs):
            return fresh_response

    result = fetch_cached_page(
        FreshSession(),
        fresh_response.url,
        cached_page,
        delay_seconds=0,
    )

    assert result == "<html><title>League Settings</title>fresh page</html>"
    assert cached_page.read_text(encoding="utf-8") == result


def test_replaces_cached_yahoo_error_shell_before_parsing(tmp_path):
    """A cached inaccessible-league page must not survive a valid retry."""
    cached_page = tmp_path / "settings.html"
    cached_page.write_bytes(
        b"<html><title>There was a problem | Fantasy Football | Yahoo! Sports</title>"
        + b"x" * 2_000
    )

    fresh_response = Response()
    fresh_response.status_code = 200
    fresh_response.url = "https://football.fantasysports.yahoo.com/2026/f1/71778/settings"
    fresh_response._content = b"<html><title>League Settings</title>fresh page</html>"

    class FreshSession:
        def get(self, *_args, **_kwargs):
            return fresh_response

    result = fetch_cached_page(
        FreshSession(),
        fresh_response.url,
        cached_page,
        delay_seconds=0,
    )

    assert result == "<html><title>League Settings</title>fresh page</html>"
    assert cached_page.read_text(encoding="utf-8") == result
