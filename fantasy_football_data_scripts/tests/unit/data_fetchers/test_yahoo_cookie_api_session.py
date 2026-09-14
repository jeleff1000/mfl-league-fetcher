from __future__ import annotations

from dataclasses import dataclass

from requests.cookies import RequestsCookieJar


@dataclass
class FakeResponse:
    status_code: int
    content: bytes
    headers: dict[str, str]

    @property
    def text(self) -> str:
        return self.content.decode("utf-8")


class FakeSession:
    def __init__(self):
        self.headers: dict[str, str] = {}
        self.cookies = RequestsCookieJar()
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, **kwargs):
        self.calls.append((url, kwargs))
        if url.endswith("/crumb"):
            return FakeResponse(200, b"<fantasy_content><crumb>test-crumb</crumb></fantasy_content>", {})
        return FakeResponse(200, b"<fantasy_content><league/></fantasy_content>", {})


def test_cookie_session_rewrites_an_oauth_xml_request_to_the_browser_api():
    """Cookie auth must use the existing XML fetcher URLs, not HTML pages."""
    from multi_league.data_fetchers.yahoo.cookie_api_session import CookieYahooApiSession

    raw = FakeSession()
    session = CookieYahooApiSession(
        {"cookies": [{"domain": ".yahoo.com", "name": "T", "value": "secret"}]},
        raw_session=raw,
    )

    response = session.get(
        "https://fantasysports.yahooapis.com/fantasy/v2/league/461.l.90939/scoreboard;week=1"
    )

    assert response.status_code == 200
    url, kwargs = raw.calls[-1]
    assert url == "https://pub-api-rw.fantasysports.yahoo.com/fantasy/v2/league/461.l.90939/scoreboard;week=1"
    assert kwargs["params"] == {"format": "xml"}
    assert kwargs["headers"]["x-crumb"] == "test-crumb"


def test_cookie_session_preserves_yahoo_fantasy_api_json_format_requests():
    """The bundled Yahoo client passes format=json and must keep receiving JSON."""
    from multi_league.data_fetchers.yahoo.cookie_api_session import CookieYahooApiSession

    raw = FakeSession()
    session = CookieYahooApiSession(
        {"cookies": [{"domain": ".yahoo.com", "name": "Y", "value": "secret"}]},
        raw_session=raw,
    )

    session.get(
        "https://fantasysports.yahooapis.com/fantasy/v2/league/461.l.90939/settings",
        params={"format": "json"},
    )

    _, kwargs = raw.calls[-1]
    assert kwargs["params"] == {"format": "json"}
