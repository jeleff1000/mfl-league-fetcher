"""Cookie-backed transport for Yahoo's first-party Fantasy v2 API.

The existing Yahoo fetchers already target the public OAuth API and parse its
XML/JSON schema.  Yahoo's web client exposes the same schema through its
authenticated browser API when the request includes its session crumb.  This
adapter changes only the transport, keeping those fetchers and their response
contracts intact.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit
from xml.etree import ElementTree as ET

import requests

from multi_league.utils.yahoo_auth_mode import validate_cookie_jar_payload


OAUTH_API_HOST = "fantasysports.yahooapis.com"
COOKIE_API_HOST = "pub-api-rw.fantasysports.yahoo.com"
COOKIE_API_ROOT = f"https://{COOKIE_API_HOST}/fantasy/v2"
COOKIE_CRUMB_URL = f"{COOKIE_API_ROOT}/crumb"
COOKIE_HOME_URL = "https://football.fantasysports.yahoo.com/"
RECOVERABLE_STATUS_CODES = {401, 403, 999}


class CookieYahooApiSession:
    """A requests-compatible session that redirects OAuth API calls to cookie auth."""

    def __init__(
        self,
        cookie_payload: Mapping[str, Any],
        *,
        raw_session: requests.Session | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        validate_cookie_jar_payload(dict(cookie_payload))
        self._payload = dict(cookie_payload)
        self._session = raw_session or requests.Session()
        self._timeout_seconds = float(timeout_seconds)
        self._crumb: str | None = None
        self._session.headers.update(
            {
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139 Safari/537.36"
                ),
                "Referer": COOKIE_HOME_URL,
                "Origin": "https://football.fantasysports.yahoo.com",
            }
        )
        for cookie in self._payload["cookies"]:
            self._session.cookies.set(
                str(cookie["name"]),
                str(cookie["value"]).strip(),
                domain=str(cookie["domain"]),
                path=str(cookie.get("path") or "/"),
            )

    @property
    def cookies(self):
        return self._session.cookies

    @staticmethod
    def _cookie_api_url(url: str) -> str:
        parsed = urlsplit(url)
        if parsed.hostname != OAUTH_API_HOST:
            return url
        return urlunsplit(("https", COOKIE_API_HOST, parsed.path, parsed.query, parsed.fragment))

    def _renew_crumb(self) -> str:
        # Priming lets Yahoo issue normal redirect/session cookies before the
        # crumb request.  Neither response is persisted or logged.
        self._session.get(COOKIE_HOME_URL, timeout=self._timeout_seconds)
        response = self._session.get(
            COOKIE_CRUMB_URL,
            params={"format": "xml"},
            timeout=self._timeout_seconds,
        )
        if response.status_code != 200:
            raise RuntimeError(f"Yahoo cookie crumb request failed with HTTP {response.status_code}")
        try:
            root = ET.fromstring(response.content)
        except ET.ParseError as exc:
            raise RuntimeError("Yahoo cookie crumb response was not XML") from exc
        crumb = next(
            (node.text for node in root.iter() if node.tag.rsplit("}", 1)[-1] == "crumb" and node.text),
            None,
        )
        if not crumb:
            raise RuntimeError("Yahoo cookie crumb response did not contain a crumb")
        self._crumb = crumb
        return crumb

    def get(self, url: str, **kwargs):
        params = dict(kwargs.pop("params", {}) or {})
        params.setdefault("format", "xml")
        headers = dict(kwargs.pop("headers", {}) or {})

        for attempt in range(2):
            crumb = self._crumb or self._renew_crumb()
            headers["x-crumb"] = crumb
            response = self._session.get(
                self._cookie_api_url(url),
                params=params,
                headers=headers,
                timeout=kwargs.pop("timeout", self._timeout_seconds),
                **kwargs,
            )
            if response.status_code not in RECOVERABLE_STATUS_CODES or attempt:
                return response
            self._crumb = None

        raise AssertionError("unreachable")

    def close(self) -> None:
        self._session.close()


class CookieYahooApiAuth:
    """The small authentication surface required by the read-only Yahoo clients."""

    def __init__(self, cookie_payload: Mapping[str, Any]) -> None:
        self.session = CookieYahooApiSession(cookie_payload)

    def token_is_valid(self) -> bool:
        # Browser cookies are validated by the first API request.  There is no
        # OAuth access token to refresh in this mode.
        return True


def load_cookie_payload(path: str) -> dict[str, Any]:
    """Load a normalized browser-cookie export without exposing its values."""
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if isinstance(raw, list):
        raw = {"format": "json", "cookies": raw}
    if not isinstance(raw, dict):
        raise ValueError("Yahoo cookie file must contain an object or cookie list")
    cookies = raw.get("cookies")
    if isinstance(cookies, list):
        raw = {
            **raw,
            "cookies": [
                {**cookie, "value": str(cookie.get("value", "")).strip()}
                for cookie in cookies
                if isinstance(cookie, dict)
            ],
        }
    validate_cookie_jar_payload(raw)
    return raw
