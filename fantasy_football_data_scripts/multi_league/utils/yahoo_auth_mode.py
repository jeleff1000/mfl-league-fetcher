"""Yahoo authentication-mode and browser-session credential primitives.

This module deliberately contains no Yahoo network calls.  It provides the
small contract shared by the OAuth and browser-cookie import dispatchers:
mode selection is explicit, cookie payloads are validated before use, and
validation results never contain cookie values.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class YahooAuthMode(StrEnum):
    OAUTH = "oauth"
    COOKIE = "cookie"


def resolve_yahoo_auth_mode(explicit: str | None, stored: str | None) -> YahooAuthMode:
    """Resolve the active Yahoo auth mode, defaulting safely to OAuth."""
    candidate = explicit if explicit is not None and str(explicit).strip() else stored
    normalized = str(candidate or YahooAuthMode.OAUTH).strip().lower()
    try:
        return YahooAuthMode(normalized)
    except ValueError as exc:
        raise ValueError(f"Unsupported Yahoo auth mode: {candidate!r}; expected oauth or cookie") from exc


def validate_cookie_jar_payload(payload: Any) -> dict[str, Any]:
    """Validate a normalized cookie payload and return redacted metadata only.

    The persisted encrypted value may contain arbitrary browser-cookie fields,
    but callers should pass the normalized object here before logging or
    writing import manifests.
    """
    if not isinstance(payload, dict):
        raise ValueError("Yahoo cookie payload must be an object")
    cookies = payload.get("cookies")
    if not isinstance(cookies, list) or not cookies:
        raise ValueError("Yahoo cookie payload must contain a non-empty cookies list")

    for index, cookie in enumerate(cookies):
        if not isinstance(cookie, dict):
            raise ValueError(f"Yahoo cookie entry {index} must be an object")
        for field in ("domain", "name", "value"):
            if not str(cookie.get(field) or "").strip():
                raise ValueError(f"Yahoo cookie entry {index} is missing {field}")

    return {
        "format": str(payload.get("format") or "json").strip().lower(),
        "cookie_count": len(cookies),
        "domains": sorted({str(cookie["domain"]).strip().lower() for cookie in cookies}),
    }
