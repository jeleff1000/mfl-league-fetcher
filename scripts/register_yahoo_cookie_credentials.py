"""Register a local Yahoo browser-cookie export as encrypted Fly credentials."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ROOT = PROJECT_ROOT / "fantasy_football_data_scripts"
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
try:
    from dotenv import load_dotenv

    load_dotenv(PROJECT_ROOT / ".env")
except ImportError:
    pass


def load_cookie_payload(path: Path) -> dict:
    raw = path.expanduser().resolve().read_text(encoding="utf-8")
    stripped = raw.lstrip()
    if stripped.startswith("{") or stripped.startswith("["):
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            # Some browser exporters line-wrap a long cookie value inside the
            # JSON string. Cookie values cannot contain raw newlines, so retry
            # after removing only line breaks from the export text.
            payload = json.loads(raw.replace("\r", "").replace("\n", ""))
        cookies = payload.get("cookies", payload) if isinstance(payload, dict) else payload
        if isinstance(cookies, dict):
            cookies = [
                {"domain": ".yahoo.com", "name": str(name), "value": str(value)}
                for name, value in cookies.items()
            ]
        if not isinstance(cookies, list):
            raise ValueError("JSON cookie export must contain a cookies list")
        return {"format": "json", "cookies": cookies}

    def looks_like_domain(value: str) -> bool:
        candidate = value.strip().lower().lstrip(".")
        return "." in candidate and " " not in candidate and "/" not in candidate

    cookies = []
    for line in raw.splitlines():
        if not line or line.startswith("#"):
            continue
        fields = line.split("\t")
        if len(fields) < 7:
            continue
        # Accept both common browser export layouts.
        if looks_like_domain(fields[2]):
            domain, name, value = fields[2], fields[0], fields[1]
        elif looks_like_domain(fields[0]):
            domain, name, value = fields[0], fields[5], fields[6]
        else:
            continue
        cookies.append({"domain": domain, "name": name, "value": value})
    if not cookies:
        raise ValueError("cookie export contains no usable entries")
    return {"format": "netscape", "cookies": cookies}


def main() -> int:
    parser = argparse.ArgumentParser(description="Encrypt and register a Yahoo cookie export in Fly")
    parser.add_argument("--cookie-jar", required=True, type=Path)
    parser.add_argument("--league-id", required=True)
    parser.add_argument("--league-name", required=True)
    parser.add_argument("--database-name", required=True)
    parser.add_argument("--captured-at")
    parser.add_argument("--expires-at")
    parser.add_argument("--status", default="active", choices=["active", "disabled"])
    args = parser.parse_args()

    if os.environ.get("DATABASE_BACKEND", "fly").lower() != "fly":
        raise RuntimeError("Cookie credential registration requires DATABASE_BACKEND=fly")
    payload = load_cookie_payload(args.cookie_jar)
    from multi_league.utils.credential_store import store_yahoo_cookie_credentials

    ok = store_yahoo_cookie_credentials(
        league_id=args.league_id,
        league_name=args.league_name,
        cookie_payload=payload,
        database_name=args.database_name,
        captured_at=args.captured_at,
        expires_at=args.expires_at,
        status=args.status,
    )
    if not ok:
        raise RuntimeError("Yahoo cookie credential registration failed")
    print(f"Registered encrypted Yahoo cookie credentials for {args.database_name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
