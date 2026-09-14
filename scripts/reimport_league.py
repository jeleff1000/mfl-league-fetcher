#!/usr/bin/env python3
"""Reimport a single league by database name — exactly like the frontend.

Usage:
    python scripts/reimport_league.py cool_guy_dynasty
    python scripts/reimport_league.py cool_guy_dynasty --mode quick
    python scripts/reimport_league.py cool_guy_dynasty henningbowl_friends_aeaa72  # multiple
    python scripts/reimport_league.py --list  # show all leagues

Looks up league metadata from MotherDuck, builds the same payload the frontend
sends, and dispatches via repository_dispatch to the public worker repository.
"""

import argparse
import base64
import hashlib
import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Load env vars from .env and frontend/.env.local
ROOT = Path(__file__).resolve().parent.parent
for env_file in [ROOT / ".env", ROOT / "frontend" / ".env.local"]:
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))


def get_motherduck_token():
    """Return MotherDuck token, or None on Fly backend."""
    if os.environ.get("DATABASE_BACKEND") == "fly":
        return None
    token = os.environ.get("MOTHERDUCK_TOKEN") or os.environ.get("motherduck_token")
    if not token:
        sys.exit("MOTHERDUCK_TOKEN not set in environment or .env (set DATABASE_BACKEND=fly for Fly)")
    return token


def get_github_token():
    token = os.environ.get("GITHUB_TOKEN")
    if not token:
        sys.exit("GITHUB_TOKEN not set (check frontend/.env.local)")
    return token


def lookup_league(db_name: str, reader) -> dict:
    """Look up league metadata from MotherDuck."""
    rows = reader.query(
        f"SELECT year, league_key, platform, num_teams "
        f"FROM public.league_settings "
        f"WHERE db_name = '{db_name}' ORDER BY year",
        database="___leagues",
    )
    if not rows:
        return {}

    platform = rows[0]["platform"]
    league_ids = {str(r["year"]): r["league_key"] for r in rows}
    latest_year = max(int(y) for y in league_ids)
    latest_league_id = league_ids[str(latest_year)]
    num_teams = rows[-1]["num_teams"]

    # Get league name from the latest year's Sleeper API or from db_name
    league_name = db_name  # fallback

    return {
        "platform": platform,
        "league_ids": league_ids,
        "latest_league_id": latest_league_id,
        "latest_year": latest_year,
        "num_teams": num_teams,
        "league_name": league_name,
        "db_name": db_name,
        "start_year": min(int(y) for y in league_ids),
    }


def dispatch_sleeper(league: dict, mode: str, github_token: str):
    """Dispatch a Sleeper import — same as frontend POST /api/sleeper/import."""
    import urllib.request

    payload = {
        "sleeper_league_id": league["latest_league_id"],
        "league_name": league["league_name"],
        "database_name": league["db_name"],
        "league_ids": league["league_ids"],
        "import_mode": mode,
        "season": league["latest_year"],
        "start_year": league["start_year"],
        "num_teams": league["num_teams"],
    }
    b64 = base64.b64encode(json.dumps(payload).encode()).decode()

    event_type = f"sleeper_{mode}_import"
    body = json.dumps(
        {
            "event_type": event_type,
            "client_payload": {
                "league_data_b64": b64,
                "user_id": hashlib.sha256(
                    f"{league['latest_league_id']}_{league['latest_year']}_{datetime.now().isoformat()}".encode()
                ).hexdigest()[:16],
            },
        }
    ).encode()

    req = urllib.request.Request(
        "https://api.github.com/repos/jeleff1000/mfl-league-fetcher/dispatches",
        data=body,
        headers={
            "Authorization": f"token {github_token}",
            "Content-Type": "application/json",
            "Accept": "application/vnd.github.v3+json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            status = resp.status
    except urllib.error.HTTPError as e:
        sys.exit(f"  GitHub API error {e.code}: {e.read().decode()}")

    if status in (204, 200):
        print(f"  Dispatched: {league['db_name']} ({event_type})")
    else:
        print(f"  Warning: HTTP {status} for {league['db_name']}")


def main():
    parser = argparse.ArgumentParser(description="Reimport league(s) by database name")
    parser.add_argument("db_names", nargs="*", help="Database name(s) to reimport")
    parser.add_argument("--mode", choices=["full", "quick"], default="full", help="Import mode (default: full)")
    parser.add_argument("--list", action="store_true", help="List all leagues")
    parser.add_argument("--dry-run", action="store_true", help="Show payload without dispatching")
    args = parser.parse_args()

    from multi_league.core.db_reader import get_reader

    reader = get_reader()

    if args.list:
        rows = reader.query(
            "SELECT db_name, platform, MIN(year) as first, MAX(year) as last, MAX(num_teams) as teams "
            "FROM public.league_settings "
            "GROUP BY db_name, platform ORDER BY platform, db_name",
            database="___leagues",
        )
        for r in rows:
            print(f"  {r['platform']:8s} {r['db_name']:50s} {r['first']}-{r['last']}  ({r['teams']} teams)")
        print(f"\n  {len(rows)} leagues total")
        return

    if not args.db_names:
        parser.error("Provide at least one db_name or --list")

    github_token = get_github_token()
    dispatched = 0

    for db_name in args.db_names:
        league = lookup_league(db_name, reader)
        if not league:
            print(f"  {db_name}: NOT FOUND in MotherDuck")
            continue

        platform = league["platform"]
        years = f"{league['start_year']}-{league['latest_year']}"
        print(f"  {db_name} ({platform}, {years}, {league['num_teams']} teams)")

        if args.dry_run:
            print(f"    Payload: {json.dumps(league, indent=2)}")
            continue

        if platform == "sleeper":
            dispatch_sleeper(league, args.mode, github_token)
            dispatched += 1
        else:
            print(f"    Skipping: {platform} reimport not yet supported (use import_dispatch.py)")

    if dispatched:
        print(f"\n  Dispatched {dispatched} import(s)")
        print("  Monitor: https://github.com/jeleff1000/mfl-league-fetcher/actions")


if __name__ == "__main__":
    main()
