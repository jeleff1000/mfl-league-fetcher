#!/usr/bin/env python3
"""
Yahoo Import Dispatch CLI

Mirrors the frontend Yahoo OAuth flow:
1. Pull all Yahoo credentials from MotherDuck
2. Refresh the OAuth token
3. Call Yahoo API to discover all user's leagues
4. Follow renewal chains to build year→league_key mappings
5. Present leagues for selection
6. Dispatch GH Actions full import for selected leagues

Usage:
    python scripts/yahoo_import_dispatch.py
    python scripts/yahoo_import_dispatch.py --db yk_jff_vilde_hatzooleh_league   # single league
    python scripts/yahoo_import_dispatch.py --list                                # just list, don't import
    python scripts/yahoo_import_dispatch.py --all                                 # import all Yahoo leagues
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import sys
import time
from pathlib import Path

# Add project root to path
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
FFS_DIR = PROJECT_ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(FFS_DIR))

import duckdb
import requests

from multi_league.utils.credential_store import decrypt_token, get_encryption_key

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
YAHOO_API_BASE = "https://fantasysports.yahooapis.com"
YAHOO_TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
GH_WORKERS_REPO = "jeleff1000/league-history-workers"
YAHOO_FULL_IMPORT_WORKFLOW_ID = 252727129

# ---------------------------------------------------------------------------
# Load env
# ---------------------------------------------------------------------------


def _load_dotenv():
    """Load .env from project root if python-dotenv available, else manual."""
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())

    # Also load frontend/.env.local for GITHUB_TOKEN
    fe_env = PROJECT_ROOT / "frontend" / ".env.local"
    if fe_env.exists():
        for line in fe_env.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip())


# ---------------------------------------------------------------------------
# Yahoo OAuth helpers
# ---------------------------------------------------------------------------


def refresh_yahoo_token(refresh_token: str, client_id: str, client_secret: str) -> dict:
    """Refresh a Yahoo OAuth token and return new tokens."""
    resp = requests.post(
        YAHOO_TOKEN_URL,
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def yahoo_api_get(url: str, access_token: str) -> dict:
    """Make an authenticated GET to the Yahoo Fantasy API (JSON)."""
    resp = requests.get(
        url,
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    if resp.status_code == 401:
        raise PermissionError("Yahoo token expired or invalid")
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# League discovery (mirrors frontend /api/yahoo/leagues)
# ---------------------------------------------------------------------------


def discover_user_leagues(access_token: str) -> list[dict]:
    """Discover all NFL fantasy leagues for the authenticated user.

    Returns list of dicts with: league_key, league_id, name, season, num_teams, game_key
    """
    # Phase 1: get all NFL games (seasons)
    games_url = f"{YAHOO_API_BASE}/fantasy/v2/users;use_login=1/games?format=json"
    games_data = yahoo_api_get(games_url, access_token)

    games = []
    try:
        fc = games_data["fantasy_content"]
        users = fc["users"]
        user = users["0"]["user"]
        games_obj = user[1]["games"]
        i = 0
        while str(i) in games_obj:
            wrapper = games_obj[str(i)]
            game = wrapper.get("game")
            if isinstance(game, list):
                game = game[0]
            if game and game.get("code") == "nfl":
                games.append(
                    {
                        "game_key": str(game["game_key"]),
                        "season": str(game.get("season", "")),
                    }
                )
            i += 1
    except (KeyError, TypeError, IndexError):
        pass

    if not games:
        print("  No NFL games found for this user.")
        return []

    # Phase 2: get leagues per game
    leagues = []
    for game in games:
        gk = game["game_key"]
        url = f"{YAHOO_API_BASE}/fantasy/v2/users;use_login=1/games;game_keys={gk}/leagues?format=json"
        try:
            data = yahoo_api_get(url, access_token)
            fc = data["fantasy_content"]
            users = fc["users"]
            user = users["0"]["user"]
            games_section = user[1]["games"]

            j = 0
            while str(j) in games_section:
                game_wrapper = games_section[str(j)]
                game_arr = game_wrapper.get("game", [])
                if isinstance(game_arr, list) and len(game_arr) > 1:
                    leagues_obj = game_arr[1].get("leagues", {})
                elif isinstance(game_arr, dict):
                    leagues_obj = game_arr.get("leagues", {})
                else:
                    j += 1
                    continue

                k = 0
                while str(k) in leagues_obj:
                    league_wrapper = leagues_obj[str(k)]
                    league = league_wrapper.get("league")
                    if isinstance(league, list):
                        league = league[0]
                    if league:
                        leagues.append(
                            {
                                "league_key": str(league.get("league_key", "")),
                                "league_id": str(league.get("league_id", "")),
                                "name": str(league.get("name", "Unknown")),
                                "season": str(league.get("season", game["season"])),
                                "num_teams": int(league.get("num_teams", 0)),
                                "game_key": gk,
                            }
                        )
                    k += 1
                j += 1
        except Exception as e:
            print(f"  Warning: Could not fetch leagues for game {gk}: {e}")
            continue

    return leagues


# ---------------------------------------------------------------------------
# Renewal chain discovery (mirrors frontend /api/yahoo/league-chains)
# ---------------------------------------------------------------------------


def discover_renewal_chain(league_key: str, access_token: str) -> dict[str, str]:
    """Walk Yahoo renew/renewed links to build year→league_key mapping.

    Returns dict like {"2015": "359.l.12345", "2016": "380.l.23456", ...}
    """
    visited = set()
    league_ids = {}
    queue = [league_key]

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)

        try:
            url = f"{YAHOO_API_BASE}/fantasy/v2/league/{current}/settings?format=json"
            data = yahoo_api_get(url, access_token)

            fc = data.get("fantasy_content", {})
            league = fc.get("league", [])
            if isinstance(league, list) and len(league) > 0:
                meta = league[0] if isinstance(league[0], dict) else {}
            elif isinstance(league, dict):
                meta = league
            else:
                continue

            season = str(meta.get("season", ""))
            if season:
                league_ids[season] = current

            # Follow renewal links
            renew = meta.get("renew", "")
            renewed = meta.get("renewed", "")

            for link in [renew, renewed]:
                if link:
                    parts = str(link).split("_")
                    if len(parts) >= 2:
                        linked_key = f"{parts[0]}.l.{'_'.join(parts[1:])}"
                        if linked_key not in visited:
                            queue.append(linked_key)
        except Exception as e:
            print(f"  Warning: Could not follow chain for {current}: {e}")
            continue

    return dict(sorted(league_ids.items(), key=lambda x: int(x[0])))


# ---------------------------------------------------------------------------
# Group leagues by chain
# ---------------------------------------------------------------------------


def group_leagues_by_chain(leagues: list[dict], access_token: str) -> list[dict]:
    """Group discovered leagues into chains (same league across years).

    Returns list of chain groups, each with:
        name, num_teams, seasons (list of {season, league_key}), league_ids (year→key)
    """
    # Group by league name first (fast heuristic)
    by_name: dict[str, list[dict]] = {}
    for lg in leagues:
        by_name.setdefault(lg["name"], []).append(lg)

    chains = []
    processed_keys = set()

    for name, group in sorted(by_name.items()):
        # Pick the most recent season's league_key as chain anchor
        group.sort(key=lambda x: int(x["season"]), reverse=True)
        anchor = group[0]

        if anchor["league_key"] in processed_keys:
            continue

        print(f"  Discovering history for: {name}...", end=" ", flush=True)
        league_ids = discover_renewal_chain(anchor["league_key"], access_token)
        print(f"{len(league_ids)} years")

        for key in league_ids.values():
            processed_keys.add(key)

        seasons = [{"season": yr, "league_key": key} for yr, key in league_ids.items()]
        years = sorted(int(y) for y in league_ids.keys())

        chains.append(
            {
                "name": name,
                "num_teams": anchor["num_teams"],
                "seasons": seasons,
                "league_ids": league_ids,
                "start_year": min(years) if years else int(anchor["season"]),
                "end_year": max(years) if years else int(anchor["season"]),
                "latest_key": league_ids.get(str(max(years))) if years else anchor["league_key"],
            }
        )

    return chains


# ---------------------------------------------------------------------------
# GH Actions dispatch
# ---------------------------------------------------------------------------


def dispatch_yahoo_import(
    chain: dict,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    database_name: str | None = None,
    github_token: str | None = None,
    dry_run: bool = False,
) -> bool:
    """Dispatch a Yahoo full import via GH Actions, just like the frontend does."""
    gh_token = github_token or os.environ.get("GITHUB_TOKEN")
    if not gh_token:
        print("  ERROR: No GITHUB_TOKEN available")
        return False

    # Build payload matching frontend structure
    league_data = {
        "league_id": chain["latest_key"],
        "league_name": chain["name"],
        "season": chain["end_year"],
        "start_year": chain["start_year"],
        "league_ids": chain["league_ids"],
        "num_teams": chain["num_teams"],
        "oauth_token": {
            "refresh_token": refresh_token,
            "consumer_key": client_id,
            "consumer_secret": client_secret,
        },
    }
    if database_name:
        league_data["database_name"] = database_name

    b64 = base64.b64encode(json.dumps(league_data).encode()).decode()
    user_id = f"cli-{chain['name'].lower().replace(' ', '-')[:20]}-{int(time.time()) % 100000}"

    if dry_run:
        print(f"  [DRY RUN] Would dispatch: {chain['name']} ({chain['start_year']}-{chain['end_year']})")
        print(f"            league_ids: {json.dumps(chain['league_ids'], indent=2)}")
        return True

    resp = requests.post(
        f"https://api.github.com/repos/{GH_WORKERS_REPO}/actions/workflows/{YAHOO_FULL_IMPORT_WORKFLOW_ID}/dispatches",
        headers={
            "Authorization": f"token {gh_token}",
            "Accept": "application/vnd.github.v3+json",
        },
        json={"ref": "main", "inputs": {"league_data_b64": b64, "user_id": user_id}},
        timeout=30,
    )

    if resp.status_code == 204:
        print(
            f"  Dispatched: {chain['name']} ({chain['start_year']}-{chain['end_year']}, {len(chain['league_ids'])} years)"
        )
        return True
    else:
        print(f"  FAILED ({resp.status_code}): {resp.text[:200]}")
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="Yahoo Import Dispatch CLI — discover leagues and dispatch GH Actions imports",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--db", type=str, help="Import a specific database (by name in MotherDuck)")
    parser.add_argument("--list", action="store_true", help="List leagues only, don't dispatch imports")
    parser.add_argument("--all", action="store_true", help="Import all discovered leagues (no prompt)")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be dispatched without dispatching")
    args = parser.parse_args()

    _load_dotenv()

    # Required env vars
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    encryption_key = get_encryption_key()
    client_id = os.environ.get("YAHOO_CLIENT_ID")
    client_secret = os.environ.get("YAHOO_CLIENT_SECRET")

    if backend != "fly" and not md_token:
        print("ERROR: MOTHERDUCK_TOKEN not set (check .env)")
        sys.exit(1)
    if not encryption_key:
        print("ERROR: CREDENTIAL_ENCRYPTION_KEY / CREDENTIAL_ENCRYPTION_KEY_NEW not set (check .env)")
        sys.exit(1)
    if not client_id or not client_secret:
        print("ERROR: YAHOO_CLIENT_ID / YAHOO_CLIENT_SECRET not set (check .env)")
        sys.exit(1)

    # ---- Pull Yahoo credentials from database backend ----
    print("Connecting to database backend...")
    from multi_league.core.db_reader import get_reader

    reader = get_reader()

    if args.db:
        # Single league mode
        creds = reader.query(
            f"SELECT database_name, league_id, league_name, encrypted_refresh_token "
            f"FROM main.league_credentials WHERE database_name = '{args.db}'",
            database="___ops",
        )
        if not creds:
            print(f"ERROR: {args.db} not found in ___ops.main.league_credentials")
            sys.exit(1)
    else:
        # All Yahoo leagues
        creds = reader.query(
            "SELECT database_name, league_id, league_name, encrypted_refresh_token "
            "FROM main.league_credentials "
            "WHERE encrypted_refresh_token IS NOT NULL "
            "ORDER BY league_name",
            database="___ops",
        )

    if not creds:
        print("No Yahoo leagues with credentials found.")
        sys.exit(0)

    # ---- Group by unique refresh token (same user = same token) ----
    # Each user has one refresh token across all their leagues
    token_to_creds: dict[str, list] = {}
    for cr in creds:
        db_name, league_id, league_name, encrypted_token = (
            cr["database_name"],
            cr["league_id"],
            cr["league_name"],
            cr["encrypted_refresh_token"],
        )
        try:
            refresh_token = decrypt_token(encrypted_token, encryption_key)
        except Exception as e:
            print(f"  Warning: Could not decrypt token for {db_name}: {e}")
            continue
        token_to_creds.setdefault(refresh_token, []).append((db_name, league_id, league_name))

    print(f"Found {len(creds)} Yahoo league(s) across {len(token_to_creds)} user(s)\n")

    # ---- Process each user's token ----
    all_dispatched = []

    for refresh_token, user_leagues in token_to_creds.items():
        user_db_names = {lg[0] for lg in user_leagues}
        user_league_names = [lg[2] for lg in user_leagues]
        print(f"{'='*60}")
        print(f"User leagues: {', '.join(user_league_names[:5])}{'...' if len(user_league_names) > 5 else ''}")
        print(f"{'='*60}")

        # Refresh the token
        print("  Refreshing Yahoo token...", end=" ", flush=True)
        try:
            tokens = refresh_yahoo_token(refresh_token, client_id, client_secret)
            access_token = tokens["access_token"]
            new_refresh = tokens.get("refresh_token", refresh_token)
            print("OK")
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        # Discover all leagues for this user
        print("  Discovering leagues from Yahoo API...")
        all_leagues = discover_user_leagues(access_token)
        print(f"  Found {len(all_leagues)} league-seasons")

        if not all_leagues:
            continue

        # Build renewal chains
        print("\n  Building renewal chains...")
        chains = group_leagues_by_chain(all_leagues, access_token)
        print()

        # Display chains
        for i, chain in enumerate(chains):
            years = (
                f"{chain['start_year']}-{chain['end_year']}"
                if chain["start_year"] != chain["end_year"]
                else str(chain["start_year"])
            )
            # Check if we already have credentials for this league
            db_match = None
            for db_name, lid, lname in user_leagues:
                if lname == chain["name"] or lid == chain["latest_key"]:
                    db_match = db_name
                    break
            marker = f" [db: {db_match}]" if db_match else " [NEW]"
            print(
                f"  [{i+1}] {chain['name']:40s} {years:12s} ({len(chain['league_ids'])} yrs, {chain['num_teams']} teams){marker}"
            )

        if args.list:
            continue

        # Select leagues
        if args.all:
            selected = list(range(len(chains)))
        elif args.db:
            # In --db mode, only import the matching chain
            selected = []
            for i, chain in enumerate(chains):
                for db_name, lid, lname in user_leagues:
                    if db_name == args.db and (lname == chain["name"] or lid == chain["latest_key"]):
                        selected.append(i)
                        break
            if not selected:
                # Try matching by league name similarity
                for i, chain in enumerate(chains):
                    db_clean = args.db.replace("_", " ").lower()
                    if chain["name"].lower() in db_clean or db_clean in chain["name"].lower():
                        selected.append(i)
                        break
        else:
            print("\n  Enter league numbers to import (comma-separated), 'all', or 'skip':")
            choice = input("  > ").strip().lower()
            if choice in ("skip", "s", ""):
                continue
            elif choice == "all":
                selected = list(range(len(chains)))
            else:
                try:
                    selected = [int(x.strip()) - 1 for x in choice.split(",")]
                except ValueError:
                    print("  Invalid input, skipping.")
                    continue

        # Dispatch selected leagues
        print()
        for idx in selected:
            if idx < 0 or idx >= len(chains):
                continue
            chain = chains[idx]

            # Resolve database name
            db_name = None
            for d, lid, lname in user_leagues:
                if lname == chain["name"] or lid == chain["latest_key"]:
                    db_name = d
                    break

            ok = dispatch_yahoo_import(
                chain=chain,
                refresh_token=new_refresh,
                client_id=client_id,
                client_secret=client_secret,
                database_name=db_name,
                dry_run=args.dry_run,
            )
            if ok:
                all_dispatched.append(chain["name"])

    # Summary
    if all_dispatched:
        print(f"\n{'='*60}")
        print(f"Dispatched {len(all_dispatched)} import(s):")
        for name in all_dispatched:
            print(f"  - {name}")
        print(f"\nMonitor at: https://github.com/{GH_WORKERS_REPO}/actions")


if __name__ == "__main__":
    main()
