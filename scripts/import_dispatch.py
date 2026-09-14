#!/usr/bin/env python3
"""
Unified Import Dispatch CLI — Sleeper / ESPN / Yahoo

Mirrors the frontend import flow for all 3 platforms:
- Discover leagues from platform APIs
- Follow history/renewal chains to build year→league_id mappings
- Present leagues for selection (combine multiple into one import)
- Optionally merge cross-platform history from an existing DB
- Dispatch GH Actions full import

Usage:
    # Sleeper — by username (no auth needed)
    python scripts/import_dispatch.py sleeper --username jeleff1000

    # ESPN — by SWID (+ espn_s2 for private leagues)
    python scripts/import_dispatch.py espn --swid "{GUID}" --s2 "cookie_value"

    # Yahoo — by database name (pulls credentials from MotherDuck)
    python scripts/import_dispatch.py yahoo --db yk_jff_vilde_hatzooleh_league
    python scripts/import_dispatch.py yahoo                # all Yahoo users

    # Common flags
    --list          List leagues only, don't dispatch
    --dry-run       Show payload without dispatching
    --all           Import all discovered leagues
    --merge-from DB Merge historical data from an existing league DB
    --merge-years 2012-2020 Restrict source years copied by the worker-side merge
    --manager-map "Old Name=New Name" Map source managers to target managers
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
FFS_DIR = PROJECT_ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(FFS_DIR))

import requests

# Fix Windows console encoding for emoji in league names
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SLEEPER_API = "https://api.sleeper.app/v1"
ESPN_FAN_API = "https://fan.api.espn.com/apis/v2"
ESPN_LEAGUE_API = "https://lm-api-reads.fantasy.espn.com/apis/v3"
YAHOO_API = "https://fantasysports.yahooapis.com"
YAHOO_TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"

GH_WORKERS_REPO = "jeleff1000/mfl-league-fetcher"
WORKFLOW_IDS = {
    "sleeper_full": 244876060,
    "yahoo_full": 252727129,
    "espn_full": 244876052,
}

CURRENT_YEAR = time.localtime().tm_year


def _load_dotenv():
    """Load .env and frontend/.env.local into os.environ."""
    for env_file in [PROJECT_ROOT / ".env", PROJECT_ROOT / "frontend" / ".env.local"]:
        if env_file.exists():
            for line in env_file.read_text().splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    k, _, v = line.partition("=")
                    os.environ.setdefault(k.strip(), v.strip())


def _make_user_id(name: str) -> str:
    raw = f"{name}_{int(time.time())}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def parse_merge_years(raw: str | None) -> list[int]:
    """Parse comma-separated years and inclusive ranges for merge_source."""
    if not raw:
        return []

    years: set[int] = set()
    for chunk in raw.split(","):
        part = chunk.strip()
        if not part:
            continue
        if "-" in part:
            start_raw, _, end_raw = part.partition("-")
            start = int(start_raw.strip())
            end = int(end_raw.strip())
            if end < start:
                start, end = end, start
            years.update(range(start, end + 1))
        else:
            years.add(int(part))

    parsed = sorted(years)
    for year in parsed:
        if year < 1900 or year > 2100:
            raise ValueError(f"Invalid merge year: {year}")
    return parsed


def parse_manager_maps(raw_maps: list[str] | None) -> dict[str, str]:
    """Parse repeated FROM=TO manager mappings."""
    mappings: dict[str, str] = {}
    for raw in raw_maps or []:
        if "=" not in raw:
            raise ValueError(f"Invalid --manager-map {raw!r}; expected FROM=TO")
        source, _, target = raw.partition("=")
        source = source.strip()
        target = target.strip()
        if source and target and source != target:
            mappings[source] = target
    return mappings


def build_merge_source_from_args(args) -> dict | None:
    source_db = getattr(args, "merge_from", None)
    raw_years = getattr(args, "merge_years", None)
    raw_maps = getattr(args, "manager_maps", None)

    if not source_db:
        if raw_years or raw_maps:
            raise ValueError("--merge-years and --manager-map require --merge-from")
        return None
    source_db = source_db.strip()
    if not source_db:
        raise ValueError("--merge-from must not be empty")

    merge_source: dict = {
        "source_db": source_db,
        "manager_mapping": parse_manager_maps(raw_maps),
    }
    years = parse_merge_years(raw_years)
    if years:
        merge_source["merge_years"] = years
    return merge_source


def attach_merge_source(league_data: dict, merge_source: dict | None) -> dict:
    """Attach an in-system historical source copied by import/merge workers."""
    if not merge_source:
        return league_data
    league_data["merge_source"] = merge_source
    league_data["merge_sources"] = [merge_source]
    league_data["has_external_data"] = True
    return league_data


# ============================= GITHUB DISPATCH =============================


def dispatch_workflow(platform: str, league_data: dict, dry_run: bool = False) -> bool:
    """Dispatch a GH Actions full import workflow."""
    gh_token = os.environ.get("GITHUB_TOKEN")
    if not gh_token:
        print("  ERROR: No GITHUB_TOKEN (check frontend/.env.local)")
        return False

    workflow_id = WORKFLOW_IDS[f"{platform}_full"]
    b64 = base64.b64encode(json.dumps(league_data).encode()).decode()
    user_id = _make_user_id(league_data.get("league_name", "cli"))

    name = league_data.get("league_name", "?")
    start = league_data.get("start_year", "?")
    end = league_data.get("season", league_data.get("end_year", "?"))
    n_years = len(league_data.get("league_ids", {})) or 1

    if dry_run:
        print(f"  [DRY RUN] Would dispatch: {name} ({start}-{end}, {n_years} yrs)")
        if league_data.get("league_ids"):
            print(f"            league_ids: {json.dumps(league_data['league_ids'], indent=2)}")
        if league_data.get("merge_source"):
            print(f"            merge_source: {json.dumps(league_data['merge_source'], indent=2)}")
        return True

    resp = requests.post(
        f"https://api.github.com/repos/{GH_WORKERS_REPO}/actions/workflows/{workflow_id}/dispatches",
        headers={
            "Authorization": f"token {gh_token}",
            "Accept": "application/vnd.github.v3+json",
        },
        json={"ref": "main", "inputs": {"league_data_b64": b64, "user_id": user_id}},
        timeout=30,
    )
    if resp.status_code == 204:
        print(f"  Dispatched: {name} ({start}-{end}, {n_years} yrs)")
        return True
    else:
        print(f"  FAILED ({resp.status_code}): {resp.text[:200]}")
        return False


# ============================= SLEEPER =====================================


def sleeper_get(path: str) -> dict | list | None:
    resp = requests.get(f"{SLEEPER_API}{path}", timeout=15)
    if resp.status_code == 200:
        return resp.json()
    return None


def sleeper_discover(username: str) -> list[dict]:
    """Discover all Sleeper NFL leagues for a username."""
    user = sleeper_get(f"/user/{username}")
    if not user:
        print(f"  ERROR: Sleeper user '{username}' not found")
        return []

    user_id = user["user_id"]
    display_name = user.get("display_name", username)
    print(f"  User: {display_name} ({user_id})")

    # Fetch leagues for each year (Sleeper data starts 2017)
    all_leagues = []
    for year in range(2017, CURRENT_YEAR + 1):
        leagues = sleeper_get(f"/user/{user_id}/leagues/nfl/{year}")
        if leagues:
            for lg in leagues:
                if lg.get("sport") == "nfl":
                    all_leagues.append(
                        {
                            "league_id": lg["league_id"],
                            "name": lg.get("name", "Unknown"),
                            "season": int(lg.get("season", year)),
                            "num_teams": lg.get("total_rosters", 0),
                            "previous_league_id": lg.get("previous_league_id"),
                        }
                    )

    print(f"  Found {len(all_leagues)} league-seasons")
    return all_leagues


def sleeper_build_chains(leagues: list[dict]) -> list[dict]:
    """Group Sleeper leagues into chains via previous_league_id."""
    by_id = {lg["league_id"]: lg for lg in leagues}

    # Build forward links
    children: dict[str, str] = {}
    for lg in leagues:
        prev = lg.get("previous_league_id")
        if prev:
            children[prev] = lg["league_id"]

    # Find chain roots (no parent pointing to them via previous_league_id)
    all_prev_ids = {lg["previous_league_id"] for lg in leagues if lg.get("previous_league_id")}
    # A root is a league whose ID is not a previous_league_id AND whose previous_league_id is not in our set
    roots = []
    visited = set()

    for lg in leagues:
        lid = lg["league_id"]
        if lid in visited:
            continue
        # Walk backwards to find root
        current = lid
        while True:
            parent = by_id.get(current, {}).get("previous_league_id")
            if parent and parent in by_id:
                current = parent
            else:
                break

        # Walk forward from root to build chain
        chain_leagues = []
        cursor = current
        while cursor and cursor in by_id:
            chain_leagues.append(by_id[cursor])
            visited.add(cursor)
            cursor = children.get(cursor)

        if chain_leagues:
            chain_leagues.sort(key=lambda x: x["season"])
            league_ids = {str(lg["season"]): lg["league_id"] for lg in chain_leagues}
            latest = chain_leagues[-1]
            roots.append(
                {
                    "name": latest["name"],
                    "num_teams": latest["num_teams"],
                    "seasons": [{"season": str(lg["season"]), "league_key": lg["league_id"]} for lg in chain_leagues],
                    "league_ids": league_ids,
                    "start_year": chain_leagues[0]["season"],
                    "end_year": latest["season"],
                    "latest_id": latest["league_id"],
                }
            )

    roots.sort(key=lambda x: x["name"])
    return roots


def sleeper_dispatch(
    chain: dict,
    database_name: str | None = None,
    dry_run: bool = False,
    merge_source: dict | None = None,
) -> bool:
    league_data = {
        "sleeper_league_id": chain["latest_id"],
        "league_name": chain["name"],
        "database_name": database_name or "",
        "season": chain["end_year"],
        "start_year": chain["start_year"],
        "num_teams": chain["num_teams"],
        "league_ids": chain["league_ids"],
        "import_mode": "full",
    }
    attach_merge_source(league_data, merge_source)
    return dispatch_workflow("sleeper", league_data, dry_run=dry_run)


# ============================= ESPN ========================================


def espn_discover(swid: str) -> list[dict]:
    """Discover all ESPN FFL leagues for a SWID."""
    # Normalize SWID
    normalized = swid.strip("{}")
    url = f"{ESPN_FAN_API}/fans/{{{normalized}}}?displayEvents=true&displayNow=true&displayRecs=true"

    resp = requests.get(url, timeout=15)
    if resp.status_code != 200:
        print(f"  ERROR: ESPN fan API returned {resp.status_code}")
        return []

    data = resp.json()
    leagues = []

    for pref in data.get("preferences", []):
        meta = pref.get("metaData", {})
        entry = meta.get("entry", {})
        groups = entry.get("groups", [])
        for group in groups:
            if entry.get("abbrev") == "FFL":
                league_id = group.get("groupId")
                league_name = group.get("groupName", f"ESPN League {league_id}")
                if league_id:
                    leagues.append(
                        {
                            "league_id": int(league_id),
                            "name": league_name,
                            "is_commissioner": group.get("isCommissioner", False),
                        }
                    )

    # Deduplicate
    seen = set()
    unique = []
    for lg in leagues:
        if lg["league_id"] not in seen:
            seen.add(lg["league_id"])
            unique.append(lg)

    print(f"  Found {len(unique)} ESPN leagues")
    return unique


def espn_discover_history(league_id: int, espn_s2: str | None = None, swid: str | None = None) -> dict[str, int]:
    """Probe ESPN API to find all available years for a league."""
    cookies = {}
    if espn_s2:
        cookies["espn_s2"] = espn_s2
    if swid:
        cookies["SWID"] = swid if swid.startswith("{") else f"{{{swid}}}"

    league_ids = {}
    # ESPN data goes back to ~2004, probe in batches
    for year in range(CURRENT_YEAR, 2003, -1):
        try:
            if year >= 2018:
                url = f"{ESPN_LEAGUE_API}/games/ffl/seasons/{year}/segments/0/leagues/{league_id}?view=mSettings"
            else:
                url = f"{ESPN_LEAGUE_API}/games/ffl/leagueHistory/{league_id}?seasonId={year}&view=mSettings"

            resp = requests.get(url, cookies=cookies if cookies else None, timeout=10)
            if resp.status_code == 200:
                league_ids[str(year)] = league_id
            elif resp.status_code in (401, 403, 404):
                # Private or doesn't exist for this year
                if resp.status_code == 404:
                    break  # No more history
                continue
        except Exception:
            continue

    return league_ids


def espn_build_chains(leagues: list[dict], espn_s2: str | None = None, swid: str | None = None) -> list[dict]:
    """Build chains for ESPN leagues by probing year availability."""
    chains = []
    for lg in leagues:
        print(f"  Discovering history for: {lg['name']}...", end=" ", flush=True)
        league_ids = espn_discover_history(lg["league_id"], espn_s2, swid)
        years = sorted(int(y) for y in league_ids.keys())
        print(f"{len(years)} years")

        if not years:
            continue

        # ESPN uses the same league_id across years
        chains.append(
            {
                "name": lg["name"],
                "num_teams": 0,  # Will be populated by importer
                "seasons": [{"season": str(y), "league_key": str(lg["league_id"])} for y in years],
                "league_ids": league_ids,
                "start_year": min(years),
                "end_year": max(years),
                "league_id": lg["league_id"],
            }
        )

    return chains


def espn_dispatch(
    chain: dict,
    espn_s2: str | None = None,
    swid: str | None = None,
    database_name: str | None = None,
    dry_run: bool = False,
    merge_source: dict | None = None,
) -> bool:
    league_data: dict = {
        "espn_league_id": chain["league_id"],
        "league_name": chain["name"],
        "database_name": database_name or "",
        "season": chain["end_year"],
        "start_year": chain["start_year"],
        "import_mode": "full",
    }
    if espn_s2:
        league_data["espn_s2"] = espn_s2
    if swid:
        league_data["swid"] = swid if swid.startswith("{") else f"{{{swid}}}"
    attach_merge_source(league_data, merge_source)
    return dispatch_workflow("espn", league_data, dry_run=dry_run)


# ============================= YAHOO =======================================
# (Reuses logic from yahoo_import_dispatch.py)


def yahoo_refresh_token(refresh_token: str, client_id: str, client_secret: str) -> dict:
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
    resp = requests.get(url, headers={"Authorization": f"Bearer {access_token}"}, timeout=30)
    resp.raise_for_status()
    return resp.json()


def yahoo_discover_leagues(access_token: str) -> list[dict]:
    """Discover all Yahoo NFL leagues for authenticated user."""
    games_url = f"{YAHOO_API}/fantasy/v2/users;use_login=1/games?format=json"
    data = yahoo_api_get(games_url, access_token)

    games = []
    try:
        fc = data["fantasy_content"]
        user = fc["users"]["0"]["user"]
        games_obj = user[1]["games"]
        i = 0
        while str(i) in games_obj:
            game = games_obj[str(i)].get("game")
            if isinstance(game, list):
                game = game[0]
            if game and game.get("code") == "nfl":
                games.append({"game_key": str(game["game_key"]), "season": str(game.get("season", ""))})
            i += 1
    except (KeyError, TypeError, IndexError):
        pass

    leagues = []
    for game in games:
        gk = game["game_key"]
        try:
            url = f"{YAHOO_API}/fantasy/v2/users;use_login=1/games;game_keys={gk}/leagues?format=json"
            resp = yahoo_api_get(url, access_token)
            fc = resp["fantasy_content"]
            user = fc["users"]["0"]["user"]
            games_section = user[1]["games"]
            j = 0
            while str(j) in games_section:
                game_arr = games_section[str(j)].get("game", [])
                if isinstance(game_arr, list) and len(game_arr) > 1:
                    leagues_obj = game_arr[1].get("leagues", {})
                elif isinstance(game_arr, dict):
                    leagues_obj = game_arr.get("leagues", {})
                else:
                    j += 1
                    continue
                k = 0
                while str(k) in leagues_obj:
                    league = leagues_obj[str(k)].get("league")
                    if isinstance(league, list):
                        league = league[0]
                    if league:
                        leagues.append(
                            {
                                "league_key": str(league.get("league_key", "")),
                                "name": str(league.get("name", "Unknown")),
                                "season": str(league.get("season", game["season"])),
                                "num_teams": int(league.get("num_teams", 0)),
                            }
                        )
                    k += 1
                j += 1
        except Exception as e:
            print(f"  Warning: game {gk}: {e}")

    print(f"  Found {len(leagues)} league-seasons")
    return leagues


def yahoo_discover_chain(league_key: str, access_token: str) -> dict[str, str]:
    """Walk Yahoo renew/renewed links to build year→league_key mapping."""
    visited: set[str] = set()
    league_ids: dict[str, str] = {}
    queue = [league_key]

    while queue:
        current = queue.pop(0)
        if current in visited:
            continue
        visited.add(current)
        try:
            url = f"{YAHOO_API}/fantasy/v2/league/{current}/settings?format=json"
            data = yahoo_api_get(url, access_token)
            fc = data.get("fantasy_content", {})
            league = fc.get("league", [])
            meta = league[0] if isinstance(league, list) and league else league if isinstance(league, dict) else {}
            season = str(meta.get("season", ""))
            if season:
                league_ids[season] = current
            for link in [meta.get("renew", ""), meta.get("renewed", "")]:
                if link:
                    parts = str(link).split("_")
                    if len(parts) >= 2:
                        linked = f"{parts[0]}.l.{'_'.join(parts[1:])}"
                        if linked not in visited:
                            queue.append(linked)
        except Exception:
            continue

    return dict(sorted(league_ids.items(), key=lambda x: int(x[0])))


def yahoo_build_chains(leagues: list[dict], access_token: str) -> list[dict]:
    """Group Yahoo leagues into chains via renewal links."""
    by_name: dict[str, list[dict]] = {}
    for lg in leagues:
        by_name.setdefault(lg["name"], []).append(lg)

    chains = []
    processed = set()
    for name, group in sorted(by_name.items()):
        group.sort(key=lambda x: int(x["season"]), reverse=True)
        anchor = group[0]
        if anchor["league_key"] in processed:
            continue

        print(f"  Discovering history for: {name}...", end=" ", flush=True)
        league_ids = yahoo_discover_chain(anchor["league_key"], access_token)
        print(f"{len(league_ids)} years")

        for key in league_ids.values():
            processed.add(key)

        years = sorted(int(y) for y in league_ids.keys())
        chains.append(
            {
                "name": name,
                "num_teams": anchor["num_teams"],
                "seasons": [{"season": yr, "league_key": key} for yr, key in league_ids.items()],
                "league_ids": league_ids,
                "start_year": min(years) if years else int(anchor["season"]),
                "end_year": max(years) if years else int(anchor["season"]),
                "latest_key": league_ids.get(str(max(years))) if years else anchor["league_key"],
            }
        )

    return chains


def yahoo_dispatch(
    chain: dict,
    refresh_token: str,
    client_id: str,
    client_secret: str,
    database_name: str | None = None,
    dry_run: bool = False,
    merge_source: dict | None = None,
) -> bool:
    league_data = {
        "league_id": chain["latest_key"],
        "league_name": chain["name"],
        "database_name": database_name or "",
        "season": chain["end_year"],
        "start_year": chain["start_year"],
        "num_teams": chain["num_teams"],
        "league_ids": chain["league_ids"],
        "oauth_token": {
            "refresh_token": refresh_token,
            "consumer_key": client_id,
            "consumer_secret": client_secret,
        },
    }
    attach_merge_source(league_data, merge_source)
    return dispatch_workflow("yahoo", league_data, dry_run=dry_run)


# ============================= UI / SELECTION ==============================


def display_chains(chains: list[dict], platform: str, known_dbs: dict[str, str] | None = None):
    """Print chains and return them for selection."""
    known_dbs = known_dbs or {}
    for i, chain in enumerate(chains):
        yrs = (
            f"{chain['start_year']}-{chain['end_year']}"
            if chain["start_year"] != chain["end_year"]
            else str(chain["start_year"])
        )
        n = len(chain.get("league_ids", chain.get("seasons", [])))
        teams = chain.get("num_teams", "?")
        db = known_dbs.get(chain["name"], "")
        marker = f" [db: {db}]" if db else ""
        print(f"  [{i+1}] {chain['name']:40s} {yrs:12s} ({n} yrs, {teams} teams){marker}")


def select_chains(chains: list[dict], auto_all: bool = False, single_db: str | None = None) -> list[int]:
    """Prompt user to select chains. Returns list of indices."""
    if auto_all:
        return list(range(len(chains)))

    if single_db:
        # Auto-select the matching chain
        for i, chain in enumerate(chains):
            db_clean = single_db.replace("_", " ").lower()
            if chain["name"].lower() in db_clean or db_clean in chain["name"].lower():
                return [i]
        print(f"  No chain matches '{single_db}', showing all:")
        # Fall through to interactive

    print("\n  Enter numbers to import (comma-separated), 'all', or 'skip':")
    choice = input("  > ").strip().lower()
    if choice in ("skip", "s", ""):
        return []
    if choice == "all":
        return list(range(len(chains)))
    try:
        return [int(x.strip()) - 1 for x in choice.split(",")]
    except ValueError:
        print("  Invalid input.")
        return []


# ============================= MAIN ========================================


def cmd_sleeper(args):
    """Handle Sleeper platform."""
    if not args.username:
        print("ERROR: --username required for Sleeper")
        sys.exit(1)
    try:
        merge_source = build_merge_source_from_args(args)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print(f"Discovering Sleeper leagues for: {args.username}")
    leagues = sleeper_discover(args.username)
    if not leagues:
        return

    chains = sleeper_build_chains(leagues)
    print(f"\n  {len(chains)} league chain(s):\n")
    display_chains(chains, "sleeper")

    if args.list:
        return

    selected = select_chains(chains, auto_all=args.all, single_db=args.db)
    if args.db and len(selected) > 1:
        print(
            f"  WARNING: --db {args.db} ignored ({len(selected)} chains selected); each chain uses its derived db_name"
        )
    dispatched = []
    for idx in selected:
        if 0 <= idx < len(chains):
            db_override = args.db if len(selected) == 1 else None
            if sleeper_dispatch(
                chains[idx],
                database_name=db_override,
                dry_run=args.dry_run,
                merge_source=merge_source,
            ):
                dispatched.append(chains[idx]["name"])

    _print_summary(dispatched)


def cmd_espn(args):
    """Handle ESPN platform."""
    if not args.swid:
        print("ERROR: --swid required for ESPN")
        sys.exit(1)
    try:
        merge_source = build_merge_source_from_args(args)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    print(f"Discovering ESPN leagues for SWID: {args.swid[:20]}...")
    leagues = espn_discover(args.swid)
    if not leagues:
        return

    print("\n  Building year histories...")
    chains = espn_build_chains(leagues, espn_s2=args.s2, swid=args.swid)
    print(f"\n  {len(chains)} league chain(s):\n")
    display_chains(chains, "espn")

    if args.list:
        return

    selected = select_chains(chains, auto_all=args.all, single_db=args.db)
    if args.db and len(selected) > 1:
        print(
            f"  WARNING: --db {args.db} ignored ({len(selected)} chains selected); each chain uses its derived db_name"
        )
    dispatched = []
    for idx in selected:
        if 0 <= idx < len(chains):
            db_override = args.db if len(selected) == 1 else None
            if espn_dispatch(
                chains[idx],
                espn_s2=args.s2,
                swid=args.swid,
                database_name=db_override,
                dry_run=args.dry_run,
                merge_source=merge_source,
            ):
                dispatched.append(chains[idx]["name"])

    _print_summary(dispatched)


def cmd_yahoo(args):
    """Handle Yahoo platform."""
    import duckdb

    from multi_league.utils.credential_store import decrypt_token, get_encryption_key

    try:
        merge_source = build_merge_source_from_args(args)
    except ValueError as exc:
        print(f"ERROR: {exc}")
        sys.exit(1)

    backend = os.environ.get("DATABASE_BACKEND", "fly")
    md_token = os.environ.get("MOTHERDUCK_TOKEN")
    encryption_key = get_encryption_key()
    client_id = os.environ.get("YAHOO_CLIENT_ID")
    client_secret = os.environ.get("YAHOO_CLIENT_SECRET")

    required = [
        ("encryption key", encryption_key),
        ("YAHOO_CLIENT_ID", client_id),
        ("YAHOO_CLIENT_SECRET", client_secret),
    ]
    if backend != "fly":
        required.insert(0, ("MOTHERDUCK_TOKEN", md_token))
    for name, val in required:
        if not val:
            print(f"ERROR: {name} not set")
            sys.exit(1)

    print("Connecting to database backend...")
    from multi_league.core.db_reader import get_reader

    reader = get_reader()
    if args.db:
        rows = reader.query(
            f"SELECT database_name, league_id, league_name, encrypted_refresh_token "
            f"FROM main.league_credentials WHERE database_name = '{args.db}'",
            database="___ops",
        )
    else:
        rows = reader.query(
            "SELECT database_name, league_id, league_name, encrypted_refresh_token "
            "FROM main.league_credentials WHERE encrypted_refresh_token IS NOT NULL ORDER BY league_name",
            database="___ops",
        )

    if not rows:
        print("No Yahoo credentials found.")
        return

    # Group by refresh token (= same user)
    token_groups: dict[str, list] = {}
    for r in rows:
        db_name, league_id, league_name, enc_token = (
            r["database_name"],
            r["league_id"],
            r["league_name"],
            r["encrypted_refresh_token"],
        )
        try:
            rt = decrypt_token(enc_token, encryption_key)
        except Exception as e:
            print(f"  Warning: decrypt failed for {db_name}: {e}")
            continue
        token_groups.setdefault(rt, []).append((db_name, league_id, league_name))

    print(f"Found {len(rows)} league(s) across {len(token_groups)} user(s)\n")

    dispatched = []
    for refresh_token, user_leagues in token_groups.items():
        names = [lg[2] for lg in user_leagues]
        known_dbs = {lg[2]: lg[0] for lg in user_leagues}
        print(f"{'='*60}")
        print(f"User: {', '.join(names[:5])}{'...' if len(names) > 5 else ''}")
        print(f"{'='*60}")

        print("  Refreshing token...", end=" ", flush=True)
        try:
            tokens = yahoo_refresh_token(refresh_token, client_id, client_secret)
            access_token = tokens["access_token"]
            new_refresh = tokens.get("refresh_token", refresh_token)
            print("OK")
        except Exception as e:
            print(f"FAILED: {e}")
            continue

        print("  Discovering leagues...")
        leagues = yahoo_discover_leagues(access_token)
        if not leagues:
            continue

        print("\n  Building renewal chains...")
        chains = yahoo_build_chains(leagues, access_token)
        print(f"\n  {len(chains)} chain(s):\n")
        display_chains(chains, "yahoo", known_dbs)

        if args.list:
            continue

        selected = select_chains(chains, auto_all=args.all, single_db=args.db)
        for idx in selected:
            if 0 <= idx < len(chains):
                chain = chains[idx]
                db_name = known_dbs.get(chain["name"])
                if yahoo_dispatch(
                    chain,
                    new_refresh,
                    client_id,
                    client_secret,
                    database_name=db_name,
                    dry_run=args.dry_run,
                    merge_source=merge_source,
                ):
                    dispatched.append(chain["name"])

    _print_summary(dispatched)


def _print_summary(dispatched: list[str]):
    if dispatched:
        print(f"\n{'='*60}")
        print(f"Dispatched {len(dispatched)} import(s):")
        for name in dispatched:
            print(f"  - {name}")
        print(f"\nMonitor: https://github.com/{GH_WORKERS_REPO}/actions")


def main():
    parser = argparse.ArgumentParser(
        description="Unified Import Dispatch CLI — Sleeper / ESPN / Yahoo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    sub = parser.add_subparsers(dest="platform", help="Platform to import from")

    # Sleeper
    sp = sub.add_parser("sleeper", help="Import from Sleeper")
    sp.add_argument("--username", required=True, help="Sleeper username")

    # ESPN
    ep = sub.add_parser("espn", help="Import from ESPN")
    ep.add_argument("--swid", required=True, help="ESPN SWID cookie ({GUID})")
    ep.add_argument("--s2", help="ESPN espn_s2 cookie (required for private leagues)")

    # Yahoo
    yp = sub.add_parser("yahoo", help="Import from Yahoo")

    # Common flags on each subparser
    for p in [sp, ep, yp]:
        p.add_argument("--db", help="Target database name (auto-match or override)")
        p.add_argument("--merge-from", dest="merge_from", help="Existing source db to copy older history from")
        p.add_argument(
            "--merge-years",
            dest="merge_years",
            help="Source years to copy, e.g. 2012-2020 or 2012,2014,2016-2020",
        )
        p.add_argument(
            "--manager-map",
            dest="manager_maps",
            action="append",
            default=[],
            help='Manager mapping for copied history, e.g. "Old Name=New Name" (repeatable)',
        )
        p.add_argument("--list", action="store_true", help="List leagues only")
        p.add_argument("--all", action="store_true", help="Import all leagues")
        p.add_argument("--dry-run", action="store_true", help="Preview without dispatching")

    args = parser.parse_args()
    if not args.platform:
        parser.print_help()
        sys.exit(1)

    _load_dotenv()

    if args.platform == "sleeper":
        cmd_sleeper(args)
    elif args.platform == "espn":
        cmd_espn(args)
    elif args.platform == "yahoo":
        cmd_yahoo(args)


if __name__ == "__main__":
    main()
