#!/usr/bin/env python3
"""
Check Yahoo, Sleeper, and ESPN leagues for offseason draft changes.

Default behavior is read-only: fetch only draft data for each league's next
season, compare it to ___leagues.public.draft, and report whether Fly needs an
update. Use --execute to replace changed/missing draft rows, run draft SQL
enrichments, rebuild draft aggregates, and rebuild homepage aggregates.

Examples:
  python scripts/check_offseason_drafts.py --db the_infirmary
  python scripts/check_offseason_drafts.py --platform sleeper --execute
  python scripts/check_offseason_drafts.py --platform yahoo --draft-year 2026 --execute
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from multi_league.core.offseason_update_status import (  # noqa: E402
    classify_offseason_update_result,
    record_offseason_update_status,
)

from update_sleeper_offseason_draft import (  # noqa: E402
    FlyDuckDBConnection,
    fly_rows,
    fly_scalar,
    league_publish_generation,
    load_dotenv,
    parse_json_field,
    replace_draft_year,
    run_complete_local_offseason_update,
    sleeper_api_json,
    sql_literal,
)


SUPPORTED_PLATFORMS = {"sleeper", "espn", "yahoo"}
YAHOO_API = "https://fantasysports.yahooapis.com"
YAHOO_TOKEN_URL = "https://api.login.yahoo.com/oauth2/get_token"
SNAPSHOT_COLUMNS = [
    "draft_id",
    "draft_category",
    "draft_type",
    "pick",
    "round",
    "pick_in_round",
    "draft_slot",
    "draft_slot_roster_id",
    "team_key",
    "yahoo_player_id",
    "sleeper_player_id",
    "espn_player_id",
    "player",
    "cost",
    "is_keeper",
]
PLATFORM_PLAYER_ID_COLUMNS = (
    "yahoo_player_id",
    "sleeper_player_id",
    "espn_player_id",
)
SLEEPER_SNAPSHOT_COLUMNS = [
    "draft_id",
    "draft_type",
    "pick",
    "round",
    "draft_slot",
    "team_key",
    "sleeper_player_id",
    "cost",
]


class DraftFetchSkipped(Exception):
    def __init__(self, status: str, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


@dataclass(frozen=True)
class PlatformDraftPlan:
    db_name: str
    platform: str
    league_id: str
    league_name: str
    draft_year: int
    completed_season: int | None
    manager_name_overrides: dict[str, str]
    league_ids: dict[str, str] = field(default_factory=dict)
    franchise_merges: list[dict[str, Any]] = field(default_factory=list)
    keeper_rules: dict[str, Any] | None = None
    league_rules: dict[str, Any] | None = None
    standings_weights: dict[str, Any] | None = None
    league_id_was_overridden: bool = False


@dataclass
class DraftCheckResult:
    db_name: str
    platform: str | None
    league_name: str | None
    league_id: str | None
    draft_year: int | None
    status: str
    api_pick_count: int = 0
    stored_pick_count: int = 0
    api_draft_ids: list[str] | None = None
    stored_draft_ids: list[str] | None = None
    api_fingerprint: str | None = None
    stored_fingerprint: str | None = None
    updated: bool = False
    error: str | None = None


def normalize_token(value: Any) -> str | None:
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    text = str(value).strip()
    if text in {"", "None", "nan", "NaN", "<NA>"}:
        return None
    if re.fullmatch(r"-?\d+", text):
        return text
    decimal_integer = re.fullmatch(r"(-?\d+)\.0+", text)
    if decimal_integer:
        return decimal_integer.group(1)
    try:
        number = float(text)
    except ValueError:
        return text
    if number.is_integer():
        return str(int(number))
    return f"{number:g}"


def normalize_draft_type(value: Any) -> str | None:
    draft_type = normalize_token(value)
    if draft_type in {"linear", "snake"}:
        return "snake"
    return draft_type


def stable_fingerprint(rows: list[dict[str, Any]]) -> str:
    payload = json.dumps(rows, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def draft_ids_from_rows(rows: list[dict[str, Any]]) -> list[str]:
    return sorted({str(row["draft_id"]) for row in rows if row.get("draft_id")})


def snapshot_from_dataframe(
    df: pd.DataFrame | None,
    *,
    snapshot_columns: list[str] = SNAPSHOT_COLUMNS,
) -> tuple[list[dict[str, Any]], list[str]]:
    if df is None or df.empty:
        return [], []
    rows: list[dict[str, Any]] = []
    sortable = df.copy()
    for column in ["draft_id", "pick"]:
        if column not in sortable.columns:
            sortable[column] = None
    sortable["_pick_sort"] = pd.to_numeric(sortable["pick"], errors="coerce")
    sortable = sortable.sort_values(["draft_id", "_pick_sort"], na_position="last")
    for raw in sortable.to_dict("records"):
        row = {column: normalize_token(raw.get(column)) for column in snapshot_columns}
        row["draft_type"] = normalize_draft_type(row.get("draft_type"))
        # The pipeline may canonicalize a display name after ingest (for
        # example, Yahoo's "Kenneth Walker" becomes "Kenneth Walker III").
        # A supplied platform ID is the durable source identity, so do not
        # trigger a needless republish solely because the display label was
        # enriched. Keep names as the fallback identity when no ID exists.
        if row.get("player") and any(row.get(column) for column in PLATFORM_PLAYER_ID_COLUMNS):
            row["player"] = None
        rows.append(row)
    return rows, draft_ids_from_rows(rows)


def stored_draft_snapshot(
    conn: FlyDuckDBConnection,
    db_name: str,
    draft_year: int,
    *,
    snapshot_columns: list[str] = SNAPSHOT_COLUMNS,
) -> tuple[list[dict[str, Any]], list[str]]:
    columns_sql = ", ".join(snapshot_columns)
    rows_df = conn.execute(
        f"SELECT {columns_sql} "
        "FROM public.draft "
        f"WHERE db_name = {sql_literal(db_name)} AND year = {int(draft_year)} "
        "ORDER BY draft_id, pick"
    ).fetchdf()
    return snapshot_from_dataframe(rows_df, snapshot_columns=snapshot_columns)


def snapshot_columns_for_platform(platform: str) -> list[str]:
    return SLEEPER_SNAPSHOT_COLUMNS if platform == "sleeper" else SNAPSHOT_COLUMNS


def completed_season_for_db(conn: FlyDuckDBConnection, db_name: str) -> int | None:
    season = fly_scalar(
        conn,
        "SELECT MAX(TRY_CAST(year AS INT)) AS season " "FROM public.matchup " f"WHERE db_name = {sql_literal(db_name)}",
    )
    return int(season) if season is not None else None


def parse_context_json(value: Any) -> dict[str, Any] | None:
    parsed = parse_json_field(value, None)
    return parsed if isinstance(parsed, dict) else None


def persisted_league_settings_ids(conn: FlyDuckDBConnection, db_name: str) -> dict[str, str]:
    """Recover registered year-to-league IDs from canonical per-year settings."""
    try:
        rows = fly_rows(
            conn,
            "SELECT TRY_CAST(year AS INTEGER) AS year, league_key "
            "FROM public.league_settings "
            f"WHERE db_name = {sql_literal(db_name)} "
            "AND TRY_CAST(year AS INTEGER) IS NOT NULL "
            "AND NULLIF(TRIM(COALESCE(league_key, '')), '') IS NOT NULL",
        )
    except Exception:
        return {}
    return {
        str(int(row["year"])): str(row["league_key"]).strip()
        for row in rows
        if row.get("year") is not None and str(row.get("league_key") or "").strip()
    }


def build_plan_from_row(
    conn: FlyDuckDBConnection,
    row: dict[str, Any],
    *,
    draft_year: int | None,
    league_id_override: str | None,
) -> PlatformDraftPlan:
    db_name = str(row.get("db_name") or "").strip()
    platform = str(row.get("platform") or "").strip().lower()
    completed_season = completed_season_for_db(conn, db_name)
    current_season = datetime.now(UTC).year
    resolved_draft_year = int(
        draft_year
        or min((completed_season + 1) if completed_season else current_season, current_season)
    )
    raw_league_ids = parse_context_json(row.get("league_ids_json")) or {}
    league_ids = {
        str(year): str(league_id).strip()
        for year, league_id in raw_league_ids.items()
        if str(year).strip() and str(league_id).strip()
    } if isinstance(raw_league_ids, dict) else {}
    league_ids = {**persisted_league_settings_ids(conn, db_name), **league_ids}
    league_id = str(
        league_id_override
        or league_ids.get(str(resolved_draft_year))
        or row.get("league_id")
        or ""
    ).strip()
    overrides = parse_context_json(row.get("manager_name_overrides_json")) or {}
    raw_franchise_merges = parse_json_field(row.get("franchise_merges_json"), [])
    franchise_merges = raw_franchise_merges if isinstance(raw_franchise_merges, list) else []
    return PlatformDraftPlan(
        db_name=db_name,
        platform=platform,
        league_id=league_id,
        league_name=str(row.get("league_name") or db_name),
        draft_year=resolved_draft_year,
        completed_season=completed_season,
        manager_name_overrides={str(key): str(value) for key, value in overrides.items()},
        league_ids=league_ids,
        franchise_merges=[merge for merge in franchise_merges if isinstance(merge, dict)],
        keeper_rules=parse_context_json(row.get("keeper_rules_json")),
        league_rules=parse_context_json(row.get("league_rules_json")),
        standings_weights=parse_context_json(row.get("standings_weights_json")),
        league_id_was_overridden=bool(league_id_override),
    )


def list_context_rows(
    conn: FlyDuckDBConnection,
    *,
    platform: str,
    db_names: list[str] | None,
) -> list[dict[str, Any]]:
    filters = [
        "league_id IS NOT NULL",
        "TRIM(CAST(league_id AS VARCHAR)) != ''",
    ]
    if platform != "all":
        filters.append(f"LOWER(COALESCE(platform, '')) = {sql_literal(platform)}")
    else:
        platform_sql = ", ".join(sql_literal(item) for item in sorted(SUPPORTED_PLATFORMS))
        filters.append(f"LOWER(COALESCE(platform, '')) IN ({platform_sql})")
    if db_names:
        db_sql = ", ".join(sql_literal(db_name) for db_name in db_names)
        filters.append(f"db_name IN ({db_sql})")

    where_sql = " AND ".join(filters)
    return fly_rows(
        conn,
        "SELECT db_name, platform, league_id, league_name, league_ids_json, manager_name_overrides_json, "
        "franchise_merges_json, "
        "keeper_rules_json, league_rules_json, standings_weights_json "
        "FROM public.league_context "
        f"WHERE {where_sql} "
        "ORDER BY LOWER(COALESCE(platform, '')), db_name",
    )


def select_context_shard(
    rows: list[dict[str, Any]],
    *,
    shard_index: int,
    shard_count: int,
) -> list[dict[str, Any]]:
    if shard_count < 1:
        raise ValueError("shard_count must be at least 1")
    if shard_index < 0 or shard_index >= shard_count:
        raise ValueError("shard_index must be between 0 and shard_count - 1")
    return rows[shard_index::shard_count]


def fetch_sleeper_api_draft_df(plan: PlatformDraftPlan, *, allow_incomplete_draft: bool) -> pd.DataFrame:
    try:
        drafts = sleeper_api_json(f"league/{plan.league_id}/drafts")
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise DraftFetchSkipped("no_api_draft", str(exc)) from exc

    candidates = [
        draft
        for draft in (drafts or [])
        if str(draft.get("season") or "") == str(plan.draft_year)
        and (allow_incomplete_draft or str(draft.get("status") or "").lower() == "complete")
    ]
    if not candidates:
        completeness = "" if allow_incomplete_draft else " complete"
        raise DraftFetchSkipped(
            "no_api_draft",
            f"No{completeness} Sleeper draft found for {plan.league_name} season {plan.draft_year} "
            f"on league_id {plan.league_id}.",
        )

    rows: list[dict[str, Any]] = []
    for draft in sorted(candidates, key=lambda item: str(item.get("draft_id") or "")):
        draft_id = normalize_token(draft.get("draft_id"))
        if not draft_id:
            continue
        try:
            picks = sleeper_api_json(f"draft/{draft_id}/picks")
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise DraftFetchSkipped("no_api_draft", str(exc)) from exc
        slot_to_roster = draft.get("slot_to_roster_id") or {}
        for pick in sorted(picks or [], key=lambda item: int(item.get("pick_no") or 0)):
            player_id = normalize_token(pick.get("player_id"))
            if not player_id:
                continue
            draft_slot = pick.get("draft_slot")
            team_key = normalize_token(pick.get("roster_id"))
            if not team_key:
                team_key = normalize_token(slot_to_roster.get(str(draft_slot), slot_to_roster.get(draft_slot)))
            metadata = pick.get("metadata") or {}
            rows.append(
                {
                    "draft_id": draft_id,
                    "draft_type": normalize_draft_type(draft.get("type")),
                    "pick": pick.get("pick_no"),
                    "round": pick.get("round"),
                    "draft_slot": draft_slot,
                    "team_key": team_key,
                    "sleeper_player_id": player_id,
                    "cost": metadata.get("amount"),
                }
            )
    return pd.DataFrame(rows, columns=SLEEPER_SNAPSHOT_COLUMNS)


def sleeper_draft_category(draft: dict[str, Any]) -> str:
    """Classify a Sleeper draft from its own settings without the player dump."""
    settings = draft.get("settings") or {}
    try:
        if int(settings.get("player_type")) == 1:
            return "rookie"
    except (TypeError, ValueError):
        pass
    try:
        return "startup" if int(settings.get("rounds") or 0) > 10 else "veteran"
    except (TypeError, ValueError):
        return "unknown"


def fetch_sleeper_publish_draft_df(plan: PlatformDraftPlan, *, allow_incomplete_draft: bool) -> pd.DataFrame:
    """Fetch canonical Sleeper draft rows without the cold full-player-cache download.

    Sleeper embeds each selected player's name, position, and team in the pick
    payload.  The roster and user endpoints provide the remaining manager and
    franchise fields.  That is all required for the draft-only publish; the
    normal SQL enrichment pass can later add optional NFL identifiers.
    """
    from multi_league.core.canonical_draft import normalize_draft_df

    try:
        drafts = sleeper_api_json(f"league/{plan.league_id}/drafts")
        rosters = sleeper_api_json(f"league/{plan.league_id}/rosters")
        users = sleeper_api_json(f"league/{plan.league_id}/users")
    except (URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise DraftFetchSkipped("no_api_draft", str(exc)) from exc

    candidates = [
        draft
        for draft in (drafts or [])
        if str(draft.get("season") or "") == str(plan.draft_year)
        and (allow_incomplete_draft or str(draft.get("status") or "").lower() == "complete")
    ]
    if not candidates:
        completeness = "" if allow_incomplete_draft else " complete"
        raise DraftFetchSkipped(
            "no_api_draft",
            f"No{completeness} Sleeper draft found for {plan.league_name} season {plan.draft_year} "
            f"on league_id {plan.league_id}.",
        )

    users_by_id: dict[str, dict[str, str | None]] = {}
    for user in users or []:
        user_id = normalize_token(user.get("user_id"))
        if not user_id:
            continue
        metadata = user.get("metadata") or {}
        users_by_id[user_id] = {
            "manager": normalize_token(user.get("display_name")) or normalize_token(user.get("username")) or "Unknown",
            "team_name": normalize_token(metadata.get("team_name")),
        }

    roster_owners: dict[str, str] = {}
    for roster in rosters or []:
        roster_id = normalize_token(roster.get("roster_id"))
        if roster_id:
            roster_owners[roster_id] = normalize_token(roster.get("owner_id"))

    rows: list[dict[str, Any]] = []
    for draft in sorted(candidates, key=lambda item: str(item.get("draft_id") or "")):
        draft_id = normalize_token(draft.get("draft_id"))
        if not draft_id:
            continue
        try:
            picks = sleeper_api_json(f"draft/{draft_id}/picks")
        except (URLError, TimeoutError, json.JSONDecodeError) as exc:
            raise DraftFetchSkipped("no_api_draft", str(exc)) from exc
        slot_to_roster = draft.get("slot_to_roster_id") or {}
        category = sleeper_draft_category(draft)
        for pick in sorted(picks or [], key=lambda item: int(item.get("pick_no") or 0)):
            player_id = normalize_token(pick.get("player_id"))
            if not player_id:
                continue
            draft_slot = pick.get("draft_slot")
            roster_id = normalize_token(pick.get("roster_id"))
            if not roster_id:
                roster_id = normalize_token(slot_to_roster.get(str(draft_slot), slot_to_roster.get(draft_slot)))
            owner_id = roster_owners.get(roster_id) or normalize_token(pick.get("picked_by"))
            user = users_by_id.get(owner_id, {})
            manager = str(user.get("manager") or "Unknown")
            manager = plan.manager_name_overrides.get(manager, manager)
            metadata = pick.get("metadata") or {}
            player = " ".join(
                part
                for part in [normalize_token(metadata.get("first_name")), normalize_token(metadata.get("last_name"))]
                if part
            )
            rows.append(
                {
                    "year": plan.draft_year,
                    "round": pick.get("round"),
                    "pick": pick.get("pick_no"),
                    "draft_slot": draft_slot,
                    "draft_slot_roster_id": normalize_token(
                        slot_to_roster.get(str(draft_slot), slot_to_roster.get(draft_slot))
                    ),
                    "manager": manager,
                    "manager_guid": owner_id or f"orp{str(roster_id).zfill(5)}",
                    "team_key": roster_id,
                    "team_name": user.get("team_name"),
                    "league_id": plan.league_id,
                    "player": player or None,
                    "position": normalize_token(metadata.get("position")),
                    "nfl_team_api": normalize_token(metadata.get("team")),
                    "sleeper_player_id": player_id,
                    "cost": metadata.get("amount"),
                    "draft_type": normalize_draft_type(draft.get("type")),
                    "is_keeper": pick.get("is_keeper"),
                    "draft_id": draft_id,
                    "draft_category": category,
                }
            )

    raw = pd.DataFrame(rows)
    if raw.empty:
        return raw
    raw["db_name"] = plan.db_name
    normalized = normalize_draft_df(raw, platform="sleeper", league_id=plan.league_id)
    normalized["db_name"] = plan.db_name
    return normalized


def retrieve_required_espn_credentials(plan: PlatformDraftPlan) -> dict[str, Any]:
    from multi_league.utils.credential_store import retrieve_espn_credentials

    creds = retrieve_espn_credentials(plan.db_name)
    if not creds:
        raise DraftFetchSkipped(
            "auth_missing",
            f"No decrypted ESPN credentials found for {plan.db_name!r}; set CREDENTIAL_ENCRYPTION_KEY or store cookies.",
        )
    return creds


def team_id_from_espn_team(team: Any) -> int | None:
    value = getattr(team, "team_id", None)
    if value is None and isinstance(team, dict):
        value = team.get("team_id") or team.get("id")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def team_name_from_espn_team(team: Any) -> str:
    if isinstance(team, dict):
        return str(team.get("team_name") or team.get("name") or "")
    return str(getattr(team, "team_name", None) or getattr(team, "name", None) or "")


def owner_guid_from_espn_team(team: Any) -> str:
    owners = getattr(team, "owners", None)
    if owners is None and isinstance(team, dict):
        owners = team.get("owners", [])
    if not owners:
        return ""
    owner = owners[0] if isinstance(owners, list) else owners
    if isinstance(owner, dict):
        return str(owner.get("id") or "")
    return str(getattr(owner, "id", "") or "")


def hydrate_espn_team_maps(ctx: Any, year: int) -> None:
    from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient
    from multi_league.data_fetchers.espn.espn_context import build_manager_names

    client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)
    league = client.get_league(year)
    teams = list(league.teams or [])
    manager_map = build_manager_names(teams)
    guid_map: dict[int, str] = {}
    team_name_map: dict[int, str] = {}
    for team in teams:
        team_id = team_id_from_espn_team(team)
        if team_id is None:
            continue
        guid_map[team_id] = owner_guid_from_espn_team(team)
        team_name_map[team_id] = team_name_from_espn_team(team)

    ctx.team_to_manager = manager_map
    ctx.team_to_guid = guid_map
    ctx.team_to_team_name[str(year)] = team_name_map
    ctx.team_to_manager_by_year[str(year)] = manager_map
    ctx.team_to_guid_by_year[str(year)] = guid_map


def fetch_espn_api_draft_df(plan: PlatformDraftPlan) -> pd.DataFrame:
    from multi_league.core.canonical_draft import normalize_draft_df
    from multi_league.data_fetchers.espn.espn_context import ESPNContext
    from multi_league.data_fetchers.espn.espn_draft import fetch_espn_draft

    creds = retrieve_required_espn_credentials(plan)
    league_id = str(creds.get("league_id") or plan.league_id).strip()
    if not league_id:
        raise DraftFetchSkipped("auth_missing", f"No ESPN league_id found for {plan.db_name!r}")

    with tempfile.TemporaryDirectory(prefix=f"{plan.db_name}_offseason_draft_") as tmp:
        ctx = ESPNContext(
            league_id=int(league_id),
            league_name=plan.league_name,
            espn_s2=creds.get("espn_s2"),
            swid=creds.get("swid"),
            start_year=plan.draft_year,
            end_year=plan.draft_year,
            data_directory=Path(tmp) / plan.db_name,
            league_ids={str(plan.draft_year): int(league_id)},
            manager_name_overrides=plan.manager_name_overrides,
            franchise_merges=plan.franchise_merges,
            keeper_rules=plan.keeper_rules,
            league_rules=plan.league_rules,
            standings_weights=plan.standings_weights,
            database_name=plan.db_name,
            import_mode="offseason_draft",
        )
        hydrate_espn_team_maps(ctx, plan.draft_year)
        raw = fetch_espn_draft(ctx, plan.draft_year)

    if raw is None or raw.empty:
        return pd.DataFrame()

    raw = raw.copy()
    raw["db_name"] = plan.db_name
    normalized = normalize_draft_df(raw, platform="espn", league_id=league_id)
    normalized["db_name"] = plan.db_name
    return normalized


def yahoo_client_id_secret() -> tuple[str, str]:
    client_id = os.environ.get("YAHOO_CLIENT_ID") or os.environ.get("YAHOO_CONSUMER_KEY")
    client_secret = os.environ.get("YAHOO_CLIENT_SECRET") or os.environ.get("YAHOO_CONSUMER_SECRET")
    if not client_id or not client_secret:
        raise DraftFetchSkipped(
            "auth_missing",
            "YAHOO_CLIENT_ID/YAHOO_CLIENT_SECRET or YAHOO_CONSUMER_KEY/YAHOO_CONSUMER_SECRET are required.",
        )
    return client_id, client_secret


def retrieve_required_yahoo_credentials(plan: PlatformDraftPlan) -> dict[str, Any]:
    from multi_league.utils.credential_store import retrieve_league_credentials

    creds = retrieve_league_credentials(plan.db_name)
    if not creds or not creds.get("refresh_token"):
        raise DraftFetchSkipped(
            "auth_missing",
            f"No decrypted Yahoo refresh token found for {plan.db_name!r}; set CREDENTIAL_ENCRYPTION_KEY.",
        )
    return creds


def yahoo_refresh_token(refresh_token: str, client_id: str, client_secret: str) -> dict[str, Any]:
    payload = urlencode(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("utf-8")
    request = Request(
        YAHOO_TOKEN_URL,
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def yahoo_api_get(url: str, access_token: str) -> dict[str, Any]:
    request = Request(url, headers={"Authorization": f"Bearer {access_token}"})
    with urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def yahoo_discover_chain(league_key: str, access_token: str) -> dict[str, str]:
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
            league = data.get("fantasy_content", {}).get("league", [])
            meta = league[0] if isinstance(league, list) and league else league if isinstance(league, dict) else {}
            season = str(meta.get("season") or "")
            if season:
                league_ids[season] = current
            for link in [meta.get("renew", ""), meta.get("renewed", "")]:
                if not link:
                    continue
                parts = str(link).split("_")
                if len(parts) >= 2:
                    linked = f"{parts[0]}.l.{'_'.join(parts[1:])}"
                    if linked not in visited:
                        queue.append(linked)
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError):
            continue

    return dict(sorted(league_ids.items(), key=lambda item: int(item[0])))


def resolve_yahoo_league_key(
    plan: PlatformDraftPlan,
    *,
    access_token: str,
) -> str:
    if not plan.league_id:
        raise DraftFetchSkipped("auth_missing", f"No Yahoo league_id found for {plan.db_name!r}")

    registered_target = plan.league_ids.get(str(plan.draft_year))
    if registered_target:
        return registered_target
    if plan.league_id_was_overridden:
        return plan.league_id

    chain = yahoo_discover_chain(plan.league_id, access_token)
    resolved = chain.get(str(plan.draft_year))
    if resolved:
        return resolved
    years = ", ".join(chain) if chain else "none"
    raise DraftFetchSkipped(
        "no_api_draft",
        f"Could not find Yahoo league_key for {plan.db_name!r} season {plan.draft_year}; discovered years: {years}.",
    )


def fetch_yahoo_api_draft_df(plan: PlatformDraftPlan) -> pd.DataFrame:
    from multi_league.core.canonical_draft import normalize_draft_df
    from multi_league.core.league_context import LeagueContext
    from multi_league.data_fetchers.yahoo.yahoo_draft import fetch_draft_data

    creds = retrieve_required_yahoo_credentials(plan)
    client_id, client_secret = yahoo_client_id_secret()
    try:
        tokens = yahoo_refresh_token(str(creds["refresh_token"]), client_id, client_secret)
    except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
        raise DraftFetchSkipped("auth_missing", f"Yahoo token refresh failed for {plan.db_name!r}: {exc}") from exc

    access_token = str(tokens.get("access_token") or "")
    if not access_token:
        raise DraftFetchSkipped("auth_missing", f"Yahoo token refresh returned no access token for {plan.db_name!r}")
    league_key = resolve_yahoo_league_key(plan, access_token=access_token)
    oauth_credentials = {
        "access_token": access_token,
        "refresh_token": creds["refresh_token"],
        "consumer_key": client_id,
        "consumer_secret": client_secret,
        "token_type": tokens.get("token_type", "bearer"),
        "expires_in": tokens.get("expires_in", 3600),
        "token_time": time.time(),
        "guid": tokens.get("xoauth_yahoo_guid") or tokens.get("guid"),
    }

    with tempfile.TemporaryDirectory(prefix=f"{plan.db_name}_offseason_draft_") as tmp:
        ctx = LeagueContext(
            league_id=league_key,
            league_name=plan.league_name,
            oauth_credentials=oauth_credentials,
            start_year=plan.draft_year,
            end_year=plan.draft_year,
            data_directory=Path(tmp) / plan.db_name,
            league_ids={str(plan.draft_year): league_key},
            manager_name_overrides=plan.manager_name_overrides,
            franchise_merges=plan.franchise_merges,
            keeper_rules=plan.keeper_rules,
            league_rules=plan.league_rules,
            standings_weights=plan.standings_weights,
            database_name=plan.db_name,
            import_mode="offseason_draft",
        )
        raw = fetch_draft_data(ctx=ctx, year=plan.draft_year, data_dir=ctx.data_directory)

    if raw is None or raw.empty:
        return pd.DataFrame()

    raw = raw.copy()
    raw["db_name"] = plan.db_name
    normalized = normalize_draft_df(raw, platform="yahoo", league_id=league_key)
    normalized["db_name"] = plan.db_name
    return normalized


def fetch_platform_draft_dataframe_once(
    plan: PlatformDraftPlan,
    *,
    allow_incomplete_draft: bool,
) -> pd.DataFrame:
    if plan.platform == "sleeper":
        return fetch_sleeper_api_draft_df(plan, allow_incomplete_draft=allow_incomplete_draft)
    if plan.platform == "espn":
        return fetch_espn_api_draft_df(plan)
    if plan.platform == "yahoo":
        return fetch_yahoo_api_draft_df(plan)
    raise DraftFetchSkipped("unsupported_platform", f"Unsupported platform for {plan.db_name!r}: {plan.platform!r}")


def is_transient_fetch_error(exc: BaseException) -> bool:
    message = str(exc).lower()
    return any(token in message for token in ["timed out", "timeout", "temporarily", "connection reset", "502", "503"])


def fetch_platform_draft_dataframe(
    plan: PlatformDraftPlan,
    *,
    allow_incomplete_draft: bool,
    api_retries: int,
) -> pd.DataFrame:
    attempts = max(1, int(api_retries))
    for attempt in range(1, attempts + 1):
        try:
            return fetch_platform_draft_dataframe_once(plan, allow_incomplete_draft=allow_incomplete_draft)
        except DraftFetchSkipped as exc:
            if attempt < attempts and is_transient_fetch_error(exc):
                print(f"  transient fetch error for {plan.db_name}; retrying ({attempt + 1}/{attempts}): {exc.message}")
                time.sleep(min(10, 2 * attempt))
                continue
            raise
        except (HTTPError, URLError, TimeoutError, json.JSONDecodeError) as exc:
            if attempt < attempts and is_transient_fetch_error(exc):
                print(f"  transient fetch error for {plan.db_name}; retrying ({attempt + 1}/{attempts}): {exc}")
                time.sleep(min(10, 2 * attempt))
                continue
            raise
    raise DraftFetchSkipped("error", f"Unexpected fetch retry exhaustion for {plan.db_name!r}")


def check_one(
    conn: FlyDuckDBConnection,
    row: dict[str, Any],
    *,
    draft_year: int | None,
    league_id_override: str | None,
    allow_incomplete_draft: bool,
    api_retries: int,
) -> tuple[DraftCheckResult, PlatformDraftPlan | None, pd.DataFrame | None]:
    plan = build_plan_from_row(conn, row, draft_year=draft_year, league_id_override=league_id_override)
    result = DraftCheckResult(
        db_name=plan.db_name,
        platform=plan.platform,
        league_name=plan.league_name,
        league_id=plan.league_id,
        draft_year=plan.draft_year,
        status="unknown",
    )

    try:
        api_df = fetch_platform_draft_dataframe(
            plan,
            allow_incomplete_draft=allow_incomplete_draft,
            api_retries=api_retries,
        )
    except DraftFetchSkipped as exc:
        result.status = exc.status
        result.error = exc.message
        return result, plan, None

    snapshot_columns = snapshot_columns_for_platform(plan.platform)
    api_rows, api_draft_ids = snapshot_from_dataframe(api_df, snapshot_columns=snapshot_columns)
    stored_rows, stored_draft_ids = stored_draft_snapshot(
        conn,
        plan.db_name,
        plan.draft_year,
        snapshot_columns=snapshot_columns,
    )
    result.api_pick_count = len(api_rows)
    result.stored_pick_count = len(stored_rows)
    result.api_draft_ids = api_draft_ids
    result.stored_draft_ids = stored_draft_ids
    result.api_fingerprint = stable_fingerprint(api_rows)
    result.stored_fingerprint = stable_fingerprint(stored_rows)

    if not api_rows:
        result.status = "no_api_picks"
    elif result.api_fingerprint == result.stored_fingerprint:
        result.status = "up_to_date"
    elif not stored_rows:
        result.status = "missing_in_fly"
    else:
        result.status = "changed"
    return result, plan, api_df


def execute_update(
    conn: FlyDuckDBConnection,
    result: DraftCheckResult,
    plan: PlatformDraftPlan,
    *,
    checker_draft_df: pd.DataFrame | None = None,
    allow_incomplete_draft: bool,
    api_retries: int,
    skip_sql_enrichments: bool,
    include_ops_enrichments: bool,
    strict_sql_enrichments: bool,
    skip_aggregates: bool,
    skip_homepage: bool,
    chunk_size: int,
    base_generation: int | None = None,
) -> None:
    # Bind the version of the league that this provider refetch is allowed to
    # replace. A fleet sweep has no per-league GitHub queue; Fly rejects a
    # bundle if a weekly/import writer advanced this generation meanwhile.
    if base_generation is None:
        base_generation = league_publish_generation(conn, plan.db_name)
    if plan.platform == "sleeper":
        # The check fetch is deliberately a small source fingerprint and does
        # not include publish fields such as ``year``. Fetch canonical rows
        # from Sleeper's draft, roster, and user endpoints for the write.
        draft_df = fetch_sleeper_publish_draft_df(plan, allow_incomplete_draft=allow_incomplete_draft)
    elif checker_draft_df is not None:
        draft_df = checker_draft_df
    else:
        draft_df = fetch_platform_draft_dataframe(
            plan,
            allow_incomplete_draft=allow_incomplete_draft,
            api_retries=api_retries,
        )
    if skip_sql_enrichments and skip_aggregates and skip_homepage:
        # Explicit draft-table-only maintenance remains available for operators.
        # Product-triggered updates always use the complete local stage below.
        replace_draft_year(conn, plan, draft_df, chunk_size=chunk_size, dry_run=False,
                           base_generation=base_generation)
    else:
        run_complete_local_offseason_update(
            conn,
            plan,
            draft_df,
            include_ops_enrichments=include_ops_enrichments,
            strict_sql_enrichments=strict_sql_enrichments,
            skip_sql_enrichments=skip_sql_enrichments,
            skip_aggregates=skip_aggregates,
            skip_homepage=skip_homepage,
            chunk_size=chunk_size,
            base_generation=base_generation,
        )

    snapshot_columns = snapshot_columns_for_platform(plan.platform)
    expected_rows, expected_draft_ids = snapshot_from_dataframe(
        draft_df,
        snapshot_columns=snapshot_columns,
    )
    stored_rows, stored_draft_ids = stored_draft_snapshot(
        conn,
        plan.db_name,
        plan.draft_year,
        snapshot_columns=snapshot_columns,
    )
    if stable_fingerprint(expected_rows) != stable_fingerprint(stored_rows):
        raise RuntimeError(
            "post-update draft fingerprint mismatch "
            f"for {plan.db_name!r} year={plan.draft_year}: "
            f"expected_drafts={expected_draft_ids}, stored_drafts={stored_draft_ids}"
        )

    result.api_pick_count = len(expected_rows)
    result.stored_pick_count = len(stored_rows)
    result.api_draft_ids = expected_draft_ids
    result.stored_draft_ids = stored_draft_ids
    result.api_fingerprint = stable_fingerprint(expected_rows)
    result.stored_fingerprint = stable_fingerprint(stored_rows)
    result.updated = True


def print_result(result: DraftCheckResult) -> None:
    if result.status in {"missing_in_fly", "changed"}:
        marker = "UPDATE_NEEDED"
    elif result.status == "up_to_date":
        marker = "OK"
    else:
        marker = "SKIP"
    print(
        f"[{marker}] {result.db_name} ({result.platform or 'unknown'}): "
        f"status={result.status} year={result.draft_year} api_picks={result.api_pick_count} "
        f"stored_picks={result.stored_pick_count} api_drafts={result.api_draft_ids or []} "
        f"stored_drafts={result.stored_draft_ids or []}"
    )
    if result.error:
        print(f"  {result.error}")


def finalize_execution_result(
    result: DraftCheckResult,
    *,
    writer: Any,
    workflow_run_id: int | str | None,
    dispatch_token: str | None = None,
    revalidate_cache: Any | None = None,
) -> str:
    """Refresh public cache, then persist the worker's truthful terminal state."""
    terminal_status = classify_offseason_update_result(result)
    if terminal_status != "failed" and revalidate_cache is not None:
        try:
            revalidate_cache(result.db_name)
        except Exception as exc:
            result.status = "error"
            result.error = f"Cache refresh failed: {exc}"
            terminal_status = "failed"

    record_offseason_update_status(
        writer,
        database_name=result.db_name,
        draft_year=int(result.draft_year or 0),
        platform=str(result.platform or "unknown"),
        status=terminal_status,
        workflow_run_id=workflow_run_id,
        dispatch_token=dispatch_token,
        error=result.error if terminal_status == "failed" else None,
    )
    return terminal_status


def execution_exit_code(results: list[DraftCheckResult], *, execute: bool) -> int:
    """Fail an executing workflow when any requested provider update did not succeed."""
    if execute:
        return 1 if any(classify_offseason_update_result(result) == "failed" for result in results) else 0
    return 1 if any(result.status == "error" for result in results) else 0


def missing_requested_context_results(
    rows: list[dict[str, Any]],
    *,
    requested_db_names: list[str] | None,
    draft_year: int | None,
) -> list[DraftCheckResult]:
    """Make an explicit missing target visible instead of returning a green empty run."""
    if not requested_db_names:
        return []
    found = {str(row.get("db_name") or "").strip() for row in rows}
    year = int(draft_year or datetime.now(UTC).year)
    return [
        DraftCheckResult(
            db_name=db_name,
            platform="unknown",
            league_name=None,
            league_id=None,
            draft_year=year,
            status="error",
            error=f"Fly has no saved league context for {db_name!r}",
        )
        for db_name in requested_db_names
        if db_name not in found
    ]


def revalidate_public_league_cache(db_name: str) -> None:
    """Expire the league's public cache after the Fly update is visible."""
    from warm_vercel_cache import normalize_secret, revalidate

    secret = normalize_secret(os.environ.get("REVALIDATION_SECRET"))
    if not secret:
        raise RuntimeError("REVALIDATION_SECRET is not configured")
    revalidate(
        os.environ.get("LEAGUE_HISTORY_SITE_URL", "https://www.leaguehistory.app"),
        db_name,
        secret,
        "expire",
        25,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Check offseason drafts and optionally update changed leagues.")
    parser.add_argument("--platform", choices=["all", "sleeper", "espn", "yahoo"], default="all")
    parser.add_argument("--db", action="append", dest="db_names", help="Limit to one db_name; repeatable.")
    parser.add_argument(
        "--draft-year", type=int, help="Draft year to check. Defaults to max matchup year + 1 per league."
    )
    parser.add_argument("--league-id", help="Override league id/key for a single --db check.")
    parser.add_argument("--execute", action="store_true", help="Apply draft-only updates for changed/missing leagues.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run the complete scoped refresh even when the source draft fingerprint is unchanged.",
    )
    parser.add_argument(
        "--allow-incomplete-draft", action="store_true", help="Compare/update incomplete drafts when supported."
    )
    parser.add_argument(
        "--api-retries", type=int, default=3, help="API fetch attempts per league before skipping/failing."
    )
    parser.add_argument("--limit", type=int, help="Limit number of leagues checked.")
    parser.add_argument("--shard-index", type=int, default=0, help="Zero-based context shard index.")
    parser.add_argument("--shard-count", type=int, default=1, help="Total number of context shards.")
    parser.add_argument("--json-out", help="Write check results to this JSON file.")
    parser.add_argument(
        "--skip-sql-enrichments", action="store_true", help="When executing, skip draft SQL enrichments."
    )
    parser.add_argument(
        "--include-ops-enrichments",
        action="store_true",
        help="When executing, include ops-backed draft SQL enrichments.",
    )
    parser.add_argument(
        "--strict-sql-enrichments",
        action="store_true",
        help="When executing, fail on draft SQL enrichment errors.",
    )
    parser.add_argument("--skip-aggregates", action="store_true", help="When executing, skip draft aggregate rebuilds.")
    parser.add_argument(
        "--skip-homepage", action="store_true", help="When executing, skip homepage aggregate rebuilds."
    )
    parser.add_argument("--chunk-size", type=int, default=50, help="Rows per Fly INSERT statement during updates.")
    parser.add_argument("--dispatch-token", help="Opaque claim token for guarded worker status updates.")
    parser.add_argument(
        "--track-dispatch",
        action="store_true",
        help="Write running and terminal states to the Fly dispatch ledger.",
    )
    parser.add_argument(
        "--revalidate-cache",
        action="store_true",
        help="Expire the league's public Vercel cache before reporting success.",
    )
    args = parser.parse_args()

    load_dotenv(REPO_ROOT / ".env")
    if args.league_id and (not args.db_names or len(args.db_names) != 1):
        parser.error("--league-id can only be used with exactly one --db")

    conn = FlyDuckDBConnection()
    rows = list_context_rows(conn, platform=args.platform, db_names=args.db_names)
    missing_results = missing_requested_context_results(
        rows,
        requested_db_names=args.db_names,
        draft_year=args.draft_year,
    )
    try:
        rows = select_context_shard(rows, shard_index=args.shard_index, shard_count=args.shard_count)
    except ValueError as exc:
        parser.error(str(exc))
    if args.limit:
        rows = rows[: args.limit]

    results: list[DraftCheckResult] = list(missing_results)
    workflow_run_id = os.environ.get("GITHUB_RUN_ID")
    if args.execute and args.track_dispatch:
        for result in missing_results:
            finalize_execution_result(
                result,
                writer=conn.writer,
                workflow_run_id=workflow_run_id,
                dispatch_token=args.dispatch_token,
            )
    for row in rows:
        plan: PlatformDraftPlan | None = None
        if args.execute and args.track_dispatch:
            status_claimed = record_offseason_update_status(
                conn.writer,
                database_name=str(row.get("db_name") or "unknown"),
                draft_year=int(args.draft_year or datetime.now(UTC).year),
                platform=str(row.get("platform") or "unknown"),
                status="running",
                workflow_run_id=workflow_run_id,
                dispatch_token=args.dispatch_token,
            )
            if args.dispatch_token and not status_claimed:
                result = DraftCheckResult(
                    db_name=str(row.get("db_name") or "unknown"),
                    platform=str(row.get("platform") or "unknown"),
                    league_name=str(row.get("league_name") or "") or None,
                    league_id=str(row.get("league_id") or "") or None,
                    draft_year=args.draft_year,
                    status="error",
                    error="This worker no longer owns the active update claim; refusing to fetch or write data.",
                )
                print_result(result)
                results.append(result)
                continue
        try:
            # Yahoo may publish the first checker fetch directly. Capture the
            # Fly version before even that read, not only before a refetch.
            base_generation = league_publish_generation(conn, str(row.get("db_name") or "")) if args.execute else None
            result, plan, checker_draft_df = check_one(
                conn,
                row,
                draft_year=args.draft_year,
                league_id_override=args.league_id,
                allow_incomplete_draft=args.allow_incomplete_draft,
                api_retries=args.api_retries,
            )
            print_result(result)
            if args.execute and plan is not None and (args.force or result.status in {"missing_in_fly", "changed"}):
                execute_update(
                    conn,
                    result,
                    plan,
                    checker_draft_df=checker_draft_df,
                    allow_incomplete_draft=args.allow_incomplete_draft,
                    api_retries=args.api_retries,
                    skip_sql_enrichments=args.skip_sql_enrichments,
                    include_ops_enrichments=args.include_ops_enrichments,
                    strict_sql_enrichments=args.strict_sql_enrichments,
                    skip_aggregates=args.skip_aggregates,
                    skip_homepage=args.skip_homepage,
                    chunk_size=args.chunk_size,
                    base_generation=base_generation,
                )
                print(f"  updated={result.updated}")
        except (Exception, SystemExit) as exc:
            result = DraftCheckResult(
                db_name=str(row.get("db_name") or "unknown"),
                platform=str(row.get("platform") or "unknown"),
                league_name=str(row.get("league_name") or "") or None,
                league_id=str(row.get("league_id") or "") or None,
                draft_year=args.draft_year,
                status="error",
                error=str(exc),
            )
            print_result(result)
        if args.execute and args.track_dispatch:
            finalize_execution_result(
                result,
                writer=conn.writer,
                workflow_run_id=workflow_run_id,
                dispatch_token=args.dispatch_token,
                revalidate_cache=revalidate_public_league_cache if args.revalidate_cache else None,
            )
        results.append(result)

    summary: dict[str, int] = {}
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
    print("\nSummary:")
    for status, count in sorted(summary.items()):
        print(f"  {status}: {count}")
    if args.execute:
        print(f"  updated: {sum(1 for result in results if result.updated)}")

    if args.json_out:
        Path(args.json_out).write_text(
            json.dumps([asdict(result) for result in results], indent=2, sort_keys=True),
            encoding="utf-8",
        )

    return execution_exit_code(results, execute=args.execute)


if __name__ == "__main__":
    raise SystemExit(main())
