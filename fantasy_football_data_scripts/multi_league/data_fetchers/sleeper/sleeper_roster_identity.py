"""Shared roster identity helpers for historical Sleeper fetchers."""

from __future__ import annotations

import json
import logging
import re
from typing import Any
from collections.abc import Callable

from .sleeper_api_client import SleeperAPIClient
from .sleeper_context import SleeperContext

logger = logging.getLogger(__name__)

LogFn = Callable[[str], None]


def _emit(log_fn: LogFn | None, message: str) -> None:
    if log_fn:
        log_fn(message)
    else:
        logger.info(message)


def _normalize_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"", "none", "nan"}:
        return ""
    return text


def _manager_name(ctx: SleeperContext, raw_name: str) -> str:
    manager_name = raw_name or "Unknown"
    return ctx.manager_name_overrides.get(manager_name, manager_name)


def _synthetic_roster_entry(ctx: SleeperContext, roster_id: int) -> dict[str, str]:
    synthetic_name = _manager_name(ctx, f"Team {roster_id}")
    return {
        "manager_name": synthetic_name,
        "manager_guid": f"orp{int(roster_id):04d}00",
        "team_name": synthetic_name,
    }


def _is_placeholder_name(value: Any) -> bool:
    text = _normalize_text(value)
    if not text:
        return True
    lowered = text.lower()
    return lowered == "unknown" or bool(re.fullmatch(r"team\s+\d+", lowered))


def _same_roster_label(existing: dict[str, str], incoming: dict[str, str]) -> bool:
    for key in ("manager_name", "team_name"):
        existing_value = _normalize_text(existing.get(key))
        incoming_value = _normalize_text(incoming.get(key))
        if existing_value and incoming_value and existing_value == incoming_value:
            return True
    return False


def _merge_entry(existing: dict[str, str] | None, incoming: dict[str, str]) -> dict[str, str]:
    normalized_incoming = {
        key: _normalize_text(incoming.get(key)) for key in ("manager_name", "manager_guid", "team_name")
    }
    if not existing:
        return normalized_incoming

    merged = {key: _normalize_text((existing or {}).get(key)) for key in ("manager_name", "manager_guid", "team_name")}
    existing_guid = merged.get("manager_guid", "")
    incoming_guid = normalized_incoming.get("manager_guid", "")

    # Never splice manager labels and GUIDs from different identities together.
    # If two sources disagree on GUID, either keep the existing coherent entry
    # or replace it wholesale when the old entry is clearly a placeholder/stale
    # copy of the same roster identity.
    if existing_guid and incoming_guid and existing_guid != incoming_guid:
        existing_manager = merged.get("manager_name", "")
        if existing_manager.lower() == "unknown" or _same_roster_label(merged, normalized_incoming):
            return normalized_incoming
        return merged

    for key in ("manager_name", "manager_guid", "team_name"):
        incoming_value = normalized_incoming.get(key, "")
        if not incoming_value:
            continue
        existing_value = merged.get(key, "")
        if not existing_value:
            merged[key] = incoming_value
            continue
        if key != "manager_guid" and _is_placeholder_name(existing_value) and not _is_placeholder_name(incoming_value):
            merged[key] = incoming_value
    return merged


def build_api_roster_map(
    ctx: SleeperContext,
    client: SleeperAPIClient,
    league_id: str,
) -> dict[int, dict[str, str]]:
    """Build a roster_id -> manager mapping from Sleeper API state."""
    rosters = client.get_league_rosters(league_id) or []
    users = client.get_league_users(league_id) or []

    user_info: dict[str, dict[str, str]] = {}
    for user in users:
        user_id = _normalize_text(user.get("user_id"))
        if not user_id:
            continue
        display_name = _manager_name(ctx, user.get("display_name") or user.get("username") or "Unknown")
        team_name = _normalize_text((user.get("metadata") or {}).get("team_name")) or display_name
        user_info[user_id] = {
            "display_name": display_name,
            "team_name": team_name,
        }

    roster_map: dict[int, dict[str, str]] = {}
    for roster in rosters:
        roster_id = roster.get("roster_id")
        if roster_id is None:
            continue
        owner_id = _normalize_text(roster.get("owner_id"))
        info = user_info.get(owner_id, {})
        manager_name = info.get("display_name") or _manager_name(ctx, f"Team {roster_id}")
        manager_guid = owner_id or f"orp{int(roster_id):04d}00"
        team_name = info.get("team_name") or manager_name
        roster_map[int(roster_id)] = {
            "manager_name": manager_name,
            "manager_guid": manager_guid,
            "team_name": team_name,
        }

    return roster_map


def load_matchup_roster_map(
    ctx: SleeperContext,
    year: int,
    db=None,
    log_fn: LogFn | None = None,
) -> dict[int, dict[str, str]]:
    """Load historical roster mapping from already-fetched matchup rows."""
    if db is None:
        return {}

    try:
        conn = db.connect()
        matchup_df = conn.execute(
            "SELECT DISTINCT manager, manager_guid, team_key FROM public.matchup WHERE year = ?",
            [year],
        ).fetchdf()
    except Exception as exc:
        _emit(log_fn, f"  [WARN] Could not read matchup roster map for {year}: {exc}")
        return {}

    if matchup_df.empty or "team_key" not in matchup_df.columns:
        return {}

    roster_map: dict[int, dict[str, str]] = {}
    for _, row in matchup_df.drop_duplicates(subset=["manager", "team_key"]).iterrows():
        manager_name = _normalize_text(row.get("manager"))
        team_key = _normalize_text(row.get("team_key"))
        if not manager_name or manager_name == "Unknown" or not team_key:
            continue
        try:
            roster_id = int(team_key)
        except (TypeError, ValueError):
            continue
        roster_map.setdefault(
            roster_id,
            {
                "manager_name": _manager_name(ctx, manager_name),
                "manager_guid": _normalize_text(row.get("manager_guid")),
                "team_name": _normalize_text(row.get("team_name")) or _manager_name(ctx, manager_name),
            },
        )

    if roster_map:
        _emit(log_fn, f"  Loaded {len(roster_map)} roster mappings from matchup data for {year}")
    return roster_map


def load_manifest_roster_map(
    ctx: SleeperContext,
    year: int,
    log_fn: LogFn | None = None,
) -> dict[int, dict[str, str]]:
    """Load roster mapping from the matchup-generated Sleeper manager manifest."""
    manifest_path = ctx.data_directory / "sleeper_roster_manager_manifest.json"
    if not manifest_path.exists():
        return {}

    try:
        manifest = json.loads(manifest_path.read_text())
    except Exception as exc:
        _emit(log_fn, f"  [WARN] Could not read Sleeper manager manifest: {exc}")
        return {}

    roster_map: dict[int, dict[str, str]] = {}
    for key, info in manifest.items():
        parts = str(key).split("_", 1)
        if len(parts) != 2:
            continue
        try:
            manifest_year = int(parts[0])
            roster_id = int(parts[1])
        except (TypeError, ValueError):
            continue
        if manifest_year != year or roster_id in roster_map:
            continue
        manager_name = _manager_name(ctx, _normalize_text(info.get("manager")) or "Unknown")
        roster_map[roster_id] = {
            "manager_name": manager_name,
            "manager_guid": _normalize_text(info.get("manager_guid")),
            "team_name": _normalize_text(info.get("team_name")) or manager_name,
        }

    if roster_map:
        _emit(log_fn, f"  Loaded {len(roster_map)} roster mappings from manager manifest for {year}")
    return roster_map


def build_year_roster_map(
    ctx: SleeperContext,
    client: SleeperAPIClient,
    league_id: str,
    year: int,
    db=None,
    required_roster_ids: set[int] | None = None,
    log_fn: LogFn | None = None,
) -> dict[int, dict[str, str]]:
    """Build the most complete roster map possible for a Sleeper season."""
    roster_map = load_matchup_roster_map(ctx, year, db=db, log_fn=log_fn)
    if not roster_map:
        roster_map = load_manifest_roster_map(ctx, year, log_fn=log_fn)

    current_api_map = build_api_roster_map(ctx, client, league_id)
    for roster_id, info in current_api_map.items():
        roster_map[roster_id] = _merge_entry(roster_map.get(roster_id), info)

    unresolved = set(required_roster_ids or set()) - set(roster_map)
    if not unresolved:
        return roster_map

    prior_years = sorted({int(y) for y in ctx.league_ids.keys() if int(y) < year}, reverse=True)
    for prior_year in prior_years:
        prior_league_id = ctx.get_league_id_for_year(prior_year)
        if not prior_league_id or prior_league_id == league_id:
            continue
        prior_api_map = build_api_roster_map(ctx, client, prior_league_id)
        matched_ids = unresolved & set(prior_api_map)
        if matched_ids:
            for roster_id in sorted(matched_ids):
                roster_map[roster_id] = _merge_entry(roster_map.get(roster_id), prior_api_map[roster_id])
            unresolved -= matched_ids
            _emit(
                log_fn,
                f"  Resolved {len(matched_ids)} retired roster id(s) for {year} from prior league {prior_year}",
            )
        if not unresolved:
            break

    if unresolved:
        for roster_id in sorted(unresolved):
            roster_map[roster_id] = _synthetic_roster_entry(ctx, roster_id)
        _emit(
            log_fn,
            f"  [WARN] Using synthetic identity for {len(unresolved)} unresolved Sleeper roster id(s): {sorted(unresolved)}",
        )

    return roster_map
