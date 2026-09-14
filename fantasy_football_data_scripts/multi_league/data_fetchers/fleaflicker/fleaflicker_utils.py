"""Shared Fleaflicker parsing helpers."""

from __future__ import annotations

import hashlib
from typing import Any


def get_any(mapping: dict | None, *keys):
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            return mapping.get(key)
    return None


def value_of(value) -> float | None:
    if isinstance(value, dict):
        value = get_any(value, "value")
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def clean_id(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"none", "nan", "<na>", "null"}:
        return None
    return text.removesuffix(".0")


def stable_hash(*parts: Any) -> str:
    raw = "\x1f".join("" if part is None else str(part) for part in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:20]


def first_owner(team: dict | None) -> dict:
    owners = get_any(team, "owners") or []
    if isinstance(owners, list) and owners:
        return owners[0] if isinstance(owners[0], dict) else {}
    if isinstance(owners, dict):
        return owners
    return {}


def team_identity(team: dict | None, fallback_lookup: dict[str, dict] | None = None) -> dict[str, str | None]:
    team = dict(team or {})
    team_id = clean_id(get_any(team, "id"))
    if fallback_lookup and team_id in fallback_lookup:
        merged = dict(fallback_lookup[team_id])
        merged.update({k: v for k, v in team.items() if v is not None})
        team = merged
    owner = first_owner(team)
    owner_id = clean_id(get_any(owner, "id"))
    team_name = get_any(team, "name")
    manager = get_any(owner, "displayName", "display_name", "name") or team_name
    franchise_id = f"ff_team_{team_id}" if team_id else None
    manager_guid = f"ff_owner_{owner_id}" if owner_id else franchise_id
    return {
        "team_key": team_id,
        "team_name": team_name,
        "team_logo": get_any(team, "logoUrl", "logo_url"),
        "manager": manager,
        "manager_guid": manager_guid,
        "franchise_id": franchise_id,
        "waiver_rank": get_any(team, "waiverPosition", "waiver_position"),
        "division_id": clean_id(get_any(team, "divisionId", "division_id")),
    }


def team_lookup_from_standings(standings: dict | None) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    for division in (standings or {}).get("divisions") or []:
        if not isinstance(division, dict):
            continue
        division_id = clean_id(get_any(division, "id"))
        for team in division.get("teams") or []:
            if not isinstance(team, dict):
                continue
            team_id = clean_id(get_any(team, "id"))
            if team_id:
                enriched = dict(team)
                enriched["divisionId"] = division_id
                lookup[team_id] = enriched
    return lookup


def score_value(score: dict | None) -> float:
    if not isinstance(score, dict):
        return 0.0
    return value_of(get_any(score, "score")) or value_of(score) or 0.0


def result_flags(result: str | None, team_points: float, opponent_points: float) -> tuple[int, int, int]:
    normalized = str(result or "").upper()
    if normalized == "WIN":
        return 1, 0, 0
    if normalized == "LOSE":
        return 0, 1, 0
    if normalized == "TIE":
        return 0, 0, 1
    if team_points == opponent_points and team_points > 0:
        return 0, 0, 1
    if team_points > opponent_points:
        return 1, 0, 0
    if team_points < opponent_points:
        return 0, 1, 0
    return 0, 0, 0


def pro_player(payload: dict | None) -> dict:
    if not isinstance(payload, dict):
        return {}
    if isinstance(payload.get("proPlayer"), dict):
        return payload["proPlayer"]
    if isinstance(payload.get("pro_player"), dict):
        return payload["pro_player"]
    return payload


def player_fields(player_payload: dict | None, year: int | None = None) -> dict[str, Any]:
    player = pro_player(player_payload)
    player_id = clean_id(get_any(player, "id"))
    position = get_any(player, "position")
    if position in {"D/ST", "DST"}:
        position = "DEF"
    nfl_team = get_any(player, "proTeamAbbreviation", "pro_team_abbreviation")
    nfl_player_id = None
    if position == "DEF" and nfl_team and year:
        try:
            from nfl_data.nfl_franchises import get_def_player_id

            nfl_player_id = get_def_player_id(str(nfl_team).strip().upper(), int(year))
        except Exception:
            nfl_player_id = None
    eligible = get_any(player, "positionEligibility", "position_eligibility")
    if isinstance(eligible, list):
        eligible_positions = ",".join(str(pos) for pos in eligible)
    else:
        eligible_positions = str(eligible) if eligible else None
    return {
        "player": get_any(player, "nameFull", "name_full", "name"),
        "position": position,
        "nfl_team_api": nfl_team,
        "fleaflicker_player_id": player_id,
        "NFL_player_id": nfl_player_id,
        "eligible_positions": eligible_positions,
    }


def week_bounds_from_scoreboard(scoreboard: dict | None) -> tuple[int, int]:
    periods = (
        (scoreboard or {}).get("eligibleSchedulePeriods") or (scoreboard or {}).get("eligible_schedule_periods") or []
    )
    weeks = []
    for period in periods:
        if not isinstance(period, dict):
            continue
        raw = get_any(period, "value", "ordinal")
        try:
            weeks.append(int(raw))
        except (TypeError, ValueError):
            continue
    if not weeks:
        return 1, 17
    return min(weeks), max(weeks)


def period_starts_from_scoreboard(scoreboard: dict | None) -> list[tuple[int, int]]:
    periods = (
        (scoreboard or {}).get("eligibleSchedulePeriods") or (scoreboard or {}).get("eligible_schedule_periods") or []
    )
    starts: list[tuple[int, int]] = []
    for period in periods:
        if not isinstance(period, dict):
            continue
        week = clean_id(get_any(period, "value", "ordinal"))
        low = get_any(period, "low") or period
        start = clean_id(get_any(low, "startEpochMilli", "start_epoch_milli"))
        try:
            if week and start:
                starts.append((int(week), int(start)))
        except (TypeError, ValueError):
            continue
    return sorted(starts, key=lambda item: item[1])


def week_for_timestamp(timestamp_ms: int | None, period_starts: list[tuple[int, int]]) -> int | None:
    if timestamp_ms is None or not period_starts:
        return None
    selected = period_starts[0][0]
    for week, start_ms in period_starts:
        if timestamp_ms >= start_ms:
            selected = week
        else:
            break
    return selected
