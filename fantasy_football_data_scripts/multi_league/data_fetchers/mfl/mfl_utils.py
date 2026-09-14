"""Shared MFL parsing helpers.

Payload facts (live-verified 2026-07-17 against league 2024:63886):
- MFL JSON collapses single-element lists to bare dicts (history.league,
  matchup, draftUnit, ...). Use as_list() everywhere.
- weeklyResults: {week, matchup: [{regularSeason: "1"/"0", franchise: [a, b]}],
  franchise: [...]} where the flat franchise list holds teams without a
  matchup that week (playoff byes). Each franchise entry: {id "0001", score,
  result "W"/"L"/"T", isHome, opt_pts, optimal, starters, nonstarters,
  player: [{id, score, status "starter"/"nonstarter", shouldStart}]}.
- players DB records: {id, name "Last, First", position, team, sportsdata_id
  (sportradar UUID), espn_id, nfl_id, stats_global_id, cbs_id, rotowire_id}.
- Franchise identity: league.franchises.franchise [{id "0001", name, ...}].
  The 4-digit franchise id IS stable within a league across years, while the
  league id namespace changes per year -> franchise_id keys off the SEED
  league id passed on the CLI.
"""

from __future__ import annotations

import datetime as dt
import hashlib
from typing import Any


def get_any(mapping: dict | None, *keys):
    if not isinstance(mapping, dict):
        return None
    for key in keys:
        if key in mapping:
            return mapping.get(key)
    return None


def as_list(value) -> list:
    """MFL collapses single-element JSON lists to dicts; normalize to a list."""
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def text_of(value) -> str | None:
    """Unwrap MFL's XML-ish {"$t": "..."} JSON leaves."""
    if isinstance(value, dict):
        value = value.get("$t")
    if value is None:
        return None
    return str(value)


def value_of(value) -> float | None:
    if isinstance(value, dict):
        value = get_any(value, "$t", "value")
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


def split_ids(csv_text) -> list[str]:
    """Split MFL's trailing-comma CSV id strings ("123,456,") into clean ids."""
    if csv_text is None:
        return []
    return [token.strip() for token in str(csv_text).split(",") if token.strip()]


def is_player_id(token: str | None) -> bool:
    """True for MFL player ids (numeric); False for pick tokens (FP_/DP_...)."""
    return bool(token) and str(token).strip().isdigit()


def flip_name(name: str | None) -> str | None:
    """MFL player DB names are "Last, First" -> flip to "First Last"."""
    if not name:
        return None
    text = str(name).strip()
    if "," in text:
        last, _, first = text.partition(",")
        flipped = f"{first.strip()} {last.strip()}".strip()
        return flipped or text
    return text


# MFL 3-letter team codes that differ from our standard abbreviations.
MFL_TO_STANDARD_TEAM: dict[str, str] = {
    "GBP": "GB",
    "KCC": "KC",
    "NEP": "NE",
    "NOS": "NO",
    "SFO": "SF",
    "TBB": "TB",
    "LVR": "LV",
    "SDC": "SD",
    "JAC": "JAX",
    "RAM": "LAR",
    "HST": "HOU",
    "BLT": "BAL",
    "CLV": "CLE",
    "ARZ": "ARI",
}

_DEF_POSITION_TOKENS = {"DEF", "DF", "DST", "D/ST", "TMDF"}


def normalize_nfl_team(abbrev) -> str | None:
    team = clean_id(abbrev)
    if not team:
        return None
    team = team.upper()
    return MFL_TO_STANDARD_TEAM.get(team, team)


def normalize_mfl_position(position) -> str | None:
    pos = clean_id(position)
    if not pos:
        return None
    upper = pos.upper()
    if upper in _DEF_POSITION_TOKENS:
        return "DEF"
    if upper == "PK":
        return "K"
    return upper


def franchise_identity(
    franchise: dict | None,
    seed_league_id: str,
    fallback_lookup: dict[str, dict] | None = None,
) -> dict[str, str | None]:
    """Build canonical team identity from an MFL franchise dict.

    franchise_id uses the SEED league id (the CLI-provided one) because MFL
    league ids are per-season namespaces while the 0001-style franchise id is
    stable within the league across years.
    """
    franchise = dict(franchise or {})
    franchise_num = clean_id(get_any(franchise, "id", "franchise_id"))
    if fallback_lookup and franchise_num in fallback_lookup:
        merged = dict(fallback_lookup[franchise_num])
        merged.update({k: v for k, v in franchise.items() if v is not None})
        franchise = merged
    team_name = get_any(franchise, "name")
    owner_name = clean_id(get_any(franchise, "owner_name", "ownerName", "username"))
    franchise_id = f"mfl_team_{seed_league_id}_{franchise_num}" if franchise_num else None
    manager_guid = f"mfl_owner_{owner_name}" if owner_name else franchise_id
    return {
        "team_key": franchise_num,
        "team_name": team_name,
        "team_logo": get_any(franchise, "icon", "logo"),
        "manager": owner_name or team_name,
        "manager_guid": manager_guid,
        "franchise_id": franchise_id,
        "waiver_rank": get_any(franchise, "waiverSortOrder", "waiver_sort_order"),
        "division_id": clean_id(get_any(franchise, "division")),
    }


def franchise_lookup_from_league(league_payload: dict | None) -> dict[str, dict]:
    lookup: dict[str, dict] = {}
    franchises = get_any((league_payload or {}).get("franchises") or {}, "franchise")
    for franchise in as_list(franchises):
        if not isinstance(franchise, dict):
            continue
        franchise_num = clean_id(franchise.get("id"))
        if franchise_num:
            lookup[franchise_num] = franchise
    return lookup


def player_fields(
    player_id: str | None,
    players_db: dict[str, dict] | None,
    year: int | None = None,
) -> dict[str, Any]:
    """Resolve an MFL player id to canonical player columns via the player DB."""
    record = (players_db or {}).get(str(player_id)) if player_id else None
    record = record or {}
    position = normalize_mfl_position(record.get("position"))
    nfl_team = normalize_nfl_team(record.get("team"))
    nfl_player_id = None
    if position == "DEF" and nfl_team and year:
        try:
            from nfl_data.nfl_franchises import get_def_player_id

            nfl_player_id = get_def_player_id(str(nfl_team).strip().upper(), int(year))
        except Exception:
            nfl_player_id = None
    return {
        "player": flip_name(record.get("name")),
        "position": position,
        "nfl_team_api": nfl_team,
        "mfl_player_id": clean_id(player_id),
        "NFL_player_id": nfl_player_id,
        "eligible_positions": position,
        "sportradar_id": clean_id(record.get("sportsdata_id")),
    }


def result_flags(result: str | None, team_points: float, opponent_points: float) -> tuple[int, int, int]:
    """W/L/T flags from MFL's single-letter result, falling back to scores."""
    normalized = str(result or "").strip().upper()
    if normalized in {"W", "WIN"}:
        return 1, 0, 0
    if normalized in {"L", "LOSE", "LOSS"}:
        return 0, 1, 0
    if normalized in {"T", "TIE"}:
        return 0, 0, 1
    if team_points == opponent_points and team_points > 0:
        return 0, 0, 1
    if team_points > opponent_points:
        return 1, 0, 0
    if team_points < opponent_points:
        return 0, 1, 0
    return 0, 0, 0


def season_week_starts(year: int, num_weeks: int = 18) -> list[tuple[int, int]]:
    """Approximate NFL week windows: week 1 opens the Tuesday before the first
    Thursday of September; each week advances 7 days.

    MFL has no per-league schedule-period epochs (unlike Fleaflicker), so
    transaction week attribution uses these calendar windows.
    """
    first = dt.datetime(int(year), 9, 1, tzinfo=dt.timezone.utc)
    days_until_thursday = (3 - first.weekday()) % 7
    kickoff_thursday = first + dt.timedelta(days=days_until_thursday)
    week1_start = kickoff_thursday - dt.timedelta(days=2)
    starts: list[tuple[int, int]] = []
    for week in range(1, num_weeks + 1):
        start = week1_start + dt.timedelta(days=7 * (week - 1))
        starts.append((week, int(start.timestamp())))
    return starts


def week_for_timestamp(timestamp_s: int | None, period_starts: list[tuple[int, int]]) -> int | None:
    if timestamp_s is None or not period_starts:
        return None
    selected = period_starts[0][0]
    for week, start_s in period_starts:
        if timestamp_s >= start_s:
            selected = week
        else:
            break
    return selected
