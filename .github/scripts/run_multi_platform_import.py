#!/usr/bin/env python3
"""Run an ordered multi-platform full import.

Payload contract:

{
  "target_db": "fantasy_elite",
  "league_name": "Fantasy Elite",
  "target_segment_index": 2,          # optional; defaults to last segment
  "segments": [
    {
      "platform": "yahoo",
      "database_name": "fantasy_elite_yahoo_ab12cd",
      "league_name": "Fantasy Elite",
      "league_id": "406.l.167585",
      "start_year": 2010,
      "end_year": 2019,
      "league_ids": {"2010": "242.l.331113"},
      "oauth_token": {"access_token": "...", "refresh_token": "..."},
      "manager_mapping": {"Old Yahoo Name": "Current Target Name"}
    },
    {
      "platform": "espn",
      "database_name": "fantasy_elite_espn_ef34ab",
      "espn_league_id": 71580,
      "start_year": 2020,
      "end_year": 2021,
      "league_ids": {"2020": 71580}
    },
    {
      "platform": "sleeper",
      "database_name": "fantasy_elite",
      "sleeper_league_id": "1257088277819691008",
      "league_name": "Fantasy Elite",
      "start_year": 2022,
      "end_year": 2025,
      "league_ids": {"2022": "936..."}
    }
  ]
}

Each segment is imported into its own local DuckDB. The selected source years
are copied into the target segment's local DuckDB, then the combined target is
post-processed and uploaded once. Older one-off Yahoo->Sleeper payloads are
normalized into this shape.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path
from typing import Any


REPO_ROOT = Path.cwd()
FANTASY_DIR = REPO_ROOT / "fantasy_football_data_scripts"
LOG_DIR = REPO_ROOT / "multi_platform_logs"
DB_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]{0,62}$")
SUPPORTED_PLATFORMS = {"yahoo", "sleeper", "espn"}


def log(message: str = "") -> None:
    print(message, flush=True)


def fail(message: str) -> None:
    raise SystemExit(f"[multi-platform] {message}")


def sanitize_db_name(value: str) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9_]+", "_", raw)
    raw = re.sub(r"_+", "_", raw).strip("_")
    if not raw:
        raw = "league"
    if raw[0].isdigit():
        raw = f"league_{raw}"
    return raw[:63]


def assert_db_name(value: str, label: str) -> str:
    db_name = str(value or "").strip()
    if not DB_RE.match(db_name):
        fail(f"{label} must be a safe db_name, got {value!r}")
    return db_name


def _sql_literal(value: object) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def derive_source_db(target_db: str, platform: str, stable_id: str, ordinal: int) -> str:
    digest = hashlib.md5(f"{platform}:{stable_id}:{ordinal}".encode()).hexdigest()[:6]
    base = sanitize_db_name(f"{target_db}_{platform}")
    return f"{base[: 63 - len(digest) - 1]}_{digest}"


def clean_year_id_map(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    cleaned: dict[str, str] = {}
    for year, league_id in raw.items():
        try:
            numeric_year = int(year)
        except (TypeError, ValueError):
            continue
        value = str(league_id).strip()
        if value:
            cleaned[str(numeric_year)] = value
    return dict(sorted(cleaned.items(), key=lambda item: int(item[0])))


def clean_manager_mapping(raw: Any) -> dict[str, str]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return {}
    if not isinstance(raw, dict):
        return {}
    cleaned: dict[str, str] = {}
    for source, target in raw.items():
        source_name = str(source or "").strip()
        target_name = str(target or "").strip()
        if source_name and target_name and target_name != "__none__" and source_name != target_name:
            cleaned[source_name] = target_name
    return cleaned


def clean_franchise_merges(raw: Any) -> list[dict[str, Any]]:
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []

    cleaned: list[dict[str, Any]] = []
    for entry in raw:
        if not isinstance(entry, dict):
            continue
        owner_ids = []
        for owner_id in entry.get("owner_ids") or []:
            owner = str(owner_id or "").strip()
            if owner and owner not in owner_ids:
                owner_ids.append(owner)
        franchise_ids = []
        for franchise_id in entry.get("franchise_ids") or []:
            fid = str(franchise_id or "").strip()
            if fid and fid not in franchise_ids:
                franchise_ids.append(fid)
        if len(owner_ids) < 2 and not franchise_ids:
            continue
        cleaned.append(
            {
                **entry,
                "owner_ids": owner_ids,
                "franchise_ids": franchise_ids,
            }
        )
    return cleaned


def load_existing_manager_settings(target_db: str) -> dict[str, Any]:
    """Read live manager settings so full reimports do not wipe UI merges."""
    server_url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    read_token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not server_url or not read_token:
        return {}

    select_cols = "manager_name_overrides_json, franchise_merges_json"
    fallback_select_cols = "manager_name_overrides_json"
    for cols in (select_cols, fallback_select_cols):
        sql = f"SELECT {cols} " "FROM public.league_context " f"WHERE db_name = {_sql_literal(target_db)} " "LIMIT 1"
        payload = json.dumps({"database": "___leagues", "sql": sql}).encode("utf-8")
        request = urllib.request.Request(
            f"{server_url}/query",
            data=payload,
            headers={
                "Authorization": f"Bearer {read_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                rows = json.loads(response.read().decode("utf-8"))
        except Exception as exc:
            if cols == select_cols:
                continue
            log(f"[settings] WARN: could not load existing manager settings for {target_db}: {exc}")
            return {}

        if not isinstance(rows, list) or not rows:
            return {}
        row = rows[0]
        return {
            "manager_name_overrides": clean_manager_mapping(row.get("manager_name_overrides_json")),
            "franchise_merges": clean_franchise_merges(row.get("franchise_merges_json")),
        }

    return {}


def load_existing_manager_overrides(target_db: str) -> dict[str, str]:
    """Read live manager overrides so full reimports do not wipe UI merges."""
    return dict(load_existing_manager_settings(target_db).get("manager_name_overrides") or {})


def years_from_segment(segment: dict[str, Any]) -> list[int]:
    if isinstance(segment.get("merge_years"), list):
        years = [int(y) for y in segment["merge_years"] if str(y).strip()]
        return sorted(set(years))
    league_ids = clean_year_id_map(segment.get("league_ids"))
    if league_ids:
        return [int(year) for year in league_ids]
    start = int(segment.get("start_year") or segment.get("season") or 0)
    end = int(segment.get("end_year") or segment.get("season") or start)
    if start <= 0 or end <= 0:
        return []
    return list(range(start, end + 1))


def year_in_sql(years: list[int]) -> str:
    return ", ".join(str(int(year)) for year in sorted(set(years)))


def duckdb_string(value: object) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def duckdb_ident(value: object) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def segment_stable_id(segment: dict[str, Any]) -> str:
    return str(
        segment.get("league_key")
        or segment.get("league_id")
        or segment.get("sleeper_league_id")
        or segment.get("espn_league_id")
        or segment.get("league_name")
        or "segment"
    )


def normalize_legacy_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Convert the first Yahoo->Sleeper payload into generic segments."""
    if any(isinstance(payload.get(key), list) for key in ("segments", "platform_segments", "multi_platform_segments")):
        return payload

    yahoo = payload.get("yahoo") if isinstance(payload.get("yahoo"), dict) else {}
    if not yahoo and not payload.get("yahoo_source_db"):
        return payload

    yahoo_league_ids = clean_year_id_map(yahoo.get("league_ids") or payload.get("yahoo_league_ids"))
    yahoo_years = [int(year) for year in yahoo_league_ids] or [
        int(yahoo.get("start_year") or payload.get("yahoo_start_year") or 0),
        int(yahoo.get("end_year") or payload.get("yahoo_end_year") or 0),
    ]
    yahoo_years = sorted({year for year in yahoo_years if year > 0})
    sleeper_league_ids = clean_year_id_map(payload.get("league_ids"))

    source_segment = {
        "platform": "yahoo",
        "database_name": payload.get("yahoo_source_db") or payload.get("source_db"),
        "league_id": yahoo.get("league_id") or yahoo.get("league_key") or payload.get("yahoo_league_id"),
        "league_key": yahoo.get("league_key") or yahoo.get("league_id") or payload.get("yahoo_league_id"),
        "league_name": yahoo.get("league_name") or payload.get("league_name"),
        "start_year": yahoo.get("start_year") or (min(yahoo_years) if yahoo_years else None),
        "end_year": yahoo.get("end_year") or (max(yahoo_years) if yahoo_years else None),
        "num_teams": yahoo.get("num_teams") or payload.get("num_teams"),
        "league_ids": yahoo_league_ids,
        "oauth_token": yahoo.get("oauth_token") or payload.get("yahoo_oauth_token") or payload.get("oauth_token"),
        "manager_mapping": payload.get("manager_mapping") or {},
        "merge_years": payload.get("merge_years") or yahoo_years,
    }
    target_segment = {
        "platform": "sleeper",
        "database_name": payload.get("target_db") or payload.get("database_name"),
        "league_id": payload.get("sleeper_league_id") or payload.get("league_id"),
        "sleeper_league_id": payload.get("sleeper_league_id") or payload.get("league_id"),
        "league_name": payload.get("league_name"),
        "season": payload.get("season"),
        "start_year": payload.get("start_year"),
        "end_year": payload.get("end_year") or payload.get("season"),
        "num_teams": payload.get("num_teams"),
        "league_ids": sleeper_league_ids,
    }

    normalized = dict(payload)
    normalized["segments"] = [source_segment, target_segment]
    normalized["target_segment_index"] = 1
    normalized["target_db"] = payload.get("target_db") or payload.get("database_name")
    normalized["target_platform"] = "sleeper"
    return normalized


def load_payload(args: argparse.Namespace) -> dict[str, Any]:
    if args.payload:
        raw = Path(args.payload).read_text(encoding="utf-8-sig").strip()
    else:
        raw = os.environ.get("IMPORT_DATA_B64", "").strip()
    if not raw:
        fail("payload file or IMPORT_DATA_B64 is required")
    try:
        if raw.lstrip().startswith("{"):
            payload = json.loads(raw)
        else:
            payload = json.loads(base64.b64decode(raw).decode("utf-8"))
    except Exception as exc:
        fail(f"could not parse payload: {exc}")
    if not isinstance(payload, dict):
        fail("payload must be a JSON object")
    return normalize_legacy_payload(payload)


def normalize_segments(payload: dict[str, Any]) -> tuple[list[dict[str, Any]], int, str]:
    raw_segments = payload.get("segments") or payload.get("platform_segments") or payload.get("multi_platform_segments")
    if not isinstance(raw_segments, list) or len(raw_segments) < 2:
        fail("multi-platform payload requires at least two ordered segments")

    target_index = payload.get("target_segment_index")
    if target_index is None:
        target_index = next(
            (
                idx
                for idx, segment in enumerate(raw_segments)
                if isinstance(segment, dict) and (segment.get("role") == "target" or segment.get("target") is True)
            ),
            len(raw_segments) - 1,
        )
    target_index = int(target_index)
    if target_index < 0 or target_index >= len(raw_segments):
        fail(f"target_segment_index out of range: {target_index}")

    target_db = assert_db_name(
        payload.get("target_db")
        or payload.get("database_name")
        or raw_segments[target_index].get("database_name")
        or raw_segments[target_index].get("db_name"),
        "target_db",
    )
    existing_manager_settings = load_existing_manager_settings(target_db)
    existing_manager_overrides = dict(existing_manager_settings.get("manager_name_overrides") or {})
    existing_franchise_merges = list(existing_manager_settings.get("franchise_merges") or [])

    shared_defaults = {
        key: payload.get(key) for key in ("manager_name_overrides", "franchise_merges") if payload.get(key) is not None
    }
    target_defaults = {
        key: payload.get(key)
        for key in (
            "keeper_rules",
            "league_rules",
            "standings_weights",
            "manager_name_overrides",
            "franchise_merges",
            "external_column_maps",
            "external_identity_maps",
            "is_private",
        )
        if payload.get(key) is not None
    }
    if existing_manager_overrides and "manager_name_overrides" not in target_defaults:
        target_defaults["manager_name_overrides"] = existing_manager_overrides
    if existing_franchise_merges and "franchise_merges" not in target_defaults:
        target_defaults["franchise_merges"] = existing_franchise_merges

    normalized: list[dict[str, Any]] = []
    claimed_years: dict[int, str] = {}
    for idx, raw in enumerate(raw_segments):
        if not isinstance(raw, dict):
            fail(f"segment {idx + 1} must be an object")
        segment = dict(raw)
        platform = str(segment.get("platform") or "").strip().lower()
        if platform not in SUPPORTED_PLATFORMS:
            fail(f"segment {idx + 1} platform must be one of {sorted(SUPPORTED_PLATFORMS)}, got {platform!r}")
        segment["platform"] = platform
        if platform == "yahoo":
            try:
                sys.path.insert(0, str(FANTASY_DIR))
                from multi_league.utils.yahoo_auth_mode import resolve_yahoo_auth_mode

                auth_mode = resolve_yahoo_auth_mode(
                    segment.get("auth_mode") or segment.get("yahoo_auth_mode"),
                    segment.get("stored_auth_mode"),
                ).value
            except ValueError as exc:
                fail(f"segment {idx + 1} Yahoo auth_mode is invalid: {exc}")
            segment["auth_mode"] = auth_mode
        segment["league_ids"] = clean_year_id_map(segment.get("league_ids"))
        segment["merge_years"] = years_from_segment(segment)
        if not segment["merge_years"]:
            fail(f"segment {idx + 1} ({platform}) has no import/merge years")
        for key, value in shared_defaults.items():
            segment.setdefault(key, value)
        for year in segment["merge_years"]:
            if year in claimed_years:
                fail(
                    f"year {year} is present in both {claimed_years[year]} "
                    f"and segment {idx + 1} ({platform}); multi-platform segments must not overlap"
                )
            claimed_years[year] = f"segment {idx + 1} ({platform})"

        if idx == target_index:
            db_name = target_db
            segment["role"] = "target"
            for key, value in target_defaults.items():
                segment.setdefault(key, value)
        else:
            db_name = (
                segment.get("database_name")
                or segment.get("db_name")
                or segment.get("source_db")
                or derive_source_db(target_db, platform, segment_stable_id(segment), idx + 1)
            )
            segment["role"] = "source"
        segment["database_name"] = assert_db_name(db_name, f"segment {idx + 1} database_name")
        if segment["database_name"] == target_db and idx != target_index:
            fail(f"source segment {idx + 1} database_name cannot equal target_db")
        if idx != target_index and reuse_existing_source(segment):
            fail(
                f"source segment {idx + 1} requested reuse_existing_source/skip_import, "
                "but multi-platform imports now merge local segment imports. "
                "Remove the skip flag so the platform importer can build the source segment locally."
            )

        normalized.append(segment)

    merge_sources = payload.get("merge_sources")
    if not isinstance(merge_sources, list) or not merge_sources:
        merge_sources = []
        for idx, segment in enumerate(normalized):
            if idx == target_index:
                continue
            merge_sources.append(
                {
                    "source_db": segment["database_name"],
                    "source_platform": segment["platform"],
                    "merge_years": segment["merge_years"],
                    "manager_mapping": segment.get("manager_mapping") or {},
                }
            )
    normalized[target_index]["merge_sources"] = merge_sources
    normalized[target_index]["merge_source"] = merge_sources[0] if merge_sources else None
    normalized[target_index]["has_external_data"] = True
    return normalized, target_index, target_db


def sanitized_payload(payload: dict[str, Any]) -> dict[str, Any]:
    redacted = json.loads(json.dumps(payload, default=str))
    for segment in redacted.get("segments", []):
        if isinstance(segment, dict):
            segment.pop("oauth_token", None)
            segment.pop("espn_s2", None)
            segment.pop("swid", None)
            credentials = segment.get("credentials")
            if isinstance(credentials, dict):
                credentials.pop("oauth_token", None)
                credentials.pop("espn_s2", None)
                credentials.pop("swid", None)
    for key in ("oauth_token", "yahoo_oauth_token", "espn_s2", "swid"):
        redacted.pop(key, None)
    return redacted


def reuse_existing_source(segment: dict[str, Any]) -> bool:
    return bool(
        segment.get("reuse_existing_source")
        or segment.get("existing_source")
        or segment.get("skip_source_import")
        or segment.get("skip_import")
    )


def platform_league_id(segment: dict[str, Any]) -> str:
    platform = segment["platform"]
    if platform == "yahoo":
        return str(segment.get("league_key") or segment.get("league_id") or "").strip()
    if platform == "sleeper":
        return str(segment.get("sleeper_league_id") or segment.get("league_id") or "").strip()
    if platform == "espn":
        return str(segment.get("espn_league_id") or segment.get("league_id") or "").strip()
    return ""


def resolve_yahoo_oauth_token(segment: dict[str, Any], index: int, league_id: str) -> dict[str, Any]:
    oauth_token = segment.get("oauth_token")
    if not oauth_token and isinstance(segment.get("credentials"), dict):
        oauth_token = segment["credentials"].get("oauth_token")
    if isinstance(oauth_token, dict) and oauth_token.get("refresh_token"):
        return oauth_token

    try:
        sys.path.insert(0, str(FANTASY_DIR))
        from multi_league.utils.credential_store import get_league_credentials, retrieve_league_credentials

        checked: list[str] = []
        for db_name in (
            segment.get("credential_database_name"),
            segment.get("database_name"),
        ):
            safe_db = str(db_name or "").strip()
            if not safe_db or safe_db in checked:
                continue
            checked.append(safe_db)
            credentials = retrieve_league_credentials(safe_db)
            if credentials and credentials.get("refresh_token"):
                log(f"[credentials] Yahoo segment {index} using stored credential for {safe_db}")
                return {
                    "access_token": "expired",
                    "refresh_token": credentials["refresh_token"],
                    "token_type": "bearer",
                    "token_time": 0,
                }

        if league_id:
            refresh_token = get_league_credentials(league_id)
            if refresh_token:
                log(f"[credentials] Yahoo segment {index} using stored credential for league_id {league_id}")
                return {
                    "access_token": "expired",
                    "refresh_token": refresh_token,
                    "token_type": "bearer",
                    "token_time": 0,
                }
    except Exception as exc:
        log(f"[credentials] WARN: could not load stored Yahoo credential for segment {index}: {exc}")

    fail(f"Yahoo segment {index} requires oauth_token.refresh_token or stored credentials")


def resolve_espn_credentials(segment: dict[str, Any], index: int) -> tuple[str, str]:
    espn_s2 = segment.get("espn_s2")
    swid = segment.get("swid")
    credentials = segment.get("credentials") if isinstance(segment.get("credentials"), dict) else {}
    if not espn_s2:
        espn_s2 = credentials.get("espn_s2")
    if not swid:
        swid = credentials.get("swid")
    if espn_s2 and swid:
        return str(espn_s2), str(swid)

    try:
        sys.path.insert(0, str(FANTASY_DIR))
        from multi_league.utils.credential_store import (
            retrieve_espn_credentials,
            retrieve_espn_credentials_by_league_id,
        )

        checked: list[str] = []
        for db_name in (
            segment.get("credential_database_name"),
            segment.get("database_name"),
        ):
            safe_db = str(db_name or "").strip()
            if not safe_db or safe_db in checked:
                continue
            checked.append(safe_db)
            stored = retrieve_espn_credentials(safe_db)
            if stored and stored.get("espn_s2") and stored.get("swid"):
                log(f"[credentials] ESPN segment {index} using stored credentials for {safe_db}")
                return str(stored["espn_s2"]), str(stored["swid"])

        league_id = platform_league_id(segment)
        if league_id:
            stored = retrieve_espn_credentials_by_league_id(league_id)
            if stored and stored.get("espn_s2") and stored.get("swid"):
                log(
                    f"[credentials] ESPN segment {index} using stored credentials "
                    f"for league_id {league_id} ({stored.get('database_name')})"
                )
                return str(stored["espn_s2"]), str(stored["swid"])
    except Exception as exc:
        log(f"[credentials] WARN: could not load stored ESPN credentials for segment {index}: {exc}")

    return str(espn_s2 or ""), str(swid or "")


def segment_year_bounds(segment: dict[str, Any]) -> tuple[int, int]:
    years = years_from_segment(segment)
    start = int(segment.get("start_year") or min(years))
    end = int(segment.get("end_year") or segment.get("season") or max(years))
    return start, end


def write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")


def build_context(segment: dict[str, Any], index: int) -> tuple[Path, Path]:
    platform = segment["platform"]
    db_name = segment["database_name"]
    start_year, end_year = segment_year_bounds(segment)
    league_name = str(segment.get("league_name") or db_name)
    league_id = platform_league_id(segment)
    if not league_id:
        fail(f"segment {index} ({platform}) missing league id")

    data_dir = (REPO_ROOT / "fantasy_football_data" / f"{platform}_{db_name}").resolve()
    for child in ("league_settings", "player_data", "matchup_data", "draft_data", "transaction_data", "schedule_data"):
        (data_dir / child).mkdir(parents=True, exist_ok=True)

    common = {
        "league_name": league_name,
        "database_name": db_name,
        "start_year": start_year,
        "end_year": end_year,
        "num_teams": segment.get("num_teams"),
        "playoff_teams": segment.get("playoff_teams", 6),
        "regular_season_weeks": segment.get("regular_season_weeks"),
        "data_directory": str(data_dir),
        "manager_name_overrides": clean_manager_mapping(segment.get("manager_name_overrides") or {}),
        "franchise_merges": clean_franchise_merges(segment.get("franchise_merges") or []),
        "external_column_maps": segment.get("external_column_maps") or [],
        "external_identity_maps": segment.get("external_identity_maps") or {},
        "has_external_data": bool(segment.get("has_external_data")),
        "merge_source": segment.get("merge_source"),
        "merge_sources": segment.get("merge_sources") or [],
        "created_at": datetime.now().isoformat(),
        "updated_at": datetime.now().isoformat(),
        "import_mode": "full",
        "keeper_rules": segment.get("keeper_rules"),
        "league_rules": segment.get("league_rules"),
        "standings_weights": segment.get("standings_weights"),
        "league_ids": segment.get("league_ids") or {},
    }

    if platform == "yahoo":
        auth_mode = str(segment.get("auth_mode") or segment.get("yahoo_auth_mode") or "oauth").strip().lower()
        if auth_mode not in {"oauth", "cookie"}:
            fail(f"segment {index} Yahoo auth_mode must be oauth or cookie, got {auth_mode!r}")
        oauth_token = resolve_yahoo_oauth_token(segment, index, league_id) if auth_mode == "oauth" else None
        access_token = oauth_token.get("access_token") or "expired" if oauth_token else "expired"
        token_time = oauth_token.get("token_time") if oauth_token else None
        if token_time is None:
            token_time = 0 if access_token == "expired" else time.time()
        oauth_file = None
        if auth_mode == "oauth":
            oauth_dir = REPO_ROOT / "oauth"
            oauth_dir.mkdir(exist_ok=True)
            oauth_file = oauth_dir / f"Oauth_segment_{index}.json"
            write_json(
                oauth_file,
                {
                    "access_token": access_token,
                    "refresh_token": oauth_token.get("refresh_token"),
                    "consumer_key": os.environ.get("YAHOO_CLIENT_ID"),
                    "consumer_secret": os.environ.get("YAHOO_CLIENT_SECRET"),
                    "token_type": oauth_token.get("token_type", "bearer"),
                    "expires_in": oauth_token.get("expires_in", 3600),
                    "token_time": token_time,
                    "guid": oauth_token.get("xoauth_yahoo_guid") or oauth_token.get("guid"),
                },
            )
        context = {
            **common,
            "league_id": league_id,
            "oauth_file_path": str(oauth_file.resolve()) if oauth_file else None,
            "game_code": segment.get("game_code", "nfl"),
            "max_workers": 5,
            "enable_caching": True,
            "rate_limit_per_sec": 4.0,
            "platform": "yahoo",
            "yahoo_auth_mode": auth_mode,
            "cookie_jar_path": (
                f"fly://___ops.main.yahoo_web_credentials/"
                f"{segment.get('credential_database_name') or segment.get('database_name')}"
                if auth_mode == "cookie"
                else None
            ),
            "require_oauth": auth_mode == "oauth",
        }
        context_path = REPO_ROOT / f"league_context_segment_{index}.json"
    elif platform == "sleeper":
        context = {
            **common,
            "league_id": league_id,
            "username": str(segment.get("username") or ""),
            "sport": "nfl",
            "max_workers": 3,
            "enable_caching": True,
            "rate_limit_per_min": 1000,
            "roster_to_manager": {},
            "roster_to_guid": {},
            "platform": "sleeper",
            "require_oauth": False,
        }
        context_path = REPO_ROOT / f"sleeper_context_segment_{index}.json"
    else:
        espn_s2, swid = resolve_espn_credentials(segment, index)
        playoff_teams = segment.get("playoff_teams", 6)
        try:
            bye_teams = (
                2 ** math.ceil(math.log2(playoff_teams)) - playoff_teams if playoff_teams and playoff_teams > 1 else 0
            )
        except Exception:
            bye_teams = segment.get("bye_teams")
        context = {
            **common,
            "league_id": int(league_id),
            "espn_s2": espn_s2 or "",
            "swid": swid or "",
            "sport": "nfl",
            "explicit_import_years": segment.get("merge_years") or [],
            "bye_teams": segment.get("bye_teams", bye_teams),
            "playoff_start_week": segment.get("playoff_start_week"),
            "uses_median_scoring": segment.get("uses_median_scoring", False),
            "max_workers": 3,
            "enable_caching": True,
            "team_to_manager": {},
            "team_to_guid": {},
            "team_to_team_name": {},
            "platform": "espn",
            "require_oauth": False,
        }
        context_path = REPO_ROOT / f"espn_context_segment_{index}.json"

    write_json(context_path, context)
    context_file_in_data_dir = data_dir / ("league_context.json" if platform == "yahoo" else f"{platform}_context.json")
    write_json(context_file_in_data_dir, context)
    log(f"[context] segment {index}: {platform} {start_year}-{end_year} -> {db_name}")
    return context_path, data_dir


def run_logged(name: str, cmd: list[str], *, cwd: Path, log_file: Path, env: dict[str, str] | None = None) -> None:
    log("")
    log("=" * 96)
    log(name)
    log("=" * 96)
    log("$ " + " ".join(cmd))
    log_file.parent.mkdir(parents=True, exist_ok=True)
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    with log_file.open("w", encoding="utf-8") as handle:
        process = subprocess.Popen(
            cmd,
            cwd=str(cwd),
            env=merged_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        assert process.stdout is not None
        for line in process.stdout:
            print(line, end="")
            handle.write(line)
        exit_code = process.wait()
    if exit_code != 0:
        fail(f"{name} failed with exit code {exit_code}; see {log_file}")


def persist_yahoo_refresh_token(segment: dict[str, Any], index: int) -> None:
    if segment.get("platform") != "yahoo" or str(segment.get("auth_mode") or segment.get("yahoo_auth_mode") or "oauth").strip().lower() != "oauth":
        return

    oauth_file = REPO_ROOT / "oauth" / f"Oauth_segment_{index}.json"
    if not oauth_file.exists():
        return
    try:
        token_data = json.loads(oauth_file.read_text(encoding="utf-8"))
    except Exception as exc:
        log(f"[credentials] WARN: could not read Yahoo OAuth file for segment {index}: {exc}")
        return

    refresh_token = str(token_data.get("refresh_token") or "").strip()
    if not refresh_token:
        return

    try:
        sys.path.insert(0, str(FANTASY_DIR))
        from multi_league.core.fly_writer import FlyWriter
        from multi_league.utils.credential_store import encrypt_token, get_encryption_key

        credential_db = str(segment.get("credential_database_name") or segment.get("database_name") or "").strip()
        league_id = str(segment.get("league_key") or segment.get("league_id") or "")
        league_name = str(segment.get("league_name") or "")
        encrypted = encrypt_token(refresh_token, get_encryption_key())
        safe_league_id = _sql_literal(league_id)
        safe_league_name = _sql_literal(league_name)
        safe_database_name = _sql_literal(credential_db)
        safe_encrypted = _sql_literal(encrypted)
        FlyWriter().execute(
            f"""
            CREATE SCHEMA IF NOT EXISTS main;
            CREATE TABLE IF NOT EXISTS main.league_credentials (
                league_id TEXT PRIMARY KEY,
                league_name TEXT,
                database_name TEXT,
                encrypted_refresh_token TEXT,
                updated_at TIMESTAMP DEFAULT current_timestamp
            );

            DELETE FROM main.league_credentials
            WHERE league_id = {safe_league_id}
               OR database_name = {safe_database_name};

            INSERT INTO main.league_credentials
                (league_id, league_name, database_name, encrypted_refresh_token, updated_at)
            VALUES
                ({safe_league_id}, {safe_league_name}, {safe_database_name}, {safe_encrypted}, current_timestamp);
            """,
            database="___ops",
        )
        log(f"[credentials] Yahoo refresh token persisted for {credential_db}")
    except Exception as exc:
        log(f"[credentials] WARN: could not persist Yahoo refresh token for segment {index}: {exc}")


def import_segment(
    segment: dict[str, Any],
    index: int,
    *,
    is_target: bool,
    persist_credentials: bool = True,
) -> tuple[Path, Path]:
    context_path, data_dir = build_context(segment, index)
    platform = segment["platform"]
    rel_context = os.path.relpath(context_path, FANTASY_DIR)
    log_path = LOG_DIR / f"{index:02d}_{platform}_{'target' if is_target else 'source'}_import.log"
    env = {"AUTO_CONFIRM": "1"}
    if platform == "yahoo":
        auth_mode = str(segment.get("auth_mode") or segment.get("yahoo_auth_mode") or "oauth").strip().lower()
        if auth_mode == "cookie":
            start_year, end_year = segment_year_bounds(segment)
            cmd = [
                "python",
                os.path.relpath(REPO_ROOT / "scripts" / "yahoo_cookie_worker.py", FANTASY_DIR),
                "--db-name", segment["database_name"],
                "--ops-cache", os.environ["OPS_CACHE_PATH"],
                "--draft-global-source",
                os.environ.get(
                    "DRAFT_GLOBAL_SOURCE_PATH",
                    str(REPO_ROOT.parent / "league-history-data" / "fantasy_leagues" / "sampling_corpus" / "github_dependencies_v1" / "draft_global_source.parquet"),
                ),
                "--output-dir", str(data_dir),
                "--league-name", str(segment.get("league_name") or segment["database_name"]),
                "--start-year", str(start_year),
                "--end-year", str(end_year),
                "--league-keys-json", json.dumps(segment.get("league_ids") or {}, sort_keys=True),
                "--team-count", str(segment.get("num_teams") or 10),
                "--import-mode", str(segment.get("import_mode") or "full"),
                "--request-delay", str(segment.get("request_delay", 0.5)),
                "--throttle-retries", str(segment.get("throttle_retries", 3)),
            ]
            if not is_target or not persist_credentials:
                cmd.append("--skip-upload")
        else:
            cmd = ["python", "initial_import_v3.py", "--context", rel_context]
            if not is_target:
                cmd.append("--allow-partial-history-source")
            cmd.append("--skip-track-2-upload")
    elif platform == "sleeper":
        cmd = ["python", "sleeper_initial_import.py", "--context", rel_context, "--import-mode", "full"]
        cmd.append("--skip-track-2-upload")
    else:
        cmd = ["python", "espn_initial_import.py", "--context", rel_context, "--import-mode", "full"]
        cmd.append("--skip-track-2-upload")
    run_logged(
        f"{platform.upper()} {'TARGET' if is_target else 'SOURCE'} IMPORT",
        cmd,
        cwd=FANTASY_DIR,
        log_file=log_path,
        env=env,
    )
    if persist_credentials:
        persist_yahoo_refresh_token(segment, index)
    return context_path, data_dir


def local_matchup_count(db_name: str, data_dir: Path) -> int:
    script = (
        "import duckdb, pathlib, sys; "
        f"p=pathlib.Path(r'{str(data_dir)}') / '{db_name}.duckdb'; "
        "conn=duckdb.connect(str(p), read_only=True); "
        "print(conn.execute('SELECT COUNT(*) FROM public.matchup').fetchone()[0]); "
        "conn.close()"
    )
    result = subprocess.run(
        ["python", "-c", script],
        cwd=str(FANTASY_DIR),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode != 0:
        fail(f"Could not count local matchup rows: {result.stderr.strip()}")
    return int(result.stdout.strip() or "0")


def upload_local_db(
    segment: dict[str, Any],
    data_dir: Path,
    *,
    name: str,
    log_file: Path,
    finalize_merge_source: bool = False,
) -> None:
    db_name = segment["database_name"]
    platform = segment["platform"]
    finalize_merge_source_literal = "True" if finalize_merge_source else "False"
    upload_script = f"""
import os, sys
sys.path.insert(0, ".")
from multi_league.core.local_db import LocalLeagueDB
db = LocalLeagueDB(r"{str(data_dir)}", "{db_name}")
db.connect()
try:
    db.upload_to_fly("{db_name}", platform="{platform}", finalize_merge_source={finalize_merge_source_literal})
finally:
    db.close()
print("Upload to Fly complete")
"""
    run_logged(
        name,
        ["python", "-c", upload_script],
        cwd=FANTASY_DIR,
        log_file=log_file,
    )


def local_db_path(segment: dict[str, Any], data_dir: Path) -> Path:
    return data_dir / f"{segment['database_name']}.duckdb"


def _local_catalog_name(conn, catalog: str) -> str:
    if catalog == "main":
        return str(conn.execute("SELECT CURRENT_DATABASE()").fetchone()[0])
    return catalog


def _local_year_scoped_tables(conn, catalog: str) -> list[str]:
    database_name = _local_catalog_name(conn, catalog)
    rows = conn.execute(
        """
        SELECT table_name
        FROM duckdb_columns()
        WHERE database_name = ?
          AND schema_name = 'public'
          AND column_name IN ('db_name', 'year')
        GROUP BY table_name
        HAVING COUNT(DISTINCT column_name) = 2
        ORDER BY table_name
        """,
        [database_name],
    ).fetchall()
    return [str(row[0]) for row in rows]


def _local_columns(conn, catalog: str, table_name: str) -> list[str]:
    database_name = _local_catalog_name(conn, catalog)
    return [
        str(row[0])
        for row in conn.execute(
            """
            SELECT column_name
            FROM duckdb_columns()
            WHERE database_name = ?
              AND schema_name = 'public'
              AND table_name = ?
            ORDER BY column_index
            """,
            [database_name, table_name],
        ).fetchall()
    ]


def _local_table_exists(conn, table_name: str) -> bool:
    database_name = _local_catalog_name(conn, "main")
    row = conn.execute(
        """
        SELECT COUNT(*)
        FROM duckdb_tables()
        WHERE database_name = ?
          AND schema_name = 'public'
          AND table_name = ?
        """,
        [database_name, table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _local_manager_case_sql(column: str, mappings: dict[str, str]) -> str:
    quoted_col = duckdb_ident(column)
    if not mappings:
        return quoted_col
    clauses = [
        f"WHEN {quoted_col} = {duckdb_string(source)} THEN {duckdb_string(target)}"
        for source, target in mappings.items()
    ]
    # Yahoo disambiguates one account controlling multiple historical teams as
    # ``Manager - Team Name``.  A user's explicit ``Manager -> target`` bridge
    # applies to those generated display variants too.
    disambiguated_clauses = [
        f"WHEN STARTS_WITH({quoted_col}, {duckdb_string(source + ' - ')}) THEN {duckdb_string(target)}"
        for source, target in sorted(mappings.items(), key=lambda item: len(item[0]), reverse=True)
    ]
    # A source importer may apply the explicit bridge before franchise
    # discovery adds `` - Team Name``. Collapse those destination-prefixed
    # variants back to the destination identity as well.
    destination_clauses = [
        f"WHEN STARTS_WITH({quoted_col}, {duckdb_string(target + ' - ')}) THEN {duckdb_string(target)}"
        for target in sorted(set(mappings.values()), key=len, reverse=True)
    ]
    normalized_clauses = []
    for source, target in mappings.items():
        normalized = re.sub(r"[^a-z0-9]+", "", source.strip().lower())
        if normalized:
            normalized_clauses.append(
                "WHEN LOWER(REGEXP_REPLACE(TRIM(COALESCE("
                f"{quoted_col}, '')), '[^A-Za-z0-9]+', '', 'g')) = {duckdb_string(normalized)} "
                f"THEN {duckdb_string(target)}"
            )
    return f"CASE {' '.join(clauses + disambiguated_clauses + destination_clauses + normalized_clauses)} ELSE {quoted_col} END"


def _local_select_expr(column: str, target_db: str, mappings: dict[str, str]) -> str:
    if column == "db_name":
        return f"{duckdb_string(target_db)} AS {duckdb_ident(column)}"
    if column in {"manager", "managers", "opponent", "source_manager", "destination_manager"}:
        return f"{_local_manager_case_sql(column, mappings)} AS {duckdb_ident(column)}"
    return duckdb_ident(column)


def _sanitize_manager_mapping(raw: Any) -> dict[str, str]:
    if not isinstance(raw, dict):
        return {}
    cleaned: dict[str, str] = {}
    for source, target in raw.items():
        source_name = str(source or "").strip()
        target_name = str(target or "").strip()
        if source_name and target_name and target_name != "__none__" and source_name != target_name:
            cleaned[source_name] = target_name
    return cleaned


def _compose_manager_mappings(
    source_mapping: dict[str, str],
    manager_overrides: dict[str, str],
) -> dict[str, str]:
    """Apply manual final-name overrides on top of platform bridge mappings."""
    composed: dict[str, str] = {}
    for source, bridge_target in source_mapping.items():
        composed[source] = manager_overrides.get(bridge_target, bridge_target)
    for source, final_target in manager_overrides.items():
        composed[source] = final_target
    return composed


def _apply_mapped_source_franchise_names(
    conn,
    *,
    merge_years: list[int],
    mappings: dict[str, str],
) -> None:
    """Propagate mapped manager names to copied source rows sharing the same franchise id."""
    if not mappings:
        return

    year_sql = year_in_sql(merge_years)
    target_names = sorted({target for target in mappings.values() if target})
    if not target_names:
        return
    target_name_sql = ", ".join(duckdb_string(target) for target in target_names)
    rows = conn.execute(
        f"""
        SELECT franchise_id, manager, COUNT(*) AS row_count
        FROM public.matchup
        WHERE TRY_CAST(year AS INTEGER) IN ({year_sql})
          AND manager IN ({target_name_sql})
          AND franchise_id IS NOT NULL
          AND TRIM(franchise_id) <> ''
        GROUP BY franchise_id, manager
        """
    ).fetchall()

    source_id_targets: dict[str, dict[str, int]] = {}
    for franchise_id, manager, row_count in rows:
        fid = str(franchise_id or "").strip()
        name = str(manager or "").strip()
        if not fid or not name:
            continue
        source_id_targets.setdefault(fid, {})
        source_id_targets[fid][name] = source_id_targets[fid].get(name, 0) + int(row_count or 0)

    inferred = {
        fid: next(iter(target_counts)) for fid, target_counts in source_id_targets.items() if len(target_counts) == 1
    }
    if not inferred:
        return

    conn.execute("DROP TABLE IF EXISTS temp._mapped_source_franchise_names")
    conn.execute(
        """
        CREATE TEMP TABLE _mapped_source_franchise_names (
            source_franchise_id VARCHAR,
            target_manager VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO _mapped_source_franchise_names VALUES (?, ?)",
        sorted(inferred.items()),
    )

    for table_name in _local_year_scoped_tables(conn, "main"):
        columns = set(_local_columns(conn, "main", table_name))
        for franchise_col, name_col in (
            ("franchise_id", "manager"),
            ("franchise_id", "managers"),
            ("opponent_franchise_id", "opponent"),
            ("source_franchise_id", "source_manager"),
            ("destination_franchise_id", "destination_manager"),
        ):
            if franchise_col not in columns or name_col not in columns:
                continue
            conn.execute(
                f"""
                UPDATE public.{duckdb_ident(table_name)} AS t
                SET {duckdb_ident(name_col)} = mapped.target_manager
                FROM _mapped_source_franchise_names AS mapped
                WHERE TRY_CAST(t.year AS INTEGER) IN ({year_sql})
                  AND t.{duckdb_ident(franchise_col)} = mapped.source_franchise_id
                  AND mapped.target_manager IS NOT NULL
                """
            )
    log(f"[local-merge] Propagated mapped source franchise names for {len(inferred)} copied identity ids")


def _remap_local_franchise_ids(
    conn,
    *,
    target_db: str,
    merge_years: list[int],
    target_years: list[int],
) -> None:
    year_sql = year_in_sql(merge_years)
    target_year_sql = year_in_sql(target_years)
    target_ids = conn.execute(
        f"""
        SELECT COUNT(*)
        FROM public.matchup
        WHERE db_name = {duckdb_string(target_db)}
          AND TRY_CAST(year AS INTEGER) IN ({target_year_sql})
          AND franchise_id IS NOT NULL
        """
    ).fetchone()[0]
    if not target_ids:
        return

    conn.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE _local_source_franchise_id_map AS
        WITH target_ids AS (
                    SELECT manager, ARG_MAX(franchise_id, TRY_CAST(year AS INTEGER)) AS target_franchise_id
                    FROM public.matchup
            WHERE db_name = {duckdb_string(target_db)}
              AND TRY_CAST(year AS INTEGER) IN ({target_year_sql})
                      AND manager IS NOT NULL
                      AND TRIM(manager) <> ''
                      AND franchise_id IS NOT NULL
                    GROUP BY manager
        ), source_identities AS (
            SELECT DISTINCT franchise_id, manager
            FROM public.matchup
            WHERE db_name = {duckdb_string(target_db)}
              AND TRY_CAST(year AS INTEGER) IN ({year_sql})
              AND franchise_id IS NOT NULL
              AND manager IS NOT NULL
              AND TRIM(manager) <> ''
        )
        SELECT source.franchise_id AS source_franchise_id,
               MIN(target.target_franchise_id) AS target_franchise_id
        FROM source_identities AS source
        JOIN target_ids AS target
          ON source.manager = target.manager
        GROUP BY source.franchise_id
        HAVING COUNT(DISTINCT target.target_franchise_id) = 1
        """
    )

    for table_name in _local_year_scoped_tables(conn, "main"):
        columns = set(_local_columns(conn, "main", table_name))
        for franchise_col in (
            "franchise_id",
            "opponent_franchise_id",
            "source_franchise_id",
            "destination_franchise_id",
        ):
            if franchise_col not in columns:
                continue
            conn.execute(
                f"""
                UPDATE public.{duckdb_ident(table_name)} AS t
                SET {duckdb_ident(franchise_col)} = ids.target_franchise_id
                FROM _local_source_franchise_id_map AS ids
                WHERE t.db_name = {duckdb_string(target_db)}
                  AND TRY_CAST(t.year AS INTEGER) IN ({year_sql})
                  AND t.{duckdb_ident(franchise_col)} = ids.source_franchise_id
                """
            )
    mapped_ids = conn.execute("SELECT COUNT(*) FROM _local_source_franchise_id_map").fetchone()[0]
    log(f"[local-merge] Remapped {int(mapped_ids):,} copied franchise ids to target identities")


def reconcile_explicit_manager_bridges_after_enrichment(
    *,
    segments: list[dict[str, Any]],
    target_index: int,
    target_data_dir: Path,
) -> int:
    """Restore explicit source->target identities after shared enrichment.

    The shared franchise resolver may split a mapped target id when historical
    Yahoo rows share an owner GUID. Explicit multiplatform mappings remain
    authoritative for years with exactly one mapped franchise. Years where the
    owner genuinely has multiple simultaneous teams are intentionally left
    split so matchup identity is never collapsed.
    """
    import duckdb

    target_segment = segments[target_index]
    target_db = target_segment["database_name"]
    target_years = years_from_segment(target_segment)
    bridge_targets = sorted(
        {
            target
            for idx, segment in enumerate(segments)
            if idx != target_index
            for target in _sanitize_manager_mapping(segment.get("manager_mapping")).values()
            if target
        }
    )
    source_years = sorted(
        {
            int(year)
            for idx, segment in enumerate(segments)
            if idx != target_index
            for year in (segment.get("merge_years") or years_from_segment(segment))
        }
    )
    if not bridge_targets or not source_years or not target_years:
        return 0

    conn = duckdb.connect(str(local_db_path(target_segment, target_data_dir)))
    try:
        conn.execute("CREATE OR REPLACE TEMP TABLE _bridge_target_names (target_manager VARCHAR)")
        conn.executemany("INSERT INTO _bridge_target_names VALUES (?)", [(name,) for name in bridge_targets])
        source_year_sql = year_in_sql(source_years)
        target_year_sql = year_in_sql(target_years)
        manager_match = (
            "m.manager = n.target_manager "
            "OR STARTS_WITH(m.manager, n.target_manager || ' - ')"
        )
        conn.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE _post_enrichment_franchise_bridge AS
            WITH target_candidates AS (
                SELECT n.target_manager, m.franchise_id, MAX(TRY_CAST(m.year AS INTEGER)) AS latest_year,
                       COUNT(*) AS row_count
                FROM public.matchup m
                JOIN _bridge_target_names n ON ({manager_match})
                WHERE m.db_name = {duckdb_string(target_db)}
                  AND TRY_CAST(m.year AS INTEGER) IN ({target_year_sql})
                  AND m.franchise_id IS NOT NULL
                GROUP BY n.target_manager, m.franchise_id
            ), target_ids AS (
                SELECT target_manager, franchise_id AS target_franchise_id
                FROM (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY target_manager
                        ORDER BY latest_year DESC, row_count DESC, franchise_id
                    ) AS rn
                    FROM target_candidates
                ) ranked
                WHERE rn = 1
            ), matchup_source_candidates AS (
                SELECT n.target_manager, TRY_CAST(m.year AS INTEGER) AS year,
                       m.franchise_id AS source_franchise_id
                FROM public.matchup m
                JOIN _bridge_target_names n ON ({manager_match})
                WHERE m.db_name = {duckdb_string(target_db)}
                  AND TRY_CAST(m.year AS INTEGER) IN ({source_year_sql})
                  AND m.franchise_id IS NOT NULL
                GROUP BY n.target_manager, TRY_CAST(m.year AS INTEGER), m.franchise_id
            ), player_source_candidates AS (
                SELECT n.target_manager, TRY_CAST(p.year AS INTEGER) AS year,
                       p.franchise_id AS source_franchise_id
                FROM public.player_fantasy p
                JOIN _bridge_target_names n ON (
                    p.manager = n.target_manager
                    OR STARTS_WITH(p.manager, n.target_manager || ' - ')
                )
                WHERE p.db_name = {duckdb_string(target_db)}
                  AND TRY_CAST(p.year AS INTEGER) IN ({source_year_sql})
                  AND p.franchise_id IS NOT NULL
                GROUP BY n.target_manager, TRY_CAST(p.year AS INTEGER), p.franchise_id
            ), source_candidates AS (
                SELECT * FROM matchup_source_candidates
                UNION ALL
                SELECT p.*
                FROM player_source_candidates p
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM matchup_source_candidates m
                    WHERE m.target_manager = p.target_manager
                      AND m.year = p.year
                )
            ), unambiguous_years AS (
                SELECT target_manager, year, MIN(source_franchise_id) AS source_franchise_id
                FROM source_candidates
                GROUP BY target_manager, year
                HAVING COUNT(DISTINCT source_franchise_id) = 1
            )
            SELECT u.year, u.target_manager, u.source_franchise_id, t.target_franchise_id
            FROM unambiguous_years u
            JOIN target_ids t USING (target_manager)
            """
        )
        mapped = int(conn.execute("SELECT COUNT(*) FROM _post_enrichment_franchise_bridge").fetchone()[0] or 0)
        if not mapped:
            return 0

        for table_name in _local_year_scoped_tables(conn, "main"):
            columns = set(_local_columns(conn, "main", table_name))
            if "year" not in columns:
                continue
            table_ref = f"public.{duckdb_ident(table_name)}"
            for franchise_col, name_col in (
                ("franchise_id", "manager"),
                ("franchise_id", "managers"),
                ("opponent_franchise_id", "opponent"),
                ("source_franchise_id", "source_manager"),
                ("destination_franchise_id", "destination_manager"),
            ):
                if franchise_col not in columns:
                    continue
                name_set = f", {duckdb_ident(name_col)} = b.target_manager" if name_col in columns else ""
                identity_match = f"t.{duckdb_ident(franchise_col)} = b.source_franchise_id"
                if name_col in columns:
                    quoted_name = duckdb_ident(name_col)
                    identity_match = (
                        f"({identity_match} OR (t.{duckdb_ident(franchise_col)} IS NULL AND ("
                        f"t.{quoted_name} = b.target_manager OR "
                        f"STARTS_WITH(t.{quoted_name}, b.target_manager || ' - '))))"
                    )
                conn.execute(
                    f"""
                    UPDATE {table_ref} t
                    SET {duckdb_ident(franchise_col)} = b.target_franchise_id{name_set}
                    FROM _post_enrichment_franchise_bridge b
                    WHERE t.db_name = {duckdb_string(target_db)}
                      AND TRY_CAST(t.year AS INTEGER) = b.year
                      AND {identity_match}
                    """
                )
            for name_col in (
                "manager",
                "managers",
                "opponent",
                "source_manager",
                "destination_manager",
            ):
                if name_col not in columns:
                    continue
                quoted_name = duckdb_ident(name_col)
                conn.execute(
                    f"""
                    UPDATE {table_ref} t
                    SET {quoted_name} = b.target_manager
                    FROM _post_enrichment_franchise_bridge b
                    WHERE t.db_name = {duckdb_string(target_db)}
                      AND TRY_CAST(t.year AS INTEGER) = b.year
                      AND (
                          t.{quoted_name} = b.target_manager
                          OR STARTS_WITH(t.{quoted_name}, b.target_manager || ' - ')
                      )
                    """
                )
        conn.execute("CHECKPOINT")
        log(f"[local-merge] Reconciled {mapped} explicit manager franchise-year bridge(s) after enrichment")
        return mapped
    finally:
        conn.close()


def merge_local_source_into_target(
    *,
    target_segment: dict[str, Any],
    target_data_dir: Path,
    source_segment: dict[str, Any],
    source_data_dir: Path,
    index: int,
) -> dict[str, int | str]:
    import duckdb

    target_db = target_segment["database_name"]
    source_db = source_segment["database_name"]
    merge_years = [int(year) for year in source_segment.get("merge_years") or years_from_segment(source_segment)]
    if not merge_years:
        fail(f"source segment {index} ({source_db}) has no years to merge")

    target_path = local_db_path(target_segment, target_data_dir)
    source_path = local_db_path(source_segment, source_data_dir)
    if not target_path.exists():
        fail(f"target local DB missing: {target_path}")
    if not source_path.exists():
        fail(f"source local DB missing for segment {index}: {source_path}")

    source_alias = f"source_{index}"
    year_sql = year_in_sql(merge_years)
    stats: dict[str, int | str] = {"status": "copied_local", "merge_years": year_sql}
    source_mapping = _sanitize_manager_mapping(source_segment.get("manager_mapping"))
    manager_overrides = _sanitize_manager_mapping(target_segment.get("manager_name_overrides"))
    manager_overrides.update(_sanitize_manager_mapping(source_segment.get("manager_name_overrides")))
    mappings = _compose_manager_mappings(source_mapping, manager_overrides)

    conn = duckdb.connect(str(target_path))
    try:
        conn.execute(f"ATTACH {duckdb_string(source_path)} AS {duckdb_ident(source_alias)} (READ_ONLY)")
        source_tables = set(_local_year_scoped_tables(conn, source_alias))
        target_tables = set(_local_year_scoped_tables(conn, "main"))

        log(f"[local-merge] Copying {source_db} -> {target_db} for year(s): {year_sql}")
        for table_name in sorted(source_tables & target_tables):
            if not _local_table_exists(conn, table_name):
                continue
            source_columns = set(_local_columns(conn, source_alias, table_name))
            target_columns = _local_columns(conn, "main", table_name)
            common = [column for column in target_columns if column in source_columns]
            if "year" not in common:
                continue

            column_sql = ", ".join(duckdb_ident(column) for column in common)
            select_sql = ",\n                  ".join(
                _local_select_expr(column, target_db, mappings) for column in common
            )
            source_filter = f"TRY_CAST(year AS INTEGER) IN ({year_sql})"
            if "db_name" in source_columns:
                source_filter += f" AND db_name = {duckdb_string(source_db)}"
            conn.execute("BEGIN TRANSACTION")
            conn.execute(
                f"""
                DELETE FROM public.{duckdb_ident(table_name)}
                WHERE TRY_CAST(year AS INTEGER) IN ({year_sql})
                """
            )
            conn.execute(
                f"""
                INSERT INTO public.{duckdb_ident(table_name)} ({column_sql})
                SELECT
                  {select_sql}
                FROM {duckdb_ident(source_alias)}.public.{duckdb_ident(table_name)}
                WHERE {source_filter}
                """
            )
            conn.execute("COMMIT")

            copied = conn.execute(
                f"""
                SELECT COUNT(*)
                FROM public.{duckdb_ident(table_name)}
                WHERE TRY_CAST(year AS INTEGER) IN ({year_sql})
                """
            ).fetchone()[0]
            if copied:
                stats[table_name] = int(copied)
                log(f"[local-merge] {table_name}: {int(copied):,} rows")

        _apply_mapped_source_franchise_names(conn, merge_years=merge_years, mappings=mappings)
        _remap_local_franchise_ids(
            conn,
            target_db=target_db,
            merge_years=merge_years,
            target_years=years_from_segment(target_segment),
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    copied_tables = [key for key in stats if key not in {"status", "merge_years"}]
    if not copied_tables:
        fail(f"No local rows copied from source segment {index} ({source_db}) for years {year_sql}")
    allow_partial = bool(source_segment.get("allow_partial_history_source"))
    required_tables = ("league_settings",) if allow_partial else ("league_settings", "matchup")
    for required in required_tables:
        if required not in stats:
            fail(f"Local merge from source segment {index} copied no {required} rows for years {year_sql}")
    if allow_partial and "matchup" not in stats:
        if "player_fantasy" not in stats:
            fail(
                f"Partial local merge from source segment {index} copied neither matchup nor "
                f"player_fantasy rows for years {year_sql}"
            )
        stats["status"] = "copied_partial_local"
        log(
            f"[local-merge] source segment {index} has no matchup rows for {year_sql}; "
            "retaining its available settings/player/draft/transaction history"
        )
    return stats


def merge_local_sources_into_target(
    *,
    segments: list[dict[str, Any]],
    target_index: int,
    target_data_dir: Path,
    source_data_dirs: dict[int, Path],
) -> None:
    target_segment = segments[target_index]
    for idx, segment in enumerate(segments):
        if idx == target_index:
            continue
        source_data_dir = source_data_dirs.get(idx)
        if source_data_dir is None:
            fail(f"source segment {idx + 1} has no local data directory")
        stats = merge_local_source_into_target(
            target_segment=target_segment,
            target_data_dir=target_data_dir,
            source_segment=segment,
            source_data_dir=source_data_dir,
            index=idx + 1,
        )
        log(f"[local-merge] source {idx + 1} complete: {stats}")


def trim_local_target_to_selected_years(segment: dict[str, Any], data_dir: Path) -> None:
    import duckdb

    db_name = segment["database_name"]
    selected_years = [int(year) for year in segment.get("merge_years") or years_from_segment(segment)]
    if not selected_years:
        fail(f"target segment ({db_name}) has no selected years")

    target_path = local_db_path(segment, data_dir)
    if not target_path.exists():
        fail(f"target local DB missing: {target_path}")

    year_sql = year_in_sql(selected_years)
    trimmed: dict[str, int] = {}
    conn = duckdb.connect(str(target_path))
    try:
        for table_name in _local_year_scoped_tables(conn, "main"):
            columns = set(_local_columns(conn, "main", table_name))
            db_filter = f" AND db_name = {duckdb_string(db_name)}" if "db_name" in columns else ""
            count = int(
                conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM public.{duckdb_ident(table_name)}
                    WHERE TRY_CAST(year AS INTEGER) NOT IN ({year_sql})
                    {db_filter}
                    """
                ).fetchone()[0]
                or 0
            )
            if count <= 0:
                continue
            conn.execute(
                f"""
                DELETE FROM public.{duckdb_ident(table_name)}
                WHERE TRY_CAST(year AS INTEGER) NOT IN ({year_sql})
                {db_filter}
                """
            )
            trimmed[table_name] = count
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    if trimmed:
        table_summary = ", ".join(f"{table}: {count:,}" for table, count in sorted(trimmed.items()))
        log(f"[local-merge] Trimmed target to selected year(s) {year_sql}: {table_summary}")


def run_local_sql_enrichments_after_merge(segment: dict[str, Any], data_dir: Path) -> None:
    """Recompute derived metrics over the complete, merged league history.

    Source segments run their own enrichments before the local merge.  Metrics
    such as transaction grades depend on the size of the entire history, so a
    source that is too small to grade independently must be enriched again
    after every source has been copied into the target database.
    """
    db_name = segment["database_name"]
    enrichment_script = f"""
from multi_league.core.local_db import LocalLeagueDB
from multi_league.transformations.sql_enrichments import SQLEnrichments

db = LocalLeagueDB(r\"{str(data_dir)}\", \"{db_name}\")
eng = None
try:
    conn = db.connect()
    eng = SQLEnrichments(
        db_name=\"{db_name}\",
        data_dir=r\"{str(data_dir)}\",
        ppr=0.5,
        pass_td_pts=4,
        quick=False,
        conn=conn,
    )
    eng.load_settings_from_db()
    results = eng.run_all()
    failures = {{name: value for name, value in results.items() if isinstance(value, tuple)}}
    if failures:
        raise RuntimeError(f\"SQL enrichments failed: {{failures}}\")
finally:
    if eng is not None:
        eng.close()
    db.close()
print(\"Post-merge SQL enrichments complete\")
"""
    run_logged(
        "RUN SQL ENRICHMENTS AFTER LOCAL MERGE",
        ["python", "-c", enrichment_script],
        cwd=FANTASY_DIR,
        log_file=LOG_DIR / "target_post_merge_enrichments.log",
    )


def postprocess_and_upload_target(
    segment: dict[str, Any],
    data_dir: Path,
    *,
    segments: list[dict[str, Any]] | None = None,
    target_index: int | None = None,
    publish: bool = True,
) -> None:
    db_name = segment["database_name"]
    run_local_sql_enrichments_after_merge(segment, data_dir)
    if segments is not None and target_index is not None:
        reconcile_explicit_manager_bridges_after_enrichment(
            segments=segments,
            target_index=target_index,
            target_data_dir=data_dir,
        )
    count = local_matchup_count(db_name, data_dir)
    log(f"[target] matchup rows: {count:,}")

    if count > 0:
        run_logged(
            "CALCULATE EXPECTED RECORDS",
            [
                "python",
                "-m",
                "multi_league.transformations.matchup.expected_record_v2",
                "--db",
                db_name,
                "--data-dir",
                str(data_dir),
                "--n-sims",
                "10000",
            ],
            cwd=FANTASY_DIR,
            log_file=LOG_DIR / "target_expected_record.log",
        )
        run_logged(
            "CALCULATE PLAYOFF ODDS",
            [
                "python",
                "-m",
                "multi_league.transformations.matchup.playoff_odds_import",
                "--db",
                db_name,
                "--data-dir",
                str(data_dir),
                "--n-sims",
                "10000",
            ],
            cwd=FANTASY_DIR,
            log_file=LOG_DIR / "target_playoff_odds.log",
        )

    run_logged(
        "RUN AGGREGATIONS",
        ["python", "../scripts/refresh_aggregates.py", "--db", db_name, "--data-dir", str(data_dir)],
        cwd=FANTASY_DIR,
        log_file=LOG_DIR / "target_refresh_aggregates.log",
    )

    if not publish:
        log("[verify-only] Local merged target complete; skipping Fly upload, live validation, and cache warm")
        return

    upload_local_db(
        segment,
        data_dir,
        name="UPLOAD MERGED TARGET",
        log_file=LOG_DIR / "target_upload.log",
    )

    run_logged(
        "VALIDATE MERGED TARGET",
        ["python", "-m", "multi_league.validation_v2", "--db", db_name, "-v", "--ci"],
        cwd=FANTASY_DIR,
        log_file=LOG_DIR / "validation.log",
    )

    secret = os.environ.get("REVALIDATION_SECRET")
    if secret:
        run_logged(
            "REVALIDATE AND WARM VERCEL CACHE",
            [
                "python",
                "scripts/warm_vercel_cache.py",
                "--db",
                db_name,
                "--secret",
                secret,
                "--site-url",
                "https://www.leaguehistory.app",
                "--mode",
                "full",
                "--strategy",
                "expire",
                "--jitter-seconds",
                "0",
                "--strict",
                "--verify-hot",
            ],
            cwd=REPO_ROOT,
            log_file=LOG_DIR / "warm_vercel_cache.log",
        )
    else:
        log("[warm-cache] REVALIDATION_SECRET not set; skipping cache warm")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a generic multi-platform full import")
    parser.add_argument("--payload", help="Path to payload JSON, or omit to use IMPORT_DATA_B64")
    parser.add_argument("--plan-only", action="store_true", help="Validate and print the import plan only")
    parser.add_argument("--verify-only", action="store_true", help="Run locally without credentials or publication")
    args = parser.parse_args()

    LOG_DIR.mkdir(exist_ok=True)
    payload = load_payload(args)
    segments, target_index, target_db = normalize_segments(payload)
    payload["segments"] = segments
    payload["target_segment_index"] = target_index
    payload["target_db"] = target_db
    payload["database_name"] = target_db

    write_json(REPO_ROOT / "multi_platform_payload.normalized.json", sanitized_payload(payload))
    summary = {
        "target_db": target_db,
        "target_platform": segments[target_index]["platform"],
        "target_segment_index": target_index,
        "segments": [
            {
                "index": idx + 1,
                "role": segment["role"],
                "platform": segment["platform"],
                "database_name": segment["database_name"],
                "years": segment["merge_years"],
                "reuse_existing_source": reuse_existing_source(segment) if idx != target_index else False,
            }
            for idx, segment in enumerate(segments)
        ],
    }
    write_json(REPO_ROOT / "multi_platform_summary.json", summary)
    log(json.dumps(summary, indent=2))

    if args.plan_only:
        return

    source_data_dirs: dict[int, Path] = {}
    for idx, segment in enumerate(segments):
        if segment["platform"] in {"yahoo", "espn"}:
            segment.setdefault("credential_database_name", target_db)
        if idx == target_index:
            continue
        segment["allow_partial_history_source"] = segment["platform"] == "yahoo"
        if reuse_existing_source(segment):
            fail(
                f"source segment {idx + 1} requested reuse_existing_source, but the multi-platform worker "
                "now merges local segment imports. Remove reuse_existing_source so the platform import can run locally."
            )
        _, source_data_dir = import_segment(
            segment,
            idx + 1,
            is_target=False,
            persist_credentials=not args.verify_only,
        )
        source_data_dirs[idx] = source_data_dir

    _, target_data_dir = import_segment(
        segments[target_index],
        target_index + 1,
        is_target=True,
        persist_credentials=not args.verify_only,
    )
    trim_local_target_to_selected_years(segments[target_index], target_data_dir)
    merge_local_sources_into_target(
        segments=segments,
        target_index=target_index,
        target_data_dir=target_data_dir,
        source_data_dirs=source_data_dirs,
    )
    postprocess_and_upload_target(
        segments[target_index],
        target_data_dir,
        segments=segments,
        target_index=target_index,
        publish=not args.verify_only,
    )
    log("")
    log("[multi-platform] import complete")


if __name__ == "__main__":
    main()
