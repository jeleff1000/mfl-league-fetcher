#!/usr/bin/env python3
"""
Normalize Sleeper workflow payloads before import steps run.

This keeps reimports idempotent by resolving the database name from the
canonical/current Sleeper league id when a payload includes historical
league_ids by year.
"""

from __future__ import annotations

import base64
import json
import os
import re
import subprocess
import sys
import urllib.request
from pathlib import Path


def decode_league_data() -> dict:
    league_data_b64 = os.environ.get("LEAGUE_DATA_B64", "").strip()
    league_data_raw = os.environ.get("LEAGUE_DATA_RAW", "").strip()

    if league_data_b64:
        try:
            return json.loads(base64.b64decode(league_data_b64).decode("utf-8"))
        except Exception:
            return json.loads(league_data_b64)

    if league_data_raw:
        return json.loads(league_data_raw)

    raise SystemExit("ERROR: No league data provided")


def slugify(name: str) -> str:
    value = re.sub(r"[^a-zA-Z0-9]+", "_", name.strip().lower()).strip("_")
    if not value:
        return "sleeper_league"
    if value[0].isdigit():
        value = f"l_{value}"
    return value[:63]


def parse_history_ids(league_ids: object) -> dict[int, str]:
    by_year: dict[int, str] = {}
    if not isinstance(league_ids, dict):
        return by_year

    for year_key, league_id in league_ids.items():
        league_id_str = str(league_id or "").strip()
        if not league_id_str:
            continue
        try:
            year = int(year_key)
        except (TypeError, ValueError):
            continue
        by_year[year] = league_id_str

    return by_year


def fly_query(sql: str, database: str = "___ops") -> list[dict]:
    server_url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    read_token = os.environ.get("DATABASE_READ_TOKEN", "").strip()
    if not server_url or not read_token:
        return []

    data = json.dumps({"sql": sql, "database": database}).encode("utf-8")
    req = urllib.request.Request(
        f"{server_url}/query",
        data=data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {read_token}",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read())
    return payload if isinstance(payload, list) else payload.get("rows", [])


def canonical_sleeper_league_id(league_data: dict) -> tuple[str, int | None, int | None]:
    direct_id = str(league_data.get("sleeper_league_id") or league_data.get("league_id") or "").strip()
    history_ids = parse_history_ids(league_data.get("league_ids") or {})
    if not history_ids:
        return direct_id, None, None

    preferred_years: list[int] = []
    for key in ("end_year", "season", "year"):
        value = league_data.get(key)
        try:
            preferred_years.append(int(value))
        except (TypeError, ValueError):
            continue

    latest_year = max(history_ids)
    preferred_years.append(latest_year)

    for year in preferred_years:
        league_id = history_ids.get(year)
        if league_id:
            return league_id, min(history_ids), latest_year

    return history_ids[latest_year], min(history_ids), latest_year


def lookup_registered_sleeper_league_id(database_name: str) -> str:
    database_name = str(database_name or "").strip()
    if not database_name:
        return ""

    escaped_db_name = database_name.replace("'", "''")

    try:
        rows = fly_query(
            "SELECT sleeper_league_id "
            "FROM main.sleeper_leagues "
            f"WHERE database_name = '{escaped_db_name}' "
            "ORDER BY updated_at DESC "
            "LIMIT 1"
        )
    except Exception as exc:
        print(f"WARNING: current Sleeper mapping lookup failed: {exc}", file=sys.stderr)
        return ""

    if not rows:
        return ""
    return str(rows[0].get("sleeper_league_id") or "").strip()


def resolve_db_name(league_id: str, league_name: str, pre_resolved_db_name: str) -> str:
    result = subprocess.run(
        [
            "python",
            ".github/scripts/resolve_db_name.py",
            "--league-id",
            str(league_id),
            "--league-name",
            league_name,
            "--platform",
            "sleeper",
            "--database-name",
            pre_resolved_db_name,
        ],
        capture_output=True,
        text=True,
        check=False,
    )

    if result.returncode == 0:
        resolved = result.stdout.strip().splitlines()
        if resolved:
            return resolved[-1]

    warning = result.stderr.strip() or "unknown resolver error"
    print(f"WARNING: resolve_db_name.py failed: {warning}", file=sys.stderr)
    return slugify(league_name)


def write_github_outputs(outputs: dict[str, str]) -> None:
    github_output = os.environ.get("GITHUB_OUTPUT", "").strip()
    if not github_output:
        return

    with open(github_output, "a", encoding="utf-8") as handle:
        for key, value in outputs.items():
            handle.write(f"{key}={value}\n")


def main() -> None:
    league_data = decode_league_data()

    league_name = str(league_data.get("league_name") or "Sleeper League")
    username = str(league_data.get("username") or "").strip()
    user_id = os.environ.get("USER_ID", "").strip()
    import_mode = os.environ.get("IMPORT_MODE", "").strip() or str(league_data.get("import_mode") or "quick").strip()

    canonical_id, history_start_year, history_end_year = canonical_sleeper_league_id(league_data)
    if not canonical_id:
        raise SystemExit("ERROR: sleeper_league_id (or league_id) is required")

    if history_start_year is None:
        try:
            start_year = int(league_data.get("start_year", 2024))
        except (TypeError, ValueError):
            start_year = 2024
    else:
        start_year = history_start_year

    if history_end_year is None:
        try:
            end_year = int(league_data.get("end_year") or league_data.get("season") or start_year)
        except (TypeError, ValueError):
            end_year = start_year
    else:
        end_year = history_end_year

    league_data["sleeper_league_id"] = canonical_id
    league_data["league_id"] = canonical_id
    league_data["start_year"] = start_year
    league_data["end_year"] = end_year
    league_data["season"] = end_year

    env_pre_resolved_db_name = os.environ.get("PRE_RESOLVED_DATABASE_NAME", "").strip()
    payload_db_name = str(league_data.get("database_name") or "").strip()
    pre_resolved_db_name = env_pre_resolved_db_name or payload_db_name

    if env_pre_resolved_db_name and payload_db_name and env_pre_resolved_db_name != payload_db_name:
        payload_registered_id = lookup_registered_sleeper_league_id(payload_db_name)
        if payload_registered_id and payload_registered_id == canonical_id:
            print(
                f"INFO: Preferring payload database_name {payload_db_name} over pre-resolved "
                f"{env_pre_resolved_db_name} because it is registered to Sleeper id {canonical_id}",
                file=sys.stderr,
            )
            pre_resolved_db_name = payload_db_name

    registered_canonical_id = lookup_registered_sleeper_league_id(pre_resolved_db_name)
    if registered_canonical_id and registered_canonical_id != canonical_id:
        print(
            f"INFO: Promoting {pre_resolved_db_name} to current registered Sleeper id {registered_canonical_id}",
            file=sys.stderr,
        )
        canonical_id = registered_canonical_id
        league_data["sleeper_league_id"] = canonical_id
        league_data["league_id"] = canonical_id

    database_name = resolve_db_name(canonical_id, league_name, pre_resolved_db_name)

    outputs = {
        "sleeper_league_id": canonical_id,
        "league_name": league_name,
        "username": username,
        "database_name": database_name,
        "user_id": user_id,
        "import_mode": import_mode,
        "start_year": str(start_year),
        "end_year": str(end_year),
    }

    output_json_path = os.environ.get("OUTPUT_JSON_PATH", "").strip()
    if output_json_path:
        Path(output_json_path).write_text(json.dumps(league_data, indent=2), encoding="utf-8")

    write_github_outputs(outputs)

    print(json.dumps({"outputs": outputs, "league_data": league_data}, indent=2))


if __name__ == "__main__":
    main()
