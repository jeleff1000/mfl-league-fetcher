#!/usr/bin/env python3
"""Run an admin league-to-league merge from a GitHub Actions payload.

This intentionally delegates to the same merge_source copier used after normal
imports, so copied historical rows and no-year aggregates stay in sync.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import requests

ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

DB_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,62}$")


def _decode_payload(raw: str) -> dict[str, Any]:
    try:
        text = base64.b64decode(raw).decode("utf-8")
        data = json.loads(text)
    except Exception as exc:  # pragma: no cover - CLI guard
        raise SystemExit(f"Invalid merge_data_b64 payload: {exc}") from exc

    if not isinstance(data, dict):
        raise SystemExit("merge_data_b64 payload must decode to a JSON object")
    return data


def _assert_db_name(value: Any, field: str) -> str:
    db_name = str(value or "").strip()
    if not DB_NAME_RE.fullmatch(db_name):
        raise SystemExit(f"Invalid {field}: {value!r}")
    return db_name


def _merge_years(value: Any) -> list[int]:
    if not isinstance(value, list) or not value:
        return []
    years = sorted({int(year) for year in value})
    for year in years:
        if year < 1900 or year > 2100:
            raise SystemExit(f"Invalid merge year: {year}")
    return years


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _persist_merged_league_ids(
    *,
    reader,
    writer,
    source_db: str,
    target_db: str,
    merge_years: list[int],
) -> dict[str, str]:
    """Persist explicit replacement years in the target's Yahoo renewal chain."""
    rows = reader.query(
        "SELECT db_name, league_ids_json FROM public.league_context "
        f"WHERE db_name IN ({_sql_literal(source_db)}, {_sql_literal(target_db)})",
        database="___leagues",
    )
    by_db = {str(row.get("db_name")): row for row in rows}
    missing_contexts = [db_name for db_name in (source_db, target_db) if db_name not in by_db]
    if missing_contexts:
        raise RuntimeError(
            "Missing league_context row(s): " + ", ".join(missing_contexts)
        )

    def parse_ids(db_name: str) -> dict[str, str]:
        raw = by_db[db_name].get("league_ids_json")
        try:
            parsed = json.loads(raw or "{}")
        except (TypeError, ValueError) as exc:
            raise RuntimeError(f"Invalid league_ids_json for {db_name}") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError(f"Invalid league_ids_json for {db_name}")
        return {
            str(year): str(league_id)
            for year, league_id in parsed.items()
            if str(year).strip() and str(league_id).strip()
        }

    source_ids = parse_ids(source_db)
    target_ids = parse_ids(target_db)
    missing_years = [year for year in merge_years if str(year) not in source_ids]
    if missing_years:
        raise RuntimeError(
            "Source league_context is missing league IDs for merge years: "
            + ", ".join(map(str, missing_years))
        )

    for year in merge_years:
        target_ids[str(year)] = source_ids[str(year)]

    serialized = json.dumps(target_ids, sort_keys=True, separators=(",", ":"))
    writer.execute(
        "UPDATE public.league_context "
        f"SET league_ids_json = {_sql_literal(serialized)}, updated_at = CURRENT_TIMESTAMP "
        f"WHERE db_name = {_sql_literal(target_db)}",
        database="___leagues",
    )
    return {
        "league_context_year_ids_persisted": ", ".join(map(str, merge_years))
    }


def _rebuild_league_derived(*, target_db: str, run_id: str) -> dict[str, Any]:
    """Run the existing bounded, atomic full-chain derived rebuild."""
    server_url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    admin_token = os.environ.get("DATABASE_ADMIN_TOKEN", "")
    if not server_url or not admin_token:
        raise RuntimeError("DATABASE_SERVER_URL and DATABASE_ADMIN_TOKEN are required")

    headers = {"Authorization": f"Bearer {admin_token}"}
    machine_id = os.environ.get("FLY_PRIMARY_MACHINE_ID", "").strip()
    if machine_id:
        headers["fly-force-instance-id"] = machine_id
    body = {"db_name": target_db, "run_id": run_id, "timeout_seconds": 90}

    response = None
    for attempt in range(2):
        try:
            response = requests.post(
                f"{server_url}/rebuild-league-derived",
                json=body,
                headers=headers,
                timeout=50,
            )
        except requests.exceptions.RequestException as exc:
            if attempt == 0:
                time.sleep(1)
                continue
            raise RuntimeError("Derived rebuild response remained unavailable") from exc
        if response.status_code in {429, 500, 502, 503, 504} and attempt == 0:
            time.sleep(1)
            continue
        break

    if response is None or response.status_code != 200:
        status = response.status_code if response is not None else "unavailable"
        detail = response.text[:500] if response is not None else "no response"
        raise RuntimeError(f"Derived rebuild failed ({status}): {detail}")
    result = response.json()
    if result.get("status") not in {"COMMITTED", "ALREADY_COMMITTED"}:
        raise RuntimeError(f"Derived rebuild returned unexpected status: {result}")
    if result.get("db_name") != target_db:
        raise RuntimeError("Derived rebuild receipt db_name mismatch")
    return result


def run_merge(payload: dict[str, Any], *, reader=None, writer=None) -> dict[str, Any]:
    """Copy one saved league into another and close only missing rollup gaps."""
    source_db = _assert_db_name(payload.get("source_db"), "source_db")
    target_db = _assert_db_name(payload.get("target_db"), "target_db")
    if source_db == target_db:
        raise SystemExit("source_db and target_db must differ")

    merge_source: dict[str, Any] = {"source_db": source_db}
    years = _merge_years(payload.get("merge_years"))
    if years:
        merge_source["merge_years"] = years

    manager_mapping = payload.get("manager_mapping")
    if isinstance(manager_mapping, dict):
        merge_source["manager_mapping"] = {
            str(source).strip(): str(target).strip()
            for source, target in manager_mapping.items()
            if str(source).strip() and str(target).strip()
        }

    from multi_league.core.db_reader import get_reader
    from multi_league.core.fly_writer import FlyWriter
    from multi_league.data_fetchers.shared.merge_source_copier import (
        _repair_copied_trade_mirrors,
        copy_merge_source_to_public,
    )

    reader = reader or get_reader()
    writer = writer or FlyWriter()
    ctx = SimpleNamespace(merge_source=merge_source, import_mode="full")
    finalize_only = payload.get("finalize_only") is True
    if finalize_only:
        stats: dict[str, Any] = {"status": "finalizing_existing_copy"}
        stats.update(
            _repair_copied_trade_mirrors(
                reader=reader,
                writer=writer,
                target_db=target_db,
                merge_years=years,
            )
        )
    else:
        stats = dict(
            copy_merge_source_to_public(
                ctx,
                target_db,
                reader=reader,
                writer=writer,
                refresh_aggregates=False,
            )
        )
    if years:
        stats.update(
            _persist_merged_league_ids(
                reader=reader,
                writer=writer,
                source_db=source_db,
                target_db=target_db,
                merge_years=years,
            )
        )

    github_run_id = os.environ.get("GITHUB_RUN_ID", "local")
    requested_run_id = str(payload.get("run_id") or "").strip()
    run_id = requested_run_id or f"league-admin-merge-{github_run_id}-{target_db}"
    derived = _rebuild_league_derived(target_db=target_db, run_id=run_id[:200])
    stats["derived_rebuild_status"] = derived["status"]
    if derived.get("generation") is not None:
        stats["derived_generation"] = int(derived["generation"])
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a league admin merge")
    parser.add_argument("--merge-data-b64", default=os.environ.get("MERGE_DATA_B64", ""))
    args = parser.parse_args()

    if not args.merge_data_b64:
        raise SystemExit("MERGE_DATA_B64 is required")

    payload = _decode_payload(args.merge_data_b64)
    stats = run_merge(payload)
    print(json.dumps(stats, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
