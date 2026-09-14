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
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a league admin merge")
    parser.add_argument("--merge-data-b64", default=os.environ.get("MERGE_DATA_B64", ""))
    args = parser.parse_args()

    if not args.merge_data_b64:
        raise SystemExit("MERGE_DATA_B64 is required")

    payload = _decode_payload(args.merge_data_b64)
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

    from multi_league.data_fetchers.shared.merge_source_copier import (
        copy_merge_source_to_public,
    )

    ctx = SimpleNamespace(merge_source=merge_source, import_mode="full")
    stats = copy_merge_source_to_public(ctx, target_db)
    print(json.dumps(stats, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
