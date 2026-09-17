#!/usr/bin/env python3
"""Run the server-local league identity rename from a public worker payload."""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))


DB_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,62}$")
PAYLOAD_FIELDS = {"source_db", "target_db", "display_name", "operation_id"}


def decode_payload(raw: str) -> dict[str, str]:
    try:
        payload = json.loads(base64.b64decode(raw, validate=True).decode("utf-8"))
    except Exception as exc:
        raise SystemExit(f"Invalid rename_data_b64 payload: {exc}") from exc
    if not isinstance(payload, dict) or set(payload) != PAYLOAD_FIELDS:
        raise SystemExit(
            "rename_data_b64 payload must contain exactly source_db, target_db, display_name, and operation_id"
        )
    source_db = str(payload["source_db"] or "").strip()
    target_db = str(payload["target_db"] or "").strip()
    display_name = str(payload["display_name"] or "").strip()
    operation_id = str(payload["operation_id"] or "").strip()
    if not DB_NAME_RE.fullmatch(source_db) or not DB_NAME_RE.fullmatch(target_db):
        raise SystemExit("Invalid source_db or target_db")
    if source_db == target_db:
        raise SystemExit("source_db and target_db must differ")
    if not display_name or len(display_name) > 100:
        raise SystemExit("display_name is required and must be at most 100 characters")
    if not operation_id or len(operation_id) > 200:
        raise SystemExit("operation_id is required and must be at most 200 characters")
    return {
        "source_db": source_db,
        "target_db": target_db,
        "display_name": display_name,
        "operation_id": operation_id,
    }


def run_rename(payload: dict[str, str], *, target: Any | None = None) -> dict:
    if target is None:
        from multi_league.core.targets.fly_target import FlyTarget

        target = FlyTarget()
    return target.rename_league(**payload)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run an atomic league URL rename")
    parser.add_argument("--rename-data-b64", default=os.environ.get("RENAME_DATA_B64", ""))
    args = parser.parse_args()
    if not args.rename_data_b64:
        raise SystemExit("RENAME_DATA_B64 is required")
    payload = decode_payload(args.rename_data_b64)
    result = run_rename(payload)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
