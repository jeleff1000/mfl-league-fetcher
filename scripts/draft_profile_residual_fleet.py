#!/usr/bin/env python3
"""Run targeted position/profile residual discovery for draft dossiers."""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import json
import os
from pathlib import Path
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
for path in (ROOT, SCRIPTS_ROOT, SCRIPTS_ROOT / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


from multi_league.core.readers.fly_reader import FlyReader
from multi_league.transformations.draft.cross_table_miner import build_cross_table_base_sql
from multi_league.transformations.draft.profile_residual_miner import run_profile_residual_miner_df
from multi_league.transformations.draft.tendency_validation import validate_inefficiency_candidates
from scripts.draft_cross_table_batch import DEFAULT_LEAGUES


def load_env_file() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key in {"DATABASE_SERVER_URL", "DATABASE_READ_TOKEN"} and key not in os.environ:
            os.environ[key] = value.strip().strip('"').strip("'")


def fly_query_with_retry(sql: str, *, attempts: int = 5, sleep_seconds: int = 30) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return FlyReader().query(sql, database="___leagues")
        except Exception as exc:
            last_error = exc
            message = str(exc).lower()
            retryable = any(
                token in message
                for token in ("503", "busy", "starting", "timeout", "network", "ssl", "eof", "max retries")
            )
            if not retryable or attempt == attempts:
                raise
            wait = sleep_seconds * attempt
            print(f"query_retry attempt={attempt} wait_seconds={wait} error={type(exc).__name__}: {exc}", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"fly query failed after retries: {last_error}")


def compact_raw(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "feature_type": row.get("feature_type"),
        "feature_value": row.get("feature_value"),
        "profile_label": row.get("profile_label"),
        "profile_family": row.get("profile_family"),
        "profile_blurb": row.get("profile_blurb"),
        "value_z_score": row.get("value_z_score"),
        "repeatability": row.get("repeatability"),
        "excess_residual": row.get("excess_residual"),
        "picks": row.get("picks"),
        "years_seen": row.get("years_seen"),
    }


def compact_state(row: dict[str, Any], raw_by_key: dict[tuple[str, str], dict[str, Any]]) -> dict[str, Any]:
    raw = raw_by_key.get((str(row.get("feature_type")), str(row.get("feature_value"))), {})
    return {
        "promotion_level": row.get("promotion_level"),
        "promotion_rank": row.get("promotion_rank"),
        "feature_type": row.get("feature_type"),
        "feature_value": row.get("feature_value"),
        "profile_label": raw.get("profile_label"),
        "profile_family": raw.get("profile_family"),
        "profile_blurb": raw.get("profile_blurb"),
        "state_family": row.get("state_family"),
        "shrunk_z_score": row.get("shrunk_z_score"),
        "q_value": row.get("q_value"),
        "repeatability": row.get("repeatability"),
        "value_delta": row.get("value_delta"),
        "picks": row.get("picks"),
        "years_seen": row.get("years_seen"),
    }


def level_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(str(row.get("promotion_level")) for row in rows))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run targeted draft profile residual discovery.")
    parser.add_argument("--db", help="Single league db_name.")
    parser.add_argument("--league", action="append", dest="leagues", help="Restrict to db_name. Repeatable.")
    parser.add_argument("--default-leagues", action="store_true", help="Use canonical 18-league cohort.")
    parser.add_argument("--row-limit", type=int)
    parser.add_argument("--min-picks", type=int, default=100)
    parser.add_argument("--min-years", type=int, default=12)
    parser.add_argument("--min-abs-z", type=float, default=2.5)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    load_env_file()
    if args.db:
        scope_label = args.db
        db_names = [args.db]
    elif args.leagues:
        db_names = args.leagues
        scope_label = "fleet_custom"
    elif args.default_leagues:
        db_names = DEFAULT_LEAGUES
        scope_label = "fleet_default18"
    else:
        db_names = None
        scope_label = "fleet_all"

    out = (
        args.out
        or ROOT / "tmp" / f"draft_profile_residual_{scope_label}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    )
    out.parent.mkdir(parents=True, exist_ok=True)

    print("querying_profile_matrix", flush=True)
    if db_names:
        print(f"scope_leagues={len(db_names)}", flush=True)
    rows = fly_query_with_retry(build_cross_table_base_sql(None, row_limit=args.row_limit, db_names=db_names))

    import pandas as pd

    df = pd.DataFrame(rows)
    print(f"rows={len(df)} cols={len(df.columns) if not df.empty else 0}", flush=True)
    raw = run_profile_residual_miner_df(
        df,
        scope_label=scope_label,
        min_picks=args.min_picks,
        min_years=args.min_years,
        min_abs_z=args.min_abs_z,
        limit=args.limit,
    )

    payload: dict[str, Any] = {
        "status": "ok",
        "scope_label": scope_label,
        "scope_leagues": db_names,
        "rows": len(df),
        "raw_counts": {key: len(value) for key, value in raw.items()},
        "sections": {},
    }
    for section, raw_rows in raw.items():
        states = validate_inefficiency_candidates(raw_rows)
        states = sorted(
            states,
            key=lambda row: (
                row.get("promotion_rank") is None,
                row.get("promotion_rank") or 9999,
                -abs(float(row.get("shrunk_z_score") or 0)),
            ),
        )
        raw_by_key = {(str(row.get("feature_type")), str(row.get("feature_value"))): row for row in raw_rows}
        payload["sections"][section] = {
            "promotion_counts": level_counts(states),
            "top_states": [compact_state(row, raw_by_key) for row in states[:25]],
            "top_raw": [compact_raw(row) for row in raw_rows[:25]],
        }

    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(json.dumps({key: value["promotion_counts"] for key, value in payload["sections"].items()}), flush=True)
    print(f"out={out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
