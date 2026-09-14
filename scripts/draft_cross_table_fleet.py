#!/usr/bin/env python3
"""Run pooled fleet-wide cross-table draft inefficiency discovery."""

from __future__ import annotations

import argparse
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
from multi_league.transformations.draft.tendency_validation import validate_inefficiency_candidates
from multi_league.transformations.draft.wide_correlation_miner import (
    _score_league_inefficiencies,
    build_wide_features,
)
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


def compact_state(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "promotion_level": row.get("promotion_level"),
        "promotion_rank": row.get("promotion_rank"),
        "feature_type": row.get("feature_type"),
        "feature_value": row.get("feature_value"),
        "state_family": row.get("state_family"),
        "shrunk_z_score": row.get("shrunk_z_score"),
        "q_value": row.get("q_value"),
        "repeatability": row.get("repeatability"),
        "value_delta": row.get("value_delta"),
        "picks": row.get("picks"),
        "years_seen": row.get("years_seen"),
    }


def level_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        level = str(row.get("promotion_level"))
        counts[level] = counts.get(level, 0) + 1
    return counts


def fly_query_with_retry(sql: str, *, attempts: int = 5, sleep_seconds: int = 30) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return FlyReader().query(sql, database="___leagues")
        except Exception as exc:  # Fly can return transient 503 while waking.
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Run pooled fleet-wide draft inefficiency discovery.")
    parser.add_argument("--row-limit", type=int)
    parser.add_argument("--league", action="append", dest="leagues", help="Restrict to db_name. Repeatable.")
    parser.add_argument(
        "--default-leagues",
        action="store_true",
        help="Restrict to the same canonical 18-league batch used by draft_cross_table_batch.py.",
    )
    parser.add_argument("--min-support", type=int, default=50)
    parser.add_argument("--min-picks", type=int, default=100)
    parser.add_argument("--min-years", type=int, default=12)
    parser.add_argument("--min-abs-z", type=float, default=2.5)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument(
        "--feature-prefix",
        action="append",
        dest="feature_prefixes",
        help="Restrict scored feature events to feature_type prefixes. Repeatable.",
    )
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    load_env_file()
    db_names = args.leagues or (DEFAULT_LEAGUES if args.default_leagues else None)
    out = args.out or ROOT / "tmp" / f"draft_cross_table_fleet_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    out.parent.mkdir(parents=True, exist_ok=True)

    print("querying_fleet_matrix", flush=True)
    if db_names:
        print(f"scope_leagues={len(db_names)}", flush=True)
    rows = fly_query_with_retry(build_cross_table_base_sql(None, row_limit=args.row_limit, db_names=db_names))

    import pandas as pd

    df = pd.DataFrame(rows)
    print(f"rows={len(df)} cols={len(df.columns) if not df.empty else 0}", flush=True)
    if df.empty:
        payload = {"status": "empty", "rows": 0, "states": []}
        out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        print(f"out={out}", flush=True)
        return 0

    print("building_features", flush=True)
    features = build_wide_features(df, min_support=args.min_support)
    print(f"feature_events={len(features)}", flush=True)
    if args.feature_prefixes:
        prefixes = tuple(args.feature_prefixes)
        features = [row for row in features if str(row.get("feature_type") or "").startswith(prefixes)]
        print(f"filtered_feature_events={len(features)} prefixes={list(prefixes)}", flush=True)

    print("scoring_inefficiencies", flush=True)
    raw = _score_league_inefficiencies(
        df,
        features,
        min_picks=args.min_picks,
        min_years=args.min_years,
        min_abs_z=args.min_abs_z,
        limit=args.limit,
    )
    states = validate_inefficiency_candidates(raw)
    states = sorted(
        states,
        key=lambda row: (
            row.get("promotion_rank") is None,
            row.get("promotion_rank") or 9999,
            -abs(float(row.get("shrunk_z_score") or 0)),
        ),
    )
    payload = {
        "status": "ok",
        "rows": len(df),
        "scope_leagues": db_names,
        "feature_events": len(features),
        "raw_count": len(raw),
        "promotion_counts": level_counts(states),
        "top_states": [compact_state(row) for row in states[:25]],
    }
    out.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print(json.dumps(payload["promotion_counts"], default=str), flush=True)
    print(f"out={out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
