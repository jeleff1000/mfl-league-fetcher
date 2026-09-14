#!/usr/bin/env python3
"""Mine league-level draft intelligence states from the live fleet.

Manager profiling asks, "who behaves differently?" League intelligence asks,
"what does this room misprice?" This runner keeps that second pass local and
artifact-only: it reads Fly, validates candidates through the state-machine
promotion gate, and writes JSON files that the product can lazy-load instantly.
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from datetime import datetime, timezone, UTC
import json
import os
from pathlib import Path
import sys
import time
from typing import Any
from collections.abc import Callable


REPO_ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_ROOT = REPO_ROOT / "fantasy_football_data_scripts"
for module_path in (REPO_ROOT, SCRIPTS_ROOT, SCRIPTS_ROOT / "multi_league"):
    path_str = str(module_path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.core.readers.fly_reader import FlyReader
from multi_league.transformations.draft.league_inefficiency_miner import (
    run_league_inefficiency_miner_fly,
)
from multi_league.transformations.draft.tendency_validation import (
    validate_inefficiency_candidates,
)


MODEL_VERSION = "draft-league-intelligence-live-v0.1"


def _load_env() -> None:
    for name in [".env", ".env.local"]:
        path = REPO_ROOT / name
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _read_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: Any) -> None:
    tmp_path = path.with_name(f"{path.name}.tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True, default=str), encoding="utf-8")
    tmp_path.replace(path)


def _q(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed


def fetch_leagues(reader: FlyReader, limit: int | None = None) -> list[str]:
    sql = """
    SELECT
        db_name,
        COUNT(*) AS picks,
        COUNT(DISTINCT year) AS seasons
    FROM public.draft
    WHERE db_name IS NOT NULL
      AND year IS NOT NULL
      AND manager IS NOT NULL
      AND NFL_player_id IS NOT NULL
      AND manager_lamar IS NOT NULL
      AND expected_lamar IS NOT NULL
    GROUP BY db_name
    HAVING COUNT(*) >= 80
       AND COUNT(DISTINCT year) >= 2
    ORDER BY picks DESC
    """
    if limit:
        sql += f"\nLIMIT {int(limit)}"
    return [str(row["db_name"]) for row in reader.query(sql, database="___leagues")]


def call_with_retry(fn: Callable[[], list[dict[str, Any]]], *, attempts: int = 4) -> list[dict[str, Any]]:
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as exc:  # Fly can wake, restart, or briefly throttle.
            last_error = exc
            retryable = any(
                token in str(exc).lower()
                for token in ("503", "busy", "starting", "timeout", "network", "ssl", "eof", "max retries")
            )
            if not retryable or attempt == attempts:
                raise
            wait_seconds = 15 * attempt
            print(
                json.dumps({"retry": attempt, "wait_seconds": wait_seconds, "error": str(exc)[:240]}),
                flush=True,
            )
            time.sleep(wait_seconds)
    raise RuntimeError(f"query failed after retries: {last_error}")


def compact_raw(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "db_name": row.get("db_name"),
        "scope_type": row.get("scope_type"),
        "scope_key": row.get("scope_key"),
        "scope_label": row.get("scope_label"),
        "feature_type": row.get("feature_type"),
        "feature_value": row.get("feature_value"),
        "picks": row.get("picks"),
        "years_seen": row.get("years_seen"),
        "earliest_year": row.get("earliest_year"),
        "latest_year": row.get("latest_year"),
        "recency_weight": row.get("recency_weight"),
        "positive_years": row.get("positive_years"),
        "negative_years": row.get("negative_years"),
        "repeatability": row.get("repeatability"),
        "observed_capital": row.get("observed_capital"),
        "observed_residual": row.get("observed_residual"),
        "expected_residual": row.get("expected_residual"),
        "excess_residual": row.get("excess_residual"),
        "observed_pick_score": row.get("observed_pick_score"),
        "expected_pick_score": row.get("expected_pick_score"),
        "pick_score_delta": row.get("pick_score_delta"),
        "value_z_score": row.get("value_z_score"),
        "confidence": row.get("confidence"),
        "evidence_level": row.get("evidence_level"),
        "model_version": row.get("model_version"),
    }


def compact_state(row: dict[str, Any]) -> dict[str, Any]:
    return {
        **row,
        "profile_kind": "league",
        "profile_id": f"league_value_inefficiency:{row.get('feature_type')}:{row.get('feature_value')}:{row.get('signal_direction')}",
        "profile_status": "catalog_ready" if row.get("promotion_level") == "state_ready" else "catalog_watch",
        "assignment_score": row.get("validation_score"),
    }


def summarize(
    raw_rows: list[dict[str, Any]], state_rows: list[dict[str, Any]], failures: list[dict[str, Any]], started: float
) -> dict[str, Any]:
    promoted = [row for row in state_rows if row.get("promotion_level") != "explore_only"]
    by_level = Counter(str(row.get("promotion_level")) for row in state_rows)
    by_feature = Counter(str(row.get("feature_type")) for row in promoted)
    by_league = Counter(str(row.get("db_name")) for row in promoted)
    return {
        "model_version": MODEL_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "elapsed_sec": round(time.time() - started, 1),
        "raw_signal_count": len(raw_rows),
        "state_count": len(state_rows),
        "promoted_state_count": len(promoted),
        "league_count": len({row.get("db_name") for row in raw_rows}),
        "indexed_league_count": len(by_league),
        "failed_leagues": len(failures),
        "promotion_counts": dict(by_level),
        "top_feature_types": [{"feature_type": key, "count": value} for key, value in by_feature.most_common(40)],
        "top_leagues": [{"db_name": key, "count": value} for key, value in by_league.most_common(25)],
        "top_states": sorted(
            [
                {
                    "db_name": row.get("db_name"),
                    "promotion_level": row.get("promotion_level"),
                    "feature_type": row.get("feature_type"),
                    "feature_value": row.get("feature_value"),
                    "signal_direction": row.get("signal_direction"),
                    "validation_score": row.get("validation_score"),
                    "shrunk_z_score": row.get("shrunk_z_score"),
                    "value_delta": row.get("value_delta"),
                    "pick_score_delta": row.get("pick_score_delta"),
                    "picks": row.get("picks"),
                    "years_seen": row.get("years_seen"),
                }
                for row in promoted
            ],
            key=lambda row: (_float(row.get("validation_score")), abs(_float(row.get("shrunk_z_score")))),
            reverse=True,
        )[:50],
    }


def mine_league(
    db_name: str,
    *,
    min_picks: int,
    min_years: int,
    min_abs_z: float,
    limit: int,
    promoted_only: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any] | None]:
    league_started = time.time()
    try:
        raw = call_with_retry(
            lambda: run_league_inefficiency_miner_fly(
                db_name,
                min_picks=min_picks,
                min_years=min_years,
                min_abs_z=min_abs_z,
                limit=limit,
            )
        )
        states = validate_inefficiency_candidates(raw, include_explore=not promoted_only)
        states = [
            compact_state(row) for row in states if not promoted_only or row.get("promotion_level") != "explore_only"
        ]
        raw_compact = [compact_raw(row) for row in raw]
        event = {
            "db_name": db_name,
            "raw": len(raw),
            "states": len(states),
            "promoted": sum(1 for row in states if row.get("promotion_level") != "explore_only"),
            "elapsed_sec": round(time.time() - league_started, 1),
        }
        return raw_compact, states, event, None
    except Exception as exc:
        event = {
            "db_name": db_name,
            "error": str(exc)[:1000],
            "elapsed_sec": round(time.time() - league_started, 1),
        }
        return [], [], event, event


def main() -> int:
    parser = argparse.ArgumentParser(description="Build league-level draft intelligence artifacts from Fly.")
    parser.add_argument("--run-dir", type=Path, required=True, help="Existing SOTA artifact run directory to augment.")
    parser.add_argument("--league", action="append", dest="leagues", help="Restrict to db_name. Repeatable.")
    parser.add_argument("--league-limit", type=int, default=None)
    parser.add_argument("--min-picks", type=int, default=8)
    parser.add_argument("--min-years", type=int, default=2)
    parser.add_argument("--min-abs-z", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--promoted-only", action="store_true", help="Store only state_ready/briefing_watch rows.")
    parser.add_argument("--resume", action="store_true", help="Skip leagues already present in league_signals.json.")
    parser.add_argument("--workers", type=int, default=4, help="Concurrent league queries.")
    parser.add_argument("--checkpoint-interval", type=int, default=50)
    args = parser.parse_args()

    _load_env()
    args.run_dir.mkdir(parents=True, exist_ok=True)
    reader = FlyReader()
    leagues = args.leagues or fetch_leagues(reader, limit=args.league_limit)
    existing_raw: list[dict[str, Any]] = _read_json(args.run_dir / "league_signals.json", []) if args.resume else []
    existing_states: list[dict[str, Any]] = (
        _read_json(args.run_dir / "league_state_candidates.json", []) if args.resume else []
    )
    completed = {str(row.get("db_name")) for row in existing_states}
    raw_rows = [row for row in existing_raw if str(row.get("db_name")) in completed]
    state_rows = list(existing_states)
    failures: list[dict[str, Any]] = _read_json(args.run_dir / "league_failures.json", []) if args.resume else []
    started = time.time()
    log_path = args.run_dir / "league_batches.jsonl"

    pending_leagues = [db_name for db_name in leagues if not (args.resume and db_name in completed)]

    def handle_result(
        completed_count: int,
        raw: list[dict[str, Any]],
        states: list[dict[str, Any]],
        event: dict[str, Any],
        failure: dict[str, Any] | None,
    ) -> None:
        raw_rows.extend(raw)
        state_rows.extend(states)
        if failure:
            failures.append(failure)
        event = {"index": completed_count, "leagues": len(pending_leagues), **event}
        with log_path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event, default=str) + "\n")
        print(json.dumps(event, default=str), flush=True)

        if completed_count % max(1, args.checkpoint_interval) == 0:
            _write_json(args.run_dir / "league_signals.json", raw_rows)
            _write_json(args.run_dir / "league_state_candidates.json", state_rows)
            _write_json(args.run_dir / "league_failures.json", failures)
            _write_json(args.run_dir / "league_summary.json", summarize(raw_rows, state_rows, failures, started))

    if args.workers <= 1:
        for index, db_name in enumerate(pending_leagues, start=1):
            raw, states, event, failure = mine_league(
                db_name,
                min_picks=args.min_picks,
                min_years=args.min_years,
                min_abs_z=args.min_abs_z,
                limit=args.limit,
                promoted_only=args.promoted_only,
            )
            handle_result(index, raw, states, event, failure)
    else:
        with ThreadPoolExecutor(max_workers=max(1, args.workers)) as executor:
            futures = {
                executor.submit(
                    mine_league,
                    db_name,
                    min_picks=args.min_picks,
                    min_years=args.min_years,
                    min_abs_z=args.min_abs_z,
                    limit=args.limit,
                    promoted_only=args.promoted_only,
                ): db_name
                for db_name in pending_leagues
            }
            for completed_count, future in enumerate(as_completed(futures), start=1):
                raw, states, event, failure = future.result()
                handle_result(completed_count, raw, states, event, failure)

    _write_json(args.run_dir / "league_signals.json", raw_rows)
    _write_json(args.run_dir / "league_state_candidates.json", state_rows)
    _write_json(args.run_dir / "league_failures.json", failures)
    summary = summarize(raw_rows, state_rows, failures, started)
    summary["params"] = vars(args)
    _write_json(args.run_dir / "league_summary.json", summary)
    print(json.dumps(summary, indent=2, default=str), flush=True)
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
