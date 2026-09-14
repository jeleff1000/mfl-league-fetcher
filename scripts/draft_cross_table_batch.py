#!/usr/bin/env python3
"""Run cross-table draft discovery for a batch of leagues.

Read-only by default: this pulls from Fly, validates candidates in memory, and
writes local JSONL progress so long runs can be resumed or inspected.
"""

from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime
import json
from pathlib import Path
import sys
import time
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
for path in (SCRIPTS_ROOT, SCRIPTS_ROOT / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


from multi_league.core.readers.fly_reader import FlyReader
from multi_league.transformations.draft.cross_table_miner import run_cross_table_miner_fly
from multi_league.transformations.draft.tendency_validation import (
    validate_inefficiency_candidates,
    validate_tendency_candidates,
)


DEFAULT_LEAGUES = [
    "kmffl",
    "the_league",
    "nyu_ffl",
    "tfl_of_extraordinary_gentleman",
    "keeper_league",
    "degenerate_gamblers_football_league",
    "the_pigskin_platoon",
    "rock_hill_fantasy_league",
    "champions_branch_out",
    "theta_chi_oe",
    "playa_hater_s_ball",
    "h_town_auction",
    "wbffl",
    "arkaholics",
    "fantasy_elite",
    "dom_s_year",
    "keeper_league_7ae1",
    "the_precious_league",
]


KNOWN_PREFIXES = (
    "draft.nfl_team_api",
    "bio.conference",
    "draft.draft_age",
    "bio.college",
    "bio.nfl_draft_team",
)


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
        if key in {"DATABASE_SERVER_URL", "DATABASE_READ_TOKEN"}:
            value = value.strip().strip('"').strip("'")
            if value and key not in __import__("os").environ:
                __import__("os").environ[key] = value


def top_leagues(limit: int) -> list[str]:
    rows = FlyReader().query(
        f"""
        SELECT db_name
        FROM ___leagues.public.draft
        WHERE db_name IS NOT NULL
        GROUP BY db_name
        HAVING COUNT(*) >= 500
        ORDER BY COUNT(*) DESC
        LIMIT {int(limit)}
        """,
        database="___leagues",
    )
    return [str(row["db_name"]) for row in rows]


def level_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    return dict(Counter(row["promotion_level"] for row in rows))


def top_rows(rows: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (
            row.get("promotion_rank") is None,
            row.get("promotion_rank") or 9999,
            -float(row.get("validation_score") or 0),
        ),
    )
    keep = []
    for row in ordered[:limit]:
        keep.append(
            {
                "promotion_level": row.get("promotion_level"),
                "promotion_rank": row.get("promotion_rank"),
                "scope_label": row.get("scope_label"),
                "feature_type": row.get("feature_type"),
                "feature_value": row.get("feature_value"),
                "shrunk_z_score": row.get("shrunk_z_score"),
                "q_value": row.get("q_value"),
                "repeatability": row.get("repeatability"),
                "value_delta": row.get("value_delta"),
            }
        )
    return keep


def novel_rows(rows: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    novel = [
        row
        for row in rows
        if not str(row.get("feature_type") or "").startswith(KNOWN_PREFIXES)
        and row.get("promotion_level") != "explore_only"
    ]
    return top_rows(novel, limit=limit)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run cross-table draft discovery for multiple leagues.")
    parser.add_argument("--league", action="append", dest="leagues", help="League db_name. Repeatable.")
    parser.add_argument("--top", type=int, help="Use top N leagues by draft row count.")
    parser.add_argument("--out", type=Path, help="JSONL output path.")
    parser.add_argument("--min-support", type=int, default=5)
    parser.add_argument("--min-picks", type=int, default=8)
    parser.add_argument("--min-years", type=int, default=2)
    parser.add_argument("--min-abs-z", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    load_env_file()
    leagues = args.leagues or DEFAULT_LEAGUES
    if args.top:
        top = top_leagues(args.top)
        leagues = list(dict.fromkeys([*leagues, *top]))

    out = args.out or ROOT / "tmp" / f"draft_cross_table_batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jsonl"
    out.parent.mkdir(parents=True, exist_ok=True)

    print(f"out={out}", flush=True)
    print(f"leagues={len(leagues)}", flush=True)

    started = time.time()
    completed = 0
    failures = 0
    with out.open("a", encoding="utf-8") as handle:
        for index, db_name in enumerate(leagues, start=1):
            league_started = time.time()
            print(f"[{index}/{len(leagues)}] {db_name} start", flush=True)
            try:
                raw = run_cross_table_miner_fly(
                    db_name,
                    min_support=args.min_support,
                    min_picks=args.min_picks,
                    min_years=args.min_years,
                    min_abs_z=args.min_abs_z,
                    limit=args.limit,
                )
                manager_states = validate_tendency_candidates(raw["manager_tendencies"])
                ineff_states = validate_inefficiency_candidates(raw["league_inefficiencies"])
                payload = {
                    "db_name": db_name,
                    "status": "ok",
                    "seconds": round(time.time() - league_started, 2),
                    "raw_counts": {
                        "manager_tendencies": len(raw["manager_tendencies"]),
                        "league_inefficiencies": len(raw["league_inefficiencies"]),
                    },
                    "promotion_counts": {
                        "manager_tendencies": level_counts(manager_states),
                        "league_inefficiencies": level_counts(ineff_states),
                    },
                    "top_manager": top_rows(manager_states),
                    "top_league": top_rows(ineff_states),
                    "novel_manager": novel_rows(manager_states),
                    "novel_league": novel_rows(ineff_states),
                }
                completed += 1
                print(
                    f"[{index}/{len(leagues)}] {db_name} ok "
                    f"raw={payload['raw_counts']} promos={payload['promotion_counts']} "
                    f"seconds={payload['seconds']}",
                    flush=True,
                )
            except Exception as exc:
                failures += 1
                payload = {
                    "db_name": db_name,
                    "status": "error",
                    "seconds": round(time.time() - league_started, 2),
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
                print(f"[{index}/{len(leagues)}] {db_name} error {type(exc).__name__}: {exc}", flush=True)
            handle.write(json.dumps(payload, default=str) + "\n")
            handle.flush()

    print(
        f"done completed={completed} failures={failures} seconds={round(time.time() - started, 2)} out={out}",
        flush=True,
    )
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
