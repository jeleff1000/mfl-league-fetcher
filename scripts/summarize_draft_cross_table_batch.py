#!/usr/bin/env python3
"""Summarize local cross-table draft discovery JSONL output."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parent.parent
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
for path in (SCRIPTS_ROOT, SCRIPTS_ROOT / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


from multi_league.transformations.draft.state_registry import classify_feature


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_rows(path: Path) -> list[dict[str, Any]]:
    rows = []
    if not path.exists():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def _promotion_counts(rows: list[dict[str, Any]], section: str) -> Counter:
    counts: Counter = Counter()
    for row in rows:
        for level, count in (row.get("promotion_counts", {}).get(section, {}) or {}).items():
            counts[level] += int(count)
    return counts


def _family(feature_type: str, feature_value: str) -> str:
    return str(classify_feature(feature_type, feature_value)["state_family"])


def summarize(rows: list[dict[str, Any]], *, top: int = 12) -> dict[str, Any]:
    ok_rows = [row for row in rows if row.get("status") == "ok"]
    error_rows = [row for row in rows if row.get("status") != "ok"]
    manager_counts = _promotion_counts(ok_rows, "manager_tendencies")
    league_counts = _promotion_counts(ok_rows, "league_inefficiencies")

    league_groups: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in ok_rows:
        db_name = row.get("db_name")
        for signal in row.get("novel_league", []) or []:
            if signal.get("promotion_level") == "explore_only":
                continue
            feature_type = str(signal.get("feature_type") or "")
            feature_value = str(signal.get("feature_value") or "")
            direction = "positive" if _float(signal.get("shrunk_z_score")) >= 0 else "negative"
            key = (_family(feature_type, feature_value), feature_type, direction)
            group = league_groups.setdefault(
                key,
                {
                    "state_family": key[0],
                    "feature_type": feature_type,
                    "direction": direction,
                    "leagues": set(),
                    "examples": [],
                    "best_abs_z": 0.0,
                },
            )
            group["leagues"].add(db_name)
            group["best_abs_z"] = max(group["best_abs_z"], abs(_float(signal.get("shrunk_z_score"))))
            if len(group["examples"]) < 5:
                group["examples"].append(
                    {
                        "db_name": db_name,
                        "feature_value": feature_value,
                        "promotion_level": signal.get("promotion_level"),
                        "shrunk_z_score": signal.get("shrunk_z_score"),
                        "value_delta": signal.get("value_delta"),
                    }
                )

    recurring_league = []
    for group in league_groups.values():
        recurring_league.append(
            {
                **{key: value for key, value in group.items() if key != "leagues"},
                "league_count": len(group["leagues"]),
                "leagues": sorted(group["leagues"]),
            }
        )
    recurring_league.sort(key=lambda item: (item["league_count"], item["best_abs_z"]), reverse=True)

    manager_family_counts: Counter = Counter()
    manager_feature_counts: Counter = Counter()
    for row in ok_rows:
        for signal in row.get("novel_manager", []) or []:
            if signal.get("promotion_level") == "explore_only":
                continue
            feature_type = str(signal.get("feature_type") or "")
            feature_value = str(signal.get("feature_value") or "")
            manager_family_counts[_family(feature_type, feature_value)] += 1
            manager_feature_counts[feature_type] += 1

    return {
        "completed": len(ok_rows),
        "errors": [{"db_name": row.get("db_name"), "error": row.get("error")} for row in error_rows],
        "manager_promotion_counts": dict(manager_counts),
        "league_promotion_counts": dict(league_counts),
        "recurring_league_signals": recurring_league[:top],
        "novel_manager_state_families": manager_family_counts.most_common(top),
        "novel_manager_feature_types": manager_feature_counts.most_common(top),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize a draft cross-table batch JSONL file.")
    parser.add_argument("path", type=Path)
    parser.add_argument("--top", type=int, default=12)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()

    summary = summarize(load_rows(args.path), top=args.top)
    if args.json:
        print(json.dumps(summary, indent=2, default=str))
        return 0

    print(f"completed={summary['completed']} errors={len(summary['errors'])}")
    print(f"manager_promotions={summary['manager_promotion_counts']}")
    print(f"league_promotions={summary['league_promotion_counts']}")
    print("recurring_league_signals:")
    for row in summary["recurring_league_signals"]:
        print(
            f"  {row['league_count']} leagues | {row['state_family']} | "
            f"{row['feature_type']} | {row['direction']} | best_abs_z={row['best_abs_z']:.3f}"
        )
    print(f"novel_manager_state_families={summary['novel_manager_state_families']}")
    print(f"novel_manager_feature_types={summary['novel_manager_feature_types']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
