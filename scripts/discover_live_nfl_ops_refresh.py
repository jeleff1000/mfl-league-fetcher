#!/usr/bin/env python3
"""Discover the one safe live NFL ops refresh scope for a scheduler date.

This command is read-only: it loads the public NFLverse schedule, emits a
small JSON receipt, and never constructs a Fly target or an ops artifact.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DATA_SCRIPTS = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(DATA_SCRIPTS))

from multi_league.data_fetchers.live_nfl_ops_refresh import discover_scheduled_refresh_scope  # noqa: E402


SCHEDULE_URL = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.parquet"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--game-date", required=True, help="America/New_York calendar date in YYYY-MM-DD form")
    parser.add_argument("--output", type=Path, required=True, help="JSON no-op or ready-scope receipt")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    schedule = pd.read_parquet(SCHEDULE_URL)
    scope = discover_scheduled_refresh_scope(schedule, season=args.season, game_date=args.game_date)
    payload: dict[str, object] = {
        "status": "ready" if scope is not None else "no-op",
        "game_date": args.game_date,
        "scope": scope,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
