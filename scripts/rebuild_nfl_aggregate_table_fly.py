#!/usr/bin/env python3
"""Rebuild one Fly NFL aggregate cache table."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.aggregate_nfl_stats_fly import (  # noqa: E402
    CAREER_ALL_TABLE,
    CAREER_TABLE,
    SEASON_ALL_TABLE,
    SEASON_FACT_TABLE,
    SEASON_TABLE,
    LongFlyWriter,
    backup_table,
    build_stage_table,
    discover_aggregate_columns,
    load_env,
    rank_specs_for_scope,
    recompute_ranks,
    swap_stage_to_live,
    table_exists,
    validate_rank_specs,
    verify_count,
)


TARGETS = {
    "season": {
        "table": SEASON_TABLE,
        "include_playoffs": False,
        "season": True,
        "rank_scope": "season",
    },
    "season_all": {
        "table": SEASON_ALL_TABLE,
        "include_playoffs": True,
        "season": True,
        "rank_scope": "season",
    },
    "career": {
        "table": CAREER_TABLE,
        "include_playoffs": False,
        "season": False,
        "rank_scope": "alltime",
    },
    "career_all": {
        "table": CAREER_ALL_TABLE,
        "include_playoffs": True,
        "season": False,
        "rank_scope": "alltime",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("target", choices=sorted(TARGETS))
    parser.add_argument("--year", type=int, default=None, help="Optional single year for season targets.")
    parser.add_argument("--aggregate-chunk-size", type=int, default=4)
    parser.add_argument("--rank-chunk-size", type=int, default=8)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    os.environ.setdefault("NFL_AGGREGATE_CHUNK_SIZE", str(args.aggregate_chunk_size))
    os.environ.setdefault("NFL_RANK_CHUNK_SIZE", str(args.rank_chunk_size))
    load_env()
    validate_rank_specs()
    writer = LongFlyWriter()
    aggregate_cols, lamar_cols, _fpts_cols, _bonus_cols = discover_aggregate_columns(writer)
    use_season_fact_adjustments = table_exists(writer, SEASON_FACT_TABLE)
    spec = TARGETS[args.target]
    table = spec["table"]
    season = bool(spec["season"])
    if args.year is not None and not season:
        raise RuntimeError("--year is only supported for season aggregate targets")
    backup_table(writer, table)
    build_stage_table(
        writer,
        table,
        aggregate_cols=aggregate_cols,
        lamar_cols=lamar_cols,
        include_playoffs=bool(spec["include_playoffs"]),
        season=season,
        year=args.year,
        use_season_fact_adjustments=use_season_fact_adjustments,
    )
    swap_stage_to_live(writer, table)
    recompute_ranks(writer, table, rank_specs_for_scope(str(spec["rank_scope"])), str(spec["rank_scope"]))
    rows = verify_count(writer, table, include_playoffs=bool(spec["include_playoffs"]), season=season)
    print({args.target: rows}, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
