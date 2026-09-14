#!/usr/bin/env python3
"""Rebuild only the Fly NFL all-games career aggregate table."""

from __future__ import annotations

import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.aggregate_nfl_stats_fly import (  # noqa: E402
    CAREER_ALL_TABLE,
    SEASON_FACT_TABLE,
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


def main() -> int:
    os.environ.setdefault("NFL_AGGREGATE_CHUNK_SIZE", "8")
    os.environ.setdefault("NFL_RANK_CHUNK_SIZE", "12")
    load_env()
    validate_rank_specs()
    writer = LongFlyWriter()
    aggregate_cols, lamar_cols, _fpts_cols, _bonus_cols = discover_aggregate_columns(writer)
    use_season_fact_adjustments = table_exists(writer, SEASON_FACT_TABLE)
    backup_table(writer, CAREER_ALL_TABLE)
    build_stage_table(
        writer,
        CAREER_ALL_TABLE,
        aggregate_cols=aggregate_cols,
        lamar_cols=lamar_cols,
        include_playoffs=True,
        season=False,
        use_season_fact_adjustments=use_season_fact_adjustments,
    )
    swap_stage_to_live(writer, CAREER_ALL_TABLE)
    recompute_ranks(writer, CAREER_ALL_TABLE, rank_specs_for_scope("alltime"), "alltime")
    rows = verify_count(writer, CAREER_ALL_TABLE, include_playoffs=True, season=False)
    print({"career_all": rows}, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
