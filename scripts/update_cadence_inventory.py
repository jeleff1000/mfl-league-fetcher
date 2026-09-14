#!/usr/bin/env python3
"""Inventory table/column update cadence for the September batch plan.

The goal is not to execute any writes. This script turns canonical DDL plus
optional live Fly schemas into a reviewable CSV/Markdown map:

    table,column,type,source,cadence,scope,recommended_publish,reason

Run examples:

    python scripts/update_cadence_inventory.py --summary
    python scripts/update_cadence_inventory.py --csv scripts/_artifacts/update_cadence.csv
    python scripts/update_cadence_inventory.py --include-live-ops --summary

When --include-live-ops is used, DATABASE_SERVER_URL and DATABASE_READ_TOKEN
are read from the environment or a simple .env file in the repo root.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import urllib.request
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_OUT = ROOT / "scripts" / "_artifacts"

sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS  # noqa: E402
from multi_league.core.canonical_draft import DRAFT_SCHEMA  # noqa: E402
from multi_league.core.canonical_matchup import (  # noqa: E402
    MATCHUP_SCHEMA,
    PLAYOFF_SIM_COLUMNS,
    SCHEDULE_LUCK_SIM_COLUMNS,
    SOS_LUCK_SIM_COLUMNS,
)
from multi_league.core.canonical_player import PLAYER_FANTASY_SCHEMA  # noqa: E402
from multi_league.core.canonical_schedule import SCHEDULE_SCHEMA  # noqa: E402
from multi_league.core.canonical_transaction import TRANSACTION_SCHEMA  # noqa: E402
from multi_league.core.local_db import CONFIG_TABLE_COLUMN_TYPES, _TABLE_COLUMN_TYPES  # noqa: E402


@dataclass(frozen=True)
class ColumnCadence:
    table: str
    column: str
    dtype: str
    source: str
    cadence: str
    scope: str
    recommended_publish: str
    reason: str


CORE_SCHEMAS: dict[str, list[tuple[str, str, str]]] = {
    "player_fantasy": PLAYER_FANTASY_SCHEMA,
    "matchup": MATCHUP_SCHEMA,
    "draft": DRAFT_SCHEMA,
    "transactions": TRANSACTION_SCHEMA,
    "schedule": SCHEDULE_SCHEMA,
}

STATIC_IDENTITY_NAMES = {
    "db_name",
    "league_id",
    "platform",
    "manager",
    "manager_guid",
    "franchise_id",
    "franchise_name",
    "team_key",
    "team_name",
    "player",
    "NFL_player_id",
    "yahoo_player_id",
    "sleeper_player_id",
    "espn_player_id",
    "fleaflicker_player_id",
    "player_week",
    "manager_week",
    "manager_year",
    "cumulative_week",
}

MATCHUP_CAREER_BROADCAST = {
    "manager_all_time_ranking",
    "manager_all_time_percentile",
    "manager_all_time_gp",
    "manager_all_time_wins",
    "manager_all_time_losses",
    "manager_all_time_ties",
    "manager_all_time_win_pct",
    "cumulative_wins",
    "cumulative_losses",
    "cumulative_ties",
    "win_streak",
    "loss_streak",
}

MATCHUP_SEASON_TO_DATE = {
    "wins_to_date",
    "losses_to_date",
    "ties_to_date",
    "points_scored_to_date",
    "playoff_seed_to_date",
    "weekly_mean",
    "weekly_median",
    "manager_season_mean",
    "manager_season_median",
    "manager_season_ranking",
    "inflation_rate",
    "final_playoff_seed",
    "playoff_seed",
    "playoff_round",
    "playoff_round_num",
    "playoff_week_index",
    "postseason",
    "is_bye_week",
    "placement_rank",
    "placement_game",
    "season_result",
}

PLAYER_CURRENT_SEASON_BROADCAST_PREFIXES = (
    "season_",
    "player_lamar_",
    "manager_lamar_",
    "bench_lamar_",
    "replacement_ppg_",
)

PLAYER_CAREER_BROADCAST_NAMES = {
    "position_alltime_rank",
    "all_players_alltime_rank",
    "manager_position_alltime_rank",
    "manager_player_alltime_rank",
    "alltime_ppg",
    "weighted_ppg",
    "consistency_score",
}

PLAYER_CAREER_BROADCAST_SUFFIXES = ("_alltime_rank", "_alltime_pct")

PLAYER_SEASON_BROADCAST_SUFFIXES = ("_season_rank", "_season_pct")

WEEK_LOCAL_NAMES = {
    "fantasy_points",
    "bonus_points",
    "te_premium_points",
    "projected_points",
    "team_points",
    "opponent_points",
    "margin",
    "win",
    "loss",
    "tie",
    "matchup_name",
    "team_1",
    "team_2",
    "above_league_median",
    "below_league_median",
    "teams_beat_this_week",
    "opponent_teams_beat_this_week",
    "league_weekly_mean",
    "league_weekly_median",
    "close_margin",
    "total_matchup_score",
    "expected_spread",
    "expected_odds",
    "proj_score_error",
    "abs_proj_score_error",
    "above_proj_score",
    "below_proj_score",
    "win_vs_spread",
    "lose_vs_spread",
    "underdog_wins",
    "favorite_losses",
    "proj_wins",
    "proj_losses",
    "gpa",
    "opponent",
    "opponent_franchise_id",
    "opponent_year",
    "position_rank",
    "position_week_rank",
    "all_players_week_rank",
    "all_players_week_pct",
    "optimal_player",
    "optimal_position",
    "optimal_points",
    "lineup_efficiency",
    "bench_points",
    "league_wide_optimal_player",
    "league_wide_optimal_position",
    "starter_baseline_lamar",
    "above_baseline",
    "below_baseline",
    "odds_delta",
    "team_above_baseline",
    "team_below_baseline",
}

WEEK_LOCAL_SUFFIXES = ("_week_rank", "_week_pct")

SIM_COLUMNS = set(SCHEDULE_LUCK_SIM_COLUMNS) | set(SOS_LUCK_SIM_COLUMNS) | set(PLAYOFF_SIM_COLUMNS)
PLAYER_SIM_COLUMNS = {
    "clutch_equity",
    "starter_baseline_lamar",
    "above_baseline",
    "below_baseline",
    "odds_delta",
    "team_above_baseline",
    "team_below_baseline",
}


def load_dotenv() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def fly_query(sql: str, database: str) -> list[dict]:
    url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not url or not token:
        raise RuntimeError("DATABASE_SERVER_URL and DATABASE_READ_TOKEN are required for --include-live-ops")
    payload = json.dumps({"sql": sql, "database": database}).encode("utf-8")
    req = urllib.request.Request(
        f"{url}/query",
        data=payload,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=75) as resp:
        return json.loads(resp.read().decode("utf-8"))


def live_columns(database: str, schema: str, table: str) -> list[tuple[str, str, str]]:
    sql = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = '{schema}'
      AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    rows = fly_query(sql, database)
    return [(str(r["column_name"]), str(r["data_type"]), "live") for r in rows]


def classify_core(table: str, column: str, source: str) -> tuple[str, str, str, str]:
    lower = column.lower()

    if source == "system" or column in STATIC_IDENTITY_NAMES:
        return (
            "identity_or_config",
            "row identity; changes only on import, identity repair, or league setting repair",
            "same as owning table",
            "Keep in owning table.",
        )

    if table == "matchup":
        if column in SIM_COLUMNS or source == "sim":
            return (
                "current_season_simulation",
                "current season matchup rows; rerun after each completed week or sim model change",
                "replace (db_name, year) for active season",
                "Do not rewrite locked seasons unless sim/model/settings logic changes.",
            )
        if column in MATCHUP_CAREER_BROADCAST:
            return (
                "career_broadcast_on_weekly_rows",
                "entire league history currently changes because career/rank snapshots are stored on weekly rows",
                "replace (db_name) if kept; preferred sidecar/matchup_career",
                "High-value sidecar candidate.",
            )
        if column in MATCHUP_SEASON_TO_DATE:
            return (
                "current_season_snapshot",
                "current season rows; old seasons locked after season finalization",
                "replace (db_name, year)",
                "Correction to an old week requires recomputing that season from changed week forward.",
            )
        if source in {"api", "join_key"}:
            return (
                "weekly_fact",
                "new/changed matchup week only",
                "replace (db_name, year, week)",
                "Historical rows should not move unless API/backfill corrections arrive.",
            )
        return (
            "week_local_derived",
            "new/changed matchup week only",
            "replace (db_name, year, week)",
            "Derived from week-local matchup/player inputs.",
        )

    if table == "player_fantasy":
        if column in PLAYER_SIM_COLUMNS or source == "sim":
            return (
                "current_season_simulation",
                "current season player rows after playoff odds/clutch run",
                "replace (db_name, year)",
                "Clutch depends on playoff odds deltas and should follow sim finalization.",
            )
        if (
            column in PLAYER_CAREER_BROADCAST_NAMES
            or lower.endswith(PLAYER_CAREER_BROADCAST_SUFFIXES)
            or lower.startswith("manager_") and "alltime" in lower
        ):
            return (
                "career_broadcast_on_weekly_rows",
                "entire league player history can change because all-time ranks/PPG are stored on weekly rows",
                "replace (db_name) if kept; preferred sidecar/player_fantasy_career",
                "Largest league-specific history rewrite candidate.",
            )
        if lower.endswith(PLAYER_SEASON_BROADCAST_SUFFIXES) or lower in {
            "season_ppg",
            "season_games",
            "rolling_point_total",
            "player_lamar_ytd",
            "manager_lamar_ytd",
            "bench_lamar_ytd",
            "manager_player_season_rank",
            "manager_player_season_pct",
            "manager_position_season_rank",
        }:
            return (
                "current_season_snapshot",
                "current season player rows; old seasons locked unless correction/backfill",
                "replace (db_name, year)",
                "Adding a week changes season totals/ranks for current season.",
            )
        if column in WEEK_LOCAL_NAMES or lower.endswith(WEEK_LOCAL_SUFFIXES):
            return (
                "week_local_derived",
                "new/changed player week only",
                "replace (db_name, year, week)",
                "Derived from one week of stats, roster, scoring, or optimal lineup.",
            )
        if lower.startswith(PLAYER_CURRENT_SEASON_BROADCAST_PREFIXES) and lower.endswith("_ytd"):
            return (
                "current_season_snapshot",
                "current season player rows",
                "replace (db_name, year)",
                "YTD cumulative values change for current season and downstream rows after corrections.",
            )
        if source in {"api", "join_key"}:
            return (
                "weekly_fact",
                "new/changed roster/player week only",
                "replace (db_name, year, week)",
                "Historical rows should be locked unless platform corrections arrive.",
            )
        return (
            "week_local_derived",
            "new/changed player week unless rule/settings correction",
            "replace (db_name, year, week)",
            "Default SQL enrichment bucket.",
        )

    if table == "draft":
        if source in {"api", "join_key"}:
            return (
                "season_fact",
                "draft year rows; normally immutable after draft import",
                "replace (db_name, year)",
                "Current-season draft rows can arrive before player outcomes exist.",
            )
        if lower in {"manager_draft_percentile_alltime", "manager_total_lamar", "manager_avg_lamar"}:
            return (
                "career_broadcast_on_pick_rows",
                "league draft history can change if all-time draft percentiles/manager totals are stored on picks",
                "replace (db_name) if kept; preferred draft rollup sidecar",
                "Use draft_manager_* tables for career values.",
            )
        return (
            "current_season_outcome",
            "current draft season rows update weekly as fantasy outcomes/LAMAR accrue",
            "replace (db_name, year)",
            "Old draft seasons should be locked unless player or scoring backfill changes.",
        )

    if table == "transactions":
        if source in {"api", "join_key"}:
            return (
                "weekly_fact",
                "new/changed transaction week only",
                "replace (db_name, year, week)",
                "Week 1 is large because imports include offseason/preseason transactions.",
            )
        if lower.endswith("_ros") or "_ros_" in lower or lower in {
            "transaction_score",
            "transaction_grade",
            "score_percentile",
            "trade_percentile",
        }:
            return (
                "current_season_outcome",
                "current season transaction rows update weekly as rest-of-season outcome becomes known",
                "replace (db_name, year)",
                "Old seasons lock after finalization unless player/scoring corrections change history.",
            )
        return (
            "week_local_or_identity_enrichment",
            "new/changed transaction week, with current-season recompute for outcome metrics",
            "replace (db_name, year, week) or (db_name, year) by column family",
            "Split transaction publish by fact vs outcome columns if optimizing further.",
        )

    if table == "schedule":
        if source in {"api", "join_key", "enrichment"}:
            return (
                "season_schedule",
                "season schedule/bracket rows; usually set once and repaired by season",
                "replace (db_name, year)",
                "Do not rewrite all league history for weekly score updates.",
            )

    return (
        "table_default",
        "depends on owning table",
        "same as owning table",
        "Review if this table becomes a weekly bottleneck.",
    )


def classify_aggregate(table: str, column: str, dtype: str) -> ColumnCadence:
    spec = AGGREGATE_TABLE_SPECS[table]
    desc = spec.description

    if table.endswith("_season") or table.endswith("_season_all") or table in {
        "matchup_season",
        "matchup_h2h_season",
        "h2h_season",
        "schedule_swap_season",
        "standings_by_year",
        "draft_manager_season",
        "transaction_manager_season",
        "transaction_report_card",
    }:
        cadence = "current_season_rollup"
        scope = "active (db_name, year); old seasons locked after finalization"
        publish = "replace (db_name, year)"
    elif table.endswith("_career") or table.endswith("_career_all") or "career" in table:
        cadence = "career_rollup"
        scope = "entire league career rollup, small row count"
        publish = "replace (db_name)"
    elif table.startswith("homepage_"):
        cadence = "homepage_payload"
        scope = "small per-league payload after all weekly updates and sims"
        publish = "replace (db_name)"
    elif table in {"all_play", "schedule_swap"}:
        cadence = "weekly_matrix"
        scope = "active (db_name, year, week), or active season for corrections"
        publish = "replace (db_name, year, week)"
    else:
        cadence = "aggregate"
        scope = "depends on source tables"
        publish = "same as aggregate grain"

    return ColumnCadence(
        table=table,
        column=column,
        dtype=dtype,
        source="aggregate",
        cadence=cadence,
        scope=scope,
        recommended_publish=publish,
        reason=desc,
    )


def classify_config(table: str, column: str, dtype: str) -> ColumnCadence:
    if table == "league_settings" or column in {"year", "keeper_rules_json", "league_rules_json"}:
        cadence = "season_config"
        scope = "replace (db_name, year) when settings or rules change"
        publish = "replace (db_name, year) or replace (db_name) for small config tables"
    else:
        cadence = "identity_or_config"
        scope = "changes only on user config, identity repair, or import metadata update"
        publish = "replace (db_name)"
    return ColumnCadence(table, column, dtype, "config", cadence, scope, publish, "Configuration/control-plane data.")


def classify_ops(table: str, column: str, dtype: str) -> ColumnCadence:
    lower = column.lower()

    if table == "player_bio":
        if lower in {
            "nfl_player_id",
            "player",
            "nfl_position",
            "birth_date",
            "height",
            "weight",
            "college",
            "draft_year",
            "draft_round",
            "draft_overall",
            "nfl_draft_team",
            "pfr_id",
            "yahoo_player_id",
            "sleeper_player_id",
            "espn_id",
        }:
            cadence = "static_bio_or_identity"
            scope = "insert new rookies/identity fixes; otherwise locked"
            publish = "ops artifact replace or targeted upsert before league import"
        elif lower in {"latest_team", "status", "headshot_url", "career_games", "first_year", "last_year", "years_active"}:
            cadence = "periodic_bio_refresh"
            scope = "new/changed players; not league-multiplied"
            publish = "ops artifact replace or targeted player_bio patch"
        else:
            cadence = "bio_enrichment"
            scope = "occasional backfill/source improvement"
            publish = "ops artifact replace or targeted patch"
        return ColumnCadence(table, column, dtype, "ops_live", cadence, scope, publish, "Player bio lives once globally.")

    if table in {"player_nfl_season", "player_nfl_season_all"}:
        return ColumnCadence(
            table,
            column,
            dtype,
            "ops_live",
            "current_season_global_rollup",
            "active NFL season rows; old seasons locked unless source correction",
            "offline rebuild active season artifact, then promote",
            "Season aggregate table should be rebuilt outside serving DuckDB.",
        )

    if table in {"player_nfl_career", "player_nfl_career_all"}:
        return ColumnCadence(
            table,
            column,
            dtype,
            "ops_live",
            "global_career_rollup",
            "all active NFL player career rows; about 24k rows",
            "offline rebuild whole table, then promote",
            "Small enough to rebuild weekly as a finished artifact.",
        )

    if lower in {
        "player_week",
        "nfl_player_id",
        "player",
        "position",
        "nfl_position",
        "nfl_team",
        "opponent_nfl_team",
        "headshot_url",
        "year",
        "week",
        "season_type",
        "data_source",
        "fantasy_position",
    }:
        cadence = "weekly_global_fact"
        scope = "new/changed NFL player-week rows"
        publish = "replace affected NFL (year, week) artifact"
        reason = "Identity/context for player-week facts."
    elif lower.startswith("rank_alltime_"):
        cadence = "global_alltime_rank"
        scope = "global rank sidecar; adding a week can shift historical ranks"
        publish = "offline rebuild rank sidecar or materialized compatibility artifact"
        reason = "Do not update these in-place on the live wide table."
    elif lower.startswith("rank_season_"):
        cadence = "current_season_global_rank"
        scope = "active NFL season rank sidecar; old seasons locked unless correction"
        publish = "offline rebuild active season rank artifact"
        reason = "Season ranks slide for current season when new games arrive."
    elif lower.startswith("rank_"):
        cadence = "weekly_global_rank"
        scope = "affected NFL week only"
        publish = "replace affected NFL (year, week) rank artifact"
        reason = "Weekly ranks only need the affected week unless source corrections touch old weeks."
    elif lower.startswith("ppg_alltime_"):
        cadence = "global_player_career_metric"
        scope = "active players' historical rows if stored wide; preferred career sidecar"
        publish = "offline rebuild sidecar/materialized compatibility artifact"
        reason = "Current implementation broadcasts all-time PPG across player history."
    elif lower.startswith(("ppg_season_", "consistency_", "rolling_total_")):
        cadence = "current_season_player_metric"
        scope = "active NFL season rows"
        publish = "offline rebuild active season metric artifact"
        reason = "Season metrics change during the active season."
    elif lower.startswith(("rolling_3_", "rolling_5_", "weighted_ppg_")):
        cadence = "new_week_or_forward_from_correction"
        scope = "new NFL week; corrections require affected player forward window"
        publish = "replace affected week/active season artifact"
        reason = "Rolling/as-of metrics do not need to rewrite older rows for normal new weeks."
    elif lower.startswith("avg_pts_next_year_"):
        cadence = "year_boundary_metric"
        scope = "do not update weekly unless intentionally allowing partial next-year values"
        publish = "season-final artifact only"
        reason = "Forward-looking metric should be finalized deliberately, not churn weekly."
    elif lower.startswith(("fpts_", "pts_", "bonus_")):
        cadence = "weekly_global_scoring"
        scope = "new/changed NFL player-week rows"
        publish = "replace affected NFL (year, week) artifact"
        reason = "Scoring components derive from raw weekly stats."
    else:
        cadence = "raw_or_source_stat"
        scope = "new/changed NFL player-week rows; targeted historical corrections"
        publish = "replace affected NFL (year, week) artifact"
        reason = "Raw/source stat column."

    return ColumnCadence(table, column, dtype, "ops_live", cadence, scope, publish, reason)


def iter_columns(include_live_ops: bool) -> Iterable[ColumnCadence]:
    for table, schema in CORE_SCHEMAS.items():
        for column, dtype, source in schema:
            cadence, scope, publish, reason = classify_core(table, column, source)
            yield ColumnCadence(table, column, dtype, source, cadence, scope, publish, reason)

    for table, column_types in _TABLE_COLUMN_TYPES.items():
        if table in CORE_SCHEMAS:
            continue
        for column, dtype in column_types.items():
            yield classify_config(table, column, dtype)

    for table, spec in AGGREGATE_TABLE_SPECS.items():
        for column, dtype in spec.column_types.items():
            yield classify_aggregate(table, column, dtype)

    if include_live_ops:
        for table in (
            "nfl_player_stats_all",
            "player_bio",
            "player_nfl_season",
            "player_nfl_career",
            "player_nfl_season_all",
            "player_nfl_career_all",
        ):
            for column, dtype, _source in live_columns("___ops", "nfl_historical", table):
                yield classify_ops(table, column, dtype)


def print_summary(rows: list[ColumnCadence]) -> None:
    by_table = Counter(r.table for r in rows)
    by_cadence = Counter(r.cadence for r in rows)
    print("Tables")
    for table, count in sorted(by_table.items()):
        print(f"  {table}: {count}")
    print("\nCadence buckets")
    for cadence, count in by_cadence.most_common():
        print(f"  {cadence}: {count}")


def write_csv(rows: list[ColumnCadence], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "table",
                "column",
                "dtype",
                "source",
                "cadence",
                "scope",
                "recommended_publish",
                "reason",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--include-live-ops", action="store_true", help="Include live ___ops super/bio schemas.")
    parser.add_argument("--summary", action="store_true", help="Print a summary to stdout.")
    parser.add_argument("--csv", type=Path, help="Write full column cadence CSV.")
    args = parser.parse_args()

    if args.include_live_ops:
        load_dotenv()

    rows = list(iter_columns(args.include_live_ops))

    if args.csv:
        write_csv(rows, args.csv)
        print(f"Wrote {len(rows):,} column cadence rows to {args.csv}")

    if args.summary or not args.csv:
        print_summary(rows)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
