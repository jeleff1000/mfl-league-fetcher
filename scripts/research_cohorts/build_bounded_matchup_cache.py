"""Build the complete research lattice in bounded year windows.

The year databases are private implementation details of one build.  They are
merged into the final adaptive DuckDB and deleted before the command returns;
no shard artifacts are published or used by the application.
"""
from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb


TABLES = (
    "research_matchup_adaptive_weekly",
    "research_matchup_adaptive",
)
POSITION_GROUPS = ("QB", "RB", "WR", "TE", "K", "DEF", "IDP")
POSITION_SPLIT_START = 2020

PLAYER_FANTASY_COLUMNS = (
    "db_name", "year", "week", "cohort_teams", "cohort_roster",
    "cohort_scoring", "cohort_pass_td", "cohort_playoff_teams",
    "cohort_dynasty", "cohort_best_ball", "cohort_position_eligible",
    "position", "NFL_player_id", "manager", "is_rostered", "is_started",
    "fantasy_points", "clutch_equity", "win", "is_playoffs",
    "final_playoff_seed", "champion",
)
MATCHUP_COLUMNS = (
    "db_name", "year", "week", "manager", "win", "loss", "tie",
    "team_points", "opponent_points", "is_playoffs", "final_playoff_seed",
    "champion",
)


def materialize_year_snapshot(source: Path, target: Path, year: int) -> None:
    if target.exists():
        target.unlink()
    con = duckdb.connect(str(target))
    try:
        con.execute(f"ATTACH '{source.as_posix()}' AS src (READ_ONLY)")
        con.execute("CREATE SCHEMA public")
        player_columns = {row[0] for row in con.execute(
            "DESCRIBE src.public.player_fantasy"
        ).fetchall()}
        missing_player = set(PLAYER_FANTASY_COLUMNS) - player_columns
        if missing_player:
            raise RuntimeError(f"cache missing player rollup columns={sorted(missing_player)}")
        player_select = ", ".join(f'"{column}"' for column in PLAYER_FANTASY_COLUMNS)
        con.execute(f"""
          CREATE TABLE public.player_fantasy AS
          SELECT {player_select} FROM src.public.player_fantasy
          WHERE CAST(year AS INTEGER)={year} AND week IS NOT NULL
        """)
        matchup_columns = {row[0] for row in con.execute(
            "DESCRIBE src.public.matchup"
        ).fetchall()}
        missing_matchup = set(MATCHUP_COLUMNS) - matchup_columns
        if missing_matchup:
            raise RuntimeError(f"cache missing matchup rollup columns={sorted(missing_matchup)}")
        matchup_select = ", ".join(f'"{column}"' for column in MATCHUP_COLUMNS)
        con.execute(f"""
          CREATE TABLE public.matchup AS
          SELECT {matchup_select} FROM src.public.matchup
          WHERE CAST(year AS INTEGER)={year}
        """)
        source_tables = {
            row[0] for row in con.execute("""
              SELECT table_name FROM information_schema.tables
              WHERE table_catalog='src' AND table_schema='public'
                AND table_name IN ('player_active_week','player_team_game_week')
            """).fetchall()
        }
        for table in ("player_active_week", "player_team_game_week"):
            if table in source_tables:
                con.execute(f"""
                  CREATE TABLE public.{table} AS
                  SELECT * FROM src.public.{table}
                  WHERE CAST(year AS INTEGER)={year} AND week IS NOT NULL
                """)
    finally:
        con.close()


def materialize_ops_year_snapshot(source: Path, target: Path, year: int) -> None:
    """Copy only the ops columns needed by the position/active joins for one year."""
    if target.exists():
        target.unlink()
    con = duckdb.connect(str(target))
    try:
        con.execute(f"ATTACH '{source.as_posix()}' AS src (READ_ONLY)")
        columns = {row[0] for row in con.execute(
            "DESCRIBE src.nfl_historical.nfl_player_stats_all"
        ).fetchall()}
        required = {
            "NFL_player_id", "year", "week", "player_week", "nfl_team",
            "position", "nfl_position", "season_type",
        }
        missing = required - columns
        if missing:
            raise RuntimeError(f"ops cache missing position fanout columns={sorted(missing)}")
        con.execute("CREATE SCHEMA nfl_historical")
        con.execute(f"""
          CREATE TABLE nfl_historical.nfl_player_stats_all AS
          SELECT "NFL_player_id", "year", "week", "player_week", "nfl_team",
                 "position", "nfl_position", "season_type"
          FROM src.nfl_historical.nfl_player_stats_all
          WHERE CAST(year AS INTEGER)={year}
            AND week IS NOT NULL
        """)
    finally:
        con.close()


def merge(parts: list[Path], output: Path) -> None:
    if output.exists():
        output.unlink()
    con = duckdb.connect(str(output))
    try:
        for index, part in enumerate(parts):
            alias = f"part_{index}"
            con.execute(f"ATTACH '{part.as_posix()}' AS {alias} (READ_ONLY)")
            for table in TABLES:
                if index == 0:
                    con.execute(
                        f'CREATE TABLE "{table}" AS SELECT * FROM {alias}.main."{table}"'
                    )
                else:
                    con.execute(
                        f'INSERT INTO "{table}" SELECT * FROM {alias}.main."{table}"'
                    )
            con.execute(f"DETACH {alias}")

        con.execute("""
          CREATE TABLE research_matchup_adaptive_career AS
          SELECT q_teams,q_roster,q_ppr,q_td,q_bracket,q_league_type,q_lineup_mode,NFL_player_id,
            SUM(total_points_observed) AS total_points_observed,
            SUM(rostered_leagues)*100.0/NULLIF(SUM(n_leagues),0) AS roster_rate_pct,
            SUM(expected_starts)*100.0/NULLIF(SUM(active_weeks),0) AS start_rate_pct,
            SUM(healthy_expected_starts)*100.0/NULLIF(SUM(active_weeks),0) AS healthy_start_rate_pct,
            SUM(expected_wins)/NULLIF(SUM(expected_starts),0)*100.0 AS win_rate_pct,
            SUM(expected_wins)::DOUBLE AS expected_wins,
            SUM(expected_losses)::DOUBLE AS expected_losses,
            SUM(expected_starts)::DOUBLE AS expected_starts,
            SUM(total_points_observed)/NULLIF(SUM(started_leagues),0) AS ppg_when_started,
            SUM(clutch_sum) AS avg_clutch_started,
            AVG(champ_as_starter_pct) AS champ_as_starter_pct,
            AVG(playoff_as_starter_pct) AS playoff_as_starter_pct,
            SUM(1.0*champ_started/NULLIF(champ_eligible_leagues,0)) AS expected_champs,
            SUM(1.0*playoff_started/NULLIF(playoff_eligible_leagues,0)) AS expected_playoffs,
            SUM(active_weeks)::BIGINT AS active_weeks,
            SUM(inactive_weeks)::BIGINT AS inactive_weeks,
            SUM(started_weeks)::BIGINT AS started_weeks,
            COUNT(DISTINCT year)::BIGINT AS n_years,
            NULL::DOUBLE AS avg_lamar_started,
            MAX(cohort_level) AS cohort_level,
            MAX(n_leagues)::BIGINT AS n_leagues
          FROM research_matchup_adaptive
          GROUP BY ALL
        """)
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", type=Path, required=True)
    parser.add_argument("--ops-cache", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--year-start", type=int, default=1997)
    parser.add_argument("--year-end", type=int, default=2025)
    parser.add_argument("--position-group", choices=["QB", "RB", "WR", "TE", "K", "DEF", "IDP"])
    parser.add_argument("--player-id")
    parser.add_argument("--week", type=int)
    parser.add_argument("--player-buckets", type=int)
    parser.add_argument("--parallelism", type=int)
    args = parser.parse_args()

    parts_dir = args.output.parent / "_research_year_parts"
    if parts_dir.exists():
        shutil.rmtree(parts_dir)
    parts_dir.mkdir(parents=True, exist_ok=True)
    parts: list[Path] = []
    builder = Path(__file__).with_name("build_direct_matchup_cache.py")
    try:
        # Smaller historical years fit in one window.  The modern population
        # is large enough that one all-position window can exceed runner
        # memory, so only those years use position-bounded windows.
        for year in range(args.year_start, args.year_end + 1):
            year_snapshot = parts_dir / f"snapshot_{year}.duckdb"
            ops_snapshot = parts_dir / f"ops_{year}.duckdb"
            started = time.monotonic()
            print(f"materializing year snapshot year={year}", flush=True)
            materialize_year_snapshot(args.snapshot, year_snapshot, year)
            materialize_ops_year_snapshot(args.ops_cache, ops_snapshot, year)
            print(f"completed year snapshot year={year} seconds={time.monotonic() - started:.1f}", flush=True)
            # Position pruning happens before the expensive player joins.
            # Keep separate position windows for memory locality; large groups
            # use transient player buckets below.
            groups = (args.position_group,) if args.position_group else (
                POSITION_GROUPS if year >= POSITION_SPLIT_START else (None,)
            )
            try:
                tasks = []
                for position_group in groups:
                    bucket_count = args.player_buckets or (
                        8 if position_group == "RB" and not args.player_id
                        else 4 if position_group in ("IDP", "WR") and not args.player_id
                        else 1
                    )
                    # IDP buckets are an in-process memory bound, not
                    # independent work units.  Keep one task so the direct
                    # builder can reuse the shared denominator graph.
                    in_process_buckets = (
                        bucket_count > 1 and not args.player_id and position_group != "RB"
                    )
                    task_buckets = (0,) if in_process_buckets else range(bucket_count)
                    for bucket in task_buckets:
                        suffix = position_group or "all"
                        bucket_suffix = f"_bucket{bucket}" if bucket_count > 1 and not in_process_buckets else ""
                        part = parts_dir / f"part_{year}_{suffix}{bucket_suffix}.duckdb"
                        label = f"year={year} position={position_group or 'ALL'}"
                        if bucket_count > 1:
                            label += f" buckets={bucket_count}" if not args.player_id else f" bucket={bucket + 1}/{bucket_count}"
                        tasks.append((position_group, bucket, bucket_count, part, label))

                def run_task(task):
                    position_group, bucket, bucket_count, part, label = task
                    started = time.monotonic()
                    print(f"building {label}", flush=True)
                    command = [
                        sys.executable, str(builder),
                        "--snapshot", str(year_snapshot),
                        "--ops-cache", str(ops_snapshot),
                        "--output", str(part),
                        "--year-start", str(year),
                        "--year-end", str(year),
                    ]
                    if position_group:
                        command.extend(["--position-group", position_group])
                    if args.player_id:
                        command.extend(["--player-id", args.player_id])
                    if args.week is not None:
                        command.extend(["--week", str(args.week)])
                    if bucket_count > 1:
                        command.extend(["--player-buckets", str(bucket_count)])
                        if args.player_id or position_group == "RB":
                            command.extend(["--player-bucket", str(bucket)])
                        else:
                            command.append("--reuse-player-buckets")
                    # Explicit parallelism is an opt-in benchmark knob.  The default\n                    # is intentionally serial: each child reuses the same snapshot but\n                    # still materializes its own DuckDB hash state, so concurrent children\n                    # duplicate the expensive population/denominator work and can be killed\n                    # by the hosted runner.\n                    parallelism = args.parallelism or 1
                    if parallelism > 1:
                        command.extend(["--threads", "2", "--memory-limit-mb", "6000"])
                    subprocess.run(command, check=True)
                    return part, label, time.monotonic() - started

                parallelism = args.parallelism or 1
                with ThreadPoolExecutor(max_workers=parallelism) as executor:
                    futures = [executor.submit(run_task, task) for task in tasks]
                    results = [future.result() for future in as_completed(futures)]
                for part, label, elapsed in sorted(results, key=lambda item: str(item[0])):
                    parts.append(part)
                    print(f"completed {label} seconds={elapsed:.1f}", flush=True)
            finally:
                year_snapshot.unlink(missing_ok=True)
                ops_snapshot.unlink(missing_ok=True)
        merge(parts, args.output)
    finally:
        shutil.rmtree(parts_dir, ignore_errors=True)


if __name__ == "__main__":
    main()
