"""Materialize deterministic missing-outcome starter-week targets.

This is the inventory lane for the outcome sidecar rescue.  It never writes to
the research lake and never changes canonical rows.  The same ordered target
set is sliced across the GitHub matrix so retries are idempotent.
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--start-row", type=int, default=0,
                    help="inclusive global target ordinal")
    ap.add_argument("--max-rows", type=int, default=90_000)
    ap.add_argument("--shard-index", type=int, required=True)
    ap.add_argument("--shard-count", type=int, default=256)
    args = ap.parse_args()
    if (args.start_row < 0 or args.max_rows <= args.start_row or args.shard_count <= 0
            or not 0 <= args.shard_index < args.shard_count):
        raise SystemExit("invalid target/shard arguments")

    con = duckdb.connect()
    con.execute(f"ATTACH '{args.snapshot.resolve().as_posix()}' AS lake (READ_ONLY)")
    cols = {r[0] for r in con.execute("DESCRIBE lake.public.player_fantasy").fetchall()}
    required = {"db_name", "year", "week", "NFL_player_id", "is_started"}
    if not required <= cols:
        raise SystemExit(f"player_fantasy missing required columns: {sorted(required - cols)}")
    optional = [c for c in ("manager", "franchise_id", "team_key", "team_name", "platform", "win", "loss", "tie",
                            "team_points", "opponent_points") if c in cols]
    select = ["db_name", "year", "week", "NFL_player_id", "is_started"] + optional
    select_sql = ", ".join(f'"{c}"' for c in dict.fromkeys(select))
    manager_order = 'COALESCE(CAST("manager" AS VARCHAR), \'\')' if "manager" in cols else "''"
    outcome = [f'"{c}" IS NOT NULL' for c in ("win", "loss", "tie") if c in cols]
    if {"team_points", "opponent_points"} <= cols:
        outcome.append('"team_points" IS NOT NULL AND "opponent_points" IS NOT NULL')
    if not outcome:
        raise SystemExit("no recognized outcome columns")
    missing = "NOT (" + " OR ".join(outcome) + ")"
    args.out.parent.mkdir(parents=True, exist_ok=True)
    con.execute(f"""
        COPY (
          WITH ranked AS (
            SELECT {select_sql},
                   ROW_NUMBER() OVER (
                     ORDER BY CAST(db_name AS VARCHAR), CAST(year AS INTEGER),
                              CAST(week AS INTEGER), CAST(NFL_player_id AS VARCHAR),
                              {manager_order}
                   ) - 1 AS target_ordinal
            FROM lake.public.player_fantasy
            WHERE CAST(is_started AS INTEGER) = 1
              AND NFL_player_id IS NOT NULL
              AND {missing}
          )
          SELECT * FROM ranked
          WHERE target_ordinal >= {args.start_row}
            AND target_ordinal < {args.max_rows}
            AND target_ordinal % {args.shard_count} = {args.shard_index}
        ) TO '{args.out.resolve().as_posix()}' (FORMAT PARQUET)
    """)
    count = con.execute(f"SELECT COUNT(*) FROM read_parquet('{args.out.resolve().as_posix()}')").fetchone()[0]
    print(f"target shard {args.shard_index}/{args.shard_count}: {count:,} rows -> {args.out}")


if __name__ == "__main__":
    main()
