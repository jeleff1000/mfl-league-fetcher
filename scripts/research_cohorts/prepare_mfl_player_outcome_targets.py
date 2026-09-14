"""Materialize the exact MFL started-player rows with missing outcomes.

This reads only the frozen canonical research snapshot.  It creates an
inventory/manifest for a targeted source audit; it never writes the snapshot
or creates a lineage.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


EXPECTED_MFL_MISSING_WIN_ROWS = 1_359_202


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--targets", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--expected", type=int, default=EXPECTED_MFL_MISSING_WIN_ROWS)
    args = ap.parse_args()

    con = duckdb.connect(str(args.snapshot), read_only=True)
    player_cols = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
    setting_cols = {r[0] for r in con.execute("DESCRIBE public.league_settings").fetchall()}
    required_player = {"db_name", "year", "week", "NFL_player_id", "manager", "is_started", "win"}
    required_settings = {"db_name", "year", "platform", "league_key", "playoff_start_week"}
    missing = {
        "player_fantasy": sorted(required_player - player_cols),
        "league_settings": sorted(required_settings - setting_cols),
    }
    if any(missing.values()):
        raise SystemExit(f"canonical schema missing required columns: {missing}")

    sql = """
        SELECT
          ROW_NUMBER() OVER (ORDER BY p.db_name, p.year, p.week,
                                      p.NFL_player_id, p.manager) - 1 AS target_ordinal,
          CAST(p.db_name AS VARCHAR) AS db_name,
          CAST(p.year AS INTEGER) AS year,
          CAST(p.week AS INTEGER) AS week,
          CAST(p.NFL_player_id AS VARCHAR) AS NFL_player_id,
          CAST(p.manager AS VARCHAR) AS manager,
          CAST(s.league_key AS VARCHAR) AS source_id,
          TRY_CAST(s.playoff_start_week AS INTEGER) AS playoff_start_week
        FROM public.player_fantasy p
        JOIN public.league_settings s
          ON s.db_name = p.db_name AND CAST(s.year AS INTEGER) = CAST(p.year AS INTEGER)
        WHERE LOWER(CAST(s.platform AS VARCHAR)) = 'mfl'
          AND CAST(p.is_started AS INTEGER) = 1
          AND p.win IS NULL
        ORDER BY target_ordinal
    """
    con.execute("COPY (" + sql + ") TO ? (FORMAT PARQUET, COMPRESSION ZSTD)", [str(args.targets)])
    count = int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(args.targets)]).fetchone()[0])
    null_sources = int(con.execute("SELECT COUNT(*) FROM read_parquet(?) WHERE source_id IS NULL OR TRIM(source_id) = ''", [str(args.targets)]).fetchone()[0])
    bad_ordinals = int(con.execute("SELECT COUNT(*) - COUNT(DISTINCT target_ordinal) FROM read_parquet(?)", [str(args.targets)]).fetchone()[0])
    manifest_rows = con.execute("""
        SELECT db_name, year, week, MAX(source_id) AS source_id,
               MAX(playoff_start_week) AS playoff_start_week,
               COUNT(*) AS target_rows,
               MIN(target_ordinal) AS first_target_ordinal,
               MAX(target_ordinal) AS last_target_ordinal
        FROM read_parquet(?)
        GROUP BY 1,2,3
        ORDER BY 1,2,3
    """, [str(args.targets)]).fetchall()
    con.close()

    if count != args.expected:
        raise SystemExit(f"fail-closed target count: expected {args.expected}, got {count}")
    if null_sources or bad_ordinals:
        raise SystemExit(f"invalid target inventory: null_sources={null_sources} duplicate_ordinal_excess={bad_ordinals}")

    groups = [
        {
            "db_name": row[0], "year": int(row[1]), "week": int(row[2]),
            "source_id": row[3],
            "playoff_start_week": int(row[4]) if row[4] is not None else None,
            "target_rows": int(row[5]),
            "first_target_ordinal": int(row[6]), "last_target_ordinal": int(row[7]),
        }
        for row in manifest_rows
    ]
    args.manifest.parent.mkdir(parents=True, exist_ok=True)
    args.manifest.write_text(json.dumps({
        "schema_version": 1,
        "population": "canonical public.player_fantasy MFL started rows where win IS NULL",
        "lineage_id": "research-matchup-v2-final-playoff-anchor-outcomes",
        "target_rows": count,
        "group_count": len(groups),
        "groups": groups,
    }, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"target_rows": count, "groups": len(groups), "null_sources": null_sources, "duplicate_ordinal_excess": bad_ordinals}, sort_keys=True))


if __name__ == "__main__":
    main()
