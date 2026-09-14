"""Build exact Sleeper league-season/week targets for missing started outcomes."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def qi(s: str) -> str:
    return '"' + s.replace('"', '""') + '"'


def cols(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {relation}").fetchall()}


def build(snapshot: Path, out: Path) -> dict:
    con = duckdb.connect(str(snapshot), read_only=True)
    pf = cols(con, "public.player_fantasy")
    ls = cols(con, "public.league_settings")
    required = {"db_name", "year", "week", "is_started"} - pf
    if required:
        raise SystemExit(f"player_fantasy missing required columns: {sorted(required)}")
    if not {"db_name", "year", "platform"} <= ls:
        raise SystemExit("league_settings lacks db_name/year/platform")
    outcome = [c for c in ("win", "loss", "tie") if c in pf]
    if {"team_points", "opponent_points"} <= pf:
        outcome.append("team_points IS NOT NULL AND opponent_points IS NOT NULL")
    known = " OR ".join([f"{qi(c)} IS NOT NULL" for c in outcome if " " not in c] +
                         [c for c in outcome if " " in c]) or "FALSE"
    source_id = "MAX(CAST(league_key AS VARCHAR))" if "league_key" in ls else "CAST(NULL AS VARCHAR)"
    sql = f"""
      WITH sleeper AS (
        SELECT CAST(p.db_name AS VARCHAR) AS db_name,
               CAST(p.year AS INTEGER) AS year,
               CAST(p.week AS INTEGER) AS week,
               COUNT(*) AS target_rows
        FROM public.player_fantasy p
        JOIN public.league_settings s
          ON CAST(s.db_name AS VARCHAR)=CAST(p.db_name AS VARCHAR)
         AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
        WHERE LOWER(CAST(s.platform AS VARCHAR))='sleeper'
          AND CAST(p.is_started AS INTEGER)=1
          AND NOT ({known})
        GROUP BY 1,2,3
      ), grouped AS (
        SELECT db_name, year,
               list_sort(list_distinct(list(week))) AS weeks,
               SUM(target_rows)::BIGINT AS target_rows
        FROM sleeper GROUP BY 1,2
      ), settings AS (
        SELECT CAST(db_name AS VARCHAR) AS db_name,
               CAST(year AS INTEGER) AS year,
               {source_id} AS source_id
        FROM public.league_settings
        WHERE LOWER(CAST(platform AS VARCHAR))='sleeper'
        GROUP BY 1,2
      )
      SELECT g.db_name, g.year, 'sleeper' AS platform, s.source_id,
             g.weeks, g.target_rows
      FROM grouped g LEFT JOIN settings s USING (db_name, year)
      ORDER BY 1,2
    """
    rows = []
    for db_name, year, platform, source_id, weeks, target_rows in con.execute(sql).fetchall():
        rows.append({"db_name": db_name, "year": int(year), "platform": platform,
                     "source_id": source_id, "weeks": [int(w) for w in weeks],
                     "target_rows": int(target_rows),
                     "reason_codes": ["started_rows_missing_win_outcome"]})
    payload = {
        "schema_version": 1,
        "platform": "sleeper",
        "target_rows": sum(r["target_rows"] for r in rows),
        "target_groups": sum(len(r["weeks"]) for r in rows),
        "league_years": len(rows),
        "targets": rows,
    }
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: payload[k] for k in ("target_rows", "target_groups", "league_years")}, indent=2))
    con.close()
    return payload


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    build(args.snapshot, args.out)
