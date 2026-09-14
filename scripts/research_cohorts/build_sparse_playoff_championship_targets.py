"""Build source targets for league-years with sparse playoff/champ coverage.

The unit is one populated league-year.  The output is platform-filterable and
contains only source IDs/weeks needed by the playoff/champ rescue; it never
creates a cache or writes the canonical lake.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def cols(con: duckdb.DuckDBPyConnection, relation: str) -> set[str]:
    return {r[0] for r in con.execute(f"DESCRIBE {relation}").fetchall()}


def build(root: Path, out: Path, platform: str, playoff_threshold: int, champ_threshold: int) -> dict:
    snapshot = root / "corpus_snapshot.duckdb"
    if not snapshot.is_file() or snapshot.stat().st_size == 0:
        raise SystemExit(f"missing canonical snapshot: {snapshot}")
    con = duckdb.connect(str(snapshot), read_only=True)
    try:
        pc = cols(con, "public.player_fantasy")
        sc = cols(con, "public.league_settings")
        required = {"db_name", "year", "NFL_player_id", "is_playoffs", "champion"}
        if not required <= pc or not {"db_name", "year", "platform", "league_key"} <= sc:
            raise SystemExit("canonical schema lacks sparse target columns")
        source_id = "CAST(s.league_key AS VARCHAR)"
        start = "CAST(COALESCE(s.playoff_start_week, CASE WHEN CAST(s.year AS INTEGER)<2021 THEN 15 ELSE 18 END) AS INTEGER)" if "playoff_start_week" in sc else "CASE WHEN CAST(s.year AS INTEGER)<2021 THEN 15 ELSE 18 END"
        rows = con.execute(f"""
          WITH counts AS (
            SELECT CAST(p.db_name AS VARCHAR) AS db_name, CAST(p.year AS INTEGER) AS year,
                   LOWER(TRIM(CAST(s.platform AS VARCHAR))) AS platform,
                   {source_id} AS source_id, {start} AS playoff_start_week,
                   MAX(CAST(p.week AS INTEGER)) AS max_player_week,
                   COUNT(DISTINCT CASE WHEN CAST(p.is_playoffs AS INTEGER)=1 THEN CAST(p.NFL_player_id AS VARCHAR) END) AS playoff_players,
                   COUNT(DISTINCT CASE WHEN CAST(p.champion AS INTEGER)=1 THEN CAST(p.NFL_player_id AS VARCHAR) END) AS champion_marked_players,
                   COUNT(*) FILTER (WHERE p.NFL_player_id IS NULL) AS unmapped_player_rows
            FROM public.player_fantasy p
            JOIN public.league_settings s ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=CAST(p.year AS INTEGER)
            GROUP BY 1,2,3,4,5
          )
          SELECT * FROM counts
          WHERE (playoff_players < {int(playoff_threshold)} OR champion_marked_players < {int(champ_threshold)})
            AND platform = ?
          ORDER BY year, db_name
        """, [platform]).fetchall()
        names = [d[0] for d in con.description]
        targets, missing_ids = [], []
        for row in rows:
            x = dict(zip(names, row))
            year = int(x["year"])
            start_week = int(x["playoff_start_week"] or (15 if year < 2021 else 18))
            max_week = int(x["max_player_week"] or start_week)
            reasons = []
            if int(x["playoff_players"] or 0) < playoff_threshold:
                reasons.append("playoff_players_below_threshold")
            if int(x["champion_marked_players"] or 0) < champ_threshold:
                reasons.append("champion_marked_players_below_threshold")
            if not str(x["source_id"] or "").strip():
                missing_ids.append({**x, "reason_codes": reasons + ["missing_source_id"]})
                continue
            targets.append({
                "db_name": x["db_name"], "year": year, "platform": platform,
                "source_id": str(x["source_id"]),
                "weeks": list(range(start_week, max(max_week, start_week) + 1)),
                "reason_codes": reasons,
                "sparsity": {"playoff_players": int(x["playoff_players"] or 0), "champion_marked_players": int(x["champion_marked_players"] or 0), "unmapped_player_rows": int(x["unmapped_player_rows"] or 0)},
            })
        result = {"schema_version": 1, "platform": platform, "thresholds": {"playoff_players_lt": playoff_threshold, "champion_marked_players_lt": champ_threshold}, "candidate_count": len(rows), "target_count": len(targets), "missing_source_id_count": len(missing_ids), "targets": targets, "missing_source_ids": missing_ids}
        out.mkdir(parents=True, exist_ok=True)
        (out / "sparse_playoff_championship_targets.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        (out / "sparse_playoff_championship_summary.json").write_text(json.dumps({k: result[k] for k in ("schema_version", "platform", "thresholds", "candidate_count", "target_count", "missing_source_id_count")}, indent=2) + "\n", encoding="utf-8")
        print(json.dumps({k: result[k] for k in ("platform", "candidate_count", "target_count", "missing_source_id_count")}, sort_keys=True))
        return result
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--platform", choices=("fleaflicker", "mfl", "sleeper"), required=True)
    ap.add_argument("--playoff-threshold", type=int, default=10)
    ap.add_argument("--champ-threshold", type=int, default=5)
    args = ap.parse_args()
    build(args.root, args.out, args.platform, args.playoff_threshold, args.champ_threshold)


if __name__ == "__main__":
    main()
