"""Turn missing player-week outcome targets into source league-season targets."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def build_manifest(snapshot: Path, inputs: list[Path]) -> dict:
    con = duckdb.connect()
    con.execute(f"attach '{snapshot.resolve().as_posix()}' as lake (read_only)")
    con.execute("create temp table targets as select * from read_parquet(?)", [[str(p) for p in inputs]])
    target_cols = {r[0] for r in con.execute("describe targets").fetchall()}
    required = {"db_name", "year", "platform"}
    if not required <= target_cols:
        raise ValueError(f"outcome targets missing {sorted(required - target_cols)}")
    rows = con.execute(
        """
        with counts as (
          select cast(db_name as varchar) db_name, cast(year as integer) season_year,
                 max(cast(platform as varchar)) platform, count(*) target_rows,
                 list_sort(list_distinct(list(cast(week as integer)))) target_weeks
          from targets group by 1,2
        ), settings as (
          select cast(db_name as varchar) db_name, cast(year as integer) season_year,
                 max(cast(platform as varchar)) settings_platform,
                 max(cast(league_key as varchar)) source_id
          from lake.public.league_settings group by 1,2
        )
        select c.db_name, c.season_year, coalesce(c.platform, s.settings_platform) platform,
               s.source_id, c.target_rows
        from counts c left join settings s using (db_name, season_year)
        order by c.db_name, c.season_year
        """
    ).fetchall()
    targets = [
        {
            "db_name": db,
            "year": year,
            "platform": str(platform or "").lower(),
            "source_id": source_id,
            "target_rows": target_rows,
            "weeks": target_weeks or [],
            "reason_codes": ["missing_player_outcome"],
        }
        for db, year, platform, source_id, target_rows, target_weeks in rows
    ]
    return {
        "schema_version": 1,
        "target_kind": "league-season-source-matchup",
        "target_rows": sum(r["target_rows"] for r in targets),
        "targets": targets,
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--input", type=Path, action="append", required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    manifest = build_manifest(args.snapshot, args.input)
    if not manifest["targets"]:
        raise SystemExit("empty outcome source target manifest")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print({"league_year_targets": len(manifest["targets"]), "player_week_targets": manifest["target_rows"]})


if __name__ == "__main__":
    main()
