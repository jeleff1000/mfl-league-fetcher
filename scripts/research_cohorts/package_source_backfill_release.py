"""Package local research source DuckDBs into balanced private-release assets.

The resulting numbered tar parts are consumed by the 15-way GitHub Actions
backfill workflow.  The source files stay out of git and are never copied into
the repository working tree.
"""
from __future__ import annotations

import argparse
import json
import shutil
import tarfile
from pathlib import Path

import duckdb


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source-dir", type=Path, required=True)
    p.add_argument("--output-dir", type=Path, required=True)
    p.add_argument("--buckets", type=int, default=15)
    p.add_argument("--bucket", type=int, action="append", dest="only_buckets")
    p.add_argument("--results-inventory", type=Path, help="JSONL result inventory to restrict the source population")
    p.add_argument("--status", action="append", dest="statuses", help="Allowed status when --results-inventory is used")
    p.add_argument(
        "--target-inventory", type=Path,
        help="JSON league-year inventory/target list; package only DBs appearing in its db_name field",
    )
    args = p.parse_args()
    if args.buckets < 1:
        raise SystemExit("--buckets must be positive")
    allowed = None
    if args.results_inventory:
        rows = [json.loads(line) for line in args.results_inventory.read_text(encoding="utf-8").splitlines() if line.strip()]
        statuses = set(args.statuses or [])
        allowed = {row["db_name"] for row in rows if not statuses or row.get("status") in statuses}
    if args.target_inventory:
        payload = json.loads(args.target_inventory.read_text(encoding="utf-8"))
        rows = payload.get("targets", []) if isinstance(payload, dict) else payload if isinstance(payload, list) else []
        target_names = {str(row["db_name"]) for row in rows if isinstance(row, dict) and row.get("db_name")}
        if not target_names:
            raise SystemExit("--target-inventory contains no db_name targets")
        allowed = target_names if allowed is None else allowed & target_names
    dirs = []
    for d in sorted(args.source_dir.iterdir()):
        db = d / f"{d.name}.duckdb"
        if d.is_dir() and db.is_file() and (allowed is None or d.name in allowed):
            dirs.append((d, db.stat().st_size))
    if not dirs:
        raise SystemExit(f"no source DuckDBs found under {args.source_dir}")
    buckets = [{"bytes": 0, "db_names": [], "dirs": []} for _ in range(args.buckets)]
    for d, size in sorted(dirs, key=lambda x: (-x[1], x[0].name)):
        target = min(range(args.buckets), key=lambda i: buckets[i]["bytes"])
        buckets[target]["bytes"] += size
        buckets[target]["db_names"].append(d.name)
        buckets[target]["dirs"].append(d)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    staging = args.output_dir / ".staging"
    if staging.exists():
        shutil.rmtree(staging)
    # These are the only tables read or written by the playoff/clutch passes.
    # Omitting draft/transactions/NFL expansion tables makes the transfer bundle
    # materially smaller without changing the calculation inputs.
    needed_tables = ("matchup", "schedule", "league_settings", "league_context", "player_fantasy")
    needed_columns = {
        "matchup": (
            "year", "week", "manager", "team_points", "opponent", "opponent_points", "win", "loss", "tie",
            "franchise_id", "opponent_franchise_id", "team_name", "is_playoffs", "is_consolation", "is_bye_week",
            "wins_to_date", "losses_to_date", "ties_to_date", "playoff_seed", "final_playoff_seed", "points_to_date",
            "inflation_rate", "above_league_median", "num_playoff_teams", "num_bye_teams", "playoff_round",
            "consolation_round", "season_result", "matchup_id",
        ),
        "schedule": (
            "year", "week", "manager", "opponent", "franchise_id", "opponent_franchise_id",
            "is_playoffs", "is_consolation",
        ),
        "player_fantasy": (
            "year", "week", "manager", "position", "fantasy_position", "manager_lamar", "player_lamar",
            "franchise_id", "player", "player_week", "NFL_player_id", "yahoo_player_id", "sleeper_player_id",
            "espn_player_id", "fleaflicker_player_id", "mfl_player_id", "is_rostered", "is_started", "win", "loss", "tie",
            "team_points", "opponent_points", "champion", "clutch_equity", "final_playoff_seed", "made_playoffs",
        ),
    }

    def materialize(source_dir: Path, target_dir: Path) -> None:
        source = source_dir / f"{source_dir.name}.duckdb"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / source.name
        src = duckdb.connect(str(source), read_only=True)
        out = duckdb.connect(str(target))
        try:
            out.execute("PRAGMA memory_limit='512MB'")
            out.execute("PRAGMA threads=1")
            out.execute("PRAGMA preserve_insertion_order=false")
            out.execute("CREATE SCHEMA IF NOT EXISTS public")
            tables = {r[0] for r in src.execute("select table_name from duckdb_tables() where schema_name='public'").fetchall()}
            out.execute(f"ATTACH '{source.resolve().as_posix().replace(chr(39), chr(39) * 2)}' AS src (READ_ONLY)")
            for table in needed_tables:
                if table not in tables:
                    continue
                safe = table.replace('"', '""')
                if table in needed_columns:
                    available = {r[0] for r in src.execute(f'DESCRIBE public."{safe}"').fetchall()}
                    selected = [c for c in needed_columns[table] if c in available]
                    if not selected:
                        continue
                    select_sql = ", ".join('"' + c.replace('"', '""') + '"' for c in selected)
                    out.execute(f'CREATE TABLE public."{safe}" AS SELECT {select_sql} FROM src.public."{safe}"')
                else:
                    out.execute(f'CREATE TABLE public."{safe}" AS SELECT * FROM src.public."{safe}"')
        finally:
            try:
                out.execute("DETACH src")
            except Exception:
                pass
            out.close()
            src.close()
    manifest = {"schema_version": 1, "buckets": []}
    for i, bucket in enumerate(buckets):
        if args.only_buckets is not None and i not in set(args.only_buckets):
            continue
        name = f"research-source-backfill-v1.tar.part-{i:02d}"
        archive = args.output_dir / name
        shard = {"schema_version": 1, "bucket": i, "db_names": sorted(bucket["db_names"])}
        shard_path = args.output_dir / f"source_manifest_{i:02d}.json"
        shard_path.write_text(json.dumps(shard, indent=2) + "\n", encoding="utf-8")
        if staging.exists():
            shutil.rmtree(staging)
        for source_dir in bucket["dirs"]:
            materialize(source_dir, staging / "leagues" / source_dir.name)
        with tarfile.open(archive, "w") as tar:
            tar.add(staging / "leagues", arcname="leagues")
            tar.add(shard_path, arcname=shard_path.name)
        shutil.rmtree(staging)
        manifest["buckets"].append({"bucket": i, "asset": name, "db_count": len(bucket["db_names"]), "bytes": bucket["bytes"]})
    (args.output_dir / "source_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
