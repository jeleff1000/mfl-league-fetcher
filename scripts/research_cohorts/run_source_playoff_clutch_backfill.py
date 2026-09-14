"""Rerun playoff odds and clutch on every local research source database.

This is deliberately source-oriented: it does not decide population membership
from the folded snapshot.  Each source DuckDB is rerun, then checked at
league-year grain before it is eligible for refolding.
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[2]
CORPUS = Path(os.environ.get("RESEARCH_CORPUS_DIR", "D:/league-history-data/fantasy_leagues/sampling_corpus"))
SOURCE_DIR = CORPUS / "leagues"
SIM = ROOT / "fantasy_football_data_scripts/multi_league/transformations/matchup/playoff_odds_import.py"
CLUTCH = ROOT / "fantasy_football_data_scripts/multi_league/transformations/player/clutch_to_player.py"


def platform_for(db_name: str) -> str:
    if db_name.startswith("smpl_mfl_"):
        return "mfl"
    if db_name.startswith("smpl_ffl_"):
        return "fleaflicker"
    return "sleeper"


def source_readiness(db_path: Path) -> dict:
    """Classify whether a source can support the matchup-derived passes.

    Some archived league databases contain only the full-NFL ``Unrostered``
    expansion.  They have a player table but no team-week outcome population;
    running the matchup engines on those records can never produce playoff or
    clutch facts.  Detect that condition before invoking either engine.
    """
    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {
            r[0]
            for r in con.execute(
                "select table_name from duckdb_tables() where schema_name='public'"
            ).fetchall()
        }
        if "player_fantasy" not in tables:
            return {
                "status": "skipped_no_matchup_evidence",
                "reason": "player_fantasy table is absent; no source outcome population",
                "matchup_rows": None,
                "outcome_rows": 0,
            }
        player_cols = {r[0] for r in con.execute("describe public.player_fantasy").fetchall()}
        if "matchup" in tables:
            matchup_rows = con.execute("select count(*) from public.matchup").fetchone()[0]
            if matchup_rows:
                return {"status": "ready", "matchup_rows": matchup_rows}
        else:
            matchup_rows = None

        # A fallback is only safe when there are actual rostered team-week
        # outcomes.  Franchise/team labels alone are insufficient: MFL has
        # some such rows but no scores or opponents to establish a result.
        required = {"manager", "team_points", "opponent_points"}
        if required <= player_cols:
            evidence = con.execute(
                """
                select count(*)
                from public.player_fantasy
                where manager is not null
                  and lower(trim(cast(manager as varchar))) not in ('', 'unrostered')
                  and team_points is not null
                  and opponent_points is not null
                """
            ).fetchone()[0]
        else:
            evidence = 0
        if evidence:
            return {
                "status": "source_missing_matchup",
                "reason": "team-week outcome evidence exists but matchup is absent/empty",
                "matchup_rows": matchup_rows,
                "outcome_rows": evidence,
            }
        return {
            "status": "skipped_no_matchup_evidence",
            "reason": "no matchup rows and no rostered team-week scores/opponents",
            "matchup_rows": matchup_rows,
            "outcome_rows": 0,
        }
    finally:
        con.close()


def source_paths(
    platforms: set[str] | None,
    selected: set[str] | None = None,
    manifest: Path | None = None,
) -> list[tuple[str, Path]]:
    allowed = selected
    if manifest is not None:
        payload = json.loads(manifest.read_text(encoding="utf-8"))
        names = payload.get("db_names", payload if isinstance(payload, list) else [])
        allowed = set(names)
    rows = []
    for directory in sorted(SOURCE_DIR.iterdir()):
        if not directory.is_dir():
            continue
        db = directory.name
        if allowed and db not in allowed:
            continue
        db_path = directory / f"{db}.duckdb"
        if not db_path.is_file():
            continue
        platform = platform_for(db)
        if platforms and platform not in platforms:
            continue
        rows.append((db, db_path))
    return rows


def run_one(db: str, db_path: Path, n_sims: int, log_dir: Path) -> dict:
    started = time.time()
    log_dir.mkdir(parents=True, exist_ok=True)
    task_log = log_dir / f"{db}.log"
    ddir = str(db_path.parent)
    readiness = source_readiness(db_path)
    if readiness["status"] != "ready":
        return {
            "db_name": db,
            "platform": platform_for(db),
            **readiness,
            "log": str(task_log),
            "seconds": round(time.time() - started, 1),
        }
    env = os.environ.copy()
    env.setdefault("PYTHONPATH", str(ROOT))
    commands = [
        [sys.executable, str(SIM), "--db", db, "--data-dir", ddir, "--n-sims", str(n_sims)],
        [sys.executable, str(CLUTCH), "--db", db, "--data-dir", ddir],
    ]
    with task_log.open("w", encoding="utf-8") as fh:
        for command in commands:
            result = subprocess.run(
                command,
                cwd=str(ROOT),
                env=env,
                stdout=fh,
                stderr=subprocess.STDOUT,
                text=True,
                timeout=3600,
            )
            if result.returncode:
                return {
                    "db_name": db,
                    "platform": platform_for(db),
                    "status": "engine_failed",
                    "returncode": result.returncode,
                    "log": str(task_log),
                    "seconds": round(time.time() - started, 1),
                }

    con = duckdb.connect(str(db_path), read_only=True)
    try:
        tables = {r[0] for r in con.execute("select table_name from duckdb_tables() where schema_name='public'").fetchall()}
        if "matchup" not in tables or "player_fantasy" not in tables:
            return {
                "db_name": db,
                "platform": platform_for(db),
                "status": "skipped_no_matchup_evidence",
                "reason": "backfill engine produced no source outcome population",
            }
        matchup_cols = {r[0] for r in con.execute("describe public.matchup").fetchall()}
        player_cols = {r[0] for r in con.execute("describe public.player_fantasy").fetchall()}
        if "p_champ" not in matchup_cols or "clutch_equity" not in player_cols:
            return {"db_name": db, "platform": platform_for(db), "status": "missing_output_column"}
        missing_matchup = con.execute(
            "select count(*) from public.matchup where p_champ is null"
        ).fetchone()[0]
        if "is_rostered" in player_cols:
            missing_clutch = con.execute(
                "select count(*) from public.player_fantasy "
                "where cast(is_rostered as integer)=1 and clutch_equity is null"
            ).fetchone()[0]
        else:
            missing_clutch = con.execute(
                "select count(*) from public.player_fantasy "
                "where manager is not null and trim(cast(manager as varchar)) not in ('', 'Unrostered') "
                "and clutch_equity is null"
            ).fetchone()[0]
        years = con.execute("select count(distinct year) from public.matchup").fetchone()[0]
        status = "done" if not missing_matchup and not missing_clutch else "incomplete"
        return {
            "db_name": db,
            "platform": platform_for(db),
            "status": status,
            "years": years,
            "missing_matchup_p_champ": missing_matchup,
            "missing_rostered_clutch": missing_clutch,
            "log": str(task_log),
            "seconds": round(time.time() - started, 1),
        }
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workers", type=int, default=6)
    parser.add_argument("--n-sims", type=int, default=1000)
    parser.add_argument("--platform", action="append", choices=("mfl", "fleaflicker", "sleeper"))
    parser.add_argument("--limit", type=int)
    parser.add_argument("--db", action="append", dest="databases")
    parser.add_argument("--manifest", type=Path, help="JSON shard manifest containing db_names")
    parser.add_argument("--results", type=Path, help="Result JSONL path (default: corpus path)")
    parser.add_argument("--log-dir", default=str(CORPUS / "source_playoff_clutch_logs"))
    args = parser.parse_args()
    targets = source_paths(
        set(args.platform) if args.platform else None,
        set(args.databases) if args.databases else None,
        args.manifest,
    )
    if args.limit:
        targets = targets[: args.limit]
    print(f"targets={len(targets)} workers={args.workers} n_sims={args.n_sims}", flush=True)
    results = []
    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(run_one, db, path, args.n_sims, Path(args.log_dir)) for db, path in targets]
        for i, future in enumerate(as_completed(futures), 1):
            result = future.result()
            results.append(result)
            print(f"[{i}/{len(futures)}] {result['db_name']} {result['status']}", flush=True)
    out = args.results or (CORPUS / "source_playoff_clutch_backfill_results.jsonl")
    with out.open("w", encoding="utf-8") as fh:
        for result in sorted(results, key=lambda x: x["db_name"]):
            fh.write(json.dumps(result, sort_keys=True) + "\n")
    counts = {}
    for result in results:
        counts[result["status"]] = counts.get(result["status"], 0) + 1
    print(json.dumps({"results": str(out), "counts": counts}, sort_keys=True), flush=True)


if __name__ == "__main__":
    main()
