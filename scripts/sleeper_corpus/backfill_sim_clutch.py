"""backfill_sim_clutch.py -- give already-crawled leagues their playoff odds + clutch.

No re-ingest: the raw Sleeper data is already in each smpl_*/smpl_*.duckdb. Both engines take
--db/--data-dir and read/write that db in place, so we just re-run them:

    1. playoff_odds_import.py       -> p_champ on the matchup table
    2. aggregate_fantasy_context.py -> clutch_equity on player_fantasy (REQUIRES p_champ)

Then the league is EVICTED from the corpus snapshot and re-folded, because the _sources ledger
would otherwise skip it and the new columns would never reach the lake.

Why this exists: the grind ran best_ball with skip_sim=True on the rationale "no H2H". That is
false -- 97.0% of best-ball player rows carry a `win` and there are 18,409 champion rows. So
every best-ball league we crawled is missing clutch for no good reason. batch_ingest_corpus.py
now sims best_ball during the crawl; this backfills the ones already on disk.

    py -3 scripts/sleeper_corpus/backfill_sim_clutch.py --limit 3            # time it first
    py -3 scripts/sleeper_corpus/backfill_sim_clutch.py --workers 6
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_corpus_snapshot import OUT, evict_league, fold_league, open_snapshot
# reuse the grind's env setup: it exports OPS_CACHE_PATH (the clutch step attaches ___ops from
# it) and PYTHONPATH. Duplicating it here is how the first run failed with "Catalog ___ops
# does not exist".
from batch_ingest_corpus import load_env

ROOT = Path(os.environ.get("YAHOO_OAUTH_ROOT", "d:/yahoo_oauth"))
CORPUS = Path(os.environ.get("CORPUS_DIR", "D:/league-history-data/fantasy_leagues/sampling_corpus"))
SMPL_DIR = CORPUS / "leagues"
FFDS = ROOT / "fantasy_football_data_scripts"
SIM_PY = FFDS / "multi_league" / "transformations" / "matchup" / "playoff_odds_import.py"
AGG_PY = FFDS / "multi_league" / "transformations" / "aggregation" / "aggregate_fantasy_context.py"
CLUTCH_PY = FFDS / "multi_league" / "transformations" / "player" / "clutch_to_player.py"

_lock = threading.Lock()


def needs_clutch(
    snap_path: Path,
    platforms: list[str] | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
    only_with_source: bool = False,
) -> list[str]:
    """db_names folded into the snapshot that have no clutch_equity at all."""
    con = duckdb.connect(str(snap_path), read_only=True)
    try:
        where: list[str] = []
        having = ["COUNT(p.clutch_equity) = 0"]
        params: list[object] = []
        join = ""
        if platforms or start_year is not None or end_year is not None:
            join = " JOIN public.league_settings s USING (db_name, year)"
        if platforms:
            where.append("s.platform IN (" + ",".join("?" for _ in platforms) + ")")
            params.extend(platforms)
        if start_year is not None:
            where.append("s.year >= ?"); params.append(start_year)
        if end_year is not None:
            where.append("s.year <= ?"); params.append(end_year)
        rows = con.execute(
            f"SELECT p.db_name FROM public.player_fantasy p{join} "
            f"{'WHERE ' + ' AND '.join(where) if where else ''} GROUP BY 1 "
            f"HAVING {' AND '.join(having)} ORDER BY 1", params
        ).fetchall()
        result = [r[0] for r in rows]
        if only_with_source:
            result = [db for db in result if (SMPL_DIR / db / f"{db}.duckdb").is_file()]
        return result
    finally:
        con.close()


def run_engines(db: str, env: dict, n_sims: int | None) -> tuple[bool, str]:
    ddir = SMPL_DIR / db
    if not (ddir / f"{db}.duckdb").exists():
        return False, "source db missing (pruned?)"
    sim_cmd = [sys.executable, str(SIM_PY), "--db", db, "--data-dir", str(ddir)]
    if n_sims:
        sim_cmd += ["--n-sims", str(n_sims)]
    p = subprocess.run(sim_cmd, env=env, capture_output=True, text=True, timeout=3000)
    if p.returncode != 0:
        return False, "sim: " + (p.stderr or p.stdout)[-160:].replace("\n", " ")
    # clutch REQUIRES p_champ from the sim above -- order matters
    p = subprocess.run([sys.executable, str(AGG_PY), "--db", db, "--data-dir", str(ddir)],
                       env=env, capture_output=True, text=True, timeout=3000)
    if p.returncode != 0:
        return False, "clutch: " + (p.stderr or p.stdout)[-160:].replace("\n", " ")
    p = subprocess.run([sys.executable, str(CLUTCH_PY), "--db", db, "--data-dir", str(ddir)],
                       env=env, capture_output=True, text=True, timeout=3000)
    if p.returncode != 0:
        return False, "clutch_to_player: " + (p.stderr or p.stdout)[-160:].replace("\n", " ")
    db_file = ddir / f"{db}.duckdb"
    verify = duckdb.connect(str(db_file), read_only=True)
    try:
        columns = {row[0] for row in verify.execute("DESCRIBE public.player_fantasy").fetchall()}
        non_null = verify.execute(
            "SELECT COUNT(*) FROM public.player_fantasy WHERE clutch_equity IS NOT NULL"
        ).fetchone()[0] if "clutch_equity" in columns else 0
    finally:
        verify.close()
    if not non_null:
        return False, "clutch_to_player produced no non-null clutch_equity (missing LAMAR/input evidence)"
    return True, ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None, help="only N leagues (use a small N to time it)")
    ap.add_argument("--workers", type=int, default=3)
    ap.add_argument("--n-sims", type=int, default=None, help="override Monte Carlo count (cost dial)")
    ap.add_argument("--snapshot", default=str(OUT))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--platform", action="append", choices=("mfl", "fleaflicker", "sleeper"))
    ap.add_argument("--start-year", type=int)
    ap.add_argument("--end-year", type=int)
    ap.add_argument("--only-raw", action="store_true", help="skip folded leagues whose raw DuckDB was pruned")
    args = ap.parse_args()

    todo = needs_clutch(Path(args.snapshot), args.platform, args.start_year, args.end_year, args.only_raw)
    if args.limit:
        todo = todo[: args.limit]
    print(f"[backfill] {len(todo):,} folded leagues have no clutch", flush=True)
    if args.dry_run or not todo:
        for d in todo[:10]:
            print("   ", d)
        return

    env = load_env(args.workers, require_draft_source=False)   # clutch-only run does not import drafts
    snap = open_snapshot(Path(args.snapshot))
    counts = {"ok": 0, "fail": 0}
    t0 = time.time()

    def run(db: str) -> None:
        t = time.time()
        ok, msg = run_engines(db, env, args.n_sims)
        with _lock:  # DuckDB single-writer: evict+refold serialize behind the parallel engines
            if ok:
                evict_league(snap, db)
                good, fmsg = fold_league(snap, SMPL_DIR / db)
                if not good:
                    ok, msg = False, f"refold: {fmsg}"
            counts["ok" if ok else "fail"] += 1
            n = counts["ok"] + counts["fail"]
            print(f"  [{n}/{len(todo)}] {db} -> {'OK' if ok else 'FAIL'} ({time.time()-t:.0f}s)"
                  f"{' | ' + msg if msg else ''}", flush=True)

    if args.workers <= 1:
        for db in todo:
            run(db)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(as_completed(ex.submit(run, db) for db in todo))

    snap.close()
    el = time.time() - t0
    rate = counts["ok"] / (el / 3600) if el else 0
    print(f"\n[backfill] {counts['ok']} ok, {counts['fail']} failed in {el/60:.1f} min "
          f"({rate:.0f} leagues/hr at {args.workers} workers)", flush=True)
    if counts["ok"] and rate:
        print(f"[backfill] projected for all {len(needs_clutch(Path(args.snapshot), args.platform, args.start_year, args.end_year, args.only_raw)):,} remaining: "
              f"{len(needs_clutch(Path(args.snapshot)))/rate:.1f} hours", flush=True)


if __name__ == "__main__":
    main()
