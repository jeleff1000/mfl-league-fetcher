"""pilot_ingest_fleaflicker.py -- run discovered Fleaflicker leagues through the corpus recipe.

Mirrors scripts/sleeper_corpus/batch_ingest_corpus.py:ingest_one, with the Fleaflicker
importer swapped in:
  1. fleaflicker_initial_import --stop-after 3 (fetch + transforms + sql enrichments, NO upload)
  2. playoff_odds_import (clutch) -- platform-agnostic, reads the local matchup table
  3. fold_league into a PILOT snapshot (NOT the main 4,274-league lake; merge after review)

IMPORTANT: passes ONLY --discover-years (never --start-year/--end-year) -- bounding years
re-expands ctx.league_ids to a contiguous range, which re-introduces the season-fallback
trap for gap years the league never played.

    py -3 scripts/fleaflicker_corpus/pilot_ingest_fleaflicker.py --workers 2
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))
from build_corpus_snapshot import fold_league, open_snapshot  # noqa: E402

CORPUS = Path("D:/league-history-data/fantasy_leagues/sampling_corpus")
LEDGER_IN = CORPUS / "discovery" / "fleaflicker_pilot.json"
SMPL_DIR = CORPUS / "leagues"
DONE = CORPUS / "ffl_pilot_ledger.json"
PILOT_SNAPSHOT = CORPUS / "corpus_pilot_extraplatform.duckdb"
IMPORT_PY = ROOT / "fantasy_football_data_scripts" / "fleaflicker_initial_import.py"
SIM_PY = ROOT / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "matchup" / "playoff_odds_import.py"

# Observed server behavior (2026-07-17 spike): bursts of 11 req/s pass, but ~300 requests
# inside ~20 min trips a soft per-IP 403 that clears in <=60s. The client retries 403 with
# 5-45s backoff, so a total host budget around 60/min rides just under the quota.
HOST_RATE_BUDGET = 60

_lock = threading.Lock()


def _err(p: subprocess.CompletedProcess) -> str:
    out = (p.stderr or p.stdout or "").strip()
    if not out:
        return "(no stderr)"
    lines = [ln for ln in out.splitlines() if ln.strip()]
    exc = next((ln.strip() for ln in reversed(lines)
                if ln.strip() and not ln.strip().startswith(("File ", "  ", "Traceback"))), lines[-1].strip())
    return f"{exc}  ||tail|| {out[-600:]}"


def load_env(workers: int) -> dict:
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "fantasy_football_data_scripts")
    env["FLEAFLICKER_RATE_LIMIT_PER_MIN"] = str(max(10, HOST_RATE_BUDGET // max(1, workers)))
    # SQL enrichments resolve ___ops through the local cache; without this they try the
    # real catalog and every draft/player enrichment binder-errors (smoke test 2026-07-17).
    env["OPS_CACHE_PATH"] = str(CORPUS / "ops_cache.duckdb")
    # Fail-closed against ALL Fly access (assert_fly_access_allowed raises) and skip the
    # keeper-config Fly read. Corpus ingests must stay fully offline from production.
    env["CORPUS_MODE"] = "1"
    # draft_value_zscore's baseline; in corpus mode the Fly fallback is disabled and a
    # missing path hard-fails the league. Local copy lives in the GH dependency bundle.
    env["DRAFT_GLOBAL_SOURCE_PATH"] = str(CORPUS / "github_dependencies_v1" / "draft_global_source.parquet")
    return env


def ingest_one(league_id: int, env: dict) -> tuple[str, str]:
    db = f"smpl_ffl_{league_id}"
    ddir = SMPL_DIR / db
    p1 = subprocess.run(
        [sys.executable, str(IMPORT_PY), "--league-id", str(league_id),
         "--data-dir", str(ddir), "--database-name", db,
         "--skip-track-1", "--stop-after", "3", "--discover-years"],
        env=env, capture_output=True, text=True, timeout=7200)
    if p1.returncode != 0:
        return "import_failed", _err(p1)
    p2 = subprocess.run(
        [sys.executable, str(SIM_PY), "--db", db, "--data-dir", str(ddir)],
        env=env, capture_output=True, text=True, timeout=3000)
    if p2.returncode != 0:
        return "sim_failed", _err(p2)
    return "done", ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--league-id", type=int, default=None, help="ingest ONE league (smoke test)")
    args = ap.parse_args()

    if args.league_id:
        ids = [args.league_id]
    else:
        picked = json.loads(LEDGER_IN.read_text())["picked"]
        ids = [r["league_id"] for r in picked]
    done = json.loads(DONE.read_text()) if DONE.exists() else {}
    todo = [lid for lid in ids if done.get(str(lid)) != "done"]
    if args.limit:
        todo = todo[:args.limit]
    print(f"[plan] {len(todo)} leagues to ingest ({len(ids) - len(todo)} already done)", flush=True)
    if not todo:
        return

    SMPL_DIR.mkdir(parents=True, exist_ok=True)
    env = load_env(args.workers)
    print(f"[rate] {args.workers} workers x {env['FLEAFLICKER_RATE_LIMIT_PER_MIN']}/min "
          f"(host budget {HOST_RATE_BUDGET}/min)", flush=True)
    snap = open_snapshot(PILOT_SNAPSHOT)
    counts = {"done": 0, "fail": 0, "folded": 0}
    t0 = time.time()

    def run(lid: int) -> None:
        t1 = time.time()
        status, msg = ingest_one(lid, env)
        with _lock:
            done[str(lid)] = status
            DONE.write_text(json.dumps(done))
            counts["done" if status == "done" else "fail"] += 1
            if status == "done":
                ok, fmsg = fold_league(snap, SMPL_DIR / f"smpl_ffl_{lid}")
                if ok:
                    counts["folded"] += 1
                else:
                    msg = (msg + f" | fold: {fmsg}").strip(" |")
            n = counts["done"] + counts["fail"]
            detail = (" | " + msg) if msg else ""
            print(f"  [{n}/{len(todo)}] ffl {lid} -> {status} ({time.time()-t1:.0f}s){detail}", flush=True)

    if args.workers <= 1:
        for lid in todo:
            run(lid)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(as_completed(ex.submit(run, lid) for lid in todo))

    snap.close()
    print(f"\n[done] {counts['done']} ok, {counts['fail']} failed, {counts['folded']} folded "
          f"in {(time.time()-t0)/60:.0f} min -> {PILOT_SNAPSHOT}", flush=True)
    if todo and not counts["done"]:
        raise SystemExit(f"[fatal] 0 of {len(todo)} leagues ingested")


if __name__ == "__main__":
    main()
