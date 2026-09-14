"""pilot_ingest_mfl.py -- run seed MFL leagues through the corpus recipe.

Mirror of scripts/fleaflicker_corpus/pilot_ingest_fleaflicker.py with the MFL importer.
Seeds are YEAR:ID strings (MFL league ids are per-season namespaces; the importer walks
the league's own history links to build the {year: id} map).

MFL throttle is aggressive (429 after ~70-90 fast requests, no Retry-After, escalating
penalty) -- ONE worker, 25/min, non-negotiable until we register an API client.

    py -3 scripts/mfl_corpus/pilot_ingest_mfl.py --seeds 2024:39859,2024:63886
"""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts" / "sleeper_corpus"))
from build_corpus_snapshot import fold_league, open_snapshot  # noqa: E402

CORPUS = Path("D:/league-history-data/fantasy_leagues/sampling_corpus")
SMPL_DIR = CORPUS / "leagues"
DONE = CORPUS / "mfl_pilot_ledger.json"
# Separate from the fleaflicker pilot snapshot: both drivers run concurrently and a
# DuckDB file is single-writer (first smoke died on the shared-file lock).
PILOT_SNAPSHOT = CORPUS / "corpus_pilot_mfl.duckdb"
IMPORT_PY = ROOT / "fantasy_football_data_scripts" / "mfl_initial_import.py"
SIM_PY = ROOT / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "matchup" / "playoff_odds_import.py"


def _err(p: subprocess.CompletedProcess) -> str:
    out = (p.stderr or p.stdout or "").strip()
    if not out:
        return "(no stderr)"
    lines = [ln for ln in out.splitlines() if ln.strip()]
    exc = next((ln.strip() for ln in reversed(lines)
                if ln.strip() and not ln.strip().startswith(("File ", "  ", "Traceback"))), lines[-1].strip())
    return f"{exc}  ||tail|| {out[-600:]}"


def load_env() -> dict:
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "fantasy_football_data_scripts")
    # 15/min, not 25: the throttle penalty ESCALATES once tripped (observed: first call
    # 429 -> 132s -> 264s after ~40 probe calls earlier the same hour). Stay well under.
    env["MFL_RATE_LIMIT_PER_MIN"] = "15"
    env["OPS_CACHE_PATH"] = str(CORPUS / "ops_cache.duckdb")
    # Fail-closed against ALL Fly access; corpus ingests stay fully offline from production.
    env["CORPUS_MODE"] = "1"
    env["DRAFT_GLOBAL_SOURCE_PATH"] = str(CORPUS / "github_dependencies_v1" / "draft_global_source.parquet")
    return env


def ingest_one(seed: str, env: dict, start_year: int | None = None, end_year: int | None = None) -> tuple[str, str]:
    seed_year, _, seed_id = seed.partition(":")
    db = f"smpl_mfl_{seed_year}_{seed_id}"
    ddir = SMPL_DIR / db
    import_args = [sys.executable, str(IMPORT_PY), "--league-id", seed,
                   "--data-dir", str(ddir), "--database-name", db,
                   "--skip-track-1", "--stop-after", "3"]
    if start_year is not None:
        import_args.extend(["--start-year", str(start_year)])
    if end_year is not None:
        import_args.extend(["--end-year", str(end_year)])
    p1 = subprocess.run(
        import_args,
        env=env, capture_output=True, text=True, timeout=14400)
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
    ap.add_argument("--seeds", required=True, help="comma-separated YEAR:ID seeds")
    ap.add_argument("--start-year", type=int)
    ap.add_argument("--end-year", type=int)
    args = ap.parse_args()

    seeds = [s.strip() for s in args.seeds.split(",") if s.strip()]
    done = json.loads(DONE.read_text()) if DONE.exists() else {}
    todo = [s for s in seeds if done.get(s) != "done"]
    print(f"[plan] {len(todo)} MFL leagues to ingest ({len(seeds) - len(todo)} already done)", flush=True)
    if not todo:
        return

    SMPL_DIR.mkdir(parents=True, exist_ok=True)
    env = load_env()
    snap = open_snapshot(PILOT_SNAPSHOT)
    counts = {"done": 0, "fail": 0, "folded": 0}
    t0 = time.time()

    for seed in todo:
        t1 = time.time()
        status, msg = ingest_one(seed, env, args.start_year, args.end_year)
        done[seed] = status
        DONE.write_text(json.dumps(done))
        counts["done" if status == "done" else "fail"] += 1
        if status == "done":
            seed_year, _, seed_id = seed.partition(":")
            ok, fmsg = fold_league(snap, SMPL_DIR / f"smpl_mfl_{seed_year}_{seed_id}")
            if ok:
                counts["folded"] += 1
            else:
                msg = (msg + f" | fold: {fmsg}").strip(" |")
        n = counts["done"] + counts["fail"]
        detail = (" | " + msg) if msg else ""
        print(f"  [{n}/{len(todo)}] mfl {seed} -> {status} ({time.time()-t1:.0f}s){detail}", flush=True)

    snap.close()
    print(f"\n[done] {counts['done']} ok, {counts['fail']} failed, {counts['folded']} folded "
          f"in {(time.time()-t0)/60:.0f} min -> {PILOT_SNAPSHOT}", flush=True)
    if todo and not counts["done"]:
        raise SystemExit(f"[fatal] 0 of {len(todo)} MFL leagues ingested")


if __name__ == "__main__":
    main()
