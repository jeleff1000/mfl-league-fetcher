"""batch_ingest_extraplatform.py -- MFL + Fleaflicker corpus ingest driver.

The extra-platform sibling of scripts/sleeper_corpus/batch_ingest_corpus.py. Reads a
platform discovery seed, runs each league through its importer (--skip-track-1 --stop-after 3,
so NO Fly write and NO super-table read), then the playoff sim (clutch) and a fold into a
compact snapshot -- exactly the Sleeper recipe, platform-swapped.

Runs IDENTICALLY locally and on a GitHub runner: every path is env-overridable, and
load_env() fail-closes against Fly (CORPUS_MODE=1). Matrix jobs carve disjoint slices via
--offset/--limit off the deterministic (sorted) plan.

    py -3 scripts/extraplatform_corpus/batch_ingest_extraplatform.py --platform mfl --plan-only
    py -3 scripts/extraplatform_corpus/batch_ingest_extraplatform.py --platform fleaflicker \
        --limit 3 --fold-into D:/tmp/slice.duckdb
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sleeper_corpus"))
from build_corpus_snapshot import fold_league, open_snapshot  # noqa: E402

# Env-overridable so this runs unchanged on a Linux GH runner (nothing on D: there).
ROOT = Path(os.environ.get("YAHOO_OAUTH_ROOT") or Path(__file__).resolve().parents[2])
CORPUS = Path(os.environ.get("CORPUS_DIR", "D:/league-history-data/fantasy_leagues/sampling_corpus"))
SMPL_DIR = CORPUS / "leagues"
OPS_CACHE = Path(os.environ.get("OPS_CACHE_PATH", str(CORPUS / "ops_cache.duckdb")))
DRAFT_GLOBAL = Path(os.environ.get(
    "DRAFT_GLOBAL_SOURCE_PATH", str(CORPUS / "github_dependencies_v1" / "draft_global_source.parquet")))
SIM_PY = ROOT / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "matchup" / "playoff_odds_import.py"
AGG_PY = ROOT / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "aggregation" / "aggregate_fantasy_context.py"
CLUTCH_PY = ROOT / "fantasy_football_data_scripts" / "multi_league" / "transformations" / "player" / "clutch_to_player.py"

# platform -> (importer, seed filename, per-league-id-key, importer-extra-args, safe rate/min, db-prefix)
PLATFORMS = {
    "mfl": {
        "importer": ROOT / "fantasy_football_data_scripts" / "mfl_initial_import.py",
        "seed": "mfl_crawl_seed.json",
        "id_key": "seed",                 # YEAR:ID
        "extra_args": [],
        "rate_env": "MFL_RATE_LIMIT_PER_MIN",
        "rate": "15",                     # hard escalating 429 -> stay low
        "db_prefix": "smpl_mfl",
    },
    "fleaflicker": {
        "importer": ROOT / "fantasy_football_data_scripts" / "fleaflicker_initial_import.py",
        "seed": "fleaflicker_crawl_seed.json",
        "id_key": "league_id",
        "extra_args": ["--discover-years"],   # epoch-witness season discovery
        "rate_env": "FLEAFLICKER_RATE_LIMIT_PER_MIN",
        "rate": "45",                     # soft 403 -> recovers <=60s
        "db_prefix": "smpl_ffl",
    },
}

_lock = threading.Lock()


def load_env(platform: str, workers: int) -> dict:
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "fantasy_football_data_scripts")
    # Fail-closed against ALL Fly access; read super-table/draft baseline from local caches.
    env["CORPUS_MODE"] = "1"
    env["OPS_CACHE_PATH"] = str(OPS_CACHE)
    env["DRAFT_GLOBAL_SOURCE_PATH"] = str(DRAFT_GLOBAL)
    # Share the global per-year MFL player DB across league subprocesses (one API
    # call per year per HOST instead of per league-year).
    env["MFL_PLAYERS_CACHE_DIR"] = str(CORPUS / "players_cache")
    # Per-process rate limiter; workers multiply the host rate, so divide across them.
    cfg = PLATFORMS[platform]
    env[cfg["rate_env"]] = str(max(1, int(cfg["rate"]) // max(1, workers)))
    return env


def _seed_ids(platform: str) -> list[str]:
    cfg = PLATFORMS[platform]
    seed_path = CORPUS / "discovery" / cfg["seed"]
    if not seed_path.is_file():
        raise SystemExit(f"[fatal] seed not found: {seed_path} -- run discovery first")
    data = json.loads(seed_path.read_text())
    ids = []
    seen = set()
    for row in data.get("leagues", []):
        key = str(row.get(cfg["id_key"]) or "").strip()
        if key and key not in seen:
            seen.add(key)
            ids.append(key)
    # Deterministic order so --offset/--limit slices are disjoint across matrix jobs.
    return sorted(ids)


def _seed_year_args(platform: str, league_key: str) -> list[str]:
    """Use discovery-verified season bounds instead of re-probing lineage."""
    cfg = PLATFORMS[platform]
    seed_path = CORPUS / "discovery" / cfg["seed"]
    try:
        data = json.loads(seed_path.read_text(encoding="utf-8"))
        row = next(
            (r for r in data.get("leagues", [])
             if str(r.get(cfg["id_key"]) or "").strip() == league_key),
            None,
        )
        if platform == "mfl":
            # MFL's YEAR:ID is only the seed label. The MFL importer follows the
            # league's history.league links and builds the complete year -> ID map.
            # Passing seed-year bounds here silently truncates a multi-year league
            # to the year embedded in its database name.
            return []
        else:
            seasons = sorted({int(y) for y in (row or {}).get("seasons", [])})
            if seasons:
                return ["--start-year", str(seasons[0]), "--end-year", str(seasons[-1])]
    except (OSError, ValueError, TypeError):
        pass
    return list(cfg["extra_args"])


def _db_name(platform: str, league_key: str) -> str:
    safe = league_key.replace(":", "_")
    return f"{PLATFORMS[platform]['db_prefix']}_{safe}"


def _seed_key_from_db(platform: str, db_name: str) -> str:
    """Convert a landed database suffix back to the seed's canonical key."""
    prefix = PLATFORMS[platform]["db_prefix"] + "_"
    suffix = db_name[len(prefix):]
    # MFL seed keys are YEAR:LEAGUE, while filesystem/database names use YEAR_LEAGUE.
    if platform == "mfl" and "_" in suffix:
        year, league_id = suffix.split("_", 1)
        return f"{year}:{league_id}"
    return suffix


def _err(p: subprocess.CompletedProcess) -> str:
    out = (p.stderr or p.stdout or "").strip()
    if not out:
        return "(no stderr)"
    lines = [ln for ln in out.splitlines() if ln.strip()]
    exc = next((ln.strip() for ln in reversed(lines)
                if ln.strip() and not ln.strip().startswith(("File ", "  ", "Traceback"))), lines[-1].strip())
    return f"{exc}  ||tail|| {out[-500:]}"


def _source_core_ready(league_dir: Path) -> bool:
    """Whether a runner failure still left a complete schema worth folding."""
    db = league_dir / f"{league_dir.name}.duckdb"
    if not db.is_file():
        return False
    try:
        import duckdb
        con = duckdb.connect(str(db), read_only=True)
        try:
            tables = {r[0] for r in con.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='public'"
            ).fetchall()}
            # Matchup is optional for schema salvage: some MFL leagues expose player
            # history but fail while reconciling outcomes.  Such rows are useful for
            # coverage, but cannot receive playoff/champ evidence until a matchup
            # source is available.
            if not {"league_settings", "player_fantasy"} <= tables:
                return False
            return con.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0] > 0
        finally:
            con.close()
    except Exception:
        # Locked/partial databases are retried later; never fold them speculatively.
        return False


def ingest_one(platform: str, league_key: str, env: dict) -> tuple[str, str]:
    cfg = PLATFORMS[platform]
    db = _db_name(platform, league_key)
    ddir = SMPL_DIR / db
    importer_args = _seed_year_args(platform, league_key) or list(cfg["extra_args"])
    cmd = [sys.executable, str(cfg["importer"]), "--league-id", league_key,
           "--data-dir", str(ddir), "--database-name", db,
           "--skip-track-1", "--stop-after", "3", *importer_args]
    p1 = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=14400)
    if p1.returncode != 0:
        return "import_failed", _err(p1)
    p2 = subprocess.run(
        [sys.executable, str(SIM_PY), "--db", db, "--data-dir", str(ddir)],
        env=env, capture_output=True, text=True, timeout=3600)
    if p2.returncode != 0:
        return "sim_failed", _err(p2)
    # Clutch is a playoff-runner product, never an importer-side approximation:
    # playoff odds must write p_champ first, then aggregation derives the fantasy
    # context, and only clutch_to_player materializes clutch_equity.  Fail closed
    # when the source league lacks the LAMAR/evidence inputs needed to produce it.
    for stage, script in (("aggregate", AGG_PY), ("clutch", CLUTCH_PY)):
        p = subprocess.run(
            [sys.executable, str(script), "--db", db, "--data-dir", str(ddir)],
            env=env, capture_output=True, text=True, timeout=3600)
        if p.returncode != 0:
            return "sim_failed", f"{stage}: {_err(p)}"
    try:
        import duckdb
        check = duckdb.connect(str(ddir / f"{db}.duckdb"), read_only=True)
        try:
            non_null = check.execute(
                "SELECT COUNT(*) FROM public.player_fantasy WHERE clutch_equity IS NOT NULL"
            ).fetchone()[0]
        finally:
            check.close()
    except Exception as exc:
        return "sim_failed", f"clutch verification: {exc}"
    if not non_null:
        return "sim_failed", "clutch runner produced no non-null clutch_equity (missing LAMAR/input evidence)"
    return "done", ""


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--platform", required=True, choices=sorted(PLATFORMS))
    ap.add_argument("--offset", type=int, default=0, help="skip first N of the deterministic plan")
    ap.add_argument("--limit", type=int, default=None, help="cap this slice (with --offset carves a matrix slice)")
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--plan-only", action="store_true")
    ap.add_argument("--fold-into", default=None, help="snapshot to fold each league into as it lands")
    ap.add_argument("--prune", action="store_true", help="rm each raw league dir after folding (requires --fold-into)")
    args = ap.parse_args()
    if args.prune and not args.fold_into:
        ap.error("--prune requires --fold-into")

    platform = args.platform
    ids = _seed_ids(platform)
    done_path = CORPUS / f"{platform}_crawl_ledger.json"
    with _lock:
        done = json.loads(done_path.read_text()) if done_path.exists() else {}
    # A crawl ledger may be absent on a fresh/local runner, while the compact lake
    # (or a previous interrupted run) already contains the league.  Treat those
    # databases as landed so a retry does not re-import the deterministic first ID
    # forever.  This mirrors the Sleeper harvester's landed-database skip list.
    prefix = PLATFORMS[platform]["db_prefix"] + "_"
    for src in SMPL_DIR.glob(prefix + "*"):
        db = src.name
        if (src / f"{db}.duckdb").is_file():
            done.setdefault(_seed_key_from_db(platform, db), "done")
    landed_file = CORPUS / "landed_dbs.txt"
    if not landed_file.exists():
        landed_file = ROOT / "corpus_seed" / "landed_dbs.txt"
    if landed_file.exists():
        for line in landed_file.read_text(encoding="utf-8").splitlines():
            db = line.strip()
            if db.startswith(prefix):
                done.setdefault(_seed_key_from_db(platform, db), "done")
    todo = [k for k in ids if done.get(k) != "done"]
    if args.offset:
        todo = todo[args.offset:]
    if args.limit:
        todo = todo[:args.limit]
    print(f"[plan] {platform}: {len(ids)} in seed, {len(todo)} in this slice "
          f"(offset={args.offset}, limit={args.limit}, "
          f"{sum(1 for v in done.values() if v == 'done')} already done)", flush=True)
    if args.plan_only:
        for k in todo[:12]:
            print(f"    {k}", flush=True)
        return

    SMPL_DIR.mkdir(parents=True, exist_ok=True)
    env = load_env(platform, args.workers)
    print(f"[rate] {args.workers} workers x {env[PLATFORMS[platform]['rate_env']]}/min", flush=True)
    snap = open_snapshot(args.fold_into) if args.fold_into else None
    counts = {"done": 0, "fail": 0, "folded": 0}
    t0 = time.time()

    def run(league_key: str) -> None:
        t1 = time.time()
        status, msg = ingest_one(platform, league_key, env)
        with _lock:
            done[league_key] = status
            done_path.write_text(json.dumps(done))
            counts["done" if status == "done" else "fail"] += 1
            # A complete fetch with a late-stage importer or runner failure is still
            # valuable schema coverage. Fold it, but retain the failure status so the
            # importer/runner can retry after a fix. _source_core_ready() rejects
            # partial/locked databases and requires non-empty player_fantasy, so this
            # cannot turn an early fetch failure into speculative lake rows. Playoff
            # evidence remains unresolved until a matchup source and runner succeed.
            foldable = (
                status == "done"
                or status in {"import_failed", "sim_failed"}
                and _source_core_ready(SMPL_DIR / _db_name(platform, league_key))
            )
            if snap is not None and foldable:
                ddir = SMPL_DIR / _db_name(platform, league_key)
                ok, fmsg = fold_league(snap, ddir)
                if ok:
                    # CHECKPOINT after every fold: snap is a long-lived connection whose
                    # close() never runs when a runner is SIGKILLed at timeout-minutes, so
                    # anything still in the WAL dies with the runner (the .wal is not in the
                    # artifact upload path).  Run 30307350629 lost 96 of 579 crawled leagues
                    # this way -- ~185 runner-hours.  A checkpoint per league is negligible
                    # against a ~1,100s/league crawl and makes the slice durable at all times.
                    snap.execute("CHECKPOINT")
                    counts["folded"] += 1
                    if args.prune:
                        shutil.rmtree(ddir, ignore_errors=True)
                else:
                    msg = (msg + f" | fold: {fmsg}").strip(" |")
            n = counts["done"] + counts["fail"]
            tag = "OK" if status == "done" else status.upper()
            detail = (" | " + msg) if msg else ""
            print(f"  [{n}/{len(todo)}] {platform} {league_key} -> {tag} ({time.time()-t1:.0f}s){detail}", flush=True)

    if args.workers <= 1:
        for k in todo:
            run(k)
    else:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            list(as_completed(ex.submit(run, k) for k in todo))

    if snap is not None:
        snap.close()
    print(f"\n[done] {counts['done']} ok, {counts['fail']} failed, {counts['folded']} folded "
          f"in {(time.time()-t0)/60:.0f} min", flush=True)
    # Exit non-zero when nothing worked -- a driver that counts failures but returns 0 lets a
    # CI job go green on a 100% failure rate.
    if todo and not counts["done"]:
        raise SystemExit(f"[fatal] 0 of {len(todo)} {platform} leagues ingested")


if __name__ == "__main__":
    main()
