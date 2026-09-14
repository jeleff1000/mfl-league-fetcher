"""patch_waiver_budget.py -- backfill waiver_budget/waiver_type into the CORPUS snapshot.

FAAB bids only compare as % of budget (Joe 2026-07-17): 28% of budget-known leagues are not
$100 (200/1000/500...), so pooling raw dollars mixes units -- observed avg_faab_bid max 5,259.
The real-league snapshot already carries waiver_budget (its league_settings pull is SELECT *);
the corpus snapshot's LS contract predated the column, so:

  1. open_snapshot() migrates the schema in place (contract now includes both columns), then
  2. leagues with a LOCAL raw dir get per-year values from their own duckdb, and
  3. GH-crawled leagues (raw dirs died with the runners) get ONE Sleeper API call each --
     a Sleeper league_id IS one season, so the league's own settings are that season's truth.

Idempotent: only rows with waiver_budget IS NULL are touched; re-running costs only the
still-missing API calls. Future crawls carry the columns natively (LS_COLS bumped).

    py -3 scripts/sleeper_corpus/patch_waiver_budget.py [--no-api] [--max-api N]
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from build_corpus_snapshot import open_snapshot  # migrates schema per bumped LS_COLS

CORPUS = Path("D:/league-history-data/fantasy_leagues/sampling_corpus")
SNAP = CORPUS / "corpus_snapshot.duckdb"


def load_env() -> None:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            import os
            os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def raw_dir(db_name: str) -> Path | None:
    for cand in (CORPUS / "leagues" / db_name, CORPUS / db_name):
        if (cand / f"{db_name}.duckdb").exists():
            return cand / f"{db_name}.duckdb"
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--no-api", action="store_true", help="raw dirs only; skip Sleeper API fills")
    ap.add_argument("--max-api", type=int, default=5000, help="cap on API calls this run")
    args = ap.parse_args()
    load_env()

    con = open_snapshot(SNAP)
    missing = [r[0] for r in con.execute(
        "SELECT DISTINCT db_name FROM public.league_settings WHERE waiver_budget IS NULL"
    ).fetchall()]
    print(f"[patch] {len(missing):,} leagues missing waiver_budget")

    filled_raw = skipped_locked = 0
    api_queue: list[str] = []
    for db in missing:
        p = raw_dir(db)
        if p is None:
            api_queue.append(db)
            continue
        try:
            con.execute(f"ATTACH '{p.as_posix()}' AS src (READ_ONLY)")
        except Exception:
            skipped_locked += 1  # in-flight grind ingest; a re-run picks it up
            continue
        try:
            src_cols = {r[0] for r in con.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_catalog='src' AND table_name='league_settings'").fetchall()}
            if "waiver_budget" in src_cols:
                wt = ("CAST(s.waiver_type AS VARCHAR)" if "waiver_type" in src_cols else "NULL")
                con.execute(f"""
                    UPDATE public.league_settings t
                    SET waiver_budget = CAST(s.waiver_budget AS INTEGER), waiver_type = {wt}
                    FROM src.public.league_settings s
                    WHERE t.db_name = ? AND s.db_name = t.db_name AND s.year = t.year
                      AND t.waiver_budget IS NULL""", [db])
                filled_raw += 1
            else:
                api_queue.append(db)
        finally:
            try:
                con.execute("DETACH src")
            except Exception:
                pass
    print(f"[patch] raw dirs: filled {filled_raw:,}, locked/skipped {skipped_locked:,}, "
          f"api queue {len(api_queue):,}")

    if not args.no_api and api_queue:
        from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient
        client = SleeperAPIClient()
        ok = err = 0
        for db in api_queue[: args.max_api]:
            lid = db.removeprefix("smpl_")
            try:
                league = client.get_league(lid) or {}
                st = league.get("settings") or {}
                budget = st.get("waiver_budget")
                wt = st.get("waiver_type")
                if budget is not None:
                    con.execute(
                        "UPDATE public.league_settings SET waiver_budget = ?, waiver_type = ? "
                        "WHERE db_name = ? AND waiver_budget IS NULL",
                        [int(budget), str(wt) if wt is not None else None, db])
                    ok += 1
                else:
                    err += 1
            except Exception:
                err += 1
                time.sleep(1)
            if (ok + err) % 250 == 0:
                print(f"[patch] api: {ok:,} filled, {err:,} failed", flush=True)
        print(f"[patch] api done: {ok:,} filled, {err:,} failed")

    n, nb = con.execute("SELECT COUNT(*), COUNT(waiver_budget) FROM public.league_settings").fetchone()
    print(f"[patch] league_settings coverage: {nb:,}/{n:,} league-years with waiver_budget")
    for r in con.execute("SELECT waiver_budget, COUNT(*) FROM public.league_settings "
                         "GROUP BY 1 ORDER BY 2 DESC LIMIT 8").fetchall():
        print("   ", r)
    con.close()


if __name__ == "__main__":
    main()
