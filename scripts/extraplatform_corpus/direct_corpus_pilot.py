"""direct_corpus_pilot.py -- FETCH-ONLY corpus ingest for MFL + Fleaflicker.

The full importer (fetch -> transforms -> SQL enrichments -> sim) is overkill for the corpus:
the lake needs exactly four skinny tables, and stage-1 fetch already writes all four table
NAMES (mfl/fleaflicker rosters fetchers emit player_fantasy rows with is_started + league-scored
fantasy_points; matchup fetchers apply playoff outcomes at fetch time). The transform/sim stages
were also where every 2026-07-18 crawl failure lived (run 29626415574: playoff-settings sims,
enrichment timeouts) -- so skip them wholesale.

Per league: importer --stop-after 1 (fetch only, CORPUS_MODE=1, no Fly) -> enrich-lite in
DuckDB (NFL_player_id name-map vs the local ops cache, win/champion joined from matchup,
draft season points) -> fold_league() into a slice. Columns the full pipeline derives
(manager_lamar, *_zscore, transaction_score, clutch_equity) stay NULL -- the cohort builders
average over non-NULL and the per-metric ladder gates on n; these leagues buy the CHEAP-metric
coverage (ADP / start% / add-rate / champ / win) that pre-2017 cells starve for.

QC contract: a league only folds if >=100 rostered player-weeks carry NFL_player_id and >=85%
of rostered rows are identified -- same bar as gated_fold / LocalReader's denominator-ghost
gate, enforced HERE so a bad league never even reaches a slice.

    py -3 scripts/extraplatform_corpus/direct_corpus_pilot.py --platform mfl \
        --leagues 2024:57289 --fold-into D:/tmp/pilot_mfl_slice.duckdb
    py -3 scripts/extraplatform_corpus/direct_corpus_pilot.py --platform fleaflicker \
        --leagues 12345 --fold-into D:/tmp/pilot_ffl_slice.duckdb
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "sleeper_corpus"))
from batch_ingest_extraplatform import (  # noqa: E402
    PLATFORMS, SMPL_DIR, OPS_CACHE, load_env, _db_name, _seed_ids)
from build_corpus_snapshot import fold_league, open_snapshot  # noqa: E402

PID_COLS = ("mfl_player_id", "fleaflicker_player_id", "sleeper_player_id", "yahoo_player_id")

import json as _json
import urllib.request as _rq


def mfl_exposes_players(league_key: str) -> bool:
    """One probe call: does this league's weeklyResults carry player arrays?

    MFL leagues VARY: some commissioners hide per-player detail, leaving only team
    scores (observed: 2024:51125 -- side keys isHome/score/result/id, no `player`).
    Such leagues can never yield player-grain corpus rows; skip them BEFORE spending
    the 15/min budget on a full fetch.
    """
    year, _, lid = league_key.partition(":")
    url = (f"https://api.myfantasyleague.com/{year}/export?TYPE=weeklyResults"
           f"&L={lid}&W=1&JSON=1")
    try:
        req = _rq.Request(url, headers={"User-Agent": "leaguehistory-corpus-pilot"})
        with _rq.urlopen(req, timeout=30) as r:
            d = _json.loads(r.read().decode("utf-8", "replace"))
        wr = d.get("weeklyResults", d) or {}
        mts = wr.get("matchup")
        mts = [mts] if isinstance(mts, dict) else (mts or [])
        for m in mts:
            sides = m.get("franchise")
            sides = [sides] if isinstance(sides, dict) else (sides or [])
            for s in sides:
                if s.get("player") or s.get("players"):
                    return True
        for f in (wr.get("franchise") or []):
            if isinstance(f, dict) and (f.get("player") or f.get("players")):
                return True
        return False
    except Exception:
        return True  # probe inconclusive -> let the real fetch decide

# Name normalization: lower, strip suffix tokens, drop non-alpha. Mirrors the pipeline's
# matcher closely enough for corpus grade; the >=85% fold gate catches any league where
# name quality is too poor to trust.
NAME_NORM = ("regexp_replace(regexp_replace(lower(COALESCE({c}, '')), "
             "'\\s+(jr|sr|ii|iii|iv|v)\\.?$', ''), '[^a-z]', '', 'g')")
POS_GRP = ("CASE WHEN {c} LIKE '%QB%' THEN 'QB' WHEN {c} LIKE '%RB%' OR {c}='FB' THEN 'RB' "
           "WHEN {c} LIKE '%WR%' THEN 'WR' WHEN {c} LIKE '%TE%' THEN 'TE' "
           "WHEN {c}='DEF' THEN 'DEF' WHEN {c} LIKE '%K%' THEN 'K' ELSE COALESCE({c},'') END")


def pid_col(con: duckdb.DuckDBPyConnection, table: str) -> str | None:
    cols = {r[0] for r in con.execute(f"DESCRIBE {table}").fetchall()}
    return next((c for c in PID_COLS if c in cols), None)


def fetch_stage(platform: str, league_key: str, env: dict) -> tuple[Path, str]:
    cfg = PLATFORMS[platform]
    db = _db_name(platform, league_key)
    ddir = SMPL_DIR / db
    cmd = [sys.executable, str(cfg["importer"]), "--league-id", league_key,
           "--data-dir", str(ddir), "--database-name", db,
           "--skip-track-1", "--stop-after", "1", *cfg["extra_args"]]
    p = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=14400)
    if p.returncode != 0:
        tail = (p.stderr or p.stdout or "").strip().splitlines()
        raise RuntimeError("fetch failed: " + (tail[-1][:160] if tail else "(no output)"))
    return ddir, db


def enrich_lite(ddir: Path, db: str) -> dict[str, float]:
    """NFL_player_id map + win/champion + draft season points, in place. Returns QC stats."""
    con = duckdb.connect(str(ddir / f"{db}.duckdb"))
    try:
        con.execute(f"ATTACH '{OPS_CACHE.as_posix()}' AS ops (READ_ONLY)")
        tables = {r[0] for r in con.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()}
        if "player_fantasy" not in tables:
            raise RuntimeError("fetch produced no player_fantasy table")

        # --- candidate NFL ids: name+position(+era) from the local super table -------------
        nn, pg = NAME_NORM.format(c="player"), POS_GRP.format(c='"position"')
        con.execute(f"""CREATE TEMP TABLE cand AS
            SELECT {nn} AS name_norm, {pg} AS pos_grp, NFL_player_id,
                   MIN(year) AS y0, MAX(year) AS y1
            FROM ops.nfl_historical.nfl_player_stats_all
            WHERE player IS NOT NULL AND NFL_player_id IS NOT NULL
            GROUP BY 1, 2, 3""")

        # --- league-side distinct players (id + name + position), across all 3 tables ------
        parts = []
        for t in ("player_fantasy", "draft", "transactions"):
            if t not in tables:
                continue
            pc = pid_col(con, f"public.{t}")
            if pc:
                parts.append(f"SELECT CAST({pc} AS VARCHAR) pid, player, \"position\", year "
                             f"FROM public.{t} WHERE {pc} IS NOT NULL AND player IS NOT NULL")
        if not parts:
            raise RuntimeError("no platform player id column found")
        con.execute(f"""CREATE TEMP TABLE lg_players AS
            SELECT pid, ANY_VALUE(player) AS player, ANY_VALUE("position") AS position,
                   MIN(year) AS ly0, MAX(year) AS ly1
            FROM ({' UNION ALL '.join(parts)}) GROUP BY pid""")

        # tier 1: name+pos, era overlap, unique -> tier 2: name only, era overlap, unique
        nn_l, pg_l = NAME_NORM.format(c="player"), POS_GRP.format(c='"position"')
        con.execute(f"""CREATE TEMP TABLE pid_map AS
            WITH t1 AS (
              SELECT l.pid, c.NFL_player_id,
                     COUNT(DISTINCT c.NFL_player_id) OVER (PARTITION BY l.pid) AS n
              FROM lg_players l JOIN cand c
                ON c.name_norm = {nn_l} AND c.pos_grp = {pg_l}
               AND c.y1 >= l.ly0 - 1 AND c.y0 <= l.ly1 + 1),
            t2 AS (
              SELECT l.pid, c.NFL_player_id,
                     COUNT(DISTINCT c.NFL_player_id) OVER (PARTITION BY l.pid) AS n
              FROM lg_players l JOIN cand c
                ON c.name_norm = {nn_l}
               AND c.y1 >= l.ly0 - 1 AND c.y0 <= l.ly1 + 1
              WHERE l.pid NOT IN (SELECT pid FROM t1 WHERE n = 1))
            SELECT DISTINCT pid, NFL_player_id FROM t1 WHERE n = 1
            UNION ALL SELECT DISTINCT pid, NFL_player_id FROM t2 WHERE n = 1""")

        # Name-level map for tables WITHOUT a platform id column (canonical player_fantasy
        # drops mfl/fleaflicker ids): names that resolve to exactly one NFL id in the
        # league's era window, tiered name+pos then name-only.
        con.execute(f"""CREATE TEMP TABLE lg_years AS
            SELECT MIN(year) AS ly0, MAX(year) AS ly1 FROM public.player_fantasy""")
        con.execute("""CREATE TEMP TABLE name_map AS
            WITH era AS (SELECT c.* FROM cand c, lg_years y
                         WHERE c.y1 >= y.ly0 - 1 AND c.y0 <= y.ly1 + 1),
            t1 AS (SELECT name_norm, pos_grp, NFL_player_id,
                          COUNT(DISTINCT NFL_player_id) OVER (PARTITION BY name_norm, pos_grp) n
                   FROM era)
            SELECT DISTINCT name_norm, pos_grp, NFL_player_id FROM t1 WHERE n = 1""")

        for t in ("player_fantasy", "draft", "transactions"):
            if t not in tables:
                continue
            pc = pid_col(con, f"public.{t}")
            if pc:
                con.execute(f"""UPDATE public.{t} x SET NFL_player_id = m.NFL_player_id
                    FROM pid_map m WHERE x.NFL_player_id IS NULL
                      AND CAST(x.{pc} AS VARCHAR) = m.pid""")
            tcols = {r[0] for r in con.execute(f"DESCRIBE public.{t}").fetchall()}
            if {"player", "position"} <= tcols:
                nn_x, pg_x = NAME_NORM.format(c="x.player"), POS_GRP.format(c='x."position"')
                con.execute(f"""UPDATE public.{t} x SET NFL_player_id = nm.NFL_player_id
                    FROM name_map nm WHERE x.NFL_player_id IS NULL
                      AND nm.name_norm = {nn_x} AND nm.pos_grp = {pg_x}""")

        # --- win/champion from the matchup table (team-week -> player-week) ----------------
        if "matchup" in tables:
            mcols = {r[0] for r in con.execute("DESCRIBE public.matchup").fetchall()}
            pcols = {r[0] for r in con.execute("DESCRIBE public.player_fantasy").fetchall()}
            if {"franchise_id", "win"} <= mcols and "franchise_id" in pcols:
                champ = "COALESCE(CAST(m.champion AS INT), 0)" if "champion" in mcols else "0"
                for col, typ in (("win", "INTEGER"), ("champion", "INTEGER")):
                    if col not in pcols:
                        con.execute(f"ALTER TABLE public.player_fantasy ADD COLUMN {col} {typ}")
                con.execute(f"""UPDATE public.player_fantasy p
                    SET win = CAST(m.win AS INT), champion = {champ}
                    FROM public.matchup m
                    WHERE m.franchise_id = p.franchise_id AND m.year = p.year AND m.week = p.week""")

        # --- draft season points: sum of the league's own weekly scores --------------------
        if "draft" in tables:
            dcols = {r[0] for r in con.execute("DESCRIBE public.draft").fetchall()}
            if "total_fantasy_points" not in dcols:
                con.execute("ALTER TABLE public.draft ADD COLUMN total_fantasy_points DOUBLE")
            con.execute("""UPDATE public.draft d SET total_fantasy_points = s.pts
                FROM (SELECT NFL_player_id, year, SUM(fantasy_points) AS pts
                      FROM public.player_fantasy WHERE NFL_player_id IS NOT NULL
                      GROUP BY 1, 2) s
                WHERE d.total_fantasy_points IS NULL
                  AND d.NFL_player_id = s.NFL_player_id AND d.year = s.year""")

        rid, rall = con.execute("""SELECT
            COUNT(*) FILTER (WHERE CAST(is_rostered AS INT)=1 AND NFL_player_id IS NOT NULL),
            COUNT(*) FILTER (WHERE CAST(is_rostered AS INT)=1) FROM public.player_fantasy""").fetchone()
        years = con.execute(
            "SELECT COUNT(DISTINCT year) FROM public.player_fantasy").fetchone()[0]
        return {"rostered": rall or 0, "identified": rid or 0,
                "id_pct": (100.0 * rid / rall) if rall else 0.0, "years": years or 0}
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--platform", required=True, choices=sorted(PLATFORMS))
    ap.add_argument("--leagues", default=None,
                    help="comma-separated league keys (mfl: YEAR:ID, fleaflicker: ID)")
    ap.add_argument("--from-seed", action="store_true",
                    help="take league keys from the platform's discovery seed instead of --leagues")
    ap.add_argument("--offset", type=int, default=0, help="with --from-seed: skip first N")
    ap.add_argument("--limit", type=int, default=None, help="with --from-seed: cap this slice")
    ap.add_argument("--fold-into", default=None, help="slice duckdb to fold passing leagues into")
    ap.add_argument("--skip-fetch", action="store_true",
                    help="reuse an existing raw dir (re-run enrich+fold only)")
    ap.add_argument("--skip-done", action="store_true",
                    help="skip leagues already folded into the target slice or the corpus lake")
    ap.add_argument("--skip-list", default=None,
                    help="file of db_names (one per line) to skip -- for runners with no lake")
    args = ap.parse_args()

    if args.from_seed:
        keys = _seed_ids(args.platform)[args.offset:]
        if args.limit is not None:
            keys = keys[:args.limit]
        args.leagues = ",".join(keys)
    if not args.leagues:
        raise SystemExit("need --leagues or --from-seed")

    env = load_env(args.platform, workers=1)
    done: set[str] = set()
    if args.skip_list and Path(args.skip_list).is_file():
        done |= {ln.strip() for ln in Path(args.skip_list).read_text().splitlines() if ln.strip()}
        args.skip_done = True
    if args.skip_done:
        lake = "D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb"
        for path in ([args.fold_into] if args.fold_into else []) + [lake]:
            try:
                c = duckdb.connect(path, read_only=True)
                done |= {r[0] for r in c.execute("SELECT db_name FROM _sources").fetchall()}
                c.close()
            except Exception as e:
                print(f"[skip-done] could not read {path}: {str(e).splitlines()[0][:80]}", flush=True)
        print(f"[skip-done] {len(done):,} leagues already landed", flush=True)
    results = []
    for key in [k.strip() for k in args.leagues.split(",") if k.strip()]:
        t0 = time.time()
        db = _db_name(args.platform, key)
        ddir = SMPL_DIR / db
        if args.skip_done and db in done:
            print(f"[{key}] SKIP_ALREADY_LANDED", flush=True)
            continue
        try:
            if args.platform == "mfl" and not args.skip_fetch and not mfl_exposes_players(key):
                results.append((key, "SKIP_NO_PLAYER_EXPOSURE", {}, 0, time.time() - t0))
                print(f"[{key}] SKIP_NO_PLAYER_EXPOSURE (probe, {time.time()-t0:.0f}s)", flush=True)
                continue
            if not args.skip_fetch:
                ddir, db = fetch_stage(args.platform, key, env)
            t_fetch = time.time() - t0
            qc = enrich_lite(ddir, db)
            ok = qc["identified"] >= 100 and qc["id_pct"] >= 85.0
            status = "PASS" if ok else "GATE_FAIL"
            if ok and args.fold_into:
                snap = open_snapshot(Path(args.fold_into))
                folded, msg = fold_league(snap, ddir)
                snap.close()
                if not folded:
                    status, ok = f"FOLD_FAIL({msg})", False
            results.append((key, status, qc, t_fetch, time.time() - t0))
            print(f"[{key}] {status} | {qc['years']} yrs | rostered {qc['rostered']:,} "
                  f"| id {qc['id_pct']:.1f}% | fetch {t_fetch:.0f}s | total {time.time()-t0:.0f}s",
                  flush=True)
        except Exception as e:
            results.append((key, "FAIL", {}, 0, time.time() - t0))
            print(f"[{key}] FAIL ({time.time()-t0:.0f}s): {str(e).splitlines()[0][:160]}", flush=True)

    ok_n = sum(1 for _, s, *_ in results if s == "PASS")
    print(f"\n[done] {ok_n}/{len(results)} passed", flush=True)
    if args.fold_into and ok_n:
        snap = duckdb.connect(args.fold_into, read_only=True)
        n = snap.execute("SELECT COUNT(*) FROM _sources").fetchone()[0]
        print(f"[slice] {args.fold_into}: {n} league(s)")
        snap.close()


if __name__ == "__main__":
    main()
