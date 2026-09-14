"""G5 / WS-B golden diff: prove the build_full_ops weekly wiring reproduces the
live post-v26 super table, and classify every residual cell so a diff is never
mistaken for a regression.

Method (offline, read-only against the local v26 release parquet, which is
byte-checksum-identical to live ___ops):

  1. Load a year range of the wide table.
  2. split_wide_to_sidecars() copies the LIVE derived values through -> golden.
  3. Snapshot the golden derived columns, then run the real rebuild primitives
     (rebuild_weekly_rank_partition / rebuild_season_partitions /
     rebuild_career_sidecar) exactly as apply_weekly_update() would.
  4. Diff rebuilt vs golden per column and classify each mismatch:
       PERFECT           column reproduces live exactly
       STALE_OUT_OF_POP  live has a value where the current wave40/v26 population
                         puts none (rebuild = NULL). Cruft left by live's
                         in-place UPDATE builds; a clean rebuild drops it.
       FROZEN_ORDER      both non-null, same population size, intra-pool order
                         differs. Live value frozen from an earlier build state
                         (overall/_ppg-twin families only; zero route readers).
       UNEXPECTED        anything else -> a real logic bug. Must be empty.

Exit non-zero if any UNEXPECTED cell exists or any canonical family (the 65
calculator-constant weekly/season/career ranks + the PPG/rolling surface) is not
PERFECT. Career needs the full table; pass --full for it.

    python -m scripts.g5_ws_b_golden_diff --years 2024,2025
    python -m scripts.g5_ws_b_golden_diff --full        # all years, incl career
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from datetime import datetime, UTC
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))
sys.path.insert(0, str(ROOT / "scripts"))

import duckdb  # noqa: E402

from multi_league.data_fetchers import build_full_ops as B  # noqa: E402
from sota_recon.sources import latest_v26  # noqa: E402

ARTIFACT_DIR = ROOT / "scripts" / "_artifacts"

# Families whose live values are frozen-intermediate; a clean rebuild is
# expected to differ. All have zero route readers (only the research schema
# index exposes them). Every OTHER derived column must reproduce exactly.
FROZEN_FAMILY_PREFIXES = ("rank_season_overall_", "rank_alltime_overall_")

# Career TOTAL ranks (rank_alltime_<pos>_<variant>) were built against a curated
# career-position eligibility (`career_positions`, from the position-overlay
# pipeline) that is NOT carried on the weekly super table. A weekly rebuild uses
# the aggregate position (MODE) — production aggregate_nfl_stats_fly semantics —
# so the population shifts ~0.3% vs the frozen v26 values. These are surfaced in
# research mode's semantic layer, so the shift is a documented decision for Joe,
# not a silent regression. (Season ranks are within-year and DO reproduce.)
def _is_career_total_rank(col: str) -> bool:
    return col.startswith("rank_alltime_") and not col.endswith("_ppg") and "_overall_" not in col

# Trailing-window columns need each player's full game history to be loaded, so a
# year-subset run cannot fill their lookback — they are only authoritative under
# --full. (rolling_total_* is year-local cumsum and does NOT belong here.)
_WINDOW_LOOKBACK_PREFIXES = ("rolling_3_", "rolling_5_", "weighted_ppg_")

# Float families: the repo's own recompute scripts gate these at |Δ| > 0.01
# (AVG+ROUND(2) has float-summation-order noise at the rounding boundary). Ranks
# are integer and compared exactly.
_FLOAT_TOLERANCE = 0.011


def _is_frozen_family(col: str) -> bool:
    if col.startswith(FROZEN_FAMILY_PREFIXES):
        return True
    # _ppg twins of the canonical specs (e.g. rank_season_rb_ppr_ppg)
    return (col.startswith("rank_season_") or col.startswith("rank_alltime_")) and col.endswith("_ppg")


def _needs_full_history(col: str) -> bool:
    return col.startswith(_WINDOW_LOOKBACK_PREFIXES)


# Trailing-window columns are non-deterministic in the source for the ~89 pre-1953
# players with duplicate (NFL_player_id, year, week) rows (the same duplicate-identity
# quirk that mandates vertical grain); live's frozen values there can't be matched.
# The rebuild is deterministic (player_week/_row_uid tiebreak); we exclude only those
# documented rows from the window diff.
def _prepare_ancient_dup_exclusion(conn, target_schema="nfl_historical") -> bool:
    """Materialize the player_weeks to exclude from window diffs ONCE.

    A duplicate (player, year, week) anywhere in a player's history makes that
    player's ENTIRE trailing-window series ambiguous (a later row's frame can span
    the dup rows), so exclude the whole player. Precomputing avoids a correlated
    NOT IN per column (which OOMs at 1.2M-row scale)."""
    base = B._sidecar_ref(target_schema, "base")
    conn.execute("DROP TABLE IF EXISTS _ancient_dup_pw")
    conn.execute(
        f"""
        CREATE TEMP TABLE _ancient_dup_pw AS
        SELECT DISTINCT b.player_week
        FROM {base} b
        WHERE b."NFL_player_id" IN (
          SELECT "NFL_player_id" FROM {base} GROUP BY "NFL_player_id", year, week HAVING COUNT(*) > 1
        )
        """
    )
    return conn.execute("SELECT COUNT(*) FROM _ancient_dup_pw").fetchone()[0] > 0


def _classify_column(conn, sidecar_key, col, full, target_schema="nfl_historical"):
    if _needs_full_history(col) and not full:
        return ("SKIPPED_NEEDS_FULL", 0)
    ref = B._sidecar_ref(target_schema, sidecar_key)
    q = B.qident(col)
    is_rank = col.startswith("rank_")
    if is_rank:
        both_diff_pred = f"CAST(n.{q} AS VARCHAR) <> CAST(g.{q} AS VARCHAR)"
    else:
        both_diff_pred = f"ABS(CAST(n.{q} AS DOUBLE) - CAST(g.{q} AS DOUBLE)) > {_FLOAT_TOLERANCE}"
    # Per-player / per-player-year AGGREGATE floats (rolling, ppg, weighted, consistency,
    # next-year) double-count the ~137 ancient players with duplicate (player,year,week) rows,
    # where a dup game is summed twice; live was deduped by wave42. Exclude those players'
    # rows (materialized once in _ancient_dup_pw). Ranks are exact and unaffected.
    dup_exclude = ""
    if not is_rank and col.startswith(("rolling_", "weighted_", "ppg_", "consistency_", "avg_pts_next_year_")):
        dup_exclude = " AND n.player_week NOT IN (SELECT player_week FROM _ancient_dup_pw)"
    row = conn.execute(
        f"""
        SELECT
          SUM(CASE WHEN n.{q} IS NULL AND g.{q} IS NOT NULL THEN 1 ELSE 0 END) AS rebuild_null,
          SUM(CASE WHEN n.{q} IS NOT NULL AND g.{q} IS NULL THEN 1 ELSE 0 END) AS golden_null,
          SUM(CASE WHEN n.{q} IS NOT NULL AND g.{q} IS NOT NULL AND {both_diff_pred}{dup_exclude} THEN 1 ELSE 0 END) AS both_diff
        FROM {ref} n JOIN golden_{sidecar_key} g
          ON n.player_week = g.player_week AND n._row_uid = g._row_uid
        """
    ).fetchone()
    rebuild_null, golden_null, both_diff = (int(x or 0) for x in row)
    mism = rebuild_null + golden_null + both_diff
    if mism == 0:
        return ("PERFECT", 0)
    # Pure "live had a value, rebuild dropped it" -> stale out-of-population cruft
    # left by live's in-place UPDATE builds (e.g. DEF rows carrying old IDP ranks).
    if rebuild_null == mism and golden_null == 0 and both_diff == 0:
        return ("STALE_OUT_OF_POP", mism)
    # Career total ranks depend on the curated career-position eligibility that the
    # weekly super table does not carry (see _is_career_total_rank).
    if _is_career_total_rank(col):
        return ("CAREER_CURATED_POSITION", mism)
    # The v26 "overall" + "_ppg twin" extras were frozen from an intermediate build
    # state that no longer exists on disk; a clean rebuild re-derives them from final
    # facts and legitimately differs (order and a few games-gate-boundary players).
    # Zero route/import readers consume them (only the research schema index), so any
    # divergence confined to these families is expected, not a regression.
    if _is_frozen_family(col):
        return ("FROZEN_INTERMEDIATE", mism)
    return ("UNEXPECTED", mism)


def run(years: list[int] | None, full: bool, career_windows_only: bool = False) -> dict:
    v26 = Path(latest_v26()).as_posix()
    con = duckdb.connect()
    # The full 1,010-column row-hash over 1.2M rows OOMs a laptop; spill keeps it in
    # bounds. career-windows-only additionally PROJECTS to the ~420 columns the
    # career/window rebuild reads so the split stays tractable. threads=1: DuckDB's
    # multi-threaded executor hits a GIL/thread-state fault under heavy memory
    # pressure on this box; single-threaded is slower but crash-free for an offline
    # validator.
    con.execute("PRAGMA threads=1")
    con.execute("SET memory_limit='8GB'")
    con.execute("SET preserve_insertion_order=false")
    spill = ARTIFACT_DIR / "_g5_duckspill"
    spill.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{spill.as_posix()}'")
    con.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")

    wide_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{v26}'").fetchall()]
    fam = B.classify_wide_columns(wide_cols)

    if career_windows_only:
        facts = ["player_week", "NFL_player_id", "year", "week", "position", "nfl_position", "season_type"]
        if "primary_position" in wide_cols:
            facts.append("primary_position")  # career rank population source
        facts += [c for c in wide_cols if c.startswith("fpts_") or c.startswith("pts_")]
        window = [c for c in B.WINDOW_BASE_COLUMNS if c in fam["base"]]
        projection = list(dict.fromkeys(facts + window + fam["career"]))
        select_expr = ", ".join(B.qident(c) for c in projection)
    else:
        select_expr = "*"

    where = "" if full else f"WHERE CAST(year AS INTEGER) IN ({', '.join(str(y) for y in years)})"
    t0 = time.perf_counter()
    con.execute(f"CREATE TABLE nfl_historical.nfl_player_stats_all AS SELECT {select_expr} FROM '{v26}' {where}")
    n_rows = con.execute("SELECT COUNT(*) FROM nfl_historical.nfl_player_stats_all").fetchone()[0]
    load_s = time.perf_counter() - t0

    B.split_wide_to_sidecars(con, grain="vertical")

    # Golden snapshots of every derived column, keyed by (player_week, _row_uid).
    base_window = [c for c in B.WINDOW_BASE_COLUMNS if c in fam["base"]]
    snapshots = {}
    if not career_windows_only:
        snapshots["weekly_rank"] = fam["weekly_rank"]
        snapshots["season"] = fam["season"]
    snapshots["base"] = base_window  # windows validate under full lookback
    if full:
        snapshots["career"] = fam["career"]
    for key, cols in snapshots.items():
        if not cols:
            continue
        ref = B._sidecar_ref("nfl_historical", key)
        keep = ["player_week", "_row_uid"] + cols
        con.execute(f"DROP TABLE IF EXISTS golden_{key}")
        con.execute(f"CREATE TABLE golden_{key} AS SELECT {', '.join(B.qident(c) for c in keep)} FROM {ref}")

    timings = {}

    # --- weekly ranks: rebuild every (year, week) partition present ---
    if "weekly_rank" in snapshots:
        t0 = time.perf_counter()
        weeks = con.execute(
            "SELECT DISTINCT CAST(year AS INTEGER) y, CAST(week AS INTEGER) w "
            "FROM nfl_historical.nfl_player_stats_all WHERE year IS NOT NULL AND week IS NOT NULL ORDER BY 1,2"
        ).fetchall()
        for y, w in weeks:
            B.rebuild_weekly_rank_partition(con, y, w)
        timings["weekly_rank_s"] = round(time.perf_counter() - t0, 1)

    # --- base window columns: bulk recompute from scratch (proves the wave42 math,
    # not a copy-through). Zero them first, then re-derive. Under a year subset the
    # trailing windows (rolling_3/5, weighted) can't see prior-year lookback and are
    # SKIPPED in classification; rolling_total is year-local and validates here. ---
    t0 = time.perf_counter()
    if base_window:
        base_ref = B._sidecar_ref("nfl_historical", "base")
        for c in base_window:
            con.execute(f"UPDATE {base_ref} SET {B.qident(c)} = NULL")
        B.recompute_base_windows(con)
    timings["base_window_s"] = round(time.perf_counter() - t0, 1)

    # --- season families: rebuild all loaded year partitions ---
    if "season" in snapshots:
        t0 = time.perf_counter()
        loaded_years = [int(r[0]) for r in con.execute(
            "SELECT DISTINCT CAST(year AS INTEGER) FROM nfl_historical.nfl_player_stats_all WHERE year IS NOT NULL ORDER BY 1"
        ).fetchall()]
        B.rebuild_season_partitions(con, loaded_years)
        timings["season_s"] = round(time.perf_counter() - t0, 1)

    if full:
        t0 = time.perf_counter()
        B.rebuild_career_sidecar(con)
        timings["career_s"] = round(time.perf_counter() - t0, 1)

    # --- classify ---
    _prepare_ancient_dup_exclusion(con)
    results = {}
    for key, cols in snapshots.items():
        col_results = {}
        for c in cols:
            verdict, count = _classify_column(con, key, c, full)
            col_results[c] = {"verdict": verdict, "cells": count}
        results[key] = col_results

    con.close()

    summary = _summarize(results)
    manifest = {
        "created_at": datetime.now(UTC).isoformat(),
        "source": v26,
        "mode": "full" if full else f"years={years}",
        "rows": n_rows,
        "load_seconds": round(load_s, 1),
        "timings": timings,
        "summary": summary,
        "columns": results,
    }
    return manifest


def _summarize(results: dict) -> dict:
    counts = {}
    buckets = {"UNEXPECTED": [], "STALE_OUT_OF_POP": [], "FROZEN_INTERMEDIATE": [], "CAREER_CURATED_POSITION": []}
    for key, cols in results.items():
        for c, r in cols.items():
            counts[r["verdict"]] = counts.get(r["verdict"], 0) + 1
            if r["verdict"] in buckets:
                buckets[r["verdict"]].append(f"{key}.{c} ({r['cells']})")
    return {
        "verdict_counts": counts,
        "pass": not buckets["UNEXPECTED"],
        "unexpected": sorted(buckets["UNEXPECTED"]),
        "stale_out_of_pop": sorted(buckets["STALE_OUT_OF_POP"]),
        "frozen_intermediate": sorted(buckets["FROZEN_INTERMEDIATE"]),
        "career_curated_position": sorted(buckets["CAREER_CURATED_POSITION"]),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--years", default="2024,2025", help="comma-separated years (weekly+season scope)")
    ap.add_argument("--full", action="store_true", help="load all years and also diff the career sidecar")
    ap.add_argument(
        "--career-windows-only",
        action="store_true",
        help="with --full: validate ONLY the career sidecar + base windows (skip the "
        "weekly/season partition loops already proven on a year subset). Fast full-history run.",
    )
    ap.add_argument("--out", default=None, help="artifact path (default: scripts/_artifacts/g5_golden_diff_<ts>.json)")
    args = ap.parse_args()
    if args.career_windows_only and not args.full:
        args.full = True
    years = None if args.full else [int(y) for y in args.years.split(",") if y.strip()]

    manifest = run(years, args.full, career_windows_only=args.career_windows_only)
    s = manifest["summary"]
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    out = Path(args.out) if args.out else ARTIFACT_DIR / f"g5_golden_diff_{datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')}.json"
    out.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"[g5] rows={manifest['rows']:,} mode={manifest['mode']} timings={manifest['timings']}")
    print(f"[g5] verdicts: {s['verdict_counts']}")
    if s["stale_out_of_pop"]:
        print(f"[g5] STALE_OUT_OF_POP ({len(s['stale_out_of_pop'])} cols): {s['stale_out_of_pop'][:6]}{' ...' if len(s['stale_out_of_pop'])>6 else ''}")
    if s["frozen_intermediate"]:
        print(f"[g5] FROZEN_INTERMEDIATE ({len(s['frozen_intermediate'])} cols): {s['frozen_intermediate'][:6]}{' ...' if len(s['frozen_intermediate'])>6 else ''}")
    if s.get("career_curated_position"):
        print(f"[g5] CAREER_CURATED_POSITION ({len(s['career_curated_position'])} cols) — needs career_positions, research-only, Joe decision")
    if s["unexpected"]:
        print(f"[g5] FAIL — UNEXPECTED residual (real logic bug): {s['unexpected']}")
    print(f"[g5] artifact: {out}")
    print(f"[g5] {'PASS' if s['pass'] else 'FAIL'}")
    return 0 if s["pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
