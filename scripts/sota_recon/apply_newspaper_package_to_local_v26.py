"""
sota_recon/apply_newspaper_package_to_local_v26.py -- the PIPELINE apply of the settled
newspaper package (never a cell poke): v26 subject + staged package -> NEW release dir.

This is the v23-precedent local apply (apply_pfa_ancient_upsert_bundle_to_local) rebuilt
for the newspaper settlement:
  * input subject is the CLEAN v26 (sha-gated to the sha the routing was adjudicated on)
  * null-fill pass: fills ONLY currently-NULL cells; ANY non-NULL differing cell ABORTS
    fail-closed (the routing already proved 0 overwrite violations; this re-proves it)
  * insert pass: NOT EXISTS-protected net-new player_weeks (canonical HIST/bio id space --
    the routing's bio.pfr_id crosswalk guarantees no twins)
  * output is a NEW release dir on the v26 line (latest_v26() picks it up so the derived
    recompute chain -- rederive composites, deterministic recompute, rescore fpts -- and
    the recon lanes run against it natively); the input release is never touched
  * per-cell facts ledger + report written into the release dir

    python -m scripts.sota_recon.apply_newspaper_package_to_local_v26              # dry run
    python -m scripts.sota_recon.apply_newspaper_package_to_local_v26 --apply
"""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .sources import DATA_LAKE, latest_v26

# the clean supertable the routing/package were adjudicated against
EXPECTED_SUBJECT_SHA = "165ef7b92f1723b969817b05644aba90a3f4f20a663f9943036bc2897850aa76"
PKG_ROOT = Path(DATA_LAKE) / "curated" / "ancient_source_recovery" / "newspaper_ocr_recovery"
RELEASES = Path(DATA_LAKE) / "releases"

STAT_COLS = [
    "carries", "rushing_yards", "rushing_tds", "attempts", "completions",
    "passing_yards", "passing_tds", "passing_interceptions", "receptions",
    "receiving_yards", "receiving_tds", "pat_made", "pat_att", "fg_made",
    "fg_att", "fg_long", "def_interceptions", "fumbles", "fumbles_lost", "punts",
    "def_int_ret_td", "fum_ret_td", "kickoff_return_tds", "punt_return_tds",
]
IDENTITY_COLS = ["player_week", "NFL_player_id", "player", "year", "week",
                 "season_type", "nfl_team", "opponent_nfl_team", "data_source"]


def _sha256(p: str) -> str:
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def latest_package() -> Path:
    dirs = sorted(PKG_ROOT.glob("*_pilot_v1"))
    if not dirs:
        raise SystemExit(f"No staged package under {PKG_ROOT}")
    return dirs[-1]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    subject = latest_v26()
    sha = _sha256(subject)
    if sha != EXPECTED_SUBJECT_SHA:
        raise SystemExit(f"GUARD: subject sha {sha[:12]} != adjudicated {EXPECTED_SUBJECT_SHA[:12]}\n"
                         f"  subject: {subject}\n  (a newer v26 release exists; re-run the routing first)")
    pkg_dir = latest_package()
    pkg_csv = (pkg_dir / "newspaper_weekly_stat_upsert_rows.csv").as_posix()
    idn_csv = (pkg_dir / "newspaper_player_identity_upsert_rows.csv").as_posix()

    con = duckdb.connect()
    con.execute(f"CREATE TABLE pkg AS SELECT * FROM read_csv_auto('{pkg_csv}', sample_size=-1)")
    n_pkg, = con.execute("SELECT COUNT(*) FROM pkg").fetchone()
    n_syn, = con.execute(f"""
        SELECT COUNT(*) FROM read_csv_auto('{idn_csv}', sample_size=-1)
        WHERE player_bio_upsert_state = 'insert_synthetic_player_bio_candidate'""").fetchone()
    if n_syn:
        raise SystemExit(f"GUARD: {n_syn} synthetic-bio candidates present; identity lane must run first")

    print(f"subject : {subject}\npackage : {pkg_csv}\nrows    : {n_pkg}")
    con.execute(f"CREATE TABLE super AS SELECT * FROM read_parquet('{Path(subject).as_posix()}')")
    rows_before, = con.execute("SELECT COUNT(*) FROM super").fetchone()

    # ---- conflict re-proof (fail-closed) --------------------------------------------
    conflicts = []
    for c in STAT_COLS:
        n, = con.execute(f"""
            SELECT COUNT(*) FROM pkg p JOIN super s USING (player_week)
            WHERE p.upsert_state = 'existing_player_week_review'
              AND p.{c} IS NOT NULL AND s.{c} IS NOT NULL
              AND ABS(TRY_CAST(s.{c} AS DOUBLE) - TRY_CAST(p.{c} AS DOUBLE)) > 1e-9""").fetchone()
        if n:
            conflicts.append((c, n))
    if conflicts:
        raise SystemExit(f"GUARD: non-NULL differing cells present, refusing: {conflicts}")

    # ---- fill plan -------------------------------------------------------------------
    fill_counts = {}
    for c in STAT_COLS:
        n, = con.execute(f"""
            SELECT COUNT(*) FROM pkg p JOIN super s USING (player_week)
            WHERE p.upsert_state = 'existing_player_week_review'
              AND p.{c} IS NOT NULL AND s.{c} IS NULL""").fetchone()
        if n:
            fill_counts[c] = n
    n_fills = sum(fill_counts.values())

    n_inserts, = con.execute("""
        SELECT COUNT(*) FROM pkg p
        WHERE p.upsert_state = 'insert_candidate'
          AND NOT EXISTS (SELECT 1 FROM super s WHERE s.player_week = p.player_week)""").fetchone()
    n_ins_collide, = con.execute("""
        SELECT COUNT(*) FROM pkg p
        WHERE p.upsert_state = 'insert_candidate'
          AND EXISTS (SELECT 1 FROM super s WHERE s.player_week = p.player_week)""").fetchone()

    print(f"plan    : {n_fills} cell fills across {len(fill_counts)} cols; "
          f"{n_inserts} row inserts ({n_ins_collide} collisions skipped)")
    for c, n in sorted(fill_counts.items()):
        print(f"    fill {c:24s} {n}")

    if not args.apply:
        print("\nDRY RUN -- no writes. Re-run with --apply.")
        return

    # ---- apply -----------------------------------------------------------------------
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    release_name = f"nfl_local_release_newspaper_settlement_{stamp}_v26"
    out_dir = RELEASES / release_name / "tables"
    out_dir.mkdir(parents=True, exist_ok=False)
    out_parquet = (out_dir / "nfl_player_stats_all.parquet").as_posix()

    # facts ledger BEFORE mutation (old values provably NULL by the plan)
    facts_sql = f"""
        SELECT p.player_week, p.NFL_player_id, p.player, col.c stat_col,
               NULL old_value, col.v new_value, p.source_files boxscore_id,
               p.identity_resolution, 'newspaper_settlement_{stamp}' wave
        FROM pkg p JOIN super s USING (player_week)
        CROSS JOIN LATERAL (VALUES {", ".join(f"('{c}', p.{c})" for c in STAT_COLS)}) col(c, v)
        WHERE p.upsert_state = 'existing_player_week_review'
          AND col.v IS NOT NULL
          AND CASE col.c {" ".join(f"WHEN '{c}' THEN s.{c} IS NULL" for c in STAT_COLS)} END"""
    con.execute(f"COPY ({facts_sql}) TO '{(out_dir.parent / 'newspaper_settlement_cell_facts.csv').as_posix()}' (HEADER, DELIMITER ',')")

    for c in fill_counts:
        con.execute(f"""
            UPDATE super SET {c} = TRY_CAST(p.{c} AS DOUBLE)
            FROM pkg p
            WHERE super.player_week = p.player_week
              AND p.upsert_state = 'existing_player_week_review'
              AND p.{c} IS NOT NULL AND super.{c} IS NULL""")

    ins_stats = ", ".join(f"TRY_CAST(p.{c} AS DOUBLE)" for c in STAT_COLS)
    con.execute(f"""
        INSERT INTO super ({", ".join(IDENTITY_COLS)}, {", ".join(STAT_COLS)})
        SELECT p.player_week, p.NFL_player_id, p.player,
               TRY_CAST(p.year AS DOUBLE), TRY_CAST(p.week AS DOUBLE),
               p.season_type, p.nfl_team, p.opponent_nfl_team, p.data_source,
               {ins_stats}
        FROM pkg p
        WHERE p.upsert_state = 'insert_candidate'
          AND NOT EXISTS (SELECT 1 FROM super s WHERE s.player_week = p.player_week)""")

    rows_after, = con.execute("SELECT COUNT(*) FROM super").fetchone()
    if rows_after != rows_before + n_inserts:
        raise SystemExit(f"GUARD: row delta {rows_after - rows_before} != planned inserts {n_inserts}")

    # per-cell recount proof: every planned fill/insert cell now equals the package value
    bad, = con.execute(f"""
        SELECT COUNT(*) FROM pkg p JOIN super s USING (player_week)
        WHERE {" OR ".join(
            f"(p.{c} IS NOT NULL AND (s.{c} IS NULL OR ABS(TRY_CAST(s.{c} AS DOUBLE) - TRY_CAST(p.{c} AS DOUBLE)) > 1e-9))"
            for c in STAT_COLS)}""").fetchone()
    if bad:
        raise SystemExit(f"GUARD: {bad} package cells not reflected post-apply")

    print(f"\nwriting {out_parquet}")
    con.execute(f"COPY super TO '{out_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)")

    report = {
        "wave": f"newspaper_settlement_{stamp}",
        "input_release": subject, "input_sha256": EXPECTED_SUBJECT_SHA,
        "package": pkg_csv, "output_release": out_parquet,
        "rows_before": rows_before, "rows_after": rows_after,
        "cells_filled": n_fills, "fill_counts": fill_counts,
        "rows_inserted": n_inserts, "insert_collisions_skipped": n_ins_collide,
        "conflict_cells": 0,
        "note_position": "inserted rows carry NULL position pending the position backfill "
                         "lane (v23 precedent); run build_position_backfill_newspaper_v26 next",
        "next": ["build_rederive_composites (affected cols)",
                 "build_deterministic_recompute --apply",
                 "build_rescore_fpts_v26 --apply",
                 "recon_newspaper_sidecar (expect fill_signal/missing_row -> 0 for settled set)"],
        "cloud_write_performed": False,
    }
    (out_dir.parent / "APPLY_REPORT.json").write_text(json.dumps(report, indent=2))
    print(json.dumps({k: report[k] for k in ("cells_filled", "rows_inserted", "rows_after")}, indent=2))
    print(f"report  : {(out_dir.parent / 'APPLY_REPORT.json')}")


if __name__ == "__main__":
    main()
