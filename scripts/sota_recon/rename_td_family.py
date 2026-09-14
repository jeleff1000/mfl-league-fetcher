"""TD-FAMILY RENAME (Joe 2026-08-04: "lets fix the naming").

    total_tds_scored  -> total_tds_scored          (ALL touchdowns the player scored)
    total_tds_accounted_for -> total_tds_accounted_for   (scored scrimmage + thrown)
    scrimmage_tds                          (unchanged, already exact)

TWO HAZARDS, both handled:
  1. `total_tds_scored` is a SUBSTRING of `total_tds_accounted_for` -- a naive replace corrupts
     both. Every rename is word-boundary anchored and the LONGER name is
     replaced first.
  2. Column ORDER in the plane is load-bearing (positional verification).
     The rewrite builds the full explicit column list in original order
     with only the two names swapped -- never SELECT * EXCLUDE + append.

Run with --plane to rewrite the plane, --repo to sweep source files,
--verify to check. Default prints the plan.
"""
from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
ROOT = Path(__file__).resolve().parents[2]
# APPLIED 2026-08-04. Kept as history; the tool self-swept its own table
# into identity pairs on the live run -- SELF-EXCLUSION added below.
RENAMES = [("total_tds", "total_tds_accounted_for"),   # longest first
           ("total_td", "total_tds_scored")]
RECEIPT = LAKE / "td_rename_receipt.json"


def rename_plane(con: duckdb.DuckDBPyConnection) -> dict:
    src = Path(S.latest_v26())
    out = src.with_name(src.stem + ".renamed" + src.suffix)
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{src.as_posix()}') LIMIT 0"
    ).fetchall()]
    mapping = dict(RENAMES)
    missing = [o for o, _ in RENAMES if o not in cols]
    assert not missing, f"columns absent from plane: {missing}"
    collide = [n for _, n in RENAMES if n in cols]
    assert not collide, f"new names already exist: {collide}"
    proj = ", ".join(
        f'"{c}" AS "{mapping[c]}"' if c in mapping else f'"{c}"'
        for c in cols)          # ORDER PRESERVED exactly
    con.execute(f"""COPY (SELECT {proj}
        FROM read_parquet('{src.as_posix()}'))
        TO '{out.as_posix()}' (FORMAT parquet)""")
    n_in = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{src.as_posix()}')").fetchone()[0]
    n_out = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')").fetchone()[0]
    assert n_in == n_out, f"rows {n_in} -> {n_out}: refuse"
    new_cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{out.as_posix()}') LIMIT 0"
    ).fetchall()]
    assert len(new_cols) == len(cols), "column count changed: refuse"
    for i, c in enumerate(cols):          # positional check
        assert new_cols[i] == mapping.get(c, c), (
            f"order broke at {i}: {new_cols[i]} != {mapping.get(c, c)}")
    bak = src.with_name(src.stem + ".pre_rename" + src.suffix)
    if not bak.exists():
        shutil.copy2(src, bak)
    shutil.move(str(out), str(src))
    return {"rows": n_in, "columns": len(cols), "backup": str(bak)}


SKIP_DIRS = {".git", "node_modules", ".next", "__pycache__", "weekly_repaired_parts"}
# a sweep must never rewrite (a) itself, or (b) the historical record --
# ledgers/adjudications quote the OLD names as evidence, and renaming them
# there destroys the provenance the receipts exist to carry.
SKIP_FILES = {"rename_td_family.py"}
SKIP_PATH_PARTS = {"era-rulings-ledger-2026-08-03.md",
                   "disagreement_adjudications.v1.json",
                   "supertable-collision-audit-plan-2026-08-01.md"}
EXTS = {".py", ".ts", ".tsx", ".json", ".sql", ".md"}


def rename_repo() -> dict:
    touched = {}
    for p in ROOT.rglob("*"):
        if not p.is_file() or p.suffix not in EXTS:
            continue
        if any(part in SKIP_DIRS for part in p.parts):
            continue
        if p.name in SKIP_FILES or p.name in SKIP_PATH_PARTS:
            continue
        try:
            t = p.read_text(encoding="utf-8")
        except (UnicodeDecodeError, PermissionError, OSError):
            continue
        if "total_tds_scored" not in t:
            continue
        orig, n = t, 0
        for old, new in RENAMES:            # longest first, word-anchored
            t, k = re.subn(rf"\b{old}\b", new, t)
            n += k
        if t != orig:
            p.write_text(t, encoding="utf-8")
            touched[str(p.relative_to(ROOT))] = n
    return touched


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--plane", action="store_true")
    ap.add_argument("--repo", action="store_true")
    a = ap.parse_args()
    rec = {"date": time.strftime("%Y-%m-%d %H:%M"), "renames": RENAMES}
    if a.plane:
        con = duckdb.connect()
        con.execute("SET memory_limit='1500MB'")
        con.execute("SET threads=2")
        con.execute("SET preserve_insertion_order=false")
        con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
        rec["plane"] = rename_plane(con)
        print("PLANE RENAMED:", json.dumps(rec["plane"]))
    if a.repo:
        rec["repo_files"] = rename_repo()
        print(f"REPO: {len(rec['repo_files'])} files, "
              f"{sum(rec['repo_files'].values())} references")
    if not (a.plane or a.repo):
        print("plan only -- pass --plane and/or --repo")
    if a.plane or a.repo:
        RECEIPT.write_text(json.dumps(rec, indent=1), encoding="utf-8")
