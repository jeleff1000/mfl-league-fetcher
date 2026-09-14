"""
sota_recon/build_legacy_vocab_drop_v26.py  --  drop seven legacy-vocabulary columns that carry
only a handful of contaminated rows, each value already carried canonically.

WHAT THESE ARE (docs/player-column-coverage/supertable-issues.md section 4): fgm, pts, xp%, fg%,
2pm, sfty, rate -- legacy_motherduck-vocabulary columns that hold at most 5 rows out of
1,219,756, every one Tommy Davis / SFO / 1961-1968. Verified row-by-row against the canonical
schema:

    fgm  == fg_made            pts  == pts_k_std        xp%  == pat_pct (0-100 vs 0-1)
    fg%  == fg_pct (0-100)     2pm  -- all zero         sfty == def_safeties (all zero)
    rate == passer_rating (string-typed with empty-string values)

So this is ZERO data loss: nothing here exists only in these columns. Dropping makes the absence
explicit instead of carrying seven permanent-width legacy names for five contaminated rows.

WHY A DROP AND NOT A ROW-FIX: the five rows themselves are legitimate Tommy Davis games and are
already present under the canonical schema; only the legacy COLUMNS are redundant.

SCOPE: weekly super table only -- these columns do not exist at season/career grain (verified).
The values are carried canonically, so no rank/aggregate/score reads them (they are not in the
season/career aggregate spec, DEF_COL_MAP, IDP_COL_MAP, or any scoring map). Downstream:
  - build_census_cleanup_v26.COERCE drops these names in the same change
  - research-live-schema-index.json regenerates from live Fly post-promote (auto-excludes)
  - research-schema.ts references are search ALIASES ("fg%" -> fg_pct), not column reads -- unchanged
  - the promote passes these seven to --allow-drop; the ___ops views regenerate from the live
    schema and auto-exclude them

    python -m scripts.sota_recon.build_legacy_vocab_drop_v26            # dry-run
    python -m scripts.sota_recon.build_legacy_vocab_drop_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave65.legacy_vocab_drop"
DROP_COLS = ["fgm", "pts", "xp%", "fg%", "2pm", "sfty", "rate"]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("PRAGMA disable_progress_bar")

    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    present = [c for c in DROP_COLS if c in cols]
    missing = [c for c in DROP_COLS if c not in cols]
    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]

    if not present:
        con.close()
        return {"note": "none of the legacy columns are present -- already dropped", "missing": missing}

    # a value carried only in a legacy column would be real data loss; prove each is redundant
    # (0 rows where the legacy column is populated but its canonical twin is not)
    canonical = {"fgm": "fg_made", "pts": "pts_k_std", "fg%": "fg_pct", "xp%": "pat_pct"}
    orphans = {}
    for legacy, canon in canonical.items():
        if legacy in present and canon in cols:
            n = con.execute(
                f"SELECT COUNT(*) FROM '{vq}' WHERE {_q(legacy)} IS NOT NULL AND {_q(canon)} IS NULL"
            ).fetchone()[0]
            orphans[legacy] = n
    leaking = {k: v for k, v in orphans.items() if v}
    if leaking:
        con.close()
        raise SystemExit(f"ABORT: legacy columns hold values absent from their canonical twin: {leaking}")

    nonnull = {c: con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {_q(c)} IS NOT NULL").fetchone()[0]
               for c in present}

    if not apply:
        con.close()
        return {
            "rows": before_rows,
            "columns_before": len(cols),
            "would_drop": present,
            "already_absent": missing,
            "nonnull_rows_per_col": nonnull,
            "canonical_orphan_check": orphans,
        }

    stamp = utc_stamp()
    spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(spill, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{spill}'")

    excl = ", ".join(_q(c) for c in present)
    out_sql = f"SELECT * EXCLUDE ({excl}) FROM '{vq}'"

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_legacydrop.parquet")
    reader = con.execute(out_sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), reader.schema)
    for batch in reader:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()

    problems = []
    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    if after_rows != before_rows:
        problems.append(f"row count moved {before_rows} -> {after_rows}")

    new_cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{tq}'").fetchall()]
    expected = [c for c in cols if c not in present]
    if new_cols != expected:
        problems.append("remaining column set/order changed beyond the intended drop")
    still = [c for c in present if c in new_cols]
    if still:
        problems.append(f"columns not actually dropped: {still}")

    # spot-check a few surviving columns are byte-for-byte unchanged (EXCLUDE is a pure
    # projection, but a value-multiset check is cheap insurance against a wrong tmp file)
    for probe in ("fg_made", "pts_k_std", "NFL_player_id", "passing_yards"):
        if probe in expected:
            moved = con.execute(f"""
                SELECT COALESCE(SUM(ABS(d)),0) FROM (
                  SELECT COALESCE(x.c,0)-COALESCE(y.c,0) d FROM
                    (SELECT {_q(probe)} v, COUNT(*) c FROM '{vq}' GROUP BY 1) x
                  FULL OUTER JOIN
                    (SELECT {_q(probe)} v, COUNT(*) c FROM '{tq}' GROUP BY 1) y
                    ON x.v IS NOT DISTINCT FROM y.v) WHERE d <> 0
            """).fetchone()[0]
            if moved:
                problems.append(f"surviving column {probe} changed ({moved} value-count delta)")

    if problems:
        tmp.unlink(missing_ok=True)
        con.close()
        raise SystemExit("ABORT (no swap): " + "; ".join(problems))

    backup = vp.with_name(vp.stem + f"_prew65_{stamp}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    con.close()
    return {
        "rows": after_rows,
        "dropped": present,
        "columns_before": len(cols),
        "columns_after": len(new_cols),
        "backup": str(backup),
        "provenance": PROV,
        "next": "promote with --allow-drop=" + ",".join(present),
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the dropped-column release")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
