"""
sota_recon/build_success_evidenceless_null_v26.py  --  retract success counts for seasons
that never had the evidence to support them.

WHAT WAS WRONG: wave62 backfilled pass/rush/rec success counts to 1978 from the merged PBP's
play-level `success` column, gated on `success IS NOT NULL`. That gate cannot see a season
whose success arrived pre-filled with a default zero.

The twin parser (parse_stathead_pbp_to_nflverse_twin.py) scrapes epa from Stathead's
`exp_pts_diff` and derives success as epa > 0, but `success` also sits in that parser's
zero-default field list -- so a play with no scraped exp_pts_diff kept success = 0.0 rather
than NULL. Stathead returned no exp_pts_diff at all for 1993: 39,029 plays, 0 with epa, and
success = 0.0 uniformly. It is the ONLY such season in 1978-2025 (epa otherwise runs unbroken
from 1978 -- 1993 is a lone hole, not an era floor).

So the release now carries, for 1993, 13,743 rush plays / 0 successes, 15,305 rec plays / 0
successes, 15,869 pass plays / 0 successes. Neighbouring seasons run ~0.37 rush and ~0.47 rec.
A rate of exactly 0.000 for a full modern season is not a plausible measurement -- it is the
absence of one wearing a measurement's clothes, and it moves every era leaderboard it touches.

WHY NULL AND NOT RE-DERIVE: 1993's model-free inputs are intact (yards_gained 39,029/39,029,
ydstogo 39,029/39,029, down 34,878 -- the same ratio as 1994's control), so a yards-vs-distance
success COULD be computed. It must not be. Every other season defines success as epa > 0;
a model-free 1993 would be a silent definitional seam inside a column that is compared across
eras. Recovering 1993 properly means re-scraping Stathead for exp_pts_diff -- an ingestion
job, tracked separately. Until then NULL is the honest value, and it is what `epa` itself
already carries for that season.

FIX-AT-SOURCE, three layers:
  1. parse_stathead_pbp_to_nflverse_twin.py  -- success is NULL where epa is NULL (regeneration)
  2. build_success_rate_backfill_1978_v26.evidenceless_seasons() -- the rollup skips such
     seasons, with a gate proving they stay NULL (protects the artifact as it stands)
  3. this wave -- retracts what layer 2 was not yet in place to prevent

The season list is DERIVED, not hardcoded: it calls evidenceless_seasons() against the same
PBP. If Stathead is re-scraped and 1993 gains epa, this wave becomes a no-op on its own.

    python -m scripts.sota_recon.build_success_evidenceless_null_v26            # dry-run
    python -m scripts.sota_recon.build_success_evidenceless_null_v26 --apply
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
from .build_success_rate_backfill_1978_v26 import FAMILIES, evidenceless_seasons

PROV = "wave64.success_evidenceless_null"

# the six columns wave62 wrote; both numerator and denominator go, since a denominator
# with no knowable numerator is not a usable measurement either
TARGET_COLS = [c for _a, (_pid, _at, s, p) in FAMILIES.items() for c in (s, p)]


def _multiset_delta(con: duckdb.DuckDBPyConnection, col: str, a: str, b: str, where: str) -> int:
    """Absolute count delta of `col` values between two parquets, restricted by `where`.

    Deliberately NOT a player_week join: player_week is not unique (219 values cover 442
    rows -- the 1920s-30s doubleheaders this codebase preserves on purpose), so joining on
    it fans rows out and compares mismatched pairs. It aborted a correct wave61 run with a
    phantom "nfl_position changed on 2 rows". A value-multiset comparison is order-independent
    and duplicate-safe.
    """
    return con.execute(f"""
        SELECT COALESCE(SUM(ABS(d)), 0) FROM (
            SELECT COALESCE(x.c, 0) - COALESCE(y.c, 0) AS d
            FROM (SELECT {col} AS v, COUNT(*) c FROM '{a}' WHERE {where} GROUP BY 1) x
            FULL OUTER JOIN
                 (SELECT {col} AS v, COUNT(*) c FROM '{b}' WHERE {where} GROUP BY 1) y
              ON x.v IS NOT DISTINCT FROM y.v
        ) WHERE d <> 0
    """).fetchone()[0]


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("PRAGMA disable_progress_bar")

    seasons = evidenceless_seasons(con)
    if not seasons:
        con.close()
        return {"seasons": [], "note": "no evidenceless seasons in the PBP -- nothing to retract"}

    in_list = ", ".join(str(s) for s in seasons)
    scope = f"CAST(year AS INT) IN ({in_list})"
    not_null = " OR ".join(f"{c} IS NOT NULL" for c in TARGET_COLS)

    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    would = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {scope} AND ({not_null})").fetchone()[0]

    # a season is only safe to retract if it is genuinely all-zero; a season with any real
    # success would be a different problem and must not be silently blanked
    nonzero = con.execute(
        f"SELECT COUNT(*) FROM '{vq}' WHERE {scope} AND ("
        + " OR ".join(f"{s} > 0" for _a, (_pid, _at, s, _p) in FAMILIES.items())
        + ")"
    ).fetchone()[0]
    if nonzero:
        con.close()
        raise SystemExit(
            f"ABORT: {nonzero} rows in seasons {seasons} carry a POSITIVE success count. "
            "That is not the evidenceless pattern this wave retracts -- investigate before rerunning."
        )

    if not apply:
        by_year = con.execute(
            f"""SELECT CAST(year AS INT) y, COUNT(*) n FROM '{vq}'
                WHERE {scope} AND ({not_null}) GROUP BY 1 ORDER BY 1"""
        ).fetchall()
        con.close()
        return {
            "rows": before_rows,
            "evidenceless_seasons": seasons,
            "would_null_rows": would,
            "by_year": by_year,
            "columns": TARGET_COLS,
        }

    stamp = utc_stamp()
    spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(spill, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{spill}'")

    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    repl = [f"CASE WHEN {scope} THEN NULL ELSE w.{c} END AS {c}" for c in TARGET_COLS]
    if "recon_correction_log" in cols:
        repl.append(
            f"CASE WHEN {scope} THEN ("
            "  CASE WHEN w.recon_correction_log IS NULL OR w.recon_correction_log='' "
            f"  THEN '{PROV}' ELSE w.recon_correction_log||',{PROV}' END"
            ") ELSE w.recon_correction_log END AS recon_correction_log"
        )
    out_sql = f"SELECT w.* REPLACE ({', '.join(repl)}) FROM '{vq}' w"

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_evnull.parquet")
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

    # the target seasons must now be fully NULL on all six columns
    residual = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {scope} AND ({not_null})").fetchone()[0]
    if residual:
        problems.append(f"{residual} rows still non-NULL in seasons {seasons}")

    # nothing outside the target seasons may move, on any of the six columns
    outside = f"CAST(year AS INT) NOT IN ({in_list})"
    for c in TARGET_COLS:
        moved = _multiset_delta(con, c, vq, tq, outside)
        if moved:
            problems.append(f"{moved} rows outside {seasons} changed on {c} (must be 0)")

    if problems:
        tmp.unlink(missing_ok=True)
        con.close()
        raise SystemExit("ABORT (no swap): " + "; ".join(problems))

    backup = vp.with_name(vp.stem + f"_prew64_{stamp}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    con.close()
    return {
        "rows": after_rows,
        "evidenceless_seasons": seasons,
        "nulled_rows": would,
        "columns": TARGET_COLS,
        "backup": str(backup),
        "provenance": PROV,
        "next": "rebuild season/career (build_season_career_v26)",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the retraction")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
