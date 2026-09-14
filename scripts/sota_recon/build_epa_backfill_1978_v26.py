"""
sota_recon/build_epa_backfill_1978_v26.py  --  extend rushing / receiving / passing EPA to 1978.

WHAT WAS WRONG: rushing_epa, receiving_epa and passing_epa are populated only from 1999 (0 non-null
before), so a back like Emmitt Smith has EPA for 1999-2004 but NOTHING for his 1990-1998 prime, and
old QBs have no passing EPA at all. This was never a derivation gap: the merged PBP carries a
play-level `epa` for 1978-2025 (every rush AND pass play 1990-1998 has it), and the stored 1999+
value is a SUM(epa) per player-week. Same shape as the success-rate backfill (wave62) -- the ROLLUP
stopped at 1999, not the source. Passing and receiving are the SAME pass plays: if receiving_epa is
derivable, passing_epa is too, credited to the passer instead of the receiver.

    rushing_epa   = SUM(epa) over the rusher's rush plays     (verified 100.0% == stored on 2015)
    receiving_epa = SUM(epa) over the receiver's pass plays   (verified 100.0% == stored on 2015)
    passing_epa   = SUM(epa) over the passer's pass plays     (see NUANCE below)

NUANCE on passing_epa: the STORED 1999+ column was built from nflverse `qb_epa` (which folds in
sack/scramble attribution), not raw `epa`. Raw-epa reproduces the stored column at the median
exactly (median |delta| = 0.0) and within 0.05 on ~89% of games; the residual is a small tail
(~0.5 EPA/game mean, from qb_epa's sack attribution). The pre-1999 backfill is FILL-ONLY, so the
qb_epa-based 1999+ values are never touched; pre-1999 uses the raw play-epa passing sum -- a
legitimate, parallel-to-receiving measure. The seam is small and documented; qb_epa itself cannot
be reproduced pre-1999 (it is 0 in the PBP before 1999).

NOT INCLUDED, on purpose:
  - total_epa: = passing_epa + rushing_epa + receiving_epa. Its pre-1999 recompute is deferred to a
    follow-up (it must sum the three backfilled components); left at 1999 for now.
  - wpa / qb_epa / air_epa / yac_epa: 0 in the PBP before 1999. No source. Genuine 1999 floor.

1993 needs no special handling: its PBP `epa` is uniformly NULL (Stathead returned no exp_pts_diff,
the lone such season), so `epa IS NOT NULL` yields no 1993 rollup rows and the year stays NULL --
the honest value, consistent with [[project-1993-pbp-epa-gap]]. A gate asserts it stayed NULL.

ID SPACES: pre-1999 PBP ids are PFR-index (prefixed 'pfr:'); 1999+ are gsis (already NFL_player_id).
The super table carries a mix. Route through bio.pfr_id exactly as wave62 does -- direct-join the
bare id matches ~1.5% pre-1999, the bio crosswalk 100%.

FILL-ONLY: a row is written only where the target is currently NULL; 1999+ is never overwritten and
the gate proves it as a value-multiset (player_week is NOT unique -- doubleheaders).

    python -m scripts.sota_recon.build_epa_backfill_1978_v26            # dry-run
    python -m scripts.sota_recon.build_epa_backfill_1978_v26 --apply
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

PROV = "wave67.epa_backfill_1978"
PBP = "D:/league-history-data/nfl/raw/stathead/generated/pbp_merged_1978_2025/nfl_pbp_1978_2025_merged.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
FIRST_YEAR = 1978
CUTOVER = 1999  # 1999+ is already populated and must not move

# family -> (pbp player id column, play predicate, stored epa column)
FAMILIES = {
    "rush": ("rusher_player_id", "rush_attempt = 1", "rushing_epa"),
    "rec": ("receiver_player_id", "pass_attempt = 1", "receiving_epa"),
    "pass": ("passer_player_id", "pass_attempt = 1", "passing_epa"),
}


def _rollup_sql(alias: str, pid_col: str, attempt: str) -> str:
    return f"""
        WITH raw AS (
            SELECT regexp_replace(CAST({pid_col} AS VARCHAR), '^pfr:', '') AS src_id,
                   CAST(season AS INT) AS yr,
                   CAST(week AS INT) AS wk,
                   season_type AS st,
                   SUM(CAST(epa AS DOUBLE)) AS {alias}_e
            FROM '{PBP}'
            WHERE season BETWEEN {FIRST_YEAR} AND {CUTOVER - 1}
              AND {attempt}
              AND {pid_col} IS NOT NULL
              AND epa IS NOT NULL
            GROUP BY 1, 2, 3, 4
        ), xw AS (
            SELECT DISTINCT CAST(pfr_id AS VARCHAR) AS pfr_id,
                            CAST(NFL_player_id AS VARCHAR) AS nfl_id
            FROM '{BIO}' WHERE pfr_id IS NOT NULL
        )
        SELECT COALESCE(xw.nfl_id, raw.src_id) AS pid,
               raw.yr, raw.wk, raw.st, raw.{alias}_e
        FROM raw LEFT JOIN xw ON raw.src_id = xw.pfr_id
    """


def _stage(con: duckdb.DuckDBPyConnection) -> None:
    for alias, (pid_col, attempt, _e) in FAMILIES.items():
        con.execute(f"CREATE OR REPLACE TEMP TABLE roll_{alias} AS {_rollup_sql(alias, pid_col, attempt)}")


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("PRAGMA disable_progress_bar")

    _stage(con)
    staged = {a: con.execute(f"SELECT COUNT(*) FROM roll_{a}").fetchone()[0] for a in FAMILIES}

    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    pre_filled = {
        e: con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE year < {CUTOVER} AND {e} IS NOT NULL").fetchone()[0]
        for _a, (_pid, _at, e) in FAMILIES.items()
    }
    if any(pre_filled.values()):
        con.close()
        raise SystemExit(f"ABORT: pre-{CUTOVER} EPA already present -- refusing to overwrite: {pre_filled}")

    join = """
        LEFT JOIN roll_{a} r_{a}
          ON CAST(w.NFL_player_id AS VARCHAR) = r_{a}.pid
         AND CAST(w.year AS INT) = r_{a}.yr
         AND CAST(w.week AS INT) = r_{a}.wk
         AND w.season_type = r_{a}.st
    """
    joins = "".join(join.format(a=a) for a in FAMILIES)
    would = con.execute(
        f"""SELECT COUNT(*) FROM '{vq}' w {joins}
            WHERE w.year BETWEEN {FIRST_YEAR} AND {CUTOVER - 1}
              AND (r_rush.pid IS NOT NULL OR r_rec.pid IS NOT NULL OR r_pass.pid IS NOT NULL)"""
    ).fetchone()[0]

    if not apply:
        by_year = con.execute(
            f"""SELECT CAST(w.year AS INT) AS y, COUNT(*) AS n FROM '{vq}' w {joins}
                WHERE w.year BETWEEN {FIRST_YEAR} AND {CUTOVER - 1}
                  AND (r_rush.pid IS NOT NULL OR r_rec.pid IS NOT NULL OR r_pass.pid IS NOT NULL)
                GROUP BY 1 ORDER BY 1"""
        ).fetchall()
        con.close()
        return {
            "rows": before_rows,
            "staged_pbp_rollup_rows": staged,
            "would_fill_player_weeks": would,
            "by_year": by_year,
            "backfilled": [e for _a, (_p, _at, e) in FAMILIES.items()],
            "not_backfilled": ["total_epa (deferred)", "qb_epa / *_wpa / air_epa (no pre-1999 source)"],
        }

    stamp = utc_stamp()
    spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(spill, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{spill}'")

    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    repl = []
    for a, (_pid, _at, ecol) in FAMILIES.items():
        repl.append(f"COALESCE(w.{ecol}, r_{a}.{a}_e) AS {ecol}")
    if "recon_correction_log" in cols:
        repl.append(
            "CASE WHEN w.recon_correction_log IS NULL OR w.recon_correction_log='' "
            f"THEN '{PROV}' ELSE w.recon_correction_log||',{PROV}' END AS recon_correction_log"
        )
    out_sql = f"SELECT w.* REPLACE ({', '.join(repl)}) FROM '{vq}' w {joins}"

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_epa78.parquet")
    reader = con.execute(out_sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), reader.schema)
    for batch in reader:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()

    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    problems = []
    if after_rows != before_rows:
        problems.append(f"row count moved {before_rows} -> {after_rows}")

    # 1999+ must be byte-identical -- value-multiset compare (player_week is NOT unique: doubleheaders)
    moved_modern = 0
    for _a, (_p, _at, ecol) in FAMILIES.items():
        moved_modern += int(con.execute(f"""
            SELECT COALESCE(SUM(ABS(d)), 0) FROM (
                SELECT COALESCE(x.c,0) - COALESCE(y.c,0) AS d
                FROM (SELECT {ecol} AS v, COUNT(*) c FROM '{vq}' WHERE year >= {CUTOVER} GROUP BY 1) x
                FULL OUTER JOIN
                     (SELECT {ecol} AS v, COUNT(*) c FROM '{tq}' WHERE year >= {CUTOVER} GROUP BY 1) y
                  ON x.v IS NOT DISTINCT FROM y.v
            ) WHERE d <> 0""").fetchone()[0])
    if moved_modern:
        problems.append(f"{moved_modern} rows at/after {CUTOVER} changed (must be 0)")

    # 1993 has no epa source -> must stay NULL
    for _a, (_p, _at, ecol) in FAMILIES.items():
        leaked = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE year=1993 AND {ecol} IS NOT NULL").fetchone()[0]
        if leaked:
            problems.append(f"{leaked} rows filled {ecol} for 1993 (no epa source; must stay NULL)")

    filled = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE year < {CUTOVER} AND rushing_epa IS NOT NULL"
    ).fetchone()[0]

    if problems:
        tmp.unlink(missing_ok=True)
        con.close()
        raise SystemExit("ABORT (no swap): " + "; ".join(problems))

    backup = vp.with_name(vp.stem + f"_prew67_{stamp}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    con.close()
    return {
        "rows": after_rows,
        "filled_player_weeks": would,
        "pre_cutover_rows_with_rushing_epa": filled,
        "rows_changed_at_or_after_cutover": moved_modern,
        "backup": str(backup),
        "provenance": PROV,
        "next": "update COVERAGE_FROM rushing_epa/receiving_epa to 1978; re-promote",
    }


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="write the backfilled EPA columns")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
