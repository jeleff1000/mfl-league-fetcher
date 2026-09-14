"""
sota_recon/build_success_rate_backfill_1978_v26.py  --  extend success-rate counts back to 1978.

WHAT WAS WRONG: pass/rush/rec success counts were treated as EPA-sign flags. The canonical atom is
instead a count of successful play rows under the official down/distance rule: 40% of yards-to-go on
first down, 60% on second, and 100% on third/fourth. The rate is separately derived as count/plays.
The old counts start in 1999 and are completely empty before it --
0 non-null across all 601,842 pre-1999 rows. This was never a derivation gap. The merged PBP carries
a fully populated play-level `success` column for 1978-2025 (2,092,435 of 2,107,143 plays; 40,006/40,006
in 1978). The ROLLUP simply stopped at 1999. Same shape as the DST backfill: the atom was always there.

DEFINITION -- reverse-engineered from the existing 1999+ data, not invented. Reconciles EXACTLY:

    filter: season_type matched, <family>_attempt = 1, two_point_attempt = 0,
            <family>_player_id IS NOT NULL, success IS NOT NULL
    <family>_success       = COUNT(*) WHERE success = 1
    <family>_success_plays = COUNT(*)

    reconciliation vs the shipped 1999+ values (REG):
        year   pass            rush            rec
        2010   100.00/100.00   100.00/100.00   100.00/100.00
        2005   100.00/100.00   100.00/100.00   100.00/100.00
        1999   100.00/100.00   100.00/100.00   100.00/100.00

The `two_point_attempt = 0` exclusion is load-bearing and was NOT obvious: without it pass reconciles at
only 97.0/94.6. Two-point plays carry a `success` value in the PBP but are excluded from the shipped
success denominators. Do not drop that predicate.

WEIGHTED AVERAGE AT SEASON/CAREER IS AUTOMATIC. These columns store COUNTS (numerator + denominator),
never a rate, so a season/career rate is SUM(success)/SUM(plays) -- inherently weighted by play volume,
with no mean-of-means error possible. Verified: player_nfl_season.rush_success is an exact sum of the
weekly REG rows (311/311 = 100.0% on 2015). So this weekly backfill propagates correctly to season and
career the moment those aggregates are rebuilt -- but they MUST be rebuilt (build_season_career_v26)
or season/career will stay 1999+ while weekly reaches 1978.

SCOPE: pass / rush / rec, REG and POST (1999+ POST is populated, 18,979 rows, so POST is in scope).
NOT INCLUDED: def_success_allowed / def_success_plays. Those live on team DEF rows keyed by defteam
rather than by player id -- a different join that this wave does not validate, so it does not touch them.

FILL-ONLY: a row is written only where the target is currently NULL. 1999+ values are never overwritten,
and the gate proves it byte-for-byte. An unmatched player-week stays NULL rather than becoming 0 --
"no PBP evidence" and "zero successes" are different claims and must not be conflated.

That last rule needed enforcing at the SOURCE too, not just at the join. The twin parser
defaulted `success` to 0.0 and only overwrote it where scraped epa existed, so a season with
no exp_pts_diff at all arrived here as 0.0 on every play and sailed through `success IS NOT
NULL`. 1993 did exactly that (39,029 plays, 0 with epa) and shipped 13,743 rush plays / 0
successes into the release. `evidenceless_seasons()` now excludes any such season and a gate
proves those years stay NULL. The parser fix is in parse_stathead_pbp_to_nflverse_twin.py;
this guard covers the artifact until the twin is regenerated.

    python -m scripts.sota_recon.build_success_rate_backfill_1978_v26            # dry-run
    python -m scripts.sota_recon.build_success_rate_backfill_1978_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import latest_v26

PROV = "wave62.success_rate_1978"
PBP = "D:/league-history-data/nfl/raw/stathead/generated/pbp_merged_1978_2025/nfl_pbp_1978_2025_merged.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
FIRST_YEAR = 1978
CUTOVER = 1999  # 1999+ is already populated and must not move

# family -> (pbp player id column, attempt predicate, success column, plays column)
FAMILIES = {
    "pass": ("passer_player_id", "pass_attempt = 1", "pass_success", "pass_success_plays"),
    "rush": ("rusher_player_id", "rush_attempt = 1", "rush_success", "rush_success_plays"),
    "rec": ("receiver_player_id", "pass_attempt = 1", "rec_success", "rec_success_plays"),
}


def evidenceless_seasons(con: duckdb.DuckDBPyConnection) -> list[int]:
    """Seasons whose PBP carries `success` but never once positive -- i.e. no evidence.

    `success` is defined as epa > 0 and the twin parser only knows it where the scraped
    exp_pts_diff exists. It shares that parser's zero-default field list, so a season for
    which Stathead returned no exp_pts_diff at all lands as success = 0.0 on EVERY play
    rather than NULL. That is indistinguishable, to a `success IS NOT NULL` filter, from a
    season in which nobody succeeded.

    Measured on the current merged artifact: 1993 is the only such season in 1978-2025
    (39,029 plays, 0 with epa, success = 0.0 uniformly; neighbours run ~0.37 rush / ~0.47
    rec). Rolling it up yields 13,743 rush plays and 0 successes -- a false zero that reads
    as a real one and moves every era leaderboard it touches.

    The parser now emits NULL for absent epa, but that only takes effect when the twin is
    regenerated. This guard makes the rollup safe against the artifact as it stands, and
    stays correct afterwards (a fully-NULL season has no rows to find).
    """
    rows = con.execute(
        f"""SELECT CAST(season AS INT) FROM '{PBP}'
            WHERE season BETWEEN {FIRST_YEAR} AND {CUTOVER - 1}
            GROUP BY 1
            HAVING COUNT(*) FILTER (WHERE down IS NOT NULL AND ydstogo IS NOT NULL
                                    AND yards_gained IS NOT NULL) = 0
            ORDER BY 1"""
    ).fetchall()
    return [r[0] for r in rows]


def _rollup_sql(alias: str, pid_col: str, attempt: str, skip_seasons: list[int]) -> str:
    """Roll up PBP success per player-week, resolving the id into NFL_player_id space.

    THE ID SPACES DIVERGE AT THE CUTOVER and a naive join silently returns zero rows:

        1999+      PBP '00-0026899'      gsis -- already NFL_player_id space
        pre-1999   PBP 'pfr:BaabMi20'    PFR index, prefixed

    while the super table carries a MIX (bare PFR 'WashJo00', gsis '00-0017415', and others).
    Measured on the 43,340 pre-1999 rusher-weeks: joining the bare id direct matches 1.5%;
    routing through bio.pfr_id matches 100.0%, of which 99.8% land on a super-table row.
    So the crosswalk is mandatory, not an optimisation.

    Strip the 'pfr:' prefix, LEFT JOIN bio.pfr_id, and COALESCE back to the raw id -- that
    handles both eras in one expression (1999+ misses the bio join and falls through to gsis).
    """
    return f"""
        WITH raw AS (
            SELECT regexp_replace(CAST({pid_col} AS VARCHAR), '^pfr:', '') AS src_id,
                   CAST(season AS INT) AS yr,
                   CAST(week AS INT) AS wk,
                   season_type AS st,
                   SUM(CASE WHEN down IS NOT NULL AND ydstogo IS NOT NULL
                                  AND yards_gained IS NOT NULL AND (
                         (CAST(down AS INTEGER) = 1 AND CAST(yards_gained AS DOUBLE) >= 0.4 * CAST(ydstogo AS DOUBLE))
                      OR (CAST(down AS INTEGER) = 2 AND CAST(yards_gained AS DOUBLE) >= 0.6 * CAST(ydstogo AS DOUBLE))
                      OR (CAST(down AS INTEGER) IN (3,4) AND CAST(yards_gained AS DOUBLE) >= CAST(ydstogo AS DOUBLE))
                               ) THEN 1 ELSE 0 END) AS {alias}_s,
                   COUNT(*) AS {alias}_p
            FROM '{PBP}'
            WHERE season BETWEEN {FIRST_YEAR} AND {CUTOVER - 1}
              AND {attempt}
              AND COALESCE(two_point_attempt, 0) = 0
              AND {pid_col} IS NOT NULL
              AND down IS NOT NULL AND ydstogo IS NOT NULL AND yards_gained IS NOT NULL
              {"AND season NOT IN (" + ", ".join(str(s) for s in skip_seasons) + ")" if skip_seasons else ""}
            GROUP BY 1, 2, 3, 4
        ), xw AS (
            SELECT DISTINCT CAST(pfr_id AS VARCHAR) AS pfr_id,
                            CAST(NFL_player_id AS VARCHAR) AS nfl_id
            FROM '{BIO}' WHERE pfr_id IS NOT NULL
        )
        SELECT COALESCE(xw.nfl_id, raw.src_id) AS pid,
               raw.yr, raw.wk, raw.st, raw.{alias}_s, raw.{alias}_p
        FROM raw LEFT JOIN xw ON raw.src_id = xw.pfr_id
    """


def _stage(con: duckdb.DuckDBPyConnection, skip_seasons: list[int]) -> None:
    for alias, (pid_col, attempt, _s, _p) in FAMILIES.items():
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE roll_{alias} AS "
            f"{_rollup_sql(alias, pid_col, attempt, skip_seasons)}"
        )


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute("PRAGMA disable_progress_bar")

    skipped = evidenceless_seasons(con)
    _stage(con, skipped)
    staged = {a: con.execute(f"SELECT COUNT(*) FROM roll_{a}").fetchone()[0] for a in FAMILIES}

    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    pre_filled = {
        s: con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE year < {CUTOVER} AND {s} IS NOT NULL").fetchone()[0]
        for _a, (_pid, _at, s, _p) in FAMILIES.items()
    }
    if any(pre_filled.values()):
        con.close()
        raise SystemExit(f"ABORT: pre-{CUTOVER} success values already present -- refusing to overwrite: {pre_filled}")

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
              AND (r_pass.pid IS NOT NULL OR r_rush.pid IS NOT NULL OR r_rec.pid IS NOT NULL)"""
    ).fetchone()[0]

    if not apply:
        by_year = con.execute(
            f"""SELECT CAST(w.year AS INT) AS y, COUNT(*) AS n FROM '{vq}' w {joins}
                WHERE w.year BETWEEN {FIRST_YEAR} AND {CUTOVER - 1}
                  AND (r_pass.pid IS NOT NULL OR r_rush.pid IS NOT NULL OR r_rec.pid IS NOT NULL)
                GROUP BY 1 ORDER BY 1"""
        ).fetchall()
        con.close()
        return {
            "rows": before_rows,
            "staged_pbp_rollup_rows": staged,
            "would_fill_player_weeks": would,
            "by_year": by_year,
            "evidenceless_seasons_skipped": skipped,
        }

    stamp = utc_stamp()
    spill = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(spill, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{spill}'")

    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    repl = []
    for a, (_pid, _at, scol, pcol) in FAMILIES.items():
        # fill-only: never touch a row that already has a value (i.e. never touch 1999+)
        repl.append(f"COALESCE(w.{scol}, r_{a}.{a}_s) AS {scol}")
        repl.append(f"COALESCE(w.{pcol}, r_{a}.{a}_p) AS {pcol}")
    if "recon_correction_log" in cols:
        repl.append(
            "CASE WHEN w.recon_correction_log IS NULL OR w.recon_correction_log='' "
            f"THEN '{PROV}' ELSE w.recon_correction_log||',{PROV}' END AS recon_correction_log"
        )
    out_sql = f"SELECT w.* REPLACE ({', '.join(repl)}) FROM '{vq}' w {joins}"

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_success78.parquet")
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

    # 1999+ must be byte-identical across every success column.
    #
    # Compared as a value MULTISET per column, never as a join on player_week: that key is
    # NOT unique. 219 player_week values cover 442 rows -- the 1920s-30s doubleheaders (two
    # games, distinct opponents, one week) this codebase preserves on purpose. Joining on it
    # fans those rows out and compares mismatched pairs, which produces phantom diffs and
    # aborts runs whose transform is correct (it did exactly that to wave61).
    moved_modern = 0
    for _a, (_p, _at, s, p) in FAMILIES.items():
        for col in (s, p):
            moved_modern += int(
                con.execute(f"""
                    SELECT COALESCE(SUM(ABS(d)), 0) FROM (
                        SELECT COALESCE(x.v, y.v) AS v, COALESCE(x.c,0) - COALESCE(y.c,0) AS d
                        FROM (SELECT {col} AS v, COUNT(*) c FROM '{vq}'
                              WHERE year >= {CUTOVER} GROUP BY 1) x
                        FULL OUTER JOIN
                             (SELECT {col} AS v, COUNT(*) c FROM '{tq}'
                              WHERE year >= {CUTOVER} GROUP BY 1) y
                          ON x.v IS NOT DISTINCT FROM y.v
                    ) WHERE d <> 0
                """).fetchone()[0]
            )
    if moved_modern:
        problems.append(f"{moved_modern} rows at/after {CUTOVER} changed (must be 0)")

    filled = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE year < {CUTOVER} AND rush_success IS NOT NULL"
    ).fetchone()[0]
    # successes can never exceed plays, in any era
    impossible = con.execute(
        f"""SELECT COUNT(*) FROM '{tq}' WHERE """
        + " OR ".join(f"({s} > {p})" for _a, (_pid, _at, s, p) in FAMILIES.items())
    ).fetchone()[0]
    if impossible:
        problems.append(f"{impossible} rows have success > plays")

    # an evidenceless season must stay NULL, never be filled with a zero that reads as real
    for season in skipped:
        leaked = con.execute(
            f"""SELECT COUNT(*) FROM '{tq}' WHERE year = {season} AND ("""
            + " OR ".join(f"{s} IS NOT NULL OR {p} IS NOT NULL" for _a, (_pid, _at, s, p) in FAMILIES.items())
            + ")"
        ).fetchone()[0]
        if leaked:
            problems.append(f"{leaked} rows filled for evidenceless season {season} (must stay NULL)")

    if problems:
        tmp.unlink(missing_ok=True)
        con.close()
        raise SystemExit("ABORT (no swap): " + "; ".join(problems))

    backup = vp.with_name(vp.stem + f"_prew62_{stamp}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    con.close()
    return {
        "rows": after_rows,
        "filled_player_weeks": would,
        "pre_cutover_rows_with_rush_success": filled,
        "rows_changed_at_or_after_cutover": moved_modern,
        "backup": str(backup),
        "evidenceless_seasons_skipped": skipped,
        "provenance": PROV,
        "next": "rebuild season/career (build_season_career_v26) or they stay 1999+",
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="write the backfilled success columns")
    args = ap.parse_args()
    print(json.dumps(run(apply=args.apply), indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
