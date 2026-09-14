"""
sota_recon/build_derived_columns_v26.py  --  wave52: derived + context columns, populated.

Adds 20 columns to the weekly table and fills home_away's 636K NULLs. Everything here is
a DETERMINISTIC DERIVATION from tables already on the lake (never a correction, so no
per-cell facts -- the script itself is the provenance; backup + gates still mandatory).

TWO-PASS DESIGN (hard-won 2026-07-11: every single-pass join+wide-write attempt wedged
DuckDB into spill thrash -- multiple 30-minute stalls at 0 output):
  pass 1 (DuckDB)  : in-row composites/rates as a PURE scan+project COPY -- no join,
                     the shape that streams (waves 49-51 precedent)
  pass 2 (pyarrow) : game-context columns + home_away fill attached BATCH-WISE via a
                     python dict lookup (no SQL join at all); streaming ParquetWriter,
                     bounded memory

Columns:
  composites : touches, opportunities, turnovers, scrimmage_tds, total_return_yards,
               def_tackles_combined, all_purpose_yards, total_points_scored, dropbacks
  rates      : yards_per_touch, passing_adjusted_yards_per_attempt,
               passing_net_yards_per_attempt, passing_adjusted_net_yards_per_attempt
               (season aggregation = ratio-of-sums per the WS2b registry)
  buckets    : fg_made_60plus = fg_made - Σ(buckets) when every bucket is present
  context    : is_win, game_margin, team_points, opponent_points, is_overtime
               (from nfl_team_games_all; doubleheader-ambiguous team-weeks stay NULL)

Definition notes (witness-graded):
  * total_points_scored uses SCORED TDs only (total_tds_accounted_for INCLUDES passing TDs -- measured)
  * all_purpose_yards = rush+rec+KR+PR+int-return+fumble-return yards (PFR definition)

Gates: row count identical; column count +20; untouched-column checksums exact;
home_away NULLs strictly decrease; is_win share sane. Swap only on gate-pass.

    python -m scripts.sota_recon.build_derived_columns_v26            # DRY RUN
    python -m scripts.sota_recon.build_derived_columns_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import TEAM_GAMES, latest_v26

WAVE = "wave52.derived_columns"


def _nsum(*cols: str) -> str:
    coal = " + ".join(f"COALESCE({c}, 0)" for c in cols)
    allnull = " AND ".join(f"{c} IS NULL" for c in cols)
    return f"CASE WHEN {allnull} THEN NULL ELSE {coal} END"


SCORED_TDS = _nsum("rushing_tds", "receiving_tds", "kickoff_return_tds",
                   "punt_return_tds", "def_int_ret_td", "fum_ret_td")

# pass-1 columns: in-row only, no join needed
INROW_COLS: dict[str, str] = {
    "touches": _nsum("carries", "receptions"),
    "opportunities": _nsum("carries", "targets"),
    "turnovers": _nsum("passing_interceptions", "fumbles_lost"),
    "scrimmage_tds": _nsum("rushing_tds", "receiving_tds"),
    "total_return_yards": _nsum("kickoff_return_yards", "punt_return_yards"),
    "def_tackles_combined": _nsum("def_tackles_solo", "def_tackle_assists"),
    "all_purpose_yards": _nsum("rushing_yards", "receiving_yards",
                               "kickoff_return_yards", "punt_return_yards",
                               "def_interception_yards", "fum_rec_yds"),
    "total_points_scored": (
        f"CASE WHEN ({SCORED_TDS}) IS NULL AND fg_made IS NULL AND pat_made IS NULL "
        f"THEN NULL ELSE 6 * COALESCE(({SCORED_TDS}), 0) + 3 * COALESCE(fg_made, 0) "
        f"+ COALESCE(pat_made, 0) "
        f"+ 2 * (COALESCE(rushing_2pt_conversions, 0) + COALESCE(receiving_2pt_conversions, 0)) "
        f"+ 2 * COALESCE(def_safeties, 0) END"),
    "dropbacks": _nsum("attempts", "sacks_suffered"),
    "yards_per_touch": (
        "CASE WHEN COALESCE(carries, 0) + COALESCE(receptions, 0) > 0 THEN "
        "(COALESCE(rushing_yards, 0) + COALESCE(receiving_yards, 0)) "
        "/ (COALESCE(carries, 0) + COALESCE(receptions, 0)) END"),
    "passing_adjusted_yards_per_attempt": (
        "CASE WHEN COALESCE(attempts, 0) > 0 THEN "
        "(COALESCE(passing_yards, 0) + 20 * COALESCE(passing_tds, 0) "
        "- 45 * COALESCE(passing_interceptions, 0)) / attempts END"),
    "passing_net_yards_per_attempt": (
        "CASE WHEN COALESCE(attempts, 0) + COALESCE(sacks_suffered, 0) > 0 THEN "
        "(COALESCE(passing_yards, 0) - COALESCE(sack_yards_lost, 0)) "
        "/ (COALESCE(attempts, 0) + COALESCE(sacks_suffered, 0)) END"),
    "passing_adjusted_net_yards_per_attempt": (
        "CASE WHEN COALESCE(attempts, 0) + COALESCE(sacks_suffered, 0) > 0 THEN "
        "(COALESCE(passing_yards, 0) - COALESCE(sack_yards_lost, 0) "
        "+ 20 * COALESCE(passing_tds, 0) - 45 * COALESCE(passing_interceptions, 0)) "
        "/ (COALESCE(attempts, 0) + COALESCE(sacks_suffered, 0)) END"),
    "fg_made_60plus": (
        "CASE WHEN fg_made IS NOT NULL AND fg_made_0_19 IS NOT NULL "
        "AND fg_made_20_29 IS NOT NULL AND fg_made_30_39 IS NOT NULL "
        "AND fg_made_40_49 IS NOT NULL AND fg_made_50_59 IS NOT NULL "
        "AND fg_made >= fg_made_0_19 + fg_made_20_29 + fg_made_30_39 "
        "+ fg_made_40_49 + fg_made_50_59 "
        "THEN fg_made - (fg_made_0_19 + fg_made_20_29 + fg_made_30_39 "
        "+ fg_made_40_49 + fg_made_50_59) END"),
}
CONTEXT_COLS = ["is_win", "game_margin", "team_points", "opponent_points", "is_overtime"]

CHECKSUM_COLS = ["passing_yards", "rushing_yards", "receiving_yards", "fg_made",
                 "def_tackles_solo"]
BATCH = 131_072


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _checksums(con, path: str) -> dict:
    sums = ", ".join(f"SUM({c}) AS {c}" for c in CHECKSUM_COLS)
    row = con.execute(f"SELECT COUNT(*) AS n, {sums} FROM '{path}'").fetchone()
    per_st = con.execute(
        f"SELECT season_type, COUNT(*) FROM '{path}' GROUP BY 1 ORDER BY 1").fetchall()
    return {"row": row, "per_st": per_st}


def _ctx_map(con, vq: str) -> dict:
    """(player_week, season_type) -> (result, team_pts, opp_pts, is_ot, is_home).
    Unique team-week games only; doubleheader-ambiguous rows never get context."""
    tg = _q(TEAM_GAMES)
    rows = con.execute(f"""
        WITH g AS (
          SELECT team_fid, year, CAST(week AS INT) AS week, season_type,
                 ANY_VALUE(result) AS result,
                 ANY_VALUE(team_points) AS team_points,
                 ANY_VALUE(opponent_points) AS opponent_points,
                 ANY_VALUE(COALESCE(is_overtime, FALSE)) AS is_overtime,
                 ANY_VALUE(COALESCE(is_home, FALSE)) AS is_home
          FROM '{tg}' GROUP BY 1, 2, 3, 4 HAVING COUNT(*) = 1)
        SELECT t.player_week, t.season_type,
               ANY_VALUE(g.result), ANY_VALUE(g.team_points),
               ANY_VALUE(g.opponent_points), ANY_VALUE(g.is_overtime),
               ANY_VALUE(g.is_home)
        FROM (SELECT DISTINCT player_week, season_type, nfl_franchise_number,
                     year, CAST(week AS INT) AS week FROM '{vq}') t
        JOIN g ON g.team_fid = t.nfl_franchise_number AND g.year = t.year
              AND g.week = t.week AND g.season_type = t.season_type
        GROUP BY 1, 2 HAVING COUNT(*) = 1""").fetchall()
    return {(pw, st): (r, tp, op, ot, ih) for pw, st, r, tp, op, ot, ih in rows}


def _pass2_attach(tmp1: str, tmp2: str, ctx: dict) -> None:
    """Stream tmp1 batch-wise, attach context columns + home_away fill, write tmp2."""
    pf = pq.ParquetFile(tmp1)
    writer = None
    try:
        for batch in pf.iter_batches(batch_size=BATCH):
            tbl = pa.Table.from_batches([batch])
            pws = tbl.column("player_week").to_pylist()
            sts = tbl.column("season_type").to_pylist()
            ha = tbl.column("home_away").to_pylist()
            iw, gm, tp, op, ot, ha2 = [], [], [], [], [], []
            for i in range(len(pws)):
                c = ctx.get((pws[i], sts[i]))
                if c is None:
                    iw.append(None); gm.append(None); tp.append(None)
                    op.append(None); ot.append(None); ha2.append(ha[i])
                else:
                    r, tpv, opv, otv, ish = c
                    iw.append(None if r is None else r == "W")
                    gm.append(None if tpv is None or opv is None else tpv - opv)
                    tp.append(tpv); op.append(opv); ot.append(bool(otv))
                    ha2.append(ha[i] if ha[i] is not None
                               else ("home" if ish else "away"))
            idx = tbl.schema.get_field_index("home_away")
            tbl = tbl.set_column(idx, "home_away", pa.array(ha2, pa.string()))
            tbl = tbl.append_column("is_win", pa.array(iw, pa.bool_()))
            tbl = tbl.append_column("game_margin", pa.array(gm, pa.float64()))
            tbl = tbl.append_column("team_points", pa.array(tp, pa.float64()))
            tbl = tbl.append_column("opponent_points", pa.array(op, pa.float64()))
            tbl = tbl.append_column("is_overtime", pa.array(ot, pa.bool_()))
            if writer is None:
                writer = pq.ParquetWriter(tmp2, tbl.schema)
            writer.write_table(tbl)
    finally:
        if writer is not None:
            writer.close()


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET preserve_insertion_order = false")
    v26_p = Path(latest_v26())
    vq = _q(v26_p)

    have = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()}
    clash = [c for c in list(INROW_COLS) + CONTEXT_COLS if c in have]
    if clash:
        raise SystemExit(f"columns already exist, refusing: {clash}")

    before = _checksums(con, vq)
    ha_null_before = con.execute(
        f"SELECT COUNT(*) FROM '{vq}' WHERE home_away IS NULL").fetchone()[0]
    ctx = _ctx_map(con, vq)

    inrow = ",\n           ".join(f"{e} AS {n}" for n, e in INROW_COLS.items())
    tmp1 = v26_p.with_name(v26_p.stem + "_w52a.parquet")
    tmp2 = v26_p.with_name(v26_p.stem + "_w52.parquet")
    con.execute(f"""
        COPY (SELECT *, {inrow} FROM '{vq}')
        TO '{Path(tmp1).as_posix()}' (FORMAT PARQUET)""")
    _pass2_attach(Path(tmp1).as_posix(), Path(tmp2).as_posix(), ctx)
    os.remove(tmp1)

    tq = Path(tmp2).as_posix()
    after = _checksums(con, tq)
    ncols_after = len(con.execute(f"DESCRIBE SELECT * FROM '{tq}'").fetchall())
    ha_null_after = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE home_away IS NULL").fetchone()[0]
    win_share, ctx_filled, touches_n = con.execute(f"""
        SELECT AVG(CASE WHEN is_win THEN 1.0 ELSE 0.0 END),
               COUNT(is_win), COUNT(touches) FROM '{tq}'""").fetchone()

    gate = (before == after
            and ncols_after == len(have) + len(INROW_COLS) + len(CONTEXT_COLS)
            and ha_null_after < ha_null_before
            and ctx_filled > 0 and 0.40 <= (win_share or 0) <= 0.60)
    res = {"mode": "APPLY" if apply else "DRY-RUN",
           "cols_before": len(have), "cols_after": ncols_after,
           "checksums_identical": before == after,
           "home_away_nulls": [ha_null_before, ha_null_after],
           "context_rows_filled": ctx_filled, "is_win_share": round(win_share or 0, 4),
           "touches_populated": touches_n, "gate_pass": bool(gate)}
    if apply and gate:
        bk = v26_p.with_name(v26_p.stem + f"_prew52_{utc_stamp()}.parquet")
        shutil.copy2(v26_p, bk)
        os.replace(tmp2, v26_p)
        res.update(backup=str(bk), swapped=True)
    else:
        os.remove(tmp2)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
