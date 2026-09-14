"""
sota_recon/build_advanced_layer2_v26.py  --  fold the SECOND advanced layer onto v26 in ONE
rewrite pass (rewrites of the ~1050-col table are expensive on this box, so both column
families ride the same pass): TEAM-DST allowed-mirror (16 cols, DEF rows, franchise-keyed)
+ NGS published metrics (18 cols, 2016+, keyed by gsis_id == NFL_player_id).

Team-DST: see build_team_dst_advanced_v26 (canon franchise join, EPA/WP/success NULL pre-1999,
model-free allowed to 1978). NGS: separation/cushion/YAC-over-expected/RYOE/time-to-throw/
CPOE-above-expectation/aggressiveness -- direct ingest of the published nflverse value (L1),
NULL before 2016 (the tracking era). Both join with a UNIQUE small build side -> no fan-out.

Gate: row count unchanged, all 34 cols present, team-DST DEF-only & EPA-allowed 1999+, NGS
2016+ only & populated, golden_samples 56/56 -> backup + atomic swap. Idempotent.

    python -m scripts.sota_recon.build_advanced_layer2_v26            # dry-run
    python -m scripts.sota_recon.build_advanced_layer2_v26 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb

from .build_team_dst_advanced_v26 import DEF_COLS, EP_ERA_START, _def_lookup_sql, _tda_sql
from .recon_common import utc_stamp
from .sources import latest_v26

NGS_SOURCE = Path("D:/league-history-data/nfl/raw/nextgen_stats/ngs_weekly_2016_2025.parquet")
NGS_ERA_START = 2016
PLAYER_BIO = Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet")


def _ngs_cols() -> list[str]:
    con = duckdb.connect()
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{NGS_SOURCE.as_posix()}')").fetchall()]
    con.close()
    return [c for c in cols if c.startswith("ngs_")]


NGS_COLS = _ngs_cols()
ADDED = DEF_COLS + NGS_COLS


def _select_sql(v26: str, exclude: list[str]) -> str:
    star = "s.*" if not exclude else f"s.* EXCLUDE ({', '.join(exclude)})"
    dcols = ",\n            ".join(f"d.{c}" for c in DEF_COLS)
    ncols = ",\n            ".join(f"n.{c}" for c in NGS_COLS)
    return f"""
        SELECT
            {star},
            {dcols},
            {ncols}
        FROM read_parquet('{v26}') s
        LEFT JOIN def_lookup d
          ON s.position = 'DEF' AND s.nfl_team = d.nfl_team
         AND CAST(s.year AS INTEGER) = d.year AND CAST(s.week AS INTEGER) = d.week
        LEFT JOIN read_parquet('{NGS_SOURCE.as_posix()}') n
          ON s.NFL_player_id = n.NFL_player_id
         AND CAST(s.year AS INTEGER) = n.year AND CAST(s.week AS INTEGER) = n.week
    """


def _ngs_join_coverage(con: duckdb.DuckDBPyConnection, v26: str) -> dict[str, int | bool]:
    """Receipt the NGS source universe against the weekly target universe.

    The target's historical post-season calendar ends at week 21 (2016-2020),
    week 22 (2021-2025), while the NGS feed carries the final game as week 22/23.
    Those rows are source-native NGS coverage, not identity failures. Any unmatched
    row inside the target's week range is an actual mapping defect and blocks a write.
    """
    row = con.execute(f"""
        WITH target AS (
            SELECT DISTINCT NFL_player_id, CAST(year AS INTEGER) AS year,
                            CAST(week AS INTEGER) AS week
            FROM read_parquet('{v26}')
        ), max_week AS (
            SELECT year, MAX(week) AS max_target_week
            FROM target GROUP BY year
        ), source AS (
            SELECT DISTINCT NFL_player_id, CAST(year AS INTEGER) AS year,
                            CAST(week AS INTEGER) AS week
            FROM read_parquet('{NGS_SOURCE.as_posix()}')
        ), source_ids AS (
            SELECT DISTINCT NFL_player_id FROM source
        ), bio AS (
            SELECT DISTINCT NFL_player_id FROM read_parquet('{PLAYER_BIO.as_posix()}')
        ), joined AS (
            SELECT s.*, t.NFL_player_id AS target_id,
                   m.max_target_week
            FROM source s
            LEFT JOIN target t USING (NFL_player_id, year, week)
            LEFT JOIN max_week m USING (year)
        )
        SELECT COUNT(*) AS source_rows,
               COUNT(*) FILTER (WHERE target_id IS NOT NULL) AS target_rows,
               COUNT(*) FILTER (WHERE target_id IS NULL) AS unmatched_rows,
               COUNT(*) FILTER (WHERE target_id IS NULL AND week > max_target_week)
                   AS source_native_boundary_rows,
               COUNT(*) FILTER (WHERE target_id IS NULL AND week <= max_target_week)
                   AS unexpected_unmatched_rows,
               (SELECT COUNT(*) FROM source_ids) AS source_ids,
               (SELECT COUNT(*) FROM source_ids i JOIN bio b USING (NFL_player_id))
                   AS source_ids_in_bio
        FROM joined
    """).fetchone()
    source_rows, target_rows, unmatched_rows, boundary_rows, unexpected_rows, source_ids, source_ids_in_bio = (int(v) for v in row)
    return {
        "source_rows": source_rows,
        "target_rows": target_rows,
        "unmatched_rows": unmatched_rows,
        "source_native_boundary_rows": boundary_rows,
        "unexpected_unmatched_rows": unexpected_rows,
        "source_ids": source_ids,
        "source_ids_in_bio": source_ids_in_bio,
        "source_ids_missing_bio": source_ids - source_ids_in_bio,
        "passed": unexpected_rows == 0 and source_ids == source_ids_in_bio,
    }


def run(apply: bool) -> dict:
    for p in (NGS_SOURCE,):
        if not p.exists():
            raise FileNotFoundError(f"missing source: {p}")
    v26 = latest_v26()
    stamp = utc_stamp()
    con = duckdb.connect()
    con.execute("PRAGMA threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='1200MB'")
    _tmp = Path("D:/league-history-data/nfl/tmp/duckdb")
    _tmp.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{_tmp.as_posix()}'")

    vqp = Path(v26).as_posix()
    cols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{vqp}')").fetchall()]
    existing = [c for c in ADDED if c in cols]
    con.execute(f"CREATE TEMP TABLE tda AS {_tda_sql()}")
    con.execute(f"CREATE TEMP TABLE def_lookup AS {_def_lookup_sql(vqp)}")
    before = con.execute(f"SELECT COUNT(*) FROM read_parquet('{vqp}')").fetchone()[0]
    ngs_coverage = _ngs_join_coverage(con, vqp)
    info = {"existing": len(existing), "def_cols": len(DEF_COLS), "ngs_cols": len(NGS_COLS)}
    if not apply:
        con.close()
        return {"before": int(before), "info": info, "ngs_coverage": ngs_coverage, "swapped": False}

    sql = _select_sql(vqp, existing)
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_layer2.parquet")
    con.execute(f"COPY ({sql}) TO '{tmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 12000)")

    tq = f"read_parquet('{tmp.as_posix()}')"
    after = con.execute(f"SELECT COUNT(*) FROM {tq}").fetchone()[0]
    out_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM {tq}").fetchall()}
    missing = [c for c in ADDED if c not in out_cols]
    nondef_leak = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE position IS DISTINCT FROM 'DEF' AND def_epa_allowed IS NOT NULL").fetchone()[0]
    pre99_epa = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE CAST(year AS INTEGER) < {EP_ERA_START} AND def_epa_allowed IS NOT NULL").fetchone()[0]
    pre16_ngs = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE CAST(year AS INTEGER) < {NGS_ERA_START} AND ngs_avg_separation IS NOT NULL").fetchone()[0]
    def_pop = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE position='DEF' AND CAST(year AS INTEGER)=2023 AND def_epa_allowed IS NOT NULL").fetchone()[0]
    ngs_pop = con.execute(f"SELECT COUNT(*) FROM {tq} WHERE CAST(year AS INTEGER)=2023 AND ngs_avg_separation IS NOT NULL").fetchone()[0]
    con.close()

    import scripts.sota_recon.sources as S
    o = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        from . import golden_samples
        g = golden_samples.run()
    finally:
        S.latest_v26 = o

    gate = (after == before and not missing and ngs_coverage["passed"]
            and nondef_leak == 0 and pre99_epa == 0
            and pre16_ngs == 0 and def_pop > 0 and ngs_pop > 0 and g["failed"] == 0)
    res = {"before": int(before), "after": int(after), "info": info, "missing": missing,
           "ngs_coverage": ngs_coverage,
           "nondef_leak": nondef_leak, "pre1999_epa": pre99_epa, "pre2016_ngs": pre16_ngs,
           "def_2023": def_pop, "ngs_2023": ngs_pop, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prelayer2_backup_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(a.apply)
    if not a.apply:
        print("DRY-RUN:", r["info"], "| rows", f"{r['before']:,}")
    else:
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']} | missing {r['missing']} | "
              f"nondef_leak {r['nondef_leak']} pre99_epa {r['pre1999_epa']} pre16_ngs {r['pre2016_ngs']} | "
              f"def2023 {r['def_2023']:,} ngs2023 {r['ngs_2023']:,}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
