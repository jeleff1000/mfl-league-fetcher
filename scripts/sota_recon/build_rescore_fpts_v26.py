"""
sota_recon/build_rescore_fpts_v26.py -- rescore offensive fantasy lanes from stat atoms.

Some historical rows can have real offensive production but stale zero/null fpts
and pts_pass/pts_rush/pts_rec components after late stat backfills. This rewrites
only the offensive/kicking/composite lanes for rows whose current fpts_4pt_half
does not match the scoring atoms. IDP and DST lanes are preserved.

    python -m scripts.sota_recon.build_rescore_fpts_v26 [--apply]
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.fantasy_points_calculator import (  # noqa: E402
    calculate_all_fantasy_points,
    calculate_composite_fantasy_points,
    safe_col,
)

from .sources import latest_v26  # noqa: E402
from .recon_common import utc_stamp  # noqa: E402
# Single source of truth for the offensive scoring recipe (shared with the golden_points GATE, so a
# scoring change moves the rescore + gate in lockstep). See offense_recipe.py.
from .offense_recipe import EXPECTED_4PT_HALF, FIRST_DOWN_BONUS, OFF_ELIG  # noqa: E402

PROV = "wave57.rescore_stale_fpts"

STALE_4PT_HALF_TARGET = (
    f"{OFF_ELIG} "
    f"AND ABS(COALESCE(fpts_4pt_half, 0) - ({EXPECTED_4PT_HALF})) > 0.01"
)

PPFD_TARGET_TERMS = [
    f"TRY_CAST(fpts_{td}pt_half AS DOUBLE) IS NOT NULL "
    f"AND (fpts_{td}pt_ppfd IS NULL "
    f"OR ABS(COALESCE(TRY_CAST(fpts_{td}pt_ppfd AS DOUBLE), 0) "
    f"- (COALESCE(TRY_CAST(fpts_{td}pt_half AS DOUBLE), 0) + ({FIRST_DOWN_BONUS}))) > 0.01)"
    for td in ("4", "5", "6")
]

PPFD_TARGET = f"{OFF_ELIG} AND (" + " OR ".join(PPFD_TARGET_TERMS) + ")"

TARGET = f"(({STALE_4PT_HALF_TARGET}) OR ({PPFD_TARGET}))"


def _is_rescore_output(col: str) -> bool:
    if col.startswith("fpts_"):
        return True
    if (
        col.startswith("pts_idp_")
        or col.startswith("pts_def_")
        or col.startswith("pts_allow_")
        or col.startswith("yds_allow_")
    ):
        return False
    return col.startswith(
        (
            "pts_pass_",
            "pts_rush",
            "pts_rec",
            "pts_misc",
            "pts_fum_lost",
            "pts_ret_yds",
            "pts_first_downs",
            "pts_k_",
        )
    )


def _add_ppfd(df):
    fd = safe_col(df, "rushing_first_downs") + safe_col(df, "receiving_first_downs")
    for td in ("4", "5", "6"):
        half = f"fpts_{td}pt_half"
        ppfd = f"fpts_{td}pt_ppfd"
        if half in df.columns and ppfd in df.columns:
            df[ppfd] = df[half] + fd * 0.5
    return df


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    # 4GB limit (streams the rewrite via record batches; only TARGET rows are materialized) keeps peak
    # memory bounded on the local box, avoiding the wide-table OOM seen at 6GB when jobs compete.
    con.execute("SET memory_limit='4GB'")
    n_target = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {TARGET}").fetchone()[0]

    # unsplit-doubleheader weeks duplicate player_week, so a player_week-keyed
    # recompute would stamp one game's values onto both physical rows -- those
    # stay with the DH wave-2 queue and remain targets after the swap
    con.execute(f"""CREATE TEMP TABLE dupes AS
        SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*) > 1""")
    n_dup_target = con.execute(
        f"SELECT COUNT(*) FROM '{vq}' WHERE {TARGET} "
        "AND player_week IN (SELECT player_week FROM dupes)").fetchone()[0]

    df = con.execute(
        f"SELECT * FROM '{vq}' WHERE {TARGET} "
        "AND player_week NOT IN (SELECT player_week FROM dupes)").df()
    cols_before = set(df.columns)
    if not df.empty:
        df = calculate_all_fantasy_points(df)
        df = calculate_composite_fantasy_points(df)
        df = _add_ppfd(df)
    produced = [c for c in df.columns if c in cols_before and _is_rescore_output(c)]

    norm = df[df["player_week"] == "HIST-68166868_1951_1"] if "player_week" in df.columns else df.iloc[0:0]
    norm_fp = float(norm["fpts_4pt_half"].fillna(0).iloc[0]) if len(norm) else 0.0

    if not apply:
        con.close()
        return {
            "target_rows": n_target,
            "dup_target_rows_held_for_dh_queue": n_dup_target,
            "produced_cols": len(produced),
            "norm_van_brocklin_1951_wk1_fpts_4pt_half": round(norm_fp, 2),
            "sample_cols": produced[:12],
        }

    keep = ["player_week"] + produced
    recomp = df[keep].copy()
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{sp}'")
    con.register("recomp", recomp)
    con.execute("CREATE TEMP TABLE r AS SELECT * FROM recomp")
    wkcols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    repl = [f'CASE WHEN r.player_week IS NOT NULL THEN r."{c}" ELSE w."{c}" END AS "{c}"' for c in produced]
    if "recon_correction_log" in wkcols:
        repl.append(
            "CASE WHEN r.player_week IS NOT NULL THEN "
            f"(CASE WHEN w.recon_correction_log IS NULL OR w.recon_correction_log='' THEN '{PROV}' "
            f"ELSE w.recon_correction_log||',{PROV}' END) ELSE w.recon_correction_log END AS recon_correction_log"
        )
    out_sql = f"SELECT w.* REPLACE ({', '.join(repl)}) FROM '{vq}' w LEFT JOIN r ON w.player_week = r.player_week"

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_rescore.parquet")
    before_rows = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    before_target = con.execute(f"SELECT COUNT(*) FROM '{vq}' WHERE {TARGET}").fetchone()[0]
    rb = con.execute(out_sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), rb.schema)
    for batch in rb:
        writer.write_batch(batch)
    writer.close()
    tq = tmp.as_posix()
    after_rows = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_target = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE {TARGET}").fetchone()[0]
    norm_after = con.execute(
        f"SELECT ROUND(fpts_4pt_half, 2), ROUND(fpts_6pt_half, 2) FROM '{tq}' "
        "WHERE player_week = 'HIST-68166868_1951_1'"
    ).fetchone()
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S

    old_latest = S.latest_v26
    old_golden_latest = golden_samples.latest_v26
    S.latest_v26 = lambda: str(tmp)
    golden_samples.latest_v26 = lambda: str(tmp)
    try:
        golden = golden_samples.run()
    finally:
        S.latest_v26 = old_latest
        golden_samples.latest_v26 = old_golden_latest

    gate = (
        golden["failed"] == 0
        and after_rows == before_rows
        and after_target == n_dup_target
        and before_target == n_target
        and (norm_after[0] if norm_after else 0) == 43.86
    )
    res = {
        "target_rows": n_target,
        "dup_target_rows_held_for_dh_queue": n_dup_target,
        "rows": after_rows,
        "remaining_target_rows": after_target,
        "norm_van_brocklin_1951_wk1_fpts_4pt_half": norm_after[0] if norm_after else None,
        "norm_van_brocklin_1951_wk1_fpts_6pt_half": norm_after[1] if norm_after else None,
        "golden": f"{golden['passed']}/{golden['total']}",
        "gate_pass": bool(gate),
    }
    if gate:
        backup = vp.with_name(vp.stem + f"_prerescore_{stamp}.parquet")
        shutil.copy2(vp, backup)
        os.replace(tmp, vp)
        res["backup"] = backup.name
        res["swapped"] = True
    else:
        res["swapped"] = False
        res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    print(run(apply=args.apply))
