"""
sota_recon/build_weekly_ppg_all_v26.py  --  recompute ALL weekly ppg columns on v26.

The weekly super table carries a per-row ppg "convenience" family for every scoring format:
ppg_season / ppg_alltime / rolling_total / rolling_3 / rolling_5 / weighted_ppg /
consistency / avg_pts_next_year. After this program corrected atoms + recomputed fpts
(incl new ppfd/tep), the STORED ppg columns are stale w.r.t. the corrected fpts (verified:
naive AVG-over-all-rows reproduces stored ppg_season_4pt_tep for 99.89% of player-years; the
0.11% residual is exactly the corrected-fpts drift). ppfd had no ppg columns at all.

So recompute EVERY weekly ppg column from the current fpts/pts, using the EXACT canonical
formulas from update_nfl_super_table (backfill_all_ppg_columns + calculate_rolling_totals):
  15 scoring variants {4pt,5pt,6pt} x {0ppr,half,ppr,tep,ppfd} x 8 families  = 120 cols
  + rolling_total_def (pts_def_std, DEF) + rolling_total_k (pts_k_yds, K)    = 122 cols total
This makes ppfd reach full parity AND refreshes all other formats to the corrected atoms.

Sanity (reported, not gated -- drift is legitimate): for an existing variant (4pt_0ppr) the
recomputed ppg_season vs stored should match >99% of player-years (confirms formula == canon).
Gated: golden_samples 24/24, rows unchanged, all 122 target cols populated.

    python -m scripts.sota_recon.build_weekly_ppg_all_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave42.weekly_ppg_all"
TDS = ("4pt", "5pt", "6pt")
PPRS = ("0ppr", "half", "ppr", "tep", "ppfd")
FAMILIES = ("ppg_season", "ppg_alltime", "rolling_total", "rolling_3", "rolling_5",
            "weighted_ppg", "consistency", "avg_pts_next_year")
T = "st"


def _family_updates(td: str, ppr: str, fpts: str) -> list[tuple[str, str]]:
    """The 8 canonical ppg UPDATEs for one scoring variant (mirrors update_nfl_super_table)."""
    f = fpts
    return [
        (f"ppg_season_{td}_{ppr}", f"""
            UPDATE {T} t SET ppg_season_{td}_{ppr} = ROUND(c.avg_pts, 2)
            FROM (SELECT NFL_player_id, year, AVG({f}) avg_pts FROM {T}
                  WHERE {f} IS NOT NULL GROUP BY NFL_player_id, year) c
            WHERE t.NFL_player_id=c.NFL_player_id AND t.year=c.year"""),
        (f"ppg_alltime_{td}_{ppr}", f"""
            UPDATE {T} t SET ppg_alltime_{td}_{ppr} = ROUND(c.avg_pts, 2)
            FROM (SELECT NFL_player_id, AVG({f}) avg_pts FROM {T}
                  WHERE {f} IS NOT NULL GROUP BY NFL_player_id) c
            WHERE t.NFL_player_id=c.NFL_player_id"""),
        (f"rolling_total_{td}_{ppr}", f"""
            UPDATE {T} t SET rolling_total_{td}_{ppr} = ROUND(c.s, 2)
            FROM (SELECT player_week, SUM({f}) OVER (PARTITION BY NFL_player_id, year
                    ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s
                  FROM {T} WHERE {f} IS NOT NULL) c
            WHERE t.player_week=c.player_week"""),
        (f"rolling_3_{td}_{ppr}", f"""
            UPDATE {T} t SET rolling_3_{td}_{ppr} = ROUND(c.a, 2)
            FROM (SELECT player_week, AVG({f}) OVER (PARTITION BY NFL_player_id
                    ORDER BY year, week ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) a
                  FROM {T} WHERE {f} IS NOT NULL) c
            WHERE t.player_week=c.player_week"""),
        (f"rolling_5_{td}_{ppr}", f"""
            UPDATE {T} t SET rolling_5_{td}_{ppr} = ROUND(c.a, 2)
            FROM (SELECT player_week, AVG({f}) OVER (PARTITION BY NFL_player_id
                    ORDER BY year, week ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) a
                  FROM {T} WHERE {f} IS NOT NULL) c
            WHERE t.player_week=c.player_week"""),
        (f"weighted_ppg_{td}_{ppr}", f"""
            UPDATE {T} t SET weighted_ppg_{td}_{ppr} = ROUND(c.w, 2)
            FROM (SELECT player_week, (
                    COALESCE(LAG({f},0) OVER w*5,0)+COALESCE(LAG({f},1) OVER w*4,0)+
                    COALESCE(LAG({f},2) OVER w*3,0)+COALESCE(LAG({f},3) OVER w*2,0)+
                    COALESCE(LAG({f},4) OVER w*1,0)) / NULLIF(
                    (CASE WHEN LAG({f},0) OVER w IS NOT NULL THEN 5 ELSE 0 END)+
                    (CASE WHEN LAG({f},1) OVER w IS NOT NULL THEN 4 ELSE 0 END)+
                    (CASE WHEN LAG({f},2) OVER w IS NOT NULL THEN 3 ELSE 0 END)+
                    (CASE WHEN LAG({f},3) OVER w IS NOT NULL THEN 2 ELSE 0 END)+
                    (CASE WHEN LAG({f},4) OVER w IS NOT NULL THEN 1 ELSE 0 END),0) w
                  FROM {T} WHERE {f} IS NOT NULL
                  WINDOW w AS (PARTITION BY NFL_player_id ORDER BY year, week)) c
            WHERE t.player_week=c.player_week"""),
        (f"consistency_{td}_{ppr}", f"""
            UPDATE {T} t SET consistency_{td}_{ppr} = ROUND(c.cv, 3)
            FROM (SELECT NFL_player_id, year,
                    CASE WHEN AVG({f})>0 THEN STDDEV({f})/AVG({f}) ELSE 0 END cv
                  FROM {T} WHERE {f} IS NOT NULL GROUP BY NFL_player_id, year) c
            WHERE t.NFL_player_id=c.NFL_player_id AND t.year=c.year"""),
        (f"avg_pts_next_year_{td}_{ppr}", f"""
            UPDATE {T} t SET avg_pts_next_year_{td}_{ppr} = ROUND(c.avg_pts, 2)
            FROM (SELECT NFL_player_id, year, AVG({f}) avg_pts FROM {T}
                  WHERE {f} IS NOT NULL GROUP BY NFL_player_id, year) c
            WHERE t.NFL_player_id=c.NFL_player_id AND t.year=c.year-1"""),
    ]


def _special_rolling(col: str, pts: str, position: str) -> tuple[str, str]:
    return (col, f"""
        UPDATE {T} t SET {col} = ROUND(c.s, 2)
        FROM (SELECT player_week, SUM({pts}) OVER (PARTITION BY NFL_player_id, year
                ORDER BY week ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) s
              FROM {T} WHERE {pts} IS NOT NULL AND nfl_position='{position}') c
        WHERE t.player_week=c.player_week""")


def run(apply=False):
    v26 = latest_v26()
    target = [f"{fam}_{td}_{ppr}" for td in TDS for ppr in PPRS for fam in FAMILIES] + \
             ["rolling_total_def", "rolling_total_k"]
    if not apply:
        return len(target)
    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='4GB'"); con.execute(f"SET temp_directory='{sp}'")
    vq = Path(v26).as_posix()

    # full v26 column order (to preserve schema on output) + source cols for ppg
    v26cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    src_cols = ["player_week", "NFL_player_id", "year", "week", "nfl_position", "recon_correction_log"]
    src_cols += [f"fpts_{td}_{ppr}" for td in TDS for ppr in PPRS]
    src_cols += ["pts_def_std", "pts_k_yds"]
    src_cols = [c for c in dict.fromkeys(src_cols) if c in v26cols]

    # NARROW working table: only the ~45 cols ppg needs (avoids 740-col materialization)
    con.execute(f"CREATE TABLE {T} AS SELECT {', '.join(src_cols)} FROM '{vq}'")
    existing = set(c[0] for c in con.execute(f"DESCRIBE {T}").fetchall())
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]

    # sanity snapshot: stored ppg_season_4pt_0ppr at (player,year) grain (existing format)
    con.execute(f"""CREATE TEMP TABLE _stored_ref AS
        SELECT DISTINCT NFL_player_id, year, ppg_season_4pt_0ppr AS s
        FROM '{vq}' WHERE ppg_season_4pt_0ppr IS NOT NULL""")

    for c in target:
        if c not in existing:
            con.execute(f"ALTER TABLE {T} ADD COLUMN {c} DOUBLE")

    n = 0
    for td in TDS:
        for ppr in PPRS:
            fpts = f"fpts_{td}_{ppr}"
            for _, sql in _family_updates(td, ppr, fpts):
                con.execute(sql); n += 1
    for col, sql in (_special_rolling("rolling_total_def", "pts_def_std", "DEF"),
                     _special_rolling("rolling_total_k", "pts_k_yds", "K")):
        con.execute(sql); n += 1

    # sanity: recomputed vs stored for the existing 4pt_0ppr format (drift expected small)
    san = con.execute("""
        WITH new AS (SELECT DISTINCT NFL_player_id, year, ppg_season_4pt_0ppr AS v FROM st
                     WHERE ppg_season_4pt_0ppr IS NOT NULL)
        SELECT COUNT(*) AS total,
               COUNT(*) FILTER (WHERE ABS(r.s - n.v) > 0.011) AS changed
        FROM _stored_ref r JOIN new n USING (NFL_player_id, year)""").fetchone()
    san_total, san_changed = san[0], san[1]
    san_match_pct = round(100.0 * (san_total - san_changed) / san_total, 3) if san_total else 0.0

    popn = {c: con.execute(f"SELECT COUNT(*) FROM {T} WHERE {c} IS NOT NULL").fetchone()[0] for c in target}
    allpop = all(v > 0 for v in popn.values())
    zero_cols = [c for c, v in popn.items() if v == 0]

    # OUTPUT: stream v26, replacing the 122 ppg cols from the narrow table (preserve col order)
    has_log = "recon_correction_log" in v26cols
    log_expr = (f"""CASE WHEN (f.recon_correction_log IS NULL OR f.recon_correction_log='')
                THEN '{PROV}' ELSE f.recon_correction_log||',{PROV}' END AS recon_correction_log"""
                if has_log else None)
    sel = []
    tset = set(target)
    for c in v26cols:
        if c in tset:
            sel.append(f'p."{c}"')
        elif c == "recon_correction_log":
            sel.append(log_expr)
        else:
            sel.append(f'f."{c}"')
    # append any target cols not present in original v26 (the new ppfd family)
    for c in target:
        if c not in v26cols:
            sel.append(f'p."{c}"')
    if not has_log:
        sel.append(f"'{PROV}' AS recon_correction_log")
    # dedup right side on player_week: v26 has a few duplicate player_week rows; both share the
    # same NFL_player_id/year so ppg values are identical -> dedup keeps row count exact (no fanout).
    pdedup = (f"(SELECT * FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY player_week) AS _rn "
              f"FROM {T}) WHERE _rn = 1)")
    out_sql = (f"SELECT {', '.join(sel)} FROM '{vq}' f "
               f"LEFT JOIN {pdedup} p ON f.player_week = p.player_week")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_ppgall.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    after = con.execute(f"SELECT COUNT(*) FROM '{Path(tmp).as_posix()}'").fetchone()[0]
    con.close(); shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o
    gate = (g["failed"] == 0) and (after == before) and allpop
    res = {"before": before, "after": after, "n_updates": n, "n_target_cols": len(target),
           "all_populated": allpop, "zero_cols": zero_cols,
           "sanity_4pt_0ppr_match_pct": san_match_pct, "sanity_total": san_total, "sanity_changed": san_changed,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_preppgall_{stamp}.parquet"); shutil.copy2(vp, bk)
        os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    if not a.apply:
        print(f"would recompute {run()} weekly ppg columns")
    else:
        r = run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | {r['n_updates']} updates -> {r['n_target_cols']} cols "
              f"all_pop={r['all_populated']} zero={r['zero_cols']}")
        print(f"sanity 4pt_0ppr drift: {r['sanity_changed']}/{r['sanity_total']} player-years changed "
              f"(match {r['sanity_4pt_0ppr_match_pct']}%) | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
