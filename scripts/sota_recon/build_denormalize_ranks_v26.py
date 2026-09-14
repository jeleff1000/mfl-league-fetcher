"""
sota_recon/build_denormalize_ranks_v26.py  --  denormalize season/career ranks onto weekly rows.

The league import (nfl_rankings.calculate_nfl_rankings) READS rank_season_*/rank_alltime_* straight off
the weekly super table ("uses pre-computed rank columns ... simply aliases them"), and the frontend
research mode filters on t.rank_season_*/t.rank_alltime_*. So the weekly super table must carry the
season/career rank of each player stamped on every one of that player's weekly rows.

The season/career RANKS were just recomputed in player_nfl_season / player_nfl_career, but never copied
back to the weekly rows -> weekly's denormalized copies were STALE (42% of rank_season_qb_4pt mismatched)
and DST (rank_season_def / rank_alltime_def) was never populated at all. This is the missing final link:
  weekly rank_season_*  <- player_nfl_season.rank_season_*  (join NFL_player_id+year)   [REG season]
  weekly rank_alltime_* <- player_nfl_career.rank_alltime_*  (join NFL_player_id)        [REG career]
for ALL positions incl DEF. Streaming SELECT * REPLACE (LEFT JOINs, no materialization).

Gate: golden 56/56; rows unchanged; rank_season_def now populated; weekly rank_season_qb_4pt now
matches the season table (0 mismatch).

    python -m scripts.sota_recon.build_denormalize_ranks_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave48.denorm_season_career_ranks"


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    art = Path(v26).parent / "season_career_v26"
    sea = (art / "player_nfl_season.parquet").as_posix()
    car = (art / "player_nfl_career.parquet").as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
    wkcols = [r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{vq}'").fetchall()]
    seacols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{sea}'").fetchall()}
    carcols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{car}'").fetchall()}
    wkset = set(wkcols)
    # ALL season/career rank cols (denormalize every one). Existing weekly cols -> REPLACE;
    # cols only in the season/career tables (new ppfd/tep formats) -> ADD as new weekly columns.
    rank_season = sorted(c for c in seacols if c.startswith("rank_season_"))
    rank_alltime = sorted(c for c in carcols if c.startswith("rank_alltime_"))
    new_season = [c for c in rank_season if c not in wkset]
    new_alltime = [c for c in rank_alltime if c not in wkset]
    if not apply:
        con.close()
        return {"rank_season_cols": len(rank_season), "rank_alltime_cols": len(rank_alltime),
                "new_weekly_cols": len(new_season) + len(new_alltime)}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")
    # dedup season/career to one row per key (safety; they already are unique)
    con.execute(f"CREATE TEMP TABLE _se AS SELECT NFL_player_id, year, {', '.join(rank_season)} FROM '{sea}'")
    con.execute(f"CREATE TEMP TABLE _ca AS SELECT NFL_player_id, {', '.join(rank_alltime)} FROM '{car}'")
    # REPLACE existing weekly rank cols; APPEND the new ppfd/tep ones as fresh weekly columns
    repl = ([f's."{c}" AS "{c}"' for c in rank_season if c in wkset]
            + [f'c."{c}" AS "{c}"' for c in rank_alltime if c in wkset])
    has_log = "recon_correction_log" in wkcols
    if has_log:
        repl.append(f"CASE WHEN w.recon_correction_log IS NULL OR w.recon_correction_log='' THEN '{PROV}' "
                    f"ELSE w.recon_correction_log||',{PROV}' END AS recon_correction_log")
    append = ([f'CAST(s."{c}" AS INTEGER) AS "{c}"' for c in new_season]
              + [f'CAST(c."{c}" AS INTEGER) AS "{c}"' for c in new_alltime])
    append_sql = (", " + ", ".join(append)) if append else ""
    out_sql = (f"SELECT w.* REPLACE ({', '.join(repl)}){append_sql} FROM '{vq}' w "
               f"LEFT JOIN _se s ON w.NFL_player_id=s.NFL_player_id AND w.year=s.year "
               f"LEFT JOIN _ca c ON w.NFL_player_id=c.NFL_player_id")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_denorm.parquet")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()
    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    defnz = con.execute(f"SELECT COUNT(*) FROM '{tq}' WHERE rank_season_def IS NOT NULL").fetchone()[0]
    # weekly rank_season_qb_4pt now == season table?
    mm = con.execute(f"""WITH wk AS (SELECT DISTINCT NFL_player_id,year,rank_season_qb_4pt q FROM '{tq}' WHERE rank_season_qb_4pt IS NOT NULL)
        SELECT COUNT(*) FROM wk JOIN '{sea}' s USING(NFL_player_id,year) WHERE wk.q<>s.rank_season_qb_4pt""").fetchone()[0]
    con.close()
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o
    gate = (g["failed"] == 0 and after == before and defnz > 0 and mm == 0)
    res = {"before": before, "after": after, "rank_season_def_nonnull": defnz, "qb_mismatch_vs_season": mm,
           "n_season_cols": len(rank_season), "n_alltime_cols": len(rank_alltime),
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predenorm_{stamp}.parquet"); shutil.copy2(vp, bk); os.replace(tmp, vp)
        res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    if not a.apply:
        print("DRY:", run())
    else:
        r = run(apply=True)
        print(f"rows {r['before']:,}->{r['after']:,} | denorm {r['n_season_cols']} season + {r['n_alltime_cols']} alltime rank cols "
              f"| rank_season_def_nonnull={r['rank_season_def_nonnull']:,} qb_mismatch_vs_season={r['qb_mismatch_vs_season']} | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
