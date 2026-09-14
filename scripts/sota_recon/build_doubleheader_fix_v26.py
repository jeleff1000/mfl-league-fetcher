"""
sota_recon/build_doubleheader_fix_v26.py  --  remove phantom un-split doubleheader rows on v26.

The 1920s-40s NFL played doubleheaders (a player has 2 real game rows in one week, distinct
opponent + date). The game-fragment split (player_game_fragments_merged_at_20260504) created the
per-game rows but LEFT BEHIND the pre-split combined row for 16 player-weeks. That phantom has
game_date=NULL AND nfl_position=NULL and stats = SUM of the real games -> it double-counts raw
stats + games into season/career aggregates.

Fix (surgical, keeps every real game):
  * DELETE phantom rows: within duplicated player_weeks, rows where game_date IS NULL AND
    nfl_position IS NULL (the un-split aggregate leftovers).
  * DELETE the one TRUE duplicate (Lee Woodruff 1931 wk12 vs GNB recorded twice): drop the
    less-complete copy (passing_tds IS NULL).
Streaming row-filter (read_parquet -> filtered SELECT -> ParquetWriter); no 740-col materialization.

Gate: golden_samples 24/24; rows_after == rows_before - expected_deleted; AND post-fix every
remaining duplicated player_week is a LEGIT doubleheader (distinct opponent+date, no NULL
date/position) -> i.e. zero phantoms/true-dups remain at ANY year.

    python -m scripts.sota_recon.build_doubleheader_fix_v26 [--apply]
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

PROV = "wave43.doubleheader_phantom_fix"


def _diag(con, src):
    dup = con.execute(f"""SELECT player_week FROM '{src}' WHERE player_week IS NOT NULL
        GROUP BY player_week HAVING COUNT(*)>1""").fetchall()
    dupn = len(dup)
    phantom = con.execute(f"""SELECT COUNT(*) FROM '{src}' WHERE game_date IS NULL AND nfl_position IS NULL
        AND player_week IN (SELECT player_week FROM '{src}' WHERE player_week IS NOT NULL
        GROUP BY player_week HAVING COUNT(*)>1)""").fetchone()[0]
    return dupn, phantom


def run(apply=False):
    v26 = latest_v26(); vq = Path(v26).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    dupn, phantom = _diag(con, vq)
    expected_deleted = phantom + 1  # + Woodruff true-dup
    if not apply:
        con.close()
        return {"rows": before, "dup_player_weeks": dupn, "phantom_rows": phantom,
                "expected_deleted": expected_deleted}

    stamp = utc_stamp(); sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute(f"SET temp_directory='{sp}'")

    out_sql = f"""
    WITH dups AS (SELECT player_week FROM '{vq}' WHERE player_week IS NOT NULL
                  GROUP BY player_week HAVING COUNT(*)>1)
    SELECT t.* FROM '{vq}' t
    WHERE NOT (t.player_week IN (SELECT player_week FROM dups)
               AND t.game_date IS NULL AND t.nfl_position IS NULL)
      AND NOT (t.player_week = 'WoodLe20_1931_12' AND t.passing_tds IS NULL)
    """
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_dhfix.parquet")
    r = con.execute(out_sql).fetch_record_batch(50000); w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r: w.write_batch(b)
    w.close()

    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    # post-fix: every remaining dup player_week must be a legit doubleheader
    bad = con.execute(f"""
        WITH dups AS (SELECT player_week FROM '{tq}' WHERE player_week IS NOT NULL
                      GROUP BY player_week HAVING COUNT(*)>1)
        SELECT COUNT(*) FROM (
          SELECT player_week,
                 COUNT(*) AS n,
                 COUNT(DISTINCT (opponent_nfl_team, game_date)) AS distinct_games,
                 COUNT(*) FILTER (WHERE game_date IS NULL OR nfl_position IS NULL) AS nullish
          FROM '{tq}' WHERE player_week IN (SELECT player_week FROM dups)
          GROUP BY player_week
        ) WHERE n <> distinct_games OR nullish > 0""").fetchone()[0]
    remaining_dups = con.execute(f"""SELECT COUNT(*) FROM (SELECT player_week FROM '{tq}'
        WHERE player_week IS NOT NULL GROUP BY player_week HAVING COUNT(*)>1)""").fetchone()[0]
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try: g = golden_samples.run()
    finally: S.latest_v26 = o

    gate = (g["failed"] == 0) and (after == before - expected_deleted) and (bad == 0)
    res = {"before": before, "after": after, "expected_deleted": expected_deleted,
           "actual_deleted": before - after, "remaining_legit_doubleheaders": remaining_dups,
           "bad_dup_groups": bad, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predhfix_{stamp}.parquet"); shutil.copy2(vp, bk)
        os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
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
        print(f"rows {r['before']:,}->{r['after']:,} (deleted {r['actual_deleted']}, expected {r['expected_deleted']}) "
              f"| remaining legit doubleheaders={r['remaining_legit_doubleheaders']} bad_groups={r['bad_dup_groups']} "
              f"| golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else f"NOT swapped {r['temp']}"))
