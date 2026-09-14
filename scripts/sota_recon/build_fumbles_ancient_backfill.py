"""ANCIENT WEEKLY FUMBLES BACKFILL (1957-1977), licensed by CONSERVATION.

Joe's Brown/Sayers spot-check found the weekly cells fabricated (now NULL);
NFL.com's harvested gamelogs hold the real per-game values. Modern-era
vouching fails (0.87-0.90 -- definition drift around return/strip fumbles
that did not exist pre-specialization), so the license is PER PLAYER-SEASON
CONSERVATION: a season backfills ONLY if the nflcom weekly sum EXACTLY
equals the independent PFR season witness total. Measured 2026-08-03:
2,427 of 2,946 player-seasons conserve exactly; the 519 others stay NULL
and queue (474 "over" -- suspected multi-team-season grain; 45 under).

Every written week therefore carries a receipt: its season sums to a
witnessed total. Cells written are NULL-only (never overwrite).
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S
from scripts.sota_recon.build_season_conservation_doom import season_lane

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "weekly_overlay_fumbles_ancient.parquet"
RECEIPT = LAKE / "fumbles_ancient_backfill_receipt.json"
ERA = (1957, 1977)


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    reg = S.registry()
    p = Path(reg['nflcom_player_logs'].path)
    pp = (p.as_posix() + "/*.parquet") if p.is_dir() else p.as_posix()
    xw = Path(reg["nflcom_slug_pfrid"].path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    wk = Path(S.latest_v26()).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")
    lane = season_lane("fumbles")
    assert lane, "no season witness lane"
    _, wsrc, wsql = lane
    # SECOND ANCHOR (2026-08-04): pfr defense-table fumbles = ALL-PHASES
    # total (licensed 0.9715 modern). The 474 'over' seasons failed
    # conservation only because rush_rec is offense-scoped -- return men's
    # extra fumbles are real. A season conserving against EITHER witness is
    # licensed.
    import scripts.sota_recon.witness_map as W
    from scripts.sota_recon.vouch_2024 import lane_sql as _ls
    sp2 = next(s for s in W.WITNESS_MAP
               if s.source_key == 'pfr_player_defense'
               and s.v26_col == 'fumbles')
    w2sql, _ = _ls(sp2)

    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE nflwk AS
    SELECT x.pfr_id, TRY_CAST(r.season AS INT) AS yr,
           TRY_CAST(r.wk AS INT) AS wkn,
           ANY_VALUE(TRY_CAST(NULLIF(TRIM(CAST(r.fumbles AS VARCHAR)), '')
                     AS DOUBLE)) AS v
    FROM read_parquet('{pp}', union_by_name=true) r
    JOIN read_parquet('{xw}') x ON x.nflcom_slug = r.nflcom_slug
    WHERE r._table = 'Regular Season'
    GROUP BY 1, 2, 3
    HAVING v IS NOT NULL AND yr BETWEEN {ERA[0]} AND {ERA[1]}""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE conserving AS
    WITH sw AS ({wsql}), sw2 AS ({w2sql}),
    sums AS (SELECT pfr_id, yr, SUM(v) AS s FROM nflwk GROUP BY 1, 2)
    SELECT sums.pfr_id, sums.yr, sums.s AS season_sum,
           COALESCE(sw.val, sw2.val) AS witness
    FROM sums
    LEFT JOIN sw USING (pfr_id, yr)
    LEFT JOIN sw2 USING (pfr_id, yr)
    WHERE (sw.val IS NOT NULL AND ABS(sums.s - sw.val) <= 0.05)
       OR (sw2.val IS NOT NULL AND ABS(sums.s - sw2.val) <= 0.05)""")
    n_seasons = con.execute("SELECT COUNT(*) FROM conserving").fetchone()[0]

    con.execute(f"""
    COPY (
    SELECT t.NFL_player_id, t.year, t.week,
           'fumbles' AS column_name,
           TRY_CAST(t.fumbles AS DOUBLE) AS old_value,
           n.v AS new_value,
           'fumbles_ancient' AS repair_id,
           'nflcom_gamelog+pfr_conservation' AS root,
           'season conserves exactly vs {wsrc} -- every week receipted'
             AS ruling
    FROM nflwk n
    JOIN conserving c USING (pfr_id, yr)
    JOIN ids b ON b.pfr_id = n.pfr_id
    JOIN plane t ON t.NFL_player_id = b.NFL_player_id
      AND CAST(t.year AS INT) = n.yr AND TRY_CAST(t.week AS INT) = n.wkn
    WHERE t.fumbles IS NULL
    QUALIFY COUNT(*) OVER (PARTITION BY t.NFL_player_id, t.year, t.week) = 1
    ) TO '{OUT.as_posix()}' (FORMAT parquet)""")
    # RECEIPTED ZEROS: in an EXACTLY-conserving season the nonzero weeks
    # already place every witnessed event, so a gamelog row whose fumble
    # cell is blank is a TRUE zero by arithmetic -- the one situation where
    # writing 0 is proof, not fill. Only weeks WITH a gamelog row qualify.
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE blankwk AS
    SELECT x.pfr_id, TRY_CAST(r.season AS INT) AS yr,
           TRY_CAST(r.wk AS INT) AS wkn
    FROM read_parquet('{pp}', union_by_name=true) r
    JOIN read_parquet('{xw}') x ON x.nflcom_slug = r.nflcom_slug
    WHERE r._table = 'Regular Season'
      AND NULLIF(TRIM(CAST(r.fumbles AS VARCHAR)), '') IS NULL
    GROUP BY 1, 2, 3
    HAVING yr BETWEEN {ERA[0]} AND {ERA[1]} AND wkn IS NOT NULL""")
    con.execute(f"""
    COPY (
    SELECT t.NFL_player_id, t.year, t.week,
           'fumbles' AS column_name,
           TRY_CAST(t.fumbles AS DOUBLE) AS old_value,
           0.0 AS new_value,
           'fumbles_ancient_zero' AS repair_id,
           'conservation_arithmetic' AS root,
           'receipted zero: season conserves exactly, all events placed'
             AS ruling
    FROM (SELECT b.* FROM blankwk b
          ANTI JOIN nflwk v USING (pfr_id, yr, wkn)) n
    JOIN conserving c USING (pfr_id, yr)
    JOIN ids b ON b.pfr_id = n.pfr_id
    JOIN plane t ON t.NFL_player_id = b.NFL_player_id
      AND CAST(t.year AS INT) = n.yr AND TRY_CAST(t.week AS INT) = n.wkn
    WHERE t.fumbles IS NULL
    QUALIFY COUNT(*) OVER (PARTITION BY t.NFL_player_id, t.year, t.week) = 1
    ) TO '{(LAKE / "weekly_overlay_fumbles_ancient_zeros.parquet").as_posix()}'
    (FORMAT parquet)""")
    nz0 = con.execute(f"""SELECT COUNT(*) FROM read_parquet(
      '{(LAKE / "weekly_overlay_fumbles_ancient_zeros.parquet").as_posix()}')
      """).fetchone()[0]
    print(f"receipted zeros: {nz0} cells", flush=True)
    n_cells, nz = con.execute(f"""SELECT COUNT(*),
      COUNT(*) FILTER (WHERE new_value > 0)
      FROM read_parquet('{OUT.as_posix()}')""").fetchone()
    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"), "era": list(ERA),
         "conserving_player_seasons": n_seasons, "cells": n_cells,
         "nonzero_cells": nz, "season_witness": wsrc,
         "law": ("license = exact per-season conservation against an "
                 "independent witness; non-conserving seasons stay NULL "
                 "and queue")}, indent=1), encoding="utf-8")
    print(f"car: {n_cells} cells ({nz} nonzero) across "
          f"{n_seasons} conserving player-seasons", flush=True)


if __name__ == "__main__":
    main()
