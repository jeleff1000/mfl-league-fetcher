"""Backfill ___ops.nfl_historical.nfl_player_stats_all.pts_def_st_td for DEF rows.

Background
----------
DEF_COL_MAP in multi_league/transformations/common/sql_base.py maps
`scoring_def_st_td` (Yahoo stat_id=49 "Kick and Punt Ret TD") to the
`pts_def_st_td` super_table column. That column was added later than
`pts_def_ret_td` (the legacy aggregate) and was only backfilled for
~5,925 of 38,448 DEF rows. The other 30,361 DEF rows have NULL
`pts_def_st_td` even when `pts_def_ret_td` has the value.

Fix 1 (commit e1b547a0) turned on Yahoo DEF recompute, but the SQL
multiplies `COALESCE(s.pts_def_st_td, 0) * scoring_def_st_td` which
produces 0 for the un-backfilled rows. Result: Broncos 2013 wk2 stays
at 9 (1 sack + 4 INT*2) instead of recovering the +6 from the kick
return TD.

The two columns differ semantically:
- `pts_def_st_td` = kick return TDs + punt return TDs only (kr_td + pr_td)
- `pts_def_ret_td` = kr_td + pr_td + blk_kick_td (broader aggregate)

For the 18 rows where both are populated and they disagree, the diff is
exactly the `pts_def_blk_kick_td` count. So:

    pts_def_st_td = pts_def_ret_td - COALESCE(pts_def_blk_kick_td, 0)

Backfill scope: DEF rows where pts_def_st_td IS NULL AND pts_def_ret_td
IS NOT NULL. Existing populated rows are not touched.

Backup table:
    ___ops.public.pts_def_st_td_backfill_backup_20260428
captures (player_week, NFL_player_id, year, week, pts_def_st_td_pre,
pts_def_ret_td, pts_def_blk_kick_td) for the affected rows so the
backfill is fully reversible.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.core.fly_writer import FlyWriter  # noqa: E402


def load_env():
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def main():
    load_env()
    writer = FlyWriter()

    # Backup
    print("[backup] Snapshotting affected DEF rows pre-backfill")
    writer.execute(
        """
        CREATE OR REPLACE TABLE ___ops.public.pts_def_st_td_backfill_backup_20260428 AS
        SELECT player_week, NFL_player_id, year, week,
               pts_def_st_td AS pts_def_st_td_pre,
               pts_def_ret_td, pts_def_blk_kick_td
        FROM ___ops.nfl_historical.nfl_player_stats_all
        WHERE position = 'DEF'
          AND pts_def_st_td IS NULL
          AND pts_def_ret_td IS NOT NULL
        """,
        database="___ops",
    )
    backup_count = writer.execute(
        "SELECT COUNT(*) AS n FROM ___ops.public.pts_def_st_td_backfill_backup_20260428",
        database="___ops",
    )
    print(f"[backup] Captured {backup_count[0]['n']:,} affected rows")

    # Apply backfill
    print("[apply] Backfilling pts_def_st_td from pts_def_ret_td - blk_kick_td")
    res = writer.execute(
        """
        UPDATE ___ops.nfl_historical.nfl_player_stats_all
        SET pts_def_st_td = pts_def_ret_td - COALESCE(pts_def_blk_kick_td, 0)
        WHERE position = 'DEF'
          AND pts_def_st_td IS NULL
          AND pts_def_ret_td IS NOT NULL
        """,
        database="___ops",
    )
    print(f"[apply] Result: {res}")

    # Verify Broncos 2013 wk2/wk4
    print("[verify] Broncos 2013 wk2/wk4 (should now have pts_def_st_td populated)")
    rows = writer.execute(
        """
        SELECT player, year, week, pts_def_st_td, pts_def_ret_td, pts_def_blk_kick_td
        FROM ___ops.nfl_historical.nfl_player_stats_all
        WHERE position='DEF' AND year=2013 AND week IN (2, 4) AND player ILIKE '%bronco%'
        ORDER BY week
        """,
        database="___ops",
    )
    for r in rows:
        print(f"  {r}")


if __name__ == "__main__":
    main()
