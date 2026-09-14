"""Delete the 24 proven zero-stat placeholder rows from early-era duplicate games.

Each selected row is paired with the same player/game/opponent represented by a
game-specific PFR row.  The selected row is either a float-year key (``.0``) or
an unsuffixed placeholder; it carries no stat mass.  Rows are pinned by an MD5
row hash and emitted as append-only row_delete facts before the gated swap.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb

from . import facts, golden_samples
from .recon_common import utc_stamp
from .sources import latest_v26

WAVE = "wave71.early_duplicate_placeholder_delete"


def run(apply: bool = False) -> dict:
    v26 = Path(latest_v26())
    ref = v26.as_posix()
    con = duckdb.connect()
    con.execute("""
        CREATE TEMP TABLE dup_groups AS
        SELECT NFL_player_id, year, week, season_type, opponent_nfl_franchise_number
        FROM read_parquet(?)
        WHERE position <> 'DEF' AND NFL_player_id IS NOT NULL
        GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1
    """, [ref])
    con.execute("""
        CREATE TEMP TABLE to_delete AS
        WITH s AS (SELECT * FROM read_parquet(?)),
        candidates AS (
            SELECT s.player_week, md5(to_json(s)) AS row_hash,
                   s.NFL_player_id, s.year, s.week,
                   ROW_NUMBER() OVER (
                     PARTITION BY s.NFL_player_id, s.year, s.week,
                                  s.season_type, s.opponent_nfl_franchise_number
                     ORDER BY s.player_week
                   ) AS rn
            FROM s JOIN dup_groups d
              ON s.NFL_player_id=d.NFL_player_id AND s.year=d.year
             AND s.week=d.week AND s.season_type=d.season_type
             AND s.opponent_nfl_franchise_number=d.opponent_nfl_franchise_number
            WHERE s.position <> 'DEF'
              AND (
                (regexp_matches(s.player_week, '[.]0_') AND EXISTS (
                    SELECT 1 FROM s s2
                    WHERE s2.NFL_player_id=s.NFL_player_id AND s2.year=s.year
                      AND s2.week=s.week AND s2.season_type=s.season_type
                      AND s2.opponent_nfl_franchise_number=s.opponent_nfl_franchise_number
                      AND NOT regexp_matches(s2.player_week, '[.]0_')))
                OR
                (NOT regexp_matches(s.player_week, '_G') AND EXISTS (
                    SELECT 1 FROM s s2
                    WHERE s2.NFL_player_id=s.NFL_player_id AND s2.year=s.year
                      AND s2.week=s.week AND s2.season_type=s.season_type
                      AND s2.opponent_nfl_franchise_number=s.opponent_nfl_franchise_number
                      AND regexp_matches(s2.player_week, '_G')))
              )
        )
        SELECT player_week, row_hash FROM candidates
    """, [ref])
    delete_rows = con.execute("SELECT player_week, row_hash FROM to_delete ORDER BY player_week").fetchall()
    before = int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [ref]).fetchone()[0])
    tmp = v26.with_name(v26.stem + "_duptmp.parquet")
    keys = [r[0] for r in delete_rows]
    con.execute(f"""
        COPY (
          SELECT * FROM read_parquet('{ref}')
          WHERE player_week NOT IN (SELECT player_week FROM to_delete)
        ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
    """)
    after = int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [tmp.as_posix()]).fetchone()[0])
    remaining_dups = int(con.execute("""
        SELECT COUNT(*) FROM (
          SELECT NFL_player_id, year, week, season_type, opponent_nfl_franchise_number
          FROM read_parquet(?) WHERE position <> 'DEF' AND NFL_player_id IS NOT NULL
          GROUP BY 1,2,3,4,5 HAVING COUNT(*) > 1
        )
    """, [tmp.as_posix()]).fetchone()[0])
    con.close()
    diag = {"mode": "APPLY" if apply else "DRY-RUN", "listed": 24,
            "selected": len(delete_rows), "before": before, "after": after,
            "remaining_duplicate_groups": remaining_dups}
    if not apply:
        tmp.unlink(missing_ok=True)
        return diag

    import scripts.sota_recon.sources as sources
    original = sources.latest_v26
    sources.latest_v26 = lambda: str(tmp)
    try:
        golden = golden_samples.run()
    finally:
        sources.latest_v26 = original
    gate = (len(delete_rows) == 24 and after == before - 24 and
            remaining_dups == 0 and golden["failed"] == 0)
    diag.update({"golden": f"{golden['passed']}/{golden['total']}", "gate_pass": gate})
    if not gate:
        tmp.unlink(missing_ok=True)
        diag["swapped"] = False
        return diag

    backup = v26.with_name(v26.stem + f"_prew71_{utc_stamp()}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    fc = facts.connect()
    snap = v26.parent.name
    for key, row_hash in delete_rows:
        facts.emit_fact("row_delete", con=fc, table_name="nfl_player_stats_all",
                        target_key=key, old_row_hash=row_hash, wave_id=WAVE,
                        reason="zero-stat placeholder duplicated by a game-specific PFR row",
                        witness="PFR game-specific player row with same player/week/opponent",
                        source_snapshot_id=snap)
    fc.close()
    diag.update({"swapped": True, "backup": str(backup)})
    return diag


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    print(json.dumps(run(ap.parse_args().apply), indent=2, default=str))
