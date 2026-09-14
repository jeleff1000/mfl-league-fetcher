"""Repair the remaining 1944 BOS/WAS rows using the imported NFL.com roster witness.

This is intentionally an explicit seven-row adjudication, not a blanket abbreviation rule:
NFL.com places five player-game rows on Washington; the two DEF rows are the Washington
defense side of the same two BOS-WAS games.  Every row has a stale-value guard and emits
cell facts before the immutable local release swap.
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

WAVE = "wave69.1944_roster_team_repair"
PLAYER_ROWS = (
    "FiorAl20_1944_5", "FiorAl20_1944_11", "PiasAl20_1944_5",
    "ZenoJo20_1944_5", "ZenoJo20_1944_11",
)
DST_ROWS = ("DEF-148_1944_5", "DEF-148_1944_11")
ROWS = PLAYER_ROWS + DST_ROWS


def run(apply: bool = False) -> dict:
    v26 = Path(latest_v26())
    con = duckdb.connect()
    ref = v26.as_posix()
    con.execute("""CREATE TEMP TABLE repairs (
        player_week VARCHAR, old_nfl_franchise DOUBLE, old_opp_franchise DOUBLE,
        new_player VARCHAR
    )""")
    con.executemany(
        "INSERT INTO repairs VALUES (?, ?, ?, ?)",
        [(key, 148.0, 148.0, "Commanders DST" if key in DST_ROWS else None) for key in ROWS],
    )
    quoted = ", ".join("'" + key + "'" for key in ROWS)
    current = con.execute(f"""
        SELECT player_week, player, nfl_team, opponent_nfl_team,
               nfl_franchise_number, opponent_nfl_franchise_number
        FROM read_parquet('{ref}')
        WHERE player_week IN ({quoted})
          AND nfl_franchise_number=148 AND opponent_nfl_franchise_number=148
    """).fetchall()
    stale = [] if len(current) == len(ROWS) else [("expected_rows", len(current))]
    diag = {"listed": len(ROWS), "found": len(current), "stale": stale}
    if stale:
        con.close()
        return {"mode": "APPLY-REFUSED", **diag}
    tmp = v26.with_name(v26.stem + "_w69tmp.parquet")
    key_list = ", ".join("'" + key.replace("'", "''") + "'" for key in ROWS)
    dst_list = ", ".join("'" + key.replace("'", "''") + "'" for key in DST_ROWS)
    predicate = (
        f"player_week IN ({key_list}) AND nfl_franchise_number = 148 AND opponent_nfl_franchise_number = 148"
    )


    con.execute(f"""
        COPY (
            SELECT * REPLACE (
                CASE WHEN {predicate} THEN 'WAS' ELSE nfl_team END AS nfl_team,
                CASE WHEN {predicate} THEN 'BOS' ELSE opponent_nfl_team END AS opponent_nfl_team,
                CASE WHEN {predicate} THEN 4.0 ELSE nfl_franchise_number END AS nfl_franchise_number,
                CASE WHEN {predicate} THEN 148.0 ELSE opponent_nfl_franchise_number END AS opponent_nfl_franchise_number,
                CASE WHEN {predicate} AND player_week IN ({dst_list})
                     THEN 'Commanders DST' ELSE player END AS player,
                CASE WHEN {predicate}
                     THEN CASE WHEN recon_correction_log IS NULL OR recon_correction_log = ''
                               THEN '{WAVE}' ELSE recon_correction_log || ',{WAVE}' END
                     ELSE recon_correction_log END AS recon_correction_log
            )
            FROM read_parquet('{ref}')
        ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
    """)
    before = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{ref}')").fetchone()[0])
    after = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}')").fetchone()[0])
    changed = int(con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}')
        WHERE player_week IN ({key_list})
          AND nfl_team = 'WAS' AND opponent_nfl_team = 'BOS'
          AND nfl_franchise_number = 4 AND opponent_nfl_franchise_number = 148
          AND recon_correction_log LIKE '%{WAVE}%'
    """).fetchone()[0])
    con.close()
    if not apply:
        tmp.unlink(missing_ok=True)
        return {"mode": "DRY-RUN", **diag, "before": before, "after": after, "changed": changed}

    import scripts.sota_recon.sources as sources
    old_latest = sources.latest_v26
    sources.latest_v26 = lambda: str(tmp)
    try:
        golden = golden_samples.run()
    finally:
        sources.latest_v26 = old_latest
    gate = before == after and changed == len(ROWS) and golden["failed"] == 0
    result = {"mode": "APPLY", **diag, "before": before, "after": after,
              "changed": changed, "golden": f"{golden['passed']}/{golden['total']}",
              "gate_pass": gate}
    if not gate:
        tmp.unlink(missing_ok=True)
        result["swapped"] = False
        con = None
        return result
    backup = v26.with_name(v26.stem + f"_prew69_{utc_stamp()}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    fc = facts.connect()
    snap = v26.parent.name
    for key in ROWS:

        target = key + "|148|148" if key in DST_ROWS else key
        for column, old, new in (
            ("nfl_team", "BOS", "WAS"),
            ("opponent_nfl_team", "BOS", "BOS"),
            ("nfl_franchise_number", "148.0", "4.0"),
            ("opponent_nfl_franchise_number", "148.0", "148.0"),
        ):
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=target, column_name=column, old_value=old,
                            new_value=new, wave_id=WAVE,
                            reason="1944 BOS/WAS roster/team attribution; NFL.com roster witness",
                            witness="NFL.com 1944 Washington roster; PFR BOS-WAS schedule",
                            source_snapshot_id=snap)
        if key in DST_ROWS:
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=target, column_name="player", old_value="Boston Yanks DST",
                            new_value="Commanders DST", wave_id=WAVE,
                            reason="1944 Washington defense side re-keyed from duplicated DEF-148 row",
                            witness="PFR BOS-WAS schedule plus paired DEF stat lines",
                            source_snapshot_id=snap)
    fc.close()
    result.update({"swapped": True, "backup": str(backup)})
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    print(json.dumps(run(args.apply), indent=2, default=str))
