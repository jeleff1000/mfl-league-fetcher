"""Repair seven player rows whose team binding disagrees with the PFR box witness.

The rows are exact PFR player/game matches; their stat lines agree with the raw
boxscore, but the materialized team/opponent fields were attached to the wrong
side or wrong game. Dick Nesbitt (1933) and Boyd Brumbaugh (1939) are deliberately
excluded because their conflicts are an abbreviation/doubleheader join and an
identity-key problem respectively.
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

WAVE = "wave70.pfr_box_witness_team_binding_repair"

# (player_week, old_team, old_opp, old_tf, old_of, new_team, new_opp, new_tf, new_of)
REPAIRS = (
    ("MertBu20_1949_1_G194908280sfo_BCL_132", "NYG", "PIT", 2, 24, "BCL", "SFO", 132, 15),
    ("MertBu20_1949_1_G194909250pit_NYG_2", "BCL", "SFO", 132, 15, "NYG", "PIT", 2, 24),
    ("SmitBo02_1949_2", "DET", "PHI", 6, 3, "CHH", "SFO", 131, 15),
    ("PollAl20_1951_4", "NYY", "DET", 114, 6, "PHI", "NYG", 3, 2),
    ("AgajBe20_1961_10_G19611112_DTX_BUF", "GNB", "RAM", 7, 14, "DTX", "BUF", 30, 17),
    ("MingGe20_1967_7_G19671029_WAS_BAL", "MIA", "BOS", 18, 19, "WAS", "BAL", 4, 26),
    ("MingGe20_1967_8_G19671105_WAS_STL", "MIA", "NYJ", 18, 20, "WAS", "STL", 4, 13),
)


def _sql_lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def run(apply: bool = False) -> dict:
    v26 = Path(latest_v26())
    ref = v26.as_posix()
    con = duckdb.connect()
    key_list = ", ".join(_sql_lit(r[0]) for r in REPAIRS)
    stale_predicates = [
        f"(player_week={_sql_lit(k)} AND nfl_team={_sql_lit(t)} AND opponent_nfl_team="
        f"{_sql_lit(o)} AND nfl_franchise_number={tf} AND opponent_nfl_franchise_number="
        f"{of})"
        for k, t, o, tf, of, *_ in REPAIRS
    ]
    stale_where = " OR ".join(stale_predicates)
    found = int(con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{ref}') WHERE {stale_where}"
    ).fetchone()[0])
    diag = {"listed": len(REPAIRS), "found": found,
            "stale": [] if found == len(REPAIRS) else ["stale_guard"]}
    if found != len(REPAIRS):
        con.close()
        return {"mode": "APPLY-REFUSED", **diag}

    def expr(column: str, index: int) -> str:
        cases = []
        for row in REPAIRS:
            key, old_t, old_o, old_tf, old_of, new_t, new_o, new_tf, new_of = row
            new_value = {
                "nfl_team": _sql_lit(new_t),
                "opponent_nfl_team": _sql_lit(new_o),
                "nfl_franchise_number": str(new_tf),
                "opponent_nfl_franchise_number": str(new_of),
            }[column]
            cases.append(
                f"WHEN player_week={_sql_lit(key)} AND nfl_team={_sql_lit(old_t)} AND opponent_nfl_team="
                f"{_sql_lit(old_o)} AND nfl_franchise_number={old_tf} AND opponent_nfl_franchise_number="
                f"{old_of} THEN {new_value}"
            )
        return "CASE " + " ".join(cases) + f" ELSE {column} END AS {column}"

    log_cases = []
    for key, old_t, old_o, old_tf, old_of, *_ in REPAIRS:
        log_cases.append(
            f"WHEN player_week={_sql_lit(key)} AND nfl_team={_sql_lit(old_t)} AND opponent_nfl_team="
            f"{_sql_lit(old_o)} AND nfl_franchise_number={old_tf} AND opponent_nfl_franchise_number="
            f"{old_of} THEN CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''"
            " THEN "
            f"{_sql_lit(WAVE)} ELSE recon_correction_log||','||{_sql_lit(WAVE)} END"
        )
    tmp = v26.with_name(v26.stem + "_boxteamtmp.parquet")
    con.execute(f"""
        COPY (
          SELECT * REPLACE (
            {expr('nfl_team', 0)},
            {expr('opponent_nfl_team', 0)},
            {expr('nfl_franchise_number', 0)},
            {expr('opponent_nfl_franchise_number', 0)},
            CASE {' '.join(log_cases)} ELSE recon_correction_log END AS recon_correction_log
          )
          FROM read_parquet('{ref}')
        ) TO '{tmp.as_posix()}' (FORMAT PARQUET)
    """)
    before = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{ref}')").fetchone()[0])
    after = int(con.execute(f"SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}')").fetchone()[0])
    changed = int(con.execute(f"""
        SELECT COUNT(*) FROM read_parquet('{tmp.as_posix()}')
        WHERE player_week IN ({key_list}) AND recon_correction_log LIKE '%{WAVE}%'
    """).fetchone()[0])
    con.close()
    if not apply:
        tmp.unlink(missing_ok=True)
        return {"mode": "DRY-RUN", **diag, "before": before, "after": after, "changed": changed}

    import scripts.sota_recon.sources as sources
    original = sources.latest_v26
    sources.latest_v26 = lambda: str(tmp)
    try:
        golden = golden_samples.run()
    finally:
        sources.latest_v26 = original
    gate = before == after and changed == len(REPAIRS) and golden["failed"] == 0
    result = {"mode": "APPLY", **diag, "before": before, "after": after,
              "changed": changed, "golden": f"{golden['passed']}/{golden['total']}",
              "gate_pass": gate}
    if not gate:
        tmp.unlink(missing_ok=True)
        result["swapped"] = False
        return result

    backup = v26.with_name(v26.stem + f"_prew70_{utc_stamp()}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    fc = facts.connect()
    snap = v26.parent.name
    for key, old_t, old_o, old_tf, old_of, new_t, new_o, new_tf, new_of in REPAIRS:
        target = key
        for column, old, new in (
            ("nfl_team", old_t, new_t),
            ("opponent_nfl_team", old_o, new_o),
            ("nfl_franchise_number", str(old_tf), str(new_tf)),
            ("opponent_nfl_franchise_number", str(old_of), str(new_of)),
        ):
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=target, column_name=column, old_value=old,
                            new_value=new, wave_id=WAVE,
                            reason="PFR boxscore player/team binding witness",
                            witness="PFR player boxscore plus nfl_team_games_all",
                            source_snapshot_id=snap)
    fc.close()
    result.update({"swapped": True, "backup": str(backup)})
    return result


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()
    print(json.dumps(run(args.apply), indent=2, default=str))
