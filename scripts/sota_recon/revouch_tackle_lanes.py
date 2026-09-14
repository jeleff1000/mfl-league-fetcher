"""Re-vouch the 7 re-mapped nflcom def_tackles_combined lanes (computed
solo+ast) and refresh the lockfile.

RULE: the old `total` lane held a 2024 lock at 0.9995. If the computed lane
scores >= the old lock, the ruling is confirmed empirically and the lock is
replaced (new fingerprint). If it scores LOWER, that is evidence AGAINST the
2026-08-02 adjudication -- ESCALATE (print + receipt), do not slip it in.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

import scripts.sota_recon.witness_map as W
from scripts.sota_recon import sources as S
from scripts.sota_recon.vouch_2024 import lane_sql, fingerprint, compare

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
LOCKFILE = Path(__file__).parent / "witness_gate" / "contracts" / "witness_locks.v1.json"
SOURCES = ("nflcom_player_logs", "nflcom_player_logs_targeted",
           "nflcom_player_situational", "nflcom_player_splits")


def main() -> int:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane_post AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'POST'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    doc = json.loads(LOCKFILE.read_text(encoding="utf-8"))
    old = [l for l in doc["locks"] if l["column"] == "def_tackles_combined"
           and l["source"] in SOURCES]
    old_best = max((l["agree"] for l in old), default=0.0)
    print(f"old locks on these lanes: {len(old)} (best {old_best})")

    lanes = [s for s in W.WITNESS_MAP if s.v26_col == "def_tackles_combined"
             and s.source_key in SOURCES and s.source_col == "total"]
    results, escalate = [], False
    for sp in lanes:
        assert sp.source_expr, f"override missing on {sp.source_key}"
        sql, keys = lane_sql(sp)
        if not sql:
            results.append({"lane": sp.source_key, "table": sp.source_table,
                            "verdict": "NO-LANE"})
            continue
        try:
            n, agree = compare(con, sp, sql, keys, 2024, 2024)
        except Exception as e:
            results.append({"lane": sp.source_key, "table": sp.source_table,
                            "verdict": f"BROKEN {str(e).splitlines()[0][:60]}"})
            continue
        pct = round(agree / n, 4) if n else None
        rec = {"lane": sp.source_key, "table": sp.source_table, "n": n,
               "agree": pct, "fingerprint": fingerprint(sp)}
        if n >= 25 and pct is not None and pct >= max(0.95, old_best - 0.005):
            rec["verdict"] = "VOUCHED"
        elif n < 25:
            rec["verdict"] = "THIN"
        else:
            rec["verdict"] = "WORSE-THAN-OLD -- ESCALATE"
            escalate = True
        results.append(rec)
        print(json.dumps(rec), flush=True)

    (LAKE / "tackle_remap_revouch.json").write_text(
        json.dumps({"date": time.strftime("%Y-%m-%d %H:%M"),
                    "old_locks": old, "results": results}, indent=1),
        encoding="utf-8")
    if escalate:
        print("ESCALATION: computed solo+ast scored below the old total lane "
              "-- adjudication challenged, locks NOT touched")
        return 1
    # refresh: drop orphaned old locks, mint new ones for vouched lanes
    doc["locks"] = [l for l in doc["locks"] if not (
        l["column"] == "def_tackles_combined" and l["source"] in SOURCES)]
    for r in results:
        if r.get("verdict") == "VOUCHED":
            doc["locks"].append({
                "column": "def_tackles_combined", "source": r["lane"],
                "shape": "nflcom_log_week" if "logs" in r["lane"] else "nflcom",
                "grain": "week" if "logs" in r["lane"] else "season",
                "window": "2024", "n": r["n"], "agree": r["agree"],
                "fingerprint": r["fingerprint"],
                "locked": time.strftime("%Y-%m-%d"),
                "note": "computed solo+ast per 2026-08-02 tackle ruling"})
    LOCKFILE.write_text(json.dumps(doc, indent=1), encoding="utf-8")
    print(f"locks refreshed: {sum(1 for r in results if r.get('verdict')=='VOUCHED')} minted")
    return 0


if __name__ == "__main__":
    sys.exit(main())
