"""FIX-QUEUE DIAGNOSIS: measure every lane of every specced-but-unlocked
column and classify the failure shape.

Per lane: compare on 2024, 2020-2024, and the lane's peak-density window;
report n + agreement per window and a failure class:
    NO-OVERLAP    lane never joins the plane (key/coverage gap)
    THIN          joins but n < 25 everywhere
    NEAR-MISS     best >= 0.90 (conflict class, arbitration jurisdiction)
    MID           0.50-0.90 (definition/scale suspects)
    FAR           < 0.50 (mapping suspects -- scanner jurisdiction)
    BROKEN        SQL error (harness bug)
No fixes are applied here -- the output is the routed work list.
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
from scripts.sota_recon.vouch_2024 import lane_sql, compare, peak_year

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "fixqueue_diagnosis.json"


def main() -> None:
    tri = json.loads((LAKE / "unwitnessed_triage.json").read_text("utf-8"))
    closed = {p.stem.replace("closure_", "")
              for p in LAKE.glob("closure_*.json")}
    targets = sorted(c for c, cls in tri["columns"].items()
                     if cls.startswith("specced-but-unlocked")
                     and c not in closed)
    print(f"{len(targets)} fix-queue columns", flush=True)

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

    out = {}
    for i, col in enumerate(targets):
        lanes = []
        for sp in W.WITNESS_MAP:
            if sp.v26_col != col:
                continue
            sql, keys = lane_sql(sp)
            if not sql:
                lanes.append({"source": sp.source_key, "class": "NO-LANE"})
                continue
            rec = {"source": sp.source_key,
                   "table": sp.source_table or "", "windows": {}}
            best = 0.0
            for name, lo, hi in (("2024", 2024, 2024),
                                 ("2020-2024", 2020, 2024)):
                try:
                    n, agree = compare(con, sp, sql, keys, lo, hi)
                except Exception as e:
                    rec["windows"][name] = f"BROKEN {str(e)[:40]}"
                    continue
                pct = round(agree / n, 4) if n else None
                rec["windows"][name] = {"n": n, "agree": pct}
                if n and n >= 25:
                    best = max(best, pct)
            if best == 0.0:
                py = peak_year(con, sql, keys)
                if py:
                    try:
                        n, agree = compare(con, sp, sql, keys,
                                           py - 1, py + 1)
                        pct = round(agree / n, 4) if n else None
                        rec["windows"][f"peak {py-1}-{py+1}"] = {
                            "n": n, "agree": pct}
                        if n and n >= 25:
                            best = max(best, pct)
                    except Exception:
                        pass
            rec["class"] = ("NO-OVERLAP" if best == 0.0 else
                            "NEAR-MISS" if best >= 0.90 else
                            "MID" if best >= 0.50 else "FAR")
            rec["best"] = best
            lanes.append(rec)
        out[col] = lanes
        if i % 5 == 0:
            print(f"[{time.strftime('%H:%M:%S')}] {i+1}/{len(targets)} {col}",
                  flush=True)
            OUT.write_text(json.dumps(out, indent=1), encoding="utf-8")
    OUT.write_text(json.dumps(out, indent=1), encoding="utf-8")
    from collections import Counter
    cls = Counter(l.get("class") for ls in out.values() for l in ls)
    print("lane classes:", dict(cls), flush=True)


if __name__ == "__main__":
    main()
