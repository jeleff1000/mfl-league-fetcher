"""NEAR-MISS QUORUM CARS: the fumbles_lost-1980s pattern, generalized.

For each blocked near-miss column, in exactly its blocked decades: find
cells where a week-keyed lane disputes the plane, vote across every other
week-keyed lane, and car ONLY cells where >=2 independent lineage roots
agree on the same value against the plane. Locked-lane countersign blocks
(locks outrank quorum-of-two here as well -- constitution). Dissents and
three-way splits logged, never written.
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
from scripts.sota_recon.vouch_2024 import lane_sql, fingerprint

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "weekly_overlay_nearmiss_quorum.parquet"
RECEIPT = LAKE / "nearmiss_quorum_receipt.json"


def root_of(k: str) -> str:
    k = k.lower()
    if "legacy" in k:
        return "legacy"
    if k.startswith(("pfr", "ancient_pfr", "ancient_pbp1978")):
        return "pfr"
    if k.startswith("nflcom"):
        return "nflcom"
    if k.startswith("pbp"):
        return "pbp"
    if k.startswith("statscrew"):
        return "statscrew"
    if "pfa" in k:
        return "pfa"
    return "internal"


def main() -> None:
    targets = {}
    for p in LAKE.glob("closure_*.json"):
        d = json.loads(p.read_text("utf-8"))
        if d["verdict"] != "BLOCKED":
            continue
        b = [x for x in d["blocking"]
             if isinstance(x, dict) and "decade" in x]
        if b and all((x.get("best_agreement") or 0) >= 0.85 for x in b):
            targets[d["column"]] = [x["decade"] for x in b]
    print(f"{len(targets)} near-miss columns: {sorted(targets)}", flush=True)

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
    locked_fps = {l["fingerprint"] for l in json.loads(
        (Path(__file__).parent / "witness_gate" / "contracts" /
         "witness_locks.v1.json").read_text("utf-8"))["locks"]}

    cars, ledger = [], []
    for col, decades in sorted(targets.items()):
        lanes = []
        for sp in W.WITNESS_MAP:
            if sp.v26_col != col:
                continue
            if ((sp.season_type or "REG").upper() != "REG"
                    or "post" in (sp.source_table or "").lower()
                    or "preseason" in (sp.source_table or "").lower()):
                continue
            sql, keys = lane_sql(sp)
            if sql and keys == ("pid", "yr", "wk"):
                lanes.append((sp, sql))
        if len(lanes) < 2:
            ledger.append({"column": col,
                           "ruling": f"SKIP: {len(lanes)} week lanes, "
                                     "quorum impossible"})
            continue
        for d0 in decades:
            lo, hi = d0, d0 + 9
            won = kept = 0
            for i, (sp, sql) in enumerate(lanes):
                others = [(o, osql) for j, (o, osql) in enumerate(lanes)
                          if j != i]
                joins = " ".join(
                    f"LEFT JOIN ({osql}) o{j} ON o{j}.pid = w.pid "
                    f"AND o{j}.yr = w.yr AND o{j}.wk = w.wk"
                    for j, (o, osql) in enumerate(others))
                sel = ", ".join(f"o{j}.val AS v{j}"
                                for j in range(len(others)))
                try:
                    rows = con.execute(f"""
                    WITH w AS ({sql})
                    SELECT w.pid, w.yr, w.wk, w.val,
                           TRY_CAST(t.{col} AS DOUBLE) AS stored
                           {(', ' + sel) if sel else ''}
                    FROM w JOIN plane t ON t.NFL_player_id = w.pid
                      AND t.year = w.yr AND t.week = w.wk
                    {joins}
                    WHERE w.yr BETWEEN {lo} AND {hi}
                      AND t.{col} IS NOT NULL
                      AND ABS(w.val - TRY_CAST(t.{col} AS DOUBLE)) > 0.05
                    QUALIFY COUNT(*) OVER (
                      PARTITION BY w.pid, w.yr, w.wk) = 1""").fetchall()
                except Exception:
                    continue
                my_root = root_of(sp.source_key)
                oroots = [root_of(o.source_key) for o, _ in others]
                olocked = [fingerprint(o) in locked_fps for o, _ in others]
                for r in rows:
                    pid, yr, wkn, claim, stored = r[:5]
                    with_claim = {my_root}
                    blocked = False
                    for rt, lk, v in zip(oroots, olocked, r[5:]):
                        if v is None or rt == "legacy":
                            continue
                        if abs(v - claim) <= 0.05:
                            with_claim.add(rt)
                        elif abs(v - stored) <= 0.05 and lk:
                            blocked = True
                    if blocked or len(with_claim) < 2:
                        kept += 1
                        continue
                    key = (pid, yr, wkn, col)
                    if any(c["NFL_player_id"] == pid and c["year"] == yr
                           and c["week"] == wkn and c["column_name"] == col
                           for c in cars):
                        continue
                    won += 1
                    cars.append({
                        "NFL_player_id": pid, "year": yr, "week": wkn,
                        "column_name": col, "old_value": stored,
                        "new_value": claim,
                        "repair_id": "nearmiss_quorum",
                        "root": "+".join(sorted(with_claim)),
                        "ruling": (f"{sorted(with_claim)} agree vs plane "
                                   f"in blocked decade {d0}")})
            ledger.append({"column": col, "decade": d0,
                           "won": won, "kept": kept})
            print(f"[{time.strftime('%H:%M:%S')}] {col} {d0}s: "
                  f"{won} won, {kept} kept", flush=True)

    if cars:
        import pandas as pd
        con.register("cars_df", pd.DataFrame(cars))
        con.execute(f"""COPY (SELECT * FROM cars_df)
        TO '{OUT.as_posix()}' (FORMAT parquet)""")
    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"), "cells": len(cars),
         "ledger": ledger}, indent=1), encoding="utf-8")
    print(f"DONE: {len(cars)} quorum cells", flush=True)


if __name__ == "__main__":
    main()
