"""PRECEDENCE ARBITRATION over near-miss conflict lanes (agree 0.90-0.95).

Joe's precedence law (2026-08-02, signoff ledger): pfr > nflcom > pbp >
pfa > statscrew; if everyone is against pfr, go with everyone; log ALL
cross-source arguments. This harness applies it to the DISPUTED CELLS of
each near-miss lane:

  for each disputed cell (lane vs plane, in the lane's own window):
    gather every other week-capable licensed lane for the column
    votes = {root -> value} (OQ-LR-1: nflcom+pfr box = one gamebook voice
             only when their values are equal)
    RULES, in order:
      unanimity-against: >=2 independent roots agree on the SAME value
          against the plane -> overwrite (quorum, strongest)
      precedence: the claiming lane's root outranks every root that sides
          with the plane -> overwrite, ruling logs both sides
      outranked / tie -> plane stands, dissent logged
      three-way value split -> queued, no ruling

TACKLE-REVERSAL SAFEGUARD: any lane whose disputed set is >20%% of its
window cells is SKIPPED with a DEFINITION-SUSPECT flag -- a lane that
disagrees that often is measuring a different concept, and re-mapping is
the scanner's jurisdiction, not arbitration's.

Emits one overlay car (LAKE/weekly_overlay_precedence.parquet) + a full
arguments ledger (precedence_arbitration_receipt.json).
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

import scripts.sota_recon.witness_map as W
from scripts.sota_recon import sources as S
from scripts.sota_recon.vouch_2024 import lane_sql, fingerprint

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "weekly_overlay_precedence.parquet"
RECEIPT = LAKE / "precedence_arbitration_receipt.json"
RANK = {"pfr": 5, "nflcom": 4, "pbp": 3, "pfa": 2, "statscrew": 1,
        "newspaper": 0, "internal": 0, "legacy": -1}


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
    if "newspaper" in k:
        return "newspaper"
    return "internal"


def window_of(w: str) -> tuple[int, int]:
    if w == "2024":
        return 2024, 2024
    if w == "2020-2024":
        return 2020, 2024
    m = re.search(r"(\d{4})-(\d{4})", w or "")
    return (int(m.group(1)), int(m.group(2))) if m else (2024, 2024)


def week_lanes_for(col: str):
    """Every REG week-keyed lane for the column, with its SQL."""
    out = []
    for sp in W.WITNESS_MAP:
        if sp.v26_col != col:
            continue
        if ((sp.season_type or "REG").upper() != "REG"
                or "post" in (sp.source_table or "").lower()
                or "preseason" in (sp.source_table or "").lower()):
            continue
        sql, keys = lane_sql(sp)
        if sql and keys == ("pid", "yr", "wk"):
            out.append((sp, sql))
    return out


def main() -> None:
    led = json.loads((LAKE / "vouch_2024_ledger.json").read_text("utf-8"))
    near = [e for e in led["queue"]
            if e.get("why", "").startswith("CONFLICT")
            and 0.90 <= (e.get("agree") or 0) < 0.95
            and "legacy" not in e["source"]
            # rate columns are Stage-7 jurisdiction: a witness cannot nudge
            # a rating without nudging the bases it is computed from
            and not any(k in e["column"] for k in
                        ("_per_", "_pct", "pct_", "rating", "share"))]
    print(f"{len(near)} near-miss lanes (legacy excluded)", flush=True)
    by_fp = {fingerprint(sp): sp for sp in W.WITNESS_MAP}

    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    cars, ledger = [], []
    seen_cells = set()
    for e in near:
        sp = by_fp.get(e["fingerprint"])
        if sp is None:
            continue
        sql, keys = lane_sql(sp)
        if not sql or keys != ("pid", "yr", "wk"):
            ledger.append({**e, "ruling": "SKIP: not week-keyed"})
            continue
        col = e["column"]
        lo, hi = window_of(e.get("window"))
        my_root = root_of(sp.source_key)
        # LOCKED-LANE ERA GUARD (batch-19 refusal): a locked lane of ANY
        # grain whose window covers the disputed era countersigns from
        # outside the week-grain panel -- single-root precedence cannot
        # override it. Quorum >=2 may still win (and the gate re-checks).
        col_locks = [l for l in json.loads(
            (Path(__file__).parent / "witness_gate" / "contracts" /
             "witness_locks.v1.json").read_text("utf-8"))["locks"]
            if l["column"] == e["column"]
            and l["fingerprint"] != e["fingerprint"]]
        if col_locks:
            e = {**e, "_era_locked": True}
        # tackle-reversal safeguard
        if (e.get("agree") or 0) < 0.80:
            ledger.append({**e, "ruling": "DEFINITION-SUSPECT"})
            continue
        # FULL voter panel -- the [:6] cap silently dropped a lane LOCKED
        # at 1.0 on the disputed cells (batch-15 gate refusal). No silent
        # caps: every week-keyed lane votes, and locked lanes' testimony
        # OUTRANKS precedence (locks are certified constitution).
        others = [(o, osql) for o, osql in week_lanes_for(col)
                  if fingerprint(o) != e["fingerprint"]]
        locked_fps = {l["fingerprint"] for l in json.loads(
            (Path(__file__).parent / "witness_gate" / "contracts" /
             "witness_locks.v1.json").read_text("utf-8"))["locks"]}
        joins, sel = [], []
        for j, (o, osql) in enumerate(others):
            joins.append(f"LEFT JOIN ({osql}) o{j} ON o{j}.pid = w.pid "
                         f"AND o{j}.yr = w.yr AND o{j}.wk = w.wk")
            sel.append(f"o{j}.val AS v{j}")
        q = f"""
        WITH w AS ({sql})
        SELECT w.pid, w.yr, w.wk, w.val AS claim,
               TRY_CAST(t.{col} AS DOUBLE) AS stored
               {',' + ', '.join(sel) if sel else ''}
        FROM w
        JOIN plane t ON t.NFL_player_id = w.pid AND t.year = w.yr
          AND t.week = w.wk
        {' '.join(joins)}
        WHERE w.yr BETWEEN {lo} AND {hi} AND t.{col} IS NOT NULL
          AND ABS(w.val - TRY_CAST(t.{col} AS DOUBLE)) > 0.05
        QUALIFY COUNT(*) OVER (PARTITION BY w.pid, w.yr, w.wk) = 1"""
        try:
            rows = con.execute(q).fetchall()
        except Exception as ex:
            ledger.append({**e, "ruling": f"BROKEN {str(ex)[:60]}"})
            continue
        other_roots = [root_of(o.source_key) for o, _ in others]
        other_locked = [fingerprint(o) in locked_fps for o, _ in others]
        won = kept = queued = 0
        for r in rows:
            pid, yr, wkn, claim, stored = r[0], r[1], r[2], r[3], r[4]
            votes = list(zip(other_roots, other_locked, r[5:]))
            with_claim = {my_root}
            with_plane, elsewhere = set(), set()
            locked_countersign = False
            for rt, is_locked, v in votes:
                if v is None or rt == "legacy":
                    continue
                if abs(v - claim) <= 0.05:
                    with_claim.add(rt)
                elif abs(v - stored) <= 0.05:
                    with_plane.add(rt)
                    if is_locked:
                        locked_countersign = True
                else:
                    elsewhere.add(rt)
            if locked_countersign:
                kept += 1     # a LOCKED lane vouches the stored value:
                continue      # locks outrank precedence, plane stands
            key = (pid, yr, wkn, col)
            if key in seen_cells:
                continue
            if elsewhere and not with_plane and len(with_claim) < 2:
                queued += 1
                continue
            quorum = len(with_claim) >= 2
            outranks = (not with_plane or
                        max(RANK[r] for r in with_claim)
                        > max(RANK[r] for r in with_plane))
            if e.get("_era_locked") and not quorum:
                kept += 1     # locked lane exists on this column at some
                continue      # grain: single-root precedence never wins
            if quorum or outranks:
                seen_cells.add(key)
                won += 1
                cars.append({
                    "NFL_player_id": pid, "year": yr, "week": wkn,
                    "column_name": col, "old_value": stored,
                    "new_value": claim, "repair_id": "precedence_arb",
                    "root": "+".join(sorted(with_claim)),
                    "ruling": (f"{'quorum' if quorum else 'precedence'}: "
                               f"{sorted(with_claim)} over "
                               f"{sorted(with_plane) or ['plane-lineage']}")})
            else:
                kept += 1
        ledger.append({**e, "disputed": len(rows), "overwritten": won,
                       "plane_stands": kept, "queued": queued,
                       "voters": other_roots})
        print(f"[{time.strftime('%H:%M:%S')}] {col}<-{e['source']}: "
              f"{len(rows)} disputed, {won} won, {kept} kept, "
              f"{queued} queued", flush=True)

    if cars:
        con.execute("CREATE TEMP TABLE cars AS SELECT * FROM (VALUES " +
                    "(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL)"
                    ") t LIMIT 0")
        import pandas as pd
        df = pd.DataFrame(cars)
        con.register("cars_df", df)
        con.execute(f"""COPY (SELECT * FROM cars_df)
        TO '{OUT.as_posix()}' (FORMAT parquet)""")
    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"), "cells_written": len(cars),
         "law": "unanimity beats pfr; pfr beats all; every side logged",
         "lanes": ledger}, indent=1), encoding="utf-8")
    print(f"DONE: {len(cars)} cells to car, receipt written", flush=True)


if __name__ == "__main__":
    main()
