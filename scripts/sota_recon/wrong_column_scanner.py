"""WRONG-COLUMN SCANNER: for every conflict lane at <=0.30 agreement,
measure which plane column the lane's values ACTUALLY match.

The tackle family taught the method twice (once in each direction): a lane
agreeing at near-zero is usually a mis-mapped or scale-shifted column, and
the ruling must come from measurement, never from reading names. For each
low lane this scans a candidate set of plane columns (name-family tokens +
the declared target) in the lane's own window and reports the best matches.
Nothing is re-mapped here -- the output is the evidence table for
adjudication.
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
OUT = LAKE / "wrong_column_scan.json"
TOKENS = re.compile(r"[a-z]+")


def candidates(col: str, plane_cols: set[str]) -> list[str]:
    toks = set(TOKENS.findall(col.lower())) - {"def", "the", "of", "per"}
    cands = {c for c in plane_cols
             if toks & set(TOKENS.findall(c.lower()))}
    cands.add(col)
    return sorted(cands)[:40]


def window_of(w: str) -> tuple[int, int]:
    if w == "2024":
        return 2024, 2024
    if w == "2020-2024":
        return 2020, 2024
    m = re.search(r"(\d{4})-(\d{4})", w)
    return (int(m.group(1)), int(m.group(2))) if m else (2024, 2024)


def main() -> None:
    led = json.loads((LAKE / "vouch_2024_ledger.json").read_text("utf-8"))
    low = [e for e in led["queue"]
           if e.get("why", "").startswith("CONFLICT")
           and (e.get("agree") or 0) <= 0.30]
    # RESUME from the incremental snapshot (write-as-you-go law): a crash
    # costs the last <20 lanes, never the run
    done_keys, results = set(), []
    if OUT.exists():
        prior = json.loads(OUT.read_text("utf-8"))
        results = prior.get("results", [])
        done_keys = {(r["source"], r["declared"], r.get("window"))
                     for r in results}
        print(f"resuming past {len(results)} scanned lanes", flush=True)
    low = [e for e in low
           if (e["source"], e["column"], e.get("window")) not in done_keys]
    print(f"{len(low)} low-agreement lanes to scan", flush=True)
    by_fp = {fingerprint(sp): sp for sp in W.WITNESS_MAP}

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
    plane_cols = {r[0] for r in con.execute(
        "DESCRIBE SELECT * FROM plane LIMIT 0").fetchall()}

    for i, e in enumerate(low):
        sp = by_fp.get(e["fingerprint"])
        if sp is None:
            continue
        sql, keys = lane_sql(sp)
        if not sql:
            continue
        lo, hi = window_of(e.get("window", "2024"))
        row = {"source": e["source"], "declared": e["column"],
               "window": e.get("window"), "declared_agree": e.get("agree"),
               "matches": []}
        for cand in candidates(e["column"], plane_cols):
            try:
                if keys == ("pid", "yr", "wk"):
                    n, ok = con.execute(f"""WITH w AS ({sql})
                    SELECT COUNT(*), COUNT(*) FILTER (
                      WHERE ABS(w.val - TRY_CAST(t.{cand} AS DOUBLE)) <= 0.05)
                    FROM w JOIN plane t ON t.NFL_player_id = w.pid
                      AND t.year = w.yr AND t.week = w.wk
                    WHERE w.yr BETWEEN {lo} AND {hi}
                      AND t.{cand} IS NOT NULL""").fetchone()
                elif keys == ("team", "yr", "wk"):
                    n, ok = con.execute(f"""WITH w AS ({sql})
                    SELECT COUNT(*), COUNT(*) FILTER (
                      WHERE ABS(w.val - TRY_CAST(t.{cand} AS DOUBLE)) <= 0.05)
                    FROM w JOIN plane t ON t.nfl_team = w.team
                      AND t.year = w.yr AND t.week = w.wk
                      AND t.position = 'DEF'
                    WHERE w.yr BETWEEN {lo} AND {hi}
                      AND t.{cand} IS NOT NULL""").fetchone()
                else:
                    n, ok = con.execute(f"""WITH w AS ({sql}),
                    v AS (SELECT b.pfr_id, t.year AS yr,
                                 SUM(TRY_CAST(t.{cand} AS DOUBLE)) AS sv
                          FROM plane t JOIN ids b USING (NFL_player_id)
                          WHERE t.{cand} IS NOT NULL
                            AND t.year BETWEEN {lo} AND {hi}
                          GROUP BY 1, 2)
                    SELECT COUNT(*), COUNT(*) FILTER (
                      WHERE ABS(w.val - v.sv) <= 0.05)
                    FROM w JOIN v USING (pfr_id, yr)
                    WHERE w.yr BETWEEN {lo} AND {hi}""").fetchone()
            except Exception:
                continue
            if n and n >= 25:
                row["matches"].append(
                    {"column": cand, "n": n, "agree": round(ok / n, 4)})
        row["matches"].sort(key=lambda m: -m["agree"])
        row["matches"] = row["matches"][:5]
        results.append(row)
        if i % 20 == 0:
            print(f"[{time.strftime('%H:%M:%S')}] {i}/{len(low)}", flush=True)
            OUT.write_text(json.dumps(
                {"progress": f"{i}/{len(low)}", "results": results},
                indent=1), encoding="utf-8")
    OUT.write_text(json.dumps(
        {"done": True, "scanned": len(results), "results": results},
        indent=1), encoding="utf-8")
    strong = [r for r in results if r["matches"]
              and r["matches"][0]["agree"] >= 0.95
              and r["matches"][0]["column"] != r["declared"]]
    print(f"DONE: {len(results)} scanned, {len(strong)} lanes match a "
          f"DIFFERENT column at >=0.95 (re-map evidence)", flush=True)


if __name__ == "__main__":
    main()
