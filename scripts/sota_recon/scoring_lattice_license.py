"""SCORING-LATTICE LICENSING, stage 1: composites vs components.

The plane's scoring system is a three-layer lattice:
    components  pts_pass_4pt, pts_rush, pts_rec_ppr, pts_misc, ...
    composites  fpts_{4|5|6}pt_{0ppr|half|ppr|ppfd|tep}, fantasy_points_ppr
    ranks       rank_{pos}_{variant}, rank_alltime_*, *_ppg
Each layer is a deterministic function of the one below. This stage tests
every composite against the SUM of its name-matched components on modern
years (2015-2024) and licenses identities that hold >= 99.9%. Nothing is
assumed from names alone -- names propose, measurement licenses.

Output: scoring_lattice.v1.json {composite: {components, agree, n}} +
unlicensed list for inspection.
"""
from __future__ import annotations

import json
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
REG = Path(__file__).parent / "witness_gate" / "contracts" / "scoring_lattice.v1.json"


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    cols = [r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{wk}') LIMIT 0").fetchall()]
    colset = set(cols)
    con.execute(f"""CREATE OR REPLACE TEMP VIEW modern AS
    SELECT * FROM read_parquet('{wk}')
    WHERE season_type = 'REG' AND CAST(year AS INT) BETWEEN 2015 AND 2024""")

    composites = [c for c in cols if re.match(r'^fpts_\dpt_', c)]
    licensed, queued = {}, []
    for comp in composites:
        m = re.match(r'^fpts_(\d)pt_(\w+)$', comp)
        if not m:
            queued.append({"composite": comp, "why": "unparsed name"})
            continue
        td, rec = m.group(1), m.group(2)
        parts = [f"pts_pass_{td}pt", "pts_rush", f"pts_rec_{rec}",
                 "pts_misc"]
        missing = [p for p in parts if p not in colset]
        if missing:
            queued.append({"composite": comp, "why": f"missing {missing}"})
            continue
        expr = " + ".join(f"COALESCE(TRY_CAST({p} AS DOUBLE), 0)"
                          for p in parts)
        n, ok = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (
          WHERE ABS(TRY_CAST({comp} AS DOUBLE) - ({expr})) <= 0.011)
        FROM modern WHERE {comp} IS NOT NULL""").fetchone()
        agree = ok / n if n else 0.0
        rec_d = {"components": parts, "n": n, "agree": round(agree, 5)}
        if n >= 10000 and agree >= 0.999:
            licensed[comp] = rec_d
        else:
            queued.append({"composite": comp, **rec_d,
                           "why": "identity does not hold"})
        print(f"{comp}: n={n} agree={agree:.5f}"
              f" {'LICENSED' if comp in licensed else 'queued'}", flush=True)

    REG.write_text(json.dumps(
        {"version": "v1", "generated": time.strftime("%Y-%m-%d %H:%M"),
         "window": "2015-2024", "min_agree": 0.999,
         "licensed_composites": licensed, "queued": queued},
        indent=1), encoding="utf-8")
    print(f"DONE: {len(licensed)} licensed, {len(queued)} queued", flush=True)


if __name__ == "__main__":
    main()
