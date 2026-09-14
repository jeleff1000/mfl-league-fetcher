"""
sota_recon/lineage_flip_report.py  --  O.5: THE FLIP REPORT (what collapse changes)

For every licensed REG season-sum stat cell (player-season), the verdict is computed
TWICE over the identical witness values:

  naive     : every primary SOURCE casts its own vote -- the pre-O.5 fiction in which
              correlated copies (PFR pages + PFR box + PFR-rooted pbp 1978-98) count as
              separate confirmations
  collapsed : votes collapse to independent lineage ROOTS via lineage_roots.v1.json
              (root_of; intra-root disagreement -> the root ABSTAINS as
              INTRA-LINEAGE-INCONSISTENT)

Every cell whose verdict class changes is a FLIP. The expected honest outcome: a large
"N-witness UNANIMOUS" population collapses to SINGLE (one root, zero cross-examination)
-- those cells were never independently confirmed, and the root-diversity map (§16.1)
inherits them as its closure queue.

Outputs:
  docs/lineage-flip-report.json                  committed summary: per-stat verdict
                                                 distributions both regimes + flip matrix
  {receipts}/lineage_flip/flip_samples.csv       sample flipped cells (regenerable)

Run:  python -m scripts.sota_recon.lineage_flip_report [--stat passing_yards] [--limit N]
"""

from __future__ import annotations

import argparse
import json
import os
from collections import Counter, defaultdict

import duckdb

from . import sources as S
from . import witness_votes as WV

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "lineage-flip-report.json")
LANE = "lineage_flip"


def _naive_vote(vals: dict[str, float], tol: float):
    """Every primary source = one vote; no root collapse, no intra-root abstention."""
    reg = S.registry()
    votes = {k: v for k, v in vals.items()
             if k != "v26" and v is not None and reg[k].witness_class == "primary"}
    verdict, consensus, agree, disagree = WV._cluster(votes, tol)
    return verdict, agree, disagree, len(votes)


def run(stats: list[str] | None = None, sample_limit: int = 2000) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    all_stats = WV.stats_from_licensed()
    if stats:
        all_stats = {k: v for k, v in all_stats.items() if k in set(stats)}
    per_stat: dict[str, dict] = {}
    flip_matrix: Counter = Counter()
    samples: list[dict] = []
    for stat, tol in sorted(all_stats.items()):
        fam = WV._family_of(stat)
        rows = con.execute(f"""
            SELECT pfr_id, yr, array_agg(witness), array_agg(val)
            FROM ({WV._long_sql(stat)}) t(witness, pfr_id, yr, val)
            WHERE pfr_id IS NOT NULL AND yr IS NOT NULL AND val IS NOT NULL
            GROUP BY pfr_id, yr""").fetchall()
        naive_cnt: Counter = Counter()
        coll_cnt: Counter = Counter()
        flips = 0
        confirmed_to_single = 0
        for pfr_id, yr, wits, vals in rows:
            d = dict(zip(wits, vals))
            n_verdict, n_agree, n_dis, n_votes = _naive_vote(d, tol)
            c_verdict, _, c_agree, c_dis, inconsistent = WV._vote(d, tol, int(yr), fam)
            # verdict CLASS labels carry the confirmation count -- "UNANIMOUS x3" and
            # "UNANIMOUS x2" are different claims, and the flip from either to SINGLE
            # is exactly the loss of independent confirmation being reported.
            n_label = f"{n_verdict}x{n_agree}" if n_verdict in ("UNANIMOUS", "MAJORITY") else n_verdict
            c_label = f"{c_verdict}x{c_agree}" if c_verdict in ("UNANIMOUS", "MAJORITY") else c_verdict
            naive_cnt[n_label] += 1
            coll_cnt[c_label] += 1
            if n_label != c_label:
                flips += 1
                flip_matrix[(stat, n_label, c_label)] += 1
                if n_verdict in ("UNANIMOUS", "MAJORITY") and n_agree >= 2 \
                        and c_verdict == "SINGLE":
                    confirmed_to_single += 1
                if len(samples) < sample_limit:
                    samples.append(dict(
                        stat=stat, pfr_id=pfr_id, year=int(yr),
                        naive=n_label, collapsed=c_label,
                        intra_inconsistent=";".join(inconsistent),
                        witnesses=json.dumps({k: v for k, v in d.items() if k != "v26"}),
                    ))
        per_stat[stat] = {
            "cells": len(rows),
            "naive": dict(naive_cnt),
            "collapsed": dict(coll_cnt),
            "flips": flips,
            "flip_rate": round(flips / len(rows), 6) if rows else None,
            "confirmed_to_single": confirmed_to_single,
        }
    con.close()
    total_cells = sum(s["cells"] for s in per_stat.values())
    total_flips = sum(s["flips"] for s in per_stat.values())
    total_cts = sum(s["confirmed_to_single"] for s in per_stat.values())
    return {
        "contract": "lineage_roots.v1.json",
        "regimes": {"naive": "one vote per primary source (copies vote)",
                    "collapsed": "one vote per independent root (root_of; "
                                 "intra-root disagreement abstains)"},
        "totals": {
            "stats": len(per_stat),
            "cells": total_cells,
            "flips": total_flips,
            "flip_rate": round(total_flips / total_cells, 6) if total_cells else None,
            "confirmed_to_single": total_cts,
        },
        "flip_matrix": [
            {"stat": k[0], "naive": k[1], "collapsed": k[2], "cells": v}
            for k, v in sorted(flip_matrix.items(), key=lambda kv: -kv[1])
        ],
        "per_stat": per_stat,
        "_samples": samples,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stat", action="append", default=None,
                    help="restrict to specific stats (repeatable)")
    ap.add_argument("--limit", type=int, default=2000, help="max sample rows")
    ap.add_argument("--no-write", action="store_true",
                    help="print only; do not touch the committed summary")
    a = ap.parse_args()
    rep = run(stats=a.stat, sample_limit=a.limit)
    samples = rep.pop("_samples")
    t = rep["totals"]
    print(f"cells={t['cells']:,}  flips={t['flips']:,} ({t['flip_rate']:.2%})  "
          f"confirmed->single={t['confirmed_to_single']:,}")
    for row in rep["flip_matrix"][:15]:
        print(f"  {row['stat']:24s} {row['naive']:>14s} -> {row['collapsed']:<14s} {row['cells']:>8,}")
    if a.no_write:
        return 0
    from .recon_common import lane_dir, new_run_dir, utc_stamp
    rep["generated_utc"] = utc_stamp()
    if a.stat:
        rep["scope"] = sorted(a.stat)  # partial runs are labeled, never mistaken for full
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(rep, f, indent=2)
    out = lane_dir(new_run_dir(), LANE)
    if samples:
        import csv
        with open(os.path.join(out, "flip_samples.csv"), "w", newline="",
                  encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(samples[0]))
            w.writeheader()
            w.writerows(samples)
    print(f"summary -> {os.path.abspath(SUMMARY_PATH)}")
    print(f"samples -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
