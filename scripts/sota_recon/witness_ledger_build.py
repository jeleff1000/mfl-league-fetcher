"""Assemble the Witness Coverage Ledger page from the pivot receipt.

The ledger artifact (claude.ai/code/artifact/f35595c7-3a18-44d7-8343-f6187352b6a8) is
presentation over generated state; this builder makes it reproducible from the repo:

    python -m scripts.sota_recon.audit_mapspec_full        # (re)measure vs planes
    python -m scripts.sota_recon.merge_mapspec_full
    python -m scripts.sota_recon.witness_reach             # density starts
    python -m scripts.sota_recon.witness_pivot             # the pivot receipt
    python -m scripts.sota_recon.witness_ledger_build --out <path>.html
    # then republish <path>.html to the SAME artifact URL

Compacts witness_pivot.json into the embedded blob: per cell
[count, year, measured, kind(0 measured/1 declared/2 none), [[source, start],...],
"1930s:45% 1940s:62%..."] where the density string belongs to the earliest-starting
source in the cell.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

PIVOT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master/witness_pivot.json")
TEMPLATE = Path(__file__).with_name("witness_ledger_template.html")
KIND = {"measured": 0, "declared": 1, "none": 2}


def compact(pivot: dict) -> str:
    roots = pivot["root_order"]
    out = {"roots": roots, "labels": pivot["root_labels"],
           "cats": pivot["category_order"], "tabs": {}}
    for tab, rows in pivot["tabs"].items():
        orows = []
        for r in rows:
            cells = []
            for rt in roots:
                v = r["roots"].get(rt)
                if not v:
                    cells.append(0)
                    continue
                srcs = sorted(v["sources"].items(),
                              key=lambda kv: (kv[1]["start"] is None, kv[1]["start"] or 9999))
                dens = ""
                if srcs and srcs[0][1].get("density"):
                    dens = " ".join(f"{dec}s:{round(pct * 100)}%"
                                    for dec, pct in sorted(srcs[0][1]["density"].items())
                                    if pct > 0)
                cells.append([v["count"], v["year"] or 0, v["measured"],
                              KIND[v["year_kind"]],
                              [[k, s["start"] or 0] for k, s in srcs], dens])
            orows.append([r["column"], r["category"], cells, r["total"], r["min_year"]])
        out["tabs"][tab] = orows
    return json.dumps(out, separators=(",", ":"))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True, help="output .html path for the artifact")
    args = ap.parse_args()
    pivot = json.loads(PIVOT.read_text(encoding="utf-8"))
    template = TEMPLATE.read_text(encoding="utf-8")
    assert "__DATA__" in template, "template lost its data placeholder"
    Path(args.out).write_text(template.replace("__DATA__", compact(pivot)),
                              encoding="utf-8")
    print(json.dumps({"out": args.out, "tabs": list(pivot["tabs"])}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
