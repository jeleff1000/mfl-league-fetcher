"""
sota_recon/root_diversity.py  --  O.5: the ROOT-DIVERSITY MAP (master plan §16.1)

Cross-examination only exists where >=2 INDEPENDENT roots witness a cell. This lane
computes the independent-root count per (stat x era) from the lineage_roots contract +
the licensed witness map, and enumerates the single-root population -- the exact cells
where the R1-R9 relationship grid is the ONLY honesty mechanism, so relationship closure
is prioritized root-poorest-first.

Two counts per (stat x era), never conflated:
  voting_roots     roots holding a LICENSED mapping whose source may vote
                   (witness_class == primary; licensed = validated mapping receipt)
  declared_roots   voting_roots + registered-but-non-voting root coverage (newspaper:
                   registered root, vote engine blocked until identity/conflict holds
                   clear) -- what diversity BECOMES when those holds clear

Eras follow the voting era split (lineage_roots pbp split + the classic recon bands):
pre1933 / 1933_49 / 1950_77 / 1978_98 / 1999_2025.

Output: docs/root-diversity-map.json (committed; scoreboard carries the counters).

Run:  python -m scripts.sota_recon.root_diversity
"""

from __future__ import annotations

import json
import os
from collections import defaultdict

from . import sources as S
from . import witness_map as WM
from .lineage_roots import root_of
from .witness_votes import _family_of

SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "root-diversity-map.json")

ERAS = [("pre1933", 1920, 1932), ("1933_49", 1933, 1949), ("1950_77", 1950, 1977),
        ("1978_98", 1978, 1998), ("1999_2025", 1999, 2025)]

# Newspaper witnesses stat families via long-grain stat cells, not per-stat licensed
# mappings; its declared-root contribution is family x era scoped (1920-40 bundle).
NEWSPAPER_DECLARED = {
    "source": "newspaper_player_cells",
    "year_min": 1920, "year_max": 1940,
    "families": {"passing", "rushing", "receiving", "kicking", "punting", "returns",
                 "defense", "fumbles", "general", "special_teams"},
}


def run() -> dict:
    reg = S.registry()
    lic = WM.licensed()
    # (stat, era) -> set of roots with a licensed primary mapping overlapping the era
    voting: dict[tuple[str, str], set[str]] = defaultdict(set)
    stats_seen: set[str] = set()
    for m in WM.WITNESS_MAP:
        if (m.source_key, m.v26_col) not in lic:
            continue
        src = reg[m.source_key]
        if src.witness_class != "primary":
            continue
        stats_seen.add(m.v26_col)
        fam = _family_of(m.v26_col)
        for era, lo, hi in ERAS:
            olo, ohi = max(lo, src.year_min), min(hi, src.year_max)
            if olo > ohi:
                continue
            # a window straddling the pbp split contributes BOTH roots to that era only
            # if the era itself straddles it -- eras here are split-aligned, so the
            # era-midpoint resolution is exact.
            voting[(m.v26_col, era)].add(root_of(m.source_key, (olo + ohi) // 2, fam))

    cells = []
    single_root = []
    zero_covered = []
    for stat in sorted(stats_seen):
        fam = _family_of(stat)
        for era, lo, hi in ERAS:
            v = sorted(voting.get((stat, era), set()))
            declared = set(v)
            np = NEWSPAPER_DECLARED
            if fam in np["families"] and not (hi < np["year_min"] or lo > np["year_max"]):
                declared.add("newspaper")
            cell = {"stat": stat, "family": fam, "era": era,
                    "voting_roots": v, "n_voting_roots": len(v),
                    "declared_roots": sorted(declared), "n_declared_roots": len(declared)}
            cells.append(cell)
            if len(v) == 1:
                single_root.append({"stat": stat, "era": era, "root": v[0]})
            elif len(v) == 0:
                zero_covered.append({"stat": stat, "era": era})

    n_multi = sum(1 for c in cells if c["n_voting_roots"] >= 2)
    return {
        "contract": "lineage_roots.v1.json",
        "licensed_source": "MAPPING_LICENSES.json (validated mappings only)",
        "eras": [e[0] for e in ERAS],
        "counters": {
            "stat_era_cells": len(cells),
            "multi_root_cells": n_multi,
            "single_root_cells": len(single_root),
            "zero_voting_root_cells": len(zero_covered),
            "cross_examined_share": round(n_multi / len(cells), 4) if cells else None,
        },
        "single_root_population": single_root,
        "zero_voting_root_population": zero_covered,
        "cells": cells,
    }


def main() -> int:
    doc = run()
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    c = doc["counters"]
    print(f"stat x era cells: {c['stat_era_cells']}  multi-root: {c['multi_root_cells']}  "
          f"single-root: {c['single_root_cells']}  zero-root: {c['zero_voting_root_cells']}  "
          f"cross-examined: {c['cross_examined_share']:.1%}")
    from collections import Counter
    by_era = Counter(x["era"] for x in doc["single_root_population"])
    print("single-root by era:", dict(by_era))
    print(f"map -> {os.path.abspath(SUMMARY_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
