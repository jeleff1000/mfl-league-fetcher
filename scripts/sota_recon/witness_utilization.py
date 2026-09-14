"""
sota_recon/witness_utilization.py  --  LANE: held-but-unwired witness audit (WS4b, Phase 2).

The scorecard's utilization gate: every witness-capable dataset we HOLD on the lake must be
wired into the source registry. This lane makes that systematic instead of anecdotal -- it
diffs the lake's own DATA_CATALOG (canonical-tier NFL logical tables) against the paths
`sources.py` registers, and reports what is held but unwired.

Not everything canonical is a witness (logs, manifests, our own derived outputs), so
non-witness categories are excluded by rule, and each remaining unwired entry is a work
item: register it, or record in the ledger why it cannot witness anything.

    python -m scripts.sota_recon.witness_utilization
"""
from __future__ import annotations

import argparse
import json
import os

from .sources import registry

CATALOG = r"D:\league-history-data\DATA_CATALOG.json"

# canonical-tier categories/paths that are not witnesses by construction
EXCLUDE_PREFIXES = (
    "nfl/releases", "nfl/derived", "nfl/staging", "nfl/tmp", "nfl/logs",
    "nfl/artifacts", "nfl/fly_snapshots", "nfl/fly_downloads", "nfl/ops_cache",
    "nfl/test_fixtures", "nfl/tools", "nfl/curated",  # curated = our review artifacts
    "cfb/",  # college domain -- context, not NFL stat witnesses
)

# held but deliberately NOT wired -- each entry needs a reason (fail-closed: anything
# neither wired nor listed here counts against utilization)
DOCUMENTED_NON_WITNESS = {
    "nfl/ops_data": "our own upload staging + ancient upsert patches -- not independent",
    "nfl/raw/legacy_supertable_backup_sources":
        "legacy export copies; the one diff-context file is registered as legacy_supertable",
    "nfl/raw/pfa/player_index_source_leads":
        "acquisition leads, not stats; PFA stat facts live in the ancient bundle + Fly tables",
    "nfl/raw/newspaper_archives":
        "source images/lineage for the WS7f sampled audit -- human-verification tier, not cell diffs",
    "nfl/raw/stathead/generated/pbp_backfill_1978_1998":
        "superseded input to pbp_merged_1978_2025 (registered); not an independent witness",
    "nfl/raw/stathead/generated/_archives": "archived intermediate runs",
    "nfl/raw/pfr/players/tables/sim_scores": "PFR similarity scores -- model output, no atom value",
    "nfl/raw/pfr/players/tables/all_pro": "honors context; may later witness award-flag columns",
    "nfl/raw/pfr/players/tables/ol_penalties": "OL penalty context -- no v26 column yet",
    "nfl/raw/pfr/players/tables/player_fantasy":
        "duplicate of fantasy table type (registered as pfr_player_fantasy)",
    "nfl/raw/pfr/cache": "scrape cache; master schedule inside is registered directly",
    "nfl/raw/pfr/context": "draft/combine context; combine is registered directly",
    "nfl/raw/pfr/players/pfr_players_probe": "scrape probe sample",
    "nfl/raw/pfr/players/pfr_players_smoke": "scrape smoke sample",
}


def run(verbose: bool = False) -> dict:
    cat = json.load(open(CATALOG, encoding="utf-8"))
    lake_root = cat["root"].rstrip("/")

    reg_paths = [os.path.normpath(s.path).replace("\\", "/").lower()
                 for s in registry().values()]

    def wired(logical: str) -> bool:
        full = f"{lake_root}/{logical}".replace("\\", "/").lower()
        # a logical table is wired if any registered source path sits under it (or equals it)
        return any(p.startswith(full + "/") or os.path.dirname(p) == full or p == full
                   for p in reg_paths)

    held, unwired, documented = [], [], []
    for t in cat["tables"]:
        if t["tier"] != "canonical" or t["sport"] != "nfl":
            continue
        logical = t["logical_table"]
        if any(logical.startswith(x) or f"/{x}" in logical for x in EXCLUDE_PREFIXES):
            continue
        held.append(logical)
        doc = next((reason for pfx, reason in DOCUMENTED_NON_WITNESS.items()
                    if logical == pfx or logical.startswith(pfx + "/")), None)
        if doc:
            documented.append({"logical_table": logical, "reason": doc})
        elif not wired(logical):
            unwired.append({"logical_table": logical, "rows": t.get("rows"),
                            "has_combined": t.get("has_combined")})

    denom = len(held) - len(documented)
    util = 1 - len(unwired) / denom if denom else 1.0
    return {"held_witness_candidates": len(held), "documented_non_witness": len(documented),
            "unwired": len(unwired), "utilization": round(util, 3),
            "unwired_list": unwired, "documented_list": documented}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-v", "--verbose", action="store_true")
    a = ap.parse_args()
    r = run(a.verbose)
    wired_n = r["held_witness_candidates"] - r["documented_non_witness"] - r["unwired"]
    print(f"WITNESS UTILIZATION: {r['utilization']:.1%} "
          f"({wired_n} wired / {r['unwired']} unwired / "
          f"{r['documented_non_witness']} documented-non-witness "
          f"of {r['held_witness_candidates']} candidates)")
    for u in sorted(r["unwired_list"], key=lambda x: -(x["rows"] or 0)):
        print(f"  UNWIRED  {u['logical_table']:70s} rows={u['rows'] or '?':>12} "
              f"combined={u['has_combined']}")
    if a.verbose:
        for d in r["documented_list"]:
            print(f"  DOCUMENTED {d['logical_table']:68s} {d['reason']}")
