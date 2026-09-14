"""
sota_recon/derivation_dag.py  --  O.6: the INDEPENDENT-DERIVATION DAG (§16.1 / §21.1)

Per canonical stat x era: every derivation path with its ROOT SET, composed from
  direct witnesses     voting roots per (stat x era) from the root-diversity map
                       (lineage_roots contract + licensed witness map)
  identity-derived     enforceable (CONFIRMED / CONDITIONAL-in-scope) eq edges from
                       relationship_edges.v1.json: lhs derivable from rhs components
                       whose OWN roots supply the path's root set; 2-term identities
                       also derive each component by rearrangement
  aggregation-derived  enforceable R6 grain edges (season/career from weekly) --
                       grain redundancy, root set unchanged

Derivation-path diversity (count of DISTINCT root sets across paths) feeds Atom
Certificates (§21.1): a stat whose only paths all bottom out in one root is still
single-root no matter how many formulas reach it -- the DAG makes that honest.

Burn-down (§16.1 root-poorest-first): the scoreboard counter is
  single_root_cells_with_zero_enforceable_edges
-- single-root (stat x era) cells not yet touched by ANY enforceable relationship
edge in scope. For those cells the R1-R9 grid provides no honesty mechanism yet;
they are the priority queue, ordered root-poorest-first then by family.

Output: docs/derivation-dag.json (committed; scoreboard reads the counters).

Run:  python -m scripts.sota_recon.derivation_dag
"""

from __future__ import annotations

import json
import os
from collections import defaultdict

from . import relationship_edges as RE

DIVERSITY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                              "docs", "root-diversity-map.json")
SUMMARY_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                            "docs", "derivation-dag.json")

ERAS = ["pre1933", "1933_49", "1950_77", "1978_98", "1999_2025"]

# Structurally single-source cells: NGS tracking-model outputs have no possible
# relationship edge (model metrics, not event counts -- nothing sums, mirrors, or
# conserves them). EXCLUDED:model_metric closes their burn-down cells honestly
# instead of leaving them as forever-open queue rows (O.7 slice 2).
EXCLUDED_MODEL_METRIC_PREFIXES = ("ngs_",)

# EXCLUDED:definition_isolated (O.7 slice 3, receipt-gated): charting stats whose
# ONLY candidate relation was generated, run, and REFUTED as a definition mismatch
# (30-90% violation rates -- the two sides chart different events, e.g. blitzers-
# per-play vs plays-blitzed). The exclusion is honored ONLY while the named
# do-not-enforce edge exists in the committed contract (§7.5 rejection registry);
# if the receipt disappears the cell falls back to the queue -- never a silent
# exclusion. Cross-side run receipts: docs/relationship-verdicts.v1.json
# (2026-07-26; def_blitzes=passing_blitzed 3985/4454 violated, def_hurries
# 1328/4454, def_pressures 2039/4454, def_tackles_missed 3675/4454).
EXCLUDED_DEFINITION_ISOLATED = {
    "def_blitzes": "R5:team_week:def_blitzes=passing_blitzed@idp_rows",
    "passing_blitzed": "R5:team_week:def_blitzes=passing_blitzed@idp_rows",
    "def_hurries": "R5:team_week:def_hurries=passing_hurried@idp_rows",
    "passing_hurried": "R5:team_week:def_hurries=passing_hurried@idp_rows",
    "passing_pressured": "R5:team_week:def_pressures=passing_pressured@idp_rows",
    "rushing_broken_tackles":
        "R5:team_week:def_tackles_missed=rushing_broken_tackles+receiving_broken_tackles@idp_rows",
    "receiving_broken_tackles":
        "R5:team_week:def_tackles_missed=rushing_broken_tackles+receiving_broken_tackles@idp_rows",
}

# EXCLUDED:era_out_of_domain (O.7 slice 3): the stat has no recorded domain in the
# era -- the diversity-map cell sits on stray all-zero rows, not on data. Density
# receipts measured 2026-07-26 against the live release.
EXCLUDED_ERA_OUT_OF_DOMAIN = {
    ("sacks_suffered", "pre1933"): "3 non-null rows, all zero (sacks unrecorded pre-1933)",
    ("sack_yards_lost", "pre1933"): "3 non-null rows, all zero (sacks unrecorded pre-1933)",
}


def _load_diversity() -> dict:
    with open(os.path.abspath(DIVERSITY_PATH), encoding="utf-8") as f:
        return json.load(f)


def _rearrangements(edge: dict) -> list[tuple[str, tuple[str, ...], str]]:
    """Derivations an eq edge licenses: lhs from rhs, plus each component of a
    2-term identity from the others (a = b + c  =>  b = a - c, c = a - b;
    a = b - c  =>  b = a + c, c = b - a)."""
    lhs, rhs = edge["lhs"], tuple(edge["rhs"])
    outs = [(lhs, rhs, "forward")]
    weights = tuple(edge["weights"]) or tuple(1.0 for _ in rhs)
    if edge["op"] == "eq" and len(rhs) == 2 and set(weights) <= {1.0, -1.0}:
        b, c = rhs
        outs.append((b, (lhs, c), "rearranged"))
        outs.append((c, (lhs, b), "rearranged"))
    return outs


def build(edges_doc: dict | None = None, diversity: dict | None = None) -> dict:
    edges_doc = edges_doc if edges_doc is not None else RE.load()
    diversity = diversity if diversity is not None else _load_diversity()

    roots: dict[tuple[str, str], set[str]] = {}
    families: dict[str, str] = {}
    for cell in diversity["cells"]:
        roots[(cell["stat"], cell["era"])] = set(cell["voting_roots"])
        families[cell["stat"]] = cell["family"]
    witnessed_stats = sorted({s for s, _ in roots})

    enforceable = [e for e in edges_doc["edges"] if e["enforceable"]]
    row_eq = [e for e in enforceable
              if e["op"] == "eq" and e["grain"] == "player_week"]

    # stat -> era -> derivation entries (identity paths need every component
    # rooted in that era; unrooted components make the path BLOCKED, recorded)
    derives: dict[tuple[str, str], list[dict]] = defaultdict(list)
    edge_touch: dict[tuple[str, str], set[str]] = defaultdict(set)

    for e in enforceable:
        scope = [x for x in e["era_scope"] if x in ERAS] or \
                (ERAS if e["grain"].startswith("player_career") else [])
        for era in scope:
            for stat in {e["lhs"], *e["rhs"]}:
                if (stat, era) in roots:
                    edge_touch[(stat, era)].add(e["edge_id"])

    for e in row_eq:
        for target, comps, how in _rearrangements(e):
            if target not in families:
                continue
            for era in e["era_scope"]:
                if era not in ERAS:
                    continue
                comp_roots = [roots.get((c, era), set()) for c in comps]
                missing = [c for c, r in zip(comps, comp_roots) if not r]
                path = {
                    "path_type": "identity_derived",
                    "via_edge": e["edge_id"],
                    "direction": how,
                    "components": list(comps),
                    "root_set": sorted(set().union(*comp_roots)) if comp_roots else [],
                    "blocked_by_unrooted": sorted(missing),
                }
                derives[(target, era)].append(path)

    grain_edges: dict[str, list[str]] = defaultdict(list)
    for e in enforceable:
        if e["r_class"] == "R6":
            grain_edges[e["lhs"]].append(e["edge_id"])

    # receipt gate for definition-isolated exclusions + rejection-registry pointers
    do_not_enforce_ids = {e["edge_id"] for e in edges_doc["edges"]
                          if e.get("do_not_enforce")}
    dne_touch: dict[str, list[str]] = defaultdict(list)
    for e in edges_doc["edges"]:
        if e.get("do_not_enforce"):
            for stat in {e["lhs"], *[r.split(".")[-1] for r in e["rhs"]]}:
                dne_touch[stat].append(e["edge_id"])

    cells = []
    zero_edge_single_root = []
    for stat in witnessed_stats:
        for era in ERAS:
            if (stat, era) not in roots:
                continue
            direct = sorted(roots[(stat, era)])
            paths = [{"path_type": "direct_witness", "root_set": [r]} for r in direct]
            id_paths = [p for p in derives.get((stat, era), [])
                        if not p["blocked_by_unrooted"]]
            blocked = [p for p in derives.get((stat, era), [])
                       if p["blocked_by_unrooted"]]
            paths += id_paths
            root_sets = {tuple(p["root_set"]) for p in paths if p["root_set"]}
            # independent = a derivation whose root set adds no dependence on any
            # single direct root (i.e. differs from every singleton direct set)
            n_single = len(direct) == 1
            cell = {
                "stat": stat, "family": families[stat], "era": era,
                "direct_roots": direct,
                "n_direct_roots": len(direct),
                "identity_paths": id_paths,
                "blocked_paths": blocked,
                "grain_edges": sorted(grain_edges.get(stat, [])),
                "n_paths": len(paths),
                "distinct_root_sets": sorted(sorted(s) for s in root_sets),
                "derivation_diversity": len(root_sets),
                "single_root": n_single,
                "enforceable_edges_touching": sorted(edge_touch.get((stat, era), set())),
                "n_enforceable_edges": len(edge_touch.get((stat, era), set())),
            }
            cell["excluded_model_metric"] = stat.startswith(
                EXCLUDED_MODEL_METRIC_PREFIXES)
            di_edge = EXCLUDED_DEFINITION_ISOLATED.get(stat)
            cell["excluded_definition_isolated"] = bool(
                di_edge and di_edge in do_not_enforce_ids)
            if cell["excluded_definition_isolated"]:
                cell["definition_isolated_receipt"] = di_edge
            ood = EXCLUDED_ERA_OUT_OF_DOMAIN.get((stat, era))
            cell["excluded_era_out_of_domain"] = bool(ood)
            if ood:
                cell["era_out_of_domain_receipt"] = ood
            excluded_any = (cell["excluded_model_metric"]
                            or cell["excluded_definition_isolated"]
                            or cell["excluded_era_out_of_domain"])
            cells.append(cell)
            if n_single and cell["n_enforceable_edges"] == 0 and not excluded_any:
                zero_edge_single_root.append({
                    "stat": stat, "era": era, "family": families[stat],
                    "root": direct[0],
                    "do_not_enforce_edges_touching": sorted(set(dne_touch.get(stat, []))),
                })

    single_root_cells = [c for c in cells if c["single_root"]]
    burned = [c for c in single_root_cells if c["n_enforceable_edges"] > 0]

    def _excl(cls: str) -> int:
        return sum(1 for c in single_root_cells
                   if c[cls] and c["n_enforceable_edges"] == 0
                   and not any(c[o] for o in EXCLUSION_CLASSES[:EXCLUSION_CLASSES.index(cls)]))

    return {
        "contract_inputs": {
            "edges": "witness_gate/contracts/relationship_edges.v1.json",
            "diversity": "docs/root-diversity-map.json",
        },
        "counters": {
            "stat_era_cells": len(cells),
            "single_root_cells": len(single_root_cells),
            "single_root_cells_with_enforceable_edges": len(burned),
            "single_root_cells_excluded_model_metric": _excl("excluded_model_metric"),
            "single_root_cells_excluded_definition_isolated":
                _excl("excluded_definition_isolated"),
            "single_root_cells_excluded_era_out_of_domain":
                _excl("excluded_era_out_of_domain"),
            "single_root_cells_with_zero_enforceable_edges": len(zero_edge_single_root),
            "cells_with_identity_paths": sum(1 for c in cells if c["identity_paths"]),
            "cells_with_multi_rootset_diversity":
                sum(1 for c in cells if c["derivation_diversity"] >= 2),
        },
        "burn_down_queue_root_poorest_first": sorted(
            zero_edge_single_root, key=lambda x: (x["root"], x["family"], x["stat"],
                                                  x["era"])),
        "cells": cells,
    }


# exclusion precedence for counter partitioning (a cell counts once, first match)
EXCLUSION_CLASSES = ["excluded_model_metric", "excluded_definition_isolated",
                     "excluded_era_out_of_domain"]


def main() -> int:
    doc = build()
    from .recon_common import utc_stamp
    doc["generated_utc"] = utc_stamp()
    with open(os.path.abspath(SUMMARY_PATH), "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    c = doc["counters"]
    print(f"cells={c['stat_era_cells']}  single-root={c['single_root_cells']}  "
          f"burned (edge-covered)={c['single_root_cells_with_enforceable_edges']}  "
          f"REMAINING zero-edge={c['single_root_cells_with_zero_enforceable_edges']}")
    print(f"identity-path cells={c['cells_with_identity_paths']}  "
          f"multi-rootset diversity={c['cells_with_multi_rootset_diversity']}")
    print(f"dag -> {os.path.abspath(SUMMARY_PATH)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
