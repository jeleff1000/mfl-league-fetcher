"""
sota_recon/relationship_edges.py  --  O.6: the RELATIONSHIP-EDGE CONTRACT (§19.3)

Composes witness_gate/contracts/relationship_edges.v1.json from the mechanical
candidate space (relationship_candidates) joined with the committed verdict
receipts (docs/relationship-verdicts.v1.json). The composition is deterministic
and cheap, so the committed contract is regen-diff-tested (§18 law: load() ==
generate()); the heavy empirical runs live in relationship_verdicts.py and are
receipts, not code paths of this composer.

Edge semantics (proof-or-pending, §19):
  enforceable        verdict CONFIRMED or CONDITIONAL -- may be used as a recon
                     gate WITHIN its era/definition scope, nowhere else
  do_not_enforce     verdict REFUTED -- registered so nobody re-adds it later;
                     carries its counterexample
  open               UNDECIDABLE / PENDING_TEST / ESCALATED -- queued, unusable

Run:  python -m scripts.sota_recon.relationship_edges          # regenerate + write
"""

from __future__ import annotations

import argparse
import json
import os

CONTRACT_PATH = os.path.join(os.path.dirname(__file__), "witness_gate", "contracts",
                             "relationship_edges.v1.json")
VERDICTS_PATH = os.path.join(os.path.dirname(__file__), "..", "..",
                             "docs", "relationship-verdicts.v1.json")

VERDICT_VOCAB = {"CONFIRMED", "CONDITIONAL", "REFUTED", "UNDECIDABLE",
                 "PENDING_TEST", "ESCALATED"}
ENFORCEABLE = {"CONFIRMED", "CONDITIONAL"}


def _load_verdicts() -> dict:
    with open(os.path.abspath(VERDICTS_PATH), encoding="utf-8") as f:
        return json.load(f)


def generate(verdicts_doc: dict | None = None) -> dict:
    doc = verdicts_doc if verdicts_doc is not None else _load_verdicts()
    edges = []
    for v in doc["verdicts"]:
        edge = {
            "edge_id": v["cand_id"],
            "r_class": v["r_class"],
            "kind": v["kind"],
            "op": v["op"],
            "lhs": v["lhs"],
            "rhs": list(v["rhs"]),
            "weights": list(v["weights"]),
            "grain": v["grain"],
            "agg": v["agg"],
            "population": v.get("population", "all"),
            "proposal_basis": v["proposal_basis"],
            "verdict": v["verdict"],
            "era_scope": sorted(v.get("era_scope") or []),
            "enforceable": v["verdict"] in ENFORCEABLE,
        }
        if v.get("condition"):
            edge["condition"] = v["condition"]
        if v["verdict"] == "REFUTED":
            edge["do_not_enforce"] = True
            cx = v.get("counterexamples") or []
            if cx:
                edge["counterexample"] = cx[0]
        if v.get("deficit"):
            edge["deficit"] = v["deficit"]
        if v.get("escalation"):
            edge["escalation"] = v["escalation"]
        if v.get("near_miss_eras"):
            edge["near_miss_eras"] = sorted(v["near_miss_eras"])
        if v.get("note"):
            edge["note"] = v["note"]
        edges.append(edge)
    edges.sort(key=lambda e: (e["r_class"], e["edge_id"]))

    by_class_verdict: dict[str, dict[str, int]] = {}
    for e in edges:
        by_class_verdict.setdefault(e["r_class"], {}).setdefault(e["verdict"], 0)
        by_class_verdict[e["r_class"]][e["verdict"]] += 1
    return {
        "contract_version": "1",
        "generated_by": "scripts/sota_recon/relationship_edges.py",
        "authority": ("edge verdicts are receipts from relationship_verdicts.py runs "
                      "(docs/relationship-verdicts.v1.json); tolerance law = "
                      "stat_contracts tolerance_policy (§17.1, EXACT default)"),
        "verdicts_generated_utc": doc.get("generated_utc"),
        "release": doc.get("release"),
        "counts": {
            "edges": len(edges),
            "enforceable": sum(1 for e in edges if e["enforceable"]),
            "do_not_enforce": sum(1 for e in edges if e.get("do_not_enforce")),
            "open": sum(1 for e in edges
                        if e["verdict"] not in ENFORCEABLE and not e.get("do_not_enforce")),
            "by_class_verdict": {k: dict(sorted(v.items()))
                                 for k, v in sorted(by_class_verdict.items())},
            "near_miss_flagged": sum(1 for e in edges if e.get("near_miss_eras")),
            "mined": sum(1 for e in edges
                         if e["proposal_basis"] == "empirical_mined"),
        },
        "edges": edges,
    }


def load() -> dict:
    with open(CONTRACT_PATH, encoding="utf-8") as f:
        return json.load(f)


def validate(doc: dict) -> list[str]:
    problems = []
    seen = set()
    for e in doc.get("edges", []):
        if e["edge_id"] in seen:
            problems.append(f"duplicate edge_id {e['edge_id']}")
        seen.add(e["edge_id"])
        if e["verdict"] not in VERDICT_VOCAB:
            problems.append(f"{e['edge_id']}: unknown verdict {e['verdict']}")
        if e["enforceable"] != (e["verdict"] in ENFORCEABLE):
            problems.append(f"{e['edge_id']}: enforceable flag inconsistent")
        if e["verdict"] == "REFUTED" and not e.get("do_not_enforce"):
            problems.append(f"{e['edge_id']}: REFUTED without do_not_enforce")
        if e["verdict"] == "CONFIRMED" and not e["era_scope"]:
            problems.append(f"{e['edge_id']}: CONFIRMED with empty era_scope")
        if e.get("escalation") and e["verdict"] != "ESCALATED":
            problems.append(f"{e['edge_id']}: escalation set but verdict ran")
    return problems


def enforceable_edges(doc: dict | None = None) -> list[dict]:
    doc = doc if doc is not None else load()
    return [e for e in doc["edges"] if e["enforceable"]]


# ---------------------------------------------------------------------------
# consumable gate API (O.7 slice 4): recon lanes render enforceable edges into
# SQL gates from the contract instead of carrying ad-hoc relationship rows.
# ---------------------------------------------------------------------------

ERA_BOUNDS = {"pre1933": (1920, 1932), "1933_49": (1933, 1949),
              "1950_77": (1950, 1977), "1978_98": (1978, 1998),
              "1999_2025": (1999, 2025)}

ROW_TEST_KINDS = {"row_formula", "row_bound", "partition_sum", "row_rate"}


def era_filter_sql(edge: dict, year_col: str = "year") -> str:
    """Year predicate restricting a gate to the edge's proven era scope --
    an enforceable edge may be used WITHIN its scope, nowhere else."""
    eras = [e for e in (edge.get("era_scope") or []) if e in ERA_BOUNDS]
    if not eras or len(eras) == len(ERA_BOUNDS):
        return "1=1"
    parts = [f"({year_col} BETWEEN {ERA_BOUNDS[e][0]} AND {ERA_BOUNDS[e][1]})"
             for e in sorted(eras)]
    return "(" + " OR ".join(parts) + ")"


def row_gate_sql(edge: dict, eps: float) -> tuple[str, str] | None:
    """(guard, violation) SQL for a row-grain enforceable edge, honoring the
    edge's era scope, weights, and declared definition form (rate rounding).
    Returns None for edges this renderer cannot express (non-row grains,
    populations other than 'all')."""
    if edge["grain"] != "player_week" or edge.get("population", "all") != "all":
        return None
    lhs, rhs = edge["lhs"], edge["rhs"]
    if edge["kind"] == "rate_identity":
        num, den = rhs
        computed = f"({num} * 1.0 / {den})"
        form = ""
        cond = edge.get("condition") or {}
        if cond.get("type") == "definition":
            form = cond.get("definition", "").replace("stored form = ", "")
        expr = {"round1": f"ROUND({computed}, 1)",
                "round2": f"ROUND({computed}, 2)",
                "pct_round1": f"ROUND(100.0 * {computed}, 1)"}.get(form, computed)
        guard = f"{lhs} IS NOT NULL AND {num} IS NOT NULL AND {den} > 0"
        viol = f"ABS({lhs} - {expr}) > {eps}"
    else:
        weights = edge.get("weights") or [1.0] * len(rhs)
        expr = " + ".join(f"({w}) * {c}" for c, w in zip(rhs, weights))
        guard = " AND ".join(f"{c} IS NOT NULL" for c in [lhs, *rhs])
        if edge["op"] == "eq":
            viol = f"ABS({lhs} - ({expr})) > {eps}"
        else:
            viol = f"{lhs} > ({expr}) + {eps}"
    return f"({guard}) AND {era_filter_sql(edge)}", viol


def row_gate_edges(doc: dict | None = None) -> list[dict]:
    """The enforceable row-grain edges a recon lane can gate on directly."""
    return [e for e in enforceable_edges(doc)
            if e["grain"] == "player_week" and e.get("population", "all") == "all"
            and e["r_class"] in ("R1", "R2", "R7", "R9")]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true",
                    help="verify committed contract matches regeneration")
    a = ap.parse_args()
    doc = generate()
    problems = validate(doc)
    if problems:
        for p in problems:
            print("INVALID:", p)
        return 1
    if a.check:
        ok = load() == doc
        print("regen-diff:", "MATCH" if ok else "MISMATCH")
        return 0 if ok else 1
    with open(CONTRACT_PATH, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    c = doc["counts"]
    print(f"edges={c['edges']}  enforceable={c['enforceable']}  "
          f"do-not-enforce={c['do_not_enforce']}  open={c['open']}  mined={c['mined']}")
    print(f"contract -> {CONTRACT_PATH}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
