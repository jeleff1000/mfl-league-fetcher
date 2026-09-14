#!/usr/bin/env python3
"""
Enforce the rule: WE EITHER KNOW THE FORMULA OR WE DON'T.

Cross-checks the live advanced atoms emitted by the aggregation
(aggregate_merged_pbp_for_supertable_audit.STAT_COLUMNS) against
docs/advanced-stats-registry.json. Fails (exit 1) if:

  1. a BLOCKED metric (formula=null, e.g. xfp/yprr/def_epa_player) is emitted as an
     atom — i.e. someone approximated something we said we can't compute; or
  2. an advanced atom is emitted with NO registry entry / a null formula — i.e. an
     unregistered (potentially hallucinated) formula shipped without a cited source.

Also prints the scope matrix (public_from -> we_extend_to, baseline level, status) and
the explicit do-not-build list, so our limitations are always visible.

    python scripts/check_stats_registry.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
from scripts.aggregate_merged_pbp_for_supertable_audit import STAT_COLUMNS  # noqa: E402

REGISTRY = REPO / "docs" / "advanced-stats-registry.json"

# Advanced atoms are formula-hallucination-prone and MUST be registered. Base counting
# atoms (attempts, completions, fg_made, ...) are low-risk and exempt from the coverage
# requirement (they still may appear in the registry). Map atom -> registry metric where
# the emitted column name differs from the public metric name.
ADVANCED_MARKERS = ("epa", "wpa", "cpoe", "air_yards", "explosive", "rz_", "_2pt_", "pacr", "racr")
ATOM_TO_METRIC = {
    "passing_cpoe_sum": "passing_cpoe",
    "passing_cpoe_n": "passing_cpoe",
}


def is_advanced(atom: str) -> bool:
    return any(m in atom for m in ADVANCED_MARKERS)


def main() -> int:
    reg = json.loads(REGISTRY.read_text(encoding="utf-8"))["stats"]
    by_name = {s["name"]: s for s in reg}
    blocked = {s["name"] for s in reg if s.get("formula") is None}
    emitted = set(STAT_COLUMNS)

    failures: list[str] = []

    # (1) no blocked metric may be emitted as an atom
    leaked = sorted(blocked & emitted)
    for name in leaked:
        failures.append(f"BLOCKED metric '{name}' is emitted as an atom — {by_name[name].get('reason', 'no known formula')}")

    # (2) every advanced atom must map to a registry entry with a non-null formula
    for atom in sorted(a for a in emitted if is_advanced(a)):
        metric = ATOM_TO_METRIC.get(atom, atom)
        entry = by_name.get(metric)
        if entry is None:
            failures.append(f"advanced atom '{atom}' has NO registry entry — register its formula+source before shipping")
        elif entry.get("formula") is None:
            failures.append(f"advanced atom '{atom}' maps to a null-formula (blocked) metric '{metric}'")

    # ---- report: scope matrix ----
    order = {"validated": 0, "by_construction": 1, "planned": 2, "blocked_no_formula": 3}
    rows = sorted(reg, key=lambda s: (order.get(s["status"], 9), s.get("family", ""), s["name"]))
    print(f"{'metric':<30} {'family':<14} {'lvl':<4} {'public->extend':<16} {'status':<18} formula?")
    print("-" * 100)
    for s in rows:
        yrs = f"{s.get('public_from')}->{s.get('we_extend_to')}" if s.get("formula") else "-"
        fk = "KNOWN" if s.get("formula") else "UNKNOWN"
        print(f"{s['name']:<30} {s.get('family',''):<14} {str(s.get('baseline_level') or '-'):<4} {yrs:<16} {s['status']:<18} {fk}")

    print("\n" + "=" * 60)
    print(f"  emitted atoms: {len(emitted)}  |  advanced emitted: {sum(1 for a in emitted if is_advanced(a))}")
    print(f"  registry: {len(reg)} metrics  |  BLOCKED (do-not-build): {sorted(blocked)}")
    if failures:
        print(f"\n  FAIL ({len(failures)}):")
        for f in failures:
            print(f"    - {f}")
        print("=" * 60)
        return 1
    print("  OK — every advanced atom has a known, sourced formula; no blocked metric emitted.")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
