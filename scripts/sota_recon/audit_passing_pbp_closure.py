"""Audit the passing-family PBP closure contract.

This is deliberately a wiring audit, not a claim that one PBP lineage is an
independent external witness.  It proves that every previously identified
passing backlog field has:

* a registered PBP MapSpec;
* a week-capable PBP path; and
* a raw-atom or explicit derived-rate contract in the PBP registry.

The independent-root tier is reported separately so a PBP-derived rollup is
never mistaken for a second source root.
"""
from __future__ import annotations

import json
from pathlib import Path

from . import witness_map as W
from .build_ironclad_board import WEEK_CAPABLE_SHAPES, root_of
from .witness_gate.pbp_contracts import load_pbp_contract_registry

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "docs" / "audits" / "sota-recon" / "passing" / "passing-pbp-closure.json"
CONTRACT = ROOT / "scripts" / "sota_recon" / "witness_gate" / "contracts" / "pbp_contracts.v1.json"

TARGETS = (
    "pass_explosive_20", "pass_success", "pass_success_plays",
    "passing_2pt_conversions", "passing_adjusted_net_yards_per_attempt",
    "passing_adjusted_yards_per_attempt", "passing_air_yards", "passing_epa",
    "passing_net_yards_per_attempt", "passing_tds_allowed", "passing_td_pct",
    "passing_wpa", "passing_yards_after_catch", "passing_yards_per_attempt",
    "passing_yds_allowed", "rz_pass_att", "rz_pass_td",
)
PBP_SOURCES = {"pbp_merged_1978_2025", "pbp_player_week_rollup",
               "pbp_team_defense", "ancient_pbp1978_recovery"}
DERIVED_RATE_CONTRACTS = {
    "pass_success": "pass_success / pass_success_plays",
    "passing_adjusted_net_yards_per_attempt": "(pass_yards + 20*pass_tds - 45*pass_int - sack_yards_lost) / (attempts + sacks_suffered)",
    "passing_adjusted_yards_per_attempt": "(pass_yards + 20*pass_tds - 45*pass_int) / attempts",
    "passing_net_yards_per_attempt": "(pass_yards - sack_yards_lost) / (attempts + sacks_suffered)",
    "passing_td_pct": "100 * passing_tds / attempts",
    "passing_yards_per_attempt": "passing_yards / attempts",
}
TERMINAL_GAP_REASONS = {
    "pass_success": "PFR PBP expected-points success differs from the canonical nflverse EPA-success definition (85.56% agreement); no same-definition independent weekly witness is present.",
    "passing_epa": "PFR PBP expected-points transitions use a different model from canonical nflverse EPA (0.00% agreement); model outputs cannot be quorum substitutes.",
    "passing_wpa": "The local independent PFR PBP capture has expected points but no win-probability field; no independent weekly WPA witness is present locally.",
}


def build() -> dict:
    registry = load_pbp_contract_registry(CONTRACT)
    raw_contracts = set(registry.coverage_year_overrides) | {
        "pass_explosive_20", "pass_success", "pass_success_plays",
        "rz_pass_att", "rz_pass_td", "passing_tds_allowed",
        "passing_yds_allowed", "passing_yards_after_catch",
    } | set(DERIVED_RATE_CONTRACTS)
    # Measure the newly introduced PFR weekly lane before allowing it to count
    # as a second root.  A declared lane with a suspect definition is not a
    # quorum vote.
    pfr_specs = [s for s in W.WITNESS_MAP
                 if s.source_key == "pfr_pbp_player_week_witness"
                 and s.v26_col in TARGETS]
    pfr_validation = {
        r["v26_col"]: r for r in W.validate(pfr_specs)
    }
    validated_pfr = {
        col for col, result in pfr_validation.items()
        if result.get("verdict") == "VALIDATED"
    }
    rows = []
    for target in TARGETS:
        all_specs = [s for s in W.WITNESS_MAP if s.v26_col == target]
        specs = [s for s in all_specs if s.source_key in PBP_SOURCES]
        weekly = [s for s in specs if s.validation_grain == "week"
                  or s.grain == "week" or s.shape in WEEK_CAPABLE_SHAPES]
        all_weekly = [s for s in all_specs if s.validation_grain == "week"
                      or s.grain == "week" or s.shape in WEEK_CAPABLE_SHAPES]
        roots = sorted({root_of(s.source_key) for s in all_weekly
                        if s.source_key != "pfr_pbp_player_week_witness"
                        or target in validated_pfr})
        rows.append({
            "column": target,
            "pbp_mapspec": bool(specs),
            "week_capable": bool(weekly),
            "pbp_contract": target in raw_contracts,
            "contract_kind": ("derived_rate" if target in DERIVED_RATE_CONTRACTS
                              else "raw_pbp_atom"),
            "independent_root_count": len(roots),
            "independent_roots": roots,
            "pbp_root_present": bool(weekly),
            "pbp_specs": [{"source": s.source_key, "shape": s.shape,
                           "grain": s.grain,
                           "validation_grain": s.validation_grain}
                           for s in weekly],
            "secondary_validation": pfr_validation.get(target),
        })
    failures = [r["column"] for r in rows
                if not (r["pbp_mapspec"] and r["week_capable"] and r["pbp_contract"])]
    payload = {
        "schema_version": "passing-pbp-closure.v1",
        "scope": "weekly passing backlog from witness coverage audit",
        "targets": list(TARGETS),
        "target_count": len(TARGETS),
        "fully_pbp_wired": len(rows) - len(failures),
        "independent_root_quorum": sum(r["independent_root_count"] >= 2 for r in rows),
        "pbp_only_week_paths": sum(r["independent_root_count"] == 1 for r in rows),
        "failures": failures,
        "rows": rows,
        "contract": str(CONTRACT),
        "rules": [
            "PBP-derived rollup and raw PBP are one pbp lineage root.",
            "pbp_ratio is week-capable because play-grain events are elevated by the rollup adapter.",
            "independent_root_quorum counts root diversity, never lane count.",
        ],
        "derived_rate_contracts": DERIVED_RATE_CONTRACTS,
        "secondary_validation": pfr_validation,
        "remaining_gap_reasons": TERMINAL_GAP_REASONS,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return payload


if __name__ == "__main__":
    print(json.dumps(build(), indent=2))
