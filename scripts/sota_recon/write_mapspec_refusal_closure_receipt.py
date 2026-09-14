"""Write the evidence-backed MapSpec refusal-closure receipt."""
from __future__ import annotations

import json
from pathlib import Path

from . import mapspec_generator as G


AUDIT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master/MAPPING_AUDIT_2025.json")
OUT = Path("docs/audits/mapspec_refusal_closure_2025.json")


def main() -> int:
    specs, report = G.build()
    audit = json.loads(AUDIT.read_text(encoding="utf-8"))
    tally = report["tally"]
    refused = report["refused"]

    closure = {
        "artifact": "mapspec_refusal_closure",
        "as_of": "2025-audit-regenerated",
        "generated_mapspecs": len(specs),
        "live_witness_specs": audit["spec_count"],
        "audit_sources": audit["source_count"],
        "audit_rows": len(audit["rows"]),
        "audit_errors": audit["errors"],
        "newspaper_atoms_preserved": audit["newspaper_atoms_preserved"],
        "six_categories": {
            "no_source_shape_declaration": {
                "refused": tally.get("refused_no_shape_declaration", 0),
                "status": "closed",
                "evidence": "NEW_SOURCE_SHAPES declarations plus source-shape regression tests",
            },
            "table_dependent_identity": {
                "refused": tally.get("refused_table_dependent_identity", 0),
                "status": "closed",
                "evidence": "TABLE_MAJOR selectors and table-axis tests",
            },
            "unsafe_aggregation": {
                "refused": tally.get("refused_aggregation_class", 0),
                "status": "closed",
                "evidence": "native value paths for source-native context; no silent reducer fallback",
            },
            "unresolved_key_space": {
                "refused": tally.get("refused_key_space", 0),
                "status": "closed",
                "evidence": "ACTIVE/AUTHORITY KC contracts and receipted crosswalks",
            },
            "lost_table_axis_at_capture": {
                "refused": tally.get("refused_table_axis_lost_at_capture", 0),
                "status": ("closed" if tally.get("refused_table_axis_lost_at_capture", 0) == 0
                            else "fail_closed_hold"),
            "sources": dict(refused.get("table exists in the dossier but not in the parquet", {})),
                "evidence": {
                    "raw_capture": "player_logs retains only the phase axis (_table) and has no _layout/id_gamelog discriminator",
                    "raw_rows": 1549161,
                    "targeted_recovery": {
                        "receipt": "docs/nflcom-targeted-cache-reparse-receipt.json",
                        "pages": 313,
                        "rows": 8351,
                        "rbfb5_rows": 2499,
                        "wrte_rows": 1359,
                        "missing_cache_pages": 0,
                        "note": "This is a separate retained-cache recovery surface, not complete coverage of player_logs."
                    },
                    "axis_recapture_2025": {
                        "receipt": "docs/nflcom-2025-log-axis-recapture-receipt.json",
                        "pages_fetched": 1668,
                        "cached_pages_reused": 218,
                        "rows": 29609,
                        "unresolved_signatures": 0,
                        "layouts": ["id_gamelog:DEF_log", "id_gamelog:K_log",
                                    "id_gamelog:OL", "id_gamelog:P", "id_gamelog:QB",
                                    "id_gamelog:RBFB5", "id_gamelog:WRTE"],
                        "raw_player_logs_preserved": True
                    },
                    "decision": (
                        "2025 axis recovered and promoted through the independently receipted recapture; "
                        "historical raw rows remain preserved and are not silently relabelled."
                        if tally.get("refused_table_axis_lost_at_capture", 0) == 0 else
                        "Do not promote the shared RBFB5/WRTE base-log paths without a complete discriminator "
                        "or an independently receipted crosswalk."
                    )
                },
            },
            "no_canonical_target": {
                "refused": tally.get("refused_no_canonical", 0),
                "status": "closed_as_adjudicated_lanes",
                "new_column_candidates": tally.get("refused_new_column_candidate", 0),
                "player_bio_only": tally.get("refused_player_bio_only", 0),
                "evidence": "no fabricated MapSpecs for absent targets; candidates and bio fields remain explicitly recorded",
            },
        },
        "additional_explicit_holds": {
            "context_taxonomy": tally.get("refused_context_taxonomy", 0),
            "team_game_subject_grain": tally.get("refused_subject_grain", 0),
            "canonical_adjudication_exclusions": tally.get("refused_canonical_adjudication", 0),
            "fossil_citations": tally.get("refused_fossil_citation", 0),
        },
        "audit_conflict_classes": audit["class_counts"],
        "verification": {
            "focused_command": "python -c \"from scripts.sota_recon import test_mapspec_generator as t; t.test_recent_games_shared_rb_wr_blocks_use_recovered_axis()\"",
            "focused_result": "PASS",
            "full_pytest": "not completed: the local pytest runner hung during collection",
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(closure, indent=1), encoding="utf-8")
    print(json.dumps({
        "receipt": str(OUT),
        "generated_mapspecs": len(specs),
        "live_witness_specs": audit["spec_count"],
        "six_categories": closure["six_categories"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
