"""Write the explicit 2025 NULL/blank/zero/value conflict receipt."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
INPUT = OUT / "MAPPING_AUDIT_2025.json"
OUTPUT = OUT / "MAPPING_CONFLICTS_2025.json"


def status(row: dict) -> str:
    fault = row.get("fault", "")
    if fault == "SOURCE_DEFECT":
        return "SUPERTABLE_RIGHT_SOURCE_WRONG"
    if fault in {"SUPERTABLE_VALUE", "SUPERTABLE_GAP"}:
        return "SUPERTABLE_WRONG_SOURCE_RIGHT"
    if row.get("source_null_or_blank", 0):
        return "SOURCE_NULL_OR_BLANK_REQUIRES_SOURCE_POLICY"
    if row.get("source_zero_supertable_nonzero", 0):
        return "SOURCE_ZERO_OR_BLANK_AGAINST_SUPERTABLE_POSITIVE"
    if row.get("source_nonzero_supertable_zero", 0):
        return "SOURCE_POSITIVE_AGAINST_SUPERTABLE_ZERO"
    if row.get("n_we_hold_null", 0) or row.get("n_we_hold_no_row", 0):
        return "SUPERTABLE_NULL_OR_MISSING"
    if row.get("source_exceeds_us", 0) or row.get("we_exceed_source", 0):
        return "VALUE_CONFLICT_UNADJUDICATED"
    return "NO_DIRECTIONAL_CONFLICT"


def main() -> int:
    data = json.loads(INPUT.read_text(encoding="utf-8"))
    rows = []
    for row in data["rows"]:
        item = dict(row)
        item["adjudication_status"] = status(row)
        rows.append(item)
    payload = {
        "audit": "mapspec_conflicts_2025",
        "source_receipt": str(INPUT),
        "spec_count": data["spec_count"],
        "source_count": data["source_count"],
        "rows": rows,
        "status_counts": dict(Counter(r["adjudication_status"] for r in rows)),
        "conflict_counts": data["class_counts"],
        "notes": [
            "No row or atom is changed by this receipt.",
            "NULL/blank is kept separate from numeric zero whenever the validator exposes source_null_or_blank.",
            "SUPERTABLE_RIGHT_SOURCE_WRONG and SUPERTABLE_WRONG_SOURCE_RIGHT require an explicit fault basis; all other directional conflicts remain unadjudicated.",
            "Newspaper atoms are preserved and remain eligible witnesses.",
        ],
        "rightness_buckets": {
            "confirmed_supertable_wrong_source_right": {
                "row_count": sum(r.get("fault") in {"SUPERTABLE_VALUE", "SUPERTABLE_GAP"} for r in rows),
                "status": "confirmed",
                "meaning": "The super-table value or coverage is the defect; the mapped source witness is supported.",
            },
            "confirmed_source_wrong_supertable_right": {
                "row_count": sum(r.get("fault") == "SOURCE_DEFECT" for r in rows),
                "status": "confirmed",
                "meaning": "The source value is the defect; the super-table value is supported.",
            },
            "source_positive_against_supertable_zero": {
                "row_count": sum(r.get("source_nonzero_supertable_zero", 0) > 0 for r in rows),
                "status": "directional_only_unless_evidence_attached",
                "meaning": "A source positive meets a super-table zero. This is not automatically a source error or a table error.",
            },
            "source_zero_or_blank_against_supertable_positive": {
                "row_count": sum(r.get("source_zero_supertable_nonzero", 0) > 0 for r in rows),
                "status": "directional_only_unless_evidence_attached",
                "meaning": "A source zero/blank meets a super-table positive. This is not automatically a source error or a table error.",
            },
            "null_blank_policy_cases": {
                "row_count": sum(bool(r.get("source_null_or_blank", 0)) for r in rows),
                "status": "requires_source_policy",
                "meaning": "NULL/blank is retained as distinct from numeric zero; no imputation is performed here.",
            },
            "nflcom_2025_fumble_witness": {
                "status": "confirmed_supertable_gap_candidate",
                "rows": [
                    {
                        "source_table": "id_gamelog:RBFB5",
                        "column": "fumbles",
                        "compared": 136,
                        "source_positive_supertable_zero": 3,
                    },
                    {
                        "source_table": "id_gamelog:RBFB5",
                        "column": "fumbles_lost",
                        "compared": 136,
                        "source_positive_supertable_zero": 2,
                    },
                    {
                        "source_table": "id_gamelog:WRTE",
                        "column": "fumbles",
                        "compared": 164,
                        "source_positive_supertable_zero": 23,
                    },
                    {
                        "source_table": "id_gamelog:WRTE",
                        "column": "fumbles_lost",
                        "compared": 164,
                        "source_positive_supertable_zero": 9,
                    },
                ],
                "independent_witness": "pbp_player_week_rollup",
                "basis": "The PBP rollup independently matches the NFL.com positive fumble observations on the mismatching keys; preserve these as super-table gap candidates, not source defects.",
                "action": "Keep this as a schema-level conflict for adjudication; this audit performs no table mutation and does not delete newspaper atoms.",
            },
        },
    }
    OUTPUT.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    print(json.dumps({"receipt": str(OUTPUT), "status_counts": payload["status_counts"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
