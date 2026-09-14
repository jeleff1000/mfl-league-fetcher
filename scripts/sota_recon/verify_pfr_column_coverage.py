"""Verify that each inventoried PFR sample column appears in a receipt matrix."""
from __future__ import annotations

import json
from pathlib import Path

from .build_pfr_audit_coverage import artifacts_for

INV = Path("docs/pfr-lake-inventory.json")
AUDITS = Path("docs/audits")
OUT = AUDITS / "pfr-column-coverage-2025.json"


def matrix_columns(value: object) -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        if isinstance(value.get("column"), str):
            found.add(value["column"])
        for nested in value.values():
            found |= matrix_columns(nested)
    elif isinstance(value, list):
        for nested in value:
            found |= matrix_columns(nested)
    return found


def main() -> None:
    inventory = json.loads(INV.read_text(encoding="utf-8"))
    rows = []
    for item in inventory["tables"]:
        expected = set(item.get("sample", {}).get("columns", []))
        # Root containers are sampled from one child parquet; their canonical
        # schema is the compact manifest, not the child's football columns.
        if item["relative_path"] in {"boxscores", "context", "players"}:
            expected = {"table_id", "rows", "path"}
        artifacts = artifacts_for(item["relative_path"])
        receipt_columns: set[str] = set()
        missing_artifacts = []
        for artifact in artifacts:
            path = AUDITS / artifact
            if not path.exists():
                missing_artifacts.append(artifact)
                continue
            receipt_columns |= matrix_columns(json.loads(path.read_text(encoding="utf-8")))
        missing = sorted(expected - receipt_columns)
        rows.append({
            "relative_path": item["relative_path"],
            "registration_status": item["registration_status"],
            "expected_columns": len(expected),
            "receipted_columns": len(expected - set(missing)),
            "missing_columns": missing,
            "missing_artifacts": missing_artifacts,
            "status": "COMPLETE" if not missing and not missing_artifacts else "INCOMPLETE",
        })
    incomplete = [row for row in rows if row["status"] != "COMPLETE"]
    output = {
        "generated_at_utc": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
        "groups": len(rows),
        "complete_groups": len(rows) - len(incomplete),
        "incomplete_groups": len(incomplete),
        "rows": rows,
        "notes": ["This is a column-presence gate; semantic correctness and equality remain in the individual receipts."],
    }
    OUT.write_text(json.dumps(output, indent=2), encoding="utf-8")
    print({"written": str(OUT), "groups": len(rows), "complete_groups": len(rows) - len(incomplete), "incomplete_groups": len(incomplete)})
    if incomplete:
        for row in incomplete:
            print(row["relative_path"], row["missing_columns"], row["missing_artifacts"])


if __name__ == "__main__":
    main()
