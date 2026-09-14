"""Reconcile the complete artifact inventory with extracted candidates and cache audit.

This is a fail-closed gate.  It does not mutate a cache.  A promotion may only
follow when every manifest file has exactly one ledger disposition, every
selected candidate was extracted exactly once, and the read-only comparison
proves that the canonical schema and lineage invariants held.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path


EXPECTED_ARTIFACTS = 9_479
EXPECTED_FILES = 24_982


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--extract-ledger", type=Path, nargs="+", required=True)
    ap.add_argument("--comparison", type=Path, required=True)
    ap.add_argument("--structured-audit", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()

    manifest = read_json(args.manifest)
    files = manifest.get("files", [])
    if int(manifest.get("artifact_count", 0)) != EXPECTED_ARTIFACTS:
        raise SystemExit("manifest artifact count is not 9,479")
    if int(manifest.get("file_count", 0)) != EXPECTED_FILES or len(files) != EXPECTED_FILES:
        raise SystemExit("manifest file count is not 24,982")

    def key(row: dict) -> tuple[int, str]:
        return int(row["artifact_id"]), str(row["file"])

    expected = {key(row): row for row in files}
    if len(expected) != EXPECTED_FILES:
        raise SystemExit("manifest contains duplicate artifact/file keys")

    extracted: list[dict] = []
    for path in args.extract_ledger:
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                extracted.append(json.loads(line))
    by_key: dict[tuple[int, str], dict] = {}
    duplicate_keys: set[tuple[int, str]] = set()
    for row in extracted:
        k = key(row)
        if k in by_key:
            duplicate_keys.add(k)
        else:
            by_key[k] = row

    selected = {k for k, row in expected.items() if row.get("candidate")}
    actual = set(by_key)
    missing_selected = sorted(selected - actual)
    out_of_scope = sorted(actual - set(expected))
    extraction_failures = sorted(
        k for k in selected
        if by_key.get(k, {}).get("status") not in {
            "candidate_extracted", "structured_evidence_extracted"
        }
    )
    comparison = read_json(args.comparison)
    structured = read_json(args.structured_audit)
    cache_invariants = {
        "cache_mutated": bool(comparison.get("cache_mutated")),
        "new_lineage": bool(comparison.get("new_lineage")),
        "canonical_schema_unchanged": bool(comparison.get("canonical_schema_unchanged")),
    }
    complete = not missing_selected and not duplicate_keys and not out_of_scope and not extraction_failures
    unresolved_structured_payloads = int(structured.get("promotable_structured_rows", 0)) + int(structured.get("settings_rows", 0))
    promotion_ready = complete and unresolved_structured_payloads == 0 and not cache_invariants["cache_mutated"] and not cache_invariants["new_lineage"] and cache_invariants["canonical_schema_unchanged"]

    disposition_counts = Counter(row.get("disposition", "unknown") for row in files)
    status_counts = Counter(row.get("status", "missing") for row in by_key.values())
    report = {
        "complete_artifact_audit_run": manifest.get("complete_artifact_audit_run"),
        "manifest_artifacts": EXPECTED_ARTIFACTS,
        "manifest_files": EXPECTED_FILES,
        "selected_candidate_files": len(selected),
        "extracted_candidate_files": len(actual & selected),
        "missing_candidate_files": len(missing_selected),
        "duplicate_extracted_keys": len(duplicate_keys),
        "out_of_scope_extracted_keys": len(out_of_scope),
        "extraction_failures": len(extraction_failures),
        "disposition_counts": dict(sorted(disposition_counts.items())),
        "extraction_status_counts": dict(sorted(status_counts.items())),
        "cache_invariants": cache_invariants,
        "complete_file_reconciliation": complete,
        "promotion_ready": promotion_ready,
        "missing_candidate_keys": [list(k) for k in missing_selected[:100]],
        "duplicate_extracted_keys_sample": [list(k) for k in sorted(duplicate_keys)[:100]],
        "out_of_scope_extracted_keys_sample": [list(k) for k in sorted(out_of_scope)[:100]],
        "extraction_failure_keys_sample": [list(k) for k in extraction_failures[:100]],
        "structured_audit": {k: structured.get(k) for k in ("files", "direct_signal_files", "settings_payload_files", "promotable_structured_rows", "settings_rows")},
        "unresolved_structured_payloads": unresolved_structured_payloads,
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(report, sort_keys=True))
    if not complete:
        raise SystemExit("artifact/file reconciliation incomplete; promotion is forbidden")


if __name__ == "__main__":
    main()
