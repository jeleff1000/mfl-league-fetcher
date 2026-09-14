"""Merge the resumable 2025 MapSpec audit batch receipts with integrity checks."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

from . import audit_mapspec_2025 as audit
from .witness_map import WITNESS_MAP

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")


def main() -> int:
    batches = []
    for i in range(9):
        path = OUT / f"MAPPING_AUDIT_2025_batch_{i:02d}.json"
        if not path.exists():
            raise SystemExit(f"missing batch receipt: {path}")
        batches.append(json.loads(path.read_text(encoding="utf-8")))

    source_lists = [source for batch in batches for source in batch["sources"]]
    if len(source_lists) != len(set(source_lists)):
        raise SystemExit("duplicate source across batch receipts")
    errors = [error for batch in batches for error in batch["errors"]]
    rows = [row for batch in batches for row in batch["rows"]]
    spec_count = sum(batch["spec_count"] for batch in batches)
    if len(rows) != spec_count:
        raise SystemExit(f"denominator mismatch: rows={len(rows)} specs={spec_count}")

    # Source/canonical is unique in plane declaration even when a source has repeated
    # physical table rows. Recompute the plane from the current MapSpec declarations so
    # a season column is not mistaken for career merely because both planes contain it.
    # A canonical column can be witnessed at more than one source grain.  For
    # example, nflcom_player_career.carries has a season MapSpec and a recovered
    # Recent Games weekly MapSpec.  The source table/grain is part of the MapSpec
    # identity; keying only on (source, v26_col) falsely reports a mixed-plane
    # defect when both are intentionally live.
    plane_keys = {}
    for spec in WITNESS_MAP:
        key = (spec.source_key, spec.v26_col, spec.source_table,
               spec.validation_grain)
        plane = audit._plane_for({"validation_grain": spec.validation_grain,
                                  "grain": spec.grain})
        prior = plane_keys.setdefault(key, plane)
        if prior != plane:
            raise SystemExit(f"mixed validation planes for {key}: {prior}, {plane}")
    for row in rows:
        key = (row["source"], row["v26_col"], row.get("source_table"),
               row.get("validation_grain"))
        if key in plane_keys:
            row["target_plane"] = plane_keys[key]
            continue
        # Some pre-regeneration rows carry source_table but used the old
        # validation-grain label. If that table/column still resolves to one
        # live plane, use the live declaration rather than rejecting the row.
        table_candidates = {plane for (source, col, table, _grain), plane in plane_keys.items()
                            if source == row["source"] and col == row["v26_col"]
                            and table == row.get("source_table")}
        if len(table_candidates) == 1:
            row["target_plane"] = table_candidates.pop()
            continue
        # Older batch receipts predate source_table/validation_grain fields.
        # Resolve those rows only when the live MapSpec identity is unambiguous;
        # new multi-grain rows always carry the full declaration.
        candidates = {plane for (source, col, _table, _grain), plane in plane_keys.items()
                      if source == row["source"] and col == row["v26_col"]}
        if len(candidates) == 1 and not row.get("source_table") and not row.get("validation_grain"):
            row["target_plane"] = candidates.pop()
            continue
        raise SystemExit(f"row has no live MapSpec plane: {key}")

    receipt = {
        "audit": "mapspec_vs_supertable",
        "mode": "resumable_source_batches",
        "batch_count": len(batches),
        "years": [2025, 2025],
        "spec_count": spec_count,
        "source_count": len(source_lists),
        "rows": rows,
        "errors": errors,
        "class_counts": dict(Counter(row["conflict_class"] for row in rows)),
        "plane_counts": dict(Counter(row["target_plane"] for row in rows)),
        "newspaper_atoms_preserved": all(row["newspaper_atoms_preserved"] for row in rows),
        "batch_receipts": [batch["batch"] for batch in batches],
        "notes": [
            "Every executable MapSpec is represented exactly once by source batch.",
            "Source NULL/blank cells retain their denominator and are distinct from zero-direction classes.",
            "Newspaper atoms are immutable witness inputs; this audit does not delete or rewrite them.",
        ],
    }
    payload = json.dumps(receipt, indent=1)
    path = OUT / "MAPPING_AUDIT_2025_batched.json"
    path.write_text(payload, encoding="utf-8")
    # Replace the old monolithic receipt with the verified merged receipt so consumers
    # cannot accidentally read a pre-regeneration 1,986-spec audit.
    (OUT / "MAPPING_AUDIT_2025.json").write_text(payload, encoding="utf-8")
    print(json.dumps({
        "receipt": str(path),
        "spec_count": spec_count,
        "source_count": len(source_lists),
        "rows": len(rows),
        "errors": errors,
        "class_counts": receipt["class_counts"],
    }, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
