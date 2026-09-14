"""Merge full-stratum audit receipts into the per-column collision profile.

Phase B of docs/runbooks/supertable-collision-audit-plan-2026-08-01.md.  Reads the
per-source receipts written by audit_mapspec_full and produces:

* ``MAPPING_AUDIT_FULL.json`` — every spec row, merged, with disposition;
* a ``collision_profile`` keyed by (v26_col, target_plane): the columns Joe asked
  for — which mapped columns collide, in which class, witnessed by which sources
  and which INDEPENDENT lineage roots.

Dispositions are CANDIDATES.  Nothing here edits a plane; confirmed items enter
repair_queue with executable predicates.  A supertable conviction candidate needs
>=2 distinct lineage roots exceeding us the same way — PFR-descended witnesses
agreeing with each other is one vote, not several (lineage_roots.root_of is the
only resolver used).
"""
from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path

from .audit_mapspec_full import FULL_YEARS, OUT, RECEIPT_DIR
from .lineage_roots import root_of
from . import sources as S

MERGED = OUT / "MAPPING_AUDIT_FULL.json"

CONFLICT_CLASSES = {
    "SUPERTABLE_ZERO_SOURCE_POSITIVE", "SOURCE_ZERO_OR_BLANK_SUPERTABLE_POSITIVE",
    "SOURCE_EXCEEDS_SUPERTABLE", "SUPERTABLE_EXCEEDS_SOURCE",
    "TWO_SIDED_VALUE_CONFLICT", "VALUE_CONFLICT_UNDIRECTED", "MAPPING_DEFECT_SQL",
}


def _roots(source: str) -> list[str]:
    """Distinct lineage roots a source speaks under across its registry span.

    Era-split sources change root at the split year, so both endpoints are
    resolved; the profile records the SET, and the conviction rule counts roots
    from the union across sources.
    """
    src = S.registry()[source]
    lo = max(src.year_min, FULL_YEARS[0])
    hi = min(src.year_max, FULL_YEARS[1])
    return sorted({root_of(source, lo), root_of(source, hi)})


def disposition(row: dict) -> str:
    """Per-row DOMINANT candidate lane, by cell count.  A row can hold several lanes
    at once (99% agreement + 33 conflict cells + 191 backfill cells); the subordinate
    lanes stay visible in the cell fields and the profile totals — this label only
    names the biggest one.  Adjudicated faults are carried through, never re-derived."""
    fault = row.get("fault", "")
    if fault in {"SUPERTABLE_VALUE", "SUPERTABLE_GAP"}:
        return "KNOWN_FINDING_SUPERTABLE_FAULT"
    if fault == "SOURCE_DEFECT":
        return "KNOWN_FINDING_SOURCE_FAULT"
    if fault == "MAPPING_DEFECT":
        return "KNOWN_FINDING_MAPPING_FAULT"
    cls = row["conflict_class"]
    if cls == "MAPPING_DEFECT_SQL":
        return "FIX_VALUE_PATH"
    if cls == "NO_OVERLAP":
        return "NO_ACTION"
    conflict = row.get("value_conflict_cells") or 0
    backfill = row.get("backfill_cells") or 0
    if not conflict and not backfill:
        return "NO_ACTION"
    if backfill > conflict:
        return "BACKFILL_CANDIDATE"
    if cls == "SOURCE_ZERO_OR_BLANK_SUPERTABLE_POSITIVE":
        return "SOURCE_POLICY"
    return "ADJUDICATE"


def main() -> int:
    receipts = sorted(RECEIPT_DIR.glob("*.json"))
    if not receipts:
        raise SystemExit(f"no receipts in {RECEIPT_DIR}")
    rows: list[dict] = []
    for path in receipts:
        doc = json.loads(path.read_text(encoding="utf-8"))
        rows.extend(doc["rows"])

    for row in rows:
        row["lineage_roots"] = _roots(row["source"])
        row["disposition"] = disposition(row)

    profile: dict[tuple, dict] = {}
    for row in rows:
        key = (row["v26_col"], row["target_plane"])
        p = profile.setdefault(key, {
            "v26_col": row["v26_col"], "plane": row["target_plane"],
            "classes": Counter(), "sources": defaultdict(list),
            "roots_in_conflict": set(), "roots_witnessing": set(),
            "dispositions": Counter(), "cells": Counter(),
        })
        p["classes"][row["conflict_class"]] += 1
        p["sources"][row["conflict_class"]].append(row["source"])
        p["roots_witnessing"].update(row["lineage_roots"])
        p["dispositions"][row["disposition"]] += 1
        # CELL magnitudes are the signal; spec-level classes are only headlines.
        # Cells are summed ACROSS SOURCES, so one bad supertable cell witnessed by
        # three sources counts three times here — this is witness-observation count,
        # not distinct-defect count; distinct cells come from Phase C drill-down.
        p["cells"]["compared"] += row.get("n") or 0
        p["cells"]["value_conflict"] += row.get("value_conflict_cells") or 0
        p["cells"]["backfill"] += row.get("backfill_cells") or 0
        p["cells"]["stored_zero"] += row.get("stored_zero_conflict_cells") or 0
        p["cells"]["source_zero"] += row.get("source_zero_conflict_cells") or 0
        if (row.get("value_conflict_cells") or 0) > 0:
            p["roots_in_conflict"].update(row["lineage_roots"])

    out_profile = []
    for p in profile.values():
        cells = p["cells"]
        has_conflict = cells["value_conflict"] > 0
        # >=2 independent roots in conflict = the cross-root escalation candidate;
        # the DIRECTION still has to be confirmed row-by-row before conviction
        multi_root = len(p["roots_in_conflict"]) >= 2
        out_profile.append({
            "v26_col": p["v26_col"], "plane": p["plane"],
            "has_conflict": has_conflict,
            "has_backfill": cells["backfill"] > 0,
            "cells": dict(cells),
            "conflict_share": (round(cells["value_conflict"] / cells["compared"], 4)
                               if cells["compared"] else None),
            "class_counts": dict(p["classes"]),
            "sources_by_class": {k: sorted(set(v)) for k, v in p["sources"].items()},
            "lineage_roots_witnessing": sorted(p["roots_witnessing"]),
            "lineage_roots_in_conflict": sorted(p["roots_in_conflict"]),
            "cross_root_escalation": multi_root,
            "disposition_counts": dict(p["dispositions"]),
        })
    out_profile.sort(key=lambda r: (not r["has_conflict"],
                                    -(r["cells"]["value_conflict"] + r["cells"]["backfill"]),
                                    r["plane"], r["v26_col"]))

    payload = {
        "audit": "mapspec_vs_supertable_full_merged",
        "years": list(FULL_YEARS),
        "receipt_count": len(receipts),
        "sources": [p.stem for p in receipts],
        "spec_rows": len(rows),
        "class_counts": dict(Counter(r["conflict_class"] for r in rows)),
        "plane_counts": dict(Counter(r["target_plane"] for r in rows)),
        "disposition_counts": dict(Counter(r["disposition"] for r in rows)),
        "columns_total": len(out_profile),
        "columns_with_conflict": sum(r["has_conflict"] for r in out_profile),
        "columns_with_backfill": sum(r["has_backfill"] for r in out_profile),
        "columns_cross_root": sum(r["cross_root_escalation"] for r in out_profile),
        "cells_total": {k: sum(r["cells"][k] for r in out_profile)
                        for k in ("compared", "value_conflict", "backfill",
                                  "stored_zero", "source_zero")},
        "collision_profile": out_profile,
        "rows": rows,
        "notes": [
            "Dispositions are candidates; nothing is edited by this receipt.",
            "cross_root_escalation marks >=2 independent lineage roots in conflict on the column; direction must be confirmed before conviction.",
            "Adjudicated faults are carried from disagreement_adjudications, never re-derived here.",
            "Naive denominator = spec_rows; the gated numerator for any repair is the per-row informative_n after zero-base removal.",
        ],
    }
    MERGED.write_text(json.dumps(payload, indent=1), encoding="utf-8")
    print(json.dumps({k: payload[k] for k in (
        "receipt_count", "spec_rows", "class_counts", "plane_counts",
        "disposition_counts", "columns_total", "columns_with_conflict",
        "columns_with_backfill", "columns_cross_root", "cells_total")}, indent=2))
    print(f"merged -> {MERGED}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
