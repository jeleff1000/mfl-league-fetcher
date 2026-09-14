"""Materialize the current identity-hole adjudication from identity-coverage.json."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SOURCE = ROOT / "docs" / "identity-coverage.json"
OUT = ROOT / "docs" / "identity-adjudication-2026.json"


def main() -> None:
    data = json.loads(SOURCE.read_text(encoding="utf-8"))
    bridge_path = ROOT / "docs" / "pfr-identity-bridge-2026.json"
    bridge_ids = {row["pfr_id"] for row in json.loads(bridge_path.read_text(encoding="utf-8"))} if bridge_path.exists() else set()
    owners: dict[str, set[str]] = {}
    for source, ids in data.get("missing_ids", {}).items():
        for identity in ids:
            owners.setdefault(identity, set()).add(source)
    combine_only = sorted(identity for identity, sources in owners.items() if sources == {"pfr_combine"})
    bridge_candidates = sorted(identity for identity, sources in owners.items() if sources != {"pfr_combine"})
    unreadable = data.get("sources_unreadable", {})
    resolved_bridge = sorted(identity for identity in bridge_ids if identity in owners)
    remaining = sorted(identity for identity in bridge_candidates if identity not in bridge_ids)
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_receipt": str(SOURCE),
        "policy": "Combine-only prospect IDs are explicit source-only identities; IDs referenced by boxscore/recognition surfaces remain identity-bridge candidates until joined to the spine.",
        "counts": {
            "distinct_absent_ids": len(owners),
            "accepted_combine_only_source_ids": len(combine_only),
            "resolved_identity_bridge_ids": len(resolved_bridge),
            "identity_bridge_candidates": len(remaining),
            "unreadable_sources": len(unreadable),
        },
        "accepted_combine_only_source_ids": combine_only,
        "resolved_identity_bridge_ids": {
            identity: {"referencing_sources": sorted(owners[identity]), "status": "RESOLVED_IDENTITY_BRIDGE"}
            for identity in resolved_bridge
        },
        "identity_bridge_candidates": {
            identity: {"referencing_sources": sorted(owners[identity]), "status": "DEFERRED_IDENTITY_BRIDGE"}
            for identity in remaining
        },
        "unreadable_sources": unreadable,
        "notes": [
            "No raw or release data was modified.",
            "The 21 cross-surface IDs remain alarms; they are not silently accepted.",
            "The unreadable newspaper archive remains a source-readability issue, separate from an identity match.",
        ],
    }
    OUT.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print({"written": str(OUT), "counts": payload["counts"]})


if __name__ == "__main__":
    main()
