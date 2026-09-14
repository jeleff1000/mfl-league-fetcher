#!/usr/bin/env python3
"""Build the player column coverage ledger: one row per canonical physical column.

Joins four evidence layers, each authoritative for a different question:
  source-schema.json        WHICH columns exist            (inventory)
  source-coverage.json      WHETHER they hold data         (availability)
  witness-evidence.json     WHAT they are + era + lineage  (semantics)
  frontend-registration.json WHERE the product exposes them (exposure)

Dispositions are assigned from evidence, not from name guessing, and every
non-user-facing row carries a reason. Rows the evidence cannot dispose of land
in the review queue rather than being silently dropped.

    python scripts/player_column_coverage/build_ledger.py --out-dir docs/player-column-coverage
"""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

# Pipeline provenance stamps: written by a migration, never user-facing.
PROVENANCE_MARKERS = ("_repaired_at_", "_recomputed_at_", "_merged_at_", "_populated_at_")

# Placeholder labels used for columns that are never rendered as a named column.
PLACEHOLDER_DISPLAYS = {"—", "-", "", None}

JOIN_KEYS = {
    "NFL_player_id",
    "player_week",
    "player_id",
    "game_id",
    "pfr_id",
}


def load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def dispose(
    name: str,
    witness: dict[str, Any],
    coverage: dict[str, Any],
    registration: dict[str, Any] | None,
) -> tuple[str, str]:
    """Return (disposition, reason). Evidence order matters: identity and
    provenance first, then emptiness, then product exposure."""
    family = witness.get("family")
    empty = coverage.get("state") == "empty"

    if any(marker in name for marker in PROVENANCE_MARKERS):
        return "internal", "pipeline provenance stamp (migration timestamp)"
    if family == "flag_provenance":
        return "internal", "witness family=flag_provenance (pipeline bookkeeping)"
    if name in JOIN_KEYS:
        return "internal", "join key"
    if family == "identity" and registration is None:
        return "internal", "witness family=identity, not registered for display"

    if empty:
        if registration is not None:
            return "pipelineRequired", "registered for display but zero non-null rows in the canonical release"
        return "pipelineRequired", "zero non-null rows in the canonical release — nothing to expose until the pipeline populates it"

    if registration is not None:
        return "userFacing", "registered in COLUMN_REGISTRY"

    return "reviewQueue", f"populated ({coverage.get('nonNullCount', 0)} non-null rows), witness family={family}, but not registered in any frontend registry"


def build(
    schema: dict[str, Any],
    coverage: dict[str, Any],
    witness: dict[str, Any],
    registration: dict[str, Any],
    live_columns: set[str],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for entry in schema["columns"]:
        name = entry["name"]
        cov = coverage["columns"].get(name, {})
        wit = witness["columns"].get(name, {})
        reg = registration["columns"].get(name)
        disposition, reason = dispose(name, wit, cov, reg)
        era = wit.get("eraCoverage", {})
        relations = wit.get("relations", {})

        rows.append({
            "ordinal": entry["ordinal"],
            "sourceColumn": name,
            "sourceType": entry["type"],
            "family": wit.get("family"),
            "verdict": wit.get("verdict"),
            "witnessCount": wit.get("witnessCount"),
            "minYear": cov.get("minYear"),
            "maxYear": cov.get("maxYear"),
            "coverageState": cov.get("state"),
            "nonNullCount": cov.get("nonNullCount"),
            "interiorGapYears": len(cov.get("missingYears") or []),
            "positionsWithValues": cov.get("positionsWithValues") or [],
            "witnessEraMin": era.get("minYear"),
            "derivedFrom": relations.get("derivedFrom") or [],
            "componentOf": relations.get("componentOf") or [],
            "displayName": (reg or {}).get("display"),
            "registryCategory": (reg or {}).get("category"),
            "registryViews": (reg or {}).get("views") or [],
            "registryPositions": (reg or {}).get("positions") or [],
            "statFamily": (reg or {}).get("statFamily"),
            "sortable": (reg or {}).get("sortable", False),
            "inPlayerStatCatalog": (reg or {}).get("inPlayerStatCatalog", False),
            "inAdvancedPositionColumns": (reg or {}).get("inAdvancedPositionColumns") or [],
            "displayCollision": (reg or {}).get("displayCollision") or [],
            "liveOnFly": name in live_columns,
            "disposition": disposition,
            "reason": reason,
            "reviewStatus": "inferred",
        })

    by_disposition: dict[str, int] = {}
    by_family_unregistered: dict[str, int] = {}
    for row in rows:
        by_disposition[row["disposition"]] = by_disposition.get(row["disposition"], 0) + 1
        if row["disposition"] == "reviewQueue":
            key = str(row["family"])
            by_family_unregistered[key] = by_family_unregistered.get(key, 0) + 1

    # A display collision only matters when two columns compete for a REAL label.
    # `displayable: false` columns carry an em-dash placeholder, so several of them
    # share it by design — NFL_player_id and headshot_url are unrelated columns that
    # both use it. Counting those as collisions is a false positive.
    # Two columns sharing a display name is only a CONFLICT when they could both
    # serve the same request. Legitimate shared displays:
    #   - placeholder labels on non-displayable columns (em dash)
    #   - grain partitions: fumbles_lost is weekly-only, fum_lost is season/career,
    #     one label served by the grain-appropriate source
    #   - a physical column plus a non-sortable client-side fallback (fg_pct /
    #     fg_pct_derived), where the physical one always wins
    by_display: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if row["displayName"] and row["displayName"] not in PLACEHOLDER_DISPLAYS:
            by_display.setdefault(row["displayName"], []).append(row)

    registration = {name: rec for name, rec in registration.get("columns", {}).items()}
    for row in rows:
        if row["displayName"] in PLACEHOLDER_DISPLAYS:
            row["displayCollision"] = []
            continue
        conflicts = []
        for other in by_display.get(row["displayName"], []):
            if other["sourceColumn"] == row["sourceColumn"]:
                continue
            mine = set(row["registryViews"])
            theirs = set(other["registryViews"])
            if mine and theirs and not (mine & theirs):
                continue  # grain partition, not a conflict
            if not (row["sortable"] and other["sortable"]):
                continue  # a non-sortable fallback never competes for a sort
            conflicts.append(other["sourceColumn"])
        row["displayCollision"] = sorted(conflicts)

    review_queue = [row for row in rows if row["disposition"] == "reviewQueue"]
    pipeline_required = [row for row in rows if row["disposition"] == "pipelineRequired"]
    canonical_not_live = sorted(row["sourceColumn"] for row in rows if not row["liveOnFly"])
    collisions = [row for row in rows if row["displayCollision"]]

    return {
        "provenance": {
            "releaseId": schema["release_id"],
            "schemaFingerprint": schema["schema_fingerprint"],
            "evidenceFingerprint": witness.get("evidence_fingerprint"),
            "generatorVersion": "player-column-coverage/1.0.0",
        },
        "summary": {
            "canonicalColumns": len(rows),
            "byDisposition": dict(sorted(by_disposition.items())),
            "registeredColumns": sum(1 for row in rows if row["displayName"]),
            "reviewQueueCount": len(review_queue),
            "reviewQueueByFamily": dict(sorted(by_family_unregistered.items())),
            "pipelineRequiredCount": len(pipeline_required),
            "canonicalNotLiveCount": len(canonical_not_live),
            "canonicalNotLive": canonical_not_live,
            "displayCollisionCount": len(collisions),
            "emptyColumnCount": coverage["summary"]["emptyColumnCount"],
            "partialColumnCount": coverage["summary"]["partialColumnCount"],
        },
        "ledger": rows,
    }


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    fields = [
        "ordinal", "sourceColumn", "sourceType", "family", "verdict", "disposition",
        "displayName", "registryCategory", "statFamily", "minYear", "maxYear",
        "coverageState", "nonNullCount", "interiorGapYears", "liveOnFly", "sortable",
        "inPlayerStatCatalog", "reviewStatus", "reason",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def write_report(payload: dict[str, Any], path: Path) -> None:
    summary = payload["summary"]
    rows = payload["ledger"]
    lines: list[str] = []
    add = lines.append

    add("# Player Column Coverage Ledger")
    add("")
    add(f"Release `{payload['provenance']['releaseId']}`  ")
    add(f"Schema fingerprint `{payload['provenance']['schemaFingerprint'][:16]}`  ")
    add(f"Generator `{payload['provenance']['generatorVersion']}`")
    add("")
    add("One row per physical column in the canonical supertable. Inventory comes from the")
    add("parquet footer, availability from a first-party profile of the release, semantics")
    add("and era from the witness matrix, and exposure from the frontend registries.")
    add("")
    add("## Summary")
    add("")
    add("| Metric | Count |")
    add("|---|---|")
    add(f"| Canonical physical columns | {summary['canonicalColumns']} |")
    add(f"| Registered for display | {summary['registeredColumns']} |")
    add(f"| Review queue (populated, unregistered) | {summary['reviewQueueCount']} |")
    add(f"| Pipeline required (empty in release) | {summary['pipelineRequiredCount']} |")
    add(f"| Canonical but not live on Fly | {summary['canonicalNotLiveCount']} |")
    add(f"| Partial coverage (interior year gaps) | {summary['partialColumnCount']} |")
    add(f"| Display-name collisions | {summary['displayCollisionCount']} |")
    add("")
    add("## Exposure signal: what this ledger does and does not see")
    add("")
    add("`userFacing` currently means **registered in `COLUMN_REGISTRY`**. That is one of")
    add("several exposure paths, so the review queue contains a known false-positive class:")
    add("")
    add("- **Rank families (385 columns)** are exposed through the rankings/variant templating")
    add("  path (`variant-columns.ts` `positionRank`, `ppg_season_*`, `ppg_alltime_*`), not")
    add("  through `COLUMN_REGISTRY`. They appear here as unregistered because this ledger")
    add("  does not yet join that path — treat the rank count as *unverified*, not as a gap.")
    add("- **Scoring variants (`fpts_*`, `pts_*`)** are likewise selected by scoring-variant")
    add("  templating rather than static registration.")
    add("")
    add("Wiring those two paths into the exposure join is the next step before any coverage")
    add("percentage from this file is quoted as fact. The `atom` family review queue is the")
    add("portion most likely to be a genuine exposure gap.")
    add("")
    add("### By disposition")
    add("")
    add("| Disposition | Count |")
    add("|---|---|")
    for key, value in summary["byDisposition"].items():
        add(f"| {key} | {value} |")
    add("")
    add("### Review queue by witness family")
    add("")
    add("| Witness family | Unregistered columns |")
    add("|---|---|")
    for key, value in summary["reviewQueueByFamily"].items():
        add(f"| {key} | {value} |")
    add("")
    add("## Canonical columns absent from the live Fly schema")
    add("")
    add("These exist in the canonical release but the live schema index does not carry them,")
    add("so they cannot be exposed today regardless of disposition.")
    add("")
    for name in summary["canonicalNotLive"]:
        add(f"- `{name}`")
    add("")
    add("## Pipeline-required columns (zero non-null rows)")
    add("")
    for row in rows:
        if row["disposition"] == "pipelineRequired":
            add(f"- `{row['sourceColumn']}` — {row['reason']}")
    add("")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=Path("docs/player-column-coverage"))
    parser.add_argument(
        "--live-schema-index",
        type=Path,
        default=Path("frontend/src/generated/research-live-schema-index.json"),
    )
    args = parser.parse_args()
    out = args.out_dir

    schema = load(out / "source-schema.json")
    coverage = load(out / "source-coverage.json")
    witness = load(out / "witness-evidence.json")
    registration = load(out / "frontend-registration.json")

    live_columns: set[str] = set()
    index = load(args.live_schema_index)
    for table in index["tables"]:
        if table["table"].endswith("nfl_player_stats_all"):
            live_columns = {str(column[0]) for column in table["columns"]}

    payload = build(schema, coverage, witness, registration, live_columns)

    with (out / "ledger.json").open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")
    write_csv(payload["ledger"], out / "ledger.csv")
    write_report(payload, out / "report.md")

    queue = [row for row in payload["ledger"] if row["disposition"] == "reviewQueue"]
    with (out / "review-queue.json").open("w", encoding="utf-8", newline="\n") as handle:
        json.dump({"count": len(queue), "columns": queue}, handle, indent=2)
        handle.write("\n")

    summary = payload["summary"]
    print(f"ledger rows: {summary['canonicalColumns']}")
    print(f"  by disposition: {summary['byDisposition']}")
    print(f"  review queue: {summary['reviewQueueCount']} (by family {summary['reviewQueueByFamily']})")
    print(f"  canonical-not-live: {summary['canonicalNotLiveCount']}")
    print(f"  display collisions: {summary['displayCollisionCount']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
