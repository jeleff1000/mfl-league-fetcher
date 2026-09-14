# -*- coding: utf-8 -*-
"""Build a lake-wide classification ledger for witness discovery.

This scanner is intentionally read-only. It inventories stable lake artifacts and
catalog metadata, classifies each artifact by operational role, and joins paths to
the existing witness registry. Volatile release/staging/log trees are excluded by
default because they contain repeated build products; use --include-volatile for a
literal all-files scan.

Run:
    python -m scripts.sota_recon.witness_audit_v2.build_lake_classification
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

REPO = Path(__file__).resolve().parents[3]
LAKE = Path(r"D:\league-history-data\nfl")
CATALOG = Path(r"D:\league-history-data\_catalog")
DOCS = REPO / "docs"
RULE_VERSION = "2026-07-21.1"

VOLATILE_DIRS = {
    "releases", "staging", "tmp", "logs", "fly_downloads", "fly_snapshots",
}

# Trees whose contents are deliberately represented as opaque containers in the
# default bounded scan (huge, repetitive, or already cataloged elsewhere).
OPAQUE_RELATIVE_DIRS = {
    "raw/pfr/cache",
    "raw/pfr/boxscores",
    "raw/pfr/players",
    "raw/newspaper_archives",
    "raw/legacy_supertable_backup_sources",
    "ops_data/public",
    "curated/source_search_queues",
    "curated/source_search_execution_batches",
    "curated/source_search_local_evidence_packets",
    "curated/legacy_catalog",
    "curated/c_to_d_consolidation",
    "derived/validation",
}
DATA_SUFFIXES = {
    ".parquet", ".csv", ".tsv", ".json", ".jsonl", ".ndjson", ".feather",
    ".arrow", ".orc", ".avro", ".duckdb", ".db", ".sqlite", ".sqlite3",
}
TEXT_SUFFIXES = {".md", ".txt", ".sql", ".yml", ".yaml", ".toml", ".log"}
KNOWN_FAMILY_TOKENS = {
    "pfr": "PFR", "nflcom": "NFL.com", "newspaper": "Newspaper", "stathead": "Stathead",
    "nflverse": "nflverse", "pfa": "PFA", "statscrew": "StatsCrew", "ngs": "Next Gen Stats",
    "nextgen": "Next Gen Stats", "pbp": "PBP", "ancient": "Ancient recovery",
    "team_games": "Team-game catalog", "schedule": "Schedule", "legacy_supertable": "Legacy supertable",
}


def load_registry() -> tuple[set[str], set[str], dict]:
    """Load source keys and page labels from the current witness artifacts."""
    keys: set[str] = set()
    page_labels: set[str] = set()
    inv_path = DOCS / "witness-coverage-master-inventory.json"
    contract_path = DOCS / "witness-contracts-v2.json"
    inv = json.loads(inv_path.read_text(encoding="utf-8")) if inv_path.exists() else {}
    contracts = {}
    if contract_path.exists():
        contracts = json.loads(contract_path.read_text(encoding="utf-8")).get("contracts", {})
        keys.update(contracts)
    for label, body in inv.get("witness_registry", {}).items():
        page_labels.add(label.lower())
        keys.update(body.get("sources", []))
    return keys, page_labels, {"inventory_exists": inv_path.exists(), "contracts_exists": contract_path.exists()}


def source_tokens(source_keys: Iterable[str]) -> list[tuple[str, str]]:
    """Return safe path tokens, longest first, avoiding generic tokens."""
    pairs = []
    for key in source_keys:
        token = key.lower().replace(":", "_").replace(" ", "_").replace("-", "_")
        token = re.sub(r"[^a-z0-9_]+", "_", token).strip("_")
        if len(token) >= 5:
            pairs.append((token, key))
    return sorted(set(pairs), key=lambda x: len(x[0]), reverse=True)


def iter_files(root: Path, include_volatile: bool, max_depth: int = 6) -> tuple[list[Path], list[dict]]:
    files: list[Path] = []
    exclusions: list[dict] = []
    if not root.exists():
        exclusions.append({"root": str(root), "reason": "missing_root"})
        return files, exclusions
    pending = [(root, 0)]
    while pending:
        base_path, depth = pending.pop()
        try:
            entries = list(os.scandir(base_path))
        except (OSError, PermissionError) as exc:
            exclusions.append({"path": str(base_path), "reason": f"scan_error:{type(exc).__name__}"})
            continue
        if root == LAKE or LAKE in base_path.parents:
            try:
                rel_base = base_path.relative_to(LAKE).as_posix().lower()
            except ValueError:
                rel_base = ""
            if rel_base in OPAQUE_RELATIVE_DIRS:
                exclusions.append({"path": str(base_path), "reason": "opaque_high_volume_tree"})
                continue
        for entry in entries:
            child = base_path / entry.name
            try:
                is_dir = entry.is_dir(follow_symlinks=False)
            except OSError:
                is_dir = False
            if is_dir:
                if not include_volatile and entry.name.lower() in VOLATILE_DIRS:
                    exclusions.append({"path": str(child), "reason": "volatile_tree_excluded"})
                    continue
                if root == LAKE or LAKE in child.parents:
                    try:
                        rel_child = child.relative_to(LAKE).as_posix().lower()
                    except ValueError:
                        rel_child = ""
                    if rel_child in OPAQUE_RELATIVE_DIRS:
                        exclusions.append({"path": str(child), "reason": "opaque_high_volume_tree"})
                        continue
                if depth >= max_depth:
                    exclusions.append({"path": str(child), "reason": "max_depth_excluded"})
                else:
                    pending.append((child, depth + 1))
            else:
                files.append(child)
    return files, exclusions


def iter_data_files_fast(root: Path, max_depth: int = 20) -> list[Path]:
    """Enumerate data-bearing descendants without stat calls or directory lists."""
    files: list[Path] = []
    pending = [(root, 0)]
    while pending:
        base, depth = pending.pop()
        try:
            it = os.scandir(base)
        except (OSError, PermissionError):
            continue
        with it:
            for entry in it:
                child = Path(base) / entry.name
                try:
                    if entry.is_dir(follow_symlinks=False):
                        if depth < max_depth:
                            pending.append((child, depth + 1))
                    elif Path(entry.name).suffix.lower() in DATA_SUFFIXES:
                        files.append(child)
                except OSError:
                    continue
    return files


def root_area(path: Path) -> str:
    try:
        rel = path.relative_to(LAKE)
        return rel.parts[0] if rel.parts else "lake_root"
    except ValueError:
        try:
            rel = path.relative_to(CATALOG)
            return "_catalog/" + (rel.parts[0] if rel.parts else "root")
        except ValueError:
            return "outside_lake"


def file_kind(path: Path) -> str:
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".feather", ".arrow", ".orc", ".avro"}:
        return "table_file"
    if suffix in {".duckdb", ".db", ".sqlite", ".sqlite3"}:
        return "database_container"
    if suffix in {".csv", ".tsv", ".jsonl", ".ndjson"}:
        return "tabular_text"
    if suffix == ".json":
        return "json_artifact"
    if suffix in TEXT_SUFFIXES:
        return "text_artifact"
    return "other_file"


def classify(path: Path, kind: str, matched_sources: list[str]) -> tuple[str, str, str]:
    """Return primary_role, confidence, and reasoning."""
    s = str(path).lower().replace("\\", "/")
    name = path.name.lower()
    area = root_area(path)
    if matched_sources and kind in {"table_file", "database_container", "tabular_text"}:
        return "registered_witness_artifact", "high", "path matches registered witness source key"
    if "/derived/external_witness_recon/" in s or "resolved_event" in name:
        return "reconciled_evidence", "high", "resolved external-witness output"
    if "/derived/external_witness_intake/" in s or "/derived/external_witness_crawl/" in s:
        return "candidate_witness_evidence", "high", "external-witness intake/crawl artifact"
    if "/derived/candidate_substrate/" in s:
        return "candidate_witness_catalog", "high", "candidate-source substrate"
    if "/curated/source_acquisition_packets/" in s or "/curated/source_horizon_" in s:
        return "candidate_witness_evidence", "high", "source acquisition or horizon artifact"
    if "/curated/completeness/" in s:
        return "coverage_control", "high", "completeness/control dataset"
    if "/curated/source_of_truth_candidates/" in s:
        return "release_readiness_control", "high", "source-of-truth candidate pointer or manifest"
    if "/curated/c_to_d_consolidation/" in s:
        return "provenance_review_queue", "high", "copy/consolidation ledger or review queue"
    if "/curated/manual_review/" in s or "review_queue" in name or "manual_review" in name:
        return "review_queue", "high", "manual-review or adjudication artifact"
    if "/curated/ancient_source_recovery/" in s or "ancient_bundle" in s:
        return "historical_evidence", "medium", "ancient-source recovery artifact"
    if "/curated/player_identity/" in s or "identity" in name or "collision" in name:
        return "identity_control", "high", "identity bridge/collision control"
    if "/curated/team_games/" in s or "team_game" in name or "schedule" in name:
        return "structural_control", "medium", "team-game or schedule structure"
    if "/derived/newspaper_atoms/" in s or "newspaper" in s:
        return "historical_evidence", "medium", "newspaper/source-document artifact"
    if "/derived/validation/" in s or "audit" in name or "validator" in name:
        return "validation_control", "medium", "validation or audit artifact"
    if "/derived/fantasy_scoring/" in s or "/derived/player_features/" in s or "/derived/scoring_summary/" in s:
        return "derived_feature", "high", "derived feature/scoring output, not independent witness"
    if "/derived/rank_patches/" in s or "/derived/aggregates/" in s:
        return "derived_feature", "high", "derived aggregate/rank output"
    if area == "raw":
        return "raw_source_capture", "medium", "raw source or source cache"
    if area == "ops_data":
        return "operational_data", "medium", "operational lake data"
    if area == "releases":
        return "release_artifact", "high", "materialized release product"
    if area.startswith("_catalog"):
        return "catalog_metadata", "high", "lake catalog/manifests/schema metadata"
    if area == "curated":
        return "curated_artifact", "low", "curated artifact without a narrower rule"
    if area == "derived":
        return "derived_artifact", "low", "derived artifact without a narrower rule"
    return "unresolved", "low", "no deterministic classification rule matched"


def sha256_prefix(path: Path, limit: int = 1024 * 1024) -> str | None:
    try:
        h = hashlib.sha256()
        with path.open("rb") as f:
            h.update(f.read(limit))
        return h.hexdigest()
    except (OSError, PermissionError):
        return None


def artifact_row(path: Path, tokens: list[tuple[str, str]], compute_hash: bool = False, metadata: bool = True) -> dict:
    kind = file_kind(path)
    normalized = str(path).lower().replace("\\", "/")
    matched = [key for token, key in tokens if token in normalized]
    family_matches = sorted({label for token, label in KNOWN_FAMILY_TOKENS.items() if token in normalized})
    role, confidence, reason = classify(path, kind, matched)
    if metadata:
        try:
            stat = path.stat()
            size = stat.st_size
            mtime = datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat()
        except OSError:
            size, mtime = None, None
    else:
        size, mtime = None, None
    return {
        "path": str(path),
        "relative_path": str(path.relative_to(REPO)) if path.is_relative_to(REPO) else str(path),
        "area": root_area(path),
        "kind": kind,
        "suffix": path.suffix.lower(),
        "bytes": size,
        "modified_utc": mtime,
        "sha256_prefix_1mb": sha256_prefix(path) if compute_hash else None,
        "primary_role": role,
        "confidence": confidence,
        "reason": reason,
        "registered_source_matches": matched,
        "source_family_matches": family_matches,
        "enumeration_status": "file_enumerated",
    }


def container_row(exclusion: dict) -> dict:
    path = Path(exclusion.get("path", exclusion.get("root", "")))
    reason = exclusion["reason"]
    low = str(path).lower().replace("\\", "/")
    if reason == "volatile_tree_excluded":
        role, confidence, why = "volatile_container", "high", "volatile tree explicitly excluded from default scan"
    elif "/_catalog/migration_logs/" in low:
        role, confidence, why = "migration_provenance", "high", "cataloged migration/consolidation log container"
    elif "/_catalog/inventories/" in low:
        role, confidence, why = "catalog_metadata", "high", "catalog inventory container"
    elif any(x in low for x in ("/external_witness_intake/", "/external_witness_crawl/")):
        role, confidence, why = "candidate_witness_evidence", "high", "external witness intake/crawl container"
    elif low.rstrip("/").endswith(("/raw", "/curated", "/derived", "/ops_data", "/releases", "/staging", "/tools", "/artifacts", "/facts", "/ff_assets")):
        role, confidence, why = "lake_surface_container", "high", "top-level lake surface container"
    elif "/cache" in low:
        role, confidence, why = "source_cache", "high", "source or operational cache container"
    elif any(x in low for x in ("/review", "review_decision", "manual_review", "collision_review")):
        role, confidence, why = "review_queue", "high", "container name identifies review/adjudication work"
    elif any(x in low for x in ("/audit", "validation", "replay", "drift", "checkpoint")):
        role, confidence, why = "validation_control", "high", "container name identifies validation/audit/replay control"
    elif any(x in low for x in ("/overlay", "handoff", "promotion", "promoted", "/apply", "resolved_event")):
        role, confidence, why = "reconciled_evidence", "high", "container name identifies promoted/reconciled evidence"
    elif any(x in low for x in ("ocr", "source_document", "newspaper", "capture", "archive")):
        role, confidence, why = "historical_evidence", "high", "container name identifies source-document evidence"
    elif any(x in low for x in ("backup", "cache", "snapshot")):
        role, confidence, why = "operational_backup", "high", "container name identifies cache/backup/snapshot"
    elif reason in {"opaque_high_volume_tree", "max_depth_excluded", "container_only_surface_default_scan"}:
        role, confidence, why = "opaque_container", "high", reason
    else:
        role, confidence, why = "unscanned_container", "medium", reason
    return {
        "path": str(path),
        "relative_path": str(path.relative_to(REPO)) if path.is_relative_to(REPO) else str(path),
        "area": root_area(path),
        "kind": "container",
        "suffix": "",
        "bytes": None,
        "modified_utc": None,
        "sha256_prefix_1mb": None,
        "primary_role": role,
        "confidence": confidence,
        "reason": why,
        "registered_source_matches": [],
        "source_family_matches": sorted({label for token, label in KNOWN_FAMILY_TOKENS.items() if token in str(path).lower().replace("\\", "/")}),
        "enumeration_status": "children_not_enumerated",
    }


def catalog_inventory_rows() -> list[dict]:
    """Materialize lake-root inventory rows from the existing migration catalog."""
    p = CATALOG / "inventories" / "workspace_data_inventory_20260606.csv"
    if not p.exists():
        return []
    out = []
    with p.open(newline="", encoding="utf-8-sig") as f:
        for rec in csv.DictReader(f):
            dest = rec.get("destination", "")
            if "d:\\league-history-data\\nfl" not in dest.lower():
                continue
            low = dest.lower().replace("\\", "/")
            if "/raw/pfr/cache" in low:
                role, reason = "source_cache", "cataloged PFR source cache"
            elif "/ops_cache" in low:
                role, reason = "opaque_container", "cataloged operational DuckDB cache"
            elif "/curated/" in low:
                role, reason = "candidate_witness_evidence", "cataloged curated source artifact"
            else:
                role, reason = "catalog_metadata", "cataloged lake destination"
            out.append({
                "path": dest,
                "relative_path": dest,
                "area": root_area(Path(dest)),
                "kind": "catalog_entry",
                "suffix": "",
                "bytes": int(rec["bytes"]) if rec.get("bytes", "").isdigit() else None,
                "modified_utc": None,
                "sha256_prefix_1mb": None,
                "primary_role": role,
                "confidence": "high",
                "reason": reason,
                "registered_source_matches": [],
                "source_family_matches": sorted({label for token, label in KNOWN_FAMILY_TOKENS.items() if token in low}),
                "enumeration_status": "catalog_entry",
                "catalog_source": rec.get("source"),
                "catalog_file_count": int(rec["files"]) if rec.get("files", "").isdigit() else None,
                "catalog_notes": rec.get("notes"),
            })
    return out


def write_outputs(payload: dict, rows: list[dict]) -> None:
    json_path = DOCS / "lake-wide-classification.json"
    csv_path = DOCS / "lake-wide-classification.csv"
    md_path = DOCS / "lake-wide-classification.md"
    json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    fields = sorted({key for row in rows for key in row}) if rows else []
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader(); writer.writerows(rows)
    s = payload["summary"]
    lines = [
        "# Lake-Wide Classification Ledger",
        "",
        f"Generated: `{payload['generated_utc']}`  ",
        f"Rule version: `{payload['rule_version']}`  ",
        f"Artifact/container records: **{payload['artifact_count']:,}**  ",
        "",
        "## Scope",
        "",
        "Stable lake artifacts under `D:\\league-history-data\\nfl` plus catalog metadata under `D:\\league-history-data\\_catalog`. Volatile release/staging/log trees are excluded unless the scanner is run with `--include-volatile`; exclusions are recorded in the JSON ledger.",
        "",
        "## Classification summary",
        "",
        "| Role | Count |",
        "|---|---:|",
    ]
    for role, count in sorted(s["roles"].items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"| `{role}` | {count:,} |")
    lines += ["", "## Source-family tags", "", "Path-derived family tags are discovery labels, not proof of witness independence.", "", "| Family | Records |", "|---|---:|"]
    for family, count in sorted(s.get("source_family_matches", {}).items(), key=lambda x: (-x[1], x[0])):
        lines.append(f"| `{family}` | {count:,} |")
    lines += ["", "## Unresolved data-bearing artifacts", ""]
    unresolved = [r for r in rows if r["primary_role"] == "unresolved" and r["kind"] in {"table_file", "database_container", "tabular_text"}]
    if unresolved:
        lines += [f"**{len(unresolved):,} unresolved data-bearing artifacts remain.**", ""]
        lines += [f"- `{r['path']}`" for r in unresolved[:100]]
    else:
        lines.append("None in the scanned scope.")
    lines += ["", "## Opaque containers requiring expansion", "", "These containers are classified but not enumerated at child-file level in the default bounded scan. They are the remaining lake-wide census gap.", ""]
    opaque = [x for x in rows if x["primary_role"] == "opaque_container"]
    lines.append(f"**{len(opaque):,} opaque container records.**")
    lines += [f"- `{r['path']}` — {r['reason']}" for r in opaque[:200]]
    lines += ["", "## Registered witness artifacts", "", "These are path-level matches to the existing witness registry; they still require independence and value-control review.", ""]
    for r in [x for x in rows if x["primary_role"] == "registered_witness_artifact"][:200]:
        lines.append(f"- `{r['path']}` — {', '.join(r['registered_source_matches'])}")
    lines += ["", "## Exclusions", ""]
    lines += [f"- `{x.get('path', x.get('root'))}` — {x['reason']}" for x in payload["exclusions"][:200]]
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--include-volatile", action="store_true", help="scan releases/staging/tmp/log/fly snapshot trees too")
    parser.add_argument("--deep-all", action="store_true", help="attempt recursive scans of raw/ops_data too; normally they are represented as containers")
    parser.add_argument("--expand-newspaper", action="store_true", help="fast-enumerate data files nested below derived/newspaper_atoms")
    parser.add_argument("--hash", action="store_true", help="compute 1MB SHA-256 prefixes; slower on large lakes")
    args = parser.parse_args()
    keys, page_labels, registry_meta = load_registry()
    tokens = source_tokens(keys)
    all_files: list[Path] = []
    exclusions: list[dict] = []

    # Bounded default scan: enumerate the witness-bearing lanes at file level,
    # represent the huge raw/ops trees as containers, and record everything
    # excluded so the ledger states its own blind spots.
    if args.deep_all:
        roots = (LAKE, CATALOG)
    else:
        roots = (
            (LAKE / "curated" / "completeness", 2),
            (LAKE / "curated" / "source_of_truth_candidates", 2),
            (LAKE / "derived" / "candidate_substrate", 2),
            (LAKE / "derived" / "external_witness_intake", 1),
            (LAKE / "derived" / "external_witness_recon", 1),
            (LAKE / "derived" / "external_witness_crawl", 1),
            (LAKE / "derived" / "newspaper_atoms", 1),
            (CATALOG, 1),
        )
    scan_depth = 6 if args.deep_all else None
    for root_spec in roots:
        root, bounded_depth = root_spec if isinstance(root_spec, tuple) else (root_spec, scan_depth)
        files, excluded = iter_files(root, args.include_volatile, max_depth=scan_depth or bounded_depth)
        all_files.extend(files); exclusions.extend(excluded)
    if args.expand_newspaper and not args.deep_all:
        all_files.extend(iter_data_files_fast(LAKE / "derived" / "newspaper_atoms"))
    if not args.deep_all:
        for rel in ("raw", "curated", "derived", "ops_data", "ops_cache", "releases", "staging", "tools", "artifacts", "facts", "ff_assets"):
            p = LAKE / rel
            if p.exists():
                exclusions.append({"path": str(p), "reason": "container_only_surface_default_scan"})
    rows = []
    fast_newspaper = args.expand_newspaper and not args.deep_all
    for p in sorted(set(all_files), key=lambda x: str(x).lower()):
        row = artifact_row(p, tokens, compute_hash=args.hash, metadata=not (fast_newspaper and "newspaper_atoms" in str(p).lower()))
        rows.append(row)
    existing_paths = {r["path"] for r in rows}
    for exclusion in exclusions:
        p = exclusion.get("path", exclusion.get("root"))
        if p and p not in existing_paths:
            rows.append(container_row(exclusion))
            existing_paths.add(p)
    for rec in catalog_inventory_rows():
        if rec["path"] not in existing_paths:
            rows.append(rec)
            existing_paths.add(rec["path"])
    roles = Counter(r["primary_role"] for r in rows)
    kinds = Counter(r["kind"] for r in rows)
    source_matches = Counter(k for r in rows for k in r["registered_source_matches"])
    family_matches = Counter(k for r in rows for k in r.get("source_family_matches", []))
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "rule_version": RULE_VERSION,
        "roots": [str(LAKE), str(CATALOG)],
        "include_volatile": args.include_volatile,
        "artifact_count": len(rows),
        "exclusion_count": len(exclusions),
        "registry": {"source_key_count": len(keys), "page_label_count": len(page_labels), **registry_meta},
        "summary": {"roles": dict(roles), "kinds": dict(kinds), "registered_source_matches": dict(source_matches), "source_family_matches": dict(family_matches)},
        "exclusions": exclusions,
        "artifacts": rows,
    }
    write_outputs(payload, rows)
    print(f"scanned {len(rows):,} artifacts; excluded {len(exclusions):,} volatile/missing roots")
    print("roles:", ", ".join(f"{k}={v}" for k, v in roles.most_common()))
    print(f"wrote {DOCS / 'lake-wide-classification.json'}")
    print(f"wrote {DOCS / 'lake-wide-classification.csv'}")
    print(f"wrote {DOCS / 'lake-wide-classification.md'}")


if __name__ == "__main__":
    main()
