"""Merge independent MFL discovery shard artifacts into one lineage inventory."""
from __future__ import annotations

import argparse
import glob
import hashlib
import json
from pathlib import Path


def _priority(status: str | None) -> int:
    return {"available": 2, "unknown": 1, "none_or_restricted": 0}.get(status or "unknown", 0)


def _meaningful_name(value: object) -> str | None:
    text = " ".join(str(value or "").split()).strip().lower()
    # Single generic labels ("league", "football", "dynasty") are not lineage
    # evidence; they would collapse much of the public MFL directory.
    if len(text) < 8 or len(text.split()) < 2:
        return None
    return text


def _roster_set(row: dict) -> set[str]:
    values = row.get("roster_names") or []
    return {str(value).strip().lower() for value in values if str(value).strip()}


def merge_payloads(payloads: list[dict], existing_seed: list[dict]) -> dict:
    shard_status = {
        str(payload.get("shard_index")): {
            "complete": bool(payload.get("complete", True)),
            "completed_through_year": payload.get("completed_through_year"),
            "records": len(payload.get("records", [])),
        }
        for payload in payloads
        if payload.get("shard_index") is not None
    }
    records: dict[tuple[int, str], dict] = {}
    lineage_sets: dict[tuple[int, str], set[str]] = {}
    seed_candidates: dict[str, dict] = {}

    def add(raw: dict, *, existing: bool = False) -> None:
        if raw.get("year") is None or raw.get("id") is None:
            return
        key = (int(raw["year"]), str(raw["id"]))
        seed = str(raw.get("lineage_seed") or raw.get("seed") or f"{key[0]}:{key[1]}")
        item = dict(raw)
        item["year"], item["id"] = key
        item["lineage_seeds"] = sorted(set(raw.get("lineage_seeds") or [seed]))
        item.setdefault("link_type", "seed" if existing else "history")
        item.setdefault("draft_status", "unknown")
        item.setdefault("owner_data_status", "restricted_without_credentials")
        if existing or raw.get("link_type") in (None, "seed"):
            seed_candidates.setdefault(seed, item)
        current = records.get(key)
        if current is None or _priority(item.get("draft_status")) > _priority(current.get("draft_status")):
            records[key] = item
        else:
            current["lineage_seeds"] = sorted(set(current.get("lineage_seeds", [])) | set(item["lineage_seeds"]))
        lineage_sets.setdefault(key, set()).update(item["lineage_seeds"])

    for row in existing_seed:
        add(row, existing=True)
    for payload in payloads:
        # Numeric shards emit season/history records.  The older keyword-directory
        # crawler emits the same information as a seed-shaped ``leagues`` array;
        # accept both so directory discovery is additive rather than a separate,
        # silently omitted population.
        rows = list(payload.get("records", []))
        for row in payload.get("leagues", []):
            item = dict(row)
            item.setdefault("lineage_seed", item.get("seed"))
            item.setdefault("link_type", "seed")
            rows.append(item)
        for row in rows:
            add(row)

    # Union lineages whenever separate seeds point to the same season/id. This is
    # stronger than matching names alone and retains an auditable seed list.
    parent: dict[str, str] = {}
    edge_evidence: dict[tuple[str, str], set[str]] = {}

    def find(value: str) -> str:
        parent.setdefault(value, value)
        while parent[value] != value:
            parent[value] = parent[parent[value]]
            value = parent[value]
        return value

    def union(left: str, right: str, evidence: str | None = None) -> None:
        if evidence and left != right:
            edge_evidence.setdefault(tuple(sorted((left, right))), set()).add(evidence)
        a, b = find(left), find(right)
        if a != b:
            parent[max(a, b)] = min(a, b)

    for seeds in lineage_sets.values():
        seeds = sorted(seeds)
        for seed in seeds[1:]:
            union(seeds[0], seed)

    # Link independently discovered season records conservatively. These are
    # separate from explicit MFL history URLs and are retained as evidence rather
    # than treated as proof of identity. Owner/commissioner fields are supported
    # when present, but public MFL exports normally omit them.
    seed_rows: dict[str, dict] = {}
    for row in records.values():
        for seed in row.get("lineage_seeds", []):
            seed_rows.setdefault(seed, row)

    def link_index(field: str, *, evidence: str, meaningful: bool = False) -> None:
        groups: dict[str, list[str]] = {}
        for seed, row in seed_rows.items():
            value = row.get(field)
            value = _meaningful_name(value) if meaningful else str(value or "").strip().lower()
            if value:
                groups.setdefault(value, []).append(seed)
        for seeds in groups.values():
            seeds = sorted(set(seeds))
            for seed in seeds[1:]:
                union(seeds[0], seed, evidence)

    link_index("name_key", evidence="league_name_exact", meaningful=True)
    for field in ("commissioner_id", "commissioner", "commissioner_name"):
        link_index(field, evidence="commissioner_exact")
    fingerprint_groups: dict[str, list[str]] = {}
    for seed, row in seed_rows.items():
        fingerprint = str(row.get("roster_fingerprint") or "").strip().lower()
        if fingerprint and len(_roster_set(row)) >= 4:
            fingerprint_groups.setdefault(fingerprint, []).append(seed)
    for seeds in fingerprint_groups.values():
        seeds = sorted(set(seeds))
        for seed in seeds[1:]:
            union(seeds[0], seed, "roster_fingerprint_exact")

    # Roster continuity tolerates renamed teams but requires substantial overlap
    # and at least four franchises on both sides.
    seed_items = sorted(seed_rows.items())
    for index, (left_seed, left_row) in enumerate(seed_items):
        left_roster = _roster_set(left_row)
        if len(left_roster) < 4:
            continue
        for right_seed, right_row in seed_items[index + 1:]:
            right_roster = _roster_set(right_row)
            if len(right_roster) < 4 or left_seed == right_seed:
                continue
            overlap = len(left_roster & right_roster) / max(len(left_roster), len(right_roster))
            if overlap >= 0.8:
                union(left_seed, right_seed, "roster_overlap_ge_0.8")

    def lineage_id(seeds: set[str]) -> str:
        roots = sorted({find(seed) for seed in seeds})
        return "mfl_lineage_" + hashlib.sha1("|".join(roots).encode()).hexdigest()[:16]

    output = []
    for key in sorted(records):
        row = records[key]
        seeds = lineage_sets.get(key, set(row.get("lineage_seeds", [])))
        row["lineage_seeds"] = sorted(seeds)
        row["lineage_id"] = lineage_id(seeds)
        output.append(row)
    lineages: dict[str, set[str]] = {}
    for row in output:
        lineages.setdefault(row["lineage_id"], set()).update(row["lineage_seeds"])
    lineage_edges: dict[str, list[dict]] = {}
    for (left, right), evidence in sorted(edge_evidence.items()):
        lid = lineage_id({left, right})
        lineage_edges.setdefault(lid, []).append(
            {"left": left, "right": right, "evidence": sorted(evidence)}
        )
    leagues = []
    for seed, raw in sorted(seed_candidates.items()):
        seed_year, seed_id = seed.split(":", 1)
        row = dict(raw)
        row["seed"] = seed
        row["year"] = int(seed_year)
        row["id"] = seed_id
        row["link_type"] = "seed"
        row["lineage_id"] = lineage_id({seed})
        leagues.append(row)
    return {
        "platform": "mfl",
        "shards": shard_status,
        "shards_total": len(shard_status),
        "shards_complete": sum(item["complete"] for item in shard_status.values()),
        "shards_partial": sum(not item["complete"] for item in shard_status.values()),
        "leagues": leagues,
        "unique_season_leagues": len(output),
        "records_with_draft": sum(row.get("draft_status") == "available" for row in output),
        "lineage_count": len(lineages),
        "lineages": {key: sorted(value) for key, value in sorted(lineages.items())},
        "lineage_link_evidence": {key: value for key, value in sorted(lineage_edges.items())},
        "records": output,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-glob", required=True)
    parser.add_argument("--existing-seed", type=Path)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    payloads = [json.loads(Path(path).read_text(encoding="utf-8")) for path in sorted(glob.glob(args.input_glob))]
    existing = []
    if args.existing_seed and args.existing_seed.is_file():
        existing = json.loads(args.existing_seed.read_text(encoding="utf-8")).get("leagues", [])
    result = merge_payloads(payloads, existing)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.out.with_suffix(args.out.suffix + ".tmp")
    tmp.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    tmp.replace(args.out)
    print(json.dumps({key: result[key] for key in ("unique_season_leagues", "records_with_draft", "lineage_count")}))


if __name__ == "__main__":
    main()
