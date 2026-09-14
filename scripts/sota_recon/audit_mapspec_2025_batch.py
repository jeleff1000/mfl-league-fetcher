"""Run one bounded, resumable batch of the 2025 MapSpec audit.

The original audit is deliberately source-grouped, but a single process over all
sources can take long enough that its final receipt is easy to mistake for a stale
one.  This wrapper preserves the same validator and conflict classifier while writing
one complete receipt per source batch.  Batch receipts are mergeable by source and
retain the full denominator fields needed to distinguish abstention, zero, and value
conflicts.
"""
from __future__ import annotations

import argparse
import json
from collections import Counter, defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from . import audit_mapspec_2025 as audit
from . import sources as S
from .witness_map import WITNESS_MAP


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, required=True)
    ap.add_argument("--batch-size", type=int, default=8)
    args = ap.parse_args()

    import duckdb
    con = duckdb.connect()
    try:
        def cols(path: str) -> set[str]:
            p = Path(path).as_posix()
            return {r[0] for r in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{p}')"
            ).fetchall()}
        audit._SEASON_COLS = cols(S.v26_plane("season"))
        audit._CAREER_COLS = cols(S.v26_plane("career"))
    finally:
        con.close()

    groups = defaultdict(list)
    for spec in WITNESS_MAP:
        groups[spec.source_key].append(spec)
    sources = sorted(groups)
    start = args.batch * args.batch_size
    selected = sources[start:start + args.batch_size]
    if not selected:
        raise SystemExit(f"batch {args.batch} is empty; source_count={len(sources)}")

    items = [(source, groups[source]) for source in selected]
    rows: list[dict] = []
    errors: list[dict] = []
    workers = min(4, len(items))
    with ProcessPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(audit._run_source, item): item[0] for item in items}
        for future, source in [(f, s) for f, s in futures.items()]:
            try:
                rows.extend(future.result())
            except Exception as exc:
                errors.append({"source": source, "error": repr(exc)})

    for row in rows:
        row["audit_year"] = 2025
        row["target_plane"] = audit._plane_for(row)
        row["conflict_class"] = audit.classify(row)
        row["newspaper_atoms_preserved"] = True

    receipt = {
        "audit": "mapspec_vs_supertable",
        "batch": args.batch,
        "batch_size": args.batch_size,
        "sources": selected,
        "years": [2025, 2025],
        "spec_count": sum(len(groups[s]) for s in selected),
        "source_count": len(selected),
        "workers": workers,
        "rows": rows,
        "errors": errors,
        "class_counts": dict(Counter(r["conflict_class"] for r in rows)),
        "plane_counts": dict(Counter(r["target_plane"] for r in rows)),
        "notes": [
            "Batch uses audit_mapspec_2025._run_source and classify without changing their semantics.",
            "Source NULL/blank cells remain denominator-preserving abstentions; zero-direction classes are retained.",
            "Newspaper atoms are immutable witness inputs.",
        ],
    }
    OUT.mkdir(parents=True, exist_ok=True)
    path = OUT / f"MAPPING_AUDIT_2025_batch_{args.batch:02d}.json"
    path.write_text(json.dumps(receipt, indent=1), encoding="utf-8")
    print(json.dumps({
        "receipt": str(path),
        "batch": args.batch,
        "sources": selected,
        "spec_count": receipt["spec_count"],
        "rows": len(rows),
        "errors": errors,
        "class_counts": receipt["class_counts"],
    }, indent=2))
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
