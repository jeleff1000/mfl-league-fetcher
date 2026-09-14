"""Evict and refold source DBs after playoff/clutch materialization."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.sleeper_corpus.build_corpus_snapshot import (
    CORPUS,
    OUT,
    evict_league,
    fold_league,
    open_snapshot,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--results", default=str(CORPUS / "source_playoff_clutch_backfill_results.jsonl"))
    parser.add_argument("--snapshot", default=str(OUT))
    parser.add_argument("--status", action="append", default=["done"])
    parser.add_argument("--offset", type=int, default=0,
                        help="Skip this many deterministic target DBs (for bounded resumes).")
    parser.add_argument("--limit", type=int,
                        help="Fold at most this many target DBs, then release the snapshot writer.")
    args = parser.parse_args()
    result_path = Path(args.results)
    rows = [json.loads(line) for line in result_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    all_targets = sorted(set(
        row["db_name"] for row in rows if row.get("status") in set(args.status)))
    if args.offset < 0:
        raise SystemExit("--offset must be non-negative")
    targets = all_targets[args.offset:]
    if args.limit is not None:
        if args.limit < 1:
            raise SystemExit("--limit must be positive")
        targets = targets[:args.limit]
    snapshot = open_snapshot(Path(args.snapshot))
    ok = failed = 0
    try:
        for i, db in enumerate(targets, args.offset + 1):
            source = CORPUS / "leagues" / db
            try:
                evict_league(snapshot, db)
                good, message = fold_league(snapshot, source)
            except Exception as exc:  # keep the rest of the corpus moving
                good, message = False, str(exc)
            if good:
                ok += 1
                print(f"[{i}/{len(all_targets)}] {db} folded", flush=True)
            else:
                failed += 1
                print(f"[{i}/{len(all_targets)}] {db} FAILED {message}", flush=True)
    finally:
        snapshot.close()
    print(json.dumps({"targets": len(targets), "offset": args.offset,
                      "folded": ok, "failed": failed}, sort_keys=True))


if __name__ == "__main__":
    main()
