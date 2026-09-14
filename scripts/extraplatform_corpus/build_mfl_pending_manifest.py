"""Build a disjoint MFL work manifest from the canonical register index.

The population manifest is ordered by discovery history, not by remaining
work.  This helper removes accepted and recorded-rejected season/league keys
and orders the remaining rows by yearly deficit so parallel fetch campaigns
do useful work instead of repeatedly revalidating landed leagues.
"""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Iterable


Key = tuple[int, str]


def row_key(row: dict) -> Key:
    return int(row["season"]), str(row["league_id"]).zfill(5)


def pending_rows(
    manifest_rows: Iterable[dict],
    index: dict,
    *,
    target_per_year: int = 10_000,
    target_years: set[int] | None = None,
    collect_all: bool = True,
    retry_rejected: bool = False,
    preserve_order: bool = False,
) -> list[dict]:
    """Return only rows not already accepted or rejected in ``index``.

    Rows are sorted by descending remaining yearly deficit, then season and
    original manifest position.  Stable ordering makes disjoint batch slices
    reproducible across workers.
    """
    rows = list(manifest_rows)
    accepted = {row_key(row) for row in index.get("leagues", [])}
    rejected = {row_key(row) for row in index.get("rejected", [])}
    used = accepted | rejected
    accepted_by_year = Counter(int(year) for year, count in index.get("accepted_by_year", {}).items() for _ in range(int(count)))
    # Prefer the explicit counts only when they are consistent with the
    # receipt list; the receipt keys are authoritative for filtering.
    accepted_counts = Counter(row_key(row)[0] for row in index.get("leagues", []))
    if accepted_counts:
        accepted_by_year = accepted_counts
    target_years = target_years or {int(row["season"]) for row in rows}
    deficits = {
        year: max(0, target_per_year - accepted_by_year.get(year, 0))
        for year in target_years
    }

    candidates: list[tuple[int, int, dict]] = []
    seen: set[Key] = set()
    for position, row in enumerate(rows):
        key = row_key(row)
        if retry_rejected:
            if key not in rejected or key in accepted or key in seen:
                continue
        elif key in used or key in seen:
            continue
        seen.add(key)
        year = key[0]
        if not collect_all and year in target_years and deficits.get(year, 0) <= 0:
            continue
        candidates.append((-deficits.get(year, 0), position, row))
    if preserve_order:
        candidates.sort(key=lambda item: item[1])
    else:
        candidates.sort(key=lambda item: (item[0], int(item[2]["season"]), item[1]))
    return [row for _, _, row in candidates]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--target-per-year", type=int, default=10_000)
    parser.add_argument("--target-years", default="")
    parser.add_argument("--collect-all", action="store_true")
    parser.add_argument("--retry-rejected", action="store_true")
    parser.add_argument("--preserve-order", action="store_true")
    args = parser.parse_args()

    source = json.loads(args.manifest.read_text(encoding="utf-8"))
    rows = source.get("rows", source.get("leagues", source))
    index = json.loads(args.index.read_text(encoding="utf-8"))
    years = {int(value) for value in args.target_years.split(",") if value.strip()} or None
    result_rows = pending_rows(
        rows,
        index,
        target_per_year=args.target_per_year,
        target_years=years,
        collect_all=args.collect_all,
        retry_rejected=args.retry_rejected,
        preserve_order=args.preserve_order,
    )
    payload = {
        "schema_version": "mfl_pending_manifest_v1",
        "source_schema_version": source.get("schema_version"),
        "target_per_year": args.target_per_year,
        "target_years": sorted(years) if years else sorted({int(row["season"]) for row in rows}),
        "excluded_accepted": len(index.get("leagues", [])),
        "excluded_rejected": len(index.get("rejected", [])),
        "retry_rejected": args.retry_rejected,
        "rows": result_rows,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"pending_count": len(result_rows), "output": str(args.output)}, sort_keys=True))


if __name__ == "__main__":
    main()
