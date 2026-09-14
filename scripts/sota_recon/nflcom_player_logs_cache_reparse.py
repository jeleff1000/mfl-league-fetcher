"""Reparse retained generic NFL.com log HTML and emit a source-layout receipt.

This is a source-only measurement lane.  The generic parquet shards dropped the table
header/layout axis, but the harvester cache retains the original HTML.  Each cached page
is reparsed with the existing header grammar; unresolved headers are counted and never
guessed.  Rows are written to JSONL only as an audit surface, not back into the raw
capture or any witness licence.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import multiprocessing as mp
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from .nflcom_block_grammar import resolve_signature
from .nflcom_harvest import parse_all_tables


BASE = Path(r"D:/league-history-data/nfl/raw/nflcom")
CACHE = BASE / "cache"
UNIVERSE = BASE / "player_universe.json"
OUT_DIR = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master/"
    "nflcom_player_logs_cache_reparse"
)


@dataclass(frozen=True)
class Page:
    slug: str
    season: str
    path: str


def page_inventory(*, limit: int | None = None) -> tuple[list[Page], int]:
    universe = json.loads(UNIVERSE.read_text(encoding="utf-8"))
    # One directory listing is materially cheaper than 110k individual Windows
    # existence calls.  The cache is immutable for this measurement.
    cached_names = {path.name for path in CACHE.glob("*.html")}
    pages: list[Page] = []
    missing = 0
    for slug, seasons in sorted(universe.items()):
        for season in sorted({str(value) for value in seasons}):
            url = f"https://www.nfl.com/players/{slug}/stats/logs/{season}/"
            filename = hashlib.sha1(url.encode()).hexdigest() + ".html"
            if filename not in cached_names:
                missing += 1
                continue
            path = CACHE / filename
            pages.append(Page(slug, season, str(path)))
            if limit is not None and len(pages) >= limit:
                return pages, missing
    return pages, missing


def parse_page(page: Page) -> dict:
    text = Path(page.path).read_text(encoding="utf-8", errors="replace")
    rows: list[dict] = []
    statuses: Counter[str] = Counter()
    failures: list[dict] = []
    for table_index, table in enumerate(parse_all_tables(text)):
        resolved = resolve_signature("player_logs", table.get("headers") or [])
        status = str(resolved.get("status"))
        statuses[status] += 1
        if status != "RESOLVED":
            failures.append(
                {
                    "caption": table.get("caption"),
                    "headers": table.get("headers") or [],
                    "status": status,
                }
            )
            continue
        layout = resolved["layout"]
        for row_index, source_row in enumerate(table.get("rows") or []):
            row = dict(source_row)
            row.update(
                {
                    "nflcom_slug": page.slug,
                    "season": page.season,
                    "_table": table.get("caption") or "",
                    "_layout": layout,
                    "_source_table_index": table_index,
                    "_source_row_index": row_index,
                }
            )
            rows.append(row)
    return {
        "slug": page.slug,
        "season": page.season,
        "rows": rows,
        "statuses": dict(statuses),
        "failures": failures,
    }


def run(*, limit: int | None = None, workers: int = 1, out_dir: Path = OUT_DIR) -> dict:
    pages, missing = page_inventory(limit=limit)
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / "resolved_rows.jsonl"
    receipt_path = out_dir / "receipt.json"
    status_counts: Counter[str] = Counter()
    layout_counts: Counter[str] = Counter()
    failures: list[dict] = []
    row_count = 0
    with rows_path.open("w", encoding="utf-8") as handle:
        pool = mp.Pool(processes=workers) if workers > 1 else None
        iterator = pool.imap_unordered(parse_page, pages) if pool else map(parse_page, pages)
        try:
            for result in iterator:
                status_counts.update(result["statuses"])
                failures.extend(
                    {"slug": result["slug"], "season": result["season"], **failure}
                    for failure in result["failures"]
                )
                for row in result["rows"]:
                    layout_counts[str(row["_layout"])] += 1
                    handle.write(json.dumps(row, separators=(",", ":")) + "\n")
                    row_count += 1
        finally:
            if pool is not None:
                pool.close()
                pool.join()
    receipt = {
        "version": "1",
        "source_only": True,
        "network_access": False,
        "claim": "retained generic NFL.com log HTML was reparsed through the header grammar",
        "pages_considered": len(pages),
        "missing_cache_pages": missing,
        "resolved_rows": row_count,
        "resolved_layout_counts": dict(sorted(layout_counts.items())),
        "table_signature_status_counts": dict(sorted(status_counts.items())),
        "unresolved_signature_count": len(failures),
        "unresolved_signature_examples": failures[:20],
        "output_rows": rows_path.as_posix(),
        "status": "SOURCE_HTML_REPARSED" if not failures else "SOURCE_HTML_PARTIAL",
        "no_raw_capture_or_license_change": True,
    }
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-pages", type=int)
    parser.add_argument("--workers", type=int, default=1)
    args = parser.parse_args()
    print(json.dumps(run(limit=args.limit_pages, workers=args.workers), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
