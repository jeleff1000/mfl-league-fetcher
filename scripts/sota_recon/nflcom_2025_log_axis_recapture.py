"""Recapture and normalize NFL.com 2025 player-log table axes.

The historical ``player_logs`` parquet retains the phase caption but lost the
position/layout discriminator.  This is a network-backed, 2025-only recovery
surface: it preserves the raw parquet, reuses the existing cache, fetches only
the missing 2025 pages, and writes rows with the grammar-resolved ``_layout``.
Empty/404 pages are valid no-stat pages; unresolved table signatures are not.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

from .nflcom_block_grammar import resolve_signature
from .nflcom_harvest import BASE, _cache_file, _write_parquet, http_get, parse_all_tables


UNIVERSE = BASE / "player_universe.json"
OUT = (BASE.parents[1] / "derived" / "validation" / "sota_recon_master"
       / "nflcom_player_logs_2025_axis_recovered.parquet")
RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "nflcom-2025-log-axis-recapture-receipt.json"


def _slugs() -> list[str]:
    universe = json.loads(UNIVERSE.read_text(encoding="utf-8"))
    return sorted(slug for slug, years in universe.items() if 2025 in years)


def _one(slug: str) -> tuple[str, list[dict], list[dict], bool]:
    url = f"https://www.nfl.com/players/{slug}/stats/logs/2025/"
    cache = _cache_file(url)
    was_cached = cache.exists() and cache.stat().st_size > 0
    html = http_get(url, use_cache=True)
    if not html:
        return slug, [], [], was_cached
    rows: list[dict] = []
    failures: list[dict] = []
    for table_index, table in enumerate(parse_all_tables(html)):
        resolved = resolve_signature("player_logs", table.get("headers") or [])
        if resolved.get("status") != "RESOLVED":
            failures.append({"slug": slug, "caption": table.get("caption"),
                             "headers": table.get("headers"),
                             "status": resolved.get("status")})
            continue
        signature = hashlib.sha256(
            json.dumps(table.get("headers") or [], ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        for row_index, original in enumerate(table.get("rows") or []):
            row = dict(original)
            row.update({
                "_view": "logs", "_table": table.get("caption") or "",
                "_layout": resolved["layout"],
                "_header_signature_sha256": signature,
                "_cache_sha1": cache.stem,
                "_source_table_index": str(table_index),
                "_source_row_index": str(row_index),
                "nflcom_slug": slug, "season": "2025",
            })
            if "fum" in table.get("keys", []) and "lost" in table.get("keys", []):
                row["fumbles"] = row.get("fum")
                row["fumbles_lost"] = row.get("lost")
            rows.append(row)
    return slug, rows, failures, was_cached


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--workers", type=int, default=8)
    args = ap.parse_args()
    slugs = _slugs()
    rows: list[dict] = []
    failures: list[dict] = []
    cached = fetched = empty = 0
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(_one, slug) for slug in slugs]
        for future in as_completed(futures):
            slug, got, bad, was_cached = future.result()
            cached += int(was_cached)
            fetched += int(not was_cached)
            if not got:
                empty += 1
            rows.extend(got)
            failures.extend(bad)
    if failures:
        raise SystemExit(f"unresolved NFL.com 2025 signatures: {len(failures)}; {failures[:3]}")
    if not rows:
        raise SystemExit("no normalized 2025 log rows recovered")
    OUT.parent.mkdir(parents=True, exist_ok=True)
    tmp = OUT.with_suffix(".tmp.parquet")
    if tmp.exists():
        tmp.unlink()
    _write_parquet(rows, tmp)
    tmp.replace(OUT)
    by_layout: dict[str, int] = {}
    for row in rows:
        by_layout[row["_layout"]] = by_layout.get(row["_layout"], 0) + 1
    receipt = {
        "version": "1", "generated_utc": datetime.now(timezone.utc).isoformat(),
        "network_access": True, "year": 2025, "slugs": len(slugs),
        "cached_pages_reused": cached, "pages_fetched": fetched,
        "empty_or_404_pages": empty, "unresolved_signatures": len(failures),
        "rows": len(rows), "by_layout": dict(sorted(by_layout.items())),
        "output": {"path": OUT.as_posix(),
                   "sha256": hashlib.sha256(OUT.read_bytes()).hexdigest()},
        "raw_player_logs_preserved": True,
    }
    RECEIPT.write_text(json.dumps(receipt, indent=1), encoding="utf-8")
    print(json.dumps(receipt, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
