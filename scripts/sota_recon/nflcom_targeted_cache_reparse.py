"""Recover exact layouts for the two targeted NFL.com log captures from local cache.

The old targeted writer concatenated every table on a page into one pandas frame without
retaining the table's header signature.  In the unioned parquet, names such as ``yds`` and
``att`` are therefore polysemous.  The retained HTML cache still has the headers.  This
module reparses those local bytes, resolves every header through the existing block
grammar, and writes a derived audit table with a real ``_layout`` column.

No network function is called.  A missing cache page or unresolved signature aborts the
build; neither is guessed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import duckdb

from .nflcom_block_grammar import resolve_signature
from .nflcom_harvest import BASE, _cache_file, _write_parquet, parse_all_tables


INPUT_DIR = BASE / "tables" / "player_logs_targeted"
OUT_PATH = (
    BASE.parents[1]
    / "derived"
    / "validation"
    / "sota_recon_master"
    / "nflcom_player_logs_targeted_normalized.parquet"
)
RECEIPT_PATH = (
    Path(__file__).resolve().parents[2]
    / "docs"
    / "nflcom-targeted-cache-reparse-receipt.json"
)


def normalize_tables(
    tables: Iterable[dict],
    *,
    slug: str,
    season: str,
    artifacts: Iterable[str],
    cache_sha1: str,
) -> tuple[list[dict], list[dict]]:
    """Attach the uniquely resolved layout to every row of parsed log tables."""
    rows: list[dict] = []
    failures: list[dict] = []
    artifact_text = ";".join(sorted(set(artifacts)))
    for table_index, table in enumerate(tables):
        resolved = resolve_signature("player_logs", table.get("headers") or [])
        if resolved.get("status") != "RESOLVED":
            failures.append(
                {
                    "slug": slug,
                    "season": str(season),
                    "caption": table.get("caption"),
                    "headers": table.get("headers"),
                    "status": resolved.get("status"),
                }
            )
            continue
        signature = hashlib.sha256(
            json.dumps(table.get("headers") or [], ensure_ascii=True).encode("utf-8")
        ).hexdigest()
        for row_index, original in enumerate(table.get("rows") or []):
            row = dict(original)
            row.update(
                {
                    "_view": "logs",
                    "_table": table.get("caption") or "",
                    "_layout": resolved["layout"],
                    "_header_signature_sha256": signature,
                    "_cache_sha1": cache_sha1,
                    "_source_artifacts": artifact_text,
                    "_source_table_index": str(table_index),
                    "_source_row_index": str(row_index),
                    "nflcom_slug": slug,
                    "season": str(season),
                }
            )
            rows.append(row)
    return rows, failures


def _memberships(input_dir: Path) -> tuple[dict[tuple[str, str], set[str]], list[dict]]:
    con = duckdb.connect()
    memberships: dict[tuple[str, str], set[str]] = defaultdict(set)
    inputs: list[dict] = []
    try:
        for path in sorted(input_dir.glob("*.parquet")):
            pairs = con.execute(
                f"SELECT DISTINCT CAST(nflcom_slug AS VARCHAR), CAST(season AS VARCHAR) "
                f"FROM read_parquet('{path.as_posix()}') "
                "WHERE nflcom_slug IS NOT NULL AND season IS NOT NULL"
            ).fetchall()
            for slug, season in pairs:
                memberships[(slug, season)].add(path.stem)
            inputs.append(
                {
                    "path": path.as_posix(),
                    "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                    "distinct_pages": len(pairs),
                }
            )
    finally:
        con.close()
    return memberships, inputs


def build(
    input_dir: Path = INPUT_DIR,
    out_path: Path = OUT_PATH,
    receipt_path: Path = RECEIPT_PATH,
) -> dict:
    memberships, inputs = _memberships(input_dir)
    if not inputs:
        raise RuntimeError(f"no targeted parquet inputs under {input_dir}")

    rows: list[dict] = []
    missing: list[str] = []
    failures: list[dict] = []
    per_layout: dict[str, int] = defaultdict(int)
    for (slug, season), artifacts in sorted(memberships.items()):
        url = f"https://www.nfl.com/players/{slug}/stats/logs/{season}/"
        cache_path = _cache_file(url)
        if not cache_path.exists():
            missing.append(f"{slug}|{season}")
            continue
        tables = parse_all_tables(cache_path.read_text(encoding="utf-8", errors="replace"))
        normalized, page_failures = normalize_tables(
            tables,
            slug=slug,
            season=season,
            artifacts=artifacts,
            cache_sha1=cache_path.stem,
        )
        rows.extend(normalized)
        failures.extend(page_failures)
        for row in normalized:
            per_layout[row["_layout"]] += 1

    if missing or failures:
        raise RuntimeError(
            f"targeted cache reparse refused: missing_pages={len(missing)}, "
            f"unresolved_signatures={len(failures)}; examples="
            f"{(missing[:3] + [str(x) for x in failures[:3]])}"
        )
    if not rows:
        raise RuntimeError("targeted cache reparse produced no rows")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(out_path.stem + ".tmp.parquet")
    if tmp_path.exists():
        tmp_path.unlink()
    _write_parquet(rows, tmp_path)
    tmp_path.replace(out_path)

    receipt = {
        "version": "1",
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "claim": "targeted NFL.com log rows were re-parsed from retained local HTML and every table received a uniquely resolved header layout",
        "network_access": False,
        "inputs": inputs,
        "counts": {
            "distinct_cached_pages": len(memberships),
            "rows": len(rows),
            "missing_cache_pages": len(missing),
            "unresolved_signatures": len(failures),
            "by_layout": dict(sorted(per_layout.items())),
        },
        "output": {
            "path": out_path.as_posix(),
            "sha256": hashlib.sha256(out_path.read_bytes()).hexdigest(),
        },
        "status": "PASS",
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_text(json.dumps(receipt, indent=2) + "\n", encoding="utf-8")
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, default=INPUT_DIR)
    parser.add_argument("--out", type=Path, default=OUT_PATH)
    parser.add_argument("--receipt", type=Path, default=RECEIPT_PATH)
    args = parser.parse_args()
    receipt = build(args.input_dir, args.out, args.receipt)
    print(f"wrote {receipt['counts']['rows']:,} rows -> {args.out}")
    print(f"layouts: {receipt['counts']['by_layout']}")
    print(f"receipt: {args.receipt}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
