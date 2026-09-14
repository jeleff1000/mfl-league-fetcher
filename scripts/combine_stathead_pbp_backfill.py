"""Combine Stathead Play Finder PBP query CSVs into parquet and summaries."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


DEFAULT_RAW_DIR = Path("tmp/stathead_pbp_backfill_raw_1978_1998")
DEFAULT_OUT = DEFAULT_RAW_DIR / "stathead_pbp_1978_1998_raw.parquet"


def read_progress(raw_dir: Path) -> list[dict[str, str]]:
    progress = raw_dir / "progress.csv"
    with progress.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    fieldnames = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fieldnames.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_summary(raw_dir: Path, progress_rows: list[dict[str, str]]) -> dict[str, object]:
    season_counts = defaultdict(lambda: {"queries": 0, "play_rows": 0, "min_rows": None, "max_rows": None})
    status_counts = Counter()
    for row in progress_rows:
        status_counts[row["status"]] += 1
        season = row["query_key"].split("_", 1)[0]
        rows = int(row["rows"] or 0)
        bucket = season_counts[season]
        bucket["queries"] += 1
        bucket["play_rows"] += rows
        bucket["min_rows"] = rows if bucket["min_rows"] is None else min(bucket["min_rows"], rows)
        bucket["max_rows"] = rows if bucket["max_rows"] is None else max(bucket["max_rows"], rows)

    summary_rows = [
        {
            "season": season,
            "queries": values["queries"],
            "play_rows": values["play_rows"],
            "min_query_rows": values["min_rows"],
            "max_query_rows": values["max_rows"],
        }
        for season, values in sorted(season_counts.items())
    ]
    write_csv(raw_dir / "season_summary.csv", summary_rows)
    return {
        "status_counts": dict(status_counts),
        "season_summary_csv": str(raw_dir / "season_summary.csv"),
        "season_summary": summary_rows,
    }


def combine(raw_dir: Path, out_path: Path, progress_rows: list[dict[str, str]]) -> dict[str, object]:
    csv_paths = [Path(row["csv"]) for row in progress_rows if row["status"] == "ok" and row.get("csv")]
    if not csv_paths:
        raise ValueError("No successful CSV paths found in progress.csv")

    first = pd.read_csv(csv_paths[0], dtype=str, keep_default_na=False)
    columns = list(first.columns)
    schema = pa.schema([pa.field(column, pa.string()) for column in columns])

    total_rows = 0
    writer = pq.ParquetWriter(out_path, schema=schema, compression="zstd")
    try:
        for index, path in enumerate(csv_paths, start=1):
            df = first if index == 1 else pd.read_csv(path, dtype=str, keep_default_na=False)
            for column in columns:
                if column not in df.columns:
                    df[column] = ""
            df = df[columns].astype(str)
            total_rows += len(df)
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
            writer.write_table(table)
            if index % 500 == 0:
                print(f"combined {index}/{len(csv_paths)} files rows={total_rows}", flush=True)
    finally:
        writer.close()

    return {
        "parquet": str(out_path),
        "files_combined": len(csv_paths),
        "rows_combined": total_rows,
        "columns": len(columns),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw-dir", type=Path, default=DEFAULT_RAW_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = parser.parse_args()

    progress_rows = read_progress(args.raw_dir)
    summary = build_summary(args.raw_dir, progress_rows)
    combined = combine(args.raw_dir, args.out, progress_rows)
    manifest = {
        **combined,
        **summary,
    }
    manifest_path = args.raw_dir / "combined_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2), flush=True)


if __name__ == "__main__":
    main()
