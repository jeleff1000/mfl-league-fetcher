#!/usr/bin/env python3
"""Profile REAL data availability per column in the canonical supertable release.

Schema presence is not availability. A column can sit in the parquet footer with
zero populated rows, or be populated only for a handful of modern seasons, or
only for one position. The coverage audit gates era/grain/position exposure on
THIS file, not on the schema snapshot.

Runs batched aggregates over explicit column lists (never SELECT *):
  per column -> non-null count, min/max year, interior missing years,
                first/last year with a non-null value, positions with values.

    python scripts/player_column_coverage/profile_local_coverage.py \
        --parquet <release>/tables/nfl_player_stats_all.parquet \
        --schema docs/player-column-coverage/source-schema.json \
        --output docs/player-column-coverage/source-coverage.json
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

YEAR_COLUMN = "year"
POSITION_COLUMN = "position"


def quote(name: str) -> str:
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def batched(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def profile_by_year(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    columns: list[str],
    batch_size: int,
) -> dict[str, dict[int, int]]:
    """Return {column: {year: non_null_count}} for every requested column."""
    result: dict[str, dict[int, int]] = {column: {} for column in columns}
    for batch in batched(columns, batch_size):
        selects = ", ".join(
            f"COUNT({quote(column)}) AS c{index}" for index, column in enumerate(batch)
        )
        rows = connection.execute(
            f"SELECT CAST({quote(YEAR_COLUMN)} AS INTEGER) AS yr, {selects} "
            f"FROM {source} GROUP BY yr ORDER BY yr"
        ).fetchall()
        for row in rows:
            year = row[0]
            if year is None:
                continue
            for index, column in enumerate(batch):
                count = row[index + 1]
                if count:
                    result[column][int(year)] = int(count)
    return result


def profile_by_position(
    connection: duckdb.DuckDBPyConnection,
    source: str,
    columns: list[str],
    batch_size: int,
) -> dict[str, list[str]]:
    """Return {column: [positions that ever carry a non-null value]}."""
    result: dict[str, set[str]] = {column: set() for column in columns}
    for batch in batched(columns, batch_size):
        selects = ", ".join(
            f"COUNT({quote(column)}) AS c{index}" for index, column in enumerate(batch)
        )
        rows = connection.execute(
            f"SELECT {quote(POSITION_COLUMN)} AS pos, {selects} "
            f"FROM {source} GROUP BY pos"
        ).fetchall()
        for row in rows:
            position = row[0]
            if position is None:
                continue
            for index, column in enumerate(batch):
                if row[index + 1]:
                    result[column].add(str(position))
    return {column: sorted(values) for column, values in result.items()}


def coverage_record(
    year_counts: dict[int, int], positions: list[str]
) -> dict[str, Any]:
    if not year_counts:
        return {
            "nonNullCount": 0,
            "minYear": None,
            "maxYear": None,
            "coveredYearCount": 0,
            "missingYears": [],
            "positionsWithValues": positions,
            "state": "empty",
        }
    years = sorted(year_counts)
    covered = set(years)
    missing = [year for year in range(years[0], years[-1] + 1) if year not in covered]
    return {
        "nonNullCount": sum(year_counts.values()),
        "minYear": years[0],
        "maxYear": years[-1],
        "coveredYearCount": len(years),
        "missingYears": missing,
        "positionsWithValues": positions,
        "state": "partial" if missing else "continuous",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument("--schema", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=120)
    parser.add_argument("--skip-positions", action="store_true")
    args = parser.parse_args()

    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    columns = [entry["name"] for entry in schema["columns"]]

    connection = duckdb.connect()
    connection.execute("PRAGMA threads=4")
    source = f"read_parquet('{args.parquet.as_posix()}')"

    print(f"profiling {len(columns)} columns by year ...", flush=True)
    by_year = profile_by_year(connection, source, columns, args.batch_size)

    if args.skip_positions:
        by_position = {column: [] for column in columns}
    else:
        print("profiling by position ...", flush=True)
        by_position = profile_by_position(connection, source, columns, args.batch_size)

    total_rows = connection.execute(f"SELECT COUNT(*) FROM {source}").fetchone()[0]

    records = {
        column: coverage_record(by_year[column], by_position[column])
        for column in columns
    }
    empty = sorted(name for name, record in records.items() if record["state"] == "empty")
    payload = {
        "release_id": schema["release_id"],
        "schema_fingerprint": schema["schema_fingerprint"],
        "rows_total": int(total_rows),
        "column_count": len(records),
        "summary": {
            "emptyColumns": empty,
            "emptyColumnCount": len(empty),
            "partialColumnCount": sum(
                1 for record in records.values() if record["state"] == "partial"
            ),
        },
        "columns": dict(sorted(records.items())),
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    print(f"{len(records)} columns, {total_rows} rows -> {args.output}")
    print(f"  empty: {len(empty)}")
    print(f"  partial (interior gaps): {payload['summary']['partialColumnCount']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
