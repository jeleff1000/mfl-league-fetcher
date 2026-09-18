"""Rebuild a DuckDB file table-by-table while emptying known damaged tables.

This is a storage-recovery helper. It does not transform league data. Every
healthy table is copied exactly from the source file using its catalog DDL;
explicitly excluded tables keep their exact schema but start empty so their
canonical recovery can be applied afterward.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def rebuild_database(
    source_path: Path,
    target_path: Path,
    *,
    empty_tables: set[tuple[str, str]],
    skip_tables: set[tuple[str, str]] | None = None,
    overwrite: bool = False,
) -> dict:
    source_path = source_path.resolve()
    target_path = target_path.resolve()
    if source_path == target_path:
        raise ValueError("source and target database paths must differ")
    if not source_path.is_file():
        raise FileNotFoundError(source_path)
    if target_path.exists():
        if not overwrite:
            raise FileExistsError(target_path)
        target_path.unlink()
    Path(f"{target_path}.wal").unlink(missing_ok=True)

    conn = duckdb.connect(str(target_path))
    attached = False
    copied: dict[str, dict[str, int | bool]] = {}
    skip_tables = set(skip_tables or ())
    rebuild_started = time.monotonic()
    try:
        conn.execute(f"ATTACH {_quote_literal(source_path.as_posix())} AS source (READ_ONLY)")
        attached = True

        unsupported = {
            "views": int(
                conn.execute(
                    "SELECT COUNT(*) FROM duckdb_views() "
                    "WHERE database_name = 'source' AND NOT internal"
                ).fetchone()[0]
            ),
            "indexes": int(
                conn.execute(
                    "SELECT COUNT(*) FROM duckdb_indexes() WHERE database_name = 'source'"
                ).fetchone()[0]
            ),
            "sequences": int(
                conn.execute(
                    "SELECT COUNT(*) FROM duckdb_sequences() WHERE database_name = 'source'"
                ).fetchone()[0]
            ),
            "types": int(
                conn.execute(
                    "SELECT COUNT(*) FROM duckdb_types() "
                    "WHERE database_name = 'source' AND NOT internal"
                ).fetchone()[0]
            ),
        }
        present_unsupported = {name: count for name, count in unsupported.items() if count}
        if present_unsupported:
            raise RuntimeError(
                "source contains unsupported catalog objects; refuse partial rebuild: "
                f"{present_unsupported}"
            )

        schemas = [
            str(row[0])
            for row in conn.execute(
                "SELECT schema_name FROM duckdb_schemas() "
                "WHERE database_name = 'source' AND NOT internal ORDER BY schema_name"
            ).fetchall()
        ]
        for schema in schemas:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_quote_identifier(schema)}")

        tables = conn.execute(
            "SELECT schema_name, table_name, sql FROM duckdb_tables() "
            "WHERE database_name = 'source' AND NOT internal "
            "ORDER BY schema_name, table_name"
        ).fetchall()
        available = {(str(schema), str(table)) for schema, table, _ddl in tables}
        missing_exclusions = sorted(empty_tables - available)
        if missing_exclusions:
            raise RuntimeError(f"excluded tables are absent from source: {missing_exclusions}")
        missing_skips = sorted(skip_tables - available)
        if missing_skips:
            raise RuntimeError(f"skipped tables are absent from source: {missing_skips}")

        copy_tables = [
            row for row in tables
            if (str(row[0]), str(row[1])) not in skip_tables
        ]
        print(
            json.dumps(
                {
                    "event": "rebuild_started",
                    "tables_total": len(copy_tables),
                    "tables_skipped": len(skip_tables),
                },
                sort_keys=True,
            ),
            flush=True,
        )
        for index, (schema_raw, table_raw, ddl_raw) in enumerate(copy_tables, start=1):
            schema = str(schema_raw)
            table = str(table_raw)
            ddl = str(ddl_raw or "").strip()
            excluded = (schema, table) in empty_tables
            table_started = time.monotonic()
            print(
                json.dumps(
                    {
                        "event": "copy_table_started",
                        "index": index,
                        "total": len(copy_tables),
                        "table": f"{schema}.{table}",
                        "emptied": excluded,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            if not ddl.upper().startswith("CREATE TABLE"):
                raise RuntimeError(f"source has no usable DDL for {schema}.{table}")
            conn.execute(ddl)
            target_ref = f"{_quote_identifier(schema)}.{_quote_identifier(table)}"
            source_ref = (
                f"source.{_quote_identifier(schema)}.{_quote_identifier(table)}"
            )
            source_rows = -1
            if not excluded:
                conn.execute(f"INSERT INTO {target_ref} BY NAME SELECT * FROM {source_ref}")
                source_rows = int(conn.execute(f"SELECT COUNT(*) FROM {source_ref}").fetchone()[0])
            target_rows = int(conn.execute(f"SELECT COUNT(*) FROM {target_ref}").fetchone()[0])
            if excluded:
                if target_rows != 0:
                    raise RuntimeError(f"excluded table is not empty: {schema}.{table}")
            elif target_rows != source_rows:
                raise RuntimeError(
                    f"row-count mismatch for {schema}.{table}: "
                    f"source={source_rows}, target={target_rows}"
                )
            copied[f"{schema}.{table}"] = {
                "source_rows": source_rows,
                "target_rows": target_rows,
                "emptied": excluded,
            }
            print(
                json.dumps(
                    {
                        "event": "copy_table_completed",
                        "index": index,
                        "total": len(copy_tables),
                        "table": f"{schema}.{table}",
                        "source_rows": source_rows,
                        "target_rows": target_rows,
                        "emptied": excluded,
                        "elapsed_seconds": round(time.monotonic() - table_started, 3),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )

        conn.execute("CHECKPOINT")
        conn.execute("DETACH source")
        attached = False
    finally:
        if attached:
            try:
                conn.execute("DETACH source")
            except Exception:
                pass
        conn.close()

    wal_path = Path(f"{target_path}.wal")
    if wal_path.exists() and wal_path.stat().st_size:
        raise RuntimeError(f"rebuilt database retained a WAL: {wal_path}")
    print(
        json.dumps(
            {
                "event": "rebuild_completed",
                "tables_copied": len(copied),
                "tables_skipped": len(skip_tables),
                "elapsed_seconds": round(time.monotonic() - rebuild_started, 3),
            },
            sort_keys=True,
        ),
        flush=True,
    )
    return {
        "source": str(source_path),
        "target": str(target_path),
        "source_bytes": source_path.stat().st_size,
        "target_bytes": target_path.stat().st_size,
        "schemas": schemas,
        "tables": copied,
        "empty_tables": sorted(f"{schema}.{table}" for schema, table in empty_tables),
        "skipped_tables": sorted(f"{schema}.{table}" for schema, table in skip_tables),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, type=Path)
    parser.add_argument("--target", required=True, type=Path)
    parser.add_argument(
        "--empty-table",
        action="append",
        default=[],
        help="schema.table to recreate empty; repeat for each damaged table",
    )
    parser.add_argument(
        "--skip-table",
        action="append",
        default=[],
        help="schema.table to omit entirely; repeat only for quarantined objects",
    )
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()
    empty_tables: set[tuple[str, str]] = set()
    for value in args.empty_table:
        parts = value.split(".", 1)
        if len(parts) != 2 or not all(parts):
            parser.error(f"--empty-table must be schema.table: {value!r}")
        empty_tables.add((parts[0], parts[1]))
    skip_tables: set[tuple[str, str]] = set()
    for value in args.skip_table:
        parts = value.split(".", 1)
        if len(parts) != 2 or not all(parts):
            parser.error(f"--skip-table must be schema.table: {value!r}")
        skip_tables.add((parts[0], parts[1]))
    result = rebuild_database(
        args.source,
        args.target,
        empty_tables=empty_tables,
        skip_tables=skip_tables,
        overwrite=args.overwrite,
    )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
