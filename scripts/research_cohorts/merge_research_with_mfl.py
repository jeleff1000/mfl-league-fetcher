"""Create an ephemeral, same-schema union of the research corpus and MFL cache.

The registered MFL cache is authoritative for the exact (db_name, year) pairs
recorded in its immutable index.  Source caches are attached read-only; this
script never saves, deletes, or modifies them.
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import duckdb


TABLES = ("league_settings", "matchup", "player_fantasy")


def qi(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def table_columns(
    con: duckdb.DuckDBPyConnection, database: str, schema: str, table: str
) -> list[tuple[str, str]]:
    return [(str(name), str(type_name)) for name, type_name, *_ in con.execute(
        f"DESCRIBE {database}.{qi(schema)}.{qi(table)}"
    ).fetchall()]


def public_tables(con: duckdb.DuckDBPyConnection, database: str) -> list[str]:
    return [
        str(row[0])
        for row in con.execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name=? AND schema_name='public'",
            [database],
        ).fetchall()
    ]


def table_schema(con: duckdb.DuckDBPyConnection, database: str, table: str) -> str | None:
    row = con.execute(
        "SELECT schema_name FROM duckdb_tables() "
        "WHERE database_name=? AND table_name=? "
        "ORDER BY CASE schema_name WHEN 'public' THEN 0 WHEN 'main' THEN 1 ELSE 2 END LIMIT 1",
        [database, table],
    ).fetchone()
    return str(row[0]) if row else None


def relation(database: str, schema: str, table: str) -> str:
    return f"{database}.{qi(schema)}.{qi(table)}"


def parse_ledger(raw: str) -> dict[int, int]:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SystemExit("expected ledger must be a JSON object of year to league count")
    return {int(year): int(count) for year, count in value.items()}


def protected_mfl_pairs(index: dict, expected_ledger: dict[int, int]) -> list[tuple[str, int]]:
    accepted = {int(year): int(count) for year, count in index.get("accepted_by_year", {}).items()}
    if accepted != expected_ledger or int(index.get("league_count", -1)) != sum(expected_ledger.values()):
        raise SystemExit(
            "protected MFL league-year ledger mismatch: "
            f"expected={expected_ledger}, accepted={accepted}, league_count={index.get('league_count')}"
        )
    pairs: list[tuple[str, int]] = []
    for row in index.get("leagues", []):
        db_name = row.get("db_name")
        season = row.get("season")
        if not db_name or season is None:
            raise SystemExit(f"MFL index has incomplete league record: {row}")
        pairs.append((str(db_name), int(season)))
    if len(pairs) != len(set(pairs)):
        raise SystemExit("MFL index contains duplicate protected league-year keys")
    if len(pairs) != sum(expected_ledger.values()):
        raise SystemExit(
            f"MFL index pair count mismatch: expected={sum(expected_ledger.values())}, actual={len(pairs)}"
        )
    actual_ledger = Counter(year for _, year in pairs)
    if dict(sorted(actual_ledger.items())) != dict(sorted(expected_ledger.items())):
        raise SystemExit(
            f"MFL index pair-year ledger mismatch: expected={expected_ledger}, actual={dict(actual_ledger)}"
        )
    return pairs


def count_rows_for_pairs(con: duckdb.DuckDBPyConnection, relation: str) -> int:
    return int(con.execute(
        f"SELECT COUNT(*) FROM {relation} s JOIN _mfl_pairs p "
        "ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=p.year"
    ).fetchone()[0])


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--mfl", type=Path, required=True)
    parser.add_argument("--mfl-index", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--expected-ledger", required=True)
    args = parser.parse_args()

    if args.out.exists():
        args.out.unlink()
    expected_ledger = parse_ledger(args.expected_ledger)
    index = json.loads(args.mfl_index.read_text(encoding="utf-8"))
    pairs = protected_mfl_pairs(index, expected_ledger)

    con = duckdb.connect(str(args.out))
    try:
        base_path = str(args.base.resolve()).replace("'", "''")
        mfl_path = str(args.mfl.resolve()).replace("'", "''")
        con.execute(f"ATTACH '{base_path}' AS base (READ_ONLY)")
        con.execute(f"ATTACH '{mfl_path}' AS mfl (READ_ONLY)")
        con.execute("CREATE SCHEMA public")
        base_tables = public_tables(con, "base")
        missing = [table for table in TABLES if table not in base_tables]
        if missing:
            raise SystemExit(f"base corpus missing required tables: {missing}")
        mfl_schemas = {table: table_schema(con, "mfl", table) for table in TABLES}
        missing = [table for table, schema in mfl_schemas.items() if schema is None]
        if missing:
            raise SystemExit(f"MFL cache missing required tables: {missing}")

        con.execute("CREATE TEMP TABLE _mfl_pairs (db_name VARCHAR, year INTEGER)")
        con.executemany("INSERT INTO _mfl_pairs VALUES (?, ?)", pairs)
        report: dict[str, object] = {
            "schema_unchanged": True,
            "new_lineage": False,
            "protected_mfl_ledger": dict(sorted(expected_ledger.items())),
            "protected_mfl_league_years": len(pairs),
            "tables": {},
        }

        for table in base_tables:
            table_started = time.monotonic()
            base_schema = table_columns(con, "base", "public", table)
            columns = ", ".join(qi(name) for name, _ in base_schema)
            if table not in TABLES:
                con.execute(f"CREATE TABLE public.{qi(table)} AS SELECT {columns} FROM base.public.{qi(table)}")
                continue
            mfl_schema_name = mfl_schemas[table]
            assert mfl_schema_name is not None
            mfl_relation = relation("mfl", mfl_schema_name, table)
            mfl_schema = table_columns(con, "mfl", mfl_schema_name, table)
            mfl_columns = {name for name, _ in mfl_schema}
            missing_keys = {"db_name", "year"} - mfl_columns
            if missing_keys:
                raise SystemExit(f"MFL {table} is missing required merge keys: {sorted(missing_keys)}")
            # The research corpus schema is canonical at this boundary.  MFL
            # has a wider settings contract, so project by canonical column
            # name and cast to the corpus type instead of requiring identical
            # physical schemas.  MFL-only fields remain in the protected MFL
            # cache and are intentionally not invented in the research lake.
            source_columns = ", ".join(
                (
                    f"CAST(s.{qi(name)} AS {type_name}) AS {qi(name)}"
                    if name in mfl_columns
                    else f"CAST(NULL AS {type_name}) AS {qi(name)}"
                )
                for name, type_name in base_schema
            )
            source_rows = count_rows_for_pairs(con, mfl_relation)
            con.execute(
                "CREATE OR REPLACE TEMP TABLE _mfl_table_pairs AS "
                f"SELECT DISTINCT s.db_name, CAST(s.year AS INTEGER) AS year "
                f"FROM {mfl_relation} s JOIN _mfl_pairs p "
                "ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=p.year"
            )
            source_pair_count = int(con.execute("SELECT COUNT(*) FROM _mfl_table_pairs").fetchone()[0])
            print(
                f"[mfl-merge] phase=table_source table={table} "
                f"mfl_source_rows={source_rows} source_league_years={source_pair_count} "
                f"missing_league_years={len(pairs) - source_pair_count}",
                flush=True,
            )
            # A registered MFL league-year can legitimately lack one table
            # (notably league_settings).  That is not a deletion signal: only
            # source pairs physically present in this table supersede the base.
            base_preserved = int(con.execute(
                f"SELECT COUNT(*) FROM base.public.{qi(table)} b WHERE NOT EXISTS "
                "(SELECT 1 FROM _mfl_table_pairs p WHERE b.db_name=p.db_name "
                "AND CAST(b.year AS INTEGER)=p.year)"
            ).fetchone()[0])
            con.execute(
                f"CREATE TABLE public.{qi(table)} AS "
                f"SELECT {columns} FROM base.public.{qi(table)} b WHERE NOT EXISTS "
                "(SELECT 1 FROM _mfl_table_pairs p WHERE b.db_name=p.db_name "
                "AND CAST(b.year AS INTEGER)=p.year) "
                "UNION ALL "
                f"SELECT {source_columns} FROM {mfl_relation} s JOIN _mfl_pairs p "
                "ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=p.year"
            )
            output_source_rows = int(con.execute(
                f"SELECT COUNT(*) FROM public.{qi(table)} s JOIN _mfl_table_pairs p "
                "ON s.db_name=p.db_name AND CAST(s.year AS INTEGER)=p.year"
            ).fetchone()[0])
            output_preserved = int(con.execute(
                f"SELECT COUNT(*) FROM public.{qi(table)} b WHERE NOT EXISTS "
                "(SELECT 1 FROM _mfl_table_pairs p WHERE b.db_name=p.db_name "
                "AND CAST(b.year AS INTEGER)=p.year)"
            ).fetchone()[0])
            if output_source_rows != source_rows or output_preserved != base_preserved:
                raise SystemExit(
                    f"row preservation failed for {table}: source={source_rows}/{output_source_rows}, "
                    f"preserved={base_preserved}/{output_preserved}"
                )
            report["tables"][table] = {
                "mfl_source_rows": source_rows,
                "mfl_source_league_years": source_pair_count,
                "mfl_missing_league_years": len(pairs) - source_pair_count,
                "preserved_base_rows": base_preserved,
                "output_rows": int(con.execute(f"SELECT COUNT(*) FROM public.{qi(table)}").fetchone()[0]),
            }
            print(
                f"[mfl-merge] phase=table_complete table={table} "
                f"output_rows={report['tables'][table]['output_rows']} "
                f"seconds={time.monotonic() - table_started:.3f}",
                flush=True,
            )

        args.out.with_suffix(".json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
