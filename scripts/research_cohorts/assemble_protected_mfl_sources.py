"""Assemble every immutable MFL source chunk into one ephemeral input database.

Inputs are always attached read-only.  The output is a runner-local staging
database and index; it is never saved as a cache or used to replace a source.
"""
from __future__ import annotations

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import duckdb


TABLES = ("league_settings", "matchup", "player_fantasy")


def qi(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def parse_ledger(raw: str) -> dict[int, int]:
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise SystemExit("expected ledger must be a JSON object")
    return {int(year): int(count) for year, count in value.items()}


def source_schema(con: duckdb.DuckDBPyConnection, database: str, table: str) -> str:
    row = con.execute(
        "SELECT schema_name FROM duckdb_tables() WHERE database_name=? AND table_name=? "
        "ORDER BY CASE schema_name WHEN 'public' THEN 0 WHEN 'main' THEN 1 ELSE 2 END LIMIT 1",
        [database, table],
    ).fetchone()
    if not row:
        raise SystemExit(f"source database {database} is missing {table}")
    return str(row[0])


def source_columns(
    con: duckdb.DuckDBPyConnection, database: str, schema: str, table: str
) -> list[tuple[str, str]]:
    return [
        (str(name), str(type_name))
        for name, type_name, *_ in con.execute(f"DESCRIBE {database}.{qi(schema)}.{qi(table)}").fetchall()
    ]


def indexed_records(path: Path) -> dict[tuple[str, int], dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    records: dict[tuple[str, int], dict] = {}
    for row in payload.get("leagues", []):
        db_name, season = row.get("db_name"), row.get("season")
        if not db_name or season is None:
            raise SystemExit(f"incomplete MFL index record in {path}: {row}")
        key = (str(db_name), int(season))
        if key in records:
            raise SystemExit(f"duplicate MFL index pair in {path}: {key}")
        records[key] = row
    return records


def physical_pairs(
    con: duckdb.DuckDBPyConnection, database: str, schemas: dict[str, str]
) -> set[tuple[str, int]]:
    table_pairs: dict[str, set[tuple[str, int]]] = {}
    for table in TABLES:
        relation = f"{database}.{qi(schemas[table])}.{qi(table)}"
        columns = {name for name, _ in source_columns(con, database, schemas[table], table)}
        missing = {"db_name", "year"} - columns
        if missing:
            raise SystemExit(f"{database}.{table} lacks source keys: {sorted(missing)}")
        table_pairs[table] = {
            (str(db_name), int(year))
            for db_name, year in con.execute(
                f"SELECT DISTINCT db_name, CAST(year AS INTEGER) FROM {relation}"
            ).fetchall()
        }
    reference = table_pairs[TABLES[0]]
    mismatched = {table: len(pairs) for table, pairs in table_pairs.items() if pairs != reference}
    if mismatched:
        raise SystemExit(f"source tables do not contain the same league-years: {mismatched}")
    return reference


def discover_population_sources(root: Path) -> list[tuple[Path, Path]]:
    sources: list[tuple[Path, Path]] = []
    for chunk in sorted(root.rglob("mfl_register_chunk.duckdb")):
        index = chunk.parent.parent / "index_state" / "mfl_register_all_runs.json"
        if not index.is_file():
            raise SystemExit(f"population chunk lacks sibling index: {chunk}")
        sources.append((chunk, index))
    if not sources:
        raise SystemExit(f"no population MFL chunks found in {root}")
    return sources


def assemble(
    recovery: Path,
    recovery_index: Path,
    population_root: Path,
    output: Path,
    output_index: Path,
    expected_ledger: dict[int, int],
    expected_population_total: int,
) -> dict[str, object]:
    if output.exists():
        output.unlink()
    all_sources = [("recovery", recovery, recovery_index)] + [
        ("population", chunk, index) for chunk, index in discover_population_sources(population_root)
    ]
    con = duckdb.connect(str(output))
    selected: dict[tuple[str, int], dict] = {}
    schemas: dict[str, list[tuple[str, str]]] = {}
    source_counts: Counter[str] = Counter()
    selected_counts: Counter[str] = Counter()
    table_rows: Counter[str] = Counter()
    overlapping_league_years = 0
    started_at = time.monotonic()
    try:
        for ordinal, (source_kind, source_path, index_path) in enumerate(all_sources):
            database = f"source_{ordinal}"
            print(
                f"[mfl-assemble] phase=source_begin source={ordinal + 1}/{len(all_sources)} "
                f"kind={source_kind} path={source_path} elapsed={time.monotonic() - started_at:.3f}",
                flush=True,
            )
            escaped = str(source_path.resolve()).replace("'", "''")
            con.execute(f"ATTACH '{escaped}' AS {database} (READ_ONLY)")
            try:
                table_schemas = {table: source_schema(con, database, table) for table in TABLES}
                pairs = physical_pairs(con, database, table_schemas)
                records = indexed_records(index_path)
                print(
                    f"[mfl-assemble] phase=source_inventory source={ordinal + 1}/{len(all_sources)} "
                    f"kind={source_kind} physical_league_years={len(pairs)} "
                    f"indexed_league_years={len(records)} elapsed={time.monotonic() - started_at:.3f}",
                    flush=True,
                )
                unknown = pairs - set(records)
                if unknown:
                    raise SystemExit(f"{source_path} has physical rows absent from its index: {next(iter(unknown))}")
                duplicate = pairs & set(selected)
                new_pairs = pairs - duplicate
                overlapping_league_years += len(duplicate)
                print(
                    f"[mfl-assemble] phase=source_overlap source={ordinal + 1}/{len(all_sources)} "
                    f"kind={source_kind} overlapping_league_years={len(duplicate)} "
                    f"selected_league_years={len(new_pairs)} "
                    f"precedence={'existing' if duplicate else 'none'} "
                    f"elapsed={time.monotonic() - started_at:.3f}",
                    flush=True,
                )
                for key in new_pairs:
                    selected[key] = records[key]
                source_counts[source_kind] += len(pairs)
                selected_counts[source_kind] += len(new_pairs)
                if duplicate:
                    con.execute(
                        "CREATE OR REPLACE TEMP TABLE _source_overlap_pairs "
                        "(db_name VARCHAR, year INTEGER)"
                    )
                    con.executemany(
                        "INSERT INTO _source_overlap_pairs VALUES (?, ?)",
                        sorted(duplicate, key=lambda key: (key[1], key[0])),
                    )
                if new_pairs:
                    con.execute(
                        "CREATE OR REPLACE TEMP TABLE _source_selected_pairs "
                        "(db_name VARCHAR, year INTEGER)"
                    )
                    con.executemany(
                        "INSERT INTO _source_selected_pairs VALUES (?, ?)",
                        sorted(new_pairs, key=lambda key: (key[1], key[0])),
                    )
                for table in TABLES:
                    schema = source_columns(con, database, table_schemas[table], table)
                    if table in schemas and schemas[table] != schema:
                        raise SystemExit(f"MFL source schema differs for {table}")
                    schemas.setdefault(table, schema)
                    relation = f"{database}.{qi(table_schemas[table])}.{qi(table)}"
                    columns = ", ".join(f"s.{qi(name)}" for name, _ in schema)
                    if duplicate:
                        existing_columns = ", ".join(f"o.{qi(name)}" for name, _ in schema)
                        existing_overlap = (
                            f"SELECT {existing_columns} FROM {qi(table)} AS o "
                            "INNER JOIN _source_overlap_pairs AS p "
                            "ON o.db_name = p.db_name AND CAST(o.year AS INTEGER) = p.year"
                        )
                        source_overlap = (
                            f"SELECT {columns} FROM {relation} AS s "
                            "INNER JOIN _source_overlap_pairs AS p "
                            "ON s.db_name = p.db_name AND CAST(s.year AS INTEGER) = p.year"
                        )
                        existing_rows, source_rows, existing_only, source_only = con.execute(
                            "WITH existing_rows AS (" + existing_overlap + "), "
                            "source_rows AS (" + source_overlap + ") "
                            "SELECT "
                            "(SELECT COUNT(*) FROM existing_rows), "
                            "(SELECT COUNT(*) FROM source_rows), "
                            "(SELECT COUNT(*) FROM (SELECT * FROM existing_rows EXCEPT ALL SELECT * FROM source_rows)), "
                            "(SELECT COUNT(*) FROM (SELECT * FROM source_rows EXCEPT ALL SELECT * FROM existing_rows))"
                        ).fetchone()
                        if existing_only or source_only:
                            raise SystemExit(
                                "conflicting MFL source rows: "
                                f"source={source_path} table={table} "
                                f"overlapping_league_years={len(duplicate)} "
                                f"existing_rows={existing_rows} source_rows={source_rows} "
                                f"existing_only={existing_only} source_only={source_only}"
                            )
                    selected_rows = (
                        f"SELECT {columns} FROM {relation} AS s "
                        "INNER JOIN _source_selected_pairs AS p "
                        "ON s.db_name = p.db_name AND CAST(s.year AS INTEGER) = p.year"
                    )
                    if not new_pairs:
                        appended = 0
                    elif table not in table_rows:
                        con.execute(f"CREATE TABLE {qi(table)} AS {selected_rows}")
                        appended = int(con.execute(f"SELECT COUNT(*) FROM ({selected_rows})").fetchone()[0])
                    else:
                        con.execute(f"INSERT INTO {qi(table)} {selected_rows}")
                        appended = int(con.execute(f"SELECT COUNT(*) FROM ({selected_rows})").fetchone()[0])
                    table_rows[table] += appended
                    print(
                        f"[mfl-assemble] phase=table_append source={ordinal + 1}/{len(all_sources)} "
                        f"kind={source_kind} table={table} appended_rows={appended} "
                        f"total_rows={table_rows[table]} elapsed={time.monotonic() - started_at:.3f}",
                        flush=True,
                    )
            finally:
                con.execute(f"DETACH {database}")

        actual_ledger = Counter(year for _, year in selected)
        print(
            f"[mfl-assemble] phase=ledger_check recovery_league_years={source_counts['recovery']} "
            f"population_league_years={source_counts['population']} total_league_years={len(selected)} "
            f"recovery_selected_league_years={selected_counts['recovery']} "
            f"population_selected_league_years={selected_counts['population']} "
            f"overlapping_league_years={overlapping_league_years} "
            f"expected_population_league_years={expected_population_total} "
            f"expected_total_league_years={sum(expected_ledger.values())} "
            f"elapsed={time.monotonic() - started_at:.3f}",
            flush=True,
        )
        if dict(sorted(actual_ledger.items())) != dict(sorted(expected_ledger.items())):
            raise SystemExit(
                "full protected MFL ledger mismatch: "
                f"expected={expected_ledger}, actual={dict(sorted(actual_ledger.items()))}"
            )
        if source_counts["population"] != expected_population_total:
            raise SystemExit(
                f"population MFL chunk total mismatch: expected={expected_population_total}, "
                f"actual={source_counts['population']}"
            )
        if len(selected) != sum(expected_ledger.values()):
            raise SystemExit(f"full protected MFL count mismatch: actual={len(selected)}")
    finally:
        con.close()

    index = {
        "schema_version": "mfl_register_results_v1",
        "leagues": [selected[key] for key in sorted(selected, key=lambda key: (key[1], key[0]))],
        "accepted_by_year": {str(year): int(count) for year, count in sorted(actual_ledger.items())},
        "league_count": len(selected),
        "table_rows": dict(table_rows),
    }
    output_index.write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    proof: dict[str, object] = {
        "schema_unchanged": True,
        "new_lineage": False,
        "recovery_league_years": source_counts["recovery"],
        "population_league_years": source_counts["population"],
        "recovery_selected_league_years": selected_counts["recovery"],
        "population_selected_league_years": selected_counts["population"],
        "overlapping_league_years": overlapping_league_years,
        "protected_mfl_league_years": len(selected),
        "protected_mfl_ledger": dict(sorted(expected_ledger.items())),
        "table_rows": dict(table_rows),
    }
    output.with_name(output.stem + "_proof.json").write_text(
        json.dumps(proof, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(proof, sort_keys=True))
    return proof


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--recovery", type=Path, required=True)
    parser.add_argument("--recovery-index", type=Path, required=True)
    parser.add_argument("--population-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--out-index", type=Path, required=True)
    parser.add_argument("--expected-ledger", required=True)
    parser.add_argument("--expected-population-total", type=int, required=True)
    args = parser.parse_args()
    assemble(
        args.recovery,
        args.recovery_index,
        args.population_root,
        args.out,
        args.out_index,
        parse_ledger(args.expected_ledger),
        args.expected_population_total,
    )


if __name__ == "__main__":
    main()
