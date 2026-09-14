"""Build a row-level census of statistics encoded inside lake values."""

from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

import duckdb

from .value_domain_census import extract_json_paths, parse_numeric, stable_examples


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _source_path(source: dict[str, Any]) -> str:
    path = source.get("path") or source.get("parquet_path") or source.get("file")
    if not path:
        raise ValueError(f"source {source.get('source_id')} has no path")
    return str(path)


def scan_source(source: dict[str, Any], *, sample_limit: int = 5) -> list[dict[str, Any]]:
    path = _source_path(source)
    source_id = str(source.get("source_id") or source.get("id") or Path(path).stem)
    con = duckdb.connect()
    try:
        schema = con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchall()
        columns = {str(row[0]) for row in schema}
        configured = source.get("value_fields") or [name for name in columns if any(token in name.lower() for token in ("stat", "json", "list", "text", "html", "payload"))]
        fields = [str(name) for name in configured if str(name) in columns]
        label_fields = {str(name) for name in source.get("label_fields", []) if str(name) in columns}
        json_fields = {str(name) for name in source.get("json_fields", []) if str(name) in columns}
        year_field = str(source.get("year_field") or "year")
        select_names = [*fields]
        if year_field in columns and year_field not in select_names:
            select_names.append(year_field)
        if not select_names:
            return []
        rows = con.execute(
            f"SELECT {', '.join(_quote(name) for name in select_names)} FROM read_parquet(?)",
            [path],
        ).fetchall()
        index = {name: pos for pos, name in enumerate(select_names)}
    finally:
        con.close()

    aggregates: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        year = row[index[year_field]] if year_field in index else None
        for field in fields:
            raw = row[index[field]]
            if field in json_fields:
                values = extract_json_paths(raw)
            else:
                values = [("$." + field, raw)]
            for value_path, value in values:
                if value is None:
                    continue
                raw_label = str(value) if field in label_fields else None
                key = (field, value_path if field in json_fields else (raw_label or "__value__"))
                item = aggregates.setdefault(
                    key,
                    {
                        "source_id": source_id,
                        "table_id": source.get("table_id") or source.get("table") or source_id,
                        "field_name": field,
                        "value_path": value_path,
                        "raw_label": raw_label,
                        "row_count": 0,
                        "non_null_count": 0,
                        "numeric_parse_count": 0,
                        "observed_zero_count": 0,
                        "year_min": None,
                        "year_max": None,
                        "examples": [],
                        "scan_status": "SCANNED",
                    },
                )
                item["row_count"] += 1
                item["non_null_count"] += 1
                number, state = parse_numeric(value)
                if number is not None:
                    item["numeric_parse_count"] += 1
                if state == "OBSERVED_ZERO":
                    item["observed_zero_count"] += 1
                item["examples"].append(value)
                if isinstance(year, (int, float)):
                    item["year_min"] = year if item["year_min"] is None else min(item["year_min"], year)
                    item["year_max"] = year if item["year_max"] is None else max(item["year_max"], year)

    output = []
    for item in aggregates.values():
        item["numeric_parse_rate"] = round(item["numeric_parse_count"] / item["non_null_count"], 6) if item["non_null_count"] else None
        item["examples"] = stable_examples(item["examples"], sample_limit)
        output.append(item)
    return sorted(output, key=lambda item: (item["source_id"], item["field_name"], item["value_path"], item["raw_label"] or ""))


def _load_sources(inventory_path: Path) -> list[dict[str, Any]]:
    payload = json.loads(inventory_path.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return payload
    return payload.get("sources") or payload.get("registered_sources") or payload.get("artifacts") or []


def build_census(inventory_path: Path, output_json: Path, output_csv: Path) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    for source in _load_sources(inventory_path):
        try:
            records.extend(scan_source(source))
        except Exception as exc:  # audit output records a failure rather than hiding it
            failures.append({"source_id": str(source.get("source_id") or source.get("id") or "unknown"), "error": str(exc)})
    result = {
        "schema_version": 1,
        "inventory_path": str(inventory_path),
        "record_count": len(records),
        "source_count": len(_load_sources(inventory_path)),
        "failure_count": len(failures),
        "records": records,
        "failures": failures,
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(result, indent=2, sort_keys=True, default=str), encoding="utf-8")
    fieldnames = sorted({key for record in records for key in record})
    with output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows({key: json.dumps(value, sort_keys=True) if isinstance(value, list) else value for key, value in record.items()} for record in records)
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--output-json", type=Path, default=Path("docs/value-domain-census.json"))
    parser.add_argument("--output-csv", type=Path, default=Path("docs/value-domain-census.csv"))
    args = parser.parse_args()
    print(json.dumps(build_census(args.inventory, args.output_json, args.output_csv), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
