from __future__ import annotations

import glob
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .census import CensusDataset
from .gate_planes import Finding


def _hash(payload: object) -> str:
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"sha256:{hashlib.sha256(body.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True)
class FileInventory:
    path: str
    row_count: int
    size_bytes: int
    schema: tuple[tuple[str, str], ...]
    metadata_fingerprint: str


@dataclass(frozen=True)
class ColumnProfile:
    field: str
    arrow_type: str
    null_count: int | None
    non_null_count: int | None
    zero_count: int | None


@dataclass(frozen=True)
class DatasetInventory:
    dataset_id: str
    files: tuple[FileInventory, ...]
    observed_years: tuple[int, ...]
    column_profiles: dict[str, ColumnProfile]
    findings: tuple[Finding, ...]
    snapshot_fingerprint: str

    @property
    def status(self) -> str:
        return "fail" if any(item.severity == "fail" for item in self.findings) else "pass"


def _file_metadata(path: Path, root: Path) -> tuple[FileInventory, pq.ParquetFile]:
    parquet = pq.ParquetFile(path)
    schema = tuple((field.name, str(field.type)) for field in parquet.schema_arrow)
    row_groups: list[dict[str, Any]] = []
    for group_index in range(parquet.metadata.num_row_groups):
        group = parquet.metadata.row_group(group_index)
        columns: list[dict[str, Any]] = []
        for column_index in range(group.num_columns):
            column = group.column(column_index)
            stats = column.statistics
            columns.append(
                {
                    "path": column.path_in_schema,
                    "null_count": stats.null_count if stats is not None else None,
                    "min": str(stats.min) if stats is not None and stats.has_min_max else None,
                    "max": str(stats.max) if stats is not None and stats.has_min_max else None,
                }
            )
        row_groups.append({"rows": group.num_rows, "columns": columns})
    relative = path.relative_to(root).as_posix()
    payload = {
        "path": relative,
        "size_bytes": path.stat().st_size,
        "rows": parquet.metadata.num_rows,
        "schema": schema,
        "row_groups": row_groups,
    }
    return (
        FileInventory(
            path=relative,
            row_count=parquet.metadata.num_rows,
            size_bytes=path.stat().st_size,
            schema=schema,
            metadata_fingerprint=_hash(payload),
        ),
        parquet,
    )


def _profile_column(field: pa.Field, parquets: list[pq.ParquetFile]) -> ColumnProfile:
    null_count = 0
    non_null_count = 0
    zero_count = 0
    counts_known = True
    zero_known = pa.types.is_integer(field.type) or pa.types.is_floating(field.type)
    for parquet in parquets:
        index = parquet.schema_arrow.get_field_index(field.name)
        if index < 0:
            counts_known = False
            zero_known = False
            continue
        for group_index in range(parquet.metadata.num_row_groups):
            row_group = parquet.metadata.row_group(group_index)
            if row_group.num_rows == 0:
                continue
            column = row_group.column(index)
            stats = column.statistics
            if stats is None or stats.null_count is None:
                counts_known = False
            else:
                null_count += stats.null_count
                non_null_count += column.num_values - stats.null_count
            if not zero_known or stats is None or not stats.has_min_max:
                zero_known = False
                continue
            minimum, maximum = stats.min, stats.max
            if minimum == 0 and maximum == 0:
                zero_count += column.num_values - (stats.null_count or 0)
            elif minimum > 0 or maximum < 0:
                continue
            else:
                zero_known = False
    return ColumnProfile(
        field=field.name,
        arrow_type=str(field.type),
        null_count=null_count if counts_known else None,
        non_null_count=non_null_count if counts_known else None,
        zero_count=zero_count if zero_known else None,
    )


def _observed_years(paths: tuple[Path, ...], year_field: str | None) -> tuple[int, ...]:
    if year_field is None or not paths:
        return ()
    normalized = [path.as_posix() for path in paths]
    quoted_year = f'"{year_field.replace(chr(34), chr(34) * 2)}"'
    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"SELECT DISTINCT CAST({quoted_year} AS INTEGER) AS year "
            "FROM read_parquet(?, union_by_name=true) "
            f"WHERE {quoted_year} IS NOT NULL ORDER BY year",
            [normalized],
        ).fetchall()
    finally:
        connection.close()
    return tuple(row[0] for row in rows)


def inventory_dataset(
    lake_root: str | Path,
    dataset: CensusDataset,
    *,
    registered_fields: set[str],
) -> DatasetInventory:
    root = Path(lake_root).resolve()
    matched: set[Path] = set()
    findings: list[Finding] = []
    for pattern in dataset.physical_globs:
        pattern_matches = {
            Path(item).resolve()
            for item in glob.glob(str(root / pattern), recursive=True)
            if Path(item).is_file()
        }
        if not pattern_matches:
            findings.append(
                Finding(
                    code="DATASET_GLOB_UNMATCHED",
                    severity="fail",
                    scope={"dataset_id": dataset.dataset_id, "physical_glob": pattern},
                    evidence_refs=(),
                    remediation="restore, repin, or version the finite source census",
                )
            )
        matched.update(pattern_matches)

    paths = tuple(sorted(matched, key=lambda item: item.as_posix().casefold()))
    file_inventories: list[FileInventory] = []
    parquets: list[pq.ParquetFile] = []
    for path in paths:
        file_inventory, parquet = _file_metadata(path, root)
        file_inventories.append(file_inventory)
        parquets.append(parquet)
        if file_inventory.row_count == 0:
            findings.append(
                Finding(
                    code="EMPTY_SHARD",
                    severity="fail",
                    scope={"dataset_id": dataset.dataset_id, "path": file_inventory.path},
                    evidence_refs=(file_inventory.metadata_fingerprint,),
                    remediation="repair or explicitly exclude the empty shard in a new census version",
                )
            )

    schemas = {item.schema for item in file_inventories}
    if len(schemas) != dataset.schema_variant_count:
        findings.append(
            Finding(
                code="SCHEMA_DRIFT",
                severity="fail",
                scope={
                    "dataset_id": dataset.dataset_id,
                    "schema_count": len(schemas),
                    "expected_schema_count": dataset.schema_variant_count,
                },
                evidence_refs=tuple(item.metadata_fingerprint for item in file_inventories),
                remediation="version and map the schema variants",
            )
        )

    fields_by_name: dict[str, pa.Field] = {}
    for parquet in parquets:
        for field in parquet.schema_arrow:
            fields_by_name.setdefault(field.name, field)
    for field_name in sorted(set(fields_by_name) - registered_fields):
        findings.append(
            Finding(
                code="UNREGISTERED_FIELD",
                severity="fail",
                scope={"dataset_id": dataset.dataset_id, "field": field_name},
                evidence_refs=tuple(item.metadata_fingerprint for item in file_inventories),
                remediation="map the field to an atom or add a versioned exclusion",
            )
        )

    observed_years = _observed_years(
        paths,
        dataset.year_field if dataset.year_field in fields_by_name else None,
    )
    if dataset.year_start is not None and dataset.year_end is not None:
        expected_years = (
            set(dataset.required_years)
            if dataset.required_years
            else set(range(dataset.year_start, dataset.year_end + 1))
        )
        for year in sorted(expected_years - set(observed_years)):
            findings.append(
                Finding(
                    code="MISSING_REQUIRED_YEAR",
                    severity="fail",
                    scope={"dataset_id": dataset.dataset_id, "year": year},
                    evidence_refs=tuple(item.metadata_fingerprint for item in file_inventories),
                    remediation="restore the year or narrow the versioned coverage contract",
                )
            )

    profiles = {
        name: _profile_column(field, parquets)
        for name, field in sorted(fields_by_name.items())
    }
    snapshot_payload = {
        "dataset_id": dataset.dataset_id,
        "files": [item.metadata_fingerprint for item in file_inventories],
        "observed_years": observed_years,
        "profiles": {
            name: {
                "type": profile.arrow_type,
                "null_count": profile.null_count,
                "non_null_count": profile.non_null_count,
                "zero_count": profile.zero_count,
            }
            for name, profile in profiles.items()
        },
    }
    return DatasetInventory(
        dataset_id=dataset.dataset_id,
        files=tuple(file_inventories),
        observed_years=observed_years,
        column_profiles=profiles,
        findings=tuple(findings),
        snapshot_fingerprint=_hash(snapshot_payload),
    )
