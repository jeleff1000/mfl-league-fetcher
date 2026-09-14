from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.sota_recon.witness_gate.census import CensusDataset, ProducerPin
from scripts.sota_recon.witness_gate.inventory import inventory_dataset
from scripts.sota_recon.witness_gate.models import DatasetContract, SourceClass


def dataset(
    pattern: str,
    year_start: int = 2000,
    year_end: int = 2002,
    required_years: tuple[int, ...] = (),
) -> CensusDataset:
    return CensusDataset(
        contract=DatasetContract(
            contract_version="1",
            dataset_id="fixture.logs",
            source_class=SourceClass.CANONICAL,
            physical_globs=(pattern,),
            year_start=year_start,
            year_end=year_end,
            required_years=required_years,
        ),
        producer=ProducerPin(kind="local_lake", local_only=True),
    )


def write_parquet(path: Path, body: dict[str, list[object]], schema: pa.Schema | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pydict(body, schema=schema)
    pq.write_table(table, path)


def finding_codes(result) -> set[str]:
    return {item.code for item in result.findings}


def test_unmatched_glob_fails_source_health(tmp_path: Path) -> None:
    result = inventory_dataset(tmp_path, dataset("missing/*.parquet"), registered_fields={"year"})

    assert result.status == "fail"
    assert "DATASET_GLOB_UNMATCHED" in finding_codes(result)


def test_empty_shard_and_missing_year_fail_loudly(tmp_path: Path) -> None:
    write_parquet(tmp_path / "logs" / "empty.parquet", {"year": [], "attempts": []})
    write_parquet(tmp_path / "logs" / "data.parquet", {"year": [2000, 2002], "attempts": [0, 12]})

    result = inventory_dataset(
        tmp_path,
        dataset("logs/*.parquet"),
        registered_fields={"year", "attempts"},
    )

    assert {"EMPTY_SHARD", "MISSING_REQUIRED_YEAR"} <= finding_codes(result)
    assert result.observed_years == (2000, 2002)
    assert result.column_profiles["attempts"].null_count == 0


def test_schema_drift_and_unregistered_columns_fail_global_contract(tmp_path: Path) -> None:
    write_parquet(tmp_path / "logs" / "a.parquet", {"year": [2000], "attempts": [2]})
    write_parquet(
        tmp_path / "logs" / "b.parquet",
        {"year": [2001], "attempts": [3.0], "mystery": [9]},
    )

    result = inventory_dataset(
        tmp_path,
        dataset("logs/*.parquet", year_end=2001),
        registered_fields={"year", "attempts"},
    )

    assert "SCHEMA_DRIFT" in finding_codes(result)
    assert "UNREGISTERED_FIELD" in finding_codes(result)
    mystery = next(item for item in result.findings if item.code == "UNREGISTERED_FIELD")
    assert mystery.scope["field"] == "mystery"


def test_sharded_dataset_has_one_deterministic_snapshot(tmp_path: Path) -> None:
    write_parquet(tmp_path / "logs" / "a.parquet", {"year": [2000], "attempts": [0]})
    write_parquet(tmp_path / "logs" / "b.parquet", {"year": [2001], "attempts": [4]})
    contract = dataset("logs/*.parquet", year_end=2001)

    first = inventory_dataset(tmp_path, contract, registered_fields={"year", "attempts"})
    second = inventory_dataset(tmp_path, contract, registered_fields={"year", "attempts"})

    assert first.status == "pass"
    assert len(first.files) == 2
    assert first.snapshot_fingerprint == second.snapshot_fingerprint
    assert first.column_profiles["attempts"].zero_count == 1


def test_explicit_year_census_allows_a_versioned_source_gap(tmp_path: Path) -> None:
    write_parquet(tmp_path / "logs" / "data.parquet", {"year": [2000, 2002], "attempts": [2, 4]})

    result = inventory_dataset(
        tmp_path,
        dataset("logs/*.parquet", required_years=(2000, 2002)),
        registered_fields={"year", "attempts"},
    )

    assert "MISSING_REQUIRED_YEAR" not in finding_codes(result)


def test_real_external_witness_index_smoke() -> None:
    lake_root = Path("D:/league-history-data/nfl")
    witness_index = lake_root / "derived/external_witness_intake/EXTERNAL_WITNESS_INDEX.parquet"
    if not witness_index.exists():
        pytest.skip("local witness lake is not mounted")
    schema = pq.ParquetFile(witness_index).schema_arrow
    contract = dataset(
        "derived/external_witness_intake/EXTERNAL_WITNESS_INDEX.parquet",
        year_start=1919,
        year_end=2025,
    )

    result = inventory_dataset(lake_root, contract, registered_fields=set(schema.names))

    assert len(result.files) == 1
    assert result.files[0].row_count > 10_000
