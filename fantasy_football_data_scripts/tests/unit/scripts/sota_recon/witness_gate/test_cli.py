from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.sota_recon.witness_gate.cli import main


def write_fixture_contracts(tmp_path: Path, *, reviewed_fields: list[str]) -> tuple[Path, Path, Path]:
    lake = tmp_path / "lake"
    lake.mkdir()
    pq.write_table(
        pa.Table.from_pydict({"year": [2000], "attempts": [4]}),
        lake / "logs.parquet",
    )
    census = tmp_path / "census.json"
    census.write_text(
        json.dumps(
            {
                "contract_version": "1",
                "source_universe_version": "fixture-v1",
                "datasets": [
                    {
                        "contract_version": "1",
                        "dataset_id": "fixture.logs",
                        "source_class": "canonical",
                        "physical_globs": ["logs.parquet"],
                        "year_start": 2000,
                        "year_end": 2000,
                        "producer": {"kind": "local_lake", "local_only": True},
                    }
                ],
                "discoveries": [],
            }
        ),
        encoding="utf-8",
    )
    registry = tmp_path / "fields.json"
    registry.write_text(
        json.dumps(
            {
                "contract_version": "1",
                "field_sets": {"fixture": sorted(reviewed_fields)},
                "datasets": {
                    "fixture.logs": {
                        "field_set": "fixture",
                        "discriminator_fields": ["year"],
                        "atom_template": "raw.fixture.{field}",
                        "excluded_fields": {},
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    return lake, census, registry


def test_run_all_writes_four_independent_manifests_and_rollup(tmp_path: Path) -> None:
    lake, census, registry = write_fixture_contracts(tmp_path, reviewed_fields=["year", "attempts"])
    output = tmp_path / "out"

    exit_code = main(
        [
            "run-all",
            "--lake-root",
            str(lake),
            "--source-census",
            str(census),
            "--field-registry",
            str(registry),
            "--output-dir",
            str(output),
        ]
    )

    assert exit_code == 0
    for name in ("global_research_health", "source_health", "candidate_promotion", "release"):
        assert (output / f"{name}.json").exists()
    rollup = json.loads((output / "witness_gate_rollup.json").read_text(encoding="utf-8"))
    assert rollup["release_status"] == "pass"


def test_run_all_exits_nonzero_on_unregistered_field(tmp_path: Path) -> None:
    lake, census, registry = write_fixture_contracts(tmp_path, reviewed_fields=["year"])

    exit_code = main(
        [
            "run-all",
            "--lake-root",
            str(lake),
            "--source-census",
            str(census),
            "--field-registry",
            str(registry),
            "--output-dir",
            str(tmp_path / "out"),
        ]
    )

    assert exit_code == 1


def test_legacy_recon_runner_registers_typed_witness_gate_lane() -> None:
    from scripts.sota_recon.run_all import LANES

    assert "witness_gate" in LANES


def test_validate_positions_command_writes_separate_gate_manifests(tmp_path: Path) -> None:
    position_frame = tmp_path / "positions.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [
                {
                    "source": "fixture",
                    "row_id": "p1",
                    "position": "CB",
                    "nfl_position": "CB",
                }
            ]
        ),
        position_frame,
    )
    output = tmp_path / "out"

    exit_code = main(
        [
            "validate-positions",
            "--position-frame",
            str(position_frame),
            "--output-dir",
            str(output),
        ]
    )

    assert exit_code == 1
    candidate = json.loads((output / "candidate_promotion.json").read_text(encoding="utf-8"))
    source = json.loads((output / "source_health.json").read_text(encoding="utf-8"))
    assert candidate["status"] == "fail"
    assert candidate["findings"][0]["code"] == "DETAILED_POSITION_IN_BROAD_COLUMN"
    assert source["status"] == "pass"


def test_selected_gate_command_exit_is_not_poisoned_by_unrelated_plane(tmp_path: Path) -> None:
    lake, census, registry = write_fixture_contracts(tmp_path, reviewed_fields=["year", "attempts"])
    missing_rollup = tmp_path / "missing-pbp.parquet"

    exit_code = main(
        [
            "promote",
            "--lake-root",
            str(lake),
            "--source-census",
            str(census),
            "--field-registry",
            str(registry),
            "--output-dir",
            str(tmp_path / "out"),
            "--pbp-rollup",
            str(missing_rollup),
            "--pbp-contracts",
            str(tmp_path / "unused-contract.json"),
        ]
    )

    assert exit_code == 0


def test_release_command_reads_unresolved_collision_ledger(tmp_path: Path) -> None:
    lake, census, registry = write_fixture_contracts(tmp_path, reviewed_fields=["year", "attempts"])
    collision_ledger = tmp_path / "collision-decisions.parquet"
    pq.write_table(
        pa.Table.from_pylist(
            [{"collision_id": "collision-1", "status": "collision_review"}]
        ),
        collision_ledger,
    )
    output = tmp_path / "out"

    exit_code = main(
        [
            "release",
            "--lake-root",
            str(lake),
            "--source-census",
            str(census),
            "--field-registry",
            str(registry),
            "--output-dir",
            str(output),
            "--collision-ledger",
            str(collision_ledger),
        ]
    )

    assert exit_code == 1
    release = json.loads((output / "release.json").read_text(encoding="utf-8"))
    assert any(item["code"] == "UNRESOLVED_ROSTER_COLLISION" for item in release["findings"])


def test_collision_ledger_rejects_null_identity(tmp_path: Path) -> None:
    from scripts.sota_recon.witness_gate.cli import _unresolved_collisions

    collision_ledger = tmp_path / "collision-decisions.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"collision_id": None, "status": "collision_review"}]),
        collision_ledger,
    )

    with pytest.raises(ValueError, match="blank collision_id"):
        _unresolved_collisions(collision_ledger)


def test_validate_positions_reads_unresolved_collision_ledger(tmp_path: Path) -> None:
    position_frame = tmp_path / "positions.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"position": "QB", "nfl_position": "QB"}]),
        position_frame,
    )
    collision_ledger = tmp_path / "collision-decisions.parquet"
    pq.write_table(
        pa.Table.from_pylist([{"collision_id": "collision-1", "status": "collision_review"}]),
        collision_ledger,
    )
    output = tmp_path / "out"

    exit_code = main(
        [
            "validate-positions",
            "--position-frame",
            str(position_frame),
            "--collision-ledger",
            str(collision_ledger),
            "--output-dir",
            str(output),
        ]
    )

    assert exit_code == 1
    release = json.loads((output / "release.json").read_text(encoding="utf-8"))
    assert any(item["code"] == "UNRESOLVED_ROSTER_COLLISION" for item in release["findings"])
