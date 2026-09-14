from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.sota_recon.witness_gate import pfa_participation
from scripts.sota_recon.witness_gate.pfa_participation import (
    _deduplicate,
    main,
    materialize_pfa_participation,
)


SOURCE = "profootballarchives"
DATASET = "player_game_participation"
RUN_ID = "run-1"
MANIFEST_PIN = f"ffassets-run:{RUN_ID}:sha256:{'a' * 64}"


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _imported_dataset(
    tmp_path: Path,
    shards: dict[int, list[dict[str, object]]],
    *,
    reverse_manifest_order: bool = False,
) -> Path:
    root = tmp_path / "imported"
    shard_manifests = []
    for shard_id, rows in sorted(shards.items()):
        artifact = root / "shards" / f"shard-{shard_id}"
        artifact.mkdir(parents=True)
        records = artifact / "records.parquet"
        materialized = [{**row, "shard_id": shard_id} for row in rows]
        pq.write_table(pa.Table.from_pylist(materialized), records)
        manifest = {
            "schema_version": 1,
            "source": SOURCE,
            "dataset": DATASET,
            "shard_id": shard_id,
            "shard_count": len(shards),
            "artifact_run_id": RUN_ID,
            "files": [
                {
                    "path": "records.parquet",
                    "bytes": records.stat().st_size,
                    "sha256": _sha256(records),
                }
            ],
        }
        manifest_path = artifact / "ARTIFACT_MANIFEST.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        shard_manifests.append(
            {
                "shard_id": shard_id,
                "sha256": _sha256(manifest_path),
                "source_path": str(manifest_path),
            }
        )
    if reverse_manifest_order:
        shard_manifests.reverse()
    import_manifest = root / "IMPORT_MANIFEST.json"
    import_manifest.write_text(
        json.dumps(
            {
                "source": SOURCE,
                "dataset": DATASET,
                "run_id": RUN_ID,
                "manifest_pin": MANIFEST_PIN,
                "shard_manifests": shard_manifests,
            }
        ),
        encoding="utf-8",
    )
    return import_manifest


def _row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        "season": 1988,
        "game_id": "1988-09-04-sf-no",
        "team": "San Francisco 49ers",
        "lineup_side": "Offense",
        "player": "Jerry Rice",
        "source_player_id": "jerry-rice",
        "source_position_raw": "E",
    }
    row.update(overrides)
    return row


def _read(path: Path):
    return pq.read_table(path).to_pandas()


def _read_rows(path: Path) -> list[dict[str, object]]:
    return pq.read_table(path).to_pylist()


def test_materialization_repairs_legacy_side_in_team_and_deduplicates(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(
        tmp_path,
        {
            0: [
                _row(team="Offense", lineup_side=None),
                _row(),
            ]
        },
    )

    result = materialize_pfa_participation(import_manifest, tmp_path / "output")
    rows = _read(result.data_path)

    assert result.input_rows == 2
    assert result.output_rows == 1
    assert rows.iloc[0].team == "San Francisco 49ers"
    assert rows.iloc[0].lineup_side == "Offense"
    assert rows.iloc[0].position == "WR"
    assert rows.iloc[0].nfl_position == "E"
    assert bool(rows.iloc[0].participated) is True


def test_materialization_reads_records_in_bounded_batches(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import_manifest = _imported_dataset(
        tmp_path,
        {0: [_row(game_id=f"game-{index}") for index in range(5)]},
    )
    observed_batch_sizes: list[int] = []
    observed_normalize_sizes: list[int] = []
    original_iter_batches = pq.ParquetFile.iter_batches
    original_normalize_rows = pfa_participation._normalize_rows

    def bounded_batches(self, *, batch_size, **kwargs):
        for batch in original_iter_batches(self, batch_size=batch_size, **kwargs):
            observed_batch_sizes.append(batch.num_rows)
            yield batch

    def reject_eager_read(*args, **kwargs):
        raise AssertionError("materialization must not eagerly read an entire records parquet")

    def normalize_bounded(rows, taxonomy_version):
        observed_normalize_sizes.append(len(rows))
        return original_normalize_rows(rows, taxonomy_version)

    monkeypatch.setattr(pfa_participation, "_BATCH_SIZE", 2, raising=False)
    monkeypatch.setattr(pq.ParquetFile, "iter_batches", bounded_batches)
    monkeypatch.setattr(pfa_participation.pq, "read_table", reject_eager_read)
    monkeypatch.setattr(pfa_participation, "_normalize_rows", normalize_bounded)

    result = materialize_pfa_participation(import_manifest, tmp_path / "output")

    assert result.input_rows == 5
    assert result.output_rows == 5
    assert observed_batch_sizes
    assert max(observed_batch_sizes) <= 2
    assert observed_normalize_sizes
    assert max(observed_normalize_sizes) <= 2


def test_legacy_repair_omits_team_when_club_is_not_unique(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(
        tmp_path,
        {
            0: [
                _row(team="Offense", lineup_side=None),
                _row(team="San Francisco 49ers", lineup_side="Offense"),
                _row(team="New Orleans Saints", lineup_side="Offense"),
            ]
        },
    )

    rows = _read(materialize_pfa_participation(import_manifest, tmp_path / "output").data_path)

    legacy = rows[rows["team"].isna()].iloc[0]
    assert legacy.lineup_side == "Offense"


def test_every_row_has_leaf_level_shard_lineage(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(
        tmp_path,
        {0: [_row()], 1: [_row(game_id="1988-09-11-sf-nyg")]},
    )
    imported = json.loads(import_manifest.read_text(encoding="utf-8"))
    expected_hashes = {
        item["shard_id"]: item["sha256"] for item in imported["shard_manifests"]
    }

    rows = _read_rows(
        materialize_pfa_participation(import_manifest, tmp_path / "output").data_path
    )

    assert {row["source"] for row in rows} == {SOURCE}
    assert {row["dataset"] for row in rows} == {DATASET}
    assert {row["artifact_run_id"] for row in rows} == {RUN_ID}
    assert {row["manifest_pin"] for row in rows} == {MANIFEST_PIN}
    assert {row["taxonomy_version"] for row in rows} == {"position-taxonomy.v1"}
    assert {
        row["game_id"]: row["lineage_leaves"] for row in rows
    } == {
        "1988-09-04-sf-no": [
            {"shard_id": 0, "manifest_sha256": expected_hashes[0]}
        ],
        "1988-09-11-sf-nyg": [
            {"shard_id": 1, "manifest_sha256": expected_hashes[1]}
        ],
    }


def test_cross_shard_semantic_duplicate_retains_all_verified_leaves(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(tmp_path, {0: [_row()], 1: [_row()]})
    imported = json.loads(import_manifest.read_text(encoding="utf-8"))
    expected = [
        {"shard_id": item["shard_id"], "manifest_sha256": item["sha256"]}
        for item in imported["shard_manifests"]
    ]

    result = materialize_pfa_participation(import_manifest, tmp_path / "output")
    rows = _read_rows(result.data_path)

    assert result.input_rows == 2
    assert result.output_rows == 1
    assert rows[0]["lineage_leaves"] == expected
    assert "shard_id" not in rows[0]
    assert "manifest_sha256" not in rows[0]


def test_cross_shard_lineage_is_deterministic_under_reversed_manifest_order(
    tmp_path: Path,
) -> None:
    forward_manifest = _imported_dataset(
        tmp_path / "forward", {0: [_row()], 1: [_row()]}
    )
    reverse_manifest = _imported_dataset(
        tmp_path / "reverse",
        {0: [_row()], 1: [_row()]},
        reverse_manifest_order=True,
    )

    forward = _read_rows(
        materialize_pfa_participation(forward_manifest, tmp_path / "forward-output").data_path
    )
    reverse = _read_rows(
        materialize_pfa_participation(reverse_manifest, tmp_path / "reverse-output").data_path
    )

    assert forward == reverse
    assert [leaf["shard_id"] for leaf in forward[0]["lineage_leaves"]] == [0, 1]


def test_same_verified_leaf_with_conflicting_semantics_fails_loudly(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(
        tmp_path,
        {0: [_row(player="Jerry Rice"), _row(player="Conflicting Player")]},
    )

    with pytest.raises(ValueError, match=r"conflicting duplicate.*player.*shard_id=0"):
        materialize_pfa_participation(import_manifest, tmp_path / "output")


def test_same_shard_id_cannot_claim_two_manifest_hashes() -> None:
    semantic = {
        "season": 1988,
        "game_id": "1988-09-04-sf-no",
        "team": "San Francisco 49ers",
        "lineup_side": "Offense",
        "player": "Jerry Rice",
        "source_player_id": "jerry-rice",
        "source_position_raw": "E",
        "position": "WR",
        "nfl_position": "E",
        "participated": True,
        "source": SOURCE,
        "dataset": DATASET,
        "artifact_run_id": RUN_ID,
        "manifest_pin": MANIFEST_PIN,
        "taxonomy_version": "position-taxonomy.v1",
    }
    rows = [
        {
            **semantic,
            "lineage_leaves": [{"shard_id": 0, "manifest_sha256": "a" * 64}],
        },
        {
            **semantic,
            "lineage_leaves": [{"shard_id": 0, "manifest_sha256": "b" * 64}],
        },
    ]

    with pytest.raises(ValueError, match="conflicting provenance definition.*shard_id=0"):
        _deduplicate(rows)


def test_unknown_pfa_position_aborts_with_row_lineage(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(
        tmp_path,
        {0: [_row(source_position_raw="MYSTERY-RUSHER")]},
    )
    output = tmp_path / "output"

    with pytest.raises(
        ValueError,
        match=r"MYSTERY-RUSHER.*profootballarchives.*shard_id=0.*row_index=0",
    ):
        materialize_pfa_participation(import_manifest, output)

    assert not output.exists()


def test_conflicting_nonnull_duplicates_fail_instead_of_using_row_order(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(
        tmp_path,
        {0: [_row(player="Jerry Rice"), _row(player="Not Jerry Rice")]},
    )

    with pytest.raises(ValueError, match="conflicting duplicate.*player"):
        materialize_pfa_participation(import_manifest, tmp_path / "output")


def test_materialization_reverifies_declared_shard_manifest(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(tmp_path, {0: [_row()]})
    imported = json.loads(import_manifest.read_text(encoding="utf-8"))
    manifest_path = Path(imported["shard_manifests"][0]["source_path"])
    manifest_path.write_text(manifest_path.read_text(encoding="utf-8") + " ", encoding="utf-8")

    with pytest.raises(ValueError, match="shard manifest checksum mismatch"):
        materialize_pfa_participation(import_manifest, tmp_path / "output")


def test_existing_output_target_is_immutable(tmp_path: Path) -> None:
    import_manifest = _imported_dataset(tmp_path, {0: [_row()]})
    output = tmp_path / "output"
    output.mkdir()

    with pytest.raises(FileExistsError, match="immutable materialization target"):
        materialize_pfa_participation(import_manifest, output)


def test_cli_accepts_import_manifest_and_output_root(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import_manifest = _imported_dataset(tmp_path, {0: [_row()]})
    output = tmp_path / "output"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "pfa_participation",
            "--import-manifest",
            str(import_manifest),
            "--output-root",
            str(output),
        ],
    )

    assert main() == 0
    assert (output / "PFA_PARTICIPATION.parquet").is_file()
    assert (output / "MATERIALIZATION_MANIFEST.json").is_file()


def test_contract_registers_only_supported_pfa_claims() -> None:
    contracts = (
        Path(__file__).parents[6]
        / "scripts/sota_recon/witness_gate/contracts"
    )
    mappings = json.loads((contracts / "field_mappings.v1.json").read_text(encoding="utf-8"))
    dataset = mappings["datasets"]["derived.profootballarchives.player_game_participation"]
    fields = set(mappings["field_sets"][dataset["field_set"]])
    mapped = fields - set(dataset["excluded_fields"])

    assert mapped == {
        "game_id",
        "lineup_side",
        "nfl_position",
        "participated",
        "player",
        "position",
        "season",
        "source_player_id",
        "source_position_raw",
        "team",
    }
    assert not ({"starts", "snaps", "air_yards"} & fields)
    assert "lineage_leaves" in fields
    assert dataset["excluded_fields"]["lineage_leaves"] == "lineage_metadata"
    assert "shard_id" not in fields
    assert "manifest_sha256" not in fields

    census = json.loads((contracts / "source_census.v1.json").read_text(encoding="utf-8"))
    registered = {item["dataset_id"] for item in census["datasets"]}
    assert "derived.profootballarchives.player_game_participation" in registered
