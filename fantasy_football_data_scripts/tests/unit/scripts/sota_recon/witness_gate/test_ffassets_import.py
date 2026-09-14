from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.sota_recon.witness_gate import ffassets_import
from scripts.sota_recon.witness_gate.ffassets_import import import_harvest_run, main


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(
    root: Path,
    shard: int,
    *,
    run_id: str = "run-1",
    shard_count: int = 2,
    source: str = "nflcom",
    dataset: str = "team_season_roster",
    artifact_suffix: str = "",
) -> None:
    artifact = (
        root
        / f"witness-harvest-{source}-{dataset}-{shard}{artifact_suffix}"
        / f"shard-{shard}"
    )
    artifact.mkdir(parents=True)
    records = artifact / "records.parquet"
    pq.write_table(
        pa.table({"season": [2000 + shard], "player": [f"Player {shard}"]}),
        records,
    )
    ledger = artifact / "REQUEST_LEDGER.jsonl"
    ledger.write_text('{"status":"ok"}\n', encoding="utf-8")
    manifest = {
        "schema_version": 1,
        "source": source,
        "dataset": dataset,
        "shard_id": shard,
        "shard_count": shard_count,
        "artifact_run_id": run_id,
        "files": [
            {"path": "REQUEST_LEDGER.jsonl", "bytes": ledger.stat().st_size, "sha256": _sha256(ledger)},
            {"path": "records.parquet", "bytes": records.stat().st_size, "sha256": _sha256(records)},
        ],
    }
    (artifact / "ARTIFACT_MANIFEST.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_import_harvest_run_verifies_and_preserves_shards(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    _artifact(downloads, 0)
    _artifact(downloads, 1)

    result = import_harvest_run(
        downloads,
        tmp_path / "lake",
        expected_run_id="run-1",
        expected_shards=2,
    )

    assert result["datasets"][0]["row_count"] == 2
    assert result["datasets"][0]["observed_years"] == [2000, 2001]
    assert result["manifest_pin"].startswith("ffassets-run:run-1:sha256:")
    target = tmp_path / "lake/ff_assets/nflcom/team_season_roster/run-1"
    assert (target / "shards/shard-0/records.parquet").is_file()
    assert (target / "IMPORT_MANIFEST.json").is_file()


def test_import_harvest_run_rejects_missing_or_tampered_shards(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    _artifact(downloads, 0)

    with pytest.raises(ValueError, match="complete shard set"):
        import_harvest_run(downloads, tmp_path / "lake", expected_run_id="run-1", expected_shards=2)

    _artifact(downloads, 1)
    records = downloads / "witness-harvest-nflcom-team_season_roster-1/shard-1/records.parquet"
    records.write_bytes(b"tampered")
    with pytest.raises(ValueError, match="mismatch"):
        import_harvest_run(downloads, tmp_path / "lake2", expected_run_id="run-1", expected_shards=2)


def test_partial_import_records_missing_shards_and_blocks_absence_claims(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    for shard in range(19):
        _artifact(downloads, shard, shard_count=20)

    summary = import_harvest_run(
        downloads_root=downloads,
        lake_root=tmp_path / "lake",
        expected_run_id="run-1",
        expected_shards=20,
        expected_datasets=(("nflcom", "team_season_roster"),),
        coverage_mode="verified_partitions",
    )

    dataset = summary["datasets"][0]
    assert dataset["expected_shards"] == 20
    assert dataset["observed_shards"] == list(range(19))
    assert dataset["missing_shards"] == [19]
    assert dataset["coverage_complete"] is False
    assert dataset["positive_claims_admissible"] is True
    assert dataset["absence_claims_admissible"] is False


def test_complete_mode_still_rejects_a_missing_shard(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    _artifact(downloads, 0)

    with pytest.raises(ValueError, match="complete shard set"):
        import_harvest_run(
            downloads,
            tmp_path / "lake",
            expected_run_id="run-1",
            expected_shards=2,
        )


@pytest.mark.parametrize(
    ("configure", "message"),
    (
        (
            lambda root: (
                _artifact(root, 0),
                _artifact(root, 0, artifact_suffix="-duplicate"),
            ),
            "duplicate shard IDs",
        ),
        (
            lambda root: (
                _artifact(root, 0),
                _artifact(root, 1, shard_count=3),
            ),
            "shard count mismatch",
        ),
        (lambda root: _artifact(root, 2), "out of range"),
    ),
)
def test_verified_partitions_rejects_invalid_shard_topology(tmp_path: Path, configure, message: str) -> None:
    downloads = tmp_path / "downloads"
    configure(downloads)

    with pytest.raises(ValueError, match=message):
        import_harvest_run(
            downloads,
            tmp_path / "lake",
            expected_run_id="run-1",
            expected_shards=2,
            expected_datasets=(("nflcom", "team_season_roster"),),
            coverage_mode="verified_partitions",
        )


def test_verified_partitions_rejects_unexpected_datasets(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    _artifact(downloads, 0)

    with pytest.raises(ValueError, match="dataset set mismatch"):
        import_harvest_run(
            downloads,
            tmp_path / "lake",
            expected_run_id="run-1",
            expected_shards=2,
            expected_datasets=(("statscrew", "team_season_roster"),),
            coverage_mode="verified_partitions",
        )


def test_verified_partitions_requires_explicit_dataset_allowlist(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    _artifact(downloads, 0)

    with pytest.raises(ValueError, match="explicit expected_datasets allowlist"):
        import_harvest_run(
            downloads,
            tmp_path / "lake",
            expected_run_id="run-1",
            expected_shards=2,
            coverage_mode="verified_partitions",
        )


def test_resolved_manifest_payload_requires_existing_contained_file(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact"
    artifact.mkdir()
    payload = artifact / "records.parquet"
    payload.write_bytes(b"payload")
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"outside")

    assert ffassets_import._resolve_manifest_payload(
        artifact, Path("records.parquet")
    ) == payload.resolve(strict=True)
    with pytest.raises(ValueError, match="manifest file missing"):
        ffassets_import._resolve_manifest_payload(artifact, Path("missing.parquet"))
    with pytest.raises(ValueError, match="unsafe manifest path"):
        ffassets_import._resolve_manifest_payload(artifact, Path("../outside.parquet"))


def test_import_rejects_manifest_payload_symlink_escape(tmp_path: Path) -> None:
    downloads = tmp_path / "downloads"
    _artifact(downloads, 0, shard_count=1)
    artifact = downloads / "witness-harvest-nflcom-team_season_roster-0/shard-0"
    ledger = artifact / "REQUEST_LEDGER.jsonl"
    outside = tmp_path / "outside-ledger.jsonl"
    outside.write_text('{"status":"outside"}\n', encoding="utf-8")
    ledger.unlink()
    try:
        ledger.symlink_to(outside)
    except OSError as exc:
        pytest.skip(f"symlink creation is not permitted on this Windows host: {exc}")
    manifest_path = artifact / "ARTIFACT_MANIFEST.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"][0].update(
        {"bytes": outside.stat().st_size, "sha256": _sha256(outside)}
    )
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    with pytest.raises(ValueError, match="escapes artifact root"):
        import_harvest_run(
            downloads,
            tmp_path / "lake",
            expected_run_id="run-1",
            expected_shards=1,
        )


def test_cli_supports_partial_coverage_and_repeatable_dataset_filters(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    downloads = tmp_path / "downloads"
    _artifact(
        downloads,
        0,
        shard_count=2,
        source="profootballarchives",
        dataset="player_game_participation",
    )
    _artifact(
        downloads,
        0,
        shard_count=2,
        source="statscrew",
        dataset="team_season_roster",
    )
    lake = tmp_path / "lake"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ffassets_import",
            "--downloads-root",
            str(downloads),
            "--lake-root",
            str(lake),
            "--run-id",
            "run-1",
            "--expected-shards",
            "2",
            "--coverage-mode",
            "verified_partitions",
            "--dataset",
            "profootballarchives:player_game_participation",
            "--dataset",
            "statscrew:team_season_roster",
        ],
    )

    assert main() == 0
    assert (
        lake
        / "ff_assets/profootballarchives/player_game_participation/run-1/IMPORT_MANIFEST.json"
    ).is_file()
    assert (lake / "ff_assets/statscrew/team_season_roster/run-1/IMPORT_MANIFEST.json").is_file()
