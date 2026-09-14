from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.sota_recon.external_witness_intake import ingest_artifacts


def _artifact(root: Path) -> Path:
    root.mkdir()
    pd.DataFrame(
        [{"source": "nflcom", "dataset": "team_season_roster", "season": 1921, "team": "dayton", "player": "Herb Sies"}]
    ).to_parquet(root / "records.parquet", index=False)
    payload = root / "records.parquet"
    manifest = {
        "schema_version": 1,
        "source": "nflcom",
        "dataset": "team_season_roster",
        "shard_id": 0,
        "shard_count": 1,
        "artifact_run_id": "fixture",
        "created_at_utc": "2026-07-18T00:00:00Z",
        "files": [{"path": "records.parquet", "bytes": payload.stat().st_size, "sha256": hashlib.sha256(payload.read_bytes()).hexdigest()}],
    }
    (root / "ARTIFACT_MANIFEST.json").write_text(json.dumps(manifest), encoding="utf-8")
    return root


def test_intake_verifies_and_copies_artifact_immutably(tmp_path: Path) -> None:
    source = _artifact(tmp_path / "artifact")
    result = ingest_artifacts([source], tmp_path / "intake")
    assert result["artifact_count"] == 1
    assert result["record_count"] == 1
    destination = Path(result["artifacts"][0]["destination"])
    assert destination.joinpath("records.parquet").is_file()
    assert (tmp_path / "intake" / "EXTERNAL_WITNESS_INDEX.parquet").is_file()
    with pytest.raises(FileExistsError):
        ingest_artifacts([source], tmp_path / "intake")


def test_intake_rejects_tampered_payload(tmp_path: Path) -> None:
    source = _artifact(tmp_path / "artifact")
    (source / "records.parquet").write_bytes(b"tampered")
    with pytest.raises(ValueError, match="checksum"):
        ingest_artifacts([source], tmp_path / "intake")
