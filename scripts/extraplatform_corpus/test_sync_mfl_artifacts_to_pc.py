from __future__ import annotations

import hashlib
import json
from pathlib import Path
import zipfile

from sync_mfl_artifacts_to_pc import (
    ArtifactLandingError,
    audit_landed_lane_bundles,
    download_artifact_with_gh_cli,
    land_artifact,
    resolve_github_token,
    resolve_required_files,
)


def _archive(path: Path, files: dict[str, bytes]) -> str:
    with zipfile.ZipFile(path, "w") as zipped:
        for name, content in files.items():
            zipped.writestr(name, content)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _artifact(archive_sha256: str) -> dict[str, object]:
    return {
        "id": 123,
        "name": "mfl-register-chunk-456",
        "archive_download_url": "https://example.invalid/artifact.zip",
        "digest": f"sha256:{archive_sha256}",
        "size_in_bytes": 1,
        "created_at": "2026-08-20T12:00:00Z",
        "workflow_run": {"id": 456},
    }


def test_landing_a_chunk_is_resumable_and_writes_a_provenance_receipt(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    digest = _archive(
        archive,
        {
            "mfl_register_chunk.duckdb": b"duckdb",
            "mfl_register_all_runs.json": b"{}",
            "cache_append_proof.json": b'{"schema_unchanged": true}',
            "mfl_research_overlay.duckdb": b"overlay",
            "mfl_research_overlay_proof.json": b"{}",
        },
    )
    destination = tmp_path / "D" / "pc_artifacts"
    calls: list[str] = []

    def download(url: str, target: Path) -> None:
        calls.append(url)
        target.write_bytes(archive.read_bytes())

    first = land_artifact(
        _artifact(digest),
        destination=destination,
        download=download,
        required_files={
            "mfl_register_chunk.duckdb",
            "mfl_register_all_runs.json",
            "cache_append_proof.json",
            "mfl_research_overlay.duckdb",
            "mfl_research_overlay_proof.json",
        },
    )

    landed = destination / "campaigns" / "456"
    receipt = json.loads((landed / ".landed.json").read_text(encoding="utf-8"))
    assert first["status"] == "landed"
    assert calls == ["https://example.invalid/artifact.zip"]
    assert receipt["artifact_id"] == 123
    assert receipt["archive_sha256"] == digest
    assert sorted(receipt["files"]) == [
        "cache_append_proof.json",
        "mfl_register_all_runs.json",
        "mfl_register_chunk.duckdb",
        "mfl_research_overlay.duckdb",
        "mfl_research_overlay_proof.json",
    ]

    second = land_artifact(
        _artifact(digest),
        destination=destination,
        download=download,
        required_files={"mfl_register_chunk.duckdb"},
    )
    assert second["status"] == "already_landed"
    assert len(calls) == 1


def test_landing_refuses_to_publish_an_incomplete_chunk(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    digest = _archive(archive, {"mfl_register_chunk.duckdb": b"duckdb"})
    destination = tmp_path / "D" / "pc_artifacts"

    try:
        land_artifact(
            _artifact(digest),
            destination=destination,
            download=lambda _url, target: target.write_bytes(archive.read_bytes()),
            required_files={"mfl_register_chunk.duckdb", "mfl_register_all_runs.json"},
        )
    except ArtifactLandingError as exc:
        assert "required files" in str(exc)
    else:
        raise AssertionError("incomplete artifact was published")

    assert not (destination / "campaigns" / "456").exists()


def test_existing_bom_legacy_receipt_is_preserved_but_marked_unverified(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    digest = _archive(archive, {"mfl_register_chunk.duckdb": b"duckdb"})
    destination = tmp_path / "D" / "pc_artifacts"
    landing = destination / "campaigns" / "456"
    landing.mkdir(parents=True)
    (landing / "mfl_register_chunk.duckdb").write_bytes(b"duckdb")
    (landing / ".landed.json").write_text(
        json.dumps({"run_id": 456, "source": "mfl-register-chunk-456"}),
        encoding="utf-8-sig",
    )

    result = land_artifact(
        _artifact(digest),
        destination=destination,
        download=lambda *_: (_ for _ in ()).throw(AssertionError("existing landing downloaded again")),
        required_files={"mfl_register_chunk.duckdb"},
    )

    assert result["status"] == "legacy_present_unverified"


def test_registry_artifact_can_supply_its_own_strict_file_contract() -> None:
    assert resolve_required_files(["mfl_original_39368_registry.json", "registry_proof.json"]) == {
        "mfl_original_39368_registry.json",
        "registry_proof.json",
    }


def test_resolve_github_token_uses_authenticated_gh_cli_when_environment_is_empty() -> None:
    """Protects unattended D: landing when no token was copied into the shell."""

    token = resolve_github_token(
        "GH_TOKEN",
        environment={},
        run_command=lambda command: "gho_from_existing_cli\n" if command == ["gh", "auth", "token"] else "",
    )

    assert token == "gho_from_existing_cli"


def test_gh_cli_archive_download_writes_the_requested_artifact_to_the_staging_path(tmp_path: Path) -> None:
    """Protects artifact landing from GitHub's signed-host redirect behavior."""

    archive = tmp_path / "artifact.zip"

    def run_command(command: list[str], output: Path) -> None:
        assert command == [
            "gh",
            "api",
            "repos/jeleff1000/mfl-league-fetcher/actions/artifacts/9269839016/zip",
        ]
        output.write_bytes(b"zip-bytes")

    download_artifact_with_gh_cli(
        repo="jeleff1000/mfl-league-fetcher",
        artifact_id=9269839016,
        target=archive,
        run_command=run_command,
    )

    assert archive.read_bytes() == b"zip-bytes"


def test_multi_artifact_layout_keeps_same_run_artifacts_in_distinct_verified_landings(tmp_path: Path) -> None:
    archive = tmp_path / "source.zip"
    digest = _archive(archive, {"lane-manifest.json": b"{}"})
    artifact = _artifact(digest)
    artifact["id"] = 789
    artifact["name"] = "mfl-original-source-archives-lane-0-456"
    destination = tmp_path / "D" / "pc_artifacts"

    result = land_artifact(
        artifact,
        destination=destination,
        download=lambda _url, target: target.write_bytes(archive.read_bytes()),
        required_files={"lane-manifest.json"},
        multi_artifact_layout=True,
    )

    assert result["path"] == str(destination / "campaigns" / "456" / "789")
    assert (destination / "campaigns" / "456" / "789" / "lane-manifest.json").is_file()


def test_lane_bundle_audit_requires_exact_lanes_and_verifies_every_inner_archive_digest(tmp_path: Path) -> None:
    destination = tmp_path / "D" / "pc_artifacts"
    campaign = destination / "campaigns" / "456"
    for lane, artifact_id in enumerate((11, 12)):
        landing = campaign / str(100 + lane)
        artifacts = landing / "artifacts"
        artifacts.mkdir(parents=True)
        archive = artifacts / f"{artifact_id}.zip"
        _archive(archive, {"batch_receipt.json": b"{}"})
        digest = hashlib.sha256(archive.read_bytes()).hexdigest()
        manifest = {
            "inventory_run_id": 9,
            "lane": lane,
            "lane_count": 2,
            "artifact_count": 1,
            "bytes": archive.stat().st_size,
            "artifacts": [
                {
                    "artifact_id": artifact_id,
                    "run_id": 1,
                    "name": "mfl-register-batch",
                    "size_in_bytes": archive.stat().st_size,
                    "digest": f"sha256:{digest}",
                }
            ],
        }
        manifest_path = landing / "lane-manifest.json"
        manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
        (landing / ".landed.json").write_text(
            json.dumps(
                {
                    "artifact_id": 100 + lane,
                    "archive_sha256": "a" * 64,
                    "files": {"lane-manifest.json": {"sha256": hashlib.sha256(manifest_path.read_bytes()).hexdigest()}},
                }
            ),
            encoding="utf-8",
        )

    report = audit_landed_lane_bundles(destination, run_id=456, expected_lane_count=2)

    assert report["lane_count"] == 2
    assert report["source_artifact_count"] == 2
    assert report["source_artifact_ids"] == [11, 12]
