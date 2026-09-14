"""Land immutable GitHub Actions MFL artifacts on D: without touching caches.

This is intentionally a receiver, not a merger or cache writer.  It downloads
one already-complete Actions artifact at a time, verifies GitHub's archive
digest, checks its required files, then atomically publishes it below the
user's local artifact root with a provenance receipt.  A matching receipt
makes reruns no-ops; an incomplete or conflicting landing fails closed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Callable, Iterable, Mapping
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import zipfile


DEFAULT_ARTIFACT_PREFIX = "mfl-register-chunk-"
DEFAULT_REQUIRED_FILES = frozenset(
    {
        "mfl_register_chunk.duckdb",
        "mfl_register_all_runs.json",
        "cache_append_proof.json",
        "mfl_research_overlay.duckdb",
        "mfl_research_overlay_proof.json",
    }
)


class ArtifactLandingError(RuntimeError):
    """An artifact cannot be safely admitted to the local evidence store."""


def resolve_required_files(custom_files: Iterable[str]) -> set[str] | frozenset[str]:
    """Return a strict archive contract for chunks or another named artifact."""

    requested = {str(name).replace("\\", "/") for name in custom_files if str(name).strip()}
    if not requested:
        return DEFAULT_REQUIRED_FILES
    for name in requested:
        path = PurePosixPath(name)
        if path.is_absolute() or ".." in path.parts or name != path.as_posix():
            raise ArtifactLandingError(f"required artifact file is unsafe: {name!r}")
    return requested


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(8 * 1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _expected_digest(artifact: dict[str, object]) -> str:
    digest = str(artifact.get("digest", "")).removeprefix("sha256:").lower()
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ArtifactLandingError(f"artifact has no valid SHA-256 digest: {artifact.get('id')!r}")
    return digest


def _artifact_id(artifact: dict[str, object]) -> int:
    try:
        return int(artifact["id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ArtifactLandingError("artifact has no numeric id") from exc


def _run_id(artifact: dict[str, object]) -> int:
    workflow_run = artifact.get("workflow_run")
    if not isinstance(workflow_run, dict):
        raise ArtifactLandingError(f"artifact {_artifact_id(artifact)} has no workflow run provenance")
    try:
        return int(workflow_run["id"])
    except (KeyError, TypeError, ValueError) as exc:
        raise ArtifactLandingError(f"artifact {_artifact_id(artifact)} has no numeric workflow run id") from exc


def _safe_archive_members(archive: zipfile.ZipFile) -> list[zipfile.ZipInfo]:
    members: list[zipfile.ZipInfo] = []
    names: set[str] = set()
    for member in archive.infolist():
        name = member.filename.replace("\\", "/")
        path = PurePosixPath(name)
        if not name or path.is_absolute() or ".." in path.parts:
            raise ArtifactLandingError(f"archive contains unsafe path: {member.filename!r}")
        if name in names:
            raise ArtifactLandingError(f"archive contains duplicate path: {name!r}")
        names.add(name)
        if not member.is_dir():
            members.append(member)
    return members


def _extract_archive(archive_path: Path, payload_dir: Path, required_files: set[str]) -> dict[str, dict[str, object]]:
    with zipfile.ZipFile(archive_path) as archive:
        members = _safe_archive_members(archive)
        names = {member.filename.replace("\\", "/") for member in members}
        missing = sorted(required_files - names)
        if missing:
            raise ArtifactLandingError(f"artifact is missing required files: {missing}")
        for member in members:
            target = payload_dir / PurePosixPath(member.filename.replace("\\", "/"))
            target.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, target.open("xb") as output:
                shutil.copyfileobj(source, output, length=8 * 1024 * 1024)

    files: dict[str, dict[str, object]] = {}
    for path in sorted(payload_dir.rglob("*")):
        if path.is_file():
            files[path.relative_to(payload_dir).as_posix()] = {
                "bytes": path.stat().st_size,
                "sha256": sha256(path),
            }
    return files


def land_artifact(
    artifact: dict[str, object],
    *,
    destination: Path,
    download: Callable[[str, Path], None],
    required_files: set[str] | frozenset[str],
    multi_artifact_layout: bool = False,
) -> dict[str, object]:
    """Download, verify, and atomically land exactly one artifact.

    ``download`` writes the archive to the supplied D:-rooted staging path.
    It is injected so the integrity behavior can be exercised without network
    credentials.  This function never alters an existing finalized landing.
    """

    destination = Path(destination)
    artifact_id = _artifact_id(artifact)
    run_id = _run_id(artifact)
    expected_digest = _expected_digest(artifact)
    archive_url = str(artifact.get("archive_download_url", ""))
    if not archive_url:
        raise ArtifactLandingError(f"artifact {artifact_id} has no archive download URL")

    final_dir = destination / "campaigns" / str(run_id)
    if multi_artifact_layout:
        final_dir = final_dir / str(artifact_id)
    receipt_path = final_dir / ".landed.json"
    if final_dir.exists():
        if not receipt_path.is_file():
            raise ArtifactLandingError(f"final landing already exists without receipt: {final_dir}")
        # Earlier receiver runs wrote Windows BOM-prefixed JSON.  Accept that
        # immutable receipt format without rewriting the existing landing.
        receipt = json.loads(receipt_path.read_text(encoding="utf-8-sig"))
        if receipt.get("artifact_id") == artifact_id and receipt.get("archive_sha256") == expected_digest:
            return {"status": "already_landed", "artifact_id": artifact_id, "run_id": run_id, "path": str(final_dir)}
        # Pre-receiver folders carry only run/name provenance.  They are
        # preserved and explicitly classified as unverified rather than
        # overwritten or misreported as digest-verified.
        if receipt.get("run_id") == run_id and receipt.get("source") == artifact.get("name"):
            missing = sorted(name for name in required_files if not (final_dir / name).is_file())
            if missing:
                raise ArtifactLandingError(f"legacy landing lacks required files: {missing}")
            return {
                "status": "legacy_present_unverified",
                "artifact_id": artifact_id,
                "run_id": run_id,
                "path": str(final_dir),
            }
        else:
            raise ArtifactLandingError(f"final landing conflicts with requested artifact: {final_dir}")

    staging_root = destination / "_staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    stage = Path(tempfile.mkdtemp(prefix=f"artifact-{artifact_id}-", dir=staging_root))
    archive_path = stage / "artifact.zip"
    payload_dir = stage / "payload"
    try:
        download(archive_url, archive_path)
        actual_digest = sha256(archive_path)
        if actual_digest != expected_digest:
            raise ArtifactLandingError(
                f"artifact {artifact_id} archive SHA-256 mismatch: expected={expected_digest} actual={actual_digest}"
            )
        payload_dir.mkdir()
        files = _extract_archive(archive_path, payload_dir, set(required_files))
        receipt = {
            "artifact_id": artifact_id,
            "artifact_name": str(artifact.get("name", "")),
            "workflow_run_id": run_id,
            "created_at": artifact.get("created_at"),
            "archive_sha256": actual_digest,
            "archive_bytes": archive_path.stat().st_size,
            "files": files,
        }
        (payload_dir / ".landed.json").write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
        final_dir.parent.mkdir(parents=True, exist_ok=True)
        if final_dir.exists():
            raise ArtifactLandingError(f"final landing appeared during staging: {final_dir}")
        payload_dir.replace(final_dir)
        return {"status": "landed", "artifact_id": artifact_id, "run_id": run_id, "path": str(final_dir)}
    finally:
        if stage.exists():
            # The staging directory is newly created by this invocation only.
            # It contains no source cache or prior landing.
            shutil.rmtree(stage, ignore_errors=True)


def _http_json(url: str, token: str | None) -> dict[str, object]:
    headers = {"Accept": "application/vnd.github+json", "X-GitHub-Api-Version": "2022-11-28"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers), timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def _download_http(url: str, target: Path, token: str | None) -> None:
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    with urlopen(Request(url, headers=headers), timeout=600) as response, target.open("xb") as output:
        shutil.copyfileobj(response, output, length=8 * 1024 * 1024)


def download_artifact_with_gh_cli(
    *,
    repo: str,
    artifact_id: int,
    target: Path,
    run_command: Callable[[list[str], Path], None] | None = None,
) -> None:
    """Download an Actions archive through ``gh`` so signed redirects stay valid."""

    command = ["gh", "api", f"repos/{repo}/actions/artifacts/{artifact_id}/zip"]
    if run_command is None:
        def run_command(command: list[str], output: Path) -> None:
            with output.open("xb") as handle:
                subprocess.run(command, check=True, stdout=handle, stderr=subprocess.DEVNULL)
    try:
        run_command(command, target)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ArtifactLandingError(f"GitHub CLI could not download artifact {artifact_id}") from exc


def artifacts_for_run(repo: str, run_id: int, token: str | None) -> list[dict[str, object]]:
    payload = _http_json(f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/artifacts?per_page=100", token)
    artifacts = payload.get("artifacts")
    if not isinstance(artifacts, list):
        raise ArtifactLandingError(f"GitHub returned no artifact list for workflow run {run_id}")
    return [artifact for artifact in artifacts if isinstance(artifact, dict)]


def resolve_github_token(
    token_env: str,
    *,
    environment: Mapping[str, str] | None = None,
    run_command: Callable[[list[str]], str] | None = None,
) -> str:
    """Use an explicit token first, then the authenticated local GitHub CLI."""

    environment = os.environ if environment is None else environment
    token = environment.get(token_env) or environment.get("GITHUB_TOKEN")
    if token:
        return token
    if run_command is None:
        def run_command(command: list[str]) -> str:
            return subprocess.check_output(command, text=True, stderr=subprocess.DEVNULL)
    try:
        token = run_command(["gh", "auth", "token"]).strip()
    except (OSError, subprocess.CalledProcessError) as exc:
        raise ArtifactLandingError(
            f"set {token_env} or authenticate the GitHub CLI with 'gh auth login' before downloading artifacts"
        ) from exc
    if not token:
        raise ArtifactLandingError(
            f"set {token_env} or authenticate the GitHub CLI with 'gh auth login' before downloading artifacts"
        )
    return token


def require_d_destination(destination: Path) -> None:
    if Path(destination).drive.upper() != "D:":
        raise ArtifactLandingError("destination must be on D:; no MFL artifact may land on C:")


def _load_object(path: Path, *, label: str) -> dict[str, object]:
    try:
        value = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArtifactLandingError(f"cannot read {label}: {path}") from exc
    if not isinstance(value, dict):
        raise ArtifactLandingError(f"{label} is not a JSON object: {path}")
    return value


def audit_landed_lane_bundles(
    destination: Path,
    *,
    run_id: int,
    expected_lane_count: int,
) -> dict[str, object]:
    """Verify all landed lane bundles before any source payload is inspected.

    This is deliberately read-only.  It confirms that every outer Actions
    archive has its receipt, each lane has the expected manifest, and every
    enclosed original artifact archive still matches the immutable digest
    recorded by Actions.  It does not merge, extract, or alter source rows.
    """

    if expected_lane_count <= 0:
        raise ArtifactLandingError("expected_lane_count must be positive")
    campaign = Path(destination) / "campaigns" / str(run_id)
    if not campaign.is_dir():
        raise ArtifactLandingError(f"landed lane campaign does not exist: {campaign}")

    landings = sorted(path for path in campaign.iterdir() if path.is_dir())
    if len(landings) != expected_lane_count:
        raise ArtifactLandingError(
            f"expected {expected_lane_count} landed lane bundles, found {len(landings)} under {campaign}"
        )

    lanes: set[int] = set()
    inventory_run_ids: set[int] = set()
    source_artifact_ids: set[int] = set()
    source_bytes = 0
    for landing in landings:
        try:
            outer_artifact_id = int(landing.name)
        except ValueError as exc:
            raise ArtifactLandingError(f"lane landing has non-numeric artifact directory: {landing}") from exc
        receipt = _load_object(landing / ".landed.json", label="outer landing receipt")
        if int(receipt.get("artifact_id", -1)) != outer_artifact_id:
            raise ArtifactLandingError(f"outer landing receipt does not match directory: {landing}")
        if len(str(receipt.get("archive_sha256", ""))) != 64:
            raise ArtifactLandingError(f"outer landing receipt lacks archive digest: {landing}")
        files = receipt.get("files")
        if not isinstance(files, dict) or not isinstance(files.get("lane-manifest.json"), dict):
            raise ArtifactLandingError(f"outer landing receipt lacks lane-manifest provenance: {landing}")
        recorded_manifest_digest = str(files["lane-manifest.json"].get("sha256", ""))
        manifest_path = landing / "lane-manifest.json"
        if sha256(manifest_path) != recorded_manifest_digest:
            raise ArtifactLandingError(f"lane manifest changed after outer archive landing: {landing}")
        manifest = _load_object(manifest_path, label="lane manifest")
        try:
            lane = int(manifest["lane"])
            lane_count = int(manifest["lane_count"])
            inventory_run_ids.add(int(manifest["inventory_run_id"]))
            reported_artifact_count = int(manifest["artifact_count"])
            reported_bytes = int(manifest["bytes"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ArtifactLandingError(f"lane manifest has malformed numeric provenance: {landing}") from exc
        if lane_count != expected_lane_count or lane < 0 or lane >= expected_lane_count or lane in lanes:
            raise ArtifactLandingError(f"lane manifest has invalid or duplicate lane: {landing}")
        lanes.add(lane)
        manifest_artifacts = manifest.get("artifacts")
        if not isinstance(manifest_artifacts, list) or len(manifest_artifacts) != reported_artifact_count:
            raise ArtifactLandingError(f"lane manifest has an incomplete artifact list: {landing}")

        artifact_dir = landing / "artifacts"
        expected_files: set[str] = set()
        actual_bytes = 0
        for artifact in manifest_artifacts:
            if not isinstance(artifact, dict):
                raise ArtifactLandingError(f"lane manifest has malformed source artifact: {landing}")
            try:
                artifact_id = int(artifact["artifact_id"])
            except (KeyError, TypeError, ValueError) as exc:
                raise ArtifactLandingError(f"lane manifest source artifact has no numeric id: {landing}") from exc
            if artifact_id <= 0 or artifact_id in source_artifact_ids:
                raise ArtifactLandingError(f"duplicate or invalid original source artifact id: {artifact_id}")
            source_artifact_ids.add(artifact_id)
            expected_file = f"{artifact_id}.zip"
            expected_files.add(expected_file)
            archive = artifact_dir / expected_file
            if not archive.is_file():
                raise ArtifactLandingError(f"lane is missing original source archive: {archive}")
            expected_digest = str(artifact.get("digest", "")).removeprefix("sha256:").lower()
            if sha256(archive) != expected_digest:
                raise ArtifactLandingError(f"original source archive digest mismatch: {archive}")
            actual_bytes += archive.stat().st_size

        actual_files = {path.name for path in artifact_dir.glob("*.zip")} if artifact_dir.is_dir() else set()
        if actual_files != expected_files or actual_bytes != reported_bytes:
            raise ArtifactLandingError(f"lane source archive inventory does not match its manifest: {landing}")
        source_bytes += actual_bytes

    if lanes != set(range(expected_lane_count)):
        raise ArtifactLandingError(f"landed lane set is incomplete: {sorted(lanes)}")
    if len(inventory_run_ids) != 1:
        raise ArtifactLandingError(f"landed lanes disagree on their immutable inventory: {sorted(inventory_run_ids)}")
    return {
        "run_id": run_id,
        "lane_count": expected_lane_count,
        "lanes": sorted(lanes),
        "inventory_run_id": next(iter(inventory_run_ids)),
        "source_artifact_count": len(source_artifact_ids),
        "source_artifact_ids": sorted(source_artifact_ids),
        "source_bytes": source_bytes,
    }


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Land verified MFL Actions artifacts directly on D:")
    parser.add_argument("--repo", default="jeleff1000/mfl-league-fetcher")
    parser.add_argument("--run-id", type=int, action="append", required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--artifact-prefix", default=DEFAULT_ARTIFACT_PREFIX)
    parser.add_argument("--required-file", action="append", default=[])
    parser.add_argument("--token-env", default="GH_TOKEN")
    parser.add_argument("--multi-artifact-layout", action="store_true")
    parser.add_argument(
        "--audit-lane-count",
        type=int,
        help="after landing, verify this complete number of lane bundles for every requested workflow run",
    )
    args = parser.parse_args(list(argv) if argv is not None else None)

    require_d_destination(args.destination)
    if args.audit_lane_count is not None and args.audit_lane_count <= 0:
        parser.error("--audit-lane-count must be positive")
    token = resolve_github_token(args.token_env)
    required_files = resolve_required_files(args.required_file)

    reports: list[dict[str, object]] = []
    for run_id in args.run_id:
        artifacts = artifacts_for_run(args.repo, run_id, token)
        selected = [
            artifact
            for artifact in artifacts
            if str(artifact.get("name", "")).startswith(args.artifact_prefix)
        ]
        if not selected:
            raise ArtifactLandingError(f"workflow run {run_id} has no artifact starting {args.artifact_prefix!r}")
        for artifact in selected:
            if artifact.get("expired") is True:
                raise ArtifactLandingError(f"workflow run {run_id} artifact {artifact.get('id')} is expired")
            reports.append(
                land_artifact(
                    artifact,
                    destination=args.destination,
                    download=lambda _url, target, artifact_id=int(artifact["id"]): download_artifact_with_gh_cli(
                        repo=args.repo,
                        artifact_id=artifact_id,
                        target=target,
                    ),
                    required_files=required_files,
                    multi_artifact_layout=args.multi_artifact_layout,
                )
            )
    lane_audits = []
    if args.audit_lane_count is not None:
        for run_id in args.run_id:
            lane_audits.append(
                audit_landed_lane_bundles(
                    args.destination,
                    run_id=run_id,
                    expected_lane_count=args.audit_lane_count,
                )
            )
    print(json.dumps({"artifacts": reports, "lane_audits": lane_audits}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (ArtifactLandingError, HTTPError) as error:
        print(f"artifact landing failed: {error}", file=sys.stderr)
        raise SystemExit(2)
