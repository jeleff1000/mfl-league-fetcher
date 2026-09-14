"""Safely archive and remove completed MFL campaign runs.

The MFL campaign created one durable chunk plus hundreds of transport
artifacts per successful run.  This module defines the fail-closed planning
and archive-verification gates used before a caller can remove such a run.
"""

from __future__ import annotations

import argparse
import base64
from dataclasses import dataclass
import json
import os
from pathlib import PurePosixPath
import re
import subprocess
import sys
import time
from typing import Callable, Iterable, Mapping
from urllib.error import HTTPError
from urllib.parse import urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener


DEFAULT_REQUIRED_FILES = frozenset(
    {
        "mfl_register_chunk.duckdb",
        "mfl_register_all_runs.json",
        "cache_append_proof.json",
        "mfl_research_overlay.duckdb",
        "mfl_research_overlay_proof.json",
    }
)
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_SAFE_NAME = re.compile(r"^[A-Za-z0-9_.-]+$")


class CampaignArchiveError(RuntimeError):
    """A campaign run cannot safely be archived and removed."""


@dataclass(frozen=True)
class CompletedChunkPlan:
    """The single durable artifact that permits deleting one campaign run."""

    run_id: int
    artifact_id: int
    artifact_name: str
    expected_sha256: str
    remote_archive: str


def _positive_int(value: object, *, label: str) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise CampaignArchiveError(f"{label} must be a positive integer") from exc
    if result <= 0:
        raise CampaignArchiveError(f"{label} must be a positive integer")
    return result


def _digest(value: object) -> str:
    digest = str(value or "").removeprefix("sha256:").lower()
    if not _SHA256.fullmatch(digest):
        raise CampaignArchiveError("durable chunk has no valid SHA-256 digest")
    return digest


def _remote_archive_path(remote_root: str, *, run_id: int, artifact_id: int, artifact_name: str) -> str:
    root = PurePosixPath(remote_root)
    if not root.is_absolute() or ".." in root.parts:
        raise CampaignArchiveError("remote archive root must be an absolute safe POSIX path")
    if not _SAFE_NAME.fullmatch(artifact_name):
        raise CampaignArchiveError(f"durable artifact name is unsafe: {artifact_name!r}")
    return str(root / str(run_id) / f"{artifact_name}-{artifact_id}.zip")


def _is_known_supporting_artifact(name: str, run_id: int) -> bool:
    return name.startswith("mfl-register-batch-") or name in {
        f"mfl-accepted-keys-{run_id}",
        f"mfl-batch-plan-{run_id}",
        f"mfl-wave-manifest-{run_id}",
        f"mfl-pending-manifest-{run_id}",
    }


def plan_completed_chunk_run(
    run: Mapping[str, object],
    artifacts: Iterable[Mapping[str, object]],
    *,
    remote_root: str,
) -> CompletedChunkPlan:
    """Return a deletion-eligible plan only for one known successful run."""

    run_id = _positive_int(run.get("id"), label="workflow run id")
    if run.get("status") != "completed" or run.get("conclusion") != "success":
        raise CampaignArchiveError(f"workflow run {run_id} is not a completed successful run")

    durable_name = f"mfl-register-chunk-{run_id}"
    chunks: list[Mapping[str, object]] = []
    for artifact in artifacts:
        name = str(artifact.get("name", ""))
        if name == durable_name:
            chunks.append(artifact)
        elif not _is_known_supporting_artifact(name, run_id):
            raise CampaignArchiveError(f"workflow run {run_id} has unknown artifact: {name!r}")

    if len(chunks) != 1:
        raise CampaignArchiveError(f"workflow run {run_id} must contain exactly one durable chunk")
    chunk = chunks[0]
    if chunk.get("expired") is True:
        raise CampaignArchiveError(f"workflow run {run_id} durable chunk is expired")
    artifact_id = _positive_int(chunk.get("id"), label="durable artifact id")
    expected_sha256 = _digest(chunk.get("digest"))
    return CompletedChunkPlan(
        run_id=run_id,
        artifact_id=artifact_id,
        artifact_name=durable_name,
        expected_sha256=expected_sha256,
        remote_archive=_remote_archive_path(
            remote_root,
            run_id=run_id,
            artifact_id=artifact_id,
            artifact_name=durable_name,
        ),
    )


def verify_remote_chunk_archive(
    *,
    expected_sha256: str,
    actual_sha256: str,
    members: Iterable[str],
) -> None:
    """Require an immutable GitHub digest and the complete durable payload."""

    expected = _digest(expected_sha256)
    actual = _digest(actual_sha256)
    if actual != expected:
        raise CampaignArchiveError(f"remote archive SHA-256 mismatch: expected={expected} actual={actual}")
    actual_members = {str(member).replace("\\", "/") for member in members}
    missing = sorted(DEFAULT_REQUIRED_FILES - actual_members)
    if missing:
        raise CampaignArchiveError(f"remote archive is missing required files: {missing}")


def run_artifacts_from_pages(
    load_page: Callable[[int], Mapping[str, object]],
) -> list[Mapping[str, object]]:
    """Read every page of a run's artifacts; partial lists are never usable."""

    first = load_page(1)
    total = _positive_int(first.get("total_count"), label="artifact total count")
    pages = (total + 99) // 100
    artifacts = list(first.get("artifacts", []))
    for page in range(2, pages + 1):
        artifacts.extend(load_page(page).get("artifacts", []))
    if len(artifacts) != total:
        raise CampaignArchiveError(f"artifact pagination mismatch: expected {total}, got {len(artifacts)}")
    if not all(isinstance(artifact, Mapping) for artifact in artifacts):
        raise CampaignArchiveError("GitHub returned a malformed artifact row")
    return artifacts


def parse_remote_archive_output(output: str) -> tuple[str, list[str]]:
    """Extract the remote archive checksum and `unzip -Z1` member list."""

    digest: str | None = None
    members: list[str] = []
    for line in str(output).splitlines():
        match = re.match(r"^([0-9a-f]{64})\s+", line)
        if match:
            if digest is not None:
                raise CampaignArchiveError("remote archive command returned multiple SHA-256 values")
            digest = match.group(1)
            continue
        candidate = line.strip().replace("\\", "/")
        if candidate:
            path = PurePosixPath(candidate)
            if path.is_absolute() or ".." in path.parts:
                raise CampaignArchiveError(f"remote archive has unsafe member: {candidate!r}")
            if candidate in DEFAULT_REQUIRED_FILES:
                members.append(candidate)
    if digest is None:
        raise CampaignArchiveError("remote archive command returned no SHA-256 value")
    return digest, members


def recover_completed_chunk_run(
    plan: CompletedChunkPlan,
    *,
    apply: bool,
    signed_download_url: Callable[[int], str],
    archive_remote: Callable[[CompletedChunkPlan, str], tuple[str, Iterable[str]]],
    record_remote: Callable[[CompletedChunkPlan], None],
    delete_run: Callable[[int], None],
) -> dict[str, object]:
    """Archive one plan, verify it, and only then remove its Actions run."""

    if not apply:
        return {"status": "dry_run", "run_id": plan.run_id, "artifact_id": plan.artifact_id}
    signed_url = signed_download_url(plan.artifact_id)
    actual_sha256, members = archive_remote(plan, signed_url)
    verify_remote_chunk_archive(
        expected_sha256=plan.expected_sha256,
        actual_sha256=actual_sha256,
        members=members,
    )
    record_remote(plan)
    delete_run(plan.run_id)
    return {"status": "archived_and_delete_requested", "run_id": plan.run_id, "artifact_id": plan.artifact_id}


def _gh_json(arguments: list[str]) -> Mapping[str, object]:
    try:
        payload = json.loads(subprocess.check_output(["gh", "api", *arguments], text=True, stderr=subprocess.DEVNULL))
    except (OSError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        raise CampaignArchiveError(f"GitHub CLI request failed: {' '.join(arguments)}") from exc
    if not isinstance(payload, Mapping):
        raise CampaignArchiveError("GitHub returned a non-object response")
    return payload


def _github_token(token_env: str) -> str:
    token = os.environ.get(token_env) or os.environ.get("GITHUB_TOKEN")
    if not token:
        try:
            token = subprocess.check_output(["gh", "auth", "token"], text=True, stderr=subprocess.DEVNULL).strip()
        except (OSError, subprocess.CalledProcessError) as exc:
            raise CampaignArchiveError(f"set {token_env} or authenticate gh before applying recovery") from exc
    if not token:
        raise CampaignArchiveError(f"set {token_env} or authenticate gh before applying recovery")
    return token


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req: Request, fp: object, code: int, msg: str, headers: object, newurl: str) -> None:
        return None


def _signed_download_url(*, repo: str, artifact_id: int, token: str) -> str:
    request = Request(
        f"https://api.github.com/repos/{repo}/actions/artifacts/{artifact_id}/zip",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        response = build_opener(_NoRedirect()).open(request, timeout=60)
    except HTTPError as exc:
        response = exc
    if getattr(response, "code", None) != 302:
        raise CampaignArchiveError(f"GitHub did not issue a download redirect for artifact {artifact_id}")
    url = str(response.headers.get("Location", ""))
    parsed = urlparse(url)
    if parsed.scheme != "https" or not parsed.netloc or any(character.isspace() or character == "'" for character in url):
        raise CampaignArchiveError("GitHub returned an unsafe signed artifact download URL")
    return url


def winscp_command_args(*, winscp: str, sftp_site: str, remote_command: str) -> list[str]:
    """Build WinSCP arguments without shell quotes leaking into the site name."""

    if not _SAFE_NAME.fullmatch(sftp_site.replace("@", "_")):
        raise CampaignArchiveError("WinSCP site name is unsafe")
    return [winscp, "/command", f"open {sftp_site}", f"call {remote_command}", "exit"]


def _remote_call(*, winscp: str, sftp_site: str, command: str) -> str:
    try:
        completed = subprocess.run(
            winscp_command_args(winscp=winscp, sftp_site=sftp_site, remote_command=command),
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
    except OSError as exc:
        raise CampaignArchiveError("WinSCP remote archive command could not start") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stdout or "").strip().replace("\r", "\n")[-1200:]
        raise CampaignArchiveError(f"WinSCP remote archive command failed: {detail}") from exc
    return completed.stdout


def _quote_remote(value: str) -> str:
    if not value or "'" in value or any(character.isspace() for character in value):
        raise CampaignArchiveError("remote command value is unsafe")
    return f"'{value}'"


def _archive_remote_chunk(*, plan: CompletedChunkPlan, signed_url: str, winscp: str, sftp_site: str) -> tuple[str, Iterable[str]]:
    archive = plan.remote_archive
    archive_dir = str(PurePosixPath(archive).parent)
    partial = f"{archive}.partial"
    command = (
        f"set -eu; mkdir -p {_quote_remote(archive_dir)}; "
        f"if [ -e {_quote_remote(archive)} ]; then test -f {_quote_remote(archive)}; "
        f"else curl --fail --location --silent --show-error --retry 2 --output {_quote_remote(partial)} {_quote_remote(signed_url)} "
        f"&& mv {_quote_remote(partial)} {_quote_remote(archive)}; fi; "
        f"sha256sum {_quote_remote(archive)}; unzip -Z1 {_quote_remote(archive)} | sort"
    )
    return parse_remote_archive_output(_remote_call(winscp=winscp, sftp_site=sftp_site, command=command))


def _write_remote_receipt(*, plan: CompletedChunkPlan, winscp: str, sftp_site: str) -> None:
    receipt = {
        "artifact_id": plan.artifact_id,
        "artifact_name": plan.artifact_name,
        "archive_sha256": plan.expected_sha256,
        "remote_archive": plan.remote_archive,
        "workflow_run_id": plan.run_id,
    }
    encoded = base64.b64encode(json.dumps(receipt, sort_keys=True).encode("utf-8")).decode("ascii")
    receipt_path = f"{plan.remote_archive}.receipt.json"
    command = (
        f"set -eu; if [ ! -e {_quote_remote(receipt_path)} ]; then "
        f"printf '%s' {_quote_remote(encoded)} | base64 -d > {_quote_remote(receipt_path)}.partial "
        f"&& mv {_quote_remote(receipt_path)}.partial {_quote_remote(receipt_path)}; fi"
    )
    _remote_call(winscp=winscp, sftp_site=sftp_site, command=command)


def _delete_run(*, repo: str, run_id: int) -> None:
    try:
        subprocess.run(["gh", "api", "--method", "DELETE", f"repos/{repo}/actions/runs/{run_id}"], check=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        raise CampaignArchiveError(f"GitHub could not delete completed run {run_id}") from exc


def _wait_for_artifact_removal(*, repo: str, artifact_id: int, timeout_seconds: int, poll_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        probe = subprocess.run(
            ["gh", "api", f"repos/{repo}/actions/artifacts/{artifact_id}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if probe.returncode != 0:
            return
        time.sleep(poll_seconds)
    raise CampaignArchiveError(f"artifact {artifact_id} remained after deleting its completed workflow run")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Archive verified MFL campaign chunks to SFTP before removing completed Actions runs")
    parser.add_argument("--run-id", type=int, action="append", required=True)
    parser.add_argument("--repo", default="jeleff1000/league-history-workers")
    parser.add_argument("--remote-root", default="/home/kmffl/actions_artifacts/_github_archives")
    parser.add_argument("--sftp-site", default="kmffl@56.lw.itsby.design")
    parser.add_argument("--winscp", default=r"C:\Program Files (x86)\WinSCP\WinSCP.com")
    parser.add_argument("--token-env", default="GH_TOKEN")
    parser.add_argument("--apply", action="store_true", help="perform remote archive and delete; otherwise print dry-run plans")
    parser.add_argument("--delete-timeout-seconds", type=int, default=120)
    parser.add_argument("--delete-poll-seconds", type=int, default=5)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.delete_timeout_seconds <= 0 or args.delete_poll_seconds <= 0:
        parser.error("delete timeout and poll values must be positive")

    token = _github_token(args.token_env) if args.apply else ""
    reports: list[dict[str, object]] = []
    for run_id in args.run_id:
        run = _gh_json([f"repos/{args.repo}/actions/runs/{run_id}"])
        artifacts = run_artifacts_from_pages(
            lambda page, current_run_id=run_id: _gh_json(
                [f"repos/{args.repo}/actions/runs/{current_run_id}/artifacts?per_page=100&page={page}"]
            )
        )
        plan = plan_completed_chunk_run(run, artifacts, remote_root=args.remote_root)
        report = recover_completed_chunk_run(
            plan,
            apply=args.apply,
            signed_download_url=lambda artifact_id: _signed_download_url(repo=args.repo, artifact_id=artifact_id, token=token),
            archive_remote=lambda current_plan, url: _archive_remote_chunk(
                plan=current_plan, signed_url=url, winscp=args.winscp, sftp_site=args.sftp_site
            ),
            record_remote=lambda current_plan: _write_remote_receipt(
                plan=current_plan, winscp=args.winscp, sftp_site=args.sftp_site
            ),
            delete_run=lambda current_run_id: _delete_run(repo=args.repo, run_id=current_run_id),
        )
        if args.apply:
            _wait_for_artifact_removal(
                repo=args.repo,
                artifact_id=plan.artifact_id,
                timeout_seconds=args.delete_timeout_seconds,
                poll_seconds=args.delete_poll_seconds,
            )
            report["status"] = "archived_and_deleted"
        reports.append(report)
    print(json.dumps({"apply": args.apply, "runs": reports}, sort_keys=True))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except CampaignArchiveError as exc:
        print(f"MFL campaign recovery failed: {exc}", file=sys.stderr)
        raise SystemExit(2)
