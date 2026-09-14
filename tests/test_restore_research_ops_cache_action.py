"""Behavioral coverage for the shared GitHub Actions ops-cache fallback."""

from __future__ import annotations

import subprocess
from pathlib import Path

from scripts import download_release_asset


def test_release_backup_retries_a_transient_github_failure(tmp_path: Path, monkeypatch) -> None:
    """A one-off GitHub 500 must not abort a full import before enrichment starts."""
    output_dir = tmp_path / "research_public_lake"
    invocations: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
        invocations.append(command)
        if len(invocations) == 1:
            return subprocess.CompletedProcess(command, returncode=1)
        (output_dir / "ops_cache.duckdb").write_text("cached-ops", encoding="utf-8")
        return subprocess.CompletedProcess(command, returncode=0)

    monkeypatch.setattr(download_release_asset.subprocess, "run", fake_run)
    monkeypatch.setattr(download_release_asset.time, "sleep", lambda _: None)

    result = download_release_asset.download_release_asset(
        release_tag="ops-cache-test",
        release_repository="league-history-workers/mfl-league-fetcher",
        pattern="ops_cache.duckdb",
        output_dir=output_dir,
        attempts=3,
    )

    assert result == 0
    assert len(invocations) == 2
    assert (output_dir / "ops_cache.duckdb").read_text(encoding="utf-8") == "cached-ops"


def test_release_backup_removes_a_partial_asset_before_retrying(tmp_path: Path, monkeypatch) -> None:
    """A failed first download cannot prevent the retry from replacing its partial file."""
    output_dir = tmp_path / "research_public_lake"
    asset_path = output_dir / "ops_cache.duckdb"
    invocations: list[list[str]] = []

    def fake_run(command: list[str], *, check: bool) -> subprocess.CompletedProcess:
        invocations.append(command)
        if len(invocations) == 1:
            asset_path.write_text("partial", encoding="utf-8")
            return subprocess.CompletedProcess(command, returncode=1)
        if asset_path.exists():
            return subprocess.CompletedProcess(command, returncode=1)
        asset_path.write_text("cached-ops", encoding="utf-8")
        return subprocess.CompletedProcess(command, returncode=0)

    monkeypatch.setattr(download_release_asset.subprocess, "run", fake_run)
    monkeypatch.setattr(download_release_asset.time, "sleep", lambda _: None)

    result = download_release_asset.download_release_asset(
        release_tag="ops-cache-test",
        release_repository="league-history-workers/mfl-league-fetcher",
        pattern="ops_cache.duckdb",
        output_dir=output_dir,
        attempts=3,
    )

    assert result == 0
    assert len(invocations) == 2
    assert asset_path.read_text(encoding="utf-8") == "cached-ops"
