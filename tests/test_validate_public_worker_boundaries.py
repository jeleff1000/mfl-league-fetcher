from pathlib import Path

from scripts.validate_public_worker_boundaries import scan_repository


def test_scan_repository_reports_every_private_worker_dependency(tmp_path: Path) -> None:
    workflow_dir = tmp_path / ".github" / "workflows"
    workflow_dir.mkdir(parents=True)
    (workflow_dir / "bad.yml").write_text(
        """
jobs:
  worker:
    steps:
      - uses: actions/checkout@v5
        with:
          repository: jeleff1000/yahoo_oauth
          token: ${{ secrets.PRIVATE_REPO_PAT }}
          ref: ${{ inputs.yahoo_oauth_ref }}
      - run: gh api repos/league-history-workers/mfl-league-fetcher/actions/runs
""".lstrip(),
        encoding="utf-8",
    )

    violations = scan_repository(tmp_path)

    assert [violation.rule for violation in violations] == [
        "enterprise-worker-repository",
        "private-application-checkout",
        "private-repository-token",
        "private-source-ref",
    ]


def test_scan_repository_ignores_public_self_checkout_and_documentation(tmp_path: Path) -> None:
    workflow_dir = tmp_path / ".github" / "workflows"
    workflow_dir.mkdir(parents=True)
    (workflow_dir / "good.yml").write_text(
        """
jobs:
  worker:
    steps:
      - uses: actions/checkout@v5
      - run: gh api repos/jeleff1000/mfl-league-fetcher/actions/runs
""".lstrip(),
        encoding="utf-8",
    )
    (tmp_path / "migration-notes.md").write_text(
        "Historical note: repository: jeleff1000/yahoo_oauth\n",
        encoding="utf-8",
    )

    assert scan_repository(tmp_path) == []


def test_scan_repository_rejects_renamed_mutable_worker_ref(tmp_path: Path) -> None:
    workflow_dir = tmp_path / ".github" / "workflows"
    workflow_dir.mkdir(parents=True)
    (workflow_dir / "branch.yml").write_text(
        "on:\n  workflow_dispatch:\n    inputs:\n      worker_ref:\n",
        encoding="utf-8",
    )

    violations = scan_repository(tmp_path)

    assert [violation.rule for violation in violations] == ["mutable-worker-ref"]


def test_scan_repository_rejects_redirected_public_repository_name(tmp_path: Path) -> None:
    workflow_dir = tmp_path / ".github" / "workflows"
    workflow_dir.mkdir(parents=True)
    (workflow_dir / "redirect.yml").write_text(
        "steps:\n  - run: gh api repos/jeleff1000/league-history-workers/actions/runs\n",
        encoding="utf-8",
    )

    violations = scan_repository(tmp_path)

    assert [violation.rule for violation in violations] == ["redirected-public-repository"]
