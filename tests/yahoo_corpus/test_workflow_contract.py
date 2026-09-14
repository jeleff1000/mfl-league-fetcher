from __future__ import annotations

from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[2] / ".github" / "workflows" / "yahoo_corpus_pilot.yml"


def workflow_text() -> str:
    return WORKFLOW.read_text(encoding="utf-8")


def test_workflow_is_worker_owned_and_defaults_to_small_pilot() -> None:
    text = workflow_text()

    assert "name: Yahoo Corpus Pilot" in text
    assert "github.repository == 'jeleff1000/mfl-league-fetcher'" in text
    assert "default: '12'" in text
    assert "default: '1'" in text
    assert "worker_ref" not in text
    assert "scripts/yahoo_corpus/cli.py" in text


def test_workflow_has_read_only_planning_secrets_and_no_fly_write_secret() -> None:
    text = workflow_text()

    assert "secrets.DATABASE_SERVER_URL" in text
    assert "secrets.DATABASE_READ_TOKEN" in text
    assert "secrets.CREDENTIAL_ENCRYPTION_KEY" in text
    assert "secrets.YAHOO_CLIENT_ID" in text
    assert "secrets.YAHOO_CLIENT_SECRET" in text
    assert "DATABASE_WRITE_TOKEN" not in text
    assert "DATABASE_ADMIN_TOKEN" not in text
    assert "FLY_API_TOKEN" not in text


def test_workflow_restores_dependencies_and_runs_controlled_resume() -> None:
    text = workflow_text()

    assert "corpus-dependencies-v1-Linux-20260717" in text
    assert "            /scripts/\n" in text
    assert "--stop-after 4" in text
    assert "--resume" in text
    assert "if: always()" in text


def test_artifact_allowlist_excludes_private_and_raw_paths() -> None:
    text = workflow_text()
    upload = text.split("uses: actions/upload-artifact@v4", 1)[1]

    for allowed in (
        "yahoo_corpus_slice.duckdb",
        "ledger.json",
        "report.json",
        "driver.log",
    ):
        assert allowed in upload
    assert "plan.json" not in upload
    assert ".private" not in upload
    assert "league_context" not in upload
    assert "**" not in upload
