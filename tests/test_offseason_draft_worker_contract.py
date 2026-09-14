from pathlib import Path


WORKFLOW = Path(__file__).resolve().parents[1] / ".github" / "workflows" / "offseason_draft_update_worker.yml"


def test_scoped_execute_reports_status_and_revalidates_public_cache():
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "REVALIDATION_SECRET: ${{ secrets.REVALIDATION_SECRET }}" in workflow
    assert 'if [ -n "${DB_NAMES}" ] && [ -n "${DISPATCH_TOKEN}" ]; then' in workflow
    assert "args+=(--track-dispatch --revalidate-cache)" in workflow
    assert "args+=(--dispatch-token \"${DISPATCH_TOKEN}\")" in workflow


def test_worker_converts_system_exit_into_a_terminal_failure():
    script = (Path(__file__).resolve().parents[1] / "scripts" / "check_offseason_drafts.py").read_text(
        encoding="utf-8"
    )

    assert "except (Exception, SystemExit) as exc:" in script
    assert "if args.dispatch_token and not status_claimed:" in script
    assert "refusing to fetch or write data" in script
