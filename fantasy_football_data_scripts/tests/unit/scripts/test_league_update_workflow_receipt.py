from __future__ import annotations

import pytest

from scripts.league_update_workflow_receipt import classify_publication, failure_status


def test_manual_publication_runs_cache_only_for_a_committed_receipt():
    assert classify_publication({"status": "COMMITTED", "executed": True}, require_publication=False) is True
    assert classify_publication({"status": "NO_FINALIZED_WEEKS", "executed": True}, require_publication=False) is False


def test_ui_update_rejects_a_no_op_before_cache_or_success_status():
    with pytest.raises(ValueError, match="did not publish"):
        classify_publication({"status": "NO_ACTIVE_RENEWAL", "executed": True}, require_publication=True)
    with pytest.raises(ValueError, match="missing"):
        classify_publication(None, require_publication=True)


def test_post_commit_cache_failure_is_recoverable_without_republishing():
    assert failure_status({"status": "COMMITTED", "executed": True, "source_fingerprint": "2026:1:changed"}) == "committed_cache_pending"
    assert failure_status({"status": "COMMITTED", "executed": False, "source_fingerprint": "2026:1:changed"}) == "failed"
    assert failure_status({"status": "NO_ACTIVE_RENEWAL"}) == "incomplete_source"
    assert failure_status(None) == "failed"
