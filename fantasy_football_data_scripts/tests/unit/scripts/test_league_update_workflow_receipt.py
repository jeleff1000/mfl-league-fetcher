from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts import league_update_workflow_receipt as subject
from scripts.league_update_workflow_receipt import classify_publication, failure_status


def test_manual_no_change_is_not_a_commit_and_ui_no_change_is_rejected():
    assert classify_publication({"status": "NO_FINALIZED_WEEKS"}, require_publication=False) is False
    with pytest.raises(ValueError, match="did not publish"):
        classify_publication({"status": "NO_FINALIZED_WEEKS"}, require_publication=True)


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


def test_cancelled_uncommitted_claim_is_terminal_but_committed_data_stays_recoverable():
    assert failure_status(None, cancelled=True) == "cancelled"
    assert failure_status(
        {"status": "COMMITTED", "executed": True, "source_fingerprint": "2026:1:changed"},
        cancelled=True,
    ) == "committed_cache_pending"


def _committed_input():
    return {
        "db_name": "receipt_canary", "year": 2026, "executed": True,
        "source_year": 2026, "source_week": 1, "base_generation": 3,
        "source_manifest_digest": "captured-source", "source_manifest_complete": False,
    }


@pytest.mark.parametrize("error", [TimeoutError("diagnostic read"), OSError("cleanup"), KeyboardInterrupt()])
def test_saved_commit_survives_later_worker_failure(tmp_path, error):
    path = tmp_path / "receipt.json"
    receipt = _committed_input()
    with pytest.raises(type(error)):
        subject.record_publication_commit(
            receipt, result={"status": "COMMITTED"}, bundle_id="bundle-1", path=path,
        )
        raise error

    saved = subject._read_receipt(path)
    assert saved == {
        **_committed_input(), "status": "COMMITTED", "bundle_id": "bundle-1",
        "data_bundle_id": "bundle-1", "homepage_bundle_id": "bundle-1",
    }
    assert classify_publication(saved, require_publication=True) is True
    assert failure_status(saved, cancelled=isinstance(error, KeyboardInterrupt)) == "committed_cache_pending"
    # A data commit is not an acknowledgment that the entire source manifest was consumed.
    assert saved["source_manifest_complete"] is False


def test_commit_receipt_keeps_server_stage_timings(tmp_path):
    path = tmp_path / "receipt.json"
    receipt = _committed_input()
    result = {
        "status": "COMMITTED",
        "elapsed_seconds": 12.5,
        "merge_seconds": 13.0,
        "server_total_seconds": 13.2,
        "lock_wait_seconds": 0.2,
        "season_stage_seconds": {"receipt_canary": {"rollup_build": 2.1}},
        "timings": {"matchup": 0.4},
    }

    subject.record_publication_commit(
        receipt, result=result, bundle_id="bundle-1", path=path,
    )

    assert receipt["publication_timing"] == {
        "elapsed_seconds": 12.5,
        "merge_seconds": 13.0,
        "server_total_seconds": 13.2,
        "lock_wait_seconds": 0.2,
        "season_stage_seconds": {"receipt_canary": {"rollup_build": 2.1}},
        "timings": {"matchup": 0.4},
    }


@pytest.mark.parametrize("status,executed,bundle_id", [
    ("VALIDATED", True, "bundle-1"), ("FAILED_MERGE", True, "bundle-1"),
    ("COMMITTED", False, "bundle-1"), ("COMMITTED", True, ""),
])
def test_unconfirmed_publication_cannot_create_a_commit_receipt(tmp_path, status, executed, bundle_id):
    path = tmp_path / "receipt.json"
    receipt = {**_committed_input(), "executed": executed}
    with pytest.raises(ValueError):
        subject.record_publication_commit(receipt, result={"status": status}, bundle_id=bundle_id, path=path)
    assert not path.exists()
    assert receipt.get("status") != "COMMITTED"


@pytest.mark.parametrize("worker_name", ["espn", "sleeper"])
def test_worker_receipt_is_saved_even_if_stdout_is_closed(tmp_path, monkeypatch, worker_name):
    import importlib

    worker = importlib.import_module(f"scripts.refresh_{worker_name}_active_season")
    path = tmp_path / "receipt.json"
    receipt = {**_committed_input(), "status": "COMMITTED"}

    def closed_stdout(*_args, **_kwargs):
        raise BrokenPipeError("closed diagnostic pipe")

    monkeypatch.setattr("builtins.print", closed_stdout)
    worker._write_receipt(receipt, path)
    assert json.loads(path.read_text(encoding="utf-8")) == receipt


@pytest.mark.parametrize("failure_point", ["write", "replace", "serialize"])
def test_failed_receipt_enrichment_never_truncates_the_saved_commit(tmp_path, monkeypatch, failure_point):
    path = tmp_path / "receipt.json"
    receipt = _committed_input()
    subject.record_publication_commit(receipt, result={"status": "COMMITTED"}, bundle_id="bundle-1", path=path)
    original_bytes = path.read_bytes()
    enriched = {**receipt, "post_publish_counts": {"matchup": 12}}
    if failure_point == "serialize":
        enriched["not_serializable"] = object()
        error_type = TypeError
    else:
        error_type = OSError
        if failure_point == "write":
            write_text = Path.write_text

            def interrupted_write(self, *args, **kwargs):
                write_text(self, "{partial", encoding="utf-8")
                raise OSError("disk write interrupted")

            monkeypatch.setattr(Path, "write_text", interrupted_write)
        else:
            def interrupted_replace(*_args, **_kwargs):
                raise OSError("replace interrupted")

            monkeypatch.setattr(Path, "replace", interrupted_replace)

    with pytest.raises(error_type):
        subject.write_refresh_receipt(enriched, path)
    assert path.read_bytes() == original_bytes
    assert failure_status(subject._read_receipt(path)) == "committed_cache_pending"


def test_successful_receipt_enrichment_replaces_the_previous_complete_json(tmp_path):
    path = tmp_path / "receipt.json"
    receipt = _committed_input()
    subject.record_publication_commit(receipt, result={"status": "COMMITTED"}, bundle_id="bundle-1", path=path)
    receipt["post_publish_counts"] = {"matchup": 12}
    subject.write_refresh_receipt(receipt, path)
    assert json.loads(path.read_text(encoding="utf-8")) == receipt
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_early_receipt_drives_real_cache_only_recovery_without_republishing(tmp_path, platform):
    import duckdb

    from multi_league.core.league_update_status import (
        build_cache_recovery_receipt, record_league_update_status,
    )

    path = tmp_path / "receipt.json"
    receipt = _committed_input()
    subject.record_publication_commit(receipt, result={"status": "COMMITTED"}, bundle_id="bundle-1", path=path)
    # No diagnostic counts or later worker fields were saved. The workflow's
    # failure handler must still persist the exact committed/cache-pending state.
    saved = subject._read_receipt(path)
    with duckdb.connect(":memory:") as conn:
        class LocalWriter:
            def execute(self, sql, *, database):
                assert database == "___ops"
                return conn.execute(sql)

        writer = LocalWriter()
        owner = dict(database_name="receipt_canary", platform=platform,
                     dispatch_token="owner", workflow_run_id=42)
        assert record_league_update_status(writer, **owner, status="running")
        assert record_league_update_status(writer, **owner, status=failure_status(saved), receipt=saved)
        stored = conn.execute(
            "SELECT * FROM accounts.league_update_dispatches WHERE database_name='receipt_canary'"
        ).df().iloc[0].to_dict()
        assert stored["status"] == "committed_cache_pending"
        recovered = build_cache_recovery_receipt(stored, current_generation=4)
        assert recovered["bundle_id"] == "bundle-1"
        assert recovered["source_manifest_complete"] is False
        assert record_league_update_status(
            writer, **owner, status="succeeded", receipt=recovered, cache_verified=True,
        )
        assert conn.execute(
            "SELECT status,bundle_id FROM accounts.league_update_dispatches WHERE database_name='receipt_canary'"
        ).fetchone() == ("succeeded", "bundle-1")
        with pytest.raises(ValueError, match="newer publication"):
            build_cache_recovery_receipt(stored, current_generation=5)


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_worker_cli_loads_publication_helpers_without_pytest_root_path(tmp_path, platform):
    root = Path(subject.__file__).resolve().parents[1]
    probe = """
import pathlib, runpy, sys
root = pathlib.Path(sys.argv[1])
script = root / 'scripts' / ('refresh_' + sys.argv[2] + '_active_season.py')
assert str(root) not in sys.path
sys.path.insert(0, str(script.parent))
sys.path.insert(0, str(root / 'fantasy_football_data_scripts'))
from multi_league.core.readers.fly_reader import FlyReader
class StartupComplete(BaseException):
    pass
def stop_before_io(self):
    raise StartupComplete
FlyReader.__init__ = stop_before_io
def reject_network(event, args):
    if event in {'socket.connect', 'socket.getaddrinfo'}:
        raise AssertionError('CLI startup must not use network')
sys.addaudithook(reject_network)
sys.argv = [str(script), '--db', 'receipt_canary', '--year', '2026']
try:
    runpy.run_path(str(script), run_name='__main__')
except StartupComplete:
    print('IMPORTS_COMPLETE_NO_IO')
else:
    raise AssertionError('Worker did not reach the reader boundary')
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", probe, str(root), platform],
        cwd=tmp_path, text=True, capture_output=True, timeout=10,
    )
    assert result.returncode == 0, result.stderr
    assert "IMPORTS_COMPLETE_NO_IO" in result.stdout
