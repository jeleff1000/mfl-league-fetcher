"""Permanent storage faults must not fan out into repeated Fly requests."""
import json
from unittest.mock import patch

import pytest
import requests

from multi_league.core.fly_writer import FlyWriter
from multi_league.core.readers.fly_reader import FlyReader
from multi_league.core.targets.fly_target import FlyTarget


@pytest.mark.parametrize("detail", [
    "IO Error: Corrupt database file at block 90714112",
    "IO Error: Computed checksum 5168518579405463287 does not match stored checksum 18392342689821271652",
    "FATAL Error: database has been invalidated because of a previous fatal error",
    "TransactionContext Error: Current transaction is aborted (please ROLLBACK)",
    "Binder Error: No function matches trim(INTEGER)",
])
@pytest.mark.parametrize("operation", ["json", "parquet", "write", "upload"])
def test_permanent_storage_error_stops_after_one_request(monkeypatch, tmp_path, detail, operation):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_READ_TOKEN", "read-test")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-test")
    if operation == "upload":
        # Exercise the upload contract without an unrelated inventory read.
        monkeypatch.delenv("DATABASE_READ_TOKEN")
    response = requests.Response()
    response.status_code = 500
    response._content = detail.encode()
    payload = tmp_path / "small.duckdb"
    payload.write_bytes(b"test-upload")
    with patch("requests.post", return_value=response) as post, patch("time.sleep") as sleep:
        with pytest.raises(RuntimeError, match="500"):
            if operation == "json":
                FlyReader().query("SELECT 1", database="___leagues")
            elif operation == "parquet":
                FlyReader().query_df_parquet("SELECT 1", database="___leagues")
            elif operation == "write":
                FlyWriter().execute("SELECT 1")
            else:
                FlyTarget().merge_league("test_league", payload)
    assert post.call_count == 1
    sleep.assert_not_called()


@pytest.mark.parametrize("lane", ["delta", "fleet"])
def test_storage_error_after_commit_still_reconciles_receipt(monkeypatch, tmp_path, lane):
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.test")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-test")
    failure = requests.Response()
    failure.status_code = 500
    failure._content = b"FATAL Error: database has been invalidated"
    receipt = requests.Response()
    receipt.status_code = 200
    receipt._content = json.dumps({
        "status": "COMMITTED", "bundle_id": "same-bundle", "bundle_hash": "same-hash",
    }).encode()
    bundle = tmp_path / "bundle.tar.gz"
    bundle.write_bytes(b"small-bundle")
    with (
        patch("requests.post", return_value=failure) as post,
        patch("requests.get", return_value=receipt) as get,
        patch("time.sleep") as sleep,
    ):
        target = FlyTarget()
        kwargs = {"bundle_id": "same-bundle", "bundle_hash": "same-hash"}
        if lane == "delta":
            result = target.merge_league_delta("test_league", bundle, **kwargs)
        else:
            result = target.merge_fleet_partition(bundle, **kwargs)
    assert result["status"] == "COMMITTED"
    assert result["recovered_after_ambiguous_failure"] is True
    assert post.call_count == 1
    assert get.call_count == 1
    assert get.call_args.kwargs["params"] == {
        "db_name": "test_league" if lane == "delta" else "___fleet", "bundle_id": "same-bundle",
    }
    sleep.assert_not_called()
