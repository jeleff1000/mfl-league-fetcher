"""Real merge requests must not arm process exit during autocommit writes."""
import threading

import pytest

from tests.test_fleet_partition import _build_bundle, data_dir  # noqa: F401
from tests.test_integration import _make_delta_bundle, client  # noqa: F401


@pytest.mark.parametrize("lane", ["delta", "fleet"])
@pytest.mark.parametrize("boundary", ["setup", "VALIDATED", "FAILED_MERGE", "STAGED"])
def test_merge_kill_timer_is_confined_to_transaction(data_dir, client, monkeypatch, lane, boundary):  # noqa: F811
    import main as main_mod

    timers = []
    killed = []
    visits = []

    def start_timer(*_args):
        timer = threading.Timer(0, lambda: killed.append("process_exit"))
        timers.append(timer)
        return timer

    def expire_at(name):
        if name == boundary:
            visits.append(name)
            for timer in timers:
                timer.run()

    ensure = main_mod._ensure_delta_state_table
    first_setup = True

    def setup(conn):
        nonlocal first_setup
        if first_setup:
            first_setup = False
            expire_at("setup")
        return ensure(conn)

    upsert = main_mod._delta_upsert_state

    def record(conn, manifest, status, **kwargs):
        expire_at(status)
        result = upsert(conn, manifest, status, **kwargs)
        if boundary == "FAILED_MERGE" and status == "STAGED":
            raise RuntimeError("injected transaction failure")
        return result

    monkeypatch.setattr(main_mod, "_start_merge_hard_exit_timer", start_timer)
    monkeypatch.setattr(main_mod, "_ensure_delta_state_table", setup)
    monkeypatch.setattr(main_mod, "_delta_upsert_state", record)
    if lane == "delta":
        path, manifest = _make_delta_bundle(data_dir, main_mod)
        db_name = manifest["db_name"]
        endpoint = "/merge-league-delta"
    else:
        bundle = _build_bundle(data_dir)
        path = bundle.path
        manifest = {"bundle_id": bundle.bundle_id, "bundle_hash": bundle.bundle_hash}
        db_name = "___fleet"
        endpoint = "/merge-fleet-partition"
    headers = {
        "Authorization": "Bearer test-admin", "X-Db-Name": db_name,
        "X-Bundle-Id": manifest["bundle_id"], "X-Bundle-Hash": manifest["bundle_hash"],
    }
    with path.open("rb") as stream:
        response = client.post(endpoint, headers=headers,
                               files={"file": ("bundle.tar.gz", stream, "application/gzip")})
    assert response.status_code == (500 if boundary == "FAILED_MERGE" else 200), response.text
    status = client.get("/merge-league-delta/status", headers=headers,
                        params={"db_name": db_name, "bundle_id": manifest["bundle_id"]})
    assert status.status_code == 200, status.text
    assert status.json()["status"] == ("FAILED_MERGE" if boundary == "FAILED_MERGE" else "COMMITTED")
    assert visits == [boundary]
    assert killed == (["process_exit"] if boundary == "STAGED" else [])
    assert len(timers) == 1
    assert timers[0].finished.is_set()
