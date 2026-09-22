import asyncio
from pathlib import Path
import time


def test_server_source_contains_no_process_hard_exit():
    source = (Path(__file__).parents[1] / "main.py").read_text(encoding="utf-8")
    assert "os._exit(" not in source


def test_fly_grants_sigterm_enough_time_for_bounded_drain():
    config = (Path(__file__).parents[1] / "fly.toml").read_text(encoding="utf-8")
    assert "kill_signal = 'SIGTERM'" in config
    assert "kill_timeout = 300" in config
    assert "SHUTDOWN_DRAIN_SECONDS = '240'" in config


def test_shutdown_orders_admission_checkpoint_close(monkeypatch):
    import main

    events = []

    async def checkpoint(reason, *, force=False):
        assert main._state["status"] == "shutting_down"
        events.append(("checkpoint", reason, force))
        return True

    monkeypatch.setattr(main, "_checkpoint_if_due", checkpoint)
    monkeypatch.setattr(main.db, "get_active_count", lambda: 0)
    monkeypatch.setattr(main.db, "is_storage_recovery_mode", lambda: False)
    monkeypatch.setattr(main.db, "close_all", lambda: events.append(("close",)))
    monkeypatch.setattr(main, "_ops_read_count", 0)
    monkeypatch.setattr(main, "_ops_write_count", 0)
    monkeypatch.setattr(main, "_delta_publish_inflight", 0)

    async def run_case():
        main._merge_lock = asyncio.Lock()
        main._state["status"] = "serving"
        assert await main.shutdown_database(deadline=time.monotonic() + 1) is True

    asyncio.run(run_case())
    assert events == [("checkpoint", "shutdown", True), ("close",)]
    assert main._state["status"] == "stopped"


def test_shutdown_does_not_close_storage_before_activity_drains(monkeypatch):
    import main

    closed = []
    monkeypatch.setattr(main.db, "get_active_count", lambda: 1)
    monkeypatch.setattr(main.db, "close_all", lambda: closed.append(True))
    monkeypatch.setattr(main, "_ops_read_count", 0)
    monkeypatch.setattr(main, "_ops_write_count", 0)
    monkeypatch.setattr(main, "_delta_publish_inflight", 0)

    async def run_case():
        main._merge_lock = asyncio.Lock()
        assert await main.shutdown_database(deadline=time.monotonic() + 0.02) is False

    asyncio.run(run_case())
    assert closed == []
    assert main._state["status"] == "shutdown_blocked"
