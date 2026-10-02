from datetime import datetime, timezone

from scripts.live_nfl_ops_dispatch_gate import should_run


def test_delayed_scheduled_run_remains_authorized() -> None:
    delayed_morning_run = datetime(2026, 10, 2, 13, 53, tzinfo=timezone.utc)

    assert should_run("schedule", delayed_morning_run) is True


def test_late_repository_dispatch_remains_rejected() -> None:
    late_morning_dispatch = datetime(2026, 10, 2, 13, 53, tzinfo=timezone.utc)

    assert should_run("repository_dispatch", late_morning_dispatch) is False
