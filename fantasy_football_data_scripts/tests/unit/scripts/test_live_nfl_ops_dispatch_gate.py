from datetime import datetime, timezone

from scripts.live_nfl_ops_dispatch_gate import should_run


def test_delayed_scheduled_run_is_rejected() -> None:
    delayed_morning_run = datetime(2026, 10, 2, 13, 53, tzinfo=timezone.utc)

    assert should_run("schedule", delayed_morning_run) is False


def test_scheduled_run_is_admitted_during_three_am_eastern_in_daylight_time() -> None:
    three_seventeen_am_edt = datetime(2026, 10, 2, 7, 17, tzinfo=timezone.utc)

    assert should_run("schedule", three_seventeen_am_edt) is True


def test_scheduled_run_is_admitted_during_three_am_eastern_in_standard_time() -> None:
    three_seventeen_am_est = datetime(2026, 12, 2, 8, 17, tzinfo=timezone.utc)

    assert should_run("schedule", three_seventeen_am_est) is True


def test_late_repository_dispatch_remains_rejected() -> None:
    late_morning_dispatch = datetime(2026, 10, 2, 13, 53, tzinfo=timezone.utc)

    assert should_run("repository_dispatch", late_morning_dispatch) is False
