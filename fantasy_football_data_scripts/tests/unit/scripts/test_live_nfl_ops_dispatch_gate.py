from datetime import datetime, timezone

from scripts.live_nfl_ops_dispatch_gate import is_duplicate, should_run


def test_delayed_scheduled_run_remains_authorized() -> None:
    delayed_morning_run = datetime(2026, 10, 2, 13, 53, tzinfo=timezone.utc)

    assert should_run("schedule", delayed_morning_run) is True


def test_late_repository_dispatch_remains_rejected() -> None:
    late_morning_dispatch = datetime(2026, 10, 2, 13, 53, tzinfo=timezone.utc)

    assert should_run("repository_dispatch", late_morning_dispatch) is False


def test_clock_dispatch_can_start_late_without_losing_its_original_date():
    now = datetime(2026, 10, 12, 19, tzinfo=timezone.utc)
    assert should_run('repository_dispatch', now,
        scheduled_for='2026-10-12T03:00:00-04:00', dispatch_key='ops-2026-10-12') is True


def test_future_clock_dispatch_is_not_admitted():
    now = datetime(2026, 10, 12, 6, tzinfo=timezone.utc)
    assert should_run('repository_dispatch', now,
        scheduled_for='2026-10-12T03:00:00-04:00', dispatch_key='ops-2026-10-12') is False


def test_clock_date_and_key_must_match():
    now = datetime(2026, 10, 12, 19, tzinfo=timezone.utc)
    assert should_run('repository_dispatch', now,
        scheduled_for='2026-10-12T03:00:00-04:00', dispatch_key='ops-2026-10-11') is False


def test_successful_earlier_daily_run_suppresses_duplicate():
    runs = [{'id': 10, 'display_title': 'Refresh Live NFL Ops scheduled ops-2026-10-12',
        'status': 'completed', 'conclusion': 'success'}]
    assert is_duplicate('ops-2026-10-12', 11, runs) is True


def test_failed_earlier_daily_run_does_not_block_recovery():
    runs = [{'id': 10, 'display_title': 'Refresh Live NFL Ops scheduled ops-2026-10-12',
        'status': 'completed', 'conclusion': 'failure'}]
    assert is_duplicate('ops-2026-10-12', 11, runs) is False


def test_pending_earlier_daily_run_retains_ownership():
    runs = [{'id': 10, 'display_title': 'Refresh Live NFL Ops scheduled ops-2026-10-12',
        'status': 'queued', 'conclusion': None}]
    assert is_duplicate('ops-2026-10-12', 11, runs) is True
