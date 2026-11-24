"""Update budgets are sized to the work: scoped 120s, full rebuilds 210s.

Regression: bohannon_s_league_ng's one-time full-season rebuild was killed at
the flat 120s deadline while its Yahoo download was still progressing
(runs 45433062679, 45433763568).
"""

import pytest

from multi_league.core import league_refresh as lr


@pytest.fixture
def window(monkeypatch):
    monkeypatch.setattr(lr.time, "time", lambda: 1000.0)
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", str(1000 + lr.UPDATE_DEADLINE_FULL_SECONDS))


def test_scoped_update_tightens_to_two_minutes(window, monkeypatch):
    assert lr.size_update_deadline([10, 11], arm_hard_exit=False) == 120
    assert lr.os.environ["LEAGUE_UPDATE_DEADLINE_EPOCH"] == "1120"


def test_three_week_update_is_still_scoped(window):
    assert lr.size_update_deadline([10, 11, 12], arm_hard_exit=False) == 120
    assert lr.os.environ["LEAGUE_UPDATE_DEADLINE_EPOCH"] == "1120"


def test_full_rebuild_keeps_the_rebuild_window(window):
    assert lr.size_update_deadline(range(1, 12), arm_hard_exit=False) == 210
    assert lr.os.environ["LEAGUE_UPDATE_DEADLINE_EPOCH"] == "1210"


def test_scoped_budget_is_measured_from_the_workflow_window_start(monkeypatch):
    # The window started at (deadline - 210); scoped ends 120s after that,
    # however far into the run the plan is decided.
    monkeypatch.setattr(lr.time, "time", lambda: 1030.0)
    monkeypatch.setenv("LEAGUE_UPDATE_DEADLINE_EPOCH", "1210")
    lr.size_update_deadline([11], arm_hard_exit=False)
    assert lr.os.environ["LEAGUE_UPDATE_DEADLINE_EPOCH"] == "1120"


def test_no_deadline_outside_update_workers(monkeypatch):
    monkeypatch.delenv("LEAGUE_UPDATE_DEADLINE_EPOCH", raising=False)
    assert lr.size_update_deadline([11], arm_hard_exit=False) is None
    assert "LEAGUE_UPDATE_DEADLINE_EPOCH" not in lr.os.environ


def test_scoped_hard_exit_fires_at_the_scoped_deadline(window, monkeypatch):
    armed = {}

    class _Timer:
        def __init__(self, delay, fn):
            armed["delay"] = delay
            self.daemon = False

        def start(self):
            armed["started"] = True

    import threading

    monkeypatch.setattr(threading, "Timer", _Timer)
    lr.size_update_deadline([11])
    assert armed == {"delay": 120.0, "started": True}
