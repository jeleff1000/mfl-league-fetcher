import pytest

from multi_league.core.league_update_timing import PhaseTimer


def test_phase_timer_separates_processing_phases_and_total():
    times = iter([10.0, 11.0, 15.0, 18.0])
    timer = PhaseTimer(clock=lambda: next(times))
    timer.mark("source_plan")
    timer.mark("provider_fetch")
    receipt = timer.finish()
    assert receipt == {
        "source_plan": 1.0,
        "provider_fetch": 4.0,
        "unmarked": 3.0,
        "total": 8.0,
    }


def test_phase_timer_rejects_duplicate_or_negative_phase_times():
    times = iter([10.0, 12.0, 11.0])
    timer = PhaseTimer(clock=lambda: next(times))
    timer.mark("source_plan")
    try:
        timer.mark("source_plan")
    except ValueError:
        pass
    else:
        raise AssertionError("duplicate phase was admitted")
    try:
        timer.mark("provider_fetch")
    except ValueError:
        pass
    else:
        raise AssertionError("backward clock was admitted")


def test_completed_phase_is_visible_before_worker_finishes(capsys):
    times = iter([10.0, 11.25, 13.5, 14.0])
    timer = PhaseTimer(clock=lambda: next(times))
    timer.mark("provider_fetch")
    assert capsys.readouterr().out == (
        "[weekly-refresh] phase=provider_fetch seconds=1.250 elapsed_seconds=1.250\n"
    )
    timer.mark("shared_transformations")
    assert capsys.readouterr().out == (
        "[weekly-refresh] phase=shared_transformations seconds=2.250 elapsed_seconds=3.500\n"
    )
    assert timer.finish() == {
        "provider_fetch": 1.25, "shared_transformations": 2.25,
        "unmarked": 0.5, "total": 4.0,
    }


@pytest.mark.parametrize("error", [BrokenPipeError(), OSError(), ValueError("closed stdout")])
def test_log_sink_failure_does_not_change_receipt_or_timing_validation(monkeypatch, error):
    def failed_print(*args, **kwargs):
        raise error

    monkeypatch.setattr("builtins.print", failed_print)
    times = iter([10.0, 11.0, 12.0])
    timer = PhaseTimer(clock=lambda: next(times))
    timer.mark("fly_publication")
    with pytest.raises(ValueError, match="Duplicate or reserved"):
        timer.mark("fly_publication")
    assert timer.finish() == {"fly_publication": 1.0, "unmarked": 1.0, "total": 2.0}
