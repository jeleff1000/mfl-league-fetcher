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
