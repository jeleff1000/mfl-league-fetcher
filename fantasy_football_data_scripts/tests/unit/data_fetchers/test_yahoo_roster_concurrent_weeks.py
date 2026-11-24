"""Live refreshes fetch Yahoo roster weeks concurrently through one rate limiter.

Regression: bohannon_s_league_ng full rebuild (runs 45433062679, 45433763568)
spent 76s on 11 sequential roster weeks after two Yahoo calls stalled 14s and
26s, and the 120s update deadline expired before publication.
"""

import threading
import time

import pandas as pd

from multi_league.data_fetchers.yahoo.yahoo_rosters import YahooRosterFetcher


def _fetcher(concurrent_weeks, stall_week=None, stall_seconds=0.0, fail_week=None):
    fetcher = YahooRosterFetcher.__new__(YahooRosterFetcher)
    fetcher.concurrent_weeks = concurrent_weeks
    fetcher.calls = []
    lock = threading.Lock()

    def fetch_all_rosters_for_week(year, week, teams):
        with lock:
            fetcher.calls.append(week)
        if week == stall_week:
            time.sleep(stall_seconds)
        if week == fail_week:
            raise RuntimeError(f"week {week} transport error")
        failures = [("t.2", teams["t.2"])] if week == 2 else []
        return pd.DataFrame([{"year": year, "week": week, "team_key": key} for key in teams]), failures

    fetcher.fetch_all_rosters_for_week = fetch_all_rosters_for_week
    return fetcher


TEAMS = {"t.1": {"manager_name": "A"}, "t.2": {"manager_name": "B"}}


def test_concurrent_weeks_match_sequential_rows_and_failures(monkeypatch):
    weeks = list(range(1, 12))
    seq = _fetcher(1)
    par = _fetcher(4)
    monkeypatch.setattr("multi_league.data_fetchers.yahoo.yahoo_rosters.time.sleep", lambda _s: None)
    seq_df = seq._fetch_all_weeks_batch(2026, weeks, TEAMS)
    par_df = par._fetch_all_weeks_batch(2026, weeks, TEAMS)

    pd.testing.assert_frame_equal(seq_df.reset_index(drop=True), par_df.reset_index(drop=True))
    assert seq._failed_team_weeks == par._failed_team_weeks == {2: [("t.2", TEAMS["t.2"])]}
    assert sorted(par.calls) == weeks  # same requests, no extras


def test_one_stalled_week_does_not_serialize_the_rest():
    weeks = list(range(1, 9))
    par = _fetcher(4, stall_week=1, stall_seconds=0.6)

    def slow(year, week, teams, _inner=par.fetch_all_rosters_for_week):
        time.sleep(0.15)
        return _inner(year, week, teams)

    par.fetch_all_rosters_for_week = slow
    started = time.monotonic()
    df = par._fetch_all_weeks_batch(2026, weeks, TEAMS)
    elapsed = time.monotonic() - started
    assert sorted(df["week"].unique().tolist()) == weeks
    # Sequential would be 8*0.15 + 0.6 + 7*0.5 sleeps = 5.3s.
    assert elapsed < 1.5


def test_concurrent_week_error_is_reported_in_week_order(monkeypatch):
    weeks = [1, 2, 3, 4]
    par = _fetcher(4, fail_week=3)
    monkeypatch.setattr("multi_league.data_fetchers.yahoo.yahoo_rosters.time.sleep", lambda _s: None)
    df = par._fetch_all_weeks_batch(2026, weeks, TEAMS)
    assert sorted(df["week"].unique().tolist()) == [1, 2, 4]


def test_imports_stay_sequential_by_default():
    import inspect

    from multi_league.data_fetchers.yahoo.yahoo_rosters import fetch_rosters_for_year

    assert inspect.signature(fetch_rosters_for_year).parameters["concurrent_weeks"].default == 1
