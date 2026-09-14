import pandas as pd
import pytest

from run_position_rank_slot_pooling import _period_atoms, build_atoms, summarize_curves


def test_season_period_aggregates_weekly_counts_and_retains_year():
    atoms = pd.DataFrame([
        {"lane": "flx", "pos_slots": "10t", "flx_slots": "flx2", "ppr": "ppr", "td": "4pt", "bracket": "4po", "pid": "p1", "week": 1, "eligible": 2, "started": 1, "wins": 1, "losses": 0},
        {"lane": "flx", "pos_slots": "10t", "flx_slots": "flx2", "ppr": "ppr", "td": "4pt", "bracket": "4po", "pid": "p1", "week": 2, "eligible": 2, "started": 2, "wins": 0, "losses": 2},
    ])

    season, periods = _period_atoms(atoms, "season")

    assert periods == ("year",)
    row = season.iloc[0]
    assert row.year == 2025
    assert (row.eligible, row.started, row.wins, row.losses) == (4, 3, 1, 2)


def test_period_atoms_rejects_unknown_grain():
    with pytest.raises(ValueError, match="unsupported grain"):
        _period_atoms(pd.DataFrame(), "monthly")


def test_summary_averages_rank_curve_over_slots_but_keeps_lane_and_dimension():
    curves = pd.DataFrame([
        {"position": "WR", "grain": "weekly", "rank_by": "start_pct", "lane": "flx", "dimension": "td", "slot": 1, "start_pct_spread": 2, "win_pct_spread": 4, "expected_wl_spread": .04},
        {"position": "WR", "grain": "weekly", "rank_by": "start_pct", "lane": "flx", "dimension": "td", "slot": 2, "start_pct_spread": 4, "win_pct_spread": 6, "expected_wl_spread": .06},
    ])

    summary = summarize_curves(curves)

    assert len(summary) == 1
    row = summary.iloc[0]
    assert row.start_pct_spread == pytest.approx(3)
    assert row.win_pct_spread == pytest.approx(5)
    assert row.expected_wl_spread == pytest.approx(.05)
