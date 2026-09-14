import pandas as pd
import pytest

from weekly_rb_rank_slot_pooling import assign_rank_slots, rank_slot_spread


def test_rank_slots_are_recomputed_inside_each_cohort_and_week():
    atoms = pd.DataFrame([
        # The ordering changes between weeks and between cohorts: rank is a slot.
        {"cohort": "A", "week": 1, "pid": "x", "eligible": 10, "started": 9, "wins": 6, "losses": 3},
        {"cohort": "A", "week": 1, "pid": "y", "eligible": 10, "started": 8, "wins": 4, "losses": 4},
        {"cohort": "A", "week": 2, "pid": "x", "eligible": 10, "started": 7, "wins": 4, "losses": 3},
        {"cohort": "A", "week": 2, "pid": "y", "eligible": 10, "started": 9, "wins": 5, "losses": 4},
        {"cohort": "B", "week": 1, "pid": "x", "eligible": 10, "started": 6, "wins": 3, "losses": 3},
        {"cohort": "B", "week": 1, "pid": "y", "eligible": 10, "started": 7, "wins": 4, "losses": 3},
        {"cohort": "B", "week": 2, "pid": "x", "eligible": 10, "started": 9, "wins": 5, "losses": 4},
        {"cohort": "B", "week": 2, "pid": "y", "eligible": 10, "started": 8, "wins": 4, "losses": 4},
    ])

    ranked = assign_rank_slots(atoms, rank_by="start_pct", max_rank=2)

    got = ranked.set_index(["cohort", "week", "slot"])["pid"].to_dict()
    assert got[("A", 1, 1)] == "x"
    assert got[("A", 2, 1)] == "y"
    assert got[("B", 1, 1)] == "y"
    assert got[("B", 2, 1)] == "x"


def test_rank_slot_spread_is_computed_per_week_and_slot_across_levels():
    atoms = pd.DataFrame([
        {"cohort": "A", "week": 1, "pid": "x", "eligible": 10, "started": 9, "wins": 6, "losses": 3},
        {"cohort": "A", "week": 1, "pid": "y", "eligible": 10, "started": 8, "wins": 4, "losses": 4},
        {"cohort": "B", "week": 1, "pid": "x", "eligible": 10, "started": 7, "wins": 3, "losses": 4},
        {"cohort": "B", "week": 1, "pid": "y", "eligible": 10, "started": 6, "wins": 3, "losses": 3},
    ])
    ranked = assign_rank_slots(atoms, rank_by="start_pct", max_rank=2)

    spread = rank_slot_spread(ranked, cohort_col="cohort")
    rb1 = spread[(spread.week == 1) & (spread.slot == 1)].iloc[0]
    assert rb1.start_pct_spread == 20.0
    assert rb1.win_pct_spread == pytest.approx(23.8095238)
    assert rb1.expected_wl_spread == pytest.approx(0.4)


def test_null_cohort_levels_are_ranked_in_their_own_weekly_group():
    atoms = pd.DataFrame([
        {"cohort": None, "week": 1, "pid": "x", "eligible": 10, "started": 9, "wins": 5, "losses": 4},
        {"cohort": None, "week": 1, "pid": "y", "eligible": 10, "started": 8, "wins": 4, "losses": 4},
    ])

    ranked = assign_rank_slots(atoms, rank_by="start_pct", max_rank=2)

    assert ranked["slot"].tolist() == [1, 2]


def test_rank_slots_can_use_season_period_instead_of_week():
    atoms = pd.DataFrame([
        {"cohort": "A", "year": 2025, "pid": "x", "eligible": 10, "started": 9, "wins": 6, "losses": 3},
        {"cohort": "B", "year": 2025, "pid": "y", "eligible": 10, "started": 8, "wins": 4, "losses": 4},
    ])

    ranked = assign_rank_slots(atoms, rank_by="start_pct", period_cols=("year",))

    assert set(ranked["slot"]) == {1}
    assert set(ranked["year"]) == {2025}


def test_rank_slot_spread_can_return_named_period_column():
    ranked = pd.DataFrame([
        {"cohort": "A", "year": 2025, "slot": 1, "start_pct": 80, "win_pct": 60, "expected_wl": 0.16},
        {"cohort": "B", "year": 2025, "slot": 1, "start_pct": 70, "win_pct": 50, "expected_wl": 0.00},
    ])

    spread = rank_slot_spread(ranked, period_cols=("year",))

    assert list(spread["year"]) == [2025]
    assert spread.iloc[0].start_pct_spread == pytest.approx(10)
