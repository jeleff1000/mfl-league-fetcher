"""Unassigned future opponents must not disappear from season projections."""

import numpy as np
import pandas as pd
import pytest

from multi_league.transformations.matchup import playoff_odds_import as poi


def _schedule():
    return pd.DataFrame([
        {"year": 2026, "week": week, "manager": f"Manager {fid}",
         "franchise_id": fid, "opponent": "TBD" if opponent is None else f"Manager {opponent}",
         "opponent_franchise_id": opponent, "is_playoffs": 0}
        for week, pairs in [(2, [("a", "b"), ("b", "a"), ("c", "d"), ("d", "c")]),
                            (3, [(fid, None) for fid in "abcd"])]
        for fid, opponent in pairs
    ])


def test_future_schedule_retains_each_unassigned_franchise_without_fake_opponents():
    schedule = _schedule()
    result = poi.future_regular_from_schedule(schedule, 2026, 1)
    assert len(result) == 6  # Two known pairs, four unassigned team slots.
    unknown = result.loc[result.week.eq(3)]
    assert unknown.franchise_id.tolist() == list("abcd")
    assert unknown.opponent_franchise_id.isna().all()
    pd.testing.assert_frame_equal(schedule, _schedule())


def test_trial_pairings_cover_unknown_teams_once_and_keep_known_pairs():
    future = poi.future_regular_from_schedule(_schedule(), 2026, 1)
    a, b, weeks = poi._future_schedule_indices(
        future, manager_to_index={fid: index for index, fid in enumerate("abcd")},
        rng=np.random.default_rng(42), n_sims=256,
    )
    assert weeks.tolist() == [2, 2, 3, 3]
    assert a.shape == b.shape == (256, 4)
    assert (a[:, :2] == [0, 2]).all()
    assert (b[:, :2] == [1, 3]).all()
    assert (a != b).all()
    assert (np.sort(np.concatenate([a[:, 2:], b[:, 2:]], axis=1), axis=1) == [0, 1, 2, 3]).all()
    assert len({tuple(row) for row in np.concatenate([a[:, 2:], b[:, 2:]], axis=1)}) > 10


@pytest.mark.parametrize("median", [False, True])
def test_unassigned_week_contributes_to_full_record_and_points_projections(monkeypatch, median):
    monkeypatch.setattr(poi, "N_SIMS", 256)
    played = pd.DataFrame([
        {"year": 2026, "week": 1, "manager": fid, "opponent": opponent,
         "franchise_id": fid, "opponent_franchise_id": opponent,
         "team_points": points, "opponent_points": opponent_points,
         "win": win, "loss": 1-win, "above_league_median": win}
        for fid, opponent, points, opponent_points, win in [
            ("a", "b", 110, 90, 1), ("b", "a", 90, 110, 0),
            ("c", "d", 120, 80, 1), ("d", "c", 80, 120, 0)]
    ])
    future = poi.future_regular_from_schedule(_schedule(), 2026, 1)
    pack, seeds, records = poi._vectorized_regular_and_bracket(
        played, future, {fid: 100 for fid in "abcd"}, {fid: 10 for fid in "abcd"},
        2026, 1, playoff_slots=2, bye_slots=0, regular_season_weeks=3,
        num_teams=4, use_median=median,
    )
    assert pack["Exp_Final_Wins"].sum() == pytest.approx(12 if median else 6)
    assert pack["Exp_Final_PF"].sum() == pytest.approx(1200, abs=15)
    assert (seeds.sum(axis=1) == 100).all()
    assert np.allclose(records.sum(axis=1), 100)
    assert pack["P_Champ"].sum() == pytest.approx(100)


def test_assigned_schedule_keeps_one_dimensional_fast_path():
    future = poi.future_regular_from_schedule(_schedule().query("week == 2"), 2026, 1)
    a, b, weeks = poi._future_schedule_indices(
        future, manager_to_index={fid: index for index, fid in enumerate("abcd")},
        rng=np.random.default_rng(42), n_sims=256,
    )
    assert a.tolist() == [0, 2]
    assert b.tolist() == [1, 3]
    assert weeks.tolist() == [2, 2]


def test_unassigned_opponents_do_not_become_artificial_team_models():
    from multi_league.transformations.matchup.modules.team_model import ensure_params_for_future

    means = {fid: 100 for fid in "abcd"}
    deviations = {fid: 10 for fid in "abcd"}
    samples = {}
    future = poi.future_regular_from_schedule(_schedule(), 2026, 1)
    ensure_params_for_future(means, deviations, samples, future, 100, 10)
    assert set(means) == set(deviations) == set(samples) == set("abcd")


def test_partial_unassigned_week_preserves_its_assigned_pair_in_every_trial():
    schedule = _schedule()
    schedule.loc[schedule.week.eq(3) & schedule.franchise_id.eq("a"), ["opponent", "opponent_franchise_id"]] = ["Manager b", "b"]
    schedule.loc[schedule.week.eq(3) & schedule.franchise_id.eq("b"), ["opponent", "opponent_franchise_id"]] = ["Manager a", "a"]
    future = poi.future_regular_from_schedule(schedule, 2026, 1)
    a, b, weeks = poi._future_schedule_indices(
        future, manager_to_index={fid: index for index, fid in enumerate("abcd")},
        rng=np.random.default_rng(42), n_sims=256,
    )
    assert weeks.tolist() == [2, 2, 3, 3]
    assert (a[:, 2] == 0).all() and (b[:, 2] == 1).all()
    assert (np.sort(np.stack([a[:, 3], b[:, 3]], axis=1), axis=1) == [2, 3]).all()
    again = poi._future_schedule_indices(
        future, manager_to_index={fid: index for index, fid in enumerate("abcd")},
        rng=np.random.default_rng(42), n_sims=256,
    )
    for actual, expected in zip(again, (a, b, weeks), strict=True):
        assert np.array_equal(actual, expected)
