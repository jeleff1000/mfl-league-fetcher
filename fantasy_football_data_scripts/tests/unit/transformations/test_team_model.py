"""Tests for team performance modeling module."""

import pytest
import numpy as np
import pandas as pd
import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.transformations.matchup.modules.team_model import (
    safe_mean,
    safe_get,
    safe_col_get,
    compress_early_season,
    recency_weights,
    build_team_models,
    compute_power_ratings,
    index_power_rating,
    draw_score,
    normalize_power_rating,
    calculate_model_confidence,
    ensure_params_for_future,
    _sim_game,
    HALF_LIFE_WEEKS,
    SHRINK_K,
)


@pytest.fixture
def simple_matchup_df():
    """6 weeks of matchup data for 4 managers."""
    rng = np.random.default_rng(42)
    rows = []
    managers = ["Alice", "Bob", "Carol", "Dave"]
    for week in range(1, 7):
        for i in range(0, len(managers), 2):
            m, o = managers[i], managers[i + 1]
            rows.append(
                {
                    "year": 2024,
                    "week": week,
                    "manager": m,
                    "franchise_id": m,
                    "opponent": o,
                    "opponent_franchise_id": o,
                    "team_points": rng.normal(110, 15),
                }
            )
            rows.append(
                {
                    "year": 2024,
                    "week": week,
                    "manager": o,
                    "franchise_id": o,
                    "opponent": m,
                    "opponent_franchise_id": m,
                    "team_points": rng.normal(110, 15),
                }
            )
    return pd.DataFrame(rows)


@pytest.fixture
def multi_season_df(simple_matchup_df):
    """Two seasons of matchup data."""
    s1 = simple_matchup_df.copy()
    s2 = simple_matchup_df.copy()
    s2["year"] = 2023
    return pd.concat([s1, s2], ignore_index=True)


class TestSafeMean:
    def test_normal(self):
        assert safe_mean([1, 2, 3]) == 2.0

    def test_empty(self):
        assert safe_mean([]) == 100.0

    def test_all_nan(self):
        assert safe_mean([np.nan, np.nan]) == 100.0

    def test_custom_default(self):
        assert safe_mean([], default=0.0) == 0.0


class TestSafeColGet:
    def test_column_exists(self):
        df = pd.DataFrame({"a": [10.0, 20.0]})
        assert safe_col_get(df, 0, "a") == 10.0
        assert safe_col_get(df, 1, "a") == 20.0

    def test_column_missing_returns_default(self):
        df = pd.DataFrame({"a": [10.0]})
        assert safe_col_get(df, 0, "b") is None
        assert safe_col_get(df, 0, "b", default=99.0) == 99.0

    def test_value_nan_returns_default(self):
        df = pd.DataFrame({"a": [np.nan, 5.0]})
        assert safe_col_get(df, 0, "a", default=-1.0) == -1.0
        assert safe_col_get(df, 1, "a", default=-1.0) == 5.0

    def test_bad_index_returns_default(self):
        df = pd.DataFrame({"a": [10.0]})
        assert safe_col_get(df, 99, "a", default=0.0) == 0.0


class TestSafeGet:
    def test_key_exists(self):
        assert safe_get({"a": 5.0}, "a") == 5.0

    def test_key_missing_uses_fallback_mean(self):
        result = safe_get({}, "a", fallback_dict={"b": 10.0, "c": 20.0})
        assert result == 15.0

    def test_key_nan_uses_fallback(self):
        result = safe_get({"a": np.nan}, "a", fallback_dict={"b": 10.0})
        assert result == 10.0

    def test_all_missing(self):
        assert safe_get({}, "a", default=42.0) == 42.0


class TestCompressEarlySeason:
    def test_week5_no_compression(self):
        assert compress_early_season(90.0, 5) == 90.0

    def test_week1_compresses_toward_base(self):
        result = compress_early_season(90.0, 1)
        assert 60 < result < 80

    def test_week0_fully_base(self):
        assert compress_early_season(90.0, 0) == 50.0

    def test_custom_base_rate(self):
        result = compress_early_season(90.0, 0, base_rate=30.0)
        assert result == 30.0


class TestRecencyWeights:
    def test_current_season_weight_1(self, simple_matchup_df):
        w = recency_weights(simple_matchup_df, season=2024, week=6, half_life=10)
        mask = (simple_matchup_df["year"] == 2024) & (simple_matchup_df["week"] == 6)
        assert np.allclose(w[mask], 1.0)

    def test_prior_season_penalized(self, multi_season_df):
        w = recency_weights(multi_season_df, season=2024, week=1, half_life=10)
        current = w[multi_season_df["year"] == 2024].mean()
        prior = w[multi_season_df["year"] == 2023].mean()
        assert prior < current

    def test_older_weeks_decay(self, simple_matchup_df):
        w = recency_weights(simple_matchup_df, season=2024, week=6, half_life=10)
        w1 = w[simple_matchup_df["week"] == 1].mean()
        w5 = w[simple_matchup_df["week"] == 5].mean()
        assert w1 < w5


class TestBuildTeamModels:
    def test_returns_all_managers(self, simple_matchup_df):
        mu, sigma, samples, lmu, sf = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        managers = set(simple_matchup_df["manager"].unique())
        assert set(mu.keys()) == managers
        assert set(sigma.keys()) == managers

    def test_mu_reasonable_range(self, simple_matchup_df):
        mu, sigma, _, lmu, _ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        for m, val in mu.items():
            assert abs(val - lmu) < 3 * 15

    def test_sigma_above_floor(self, simple_matchup_df):
        mu, sigma, _, _, sf = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        for m, val in sigma.items():
            assert val >= sf - 0.01

    def test_early_week_more_shrinkage(self, simple_matchup_df):
        mu_w1, *_ = build_team_models(simple_matchup_df, 2024, 1, HALF_LIFE_WEEKS, SHRINK_K)
        mu_w6, *_ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        spread_w1 = np.std(list(mu_w1.values()))
        spread_w6 = np.std(list(mu_w6.values()))
        assert spread_w1 <= spread_w6 + 1.0


class TestComputePowerRatings:
    def test_returns_series(self, simple_matchup_df):
        mu, sigma, samples, _, _ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        pr = compute_power_ratings(mu, samples)
        assert isinstance(pr, pd.Series)
        assert len(pr) == 4


class TestDrawScore:
    def test_returns_float(self, simple_matchup_df):
        mu, sigma, samples, _, _ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        rng = np.random.default_rng(42)
        score = draw_score("Alice", rng, mu, sigma, samples)
        assert isinstance(score, float)

    def test_bootstrap_vs_gaussian(self, simple_matchup_df):
        mu, sigma, samples, _, _ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        rng = np.random.default_rng(42)
        score_boot = draw_score("Alice", rng, mu, sigma, samples, use_bootstrap=True)
        score_gauss = draw_score("Alice", rng, mu, sigma, {}, use_bootstrap=True)
        assert isinstance(score_boot, float)
        assert isinstance(score_gauss, float)


class TestNormalizePowerRating:
    def test_indexes_to_median_100(self):
        s = pd.Series([100.0, 200.0, 300.0])
        result = index_power_rating(s)
        assert list(result) == [50.0, 100.0, 150.0]

    def test_inflation_argument_is_ignored(self):
        s = pd.Series([100.0, 200.0, 300.0])
        result = normalize_power_rating(s, 2.0)
        assert list(result) == [50.0, 100.0, 150.0]

    def test_single_team_cohort_stays_100(self):
        s = pd.Series([100.0])
        result = normalize_power_rating(s)
        assert list(result) == [100.0]


class TestCalculateModelConfidence:
    def test_first_season(self, simple_matchup_df):
        level, n = calculate_model_confidence(simple_matchup_df, 2024)
        assert level == "first_season"
        assert n == 0

    def test_high_confidence(self, simple_matchup_df):
        frames = [simple_matchup_df.copy()]
        for yr in range(2017, 2024):
            f = simple_matchup_df.copy()
            f["year"] = yr
            frames.append(f)
        df = pd.concat(frames, ignore_index=True)
        level, n = calculate_model_confidence(df, 2024)
        assert level == "high"
        assert n == 7


class TestEnsureParamsForFuture:
    def test_adds_missing_managers(self):
        mu_hat = {"Alice": 110.0}
        sigma_hat = {"Alice": 12.0}
        samples = {"Alice": pd.DataFrame({"team_points": [100.0], "w": [1.0]})}
        df_future = pd.DataFrame(
            {
                "manager": ["Alice", "NewGuy"],
                "franchise_id": ["Alice", "NewGuy"],
                "opponent": ["NewGuy", "Alice"],
                "opponent_franchise_id": ["NewGuy", "Alice"],
            }
        )
        ensure_params_for_future(mu_hat, sigma_hat, samples, df_future, 105.0, 10.0)
        assert "NewGuy" in mu_hat
        assert mu_hat["NewGuy"] == 105.0
        assert sigma_hat["NewGuy"] == 10.0
        assert "NewGuy" in samples
        # Alice unchanged
        assert mu_hat["Alice"] == 110.0

    def test_noop_when_all_present(self):
        mu_hat = {"Alice": 110.0, "Bob": 108.0}
        sigma_hat = {"Alice": 12.0, "Bob": 11.0}
        samples = {
            "Alice": pd.DataFrame({"team_points": [100.0], "w": [1.0]}),
            "Bob": pd.DataFrame({"team_points": [95.0], "w": [1.0]}),
        }
        df_future = pd.DataFrame(
            {
                "manager": ["Alice"],
                "franchise_id": ["Alice"],
                "opponent": ["Bob"],
                "opponent_franchise_id": ["Bob"],
            }
        )
        ensure_params_for_future(mu_hat, sigma_hat, samples, df_future, 105.0, 10.0)
        assert mu_hat["Alice"] == 110.0
        assert mu_hat["Bob"] == 108.0

    def test_handles_empty_future_schedule(self):
        mu_hat = {"Alice": 110.0}
        sigma_hat = {"Alice": 12.0}
        samples = {}
        ensure_params_for_future(mu_hat, sigma_hat, samples, pd.DataFrame(), 105.0, 10.0)
        # No changes
        assert len(mu_hat) == 1

    def test_handles_none_future_schedule(self):
        mu_hat = {"Alice": 110.0}
        sigma_hat = {"Alice": 12.0}
        samples = {}
        ensure_params_for_future(mu_hat, sigma_hat, samples, None, 105.0, 10.0)
        assert len(mu_hat) == 1


class TestSimGame:
    def test_returns_winner_name(self, simple_matchup_df):
        mu, sigma, samples, _, _ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        rng = np.random.default_rng(42)
        winner = _sim_game("Alice", "Bob", rng, mu, sigma, samples)
        assert winner in ("Alice", "Bob")

    def test_seed_controlled_results(self, simple_matchup_df):
        mu, sigma, samples, _, _ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        results_a = [_sim_game("Alice", "Bob", np.random.default_rng(99), mu, sigma, samples) for _ in range(10)]
        results_b = [_sim_game("Alice", "Bob", np.random.default_rng(99), mu, sigma, samples) for _ in range(10)]
        assert results_a == results_b

    def test_both_players_can_win(self, simple_matchup_df):
        mu, sigma, samples, _, _ = build_team_models(simple_matchup_df, 2024, 6, HALF_LIFE_WEEKS, SHRINK_K)
        winners = set()
        for seed in range(200):
            rng = np.random.default_rng(seed)
            winners.add(_sim_game("Alice", "Bob", rng, mu, sigma, samples))
        assert "Alice" in winners
        assert "Bob" in winners
