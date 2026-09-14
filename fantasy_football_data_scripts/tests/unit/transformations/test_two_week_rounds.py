import sys
from pathlib import Path

_root = Path(__file__).resolve().parents[3] / "multi_league"
sys.path.insert(0, str(_root.parent))
sys.path.insert(0, str(_root))

from transformations.matchup.modules.playoff_config import round_weeks


class TestRoundWeeks:
    def test_prt0_single_week_rounds(self):
        result = round_weeks(4, 14, 16, 0)
        assert result == [(14, 14), (15, 15)]

    def test_prt1_all_two_week(self):
        result = round_weeks(4, 14, 17, 1)
        assert result == [(14, 15), (16, 17)]

    def test_prt2_championship_only(self):
        result = round_weeks(4, 14, 16, 2)
        assert result == [(14, 14), (15, 16)]

    def test_6_team_prt2(self):
        result = round_weeks(6, 14, 17, 2)
        assert result == [(14, 14), (15, 15), (16, 17)]

    def test_end_week_clamp(self):
        result = round_weeks(4, 14, 15, 1)
        assert result[0] == (14, 15)
        assert result[1][1] <= 15


import numpy as np
from transformations.matchup.modules.bracket_simulation import generate_round_scores


class TestGenerateRoundScores:
    def test_prt0_single_draw(self):
        """prt=0: all rounds single-week, scores = single draw."""
        rng = np.random.default_rng(42)
        mu = np.array([100.0, 110.0])
        sigma = np.array([15.0, 15.0])
        round_ranges = [(14, 14), (15, 15)]
        scores, _ = generate_round_scores(
            mu,
            sigma,
            n_sims=1000,
            n_teams=2,
            max_rounds=2,
            playoff_round_type=0,
            round_actuals=None,
            mgr_to_idx={},
            rng=rng,
            round_week_ranges=round_ranges,
        )
        assert scores.shape == (1000, 2, 2)
        assert abs(scores[:, 0, 0].mean() - 100.0) < 5.0

    def test_prt1_two_draws_summed(self):
        """2-week rounds: combined score has ~2x mean and sqrt(2)*sigma std."""
        rng = np.random.default_rng(42)
        mu = np.array([100.0, 100.0])
        sigma = np.array([15.0, 15.0])
        round_ranges = [(14, 15), (16, 17)]
        scores, _ = generate_round_scores(
            mu,
            sigma,
            n_sims=10000,
            n_teams=2,
            max_rounds=2,
            playoff_round_type=0,
            round_actuals=None,
            mgr_to_idx={},
            rng=rng,
            round_week_ranges=round_ranges,
        )
        assert abs(scores[:, 0, 0].mean() - 200.0) < 5.0
        assert abs(scores[:, 0, 0].std() - 21.2) < 3.0

    def test_round_actuals_substitution(self):
        """Week 1 actuals replace first draw, keep second simulated."""
        rng = np.random.default_rng(42)
        mu = np.array([100.0, 100.0])
        sigma = np.array([15.0, 15.0])
        round_ranges = [(14, 15)]
        mgr_to_idx = {"team_a": 0, "team_b": 1}
        round_actuals = {(14, 15): {"team_a": 150.0, "team_b": 100.0}}
        scores, _ = generate_round_scores(
            mu,
            sigma,
            n_sims=10000,
            n_teams=2,
            max_rounds=1,
            playoff_round_type=0,
            round_actuals=round_actuals,
            mgr_to_idx=mgr_to_idx,
            rng=rng,
            round_week_ranges=round_ranges,
        )
        # Team A: 150 + sim(100,15) → mean ~250
        assert abs(scores[:, 0, 0].mean() - 250.0) < 5.0
        # Team B: 100 + sim(100,15) → mean ~200
        assert abs(scores[:, 1, 0].mean() - 200.0) < 5.0

    def test_single_week_round_ignores_actuals(self):
        """round_actuals for a 1-week round are ignored."""
        rng = np.random.default_rng(42)
        mu = np.array([100.0])
        sigma = np.array([15.0])
        round_ranges = [(14, 14)]
        mgr_to_idx = {"team_a": 0}
        round_actuals = {(14, 14): {"team_a": 200.0}}
        scores, _ = generate_round_scores(
            mu,
            sigma,
            n_sims=1000,
            n_teams=1,
            max_rounds=1,
            playoff_round_type=0,
            round_actuals=round_actuals,
            mgr_to_idx=mgr_to_idx,
            rng=rng,
            round_week_ranges=round_ranges,
        )
        assert abs(scores[:, 0, 0].mean() - 100.0) < 5.0


from transformations.matchup.modules.bracket_simulation import simulate_playoff_bracket_vectorized


class TestVectorizedSim2Week:
    def test_two_week_reduces_upset(self):
        """Favorite wins more often in 2-week round than 1-week."""
        teams = ["fav", "dog"]
        seeds = {"fav": 1, "dog": 2}
        mu = {"fav": 120.0, "dog": 100.0}
        sigma = {"fav": 15.0, "dog": 15.0}

        r1 = simulate_playoff_bracket_vectorized(
            teams,
            seeds,
            mu,
            sigma,
            n_sims=50000,
            rng_seed=42,
            playoff_round_type=0,
            round_week_ranges=[(14, 14)],
        )
        r2 = simulate_playoff_bracket_vectorized(
            teams,
            seeds,
            mu,
            sigma,
            n_sims=50000,
            rng_seed=42,
            playoff_round_type=0,
            round_week_ranges=[(14, 15)],
        )
        assert r2["p_champ"]["fav"] > r1["p_champ"]["fav"]

    def test_round_actuals_with_lead(self):
        """50-point lead should produce >90% win probability."""
        teams = ["leader", "trailer"]
        seeds = {"leader": 1, "trailer": 2}
        mu = {"leader": 100.0, "trailer": 100.0}
        sigma = {"leader": 15.0, "trailer": 15.0}

        result = simulate_playoff_bracket_vectorized(
            teams,
            seeds,
            mu,
            sigma,
            n_sims=50000,
            rng_seed=42,
            playoff_round_type=0,
            round_week_ranges=[(14, 15)],
            round_actuals={(14, 15): {"leader": 150.0, "trailer": 100.0}},
        )
        assert result["p_champ"]["leader"] > 90.0

    def test_deterministic_tie_higher_seed_wins(self):
        """Ties resolved by higher seed, not random."""
        teams = ["seed1", "seed2"]
        seeds = {"seed1": 1, "seed2": 2}
        mu = {"seed1": 100.0, "seed2": 100.0}
        sigma = {"seed1": 0.0, "seed2": 0.0}  # zero variance = always tie

        result = simulate_playoff_bracket_vectorized(
            teams,
            seeds,
            mu,
            sigma,
            n_sims=1000,
            rng_seed=42,
            playoff_round_type=0,
            round_week_ranges=[(14, 14)],
        )
        assert result["p_champ"]["seed1"] == 100.0


class TestReseedingSim2Week:
    def test_reseeding_two_week_reduces_upset(self):
        """Reseeding bracket: 2-week round reduces upset probability."""
        teams = ["s1", "s2", "s3", "s4"]
        seeds = {"s1": 1, "s2": 2, "s3": 3, "s4": 4}
        mu = {"s1": 120.0, "s2": 110.0, "s3": 100.0, "s4": 90.0}
        sigma = {t: 15.0 for t in teams}

        r1 = simulate_playoff_bracket_vectorized(
            teams,
            seeds,
            mu,
            sigma,
            n_sims=50000,
            rng_seed=42,
            uses_reseeding=True,
            playoff_round_type=0,
            round_week_ranges=[(14, 14), (15, 15)],
        )
        r2 = simulate_playoff_bracket_vectorized(
            teams,
            seeds,
            mu,
            sigma,
            n_sims=50000,
            rng_seed=42,
            uses_reseeding=True,
            playoff_round_type=0,
            round_week_ranges=[(14, 15), (16, 17)],
        )
        assert r2["p_champ"]["s1"] > r1["p_champ"]["s1"]

    def test_reseeding_deterministic_tie(self):
        """Reseeding bracket: ties resolved by higher seed."""
        teams = ["s1", "s2"]
        seeds = {"s1": 1, "s2": 2}
        mu = {"s1": 100.0, "s2": 100.0}
        sigma = {"s1": 0.0, "s2": 0.0}

        result = simulate_playoff_bracket_vectorized(
            teams,
            seeds,
            mu,
            sigma,
            n_sims=1000,
            rng_seed=42,
            uses_reseeding=True,
            playoff_round_type=0,
            round_week_ranges=[(14, 14)],
        )
        assert result["p_champ"]["s1"] == 100.0


import pandas as pd


class TestBracketPlayoffMatchups:
    def test_week1_loser_stays_alive_in_2_week_round(self):
        """Team that loses week 1 of 2-week round is still alive."""
        from transformations.matchup.playoff_odds_import import get_actual_playoff_matchups

        bracket_df = pd.DataFrame(
            [
                {
                    "week": 15,
                    "franchise_id": "A",
                    "opponent_franchise_id": "B",
                    "team_points": 130.0,
                    "win": 1,
                    "loss": 0,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
                {
                    "week": 15,
                    "franchise_id": "B",
                    "opponent_franchise_id": "A",
                    "team_points": 110.0,
                    "win": 0,
                    "loss": 1,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
            ]
        )

        result = get_actual_playoff_matchups(
            bracket_df,
            week=15,
            is_championship_bracket=True,
            playoff_qualifiers={"A", "B"},
            playoff_round_type=2,
            playoff_start_week=15,
            end_week=16,
            num_playoff_teams=2,
        )

        # Both teams should still be alive
        assert "A" in result["alive"]
        assert "B" in result["alive"]
        assert 15 not in result["completed_weeks"]

    def test_week2_combined_score_determines_winner(self):
        """After both weeks, combined score determines winner."""
        from transformations.matchup.playoff_odds_import import get_actual_playoff_matchups

        bracket_df = pd.DataFrame(
            [
                # Week 15: A wins week 1
                {
                    "week": 15,
                    "franchise_id": "A",
                    "opponent_franchise_id": "B",
                    "team_points": 130.0,
                    "win": 1,
                    "loss": 0,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
                {
                    "week": 15,
                    "franchise_id": "B",
                    "opponent_franchise_id": "A",
                    "team_points": 110.0,
                    "win": 0,
                    "loss": 1,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
                # Week 16: B wins week 2 with bigger margin → B wins combined
                {
                    "week": 16,
                    "franchise_id": "A",
                    "opponent_franchise_id": "B",
                    "team_points": 90.0,
                    "win": 0,
                    "loss": 1,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
                {
                    "week": 16,
                    "franchise_id": "B",
                    "opponent_franchise_id": "A",
                    "team_points": 140.0,
                    "win": 1,
                    "loss": 0,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
            ]
        )

        result = get_actual_playoff_matchups(
            bracket_df,
            week=16,
            is_championship_bracket=True,
            playoff_qualifiers={"A", "B"},
            playoff_round_type=2,
            playoff_start_week=15,
            end_week=16,
            num_playoff_teams=2,
        )

        # A: 130+90=220, B: 110+140=250 → B wins
        assert "B" in result["alive"]
        assert "A" not in result["alive"]

    def test_prt0_unchanged_behavior(self):
        """playoff_round_type=0: behavior unchanged."""
        from transformations.matchup.playoff_odds_import import get_actual_playoff_matchups

        bracket_df = pd.DataFrame(
            [
                {
                    "week": 15,
                    "franchise_id": "A",
                    "opponent_franchise_id": "B",
                    "team_points": 130.0,
                    "win": 1,
                    "loss": 0,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
                {
                    "week": 15,
                    "franchise_id": "B",
                    "opponent_franchise_id": "A",
                    "team_points": 110.0,
                    "win": 0,
                    "loss": 1,
                    "is_playoffs": 1,
                    "is_consolation": 0,
                },
            ]
        )

        result = get_actual_playoff_matchups(
            bracket_df,
            week=15,
            is_championship_bracket=True,
            playoff_qualifiers={"A", "B"},
            playoff_round_type=0,
        )

        # A wins, B is eliminated (normal 1-week behavior)
        assert "A" in result["alive"]
        assert "B" not in result["alive"]


class TestHoldoverTwoWeek:
    def test_no_elimination_after_week1_loss(self):
        """Team losing week 1 of 2-week round is NOT eliminated."""
        from transformations.matchup.modules.playoff_holdover import hold_playoff_values_for_eliminated

        _base = {
            "tie": 0,
            "champion": 0,
            "is_champion": 0,
            "p_playoffs": 100.0,
            "p_bye": 0.0,
            "p_semis": 100.0,
            "p_final": 50.0,
            "exp_final_wins": 0.0,
            "exp_final_pf": 0.0,
            "avg_seed": 0.0,
            "opponent_points": 0.0,
            "wins_to_date": 0,
            "losses_to_date": 0,
            "ties_to_date": 0,
            "final_playoff_seed": 1,
        }
        df = pd.DataFrame(
            [
                {
                    **_base,
                    "year": 2024,
                    "week": 14,
                    "franchise_id": "A",
                    "manager": "A",
                    "is_playoffs": 0,
                    "is_consolation": 0,
                    "is_bye_week": 0,
                    "team_points": 100,
                    "opponent": "C",
                    "opponent_franchise_id": "C",
                    "win": 1,
                    "loss": 0,
                    "p_champ": 60.0,
                    "power_rating": 1.5,
                },
                {
                    **_base,
                    "year": 2024,
                    "week": 14,
                    "franchise_id": "B",
                    "manager": "B",
                    "is_playoffs": 0,
                    "is_consolation": 0,
                    "is_bye_week": 0,
                    "team_points": 95,
                    "opponent": "D",
                    "opponent_franchise_id": "D",
                    "win": 1,
                    "loss": 0,
                    "p_champ": 40.0,
                    "power_rating": 1.3,
                },
                {
                    **_base,
                    "year": 2024,
                    "week": 15,
                    "franchise_id": "A",
                    "manager": "A",
                    "is_playoffs": 1,
                    "is_consolation": 0,
                    "is_bye_week": 0,
                    "team_points": 130,
                    "opponent": "B",
                    "opponent_franchise_id": "B",
                    "win": 1,
                    "loss": 0,
                    "p_champ": 75.0,
                    "power_rating": 1.6,
                },
                {
                    **_base,
                    "year": 2024,
                    "week": 15,
                    "franchise_id": "B",
                    "manager": "B",
                    "is_playoffs": 1,
                    "is_consolation": 0,
                    "is_bye_week": 0,
                    "team_points": 110,
                    "opponent": "A",
                    "opponent_franchise_id": "A",
                    "win": 0,
                    "loss": 1,
                    "p_champ": 25.0,
                    "power_rating": 1.2,
                },
            ]
        )

        result = hold_playoff_values_for_eliminated(
            df,
            data_directory=".",
            playoff_round_type=2,
            playoff_start_week=15,
            end_week=16,
            num_playoff_teams=2,
        )

        b_week15 = result[(result["franchise_id"] == "B") & (result["week"] == 15)]
        assert b_week15.iloc[0]["p_champ"] > 0


class TestSimGameScore:
    def test_returns_single_score(self):
        """_sim_game_score returns a float score for one team."""
        from transformations.matchup.modules.team_model import _sim_game_score

        rng = np.random.default_rng(42)
        mu_hat = {"team_a": 100.0}
        sigma_hat = {"team_a": 15.0}
        score = _sim_game_score("team_a", rng, mu_hat, sigma_hat)
        assert isinstance(score, float)
        assert 30.0 < score < 200.0

    def test_uses_default_for_unknown_team(self):
        """Unknown team uses default mu=100, sigma=15."""
        from transformations.matchup.modules.team_model import _sim_game_score

        rng = np.random.default_rng(42)
        score = _sim_game_score("unknown", rng, {}, {})
        assert isinstance(score, float)
        assert 30.0 < score < 200.0
