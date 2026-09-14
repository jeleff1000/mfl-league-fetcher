"""
Unit tests for bracket_simulation.py module.

Tests cover:
- _get_bracket_side: Bracket side assignment for various playoff sizes
- calculate_effective_byes: Bye calculation logic for different round states
- apply_completed_round_overrides: Probability overrides for completed games
- build_playoff_week_mask: Mask creation for playoff rows including bye teams
- simulate_playoff_bracket_vectorized: Monte Carlo simulation correctness
"""

import pytest
import numpy as np
import pandas as pd

from bracket_simulation import (
    _get_bracket_side,
    calculate_effective_byes,
    apply_completed_round_overrides,
    build_playoff_week_mask,
    simulate_playoff_bracket_vectorized,
)


# =============================================================================
# Tests for _get_bracket_side
# =============================================================================


class TestGetBracketSide:
    """Tests for bracket side assignment based on seed and playoff size."""

    def test_4_team_bracket(self):
        """4-team bracket: Side A = [1,4], Side B = [2,3]"""
        assert _get_bracket_side(1, 4) == "A"
        assert _get_bracket_side(2, 4) == "B"
        assert _get_bracket_side(3, 4) == "B"
        assert _get_bracket_side(4, 4) == "A"

    def test_6_team_bracket(self):
        """6-team bracket: Side A = [1,4,5], Side B = [2,3,6]"""
        assert _get_bracket_side(1, 6) == "A"
        assert _get_bracket_side(2, 6) == "B"
        assert _get_bracket_side(3, 6) == "B"
        assert _get_bracket_side(4, 6) == "A"
        assert _get_bracket_side(5, 6) == "A"
        assert _get_bracket_side(6, 6) == "B"

    def test_8_team_bracket(self):
        """8-team bracket: Side A = [1,4,5,8], Side B = [2,3,6,7]"""
        assert _get_bracket_side(1, 8) == "A"
        assert _get_bracket_side(2, 8) == "B"
        assert _get_bracket_side(3, 8) == "B"
        assert _get_bracket_side(4, 8) == "A"
        assert _get_bracket_side(5, 8) == "A"
        assert _get_bracket_side(6, 8) == "B"
        assert _get_bracket_side(7, 8) == "B"
        assert _get_bracket_side(8, 8) == "A"

    def test_accepts_integer_valued_float_seed(self):
        """Integer-valued float seeds from pandas should be accepted."""
        assert _get_bracket_side(2.0, 6) == "B"

    def test_bracket_sides_balanced(self):
        """Each bracket size should have equal teams on each side."""
        for num_teams in [4, 6, 8]:
            side_a = sum(1 for s in range(1, num_teams + 1) if _get_bracket_side(s, num_teams) == "A")
            side_b = sum(1 for s in range(1, num_teams + 1) if _get_bracket_side(s, num_teams) == "B")
            assert side_a == side_b, f"Unbalanced bracket for {num_teams} teams: A={side_a}, B={side_b}"

    def test_top_seeds_on_opposite_sides(self):
        """Seeds 1 and 2 should always be on opposite sides (meet only in finals)."""
        for num_teams in [4, 6, 8]:
            assert _get_bracket_side(1, num_teams) != _get_bracket_side(
                2, num_teams
            ), f"Seeds 1 and 2 on same side for {num_teams}-team bracket"


# =============================================================================
# Tests for calculate_effective_byes
# =============================================================================


class TestCalculateEffectiveByes:
    """Tests for effective bye calculation based on round state."""

    def test_wild_card_not_complete(self):
        """During wild card round, byes should be active."""
        assert calculate_effective_byes(0, 2, False) == 2
        assert calculate_effective_byes(0, 1, False) == 1
        assert calculate_effective_byes(0, 0, False) == 0

    def test_wild_card_complete(self):
        """After wild card complete, effective byes = 0."""
        assert calculate_effective_byes(0, 2, True) == 0
        assert calculate_effective_byes(0, 1, True) == 0

    def test_later_rounds(self):
        """In rounds after wild card, byes should always be 0."""
        # Round 1 (semis)
        assert calculate_effective_byes(1, 2, False) == 0
        assert calculate_effective_byes(1, 2, True) == 0
        # Round 2 (finals)
        assert calculate_effective_byes(2, 2, False) == 0
        assert calculate_effective_byes(2, 2, True) == 0

    def test_no_byes_setting(self):
        """When bye_teams=0, should always return 0."""
        assert calculate_effective_byes(0, 0, False) == 0
        assert calculate_effective_byes(0, 0, True) == 0
        assert calculate_effective_byes(1, 0, False) == 0


# =============================================================================
# Tests for apply_completed_round_overrides
# =============================================================================


class TestApplyCompletedRoundOverrides:
    """Tests for probability overrides when rounds are complete."""

    def _make_odds_df(self, managers: list[str]) -> pd.DataFrame:
        """Create a test odds DataFrame."""
        return pd.DataFrame(
            {
                "P_Semis": [50.0] * len(managers),
                "P_Final": [25.0] * len(managers),
                "P_Champ": [10.0] * len(managers),
            },
            index=managers,
        )

    def test_wild_card_complete_sets_semis(self):
        """When wild card complete, surviving teams get P_Semis=100%."""
        managers = ["A", "B", "C", "D", "E", "F"]
        odds = self._make_odds_df(managers)
        teams_alive = ["A", "B", "C", "D"]  # E, F eliminated
        playoff_teams = managers

        result = apply_completed_round_overrides(
            odds=odds,
            teams_alive=teams_alive,
            playoff_teams=playoff_teams,
            bracket_info=None,
            current_playoff_round=0,
            games_this_round_complete=True,
        )

        # Survivors should have P_Semis = 100%
        for m in teams_alive:
            assert result.at[m, "P_Semis"] == 100.0, f"{m} should have P_Semis=100%"

        # Eliminated should have P_Semis = 0%
        for m in ["E", "F"]:
            assert result.at[m, "P_Semis"] == 0.0, f"{m} should have P_Semis=0%"

    def test_championship_locked_two_teams(self):
        """When 2 teams remain, both get P_Final=100%."""
        managers = ["A", "B", "C", "D"]
        odds = self._make_odds_df(managers)
        teams_alive = ["A", "B"]  # Finalists
        playoff_teams = managers

        result = apply_completed_round_overrides(
            odds=odds,
            teams_alive=teams_alive,
            playoff_teams=playoff_teams,
            bracket_info=None,
            current_playoff_round=1,
            games_this_round_complete=False,
        )

        # Finalists are deterministically locked into the final.
        assert result.at["A", "P_Final"] == 100.0
        assert result.at["B", "P_Final"] == 100.0

        # Non-finalists are left to the simulated values (the sim, run with only the
        # 2 alive teams, already gives them P_Final≈0); the override doesn't touch them.
        assert result.at["C", "P_Final"] == 25.0
        assert result.at["D", "P_Final"] == 25.0

    def test_championship_complete_preserves_pregame_champ_odds(self):
        """During the championship week, P_Champ is intentionally NOT overridden.

        Pre-game simulated odds are preserved so clutch-equity ("odds at the time")
        is computed correctly; the holdover logic sets deterministic P_Champ for the
        weeks AFTER the title game. The finals lock (P_Final=100 for the 2 alive
        teams) is still applied.
        """
        managers = ["A", "B", "C", "D"]
        odds = self._make_odds_df(managers)
        teams_alive = ["A", "B"]  # Finalists
        playoff_teams = managers
        bracket_info = {"games": [("A", "B", "A")]}  # A beat B

        result = apply_completed_round_overrides(
            odds=odds,
            teams_alive=teams_alive,
            playoff_teams=playoff_teams,
            bracket_info=bracket_info,
            current_playoff_round=2,
            games_this_round_complete=True,
        )

        # Finalists are locked into the final.
        assert result.at["A", "P_Final"] == 100.0
        assert result.at["B", "P_Final"] == 100.0
        # P_Champ is preserved (pre-game simulated value), not forced to 100/0.
        assert result.at["A", "P_Champ"] == 10.0
        assert result.at["B", "P_Champ"] == 10.0

    def test_single_team_remaining_preserves_champ_odds(self):
        """With 1 team left, P_Champ is left to the holdover logic, not forced here."""
        managers = ["A", "B", "C", "D"]
        odds = self._make_odds_df(managers)
        teams_alive = ["A"]  # Champion
        playoff_teams = managers

        result = apply_completed_round_overrides(
            odds=odds,
            teams_alive=teams_alive,
            playoff_teams=playoff_teams,
            bracket_info=None,
            current_playoff_round=2,
            games_this_round_complete=True,
        )

        # Override does not mutate P_Champ here (preserved for clutch equity).
        assert result.at["A", "P_Champ"] == 10.0


# =============================================================================
# Tests for build_playoff_week_mask
# =============================================================================


class TestBuildPlayoffWeekMask:
    """Tests for playoff week mask including bye teams."""

    def _make_test_df(self) -> pd.DataFrame:
        """Create a test DataFrame with playoff and bye team rows.

        Byes are identified by the authoritative `is_bye_week` flag plus the stable
        `franchise_id` identity column (not a manager-name / is_playoffs heuristic).
        """
        return pd.DataFrame(
            {
                "year": [2024, 2024, 2024, 2024, 2024, 2024],
                "week": [15, 15, 15, 15, 15, 15],
                "manager": ["A", "B", "C", "D", "E", "F"],
                "franchise_id": ["A", "B", "C", "D", "E", "F"],
                "is_playoffs": [1, 1, 1, 1, np.nan, np.nan],  # E, F are bye teams
                "is_consolation": [0, 0, 0, 0, np.nan, np.nan],
                "is_bye_week": [0, 0, 0, 0, 1, 1],  # E, F on bye
            }
        )

    def test_includes_normal_playoff_rows(self):
        """Mask should include rows where is_playoffs=1."""
        df = self._make_test_df()
        playoff_team_set = {"A", "B", "C", "D", "E", "F"}

        mask = build_playoff_week_mask(df, 2024, 15, playoff_team_set)

        # Normal playoff rows (A, B, C, D) should be included
        assert mask.iloc[0] == True  # A
        assert mask.iloc[1] == True  # B
        assert mask.iloc[2] == True  # C
        assert mask.iloc[3] == True  # D

    def test_includes_bye_team_rows(self):
        """Mask should include bye team rows (is_playoffs=NaN, manager in playoff set)."""
        df = self._make_test_df()
        playoff_team_set = {"A", "B", "C", "D", "E", "F"}

        mask = build_playoff_week_mask(df, 2024, 15, playoff_team_set)

        # Bye team rows (E, F) should be included
        assert mask.iloc[4] == True  # E (bye)
        assert mask.iloc[5] == True  # F (bye)

    def test_excludes_non_playoff_managers(self):
        """A bye row whose franchise is not in the playoff set is excluded."""
        df = self._make_test_df()
        df.loc[5, "franchise_id"] = "X"  # F's bye row now belongs to non-playoff franchise X
        playoff_team_set = {"A", "B", "C", "D", "E", "F"}  # X not in set

        mask = build_playoff_week_mask(df, 2024, 15, playoff_team_set)

        # X should be excluded (franchise_id not in playoff_team_set)
        assert mask.iloc[5] == False

    def test_excludes_wrong_year(self):
        """Mask should exclude rows from different years."""
        df = self._make_test_df()
        df.loc[0, "year"] = 2023  # Change A to different year
        playoff_team_set = {"A", "B", "C", "D", "E", "F"}

        mask = build_playoff_week_mask(df, 2024, 15, playoff_team_set)

        # A (wrong year) should be excluded
        assert mask.iloc[0] == False

    def test_excludes_wrong_week(self):
        """Mask should exclude rows from different weeks."""
        df = self._make_test_df()
        df.loc[0, "week"] = 14  # Change A to different week
        playoff_team_set = {"A", "B", "C", "D", "E", "F"}

        mask = build_playoff_week_mask(df, 2024, 15, playoff_team_set)

        # A (wrong week) should be excluded
        assert mask.iloc[0] == False


# =============================================================================
# Tests for simulate_playoff_bracket_vectorized
# =============================================================================


class TestSimulatePlayoffBracketVectorized:
    """Tests for Monte Carlo bracket simulation."""

    def test_single_team_returns_100_percent(self):
        """Single team remaining should have 100% for all probabilities."""
        result = simulate_playoff_bracket_vectorized(
            teams_alive=["Champion"],
            seeds_map={"Champion": 1},
            mu_hat={"Champion": 100},
            sigma_hat={"Champion": 10},
            n_sims=100,
        )

        assert result["p_semis"]["Champion"] == 100.0
        assert result["p_final"]["Champion"] == 100.0
        assert result["p_champ"]["Champion"] == 100.0

    def test_empty_teams_returns_empty(self):
        """Empty teams list should return empty results."""
        result = simulate_playoff_bracket_vectorized(
            teams_alive=[],
            seeds_map={},
            mu_hat={},
            sigma_hat={},
            n_sims=100,
        )

        assert result["p_semis"] == {}
        assert result["p_final"] == {}
        assert result["p_champ"] == {}

    def test_probabilities_sum_correctly(self):
        """Probabilities should sum to expected values."""
        teams = ["A", "B", "C", "D"]
        seeds_map = {"A": 1, "B": 2, "C": 3, "D": 4}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=0,
            rng_seed=42,
        )

        # p_champ should sum to 100%
        champ_sum = sum(result["p_champ"].values())
        assert abs(champ_sum - 100.0) < 0.1, f"p_champ sum = {champ_sum}, expected 100"

        # p_final should sum to 200% (2 finalists)
        final_sum = sum(result["p_final"].values())
        assert abs(final_sum - 200.0) < 0.1, f"p_final sum = {final_sum}, expected 200"

    def test_6_team_bracket_with_byes(self):
        """6-team bracket with 2 byes should have correct sums."""
        teams = ["A", "B", "C", "D", "E", "F"]
        seeds_map = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=2,
            rng_seed=42,
            original_playoff_teams=6,
        )

        # p_champ should sum to 100%
        champ_sum = sum(result["p_champ"].values())
        assert abs(champ_sum - 100.0) < 0.5, f"p_champ sum = {champ_sum}, expected 100"

        # p_final should sum to 200%
        final_sum = sum(result["p_final"].values())
        assert abs(final_sum - 200.0) < 0.5, f"p_final sum = {final_sum}, expected 200"

        # p_semis should sum to 400% (4 semifinalists)
        semis_sum = sum(result["p_semis"].values())
        assert abs(semis_sum - 400.0) < 0.5, f"p_semis sum = {semis_sum}, expected 400"

    def test_actual_results_override_simulation(self):
        """Actual game results should override simulation."""
        teams = ["A", "B"]
        seeds_map = {"A": 1, "B": 2}
        mu_hat = {"A": 100, "B": 100}
        sigma_hat = {"A": 15, "B": 15}

        # A beat B in actual game
        actual_results = {("A", "B"): "A", ("B", "A"): "A"}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            actual_results=actual_results,
            rng_seed=42,
        )

        # A should have 100% championship probability
        assert result["p_champ"]["A"] == 100.0
        assert result["p_champ"]["B"] == 0.0

    def test_reproducibility_with_seed(self):
        """Same rng_seed should produce same results."""
        teams = ["A", "B", "C", "D"]
        seeds_map = {"A": 1, "B": 2, "C": 3, "D": 4}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        result1 = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            rng_seed=12345,
        )

        result2 = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            rng_seed=12345,
        )

        for key in ["p_semis", "p_final", "p_champ"]:
            for team in teams:
                assert result1[key][team] == result2[key][team], f"Results differ for {team} {key}"

    def test_higher_mu_favors_team(self):
        """Team with higher mu should have higher championship probability."""
        teams = ["Strong", "Weak"]
        seeds_map = {"Strong": 1, "Weak": 2}
        mu_hat = {"Strong": 120, "Weak": 80}  # Strong scores 40 points more on average
        sigma_hat = {"Strong": 10, "Weak": 10}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            rng_seed=42,
        )

        # Strong team should have much higher championship probability
        assert result["p_champ"]["Strong"] > 90, f"Strong team should dominate, got {result['p_champ']['Strong']}%"

    def test_bracket_sides_preserved_after_elimination(self):
        """Teams should stay on their bracket side even after eliminations.

        Note: The simulation calculates FORWARD probabilities from current state.
        When 4 teams remain (semis), p_semis represents prob of winning semi,
        not "already made semis" (that's what apply_completed_round_overrides handles).
        """
        # Start with 6 teams, simulate after wild card where 4 remain
        # Seeds: A=1 (Side A), B=2 (Side B), C=3 (Side B), E=5 (Side A)
        teams_remaining = ["A", "B", "C", "E"]  # D and F eliminated
        seeds_map = {"A": 1, "B": 2, "C": 3, "E": 5}
        mu_hat = {t: 100 for t in teams_remaining}
        sigma_hat = {t: 15 for t in teams_remaining}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams_remaining,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=0,  # Byes already processed
            rng_seed=42,
            original_playoff_teams=6,  # Original bracket was 6 teams
        )

        # p_final should sum to 200% (2 finalists)
        final_sum = sum(result["p_final"].values())
        assert abs(final_sum - 200.0) < 1.0, f"p_final sum = {final_sum}"

        # p_champ should sum to 100%
        champ_sum = sum(result["p_champ"].values())
        assert abs(champ_sum - 100.0) < 1.0, f"p_champ sum = {champ_sum}"

        # With equal strength, each team should have ~25% championship probability
        for team in teams_remaining:
            assert 15 < result["p_champ"][team] < 35, f"{team} should have ~25% p_champ, got {result['p_champ'][team]}%"

        # Bracket sides should be preserved - A and E on one side, B and C on other
        # So (A or E) meets (B or C) in finals, not A vs E or B vs C
        # This is tested by the probability distribution being roughly even


# =============================================================================
# Integration Tests
# =============================================================================


class TestIntegration:
    """Integration tests combining multiple functions."""

    def test_full_playoff_progression(self):
        """Test a complete playoff progression through all rounds."""
        teams = ["T1", "T2", "T3", "T4", "T5", "T6"]
        seeds_map = {f"T{i}": i for i in range(1, 7)}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        # Week 15: Wild card (T3 vs T6, T4 vs T5)
        # T1, T2 have byes
        effective_byes = calculate_effective_byes(0, 2, False)
        assert effective_byes == 2

        result_wc = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            bye_teams=effective_byes,
            rng_seed=42,
            original_playoff_teams=6,
        )

        # All teams should have some championship probability
        for team in teams:
            assert result_wc["p_champ"][team] > 0, f"{team} should have p_champ > 0"

        # Probability sums should be correct
        assert abs(sum(result_wc["p_champ"].values()) - 100) < 1
        assert abs(sum(result_wc["p_final"].values()) - 200) < 1
        assert abs(sum(result_wc["p_semis"].values()) - 400) < 1

        # After wild card, say T3 and T4 won (T5, T6 eliminated)
        teams_alive_semis = ["T1", "T2", "T3", "T4"]
        effective_byes_semis = calculate_effective_byes(0, 2, True)
        assert effective_byes_semis == 0  # Byes processed

        result_semis = simulate_playoff_bracket_vectorized(
            teams_alive=teams_alive_semis,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            bye_teams=effective_byes_semis,
            rng_seed=42,
            original_playoff_teams=6,
        )

        # Probability sums should still be correct for 4 remaining teams
        assert abs(sum(result_semis["p_champ"].values()) - 100) < 1
        assert abs(sum(result_semis["p_final"].values()) - 200) < 1

        # Use apply_completed_round_overrides to mark all 4 as semifinalists
        import pandas as pd

        odds_df = pd.DataFrame(
            {
                "P_Semis": [result_semis["p_semis"][t] for t in teams_alive_semis],
                "P_Final": [result_semis["p_final"][t] for t in teams_alive_semis],
                "P_Champ": [result_semis["p_champ"][t] for t in teams_alive_semis],
            },
            index=teams_alive_semis,
        )

        # After wild card complete, apply overrides
        odds_df = apply_completed_round_overrides(
            odds=odds_df,
            teams_alive=teams_alive_semis,
            playoff_teams=teams,
            bracket_info=None,
            current_playoff_round=0,  # Wild card
            games_this_round_complete=True,
        )

        # Now all 4 remaining teams should be semifinalists (100%)
        for team in teams_alive_semis:
            assert odds_df.at[team, "P_Semis"] == 100.0, f"{team} should have P_Semis=100% after override"


# =============================================================================
# Tests for Input Validation
# =============================================================================

# =============================================================================
# Tests for Reseeding Brackets
# =============================================================================


class TestReseedingBracket:
    """Tests for reseeding bracket simulation."""

    def test_reseeding_probabilities_sum_correctly(self):
        """Reseeding bracket probabilities should sum to expected values."""
        teams = ["A", "B", "C", "D"]
        seeds_map = {"A": 1, "B": 2, "C": 3, "D": 4}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=0,
            uses_reseeding=True,
            rng_seed=42,
        )

        # p_champ should sum to 100%
        champ_sum = sum(result["p_champ"].values())
        assert abs(champ_sum - 100.0) < 0.5, f"p_champ sum = {champ_sum}, expected 100"

        # p_final should sum to 200% (2 finalists)
        final_sum = sum(result["p_final"].values())
        assert abs(final_sum - 200.0) < 0.5, f"p_final sum = {final_sum}, expected 200"

    def test_reseeding_6_team_with_byes(self):
        """6-team reseeding bracket with 2 byes should work correctly."""
        teams = ["A", "B", "C", "D", "E", "F"]
        seeds_map = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=2,
            uses_reseeding=True,
            rng_seed=42,
        )

        # p_champ should sum to 100%
        champ_sum = sum(result["p_champ"].values())
        assert abs(champ_sum - 100.0) < 0.5, f"p_champ sum = {champ_sum}"

        # p_final should sum to 200%
        final_sum = sum(result["p_final"].values())
        assert abs(final_sum - 200.0) < 0.5, f"p_final sum = {final_sum}"

        # p_semis should sum to 400% (4 semifinalists)
        semis_sum = sum(result["p_semis"].values())
        assert abs(semis_sum - 400.0) < 0.5, f"p_semis sum = {semis_sum}"

        # Bye teams (seeds 1, 2) should have 100% p_semis (they skip wild card)
        assert result["p_semis"]["A"] == 100.0, "Seed 1 should have 100% p_semis"
        assert result["p_semis"]["B"] == 100.0, "Seed 2 should have 100% p_semis"

    def test_reseeding_higher_seed_advantage(self):
        """In reseeding, higher seeds should have advantage (always play lowest)."""
        teams = ["Top", "Mid", "Low"]
        seeds_map = {"Top": 1, "Mid": 2, "Low": 3}
        # Give Low team much higher variance - can upset but less consistent
        mu_hat = {"Top": 110, "Mid": 100, "Low": 90}
        sigma_hat = {"Top": 10, "Mid": 10, "Low": 10}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=1,  # Top has bye
            uses_reseeding=True,
            rng_seed=42,
        )

        # Top seed should have highest championship probability
        assert (
            result["p_champ"]["Top"] > result["p_champ"]["Mid"]
        ), f"Top ({result['p_champ']['Top']}) should beat Mid ({result['p_champ']['Mid']})"
        assert (
            result["p_champ"]["Top"] > result["p_champ"]["Low"]
        ), f"Top ({result['p_champ']['Top']}) should beat Low ({result['p_champ']['Low']})"

    def test_reseeding_vs_fixed_bracket_difference(self):
        """Reseeding and fixed brackets should give different results.

        In fixed bracket: #1 (Side A) always meets winner of #4/#5 (Side A)
        In reseeding: #1 always meets the LOWEST remaining seed

        This matters when upsets occur:
        - Fixed: if #5 beats #4, #1 still plays #5
        - Reseeding: if #6 beats #3 and #5 beats #4, #1 plays #6 (lowest remaining)
        """
        teams = ["A", "B", "C", "D", "E", "F"]
        seeds_map = {"A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        result_fixed = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=2,
            uses_reseeding=False,
            rng_seed=42,
        )

        result_reseeding = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=10000,
            bye_teams=2,
            uses_reseeding=True,
            rng_seed=42,
        )

        # Results should be different (not exactly equal)
        # With equal strength teams, reseeding slightly favors top seeds
        # because they always play lowest remaining, not just their bracket side
        fixed_champ_a = result_fixed["p_champ"]["A"]
        reseeding_champ_a = result_reseeding["p_champ"]["A"]

        # They should both be roughly similar but not identical
        # (same RNG seed but different matchup logic)
        assert abs(fixed_champ_a - reseeding_champ_a) < 10, "Results shouldn't be wildly different for equal teams"

    def test_reseeding_actual_results_override(self):
        """Actual game results should override simulation in reseeding."""
        teams = ["A", "B"]
        seeds_map = {"A": 1, "B": 2}
        mu_hat = {"A": 100, "B": 100}
        sigma_hat = {"A": 15, "B": 15}

        # B beat A in actual game
        actual_results = {("A", "B"): "B", ("B", "A"): "B"}

        result = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            uses_reseeding=True,
            actual_results=actual_results,
            rng_seed=42,
        )

        # B should have 100% championship probability
        assert result["p_champ"]["B"] == 100.0
        assert result["p_champ"]["A"] == 0.0

    def test_reseeding_reproducibility(self):
        """Same rng_seed should produce same reseeding results."""
        teams = ["A", "B", "C", "D"]
        seeds_map = {"A": 1, "B": 2, "C": 3, "D": 4}
        mu_hat = {t: 100 for t in teams}
        sigma_hat = {t: 15 for t in teams}

        result1 = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            uses_reseeding=True,
            rng_seed=12345,
        )

        result2 = simulate_playoff_bracket_vectorized(
            teams_alive=teams,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=1000,
            uses_reseeding=True,
            rng_seed=12345,
        )

        for key in ["p_semis", "p_final", "p_champ"]:
            for team in teams:
                assert result1[key][team] == result2[key][team], f"Reseeding results differ for {team} {key}"


class TestInputValidation:
    """Tests for input validation and error handling."""

    # ----- _get_bracket_side validation -----

    def test_get_bracket_side_invalid_seed_zero(self):
        """Seed 0 should raise ValueError."""
        with pytest.raises(ValueError, match="seed must be a positive integer"):
            _get_bracket_side(0, 4)

    def test_get_bracket_side_invalid_seed_negative(self):
        """Negative seed should raise ValueError."""
        with pytest.raises(ValueError, match="seed must be a positive integer"):
            _get_bracket_side(-1, 4)

    def test_get_bracket_side_invalid_num_playoff_teams(self):
        """num_playoff_teams < 2 should raise ValueError."""
        with pytest.raises(ValueError, match="num_playoff_teams must be >= 2"):
            _get_bracket_side(1, 1)

    # ----- calculate_effective_byes validation -----

    def test_effective_byes_negative_round(self):
        """Negative playoff round should raise ValueError."""
        with pytest.raises(ValueError, match="current_playoff_round must be non-negative"):
            calculate_effective_byes(-1, 2, False)

    def test_effective_byes_negative_bye_teams(self):
        """Negative bye_teams should raise ValueError."""
        with pytest.raises(ValueError, match="bye_teams must be non-negative"):
            calculate_effective_byes(0, -1, False)

    # ----- apply_completed_round_overrides validation -----

    def test_apply_overrides_missing_columns(self):
        """DataFrame missing required columns should raise ValueError."""
        df = pd.DataFrame({"P_Semis": [50.0], "P_Final": [25.0]}, index=["A"])  # Missing P_Champ
        with pytest.raises(ValueError, match="missing required columns"):
            apply_completed_round_overrides(
                odds=df,
                teams_alive=["A"],
                playoff_teams=["A"],
                bracket_info=None,
                current_playoff_round=0,
                games_this_round_complete=True,
            )

    def test_apply_overrides_empty_df_returns_empty(self):
        """Empty DataFrame should return empty DataFrame without error."""
        df = pd.DataFrame()
        result = apply_completed_round_overrides(
            odds=df,
            teams_alive=["A"],
            playoff_teams=["A"],
            bracket_info=None,
            current_playoff_round=0,
            games_this_round_complete=True,
        )
        assert result.empty

    # ----- build_playoff_week_mask validation -----

    def test_build_mask_missing_columns(self):
        """DataFrame missing required columns should raise ValueError."""
        df = pd.DataFrame({"year": [2024], "week": [15]})  # Missing is_playoffs, etc.
        with pytest.raises(ValueError, match="missing required columns"):
            build_playoff_week_mask(df, 2024, 15, {"A"})

    def test_build_mask_empty_df_returns_empty_series(self):
        """Empty DataFrame should return empty Series without error."""
        df = pd.DataFrame()
        result = build_playoff_week_mask(df, 2024, 15, {"A"})
        assert len(result) == 0

    # ----- simulate_playoff_bracket_vectorized validation -----

    def test_simulate_teams_alive_none(self):
        """teams_alive=None should raise TypeError."""
        with pytest.raises(TypeError, match="teams_alive cannot be None"):
            simulate_playoff_bracket_vectorized(
                teams_alive=None,
                seeds_map={},
                mu_hat={},
                sigma_hat={},
            )

    def test_simulate_teams_alive_wrong_type(self):
        """teams_alive as string should raise TypeError."""
        with pytest.raises(TypeError, match="teams_alive must be a list or tuple"):
            simulate_playoff_bracket_vectorized(
                teams_alive="TeamA",
                seeds_map={},
                mu_hat={},
                sigma_hat={},
            )

    def test_simulate_seeds_map_none(self):
        """seeds_map=None should raise TypeError."""
        with pytest.raises(TypeError, match="seeds_map cannot be None"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A"],
                seeds_map=None,
                mu_hat={},
                sigma_hat={},
            )

    def test_simulate_mu_hat_none(self):
        """mu_hat=None should raise TypeError."""
        with pytest.raises(TypeError, match="mu_hat cannot be None"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A"],
                seeds_map={"A": 1},
                mu_hat=None,
                sigma_hat={},
            )

    def test_simulate_sigma_hat_none(self):
        """sigma_hat=None should raise TypeError."""
        with pytest.raises(TypeError, match="sigma_hat cannot be None"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A"],
                seeds_map={"A": 1},
                mu_hat={"A": 100},
                sigma_hat=None,
            )

    def test_simulate_n_sims_zero(self):
        """n_sims=0 should raise ValueError."""
        with pytest.raises(ValueError, match="n_sims must be a positive integer"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A", "B"],
                seeds_map={"A": 1, "B": 2},
                mu_hat={"A": 100, "B": 100},
                sigma_hat={"A": 15, "B": 15},
                n_sims=0,
            )

    def test_simulate_n_sims_negative(self):
        """Negative n_sims should raise ValueError."""
        with pytest.raises(ValueError, match="n_sims must be a positive integer"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A", "B"],
                seeds_map={"A": 1, "B": 2},
                mu_hat={"A": 100, "B": 100},
                sigma_hat={"A": 15, "B": 15},
                n_sims=-100,
            )

    def test_simulate_bye_teams_negative(self):
        """Negative bye_teams should raise ValueError."""
        with pytest.raises(ValueError, match="bye_teams must be a non-negative integer"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A", "B"],
                seeds_map={"A": 1, "B": 2},
                mu_hat={"A": 100, "B": 100},
                sigma_hat={"A": 15, "B": 15},
                bye_teams=-1,
            )

    def test_simulate_bye_teams_exceeds_teams(self):
        """bye_teams > len(teams_alive) should raise ValueError."""
        with pytest.raises(ValueError, match="bye_teams.*cannot exceed"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A", "B"],
                seeds_map={"A": 1, "B": 2},
                mu_hat={"A": 100, "B": 100},
                sigma_hat={"A": 15, "B": 15},
                bye_teams=3,
            )

    def test_simulate_original_playoff_teams_too_small(self):
        """original_playoff_teams < len(teams_alive) should raise ValueError."""
        with pytest.raises(ValueError, match="original_playoff_teams.*cannot be less than"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A", "B", "C", "D"],
                seeds_map={"A": 1, "B": 2, "C": 3, "D": 4},
                mu_hat={"A": 100, "B": 100, "C": 100, "D": 100},
                sigma_hat={"A": 15, "B": 15, "C": 15, "D": 15},
                original_playoff_teams=2,  # Less than 4 teams
            )

    def test_simulate_original_playoff_teams_invalid(self):
        """original_playoff_teams < 2 should raise ValueError."""
        with pytest.raises(ValueError, match="original_playoff_teams must be >= 2"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A"],
                seeds_map={"A": 1},
                mu_hat={"A": 100},
                sigma_hat={"A": 15},
                original_playoff_teams=1,
            )

    def test_simulate_actual_results_wrong_type(self):
        """actual_results as list should raise TypeError."""
        with pytest.raises(TypeError, match="actual_results must be a dict"):
            simulate_playoff_bracket_vectorized(
                teams_alive=["A", "B"],
                seeds_map={"A": 1, "B": 2},
                mu_hat={"A": 100, "B": 100},
                sigma_hat={"A": 15, "B": 15},
                actual_results=[("A", "B", "A")],  # Should be dict
            )

    def test_simulate_accepts_integer_valued_float_seeds(self):
        """Seed maps widened to float by pandas should still simulate."""
        result = simulate_playoff_bracket_vectorized(
            teams_alive=["A", "B", "C", "D"],
            seeds_map={"A": 1.0, "B": 2.0, "C": 3.0, "D": 4.0},
            mu_hat={"A": 100, "B": 100, "C": 100, "D": 100},
            sigma_hat={"A": 15, "B": 15, "C": 15, "D": 15},
            n_sims=100,
            rng_seed=7,
            original_playoff_teams=4,
        )

        assert set(result["p_champ"]) == {"A", "B", "C", "D"}


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
