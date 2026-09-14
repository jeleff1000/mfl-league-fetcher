"""
Tests for the deterministic Bracket model.

Covers bracket shape, week mapping, bracket tracing (champion detection),
reseeding vs fixed bracket, and is_championship_game.
"""

import pytest
import sys
from pathlib import Path

import pandas as pd

# Add paths for imports
SCRIPT_DIR = Path(__file__).resolve().parent
TESTS_DIR = SCRIPT_DIR.parent.parent
SCRIPTS_DIR = TESTS_DIR.parent
MULTI_LEAGUE_DIR = SCRIPTS_DIR / "multi_league"
MODULES_DIR = MULTI_LEAGUE_DIR / "transformations" / "matchup" / "modules"
sys.path.insert(0, str(SCRIPTS_DIR))
sys.path.insert(0, str(MULTI_LEAGUE_DIR))
sys.path.insert(0, str(MODULES_DIR))

from playoff_bracket.bracket import Bracket


class TestBracketShape:
    @pytest.mark.parametrize(
        "teams,expected_rounds,expected_byes",
        [
            (2, 1, 0),
            (3, 2, 1),
            (4, 2, 0),
            (5, 3, 3),
            (6, 3, 2),
            (7, 3, 1),
            (8, 3, 0),
            (10, 4, 6),
            (12, 4, 4),
            (14, 4, 2),
            (16, 4, 0),
            (32, 5, 0),
            (64, 6, 0),
        ],
    )
    def test_rounds_and_byes(self, teams, expected_rounds, expected_byes):
        b = Bracket(num_playoff_teams=teams)
        assert b.num_rounds == expected_rounds
        assert b.byes == expected_byes

    def test_6_team_seeding_round1(self):
        """6-team: seeds 1,2 get byes. Round 1 games: 3v6, 4v5."""
        b = Bracket(num_playoff_teams=6)
        r1 = b.round_matchups(1)  # Only actual games, no byes
        assert len(r1) == 2
        pairs = [frozenset(m) for m in r1]
        assert frozenset({3, 6}) in pairs
        assert frozenset({4, 5}) in pairs

    def test_6_team_byes(self):
        b = Bracket(num_playoff_teams=6)
        assert b.byes == 2
        assert b.get_bye_seeds() == {1, 2}

    def test_8_team_seeding_round1(self):
        """8-team, no byes: 1v8, 4v5, 2v7, 3v6."""
        b = Bracket(num_playoff_teams=8)
        r1 = b.round_matchups(1)
        assert len(r1) == 4
        seeds = set()
        for a, bs in r1:
            seeds.add(a)
            seeds.add(bs)
        assert seeds == {1, 2, 3, 4, 5, 6, 7, 8}

    def test_10_team_bracket(self):
        """10-team: 6 byes, 2 games in round 1."""
        b = Bracket(num_playoff_teams=10)
        assert b.byes == 6
        r1 = b.round_matchups(1)
        assert len(r1) == 2


class TestWeekMapping:
    def test_type0_6team(self):
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0)
        assert b.week_range_for_round(1) == (15, 15)
        assert b.week_range_for_round(2) == (16, 16)
        assert b.week_range_for_round(3) == (17, 17)
        assert b.championship_week == 17

    def test_type1_6team(self):
        b = Bracket(num_playoff_teams=6, playoff_start_week=14, round_type=1)
        assert b.week_range_for_round(1) == (14, 15)
        assert b.week_range_for_round(2) == (16, 17)
        assert b.week_range_for_round(3) == (18, 19)
        assert b.championship_week == 19

    def test_type2_6team(self):
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=2)
        assert b.week_range_for_round(1) == (15, 15)
        assert b.week_range_for_round(2) == (16, 16)
        assert b.week_range_for_round(3) == (17, 18)
        assert b.championship_week == 18

    def test_type0_4team(self):
        b = Bracket(num_playoff_teams=4, playoff_start_week=15, round_type=0)
        assert b.championship_week == 16

    def test_type0_8team(self):
        b = Bracket(num_playoff_teams=8, playoff_start_week=15, round_type=0)
        assert b.championship_week == 17


class TestBracketTracing:
    def test_6team_standard_champion(self):
        """Seed 1 wins it all."""
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0)
        b.record_result(1, winner_seed=3, loser_seed=6)
        b.record_result(1, winner_seed=4, loser_seed=5)
        b.record_result(2, winner_seed=1, loser_seed=4)
        b.record_result(2, winner_seed=2, loser_seed=3)
        b.record_result(3, winner_seed=1, loser_seed=2)
        assert b.get_champion_seed() == 1

    def test_6team_upset_champion(self):
        """Seed 3 upsets through the bracket (real scenario from the_league 2024)."""
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0)
        b.record_result(1, winner_seed=3, loser_seed=6)
        b.record_result(1, winner_seed=5, loser_seed=4)
        b.record_result(2, winner_seed=1, loser_seed=5)
        b.record_result(2, winner_seed=3, loser_seed=2)
        b.record_result(3, winner_seed=3, loser_seed=1)
        assert b.get_champion_seed() == 3

    def test_8team_5seed_wins(self):
        """Seed 5 wins 8-team bracket."""
        b = Bracket(num_playoff_teams=8, playoff_start_week=15, round_type=0)
        b.record_result(1, winner_seed=1, loser_seed=8)
        b.record_result(1, winner_seed=5, loser_seed=4)
        b.record_result(1, winner_seed=2, loser_seed=7)
        b.record_result(1, winner_seed=6, loser_seed=3)
        b.record_result(2, winner_seed=5, loser_seed=1)
        b.record_result(2, winner_seed=6, loser_seed=2)
        b.record_result(3, winner_seed=5, loser_seed=6)
        assert b.get_champion_seed() == 5

    def test_no_results_no_champion(self):
        b = Bracket(num_playoff_teams=6)
        assert b.get_champion_seed() is None

    def test_4team_bracket(self):
        b = Bracket(num_playoff_teams=4, playoff_start_week=15, round_type=0)
        b.record_result(1, winner_seed=1, loser_seed=4)
        b.record_result(1, winner_seed=3, loser_seed=2)
        b.record_result(2, winner_seed=3, loser_seed=1)
        assert b.get_champion_seed() == 3

    def test_2team_bracket(self):
        b = Bracket(num_playoff_teams=2, playoff_start_week=17, round_type=0)
        b.record_result(1, winner_seed=2, loser_seed=1)
        assert b.get_champion_seed() == 2


class TestReseeding:
    def test_6team_reseeding_round2(self):
        """After QF upsets, remaining teams re-paired by seed."""
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0, uses_reseeding=True)
        # Round 1: 6 beats 3 (upset!), 5 beats 4 (upset!)
        b.record_result(1, winner_seed=6, loser_seed=3)
        b.record_result(1, winner_seed=5, loser_seed=4)
        # Remaining: {1, 2, 5, 6}. Reseeded: 1v6, 2v5
        r2 = b.round_matchups(2)
        pairs = [frozenset(m) for m in r2]
        assert frozenset({1, 6}) in pairs
        assert frozenset({2, 5}) in pairs

    def test_6team_fixed_bracket_round2(self):
        """Without reseeding, bracket sides preserved."""
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0, uses_reseeding=False)
        b.record_result(1, winner_seed=6, loser_seed=3)
        b.record_result(1, winner_seed=5, loser_seed=4)
        # Fixed bracket: Side A = {1, (4v5)}, Side B = {2, (3v6)}
        # So: 1 vs W(4v5)=5, 2 vs W(3v6)=6
        r2 = b.round_matchups(2)
        pairs = [frozenset(m) for m in r2]
        assert frozenset({1, 5}) in pairs
        assert frozenset({2, 6}) in pairs

    def test_reseeding_champion(self):
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0, uses_reseeding=True)
        b.record_result(1, winner_seed=6, loser_seed=3)
        b.record_result(1, winner_seed=5, loser_seed=4)
        # Reseeded SF: 1v6, 2v5
        b.record_result(2, winner_seed=6, loser_seed=1)
        b.record_result(2, winner_seed=5, loser_seed=2)
        # Finals: 5v6 (reseeded: best remaining vs worst remaining)
        b.record_result(3, winner_seed=6, loser_seed=5)
        assert b.get_champion_seed() == 6


class TestIsChampionshipGame:
    def test_championship_game_identified(self):
        """The finals matchup should be identified as championship."""
        seeds = {"Dan": 1, "Gary": 2, "Michael": 3, "Vince": 4, "Collins": 5, "Exp": 6}
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0)
        b.record_result(1, winner_seed=3, loser_seed=6)
        b.record_result(1, winner_seed=5, loser_seed=4)
        b.record_result(2, winner_seed=1, loser_seed=5)
        b.record_result(2, winner_seed=3, loser_seed=2)
        # Finals: seed 1 vs seed 3 in week 17
        assert b.is_championship_game("Dan", "Michael", 17, seeds) is True
        assert b.is_championship_game("Michael", "Dan", 17, seeds) is True

    def test_placement_game_not_championship(self):
        """A 3rd place game in the same week should NOT be championship."""
        seeds = {"Dan": 1, "Gary": 2, "Michael": 3, "Vince": 4, "Collins": 5, "Exp": 6}
        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0)
        b.record_result(1, winner_seed=3, loser_seed=6)
        b.record_result(1, winner_seed=5, loser_seed=4)
        b.record_result(2, winner_seed=1, loser_seed=5)
        b.record_result(2, winner_seed=3, loser_seed=2)
        # Gary (2) vs Collins (5) is a placement game, not championship
        assert b.is_championship_game("Gary", "Collins", 17, seeds) is False

    def test_wrong_week_not_championship(self):
        seeds = {"A": 1, "B": 2}
        b = Bracket(num_playoff_teams=2, playoff_start_week=17, round_type=0)
        assert b.is_championship_game("A", "B", 16, seeds) is False


class TestFillFromData:
    """Bracket.fill_from_matchup_data() reads a matchup DataFrame and traces the bracket."""

    def test_the_league_2024(self):
        """Real scenario: the_league 2024, 6 teams, seed 3 wins."""
        seeds = {"Dan": 1, "Gary": 2, "Michael": 3, "Vince Dejesus": 4, "Michael Collins": 5, "Exp": 6}
        matchup_rows = [
            # Week 15 QF
            {
                "year": 2024,
                "week": 15,
                "manager": "Michael",
                "opponent": "Exp",
                "team_points": 144.6,
                "opponent_points": 121.6,
                "win": 1,
                "loss": 0,
                "is_playoffs": 1,
                "is_consolation": 0,
            },
            {
                "year": 2024,
                "week": 15,
                "manager": "Michael Collins",
                "opponent": "Vince Dejesus",
                "team_points": 143.2,
                "opponent_points": 83.2,
                "win": 1,
                "loss": 0,
                "is_playoffs": 1,
                "is_consolation": 0,
            },
            # Week 16 SF
            {
                "year": 2024,
                "week": 16,
                "manager": "Dan",
                "opponent": "Michael Collins",
                "team_points": 135.1,
                "opponent_points": 124.2,
                "win": 1,
                "loss": 0,
                "is_playoffs": 1,
                "is_consolation": 0,
            },
            {
                "year": 2024,
                "week": 16,
                "manager": "Michael",
                "opponent": "Gary",
                "team_points": 124.0,
                "opponent_points": 109.3,
                "win": 1,
                "loss": 0,
                "is_playoffs": 1,
                "is_consolation": 0,
            },
            # Week 17 Finals
            {
                "year": 2024,
                "week": 17,
                "manager": "Michael",
                "opponent": "Dan",
                "team_points": 131.1,
                "opponent_points": 125.3,
                "win": 1,
                "loss": 0,
                "is_playoffs": 1,
                "is_consolation": 0,
            },
        ]
        df = pd.DataFrame(matchup_rows)

        b = Bracket(num_playoff_teams=6, playoff_start_week=15, round_type=0)
        b.fill_from_matchup_data(df, year=2024, seeds=seeds)

        assert b.get_champion_seed() == 3  # Michael = seed 3

    def test_8team_fill(self):
        """8-team bracket filled from data."""
        seeds = {f"T{i}": i for i in range(1, 9)}
        rows = [
            # QF week 15: 1 beats 8, 4 beats 5, 2 beats 7, 3 beats 6
            {
                "year": 2024,
                "week": 15,
                "manager": "T1",
                "opponent": "T8",
                "team_points": 100,
                "opponent_points": 90,
                "win": 1,
                "loss": 0,
            },
            {
                "year": 2024,
                "week": 15,
                "manager": "T4",
                "opponent": "T5",
                "team_points": 100,
                "opponent_points": 90,
                "win": 1,
                "loss": 0,
            },
            {
                "year": 2024,
                "week": 15,
                "manager": "T2",
                "opponent": "T7",
                "team_points": 100,
                "opponent_points": 90,
                "win": 1,
                "loss": 0,
            },
            {
                "year": 2024,
                "week": 15,
                "manager": "T3",
                "opponent": "T6",
                "team_points": 100,
                "opponent_points": 90,
                "win": 1,
                "loss": 0,
            },
            # SF week 16: 1 beats 4, 2 beats 3
            {
                "year": 2024,
                "week": 16,
                "manager": "T1",
                "opponent": "T4",
                "team_points": 110,
                "opponent_points": 90,
                "win": 1,
                "loss": 0,
            },
            {
                "year": 2024,
                "week": 16,
                "manager": "T2",
                "opponent": "T3",
                "team_points": 110,
                "opponent_points": 90,
                "win": 1,
                "loss": 0,
            },
            # Finals week 17: 2 beats 1
            {
                "year": 2024,
                "week": 17,
                "manager": "T2",
                "opponent": "T1",
                "team_points": 120,
                "opponent_points": 100,
                "win": 1,
                "loss": 0,
            },
        ]
        df = pd.DataFrame(rows)

        b = Bracket(num_playoff_teams=8, playoff_start_week=15, round_type=0)
        b.fill_from_matchup_data(df, year=2024, seeds=seeds)
        assert b.get_champion_seed() == 2

    def test_fill_with_missing_game(self):
        """If a game is missing from data, bracket still works for games that exist."""
        seeds = {"A": 1, "B": 2, "C": 3, "D": 4}
        rows = [
            # Only SF: 1 beats 4
            {
                "year": 2024,
                "week": 15,
                "manager": "A",
                "opponent": "D",
                "team_points": 100,
                "opponent_points": 90,
                "win": 1,
                "loss": 0,
            },
        ]
        df = pd.DataFrame(rows)
        b = Bracket(num_playoff_teams=4, playoff_start_week=15, round_type=0)
        b.fill_from_matchup_data(df, year=2024, seeds=seeds)
        # Champion unknown since other SF and finals missing
        assert b.get_champion_seed() is None or b.get_champion_seed() == 1

    def test_fill_with_points_tiebreak(self):
        """When win flag is 0 for both, use points to determine winner."""
        seeds = {"A": 1, "B": 2}
        rows = [
            {
                "year": 2024,
                "week": 17,
                "manager": "A",
                "opponent": "B",
                "team_points": 120,
                "opponent_points": 100,
                "win": 0,
                "loss": 0,
            },
        ]
        df = pd.DataFrame(rows)
        b = Bracket(num_playoff_teams=2, playoff_start_week=17, round_type=0)
        b.fill_from_matchup_data(df, year=2024, seeds=seeds)
        assert b.get_champion_seed() == 1  # A has more points
