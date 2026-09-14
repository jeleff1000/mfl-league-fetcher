"""Tests for PlayoffConfig dataclass."""

import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.transformations.matchup.modules.playoff_config import PlayoffConfig


class TestPlayoffConfig:
    def test_create_with_all_fields(self):
        cfg = PlayoffConfig(
            playoff_slots=6,
            bye_slots=2,
            bracket_reseed=False,
            num_teams=12,
            regular_season_weeks=14,
            tiebreaker_order=["total_points", "head_to_head"],
        )
        assert cfg.playoff_slots == 6
        assert cfg.bye_slots == 2
        assert cfg.bracket_reseed is False
        assert cfg.num_teams == 12
        assert cfg.regular_season_weeks == 14
        assert cfg.tiebreaker_order == ["total_points", "head_to_head"]

    def test_defaults(self):
        cfg = PlayoffConfig(playoff_slots=6, bye_slots=2, num_teams=10, regular_season_weeks=14)
        assert cfg.bracket_reseed is False
        assert cfg.tiebreaker_order == ["total_points", "head_to_head"]

    def test_target_cols_property(self):
        cfg = PlayoffConfig(playoff_slots=6, bye_slots=2, num_teams=10, regular_season_weeks=14)
        cols = cfg.target_cols
        assert "p_playoffs" in cols
        assert "p_champ" in cols
        assert "x10_seed" in cols
        assert "x64_seed" in cols
        assert "x65_seed" not in cols
        assert "x14_win" in cols
        assert "x36_win" in cols
        assert "x37_win" not in cols

    def test_target_cols_base_columns(self):
        """Verify all base columns are present."""
        cfg = PlayoffConfig(playoff_slots=4, bye_slots=0, num_teams=8, regular_season_weeks=13)
        cols = cfg.target_cols
        for base in [
            "avg_seed",
            "p_playoffs",
            "p_bye",
            "exp_final_wins",
            "exp_final_pf",
            "p_semis",
            "p_final",
            "p_champ",
            "power_rating",
        ]:
            assert base in cols

    def test_target_cols_seed_range(self):
        """Canonical seed columns should always reserve x1_seed through x64_seed."""
        cfg = PlayoffConfig(playoff_slots=4, bye_slots=0, num_teams=8, regular_season_weeks=13)
        cols = cfg.target_cols
        for i in range(1, 65):
            assert f"x{i}_seed" in cols
        assert "x0_seed" not in cols
        assert "x65_seed" not in cols

    def test_target_cols_win_range(self):
        """Canonical win columns should always reserve x0_win through x36_win."""
        cfg = PlayoffConfig(playoff_slots=4, bye_slots=0, num_teams=8, regular_season_weeks=13)
        cols = cfg.target_cols
        for i in range(0, 37):
            assert f"x{i}_win" in cols
        assert "x37_win" not in cols

    def test_target_cols_win_range_doubles_for_h2h_median(self):
        cfg = PlayoffConfig(playoff_slots=6, bye_slots=2, num_teams=12, regular_season_weeks=18, use_median=True)
        cols = cfg.target_cols
        assert "x36_win" in cols
        assert "x37_win" not in cols

    def test_tiebreaker_order_is_independent_copy(self):
        """Ensure default tiebreaker_order is a fresh list, not shared reference."""
        cfg1 = PlayoffConfig(playoff_slots=4, bye_slots=0, num_teams=8, regular_season_weeks=13)
        cfg2 = PlayoffConfig(playoff_slots=4, bye_slots=0, num_teams=8, regular_season_weeks=13)
        cfg1.tiebreaker_order.append("points_against")
        assert "points_against" not in cfg2.tiebreaker_order
