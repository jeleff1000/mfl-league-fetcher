from pathlib import Path

import pytest

from multi_league.transformations.matchup.expected_record_v2 import detect_median_scoring
from multi_league.transformations.matchup.modules.playoff_bracket.utils import (
    get_expected_championship_week,
    load_league_settings,
    validate_bracket_structure,
)
from multi_league.transformations.matchup.playoff_odds_import import load_playoff_config_from_settings


def test_detect_median_scoring_prefers_canonical_settings_rows():
    settings_by_year = {
        2024: {"year": 2024, "uses_median": False},
        2025: {"year": 2025, "uses_median": True},
    }

    assert detect_median_scoring(settings_by_year=settings_by_year) is True
    assert detect_median_scoring(settings_by_year=settings_by_year, year=2024) is False
    assert detect_median_scoring(settings_by_year=settings_by_year, year=2025) is True


def test_load_league_settings_accepts_flat_settings_rows():
    settings_by_year = {
        2025: {
            "year": 2025,
            "playoff_start_week": 15,
            "playoff_teams": 6,
            "bye_teams": 2,
            "uses_playoff_reseeding": True,
            "num_teams": 12,
            "has_multiweek_championship": False,
            "playoff_round_type": 0,
            "uses_median": True,
        }
    }

    settings = load_league_settings(2025, settings_by_year=settings_by_year)

    assert settings["playoff_start_week"] == 15
    assert settings["num_playoff_teams"] == 6
    assert settings["bye_teams"] == 2
    assert settings["uses_playoff_reseeding"] == 1
    assert settings["num_teams"] == 12
    assert settings["uses_median"] is True


def test_load_playoff_config_from_settings_uses_flat_settings_rows():
    class _Ctx:
        def __init__(self):
            self.data_directory = Path(".")

    settings_by_year = {
        2024: {"year": 2024, "playoff_start_week": 14, "playoff_teams": 4, "bye_teams": 0, "num_teams": 10},
        2025: {"year": 2025, "playoff_start_week": 15, "playoff_teams": 6, "bye_teams": 2, "num_teams": 12},
    }

    playoff_slots, bye_slots, bracket_reseed, config = load_playoff_config_from_settings(
        _Ctx(),
        df=None,
        settings_by_year=settings_by_year,
    )

    assert playoff_slots == 6
    assert bye_slots == 2
    assert bracket_reseed is False
    assert config.num_teams == 12
    assert config.regular_season_weeks == 14


def test_load_playoff_config_accepts_disabled_playoffs():
    class _Ctx:
        def __init__(self):
            self.data_directory = Path(".")

    settings_by_year = {
        2025: {
            "year": 2025,
            "playoff_start_week": 19,
            "playoff_teams": 0,
            "bye_teams": 0,
            "num_teams": 10,
            "end_week": 18,
        }
    }

    settings = load_league_settings(2025, settings_by_year=settings_by_year)
    playoff_slots, bye_slots, bracket_reseed, config = load_playoff_config_from_settings(
        _Ctx(),
        df=None,
        settings_by_year=settings_by_year,
    )

    assert settings["num_playoff_teams"] == 0
    assert settings["playoff_start_week"] == 19
    assert settings["end_week"] == 18
    assert playoff_slots == 0
    assert bye_slots == 0
    assert bracket_reseed is False
    assert config.playoff_slots == 0
    assert config.regular_season_weeks == 18


def test_load_league_settings_accepts_two_team_championship():
    settings_by_year = {
        2023: {
            "year": 2023,
            "playoff_start_week": 16,
            "playoff_teams": 2,
            "bye_teams": 0,
            "num_teams": 12,
            "end_week": 17,
            "has_multiweek_championship": True,
        }
    }

    settings = load_league_settings(2023, settings_by_year=settings_by_year)

    assert validate_bracket_structure(2, 0, 12) == (True, "")
    assert settings["num_playoff_teams"] == 2
    assert settings["bye_teams"] == 0
    assert settings["playoff_start_week"] == 16
    assert settings["end_week"] == 17


def test_load_league_settings_requires_canonical_playoff_fields():
    settings_by_year = {
        2025: {
            "year": 2025,
            "playoff_start_week": 15,
            "playoff_teams": 6,
            "num_teams": 12,
        }
    }

    with pytest.raises(ValueError, match="bye_teams"):
        load_league_settings(2025, settings_by_year=settings_by_year)


def test_load_playoff_config_from_settings_requires_canonical_playoff_fields():
    class _Ctx:
        def __init__(self):
            self.data_directory = Path(".")

    settings_by_year = {
        2025: {
            "year": 2025,
            "playoff_teams": 6,
            "bye_teams": 2,
            "num_teams": 12,
        }
    }

    with pytest.raises(ValueError, match="playoff_start_week"):
        load_playoff_config_from_settings(_Ctx(), df=None, settings_by_year=settings_by_year)


def test_get_expected_championship_week_requires_explicit_settings():
    with pytest.raises(ValueError, match="num_playoff_teams"):
        get_expected_championship_week({"playoff_start_week": 15})
