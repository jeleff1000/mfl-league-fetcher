"""Tests for consolation bracket columns across all 3 platforms."""

from multi_league.core.canonical_settings import flatten_settings


# ---------------------------------------------------------------------------
# Yahoo
# ---------------------------------------------------------------------------


def test_yahoo_consolation_columns_populated():
    raw = {
        "metadata": {
            "num_playoff_consolation_teams": 4,
            "num_teams": 10,
            "num_playoff_teams": 6,
        },
        "canonical_scoring": {},
        "roster_positions": [],
    }
    flat = flatten_settings(raw, platform="yahoo", year=2025, league_key="461.l.90939")
    assert flat["has_consolation_bracket"] is True
    assert flat["num_playoff_consolation_teams"] == 4


def test_yahoo_no_consolation():
    raw = {
        "metadata": {
            "num_playoff_consolation_teams": 0,
            "num_teams": 10,
            "num_playoff_teams": 6,
        },
        "canonical_scoring": {},
        "roster_positions": [],
    }
    flat = flatten_settings(raw, platform="yahoo", year=2025, league_key="461.l.90939")
    assert flat["has_consolation_bracket"] is False
    assert flat["num_playoff_consolation_teams"] == 0


def test_yahoo_consolation_unknown():
    raw = {
        "metadata": {
            "num_teams": 10,
            "num_playoff_teams": 6,
        },
        "canonical_scoring": {},
        "roster_positions": [],
    }
    flat = flatten_settings(raw, platform="yahoo", year=2025, league_key="461.l.90939")
    assert flat["has_consolation_bracket"] is None
    assert flat["num_playoff_consolation_teams"] is None


# ---------------------------------------------------------------------------
# ESPN
# ---------------------------------------------------------------------------


def test_espn_consolation_enabled():
    raw = {
        "settings": {
            "scheduleSettings": {
                "consolationLadderDisabled": False,
                "playoffTeamCount": 6,
                "matchupPeriodCount": 14,
                "playoffSeedingRule": "TOTAL_POINTS_SCORED",
                "playoffSeedingRuleBy": 0,
            },
            "scoringSettings": {
                "homeTeamBonus": 1,
                "playoffHomeTeamBonus": 3,
                "matchupTieRule": "NONE",
                "matchupTieRuleBy": "NONE",
                "playoffMatchupTieRule": "NONE",
                "playoffMatchupTieRuleBy": "NONE",
            },
            "size": 12,
        },
    }
    flat = flatten_settings(raw, platform="espn", year=2024, league_key="71580")
    assert flat["has_consolation_bracket"] is True
    assert flat["num_playoff_consolation_teams"] == 6
    assert flat["playoff_bracket_source"] == "api"
    assert flat["playoff_seeding_rule"] == "TOTAL_POINTS_SCORED"
    assert flat["playoff_seeding_rule_by"] == 0
    assert flat["home_team_bonus"] == 1.0
    assert flat["playoff_home_team_bonus"] == 3.0
    assert flat["matchup_tie_rule"] == "NONE"
    assert flat["playoff_matchup_tie_rule"] == "NONE"


def test_espn_consolation_disabled():
    raw = {
        "settings": {
            "scheduleSettings": {
                "consolationLadderDisabled": True,
                "playoffTeamCount": 6,
                "matchupPeriodCount": 14,
            },
            "size": 12,
        },
    }
    flat = flatten_settings(raw, platform="espn", year=2024, league_key="71580")
    assert flat["has_consolation_bracket"] is False
    assert flat["num_playoff_consolation_teams"] == 0


def test_espn_raw_slot_counts_canonicalize_super_flex_and_dp():
    raw = {
        "settings": {
            "size": 12,
            "scheduleSettings": {
                "playoffTeamCount": 6,
                "matchupPeriodCount": 14,
            },
            "rosterSettings": {
                "lineupSlotCounts": {
                    "0": 1,
                    "3": 1,
                    "5": 1,
                    "7": 1,
                    "15": 1,
                    "23": 2,
                    "20": 6,
                }
            },
        },
    }
    flat = flatten_settings(raw, platform="espn", year=2024, league_key="71580")
    assert flat["roster_QB"] == 1
    assert flat["roster_W/R"] == 1
    assert flat["roster_REC_FLEX"] == 1
    assert flat["roster_SUPER_FLEX"] == 1
    assert flat["roster_IDP"] == 1
    assert flat["roster_FLX"] == 2
    assert flat["roster_BN"] == 6


def test_espn_preprocessed_roster_counts_canonicalize_aliases():
    raw = {
        "scoring_settings": {},
        "num_teams": 12,
        "playoff_teams": 6,
        "playoff_start_week": 15,
        "regular_season_length": 14,
        "roster_position_counts": {
            "OP": 1,
            "DP": 1,
            "RB/WR/TE": 1,
            "RB/WR": 1,
            "WR/TE": 1,
            "BE": 5,
            "ER": 2,
        },
    }
    flat = flatten_settings(raw, platform="espn", year=2024, league_key="71580")
    assert flat["roster_SUPER_FLEX"] == 1
    assert flat["roster_IDP"] == 1
    assert flat["roster_FLX"] == 1
    assert flat["roster_W/R"] == 1
    assert flat["roster_REC_FLEX"] == 1
    assert flat["roster_BN"] == 5
    assert flat["roster_IR"] == 2


def test_espn_standard_scoring_defaults_missing_rec_to_zero():
    raw = {
        "scoring_settings": {"pass_td": 4.0, "pass_yd": 0.04},
        "num_teams": 12,
        "playoff_teams": 6,
        "playoff_start_week": 15,
        "regular_season_length": 14,
        "roster_position_counts": {"QB": 1},
    }
    flat = flatten_settings(raw, platform="espn", year=2024, league_key="71580")
    assert flat["scoring_rec"] == 0.0
    assert flat["scoring_type"] == "standard"


# ---------------------------------------------------------------------------
# Sleeper
# ---------------------------------------------------------------------------


def test_sleeper_consolation_populated_bracket():
    raw = {
        "losers_bracket_teams": [7, 8, 9, 10, 11, 12],
        "settings": {"num_teams": 12},
        "total_rosters": 12,
        "playoff_teams": 6,
        "scoring_settings": {},
        "roster_positions": [],
    }
    flat = flatten_settings(raw, platform="sleeper", year=2024, league_key="123")
    assert flat["has_consolation_bracket"] is True
    assert flat["num_playoff_consolation_teams"] == 6


def test_sleeper_consolation_empty_post_season():
    raw = {
        "losers_bracket_teams": [],
        "settings": {"num_teams": 12},
        "total_rosters": 12,
        "playoff_teams": 6,
        "scoring_settings": {},
        "roster_positions": [],
    }
    flat = flatten_settings(raw, platform="sleeper", year=2024, league_key="123")
    assert flat["has_consolation_bracket"] is False
    assert flat["num_playoff_consolation_teams"] == 0


def test_sleeper_consolation_unknown_in_season():
    # Fetcher passes None (not []) when season is in-progress and API returned empty
    raw = {
        "losers_bracket_teams": None,
        "settings": {"num_teams": 12},
        "total_rosters": 12,
        "playoff_teams": 6,
        "scoring_settings": {},
        "roster_positions": [],
    }
    flat = flatten_settings(raw, platform="sleeper", year=2025, league_key="123")
    assert flat["has_consolation_bracket"] is None
    assert flat["num_playoff_consolation_teams"] is None


def test_sleeper_standard_scoring_defaults_missing_rec_to_zero():
    raw = {
        "settings": {"num_teams": 12},
        "total_rosters": 12,
        "playoff_teams": 6,
        "scoring_settings": {"pass_td": 4.0, "pass_yd": 0.04},
        "roster_positions": ["QB", "RB", "WR", "TE"],
    }
    flat = flatten_settings(raw, platform="sleeper", year=2024, league_key="123")
    assert flat["scoring_rec"] == 0.0
    assert flat["scoring_type"] == "standard"
