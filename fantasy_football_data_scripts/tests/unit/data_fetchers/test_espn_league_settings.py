"""
Unit tests for espn_league_settings._process_scoring_format.
Verifies that ESPN stat IDs are mapped to canonical Sleeper-style keys.
"""

from types import SimpleNamespace


def test_espn_normalizer_produces_canonical_kicker_keys():
    """ESPN per-bracket FG scoring must land in scoring_settings under
    canonical names (fgm_0_39, fgm_40_49, xpm), not legacy names.

    Updated 2026-05-02 per ESPN_STAT_ID_MAP audit:
    - stat_id 77 = "FG Made (40-49 yards)" -> fgm_40_49 (was wrongly fgm_0_39)
    - stat_id 78 = "FG Attempted (40-49 yards)" -> dropped (attempt counter, not scoring)
    - stat_id 80 = "FG Made (0-39 yards)" -> fgm_0_39 (this is the canonical 0-39 made stat_id)
    - stat_id 86 = "Each PAT Made" -> xpm
    """
    from multi_league.data_fetchers.espn.espn_league_settings import _process_scoring_format

    # Mock ESPN scoring_format payload — simulate stat IDs 80, 77, 86
    raw_scoring_format = [
        {"id": 80, "value": 4.0},  # FG Made (0-39 yards) -> fgm_0_39
        {"id": 77, "value": 5.0},  # FG Made (40-49 yards) -> fgm_40_49
        {"id": 86, "value": 1.0},  # Each PAT Made -> xpm
    ]
    scoring_settings = {}
    _process_scoring_format(raw_scoring_format, scoring_settings)

    assert "fgm_0_39" in scoring_settings, f"missing canonical key, got: {list(scoring_settings.keys())}"
    assert scoring_settings["fgm_0_39"] == 4.0
    assert scoring_settings["fgm_40_49"] == 5.0
    assert scoring_settings["xpm"] == 1.0
    # Old non-canonical names must NOT be present
    assert "fg_made_0_39" not in scoring_settings
    assert "fg_made_40_49" not in scoring_settings
    assert "pat_made" not in scoring_settings


def test_espn_normalizer_routes_yardage_kicker_stat_to_fg_yards():
    """ESPN stat_id 214 is field-goal made yards, not return yards."""
    from multi_league.data_fetchers.espn.espn_league_settings import _process_scoring_format

    scoring_settings = {}
    _process_scoring_format([{"id": 214, "value": 0.1}], scoring_settings)

    assert scoring_settings["fgm_yds"] == 0.1
    assert "st_yd" not in scoring_settings


def test_espn_normalizer_routes_fg_miss_to_negative_miss_key():
    """ESPN stat_id 85 is total FG missed, not FG percentage."""
    from multi_league.data_fetchers.espn.espn_league_settings import _process_scoring_format

    scoring_settings = {}
    _process_scoring_format([{"id": 85, "value": -1.0}], scoring_settings)

    assert scoring_settings["fgmiss"] == -1.0
    assert "fg_pct" not in scoring_settings


def test_espn_normalizer_routes_return_scoring_to_player_and_dst_contexts():
    """ESPN return rules score both individual returners and team D/ST."""
    from multi_league.data_fetchers.espn.espn_league_settings import _process_scoring_format

    scoring_settings = {}
    _process_scoring_format(
        [
            {"id": 101, "value": 8.0},
            {"id": 102, "value": 8.0},
            {"id": 114, "value": 0.04},
            {"id": 115, "value": 0.04},
        ],
        scoring_settings,
    )

    assert scoring_settings["def_kr_td"] == 8.0
    assert scoring_settings["def_pr_td"] == 8.0
    assert scoring_settings["st_td"] == 8.0
    assert scoring_settings["def_kr_yd"] == 0.04
    assert scoring_settings["def_pr_yd"] == 0.04
    assert scoring_settings["kr_yd"] == 0.04
    assert scoring_settings["pr_yd"] == 0.04


def test_espn_normalizer_routes_advanced_defense_to_idp_and_dst_contexts():
    """ESPN stuffs/pass-defensed rules apply to IDP and team D/ST scoring."""
    from multi_league.data_fetchers.espn.espn_league_settings import _process_scoring_format

    scoring_settings = {}
    _process_scoring_format(
        [
            {"id": 112, "value": 0.5},
            {"id": 113, "value": 0.25},
        ],
        scoring_settings,
    )

    assert scoring_settings["idp_tkl_loss"] == 0.5
    assert scoring_settings["tkl_loss"] == 0.5
    assert scoring_settings["idp_pass_def"] == 0.25
    assert scoring_settings["def_pass_def"] == 0.25


def test_espn_dict_normalizer_routes_native_return_keys_to_player_and_dst_contexts():
    from multi_league.core.scoring_config import normalize_espn

    scoring_settings = normalize_espn(
        {
            "dst_kickoff_return_td": 8.0,
            "dst_punt_return_td": 8.0,
        }
    )

    assert scoring_settings["def_kr_td"] == 8.0
    assert scoring_settings["def_pr_td"] == 8.0
    assert scoring_settings["st_td"] == 8.0


def test_espn_normalizer_produces_canonical_dst_keys():
    """ESPN DST stat IDs must map to canonical Sleeper-style DST keys."""
    from multi_league.data_fetchers.espn.espn_league_settings import _process_scoring_format

    raw_scoring_format = [
        {"id": 99, "value": 1.0},  # sack → canonical 'sack'
        {"id": 95, "value": 2.0},  # int → canonical 'int'
        {"id": 94, "value": 6.0},  # def_td → canonical 'def_td'
        {"id": 89, "value": 10.0},  # pts_allow_0 → canonical 'pts_allow_0'
    ]
    scoring_settings = {}
    _process_scoring_format(raw_scoring_format, scoring_settings)

    assert scoring_settings.get("sack") == 1.0
    assert scoring_settings.get("int") == 2.0
    assert scoring_settings.get("def_td") == 6.0
    assert scoring_settings.get("pts_allow_0") == 10.0
    # Non-canonical names must not be present
    assert "dst_sacks" not in scoring_settings
    assert "dst_interceptions" not in scoring_settings
    assert "dst_td" not in scoring_settings
    assert "dst_points_allowed_0" not in scoring_settings


def test_espn_normalizer_unknown_stat_id_dropped():
    """Stat IDs not in the canonical map are silently dropped.

    Updated 2026-05-02: stat_id 80 = FG Made (0-39 yards) -> fgm_0_39
    (replacing pre-audit 77, which was wrong).
    """
    from multi_league.data_fetchers.espn.espn_league_settings import _process_scoring_format

    raw_scoring_format = [
        {"id": 9999, "value": 3.0},  # Unknown stat ID
        {"id": 80, "value": 4.0},  # Known: FG Made (0-39 yards) -> fgm_0_39
    ]
    scoring_settings = {}
    _process_scoring_format(raw_scoring_format, scoring_settings)

    # Unknown stat_9999 key must not appear
    assert "stat_9999" not in scoring_settings
    assert len(scoring_settings) == 1
    assert scoring_settings["fgm_0_39"] == 4.0


def test_espn_playoff_metadata_preserves_no_playoff_zero():
    from multi_league.core.espn_playoff_settings import derive_espn_playoff_metadata

    metadata = derive_espn_playoff_metadata(
        playoff_teams=0,
        regular_season_length=12,
        playoff_matchup_period_length=2,
        matchup_periods={str(week): [week] for week in range(1, 13)},
    )

    assert metadata["num_playoff_teams"] == 0
    assert metadata["bye_teams"] == 0
    assert metadata["num_rounds"] == 0
    assert metadata["playoff_start_week"] == 13
    assert metadata["end_week"] == 12
    assert metadata["championship_week"] is None
    assert metadata["has_multiweek_championship"] == 0


def test_espn_playoff_metadata_uses_variable_championship_rounds():
    from multi_league.core.espn_playoff_settings import derive_espn_playoff_metadata

    metadata = derive_espn_playoff_metadata(
        playoff_teams=6,
        regular_season_length=13,
        playoff_matchup_period_length=0,
        matchup_periods={
            "1": [1],
            "13": [13],
            "14": [14],
            "15": [15],
            "16": [16, 17],
        },
    )

    assert metadata["bye_teams"] == 2
    assert metadata["num_rounds"] == 3
    assert metadata["playoff_round_lengths"] == [1, 1, 2]
    assert metadata["playoff_round_type"] == 2
    assert metadata["end_week"] == 17
    assert metadata["championship_week"] == 17
    assert metadata["has_multiweek_championship"] == 1


def test_espn_flatten_settings_handles_preprocessed_no_playoff_year():
    from multi_league.core.canonical_settings import flatten_settings

    flat = flatten_settings(
        {
            "scoring_settings": {"rec": 0},
            "roster_position_counts": {"QB": 1},
            "playoff_teams": 0,
            "num_playoff_teams": 0,
            "regular_season_length": 12,
            "playoff_start_week": 13,
            "playoff_matchup_period_length": 2,
            "matchup_periods": {str(week): [week] for week in range(1, 13)},
        },
        platform="espn",
        year=2018,
        league_key="971000",
    )

    assert flat["playoff_teams"] == 0
    assert flat["bye_teams"] == 0
    assert flat["playoff_start_week"] == 13
    assert flat["regular_season_weeks"] == 12
    assert flat["end_week"] == 12
    assert flat["has_multiweek_championship"] is False


def test_espn_flatten_settings_handles_variable_championship_round():
    from multi_league.core.canonical_settings import flatten_settings

    flat = flatten_settings(
        {
            "scoring_settings": {"rec": 1},
            "roster_position_counts": {"QB": 1},
            "playoff_teams": 6,
            "num_playoff_teams": 6,
            "regular_season_length": 13,
            "playoff_matchup_period_length": 0,
            "matchup_periods": {
                "1": [1],
                "13": [13],
                "14": [14],
                "15": [15],
                "16": [16, 17],
            },
        },
        platform="espn",
        year=2024,
        league_key="971000",
    )

    assert flat["playoff_teams"] == 6
    assert flat["bye_teams"] == 2
    assert flat["playoff_start_week"] == 14
    assert flat["regular_season_weeks"] == 13
    assert flat["end_week"] == 17
    assert flat["has_multiweek_championship"] is True


def test_fetch_espn_settings_captures_raw_playoff_seeding_rule(monkeypatch):
    from multi_league.data_fetchers.espn.espn_league_settings import fetch_espn_settings

    class _FakeCtx:
        espn_s2 = None
        swid = None
        league_name = "Demo"
        num_teams = 4

        def get_league_id_for_year(self, year):
            assert year == 2024
            return 971000

    league_settings = SimpleNamespace(
        name="Demo",
        position_slot_counts={"QB": 1},
        playoff_team_count=4,
        reg_season_count=13,
        playoff_matchup_period_length=1,
        matchup_periods={},
        keeper_count=0,
        faab=False,
        acquisition_budget=0,
        trade_deadline=None,
        scoring_format=[],
    )
    fake_league = SimpleNamespace(settings=league_settings, teams=[1, 2, 3, 4], draft=[])

    class _FakeClient:
        def __init__(self, *_args, **_kwargs):
            pass

        def get_league(self, year):
            assert year == 2024
            return fake_league

        def get_league_settings_raw(self, year):
            assert year == 2024
            return {
                "scheduleSettings": {
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
            }

    monkeypatch.setattr("multi_league.data_fetchers.espn.espn_api_client.ESPNAPIClient", _FakeClient)

    settings = fetch_espn_settings(_FakeCtx(), 2024)

    assert settings["playoff_bracket_source"] == "api"
    assert settings["playoff_seeding_rule"] == "TOTAL_POINTS_SCORED"
    assert settings["playoff_seeding_rule_by"] == 0
    assert settings["home_team_bonus"] == 1
    assert settings["playoff_home_team_bonus"] == 3
    assert settings["matchup_tie_rule"] == "NONE"
    assert settings["playoff_matchup_tie_rule"] == "NONE"
