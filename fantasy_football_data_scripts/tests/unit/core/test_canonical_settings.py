"""Unit tests for canonical_settings._normalize_scoring."""


def test_normalize_scoring_warns_on_unknown_keys():
    from multi_league.core.canonical_settings import _normalize_scoring
    from multi_league.transformations.player.modules import silent_drop_logger as sdl

    sdl.reset()
    bad = {"rec": 0.5, "definitely_not_canonical": 1.0}
    _normalize_scoring(bad, league="test_league", year=2024)
    drops = sdl.get_drops()
    assert any(
        d["key"] == "definitely_not_canonical" for d in drops
    ), f"expected drop for 'definitely_not_canonical' in {drops}"


def test_yahoo_standard_scoring_defaults_missing_rec_to_zero():
    from multi_league.core.canonical_settings import flatten_settings

    flat = flatten_settings(
        {
            "metadata": {"num_teams": 12},
            "canonical_scoring": {
                "pass_td": 6.0,
                "pass_yd": 0.04,
                "rush_yd": 0.066666666666667,
                "rec_yd": 0.066666666666667,
            },
            "roster_position_counts": {"QB": 1, "RB": 2, "WR": 3},
        },
        platform="yahoo",
        year=2020,
        league_key="242.l.494476",
    )

    assert flat["scoring_rec"] == 0.0
    assert flat["scoring_type"] == "standard"


def test_yahoo_flatten_settings_normalizes_raw_scoring_rules_to_ddl():
    from multi_league.core.canonical_settings import flatten_settings

    flat = flatten_settings(
        {
            "metadata": {"num_teams": 12},
            "scoring_rules": [
                {"stat_id": "66", "name": "Turnover Return Yards", "points": 0.1},
                {"stat_id": "79", "name": "Passing 1st Downs", "points": 0.5},
                {"stat_id": "80", "name": "Receiving 1st Downs", "points": 0.5},
                {"stat_id": "81", "name": "Rushing 1st Downs", "points": 0.5},
            ],
            "roster_position_counts": {"QB": 1, "RB": 2, "WR": 3},
        },
        platform="yahoo",
        year=2025,
        league_key="461.l.12345",
    )

    assert flat["scoring_pass_fd"] == 0.5
    assert flat["scoring_rec_fd"] == 0.5
    assert flat["scoring_rush_fd"] == 0.5
    assert flat["scoring_int_ret_yd"] == 0.1
    assert flat["scoring_fum_ret_yd"] == 0.1


def test_yahoo_flatten_settings_handles_no_playoff_leagues_with_blank_fields():
    from multi_league.core.canonical_settings import flatten_settings

    flat = flatten_settings(
        {
            "metadata": {
                "num_teams": "10",
                "draft_type": "live",
                "start_week": "1",
                "end_week": "18",
                "uses_playoff": "0",
                "playoff_start_week": "",
                "num_playoff_teams": "",
                "num_playoff_consolation_teams": "",
                "bye_teams": "",
            },
            "canonical_scoring": {"rec": "0.5", "pass_td": "4"},
            "roster_position_counts": {"QB": "1", "RB": "2", "WR": "3", "TE": "1", "BN": "5"},
        },
        platform="yahoo",
        year=2025,
        league_key="461.l.279763",
    )

    assert flat["num_teams"] == 10
    assert flat["playoff_teams"] == 0
    assert flat["playoff_start_week"] == 19
    assert flat["regular_season_weeks"] == 18
    assert flat["bye_teams"] == 0
    assert flat["num_playoff_consolation_teams"] == 0
    assert flat["has_consolation_bracket"] is False
    assert flat["scoring_rec"] == 0.5
    assert flat["roster_QB"] == 1


def test_yahoo_flatten_settings_preserves_standard_playoff_fields():
    from multi_league.core.canonical_settings import flatten_settings

    flat = flatten_settings(
        {
            "metadata": {
                "num_teams": "12",
                "start_week": "1",
                "end_week": "17",
                "uses_playoff": "1",
                "playoff_start_week": "15",
                "num_playoff_teams": "6",
                "bye_teams": "2",
                "num_playoff_consolation_teams": "4",
            },
            "canonical_scoring": {"rec": "1", "pass_td": "4"},
            "roster_position_counts": {"QB": "1", "RB": "2", "WR": "2", "TE": "1", "BN": "6"},
        },
        platform="yahoo",
        year=2025,
        league_key="461.l.12345",
    )

    assert flat["num_teams"] == 12
    assert flat["playoff_teams"] == 6
    assert flat["playoff_start_week"] == 15
    assert flat["regular_season_weeks"] == 14
    assert flat["bye_teams"] == 2
    assert flat["num_playoff_consolation_teams"] == 4
    assert flat["has_consolation_bracket"] is True
    assert flat["scoring_type"] == "ppr"


def test_yahoo_flatten_settings_captures_waiver_fields_from_metadata():
    from multi_league.core.canonical_settings import flatten_settings

    faab_flat = flatten_settings(
        {
            "metadata": {
                "num_teams": "12",
                "waiver_type": "FR",
                "uses_faab": "1",
                "trade_end_date": "2025-11-22",
            },
            "canonical_scoring": {"rec": "0", "pass_td": "6"},
            "roster_position_counts": {"QB": "1", "RB": "2", "WR": "2"},
        },
        platform="yahoo",
        year=2025,
        league_key="461.l.12345",
    )
    normal_flat = flatten_settings(
        {
            "metadata": {
                "num_teams": "12",
                "waiver_type": "R",
                "uses_faab": "0",
            },
            "canonical_scoring": {"rec": "0", "pass_td": "6"},
            "roster_position_counts": {"QB": "1", "RB": "2", "WR": "2"},
        },
        platform="yahoo",
        year=2015,
        league_key="348.l.12345",
    )
    code_only_faab_flat = flatten_settings(
        {
            "metadata": {
                "num_teams": "12",
                "waiver_type": "FWR",
            },
            "canonical_scoring": {"rec": "0", "pass_td": "6"},
            "roster_position_counts": {"QB": "1", "RB": "2", "WR": "2"},
        },
        platform="yahoo",
        year=2024,
        league_key="449.l.12345",
    )

    assert faab_flat["waiver_type"] == "faab"
    assert faab_flat["waiver_budget"] == 100
    assert faab_flat["trade_deadline"] is None
    assert normal_flat["waiver_type"] == "normal"
    assert normal_flat["waiver_budget"] == 0
    assert code_only_faab_flat["waiver_type"] == "faab"
    assert code_only_faab_flat["waiver_budget"] == 100


def test_yahoo_metadata_parser_marks_disabled_playoffs_after_end_week():
    from xml.etree import ElementTree as ET

    from multi_league.core.yahoo_league_settings import _parse_league_metadata

    root = ET.fromstring(
        """
        <fantasy_content>
          <league>
            <league_key>461.l.279763</league_key>
            <league_id>279763</league_id>
            <name>2025 FFL</name>
            <num_teams>10</num_teams>
            <start_week>1</start_week>
            <end_week>18</end_week>
            <current_week>18</current_week>
            <season>2025</season>
            <settings>
              <draft_type>live</draft_type>
              <uses_playoff>0</uses_playoff>
              <uses_median_score></uses_median_score>
            </settings>
          </league>
        </fantasy_content>
        """
    )

    metadata = _parse_league_metadata(root)

    assert metadata["uses_playoff"] is False
    assert metadata["playoff_start_week"] == "19"
    assert metadata["num_playoff_teams"] == "0"
    assert metadata["num_playoff_consolation_teams"] == "0"
    assert metadata["playoff_teams"] is None
    assert metadata["bye_teams"] == 0
    assert metadata["num_rounds"] == 0
    assert metadata["championship_week"] is None


def test_yahoo_metadata_parser_preserves_waiver_and_trade_xml_fields():
    from xml.etree import ElementTree as ET

    from multi_league.core.yahoo_league_settings import _parse_league_metadata

    root = ET.fromstring(
        """
        <fantasy_content>
          <league>
            <league_key>461.l.12345</league_key>
            <league_id>12345</league_id>
            <name>2025 FFL</name>
            <num_teams>12</num_teams>
            <start_week>1</start_week>
            <end_week>17</end_week>
            <season>2025</season>
            <settings>
              <draft_type>live</draft_type>
              <uses_playoff>1</uses_playoff>
              <playoff_start_week>15</playoff_start_week>
              <num_playoff_teams>8</num_playoff_teams>
              <num_playoff_consolation_teams>0</num_playoff_consolation_teams>
              <waiver_type>FR</waiver_type>
              <waiver_rule>gametime</waiver_rule>
              <uses_faab>1</uses_faab>
              <waiver_time>2</waiver_time>
              <trade_end_date>2025-11-22</trade_end_date>
              <trade_ratify_type>commish</trade_ratify_type>
              <trade_reject_time>2</trade_reject_time>
            </settings>
          </league>
        </fantasy_content>
        """
    )

    metadata = _parse_league_metadata(root)

    assert metadata["waiver_type"] == "FR"
    assert metadata["waiver_rule"] == "gametime"
    assert metadata["uses_faab"] == "1"
    assert metadata["waiver_time"] == "2"
    assert metadata["trade_end_date"] == "2025-11-22"
    assert metadata["trade_ratify_type"] == "commish"
    assert metadata["trade_reject_time"] == "2"
