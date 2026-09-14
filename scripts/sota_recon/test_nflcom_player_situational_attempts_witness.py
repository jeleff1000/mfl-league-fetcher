from .nflcom_player_situational_attempts_witness import (
    ATTEMPTS_LAYOUTS,
    CANDIDATE_COLUMNS,
    FIELD_POSITION_CANDIDATES,
    FIELD_POSITION_LAYOUTS,
    HOME_VS_ROAD_LAYOUTS,
    STADIUM_SURFACES_LAYOUTS,
    QUARTERS_EXCLUDED_SPLITS,
    QUARTERS_LAYOUTS,
    GAME_HALVES_EXCLUDED_SPLITS,
    GAME_HALVES_LAYOUTS,
    MARGIN_OF_VICTORY_LAYOUTS,
    POINT_DIFFERENTIAL_EXCLUDED_SPLITS,
    POINT_DIFFERENTIAL_LAYOUTS,
    dimension_where,
    identity_checks,
    check_capture_recoverability,
    _candidate_measurement,
    witness_form,
)

import duckdb


def test_attempts_layouts_are_the_two_stat_blocks():
    assert ATTEMPTS_LAYOUTS == {
        "player_situational_L3": "rushing",
        "player_situational_L5": "passing",
    }


def test_attempts_counts_sum_across_attempt_buckets_and_longs_use_max():
    assert witness_form("player_situational_L3", "yds") == "SUM"
    assert witness_form("player_situational_L3", "lng") == "MAX"
    assert witness_form("player_situational_L5", "sck") == "SUM"


def test_attempts_candidate_sets_keep_natural_columns_first():
    assert CANDIDATE_COLUMNS[("player_situational_L5", "1st")][0] == "passing_first_downs"
    assert CANDIDATE_COLUMNS[("player_situational_L5", "sck")][0] == "sacks_suffered"
    assert CANDIDATE_COLUMNS[("player_situational_L3", "att")][0] == "carries"


def test_field_position_layouts_and_candidates_use_their_native_families():
    assert FIELD_POSITION_LAYOUTS["player_situational_L0"] == "defense"
    assert FIELD_POSITION_LAYOUTS["player_situational_L2"] == "receiving"
    assert FIELD_POSITION_LAYOUTS["player_situational_L7"] == "field-goals"
    assert FIELD_POSITION_CANDIDATES[("player_situational_L0", "solo")][0] == "def_tackles_solo"
    assert FIELD_POSITION_CANDIDATES[("player_situational_L1", "opp_fr")][0] == "fumble_recovery_opp"


def test_home_vs_road_uses_the_same_eight_native_layouts():
    assert HOME_VS_ROAD_LAYOUTS == {
        "player_situational_L0": "defense",
        "player_situational_L1": "fumbles",
        "player_situational_L2": "receiving",
        "player_situational_L3": "rushing",
        "player_situational_L4": "returns",
        "player_situational_L5": "passing",
        "player_situational_L6": "punts",
        "player_situational_L7": "field-goals",
    }


def test_stadium_surfaces_uses_the_same_eight_native_layouts():
    assert STADIUM_SURFACES_LAYOUTS == {
        "player_situational_L0": "defense",
        "player_situational_L1": "fumbles",
        "player_situational_L2": "receiving",
        "player_situational_L3": "rushing",
        "player_situational_L4": "returns",
        "player_situational_L5": "passing",
        "player_situational_L6": "punts",
        "player_situational_L7": "field-goals",
    }


def test_quarters_excludes_only_the_nested_split_from_the_witness_denominator():
    assert QUARTERS_EXCLUDED_SPLITS == ("4th Quarter within 7",)
    assert dimension_where("Quarters") == (
        "_table='Quarters' AND split_value <> '4th Quarter within 7'"
    )
    assert dimension_where("Home vs Road") == "_table='Home vs Road'"
    assert len(QUARTERS_LAYOUTS) == 8


def test_game_halves_excludes_only_the_nested_last_two_minutes_split():
    assert GAME_HALVES_EXCLUDED_SPLITS == ("Last Two Minutes of Half",)
    assert dimension_where("Game Halves") == (
        "_table='Game Halves' AND split_value <> 'Last Two Minutes of Half'"
    )
    assert len(GAME_HALVES_LAYOUTS) == 8


def test_margin_of_victory_is_a_three_band_partition():
    assert MARGIN_OF_VICTORY_LAYOUTS == GAME_HALVES_LAYOUTS
    assert dimension_where("Margin of Victory") == "_table='Margin of Victory'"


def test_point_differential_excludes_both_nested_detail_bands():
    assert POINT_DIFFERENTIAL_EXCLUDED_SPLITS == (
        "Ahead by 1-8 Points",
        "Ahead by 9-16 Points",
        "Behind by 1-8 Points",
        "Behind by 9-16 Points",
    )
    assert dimension_where("Point Differential") == (
        "_table='Point Differential' AND split_value NOT IN "
        "('Ahead by 1-8 Points', 'Ahead by 9-16 Points', "
        "'Behind by 1-8 Points', 'Behind by 9-16 Points')"
    )
    assert POINT_DIFFERENTIAL_LAYOUTS == GAME_HALVES_LAYOUTS


def test_new_dimensions_total_identity_is_available_as_a_cross_check():
    con = duckdb.connect()
    con.execute(
        """CREATE TEMP TABLE src(
            _table VARCHAR, _layout VARCHAR, total VARCHAR, solo VARCHAR, ast VARCHAR
        )"""
    )
    con.execute(
        """INSERT INTO src VALUES
        ('Margin of Victory', 'player_situational_L0', '11', '7', '4'),
        ('Point Differential', 'player_situational_L0', '4', '4', '0')"""
    )
    for dimension in ('Margin of Victory', 'Point Differential'):
        result = identity_checks(con, dimension)
        assert result['complete_n'] == 1
        assert result['equal_n'] == 1


def test_field_position_total_identity_is_available_as_a_cross_check():
    con = duckdb.connect()
    con.execute(
        """CREATE TEMP TABLE src(
            _table VARCHAR, _layout VARCHAR, total VARCHAR, solo VARCHAR, ast VARCHAR
        )"""
    )
    con.execute(
        """INSERT INTO src VALUES
        ('Field Position', 'player_situational_L0', '7', '5', '2'),
        ('Field Position', 'player_situational_L0', '3', '3', '0')"""
    )
    result = identity_checks(con, 'Field Position')
    assert result['complete_n'] == 2
    assert result['equal_n'] == 2


def test_stadium_surfaces_total_identity_is_available_as_a_cross_check():
    con = duckdb.connect()
    con.execute(
        """CREATE TEMP TABLE src(
            _table VARCHAR, _layout VARCHAR, total VARCHAR, solo VARCHAR, ast VARCHAR
        )"""
    )
    con.execute(
        """INSERT INTO src VALUES
        ('Stadium Surfaces', 'player_situational_L0', '8', '6', '2'),
        ('Stadium Surfaces', 'player_situational_L0', '4', '4', '0')"""
    )
    result = identity_checks(con, 'Stadium Surfaces')
    assert result['complete_n'] == 2
    assert result['equal_n'] == 2


def test_quarters_total_identity_is_available_as_a_cross_check():
    con = duckdb.connect()
    con.execute(
        """CREATE TEMP TABLE src(
            _table VARCHAR, _layout VARCHAR, total VARCHAR, solo VARCHAR, ast VARCHAR
        )"""
    )
    con.execute(
        """INSERT INTO src VALUES
        ('Quarters', 'player_situational_L0', '9', '7', '2'),
        ('Quarters', 'player_situational_L0', '5', '5', '0')"""
    )
    result = identity_checks(con, 'Quarters')
    assert result['complete_n'] == 2
    assert result['equal_n'] == 2


def test_game_halves_total_identity_is_available_as_a_cross_check():
    con = duckdb.connect()
    con.execute(
        """CREATE TEMP TABLE src(
            _table VARCHAR, _layout VARCHAR, total VARCHAR, solo VARCHAR, ast VARCHAR
        )"""
    )
    con.execute(
        """INSERT INTO src VALUES
        ('Game Halves', 'player_situational_L0', '10', '8', '2'),
        ('Game Halves', 'player_situational_L0', '6', '5', '1')"""
    )
    result = identity_checks(con, 'Game Halves')
    assert result['complete_n'] == 2
    assert result['equal_n'] == 2


def test_candidate_denominator_keeps_a_joined_unknown_target_value():
    con = duckdb.connect()
    con.execute(
        """CREATE TEMP TABLE src(
            nflcom_slug VARCHAR, season VARCHAR, _table VARCHAR, _layout VARCHAR, sck VARCHAR
        )"""
    )
    con.execute("INSERT INTO src VALUES ('p', '2025', 'Attempts', 'player_situational_L5', '5')")
    con.execute("CREATE TEMP TABLE ids(nflcom_slug VARCHAR, NFL_player_id VARCHAR)")
    con.execute("INSERT INTO ids VALUES ('p', 'id')")
    con.execute(
        """CREATE TEMP TABLE v26(
            NFL_player_id VARCHAR, year INTEGER, season_type VARCHAR, sacks_suffered DOUBLE
        )"""
    )
    con.execute("INSERT INTO v26 VALUES ('id', 2025, 'REG', NULL)")

    result = _candidate_measurement(
        con, 'Attempts', 'player_situational_L5', 'sck', 'sacks_suffered'
    )

    assert result['joined_n'] == 1
    assert result['informative_n'] == 1
    assert result['agree_n'] == 0
    assert result['source_nonzero_n'] == 1
    assert result['target_nonzero_n'] == 0


def test_selected_situational_fields_do_not_use_the_layout_lost_field():
    result = check_capture_recoverability(
        {
            "player_situational_L0.total": [],
            "player_situational_L0.lng": [],
            "player_situational_L1.fum": [],
            "player_situational_L5.sck": [],
        },
        {
            "player_situational_L0": {"lng": 10},
            "player_situational_L1": {"td": 10},
            "player_situational_L5": {"rate": 10},
        },
    )
    by_layout = {row["layout"]: row for row in result}
    assert by_layout["player_situational_L0"]["status"] == "PROOF_PENDING_CAPTURE"
    assert by_layout["player_situational_L0"]["lost_witness_columns"] == ["lng"]
    assert by_layout["player_situational_L1"]["status"] == "RECOVERABLE_WITNESS_FIELDS"
    assert by_layout["player_situational_L5"]["status"] == "RECOVERABLE_WITNESS_FIELDS"
