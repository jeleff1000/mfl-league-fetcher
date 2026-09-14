"""Shape tests for the player-log weekly measurement lane."""

from __future__ import annotations

from .nflcom_player_logs_weekly_witness import (
    _close_to_published,
    _passer_rating,
    _layout_expression,
    _defense_era,
    measure_defense_source_identity,
    score_candidate_pairs,
    _target_join_predicate,
)


def test_published_rate_tolerance_uses_source_precision():
    assert _close_to_published("34.3", 34.3333333333)
    assert _close_to_published("8.4", 262 / 31)
    assert not _close_to_published("34.3", 34.4)
    assert _close_to_published("100", 100.0)


def test_passer_rating_recompute_matches_the_standard_formula():
    value = _passer_rating(20, 30, 250, 2, 1)
    assert value is not None
    assert round(value, 1) == 100.7


def test_passer_rating_has_no_value_when_attempts_are_zero():
    assert _passer_rating(0, 0, 0, 0, 0) is None


def test_targeted_logs_use_date_key_not_a_guessed_week_offset():
    predicate = _target_join_predicate(True, "REG", 0)
    assert "CAST(v.game_date AS DATE)=s.parsed_date" in predicate
    assert "v.week" not in predicate


def test_current_logs_keep_the_explicit_week_offset_witness():
    predicate = _target_join_predicate(False, "REG", 0)
    assert "v.week=TRY_CAST(s.wk AS INT)+0" in predicate
    assert "s.parsed_date" not in predicate


def test_all_year_raw_logs_use_date_opponent_key_not_week_translation():
    predicate = _target_join_predicate(False, "POST", 18, date_keyed=True)
    assert "v.year=TRY_CAST(s.season AS INT)" in predicate
    assert "CAST(v.game_date AS DATE)=s.parsed_date" in predicate
    assert "v.week" not in predicate


def test_targeted_logs_use_the_captured_layout_axis():
    assert _layout_expression(True) == "replace(_layout, 'id_gamelog:', '')"
    assert "TRY_CAST(comp AS DOUBLE)" in _layout_expression(False)


def test_candidate_score_excludes_unknown_target_from_informative_denominator():
    assert score_candidate_pairs([(2, None), (0, 0), (2, 1), (1, 2)]) == {
        "joined_n": 4,
        "target_available_n": 3,
        "informative_n": 2,
        "agree_n": 0,
        "source_exceeds_target": 1,
        "target_exceeds_source": 1,
        "source_nonzero_n": 3,
        "target_nonzero_n": 2,
    }


def test_defense_era_boundaries_match_recording_law():
    assert [_defense_era(y) for y in (1981, 1982, 1993, 1994, 1999, 2000, 2009, 2010)] == [
        "<1982", "1982-93", "1982-93", "1994-99", "1994-99", "2000-09",
        "2000-09", "2010+",
    ]


def test_defense_source_identity_records_complete_denominator_and_directions():
    import duckdb

    con = duckdb.connect()
    con.execute("""CREATE TABLE src_scope(
        season VARCHAR, recovered_layout VARCHAR, total VARCHAR, solo VARCHAR, ast VARCHAR
    )""")
    con.executemany("INSERT INTO src_scope VALUES (?, ?, ?, ?, ?)", [
        ("2000", "DEF_log", "7", "5", "2"),
        ("2000", "DEF_log", "8", "5", "2"),
        ("2000", "DEF_log", "6", "5", "2"),
        ("2000", "DEF_log", "7", None, "2"),
    ])
    rows = measure_defense_source_identity(con)
    assert rows[3] == {
        "era": "2000-09", "complete_n": 3, "agree_n": 1,
        "source_exceeds_parts": 1, "parts_exceed_source": 1,
    }
    con.close()
