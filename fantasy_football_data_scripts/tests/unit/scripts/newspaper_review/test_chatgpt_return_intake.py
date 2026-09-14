from __future__ import annotations

import json

from scripts.newspaper_review.chatgpt_return_intake import (
    normalize_rows,
    source_dedupe_disposition,
)


def _row(**overrides: str) -> dict[str, str]:
    row = {
        "packet_id": "queue-076",
        "boxscore_id": "193611150crd",
        "source_document_id": "193611150crd#2:182145061",
        "evidence_file": "page.pdf",
        "finding_type": "missing",
        "proposed_atom_type": "game_final_score",
        "proposed_value": json.dumps(
            {
                "away_team": "Pittsburgh Pirates",
                "away_score": 6,
                "home_team": "Chicago Cardinals",
                "home_score": 14,
            }
        ),
        "confidence": "high",
        "reason": "Printed final score.",
        "needs_visual_adjudication": "false",
        "return_file": "return.csv",
        "return_row_number": "2",
        "queue_number_from_filename": "76",
    }
    row.update(overrides)
    return row


def test_final_score_expands_each_cited_source_document_and_routes_to_game_candidate():
    rows = normalize_rows([
        _row(source_document_id="193611150crd#2:182145061;193611150crd#3:721779982")
    ])

    assert len(rows) == 2
    assert {row["source_document_id"] for row in rows} == {
        "193611150crd#2:182145061",
        "193611150crd#3:721779982",
    }
    assert {row["target_table"] for row in rows} == {"game_candidate"}
    assert {row["normalization_status"] for row in rows} == {"routed_structured"}
    fields = json.loads(rows[0]["candidate_fields_json"])
    assert fields == {
        "away_score": 6,
        "away_team": "Pittsburgh Pirates",
        "home_score": 14,
        "home_team": "Chicago Cardinals",
    }


def test_conflicting_return_is_held_even_when_its_atom_type_is_known():
    rows = normalize_rows([
        _row(finding_type="contradictory", proposed_atom_type="scoring_play.field_goal")
    ])

    assert len(rows) == 1
    assert rows[0]["target_table"] == "scoring_event"
    assert rows[0]["normalization_status"] == "hold_source_conflict"
    assert rows[0]["apply_eligibility"] == "never_direct_apply"


def test_textual_turnover_routes_to_play_by_play_and_is_preserved_verbatim():
    rows = normalize_rows([
        _row(
            proposed_atom_type="turnover",
            proposed_value="Q2 Monnett fumbled a lateral; Bill Smith recovered for Cardinals.",
        )
    ])

    assert len(rows) == 1
    assert rows[0]["target_table"] == "play_by_play_event"
    assert rows[0]["normalization_status"] == "routed_textual"
    assert rows[0]["proposed_value"] == "Q2 Monnett fumbled a lateral; Bill Smith recovered for Cardinals."


def test_unrecognized_type_is_retained_as_an_explicit_hold_not_dropped():
    rows = normalize_rows([_row(proposed_atom_type="mystery_newspaper_fact")])

    assert len(rows) == 1
    assert rows[0]["target_table"] == ""
    assert rows[0]["normalization_status"] == "hold_unrecognized_type"
    assert rows[0]["apply_eligibility"] == "never_direct_apply"


def test_exact_same_source_final_score_is_classified_as_a_duplicate():
    candidate = normalize_rows([_row()])[0]
    existing_rows = [
        {
            "target_table": "game_candidate",
            "source_document_id": "193611150crd#2:182145061",
            "team_1_raw": "Chicago Cardinals",
            "team_1_score": "14",
            "team_2_raw": "Pittsburgh Pirates",
            "team_2_score": "6",
        }
    ]

    assert source_dedupe_disposition(candidate, existing_rows) == "exact_source_duplicate"


def test_nonmatching_or_textual_candidates_remain_in_visual_semantic_dedupe():
    structured = normalize_rows([_row()])[0]
    textual = normalize_rows([
        _row(proposed_atom_type="turnover", proposed_value="Monnett fumbled a lateral.")
    ])[0]

    assert source_dedupe_disposition(structured, []) == "visual_semantic_dedupe_required"
    assert source_dedupe_disposition(textual, []) == "visual_semantic_dedupe_required"


def test_text_final_score_extracts_codes_for_an_exact_source_duplicate_check():
    candidate = normalize_rows([
        _row(proposed_atom_type="final_score", proposed_value="CRD 0, GNB 15")
    ])[0]
    existing_rows = [
        {
            "target_table": "game_candidate",
            "source_document_id": "193611150crd#2:182145061",
            "team_1_resolved": "GNB",
            "team_1_score": "15",
            "team_2_resolved": "CRD",
            "team_2_score": "0",
        }
    ]

    assert json.loads(candidate["candidate_fields_json"]) == {
        "team_1_raw": "CRD",
        "team_1_score": "0",
        "team_2_raw": "GNB",
        "team_2_score": "15",
    }
    assert source_dedupe_disposition(candidate, existing_rows) == "exact_source_duplicate"


def test_exact_structured_scoring_event_is_classified_as_a_same_source_duplicate():
    candidate = normalize_rows([
        _row(
            proposed_atom_type="scoring_play.field_goal",
            proposed_value=json.dumps(
                {"team": "GNB", "kicker": "Hinkle", "event": "field_goal", "quarter": 2}
            ),
        )
    ])[0]
    existing_rows = [
        {
            "target_table": "scoring_event",
            "source_document_id": "193611150crd#2:182145061",
            "scoring_team": "GNB",
            "scoring_team_raw": "Green Bay Packers",
            "scoring_player_raw": "Hinkle",
            "event_type": "field_goal",
            "period_raw": "second period",
        }
    ]

    assert source_dedupe_disposition(candidate, existing_rows) == "exact_source_duplicate"


def test_scoring_count_summary_is_not_collapsed_into_one_existing_scoring_event():
    candidate = normalize_rows([
        _row(
            proposed_atom_type="scoring_event",
            proposed_value=json.dumps(
                {"team": "GNB", "player": "Hinkle", "event": "touchdown", "count": 2}
            ),
        )
    ])[0]
    existing_rows = [
        {
            "target_table": "scoring_event",
            "source_document_id": "193611150crd#2:182145061",
            "scoring_team": "GNB",
            "scoring_player_raw": "Hinkle",
            "event_type": "rushing_touchdown",
            "period_raw": "second period",
        }
    ]

    assert source_dedupe_disposition(candidate, existing_rows) == "visual_semantic_dedupe_required"


def test_player_touchdown_summary_matches_an_equal_count_of_source_scoring_events():
    candidate = normalize_rows([
        _row(
            proposed_atom_type="player_touchdowns",
            proposed_value=json.dumps({"team": "GNB", "player": "Laws", "touchdowns": 1}),
        )
    ])[0]
    existing_rows = [
        {
            "target_table": "scoring_event",
            "source_document_id": "193611150crd#2:182145061",
            "scoring_team": "GNB",
            "scoring_player_raw": "Laws",
            "event_type": "touchdown",
        }
    ]

    assert source_dedupe_disposition(candidate, existing_rows) == "exact_source_duplicate"


def test_team_first_down_side_matches_the_existing_paired_source_claim():
    candidate = normalize_rows([
        _row(
            proposed_atom_type="team_first_downs",
            proposed_value=json.dumps({"team": "GNB", "value": 15}),
        )
    ])[0]
    existing_rows = [
        {
            "target_table": "team_game_stat_claim",
            "source_document_id": "193611150crd#2:182145061",
            "stat_name": "first_downs",
            "team_1_nfl_team": "GNB",
            "team_1_value": "15",
            "team_2_nfl_team": "STL",
            "team_2_value": "13",
        }
    ]

    assert source_dedupe_disposition(candidate, existing_rows) == "exact_source_duplicate"
