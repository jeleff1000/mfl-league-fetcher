from __future__ import annotations

import pandas as pd

from scripts.sota_recon.materialize_resolved_newspaper_events import materialize_resolved_events


def test_materializes_unique_role_and_holds_partially_resolved_event() -> None:
    tasks = pd.DataFrame(
        [
            {
                "identity_task_id": "score",
                "source_surface": "scoring_event",
                "boxscore_id": "192111270chi",
                "role": "scoring_player",
                "raw_player": "Halas",
            },
            {
                "identity_task_id": "primary",
                "source_surface": "play_by_play_event",
                "boxscore_id": "192111200chi",
                "role": "primary_player",
                "raw_player": "Huffine",
            },
        ]
    )
    ledger = pd.DataFrame(
        [
            {"identity_task_id": "score", "verdict": "resolved_external_witness_corroborated", "resolved_nfl_id": "HalaGe20", "resolved_player": "George Halas"},
            {"identity_task_id": "primary", "verdict": "resolved_external_witness_corroborated", "resolved_nfl_id": "HuffKe21", "resolved_player": "Ken Huffine"},
        ]
    )
    expanded = pd.DataFrame(
        [
            {
                "target_table": "scoring_event",
                "target_entity_key": "score-event",
                "boxscore_id": "192111270chi",
                "eff_created_at_utc": "2026-06-01T00:00:00Z",
                "eff_scoring_player_raw": "Halas",
                "eff_scoring_NFL_player_id": None,
                "eff_scoring_team": "CHI",
                "eff_event_type": "rushing_touchdown",
                "eff_points": 6,
            },
            {
                "target_table": "play_by_play_event",
                "target_entity_key": "pbp-event",
                "boxscore_id": "192111200chi",
                "eff_created_at_utc": "2026-06-01T00:00:00Z",
                "eff_primary_player_raw": "Huffine",
                "eff_primary_NFL_player_id": None,
                "eff_secondary_player_raw": "Unknown",
                "eff_secondary_NFL_player_id": None,
                "eff_play_type": "lateral",
            },
        ]
    )

    scoring, pbp, holds, summary = materialize_resolved_events(tasks, ledger, expanded)

    assert scoring.iloc[0]["scoring_NFL_player_id"] == "HalaGe20"
    assert scoring.iloc[0]["identity_resolution"] == "ledger_resolved_external_witness_corroborated"
    assert pbp.empty
    assert holds.iloc[0]["target_entity_key"] == "pbp-event"
    assert "secondary_player" in holds.iloc[0]["unresolved_roles_json"]
    assert summary == {"scoring_events": 1, "play_by_play_events": 0, "partial_holds": 1, "collisions": 0}


def test_receiving_scorer_resolution_populates_matching_receiver_role() -> None:
    tasks = pd.DataFrame(
        [
            {
                "identity_task_id": "receiver",
                "source_surface": "scoring_event",
                "boxscore_id": "192212100tol",
                "role": "receiver",
                "raw_player": "Chamberlain",
            }
        ]
    )
    ledger = pd.DataFrame(
        [
            {
                "identity_task_id": "receiver",
                "verdict": "resolved_external_witness_corroborated",
                "resolved_nfl_id": "ChamGa20",
                "resolved_player": "Garth Chamberlain",
            }
        ]
    )
    expanded = pd.DataFrame(
        [
            {
                "target_table": "scoring_event",
                "target_entity_key": "receiving-event",
                "boxscore_id": "192212100tol",
                "eff_created_at_utc": "2026-06-01T00:00:00Z",
                "eff_scoring_player_raw": "Chamberlain",
                "eff_receiver_raw": "Chamberlain",
                "eff_event_type": "receiving_touchdown",
                "eff_points": 6,
            }
        ]
    )

    scoring, _, holds, _ = materialize_resolved_events(tasks, ledger, expanded)

    assert holds.empty
    assert scoring.iloc[0]["scoring_NFL_player_id"] == "ChamGa20"
    assert scoring.iloc[0]["receiver_NFL_player_id"] == "ChamGa20"
