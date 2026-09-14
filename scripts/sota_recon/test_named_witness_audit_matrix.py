from __future__ import annotations

import pytest

from .named_witness_audit_matrix import build_rows, named_source
from .witness_map import MapSpec
from .witness_map import WITNESS_MAP


def _row(**overrides):
    row = {
        "source": "nflcom_player_season",
        "lineage": "nflcom",
        "regime": "MULTI_TABLE",
        "table_key": "passing|reg",
        "column": "pass_yds",
        "disposition": "MAPPED_TO_CANONICAL",
        "canonical": "passing_yards",
        "reason": "adjudicated",
        "closed_by": "adjudication",
    }
    row.update(overrides)
    return row


def _contracts():
    return {
        "passing_yards": {
            "canonical_name": "passing_yards",
            "natural_grain": "player_game",
            "aggregation_class": "SUM",
            "unit": "yards",
            "tolerance_policy": {"policy": "EXACT"},
        }
    }


def test_named_source_is_exactly_the_requested_five_families():
    assert named_source("nflcom_player_logs")
    assert named_source("statscrew_team_season_stats")
    assert named_source("pfr_player_kicking")
    assert named_source("pfa_player_game_participation")
    assert named_source("ancient_pfa_gamelog")
    assert named_source("ngs_weekly_raw")
    assert not named_source("pbp_player_week_rollup")
    assert not named_source("newspaper_player_cells")


def test_mapped_row_without_runner_is_declared_only_not_executable():
    rows = build_rows([_row()], [], _contracts(), {}, {})
    assert rows[0]["mapping_status"] == "DECLARED_ONLY"
    assert rows[0]["executor_id"] is None
    assert rows[0]["canonical_unit"] == "yards"
    assert rows[0]["canonical_aggregation"] == "SUM"


def test_legacy_mapspec_is_an_executable_route_with_full_semantics():
    source_row = _row(
        source="pfr_player_season_passing",
        lineage="pfr",
        table_key="*",
        column="pass_yds",
    )
    spec = MapSpec(
        "pfr_player_season_passing", "passing_yards", "pass_yds", "pages"
    )
    rows = build_rows([source_row], [spec], _contracts(), {}, {})
    assert rows[0]["mapping_status"] == "EXECUTABLE"
    assert rows[0]["executor_id"] == "witness_map.validate"
    assert rows[0]["source_aggregation"] == "sum"
    assert rows[0]["source_scale"] == 1.0
    assert rows[0]["comparison_grain"] == "season"


def test_exact_executor_catalog_can_activate_a_table_specific_mapping():
    key = "nflcom_player_season|passing|reg|pass_yds"
    executor = {
        key: {
            "executor_id": "nflcom_column_audit",
            "comparison_grain": "season",
            "source_aggregation": "sum",
            "key_route": "nflcom_slug_pfrid",
            "crosswalk_receipt": "nflcom_slug_pfrid",
        }
    }
    rows = build_rows([_row()], [], _contracts(), executor, {})
    assert rows[0]["mapping_status"] == "EXECUTABLE"
    assert rows[0]["executor_id"] == "nflcom_column_audit"
    assert rows[0]["crosswalk_receipt"] == "nflcom_slug_pfrid"


def test_mapspec_and_ledger_target_disagreement_fails_loudly():
    source_row = _row(
        source="pfr_player_season_passing", lineage="pfr", table_key="*"
    )
    wrong = MapSpec(
        "pfr_player_season_passing", "passing_tds", "pass_yds", "pages"
    )
    with pytest.raises(ValueError, match="contradicts dossier"):
        build_rows([source_row], [wrong], _contracts(), {}, {})


def test_pfr_games_map_targets_the_real_season_canonical():
    spec = next(row for row in WITNESS_MAP if row.source_key == "pfr_games_played")
    assert spec.v26_col == "games_played"
    assert spec.agg == "value"
    assert spec.v26_expr == ""


@pytest.mark.parametrize(
    ("disposition", "expected"),
    [
        ("NEW_SUPERTABLE_COLUMN_CANDIDATE", "CANDIDATE_NO_TARGET"),
        ("EXCLUDED_WITH_REASON", "EXCLUDED_NON_WITNESS"),
        ("EXCLUDED_KEY_COLUMN", "EXCLUDED_NON_WITNESS"),
        ("DUPLICATE_OF", "DUPLICATE"),
        ("OPEN", "OPEN_UNADJUDICATED"),
    ],
)
def test_non_mapped_dispositions_have_truthful_routes(disposition, expected):
    row = _row(disposition=disposition, canonical=None)
    result = build_rows([row], [], _contracts(), {}, {}, {})[0]
    assert result["mapping_status"] == expected
    assert result["executor_id"] is None


def test_lane_catalog_routes_non_column_witnesses_without_calling_them_mapspecs():
    row = _row(
        source="pfa_player_game_participation",
        lineage="pfa_loc",
        table_key="*",
        column="position",
        disposition="OPEN",
        canonical=None,
    )
    lane_routes = {
        "pfa_player_game_participation": {
            "executor_id": "entity_universes.appearance",
            "comparison_grain": "game",
            "key_route": "season+team+game_id+player",
        }
    }
    result = build_rows([row], [], _contracts(), {}, lane_routes, {})[0]
    assert result["mapping_status"] == "LANE_EXECUTABLE"
    assert result["executor_id"] == "entity_universes.appearance"


def test_named_blocker_is_not_reported_as_unadjudicated_or_executable():
    row = _row(
        source="nflcom_player_logs_targeted",
        table_key="REG|DEF",
        column="fr",
        disposition="OPEN",
        canonical=None,
    )
    key = "nflcom_player_logs_targeted|REG|DEF|fr"
    blockers = {
        key: {
            "generator": "nflcom_column_adjudication",
            "question": "Does FR mean fumble recoveries or fumbles recovered?",
        }
    }
    result = build_rows([row], [], _contracts(), {}, {}, blockers)[0]
    assert result["mapping_status"] == "BLOCKED_NAMED"
    assert result["blocker_generator"] == "nflcom_column_adjudication"
    assert result["blocker_question"].startswith("Does FR")
    assert result["executor_id"] is None
