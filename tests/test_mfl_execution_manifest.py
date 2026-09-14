from scripts.extraplatform_corpus.build_mfl_execution_manifest import (
    select_manifest_rows,
    validate_receipt,
)


def test_selection_uses_observation_season_not_database_terminal_year():
    rows = [
        {"season": 1993, "league_id": "10", "name": "A", "draft_status": "available"},
        {"season": 1994, "league_id": "10", "name": "A", "draft_status": "available"},
    ]

    result = select_manifest_rows(rows, start_year=1993, end_year=1994, per_year=1)

    assert [(r["season"], r["league_id"], r["seed"]) for r in result] == [
        (1993, "10", "1993:10"),
        (1994, "10", "1994:10"),
    ]


def test_selection_fails_when_a_year_has_fewer_than_target_rows():
    rows = [{"season": 2023, "league_id": "7", "name": "Only one"}]

    try:
        select_manifest_rows(rows, start_year=2023, end_year=2023, per_year=10)
    except ValueError as exc:
        assert "2023" in str(exc)
        assert "10" in str(exc)
    else:
        raise AssertionError("selection should fail closed on an underpopulated year")


def test_receipt_requires_actual_season_and_all_required_population_signals():
    manifest = [{"season": 1993, "league_id": "10", "seed": "1993:10"}]
    receipt = [{
        "season": 1993,
        "league_id": "10",
        "db_name": "mfl_1993_10",
        "row_counts": {"player_fantasy": 120, "matchup": 14},
        "non_null_counts": {
            "manager": 120,
            "win": 14,
            "is_playoffs": 4,
            "champion": 1,
            "clutch_equity": 120,
        },
        "schema_columns": {
            "player_fantasy": [
                "db_name", "year", "week", "NFL_player_id", "is_started", "is_rostered",
                "fantasy_points", "win", "champion", "clutch_equity", "manager",
            ],
            "matchup": [
                "db_name", "year", "week", "manager", "team_points", "win", "loss",
                "is_playoffs", "final_playoff_seed", "champion", "is_championship",
            ],
        },
    }]

    assert validate_receipt(manifest, receipt, start_year=1993, end_year=1993, per_year=1) == []


def test_receipt_rejects_terminal_year_mismatch_and_missing_champion_signal():
    manifest = [{"season": 1993, "league_id": "10", "seed": "1993:10"}]
    receipt = [{
        "season": 2024,
        "league_id": "10",
        "db_name": "mfl_1993_10",
        "row_counts": {"player_fantasy": 120, "matchup": 14},
        "non_null_counts": {
            "manager": 120,
            "win": 14,
            "is_playoffs": 4,
            "champion": 0,
            "clutch_equity": 120,
        },
        "schema_columns": {"player_fantasy": [], "matchup": []},
    }]

    errors = validate_receipt(manifest, receipt, start_year=1993, end_year=1993, per_year=1)

    assert any("season" in error and "1993" in error for error in errors)
    assert any("champion" in error for error in errors)
