import pandas as pd
from multi_league.external_ingest.safety import (
    check_ignored_resurfaced,
    IgnoreSnapshot,
    check_mapping_conflict,
)


def test_no_warning_when_ignored_manager_unchanged():
    snapshot = IgnoreSnapshot(row_count=47, tables=["matchup"], years=[2018])
    warnings = check_ignored_resurfaced(
        "TEST_USER",
        snapshot,
        current_row_count=50,
        current_tables=["matchup"],
        current_years=[2018],
    )
    assert warnings == []


def test_warning_when_row_count_doubles():
    snapshot = IgnoreSnapshot(row_count=10, tables=["matchup"], years=[2018])
    warnings = check_ignored_resurfaced(
        "TEST_USER",
        snapshot,
        current_row_count=25,
        current_tables=["matchup"],
        current_years=[2018],
    )
    assert len(warnings) == 1
    assert "previously ignored" in warnings[0].message.lower()


def test_warning_when_new_table_appears():
    snapshot = IgnoreSnapshot(row_count=10, tables=["matchup"], years=[2018])
    warnings = check_ignored_resurfaced(
        "TEST_USER",
        snapshot,
        current_row_count=12,
        current_tables=["matchup", "draft"],
        current_years=[2018],
    )
    assert len(warnings) == 1


def test_warning_when_new_year_appears():
    snapshot = IgnoreSnapshot(row_count=10, tables=["matchup"], years=[2018])
    warnings = check_ignored_resurfaced(
        "TEST_USER",
        snapshot,
        current_row_count=12,
        current_tables=["matchup"],
        current_years=[2018, 2019],
    )
    assert len(warnings) == 1


def test_no_conflict_when_pure_extension():
    """KMFFL pattern: external 2013/2014, canonical starts 2015 — no overlap."""
    canonical_matchup = pd.DataFrame(
        {
            "franchise_id": ["g_1"] * 2,
            "year": [2015, 2015],
            "week": [1, 2],
            "team_points": [100.0, 110.0],
        }
    )
    external_rows = pd.DataFrame(
        {
            "year": [2013, 2013],
            "week": [1, 2],
            "team_points": [120.0, 130.0],
        }
    )
    warnings = check_mapping_conflict("Ezra", "g_1", external_rows, canonical_matchup)
    assert warnings == []


def test_conflict_when_overlap_with_different_points():
    canonical_matchup = pd.DataFrame(
        {
            "franchise_id": ["g_1"],
            "year": [2018],
            "week": [5],
            "team_points": [100.0],
        }
    )
    external_rows = pd.DataFrame({"year": [2018], "week": [5], "team_points": [88.0]})
    warnings = check_mapping_conflict("Brent M", "g_1", external_rows, canonical_matchup)
    assert len(warnings) == 1
    assert "duplicate" in warnings[0].message.lower()


def test_no_conflict_when_overlap_with_matching_points():
    """If points match, it's a same-data re-import, not a conflict."""
    canonical_matchup = pd.DataFrame(
        {
            "franchise_id": ["g_1"],
            "year": [2018],
            "week": [5],
            "team_points": [100.0],
        }
    )
    external_rows = pd.DataFrame({"year": [2018], "week": [5], "team_points": [100.0]})
    warnings = check_mapping_conflict("Brent M", "g_1", external_rows, canonical_matchup)
    assert warnings == []
