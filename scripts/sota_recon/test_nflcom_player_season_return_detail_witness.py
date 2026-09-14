import json

from scripts.sota_recon.nflcom_player_season_return_detail_witness import (
    RETURN_DETAIL_FIELDS,
    RETURN_PAGE_CANDIDATES,
    _agreement_counts,
    return_detail_inventory,
    witness_form,
)


def test_return_pages_cover_the_eight_deferred_detail_fields():
    assert set(RETURN_DETAIL_FIELDS) == {"fc", "fum", "20", "40"}
    assert set(RETURN_PAGE_CANDIDATES) == {"kickoff-returns", "punt-returns"}
    assert all(set(RETURN_DETAIL_FIELDS).issubset(fields) for fields in RETURN_PAGE_CANDIDATES.values())


def test_return_witness_forms_keep_long_as_max_and_counts_as_sum():
    assert witness_form("lng") == "MAX"
    assert witness_form("ret") == "SUM"
    assert witness_form("fum") == "SUM"


def test_return_detail_inventory_refuses_missing_canonicals():
    inventory = return_detail_inventory({"kickoff_returns", "punt_returns"})
    assert inventory["kickoff-returns"]["fc"]["status"] == "NO_CURRENT_V26_CANONICAL"
    assert inventory["punt-returns"]["20"]["status"] == "NO_CURRENT_V26_CANONICAL"
    assert inventory["kickoff-returns"]["fc"]["schema_search_found"] is False


def test_return_detail_inventory_reports_each_subject_plane():
    inventory = return_detail_inventory({
        "weekly": {"kickoff_return_fair_catches"},
        "season": set(),
        "career": set(),
        "season_team": set(),
    })
    assert inventory["kickoff-returns"]["fc"]["schema_search_found"] is True
    assert inventory["kickoff-returns"]["fc"]["schema_search_found_by_plane"] == {
        "weekly": True,
        "season": False,
        "career": False,
        "season_team": False,
    }


def test_zero_rows_cannot_vote_as_informative_agreements():
    assert _agreement_counts([(0, 0), (2, 2), (2, 1), (3, None)]) == (4, 2, 1, 1, 0)
