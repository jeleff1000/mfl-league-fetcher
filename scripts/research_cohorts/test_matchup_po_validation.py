from decimal import Decimal

from build_research_matchup_cohort import po_validation_summary


def test_po_validation_summary_treats_empty_aggregate_as_zero():
    assert po_validation_summary((2019, None, None)) == (2019, 0, 0, 0.0)


def test_po_validation_summary_accepts_duckdb_decimals():
    assert po_validation_summary((2021, Decimal("8"), Decimal("6"))) == (
        2021,
        8,
        6,
        75.0,
    )
