import pytest

from snapshot_real_leagues import snapshot_years


def test_snapshot_defaults_cover_the_complete_research_lake():
    years = snapshot_years()
    assert years.start == 1997
    assert years.stop == 2026


def test_snapshot_range_fails_closed():
    with pytest.raises(ValueError):
        snapshot_years(2010, 2009)
    with pytest.raises(ValueError):
        snapshot_years(1996, 2025)
