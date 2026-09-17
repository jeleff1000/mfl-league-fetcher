"""Contracts for selecting one active leg from a saved multi-platform chain."""

from __future__ import annotations

import pytest


class _SplitHistoryReader:
    def __init__(self, *, aliases, years):
        self.aliases = aliases
        self.years = years

    def query(self, sql, database):
        if database == "___ops":
            return [{"database_name": alias} for alias in self.aliases]
        if database == "___leagues":
            return [
                {"db_name": db_name, "year": year}
                for db_name, values in self.years.items()
                for year in values
            ]
        raise AssertionError(database)


def test_canonical_history_guard_rejects_years_stranded_under_legacy_slug():
    from multi_league.core.league_update_lineage import (
        ActiveUpdateSegmentError,
        assert_canonical_history_complete,
    )

    reader = _SplitHistoryReader(
        aliases=["legacy_slug"],
        years={"canonical_slug": [2026], "legacy_slug": [2009, 2010, 2025]},
    )

    with pytest.raises(ActiveUpdateSegmentError, match="legacy_slug.*2009, 2010, 2025"):
        assert_canonical_history_complete(
            reader,
            database_name="canonical_slug",
            active_season=2026,
        )


def test_canonical_history_guard_accepts_already_copied_legacy_years():
    from multi_league.core.league_update_lineage import assert_canonical_history_complete

    reader = _SplitHistoryReader(
        aliases=["legacy_slug"],
        years={
            "canonical_slug": [2009, 2010, 2025, 2026],
            "legacy_slug": [2009, 2010, 2025],
        },
    )

    receipt = assert_canonical_history_complete(
        reader,
        database_name="canonical_slug",
        active_season=2026,
    )

    assert receipt == {
        "canonical_db": "canonical_slug",
        "legacy_databases": ["legacy_slug"],
        "historical_years_verified": [2009, 2010, 2025],
    }


def test_resolve_active_update_segment_uses_only_the_active_platform_leg():
    """A weekly Sleeper leg must retain, not refetch, older Yahoo seasons."""
    from multi_league.core.league_update_lineage import resolve_active_update_segment

    segment = resolve_active_update_segment(
        active_year=2026,
        context_platform="sleeper",
        context_league_id="1257433021309530112",
        settings_rows=[
            {"year": 2014, "platform": "yahoo", "league_key": "331.l.1398746"},
            {"year": 2015, "platform": "yahoo", "league_key": "348.l.458648"},
            {"year": 2024, "platform": "sleeper", "league_key": "1124815297790898176"},
            {"year": 2025, "platform": "sleeper", "league_key": "1257433021309530112"},
        ],
    )

    assert segment.platform == "sleeper"
    assert segment.current_league_id is None
    assert segment.league_ids == {
        "2024": "1124815297790898176",
        "2025": "1257433021309530112",
    }
    assert segment.historical_platforms == ("yahoo",)


def test_resolve_active_update_segment_rejects_a_worker_for_the_wrong_active_platform():
    """A misrouted weekly worker must fail before any provider request."""
    from multi_league.core.league_update_lineage import (
        ActiveUpdateSegmentError,
        resolve_active_update_segment,
    )

    with pytest.raises(ActiveUpdateSegmentError, match="expected sleeper"):
        resolve_active_update_segment(
            active_year=2026,
            context_platform="sleeper",
            context_league_id="old-sleeper-id",
            expected_platform="sleeper",
            settings_rows=[
                {"year": 2025, "platform": "sleeper", "league_key": "old-sleeper-id"},
                {"year": 2026, "platform": "yahoo", "league_key": "470.l.999"},
            ],
        )


def test_resolve_active_update_segment_rejects_overlapping_active_platform_ownership():
    """One active season cannot be refreshed by two providers at once."""
    from multi_league.core.league_update_lineage import (
        ActiveUpdateSegmentError,
        resolve_active_update_segment,
    )

    with pytest.raises(ActiveUpdateSegmentError, match="multiple providers"):
        resolve_active_update_segment(
            active_year=2026,
            context_platform="sleeper",
            context_league_id="sleeper-2026",
            settings_rows=[
                {"year": 2026, "platform": "sleeper", "league_key": "sleeper-2026"},
                {"year": 2026, "platform": "yahoo", "league_key": "470.l.999"},
            ],
        )


@pytest.mark.parametrize(
    "saved_platform,unexpected_platform,unexpected_id",
    [("sleeper", "yahoo", "470.l.164172"),
     ("yahoo", "espn", "110800"),
     ("espn", "sleeper", "1385696375349448704")],
)
def test_active_settings_cannot_silently_change_the_import_target(
    saved_platform, unexpected_platform, unexpected_id,
):
    """A prior bad update must not authorize another unrelated provider leg."""
    from multi_league.core.league_update_lineage import (
        ActiveUpdateSegmentError,
        resolve_active_update_segment,
    )

    with pytest.raises(ActiveUpdateSegmentError, match="conflicts with saved import target"):
        resolve_active_update_segment(
            active_year=2026,
            context_platform=saved_platform,
            context_league_id="saved-import-target",
            expected_platform=unexpected_platform,
            settings_rows=[
                {"year": 2025, "platform": saved_platform, "league_key": "saved-2025"},
                {"year": 2026, "platform": unexpected_platform, "league_key": unexpected_id},
            ],
        )
