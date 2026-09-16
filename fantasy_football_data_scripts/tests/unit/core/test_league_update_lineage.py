"""Contracts for selecting one active leg from a saved multi-platform chain."""

from __future__ import annotations

import pytest


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
