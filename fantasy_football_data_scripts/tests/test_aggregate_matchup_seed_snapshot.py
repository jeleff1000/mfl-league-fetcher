from __future__ import annotations

from multi_league.transformations.aggregation.aggregate_matchup_context import _build_seed_snapshot_select


def test_seed_snapshot_prefers_frozen_final_playoff_seed():
    sql = _build_seed_snapshot_select(has_final_playoff_seed=True)

    assert "COALESCE(MAX(final_playoff_seed)" in sql
    assert "COALESCE(CAST(is_consolation AS INT), 0) = 0" in sql
    assert "COALESCE(CAST(is_bye_week AS INT), 0) = 0" in sql


def test_seed_snapshot_can_fallback_without_final_playoff_seed():
    sql = _build_seed_snapshot_select(has_final_playoff_seed=False)

    assert "MAX(final_playoff_seed)" not in sql
    assert "ARG_MAX(" in sql
