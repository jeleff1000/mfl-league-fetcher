from __future__ import annotations

import pandas as pd

from scripts import refresh_yahoo_active_season as subject


def test_yahoo_rosters_only_cover_weeks_with_finalized_nfl_games() -> None:
    assert subject._yahoo_finalized_roster_weeks(
        refresh_weeks=[2, 3],
        finalized_ops=pd.DataFrame({"week": [2, 2]}),
    ) == [2]


def test_demo_promotion_failure_after_commit_is_recorded_not_raised():
    from types import SimpleNamespace

    class BrokenWriter:
        def execute(self, sql, *, database):
            raise RuntimeError("fly busy")

    receipt: dict = {}
    subject._promote_scheduled_demo_publication(
        receipt, BrokenWriter(), database_name="demo_league",
        persisted_plan=SimpleNamespace(observed_manifest_digest="obs-12"),
    )
    assert receipt["scheduled_demo_manifest_promoted"] is False
    assert "fly busy" in receipt["scheduled_demo_manifest_error"]
