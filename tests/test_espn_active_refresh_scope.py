from __future__ import annotations

import pandas as pd

from scripts import refresh_espn_active_season as subject


def test_espn_rosters_only_cover_weeks_with_finalized_nfl_games() -> None:
    selector = getattr(
        subject,
        "_espn_finalized_roster_weeks",
        lambda *, refresh_weeks, finalized_ops: list(refresh_weeks),
    )

    assert selector(
        refresh_weeks=[2, 3],
        finalized_ops=pd.DataFrame({"week": [2, 2]}),
    ) == [2]
