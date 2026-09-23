from __future__ import annotations

import pandas as pd

from scripts import refresh_yahoo_active_season as subject


def test_yahoo_rosters_only_cover_weeks_with_finalized_nfl_games() -> None:
    assert subject._yahoo_finalized_roster_weeks(
        refresh_weeks=[2, 3],
        finalized_ops=pd.DataFrame({"week": [2, 2]}),
    ) == [2]
