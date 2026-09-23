from __future__ import annotations

import pandas as pd

from scripts import refresh_sleeper_active_season as subject


def test_sleeper_rosters_only_cover_weeks_with_finalized_nfl_games() -> None:
    assert subject._sleeper_finalized_roster_weeks(
        refresh_weeks=[1, 2, 3],
        finalized_ops=pd.DataFrame({"week": [1, 1, 2, 2]}),
    ) == [1, 2]
