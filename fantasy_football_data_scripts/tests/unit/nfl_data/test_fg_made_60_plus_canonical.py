import pandas as pd
from fantasy_football_data_scripts.nfl_data.build_nfl_super_table import (
    populate_fg_made_60_plus_canonical,
)


def test_canonical_copies_from_fg_made_60_():
    """Single-column scenario per Task 1 findings: canonical = COALESCE(fg_made_60_, 0)."""
    df = pd.DataFrame(
        {
            "fg_made_60_": [0, 1, 2, None, 0],
        }
    )
    out = populate_fg_made_60_plus_canonical(df)
    assert list(out["fg_made_60_plus_canonical"]) == [0, 1, 2, 0, 0]


def test_canonical_handles_missing_source_col():
    """If fg_made_60_ is absent (e.g., pre-1980 era), canonical defaults to 0."""
    df = pd.DataFrame({"unrelated": [1, 2, 3]})
    out = populate_fg_made_60_plus_canonical(df)
    assert list(out["fg_made_60_plus_canonical"]) == [0, 0, 0]
