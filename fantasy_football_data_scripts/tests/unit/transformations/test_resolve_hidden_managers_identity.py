import sys
from pathlib import Path

import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.transformations.matchup.resolve_hidden_managers import build_guid_to_name_mapping, is_valid_guid


def test_yahoo_hidden_guid_literal_is_not_valid_for_name_unification():
    assert not is_valid_guid("--hidden--")

    mapping = build_guid_to_name_mapping(
        {
            "matchup": pd.DataFrame(
                [
                    {"manager_guid": "--hidden--", "manager": "Ali S", "year": 2004},
                    {"manager_guid": "--hidden--", "manager": "Riaz D", "year": 2004},
                    {"manager_guid": "real-guid", "manager": "Visible", "year": 2004},
                ]
            )
        }
    )

    assert mapping == {"real-guid": "Visible"}
