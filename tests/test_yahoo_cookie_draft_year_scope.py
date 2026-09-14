import pandas as pd

from scripts.build_kmffl_cookie_model import prepare_draft_source


def test_prepare_draft_source_ignores_rows_outside_requested_renewal_chain():
    draft = pd.DataFrame(
        [
            {"year": 2014, "team": "Old Team", "pick": 1, "cost": 0, "player": "Old Player"},
            {"year": 2015, "team": "Current Team", "pick": 1, "cost": 0, "player": "Current Player"},
        ]
    )
    team_names = {(2014, 1): "Old Team", (2015, 1): "Current Team"}
    identities = {
        (2014, 1): {"manager": "old", "manager_guid": "old-guid"},
        (2015, 1): {"manager": "current", "manager_guid": "current-guid"},
    }

    result = prepare_draft_source(
        draft,
        team_names,
        identities,
        {2015: "348.l.727365"},
    )

    assert result["year"].tolist() == [2015]
    assert result["league_id"].tolist() == ["348.l.727365"]
