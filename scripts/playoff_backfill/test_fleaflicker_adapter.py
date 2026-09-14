import pandas as pd

from scripts.playoff_backfill.fleaflicker_adapter import build_fleaflicker_evidence


def test_fleaflicker_schedule_flags_exclude_consolation():
    rosters = pd.DataFrame([
        {"db_name": "ff", "year": 2018, "week": 14, "NFL_player_id": "p1", "team_key": "42"},
        {"db_name": "ff", "year": 2018, "week": 14, "NFL_player_id": "p2", "team_key": "43"},
        {"db_name": "ff", "year": 2018, "week": 15, "NFL_player_id": "p1", "team_key": "42"},
    ])
    schedule = pd.DataFrame([
        {"week": 14, "team_key": "42", "is_playoffs": 1, "is_consolation": 0, "team_made_playoffs": 1},
        {"week": 14, "team_key": "43", "is_playoffs": 0, "is_consolation": 1, "team_made_playoffs": 0},
        {"week": 15, "team_key": "42", "is_playoffs": 1, "is_consolation": 0, "team_made_playoffs": 1},
    ])
    result = build_fleaflicker_evidence(rosters, schedule, db_name="ff", year=2018)
    p1 = result[result.NFL_player_id == "p1"]
    assert tuple(p1.is_playoffs_bf) == (1, 1)
    assert result.loc[result.NFL_player_id == "p2", "made_po_bf"].iloc[0] == 0
