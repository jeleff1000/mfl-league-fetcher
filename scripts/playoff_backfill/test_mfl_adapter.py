import pandas as pd

from scripts.playoff_backfill.mfl_adapter import build_mfl_evidence


def test_mfl_bracket_pairs_distinguish_championship_from_consolation():
    rosters = pd.DataFrame([
        {"db_name": "mfl", "year": 2018, "week": 14, "NFL_player_id": "p1", "team_key": "0001"},
        {"db_name": "mfl", "year": 2018, "week": 14, "NFL_player_id": "p3", "team_key": "0003"},
        {"db_name": "mfl", "year": 2018, "week": 15, "NFL_player_id": "p1", "team_key": "0001"},
    ])
    bracket_pairs = {(14, frozenset({"0001", "0002"})), (15, frozenset({"0001", "0004"}))}
    result = build_mfl_evidence(rosters, bracket_pairs, db_name="mfl", year=2018)
    assert tuple(result.loc[result.NFL_player_id == "p1", "is_playoffs_bf"]) == (1, 1)
    assert tuple(result.loc[result.NFL_player_id == "p3", "is_playoffs_bf"]) == (0,)
    assert set(result.loc[result.made_po_bf == 1, "NFL_player_id"]) == {"p1"}
