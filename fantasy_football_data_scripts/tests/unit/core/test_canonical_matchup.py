import sys
from pathlib import Path

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.core.aggregate_ddl import MATCHUP_SEASON_SNAPSHOT_COLUMNS
from multi_league.core.canonical_matchup import (
    SCHEDULE_LUCK_SIM_COLUMNS,
    SOS_LUCK_SIM_COLUMNS,
    LUCK_SIM_COLUMNS,
    PLAYOFF_SIM_COLUMNS,
    SIM_COLUMNS,
    normalize_matchup_df,
)


def test_simulation_column_families_are_disjoint_and_complete():
    schedule_luck = set(SCHEDULE_LUCK_SIM_COLUMNS)
    sos_luck = set(SOS_LUCK_SIM_COLUMNS)
    luck = set(LUCK_SIM_COLUMNS)
    playoff = set(PLAYOFF_SIM_COLUMNS)
    sim = set(SIM_COLUMNS)

    assert schedule_luck.isdisjoint(sos_luck)
    assert schedule_luck | sos_luck == luck
    assert luck.isdisjoint(playoff)
    assert luck | playoff == sim


def test_schedule_sos_and_playoff_examples_land_in_correct_family():
    assert "shuffle_avg_wins" in SCHEDULE_LUCK_SIM_COLUMNS
    assert "wins_vs_shuffle_wins" in SCHEDULE_LUCK_SIM_COLUMNS
    assert "shuffle_12_win" in SCHEDULE_LUCK_SIM_COLUMNS
    assert "opp_shuffle_12_win" not in SCHEDULE_LUCK_SIM_COLUMNS
    assert "p_playoffs" not in SCHEDULE_LUCK_SIM_COLUMNS

    assert "opp_shuffle_avg_wins" in SOS_LUCK_SIM_COLUMNS
    assert "wins_vs_opp_shuffle_wins" in SOS_LUCK_SIM_COLUMNS
    assert "opp_shuffle_12_win" in SOS_LUCK_SIM_COLUMNS
    assert "shuffle_12_win" not in SOS_LUCK_SIM_COLUMNS
    assert "x12_win" not in SOS_LUCK_SIM_COLUMNS

    assert "p_playoffs" in PLAYOFF_SIM_COLUMNS
    assert "exp_final_wins" in PLAYOFF_SIM_COLUMNS
    assert "x12_win" in PLAYOFF_SIM_COLUMNS
    assert "shuffle_12_win" not in PLAYOFF_SIM_COLUMNS
    assert "opp_shuffle_avg_wins" not in PLAYOFF_SIM_COLUMNS


def test_matchup_season_snapshot_columns_include_both_simulation_families():
    snapshot = set(MATCHUP_SEASON_SNAPSHOT_COLUMNS)

    assert set(SCHEDULE_LUCK_SIM_COLUMNS).issubset(snapshot)
    assert set(SOS_LUCK_SIM_COLUMNS).issubset(snapshot)
    assert set(PLAYOFF_SIM_COLUMNS).issubset(snapshot)
    assert "wins_to_date" in snapshot
    assert "playoff_seed_to_date" in snapshot


def test_normalize_matchup_df_recovers_hidden_yahoo_team_key_from_url():
    import pandas as pd

    raw = pd.DataFrame(
        [
            {
                "year": 2013,
                "week": 1,
                "manager": "Moneyball",
                "manager_guid": "--",
                "team_key": "--",
                "team_name": "MoneyBall",
                "league_id": "314.l.134824",
                "url": "https://football.fantasysports.yahoo.com/2013/f1/134824/2",
                "team_points": 100.0,
                "opponent": "Paul",
                "opponent_guid": "paul-guid",
                "opponent_team_key": "314.l.134824.t.7",
                "opponent_points": 90.0,
            }
        ]
    )

    normalized = normalize_matchup_df(raw, platform="yahoo", league_id="314.l.134824")

    assert normalized.loc[0, "team_key"] == "314.l.134824.t.2"
    assert normalized.loc[0, "opponent_team_key"] == "314.l.134824.t.7"


def test_normalize_matchup_df_does_not_trust_yahoo_hidden_guid_as_franchise_id():
    import pandas as pd

    raw = pd.DataFrame(
        [
            {
                "year": 2004,
                "week": 1,
                "manager": "Ali S",
                "manager_guid": "--hidden--",
                "team_key": "101.l.83266.t.1",
                "team_name": "COMMISH",
                "league_id": "101.l.83266",
                "team_points": 100.0,
                "opponent": "Riaz D",
                "opponent_guid": "--hidden--",
                "opponent_team_key": "101.l.83266.t.12",
                "opponent_points": 90.0,
            }
        ]
    )

    normalized = normalize_matchup_df(raw, platform="yahoo", league_id="101.l.83266")

    assert normalized.loc[0, "franchise_id"] is None
    assert normalized.loc[0, "opponent_franchise_id"] is None
