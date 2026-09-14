from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.player_identity_guards import (
    apply_known_context_identity_rebuilds,
    apply_known_special_teams_identity_repairs,
    apply_known_stat_family_splits,
)


def test_stat_family_split_separates_mark_carrier_wr_and_db(tmp_path):
    source = pd.DataFrame(
        [
            {
                "player": "Mark Carrier",
                "NFL_player_id": "00-0002688",
                "player_week": "00-0002688_1994_1",
                "position": "DB",
                "nfl_position": "DB",
                "fantasy_position": "DB",
                "nfl_team": "CLE",
                "opponent_nfl_team": "CIN",
                "year": 1994,
                "week": 1,
                "season_type": "REG",
                "receptions": 5.0,
                "receiving_yards": 97.0,
                "punt_return_yards": 12.0,
                "def_tackles_solo": 9.0,
                "def_interceptions": 1.0,
            }
        ]
    )
    pbp_rollup = pd.DataFrame(
        [
            {
                "player": "Mark Carrier",
                "NFL_player_id": "00-0002687",
                "player_week": "00-0002687_1994_1",
                "position": "WR",
                "nfl_team": "CLE",
                "opponent_nfl_team": "CIN",
                "year": 1994,
                "week": 1,
                "season_type": "REG",
                "receptions": 3.0,
                "receiving_yards": 47.0,
                "punt_return_yards": 4.0,
                "def_tackles_solo": 0.0,
                "def_interceptions": 0.0,
            },
            {
                "player": "Mark Carrier",
                "NFL_player_id": "00-0002688",
                "player_week": "00-0002688_1994_1",
                "position": "DB",
                "nfl_team": "CHI",
                "opponent_nfl_team": "TB",
                "year": 1994,
                "week": 1,
                "season_type": "REG",
                "receptions": 0.0,
                "receiving_yards": 0.0,
                "punt_return_yards": 0.0,
                "def_tackles_solo": 6.0,
                "def_interceptions": 0.0,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    pbp_rollup.to_parquet(rollup_path, index=False)

    out = apply_known_stat_family_splits(source, rollup_path=rollup_path)

    assert sorted(out["player_week"].tolist()) == [
        "00-0002687_1994_1",
        "00-0002688_1994_1",
    ]
    wr = out[out["NFL_player_id"].eq("00-0002687")].iloc[0]
    db = out[out["NFL_player_id"].eq("00-0002688")].iloc[0]

    assert wr["position"] == "WR"
    assert wr["nfl_team"] == "CLE"
    assert wr["receiving_yards"] == 47.0
    assert wr["punt_return_yards"] == 4.0
    assert wr["def_tackles_solo"] == 0.0
    assert wr["def_interceptions"] == 0.0

    assert db["position"] == "DB"
    assert db["nfl_team"] == "CHI"
    assert db["receiving_yards"] == 0.0
    assert db["punt_return_yards"] == 0.0
    assert db["def_tackles_solo"] == 9.0
    assert db["def_interceptions"] == 1.0


def test_special_teams_repair_separates_steve_jordan_te_and_k(tmp_path):
    source = pd.DataFrame(
        [
            {
                "player": "Steve Jordan",
                "NFL_player_id": "00-0008927",
                "player_week": "00-0008927_1987_1",
                "position": "TE",
                "nfl_position": "TE",
                "fantasy_position": "TE",
                "nfl_team": "MIN",
                "opponent_nfl_team": "DET",
                "year": 1987,
                "week": 1,
                "season_type": "REG",
                "receptions": 3.0,
                "receiving_yards": 43.0,
                "fg_att": 2.0,
                "fg_made": 1.0,
                "fg_missed": 0.0,
                "fg_yards": 0.0,
                "pat_att": 2.0,
                "pat_made": 2.0,
            },
            {
                "player": "Steve Jordan",
                "NFL_player_id": "00-0008927",
                "player_week": "JOR577792_1987_4",
                "position": "K",
                "nfl_position": "K",
                "fantasy_position": "K",
                "nfl_team": "IND",
                "opponent_nfl_team": "BUF",
                "year": 1987,
                "week": 4,
                "season_type": "REG",
                "receptions": 1.0,
                "receiving_yards": 9.0,
                "fg_att": 1.0,
                "fg_made": 1.0,
                "fg_missed": 0.0,
                "fg_yards": 36.0,
                "pat_att": 0.0,
                "pat_made": 0.0,
            },
        ]
    )
    pbp_rollup = pd.DataFrame(
        [
            {
                "player": "Steve Jordan",
                "NFL_player_id": "JOR577792",
                "player_week": "JOR577792_1987_4",
                "position": "K",
                "nfl_team": "IND",
                "opponent_nfl_team": "BUF",
                "year": 1987,
                "week": 4,
                "season_type": "REG",
                "fg_att": 1.0,
                "fg_made": 1.0,
                "fg_missed": 0.0,
                "fg_yards": 36.0,
                "fg_long": 36.0,
                "pat_att": 6.0,
                "pat_made": 6.0,
            },
            {
                "player": "Steve Jordan",
                "NFL_player_id": "JOR577792",
                "player_week": "JOR577792_1987_6",
                "position": "K",
                "nfl_team": "IND",
                "opponent_nfl_team": "PIT",
                "year": 1987,
                "week": 6,
                "season_type": "REG",
                "fg_att": 0.0,
                "fg_made": 0.0,
                "fg_missed": 0.0,
                "fg_yards": 0.0,
                "fg_long": 0.0,
                "pat_att": 1.0,
                "pat_made": 1.0,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    pbp_rollup.to_parquet(rollup_path, index=False)

    out = apply_known_special_teams_identity_repairs(source, rollup_path=rollup_path)

    assert sorted(out["player_week"].tolist()) == [
        "00-0008927_1987_1",
        "JOR577792_1987_4",
        "JOR577792_1987_6",
    ]
    te = out[out["player_week"].eq("00-0008927_1987_1")].iloc[0]
    k4 = out[out["player_week"].eq("JOR577792_1987_4")].iloc[0]
    k6 = out[out["player_week"].eq("JOR577792_1987_6")].iloc[0]

    assert te["NFL_player_id"] == "00-0008927"
    assert te["position"] == "TE"
    assert te["receiving_yards"] == 43.0
    assert te["fg_att"] == 0.0
    assert te["pat_att"] == 0.0

    assert k4["NFL_player_id"] == "JOR577792"
    assert k4["position"] == "K"
    assert k4["receiving_yards"] == 0.0
    assert k4["pat_att"] == 6.0
    assert k4["pat_made"] == 6.0

    assert k6["NFL_player_id"] == "JOR577792"
    assert k6["nfl_team"] == "IND"
    assert k6["opponent_nfl_team"] == "PIT"
    assert k6["pat_att"] == 1.0


def test_context_identity_rebuild_separates_keith_jones_browns_and_falcons(tmp_path):
    source = pd.DataFrame(
        [
            {
                "player": "Keith Jones",
                "NFL_player_id": "JoneKe00",
                "player_week": "JoneKe00_1989_2",
                "position": "RB",
                "nfl_position": "RB",
                "fantasy_position": "RB",
                "nfl_team": "CLE",
                "opponent_nfl_team": "DAL",
                "year": 1989,
                "week": 2,
                "season_type": "REG",
                "carries": 0.0,
                "rushing_yards": 0.0,
                "rushing_tds": 0.0,
                "targets": 0.0,
                "receptions": 0.0,
                "receiving_yards": 0.0,
                "kickoff_returns": 4.0,
                "kickoff_return_yards": 69.0,
            }
        ]
    )
    pfr = pd.DataFrame(
        [
            {
                "Player": "Keith Jones",
                "pfr_player_id": "JoneKe00",
                "Date": "1989-09-17",
                "Week": 2,
                "Team": "CLE",
                "Opp": "NYJ",
                "Age": "23-181",
                "Rushing_Att": 9,
                "Rushing_Yds": 49,
                "Rushing_TD": 1,
                "Receiving_Tgt": 4,
                "Receiving_Rec": 3,
                "Receiving_Yds": 55,
                "Kick Returns_Ret": 1,
                "Kick Returns_Yds": 25,
                "Punt Returns_Ret": 0,
                "Punt Returns_Yds": 0,
                "Pos.": "RB",
            },
            {
                "Player": "Keith Jones",
                "pfr_player_id": "JoneKe01",
                "Date": "1989-09-17",
                "Week": 2,
                "Team": "ATL",
                "Opp": "DAL",
                "Age": "23-174",
                "Rushing_Att": 0,
                "Rushing_Yds": 0,
                "Rushing_TD": 0,
                "Receiving_Tgt": 0,
                "Receiving_Rec": 0,
                "Receiving_Yds": 0,
                "Kick Returns_Ret": 4,
                "Kick Returns_Yds": 69,
                "Punt Returns_Ret": 0,
                "Punt Returns_Yds": 0,
                "Pos.": "RB",
            },
        ]
    )
    pfr.to_parquet(tmp_path / "flx1980s.parquet", index=False)

    out = apply_known_context_identity_rebuilds(source, pfr_cache_dir=tmp_path)

    assert sorted(out["player_week"].tolist()) == ["JoneKe00_1989_2", "JoneKe01_1989_2"]
    browns = out[out["player_week"].eq("JoneKe00_1989_2")].iloc[0]
    falcons = out[out["player_week"].eq("JoneKe01_1989_2")].iloc[0]

    assert browns["NFL_player_id"] == "JoneKe00"
    assert browns["nfl_team"] == "CLE"
    assert browns["opponent_nfl_team"] == "NYJ"
    assert browns["rushing_yards"] == 49.0
    assert browns["rushing_tds"] == 1.0
    assert browns["receiving_yards"] == 55.0
    assert browns["kickoff_returns"] == 1.0

    assert falcons["NFL_player_id"] == "JoneKe01"
    assert falcons["nfl_team"] == "ATL"
    assert falcons["opponent_nfl_team"] == "DAL"
    assert falcons["kickoff_returns"] == 4.0
    assert falcons["kickoff_return_yards"] == 69.0
