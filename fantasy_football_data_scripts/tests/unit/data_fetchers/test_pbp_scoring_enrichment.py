from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.pbp_scoring_enrichment import (
    enrich_with_pbp_scoring_rollup,
    enrich_with_pbp_scoring_stats,
)


def test_enrich_with_pbp_scoring_stats_adds_big_play_buckets_and_st_tackles():
    player_week = pd.DataFrame(
        [
            {"year": 2025, "week": 3, "season_type": "REG", "NFL_player_id": "QB1", "player": "Quarter Back"},
            {"year": 2025, "week": 3, "season_type": "REG", "NFL_player_id": "WR1", "player": "Wide Receiver"},
            {"year": 2025, "week": 3, "season_type": "REG", "NFL_player_id": "DB1", "player": "Coverage Player"},
        ]
    )
    pbp = pd.DataFrame(
        [
            {
                "season": 2025,
                "week": 3,
                "season_type": "REG",
                "passer_player_id": "QB1",
                "receiver_player_id": "WR1",
                "complete_pass": 1,
                "pass_touchdown": 1,
                "yards_gained": 55,
                "receiving_yards": 55,
                "special_teams_play": 0,
            },
            {
                "season": 2025,
                "week": 3,
                "season_type": "REG",
                "passer_player_id": "QB1",
                "receiver_player_id": "WR1",
                "complete_pass": 1,
                "pass_touchdown": 0,
                "yards_gained": 7,
                "receiving_yards": 7,
                "special_teams_play": 0,
            },
            {
                "season": 2025,
                "week": 3,
                "season_type": "REG",
                "passer_player_id": "QB1",
                "receiver_player_id": "WR1",
                "complete_pass": 1,
                "pass_touchdown": 0,
                "yards_gained": 18,
                "receiving_yards": 18,
                "special_teams_play": 0,
            },
            {
                "season": 2025,
                "week": 3,
                "season_type": "REG",
                "passer_player_id": "QB1",
                "receiver_player_id": "WR1",
                "complete_pass": 1,
                "pass_touchdown": 0,
                "yards_gained": 3,
                "receiving_yards": 3,
                "special_teams_play": 0,
            },
            {
                "season": 2025,
                "week": 3,
                "season_type": "REG",
                "passer_player_id": "QB1",
                "receiver_player_id": "WR1",
                "complete_pass": 1,
                "pass_touchdown": 0,
                "yards_gained": 25,
                "receiving_yards": 25,
                "special_teams_play": 0,
            },
            {
                "season": 2025,
                "week": 3,
                "season_type": "REG",
                "passer_player_id": "QB1",
                "receiver_player_id": "WR1",
                "complete_pass": 1,
                "pass_touchdown": 0,
                "yards_gained": 35,
                "receiving_yards": 35,
                "special_teams_play": 0,
            },
            {
                "season": 2025,
                "week": 3,
                "season_type": "REG",
                "passer_player_id": None,
                "receiver_player_id": None,
                "complete_pass": 0,
                "pass_touchdown": 0,
                "yards_gained": 0,
                "receiving_yards": None,
                "special_teams_play": 1,
                "solo_tackle_1_player_id": "DB1",
            },
        ]
    )

    out = enrich_with_pbp_scoring_stats(player_week, 2025, week=3, pbp_df=pbp)
    by_id = out.set_index("NFL_player_id")

    assert by_id.loc["QB1", "completions_40plus"] == 1
    assert by_id.loc["QB1", "completions_50plus"] == 1
    assert by_id.loc["QB1", "passing_tds_40plus"] == 1
    assert by_id.loc["QB1", "passing_tds_50plus"] == 1
    assert by_id.loc["WR1", "receptions_0_4"] == 1
    assert by_id.loc["WR1", "receptions_5_9"] == 1
    assert by_id.loc["WR1", "receptions_10_19"] == 1
    assert by_id.loc["WR1", "receptions_20_29"] == 1
    assert by_id.loc["WR1", "receptions_30_39"] == 1
    assert by_id.loc["WR1", "receptions_40plus"] == 1
    assert by_id.loc["WR1", "receiving_tds_40plus"] == 1
    assert by_id.loc["WR1", "receiving_tds_50plus"] == 1
    assert by_id.loc["DB1", "special_teams_tackles_solo"] == 1


def test_enrich_with_pbp_scoring_stats_zero_fills_pre_1999():
    rows = pd.DataFrame([{"year": 1998, "week": 1, "NFL_player_id": "QB1"}])

    out = enrich_with_pbp_scoring_stats(rows, 1998)

    assert out.loc[0, "completions_50plus"] == 0
    assert out.loc[0, "receptions_0_4"] == 0
    assert out.loc[0, "special_teams_tackles_solo"] == 0


def test_enrich_with_pbp_scoring_rollup_updates_existing_rows_only(tmp_path):
    rows = pd.DataFrame(
        [
            {"year": 2025, "week": 3, "player_week": "QB1_2025_3", "NFL_player_id": "QB1"},
            {"year": 2025, "week": 3, "player_week": "WR1_2025_3", "NFL_player_id": "WR1"},
            {"year": 2025, "week": 3, "player_week": "DB1_2025_3", "NFL_player_id": "DB1"},
        ]
    )
    rollup = pd.DataFrame(
        [
            {
                "player_week": "QB1_2025_3",
                "completions_50plus": 1,
                "receptions_0_4": 0,
                "receptions_5_9": 0,
                "receptions_10_19": 0,
                "receptions_20_29": 0,
                "receptions_30_39": 0,
                "special_teams_tackles_solo": 0,
                "fumble_recovery_yards_own": 0,
                "fumble_recovery_yards_opp": 0,
                "fumble_recovery_yards": 0,
            },
            {
                "player_week": "DB1_2025_3",
                "completions_50plus": 0,
                "receptions_0_4": 0,
                "receptions_5_9": 0,
                "receptions_10_19": 0,
                "receptions_20_29": 0,
                "receptions_30_39": 0,
                "special_teams_tackles_solo": 1,
                "fumble_recovery_yards_own": 4,
                "fumble_recovery_yards_opp": 11,
                "fumble_recovery_yards": 15,
            },
            {
                "player_week": "MISSING_2025_3",
                "completions_50plus": 9,
                "receptions_0_4": 0,
                "receptions_5_9": 0,
                "receptions_10_19": 0,
                "receptions_20_29": 0,
                "receptions_30_39": 0,
                "special_teams_tackles_solo": 0,
                "fumble_recovery_yards_own": 0,
                "fumble_recovery_yards_opp": 0,
                "fumble_recovery_yards": 0,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_scoring_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)

    out = enrich_with_pbp_scoring_rollup(rows, rollup_path=rollup_path)
    by_id = out.set_index("NFL_player_id")

    assert len(out) == len(rows)
    assert by_id.loc["QB1", "completions_50plus"] == 1
    assert by_id.loc["WR1", "completions_50plus"] == 0
    assert by_id.loc["DB1", "special_teams_tackles_solo"] == 1
    assert by_id.loc["DB1", "fumble_recovery_yards_own"] == 4
    assert by_id.loc["DB1", "fumble_recovery_yards_opp"] == 11
    assert by_id.loc["DB1", "fum_rec_yds"] == 15
