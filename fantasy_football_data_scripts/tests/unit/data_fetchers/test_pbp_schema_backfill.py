from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.pbp_schema_backfill import (
    apply_pbp_postseason_official_repairs,
    apply_pbp_schema_backfill,
    apply_pbp_truth_atom_overlays,
    build_pbp_safe_missing_rows,
)


def test_pbp_schema_backfill_updates_existing_rows_only(tmp_path):
    rows = pd.DataFrame(
        [
            {"player_week": "QB1_2025_3", "fumbles": 0, "punt_yards": 0},
            {"player_week": "WR1_2025_3", "fumbles": 0, "punt_yards": 0},
        ]
    )
    rollup = pd.DataFrame(
        [
            {"player_week": "QB1_2025_3", "fumbles": 1, "punt_yards": 0},
            {"player_week": "MISSING_2025_3", "fumbles": 9, "punt_yards": 70},
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)

    out = apply_pbp_schema_backfill(rows, rollup_path=rollup_path, columns=["fumbles", "punt_yards"])
    by_key = out.set_index("player_week")

    assert by_key.loc["QB1_2025_3", "fumbles"] == 1
    assert by_key.loc["WR1_2025_3", "fumbles"] == 0
    assert len(out) == len(rows)


def test_postseason_official_repairs_overwrite_only_high_confidence_week18_context_matches(tmp_path):
    rows = pd.DataFrame(
        [
            {
                "player_week": "QB1_1986_18",
                "year": 1986,
                "week": 18,
                "season_type": "POST",
                "nfl_team": "CLE",
                "opponent_nfl_team": "NYJ",
                "attempts": 19,
                "completions": 10,
                "passing_yards": 66,
                "passing_tds": 1,
            },
            {
                "player_week": "WR1_1986_18",
                "year": 1986,
                "week": 18,
                "season_type": "POST",
                "nfl_team": "CLE",
                "opponent_nfl_team": "NYJ",
                "receptions": 2,
                "receiving_yards": 21,
            },
            {
                "player_week": "QB2_1986_18",
                "year": 1986,
                "week": 18,
                "season_type": "POST",
                "nfl_team": "DEN",
                "opponent_nfl_team": "CLE",
                "passing_yards": 200,
            },
            {
                "player_week": "QB3_1993_18",
                "year": 1993,
                "week": 18,
                "season_type": "POST",
                "nfl_team": "BUF",
                "opponent_nfl_team": "IND",
                "attempts": 34,
                "completions": 21,
                "passing_yards": 289,
                "passing_tds": 4,
            },
            {
                "player_week": "QB1_1986_17",
                "year": 1986,
                "week": 17,
                "season_type": "REG",
                "nfl_team": "CLE",
                "opponent_nfl_team": "PIT",
                "passing_yards": 100,
            },
        ]
    )
    rollup = pd.DataFrame(
        [
            {
                "player_week": "QB1_1986_18",
                "year": 1986,
                "week": 18,
                "season_type": "POST",
                "nfl_team": "CLE",
                "opponent_nfl_team": "NYJ",
                "attempts": 64,
                "completions": 33,
                "passing_yards": 489,
                "passing_tds": 1,
            },
            {
                "player_week": "WR1_1986_18",
                "year": 1986,
                "week": 18,
                "season_type": "POST",
                "nfl_team": "CLE",
                "opponent_nfl_team": "NYJ",
                "receptions": 2,
                "receiving_yards": 24,
            },
            {
                "player_week": "QB2_1986_18",
                "year": 1986,
                "week": 18,
                "season_type": "POST",
                "nfl_team": "CLE",
                "opponent_nfl_team": "DEN",
                "passing_yards": 360,
            },
            {
                "player_week": "QB3_1993_18",
                "year": 1993,
                "week": 18,
                "season_type": "REG",
                "nfl_team": "BUF",
                "opponent_nfl_team": "IND",
                "attempts": 1,
                "completions": 1,
                "passing_yards": 30,
                "passing_tds": 1,
            },
            {
                "player_week": "QB1_1986_17",
                "year": 1986,
                "week": 17,
                "season_type": "REG",
                "nfl_team": "CLE",
                "opponent_nfl_team": "PIT",
                "passing_yards": 350,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)

    out = apply_pbp_postseason_official_repairs(rows, rollup_path=rollup_path)
    by_key = out.set_index("player_week")

    assert by_key.loc["QB1_1986_18", "attempts"] == 64
    assert by_key.loc["QB1_1986_18", "passing_yards"] == 489
    assert by_key.loc["WR1_1986_18", "receiving_yards"] == 21
    assert by_key.loc["QB2_1986_18", "passing_yards"] == 200
    assert by_key.loc["QB3_1993_18", "season_type"] == "REG"
    assert by_key.loc["QB3_1993_18", "attempts"] == 1
    assert by_key.loc["QB3_1993_18", "passing_yards"] == 30
    assert by_key.loc["QB1_1986_17", "passing_yards"] == 100


def test_pbp_truth_atom_overlay_requires_pre_1999_context_match(tmp_path):
    rows = pd.DataFrame(
        [
            {
                "player_week": "K1_1983_13",
                "year": 1983,
                "week": 13,
                "season_type": "REG",
                "nfl_team": "NYJ",
                "opponent_nfl_team": "NE",
                "fg_yards": 0,
                "fg_long": 0,
                "fg_made_distance": 0,
                "kickoff_return_yards": 0,
                "receptions_40plus": 0,
            },
            {
                "player_week": "RET1_1981_12",
                "year": 1981,
                "week": 12,
                "season_type": "REG",
                "nfl_team": "ARI",
                "opponent_nfl_team": "DAL",
                "fg_yards": 0,
                "fg_long": 0,
                "fg_made_distance": 0,
                "kickoff_return_yards": 0,
                "receptions_40plus": 0,
            },
            {
                "player_week": "K2_1999_1",
                "year": 1999,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "MIN",
                "opponent_nfl_team": "ATL",
                "fg_yards": 10,
                "fg_long": 10,
                "fg_made_distance": 10,
                "kickoff_return_yards": 0,
                "receptions_40plus": 0,
            },
        ]
    )
    rollup = pd.DataFrame(
        [
            {
                "player_week": "K1_1983_13",
                "year": 1983,
                "week": 13,
                "season_type": "REG",
                "nfl_team": "NYJ",
                "opponent_nfl_team": "NWE",
                "fg_yards": 106,
                "fg_long": 35,
                "kickoff_return_yards": 0,
                "receptions_40plus": 0,
            },
            {
                "player_week": "RET1_1981_12",
                "year": 1981,
                "week": 12,
                "season_type": "REG",
                "nfl_team": "ARI",
                "opponent_nfl_team": "WAS",
                "fg_yards": 0,
                "fg_long": 0,
                "kickoff_return_yards": 56,
                "receptions_40plus": 1,
            },
            {
                "player_week": "K2_1999_1",
                "year": 1999,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "MIN",
                "opponent_nfl_team": "ATL",
                "fg_yards": 99,
                "fg_long": 51,
                "kickoff_return_yards": 0,
                "receptions_40plus": 0,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)

    out = apply_pbp_truth_atom_overlays(rows, rollup_path=rollup_path)
    by_key = out.set_index("player_week")

    assert by_key.loc["K1_1983_13", "fg_yards"] == 106
    assert by_key.loc["K1_1983_13", "fg_long"] == 35
    assert by_key.loc["K1_1983_13", "fg_made_distance"] == 106
    assert by_key.loc["RET1_1981_12", "kickoff_return_yards"] == 0
    assert by_key.loc["RET1_1981_12", "receptions_40plus"] == 0
    assert by_key.loc["K2_1999_1", "fg_yards"] == 10


def test_build_pbp_safe_missing_rows_includes_only_guarded_fantasy_rows(tmp_path):
    existing = pd.DataFrame({"player_week": ["EXISTING_1983_1"], "fg_att": [1.0]})
    rollup = pd.DataFrame(
        [
            {
                "player_week": "KICK_1983_1",
                "NFL_player_id": "KICK",
                "player": "Kicker",
                "position": "K",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "kicker",
                "fg_att": 2,
                "fg_made": 1,
                "fg_yards": 47,
                "pat_att": 3,
                "pat_made": 3,
            },
            {
                "player_week": "RET_1981_12",
                "NFL_player_id": "RET",
                "player": "Returner",
                "position": "RB",
                "nfl_team": "ARI",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1981,
                "week": 12,
                "season_type": "REG",
                "event_roles": "kickoff_returner;receiver",
                "kickoff_returns": 3,
                "kickoff_return_yards": 56,
                "receptions": 1,
                "receiving_yards": 9,
                "targets": 9,
            },
            {
                "player_week": "OFF_1982_17",
                "NFL_player_id": "OFF",
                "player": "Receiver",
                "position": "WR",
                "nfl_team": "CIN",
                "opponent_nfl_team": "TEN",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1982,
                "week": 17,
                "season_type": "REG",
                "event_roles": "receiver",
                "receptions": 5,
                "receiving_yards": 49,
                "receiving_tds": 1,
                "targets": 8,
            },
            {
                "player_week": "PUNT_1983_1",
                "NFL_player_id": "PUNT",
                "player": "Punter",
                "position": "P",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "punter",
                "punts": 5,
            },
            {
                "player_week": "IDP_1983_1",
                "NFL_player_id": "IDP",
                "player": "Linebacker",
                "position": "LB",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "solo_tackle_1",
                "def_tackles_solo": 7,
            },
            {
                "player_week": "FUM_1983_1",
                "NFL_player_id": "FUM",
                "player": "Fumble Guy",
                "position": "RB",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "fumble_recovery_1",
                "fum_rec": 1,
            },
            {
                "player_week": "MISSING_CONTEXT_1983_1",
                "NFL_player_id": "CTX",
                "player": "No Team",
                "position": "RB",
                "nfl_team": None,
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 0,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "kickoff_returner",
                "kickoff_return_yards": 80,
            },
            {
                "player_week": "EXISTING_1983_1",
                "NFL_player_id": "OLD",
                "player": "Existing",
                "position": "K",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "kicker",
                "fg_att": 5,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)
    columns = [
        "player_week",
        "NFL_player_id",
        "player",
        "position",
        "nfl_position",
        "fantasy_position",
        "nfl_team",
        "opponent_nfl_team",
        "year",
        "week",
        "season_type",
        "data_source",
        "safe_missing_bucket",
        "fg_att",
        "fg_made",
        "fg_yards",
        "fg_made_distance",
        "pat_att",
        "pat_made",
        "kickoff_returns",
        "kickoff_return_yards",
        "receptions",
        "receiving_yards",
        "receiving_tds",
        "targets",
    ]

    out = build_pbp_safe_missing_rows(existing, rollup_path=rollup_path, columns=columns)
    by_key = out.set_index("player_week")

    assert set(by_key.index) == {"KICK_1983_1", "RET_1981_12", "OFF_1982_17"}
    assert by_key.loc["KICK_1983_1", "safe_missing_bucket"] == "pure_kicker"
    assert by_key.loc["KICK_1983_1", "fg_made_distance"] == 47
    assert by_key.loc["RET_1981_12", "safe_missing_bucket"] == "returner"
    assert by_key.loc["RET_1981_12", "kickoff_return_yards"] == 56
    assert by_key.loc["RET_1981_12", "targets"] == 0
    assert by_key.loc["OFF_1982_17", "safe_missing_bucket"] == "offense"
    assert by_key.loc["OFF_1982_17", "receiving_yards"] == 49


def test_build_pbp_safe_missing_rows_skips_target_only_rows(tmp_path):
    existing = pd.DataFrame({"player_week": ["EXISTING_1999_1"]})
    rollup = pd.DataFrame(
        [
            {
                "player_week": "TARGET_ONLY_1999_1",
                "NFL_player_id": "TARGET_ONLY",
                "player": "Target Only",
                "position": "TE",
                "nfl_team": "CHI",
                "opponent_nfl_team": "GB",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1999,
                "week": 1,
                "season_type": "REG",
                "event_roles": "receiver",
                "targets": 1,
                "receptions": 0,
                "receiving_yards": 0,
                "receiving_tds": 0,
            }
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)

    out = build_pbp_safe_missing_rows(
        existing,
        rollup_path=rollup_path,
        columns=[
            "player_week",
            "NFL_player_id",
            "player",
            "position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
            "season_type",
            "data_source",
            "safe_missing_bucket",
            "receptions",
            "receiving_yards",
            "receiving_tds",
            "targets",
        ],
        min_year=1999,
        max_year=2025,
    )

    assert out.empty


def test_build_pbp_safe_missing_rows_can_stage_punters_only(tmp_path):
    existing = pd.DataFrame({"player_week": ["EXISTING_1983_1"]})
    rollup = pd.DataFrame(
        [
            {
                "player_week": "PUNT_1983_1",
                "NFL_player_id": "PUNT",
                "player": "Punter",
                "position": "P",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "punter;solo_tackle_1",
                "punts": 5,
                "punt_yards": 211,
                "punt_long": 54,
                "punts_blocked": 0,
            },
            {
                "player_week": "KICK_1983_1",
                "NFL_player_id": "KICK",
                "player": "Kicker",
                "position": "K",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "kicker",
                "fg_att": 1,
                "fg_made": 1,
                "fg_yards": 31,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)

    out = build_pbp_safe_missing_rows(
        existing,
        rollup_path=rollup_path,
        columns=[
            "player_week",
            "NFL_player_id",
            "player",
            "position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
            "season_type",
            "safe_missing_bucket",
            "punts",
            "punt_yards",
            "punt_long",
            "punts_blocked",
            "fg_made",
            "fg_yards",
        ],
        include_punters=True,
        punters_only=True,
    )

    assert out["player_week"].tolist() == ["PUNT_1983_1"]
    assert out.loc[0, "safe_missing_bucket"] == "punter"
    assert out.loc[0, "punts"] == 5
    assert out.loc[0, "punt_yards"] == 211
    assert out.loc[0, "punt_long"] == 54


def test_build_pbp_safe_missing_rows_can_stage_idp_only_without_fumble_only(tmp_path):
    existing = pd.DataFrame({"player_week": ["EXISTING_1983_1"]})
    rollup = pd.DataFrame(
        [
            {
                "player_week": "IDP_1983_1",
                "NFL_player_id": "IDP",
                "player": "Linebacker",
                "position": "LB",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "assist_tackle_1;solo_tackle_1",
                "def_tackles_solo": 6,
                "def_tackle_assists": 2,
                "def_tackles_with_assist": 2,
            },
            {
                "player_week": "BLOCK_1983_1",
                "NFL_player_id": "BLOCK",
                "player": "Blocker",
                "position": "DL",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "blocked_kick",
                "def_blk_kick": 1,
            },
            {
                "player_week": "FUM_ONLY_1983_1",
                "NFL_player_id": "FUM",
                "player": "Fumble Only",
                "position": "DB",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "fumble_recovery_1",
                "fum_rec": 1,
            },
            {
                "player_week": "OFF_1983_1",
                "NFL_player_id": "OFF",
                "player": "Receiver",
                "position": "WR",
                "nfl_team": "GB",
                "opponent_nfl_team": "DAL",
                "nfl_team_context_count": 1,
                "opponent_context_count": 1,
                "year": 1983,
                "week": 1,
                "season_type": "REG",
                "event_roles": "receiver",
                "receptions": 1,
                "receiving_yards": 12,
            },
        ]
    )
    rollup_path = tmp_path / "pbp_player_week_rollup.parquet"
    rollup.to_parquet(rollup_path, index=False)

    out = build_pbp_safe_missing_rows(
        existing,
        rollup_path=rollup_path,
        columns=[
            "player_week",
            "NFL_player_id",
            "player",
            "position",
            "nfl_team",
            "opponent_nfl_team",
            "year",
            "week",
            "season_type",
            "safe_missing_bucket",
            "def_tackles_solo",
            "def_tackle_assists",
            "def_tackles_with_assist",
            "def_blk_kick",
            "receptions",
            "receiving_yards",
        ],
        idp_only=True,
    )

    by_key = out.set_index("player_week")
    assert set(by_key.index) == {"IDP_1983_1", "BLOCK_1983_1"}
    assert by_key.loc["IDP_1983_1", "safe_missing_bucket"] == "idp"
    assert by_key.loc["IDP_1983_1", "def_tackles_solo"] == 6
    assert by_key.loc["IDP_1983_1", "def_tackle_assists"] == 2
    assert by_key.loc["BLOCK_1983_1", "def_blk_kick"] == 1
