from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers import defense_stats
from nfl_data.team_margin import team_margin_values


def test_transform_to_defensive_stats_uses_net_yards_allowed(monkeypatch):
    monkeypatch.setattr(defense_stats, "FRANCHISE_FUNCTIONS_AVAILABLE", False)

    df = pd.DataFrame(
        [
            {
                "team": "CHI",
                "opponent_team": "BAL",
                "season": 2025,
                "week": 9,
                "season_type": "REG",
                "passing_yards": 150,
                "rushing_yards": 80,
                "passing_tds": 1,
                "rushing_tds": 0,
                "receiving_tds": 1,
                "fg_made": 1,
                "fg_blocked": 0,
                "pat_made": 1,
                "passing_2pt_conversions": 0,
                "rushing_2pt_conversions": 0,
                "receiving_2pt_conversions": 0,
                "def_sacks": 3,
                "def_sack_yards": 21,
                "def_qb_hits": 0,
                "def_interceptions": 2,
                "def_interception_yards": 0,
                "def_pass_defended": 0,
                "def_tackles_solo": 0,
                "def_tackles_with_assist": 0,
                "def_tackle_assists": 0,
                "def_tackles_for_loss": 4,
                "def_tackles_for_loss_yards": 0,
                "def_fumbles_forced": 0,
                "def_tds": 0,
                "def_fumbles": 0,
                "def_safeties": 0,
                "special_teams_tds": 0,
                "fumble_recovery_opp": 1,
                "fumble_recovery_tds": 0,
                "misc_yards": 0,
                "penalties": 0,
                "penalty_yards": 0,
                "timeouts": 0,
            },
            {
                "team": "BAL",
                "opponent_team": "CHI",
                "season": 2025,
                "week": 9,
                "season_type": "REG",
                "passing_yards": 316,
                "rushing_yards": 200,
                "passing_tds": 2,
                "rushing_tds": 1,
                "receiving_tds": 2,
                "fg_made": 1,
                "fg_blocked": 0,
                "pat_made": 3,
                "passing_2pt_conversions": 0,
                "rushing_2pt_conversions": 0,
                "receiving_2pt_conversions": 0,
                "def_sacks": 0,
                "def_sack_yards": 0,
                "def_qb_hits": 0,
                "def_interceptions": 0,
                "def_interception_yards": 0,
                "def_pass_defended": 0,
                "def_tackles_solo": 0,
                "def_tackles_with_assist": 0,
                "def_tackle_assists": 0,
                "def_tackles_for_loss": 0,
                "def_tackles_for_loss_yards": 0,
                "def_fumbles_forced": 0,
                "def_tds": 0,
                "def_fumbles": 0,
                "def_safeties": 0,
                "special_teams_tds": 0,
                "fumble_recovery_opp": 0,
                "fumble_recovery_tds": 0,
                "misc_yards": 0,
                "penalties": 0,
                "penalty_yards": 0,
                "timeouts": 0,
            },
        ]
    )

    transformed = defense_stats.transform_to_defensive_stats(df)
    bears = transformed[transformed["nfl_team"] == "CHI"].iloc[0]

    assert bears["passing_yds_allowed"] == 316
    assert bears["rushing_yds_allowed"] == 200
    assert bears["total_yds_allowed"] == 495
    # Total points allowed = BAL's full score: 3 offensive TDs (18) + 3 PAT (3) + 1 FG (3) = 24
    assert bears["points_allowed"] == 24
    assert bears["dst_points_allowed"] == 24  # no pick-6 / safety -> eligible == total


def test_transform_points_allowed_includes_return_tds_and_safety(monkeypatch):
    """points_allowed must count the opponent's NON-offensive scoring (pick-6, ST-return TD, safety),
    and eligible PA (dst_points_allowed) must exclude the pick-6 + safety but keep the ST-return TD."""
    monkeypatch.setattr(defense_stats, "FRANCHISE_FUNCTIONS_AVAILABLE", False)

    def team_row(team, opp, **overrides):
        base = {
            "team": team, "opponent_team": opp, "season": 2025, "week": 10, "season_type": "REG",
            "passing_yards": 0, "rushing_yards": 0, "passing_tds": 0, "rushing_tds": 0, "receiving_tds": 0,
            "fg_made": 0, "fg_blocked": 0, "pat_made": 0,
            "passing_2pt_conversions": 0, "rushing_2pt_conversions": 0, "receiving_2pt_conversions": 0,
            "def_sacks": 0, "def_sack_yards": 0, "def_qb_hits": 0, "def_interceptions": 0,
            "def_interception_yards": 0, "def_pass_defended": 0, "def_tackles_solo": 0,
            "def_tackles_with_assist": 0, "def_tackle_assists": 0, "def_tackles_for_loss": 0,
            "def_tackles_for_loss_yards": 0, "def_fumbles_forced": 0, "def_tds": 0, "def_fumbles": 0,
            "def_safeties": 0, "special_teams_tds": 0, "fumble_recovery_opp": 0, "fumble_recovery_tds": 0,
            "misc_yards": 0, "penalties": 0, "penalty_yards": 0, "timeouts": 0,
        }
        base.update(overrides)
        return base

    # BAL scores against CHI: 1 rushing TD (offense) + 1 pick-6 + 1 return TD + 1 safety, 3 PATs.
    df = pd.DataFrame([
        team_row("CHI", "BAL"),
        team_row("BAL", "CHI", rushing_tds=1, def_tds=1, special_teams_tds=1, def_safeties=1, pat_made=3),
    ])
    transformed = defense_stats.transform_to_defensive_stats(df)
    bears = transformed[transformed["nfl_team"] == "CHI"].iloc[0]

    # Total = 3 TDs (18) + 3 PAT (3) + safety (2) = 23
    assert bears["points_allowed"] == 23
    # Eligible = 23 - pick-6 (6) - safety (2) = 15 (keeps offensive TD + ST-return TD + PATs)
    assert bears["dst_points_allowed"] == 15
    assert bears["pts_allow"] == 15  # buckets tier off eligible


def test_compute_advanced_def_columns_splits_block_types():
    def play(defteam, **overrides):
        base = {
            "season": 2025,
            "week": 3,
            "defteam": defteam,
            "posteam": "NO",
            "play_type": "punt",
            "touchdown": 0,
            "td_team": None,
            "interception": 0,
            "fumble": 0,
            "fumble_recovery_1_team": None,
            "fumble_forced": 0,
            "punt_blocked": 0,
            "field_goal_result": "",
            "extra_point_result": "",
            "two_point_attempt": 0,
            "defteam_score": 0,
            "defteam_score_post": 0,
            "return_yards": 0,
            "drive": 1,
            "punt_attempt": 0,
        }
        base.update(overrides)
        return base

    pbp = pd.DataFrame(
        [
            play("SEA", punt_blocked=1, punt_attempt=1),
            play("PHI", play_type="field_goal", field_goal_result="blocked"),
            play("NYJ", play_type="extra_point", extra_point_result="blocked"),
        ]
    )
    team_stats = pd.DataFrame(
        [
            {
                "season": 2025,
                "week": 3,
                "team": team,
                "def_pass_defended": 0,
                "def_sack_yards": 0,
                "def_sacks": 0,
                "def_tackles_solo": 0,
                "def_tackles_with_assist": 0,
                "def_tackle_assists": 0,
            }
            for team in ["SEA", "PHI", "NYJ"]
        ]
    )

    out = defense_stats.compute_advanced_def_columns(pbp, team_stats).set_index("nfl_team")

    assert out.loc["SEA", "pts_def_punt_block"] == 1
    assert out.loc["SEA", "pts_def_block"] == 1
    assert out.loc["PHI", "pts_def_fg_block"] == 1
    assert out.loc["PHI", "pts_def_block"] == 1
    assert out.loc["NYJ", "pts_def_pat_block"] == 1
    assert out.loc["NYJ", "pts_def_block"] == 1


def test_compute_advanced_def_columns_tackle_bonus_uses_solo_plus_assists():
    pbp = pd.DataFrame(
        [
            {
                "season": 2025,
                "week": 4,
                "defteam": "DEN",
                "posteam": "KC",
                "play_type": "run",
                "touchdown": 0,
                "td_team": None,
                "interception": 0,
                "fumble": 0,
                "fumble_recovery_1_team": None,
                "fumble_forced": 0,
                "punt_blocked": 0,
                "field_goal_result": "",
                "extra_point_result": "",
                "two_point_attempt": 0,
                "defteam_score": 0,
                "defteam_score_post": 0,
                "return_yards": 0,
                "drive": 1,
                "punt_attempt": 0,
            }
        ]
    )
    team_stats = pd.DataFrame(
        [
            {
                "season": 2025,
                "week": 4,
                "team": "DEN",
                "def_pass_defended": 0,
                "def_sack_yards": 0,
                "def_sacks": 0,
                "def_tackles_solo": 7,
                "def_tackle_assists": 3,
                "def_tackles_with_assist": 0,
            },
        ]
    )

    out = defense_stats.compute_advanced_def_columns(pbp, team_stats).set_index("nfl_team")

    assert out.loc["DEN", "pts_def_bonus_tkl_10p"] == 1


def test_team_margin_values_bucket_win_loss_and_score():
    seahawks = team_margin_values(44, 13)
    saints = team_margin_values(13, 44)

    assert seahawks["pts_def_team_win"] == 1
    assert seahawks["pts_def_team_pts"] == 44
    assert seahawks["pts_def_team_margin"] == 31
    assert seahawks["pts_def_team_win_margin_25p"] == 1
    assert saints["pts_def_team_loss"] == 1
    assert saints["pts_def_team_pts"] == 13
    assert saints["pts_def_team_margin"] == -31
    assert saints["pts_def_team_loss_margin_25p"] == 1
