from __future__ import annotations

import pandas as pd

from multi_league.data_fetchers.pfr_supertable_backfill import apply_pfr_supertable_update_package


def test_pfr_package_overlays_context_matches_and_appends_missing_rows(tmp_path):
    package_dir = tmp_path / "pfr_supertable_update_package_test"
    package_dir.mkdir()

    existing = pd.DataFrame(
        [
            {
                "player_week": "00-0000001_1984_1",
                "NFL_player_id": "00-0000001",
                "player": "Matched QB",
                "position": "QB",
                "nfl_position": "QB",
                "year": 1984,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "MIA",
                "opponent_nfl_team": "WAS",
                "passing_yards": 220.0,
                "rushing_yards": 0.0,
                "data_source": "historical",
            },
            {
                "player_week": "00-0000002_1984_1",
                "NFL_player_id": "00-0000002",
                "player": "Wrong Context QB",
                "position": "QB",
                "nfl_position": "QB",
                "year": 1984,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "SF",
                "opponent_nfl_team": "DAL",
                "passing_yards": 100.0,
                "rushing_yards": 0.0,
                "data_source": "historical",
            },
        ]
    )
    safe_updates = pd.DataFrame(
        [
            {
                "player_week": "00-0000001_1984_1",
                "year": 1984,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "MIA",
                "opponent_nfl_team": "WAS",
                "passing_yards": 340.0,
                "rushing_yards": 4.0,
            },
            {
                "player_week": "00-0000002_1984_1",
                "year": 1984,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "DAL",
                "opponent_nfl_team": "SF",
                "passing_yards": 999.0,
                "rushing_yards": 9.0,
            },
        ]
    )
    inserts = pd.DataFrame(
        [
            {
                "player_week": "00-0000003_1984_1",
                "NFL_player_id": "00-0000003",
                "player": "Missing RB",
                "position": "RB",
                "nfl_position": "RB",
                "year": 1984,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "CHI",
                "opponent_nfl_team": "GB",
                "passing_yards": 0.0,
                "rushing_yards": 88.0,
                "data_source": "pfr_boxscore_weekly_insert",
            }
        ]
    )
    safe_updates.to_parquet(package_dir / "pfr_supertable_safe_stat_updates_wide.parquet", index=False)
    inserts.to_parquet(package_dir / "pfr_supertable_insert_ready_rows.parquet", index=False)

    out = apply_pfr_supertable_update_package(existing, package_dir=package_dir)
    by_key = out.set_index("player_week")

    assert len(out) == 3
    assert by_key.loc["00-0000001_1984_1", "passing_yards"] == 340.0
    assert by_key.loc["00-0000001_1984_1", "rushing_yards"] == 4.0
    assert by_key.loc["00-0000002_1984_1", "passing_yards"] == 100.0
    assert by_key.loc["00-0000003_1984_1", "player"] == "Missing RB"
    assert by_key.loc["00-0000003_1984_1", "rushing_yards"] == 88.0


def test_pfr_package_context_matches_common_team_code_aliases(tmp_path):
    package_dir = tmp_path / "pfr_supertable_update_package_test"
    package_dir.mkdir()

    existing = pd.DataFrame(
        [
            {
                "player_week": "00-0007744_2003_15",
                "NFL_player_id": "00-0007744",
                "player": "Joe Horn",
                "position": "WR",
                "year": 2003,
                "week": 15,
                "season_type": "REG",
                "nfl_team": "NO",
                "opponent_nfl_team": "NYG",
                "targets": 0.0,
                "receptions": 9.0,
            }
        ]
    )
    safe_updates = pd.DataFrame(
        [
            {
                "player_week": "00-0007744_2003_15",
                "year": 2003,
                "week": 15,
                "season_type": "REG",
                "nfl_team": "NOR",
                "opponent_nfl_team": "NYG",
                "targets": 12.0,
                "receptions": 9.0,
            }
        ]
    )
    inserts = pd.DataFrame(columns=["player_week"])
    safe_updates.to_parquet(package_dir / "pfr_supertable_safe_stat_updates_wide.parquet", index=False)
    inserts.to_parquet(package_dir / "pfr_supertable_insert_ready_rows.parquet", index=False)

    out = apply_pfr_supertable_update_package(existing, package_dir=package_dir)

    assert out.loc[0, "targets"] == 12.0


def test_pfr_package_skips_when_explicit_package_missing(tmp_path):
    existing = pd.DataFrame(
        [
            {
                "player_week": "00-0000001_1984_1",
                "year": 1984,
                "week": 1,
                "season_type": "REG",
                "nfl_team": "MIA",
                "opponent_nfl_team": "WAS",
                "passing_yards": 220.0,
            }
        ]
    )

    out = apply_pfr_supertable_update_package(existing, package_dir=tmp_path / "missing_package")

    assert len(out) == 1
    assert out.loc[0, "passing_yards"] == 220.0
