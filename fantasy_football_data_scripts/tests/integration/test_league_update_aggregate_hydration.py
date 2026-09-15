import pandas as pd

from multi_league.core.league_refresh import (
    hydrate_local_refresh_sources,
    merge_provider_refresh_table,
)
from multi_league.core.local_db import LocalLeagueDB


def test_active_refresh_hydrates_existing_noncanonical_career_and_homepage_tables(tmp_path):
    """The real ESPN source snapshot includes aggregates without local DDL."""
    local = LocalLeagueDB(tmp_path, "afi_data")
    try:
        frames = {
            "draft_manager_career": pd.DataFrame([{
                "db_name": "afi_data", "franchise_id": "f-gray", "manager": "Gray",
                "picks": 14,
            }]),
            "homepage_manager_profiles": pd.DataFrame([{
                "db_name": "afi_data", "franchise_id": "f-gray", "manager": "Gray",
                "games": 1,
            }]),
        }

        assert hydrate_local_refresh_sources(
            local, frames, db_name="afi_data", active_year=2026,
            expected_platform="espn",
        ) == {"draft_manager_career": 1, "homepage_manager_profiles": 1}
        assert local.read_table("draft_manager_career")["picks"].tolist() == [14]
        assert local.read_table("homepage_manager_profiles")["games"].tolist() == [1]
    finally:
        local.close()


def test_espn_flat_settings_drop_noncanonical_raw_payload_before_ownership_merge(tmp_path):
    local = LocalLeagueDB(tmp_path, "afi_data")
    try:
        local.ensure_table("league_settings")
        incoming = pd.DataFrame([{
            "db_name": "afi_data", "year": 2026, "platform": "espn",
            "scoring_type": "H2H",
            "_raw": {"scoringSettings": {"scoringItems": []}},
        }])

        merge_provider_refresh_table(
            local, "league_settings", incoming,
            platform="espn", league_id="1633155277",
        )
        stored = local.read_table("league_settings")
        assert len(stored) == 1
        assert "_raw" not in stored.columns
        assert stored["scoring_type"].tolist() == ["H2H"]
    finally:
        local.close()


def test_espn_roster_overlay_uses_provider_identity_before_player_week_rebuild(tmp_path):
    local = LocalLeagueDB(tmp_path, "afi_data")
    try:
        local.ensure_table("player_fantasy")
        local._insert_into_table("player_fantasy", pd.DataFrame([{
            "db_name": "afi_data", "year": 2026, "week": 1,
            "team_key": "10", "manager": "Gray", "espn_player_id": "-16012",
            "NFL_player_id": "chiefs-def", "player_week": "chiefs-def_2026_1",
            "fantasy_points": 13.0, "clutch_equity": 4.5,
        }]))
        incoming = pd.DataFrame([{
            "year": 2026, "week": 1, "team_key": "10", "manager": "Gray",
            "espn_player_id": "-16012", "fantasy_points": 14.0,
        }])

        merge_provider_refresh_table(
            local, "player_fantasy", incoming,
            platform="espn", league_id="1633155277",
        )
        stored = local.read_table("player_fantasy")
        assert len(stored) == 1
        assert stored["fantasy_points"].tolist() == [14.0]
        assert stored["player_week"].tolist() == ["chiefs-def_2026_1"]
        assert stored["NFL_player_id"].tolist() == ["chiefs-def"]
        assert stored["clutch_equity"].tolist() == [4.5]
    finally:
        local.close()
