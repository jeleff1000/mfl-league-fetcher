import pandas as pd

from multi_league.core.league_refresh import (
    hydrate_local_refresh_sources,
    merge_provider_refresh_table,
)
from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.aggregate_ddl import ensure_aggregate_table


def test_existing_career_hydration_preserves_canonical_integer_and_timestamp_types(tmp_path):
    """The full weekly aggregate runner must be able to rebuild the preserved table."""
    local = LocalLeagueDB(tmp_path, "afi_data")
    try:
        source = pd.DataFrame([{
            "db_name": "afi_data", "NFL_player_id": "00-001",
            "wins": 61, "games_rostered": 101,
            "last_updated": "2026-09-15 12:00:00",
        }])
        hydrate_local_refresh_sources(
            local, {"player_fantasy_career": source},
            db_name="afi_data", active_year=2026, expected_platform="espn",
        )
        catalog = local.connect().execute("SELECT current_database()").fetchone()[0]
        ensure_aggregate_table(local.connect(), catalog, "player_fantasy_career")
        rows = local.connect().execute(
            "SELECT wins, games_rostered, last_updated "
            "FROM public.player_fantasy_career WHERE db_name='afi_data'"
        ).fetchall()
        assert rows[0][0:2] == (61, 101)
        types = {
            name: dtype for name, dtype, *_ in
            local.connect().execute("DESCRIBE public.player_fantasy_career").fetchall()
        }
        assert types["wins"] == "INTEGER"
        assert types["last_updated"] == "TIMESTAMP"
    finally:
        local.close()


def test_active_refresh_hydrates_existing_career_and_homepage_tables(tmp_path):
    """The ESPN source snapshot uses canonical aggregate DDL, not Pandas inference."""
    local = LocalLeagueDB(tmp_path, "afi_data")
    try:
        frames = {
            "draft_manager_career": pd.DataFrame([{
                "db_name": "afi_data", "franchise_id": "f-gray", "manager": "Gray",
                "draft_category": "snake",
                "total_picks": 14,
            }]),
            "homepage_manager_profiles": pd.DataFrame([{
                "db_name": "afi_data", "franchise_id": "f-gray", "manager": "Gray",
                "best_career_lamar_value": 1.0,
            }]),
        }

        assert hydrate_local_refresh_sources(
            local, frames, db_name="afi_data", active_year=2026,
            expected_platform="espn",
        ) == {"draft_manager_career": 1, "homepage_manager_profiles": 1}
        assert local.read_table("draft_manager_career")["total_picks"].tolist() == [14]
        assert local.read_table("homepage_manager_profiles")["best_career_lamar_value"].tolist() == [1.0]
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


def test_matchup_refresh_replaces_a_prior_enriched_team_week_by_provider_identity(tmp_path):
    """A second identical update cannot append a row when manager_week was rebuilt."""
    local = LocalLeagueDB(tmp_path, "afi_data")
    try:
        local.ensure_table("matchup")
        local._insert_into_table("matchup", pd.DataFrame([{
            "db_name": "afi_data", "year": 2026, "week": 1,
            "team_key": "1", "manager": "Gray", "manager_guid": "stable-owner",
            "manager_week": "stable-owner_2026_1", "opponent": "Tani",
            "team_points": 101.0, "opponent_points": 99.0,
        }]))
        incoming = pd.DataFrame([{
            "year": 2026, "week": 1, "team_key": "1",
            "manager": "Gray", "opponent": "Tani",
            "team_points": 102.0, "opponent_points": 99.0,
        }])

        merge_provider_refresh_table(
            local, "matchup", incoming,
            platform="sleeper", league_id="1352102370921705472",
        )

        stored = local.read_table("matchup")
        assert len(stored) == 1
        assert stored["team_points"].tolist() == [102.0]
    finally:
        local.close()
