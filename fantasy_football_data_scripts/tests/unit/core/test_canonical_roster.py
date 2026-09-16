import pandas as pd
import duckdb
import pytest

from multi_league.core.canonical_roster import get_sql_enrichments, normalize_roster_df


def test_normalize_roster_df_normalizes_fantasy_position():
    """Platform-specific fantasy_position aliases must resolve to canonical names."""
    df = pd.DataFrame(
        {
            "year": [2024] * 10,
            "week": [1] * 10,
            "manager": ["A"] * 10,
            "manager_guid": ["abc12345"] * 10,
            "player": [f"P{i}" for i in range(10)],
            "fantasy_position": [
                "W/R/T",
                "FLEX",
                "Q/W/R/T",
                "IDP_FLEX",
                "BN",
                None,
                "OP",
                "W/T",
                "DST",
                "D/ST",
            ],
            "fantasy_points": [10, 15, 20, 5, 3, 0, 8, 12, 7, 7],
        }
    )
    result = normalize_roster_df(df, platform="yahoo")
    positions = result["fantasy_position"].tolist()
    assert positions[0] == "FLX"  # W/R/T -> FLX (Yahoo)
    assert positions[1] == "FLX"  # FLEX -> FLX (Sleeper/ESPN)
    assert positions[2] == "SUPER_FLEX"  # Q/W/R/T -> SUPER_FLEX (Yahoo)
    assert positions[3] == "IDP"  # IDP_FLEX -> IDP (Sleeper)
    assert positions[4] == "BN"  # BN stays BN
    assert pd.isna(positions[5])  # None stays None
    assert positions[6] == "SUPER_FLEX"  # OP -> SUPER_FLEX (Sleeper)
    assert positions[7] == "REC_FLEX"  # W/T -> REC_FLEX (Yahoo)
    assert positions[8] == "DEF"  # DST -> DEF (ESPN)
    assert positions[9] == "DEF"  # D/ST -> DEF (ESPN)
    # Verify is_started computed from NORMALIZED positions (FLX is a starter)
    assert result["is_started"].iloc[0] == 1  # FLX = starter
    assert result["is_started"].iloc[4] == 0  # BN = bench


class TestNormalizeRosterDF:
    def test_preserves_fetcher_is_rostered_flag(self):
        df = pd.DataFrame(
            {
                "year": [2025],
                "week": [1],
                "manager": ["alice"],
                "player": ["Bench Player"],
                "position": ["WR"],
                "fantasy_position": ["BN"],
                "is_rostered": [True],
            }
        )

        result = normalize_roster_df(df, platform="sleeper", league_id="league123")

        assert "is_rostered" in result.columns
        assert int(result.loc[0, "is_rostered"]) == 1
        assert int(result.loc[0, "is_started"]) == 0

    def test_infers_is_rostered_from_manager_when_missing(self):
        df = pd.DataFrame(
            {
                "year": [2025, 2025],
                "week": [1, 1],
                "manager": ["alice", "Unrostered"],
                "player": ["Rostered Player", "Free Agent"],
                "position": ["RB", "RB"],
                "fantasy_position": ["RB", "BN"],
            }
        )

        result = normalize_roster_df(df, platform="yahoo", league_id="league123")

        assert list(result["is_rostered"].astype("Int64")) == [1, 0]
        assert list(result["is_started"].astype("Int64")) == [1, 0]

    def test_hidden_yahoo_guid_does_not_seed_franchise_id(self):
        df = pd.DataFrame(
            {
                "year": [2003, 2003],
                "week": [1, 1],
                "manager": ["Hidden Team", "Known Manager"],
                "manager_guid": ["--hidden--", "known-guid"],
                "franchise_id": ["--hidden--", None],
                "team_name": ["Hidden Team", "Known Team"],
                "player": ["Player A", "Player B"],
                "fantasy_position": ["QB", "RB"],
            }
        )

        result = normalize_roster_df(df, platform="yahoo", league_id="league123")

        assert pd.isna(result.loc[0, "franchise_id"])
        assert result.loc[0, "manager_week"] == "HiddenTeam200301"
        assert result.loc[1, "franchise_id"] == "known-guid"

    def test_platform_id_enrichment_corrects_stale_nfl_player_id(self):
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("ATTACH ':memory:' AS test_db")
            conn.execute("CREATE SCHEMA test_db.public")
            conn.execute("ATTACH ':memory:' AS ___ops")
            conn.execute("CREATE SCHEMA ___ops.nfl_historical")
            conn.execute(
                """
                CREATE TABLE test_db.public.player_fantasy (
                    yahoo_player_id VARCHAR,
                    sleeper_player_id VARCHAR,
                    espn_player_id VARCHAR,
                    NFL_player_id VARCHAR,
                    player_week VARCHAR,
                    year INTEGER,
                    week INTEGER,
                    manager_guid VARCHAR,
                    is_started INTEGER,
                    projected_points DOUBLE
                )
                """
            )
            conn.execute(
                """
                INSERT INTO test_db.public.player_fantasy
                VALUES (NULL, '6888', NULL, '00-0018093', '00-0018093_2025_8', 2025, 8, 'mgr-1', 1, NULL)
                """
            )
            conn.execute(
                """
                CREATE TABLE ___ops.nfl_historical.player_bio (
                    NFL_player_id VARCHAR,
                    yahoo_player_id DOUBLE,
                    sleeper_player_id DOUBLE,
                    espn_id VARCHAR
                )
                """
            )
            conn.execute(
                """
                INSERT INTO ___ops.nfl_historical.player_bio
                VALUES ('00-0036411', NULL, 6888, NULL)
                """
            )
            conn.execute(
                "CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (player_week VARCHAR, nfl_team VARCHAR)"
            )
            conn.execute(
                """
                CREATE TABLE test_db.public.matchup (
                    year INTEGER,
                    week INTEGER,
                    manager_guid VARCHAR,
                    opponent_guid VARCHAR,
                    team_projected_points DOUBLE,
                    opponent_projected_points DOUBLE
                )
                """
            )

            for stmt in get_sql_enrichments("test_db"):
                conn.execute(stmt)

            row = conn.execute(
                """
                SELECT NFL_player_id, player_week
                FROM test_db.public.player_fantasy
                """
            ).fetchone()
        finally:
            conn.close()

        assert row == ("00-0036411", "00-0036411_2025_8")

    def test_sql_enrichment_builds_unmapped_player_week_fallback(self):
        conn = duckdb.connect(":memory:")
        try:
            conn.execute("ATTACH ':memory:' AS test_db")
            conn.execute("CREATE SCHEMA test_db.public")
            conn.execute("ATTACH ':memory:' AS ___ops")
            conn.execute("CREATE SCHEMA ___ops.nfl_historical")
            conn.execute(
                """
                CREATE TABLE test_db.public.player_fantasy (
                    yahoo_player_id VARCHAR,
                    sleeper_player_id VARCHAR,
                    espn_player_id VARCHAR,
                    NFL_player_id VARCHAR,
                    player_week VARCHAR,
                    player VARCHAR,
                    fantasy_position VARCHAR,
                    year INTEGER,
                    week INTEGER,
                    manager_week VARCHAR,
                    manager_guid VARCHAR,
                    is_started INTEGER,
                    projected_points DOUBLE
                )
                """
            )
            conn.execute(
                """
                INSERT INTO test_db.public.player_fantasy
                VALUES (NULL, '999999999', NULL, NULL, NULL, 'Mystery Player', 'BN', 2025, 17, 'mgr_2025_17', 'mgr', 0, NULL)
                """
            )
            conn.execute(
                """
                CREATE TABLE ___ops.nfl_historical.player_bio (
                    NFL_player_id VARCHAR,
                    yahoo_player_id DOUBLE,
                    sleeper_player_id DOUBLE,
                    espn_id VARCHAR
                )
                """
            )
            conn.execute(
                "CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (player_week VARCHAR, nfl_team VARCHAR)"
            )
            conn.execute(
                """
                CREATE TABLE test_db.public.matchup (
                    year INTEGER,
                    week INTEGER,
                    manager_guid VARCHAR,
                    opponent_guid VARCHAR,
                    team_projected_points DOUBLE,
                    opponent_projected_points DOUBLE
                )
                """
            )

            for stmt in get_sql_enrichments("test_db"):
                conn.execute(stmt)

            player_week = conn.execute("SELECT player_week FROM test_db.public.player_fantasy").fetchone()[0]
        finally:
            conn.close()

        assert player_week.startswith("UNMAPPED_")
        assert player_week.endswith("_2025_17")
@pytest.mark.parametrize("platform,provider_id", [("yahoo", "100008"), ("espn", "8")])
def test_normalize_defense_uses_shared_franchise_identity_for_all_providers(platform, provider_id):
    raw = pd.DataFrame([
        {"year": 2026, "week": 1, "player": "Detroit", "position": "DEF",
         "nfl_team": "DET", f"{platform}_player_id": provider_id, "fantasy_position": "BN"},
        {"year": 2026, "week": 1, "player": "Individual Defender", "position": "LB",
         "nfl_team": "DET", f"{platform}_player_id": "123", "fantasy_position": "D"},
    ])
    result = normalize_roster_df(raw, platform=platform)
    assert result.iloc[0]["NFL_player_id"] == "DEF-6"
    assert result.iloc[0]["player_week"] == "DEF-6_2026_1"
    assert result.iloc[0][f"{platform}_player_id"] == provider_id
    assert pd.isna(result.iloc[1]["NFL_player_id"])


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_unresolved_defenses_do_not_get_colliding_none_player_week_keys(platform):
    raw = pd.DataFrame([
        {"year": 2026, "week": 1, "player": "Unknown Defense A", "position": "DEF",
         f"{platform}_player_id": "991", "fantasy_position": "BN"},
        {"year": 2026, "week": 1, "player": "Unknown Defense B", "position": "DEF",
         f"{platform}_player_id": "992", "fantasy_position": "DEF"},
    ])
    result = normalize_roster_df(raw, platform=platform)
    assert result["NFL_player_id"].isna().all()
    assert result["player_week"].isna().all()


@pytest.mark.parametrize("year,week", [("2026", "1.0"), ("2026.0", "1")])
def test_defense_normalizer_accepts_numeric_provider_season_week_strings(year, week):
    raw = pd.DataFrame([{
        "year": year, "week": week, "position": "DEF", "nfl_team": "DET",
        "yahoo_player_id": "100008", "fantasy_position": "DEF",
    }])
    result = normalize_roster_df(raw, platform="yahoo")
    assert result["NFL_player_id"].tolist() == ["DEF-6"]
    assert result["player_week"].tolist() == ["DEF-6_2026_1"]
