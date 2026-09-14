import pandas as pd
import pytest
import duckdb

from multi_league.data_fetchers.live_nfl_ops_refresh import (
    REQUIRED_OPS_TABLES,
    RefreshGateError,
    align_facts_to_wide_schema,
    assert_complete_ops_artifact,
    assert_ops_schema_compatible,
    build_player_bio_rows,
    discover_scheduled_refresh_scope,
    finalized_game_scope,
    prepare_weekly_facts,
    replace_completed_game_rows,
    upsert_player_bio_rows,
)


def test_finalized_game_scope_admits_only_scored_regular_season_games():
    """A premature refresh must not treat an unscored game as ready to publish."""
    schedule = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-09",
                "away_team": "NE",
                "away_score": 10,
                "home_team": "SEA",
                "home_score": 13,
            },
            {
                "game_id": "2026_01_DAL_NYG",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-13",
                "away_team": "DAL",
                "away_score": None,
                "home_team": "NYG",
                "home_score": None,
            },
            {
                "game_id": "2026_PRE_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "PRE",
                "gameday": "2026-08-09",
                "away_team": "NE",
                "away_score": 10,
                "home_team": "SEA",
                "home_score": 13,
            },
        ]
    )

    games = finalized_game_scope(schedule, year=2026, week=1)

    assert games.to_dict("records") == [
        {
            "game_id": "2026_01_NE_SEA",
            "game_date": "2026-09-09",
            "away_team": "NE",
            "away_score": 10.0,
            "home_team": "SEA",
            "home_score": 13.0,
        }
    ]


def test_finalized_game_scope_rejects_duplicate_game_ids():
    """A duplicate schedule identity must stop the worker before a replace scope is built."""
    schedule = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-09",
                "away_team": "NE",
                "away_score": 10,
                "home_team": "SEA",
                "home_score": 13,
            },
            {
                "game_id": "2026_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-09",
                "away_team": "NE",
                "away_score": 10,
                "home_team": "SEA",
                "home_score": 13,
            },
        ]
    )

    with pytest.raises(RefreshGateError, match="duplicate finalized game_id"):
        finalized_game_scope(schedule, year=2026, week=1)


def test_finalized_game_scope_admits_a_scored_postseason_game_on_the_requested_date():
    schedule = pd.DataFrame(
        [
            {
                "game_id": "2026_19_NE_SEA",
                "season": 2026,
                "week": 19,
                "game_type": "POST",
                "gameday": "2027-01-17",
                "away_team": "NE",
                "away_score": 10,
                "home_team": "SEA",
                "home_score": 13,
            },
            {
                "game_id": "2026_19_KC_BUF",
                "season": 2026,
                "week": 19,
                "game_type": "POST",
                "gameday": "2027-01-18",
                "away_team": "KC",
                "away_score": 21,
                "home_team": "BUF",
                "home_score": 24,
            },
        ]
    )

    games = finalized_game_scope(
        schedule,
        year=2026,
        week=19,
        season_types=("POST",),
        game_date="2027-01-17",
    )

    assert games["game_id"].tolist() == ["2026_19_NE_SEA"]


def test_discover_scheduled_refresh_scope_returns_none_without_a_final_game():
    schedule = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-10",
                "away_team": "NE",
                "away_score": None,
                "home_team": "SEA",
                "home_score": None,
            }
        ]
    )

    assert discover_scheduled_refresh_scope(schedule, season=2026, game_date="2026-09-10") is None


def test_discover_scheduled_refresh_scope_returns_one_exact_ready_work_item():
    schedule = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-10",
                "away_team": "NE",
                "away_score": 10,
                "home_team": "SEA",
                "home_score": 13,
            },
            {
                "game_id": "2026_01_DAL_NYG",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-13",
                "away_team": "DAL",
                "away_score": None,
                "home_team": "NYG",
                "home_score": None,
            },
        ]
    )

    assert discover_scheduled_refresh_scope(schedule, season=2026, game_date="2026-09-10") == {
        "year": 2026,
        "week": 1,
        "season_type": "REG",
        "game_date": "2026-09-10",
        "game_ids": ["2026_01_NE_SEA"],
    }


def test_discover_scheduled_refresh_scope_rejects_multiple_work_item_keys():
    schedule = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "season": 2026,
                "week": 1,
                "game_type": "REG",
                "gameday": "2026-09-10",
                "away_team": "NE",
                "away_score": 10,
                "home_team": "SEA",
                "home_score": 13,
            },
            {
                "game_id": "2026_02_DAL_NYG",
                "season": 2026,
                "week": 2,
                "game_type": "REG",
                "gameday": "2026-09-10",
                "away_team": "DAL",
                "away_score": 14,
                "home_team": "NYG",
                "home_score": 17,
            },
        ]
    )

    with pytest.raises(RefreshGateError, match="multiple finalized live refresh work items"):
        discover_scheduled_refresh_scope(schedule, season=2026, game_date="2026-09-10")


def test_prepare_weekly_facts_keeps_valid_players_and_derives_dst_game_context():
    """Blank-ID provider rows cannot become canonical records, while DST rows need schedule context."""
    games = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "game_date": "2026-09-09",
                "away_team": "NE",
                "away_score": 10.0,
                "home_team": "SEA",
                "home_score": 13.0,
            }
        ]
    )
    fetched = pd.DataFrame(
        [
            {
                "NFL_player_id": "00-0039851",
                "player": "Drake Maye",
                "year": 2026,
                "week": 1,
                "season_type": "REG",
                "game_id": "2026_01_NE_SEA",
                "nfl_team": "NE",
                "opponent_nfl_team": "SEA",
                "position": "QB",
            },
            {
                "NFL_player_id": "DEF-27",
                "player": "Seattle Seahawks DST",
                "year": 2026,
                "week": 1,
                "season_type": "REG",
                "game_id": None,
                "nfl_team": "SEA",
                "opponent_nfl_team": "NE",
                "position": "DEF",
            },
            {
                "NFL_player_id": None,
                "player": None,
                "year": 2026,
                "week": 1,
                "season_type": "REG",
                "game_id": "2026_01_NE_SEA",
                "nfl_team": "SEA",
                "opponent_nfl_team": "NE",
                "position": None,
            },
        ]
    )

    facts = prepare_weekly_facts(fetched, games, year=2026, week=1)

    assert facts[["NFL_player_id", "game_id", "game_date", "player_week"]].to_dict("records") == [
        {
            "NFL_player_id": "00-0039851",
            "game_id": "2026_01_NE_SEA",
            "game_date": "2026-09-09",
            "player_week": "00-0039851_2026_1",
        },
        {
            "NFL_player_id": "DEF-27",
            "game_id": "2026_01_NE_SEA",
            "game_date": "2026-09-09",
            "player_week": "DEF-27_2026_1",
        },
    ]


def test_prepare_weekly_facts_excludes_other_final_games_before_deriving_blank_dst_ids():
    """A date-scoped refresh must ignore prior final games from the same NFL week."""
    games = pd.DataFrame(
        [
            {
                "game_id": "2026_01_SF_LA",
                "game_date": "2026-09-10",
                "away_team": "SF",
                "away_score": 24.0,
                "home_team": "LA",
                "home_score": 17.0,
            }
        ]
    )
    fetched = pd.DataFrame(
        [
            {
                "NFL_player_id": "p-sf",
                "player": "San Francisco QB",
                "year": 2026,
                "week": 1,
                "game_id": "2026_01_SF_LA",
                "nfl_team": "SF",
                "opponent_nfl_team": "LA",
                "position": "QB",
            },
            {
                "NFL_player_id": "DEF-LA",
                "player": "Los Angeles DST",
                "year": 2026,
                "week": 1,
                "game_id": None,
                "nfl_team": "LA",
                "opponent_nfl_team": "SF",
                "position": "DEF",
            },
            {
                "NFL_player_id": "p-ne",
                "player": "New England QB",
                "year": 2026,
                "week": 1,
                "game_id": "2026_01_NE_SEA",
                "nfl_team": "NE",
                "opponent_nfl_team": "SEA",
                "position": "QB",
            },
            {
                "NFL_player_id": "DEF-SEA",
                "player": "Seattle DST",
                "year": 2026,
                "week": 1,
                "game_id": None,
                "nfl_team": "SEA",
                "opponent_nfl_team": "NE",
                "position": "DEF",
            },
        ]
    )

    facts = prepare_weekly_facts(fetched, games, year=2026, week=1)

    assert facts[["NFL_player_id", "game_id", "game_date"]].to_dict("records") == [
        {"NFL_player_id": "DEF-LA", "game_id": "2026_01_SF_LA", "game_date": "2026-09-10"},
        {"NFL_player_id": "p-sf", "game_id": "2026_01_SF_LA", "game_date": "2026-09-10"},
    ]


def test_prepare_weekly_facts_rejects_duplicate_canonical_keys():
    """Duplicating a player/game/team identity must abort before the local artifact is mutated."""
    games = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "game_date": "2026-09-09",
                "away_team": "NE",
                "away_score": 10.0,
                "home_team": "SEA",
                "home_score": 13.0,
            }
        ]
    )
    fetched = pd.DataFrame(
        [
            {
                "NFL_player_id": "00-0039851",
                "player": "Drake Maye",
                "year": 2026,
                "week": 1,
                "season_type": "REG",
                "game_id": "2026_01_NE_SEA",
                "nfl_team": "NE",
                "opponent_nfl_team": "SEA",
            },
            {
                "NFL_player_id": "00-0039851",
                "player": "Drake Maye",
                "year": 2026,
                "week": 1,
                "season_type": "REG",
                "game_id": "2026_01_NE_SEA",
                "nfl_team": "NE",
                "opponent_nfl_team": "SEA",
            },
        ]
    )

    with pytest.raises(RefreshGateError, match="duplicate canonical full key"):
        prepare_weekly_facts(fetched, games, year=2026, week=1)


def test_align_facts_to_wide_schema_preserves_known_values_and_uses_typed_nulls():
    """New providers may lag the wide schema; that must never drop an established column."""
    facts = pd.DataFrame(
        [
            {
                "NFL_player_id": "00-0039851",
                "player": "Drake Maye",
                "passing_yards": 251.0,
                "game_date": "2026-09-09",
            }
        ]
    )
    schema = [
        ("NFL_player_id", "VARCHAR"),
        ("player", "VARCHAR"),
        ("passing_yards", "DOUBLE"),
        ("ngs_avg_separation", "DOUBLE"),
        ("primary_position", "VARCHAR"),
    ]

    aligned = align_facts_to_wide_schema(facts, schema)

    assert list(aligned.columns) == [column for column, _ in schema]
    assert aligned.loc[0, "passing_yards"] == 251.0
    assert pd.isna(aligned.loc[0, "ngs_avg_separation"])
    assert pd.isna(aligned.loc[0, "primary_position"])


def test_replace_completed_game_rows_replaces_only_the_finalized_game_scope():
    """Re-running an opener replaces that game only and cannot erase another Week 1 game."""
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE weekly (
          NFL_player_id VARCHAR, game_date VARCHAR, week INTEGER, season_type VARCHAR,
          nfl_team VARCHAR, opponent_nfl_team VARCHAR, game_id VARCHAR, year INTEGER, points DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO weekly VALUES
          ('p-ne', '2026-09-09', 1, 'REG', 'NWE', 'SEA', '2026_01_NE_SEA', 2026, 10),
          ('p-dal', '2026-09-13', 1, 'REG', 'DAL', 'NYG', '2026_01_DAL_NYG', 2026, 11)
        """
    )
    facts = pd.DataFrame(
        [
            {
                "NFL_player_id": "p-ne",
                "game_date": "2026-09-09",
                "week": 1,
                "season_type": "REG",
                "nfl_team": "NWE",
                "opponent_nfl_team": "SEA",
                "game_id": "2026_01_NE_SEA",
                "year": 2026,
                "points": 13.0,
            },
            {
                "NFL_player_id": "p-sea",
                "game_date": "2026-09-09",
                "week": 1,
                "season_type": "REG",
                "nfl_team": "SEA",
                "opponent_nfl_team": "NWE",
                "game_id": "2026_01_NE_SEA",
                "year": 2026,
                "points": 14.0,
            },
        ]
    )
    games = pd.DataFrame(
        [
            {
                "game_id": "2026_01_NE_SEA",
                "game_date": "2026-09-09",
                "away_team": "NE",
                "home_team": "SEA",
            }
        ]
    )

    replaced = replace_completed_game_rows(con, "weekly", facts, games, year=2026, week=1)

    assert replaced == 2
    assert con.execute("SELECT NFL_player_id, points FROM weekly ORDER BY NFL_player_id").fetchall() == [
        ("p-dal", 11.0),
        ("p-ne", 13.0),
        ("p-sea", 14.0),
    ]


def test_complete_ops_artifact_rejects_an_incomplete_package(tmp_path):
    """The swap must be impossible until every public ops table is materialized."""
    artifact = tmp_path / "ops.duckdb"
    con = duckdb.connect(str(artifact))
    con.execute("CREATE SCHEMA nfl_historical")
    con.execute("CREATE TABLE nfl_historical.nfl_player_stats_all (NFL_player_id VARCHAR)")
    con.close()

    with pytest.raises(RefreshGateError, match="missing required ops tables"):
        assert_complete_ops_artifact(artifact)

    assert set(REQUIRED_OPS_TABLES) == {
        "nfl_player_stats_all",
        "player_nfl_season",
        "player_nfl_season_all",
        "player_nfl_career",
        "player_nfl_career_all",
        "player_bio",
        "player_nfl_season_team",
        "player_nfl_season_team_all",
    }


def test_build_player_bio_rows_maps_live_rookie_metadata_to_the_existing_bio_contract():
    facts = pd.DataFrame(
        [{"NFL_player_id": "00-0041569", "player": "Gabe Jacas", "position": "LB", "nfl_team": "NWE"}]
    )
    players = pd.DataFrame(
        [
            {
                "gsis_id": "00-0041569",
                "display_name": "Gabe Jacas",
                "position": "LB",
                "latest_team": "NE",
                "headshot": "https://example.test/gabe.jpg",
                "height": 76,
                "weight": 261,
                "college_name": "Illinois",
                "college_conference": "Big Ten Conference",
                "draft_year": 2026,
                "draft_round": 2,
                "draft_pick": 55,
                "draft_team": "NE",
                "rookie_season": 2026,
                "last_season": 2026,
                "status": "ACT",
                "pfr_id": "JacaGa00",
                "espn_id": 4837244,
                "birth_date": "2004-05-27",
            }
        ]
    )
    bio_schema = [
        ("NFL_player_id", "VARCHAR"),
        ("player", "VARCHAR"),
        ("nfl_position", "VARCHAR"),
        ("latest_team", "VARCHAR"),
        ("headshot_url", "VARCHAR"),
        ("height", "DOUBLE"),
        ("weight", "DOUBLE"),
        ("college", "VARCHAR"),
        ("conference", "VARCHAR"),
        ("draft_year", "DOUBLE"),
        ("draft_round", "DOUBLE"),
        ("draft_overall", "DOUBLE"),
        ("nfl_draft_team", "VARCHAR"),
        ("rookie_year", "DOUBLE"),
        ("last_year", "DOUBLE"),
        ("status", "VARCHAR"),
        ("pfr_id", "VARCHAR"),
        ("espn_id", "VARCHAR"),
        ("birth_date", "VARCHAR"),
    ]

    bio = build_player_bio_rows(facts, players, bio_schema)

    assert bio.to_dict("records") == [
        {
            "NFL_player_id": "00-0041569",
            "player": "Gabe Jacas",
            "nfl_position": "LB",
            "latest_team": "NWE",
            "headshot_url": "https://example.test/gabe.jpg",
            "height": 76,
            "weight": 261,
            "college": "Illinois",
            "conference": "Big Ten Conference",
            "draft_year": 2026,
            "draft_round": 2,
            "draft_overall": 55,
            "nfl_draft_team": "NWE",
            "rookie_year": 2026,
            "last_year": 2026,
            "status": "ACT",
            "pfr_id": "JacaGa00",
            "espn_id": "4837244",
            "birth_date": "2004-05-27",
        }
    ]


def test_build_player_bio_rows_includes_unplayed_rostered_players_with_platform_ids():
    """Week-one rookies must resolve before they record a finalized NFL stat."""
    facts = pd.DataFrame(
        [{"NFL_player_id": "00-0041569", "player": "Gabe Jacas", "position": "LB", "nfl_team": "NWE"}]
    )
    roster = pd.DataFrame(
        [
            {
                "gsis_id": "00-0041569",
                "full_name": "Gabe Jacas",
                "position": "LB",
                "team": "NE",
                "sleeper_id": "13301",
                "espn_id": "4837244",
            },
            {
                "gsis_id": "00-0040888",
                "full_name": "Adam Randall",
                "position": "WR",
                "team": "TB",
                "sleeper_id": "13302",
                "espn_id": "4685526",
            },
        ]
    )
    bio_schema = [
        ("NFL_player_id", "VARCHAR"),
        ("player", "VARCHAR"),
        ("nfl_position", "VARCHAR"),
        ("latest_team", "VARCHAR"),
        ("sleeper_player_id", "DOUBLE"),
        ("espn_id", "VARCHAR"),
    ]

    bio = build_player_bio_rows(facts, roster, bio_schema).set_index("NFL_player_id")

    assert set(bio.index) == {"00-0041569", "00-0040888"}
    assert bio.loc["00-0040888", "player"] == "Adam Randall"
    assert bio.loc["00-0040888", "nfl_position"] == "WR"
    assert bio.loc["00-0040888", "latest_team"] == "TAM"
    assert bio.loc["00-0040888", "sleeper_player_id"] == 13302
    assert bio.loc["00-0040888", "espn_id"] == "4685526"


def test_ops_schema_compatibility_rejects_a_candidate_that_drops_an_existing_field(tmp_path):
    base = tmp_path / "base.duckdb"
    output = tmp_path / "output.duckdb"
    for path, columns in ((base, "NFL_player_id VARCHAR, retained DOUBLE"), (output, "NFL_player_id VARCHAR")):
        con = duckdb.connect(str(path))
        con.execute("CREATE SCHEMA nfl_historical")
        for table in REQUIRED_OPS_TABLES:
            con.execute(f"CREATE TABLE nfl_historical.{table} ({columns})")
            con.execute(f"INSERT INTO nfl_historical.{table} VALUES ('p1'{', 1.0' if 'retained' in columns else ''})")
        con.close()

    with pytest.raises(RefreshGateError, match="would drop existing columns"):
        assert_ops_schema_compatible(base, output)


def test_upsert_player_bio_rows_refreshes_known_metadata_without_erasing_unknown_values():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE player_bio (
            NFL_player_id VARCHAR,
            player VARCHAR,
            nfl_position VARCHAR,
            headshot_url VARCHAR,
            college VARCHAR
        )
        """
    )
    con.execute(
        """
        INSERT INTO player_bio VALUES
          ('00-0031330', 'Morgan Moses', 'OT', 'old-headshot', 'Virginia'),
          ('existing-null', 'Existing Null', 'WR', NULL, 'Michigan')
        """
    )
    source = pd.DataFrame(
        [
            {
                "NFL_player_id": "00-0031330",
                "player": "Morgan Moses",
                "nfl_position": "OT",
                "headshot_url": "fresh-headshot",
                "college": pd.NA,
            },
            {
                "NFL_player_id": "00-0041569",
                "player": "Gabe Jacas",
                "nfl_position": "LB",
                "headshot_url": "gabe-headshot",
                "college": "Illinois",
            },
        ]
    )

    inserted, updated = upsert_player_bio_rows(con, source, table="player_bio")

    assert (inserted, updated) == (1, 1)
    assert con.execute(
        "SELECT player, nfl_position, headshot_url, college FROM player_bio WHERE NFL_player_id = '00-0031330'"
    ).fetchone() == ("Morgan Moses", "OT", "fresh-headshot", "Virginia")
    assert con.execute(
        "SELECT player, nfl_position, headshot_url, college FROM player_bio WHERE NFL_player_id = '00-0041569'"
    ).fetchone() == ("Gabe Jacas", "LB", "gabe-headshot", "Illinois")
    con.close()
