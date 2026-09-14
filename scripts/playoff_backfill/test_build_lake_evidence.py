import duckdb

from scripts.playoff_backfill.build_lake_evidence import build_evidence


def connection_with_fixture():
    con = duckdb.connect()
    con.execute(
        """
        create table league_settings (
            db_name varchar, year integer, platform varchar,
            playoff_start_week integer, playoff_teams integer,
            regular_season_weeks integer
        )
        """
    )
    con.execute(
        "insert into league_settings values ('mfl_old', 2018, 'mfl', 14, 2, 13)"
    )
    con.execute(
        """
        create table matchup (
            db_name varchar, year integer, week integer, manager varchar,
            team_points double, is_playoffs integer, final_playoff_seed integer,
            champion integer
        )
        """
    )
    con.executemany(
        "insert into matchup values (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("mfl_old", 2018, 13, "A", 140.0, 0, 1, 0),
            ("mfl_old", 2018, 13, "B", 130.0, 0, 2, 0),
            ("mfl_old", 2018, 13, "C", 120.0, 0, 3, 0),
            ("mfl_old", 2018, 14, "A", 110.0, 1, 1, 0),
            ("mfl_old", 2018, 14, "B", 100.0, 1, 2, 0),
            ("mfl_old", 2018, 14, "C", 150.0, 0, 3, 0),
            ("mfl_old", 2018, 15, "A", 115.0, 1, 1, 1),
            ("mfl_old", 2018, 15, "B", 95.0, 1, 2, 0),
            ("mfl_old", 2018, 15, "C", 125.0, 0, 3, 0),
        ],
    )
    con.execute(
        """
        create table player_fantasy (
            db_name varchar, year integer, week integer, NFL_player_id varchar,
            manager varchar, is_started integer, fantasy_points double,
            champion integer default 0
        )
        """
    )
    con.executemany(
        """
        insert into player_fantasy
        (db_name, year, week, NFL_player_id, manager, is_started, fantasy_points)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("mfl_old", 2018, 13, "p-a", "A", 1, 20),
            ("mfl_old", 2018, 14, "p-a", "A", 1, 20),
            ("mfl_old", 2018, 14, "p-c", "C", 1, 30),
            ("mfl_old", 2018, 15, "p-a", "A", 1, 20),
            ("mfl_old", 2018, 15, "p-b", "B", 0, 10),
            ("mfl_old", 2018, 15, "p-c", "C", 1, 30),
        ],
    )
    return con


def test_builder_projects_qualification_and_excludes_consolation_games():
    con = connection_with_fixture()
    result = build_evidence(con)
    a14 = result.query("NFL_player_id == 'p-a' and week == 14").iloc[0]
    c14 = result.query("NFL_player_id == 'p-c' and week == 14").iloc[0]
    c15 = result.query("NFL_player_id == 'p-c' and week == 15").iloc[0]
    assert a14.made_po_bf == 1 and a14.is_playoffs_bf == 1
    assert c14.made_po_bf == 0 and c14.is_playoffs_bf == 0
    assert c15.made_po_bf == 0 and c15.is_playoffs_bf == 0


def test_builder_uses_started_player_points_when_matchup_team_points_missing():
    con = connection_with_fixture()
    con.execute("update matchup set team_points = null where week = 13")
    result = build_evidence(con)
    assert set(result.query("week == 13").made_po_bf) == {1}


def test_native_bracket_evidence_implies_qualification_without_seed():
    con = connection_with_fixture()
    con.execute("update league_settings set playoff_teams = 0")
    con.execute("update matchup set final_playoff_seed = null where is_playoffs = 1")
    result = build_evidence(con)
    a14 = result.query("NFL_player_id == 'p-a' and week == 14").iloc[0]
    assert a14.made_po_bf == 1 and a14.is_playoffs_bf == 1


def test_native_player_champion_implies_season_qualification_only():
    con = connection_with_fixture()
    con.execute("update matchup set final_playoff_seed = null, is_playoffs = 0")
    con.execute("update league_settings set playoff_teams = 0")
    con.execute("update player_fantasy set champion = 1 where NFL_player_id = 'p-a'")
    result = build_evidence(con)
    a13 = result.query("NFL_player_id == 'p-a' and week == 13").iloc[0]
    a14 = result.query("NFL_player_id == 'p-a' and week == 14").iloc[0]
    assert a13.made_po_bf == 1 and a13.is_playoffs_bf == 0
    assert a14.made_po_bf == 1 and a14.is_playoffs_bf == 0
