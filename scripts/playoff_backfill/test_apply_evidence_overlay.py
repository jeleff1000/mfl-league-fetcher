import duckdb
import pandas as pd

from scripts.playoff_backfill.apply_evidence_overlay import apply_overlay


def test_overlay_fills_nulls_and_preserves_native_values():
    con = duckdb.connect()
    con.execute(
        """
        create table player_fantasy (
            db_name varchar, year integer, week integer, NFL_player_id varchar,
            final_playoff_seed integer, made_po_bf tinyint, is_playoffs_bf tinyint
        )
        """
    )
    con.executemany(
        "insert into player_fantasy values (?, ?, ?, ?, ?, ?, ?)",
        [
            ("lg", 2018, 14, "p1", None, None, None),
            ("lg", 2018, 15, "p1", 2, None, None),
            ("lg", 2018, 15, "p2", None, 0, 0),
        ],
    )
    evidence = pd.DataFrame(
        [
            {"db_name": "lg", "year": 2018, "week": 14, "NFL_player_id": "p1", "made_po_bf": 1, "is_playoffs_bf": 1},
            {"db_name": "lg", "year": 2018, "week": 14, "NFL_player_id": "p1", "made_po_bf": 1, "is_playoffs_bf": 1},
            {"db_name": "lg", "year": 2018, "week": 15, "NFL_player_id": "p1", "made_po_bf": 1, "is_playoffs_bf": 1},
            {"db_name": "lg", "year": 2018, "week": 15, "NFL_player_id": "p2", "made_po_bf": 1, "is_playoffs_bf": 1},
        ]
    )
    stats = apply_overlay(con, evidence)
    rows = con.execute(
        "select week, NFL_player_id, final_playoff_seed, made_po_bf, is_playoffs_bf "
        "from player_fantasy order by week, NFL_player_id"
    ).fetchall()
    assert rows == [
        (14, "p1", None, 1, 1),
        (15, "p1", 2, 1, 1),
        (15, "p2", None, 0, 0),
    ]
    assert stats == {"evidence_rows": 3, "matched_rows": 3, "filled_rows": 2, "champion_filled_rows": 0}


def test_overlay_is_idempotent():
    con = duckdb.connect()
    con.execute(
        "create table player_fantasy (db_name varchar, year integer, week integer, NFL_player_id varchar, "
        "final_playoff_seed integer, made_po_bf tinyint, is_playoffs_bf tinyint)"
    )
    con.execute("insert into player_fantasy values ('lg', 2018, 14, 'p1', null, null, null)")
    evidence = pd.DataFrame([{"db_name": "lg", "year": 2018, "week": 14, "NFL_player_id": "p1", "made_po_bf": 1, "is_playoffs_bf": 1}])
    assert apply_overlay(con, evidence)["filled_rows"] == 1
    assert apply_overlay(con, evidence)["filled_rows"] == 0


def test_unresolved_rows_are_not_stamped():
    con = duckdb.connect()
    con.execute(
        "create table player_fantasy (db_name varchar, year integer, week integer, NFL_player_id varchar, "
        "final_playoff_seed integer, made_po_bf tinyint, is_playoffs_bf tinyint)"
    )
    con.execute("insert into player_fantasy values ('lg', 1999, 14, 'p1', null, null, null)")
    evidence = pd.DataFrame([{
        "db_name": "lg", "year": 1999, "week": 14, "NFL_player_id": "p1",
        "made_po_bf": 0, "is_playoffs_bf": 0, "evidence_kind": "unresolved",
    }])
    assert apply_overlay(con, evidence)["filled_rows"] == 0
    assert con.execute("select made_po_bf, is_playoffs_bf from player_fantasy").fetchone() == (None, None)


def test_champion_backfill_fills_only_positive_title_evidence():
    con = duckdb.connect()
    con.execute(
        "create table player_fantasy (db_name varchar, year integer, week integer, NFL_player_id varchar, "
        "final_playoff_seed integer, made_po_bf tinyint, is_playoffs_bf tinyint, champion integer)"
    )
    con.executemany("insert into player_fantasy values (?, ?, ?, ?, ?, ?, ?, ?)", [
        ("lg", 2018, 15, "p1", None, None, None, None),
        ("lg", 2018, 15, "p2", None, None, None, 0),
    ])
    evidence = pd.DataFrame([{
        "db_name": "lg", "year": 2018, "week": 15, "NFL_player_id": "p1",
        "made_po_bf": 1, "is_playoffs_bf": 1, "champion_bf": 1,
    }, {
        "db_name": "lg", "year": 2018, "week": 15, "NFL_player_id": "p2",
        "made_po_bf": 1, "is_playoffs_bf": 1, "champion_bf": 0,
    }])
    stats = apply_overlay(con, evidence)
    assert con.execute("select champion from player_fantasy where NFL_player_id='p1'").fetchone() == (1,)
    assert con.execute("select champion from player_fantasy where NFL_player_id='p2'").fetchone() == (0,)
    assert stats["champion_filled_rows"] == 1
