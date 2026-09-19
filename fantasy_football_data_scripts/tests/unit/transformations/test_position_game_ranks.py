"""League-scored individual games, never NFL career totals or a truncated year."""

import duckdb
import pytest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from multi_league.transformations.aggregation.modules import optimal_lineup


@pytest.fixture
def games():
    with duckdb.connect(":memory:") as con:
        con.execute("""
            CREATE TABLE games (
                db_name VARCHAR, player_week VARCHAR, year INTEGER, week INTEGER,
                position VARCHAR, fantasy_points DOUBLE, manager VARCHAR,
                position_season_rank INTEGER, position_alltime_rank INTEGER,
                position_week_rank INTEGER, season_ppg DOUBLE
            );
            INSERT INTO games VALUES
                ('league','old_a',2025,1,'QB',30,'A',1,1,88,12),
                ('league','old_b',2025,2,'QB',20,'A',2,2,88,12),
                ('league','old_b',2025,2,'QB',20,'Duplicate roster',2,2,88,12),
                ('league','tie_a',2026,1,'QB',20,'A',216,216,88,12),
                ('league','tie_b',2026,1,'QB',20,'B',216,216,88,12),
                ('league','unrostered',2024,1,'QB',40,'Unrostered',1,1,88,12),
                ('league','rb',2026,1,'RB',50,'B',1,1,88,12),
                ('other','old_a',2025,1,'QB',999,'Other',99,99,88,12)
        """)
        yield con


def _refresh(con):
    return optimal_lineup.refresh_position_game_ranks(con, "games", db_name="league")


def test_ranks_games_with_ties_unrostered_and_roster_duplicates(games):
    _refresh(games)
    assert games.execute("""
        SELECT player_week, position_season_rank, position_alltime_rank
        FROM games WHERE db_name='league' ORDER BY player_week,manager
    """).fetchall() == [
        ("old_a", 1, 2), ("old_b", 2, 3), ("old_b", 2, 3), ("rb", 1, 1),
        ("tie_a", 1, 4), ("tie_b", 2, 5), ("unrostered", 1, 1),
    ]
    assert games.execute("SELECT position_alltime_rank FROM games WHERE db_name='other'").fetchone() == (99,)


def test_new_high_game_updates_old_ranks_without_touching_other_cells(games):
    _refresh(games)
    games.execute("INSERT INTO games VALUES ('league','new_high',2026,2,'QB',60,'A',NULL,NULL,7,13)")
    untouched = games.execute("""
        SELECT db_name,player_week,year,week,position,fantasy_points,manager,position_week_rank,season_ppg
        FROM games ORDER BY db_name,player_week,manager
    """).fetchall()
    counts = _refresh(games)
    assert counts == {"position_season_rank": 3, "position_alltime_rank": 7}
    assert games.execute("""
        SELECT player_week,position_alltime_rank FROM games
        WHERE db_name='league' AND player_week IN ('new_high','old_a','unrostered') ORDER BY player_week
    """).fetchall() == [("new_high", 1), ("old_a", 3), ("unrostered", 2)]
    assert games.execute("""
        SELECT db_name,player_week,year,week,position,fantasy_points,manager,position_week_rank,season_ppg
        FROM games ORDER BY db_name,player_week,manager
    """).fetchall() == untouched
    assert _refresh(games) == {"position_season_rank": 0, "position_alltime_rank": 0}


def test_rank_changes_roll_back_with_callers_transaction(games):
    before = games.execute("SELECT position_season_rank,position_alltime_rank FROM games ORDER BY player_week,manager").fetchall()
    games.execute("BEGIN")
    _refresh(games)
    games.execute("ROLLBACK")
    assert games.execute("SELECT position_season_rank,position_alltime_rank FROM games ORDER BY player_week,manager").fetchall() == before


def test_unrankable_games_clear_only_stale_game_ranks(games):
    games.execute("""
        INSERT INTO games VALUES
            ('league','no_points',2026,1,'QB',NULL,'A',216,216,88,12),
            ('league','no_position',2026,1,'',100,'A',216,216,88,12)
    """)
    _refresh(games)
    assert games.execute("""
        SELECT position_season_rank,position_alltime_rank,position_week_rank,season_ppg
        FROM games WHERE player_week IN ('no_points','no_position')
    """).fetchall() == [(None, None, 88, 12), (None, None, 88, 12)]


def test_refresh_writes_both_game_rank_columns_in_one_scoped_update(games):
    class RecordingConnection:
        def __init__(self, raw):
            self.raw = raw
            self.sql = []

        def execute(self, sql, params=None):
            self.sql.append(" ".join(str(sql).split()))
            return self.raw.execute(sql, params) if params is not None else self.raw.execute(sql)

    recorded = RecordingConnection(games)
    _refresh(recorded)

    updates = [sql for sql in recorded.sql if sql.upper().startswith("UPDATE GAMES ")]
    assert len(updates) == 1
    assert "POSITION_SEASON_RANK" in updates[0].upper()
    assert "POSITION_ALLTIME_RANK" in updates[0].upper()
