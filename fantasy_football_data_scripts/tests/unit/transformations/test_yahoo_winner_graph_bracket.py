import duckdb
import pandas as pd

from multi_league.core.canonical_matchup import RAW_COLUMNS, normalize_matchup_df
from multi_league.transformations.matchup.modules.playoff_bracket.bracket_tracer import (
    trace_championship_bracket_sql,
)


def _insert_matchup(conn, year, week, a, b, a_points, b_points, winner):
    def row(team, opp, points, opp_points):
        win = 1 if points > opp_points else 0
        loss = 1 if points < opp_points else 0
        return (
            year,
            week,
            team,
            opp,
            f"t.{team}",
            f"t.{winner}",
            points,
            opp_points,
            win,
            loss,
            0,
            0,
            0,
            False,
            0,
            None,
            None,
        )

    conn.executemany(
        "INSERT INTO matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            row(a, b, a_points, b_points),
            row(b, a, b_points, a_points),
        ],
    )


def test_winner_team_key_survives_matchup_normalization_as_raw_column():
    raw = pd.DataFrame(
        [
            {
                "league_id": "461.l.12345",
                "year": 2024,
                "week": 15,
                "manager": "Winner",
                "opponent": "Runner Up",
                "team_key": "461.l.12345.t.1",
                "opponent_team_key": "461.l.12345.t.2",
                "winner_team_key": "461.l.12345.t.1",
                "team_points": "101.5",
                "opponent_points": "100.5",
            }
        ]
    )

    normalized = normalize_matchup_df(raw, platform="yahoo", league_id="461.l.12345")

    assert "winner_team_key" in RAW_COLUMNS
    assert "winner_team_key" in normalized.columns
    assert normalized.loc[0, "winner_team_key"] == "461.l.12345.t.1"


def test_yahoo_winner_team_key_graph_beats_seed_tiebreak_for_custom_bracket():
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            team_key VARCHAR,
            winner_team_key VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            champion INTEGER,
            is_championship BOOLEAN,
            final_playoff_seed INTEGER,
            playoff_round VARCHAR,
            placement_rank INTEGER
        )
        """
    )

    # Regular-season rows seed A=1, B=2, C=3, D=4, etc.
    for idx, team in enumerate("ABCDEFGH", start=1):
        conn.execute(
            """
            INSERT INTO matchup
            (year, week, franchise_id, opponent_franchise_id, team_key, winner_team_key,
             team_points, opponent_points, win, loss, is_playoffs, is_consolation,
             champion, is_championship, final_playoff_seed, playoff_round, placement_rank)
            VALUES (2024, 13, ?, 'regular_opp', ?, NULL, ?, 0, 1, 0, 0, 0, 0, FALSE, NULL, NULL, NULL)
            """,
            [team, f"t.{team}", 200 - idx],
        )

    # Quarterfinal winners: A, B, C, D.
    _insert_matchup(conn, 2024, 14, "A", "H", 120, 80, "A")
    _insert_matchup(conn, 2024, 14, "B", "G", 119, 81, "B")
    _insert_matchup(conn, 2024, 14, "C", "F", 118, 82, "C")
    _insert_matchup(conn, 2024, 14, "D", "E", 117, 83, "D")

    # Semifinals are tied on points. Seed tiebreak would advance A and B, but
    # Yahoo's winner_team_key advances D and C.
    _insert_matchup(conn, 2024, 15, "A", "D", 100, 100, "D")
    _insert_matchup(conn, 2024, 15, "B", "C", 100, 100, "C")

    # Final plus third-place game. The title game is C-D even though A-B has the
    # better seed sum.
    _insert_matchup(conn, 2024, 16, "D", "C", 91, 88, "D")
    _insert_matchup(conn, 2024, 16, "A", "B", 97, 96, "A")

    result = trace_championship_bracket_sql(
        conn,
        2024,
        {
            "playoff_teams": 8,
            "bye_teams": 0,
            "playoff_start_week": 14,
            "end_week": 16,
            "num_teams": 8,
        },
        table="matchup",
        write_back=True,
    )

    assert result["champion"] == "D"
    assert set(result["championship_teams"]) == {"C", "D"}
    semifinal_winners = {
        matchup["winner"]
        for round_entry in result["rounds"]
        if round_entry["round"] == 2
        for matchup in round_entry["matchups"]
    }
    assert semifinal_winners == {"C", "D"}

    final_rows = conn.execute(
        """
        SELECT franchise_id
        FROM matchup
        WHERE year = 2024
          AND week = 16
          AND COALESCE(is_playoffs, 0) = 1
          AND playoff_round = 'championship'
        ORDER BY franchise_id
        """
    ).fetchall()
    assert [row[0] for row in final_rows] == ["C", "D"]


def _make_matchup_table(conn):
    conn.execute(
        """
        CREATE TABLE matchup (
            year INTEGER, week INTEGER, franchise_id VARCHAR, opponent_franchise_id VARCHAR,
            team_key VARCHAR, winner_team_key VARCHAR, team_points DOUBLE, opponent_points DOUBLE,
            win INTEGER, loss INTEGER, is_playoffs INTEGER, is_consolation INTEGER, champion INTEGER,
            is_championship BOOLEAN, final_playoff_seed INTEGER, playoff_round VARCHAR, placement_rank INTEGER
        )
        """
    )


def test_everyone_plays_every_playoff_week_champion_from_winner_graph():
    """Reproduce the ancient-Yahoo 'placement bracket' shape (u_can_t_handle_this 2013).

    All 8 playoff teams keep playing every playoff week (3rd/5th/7th-place games),
    so week 16 has four games. The real title game is Ali vs Riaz; Kavish wins the
    week-16 third-place game by a blowout (96-51). The winner-graph must follow
    who stayed undefeated (Ali, Riaz) rather than be fooled by the blowout or seeds.
    Also stress-tests Yahoo's 2013 flags: is_consolation can be mis-set, so the
    tracer must not depend on it.
    """
    conn = duckdb.connect(":memory:")
    _make_matchup_table(conn)

    # Regular-season seeding (lower seed # = better). Deliberately make Kavish a
    # top seed so a seed/score heuristic would wrongly pick his blowout game.
    seed_order = ["Ali", "Kavish", "Riaz", "Shawn", "Zain", "Imran", "Ace", "Rahim"]
    for idx, team in enumerate(seed_order, start=1):
        conn.execute(
            """
            INSERT INTO matchup VALUES
            (2013, 13, ?, 'reg_opp', ?, NULL, ?, 0, 1, 0, 0, 0, 0, FALSE, NULL, NULL, NULL)
            """,
            [team, f"t.{team}", 200 - idx],
        )

    # Week 14 — quarterfinals (winners advance, losers drop to placement games).
    _insert_matchup(conn, 2013, 14, "Riaz", "Zain", 134, 102, "Riaz")
    _insert_matchup(conn, 2013, 14, "Kavish", "Ace", 74, 73, "Kavish")
    _insert_matchup(conn, 2013, 14, "Shawn", "Rahim", 109, 85, "Shawn")
    _insert_matchup(conn, 2013, 14, "Ali", "Imran", 111, 73, "Ali")

    # Week 15 — semifinals AND placement games (losers keep playing).
    _insert_matchup(conn, 2013, 15, "Riaz", "Shawn", 112, 101, "Riaz")
    _insert_matchup(conn, 2013, 15, "Ali", "Kavish", 97, 72, "Ali")
    _insert_matchup(conn, 2013, 15, "Imran", "Ace", 91, 75, "Imran")
    _insert_matchup(conn, 2013, 15, "Zain", "Rahim", 130, 103, "Zain")

    # Week 16 — title game plus 3rd/5th/7th-place games (FOUR games this week).
    _insert_matchup(conn, 2013, 16, "Ali", "Riaz", 113, 102, "Ali")  # championship
    _insert_matchup(conn, 2013, 16, "Kavish", "Shawn", 96, 51, "Kavish")  # 3rd-place blowout
    _insert_matchup(conn, 2013, 16, "Zain", "Imran", 74, 63, "Zain")  # 5th place
    _insert_matchup(conn, 2013, 16, "Ace", "Rahim", 85, 71, "Ace")  # 7th place

    result = trace_championship_bracket_sql(
        conn,
        2013,
        {"playoff_teams": 8, "bye_teams": 0, "playoff_start_week": 14, "end_week": 16, "num_teams": 12},
        table="matchup",
        write_back=True,
    )

    assert result["champion"] == "Ali"
    assert set(result["championship_teams"]) == {"Ali", "Riaz"}


def test_all_games_flagged_consolation_still_finds_champion():
    """2003 shape: Yahoo marks EVERY playoff game is_consolation=1. The tracer must
    ignore that flag and follow winner_team_key to the true champion (Danish)."""
    conn = duckdb.connect(":memory:")
    _make_matchup_table(conn)

    seed_order = ["Riaz", "Hidden2", "Danish", "Ace", "Geoff", "Rahim", "Husain", "Imran"]
    for idx, team in enumerate(seed_order, start=1):
        conn.execute(
            "INSERT INTO matchup VALUES (2003,13,?, 'reg_opp', ?, NULL, ?, 0,1,0,0,0,0,FALSE,NULL,NULL,NULL)",
            [team, f"t.{team}", 200 - idx],
        )

    def cons(year, week, a, b, ap, bp, winner):
        # is_consolation = 1 on EVERY playoff game (the ancient-Yahoo bug).
        for team, opp, p, op in ((a, b, ap, bp), (b, a, bp, ap)):
            conn.execute(
                "INSERT INTO matchup VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [
                    year,
                    week,
                    team,
                    opp,
                    f"t.{team}",
                    f"t.{winner}",
                    p,
                    op,
                    1 if p > op else 0,
                    1 if p < op else 0,
                    1,
                    1,
                    0,
                    False,
                    None,
                    None,
                    None,
                ],
            )

    cons(2003, 14, "Riaz", "Husain", 110, 92, "Riaz")
    cons(2003, 14, "Hidden2", "Geoff", 72, 43, "Hidden2")
    cons(2003, 14, "Danish", "Rahim", 89, 61, "Danish")
    cons(2003, 14, "Imran", "Ace", 56, 45, "Imran")
    cons(2003, 15, "Danish", "Riaz", 85, 50, "Danish")
    cons(2003, 15, "Imran", "Hidden2", 98, 48, "Imran")
    cons(2003, 15, "Geoff", "Ace", 100, 44, "Geoff")
    cons(2003, 15, "Husain", "Rahim", 77, 72, "Husain")
    cons(2003, 16, "Danish", "Imran", 69, 60, "Danish")  # championship
    cons(2003, 16, "Riaz", "Hidden2", 105, 100, "Riaz")
    cons(2003, 16, "Rahim", "Ace", 74, 46, "Rahim")
    cons(2003, 16, "Husain", "Geoff", 67, 52, "Husain")

    result = trace_championship_bracket_sql(
        conn,
        2003,
        {"playoff_teams": 8, "bye_teams": 0, "playoff_start_week": 14, "end_week": 16, "num_teams": 12},
        table="matchup",
        write_back=True,
    )

    assert result["champion"] == "Danish"
    assert set(result["championship_teams"]) == {"Danish", "Imran"}


def test_seven_team_bracket_with_one_bye():
    """Odd bracket: 7 playoff teams, top seed gets a first-round bye."""
    conn = duckdb.connect(":memory:")
    _make_matchup_table(conn)

    seed_order = ["S1", "S2", "S3", "S4", "S5", "S6", "S7"]
    for idx, team in enumerate(seed_order, start=1):
        conn.execute(
            "INSERT INTO matchup VALUES (2024,12,?, 'reg_opp', ?, NULL, ?, 0,1,0,0,0,0,FALSE,NULL,NULL,NULL)",
            [team, f"t.{team}", 200 - idx],
        )

    # Round 1 (wk14): S1 byes; 2v7, 3v6, 4v5 play.
    _insert_matchup(conn, 2024, 14, "S2", "S7", 110, 90, "S2")
    _insert_matchup(conn, 2024, 14, "S3", "S6", 88, 100, "S6")  # upset
    _insert_matchup(conn, 2024, 14, "S4", "S5", 105, 99, "S4")
    # Round 2 (wk15): S1 (bye) + S2, S6, S4. Pairings S1v S6 (lowest remaining) etc.
    _insert_matchup(conn, 2024, 15, "S1", "S6", 120, 80, "S1")
    _insert_matchup(conn, 2024, 15, "S2", "S4", 95, 96, "S4")  # upset
    # Round 3 (wk16): final S1 vs S4.
    _insert_matchup(conn, 2024, 16, "S1", "S4", 101, 88, "S1")

    result = trace_championship_bracket_sql(
        conn,
        2024,
        {"playoff_teams": 7, "bye_teams": 1, "playoff_start_week": 14, "end_week": 16, "num_teams": 12},
        table="matchup",
        write_back=True,
    )

    assert result["champion"] == "S1"
    assert set(result["championship_teams"]) == {"S1", "S4"}


def test_two_week_championship_decided_by_combined_score_not_last_week_key():
    """2-week final (wk16-17). A wins wk16 big; B wins wk17 small; A wins on combined.

    Yahoo's per-week winner_team_key would name B for week 17, but the round is
    decided by COMBINED score, so the champion must be A.
    """
    conn = duckdb.connect(":memory:")
    _make_matchup_table(conn)

    for idx, team in enumerate(["A", "B", "C", "D"], start=1):
        conn.execute(
            "INSERT INTO matchup VALUES (2024,12,?, 'reg_opp', ?, NULL, ?, 0,1,0,0,0,0,FALSE,NULL,NULL,NULL)",
            [team, f"t.{team}", 200 - idx],
        )

    # Semifinals (wk15, single week): A and B advance.
    _insert_matchup(conn, 2024, 15, "A", "D", 110, 90, "A")
    _insert_matchup(conn, 2024, 15, "B", "C", 105, 95, "B")

    # Two-week final: wk16 A by 40, wk17 B by 10. Combined: A=160 vs B=130 -> A.
    _insert_matchup(conn, 2024, 16, "A", "B", 100, 60, "A")
    _insert_matchup(conn, 2024, 17, "A", "B", 60, 70, "B")  # per-week key says B

    result = trace_championship_bracket_sql(
        conn,
        2024,
        {"playoff_teams": 4, "bye_teams": 0, "playoff_start_week": 15, "end_week": 17, "num_teams": 8},
        table="matchup",
        write_back=True,
    )

    assert set(result["championship_teams"]) == {"A", "B"}
    assert result["champion"] == "A"  # combined score, not last-week winner_team_key
