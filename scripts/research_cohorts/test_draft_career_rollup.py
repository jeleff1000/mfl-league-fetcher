from __future__ import annotations

import duckdb

from draft_career_sql import draft_career_rollup_sql


def test_career_rollup_exposes_weighted_averages_extrema_years_and_spend() -> None:
    con = duckdb.connect()
    try:
        con.execute("""CREATE TABLE draft_seasons (
            teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR, bracket VARCHAR,
            league_type VARCHAR, lineup_mode VARCHAR, keeper_mode VARCHAR,
            cohort_level INTEGER, format_level INTEGER,
            NFL_player_id VARCHAR, year INTEGER, n_leagues INTEGER, n_drafted INTEGER,
            n_auction_leagues INTEGER, n_auction_eligible_leagues INTEGER,
            adp DOUBLE, draft_rate_pct DOUBLE, avg_auction_cost_pct DOUBLE,
            auction_draft_rate_pct DOUBLE, cost_pct DOUBLE,
            avg_fantasy_points DOUBLE, avg_manager_lamar DOUBLE,
            draft_score DOUBLE, draft_score_healthy DOUBLE)""")
        rows = [
            ("12t", "flx", "ppr", "4pt", "6po", "redraft", "managed", "non_keeper", 4, 3, "p1", 2021, 10, 8, 2, 4,
             5.0, 80.0, 40.0, 50.0, 20.0, 100.0, 30.0, 10.0, 8.0),
            ("12t", "flx", "ppr", "4pt", "6po", "redraft", "managed", "non_keeper", 4, 3, "p1", 2022, 30, 15, 3, 6,
             20.0, 50.0, 30.0, 50.0, 15.0, 120.0, 40.0, 30.0, 25.0),
            ("12t", "flx", "ppr", "4pt", "6po", "redraft", "managed", "non_keeper", 4, 3, "p1", 2023, 20, 4, 1, 5,
             40.0, 20.0, 25.0, 20.0, 5.0, 80.0, 10.0, -15.0, -12.0),
            ("12t", "flx", "ppr", "4pt", "6po", "dynasty", "best_ball", "keeper", 4, 3, "p1", 2023, 10, 3, 0, 1,
             50.0, 30.0, None, 0.0, None, 70.0, 5.0, -20.0, -18.0),
        ]
        con.executemany("INSERT INTO draft_seasons VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)

        result = con.execute(
            draft_career_rollup_sql("draft_seasons")
            + " HAVING league_type='redraft'"
        ).fetchone()
        columns = [item[0] for item in con.description]
        row = dict(zip(columns, result))

        assert (row["earliest_adp"], row["earliest_adp_year"]) == (5.0, 2021)
        assert (row["latest_adp"], row["latest_adp_year"]) == (40.0, 2023)
        assert row["adp"] == 24.2
        assert (row["highest_draft_rate"], row["highest_draft_rate_year"]) == (80.0, 2021)
        assert (row["lowest_draft_rate"], row["lowest_draft_rate_year"]) == (20.0, 2023)
        assert (row["best_draft_score"], row["best_draft_score_year"]) == (30.0, 2022)
        assert (row["worst_draft_score"], row["worst_draft_score_year"]) == (-15.0, 2023)
        assert (row["best_healthy_score"], row["best_healthy_score_year"]) == (25.0, 2022)
        assert (row["worst_healthy_score"], row["worst_healthy_score_year"]) == (-12.0, 2023)
        assert row["career_auction_spend_pct"] == 40.0
        assert row["auction_draft_rate_pct"] == 40.0
        assert con.execute(
            "SELECT COUNT(*) FROM (" + draft_career_rollup_sql("draft_seasons") + ")"
        ).fetchone()[0] == 2
    finally:
        con.close()
