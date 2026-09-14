import duckdb

from build_wide_bundle import adaptive_matchup_sql


def test_adaptive_lattice_prefers_thick_exact_value_and_pools_thin_bracket():
    con = duckdb.connect()
    con.execute("ATTACH ':memory:' AS rc")
    con.execute("""CREATE TABLE rc.matchup (
        teams VARCHAR, roster VARCHAR, ppr VARCHAR, td VARCHAR, bracket VARCHAR,
        league_type VARCHAR, lineup_mode VARCHAR, keeper_mode VARCHAR,
        year INTEGER, NFL_player_id VARCHAR, pos_grp VARCHAR, n_leagues INTEGER,
        confidence VARCHAR, roster_rate_pct DOUBLE
    )""")
    # Thick 4-team cell and thin 6-team cell for the same player/configuration.
    con.execute("""INSERT INTO rc.matchup VALUES
        ('12t','flx','half','4pt','4po','ALL','ALL','ALL',2024,'p1','RB',1000,'confident',80),
        ('12t','flx','half','4pt','6po','ALL','ALL','ALL',2024,'p1','RB',4,'confident',60),
        ('12t','flx','half','4pt','ALL','ALL','ALL','ALL',2024,'p1','RB',1200,'confident',70)
    """)
    con.execute(adaptive_matchup_sql(
        "adaptive", "matchup", ["NFL_player_id", "year"]
    ))
    four = con.execute("""SELECT roster_rate_pct, bracket
        FROM adaptive
        WHERE q_teams='12t' AND q_roster='flx' AND q_ppr='half' AND q_td='4pt'
          AND q_bracket='4po' AND q_league_type='ALL' AND q_lineup_mode='ALL'
          AND NFL_player_id='p1' AND year=2024""").fetchone()
    six = con.execute("""SELECT roster_rate_pct, bracket
        FROM adaptive
        WHERE q_teams='12t' AND q_roster='flx' AND q_ppr='half' AND q_td='4pt'
          AND q_bracket='6po' AND q_league_type='ALL' AND q_lineup_mode='ALL'
          AND NFL_player_id='p1' AND year=2024""").fetchone()
    assert four == (80.0, "4po")
    assert six == (70.0, "ALL")
