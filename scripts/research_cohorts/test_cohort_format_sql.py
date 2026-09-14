import duckdb

from scripts.research_cohorts.cohort_format_sql import (
    MATCHUP_ADAPTIVE_DIMENSIONS,
    MATCHUP_ADAPTIVE_MIN_LEAGUES,
    cohort_league_settings_sql,
    grouping_sets_with_formats,
    choose_matchup_dimension_value,
    matchup_grouping_sets_with_formats,
)


def test_matchup_adaptive_contract_has_seven_dimensions_and_all_format_states():
    assert MATCHUP_ADAPTIVE_DIMENSIONS == (
        "teams", "roster", "ppr", "td", "bracket", "league_type", "lineup_mode"
    )
    assert MATCHUP_ADAPTIVE_MIN_LEAGUES == 150
    sql = matchup_grouping_sets_with_formats(
        [("teams", "roster", "ppr", "td")], tail=("year", "NFL_player_id")
    )
    # Eight public bracket/dynasty/best-ball pool/split states plus one legacy
    # keeper-granular compatibility state.
    assert sql.count("year, NFL_player_id") == 9
    assert "(teams, roster, ppr, td, bracket, league_type, lineup_mode, year, NFL_player_id)" in sql
    assert "(teams, roster, ppr, td, year, NFL_player_id)" in sql


def test_adaptive_pooling_is_independent_per_dimension():
    assert choose_matchup_dimension_value("4po", 999_999_997) == "4po"
    assert choose_matchup_dimension_value("6po", 4) == "ALL"
    assert choose_matchup_dimension_value("dynasty", 149) == "ALL"
    assert choose_matchup_dimension_value("dynasty", 150) == "dynasty"


def test_format_contract_keeps_and_flags_every_league_type():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, num_teams INTEGER,
        roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
        roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
        scoring_rec DOUBLE, scoring_pass_td DOUBLE, playoff_teams INTEGER,
        is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN
    )""")
    con.execute("""INSERT INTO public.league_settings VALUES
        ('redraft', 2024, 12, 0,0,0,0,0,0,0, 0.5,4, 6, false,false),
        ('dynasty', 2024, 12, 0,0,0,0,0,0,1, 1.0,6, 4, true,false),
        ('bestball',2024, 10, 0,0,0,0,0,0,0, 0.0,4, 8, false,true),
        ('keeper',  2024, 12, 0,0,0,0,0,0,0, 0.5,4, 6, false,false)
    """)
    con.execute("""CREATE TABLE public.draft (
        db_name VARCHAR, year INTEGER, is_keeper BOOLEAN
    )""")
    con.execute("INSERT INTO public.draft VALUES ('keeper', 2024, true)")

    cur = con.execute(cohort_league_settings_sql(year=2024) + " ORDER BY db_name")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    formats = {
        row[0]: tuple(row[cols.index(name)] for name in ("league_type", "lineup_mode", "keeper_mode"))
        for row in rows
    }
    assert formats == {
        'bestball': ('redraft', 'best_ball', 'non_keeper'),
        'dynasty': ('dynasty', 'managed', 'non_keeper'),
        'keeper': ('redraft', 'managed', 'keeper'),
        'redraft': ('redraft', 'managed', 'non_keeper'),
    }


def test_grouping_sets_emit_exact_and_all_format_cells_per_rung():
    sql = grouping_sets_with_formats(
        [("teams", "roster"), ("roster",)], tail=("year", "NFL_player_id")
    )

    assert "(teams, roster, league_type, lineup_mode, keeper_mode, year, NFL_player_id)" in sql
    assert "(teams, roster, year, NFL_player_id)" in sql
    assert "(roster, league_type, lineup_mode, keeper_mode, year, NFL_player_id)" in sql
    assert "(roster, year, NFL_player_id)" in sql


def test_matchup_grouping_sets_carry_bracket_before_format_dimensions():
    sql = matchup_grouping_sets_with_formats([("teams", "roster")], tail=("year", "NFL_player_id"))

    assert "(teams, roster, bracket, league_type, lineup_mode, keeper_mode, year, NFL_player_id)" in sql
    assert "(teams, roster, bracket, year, NFL_player_id)" in sql


def test_2003_through_2010_forces_the_historical_all_format_pool():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, num_teams INTEGER,
        roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
        roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
        scoring_rec DOUBLE, scoring_pass_td DOUBLE, playoff_teams INTEGER,
        is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN
    )""")
    con.execute("""CREATE TABLE public.draft (db_name VARCHAR, year INTEGER, is_keeper BOOLEAN)""")
    # TIER CONTRACT v2: teams_* now come from OBSERVED capacity, so the generated SQL reads
    # player_fantasy and the ops position index. Both must exist for the query to bind. Empty
    # is correct here -- with no observed rows every teams_* is NULL, which is exactly the
    # "no capacity observed, invent nothing" branch this contract requires.
    con.execute("CREATE TABLE public.player_fantasy "
                "(db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR, "
                # is_started is required now that the tier emits a STARTED variant per
                # position -- 9 positions x 2 stats x 4 tiers = 72 rules.
                "is_started INTEGER)")
    # `ops` is attached by conftest.ensure_capacity_sources for every connection in this
    # directory -- attaching it again here raises "database already exists".
    con.execute("""INSERT INTO public.league_settings VALUES
        ('historic', 2005, 10, 0,0,0,0,0,0,0, 0.0,6, 4, false,false),
        ('modern', 2011, 10, 0,0,0,0,0,0,0, 0.0,6, 4, false,false)""")

    cur = con.execute(cohort_league_settings_sql() + " ORDER BY year")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    historic, modern = rows
    assert historic[3:8] == ("ALL", "ALL", "ALL", "ALL", "ALL")
    assert tuple(historic[cols.index(name)] for name in ("league_type", "lineup_mode", "keeper_mode")) == (
        "ALL", "ALL", "ALL"
    )
    assert modern[3:8] == ("10t", "flx", "std", "6pt", "4po")


def test_historic_position_slot_fields_are_also_forced_to_all():
    con = duckdb.connect()
    con.execute("CREATE SCHEMA public")
    con.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, num_teams INTEGER,
        roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER, roster_DB INTEGER,
        roster_DB_LB INTEGER, roster_DL_LB INTEGER, roster_SUPER_FLEX INTEGER,
        roster_QB INTEGER, roster_RB INTEGER, roster_WR INTEGER, roster_TE INTEGER,
        roster_FLX INTEGER, scoring_rec DOUBLE, scoring_pass_td DOUBLE,
        playoff_teams INTEGER, is_dynasty BOOLEAN, sleeper_best_ball BOOLEAN
    )""")
    con.execute("CREATE TABLE public.draft (db_name VARCHAR, year INTEGER, is_keeper BOOLEAN)")
    # TIER CONTRACT v2: teams_* now come from OBSERVED capacity, so the generated SQL reads
    # player_fantasy and the ops position index. Both must exist for the query to bind. Empty
    # is correct here -- with no observed rows every teams_* is NULL, which is exactly the
    # "no capacity observed, invent nothing" branch this contract requires.
    con.execute("CREATE TABLE public.player_fantasy "
                "(db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR, "
                # is_started is required now that the tier emits a STARTED variant per
                # position -- 9 positions x 2 stats x 4 tiers = 72 rules.
                "is_started INTEGER)")
    # `ops` is attached by conftest.ensure_capacity_sources for every connection in this
    # directory -- attaching it again here raises "database already exists".
    con.execute("""INSERT INTO public.league_settings VALUES
        ('historic', 2005, 12, 0,0,0,0,0,0,0, 1,2,3,1,2, 1.0,4,6,false,false),
        ('modern', 2024, 12, 0,0,0,0,0,0,0, 1,1,3,1,1, 1.0,4,6,false,false)""")

    cur = con.execute(cohort_league_settings_sql(position_slots=True) + " ORDER BY year")
    cols = [d[0] for d in cur.description]
    rows = cur.fetchall()
    historic, modern = rows
    # all nine positions, both stats
    for col in [f"teams_{p}{sfx}" for p in ("QB", "RB", "WR", "TE", "K", "DEF", "DL", "LB", "DB")
                for sfx in ("", "_started")]:
        assert historic[cols.index(col)] == "ALL"
        # TIER CONTRACT v2 (2026-08-02): teams_* are the OBSERVED-CAPACITY tier, a FOUR
        # bucket alphabet, not the old two-level '10t'/'12t' cut from declared slots. NULL is
        # legitimate -- a league with no player rows has no observed capacity, and inventing a
        # bucket for it would be the fabricated-zero this contract exists to prevent.
        assert modern[cols.index(col)] in {"08tm", "10tm", "12tm", "14tm", None}
