import duckdb

from repair_championship_signals import build_sidecar


def _base(con, multiweek=False):
    con.execute("attach ':memory:' as base")
    con.execute("create schema base.public")
    con.execute("""create table base.public.league_settings (
        db_name varchar, year integer, playoff_start_week integer,
        has_multiweek_championship integer)""")
    con.execute("insert into base.public.league_settings values ('std',2003,15,0),('two',2003,15,1)")
    con.execute("""create table base.public.matchup (
        db_name varchar, year integer, week integer, manager varchar,
        team_key varchar, opponent varchar, opponent_team_key varchar,
        matchup_key varchar, franchise_id varchar, is_playoffs integer,
        is_consolation integer, champion integer, is_championship integer)""")
    rows = [
        ('std',2003,15,'A','a','C','c','std-15-ac','fa',1,0,0,0),
        ('std',2003,15,'C','c','A','a','std-15-ac','fc',1,0,0,0),
        ('std',2003,16,'A','a','B','b','std-16-ab','fa',1,0,1,0),
        ('std',2003,16,'B','b','A','a','std-16-ab','fb',1,0,0,0),
        ('two',2003,15,'A','a','B','b','two-15-ab','fa',1,0,0,0),
        ('two',2003,15,'B','b','A','a','two-15-ab','fb',1,0,0,0),
        ('two',2003,16,'A','a','B','b','two-16-ab','fa',1,0,1,0),
        ('two',2003,16,'B','b','A','a','two-16-ab','fb',1,0,0,0),
    ]
    con.executemany("insert into base.public.matchup values (?,?,?,?,?,?,?,?,?,?,?,?,?)", rows)
    con.execute("""create table base.public.player_fantasy (
        db_name varchar, year integer, week integer, manager varchar,
        team_key varchar, is_rostered integer, champion integer)""")


def test_repairs_single_and_multiweek_title_games():
    con = duckdb.connect()
    _base(con)
    count = build_sidecar(con)
    got = con.execute("select db_name,week,manager from public.championship_signal_sidecar order by db_name,week,manager").fetchall()
    assert count == 6
    assert got == [
        ('std',16,'A'),('std',16,'B'),
        ('two',15,'A'),('two',15,'B'),('two',16,'A'),('two',16,'B'),
    ]


def test_falls_back_to_manager_when_player_team_key_differs():
    con = duckdb.connect()
    _base(con)
    con.execute("update base.public.matchup set champion=0 where db_name='std'")
    con.execute("insert into base.public.player_fantasy values ('std',2003,16,'A','different-key',1,1)")
    count = build_sidecar(con)
    got = con.execute("select week,manager from public.championship_signal_sidecar where db_name='std' order by manager").fetchall()
    assert count == 6
    assert got == [(16, 'A'), (16, 'B')]


def test_row_level_playoff_signal_overrides_stale_settings_cutoff():
    con = duckdb.connect()
    _base(con)
    # The source matchup graph says week 14 is a playoff/final week even though
    # the settings row incorrectly says playoffs begin in week 15.
    con.execute("update base.public.matchup set is_playoffs=1 where db_name='std' and week=15")
    con.execute("update base.public.matchup set is_playoffs=0 where db_name='std' and week=16")
    con.execute("update base.public.matchup set champion=0 where db_name='std'")
    con.execute("insert into base.public.player_fantasy values ('std',2003,15,'A','a',1,1)")
    count = build_sidecar(con)
    got = con.execute(
        "select week,manager from public.championship_signal_sidecar "
        "where db_name='std' order by manager"
    ).fetchall()
    assert con.execute(
        "select count(*) from public.championship_signal_sidecar where db_name='std'"
    ).fetchone()[0] == 2
    assert got == [(15, 'A'), (15, 'C')]


def test_season_champion_marker_anchors_to_last_playoff_week():
    con = duckdb.connect()
    _base(con)
    con.execute("update base.public.matchup set champion=0 where db_name='std'")
    # The season champion marker was retained on an earlier playoff row.  The
    # title signal must be placed on the team's final playoff matchup instead.
    con.execute("update base.public.matchup set champion=1 where db_name='std' and week=15 and manager='A'")
    build_sidecar(con)
    got = con.execute(
        "select week,manager from public.championship_signal_sidecar "
        "where db_name='std' order by manager"
    ).fetchall()
    assert got == [(16, 'A'), (16, 'B')]


def test_null_matchup_identity_does_not_mark_every_playoff_team():
    con = duckdb.connect()
    con.execute("attach ':memory:' as base")
    con.execute("create schema base.public")
    con.execute("create table base.public.league_settings (db_name varchar, year integer, playoff_start_week integer, has_multiweek_championship integer)")
    con.execute("insert into base.public.league_settings values ('nullid',2003,15,0)")
    con.execute("""create table base.public.matchup (
        db_name varchar, year integer, week integer, manager varchar,
        team_key varchar, opponent varchar, opponent_team_key varchar,
        matchup_key varchar, franchise_id varchar, is_playoffs integer,
        is_consolation integer, champion integer, is_championship integer)""")
    con.executemany(
        "insert into base.public.matchup values (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        [
            ('nullid',2003,14,'Great Ketch',None,None,None,None,None,1,0,0,0),
            ('nullid',2003,14,'Other A',None,None,None,None,None,1,0,0,0),
            ('nullid',2003,14,'Other B',None,None,None,None,None,1,0,0,0),
        ],
    )
    con.execute("""create table base.public.player_fantasy (
        db_name varchar, year integer, week integer, manager varchar,
        team_key varchar, is_rostered integer, champion integer)""")
    con.execute("insert into base.public.player_fantasy values ('nullid',2003,14,'Great Ketch',NULL,1,1)")
    build_sidecar(con)
    got = con.execute(
        "select manager from public.championship_signal_sidecar "
        "where db_name='nullid' order by manager"
    ).fetchall()
    assert got == [('Great Ketch',)]
