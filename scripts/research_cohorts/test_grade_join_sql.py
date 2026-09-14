import duckdb

from grade_join_sql import matchup_clutch_join_sql


def test_clutch_join_does_not_cross_format_grains():
    con = duckdb.connect()
    con.execute("""CREATE TABLE source AS SELECT * FROM (VALUES
      ('12t','flx','half','4pt','redraft','managed','non_keeper',4,3,2024,'p1'),
      ('12t','flx','half','4pt','dynasty','best_ball','keeper',4,3,2024,'p1')
    ) t(teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,
        cohort_level,format_level,year,NFL_player_id)""")
    con.execute("""CREATE TABLE matchup AS SELECT * FROM (VALUES
      ('12t','flx','half','4pt','redraft','managed','non_keeper',4,3,2024,'p1',1.5),
      ('12t','flx','half','4pt','dynasty','best_ball','keeper',4,3,2024,'p1',9.5)
    ) t(teams,roster,ppr,td,league_type,lineup_mode,keeper_mode,
        cohort_level,format_level,year,NFL_player_id,avg_clutch_started)""")

    rows = con.execute(
        matchup_clutch_join_sql("source", "matchup")
        + " ORDER BY league_type"
    ).fetchall()

    assert len(rows) == 2
    assert [(row[4], row[-1]) for row in rows] == [("dynasty", 9.5), ("redraft", 1.5)]
