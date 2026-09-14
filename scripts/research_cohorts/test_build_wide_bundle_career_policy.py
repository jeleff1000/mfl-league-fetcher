import duckdb

from build_wide_bundle import TABLES, career_matchup_sql


def test_career_policy_keeps_pooled_2003_2010_years():
    stats = next(spec[5] for spec in TABLES if spec[0] == "research_matchup_career")
    required = {
        "NFL_player_id", "year", "teams", "roster", "ppr", "td", "bracket",
        "league_type", "lineup_mode", "keeper_mode", "cohort_level", "format_level",
        "confidence", "n_leagues", "active_weeks", "started_weeks", "elig_league_weeks",
        "champ_elig_leagues", "po_n_rostered_leagues", "n_started_po", "n_final_po",
        "n_champ_leagues", "n_champ_start_leagues", "inactive_weeks", "po_wkwt_credit",
        "wins_started", "losses_started", "rostered_league_weeks", "roster_eligible_league_weeks",
        "started_team_game_weeks", "team_game_eligible_league_weeks", "started_active_weeks",
        "healthy_eligible_league_weeks", "started_champ_active", "playoff_eligible_leagues",
        "expected_wins", "expected_losses", "expected_starts", "total_points_observed",
        "ppg_when_started", "total_lamar_started", "sum_clutch_started_active",
    }
    columns = required | {column for column, _ in stats.values()}
    text_columns = {
        "NFL_player_id", "teams", "roster", "ppr", "td", "bracket", "league_type",
        "lineup_mode", "keeper_mode", "confidence",
    }
    con = duckdb.connect()
    con.execute("CREATE SCHEMA rc")
    ddl = []
    for column in sorted(columns):
        kind = "VARCHAR" if column in text_columns else "DOUBLE"
        if column in {"year", "cohort_level", "format_level"}:
            kind = "INTEGER"
        ddl.append(f'"{column}" {kind}')
    con.execute(f"CREATE TABLE rc.matchup ({', '.join(ddl)})")

    template = {column: 0 for column in columns}
    template.update({
        "NFL_player_id": "DEF-5", "league_type": "ALL", "lineup_mode": "ALL",
        "keeper_mode": "ALL", "bracket": "ALL", "confidence": "confident",
        "format_level": 0, "n_leagues": 100, "active_weeks": 16, "started_weeks": 100,
        "elig_league_weeks": 1600, "champ_elig_leagues": 80, "po_n_rostered_leagues": 80,
        "n_started_po": 20, "n_final_po": 30, "n_champ_leagues": 10,
        "n_champ_start_leagues": 5, "rostered_league_weeks": 800,
        "roster_eligible_league_weeks": 1600, "started_team_game_weeks": 800,
        "team_game_eligible_league_weeks": 1600, "started_active_weeks": 800,
        "healthy_eligible_league_weeks": 1600, "started_champ_active": 5,
        "playoff_eligible_leagues": 80, "expected_wins": 40, "expected_losses": 40,
        "expected_starts": 50, "ppg_when_started": 10, "sum_clutch_started_active": 4,
    })
    names = sorted(columns)
    rows = []
    for year in (2003, 2004):
        row = template.copy()
        row.update(year=year, teams="ALL", roster="ALL", ppr="ALL", td="ALL", cohort_level=0)
        rows.append(row)
    row = template.copy()
    row.update(year=2011, teams="12t", roster="flx", ppr="ppr", td="4pt",
               bracket="6po", cohort_level=4, format_level=3)
    rows.append(row)
    con.executemany(
        f"INSERT INTO rc.matchup ({', '.join(names)}) VALUES ({', '.join('?' for _ in names)})",
        [[row[column] for column in names] for row in rows],
    )

    sql = career_matchup_sql("out", "matchup", ["NFL_player_id"], stats,
                             source_columns=columns)
    assert sql is not None
    con.execute(sql)
    result = con.execute(
        """SELECT n_years_12t_flx_ppr_4pt, n_leagues_12t_flx_ppr_4pt,
                         roster_rate_12t_flx_ppr_4pt, level_12t_flx_ppr_4pt
                    FROM out WHERE NFL_player_id='DEF-5'"""
    ).fetchone()
    assert result == (3, 300.0, 50.0, 4)
