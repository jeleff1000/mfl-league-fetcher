import duckdb

from scripts.build_research_team_aggregates import (
    aggregate_expression,
    build_table_into_relation,
    _derived_expression,
    output_table_names,
    team_scope_group_columns,
)


def test_team_scope_group_columns_preserve_season_type_and_team_identity():
    assert team_scope_group_columns() == [
        "NFL_player_id",
        "year",
        "nfl_team",
        "nfl_franchise_number",
        "season_type",
    ]


def test_output_tables_split_regular_and_all_games():
    assert output_table_names() == (
        "player_nfl_season_team",
        "player_nfl_season_team_all",
    )


def test_aggregation_rules_match_research_scoped_semantics():
    assert aggregate_expression("fumbles_lost") == 'SUM(COALESCE("fumbles_lost", 0))'
    assert aggregate_expression("target_share") == 'AVG(CASE WHEN isfinite(TRY_CAST("target_share" AS DOUBLE)) THEN TRY_CAST("target_share" AS DOUBLE) ELSE NULL END)'
    assert aggregate_expression("fg_long") == 'MAX("fg_long")'
    assert aggregate_expression("fg_pct") == "CASE WHEN SUM(fg_att) > 0 THEN CAST(SUM(fg_made) AS DOUBLE) / SUM(fg_att) ELSE NULL END"
    assert "is_starter" in _derived_expression("games_started", {"is_starter"})
    assert "AVG" in _derived_expression("lamar_ppg_12t_flx_half_4pt", {"lamar_12t_flx_half_4pt"})


def test_local_team_rollup_builds_a_table_from_the_candidate_weekly_relation():
    con = duckdb.connect()
    con.execute(
        """
        CREATE TABLE weekly (
          NFL_player_id VARCHAR, year INTEGER, week INTEGER, nfl_team VARCHAR,
          nfl_franchise_number INTEGER, season_type VARCHAR, player VARCHAR,
          position VARCHAR, nfl_position VARCHAR, headshot_url VARCHAR,
          player_week VARCHAR, carries DOUBLE, rushing_yards DOUBLE
        )
        """
    )
    con.execute(
        """
        INSERT INTO weekly VALUES
          ('p1', 2026, 1, 'SEA', 27, 'REG', 'Runner', 'RB', 'RB', NULL, 'p1_2026_1', 10, 50),
          ('p1', 2026, 2, 'SEA', 27, 'REG', 'Runner', 'RB', 'RB', NULL, 'p1_2026_2', 8, 30),
          ('p1', 2026, 19, 'SEA', 27, 'POST', 'Runner', 'RB', 'RB', NULL, 'p1_2026_19', 4, 15)
        """
    )
    season_columns = [("NFL_player_id", "VARCHAR"), ("year", "INTEGER"), ("carries", "DOUBLE"), ("rushing_yards", "DOUBLE")]

    count = build_table_into_relation(
        con,
        source_ref="weekly",
        target_ref="season_team",
        include_postseason=False,
        weekly_columns={row[0] for row in con.execute("DESCRIBE weekly").fetchall()},
        season_columns=season_columns,
    )

    assert count == 1
    assert con.execute("SELECT NFL_player_id, carries, rushing_yards FROM season_team").fetchall() == [
        ("p1", 18.0, 80.0)
    ]
