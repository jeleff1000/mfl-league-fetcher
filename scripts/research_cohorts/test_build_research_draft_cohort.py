import duckdb

from scripts.research_cohorts import build_research_draft_cohort as draft


def test_pruned_psv_keeps_only_drafted_player_slug_pairs():
    con = duckdb.connect()
    con.execute("CREATE TABLE bdr (slug VARCHAR, NFL_player_id VARCHAR)")
    con.execute("""INSERT INTO bdr VALUES
        ('12t_flx_half_4pt', 'cmc'),
        ('10t_flx_ppr_6pt', 'lamar')
    """)
    con.execute("""CREATE TABLE player_slug_value (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, slug VARCHAR,
        lamar DOUBLE, fpts DOUBLE
    )""")
    con.execute("""INSERT INTO player_slug_value VALUES
        ('cmc', 2024, 1, '12t_flx_half_4pt', 10, 20),
        ('cmc', 2024, 1, '10t_flx_ppr_6pt', 11, 21),
        ('lamar', 2024, 1, '10t_flx_ppr_6pt', 12, 22),
        ('unused', 2024, 1, '12t_flx_half_4pt', 13, 23),
        ('cmc', 2023, 1, '12t_flx_half_4pt', 14, 24)
    """)

    con.execute(
        "CREATE TEMP TABLE psv AS "
        + draft.pruned_psv_sql(2024, source="player_slug_value")
    )

    assert con.execute(
        "SELECT NFL_player_id, week, slug, lamar, fpts FROM psv ORDER BY 1"
    ).fetchall() == [
        ('cmc', 1, '12t_flx_half_4pt', 10.0, 20.0),
        ('lamar', 1, '10t_flx_ppr_6pt', 12.0, 22.0),
    ]


def test_yearly_draft_helpers_scope_to_eligible_leagues_before_grouping():
    sql = " ".join(draft.player_sql(2024).split())

    assert sql.count(
        "FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year"
    ) >= 4
    assert "FROM public.player_fantasy p JOIN ls ON ls.db_name = p.db_name AND ls.year = p.year" in sql


def test_draft_value_aggregates_use_order_stable_decimal_accumulators():
    sql = " ".join(draft.player_sql(2025).split())

    for expression in (
        "psv.lamar",
        "fpts",
        "pq",
        "manager_lamar",
        "manager_lamar_native_legacy",
        "cost_pct",
        "CASE WHEN total_fantasy_points <= 700 THEN total_fantasy_points END",
    ):
        assert draft.stable_sum_sql(expression) in sql


def test_per_league_draft_sql_preserves_db_name_without_rollup_grouping_sets():
    sql = " ".join(draft.player_sql(2025, per_league=True).split())

    assert "SELECT db_name," in sql
    assert sql.count("MAX(position) AS position") == 1
    assert "GROUP BY teams, roster, ppr, td, league_type, lineup_mode, keeper_mode, db_name, NFL_player_id" in sql
    assert "GROUPING SETS" not in sql
