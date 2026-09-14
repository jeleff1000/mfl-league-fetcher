import duckdb
import pyarrow as pa

from scripts.research_cohorts import build_research_txn_cohort as txn


def test_typed_table_preserves_schema_for_empty_year_range():
    table = txn.typed_table([], txn.PLAYER_SCHEMA)

    assert table.num_rows == 0
    assert table.schema == txn.PLAYER_SCHEMA
    assert table.column_names[:4] == ["teams", "roster", "ppr", "td"]


def test_pruned_psv_cte_keeps_only_transaction_player_slugs():
    con = duckdb.connect()
    con.execute("""CREATE TABLE btx (
        slug VARCHAR, NFL_player_id VARCHAR, week INTEGER
    )""")
    con.execute("""INSERT INTO btx VALUES
        ('12t_flx_half_4pt', 'cmc', 3),
        ('12t_flx_half_4pt', 'cmc', 7),
        ('10t_flx_ppr_6pt', 'lamar', 2)
    """)
    con.execute("""CREATE TABLE player_slug_value (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER,
        slug VARCHAR, lamar DOUBLE
    )""")
    con.execute("""INSERT INTO player_slug_value VALUES
        ('cmc', 2024, 4, '12t_flx_half_4pt', 10.0),
        ('cmc', 2024, 4, '10t_flx_ppr_6pt', 11.0),
        ('lamar', 2024, 3, '10t_flx_ppr_6pt', 12.0),
        ('unused', 2024, 3, '12t_flx_half_4pt', 13.0),
        ('cmc', 2023, 4, '12t_flx_half_4pt', 14.0)
    """)

    con.execute(
        "CREATE TEMP TABLE psv AS "
        + txn.pruned_psv_sql(2024, source="player_slug_value")
    )

    assert con.execute(
        "SELECT NFL_player_id, week, slug, lamar FROM psv ORDER BY 1, 2"
    ).fetchall() == [
        ('cmc', 4, '12t_flx_half_4pt', 10.0),
        ('lamar', 3, '10t_flx_ppr_6pt', 12.0),
    ]


def test_yearly_transaction_subqueries_scope_to_eligible_leagues_before_grouping():
    sql = " ".join(txn.player_sql(2023).split())

    assert "FROM public.draft d JOIN ls ON ls.db_name = d.db_name AND ls.year = d.year" in sql
    assert "FROM public.player_fantasy p JOIN ls ON ls.db_name = p.db_name AND ls.year = p.year" in sql
    assert sql.count(
        "FROM public.transactions t JOIN ls ON ls.db_name = t.db_name AND ls.year = t.year"
    ) >= 3


def test_per_league_transaction_sql_preserves_db_name_without_rollup_grouping_sets():
    sql = " ".join(txn.player_sql(2025, per_league=True).split())

    assert "SELECT db_name," in sql
    assert sql.count("MAX(position) AS position") == 1
    assert "GROUP BY teams, roster, ppr, td, league_type, lineup_mode, keeper_mode, db_name, NFL_player_id" in sql
    assert "GROUPING SETS" not in sql
