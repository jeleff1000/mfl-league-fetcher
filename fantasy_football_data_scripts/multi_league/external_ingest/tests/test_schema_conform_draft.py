import duckdb

from multi_league.external_ingest.schema_conform import run


def test_draft_without_platform_player_id_preserves_auction_cost():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE SCHEMA staging")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.matchup VALUES
            ('beer_league', 'Eric', 'ERIC_GUID', '1'),
            ('beer_league', 'Josh', 'JOSH_GUID', '2')
        """
    )
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            year INTEGER,
            round INTEGER,
            pick INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            player VARCHAR,
            cost DOUBLE,
            draft_type VARCHAR,
            yahoo_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.draft VALUES
            ('beer_league', 2025, 1, 1, 'Eric', 'ERIC_GUID', '1', 'Roschon Johnson', 1, 'auction', '123'),
            ('beer_league', 2025, 1, 2, 'Josh', 'JOSH_GUID', '2', 'J.J. McCarthy', 18, 'auction', '456')
        """
    )
    conn.execute(
        """
        CREATE TABLE staging.staging_draft (
            db_name VARCHAR,
            filename VARCHAR,
            year VARCHAR,
            round VARCHAR,
            pick VARCHAR,
            manager VARCHAR,
            player VARCHAR,
            yahoo_player_id VARCHAR,
            cost VARCHAR,
            draft_type VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO staging.staging_draft VALUES
            ('beer_league', 'auction.csv', '2025', '1', '1', 'Eric', 'Roschon Johnson, CHI', NULL, '1', 'Auction'),
            ('beer_league', 'auction.csv', '2025', '1', '2', 'Josh', 'J.J. McCarthy, MIN', NULL, '18', 'Auction')
        """
    )

    run(conn, "beer_league", "test-run")

    out = conn.execute(
        """
        SELECT year, round, pick, manager, manager_guid, player, cost, draft_type
        FROM staging.conformed_draft
        WHERE db_name = 'beer_league'
        ORDER BY pick
        """
    ).df()

    assert out["manager_guid"].tolist() == ["ERIC_GUID", "JOSH_GUID"]
    assert out["cost"].tolist() == [1.0, 18.0]
    assert out["draft_type"].tolist() == ["Auction", "Auction"]
    assert "yahoo_player_id" not in out.columns
