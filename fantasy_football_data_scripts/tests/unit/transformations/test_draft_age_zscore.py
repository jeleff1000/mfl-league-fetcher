import duckdb

from multi_league.transformations.sql_enrichments import SQLEnrichments


def test_draft_age_zscore_accepts_varchar_birth_dates():
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute(
            """
            CREATE TABLE public.draft (
                db_name VARCHAR,
                year INTEGER,
                NFL_player_id VARCHAR,
                draft_age INTEGER,
                draft_age_zscore DOUBLE,
                expected_age DOUBLE,
                manager_weighted_age DOUBLE
            )
            """
        )
        conn.execute(
            "INSERT INTO public.draft VALUES "
            "('foo', 2025, 'valid', NULL, NULL, NULL, NULL), "
            "('foo', 2025, 'malformed', NULL, NULL, NULL, NULL)"
        )
        conn.execute("ATTACH ':memory:' AS ___ops")
        conn.execute("CREATE SCHEMA ___ops.nfl_historical")
        conn.execute(
            "CREATE TABLE ___ops.nfl_historical.player_bio "
            "(NFL_player_id VARCHAR, birth_date VARCHAR)"
        )
        conn.execute(
            "INSERT INTO ___ops.nfl_historical.player_bio VALUES "
            "('valid', '1990-09-12'), ('malformed', 'not-a-date')"
        )

        eng = SQLEnrichments(db_name="foo", data_dir="unused", conn=conn)
        eng.draft_age_zscore()

        assert conn.execute(
            "SELECT NFL_player_id, draft_age FROM public.draft ORDER BY NFL_player_id"
        ).fetchall() == [("malformed", None), ("valid", 35)]
    finally:
        conn.close()
