import duckdb

from multi_league.external_ingest.schema_conform._orchestrator import _build_context


def test_saved_franchise_merge_canonicalizes_reference_identity():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute(
        """CREATE TABLE public.matchup (
            db_name VARCHAR, manager VARCHAR, manager_guid VARCHAR, team_key VARCHAR
        )"""
    )
    conn.execute(
        "INSERT INTO public.matchup VALUES ('kmffl', 'Jesse', 'current-yahoo-guid', 'team-key')"
    )

    ctx = _build_context(
        conn,
        "kmffl",
        franchise_merges=[
            {
                "owner_ids": ["current-yahoo-guid", "yh-nick-jesse"],
                "into_franchise_id": "yh-nick-jesse",
            }
        ],
    )

    assert ctx.guid_merges == {"current-yahoo-guid": "yh-nick-jesse"}
    assert ctx.name_to_guid["jesse"] == "yh-nick-jesse"
    assert ctx.team_key_to_guid["team-key"] == "yh-nick-jesse"
