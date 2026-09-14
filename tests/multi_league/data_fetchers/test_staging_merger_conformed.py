import duckdb
from multi_league.data_fetchers.shared.staging_data_merger import _read_external_rows


def test_merger_prefers_conformed_when_present():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute("""
        CREATE TABLE staging.conformed_matchup
        (db_name VARCHAR, year INTEGER, week INTEGER,
         manager VARCHAR, manager_guid VARCHAR, team_points DOUBLE)
    """)
    conn.execute("""
        INSERT INTO staging.conformed_matchup VALUES
        ('kmffl', 2014, 1, 'Adin', 'GUID_ADIN', 100.5)
    """)
    # Also have raw — merger must NOT fall back to it
    conn.execute("""
        CREATE TABLE staging.staging_matchup
        (db_name VARCHAR, year VARCHAR, manager VARCHAR)
    """)
    conn.execute("INSERT INTO staging.staging_matchup VALUES ('kmffl', '2014', 'WrongName')")

    df = _read_external_rows(conn, table="matchup", db_name="kmffl")
    assert "manager_guid" in df.columns
    assert df["manager"].iloc[0] == "Adin"  # came from conformed, not raw


def test_merger_falls_back_to_staging_when_no_conformed():
    """Legacy path: leagues that pre-date schema_conform still work via staging_*."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA staging")
    conn.execute("""
        CREATE TABLE staging.staging_matchup
        (db_name VARCHAR, year VARCHAR, manager VARCHAR)
    """)
    conn.execute("INSERT INTO staging.staging_matchup VALUES ('legacy_league', '2020', 'OldName')")

    df = _read_external_rows(conn, table="matchup", db_name="legacy_league")
    assert df["manager"].iloc[0] == "OldName"
