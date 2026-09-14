import duckdb

from multi_league.core.db_utils import resolve_nfl_ids


def _make_conn():
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS test_db")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA test_db.public")
    conn.execute("CREATE SCHEMA ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            player VARCHAR,
            yahoo_player_id DOUBLE,
            sleeper_player_id DOUBLE,
            espn_id VARCHAR,
            nfl_position VARCHAR
        )
        """
    )
    return conn


def test_resolve_nfl_ids_skips_reference_scans_when_the_target_is_already_resolved():
    """A weekly refresh must not scan the player universe for an unchanged row."""
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS test_db")
    conn.execute("CREATE SCHEMA test_db.public")
    conn.execute(
        """
        CREATE TABLE test_db.public.player_fantasy (
            yahoo_player_id VARCHAR,
            NFL_player_id VARCHAR,
            player VARCHAR,
            position VARCHAR
        )
        """
    )
    conn.execute("INSERT INTO test_db.public.player_fantasy VALUES ('123', '00-001', 'Resolved Player', 'WR')")
    try:
        assert resolve_nfl_ids(conn, "test_db", "player_fantasy", platform="yahoo") == 0
    finally:
        conn.close()


def test_resolve_nfl_ids_disambiguates_ambiguous_yahoo_id_by_name_and_position():
    conn = _make_conn()
    conn.execute(
        """
        CREATE TABLE test_db.public.player_fantasy (
            yahoo_player_id VARCHAR,
            player VARCHAR,
            position VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO test_db.public.player_fantasy VALUES
            ('8801', 'Chris Johnson', 'RB', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('00-0021949', 'Chris Johnson', 8801, NULL, NULL, 'DB'),
            ('00-0026164', 'Chris Johnson', 8801, NULL, NULL, 'RB')
        """
    )

    try:
        resolve_nfl_ids(conn, "test_db", "player_fantasy", platform="yahoo")
        row = conn.execute("SELECT NFL_player_id FROM test_db.public.player_fantasy").fetchone()
    finally:
        conn.close()

    assert row == ("00-0026164",)


def test_resolve_nfl_ids_leaves_same_name_same_position_collisions_unresolved():
    conn = _make_conn()
    conn.execute(
        """
        CREATE TABLE test_db.public.player_fantasy (
            yahoo_player_id VARCHAR,
            player VARCHAR,
            position VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO test_db.public.player_fantasy VALUES
            ('33871', 'Elijah Ponder', 'LB', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('00-0040574', 'Elijah Ponder', 33871, NULL, NULL, 'LB'),
            ('00-0036761', 'Elijah Ponder', 33871, NULL, NULL, 'LB')
        """
    )

    try:
        resolve_nfl_ids(conn, "test_db", "player_fantasy", platform="yahoo")
        row = conn.execute("SELECT NFL_player_id FROM test_db.public.player_fantasy").fetchone()
    finally:
        conn.close()

    assert row == (None,)


def test_resolve_nfl_ids_uses_name_only_when_duplicate_yahoo_id_has_distinct_names():
    conn = _make_conn()
    conn.execute(
        """
        CREATE TABLE test_db.public.transactions (
            yahoo_player_id VARCHAR,
            player VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO test_db.public.transactions VALUES
            ('9512', 'Cameron Morrah', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('00-0026963', 'Cameron Morrah', 9512, NULL, NULL, 'TE'),
            ('00-0039046', 'DeWayne McBride', 9512, NULL, NULL, 'RB')
        """
    )

    try:
        resolve_nfl_ids(conn, "test_db", "transactions", platform="yahoo")
        row = conn.execute("SELECT NFL_player_id FROM test_db.public.transactions").fetchone()
    finally:
        conn.close()

    assert row == ("00-0026963",)


def test_resolve_nfl_ids_rejects_unique_yahoo_id_when_player_name_conflicts():
    """Yahoo can reuse a historical ID that player_bio currently assigns elsewhere."""
    conn = _make_conn()
    conn.execute(
        """
        CREATE TABLE test_db.public.draft (
            yahoo_player_id VARCHAR,
            player VARCHAR,
            position VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO test_db.public.draft VALUES
            ('118', 'Morten Andersen', 'K', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('00-0019646', 'Sebastian Janikowski', 118, NULL, NULL, 'K'),
            ('00-0000282', 'Morten Andersen', NULL, NULL, NULL, 'K')
        """
    )

    try:
        resolve_nfl_ids(conn, "test_db", "draft", platform="yahoo")
        row = conn.execute("SELECT NFL_player_id FROM test_db.public.draft").fetchone()
    finally:
        conn.close()

    assert row == ("00-0000282",)


def test_resolve_nfl_ids_falls_back_to_unique_player_bio_name_position_when_platform_id_missing():
    conn = _make_conn()
    conn.execute(
        """
        CREATE TABLE test_db.public.player_fantasy (
            yahoo_player_id VARCHAR,
            player VARCHAR,
            position VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO test_db.public.player_fantasy VALUES
            ('9039', 'BenJarvus Green-Ellis', 'RB', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('00-0025860', 'BenJarvus Green-Ellis', NULL, NULL, NULL, 'RB')
        """
    )

    try:
        resolve_nfl_ids(conn, "test_db", "player_fantasy", platform="yahoo")
        row = conn.execute("SELECT NFL_player_id FROM test_db.public.player_fantasy").fetchone()
    finally:
        conn.close()

    assert row == ("00-0025860",)


def test_resolve_nfl_ids_uses_year_scoped_super_table_for_historical_yahoo_ids():
    conn = _make_conn()
    conn.execute(
        """
        CREATE TABLE test_db.public.player_fantasy (
            year INTEGER,
            yahoo_player_id VARCHAR,
            player VARCHAR,
            position VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            year INTEGER,
            week INTEGER,
            NFL_player_id VARCHAR,
            player VARCHAR,
            nfl_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO test_db.public.player_fantasy VALUES
            (2003, '126', 'Jerry Rice', 'WR', NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('00-0013639', 'Jerry Rice', 28314, NULL, NULL, 'WR'),
            ('00-0031429', 'Jerry Rice', NULL, NULL, NULL, 'WR')
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all VALUES
            (2003, 1, '00-0013639', 'Jerry Rice', 'WR'),
            (2014, 1, '00-0031429', 'Jerry Rice', 'WR')
        """
    )

    try:
        resolve_nfl_ids(conn, "test_db", "player_fantasy", platform="yahoo")
        row = conn.execute("SELECT NFL_player_id FROM test_db.public.player_fantasy").fetchone()
    finally:
        conn.close()

    assert row == ("00-0013639",)
