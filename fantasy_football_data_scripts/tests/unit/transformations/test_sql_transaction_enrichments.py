import duckdb
import pytest

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.transaction.sql_transaction_enrichments import TransactionEnrichmentsMixin


class _TxnRunner(TransactionEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_transaction_score_indexes_to_season_median_100(tmp_path):
    db_name = "txn_score_index"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            year INTEGER,
            transaction_type VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            manager_lamar_ros_managed DOUBLE,
            fa_lamar_ros DOUBLE,
            transaction_score DOUBLE,
            transaction_grade VARCHAR,
            score_percentile DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.transactions VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, NULL)",
        [
            (2025, "add", "A", "fid_a", 10.0, 0.0),
            (2025, "pickup", "B", "fid_b", 20.0, 0.0),
            (2025, "claim", "C", "fid_c", 30.0, 0.0),
            (2025, "waiver", "D", "fid_d", 40.0, 0.0),
            (2025, "add/drop", "E", "fid_e", 50.0, 0.0),
            (2025, "drop", "F", "fid_f", 0.0, 30.0),
            (2025, "drop", "G", "fid_g", 0.0, 20.0),
            (2025, "drop", "H", "fid_h", 0.0, 10.0),
            (2025, "commish", "I", "fid_i", 99.0, 0.0),
        ],
    )
    conn.close()

    runner = _TxnRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.transaction_score()
        first = runner.conn.execute(
            """
            SELECT transaction_type, manager, transaction_score
            FROM public.transactions
            ORDER BY manager
            """
        ).fetchall()
        runner.transaction_score()
        second = runner.conn.execute(
            """
            SELECT transaction_type, manager, transaction_score
            FROM public.transactions
            ORDER BY manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert first == [
        ("add", "A", 81.0),
        ("pickup", "B", 90.5),
        ("claim", "C", 100.0),
        ("waiver", "D", 109.5),
        ("add/drop", "E", 119.0),
        ("drop", "F", 85.0),
        ("drop", "G", 100.0),
        ("drop", "H", 115.0),
        ("commish", "I", None),
    ]
    assert second == first


def _setup_txn_db(tmp_path, db_name: str):
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            cumulative_week BIGINT,
            transaction_type VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            faab_bid DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            cumulative_week BIGINT,
            manager VARCHAR,
            franchise_id VARCHAR,
            max_faab_bid_to_date DOUBLE
        )
        """
    )
    conn.close()
    return _TxnRunner(db_name=db_name, data_dir=str(tmp_path))


def test_transactions_to_player_carries_faab_forward_without_same_week_player_row(tmp_path):
    runner = _setup_txn_db(tmp_path, "txn_faab_gap")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('00-0039164', 2024, 10, 202410, 'add', 'Brock', 'fid_brock', 2)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('00-0039164', 2024, 11, 202411, 'Brock', 'fid_brock', NULL),
            ('00-0039164', 2024, 12, 202412, 'Brock', 'fid_brock', NULL)
        """
    )

    try:
        runner.transactions_to_player()
        rows = runner.conn.execute(
            """
            SELECT cumulative_week, max_faab_bid_to_date
            FROM public.player_fantasy
            ORDER BY cumulative_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [(202411, 2.0), (202412, 2.0)]


def test_transactions_to_player_scopes_faab_to_the_acquiring_franchise(tmp_path):
    runner = _setup_txn_db(tmp_path, "txn_faab_owner_scope")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('00-0039164', 2024, 10, 202410, 'add', 'Brock', 'fid_brock', 2),
            ('00-0039164', 2024, 18, 202418, 'add', 'Alex', 'fid_alex', 0)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('00-0039164', 2024, 11, 202411, 'Brock', 'fid_brock', NULL),
            ('00-0039164', 2024, 12, 202412, 'Brock', 'fid_brock', NULL),
            ('00-0039164', 2024, 18, 202418, 'Alex', 'fid_alex', NULL)
        """
    )

    try:
        runner.transactions_to_player()
        rows = runner.conn.execute(
            """
            SELECT manager, cumulative_week, max_faab_bid_to_date
            FROM public.player_fantasy
            ORDER BY cumulative_week, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Brock", 202411, 2.0),
        ("Brock", 202412, 2.0),
        ("Alex", 202418, None),
    ]


def test_transactions_to_player_treats_waiver_rows_as_faab_acquisitions(tmp_path):
    runner = _setup_txn_db(tmp_path, "txn_faab_waiver")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('p1', 2025, 4, 202504, 'waiver', 'Kim', 'fid_kim', 7)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('p1', 2025, 5, 202505, 'Kim', 'fid_kim', NULL)
        """
    )

    try:
        runner.transactions_to_player()
        value = runner.conn.execute(
            """
            SELECT max_faab_bid_to_date
            FROM public.player_fantasy
            WHERE manager = 'Kim'
            """
        ).fetchone()[0]
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert value == 7.0


def test_fix_unknown_managers_backfills_franchise_id_from_nearest_known_row(tmp_path):
    """Site 2 — fix_unknown_managers must backfill franchise_id alongside manager.

    String-only tests (e.g., 'SQL contains franchise_id = nearest.franchise_id') would
    miss alias/name typos that emit syntactically valid SQL pointing at the wrong column.
    This test materializes a real fixture and asserts the backfilled values.
    """
    runner = _setup_txn_db(tmp_path, "txn_unknown_mgr")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('00-0039164', 2024,  5, 202405, 'add', 'Brock', 'fid_brock', 0),
            ('00-0039164', 2024,  7, 202407, 'add',  NULL,        NULL,    0),
            ('00-0039164', 2024,  9, 202409, 'add', 'unknown',    NULL,    0)
        """
    )

    try:
        runner.fix_unknown_managers()
        rows = runner.conn.execute(
            """
            SELECT cumulative_week, manager, franchise_id
            FROM public.transactions
            ORDER BY cumulative_week
            """
        ).fetchall()
        assert rows == [
            (202405, "Brock", "fid_brock"),
            (202407, "Brock", "fid_brock"),
            (202409, "Brock", "fid_brock"),
        ]
    finally:
        if runner._conn is not None:
            runner._conn.close()


def test_fix_unknown_managers_raises_when_franchise_id_column_missing(tmp_path):
    """Site 2 pre-flight check: if transactions table has no franchise_id column, raise."""
    db_name = "txn_unknown_mgr_no_fid"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            cumulative_week BIGINT,
            transaction_type VARCHAR,
            manager VARCHAR,
            faab_bid DOUBLE
        )
        """
    )
    conn.close()
    runner = _TxnRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        with pytest.raises(KeyError, match="franchise_id is required"):
            runner.fix_unknown_managers()
    finally:
        if runner._conn is not None:
            runner._conn.close()


def _setup_pick_conveyance_db(tmp_path, db_name: str):
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            transaction_id VARCHAR,
            year INTEGER,
            week INTEGER,
            transaction_type VARCHAR,
            trade_direction VARCHAR,
            manager VARCHAR,
            player VARCHAR,
            sleeper_player_id VARCHAR,
            traded_pick_season INTEGER,
            traded_pick_round INTEGER,
            traded_pick_original_owner VARCHAR,
            NFL_player_id VARCHAR,
            conveyed_player VARCHAR,
            conveyed_lamar DOUBLE,
            conveyed_fantasy_points DOUBLE,
            conveyed_year INTEGER,
            is_conveyed BOOLEAN,
            manager_lamar_ros_managed DOUBLE,
            player_lamar_ros_managed DOUBLE,
            player_lamar_ros_total DOUBLE,
            fa_lamar_ros DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            round INTEGER,
            draft_slot INTEGER,
            draft_slot_roster_id INTEGER,
            draft_category VARCHAR,
            player VARCHAR,
            manager_lamar DOUBLE,
            total_fantasy_points DOUBLE,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.close()
    return _TxnRunner(db_name=db_name, data_dir=str(tmp_path))


def test_draft_pick_conveyance_uses_roster_id_mapping_without_replacing_pick_label(tmp_path):
    runner = _setup_pick_conveyance_db(tmp_path, "txn_pick_conveyance_rookie")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('tx-mahomes', 2025, 1, 'trade_pick', 'received', 'leslie1231',
             '2025 2nd (from camicies)', 'pick_2025_2_1', 2025, 2, 'camicies',
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.draft VALUES
            (2025, 2, 1, 2, 'rookie', 'Emeka Egbuka', 68.3, 178.2, 'EMEKA'),
            (2025, 2, 2, 1, 'rookie', 'Colston Loveland', 42.0, 101.5, 'COLSTON')
        """
    )

    try:
        runner.draft_pick_conveyances()
        runner.draft_pick_conveyances()
        row = runner.conn.execute(
            """
            SELECT player, conveyed_player, NFL_player_id, conveyed_lamar, is_conveyed
            FROM public.transactions
            """
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert row == (
        "2025 2nd (from camicies) (became Colston Loveland)",
        "Colston Loveland",
        "COLSTON",
        42.0,
        True,
    )
    assert row[0].count("(became Colston Loveland)") == 1


def test_draft_pick_conveyance_still_maps_startup_slots_by_draft_slot(tmp_path):
    runner = _setup_pick_conveyance_db(tmp_path, "txn_pick_conveyance_startup")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('tx-startup', 2022, 1, 'trade_pick', 'received', 'MrPool',
             '2022 1st (from camicies)', 'pick_2022_1_1', 2022, 1, 'camicies',
             NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.draft VALUES
            (2022, 1, 1, 6, 'startup', 'Josh Allen', 210.0, 430.0, 'ALLEN'),
            (2022, 1, 2, 1, 'startup', 'Justin Jefferson', 190.0, 390.0, 'JJ')
        """
    )

    try:
        runner.draft_pick_conveyances()
        row = runner.conn.execute(
            """
            SELECT player, conveyed_player, NFL_player_id
            FROM public.transactions
            """
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert row == ("2022 1st (from camicies) (became Josh Allen)", "Josh Allen", "ALLEN")


def _setup_trade_retention_db(tmp_path, db_name: str, draft_type: str):
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            transaction_id VARCHAR,
            year INTEGER,
            cumulative_week BIGINT,
            transaction_type VARCHAR,
            trade_direction VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            source_franchise_id VARCHAR,
            player VARCHAR,
            NFL_player_id VARCHAR,
            manager_lamar_ros_managed DOUBLE,
            trade_asset_lamar DOUBLE,
            trade_net_lamar DOUBLE,
            trade_grade VARCHAR,
            trade_percentile DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            cumulative_week BIGINT,
            franchise_id VARCHAR,
            manager_lamar DOUBLE,
            is_keeper INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            draft_type VARCHAR,
            max_keepers INTEGER
        )
        """
    )
    conn.execute(
        "INSERT INTO public.league_settings VALUES (2025, ?, 0), (2026, ?, 0), (2027, ?, 0)",
        [draft_type, draft_type, draft_type],
    )
    conn.close()
    return _TxnRunner(db_name=db_name, data_dir=str(tmp_path))


def test_dynasty_trade_value_extends_until_player_leaves_roster(tmp_path):
    runner = _setup_trade_retention_db(tmp_path, "txn_dynasty_trade_retention", "dynasty")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('tx1', 2025, 202501, 'trade', 'received', 'Alpha', 'fid_alpha', 'fid_beta',
             'Colston Loveland', 'COLSTON', 10, NULL, NULL, NULL, NULL),
            ('tx1', 2025, 202501, 'trade', 'sent', 'Beta', 'fid_beta', 'fid_alpha',
             'Colston Loveland', 'COLSTON', 0, NULL, NULL, NULL, NULL),
            ('drop1', 2026, 202603, 'drop', NULL, 'Alpha', 'fid_alpha', NULL,
             'Colston Loveland', 'COLSTON', 0, NULL, NULL, NULL, NULL)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('COLSTON', 2026, 1, 202601, 'fid_alpha', 5, 0),
            ('COLSTON', 2026, 2, 202602, 'fid_alpha', 7, 0),
            ('COLSTON', 2026, 3, 202603, 'fid_beta', 100, 0),
            ('COLSTON', 2027, 1, 202701, 'fid_alpha', 50, 0)
        """
    )

    try:
        runner._compute_trade_net_lamar()
        runner._compute_trade_net_lamar()
        rows = runner.conn.execute(
            """
            SELECT manager, trade_direction, trade_asset_lamar, trade_net_lamar
            FROM public.transactions
            WHERE transaction_id = 'tx1'
            ORDER BY trade_direction, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Alpha", "received", 22.0, 22.0),
        ("Beta", "sent", 22.0, -22.0),
    ]


def test_keeper_trade_value_extends_only_for_kept_years(tmp_path):
    runner = _setup_trade_retention_db(tmp_path, "txn_keeper_trade_retention", "keeper")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('tx1', 2025, 202501, 'trade', 'received', 'Alpha', 'fid_alpha', 'fid_beta',
             'Colston Loveland', 'COLSTON', 10, NULL, NULL, NULL, NULL),
            ('tx1', 2025, 202501, 'trade', 'sent', 'Beta', 'fid_beta', 'fid_alpha',
             'Colston Loveland', 'COLSTON', 0, NULL, NULL, NULL, NULL)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('COLSTON', 2026, 1, 202601, 'fid_alpha', 5, 1),
            ('COLSTON', 2027, 1, 202701, 'fid_alpha', 50, 0)
        """
    )

    try:
        runner._compute_trade_net_lamar()
        rows = runner.conn.execute(
            """
            SELECT manager, trade_direction, trade_asset_lamar, trade_net_lamar
            FROM public.transactions
            ORDER BY trade_direction, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Alpha", "received", 15.0, 15.0),
        ("Beta", "sent", 15.0, -15.0),
    ]


def test_redraft_trade_value_does_not_extend_beyond_rest_of_season(tmp_path):
    runner = _setup_trade_retention_db(tmp_path, "txn_redraft_trade_retention", "redraft")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('tx1', 2025, 202501, 'trade', 'received', 'Alpha', 'fid_alpha', 'fid_beta',
             'Colston Loveland', 'COLSTON', 10, NULL, NULL, NULL, NULL),
            ('tx1', 2025, 202501, 'trade', 'sent', 'Beta', 'fid_beta', 'fid_alpha',
             'Colston Loveland', 'COLSTON', 0, NULL, NULL, NULL, NULL)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('COLSTON', 2026, 1, 202601, 'fid_alpha', 50, 0)
        """
    )

    try:
        runner._compute_trade_net_lamar()
        rows = runner.conn.execute(
            """
            SELECT manager, trade_direction, trade_asset_lamar, trade_net_lamar
            FROM public.transactions
            ORDER BY trade_direction, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Alpha", "received", 10.0, 10.0),
        ("Beta", "sent", 10.0, -10.0),
    ]


def _setup_future_conveyed_pick_trade_db(tmp_path, db_name: str):
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            transaction_id VARCHAR,
            year INTEGER,
            cumulative_week BIGINT,
            transaction_type VARCHAR,
            trade_direction VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            source_franchise_id VARCHAR,
            player VARCHAR,
            NFL_player_id VARCHAR,
            manager_lamar_ros_managed DOUBLE,
            trade_asset_lamar DOUBLE,
            trade_net_lamar DOUBLE,
            trade_grade VARCHAR,
            trade_percentile DOUBLE,
            is_conveyed BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            cumulative_week BIGINT,
            franchise_id VARCHAR,
            manager_lamar DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            draft_type VARCHAR
        )
        """
    )
    conn.execute("INSERT INTO public.league_settings VALUES (2025, 'dynasty'), (2026, 'dynasty'), (2027, 'dynasty')")
    conn.close()
    return _TxnRunner(db_name=db_name, data_dir=str(tmp_path))


def test_future_conveyed_dynasty_pick_extends_from_conveyed_player_value(tmp_path):
    runner = _setup_future_conveyed_pick_trade_db(tmp_path, "txn_future_pick_dynasty_retention")
    runner.conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('tx-pick', 2025, 202501, 'trade_pick', 'received', 'Alpha', 'fid_alpha', 'fid_beta',
             '2026 1st (from Beta) (became Bryce Young)', 'BRYCE', 0, NULL, NULL, NULL, NULL, TRUE),
            ('tx-pick', 2025, 202501, 'trade_pick', 'sent', 'Beta', 'fid_beta', 'fid_alpha',
             '2026 1st (from Beta) (became Bryce Young)', 'BRYCE', 0, NULL, NULL, NULL, NULL, TRUE)
        """
    )
    runner.conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('BRYCE', 2026, 1, 202601, 'fid_alpha', 5),
            ('BRYCE', 2027, 1, 202701, 'fid_alpha', 7)
        """
    )

    try:
        runner._compute_trade_net_lamar()
        runner._compute_trade_net_lamar()
        rows = runner.conn.execute(
            """
            SELECT manager, trade_direction, trade_asset_lamar, trade_net_lamar
            FROM public.transactions
            ORDER BY trade_direction, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Alpha", "received", 12.0, 12.0),
        ("Beta", "sent", 12.0, -12.0),
    ]
