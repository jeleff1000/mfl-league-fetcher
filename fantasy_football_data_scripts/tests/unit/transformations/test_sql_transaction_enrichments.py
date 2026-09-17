import duckdb
import pytest

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.transaction.sql_transaction_enrichments import TransactionEnrichmentsMixin


class _TxnRunner(TransactionEnrichmentsMixin, SQLEnrichmentsBase):
    pass


@pytest.mark.parametrize(
    "drop_week,drop_timestamp,weekly_owners,expected_lamar,expected_points",
    [
        (1, 300, [(1, "owner", 5.53, 12.54)], 5.53, 12.54),
        (1, 300, [(1, "other", 5.53, 12.54)], 0, 0),
        (2, 400, [(1, "owner", 5.53, 12.54), (2, "other", 8, 20)], 5.53, 12.54),
        (2, 400, [(1, "owner", 5.53, 12.54), (2, "owner", 8, 20)], 13.53, 32.54),
        (1, 300, [(1, "owner", 5.53, 12.54), (3, "owner", 8, 20)], 5.53, 12.54),
        # A same-leg drop BEFORE the acquisition must not end that acquisition.
        (1, 100, [(1, "owner", 5.53, 12.54), (2, "owner", 8, 20)], 13.53, 32.54),
    ],
)
def test_managed_transaction_value_follows_scoring_roster_not_drop_leg(
    tmp_path, drop_week, drop_timestamp, weekly_owners, expected_lamar, expected_points,
):
    """Sleeper can report a post-game drop in the same leg as an earned start."""
    runner = _setup_txn_db(tmp_path, "txn_scoring_ownership")
    conn = runner.conn
    for column, dtype in {
        "transaction_id": "VARCHAR", "timestamp": "VARCHAR", "trade_direction": "VARCHAR",
        "manager_lamar_ros_managed": "DOUBLE", "fa_lamar_ros": "DOUBLE",
        "player_lamar_ros_total": "DOUBLE", "total_points_ros_managed": "DOUBLE",
        "ppg_ros_managed": "DOUBLE", "weeks_ros_managed": "INTEGER",
    }.items():
        conn.execute(f"ALTER TABLE public.transactions ADD COLUMN {column} {dtype}")
    for column in ("manager_lamar", "player_lamar", "fantasy_points"):
        conn.execute(f"ALTER TABLE public.player_fantasy ADD COLUMN {column} DOUBLE")
    conn.execute("CREATE TABLE public.league_settings (year INTEGER, end_week INTEGER)")
    conn.execute("INSERT INTO public.league_settings VALUES (2026, 14)")
    conn.executemany(
        "INSERT INTO public.transactions (NFL_player_id,year,week,cumulative_week,"
        "transaction_type,manager,franchise_id,transaction_id,timestamp) VALUES (?,?,?,?,?,?,?,?,?)",
        [
            ("00-0023459", 2026, 1, 202601, "add", "Old alias", "owner", "add", "200"),
            ("00-0023459", 2026, drop_week, 202600 + drop_week, "drop", "Old alias", "owner", "drop", str(drop_timestamp)),
        ],
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy (NFL_player_id,year,week,cumulative_week,manager,"
        "franchise_id,manager_lamar,player_lamar,fantasy_points) VALUES (?,?,?,?,?,?,?,?,?)",
        [("00-0023459", 2026, week, 202600 + week, "New alias", owner, lamar, lamar, points)
         for week, owner, lamar, points in weekly_owners],
    )
    try:
        for _ in range(2):
            runner.transaction_lamar_ros()
            runner._player_ros_points()
            earned = conn.execute(
                "SELECT manager_lamar_ros_managed,total_points_ros_managed "
                "FROM public.transactions WHERE transaction_id='add'",
            ).fetchone()
            assert earned == pytest.approx((expected_lamar, expected_points))
            assert conn.execute(
                "SELECT manager_lamar_ros_managed,total_points_ros_managed "
                "FROM public.transactions WHERE transaction_id='drop'",
            ).fetchone() == (0, 0)
    finally:
        conn.close()


@pytest.mark.parametrize("timestamp_type", ["VARCHAR", "TIMESTAMP", "NULL", "ABSENT"])
def test_managed_transaction_windows_accept_historical_timestamp_representations(tmp_path, timestamp_type):
    runner = _setup_managed_window_db(tmp_path)
    conn = runner.conn
    if timestamp_type != "ABSENT":
        dtype = "VARCHAR" if timestamp_type == "NULL" else timestamp_type
        conn.execute(f"ALTER TABLE public.transactions ADD COLUMN timestamp {dtype}")
    conn.execute("""
        INSERT INTO public.transactions (NFL_player_id,year,week,cumulative_week,transaction_type,
            manager,franchise_id,transaction_id)
        VALUES ('P',2026,1,202601,'drop','Alias','owner','drop'),
               ('P',2026,1,202601,'add','Alias','owner','add')
    """)
    if timestamp_type not in ("ABSENT", "NULL"):
        conn.execute("UPDATE public.transactions SET timestamp = '2026-09-01 12:00:00' WHERE transaction_id='drop'")
        conn.execute("UPDATE public.transactions SET timestamp = '2026-09-02 12:00:00' WHERE transaction_id='add'")
    conn.execute("""
        INSERT INTO public.player_fantasy VALUES
            ('P',2026,1,202601,'New alias','owner',NULL,5.53,5.53,12.54),
            ('P',2026,2,202602,'New alias','owner',NULL,8,8,20),
            ('P',2026,3,202603,'New alias','owner',NULL,4,4,10)
    """)
    try:
        runner.transaction_lamar_ros()
        runner._player_ros_points()
        assert conn.execute("SELECT manager_lamar_ros_managed,total_points_ros_managed FROM public.transactions WHERE transaction_id='add'").fetchone() == pytest.approx((17.53, 42.54))
    finally:
        conn.close()


def _setup_managed_window_db(tmp_path):
    runner = _setup_txn_db(tmp_path, "managed_windows")
    conn = runner.conn
    for column, dtype in {
        "transaction_id": "VARCHAR", "trade_direction": "VARCHAR",
        "manager_lamar_ros_managed": "DOUBLE", "fa_lamar_ros": "DOUBLE",
        "player_lamar_ros_total": "DOUBLE", "total_points_ros_managed": "DOUBLE",
        "ppg_ros_managed": "DOUBLE", "weeks_ros_managed": "INTEGER",
    }.items():
        conn.execute(f"ALTER TABLE public.transactions ADD COLUMN {column} {dtype}")
    for column in ("manager_lamar", "player_lamar", "fantasy_points"):
        conn.execute(f"ALTER TABLE public.player_fantasy ADD COLUMN {column} DOUBLE")
    conn.execute("CREATE TABLE public.league_settings (year INTEGER, end_week INTEGER)")
    conn.execute("INSERT INTO public.league_settings VALUES (2026, 14)")
    return runner


def test_managed_transaction_reacquisition_does_not_double_credit_scoring_week(tmp_path):
    runner = _setup_managed_window_db(tmp_path)
    conn = runner.conn
    conn.execute("ALTER TABLE public.transactions ADD COLUMN timestamp VARCHAR")
    conn.execute("""
        INSERT INTO public.transactions (NFL_player_id,year,week,cumulative_week,transaction_type,
            manager,franchise_id,transaction_id,timestamp)
        VALUES ('P',2026,1,202601,'add','Alias','owner','first','100'),
               ('P',2026,2,202602,'drop','Alias','owner','drop','200'),
               ('P',2026,2,202602,'add','Alias','owner','second','300')
    """)
    conn.execute("""
        INSERT INTO public.player_fantasy VALUES
            ('P',2026,1,202601,'New alias','owner',NULL,5.53,5.53,12.54),
            ('P',2026,2,202602,'New alias','owner',NULL,8,8,20),
            ('P',2026,3,202603,'New alias','owner',NULL,4,4,10)
    """)
    try:
        for _ in range(2):
            runner.transaction_lamar_ros()
            runner._player_ros_points()
            rows = conn.execute("SELECT transaction_id,manager_lamar_ros_managed,total_points_ros_managed FROM public.transactions WHERE transaction_type='add' ORDER BY transaction_id").fetchall()
            assert rows == [("first", 5.53, 12.54), ("second", 12, 30)]
    finally:
        conn.close()


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


def test_draft_pick_conveyance_uses_original_roster_identity_even_for_startup(tmp_path):
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

    # Sleeper trade roster_id=1 is not draft_slot=1. The explicit original
    # roster mapping identifies slot 2 even when legacy category says startup.
    assert row == ("2022 1st (from camicies) (became Justin Jefferson)", "Justin Jefferson", "JJ")


@pytest.mark.parametrize("category", ["startup", "veteran", "rookie"])
def test_pick_conveyance_does_not_borrow_unrelated_rosters_negative_value(tmp_path, category):
    runner = _setup_pick_conveyance_db(tmp_path, "txn_pick_original_roster")
    try:
        runner.conn.execute("""
            INSERT INTO public.transactions
                (transaction_id,year,week,transaction_type,trade_direction,manager,
                 player,sleeper_player_id,traded_pick_season,traded_pick_round,
                 traded_pick_original_owner)
            VALUES ('pick-swap',2026,1,'trade_pick','received','Shared Alias',
                    '2026 10th (from Original Owner)','pick_2026_10_4',2026,10,'Original Owner')
        """)
        runner.conn.executemany("INSERT INTO public.draft VALUES (?,?,?,?,?,?,?,?,?)", [
            (2026,10,4,3,category,'Unrelated Defense',-12.925,-2.1,'DEF-14'),
            (2026,10,6,4,category,'Correct Bench Player',0,5.7,'correct-player'),
        ])
        for _ in range(2):
            runner.draft_pick_conveyances()
            assert runner.conn.execute("""
                SELECT player,NFL_player_id,conveyed_lamar,manager_lamar_ros_managed,
                       is_conveyed,manager
                FROM public.transactions
            """).fetchone() == (
                '2026 10th (from Original Owner) (became Correct Bench Player)',
                'correct-player',0,0,True,'Shared Alias',
            )
    finally:
        if runner._conn is not None:
            runner._conn.close()


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


def test_trade_pick_mirroring_uses_stable_pick_id_not_stale_conveyed_player(tmp_path):
    runner = _setup_future_conveyed_pick_trade_db(tmp_path, "txn_stable_pick_mirror")
    runner.conn.execute("ALTER TABLE public.transactions ADD COLUMN sleeper_player_id VARCHAR")
    runner.conn.execute("ALTER TABLE public.transactions ADD COLUMN traded_pick_season INTEGER")
    runner.conn.execute("ALTER TABLE public.transactions ADD COLUMN traded_pick_round INTEGER")
    runner.conn.execute("ALTER TABLE public.transactions ADD COLUMN traded_pick_original_owner VARCHAR")
    runner.conn.execute(
        """
        INSERT INTO public.transactions
            (transaction_id, year, cumulative_week, transaction_type, trade_direction,
             manager, franchise_id, source_franchise_id, player, NFL_player_id,
             manager_lamar_ros_managed, sleeper_player_id, traded_pick_season,
             traded_pick_round, traded_pick_original_owner, is_conveyed)
        VALUES
            ('pick-swap', 2023, 202301, 'trade_pick', 'received',
             'Alpha', 'fid_alpha', 'fid_beta', 'Stale Conveyed Player', 'STALE',
             17, 'pick_2023_1_5', 2023, 1, 'Original Owner', TRUE),
            ('pick-swap', 2023, 202301, 'trade_pick', 'sent',
             'Beta', 'fid_beta', 'fid_alpha', 'Correct Conveyed Player', 'CORRECT',
             0, 'pick_2023_1_5', 2023, 1, 'Original Owner', TRUE)
        """
    )

    try:
        runner._compute_trade_net_lamar()
        rows = runner.conn.execute(
            """
            SELECT trade_direction, trade_asset_lamar, trade_net_lamar
            FROM public.transactions
            ORDER BY trade_direction
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("received", 17.0, 17.0),
        ("sent", 17.0, -17.0),
    ]
