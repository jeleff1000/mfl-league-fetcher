import duckdb

from multi_league.transformations.aggregation import homepage_summary
from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.transaction.sql_transaction_enrichments import TransactionEnrichmentsMixin


class _TxnRunner(TransactionEnrichmentsMixin, SQLEnrichmentsBase):
    pass


class _TxnCache:
    def __init__(self, columns: set[str]):
        self._columns = columns

    def exists(self, table_name: str) -> bool:
        return table_name == "transactions"

    def columns(self, table_name: str) -> set[str]:
        assert table_name == "transactions"
        return self._columns


def test_trade_rows_use_dedicated_trade_scoring(tmp_path):
    db_name = "txn_trade_direction"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            transaction_id VARCHAR,
            year INTEGER,
            transaction_type VARCHAR,
            trade_direction VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            source_franchise_id VARCHAR,
            player VARCHAR,
            manager_lamar_ros_managed DOUBLE,
            fa_lamar_ros DOUBLE,
            transaction_score DOUBLE,
            transaction_grade VARCHAR,
            score_percentile DOUBLE,
            trade_asset_lamar DOUBLE,
            trade_net_lamar DOUBLE,
            trade_grade VARCHAR,
            trade_percentile DOUBLE
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.transactions VALUES
            ('t1', 2025, 'trade', 'received', 'Gavi',   'f_gavi',   'f_yaacov', 'Nico Collins',         50, 50, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('t1', 2025, 'trade', 'sent',     'Gavi',   'f_gavi',   'f_yaacov', 'Michael Pittman Jr.',   0, 10, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('t1', 2025, 'trade', 'received', 'Yaacov', 'f_yaacov', 'f_gavi',   'Michael Pittman Jr.',  60, 10, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
            ('t1', 2025, 'trade', 'sent',     'Yaacov', 'f_yaacov', 'f_gavi',   'Nico Collins',          0, 50, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )
    conn.close()

    runner = _TxnRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.transaction_score()
        rows = runner.conn.execute(
            """
            SELECT manager, trade_direction, player, transaction_score, trade_asset_lamar, trade_net_lamar
            FROM public.transactions
            ORDER BY manager, trade_direction, player
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Gavi", "received", "Nico Collins", None, 50.0, -10.0),
        ("Gavi", "sent", "Michael Pittman Jr.", None, 60.0, -10.0),
        ("Yaacov", "received", "Michael Pittman Jr.", None, 60.0, 10.0),
        ("Yaacov", "sent", "Nico Collins", None, 50.0, 10.0),
    ]


def test_compute_best_trade_separates_got_and_gave_packages(monkeypatch):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = conn.execute("SELECT current_database()").fetchone()[0]
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            transaction_id VARCHAR,
            year INTEGER,
            week INTEGER,
            transaction_type VARCHAR,
            player VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            source_manager VARCHAR,
            trade_direction VARCHAR,
            trade_asset_lamar DOUBLE
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO public.transactions VALUES
            ('{db_name}', 't1', 2025, 8, 'trade', 'Nico Collins',         'Gavi',   'f_gavi',   'Yaacov', 'received', 90),
            ('{db_name}', 't1', 2025, 8, 'trade', 'Michael Pittman Jr.', 'Gavi',   'f_gavi',   'Yaacov', 'sent',     60),
            ('{db_name}', 't1', 2025, 8, 'trade', 'Michael Pittman Jr.', 'Yaacov', 'f_yaacov', 'Gavi',   'received', 60),
            ('{db_name}', 't1', 2025, 8, 'trade', 'Nico Collins',         'Yaacov', 'f_yaacov', 'Gavi',   'sent',     90)
        """
    )

    monkeypatch.setattr(homepage_summary, "headshot_subquery", lambda *_args, **_kwargs: "NULL")

    cache = _TxnCache(
        {
            "db_name",
            "transaction_id",
            "year",
            "week",
            "transaction_type",
            "player",
            "manager",
            "franchise_id",
            "source_manager",
            "trade_direction",
            "trade_asset_lamar",
        }
    )

    result = homepage_summary._compute_best_trade(conn, db_name, year=2025, platform="yahoo", cache=cache)

    assert result["winner"] == "Gavi"
    assert result["winner_players"] == "Nico Collins"
    assert result["winner_lamar"] == 90.0
    assert result["loser"] == "Yaacov"
    assert result["loser_players"] == "Michael Pittman Jr."
    assert result["loser_lamar"] == 60.0
    assert result["net_lamar"] == 30.0


def test_compute_best_trade_keeps_same_name_trade_partners_separate_by_franchise_id(monkeypatch):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    db_name = conn.execute("SELECT current_database()").fetchone()[0]
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR,
            transaction_id VARCHAR,
            year INTEGER,
            week INTEGER,
            transaction_type VARCHAR,
            player VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            source_manager VARCHAR,
            trade_direction VARCHAR,
            trade_asset_lamar DOUBLE
        )
        """
    )
    conn.execute(
        f"""
        INSERT INTO public.transactions VALUES
            ('{db_name}', 't_same', 2025, 9, 'trade', 'Alpha Asset', 'Chris', 'fid_alpha', 'Chris', 'received', 90),
            ('{db_name}', 't_same', 2025, 9, 'trade', 'Beta Asset',  'Chris', 'fid_alpha', 'Chris', 'sent',     60),
            ('{db_name}', 't_same', 2025, 9, 'trade', 'Beta Asset',  'Chris', 'fid_beta',  'Chris', 'received', 60),
            ('{db_name}', 't_same', 2025, 9, 'trade', 'Alpha Asset', 'Chris', 'fid_beta',  'Chris', 'sent',     90)
        """
    )

    monkeypatch.setattr(homepage_summary, "headshot_subquery", lambda *_args, **_kwargs: "NULL")

    cache = _TxnCache(
        {
            "db_name",
            "transaction_id",
            "year",
            "week",
            "transaction_type",
            "player",
            "manager",
            "franchise_id",
            "source_manager",
            "trade_direction",
            "trade_asset_lamar",
        }
    )

    result = homepage_summary._compute_best_trade(conn, db_name, year=2025, platform="yahoo", cache=cache)

    assert result["winner_players"] == "Alpha Asset"
    assert result["loser_players"] == "Beta Asset"
    assert result["winner_lamar"] == 90.0
    assert result["loser_lamar"] == 60.0
    assert result["net_lamar"] == 30.0
