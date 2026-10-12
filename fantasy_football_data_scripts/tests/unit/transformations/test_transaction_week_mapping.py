"""Transaction weeks must come from dated league windows, not a week-one default."""

import duckdb

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.transaction.sql_transaction_enrichments import TransactionEnrichmentsMixin


class Runner(TransactionEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_yahoo_transaction_dates_correct_legacy_weeks_without_touching_other_leagues():
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute("""CREATE TABLE public.transactions (
        db_name VARCHAR, transaction_id VARCHAR, platform VARCHAR, year INTEGER, week INTEGER,
        timestamp BIGINT, cumulative_week BIGINT, week_start VARCHAR, week_end VARCHAR,
        manager VARCHAR, trade_grade VARCHAR)
    """)
    conn.execute("""CREATE TABLE public.matchup (
        db_name VARCHAR, year INTEGER, week INTEGER, week_start VARCHAR, week_end VARCHAR)
    """)
    conn.execute("""INSERT INTO public.matchup VALUES
        ('kmffl',2024,6,NULL,NULL),
        ('kmffl',2024,6,'2024-10-08','2024-10-14'),
        ('kmffl',2024,6,'2024-10-08','2024-10-14'),
        ('kmffl',2024,13,'2024-11-26','2024-12-02'),
        ('other',2024,2,'2024-10-08','2024-10-14')
    """)
    conn.execute("""INSERT INTO public.transactions VALUES
        ('kmffl','oct','yahoo',2024,1,1728520236,202401,NULL,NULL,'Alias','A'),
        ('kmffl','nov','yahoo',2024,1,1733008695,202401,NULL,NULL,'Alias','B'),
        ('other','oct','yahoo',2024,1,1728520236,202401,NULL,NULL,'Keep','C'),
        ('kmffl','native','sleeper',2024,7,1728520236,202407,NULL,NULL,'Keep','D'),
        ('kmffl','invalid','yahoo',2024,9,NULL,202409,NULL,NULL,'Keep','E')
    """)
    runner = Runner('kmffl', conn=conn)
    runner._qualified_name = lambda table: f'public.{table}'
    before = conn.execute("SELECT db_name,transaction_id,manager,trade_grade FROM public.transactions ORDER BY ALL").fetchall()
    runner.transaction_weeks_from_matchup()
    assert conn.execute("SELECT week,cumulative_week,week_start,week_end FROM public.transactions WHERE transaction_id='oct' AND db_name='kmffl'").fetchone() == (6,202406,'2024-10-08','2024-10-14')
    assert conn.execute("SELECT week FROM public.transactions WHERE transaction_id='nov'").fetchone() == (13,)
    assert conn.execute("SELECT week FROM public.transactions WHERE db_name='other'").fetchone() == (1,)
    assert conn.execute("SELECT week FROM public.transactions WHERE transaction_id='native'").fetchone() == (7,)
    assert conn.execute("SELECT week FROM public.transactions WHERE transaction_id='invalid'").fetchone() == (9,)
    assert conn.execute("SELECT db_name,transaction_id,manager,trade_grade FROM public.transactions ORDER BY ALL").fetchall() == before
    first = conn.execute("SELECT * FROM public.transactions ORDER BY ALL").fetchall()
    runner.transaction_weeks_from_matchup()
    assert conn.execute("SELECT * FROM public.transactions ORDER BY ALL").fetchall() == first
    conn.close()


def test_conflicting_week_windows_do_not_guess():
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.matchup (year INTEGER,week INTEGER,week_start VARCHAR,week_end VARCHAR)")
    conn.execute("INSERT INTO public.matchup VALUES (2024,6,'2024-10-08','2024-10-14'),(2024,7,'2024-10-08','2024-10-14')")
    conn.execute("CREATE TABLE public.transactions (year INTEGER,week INTEGER,timestamp BIGINT,platform VARCHAR,cumulative_week BIGINT,week_start VARCHAR,week_end VARCHAR)")
    conn.execute("INSERT INTO public.transactions VALUES (2024,1,1728520236,'yahoo',202401,NULL,NULL)")
    Runner('kmffl', conn=conn, data_dir='local').transaction_weeks_from_matchup()
    assert conn.execute("SELECT week FROM public.transactions").fetchone() == (1,)
    conn.close()
