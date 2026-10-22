"""Undated moves stay visible without distorting dated move scores."""
import duckdb
import pytest
from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.transaction.sql_transaction_enrichments import TransactionEnrichmentsMixin


class Runner(TransactionEnrichmentsMixin, SQLEnrichmentsBase):
    pass


@pytest.mark.parametrize('platform', ['yahoo','espn','sleeper'])
def test_unresolved_timing_does_not_get_a_score_or_change_dated_scores(tmp_path, platform):
    conn = duckdb.connect(str(tmp_path/'qa.duckdb'))
    conn.execute('CREATE SCHEMA public')
    conn.execute('CREATE TABLE public.transactions (year INTEGER,cumulative_week BIGINT,'
                 'transaction_type VARCHAR,manager VARCHAR,franchise_id VARCHAR,platform VARCHAR,'
                 'manager_lamar_ros_managed DOUBLE,fa_lamar_ros DOUBLE,transaction_score DOUBLE,'
                 'transaction_grade VARCHAR,score_percentile DOUBLE)')
    conn.execute("INSERT INTO public.transactions VALUES (2026,202606,'add','Known','known',?,"
                 "10,10,NULL,NULL,NULL),(2026,NULL,'add','Undated','undated',?,999,999,NULL,NULL,NULL)",
                 [platform,platform])
    conn.close()
    runner = Runner(db_name='qa',data_dir=str(tmp_path))
    try:
        for _ in range(2):
            runner.transaction_score()
            assert runner.conn.execute('SELECT manager,transaction_score,transaction_grade '
                                       'FROM public.transactions ORDER BY manager').fetchall() == [
                                           ('Known',100.0,None),('Undated',None,None)]
    finally:
        runner.conn.close()


def _dated_and_undated_runner(tmp_path):
    from multi_league.core.canonical_transaction import TRANSACTION_SCHEMA
    conn = duckdb.connect(str(tmp_path/'qa.duckdb'))
    conn.execute('CREATE SCHEMA public')
    conn.execute('CREATE TABLE public.transactions (' + ','.join(
        f'"{name}" {dtype}' for name,dtype,_ in TRANSACTION_SCHEMA) + ')')
    conn.execute("INSERT INTO public.transactions (db_name,transaction_id,year,week,cumulative_week,transaction_type,"
        "manager,franchise_id,NFL_player_id) VALUES ('qa','known',2026,6,202606,'add','Known','known','P'),"
        "('qa','undated',2026,NULL,NULL,'add','Unknown',NULL,'P')")
    conn.execute('CREATE TABLE public.player_fantasy (NFL_player_id VARCHAR,year INTEGER,week INTEGER,'
        'cumulative_week BIGINT,franchise_id VARCHAR,fantasy_points DOUBLE,player_lamar DOUBLE,manager_lamar DOUBLE)')
    conn.execute("INSERT INTO public.player_fantasy VALUES ('P',2026,7,202607,'known',20,10,10)")
    conn.execute('CREATE TABLE public.league_settings (year INTEGER,end_week INTEGER)')
    conn.execute('INSERT INTO public.league_settings VALUES (2026,14)')
    conn.close()
    return Runner(db_name='qa',data_dir=str(tmp_path))


def test_undated_move_cannot_infer_an_arbitrary_owner(tmp_path):
    runner = _dated_and_undated_runner(tmp_path)
    try:
        runner.fix_unknown_managers()
        assert runner.conn.execute("SELECT manager,franchise_id FROM public.transactions "
            "WHERE transaction_id='undated'").fetchone() == ('Unknown',None)
    finally:
        runner.conn.close()


def test_undated_move_keeps_date_dependent_performance_null(tmp_path):
    runner = _dated_and_undated_runner(tmp_path)
    try:
        runner.player_to_transactions()
        runner.transaction_lamar_ros()
        fields = 'fa_lamar_ros,player_lamar_ros_total,manager_lamar_ros_managed,total_points_ros_managed,weeks_before,weeks_after'
        assert runner.conn.execute(f"SELECT {fields} FROM public.transactions WHERE transaction_id='undated'").fetchone() == (None,) * 6
        assert runner.conn.execute("SELECT fa_lamar_ros,total_points_ros_managed FROM public.transactions "
            "WHERE transaction_id='known'").fetchone() == (10,20)
    finally:
        runner.conn.close()


def test_cached_timing_is_resolved_before_transaction_enrichment(tmp_path):
    runner = _dated_and_undated_runner(tmp_path)
    try:
        conn = runner.conn
        conn.execute("UPDATE public.transactions SET platform='yahoo'")
        conn.execute("UPDATE public.transactions SET week=1,cumulative_week=202601,"
                     "timestamp=1792065600 WHERE transaction_id='known'")
        conn.execute("CREATE TABLE public.matchup AS SELECT 2026 AS year,6 AS week,"
                     "'2026-10-13' AS week_start,'2026-10-19' AS week_end")
        runner.transaction_weeks_from_matchup()
        assert conn.execute("SELECT transaction_id,week,cumulative_week FROM public.transactions "
                            "ORDER BY transaction_id").fetchall() == [('known',6,202606),('undated',None,None)]
        runner.player_to_transactions()
        runner.transaction_lamar_ros()
        assert conn.execute("SELECT fa_lamar_ros,total_points_ros_managed FROM public.transactions "
                            "WHERE transaction_id='known'").fetchone() == (10,20)
        assert conn.execute("SELECT fa_lamar_ros,total_points_ros_managed FROM public.transactions "
                            "WHERE transaction_id='undated'").fetchone() == (None,None)
    finally:
        runner.conn.close()


def test_unresolved_trade_timing_has_no_fabricated_package_value(tmp_path):
    from multi_league.core.canonical_transaction import TRANSACTION_SCHEMA
    conn = duckdb.connect(str(tmp_path/'qa.duckdb'))
    conn.execute('CREATE SCHEMA public')
    conn.execute('CREATE TABLE public.transactions (' + ','.join(
        f'"{name}" {dtype}' for name,dtype,_ in TRANSACTION_SCHEMA) + ')')
    conn.executemany('INSERT INTO public.transactions (db_name,transaction_id,year,cumulative_week,'
        'transaction_type,trade_direction,manager,franchise_id,source_franchise_id,NFL_player_id,'
        'manager_lamar_ros_managed) VALUES (?,?,?,?,?,?,?,?,?,?,?)', [
            ('qa','known',2026,202606,'trade','received','A','a','b','P',10),
            ('qa','known',2026,202606,'trade','sent','B','b','a','P',0),
            ('qa','undated',2026,None,'trade','received','A','a','b','Q',999),
            ('qa','undated',2026,None,'trade','sent','B','b','a','Q',0)])
    conn.close()
    runner = Runner(db_name='qa',data_dir=str(tmp_path))
    try:
        runner._compute_trade_net_lamar()
        assert runner.conn.execute('SELECT trade_asset_lamar,trade_net_lamar,trade_grade '
            "FROM public.transactions WHERE transaction_id='undated'").fetchall() == [(None,None,None)] * 2
        assert runner.conn.execute('SELECT trade_asset_lamar FROM public.transactions '
            "WHERE transaction_id='known'").fetchall() == [(10.0,), (10.0,)]
    finally:
        runner.conn.close()


@pytest.mark.parametrize('platform', ['espn', 'sleeper'])
def test_native_bad_period_is_cleared_before_pickup_drop_and_managed_ros(tmp_path, platform):
    runner = _dated_and_undated_runner(tmp_path)
    try:
        conn = runner.conn
        conn.execute('UPDATE public.transactions SET platform=?', [platform])
        conn.execute("UPDATE public.transactions SET week=23,cumulative_week=202623,"
                     "manager='Known',franchise_id='known',manager_lamar_ros_managed=999,"
                     "total_points_ros_managed=999,transaction_score=0 WHERE transaction_id='undated'")
        conn.execute("INSERT INTO public.transactions (db_name,transaction_id,year,week,cumulative_week,"
                     "platform,transaction_type,manager,franchise_id,NFL_player_id,manager_lamar_ros_managed) "
                     "VALUES ('qa','undated-drop',2026,23,202623,?,'drop','Known','known','P',999)", [platform])
        runner.transaction_weeks_from_matchup()
        assert conn.execute("SELECT week,cumulative_week,transaction_score FROM public.transactions "
                            "WHERE transaction_id LIKE 'undated%'").fetchall() == [(None,None,None)] * 2
        runner.player_to_transactions()
        runner.transaction_lamar_ros()
        runner.transaction_score()
        fields = ('fa_lamar_ros,player_lamar_ros_total,manager_lamar_ros_managed,total_points_ros_managed,'
                  'weeks_before,weeks_after,weeks_ros_managed,transaction_score')
        assert conn.execute(f"SELECT {fields} FROM public.transactions WHERE transaction_id LIKE 'undated%'").fetchall() == [(None,) * 8] * 2
        assert conn.execute("SELECT fa_lamar_ros,total_points_ros_managed FROM public.transactions "
                            "WHERE transaction_id='known'").fetchone() == (10,20)
        assert conn.execute("SELECT transaction_id,transaction_type,NFL_player_id,franchise_id "
                            "FROM public.transactions WHERE transaction_id LIKE 'undated%' ORDER BY transaction_id").fetchall() == [
                                ('undated','add','P','known'),('undated-drop','drop','P','known')]
        assert all('native period=23' in note for (note,) in conn.execute(
            "SELECT notes FROM public.transactions WHERE transaction_id LIKE 'undated%'").fetchall())
    finally:
        runner.conn.close()


@pytest.mark.parametrize('platform', ['espn', 'sleeper'])
def test_unknown_native_trade_assets_are_excluded_from_seed_mirror_and_rank_pool(tmp_path, platform):
    from multi_league.core.canonical_transaction import TRANSACTION_SCHEMA
    conn = duckdb.connect(str(tmp_path/'qa.duckdb'))
    conn.execute('CREATE SCHEMA public')
    conn.execute('CREATE TABLE public.transactions (' + ','.join(
        f'"{name}" {dtype}' for name,dtype,_ in TRANSACTION_SCHEMA) + ')')
    rows = [('qa',f'dated-{i}',2026,6,202606,platform,'trade',direction,owner,owner,other,'P',float(i+5))
            for i in range(32) for direction,owner,other in [('received','A','B'),('sent','B','A')]]
    conn.executemany('INSERT INTO public.transactions (db_name,transaction_id,year,week,cumulative_week,platform,'
        'transaction_type,trade_direction,manager,franchise_id,source_franchise_id,NFL_player_id,'
        'manager_lamar_ros_managed) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)', rows)
    conn.close()
    runner = Runner(db_name='qa',data_dir=str(tmp_path))
    try:
        conn = runner.conn
        runner._compute_trade_net_lamar()
        dated = "SELECT transaction_id,franchise_id,trade_asset_lamar,trade_net_lamar,trade_grade,trade_percentile FROM public.transactions WHERE transaction_id LIKE 'dated-%' ORDER BY ALL"
        baseline = conn.execute(dated).fetchall()
        assert len(baseline) == 64 and all(row[4] is not None for row in baseline)
        for kind,asset in [('trade','Q'),('trade_pick','pick_2027_1_A')]:
            conn.executemany('INSERT INTO public.transactions (db_name,transaction_id,year,week,cumulative_week,platform,'
                'transaction_type,trade_direction,manager,franchise_id,source_franchise_id,sleeper_player_id,'
                'manager_lamar_ros_managed,trade_asset_lamar) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)', [
                    ('qa','unknown',2026,23,202623,platform,kind,direction,owner,owner,other,asset,999,999)
                    for direction,owner,other in [('received','A','B'),('sent','B','A')]])
        runner.transaction_weeks_from_matchup()
        runner._compute_trade_net_lamar()
        assert conn.execute(dated).fetchall() == baseline
        assert conn.execute("SELECT week,cumulative_week,trade_asset_lamar,trade_net_lamar,trade_grade,trade_percentile "
                            "FROM public.transactions WHERE transaction_id='unknown'").fetchall() == [(None,) * 6] * 4
        assert conn.execute("SELECT transaction_type,sleeper_player_id,trade_direction,franchise_id "
                            "FROM public.transactions WHERE transaction_id='unknown' ORDER BY ALL").fetchall() == [
                                ('trade','Q','received','A'),('trade','Q','sent','B'),
                                ('trade_pick','pick_2027_1_A','received','A'),('trade_pick','pick_2027_1_A','sent','B')]
    finally:
        runner.conn.close()
