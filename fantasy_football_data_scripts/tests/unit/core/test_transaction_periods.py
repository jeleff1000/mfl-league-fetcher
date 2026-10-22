"""Publication must not turn unavailable transaction periods into valid weeks."""

import duckdb
import pytest

from multi_league.core.delta_publish import build_delta_bundle
from multi_league.core.fleet_publish import build_fleet_partition_bundle


def staged(platform, week, *, timestamp=1728520236, db_name="qa"):
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.transactions (db_name VARCHAR, transaction_id VARCHAR, "
                 "transaction_sequence INTEGER, platform VARCHAR, year INTEGER, week DOUBLE, "
                 "timestamp BIGINT, cumulative_week BIGINT, week_start VARCHAR, week_end VARCHAR)")
    conn.execute("INSERT INTO public.transactions VALUES (?, 'trade', 0, ?, 2024, ?, ?, ?, NULL, NULL)",
                 [db_name, platform, week, timestamp, None if week is None else int(202400 + week)])
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, "
                 "week_start VARCHAR, week_end VARCHAR, manager_week VARCHAR)")
    conn.execute("INSERT INTO public.matchup VALUES (?, 2024, 6, '2024-10-08', '2024-10-14', 'qa_2024_6')", [db_name])
    return conn


@pytest.mark.parametrize("lane", ["full", "quick", "fleet"])
@pytest.mark.parametrize("platform,week", [("sleeper", None), ("espn", 1.5)])
def test_every_bundle_lane_retains_unknown_native_period(tmp_path, lane, platform, week):
    with staged(platform, week) as conn:
        if lane == "fleet":
            bundle = build_fleet_partition_bundle(conn, active_year=2024, league_generations={"qa": 0},
                                                  tables=["transactions"], output_dir=tmp_path)
        else:
            bundle = build_delta_bundle(conn, db_name="qa", import_mode=lane, platform=platform,
                                        base_generation=0, output_dir=tmp_path)
        assert bundle.path.is_file()
        assert conn.execute("SELECT transaction_id,week,cumulative_week FROM public.transactions").fetchall() == [
            ("trade", None, None)]
        assert "native period=" in conn.execute("SELECT notes FROM public.transactions").fetchone()[0]


@pytest.mark.parametrize("platform,week,timestamp", [("sleeper", 0, None), ("sleeper", 1, 1700000000),
                                                    ("espn", 12, None), ("yahoo", 6, 1728520236)])
def test_valid_native_and_yahoo_periods(platform, week, timestamp):
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged(platform, week, timestamp=timestamp) as conn:
        validate_transaction_weeks(conn, ["qa"])


@pytest.mark.parametrize('lane', ['full', 'quick', 'fleet'])
@pytest.mark.parametrize('platform', ['espn', 'sleeper'])
def test_native_periods_publish_without_matchup_calendar(tmp_path, platform, lane):
    """Preseason, live-week and postseason moves do not depend on final games."""
    from multi_league.core.league_refresh import stage_refresh_partitions
    with staged(platform, 0, timestamp=None) as conn:
        conn.execute('DROP TABLE public.matchup')
        conn.execute("INSERT INTO public.transactions VALUES "
                     "('qa','live-week',0,?,2024,6,NULL,202406,NULL,NULL),"
                     "('qa','postseason',0,?,2024,18,NULL,202418,NULL,NULL)", [platform, platform])
        if lane == 'fleet':
            with stage_refresh_partitions(conn, db_name='qa', active_year=2024,
                                          tables=['transactions']) as stage:
                bundle = build_fleet_partition_bundle(stage, active_year=2024,
                    league_generations={'qa':0}, tables=['transactions'], output_dir=tmp_path)
        else:
            bundle = build_delta_bundle(conn, db_name='qa', import_mode=lane, platform=platform,
                                        base_generation=0, output_dir=tmp_path)
        assert bundle.path.is_file()
        assert conn.execute('SELECT week,cumulative_week FROM public.transactions ORDER BY week').fetchall() == [
            (0,202400), (6,202406), (18,202418)]
        assert '__yahoo_transaction_windows' not in str(bundle.manifest)


def test_validation_does_not_inspect_unrelated_league():
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged("yahoo", 1, db_name="other") as conn:
        validate_transaction_weeks(conn, ["qa"])


def test_missing_yahoo_date_windows_warn_without_blocking(caplog):
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged("yahoo", 6) as conn:
        conn.execute("DELETE FROM public.matchup")
        validate_transaction_weeks(conn, ["qa"])
        assert "transaction timing" in caplog.text
        assert conn.execute("SELECT COUNT(*) FROM public.transactions").fetchone()[0] == 1
        assert conn.execute("SELECT week,cumulative_week FROM public.transactions").fetchall() == [(None,None)]


def _ongoing_yahoo_calendar(conn):
    # Independent Yahoo calendar, not dates copied from transaction output.
    conn.execute("UPDATE public.matchup SET week=5, week_start='2024-10-01', week_end='2024-10-07'")
    conn.execute("CREATE TABLE public.__yahoo_transaction_windows "
                 "(db_name VARCHAR, year INTEGER, week INTEGER, week_start DATE, week_end DATE)")
    conn.execute("INSERT INTO public.__yahoo_transaction_windows VALUES "
                 "('qa',2024,5,'2024-10-01','2024-10-07'),"
                 "('qa',2024,6,'2024-10-08','2024-10-14'),"
                 "('other',2024,6,'2024-10-09','2024-10-15'),"
                 "('qa',2023,6,'2023-10-10','2023-10-16')")


@pytest.mark.parametrize('lane', ['full', 'quick', 'fleet'])
def test_ongoing_yahoo_week_publishes_without_finalizing_matchup(tmp_path, lane):
    from multi_league.core.league_refresh import stage_refresh_partitions
    with staged('yahoo', 6) as conn:
        _ongoing_yahoo_calendar(conn)
        if lane == 'fleet':
            with stage_refresh_partitions(conn, db_name='qa', active_year=2024,
                                          tables=['matchup', 'transactions']) as stage:
                bundle = build_fleet_partition_bundle(stage, active_year=2024,
                    league_generations={'qa':0}, tables=['matchup','transactions'], output_dir=tmp_path)
                assert stage.execute('SELECT db_name,year,week FROM public.__yahoo_transaction_windows ORDER BY week').fetchall() == [
                    ('qa',2024,5), ('qa',2024,6)]
        else:
            bundle = build_delta_bundle(conn, db_name='qa', import_mode=lane, platform='yahoo',
                                        base_generation=0, output_dir=tmp_path)
        assert '__yahoo_transaction_windows' not in str(bundle.manifest)
        assert conn.execute('SELECT week FROM public.matchup').fetchall() == [(5,)]


@pytest.mark.parametrize('bad', ['wrong_week', 'conflict', 'other_league', 'wrong_year'])
def test_yahoo_calendar_issues_warn_without_blocking_publication(tmp_path, bad, caplog):
    with staged('yahoo', 6) as conn:
        _ongoing_yahoo_calendar(conn)
        if bad == 'wrong_week':
            conn.execute('UPDATE public.transactions SET week=1,cumulative_week=202401')
        elif bad == 'conflict':
            conn.execute("INSERT INTO public.__yahoo_transaction_windows VALUES ('qa',2024,6,'2024-10-09','2024-10-15')")
        elif bad == 'other_league':
            conn.execute("DELETE FROM public.__yahoo_transaction_windows WHERE db_name='qa'")
        else:
            conn.execute('UPDATE public.__yahoo_transaction_windows SET year=2023')
        bundle = build_fleet_partition_bundle(conn, active_year=2024, league_generations={'qa':0},
            tables=['matchup','transactions'], output_dir=tmp_path)
        assert bundle.path.is_file()
        assert "transaction timing" in caplog.text
        assert conn.execute("SELECT COUNT(*) FROM public.transactions").fetchone()[0] == 1
        assert conn.execute("SELECT week,cumulative_week FROM public.transactions").fetchall() == [(None,None)]


def test_legacy_upload_rejects_bad_year_before_remote_write(monkeypatch, tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.targets import fly_target
    class NoWriteTarget:
        def merge_league(self, *args, **kwargs):
            pytest.fail("invalid transaction reached upload")
    monkeypatch.setattr(fly_target, "FlyTarget", NoWriteTarget)
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "duckdb")
    db = LocalLeagueDB(tmp_path, "qa")
    try:
        conn = db.connect()
        conn.execute("CREATE TABLE public.transactions (year INTEGER, week INTEGER, platform VARCHAR)")
        conn.execute("INSERT INTO public.transactions VALUES (NULL, NULL, 'espn')")
        with pytest.raises(ValueError, match="transaction week"):
            db.upload_to_fly("qa", import_mode="full", platform="espn", finalize_merge_source=False)
    finally:
        db.close()


def test_yahoo_period_uses_utc_even_when_duckdb_timezone_differs():
    from multi_league.core.transaction_periods import validate_transaction_weeks
    from datetime import UTC, datetime
    stamp = int(datetime(2024, 10, 8, 0, 0, tzinfo=UTC).timestamp())
    with staged("yahoo", 6, timestamp=stamp) as conn:
        conn.execute("SET TimeZone='America/Los_Angeles'")
        validate_transaction_weeks(conn, ["qa"])


def test_yahoo_preseason_convention_requires_an_actual_first_window():
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged("yahoo", 1, timestamp=1724000000) as conn:
        conn.execute("UPDATE public.matchup SET week=1, week_start='2024-09-05', week_end='2024-09-09'")
        validate_transaction_weeks(conn, ["qa"])


def test_explicit_estimated_espn_history_is_preserved():
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged("espn", 6, timestamp=None) as conn:
        conn.execute("ALTER TABLE public.transactions ADD COLUMN is_estimated BOOLEAN DEFAULT TRUE")
        validate_transaction_weeks(conn, ["qa"])


@pytest.mark.parametrize("script,fetcher", [("espn_initial_import.py", "fetch_all_espn_transactions"),
                                          ("sleeper_initial_import.py", "fetch_all_sleeper_transactions")])
def test_import_orchestrator_does_not_swallow_bad_transaction_period(script, fetcher):
    """Execute the real fetch try/except without installing or running an entire import."""
    import ast
    from pathlib import Path
    from types import SimpleNamespace
    from multi_league.core.transaction_periods import TransactionWeekError
    source = Path(__file__).resolve().parents[3] / script
    tree = ast.parse(source.read_text(encoding="utf-8"))
    candidates = [node for node in ast.walk(tree) if isinstance(node, ast.Try)
                  and any(isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
                          and child.func.id == fetcher for statement in node.body for child in ast.walk(statement))]
    node = min(candidates, key=lambda item: item.end_lineno - item.lineno)
    def failed_fetch(*args, **kwargs):
        raise TransactionWeekError("Unverified transaction week")
    namespace = {fetcher: failed_fetch, "TransactionWeekError": TransactionWeekError,
                 "ctx": SimpleNamespace(), "args": SimpleNamespace(dry_run=False),
                 "log": lambda *args: None, "results": {"fetchers": []}, "fetcher_dfs": {},
                 "client": None, "player_cache": None, "year_filter": None, "db": None}
    with pytest.raises(TransactionWeekError):
        exec(compile(ast.Module(body=[node], type_ignores=[]), str(source), "exec"), namespace)


def test_fleet_validates_every_league_actually_exported(tmp_path):
    with staged("sleeper", None, db_name="other") as conn:
        conn.execute("UPDATE public.transactions SET year=NULL")
        with pytest.raises(ValueError, match="transaction week"):
            build_fleet_partition_bundle(conn, active_year=2024, league_generations={"qa":0},
                                         tables=["transactions"], output_dir=tmp_path)


@pytest.mark.parametrize("lane", ["full", "quick"])
def test_yahoo_hint_applies_to_blank_cached_platform(tmp_path, lane, caplog):
    with staged(None, 1) as conn:
        bundle = build_delta_bundle(conn, db_name="qa", import_mode=lane, platform="yahoo",
                                    base_generation=0, output_dir=tmp_path)
        assert bundle.path.is_file()
        assert "transaction timing" in caplog.text


@pytest.mark.parametrize("lane", ["quick", "full", "fleet"])
def test_undated_yahoo_trade_publishes_with_null_week(tmp_path, lane, caplog):
    import tarfile
    with staged("yahoo", None, timestamp=None) as conn:
        if lane == "fleet":
            bundle = build_fleet_partition_bundle(conn, active_year=2024,
                league_generations={"qa":0}, tables=["transactions"], output_dir=tmp_path)
        else:
            bundle = build_delta_bundle(conn, db_name="qa", import_mode=lane, platform="yahoo",
                base_generation=0, output_dir=tmp_path)
        with tarfile.open(bundle.path) as archive:
            payload = archive.extractfile("tables/transactions.parquet").read()
        exported = tmp_path / "exported.parquet"
        exported.write_bytes(payload)
        assert conn.execute("SELECT transaction_id,week,cumulative_week FROM read_parquet(?)",
                            [str(exported)]).fetchall() == [("trade",None,None)]
        assert "transaction timing" in caplog.text


def test_unresolved_transaction_provenance_is_rejected():
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged(None, 1) as conn:
        with pytest.raises(ValueError, match="transaction week"):
            validate_transaction_weeks(conn,["qa"])


def test_period_guard_does_not_change_other_provider_import_contracts():
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged("mfl", None) as conn:
        validate_transaction_weeks(conn,["qa"])


@pytest.mark.parametrize("platform", ["espn", "sleeper"])
@pytest.mark.parametrize("week,cw", [(None, 202406), (1.5, 202401), (-1, 202399),
    (23, 202423), (float("nan"), None), (float("inf"), None)])
def test_cached_native_timing_retains_raw_evidence_and_clears_only_dependent_metrics(platform, week, cw):
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged(platform, 6) as conn:
        conn.execute("ALTER TABLE public.transactions ADD COLUMN notes VARCHAR DEFAULT 'original evidence'")
        conn.execute("ALTER TABLE public.transactions ADD COLUMN NFL_player_id VARCHAR DEFAULT 'P'")
        conn.execute("ALTER TABLE public.transactions ADD COLUMN transaction_score DOUBLE DEFAULT 99")
        conn.execute("ALTER TABLE public.transactions ADD COLUMN conveyed_player VARCHAR DEFAULT 'Pick result'")
        conn.execute("UPDATE public.transactions SET week=?,cumulative_week=?,week_start='stale'", [week,cw])
        validate_transaction_weeks(conn, ["qa"])
        assert conn.execute("SELECT transaction_id,NFL_player_id,conveyed_player,week,cumulative_week,"
                            "week_start,transaction_score FROM public.transactions").fetchone() == (
                                'trade','P','Pick result',None,None,None,None)
        note = conn.execute("SELECT notes FROM public.transactions").fetchone()[0]
        assert 'original evidence' in note and 'native period=' in note
        assert f'cumulative_week={cw if cw is not None else "NULL"}' in note
        validate_transaction_weeks(conn, ["qa"])
        assert conn.execute("SELECT notes FROM public.transactions").fetchone()[0] == note


@pytest.mark.parametrize('platform', ['espn', 'sleeper'])
@pytest.mark.parametrize('cw', [None, 0, 202405])
def test_known_native_period_repairs_only_derived_key(platform, cw):
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged(platform, 6) as conn:
        conn.execute("ALTER TABLE public.transactions ADD COLUMN transaction_score DOUBLE DEFAULT 99")
        conn.execute("UPDATE public.transactions SET cumulative_week=?,week_start='2024-10-08'", [cw])
        validate_transaction_weeks(conn, ['qa'])
        assert conn.execute('SELECT week,cumulative_week,week_start,transaction_score FROM public.transactions').fetchone() == (
            6,202406,'2024-10-08',99)


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_unknown_period_does_not_excuse_invalid_year(platform):
    from multi_league.core.transaction_periods import validate_transaction_weeks
    with staged(platform, None) as conn:
        conn.execute("UPDATE public.transactions SET year=NULL")
        with pytest.raises(ValueError, match="transaction week"):
            validate_transaction_weeks(conn, ["qa"])


@pytest.mark.parametrize("platform", ["espn", "sleeper"])
@pytest.mark.parametrize("period", [None, '', 'unknown', -1, 1.5, True, 23, float('nan'), float('inf')])
def test_native_unknown_period_is_nullable(platform, period):
    from multi_league.core.transaction_periods import native_transaction_week
    assert native_transaction_week(period, platform=platform, transaction_id='real') is None


@pytest.mark.parametrize("period,want", [(0,0), ('6',6), ('22',22)])
@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_native_explicit_period_is_never_guessed(platform, period, want):
    from multi_league.core.transaction_periods import native_transaction_week
    assert native_transaction_week(period, platform=platform, transaction_id='real') == want


def test_yahoo_native_calendar_validation_still_raises():
    from multi_league.core.transaction_periods import native_transaction_week
    with pytest.raises(ValueError, match="transaction week"):
        native_transaction_week(None, platform='yahoo', transaction_id='calendar')


def test_yahoo_import_wrapper_propagates_invalid_period():
    import ast
    from pathlib import Path
    from types import SimpleNamespace
    from multi_league.core.transaction_periods import TransactionWeekError
    source = Path(__file__).resolve().parents[3] / "initial_import_v3.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    fn = next(node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "run_v3_fetcher")
    node = next(node for node in ast.walk(fn) if isinstance(node, ast.Try))
    def failed_fetch(*args, **kwargs):
        raise TransactionWeekError("Unverified transaction week")
    namespace = {"fetch_year_fn": failed_fetch, "TransactionWeekError":TransactionWeekError,
                 "ensure_session_healthy":lambda oauth,ctx: oauth, "ctx":SimpleNamespace(data_directory=None),
                 "oauth":None,"year":2024,"fetcher_name":"transactions","failures":{},
                 "write_manifest":lambda *args,**kwargs:None,"log":lambda *args:None}
    with pytest.raises(TransactionWeekError):
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),"exec"),namespace)


@pytest.mark.parametrize("script,callee", [("sleeper_initial_import.py", "fetch_sleeper_transactions"),
                                         ("initial_import_v3.py", "run_yahoo_recovery")])
def test_offseason_and_outer_recovery_handlers_propagate_invalid_period(script, callee):
    import ast
    from pathlib import Path
    from multi_league.core.transaction_periods import TransactionWeekError
    source = Path(__file__).resolve().parents[3] / script
    tree = ast.parse(source.read_text(encoding="utf-8"))
    candidates = [node for node in ast.walk(tree) if isinstance(node, ast.Try)
                  and any(isinstance(child, ast.Call) and isinstance(child.func, ast.Name)
                          and child.func.id == callee for statement in node.body for child in ast.walk(statement))]
    node = min(candidates, key=lambda item:item.end_lineno - item.lineno)
    # Isolate the actual handler chain: the rest of this try requires an entire import.
    node.body = [ast.Raise(exc=ast.Call(func=ast.Name(id="TransactionWeekError",ctx=ast.Load()),
                                      args=[ast.Constant("Unverified transaction week")],keywords=[]),cause=None)]
    ast.fix_missing_locations(node)
    namespace = {"TransactionWeekError":TransactionWeekError,"log":lambda *args:None,"offseason_year":2025}
    with pytest.raises(TransactionWeekError):
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(source),"exec"),namespace)
