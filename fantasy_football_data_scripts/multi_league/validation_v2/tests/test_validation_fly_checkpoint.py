"""Post-publication validation must wait for checkpoints and stay tenant-scoped."""
from io import BytesIO
import json
from email.message import Message
from http.client import IncompleteRead
import urllib.error

import duckdb
import pytest

from multi_league.validation_v2.manifest import load_manifest
from multi_league.validation_v2.validate import FlyConnection


def http_error(code=503, reason="checkpointing", retry_after="1"):
    headers = Message()
    headers["Retry-After"] = retry_after
    return urllib.error.HTTPError(
        "https://fly.test/query", code, "busy", headers,
        BytesIO(json.dumps({"detail": "Query service is busy", "reason": reason}).encode()),
    )


def test_post_publish_manifest_read_survives_ten_second_checkpoint(monkeypatch):
    clock = {"now": 0, "calls": 0, "sleeps": []}

    def sleep(delay):
        clock["sleeps"].append(delay)
        clock["now"] += delay

    def open_request(_request, **_kwargs):
        clock["calls"] += 1
        if clock["now"] < 10:
            raise http_error()
        return BytesIO(b'[{"year":2026,"rank_alltime_qb_4pt":216}]')

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    monkeypatch.setattr("multi_league.validation_v2.validate.time.sleep", sleep)
    result = FlyConnection("https://fly.test", "test-token").execute(
        "SELECT year, rank_alltime_qb_4pt FROM public.player_fantasy WHERE db_name='target'"
    )

    assert result.fetchall() == [(2026, 216)]
    assert clock["sleeps"] == [5, 5]
    assert clock["calls"] == 3


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
def test_single_league_manifest_filters_every_data_query(platform):
    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute("ATTACH ':memory:' AS ___ops")
        conn.execute("CREATE SCHEMA ___ops.accounts")
        conn.execute("""
            CREATE TABLE public.league_settings AS SELECT
                'target'::VARCHAR AS db_name, ?::VARCHAR AS platform,
                10 AS num_teams, 15 AS playoff_start_week, 6 AS playoff_teams,
                FALSE AS uses_median, 17 AS end_week, 100 AS waiver_budget,
                2 AS max_keepers, FALSE AS is_dynasty, 'snake' AS draft_type, 2026 AS year
        """, [platform])
        conn.execute("""INSERT INTO public.league_settings
            SELECT 'unrelated', platform, num_teams, playoff_start_week, playoff_teams,
                   uses_median, end_week, waiver_budget, max_keepers, is_dynasty, draft_type, year
            FROM public.league_settings""")
        conn.execute("CREATE TABLE public.matchup AS SELECT db_name, 0.5 AS x0_win, 1 AS is_consolation FROM public.league_settings")
        conn.execute("CREATE TABLE public.draft AS SELECT db_name, 1 AS is_keeper FROM public.league_settings")
        conn.execute("CREATE TABLE public.player_fantasy AS SELECT db_name, 'Unrostered' AS manager FROM public.league_settings")
        conn.execute("CREATE TABLE ___ops.accounts.league_inventory AS SELECT db_name AS database_name, TRUE AS in_centralized FROM public.league_settings")
        queries = []

        class ScopedConnection:
            @property
            def description(self):
                return conn.description

            def execute(self, sql):
                queries.append(sql)
                return conn.execute(sql)

        manifest = load_manifest(ScopedConnection(), table_prefix="public.", db_names=["target"])
        assert manifest.all_leagues == ["target"]
        assert getattr(manifest, platform + "_leagues") == ["target"]
        assert manifest.sim_leagues == ["target"]
        assert manifest.keeper_leagues == ["target"]
        assert manifest.consolation_leagues == ["target"]
        assert manifest.full_import_leagues == ["target"]
        assert list(manifest.settings) == ["target"]
        for sql in queries:
            if not sql.startswith("DESCRIBE"):
                assert "db_name IN ('target')" in sql
    finally:
        conn.close()


def test_checkpoint_retry_exhaustion_retains_response_details(monkeypatch):
    monkeypatch.setattr(FlyConnection, "MAX_RETRIES", 3)
    sleeps = []
    monkeypatch.setattr("multi_league.validation_v2.validate.time.sleep", sleeps.append)
    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(http_error()))
    with pytest.raises(RuntimeError, match='"reason": "checkpointing"'):
        FlyConnection("https://fly.test", "test-token").execute("SELECT 1")
    assert sleeps == [5, 5]


@pytest.mark.parametrize("retry_after", ["-1", "nan", "inf", "bad"])
def test_checkpoint_retry_ignores_invalid_delay_headers(monkeypatch, retry_after):
    attempts = []
    sleeps = []

    def open_request(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise http_error(retry_after=retry_after)
        return BytesIO(b'[{"ok":1}]')

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    monkeypatch.setattr("multi_league.validation_v2.validate.time.sleep", sleeps.append)
    assert FlyConnection("https://fly.test", "test-token").execute("SELECT 1").fetchall() == [(1,)]
    assert sleeps == [5]


def test_catalog_errors_still_fail_as_real_data_errors(monkeypatch):
    error = urllib.error.HTTPError(
        "https://fly.test/query", 500, "invalid query", Message(), BytesIO(b'Catalog Error: missing table'),
    )
    monkeypatch.setattr(FlyConnection, "MAX_RETRIES", 1)
    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: (_ for _ in ()).throw(error))
    with pytest.raises(duckdb.CatalogException, match="missing table"):
        FlyConnection("https://fly.test", "test-token").execute("SELECT 1")


def test_truncated_busy_response_cannot_abort_read_recovery(monkeypatch):
    class TruncatedBody(BytesIO):
        def read(self, *_args, **_kwargs):
            raise IncompleteRead(b'{"reason":', 20)

    attempts = []
    sleeps = []
    body = TruncatedBody()

    def open_request(*_args, **_kwargs):
        attempts.append(1)
        if len(attempts) == 1:
            raise urllib.error.HTTPError("https://fly.test/query", 503, "busy", Message(), body)
        return BytesIO(b'[{"ok":1}]')

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    monkeypatch.setattr("multi_league.validation_v2.validate.time.sleep", sleeps.append)
    assert FlyConnection("https://fly.test", "test-token").execute("SELECT 1").fetchall() == [(1,)]
    assert sleeps == [1]
    assert body.closed
