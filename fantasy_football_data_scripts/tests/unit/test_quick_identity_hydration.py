"""Quick imports must use saved identities before any provider-derived rows exist."""

import importlib
import json
import os
import sys
from pathlib import Path

import duckdb
import pytest
import requests

SCRIPT_ROOT = Path(__file__).resolve().parents[2]
if str(SCRIPT_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPT_ROOT))

import initial_import_v3
from multi_league.core import db_reader
from multi_league.core.league_context import LeagueContext
from multi_league.data_fetchers.espn.espn_context import ESPNContext
from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext

SAVED_ALIASES = {"Provider": "Saved Alias"}
SAVED_MERGES = [{"into_franchise_id": "f1", "owner_ids": ["provider-f1", "f1"]}]
IDENTITY_KEYS = {"manager_name_overrides", "franchise_merges", "updated_at"}


@pytest.fixture(autouse=True)
def no_network(monkeypatch):
    from multi_league.core import date_utils

    monkeypatch.setattr(date_utils, "_get_nfl_state_from_api", lambda: {"season": "2026"})

    def forbidden(*args, **kwargs):
        raise BaseException("Unexpected network access in identity test")

    monkeypatch.setattr(requests.sessions.Session, "request", forbidden)


@pytest.fixture
def reader():
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute("SET schema = 'public'")
    conn.execute(
        "CREATE TABLE league_context (db_name VARCHAR, manager_name_overrides_json VARCHAR, franchise_merges_json VARCHAR)"
    )
    conn.execute("INSERT INTO league_context VALUES ('other_league', '{}', '[]')")

    class Reader:
        def query(self, sql, database):
            assert database == "___leagues"
            cursor = conn.execute(sql)
            return [dict(zip([col[0] for col in cursor.description], row)) for row in cursor.fetchall()]

    yield Reader(), conn
    conn.close()


def context(platform, tmp_path):
    kwargs = dict(
        league_name="Identity Test",
        database_name="identity_test",
        data_directory=tmp_path,
        start_year=2025,
        end_year=2026,
        import_mode="quick",
        manager_name_overrides={"Provider": "Stale Alias"},
        franchise_merges=[{"into_franchise_id": "stale", "owner_ids": ["provider-f1", "stale"]}],
    )
    if platform == "yahoo":
        ctx = LeagueContext(
            league_id="470.l.123",
            league_ids={"2025": "461.l.123", "2026": "470.l.123"},
            oauth_credentials={
                "refresh_token": "fake-token",
                "consumer_key": "fake-key",
                "consumer_secret": "fake-secret",
            },
            **kwargs,
        )
    elif platform == "sleeper":
        ctx = SleeperContext(league_id="123", username="owner", league_ids={"2025": "122", "2026": "123"}, **kwargs)
    else:
        ctx = ESPNContext(
            league_id=123, espn_s2="fake-cookie", swid="fake-swid", league_ids={"2025": 122, "2026": 123}, **kwargs
        )
    path = tmp_path / ("league_context.json" if platform == "yahoo" else f"{platform}_context.json")
    ctx.save(path)
    return ctx, path


def insert_saved(conn, aliases=SAVED_ALIASES, merges=SAVED_MERGES):
    conn.execute(
        "INSERT INTO league_context VALUES (?, ?, ?)", ["identity_test", json.dumps(aliases), json.dumps(merges)]
    )


@pytest.mark.parametrize("platform", ["yahoo", "sleeper", "espn"])
@pytest.mark.parametrize("saved", ["nonempty", "empty", "absent"])
@pytest.mark.parametrize("incoming", ["empty", "stale"])
def test_hydration_preserves_scope_auth_and_serializes_exact_saved_identities(
    platform, saved, incoming, tmp_path, reader, monkeypatch
):
    ctx, path = context(platform, tmp_path)
    if incoming == "empty":
        ctx.manager_name_overrides = {}
        ctx.franchise_merges = []
        ctx.save(path)
    before = json.loads(path.read_text())
    original_bytes = path.read_bytes()
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "17")
    if saved != "absent":
        insert_saved(reader[1], {} if saved == "empty" else SAVED_ALIASES, [] if saved == "empty" else SAVED_MERGES)
    initial_import_v3._hydrate_quick_identity_context(ctx, path, reader=reader[0])
    after = json.loads(path.read_text())
    aliases = before["manager_name_overrides"] if saved == "absent" else ({} if saved == "empty" else SAVED_ALIASES)
    merges = before["franchise_merges"] if saved == "absent" else ([] if saved == "empty" else SAVED_MERGES)
    assert ctx.manager_name_overrides == after["manager_name_overrides"] == aliases
    assert ctx.franchise_merges == after["franchise_merges"] == merges
    assert {k: v for k, v in before.items() if k not in IDENTITY_KEYS} == {
        k: v for k, v in after.items() if k not in IDENTITY_KEYS
    }
    assert os.environ["LEAGUE_IMPORT_BASE_GENERATION"] == "17"
    if saved == "absent":
        assert path.read_bytes() == original_bytes


@pytest.mark.parametrize(
    "bad", ["network", "malformed_alias", "malformed_merge", "wrong_alias_type", "wrong_merge_type", "duplicate"]
)
def test_unreadable_identity_stops_without_changing_context(bad, tmp_path, reader):
    ctx, path = context("yahoo", tmp_path)
    before = path.read_bytes()
    insert_saved(reader[1])
    if bad == "network":

        class FailingReader:
            def query(self, sql, database):
                raise TimeoutError("unavailable")

        source = FailingReader()
    else:
        source = reader[0]
        if bad == "duplicate":
            insert_saved(reader[1])
        else:
            field = "manager_name_overrides_json" if "alias" in bad else "franchise_merges_json"
            value = "{" if bad.startswith("malformed") else ("[]" if "alias" in bad else "{}")
            reader[1].execute(f"UPDATE league_context SET {field} = ?", [value])
    with pytest.raises((RuntimeError, ValueError), match="identity"):
        initial_import_v3._hydrate_quick_identity_context(ctx, path, reader=source)
    assert path.read_bytes() == before
    assert ctx.manager_name_overrides == {"Provider": "Stale Alias"}
    assert ctx.franchise_merges[0]["into_franchise_id"] == "stale"


class BoundaryReached(BaseException):
    pass


@pytest.mark.parametrize(
    "platform,module_name,boundary",
    [
        ("yahoo", "initial_import_v3", "_apply_quick_import"),
        ("sleeper", "sleeper_initial_import", "runtime_from_source"),
        ("espn", "espn_initial_import", "get_nfl_state"),
    ],
)
@pytest.mark.parametrize("saved", ["nonempty", "network"])
def test_import_entrypoint_hydrates_before_provider_or_runtime(
    platform, module_name, boundary, saved, tmp_path, reader, monkeypatch
):
    module = importlib.import_module(module_name)
    _, path = context(platform, tmp_path)
    before = path.read_bytes()
    insert_saved(reader[1])
    if saved == "network":

        def unavailable(*args, **kwargs):
            raise TimeoutError("unavailable")

        monkeypatch.setattr(reader[0], "query", unavailable)
    monkeypatch.setattr(db_reader, "get_reader", lambda: reader[0])

    captured = {}

    def next_phase(*args, **kwargs):
        captured["payload"] = json.loads(path.read_text())
        if args:
            captured["ctx"] = args[0]
        raise BoundaryReached

    monkeypatch.setattr(module, boundary, next_phase)
    flags = ["--quick"] if platform == "yahoo" else ["--import-mode", "quick"]
    monkeypatch.setattr(sys, "argv", [module_name, "--context", str(path), *flags])
    if saved == "network":
        with pytest.raises(RuntimeError, match="identity"):
            module.main()
        assert path.read_bytes() == before
    else:
        with pytest.raises(BoundaryReached):
            module.main()
        assert captured["payload"]["manager_name_overrides"] == SAVED_ALIASES
        assert captured["payload"]["franchise_merges"] == SAVED_MERGES
        if "ctx" in captured:
            assert captured["ctx"].manager_name_overrides == SAVED_ALIASES
            assert captured["ctx"].franchise_merges == SAVED_MERGES


def test_sleeper_bootstrap_hydrates_before_history_fetch(tmp_path, reader, monkeypatch):
    import sleeper_initial_import

    insert_saved(reader[1])
    monkeypatch.setattr(db_reader, "get_reader", lambda: reader[0])

    class BootstrapClient:
        def get_league(self, league_id):
            assert league_id == "123"
            return {"name": "Identity Test", "season": "2026"}

    monkeypatch.setattr(sleeper_initial_import, "SleeperAPIClient", BootstrapClient)

    def history(*args, **kwargs):
        raise BoundaryReached

    monkeypatch.setattr(sleeper_initial_import, "discover_league_history", history)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "sleeper_initial_import",
            "--league-id",
            "123",
            "--database-name",
            "identity_test",
            "--data-dir",
            str(tmp_path),
            "--import-mode",
            "quick",
        ],
    )
    with pytest.raises(BoundaryReached):
        sleeper_initial_import.main()
    payload = json.loads((tmp_path / "sleeper_context.json").read_text())
    assert payload["manager_name_overrides"] == SAVED_ALIASES
    assert payload["franchise_merges"] == SAVED_MERGES


def test_strict_loader_treats_sql_null_as_saved_empty(reader):
    reader[1].execute("INSERT INTO league_context VALUES ('identity_test', NULL, NULL)")
    assert initial_import_v3._load_frontend_context_settings(reader[0], "identity_test", strict_identity=True) == {
        "manager_name_overrides": {},
        "franchise_merges": [],
    }
