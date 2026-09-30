import json

import duckdb

from scripts.merge_league_admin import (
    _persist_merged_league_ids,
    _rebuild_league_derived,
    run_merge,
)


class DuckDbAdapter:
    def __init__(self) -> None:
        self.connection = duckdb.connect(":memory:")
        self.connection.execute("CREATE SCHEMA public")
        self.connection.execute(
            """
            CREATE TABLE public.league_context (
                db_name VARCHAR,
                league_id VARCHAR,
                league_ids_json VARCHAR,
                updated_at TIMESTAMP
            )
            """
        )

    def query(self, sql: str, *, database: str):
        del database
        cursor = self.connection.execute(sql)
        columns = [description[0] for description in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def execute(self, sql: str, *, database: str):
        del database
        self.connection.execute(sql)


def test_persist_merged_league_ids_replaces_only_selected_years() -> None:
    db = DuckDbAdapter()
    source_ids = {
        "2009": "222.l.same",
        "2013": "314.l.source",
        "2014": "331.l.source",
        "2015": "348.l.source",
        "2016": "359.l.source",
    }
    target_ids = {
        "2009": "222.l.same",
        "2012": "273.l.target",
        "2013": "314.l.target",
        "2014": "331.l.target",
        "2015": "348.l.target",
        "2016": "359.l.target",
        "2017": "371.l.target",
        "2026": "470.l.target",
    }
    db.connection.executemany(
        "INSERT INTO public.league_context VALUES (?, ?, ?, TIMESTAMP '2026-09-15')",
        [
            ("l_1st_down_7500", "359.l.source", json.dumps(source_ids)),
            ("pass_interferance", "470.l.target", json.dumps(target_ids)),
        ],
    )

    stats = _persist_merged_league_ids(
        reader=db,
        writer=db,
        source_db="l_1st_down_7500",
        target_db="pass_interferance",
        merge_years=[2013, 2014, 2015, 2016],
    )

    target = db.query(
        "SELECT league_id, league_ids_json FROM public.league_context "
        "WHERE db_name = 'pass_interferance'",
        database="___leagues",
    )[0]
    assert target["league_id"] == "470.l.target"
    assert json.loads(target["league_ids_json"]) == {
        "2009": "222.l.same",
        "2012": "273.l.target",
        "2013": "314.l.source",
        "2014": "331.l.source",
        "2015": "348.l.source",
        "2016": "359.l.source",
        "2017": "371.l.target",
        "2026": "470.l.target",
    }
    assert stats == {"league_context_year_ids_persisted": "2013, 2014, 2015, 2016"}


def test_persist_merged_league_ids_rejects_missing_source_year() -> None:
    db = DuckDbAdapter()
    db.connection.executemany(
        "INSERT INTO public.league_context VALUES (?, ?, ?, CURRENT_TIMESTAMP)",
        [
            ("source", "314.l.source", '{"2013":"314.l.source"}'),
            ("target", "470.l.target", '{"2013":"314.l.target","2026":"470.l.target"}'),
        ],
    )

    try:
        _persist_merged_league_ids(
            reader=db,
            writer=db,
            source_db="source",
            target_db="target",
            merge_years=[2013, 2014],
        )
    except RuntimeError as exc:
        assert str(exc) == "Source league_context is missing league IDs for merge years: 2014"
    else:  # pragma: no cover - the assertion documents the required guard
        raise AssertionError("missing source-year league ID was accepted")

    target = db.query(
        "SELECT league_ids_json FROM public.league_context WHERE db_name = 'target'",
        database="___leagues",
    )[0]
    assert json.loads(target["league_ids_json"]) == {
        "2013": "314.l.target",
        "2026": "470.l.target",
    }


def test_rebuild_league_derived_uses_bounded_atomic_server_endpoint(monkeypatch) -> None:
    observed = {}

    class Response:
        status_code = 200
        text = '{"status":"COMMITTED"}'

        @staticmethod
        def json():
            return {
                "status": "COMMITTED",
                "db_name": "pass_interferance",
                "generation": 42,
            }

    def fake_post(url, *, json, headers, timeout):
        observed.update(url=url, json=json, headers=headers, timeout=timeout)
        return Response()

    monkeypatch.setenv("DATABASE_SERVER_URL", "https://fly.example")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "admin-token")
    monkeypatch.setenv("FLY_PRIMARY_MACHINE_ID", "machine-1")
    monkeypatch.setattr("scripts.merge_league_admin.requests.post", fake_post)

    result = _rebuild_league_derived(
        target_db="pass_interferance",
        run_id="admin-merge-123",
    )

    assert result["status"] == "COMMITTED"
    assert observed == {
        "url": "https://fly.example/rebuild-league-derived",
        "json": {
            "db_name": "pass_interferance",
            "run_id": "admin-merge-123",
            "timeout_seconds": 90,
        },
        "headers": {
            "Authorization": "Bearer admin-token",
            "fly-force-instance-id": "machine-1",
        },
        "timeout": 50,
    }


def test_finalize_only_skips_source_copy_and_runs_existing_derived_rebuild(monkeypatch) -> None:
    calls = []

    monkeypatch.setattr(
        "scripts.merge_league_admin._persist_merged_league_ids",
        lambda **_kwargs: {"league_context_year_ids_persisted": "2013, 2014, 2015, 2016"},
    )
    monkeypatch.setattr(
        "scripts.merge_league_admin._rebuild_league_derived",
        lambda **kwargs: calls.append(kwargs)
        or {"status": "COMMITTED", "generation": 43, "db_name": kwargs["target_db"]},
    )
    monkeypatch.setenv("GITHUB_RUN_ID", "999")

    result = run_merge(
        {
            "source_db": "l_1st_down_7500",
            "target_db": "pass_interferance",
            "merge_years": [2013, 2014, 2015, 2016],
            "finalize_only": True,
        },
        reader=object(),
        writer=object(),
    )

    assert result == {
        "status": "finalizing_existing_copy",
        "league_context_year_ids_persisted": "2013, 2014, 2015, 2016",
        "derived_rebuild_status": "COMMITTED",
        "derived_generation": 43,
    }
    assert calls == [
        {
            "target_db": "pass_interferance",
            "run_id": "league-admin-merge-999-pass_interferance",
        }
    ]
