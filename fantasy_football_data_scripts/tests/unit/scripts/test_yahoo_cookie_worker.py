from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        db_name="you_are_a_pirate",
        league_name="You Are A Pirate",
        team_count=10,
        start_year=2014,
        end_year=2016,
        league_keys_json='{"2014":"331.l.492605","2015":"348.l.727365","2016":"359.l.272424"}',
        context_json=None,
        output_dir=str(tmp_path),
        import_mode="full",
        skip_track_1=True,
        skip_upload=False,
    )


def test_cookie_worker_dispatches_the_shared_importer(monkeypatch, tmp_path):
    """The encrypted-cookie worker must not invoke the legacy HTML runner."""
    from scripts import yahoo_cookie_worker as worker

    context_path = tmp_path / "league_context_cookie_full.json"
    context_path.write_text("{}", encoding="utf-8")
    calls: list[list[str]] = []

    monkeypatch.setattr(worker, "_write_shared_import_context", lambda _args: context_path, raising=False)
    monkeypatch.setattr(
        worker,
        "subprocess",
        SimpleNamespace(run=lambda command, **_kwargs: calls.append(command) or SimpleNamespace(returncode=0)),
        raising=False,
    )

    result = worker.run_shared_import(_args(tmp_path))

    assert result == 0
    assert calls == [[
        worker.PYTHON,
        str(worker.SHARED_IMPORTER),
        "--context",
        str(context_path),
        "--skip-track-1",
    ]]


def test_cookie_worker_uses_local_page_replay_when_cache_only(monkeypatch, tmp_path):
    """A retained raw-page cache must bypass the OAuth-oriented shared fetchers."""
    from scripts import yahoo_cookie_worker as worker

    calls: list[argparse.Namespace] = []
    monkeypatch.setenv("COOKIE_BACKUP_CACHE_ONLY", "1")
    monkeypatch.setattr(worker, "run_cached_page_replay", lambda args: calls.append(args) or 0, raising=False)
    monkeypatch.setattr(
        worker,
        "run_shared_import",
        lambda _args: (_ for _ in ()).throw(AssertionError("shared importer must not run in cache-only mode")),
    )

    assert worker.run(_args(tmp_path)) == 0
    assert calls and calls[0].db_name == "you_are_a_pirate"


def test_cookie_worker_context_keeps_the_full_renewal_chain(tmp_path):
    """Shared fetchers receive every discovered season key through cookie mode."""
    from scripts import yahoo_cookie_worker as worker

    path = worker._write_shared_import_context(_args(tmp_path))
    payload = __import__("json").loads(path.read_text(encoding="utf-8"))

    assert payload["yahoo_auth_mode"] == "cookie"
    assert payload["require_oauth"] is False
    assert payload["league_ids"] == {
        "2014": "331.l.492605",
        "2015": "348.l.727365",
        "2016": "359.l.272424",
    }
    assert payload["cookie_jar_path"] == "fly://___ops.main.yahoo_web_credentials/you_are_a_pirate"
