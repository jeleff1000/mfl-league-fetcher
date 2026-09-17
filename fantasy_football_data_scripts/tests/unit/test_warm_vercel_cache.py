import importlib.util
import json
import sys
from pathlib import Path
from urllib.error import HTTPError

import pytest


def _load_warmer():
    script = Path(__file__).resolve().parents[3] / "scripts" / "warm_vercel_cache.py"
    spec = importlib.util.spec_from_file_location("warm_vercel_cache", script)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_manifest_route_inventory_maps_touched_tables(tmp_path):
    warmer = _load_warmer()
    manifest = {
        "db_name": "speed_test",
        "tables": [
            {"table": "matchup", "included": True},
            {"table": "transactions", "included": True},
        ],
    }
    manifest_path = tmp_path / "delta_publish_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    targets = list(
        warmer.iter_warm_targets(
            "https://leaguehistory.app",
            "speed_test",
            "quick",
            str(manifest_path),
        )
    )
    urls = {target.url for target in targets}
    cache_required = {target.path for target in targets if target.cache_class == "cache_required"}

    assert "https://leaguehistory.app/speed_test/transactions" in urls
    assert "https://leaguehistory.app/api/league/speed_test/transactions" in urls
    assert "/api/league/{db}/matchups?meta=1" in cache_required
    assert "/api/league/{db}/transactions" in cache_required


def test_manifest_route_inventory_maps_luck_tables(tmp_path):
    warmer = _load_warmer()
    manifest = {
        "db_name": "speed_test",
        "tables": [
            {"table": "all_play", "included": True},
            {"table": "h2h_season", "included": True},
            {"table": "schedule_swap", "included": True},
            {"table": "schedule_swap_season", "included": True},
        ],
    }
    manifest_path = tmp_path / "delta_publish_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    targets = list(
        warmer.iter_warm_targets(
            "https://leaguehistory.app",
            "speed_test",
            "quick",
            str(manifest_path),
        )
    )
    urls = {target.url for target in targets}
    cache_required = {target.path for target in targets if target.cache_class == "cache_required"}

    assert "https://leaguehistory.app/speed_test/luck" in urls
    assert "https://leaguehistory.app/api/league/speed_test/luck" in urls
    assert "https://leaguehistory.app/api/league/speed_test/luck/h2h" in urls
    assert "https://leaguehistory.app/api/league/speed_test/luck/schedule-swap" in urls
    assert "/api/league/{db}/luck" in cache_required
    assert "/api/league/{db}/luck/h2h" in cache_required
    assert "/api/league/{db}/luck/schedule-swap" in cache_required


def test_fly_visibility_allows_merge_source_post_upload_growth(tmp_path, monkeypatch, capsys):
    warmer = _load_warmer()
    manifest_path = tmp_path / "delta_publish_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "db_name": "merge_target",
                "tables": [{"table": "draft", "included": True, "row_count": 900}],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "sleeper_context.json").write_text(
        json.dumps({"merge_source": {"source_db": "old_history"}}),
        encoding="utf-8",
    )
    monkeypatch.setattr(warmer, "_fly_query", lambda sql, timeout: [{"cnt": 2850}])

    warmer.verify_fly_commit_visible(str(manifest_path), "merge_target", 1)

    assert "post-upload merge_source finalization" in capsys.readouterr().out


def test_fly_visibility_allows_merge_sources_post_upload_growth(tmp_path, monkeypatch, capsys):
    warmer = _load_warmer()
    manifest_path = tmp_path / "delta_publish_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "db_name": "merge_target",
                "tables": [{"table": "draft", "included": True, "row_count": 900}],
            }
        ),
        encoding="utf-8",
    )
    (tmp_path / "sleeper_context.json").write_text(
        json.dumps({"merge_sources": [{"source_db": "old_history"}]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(warmer, "_fly_query", lambda sql, timeout: [{"cnt": 2850}])

    warmer.verify_fly_commit_visible(str(manifest_path), "merge_target", 1)

    assert "post-upload merge_source finalization" in capsys.readouterr().out


def test_fly_visibility_still_rejects_unexplained_count_growth(tmp_path, monkeypatch):
    warmer = _load_warmer()
    manifest_path = tmp_path / "delta_publish_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "db_name": "plain_import",
                "tables": [{"table": "draft", "included": True, "row_count": 900}],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.delenv("DATA_DIR", raising=False)
    monkeypatch.setattr(warmer, "_fly_query", lambda sql, timeout: [{"cnt": 2850}])

    with pytest.raises(RuntimeError, match="expected 900, saw 2850"):
        warmer.verify_fly_commit_visible(str(manifest_path), "plain_import", 1)


def test_percentile_95_uses_sorted_tail():
    warmer = _load_warmer()
    assert warmer.percentile_95([1, 100, 5, 10, 7]) == 100
    assert warmer.percentile_95([]) == 0


class _FakeHttpResponse:
    status = 200
    headers = {"x-vercel-cache": "HIT"}

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return b"ok"


def test_warm_one_retries_transient_errors(monkeypatch):
    warmer = _load_warmer()
    calls = {"count": 0}

    def fake_urlopen(req, timeout):
        calls["count"] += 1
        if calls["count"] == 1:
            raise TimeoutError("slow")
        if calls["count"] == 2:
            raise HTTPError(req.full_url, 429, "rate limited", hdrs=None, fp=None)
        return _FakeHttpResponse()

    monkeypatch.setattr(warmer, "urlopen", fake_urlopen)
    monkeypatch.setattr(warmer.time, "sleep", lambda seconds: None)

    result = warmer.warm_one("https://example.test/hot", timeout=1, attempts=3)

    assert result.status == 200
    assert result.cache == "HIT"
    assert result.bytes_read == 2
    assert calls["count"] == 3


def test_warm_one_does_not_retry_non_transient_http_errors(monkeypatch):
    warmer = _load_warmer()
    calls = {"count": 0}

    def fake_urlopen(req, timeout):
        calls["count"] += 1
        raise HTTPError(req.full_url, 404, "not found", hdrs=None, fp=None)

    monkeypatch.setattr(warmer, "urlopen", fake_urlopen)

    result = warmer.warm_one("https://example.test/missing", timeout=1, attempts=3)

    assert result.status == 0
    assert "HTTP Error 404" in (result.error or "")
    assert calls["count"] == 1


def test_hot_verification_retries_until_cache_required_routes_are_hot(monkeypatch):
    warmer = _load_warmer()
    calls = {"count": 0}

    def fake_warm_one(url, timeout, attempts=1):
        calls["count"] += 1
        if "overview" in url and calls["count"] <= 2:
            return warmer.WarmResult(url, 200, 120, "MISS", 10)
        return warmer.WarmResult(url, 200, 90, "HIT", 10)

    monkeypatch.setattr(warmer, "warm_one", fake_warm_one)
    monkeypatch.setattr(warmer.time, "sleep", lambda seconds: None)
    targets = [
        warmer.WarmTarget(
            "https://example.test/api/league/speed_test/overview",
            "/api/league/{db}/overview",
            "cache_required",
        ),
        warmer.WarmTarget("https://example.test/speed_test", "/{db}", "cache_expected"),
    ]

    failures, hot_ratio, p95_ms = warmer.verify_hot_targets(
        targets,
        timeout=1,
        concurrency=1,
        min_hot_ratio=0.95,
        max_p95_ms=1500,
        attempts=3,
        delay_seconds=0.01,
        warm_attempts=1,
    )

    assert failures == 0
    assert hot_ratio == 1.0
    assert p95_ms == 90
    assert calls["count"] == 4


def _run_main_with_fake_successful_setup(monkeypatch, warmer, args):
    monkeypatch.setattr(warmer, "verify_fly_commit_visible", lambda *a, **k: None)
    monkeypatch.setattr(warmer, "revalidate", lambda *a, **k: None)
    monkeypatch.setattr(sys, "argv", ["warm_vercel_cache.py", *args])
    return warmer.main()


def test_soft_warm_failures_prevent_false_import_failure(monkeypatch, capsys):
    warmer = _load_warmer()
    targets = [
        warmer.WarmTarget("https://example.test/ok", "/{db}", "cache_expected"),
        warmer.WarmTarget(
            "https://example.test/api/league/speed_test/luck",
            "/api/league/{db}/luck",
            "cache_required",
        ),
    ]

    def fake_warm_one(url, timeout, attempts=1):
        if url.endswith("/ok"):
            return warmer.WarmResult(url, 200, 20, "HIT", 2)
        return warmer.WarmResult(url, 0, 2001, "", 0, "timed out")

    monkeypatch.setattr(warmer, "iter_warm_targets", lambda *a, **k: targets)
    monkeypatch.setattr(warmer, "warm_one", fake_warm_one)

    code = _run_main_with_fake_successful_setup(
        monkeypatch,
        warmer,
        [
            "--db",
            "speed_test",
            "--secret",
            "secret",
            "--strict",
            "--soft-warm-failures",
            "--warm-attempts",
            "2",
        ],
    )

    captured = capsys.readouterr()
    assert code == 0
    assert "Warm-up completed with 1 failure(s)" in captured.err
    assert "committed import remains successful" in captured.err


def test_strict_warm_failures_still_fail_without_soft_mode(monkeypatch):
    warmer = _load_warmer()
    monkeypatch.setattr(
        warmer,
        "iter_warm_targets",
        lambda *a, **k: [warmer.WarmTarget("https://example.test/fail", "/{db}", "cache_expected")],
    )
    monkeypatch.setattr(
        warmer,
        "warm_one",
        lambda url, timeout, attempts=1: warmer.WarmResult(url, 0, 1000, "", 0, "timed out"),
    )

    code = _run_main_with_fake_successful_setup(
        monkeypatch,
        warmer,
        ["--db", "speed_test", "--secret", "secret", "--strict", "--warm-attempts", "2"],
    )

    assert code == 1


def test_required_only_skips_optional_page_warms(monkeypatch):
    warmer = _load_warmer()
    targets = [
        warmer.WarmTarget("https://example.test/speed_test", "/{db}", "cache_expected"),
        warmer.WarmTarget(
            "https://example.test/api/league/speed_test/overview",
            "/api/league/{db}/overview",
            "cache_required",
        ),
    ]
    warmed: list[str] = []
    monkeypatch.setattr(warmer, "iter_warm_targets", lambda *a, **k: targets)

    def fake_warm_one(url, timeout, attempts=1):
        warmed.append(url)
        return warmer.WarmResult(url, 200, 10, "HIT", 2)

    monkeypatch.setattr(warmer, "warm_one", fake_warm_one)

    code = _run_main_with_fake_successful_setup(
        monkeypatch,
        warmer,
        ["--db", "speed_test", "--secret", "secret", "--strict", "--required-only"],
    )

    assert code == 0
    assert warmed == ["https://example.test/api/league/speed_test/overview"]


def test_soft_warm_failures_cover_hot_verification_failures(monkeypatch, capsys):
    warmer = _load_warmer()
    target = warmer.WarmTarget(
        "https://example.test/api/league/speed_test/overview",
        "/api/league/{db}/overview",
        "cache_required",
    )
    monkeypatch.setattr(warmer, "iter_warm_targets", lambda *a, **k: [target])
    monkeypatch.setattr(
        warmer,
        "warm_one",
        lambda url, timeout, attempts=1: warmer.WarmResult(url, 200, 10, "MISS", 2),
    )
    monkeypatch.setattr(
        warmer,
        "verify_hot_targets",
        lambda *a, **k: (1, 0.0, 10_000),
    )

    code = _run_main_with_fake_successful_setup(
        monkeypatch,
        warmer,
        [
            "--db",
            "speed_test",
            "--secret",
            "secret",
            "--strict",
            "--soft-warm-failures",
            "--verify-hot",
        ],
    )

    captured = capsys.readouterr()
    assert code == 0
    assert "Repeat warm had 1 failure(s)" in captured.err
    assert "Cache-required hot ratio 0.000 below target" in captured.err
    assert "Repeat warm p95 10000ms exceeds target" in captured.err
    assert "committed import remains successful" in captured.err


@pytest.mark.parametrize(
    ("argv", "setup_patch", "expected_stderr"),
    [
        (
            ["--db", "speed_test", "--strict", "--soft-warm-failures"],
            "missing_secret",
            "REVALIDATION_SECRET is not set",
        ),
        (
            ["--db", "speed_test", "--secret", "secret", "--strict", "--soft-warm-failures"],
            "fly_visibility",
            "Fly commit visibility check failed",
        ),
        (
            ["--db", "speed_test", "--secret", "secret", "--strict", "--soft-warm-failures"],
            "revalidate",
            "Revalidation failed",
        ),
    ],
)
def test_soft_warm_failures_do_not_hide_setup_failures(
    monkeypatch,
    capsys,
    argv,
    setup_patch,
    expected_stderr,
):
    warmer = _load_warmer()
    monkeypatch.setattr(warmer, "iter_warm_targets", lambda *a, **k: [])

    if setup_patch == "fly_visibility":
        monkeypatch.setattr(
            warmer,
            "verify_fly_commit_visible",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("not visible")),
        )
        monkeypatch.setattr(warmer, "revalidate", lambda *a, **k: None)
    elif setup_patch == "revalidate":
        monkeypatch.setattr(warmer, "verify_fly_commit_visible", lambda *a, **k: None)
        monkeypatch.setattr(
            warmer,
            "revalidate",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("revalidate broke")),
        )
    else:
        monkeypatch.setattr(warmer, "verify_fly_commit_visible", lambda *a, **k: None)
        monkeypatch.setattr(warmer, "revalidate", lambda *a, **k: None)

    monkeypatch.setattr(sys, "argv", ["warm_vercel_cache.py", *argv])

    assert warmer.main() == 1
    assert expected_stderr in capsys.readouterr().err
