from argparse import Namespace
import json
import pytest

from scripts import warm_vercel_cache as cache_warmer


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("mode", ["quick", "full"])
@pytest.mark.parametrize("failure", [None, "revalidate", "warm", "visibility"])
def test_import_cache_result_never_erases_or_repeats_the_confirmed_data_commit(monkeypatch, tmp_path, platform, mode, failure):
    manifest = tmp_path / "delta_publish_manifest.json"
    publication = {"status": "COMMITTED", "executed": True, "bundle_id": "bundle-1", "finalization_status": "complete"}
    manifest.write_text(json.dumps({"db_name": "___fleet" if mode == "quick" else "league",
                                    "db_names": ["league"], "mode": mode, "platform": platform, "bundle_id": "bundle-1",
                                    "tables": [], "publication": publication}))
    output = tmp_path / "output"
    summary = tmp_path / "summary"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    monkeypatch.setattr("sys.argv", ["warm_vercel_cache", "--db", "league", "--secret", "test-only",
                                   "--manifest", str(manifest), "--strict", "--soft-warm-failures",
                                   "--verify-hot", "--jitter-seconds", "0"])
    def visibility(*args):
        if failure == "visibility": raise TimeoutError("read path not verified")
    def revalidate(*args):
        if failure == "revalidate": raise TimeoutError("cache unavailable")
    monkeypatch.setattr(cache_warmer, "verify_fly_commit_visible", visibility)
    monkeypatch.setattr(cache_warmer, "revalidate", revalidate)
    target = cache_warmer.WarmTarget("https://example.invalid/league", "/league", "cache_required")
    monkeypatch.setattr(cache_warmer, "iter_warm_targets", lambda *args: [target])
    monkeypatch.setattr(cache_warmer, "warm_one", lambda *args: cache_warmer.WarmResult(
        target.url, 500 if failure == "warm" else 200, 1, "HIT", 10))
    monkeypatch.setattr(cache_warmer, "verify_hot_targets", lambda *args: (0, 1.0, 1))
    code = cache_warmer.main()
    saved = json.loads(manifest.read_text())
    assert saved["publication"] == publication
    expected = failure is None
    assert saved["cache_verification"] == {"verified": expected, "status": "verified" if expected else "pending"}
    assert f"cache_verified={str(expected).lower()}" in output.read_text()
    assert "data_committed=true" in output.read_text()
    assert "committed_cache_pending" in summary.read_text() if failure else "complete" in summary.read_text()
    assert code == (1 if failure in {"revalidate", "visibility"} else 0)


def test_quick_cache_visibility_checks_only_the_published_seasons(monkeypatch, tmp_path):
    path = tmp_path / "delta_publish_manifest.json"
    path.write_text(json.dumps({"db_name": "___fleet", "db_names": ["league"], "mode": "quick",
        "tables": [{"table": "matchup", "row_count": 12, "scope": {"years": [2026]}}]}))
    def read(sql, timeout):
        # The league has 2,000 old rows in addition to the 12 being published.
        return [{"cnt": 12 if "year IN (2026)" in sql else 2012}]
    monkeypatch.setattr(cache_warmer, "_fly_query", read)
    cache_warmer.verify_fly_commit_visible(str(path), "league", 5)


def test_quick_cache_verification_does_not_compare_preserved_configuration_to_incoming_counts(monkeypatch, tmp_path):
    path = tmp_path / "delta_publish_manifest.json"
    path.write_text(json.dumps({"db_name": "___fleet", "db_names": ["league"], "mode": "quick",
        "tables": [{"table": "franchise_identity_audit", "row_count": 12, "merge_mode": "initialize_missing"}]}))
    monkeypatch.setattr(cache_warmer, "_fly_query", lambda *args: [{"cnt": 120}])
    cache_warmer.verify_fly_commit_visible(str(path), "league", 5)


@pytest.mark.parametrize("has_receipt", [False, True])
def test_cache_reporting_failure_does_not_truncate_the_data_commit_or_repeat_work(monkeypatch, tmp_path, has_receipt):
    path = tmp_path / "delta_publish_manifest.json"
    manifest = {"db_name": "league", "bundle_id": "bundle-1", "tables": []}
    if has_receipt:
        manifest["publication"] = {"status": "COMMITTED", "executed": True,
            "bundle_id": "bundle-1", "finalization_status": "complete"}
    path.write_text(json.dumps(manifest))
    original = path.read_bytes()
    args = Namespace(db="league", manifest=str(path))
    monkeypatch.setattr(cache_warmer, "parse_args", lambda: args)
    monkeypatch.setattr(cache_warmer, "_run_warmer", lambda args: (0, True))
    def failed_report(*args): raise OSError("diagnostic artifact unavailable")
    monkeypatch.setattr(cache_warmer.Path, "replace", failed_report)
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    assert cache_warmer.main() == 0
    assert path.read_bytes() == original
    if not has_receipt:
        assert "completion_status=publication_unconfirmed" in output.read_text()
        assert "cache_verified=false" in output.read_text()


def test_refresh_cache_reporting_does_not_override_its_existing_publication_receipt(monkeypatch, tmp_path):
    monkeypatch.setattr(cache_warmer, "resolve_manifest_path", lambda *args: None)
    monkeypatch.setattr(cache_warmer, "parse_args", lambda: Namespace(db="league", manifest=""))
    monkeypatch.setattr(cache_warmer, "_run_warmer", lambda args: (0, True))
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    assert cache_warmer.main() == 0
    assert "cache_verified=true" in output.read_text()
    assert "data_committed" not in output.read_text()


def test_disabled_p95_budget_does_not_fail_successful_cache_publication(monkeypatch):
    target = cache_warmer.WarmTarget(
        url="https://www.leaguehistory.app/test/overview",
        path="/api/league/test/overview",
        cache_class="cache_required",
    )
    args = Namespace(
        db="test",
        secret="secret",
        site_url="https://www.leaguehistory.app",
        mode="quick",
        manifest="",
        skip_fly_commit_check=False,
        strategy="expire",
        timeout=5,
        concurrency=1,
        warm_attempts=1,
        jitter_seconds=0,
        strict=True,
        soft_warm_failures=False,
        verify_hot=True,
        required_only=True,
        min_hot_ratio=0.95,
        max_p95_ms=0,
        hot_verify_attempts=1,
        hot_verify_delay_seconds=0,
    )
    monkeypatch.setattr(cache_warmer, "parse_args", lambda: args)
    monkeypatch.setattr(cache_warmer, "verify_fly_commit_visible", lambda *_args: None)
    monkeypatch.setattr(cache_warmer, "revalidate", lambda *_args: None)
    monkeypatch.setattr(cache_warmer, "iter_warm_targets", lambda *_args: [target])
    monkeypatch.setattr(
        cache_warmer,
        "warm_one",
        lambda *_args: cache_warmer.WarmResult(target.url, 200, 100, "HIT", 10),
    )
    monkeypatch.setattr(
        cache_warmer,
        "verify_hot_targets",
        lambda *_args: (0, 1.0, 1_587),
    )

    assert cache_warmer.main() == 0
