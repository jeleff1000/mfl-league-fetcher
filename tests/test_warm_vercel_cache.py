from argparse import Namespace

from scripts import warm_vercel_cache as cache_warmer


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
