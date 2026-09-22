#!/usr/bin/env python
"""Revalidate and warm the bounded public Vercel cache surface for one league.

This is intentionally small and conservative: one league tag/path invalidation,
then a handful of hot page/API URLs. It is safe to run after every successful
quick/full import because it never fans out across all leagues.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
import os
from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urljoin
from urllib.request import Request, urlopen


DB_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,62}$")
TABLE_RE = re.compile(r"^[A-Za-z0-9_]+$")


QUICK_WARM_PATHS = (
    "/{db}",
    "/api/league/{db}/overview",
    "/api/league/{db}/features",
    "/api/league/{db}/standings",
    "/api/league/{db}/matchups?meta=1",
    "/api/league/{db}/simulations",
    "/api/league/{db}/players/weekly?meta=1",
)

FULL_WARM_PATHS = QUICK_WARM_PATHS + (
    "/{db}/managers",
    "/{db}/players",
    "/{db}/draft",
    "/{db}/team-stats",
    "/{db}/transactions",
    "/{db}/luck",
    "/api/league/{db}/team-stats",
    "/api/league/{db}/transactions",
    "/api/league/{db}/luck",
    "/api/league/{db}/luck/h2h",
    "/api/league/{db}/luck/schedule-swap",
    "/api/league/{db}/draft?group=slim&sort=draft_score&dir=desc&limit=200",
    "/api/league/{db}/draft?view=manager-season",
    "/api/league/{db}/draft?view=manager-career",
    "/api/league/{db}/draft?view=player-career",
)

HOT_CACHE_HEADERS = {"HIT", "STALE", "REVALIDATED"}
TRANSIENT_HTTP_CODES = {429, 500, 502, 503, 504}

ROUTE_CACHE_CLASS = {
    "/{db}": "cache_expected",
    "/{db}/managers": "cache_expected",
    "/{db}/players": "cache_expected",
    "/{db}/draft": "cache_expected",
    "/{db}/team-stats": "cache_expected",
    "/{db}/transactions": "cache_expected",
    "/{db}/luck": "cache_expected",
    "/api/league/{db}/overview": "cache_required",
    "/api/league/{db}/features": "cache_required",
    "/api/league/{db}/standings": "cache_required",
    "/api/league/{db}/matchups?meta=1": "cache_required",
    "/api/league/{db}/simulations": "cache_required",
    "/api/league/{db}/players/weekly?meta=1": "cache_required",
    "/api/league/{db}/team-stats": "cache_required",
    "/api/league/{db}/transactions": "cache_required",
    "/api/league/{db}/luck": "cache_required",
    "/api/league/{db}/luck/h2h": "cache_required",
    "/api/league/{db}/luck/schedule-swap": "cache_required",
    "/api/league/{db}/draft?group=slim&sort=draft_score&dir=desc&limit=200": "cache_required",
    "/api/league/{db}/draft?view=manager-season": "cache_required",
    "/api/league/{db}/draft?view=manager-career": "cache_required",
    "/api/league/{db}/draft?view=player-career": "cache_required",
}

TABLE_TO_WARM_PATHS = {
    "all_play": ("/{db}/luck", "/api/league/{db}/luck", "/api/league/{db}/luck/h2h"),
    "draft": ("/{db}/draft", "/api/league/{db}/draft?group=slim&sort=draft_score&dir=desc&limit=200"),
    "draft_manager_career": ("/{db}/draft", "/api/league/{db}/draft?view=manager-career"),
    "draft_manager_season": ("/{db}/draft", "/api/league/{db}/draft?view=manager-season"),
    "draft_player_career": ("/{db}/draft", "/api/league/{db}/draft?view=player-career"),
    "h2h_season": ("/{db}/luck", "/api/league/{db}/luck", "/api/league/{db}/luck/h2h"),
    "homepage_current_standings": ("/{db}", "/api/league/{db}/overview", "/api/league/{db}/standings"),
    "homepage_league_summary": ("/{db}", "/api/league/{db}/overview"),
    "homepage_manager_profiles": ("/{db}", "/{db}/managers"),
    "homepage_manager_rankings": ("/{db}", "/{db}/managers"),
    "homepage_top_rivalries": ("/{db}", "/{db}/managers"),
    "keeper_config": ("/api/league/{db}/features",),
    "league_context": ("/{db}", "/api/league/{db}/overview", "/api/league/{db}/features"),
    "league_rules": ("/api/league/{db}/features",),
    "league_settings": (
        "/{db}",
        "/api/league/{db}/overview",
        "/api/league/{db}/features",
        "/api/league/{db}/standings",
    ),
    "manager_overrides": ("/{db}/managers",),
    "matchup": (
        "/{db}",
        "/api/league/{db}/standings",
        "/api/league/{db}/matchups?meta=1",
        "/api/league/{db}/simulations",
        "/{db}/team-stats",
    ),
    "matchup_career": ("/{db}/team-stats", "/api/league/{db}/team-stats"),
    "matchup_h2h_career": ("/{db}/team-stats", "/api/league/{db}/team-stats"),
    "matchup_h2h_season": ("/{db}/team-stats", "/api/league/{db}/team-stats"),
    "matchup_season": (
        "/{db}/team-stats",
        "/api/league/{db}/team-stats",
        "/api/league/{db}/simulations",
    ),
    "player_fantasy": ("/{db}/players", "/api/league/{db}/players/weekly?meta=1", "/{db}/team-stats"),
    "player_fantasy_career": ("/{db}/players",),
    "player_fantasy_career_all": ("/{db}/players",),
    "player_fantasy_season": ("/{db}/players",),
    "player_fantasy_season_all": ("/{db}/players",),
    "schedule": ("/api/league/{db}/standings", "/api/league/{db}/matchups?meta=1"),
    "schedule_swap": ("/{db}/luck", "/api/league/{db}/luck", "/api/league/{db}/luck/schedule-swap"),
    "schedule_swap_season": ("/{db}/luck", "/api/league/{db}/luck", "/api/league/{db}/luck/schedule-swap"),
    "standings_by_year": ("/{db}", "/api/league/{db}/standings"),
    "standings_config": ("/api/league/{db}/standings",),
    "transaction_manager_career": ("/{db}/transactions", "/api/league/{db}/transactions"),
    "transaction_manager_season": ("/{db}/transactions", "/api/league/{db}/transactions"),
    "transaction_player_career": ("/{db}/transactions", "/api/league/{db}/transactions"),
    "transaction_report_card": ("/{db}/transactions", "/api/league/{db}/transactions"),
    "transactions": ("/{db}/transactions", "/api/league/{db}/transactions"),
}


@dataclass(frozen=True)
class WarmResult:
    url: str
    status: int
    ms: int
    cache: str
    bytes_read: int
    error: str | None = None


@dataclass(frozen=True)
class WarmTarget:
    url: str
    path: str
    cache_class: str


def build_url(site_url: str, path: str) -> str:
    return urljoin(site_url.rstrip("/") + "/", path.lstrip("/"))


def revalidate(site_url: str, db: str, secret: str, strategy: str, timeout: int, attempts: int = 4) -> None:
    params = urlencode({"db": db, "secret": secret, "strategy": strategy})
    url = build_url(site_url, f"/api/revalidate?{params}")
    for attempt in range(1, attempts + 1):
        req = Request(url, method="POST")
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                if resp.status >= 400:
                    raise RuntimeError(f"revalidate failed ({resp.status}): {body}")
                payload = json.loads(body)
                if not payload.get("revalidated"):
                    raise RuntimeError(f"revalidate did not confirm success: {body}")
                return
        except HTTPError as exc:
            if exc.code in (307, 308):
                location = exc.headers.get("Location")
                if not location:
                    raise
                url = urljoin(url, location)
                continue
            if not _is_transient_error(exc) or attempt >= attempts:
                raise
            delay = _retry_delay(attempt)
            print(
                f"Revalidation attempt {attempt}/{attempts} failed transiently: {exc}. " f"Retrying in {delay:.1f}s...",
                file=sys.stderr,
            )
            time.sleep(delay)
        except Exception as exc:  # noqa: BLE001 - retry only known transient transport failures
            if not _is_transient_error(exc) or attempt >= attempts:
                raise
            delay = _retry_delay(attempt)
            print(
                f"Revalidation attempt {attempt}/{attempts} failed transiently: {exc}. " f"Retrying in {delay:.1f}s...",
                file=sys.stderr,
            )
            time.sleep(delay)
    raise RuntimeError("revalidate redirected or retried too many times")


def normalize_secret(value: str | None) -> str:
    """GitHub/Vercel secrets can accidentally gain surrounding whitespace."""
    return (value or "").strip()


def _retry_delay(attempt: int) -> float:
    return min(10.0, 1.0 * (2 ** max(0, attempt - 1))) + random.uniform(0.0, 0.5)


def _is_transient_error(exc: BaseException) -> bool:
    if isinstance(exc, HTTPError):
        return exc.code in TRANSIENT_HTTP_CODES
    return isinstance(exc, (TimeoutError, URLError, ConnectionError))


def warm_one(url: str, timeout: int, attempts: int = 3) -> WarmResult:
    start = time.perf_counter()
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        req = Request(url, headers={"User-Agent": "leaguehistory-cache-warmer/1.0"})
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read()
                ms = int((time.perf_counter() - start) * 1000)
                cache = resp.headers.get("x-vercel-cache") or resp.headers.get("x-nextjs-cache") or ""
                return WarmResult(url, resp.status, ms, cache, len(body))
        except Exception as exc:  # noqa: BLE001 - report final failure and continue warming others
            last_exc = exc
            if not _is_transient_error(exc) or attempt >= attempts:
                ms = int((time.perf_counter() - start) * 1000)
                return WarmResult(url, 0, ms, "", 0, str(exc))
            delay = _retry_delay(attempt)
            print(
                f"Warm attempt {attempt}/{attempts} failed transiently for {url}: {exc}. "
                f"Retrying in {delay:.1f}s...",
                file=sys.stderr,
            )
            time.sleep(delay)
    ms = int((time.perf_counter() - start) * 1000)
    return WarmResult(url, 0, ms, "", 0, str(last_exc))


def _discover_manifest_path(db: str) -> str | None:
    candidates = sorted(Path.cwd().glob("fantasy_football_data/**/delta_publish_manifest.json"))
    for candidate in candidates:
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        if payload.get("db_name") == db:
            return str(candidate)
    return None


def resolve_manifest_path(db: str, manifest_path: str | None) -> str | None:
    if manifest_path:
        return manifest_path if Path(manifest_path).exists() else None
    return _discover_manifest_path(db)


def _has_post_upload_merge_source(manifest_path: str | None) -> bool:
    """Return whether the import context will copy merge_source rows after upload."""
    candidates: list[Path] = []
    if manifest_path:
        manifest_dir = Path(manifest_path).resolve().parent
        candidates.extend(
            [
                manifest_dir / "sleeper_context.json",
                manifest_dir / "espn_context.json",
                manifest_dir / "league_context.json",
                manifest_dir.parent / "sleeper_context.json",
                manifest_dir.parent / "espn_context.json",
                manifest_dir.parent / "league_context.json",
            ]
        )
    data_dir = os.environ.get("DATA_DIR")
    if data_dir:
        root = Path(data_dir).resolve()
        candidates.extend(
            [
                root / "sleeper_context.json",
                root / "espn_context.json",
                root / "league_context.json",
            ]
        )

    seen: set[Path] = set()
    for candidate in candidates:
        if candidate in seen:
            continue
        seen.add(candidate)
        if not candidate.exists():
            continue
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        merge_source = payload.get("merge_source") if isinstance(payload, dict) else None
        if isinstance(merge_source, dict) and merge_source.get("source_db"):
            return True
        merge_sources = payload.get("merge_sources") if isinstance(payload, dict) else None
        if isinstance(merge_sources, list) and any(
            isinstance(source, dict) and source.get("source_db") for source in merge_sources
        ):
            return True
    return False


def _paths_from_manifest(manifest_path: str | None, mode: str, db: str) -> tuple[str, ...]:
    manifest_path = resolve_manifest_path(db, manifest_path)
    if not manifest_path or not Path(manifest_path).exists():
        return FULL_WARM_PATHS if mode == "full" else QUICK_WARM_PATHS
    payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    touched_tables = [entry["table"] for entry in payload.get("tables", []) if entry.get("included", True)]
    paths: list[str] = list(QUICK_WARM_PATHS)
    for table in touched_tables:
        paths.extend(TABLE_TO_WARM_PATHS.get(table, ()))
    if mode == "full":
        paths.extend(FULL_WARM_PATHS)
    # Preserve insertion order while removing duplicates.
    return tuple(dict.fromkeys(paths))


def _fly_query(sql: str, timeout: int, attempts: int = 6) -> list[dict]:
    server_url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    read_token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not server_url or not read_token:
        raise RuntimeError("DATABASE_SERVER_URL/DATABASE_READ_TOKEN not set")
    payload = json.dumps({"sql": sql, "database": "___leagues"}).encode("utf-8")
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        req = Request(
            f"{server_url}/query",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {read_token}",
            },
            method="POST",
        )
        try:
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8", errors="replace")
                if resp.status >= 400:
                    raise RuntimeError(f"Fly query failed ({resp.status}): {body}")
                return json.loads(body)
        except Exception as exc:  # noqa: BLE001 - retry only known transient transport failures
            last_exc = exc
            if not _is_transient_error(exc) or attempt >= attempts:
                raise
            delay = _retry_delay(attempt)
            print(
                f"Fly read check attempt {attempt}/{attempts} failed transiently: {exc}. "
                f"Retrying in {delay:.1f}s...",
                file=sys.stderr,
            )
            time.sleep(delay)
    raise RuntimeError(f"Fly query failed after {attempts} attempts: {last_exc}")


def verify_fly_commit_visible(manifest_path: str | None, db: str, timeout: int) -> None:
    manifest_path = resolve_manifest_path(db, manifest_path)
    if not manifest_path:
        return
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    if manifest.get("db_name") != db:
        raise RuntimeError(f"Manifest db_name {manifest.get('db_name')!r} does not match {db!r}")

    safe_db = db.replace("'", "''")
    bundle_id = manifest.get("bundle_id")
    if bundle_id:
        escaped_bundle = str(bundle_id).replace("'", "''")
        try:
            state_rows = _fly_query(
                f"""
                SELECT status
                FROM merge_admin.league_delta_merge_state
                WHERE db_name = '{safe_db}' AND bundle_id = '{escaped_bundle}'
                ORDER BY updated_at DESC
                LIMIT 1
                """,
                timeout,
            )
        except Exception:
            state_rows = []
        if state_rows and state_rows[0].get("status") != "COMMITTED":
            raise RuntimeError(f"Fly delta state is not committed yet: {state_rows[0]}")

    allow_post_upload_growth = _has_post_upload_merge_source(manifest_path)
    for entry in manifest.get("tables", [])[:3]:
        table = str(entry.get("table") or "")
        if not TABLE_RE.fullmatch(table):
            raise RuntimeError(f"Unsafe table name in manifest: {table!r}")
        expected = int(entry.get("row_count") or 0)
        rows = _fly_query(
            f"SELECT COUNT(*) AS cnt FROM public.\"{table}\" WHERE db_name = '{safe_db}'",
            timeout,
        )
        actual = int(rows[0].get("cnt") or 0) if rows else 0
        if actual != expected:
            if allow_post_upload_growth and actual > expected:
                print(
                    f"Fly read count for {table} is {actual} after post-upload merge_source "
                    f"finalization (delta manifest had {expected})"
                )
                continue
            raise RuntimeError(f"Fly read count mismatch for {table}: expected {expected}, saw {actual}")


def iter_warm_targets(site_url: str, db: str, mode: str, manifest_path: str | None = None) -> Iterable[WarmTarget]:
    if mode == "none":
        return ()
    paths = _paths_from_manifest(manifest_path, mode, db)
    safe_db = quote(db, safe="")
    return (
        WarmTarget(
            url=build_url(site_url, path.format(db=safe_db)),
            path=path,
            cache_class=ROUTE_CACHE_CLASS.get(path, "cache_expected"),
        )
        for path in paths
    )


def iter_warm_urls(site_url: str, db: str, mode: str) -> Iterable[str]:
    return (target.url for target in iter_warm_targets(site_url, db, mode))


def percentile_95(values: list[int]) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, int(round((len(ordered) - 1) * 0.95)))
    return ordered[idx]


def hot_verification_metrics(hot_results: list[tuple[WarmTarget, WarmResult]]) -> tuple[int, float, int]:
    repeat_failures = sum(1 for _, result in hot_results if result.status != 200 or result.error)
    cache_required = [
        result
        for target, result in hot_results
        if target.cache_class == "cache_required" and result.status == 200 and not result.error
    ]
    hot_required = [result for result in cache_required if (result.cache or "").upper() in HOT_CACHE_HEADERS]
    hot_ratio = (len(hot_required) / len(cache_required)) if cache_required else 1.0
    p95_ms = percentile_95([result.ms for _, result in hot_results if result.status == 200 and not result.error])
    return repeat_failures, hot_ratio, p95_ms


def verify_hot_targets(
    targets: list[WarmTarget],
    timeout: int,
    concurrency: int,
    min_hot_ratio: float,
    max_p95_ms: int,
    attempts: int,
    delay_seconds: float,
    warm_attempts: int,
) -> tuple[int, float, int]:
    best_metrics: tuple[int, float, int] = (len(targets) or 1, 0.0, 10**9)
    attempts = max(1, attempts)
    for attempt in range(1, attempts + 1):
        label = (
            "Verifying repeated cache hits"
            if attempt == 1
            else f"Verifying repeated cache hits attempt {attempt}/{attempts}"
        )
        print(label)
        hot_results: list[tuple[WarmTarget, WarmResult]] = []
        with ThreadPoolExecutor(max_workers=max(1, concurrency)) as pool:
            futures = {pool.submit(warm_one, target.url, timeout, warm_attempts): target for target in targets}
            for future in as_completed(futures):
                target = futures[future]
                result = future.result()
                hot_results.append((target, result))
                cache = f" cache={result.cache}" if result.cache else ""
                err = f" error={result.error}" if result.error else ""
                marker = "OK" if result.status == 200 and not result.error else "FAIL"
                print(
                    f"{marker} repeat {result.status} {result.ms}ms {result.bytes_read}B "
                    f"class={target.cache_class}{cache} {result.url}{err}"
                )

        metrics = hot_verification_metrics(hot_results)
        repeat_failures, hot_ratio, p95_ms = metrics
        print(
            f"Hot verification attempt {attempt}/{attempts}: "
            f"required_hot_ratio={hot_ratio:.3f} p95={p95_ms}ms failures={repeat_failures}"
        )
        if repeat_failures == 0 and hot_ratio >= min_hot_ratio and (p95_ms <= max_p95_ms or max_p95_ms <= 0):
            return metrics
        if _is_better_hot_metrics(metrics, best_metrics):
            best_metrics = metrics
        if attempt < attempts and delay_seconds > 0:
            time.sleep(delay_seconds)
    return best_metrics


def _is_better_hot_metrics(candidate: tuple[int, float, int], current: tuple[int, float, int]) -> bool:
    candidate_failures, candidate_ratio, candidate_p95 = candidate
    current_failures, current_ratio, current_p95 = current
    if candidate_failures != current_failures:
        return candidate_failures < current_failures
    if candidate_ratio != current_ratio:
        return candidate_ratio > current_ratio
    return candidate_p95 < current_p95


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="League db_name to revalidate/warm")
    parser.add_argument("--secret", default="", help="REVALIDATION_SECRET")
    parser.add_argument("--site-url", default="https://leaguehistory.app")
    parser.add_argument("--mode", choices=("none", "quick", "full"), default="quick")
    parser.add_argument("--manifest", default="", help="Optional delta_publish_manifest.json for touched-table warming")
    parser.add_argument(
        "--skip-fly-commit-check",
        action="store_true",
        help="Skip verifying the committed delta is visible through Fly reads before warming Vercel.",
    )
    parser.add_argument(
        "--strategy",
        choices=("swr", "expire"),
        default="expire",
        help="Use expire after imports so the warmer refreshes caches before users arrive.",
    )
    parser.add_argument("--timeout", type=int, default=25)
    parser.add_argument("--concurrency", type=int, default=6)
    parser.add_argument(
        "--warm-attempts",
        type=int,
        default=6,
        help="Per-URL attempts for warm and repeat-hit requests. Retries only transient failures.",
    )
    parser.add_argument(
        "--jitter-seconds",
        type=int,
        default=0,
        help="Sleep 0..N seconds before revalidation to spread concurrent worker cache refreshes.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return non-zero if revalidation or any warm request fails.",
    )
    parser.add_argument(
        "--soft-warm-failures",
        action="store_true",
        help=(
            "Keep strict revalidation/read-visibility checks, but report warm/verify "
            "failures as warnings after the import has already committed."
        ),
    )
    parser.add_argument(
        "--verify-hot",
        action="store_true",
        help="Repeat the warm URLs and verify cache-required routes are hot enough.",
    )
    parser.add_argument(
        "--required-only",
        action="store_true",
        help="Warm only cache-required API routes, skipping optional rendered pages.",
    )
    parser.add_argument("--min-hot-ratio", type=float, default=0.95)
    parser.add_argument("--max-p95-ms", type=int, default=1500)
    parser.add_argument("--hot-verify-attempts", type=int, default=6)
    parser.add_argument("--hot-verify-delay-seconds", type=float, default=1.5)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db = args.db.strip()
    if not DB_NAME_RE.match(db):
        print(f"Invalid db name: {db}", file=sys.stderr)
        return 2

    secret = normalize_secret(args.secret)
    if not secret:
        print("REVALIDATION_SECRET is not set; cannot revalidate Vercel cache", file=sys.stderr)
        return 1 if args.strict else 0

    if args.jitter_seconds > 0:
        delay = random.randint(0, args.jitter_seconds)
        if delay > 0:
            print(f"Sleeping {delay}s before cache refresh jitter window={args.jitter_seconds}s")
            time.sleep(delay)

    site_url = args.site_url.rstrip("/")
    if not args.skip_fly_commit_check:
        try:
            verify_fly_commit_visible(args.manifest or None, db, args.timeout)
            print("Fly committed bundle is visible to read path")
        except Exception as exc:
            print(f"Fly commit visibility check failed: {exc}", file=sys.stderr)
            return 1 if args.strict else 0

    print(f"Revalidating {db} on {site_url} with strategy={args.strategy}")
    try:
        revalidate(site_url, db, secret, args.strategy, args.timeout)
    except Exception as exc:  # noqa: BLE001 - cache warm-up is best-effort by default
        print(f"Revalidation failed: {exc}", file=sys.stderr)
        return 1 if args.strict else 0

    targets = list(iter_warm_targets(site_url, db, args.mode, args.manifest or None))
    if args.required_only:
        targets = [target for target in targets if target.cache_class == "cache_required"]
    if not targets:
        print("Warm mode is none; revalidation complete")
        return 0

    print(f"Warming {len(targets)} URL(s) with concurrency={args.concurrency}")
    failures = 0
    with ThreadPoolExecutor(max_workers=max(1, args.concurrency)) as pool:
        futures = {pool.submit(warm_one, target.url, args.timeout, args.warm_attempts): target for target in targets}
        for future in as_completed(futures):
            target = futures[future]
            result = future.result()
            marker = "OK" if result.status == 200 and not result.error else "FAIL"
            if marker == "FAIL":
                failures += 1
            cache = f" cache={result.cache}" if result.cache else ""
            err = f" error={result.error}" if result.error else ""
            print(
                f"{marker} {result.status} {result.ms}ms {result.bytes_read}B "
                f"class={target.cache_class}{cache} {result.url}{err}"
            )

    if args.verify_hot and not failures:
        repeat_failures, hot_ratio, p95_ms = verify_hot_targets(
            targets,
            args.timeout,
            args.concurrency,
            args.min_hot_ratio,
            args.max_p95_ms,
            args.hot_verify_attempts,
            args.hot_verify_delay_seconds,
            args.warm_attempts,
        )
        if repeat_failures:
            print(f"Repeat warm had {repeat_failures} failure(s)", file=sys.stderr)
            failures += repeat_failures
        if hot_ratio < args.min_hot_ratio:
            print(
                f"Cache-required hot ratio {hot_ratio:.3f} below target {args.min_hot_ratio:.3f}",
                file=sys.stderr,
            )
            failures += 1
        if args.max_p95_ms > 0 and p95_ms > args.max_p95_ms:
            print(f"Repeat warm p95 {p95_ms}ms exceeds target {args.max_p95_ms}ms", file=sys.stderr)
            failures += 1

    if failures:
        print(f"Warm-up completed with {failures} failure(s)", file=sys.stderr)
        if args.soft_warm_failures:
            print("Warm-up failures are warnings; committed import remains successful", file=sys.stderr)
            return 0
        return 1 if args.strict else 0
    print("Warm-up complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
