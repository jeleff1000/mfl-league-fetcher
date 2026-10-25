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
import subprocess
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


def _manifest_matches_league(payload: dict, db: str) -> bool:
    return payload.get("db_name") == db or (
        payload.get("db_name") == "___fleet" and payload.get("db_names") == [db]
    )


def _discover_manifest_path(db: str) -> str | None:
    candidates = sorted(Path.cwd().glob("fantasy_football_data/**/delta_publish_manifest.json"))
    for candidate in candidates:
        try:
            payload = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception:
            continue
        if _manifest_matches_league(payload, db):
            return str(candidate)
    return None


def resolve_manifest_path(db: str, manifest_path: str | None) -> str | None:
    if manifest_path:
        return manifest_path if Path(manifest_path).exists() else None
    return _discover_manifest_path(db)


def _has_post_upload_merge_source(manifest_path: str | None) -> bool:
    """Return whether the import context will copy merge_source rows after upload."""
    if manifest_path:
        payload = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
        plan = (payload.get("publication") or {}).get("finalization_plan") or {}
        if plan.get("merge_source") and (plan.get("context") or {}).get("merge_sources"):
            return True
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


def _fly_query(sql: str, timeout: int, attempts: int = 6, *, write: bool = False) -> list[dict]:
    server_url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    read_token = os.environ.get("DATABASE_ADMIN_TOKEN" if write else "DATABASE_READ_TOKEN", "")
    if not server_url or not read_token:
        raise RuntimeError("DATABASE_SERVER_URL/DATABASE_READ_TOKEN not set")
    payload = json.dumps({"sql": sql, "database": "___leagues"}).encode("utf-8")
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        req = Request(
            f"{server_url}/{'query-rw' if write else 'query'}",
            data=payload,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {read_token}",
                **({"fly-force-instance-id": os.environ["FLY_PRIMARY_MACHINE_ID"]}
                   if write and os.environ.get("FLY_PRIMARY_MACHINE_ID") else {}),
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
    if not _manifest_matches_league(manifest, db):
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
    # Initialization-only user configuration is preserved by the server. Its
    # existing row count is not an equality witness for the incoming payload.
    replaced = [entry for entry in manifest.get("tables", []) if entry.get("merge_mode") != "initialize_missing"]
    for entry in replaced[:3]:
        table = str(entry.get("table") or "")
        if not TABLE_RE.fullmatch(table):
            raise RuntimeError(f"Unsafe table name in manifest: {table!r}")
        expected = int(entry.get("row_count") or 0)
        scope = entry.get("scope") or {}
        years = scope.get("years") or ([scope["year"]] if "year" in scope else [])
        if any(type(year) is not int or not 1900 <= year <= 2200 for year in years):
            raise RuntimeError(f"Invalid publication year scope for {table}")
        year_filter = " AND year IN (" + ",".join(str(year) for year in years) + ")" if years else ""
        rows = _fly_query(
            f"SELECT COUNT(*) AS cnt FROM public.\"{table}\" WHERE db_name = '{safe_db}'" + year_filter,
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
    parser.add_argument("--publication-run-id", default="", help="Resume only completion of this existing committed import")
    parser.add_argument("--validate", action="store_true", help="Run the existing league-scoped diagnostic validator independently of cache refresh")
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


def _run_warmer(args) -> tuple[int, bool]:
    db = args.db.strip()
    if not DB_NAME_RE.match(db):
        print(f"Invalid db name: {db}", file=sys.stderr)
        return 2, False

    secret = normalize_secret(args.secret)
    if not secret:
        print("REVALIDATION_SECRET is not set; cannot revalidate Vercel cache", file=sys.stderr)
        return (1 if args.strict else 0), False

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
            return (1 if args.strict else 0), False

    print(f"Revalidating {db} on {site_url} with strategy={args.strategy}")
    try:
        revalidate(site_url, db, secret, args.strategy, args.timeout)
    except Exception as exc:  # noqa: BLE001 - cache warm-up is best-effort by default
        print(f"Revalidation failed: {exc}", file=sys.stderr)
        return (1 if args.strict else 0), False

    targets = list(iter_warm_targets(site_url, db, args.mode, args.manifest or None))
    if args.required_only:
        targets = [target for target in targets if target.cache_class == "cache_required"]
    if not targets:
        print("Warm mode is none; revalidation complete")
        return 0, False

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
            print("Data publication is unchanged; cache verification is pending", file=sys.stderr)
            return 0, False
        return (1 if args.strict else 0), False
    print("Warm-up complete")
    return 0, bool(args.verify_hot)


def read_committed_manifest(manifest_path: str | None, db: str) -> dict | None:
    """Read the existing publication receipt; missing evidence is never success."""
    if not manifest_path or not Path(manifest_path).is_file():
        return None
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    publication = manifest.get("publication") or {}
    if (
        _manifest_matches_league(manifest, db)
        and publication.get("status") == "COMMITTED"
        and publication.get("executed") is True
        and publication.get("bundle_id") == manifest.get("bundle_id")
    ):
        return manifest
    return None


def _literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _publication_generation(manifest: dict, db: str) -> int:
    base = manifest.get("base_generation", (manifest.get("league_generations") or {}).get(db))
    if type(base) is not int or base < 0:
        raise RuntimeError("Publication has no valid generation fence")
    return base + 1


def load_committed_publication(db: str, run_id: str, generation: int | None = None, *, mode: str | None = None) -> dict:
    """Recover a few KB of the existing receipt, never league tables or provider data."""
    if not DB_NAME_RE.fullmatch(db) or not str(run_id).isdigit():
        raise ValueError("Invalid publication recovery scope")
    rows = _fly_query(
        "SELECT db_name,bundle_id,bundle_hash,status,import_run_id,manifest_json,result_json "
        "FROM merge_admin.league_delta_merge_state "
        f"WHERE import_run_id = {_literal(run_id)} AND status = 'COMMITTED' "
        f"AND (db_name = {_literal(db)} OR (db_name = '___fleet' "
        f"AND json_extract_string(manifest_json,'$.db_names[0]') = {_literal(db)} "
        "AND json_array_length(json_extract(manifest_json,'$.db_names')) = 1)) "
        "ORDER BY updated_at DESC LIMIT 2", 8, 3,
    )
    for row in rows:
        manifest = json.loads(row["manifest_json"])
        result = json.loads(row["result_json"])
        completion = result.get("import_completion") or {}
        publication = completion.get("publication") or {}
        receipt_mode = manifest.get("import_mode") or publication.get("import_mode") or manifest.get("mode")
        if not _manifest_matches_league(manifest, db) or (mode and receipt_mode != mode):
            continue
        if (row.get("status") != "COMMITTED" or result.get("status") != "COMMITTED"
            or str(row.get("import_run_id")) != str(run_id)
            or str(manifest.get("import_run_id")) != str(run_id)
            or not row.get("bundle_id") or not row.get("bundle_hash")
            or any(item.get("bundle_id") != row["bundle_id"] or item.get("bundle_hash") != row["bundle_hash"]
                   for item in (manifest, result))):
            raise RuntimeError("Publication recovery receipt identity does not match")
        if generation is None:
            generations = _fly_query(
                "SELECT generation FROM merge_admin.league_publish_generations "
                f"WHERE db_name = {_literal(db)}", 8, 3,
            )
            if len(generations) != 1:
                raise RuntimeError("Publication recovery generation is unavailable")
            generation = generations[0]["generation"]
        if generation != _publication_generation(manifest, db):
            raise RuntimeError("Publication was superseded; do not repeat its data upload")
        manifest.update({key: completion[key] for key in ("publication", "cache_verification", "validation") if key in completion})
        manifest.setdefault("publication", {"status": "COMMITTED", "executed": True,
            "bundle_id": row["bundle_id"], "finalization_status": "pending"})
        # The fleet lane canonicalizes the client bundle. Keep both identities:
        # local cache routing uses this restored manifest; status checks use Fly's receipt.
        manifest["publication"].update(bundle_id=manifest["bundle_id"], state_db_name=row["db_name"],
            durable_bundle_id=row["bundle_id"], durable_bundle_hash=row["bundle_hash"])
        if receipt_mode:
            manifest["import_mode"] = receipt_mode
        return manifest
    raise RuntimeError("No matching committed import receipt; data upload was not attempted")


def _persist_completion(manifest: dict, db: str) -> None:
    """Checkpoint only metadata on the existing fenced commit row; no schema or league-data write."""
    if not os.environ.get("GITHUB_ACTIONS") or not os.environ.get("DATABASE_ADMIN_TOKEN") or not manifest.get("bundle_id"):
        return
    completion = {key: manifest[key] for key in ("publication", "cache_verification", "validation") if key in manifest}
    patch = json.dumps({"import_completion": completion}, sort_keys=True)
    publication = manifest.get("publication") or {}
    state_db = publication.get("state_db_name") or manifest["db_name"]
    bundle_id = publication.get("durable_bundle_id") or manifest["bundle_id"]
    bundle_hash = publication.get("durable_bundle_hash") or manifest["bundle_hash"]
    revision = publication.get("completion_revision", 0)
    if type(revision) is not int or revision < 0:
        raise ValueError("Invalid completion revision")
    saved_revision = "COALESCE(TRY_CAST(json_extract_string(result_json,'$.import_completion.publication.completion_revision') AS BIGINT),0)"
    guards = []
    for path, incoming, finished in (
        ("publication.finalization_status", publication.get("finalization_status"), ("complete",)),
        ("cache_verification.verified", str((manifest.get("cache_verification") or {}).get("verified", False)).lower(), ("true",)),
        ("validation.status", (manifest.get("validation") or {}).get("status"), ("passed", "warnings")),
    ):
        if incoming not in finished:
            values = ",".join(_literal(value) for value in finished)
            guards.append(f"COALESCE(json_extract_string(result_json,'$.import_completion.{path}'),'') NOT IN ({values})")
    monotonic = " AND ".join(guards) or "TRUE"
    rows = _fly_query(
        "UPDATE merge_admin.league_delta_merge_state "
        f"SET result_json = CAST(json_merge_patch(COALESCE(result_json,'{{}}'), {_literal(patch)}) AS VARCHAR) "
        f"WHERE db_name = {_literal(state_db)} AND bundle_id = {_literal(bundle_id)} "
        f"AND bundle_hash = {_literal(bundle_hash)} AND status = 'COMMITTED' "
        f"AND ({saved_revision} < {revision} OR ({saved_revision} = {revision} AND {monotonic})) "
        "AND EXISTS (SELECT 1 FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {_literal(db)} AND generation = {_publication_generation(manifest, db)}) "
        "RETURNING bundle_id", 5, 1, write=True,
    )
    if len(rows) != 1 or rows[0].get("bundle_id") != bundle_id:
        raise RuntimeError("Completion checkpoint lost its publication fence")


def record_validation_result(manifest_path: str, db: str, status: str) -> None:
    manifest = read_committed_manifest(manifest_path, db)
    if manifest is None:
        raise RuntimeError("Validation finalization has no confirmed publication")
    manifest["validation"] = {"status": status}
    path = Path(manifest_path)
    pending = path.with_suffix(".json.pending")
    pending.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
    pending.replace(path)
    try:
        _persist_completion(manifest, db)
    except Exception as exc:
        print(f"::warning::Verification result checkpoint pending ({type(exc).__name__})", file=sys.stderr)
    print(f"Publication COMMITTED; verification {status}", flush=True)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as handle:
            handle.write(f"\nPublication: `COMMITTED`; verification: `{status}`.\n")


def validate_publication(manifest_path: str, db: str) -> None:
    manifest = read_committed_manifest(manifest_path, db)
    if manifest is None:
        raise RuntimeError("Cannot finalize verification without a committed publication")
    if (manifest.get("validation") or {}).get("status") in {"passed", "warnings"}:
        print("Verification already completed for this publication; skipping")
        return
    status = "pending"
    try:
        result = subprocess.run(
            [sys.executable, "-m", "multi_league.validation_v2", "--db", db, "--ci", "-v"],
            cwd=Path(__file__).resolve().parents[1] / "fantasy_football_data_scripts",
            timeout=40, capture_output=True, text=True, check=False,
        )
        report = (result.stdout or "") + (result.stderr or "")
        print(report[-65536:], flush=True)
        counts = re.search(r"Checks: \d+ visible \((\d+) passed, (\d+) failed, (\d+) errored, (\d+) transient\)", report)
        if result.returncode and "CI FAILURE:" in report:
            status = "failed"
        elif result.returncode == 0 and counts and not any(int(counts[i]) for i in (3, 4)):
            status = "warnings" if int(counts[2]) else "passed"
    except Exception as exc:
        print(f"::warning::Data committed; verification pending ({type(exc).__name__})", file=sys.stderr)
    record_validation_result(manifest_path, db, status)


def _record_cache_result(args, verified: bool) -> None:
    """Report cache completion separately from an already-confirmed data write."""
    manifest_path = resolve_manifest_path(args.db.strip(), args.manifest or None)
    manifest = None
    if manifest_path:
        manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    publication = (manifest or {}).get("publication") or {}
    committed = bool(
        manifest and _manifest_matches_league(manifest, args.db.strip())
        and publication.get("status") == "COMMITTED"
        and publication.get("executed") is True
        and publication.get("bundle_id") == manifest.get("bundle_id")
    )
    if manifest is not None and not committed:
        verified = False
    status = "cache_verified" if verified else "cache_pending"
    if manifest is not None and not committed:
        status = "publication_unconfirmed"
    if committed:
        status = "complete" if verified else "committed_cache_pending"
        if publication.get("finalization_status") != "complete":
            status = "committed_finalization_pending"
        elif verified and (manifest.get("validation") or {}).get("status") in {"pending", "failed"}:
            status = "committed_verification_" + manifest["validation"]["status"]
        manifest["cache_verification"] = {"verified": verified, "status": "verified" if verified else "pending"}
        path = Path(manifest_path)
        pending = path.with_suffix(".json.pending")
        pending.write_text(json.dumps(manifest, sort_keys=True, indent=2), encoding="utf-8")
        pending.replace(path)
        try:
            _persist_completion(manifest, args.db.strip())
        except Exception as exc:
            print(f"::warning::Completion checkpoint pending ({type(exc).__name__})", file=sys.stderr)
    output = os.environ.get("GITHUB_OUTPUT")
    if output:
        with Path(output).open("a", encoding="utf-8") as handle:
            if manifest is not None:
                handle.write(f"data_committed={str(committed).lower()}\n")
            handle.write(f"cache_verified={str(verified).lower()}\n")
            handle.write(f"completion_status={status}\n")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a", encoding="utf-8") as handle:
            recovery = " Data upload must not be repeated for cache-only recovery." if committed else ""
            handle.write(f"\nPublication/cache status: `{status}`.{recovery}\n")


def main() -> int:
    args = parse_args()
    if args.publication_run_id:
        recovered = load_committed_publication(args.db.strip(), args.publication_run_id, mode=args.mode)
        # This is a small private runner-local receipt, not a database snapshot.
        receipt_dir = Path(os.environ.get("RUNNER_TEMP", os.environ.get("TEMP", "."))) / f"{args.db}-publication-{args.publication_run_id}"
        receipt_dir.mkdir(parents=True, exist_ok=True)
        args.manifest = str(receipt_dir / "delta_publish_manifest.json")
        Path(args.manifest).write_text(json.dumps(recovered, sort_keys=True), encoding="utf-8")
    manifest_path = resolve_manifest_path(args.db.strip(), args.manifest or None)
    committed = read_committed_manifest(manifest_path, args.db.strip())
    verified = False
    try:
        if committed and committed["publication"].get("finalization_status") != "complete":
            try:
                from importlib import import_module
                sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "fantasy_football_data_scripts"))
                LocalLeagueDB = import_module("multi_league.core.local_db").LocalLeagueDB
                db = LocalLeagueDB(Path(manifest_path).parent, args.db.strip())
                db.upload_to_fly(args.db.strip(), import_mode=committed.get("import_mode"),
                                 platform=committed.get("platform"), finalize_only=True)
                committed = read_committed_manifest(manifest_path, args.db.strip())
            except Exception as exc:
                print(f"::warning::Data committed; inventory/merge finalization pending ({type(exc).__name__})", file=sys.stderr)
        if committed and (committed.get("cache_verification") or {}).get("verified") is True:
            print("Cache already verified for this publication; skipping completed work")
            code, verified = 0, True
        else:
            try:
                code, verified = _run_warmer(args)
            except Exception as exc:
                if not committed:
                    raise
                print(f"::warning::Data committed; cache refresh pending ({type(exc).__name__})", file=sys.stderr)
                code = 1
        if committed and (args.validate or (committed.get("validation") or {}).get("status") == "pending"):
            # Cache work is independent and checkpointed before diagnostic QA.
            try:
                _record_cache_result(args, verified)
            except (OSError, ValueError, TypeError) as exc:
                print(f"::warning::Cache reporting pending ({type(exc).__name__}); data remains COMMITTED", file=sys.stderr)
            try:
                validate_publication(str(manifest_path), args.db.strip())
            except (OSError, ValueError, TypeError) as exc:
                print(f"::warning::Verification reporting pending ({type(exc).__name__}); data remains COMMITTED", file=sys.stderr)
        # A cache outage cannot undo a confirmed write or justify another import.
        # Callers without a receipt retain strict failure semantics.
        return 0 if committed and args.soft_warm_failures else code
    finally:
        try:
            _record_cache_result(args, verified)
        except (OSError, ValueError, TypeError) as exc:
            print(f"::warning::Cache reporting failed ({type(exc).__name__}); data publication is unchanged", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
