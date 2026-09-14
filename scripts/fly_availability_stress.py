"""Hammer Fly DuckDB read endpoints and report availability/latency.

Useful during import/merge runs:

    python scripts/fly_availability_stress.py --db-name the_league --duration 120 --concurrency 20
"""

from __future__ import annotations

import argparse
import os
import statistics
import threading
import time
from collections import Counter
from pathlib import Path
from typing import Any

import requests


def _load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def _default_queries(db_name: str | None) -> list[str]:
    safe_db = db_name.replace("'", "''") if db_name else ""
    db_filter = f"WHERE db_name = '{safe_db}'" if safe_db else ""
    return [
        "SELECT COUNT(*) AS cnt FROM public.league_settings",
        f"SELECT COUNT(*) AS cnt FROM public.matchup {db_filter}",
        f"SELECT COUNT(*) AS cnt FROM public.player_fantasy {db_filter}",
        f"SELECT COUNT(*) AS cnt FROM public.homepage_league_summary {db_filter}",
    ]


def _worker(
    *,
    worker_id: int,
    deadline: float,
    url: str,
    token: str,
    database: str,
    queries: list[str],
    timeout: float,
    machine_id: str | None,
    retries: int,
    retry_delay: float,
    results: list[dict[str, Any]],
    lock: threading.Lock,
) -> None:
    session = requests.Session()
    headers = {"Authorization": f"Bearer {token}"}
    if machine_id:
        headers["fly-force-instance-id"] = machine_id

    idx = worker_id
    while time.monotonic() < deadline:
        sql = queries[idx % len(queries)]
        idx += 1
        started = time.perf_counter()
        status = 0
        error = ""
        attempts = 0
        for attempt in range(retries + 1):
            attempts = attempt + 1
            try:
                resp = session.post(
                    f"{url}/query",
                    headers=headers,
                    json={"database": database, "sql": sql},
                    timeout=timeout,
                )
                status = resp.status_code
                error = resp.text[:240] if status != 200 else ""
            except Exception as exc:  # pragma: no cover - live smoke helper
                status = 0
                error = f"{type(exc).__name__}: {exc}"
            if status == 200 or (status not in {0, 429, 500, 502, 503, 504}):
                break
            if attempt < retries:
                time.sleep(retry_delay * (attempt + 1))
        elapsed = time.perf_counter() - started
        with lock:
            results.append({"status": status, "elapsed": elapsed, "error": error, "attempts": attempts})


def main() -> int:
    parser = argparse.ArgumentParser(description="Stress Fly DuckDB read availability.")
    parser.add_argument("--env-file", default=".env", help="Optional env file to load first")
    parser.add_argument("--url", default="", help="DATABASE_SERVER_URL override")
    parser.add_argument("--token", default="", help="DATABASE_READ_TOKEN override")
    parser.add_argument("--database", default="___leagues")
    parser.add_argument("--db-name", default="", help="League db_name for filtered default queries")
    parser.add_argument("--sql", action="append", default=[], help="SQL to run; can be repeated")
    parser.add_argument("--duration", type=float, default=60.0)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--machine-id", default="", help="Optional Fly machine id to force")
    parser.add_argument("--retries", type=int, default=2, help="Retries per request for transient socket/5xx failures")
    parser.add_argument("--retry-delay", type=float, default=0.1, help="Base retry sleep in seconds")
    parser.add_argument("--max-error-rate", type=float, default=0.01)
    parser.add_argument("--max-p95", type=float, default=5.0)
    args = parser.parse_args()

    _load_env(Path(args.env_file))
    url = (args.url or os.environ.get("DATABASE_SERVER_URL", "")).rstrip("/")
    token = args.token or os.environ.get("DATABASE_READ_TOKEN", "")
    if not url:
        raise SystemExit("DATABASE_SERVER_URL is required")
    if not token:
        raise SystemExit("DATABASE_READ_TOKEN is required")

    queries = args.sql or _default_queries(args.db_name or None)
    deadline = time.monotonic() + args.duration
    results: list[dict[str, Any]] = []
    lock = threading.Lock()
    threads = [
        threading.Thread(
            target=_worker,
            kwargs={
                "worker_id": i,
                "deadline": deadline,
                "url": url,
                "token": token,
                "database": args.database,
                "queries": queries,
                "timeout": args.timeout,
                "machine_id": args.machine_id or None,
                "retries": max(0, args.retries),
                "retry_delay": max(0.0, args.retry_delay),
                "results": results,
                "lock": lock,
            },
            daemon=True,
        )
        for i in range(args.concurrency)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    statuses = Counter(row["status"] for row in results)
    latencies = [row["elapsed"] for row in results if row["status"] == 200]
    errors = [row for row in results if row["status"] != 200]
    total = len(results)
    error_rate = len(errors) / total if total else 1.0
    p50 = statistics.median(latencies) if latencies else 0.0
    p95 = _percentile(latencies, 95)
    p99 = _percentile(latencies, 99)

    print(f"requests={total} statuses={dict(statuses)} error_rate={error_rate:.2%}")
    print(f"latency_seconds p50={p50:.3f} p95={p95:.3f} p99={p99:.3f} max={max(latencies or [0.0]):.3f}")
    retried = sum(1 for row in results if row.get("attempts", 1) > 1)
    if retried:
        print(f"retried_requests={retried} max_attempts={max(row.get('attempts', 1) for row in results)}")
    for row in errors[:8]:
        print(
            f"error status={row['status']} attempts={row.get('attempts', 1)} "
            f"elapsed={row['elapsed']:.3f}s detail={row['error']}"
        )

    if total == 0 or not latencies:
        return 1
    if error_rate > args.max_error_rate:
        return 2
    if p95 > args.max_p95:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
