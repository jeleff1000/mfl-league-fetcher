#!/usr/bin/env python3
"""G1 read-load generator: route-shaped reads against a duckdb-server.

Drives concurrent, realistically-shaped league queries (matchup pages,
player weekly, standings/homepage) while a publish runs, and reports
p50/p95/p99 latency, failures, and timeouts in timestamped buckets so the
publish window's effect on readers is measurable.

Usage (against the rehearsal app through fly proxy):
    fly proxy 8080:8080 -a league-history-duckdb-rehearsal   # separate shell
    set DATABASE_SERVER_URL=http://localhost:8080
    set DATABASE_READ_TOKEN=<rehearsal read token>
    python scripts/g1_read_load.py --workers 8 --duration 300 \
        --out scripts/_artifacts/g1_read_load.json

Never point this at production during a Tuesday window; it is a rehearsal
tool (live-validation policy).
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import threading
import time
from collections import defaultdict
from datetime import datetime, timezone

import requests

QUERY_SHAPES = [
    # (name, sql template) — bounded, route-shaped reads
    ("league_matchups", "SELECT * FROM public.matchup WHERE db_name = '{db}' AND year = {year} ORDER BY week LIMIT 200"),
    ("standings", "SELECT manager, SUM(win) w, SUM(loss) l, SUM(team_points) pf FROM public.matchup WHERE db_name = '{db}' AND year = {year} GROUP BY manager ORDER BY w DESC LIMIT 30"),
    ("player_weekly", "SELECT * FROM public.player_fantasy WHERE db_name = '{db}' AND year = {year} AND week = {week} AND is_started = 1 LIMIT 300"),
    ("player_season", "SELECT * FROM public.player_fantasy_season WHERE db_name = '{db}' AND year = {year} LIMIT 300"),
    ("homepage", "SELECT * FROM public.homepage_league_summary WHERE db_name = '{db}' LIMIT 5"),
    ("career", "SELECT * FROM public.matchup_career WHERE db_name = '{db}' LIMIT 50"),
]


class LoadRunner:
    def __init__(self, url: str, token: str, timeout: float):
        self.url = url.rstrip("/")
        self.token = token
        self.timeout = timeout
        self.samples: list[tuple[float, str, float, str]] = []  # (ts, shape, ms, status)
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.dbs: list[str] = []
        self.years: list[int] = []

    def query(self, sql: str) -> tuple[str, float]:
        t0 = time.perf_counter()
        try:
            resp = requests.post(
                f"{self.url}/query",
                json={"sql": sql, "database": "___leagues"},
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=self.timeout,
            )
            ms = (time.perf_counter() - t0) * 1000
            return ("ok" if resp.status_code == 200 else f"http_{resp.status_code}", ms)
        except requests.exceptions.Timeout:
            return ("timeout", (time.perf_counter() - t0) * 1000)
        except Exception as exc:
            return (f"error_{type(exc).__name__}", (time.perf_counter() - t0) * 1000)

    def discover(self) -> None:
        resp = requests.post(
            f"{self.url}/query",
            json={"sql": "SELECT DISTINCT db_name FROM public.matchup USING SAMPLE 60 ROWS", "database": "___leagues"},
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=60,
        )
        resp.raise_for_status()
        self.dbs = [r["db_name"] for r in resp.json()]
        resp = requests.post(
            f"{self.url}/query",
            json={"sql": "SELECT DISTINCT year FROM public.matchup ORDER BY year DESC LIMIT 4", "database": "___leagues"},
            headers={"Authorization": f"Bearer {self.token}"},
            timeout=60,
        )
        resp.raise_for_status()
        self.years = [int(r["year"]) for r in resp.json()]
        if not self.dbs or not self.years:
            raise RuntimeError("discovery returned no leagues/years")
        print(f"[load] discovered {len(self.dbs)} leagues, years {self.years}")

    def worker(self) -> None:
        while not self.stop.is_set():
            shape, template = random.choice(QUERY_SHAPES)
            sql = template.format(
                db=random.choice(self.dbs).replace("'", "''"),
                year=random.choice(self.years),
                week=random.randint(1, 14),
            )
            status, ms = self.query(sql)
            with self.lock:
                self.samples.append((time.time(), shape, ms, status))

    def run(self, workers: int, duration: float) -> None:
        threads = [threading.Thread(target=self.worker, daemon=True) for _ in range(workers)]
        started = time.time()
        for t in threads:
            t.start()
        try:
            while time.time() - started < duration:
                time.sleep(5)
                with self.lock:
                    n = len(self.samples)
                    recent = [s for s in self.samples if s[0] > time.time() - 5]
                bad = sum(1 for s in recent if s[3] != "ok")
                p95 = statistics.quantiles([s[2] for s in recent], n=20)[18] if len(recent) >= 20 else None
                print(f"[load] t+{time.time() - started:5.0f}s total={n} last5s={len(recent)} "
                      f"errors={bad} p95={f'{p95:.0f}ms' if p95 else 'n/a'}", flush=True)
        finally:
            self.stop.set()
            for t in threads:
                t.join(timeout=self.timeout + 5)


def summarize(samples: list[tuple[float, str, float, str]], bucket_seconds: int) -> dict:
    ok = [s[2] for s in samples if s[3] == "ok"]
    statuses = defaultdict(int)
    for s in samples:
        statuses[s[3]] += 1

    def pct(vals, q):
        return round(statistics.quantiles(vals, n=100)[q - 1], 1) if len(vals) >= 100 else None

    buckets = defaultdict(list)
    t0 = min(s[0] for s in samples) if samples else 0
    for s in samples:
        buckets[int((s[0] - t0) // bucket_seconds)].append(s)
    timeline = []
    for b in sorted(buckets):
        bs = buckets[b]
        bok = [s[2] for s in bs if s[3] == "ok"]
        timeline.append({
            "t_start_s": b * bucket_seconds,
            "requests": len(bs),
            "errors": sum(1 for s in bs if s[3] != "ok"),
            "p50_ms": round(statistics.median(bok), 1) if bok else None,
            "p95_ms": pct(bok, 95),
        })
    return {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "total_requests": len(samples),
        "status_counts": dict(statuses),
        "latency_ms": {"p50": pct(ok, 50), "p95": pct(ok, 95), "p99": pct(ok, 99),
                       "max": round(max(ok), 1) if ok else None},
        "timeline": timeline,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default=os.environ.get("DATABASE_SERVER_URL", "http://localhost:8080"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--duration", type=float, default=300)
    parser.add_argument("--timeout", type=float, default=35)
    parser.add_argument("--bucket-seconds", type=int, default=10)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not token:
        print("DATABASE_READ_TOKEN required", file=sys.stderr)
        return 2

    runner = LoadRunner(args.url, token, args.timeout)
    runner.discover()
    runner.run(args.workers, args.duration)

    report = summarize(runner.samples, args.bucket_seconds)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=1)
    print(json.dumps({k: report[k] for k in ("total_requests", "status_counts", "latency_ms")}, indent=1))
    print(f"[load] report written to {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
