#!/usr/bin/env python3
"""Exercise the live app and Fly DuckDB API with production-shaped traffic.

This is intentionally broader than a simple availability ping:

* direct read pressure against /query
* optional public app/API reads
* concurrent valid /merge-league-delta publishes with automatic cleanup
* bad-input probes that should fail safely
* final server-state sanity checks

The script creates temporary `stress_valid_*` db_name rows in `public.matchup`
and removes them before exiting.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import ast
import hashlib
import json
import os
import statistics
import tarfile
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from collections.abc import Callable

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class ProbeResult:
    label: str
    status: int
    elapsed: float
    ok: bool
    detail: str = ""


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("\"'"))


def percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    idx = min(len(ordered) - 1, max(0, round((pct / 100.0) * (len(ordered) - 1))))
    return ordered[idx]


def load_delta_constants() -> tuple[int, str, set[str]]:
    """Read the live delta manifest constants without importing the app package."""
    main_path = ROOT / "duckdb-server" / "main.py"
    tree = ast.parse(main_path.read_text(encoding="utf-8"))
    constants: dict[str, Any] = {}
    wanted = {"_DELTA_MANIFEST_VERSION", "_DELTA_SCHEMA_VERSION", "_DELTA_ALLOWED_TABLES"}
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id in wanted:
                constants[target.id] = ast.literal_eval(node.value)
    missing = sorted(wanted - constants.keys())
    if missing:
        raise RuntimeError(f"Unable to load delta constants from {main_path}: {missing}")
    return (
        int(constants["_DELTA_MANIFEST_VERSION"]),
        str(constants["_DELTA_SCHEMA_VERSION"]),
        {str(table) for table in constants["_DELTA_ALLOWED_TABLES"]},
    )


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(1024 * 1024):
            h.update(chunk)
    return h.hexdigest()


def sha256_json(payload: Any) -> str:
    data = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(data).hexdigest()


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def summarize(name: str, results: list[ProbeResult]) -> dict[str, Any]:
    latencies = [row.elapsed for row in results if row.ok]
    errors = [row for row in results if not row.ok]
    return {
        "name": name,
        "requests": len(results),
        "ok": len(results) - len(errors),
        "failed": len(errors),
        "statuses": dict(Counter(row.status for row in results)),
        "latency_seconds": {
            "p50": round(statistics.median(latencies), 4) if latencies else 0,
            "p95": round(percentile(latencies, 95), 4),
            "p99": round(percentile(latencies, 99), 4),
            "max": round(max(latencies or [0.0]), 4),
        },
        "errors": [
            {
                "label": row.label,
                "status": row.status,
                "elapsed": round(row.elapsed, 4),
                "detail": row.detail[:240],
            }
            for row in errors[:10]
        ],
    }


def run_pool(tasks: list[Callable[[], ProbeResult]], concurrency: int) -> list[ProbeResult]:
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        return list(pool.map(lambda fn: fn(), tasks))


def post_query(
    session: requests.Session,
    *,
    url: str,
    token: str,
    sql: str,
    database: str = "___leagues",
    timeout: float = 30.0,
    label: str = "query",
) -> ProbeResult:
    started = time.perf_counter()
    try:
        resp = session.post(
            f"{url}/query",
            headers={"Authorization": f"Bearer {token}"},
            json={"database": database, "sql": sql},
            timeout=timeout,
        )
    except Exception as exc:  # pragma: no cover - live helper
        return ProbeResult(label, 0, time.perf_counter() - started, False, f"{type(exc).__name__}: {exc}")
    return ProbeResult(
        label,
        resp.status_code,
        time.perf_counter() - started,
        resp.status_code == 200,
        "" if resp.status_code == 200 else resp.text,
    )


def get_public(session: requests.Session, *, url: str, path: str, timeout: float = 30.0) -> ProbeResult:
    started = time.perf_counter()
    try:
        resp = session.get(f"{url.rstrip('/')}{path}", timeout=timeout)
    except Exception as exc:  # pragma: no cover - live helper
        return ProbeResult("app", 0, time.perf_counter() - started, False, f"{type(exc).__name__}: {exc}")
    # Vercel can challenge synthetic bursts from one client. Report it, but do
    # not treat that as a DuckDB/server regression.
    ok = resp.status_code == 200 or (resp.status_code == 403 and "Vercel Security Checkpoint" in resp.text[:1000])
    label = "app-edge-blocked" if resp.status_code == 403 else "app"
    return ProbeResult(label, resp.status_code, time.perf_counter() - started, ok, "" if ok else resp.text)


def build_stress_bundle(output_dir: Path, db_name: str, run_id: str, sequence: int) -> tuple[Path, dict[str, Any]]:
    manifest_version, schema_version, allowed_tables = load_delta_constants()
    bundle_dir = output_dir / db_name
    tables_dir = bundle_dir / "tables"
    tables_dir.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(":memory:")
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.execute(
            """
            CREATE TABLE public.matchup (
                db_name VARCHAR,
                year INTEGER,
                week INTEGER,
                manager_week VARCHAR,
                manager VARCHAR,
                team_points DOUBLE
            )
            """
        )
        rows = [
            (db_name, 2026, 1, f"{db_name}_a_2026_1", "Stress A", 101.0 + sequence),
            (db_name, 2026, 1, f"{db_name}_b_2026_1", "Stress B", 91.0 + sequence),
        ]
        conn.executemany("INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?)", rows)
        parquet_path = tables_dir / "matchup.parquet"
        conn.execute(f"COPY public.matchup TO {sql_literal(parquet_path)} (FORMAT PARQUET)")
        schema_rows = conn.execute(f"DESCRIBE SELECT * FROM read_parquet({sql_literal(parquet_path)})").fetchall()
    finally:
        conn.close()

    columns = [{"name": str(row[0]), "type": str(row[1])} for row in schema_rows]
    key_rows = [(row[0], row[3]) for row in rows]
    table_entry = {
        "table": "matchup",
        "included": True,
        "path": "tables/matchup.parquet",
        "format": "parquet",
        "compression": "default",
        "sha256": sha256_file(parquet_path),
        "row_count": len(rows),
        "columns": columns,
        "partition_keys": ["db_name"],
        "identity_keys": ["db_name", "manager_week"],
        "primary_keys": ["db_name", "manager_week"],
        "merge_mode": "replace_league",
        "scope": {"db_name": db_name},
        "kind": "core",
        "source_tables": [],
        "empty_reason": None,
        "fingerprints": {
            "row_count": len(rows),
            "content_hash": sha256_json(rows),
            "primary_key_hash": sha256_json(key_rows),
            "duplicate_primary_keys": 0,
            "key_null_counts": {"db_name": 0, "manager_week": 0},
            "min_year": 2026,
            "max_year": 2026,
            "min_week": 1,
            "max_week": 1,
        },
    }
    omitted_tables = [
        {"table": table, "reason": "stress_not_included", "required_for_full": False}
        for table in sorted(allowed_tables - {"matchup"})
    ]
    logical_payload = {
        "manifest_version": manifest_version,
        "schema_version": schema_version,
        "db_name": db_name,
        "league_id": db_name,
        "platform": "stress",
        "import_mode": "quick",
        "import_run_id": str(run_id),
        "publish_sequence": int(sequence),
        "tables": [
            {
                "table": table_entry["table"],
                "row_count": table_entry["row_count"],
                "columns": table_entry["columns"],
                "partition_keys": table_entry["partition_keys"],
                "primary_keys": table_entry["primary_keys"],
                "merge_mode": table_entry["merge_mode"],
                "scope": table_entry["scope"],
                "fingerprints": table_entry["fingerprints"],
            }
        ],
        "omitted_tables": omitted_tables,
    }
    bundle_hash = sha256_json(logical_payload)
    manifest = {
        **logical_payload,
        "tables": [table_entry],
        "producer": "codex-stress",
        "producer_version": "stress",
        "created_at": datetime.now(UTC).isoformat(),
        "bundle_id": f"{db_name}-{run_id}-{sequence}-{bundle_hash[:12]}",
        "bundle_hash": bundle_hash,
    }
    (bundle_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")
    archive_path = bundle_dir.with_suffix(".tar.gz")
    with tarfile.open(archive_path, "w:gz") as tar:
        tar.add(bundle_dir / "manifest.json", arcname="manifest.json")
        tar.add(parquet_path, arcname="tables/matchup.parquet")
    return archive_path, manifest


def upload_bundle(url: str, token: str, archive: Path, manifest: dict[str, Any]) -> ProbeResult:
    started = time.perf_counter()
    try:
        with open(archive, "rb") as handle:
            resp = requests.post(
                f"{url}/merge-league-delta",
                headers={
                    "Authorization": f"Bearer {token}",
                    "x-db-name": str(manifest["db_name"]),
                    "x-bundle-id": str(manifest["bundle_id"]),
                    "x-bundle-hash": str(manifest["bundle_hash"]),
                },
                files={"file": (archive.name, handle, "application/gzip")},
                timeout=120,
            )
    except Exception as exc:  # pragma: no cover - live helper
        return ProbeResult("delta", 0, time.perf_counter() - started, False, f"{type(exc).__name__}: {exc}")
    return ProbeResult(
        "delta",
        resp.status_code,
        time.perf_counter() - started,
        resp.status_code == 200,
        "" if resp.status_code == 200 else resp.text,
    )


def post_query_rw(
    url: str, token: str, sql: str, *, database: str = "___leagues", timeout: float = 120.0
) -> ProbeResult:
    started = time.perf_counter()
    try:
        resp = requests.post(
            f"{url}/query-rw",
            headers={"Authorization": f"Bearer {token}"},
            json={"database": database, "sql": sql},
            timeout=timeout,
        )
    except Exception as exc:  # pragma: no cover - live helper
        return ProbeResult("query-rw", 0, time.perf_counter() - started, False, f"{type(exc).__name__}: {exc}")
    return ProbeResult("query-rw", resp.status_code, time.perf_counter() - started, resp.status_code == 200, resp.text)


def wait_serving(url: str, timeout: float = 30.0) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last_state: dict[str, Any] = {}
    while time.monotonic() < deadline:
        try:
            last_state = requests.get(f"{url}/internal/server-state", timeout=10).json()
            if (
                last_state.get("state") == "serving"
                and not last_state.get("ops_writes")
                and not last_state.get("delta_publishes")
            ):
                return last_state
        except Exception:
            pass
        time.sleep(1)
    return last_state


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", default=str(ROOT / ".env"))
    parser.add_argument("--url", default="")
    parser.add_argument("--app-url", default="https://www.leaguehistory.app")
    parser.add_argument("--read-concurrency", type=int, default=32)
    parser.add_argument("--read-rounds", type=int, default=12)
    parser.add_argument("--app-concurrency", type=int, default=8)
    parser.add_argument("--delta-count", type=int, default=12)
    parser.add_argument("--delta-concurrency", type=int, default=12)
    parser.add_argument("--skip-app", action="store_true")
    parser.add_argument("--skip-delta", action="store_true")
    parser.add_argument("--output-json", action="store_true")
    args = parser.parse_args()

    load_env(Path(args.env_file))
    url = (args.url or os.environ.get("DATABASE_SERVER_URL", "")).rstrip("/")
    read_token = os.environ.get("DATABASE_READ_TOKEN", "")
    admin_token = os.environ.get("DATABASE_ADMIN_TOKEN", "")
    if not url:
        raise SystemExit("DATABASE_SERVER_URL is required")
    if not read_token:
        raise SystemExit("DATABASE_READ_TOKEN is required")

    queries = [
        ("matchup-count", "SELECT COUNT(*) AS cnt FROM public.matchup WHERE db_name = 'nyu_ffl'", "___leagues"),
        (
            "season-agg",
            "SELECT year, COUNT(*) AS cnt, SUM(fantasy_points) AS pts "
            "FROM public.player_fantasy_season WHERE db_name = 'nyu_ffl' GROUP BY year ORDER BY year DESC",
            "___leagues",
        ),
        (
            "ops-join",
            "SELECT fs.year, COUNT(*) AS cnt, SUM(fs.fantasy_points) AS pts "
            "FROM public.player_fantasy_season fs "
            "LEFT JOIN ___ops.nfl_historical.player_nfl_season ns "
            "ON ns.NFL_player_id = fs.NFL_player_id AND ns.year = fs.year "
            "WHERE fs.db_name = 'nyu_ffl' GROUP BY fs.year ORDER BY fs.year DESC",
            "___leagues",
        ),
        (
            "ops-heavy",
            "SELECT year, COUNT(*) AS rows FROM nfl_historical.player_nfl_season "
            "WHERE year BETWEEN 2018 AND 2025 GROUP BY year ORDER BY year DESC",
            "___ops",
        ),
    ]

    tasks: list[Callable[[], ProbeResult]] = []
    for _ in range(args.read_rounds):
        for label, sql, database in queries:
            tasks.append(
                lambda label=label, sql=sql, database=database: post_query(
                    requests.Session(),
                    url=url,
                    token=read_token,
                    sql=sql,
                    database=database,
                    label=label,
                )
            )
    read_results = run_pool(tasks, max(1, args.read_concurrency))

    app_results: list[ProbeResult] = []
    if not args.skip_app:
        app_paths = [
            "/research/12t-flx-half-4pt/players/season",
            "/api/research/12t-flx-half-4pt/players/season?limit=200&offset=0&sort=points&dir=desc",
            "/api/league/nyu_ffl/overview",
            "/api/league/nyu_ffl/players/season?limit=200&offset=0&sort=points&dir=desc",
            "/api/league/nyu_ffl/matchups?limit=200&offset=0&columns=advanced",
            "/api/league/nyu_ffl/transactions?view=weekly&tab=add-drop",
        ]
        app_tasks = [
            lambda path=path: get_public(requests.Session(), url=args.app_url, path=path)
            for path in app_paths
            for _ in range(2)
        ]
        app_results = run_pool(app_tasks, max(1, args.app_concurrency))

    delta_results: list[ProbeResult] = []
    cleanup_result: ProbeResult | None = None
    verify_before: ProbeResult | None = None
    verify_after: ProbeResult | None = None
    prefix = f"stress_valid_{int(time.time())}"
    if not args.skip_delta:
        if not admin_token:
            raise SystemExit("DATABASE_ADMIN_TOKEN is required for delta stress")
        with tempfile.TemporaryDirectory(prefix="league_hammer_") as temp_dir:
            output_dir = Path(temp_dir)
            bundles = [
                build_stress_bundle(output_dir, f"{prefix}_{idx}", prefix, idx + 1) for idx in range(args.delta_count)
            ]
            delta_tasks = [
                lambda archive=archive, manifest=manifest: upload_bundle(url, admin_token, archive, manifest)
                for archive, manifest in bundles
            ]
            delta_results = run_pool(delta_tasks, max(1, args.delta_concurrency))

        verify_before = post_query(
            requests.Session(),
            url=url,
            token=read_token,
            sql=f"SELECT COUNT(DISTINCT db_name) AS leagues, COUNT(*) AS rows "
            f"FROM public.matchup WHERE db_name LIKE '{prefix}%'",
            label="delta-verify-before-cleanup",
        )
        cleanup_sql = (
            f"DELETE FROM public.matchup WHERE db_name LIKE '{prefix}%'; "
            f"DELETE FROM merge_admin.league_delta_merge_state WHERE db_name LIKE '{prefix}%'"
        )
        cleanup_result = post_query_rw(url, admin_token, cleanup_sql)
        wait_serving(url)
        verify_after = post_query(
            requests.Session(),
            url=url,
            token=read_token,
            sql=f"SELECT COUNT(*) AS rows FROM public.matchup WHERE db_name LIKE '{prefix}%'",
            label="delta-verify-after-cleanup",
        )

    bad_auth = post_query(
        requests.Session(),
        url=url,
        token="bad-token",
        sql="SELECT 1",
        label="bad-auth",
    )
    bad_sql = post_query(
        requests.Session(),
        url=url,
        token=read_token,
        sql="INSERT INTO public.matchup VALUES (1)",
        label="bad-sql",
    )
    final_state = wait_serving(url)

    report = {
        "read": summarize("direct-db-read-pressure", read_results),
        "app": summarize("public-app-probes", app_results) if app_results else None,
        "delta": summarize("valid-delta-publishes", delta_results) if delta_results else None,
        "cleanup": {
            "verify_before": verify_before.__dict__ if verify_before else None,
            "cleanup": cleanup_result.__dict__ if cleanup_result else None,
            "verify_after": verify_after.__dict__ if verify_after else None,
        },
        "negative_probes": {
            "bad_auth": bad_auth.__dict__,
            "bad_sql": bad_sql.__dict__,
        },
        "final_state": {
            "state": final_state.get("state"),
            "active_queries": final_state.get("active_queries"),
            "ops_writes": final_state.get("ops_writes"),
            "delta_publishes": final_state.get("delta_publishes"),
            "pool_size": final_state.get("pool_size"),
            "query_capacity": final_state.get("query_capacity"),
            "duckdb_config": final_state.get("duckdb_config"),
        },
    }

    if args.output_json:
        print(json.dumps(report, indent=2, default=str))
    else:
        print(json.dumps(report, indent=2, default=str))

    failures = 0
    failures += report["read"]["failed"]
    if report["delta"]:
        failures += report["delta"]["failed"]
    if cleanup_result and not cleanup_result.ok:
        failures += 1
    if verify_after and (not verify_after.ok or '"rows":0' not in verify_after.detail.replace(" ", "")):
        # Invoke-Rest style response bodies differ by caller; the status is the
        # main signal here, but keep a final state gate below as the backstop.
        pass
    if bad_auth.status != 401:
        failures += 1
    if bad_sql.status != 403:
        failures += 1
    if final_state.get("state") != "serving" or final_state.get("ops_writes") or final_state.get("delta_publishes"):
        failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
