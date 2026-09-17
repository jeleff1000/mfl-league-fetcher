"""DuckDB Server — self-hosted league data API."""

import asyncio
import hashlib
import json
import logging
import math
import os
import random
import re
import shutil
import tarfile
import tempfile
import threading
import time
from contextlib import asynccontextmanager, suppress
from pathlib import Path
from typing import Any

import duckdb as _duckdb
from fastapi import FastAPI, HTTPException, Request, Response, UploadFile, File, Header
from pydantic import BaseModel
from starlette.middleware.gzip import GZipMiddleware

try:
    from posthog import Posthog as _Posthog
except Exception as exc:  # pragma: no cover - optional telemetry dependency
    _Posthog = None
    _POSTHOG_IMPORT_ERROR = exc
else:
    _POSTHOG_IMPORT_ERROR = None

import db
from auth import get_bearer_token, validate_read_token, validate_admin_token, AuthError
from swap import atomic_swap, verify_checksum, startup_recovery, SwapError
import fleet_merge

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def _env_bool(name: str, default: bool = False) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(
    name: str,
    default: float,
    *,
    min_value: float | None = None,
    max_value: float | None = None,
) -> float:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
        if not math.isfinite(value):
            raise ValueError("not finite")
    except ValueError:
        logger.warning("Invalid %s=%r; using %.3f", name, raw, default)
        return default
    if min_value is not None:
        value = max(min_value, value)
    if max_value is not None:
        value = min(max_value, value)
    return value


def _init_posthog_client():
    if _env_bool("POSTHOG_DISABLED"):
        return None

    api_key = os.environ.get("POSTHOG_API_KEY", "").strip()
    if not api_key:
        return None

    if _Posthog is None:
        logger.warning("PostHog disabled: SDK import failed: %s", _POSTHOG_IMPORT_ERROR)
        return None

    try:
        return _Posthog(
            project_api_key=api_key,
            host=os.environ.get("POSTHOG_HOST", "").strip() or "https://us.i.posthog.com",
            enable_exception_autocapture=False,
        )
    except Exception as exc:
        logger.warning("PostHog disabled: client initialization failed: %s", exc)
        return None


posthog_client = _init_posthog_client()
_SERVICE_ID = os.environ.get("FLY_MACHINE_ID", "duckdb-server")
_POSTHOG_QUERY_SAMPLE_RATE = _env_float("POSTHOG_QUERY_SAMPLE_RATE", 0.02, min_value=0.0, max_value=1.0)
_POSTHOG_SLOW_QUERY_THRESHOLD_SECONDS = _env_float(
    "POSTHOG_SLOW_QUERY_THRESHOLD_SECONDS",
    5.0,
    min_value=0.1,
)

MAX_CONCURRENT_QUERIES = int(os.environ.get("MAX_CONCURRENT_QUERIES", "5"))
QUERY_QUEUE_TIMEOUT_SECONDS = _env_float("QUERY_QUEUE_TIMEOUT_SECONDS", 1.0, min_value=0.1)
QUERY_OPS_WRITE_WAIT_SECONDS = _env_float(
    "QUERY_OPS_WRITE_WAIT_SECONDS",
    QUERY_QUEUE_TIMEOUT_SECONDS,
    min_value=0.0,
    max_value=60.0,
)
PUBLIC_QUERY_TIMEOUT = int(os.environ.get("PUBLIC_QUERY_TIMEOUT", "30"))
ADMIN_QUERY_TIMEOUT = int(os.environ.get("ADMIN_QUERY_TIMEOUT", "120"))
WRITE_TIMEOUT_SECONDS = ADMIN_QUERY_TIMEOUT + 5
WRITE_DUCKDB_THREADS = int(os.environ.get("DUCKDB_WRITE_THREADS", "2"))
MERGE_STEP_TIMEOUT_SECONDS = _env_float("MERGE_STEP_TIMEOUT_SECONDS", 180.0, min_value=10.0)
MERGE_HARD_EXIT_SECONDS = _env_float("MERGE_HARD_EXIT_SECONDS", 360.0, min_value=30.0)
# Fleet partition merges touch every batched league in one transaction, so
# they get their own (larger) budgets than single-league delta merges.
FLEET_MERGE_STEP_TIMEOUT_SECONDS = _env_float("FLEET_MERGE_STEP_TIMEOUT_SECONDS", 300.0, min_value=10.0)
FLEET_MERGE_HARD_EXIT_SECONDS = _env_float("FLEET_MERGE_HARD_EXIT_SECONDS", 900.0, min_value=60.0)
RW_HARD_EXIT_SECONDS = _env_float("RW_HARD_EXIT_SECONDS", ADMIN_QUERY_TIMEOUT + 60.0, min_value=30.0)
# /merge-ops replaces whole ___ops reference tables (the ~1.2M x 820 super table takes minutes to
# CREATE OR REPLACE), so it needs its own generous ceilings independent of the 120s admin default.
OPS_MERGE_TIMEOUT = _env_float("OPS_MERGE_TIMEOUT", 900.0, min_value=120.0)  # per-table statement
OPS_MERGE_HARD_EXIT = _env_float("OPS_MERGE_HARD_EXIT", 1500.0, min_value=300.0)  # whole /merge-ops call
# /compact-db runs with the pool closed (sole open connection), so it can safely
# use more threads/memory than the per-query guardrails to rebuild wide tables fast.
COMPACT_THREADS = int(os.environ.get("COMPACT_THREADS", "4"))
COMPACT_MEMORY_LIMIT = os.environ.get("COMPACT_MEMORY_LIMIT", "5GB")
COMPACT_TIMEOUT_SECONDS = _env_float("COMPACT_TIMEOUT_SECONDS", 800.0, min_value=60.0)
MAX_INFLIGHT_DELTA_PUBLISHES = int(os.environ.get("MAX_INFLIGHT_DELTA_PUBLISHES", "1"))
DELTA_ADMISSION_TIMEOUT_SECONDS = _env_float("DELTA_ADMISSION_TIMEOUT_SECONDS", 1.0, min_value=0.1)
DELTA_BUSY_RETRY_AFTER_SECONDS = _env_float("DELTA_BUSY_RETRY_AFTER_SECONDS", 20.0, min_value=1.0)
ASYNC_DB_STARTUP = _env_bool("ASYNC_DB_STARTUP", False)
DUCKDB_CHECKPOINT_WAL_MB = _env_float("DUCKDB_CHECKPOINT_WAL_MB", 512.0, min_value=0.0)
DUCKDB_CHECKPOINT_TIMEOUT_SECONDS = _env_float("DUCKDB_CHECKPOINT_TIMEOUT_SECONDS", 600.0, min_value=30.0)
SOFT_DRAIN_TIMEOUT = 10
HARD_DRAIN_TIMEOUT = 30
POOL_REOPEN_MAX_ATTEMPTS = int(os.environ.get("POOL_REOPEN_MAX_ATTEMPTS", "8"))
POOL_REOPEN_RETRY_BASE_SECONDS = _env_float(
    "POOL_REOPEN_RETRY_BASE_SECONDS",
    0.25,
    min_value=0.05,
    max_value=5.0,
)
QUERY_RW_STARTUP_WAIT_SECONDS = _env_float(
    "QUERY_RW_STARTUP_WAIT_SECONDS",
    30.0,
    min_value=0.0,
    max_value=120.0,
)

_query_semaphore: asyncio.Semaphore | None = None
_merge_lock: asyncio.Lock | None = None
_delta_publish_semaphore: asyncio.Semaphore | None = None
_delta_publish_inflight = 0
_delta_publish_active: dict[str, dict[str, Any]] = {}
_delta_publish_active_lock = threading.Lock()
_state = {"status": "starting"}
_ops_write_count = 0
# Serializes ___ops writers without blocking the dedicated read connection while
# a replacement snapshot is being built.  The old snapshot remains queryable
# until the final close/rename/reopen handoff.
_ops_rebuild_lock = threading.Lock()


def _begin_ops_write_state(*, snapshot: bool = False) -> None:
    """Mark an ___ops write; snapshot rebuilds keep reads online."""
    global _ops_write_count
    _ops_write_count += 1
    if _state["status"] == "serving":
        _state["status"] = "ops_snapshotting" if snapshot else "ops_writing"


def _end_ops_write_state() -> None:
    """Return to serving once all concurrent ___ops write requests have finished."""
    global _ops_write_count
    _ops_write_count = max(0, _ops_write_count - 1)
    if _ops_write_count == 0 and _state["status"] in {"ops_writing", "ops_snapshotting"}:
        _state["status"] = "serving"


def _set_serving_or_ops_writing() -> None:
    _state["status"] = "ops_writing" if _ops_write_count > 0 else "serving"


async def _acquire_delta_publish_slot(db_name: str, bundle_id: str | None = None) -> str:
    global _delta_publish_inflight
    if _state["status"] not in {"serving", "ops_writing"}:
        track_event(
            "league_delta_rejected",
            {"db_name": db_name, "bundle_id": bundle_id, "reason": _state["status"]},
        )
        raise HTTPException(
            status_code=503,
            detail="Delta publish service not ready",
            headers={"Retry-After": "5", "Cache-Control": "no-store"},
        )
    if _delta_publish_semaphore is None:
        track_event("league_delta_rejected", {"db_name": db_name, "reason": "not_ready"})
        raise HTTPException(status_code=503, detail="Delta publish service not ready", headers={"Retry-After": "2"})
    try:
        await asyncio.wait_for(_delta_publish_semaphore.acquire(), timeout=DELTA_ADMISSION_TIMEOUT_SECONDS)
    except TimeoutError as e:
        retry_after = str(int(math.ceil(DELTA_BUSY_RETRY_AFTER_SECONDS)))
        track_event(
            "league_delta_rejected",
            {
                "db_name": db_name,
                "bundle_id": bundle_id,
                "reason": "admission_full",
                "inflight": _delta_publish_inflight,
                "capacity": MAX_INFLIGHT_DELTA_PUBLISHES,
            },
        )
        logger.info(
            "Delta publish admission full for %s bundle=%s inflight=%d capacity=%d retry_after=%.1fs",
            db_name,
            bundle_id or "",
            _delta_publish_inflight,
            MAX_INFLIGHT_DELTA_PUBLISHES,
            DELTA_BUSY_RETRY_AFTER_SECONDS,
        )
        raise HTTPException(
            status_code=429,
            detail="Delta publish server busy; retry later",
            headers={"Retry-After": retry_after, "Cache-Control": "no-store"},
        ) from e
    token = f"{time.time_ns()}-{random.randint(100000, 999999)}"
    with _delta_publish_active_lock:
        _delta_publish_inflight += 1
        _delta_publish_active[token] = {
            "db_name": db_name,
            "bundle_id": bundle_id,
            "stage": "admitted",
            "admitted_at": time.time(),
            "updated_at": time.time(),
        }
    logger.info(
        "Delta publish admitted for %s bundle=%s inflight=%d/%d",
        db_name,
        bundle_id or "",
        _delta_publish_inflight,
        MAX_INFLIGHT_DELTA_PUBLISHES,
    )
    return token


def _update_delta_publish_slot(token: str | None, stage: str, **fields: Any) -> None:
    if not token:
        return
    with _delta_publish_active_lock:
        entry = _delta_publish_active.get(token)
        if entry is None:
            return
        entry.update(fields)
        entry["stage"] = stage
        entry["updated_at"] = time.time()


def _active_delta_publish_snapshot() -> list[dict[str, Any]]:
    now = time.time()
    with _delta_publish_active_lock:
        rows = []
        for entry in _delta_publish_active.values():
            row = dict(entry)
            row["elapsed_seconds"] = round(now - float(row.get("admitted_at", now)), 2)
            row["updated_age_seconds"] = round(now - float(row.get("updated_at", now)), 2)
            row.pop("admitted_at", None)
            row.pop("updated_at", None)
            rows.append(row)
    rows.sort(key=lambda row: (row.get("db_name") or "", row.get("bundle_id") or ""))
    return rows


def _release_delta_publish_slot(token: str | None) -> None:
    global _delta_publish_inflight
    with _delta_publish_active_lock:
        if token is not None:
            _delta_publish_active.pop(token, None)
        _delta_publish_inflight = max(0, _delta_publish_inflight - 1)
    if _delta_publish_semaphore is not None:
        _delta_publish_semaphore.release()


def track_event(event: str, properties: dict | None = None, *, sample_rate: float = 1.0) -> None:
    """Best-effort telemetry; never let analytics affect query handling."""
    if posthog_client is None:
        return
    if sample_rate < 1.0 and random.random() >= sample_rate:
        return
    try:
        event_properties = {
            "service_id": _SERVICE_ID,
            "role": os.environ.get("FLY_ROLE", ""),
            "capacity_mode": os.environ.get("FLY_CAPACITY_MODE", ""),
            **(properties or {}),
        }
        posthog_client.capture(
            distinct_id=_SERVICE_ID,
            event=event,
            properties=event_properties,
        )
    except Exception as exc:
        logger.debug("PostHog capture failed for %s: %s", event, exc)


def cleanup_stale_uploads(data_dir: Path):
    """Remove stale files from interrupted merges. Create spill directory."""
    for pattern in ("league_upload_*.duckdb", "league_upload_*.duckdb.wal"):
        for f in data_dir.glob(pattern):
            size_mb = f.stat().st_size / (1024 * 1024)
            logger.info("Cleaning stale upload: %s (%.1f MB)", f.name, size_mb)
            f.unlink()
    for temp_dir in data_dir.glob("*.duckdb.tmp"):
        if temp_dir.is_dir():
            logger.info("Cleaning stale DuckDB temp directory: %s", temp_dir.name)
            shutil.rmtree(temp_dir, ignore_errors=True)
    (data_dir / "duckdb_tmp").mkdir(exist_ok=True)


def _check_db_health() -> bool:
    """Check if the DB query path is functional."""
    if db.get_active_count() >= db.POOL_SIZE:
        return True
    try:
        conn = db.acquire_connection(timeout=5.0)
        try:
            conn.execute("SELECT 1")
            return True
        finally:
            db.release_connection(conn)
    except Exception:
        return False


async def _watchdog():
    """Kill process if DB path broken for 90s+. Triggers Fly's on-failure restart."""
    consecutive_failures = 0
    while True:
        await asyncio.sleep(30)
        if _state["status"] != "serving":
            consecutive_failures = 0
            continue
        if await asyncio.to_thread(_check_db_health):
            consecutive_failures = 0
        else:
            consecutive_failures += 1
            logger.error("Watchdog: DB health check failed (%d consecutive)", consecutive_failures)
            if consecutive_failures >= 3:
                logger.critical("Watchdog: DB path broken for 90s+, exiting for restart")
                os._exit(1)


def _get_duckdb_memory_info() -> dict:
    """Best-effort memory telemetry without blocking request handling."""
    try:
        conn = db.acquire_connection(timeout=0.05)
        try:
            rows = conn.execute("SELECT tag, memory_usage_bytes FROM duckdb_memory()").fetchall()
            return {row[0]: row[1] for row in rows}
        finally:
            db.release_connection(conn)
    except Exception:
        return {"error": "pool busy"}


def _query_busy_response(*, reason: str) -> Response:
    """Return a retryable response while the primary is busy writing."""
    return Response(
        content=json.dumps({"detail": "Query service is busy", "reason": reason}),
        media_type="application/json",
        status_code=503,
        headers={"Retry-After": "1", "Cache-Control": "no-store"},
    )


async def _wait_for_ops_write_to_finish() -> bool:
    """Let public reads ride out short ___ops writer windows instead of failing fast."""
    if QUERY_OPS_WRITE_WAIT_SECONDS <= 0:
        return _state["status"] == "serving"
    deadline = time.perf_counter() + QUERY_OPS_WRITE_WAIT_SECONDS
    while _state["status"] == "ops_writing" and time.perf_counter() < deadline:
        await asyncio.sleep(0.05)
    return _state["status"] == "serving"


def _is_duckdb_file_handle_conflict(exc: Exception) -> bool:
    message = str(exc).lower()
    return "unique file handle conflict" in message or "in the process of being detached" in message


async def _reopen_pool_after_write(context: str) -> None:
    for attempt in range(1, POOL_REOPEN_MAX_ATTEMPTS + 1):
        try:
            db.init_pool()
            return
        except Exception as exc:
            if not _is_duckdb_file_handle_conflict(exc) or attempt >= POOL_REOPEN_MAX_ATTEMPTS:
                logger.critical("Failed to reopen DuckDB pool after %s; exiting: %s", context, exc, exc_info=True)
                os._exit(1)

            logger.warning(
                "DuckDB pool reopen hit a file-handle conflict after %s (attempt %d/%d); retrying: %s",
                context,
                attempt,
                POOL_REOPEN_MAX_ATTEMPTS,
                exc,
            )
            try:
                db.close_all()
            except Exception:
                logger.warning("Failed to close partial DuckDB pool after reopen conflict", exc_info=True)
            await asyncio.sleep(min(POOL_REOPEN_RETRY_BASE_SECONDS * attempt, 2.0))


def _wal_size_mb(db_path: Path) -> float:
    wal_path = _wal_path(db_path)
    if not wal_path.exists():
        return 0.0
    return wal_path.stat().st_size / (1024 * 1024)


def _wal_path(db_path: Path) -> Path:
    return db_path.with_name(f"{db_path.name}.wal")


def _quarantine_wal(db_path: Path, *, reason: str) -> Path | None:
    wal_path = _wal_path(db_path)
    if not wal_path.exists():
        return None
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    target = wal_path.with_name(f"{wal_path.name}.quarantine.{timestamp}")
    counter = 1
    while target.exists():
        counter += 1
        target = wal_path.with_name(f"{wal_path.name}.quarantine.{timestamp}.{counter}")
    logger.error(
        "Quarantining DuckDB WAL for %s after %s: %s -> %s",
        db_path.name,
        reason,
        wal_path,
        target,
    )
    wal_path.rename(target)
    return target


def _checkpoint_connection_if_wal_large(conn, db_path: Path, *, reason: str, force: bool = False) -> bool:
    if DUCKDB_CHECKPOINT_WAL_MB <= 0:
        return False
    before_mb = _wal_size_mb(db_path)
    if before_mb <= 0:
        return False
    if not force and before_mb < DUCKDB_CHECKPOINT_WAL_MB:
        return False
    checkpoint_start = time.perf_counter()
    logger.warning(
        "DuckDB WAL %.1f MB exceeds %.1f MB; checkpointing %s",
        before_mb,
        DUCKDB_CHECKPOINT_WAL_MB,
        reason,
    )
    try:
        _interrupting_execute(
            conn,
            "CHECKPOINT",
            step=f"checkpoint {reason}",
            timeout_seconds=DUCKDB_CHECKPOINT_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        logger.warning("DuckDB checkpoint failed for %s: %s", reason, exc, exc_info=True)
        return False
    after_mb = _wal_size_mb(db_path)
    logger.info(
        "DuckDB checkpoint complete for %s in %.2fs (wal %.1f MB -> %.1f MB)",
        reason,
        time.perf_counter() - checkpoint_start,
        before_mb,
        after_mb,
    )
    return True


def _checkpoint_database_if_wal_large(db_path: Path, *, data_dir: Path, reason: str) -> bool:
    if not db_path.exists():
        return False
    before_mb = _wal_size_mb(db_path)
    if before_mb < DUCKDB_CHECKPOINT_WAL_MB:
        logger.info(
            "DuckDB checkpoint skipped for %s; WAL %.1f MB below %.1f MB",
            reason,
            before_mb,
            DUCKDB_CHECKPOINT_WAL_MB,
        )
        return False
    logger.info("DuckDB checkpoint opening %s for %s", db_path.name, reason)
    conn = None
    try:
        conn = db.connect_database(db_path, data_dir=data_dir, threads=WRITE_DUCKDB_THREADS)
        return _checkpoint_connection_if_wal_large(conn, db_path, reason=reason)
    finally:
        if conn is not None:
            conn.close()


def _checkpoint_database_if_wal_exists(db_path: Path, *, data_dir: Path, reason: str) -> bool:
    """Checkpoint any non-empty WAL, regardless of size."""
    if not db_path.exists() or _wal_size_mb(db_path) <= 0:
        return False
    logger.info("DuckDB checkpoint opening %s for %s", db_path.name, reason)
    conn = None
    try:
        conn = db.connect_database(db_path, data_dir=data_dir, threads=WRITE_DUCKDB_THREADS)
        return _checkpoint_connection_if_wal_large(conn, db_path, reason=reason, force=True)
    finally:
        if conn is not None:
            conn.close()


def _checkpoint_database_with_wal_quarantine(db_path: Path, *, data_dir: Path, reason: str) -> bool:
    """Replay/checkpoint a startup WAL, preserving it if DuckDB cannot replay it."""
    try:
        return _checkpoint_database_if_wal_exists(db_path, data_dir=data_dir, reason=reason)
    except Exception:
        if _wal_size_mb(db_path) <= 0:
            raise
        logger.exception("DuckDB startup: %s WAL replay failed; quarantining WAL", db_path.name)
        quarantined = _quarantine_wal(db_path, reason=f"{reason} WAL replay failure")
        if quarantined is None:
            raise
        return False


def _startup_db_sync(data_dir: Path) -> None:
    logger.info("DuckDB startup: recovery begin")
    startup_recovery(data_dir)
    logger.info("DuckDB startup: cleanup begin")
    cleanup_stale_uploads(data_dir)
    logger.info("DuckDB startup: checkpoint begin")
    _checkpoint_database_with_wal_quarantine(
        data_dir / "___leagues.duckdb",
        data_dir=data_dir,
        reason="startup ___leagues",
    )
    _checkpoint_database_with_wal_quarantine(
        data_dir / "___ops.duckdb",
        data_dir=data_dir,
        reason="startup ___ops",
    )
    logger.info("DuckDB startup: pool init begin")
    db.init_pool()
    logger.info("DuckDB startup: pool init complete")


async def _startup_db_background(data_dir: Path) -> None:
    logger.info("Starting DuckDB pool initialization in background")
    startup_started = time.perf_counter()
    try:
        await asyncio.to_thread(_startup_db_sync, data_dir)
    except Exception:
        logger.exception("DuckDB startup failed; exiting so Fly can restart")
        _state["status"] = "startup_failed"
        await asyncio.sleep(1)
        os._exit(1)
    _set_serving_or_ops_writing()
    logger.info(
        "DuckDB pool initialization complete in %.2fs; state=%s",
        time.perf_counter() - startup_started,
        _state["status"],
    )


async def _acquire_query_slot(database: str) -> None:
    if _query_semaphore is None:
        track_event("query_rejected", {"database": database, "reason": "not_ready"})
        raise HTTPException(status_code=503, detail="Query service not ready", headers={"Retry-After": "2"})
    try:
        await asyncio.wait_for(_query_semaphore.acquire(), timeout=QUERY_QUEUE_TIMEOUT_SECONDS)
    except TimeoutError as e:
        track_event("query_rejected", {"database": database, "reason": "queue_timeout"})
        raise HTTPException(status_code=429, detail="Query server busy", headers={"Retry-After": "2"}) from e


async def _wait_for_startup_before_write() -> bool:
    """Avoid write/open races with async DuckDB pool initialization."""
    if _state["status"] != "starting":
        return True
    deadline = time.perf_counter() + QUERY_RW_STARTUP_WAIT_SECONDS
    while _state["status"] == "starting" and time.perf_counter() < deadline:
        await asyncio.sleep(0.25)
    return _state["status"] != "starting"


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _query_semaphore, _merge_lock, _delta_publish_semaphore, _delta_publish_inflight
    _state["status"] = "starting"
    _query_semaphore = asyncio.Semaphore(MAX_CONCURRENT_QUERIES)
    _merge_lock = asyncio.Lock()
    _delta_publish_semaphore = asyncio.Semaphore(MAX_INFLIGHT_DELTA_PUBLISHES)
    with _delta_publish_active_lock:
        _delta_publish_inflight = 0
        _delta_publish_active.clear()
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    startup_task: asyncio.Task | None = None
    if ASYNC_DB_STARTUP:
        startup_task = asyncio.create_task(_startup_db_background(data_dir))
    else:
        _startup_db_sync(data_dir)
        _set_serving_or_ops_writing()
    watchdog_task = asyncio.create_task(_watchdog())
    yield
    watchdog_task.cancel()
    if startup_task is not None:
        startup_task.cancel()
    with suppress(asyncio.CancelledError):
        await watchdog_task
    if startup_task is not None:
        with suppress(asyncio.CancelledError):
            await startup_task
    db.close_all()
    if posthog_client is not None:
        try:
            posthog_client.flush()
        except Exception as exc:
            logger.debug("PostHog flush failed: %s", exc)


app = FastAPI(lifespan=lifespan)
app.add_middleware(GZipMiddleware, minimum_size=1024, compresslevel=3)

_READ_START_PATTERN = re.compile(r"^\s*(SELECT|WITH|DESCRIBE|SHOW|EXPLAIN)\b", re.IGNORECASE)
_FORBIDDEN_SQL_TOKEN_PATTERN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|COPY|ATTACH|DETACH|LOAD|INSTALL|"
    r"CALL|PRAGMA|VACUUM|EXPORT|IMPORT|MERGE|REPLACE)\b",
    re.IGNORECASE,
)
# Table functions and env readers start with SELECT and carry no forbidden token, so
# the pattern above never sees them — and they reach the filesystem/network when the
# engine permits external access (2026-07-11 audit P0). Blocked here AND at the engine
# (db._apply_access_hardening) AND in the frontend validator (safe-sql.ts). The
# trailing `\s*\(` keeps ordinary identifiers containing these words legal.
_FORBIDDEN_FUNCTION_PATTERN = re.compile(
    r"\b(read_csv(?:_auto)?|read_json(?:_auto|_objects(?:_auto)?)?|read_ndjson(?:_auto|_objects(?:_auto)?)?|"
    r"read_parquet|parquet_scan|parquet_metadata|parquet_schema|read_text|read_blob|read_xlsx|"
    r"glob|getenv|sniff_csv|copy_database|enable_profiling|register_filesystem)\s*\(",
    re.IGNORECASE,
)
_DB_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_]+$")
_MERGE_STATE_SCHEMA = "merge_admin"
_MERGE_STATE_TABLE = "league_merge_state"
_DELTA_STATE_TABLE = "league_delta_merge_state"
_DELTA_MANIFEST_VERSION = 1
_DELTA_SCHEMA_VERSION = "league-delta-v1"
_DAMAGED_DERIVED_TARGETS = (
    "homepage_manager_rankings",
    "matchup_h2h_career",
    "player_fantasy_season",
    "player_fantasy_season_all",
    "standings_by_year",
)
_DELTA_MAX_UPLOAD_BYTES = int(os.environ.get("MAX_DELTA_UPLOAD_BYTES", str(1024 * 1024 * 1024)))
_DELTA_ALLOWED_TABLES = {
    "all_play",
    "draft",
    "draft_manager_career",
    "draft_manager_season",
    "draft_player_career",
    "franchise_identity_audit",
    "franchise_identity_registry",
    "h2h_season",
    "homepage_current_standings",
    "homepage_league_summary",
    "homepage_manager_profiles",
    "homepage_manager_rankings",
    "homepage_top_rivalries",
    "keeper_config",
    "league_context",
    "league_rules",
    "league_settings",
    "manager_overrides",
    "matchup",
    "matchup_career",
    "matchup_h2h_career",
    "matchup_h2h_season",
    "matchup_season",
    "player_fantasy",
    "player_fantasy_career",
    "player_fantasy_career_all",
    "player_fantasy_season",
    "player_fantasy_season_all",
    "schedule",
    "schedule_swap",
    "schedule_swap_season",
    "standings_by_year",
    "standings_config",
    "transaction_manager_career",
    "transaction_manager_season",
    "transaction_player_career",
    "transaction_report_card",
    "transactions",
}
_DELTA_FULL_REQUIRED_TABLES = {"matchup", "player_fantasy", "league_settings"}
_DELTA_REQUIRED_FULL_AGGREGATES = {
    "draft": {"draft_manager_career", "draft_manager_season", "draft_player_career"},
    "matchup": {
        "all_play",
        "h2h_season",
        "homepage_current_standings",
        "homepage_league_summary",
        "homepage_manager_profiles",
        "homepage_manager_rankings",
        "homepage_top_rivalries",
        "matchup_career",
        "matchup_h2h_career",
        "matchup_h2h_season",
        "matchup_season",
        "schedule_swap",
        "schedule_swap_season",
        "standings_by_year",
    },
    "player_fantasy": {
        "player_fantasy_career",
        "player_fantasy_career_all",
        "player_fantasy_season",
        "player_fantasy_season_all",
    },
    "transactions": {
        "transaction_manager_career",
        "transaction_manager_season",
        "transaction_player_career",
        "transaction_report_card",
    },
}
_DELTA_CORE_IDENTITY_KEYS = {
    "draft": ("db_name", "year", "draft_id", "round", "pick"),
    "franchise_identity_audit": ("db_name", "assignment_key"),
    "franchise_identity_registry": ("db_name", "resolved_franchise_id"),
    "keeper_config": ("db_name", "year"),
    "league_context": ("db_name",),
    "league_rules": ("db_name",),
    "league_settings": ("db_name", "year"),
    "manager_overrides": ("db_name", "id"),
    "matchup": ("db_name", "manager_week"),
    "player_fantasy": ("db_name", "player_week"),
    "schedule": ("db_name", "manager_week"),
    "standings_config": ("db_name",),
    "transactions": ("db_name", "transaction_id", "transaction_sequence"),
}
_FAST_PATH_SENTINEL_MAX_TABLES = 12
_FAST_PATH_SENTINEL_MAX_INCOMING_ROWS = 1000
_FAST_PATH_EXCLUDED_PREFIXES = ("player_fantasy",)


def _strip_sql_literals_and_comments(sql: str, *, strip_double_quoted_identifiers: bool = True) -> str:
    """Remove SQL comments/literals before statement safety checks."""
    out = []
    i = 0
    in_single_quote = False
    in_double_quote = False
    in_line_comment = False
    in_block_comment = False

    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_line_comment:
            if ch == "\n":
                in_line_comment = False
                out.append(ch)
            i += 1
            continue

        if in_block_comment:
            if ch == "*" and nxt == "/":
                in_block_comment = False
                i += 2
            else:
                i += 1
            continue

        if in_single_quote:
            if ch == "'" and nxt == "'":
                i += 2
                continue
            if ch == "'":
                in_single_quote = False
            out.append(" ")
            i += 1
            continue

        if in_double_quote:
            if ch == '"' and nxt == '"':
                if not strip_double_quoted_identifiers:
                    out.append(ch)
                    out.append(nxt)
                i += 2
                continue
            if ch == '"':
                in_double_quote = False
                out.append(" " if strip_double_quoted_identifiers else ch)
            else:
                out.append(" " if strip_double_quoted_identifiers else ch)
            i += 1
            continue

        if ch == "-" and nxt == "-":
            in_line_comment = True
            i += 2
            continue

        if ch == "/" and nxt == "*":
            in_block_comment = True
            i += 2
            continue

        if ch == "'":
            in_single_quote = True
            out.append(" ")
            i += 1
            continue

        if ch == '"':
            in_double_quote = True
            out.append(" " if strip_double_quoted_identifiers else ch)
            i += 1
            continue

        out.append(ch)
        i += 1

    return "".join(out)


def is_read_only_sql(sql: str) -> bool:
    """Check if SQL is a single read-only statement."""
    scrubbed = _strip_sql_literals_and_comments(sql)
    statements = [stmt.strip() for stmt in scrubbed.split(";") if stmt.strip()]
    if len(statements) != 1:
        return False
    stmt = statements[0]
    if not _READ_START_PATTERN.match(stmt):
        return False
    if _FORBIDDEN_FUNCTION_PATTERN.search(stmt):
        return False
    return not bool(_FORBIDDEN_SQL_TOKEN_PATTERN.search(stmt))


class QueryRequest(BaseModel):
    sql: str
    database: str = "___leagues"


@app.post("/query")
async def query_endpoint(req: QueryRequest, request: Request):
    try:
        validate_read_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/query"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if not is_read_only_sql(req.sql):
        raise HTTPException(status_code=403, detail="Write operations not allowed")

    if _state["status"] == "starting":
        return _query_busy_response(reason="starting")

    # ___ops-only queries use a dedicated connection — no pool contention.
    # These work even during draining since they don't touch ___leagues.
    if req.database == "___ops" and not re.search(r"\b___leagues\b", req.sql, re.IGNORECASE):
        t0 = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(_execute_ops_query, req.sql),
                timeout=PUBLIC_QUERY_TIMEOUT + 5,
            )
            elapsed = time.perf_counter() - t0
            if elapsed > _POSTHOG_SLOW_QUERY_THRESHOLD_SECONDS:
                logger.warning("Slow ___ops query (%.1fs): %s", elapsed, req.sql[:500])
                track_event("query_slow", {"database": "___ops", "elapsed_seconds": round(elapsed, 2)})
            track_event(
                "query_executed",
                {"database": "___ops", "elapsed_seconds": round(elapsed, 2)},
                sample_rate=_POSTHOG_QUERY_SAMPLE_RATE,
            )
            return result
        except TimeoutError as e:
            logger.error("___ops query timeout (30s): %s", req.sql[:500])
            track_event("query_timed_out", {"database": "___ops", "timeout_seconds": PUBLIC_QUERY_TIMEOUT})
            raise HTTPException(status_code=504, detail="Query timed out") from e
        except _duckdb.CatalogException as e:
            raise HTTPException(status_code=500, detail=f"Catalog Error: {e}") from e
        except _duckdb.BinderException as e:
            raise HTTPException(status_code=500, detail=f"Binder Error: {e}") from e
        except Exception as e:
            logger.error("___ops query failed: %s", req.sql[:500], exc_info=True)
            track_event("query_failed", {"database": "___ops", "error": type(e).__name__})
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e

    # ___ops reads use the dedicated connection above and continue on the old snapshot during an
    # online rebuild. League reads remain conservative for ordinary ___ops writes, which still
    # use the legacy close/reopen path.
    if _state["status"] == "ops_writing" and not await _wait_for_ops_write_to_finish():
        return _query_busy_response(reason=_state["status"])
    if _state["status"] not in {"serving", "ops_snapshotting"}:
        return _query_busy_response(reason=_state["status"])

    await _acquire_query_slot(req.database)
    conn = None
    try:
        try:
            conn = db.acquire_connection()
        except RuntimeError as e:
            track_event("query_rejected", {"database": req.database, "reason": "pool_exhausted"})
            raise HTTPException(
                status_code=429,
                detail="Too many concurrent queries",
                headers={"Retry-After": "2"},
            ) from e
        t0 = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(_execute_query, conn, req.sql, req.database),
                timeout=PUBLIC_QUERY_TIMEOUT + 5,
            )
            elapsed = time.perf_counter() - t0
            if elapsed > _POSTHOG_SLOW_QUERY_THRESHOLD_SECONDS:
                logger.warning("Slow query (%.1fs): %s", elapsed, req.sql[:500])
                track_event("query_slow", {"database": req.database, "elapsed_seconds": round(elapsed, 2)})
            track_event(
                "query_executed",
                {"database": req.database, "elapsed_seconds": round(elapsed, 2)},
                sample_rate=_POSTHOG_QUERY_SAMPLE_RATE,
            )
            return result
        except TimeoutError as e:
            logger.error("Query timeout (30s): %s", req.sql[:500])
            track_event("query_timed_out", {"database": req.database, "timeout_seconds": PUBLIC_QUERY_TIMEOUT})
            raise HTTPException(status_code=504, detail="Query timed out") from e
        except _duckdb.CatalogException as e:
            raise HTTPException(status_code=500, detail=f"Catalog Error: {e}") from e
        except _duckdb.BinderException as e:
            raise HTTPException(status_code=500, detail=f"Binder Error: {e}") from e
        except Exception as e:
            logger.error("Query failed on %s: %s", req.database, req.sql[:500], exc_info=True)
            track_event("query_failed", {"database": req.database, "error": type(e).__name__})
            raise HTTPException(status_code=500, detail=f"{type(e).__name__}: {e}") from e
    finally:
        if conn is not None:
            db.release_connection(conn)
        _query_semaphore.release()


@app.post("/query-parquet")
async def query_parquet_endpoint(req: QueryRequest, request: Request):
    """Return a read-only ___ops query as compact Parquet bytes."""
    try:
        validate_read_token(get_bearer_token(request))
    except AuthError as exc:
        raise HTTPException(status_code=401, detail="Unauthorized") from exc
    if req.database != "___ops":
        raise HTTPException(status_code=400, detail="Parquet query transport is limited to ___ops")
    if not is_read_only_sql(req.sql):
        raise HTTPException(status_code=403, detail="Write operations not allowed")
    if _state["status"] == "starting":
        return _query_busy_response(reason="starting")
    try:
        payload = await asyncio.wait_for(
            asyncio.to_thread(_execute_ops_query_parquet, req.sql),
            timeout=PUBLIC_QUERY_TIMEOUT + 5,
        )
        return Response(
            content=payload,
            media_type="application/vnd.apache.parquet",
            headers={"Content-Encoding": "identity"},
        )
    except TimeoutError as exc:
        raise HTTPException(status_code=504, detail="Query timed out") from exc
    except _duckdb.CatalogException as exc:
        raise HTTPException(status_code=500, detail=f"Catalog Error: {exc}") from exc
    except _duckdb.BinderException as exc:
        raise HTTPException(status_code=500, detail=f"Binder Error: {exc}") from exc
    except Exception as exc:
        logger.error("___ops Parquet query failed: %s", req.sql[:500], exc_info=True)
        raise HTTPException(status_code=500, detail=f"{type(exc).__name__}: {exc}") from exc


@app.post("/query-rw")
async def query_rw_endpoint(req: QueryRequest, request: Request):
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/query-rw"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if not _DB_NAME_PATTERN.fullmatch(req.database):
        raise HTTPException(status_code=400, detail="Invalid database name")

    if _state["status"] == "starting":
        if not await _wait_for_startup_before_write():
            return _query_busy_response(reason="starting")

    if _state["status"] not in {"serving", "ops_writing"}:
        return _query_busy_response(reason=_state["status"])

    if req.database == "___ops":
        # ___ops writes need to briefly close the read pool because public
        # connections attach ___ops read-only, but they do not mutate
        # ___leagues and must not sit in the same global queue as league
        # publishes. The thread-side _ops_lock still serializes the actual
        # DuckDB ___ops writer.
        t0_rw = time.perf_counter()
        try:
            result = await asyncio.wait_for(
                asyncio.to_thread(_execute_ops_query_rw_serialized, req.sql),
                timeout=ADMIN_QUERY_TIMEOUT + 5,
            )
            track_event(
                "query_rw_executed",
                {"database": "___ops", "elapsed_seconds": round(time.perf_counter() - t0_rw, 2)},
            )
            return result
        except TimeoutError as e:
            logger.error("___ops read-write query timeout (%ss): %s", ADMIN_QUERY_TIMEOUT, req.sql[:500])
            raise HTTPException(status_code=504, detail="Query timed out") from e

    async with _merge_lock:
        _state["status"] = "draining"
        elapsed = 0
        while db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
            await asyncio.sleep(0.5)
            elapsed += 0.5
        db.close_all()

        t0_rw = time.perf_counter()
        try:
            result = await asyncio.to_thread(_execute_query_rw, req.sql, req.database)
            track_event(
                "query_rw_executed",
                {"database": req.database, "elapsed_seconds": round(time.perf_counter() - t0_rw, 2)},
            )
            return result
        except TimeoutError as e:
            logger.error("Read-write query timeout (%ss): %s", ADMIN_QUERY_TIMEOUT, req.sql[:500])
            raise HTTPException(status_code=504, detail="Query timed out") from e
        finally:
            await _reopen_pool_after_write("read-write query")
            _set_serving_or_ops_writing()


def _json_safe_row(row: dict) -> dict:
    """Convert DuckDB non-finite numeric values to JSON-safe SQL NULLs."""
    return {
        key: None if isinstance(value, float) and not math.isfinite(value) else value
        for key, value in row.items()
    }


def _execute_with_timeout(conn, sql: str, timeout_seconds: float) -> list[dict]:
    """Execute query with hard wall-clock timeout via interrupt()."""
    timer = threading.Timer(timeout_seconds, conn.interrupt)
    timer.start()
    try:
        result = conn.execute(sql)
        if result.description is None:
            return []
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [_json_safe_row(dict(zip(columns, row))) for row in rows]
    except _duckdb.InterruptException as exc:
        raise TimeoutError(f"Query exceeded {timeout_seconds}s wall-clock limit") from exc
    finally:
        timer.cancel()


def _execute_script_with_timeout(conn, sql: str, timeout_seconds: float) -> list[dict]:
    """Execute one or more write statements with a hard wall-clock timeout."""
    timer = threading.Timer(timeout_seconds, conn.interrupt)
    timer.start()
    try:
        result = None
        # DuckDB versions differ on multi-statement conn.execute() behavior.
        # query-rw is admin-only, and our maintenance scripts do not contain
        # semicolon-bearing literals, so a small explicit splitter keeps the
        # endpoint predictable for staged migrations.
        statements = [stmt.strip() for stmt in sql.split(";") if stmt.strip()]
        for statement in statements:
            result = conn.execute(statement)
        if result is None or result.description is None:
            return []
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        return [_json_safe_row(dict(zip(columns, row))) for row in rows]
    except _duckdb.InterruptException as exc:
        raise TimeoutError(f"Query exceeded {timeout_seconds}s wall-clock limit") from exc
    finally:
        timer.cancel()


def _interrupting_execute(
    conn, sql: str, params=None, *, step: str, timeout_seconds: float = MERGE_STEP_TIMEOUT_SECONDS
):
    """Execute a merge write step with a DuckDB interrupt timer."""

    def interrupt() -> None:
        logger.error("Merge step timed out after %.1fs: %s", timeout_seconds, step)
        try:
            conn.interrupt()
        except Exception as exc:
            logger.warning("Failed to interrupt timed-out merge step %s: %s", step, exc)

    timer = threading.Timer(timeout_seconds, interrupt)
    timer.daemon = True
    timer.start()
    try:
        if params is None:
            return conn.execute(sql)
        return conn.execute(sql, params)
    except _duckdb.InterruptException as exc:
        raise TimeoutError(f"Merge step exceeded {timeout_seconds:.1f}s: {step}") from exc
    finally:
        timer.cancel()


def _start_merge_hard_exit_timer(db_name: str, seconds: float | None = None) -> threading.Timer:
    """Restart the process if native DuckDB gets stuck inside a merge call."""
    budget = MERGE_HARD_EXIT_SECONDS if seconds is None else seconds

    def hard_exit() -> None:
        logger.critical(
            "League merge for %s exceeded %.1fs; exiting so Fly restarts the writer",
            db_name,
            budget,
        )
        os._exit(1)

    timer = threading.Timer(budget, hard_exit)
    timer.daemon = True
    timer.start()
    return timer


def _execute_ops_query(sql: str) -> list[dict]:
    """Execute a query on the dedicated ___ops connection (thread-safe via lock)."""
    import db as _db

    with _db._ops_lock:
        conn = _db.get_ops_connection() or _db.reopen_ops_connection()
        if conn is None:
            raise RuntimeError("___ops connection is unavailable")
        return _execute_with_timeout(conn, sql, PUBLIC_QUERY_TIMEOUT)


def _execute_ops_query_parquet(sql: str) -> bytes:
    """Execute one ___ops query and serialize it in Arrow/Parquet, not Python rows."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    import db as _db

    with _db._ops_lock:
        conn = _db.get_ops_connection() or _db.reopen_ops_connection()
        if conn is None:
            raise RuntimeError("___ops connection is unavailable")
        timer = threading.Timer(PUBLIC_QUERY_TIMEOUT, conn.interrupt)
        timer.start()
        try:
            table = conn.execute(sql).to_arrow_table()
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink, compression="zstd")
            return sink.getvalue().to_pybytes()
        except _duckdb.InterruptException as exc:
            raise TimeoutError(f"Query exceeded {PUBLIC_QUERY_TIMEOUT}s wall-clock limit") from exc
        finally:
            timer.cancel()


def _execute_ops_query_rw(sql: str) -> list[dict]:
    """Execute an admin write against ___ops without draining ___leagues reads."""
    import db as _db

    data_dir = _db.get_data_dir()
    ops_path = data_dir / "___ops.duckdb"
    conn = None
    pool_closed = False

    def hard_exit() -> None:
        logger.critical(
            "___ops read-write query exceeded %.1fs; exiting so Fly restarts the writer: %s",
            RW_HARD_EXIT_SECONDS,
            sql[:500],
        )
        os._exit(1)

    hard_timer = threading.Timer(RW_HARD_EXIT_SECONDS, hard_exit)
    hard_timer.daemon = True
    hard_timer.start()
    try:
        with _db._ops_lock:
            _begin_ops_write_state()
            try:
                elapsed = 0
                while _db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
                    time.sleep(0.5)
                    elapsed += 0.5
                _db.close_ops_connection()
                last_connect_error: Exception | None = None
                for attempt in range(8):
                    try:
                        conn = _db.connect_database(ops_path, data_dir=data_dir)
                        break
                    except _duckdb.BinderException as exc:
                        last_connect_error = exc
                        if "unique file handle conflict" not in str(exc).lower() or attempt == 7:
                            raise
                        # A public query can still be detaching ___ops while the
                        # ops write starts. Give DuckDB a short moment to release
                        # the read-only handle. The public pool no longer keeps
                        # ___ops attached while idle, so rebuilding the pool is a
                        # rare fallback rather than the normal path.
                        time.sleep(0.25 * (attempt + 1))
                        while _db.get_active_count() > 0 and elapsed < HARD_DRAIN_TIMEOUT:
                            time.sleep(0.5)
                            elapsed += 0.5
                        _db.close_ops_connection()
                        if attempt >= 3 and not pool_closed:
                            logger.warning("Closing public pool during ___ops write after repeated handle conflicts")
                            _db.close_pool()
                            pool_closed = True
                if conn is None:
                    raise RuntimeError(f"Unable to open ___ops writer: {last_connect_error}")
                _db._attach_ops_nfl(conn)  # so admin writes (e.g. the one-time view cutover) can bind ___ops_nfl
                result = _execute_script_with_timeout(conn, sql, ADMIN_QUERY_TIMEOUT)
                _checkpoint_connection_if_wal_large(conn, ops_path, reason="___ops write")
                return result
            finally:
                cleanup_error: Exception | None = None
                try:
                    if conn is not None:
                        conn.close()
                except Exception as exc:
                    cleanup_error = exc
                    logger.exception("Failed closing ___ops writer")
                reopen_actions = [("reopen ___ops connection", _db.reopen_ops_connection)]
                if pool_closed:
                    reopen_actions.append(("reopen ___leagues pool", _db.reopen_pool))
                reopen_actions.append(("refresh DuckDB metadata", _db.refresh_metadata))
                for label, action in reopen_actions:
                    try:
                        action()
                    except Exception as exc:
                        if cleanup_error is None:
                            cleanup_error = exc
                        logger.exception("Failed to %s after ___ops write", label)
                try:
                    if cleanup_error is not None:
                        raise cleanup_error
                finally:
                    _end_ops_write_state()
    finally:
        hard_timer.cancel()


def _execute_ops_query_rw_serialized(sql: str) -> list[dict]:
    """Run an ___ops write without racing an online replacement snapshot."""
    with _ops_rebuild_lock:
        return _execute_ops_query_rw(sql)


_OPS_MERGE_SCHEMA = "nfl_historical"
_OPS_MERGE_IDENT = re.compile(r"^[A-Za-z0-9_]+$")


def _merge_ops_tables(incoming_path: Path) -> dict:
    """Build and atomically hand off a new ___ops snapshot.

    Rebuilding the incoming tables happens against a staging copy while the old dedicated read
    connection remains available. Only the final close/rename/reopen is serialized with readers.
    """
    import db as _db

    data_dir = _db.get_data_dir()
    ops_path = data_dir / "___ops.duckdb"
    staging_path = data_dir / f"___ops.merge.{time.time_ns()}.duckdb"
    conn = None

    def hard_exit() -> None:
        logger.critical("/merge-ops exceeded %.1fs; exiting so Fly restarts the writer", OPS_MERGE_HARD_EXIT)
        os._exit(1)

    hard_timer = threading.Timer(OPS_MERGE_HARD_EXIT, hard_exit)
    hard_timer.daemon = True
    hard_timer.start()
    result: dict | None = None
    try:
        with _ops_rebuild_lock:
            # Briefly gate ___ops readers for a consistent checkpoint. Once the
            # staging copy exists, reads resume against the old snapshot.
            _begin_ops_write_state(snapshot=False)
            try:
                _drain_ops_attachments_for_snapshot()
                with _db._ops_lock:
                    _db.close_ops_connection()
                    _db.close_pool()
                    checkpoint_conn = _db.connect_database(ops_path, data_dir=data_dir)
                    try:
                        checkpoint_conn.execute("CHECKPOINT")
                    finally:
                        checkpoint_conn.close()
                    _db.reopen_ops_connection()
                    _db.reopen_pool()

                # The expensive copy and table rebuild run with the old snapshot
                # queryable. Only the final handoff below gates readers again.
                _state["status"] = "ops_snapshotting"

                shutil.copy2(ops_path, staging_path)
                conn = _db.connect_database(staging_path, data_dir=data_dir, threads=WRITE_DUCKDB_THREADS)

                conn.execute(f"ATTACH '{incoming_path.as_posix()}' AS _incoming (READ_ONLY)")
                try:
                    rows = conn.execute(
                        "SELECT table_schema, table_name FROM information_schema.tables "
                        "WHERE table_catalog = '_incoming' AND table_type = 'BASE TABLE' "
                        "ORDER BY table_name"
                    ).fetchall()
                    if not rows:
                        raise RuntimeError("uploaded bundle contains no tables")
                    merged: dict[str, int] = {}
                    conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{_OPS_MERGE_SCHEMA}"')
                    for schema, table in rows:
                        if not _OPS_MERGE_IDENT.match(str(table)):
                            raise RuntimeError(f"unsafe table name in bundle: {table!r}")
                        tgt = f'"{_OPS_MERGE_SCHEMA}"."{table}"'
                        src = f'_incoming."{schema}"."{table}"'
                        _execute_script_with_timeout(
                            conn, f"CREATE OR REPLACE TABLE {tgt} AS SELECT * FROM {src}", OPS_MERGE_TIMEOUT
                        )
                        n = conn.execute(f"SELECT COUNT(*) FROM {tgt}").fetchone()[0]
                        merged[str(table)] = int(n)
                        logger.info("/merge-ops replaced %s.%s -> %d rows", _OPS_MERGE_SCHEMA, table, n)
                finally:
                    try:
                        conn.execute("DETACH _incoming")
                    except Exception:
                        logger.exception("/merge-ops failed to DETACH incoming bundle")
                _checkpoint_connection_if_wal_large(conn, staging_path, reason="merge-ops staging", force=True)
                result = {"status": "merged", "database": "___ops", "tables": merged}
            finally:
                cleanup_error: Exception | None = None
                try:
                    if conn is not None:
                        conn.close()
                except Exception as exc:
                    cleanup_error = exc
                    logger.exception("Failed closing ___ops writer after /merge-ops")
                if cleanup_error is None and result is not None:
                    try:
                        # The old snapshot was online while the staging file was built. This is
                        # the final exclusive handoff for in-flight ___ops reads.
                        _state["status"] = "ops_writing"
                        _drain_ops_attachments_for_snapshot()
                        with _db._ops_lock:
                            _db.close_ops_connection()
                            _db.close_pool()
                            os.replace(staging_path, ops_path)
                            _db.reopen_ops_connection()
                            _db.reopen_pool()
                            _db.refresh_metadata()
                    except Exception as exc:
                        cleanup_error = exc
                        logger.exception("Failed to hand off rebuilt ___ops snapshot")
                elif _db.get_ops_connection() is None:
                    try:
                        with _db._ops_lock:
                            _db.reopen_ops_connection()
                    except Exception as exc:
                        cleanup_error = exc
                        logger.exception("Failed to restore ___ops read connection")
                staging_path.unlink(missing_ok=True)
                try:
                    if cleanup_error is not None:
                        raise cleanup_error
                finally:
                    _end_ops_write_state()
            return result
    finally:
        hard_timer.cancel()


_OPS_REFERENCE_PATTERN = re.compile(r'(?<![\w])"?___ops"?(?=\s*\.)', re.IGNORECASE)
_ops_attachment_lock = threading.Lock()
_ops_attachment_users = 0


def _query_needs_ops_attachment(sql: str, database: str) -> bool:
    if database == "___ops":
        return True
    scrubbed = _strip_sql_literals_and_comments(sql, strip_double_quoted_identifiers=False)
    return bool(_OPS_REFERENCE_PATTERN.search(scrubbed))


def _acquire_ops_attachment(conn) -> None:
    """Keep the process-wide ___ops catalog attached while any query uses it.

    DuckDB connections opened on the same database file share attached catalogs.
    Without a user count, one pooled query can DETACH ___ops while another pooled
    query is still binding or scanning it.
    """
    global _ops_attachment_users
    ops_path = db.get_data_dir() / "___ops.duckdb"
    if not ops_path.exists():
        raise RuntimeError("___ops database is unavailable")
    with _ops_attachment_lock:
        conn.execute(f'ATTACH IF NOT EXISTS {_sql_literal(ops_path)} AS "___ops" (READ_ONLY)')
        _ops_attachment_users += 1


def _release_ops_attachment(conn) -> None:
    global _ops_attachment_users
    with _ops_attachment_lock:
        if _ops_attachment_users <= 0:
            logger.error("___ops attachment user count underflow")
            _ops_attachment_users = 0
            return
        _ops_attachment_users -= 1
        if _ops_attachment_users == 0:
            try:
                conn.execute('DETACH "___ops"')
            except Exception:
                logger.exception("Failed to detach shared ___ops catalog")


def _drain_ops_attachments_for_snapshot(timeout_seconds: float = QUERY_OPS_WRITE_WAIT_SECONDS) -> None:
    """Wait for read-pool ___ops attachments before opening or replacing its file.

    DuckDB shares attachments across connections to the same database instance. An
    active public query can therefore keep the old ___ops file attached even after
    the dedicated ___ops read connection is closed. Snapshot handoff must drain
    those readers at its two short exclusive boundaries; staging remains online.
    """
    deadline = time.perf_counter() + timeout_seconds
    while True:
        with _ops_attachment_lock:
            active = _ops_attachment_users
        if active == 0:
            return
        if time.perf_counter() >= deadline:
            raise RuntimeError(
                f"timed out waiting for {active} public ___ops attachment(s) to drain before snapshot handoff"
            )
        time.sleep(0.05)


def _execute_query(conn, sql: str, database: str = "___leagues") -> list[dict]:
    # Set the active database so unqualified table references resolve correctly
    switched = False
    attached_ops = False
    needs_ops = _query_needs_ops_attachment(sql, database)
    if needs_ops:
        _acquire_ops_attachment(conn)
        attached_ops = True
    if database and database != "___leagues":
        try:
            conn.execute(f'USE "{database}"')
            switched = True
        except Exception:
            pass
    try:
        return _execute_with_timeout(conn, sql, PUBLIC_QUERY_TIMEOUT)
    finally:
        if switched:
            try:
                conn.execute('USE "___leagues"')
            except Exception:
                pass
        if attached_ops:
            _release_ops_attachment(conn)


def _execute_query_rw(sql: str, database: str = "___leagues") -> list[dict]:
    data_dir = db.get_data_dir()
    db_path = data_dir / f"{database}.duckdb"
    conn = None

    def hard_exit() -> None:
        logger.critical(
            "Read-write query exceeded %.1fs; exiting so Fly restarts the writer: %s",
            RW_HARD_EXIT_SECONDS,
            sql[:500],
        )
        os._exit(1)

    hard_timer = threading.Timer(RW_HARD_EXIT_SECONDS, hard_exit)
    hard_timer.daemon = True
    hard_timer.start()
    try:
        # This endpoint can run while the public pool is open. Use the same
        # DuckDB connection config as the pool so concurrent connections to
        # ___leagues.duckdb are compatible.
        conn = db.connect_database(db_path, data_dir=data_dir)
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        result = _execute_script_with_timeout(conn, sql, ADMIN_QUERY_TIMEOUT)
        return result
    finally:
        hard_timer.cancel()
        if conn is not None:
            conn.close()


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "state": _state["status"],
        "databases": db.get_metadata(),
    }


@app.get("/ready")
async def ready():
    # ___ops replacement is snapshot-based: the old read snapshot remains online while the new
    # file is built. Keep Fly routing traffic during that bounded handoff instead of advertising a
    # machine outage for every research publish.
    if _state["status"] not in {"serving", "ops_snapshotting"}:
        payload = {
            "status": _state["status"],
            "ready": False,
            "accepting_queries": False,
            "role": os.environ.get("FLY_ROLE", "primary"),
            "active_queries": db.get_active_count(),
            "ops_writes": _ops_write_count,
            "delta_publishes": _delta_publish_inflight,
            "delta_publish_capacity": MAX_INFLIGHT_DELTA_PUBLISHES,
            "pool_size": db.POOL_SIZE,
        }
        return Response(
            content=json.dumps(payload),
            media_type="application/json",
            status_code=503,
            headers={"Retry-After": "2"},
        )
    meta = db.get_metadata()
    return {
        "status": _state["status"],
        "ready": True,
        "accepting_queries": True,
        "role": os.environ.get("FLY_ROLE", "primary"),
        "active_queries": db.get_active_count(),
        "ops_writes": _ops_write_count,
        "delta_publishes": _delta_publish_inflight,
        "delta_publish_capacity": MAX_INFLIGHT_DELTA_PUBLISHES,
        "pool_size": db.POOL_SIZE,
        "league_file_hash": meta.get("___leagues", {}).get("file_hash", "unknown"),
        "databases": meta,
    }


@app.get("/internal/server-state")
async def server_state():
    meta = db.get_metadata()
    try:
        memory_info = await asyncio.wait_for(asyncio.to_thread(_get_duckdb_memory_info), timeout=0.5)
    except TimeoutError:
        memory_info = {"error": "pool busy"}
    return {
        "machine_id": os.environ.get("FLY_MACHINE_ID", "unknown"),
        "role": os.environ.get("FLY_ROLE", "primary"),
        "state": _state["status"],
        "active_queries": db.get_active_count(),
        "query_capacity": MAX_CONCURRENT_QUERIES,
        "query_queue_timeout_seconds": QUERY_QUEUE_TIMEOUT_SECONDS,
        "query_ops_write_wait_seconds": QUERY_OPS_WRITE_WAIT_SECONDS,
        "pool_size": db.POOL_SIZE,
        "ops_writes": _ops_write_count,
        "delta_publishes": _delta_publish_inflight,
        "delta_publish_capacity": MAX_INFLIGHT_DELTA_PUBLISHES,
        "delta_admission_timeout_seconds": DELTA_ADMISSION_TIMEOUT_SECONDS,
        "delta_busy_retry_after_seconds": DELTA_BUSY_RETRY_AFTER_SECONDS,
        "active_delta_publishes": _active_delta_publish_snapshot(),
        "league_file_hash": meta.get("___leagues", {}).get("file_hash", "unknown"),
        "databases": meta,
        "duckdb_memory": memory_info,
        "duckdb_config": db.get_runtime_config(),
    }


# Full database replacement is admin-only and currently needs to accommodate
# the verified wide NFL Ops snapshot. Keep the ceiling bounded (the smaller
# delta lane has its own independent limit), but leave enough headroom for an
# artifact that has outgrown the legacy 5 GiB cap.
MAX_UPLOAD_BYTES = int(os.environ.get("MAX_FULL_DATABASE_UPLOAD_BYTES", str(8 * 1024 * 1024 * 1024)))
MAX_CANONICAL_TABLE_REPLACEMENT_BYTES = int(
    os.environ.get("MAX_CANONICAL_TABLE_REPLACEMENT_BYTES", str(3 * 1024 * 1024 * 1024))
)
_FILE_PARAM = File(...)

_CANONICAL_TABLE_REPLACEMENTS = {
    ("___leagues", "league_settings"): ("db_name", "year"),
    ("___leagues", "homepage_manager_rankings"): ("db_name", "franchise_id"),
    ("___leagues", "matchup_h2h_career"): (
        "db_name",
        "franchise_id",
        "opponent_franchise_id",
    ),
    ("___leagues", "player_fantasy_season"): ("db_name", "NFL_player_id", "year"),
    ("___leagues", "player_fantasy_season_all"): ("db_name", "NFL_player_id", "year"),
    ("___leagues", "standings_by_year"): ("db_name", "franchise_id", "year"),
}


def _quote_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _checkpoint_result(conn) -> tuple[bool, str | None]:
    """Checkpoint without turning a committed write into an ambiguous failure."""
    try:
        conn.execute("CHECKPOINT")
    except Exception as exc:
        return False, str(exc)
    return True, None


def _replace_canonical_table(
    database_path: Path,
    incoming_path: Path,
    *,
    database_name: str,
    table_name: str,
    expected_rows: int,
    recovery_since: str | None = None,
    expected_overlay_leagues: int | None = None,
    expected_overlay_rows: int | None = None,
) -> dict[str, Any]:
    """Atomically replace one explicitly allowlisted canonical table.

    This is a storage-recovery primitive, not an import path.  The uploaded
    bundle must contain exactly the target table with the live table's exact
    column names/types.  The live DDL is reused so constraints and indexes are
    preserved across the transactional rename.
    """
    key_columns = _CANONICAL_TABLE_REPLACEMENTS.get((database_name, table_name))
    if key_columns is None:
        raise ValueError("canonical table replacement target is not allowlisted")
    if expected_rows <= 0:
        raise ValueError("x-expected-rows must be a positive integer")

    conn = None
    attached = False
    replacement_name = f"__recovered_{table_name}_{time.time_ns()}"
    replaced_name = f"__replaced_{table_name}_{time.time_ns()}"
    target_ref = f"public.{_quote_identifier(table_name)}"
    replacement_ref = f"public.{_quote_identifier(replacement_name)}"
    replaced_ref = f"public.{_quote_identifier(replaced_name)}"
    incoming_ref = f"_incoming.public.{_quote_identifier(table_name)}"

    try:
        conn = db.connect_database(
            database_path,
            data_dir=db.get_data_dir(),
            threads=WRITE_DUCKDB_THREADS,
        )
        conn.execute(f"ATTACH '{incoming_path.as_posix()}' AS _incoming (READ_ONLY)")
        attached = True

        incoming_tables = conn.execute(
            "SELECT schema, name FROM (SHOW ALL TABLES) "
            "WHERE database = '_incoming' ORDER BY schema, name"
        ).fetchall()
        if incoming_tables != [("public", table_name)]:
            raise ValueError(
                "replacement bundle must contain exactly one public table named "
                f"{table_name}; found {incoming_tables!r}"
            )

        target_row = conn.execute(
            "SELECT sql FROM duckdb_tables() "
            "WHERE database_name = ? AND schema_name = 'public' AND table_name = ?",
            [database_name, table_name],
        ).fetchone()
        if not target_row or not target_row[0]:
            raise ValueError(f"target table public.{table_name} does not exist")
        target_ddl = str(target_row[0])

        target_schema = [(row[0], str(row[1]).upper()) for row in conn.execute(f"DESCRIBE {target_ref}").fetchall()]
        incoming_schema = [
            (row[0], str(row[1]).upper()) for row in conn.execute(f"DESCRIBE {incoming_ref}").fetchall()
        ]
        if incoming_schema != target_schema:
            raise ValueError(
                "replacement table schema does not exactly match the live canonical schema"
            )

        incoming_rows = int(conn.execute(f"SELECT COUNT(*) FROM {incoming_ref}").fetchone()[0])
        if recovery_since is None and incoming_rows != expected_rows:
            raise ValueError(
                f"replacement row count {incoming_rows} does not match expected {expected_rows}"
            )

        keys = ", ".join(_quote_identifier(column) for column in key_columns)
        null_predicate = " OR ".join(
            f"{_quote_identifier(column)} IS NULL" for column in key_columns
        )
        null_keys = int(
            conn.execute(f"SELECT COUNT(*) FROM {incoming_ref} WHERE {null_predicate}").fetchone()[0]
        )
        duplicate_keys = int(
            conn.execute(
                f"SELECT COUNT(*) FROM (SELECT {keys}, COUNT(*) AS n FROM {incoming_ref} "
                f"GROUP BY {keys} HAVING COUNT(*) <> 1)"
            ).fetchone()[0]
        )
        if null_keys or duplicate_keys:
            raise ValueError(
                "replacement table has invalid canonical keys: "
                f"null={null_keys}, duplicate={duplicate_keys}"
            )

        replacement_ddl = re.sub(
            rf"(?i)^CREATE TABLE\s+(?:\"?public\"?\.)?\"?{re.escape(table_name)}\"?",
            f"CREATE TABLE public.{_quote_identifier(replacement_name)}",
            target_ddl,
            count=1,
        )
        if replacement_ddl == target_ddl:
            raise ValueError("could not derive replacement DDL from the live canonical table")

        conn.execute(replacement_ddl)
        conn.execute(f"INSERT INTO {replacement_ref} BY NAME SELECT * FROM {incoming_ref}")

        overlay_names: list[str] = []
        overlay_rows = 0
        ops_attached = False
        if recovery_since is not None:
            try:
                conn.execute("SELECT CAST(? AS TIMESTAMP)", [recovery_since]).fetchone()
            except Exception as exc:
                raise ValueError("x-recovery-since must be an ISO timestamp") from exc

            context_table = conn.execute(
                "SELECT 1 FROM duckdb_tables() "
                "WHERE database_name = ? AND schema_name = 'public' "
                "AND table_name = 'league_context'",
                [database_name],
            ).fetchone()
            if not context_table:
                raise ValueError("live database has no public.league_context recovery ledger")
            names = {
                str(row[0])
                for row in conn.execute(
                    "SELECT DISTINCT db_name FROM public.league_context "
                    "WHERE updated_at >= CAST(? AS TIMESTAMP) AND db_name IS NOT NULL",
                    [recovery_since],
                ).fetchall()
            }

            ops_path = db.get_data_dir() / "___ops.duckdb"
            if ops_path.exists():
                conn.execute(f"ATTACH '{ops_path.as_posix()}' AS _ops_recovery (READ_ONLY)")
                ops_attached = True
                dispatch_table = conn.execute(
                    "SELECT 1 FROM duckdb_tables() "
                    "WHERE database_name = '_ops_recovery' AND schema_name = 'accounts' "
                    "AND table_name = 'league_update_dispatches'"
                ).fetchone()
                if dispatch_table:
                    names.update(
                        str(row[0])
                        for row in conn.execute(
                            "SELECT DISTINCT database_name "
                            "FROM _ops_recovery.accounts.league_update_dispatches "
                            "WHERE updated_at >= CAST(? AS TIMESTAMP) "
                            "AND database_name IS NOT NULL",
                            [recovery_since],
                        ).fetchall()
                    )

            overlay_names = sorted(name for name in names if name)
            if expected_overlay_leagues is None or expected_overlay_rows is None:
                raise ValueError(
                    "recovery overlay requires expected league and row counts"
                )
            if len(overlay_names) != expected_overlay_leagues:
                raise ValueError(
                    "recovery overlay league count does not match expectation: "
                    f"actual={len(overlay_names)}, expected={expected_overlay_leagues}"
                )

            for db_name in overlay_names:
                conn.execute(
                    f"DELETE FROM {replacement_ref} WHERE db_name = ?",
                    [db_name],
                )
                current_rows = int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM {target_ref} WHERE db_name = ?",
                        [db_name],
                    ).fetchone()[0]
                )
                conn.execute(
                    f"INSERT INTO {replacement_ref} BY NAME "
                    f"SELECT * FROM {target_ref} WHERE db_name = ?",
                    [db_name],
                )
                overlay_rows += current_rows
            if overlay_rows != expected_overlay_rows:
                raise ValueError(
                    "recovery overlay row count does not match expectation: "
                    f"actual={overlay_rows}, expected={expected_overlay_rows}"
                )
            if ops_attached:
                conn.execute("DETACH _ops_recovery")
                ops_attached = False

        staged_rows = int(conn.execute(f"SELECT COUNT(*) FROM {replacement_ref}").fetchone()[0])
        if staged_rows != expected_rows:
            raise ValueError(
                f"staged replacement row count {staged_rows} does not match expected {expected_rows}"
            )

        conn.execute("BEGIN TRANSACTION")
        try:
            conn.execute(
                f"ALTER TABLE {target_ref} RENAME TO {_quote_identifier(replaced_name)}"
            )
            conn.execute(
                f"ALTER TABLE {replacement_ref} RENAME TO {_quote_identifier(table_name)}"
            )
            conn.execute(f"DROP TABLE {replaced_ref}")
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        final_rows = int(conn.execute(f"SELECT COUNT(*) FROM {target_ref}").fetchone()[0])
        final_distinct = int(
            conn.execute(
                f"SELECT COUNT(*) FROM (SELECT {keys} FROM {target_ref} GROUP BY {keys})"
            ).fetchone()[0]
        )
        if final_rows != expected_rows or final_distinct != expected_rows:
            raise RuntimeError(
                "post-replacement validation failed: "
                f"rows={final_rows}, distinct_keys={final_distinct}, expected={expected_rows}"
            )
        checkpointed, checkpoint_error = _checkpoint_result(conn)
        return {
            "status": "replaced",
            "database": database_name,
            "table": table_name,
            "rows": final_rows,
            "distinct_keys": final_distinct,
            "snapshot_rows": incoming_rows,
            "overlay_leagues": len(overlay_names),
            "overlay_rows": overlay_rows,
            "checkpointed": checkpointed,
            "checkpoint_error": checkpoint_error,
        }
    finally:
        if conn is not None:
            with suppress(Exception):
                if attached:
                    conn.execute("DETACH _incoming")
            with suppress(Exception):
                conn.execute(f"DROP TABLE IF EXISTS {replacement_ref}")
            conn.close()


def _rebuild_league_derived_from_sources(
    database_path: Path,
    *,
    db_name: str,
    run_id: str,
) -> dict[str, Any]:
    """Rebuild one league's canonical derived outputs from persisted source facts.

    This is an admin recovery primitive for a damaged derived table, not an
    alternate import path. It calls the same complete-chain aggregators used by
    fleet publication and never fetches provider data or rewrites source facts.
    ``run_id`` is persisted in the normal generation ledger so a retry after an
    ambiguous HTTP response is idempotent.
    """
    if not _DB_NAME_PATTERN.fullmatch(db_name):
        raise ValueError("invalid league database name")
    if not run_id or len(run_id) > 200:
        raise ValueError("run_id is required and must be at most 200 characters")

    conn = db.connect_database(
        database_path,
        data_dir=db.get_data_dir(),
        threads=WRITE_DUCKDB_THREADS,
    )
    committed = False
    try:
        fleet_merge.ensure_generation_tables(conn)
        prior = conn.execute(
            "SELECT generation, lane, run_id FROM merge_admin.league_publish_generations "
            "WHERE db_name = ?",
            [db_name],
        ).fetchone()
        if prior and str(prior[1] or "") == "derived_recovery" and str(prior[2] or "") == run_id:
            return {
                "status": "ALREADY_COMMITTED",
                "db_name": db_name,
                "generation": int(prior[0]),
                "run_id": run_id,
                "checkpointed": True,
            }

        from multi_league.transformations.aggregation.aggregate_standings import (
            aggregate_standings,
        )
        from multi_league.transformations.aggregation.aggregation_utils import (
            aggregate_career_rollups,
            aggregate_complete_chain_season_rollups,
            aggregate_homepage_rollups,
        )

        source_counts = {
            table: int(
                conn.execute(
                    f"SELECT COUNT(*) FROM public.{_quote_identifier(table)} WHERE db_name = ?",
                    [db_name],
                ).fetchone()[0]
            )
            for table in ("matchup", "player_fantasy", "league_settings")
        }
        if any(source_counts[table] <= 0 for table in source_counts):
            raise ValueError(
                f"cannot rebuild {db_name}: required persisted source facts are missing {source_counts}"
            )
        years = [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT TRY_CAST(year AS INTEGER) FROM public.matchup "
                "WHERE db_name = ? AND TRY_CAST(year AS INTEGER) IS NOT NULL ORDER BY 1",
                [db_name],
            ).fetchall()
        ]
        if not years:
            raise ValueError(f"cannot rebuild {db_name}: matchup history has no seasons")

        conn.execute("BEGIN TRANSACTION")
        try:
            season_rollups = aggregate_complete_chain_season_rollups(conn, db_name)
            career_rollups = aggregate_career_rollups(conn, db_name)
            aggregate_standings(conn, db_name, years)
            homepage_rollups = aggregate_homepage_rollups(conn, db_name)
            target_counts = {
                table: int(
                    conn.execute(
                        f"SELECT COUNT(*) FROM public.{_quote_identifier(table)} WHERE db_name = ?",
                        [db_name],
                    ).fetchone()[0]
                )
                for table in (
                    "homepage_manager_rankings",
                    "matchup_h2h_career",
                    "player_fantasy_season",
                    "player_fantasy_season_all",
                    "standings_by_year",
                )
            }
            empty_targets = sorted(table for table, count in target_counts.items() if count <= 0)
            if empty_targets:
                raise RuntimeError(
                    f"derived recovery produced empty required tables for {db_name}: {empty_targets}"
                )
            fleet_merge.bump_generations(
                conn,
                [db_name],
                lane="derived_recovery",
                run_id=run_id,
            )
            generation = fleet_merge.current_generations(conn, [db_name])[db_name]
            conn.execute("COMMIT")
            committed = True
        except Exception:
            conn.execute("ROLLBACK")
            raise

        # The publication is already committed. Return an explicit durable
        # state instead of converting a post-commit maintenance failure into
        # an ambiguous HTTP 500 that callers might blindly retry.
        checkpointed, checkpoint_error = _checkpoint_result(conn)
        return {
            "status": "COMMITTED",
            "db_name": db_name,
            "run_id": run_id,
            "generation": generation,
            "checkpointed": checkpointed,
            "checkpoint_error": checkpoint_error,
            "source_counts": source_counts,
            "target_counts": target_counts,
            "season_rollups": season_rollups,
            "career_rollups": career_rollups,
            "homepage_rollups": homepage_rollups,
        }
    except Exception:
        if committed:
            logger.exception("Post-commit derived recovery failure for %s", db_name)
        raise
    finally:
        conn.close()


def _rename_league_server_side(
    *,
    data_dir: Path,
    source_db: str,
    target_db: str,
    display_name: str,
    operation_id: str,
) -> dict[str, Any]:
    """Converge league data and control-plane identity without downloading a snapshot."""
    from multi_league.core.delta_publish import canonical_table_registry
    from multi_league.core.league_rename import (
        consolidate_canonical_league,
        retarget_league_control_plane,
        validate_control_plane_rename,
    )

    leagues_path = data_dir / "___leagues.duckdb"
    ops_path = data_dir / "___ops.duckdb"
    if not leagues_path.exists() or not ops_path.exists():
        raise ValueError("league rename requires both ___leagues and ___ops databases")

    # Collision validation is read-only and must happen before either database
    # is changed. A partial prior rename is accepted only when inventory already
    # points both aliases at the same canonical target.
    ops_conn = db.connect_database(ops_path, data_dir=data_dir, threads=WRITE_DUCKDB_THREADS)
    try:
        validate_control_plane_rename(
            ops_conn,
            source_db=source_db,
            target_db=target_db,
        )
        leagues_conn = db.connect_database(
            leagues_path,
            data_dir=data_dir,
            threads=WRITE_DUCKDB_THREADS,
        )
        try:
            data_result = consolidate_canonical_league(
                leagues_conn,
                source_db=source_db,
                target_db=target_db,
                display_name=display_name,
                operation_id=operation_id,
                registry=canonical_table_registry(),
            )
            if data_result.get("status") == "ALREADY_CONSOLIDATED":
                data_checkpointed, data_checkpoint_error = False, None
            else:
                data_checkpointed, data_checkpoint_error = _checkpoint_result(leagues_conn)
        finally:
            leagues_conn.close()

        control_result = retarget_league_control_plane(
            ops_conn,
            source_db=source_db,
            target_db=target_db,
            display_name=display_name,
            operation_id=operation_id,
        )
        if control_result.get("status") == "ALREADY_COMMITTED":
            ops_checkpointed, ops_checkpoint_error = False, None
        else:
            ops_checkpointed, ops_checkpoint_error = _checkpoint_result(ops_conn)
    finally:
        ops_conn.close()

    return {
        "status": "COMMITTED",
        "operation_id": operation_id,
        "source_db": source_db,
        "target_db": target_db,
        "target_years": data_result.get("target_years", []),
        "data_status": data_result.get("status"),
        "control_status": control_result.get("status"),
        "data_checkpointed": data_checkpointed,
        "data_checkpoint_error": data_checkpoint_error,
        "ops_checkpointed": ops_checkpointed,
        "ops_checkpoint_error": ops_checkpoint_error,
    }


def _reaggregate_damaged_derived_from_sources(
    database_path: Path,
    *,
    mode: str,
) -> dict[str, Any]:
    """Rebuild only the five damaged aggregate tables from persisted facts."""
    from fly_reaggregate_derived import _attach_if_present, quarantine_corrupt_targets, reaggregate_all

    conn = db.connect_database(
        database_path,
        data_dir=db.get_data_dir(),
        threads=WRITE_DUCKDB_THREADS,
    )
    try:
        data_dir = db.get_data_dir()
        _attach_if_present(conn, data_dir / "___ops_nfl.duckdb", "___ops_nfl")
        _attach_if_present(conn, data_dir / "___ops.duckdb", "___ops")
        quarantined: dict[str, str] = {}
        if mode == "quarantine_and_rebuild":
            quarantined = quarantine_corrupt_targets(conn)
        elif mode != "resume_rebuild":
            raise ValueError("mode must be quarantine_and_rebuild or resume_rebuild")

        result = reaggregate_all(conn)
        result.update(
            {
                "status": "COMMITTED",
                "mode": mode,
                "quarantined": quarantined,
                "targets": list(_DAMAGED_DERIVED_TARGETS),
            }
        )
        return result
    finally:
        conn.close()


class DeltaValidationError(ValueError):
    """Client-supplied delta bundle is invalid and must not be retried as-is."""


class DeltaConflictError(RuntimeError):
    """Delta bundle conflicts with committed server state."""


def _qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sha256_json(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _delta_state_ref() -> str:
    return f"{_qident(_MERGE_STATE_SCHEMA)}.{_qident(_DELTA_STATE_TABLE)}"


def _ensure_delta_state_table(conn) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_qident(_MERGE_STATE_SCHEMA)}")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {_delta_state_ref()} (
            db_name VARCHAR,
            bundle_id VARCHAR,
            bundle_hash VARCHAR,
            import_run_id VARCHAR,
            publish_sequence BIGINT,
            status VARCHAR,
            received_at TIMESTAMP,
            updated_at TIMESTAMP,
            committed_at TIMESTAMP,
            import_mode VARCHAR,
            platform VARCHAR,
            manifest_json VARCHAR,
            result_json VARCHAR,
            error_type VARCHAR,
            error_message VARCHAR
        )
        """
    )


def _delta_state_row(conn, db_name: str, bundle_id: str) -> dict | None:
    row = conn.execute(
        f"""
        SELECT
            db_name,
            bundle_id,
            bundle_hash,
            import_run_id,
            publish_sequence,
            status,
            CAST(received_at AS VARCHAR) AS received_at,
            CAST(updated_at AS VARCHAR) AS updated_at,
            CAST(committed_at AS VARCHAR) AS committed_at,
            import_mode,
            platform,
            result_json,
            error_type,
            error_message
        FROM {_delta_state_ref()}
        WHERE db_name = ? AND bundle_id = ?
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        [db_name, bundle_id],
    ).fetchone()
    if row is None:
        return None
    columns = [
        "db_name",
        "bundle_id",
        "bundle_hash",
        "import_run_id",
        "publish_sequence",
        "status",
        "received_at",
        "updated_at",
        "committed_at",
        "import_mode",
        "platform",
        "result_json",
        "error_type",
        "error_message",
    ]
    payload = dict(zip(columns, row))
    if payload.get("result_json"):
        try:
            payload["result"] = json.loads(payload["result_json"])
        except Exception:
            pass
    payload.pop("result_json", None)
    return payload


def _delta_upsert_state(
    conn,
    manifest: dict,
    status: str,
    *,
    result: dict | None = None,
    error_type: str | None = None,
    error_message: str | None = None,
) -> None:
    conn.execute(
        f"DELETE FROM {_delta_state_ref()} WHERE db_name = ? AND bundle_id = ?",
        [manifest["db_name"], manifest["bundle_id"]],
    )
    conn.execute(
        f"""
        INSERT INTO {_delta_state_ref()} (
            db_name,
            bundle_id,
            bundle_hash,
            import_run_id,
            publish_sequence,
            status,
            received_at,
            updated_at,
            committed_at,
            import_mode,
            platform,
            manifest_json,
            result_json,
            error_type,
            error_message
        )
        VALUES (
            ?, ?, ?, ?, ?, ?,
            current_timestamp,
            current_timestamp,
            CASE WHEN ? = 'COMMITTED' THEN current_timestamp ELSE NULL END,
            ?, ?, ?, ?, ?, ?
        )
        """,
        [
            manifest["db_name"],
            manifest["bundle_id"],
            manifest["bundle_hash"],
            str(manifest.get("import_run_id") or ""),
            int(manifest.get("publish_sequence") or 0),
            status,
            status,
            str(manifest.get("import_mode") or ""),
            str(manifest.get("platform") or ""),
            json.dumps(manifest, sort_keys=True, default=str),
            json.dumps(result, sort_keys=True, default=str) if result is not None else None,
            error_type,
            error_message,
        ],
    )


def _delta_archive_member_name(member: tarfile.TarInfo, error_cls: type[Exception] = DeltaValidationError) -> str:
    name = member.name.replace("\\", "/")
    if not name or name.startswith("/") or name.startswith("../") or "/../" in name:
        raise error_cls(f"Unsafe archive path: {member.name!r}")
    if name != member.name:
        raise error_cls(f"Archive path must use forward slashes only: {member.name!r}")
    if member.issym() or member.islnk():
        raise error_cls(f"Archive links are not allowed: {member.name!r}")
    if member.isdir():
        if name.rstrip("/") != "tables":
            raise error_cls(f"Unexpected archive directory: {member.name!r}")
    elif not member.isfile():
        raise error_cls(f"Unexpected archive member type: {member.name!r}")
    return name.rstrip("/")


def _validate_delta_manifest_shape(
    manifest: dict,
    *,
    db_name: str,
    expected_bundle_id: str | None,
    expected_bundle_hash: str | None,
) -> tuple[set[str], dict[str, dict]]:
    required_fields = {
        "manifest_version",
        "schema_version",
        "db_name",
        "league_id",
        "platform",
        "import_mode",
        "import_run_id",
        "publish_sequence",
        "bundle_id",
        "bundle_hash",
        "tables",
        "omitted_tables",
    }
    missing = sorted(required_fields - set(manifest))
    if missing:
        raise DeltaValidationError(f"Manifest missing required fields: {', '.join(missing)}")
    if manifest["manifest_version"] != _DELTA_MANIFEST_VERSION:
        raise DeltaValidationError(f"Unsupported manifest_version: {manifest['manifest_version']!r}")
    if manifest["schema_version"] != _DELTA_SCHEMA_VERSION:
        raise DeltaValidationError(f"Unsupported schema_version: {manifest['schema_version']!r}")
    if manifest["db_name"] != db_name:
        raise DeltaValidationError("Manifest db_name does not match X-Db-Name")
    if expected_bundle_id and manifest["bundle_id"] != expected_bundle_id:
        raise DeltaValidationError("Manifest bundle_id does not match X-Bundle-Id")
    if expected_bundle_hash and manifest["bundle_hash"] != expected_bundle_hash:
        raise DeltaValidationError("Manifest bundle_hash does not match X-Bundle-Hash")
    if not isinstance(manifest["tables"], list):
        raise DeltaValidationError("Manifest tables must be a list")
    if not isinstance(manifest["omitted_tables"], list):
        raise DeltaValidationError("Manifest omitted_tables must be a list")
    if "base_generation" in manifest and (
        isinstance(manifest["base_generation"], bool)
        or not isinstance(manifest["base_generation"], int)
        or manifest["base_generation"] < 0
    ):
        raise DeltaValidationError("base_generation must be a nonnegative integer")

    logical_payload = {
        "manifest_version": manifest["manifest_version"],
        "schema_version": manifest["schema_version"],
        "db_name": manifest["db_name"],
        "league_id": manifest.get("league_id"),
        "platform": manifest.get("platform"),
        "import_mode": manifest.get("import_mode"),
        "import_run_id": str(manifest.get("import_run_id") or ""),
        "publish_sequence": int(manifest.get("publish_sequence") or 0),
        "tables": [
            {
                "table": t.get("table"),
                "row_count": t.get("row_count"),
                "columns": t.get("columns"),
                "partition_keys": t.get("partition_keys"),
                "primary_keys": t.get("primary_keys"),
                "merge_mode": t.get("merge_mode"),
                "scope": t.get("scope"),
                "fingerprints": t.get("fingerprints"),
            }
            for t in manifest["tables"]
        ],
        "omitted_tables": manifest["omitted_tables"],
    }
    if "base_generation" in manifest:
        logical_payload["base_generation"] = manifest["base_generation"]
    recalculated_bundle_hash = _sha256_json(logical_payload)
    if recalculated_bundle_hash != manifest["bundle_hash"]:
        raise DeltaValidationError("Logical bundle_hash does not match manifest content")

    seen_tables: set[str] = set()
    seen_paths: set[str] = set()
    table_entries: dict[str, dict] = {}
    allowed_paths = {"manifest.json"}
    for entry in manifest["tables"]:
        if not isinstance(entry, dict):
            raise DeltaValidationError("Each table entry must be an object")
        table = str(entry.get("table") or "")
        if table not in _DELTA_ALLOWED_TABLES:
            raise DeltaValidationError(f"Unsupported canonical table: {table!r}")
        if table in seen_tables:
            raise DeltaValidationError(f"Duplicate table entry: {table}")
        seen_tables.add(table)
        expected_path = f"tables/{table}.parquet"
        if entry.get("path") != expected_path:
            raise DeltaValidationError(f"Invalid path for {table}: {entry.get('path')!r}")
        path_key = expected_path.lower()
        if path_key in seen_paths:
            raise DeltaValidationError(f"Duplicate table path: {expected_path}")
        seen_paths.add(path_key)
        if entry.get("format") != "parquet":
            raise DeltaValidationError(f"Unsupported table format for {table}: {entry.get('format')!r}")
        if entry.get("merge_mode") != "replace_league":
            raise DeltaValidationError(f"Unsupported merge_mode for {table}: {entry.get('merge_mode')!r}")
        if not isinstance(entry.get("row_count"), int) or entry["row_count"] < 0:
            raise DeltaValidationError(f"Invalid row_count for {table}")
        declared_columns = entry.get("columns")
        if not isinstance(declared_columns, list) or not declared_columns:
            raise DeltaValidationError(f"Missing declared columns for {table}")
        declared_names = [str(col.get("name") or "") for col in declared_columns if isinstance(col, dict)]
        if len(declared_names) != len(declared_columns) or len(set(declared_names)) != len(declared_names):
            raise DeltaValidationError(f"Invalid declared columns for {table}")
        if "db_name" not in declared_names:
            raise DeltaValidationError(f"{table} is missing required db_name partition column")
        allowed_paths.add(expected_path)
        table_entries[table] = entry

    import_mode = str(manifest.get("import_mode") or "").lower()
    if import_mode in {"full", "fleet", "all_years"}:
        missing_required = sorted(_DELTA_FULL_REQUIRED_TABLES - seen_tables)
        if missing_required:
            raise DeltaValidationError(f"Full delta missing required tables: {', '.join(missing_required)}")
        for source_table, required_aggregates in sorted(_DELTA_REQUIRED_FULL_AGGREGATES.items()):
            source_entry = table_entries.get(source_table)
            source_has_rows = bool(source_entry and source_entry.get("row_count", 0) > 0)
            if source_has_rows:
                missing_aggregates = sorted(required_aggregates - seen_tables)
                if missing_aggregates:
                    raise DeltaValidationError(
                        f"Full delta with {source_table} missing required derived tables: "
                        + ", ".join(missing_aggregates)
                    )

    omitted_names = set()
    for omitted in manifest["omitted_tables"]:
        if not isinstance(omitted, dict) or "table" not in omitted or "reason" not in omitted:
            raise DeltaValidationError("Each omitted table must include table and reason")
        table = str(omitted["table"])
        if table not in _DELTA_ALLOWED_TABLES:
            raise DeltaValidationError(f"Unsupported omitted table: {table!r}")
        if table in seen_tables:
            raise DeltaValidationError(f"Table cannot be both included and omitted: {table}")
        omitted_names.add(table)

    unknown_manifest_tables = _DELTA_ALLOWED_TABLES - seen_tables - omitted_names
    if unknown_manifest_tables:
        raise DeltaValidationError(
            "Manifest must distinguish included and omitted canonical tables; missing: "
            + ", ".join(sorted(unknown_manifest_tables))
        )

    return allowed_paths, table_entries


def _validate_delta_parquet_tables(manifest: dict, table_entries: dict[str, dict], extract_dir: Path) -> None:
    validation_conn = _duckdb.connect(":memory:")
    try:
        for table, entry in sorted(table_entries.items()):
            parquet_path = extract_dir / entry["path"]
            if not parquet_path.exists():
                raise DeltaValidationError(f"Missing Parquet file for {table}")
            if _sha256_file(parquet_path) != entry.get("sha256"):
                raise DeltaValidationError(f"Parquet sha256 mismatch for {table}")

            parquet_ref = f"read_parquet({_sql_literal(parquet_path)})"
            schema_rows = validation_conn.execute(f"DESCRIBE SELECT * FROM {parquet_ref}").fetchall()
            actual_cols = [row[0] for row in schema_rows]
            declared_cols = [col["name"] for col in entry["columns"]]
            if actual_cols != declared_cols:
                raise DeltaValidationError(
                    f"Parquet schema mismatch for {table}: actual {actual_cols!r}, manifest {declared_cols!r}"
                )

            actual_count = int(validation_conn.execute(f"SELECT COUNT(*) FROM {parquet_ref}").fetchone()[0] or 0)
            if actual_count != entry["row_count"]:
                raise DeltaValidationError(
                    f"Parquet row_count mismatch for {table}: actual {actual_count}, manifest {entry['row_count']}"
                )

            bad_db_rows = int(
                validation_conn.execute(
                    f"SELECT COUNT(*) FROM {parquet_ref} WHERE db_name IS NULL OR db_name <> ?",
                    [manifest["db_name"]],
                ).fetchone()[0]
                or 0
            )
            if bad_db_rows:
                raise DeltaValidationError(f"{table} contains {bad_db_rows} rows outside db_name={manifest['db_name']}")

            identity_keys = list(entry.get("primary_keys") or entry.get("identity_keys") or ())
            if not identity_keys:
                identity_keys = [key for key in _DELTA_CORE_IDENTITY_KEYS.get(table, ()) if key in actual_cols]
            missing_identity = [key for key in identity_keys if key not in actual_cols]
            if missing_identity:
                raise DeltaValidationError(f"{table} is missing identity columns: {', '.join(missing_identity)}")
            if identity_keys:
                null_checks = ", ".join(
                    f"SUM(CASE WHEN {_qident(key)} IS NULL THEN 1 ELSE 0 END)::BIGINT" for key in identity_keys
                )
                null_row = validation_conn.execute(f"SELECT {null_checks} FROM {parquet_ref}").fetchone()
                null_counts = {key: int(null_row[idx] or 0) for idx, key in enumerate(identity_keys)}
                bad_nulls = {key: count for key, count in null_counts.items() if count}
                if bad_nulls:
                    raise DeltaValidationError(f"{table} contains null identity keys: {bad_nulls}")
                key_expr = ", ".join(_qident(key) for key in identity_keys)
                duplicates = int(
                    validation_conn.execute(
                        f"""
                        SELECT COALESCE(SUM(cnt - 1), 0)::BIGINT
                        FROM (
                            SELECT {key_expr}, COUNT(*) AS cnt
                            FROM {parquet_ref}
                            GROUP BY {key_expr}
                            HAVING COUNT(*) > 1
                        )
                        """
                    ).fetchone()[0]
                    or 0
                )
                if duplicates:
                    raise DeltaValidationError(f"{table} contains {duplicates} duplicate identity rows")
    finally:
        validation_conn.close()


def _validate_bundle_archive(
    archive_path: Path,
    *,
    extract_prefix: str,
    validate_manifest_shape,
    validate_parquet_tables,
    error_cls: type[Exception],
    invalid_label: str,
) -> tuple[dict, Path]:
    """Shared tar.gz walk for delta and fleet bundles.

    ``validate_manifest_shape(manifest) -> (allowed_paths, table_entries)``
    runs before extraction; ``validate_parquet_tables(manifest, table_entries,
    extract_dir)`` runs after. Archive-level violations raise ``error_cls``.
    """
    extract_dir = Path(tempfile.mkdtemp(prefix=extract_prefix, dir=archive_path.parent))
    try:
        try:
            with tarfile.open(archive_path, "r:gz") as tar:
                members = tar.getmembers()
                if not members:
                    raise error_cls("Archive is empty")
                seen_names: set[str] = set()
                total_size = 0
                manifest_member = None
                for member in members:
                    name = _delta_archive_member_name(member, error_cls)
                    if member.isdir():
                        continue
                    lower_name = name.lower()
                    if lower_name in seen_names:
                        raise error_cls(f"Duplicate archive path: {name}")
                    seen_names.add(lower_name)
                    total_size += int(member.size or 0)
                    if total_size > _DELTA_MAX_UPLOAD_BYTES * 2:
                        raise error_cls("Expanded archive payload is too large")
                    if name == "manifest.json":
                        manifest_member = member
                if manifest_member is None:
                    raise error_cls("Archive missing manifest.json")
                if manifest_member.size > 2 * 1024 * 1024:
                    raise error_cls("manifest.json is too large")
                manifest_fh = tar.extractfile(manifest_member)
                if manifest_fh is None:
                    raise error_cls("Unable to read manifest.json")
                manifest = json.loads(manifest_fh.read().decode("utf-8"))
                allowed_paths, table_entries = validate_manifest_shape(manifest)
                unexpected = sorted(name for name in seen_names if name not in {p.lower() for p in allowed_paths})
                if unexpected:
                    raise error_cls(f"Unexpected archive paths: {', '.join(unexpected)}")
                if len(seen_names) != len(allowed_paths):
                    missing_paths = sorted(p for p in allowed_paths if p.lower() not in seen_names)
                    raise error_cls(f"Archive missing paths: {', '.join(missing_paths)}")
                for member in members:
                    if not member.isfile():
                        continue
                    name = _delta_archive_member_name(member, error_cls)
                    target = extract_dir / name
                    target.parent.mkdir(parents=True, exist_ok=True)
                    source = tar.extractfile(member)
                    if source is None:
                        raise error_cls(f"Unable to extract archive member: {name}")
                    with source, open(target, "wb") as out:
                        shutil.copyfileobj(source, out)
        except (tarfile.TarError, OSError, json.JSONDecodeError) as exc:
            raise error_cls(f"Invalid {invalid_label} archive: {exc}") from exc

        validate_parquet_tables(manifest, table_entries, extract_dir)
        return manifest, extract_dir
    except Exception:
        shutil.rmtree(extract_dir, ignore_errors=True)
        raise


def _validate_delta_archive(
    archive_path: Path,
    *,
    db_name: str,
    expected_bundle_id: str | None,
    expected_bundle_hash: str | None,
) -> tuple[dict, Path]:
    return _validate_bundle_archive(
        archive_path,
        extract_prefix=f"delta_{db_name}_",
        validate_manifest_shape=lambda manifest: _validate_delta_manifest_shape(
            manifest,
            db_name=db_name,
            expected_bundle_id=expected_bundle_id,
            expected_bundle_hash=expected_bundle_hash,
        ),
        validate_parquet_tables=_validate_delta_parquet_tables,
        error_cls=DeltaValidationError,
        invalid_label="delta",
    )


def _validate_fleet_archive(
    archive_path: Path,
    *,
    expected_bundle_id: str | None,
    expected_bundle_hash: str | None,
) -> tuple[dict, Path]:
    return _validate_bundle_archive(
        archive_path,
        extract_prefix="fleet_partition_",
        validate_manifest_shape=lambda manifest: fleet_merge.validate_fleet_manifest_shape(
            manifest,
            allowed_tables=_DELTA_ALLOWED_TABLES,
            identity_keys=_DELTA_CORE_IDENTITY_KEYS,
            expected_bundle_id=expected_bundle_id,
            expected_bundle_hash=expected_bundle_hash,
        ),
        validate_parquet_tables=lambda manifest, table_entries, extract_dir: fleet_merge.validate_fleet_parquet_tables(
            manifest,
            table_entries,
            extract_dir,
            sha256_file=_sha256_file,
        ),
        error_cls=fleet_merge.FleetValidationError,
        invalid_label="fleet",
    )


def _delta_latest_committed_order(conn, db_name: str) -> tuple[int, int] | None:
    row = conn.execute(
        f"""
        SELECT import_run_id, publish_sequence
        FROM {_delta_state_ref()}
        WHERE db_name = ? AND status = 'COMMITTED'
        ORDER BY committed_at DESC NULLS LAST, updated_at DESC
        LIMIT 1
        """,
        [db_name],
    ).fetchone()
    if row is None:
        return None
    try:
        return int(row[0]), int(row[1])
    except Exception:
        return None


def _merge_delta_bundle(leagues_path: Path, manifest: dict, extract_dir: Path) -> dict:
    """Atomically merge a validated delta bundle into ___leagues.duckdb."""
    db_name = manifest["db_name"]
    hard_exit_timer = _start_merge_hard_exit_timer(db_name)
    conn = None
    in_transaction = False
    try:
        # Delta merges keep public reads online, so the writer must use the
        # same connection config as the read pool. DuckDB rejects concurrent
        # connections to one file when config differs.
        conn = db.connect_database(leagues_path, data_dir=db.get_data_dir())
        _ensure_delta_state_table(conn)

        # G14: repairs may not commit during the fleet publish window —
        # a mid-window repair would force the fleet bundle's generation
        # check to reject at 4am. Queued repairs retry after the window.
        if fleet_merge.fleet_publish_locked(conn):
            raise DeltaConflictError(
                "Fleet publish window active; repair-lane commits are queued — retry after the window"
            )

        existing = _delta_state_row(conn, db_name, manifest["bundle_id"])
        if existing is not None:
            if existing.get("bundle_hash") != manifest["bundle_hash"]:
                raise DeltaConflictError("Same bundle_id was previously recorded with a different bundle_hash")
            if existing.get("status") == "COMMITTED":
                result = existing.get("result") or {}
                result["idempotent_replay"] = True
                return result

        # Enable after the public import workflows have been rolled out and
        # older in-flight imports have drained. Prior committed bundles may
        # still replay idempotently, but a new unfenced writer cannot publish.
        if _env_bool("REQUIRE_DELTA_BASE_GENERATION", False) and "base_generation" not in manifest:
            raise DeltaConflictError("Delta import missing required pre-source base_generation")

        latest_order = _delta_latest_committed_order(conn, db_name)
        current_order = None
        try:
            current_order = (int(manifest.get("import_run_id") or 0), int(manifest.get("publish_sequence") or 0))
        except Exception:
            current_order = None
        if latest_order is not None and current_order is not None and current_order < latest_order:
            _delta_upsert_state(
                conn,
                manifest,
                "CONFLICT",
                error_type="older_bundle",
                error_message="Bundle order is older than the latest committed publish for this league",
            )
            raise DeltaConflictError("Older bundle cannot commit over newer committed state")

        _delta_upsert_state(conn, manifest, "RECEIVED")
        _delta_upsert_state(conn, manifest, "VALIDATED")

        def target_ref(table: str) -> str:
            return f"public.{_qident(table)}"

        def target_exists(table: str) -> bool:
            try:
                conn.execute(f"DESCRIBE {target_ref(table)}")
                return True
            except Exception:
                return False

        merged: dict[str, int] = {}
        timings: dict[str, dict[str, float]] = {}
        total_start = time.perf_counter()
        try:
            _interrupting_execute(conn, "BEGIN TRANSACTION", step=f"begin delta {db_name}")
            in_transaction = True
            if "base_generation" in manifest:
                fleet_merge.ensure_generation_tables(conn)
                current_generation = fleet_merge.current_generations(conn, [db_name])[db_name]
                if current_generation != manifest["base_generation"]:
                    raise DeltaConflictError(
                        f"Snapshot generation {manifest['base_generation']} for {db_name} "
                        f"is stale; current generation is {current_generation}"
                    )
            _delta_upsert_state(conn, manifest, "STAGED")

            for entry in manifest["tables"]:
                table = entry["table"]
                table_start = time.perf_counter()
                parquet_path = extract_dir / entry["path"]
                parquet_ref = f"read_parquet({_sql_literal(parquet_path)})"
                incoming_schema = conn.execute(f"DESCRIBE SELECT * FROM {parquet_ref}").fetchall()
                incoming_col_types = {row[0]: row[1] for row in incoming_schema}
                incoming_cols = list(incoming_col_types.keys())
                source_cols = [col for col in incoming_cols if col != "db_name"]

                if not target_exists(table):
                    if source_cols:
                        select_cols = ", ".join(_qident(col) for col in source_cols)
                        _interrupting_execute(
                            conn,
                            f"CREATE TABLE {target_ref(table)} AS "
                            f"SELECT CAST(NULL AS VARCHAR) AS db_name, {select_cols} "
                            f"FROM {parquet_ref} WHERE FALSE",
                            step=f"delta create {db_name}.{table}",
                        )
                    else:
                        _interrupting_execute(
                            conn,
                            f"CREATE TABLE {target_ref(table)} (db_name VARCHAR)",
                            step=f"delta create {db_name}.{table}",
                        )

                target_col_types = {row[0]: row[1] for row in conn.execute(f"DESCRIBE {target_ref(table)}").fetchall()}
                if "db_name" not in target_col_types:
                    _interrupting_execute(
                        conn,
                        f"ALTER TABLE {target_ref(table)} ADD COLUMN db_name VARCHAR",
                        step=f"delta add db_name {db_name}.{table}",
                    )
                    target_col_types = {
                        row[0]: row[1] for row in conn.execute(f"DESCRIBE {target_ref(table)}").fetchall()
                    }

                missing_cols = [col for col in source_cols if col not in target_col_types]
                for col in missing_cols:
                    _interrupting_execute(
                        conn,
                        f"ALTER TABLE {target_ref(table)} ADD COLUMN {_qident(col)} {incoming_col_types[col]}",
                        step=f"delta add column {db_name}.{table}.{col}",
                    )
                if missing_cols:
                    target_col_types = {
                        row[0]: row[1] for row in conn.execute(f"DESCRIBE {target_ref(table)}").fetchall()
                    }

                common_cols = [col for col in source_cols if col in target_col_types]
                insert_cols = ["db_name"] + common_cols
                select_exprs = ["? AS db_name"]
                for col in common_cols:
                    incoming_type = str(incoming_col_types.get(col) or "").upper()
                    target_type = str(target_col_types[col]).upper()
                    if incoming_type == target_type:
                        select_exprs.append(_qident(col))
                    else:
                        select_exprs.append(f"TRY_CAST({_qident(col)} AS {target_col_types[col]}) AS {_qident(col)}")

                delete_start = time.perf_counter()
                _interrupting_execute(
                    conn,
                    f"DELETE FROM {target_ref(table)} WHERE db_name = ?",
                    [db_name],
                    step=f"delta delete {db_name}.{table}",
                )
                insert_start = time.perf_counter()
                _interrupting_execute(
                    conn,
                    f"INSERT INTO {target_ref(table)} ({', '.join(_qident(col) for col in insert_cols)}) "
                    f"SELECT {', '.join(select_exprs)} FROM {parquet_ref}",
                    [db_name],
                    step=f"delta insert {db_name}.{table}",
                )
                stored_count = int(
                    conn.execute(f"SELECT COUNT(*) FROM {target_ref(table)} WHERE db_name = ?", [db_name]).fetchone()[0]
                    or 0
                )
                expected_count = int(entry["row_count"])
                if stored_count != expected_count:
                    raise RuntimeError(
                        f"Post-insert row count mismatch for {table}: expected {expected_count}, stored {stored_count}"
                    )
                merged[table] = expected_count
                timings[table] = {
                    "delete_seconds": round(insert_start - delete_start, 4),
                    "insert_verify_seconds": round(time.perf_counter() - insert_start, 4),
                    "total_seconds": round(time.perf_counter() - table_start, 4),
                }

            fleet_merge.ensure_generation_tables(conn)
            fleet_merge.bump_generations(
                conn, [db_name], lane="repair", run_id=str(manifest.get("import_run_id") or "")
            )
            result = {
                "status": "COMMITTED",
                "db_name": db_name,
                "bundle_id": manifest["bundle_id"],
                "bundle_hash": manifest["bundle_hash"],
                "tables": merged,
                "table_count": len(merged),
                "row_count": sum(merged.values()),
                "timings": timings,
                "elapsed_seconds": round(time.perf_counter() - total_start, 4),
            }
            _delta_upsert_state(conn, manifest, "COMMITTED", result=result)
            _interrupting_execute(conn, "COMMIT", step=f"commit delta {db_name}")
            in_transaction = False
            _checkpoint_connection_if_wal_large(conn, leagues_path, reason=f"delta merge {db_name}")
            return result
        except Exception as exc:
            if in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except Exception as rollback_exc:
                    logger.error("Delta rollback failed for %s: %s", db_name, rollback_exc, exc_info=True)
                in_transaction = False
            _delta_upsert_state(
                conn,
                manifest,
                "CONFLICT" if isinstance(exc, DeltaConflictError) else "FAILED_MERGE",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
    finally:
        hard_exit_timer.cancel()
        if conn is not None:
            conn.close()


def _merge_fleet_bundle(leagues_path: Path, manifest: dict, extract_dir: Path) -> dict:
    """Atomically merge a validated fleet partition bundle into ___leagues.duckdb.

    Shares the delta publish-state table (db_name = ``___fleet``) for
    idempotent replay and ordering, but every table merges with a scoped
    delete bounded by the db_names actually present in the uploaded parquet.
    """
    sentinel = fleet_merge.FLEET_DB_SENTINEL
    hard_exit_timer = _start_merge_hard_exit_timer(sentinel, FLEET_MERGE_HARD_EXIT_SECONDS)
    conn = None
    in_transaction = False
    ops_attached = False
    try:
        conn = db.connect_database(leagues_path, data_dir=db.get_data_dir())
        _ensure_delta_state_table(conn)

        existing = _delta_state_row(conn, sentinel, manifest["bundle_id"])
        if existing is not None:
            if existing.get("bundle_hash") != manifest["bundle_hash"]:
                raise DeltaConflictError("Same bundle_id was previously recorded with a different bundle_hash")
            if existing.get("status") == "COMMITTED":
                result = existing.get("result") or {}
                result["idempotent_replay"] = True
                return result

        latest_order = _delta_latest_committed_order(conn, sentinel)
        current_order = None
        try:
            current_order = (int(manifest.get("import_run_id") or 0), int(manifest.get("publish_sequence") or 0))
        except Exception:
            current_order = None
        if latest_order is not None and current_order is not None and current_order < latest_order:
            _delta_upsert_state(
                conn,
                manifest,
                "CONFLICT",
                error_type="older_bundle",
                error_message="Bundle order is older than the latest committed fleet publish",
            )
            raise DeltaConflictError("Older fleet bundle cannot commit over newer committed state")

        _delta_upsert_state(conn, manifest, "RECEIVED")
        _delta_upsert_state(conn, manifest, "VALIDATED")

        def execute_step(step_conn, sql, params=None, *, step: str = ""):
            return _interrupting_execute(
                step_conn,
                sql,
                params,
                step=step,
                timeout_seconds=FLEET_MERGE_STEP_TIMEOUT_SECONDS,
            )

        try:
            if manifest.get("schema_version") in {
                fleet_merge.FLEET_CAREER_SCHEMA_VERSION, fleet_merge.FLEET_HOMEPAGE_SCHEMA_VERSION,
            }:
                _acquire_ops_attachment(conn)
                ops_attached = True
            _interrupting_execute(conn, "BEGIN TRANSACTION", step="begin fleet partition")
            in_transaction = True
            _delta_upsert_state(conn, manifest, "STAGED")
            try:
                result = fleet_merge.apply_fleet_merge(
                    conn,
                    manifest,
                    extract_dir,
                    execute=execute_step,
                    manage_transaction=False,
                )
            except fleet_merge.FleetGenerationConflict as exc:
                raise DeltaConflictError(str(exc)) from exc
            _delta_upsert_state(conn, manifest, "COMMITTED", result=result)
            _interrupting_execute(conn, "COMMIT", step="commit fleet partition")
            in_transaction = False
            _checkpoint_connection_if_wal_large(conn, leagues_path, reason="fleet partition merge")
            return result
        except Exception as exc:
            if in_transaction:
                try:
                    conn.execute("ROLLBACK")
                except Exception as rollback_exc:
                    logger.error("Fleet merge rollback failed: %s", rollback_exc, exc_info=True)
                in_transaction = False
            _delta_upsert_state(
                conn,
                manifest,
                "FAILED_MERGE",
                error_type=type(exc).__name__,
                error_message=str(exc),
            )
            raise
    finally:
        hard_exit_timer.cancel()
        if conn is not None:
            if ops_attached:
                _release_ops_attachment(conn)
            conn.close()


@app.post("/merge-fleet-partition")
async def merge_fleet_partition(
    request: Request,
    file: UploadFile = _FILE_PARAM,
    x_bundle_id: str | None = Header(None),
    x_bundle_hash: str | None = Header(None),
):
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/merge-fleet-partition"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    sentinel = fleet_merge.FLEET_DB_SENTINEL
    publish_token: str | None = None
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    leagues_path = data_dir / "___leagues.duckdb"
    incoming_path = data_dir / f"fleet_partition_{time.time_ns()}_{random.randint(100000, 999999)}.tar.gz"
    extract_dir: Path | None = None

    written = 0
    try:
        with open(incoming_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                written += len(chunk)
                if written > _DELTA_MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Fleet bundle too large")
                f.write(chunk)

        validation_start = time.perf_counter()
        try:
            manifest, extract_dir = await asyncio.to_thread(
                _validate_fleet_archive,
                incoming_path,
                expected_bundle_id=x_bundle_id,
                expected_bundle_hash=x_bundle_hash,
            )
        except fleet_merge.FleetValidationError as exc:
            track_event("fleet_partition_validation_failed", {"error": str(exc)[:200]})
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        logger.info(
            "Fleet partition bundle validated: %s tables, %.1f MB in %.2fs",
            len(manifest.get("tables", [])),
            written / (1024 * 1024),
            time.perf_counter() - validation_start,
        )

        publish_token = await _acquire_delta_publish_slot(sentinel, manifest.get("bundle_id") or x_bundle_id)
        lock_wait_start = time.perf_counter()
        _update_delta_publish_slot(
            publish_token,
            "waiting_merge_lock",
            table_count=len(manifest.get("tables", [])),
            size_mb=round(written / (1024 * 1024), 2),
        )
        async with _merge_lock:
            lock_wait_seconds = time.perf_counter() - lock_wait_start
            merge_start = time.perf_counter()
            _update_delta_publish_slot(
                publish_token,
                "merging",
                lock_wait_seconds=round(lock_wait_seconds, 2),
                table_count=len(manifest.get("tables", [])),
                size_mb=round(written / (1024 * 1024), 2),
            )
            try:
                result = await asyncio.to_thread(_merge_fleet_bundle, leagues_path, manifest, extract_dir)
            except DeltaConflictError as exc:
                track_event("fleet_partition_conflict", {"bundle_id": manifest.get("bundle_id")})
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except fleet_merge.FleetValidationError as exc:
                track_event("fleet_partition_validation_failed", {"error": str(exc)[:200]})
                raise HTTPException(status_code=422, detail=str(exc)) from exc
            except Exception as exc:
                logger.error("Fleet partition merge failed: %s", exc, exc_info=True)
                track_event("fleet_partition_merge_failed", {"error": type(exc).__name__})
                raise HTTPException(status_code=500, detail=str(exc)) from exc

            db.refresh_metadata()
            elapsed = time.perf_counter() - merge_start
            result["merge_seconds"] = round(elapsed, 4)
            result["lock_wait_seconds"] = round(lock_wait_seconds, 4)
            result["server_total_seconds"] = round(time.perf_counter() - validation_start, 4)
            logger.info("Fleet partition merge complete (%.2fs): %s", elapsed, result)
            track_event(
                "fleet_partition_merged",
                {
                    "bundle_id": manifest.get("bundle_id"),
                    "elapsed_seconds": round(elapsed, 2),
                    "lock_wait_seconds": round(lock_wait_seconds, 2),
                    "table_count": result.get("table_count", 0),
                    "row_count": result.get("row_count", 0),
                    "size_mb": round(written / (1024 * 1024), 1),
                },
            )
            return result
    finally:
        _release_delta_publish_slot(publish_token)
        incoming_path.unlink(missing_ok=True)
        if extract_dir is not None:
            shutil.rmtree(extract_dir, ignore_errors=True)


@app.post("/fleet-publish-lock")
async def fleet_publish_lock_endpoint(request: Request):
    """Set/clear the G14 fleet publish window lock (admin).

    Body: {"locked": bool, "run_id": str}. While locked, repair-lane delta
    merges are rejected with 409 so they cannot race the fleet bundle's
    generation check. The orchestrator locks at stage-5 start and unlocks
    after the pointer flip.
    """
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/fleet-publish-lock"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    body = await request.json()
    locked = bool(body.get("locked"))
    run_id = str(body.get("run_id") or "")

    def _apply() -> dict:
        leagues_path = db.get_data_dir() / "___leagues.duckdb"
        conn = db.connect_database(leagues_path, data_dir=db.get_data_dir())
        try:
            fleet_merge.set_fleet_publish_lock(conn, locked, run_id=run_id)
            return {"locked": locked, "run_id": run_id}
        finally:
            conn.close()

    async with _merge_lock:
        result = await asyncio.to_thread(_apply)
    track_event("fleet_publish_lock", result)
    return result


@app.post("/replace-canonical-table")
async def replace_canonical_table(
    request: Request,
    file: UploadFile = _FILE_PARAM,
    x_db_name: str = Header(...),
    x_table_name: str = Header(...),
    x_expected_rows: str = Header(...),
    x_content_sha256: str = Header(None),
    x_recovery_since: str | None = Header(None),
    x_expected_overlay_leagues: str | None = Header(None),
    x_expected_overlay_rows: str | None = Header(None),
):
    """Atomically replace one allowlisted canonical table from a DuckDB bundle."""
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as exc:
        track_event("auth_failed", {"endpoint": "/replace-canonical-table"})
        raise HTTPException(status_code=401, detail="Unauthorized") from exc

    table_name = str(x_table_name or "").strip()
    if (x_db_name, table_name) not in _CANONICAL_TABLE_REPLACEMENTS:
        raise HTTPException(
            status_code=400,
            detail="canonical table replacement target is not allowlisted",
        )
    try:
        expected_rows = int(x_expected_rows)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="x-expected-rows must be an integer") from exc
    if expected_rows <= 0:
        raise HTTPException(status_code=400, detail="x-expected-rows must be positive")
    expected_overlay_leagues = None
    expected_overlay_rows = None
    if x_recovery_since:
        try:
            expected_overlay_leagues = int(str(x_expected_overlay_leagues))
            expected_overlay_rows = int(str(x_expected_overlay_rows))
        except (TypeError, ValueError) as exc:
            raise HTTPException(
                status_code=400,
                detail="recovery overlay requires integer expected league and row counts",
            ) from exc
        if expected_overlay_leagues < 0 or expected_overlay_rows < 0:
            raise HTTPException(
                status_code=400,
                detail="recovery overlay expected counts cannot be negative",
            )

    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    database_path = data_dir / f"{x_db_name}.duckdb"
    incoming_path = data_dir / (
        f"canonical_table_{table_name}_{time.time_ns()}_{random.randint(100000, 999999)}.duckdb"
    )
    written = 0
    try:
        with open(incoming_path, "wb") as output:
            while chunk := await file.read(1024 * 1024):
                written += len(chunk)
                if written > MAX_CANONICAL_TABLE_REPLACEMENT_BYTES:
                    raise HTTPException(
                        status_code=413,
                        detail="Canonical table replacement bundle is too large",
                    )
                output.write(chunk)
        if x_content_sha256:
            try:
                verify_checksum(incoming_path, x_content_sha256)
            except SwapError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        async with _merge_lock:
            _state["status"] = "draining"
            elapsed = 0.0
            while db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
                await asyncio.sleep(0.5)
                elapsed += 0.5
            if db.get_active_count() > 0:
                await asyncio.sleep(HARD_DRAIN_TIMEOUT - SOFT_DRAIN_TIMEOUT)
            db.close_pool()
            _state["status"] = "writing"
            try:
                result = await asyncio.to_thread(
                    _replace_canonical_table,
                    database_path,
                    incoming_path,
                    database_name=x_db_name,
                    table_name=table_name,
                    expected_rows=expected_rows,
                    recovery_since=x_recovery_since,
                    expected_overlay_leagues=expected_overlay_leagues,
                    expected_overlay_rows=expected_overlay_rows,
                )
            except ValueError as exc:
                await _reopen_pool_after_write("rejected canonical table replacement")
                _set_serving_or_ops_writing()
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            except Exception as exc:
                logger.error(
                    "Canonical table replacement failed for %s.%s: %s",
                    x_db_name,
                    table_name,
                    exc,
                    exc_info=True,
                )
                await _reopen_pool_after_write("failed canonical table replacement")
                _set_serving_or_ops_writing()
                raise HTTPException(status_code=500, detail=str(exc)) from exc

            await _reopen_pool_after_write(f"{x_db_name}.{table_name} replacement")
            _set_serving_or_ops_writing()
            track_event(
                "canonical_table_replaced",
                {
                    "database": x_db_name,
                    "table": table_name,
                    "rows": result["rows"],
                    "size_mb": round(written / (1024 * 1024), 2),
                },
            )
            return result
    finally:
        incoming_path.unlink(missing_ok=True)


@app.post("/rename-league")
async def rename_league(request: Request):
    """Atomically consolidate one league identity and retarget its registries."""
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as exc:
        track_event("auth_failed", {"endpoint": "/rename-league"})
        raise HTTPException(status_code=401, detail="Unauthorized") from exc

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="request body must be JSON") from exc
    source_db = str(body.get("source_db") or "").strip()
    target_db = str(body.get("target_db") or "").strip()
    display_name = str(body.get("display_name") or "").strip()
    operation_id = str(body.get("operation_id") or "").strip()
    if not _DB_NAME_PATTERN.fullmatch(source_db) or not _DB_NAME_PATTERN.fullmatch(target_db):
        raise HTTPException(status_code=400, detail="invalid league database name")
    if source_db == target_db:
        raise HTTPException(status_code=400, detail="source and target league names must differ")
    if not display_name or len(display_name) > 100:
        raise HTTPException(status_code=400, detail="display_name is required and must be at most 100 characters")
    if not operation_id or len(operation_id) > 200:
        raise HTTPException(status_code=400, detail="operation_id is required and must be at most 200 characters")

    async with _merge_lock:
        _state["status"] = "draining"
        elapsed = 0.0
        while db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
            await asyncio.sleep(0.5)
            elapsed += 0.5
        if db.get_active_count() > 0:
            await asyncio.sleep(HARD_DRAIN_TIMEOUT - SOFT_DRAIN_TIMEOUT)
        _drain_ops_attachments_for_snapshot()
        db.close_all()
        _state["status"] = "writing"
        try:
            result = await asyncio.to_thread(
                _rename_league_server_side,
                data_dir=db.get_data_dir(),
                source_db=source_db,
                target_db=target_db,
                display_name=display_name,
                operation_id=operation_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.error(
                "League rename failed for %s -> %s: %s",
                source_db,
                target_db,
                exc,
                exc_info=True,
            )
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            await _reopen_pool_after_write(f"league rename {source_db} -> {target_db}")
            _set_serving_or_ops_writing()

    track_event(
        "league_renamed",
        {
            "source_db": source_db,
            "target_db": target_db,
            "operation_id": operation_id,
            "status": result["status"],
        },
    )
    return result


@app.post("/rebuild-league-derived")
async def rebuild_league_derived(request: Request):
    """Recompute one league's derived tables from its persisted full chain."""
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as exc:
        track_event("auth_failed", {"endpoint": "/rebuild-league-derived"})
        raise HTTPException(status_code=401, detail="Unauthorized") from exc

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="request body must be JSON") from exc
    db_name = str(body.get("db_name") or "").strip()
    run_id = str(body.get("run_id") or "").strip()
    if not _DB_NAME_PATTERN.fullmatch(db_name):
        raise HTTPException(status_code=400, detail="invalid league database name")
    if not run_id or len(run_id) > 200:
        raise HTTPException(status_code=400, detail="run_id is required and must be at most 200 characters")

    database_path = db.get_data_dir() / "___leagues.duckdb"
    async with _merge_lock:
        _state["status"] = "draining"
        elapsed = 0.0
        while db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
            await asyncio.sleep(0.5)
            elapsed += 0.5
        if db.get_active_count() > 0:
            await asyncio.sleep(HARD_DRAIN_TIMEOUT - SOFT_DRAIN_TIMEOUT)
        db.close_pool()
        _state["status"] = "writing"
        try:
            result = await asyncio.to_thread(
                _rebuild_league_derived_from_sources,
                database_path,
                db_name=db_name,
                run_id=run_id,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.error("Derived recovery failed for %s: %s", db_name, exc, exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            await _reopen_pool_after_write(f"{db_name} derived recovery")
            _set_serving_or_ops_writing()

    track_event(
        "league_derived_recovered",
        {
            "db_name": db_name,
            "run_id": run_id,
            "status": result["status"],
            "generation": result["generation"],
            "checkpointed": result["checkpointed"],
        },
    )
    return result


@app.post("/reaggregate-damaged-derived")
async def reaggregate_damaged_derived(request: Request):
    """Quarantine and rebuild exactly the five known damaged aggregates."""
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as exc:
        track_event("auth_failed", {"endpoint": "/reaggregate-damaged-derived"})
        raise HTTPException(status_code=401, detail="Unauthorized") from exc

    try:
        body = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="request body must be JSON") from exc
    mode = str(body.get("mode") or "").strip()
    confirmed_targets = body.get("confirm_targets")
    if confirmed_targets != list(_DAMAGED_DERIVED_TARGETS):
        raise HTTPException(status_code=400, detail="confirm_targets must exactly match the recovery allowlist")
    if mode not in {"quarantine_and_rebuild", "resume_rebuild"}:
        raise HTTPException(status_code=400, detail="invalid recovery mode")

    database_path = db.get_data_dir() / "___leagues.duckdb"
    async with _merge_lock:
        _state["status"] = "draining"
        elapsed = 0.0
        while db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
            await asyncio.sleep(0.5)
            elapsed += 0.5
        if db.get_active_count() > 0:
            await asyncio.sleep(HARD_DRAIN_TIMEOUT - SOFT_DRAIN_TIMEOUT)
        db.close_pool()
        _state["status"] = "writing"
        try:
            result = await asyncio.to_thread(
                _reaggregate_damaged_derived_from_sources,
                database_path,
                mode=mode,
            )
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        except Exception as exc:
            logger.error("Five-table derived recovery failed: %s", exc, exc_info=True)
            raise HTTPException(status_code=500, detail=str(exc)) from exc
        finally:
            await _reopen_pool_after_write("five-table derived recovery")
            _set_serving_or_ops_writing()

    track_event(
        "damaged_derived_reaggregated",
        {"mode": mode, "leagues": result["leagues"], "targets": len(_DAMAGED_DERIVED_TARGETS)},
    )
    return result


@app.post("/replace-db")
async def replace_db(
    request: Request,
    file: UploadFile = _FILE_PARAM,
    x_db_name: str = Header(...),
    x_content_sha256: str = Header(None),
):
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/replace-db"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if x_db_name not in ("___leagues", "___ops", "___ops_nfl"):
        raise HTTPException(status_code=400, detail="Invalid db_name")

    logger.info("Starting DB replace: %s", x_db_name)
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    incoming_path = data_dir / f"{x_db_name}.duckdb.incoming"

    # Stream upload to disk
    written = 0
    with open(incoming_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            written += len(chunk)
            if written > MAX_UPLOAD_BYTES:
                incoming_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="File too large (max 5GB)")
            f.write(chunk)

    # Verify checksum if provided
    if x_content_sha256:
        try:
            verify_checksum(incoming_path, x_content_sha256)
        except SwapError as e:
            incoming_path.unlink(missing_ok=True)
            raise HTTPException(status_code=400, detail=str(e)) from e

    # Serialize write operations (replace-db and merge-league share the lock)
    async with _merge_lock:
        # Drain: wait for in-flight queries to finish
        logger.info("Draining queries (active=%d)", db.get_active_count())
        _state["status"] = "draining"
        elapsed = 0
        while db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
            await asyncio.sleep(0.5)
            elapsed += 0.5
        if db.get_active_count() > 0:
            await asyncio.sleep(HARD_DRAIN_TIMEOUT - SOFT_DRAIN_TIMEOUT)

        # Close all pooled connections
        db.close_all()

        # Swap
        try:
            result = atomic_swap(data_dir, x_db_name)
        except SwapError as e:
            await _reopen_pool_after_write("failed DB replace")
            _set_serving_or_ops_writing()
            raise HTTPException(status_code=500, detail=str(e)) from e

        # Reopen pool
        await _reopen_pool_after_write(f"{x_db_name} replace")
        _set_serving_or_ops_writing()
        logger.info("Swap complete: %s", result)
        track_event("db_replaced", {"db_name": x_db_name, "size_mb": round(written / (1024 * 1024), 1)})

    return result


@app.post("/compact-db")
async def compact_db(request: Request, x_db_name: str = Header(...)):
    """Rewrite a database file in place to reclaim free blocks.

    DuckDB frees blocks on DROP but never truncates the file, so a database that
    once held many tables keeps its peak on-disk size. This rebuilds the live
    file from scratch via COPY FROM DATABASE (which copies all schemas, tables,
    and views) into a fresh `.incoming` file, then reuses the same drain +
    atomic_swap + reopen path as /replace-db. No upload required — the compact
    copy is produced server-side from the live file.
    """
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/compact-db"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if x_db_name not in ("___leagues", "___ops", "___ops_nfl"):
        raise HTTPException(status_code=400, detail="Invalid db_name")

    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    live = data_dir / f"{x_db_name}.duckdb"
    incoming = data_dir / f"{x_db_name}.duckdb.incoming"
    incoming_wal = Path(f"{incoming}.wal")

    def _build_compact() -> dict:
        # Clear any stale incoming (e.g. interrupted prior run) so COPY FROM
        # DATABASE targets a fresh, empty file.
        incoming.unlink(missing_ok=True)
        incoming_wal.unlink(missing_ok=True)
        conn = db.connect_database(live, data_dir=data_dir)
        try:
            # The pool is closed during compaction, so this is the only open
            # connection — give it extra threads and memory so the rebuild of
            # wide tables does not crawl single-threaded or spill to temp.
            conn.execute(f"SET threads={COMPACT_THREADS}")
            conn.execute(f"SET memory_limit='{COMPACT_MEMORY_LIMIT}'")
            conn.execute(f"ATTACH '{incoming.as_posix()}' AS compact")
            conn.execute(f"COPY FROM DATABASE {x_db_name} TO compact")
            conn.execute("CHECKPOINT compact")
            conn.execute("DETACH compact")
        finally:
            conn.close()
        # atomic_swap only renames the bare .incoming file, so the compact copy
        # must be fully checkpointed with no side WAL before the swap.
        incoming_wal.unlink(missing_ok=True)
        return {
            "before_mb": round(live.stat().st_size / (1024 * 1024), 1),
            "after_mb": round(incoming.stat().st_size / (1024 * 1024), 1),
        }

    logger.info("Starting DB compact: %s", x_db_name)
    async with _merge_lock:
        _state["status"] = "draining"
        elapsed = 0
        while db.get_active_count() > 0 and elapsed < SOFT_DRAIN_TIMEOUT:
            await asyncio.sleep(0.5)
            elapsed += 0.5
        if db.get_active_count() > 0:
            await asyncio.sleep(HARD_DRAIN_TIMEOUT - SOFT_DRAIN_TIMEOUT)

        db.close_all()

        try:
            sizes = await asyncio.wait_for(
                asyncio.to_thread(_build_compact),
                timeout=COMPACT_TIMEOUT_SECONDS,
            )
            result = atomic_swap(data_dir, x_db_name)
        except TimeoutError:
            # The COPY thread cannot be cancelled and still holds the file/handle.
            # Exit cleanly so Fly restarts; startup recovery deletes the stale
            # .incoming and the untouched live file is reopened (no swap occurred).
            logger.critical("DB compact exceeded %.0fs; exiting for restart", COMPACT_TIMEOUT_SECONDS)
            os._exit(1)
        except Exception as e:
            incoming.unlink(missing_ok=True)
            incoming_wal.unlink(missing_ok=True)
            await _reopen_pool_after_write("failed DB compact")
            _set_serving_or_ops_writing()
            raise HTTPException(status_code=500, detail=str(e)) from e

        await _reopen_pool_after_write(f"{x_db_name} compact")
        _set_serving_or_ops_writing()
        result.update(sizes)
        logger.info("Compact complete: %s", result)
        track_event("db_compacted", {"db_name": x_db_name, **sizes})

    return result


@app.post("/merge-league")
async def merge_league(
    request: Request,
    file: UploadFile = _FILE_PARAM,
    x_db_name: str = Header(..., description="League db_name (e.g. 'the_league')"),
    x_skip_delete: str | None = Header(None),
):
    """Merge a per-league .duckdb file into ___leagues.

    Accepts a league's local .duckdb file, ATTACHes it, and for each table:
    DELETE existing rows for this db_name, then INSERT new rows.
    This is the legacy whole-file operational rollback path. Current workers
    should prefer /merge-league-delta.
    """
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/merge-league"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if not _DB_NAME_PATTERN.fullmatch(x_db_name):
        raise HTTPException(status_code=400, detail="Invalid db_name")

    logger.info("Starting league merge: %s", x_db_name)
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    leagues_path = data_dir / "___leagues.duckdb"
    incoming_path = data_dir / f"league_upload_{x_db_name}_{time.time_ns()}_{random.randint(100000, 999999)}.duckdb"

    # Stream upload to disk
    written = 0
    with open(incoming_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            written += len(chunk)
            if written > MAX_UPLOAD_BYTES:
                incoming_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="File too large (max 5GB)")
            f.write(chunk)

    logger.info("League file received: %s (%.1f MB)", x_db_name, written / (1024 * 1024))

    # Serialize merge operations — must drain read-only pool first because
    # DuckDB won't allow mixed read-only + read-write connections on same file.
    # The dedicated ___ops connection stays up so frontend queries keep working.
    async with _merge_lock:
        _state["status"] = "draining"
        elapsed_drain = 0
        while db.get_active_count() > 0 and elapsed_drain < SOFT_DRAIN_TIMEOUT:
            await asyncio.sleep(0.5)
            elapsed_drain += 0.5
        db.close_pool()
        _state["status"] = "writing"

        t0 = time.perf_counter()
        skip_delete = str(x_skip_delete or "").strip().lower() in {"1", "true", "yes", "on"}
        try:
            result = await asyncio.to_thread(
                _merge_league_tables,
                leagues_path,
                incoming_path,
                x_db_name,
                skip_delete,
            )
        except Exception as e:
            logger.error("Merge failed for %s: %s", x_db_name, e, exc_info=True)
            track_event("league_merge_failed", {"db_name": x_db_name, "error": type(e).__name__})
            incoming_path.unlink(missing_ok=True)
            db.reopen_pool()
            _set_serving_or_ops_writing()
            raise HTTPException(status_code=500, detail=str(e)) from e

        elapsed = time.perf_counter() - t0
        incoming_path.unlink(missing_ok=True)
        db.reopen_pool()
        _set_serving_or_ops_writing()
        logger.info("Merge complete (%.1fs): %s", elapsed, result)
        track_event(
            "league_merged",
            {
                "db_name": x_db_name,
                "elapsed_seconds": round(elapsed, 2),
                "table_count": len(result.get("tables", {})),
                "size_mb": round(written / (1024 * 1024), 1),
            },
        )

    return result


@app.post("/merge-ops")
async def merge_ops(request: Request, file: UploadFile = _FILE_PARAM):
    """Replace nfl_historical reference tables in ___ops from an uploaded .duckdb bundle.

    The bundle holds the new table(s) (e.g. nfl_player_stats_all, player_nfl_season, player_bio)
    as plain tables -- no db_name tag. Each is swapped into ___ops.nfl_historical via an atomic
    CREATE OR REPLACE. Unlike /replace-db this is per-table, so all other ___ops tables (accounts/
    credentials, franchises, etc.) are preserved, and it never touches ___leagues -- the app stays
    online. This is the supported path for super-table / season-career / bio updates.
    """
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/merge-ops"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if _state["status"] == "starting":
        if not await _wait_for_startup_before_write():
            return _query_busy_response(reason="starting")
    if _state["status"] not in {"serving", "ops_snapshotting"}:
        return _query_busy_response(reason=_state["status"])

    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    incoming_path = data_dir / f"ops_upload_{time.time_ns()}_{random.randint(100000, 999999)}.duckdb"

    written = 0
    with open(incoming_path, "wb") as f:
        while chunk := await file.read(1024 * 1024):
            written += len(chunk)
            if written > MAX_UPLOAD_BYTES:
                incoming_path.unlink(missing_ok=True)
                raise HTTPException(status_code=413, detail="File too large (max 5GB)")
            f.write(chunk)
    logger.info("/merge-ops bundle received: %.1f MB", written / (1024 * 1024))

    t0 = time.perf_counter()
    try:
        result = await asyncio.wait_for(
            asyncio.to_thread(_merge_ops_tables, incoming_path),
            timeout=OPS_MERGE_HARD_EXIT + 30,
        )
    except Exception as e:
        logger.error("/merge-ops failed: %s", e, exc_info=True)
        track_event("ops_merge_failed", {"error": type(e).__name__})
        incoming_path.unlink(missing_ok=True)
        raise HTTPException(status_code=500, detail=str(e)) from e
    finally:
        incoming_path.unlink(missing_ok=True)

    elapsed = time.perf_counter() - t0
    logger.info("/merge-ops complete (%.1fs): %s", elapsed, result)
    track_event(
        "ops_merged",
        {
            "elapsed_seconds": round(elapsed, 2),
            "table_count": len(result.get("tables", {})),
            "size_mb": round(written / (1024 * 1024), 1),
        },
    )
    return result


@app.get("/merge-league-delta/status")
async def merge_league_delta_status(request: Request, db_name: str, bundle_id: str):
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/merge-league-delta/status"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if not _DB_NAME_PATTERN.fullmatch(db_name):
        raise HTTPException(status_code=400, detail="Invalid db_name")
    if not bundle_id:
        raise HTTPException(status_code=400, detail="Missing bundle_id")

    try:
        conn = db.acquire_connection(timeout=5.0)
    except Exception as exc:
        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        leagues_path = data_dir / "___leagues.duckdb"
        if not leagues_path.exists():
            raise HTTPException(status_code=404, detail={"status": "UNKNOWN_BUNDLE", "code": "unknown_bundle"}) from exc
        conn = db.connect_database(leagues_path, data_dir=data_dir)
        release_to_pool = False
    else:
        release_to_pool = True

    try:
        try:
            row = _delta_state_row(conn, db_name, bundle_id)
        except Exception:
            row = None
        if row is None:
            raise HTTPException(status_code=404, detail={"status": "UNKNOWN_BUNDLE", "code": "unknown_bundle"})
        if row.get("status") in {"RECEIVED", "VALIDATED", "STAGED"}:
            return Response(
                content=json.dumps(row, sort_keys=True, default=str),
                media_type="application/json",
                status_code=202,
            )
        if row.get("status") == "CONFLICT":
            return Response(
                content=json.dumps(row, sort_keys=True, default=str),
                media_type="application/json",
                status_code=409,
            )
        return row
    finally:
        if release_to_pool:
            db.release_connection(conn)
        else:
            conn.close()


@app.post("/merge-league-delta")
async def merge_league_delta(
    request: Request,
    file: UploadFile = _FILE_PARAM,
    x_db_name: str = Header(..., description="League db_name (e.g. 'the_league')"),
    x_bundle_id: str | None = Header(None),
    x_bundle_hash: str | None = Header(None),
):
    try:
        validate_admin_token(get_bearer_token(request))
    except AuthError as e:
        track_event("auth_failed", {"endpoint": "/merge-league-delta"})
        raise HTTPException(status_code=401, detail="Unauthorized") from e

    if not _DB_NAME_PATTERN.fullmatch(x_db_name):
        raise HTTPException(status_code=400, detail="Invalid db_name")

    publish_token: str | None = None
    data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    leagues_path = data_dir / "___leagues.duckdb"
    incoming_path = data_dir / f"league_delta_{x_db_name}_{time.time_ns()}_{random.randint(100000, 999999)}.tar.gz"
    extract_dir: Path | None = None

    written = 0
    try:
        with open(incoming_path, "wb") as f:
            while chunk := await file.read(1024 * 1024):
                written += len(chunk)
                if written > _DELTA_MAX_UPLOAD_BYTES:
                    raise HTTPException(status_code=413, detail="Delta bundle too large")
                f.write(chunk)

        validation_start = time.perf_counter()
        _update_delta_publish_slot(
            publish_token,
            "validating",
            size_mb=round(written / (1024 * 1024), 2),
        )
        try:
            manifest, extract_dir = await asyncio.to_thread(
                _validate_delta_archive,
                incoming_path,
                db_name=x_db_name,
                expected_bundle_id=x_bundle_id,
                expected_bundle_hash=x_bundle_hash,
            )
        except DeltaValidationError as exc:
            track_event("league_delta_validation_failed", {"db_name": x_db_name, "error": str(exc)[:200]})
            raise HTTPException(status_code=400, detail=str(exc)) from exc

        logger.info(
            "Delta bundle validated for %s: %s tables, %.1f MB in %.2fs",
            x_db_name,
            len(manifest.get("tables", [])),
            written / (1024 * 1024),
            time.perf_counter() - validation_start,
        )

        publish_token = await _acquire_delta_publish_slot(x_db_name, manifest.get("bundle_id") or x_bundle_id)
        lock_wait_start = time.perf_counter()
        _update_delta_publish_slot(
            publish_token,
            "waiting_merge_lock",
            table_count=len(manifest.get("tables", [])),
            size_mb=round(written / (1024 * 1024), 2),
        )
        async with _merge_lock:
            lock_wait_seconds = time.perf_counter() - lock_wait_start
            merge_start = time.perf_counter()
            _update_delta_publish_slot(
                publish_token,
                "merging",
                lock_wait_seconds=round(lock_wait_seconds, 2),
                table_count=len(manifest.get("tables", [])),
                size_mb=round(written / (1024 * 1024), 2),
            )
            try:
                result = await asyncio.to_thread(_merge_delta_bundle, leagues_path, manifest, extract_dir)
            except DeltaConflictError as exc:
                track_event("league_delta_conflict", {"db_name": x_db_name, "bundle_id": manifest.get("bundle_id")})
                raise HTTPException(status_code=409, detail=str(exc)) from exc
            except Exception as exc:
                logger.error("Delta merge failed for %s: %s", x_db_name, exc, exc_info=True)
                track_event("league_delta_merge_failed", {"db_name": x_db_name, "error": type(exc).__name__})
                raise HTTPException(status_code=500, detail=str(exc)) from exc

            db.refresh_metadata()
            elapsed = time.perf_counter() - merge_start
            result["merge_seconds"] = round(elapsed, 4)
            result["lock_wait_seconds"] = round(lock_wait_seconds, 4)
            result["server_total_seconds"] = round(time.perf_counter() - validation_start, 4)
            logger.info("Delta merge complete (%.2fs): %s", elapsed, result)
            track_event(
                "league_delta_merged",
                {
                    "db_name": x_db_name,
                    "bundle_id": manifest.get("bundle_id"),
                    "elapsed_seconds": round(elapsed, 2),
                    "lock_wait_seconds": round(lock_wait_seconds, 2),
                    "table_count": result.get("table_count", 0),
                    "size_mb": round(written / (1024 * 1024), 1),
                },
            )
            return result
    finally:
        _release_delta_publish_slot(publish_token)
        incoming_path.unlink(missing_ok=True)
        if extract_dir is not None:
            shutil.rmtree(extract_dir, ignore_errors=True)


def _merge_league_tables(leagues_path: Path, league_path: Path, db_name: str, skip_delete: bool = False) -> dict:
    """Merge a league's tables into ___leagues.duckdb (runs in thread).

    Only merges columns that exist in the target table. If the target table
    doesn't exist yet, creates it from the incoming schema with a db_name
    column prepended. Uses TRY_CAST to handle type mismatches across platforms.
    """
    hard_exit_timer = _start_merge_hard_exit_timer(db_name)
    conn = None
    attached = False

    try:
        conn = db.connect_database(leagues_path, data_dir=db.get_data_dir(), threads=WRITE_DUCKDB_THREADS)
        conn.execute(f"ATTACH '{league_path}' AS _incoming (READ_ONLY)")
        attached = True

        def qident(name: str) -> str:
            return '"' + str(name).replace('"', '""') + '"'

        def merge_state_ref() -> str:
            return f"{qident(_MERGE_STATE_SCHEMA)}.{qident(_MERGE_STATE_TABLE)}"

        def table_ref_for(table: str) -> str:
            return f"public.{qident(table)}"

        def incoming_ref_for(table: str) -> str:
            return f"_incoming.public.{qident(table)}"

        def target_exists(table: str) -> bool:
            try:
                conn.execute(f"DESCRIBE {table_ref_for(table)}")
                return True
            except Exception:
                return False

        def target_has_db_rows(table: str) -> bool:
            if not target_exists(table):
                return False
            try:
                row = conn.execute(
                    f"SELECT 1 FROM {table_ref_for(table)} WHERE db_name = ? LIMIT 1",
                    [db_name],
                ).fetchone()
                return row is not None
            except Exception as exc:
                logger.warning(
                    "Could not inspect existing rows for fast-path guard %s.%s: %s",
                    db_name,
                    table,
                    exc,
                )
                return True

        def ensure_merge_state_table() -> None:
            conn.execute(f"CREATE SCHEMA IF NOT EXISTS {qident(_MERGE_STATE_SCHEMA)}")
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {merge_state_ref()} (
                    db_name VARCHAR,
                    merge_id VARCHAR,
                    status VARCHAR,
                    started_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    table_count INTEGER,
                    error VARCHAR
                )
                """
            )

        def has_prior_merge_state() -> bool:
            row = conn.execute(
                f"SELECT 1 FROM {merge_state_ref()} WHERE db_name = ? LIMIT 1",
                [db_name],
            ).fetchone()
            return row is not None

        def start_merge_state(merge_id: str, table_count: int) -> None:
            conn.execute(f"DELETE FROM {merge_state_ref()} WHERE db_name = ?", [db_name])
            conn.execute(
                f"""
                INSERT INTO {merge_state_ref()}
                    (db_name, merge_id, status, started_at, updated_at, table_count, error)
                VALUES (?, ?, 'running', current_timestamp, current_timestamp, ?, NULL)
                """,
                [db_name, merge_id, table_count],
            )

        def finish_merge_state(status: str, error: str | None = None) -> None:
            conn.execute(
                f"""
                UPDATE {merge_state_ref()}
                SET status = ?,
                    updated_at = current_timestamp,
                    error = ?
                WHERE db_name = ?
                """,
                [status, error, db_name],
            )

        def is_fast_path_sentinel(table: str, incoming_count: int) -> bool:
            lowered = table.lower()
            if any(lowered.startswith(prefix) for prefix in _FAST_PATH_EXCLUDED_PREFIXES):
                return False
            return incoming_count <= _FAST_PATH_SENTINEL_MAX_INCOMING_ROWS

        # Discover tables in the incoming file
        incoming_tables = [
            row[2]
            for row in conn.execute(
                "SELECT database, schema, name FROM (SHOW ALL TABLES) "
                "WHERE database = '_incoming' AND schema = 'public'"
            ).fetchall()
        ]
        incoming_counts = {
            table_name: conn.execute(f"SELECT COUNT(*) FROM {incoming_ref_for(table_name)}").fetchone()[0]
            for table_name in incoming_tables
        }

        ensure_merge_state_table()
        merge_id = f"{int(time.time())}-{random.randint(100000, 999999)}"
        prior_merge_state = has_prior_merge_state()
        requested_skip_delete = skip_delete
        if skip_delete and prior_merge_state:
            logger.warning(
                "Disabling skip delete for %s because a prior merge state exists; retry will replace rows",
                db_name,
            )
            skip_delete = False

        if skip_delete:
            sentinel_tables = [
                table_name
                for table_name in incoming_tables
                if is_fast_path_sentinel(table_name, incoming_counts[table_name])
            ][:_FAST_PATH_SENTINEL_MAX_TABLES]
            for table_name in sentinel_tables:
                if target_has_db_rows(table_name):
                    logger.warning(
                        "Disabling skip delete for %s because existing rows were found in %s; retry will replace rows",
                        db_name,
                        table_name,
                    )
                    skip_delete = False
                    break

        start_merge_state(merge_id, len(incoming_tables))

        merged = {}
        try:
            for table_name in incoming_tables:
                table_start = time.perf_counter()
                table_ref = table_ref_for(table_name)
                incoming_ref = incoming_ref_for(table_name)
                incoming_schema = conn.execute(f"DESCRIBE {incoming_ref}").fetchall()
                incoming_col_types = {row[0]: row[1] for row in incoming_schema}
                incoming_cols = list(incoming_col_types.keys())
                source_cols = [c for c in incoming_cols if c != "db_name"]
                incoming_count = incoming_counts[table_name]
                logger.info("Merging table %s for %s (%s incoming rows)", table_name, db_name, incoming_count)

                # Let DuckDB autocommit each statement. Explicit transactions
                # made the row insert itself quick but pushed all file work into
                # COMMIT, which repeatedly hung the live writer.
                target_table_exists = target_exists(table_name)
                if not target_table_exists:
                    if source_cols:
                        select_cols = ", ".join(qident(c) for c in source_cols)
                        _interrupting_execute(
                            conn,
                            f"CREATE TABLE {table_ref} AS "
                            f"SELECT CAST(NULL AS VARCHAR) AS db_name, {select_cols} "
                            f"FROM {incoming_ref} WHERE FALSE",
                            step=f"create {db_name}.{table_name}",
                        )
                    else:
                        _interrupting_execute(
                            conn,
                            f"CREATE TABLE {table_ref} (db_name VARCHAR)",
                            step=f"create {db_name}.{table_name}",
                        )

                # Build column type map for the target table
                target_col_types = {row[0]: row[1] for row in conn.execute(f"DESCRIBE {table_ref}").fetchall()}

                # Keep the centralized table schema moving with canonical DDL.
                # Older server code silently dropped new incoming columns because it
                # only inserted target-common fields. That broke additions like
                # Yahoo-native player stat columns: local imports had the data, but
                # the live shared table never learned the columns. Add any incoming
                # columns first, then rebuild the target type map.
                missing_cols = [c for c in source_cols if c not in target_col_types]
                if missing_cols:
                    for c in missing_cols:
                        _interrupting_execute(
                            conn,
                            f"ALTER TABLE {table_ref} ADD COLUMN {qident(c)} {incoming_col_types[c]}",
                            step=f"add column {db_name}.{table_name}.{c}",
                        )
                    target_col_types = {row[0]: row[1] for row in conn.execute(f"DESCRIBE {table_ref}").fetchall()}

                # Only insert columns that exist in target
                common_cols = [c for c in source_cols if c in target_col_types]
                insert_cols = ["db_name"] + common_cols

                # Build SELECT expressions with TRY_CAST only when the stored
                # type really differs. For same-type inserts, direct projection
                # keeps tiny first imports from paying unnecessary cast overhead
                # on every column.
                select_exprs = ["? AS db_name"]
                for c in common_cols:
                    target_type = target_col_types[c]
                    incoming_type = incoming_col_types.get(c)
                    if incoming_type and incoming_type.upper() == target_type.upper():
                        select_exprs.append(qident(c))
                    else:
                        select_exprs.append(f"TRY_CAST({qident(c)} AS {target_type}) AS {qident(c)}")

                if skip_delete:
                    logger.info("Skipping delete for first centralized import of %s.%s", db_name, table_name)
                else:
                    delete_start = time.perf_counter()
                    _interrupting_execute(
                        conn,
                        f"DELETE FROM {table_ref} WHERE db_name = ?",
                        [db_name],
                        step=f"delete {db_name}.{table_name}",
                    )
                    logger.info(
                        "Deleted old rows from %s for %s in %.2fs",
                        table_name,
                        db_name,
                        time.perf_counter() - delete_start,
                    )
                insert_start = time.perf_counter()
                _interrupting_execute(
                    conn,
                    f"INSERT INTO {table_ref} ("
                    + ", ".join(qident(c) for c in insert_cols)
                    + ") SELECT "
                    + ", ".join(select_exprs)
                    + f" FROM {incoming_ref}",
                    [db_name],
                    step=f"insert {db_name}.{table_name}",
                )
                logger.info(
                    "Inserted and committed %s rows into %s for %s in %.2fs",
                    incoming_count,
                    table_name,
                    db_name,
                    time.perf_counter() - insert_start,
                )
                logger.info(
                    "Merged table %s for %s in %.2fs",
                    table_name,
                    db_name,
                    time.perf_counter() - table_start,
                )
                merged[table_name] = incoming_count
        except Exception as exc:
            finish_merge_state("failed", f"{type(exc).__name__}: {exc}")
            raise

        _checkpoint_connection_if_wal_large(conn, leagues_path, reason=f"legacy merge {db_name}")
        finish_merge_state("complete")

    finally:
        hard_exit_timer.cancel()
        if conn is not None:
            if attached:
                try:
                    conn.execute("DETACH _incoming")
                except Exception as exc:
                    logger.warning("Failed to detach incoming league DB for %s: %s", db_name, exc)
            conn.close()

    return {
        "status": "merged",
        "db_name": db_name,
        "tables": merged,
        "skip_delete_requested": requested_skip_delete,
        "skip_delete_used": skip_delete,
    }
