"""DuckDB connection pool with read-only enforcement."""

import logging
import os
import hashlib
import queue
import shutil
import threading
from pathlib import Path
from datetime import datetime, UTC

import duckdb

logger = logging.getLogger(__name__)

POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "5"))
# Threads are database-wide, not per connection. Keep the configured writer
# capacity without asking readers and writers to open with conflicting options.
DEFAULT_DUCKDB_THREADS = max(
    int(os.environ.get("DUCKDB_THREADS", "2")),
    int(os.environ.get("DUCKDB_WRITE_THREADS", "1")),
)
DEFAULT_DUCKDB_MEMORY_FRACTION = float(os.environ.get("DUCKDB_MEMORY_FRACTION", "0.38"))
DEFAULT_DUCKDB_TEMP_LIMIT_GIB = int(os.environ.get("DUCKDB_DEFAULT_TEMP_LIMIT_GIB", "20"))
DEFAULT_DUCKDB_CHECKPOINT_THRESHOLD = os.environ.get("DUCKDB_CHECKPOINT_THRESHOLD", "512MB")
STORAGE_RECOVERY_CHECKPOINT_THRESHOLD = "100TB"

_data_dir: Path | None = None
_pool: queue.Queue[duckdb.DuckDBPyConnection] = queue.Queue()
_ops_conn: duckdb.DuckDBPyConnection | None = None
_ops_lock = threading.Lock()
_metadata: dict[str, dict] = {}
_active_count: int = 0
_count_lock = threading.Lock()
_temp_limit_lock = threading.Lock()
_temp_limit_by_data_dir: dict[Path, str] = {}
_storage_recovery_mode = False


def set_storage_recovery_mode(active: bool) -> None:
    """Keep the repaired database WAL-only while corrupt storage is quarantined."""
    global _storage_recovery_mode
    _storage_recovery_mode = bool(active)


def is_storage_recovery_mode() -> bool:
    return _storage_recovery_mode


def _effective_checkpoint_threshold() -> str:
    if _storage_recovery_mode:
        return STORAGE_RECOVERY_CHECKPOINT_THRESHOLD
    return DEFAULT_DUCKDB_CHECKPOINT_THRESHOLD


def _apply_storage_recovery_guardrails(conn) -> None:
    if not _storage_recovery_mode:
        return
    conn.execute(f"SET checkpoint_threshold='{STORAGE_RECOVERY_CHECKPOINT_THRESHOLD}'")
    conn.execute("PRAGMA disable_checkpoint_on_shutdown")


def _compute_memory_limit() -> str:
    """Derive DuckDB memory_limit from available system RAM.
    Uses ~38% (1536MB on 4GB). os.sysconf is POSIX-only — falls back to 1536MB on Windows.
    """
    explicit = os.environ.get("DUCKDB_MEMORY_LIMIT")
    if explicit:
        return explicit
    try:
        mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
        limit_mb = max(512, int(mem_bytes * DEFAULT_DUCKDB_MEMORY_FRACTION / (1024 * 1024)))
        return f"{limit_mb}MB"
    except Exception:
        return "1536MB"


def _compute_temp_directory_limit(data_dir: Path | None) -> str:
    explicit = os.environ.get("DUCKDB_MAX_TEMP_DIRECTORY_SIZE")
    if explicit:
        return explicit
    if not data_dir:
        return f"{DEFAULT_DUCKDB_TEMP_LIMIT_GIB}GiB"
    # This value is a connect-time DuckDB database-instance setting. Recomputing
    # it from current free space for each pool/writer connection can make the
    # second connection incompatible with the first as the disk crosses a GiB
    # boundary. The actual free disk still limits spills independently.
    key = Path(data_dir).resolve()
    with _temp_limit_lock:
        if key not in _temp_limit_by_data_dir:
            try:
                usage = shutil.disk_usage(key)
                free_gib = usage.free // (1024**3)
                limit_gib = max(2, min(DEFAULT_DUCKDB_TEMP_LIMIT_GIB, int(free_gib * 0.75)))
                _temp_limit_by_data_dir[key] = f"{limit_gib}GiB"
            except Exception:
                _temp_limit_by_data_dir[key] = f"{DEFAULT_DUCKDB_TEMP_LIMIT_GIB}GiB"
        return _temp_limit_by_data_dir[key]


def duckdb_connection_config(data_dir: Path | None = None, *, threads: int | None = None) -> dict:
    """Return DuckDB config to apply before opening a database file."""
    limit = _compute_memory_limit()
    thread_count = threads or DEFAULT_DUCKDB_THREADS
    temp_limit = _compute_temp_directory_limit(data_dir)
    config = {
        "memory_limit": limit,
        "threads": str(thread_count),
        "preserve_insertion_order": "false",
        # Connect-time options must match every handle already open on this
        # database instance. Recovery mode is applied immediately after open
        # with SET/PRAGMA so it does not create a configuration conflict.
        "checkpoint_threshold": DEFAULT_DUCKDB_CHECKPOINT_THRESHOLD,
    }
    if data_dir:
        tmp_dir = data_dir / "duckdb_tmp"
        tmp_dir.mkdir(exist_ok=True)
        config["temp_directory"] = str(tmp_dir)
        config["max_temp_directory_size"] = temp_limit
    return config


def _log_duckdb_guardrails(data_dir: Path | None = None, *, threads: int | None = None) -> None:
    thread_count = threads or DEFAULT_DUCKDB_THREADS
    logger.info(
        "DuckDB guardrails: memory_limit=%s, threads=%s, temp_limit=%s, checkpoint_threshold=%s",
        _compute_memory_limit(),
        thread_count,
        _compute_temp_directory_limit(data_dir),
        _effective_checkpoint_threshold(),
    )


def get_runtime_config(data_dir: Path | None = None) -> dict[str, str | int]:
    """Return the effective DuckDB guardrails for status endpoints."""
    return {
        "pool_size": POOL_SIZE,
        "memory_limit": _compute_memory_limit(),
        "threads": DEFAULT_DUCKDB_THREADS,
        "temp_limit": _compute_temp_directory_limit(data_dir or _data_dir),
        "checkpoint_threshold": _effective_checkpoint_threshold(),
    }


def connect_database(
    path: Path | str,
    *,
    read_only: bool = False,
    data_dir: Path | None = None,
    threads: int | None = None,
):
    """Open DuckDB once with the shared database-instance configuration.

    ``threads`` is retained for caller compatibility, but cannot vary between
    handles to the same database. The configured database-wide default wins.
    """
    resolved_path = Path(path)
    resolved_data_dir = data_dir or _data_dir or resolved_path.parent
    config = duckdb_connection_config(resolved_data_dir)
    # A storage/replay error is not a reason to reopen without guardrails.
    # Fail once with the original error and configuration intact.
    conn = duckdb.connect(str(resolved_path), read_only=read_only, config=config)
    _log_duckdb_guardrails(resolved_data_dir)
    _apply_access_hardening(conn, resolved_data_dir)
    _apply_storage_recovery_guardrails(conn)
    return conn


def _apply_access_hardening(conn, data_dir: Path | None = None) -> None:
    """Engine-level lockdown for the shared database instance (2026-07-11 audit P0).

    Every legitimate server operation — the pool's lazy ATTACH of ___ops, the
    ___ops_nfl attach, merge/compact staging files, temp spill — lives under DATA_DIR,
    so `allowed_directories` keeps them all working while `enable_external_access =
    false` kills file/URL readers outside it (read_csv('/etc/...'), http reads) at
    the ENGINE level, behind the SQL-validator regexes. Extension autoinstall and
    autoload go dark too: all five extensions in use are statically linked into the
    duckdb wheel. These settings are GLOBAL scope — connections to the same file
    share one database instance, so admin connections to the same paths inherit the
    lockdown. That is also why lock_configuration is deliberately NOT set: merge and
    reopen paths still SET memory/temp limits on the shared instance, and the query
    layer already blocks SET via the statement start-pattern."""
    allowed = (data_dir or get_data_dir()).as_posix()
    try:
        conn.execute(f"SET allowed_directories = ['{allowed}']")
        conn.execute("SET enable_external_access = false")
        conn.execute("SET autoinstall_known_extensions = false")
        conn.execute("SET autoload_known_extensions = false")
    except duckdb.Error as exc:
        logger.warning("DuckDB access hardening could not be applied: %s", exc)


def _attach_ops_nfl(conn) -> None:
    """Attach the read-only ___ops_nfl super-table file so the ___ops views resolve on this
    connection (the split moves nfl_player_stats_all + companions into ___ops_nfl.duckdb and
    leaves views behind in ___ops). ___ops_nfl is READ_ONLY and only ever changes via a
    whole-file swap under close_all, so a permanent attach has no incremental-write /
    pool-rebuild cost. No-op until the file exists (pre-split bootstrap)."""
    if conn is None:
        return
    path = get_data_dir() / "___ops_nfl.duckdb"
    if path.exists():
        conn.execute(f'ATTACH IF NOT EXISTS \'{path.as_posix()}\' AS "___ops_nfl" (READ_ONLY)')


def configure_duckdb(conn, data_dir: Path | None = None, *, threads: int | None = None):
    """Apply global DuckDB guardrails. Called once per database instance."""
    limit = _compute_memory_limit()
    thread_count = threads or DEFAULT_DUCKDB_THREADS
    temp_limit = _compute_temp_directory_limit(data_dir)
    if data_dir:
        tmp_dir = data_dir / "duckdb_tmp"
        tmp_dir.mkdir(exist_ok=True)
        try:
            conn.execute(f"SET temp_directory = '{tmp_dir}'")
        except duckdb.NotImplementedException as exc:
            logger.warning("DuckDB temp_directory already initialized; keeping current temp dir: %s", exc)
        try:
            conn.execute(f"SET max_temp_directory_size = '{temp_limit}'")
        except duckdb.Error as exc:
            logger.warning("DuckDB max_temp_directory_size could not be adjusted; keeping current limit: %s", exc)
    conn.execute(f"SET memory_limit = '{limit}'")
    conn.execute(f"SET threads = {thread_count}")
    conn.execute("SET preserve_insertion_order = false")
    try:
        conn.execute(f"SET checkpoint_threshold = '{_effective_checkpoint_threshold()}'")
    except duckdb.Error as exc:
        logger.warning("DuckDB checkpoint_threshold could not be adjusted; keeping current threshold: %s", exc)
    _log_duckdb_guardrails(data_dir, threads=threads)


def ensure_ops_credential_schema(conn) -> list[str]:
    """Provision application metadata once, before the ops reader is opened.

    Normal request and worker paths are intentionally DML-only. Keeping all
    idempotent schema repair here prevents concurrent request-time DDL from
    invalidating the shared DuckDB catalog or WAL.
    """
    operations: list[str] = []

    for schema in ("main", "accounts"):
        exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.schemata WHERE schema_name = ?",
            [schema],
        ).fetchone()[0]
        if not exists:
            conn.execute(f'CREATE SCHEMA "{schema}"')
            operations.append(f"create_schema:{schema}")

    table_specs = {
        ("main", "league_credentials"): ({
            "league_id": "TEXT NOT NULL",
            "league_name": "TEXT",
            "database_name": "TEXT",
            "encrypted_refresh_token": "TEXT",
            "updated_at": "TIMESTAMP DEFAULT current_timestamp",
        }, ('PRIMARY KEY ("league_id")',)),
        ("main", "yahoo_web_credentials"): ({
            "league_id": "TEXT NOT NULL",
            "league_name": "TEXT",
            "database_name": "TEXT",
            "encrypted_cookie_jar": "TEXT NOT NULL",
            "cookie_format": "TEXT NOT NULL DEFAULT 'json'",
            "captured_at": "TIMESTAMP DEFAULT current_timestamp",
            "expires_at": "TIMESTAMP",
            "status": "TEXT NOT NULL DEFAULT 'active'",
            "updated_at": "TIMESTAMP DEFAULT current_timestamp",
        }, ('PRIMARY KEY ("league_id")', 'UNIQUE ("database_name")')),
        ("main", "espn_leagues"): ({
            "espn_league_id": "TEXT NOT NULL",
            "league_name": "TEXT",
            "database_name": "TEXT",
            "encrypted_espn_s2": "TEXT",
            "encrypted_swid": "TEXT",
            "created_at": "TIMESTAMP DEFAULT current_timestamp",
            "updated_at": "TIMESTAMP DEFAULT current_timestamp",
        }, ('PRIMARY KEY ("espn_league_id")',)),
        ("main", "sleeper_leagues"): ({
            "sleeper_league_id": "TEXT NOT NULL",
            "league_name": "TEXT",
            "database_name": "TEXT",
            "created_at": "TIMESTAMP DEFAULT current_timestamp",
            "updated_at": "TIMESTAMP DEFAULT current_timestamp",
        }, ('PRIMARY KEY ("sleeper_league_id")',)),
        ("main", "research_extraction_cache"): ({
            "cache_key": "TEXT NOT NULL",
            "value_json": "TEXT NOT NULL",
            "created_at": "TIMESTAMP DEFAULT current_timestamp",
            "hit_count": "INTEGER DEFAULT 0",
            "last_hit_at": "TIMESTAMP",
        }, ('PRIMARY KEY ("cache_key")',)),
        ("accounts", "league_inventory"): ({
            "database_name": "VARCHAR NOT NULL",
            "platform": "VARCHAR",
            "league_name": "VARCHAR",
            "league_id": "VARCHAR",
            "tier": "VARCHAR DEFAULT 'free'",
            "entitled_mode": "VARCHAR DEFAULT 'quick'",
            "last_import_mode": "VARCHAR",
            "last_import_at": "TIMESTAMP",
            "in_centralized": "BOOLEAN DEFAULT FALSE",
            "num_teams": "INTEGER",
            "first_year": "INTEGER",
            "last_year": "INTEGER",
            "scoring_variant": "VARCHAR",
            "has_credentials": "BOOLEAN DEFAULT FALSE",
            "import_status": "VARCHAR",
            "email": "VARCHAR",
            "payment_date": "TIMESTAMP",
            "expires_at": "TIMESTAMP",
            "stripe_session_id": "VARCHAR",
            "stripe_payment_id": "VARCHAR",
            "amount_paid": "BIGINT",
            "created_at": "TIMESTAMP DEFAULT current_timestamp",
            "updated_at": "TIMESTAMP DEFAULT current_timestamp",
        }, ('PRIMARY KEY ("database_name")',)),
        ("accounts", "pending_paid_imports"): ({
            "stripe_session_id": "VARCHAR NOT NULL",
            "database_name": "VARCHAR NOT NULL",
            "platform": "VARCHAR NOT NULL",
            "league_name": "VARCHAR",
            "import_payload_json": "VARCHAR NOT NULL",
            "created_at": "TIMESTAMP DEFAULT NOW()",
        }, ('PRIMARY KEY ("stripe_session_id")',)),
        ("accounts", "stripe_processed_sessions"): ({
            "stripe_session_id": "VARCHAR NOT NULL",
            "processed_at": "TIMESTAMP DEFAULT NOW()",
        }, ('PRIMARY KEY ("stripe_session_id")',)),
        ("accounts", "paid_import_dispatches"): ({
            "stripe_session_id": "VARCHAR NOT NULL",
            "database_name": "VARCHAR NOT NULL",
            "platform": "VARCHAR NOT NULL",
            "status": "VARCHAR NOT NULL",
            "user_id": "VARCHAR",
            "workflow_file": "VARCHAR",
            "workflow_run_id": "BIGINT",
            "dispatched_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "error": "VARCHAR",
        }, ('PRIMARY KEY ("stripe_session_id")',)),
        ("accounts", "league_update_manifests"): ({
            "database_name": "VARCHAR NOT NULL",
            "platform": "VARCHAR",
            "active_season": "INTEGER",
            "through_week": "INTEGER",
            "observed_manifest_json": "VARCHAR",
            "observed_manifest_digest": "VARCHAR",
            "published_manifest_json": "VARCHAR",
            "published_manifest_digest": "VARCHAR",
            "published_at": "TIMESTAMP",
            "probe_status": "VARCHAR NOT NULL DEFAULT 'unknown'",
            "probe_error_code": "VARCHAR",
            "last_attempt_at": "TIMESTAMP",
            "last_success_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP NOT NULL DEFAULT NOW()",
        }, ('PRIMARY KEY ("database_name")',)),
        ("accounts", "league_update_dispatches"): ({
            "database_name": "VARCHAR NOT NULL",
            "platform": "VARCHAR NOT NULL",
            "status": "VARCHAR NOT NULL",
            "workflow_file": "VARCHAR",
            "workflow_run_id": "BIGINT",
            "dispatch_token": "VARCHAR",
            "source_year": "INTEGER",
            "source_week": "INTEGER",
            "source_fingerprint": "VARCHAR",
            "publish_generation": "VARCHAR",
            "healthy": "BOOLEAN DEFAULT FALSE",
            "dispatched_at": "TIMESTAMP",
            "started_at": "TIMESTAMP",
            "completed_at": "TIMESTAMP",
            "lease_expires_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "error": "VARCHAR",
            "attempt_id": "VARCHAR",
            "claim_version": "BIGINT DEFAULT 0",
            "heartbeat_at": "TIMESTAMP",
            "observed_manifest_digest": "VARCHAR",
            "base_generation": "VARCHAR",
            "bundle_id": "VARCHAR",
            "cache_state": "VARCHAR",
            "committed_at": "TIMESTAMP",
            "cache_verified_at": "TIMESTAMP",
            "publication_receipt_json": "VARCHAR",
        }, ('PRIMARY KEY ("database_name")',)),
        ("accounts", "league_update_rate_buckets"): ({
            "bucket_key": "VARCHAR NOT NULL",
            "request_count": "BIGINT NOT NULL",
            "window_started_at": "TIMESTAMP NOT NULL",
            "reset_at": "TIMESTAMP NOT NULL",
            "updated_at": "TIMESTAMP NOT NULL DEFAULT NOW()",
        }, ('PRIMARY KEY ("bucket_key")',)),
        ("accounts", "league_update_probe_leases"): ({
            "database_name": "VARCHAR NOT NULL",
            "lease_token": "VARCHAR NOT NULL",
            "lease_expires_at": "TIMESTAMP NOT NULL",
            "updated_at": "TIMESTAMP NOT NULL DEFAULT NOW()",
        }, ('PRIMARY KEY ("database_name")',)),
        ("accounts", "offseason_draft_update_dispatches"): ({
            "database_name": "VARCHAR NOT NULL",
            "draft_year": "INTEGER NOT NULL",
            "platform": "VARCHAR NOT NULL",
            "status": "VARCHAR NOT NULL",
            "workflow_file": "VARCHAR",
            "workflow_run_id": "BIGINT",
            "dispatch_token": "VARCHAR",
            "dispatched_at": "TIMESTAMP",
            "started_at": "TIMESTAMP",
            "completed_at": "TIMESTAMP",
            "lease_expires_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT NOW()",
            "error": "VARCHAR",
        }, ('PRIMARY KEY ("database_name", "draft_year")',)),
    }
    for (schema, table), (columns, constraints) in table_specs.items():
        present = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            ).fetchall()
        }
        if not present:
            definitions = ", ".join(
                [f'"{name}" {definition}' for name, definition in columns.items()]
                + list(constraints)
            )
            conn.execute(f'CREATE TABLE "{schema}"."{table}" ({definitions})')
            operations.append(f"create_table:{schema}.{table}")
            continue
        for name, definition in columns.items():
            if name not in present:
                migration_definition = definition.replace(" NOT NULL", "")
                conn.execute(
                    f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{name}" {migration_definition}'
                )
                operations.append(f"add_column:{schema}.{table}.{name}")
    return operations


def init_pool():
    """Open all connections (pool + ___ops). Called at startup and after full swaps."""
    global _data_dir, _metadata, _active_count, _ops_conn, POOL_SIZE
    POOL_SIZE = int(os.environ.get("DB_POOL_SIZE", "5"))
    _data_dir = Path(os.environ.get("DATA_DIR", "/data"))
    leagues_path = _data_dir / "___leagues.duckdb"
    ops_path = _data_dir / "___ops.duckdb"

    # Bootstrap empty databases if they don't exist yet (fresh volume)
    for path in (leagues_path, ops_path):
        if not path.exists():
            conn = connect_database(path, data_dir=_data_dir)
            conn.execute("CREATE SCHEMA IF NOT EXISTS public")
            conn.close()

    # Drain any existing pool connections
    close_pool()

    # Close existing ___ops connection
    if _ops_conn is not None:
        try:
            _ops_conn.close()
        except Exception:
            pass
        _ops_conn = None

    ops_schema_conn = connect_database(ops_path, data_dir=_data_dir)
    try:
        schema_operations = ensure_ops_credential_schema(ops_schema_conn)
        if schema_operations:
            logger.info("Provisioned ___ops application schema: %s", ", ".join(schema_operations))
    finally:
        ops_schema_conn.close()

    # Dedicated OPS control connection; it shares the pool's primary DuckDB
    # instance but is never checked out to public requests. Establish the
    # shared ___ops attachment before opening any public pool handles. Doing
    # this in the opposite order can leave several handles on the instance
    # while DuckDB recreates the attachment after a write, producing a
    # persistent unique-file-handle conflict on every retry.
    if ops_path.exists():
        # Keep ___ops attached once to the same DuckDB instance as ___leagues.
        # DuckDB rejects a writable primary handle for a file that is also an
        # attachment. Sharing one attachment preserves cross-catalog reads and
        # lets tiny serialized OPS writes reuse a hot handle.
        _ops_conn = connect_database(leagues_path, read_only=False, data_dir=_data_dir)
        _ops_conn.execute(
            f"ATTACH IF NOT EXISTS '{ops_path.as_posix()}' AS \"___ops\""
        )
        _attach_ops_nfl(_ops_conn)
        _ops_conn.execute('USE "___ops"')

    # Pool connections are normal DuckDB connections so reads can keep a
    # snapshot open while delta merges commit in another connection. They are
    # opened only after the shared OPS catalog is stable.
    _refill_pool(leagues_path)

    _active_count = 0
    _metadata = _compute_metadata()


def reopen_pool():
    """Reopen only the ___leagues pool after a merge. Leaves ___ops untouched."""
    global _active_count
    leagues_path = _data_dir / "___leagues.duckdb"
    _refill_pool(leagues_path)
    _active_count = 0


def _refill_pool(leagues_path: Path):
    """Create fresh public query pool connections."""
    for _ in range(POOL_SIZE):
        conn = connect_database(leagues_path, read_only=False, data_dir=_data_dir)
        _attach_ops_nfl(conn)
        _pool.put(conn)


def _compute_metadata() -> dict[str, dict]:
    result = {}
    for name in ("___leagues", "___ops"):
        path = _data_dir / f"{name}.duckdb"
        if path and path.exists():
            stat = path.stat()
            # Hash first+last 1MB for quick fingerprint
            try:
                with open(path, "rb") as f:
                    data = f.read(1024 * 1024)
                    f.seek(max(0, stat.st_size - 1024 * 1024))
                    data += f.read()
                file_hash = f"sha256:{hashlib.sha256(data).hexdigest()[:16]}"
            except OSError:
                # Windows denies raw file reads while DuckDB has an open RW
                # handle. Keep metadata available; Linux production still gets
                # the quick fingerprint.
                file_hash = _metadata.get(name, {}).get("file_hash", "unknown")
            result[name] = {
                "file_hash": file_hash,
                "last_replaced": datetime.fromtimestamp(stat.st_mtime, tz=UTC).isoformat(),
                "size_mb": round(stat.st_size / (1024 * 1024), 1),
            }
    return result


def acquire_connection(timeout: float = 30.0) -> duckdb.DuckDBPyConnection:
    """Get a connection from the pool. Blocks up to timeout seconds."""
    try:
        conn = _pool.get(timeout=timeout)
    except queue.Empty as e:
        raise RuntimeError("Connection pool exhausted (all connections checked out)") from e
    with _count_lock:
        global _active_count
        _active_count += 1
    _apply_storage_recovery_guardrails(conn)
    return conn


def release_connection(conn: duckdb.DuckDBPyConnection):
    """Return a connection to the pool."""
    with _count_lock:
        global _active_count
        _active_count -= 1
    _pool.put(conn)


def get_active_count() -> int:
    """Number of connections currently checked out (in-flight queries)."""
    with _count_lock:
        return _active_count


def get_ops_connection() -> duckdb.DuckDBPyConnection | None:
    """Return the dedicated ___ops connection (no pool contention)."""
    return _ops_conn


def close_ops_connection():
    """Detach ___ops from the shared instance and close its control handle."""
    global _ops_conn
    if _ops_conn is not None:
        try:
            _ops_conn.execute('USE "___leagues"')
            _ops_conn.execute('DETACH "___ops"')
        except Exception:
            pass
        try:
            _ops_conn.close()
        except Exception:
            pass
        _ops_conn = None


def reopen_ops_connection():
    """Reattach ___ops and reopen its dedicated control connection."""
    global _ops_conn
    data_dir = get_data_dir()
    leagues_path = data_dir / "___leagues.duckdb"
    ops_path = data_dir / "___ops.duckdb"
    close_ops_connection()
    if leagues_path.exists() and ops_path.exists():
        _ops_conn = connect_database(leagues_path, read_only=False, data_dir=data_dir)
        _ops_conn.execute(
            f"ATTACH IF NOT EXISTS '{ops_path.as_posix()}' AS \"___ops\""
        )
        _attach_ops_nfl(_ops_conn)
        _ops_conn.execute('USE "___ops"')
    return _ops_conn


def close_pool():
    """Close only the ___leagues pool connections, leaving ___ops alive."""
    while not _pool.empty():
        try:
            _pool.get_nowait().close()
        except Exception:
            pass


def close_all():
    """Close all connections including ___ops (for full swap)."""
    close_pool()
    close_ops_connection()


def get_metadata() -> dict:
    return _metadata


def refresh_metadata() -> dict:
    """Refresh file metadata after an online delta merge."""
    global _metadata
    _metadata = _compute_metadata()
    return _metadata


def get_data_dir() -> Path:
    """Return the configured data directory."""
    return _data_dir or Path(os.environ.get("DATA_DIR", "/data"))
