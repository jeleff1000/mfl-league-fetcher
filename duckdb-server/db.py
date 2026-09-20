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
    """Provision credential metadata only when a schema object is missing."""
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
        ("main", "league_credentials"): {
            "league_id": "TEXT",
            "league_name": "TEXT",
            "database_name": "TEXT",
            "encrypted_refresh_token": "TEXT",
            "updated_at": "TIMESTAMP DEFAULT current_timestamp",
        },
        ("accounts", "league_inventory"): {
            "database_name": "VARCHAR",
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
            "created_at": "TIMESTAMP DEFAULT current_timestamp",
            "updated_at": "TIMESTAMP DEFAULT current_timestamp",
        },
    }
    for (schema, table), columns in table_specs.items():
        present = {
            row[0]
            for row in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            ).fetchall()
        }
        if not present:
            definitions = ", ".join(f'"{name}" {definition}' for name, definition in columns.items())
            conn.execute(f'CREATE TABLE "{schema}"."{table}" ({definitions})')
            operations.append(f"create_table:{schema}.{table}")
            continue
        for name, definition in columns.items():
            if name not in present:
                conn.execute(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN "{name}" {definition}')
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
            logger.info("Provisioned ___ops credential schema: %s", ", ".join(schema_operations))
    finally:
        ops_schema_conn.close()

    # Pool connections are normal DuckDB connections so reads can keep a
    # snapshot open while delta merges commit in another connection. ___ops is
    # attached lazily per query; keeping it attached permanently forces every
    # tiny ___ops write to rebuild the whole public pool.
    _refill_pool(leagues_path)

    # Dedicated ___ops connection — bypasses the pool entirely
    if ops_path.exists():
        _ops_conn = connect_database(ops_path, read_only=True, data_dir=_data_dir)
        # Metadata never needs the NFL catalog. NFL reads use the existing
        # league pool's permanent attachment; repeated cross-instance ATTACH
        # here can deadlock native DuckDB after a metadata write.

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
    """Close only the dedicated ___ops connection."""
    global _ops_conn
    if _ops_conn is not None:
        try:
            _ops_conn.close()
        except Exception:
            pass
        _ops_conn = None


def reopen_ops_connection():
    """Reopen only the dedicated ___ops connection."""
    global _ops_conn
    ops_path = get_data_dir() / "___ops.duckdb"
    close_ops_connection()
    if ops_path.exists():
        _ops_conn = connect_database(ops_path, read_only=True, data_dir=get_data_dir())
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
