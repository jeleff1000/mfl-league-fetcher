"""Atomic file swap with drain, rollback, and recovery."""

import hashlib
import logging
from pathlib import Path

import duckdb

logger = logging.getLogger(__name__)


class SwapError(Exception):
    pass


def validate_db_file(path: Path) -> bool:
    """Open and validate a DuckDB file by querying its catalog."""
    try:
        conn = duckdb.connect(str(path), read_only=True)
        # DuckDB doesn't have PRAGMA integrity_check — validate by
        # successfully opening and querying the catalog
        conn.execute("SELECT COUNT(*) FROM information_schema.tables")
        conn.close()
        return True
    except Exception as e:
        raise SwapError(f"File integrity check failed: {e}") from e


def verify_checksum(path: Path, expected_hash: str) -> bool:
    """Verify SHA-256 of file matches expected."""
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            sha.update(chunk)
    actual = sha.hexdigest()
    if actual != expected_hash:
        raise SwapError(f"Checksum mismatch: expected {expected_hash}, got {actual}")
    return True


def atomic_swap(data_dir: Path, db_name: str) -> dict:
    """Perform the rename swap: .incoming -> live, live -> .prev.

    Caller is responsible for draining connections BEFORE calling this.
    Caller is responsible for reopening connections AFTER this returns.
    """
    live = data_dir / f"{db_name}.duckdb"
    incoming = data_dir / f"{db_name}.duckdb.incoming"
    prev = data_dir / f"{db_name}.duckdb.prev"
    bad = data_dir / f"{db_name}.duckdb.bad"

    if not incoming.exists():
        raise SwapError(f"No incoming file at {incoming}")

    # Validate incoming
    validate_db_file(incoming)

    # Swap
    if prev.exists():
        prev.unlink()
    if live.exists():
        live.rename(prev)
    incoming.rename(live)

    # Health check
    try:
        conn = duckdb.connect(str(live), read_only=True)
        conn.execute("SELECT 1")
        conn.close()
    except Exception as e:
        # ROLLBACK
        logger.error("Post-swap health check failed, rolling back: %s", e)
        if live.exists():
            live.rename(bad)
        if prev.exists():
            prev.rename(live)
        raise SwapError(f"Post-swap health check failed, rolled back: {e}") from e

    return {
        "status": "swapped",
        "db_name": db_name,
        "prev_retained": prev.exists(),
    }


def startup_recovery(data_dir: Path):
    """Handle crash recovery on server start.

    Safety rule: never delete .prev until live file is verified healthy.
    """
    for name in ("___leagues", "___ops", "___ops_nfl"):
        live = data_dir / f"{name}.duckdb"
        prev = data_dir / f"{name}.duckdb.prev"
        incoming = data_dir / f"{name}.duckdb.incoming"

        # Clean stale incoming files (interrupted uploads)
        if incoming.exists():
            logger.info("Cleaning stale incoming: %s", incoming)
            incoming.unlink()

        # If only .prev exists (main was moved but rename failed), restore it
        if not live.exists() and prev.exists():
            logger.warning("Live missing, restoring from .prev: %s", name)
            prev.rename(live)

        # If both exist, verify live is healthy before removing prev
        if live.exists() and prev.exists():
            try:
                conn = duckdb.connect(str(live), read_only=True)
                conn.execute("SELECT COUNT(*) FROM information_schema.tables")
                conn.close()
                # Live is healthy — safe to remove prev
                prev.unlink()
                logger.info("Live healthy, removed .prev: %s", name)
            except Exception:
                # Live is corrupt — restore prev
                logger.error("Live corrupt, restoring .prev: %s", name)
                live.rename(data_dir / f"{name}.duckdb.bad")
                prev.rename(live)
