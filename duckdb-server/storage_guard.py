"""Process ownership and effective DuckDB runtime contracts."""

from __future__ import annotations

from dataclasses import dataclass
import math
import os
from pathlib import Path
import re
from typing import BinaryIO


class StorageOwnershipError(RuntimeError):
    """Raised when another process already owns the live DuckDB files."""


class RuntimeContractError(RuntimeError):
    """Raised when effective DuckDB settings differ from the write contract."""


class StorageOwner:
    """Hold an operating-system lock for the lifetime of the DuckDB server."""

    def __init__(self, data_dir: Path | str):
        self.data_dir = Path(data_dir)
        self.lock_path = self.data_dir / ".duckdb-server.lock"
        self.pid = os.getpid()
        self._handle: BinaryIO | None = None

    def acquire(self) -> None:
        if self._handle is not None:
            return
        self.data_dir.mkdir(parents=True, exist_ok=True)
        handle = self.lock_path.open("a+b")
        try:
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            self._lock(handle)
            handle.seek(0)
            handle.truncate()
            handle.write(f"pid={self.pid}\n".encode("utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        except Exception:
            handle.close()
            raise
        self._handle = handle

    def release(self) -> None:
        handle = self._handle
        if handle is None:
            return
        try:
            self._unlock(handle)
        finally:
            handle.close()
            self._handle = None

    @staticmethod
    def _lock(handle: BinaryIO) -> None:
        try:
            if os.name == "nt":
                import msvcrt

                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl

                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            raise StorageOwnershipError(
                "DuckDB storage is already owned by another server process"
            ) from exc

    @staticmethod
    def _unlock(handle: BinaryIO) -> None:
        handle.seek(0)
        if os.name == "nt":
            import msvcrt

            msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@dataclass(frozen=True)
class RuntimeContract:
    duckdb_version: str
    checkpoint_threshold: str
    memory_limit: str
    threads: int
    max_temp_directory_size: str

    @classmethod
    def read(cls, conn) -> "RuntimeContract":
        row = conn.execute(
            "SELECT version(), current_setting('checkpoint_threshold'), "
            "current_setting('memory_limit'), current_setting('threads'), "
            "current_setting('max_temp_directory_size')"
        ).fetchone()
        return cls(
            duckdb_version=str(row[0]),
            checkpoint_threshold=str(row[1]),
            memory_limit=str(row[2]),
            threads=int(row[3]),
            max_temp_directory_size=str(row[4]),
        )

    def assert_matches(self, conn) -> None:
        actual = self.read(conn)
        size_fields = {
            "checkpoint_threshold",
            "memory_limit",
            "max_temp_directory_size",
        }
        mismatches = {}
        for field in self.__dataclass_fields__:
            expected = getattr(self, field)
            actual_value = getattr(actual, field)
            if field in size_fields:
                if expected == actual_value:
                    matches = True
                else:
                    try:
                        matches = math.isclose(
                            _size_bytes(expected),
                            _size_bytes(actual_value),
                            # DuckDB renders small limits with one decimal
                            # place (for example 1536MB as 1.4 GiB).
                            rel_tol=0.05,
                        )
                    except RuntimeContractError:
                        matches = False
            else:
                matches = expected == actual_value
            if not matches:
                mismatches[field] = (expected, actual_value)
        if mismatches:
            details = ", ".join(
                f"{field}: expected={expected!r}, actual={actual_value!r}"
                for field, (expected, actual_value) in mismatches.items()
            )
            raise RuntimeContractError(f"DuckDB runtime contract mismatch: {details}")


_SIZE_PATTERN = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?i?B)\s*$", re.IGNORECASE)
_SIZE_MULTIPLIERS = {
    "B": 1,
    "KB": 1000,
    "MB": 1000**2,
    "GB": 1000**3,
    "TB": 1000**4,
    "KIB": 1024,
    "MIB": 1024**2,
    "GIB": 1024**3,
    "TIB": 1024**4,
}


def _size_bytes(value: str) -> float:
    match = _SIZE_PATTERN.match(str(value))
    if not match:
        raise RuntimeContractError(f"Unrecognized DuckDB size setting: {value!r}")
    return float(match.group(1)) * _SIZE_MULTIPLIERS[match.group(2).upper()]
