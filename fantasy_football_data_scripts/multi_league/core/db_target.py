"""Database target abstraction — host portability for pipeline uploads."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol


class DatabaseTarget(Protocol):
    """Host abstraction for pipeline output (uploads + maintenance)."""

    def replace_database(self, db_name: str, local_path: Path) -> None: ...
    def health(self) -> dict: ...


def get_target() -> DatabaseTarget:
    """Factory: returns the configured DatabaseTarget implementation."""
    from multi_league.core.targets.fly_target import FlyTarget

    return FlyTarget()
