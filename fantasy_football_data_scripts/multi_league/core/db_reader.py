"""Database reader abstraction — host portability for pipeline reads."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    import pandas as pd


class DatabaseReader(Protocol):
    """Host abstraction for pipeline reads (super_table, validation).

    ``database`` defaults to ``"___ops"`` for the shared ops database.
    Pass ``""`` for an unscoped connection (cross-database queries,
    ``SHOW DATABASES``, etc.).
    """

    def query(self, sql: str, database: str = "___ops") -> list[dict]:
        """Execute *sql* and return rows as a list of dicts."""
        ...

    def query_df(self, sql: str, database: str = "___ops") -> pd.DataFrame:
        """Execute *sql* and return a pandas DataFrame."""
        ...

    def query_scalar(self, sql: str, database: str = "___ops") -> object:
        """Execute *sql* and return the first column of the first row.

        Returns ``None`` when the result set is empty.
        """
        ...


def get_reader() -> DatabaseReader:
    """Factory: returns the configured DatabaseReader implementation."""
    from multi_league.core.readers.fly_reader import FlyReader

    return FlyReader()
