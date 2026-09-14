"""Fly.io implementation of DatabaseReader — queries via POST /query."""

from __future__ import annotations

import os
import time
from typing import TYPE_CHECKING

import requests

if TYPE_CHECKING:
    import pandas as pd


class FlyReaderError(RuntimeError):
    """Base error for FlyReader query failures."""


class FlyReaderTableNotFound(FlyReaderError):
    """Server response indicates the table does not exist."""


class FlyReaderNetworkError(FlyReaderError):
    """Network-level failure (timeout, DNS, connection refused)."""


class FlyReader:
    MAX_RETRIES = 6
    RETRY_BASE_DELAY = 1
    RETRY_MAX_DELAY = 15
    TIMEOUT_SECONDS = 75
    # 500 included: Fly's DuckDB occasionally returns 500 on transient
    # query-engine hiccups (e.g. concurrent ATTACH races). 502/503/504 are
    # the standard transient gateway errors. All retry on exponential backoff.
    RETRY_STATUS = {429, 500, 502, 503, 504}
    RETRY_EXCEPTIONS = (
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ConnectionError,
        requests.exceptions.SSLError,
        requests.exceptions.Timeout,
    )

    def __init__(self):
        from multi_league.core.runtime_mode import assert_fly_access_allowed

        assert_fly_access_allowed("FlyReader")
        self.url = os.environ.get("DATABASE_SERVER_URL", "")
        self.token = os.environ.get("DATABASE_READ_TOKEN", "")
        if not self.url:
            raise RuntimeError("DATABASE_SERVER_URL not set")
        if not self.token:
            raise RuntimeError("DATABASE_READ_TOKEN not set")

    @classmethod
    def _retry_delay(cls, attempt: int, resp: requests.Response | None = None) -> float:
        retry_after = resp.headers.get("Retry-After") if resp is not None else None
        if retry_after:
            try:
                return min(float(retry_after), cls.RETRY_MAX_DELAY)
            except (TypeError, ValueError):
                pass
        return min(cls.RETRY_BASE_DELAY * (2**attempt), cls.RETRY_MAX_DELAY)

    @staticmethod
    def _sql_preview(sql: str) -> str:
        preview = " ".join(str(sql).split())
        return preview if len(preview) <= 240 else preview[:237] + "..."

    def _post(self, sql: str, database: str) -> list[dict]:
        last_error: str | None = None
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.post(
                    f"{self.url}/query",
                    json={"sql": sql, "database": database},
                    headers={"Authorization": f"Bearer {self.token}"},
                    timeout=self.TIMEOUT_SECONDS,
                )
            except self.RETRY_EXCEPTIONS as e:
                last_error = f"Network error: {e}"
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self._retry_delay(attempt))
                    continue
                raise FlyReaderNetworkError(
                    f"{last_error} after {attempt + 1}/{self.MAX_RETRIES} attempts "
                    f"[database={database}; sql={self._sql_preview(sql)}]"
                ) from e

            if resp.status_code in self.RETRY_STATUS and attempt < self.MAX_RETRIES - 1:
                last_error = f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'}"
                time.sleep(self._retry_delay(attempt, resp))
                continue

            if resp.status_code != 200:
                msg = (
                    f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'} "
                    f"[database={database}; sql={self._sql_preview(sql)}]"
                )
                text_lower = resp.text.lower()
                if "does not exist" in text_lower or "table not found" in text_lower:
                    raise FlyReaderTableNotFound(msg)
                raise FlyReaderError(msg)

            try:
                return resp.json()
            except ValueError as e:
                raise FlyReaderError(
                    f"Query returned invalid JSON [database={database}; sql={self._sql_preview(sql)}]"
                ) from e

        raise FlyReaderError(
            f"Query exhausted retries after {self.MAX_RETRIES} attempts: {last_error or 'unknown error'} "
            f"[database={database}; sql={self._sql_preview(sql)}]"
        )

    @staticmethod
    def _require_db(database: str) -> str:
        # Default-less: callers must explicitly pick the database to avoid silent
        # cross-tenant routing (e.g., a query meant for ___leagues running against
        # ___ops just because the kwarg was forgotten). Empty string is allowed
        # for cross-database statements like SHOW DATABASES.
        if not isinstance(database, str):
            raise ValueError(
                "FlyReader.query: database is required (str). "
                "Pass '___ops', '___leagues', a specific db name, or '' for cross-db."
            )
        return database

    def query(self, sql: str, database: str) -> list[dict]:
        return self._post(sql, self._require_db(database))

    def query_df(self, sql: str, database: str) -> pd.DataFrame:
        import pandas as pd

        rows = self._post(sql, self._require_db(database))
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def query_scalar(self, sql: str, database: str) -> object:
        rows = self._post(sql, self._require_db(database))
        if not rows:
            return None
        first_row = rows[0]
        return next(iter(first_row.values()))
