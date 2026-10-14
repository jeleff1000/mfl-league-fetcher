"""Fly.io implementation of DatabaseReader — queries via POST /query."""

from __future__ import annotations

import math
import os
import time
from io import BytesIO
from typing import TYPE_CHECKING

import requests

from multi_league.core.fly_errors import is_permanent_storage_error

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
    # Refresh workers need time after reads to publish and finalize.
    REFRESH_TIMEOUT_SECONDS = 10
    REFRESH_MAX_ATTEMPTS = 2
    REFRESH_RETRY_MAX_DELAY = 1
    REFRESH_FINALIZE_SECONDS = 30
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

    def _read_budget(
        self, timeout_seconds: float | None, max_retries: int | None
    ) -> tuple[float, int, float | None]:
        timeout = self.TIMEOUT_SECONDS if timeout_seconds is None else timeout_seconds
        attempts = self.MAX_RETRIES if max_retries is None else max_retries
        try:
            deadline = float(os.environ.get("LEAGUE_UPDATE_DEADLINE_EPOCH", ""))
        except ValueError:
            return timeout, attempts, None
        if not math.isfinite(deadline) or deadline <= 0:
            return timeout, attempts, None
        return (
            min(timeout, self.REFRESH_TIMEOUT_SECONDS),
            min(attempts, self.REFRESH_MAX_ATTEMPTS),
            deadline - self.REFRESH_FINALIZE_SECONDS,
        )

    def _budget_timeout(
        self, timeout: float, read_deadline: float | None, sql: str, database: str
    ) -> float:
        if read_deadline is None:
            return timeout
        remaining = read_deadline - time.time()
        if remaining <= 0:
            raise FlyReaderNetworkError(
                f"Fly refresh read budget exhausted "
                f"({self.REFRESH_FINALIZE_SECONDS}s reserved for publication/finalization) "
                f"[database={database}; sql={self._sql_preview(sql)}]"
            )
        return min(timeout, remaining)

    def _wait_for_retry(
        self,
        attempt: int,
        resp: requests.Response | None,
        read_deadline: float | None,
        sql: str,
        database: str,
    ) -> None:
        delay = self._retry_delay(attempt, resp)
        if read_deadline is not None:
            if not math.isfinite(delay):
                delay = self.REFRESH_RETRY_MAX_DELAY
            delay = max(0, min(delay, self.REFRESH_RETRY_MAX_DELAY))
        time.sleep(self._budget_timeout(delay, read_deadline, sql, database))

    def _post(
        self,
        sql: str,
        database: str,
        *,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> list[dict]:
        timeout, attempts, read_deadline = self._read_budget(timeout_seconds, max_retries)
        last_error: str | None = None
        for attempt in range(attempts):
            request_timeout = self._budget_timeout(timeout, read_deadline, sql, database)
            try:
                resp = requests.post(
                    f"{self.url}/query",
                    json={"sql": sql, "database": database},
                    headers={"Authorization": f"Bearer {self.token}"},
                    timeout=request_timeout,
                )
            except self.RETRY_EXCEPTIONS as e:
                last_error = f"Network error: {e} (timeout={request_timeout:g}s)"
                if attempt < attempts - 1:
                    self._wait_for_retry(attempt, None, read_deadline, sql, database)
                    continue
                raise FlyReaderNetworkError(
                    f"{last_error} after {attempt + 1}/{attempts} attempts "
                    f"[database={database}; sql={self._sql_preview(sql)}]"
                ) from e

            # Fly's query endpoint currently wraps DuckDB's deterministic
            # missing-table Catalog Error in HTTP 500. Retrying that binder
            # result six times adds ~30 seconds without any chance of success.
            text_lower = (resp.text or "").lower()
            transient_database_lifecycle = (
                resp.status_code == 500
                and "unique file handle conflict" in text_lower
                and "in the process of being detached" in text_lower
            )
            missing_catalog_table = (
                resp.status_code == 500
                and "catalog error" in text_lower
                and "does not exist" in text_lower
            )
            deterministic_query_error = (
                not transient_database_lifecycle
                and resp.status_code == 500
                and any(
                    marker in text_lower
                    for marker in (
                        "parserexception",
                        "parser error:",
                        "binderexception",
                        "binder error:",
                    )
                )
            )
            if (
                resp.status_code in self.RETRY_STATUS
                and attempt < attempts - 1
                and not missing_catalog_table
                and not deterministic_query_error
                and (
                    transient_database_lifecycle
                    or not is_permanent_storage_error(resp.text)
                )
            ):
                last_error = f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'}"
                self._wait_for_retry(attempt, resp, read_deadline, sql, database)
                continue

            if resp.status_code != 200:
                msg = (
                    f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'} "
                    f"[database={database}; sql={self._sql_preview(sql)}]"
                )
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
            f"Query exhausted retries after {attempts} attempts: {last_error or 'unknown error'} "
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

    def query(
        self,
        sql: str,
        database: str,
        *,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> list[dict]:
        return self._post(
            sql,
            self._require_db(database),
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )

    def query_df(
        self,
        sql: str,
        database: str,
        *,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> pd.DataFrame:
        import pandas as pd

        rows = self._post(
            sql,
            self._require_db(database),
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def query_df_parquet(
        self,
        sql: str,
        database: str,
        *,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> pd.DataFrame:
        """Read a wide result without materializing every value through JSON."""
        import pandas as pd

        database = self._require_db(database)
        timeout, attempts, read_deadline = self._read_budget(timeout_seconds, max_retries)
        last_error: str | None = None
        for attempt in range(attempts):
            request_timeout = self._budget_timeout(timeout, read_deadline, sql, database)
            try:
                resp = requests.post(
                    f"{self.url}/query-parquet",
                    json={"sql": sql, "database": database},
                    headers={"Authorization": f"Bearer {self.token}"},
                    timeout=request_timeout,
                )
            except self.RETRY_EXCEPTIONS as exc:
                last_error = f"Network error: {exc} (timeout={request_timeout:g}s)"
                if attempt < attempts - 1:
                    self._wait_for_retry(attempt, None, read_deadline, sql, database)
                    continue
                raise FlyReaderNetworkError(
                    f"{last_error} after {attempt + 1}/{attempts} attempts "
                    f"[database={database}; sql={self._sql_preview(sql)}]"
                ) from exc
            if (
                resp.status_code in self.RETRY_STATUS
                and attempt < attempts - 1
                and not is_permanent_storage_error(resp.text)
            ):
                last_error = f"Parquet query failed ({resp.status_code}): {resp.text}"
                self._wait_for_retry(attempt, resp, read_deadline, sql, database)
                continue
            if resp.status_code != 200:
                raise FlyReaderError(
                    f"Parquet query failed ({resp.status_code}): {resp.text} "
                    f"[database={database}; sql={self._sql_preview(sql)}]"
                )
            return pd.read_parquet(BytesIO(resp.content))
        raise FlyReaderError(f"Parquet query exhausted retries: {last_error or 'unknown error'}")

    def query_scalar(
        self,
        sql: str,
        database: str,
        *,
        timeout_seconds: float | None = None,
        max_retries: int | None = None,
    ) -> object:
        rows = self._post(
            sql,
            self._require_db(database),
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
        )
        if not rows:
            return None
        first_row = rows[0]
        return next(iter(first_row.values()))
