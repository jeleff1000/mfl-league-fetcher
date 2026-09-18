"""Fly.io implementation of a DatabaseWriter — posts DDL/DML via /query-rw."""

from __future__ import annotations

import os
import time

import requests

from multi_league.core.fly_errors import is_permanent_storage_error


class FlyWriter:
    MAX_RETRIES = 6
    RETRY_BASE_DELAY = 2
    RETRY_MAX_DELAY = 30
    TIMEOUT_SECONDS = 150
    # Match FlyReader: 500 covers transient DuckDB query-engine hiccups
    # (e.g. concurrent ATTACH races); 502/503/504 are standard gateway errors.
    RETRY_STATUS = {429, 500, 502, 503, 504}
    RETRY_EXCEPTIONS = (
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ConnectionError,
        requests.exceptions.SSLError,
        requests.exceptions.Timeout,
    )

    def __init__(self, machine_id: str | None = None) -> None:
        from multi_league.core.runtime_mode import assert_fly_access_allowed

        assert_fly_access_allowed("FlyWriter")
        self.url = os.environ.get("DATABASE_SERVER_URL", "")
        self.token = os.environ.get("DATABASE_ADMIN_TOKEN", "")
        self.machine_id = (machine_id or os.environ.get("FLY_PRIMARY_MACHINE_ID", "")).strip()
        self.max_retries = self._env_int(
            "FLY_RW_MAX_RETRIES", self._env_int("FLY_UPLOAD_MAX_RETRIES", self.MAX_RETRIES)
        )
        self.retry_base_delay = self._env_int(
            "FLY_RW_RETRY_BASE_DELAY",
            self._env_int("FLY_UPLOAD_RETRY_BASE_DELAY", self.RETRY_BASE_DELAY),
        )
        self.retry_max_delay = self._env_int(
            "FLY_RW_RETRY_MAX_DELAY",
            self._env_int("FLY_UPLOAD_RETRY_MAX_DELAY", self.RETRY_MAX_DELAY),
        )
        if not self.url:
            raise RuntimeError("DATABASE_SERVER_URL not set")
        if not self.token:
            raise RuntimeError("DATABASE_ADMIN_TOKEN not set")

    def execute(self, sql: str, database: str = "___leagues") -> list[dict]:
        last_error: str | None = None
        for attempt in range(self.max_retries):
            try:
                resp = requests.post(
                    f"{self.url}/query-rw",
                    json={"sql": sql, "database": database},
                    headers=self._headers(),
                    timeout=self.TIMEOUT_SECONDS,
                )
            except self.RETRY_EXCEPTIONS as exc:
                last_error = f"Network error: {exc}"
                if attempt < self.max_retries - 1:
                    time.sleep(self._retry_delay(attempt))
                    continue
                raise RuntimeError(
                    f"Query failed after {attempt + 1}/{self.max_retries} attempts: {last_error}"
                ) from exc

            if (
                resp.status_code in self.RETRY_STATUS
                and attempt < self.max_retries - 1
                and not is_permanent_storage_error(resp.text)
            ):
                last_error = f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'}"
                time.sleep(self._retry_delay(attempt, resp))
                continue

            if resp.status_code != 200:
                raise RuntimeError(f"Query failed ({resp.status_code}): {resp.text or '<empty response body>'}")

            return resp.json()

        raise RuntimeError(
            f"Query exhausted retries after {self.max_retries} attempts: {last_error or 'unknown error'}"
        )

    @staticmethod
    def _env_int(name: str, default: int) -> int:
        raw = os.environ.get(name)
        if raw is None:
            return int(default)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            return int(default)
        return value if value > 0 else int(default)

    def _retry_delay(self, attempt: int, resp: requests.Response | None = None) -> float:
        retry_after = resp.headers.get("Retry-After") if resp is not None else None
        if retry_after:
            try:
                return min(float(retry_after), self.retry_max_delay)
            except (TypeError, ValueError):
                pass
        return min(self.retry_base_delay * (2**attempt), self.retry_max_delay)

    def _headers(self) -> dict[str, str]:
        headers = {"Authorization": f"Bearer {self.token}"}
        if self.machine_id:
            headers["fly-force-instance-id"] = self.machine_id
        return headers
