"""Fly.io implementation of DatabaseTarget — uploads .duckdb files."""

from __future__ import annotations

import hashlib
import logging
import os
import random
import time
from pathlib import Path

import requests

from multi_league.core.fly_errors import is_permanent_storage_error

logger = logging.getLogger(__name__)


def _sql_literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _response_payload(resp: requests.Response) -> dict:
    try:
        payload = resp.json()
    except Exception:
        return {"detail": resp.text[:500]}
    return payload if isinstance(payload, dict) else {"detail": payload}


def _is_stale_delta_conflict(payload: dict, text: str) -> bool:
    detail = str(payload.get("detail") or payload.get("message") or text or "")
    return "older bundle cannot commit over newer committed state" in detail.lower()


class FlyTarget:
    MAX_UPLOAD_RETRIES = 6
    RETRY_BASE_DELAY = 10
    RETRY_MAX_DELAY = 120
    RETRY_STATUS = {429, 500, 502, 503, 504}
    RETRY_EXCEPTIONS = (
        requests.exceptions.ChunkedEncodingError,
        requests.exceptions.ConnectionError,
        requests.exceptions.SSLError,
        requests.exceptions.Timeout,
    )
    UPLOAD_TIMEOUT = 600

    def __init__(self):
        from multi_league.core.runtime_mode import assert_fly_access_allowed

        assert_fly_access_allowed("FlyTarget")
        self.url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
        self.token = os.environ.get("DATABASE_ADMIN_TOKEN", "")
        self.read_token = os.environ.get("DATABASE_READ_TOKEN", "")
        self.primary_machine_id = os.environ.get("FLY_PRIMARY_MACHINE_ID", "").strip()
        self.max_upload_retries = _env_int("FLY_UPLOAD_MAX_RETRIES", self.MAX_UPLOAD_RETRIES)
        self.retry_base_delay = _env_float("FLY_UPLOAD_RETRY_BASE_DELAY", self.RETRY_BASE_DELAY)
        self.retry_max_delay = _env_float("FLY_UPLOAD_RETRY_MAX_DELAY", self.RETRY_MAX_DELAY)
        self.retry_jitter_seconds = _env_float("FLY_UPLOAD_RETRY_JITTER_SECONDS", 0.0)
        if not self.url:
            raise RuntimeError("DATABASE_SERVER_URL not set")
        if not self.token:
            raise RuntimeError("DATABASE_ADMIN_TOKEN not set")

    def replace_database(self, db_name: str, local_path: Path) -> None:
        """Replace an entire database file (___leagues, ___ops, or ___ops_nfl)."""
        if db_name not in ("___leagues", "___ops", "___ops_nfl"):
            raise ValueError(f"Invalid db_name: {db_name}")
        if not local_path.exists():
            raise FileNotFoundError(f"DB file not found: {local_path}")

        checksum = self._sha256(local_path)

        resp = self._post_file(
            "replace-db",
            db_name,
            local_path,
            machine_id=self.primary_machine_id or None,
            extra_headers={"X-Content-SHA256": checksum},
        )

        if resp.status_code != 200:
            raise RuntimeError(f"Upload failed ({resp.status_code}): {resp.text}")

    def merge_league(self, db_name: str, local_path: Path) -> dict:
        """Merge a per-league .duckdb file into ___leagues on the server.

        The server ATTACHes the uploaded file and does DELETE+INSERT per table,
        mirroring the local upload merge pattern.
        """
        if not local_path.exists():
            raise FileNotFoundError(f"DB file not found: {local_path}")

        skip_delete = not self._league_in_centralized(db_name)
        extra_headers = {"X-Skip-Delete": "1"} if skip_delete else None
        resp = self._post_file(
            "merge-league",
            db_name,
            local_path,
            machine_id=self.primary_machine_id or None,
            extra_headers=extra_headers,
        )

        if resp.status_code != 200:
            raise RuntimeError(f"Merge failed ({resp.status_code}): {resp.text}")

        return resp.json()

    def rename_league(
        self,
        *,
        source_db: str,
        target_db: str,
        display_name: str,
        operation_id: str,
    ) -> dict:
        """Run the server-local, idempotent league identity rename."""
        payload = {
            "source_db": source_db,
            "target_db": target_db,
            "display_name": display_name,
            "operation_id": operation_id,
        }
        try:
            response = requests.post(
                f"{self.url}/rename-league",
                json=payload,
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=180,
            )
        except self.RETRY_EXCEPTIONS as exc:
            raise RuntimeError(
                "League rename response was ambiguous; rerun the same operation_id to reconcile"
            ) from exc
        if response.status_code != 200:
            raise RuntimeError(
                f"League rename failed ({response.status_code}): {response.text[:500]}"
            )
        return _response_payload(response)

    def merge_ops(self, local_path: Path) -> dict:
        """Replace nfl_historical reference tables in ___ops from a local .duckdb bundle.

        The bundle holds the new table(s) as plain tables (no db_name tag). The server swaps each
        into ___ops.nfl_historical via atomic CREATE OR REPLACE -- per-table, so other ___ops tables
        (credentials, franchises) are preserved, and ___leagues is never touched. This is the
        supported transport for super-table / season-career / player_bio updates.
        """
        if not local_path.exists():
            raise FileNotFoundError(f"DB file not found: {local_path}")
        resp = self._post_file(
            "merge-ops",
            "___ops",
            local_path,
            machine_id=self.primary_machine_id or None,
        )
        if resp.status_code != 200:
            raise RuntimeError(f"merge-ops failed ({resp.status_code}): {resp.text}")
        return resp.json()

    def merge_league_delta(
        self,
        db_name: str,
        bundle_path: Path,
        *,
        bundle_id: str,
        bundle_hash: str,
    ) -> dict:
        """Merge a manifested delta bundle into ___leagues on the server.

        Ambiguous transport failures are resolved by querying the server-side
        status endpoint before surfacing an error. This prevents a retry from
        blindly falling back after the server may already have committed.
        """
        if not bundle_path.exists():
            raise FileNotFoundError(f"Delta bundle not found: {bundle_path}")

        extra_headers = {
            "X-Bundle-Id": bundle_id,
            "X-Bundle-Hash": bundle_hash,
        }
        try:
            resp = self._post_file(
                "merge-league-delta",
                db_name,
                bundle_path,
                machine_id=self.primary_machine_id or None,
                extra_headers=extra_headers,
            )
        except Exception:
            recovered = self._reconcile_committed_merge(db_name, bundle_id, bundle_hash)
            if recovered is not None:
                return recovered
            raise

        if resp.status_code == 200:
            return resp.json()

        if resp.status_code == 202:
            return resp.json()

        if resp.status_code == 409 or resp.status_code >= 500:
            recovered = self._reconcile_committed_merge(db_name, bundle_id, bundle_hash)
            if recovered is not None:
                return recovered

        if resp.status_code == 409:
            payload = _response_payload(resp)
            if _is_stale_delta_conflict(payload, resp.text):
                return {
                    "status": "STALE_SKIPPED",
                    "db_name": db_name,
                    "bundle_id": bundle_id,
                    "bundle_hash": bundle_hash,
                    "http_status": resp.status_code,
                    "detail": payload.get("detail") or resp.text,
                }
            raise RuntimeError(f"Delta merge conflict ({resp.status_code}): {resp.text}")

        raise RuntimeError(f"Delta merge failed ({resp.status_code}): {resp.text}")

    def merge_fleet_partition(
        self,
        bundle_path: Path,
        *,
        bundle_id: str,
        bundle_hash: str,
        merge_timeout_seconds: int | None = None,
    ) -> dict:
        """Publish an active-season scoped bundle through Fly's fast fleet lane.

        The server derives the affected league set from the Parquet payload and
        replaces only the declared active-season scope. This is appropriate for
        a one-league offseason draft update as well as regular fleet batches.
        """
        if not bundle_path.exists():
            raise FileNotFoundError(f"Fleet partition bundle not found: {bundle_path}")

        extra_headers = {
            "X-Bundle-Id": bundle_id,
            "X-Bundle-Hash": bundle_hash,
        }
        if merge_timeout_seconds is not None:
            if not 10 <= int(merge_timeout_seconds) <= 300:
                raise ValueError("merge_timeout_seconds must be between 10 and 300")
            extra_headers["X-Merge-Step-Timeout-Seconds"] = str(int(merge_timeout_seconds))
        try:
            resp = self._post_file(
                "merge-fleet-partition",
                "___fleet",
                bundle_path,
                machine_id=self.primary_machine_id or None,
                extra_headers=extra_headers,
            )
        except Exception:
            recovered = self._reconcile_committed_merge("___fleet", bundle_id, bundle_hash)
            if recovered is not None:
                return recovered
            raise

        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 202:
            return resp.json()
        if resp.status_code == 409 or resp.status_code >= 500:
            recovered = self._reconcile_committed_merge("___fleet", bundle_id, bundle_hash)
            if recovered is not None:
                return recovered
        raise RuntimeError(f"Fleet partition merge failed ({resp.status_code}): {resp.text}")

    def _reconcile_committed_merge(self, db_name: str, bundle_id: str, bundle_hash: str) -> dict | None:
        """Trust an ambiguous response only when the durable receipt matches content."""
        status = self.get_delta_merge_status(db_name, bundle_id)
        if str(status.get("status") or "").upper() != "COMMITTED":
            return None
        if status.get("bundle_id") != bundle_id:
            raise RuntimeError("Committed merge receipt bundle ID mismatch")
        if status.get("bundle_hash") != bundle_hash:
            raise RuntimeError("Committed merge receipt bundle hash mismatch")
        return {**status, "recovered_after_ambiguous_failure": True}

    def get_delta_merge_status(self, db_name: str, bundle_id: str) -> dict:
        headers = {"Authorization": f"Bearer {self.token}"}
        if self.primary_machine_id:
            headers["fly-force-instance-id"] = self.primary_machine_id
        try:
            resp = requests.get(
                f"{self.url}/merge-league-delta/status",
                headers=headers,
                params={"db_name": db_name, "bundle_id": bundle_id},
                timeout=20,
            )
        except Exception as exc:
            return {"status": "UNKNOWN", "error": str(exc)}
        try:
            payload = resp.json()
        except Exception:
            payload = {"detail": resp.text[:500]}
        payload["http_status"] = resp.status_code
        return payload

    def _league_in_centralized(self, db_name: str) -> bool:
        """Return whether this league has already completed a centralized upload.

        First imports can skip DELETE scans against the large shared tables.
        If inventory cannot be read, stay conservative and do the delete.
        """
        if not self.read_token:
            return True
        safe_db = _sql_literal(db_name)
        sql = f"""
        SELECT COALESCE(bool_or(in_centralized), FALSE) AS in_centralized
        FROM accounts.league_inventory
        WHERE database_name = {safe_db}
        """
        try:
            resp = requests.post(
                f"{self.url}/query",
                headers={"Authorization": f"Bearer {self.read_token}"},
                json={"database": "___ops", "sql": sql},
                timeout=20,
            )
            if resp.status_code != 200:
                logger.warning(
                    "Could not read league inventory for %s (%s): %s", db_name, resp.status_code, resp.text[:300]
                )
                return True
            rows = resp.json()
            if not rows:
                return False
            return bool(rows[0].get("in_centralized"))
        except Exception as exc:
            logger.warning("Could not read league inventory for %s: %s", db_name, exc)
            return True

    def mark_league_imported(
        self,
        db_name: str,
        *,
        import_mode: str | None = None,
        platform: str | None = None,
        import_status: str = "complete",
    ) -> list[dict]:
        """Finalize the Fly inventory row after a successful league publish."""
        from multi_league.core.fly_writer import FlyWriter

        safe_db = _sql_literal(db_name)
        safe_mode = _sql_literal(import_mode)
        safe_platform = _sql_literal(platform or "unknown")
        safe_status = _sql_literal(import_status)

        sql = f"""
        UPDATE accounts.league_inventory
        SET in_centralized = TRUE,
            last_import_at = current_timestamp,
            updated_at = current_timestamp,
            import_status = {safe_status},
            last_import_mode = COALESCE({safe_mode}, last_import_mode),
            platform = COALESCE(NULLIF(platform, ''), {safe_platform})
        WHERE database_name = {safe_db};

        INSERT INTO accounts.league_inventory
            (database_name, platform, last_import_mode, last_import_at,
             in_centralized, created_at, updated_at, import_status)
        SELECT {safe_db}, {safe_platform}, {safe_mode}, current_timestamp,
               TRUE, current_timestamp, current_timestamp, {safe_status}
        WHERE NOT EXISTS (
            SELECT 1 FROM accounts.league_inventory WHERE database_name = {safe_db}
        );
        """
        return FlyWriter(machine_id=self.primary_machine_id or None).execute(sql, database="___ops")

    def health(self) -> dict:
        resp = requests.get(f"{self.url}/ready", timeout=10)
        return resp.json()

    def _post_file(
        self,
        endpoint: str,
        db_name: str,
        local_path: Path,
        *,
        machine_id: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> requests.Response:
        """POST a DuckDB upload with retries for transient Fly/proxy failures.

        The file object must be reopened for each attempt because requests
        consumes the stream. This covers SSL EOFs from Fly/proxy disconnects
        and standard transient 5xx gateway responses.
        """
        headers = {
            "Authorization": f"Bearer {self.token}",
            "X-Db-Name": db_name,
        }
        if machine_id:
            headers["fly-force-instance-id"] = machine_id
        if extra_headers:
            headers.update(extra_headers)

        for attempt in range(self.max_upload_retries):
            try:
                with open(local_path, "rb") as f:
                    resp = requests.post(
                        f"{self.url}/{endpoint}",
                        headers=headers,
                        files={"file": (f"{db_name}.duckdb", f, "application/octet-stream")},
                        timeout=self.UPLOAD_TIMEOUT,
                    )
            except self.RETRY_EXCEPTIONS as exc:
                if attempt < self.max_upload_retries - 1:
                    self._sleep_before_retry(endpoint, attempt, exc)
                    continue
                raise RuntimeError(f"{endpoint} upload failed after {self.max_upload_retries} attempts: {exc}") from exc

            if self._is_retryable_upload_response(resp) and attempt < self.max_upload_retries - 1:
                self._sleep_before_retry(
                    endpoint,
                    attempt,
                    resp,
                )
                continue

            return resp

        raise RuntimeError(f"{endpoint} upload exhausted retries")

    def _is_retryable_upload_response(self, resp: requests.Response) -> bool:
        if resp.status_code not in self.RETRY_STATUS:
            return False
        # A checksum mismatch is persistent storage corruption, not a
        # transient proxy/server failure. Retrying only consumes the weekly
        # update deadline and cannot change the result.
        return not is_permanent_storage_error(resp.text)

    def _sleep_before_retry(self, endpoint: str, attempt: int, reason: object) -> None:
        delay = self._retry_delay(attempt, reason if isinstance(reason, requests.Response) else None)
        detail = f"HTTP {reason.status_code}: {reason.text[:300]}" if isinstance(reason, requests.Response) else reason
        print(
            f"[FLY-UPLOAD] {endpoint} attempt {attempt + 1}/{self.max_upload_retries} "
            f"failed transiently: {detail}. Retrying in {delay:.1f}s..."
        )
        time.sleep(delay)

    def _retry_delay(self, attempt: int, resp: requests.Response | None = None) -> float:
        retry_after = resp.headers.get("Retry-After") if resp is not None else None
        if retry_after:
            try:
                base_delay = min(float(retry_after), self.retry_max_delay)
                return min(base_delay + self._retry_jitter(), self.retry_max_delay)
            except (TypeError, ValueError):
                pass
        return min(self.retry_base_delay * (2**attempt) + self._retry_jitter(), self.retry_max_delay)

    def _retry_jitter(self) -> float:
        if self.retry_jitter_seconds <= 0:
            return 0.0
        return random.uniform(0.0, self.retry_jitter_seconds)

    @staticmethod
    def _sha256(path: Path) -> str:
        sha = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(8192):
                sha.update(chunk)
        return sha.hexdigest()

    @staticmethod
    def _parse_machine_ids(raw: str) -> list[str]:
        seen: set[str] = set()
        ids: list[str] = []
        for machine_id in raw.replace(";", ",").split(","):
            machine_id = machine_id.strip()
            if machine_id and machine_id not in seen:
                ids.append(machine_id)
                seen.add(machine_id)
        return ids
