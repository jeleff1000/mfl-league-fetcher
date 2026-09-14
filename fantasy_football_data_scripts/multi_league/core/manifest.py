"""Import manifest — tracks pipeline stage completion in the database.

Advisory, not gatekeeping. Each stage writes what it did.
Frontend reads it to know what features are available.
reimport: true or --force ignores it entirely.
"""

from __future__ import annotations

import json
import os
from datetime import datetime

from multi_league.core.db_utils import get_db_name
from multi_league.core.script_runner import log

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS public.import_manifest (
    stage           VARCHAR NOT NULL,
    status          VARCHAR NOT NULL,
    import_mode     VARCHAR NOT NULL,
    platform        VARCHAR NOT NULL,
    years_fetched   VARCHAR,
    years_transformed VARCHAR,
    has_nfl_expansion BOOLEAN DEFAULT FALSE,
    has_lamar       BOOLEAN DEFAULT FALSE,
    has_sims        BOOLEAN DEFAULT FALSE,
    has_aggregations BOOLEAN DEFAULT FALSE,
    has_homepage    BOOLEAN DEFAULT FALSE,
    error_message   VARCHAR,
    started_at      TIMESTAMP NOT NULL,
    completed_at    TIMESTAMP,
    duration_seconds INTEGER,
    workflow_run_id VARCHAR,
    PRIMARY KEY (stage, workflow_run_id)
)
"""


def _connect(db_name: str):
    """Connect to the database and ensure manifest table exists."""
    from multi_league.core.db_reader import get_reader

    return _FlyManifestConn(get_reader(), db_name)


class _FlyManifestConn:
    """Thin wrapper around FlyReader that mimics a DuckDB connection for manifest ops."""

    def __init__(self, reader, db_name: str):
        self._reader = reader
        self._db_name = db_name
        # Ensure table exists
        self._reader.query(CREATE_TABLE_SQL, database=db_name)

    def execute(self, sql: str, params: list | None = None):
        # FlyReader.query doesn't support parameterized queries directly,
        # so we do simple substitution for the manifest use case
        if params:
            # Replace ? placeholders with values
            for p in params:
                if p is None:
                    sql = sql.replace("?", "NULL", 1)
                elif isinstance(p, (int, float, bool)):
                    sql = sql.replace("?", str(p), 1)
                else:
                    escaped = str(p).replace("'", "''")
                    sql = sql.replace("?", f"'{escaped}'", 1)
        self._reader.query(sql, database=self._db_name)
        return self

    def fetchdf(self):
        # This is called after execute for SELECT queries — not directly applicable
        # Use query_df for reads instead
        return None

    def close(self):
        pass  # FlyReader connections are managed by the reader


def write_manifest(
    ctx,
    stage: str,
    status: str,
    platform: str,
    import_mode: str = "all_years",
    years_fetched: list[int] | None = None,
    years_transformed: list[int] | None = None,
    has_nfl_expansion: bool = False,
    has_lamar: bool = False,
    has_sims: bool = False,
    has_aggregations: bool = False,
    has_homepage: bool = False,
    error_message: str | None = None,
    workflow_run_id: str | None = None,
    started_at: datetime | None = None,
) -> bool:
    """Write or update a manifest row for the given stage.

    Uses INSERT OR REPLACE on (stage, workflow_run_id).
    """
    db_name = get_db_name(ctx)
    conn = _connect(db_name)
    if not conn:
        log("[MANIFEST] No database token, skipping manifest write")
        return False

    now = datetime.utcnow()
    run_id = workflow_run_id or os.environ.get("GITHUB_RUN_ID", f"local_{now.strftime('%Y%m%d_%H%M%S')}")
    start = started_at or now
    completed = now if status in ("completed", "failed") else None
    duration = int((completed - start).total_seconds()) if completed and started_at else None

    try:
        conn.execute(
            """
            INSERT OR REPLACE INTO public.import_manifest
            (stage, status, import_mode, platform, years_fetched, years_transformed,
             has_nfl_expansion, has_lamar, has_sims, has_aggregations, has_homepage,
             error_message, started_at, completed_at, duration_seconds, workflow_run_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                stage,
                status,
                import_mode,
                platform,
                json.dumps(years_fetched) if years_fetched else None,
                json.dumps(years_transformed) if years_transformed else None,
                has_nfl_expansion,
                has_lamar,
                has_sims,
                has_aggregations,
                has_homepage,
                error_message,
                start,
                completed,
                duration,
                run_id,
            ],
        )
        conn.close()
        log(f"[MANIFEST] Wrote {stage}={status} to {db_name}")
        return True
    except Exception as e:
        log(f"[MANIFEST] Failed to write: {e}")
        return False


def read_manifest(ctx) -> list[dict]:
    """Read latest completed manifest row per stage.

    Returns list of dicts, one per stage that has completed.
    Empty list if manifest doesn't exist or has no completed rows.
    """
    db_name = get_db_name(ctx)

    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        df = reader.query_df(
            """
            SELECT * FROM public.import_manifest
            WHERE (stage, completed_at) IN (
                SELECT stage, MAX(completed_at)
                FROM public.import_manifest
                WHERE status = 'completed'
                GROUP BY stage
            )
            ORDER BY completed_at
        """,
            database=db_name,
        )
        return df.to_dict("records") if not df.empty else []
    except Exception:
        return []


def clear_manifest(ctx) -> bool:
    """Clear all manifest rows (for reimport)."""
    db_name = get_db_name(ctx)
    conn = _connect(db_name)
    if not conn:
        return False
    try:
        conn.execute("DELETE FROM public.import_manifest")
        conn.close()
        log(f"[MANIFEST] Cleared all rows from {db_name}")
        return True
    except Exception as e:
        log(f"[MANIFEST] Failed to clear: {e}")
        return False
