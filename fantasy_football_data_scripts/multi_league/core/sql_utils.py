"""
Centralized SQL safety utilities for ___leagues centralized database.

All SQL that writes to ___leagues MUST go through execute_scoped() or
validate_scoped_sql(). This prevents accidental cross-league data corruption.
"""

from __future__ import annotations

import logging
import re

logger = logging.getLogger(__name__)

DB_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_]{0,62}$")

# Patterns that indicate a scoped write operation against ___leagues
# (these require a db_name filter — single-league writes)
_WRITE_PATTERNS = [
    re.compile(r"\bUPDATE\s+___leagues\b", re.IGNORECASE),
    re.compile(r"\bDELETE\s+FROM\s+___leagues\b", re.IGNORECASE),
    re.compile(r"\bINSERT\s+INTO\s+___leagues\b", re.IGNORECASE),
]

# Patterns that indicate a full-table wipe against ___leagues — these are
# NEVER allowed in the centralized model because they would destroy data
# across every league that shares the table.
_FORBIDDEN_PATTERNS = [
    (re.compile(r"\bCREATE\s+OR\s+REPLACE\s+TABLE\s+___leagues\b", re.IGNORECASE), "CREATE OR REPLACE TABLE"),
    (re.compile(r"\bDROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?___leagues\b", re.IGNORECASE), "DROP TABLE"),
    (re.compile(r"\bTRUNCATE\s+(?:TABLE\s+)?___leagues\b", re.IGNORECASE), "TRUNCATE"),
]


def validate_db_name(db_name: str) -> str:
    """Validate and return db_name. Raises ValueError if invalid."""
    if not DB_NAME_RE.match(db_name):
        raise ValueError(f"Invalid db_name: {db_name!r} - must match {DB_NAME_RE.pattern}")
    return db_name


def validate_scoped_sql(sql: str, db_name: str) -> None:
    """
    Reject unsafe writes against ``___leagues``.

    Two classes of safety check:

    1. **Forbidden full-table operations** (CREATE OR REPLACE TABLE / DROP TABLE /
       TRUNCATE): these are *never* allowed against ``___leagues.*`` because they
       destroy data across every league. Raise immediately.

    2. **Scoped writes** (UPDATE / DELETE / INSERT): must contain the
       caller-supplied ``db_name`` value in the WHERE clause or INSERT values so
       that only a single league's rows are touched.

    Raises:
        RuntimeError: if the SQL is forbidden or if a scoped write is missing
            its db_name filter.
    """
    validate_db_name(db_name)

    for pattern, label in _FORBIDDEN_PATTERNS:
        if pattern.search(sql):
            raise RuntimeError(
                f"UNSAFE SQL: {label} against ___leagues is forbidden in the centralized model.\n"
                f"This would wipe data across ALL leagues, not just '{db_name}'.\n"
                f"Use scoped DELETE + INSERT (WHERE db_name = '{db_name}') instead.\n"
                f"SQL preview: {sql[:500]}"
            )

    escaped_db_name = re.escape(db_name)
    scoped_patterns = (
        rf"\bdb_name\s*=\s*'{escaped_db_name}'",
        rf"'{escaped_db_name}'\s+AS\s+db_name\b",
        rf"\(\s*db_name\b[^)]*\)\s*VALUES\s*\(\s*'{escaped_db_name}'",
    )

    for pattern in _WRITE_PATTERNS:
        if pattern.search(sql):
            scoped = any(
                re.search(scoped_pattern, sql, re.IGNORECASE | re.DOTALL) for scoped_pattern in scoped_patterns
            )
            if not scoped:
                raise RuntimeError(
                    f"UNSAFE SQL: write to ___leagues without db_name = '{db_name}' filter.\n"
                    f"SQL must contain: db_name = '{db_name}' in WHERE clause or INSERT values.\n"
                    f"SQL preview: {sql[:500]}"
                )
            return


def execute_scoped(conn, sql: str, db_name: str, *, label: str = "") -> int:
    """
    Validate and execute SQL against ___leagues with db_name scoping.

    Use this for ALL standalone scripts that bypass sql_base._execute().
    Returns the number of rows affected (for writes) or fetched.
    """
    validate_scoped_sql(sql, db_name)

    result = conn.execute(sql)
    try:
        rows_affected = result.fetchone()[0] if result else 0
    except Exception:
        # DuckDB doesn't always return row counts for DDL/DML; that's fine
        rows_affected = -1

    if label:
        logger.info(f"[{label}] db_name={db_name} rows_affected={rows_affected}")

    return rows_affected
