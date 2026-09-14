"""Codex MCP server for the Fly DuckDB API.

The server is intentionally read-only. It exposes small, bounded helpers for
inspecting the live Fly-backed DuckDB databases via the existing HTTP API:

    - fly_query: run a read-only SQL query
    - fly_tables: list tables
    - fly_schema: describe a table
    - fly_ready: check the Fly server readiness endpoint

Configuration is loaded from the repo .env file, then environment variables:
DATABASE_SERVER_URL and DATABASE_READ_TOKEN are required.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import requests
from mcp.server.fastmcp import FastMCP


ROOT = Path(__file__).resolve().parents[1]
ENV_PATH = ROOT / ".env"
DEFAULT_TIMEOUT = 120
MAX_RETURN_ROWS = 500
VALID_DATABASES = {"___leagues", "___ops"}
IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*){0,2}$")
WRITE_PATTERN = re.compile(
    r"^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|COPY|ATTACH|DETACH|LOAD|INSTALL)\b",
    re.IGNORECASE,
)


def _load_dotenv() -> None:
    if not ENV_PATH.exists():
        return
    for raw_line in ENV_PATH.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv()


def _server_url() -> str:
    url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
    if not url:
        raise RuntimeError("DATABASE_SERVER_URL is not set")
    return url


def _read_token() -> str:
    token = os.environ.get("DATABASE_READ_TOKEN", "")
    if not token:
        raise RuntimeError("DATABASE_READ_TOKEN is not set")
    return token


def _validate_database(database: str) -> str:
    if database not in VALID_DATABASES:
        raise ValueError(f"database must be one of {sorted(VALID_DATABASES)}")
    return database


def _validate_read_only(sql: str) -> None:
    if WRITE_PATTERN.match(sql):
        raise ValueError("fly_query is read-only; write statements are not allowed")


def _post_query(sql: str, database: str) -> list[dict[str, Any]]:
    _validate_database(database)
    _validate_read_only(sql)
    response = requests.post(
        f"{_server_url()}/query",
        json={"sql": sql, "database": database},
        headers={"Authorization": f"Bearer {_read_token()}"},
        timeout=DEFAULT_TIMEOUT,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Fly query failed ({response.status_code}): {response.text}")
    rows = response.json()
    if not isinstance(rows, list):
        raise RuntimeError(f"Unexpected Fly response shape: {type(rows).__name__}")
    return rows


def _bounded(rows: list[dict[str, Any]], max_rows: int) -> dict[str, Any]:
    max_rows = max(1, min(int(max_rows), MAX_RETURN_ROWS))
    return {
        "row_count": len(rows),
        "returned_rows": min(len(rows), max_rows),
        "truncated": len(rows) > max_rows,
        "rows": rows[:max_rows],
    }


mcp = FastMCP(
    "fly-duckdb",
    instructions=(
        "Read-only access to the live Fly DuckDB API. "
        "Use ___leagues for app data and ___ops for ops/super-table data."
    ),
)


@mcp.tool()
def fly_query(sql: str, database: str = "___leagues", max_rows: int = 100) -> dict[str, Any]:
    """Run a read-only SQL query against Fly DuckDB and return bounded rows."""
    rows = _post_query(sql, database)
    return _bounded(rows, max_rows)


@mcp.tool()
def fly_tables(database: str = "___leagues", schema_name: str = "public", max_rows: int = 200) -> dict[str, Any]:
    """List tables in a Fly DuckDB database/schema."""
    _validate_database(database)
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema_name):
        raise ValueError("schema_name must be a simple SQL identifier")
    sql = (
        "SELECT database, schema, name, list_count(column_names) AS column_count, temporary "
        "FROM (SHOW ALL TABLES) "
        f"WHERE database = '{database}' AND schema = '{schema_name}' "
        "ORDER BY name"
    )
    return _bounded(_post_query(sql, database), max_rows)


@mcp.tool()
def fly_schema(table: str, database: str = "___leagues", max_rows: int = 200) -> dict[str, Any]:
    """Describe a table. Example: table='public.matchup'."""
    _validate_database(database)
    if not IDENTIFIER_RE.fullmatch(table):
        raise ValueError("table must be a dotted SQL identifier, like public.matchup")
    return _bounded(_post_query(f"DESCRIBE {table}", database), max_rows)


@mcp.tool()
def fly_ready() -> dict[str, Any]:
    """Return the Fly DuckDB /ready response."""
    response = requests.get(f"{_server_url()}/ready", timeout=30)
    try:
        body: Any = response.json()
    except ValueError:
        body = response.text
    return {"status_code": response.status_code, "body": body}


if __name__ == "__main__":
    mcp.run(transport="stdio")
