"""Parity: backend Python SQL builder == frontend TS SQL builder.

Runs against ~5 fixtures in tests/fixtures/keeper_config/.
For each: feed the flat config to both builders; compare SQL execution
results against a synthetic player_fantasy fixture in DuckDB.

The Python builders target plain `player_fantasy` (unqualified).
The TS builders target `___leagues.public.player_fantasy` (centralised schema).
Both DuckDB connections are seeded with identical data in their respective
table namespaces; results are compared row-for-row after both UPDATE passes.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import duckdb
import pytest

from multi_league.core.keeper_config_flatten import flatten_rules_to_columns
from multi_league.transformations.player.sql_player_enrichments import (
    PlayerEnrichmentsMixin,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

# parents[0] = tests/integration/
# parents[1] = tests/
# parents[2] = fantasy_football_data_scripts/
# parents[3] = repo root
REPO_ROOT = Path(__file__).resolve().parents[3]
FIXTURES = REPO_ROOT / "tests" / "fixtures" / "keeper_config"
NODE_HELPER = REPO_ROOT / "frontend" / "src" / "scripts" / "dump-keeper-sql.ts"
FRONTEND_DIR = REPO_ROOT / "frontend"

_DB_NAME = "test_db"

# ---------------------------------------------------------------------------
# Minimal PlayerEnrichmentsMixin shim (avoids needing a real SQLEnrichments instance)
# ---------------------------------------------------------------------------


class _BareEnrich(PlayerEnrichmentsMixin):
    """Thin shim that binds a DuckDB connection without the full SQLEnrichments stack."""

    def __init__(self, conn: duckdb.DuckDBPyConnection, db_name: str) -> None:
        self.db_name = db_name
        self._conn = conn

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        return self._conn

    def _execute(self, sql: str, label: str = "") -> None:
        self._conn.execute(sql)

    def _qualified_name(self, table_name: str) -> str:
        return table_name


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

_CREATE_SQL = """
CREATE TABLE {table} (
    db_name VARCHAR, year INT, week INT, manager VARCHAR,
    NFL_player_id VARCHAR, round INT, pick INT, cost INT,
    max_faab_bid_to_date INT, keeper_year INT,
    base_keeper_cost INT, keeper_price INT
)
"""

_INSERT_SQL = """
INSERT INTO {table} VALUES
  ('{db}', 2024, 1, 'Joe',       'p1', 5, 60,  0,  0, 0, NULL, NULL),
  ('{db}', 2024, 1, 'Joe',       'p2', 3, 30,  0,  0, 1, NULL, NULL),
  ('{db}', 2024, 1, 'Joe',       'p3', 0,  0, 25, 30, 2, NULL, NULL),
  ('{db}', 2024, 1, 'unrostered','p4', 2, 20, 10,  0, 0, NULL, NULL)
"""

_SELECT_SQL = "SELECT NFL_player_id, base_keeper_cost, keeper_price " "FROM {table} ORDER BY NFL_player_id"


def _seed_py_conn() -> duckdb.DuckDBPyConnection:
    """Seed a connection with plain player_fantasy (Python builder target)."""
    conn = duckdb.connect(":memory:")
    conn.execute(_CREATE_SQL.format(table="player_fantasy"))
    conn.execute(_INSERT_SQL.format(db=_DB_NAME, table="player_fantasy"))
    return conn


def _seed_ts_conn() -> duckdb.DuckDBPyConnection:
    """Seed a connection with ___leagues.public.player_fantasy (TS builder target)."""
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___leagues.public")
    conn.execute(_CREATE_SQL.format(table="___leagues.public.player_fantasy"))
    conn.execute(_INSERT_SQL.format(db=_DB_NAME, table="___leagues.public.player_fantasy"))
    return conn


# ---------------------------------------------------------------------------
# TS helper invocation
# ---------------------------------------------------------------------------


def _ts_sql(flat_config: dict, tmp_path: Path) -> dict:
    """Invoke the Node dump script; returns dict with 'base' and 'price' SQL strings."""
    fixture_file = tmp_path / "fixture.json"
    fixture_file.write_text(json.dumps({"config": flat_config}))

    env = {**os.environ, "NODE_OPTIONS": "--conditions react-server"}

    result = subprocess.run(
        ["npx", "tsx", str(NODE_HELPER), str(fixture_file)],
        cwd=str(FRONTEND_DIR),
        capture_output=True,
        text=True,
        check=True,
        shell=True,  # required on Windows for npx to be found
        env=env,
    )
    return json.loads(result.stdout.strip())


# ---------------------------------------------------------------------------
# Parametrised test
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fixture_name",
    [
        "snake_basic.json",
        "auction_basic.json",
        "snake_with_escalation.json",
        "auction_with_position_limits.json",
        "partial_no_auction.json",
    ],
)
def test_python_and_ts_produce_equivalent_results(fixture_name: str, tmp_path: Path) -> None:
    """Both builders must produce identical row-for-row results on the synthetic fixture."""
    blob = json.loads((FIXTURES / fixture_name).read_text())
    flat = flatten_rules_to_columns(blob, strict=False)

    if not flat.get("enabled"):
        pytest.skip(f"{fixture_name}: config disabled — both sides no-op")

    # ── Python builders ──────────────────────────────────────────────────────
    py_conn = _seed_py_conn()
    eng = _BareEnrich(py_conn, _DB_NAME)
    py_conn.execute(eng._build_base_cost_sql(flat))
    py_conn.execute(eng._build_keeper_price_sql(flat))
    py_rows = py_conn.execute(_SELECT_SQL.format(table="player_fantasy")).fetchall()

    # ── TS builders via Node dump ─────────────────────────────────────────────
    ts_sqls = _ts_sql(flat, tmp_path)
    ts_conn = _seed_ts_conn()
    ts_conn.execute(ts_sqls["base"])
    ts_conn.execute(ts_sqls["price"])
    ts_rows = ts_conn.execute(_SELECT_SQL.format(table="___leagues.public.player_fantasy")).fetchall()

    assert py_rows == ts_rows, (
        f"Builder drift on {fixture_name}:\n"
        f"  Python : {py_rows}\n"
        f"  TS     : {ts_rows}\n"
        f"\n  Python base SQL : {eng._build_base_cost_sql(flat)}\n"
        f"\n  TS     base SQL : {ts_sqls['base']}"
    )
