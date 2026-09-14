"""
Unit tests for populate_max_keepers SQL enrichment.

Forward-only fix for the Bug #1 carryover (settings_enrichment dead in main flow).
The enrichment fills league_settings.max_keepers from draft keeper picks per year.
We do NOT backfill existing data — only fill on new/re-imported leagues. Already-
populated max_keepers values are preserved (no overwrite).

Each test sets up an in-memory DuckDB with a draft + league_settings table,
runs the enrichment via SQLEnrichments(data_dir=...), and asserts on the
resulting league_settings.max_keepers values.
"""

import sys
import tempfile
from pathlib import Path

import duckdb

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.transformations.sql_enrichments import SQLEnrichments  # noqa: E402


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _create_draft(conn, keeper_columns=("is_keeper",)):
    """Create public.draft with the given keeper-flag columns.

    Sleeper uses `is_keeper`; Yahoo uses `is_keeper_status` and `is_keeper_cost`.
    Tests can request the keeper columns relevant to the platform they're
    simulating to exercise the cross-platform COALESCE logic.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    base_cols = [
        ('"db_name"', "VARCHAR"),
        ('"year"', "INTEGER"),
        ('"manager"', "VARCHAR"),
        ('"round"', "INTEGER"),
        ('"pick"', "INTEGER"),
    ]
    keeper_cols = [(f'"{c}"', "INTEGER") for c in keeper_columns]
    cols_sql = ",\n            ".join(f"{name} {dtype}" for name, dtype in base_cols + keeper_cols)
    conn.execute(f"CREATE TABLE public.draft (\n            {cols_sql}\n        )")


def _insert_draft(conn, rows):
    """Insert dicts into public.draft. Caller supplies the exact keys."""
    if not rows:
        return
    keys = list(rows[0].keys())
    placeholders = ", ".join("?" for _ in keys)
    col_list = ", ".join(f'"{k}"' for k in keys)
    for r in rows:
        values = [r[k] for k in keys]
        conn.execute(f"INSERT INTO public.draft ({col_list}) VALUES ({placeholders})", values)


def _create_league_settings(conn, max_keepers_value=None):
    """Create public.league_settings with a single (db_name, year) row.

    Default leaves max_keepers NULL so the enrichment fills it.
    """
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            db_name VARCHAR,
            year INTEGER,
            max_keepers INTEGER,
            PRIMARY KEY (db_name, year)
        )
        """
    )


def _insert_settings_row(conn, year, max_keepers=None, db_name="test_league"):
    conn.execute(
        "INSERT INTO public.league_settings (db_name, year, max_keepers) VALUES (?, ?, ?)",
        [db_name, year, max_keepers],
    )


def _make_enricher(conn, db_name="test_league"):
    """Build a SQLEnrichments wired to an in-memory connection.

    Mirrors test_populate_keeper_economics._make_enricher: the injected conn
    is reused, and ATTACH ___ops is bypassed because max_keepers enrichment
    doesn't read super_table data.
    """
    eng = SQLEnrichments(db_name=db_name, data_dir=tempfile.gettempdir(), conn=conn)
    eng._ops_attached = True
    return eng


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fills_null_max_keepers_from_sleeper_is_keeper_column():
    """Sleeper-style draft (is_keeper column). Two managers each kept 2 players,
    one kept 0. league_settings.max_keepers should be filled to 2."""
    conn = duckdb.connect(":memory:")
    _create_draft(conn, keeper_columns=("is_keeper",))
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2024, max_keepers=None)

    _insert_draft(
        conn,
        [
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 1, "pick": 1, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 2, "pick": 2, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 3, "pick": 3, "is_keeper": 0},
            {"db_name": "test_league", "year": 2024, "manager": "Bob", "round": 4, "pick": 4, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Bob", "round": 5, "pick": 5, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Carol", "round": 6, "pick": 6, "is_keeper": 0},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_max_keepers()

    result = conn.execute("SELECT max_keepers FROM public.league_settings WHERE year = 2024").fetchone()
    assert result == (2,), f"Expected max_keepers=2, got {result}"


def test_does_not_overwrite_existing_max_keepers_value():
    """If max_keepers is already set (e.g. from platform settings API),
    the enrichment must NOT overwrite it. Forward-only, fill-on-NULL."""
    conn = duckdb.connect(":memory:")
    _create_draft(conn, keeper_columns=("is_keeper",))
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2024, max_keepers=4)  # API said 4

    _insert_draft(
        conn,
        [
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 1, "pick": 1, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 2, "pick": 2, "is_keeper": 1},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_max_keepers()

    result = conn.execute("SELECT max_keepers FROM public.league_settings WHERE year = 2024").fetchone()
    assert result == (4,), f"Expected max_keepers preserved at 4, got {result}"


def test_fills_from_yahoo_is_keeper_status_and_cost_columns():
    """Yahoo draft has is_keeper_status (1=kept) and is_keeper_cost (>0=kept).
    Either signal counts as a keeper pick."""
    conn = duckdb.connect(":memory:")
    _create_draft(conn, keeper_columns=("is_keeper_status", "is_keeper_cost"))
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2024, max_keepers=None)

    _insert_draft(
        conn,
        [
            # Alice: 2 keepers via is_keeper_status
            {
                "db_name": "test_league",
                "year": 2024,
                "manager": "Alice",
                "round": 1,
                "pick": 1,
                "is_keeper_status": 1,
                "is_keeper_cost": 0,
            },
            {
                "db_name": "test_league",
                "year": 2024,
                "manager": "Alice",
                "round": 2,
                "pick": 2,
                "is_keeper_status": 1,
                "is_keeper_cost": 0,
            },
            # Bob: 1 keeper via is_keeper_cost (> 0 means kept)
            {
                "db_name": "test_league",
                "year": 2024,
                "manager": "Bob",
                "round": 3,
                "pick": 3,
                "is_keeper_status": 0,
                "is_keeper_cost": 25,
            },
            # Carol: 0 keepers
            {
                "db_name": "test_league",
                "year": 2024,
                "manager": "Carol",
                "round": 4,
                "pick": 4,
                "is_keeper_status": 0,
                "is_keeper_cost": 0,
            },
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_max_keepers()

    result = conn.execute("SELECT max_keepers FROM public.league_settings WHERE year = 2024").fetchone()
    assert result == (2,), f"Expected max_keepers=2 (Alice's 2 keepers), got {result}"


def test_no_keeper_picks_leaves_max_keepers_null():
    """League with zero keeper picks across all draft rows. max_keepers stays NULL —
    the enrichment must not write 0 (which would lie about the league's keeper config)."""
    conn = duckdb.connect(":memory:")
    _create_draft(conn, keeper_columns=("is_keeper",))
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2024, max_keepers=None)

    _insert_draft(
        conn,
        [
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 1, "pick": 1, "is_keeper": 0},
            {"db_name": "test_league", "year": 2024, "manager": "Bob", "round": 2, "pick": 2, "is_keeper": 0},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_max_keepers()

    result = conn.execute("SELECT max_keepers FROM public.league_settings WHERE year = 2024").fetchone()
    assert result == (None,), f"Expected max_keepers NULL (no keeper picks), got {result}"


def test_per_year_independence():
    """A multi-year league with different keeper caps per year. Each year
    must be evaluated independently; one year's max doesn't bleed across."""
    conn = duckdb.connect(":memory:")
    _create_draft(conn, keeper_columns=("is_keeper",))
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2023, max_keepers=None)
    _insert_settings_row(conn, year=2024, max_keepers=None)

    _insert_draft(
        conn,
        [
            # 2023: Alice kept 1
            {"db_name": "test_league", "year": 2023, "manager": "Alice", "round": 1, "pick": 1, "is_keeper": 1},
            {"db_name": "test_league", "year": 2023, "manager": "Bob", "round": 2, "pick": 2, "is_keeper": 0},
            # 2024: Alice kept 3
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 1, "pick": 1, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 2, "pick": 2, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 3, "pick": 3, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Bob", "round": 4, "pick": 4, "is_keeper": 1},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_max_keepers()

    rows = conn.execute("SELECT year, max_keepers FROM public.league_settings ORDER BY year").fetchall()
    assert rows == [(2023, 1), (2024, 3)], f"Expected per-year independence, got {rows}"


def test_skips_gracefully_when_draft_table_missing():
    """No draft table → enrichment returns -1 (skipped), does not raise."""
    conn = duckdb.connect(":memory:")
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2024, max_keepers=None)

    eng = _make_enricher(conn)
    result = eng.populate_max_keepers()

    assert result == -1, f"Expected -1 (skipped), got {result}"


def test_skips_gracefully_when_no_keeper_columns_present():
    """Draft exists but has no is_keeper / is_keeper_status / is_keeper_cost
    columns at all (e.g. brand-new league with zero keeper history). Enrichment
    returns -1 instead of raising a binder error."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR, year INTEGER, manager VARCHAR, round INTEGER, pick INTEGER
        )
        """
    )
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2024, max_keepers=None)

    eng = _make_enricher(conn)
    result = eng.populate_max_keepers()

    assert result == -1, f"Expected -1 (no keeper columns), got {result}"


def test_idempotent_on_rerun():
    """Running twice must produce the same result. The second run must not
    overwrite the first run's value (because the first run already populated it)."""
    conn = duckdb.connect(":memory:")
    _create_draft(conn, keeper_columns=("is_keeper",))
    _create_league_settings(conn)
    _insert_settings_row(conn, year=2024, max_keepers=None)

    _insert_draft(
        conn,
        [
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 1, "pick": 1, "is_keeper": 1},
            {"db_name": "test_league", "year": 2024, "manager": "Alice", "round": 2, "pick": 2, "is_keeper": 1},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_max_keepers()
    after_first = conn.execute("SELECT max_keepers FROM public.league_settings WHERE year = 2024").fetchone()
    eng.populate_max_keepers()
    after_second = conn.execute("SELECT max_keepers FROM public.league_settings WHERE year = 2024").fetchone()
    assert after_first == after_second == (2,)
