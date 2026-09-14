"""
Unit tests for populate_keeper_economics SQL enrichment.

Spec: docs/superpowers/specs/2026-04-30-keeper-economics-sql-enrichment-design.md

Each test sets up an in-memory DuckDB with a player_fantasy table, runs the
enrichment via SQLEnrichments(data_dir=...), and asserts on the resulting
keeper_year + base_keeper_cost values.
"""

import sys
import tempfile
from pathlib import Path

import duckdb

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.transformations.sql_enrichments import SQLEnrichments  # noqa: E402


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

PLAYER_FANTASY_COLS = [
    ("db_name", "VARCHAR"),
    ("NFL_player_id", "VARCHAR"),
    ("franchise_id", "VARCHAR"),
    ("year", "INTEGER"),
    ("week", "INTEGER"),
    ("manager", "VARCHAR"),
    ("is_keeper", "INTEGER"),
    ("cost", "DOUBLE"),
    ("max_faab_bid_to_date", "DOUBLE"),
    ("round", "INTEGER"),
    ("keeper_year", "INTEGER"),
    ("base_keeper_cost", "DOUBLE"),
]

ROW_DEFAULTS = {
    "db_name": "test_league",
    "manager": "TestMgr",
    "is_keeper": 0,
    "cost": 0.0,
    "max_faab_bid_to_date": 0.0,
    "round": 0,
    "keeper_year": 0,
    "base_keeper_cost": 0.0,
}


def _create_player_fantasy(conn, columns=PLAYER_FANTASY_COLS):
    """Create the public.player_fantasy table with the given columns."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    cols_sql = ",\n            ".join(f'"{name}" {dtype}' for name, dtype in columns)
    conn.execute(f"CREATE TABLE public.player_fantasy (\n            {cols_sql}\n        )")


def _insert_rows(conn, rows):
    """Insert rows of dicts into public.player_fantasy. Missing keys take ROW_DEFAULTS."""
    col_names = [c for c, _ in PLAYER_FANTASY_COLS]
    for r in rows:
        merged = {**ROW_DEFAULTS, **r}
        values = [merged[c] for c in col_names]
        placeholders = ", ".join("?" for _ in col_names)
        col_list = ", ".join(f'"{c}"' for c in col_names)
        conn.execute(f"INSERT INTO public.player_fantasy ({col_list}) VALUES ({placeholders})", values)


def _make_enricher(conn, db_name="test_league"):
    """Build a SQLEnrichments wired to an in-memory connection.

    data_dir is set to a tmpdir so _db_filter returns '1=1' and _qualified_name
    returns 'public.player_fantasy' (local mode). The injected conn is reused;
    ATTACH ___ops is skipped because keeper enrichment doesn't need it.
    """
    eng = SQLEnrichments(db_name=db_name, data_dir=tempfile.gettempdir(), conn=conn)
    eng._ops_attached = True  # bypass ATTACH ___ops — keeper enrichment doesn't read it
    return eng


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_skip_returns_minus_one_when_keeper_year_column_missing():
    """Enrichment should return -1 (skipped) when player_fantasy lacks keeper_year column."""
    conn = duckdb.connect(":memory:")
    minimal_cols = [c for c in PLAYER_FANTASY_COLS if c[0] != "keeper_year"]
    _create_player_fantasy(conn, columns=minimal_cols)

    eng = _make_enricher(conn)
    result = eng.populate_keeper_economics()

    assert result == -1


def test_single_keeper_streak_marks_each_year_in_sequence():
    """A player kept by the same franchise for 3 consecutive years → keeper_year 1, 2, 3.
    base_keeper_cost is initial acquisition cost from the streak's first year."""
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    _insert_rows(
        conn,
        [
            # 2018 — initial acquisition (cost=10), is_keeper=0 (drafted, not kept)
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2018, "week": 16, "is_keeper": 0, "cost": 10.0},
            # 2019 — kept (is_keeper=1)
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2019, "week": 16, "is_keeper": 1, "cost": 10.0},
            # 2020 — kept again
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2020, "week": 16, "is_keeper": 1, "cost": 10.0},
            # 2021 — kept third time
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2021, "week": 16, "is_keeper": 1, "cost": 10.0},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    rows = conn.execute(
        "SELECT year, keeper_year, base_keeper_cost FROM public.player_fantasy ORDER BY year"
    ).fetchall()

    # 2018 was acquisition (not kept) → keeper_year = 0
    # 2019, 2020, 2021 were kept → keeper_year = 1, 2, 3
    # base_keeper_cost = initial acquisition cost (10.0) on all 3 keeper rows
    assert rows == [
        (2018, 0, 0.0),
        (2019, 1, 10.0),
        (2020, 2, 10.0),
        (2021, 3, 10.0),
    ]


def test_streak_breaks_when_player_is_dropped_then_re_acquired():
    """Player kept y1, dropped y2 (is_keeper=0 with new acquisition), kept again y3.
    Two independent streaks: y1=1 (with old base_cost), y3=1 (with new base_cost)."""
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    _insert_rows(
        conn,
        [
            # 2018 — kept (initial cost 10)
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2018, "week": 16, "is_keeper": 1, "cost": 10.0},
            # 2019 — re-acquired via auction at cost 25, NOT kept (is_keeper=0)
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2019, "week": 16, "is_keeper": 0, "cost": 25.0},
            # 2020 — kept again (new streak)
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2020, "week": 16, "is_keeper": 1, "cost": 25.0},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    rows = conn.execute(
        "SELECT year, keeper_year, base_keeper_cost FROM public.player_fantasy ORDER BY year"
    ).fetchall()

    # 2018 first streak → keeper_year = 1, base = 10
    # 2019 not kept → keeper_year = 0
    # 2020 new streak → keeper_year = 1, base = 25 (the new acquisition cost)
    assert rows == [
        (2018, 1, 10.0),
        (2019, 0, 0.0),
        (2020, 1, 25.0),
    ]


def test_streak_breaks_when_year_is_skipped():
    """Kept y1 and y3 (no row in y2 — player off all rosters that season).
    Two independent streaks at y1=1 and y3=1, despite is_keeper=1 on both."""
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    _insert_rows(
        conn,
        [
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2018, "week": 16, "is_keeper": 1, "cost": 10.0},
            # NO 2019 row — player was off all rosters that year
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2020, "week": 16, "is_keeper": 1, "cost": 15.0},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    rows = conn.execute(
        "SELECT year, keeper_year, base_keeper_cost FROM public.player_fantasy ORDER BY year"
    ).fetchall()

    # Both rows are start of new streak (year gap between them)
    assert rows == [
        (2018, 1, 10.0),
        (2020, 1, 15.0),
    ]


def test_separate_streaks_per_franchise_for_same_player():
    """Player on franchise F1 y1-y2 (kept y2), traded to franchise F2, kept by F2 y3-y4.
    franchise_id partitions the streak — F1 streak ends, F2 streak begins fresh."""
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    _insert_rows(
        conn,
        [
            # F1 acquires P1 in 2018, keeps in 2019
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2018, "week": 16, "is_keeper": 0, "cost": 10.0},
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2019, "week": 16, "is_keeper": 1, "cost": 10.0},
            # F2 acquires P1 in 2020 (trade), keeps in 2021
            {"NFL_player_id": "P1", "franchise_id": "F2", "year": 2020, "week": 16, "is_keeper": 0, "cost": 30.0},
            {"NFL_player_id": "P1", "franchise_id": "F2", "year": 2021, "week": 16, "is_keeper": 1, "cost": 30.0},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    rows = conn.execute(
        "SELECT franchise_id, year, keeper_year, base_keeper_cost "
        "FROM public.player_fantasy ORDER BY franchise_id, year"
    ).fetchall()

    assert rows == [
        ("F1", 2018, 0, 0.0),
        ("F1", 2019, 1, 10.0),
        ("F2", 2020, 0, 0.0),
        ("F2", 2021, 1, 30.0),
    ]


def test_base_keeper_cost_takes_greatest_of_cost_and_faab():
    """When acquisition was via FAAB after auction, base = max(cost, faab).

    Three players, all kept the next year:
    - P1: drafted at $10, never had a FAAB bid recorded → base = 10
    - P2: drafted free, picked up via $20 FAAB → base = 20
    - P3: drafted at $30, also had a $10 FAAB add later → base = 30 (cost wins)
    """
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    _insert_rows(
        conn,
        [
            # P1: draft cost 10, no FAAB
            {
                "NFL_player_id": "P1",
                "franchise_id": "F1",
                "year": 2018,
                "week": 16,
                "is_keeper": 0,
                "cost": 10.0,
                "max_faab_bid_to_date": 0.0,
            },
            {
                "NFL_player_id": "P1",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "is_keeper": 1,
                "cost": 10.0,
                "max_faab_bid_to_date": 0.0,
            },
            # P2: cost 0, FAAB 20
            {
                "NFL_player_id": "P2",
                "franchise_id": "F1",
                "year": 2018,
                "week": 16,
                "is_keeper": 0,
                "cost": 0.0,
                "max_faab_bid_to_date": 20.0,
            },
            {
                "NFL_player_id": "P2",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "is_keeper": 1,
                "cost": 0.0,
                "max_faab_bid_to_date": 20.0,
            },
            # P3: cost 30, FAAB 10 (cost wins)
            {
                "NFL_player_id": "P3",
                "franchise_id": "F1",
                "year": 2018,
                "week": 16,
                "is_keeper": 0,
                "cost": 30.0,
                "max_faab_bid_to_date": 10.0,
            },
            {
                "NFL_player_id": "P3",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "is_keeper": 1,
                "cost": 30.0,
                "max_faab_bid_to_date": 10.0,
            },
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    rows = conn.execute(
        "SELECT NFL_player_id, year, keeper_year, base_keeper_cost "
        "FROM public.player_fantasy WHERE keeper_year > 0 ORDER BY NFL_player_id"
    ).fetchall()

    assert rows == [
        ("P1", 2019, 1, 10.0),
        ("P2", 2019, 1, 20.0),
        ("P3", 2019, 1, 30.0),
    ]


def test_keeper_year_propagates_to_all_weeks_of_the_year():
    """End-of-season snapshot drives the streak detection, but keeper_year and
    base_keeper_cost must be written to ALL weeks of (player, franchise, year),
    not just the last week."""
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    rows = []
    # 2018: 17 weeks of acquisition
    for w in range(1, 18):
        rows.append(
            {
                "NFL_player_id": "P1",
                "franchise_id": "F1",
                "year": 2018,
                "week": w,
                "is_keeper": 0,
                "cost": 10.0,
            }
        )
    # 2019: 17 weeks, all marked is_keeper=1
    for w in range(1, 18):
        rows.append(
            {
                "NFL_player_id": "P1",
                "franchise_id": "F1",
                "year": 2019,
                "week": w,
                "is_keeper": 1,
                "cost": 10.0,
            }
        )
    _insert_rows(conn, rows)

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    # Every 2019 row should have keeper_year = 1 and base_keeper_cost = 10
    weekly = conn.execute(
        "SELECT week, keeper_year, base_keeper_cost FROM public.player_fantasy " "WHERE year = 2019 ORDER BY week"
    ).fetchall()
    assert len(weekly) == 17
    for week, ky, base in weekly:
        assert ky == 1, f"week {week} expected keeper_year=1, got {ky}"
        assert base == 10.0, f"week {week} expected base=10, got {base}"


def test_idempotent_on_rerun_and_resets_dropped_keepers():
    """Running the enrichment twice must produce identical state. AND if a row's
    is_keeper flag is flipped to 0 between runs, the previously-set keeper_year
    must reset to 0 (not stick from the prior run)."""
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    _insert_rows(
        conn,
        [
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2018, "week": 16, "is_keeper": 0, "cost": 10.0},
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2019, "week": 16, "is_keeper": 1, "cost": 10.0},
            {"NFL_player_id": "P1", "franchise_id": "F1", "year": 2020, "week": 16, "is_keeper": 1, "cost": 10.0},
        ],
    )

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    after_first = conn.execute(
        "SELECT year, keeper_year, base_keeper_cost FROM public.player_fantasy ORDER BY year"
    ).fetchall()

    eng.populate_keeper_economics()  # second run
    after_second = conn.execute(
        "SELECT year, keeper_year, base_keeper_cost FROM public.player_fantasy ORDER BY year"
    ).fetchall()

    assert after_first == after_second  # idempotent

    # Now flip 2020 to NOT a keeper, rerun, confirm 2020 keeper_year resets to 0
    conn.execute("UPDATE public.player_fantasy SET is_keeper = 0 WHERE year = 2020")
    eng.populate_keeper_economics()
    after_flip = conn.execute(
        "SELECT year, keeper_year, base_keeper_cost FROM public.player_fantasy ORDER BY year"
    ).fetchall()
    assert after_flip == [
        (2018, 0, 0.0),
        (2019, 1, 10.0),
        (2020, 0, 0.0),  # reset
    ]


def test_unrostered_and_blank_managers_are_excluded_from_streak_detection():
    """Rows with manager IN ('unrostered', 'fa', 'free agent', 'waivers'), blank,
    or NULL must NOT participate in the end-of-season snapshot. is_keeper=1 with
    a non-rostered manager should not count as a kept-by-franchise streak."""
    conn = duckdb.connect(":memory:")
    _create_player_fantasy(conn)
    _insert_rows(
        conn,
        [
            # Real keeper streak (control)
            {
                "NFL_player_id": "P1",
                "franchise_id": "F1",
                "year": 2018,
                "week": 16,
                "manager": "RealMgr",
                "is_keeper": 0,
                "cost": 10.0,
            },
            {
                "NFL_player_id": "P1",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "manager": "RealMgr",
                "is_keeper": 1,
                "cost": 10.0,
            },
            # P2 has is_keeper=1 but manager='Unrostered' — should NOT register as keeper
            {
                "NFL_player_id": "P2",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "manager": "Unrostered",
                "is_keeper": 1,
                "cost": 0.0,
            },
            # P3 has is_keeper=1 but manager='FA' — should NOT register
            {
                "NFL_player_id": "P3",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "manager": "FA",
                "is_keeper": 1,
                "cost": 0.0,
            },
            # P4 has is_keeper=1 but manager is blank — should NOT register
            {
                "NFL_player_id": "P4",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "manager": "",
                "is_keeper": 1,
                "cost": 0.0,
            },
            # P5 has is_keeper=1 but manager='Free Agent' (canonical multi-word) — should NOT register
            {
                "NFL_player_id": "P5",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "manager": "Free Agent",
                "is_keeper": 1,
                "cost": 0.0,
            },
            # P6 has is_keeper=1 but manager='Waivers' — should NOT register
            {
                "NFL_player_id": "P6",
                "franchise_id": "F1",
                "year": 2019,
                "week": 16,
                "manager": "Waivers",
                "is_keeper": 1,
                "cost": 0.0,
            },
        ],
    )

    # P7 has is_keeper=1 but manager IS NULL — should NOT register.
    # Inserted via raw SQL because _insert_rows applies ROW_DEFAULTS which would
    # coerce manager to 'TestMgr'.
    conn.execute(
        """
        INSERT INTO public.player_fantasy (
            db_name, "NFL_player_id", franchise_id, year, week, manager,
            is_keeper, cost, max_faab_bid_to_date, "round", keeper_year, base_keeper_cost
        ) VALUES (
            'test_league', 'P7', 'F1', 2019, 16, NULL, 1, 0.0, 0.0, 0, 0, 0.0
        )
        """
    )

    eng = _make_enricher(conn)
    eng.populate_keeper_economics()

    # Only P1's 2019 row should have keeper_year > 0
    rows = conn.execute(
        "SELECT NFL_player_id, year, keeper_year FROM public.player_fantasy "
        "WHERE keeper_year > 0 ORDER BY NFL_player_id"
    ).fetchall()
    assert rows == [("P1", 2019, 1)]
