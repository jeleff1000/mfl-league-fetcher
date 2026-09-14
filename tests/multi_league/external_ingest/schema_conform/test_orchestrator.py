import json
import duckdb
import pandas as pd
import pytest
from multi_league.external_ingest.schema_conform import run, SchemaConformAbort
from multi_league.external_ingest.schema_conform._abort import AbortGate
from multi_league.external_ingest.schema_conform._ledger import (
    create_schema_decisions_table,
    query_decisions_for_run,
    create_external_identity_overrides_table,
)
from multi_league.external_ingest.schema_conform._orchestrator import _build_context


def _seed_canonical(conn):
    """Plant in-process canonical rows the matcher uses as reference."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager VARCHAR, manager_guid VARCHAR,
            team_name VARCHAR, opponent VARCHAR,
            team_points DOUBLE, opponent_points DOUBLE
        )
    """)
    rows = pd.DataFrame(
        {
            "db_name": ["kmffl"] * 9,
            "year": [2024] * 9,
            "week": [1, 1, 1, 2, 2, 2, 3, 3, 3],
            "manager": ["Adin", "Marc", "Tani"] * 3,
            "manager_guid": ["3FJVUGAHEWQKCU2Z2TQQQMPWEA", "FYW5PMKZ23OGUAONDX6AGROCZQ", "JKFP2XZZK4L36MF2DI7CEWS3J4"]
            * 3,
            "team_name": ["Team A", "Team B", "Team C"] * 3,
            "opponent": ["Marc", "Adin", "Tani"] * 3,
            "team_points": [100.0, 110.0, 120.0] * 3,
            "opponent_points": [90.0, 95.0, 100.0] * 3,
        }
    )
    conn.register("rows_view", rows)
    conn.execute("INSERT INTO public.matchup SELECT * FROM rows_view")


def _seed_raw_external(conn):
    """Plant a raw 2014 matchup parquet with manager_guid present."""
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("""
        CREATE TABLE staging.staging_matchup (
            db_name VARCHAR, year VARCHAR, week VARCHAR,
            manager VARCHAR, manager_guid VARCHAR,
            team_name VARCHAR, opponent VARCHAR,
            team_points VARCHAR, opponent_points VARCHAR
        )
    """)
    rows = pd.DataFrame(
        {
            "db_name": ["kmffl"] * 3,
            "year": ["2014"] * 3,
            "week": ["1"] * 3,
            "manager": ["Adin", "Marc", "Tani"],
            "manager_guid": ["3FJVUGAHEWQKCU2Z2TQQQMPWEA", "FYW5PMKZ23OGUAONDX6AGROCZQ", "JKFP2XZZK4L36MF2DI7CEWS3J4"],
            "team_name": ["2014 Team", "2014 Team", "2014 Team"],
            "opponent": ["Marc", "Adin", "Tani"],
            "team_points": ["80.5", "90.0", "100.0"],
            "opponent_points": ["75.0", "85.5", "95.0"],
        }
    )
    conn.register("raw", rows)
    conn.execute("INSERT INTO staging.staging_matchup SELECT * FROM raw")


def test_orchestrator_no_external_is_noop():
    conn = duckdb.connect(":memory:")
    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    _seed_canonical(conn)
    # No staging.staging_* tables → no-op
    run(conn, db_name="kmffl", run_id="r1")
    rows = query_decisions_for_run(conn, "kmffl", "r1")
    assert rows == []


def test_orchestrator_happy_path_kmffl_2014():
    conn = duckdb.connect(":memory:")
    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    _seed_canonical(conn)
    _seed_raw_external(conn)

    run(conn, db_name="kmffl", run_id="r1")

    # staging.conformed_matchup must exist with 3 rows
    n = conn.execute("SELECT count(*) FROM staging.conformed_matchup WHERE db_name='kmffl'").fetchone()[0]
    assert n == 3

    # All 3 manager_guids must be canonical
    guids = (
        conn.execute("SELECT DISTINCT manager_guid FROM staging.conformed_matchup ORDER BY 1")
        .df()["manager_guid"]
        .tolist()
    )
    assert set(guids) == {"3FJVUGAHEWQKCU2Z2TQQQMPWEA", "FYW5PMKZ23OGUAONDX6AGROCZQ", "JKFP2XZZK4L36MF2DI7CEWS3J4"}

    # Ledger has rows with status='bound' for required slots
    decisions = query_decisions_for_run(conn, "kmffl", "r1")
    bound_slots = {d["ddl_slot"] for d in decisions if d["status"] == "bound"}
    for slot in ("year", "week", "manager_guid"):
        assert slot in bound_slots, f"missing bound entry for {slot}"


def test_orchestrator_applies_saved_franchise_merges_before_identity_gate():
    """Archived Yahoo owner IDs must honor the league's saved renewal-chain merges."""
    conn = duckdb.connect(":memory:")
    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    _seed_canonical(conn)
    _seed_raw_external(conn)

    conn.execute(
        "UPDATE staging.staging_matchup "
        "SET manager_guid='OLD_YAHOO_OWNER_ID' WHERE manager='Adin'"
    )

    run(
        conn,
        db_name="kmffl",
        run_id="r-archived-owner",
        franchise_merges=[
            {
                "display_name": "Adin",
                "owner_ids": ["3FJVUGAHEWQKCU2Z2TQQQMPWEA", "OLD_YAHOO_OWNER_ID"],
            }
        ],
    )

    guids = conn.execute(
        "SELECT DISTINCT manager_guid FROM staging.conformed_matchup "
        "WHERE manager='Adin'"
    ).fetchall()
    assert guids == [("3FJVUGAHEWQKCU2Z2TQQQMPWEA",)]


def test_orchestrator_aborts_on_missing_required_identity():
    """Remove `year` (a non-derivable IDENTITY slot) from raw → UNMAPPED_REQUIRED.
    Also asserts that a failure row is written to staging.import_run_summary.

    Note: `manager_guid` is in DERIVATION_RULES so removing it would not abort
    (synthetic ids get minted instead). We use `year` to test the abort path.
    """
    conn = duckdb.connect(":memory:")
    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    _seed_canonical(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("""
        CREATE TABLE staging.staging_matchup (
            db_name VARCHAR, week VARCHAR,
            manager VARCHAR, manager_guid VARCHAR,
            team_name VARCHAR, opponent VARCHAR,
            team_points VARCHAR, opponent_points VARCHAR
        )
    """)
    # Insert rows with NO year column → required IDENTITY 'year' has no candidate
    # and no derivation rule.
    conn.execute("""
        INSERT INTO staging.staging_matchup VALUES
        ('kmffl','1','Adin','3FJVUGAHEWQKCU2Z2TQQQMPWEA','X','Marc','100','90'),
        ('kmffl','2','Marc','FYW5PMKZ23OGUAONDX6AGROCZQ','Y','Adin','110','100')
    """)

    with pytest.raises(SchemaConformAbort) as exc:
        run(conn, db_name="kmffl", run_id="r1")
    assert any(f.gate in (AbortGate.UNMAPPED_REQUIRED, AbortGate.COVERAGE_BELOW_THRESHOLD) for f in exc.value.failures)

    # Gap D: import_run_summary must have a 'failed' row
    row = conn.execute(
        "SELECT status, failures_json FROM staging.import_run_summary "
        "WHERE db_name='kmffl' AND run_id='r1' AND phase='schema_conform'"
    ).fetchone()
    assert row is not None, "import_run_summary row not written on abort"
    assert row[0] == "failed"
    raw_json = row[1]
    parsed = json.loads(raw_json) if isinstance(raw_json, str) else raw_json
    assert parsed is not None and len(parsed) > 0, "failures_json must be non-empty"


def test_build_context_reraises_unexpected_errors():
    """Gap E: _build_context re-raises non-CatalogException errors from matchup query.

    We use a wrapper object whose .execute() raises RuntimeError on the matchup
    query, simulating a structural DB error that is not a CatalogException.
    """

    class _MockConn:
        """Thin wrapper: raises RuntimeError for the matchup query, passes through others."""

        def __init__(self, real_conn):
            self._real = real_conn
            self._calls = 0

        def execute(self, sql, params=None):
            if "public.matchup" in sql:
                self._calls += 1
                if self._calls <= 1:
                    raise RuntimeError("simulated structural error")
            if params is not None:
                return self._real.execute(sql, params)
            return self._real.execute(sql)

        def df(self):
            return self._real.df()

    real_conn = duckdb.connect(":memory:")
    real_conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    mock_conn = _MockConn(real_conn)

    with pytest.raises(RuntimeError, match="simulated structural error"):
        _build_context(mock_conn, db_name="kmffl")


def test_derivable_identity_slot_recovers_via_derivation():
    """Regression: KMFFL 2014 draft/transactions parquets have NO manager_guid column,
    only manager + manager_year. Orchestrator must NOT abort UNMAPPED_REQUIRED — it
    must let derivation fill manager_guid from the manager column via fuzzy lookup
    against ctx.name_to_guid. Reproduces the production failure that hit on 2026-04-26.
    """
    conn = duckdb.connect(":memory:")
    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    _seed_canonical(conn)

    # Seed public.draft so the matcher has reference data for the draft table
    # (in production this is populated by the platform fetcher before PHASE 1.7).
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR, year INTEGER, round INTEGER, pick INTEGER,
            manager VARCHAR, manager_guid VARCHAR, yahoo_player_id VARCHAR,
            player VARCHAR, cost DOUBLE
        )
        """
    )
    for i, name in enumerate(["Adin", "Marc", "Tani"], start=1):
        conn.execute(
            "INSERT INTO public.draft VALUES (?, 2024, 1, ?, ?, ?, ?, 'P', 10.0)",
            ["kmffl", i, name, f"GUID_{name.upper()}", str(5000 + i)],
        )

    # Plant a draft parquet that mirrors the real KMFFL 2014 draft shape:
    # manager column present (so derivation has a hook), but no manager_guid column,
    # AND a noisy column ("manager_year" = "Adin2014" etc.) that the matcher might
    # otherwise wrongly route to the manager_guid slot.
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        """
        CREATE TABLE staging.staging_draft (
            db_name VARCHAR, year VARCHAR, round VARCHAR, pick VARCHAR,
            manager VARCHAR, manager_year VARCHAR, yahoo_player_id VARCHAR,
            player VARCHAR, cost VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO staging.staging_draft VALUES
        ('kmffl','2014','1','1','Adin','Adin2014','5001','Player1','10'),
        ('kmffl','2014','1','2','Marc','Marc2014','5002','Player2','12'),
        ('kmffl','2014','1','3','Tani','Tani2014','5003','Player3','8')
        """
    )

    # Should NOT raise — derivation fills manager_guid from manager via fuzzy lookup.
    run(conn, db_name="kmffl", run_id="r1")

    df = conn.execute("SELECT manager, manager_guid FROM staging.conformed_draft WHERE db_name='kmffl'").df()
    assert len(df) == 3
    assert df["manager_guid"].notna().all(), "derivation failed to populate manager_guid"
    expected = {
        "Adin": "3FJVUGAHEWQKCU2Z2TQQQMPWEA",
        "Marc": "FYW5PMKZ23OGUAONDX6AGROCZQ",
        "Tani": "JKFP2XZZK4L36MF2DI7CEWS3J4",
    }
    for mgr, expected_guid in expected.items():
        rows = df[df["manager"] == mgr]
        assert (
            rows["manager_guid"] == expected_guid
        ).all(), f"{mgr} did not derive to canonical guid (got {rows['manager_guid'].tolist()})"


def test_cross_table_team_key_mismatch_demotes_not_aborts():
    """Regression: when matcher wrongly binds a non-team_key column to the team_key
    slot in matchup, cross-table check sees 0% overlap with draft's real team_keys.
    team_key is OPTIONAL — orchestrator must NULL the column in the offending table
    and continue, not hard-abort the whole import. Reproduces production failure
    on KMFFL re-import 2026-04-26.
    """
    conn = duckdb.connect(":memory:")
    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    _seed_canonical(conn)

    # Seed public.draft so matcher has reference data for draft table.
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR, year INTEGER, round INTEGER, pick INTEGER,
            manager VARCHAR, manager_guid VARCHAR, team_key VARCHAR,
            yahoo_player_id VARCHAR, player VARCHAR, cost DOUBLE
        )
        """
    )
    for i, name in enumerate(["Adin", "Marc", "Tani"], start=1):
        conn.execute(
            "INSERT INTO public.draft VALUES (?, 2024, 1, ?, ?, ?, ?, ?, 'P', 10.0)",
            ["kmffl", i, name, f"GUID_{name.upper()}", f"331.l.x.t.{i}", str(5000 + i)],
        )

    # Plant matchup with no team_key column at all.
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(
        """
        CREATE TABLE staging.staging_matchup (
            db_name VARCHAR, year VARCHAR, week VARCHAR,
            manager VARCHAR, manager_guid VARCHAR, team_name VARCHAR,
            opponent VARCHAR, team_points VARCHAR, opponent_points VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO staging.staging_matchup VALUES
        ('kmffl','2014','1','Adin','3FJVUGAHEWQKCU2Z2TQQQMPWEA','TeamA','Marc','100','90'),
        ('kmffl','2014','1','Marc','FYW5PMKZ23OGUAONDX6AGROCZQ','TeamB','Adin','110','100'),
        ('kmffl','2014','1','Tani','JKFP2XZZK4L36MF2DI7CEWS3J4','TeamC','Adin','120','95')
        """
    )

    # Plant draft with REAL team_keys that don't match anything in matchup.
    conn.execute(
        """
        CREATE TABLE staging.staging_draft (
            db_name VARCHAR, year VARCHAR, round VARCHAR, pick VARCHAR,
            manager VARCHAR, manager_guid VARCHAR, team_key VARCHAR,
            yahoo_player_id VARCHAR, player VARCHAR, cost VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO staging.staging_draft VALUES
        ('kmffl','2014','1','1','Adin','3FJVUGAHEWQKCU2Z2TQQQMPWEA','238.l.90939.t.1','5001','P1','10'),
        ('kmffl','2014','1','2','Marc','FYW5PMKZ23OGUAONDX6AGROCZQ','238.l.90939.t.2','5002','P2','12'),
        ('kmffl','2014','1','3','Tani','JKFP2XZZK4L36MF2DI7CEWS3J4','238.l.90939.t.3','5003','P3','8')
        """
    )

    # Should NOT raise — team_key mismatch demotes the offending table's column.
    run(conn, db_name="kmffl", run_id="r1")

    # Both conformed tables should exist; team_key in matchup may be NULL or absent.
    n_matchup = conn.execute("SELECT count(*) FROM staging.conformed_matchup WHERE db_name='kmffl'").fetchone()[0]
    n_draft = conn.execute("SELECT count(*) FROM staging.conformed_draft WHERE db_name='kmffl'").fetchone()[0]
    assert n_matchup == 3
    assert n_draft == 3

    # If matchup has a team_key column at all, it must be all-NULL after demotion.
    cols = [
        c[0]
        for c in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema='staging' AND table_name='conformed_matchup'"
        ).fetchall()
    ]
    if "team_key" in cols:
        nn = conn.execute(
            "SELECT count(*) FROM staging.conformed_matchup WHERE team_key IS NOT NULL AND db_name='kmffl'"
        ).fetchone()[0]
        assert nn == 0, "team_key in matchup should have been demoted to NULL"
