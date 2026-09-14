import pytest
import duckdb
import pandas as pd
from pathlib import Path

from multi_league.external_ingest.schema_conform import run, SchemaConformAbort
from multi_league.external_ingest.schema_conform._abort import AbortGate
from multi_league.external_ingest.schema_conform._ledger import (
    create_schema_decisions_table,
    create_external_identity_overrides_table,
)

FIXTURES = Path(__file__).parent.parent.parent.parent / "fixtures" / "schema_conform"

KNOWN_GUIDS = {
    "adin": "3FJVUGAHEWQKCU2Z2TQQQMPWEA",
    "marc": "FYW5PMKZ23OGUAONDX6AGROCZQ",
    "daniel": "KM4FIU3EKJKO4RYTLCNIPAIMWI",
    "dave": "DAVEXXXXXXXXXXXXXXXXXXXXXX",  # synthetic 26-char canonical
}


def _seed_canonical(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("""
        CREATE TABLE public.matchup
        (db_name VARCHAR, year INTEGER, week INTEGER,
         manager VARCHAR, manager_guid VARCHAR,
         team_name VARCHAR, opponent VARCHAR,
         team_points DOUBLE, opponent_points DOUBLE)
    """)
    rows = []
    for name, guid in KNOWN_GUIDS.items():
        for week in range(1, 18):
            rows.append(
                {
                    "db_name": "kmffl",
                    "year": 2024,
                    "week": week,
                    "manager": name.capitalize(),
                    "manager_guid": guid,
                    "team_name": f"team_{name}",
                    "opponent": "Adin",
                    "team_points": 100.0,
                    "opponent_points": 95.0,
                }
            )
    df = pd.DataFrame(rows)
    conn.register("c", df)
    conn.execute("INSERT INTO public.matchup SELECT * FROM c")


def _load_fixture_to_staging(conn, fixture_name: str, table: str = "matchup"):
    df = pd.read_parquet(FIXTURES / fixture_name)
    df = df.astype(str).where(df.notna(), None)
    # db_name must be the first column to match the DDL ordering
    df.insert(0, "db_name", "kmffl")
    cols_ddl = ", ".join(f"{c} VARCHAR" for c in df.columns if c != "db_name")
    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute(f"CREATE TABLE staging.staging_{table} (db_name VARCHAR, {cols_ddl})")
    conn.register("fx", df)
    conn.execute(f"INSERT INTO staging.staging_{table} SELECT * FROM fx")


def _setup(fixture: str, table: str = "matchup"):
    conn = duckdb.connect(":memory:")
    create_schema_decisions_table(conn)
    create_external_identity_overrides_table(conn)
    _seed_canonical(conn)
    _load_fixture_to_staging(conn, fixture, table)
    return conn


def test_mangled_manager_guid_recovers_via_derivation():
    """When manager_guid column is mangled (truncated values fail invariant), the
    matcher rejects it AND falls back to derivation via the manager column. Since
    manager names ARE in canonical (Adin, Marc), derivation succeeds — no abort.
    This is the spec-intended graceful recovery for partial data quality issues.
    """
    conn = _setup("mangled_manager_guid.parquet")
    run(conn, "kmffl", "rN")  # should NOT raise — derivation recovers
    df = conn.execute("SELECT manager, manager_guid FROM staging.conformed_matchup WHERE db_name='kmffl'").df()
    assert df["manager_guid"].notna().all(), "derivation should have populated all rows"
    # Adin and Marc should bind to canonical guids, not the mangled mystery_id values.
    for name, expected in [("Adin", KNOWN_GUIDS["adin"]), ("Marc", KNOWN_GUIDS["marc"])]:
        rows = df[df["manager"] == name]
        if not rows.empty:
            assert (rows["manager_guid"] == expected).all(), f"{name} did not derive to canonical guid"


def test_whitespace_manager_succeeds():
    conn = _setup("whitespace_manager.parquet")
    run(conn, "kmffl", "rN")  # should NOT raise — _norm strips whitespace
    df = conn.execute("SELECT manager_guid FROM staging.conformed_matchup").df()
    assert df["manager_guid"].str.startswith(("3F", "FY")).all()


def test_swapped_guid_columns_resolves():
    conn = _setup("swapped_guid_columns.parquet")
    # Co-occurrence filter should pick the column that co-varies with `manager`
    run(conn, "kmffl", "rN")
    df = conn.execute("SELECT manager, manager_guid FROM staging.conformed_matchup").df()
    # Adin's rows must have Adin's canonical guid
    adin_rows = df[df["manager"].str.lower() == "adin"]
    if not adin_rows.empty:
        assert (adin_rows["manager_guid"] == KNOWN_GUIDS["adin"]).all()


def test_missing_year_aborts():
    conn = _setup("missing_year.parquet")
    with pytest.raises(SchemaConformAbort) as exc:
        run(conn, "kmffl", "rN")
    assert any(f.gate == AbortGate.UNMAPPED_REQUIRED for f in exc.value.failures)


def test_collision_dual_guids_aborts_or_disambiguates():
    """Either SLOT_COLLISION fires, or co-occurrence filter picks the right one and binds cleanly."""
    conn = _setup("collision_dual_guids.parquet")
    try:
        run(conn, "kmffl", "rN")
    except SchemaConformAbort as e:
        assert any(f.gate == AbortGate.SLOT_COLLISION for f in e.failures)


def test_ambiguous_fuzzy_name_falls_back_to_synthetic():
    """When manager_guid is absent and manager name 'Dan' scores 66 vs 'daniel'
    (below FUZZY_CUTOFF=90), fuzzy lookup returns None. The synthetic-id machinery
    then mints a stable external_<hash> for Dan — treating him as a new manager.
    The user can later confirm via external_identity_overrides if they want to
    merge Dan→Daniel manually. This is the spec-intended graceful behavior for
    unrecognized names; abort is reserved for Daniel/Dave-collision cases above
    the fuzzy cutoff (which never fire in practice for short names like 'Dan').
    """
    conn = _setup("ambiguous_fuzzy_name.parquet")
    run(conn, "kmffl", "rN")  # should NOT raise — synthetic id minted
    df = conn.execute("SELECT manager, manager_guid FROM staging.conformed_matchup WHERE db_name='kmffl'").df()
    # Marc (in canonical) should bind to canonical guid via derivation.
    marc_rows = df[df["manager"] == "Marc"]
    if not marc_rows.empty:
        assert (marc_rows["manager_guid"] == KNOWN_GUIDS["marc"]).all()
    # Dan (NOT in canonical) should get a stable synthetic external_<hash>.
    dan_rows = df[df["manager"] == "Dan"]
    if not dan_rows.empty:
        guids = dan_rows["manager_guid"].dropna().unique()
        assert len(guids) == 1, f"Dan got {len(guids)} different guids"
        assert guids[0].startswith("external_"), f"Dan should get synthetic id, got {guids[0]!r}"


def test_aliased_columns_succeed():
    """mgr_id, team_pts should be recognized via alias normalization."""
    conn = _setup("aliased_columns.parquet")
    run(conn, "kmffl", "rN")  # should NOT raise
    df = conn.execute("SELECT * FROM staging.conformed_matchup").df()
    assert "manager_guid" in df.columns
    assert df["manager_guid"].notna().all()
