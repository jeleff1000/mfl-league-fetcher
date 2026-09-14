import json
import shutil
import pytest
import duckdb
from pathlib import Path

FIXTURE_DB = Path(__file__).parent.parent / "fixtures" / "lamar_regression.duckdb"
BASELINES = Path(__file__).parent.parent / "fixtures" / "lamar_regression_baseline.json"
COMBOS = Path(__file__).parent.parent / "fixtures" / "lamar_combos.json"


@pytest.fixture(scope="session")
def regression_db_readonly():
    """Session-scoped read-only DuckDB connection (for scoring config tests)."""
    if not FIXTURE_DB.exists():
        pytest.skip("Regression fixture not built. Run scripts/build_lamar_regression_db.py first.")
    conn = duckdb.connect(str(FIXTURE_DB), read_only=True)
    yield conn
    conn.close()


@pytest.fixture
def regression_db_writable(tmp_path):
    """Per-test writable copy of the regression DuckDB (for LAMAR calculation tests).
    Copies the fixture (and its WAL file if present) to a temp dir so
    calculate_lamar() can UPDATE tables.
    """
    if not FIXTURE_DB.exists():
        pytest.skip("Regression fixture not built. Run scripts/build_lamar_regression_db.py first.")
    tmp_db = tmp_path / "lamar_regression.duckdb"
    shutil.copy2(FIXTURE_DB, tmp_db)
    # Copy WAL file so all schemas written since last checkpoint are visible.
    wal_src = Path(str(FIXTURE_DB) + ".wal")
    if wal_src.exists():
        shutil.copy2(wal_src, Path(str(tmp_db) + ".wal"))
    conn = duckdb.connect(str(tmp_db))
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def combos():
    """Load combo manifest."""
    if not COMBOS.exists():
        pytest.skip("Combo manifest not found. Run scripts/lamar_census.py first.")
    return json.loads(COMBOS.read_text())


@pytest.fixture(scope="session")
def baselines():
    """Load regression baselines."""
    if not BASELINES.exists():
        pytest.skip("Baselines not captured. Run scripts/capture_lamar_baselines.py --capture first.")
    return json.loads(BASELINES.read_text())


@pytest.fixture(scope="session")
def available_schemas(regression_db_readonly):
    """Get list of combo schemas that have a league_settings table in the fixture DB."""
    rows = regression_db_readonly.execute(
        """
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_schema LIKE 'combo_%'
          AND table_name = 'league_settings'
        ORDER BY table_schema
        """
    ).fetchall()
    return [r[0] for r in rows]
