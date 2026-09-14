"""Local reproduction of PHASE 1.7 against real Fly state.

Pulls KMFFL's actual rows from Fly:
  - ___leagues.staging.staging_*  (the user's uploaded 2013/2014 data)
  - ___leagues.public.*            (the canonical Yahoo OAuth 2025 data)

Inserts them into a local in-memory DuckDB, runs schema_conform.run, and
reports any aborts or successful conformance.

Iterates locally in seconds — no GitHub Actions roundtrip.

Usage:
  python scripts/local_repro_kmffl.py                 # baseline run
  python scripts/local_repro_kmffl.py --idempotent    # run twice, compare
  python scripts/local_repro_kmffl.py --variation alias_columns
  python scripts/local_repro_kmffl.py --variation drop_manager_guid
  python scripts/local_repro_kmffl.py --variation null_playoff_scores

Variations mutate the staging data after pull but before schema_conform runs,
so we can probe edge cases without touching Fly.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from types import SimpleNamespace

# UTF-8 stdout for Windows
os.environ.setdefault("PYTHONIOENCODING", "utf-8")
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

from dotenv import load_dotenv

load_dotenv(ROOT / ".env")

import duckdb
import pandas as pd

from multi_league.core.readers.fly_reader import FlyReader
from multi_league.external_ingest.schema_conform import (
    SchemaConformAbort,
    run as schema_conform_run,
)

DB_NAME = "kmffl"
STAGING_DB = "___leagues"

# Local-table-name -> Fly-staging-suffix (matches _fly_bridge.LOCAL_TO_FLY_TABLE)
TABLES = {
    "matchup": "matchup",
    "player_fantasy": "player",
    "draft": "draft",
    "transactions": "transactions",
}


def banner(msg: str) -> None:
    print("=" * 80)
    print(msg)
    print("=" * 80)


def pull_fly_to_local(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Pull the actual production state from Fly into local DuckDB.

    Mirrors what would be in the local conn at PHASE 1.7 time:
      - staging.staging_<local>      from Fly's staging.staging_<fly>
      - public.<local>               from Fly's public.<table>  (canonical)
    """
    reader = FlyReader()

    conn.execute("CREATE SCHEMA IF NOT EXISTS staging")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")

    counts: dict[str, int] = {}

    # 1. Pull staging.staging_* (the user's uploaded data)
    existing = {
        r["table_name"]
        for r in reader.query(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema='staging' AND table_catalog='{STAGING_DB}'",
            database=STAGING_DB,
        )
    }
    for local, fly in TABLES.items():
        fly_table = f"staging_{fly}"
        if fly_table not in existing:
            counts[f"staging_{local}"] = 0
            continue
        df = reader.query_df(
            f"SELECT * FROM {STAGING_DB}.staging.{fly_table} WHERE db_name='{DB_NAME}'",
            database=STAGING_DB,
        )
        if df.empty:
            counts[f"staging_{local}"] = 0
            continue
        conn.register("v", df)
        conn.execute(f"DROP TABLE IF EXISTS staging.staging_{local}")
        conn.execute(f"CREATE TABLE staging.staging_{local} AS SELECT * FROM v")
        conn.unregister("v")
        counts[f"staging_{local}"] = len(df)

    # 2. Pull public.<table> for kmffl (canonical reference)
    public_existing = {
        r["table_name"]
        for r in reader.query(
            "SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema='public' AND table_catalog='{STAGING_DB}'",
            database=STAGING_DB,
        )
    }
    for local in TABLES.keys():
        if local not in public_existing:
            counts[f"public_{local}"] = 0
            continue
        try:
            df = reader.query_df(
                f"SELECT * FROM {STAGING_DB}.public.{local} WHERE db_name='{DB_NAME}'",
                database=STAGING_DB,
            )
        except Exception as e:
            print(f"  [warn] public.{local} pull failed: {e}")
            counts[f"public_{local}"] = 0
            continue
        if df.empty:
            counts[f"public_{local}"] = 0
            continue
        conn.register("v", df)
        conn.execute(f"DROP TABLE IF EXISTS public.{local}")
        conn.execute(f"CREATE TABLE public.{local} AS SELECT * FROM v")
        conn.unregister("v")
        counts[f"public_{local}"] = len(df)

    return counts


def apply_variation(conn: duckdb.DuckDBPyConnection, variation: str | None) -> None:
    """Mutate staging tables in-place to probe edge cases."""
    if not variation:
        return
    print(f"\n[VARIATION] applying: {variation}")
    if variation == "alias_columns":
        # Rename manager_guid -> mgr_id, manager -> mgr in matchup
        for local in ["matchup", "draft", "transactions"]:
            cols = [
                c[0]
                for c in conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema='staging' AND table_name='staging_{local}'"
                ).fetchall()
            ]
            renames = []
            if "manager_guid" in cols:
                renames.append(("manager_guid", "mgr_id"))
            if "manager" in cols:
                renames.append(("manager", "mgr"))
            for old, new in renames:
                conn.execute(f'ALTER TABLE staging.staging_{local} RENAME COLUMN "{old}" TO "{new}"')
                print(f"  staging_{local}: {old} -> {new}")
    elif variation == "drop_manager_guid":
        for local in ["matchup", "draft", "transactions", "player_fantasy"]:
            cols = [
                c[0]
                for c in conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema='staging' AND table_name='staging_{local}'"
                ).fetchall()
            ]
            if "manager_guid" in cols:
                conn.execute(f'ALTER TABLE staging.staging_{local} DROP COLUMN "manager_guid"')
                print(f"  dropped manager_guid from staging_{local}")
    elif variation == "null_playoff_scores":
        # Simulate 2013-style data with missing playoff points
        cols = [
            c[0]
            for c in conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='staging' AND table_name='staging_matchup'"
            ).fetchall()
        ]
        if "team_points" in cols and "week" in cols:
            conn.execute("UPDATE staging.staging_matchup SET team_points = NULL WHERE CAST(week AS INTEGER) >= 14")
            n = conn.execute("SELECT count(*) FROM staging.staging_matchup WHERE team_points IS NULL").fetchone()[0]
            print(f"  NULLed team_points for {n} playoff-week rows")
    elif variation == "whitespace_managers":
        # Whitespace + mixed case + suffix variants (Adin → "  Adin K. ", "ADIN")
        for local in ["matchup", "draft", "transactions"]:
            try:
                conn.execute(
                    f"UPDATE staging.staging_{local} "
                    f"SET manager = '  ' || UPPER(manager) || ' Jr.' "
                    f"WHERE manager IS NOT NULL"
                )
                print(f"  injected whitespace+upper+suffix into {local}.manager")
            except Exception as e:
                print(f"  {local}: {e}")
    elif variation == "unicode_managers":
        # Inject accented chars + emoji
        for local in ["matchup", "draft", "transactions"]:
            try:
                conn.execute(f"UPDATE staging.staging_{local} SET manager = 'Adín 🏆' WHERE manager = 'Adin'")
                conn.execute(f"UPDATE staging.staging_{local} SET manager = 'José' WHERE manager = 'Marc'")
                print(f"  unicode-injected into {local}.manager")
            except Exception as e:
                print(f"  {local}: {e}")
    elif variation == "drop_yahoo_player_id":
        # REQUIRED IDENTITY for draft/transactions/player_fantasy with no derivation
        for local in ["draft", "transactions", "player_fantasy"]:
            cols = [
                c[0]
                for c in conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema='staging' AND table_name='staging_{local}'"
                ).fetchall()
            ]
            if "yahoo_player_id" in cols:
                conn.execute(f'ALTER TABLE staging.staging_{local} DROP COLUMN "yahoo_player_id"')
                print(f"  dropped yahoo_player_id from staging_{local}")
    elif variation == "single_table":
        # Drop everything except matchup — common user scenario
        for local in ["draft", "transactions", "player_fantasy"]:
            try:
                conn.execute(f"DROP TABLE staging.staging_{local}")
                print(f"  dropped staging_{local}")
            except Exception:
                pass
    elif variation == "extra_aliases":
        # Aggressive renaming across the board
        renames_per_table = {
            "matchup": [("manager", "owner"), ("team_points", "tp"), ("opponent_points", "opp_pts")],
            "draft": [("manager", "owner"), ("yahoo_player_id", "pid"), ("cost", "bid")],
            "transactions": [("manager", "owner"), ("yahoo_player_id", "pid"), ("transaction_type", "txn_type")],
        }
        for local, renames in renames_per_table.items():
            cols = [
                c[0]
                for c in conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema='staging' AND table_name='staging_{local}'"
                ).fetchall()
            ]
            for old, new in renames:
                if old in cols:
                    conn.execute(f'ALTER TABLE staging.staging_{local} RENAME COLUMN "{old}" TO "{new}"')
                    print(f"  staging_{local}: {old} -> {new}")
    elif variation == "mixed_case_guids":
        # Lowercase manager_guid values (Yahoo regex requires uppercase 26-char base32)
        for local in ["matchup", "player_fantasy"]:
            cols = [
                c[0]
                for c in conn.execute(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_schema='staging' AND table_name='staging_{local}'"
                ).fetchall()
            ]
            if "manager_guid" in cols:
                conn.execute(f"UPDATE staging.staging_{local} SET manager_guid = LOWER(manager_guid)")
                print(f"  lowered manager_guid values in staging_{local}")
    else:
        print(f"  [warn] unknown variation '{variation}' — running unmodified")


def report_state(conn: duckdb.DuckDBPyConnection) -> None:
    """Print row counts + sample manager_guid distribution after schema_conform."""
    print("\n--- conformed tables ---")
    for local in TABLES.keys():
        try:
            n = conn.execute(f"SELECT count(*) FROM staging.conformed_{local} WHERE db_name='{DB_NAME}'").fetchone()[0]
            print(f"  conformed_{local}: {n} rows")
        except Exception:
            print(f"  conformed_{local}: (not written)")

    # manager_guid distribution per table
    print("\n--- manager_guid distribution ---")
    for local in TABLES.keys():
        try:
            df = conn.execute(
                f"SELECT manager, manager_guid, count(*) as n "
                f"FROM staging.conformed_{local} WHERE db_name='{DB_NAME}' "
                f"GROUP BY 1,2 ORDER BY 1"
            ).df()
            if df.empty:
                continue
            print(f"\n  conformed_{local}:")
            for _, r in df.iterrows():
                guid = r["manager_guid"]
                tag = "external" if guid and str(guid).startswith("external_") else "canonical" if guid else "NULL"
                print(f"    {r['manager']!r:30s} -> {str(guid)[:30]:30s} [{tag}] (n={r['n']})")
        except Exception as e:
            print(f"  conformed_{local}: error querying — {e}")

    # Ledger summary
    print("\n--- ledger summary ---")
    try:
        df = conn.execute(
            "SELECT table_name, status, count(*) as n "
            "FROM staging.schema_decisions WHERE db_name='kmffl' "
            "GROUP BY 1,2 ORDER BY 1,2"
        ).df()
        for _, r in df.iterrows():
            print(f"  {r['table_name']:18s} {r['status']:30s} n={r['n']}")
    except Exception as e:
        print(f"  (no ledger entries — {e})")


def gavi_check(conn: duckdb.DuckDBPyConnection) -> None:
    """Verify Gavi's 2013 rows preserve identity (proxy for HOF correctness)."""
    print("\n--- Gavi 2013 sanity check ---")
    try:
        df = conn.execute(
            "SELECT manager, manager_guid, week, team_points, opponent_points "
            "FROM staging.conformed_matchup "
            "WHERE db_name='kmffl' AND CAST(year AS INTEGER) = 2013 "
            "AND lower(manager) LIKE '%gavi%' "
            "ORDER BY CAST(week AS INTEGER)"
        ).df()
        if df.empty:
            print("  (no 2013 Gavi rows in conformed_matchup)")
            return
        print(f"  found {len(df)} Gavi 2013 rows")
        for _, r in df.iterrows():
            tp = r["team_points"]
            op = r["opponent_points"]
            print(f"    week={r['week']}  guid={str(r['manager_guid'])[:26]}  " f"pts={tp}  opp_pts={op}")
    except Exception as e:
        print(f"  query failed: {e}")


def diff_two_runs(rows1: pd.DataFrame, rows2: pd.DataFrame) -> bool:
    """Compare two conformed snapshots, return True if identical (modulo internal cols)."""
    a = rows1.drop(columns=["db_name"], errors="ignore").reset_index(drop=True)
    b = rows2.drop(columns=["db_name"], errors="ignore").reset_index(drop=True)
    try:
        pd.testing.assert_frame_equal(a, b)
        return True
    except AssertionError as e:
        print(f"  [diff] {e!s}")
        return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--variation",
        choices=[
            "alias_columns",
            "drop_manager_guid",
            "null_playoff_scores",
            "whitespace_managers",
            "unicode_managers",
            "drop_yahoo_player_id",
            "single_table",
            "extra_aliases",
            "mixed_case_guids",
        ],
        default=None,
    )
    ap.add_argument("--idempotent", action="store_true", help="run twice, diff conformed output")
    args = ap.parse_args()

    banner(f"LOCAL REPRO — KMFFL schema_conform   variation={args.variation or 'none'}")

    conn = duckdb.connect(":memory:")

    print("\n[STEP 1] Pulling Fly state into local DuckDB...")
    counts = pull_fly_to_local(conn)
    for k, v in sorted(counts.items()):
        print(f"  {k:30s} {v:6,d} rows")

    apply_variation(conn, args.variation)

    print("\n[STEP 2] Running schema_conform.run()...")
    try:
        schema_conform_run(conn=conn, db_name=DB_NAME, run_id="local-r1")
        print("  [OK] no SchemaConformAbort raised")
    except SchemaConformAbort as e:
        print(f"  [FAIL] SchemaConformAbort: {len(e.failures)} failure(s)")
        for f in e.failures:
            print(f"    - table={f.table} gate={f.gate.name} slot={f.ddl_slot}")
            print(f"      detail={f.suggested_action}")
            if f.source_cols_considered:
                print(f"      source_cols={f.source_cols_considered}")
            if f.invariant_failures:
                print(f"      invariants={f.invariant_failures}")
            if f.sample_source_values:
                for col, vals in f.sample_source_values.items():
                    print(f"      sample[{col}]={vals[:5]}")
        return 1

    report_state(conn)
    gavi_check(conn)

    if args.idempotent:
        print("\n[STEP 3] Idempotency check — running schema_conform.run() AGAIN...")
        # snapshot current state
        before = {}
        for local in TABLES.keys():
            try:
                before[local] = conn.execute(
                    f"SELECT * FROM staging.conformed_{local} WHERE db_name='{DB_NAME}' " "ORDER BY 1, 2, 3"
                ).df()
            except Exception:
                before[local] = pd.DataFrame()

        try:
            schema_conform_run(conn=conn, db_name=DB_NAME, run_id="local-r2")
        except SchemaConformAbort as e:
            print(f"  [FAIL] second run aborted: {e}")
            return 1

        all_match = True
        for local in TABLES.keys():
            try:
                after = conn.execute(
                    f"SELECT * FROM staging.conformed_{local} WHERE db_name='{DB_NAME}' " "ORDER BY 1, 2, 3"
                ).df()
            except Exception:
                after = pd.DataFrame()
            same = diff_two_runs(before[local], after)
            print(f"  conformed_{local}: {'identical' if same else 'DIFFERS'}")
            all_match = all_match and same
        if not all_match:
            return 1

    print("\n=== DONE ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
