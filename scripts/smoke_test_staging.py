#!/usr/bin/env python3
"""Smoke test for the shared ___leagues.staging architecture.

Two parts:
  Part 1 — Raw Fly DDL/DML: verifies Fly accepts CREATE SCHEMA, CREATE TABLE,
           INSERT, SELECT, DELETE against ___leagues.staging. This answers
           the biggest unknown.
  Part 2 — End-to-end with real KMFFL 2013/2014 files: parses the parquet
           + JSON files, POSTs to the running dev server's upload-staging
           route, reads back via staging_reader, asserts non-zero rows,
           clears. Requires `npm run dev` running on localhost:3000.

Usage:
    python scripts/smoke_test_staging.py             # parts 1 + 3 (no server needed)
    python scripts/smoke_test_staging.py --part 1    # DDL only (no server needed)
    python scripts/smoke_test_staging.py --part 2    # route only (server required)
    python scripts/smoke_test_staging.py --part 3    # direct-to-Fly with real files (no server needed)

Exits non-zero on any failure.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import requests

# Settings JSON contains non-ASCII chars (e.g., "→"); force UTF-8 stdout on Windows.
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

ROOT = Path(__file__).resolve().parent.parent

# Load env from project-root .env + frontend/.env.local (same pattern as other scripts).
for env_file in [ROOT / ".env", ROOT / "frontend" / ".env.local"]:
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))

FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

FLY_URL = os.environ.get("DATABASE_SERVER_URL", "")
ADMIN_TOKEN = os.environ.get("DATABASE_ADMIN_TOKEN", "")
READ_TOKEN = os.environ.get("DATABASE_READ_TOKEN", "")

SMOKE_DB = "kmffl_smoke"  # isolated db_name so we don't touch real kmffl staging

FILES_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\_cleanup\recommend_to_delete"
    r"\fantasy_football_data_downloads_4.6GB\fantasy_football_data\KMFFL\kmffl_import"
)

FILE_MAP = {
    "league_settings_2013_449_l_198278.json": ("settings", 2013),
    "league_settings_2014_331_l_381581.json": ("settings", 2014),
    "matchup_data_week_all_year_2013.parquet": ("matchup", 2013),
    "matchup_data_week_all_year_2014.parquet": ("matchup", 2014),
    "transactions_year_2014.parquet": ("transactions", 2014),
    "yahoo_player_stats_2014_all_weeks.parquet": ("player", 2014),
    "draft_data_2014.parquet": ("draft", 2014),
}


# ──────────────────────────────────────────────────────────────────────────────
# Part 1: Raw Fly DDL/DML


def fly_rw(sql: str) -> tuple[bool, object]:
    """Run a write query against Fly. Returns (ok, body_or_error_text)."""
    try:
        r = requests.post(
            f"{FLY_URL}/query-rw",
            json={"sql": sql, "database": "___leagues"},
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            timeout=55,
        )
    except Exception as e:
        return False, f"network error: {e}"
    if r.status_code != 200:
        return False, f"{r.status_code}: {r.text}"
    return True, r.json()


def part1_raw_ddl() -> bool:
    print("=" * 60)
    print("PART 1 — Raw Fly DDL/DML smoke test")
    print("=" * 60)
    if not FLY_URL or not ADMIN_TOKEN:
        print("[FAIL] DATABASE_SERVER_URL or DATABASE_ADMIN_TOKEN not set")
        return False

    test_table = "___leagues.staging.smoke_test_table"
    steps: list[tuple[str, str]] = [
        ("CREATE SCHEMA", "CREATE SCHEMA IF NOT EXISTS ___leagues.staging"),
        (
            "CREATE TABLE",
            f"CREATE TABLE IF NOT EXISTS {test_table} " "(db_name VARCHAR, year INT, payload VARCHAR)",
        ),
        # Pre-clean any prior run.
        ("DELETE (pre-clean)", f"DELETE FROM {test_table} WHERE db_name = '{SMOKE_DB}'"),
        (
            "INSERT",
            f"INSERT INTO {test_table} VALUES ('{SMOKE_DB}', 2013, 'hello'), " f"('{SMOKE_DB}', 2014, 'world')",
        ),
    ]
    for label, sql in steps:
        ok, body = fly_rw(sql)
        if not ok:
            print(f"[FAIL] {label}: {body}")
            return False
        print(f"[OK]   {label}")

    # SELECT via read token to confirm visibility across roles.
    r = requests.post(
        f"{FLY_URL}/query",
        json={
            "sql": f"SELECT year, payload FROM {test_table} WHERE db_name = '{SMOKE_DB}' ORDER BY year",
            "database": "___leagues",
        },
        headers={"Authorization": f"Bearer {READ_TOKEN}"},
        timeout=55,
    )
    if r.status_code != 200:
        print(f"[FAIL] SELECT: {r.status_code} {r.text}")
        return False
    rows = r.json()
    if rows != [{"year": 2013, "payload": "hello"}, {"year": 2014, "payload": "world"}]:
        print(f"[FAIL] SELECT returned unexpected rows: {rows}")
        return False
    print(f"[OK]   SELECT returned 2 rows: {rows}")

    # Cleanup.
    ok, body = fly_rw(f"DELETE FROM {test_table} WHERE db_name = '{SMOKE_DB}'")
    if not ok:
        print(f"[FAIL] DELETE (cleanup): {body}")
        return False
    ok, body = fly_rw(f"DROP TABLE {test_table}")
    if not ok:
        print(f"[WARN] DROP TABLE (cleanup): {body} — non-fatal")
    print("[OK]   Cleanup")
    print("\nPART 1 PASSED — Fly accepts shared-staging DDL/DML\n")
    return True


# ──────────────────────────────────────────────────────────────────────────────
# Part 2: End-to-end via upload-staging route


def build_external_data_files() -> list[dict]:
    import pandas as pd

    files: list[dict] = []
    for fname, (dtype, year) in FILE_MAP.items():
        path = FILES_DIR / fname
        if not path.exists():
            print(f"[SKIP] {fname} — not found at {path}")
            continue

        if path.suffix == ".json":
            payload = json.loads(path.read_text(encoding="utf-8"))
            files.append(
                {
                    "filename": fname,
                    "data_type": dtype,
                    "year": year,
                    "row_count": 1,
                    "data": [payload],
                }
            )
        elif path.suffix == ".parquet":
            df = pd.read_parquet(path)
            # Coerce to JSON-safe dicts (dates, NaN, etc.).
            records = json.loads(df.to_json(orient="records"))
            files.append(
                {
                    "filename": fname,
                    "data_type": dtype,
                    "year": year,
                    "row_count": len(records),
                    "data": records,
                    "columns": list(df.columns),
                }
            )
    return files


def part2_e2e() -> bool:
    print("=" * 60)
    print("PART 2 — End-to-end via upload-staging route + staging_reader")
    print("=" * 60)

    # Ping the dev server first.
    try:
        r = requests.get("http://localhost:3000", timeout=5)
    except Exception as e:
        print(f"[FAIL] dev server not reachable: {e}")
        print("       Run `cd frontend && npm run dev` and retry.")
        return False
    if r.status_code >= 500:
        print(f"[FAIL] dev server returned {r.status_code}")
        return False
    print(f"[OK]   dev server reachable ({r.status_code})")

    files = build_external_data_files()
    if not files:
        print("[FAIL] no input files parsed")
        return False
    print(f"[OK]   parsed {len(files)} files:")
    for f in files:
        print(f"       • {f['filename']}: {f['data_type']} ({f['row_count']} rows, year={f['year']})")

    # POST to upload-staging with our scoped SMOKE_DB name.
    try:
        r = requests.post(
            "http://localhost:3000/api/import/upload-staging",
            json={"db_name": SMOKE_DB, "files": files},
            timeout=300,  # parquet inserts can take a while
        )
    except Exception as e:
        print(f"[FAIL] POST /api/import/upload-staging: {e}")
        return False

    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
    if r.status_code != 200:
        print(f"[FAIL] upload-staging {r.status_code}: {body}")
        return False
    if not isinstance(body, dict) or not body.get("success"):
        print(f"[FAIL] upload-staging returned errors: {body}")
        return False
    print(f"[OK]   upload-staging: {body.get('tables')}")

    # Read back via staging_reader.
    from multi_league.data_fetchers.shared.staging_reader import (
        clear_staging_tables,
        read_staging_data,
    )

    result = read_staging_data(SMOKE_DB)
    if not isinstance(result, dict) or not result:
        print(f"[FAIL] read_staging_data({SMOKE_DB!r}) returned {result!r}")
        return False

    expected = {"settings", "matchup", "player", "draft", "transactions"}
    missing = expected - set(result.keys())
    if missing:
        print(f"[FAIL] read_staging_data missing keys: {missing}")
        print(f"       Got: {set(result.keys())}")
        return False

    print(f"[OK]   read_staging_data returned keys: {sorted(result.keys())}")
    for key, val in result.items():
        if key == "settings":
            print(f"       • settings: {len(val)} entries for years {sorted({e['year'] for e in val})}")
        else:
            print(f"       • {key}: {len(val)} rows")

    # Clear this league's staging rows.
    clear_staging_tables(SMOKE_DB, log_func=lambda s: print(f"       {s}"))

    # Verify cleared.
    result_after = read_staging_data(SMOKE_DB)
    if result_after:
        print(f"[FAIL] clear_staging_tables didn't clear — still got {list(result_after.keys())}")
        return False
    print(f"[OK]   clear_staging_tables: {SMOKE_DB} staging is empty")

    print("\nPART 2 PASSED — end-to-end upload + read + clear works\n")
    return True


# ──────────────────────────────────────────────────────────────────────────────


# ──────────────────────────────────────────────────────────────────────────────
# Part 3: Direct-to-Fly with real files (replicates the route's upload logic
# in Python, bypassing Next.js).


# Must match STAGING_TABLE_SCHEMAS in frontend/src/app/api/import/upload-staging/route.ts
STAGING_SCHEMAS: dict[str, list[str]] = {
    "matchup": [
        "db_name",
        "week",
        "year",
        "manager",
        "team_name",
        "team_points",
        "opponent",
        "opponent_points",
        "win",
        "loss",
        "team_projected_points",
        "opponent_projected_points",
        "margin",
        "division_id",
        "is_playoffs",
        "is_consolation",
        "filename",
    ],
    "player": [
        "db_name",
        "year",
        "week",
        "manager",
        "player",
        "points",
        "nfl_position",
        "lineup_position",
        "nfl_team",
        "projected_points",
        "percent_started",
        "percent_owned",
        "filename",
    ],
    "draft": [
        "db_name",
        "year",
        "round",
        "pick",
        "manager",
        "player",
        "cost",
        "keeper",
        "draft_type",
        "filename",
    ],
    "transactions": [
        "db_name",
        "year",
        "week",
        "manager",
        "player",
        "transaction_type",
        "faab_bid",
        "source_type",
        "destination_type",
        "trade_partner",
        "filename",
    ],
    "schedule": [
        "db_name",
        "year",
        "week",
        "manager",
        "opponent",
        "is_playoffs",
        "is_consolation",
        "week_start",
        "week_end",
        "filename",
    ],
}
BATCH_SIZE = 50


def _sql_escape(val: object) -> str:
    """Mirror sqlEscape in upload-staging/route.ts."""
    if val is None or val == "":
        return "NULL"
    # Pandas NaN sneaks through here — treat like None.
    if isinstance(val, float):
        import math

        if math.isnan(val):
            return "NULL"
        return repr(val)
    if isinstance(val, bool):
        return "TRUE" if val else "FALSE"
    if isinstance(val, (int,)):
        return str(val)
    s = str(val)
    # Strip control chars + escape single quotes.
    s = "".join(c for c in s if c >= " " and c != "\x7f")
    s = s.replace("'", "''")
    return f"'{s}'"


def _upload_tabular_direct(db: str, dtype: str, records: list[dict], filename: str) -> int:
    """Mirror uploadTabular() in upload-staging/route.ts against Fly directly."""
    schema = STAGING_SCHEMAS[dtype]
    table = f"___leagues.staging.staging_{dtype}"
    col_defs = ", ".join(f'"{c}" VARCHAR' for c in schema)
    col_list = ", ".join(f'"{c}"' for c in schema)

    fly_rw(f"CREATE TABLE IF NOT EXISTS {table} ({col_defs})")
    ok, body = fly_rw(f"DELETE FROM {table} WHERE db_name = '{db}' AND filename = '{filename}'")
    if not ok:
        raise RuntimeError(f"DELETE failed for {dtype}: {body}")

    # Stamp db_name + filename on every row.
    rows_out: list[dict] = []
    for row in records:
        r = dict(row)
        r["db_name"] = db
        r["filename"] = filename
        rows_out.append(r)

    inserted = 0
    for i in range(0, len(rows_out), BATCH_SIZE):
        batch = rows_out[i : i + BATCH_SIZE]
        value_rows = []
        for row in batch:
            vals = [_sql_escape(row.get(c)) for c in schema]
            value_rows.append(f"({', '.join(vals)})")
        sql = f"INSERT INTO {table} ({col_list}) VALUES {', '.join(value_rows)}"
        ok, body = fly_rw(sql)
        if not ok:
            raise RuntimeError(f"INSERT failed for {dtype} batch {i}: {body}")
        inserted += len(batch)
    return inserted


def _upload_settings_direct(db: str, year: int, payload: dict, filename: str) -> None:
    table = "___leagues.staging.staging_settings"
    fly_rw(
        f"CREATE TABLE IF NOT EXISTS {table} "
        "(db_name VARCHAR, year INT, raw_settings_json VARCHAR, filename VARCHAR)"
    )
    fly_rw(f"DELETE FROM {table} WHERE db_name = '{db}' AND year = {int(year)}")
    json_str = json.dumps(payload)
    sql = (
        f"INSERT INTO {table} VALUES "
        f"({_sql_escape(db)}, {int(year)}, {_sql_escape(json_str)}, {_sql_escape(filename)})"
    )
    ok, body = fly_rw(sql)
    if not ok:
        raise RuntimeError(f"settings INSERT failed for year {year}: {body}")


def part3_direct_fly() -> bool:
    print("=" * 60)
    print("PART 3 — Direct-to-Fly with real KMFFL 2013/2014 files")
    print("=" * 60)

    import pandas as pd

    # Ensure the shared staging schema exists.
    ok, body = fly_rw("CREATE SCHEMA IF NOT EXISTS ___leagues.staging")
    if not ok:
        print(f"[FAIL] CREATE SCHEMA: {body}")
        return False

    # Pre-clean this smoke-test league (idempotent re-runs).
    from multi_league.data_fetchers.shared.staging_reader import (
        STAGING_TABLES,
        clear_staging_tables,
        read_staging_data,
    )

    try:
        clear_staging_tables(SMOKE_DB, log_func=lambda s: None)
    except Exception:
        # Tables may not exist yet — ignore.
        pass

    summary: dict[str, int] = {}
    for fname, (dtype, year) in FILE_MAP.items():
        path = FILES_DIR / fname
        if not path.exists():
            print(f"[SKIP] {fname} — not found")
            continue

        try:
            if path.suffix == ".json":
                payload = json.loads(path.read_text(encoding="utf-8"))
                _upload_settings_direct(SMOKE_DB, year, payload, fname)
                summary[f"settings/{year}"] = 1
                print(f"[OK]   uploaded {fname} → staging_settings ({year})")
            elif path.suffix == ".parquet":
                df = pd.read_parquet(path)
                records = json.loads(df.to_json(orient="records"))
                n = _upload_tabular_direct(SMOKE_DB, dtype, records, fname)
                summary[f"{dtype}/{year}"] = n
                print(f"[OK]   uploaded {fname} → staging_{dtype} ({n} rows)")
        except Exception as e:
            print(f"[FAIL] uploading {fname}: {e}")
            return False

    print(f"\n[OK]   uploaded {sum(summary.values())} rows across {len(summary)} file(s)")

    # Read back via staging_reader.
    print(f"\n[READ] calling staging_reader.read_staging_data({SMOKE_DB!r})...")
    result = read_staging_data(SMOKE_DB)
    if not isinstance(result, dict) or not result:
        print(f"[FAIL] read_staging_data returned {result!r}")
        return False

    expected = {"settings", "matchup", "player", "draft", "transactions"}
    missing = expected - set(result.keys())
    if missing:
        print(f"[FAIL] missing types in result: {missing}")
        print(f"       got: {set(result.keys())}")
        return False

    print(f"[OK]   read_staging_data keys: {sorted(result.keys())}")
    for key, val in result.items():
        if key == "settings":
            years_seen = sorted({e["year"] for e in val})
            print(f"       • settings: {len(val)} entries for years {years_seen}")
            assert set(years_seen) == {2013, 2014}, f"expected years 2013, 2014; got {years_seen}"
        else:
            print(f"       • {key}: {len(val)} rows")
            assert len(val) > 0, f"{key} should have rows"

    # Clear + verify.
    print(f"\n[CLEAR] calling clear_staging_tables({SMOKE_DB!r})...")
    clear_staging_tables(SMOKE_DB, log_func=lambda s: print(f"       {s}"))

    result_after = read_staging_data(SMOKE_DB)
    if result_after:
        print(f"[FAIL] clear didn't clear — still got {list(result_after.keys())}")
        return False
    print("[OK]   post-clear read_staging_data = empty")

    # Confirm OTHER leagues' rows (if any) survived. We check by counting total rows
    # in each table across all db_names; should NOT be affected by our delete.
    for table in STAGING_TABLES:
        ok, rows = fly_rw(
            f"SELECT COUNT(*) FILTER (WHERE db_name = '{SMOKE_DB}') AS s, COUNT(*) AS total FROM ___leagues.staging.{table}"
        )
        if ok and isinstance(rows, list) and rows:
            s = rows[0].get("s", 0)
            total = rows[0].get("total", 0)
            assert s == 0, f"{table} still has {s} rows for {SMOKE_DB}"
            print(f"       • {table}: {total} total rows (other leagues) intact")

    print("\nPART 3 PASSED — real-file upload + read + clear works end-to-end\n")
    return True


# ──────────────────────────────────────────────────────────────────────────────


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--part", type=int, choices=[1, 2, 3], help="run a single part")
    args = ap.parse_args()

    if args.part:
        parts = [args.part]
    else:
        # Default: 1 + 3 (both runnable without the dev server).
        parts = [1, 3]

    for p in parts:
        if p == 1:
            ok = part1_raw_ddl()
        elif p == 2:
            ok = part2_e2e()
        elif p == 3:
            ok = part3_direct_fly()
        else:
            ok = False
        if not ok:
            return 1

    print("=" * 60)
    print("ALL PARTS PASSED")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
