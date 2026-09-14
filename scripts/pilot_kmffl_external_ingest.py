#!/usr/bin/env python3
"""KMFFL pilot for external data franchise merge — end-to-end validation.

Two-phase runner for the external-ingest-v1 pipeline (Task 20).

=== PHASE 1 (default) ===
Uploads 4 KMFFL parquets + 2 settings JSON files to the shared Fly staging
tables via the existing /api/import/upload-staging Next.js route (the same
code path the wizard uses). Requires `npm run dev` running on port 3000.

Then runs `multi_league.external_ingest.cli_scan` and asserts auto-resolution:

  • matchup 2013/2014 + player_fantasy 2014  → bound_via_guid
    (manager_guids are plumbed through from the Yahoo parquets)
  • draft 2014 + transactions 2014           → auto_suggested
    (name-bridge match via matchup; GUIDs not in those files)

Prints clear instructions for triggering the import via the web UI, then
exits 0.  Yahoo OAuth is hard to script non-interactively, so the import
step is intentionally an interactive handoff.

=== PHASE 2 (--post-import-verify) ===
Skips upload and scan.  Queries Fly for the final franchise count (expected:
10 distinct franchise_ids for KMFFL) and re-runs the scan to verify
idempotency — all resolutions should be bound_via_guid or auto_suggested,
zero unresolved, zero new clusters.

Usage:
    # Phase 1 — upload + scan + assert + print import instructions
    python scripts/pilot_kmffl_external_ingest.py \\
        --kmffl-dir /path/to/kmffl_import

    # Phase 2 — post-import verification (run after the import completes)
    python scripts/pilot_kmffl_external_ingest.py \\
        --kmffl-dir /path/to/kmffl_import \\
        --post-import-verify

    # Override defaults
    python scripts/pilot_kmffl_external_ingest.py \\
        --kmffl-dir /path/to/kmffl_import \\
        --db kmffl_pilot \\
        --frontend-url http://localhost:3000 \\
        --expected-franchises 10

Prerequisites (Phase 1):
    - npm run dev running on port 3000 (or --frontend-url set appropriately)
    - Fly env vars set: DATABASE_SERVER_URL, DATABASE_ADMIN_TOKEN, DATABASE_READ_TOKEN
      (loaded from project-root .env / frontend/.env.local automatically)
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

import requests

# ── Project-root bootstrap ────────────────────────────────────────────────────
ROOT = Path(__file__).resolve().parent.parent

# Force UTF-8 output on Windows (parquet/settings may have non-ASCII chars).
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

# Load env from project-root .env + frontend/.env.local (same pattern as
# other scripts: smoke_test_staging.py, league_settings_census.py).
for _env_file in [ROOT / ".env", ROOT / "frontend" / ".env.local"]:
    if _env_file.exists():
        for _line in _env_file.read_text().splitlines():
            _line = _line.strip()
            if _line and not _line.startswith("#") and "=" in _line:
                _k, _v = _line.split("=", 1)
                os.environ.setdefault(_k.strip(), _v.strip().strip('"').strip("'"))

FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

# ── Expected pilot files ──────────────────────────────────────────────────────
# Each entry: (filename, data_type, year)
PILOT_FILES: list[tuple[str, str, int]] = [
    ("matchup_data_week_all_year_2013.parquet", "matchup", 2013),
    ("matchup_data_week_all_year_2014.parquet", "matchup", 2014),
    ("draft_data_2014.parquet", "draft", 2014),
    ("transactions_year_2014.parquet", "transactions", 2014),
    ("yahoo_player_stats_2014_all_weeks.parquet", "player", 2014),
    ("league_settings_2013_449_l_198278.json", "settings", 2013),
    ("league_settings_2014_331_l_381581.json", "settings", 2014),
]

# ── Resolution state expectations ─────────────────────────────────────────────
# matchup + player_fantasy have manager_guids plumbed through → bound_via_guid.
# draft + transactions rely on name-bridge via matchup → auto_suggested.
EXPECTED_GUID_TABLES = {"matchup", "player_fantasy"}
ACCEPTED_STATES = {"bound_via_guid", "auto_suggested"}


# ─────────────────────────────────────────────────────────────────────────────
# helpers
# ─────────────────────────────────────────────────────────────────────────────


def _check_files(kmffl_dir: Path) -> None:
    """Assert all 7 pilot files are present under kmffl_dir."""
    missing = []
    for fname, _, _ in PILOT_FILES:
        p = kmffl_dir / fname
        if not p.exists():
            missing.append(str(p))
    if missing:
        print("[FAIL] Missing pilot files:")
        for m in missing:
            print(f"       {m}")
        sys.exit(1)
    print(f"[OK]   All {len(PILOT_FILES)} pilot files found under {kmffl_dir}")


def _build_external_data_files(kmffl_dir: Path) -> list[dict]:
    """Build the ExternalDataFile[] payload the upload-staging route expects.

    Shape mirrors ExternalDataFile in frontend/src/types/import.ts:
        { filename, data_type, year, row_count, data, columns? }
    """
    import pandas as pd

    files: list[dict] = []
    for fname, dtype, year in PILOT_FILES:
        path = kmffl_dir / fname
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
            # Coerce to JSON-safe dicts (dates, NaN, Timestamp, etc.).
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


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1: upload → scan → assert
# ─────────────────────────────────────────────────────────────────────────────


def phase1_upload(db_name: str, kmffl_dir: Path, frontend_url: str) -> bool:
    """Upload the 7 pilot files to staging via the Next.js route."""
    print("\n" + "=" * 60)
    print("PHASE 1 — Upload to staging via upload-staging route")
    print("=" * 60)

    # 1a. Confirm the dev server is reachable.
    try:
        r = requests.get(frontend_url, timeout=5)
    except Exception as exc:
        print(f"[FAIL] frontend not reachable at {frontend_url}: {exc}")
        print("       Run `cd frontend && npm run dev` and retry.")
        return False
    if r.status_code >= 500:
        print(f"[FAIL] frontend returned {r.status_code}")
        return False
    print(f"[OK]   frontend reachable ({r.status_code})")

    # 1b. Build the payload.
    print("[INFO] Parsing pilot files...")
    files = _build_external_data_files(kmffl_dir)
    total_rows = sum(f["row_count"] for f in files)
    print(f"[OK]   Parsed {len(files)} files, {total_rows} total rows:")
    for f in files:
        print(f"       • {f['filename']}: {f['data_type']} ({f['row_count']} rows, year={f['year']})")

    # 1c. POST to upload-staging.
    url = f"{frontend_url.rstrip('/')}/api/import/upload-staging"
    print(f"\n[INFO] POSTing to {url} ...")
    try:
        r = requests.post(
            url,
            json={"db_name": db_name, "files": files},
            timeout=300,  # parquet inserts can take a while
        )
    except Exception as exc:
        print(f"[FAIL] POST {url}: {exc}")
        return False

    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else r.text
    if r.status_code != 200 or (isinstance(body, dict) and not body.get("success")):
        print(f"[FAIL] upload-staging {r.status_code}: {body}")
        return False

    tables_summary = body.get("tables", []) if isinstance(body, dict) else []
    print(f"[OK]   upload-staging returned: {tables_summary}")
    return True


def phase1_scan(db_name: str) -> dict | None:
    """Run cli_scan and return the parsed JSON response."""
    print("\n" + "=" * 60)
    print("PHASE 1 — Run cli_scan")
    print("=" * 60)

    cmd = [
        sys.executable,
        "-m",
        "multi_league.external_ingest.cli_scan",
        "--db",
        db_name,
    ]
    print(f"[INFO] Running: {' '.join(cmd)}")
    print(f"       cwd: {FFS_DIR}")

    try:
        raw = subprocess.check_output(cmd, cwd=str(FFS_DIR), stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as exc:
        stderr = exc.stderr.decode(errors="replace") if exc.stderr else ""
        print(f"[FAIL] cli_scan exited {exc.returncode}: {stderr}")
        return None
    except Exception as exc:
        print(f"[FAIL] cli_scan failed: {exc}")
        return None

    try:
        scan = json.loads(raw)
    except json.JSONDecodeError as exc:
        print(f"[FAIL] cli_scan output is not valid JSON: {exc}")
        print(f"       raw output: {raw[:500]}")
        return None

    if "error" in scan:
        print(f"[FAIL] cli_scan returned error: {scan['error']}")
        return None

    resolutions = scan.get("identity_resolutions", [])
    tables = scan.get("tables", [])
    print(f"[OK]   cli_scan returned {len(resolutions)} resolutions, {len(tables)} tables")
    return scan


def phase1_assert_resolutions(scan: dict) -> bool:
    """Assert that all resolutions are bound_via_guid or auto_suggested."""
    print("\n" + "=" * 60)
    print("PHASE 1 — Assert auto-resolution")
    print("=" * 60)

    resolutions = scan.get("identity_resolutions", [])
    if not resolutions:
        print("[FAIL] No identity_resolutions in scan output")
        return False

    failures: list[str] = []
    for r in resolutions:
        kind = r.get("kind")
        if kind == "cluster":
            # Clusters are expected when a single franchise_id maps multiple names.
            print(f"[INFO] Cluster: franchise_id={r.get('franchise_id')} " f"managers={r.get('external_managers')}")
            continue

        manager = r.get("manager", "<unknown>")
        state = r.get("state", "<missing>")
        tables_hit = r.get("tables", [])
        years_hit = r.get("years", [])

        icon = "[OK]  " if state in ACCEPTED_STATES else "[FAIL]"
        print(f"{icon} {manager!r}: state={state}, tables={tables_hit}, years={years_hit}")

        if state not in ACCEPTED_STATES:
            failures.append(f"{manager!r} expected bound_via_guid or auto_suggested, got {state!r}")

    if failures:
        print(f"\n[FAIL] {len(failures)} resolution(s) did not meet expectation:")
        for f in failures:
            print(f"       • {f}")
        return False

    print(
        f"\n[OK]   All {len(resolutions)} resolutions are in accepted states " f"({', '.join(sorted(ACCEPTED_STATES))})"
    )
    return True


def print_import_instructions(db_name: str, frontend_url: str) -> None:
    """Print clear instructions for the user to trigger the import."""
    print("\n" + "=" * 60)
    print("NEXT STEP — Trigger the import via the web UI")
    print("=" * 60)
    print(f"""
The scan passed. To complete the pilot, trigger the KMFFL import via the web UI:

  1. Open {frontend_url} in your browser and log in.
  2. Navigate to the Yahoo import wizard for the KMFFL league.
  3. Confirm the external data is pre-loaded (it should show in the wizard).
  4. Trigger a Full Import.
  5. Wait for the import to complete (~20-40 min for 13 years of data).

After the import completes, run Phase 2 to verify the franchise count:

    python scripts/pilot_kmffl_external_ingest.py \\
        --kmffl-dir <same-dir-as-phase-1> \\
        --db {db_name} \\
        --post-import-verify

Phase 2 will:
  • Query Fly for COUNT(DISTINCT franchise_id) in {db_name}.public.matchup
    and assert it equals 10 (10 franchises × 13 years).
  • Re-run cli_scan and assert zero unresolved, zero new clusters
    (idempotency check).
""")


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2: post-import verification
# ─────────────────────────────────────────────────────────────────────────────


def phase2_franchise_count(db_name: str, expected_franchises: int) -> bool:
    """Query Fly for distinct franchise_ids in the imported matchup table."""
    print("\n" + "=" * 60)
    print("PHASE 2 — Verify final franchise count")
    print("=" * 60)

    try:
        from multi_league.core.db_reader import get_reader
    except ImportError as exc:
        print(f"[FAIL] Cannot import db_reader: {exc}")
        return False

    reader = get_reader()
    sql = f"SELECT COUNT(DISTINCT franchise_id) AS cnt " f'FROM "{db_name}".public.matchup'
    print(f"[INFO] Querying: {sql}")
    try:
        count = reader.query_scalar(sql, database="")
    except Exception as exc:
        print(f"[FAIL] Franchise count query failed: {exc}")
        print("       The import may not have completed yet. Check the import status in the UI.")
        return False

    count = int(count) if count is not None else 0
    if count == expected_franchises:
        print(f"[OK]   franchise_id count = {count} (expected {expected_franchises})")
        return True
    else:
        print(
            f"[FAIL] franchise_id count = {count}, expected {expected_franchises}\n"
            f"       If the import is still running, wait for it to complete and retry.\n"
            f"       If it has finished, check that external_column_maps and "
            f"external_identity_maps were forwarded correctly."
        )
        return False


def phase2_idempotency(db_name: str) -> bool:
    """Re-run cli_scan after import. Expect all resolutions are stable (no new unresolved)."""
    print("\n" + "=" * 60)
    print("PHASE 2 — Idempotency re-scan")
    print("=" * 60)

    scan = phase1_scan(db_name)
    if scan is None:
        return False

    resolutions = scan.get("identity_resolutions", [])
    unresolved = [r for r in resolutions if r.get("kind") == "single" and r.get("state") == "unresolved"]
    clusters = [r for r in resolutions if r.get("kind") == "cluster"]

    for r in resolutions:
        if r.get("kind") == "cluster":
            print(
                f"[INFO] Cluster (expected): franchise_id={r.get('franchise_id')} "
                f"managers={r.get('external_managers')}"
            )
            continue
        state = r.get("state", "<missing>")
        icon = "[OK]  " if state in ACCEPTED_STATES else "[FAIL]"
        print(f"{icon} {r.get('manager')!r}: state={state}")

    if unresolved:
        print(f"\n[FAIL] {len(unresolved)} still-unresolved manager(s) after import:")
        for r in unresolved:
            print(f"       • {r.get('manager')!r}")
        return False

    print(
        f"\n[OK]   Idempotency check passed — {len(resolutions)} resolutions, "
        f"0 unresolved, {len(clusters)} cluster(s)."
    )
    return True


# ─────────────────────────────────────────────────────────────────────────────
# main
# ─────────────────────────────────────────────────────────────────────────────


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--kmffl-dir",
        required=True,
        help="Directory containing the 7 KMFFL pilot files (4 parquets + 2 JSONs).",
    )
    ap.add_argument(
        "--db",
        default="kmffl_pilot",
        help="Database / league name to use for staging (default: kmffl_pilot).",
    )
    ap.add_argument(
        "--frontend-url",
        default="http://localhost:3000",
        help="Base URL of the Next.js dev server (default: http://localhost:3000).",
    )
    ap.add_argument(
        "--expected-franchises",
        type=int,
        default=10,
        help="Expected number of distinct franchise_ids after full import (default: 10).",
    )
    ap.add_argument(
        "--post-import-verify",
        action="store_true",
        help="Skip upload/scan and run post-import assertions only (Phase 2).",
    )
    args = ap.parse_args()

    kmffl_dir = Path(args.kmffl_dir).resolve()
    db_name = args.db
    frontend_url = args.frontend_url
    expected_franchises = args.expected_franchises

    # ── Validate input files exist ────────────────────────────────────────────
    _check_files(kmffl_dir)

    # ── Phase 2 only ──────────────────────────────────────────────────────────
    if args.post_import_verify:
        print("\n[INFO] Running Phase 2 — post-import verification")
        ok_count = phase2_franchise_count(db_name, expected_franchises)
        ok_idempotency = phase2_idempotency(db_name)

        print("\n" + "=" * 60)
        if ok_count and ok_idempotency:
            print("PILOT PASSED — franchise count correct and scan is idempotent")
        else:
            reasons = []
            if not ok_count:
                reasons.append(f"franchise count mismatch (expected {expected_franchises})")
            if not ok_idempotency:
                reasons.append("idempotency check failed (unresolved managers remain)")
            print("PILOT FAILED: " + "; ".join(reasons))
            sys.exit(1)
        print("=" * 60)
        return

    # ── Phase 1 (default) ─────────────────────────────────────────────────────
    print(f"\n[INFO] Running Phase 1 — upload + scan for db={db_name!r}")

    ok_upload = phase1_upload(db_name, kmffl_dir, frontend_url)
    if not ok_upload:
        print("\nPILOT FAILED: upload step did not succeed")
        sys.exit(1)

    scan = phase1_scan(db_name)
    if scan is None:
        print("\nPILOT FAILED: cli_scan did not return a valid result")
        sys.exit(1)

    ok_assert = phase1_assert_resolutions(scan)

    # Always print instructions (even on assertion failure, so the user knows
    # what to do next, and can come back with --post-import-verify after fixing).
    print_import_instructions(db_name, frontend_url)

    if not ok_assert:
        print("PILOT FAILED: resolution assertions did not pass (see above)")
        sys.exit(1)

    print("=" * 60)
    print("PHASE 1 PASSED — upload + scan + resolution check all succeeded")
    print("=" * 60)


if __name__ == "__main__":
    main()
