"""
build_ops_nfl_and_replace.py -- build the standalone ``___ops_nfl.duckdb`` (the promoted tables)
and swap it onto Fly via /replace-db. This is the fast-promote path from the ___ops split
(docs/runbooks/ops-nfl-split-migration.md); it supersedes deploy_v26_supertable_to_fly.py's
/merge-ops transport once the split lands.

  *** WRITES TO PRODUCTION when --apply is given. ***
  Requires: (1) the duckdb-server to attach & allow db '___ops_nfl', and
            (2) FlyTarget.replace_database's allow-list to include '___ops_nfl' (or use --apply here,
                which posts /replace-db directly). Until both land, use --build-only.

Modes:
  (default)      dry-run: preflight (0-dropped-column guard) + show the build plan. No file, no write.
  --build-only   build the local ___ops_nfl.duckdb only (initial bootstrap / manual volume placement).
  --apply --i-understand-this-writes-prod
                 build + /replace-db  (the ongoing ~seconds swap).

    python scripts/build_ops_nfl_and_replace.py                 # dry-run plan
    python scripts/build_ops_nfl_and_replace.py --build-only    # produce the .duckdb for bootstrap
    python scripts/build_ops_nfl_and_replace.py --apply --i-understand-this-writes-prod
"""
from __future__ import annotations
import argparse
import hashlib
import sys
from datetime import datetime, UTC
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

# Keep these in sync with deploy_v26_supertable_to_fly.py until that script is retired.
V26_DIR = Path("D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables")
ART = V26_DIR / "season_career_v26"
OPS_SCHEMA = "nfl_historical"          # target schema INSIDE ___ops_nfl.duckdb (views point here)
DB_NAME = "___ops_nfl"

YDS_ALLOWED_BUCKETS: tuple[tuple[str, int, int | None], ...] = (
    ("yds_allow_0_99", 0, 99),
    ("yds_allow_100_199", 100, 199),
    ("yds_allow_200_299", 200, 299),
    ("yds_allow_300_349", 300, 349),
    ("yds_allow_350_399", 350, 399),
    ("yds_allow_400_449", 400, 449),
    ("yds_allow_450_499", 450, 499),
    ("yds_allow_500_549", 500, 549),
    ("yds_allow_550_plus", 550, None),
)

# table -> local source parquet (the promoted tables that move into ___ops_nfl)
SOURCES: dict[str, Path] = {
    "nfl_player_stats_all": V26_DIR / "nfl_player_stats_all.parquet",
    "player_nfl_season": ART / "player_nfl_season.parquet",
    "player_nfl_season_all": ART / "player_nfl_season_all.parquet",
    "player_nfl_career": ART / "player_nfl_career.parquet",
    "player_nfl_career_all": ART / "player_nfl_career_all.parquet",
    "player_bio": Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"),
    "player_nfl_season_team": ART / "player_nfl_season_team.parquet",
    "player_nfl_season_team_all": ART / "player_nfl_season_team_all.parquet",
}
TS = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
STAGE_ROOT = Path("D:/league-history-data/nfl/tmp/ops_nfl_builds")
STAGE_PATH = STAGE_ROOT / f"ops_nfl_{TS}.duckdb"


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    import os
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        os.environ.setdefault(k.strip(), v.strip().strip('"'))


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def yds_allowed_bucket_expr(source_col: str, bucket_col: str) -> str:
    """Return the canonical one-hot defensive yard-bucket expression."""
    for name, lo, hi in YDS_ALLOWED_BUCKETS:
        if name == bucket_col:
            condition = f"{_q(source_col)} >= {lo}" if hi is None else f"{_q(source_col)} BETWEEN {lo} AND {hi}"
            return f"CASE WHEN {_q(source_col)} IS NULL THEN NULL WHEN {condition} THEN 1 ELSE 0 END"
    raise ValueError(f"unknown defensive yard bucket: {bucket_col}")


def add_derived_super_table_columns(con: duckdb.DuckDBPyConnection, table: str) -> None:
    """Restore stable weekly scoring columns omitted by the current parquet release."""
    if table != "nfl_player_stats_all":
        return
    columns = {r[0] for r in con.execute(f"DESCRIBE {OPS_SCHEMA}.{_q(table)}").fetchall()}
    if "total_yds_allowed" not in columns:
        return
    for name, _, _ in YDS_ALLOWED_BUCKETS:
        if name not in columns:
            con.execute(f"ALTER TABLE {OPS_SCHEMA}.{_q(table)} ADD COLUMN {_q(name)} DOUBLE")
            con.execute(
                f"UPDATE {OPS_SCHEMA}.{_q(table)} SET {_q(name)} = "
                f"{yds_allowed_bucket_expr('total_yds_allowed', name)}"
            )


def preflight(allowed_drops: set[str]) -> dict:
    """Local source checks + read-only Fly 0-dropped-column guard vs the LIVE tables.

    The live tables may already be views (post-cutover) or real tables (pre-cutover); either exposes
    columns, so the superset guard works in both states. Reuses the same guard as the /merge-ops deploy.
    """
    from multi_league.core.fly_writer import FlyWriter

    con = duckdb.connect()
    writer = FlyWriter()
    out = {}
    for tbl, path in SOURCES.items():
        if not path.exists():
            raise SystemExit(f"PREFLIGHT FAIL: source missing for {tbl}: {path}")
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()}
        if tbl == "nfl_player_stats_all" and "total_yds_allowed" in cols:
            cols.update(name for name, _, _ in YDS_ALLOWED_BUCKETS)
        nrows = con.execute(f"SELECT COUNT(*) FROM '{path.as_posix()}'").fetchone()[0]
        live = writer.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_catalog='___ops' "
            f"AND table_schema='{OPS_SCHEMA}' AND table_name='{tbl}'",
            database="___ops",
        )
        live_cols = {r["column_name"] for r in live}
        dropped = sorted(live_cols - cols)
        unexpected = sorted(set(dropped) - allowed_drops)
        out[tbl] = {"rows": nrows, "new_cols": len(cols), "live_cols": len(live_cols),
                    "added": len(cols - live_cols), "dropped": dropped}
        if unexpected:
            raise SystemExit(f"PREFLIGHT FAIL: {tbl} would DROP live columns: {unexpected[:10]}")
    return out


def build(dry_run: bool) -> None:
    STAGE_ROOT.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f"[build] (dry-run) would build {STAGE_PATH} with {len(SOURCES)} tables in schema {OPS_SCHEMA}")
        return
    if STAGE_PATH.exists():
        STAGE_PATH.unlink()
    con = duckdb.connect(str(STAGE_PATH))
    # Overridable for memory-pressured boxes (2026-07-09: 10GB alloc failed at 91% system load;
    # DuckDB spills to temp_directory, so a lower limit only costs build time).
    import os
    con.execute(f"SET memory_limit='{os.environ.get('OPS_NFL_BUILD_MEMORY_LIMIT', '10GB')}'")
    con.execute("SET threads=2")  # fewer concurrent spill temp files -> avoids Windows temp-delete race
    con.execute("SET preserve_insertion_order=false")
    spill = STAGE_ROOT / f"_spill_{TS}"
    spill.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{spill.as_posix()}'")
    # NB: intentionally no max_temp_directory_size -- its enforcement deletes spill files and
    # races with DuckDB's own cleanup on Windows (IOException: cannot find the file specified).
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {_q(OPS_SCHEMA)}")
    for tbl, path in SOURCES.items():
        con.execute(f"CREATE TABLE {_q(OPS_SCHEMA)}.{_q(tbl)} AS SELECT * FROM '{path.as_posix()}'")
        add_derived_super_table_columns(con, tbl)
        n = con.execute(f"SELECT COUNT(*) FROM {_q(OPS_SCHEMA)}.{_q(tbl)}").fetchone()[0]
        print(f"[build] {OPS_SCHEMA}.{tbl}: {n:,} rows")
    # atomic_swap renames the bare .duckdb; it MUST be fully checkpointed (no side WAL).
    con.execute("CHECKPOINT")
    con.close()
    wal = Path(str(STAGE_PATH) + ".wal")
    if wal.exists():
        raise SystemExit(f"BUILD FAIL: side WAL present after checkpoint: {wal} (swap requires none)")
    print(f"[build] {STAGE_PATH} ({STAGE_PATH.stat().st_size/1e6:.1f} MB, sha256 {_sha256(STAGE_PATH)[:16]}...)")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(1 << 20):
            h.update(chunk)
    return h.hexdigest()


def replace(dry_run: bool) -> None:
    """POST the built file to /replace-db as ___ops_nfl (drain -> atomic_swap -> reopen; ~seconds)."""
    import os
    if dry_run:
        print(f"[replace-db] (dry-run) would POST {STAGE_PATH.name} to /replace-db as {DB_NAME}")
        return
    url = os.environ["DATABASE_SERVER_URL"].rstrip("/")
    token = os.environ["DATABASE_ADMIN_TOKEN"]
    checksum = _sha256(STAGE_PATH)
    with open(STAGE_PATH, "rb") as fh:
        resp = requests.post(
            f"{url}/replace-db",
            headers={"Authorization": f"Bearer {token}", "X-Db-Name": DB_NAME, "X-Content-SHA256": checksum},
            files={"file": (STAGE_PATH.name, fh, "application/octet-stream")},
            timeout=1800,
        )
    if resp.status_code != 200:
        raise SystemExit(f"[replace-db] FAILED ({resp.status_code}): {resp.text[:300]}")
    print(f"[replace-db] -> {resp.json()}")


def verify(dry_run: bool) -> None:
    if dry_run:
        print("[verify] (dry-run) would compare Fly row counts (via the ___ops views) to local sources")
        return
    from multi_league.core.fly_writer import FlyWriter
    con = duckdb.connect()
    writer = FlyWriter()
    ok = True
    for tbl, path in SOURCES.items():
        local_n = con.execute(f"SELECT COUNT(*) FROM '{path.as_posix()}'").fetchone()[0]
        # read through the ___ops view (the logical name callers use)
        fly_n = writer.execute(f"SELECT COUNT(*) AS n FROM {OPS_SCHEMA}.{_q(tbl)}", database="___ops")[0]["n"]
        status = "OK" if local_n == fly_n else "MISMATCH"
        ok = ok and (local_n == fly_n)
        print(f"[verify] {tbl}: local {local_n:,} vs fly {fly_n:,} -> {status}")
    if not ok:
        raise SystemExit("VERIFY FAIL: row-count mismatch (rollback: /replace-db the prior ___ops_nfl, or .prev)")


def refresh_views(dry_run: bool) -> None:
    """Re-run the explicit-column view refresh after every swap. The ___ops views enumerate
    columns (never SELECT * -- DuckDB v1.5.1 mis-binds wide star expansion under window plans),
    so a swap that adds/renames columns MUST refresh them or the new columns stay invisible."""
    if dry_run:
        print("[views] (dry-run) would run scripts/cutover_ops_views.py --apply to refresh the explicit-column views")
        return
    import subprocess
    cmd = [sys.executable, str(ROOT / "scripts" / "cutover_ops_views.py"), "--apply", "--i-understand-this-writes-prod"]
    result = subprocess.run(cmd, cwd=str(ROOT))
    if result.returncode != 0:
        raise SystemExit("[views] REFRESH FAILED -- run scripts/cutover_ops_views.py --apply manually before traffic hits stale views")
    print("[views] explicit-column views refreshed against the new ___ops_nfl")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--build-only", action="store_true", help="build the local .duckdb; do not /replace-db")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i-understand-this-writes-prod", action="store_true")
    ap.add_argument("--allow-drop", default="", help="comma-list of intentionally-deprecated live cols permitted to drop")
    a = ap.parse_args()
    write_prod = a.apply and a.i_understand_this_writes_prod
    dry = not (write_prod or a.build_only)
    mode = "BUILD-ONLY" if (a.build_only and not write_prod) else ("APPLY (PROD WRITE)" if write_prod else "DRY-RUN")
    print(f"=== build ___ops_nfl -> /replace-db ({mode}) ts={TS} ===")
    load_env()
    allowed = {c.strip() for c in a.allow_drop.split(",") if c.strip()}
    pf = preflight(allowed)
    for tbl, d in pf.items():
        print(f"[preflight] {tbl}: {d['rows']:,} rows | new {d['new_cols']} cols (live {d['live_cols']}, "
              f"+{d['added']} added, dropped {len(d['dropped'])})")
    build_now = a.build_only or write_prod   # produce a real file for build-only OR apply
    build(dry_run=not build_now)
    replace(dry_run=not write_prod)          # only actually swap on --apply
    # Refresh views BEFORE verify. The swap can restart the Fly machine, and a transient 500 on
    # the verify read must NOT skip the view refresh: the ___ops views enumerate columns explicitly,
    # so a schema-changing swap that leaves them stale breaks the super-table view in prod (a dropped
    # column throws a binder error on every read). refresh_views is the correctness step; verify is a
    # post-check. (2026-07-21: verify 500'd here and crashed the run pre-refresh, breaking prod views.)
    refresh_views(dry_run=not write_prod)    # explicit-column views must track the new file's schema
    verify(dry_run=not write_prod)
    print("\nDONE." + ("" if write_prod else "  (no prod write)"))


if __name__ == "__main__":
    main()
