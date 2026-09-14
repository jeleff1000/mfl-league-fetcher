"""
deploy_v26_supertable_to_fly.py  --  swap the v26 NFL reference tables into Fly ___ops via /merge-ops.

  *** WRITES TO PRODUCTION ___ops WHEN --apply IS GIVEN. NOT auto-run. ***

Replaces shared NFL reference tables (no per-league data) from the local v26 build into Fly ___ops using
the dedicated /merge-ops endpoint. That endpoint uploads ONE plain .duckdb bundle and server-side does an
atomic CREATE OR REPLACE per table on the lightweight ___ops writer path -- it NEVER touches ___leagues,
so the app stays online; only ___ops-backed reads (player stat cards) briefly ride out each table rebuild.

  1. PRE-FLIGHT (local + read-only Fly): verify each source parquet exists & row counts sane; and that the
     new table is a SUPERSET of the live table's columns (0-dropped guard) so we never deploy a regression.
  2. STAGE: build one local DuckDB bundle with the target tables (plain, no db_name tag).
  3. MERGE-OPS: POST the bundle to /merge-ops; the server CREATE OR REPLACEs each ___ops.nfl_historical
     table atomically. Other ___ops tables (accounts/credentials, franchises) are untouched.
  4. VERIFY: Fly ___ops row counts == local source row counts for every table.

ROLLBACK: re-run /merge-ops with the prior (pre-v26) bundle -- the live ___ops snapshot kept on D:.
(CREATE OR REPLACE is atomic; there is no on-server __bak copy, to avoid doubling the 17GB ___ops file.)

CAVEATS:
  - Deploying the duckdb-server CODE that adds /merge-ops requires a Fly restart (one brief blip) -- the
    DATA swap itself causes no app outage. Run the deploy off-peak.
  - REGENERATE the frontend research catalog/index after (the ~93 new cols + nfl_position/IDP/type changes)
    or research mode is blind/wrong.
  - nfl_position is now normalized; 10 cols VARCHAR->DOUBLE; pts_idp_* reweighted -- confirm consumers.

    python scripts/deploy_v26_supertable_to_fly.py                 # dry-run plan (default, no writes)
    python scripts/deploy_v26_supertable_to_fly.py --apply --i-understand-this-writes-prod   # real merge
    python scripts/deploy_v26_supertable_to_fly.py --apply --i-understand-this-writes-prod --only player_bio
"""

from __future__ import annotations
import argparse
import sys
from datetime import datetime, timezone, UTC
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = ROOT / "fantasy_football_data_scripts"
sys.path.insert(0, str(SCRIPTS_ROOT))

V26_DIR = Path("D:/league-history-data/nfl/releases/nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables")
ART = V26_DIR / "season_career_v26"
OPS = "nfl_historical"

# table -> local source parquet
SOURCES: dict[str, Path] = {
    "nfl_player_stats_all": V26_DIR / "nfl_player_stats_all.parquet",
    "player_nfl_season": ART / "player_nfl_season.parquet",
    "player_nfl_season_all": ART / "player_nfl_season_all.parquet",
    "player_nfl_career": ART / "player_nfl_career.parquet",
    "player_nfl_career_all": ART / "player_nfl_career_all.parquet",
    "player_bio": Path("D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"),
}
TS = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
STAGE_ROOT = Path("D:/league-history-data/nfl/tmp/deploy_bundles")
STAGE_PATH = STAGE_ROOT / f"ops_v26_bundle_{TS}.duckdb"


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


def preflight(allowed_drops: set[str] | None = None) -> dict:
    """Local source checks + read-only Fly column-superset (0-dropped) guard.

    ``allowed_drops`` are columns intentionally deprecated (e.g. pts_k_yahoo -> pts_k_std);
    they may drop without failing the guard, but any OTHER (unexpected) drop still hard-fails.
    """
    from multi_league.core.fly_writer import FlyWriter

    allowed_drops = allowed_drops or set()
    con = duckdb.connect()
    out = {}
    writer = FlyWriter()
    for tbl, path in SOURCES.items():
        if not path.exists():
            raise SystemExit(f"PREFLIGHT FAIL: source missing for {tbl}: {path}")
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{path.as_posix()}'").fetchall()}
        nrows = con.execute(f"SELECT COUNT(*) FROM '{path.as_posix()}'").fetchone()[0]
        # live columns for this table (read-only)
        live = writer.execute(
            f"SELECT column_name FROM information_schema.columns WHERE table_catalog='___ops' "
            f"AND table_schema='{OPS}' AND table_name='{tbl}'",
            database="___ops",
        )
        live_cols = {r["column_name"] for r in live}
        dropped = sorted(live_cols - cols)
        intentional = sorted(set(dropped) & allowed_drops)
        unexpected = sorted(set(dropped) - allowed_drops)
        out[tbl] = {
            "rows": nrows,
            "new_cols": len(cols),
            "live_cols": len(live_cols),
            "dropped": dropped,
            "intentional_drops": intentional,
            "added": len(cols - live_cols),
        }
        if intentional:
            print(f"[preflight] {tbl}: DROPPING deprecated (allowed): {intentional}")
        if unexpected:
            raise SystemExit(f"PREFLIGHT FAIL: {tbl} would DROP live columns: {unexpected[:10]}")
    return out


def build_stage(dry_run: bool) -> None:
    STAGE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if dry_run:
        print(f"[stage] (dry-run) would build {STAGE_PATH} with {len(SOURCES)} tables")
        return
    if STAGE_PATH.exists():
        STAGE_PATH.unlink()
    con = duckdb.connect(str(STAGE_PATH))
    con.execute("SET memory_limit='8GB'")
    con.execute("SET threads=2")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{(STAGE_ROOT / '_spill').as_posix()}'")
    con.execute("PRAGMA max_temp_directory_size='50GiB'")
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    for tbl, path in SOURCES.items():
        # plain table, same name as the ___ops table -- /merge-ops maps it to nfl_historical.<tbl>
        con.execute(f"CREATE TABLE public.{_q(tbl)} AS SELECT * FROM '{path.as_posix()}'")
        n = con.execute(f"SELECT COUNT(*) FROM public.{_q(tbl)}").fetchone()[0]
        print(f"[stage] {tbl}: {n:,} rows")
    con.close()
    print(f"[stage] bundle: {STAGE_PATH} ({STAGE_PATH.stat().st_size/1e6:.1f} MB)")


def upload(dry_run: bool) -> None:
    """POST the bundle to /merge-ops -- server does the atomic per-table CREATE OR REPLACE in ___ops."""
    if dry_run:
        print(f"[merge-ops] (dry-run) would POST {STAGE_PATH.name} to /merge-ops")
        return
    from multi_league.core.targets.fly_target import FlyTarget

    res = FlyTarget().merge_ops(STAGE_PATH)
    print(f"[merge-ops] -> {res}")


def verify(dry_run: bool) -> None:
    if dry_run:
        print("[verify] (dry-run) would compare Fly row counts to local sources")
        return
    from multi_league.core.fly_writer import FlyWriter

    con = duckdb.connect()
    writer = FlyWriter()
    ok = True
    for tbl, path in SOURCES.items():
        local_n = con.execute(f"SELECT COUNT(*) FROM '{path.as_posix()}'").fetchone()[0]
        fly_n = writer.execute(f"SELECT COUNT(*) AS n FROM {OPS}.{_q(tbl)}", database="___ops")[0]["n"]
        status = "OK" if local_n == fly_n else "MISMATCH"
        if local_n != fly_n:
            ok = False
        print(f"[verify] {tbl}: local {local_n:,} vs fly {fly_n:,} -> {status}")
    if not ok:
        raise SystemExit("VERIFY FAIL: row-count mismatch -- re-run /merge-ops with the prior bundle to roll back.")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i-understand-this-writes-prod", action="store_true")
    ap.add_argument("--only", default=None, help="merge only this one table (e.g. player_bio) for a safe first run")
    ap.add_argument("--keep-bundle", action="store_true", help="do not delete the local staged bundle after merge")
    ap.add_argument("--allow-drop", default="", help="comma-list of intentionally-deprecated live cols permitted to drop (e.g. pts_k_yahoo)")
    a = ap.parse_args()
    if a.only:
        if a.only not in SOURCES:
            raise SystemExit(f"--only must be one of {list(SOURCES)}")
        for k in list(SOURCES):
            if k != a.only:
                del SOURCES[k]
        print(f"[scope] limited to single table: {a.only}")
    dry = not (a.apply and a.i_understand_this_writes_prod)
    print(f"=== v26 -> Fly ___ops /merge-ops ({'DRY-RUN' if dry else 'APPLY (PROD WRITE)'}) ts={TS} ===")
    load_env()
    allowed = {c.strip() for c in a.allow_drop.split(",") if c.strip()}
    pf = preflight(allowed)
    for tbl, d in pf.items():
        print(
            f"[preflight] {tbl}: {d['rows']:,} rows | new {d['new_cols']} cols (live {d['live_cols']}, "
            f"+{d['added']} added, dropped {len(d['dropped'])})"
        )
    build_stage(dry)
    upload(dry)
    verify(dry)
    if not dry and not a.keep_bundle and STAGE_PATH.exists():
        STAGE_PATH.unlink()
        print(f"[cleanup] removed local bundle {STAGE_PATH.name}")
    print(
        "\nDONE."
        + (
            "  (dry-run -- no writes)"
            if dry
            else "  ___ops updated via /merge-ops (___leagues untouched). REGEN the frontend catalog next."
        )
    )


if __name__ == "__main__":
    main()
