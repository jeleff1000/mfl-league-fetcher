"""publish_research_matchup_to_fly.py -- promote the bracket-specific research matchup tables to Fly.

  *** WRITES TO PRODUCTION only with --apply --i-understand-this-writes-prod. ***
  Default is a DRY RUN: every preflight runs read-only against live, the plan and the
  before/after boards print, and nothing is written.

## Which transport, and why not shadow+rename

The frontend reads one season/weekly/career table for each supported bracket in the ___ops file
(frontend/src/lib/research-cohort-query.ts):
    ___ops.nfl_historical.research_matchup          (season)
    ___ops.nfl_historical.research_matchup_weekly
    ___ops.nfl_historical.research_matchup_career

The obvious plan -- upload `research_matchup_new`, verify, then rename-swap -- is
**eliminated for ___ops** by our own investigation
(docs/runbooks/supertable-fly-deploy-swap-handoff.md §4 RESOLVED, 2026-07-08):

  * reads take READ-ONLY handles on ___ops.duckdb, per query, and detach in `finally`;
  * ANY write opens a fresh READ-WRITE handle to the same file, which DuckDB forbids while
    a read-only handle is open -- the conflict is at handle OPEN, not on write duration;
  * after 3 failed RW opens the server calls `close_pool()`, which closes the ___leagues
    pool too. That is the ~9-minute FULL-SITE outage, and a staging `CREATE ... _new` trips
    it exactly as `CREATE OR REPLACE` does. Shadow+rename moves the long write; it does not
    avoid the handle conflict.

`/replace-db` (the whole-file swap used for ___ops_nfl) is also unsafe here: ___ops is NOT a
read-only reference DB. It holds live mutable app state -- league_credentials, the Stripe
payment tables, import_jobs, fleet_health.validation_results -- so swapping the file loses
every write that lands during the build+upload window (§4b).

That leaves `/merge-ops`, which is the SUPPORTED path for exactly this case: it uploads one
bundle and does a per-table `CREATE OR REPLACE` inside ___ops, leaving the other ~48 tables
untouched and never touching ___leagues. Its cost is the exclusive-writer window, which
scales with the write -- and these tables are small (measured live 2026-07-26: 9,667 +
95,916 + 783 rows at ~494 columns), so the window is seconds, not the minutes the 17 GB
super table needed. All three grains ride ONE bundle so they can never serve a mixed state.

## Preflight (all read-only; any failure aborts before a byte is written)

  1. every source table exists in the bundle and is non-empty;
  2. 0-DROPPED-COLUMN guard vs the LIVE table -- a column the site currently reads may not
     vanish (pass --allow-drop for an intentional retirement);
  3. row-count band -- a build that collapsed is refused (--min-row-ratio, default 0.5);
  4. impossible-value gate on every served rate lane of the audited slug (>100% = abort);
  5. BOARD DIFF -- the same board pulled from live and from the bundle, printed side by
     side. This is the human-readable "did the numbers move the way we intended" check.

    python scripts/research_cohorts/publish_research_matchup_to_fly.py            # dry run
    python scripts/research_cohorts/publish_research_matchup_to_fly.py --build-only
    python scripts/research_cohorts/publish_research_matchup_to_fly.py \
        --apply --i-understand-this-writes-prod
"""
from __future__ import annotations

import argparse
import hashlib
import os
import sys
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import requests

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

OUT_DIR = Path(os.environ.get(
    "RESEARCH_OUT_DIR", "D:/league-history-data/fantasy_leagues/cohort_aggregates"))
BUNDLE = OUT_DIR / "research_merge_bundle.duckdb"
OPS_SCHEMA = "nfl_historical"
LEGACY_TABLES = ["research_matchup", "research_matchup_weekly", "research_matchup_career"]
LEGACY_TABLES += [f"research_matchup{grain}_{bracket}"
           for bracket in ("4po", "8po")
           for grain in ("", "_weekly", "_career")]
ADAPTIVE_TABLES = ["research_matchup_adaptive", "research_matchup_adaptive_weekly",
                   "research_matchup_adaptive_career"]
TABLES = LEGACY_TABLES + ADAPTIVE_TABLES

TS = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
STAGE_ROOT = Path(os.environ.get(
    "RESEARCH_STAGE_DIR", "D:/league-history-data/fantasy_leagues/tmp/research_publish"))
STAGE_PATH = STAGE_ROOT / f"research_matchup_{TS}.duckdb"

# Served rate lanes, per slug. >100% here is the class of defect T6/T7 existed to kill, so
# the publisher refuses to ship it even if the audit was skipped.
RATE_LANES = ["start_rate", "win_rate", "champ_total", "champ_started",
              "playoff_total", "playoff_started"]


def _q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while chunk := handle.read(1 << 20):
            digest.update(chunk)
    return digest.hexdigest()


def preflight(con, reader, allowed_drops: set[str], min_ratio: float, slug: str) -> dict:
    """Read-only checks against the bundle and the LIVE tables. Raises on any failure."""
    summary: dict[str, dict] = {}
    for table in TABLES:
        try:
            rows = con.execute(f"SELECT COUNT(*) FROM {_q(table)}").fetchone()[0]
        except duckdb.CatalogException as exc:
            raise SystemExit(f"PREFLIGHT FAIL: {table} missing from {BUNDLE}") from exc
        if rows == 0:
            raise SystemExit(f"PREFLIGHT FAIL: {table} is empty in the bundle")
        new_cols = {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}

        live = reader.query(
            "SELECT column_name FROM information_schema.columns WHERE table_catalog='___ops' "
            f"AND table_schema='{OPS_SCHEMA}' AND table_name='{table}'",
            database="___ops",
        )
        live_cols = {r["column_name"] for r in live}
        live_rows = reader.query(
            f"SELECT COUNT(*) AS n FROM {OPS_SCHEMA}.{_q(table)}", database="___ops",
        )[0]["n"] if live_cols else 0

        dropped = sorted(live_cols - new_cols)
        unexpected = sorted(set(dropped) - allowed_drops)
        if unexpected:
            raise SystemExit(
                f"PREFLIGHT FAIL: {table} would DROP live columns the site may read: "
                f"{unexpected[:12]}{'...' if len(unexpected) > 12 else ''}\n"
                f"  (pass --allow-drop {','.join(unexpected[:3])} if the retirement is intended)")
        if live_rows and rows < min_ratio * live_rows:
            raise SystemExit(
                f"PREFLIGHT FAIL: {table} collapsed -- {rows:,} rows vs {live_rows:,} live "
                f"(< {min_ratio:.0%}). Pass --min-row-ratio to override deliberately.")

        summary[table] = {
            "rows": rows, "live_rows": live_rows,
            "cols": len(new_cols), "live_cols": len(live_cols),
            "added": sorted(new_cols - live_cols), "dropped": dropped,
        }
        print(f"[preflight] {table}: {rows:,} rows ({live_rows:,} live), "
              f"{len(new_cols)} cols (+{len(new_cols - live_cols)} / -{len(dropped)})")
        if summary[table]["added"]:
            shown = summary[table]["added"][:8]
            print(f"             new columns: {', '.join(shown)}"
                  f"{' ...' if len(summary[table]['added']) > 8 else ''}")

    # Impossible-value gate on the served lanes of the audited slug.
    for table in TABLES:
        cols = {r[1] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()}
        lanes = [f"{lane}_{slug}" for lane in RATE_LANES
                 if f"{lane}_{slug}" in cols]
        if not lanes:
            continue
        checks = ", ".join(
            f"COUNT(*) FILTER (WHERE {_q(lane)} > 100.001) AS bad_{i}"
            for i, lane in enumerate(lanes))
        row = con.execute(f"SELECT {checks} FROM {_q(table)}").fetchone()
        bad = [f"{lane}={count:,}" for lane, count in zip(lanes, row, strict=True) if count]
        if bad:
            raise SystemExit(
                f"PREFLIGHT FAIL: {table} carries impossible rates (>100%): {', '.join(bad)}")
        print(f"[preflight] {table}: {len(lanes)} rate lanes all <= 100% for {slug}")
    return summary


def board_diff(con, reader, slug: str, limit: int) -> None:
    """Print the same board from live and from the bundle so the diff is readable BEFORE
    the write. Clutch is the board the exposure policy uses as its fidelity litmus."""
    metric = f"clutch_{slug}"
    sql = f"""
      SELECT n.player, ROUND(r.{_q(metric)}, 3) AS clutch
      FROM {{src}}.research_matchup r
      JOIN {{src}}.research_player_names n ON n.NFL_player_id = r.NFL_player_id
      WHERE r.year = 2023 AND n."position" = 'RB' AND r.{_q(metric)} IS NOT NULL
      ORDER BY r.{_q(metric)} DESC LIMIT {limit}"""
    try:
        live = reader.query(sql.format(src=f"___ops.{OPS_SCHEMA}"), database="___ops")
        live_rows = [(r["player"], r["clutch"]) for r in live]
    except Exception as exc:  # a missing lane live is informative, not fatal
        live_rows = []
        print(f"[board] live board unavailable: {exc}")
    try:
        new_rows = con.execute(sql.format(src="main")).fetchall()
    except duckdb.Error as exc:
        raise SystemExit(f"PREFLIGHT FAIL: cannot pull the board from the bundle: {exc}") from exc

    print(f"\n[board] RB clutch 2023, {slug} -- LIVE vs NEW (top {limit})")
    print(f"  {'#':>2}  {'LIVE':<26} {'':>8}   {'NEW':<26} {'':>8}")
    for i in range(max(len(live_rows), len(new_rows))):
        lp, lv = live_rows[i] if i < len(live_rows) else ("", "")
        np_, nv = new_rows[i] if i < len(new_rows) else ("", "")
        flag = "" if lp == np_ else "  <-- moved"
        print(f"  {i + 1:>2}  {lp:<26} {lv:>8}   {np_:<26} {nv:>8}{flag}")
    print()


def build(dry_run: bool, bundle_path: Path, expected: dict[str, dict]) -> Path | None:
    """Stage ONE bundle holding all three tables -- one upload, one swap window.

    bundle_path is passed in, never read from the module default: staging the default while
    preflighting an explicit --bundle published the WRONG data on 2026-07-27 (preflight read
    18,067 rows, staging wrote 9,667). The staged counts are asserted against the counts
    preflight actually measured, so the two can never again describe different files.
    """
    if dry_run:
        print(f"[build] (dry-run) would stage {STAGE_PATH.name} with {len(TABLES)} tables "
              f"in schema {OPS_SCHEMA}")
        return None
    STAGE_ROOT.mkdir(parents=True, exist_ok=True)
    if STAGE_PATH.exists():
        STAGE_PATH.unlink()
    con = duckdb.connect(str(STAGE_PATH))
    memory_mb = os.environ.get("RESEARCH_PUBLISH_MEMORY_MB", "3000")
    threads = os.environ.get("RESEARCH_PUBLISH_THREADS", "2")
    con.execute(f"SET memory_limit='{memory_mb}MB'")
    con.execute(f"SET threads={threads}")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"ATTACH '{Path(bundle_path).as_posix()}' AS src (READ_ONLY)")
    # /merge-ops reads the bundle's tables by bare name and writes them into
    # ___ops.nfl_historical, so they are staged unqualified in main.
    for table in TABLES:
        con.execute(f"CREATE TABLE {_q(table)} AS SELECT * FROM src.{_q(table)}")
        n = con.execute(f"SELECT COUNT(*) FROM {_q(table)}").fetchone()[0]
        want = expected[table]["rows"]
        if n != want:
            con.close()
            raise SystemExit(
                f"STAGING FAIL: {table} staged {n:,} rows but preflight measured {want:,} "
                f"in {bundle_path} -- staging and preflight are reading different files")
        print(f"[build] {table}: {n:,} rows staged")
    con.execute("DETACH src")
    con.execute("CHECKPOINT")
    con.close()
    wal = Path(str(STAGE_PATH) + ".wal")
    if wal.exists():
        raise SystemExit(f"BUILD FAIL: side WAL present after checkpoint: {wal}")
    print(f"[build] {STAGE_PATH} ({STAGE_PATH.stat().st_size / 1e6:.1f} MB, "
          f"sha256 {_sha256(STAGE_PATH)[:16]}...)")
    return STAGE_PATH


def merge_ops(path: Path, dry_run: bool) -> None:
    if dry_run:
        print(f"[merge-ops] (dry-run) would POST {STAGE_PATH.name} to /merge-ops "
              f"-> per-table CREATE OR REPLACE in ___ops.{OPS_SCHEMA} "
              f"({', '.join(TABLES)}); ___leagues untouched")
        return
    url = os.environ["DATABASE_SERVER_URL"].rstrip("/")
    token = os.environ["DATABASE_ADMIN_TOKEN"]
    with open(path, "rb") as handle:
        resp = requests.post(
            f"{url}/merge-ops",
            headers={"Authorization": f"Bearer {token}"},
            files={"file": (path.name, handle, "application/octet-stream")},
            timeout=1800,
        )
    if resp.status_code != 200:
        raise SystemExit(f"[merge-ops] FAILED ({resp.status_code}): {resp.text[:400]}")
    print(f"[merge-ops] -> {resp.json()}")


def verify(con, reader, dry_run: bool, slug: str) -> None:
    if dry_run:
        print("[verify] (dry-run) would compare Fly row counts to the bundle and re-pull "
              "the board through the live tables")
        return
    ok = True
    for table in TABLES:
        local_n = con.execute(f"SELECT COUNT(*) FROM {_q(table)}").fetchone()[0]
        fly_n = reader.query(
            f"SELECT COUNT(*) AS n FROM {OPS_SCHEMA}.{_q(table)}", database="___ops")[0]["n"]
        status = "OK" if local_n == fly_n else "MISMATCH"
        ok = ok and local_n == fly_n
        print(f"[verify] {table}: bundle {local_n:,} vs fly {fly_n:,} -> {status}")
    if not ok:
        raise SystemExit(
            "VERIFY FAIL: row-count mismatch. Rollback: re-run /merge-ops with the previous "
            "staged bundle in " + str(STAGE_ROOT))
    live = reader.query(
        f"SELECT n.player, ROUND(r.{_q('clutch_' + slug)}, 3) AS clutch "
        f"FROM {OPS_SCHEMA}.research_matchup r "
        f"JOIN {OPS_SCHEMA}.research_player_names n ON n.NFL_player_id = r.NFL_player_id "
        f"WHERE r.year = 2023 AND n.\"position\" = 'RB' "
        f"AND r.{_q('clutch_' + slug)} IS NOT NULL "
        f"ORDER BY r.{_q('clutch_' + slug)} DESC LIMIT 5", database="___ops")
    print("[verify] live RB clutch 2023 top-5 after the swap:")
    for i, row in enumerate(live, 1):
        print(f"           {i}. {row['player']} {row['clutch']}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--bundle", type=Path, default=BUNDLE)
    ap.add_argument("--slug", default="12t_flx_ppr_4pt")
    ap.add_argument("--build-only", action="store_true",
                    help="run preflight and stage the bundle; do not POST")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--i-understand-this-writes-prod", action="store_true")
    ap.add_argument("--allow-drop", default="",
                    help="comma-list of live columns whose removal is intended")
    ap.add_argument("--min-row-ratio", type=float, default=0.5,
                    help="refuse a build with fewer than this share of the live row count")
    ap.add_argument("--board-limit", type=int, default=10)
    ap.add_argument("--adaptive-only", action="store_true",
                    help="publish only the narrow adaptive serving tables; leave legacy tables untouched")
    ap.add_argument("--split-upload", action="store_true",
                    help="upload adaptive tables as separate temporary bundles to stay under /merge-ops 5GB limit")
    args = ap.parse_args()

    global TABLES
    if args.adaptive_only:
        TABLES = list(ADAPTIVE_TABLES)
    if args.split_upload and not args.adaptive_only:
        raise SystemExit("--split-upload requires --adaptive-only")

    load_env()
    write_prod = args.apply and args.i_understand_this_writes_prod
    dry = not (write_prod or args.build_only)
    mode = ("APPLY (PROD WRITE)" if write_prod
            else "BUILD-ONLY" if args.build_only else "DRY-RUN")
    print(f"=== publish research matchup -> /merge-ops ({mode}) ts={TS} ===")
    if args.apply and not args.i_understand_this_writes_prod:
        raise SystemExit("--apply requires --i-understand-this-writes-prod")

    bundle_path = Path(args.bundle)
    if not bundle_path.exists():
        raise SystemExit(f"no wide bundle at {bundle_path} -- run build_wide_bundle.py first")

    # Reads go through FlyReader /query, never /query-rw: the rw endpoint 500s on wide
    # aggregates, and every check here is read-only. The only write is the /merge-ops POST.
    from multi_league.core.readers.fly_reader import FlyReader
    con = duckdb.connect(str(bundle_path), read_only=True)
    reader = FlyReader()
    allowed = {c.strip() for c in args.allow_drop.split(",") if c.strip()}

    summary = preflight(con, reader, allowed, args.min_row_ratio, args.slug)
    if not args.adaptive_only:
        board_diff(con, reader, args.slug, args.board_limit)

    if args.split_upload:
        if not write_prod:
            raise SystemExit("--split-upload requires --apply --i-understand-this-writes-prod")
        original_tables = list(TABLES)
        for table in original_tables:
            TABLES = [table]
            print(f"[split-upload] staging and publishing {table}")
            staged = build(False, bundle_path, {table: summary[table]})
            merge_ops(staged or STAGE_PATH, False)
            n = reader.query(
                f"SELECT COUNT(*) AS n FROM {OPS_SCHEMA}.{_q(table)}",
                database="___ops",
            )[0]["n"]
            print(f"[verify] adaptive {table}: {n:,} live rows")
            if n != summary[table]["rows"]:
                raise SystemExit(
                    f"VERIFY FAIL: adaptive {table} live rows {n:,} != bundle "
                    f"{summary[table]['rows']:,}"
                )
        TABLES = original_tables
        con.close()
        print("[done] published adaptive tables to ___ops.nfl_historical via split upload")
        return 0

    staged = build(dry, bundle_path, summary)
    if args.build_only and not write_prod:
        print("[done] staged only -- nothing written to prod")
        con.close()
        return 0
    merge_ops(staged or STAGE_PATH, dry)
    if args.adaptive_only and not dry:
        for table in TABLES:
            n = reader.query(f"SELECT COUNT(*) AS n FROM {OPS_SCHEMA}.{_q(table)}", database="___ops")[0]["n"]
            print(f"[verify] adaptive {table}: {n:,} live rows")
            if not n:
                raise SystemExit(f"VERIFY FAIL: adaptive table {table} is empty on Fly")
    else:
        verify(con, reader, dry, args.slug)
    con.close()
    print("[done] " + ("dry run complete -- nothing was written"
                       if dry else "published to ___ops.nfl_historical"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
