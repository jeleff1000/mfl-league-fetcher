#!/usr/bin/env python3
"""Benchmark the fleet partition publish at Week-1 planning scale. Offline only.

Builds an on-disk ___leagues.duckdb shaped like production (player_fantasy +
matchup, full history), stages a late-season active-year recompute for the
batched leagues, builds a REAL fleet bundle, validates it with the REAL server
validators, and applies the REAL scoped merge with the merge connection pinned
to the Fly box's budget (memory_limit 2GB, threads 2).

Defaults model the worst case:
- 1,232 leagues (32 excluded as simulated fetch failures)
- 2 frozen prior seasons x 14 weeks + active season through week 17
- 513 player_fantasy rows per league-week, 12 matchup rows per league-week
- 24 filler DOUBLE stat columns to approximate real row width

Usage:
    python scripts/bench_fleet_partition_publish.py
    python scripts/bench_fleet_partition_publish.py --leagues 128 --active-weeks 8
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
import tarfile
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
PIPELINE_ROOT = ROOT / "fantasy_football_data_scripts"
ARTIFACT_DIR = ROOT / "scripts" / "_artifacts"

sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(PIPELINE_ROOT))

ACTIVE_YEAR = 2026


def load_fleet_merge():
    import importlib.util

    path = ROOT / "duckdb-server" / "fleet_merge.py"
    spec = importlib.util.spec_from_file_location("bench_fleet_merge", path)
    module = importlib.util.module_from_spec(spec)
    sys.modules["bench_fleet_merge"] = module
    spec.loader.exec_module(module)
    return module


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while chunk := fh.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def filler_cols_ddl(n: int) -> str:
    return ", ".join(f"stat_{i} DOUBLE" for i in range(n))


def filler_cols_select(n: int) -> str:
    return ", ".join(f"ROUND(((p.p * {i + 3} + w.week) % 1000) / 7.0, 3) AS stat_{i}" for i in range(n))


def create_tables(conn: duckdb.DuckDBPyConnection, filler: int) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        f"""
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER,
            player_week VARCHAR, NFL_player_id VARCHAR,
            manager VARCHAR, fantasy_points DOUBLE, {filler_cols_ddl(filler)}
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager_week VARCHAR, manager VARCHAR, team_points DOUBLE
        )
        """
    )


def fill_player_fantasy(
    conn, *, leagues: int, years_sql: str, weeks: int, rows_per_week: int, filler: int, bump: float = 0.0
) -> None:
    conn.execute(
        f"""
        INSERT INTO public.player_fantasy
        SELECT
            'league_' || CAST(l.i AS VARCHAR),
            y.year, w.week,
            'league_' || CAST(l.i AS VARCHAR) || ':' || CAST(y.year AS VARCHAR)
                || ':' || CAST(w.week AS VARCHAR) || ':P' || CAST(p.p AS VARCHAR),
            'nfl_' || CAST(p.p % 3000 AS VARCHAR),
            'Manager ' || CAST(p.p % 12 AS VARCHAR),
            ROUND(5 + ((p.p * 13 + w.week * 7 + y.year) % 400) / 10.0 + {bump}, 2),
            {filler_cols_select(filler)}
        FROM range(0, {leagues}) AS l(i)
        CROSS JOIN ({years_sql}) AS y(year)
        CROSS JOIN range(1, {weeks} + 1) AS w(week)
        CROSS JOIN range(0, {rows_per_week}) AS p(p)
        """
    )


def fill_matchup(conn, *, leagues: int, years_sql: str, weeks: int, managers: int, bump: float = 0.0) -> None:
    conn.execute(
        f"""
        INSERT INTO public.matchup
        SELECT
            'league_' || CAST(l.i AS VARCHAR),
            y.year, w.week,
            'league_' || CAST(l.i AS VARCHAR) || ':' || CAST(y.year AS VARCHAR)
                || ':' || CAST(w.week AS VARCHAR) || ':M' || CAST(m.m AS VARCHAR),
            'Manager ' || CAST(m.m AS VARCHAR),
            ROUND(80 + ((m.m * 17 + w.week * 5 + y.year) % 900) / 10.0 + {bump}, 2)
        FROM range(0, {leagues}) AS l(i)
        CROSS JOIN ({years_sql}) AS y(year)
        CROSS JOIN range(1, {weeks} + 1) AS w(week)
        CROSS JOIN range(0, {managers}) AS m(m)
        """
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leagues", type=int, default=1232)
    parser.add_argument("--excluded", type=int, default=32, help="Simulated fetch failures")
    parser.add_argument("--prior-seasons", type=int, default=2)
    parser.add_argument("--prior-weeks", type=int, default=14)
    parser.add_argument("--active-weeks", type=int, default=17, help="Late-season worst case")
    parser.add_argument("--player-rows-per-week", type=int, default=513)
    parser.add_argument("--managers-per-week", type=int, default=12)
    parser.add_argument("--filler-cols", type=int, default=24)
    parser.add_argument("--memory-limit", default="2GB", help="Merge connection budget (Fly parity)")
    parser.add_argument("--threads", type=int, default=2, help="Merge connection threads (Fly parity)")
    parser.add_argument("--workdir", default=None)
    parser.add_argument("--out", default=None)
    args = parser.parse_args()

    from multi_league.core.delta_publish import canonical_table_registry
    from multi_league.core.fleet_publish import build_fleet_partition_bundle

    fleet_merge = load_fleet_merge()
    registry = canonical_table_registry()

    workdir = Path(args.workdir) if args.workdir else Path(tempfile.mkdtemp(prefix="fleet_bench_"))
    workdir.mkdir(parents=True, exist_ok=True)
    server_path = workdir / "___leagues.duckdb"
    server_path.unlink(missing_ok=True)

    batch_leagues = args.leagues - args.excluded
    prior_years_sql = f"SELECT {ACTIVE_YEAR - 1 - i}" if args.prior_seasons == 1 else (
        "SELECT " + str(ACTIVE_YEAR) + " - 1 - CAST(i AS INTEGER) FROM range(0, " + str(args.prior_seasons) + ") AS t(i)"
    )
    active_year_sql = f"SELECT {ACTIVE_YEAR}"
    timings: dict[str, float] = {}
    report: dict = {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config": vars(args),
        "active_year": ACTIVE_YEAR,
        "batch_leagues": batch_leagues,
    }

    def stage(name: str):
        class _T:
            def __enter__(self):
                self.t0 = time.perf_counter()
                print(f"[bench] {name}...", flush=True)
                return self

            def __exit__(self, *exc):
                timings[name] = round(time.perf_counter() - self.t0, 2)
                print(f"[bench] {name} done in {timings[name]:.2f}s", flush=True)

        return _T()

    # 1. Server database: full history for all leagues (prior seasons frozen)
    #    + active season through the previous week (active_weeks - 1).
    with stage("build_server_db"):
        server_build = duckdb.connect(str(server_path))
        create_tables(server_build, args.filler_cols)
        fill_player_fantasy(
            server_build,
            leagues=args.leagues,
            years_sql=prior_years_sql,
            weeks=args.prior_weeks,
            rows_per_week=args.player_rows_per_week,
            filler=args.filler_cols,
        )
        fill_player_fantasy(
            server_build,
            leagues=args.leagues,
            years_sql=active_year_sql,
            weeks=args.active_weeks - 1,
            rows_per_week=args.player_rows_per_week,
            filler=args.filler_cols,
        )
        fill_matchup(
            server_build,
            leagues=args.leagues,
            years_sql=prior_years_sql,
            weeks=args.prior_weeks,
            managers=args.managers_per_week,
        )
        fill_matchup(
            server_build,
            leagues=args.leagues,
            years_sql=active_year_sql,
            weeks=args.active_weeks - 1,
            managers=args.managers_per_week,
        )
        report["server_rows"] = {
            "player_fantasy": server_build.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0],
            "matchup": server_build.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0],
        }
        server_build.execute("CHECKPOINT")
        server_build.close()
        report["server_db_mb"] = round(server_path.stat().st_size / (1024 * 1024), 1)

    # 2. Staged compute output: batched leagues, active season incl. the new week.
    with stage("stage_fleet_rows"):
        staged = duckdb.connect(":memory:")
        create_tables(staged, args.filler_cols)
        fill_player_fantasy(
            staged,
            leagues=batch_leagues,
            years_sql=active_year_sql,
            weeks=args.active_weeks,
            rows_per_week=args.player_rows_per_week,
            filler=args.filler_cols,
            bump=1.25,
        )
        fill_matchup(
            staged,
            leagues=batch_leagues,
            years_sql=active_year_sql,
            weeks=args.active_weeks,
            managers=args.managers_per_week,
            bump=1.25,
        )
        report["staged_rows"] = {
            "player_fantasy": staged.execute("SELECT COUNT(*) FROM public.player_fantasy").fetchone()[0],
            "matchup": staged.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0],
        }

    # 3. Real client bundle build (parquet export + fingerprints + manifest).
    with stage("build_bundle"):
        bundle = build_fleet_partition_bundle(
            staged,
            active_year=ACTIVE_YEAR,
            tables=["player_fantasy", "matchup"],
            output_dir=workdir / "bundle",
            import_run_id="bench",
            publish_sequence=1,
        )
        staged.close()
        report["bundle_mb"] = round(bundle.path.stat().st_size / (1024 * 1024), 1)

    # 4. Real server-side validation.
    with stage("validate_bundle"):
        extract_dir = workdir / "extracted"
        if extract_dir.exists():
            shutil.rmtree(extract_dir)
        extract_dir.mkdir()
        with tarfile.open(bundle.path, "r:gz") as tar:
            tar.extractall(extract_dir, filter="data")
        _, table_entries = fleet_merge.validate_fleet_manifest_shape(
            bundle.manifest,
            allowed_tables=set(registry),
            identity_keys={t: tuple(spec["primary_keys"]) for t, spec in registry.items()},
            expected_bundle_id=bundle.bundle_id,
            expected_bundle_hash=bundle.bundle_hash,
        )
        fleet_merge.validate_fleet_parquet_tables(
            bundle.manifest, table_entries, extract_dir, sha256_file=sha256_file
        )

    # 5. Real scoped merge under the Fly box's budget.
    with stage("apply_fleet_merge"):
        merge_conn = duckdb.connect(str(server_path))
        merge_conn.execute(f"SET memory_limit='{args.memory_limit}'")
        merge_conn.execute(f"SET threads={args.threads}")
        result = fleet_merge.apply_fleet_merge(merge_conn, bundle.manifest, extract_dir)
        report["merge_result"] = {k: v for k, v in result.items() if k != "bundle_hash"}

    with stage("checkpoint"):
        merge_conn.execute("CHECKPOINT")
        merge_conn.close()

    # 6. Spot verification.
    with stage("verify"):
        check = duckdb.connect(str(server_path), read_only=True)
        excluded_league = f"league_{args.leagues - 1}"
        excluded_max_week = check.execute(
            f"SELECT MAX(week) FROM public.player_fantasy WHERE db_name = '{excluded_league}' AND year = {ACTIVE_YEAR}"
        ).fetchone()[0]
        batch_max_week = check.execute(
            f"SELECT MAX(week) FROM public.player_fantasy WHERE db_name = 'league_0' AND year = {ACTIVE_YEAR}"
        ).fetchone()[0]
        prior_rows = check.execute(
            f"SELECT COUNT(*) FROM public.player_fantasy WHERE year < {ACTIVE_YEAR}"
        ).fetchone()[0]
        check.close()
        assert batch_max_week == args.active_weeks, batch_max_week
        assert excluded_max_week == args.active_weeks - 1, excluded_max_week
        expected_prior = args.leagues * args.prior_seasons * args.prior_weeks * args.player_rows_per_week
        assert prior_rows == expected_prior, (prior_rows, expected_prior)
        report["verify"] = {
            "batch_league_new_week_visible": True,
            "excluded_league_untouched": True,
            "prior_seasons_intact": True,
        }

    report["timings_seconds"] = timings
    report["fly_write_window_seconds"] = round(
        timings.get("apply_fleet_merge", 0) + timings.get("checkpoint", 0), 2
    )

    out_path = Path(args.out) if args.out else ARTIFACT_DIR / "bench_fleet_partition_publish.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("server_rows", "staged_rows", "server_db_mb", "bundle_mb", "timings_seconds", "fly_write_window_seconds")}, indent=2))
    print(f"[bench] report written to {out_path}")
    print(f"[bench] workdir (delete when done): {workdir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
