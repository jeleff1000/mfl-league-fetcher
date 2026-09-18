"""Rebuild the five damaged aggregate tables from intact persisted facts.

This is an offline storage-recovery helper. It deliberately calls only the
existing canonical aggregators for the five affected tables and validates only
those five outputs. Provider data is never fetched and no source or unrelated
derived table is rewritten.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Callable

_REPO_ROOT = Path(__file__).resolve().parents[1]
_PIPELINE_ROOT = _REPO_ROOT / "fantasy_football_data_scripts"
if str(_PIPELINE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PIPELINE_ROOT))

import duckdb

from multi_league.core.aggregate_ddl import (
    AGGREGATE_TABLE_SPECS,
    create_named_aggregate_table_sql,
)
from multi_league.transformations.aggregation.aggregate_fantasy_context import (
    aggregate_fantasy_season,
    aggregate_fantasy_season_all,
)
from multi_league.transformations.aggregation.aggregate_matchup_context import (
    aggregate_matchup_h2h,
)
from multi_league.transformations.aggregation.aggregate_standings import (
    aggregate_standings,
)
from multi_league.transformations.aggregation.aggregation_utils import (
    replace_scoped_aggregate_table_from_dataframe,
)
from multi_league.transformations.aggregation.homepage_summary import (
    compute_manager_rankings,
)


TARGET_TABLES = (
    "homepage_manager_rankings",
    "matchup_h2h_career",
    "player_fantasy_season",
    "player_fantasy_season_all",
    "standings_by_year",
)

logger = logging.getLogger(__name__)
_ATTACH_LOCK = threading.Lock()


def quarantine_corrupt_targets(conn) -> dict[str, str]:
    """Atomically move damaged targets aside and install clean canonical shells.

    The quarantined objects are deliberately retained: dropping them would read
    the damaged storage block. This helper is only for an isolated recovery
    volume and never runs as part of a normal league update.
    """
    quarantined = {
        table: f"__corrupt_recovery_{table}" for table in TARGET_TABLES
    }
    conn.execute("BEGIN TRANSACTION")
    try:
        for table in TARGET_TABLES:
            replacement = f"__repair_recovery_{table}"
            quarantine = quarantined[table]
            conn.execute(
                create_named_aggregate_table_sql(
                    "___leagues", table, replacement
                )
            )
            conn.execute(
                f'ALTER TABLE public.{_quote_identifier(table)} '
                f'RENAME TO {_quote_identifier(quarantine)}'
            )
            conn.execute(
                f'ALTER TABLE public.{_quote_identifier(replacement)} '
                f'RENAME TO {_quote_identifier(table)}'
            )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise
    return quarantined


def _public_table_names(conn) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT table_name FROM duckdb_tables() "
            "WHERE schema_name = 'public' AND NOT internal"
        ).fetchall()
    }


def quarantine_or_resume_corrupt_targets(conn) -> dict[str, str]:
    """Create the exact quarantine once, or resume when all five already exist."""
    expected = {
        table: f"__corrupt_recovery_{table}" for table in TARGET_TABLES
    }
    present = set(expected.values()) & _public_table_names(conn)
    if present == set(expected.values()):
        return expected
    if present:
        raise RuntimeError(
            "partial corrupt-table quarantine is unsafe: "
            f"found={sorted(present)}, expected={sorted(expected.values())}"
        )
    return quarantine_corrupt_targets(conn)


def _quote_identifier(value: str) -> str:
    return '"' + value.replace('"', '""') + '"'


def discover_leagues(conn) -> list[str]:
    """Return every league represented in the two persisted fact tables."""
    rows = conn.execute(
        "SELECT DISTINCT db_name FROM ("
        "SELECT db_name FROM public.matchup "
        "UNION ALL SELECT db_name FROM public.player_fantasy"
        ") WHERE db_name IS NOT NULL AND TRIM(db_name) <> '' ORDER BY db_name"
    ).fetchall()
    return [str(row[0]) for row in rows]


def reaggregate_one_league(
    conn,
    db_name: str,
    *,
    progress_callback: Callable[[dict], None] | None = None,
) -> dict[str, int]:
    """Rebuild exactly the five affected outputs for one persisted chain."""
    def run_stage(stage: str, operation: Callable[[], object]) -> object:
        started = time.monotonic()
        if progress_callback:
            progress_callback(
                {
                    "stage": "reaggregating",
                    "current_db_name": db_name,
                    "current_table": stage,
                }
            )
        logger.info(
            "derived_recovery league=%s table=%s status=started",
            db_name,
            stage,
        )
        try:
            result = operation()
        except Exception as exc:
            logger.exception(
                "derived_recovery league=%s table=%s status=failed elapsed_seconds=%.3f",
                db_name,
                stage,
                time.monotonic() - started,
            )
            raise RuntimeError(
                f"{stage} failed: {type(exc).__name__}: {exc}"
            ) from exc
        logger.info(
            "derived_recovery league=%s table=%s status=completed elapsed_seconds=%.3f",
            db_name,
            stage,
            time.monotonic() - started,
        )
        return result

    years = run_stage(
        "matchup_years",
        lambda: [
            int(row[0])
            for row in conn.execute(
                "SELECT DISTINCT TRY_CAST(year AS INTEGER) FROM public.matchup "
                "WHERE db_name = ? AND TRY_CAST(year AS INTEGER) IS NOT NULL ORDER BY 1",
                [db_name],
            ).fetchall()
        ],
    )

    run_stage("player_fantasy_season", lambda: aggregate_fantasy_season(conn, db_name))
    run_stage(
        "player_fantasy_season_all",
        lambda: aggregate_fantasy_season_all(conn, db_name),
    )
    run_stage(
        "matchup_h2h_career",
        lambda: aggregate_matchup_h2h(conn, db_name, season_years=set()),
    )
    run_stage("standings_by_year", lambda: aggregate_standings(conn, db_name, years))

    def rebuild_homepage_rankings() -> None:
        rankings = compute_manager_rankings(conn, db_name)
        replace_scoped_aggregate_table_from_dataframe(
            conn,
            db_name,
            "homepage_manager_rankings",
            rankings,
        )

    run_stage("homepage_manager_rankings", rebuild_homepage_rankings)

    return {
        table: int(
            conn.execute(
                f"SELECT COUNT(*) FROM public.{_quote_identifier(table)} WHERE db_name = ?",
                [db_name],
            ).fetchone()[0]
        )
        for table in TARGET_TABLES
    }


def validate_targets(conn) -> dict[str, dict[str, int]]:
    """Validate only the five rebuilt tables and their canonical keys."""
    results: dict[str, dict[str, int]] = {}
    for table in TARGET_TABLES:
        ref = f"public.{_quote_identifier(table)}"
        rows = int(conn.execute(f"SELECT COUNT(*) FROM {ref}").fetchone()[0])
        keys = ("db_name", *AGGREGATE_TABLE_SPECS[table].primary_key)
        key_sql = ", ".join(_quote_identifier(key) for key in keys)
        distinct_keys = int(
            conn.execute(
                f"SELECT COUNT(*) FROM (SELECT {key_sql} FROM {ref} GROUP BY {key_sql})"
            ).fetchone()[0]
        )
        if rows <= 0:
            raise RuntimeError(f"{table} is empty after reaggregation")
        if rows != distinct_keys:
            raise RuntimeError(
                f"{table} has duplicate canonical keys: rows={rows}, distinct={distinct_keys}"
            )
        results[table] = {"rows": rows, "distinct_keys": distinct_keys}
    return results


def reaggregate_all(
    conn,
    *,
    db_names: list[str] | None = None,
    progress_callback: Callable[[dict], None] | None = None,
    checkpoint: bool = True,
) -> dict:
    """Reaggregate the five targets for all leagues, one transaction at a time."""
    leagues = sorted(set(db_names or discover_leagues(conn)))
    completed = 0

    def report(stage: str, current_db_name: str | None = None) -> None:
        if progress_callback:
            progress_callback(
                {
                    "stage": stage,
                    "completed": completed,
                    "total": len(leagues),
                    "current_db_name": current_db_name,
                }
            )

    for db_name in leagues:
        report("reaggregating", db_name)
        conn.execute("BEGIN TRANSACTION")
        try:
            reaggregate_one_league(conn, db_name)
            conn.execute("COMMIT")
            completed += 1
        except Exception as exc:
            conn.execute("ROLLBACK")
            raise RuntimeError(
                json.dumps(
                    {"completed": completed, "failed_db_name": db_name, "error": str(exc)},
                    sort_keys=True,
                )
            ) from exc
    report("reaggregating")
    report("validating")
    targets = validate_targets(conn)
    if checkpoint:
        conn.execute("CHECKPOINT")
    report("complete")
    return {"leagues": completed, "targets": targets}


def clear_recovery_targets(conn) -> None:
    """Remove partial retry output from only the five clean recovery tables."""
    conn.execute("BEGIN TRANSACTION")
    try:
        for table in TARGET_TABLES:
            conn.execute(f"DELETE FROM public.{_quote_identifier(table)}")
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise


def reaggregate_parallel(
    database_path: Path,
    *,
    db_names: list[str],
    ops_path: Path | None = None,
    ops_nfl_path: Path | None = None,
    max_workers: int = 8,
    progress_callback: Callable[[dict], None] | None = None,
    connection_factory: Callable[[Path], object] | None = None,
) -> dict[str, int]:
    """Run canonical scoped aggregators concurrently on disjoint league keys."""
    leagues = sorted(set(db_names))
    worker_count = max(1, min(int(max_workers), len(leagues) or 1))
    completed = 0
    progress_lock = threading.Lock()

    def report(current_db_name: str | None) -> None:
        if progress_callback:
            progress_callback(
                {
                    "stage": "reaggregating",
                    "completed": completed,
                    "total": len(leagues),
                    "current_db_name": current_db_name,
                }
            )

    def run_shard(shard: list[str]) -> list[str]:
        nonlocal completed
        conn = connection_factory(database_path) if connection_factory else duckdb.connect(str(database_path))
        try:
            if connection_factory is None:
                conn.execute("SET threads=1")
                if ops_nfl_path:
                    _attach_if_present(conn, ops_nfl_path, "___ops_nfl")
                if ops_path:
                    _attach_if_present(conn, ops_path, "___ops")
            for db_name in shard:
                conn.execute("BEGIN TRANSACTION")
                try:
                    reaggregate_one_league(
                        conn,
                        db_name,
                        progress_callback=progress_callback,
                    )
                    conn.execute("COMMIT")
                except Exception as exc:
                    conn.execute("ROLLBACK")
                    raise RuntimeError(
                        f"{db_name}: scoped reaggregation failed: {type(exc).__name__}: {exc}"
                    ) from exc
                with progress_lock:
                    completed += 1
                    report(db_name if completed < len(leagues) else None)
            return shard
        finally:
            conn.close()

    report(None)
    shards = [leagues[index::worker_count] for index in range(worker_count)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = {executor.submit(run_shard, shard): shard for shard in shards if shard}
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                for pending in futures:
                    pending.cancel()
                raise RuntimeError(
                    json.dumps(
                        {"completed": completed, "error": str(exc)},
                        sort_keys=True,
                    )
                ) from exc
    return {"leagues": completed}


def _attach_if_present(conn, path: Path, catalog: str) -> None:
    if path.is_file():
        # DuckDB coordinates attached catalog names at the database-instance
        # level. Parallel connections can otherwise both observe the catalog
        # as absent and race on ATTACH, making the loser fail with "already
        # exists" before any league work starts.
        with _ATTACH_LOCK:
            attached = conn.execute(
                "SELECT 1 FROM duckdb_databases() WHERE database_name = ? LIMIT 1",
                [catalog],
            ).fetchone()
            if attached:
                return
            conn.execute(
                f"ATTACH '{path.as_posix().replace(chr(39), chr(39) * 2)}' "
                f"AS {_quote_identifier(catalog)} (READ_ONLY)"
            )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--ops", type=Path)
    parser.add_argument("--ops-nfl", type=Path)
    parser.add_argument("--db-name", action="append", default=[])
    parser.add_argument("--quarantine-corrupt-targets-only", action="store_true")
    args = parser.parse_args()

    conn = duckdb.connect(str(args.database.resolve()))
    try:
        if args.ops_nfl:
            _attach_if_present(conn, args.ops_nfl.resolve(), "___ops_nfl")
        if args.ops:
            _attach_if_present(conn, args.ops.resolve(), "___ops")
        if args.quarantine_corrupt_targets_only:
            result = {"quarantined": quarantine_corrupt_targets(conn)}
        else:
            result = reaggregate_all(conn, db_names=args.db_name or None)
    finally:
        conn.close()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
