"""Reporter for validation_v2.

Two concerns:
1. CLI output (print_report) — grouped by page, hides suppressed/skipped.
2. MotherDuck storage (store_results) — persists ALL results including
   suppressed/skipped for historical tracking.
3. Debug samples (run_debug_samples) — runs bounded debug_sql queries.
"""

from __future__ import annotations

import sys
import uuid
from collections import defaultdict
from datetime import datetime, UTC

import duckdb

from multi_league.validation_v2.models import CheckResult, VALIDATOR_VERSION

# Severity ordering for display
_SEVERITY_ORDER = {"BLOCKER": 0, "ERROR": 1, "WARNING": 2, "INFO": 3}


def _is_actionable_failure(result: CheckResult) -> bool:
    return (
        not result.passed
        and not result.errored
        and not result.suppressed
        and not result.skipped
        and result.check.severity != "INFO"
    )


def _is_visible_info(result: CheckResult) -> bool:
    return (
        not result.passed
        and not result.errored
        and not result.suppressed
        and not result.skipped
        and result.check.severity == "INFO"
    )


def _is_transient_error(result: CheckResult) -> bool:
    return result.errored and result.transient_error and not result.suppressed and not result.skipped


# ---------------------------------------------------------------------------
# CLI output
# ---------------------------------------------------------------------------


def print_report(results: list[CheckResult], verbose: bool = False) -> None:
    """Print a CLI report of validation results.

    Non-verbose: summary per page (error/warning counts, top failing leagues).
    Verbose: every failed check with db_name, check_name, severity, fail_count.

    Suppressed and skipped checks are hidden from CLI output.

    Args:
        results: List of CheckResult objects (post-cascade).
        verbose: If True, show individual check failures.
    """
    # Filter to visible results only (not suppressed, not skipped)
    visible = [r for r in results if not r.suppressed and not r.skipped]

    # Collect stats
    total_checks = len(visible)
    total_failed = sum(1 for r in visible if _is_actionable_failure(r))
    total_passed = sum(1 for r in visible if r.passed)
    total_transient = sum(1 for r in visible if _is_transient_error(r))
    total_errored = sum(1 for r in visible if r.errored and not r.transient_error)
    total_info = sum(1 for r in visible if _is_visible_info(r))
    suppressed_count = sum(1 for r in results if r.suppressed)
    skipped_count = sum(1 for r in results if r.skipped)

    # Unique leagues with failures / errors
    failed_leagues = {r.db_name for r in visible if _is_actionable_failure(r)}
    info_leagues = {r.db_name for r in visible if _is_visible_info(r)}
    transient_leagues = {r.db_name for r in visible if _is_transient_error(r)}
    errored_leagues = {r.db_name for r in visible if r.errored and not r.transient_error}

    print("=" * 72)
    print("  VALIDATION REPORT")
    print("=" * 72)
    print(
        f"  Checks: {total_checks} visible "
        f"({total_passed} passed, {total_failed} failed, {total_errored} errored, {total_transient} transient)"
    )
    if suppressed_count:
        print(f"  Suppressed: {suppressed_count} (hidden, root cause elsewhere)")
    if skipped_count:
        print(f"  Skipped: {skipped_count} (dependency not met)")
    print(f"  Leagues with failures: {len(failed_leagues)}")
    if total_info:
        print(f"  Leagues with info: {len(info_leagues)}")
    if total_errored:
        print(f"  Leagues with errors: {len(errored_leagues)}")
    if total_transient:
        print(f"  Leagues with transient validator errors: {len(transient_leagues)}")
    print("=" * 72)

    if total_failed == 0 and total_errored == 0 and total_transient == 0 and total_info == 0:
        print("\n  All checks passed!\n")
        return

    # Group failures by page
    by_page: dict[str, list[CheckResult]] = defaultdict(list)
    for r in visible:
        if not r.passed and not r.errored:
            by_page[r.check.page].append(r)

    # Sort pages alphabetically
    for page in sorted(by_page.keys()):
        page_results = by_page[page]

        # Count severities
        sev_counts: dict[str, int] = defaultdict(int)
        for r in page_results:
            sev_counts[r.check.severity] += 1

        sev_summary = ", ".join(
            f"{count} {sev}" for sev, count in sorted(sev_counts.items(), key=lambda x: _SEVERITY_ORDER.get(x[0], 99))
        )

        # Top failing leagues (most failures first)
        league_fail_count: dict[str, int] = defaultdict(int)
        for r in page_results:
            league_fail_count[r.db_name] += r.fail_count

        top_leagues = sorted(league_fail_count.items(), key=lambda x: -x[1])[:5]
        league_str = ", ".join(f"{lg}({cnt})" for lg, cnt in top_leagues)

        print(f"\n  [{page}] {sev_summary}")
        print(f"    Top leagues: {league_str}")

        if verbose:
            # Group by check, show top 5 leagues per check (highest fail_count)
            by_check: dict[str, list[CheckResult]] = defaultdict(list)
            for r in page_results:
                by_check[r.check.name].append(r)

            # Sort checks by severity then name
            sorted_checks = sorted(
                by_check.items(),
                key=lambda item: (
                    _SEVERITY_ORDER.get(item[1][0].check.severity, 99),
                    item[0],
                ),
            )

            for check_name, check_results in sorted_checks:
                sev = check_results[0].check.severity
                total_leagues = len(check_results)
                # Top 5 by fail_count
                top5 = sorted(check_results, key=lambda r: -r.fail_count)[:5]
                top5_str = ", ".join(f"{r.db_name}({r.fail_count})" for r in top5)
                extra = f" +{total_leagues - 5} more" if total_leagues > 5 else ""
                print(f"    {sev:8s} {check_name:45s} {top5_str}{extra}")

    # Errored checks section
    if total_errored:
        errored_by_page: dict[str, list[CheckResult]] = defaultdict(list)
        for r in visible:
            if r.errored and not r.transient_error:
                errored_by_page[r.check.page].append(r)

        for page in sorted(errored_by_page.keys()):
            page_errored = errored_by_page[page]
            print(f"\n  [{page}] {len(page_errored)} ERRORED")
            # Deduplicate by check name, preserving first error message seen
            seen: dict[str, str | None] = {}
            for r in page_errored:
                if r.check.name not in seen:
                    seen[r.check.name] = r.error_message
            for check_name, msg in seen.items():
                truncated = (msg[:80] + "...") if msg and len(msg) > 80 else (msg or "")
                print(f"    {check_name}: {truncated}")

    if total_transient:
        transient_by_page: dict[str, list[CheckResult]] = defaultdict(list)
        for r in visible:
            if _is_transient_error(r):
                transient_by_page[r.check.page].append(r)

        for page in sorted(transient_by_page.keys()):
            page_transient = transient_by_page[page]
            print(f"\n  [{page}] {len(page_transient)} TRANSIENT")
            seen: dict[str, str | None] = {}
            for r in page_transient:
                if r.check.name not in seen:
                    seen[r.check.name] = r.error_message
            for check_name, msg in seen.items():
                truncated = (msg[:80] + "...") if msg and len(msg) > 80 else (msg or "")
                print(f"    {check_name}: {truncated}")

    print()


# ---------------------------------------------------------------------------
# MotherDuck storage
# ---------------------------------------------------------------------------

_RESULTS_DDL = """
CREATE TABLE IF NOT EXISTS {prefix}validation_results (
    run_id VARCHAR NOT NULL,
    run_timestamp TIMESTAMP NOT NULL,
    validator_version VARCHAR NOT NULL,
    db_name VARCHAR NOT NULL,
    check_name VARCHAR NOT NULL,
    page VARCHAR NOT NULL,
    severity VARCHAR NOT NULL,
    passed BOOLEAN NOT NULL,
    fail_count INTEGER NOT NULL DEFAULT 0,
    threshold INTEGER NOT NULL DEFAULT 0,
    skipped BOOLEAN NOT NULL DEFAULT FALSE,
    suppressed BOOLEAN NOT NULL DEFAULT FALSE,
    suppressed_by VARCHAR,
    fix_action VARCHAR,
    description VARCHAR,
    errored BOOLEAN NOT NULL DEFAULT FALSE,
    error_message VARCHAR
)
"""

_SUMMARY_DDL = """
CREATE TABLE IF NOT EXISTS {prefix}validation_summary (
    run_id VARCHAR NOT NULL,
    run_timestamp TIMESTAMP NOT NULL,
    validator_version VARCHAR NOT NULL,
    total_checks INTEGER NOT NULL,
    total_passed INTEGER NOT NULL,
    total_failed INTEGER NOT NULL,
    total_suppressed INTEGER NOT NULL,
    total_skipped INTEGER NOT NULL,
    leagues_checked INTEGER NOT NULL,
    leagues_with_failures INTEGER NOT NULL,
    blocker_count INTEGER NOT NULL DEFAULT 0,
    error_count INTEGER NOT NULL DEFAULT 0,
    warning_count INTEGER NOT NULL DEFAULT 0,
    info_count INTEGER NOT NULL DEFAULT 0
)
"""


def _escape_sql_val(v) -> str:
    """Escape a Python value for inline SQL."""
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "TRUE" if v else "FALSE"
    if isinstance(v, int | float):
        return str(v)
    s = str(v).replace("'", "''")
    return f"'{s}'"


def store_results(
    conn,
    results: list[CheckResult],
    run_id: str | None = None,
    run_timestamp: datetime | None = None,
    validator_version: str | None = None,
    table_prefix: str = "___ops.fleet_health.",
) -> str:
    """INSERT validation results into fleet_health tables.

    Stores ALL results including suppressed and skipped for historical tracking.
    Supports both native DuckDB connections and FlyConnection (HTTP API).

    Args:
        conn: DuckDB connection or FlyConnection.
        results: List of CheckResult objects (post-cascade).
        run_id: Unique run identifier (generated if None).
        run_timestamp: Timestamp of the run (now if None).
        validator_version: Version string (from models if None).
        table_prefix: Table prefix for DDL and INSERTs.

    Returns:
        The run_id used.
    """
    if run_id is None:
        run_id = str(uuid.uuid4())[:8]
    if run_timestamp is None:
        run_timestamp = datetime.now(UTC)
    if validator_version is None:
        validator_version = VALIDATOR_VERSION

    ts_str = run_timestamp.strftime("%Y-%m-%d %H:%M:%S")

    # Detect FlyConnection (has execute_rw method, no executemany)
    is_fly = hasattr(conn, "execute_rw")

    def exec_write(sql: str):
        if is_fly:
            conn.execute_rw(sql)
        else:
            conn.execute(sql)

    # Ensure schema exists
    schema_parts = table_prefix.rstrip(".").rsplit(".", 1)
    if len(schema_parts) == 2:
        db_part, schema_part = schema_parts
        try:
            exec_write(f"CREATE SCHEMA IF NOT EXISTS {db_part}.{schema_part}")
        except Exception:
            pass
    else:
        try:
            exec_write(f"CREATE SCHEMA IF NOT EXISTS {schema_parts[0]}")
        except Exception:
            pass

    # Create tables
    exec_write(_RESULTS_DDL.format(prefix=table_prefix))
    exec_write(_SUMMARY_DDL.format(prefix=table_prefix))

    # Insert results — batch as VALUES rows in chunks
    # (no temp tables, no executemany, no parameterized queries — works over HTTP)
    BATCH_SIZE = 500
    for i in range(0, len(results), BATCH_SIZE):
        batch = results[i : i + BATCH_SIZE]
        rows_sql = ",\n".join(
            f"({_escape_sql_val(run_id)}, {_escape_sql_val(ts_str)}, "
            f"{_escape_sql_val(validator_version)}, {_escape_sql_val(r.db_name)}, "
            f"{_escape_sql_val(r.check.name)}, {_escape_sql_val(r.check.page)}, "
            f"{_escape_sql_val(r.check.severity)}, {_escape_sql_val(r.passed)}, "
            f"{_escape_sql_val(r.fail_count)}, {_escape_sql_val(r.check.threshold)}, "
            f"{_escape_sql_val(r.skipped)}, {_escape_sql_val(r.suppressed)}, "
            f"{_escape_sql_val(r.suppressed_by)}, {_escape_sql_val(r.check.fix_action)}, "
            f"{_escape_sql_val(r.check.description)}, {_escape_sql_val(r.errored)}, "
            f"{_escape_sql_val(r.error_message)})"
            for r in batch
        )
        exec_write(
            f"INSERT INTO {table_prefix}validation_results "
            f"(run_id, run_timestamp, validator_version, db_name, check_name, page, severity, "
            f"passed, fail_count, threshold, skipped, suppressed, suppressed_by, fix_action, "
            f"description, errored, error_message) VALUES\n{rows_sql}"
        )
        print(
            f"  Stored {min(i + BATCH_SIZE, len(results))}/{len(results)} results...",
            file=sys.stderr,
        )

    # Compute summary
    total = len(results)
    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if _is_actionable_failure(r))
    suppressed = sum(1 for r in results if r.suppressed)
    skipped = sum(1 for r in results if r.skipped)
    all_leagues = {r.db_name for r in results}
    failed_leagues = {r.db_name for r in results if _is_actionable_failure(r)}

    sev_counts = defaultdict(int)
    for r in results:
        if not r.passed and not r.suppressed and not r.skipped and not r.errored:
            sev_counts[r.check.severity] += 1

    exec_write(
        f"INSERT INTO {table_prefix}validation_summary VALUES "
        f"({_escape_sql_val(run_id)}, {_escape_sql_val(ts_str)}, "
        f"{_escape_sql_val(validator_version)}, {total}, {passed}, {failed}, "
        f"{suppressed}, {skipped}, {len(all_leagues)}, {len(failed_leagues)}, "
        f"{sev_counts.get('BLOCKER', 0)}, {sev_counts.get('ERROR', 0)}, "
        f"{sev_counts.get('WARNING', 0)}, {sev_counts.get('INFO', 0)})"
    )

    return run_id


# ---------------------------------------------------------------------------
# Debug samples
# ---------------------------------------------------------------------------


def run_debug_samples(
    conn: duckdb.DuckDBPyConnection,
    results: list[CheckResult],
    sample_size: int,
    table_prefix: str = "___leagues.public.",
) -> None:
    """Run debug_sql queries for failed checks, printing sample rows.

    Only runs for checks that have debug_sql defined and actually failed.
    Each query is bounded by ROW_NUMBER() PARTITION BY db_name to limit output.

    Args:
        conn: DuckDB connection.
        results: List of CheckResult objects.
        sample_size: Max rows per league to show.
        table_prefix: Table prefix for SQL substitution.
    """
    # Deduplicate: only run each check's debug SQL once
    seen_checks: set[str] = set()
    failed_with_debug: list[CheckResult] = []

    for r in results:
        if (
            not r.passed
            and not r.suppressed
            and not r.skipped
            and r.check.debug_sql
            and r.check.name not in seen_checks
        ):
            seen_checks.add(r.check.name)
            failed_with_debug.append(r)

    if not failed_with_debug:
        print("\n  No debug samples available (no failed checks with debug_sql).\n")
        return

    # Collect failed leagues per check
    failed_leagues_by_check: dict[str, set[str]] = defaultdict(set)
    for r in results:
        if not r.passed and not r.suppressed and not r.skipped:
            failed_leagues_by_check[r.check.name].add(r.db_name)

    print("\n" + "=" * 72)
    print("  DEBUG SAMPLES")
    print("=" * 72)

    for r in failed_with_debug:
        leagues = failed_leagues_by_check.get(r.check.name, set())
        if not leagues:
            continue

        league_list_sql = ", ".join(f"'{lg}'" for lg in sorted(leagues))

        # Build bounded query using ROW_NUMBER
        inner_sql = r.check.debug_sql.format(
            table=f"{table_prefix}{r.check.table}",
            table_prefix=table_prefix,
            league_list=league_list_sql,
        )

        bounded_sql = (
            f"SELECT * FROM ("
            f"  SELECT *, ROW_NUMBER() OVER (PARTITION BY db_name) AS _rn "
            f"  FROM ({inner_sql}) _debug"
            f") WHERE _rn <= {sample_size}"
        )

        print(f"\n  [{r.check.name}] {r.check.description}")
        print(f"  Leagues: {', '.join(sorted(leagues)[:10])}")
        print(f"  {'─' * 60}")

        try:
            rows = conn.execute(bounded_sql).fetchall()
            columns = [desc[0] for desc in conn.description]
            # Remove the _rn column from display
            rn_idx = columns.index("_rn") if "_rn" in columns else None

            for row in rows:
                vals = []
                for i, (col, val) in enumerate(zip(columns, row)):
                    if i == rn_idx:
                        continue
                    vals.append(f"{col}={val}")
                print(f"    {', '.join(vals)}")

            if not rows:
                print("    (no rows returned)")
        except Exception as e:
            print(f"    ERROR running debug SQL: {e}")

    print()
