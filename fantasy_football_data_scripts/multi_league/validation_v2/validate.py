"""CLI entry point for validation_v2.

Orchestrates passes 1-6 of the fleet-wide validator:
  Pass 1:   Load settings + manifest
  Pass 1.5: Manifest validation
  Pass 2:   Completeness checks
  Pass 3:   Single-table batches (core -> quality -> analytics)
  Pass 4:   Cross-table + sql_full system checks
  Pass 5:   Aggregate validation (unused tables / homepage)
  Pass 6:   Super table health

Usage:
    python -m multi_league.validation_v2                     # all leagues
    python -m multi_league.validation_v2 --db the_league     # one league
    python -m multi_league.validation_v2 --page matchups -v  # one page, verbose
    python -m multi_league.validation_v2 --store --ci        # CI mode with storage
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import json
import urllib.error
import urllib.request

import duckdb

from multi_league.validation_v2.models import Check, CheckResult, Manifest, VALIDATOR_VERSION
from multi_league.validation_v2.executor import (
    group_checks,
    build_batch_sql,
    run_batch,
    run_sql_full,
    DEFAULT_TABLE_PREFIX,
)
from multi_league.validation_v2.manifest import load_manifest
from multi_league.validation_v2.checks import (
    ALL_CHECKS,
    completeness_checks,
    manifest_checks,
    system_checks,
    super_table_checks,
)
from multi_league.validation_v2.cascade import apply_cascade
from multi_league.validation_v2.reporter import print_report, store_results, run_debug_samples


class _FlyQueryResult:
    """Mimics DuckDB query result for fetchall() + description."""

    def __init__(self, rows: list[dict]):
        self._rows = rows
        if rows:
            self._columns = list(rows[0].keys())
        else:
            self._columns = []

    @property
    def description(self):
        return [(c,) for c in self._columns]

    def fetchall(self):
        return [tuple(r[c] for c in self._columns) for r in self._rows]


class FlyConnection:
    """DuckDB-compatible connection that proxies queries to the Fly HTTP API."""

    MAX_RETRIES = int(os.environ.get("VALIDATION_FLY_MAX_RETRIES", "10"))
    RETRY_BASE_DELAY = 1
    RETRY_MAX_DELAY = int(os.environ.get("VALIDATION_FLY_RETRY_MAX_DELAY", "30"))
    RETRY_STATUS = {429, 500, 502, 503, 504}

    def __init__(self, server_url: str, read_token: str, admin_token: str | None = None):
        self._url = server_url.rstrip("/")
        self._read_token = read_token
        self._admin_token = admin_token
        self.description = None

    @classmethod
    def _retry_delay(cls, attempt: int, headers=None) -> float:
        retry_after = headers.get("Retry-After") if headers is not None else None
        if retry_after:
            try:
                return min(float(retry_after), cls.RETRY_MAX_DELAY)
            except (TypeError, ValueError):
                pass
        return min(cls.RETRY_BASE_DELAY * (2**attempt), cls.RETRY_MAX_DELAY)

    def _open_json(self, req: urllib.request.Request, *, timeout: int) -> list[dict]:
        last_error: Exception | None = None
        for attempt in range(self.MAX_RETRIES):
            try:
                with urllib.request.urlopen(req, timeout=timeout) as resp:
                    return json.loads(resp.read())
            except urllib.error.HTTPError as e:
                if e.code not in self.RETRY_STATUS or attempt >= self.MAX_RETRIES - 1:
                    raise
                last_error = e
                time.sleep(self._retry_delay(attempt, e.headers))
            except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
                if attempt >= self.MAX_RETRIES - 1:
                    raise
                last_error = e
                time.sleep(self._retry_delay(attempt))
        raise RuntimeError(f"Fly validation query exhausted retries: {last_error}")

    def execute(self, sql: str) -> _FlyQueryResult:
        database = "___ops" if "___ops" in sql else "___leagues"
        data = json.dumps({"sql": sql, "database": database}).encode()
        req = urllib.request.Request(
            f"{self._url}/query",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._read_token}",
            },
        )
        try:
            rows = self._open_json(req, timeout=120)
        except urllib.error.HTTPError as e:
            body = ""
            try:
                body = e.read().decode("utf-8", errors="replace")
            except Exception:
                pass
            # Translate HTTP 500 with "Catalog Error" to duckdb.CatalogException
            # so run_sql_full can handle missing tables gracefully
            if e.code == 500 and "Catalog Error" in body:
                import duckdb

                raise duckdb.CatalogException(body) from e
            if body:
                raise RuntimeError(f"HTTP Error {e.code}: {body}") from e
            raise
        result = _FlyQueryResult(rows)
        self.description = result.description
        return result

    def execute_rw(self, sql: str) -> _FlyQueryResult:
        if not self._admin_token:
            raise RuntimeError("Admin token not set — cannot write")
        data = json.dumps({"sql": sql, "database": "___ops"}).encode()
        req = urllib.request.Request(
            f"{self._url}/query-rw",
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {self._admin_token}",
            },
        )
        rows = self._open_json(req, timeout=120)
        return _FlyQueryResult(rows)

    def close(self):
        pass


# Batch group execution order
_BATCH_ORDER = {"core": 0, "quality": 1, "analytics": 2}

_TRANSIENT_VALIDATION_MARKERS = (
    "bad gateway",
    "connection reset",
    "duckdb server unavailable",
    "empty response body",
    "gateway timeout",
    "http error 429",
    "http error 502",
    "http error 503",
    "http error 504",
    "network failure",
    "operation was aborted",
    "ops_writing",
    "query failed (502)",
    "query failed (503)",
    "query failed (504)",
    "query service is busy",
    "query timed out",
    "service unavailable",
    "starting",
    "startup_failed",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "too many requests",
    "urlopen error",
)


def _is_transient_validation_error(error: Exception | str) -> bool:
    """Return True for Fly/API availability errors that are not data failures."""
    message = str(error).lower()
    return any(marker in message for marker in _TRANSIENT_VALIDATION_MARKERS)


def _errored_result(check: Check, db_name: str, error_message: str) -> CheckResult:
    transient = _is_transient_validation_error(error_message)
    return CheckResult(
        check=check,
        db_name=db_name,
        fail_count=0,
        passed=False,
        errored=True,
        transient_error=transient,
        error_message=error_message,
    )


def _ci_failure_results(results: list[CheckResult]) -> list[CheckResult]:
    """Return hard CI failures, excluding transient validator infrastructure errors."""
    return [
        r
        for r in results
        if not r.passed
        and not r.suppressed
        and not r.skipped
        and not (r.errored and r.transient_error)
        and r.check.severity in ("BLOCKER", "ERROR")
    ]


def _filter_checks_by_page(checks: list[Check], page: str | None) -> list[Check]:
    """Filter checks to a specific page if requested."""
    if page is None:
        return checks
    return [c for c in checks if c.page == page]


def run_checks(
    conn: duckdb.DuckDBPyConnection,
    checks: list[Check],
    manifest: Manifest,
    table_prefix: str = DEFAULT_TABLE_PREFIX,
) -> list[CheckResult]:
    """Run a list of sql_full checks one at a time.

    Handles skip-list resolution for checks with depends_on.

    Args:
        conn: DuckDB connection.
        checks: List of Check objects (all should have sql_full).
        manifest: Manifest with league lists and skip data.
        table_prefix: SQL prefix for table references.

    Returns:
        List of CheckResult objects.
    """
    results: list[CheckResult] = []

    for check in checks:
        current_check_results: list[CheckResult] = []

        # Resolve skip set from dependencies
        skip_set = manifest.skip_set_for(check.depends_on)

        # Get leagues in scope for this check's feature
        leagues = manifest.leagues_for_feature(check.feature)

        if not leagues:
            continue

        # Mark skipped leagues
        skipped_results: list[CheckResult] = []
        for lg in leagues:
            if lg in skip_set:
                skipped_results.append(
                    CheckResult(
                        check=check,
                        db_name=lg,
                        fail_count=0,
                        passed=True,
                        skipped=True,
                    )
                )
        current_check_results.extend(skipped_results)

        # Run the check for non-skipped leagues
        # We need to temporarily adjust manifest for the sql_full executor
        active_leagues = [lg for lg in leagues if lg not in skip_set]

        if active_leagues:
            # Determine chunk size: chunked checks split into small batches
            # to avoid massive self-joins timing out over HTTP
            cs = check.chunk_size if check.chunked else len(active_leagues)
            chunk_size = cs if check.chunked and len(active_leagues) > cs else len(active_leagues)
            chunks = [active_leagues[i : i + chunk_size] for i in range(0, len(active_leagues), chunk_size)]

            for chunk_idx, chunk_leagues in enumerate(chunks):
                if check.chunked and len(chunks) > 1:
                    print(
                        f"    chunk {chunk_idx + 1}/{len(chunks)} " f"({len(chunk_leagues)} leagues)",
                        file=sys.stderr,
                    )

                # Create a scoped manifest for this chunk
                chunk_set = set(chunk_leagues)
                scoped_manifest = Manifest(
                    all_leagues=chunk_leagues,
                    median_leagues=[lg for lg in manifest.median_leagues if lg in chunk_set],
                    consolation_leagues=[lg for lg in manifest.consolation_leagues if lg in chunk_set],
                    keeper_leagues=[lg for lg in manifest.keeper_leagues if lg in chunk_set],
                    dynasty_leagues=[lg for lg in manifest.dynasty_leagues if lg in chunk_set],
                    faab_leagues=[lg for lg in manifest.faab_leagues if lg in chunk_set],
                    espn_leagues=[lg for lg in manifest.espn_leagues if lg in chunk_set],
                    yahoo_leagues=[lg for lg in manifest.yahoo_leagues if lg in chunk_set],
                    sleeper_leagues=[lg for lg in manifest.sleeper_leagues if lg in chunk_set],
                    multi_year_leagues=[lg for lg in manifest.multi_year_leagues if lg in chunk_set],
                    full_import_leagues=[lg for lg in manifest.full_import_leagues if lg in chunk_set],
                    sim_leagues=[lg for lg in manifest.sim_leagues if lg in chunk_set],
                    settings=manifest.settings,
                    skip_lists=manifest.skip_lists,
                )
                try:
                    check_results = run_sql_full(conn, check, scoped_manifest, table_prefix)
                    results.extend(check_results)
                    current_check_results.extend(check_results)
                except Exception as e:
                    error_msg = str(e)
                    label = "TRANSIENT" if _is_transient_validation_error(error_msg) else "WARNING"
                    print(f"  {label}: Check {check.name} failed with error: {error_msg}", file=sys.stderr)
                    # Mark standalone query failures as execution errors, not
                    # data failures. This keeps timeout/binder/server issues
                    # out of the failure counts while still surfacing them.
                    for lg in chunk_leagues:
                        errored_result = _errored_result(check, lg, error_msg)
                        results.append(errored_result)
                        current_check_results.append(errored_result)

        results.extend(skipped_results)
        # Make same-pass dependencies work for sql_full checks. Several
        # aggregate drift checks depend on earlier "table exists" checks that
        # are also sql_full checks in Pass 4; without this incremental update,
        # they still run and report noisy derivative failures.
        manifest.update_skip_lists(current_check_results)

    return results


def _run_single_batch_with_error_handling(
    conn,
    table: str,
    feature: str | None,
    batch_group: str,
    checks: list[Check],
    manifest: Manifest,
    skip_sets: dict[str, set[str]],
    table_prefix: str,
) -> list[CheckResult]:
    """Execute one batch and return results. If the batch raises, synthesize
    errored CheckResult for every check × every league in scope so checks are
    never silently dropped from the report."""
    try:
        sql = build_batch_sql(
            checks=checks,
            table=table,
            manifest=manifest,
            feature=feature,
            skip_sets=skip_sets,
            table_prefix=table_prefix,
        )
        return run_batch(conn, sql, checks, manifest, skip_sets)
    except Exception as e:
        error_msg = str(e)
        label = "TRANSIENT" if _is_transient_validation_error(error_msg) else "ERROR"
        print(
            f"  {label}: Batch ({table}, {feature}, {batch_group}) failed — "
            f"marking {len(checks)} check(s) as errored across scope: {error_msg}",
            file=sys.stderr,
        )
        errored_results: list[CheckResult] = []
        for check in checks:
            scope_leagues = manifest.leagues_for_feature(check.feature or feature)
            for db_name in scope_leagues:
                errored_results.append(_errored_result(check, db_name, error_msg))
        return errored_results


def run_all_batches(
    conn: duckdb.DuckDBPyConnection,
    checks: list[Check],
    manifest: Manifest,
    table_prefix: str = DEFAULT_TABLE_PREFIX,
) -> list[CheckResult]:
    """Run all sql_expr checks in batched groups.

    Groups checks by (table, feature, batch_group), runs core first,
    then quality, then analytics. Skip lists are updated after core
    groups complete.

    Args:
        conn: DuckDB connection.
        checks: List of ALL Check objects.
        manifest: Manifest with league lists and skip data.
        table_prefix: SQL prefix for table references.

    Returns:
        List of CheckResult objects from all batches.
    """
    # Filter to sql_expr checks only
    expr_checks = [c for c in checks if c.sql_expr is not None]
    if not expr_checks:
        return []

    # Group checks
    groups = group_checks(expr_checks)

    # Sort groups by batch_group order
    sorted_groups = sorted(
        groups.items(),
        key=lambda item: _BATCH_ORDER.get(item[0][2], 99),
    )

    results: list[CheckResult] = []

    # Track which batch_group we last completed (for skip-list updates)
    last_batch_group: str | None = None

    for (table, feature, batch_group), group_checks_list in sorted_groups:
        # When transitioning from core -> quality, update skip lists
        if last_batch_group == "core" and batch_group != "core":
            manifest.update_skip_lists(results)

        # Build skip sets for each check in the group
        skip_sets: dict[str, set[str]] = {}
        for check in group_checks_list:
            skip_set = manifest.skip_set_for(check.depends_on)
            if skip_set:
                skip_sets[check.name] = skip_set

        # Get leagues in scope
        leagues = manifest.leagues_for_feature(feature)
        if not leagues:
            last_batch_group = batch_group
            continue

        batch_results = _run_single_batch_with_error_handling(
            conn=conn,
            table=table,
            feature=feature,
            batch_group=batch_group,
            checks=group_checks_list,
            manifest=manifest,
            skip_sets=skip_sets,
            table_prefix=table_prefix,
        )
        results.extend(batch_results)

        last_batch_group = batch_group

    return results


def main(args: list[str] | None = None) -> int:
    """CLI entry point for the fleet-wide validator.

    Args:
        args: Command-line arguments (uses sys.argv if None).

    Returns:
        Exit code: 0 for success, 1 for CI failures.
    """
    parser = argparse.ArgumentParser(
        description="Fleet-wide validator for ___leagues",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--db",
        action="append",
        help="Validate specific league(s). Can be repeated.",
    )
    parser.add_argument(
        "--page",
        help="Only run checks for this page group.",
    )
    parser.add_argument(
        "--changed-only",
        action="store_true",
        help="Validate only recently changed leagues.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Run all checks including expensive ones.",
    )
    parser.add_argument(
        "--since",
        help="Validate leagues imported after this timestamp (ISO format).",
    )
    parser.add_argument(
        "--store",
        action="store_true",
        help="Store results in ___ops.fleet_health tables.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Show individual check failures.",
    )
    parser.add_argument(
        "--ci",
        action="store_true",
        help="CI mode: exit 1 if any BLOCKER/ERROR failures.",
    )
    parser.add_argument(
        "--ci-regressions",
        action="store_true",
        help="CI mode: exit 1 if regressions from last run.",
    )
    parser.add_argument(
        "--debug-sample",
        type=int,
        default=0,
        help="Show N sample rows per league for failed checks with debug_sql.",
    )
    parser.add_argument(
        "--table-prefix",
        default=DEFAULT_TABLE_PREFIX,
        help=f"Table prefix for SQL (default: {DEFAULT_TABLE_PREFIX}).",
    )

    parsed = parser.parse_args(args)
    table_prefix = parsed.table_prefix

    # Connect
    backend = os.environ.get("DATABASE_BACKEND", "fly")
    token = os.environ.get("MOTHERDUCK_TOKEN")

    t0 = time.time()
    print(f"  Validator v{VALIDATOR_VERSION}")

    if backend == "fly":
        server_url = os.environ.get("DATABASE_SERVER_URL", "")
        read_token = os.environ.get("DATABASE_READ_TOKEN", "")
        admin_token = os.environ.get("DATABASE_ADMIN_TOKEN", "")
        if not server_url or not read_token:
            print("ERROR: DATABASE_SERVER_URL and DATABASE_READ_TOKEN required for Fly backend.", file=sys.stderr)
            return 1
        print("  Connecting to Fly DuckDB server...")
        try:
            conn = FlyConnection(server_url, read_token, admin_token or None)
            # Quick smoke test
            conn.execute("SELECT 1 AS ok")
        except Exception as e:
            print(f"ERROR: Could not connect to Fly server: {e}", file=sys.stderr)
            return 1
    else:
        if not token:
            print("ERROR: MOTHERDUCK_TOKEN not set.", file=sys.stderr)
            return 1
        print("  Connecting to MotherDuck...")
        try:
            conn = duckdb.connect(f"md:___leagues?motherduck_token={token}")
        except Exception as e:
            print(f"ERROR: Could not connect to MotherDuck: {e}", file=sys.stderr)
            return 1

    # Pass 1: Settings + Manifest
    print("  Loading manifest...")
    try:
        manifest = load_manifest(conn, table_prefix=table_prefix)
    except Exception as e:
        print(f"ERROR: Could not load manifest: {e}", file=sys.stderr)
        return 1

    if parsed.db:
        # Scope ALL manifest lists to specific leagues
        db_set = set(parsed.db)
        manifest.all_leagues = parsed.db
        manifest.median_leagues = [lg for lg in manifest.median_leagues if lg in db_set]
        manifest.consolation_leagues = [lg for lg in manifest.consolation_leagues if lg in db_set]
        manifest.keeper_leagues = [lg for lg in manifest.keeper_leagues if lg in db_set]
        manifest.dynasty_leagues = [lg for lg in manifest.dynasty_leagues if lg in db_set]
        manifest.faab_leagues = [lg for lg in manifest.faab_leagues if lg in db_set]
        manifest.espn_leagues = [lg for lg in manifest.espn_leagues if lg in db_set]
        manifest.yahoo_leagues = [lg for lg in manifest.yahoo_leagues if lg in db_set]
        manifest.sleeper_leagues = [lg for lg in manifest.sleeper_leagues if lg in db_set]
        manifest.multi_year_leagues = [lg for lg in manifest.multi_year_leagues if lg in db_set]
        manifest.full_import_leagues = [lg for lg in manifest.full_import_leagues if lg in db_set]
        manifest.sim_leagues = [lg for lg in manifest.sim_leagues if lg in db_set]
        print(f"  Scoped to {len(parsed.db)} league(s): {', '.join(parsed.db)}")

    league_count = len(manifest.all_leagues)
    print(f"  Found {league_count} leagues in manifest.")

    if league_count == 0:
        print("  No leagues to validate.")
        return 0

    # Apply page filter to all check lists
    page_filter = parsed.page
    filtered_manifest_checks = _filter_checks_by_page(manifest_checks, page_filter)
    filtered_completeness_checks = _filter_checks_by_page(completeness_checks, page_filter)
    filtered_system_checks = _filter_checks_by_page(system_checks, page_filter)
    filtered_super_table_checks = _filter_checks_by_page(super_table_checks, page_filter)
    filtered_all_checks = _filter_checks_by_page(ALL_CHECKS, page_filter)

    all_results: list[CheckResult] = []

    # Pass 1.5: Manifest validation
    if filtered_manifest_checks:
        print(f"  Pass 1.5: Manifest validation ({len(filtered_manifest_checks)} checks)...")
        manifest_results = run_checks(conn, filtered_manifest_checks, manifest, table_prefix)
        all_results.extend(manifest_results)
        manifest.update_skip_lists(manifest_results)

    # Pass 2: Completeness
    if filtered_completeness_checks:
        print(f"  Pass 2: Completeness ({len(filtered_completeness_checks)} checks)...")
        completeness_results = run_checks(conn, filtered_completeness_checks, manifest, table_prefix)
        all_results.extend(completeness_results)
        manifest.update_skip_lists(completeness_results)

    # Pass 3: Single-table batches (core -> quality -> analytics)
    batch_checks = [
        c
        for c in filtered_all_checks
        if c.sql_expr is not None
        # Exclude checks already run in other passes
        and c not in filtered_manifest_checks
        and c not in filtered_completeness_checks
        and c not in filtered_system_checks
        and c not in filtered_super_table_checks
    ]
    if batch_checks:
        print(f"  Pass 3: Single-table batches ({len(batch_checks)} checks)...")
        batch_results = run_all_batches(conn, batch_checks, manifest, table_prefix)
        all_results.extend(batch_results)
        manifest.update_skip_lists(batch_results)

    # Pass 4: Cross-table + sql_full system checks
    # Also include sql_full checks from batch modules that weren't in passes 1-2
    sql_full_remaining = [
        c
        for c in filtered_all_checks
        if c.sql_full is not None
        and c not in filtered_manifest_checks
        and c not in filtered_completeness_checks
        and c not in filtered_system_checks
        and c not in filtered_super_table_checks
    ]
    pass_4_checks = filtered_system_checks + sql_full_remaining

    if pass_4_checks:
        print(f"  Pass 4: Cross-table + system ({len(pass_4_checks)} checks)...")
        system_results = run_checks(conn, pass_4_checks, manifest, table_prefix)
        all_results.extend(system_results)

    # Pass 5: (Aggregate checks — included in pass 3/4 batches, nothing separate yet)

    # Pass 6: Super table
    if filtered_super_table_checks:
        print(f"  Pass 6: Super table ({len(filtered_super_table_checks)} checks)...")
        super_results = run_checks(conn, filtered_super_table_checks, manifest, table_prefix)
        all_results.extend(super_results)

    elapsed = time.time() - t0

    # Cascade suppression
    all_results = apply_cascade(all_results)

    # Report
    print(f"\n  Completed in {elapsed:.1f}s")
    print_report(all_results, verbose=parsed.verbose)

    # Store results
    if parsed.store:
        print("  Storing results in ___ops.fleet_health...")
        run_id = store_results(conn, all_results)
        print(f"  Stored as run_id={run_id}")

    # Debug samples
    if parsed.debug_sample > 0:
        run_debug_samples(conn, all_results, parsed.debug_sample, table_prefix)

    conn.close()

    # Exit code
    if parsed.ci:
        failures = _ci_failure_results(all_results)
        if failures:
            unique_checks = {r.check.name for r in failures}
            unique_leagues = {r.db_name for r in failures}
            print(
                f"  CI FAILURE: {len(unique_checks)} check(s) failed " f"across {len(unique_leagues)} league(s)",
                file=sys.stderr,
            )
            return 1

    return 0
