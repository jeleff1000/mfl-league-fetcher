"""SQL-first validation check infrastructure.

Defines SQLCheck (a declarative validation check), CheckResult (the outcome),
CheckRunner (executes checks against a DuckDB/MotherDuck connection),
and ValidationIssue (the canonical issue data class).
"""

import time
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# ValidationIssue — canonical definition (was in validators/base_validator.py)
# ---------------------------------------------------------------------------


@dataclass
class ValidationIssue:
    """A single validation issue found during validation."""

    severity: str  # 'ERROR', 'WARNING', 'INFO'
    table: str  # 'player_fantasy', 'matchup', etc.
    check: str  # 'player_week_format', 'win_loss_sum', etc.
    message: str  # Human-readable description
    year: int | None = None
    week: int | None = None
    row_count: int = 0
    sample_data: list[dict] | None = None
    details: dict[str, Any] | None = None
    ui_impact: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "severity": self.severity,
            "table": self.table,
            "check": self.check,
            "message": self.message,
            "year": self.year,
            "week": self.week,
            "row_count": self.row_count,
            "sample_data": self.sample_data,
            "details": self.details,
            "ui_impact": self.ui_impact,
        }


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class SQLCheck:
    """A single SQL-based validation check.

    The ``sql`` query must return a single row whose first column is a count
    of rows that *violate* the invariant.  The check passes when that count
    is <= ``threshold`` (default 0 = any violating row is a failure).

    Placeholders ``{num_teams}``, ``{playoff_start_week}``, and
    ``{playoff_teams}`` in ``sql`` are replaced at runtime via
    ``CheckRunner._inject_settings``.
    """

    name: str  # e.g. "pf_player_week_not_null"
    table: str  # Primary table being checked (for reporting)
    severity: str  # ERROR, WARNING, INFO
    description: str  # Human-readable: what this checks
    sql: str  # SQL returning single-row result (first col = count)
    threshold: int = 0  # Fail if result > threshold
    message_template: str = "{count} rows failed: {description}"
    ui_impact: str | None = None
    needs_settings: bool = False
    needs_league_type: bool = False
    needs_columns: list[str] | None = None  # Skip if columns missing
    platform: str | None = None  # Only run for this platform
    tags: list[str] = field(default_factory=list)
    # Fix action tiers (cheapest -> most expensive):
    #   "retransform_agg"   = aggregation/homepage rebuild only
    #   "retransform"       = SQL enrichments on MotherDuck (no upload needed)
    #   "reimport_targeted" = re-fetch single table from API + pipeline
    fix_action: str | None = None


@dataclass
class CheckResult:
    """Result of executing a single SQLCheck."""

    check: SQLCheck
    passed: bool
    count: int = 0
    detail: Any = None
    error: str | None = None
    elapsed_ms: int = 0


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------


class CheckRunner:
    """Execute :class:`SQLCheck` instances against a DuckDB connection.

    Parameters
    ----------
    conn : duckdb.DuckDBPyConnection
        Active DuckDB / MotherDuck connection.
    db_name : str
        Database name (used to qualify ``information_schema`` queries).
    settings : LeagueSettings, optional
        Parsed league settings for placeholder injection.
    league_type : LeagueType, optional
        Detected league type for conditional checks.
    verbose : bool
        If True, print progress to stdout.
    """

    def __init__(
        self,
        conn,
        db_name: str,
        *,
        settings=None,
        league_type=None,
        verbose: bool = False,
        table_columns: dict[str, list[str]] | None = None,
    ):
        self.conn = conn
        self.db_name = db_name
        self.settings = settings
        self.league_type = league_type
        self.verbose = verbose
        self._column_cache: dict[str, list[str]] = {k: list(v) for k, v in (table_columns or {}).items()}
        self._all_columns_loaded = bool(table_columns)

    # -- helpers -------------------------------------------------------------

    def get_table_columns(self, table_name: str) -> list[str]:
        """Return column names for *table_name* (cached)."""
        if table_name in self._column_cache:
            return self._column_cache[table_name]

        if not self._all_columns_loaded:
            self._preload_table_columns()
            if table_name in self._column_cache:
                return self._column_cache[table_name]

        try:
            rows = self.conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_catalog = current_database() "
                "AND table_schema = 'public' "
                f"AND table_name = '{table_name}'"
            ).fetchall()
            columns = [r[0] for r in rows]
        except Exception:
            columns = []

        self._column_cache[table_name] = columns
        return columns

    def _preload_table_columns(self) -> None:
        """Load all public-schema columns for the current database in one query."""
        if self._all_columns_loaded:
            return

        try:
            rows = self.conn.execute(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_catalog = current_database() AND table_schema = 'public'"
            ).fetchall()
            for table_name, column_name in rows:
                self._column_cache.setdefault(table_name, []).append(column_name)
        except Exception:
            pass
        finally:
            self._all_columns_loaded = True

    def should_skip(self, check: SQLCheck) -> str | None:
        """Return a reason string if *check* should be skipped, else ``None``."""
        # Platform filter
        if check.platform:
            if self.settings is None:
                return f"needs platform={check.platform} but no settings loaded"
            if self.settings.platform != check.platform:
                return f"platform filter: requires {check.platform}, league is {self.settings.platform}"

        # Settings dependency
        if check.needs_settings and self.settings is None:
            return "needs_settings=True but no settings loaded"

        # League-type dependency
        if check.needs_league_type and self.league_type is None:
            return "needs_league_type=True but no league_type loaded"

        # Column dependency
        if check.needs_columns:
            table_cols = self.get_table_columns(check.table)
            if table_cols:  # only enforce when we could read schema
                missing = [c for c in check.needs_columns if c not in table_cols]
                if missing:
                    return f"table '{check.table}' missing columns: {missing}"

        return None

    def run_check(self, check: SQLCheck) -> CheckResult:
        """Execute a single check and return its result."""
        skip_reason = self.should_skip(check)
        if skip_reason:
            if self.verbose:
                print(f"  SKIP  {check.name}: {skip_reason}")
            return CheckResult(check=check, passed=True, detail=f"skipped: {skip_reason}")

        sql = self._inject_settings(check.sql)

        start = time.perf_counter()
        try:
            row = self.conn.execute(sql).fetchone()
            elapsed = int((time.perf_counter() - start) * 1000)
            count = int(row[0]) if row else 0
            passed = count <= check.threshold

            if self.verbose:
                status = "PASS" if passed else "FAIL"
                print(f"  {status}  {check.name} (count={count}, {elapsed}ms)")

            return CheckResult(
                check=check,
                passed=passed,
                count=count,
                elapsed_ms=elapsed,
            )

        except Exception as exc:
            elapsed = int((time.perf_counter() - start) * 1000)
            if self.verbose:
                print(f"  ERR   {check.name}: {exc}")
            return CheckResult(
                check=check,
                passed=False,
                error=str(exc),
                elapsed_ms=elapsed,
            )

    def run_checks(self, checks: list[SQLCheck], *, batch: bool = True) -> list[CheckResult]:
        """Run checks and return all results.

        Parameters
        ----------
        batch : bool
            If True (default), batch compatible checks into single UNION ALL
            queries to reduce network roundtrips.  Each roundtrip to MotherDuck
            costs ~30-50ms, so batching 50 checks saves ~2 seconds.
        """
        if not batch:
            return [self.run_check(c) for c in checks]
        return self._run_batched(checks)

    def _run_batched(self, checks: list[SQLCheck]) -> list[CheckResult]:
        """Batch compatible checks by table into UNION ALL queries."""
        from collections import defaultdict

        results: list[CheckResult] = []
        # Separate into runnable vs skipped, and group runnable by table
        table_groups: dict[str, list[tuple[int, SQLCheck]]] = defaultdict(list)
        order_map: dict[str, int] = {}  # check.name -> original index

        for i, check in enumerate(checks):
            order_map[check.name] = i
            skip = self.should_skip(check)
            if skip:
                if self.verbose:
                    print(f"  SKIP  {check.name}: {skip}")
                results.append(CheckResult(check=check, passed=True, detail=f"skipped: {skip}"))
            else:
                table_groups[check.table].append((i, check))

        # Run each table's checks as a batch
        for _table, indexed_checks in table_groups.items():
            batch_results = self._run_table_batch(indexed_checks)
            results.extend(batch_results)

        # Sort by original order for consistent output
        results.sort(key=lambda r: order_map.get(r.check.name, 999))
        return results

    def _run_table_batch(self, indexed_checks: list[tuple[int, SQLCheck]]) -> list[CheckResult]:
        """Run a batch of checks for the same table as a UNION ALL query.

        Falls back to sequential execution if batching fails (e.g. incompatible
        SQL syntax).
        """
        MAX_BATCH = 50  # Tested up to 50 UNION ALL depth on MotherDuck

        if len(indexed_checks) <= 1:
            return [self.run_check(c) for _, c in indexed_checks]

        results: list[CheckResult] = []

        # Process in batches of MAX_BATCH
        for batch_start in range(0, len(indexed_checks), MAX_BATCH):
            batch = indexed_checks[batch_start : batch_start + MAX_BATCH]
            batch_results = self._execute_batch(batch)
            results.extend(batch_results)

        return results

    def _execute_batch(self, batch: list[tuple[int, SQLCheck]]) -> list[CheckResult]:
        """Execute a batch of checks as a single UNION ALL query."""
        # Build UNION ALL query: SELECT 'check_name' as name, (check_sql) as cnt
        parts = []
        check_map = {}
        for _, check in batch:
            sql = self._inject_settings(check.sql)
            parts.append(f"SELECT '{check.name}' as check_name, ({sql}) as cnt")
            check_map[check.name] = check

        union_sql = " UNION ALL ".join(parts)

        start = time.perf_counter()
        try:
            rows = self.conn.execute(union_sql).fetchall()
            elapsed = int((time.perf_counter() - start) * 1000)
            per_check_ms = elapsed // max(len(batch), 1)

            results = []
            row_map = {r[0]: int(r[1]) for r in rows}

            for _, check in batch:
                count = row_map.get(check.name, 0)
                passed = count <= check.threshold
                if self.verbose:
                    status = "PASS" if passed else "FAIL"
                    print(f"  {status}  {check.name} (count={count}, ~{per_check_ms}ms)")
                results.append(
                    CheckResult(
                        check=check,
                        passed=passed,
                        count=count,
                        elapsed_ms=per_check_ms,
                    )
                )
            return results

        except Exception:
            # Batch failed — fall back to sequential (some SQL may be incompatible)
            if self.verbose:
                print(f"  BATCH FALLBACK for {len(batch)} checks (running sequentially)")
            return [self.run_check(c) for _, c in batch]

    def results_to_issues(self, results: list[CheckResult]) -> list[ValidationIssue]:
        """Convert :class:`CheckResult` list to :class:`ValidationIssue` list.

        Only failed results (``passed=False``) produce issues.  Skipped checks
        and passing checks are omitted.
        """
        issues: list[ValidationIssue] = []
        for r in results:
            if r.passed:
                continue

            if r.error:
                message = f"Check error: {r.error}"
            else:
                message = r.check.message_template.format(
                    count=r.count,
                    description=r.check.description,
                )

            details = {"elapsed_ms": r.elapsed_ms, "tags": r.check.tags}
            if r.check.fix_action:
                details["fix_action"] = r.check.fix_action
            issues.append(
                ValidationIssue(
                    severity=r.check.severity,
                    table=r.check.table,
                    check=r.check.name,
                    message=message,
                    row_count=r.count,
                    ui_impact=r.check.ui_impact,
                    details=details,
                )
            )
        return issues

    # -- private -------------------------------------------------------------

    def _inject_settings(self, sql: str) -> str:
        """Replace settings placeholders in *sql*.

        Supported placeholders: ``{num_teams}``, ``{playoff_start_week}``,
        ``{playoff_teams}``, ``{espn_post2019_filter}``,
        ``{espn_post2019_filter_m}``.
        Unknown placeholders are left untouched so they don't raise KeyError.
        """
        is_espn = (
            self.settings is not None
            and getattr(self.settings, "platform", None)
            and self.settings.platform.lower() == "espn"
        )
        espn_filter = "AND year >= 2019" if is_espn else ""
        espn_filter_m = "AND m.year >= 2019" if is_espn else ""

        replacements = {
            "{espn_post2019_filter}": espn_filter,
            "{espn_post2019_filter_m}": espn_filter_m,
        }

        if self.settings is not None:
            replacements.update(
                {
                    "{num_teams}": str(self.settings.num_teams),
                    "{playoff_start_week}": str(self.settings.playoff_start_week),
                    "{playoff_teams}": str(self.settings.playoff_teams),
                }
            )

        for placeholder, value in replacements.items():
            sql = sql.replace(placeholder, value)
        return sql
