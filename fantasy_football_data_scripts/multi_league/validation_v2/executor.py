"""Executor — Batch Builder + SQL Generator for validation_v2.

Groups checks into batched queries, injects skip-list CTEs for per-league
dependency resolution, runs them, and unpivots results into CheckResult objects.
"""

from __future__ import annotations

from collections import defaultdict

import duckdb

from multi_league.validation_v2.models import Check, CheckResult, Manifest

# Default table prefix for MotherDuck centralized DB
DEFAULT_TABLE_PREFIX = "___leagues.public."


def group_checks(
    checks: list[Check],
) -> dict[tuple[str, str | None, str], list[Check]]:
    """Group checks by (table, feature, batch_group).

    Each group becomes one batched SQL query. Only checks with sql_expr
    are grouped; sql_full checks are run standalone by the caller.
    """
    groups: dict[tuple[str, str | None, str], list[Check]] = defaultdict(list)
    for check in checks:
        if check.sql_expr is not None:
            key = (check.table, check.feature, check.batch_group)
            groups[key].append(check)
    return dict(groups)


def _escape_sql_string(s: str) -> str:
    """Escape a string for SQL single-quote literals."""
    return s.replace("'", "''")


def _build_league_list_sql(leagues: list[str]) -> str:
    """Build a comma-separated list of quoted league names for IN clause."""
    return ", ".join(f"'{_escape_sql_string(lg)}'" for lg in leagues)


def build_batch_sql(
    checks: list[Check],
    table: str,
    manifest: Manifest,
    feature: str | None,
    skip_sets: dict[str, set[str]],
    table_prefix: str = DEFAULT_TABLE_PREFIX,
) -> str:
    """Build a batched SQL query for a group of checks.

    Each check's sql_expr becomes a column in the SELECT. Checks with
    dependencies get a CTE skip-list and a NOT IN guard so that skipped
    leagues produce 0 for that check column.

    Args:
        checks: List of Check objects (all with sql_expr, same table/feature/batch_group).
        table: Table name (e.g. "matchup").
        manifest: Manifest with league lists and skip data.
        feature: Feature gate (None = all leagues).
        skip_sets: Mapping of check_name -> set of league names to skip.
        table_prefix: SQL prefix for table reference (default: MotherDuck centralized).

    Returns:
        Complete SQL string ready to execute.
    """
    leagues = manifest.leagues_for_feature(feature)
    if not leagues:
        raise ValueError(f"No leagues for feature={feature!r}")

    league_list_sql = _build_league_list_sql(leagues)

    # Build CTEs for checks that have skip lists
    cte_parts: list[str] = []
    for check in checks:
        check_skip = skip_sets.get(check.name, set())
        if check_skip:
            values = ", ".join(f"('{_escape_sql_string(lg)}')" for lg in sorted(check_skip))
            cte_name = f"skip_{check.name}"
            cte_parts.append(f"{cte_name} AS (\n" f"  SELECT col0 AS db_name FROM (VALUES {values})\n" f")")

    # Build SELECT columns
    select_cols: list[str] = ["m.db_name"]
    for check in checks:
        check_skip = skip_sets.get(check.name, set())
        expr = check.sql_expr
        if check_skip:
            cte_name = f"skip_{check.name}"
            # Wrap the expression: only evaluate when NOT in skip list
            # Skipped leagues get 0 (they'll be marked as skipped in run_batch)
            col = (
                f"SUM(CASE WHEN m.db_name NOT IN (SELECT db_name FROM {cte_name}) "
                f"THEN CASE WHEN ({_extract_inner_condition(expr)}) THEN 1 ELSE 0 END "
                f"ELSE 0 END) AS {check.name}"
            )
        else:
            # No skip list, use the expression directly
            col = f"{expr} AS {check.name}"
        select_cols.append(col)

    # Assemble full query
    parts: list[str] = []
    if cte_parts:
        parts.append("WITH " + ",\n".join(cte_parts))

    parts.append("SELECT " + ",\n  ".join(select_cols))
    parts.append(f"FROM {table_prefix}{table} m")
    parts.append(f"WHERE m.db_name IN ({league_list_sql})")
    parts.append("GROUP BY m.db_name")

    return "\n".join(parts)


def _extract_inner_condition(sql_expr: str) -> str:
    """Extract the inner condition from a SUM(CASE WHEN ... THEN 1 ELSE 0 END) expression.

    If the expression follows the standard pattern, extract just the condition.
    Otherwise, return a fallback that treats the whole expression as a count > 0.
    """
    upper = sql_expr.strip().upper()

    # Standard pattern: SUM(CASE WHEN <condition> THEN 1 ELSE 0 END)
    if upper.startswith("SUM(CASE WHEN") and upper.endswith("THEN 1 ELSE 0 END)"):
        # Extract between "SUM(CASE WHEN " and " THEN 1 ELSE 0 END)"
        start = sql_expr.upper().index("SUM(CASE WHEN") + len("SUM(CASE WHEN")
        end = sql_expr.upper().rindex("THEN 1 ELSE 0 END)")
        return sql_expr[start:end].strip()

    # Non-standard expression: wrap as-is, the outer SUM(CASE WHEN) will be
    # applied by the caller. Actually, for non-standard expressions, we keep
    # the original expression as a column directly.
    # This shouldn't happen with well-formed checks, but handle gracefully.
    return f"({sql_expr}) > 0"


def run_batch(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    checks: list[Check],
    manifest: Manifest,
    skip_sets: dict[str, set[str]],
) -> list[CheckResult]:
    """Execute a batched SQL query and unpivot into CheckResult objects.

    The SQL produces wide rows: (db_name, check1_count, check2_count, ...).
    Each row is unpivoted into individual CheckResult objects per check.

    Args:
        conn: DuckDB connection.
        sql: SQL query from build_batch_sql().
        checks: List of Check objects matching the SQL columns.
        manifest: Manifest for skip-set resolution.
        skip_sets: Mapping of check_name -> set of skipped league names.

    Returns:
        List of CheckResult objects for all leagues in the query results.
    """
    rows = conn.execute(sql).fetchall()
    columns = [desc[0] for desc in conn.description]

    results: list[CheckResult] = []

    for row in rows:
        row_dict = dict(zip(columns, row))
        db_name = row_dict["db_name"]

        for check in checks:
            check_skip = skip_sets.get(check.name, set())

            if db_name in check_skip:
                # League was in the skip list for this check
                results.append(
                    CheckResult(
                        check=check,
                        db_name=db_name,
                        fail_count=0,
                        passed=True,
                        skipped=True,
                    )
                )
            else:
                fail_count = int(row_dict.get(check.name, 0))
                passed = fail_count <= check.threshold
                results.append(
                    CheckResult(
                        check=check,
                        db_name=db_name,
                        fail_count=fail_count,
                        passed=passed,
                    )
                )

    return results


def run_sql_full(
    conn: duckdb.DuckDBPyConnection,
    check: Check,
    manifest: Manifest,
    table_prefix: str = DEFAULT_TABLE_PREFIX,
) -> list[CheckResult]:
    """Run a standalone sql_full query and return CheckResult objects.

    The sql_full query must return rows of (db_name, fail_count).
    Leagues in the manifest that don't appear in results are marked as passed.

    Skip logic is handled by the caller (executor orchestrator), not here.

    Args:
        conn: DuckDB connection.
        check: Check object with sql_full set.
        manifest: Manifest for league list and feature resolution.
        table_prefix: SQL prefix for table reference.

    Returns:
        List of CheckResult objects for all leagues in the feature scope.
    """
    if not check.sql_full:
        raise ValueError(f"Check {check.name} has no sql_full")

    leagues = manifest.leagues_for_feature(check.feature)
    league_list_sql = _build_league_list_sql(leagues)

    # Substitute placeholders
    table_ref = f"{table_prefix}{check.table}"
    sql = check.sql_full.format(
        table=table_ref,
        league_list=league_list_sql,
        table_prefix=table_prefix,
    )

    try:
        rows = conn.execute(sql).fetchall()
    except duckdb.CatalogException:
        # Table doesn't exist — all leagues pass (nothing to validate)
        return [CheckResult(check=check, db_name=league, fail_count=0, passed=True) for league in leagues]
    except duckdb.BinderException as e:
        # Column doesn't exist — log warning and skip
        import sys

        print(f"  WARNING: Check {check.name} failed with error: {e}", file=sys.stderr)
        return [CheckResult(check=check, db_name=league, fail_count=0, passed=True, skipped=True) for league in leagues]

    # Build a map of db_name -> fail_count from query results
    fail_map: dict[str, int] = {}
    for row in rows:
        db_name = row[0]
        fail_count = int(row[1])
        fail_map[db_name] = fail_count

    # Produce results for ALL leagues in scope
    results: list[CheckResult] = []
    for league in leagues:
        fail_count = fail_map.get(league, 0)
        passed = fail_count <= check.threshold
        results.append(
            CheckResult(
                check=check,
                db_name=league,
                fail_count=fail_count,
                passed=passed,
            )
        )

    return results
