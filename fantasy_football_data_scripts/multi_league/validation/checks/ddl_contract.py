"""Generated DDL-contract checks for canonical core and aggregate tables."""

from __future__ import annotations

from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_COLUMN_TYPES
from multi_league.core.local_db import _TABLE_COLUMN_TYPES

from ._runner import SQLCheck


def _sql_string(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sql_type_literal(dtype: str) -> str:
    return _sql_string(str(dtype).upper().split("(")[0].strip())


def _column_values_sql(columns: list[str]) -> str:
    return ", ".join(f"({_sql_string(column)})" for column in columns)


def _column_type_values_sql(column_types: dict[str, str]) -> str:
    return ", ".join(f"({_sql_string(column)}, {_sql_type_literal(dtype)})" for column, dtype in column_types.items())


def _table_exists_clause(table_name: str) -> str:
    return (
        "EXISTS("
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_catalog=current_database() AND table_schema='public' "
        f"AND table_name='{table_name}'"
        ")"
    )


def _wrap_if_exists(table_name: str, sql: str, *, only_if_exists: bool) -> str:
    if not only_if_exists:
        return sql
    return f"SELECT CASE WHEN {_table_exists_clause(table_name)} THEN (({sql})) ELSE 0 END"


def _duplicate_columns_check(table_name: str, *, only_if_exists: bool = False) -> SQLCheck:
    sql = (
        "SELECT COUNT(*) FROM ("
        "SELECT column_name "
        "FROM information_schema.columns "
        f"WHERE table_schema='public' AND table_name='{table_name}' AND table_catalog=current_database() "
        "GROUP BY column_name HAVING COUNT(*) > 1)"
    )
    return SQLCheck(
        name=f"ddl_duplicate_columns_{table_name}",
        table=table_name,
        severity="ERROR",
        description=f"No duplicate column names in {table_name}",
        sql=_wrap_if_exists(table_name, sql, only_if_exists=only_if_exists),
        ui_impact="FATAL: Ambiguous column references",
        fix_action="retransform",
    )


def _missing_columns_check(table_name: str, expected_columns: list[str], *, only_if_exists: bool = False) -> SQLCheck:
    values_sql = _column_values_sql(expected_columns)
    sql = (
        "WITH canonical(column_name) AS ("
        f"  VALUES {values_sql}"
        ") "
        "SELECT COUNT(*) "
        "FROM canonical c "
        "LEFT JOIN information_schema.columns actual "
        "  ON actual.table_catalog = current_database() "
        " AND actual.table_schema = 'public' "
        f" AND actual.table_name = '{table_name}' "
        " AND actual.column_name = c.column_name "
        "WHERE actual.column_name IS NULL"
    )
    return SQLCheck(
        name=f"ddl_missing_canonical_columns_{table_name}",
        table=table_name,
        severity="ERROR",
        description=f"{table_name} includes all {len(expected_columns)} canonical columns",
        sql=_wrap_if_exists(table_name, sql, only_if_exists=only_if_exists),
        message_template=f"{{count}} canonical {table_name} columns are missing: {{description}}",
        fix_action="retransform",
    )


def _unexpected_columns_check(
    table_name: str, expected_columns: list[str], *, only_if_exists: bool = False
) -> SQLCheck:
    canonical_list_sql = ", ".join(_sql_string(column) for column in expected_columns)
    sql = (
        "SELECT COUNT(*) "
        "FROM information_schema.columns "
        "WHERE table_catalog=current_database() AND table_schema='public' "
        f"AND table_name='{table_name}' "
        f"AND column_name NOT IN ({canonical_list_sql})"
    )
    return SQLCheck(
        name=f"ddl_unexpected_columns_{table_name}",
        table=table_name,
        severity="ERROR",
        description=f"{table_name} has no unexpected non-canonical columns",
        sql=_wrap_if_exists(table_name, sql, only_if_exists=only_if_exists),
        message_template=f"{{count}} unexpected {table_name} columns are outside the canonical schema: {{description}}",
        fix_action="retransform",
    )


def _type_mismatch_check(table_name: str, expected_types: dict[str, str], *, only_if_exists: bool = False) -> SQLCheck:
    values_sql = _column_type_values_sql(expected_types)
    sql = (
        "WITH canonical(column_name, expected_type) AS ("
        f"  VALUES {values_sql}"
        "), actual AS ("
        "  SELECT column_name, UPPER(SPLIT_PART(data_type, '(', 1)) AS actual_type "
        "  FROM information_schema.columns "
        "  WHERE table_catalog=current_database() AND table_schema='public' "
        f"    AND table_name='{table_name}'"
        ") "
        "SELECT COUNT(*) "
        "FROM canonical c "
        "JOIN actual a ON a.column_name = c.column_name "
        "WHERE a.actual_type != c.expected_type"
    )
    return SQLCheck(
        name=f"ddl_type_mismatches_{table_name}",
        table=table_name,
        severity="ERROR",
        description=f"{table_name} canonical columns use canonical DuckDB types",
        sql=_wrap_if_exists(table_name, sql, only_if_exists=only_if_exists),
        message_template=f"{{count}} canonical {table_name} columns have type drift: {{description}}",
        fix_action="retransform",
    )


def build_contract_checks(
    table_name: str,
    expected_types: dict[str, str],
    *,
    only_if_exists: bool = False,
) -> list[SQLCheck]:
    expected_columns = list(expected_types.keys())
    return [
        _duplicate_columns_check(table_name, only_if_exists=only_if_exists),
        _missing_columns_check(table_name, expected_columns, only_if_exists=only_if_exists),
        _unexpected_columns_check(table_name, expected_columns, only_if_exists=only_if_exists),
        _type_mismatch_check(table_name, expected_types, only_if_exists=only_if_exists),
    ]


CORE_DDL_CONTRACT_CHECKS: list[SQLCheck] = []
for _table_name, _column_types in _TABLE_COLUMN_TYPES.items():
    CORE_DDL_CONTRACT_CHECKS.extend(build_contract_checks(_table_name, _column_types))


AGGREGATE_DDL_CONTRACT_CHECKS: list[SQLCheck] = []
for _table_name, _column_types in AGGREGATE_TABLE_COLUMN_TYPES.items():
    AGGREGATE_DDL_CONTRACT_CHECKS.extend(build_contract_checks(_table_name, _column_types, only_if_exists=True))
