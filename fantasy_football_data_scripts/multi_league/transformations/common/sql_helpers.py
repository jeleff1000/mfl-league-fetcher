"""
SQL builder helpers for transformation enrichments.

Generates UPDATE ... FROM (subquery) statements. Returns SQL strings
(does not execute) so all generated SQL can be logged and inspected.

Replaces boilerplate in 140+ UPDATE statements across 5 mixin files.
"""


def update_from_subquery(
    table: str,
    set_cols: dict[str, str],
    subquery_sql: str,
    join_condition: str,
    where_filter: str | None = None,
    subquery_alias: str = "sub",
) -> str:
    """Generate an UPDATE ... FROM (subquery) statement.

    Args:
        table: Target table name
        set_cols: Dict of {target_column: source_expression}
        subquery_sql: The SELECT statement for the subquery
        join_condition: JOIN condition (e.g., "table.key = sub.key")
        where_filter: Optional additional WHERE clause
        subquery_alias: Alias for the subquery (default "sub")

    Returns:
        Complete SQL UPDATE statement as a string.
    """
    set_clause = ",\n    ".join(f"{col} = {expr}" for col, expr in set_cols.items())

    sql = f"""UPDATE {table}
SET {set_clause}
FROM ({subquery_sql}) AS {subquery_alias}
WHERE {join_condition}"""

    if where_filter:
        sql += f"\n  AND {where_filter}"

    return sql
