"""Shared SQL helpers for validator league scoping."""

from __future__ import annotations

PUBLISHED_SCOPE_TABLES = ("matchup", "player_fantasy")

# Leagues whose data is structurally incomplete in a way the validator can't
# meaningfully report on, even though they technically have rows in the
# published-scope tables. Each entry must include a one-line rationale and a
# pointer to the handoff that justifies the exclusion. Kept short — additions
# need an architectural reason, not just "the warnings are noisy."
VALIDATOR_EXCLUDED_LEAGUES: tuple[str, ...] = (
    # Sleeper API only ever returned week-1 data for this league; remaining
    # weeks aren't a pipeline bug we can fix.
    # See `handoff_2026_04_28_justice_league_only_trades_partial_import.md`.
    "justice_league_only_trades",
)


def published_league_scope_sql(
    *,
    table_prefix: str = "{table_prefix}",
    league_list: str | None = "{league_list}",
) -> str:
    """Return a subquery yielding leagues published into centralized Fly tables.

    This intentionally derives scope from live app data tables first, with
    inventory as a supplemental signal. Settings-only rows can be left behind by
    failed partial imports, so they are not sufficient to mark a league as
    published.

    Excludes leagues in `VALIDATOR_EXCLUDED_LEAGUES` from the final result.
    The `--db <name>` CLI flag overrides scope at the manifest layer (see
    `validate.py:495`), so explicit per-league runs still work for excluded
    leagues — only the default fleet sweep skips them.
    """

    def _with_filter(select_sql: str) -> str:
        if league_list is None:
            return select_sql
        return f"{select_sql} WHERE db_name IN ({league_list})"

    union_parts = [
        _with_filter(f"SELECT DISTINCT db_name FROM {table_prefix}{table}") for table in PUBLISHED_SCOPE_TABLES
    ]

    inventory_sql = "SELECT DISTINCT database_name AS db_name FROM ___ops.accounts.league_inventory"
    if league_list is not None:
        inventory_sql += f" WHERE in_centralized = TRUE AND database_name IN ({league_list})"
    else:
        inventory_sql += " WHERE in_centralized = TRUE"
    union_parts.append(inventory_sql)

    union_sql = " UNION ".join(union_parts)
    if VALIDATOR_EXCLUDED_LEAGUES:
        excluded_in = ", ".join(f"'{name}'" for name in VALIDATOR_EXCLUDED_LEAGUES)
        outer_filter = f" WHERE db_name NOT IN ({excluded_in})"
    else:
        outer_filter = ""
    return f"SELECT DISTINCT db_name FROM ({union_sql}) published_scope{outer_filter}"
