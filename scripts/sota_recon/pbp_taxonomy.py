"""Canonical PBP play-disposition predicates.

All player-stat PBP witnesses use this rule.  ``(no play)`` is a nullified
down, not an offensive event; declined penalties remain official plays.
"""
from __future__ import annotations


NO_PLAY_SQL = (
    "LOWER(COALESCE(CAST({p}\"desc\" AS VARCHAR), '')) NOT LIKE '%(no play)%' "
    "AND NOT (COALESCE(CAST({p}pbp_source_system AS VARCHAR), '') = 'nflverse' "
    "AND COALESCE(CAST({p}play_type AS VARCHAR), '') = 'no_play')"
)


def official_play_sql(alias: str = "r") -> str:
    """Return the fail-closed predicate for an official, countable play."""
    prefix = f"{alias}." if alias else ""
    return NO_PLAY_SQL.format(p=prefix)


def fumble_mentions_sql(alias: str = "r") -> str:
    """Count literal primary ``FUMBLES`` mentions in a valid PBP description.

    Structured PBP can expose one fumble slot for a play whose description
    records an aborted fumble followed by a second sack fumble.  The parser
    uses this only for total-fumble cardinality; role partitions remain tied
    to the identified fumbler and play role.
    """
    prefix = f"{alias}." if alias else ""
    return (
        "GREATEST(1, array_length(regexp_split_to_array("
        f"LOWER(COALESCE(CAST({prefix}\"desc\" AS VARCHAR), '')), 'fumbles')) - 1)"
    )
