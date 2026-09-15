"""Persist compact NFL week/game revisions after the live ops refresh."""

from __future__ import annotations

import argparse
import json
import os
import re
from typing import Any

from multi_league.core.league_update_manifest import (
    MANIFEST_SCHEMA_VERSION,
    NFL_CANONICAL_IDENTITY_COLUMNS,
    NFL_SCORING_INPUT_COLUMNS,
    build_nfl_revision_rows,
    resolve_nfl_scoring_input_columns,
)


OPS_DATABASE = "___ops"
DEFAULT_SOURCE = "nfl_historical.nfl_player_stats_all"
DEFAULT_TARGET = "accounts.league_update_nfl_revisions"
_RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")


def _relation(value: str) -> str:
    if not _RELATION_RE.fullmatch(value):
        raise ValueError(f"invalid DuckDB relation: {value!r}")
    return value


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def materialize_nfl_revisions(
    reader: Any,
    writer: Any,
    *,
    year: int,
    week: int,
    source_relation: str = DEFAULT_SOURCE,
    target_relation: str = DEFAULT_TARGET,
) -> dict[str, int]:
    """Replace one compact revision scope only after source rows are present."""
    source = _relation(source_relation)
    target = _relation(target_relation)
    source_schema, source_table = source.split(".", 1)
    schema_rows = reader.query(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = {_literal(source_schema)} AND table_name = {_literal(source_table)}",
        database=OPS_DATABASE,
    )
    scoring_columns = resolve_nfl_scoring_input_columns(
        str(row["column_name"]) for row in schema_rows
    )
    if not scoring_columns:
        raise RuntimeError(f"no registered NFL scoring columns exist in {source}")
    selected = (
        *NFL_CANONICAL_IDENTITY_COLUMNS,
        "year",
        *scoring_columns,
    )
    columns = ", ".join(f'"{column}"' for column in dict.fromkeys(selected))
    rows = reader.query(
        f"""
        SELECT {columns}
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) = {int(year)}
          AND TRY_CAST(week AS INTEGER) = {int(week)}
          AND UPPER(COALESCE(season_type, 'REG')) = 'REG'
          AND NFL_player_id IS NOT NULL
          AND nfl_team IS NOT NULL
          AND opponent_nfl_team IS NOT NULL
        """,
        database=OPS_DATABASE,
    )
    if not rows:
        raise RuntimeError(f"no NFL source rows for {year} week {week}; refusing to replace revisions")

    revisions = build_nfl_revision_rows(
        rows,
        schema_version=MANIFEST_SCHEMA_VERSION,
        scoring_columns=scoring_columns,
    )
    if not revisions:
        raise RuntimeError(f"no complete NFL games for {year} week {week}; refusing to replace revisions")
    values = ",\n".join(
        "(" + ", ".join(
            (
                str(int(row["season"])),
                str(int(row["week"])),
                _literal(str(row["game_key"])),
                _literal(str(row["revision"])),
                str(int(row["schema_version"])),
                "NOW()",
            )
        ) + ")"
        for row in revisions
    )
    writer.execute(
        f"""
        BEGIN TRANSACTION;
        CREATE SCHEMA IF NOT EXISTS accounts;
        CREATE TABLE IF NOT EXISTS {target} (
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            game_key VARCHAR NOT NULL,
            revision VARCHAR NOT NULL,
            schema_version INTEGER NOT NULL,
            refreshed_at TIMESTAMP NOT NULL,
            PRIMARY KEY (season, week, game_key)
        );
        DELETE FROM {target} WHERE season = {int(year)} AND week = {int(week)};
        INSERT INTO {target}
            (season, week, game_key, revision, schema_version, refreshed_at)
        VALUES {values};
        COMMIT;
        """,
        database=OPS_DATABASE,
    )
    return {"season": int(year), "week": int(week), "games": len(revisions)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args(argv)

    os.environ["DATABASE_BACKEND"] = "fly"
    from multi_league.core.fly_writer import FlyWriter
    from multi_league.core.readers.fly_reader import FlyReader

    receipt = materialize_nfl_revisions(FlyReader(), FlyWriter(), year=args.year, week=args.week)
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
