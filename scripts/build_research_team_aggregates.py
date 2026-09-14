"""Build compact player/year/team aggregates for research team scopes.

The normal research season/career tables are intentionally one row per player/year
and player career.  A team filter therefore has to roll the weekly super-table at
request time so traded players remain correct.  This artifact preserves that
team/year grain ahead of time; the query planner can use it when no week/opponent
scope is active.

This script only builds local Parquet artifacts.  Promotion is handled by the
existing ``build_ops_nfl_and_replace.py`` atomic ___ops_nfl path.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
V26_DIR = Path(
    "D:/league-history-data/nfl/releases/"
    "nfl_local_release_franchise_backfill_20260617T122657Z_v26/tables"
)
WEEKLY_SOURCE = V26_DIR / "nfl_player_stats_all.parquet"
SEASON_SOURCE = V26_DIR / "season_career_v26/player_nfl_season.parquet"
OUTPUT_DIR = V26_DIR / "season_career_v26"

GROUP_COLUMNS = [
    "NFL_player_id",
    "year",
    "nfl_team",
    "nfl_franchise_number",
    "season_type",
]

IDENTITY_COLUMNS = {
    "NFL_player_id",
    "year",
    "nfl_team",
    "nfl_franchise_number",
    "season_type",
    "player",
    "position",
    "nfl_position",
    "headshot_url",
    "player_week",
    "week",
}

AVG_COLUMNS = {
    "target_share",
    "air_yards_share",
    "wopr",
    "pacr",
    "racr",
    "passing_epa",
    "rushing_epa",
    "receiving_epa",
}

RATIO_DEPENDENCIES: dict[str, tuple[str, str]] = {
    "fg_pct": ("fg_made", "fg_att"),
    "comp_pct": ("completions", "attempts"),
    "yards_per_attempt": ("passing_yards", "attempts"),
    "yards_per_carry": ("rushing_yards", "carries"),
    "yards_per_reception": ("receiving_yards", "receptions"),
    "catch_rate": ("receptions", "targets"),
}


def q(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def output_table_names() -> tuple[str, str]:
    return "player_nfl_season_team", "player_nfl_season_team_all"


def team_scope_group_columns() -> list[str]:
    return list(GROUP_COLUMNS)


def aggregate_expression(column: str) -> str:
    """Return the same aggregation family used by research-query.ts."""
    if column in RATIO_DEPENDENCIES:
        numerator, denominator = RATIO_DEPENDENCIES[column]
        if column == "fg_pct":
            return f"CASE WHEN SUM({denominator}) > 0 THEN CAST(SUM({numerator}) AS DOUBLE) / SUM({denominator}) ELSE NULL END"
        return f"CASE WHEN SUM({denominator}) > 0 THEN CAST(SUM({numerator}) AS DOUBLE) / SUM({denominator}) ELSE NULL END"
    if column in AVG_COLUMNS:
        return (
            f"AVG(CASE WHEN isfinite(TRY_CAST({q(column)} AS DOUBLE)) "
            f"THEN TRY_CAST({q(column)} AS DOUBLE) ELSE NULL END)"
        )
    if column.endswith("_long") or column == "fg_long":
        return f"MAX({q(column)})"
    return f"SUM(COALESCE({q(column)}, 0))"


def _season_filter(include_postseason: bool) -> str:
    if include_postseason:
        return "(season_type IS NULL OR season_type IN ('REG', 'POST'))"
    return "(season_type IS NULL OR season_type = 'REG')"


def _derived_expression(column: str, weekly: set[str]) -> str | None:
    if column == "games_played":
        return "CAST(COUNT(*) AS INTEGER)"
    if column == "nfl_team_count":
        return "CAST(COUNT(DISTINCT nfl_team) AS INTEGER)"
    if column == "nfl_teams":
        return "STRING_AGG(DISTINCT nfl_team, ', ' ORDER BY nfl_team) FILTER (WHERE nfl_team IS NOT NULL)"
    if column == "season_positions":
        return "STRING_AGG(DISTINCT COALESCE(position, nfl_position), ', ' ORDER BY COALESCE(position, nfl_position)) FILTER (WHERE COALESCE(position, nfl_position) IS NOT NULL)"
    if column == "games_started":
        return "CAST(SUM(CASE WHEN COALESCE(is_starter, false) THEN 1 ELSE 0 END) AS INTEGER)" if "is_starter" in weekly else None
    if column.startswith("ppg_season_"):
        source = "fpts_" + column.removeprefix("ppg_season_")
        return f"AVG(CAST({q(source)} AS DOUBLE))" if source in weekly else None
    if column.startswith("lamar_ppg_"):
        source = "lamar_" + column.removeprefix("lamar_ppg_")
        return f"AVG(CAST({q(source)} AS DOUBLE))" if source in weekly else None
    if column in RATIO_DEPENDENCIES:
        numerator, denominator = RATIO_DEPENDENCIES[column]
        return aggregate_expression(column) if numerator in weekly and denominator in weekly else None
    return aggregate_expression(column) if column in weekly else None


def _team_rollup_select_parts(
    *,
    weekly_columns: set[str],
    season_columns: list[tuple[str, str]],
) -> list[str]:
    """Build the shared SELECT list for file and in-artifact team aggregates."""
    select_parts = [
        '"NFL_player_id"',
        'CAST("year" AS INTEGER) AS "year"',
        '"nfl_team"',
        'TRY_CAST(ROUND(TRY_CAST("nfl_franchise_number" AS DOUBLE)) AS INTEGER) AS "nfl_franchise_number"',
        '"season_type"',
        'ARG_MAX("player", "player_week") AS "player"',
        'ARG_MAX("position", "player_week") AS "position"',
        'ARG_MAX("nfl_position", "player_week") AS "nfl_position"',
        'ARG_MAX("headshot_url", "player_week") AS "headshot_url"',
    ]
    emitted = set(GROUP_COLUMNS) | {"player", "position", "nfl_position", "headshot_url"}
    for row in season_columns:
        column = row[0]
        if column in emitted or column in {"first_year", "last_year", "years_active"}:
            continue
        expression = _derived_expression(column, weekly_columns)
        if expression is None:
            continue
        select_parts.append(f"{expression} AS {q(column)}")
        emitted.add(column)
    return select_parts


def build_table_into_relation(
    con: duckdb.DuckDBPyConnection,
    *,
    source_ref: str,
    target_ref: str,
    include_postseason: bool,
    weekly_columns: set[str],
    season_columns: list[tuple[str, str]],
) -> int:
    """Materialize a team-season cache directly in the candidate ops artifact.

    This is the same contract as ``build_table`` below, but avoids a temporary
    Parquet roundtrip so a worker can build all eight promoted tables locally.
    ``source_ref`` and ``target_ref`` are internal, fixed relation names.
    """
    select_parts = _team_rollup_select_parts(
        weekly_columns=weekly_columns,
        season_columns=season_columns,
    )
    con.execute(
        f"""
        CREATE OR REPLACE TABLE {target_ref} AS
        SELECT {', '.join(select_parts)}
        FROM {source_ref}
        WHERE NFL_player_id IS NOT NULL
          AND year IS NOT NULL
          AND week IS NOT NULL
          AND {_season_filter(include_postseason)}
        GROUP BY NFL_player_id, CAST(year AS INTEGER), nfl_team,
                 TRY_CAST(ROUND(TRY_CAST(nfl_franchise_number AS DOUBLE)) AS INTEGER),
                 season_type
        """
    )
    return int(con.execute(f"SELECT COUNT(*) FROM {target_ref}").fetchone()[0])


def build_table(
    con: duckdb.DuckDBPyConnection,
    output_path: Path,
    *,
    include_postseason: bool,
    weekly_columns: set[str],
    season_columns: list[tuple[str, str]],
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    source = WEEKLY_SOURCE.as_posix().replace("'", "''")
    weekly_table = f"read_parquet('{source}')"
    select_parts = _team_rollup_select_parts(
        weekly_columns=weekly_columns,
        season_columns=season_columns,
    )

    sql = f"""
        COPY (
          SELECT {', '.join(select_parts)}
          FROM {weekly_table}
          WHERE NFL_player_id IS NOT NULL
            AND year IS NOT NULL
            AND week IS NOT NULL
            AND {_season_filter(include_postseason)}
          GROUP BY NFL_player_id, CAST(year AS INTEGER), nfl_team,
                   TRY_CAST(ROUND(TRY_CAST(nfl_franchise_number AS DOUBLE)) AS INTEGER),
                   season_type
        ) TO '{output_path.as_posix().replace("'", "''")}'
        (FORMAT PARQUET, COMPRESSION ZSTD)
    """
    con.execute(sql)
    return con.execute(f"SELECT COUNT(*) FROM read_parquet('{output_path.as_posix()}')").fetchone()[0]


def build(only: str = "both") -> dict[str, int]:
    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")
    weekly_columns = {
        row[0]
        for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{WEEKLY_SOURCE.as_posix()}')").fetchall()
    }
    season_columns = con.execute(
        f"DESCRIBE SELECT * FROM read_parquet('{SEASON_SOURCE.as_posix()}')"
    ).fetchall()
    regular, all_games = output_table_names()
    requested = {
        "regular": [(regular, False)],
        "all": [(all_games, True)],
        "both": [(regular, False), (all_games, True)],
    }[only]
    counts = {
        name: build_table(
            con,
            OUTPUT_DIR / f"{name}.parquet",
            include_postseason=include_postseason,
            weekly_columns=weekly_columns,
            season_columns=season_columns,
        )
        for name, include_postseason in requested
    }
    con.close()
    return counts


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", action="store_true", help="write local Parquet artifacts")
    parser.add_argument("--only", choices=("regular", "all", "both"), default="both")
    args = parser.parse_args()
    if not args.build:
        print({"outputs": output_table_names(), "group_columns": team_scope_group_columns()})
    else:
        print(build(args.only))
