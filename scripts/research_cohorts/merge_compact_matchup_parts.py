"""Merge compact research task databases and compute career outer rows once."""
from __future__ import annotations

import argparse
from pathlib import Path
from time import perf_counter

import duckdb

from scripts.research_cohorts.compact_matchup_cells import (
    materialize_career_compact_outer_rows,
    materialize_compact_exact_core_selector_indices,
    materialize_compact_exact_grade_selector_indices,
    materialize_compact_grade_sidecar,
)


TABLES = ("research_matchup_compact_weekly", "research_matchup_compact_season")


def _log_phase(phase: str, **fields: object) -> None:
    """Emit compact, machine-readable progress for the single merge job."""
    details = " ".join(f"{key}={value}" for key, value in fields.items())
    print(f"[compact-merge] phase={phase} {details}".rstrip(), flush=True)


def _table_rows(connection: duckdb.DuckDBPyConnection, table: str) -> int:
    return int(connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])


def _quote_path(path: Path) -> str:
    return path.resolve().as_posix().replace("'", "''")


def _materialize_batched_career(
    connection: duckdb.DuckDBPyConnection,
    *,
    career_buckets: int,
) -> None:
    """Build exact career rows in bounded player-hash batches.

    The full season table is already compact.  Restricting it to one disjoint
    player hash bucket before unnesting prevents the global career CTE from
    holding every historical cohort cell in memory.  Each player belongs to
    exactly one bucket, so the final career table is an append, not a second
    aggregate or a change to any metric definition.
    """
    if career_buckets < 1:
        raise ValueError("career_buckets must be positive")
    connection.execute("SET preserve_insertion_order=false")
    # The career query is aggregation-heavy. A small thread count avoids each
    # parallel aggregate retaining a separate large hash state on CI runners.
    connection.execute("SET threads=2")
    part_tables: list[str] = []
    for bucket in range(career_buckets):
        bucket_started = perf_counter()
        source = "_career_source_batch"
        part = f"_career_part_{bucket}"
        connection.execute(f"DROP TABLE IF EXISTS {source}")
        connection.execute(
            f"""
            CREATE TEMP TABLE {source} AS
            SELECT *
            FROM research_matchup_compact_season
            WHERE CAST(hash(CAST(NFL_player_id AS VARCHAR)) % ? AS INTEGER) = ?
            """,
            [career_buckets, bucket],
        )
        source_rows = _table_rows(connection, source)
        materialize_career_compact_outer_rows(
            connection,
            season_compact_table=source,
            output_table=part,
        )
        part_tables.append(part)
        _log_phase(
            "career_bucket",
            bucket=f"{bucket + 1}/{career_buckets}",
            season_rows=source_rows,
            career_rows=_table_rows(connection, part),
            seconds=f"{perf_counter() - bucket_started:.3f}",
        )
    assembly_started = perf_counter()
    union_sql = " UNION ALL ".join(f"SELECT * FROM {part}" for part in part_tables)
    connection.execute(
        "CREATE TABLE research_matchup_compact_career AS " + union_sql
    )
    _log_phase(
        "career_assemble",
        career_rows=_table_rows(connection, "research_matchup_compact_career"),
        seconds=f"{perf_counter() - assembly_started:.3f}",
    )
    core_selectors_started = perf_counter()
    materialize_compact_exact_core_selector_indices(
        connection,
        output_table="research_matchup_compact_career",
    )
    _log_phase(
        "career_core_selector_indices",
        career_rows=_table_rows(connection, "research_matchup_compact_career"),
        seconds=f"{perf_counter() - core_selectors_started:.3f}",
    )
    grade_selectors_started = perf_counter()
    materialize_compact_exact_grade_selector_indices(
        connection,
        output_table="research_matchup_compact_career",
    )
    _log_phase(
        "career_grade_selector_indices",
        career_rows=_table_rows(connection, "research_matchup_compact_career"),
        seconds=f"{perf_counter() - grade_selectors_started:.3f}",
    )
    duplicate_career = connection.execute("""
      SELECT COUNT(*) FROM (
        SELECT NFL_player_id,position FROM research_matchup_compact_career
        GROUP BY 1,2 HAVING COUNT(*) <> 1
      )
    """).fetchone()[0]
    if duplicate_career:
        raise RuntimeError(f"compact career parts overlap: {duplicate_career}")
    _log_phase("career_integrity", duplicate_outer_keys=duplicate_career)
    for part in part_tables:
        connection.execute(f"DROP TABLE {part}")
    connection.execute(f"DROP TABLE {source}")


def build_compact_career_part(
    *,
    season_bundle: Path,
    output: Path,
    career_buckets: int,
    career_bucket: int,
    player_sub_buckets: int = 1,
) -> None:
    """Build one disjoint career hash part from an already compact season bundle.

    This never reads the immutable cache.  A player's seasons are all assigned
    to one hash bucket, so each part is a complete, exact career result for its
    players and the parts can later be appended without a second aggregate.
    """
    if career_buckets < 1:
        raise ValueError("career_buckets must be positive")
    if not 0 <= career_bucket < career_buckets:
        raise ValueError("career_bucket must be in [0, career_buckets)")
    if player_sub_buckets < 1:
        raise ValueError("player_sub_buckets must be positive")
    if not season_bundle.is_file():
        raise FileNotFoundError(f"missing compact season bundle: {season_bundle}")
    if output.exists():
        raise FileExistsError(f"refusing to overwrite compact career part: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output))
    started = perf_counter()
    try:
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute(f"ATTACH '{_quote_path(season_bundle)}' AS source (READ_ONLY)")
        part_tables: list[str] = []
        source_rows = 0
        for sub_bucket in range(player_sub_buckets):
            source = f"_career_source_{sub_bucket}"
            part = f"_career_part_{sub_bucket}"
            con.execute(
                f"""
                CREATE TABLE {source} AS
                SELECT *
                FROM source.research_matchup_compact_season
                WHERE CAST(hash(CAST(NFL_player_id AS VARCHAR)) % ? AS INTEGER) = ?
                  AND CAST(hash(CAST(NFL_player_id AS VARCHAR)) % ? AS INTEGER) = ?
                """,
                [career_buckets, career_bucket, player_sub_buckets, sub_bucket],
            )
            source_rows += _table_rows(con, source)
            materialize_career_compact_outer_rows(
                con,
                season_compact_table=source,
                output_table=part,
            )
            part_tables.append(part)
            con.execute(f"DROP TABLE {source}")
        union_sql = " UNION ALL ".join(f"SELECT * FROM {part}" for part in part_tables)
        con.execute("CREATE TABLE research_matchup_compact_career AS " + union_sql)
        duplicate_career = con.execute("""
          SELECT COUNT(*) FROM (
            SELECT NFL_player_id,position FROM research_matchup_compact_career
            GROUP BY 1,2 HAVING COUNT(*) <> 1
          )
        """).fetchone()[0]
        if duplicate_career:
            raise RuntimeError(f"compact career part overlaps internally: {duplicate_career}")
        _log_phase(
            "career_part",
            bucket=f"{career_bucket + 1}/{career_buckets}",
            season_rows=source_rows,
            career_rows=_table_rows(con, "research_matchup_compact_career"),
            player_sub_buckets=player_sub_buckets,
            seconds=f"{perf_counter() - started:.3f}",
        )
        for part in part_tables:
            con.execute(f"DROP TABLE {part}")
    finally:
        con.close()


def assemble_compact_career_parts(
    *,
    base_bundle: Path,
    career_parts: list[Path],
    output: Path,
) -> None:
    """Append exact disjoint career parts onto the already merged compact base."""
    if not base_bundle.is_file():
        raise FileNotFoundError(f"missing compact base bundle: {base_bundle}")
    if not career_parts:
        raise ValueError("at least one compact career part is required")
    if output.exists():
        raise FileExistsError(f"refusing to overwrite compact assembled output: {output}")
    for part in career_parts:
        if not part.is_file():
            raise FileNotFoundError(f"missing compact career part: {part}")
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output))
    started = perf_counter()
    try:
        con.execute("SET preserve_insertion_order=false")
        con.execute("SET threads=2")
        con.execute(f"ATTACH '{_quote_path(base_bundle)}' AS base (READ_ONLY)")
        for table in TABLES:
            con.execute(f"CREATE TABLE {table} AS SELECT * FROM base.{table}")
        for index, part in enumerate(career_parts):
            alias = f"career_part_{index}"
            con.execute(f"ATTACH '{_quote_path(part)}' AS {alias} (READ_ONLY)")
            if index == 0:
                con.execute(
                    f"CREATE TABLE research_matchup_compact_career AS "
                    f"SELECT * FROM {alias}.research_matchup_compact_career"
                )
            else:
                con.execute(
                    f"INSERT INTO research_matchup_compact_career "
                    f"SELECT * FROM {alias}.research_matchup_compact_career"
                )
        materialize_compact_exact_core_selector_indices(
            con,
            output_table="research_matchup_compact_career",
        )
        materialize_compact_exact_grade_selector_indices(
            con,
            output_table="research_matchup_compact_career",
        )
        duplicate_career = con.execute("""
          SELECT COUNT(*) FROM (
            SELECT NFL_player_id,position FROM research_matchup_compact_career
            GROUP BY 1,2 HAVING COUNT(*) <> 1
          )
        """).fetchone()[0]
        if duplicate_career:
            raise RuntimeError(f"compact career parts overlap: {duplicate_career}")
        materialize_compact_grade_sidecar(con, source_table="research_matchup_compact_season", output_table="research_matchup_compact_grade_season", grain="season")
        materialize_compact_grade_sidecar(con, source_table="research_matchup_compact_career", output_table="research_matchup_compact_grade_career", grain="career")
        _log_phase(
            "parallel_career_assemble",
            weekly_rows=_table_rows(con, "research_matchup_compact_weekly"),
            season_rows=_table_rows(con, "research_matchup_compact_season"),
            career_rows=_table_rows(con, "research_matchup_compact_career"),
            seconds=f"{perf_counter() - started:.3f}",
        )
    finally:
        con.close()


def merge_parts(
    parts: list[Path],
    output: Path,
    *,
    include_career: bool = True,
    career_buckets: int = 16,
) -> None:
    """Merge disjoint compact task outputs; never consume raw cache facts."""
    if not parts:
        raise ValueError("at least one compact part is required")
    if output.exists():
        raise FileExistsError(f"refusing to overwrite compact merged output: {output}")
    for part in parts:
        if not part.is_file():
            raise FileNotFoundError(f"missing compact part: {part}")
    output.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(output))
    merge_started = perf_counter()
    try:
        for index, part in enumerate(parts):
            lane_started = perf_counter()
            alias = f"part_{index}"
            con.execute(f"ATTACH '{_quote_path(part)}' AS {alias} (READ_ONLY)")
            lane_rows = {table: _table_rows(con, f"{alias}.{table}") for table in TABLES}
            for table in TABLES:
                if index == 0:
                    con.execute(f"CREATE TABLE {table} AS SELECT * FROM {alias}.{table}")
                else:
                    con.execute(f"INSERT INTO {table} SELECT * FROM {alias}.{table}")
            _log_phase(
                "append_lane",
                lane=f"{index + 1}/{len(parts)}",
                weekly_rows=lane_rows["research_matchup_compact_weekly"],
                season_rows=lane_rows["research_matchup_compact_season"],
                seconds=f"{perf_counter() - lane_started:.3f}",
            )
        duplicate_weekly = con.execute("""
          SELECT COUNT(*) FROM (
            SELECT NFL_player_id,year,week,position FROM research_matchup_compact_weekly
            GROUP BY 1,2,3,4 HAVING COUNT(*) <> 1
          )
        """).fetchone()[0]
        duplicate_season = con.execute("""
          SELECT COUNT(*) FROM (
            SELECT NFL_player_id,year,position FROM research_matchup_compact_season
            GROUP BY 1,2,3 HAVING COUNT(*) <> 1
          )
        """).fetchone()[0]
        if duplicate_weekly or duplicate_season:
            raise RuntimeError(
                f"compact parts overlap: weekly={duplicate_weekly}, season={duplicate_season}"
            )
        _log_phase(
            "append_integrity",
            weekly_duplicate_outer_keys=duplicate_weekly,
            season_duplicate_outer_keys=duplicate_season,
        )
        if include_career:
            _materialize_batched_career(con, career_buckets=career_buckets)
        _log_phase(
            "merge_complete",
            weekly_rows=_table_rows(con, "research_matchup_compact_weekly"),
            season_rows=_table_rows(con, "research_matchup_compact_season"),
            career_rows=(
                _table_rows(con, "research_matchup_compact_career")
                if include_career
                else 0
            ),
            seconds=f"{perf_counter() - merge_started:.3f}",
        )
    finally:
        con.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parts", nargs="+", type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--no-career", action="store_true")
    parser.add_argument("--career-buckets", type=int, default=16)
    parser.add_argument("--season-bundle", type=Path)
    parser.add_argument("--career-bucket", type=int)
    parser.add_argument("--player-sub-buckets", type=int, default=1)
    parser.add_argument("--base-bundle", type=Path)
    parser.add_argument("--career-parts", nargs="+", type=Path)
    args = parser.parse_args()
    if args.season_bundle is not None:
        if args.parts or args.base_bundle or args.career_parts or args.career_bucket is None:
            parser.error("--season-bundle requires --career-bucket and cannot be combined with merge inputs")
        build_compact_career_part(
            season_bundle=args.season_bundle,
            output=args.output,
            career_buckets=args.career_buckets,
            career_bucket=args.career_bucket,
            player_sub_buckets=args.player_sub_buckets,
        )
        return
    if args.base_bundle is not None:
        if args.parts or args.career_parts is None or args.career_bucket is not None:
            parser.error("--base-bundle requires --career-parts and cannot be combined with lane merge inputs")
        assemble_compact_career_parts(
            base_bundle=args.base_bundle,
            career_parts=args.career_parts,
            output=args.output,
        )
        return
    if not args.parts:
        parser.error("--parts is required for a lane merge")
    merge_parts(
        args.parts,
        args.output,
        include_career=not args.no_career,
        career_buckets=args.career_buckets,
    )


if __name__ == "__main__":
    main()
