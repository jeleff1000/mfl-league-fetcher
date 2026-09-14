"""Build one compact research-matchup task from the immutable GitHub cache.

The task emits only UI-shaped outer records.  It never mutates the cache,
uploads raw facts, or reads Fly.  A caller partitions emitted players with a
deterministic hash while every denominator continues to use the full cached
league-position inventory.
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

import duckdb

from fantasy_football_data_scripts.nfl_data.nfl_franchises import NFLVERSE_TO_DISPLAY
from scripts.research_cohorts.compact_matchup_cells import (
    materialize_narrow_position_year_source,
    materialize_compact_core_selector_indices,
    materialize_compact_grade_selector_indices,
    materialize_season_bracket_grade_position_batch,
    materialize_season_compact_outer_rows,
    materialize_season_core_cohort_position_batch,
    materialize_weekly_core_cohort_cells,
    validate_narrow_position_capacity_allocation,
)


# Match the only positions the research UI can serve.  P and OL are not an
# IDP lane and must not leak into an IDP selector.
POSITIONS = ("QB", "RB", "WR", "TE", "K", "DEF", "DB", "DL", "LB")


def assert_compact_selector_integrity(
    connection: duckdb.DuckDBPyConnection,
    *,
    table: str,
    include_grades: bool,
) -> None:
    """Fail before artifact upload if any packed selector is malformed.

    The API reads a fixed ordinal from one of these lists.  A selector that is
    short, null, or beyond its final packed cell list is not recoverable at
    query time; reject the lane while it is still an Actions-only artifact.
    """

    selector_sets = [
        ("core", "core_selector_indices", "cohort_cells", 288),
        ("roster", "roster_selector_indices", "roster_cells", 288),
    ]
    if include_grades:
        selector_sets.append(("grade", "grade_selector_indices", "grade_cells", 864))
    for label, selector_column, cell_column, expected_requests in selector_sets:
        bad_shape, bad_pointer = connection.execute(
            f"""
            WITH bad_shape AS (
              SELECT COUNT(*)::BIGINT AS count
              FROM {table}
              WHERE list_count({selector_column}) <> ?
            ),
            bad_pointer AS (
              SELECT COUNT(*)::BIGINT AS count
              FROM {table} row
              CROSS JOIN UNNEST(row.{selector_column}) AS pointers(pointer)
              WHERE pointer IS NULL
                 OR pointer < 1
                 OR pointer > list_count(row.{cell_column})
            )
            SELECT bad_shape.count, bad_pointer.count
            FROM bad_shape CROSS JOIN bad_pointer
            """,
            [expected_requests],
        ).fetchone()
        if bad_shape or bad_pointer:
            raise RuntimeError(
                f"{table} has invalid {label} selectors: "
                f"shape={bad_shape}, pointers={bad_pointer}"
            )


def emit_clutch_outlier_diagnostics(connection: duckdb.DuckDBPyConnection) -> None:
    """Log the small source witness set behind an implausible compact clutch cell."""

    rows = connection.execute(
        """
        WITH outliers AS (
          SELECT DISTINCT
            CAST(s.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            CAST(s.position AS VARCHAR) AS position
          FROM research_matchup_compact_season s
          CROSS JOIN UNNEST(s.cohort_cells) AS cells(cell)
          WHERE ABS(COALESCE(cell.clutch_season_sum, 0.0)) > 50
        )
        SELECT
          p.NFL_player_id, p.week, p.db_name, p.is_rostered, p.is_started,
          p.win, p.clutch_equity, p.team_points, p.fantasy_points
        FROM _research_position_fanout p
        INNER JOIN outliers o
          ON o.NFL_player_id = CAST(p.NFL_player_id AS VARCHAR)
         AND o.year = CAST(p.year AS INTEGER)
         AND o.position = CAST(p.position AS VARCHAR)
        WHERE CAST(p.is_started AS INTEGER) = 1
        ORDER BY ABS(COALESCE(CAST(p.clutch_equity AS DOUBLE), 0.0)) DESC,
          p.NFL_player_id, p.week
        LIMIT 50
        """
    ).fetchall()
    if rows:
        print("compact_clutch_outlier_witness=" + json.dumps(rows, default=str), flush=True)


def _canonical_nfl_team_sql(expression: str) -> str:
    """Normalize all known NFLverse team aliases to the shared display code."""
    clauses = " ".join(
        f"WHEN '{source}' THEN '{target}'"
        for source, target in sorted(NFLVERSE_TO_DISPLAY.items())
    )
    return (
        f"CASE UPPER(NULLIF(TRIM(CAST({expression} AS VARCHAR)), '')) "
        f"{clauses} ELSE UPPER(NULLIF(TRIM(CAST({expression} AS VARCHAR)), '')) END"
    )


def materialize_normalized_player_team_game_week(
    connection: duckdb.DuckDBPyConnection,
    *,
    cached_weeks_table: str,
    nfl_stats_table: str,
    target_players_table: str,
    output_table: str,
    year: int,
) -> None:
    """Use cached player weeks, supplementing only missing player-years canonically.

    The immutable cache remains the preferred availability dictionary.  The
    already attached supertable contributes any omitted team game week via the
    shared NFLverse-to-display team mapping. This temporary relation is used by
    weekly, season core, and season grade so all compact output paths have
    exactly the same calendar identity.
    """

    player_team = _canonical_nfl_team_sql("s.nfl_team")
    schedule_team = _canonical_nfl_team_sql("s.nfl_team")
    connection.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE {output_table} AS
        WITH targets AS (
          SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
          FROM {target_players_table}
        ),
        cached AS (
          SELECT DISTINCT
            CAST(w.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(w.year AS INTEGER) AS year,
            CAST(w.week AS INTEGER) AS week
          FROM {cached_weeks_table} w
          INNER JOIN targets t ON t.NFL_player_id = CAST(w.NFL_player_id AS VARCHAR)
          WHERE CAST(w.year AS INTEGER) = ?
            AND CAST(w.week AS INTEGER) BETWEEN 1 AND 18
        ),
        player_teams AS (
          SELECT
            CAST(s.NFL_player_id AS VARCHAR) AS NFL_player_id,
            CAST(s.year AS INTEGER) AS year,
            {player_team} AS nfl_team
          FROM {nfl_stats_table} s
          INNER JOIN targets t ON t.NFL_player_id = CAST(s.NFL_player_id AS VARCHAR)
          WHERE CAST(s.year AS INTEGER) = ?
            AND CAST(s.week AS INTEGER) BETWEEN 1 AND 18
            AND COALESCE(s.season_type, 'REG') = 'REG'
          GROUP BY 1,2,3
        ),
        supplemented AS (
          SELECT DISTINCT
            p.NFL_player_id,
            p.year,
            CAST(s.week AS INTEGER) AS week
          FROM player_teams p
          INNER JOIN {nfl_stats_table} s
            ON CAST(s.year AS INTEGER) = p.year
           AND {schedule_team} = p.nfl_team
          WHERE CAST(s.week AS INTEGER) BETWEEN 1 AND 18
            AND COALESCE(s.season_type, 'REG') = 'REG'
        )
        SELECT * FROM cached
        UNION
        SELECT * FROM supplemented
        """,
        [year, year],
    )


def configure_memory_bounded_rollup(
    connection: duckdb.DuckDBPyConnection,
    spill_directory: Path,
) -> None:
    """Bound a large cohort cube and give DuckDB a task-local spill target.

    A six-dimension ``CUBE`` has 64 grouping sets.  The compact output remains
    small, but one source-year can temporarily need substantially more memory
    while DuckDB builds those groups.  One thread and an 8 GiB buffer budget
    keep peak memory within the runner's RAM and allow the normal DuckDB
    out-of-core path to use this task's ephemeral spill directory.
    """

    spill_directory.mkdir(parents=True, exist_ok=True)
    spill_path = spill_directory.resolve().as_posix().replace("'", "''")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("SET enable_progress_bar=false")
    connection.execute("SET threads=1")
    connection.execute("SET memory_limit='8GiB'")
    connection.execute(f"SET temp_directory='{spill_path}'")
    connection.execute("SET max_temp_directory_size='40GiB'")


def audit_capacity_allocation(
    snapshot: Path,
    *,
    year: int,
    position: str,
    spill_directory: Path | None = None,
) -> dict[str, object]:
    """Fail fast on one immutable-cache position/year allocation, without output.

    This is the runner preflight: it uses the exact narrow fanout and capacity
    gate used by the compact build, but it never opens an output database,
    attaches Fly, or writes any cache-derived artifact.
    """

    if position not in POSITIONS:
        raise ValueError(f"unsupported research position: {position}")
    if not snapshot.is_file():
        raise FileNotFoundError(f"immutable snapshot not found: {snapshot}")
    audit_spill = spill_directory or snapshot.parent / ".capacity-audit-spill"
    con = duckdb.connect(":memory:")
    try:
        configure_memory_bounded_rollup(con, audit_spill)
        con.execute(f"ATTACH '{snapshot.resolve().as_posix()}' AS lake (READ_ONLY)")
        materialize_narrow_position_year_source(
            con,
            source_table="lake.public.player_fantasy",
            output_table="_research_position_fanout",
            year=year,
            position=position,
        )
        result = validate_narrow_position_capacity_allocation(
            con,
            source_table="_research_position_fanout",
        )
        result.update({"year": year, "position": position})
        return result
    finally:
        con.close()


def compact_outer_mismatch_diagnostics(
    connection: duckdb.DuckDBPyConnection,
    core_table: str,
    grade_table: str,
    stats_table: str,
    source_table: str,
) -> list[dict[str, object]]:
    """Describe the regular-stats calendar coverage for only mismatched keys."""
    rows = connection.execute(
        f"""
        WITH mismatch AS (
          SELECT
            COALESCE(c.NFL_player_id, g.NFL_player_id) AS NFL_player_id,
            COALESCE(c.year, g.year) AS year,
            COALESCE(c.position, g.position) AS position,
            CASE WHEN c.NFL_player_id IS NULL THEN 'grade_only' ELSE 'core_only' END AS mismatch
          FROM {core_table} c
          FULL OUTER JOIN {grade_table} g
            ON g.NFL_player_id = c.NFL_player_id
           AND g.year = c.year
           AND g.position = c.position
          WHERE c.NFL_player_id IS NULL OR g.NFL_player_id IS NULL
        ), player_stats AS (
          SELECT
            m.NFL_player_id, m.year,
            COUNT(s.week)::BIGINT AS regular_stat_rows,
            COUNT(s.nfl_team) FILTER (WHERE NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '') IS NOT NULL)::BIGINT AS non_null_team_rows,
            COALESCE(LIST(DISTINCT NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '')) FILTER (WHERE NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '') IS NOT NULL), []) AS teams,
            MODE(NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '')) AS team
          FROM mismatch m
          LEFT JOIN {stats_table} s
            ON CAST(s.NFL_player_id AS VARCHAR) = m.NFL_player_id
           AND CAST(s.year AS INTEGER) = m.year
           AND s.week IS NOT NULL
           AND COALESCE(s.season_type, 'REG') = 'REG'
          GROUP BY 1,2
        ), team_calendar AS (
          SELECT DISTINCT ps.NFL_player_id, ps.year, CAST(s.week AS INTEGER) AS week
          FROM player_stats ps
          INNER JOIN {stats_table} s
            ON CAST(s.year AS INTEGER) = ps.year
           AND NULLIF(TRIM(CAST(s.nfl_team AS VARCHAR)), '') = ps.team
          WHERE s.week IS NOT NULL AND COALESCE(s.season_type, 'REG') = 'REG'
        ), source_summary AS (
          SELECT
            m.NFL_player_id, m.year,
            COUNT(f.NFL_player_id)::BIGINT AS source_rows,
            COUNT(f.NFL_player_id) FILTER (WHERE CAST(f.is_rostered AS INTEGER)=1)::BIGINT AS explicit_rostered_source_rows,
            COALESCE(LIST(DISTINCT CAST(f.week AS INTEGER)) FILTER (WHERE f.week IS NOT NULL), []) AS source_weeks,
            COUNT(f.NFL_player_id) FILTER (WHERE tc.week IS NOT NULL)::BIGINT AS scheduled_source_rows
          FROM mismatch m
          LEFT JOIN {source_table} f
            ON CAST(f.NFL_player_id AS VARCHAR) = m.NFL_player_id
           AND CAST(f.year AS INTEGER) = m.year
          LEFT JOIN team_calendar tc
            ON tc.NFL_player_id=m.NFL_player_id AND tc.year=m.year AND tc.week=CAST(f.week AS INTEGER)
          GROUP BY 1,2
        ), eligible_profiles AS (
          SELECT DISTINCT
            CAST(db_name AS VARCHAR) AS db_name, CAST(year AS INTEGER) AS year,
            CAST(week AS INTEGER) AS week, CAST(position AS VARCHAR) AS position,
            cohort_teams, cohort_roster, cohort_scoring, cohort_pass_td,
            cohort_playoff_teams, cohort_dynasty, cohort_best_ball
          FROM {source_table}
          WHERE CAST(cohort_position_eligible AS INTEGER)=1
        ), eligible_league_weeks AS (
          SELECT DISTINCT db_name, year, week, position FROM eligible_profiles
        ), profile_summary AS (
          SELECT
            m.NFL_player_id, m.year,
            COUNT(f.NFL_player_id) FILTER (WHERE ep.db_name IS NOT NULL)::BIGINT AS resolved_profile_source_rows,
            COUNT(f.NFL_player_id) FILTER (WHERE ep.db_name IS NOT NULL AND tc.week IS NOT NULL)::BIGINT AS scheduled_resolved_profile_source_rows,
            COUNT(f.NFL_player_id) FILTER (WHERE elw.db_name IS NOT NULL)::BIGINT AS league_week_profile_source_rows,
            COUNT(f.NFL_player_id) FILTER (WHERE elw.db_name IS NOT NULL AND tc.week IS NOT NULL)::BIGINT AS scheduled_league_week_profile_source_rows
          FROM mismatch m
          LEFT JOIN {source_table} f
            ON CAST(f.NFL_player_id AS VARCHAR) = m.NFL_player_id
           AND CAST(f.year AS INTEGER) = m.year
          LEFT JOIN team_calendar tc
            ON tc.NFL_player_id=m.NFL_player_id AND tc.year=m.year AND tc.week=CAST(f.week AS INTEGER)
          LEFT JOIN eligible_profiles ep
            ON ep.db_name=CAST(f.db_name AS VARCHAR)
           AND ep.year=CAST(f.year AS INTEGER)
           AND ep.week=CAST(f.week AS INTEGER)
           AND ep.position=CAST(f.position AS VARCHAR)
           AND ep.cohort_teams IS NOT DISTINCT FROM f.cohort_teams
           AND ep.cohort_roster IS NOT DISTINCT FROM f.cohort_roster
           AND ep.cohort_scoring IS NOT DISTINCT FROM f.cohort_scoring
           AND ep.cohort_pass_td IS NOT DISTINCT FROM f.cohort_pass_td
           AND ep.cohort_playoff_teams IS NOT DISTINCT FROM f.cohort_playoff_teams
           AND ep.cohort_dynasty IS NOT DISTINCT FROM f.cohort_dynasty
           AND ep.cohort_best_ball IS NOT DISTINCT FROM f.cohort_best_ball
          LEFT JOIN eligible_league_weeks elw
            ON elw.db_name=CAST(f.db_name AS VARCHAR)
           AND elw.year=CAST(f.year AS INTEGER)
           AND elw.week=CAST(f.week AS INTEGER)
           AND elw.position=CAST(f.position AS VARCHAR)
          GROUP BY 1,2
        )
        SELECT
          m.NFL_player_id, m.year, m.position, m.mismatch,
          ps.regular_stat_rows, ps.non_null_team_rows, ps.teams,
          ss.source_rows, ss.explicit_rostered_source_rows, ss.source_weeks, ss.scheduled_source_rows,
          pfs.resolved_profile_source_rows, pfs.scheduled_resolved_profile_source_rows,
          pfs.league_week_profile_source_rows, pfs.scheduled_league_week_profile_source_rows
        FROM mismatch m
        INNER JOIN player_stats ps USING(NFL_player_id, year)
        INNER JOIN source_summary ss USING(NFL_player_id, year)
        INNER JOIN profile_summary pfs USING(NFL_player_id, year)
        ORDER BY 2,1,3
        LIMIT 20
        """
    ).fetchall()
    columns = (
        "NFL_player_id",
        "year",
        "position",
        "mismatch",
        "regular_stat_rows",
        "non_null_team_rows",
        "teams",
        "source_rows",
        "explicit_rostered_source_rows",
        "source_weeks",
        "scheduled_source_rows",
        "resolved_profile_source_rows",
        "scheduled_resolved_profile_source_rows",
        "league_week_profile_source_rows",
        "scheduled_league_week_profile_source_rows",
    )
    return [dict(zip(columns, row)) for row in rows]


def build_task(
    snapshot: Path,
    ops_cache: Path,
    output: Path,
    *,
    year: int,
    position: str,
    player_ids: list[str] | None = None,
    player_bucket_count: int = 1,
    player_bucket: int = 0,
) -> dict[str, object]:
    """Write one non-overlapping compact weekly+season task database."""
    if position not in POSITIONS:
        raise ValueError(f"unsupported research position: {position}")
    if player_ids is not None and not player_ids:
        raise ValueError("player_ids must not be empty when supplied")
    if player_ids is not None and (player_bucket_count != 1 or player_bucket != 0):
        raise ValueError("player_ids cannot be combined with hash player buckets")
    if player_bucket_count < 1 or not 0 <= player_bucket < player_bucket_count:
        raise ValueError(
            "player_bucket must be in [0, player_bucket_count)"
        )
    if output.exists():
        raise FileExistsError(f"refusing to overwrite compact task output: {output}")
    output.parent.mkdir(parents=True, exist_ok=True)
    phase_seconds: dict[str, float] = {}
    started_at = time.perf_counter()
    phase_started = started_at
    result: dict[str, object]
    con = duckdb.connect(str(output))
    try:
        configure_memory_bounded_rollup(con, output.parent / f".{output.stem}.spill")
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS lake (READ_ONLY)")
        con.execute(f"ATTACH '{ops_cache.as_posix()}' AS research_ops (READ_ONLY)")
        print("compact_task_phase=narrow_source", flush=True)
        materialize_narrow_position_year_source(
            con,
            source_table="lake.public.player_fantasy",
            output_table="_research_position_fanout",
            year=year,
            position=position,
        )
        print("compact_task_phase=narrow_source_materialized", flush=True)
        print("compact_task_phase=narrow_source_capacity_validation", flush=True)
        capacity_allocation = validate_narrow_position_capacity_allocation(
            con,
            source_table="_research_position_fanout",
        )
        print("compact_task_phase=narrow_source_capacity_validated", flush=True)
        print(
            "compact_task_capacity_allocation="
            + json.dumps(capacity_allocation, sort_keys=True),
            flush=True,
        )
        phase_seconds["narrow_source"] = round(time.perf_counter() - phase_started, 3)
        phase_started = time.perf_counter()
        if player_ids is None:
            con.execute(
                """
            CREATE OR REPLACE TEMP TABLE _research_target_players AS
            SELECT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
            FROM _research_position_fanout
            WHERE CAST(cohort_position_eligible AS INTEGER) = 1
            GROUP BY 1
            HAVING MAX(CASE WHEN is_rostered IS NULL OR CAST(is_rostered AS INTEGER) <> 0 THEN 1 ELSE 0 END) = 1
               AND CAST(hash(CAST(NFL_player_id AS VARCHAR)) % ? AS INTEGER) = ?
            """,
                [player_bucket_count, player_bucket],
            )
        else:
            con.execute("CREATE OR REPLACE TEMP TABLE _research_target_players (NFL_player_id VARCHAR)")
            con.executemany(
                "INSERT INTO _research_target_players VALUES (?)",
                [(player_id,) for player_id in player_ids],
            )
        target_players = con.execute(
            "SELECT COUNT(*) FROM _research_target_players"
        ).fetchone()[0]
        phase_seconds["target_players"] = round(time.perf_counter() - phase_started, 3)
        phase_started = time.perf_counter()
        materialize_normalized_player_team_game_week(
            con,
            cached_weeks_table="lake.public.player_team_game_week",
            nfl_stats_table="research_ops.nfl_historical.nfl_player_stats_all",
            target_players_table="_research_target_players",
            output_table="_research_player_team_game_week",
            year=year,
        )
        print(
            "compact_task_phase=weekly target_players=" + str(target_players),
            flush=True,
        )
        materialize_weekly_core_cohort_cells(
            con,
            source_table="_research_position_fanout",
            valid_player_weeks_table="_research_player_team_game_week",
            target_players_table="_research_target_players",
            output_table="research_matchup_compact_weekly",
            year=year,
            week=None,
            player_id=None,
            position=position,
            split_threshold=150,
        )
        materialize_compact_core_selector_indices(
            con,
            output_table="research_matchup_compact_weekly",
        )
        phase_seconds["weekly"] = round(time.perf_counter() - phase_started, 3)
        phase_started = time.perf_counter()
        print("compact_task_phase=season_core", flush=True)
        materialize_season_core_cohort_position_batch(
            con,
            source_table="_research_position_fanout",
            nfl_stats_table="research_ops.nfl_historical.nfl_player_stats_all",
            player_team_game_week_table="_research_player_team_game_week",
            player_active_week_table="lake.public.player_active_week",
            target_players_table="_research_target_players",
            output_table="_season_core",
            year=year,
            position=position,
            split_threshold=150,
        )
        phase_seconds["season_core"] = round(time.perf_counter() - phase_started, 3)
        phase_started = time.perf_counter()
        # Grade rows must share the post-floor core identity set.  The compact
        # core builder removes player/year rows that have no >=150 rostered
        # parent; carrying a grade-only row through would make the outer join
        # fail (or, worse, serve a low-sample grade without core metrics).
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE _research_grade_target_players AS
            SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) AS NFL_player_id
            FROM _season_core
            """
        )
        print("compact_task_phase=season_grades", flush=True)
        materialize_season_bracket_grade_position_batch(
            con,
            source_table="_research_position_fanout",
            nfl_stats_table="research_ops.nfl_historical.nfl_player_stats_all",
            player_regular_weeks_table="_research_player_team_game_week",
            target_players_table="_research_grade_target_players",
            output_table="_season_grades",
            year=year,
            position=position,
            split_threshold=150,
        )
        phase_seconds["season_grades"] = round(time.perf_counter() - phase_started, 3)
        phase_started = time.perf_counter()
        print("compact_task_phase=season_outer", flush=True)
        try:
            materialize_season_compact_outer_rows(
                con,
                core_outer_table="_season_core",
                grade_outer_table="_season_grades",
                output_table="research_matchup_compact_season",
            )
            materialize_compact_core_selector_indices(
                con,
                output_table="research_matchup_compact_season",
            )
            materialize_compact_grade_selector_indices(
                con,
                output_table="research_matchup_compact_season",
            )
        except RuntimeError:
            print(
                "compact_outer_mismatch_context="
                + json.dumps(
                    compact_outer_mismatch_diagnostics(
                        con,
                        "_season_core",
                        "_season_grades",
                        "research_ops.nfl_historical.nfl_player_stats_all",
                        "_research_position_fanout",
                    ),
                    sort_keys=True,
                ),
                flush=True,
            )
            raise
        assert_compact_selector_integrity(
            con,
            table="research_matchup_compact_weekly",
            include_grades=False,
        )
        assert_compact_selector_integrity(
            con,
            table="research_matchup_compact_season",
            include_grades=True,
        )
        print("compact_task_phase=selector_integrity_validated", flush=True)
        phase_seconds["season_outer"] = round(time.perf_counter() - phase_started, 3)
        duplicates = con.execute("""
          SELECT COUNT(*) FROM (
            SELECT NFL_player_id,year,week,position FROM research_matchup_compact_weekly
            GROUP BY 1,2,3,4 HAVING COUNT(*) <> 1
          )
        """).fetchone()[0]
        if duplicates:
            raise RuntimeError(f"weekly output has {duplicates} duplicate outer keys")
        result = {
            "year": year,
            "position": position,
            "capacity_allocation": capacity_allocation,
            "target_players": target_players,
            "weekly_outer_rows": con.execute(
                "SELECT COUNT(*) FROM research_matchup_compact_weekly"
            ).fetchone()[0],
            "season_outer_rows": con.execute(
                "SELECT COUNT(*) FROM research_matchup_compact_season"
            ).fetchone()[0],
            "phases_seconds": phase_seconds,
            "total_seconds": round(time.perf_counter() - started_at, 3),
        }
        print("compact_task_metrics=" + json.dumps(result, sort_keys=True), flush=True)
        con.execute("DROP TABLE _season_core")
        con.execute("DROP TABLE _season_grades")
    finally:
        con.close()
    result["output_bytes"] = output.stat().st_size
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot", required=True, type=Path)
    parser.add_argument("--ops-cache", type=Path)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--year", required=True, type=int)
    parser.add_argument("--position", required=True, choices=POSITIONS)
    parser.add_argument("--player-bucket-count", type=int, default=1)
    parser.add_argument("--player-bucket", type=int, default=0)
    parser.add_argument("--player-ids-json", default="")
    parser.add_argument("--capacity-only", action="store_true")
    args = parser.parse_args()
    if args.capacity_only:
        print(
            "compact_task_capacity_allocation="
            + json.dumps(
                audit_capacity_allocation(
                    args.snapshot,
                    year=args.year,
                    position=args.position,
                ),
                sort_keys=True,
            ),
            flush=True,
        )
        return
    if args.ops_cache is None or args.output is None:
        parser.error("--ops-cache and --output are required unless --capacity-only is set")
    build_task(
        args.snapshot,
        args.ops_cache,
        args.output,
        year=args.year,
        position=args.position,
        player_ids=json.loads(args.player_ids_json) if args.player_ids_json else None,
        player_bucket_count=args.player_bucket_count,
        player_bucket=args.player_bucket,
    )


if __name__ == "__main__":
    main()
