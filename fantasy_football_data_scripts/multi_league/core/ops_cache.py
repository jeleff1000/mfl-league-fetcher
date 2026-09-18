"""Shared finalized-week cache hydration; Fly is read-only, only the local cache is patched."""

from __future__ import annotations

import json
import shutil
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from multi_league.core.league_update_manifest import resolve_nfl_scoring_input_columns

OPS_DATABASE = "___ops"


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _quoted_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


_OPS_KEY_COLUMNS = ("NFL_player_id", "nfl_team", "opponent_nfl_team")
_OPS_REFRESH_EXCLUDED_COLUMNS = frozenset({"recon_correction_log"})


def _ops_revision_columns(schema_columns: Iterable[str]) -> tuple[str, ...]:
    """Use the same scoring/rank contract for source and local revision hashes."""
    return (*_OPS_KEY_COLUMNS, *resolve_nfl_scoring_input_columns(schema_columns))


def _finalized_ops(reader: Any, *, year: int, through_week: int | None) -> pd.DataFrame:
    """Read skinny revisions; rank values are hashed on Fly, not downloaded."""
    target = "nfl_historical.nfl_player_stats_all"
    schema = reader.query(f"DESCRIBE {target}", database=OPS_DATABASE)
    revision_columns = _ops_revision_columns(str(row["column_name"]) for row in schema)
    revision_expr = ", ".join(_quoted_identifier(column) for column in revision_columns)
    week_clause = f" AND week <= {int(through_week)}" if through_week is not None else ""
    return reader.query_df(
        "SELECT week, nfl_team, opponent_nfl_team, NFL_player_id, "
        f"CAST(hash({revision_expr}) AS VARCHAR) AS source_revision "
        f"FROM {target} "
        f"WHERE year = {int(year)} AND COALESCE(season_type, 'REG') = 'REG'{week_clause} "
        "AND NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL AND opponent_nfl_team IS NOT NULL",
        database=OPS_DATABASE,
    )


def _ops_refresh_source_projection(
    columns: list[str],
    fly_columns: set[str] | dict[str, str],
) -> str:
    """Project transform inputs without scanning unused large audit blobs."""
    return ", ".join(
        _quoted_identifier(column)
        if column in fly_columns and column not in _OPS_REFRESH_EXCLUDED_COLUMNS
        else f"NULL AS {_quoted_identifier(column)}"
        for column in columns
    )


def _ops_delta_keys(
    expected: pd.DataFrame,
    local: pd.DataFrame,
) -> tuple[set[tuple[str, str, str]], set[tuple[str, str, str]]]:
    """Return authoritative rows to fetch and local rows to replace/delete."""

    def revisions(frame: pd.DataFrame) -> dict[tuple[str, str, str], str]:
        if frame.empty:
            return {}
        return {
            tuple(str(row[column]) for column in _OPS_KEY_COLUMNS): str(row["source_revision"])
            for _, row in frame.iterrows()
        }

    expected_revisions = revisions(expected)
    local_revisions = revisions(local)
    fetch_keys = {
        key for key, revision in expected_revisions.items()
        if local_revisions.get(key) != revision
    }
    removed_keys = set(local_revisions) - set(expected_revisions)
    return fetch_keys, fetch_keys | removed_keys


def _patch_research_ops_cache_from_fly(
    reader: Any,
    finalized_ops: pd.DataFrame,
    *,
    base: Path,
    year: int,
    weeks: list[int],
    work_dir: Path,
    in_place: bool = False,
    authoritative_weeks: bool = False,
) -> Path:
    """Patch the reduced research cache from Fly's finalized source rows.

    The normal Actions cache intentionally contains only the wide weekly
    super-table, not the complete eight-table artifact required by
    ``refresh_live_nfl_ops.py``.  The canonical Fly ops table is therefore the
    authority here: replace only the completed-game rows in its one joined
    table before SQL enrichment.  The default retains a private copy for a
    reusable local cache; a GitHub Actions weekly worker may patch its
    disposable restored workspace cache in place and avoid copying 739 MB.
    Imports supply complete week key sets, including empty removed weeks;
    partial weekly callers must not remove games outside their admitted scope.
    """
    from multi_league.core.league_update_timing import PhaseTimer

    timer = PhaseTimer()
    if not base.is_file():
        raise RuntimeError(f"OPS_CACHE_PATH is missing: {base}")
    output = base if in_place else work_dir / "ops_cache_fly_finalized.duckdb"
    if not in_place:
        shutil.copy2(base, output)

    cache = duckdb.connect(str(output))
    timer.mark("cache_open")
    try:
        cache_columns = [
            str(row[0])
            for row in cache.execute("DESCRIBE nfl_historical.nfl_player_stats_all").fetchall()
        ]
        required = {"NFL_player_id", "year", "week", "season_type", "nfl_team", "opponent_nfl_team"}
        missing = sorted(required - set(cache_columns))
        if missing:
            raise RuntimeError("research ops cache is missing required weekly columns: " + ", ".join(missing))
        target = "nfl_historical.nfl_player_stats_all"
        fly_schema_rows = reader.query(f"DESCRIBE {target}", database=OPS_DATABASE)
        timer.mark("fly_schema")
        fly_columns = {
            str(row["column_name"]): str(row["column_type"])
            for row in fly_schema_rows
            if row.get("column_name") and row.get("column_type")
        }
        missing_fly = sorted(required - set(fly_columns))
        if missing_fly:
            raise RuntimeError("Fly ops table is missing required weekly columns: " + ", ".join(missing_fly))
        # The cache was just built from the active league's bounded scoring
        # projection.  Patch only that schema; expanding it back to every Fly
        # column would recreate the wide 1,100-column download this worker is
        # specifically designed to avoid.
        columns = cache_columns
        select_columns = ", ".join(_quoted_identifier(column) for column in columns)
        source_select = _ops_refresh_source_projection(columns, fly_columns)
        revision_columns = _ops_revision_columns(fly_columns)
        timer.mark("schema_alignment")
        for week in weeks:
            expected = finalized_ops.loc[finalized_ops["week"].astype(int) == int(week)].copy()
            if expected.empty and not authoritative_weeks:
                raise RuntimeError(f"no finalized Fly ops rows for {year} week {week}")

            expected_keys = set(
                zip(
                    expected["NFL_player_id"].astype(str),
                    expected["nfl_team"].astype(str),
                    expected["opponent_nfl_team"].astype(str),
                )
            )
            can_fetch_delta = (
                "source_revision" in expected.columns
                and set(revision_columns).issubset(columns)
            )
            local_revision_columns = tuple(column for column in revision_columns if column in columns)
            revision_expr = ", ".join(_quoted_identifier(column) for column in local_revision_columns)
            if "source_revision" in expected.columns and not can_fetch_delta and expected_keys:
                # Global freshness hashes every variant. A reduced cache only
                # compares the inputs it holds; read hashes, not wide rows.
                admission_values = ", ".join(
                    "(" + ", ".join(_sql_literal(value) for value in key) + ")"
                    for key in sorted(expected_keys)
                )
                expected = reader.query_df(
                    f"SELECT NFL_player_id, nfl_team, opponent_nfl_team, "
                    f"CAST(hash({revision_expr}) AS VARCHAR) AS source_revision "
                    f"FROM {target} WHERE year = {int(year)} AND week = {int(week)} "
                    "AND COALESCE(season_type, 'REG') = 'REG' "
                    f"AND (NFL_player_id, nfl_team, opponent_nfl_team) IN (VALUES {admission_values})",
                    database=OPS_DATABASE,
                )
                if (set(expected[list(_OPS_KEY_COLUMNS)].itertuples(index=False, name=None)) != expected_keys
                        or len(expected) != len(expected_keys)):
                    raise RuntimeError(f"Fly cache revision keys changed for {year} week {week}")
                can_fetch_delta = True
            if can_fetch_delta or authoritative_weeks:
                revision_select = f"CAST(hash({revision_expr}) AS VARCHAR)" if can_fetch_delta else "NULL"
                local_revisions = cache.execute(
                    f"SELECT NFL_player_id, nfl_team, opponent_nfl_team, "
                    f"{revision_select} AS source_revision "
                    f"FROM {target} WHERE year = ? AND week = ? "
                    "AND COALESCE(season_type, 'REG') = 'REG' "
                    "AND NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL "
                    "AND opponent_nfl_team IS NOT NULL",
                    [int(year), int(week)],
                ).fetchdf()
                if not authoritative_weeks:
                    games = set(zip(expected["nfl_team"], expected["opponent_nfl_team"]))
                    local_revisions = local_revisions.loc[
                        [(team, opponent) in games for team, opponent in zip(
                            local_revisions["nfl_team"], local_revisions["opponent_nfl_team"]
                        )]
                    ]
                if not can_fetch_delta:
                    expected = expected.assign(source_revision="refresh")
                fetch_keys, delete_keys = _ops_delta_keys(expected, local_revisions)
            else:
                fetch_keys = expected_keys
                delete_keys = expected_keys

            if fetch_keys:
                values = ", ".join(
                    "(" + ", ".join(_sql_literal(value) for value in key) + ")"
                    for key in sorted(fetch_keys)
                )
                source_sql = (
                    f"SELECT {source_select} FROM {target} "
                    f"WHERE year = {int(year)} AND week = {int(week)} "
                    "AND COALESCE(season_type, 'REG') = 'REG' "
                    "AND (NFL_player_id, nfl_team, opponent_nfl_team) "
                    f"IN (VALUES {values})"
                )
                parquet_query = getattr(reader, "query_df_parquet", None)
                source = (
                    parquet_query(source_sql, database=OPS_DATABASE)
                    if parquet_query is not None
                    else reader.query_df(source_sql, database=OPS_DATABASE)
                )
            else:
                source = pd.DataFrame(columns=columns)
            timer.mark(f"week_{int(week)}_fetch")
            if fetch_keys and source.empty:
                raise RuntimeError(f"Fly returned no changed finalized ops rows for {year} week {week}")
            source = source.reindex(columns=columns).copy()
            incoming_keys = set(
                zip(
                    source["NFL_player_id"].astype(str),
                    source["nfl_team"].astype(str),
                    source["opponent_nfl_team"].astype(str),
                )
            )
            if incoming_keys != fetch_keys or len(source) != len(fetch_keys):
                raise RuntimeError(
                    f"Fly cache refresh delta does not match finalized admission facts for {year} week {week}"
                )
            timer.mark(f"week_{int(week)}_validate")
            delete_frame = pd.DataFrame(sorted(delete_keys), columns=_OPS_KEY_COLUMNS)
            cache.register("__refresh_keys", delete_frame)
            try:
                cache.execute("BEGIN TRANSACTION")
                cache.execute(
                    f"""
                    DELETE FROM {target} AS current
                    WHERE current.year = ?
                      AND current.week = ?
                      AND COALESCE(current.season_type, 'REG') = 'REG'
                      AND EXISTS (
                        SELECT 1
                        FROM __refresh_keys AS incoming
                        WHERE current.NFL_player_id = incoming.NFL_player_id
                          AND current.nfl_team = incoming.nfl_team
                          AND current.opponent_nfl_team = incoming.opponent_nfl_team
                      )
                    """,
                    [int(year), int(week)],
                )
                if not source.empty:
                    cache.register("__refresh_facts", source)
                    try:
                        cache.execute(
                            f"INSERT INTO {target} ({select_columns}) "
                            f"SELECT {select_columns} FROM __refresh_facts"
                        )
                    finally:
                        cache.unregister("__refresh_facts")
                cache.execute("COMMIT")
            except Exception:
                cache.execute("ROLLBACK")
                raise
            finally:
                cache.unregister("__refresh_keys")
            timer.mark(f"week_{int(week)}_replace")
    finally:
        cache.close()
        timer.mark("cache_close")
        diagnostic = "[ops-cache-timing] " + json.dumps({
            "year": int(year), "weeks": [int(week) for week in weeks], "phases": timer.finish(),
        })
        try:
            print(diagnostic, flush=True)
        except (OSError, ValueError):
            # A closed diagnostic sink must not replace a fetch exception or
            # turn an otherwise successful cache patch into a failed update.
            pass

    return output
