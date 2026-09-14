"""Server-side league-to-league merge copy for canonical Fly rows."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from multi_league.core.db_utils import sanitize_database_name

CENTRAL_DB = "___leagues"
DB_NAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
NONE_VALUE = "__none__"
MANAGER_COLUMNS = {"manager", "managers", "opponent"}
FRANCHISE_MANAGER_COLUMNS = {
    "franchise_id": "manager",
    "opponent_franchise_id": "opponent",
}
PLAYER_UNROSTERED_FILTER = "LOWER(TRIM(COALESCE(manager, ''))) NOT IN ('unrostered', 'fa', 'free agent', 'waivers', '')"
BUSY_READ_ATTEMPTS = 8
BUSY_READ_MAX_SLEEP_SECONDS = 30
BUSY_ERROR_MARKERS = (
    "query service is busy",
    "ops_writing",
    "query failed (502)",
    "query failed (503)",
    "service unavailable",
    "empty response body",
)


def _quote_sql(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _quote_ident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _assert_db_name(value: str) -> str:
    db_name = sanitize_database_name(value)
    if not DB_NAME_RE.match(db_name):
        raise ValueError(f"Invalid database name: {value!r}")
    return db_name


def _sanitize_manager_mappings(raw: Any) -> dict[str, str]:
    if not isinstance(raw, Mapping):
        return {}
    cleaned: dict[str, str] = {}
    for source, target in raw.items():
        source_name = str(source or "").strip()
        target_name = str(target or "").strip()
        if not source_name or not target_name or target_name == NONE_VALUE or source_name == target_name:
            continue
        cleaned[source_name] = target_name
    return cleaned


def _normalized_manager_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").strip().lower())


def _normalized_manager_sql(column: str) -> str:
    return f"LOWER(REGEXP_REPLACE(TRIM(COALESCE({_quote_ident(column)}, '')), '[^A-Za-z0-9]+', '', 'g'))"


def _normalized_manager_mappings(mappings: Mapping[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    conflicts: set[str] = set()
    for source, target in mappings.items():
        key = _normalized_manager_key(source)
        if not key:
            continue
        existing = normalized.get(key)
        if existing is not None and existing != target:
            conflicts.add(key)
            continue
        normalized[key] = target

    for key in conflicts:
        normalized.pop(key, None)
    return normalized


def _manager_case_sql(column: str, mappings: Mapping[str, str]) -> str:
    if not mappings:
        return _quote_ident(column)
    clauses = [
        f"WHEN {_quote_ident(column)} = {_quote_sql(source)} THEN {_quote_sql(target)}"
        for source, target in mappings.items()
    ]
    clauses.extend(
        f"WHEN {_normalized_manager_sql(column)} = {_quote_sql(source)} THEN {_quote_sql(target)}"
        for source, target in _normalized_manager_mappings(mappings).items()
    )
    return f"CASE {' '.join(clauses)} ELSE {_quote_ident(column)} END"


def _select_expr(column: str, target_db: str, mappings: Mapping[str, str]) -> str:
    if column == "db_name":
        return f"{_quote_sql(target_db)} AS {_quote_ident(column)}"
    if column in MANAGER_COLUMNS:
        return f"{_manager_case_sql(column, mappings)} AS {_quote_ident(column)}"
    return _quote_ident(column)


def _ctx_get(ctx: Any, key: str, default: Any = None) -> Any:
    if isinstance(ctx, Mapping):
        return ctx.get(key, default)
    return getattr(ctx, key, default)


def _ctx_is_single_year_import(ctx: Any) -> bool:
    prop = _ctx_get(ctx, "is_single_year_import", None)
    if isinstance(prop, bool):
        return prop
    import_mode = str(_ctx_get(ctx, "import_mode", "") or "").lower()
    if import_mode == "quick":
        return True
    start_year = _ctx_get(ctx, "start_year")
    end_year = _ctx_get(ctx, "end_year")
    if start_year is not None and end_year is not None:
        try:
            return int(start_year) == int(end_year)
        except (TypeError, ValueError):
            return False
    return False


def _year_list_sql(years: list[int]) -> str:
    return ", ".join(str(int(year)) for year in years)


def _table_ref(table_name: str) -> str:
    return f"{CENTRAL_DB}.public.{table_name}"


def _is_busy_reader_error(exc: Exception) -> bool:
    message = str(exc).lower()
    return any(marker in message for marker in BUSY_ERROR_MARKERS)


def _busy_read_sleep(attempt: int) -> None:
    time.sleep(min(2**attempt, BUSY_READ_MAX_SLEEP_SECONDS))


def _query_with_busy_retry(reader, sql: str, *, database: str = CENTRAL_DB):
    for attempt in range(BUSY_READ_ATTEMPTS):
        try:
            return reader.query(sql, database=database)
        except Exception as exc:
            if not _is_busy_reader_error(exc) or attempt == BUSY_READ_ATTEMPTS - 1:
                raise
            _busy_read_sleep(attempt)
    return []


def _query_scalar_with_busy_retry(reader, sql: str, *, database: str = CENTRAL_DB):
    for attempt in range(BUSY_READ_ATTEMPTS):
        try:
            return reader.query_scalar(sql, database=database)
        except Exception as exc:
            if not _is_busy_reader_error(exc) or attempt == BUSY_READ_ATTEMPTS - 1:
                raise
            _busy_read_sleep(attempt)
    return None


def _execute_with_busy_retry(writer, sql: str, *, database: str = CENTRAL_DB):
    for attempt in range(BUSY_READ_ATTEMPTS):
        try:
            return writer.execute(sql, database=database)
        except Exception as exc:
            if not _is_busy_reader_error(exc) or attempt == BUSY_READ_ATTEMPTS - 1:
                raise
            _busy_read_sleep(attempt)
    return None


def _query_years(reader, db_name: str) -> list[int]:
    rows = _query_with_busy_retry(
        reader,
        f"""
        SELECT DISTINCT TRY_CAST(year AS INTEGER) AS year
        FROM {_table_ref("matchup")}
        WHERE db_name = {_quote_sql(db_name)}
          AND year IS NOT NULL
        ORDER BY year
        """,
    )
    return [int(row["year"]) for row in rows if row.get("year") is not None]


def _query_year_scoped_tables(reader) -> list[str]:
    rows = _query_with_busy_retry(
        reader,
        f"""
        SELECT table_name
        FROM information_schema.columns
        WHERE table_catalog = '{CENTRAL_DB}'
          AND table_schema = 'public'
          AND column_name IN ('db_name', 'year')
        GROUP BY table_name
        HAVING COUNT(DISTINCT column_name) = 2
        ORDER BY table_name
        """,
    )
    return [str(row["table_name"]) for row in rows]


def _query_columns(reader, table_name: str) -> list[str]:
    rows = _query_with_busy_retry(
        reader,
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_catalog = '{CENTRAL_DB}'
          AND table_schema = 'public'
          AND table_name = {_quote_sql(table_name)}
        ORDER BY ordinal_position
        """,
    )
    return [str(row["column_name"]) for row in rows]


def _query_no_year_aggregate_tables(reader) -> list[str]:
    rows = _query_with_busy_retry(
        reader,
        f"""
        SELECT table_name
        FROM information_schema.columns
        WHERE table_catalog = '{CENTRAL_DB}'
          AND table_schema = 'public'
        GROUP BY table_name
        HAVING SUM(CASE WHEN column_name = 'db_name' THEN 1 ELSE 0 END) > 0
           AND SUM(CASE WHEN column_name = 'year' THEN 1 ELSE 0 END) = 0
        ORDER BY table_name
        """,
    )
    return [str(row["table_name"]) for row in rows]


def _merge_years_from_config(raw_merge_source: Mapping[str, Any]) -> list[int]:
    raw_merge_years = raw_merge_source.get("merge_years")
    if isinstance(raw_merge_years, list) and raw_merge_years:
        return sorted({int(year) for year in raw_merge_years})

    year_range = raw_merge_source.get("year_range")
    if isinstance(year_range, Mapping):
        start = year_range.get("start")
        end = year_range.get("end")
        if start is not None and end is not None:
            start_year = int(start)
            end_year = int(end)
            if end_year < start_year:
                start_year, end_year = end_year, start_year
            return list(range(start_year, end_year + 1))

    return []


def _available_columns(reader, table_name: str) -> set[str]:
    return set(_query_columns(reader, table_name))


def _has_columns(reader, table_name: str, required: set[str]) -> bool:
    return required.issubset(_available_columns(reader, table_name))


def _aggregate_count(reader, table_name: str, db_name: str) -> int:
    sql = f"SELECT COUNT(*) AS cnt FROM {_table_ref(table_name)} WHERE db_name = {_quote_sql(db_name)}"
    return int(_query_scalar_with_busy_retry(reader, sql) or 0)


def _execute_replace(writer, table_name: str, target_db: str, insert_sql: str) -> None:
    _execute_with_busy_retry(
        writer,
        f"""
        BEGIN TRANSACTION;
        DELETE FROM {_table_ref(table_name)} WHERE db_name = {_quote_sql(target_db)};
        {insert_sql};
        COMMIT;
        """,
    )


def _grade_from_gpa_sql(gpa_expr: str) -> str:
    return f"""
        CASE
            WHEN {gpa_expr} >= 3.85 THEN 'A+'
            WHEN {gpa_expr} >= 3.50 THEN 'A'
            WHEN {gpa_expr} >= 3.15 THEN 'A-'
            WHEN {gpa_expr} >= 2.85 THEN 'B+'
            WHEN {gpa_expr} >= 2.50 THEN 'B'
            WHEN {gpa_expr} >= 2.15 THEN 'B-'
            WHEN {gpa_expr} >= 1.85 THEN 'C+'
            WHEN {gpa_expr} >= 1.50 THEN 'C'
            WHEN {gpa_expr} >= 1.15 THEN 'C-'
            WHEN {gpa_expr} >= 0.85 THEN 'D+'
            WHEN {gpa_expr} >= 0.50 THEN 'D'
            WHEN {gpa_expr} >= 0.15 THEN 'D-'
            ELSE 'F'
        END
    """


def _load_merge_source_context(data_dir: str | Path | None) -> Any | None:
    """Load a saved context from a local import data directory if it has merge_source data."""
    if data_dir is None:
        return None

    root = Path(data_dir)
    candidates = [
        root / "sleeper_context.json",
        root / "espn_context.json",
        root / "league_context.json",
        root.parent / "sleeper_context.json",
        root.parent / "espn_context.json",
        root.parent / "league_context.json",
    ]
    for candidate in candidates:
        if not candidate.exists():
            continue
        try:
            with open(candidate, encoding="utf-8") as handle:
                data = json.load(handle)
        except Exception:
            continue
        if not isinstance(data, Mapping):
            continue
        has_single = isinstance(data.get("merge_source"), Mapping)
        has_many = isinstance(data.get("merge_sources"), list) and any(
            isinstance(source, Mapping) and source.get("source_db") for source in data.get("merge_sources", [])
        )
        if has_single or has_many:
            return SimpleNamespace(**data)
    return None


def maybe_copy_merge_source_to_public(
    *,
    data_dir: str | Path | None,
    target_db_name: str,
    reader=None,
    writer=None,
    log_func: Callable[[str], None] = print,
) -> dict[str, int | str]:
    """Copy merge_source rows after a final Fly upload when a saved context exists."""
    ctx = _load_merge_source_context(data_dir)
    if ctx is None:
        return {"status": "no_merge_source"}
    return copy_merge_source_to_public(
        ctx,
        target_db_name,
        reader=reader,
        writer=writer,
        log_func=log_func,
    )


def _remap_copied_franchise_ids(
    *,
    reader,
    writer,
    target_db: str,
    merge_years: list[int],
    log_func: Callable[[str], None],
) -> None:
    """Align copied historical franchise ids to target ids when manager names match."""
    if not merge_years:
        return

    year_sql = _year_list_sql(merge_years)
    try:
        target_ids = _query_scalar_with_busy_retry(
            reader,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {_table_ref("matchup")}
            WHERE db_name = {_quote_sql(target_db)}
              AND TRY_CAST(year AS INTEGER) NOT IN ({year_sql})
              AND franchise_id IS NOT NULL
            """,
        )
    except Exception:
        return

    if not target_ids:
        return

    for table_name in _query_year_scoped_tables(reader):
        columns = _available_columns(reader, table_name)
        if "db_name" not in columns or "year" not in columns:
            continue
        for franchise_col, name_col in FRANCHISE_MANAGER_COLUMNS.items():
            if franchise_col not in columns or name_col not in columns:
                continue
            _execute_with_busy_retry(
                writer,
                f"""
                UPDATE {_table_ref(table_name)} AS t
                SET {franchise_col} = ids.target_franchise_id
                FROM (
                    SELECT manager, ARG_MAX(franchise_id, TRY_CAST(year AS INTEGER)) AS target_franchise_id
                    FROM {_table_ref("matchup")}
                    WHERE db_name = {_quote_sql(target_db)}
                      AND TRY_CAST(year AS INTEGER) NOT IN ({year_sql})
                      AND manager IS NOT NULL
                      AND TRIM(manager) <> ''
                      AND franchise_id IS NOT NULL
                    GROUP BY manager
                ) AS ids
                WHERE t.db_name = {_quote_sql(target_db)}
                  AND TRY_CAST(t.year AS INTEGER) IN ({year_sql})
                  AND t.{name_col} = ids.manager
                  AND ids.target_franchise_id IS NOT NULL
                """,
            )

    log_func("[MERGE_SOURCE] Remapped copied franchise ids where target manager identities matched")


def _refresh_merge_source_aggregates(
    *,
    reader,
    writer,
    target_db: str,
    log_func: Callable[[str], None],
) -> dict[str, int]:
    """Refresh no-year aggregate tables after the historical row copy."""
    stats: dict[str, int] = {}

    refreshers = [
        _refresh_player_fantasy_career,
        _refresh_draft_manager_career,
        _refresh_draft_player_career,
        _refresh_transaction_manager_career,
        _refresh_transaction_player_career,
        _refresh_matchup_career,
        _refresh_matchup_h2h_career,
        _refresh_homepage_manager_rankings,
        _refresh_homepage_current_standings,
        _refresh_homepage_top_rivalries,
        _refresh_homepage_manager_profiles,
        _refresh_homepage_league_summary,
    ]
    for refresh in refreshers:
        stats.update(refresh(reader=reader, writer=writer, target_db=target_db, log_func=log_func))

    return stats


def _refresh_player_fantasy_career(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    stats: dict[str, int] = {}
    variants = [
        ("player_fantasy_season", "player_fantasy_career"),
        ("player_fantasy_season_all", "player_fantasy_career_all"),
    ]
    required = {"db_name", "NFL_player_id", "year", "player", "fantasy_points"}
    for source_table, target_table in variants:
        if not _has_columns(reader, source_table, required) or not _has_columns(
            reader, target_table, {"db_name", "NFL_player_id", "first_year", "last_year"}
        ):
            continue
        _execute_replace(
            writer,
            target_table,
            target_db,
            f"""
            INSERT INTO {_table_ref(target_table)} (
                db_name, NFL_player_id, first_year, last_year, years_active,
                player, position, nfl_team, fantasy_points, player_lamar, manager_lamar,
                clutch_equity, games_started, games_rostered, wins, losses, managers,
                franchise_id, team_points, opponent_points, playoff_games, playoff_wins,
                playoff_losses, championships, optimal_player_count,
                league_wide_optimal_count, fantasy_position, last_updated
            )
            SELECT
                {_quote_sql(target_db)} AS db_name,
                NFL_player_id,
                MIN(year) AS first_year,
                MAX(year) AS last_year,
                COUNT(DISTINCT year) AS years_active,
                ARG_MAX(player, year) AS player,
                ARG_MAX(position, year) AS position,
                ARG_MAX(nfl_team, year) AS nfl_team,
                SUM(COALESCE(fantasy_points, 0)) AS fantasy_points,
                SUM(COALESCE(player_lamar, 0)) AS player_lamar,
                SUM(COALESCE(manager_lamar, 0)) AS manager_lamar,
                SUM(COALESCE(clutch_equity, 0)) AS clutch_equity,
                SUM(COALESCE(games_started, 0)) AS games_started,
                SUM(COALESCE(games_rostered, 0)) AS games_rostered,
                SUM(COALESCE(wins, 0)) AS wins,
                SUM(COALESCE(losses, 0)) AS losses,
                COALESCE(
                    STRING_AGG(DISTINCT NULLIF(TRIM(managers), ''), ', ' ORDER BY NULLIF(TRIM(managers), '')),
                    'Unrostered'
                ) AS managers,
                ARG_MAX(franchise_id, year) AS franchise_id,
                SUM(COALESCE(team_points, 0)) AS team_points,
                SUM(COALESCE(opponent_points, 0)) AS opponent_points,
                SUM(COALESCE(playoff_games, 0)) AS playoff_games,
                SUM(COALESCE(playoff_wins, 0)) AS playoff_wins,
                SUM(COALESCE(playoff_losses, 0)) AS playoff_losses,
                SUM(COALESCE(championships, 0)) AS championships,
                SUM(COALESCE(optimal_player_count, 0)) AS optimal_player_count,
                SUM(COALESCE(league_wide_optimal_count, 0)) AS league_wide_optimal_count,
                ARG_MAX(fantasy_position, year) AS fantasy_position,
                CURRENT_TIMESTAMP AS last_updated
            FROM {_table_ref(source_table)}
            WHERE db_name = {_quote_sql(target_db)}
              AND NFL_player_id IS NOT NULL
            GROUP BY NFL_player_id
            """,
        )
        count = _aggregate_count(reader, target_table, target_db)
        stats[target_table] = count
        log_func(f"[MERGE_SOURCE] refreshed {target_table}: {count:,} rows")
    return stats


def _refresh_draft_manager_career(*, reader, writer, target_db: str, log_func: Callable[[str], None]) -> dict[str, int]:
    if not _has_columns(
        reader,
        "draft_manager_season",
        {"db_name", "manager", "year", "franchise_id", "draft_category", "picks", "total_manager_lamar"},
    ) or not _has_columns(reader, "draft_manager_career", {"db_name", "manager", "draft_category"}):
        return {}

    gpa_sql = """
        SUM(
            CASE manager_draft_grade
                WHEN 'A+' THEN 4.0 WHEN 'A' THEN 3.67 WHEN 'A-' THEN 3.33
                WHEN 'B+' THEN 3.0 WHEN 'B' THEN 2.67 WHEN 'B-' THEN 2.33
                WHEN 'C+' THEN 2.0 WHEN 'C' THEN 1.67 WHEN 'C-' THEN 1.33
                WHEN 'D+' THEN 1.0 WHEN 'D' THEN 0.67 WHEN 'D-' THEN 0.33
                WHEN 'F' THEN 0.0 ELSE NULL
            END * picks
        ) / NULLIF(SUM(CASE WHEN manager_draft_grade IS NOT NULL THEN picks ELSE 0 END), 0)
    """
    _execute_replace(
        writer,
        "draft_manager_career",
        target_db,
        f"""
        INSERT INTO {_table_ref("draft_manager_career")} (
            db_name, manager, franchise_id, draft_category, years_active,
            total_picks, total_keeper_picks, total_cost, total_manager_lamar,
            avg_manager_lamar, total_fantasy_points, avg_season_ppg,
            career_hit_rate, total_busts, total_breakouts, avg_pick_quality_zscore,
            avg_games_played, avg_lamar_per_dollar, career_gpa, career_grade,
            best_pick_player, best_pick_lamar, worst_pick_player, worst_pick_lamar,
            last_updated
        )
        WITH career AS (
            SELECT
                ARG_MAX(manager, year) AS manager,
                COALESCE(franchise_id, manager) AS grp_key,
                MAX(franchise_id) AS franchise_id,
                draft_category,
                COUNT(DISTINCT year) AS years_active,
                SUM(picks) AS total_picks,
                SUM(keeper_picks) AS total_keeper_picks,
                SUM(total_cost) AS total_cost,
                SUM(total_manager_lamar) AS total_manager_lamar,
                SUM(total_manager_lamar) / NULLIF(SUM(picks), 0) AS avg_manager_lamar,
                SUM(total_fantasy_points) AS total_fantasy_points,
                SUM(avg_season_ppg * picks) / NULLIF(SUM(picks), 0) AS avg_season_ppg,
                CAST(SUM(hits) AS DOUBLE) / NULLIF(SUM(picks), 0) AS career_hit_rate,
                SUM(busts) AS total_busts,
                SUM(breakouts) AS total_breakouts,
                SUM(avg_pick_quality_zscore * picks) / NULLIF(SUM(picks), 0) AS avg_pick_quality_zscore,
                SUM(avg_games_played * picks) / NULLIF(SUM(picks), 0) AS avg_games_played,
                SUM(total_manager_lamar) / NULLIF(SUM(total_cost), 0) AS avg_lamar_per_dollar,
                {gpa_sql} AS career_gpa,
                ARG_MAX(best_pick_player, best_pick_lamar) AS best_pick_player,
                MAX(best_pick_lamar) AS best_pick_lamar,
                ARG_MIN(worst_pick_player, worst_pick_lamar) AS worst_pick_player,
                MIN(worst_pick_lamar) AS worst_pick_lamar
            FROM {_table_ref("draft_manager_season")}
            WHERE db_name = {_quote_sql(target_db)}
            GROUP BY COALESCE(franchise_id, manager), draft_category
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            manager, franchise_id, draft_category, years_active, total_picks,
            total_keeper_picks, total_cost, total_manager_lamar, avg_manager_lamar,
            total_fantasy_points, avg_season_ppg, career_hit_rate, total_busts,
            total_breakouts, avg_pick_quality_zscore, avg_games_played,
            avg_lamar_per_dollar, career_gpa,
            {_grade_from_gpa_sql("career_gpa")} AS career_grade,
            best_pick_player, best_pick_lamar, worst_pick_player, worst_pick_lamar,
            CURRENT_TIMESTAMP AS last_updated
        FROM career
        """,
    )
    count = _aggregate_count(reader, "draft_manager_career", target_db)
    log_func(f"[MERGE_SOURCE] refreshed draft_manager_career: {count:,} rows")
    return {"draft_manager_career": count}


def _keeper_expr(cols: set[str], alias: str) -> str:
    if "is_keeper" in cols:
        return f"COALESCE({alias}.is_keeper, 0) = 1"
    if "is_keeper_status" in cols:
        return f"COALESCE({alias}.is_keeper_status, 0) = 1"
    if "is_keeper_cost" in cols:
        return f"COALESCE({alias}.is_keeper_cost, 0) = 1"
    return "FALSE"


def _refresh_draft_player_career(*, reader, writer, target_db: str, log_func: Callable[[str], None]) -> dict[str, int]:
    cols = _available_columns(reader, "draft")
    if not {"db_name", "player", "manager", "year"}.issubset(cols) or not _has_columns(
        reader, "draft_player_career", {"db_name", "player", "position"}
    ):
        return {}

    pos_col = "position" if "position" in cols else "yahoo_position" if "yahoo_position" in cols else None
    lamar_col = "manager_lamar" if "manager_lamar" in cols else "lamar" if "lamar" in cols else None
    if not pos_col or not lamar_col:
        return {}
    quality_expr = (
        "AVG(d.pick_quality_zscore)"
        if "pick_quality_zscore" in cols
        else "AVG(d.pick_quality_score)"
        if "pick_quality_score" in cols
        else "0"
    )
    points_expr = (
        "SUM(COALESCE(d.total_fantasy_points, 0))"
        if "total_fantasy_points" in cols
        else "SUM(COALESCE(d.points, 0))"
        if "points" in cols
        else "0"
    )
    ppg_expr = "AVG(d.season_ppg)" if "season_ppg" in cols else "0"
    cost_expr = "COALESCE(d.cost, 0)" if "cost" in cols else "0"
    category_expr = "COALESCE(d.draft_category, 'standard')" if "draft_category" in cols else "'standard'"
    keeper_expr = _keeper_expr(cols, "d")
    franchise_expr = (
        "STRING_AGG(DISTINCT d.franchise_id, ', ' ORDER BY d.franchise_id)" if "franchise_id" in cols else "NULL"
    )

    _execute_replace(
        writer,
        "draft_player_career",
        target_db,
        f"""
        INSERT INTO {_table_ref("draft_player_career")} (
            db_name, player, position, draft_category, times_drafted, times_kept,
            total_cost, keeper_cost, total_manager_lamar, avg_manager_lamar,
            lamar_per_dollar, total_fantasy_points, avg_season_ppg,
            avg_pick_quality_zscore, best_manager, managers, franchise_ids,
            years, last_updated
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            d.player,
            d.{pos_col} AS position,
            {category_expr} AS draft_category,
            SUM(CASE WHEN NOT ({keeper_expr}) THEN 1 ELSE 0 END) AS times_drafted,
            SUM(CASE WHEN ({keeper_expr}) THEN 1 ELSE 0 END) AS times_kept,
            SUM(CASE WHEN NOT ({keeper_expr}) THEN {cost_expr} ELSE 0 END) AS total_cost,
            SUM(CASE WHEN ({keeper_expr}) THEN {cost_expr} ELSE 0 END) AS keeper_cost,
            SUM(COALESCE(d.{lamar_col}, 0)) AS total_manager_lamar,
            AVG(COALESCE(d.{lamar_col}, 0)) AS avg_manager_lamar,
            SUM(COALESCE(d.{lamar_col}, 0))
                / NULLIF(SUM(CASE WHEN NOT ({keeper_expr}) THEN {cost_expr} ELSE 0 END), 0) AS lamar_per_dollar,
            {points_expr} AS total_fantasy_points,
            {ppg_expr} AS avg_season_ppg,
            {quality_expr} AS avg_pick_quality_zscore,
            ARG_MAX(d.manager, COALESCE(d.{lamar_col}, 0)) AS best_manager,
            STRING_AGG(DISTINCT d.manager, ', ' ORDER BY d.manager) AS managers,
            {franchise_expr} AS franchise_ids,
            STRING_AGG(DISTINCT CAST(d.year AS VARCHAR), ', ' ORDER BY CAST(d.year AS VARCHAR)) AS years,
            CURRENT_TIMESTAMP AS last_updated
        FROM {_table_ref("draft")} d
        WHERE d.db_name = {_quote_sql(target_db)}
          AND d.player IS NOT NULL AND TRIM(d.player) <> ''
          AND d.{pos_col} IS NOT NULL AND TRIM(d.{pos_col}) <> ''
        GROUP BY d.player, d.{pos_col}, {category_expr}
        """,
    )
    count = _aggregate_count(reader, "draft_player_career", target_db)
    log_func(f"[MERGE_SOURCE] refreshed draft_player_career: {count:,} rows")
    return {"draft_player_career": count}


def _refresh_transaction_manager_career(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    if not _has_columns(
        reader,
        "transaction_manager_season",
        {"db_name", "manager", "year", "franchise_id", "adds", "drops", "total_moves", "net_lamar"},
    ) or not _has_columns(reader, "transaction_manager_career", {"db_name", "manager", "franchise_id"}):
        return {}

    _execute_replace(
        writer,
        "transaction_manager_career",
        target_db,
        f"""
        INSERT INTO {_table_ref("transaction_manager_career")} (
            db_name, manager, franchise_id, seasons, total_adds, total_drops,
            total_moves, total_faab_bid, net_lamar, net_points_ros, efficiency,
            lamar_per_season, total_transaction_score, avg_transaction_score,
            avg_timing_mult, transaction_grade, transaction_gpa, total_trades,
            trade_net_lamar, trade_avg_net, trade_win_rate, trade_net_points,
            last_updated
        )
        WITH career AS (
            SELECT
                ARG_MAX(manager, year) AS manager,
                MAX(franchise_id) AS franchise_id,
                COUNT(DISTINCT year) AS seasons,
                SUM(adds) AS total_adds,
                SUM(drops) AS total_drops,
                SUM(total_moves) AS total_moves,
                SUM(total_faab_bid) AS total_faab_bid,
                SUM(net_lamar) AS net_lamar,
                SUM(net_points_ros) AS net_points_ros,
                SUM(net_lamar) / NULLIF(SUM(total_moves), 0) AS efficiency,
                SUM(net_lamar) / NULLIF(COUNT(DISTINCT year), 0) AS lamar_per_season,
                SUM(total_transaction_score) AS total_transaction_score,
                SUM(avg_transaction_score * total_moves) / NULLIF(SUM(total_moves), 0) AS avg_transaction_score,
                SUM(avg_timing_mult * total_moves) / NULLIF(SUM(total_moves), 0) AS avg_timing_mult,
                SUM(trades) AS total_trades,
                SUM(trade_net_lamar) AS trade_net_lamar,
                SUM(trade_net_lamar) / NULLIF(SUM(trades), 0) AS trade_avg_net,
                CAST(SUM(trade_wins) AS DOUBLE) / NULLIF(SUM(trades), 0) AS trade_win_rate,
                SUM(trade_net_points) AS trade_net_points
            FROM {_table_ref("transaction_manager_season")}
            WHERE db_name = {_quote_sql(target_db)}
            GROUP BY COALESCE(franchise_id, manager)
        ),
        ranked AS (
            SELECT
                *,
                (1.0 - PERCENT_RANK() OVER (ORDER BY total_transaction_score DESC)) * 4.0 AS transaction_gpa
            FROM career
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            manager, franchise_id, seasons, total_adds, total_drops, total_moves,
            total_faab_bid, net_lamar, net_points_ros, efficiency, lamar_per_season,
            total_transaction_score, avg_transaction_score, avg_timing_mult,
            {_grade_from_gpa_sql("transaction_gpa")} AS transaction_grade,
            transaction_gpa, total_trades, trade_net_lamar, trade_avg_net,
            trade_win_rate, trade_net_points, CURRENT_TIMESTAMP AS last_updated
        FROM ranked
        """,
    )
    count = _aggregate_count(reader, "transaction_manager_career", target_db)
    log_func(f"[MERGE_SOURCE] refreshed transaction_manager_career: {count:,} rows")
    return {"transaction_manager_career": count}


def _refresh_transaction_player_career(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    cols = _available_columns(reader, "transactions")
    if not {"db_name", "player", "position", "manager", "year", "transaction_type"}.issubset(cols) or not _has_columns(
        reader, "transaction_player_career", {"db_name", "player", "position"}
    ):
        return {}

    faab_sum = (
        "SUM(CASE WHEN t.transaction_type = 'add' THEN COALESCE(t.faab_bid, 0) ELSE 0 END)"
        if "faab_bid" in cols
        else "0"
    )
    faab_avg = (
        "AVG(CASE WHEN t.transaction_type = 'add' AND COALESCE(t.faab_bid, 0) > 0 THEN t.faab_bid END)"
        if "faab_bid" in cols
        else "0"
    )
    add_lamar_col = next(
        (col for col in ("manager_lamar_ros_managed", "fa_lamar_ros", "net_lamar_ros") if col in cols), None
    )
    drop_lamar_col = next((col for col in ("player_lamar_ros_total", "fa_lamar_ros") if col in cols), None)
    add_lamar = (
        f"SUM(CASE WHEN t.transaction_type = 'add' THEN COALESCE(t.{add_lamar_col}, 0) ELSE 0 END)"
        if add_lamar_col
        else "0"
    )
    drop_source = drop_lamar_col or add_lamar_col
    drop_lamar = (
        f"SUM(CASE WHEN t.transaction_type = 'drop' THEN COALESCE(t.{drop_source}, 0) ELSE 0 END)"
        if drop_source
        else "0"
    )
    regret = (
        "AVG(CASE WHEN t.transaction_type = 'drop' THEN t.drop_regret_score END)"
        if "drop_regret_score" in cols
        else "0"
    )
    franchise_ids = (
        "STRING_AGG(DISTINCT t.franchise_id, ', ' ORDER BY t.franchise_id)" if "franchise_id" in cols else "NULL"
    )
    pick_partition_cols = "".join(
        f",\n                    {expr}"
        for expr in [
            "COALESCE(CAST(t.traded_pick_season AS VARCHAR), '')" if "traded_pick_season" in cols else "",
            "COALESCE(CAST(t.traded_pick_round AS VARCHAR), '')" if "traded_pick_round" in cols else "",
            "COALESCE(t.traded_pick_original_owner, '')" if "traded_pick_original_owner" in cols else "",
        ]
        if expr
    )
    trade_order_cols = "t.manager"
    if "source_manager" in cols:
        trade_order_cols += ", t.source_manager"

    _execute_replace(
        writer,
        "transaction_player_career",
        target_db,
        f"""
        INSERT INTO {_table_ref("transaction_player_career")} (
            db_name, player, position, times_added, times_dropped, times_traded,
            total_faab_spent, avg_faab, total_lamar_when_added,
            total_lamar_when_dropped, avg_drop_regret, managers, franchise_ids,
            years_active, last_updated
        )
        WITH deduped_trade_rows AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT
                    t.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            t.transaction_id,
                            t.transaction_type,
                            COALESCE(t.player, ''),
                            COALESCE(t.position, ''){pick_partition_cols}
                        ORDER BY {trade_order_cols}
                    ) AS rn
                FROM {_table_ref("transactions")} t
                WHERE t.db_name = {_quote_sql(target_db)}
                  AND t.transaction_type IN ('trade', 'trade_pick')
            )
            WHERE rn = 1
        ),
        base_transactions AS (
            SELECT *
            FROM {_table_ref("transactions")}
            WHERE db_name = {_quote_sql(target_db)}
              AND transaction_type NOT IN ('trade', 'trade_pick')
            UNION ALL
            SELECT * FROM deduped_trade_rows
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            t.player,
            t.position,
            SUM(CASE WHEN t.transaction_type = 'add' THEN 1 ELSE 0 END) AS times_added,
            SUM(CASE WHEN t.transaction_type = 'drop' THEN 1 ELSE 0 END) AS times_dropped,
            SUM(CASE WHEN t.transaction_type IN ('trade', 'trade_pick') THEN 1 ELSE 0 END) AS times_traded,
            {faab_sum} AS total_faab_spent,
            {faab_avg} AS avg_faab,
            {add_lamar} AS total_lamar_when_added,
            {drop_lamar} AS total_lamar_when_dropped,
            {regret} AS avg_drop_regret,
            STRING_AGG(DISTINCT t.manager, ', ' ORDER BY t.manager) AS managers,
            {franchise_ids} AS franchise_ids,
            COUNT(DISTINCT t.year) AS years_active,
            CURRENT_TIMESTAMP AS last_updated
        FROM base_transactions t
        WHERE t.player IS NOT NULL AND TRIM(t.player) <> ''
          AND t.position IS NOT NULL AND TRIM(t.position) <> ''
        GROUP BY t.player, t.position
        """,
    )
    count = _aggregate_count(reader, "transaction_player_career", target_db)
    log_func(f"[MERGE_SOURCE] refreshed transaction_player_career: {count:,} rows")
    return {"transaction_player_career": count}


def _refresh_matchup_career(*, reader, writer, target_db: str, log_func: Callable[[str], None]) -> dict[str, int]:
    if not _has_columns(
        reader,
        "matchup_season",
        {"db_name", "manager", "year", "franchise_id", "games", "wins", "losses", "ties", "total_team_points"},
    ) or not _has_columns(reader, "matchup_career", {"db_name", "manager", "franchise_id"}):
        return {}

    _execute_replace(
        writer,
        "matchup_career",
        target_db,
        f"""
        INSERT INTO {_table_ref("matchup_career")} (
            db_name, manager, seasons, games, wins, losses, ties,
            total_team_points, total_opponent_points, avg_team_points,
            avg_opponent_points, avg_margin, win_pct, close_win_pct,
            franchise_id, above_league_median, below_league_median, close_games,
            close_wins, close_losses, blowout_wins, blowout_losses,
            optimal_games, optimal_actual_pts, optimal_ceiling_pts,
            optimal_bench_pts, optimal_efficiency, optimal_wins, optimal_losses,
            optimal_missed_wins, optimal_lucky_wins, optimal_wins_actual,
            optimal_losses_actual, optimal_outcome_changes, optimal_margin,
            proj_games, proj_wins, proj_losses, proj_total_team_points,
            proj_total_opponent_points, proj_total_proj, proj_opp_proj,
            proj_above_proj, proj_below_proj, proj_beat_spread,
            proj_margin_total, proj_upset_wins, proj_upset_losses,
            proj_total_upsets, proj_total_error, proj_expected_wins,
            max_win_streak, max_loss_streak, max_team_points, min_team_points,
            avg_power_rating, avg_avg_seed, avg_p_playoffs, avg_p_bye,
            avg_p_semis, avg_p_final, avg_p_champ, avg_exp_wins,
            playoff_seasons, champion_seasons, sacko_seasons, avg_gpa
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            ARG_MAX(manager, year) AS manager,
            COUNT(DISTINCT year) AS seasons,
            SUM(games) AS games,
            SUM(wins) AS wins,
            SUM(losses) AS losses,
            SUM(ties) AS ties,
            SUM(total_team_points) AS total_team_points,
            SUM(total_opponent_points) AS total_opponent_points,
            SUM(total_team_points) / NULLIF(SUM(games), 0) AS avg_team_points,
            SUM(total_opponent_points) / NULLIF(SUM(games), 0) AS avg_opponent_points,
            (SUM(total_team_points) - SUM(total_opponent_points)) / NULLIF(SUM(games), 0) AS avg_margin,
            (SUM(wins) + 0.5 * SUM(ties)) / NULLIF(SUM(games), 0) AS win_pct,
            SUM(close_wins)::DOUBLE / NULLIF(SUM(close_games), 0) AS close_win_pct,
            MAX(franchise_id) AS franchise_id,
            SUM(above_league_median) AS above_league_median,
            SUM(below_league_median) AS below_league_median,
            SUM(close_games) AS close_games,
            SUM(close_wins) AS close_wins,
            SUM(close_losses) AS close_losses,
            SUM(blowout_wins) AS blowout_wins,
            SUM(blowout_losses) AS blowout_losses,
            SUM(optimal_games) AS optimal_games,
            SUM(optimal_actual_pts) AS optimal_actual_pts,
            SUM(optimal_ceiling_pts) AS optimal_ceiling_pts,
            SUM(optimal_ceiling_pts) - SUM(optimal_actual_pts) AS optimal_bench_pts,
            SUM(optimal_actual_pts) / NULLIF(SUM(optimal_ceiling_pts), 0) * 100 AS optimal_efficiency,
            SUM(optimal_wins) AS optimal_wins,
            SUM(optimal_games) - SUM(optimal_wins) AS optimal_losses,
            SUM(optimal_missed_wins) AS optimal_missed_wins,
            SUM(optimal_lucky_wins) AS optimal_lucky_wins,
            SUM(optimal_wins_actual) AS optimal_wins_actual,
            SUM(optimal_losses_actual) AS optimal_losses_actual,
            SUM(optimal_outcome_changes) AS optimal_outcome_changes,
            SUM(optimal_margin) AS optimal_margin,
            SUM(proj_games) AS proj_games,
            SUM(proj_wins) AS proj_wins,
            SUM(proj_losses) AS proj_losses,
            SUM(proj_total_team_points) AS proj_total_team_points,
            SUM(proj_total_opponent_points) AS proj_total_opponent_points,
            SUM(proj_total_proj) AS proj_total_proj,
            SUM(proj_opp_proj) AS proj_opp_proj,
            SUM(proj_above_proj) AS proj_above_proj,
            SUM(proj_below_proj) AS proj_below_proj,
            SUM(proj_beat_spread) AS proj_beat_spread,
            SUM(proj_margin_total) AS proj_margin_total,
            SUM(proj_upset_wins) AS proj_upset_wins,
            SUM(proj_upset_losses) AS proj_upset_losses,
            SUM(proj_upset_wins) + SUM(proj_upset_losses) AS proj_total_upsets,
            SUM(proj_total_error) AS proj_total_error,
            SUM(proj_expected_wins) AS proj_expected_wins,
            MAX(max_win_streak) AS max_win_streak,
            MAX(max_loss_streak) AS max_loss_streak,
            MAX(max_team_points) AS max_team_points,
            MIN(CASE WHEN min_team_points > 0 THEN min_team_points END) AS min_team_points,
            AVG(power_rating) AS avg_power_rating,
            AVG(avg_seed) AS avg_avg_seed,
            AVG(p_playoffs) AS avg_p_playoffs,
            AVG(p_bye) AS avg_p_bye,
            AVG(p_semis) AS avg_p_semis,
            AVG(p_final) AS avg_p_final,
            AVG(p_champ) AS avg_p_champ,
            AVG(exp_final_wins) AS avg_exp_wins,
            SUM(CASE WHEN made_playoffs = 1 THEN 1 ELSE 0 END) AS playoff_seasons,
            SUM(CASE WHEN is_champion = 1 THEN 1 ELSE 0 END) AS champion_seasons,
            SUM(CASE WHEN is_sacko = 1 THEN 1 ELSE 0 END) AS sacko_seasons,
            AVG(CASE WHEN avg_gpa > 0 THEN avg_gpa END) AS avg_gpa
        FROM {_table_ref("matchup_season")}
        WHERE db_name = {_quote_sql(target_db)}
        GROUP BY COALESCE(franchise_id, manager)
        """,
    )
    count = _aggregate_count(reader, "matchup_career", target_db)
    log_func(f"[MERGE_SOURCE] refreshed matchup_career: {count:,} rows")
    return {"matchup_career": count}


def _refresh_matchup_h2h_career(*, reader, writer, target_db: str, log_func: Callable[[str], None]) -> dict[str, int]:
    if not _has_columns(
        reader,
        "matchup_h2h_season",
        {"db_name", "manager", "opponent", "franchise_id", "opponent_franchise_id", "year", "games", "wins"},
    ) or not _has_columns(reader, "matchup_h2h_career", {"db_name", "manager", "opponent"}):
        return {}

    _execute_replace(
        writer,
        "matchup_h2h_career",
        target_db,
        f"""
        INSERT INTO {_table_ref("matchup_h2h_career")} (
            db_name, manager, opponent, franchise_id, opponent_franchise_id,
            games, wins, losses, ties, total_team_points, total_margin,
            max_team_points, min_team_points, recent_win, recent_team_points,
            recent_year, recent_week, results
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            ARG_MAX(manager, year) AS manager,
            ARG_MAX(opponent, year) AS opponent,
            franchise_id,
            opponent_franchise_id,
            SUM(games) AS games,
            SUM(wins) AS wins,
            SUM(losses) AS losses,
            SUM(ties) AS ties,
            SUM(total_team_points) AS total_team_points,
            SUM(total_margin) AS total_margin,
            MAX(max_team_points) AS max_team_points,
            MIN(CASE WHEN min_team_points > 0 THEN min_team_points END) AS min_team_points,
            ARG_MAX(CASE WHEN wins > losses THEN 1 ELSE 0 END, year) AS recent_win,
            ARG_MAX(max_team_points, year) AS recent_team_points,
            MAX(year) AS recent_year,
            NULL AS recent_week,
            ARRAY_AGG(CASE WHEN wins > losses THEN 1 ELSE 0 END ORDER BY year) AS results
        FROM {_table_ref("matchup_h2h_season")}
        WHERE db_name = {_quote_sql(target_db)}
        GROUP BY franchise_id, opponent_franchise_id
        """,
    )
    count = _aggregate_count(reader, "matchup_h2h_career", target_db)
    log_func(f"[MERGE_SOURCE] refreshed matchup_h2h_career: {count:,} rows")
    return {"matchup_h2h_career": count}


def _refresh_homepage_manager_rankings(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    if not _has_columns(reader, "matchup_career", {"db_name", "manager", "franchise_id", "wins"}) or not _has_columns(
        reader, "homepage_manager_rankings", {"db_name", "manager", "franchise_id"}
    ):
        return {}
    _execute_replace(
        writer,
        "homepage_manager_rankings",
        target_db,
        f"""
        INSERT INTO {_table_ref("homepage_manager_rankings")} (
            db_name, manager, franchise_id, wins, losses, ties, win_pct,
            championships, playoff_appearances, seasons, power_rating,
            first_year, last_year, career_rank
        )
        WITH years AS (
            SELECT COALESCE(franchise_id, manager) AS grp_key, MIN(year) AS first_year, MAX(year) AS last_year
            FROM {_table_ref("matchup_season")}
            WHERE db_name = {_quote_sql(target_db)}
            GROUP BY COALESCE(franchise_id, manager)
        ),
        playoff_results AS (
            SELECT
                COALESCE(franchise_id, manager) AS grp_key,
                SUM(COALESCE(CAST(win AS INT), 0)) AS playoff_wins,
                SUM(COALESCE(CAST(loss AS INT), 0)) AS playoff_losses,
                SUM(COALESCE(CAST(tie AS INT), 0)) AS playoff_ties
            FROM {_table_ref("matchup")}
            WHERE db_name = {_quote_sql(target_db)}
              AND COALESCE(CAST(is_bye_week AS INT), 0) = 0
              AND COALESCE(CAST(is_playoffs AS INT), 0) = 1
              AND COALESCE(CAST(is_consolation AS INT), 0) = 0
            GROUP BY COALESCE(franchise_id, manager)
        ),
        ranked AS (
            SELECT
                c.manager,
                c.franchise_id,
                c.wins + COALESCE(p.playoff_wins, 0) AS wins,
                c.losses + COALESCE(p.playoff_losses, 0) AS losses,
                c.ties + COALESCE(p.playoff_ties, 0) AS ties,
                ROUND(
                    CAST(c.wins + COALESCE(p.playoff_wins, 0) AS DOUBLE)
                    / NULLIF(
                        c.wins + c.losses + c.ties
                        + COALESCE(p.playoff_wins, 0)
                        + COALESCE(p.playoff_losses, 0)
                        + COALESCE(p.playoff_ties, 0),
                        0
                    ),
                    3
                ) AS win_pct,
                c.champion_seasons AS championships,
                c.playoff_seasons AS playoff_appearances,
                c.seasons, c.avg_power_rating AS power_rating,
                y.first_year, y.last_year,
                ROW_NUMBER() OVER (
                    ORDER BY
                        c.wins + COALESCE(p.playoff_wins, 0) DESC,
                        c.win_pct DESC,
                        c.total_team_points DESC
                ) AS career_rank
            FROM {_table_ref("matchup_career")} c
            LEFT JOIN years y ON COALESCE(c.franchise_id, c.manager) = y.grp_key
            LEFT JOIN playoff_results p ON COALESCE(c.franchise_id, c.manager) = p.grp_key
            WHERE c.db_name = {_quote_sql(target_db)}
        )
        SELECT {_quote_sql(target_db)} AS db_name, * FROM ranked
        """,
    )
    count = _aggregate_count(reader, "homepage_manager_rankings", target_db)
    log_func(f"[MERGE_SOURCE] refreshed homepage_manager_rankings: {count:,} rows")
    return {"homepage_manager_rankings": count}


def _refresh_homepage_current_standings(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    if not _has_columns(reader, "standings_by_year", {"db_name", "manager", "year", "wins"}) or not _has_columns(
        reader, "homepage_current_standings", {"db_name", "manager", "franchise_id"}
    ):
        return {}
    _execute_replace(
        writer,
        "homepage_current_standings",
        target_db,
        f"""
        INSERT INTO {_table_ref("homepage_current_standings")} (
            db_name, manager, franchise_id, team_name, wins, losses, ties,
            points_for, win_pct, power_rating, p_playoffs, p_champ, standings_rank
        )
        WITH current_year AS (
            SELECT MAX(year) AS year
            FROM {_table_ref("standings_by_year")}
            WHERE db_name = {_quote_sql(target_db)}
        ),
        base AS (
            SELECT
                s.manager, s.franchise_id, s.team_name, s.wins, s.losses, s.ties,
                s.points_for, s.win_pct,
                ms.power_rating, ms.p_playoffs, ms.p_champ,
                ROW_NUMBER() OVER (ORDER BY s.total_wins DESC, s.points_for DESC, s.wins DESC) AS standings_rank
            FROM {_table_ref("standings_by_year")} s
            LEFT JOIN {_table_ref("matchup_season")} ms
              ON ms.db_name = s.db_name
             AND ms.year = s.year
             AND COALESCE(ms.franchise_id, ms.manager) = COALESCE(s.franchise_id, s.manager)
            WHERE s.db_name = {_quote_sql(target_db)}
              AND s.year = (SELECT year FROM current_year)
        )
        SELECT {_quote_sql(target_db)} AS db_name, * FROM base
        """,
    )
    count = _aggregate_count(reader, "homepage_current_standings", target_db)
    log_func(f"[MERGE_SOURCE] refreshed homepage_current_standings: {count:,} rows")
    return {"homepage_current_standings": count}


def _refresh_homepage_top_rivalries(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    if not _has_columns(reader, "matchup_h2h_career", {"db_name", "manager", "opponent", "games"}) or not _has_columns(
        reader, "homepage_top_rivalries", {"db_name", "manager1", "manager2"}
    ):
        return {}
    _execute_replace(
        writer,
        "homepage_top_rivalries",
        target_db,
        f"""
        INSERT INTO {_table_ref("homepage_top_rivalries")} (
            db_name, manager1, manager2, franchise_id_1, franchise_id_2,
            total_games, manager1_wins, manager2_wins, ties,
            competitiveness_score, avg_margin, rivalry_rank
        )
        WITH pairs AS (
            SELECT
                manager AS manager1,
                opponent AS manager2,
                franchise_id AS franchise_id_1,
                opponent_franchise_id AS franchise_id_2,
                games AS total_games,
                wins AS manager1_wins,
                losses AS manager2_wins,
                ties,
                100.0 - ABS(COALESCE(total_margin, 0)) / NULLIF(games, 0) AS competitiveness_score,
                ABS(COALESCE(total_margin, 0)) / NULLIF(games, 0) AS avg_margin
            FROM {_table_ref("matchup_h2h_career")}
            WHERE db_name = {_quote_sql(target_db)}
              AND games > 0
              AND COALESCE(franchise_id, manager) < COALESCE(opponent_franchise_id, opponent)
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (ORDER BY competitiveness_score DESC, total_games DESC) AS rivalry_rank
            FROM pairs
        )
        SELECT {_quote_sql(target_db)} AS db_name, * FROM ranked
        """,
    )
    count = _aggregate_count(reader, "homepage_top_rivalries", target_db)
    log_func(f"[MERGE_SOURCE] refreshed homepage_top_rivalries: {count:,} rows")
    return {"homepage_top_rivalries": count}


def _txn_value_expr(cols: set[str], alias: str, *, drop: bool = False) -> str:
    parts: list[str] = []
    if drop:
        if "drop_regret_score" in cols:
            parts.append(f"NULLIF({alias}.drop_regret_score, 0)")
        if "player_lamar_ros_total" in cols:
            parts.append(f"NULLIF({alias}.player_lamar_ros_total, 0)")
        if "player_lamar_ros" in cols:
            parts.append(f"NULLIF({alias}.player_lamar_ros, 0)")
        if "total_points_ros_total" in cols:
            parts.append(f"NULLIF({alias}.total_points_ros_total, 0)")
    else:
        if "manager_lamar_ros_managed" in cols:
            parts.append(f"{alias}.manager_lamar_ros_managed")
        if "fa_lamar_ros" in cols:
            parts.append(f"{alias}.fa_lamar_ros")
        if "net_lamar_ros" in cols:
            parts.append(f"{alias}.net_lamar_ros")
        if "total_points_ros_total" in cols:
            parts.append(f"{alias}.total_points_ros_total")
    parts.append("0")
    return f"COALESCE({', '.join(parts)})"


def _txn_quality_expr(cols: set[str], alias: str) -> str:
    if "transaction_quality_score" in cols:
        return f"{alias}.transaction_quality_score"
    if "transaction_score" in cols:
        return f"{alias}.transaction_score"
    return "NULL"


def _refresh_summary_transaction_highlights(
    *,
    reader,
    writer,
    target_db: str,
    prefix: str,
    year_filter: str,
) -> None:
    summary_cols = _available_columns(reader, "homepage_league_summary")
    txn_cols = _available_columns(reader, "transactions")
    required = {"db_name", "transaction_type", "player", "manager", "year"}
    if not required.issubset(txn_cols):
        return

    add_expr = _txn_value_expr(txn_cols, "t")
    drop_expr = _txn_value_expr(txn_cols, "t", drop=True)
    candidate_sets = [
        (
            "best_add",
            "best_pickup",
            "transaction_type = 'add'",
            add_expr,
        ),
        (
            "worst_drop",
            "worst_drop",
            "transaction_type = 'drop'",
            drop_expr,
        ),
    ]

    ctes: list[str] = []
    scalar_selects: list[str] = []
    set_clauses: list[str] = []
    for cte_name, column_base, txn_filter, metric_expr in candidate_sets:
        available_fields = {
            field: f"{prefix}{column_base}_{field}"
            for field in ("player", "manager", "year", "week", "lamar", "headshot")
            if f"{prefix}{column_base}_{field}" in summary_cols
        }
        if not available_fields:
            continue
        week_expr = "t.week" if "week" in txn_cols else "NULL"
        ctes.append(
            f"""
            {cte_name} AS (
                SELECT
                    t.player,
                    t.manager,
                    t.year,
                    {week_expr} AS week,
                    {metric_expr} AS lamar,
                    NULL AS headshot
                FROM {_table_ref("transactions")} t
                WHERE t.db_name = {_quote_sql(target_db)}
                  AND t.{txn_filter}
                  AND t.player IS NOT NULL
                  AND {metric_expr} > 0
                  {year_filter}
                ORDER BY lamar DESC
                LIMIT 1
            )
            """
        )
        source_columns = {
            "player": "player",
            "manager": "manager",
            "year": "year",
            "week": "week",
            "lamar": "lamar",
            "headshot": "headshot",
        }
        for field, target_column in available_fields.items():
            alias = f"{cte_name}_{field}"
            scalar_selects.append(f"(SELECT {source_columns[field]} FROM {cte_name}) AS {alias}")
            set_clauses.append(f"{target_column} = sub.{alias}")

    if not ctes or not set_clauses:
        return

    _execute_with_busy_retry(
        writer,
        f"""
        WITH {", ".join(ctes)},
        sub AS (
            SELECT {", ".join(scalar_selects)}
        )
        UPDATE {_table_ref("homepage_league_summary")} AS s
        SET {", ".join(set_clauses)}
        FROM sub
        WHERE s.db_name = {_quote_sql(target_db)}
        """,
    )


def _refresh_profile_transaction_highlights(
    *,
    reader,
    writer,
    target_db: str,
    column_prefixes: tuple[str, ...],
    year_filter: str,
) -> None:
    profile_cols = _available_columns(reader, "homepage_manager_profiles")
    txn_cols = _available_columns(reader, "transactions")
    required = {"db_name", "transaction_type", "player", "manager", "year"}
    if not required.issubset(txn_cols):
        return

    set_clauses: list[str] = []
    for column_prefix in column_prefixes:
        mapping = {
            f"{column_prefix}_best_add_player": "add_player",
            f"{column_prefix}_best_add_lamar": "add_lamar",
            f"{column_prefix}_best_add_year": "add_year",
            f"{column_prefix}_best_add_week": "add_week",
            f"{column_prefix}_best_add_headshot": "add_headshot",
            f"{column_prefix}_worst_drop_player": "drop_player",
            f"{column_prefix}_worst_drop_lamar": "drop_lamar",
            f"{column_prefix}_worst_drop_year": "drop_year",
            f"{column_prefix}_worst_drop_week": "drop_week",
            f"{column_prefix}_worst_drop_headshot": "drop_headshot",
            f"{column_prefix}_quality_metric": "avg_quality",
        }
        for target_column, source_column in mapping.items():
            if target_column in profile_cols:
                set_clauses.append(f"{target_column} = sub.{source_column}")

    if not set_clauses:
        return

    add_expr = _txn_value_expr(txn_cols, "t")
    drop_expr = _txn_value_expr(txn_cols, "t", drop=True)
    quality_expr = _txn_quality_expr(txn_cols, "t")
    week_expr = "t.week" if "week" in txn_cols else "NULL"
    franchise_expr = "COALESCE(t.franchise_id, t.manager)" if "franchise_id" in txn_cols else "t.manager"

    _execute_with_busy_retry(
        writer,
        f"""
        WITH scoped_txn AS (
            SELECT
                {franchise_expr} AS grp_key,
                t.player,
                t.manager,
                t.year,
                {week_expr} AS week,
                t.transaction_type,
                {add_expr} AS add_lamar,
                {drop_expr} AS drop_lamar,
                {quality_expr} AS quality_score
            FROM {_table_ref("transactions")} t
            WHERE t.db_name = {_quote_sql(target_db)}
              AND t.player IS NOT NULL
              AND t.manager IS NOT NULL
              {year_filter}
        ),
        best_add AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY grp_key ORDER BY add_lamar DESC) AS rn
                FROM scoped_txn
                WHERE transaction_type = 'add' AND add_lamar > 0
            )
            WHERE rn = 1
        ),
        worst_drop AS (
            SELECT * EXCLUDE (rn)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (PARTITION BY grp_key ORDER BY drop_lamar DESC) AS rn
                FROM scoped_txn
                WHERE transaction_type = 'drop' AND drop_lamar > 0
            )
            WHERE rn = 1
        ),
        quality AS (
            SELECT grp_key, AVG(quality_score) AS avg_quality
            FROM scoped_txn
            WHERE quality_score IS NOT NULL
            GROUP BY grp_key
        ),
        sub AS (
            SELECT
                COALESCE(p.franchise_id, p.manager) AS grp_key,
                ba.player AS add_player,
                ba.add_lamar,
                ba.year AS add_year,
                ba.week AS add_week,
                NULL AS add_headshot,
                wd.player AS drop_player,
                wd.drop_lamar,
                wd.year AS drop_year,
                wd.week AS drop_week,
                NULL AS drop_headshot,
                q.avg_quality
            FROM {_table_ref("homepage_manager_profiles")} p
            LEFT JOIN best_add ba ON ba.grp_key = COALESCE(p.franchise_id, p.manager)
            LEFT JOIN worst_drop wd ON wd.grp_key = COALESCE(p.franchise_id, p.manager)
            LEFT JOIN quality q ON q.grp_key = COALESCE(p.franchise_id, p.manager)
            WHERE p.db_name = {_quote_sql(target_db)}
        )
        UPDATE {_table_ref("homepage_manager_profiles")} AS p
        SET {", ".join(set_clauses)}
        FROM sub
        WHERE p.db_name = {_quote_sql(target_db)}
          AND COALESCE(p.franchise_id, p.manager) = sub.grp_key
        """,
    )


def _refresh_homepage_manager_profiles(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    if not _has_columns(reader, "matchup_career", {"db_name", "manager", "franchise_id", "wins"}) or not _has_columns(
        reader, "homepage_manager_profiles", {"db_name", "manager", "franchise_id"}
    ):
        return {}
    # A manual manager merge can leave one already-collapsed profile row keyed
    # by the retired franchise_id. Reconcile by display name only when that
    # name identifies exactly one career row and one profile row, preserving
    # the distinct-franchise behavior for duplicate manager names.
    _execute_with_busy_retry(
        writer,
        f"""
        WITH unique_career_managers AS (
            SELECT manager, MAX(franchise_id) AS franchise_id
            FROM {_table_ref("matchup_career")}
            WHERE db_name = {_quote_sql(target_db)}
              AND manager IS NOT NULL AND TRIM(manager) <> ''
              AND franchise_id IS NOT NULL AND TRIM(franchise_id) <> ''
            GROUP BY manager
            HAVING COUNT(*) = 1
        ),
        unique_profile_managers AS (
            SELECT manager
            FROM {_table_ref("homepage_manager_profiles")}
            WHERE db_name = {_quote_sql(target_db)}
              AND manager IS NOT NULL AND TRIM(manager) <> ''
            GROUP BY manager
            HAVING COUNT(*) = 1
        )
        UPDATE {_table_ref("homepage_manager_profiles")} AS p
        SET franchise_id = c.franchise_id
        FROM unique_career_managers c
        JOIN unique_profile_managers u ON u.manager = c.manager
        WHERE p.db_name = {_quote_sql(target_db)}
          AND p.manager = c.manager
          AND p.franchise_id IS DISTINCT FROM c.franchise_id
        """,
    )
    _execute_with_busy_retry(
        writer,
        f"""
        INSERT INTO {_table_ref("homepage_manager_profiles")} (
            db_name, manager, franchise_id, current_year, total_wins, total_losses,
            total_ties, total_win_pct, reg_wins, reg_losses, reg_ties, reg_win_pct,
            championships, sacko_bowls, playoff_appearances, playoff_rate,
            first_year, last_year, seasons_played, current_team_name,
            career_points, timeline_data
        )
        WITH years AS (
            SELECT
                COALESCE(franchise_id, manager) AS grp_key,
                MIN(year) AS first_year,
                MAX(year) AS last_year,
                ARG_MAX(team_name, year) AS current_team_name
            FROM {_table_ref("standings_by_year")}
            WHERE db_name = {_quote_sql(target_db)}
            GROUP BY COALESCE(franchise_id, manager)
        ),
        max_year AS (
            SELECT MAX(year) AS current_year
            FROM {_table_ref("standings_by_year")}
            WHERE db_name = {_quote_sql(target_db)}
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            c.manager,
            c.franchise_id,
            (SELECT current_year FROM max_year) AS current_year,
            c.wins AS total_wins,
            c.losses AS total_losses,
            c.ties AS total_ties,
            c.win_pct AS total_win_pct,
            c.wins AS reg_wins,
            c.losses AS reg_losses,
            c.ties AS reg_ties,
            c.win_pct AS reg_win_pct,
            c.champion_seasons AS championships,
            c.sacko_seasons AS sacko_bowls,
            c.playoff_seasons AS playoff_appearances,
            c.playoff_seasons::DOUBLE / NULLIF(c.seasons, 0) AS playoff_rate,
            y.first_year,
            y.last_year,
            c.seasons AS seasons_played,
            y.current_team_name,
            c.total_team_points AS career_points,
            '[]' AS timeline_data
        FROM {_table_ref("matchup_career")} c
        LEFT JOIN years y ON COALESCE(c.franchise_id, c.manager) = y.grp_key
        WHERE c.db_name = {_quote_sql(target_db)}
          AND NOT EXISTS (
              SELECT 1
              FROM {_table_ref("homepage_manager_profiles")} p
              WHERE p.db_name = {_quote_sql(target_db)}
                AND COALESCE(p.franchise_id, p.manager) = COALESCE(c.franchise_id, c.manager)
          )
        """,
    )
    _execute_with_busy_retry(
        writer,
        f"""
        WITH years AS (
            SELECT
                COALESCE(franchise_id, manager) AS grp_key,
                MIN(year) AS first_year,
                MAX(year) AS last_year,
                ARG_MAX(team_name, year) AS current_team_name
            FROM {_table_ref("standings_by_year")}
            WHERE db_name = {_quote_sql(target_db)}
            GROUP BY COALESCE(franchise_id, manager)
        ),
        max_year AS (
            SELECT MAX(year) AS current_year
            FROM {_table_ref("standings_by_year")}
            WHERE db_name = {_quote_sql(target_db)}
        ),
        sub AS (
            SELECT
                COALESCE(c.franchise_id, c.manager) AS grp_key,
                c.manager,
                c.franchise_id,
                (SELECT current_year FROM max_year) AS current_year,
                c.wins AS total_wins,
                c.losses AS total_losses,
                c.ties AS total_ties,
                c.win_pct AS total_win_pct,
                c.wins AS reg_wins,
                c.losses AS reg_losses,
                c.ties AS reg_ties,
                c.win_pct AS reg_win_pct,
                c.champion_seasons AS championships,
                c.sacko_seasons AS sacko_bowls,
                c.playoff_seasons AS playoff_appearances,
                c.playoff_seasons::DOUBLE / NULLIF(c.seasons, 0) AS playoff_rate,
                y.first_year,
                y.last_year,
                c.seasons AS seasons_played,
                y.current_team_name,
                c.total_team_points AS career_points
            FROM {_table_ref("matchup_career")} c
            LEFT JOIN years y ON COALESCE(c.franchise_id, c.manager) = y.grp_key
            WHERE c.db_name = {_quote_sql(target_db)}
        )
        UPDATE {_table_ref("homepage_manager_profiles")} AS p
        SET manager = sub.manager,
            franchise_id = sub.franchise_id,
            current_year = sub.current_year,
            total_wins = sub.total_wins,
            total_losses = sub.total_losses,
            total_ties = sub.total_ties,
            total_win_pct = sub.total_win_pct,
            reg_wins = sub.reg_wins,
            reg_losses = sub.reg_losses,
            reg_ties = sub.reg_ties,
            reg_win_pct = sub.reg_win_pct,
            championships = sub.championships,
            sacko_bowls = sub.sacko_bowls,
            playoff_appearances = sub.playoff_appearances,
            playoff_rate = sub.playoff_rate,
            first_year = sub.first_year,
            last_year = sub.last_year,
            seasons_played = sub.seasons_played,
            current_team_name = sub.current_team_name,
            career_points = sub.career_points
        FROM sub
        WHERE p.db_name = {_quote_sql(target_db)}
          AND COALESCE(p.franchise_id, p.manager) = sub.grp_key
        """,
    )

    _refresh_profile_transaction_highlights(
        reader=reader,
        writer=writer,
        target_db=target_db,
        column_prefixes=("transaction", "txn"),
        year_filter="",
    )
    _refresh_profile_transaction_highlights(
        reader=reader,
        writer=writer,
        target_db=target_db,
        column_prefixes=("season_transaction", "season_txn"),
        year_filter=f"AND t.year = (SELECT MAX(year) FROM {_table_ref('transactions')} WHERE db_name = {_quote_sql(target_db)})",
    )
    count = _aggregate_count(reader, "homepage_manager_profiles", target_db)
    log_func(f"[MERGE_SOURCE] refreshed homepage_manager_profiles: {count:,} rows")
    return {"homepage_manager_profiles": count}


def _refresh_homepage_league_summary(
    *, reader, writer, target_db: str, log_func: Callable[[str], None]
) -> dict[str, int]:
    if not _has_columns(reader, "matchup", {"db_name", "manager", "year", "week", "team_points"}) or not _has_columns(
        reader, "homepage_league_summary", {"db_name", "last_updated", "data_year"}
    ):
        return {}
    _execute_with_busy_retry(
        writer,
        f"""
        INSERT INTO {_table_ref("homepage_league_summary")} (
            db_name, last_updated, data_year, data_week,
            highest_score_manager, highest_score_points, highest_score_year,
            highest_score_week, closest_game_manager, closest_game_opponent,
            closest_game_margin, closest_game_year, closest_game_week,
            biggest_blowout_manager, biggest_blowout_opponent,
            biggest_blowout_margin, biggest_blowout_year, biggest_blowout_week,
            most_championships_manager, most_championships_count
        )
        WITH base AS (
            SELECT *
            FROM {_table_ref("matchup")}
            WHERE db_name = {_quote_sql(target_db)}
              AND manager IS NOT NULL
              AND TRIM(manager) <> ''
        ),
        champs AS (
            SELECT manager, COUNT(*) AS championships
            FROM base
            WHERE COALESCE(champion, 0) = 1
            GROUP BY manager
        )
        SELECT
            {_quote_sql(target_db)} AS db_name,
            CURRENT_TIMESTAMP AS last_updated,
            MAX(year) AS data_year,
            MAX(week) FILTER (WHERE year = (SELECT MAX(year) FROM base)) AS data_week,
            ARG_MAX(manager, team_points) AS highest_score_manager,
            MAX(team_points) AS highest_score_points,
            ARG_MAX(year, team_points) AS highest_score_year,
            ARG_MAX(week, team_points) AS highest_score_week,
            ARG_MIN(manager, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_manager,
            ARG_MIN(opponent, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_opponent,
            MIN(ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_margin,
            ARG_MIN(year, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_year,
            ARG_MIN(week, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_week,
            ARG_MAX(manager, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_manager,
            ARG_MAX(opponent, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_opponent,
            MAX(ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_margin,
            ARG_MAX(year, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_year,
            ARG_MAX(week, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_week,
            (SELECT ARG_MAX(manager, championships) FROM champs) AS most_championships_manager,
            (SELECT MAX(championships) FROM champs) AS most_championships_count
        FROM base
        WHERE NOT EXISTS (
            SELECT 1
            FROM {_table_ref("homepage_league_summary")}
            WHERE db_name = {_quote_sql(target_db)}
        )
        """,
    )
    _execute_with_busy_retry(
        writer,
        f"""
        WITH base AS (
            SELECT *
            FROM {_table_ref("matchup")}
            WHERE db_name = {_quote_sql(target_db)}
              AND manager IS NOT NULL
              AND TRIM(manager) <> ''
        ),
        champs AS (
            SELECT manager, COUNT(*) AS championships
            FROM base
            WHERE COALESCE(champion, 0) = 1
            GROUP BY manager
        ),
        sub AS (
            SELECT
                CURRENT_TIMESTAMP AS last_updated,
                MAX(year) AS data_year,
                MAX(week) FILTER (WHERE year = (SELECT MAX(year) FROM base)) AS data_week,
                ARG_MAX(manager, team_points) AS highest_score_manager,
                MAX(team_points) AS highest_score_points,
                ARG_MAX(year, team_points) AS highest_score_year,
                ARG_MAX(week, team_points) AS highest_score_week,
                ARG_MIN(manager, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_manager,
                ARG_MIN(opponent, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_opponent,
                MIN(ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_margin,
                ARG_MIN(year, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_year,
                ARG_MIN(week, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS closest_game_week,
                ARG_MAX(manager, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_manager,
                ARG_MAX(opponent, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_opponent,
                MAX(ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_margin,
                ARG_MAX(year, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_year,
                ARG_MAX(week, ABS(COALESCE(team_points, 0) - COALESCE(opponent_points, 0))) AS biggest_blowout_week,
                (SELECT ARG_MAX(manager, championships) FROM champs) AS most_championships_manager,
                (SELECT MAX(championships) FROM champs) AS most_championships_count
            FROM base
        )
        UPDATE {_table_ref("homepage_league_summary")} AS s
        SET last_updated = sub.last_updated,
            data_year = sub.data_year,
            data_week = sub.data_week,
            highest_score_manager = sub.highest_score_manager,
            highest_score_points = sub.highest_score_points,
            highest_score_year = sub.highest_score_year,
            highest_score_week = sub.highest_score_week,
            closest_game_manager = sub.closest_game_manager,
            closest_game_opponent = sub.closest_game_opponent,
            closest_game_margin = sub.closest_game_margin,
            closest_game_year = sub.closest_game_year,
            closest_game_week = sub.closest_game_week,
            biggest_blowout_manager = sub.biggest_blowout_manager,
            biggest_blowout_opponent = sub.biggest_blowout_opponent,
            biggest_blowout_margin = sub.biggest_blowout_margin,
            biggest_blowout_year = sub.biggest_blowout_year,
            biggest_blowout_week = sub.biggest_blowout_week,
            most_championships_manager = sub.most_championships_manager,
            most_championships_count = sub.most_championships_count
        FROM sub
        WHERE s.db_name = {_quote_sql(target_db)}
        """,
    )
    _refresh_summary_transaction_highlights(
        reader=reader,
        writer=writer,
        target_db=target_db,
        prefix="alltime_",
        year_filter="",
    )
    _refresh_summary_transaction_highlights(
        reader=reader,
        writer=writer,
        target_db=target_db,
        prefix="season_",
        year_filter=f"AND t.year = (SELECT MAX(year) FROM {_table_ref('transactions')} WHERE db_name = {_quote_sql(target_db)})",
    )
    count = _aggregate_count(reader, "homepage_league_summary", target_db)
    log_func(f"[MERGE_SOURCE] refreshed homepage_league_summary: {count:,} rows")
    return {"homepage_league_summary": count}


def _merge_sources_from_context(ctx: Any) -> list[Mapping[str, Any]]:
    raw_sources = _ctx_get(ctx, "merge_sources")
    if isinstance(raw_sources, list):
        sources = [source for source in raw_sources if isinstance(source, Mapping) and source.get("source_db")]
        if sources:
            return sources

    single_source = _ctx_get(ctx, "merge_source")
    if isinstance(single_source, Mapping) and single_source.get("source_db"):
        return [single_source]

    return []


def _copy_one_merge_source_to_public(
    ctx: Any,
    target_db_name: str,
    merge_source: Mapping[str, Any],
    *,
    reader,
    writer,
    log_func: Callable[[str], None] = print,
) -> dict[str, int | str]:
    source_db = _assert_db_name(str(merge_source["source_db"]))
    target_db = _assert_db_name(target_db_name)
    if source_db == target_db:
        raise ValueError("merge_source source_db and target db_name must differ")

    merge_years = _merge_years_from_config(merge_source)
    if not merge_years:
        source_years = _query_years(reader, source_db)
        target_years = _query_years(reader, target_db)
        earliest_target_year = min(target_years) if target_years else None
        merge_years = [year for year in source_years if earliest_target_year is None or year < earliest_target_year]

    if not merge_years:
        return {"status": "no_merge_years"}

    mappings = _sanitize_manager_mappings(merge_source.get("manager_mapping"))
    year_sql = _year_list_sql(merge_years)
    stats: dict[str, int | str] = {"status": "copied", "merge_years": year_sql}
    stats["single_year_import"] = "true" if _ctx_is_single_year_import(ctx) else "false"

    log_func(f"[MERGE_SOURCE] Copying {source_db} -> {target_db} in Fly for year(s): {year_sql}")

    for table_name in _query_year_scoped_tables(reader):
        columns = _query_columns(reader, table_name)
        if "db_name" not in columns or "year" not in columns:
            continue

        column_sql = ", ".join(_quote_ident(column) for column in columns)
        select_sql = ",\n          ".join(_select_expr(column, target_db, mappings) for column in columns)
        table_ref = _table_ref(table_name)
        year_filter = f"TRY_CAST(year AS INTEGER) IN ({year_sql})"
        source_filter = f"db_name = {_quote_sql(source_db)} AND {year_filter}"
        if table_name == "player_fantasy" and "manager" in columns:
            source_filter += f" AND {PLAYER_UNROSTERED_FILTER}"

        _execute_with_busy_retry(
            writer,
            f"""
            BEGIN TRANSACTION;
            DELETE FROM {table_ref}
            WHERE db_name = {_quote_sql(target_db)}
              AND {year_filter};

            INSERT INTO {table_ref} ({column_sql})
            SELECT
              {select_sql}
            FROM {table_ref}
            WHERE {source_filter};
            COMMIT;
            """,
        )
        copied = _query_scalar_with_busy_retry(
            reader,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {table_ref}
            WHERE db_name = {_quote_sql(target_db)}
              AND {year_filter}
            """,
        )
        row_count = int(copied or 0)
        if row_count:
            stats[table_name] = row_count
            log_func(f"[MERGE_SOURCE] {table_name}: {row_count:,} rows")

    _remap_copied_franchise_ids(
        reader=reader,
        writer=writer,
        target_db=target_db,
        merge_years=merge_years,
        log_func=log_func,
    )
    aggregate_stats = _refresh_merge_source_aggregates(
        reader=reader,
        writer=writer,
        target_db=target_db,
        log_func=log_func,
    )
    stats.update(aggregate_stats)

    copied_tables = [key for key in stats if key not in {"status", "merge_years", "single_year_import"}]
    if not copied_tables:
        raise RuntimeError(f"No rows copied from merge_source {source_db} for years {year_sql}")

    return stats


def copy_merge_source_to_public(
    ctx: Any,
    target_db_name: str,
    *,
    reader=None,
    writer=None,
    log_func: Callable[[str], None] = print,
) -> dict[str, int | str]:
    """Copy historical source rows to the target league inside Fly.

    This is for league-to-league ``merge_source`` / ``merge_sources`` imports.
    The rows are already canonical in ``___leagues.public``; the merge only
    needs to duplicate the source years under the target ``db_name``. No table
    data is downloaded to the worker.
    """
    merge_sources = _merge_sources_from_context(ctx)
    if not merge_sources:
        return {"status": "no_merge_source"}

    if reader is None:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
    if writer is None:
        from multi_league.core.fly_writer import FlyWriter

        writer = FlyWriter()

    if len(merge_sources) == 1:
        return _copy_one_merge_source_to_public(
            ctx,
            target_db_name,
            merge_sources[0],
            reader=reader,
            writer=writer,
            log_func=log_func,
        )

    combined: dict[str, int | str] = {
        "status": "copied_multi",
        "source_count": len(merge_sources),
        "single_year_import": "true" if _ctx_is_single_year_import(ctx) else "false",
    }
    copied_any = False
    for index, merge_source in enumerate(merge_sources, start=1):
        source_db = _assert_db_name(str(merge_source["source_db"]))
        source_stats = _copy_one_merge_source_to_public(
            ctx,
            target_db_name,
            merge_source,
            reader=reader,
            writer=writer,
            log_func=log_func,
        )
        combined[f"source_{index}_db"] = source_db
        combined[f"source_{index}_status"] = str(source_stats.get("status", "unknown"))
        if source_stats.get("merge_years") is not None:
            combined[f"source_{index}_merge_years"] = str(source_stats["merge_years"])
        if source_stats.get("status") == "copied":
            copied_any = True

        for key, value in source_stats.items():
            if key in {"status", "merge_years", "single_year_import"}:
                continue
            if isinstance(value, int):
                combined[key] = int(combined.get(key, 0) or 0) + value

    if not copied_any:
        combined["status"] = "no_merge_years"

    return combined
