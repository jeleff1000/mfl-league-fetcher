"""
Import Utility Functions

Shared helper functions used by all three import orchestrators
(Yahoo, Sleeper, ESPN). Previously duplicated across each file.

Functions:
    drop_league_tables()                   - Drop all league tables for clean-slate quick import
    upload_league_tables()                 - Upload local DuckDB -> remote database (register + upload)
    upload_league_settings_to_database()   - Validate per-year settings for upload
    run_track_1_verify()                   - Verify NFL super table has required data
    run_transformations()                  - Run a set of transformation scripts

Usage:
    from multi_league.core.import_utils import (
        upload_league_settings_to_database,
        run_track_1_verify,
        run_transformations,
    )
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from multi_league.core.canonical_settings import (
    flatten_settings,
    _col_type as _settings_col_type,
    get_schema_keys as _settings_schema_keys,
)
from multi_league.core.fetch_runtime import resolve_context_path, runtime_from_source
from multi_league.core.db_utils import get_db_name
from multi_league.core.script_runner import log, run_script
from multi_league.core.sql_utils import execute_scoped


# ---------------------------------------------------------------------------
# upload_league_settings_to_database
# ---------------------------------------------------------------------------


def upload_league_settings_to_database(
    ctx: Any,
    dry_run: bool = False,
    verbose: bool = False,
) -> bool:
    """Upload league settings to the database using the flat canonical schema.

    Works with any context type (LeagueContext, SleeperContext, ESPNContext).
    Supports both ``league_settings_{year}_*.json`` (Yahoo/Sleeper) and
    ``settings_{year}.json`` (ESPN) naming conventions.

    Returns:
        True if upload succeeded, False otherwise
    """
    settings_dir = Path(ctx.data_directory) / "league_settings"
    if not settings_dir.exists():
        log("[SETTINGS UPLOAD] No league_settings directory found")
        return False

    # Settings upload is handled by FlyTarget during table merge
    log("[SETTINGS UPLOAD] Settings uploaded via FlyTarget merge")
    return True


# ---------------------------------------------------------------------------
# run_track_1_verify
# ---------------------------------------------------------------------------


def _ensure_import_ops_weeks(path: Path, year: int) -> bool:
    """Hydrate missing weeks and refresh the latest played week in the import cache.

    Only a skinny admission query and those weeks' cache-column projection are
    read from Fly. Always refresh the latest week: identical keys (and even
    scoring atoms) do not imply unchanged derived ranks after an OPS correction.
    Rank values are copied, never recalculated here. Return False only for a
    verified unplayed season, where an empty import shell is valid.
    """
    import duckdb

    from multi_league.core.db_reader import get_reader
    from multi_league.core.ops_cache import _patch_research_ops_cache_from_fly

    keys = ["week", "NFL_player_id", "nfl_team", "opponent_nfl_team"]
    projection = ", ".join(keys)
    predicate = (
        f"year = {int(year)} AND COALESCE(season_type, 'REG') = 'REG' "
        "AND NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL "
        "AND opponent_nfl_team IS NOT NULL"
    )
    query = f"SELECT {projection} FROM nfl_historical.nfl_player_stats_all WHERE {predicate}"
    reader = get_reader()
    expected = reader.query_df(query, database="___ops")

    def local_keys() -> set[tuple]:
        with duckdb.connect(str(path), read_only=True) as cache:
            local = cache.execute(query).fetchdf()
        return set(local[keys].itertuples(index=False, name=None))

    if expected.empty:
        from multi_league.core.date_utils import get_nfl_state

        state = get_nfl_state() or {}
        season_type = state.get("season_type")
        season = str(state.get("season", ""))
        league_season = str(state.get("league_season", season))
        unplayed = (
            season_type == "pre" and season == str(year)
        ) or (
            season_type == "off" and state.get("league_season") is not None
            and league_season == str(year) and season.isdecimal()
            and 0 < int(season) <= year
        )
        if unplayed and not local_keys():
            log(f"[TRACK 1] Verified unplayed NFL season {year}; no OPS weeks to hydrate")
            return False
        raise RuntimeError(f"No finalized Fly OPS inputs available for import year {year}")

    expected_keys = set(expected[keys].itertuples(index=False, name=None))

    def differing_weeks() -> list[int]:
        return sorted({int(row[0]) for row in expected_keys.symmetric_difference(local_keys())})

    weeks = sorted(set(differing_weeks()) | {int(expected["week"].max())})
    log(f"[TRACK 1] Refreshing latest/mismatched finalized OPS weeks for {year}: {weeks}")
    _patch_research_ops_cache_from_fly(
        reader, expected, base=path, year=year, weeks=weeks,
        work_dir=path.parent, in_place=True, authoritative_weeks=True,
    )
    if remaining := differing_weeks():
        raise RuntimeError(f"OPS cache still differs from finalized inputs for {year} weeks {remaining}")
    return True


def run_track_1_verify(
    start_year: int,
    end_year: int,
    dry_run: bool = False,
) -> bool:
    """Verify NFL super table has required data for the given year range.

    Queries ___ops.nfl_historical.nfl_player_stats_all to check row counts.
    Hydrate missing active-season OPS weeks before SQL enrichment. A configured
    cache that cannot supply them raises: callers historically ignore False.
    """
    if dry_run:
        log("[TRACK 1][DRY-RUN] Would verify NFL super table")
        return True

    def _verify_result(result: pd.DataFrame, source: str) -> bool:
        if result.empty:
            log(f"[TRACK 1] WARNING: No NFL data found for years {start_year}-{end_year} ({source})")
            return False

        total_rows = result["rows"].sum()
        years_found = len(result)
        log(f"[TRACK 1] NFL super table verified from {source}: {years_found} years, {total_rows:,} total rows")

        if end_year not in result["year"].values:
            log(f"[TRACK 1] WARNING: No NFL data for current year {end_year}")

        return True

    ops_cache_path = os.environ.get("OPS_CACHE_PATH")
    if ops_cache_path:
        path = Path(ops_cache_path)
        if path.exists():
            try:
                import duckdb

                conn = duckdb.connect(str(path), read_only=True)
                try:
                    result = conn.execute(
                        """
                        SELECT year, COUNT(*) as rows
                        FROM nfl_historical.nfl_player_stats_all
                        WHERE year BETWEEN ? AND ?
                        GROUP BY year ORDER BY year
                        """,
                        [start_year, end_year],
                    ).fetchdf()
                finally:
                    conn.close()
                from multi_league.core.date_utils import get_current_nfl_season_year

                if end_year not in result["year"].values or end_year == get_current_nfl_season_year(
                    reference_date=datetime.now()
                ):
                    if not _ensure_import_ops_weeks(path, end_year):
                        return True
                    with duckdb.connect(str(path), read_only=True) as cache:
                        result = cache.execute(
                            "SELECT year, COUNT(*) AS rows FROM nfl_historical.nfl_player_stats_all "
                            "WHERE year BETWEEN ? AND ? GROUP BY year ORDER BY year",
                            [start_year, end_year],
                        ).fetchdf()
                return _verify_result(result, "local ops cache")
            except Exception as e:
                raise RuntimeError(
                    f"Import OPS cache is unavailable for {end_year}; refusing to continue with missing rank inputs"
                ) from e
        raise RuntimeError(f"Import OPS cache is missing for {end_year}: {path}")

    try:
        from multi_league.core.db_reader import get_reader

        reader = get_reader()
        query = f"""
            SELECT year, COUNT(*) as rows
            FROM nfl_historical.nfl_player_stats_all
            WHERE year BETWEEN {start_year} AND {end_year}
            GROUP BY year ORDER BY year
        """
        result = reader.query_df(query, database="___ops")
        return _verify_result(result, "Fly")

    except Exception as e:
        log(f"[TRACK 1] Error verifying NFL data: {e}")
        return False


# ---------------------------------------------------------------------------
# run_transformations
# ---------------------------------------------------------------------------


def run_transformations(
    ctx: Any,
    transformations: list,
    pass_name: str,
    dry_run: bool = False,
    context_file_path: str | Path | None = None,
    *,
    db_name: str | None = None,
    data_dir: str | Path | None = None,
    quick: bool = False,
) -> list[tuple[str, bool]]:
    """Run a set of transformation scripts.

    Works with any context type. The context file path is resolved as:
    1. Explicit ``context_file_path`` if provided
    2. Auto-detected from context type: sleeper_context.json, espn_context.json,
       or league_context.json (Yahoo default)

    When ``db_name`` and ``data_dir`` are provided they are forwarded to
    :func:`run_script` which will pass ``--db``/``--data-dir`` instead of
    ``--context`` to child processes.

    Args:
        ctx: Any context object with ``data_directory`` attribute
        transformations: List of (script_path, description, timeout) or
                         (script_path, description, timeout, additional_args)
        pass_name: Name of this transformation pass (for logging)
        dry_run: If True, simulate without running
        context_file_path: Explicit path to context JSON file
        db_name: League database name (forwarded to run_script)
        data_dir: Local data directory path (forwarded to run_script)
        quick: If True, passes --quick to child scripts (forwarded to run_script)

    Returns:
        List of (description, success) tuples
    """
    results = []

    log(f"\n[TRANSFORMATIONS] {pass_name}")
    log("-" * 60)

    # Resolve context file path (only needed when not using --db/--data-dir mode)
    if context_file_path is None and not (db_name and data_dir):
        context_file_path = _detect_context_file(ctx)

    for transform in transformations:
        if len(transform) == 4:
            script_path, description, timeout, additional_args = transform
        else:
            script_path, description, timeout = transform
            additional_args = None

        if dry_run:
            log(f"  [DRY-RUN] Would run: {description}")
            results.append((description, True))
            continue

        try:
            success, output = run_script(
                script_path,
                description,
                str(context_file_path or ""),
                additional_args=additional_args,
                timeout=timeout,
                db_name=db_name,
                data_dir=str(data_dir) if data_dir else None,
                quick=quick,
            )

            if success:
                log(f"  OK: {description}")
            else:
                log(f"  FAIL: {description}")
                if output:
                    log(f"    {output[:200]}")

            results.append((description, success))

        except Exception as e:
            log(f"  ERROR: {description} - {e}")
            results.append((description, False))

    return results


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def compute_fetch_years(
    all_years: list[int],
    external_years: set[int],
    remote_skip_years: list[int],
    local_skip_years: list[int] | None,
    fetcher_name: str,
    log_func=None,
) -> list[int]:
    """Filter years to fetch, skipping external, remote-cached, and local-complete years.

    This consolidates the copy-pasted skip logic that was duplicated per fetcher.

    Args:
        all_years: Full list of years to consider
        external_years: Years with external/staging data (skip these)
        remote_skip_years: Years already cached remotely (skip these)
        local_skip_years: Years already complete in local DuckDB (skip these)
        fetcher_name: Name for log messages (e.g., "Matchups", "Rosters")
        log_func: Optional log function (defaults to module log)

    Returns:
        Filtered list of years to fetch
    """
    _log = log_func or log
    years = list(all_years)

    if external_years:
        skipped = [y for y in years if y in external_years]
        if skipped:
            _log(f"[INFO] {fetcher_name}: skipping {len(skipped)} year(s) with external data: {skipped}")
        years = [y for y in years if y not in external_years]

    if remote_skip_years:
        skipped = [y for y in years if y in remote_skip_years]
        if skipped:
            _log(f"[INFO] {fetcher_name}: skipping {len(skipped)} year(s) with remote cache: {skipped}")
        years = [y for y in years if y not in remote_skip_years]

    if local_skip_years:
        skipped = [y for y in years if y in local_skip_years]
        if skipped:
            _log(f"[INFO] {fetcher_name}: skipping {len(skipped)} year(s) already complete in local DB: {skipped}")
        years = [y for y in years if y not in local_skip_years]

    return years


# ---------------------------------------------------------------------------
# drop_league_tables  (shared across Yahoo / Sleeper / ESPN)
# ---------------------------------------------------------------------------


def drop_league_tables(
    ctx: Any,
    db_name: str,
    dry_run: bool = False,
    platform: str | None = None,
) -> int:
    """Drop all league tables for a clean-slate quick import.

    In centralized mode, clears this league's rows from the shared ___leagues
    tables instead of dropping whole per-league catalogs.

    Args:
        ctx: Any context object that ``get_db_name`` / ``register_league_early`` accept.
        db_name: League database name.
        dry_run: Log what *would* happen without executing.
        platform: Platform string for ``register_league_early`` (auto-detected if None).

    Returns:
        Number of tables dropped.
    """

    if dry_run:
        log("[FRESH START] DRY-RUN: Would clear centralized league rows")
        return 0

    # Fresh start is handled by FlyTarget.replace_database()
    log("[FRESH START] Fly backend — fresh start handled by FlyTarget")
    return 0


# ---------------------------------------------------------------------------
# upload_league_tables  (shared across Yahoo / Sleeper / ESPN)
# ---------------------------------------------------------------------------


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    return json.dumps(value, default=str, ensure_ascii=False, sort_keys=True)


def _bool_from_payload(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _keeper_rules_to_flat_columns(keeper_rules: Any) -> dict[str, Any]:
    from multi_league.core.keeper_config_flatten import flatten_rules_to_columns
    from multi_league.core.keeper_config_schema import KEEPER_CONFIG_FIELD_COLUMNS

    rules = keeper_rules if isinstance(keeper_rules, dict) else {}
    flat = flatten_rules_to_columns(rules, strict=False)
    for column in KEEPER_CONFIG_FIELD_COLUMNS:
        if column in rules and rules[column] is not None:
            flat[column] = rules[column]
    return flat


def persist_frontend_settings_tables(
    db,
    db_name: str,
    ctx: Any,
    platform: str | None = None,
) -> None:
    """Persist import context settings into the frontend-owned settings tables.

    The frontend can edit these tables directly, and imports can also receive
    the same settings in their context payload. Keeping them in the canonical
    upload set means a reimport does not silently drop keeper/rule/override
    configuration when the local DuckDB is merged into Fly.
    """
    runtime = runtime_from_source(ctx)
    resolved_platform = platform or runtime.platform
    manager_overrides = getattr(ctx, "manager_name_overrides", None) or runtime.manager_name_overrides
    keeper_rules = getattr(ctx, "keeper_rules", None)
    league_rules = getattr(ctx, "league_rules", None)
    standings_weights = getattr(ctx, "standings_weights", None)
    franchise_merges = getattr(ctx, "franchise_merges", None)
    is_private = _bool_from_payload(getattr(ctx, "is_private", False))

    for table in ("league_context", "keeper_config", "league_rules", "manager_overrides", "standings_config"):
        db.ensure_table(table)

    conn = db.connect()
    conn.execute("DELETE FROM public.league_context WHERE db_name = ?", [db_name])
    conn.execute(
        """
        INSERT INTO public.league_context (
            db_name,
            platform,
            league_id,
            league_name,
            league_ids_json,
            manager_name_overrides_json,
            franchise_merges_json,
            keeper_rules_json,
            league_rules_json,
            standings_weights_json,
            is_private,
            updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            db_name,
            resolved_platform,
            runtime.league_id,
            runtime.league_name,
            _json_or_none(runtime.league_ids),
            _json_or_none(manager_overrides),
            _json_or_none(franchise_merges),
            _json_or_none(keeper_rules),
            _json_or_none(league_rules),
            _json_or_none(standings_weights),
            is_private,
        ],
    )

    if keeper_rules is not None:
        from multi_league.core.keeper_config_schema import KEEPER_CONFIG_FIELD_COLUMNS

        flat_keeper_rules = _keeper_rules_to_flat_columns(keeper_rules)
        field_columns = list(KEEPER_CONFIG_FIELD_COLUMNS)
        field_sql = ", ".join(field_columns)
        placeholders = ", ".join(["?"] * len(field_columns))
        conn.execute(
            "DELETE FROM public.keeper_config WHERE db_name = ? AND year = 0",
            [db_name],
        )
        conn.execute(
            f"""
            INSERT INTO public.keeper_config
                (db_name, year, updated_at, {field_sql})
            VALUES (?, 0, CURRENT_TIMESTAMP, {placeholders})
            """,
            [db_name] + [flat_keeper_rules.get(column) for column in field_columns],
        )

    if league_rules is not None:
        rules = league_rules if isinstance(league_rules, dict) else {}
        conn.execute("DELETE FROM public.league_rules WHERE db_name = ?", [db_name])
        conn.execute(
            """
            INSERT INTO public.league_rules
                (db_name, sacko_mode, faab_budget, league_format, created_at, updated_at)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            """,
            [
                db_name,
                rules.get("sacko_mode"),
                _float_or_none(rules.get("faab_budget")),
                rules.get("league_format"),
            ],
        )

    if standings_weights is not None:
        conn.execute("DELETE FROM public.standings_config WHERE db_name = ?", [db_name])
        conn.execute(
            """
            INSERT INTO public.standings_config
                (db_name, config_json, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            [db_name, _json_or_none(standings_weights)],
        )


def upload_league_tables(
    db,
    db_name: str,
    ctx: Any,
    platform: str | None = None,
    dry_run: bool = False,
) -> list[tuple[str, bool]]:
    """Upload all local DuckDB tables to Fly.

    Calls ``db.upload_to_fly(db_name)`` after persisting frontend settings.

    Args:
        db: ``LocalLeagueDB`` instance (must be connected).
        db_name: Target league database name.
        ctx: Context object for ``register_league_early``.
        platform: Platform string (auto-detected if None).
        dry_run: Skip actual upload.

    Returns:
        List of ``(description, success)`` tuples for results tracking.
    """
    results: list[tuple[str, bool]] = []

    if dry_run:
        log("[TRACK 2] DRY-RUN: Would upload to Fly")
        results.append(("Fly Upload", True))
        return results

    try:
        upload_platform = platform or _detect_platform(ctx)
        persist_frontend_settings_tables(db, db_name, ctx, upload_platform)
        log(f"[TRACK 2] Uploading to Fly.io ({db_name}) via LocalLeagueDB...")
        db.upload_to_fly(
            db_name,
            platform=upload_platform,
            merge_source_ctx=ctx
            if (getattr(ctx, "merge_source", None) or getattr(ctx, "merge_sources", None))
            else None,
        )
        log(f"[TRACK 2] [OK] All tables merged to Fly ({db_name})")

        results.append(("Fly Upload", True))
    except Exception as e:
        log(f"[TRACK 2] FAIL: {e}")
        results.append(("Fly Upload", False))

    return results


def _detect_platform(ctx: Any) -> str:
    """Detect platform from a context."""
    return runtime_from_source(ctx).platform


def _detect_context_file(ctx: Any) -> Path:
    """Detect the serialized context path for a context."""
    path = resolve_context_path(ctx)
    if path is None:
        raise FileNotFoundError("Could not resolve context file path")
    return path
