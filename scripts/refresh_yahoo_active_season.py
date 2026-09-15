#!/usr/bin/env python3
"""Incrementally refresh one Yahoo league's active season through the normal pipeline.

This is the manual GitHub-worker entrypoint for a user-facing weekly refresh.
It reads one league's existing Fly source state into a local DuckDB, resolves
the Yahoo renewal chain, fetches every finalized NFL week since the prior
materialized week, rebuilds the normal enrichments and aggregates locally, and
publishes one scoped Fleet bundle.  It never issues raw Fly DML.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_SCRIPTS = ROOT / "fantasy_football_data_scripts"
if str(DATA_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DATA_SCRIPTS))


LEAGUES_DATABASE = "___leagues"
OPS_DATABASE = "___ops"
SOURCE_TABLES = (
    "matchup",
    "player_fantasy",
    "draft",
    "transactions",
    "schedule",
    "league_settings",
    "franchise_identity_audit",
    "franchise_identity_registry",
    "keeper_config",
    "league_context",
    "league_rules",
    "manager_overrides",
    "standings_config",
    "franchise_identity_audit",
    "franchise_identity_registry",
    "draft_manager_career",
    "draft_player_career",
    "matchup_career",
    "matchup_h2h_career",
    "player_fantasy_career",
    "player_fantasy_career_all",
    "transaction_manager_career",
    "transaction_player_career",
    "homepage_current_standings",
    "homepage_league_summary",
    "homepage_manager_profiles",
    "homepage_manager_rankings",
    "homepage_top_rivalries",
)
ACTIVE_REFRESH_SOURCE_TABLES = (
    "matchup",
    "player_fantasy",
    "draft",
    "transactions",
    "schedule",
    "league_settings",
    # League-level keeper configuration is an input to the normal quick
    # enrichment, but remains excluded from the weekly publish bundle.
    "keeper_config",
    "league_context",
)
UPDATE_REFRESH_SOURCE_TABLES = SOURCE_TABLES
YAHOO_LEAGUE_KEY_RE = re.compile(r"^\d+\.l\.\d+$")


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _quoted_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


_YAHOO_DRAFT_IDENTITY_KEYS = ("year", "round", "pick")


def _blank_identity_values(values: pd.Series) -> pd.Series:
    """Return the missing-value mask used for provider identity fields."""
    normalized = values.astype(str).str.strip().str.lower()
    return values.isna() | normalized.isin({"", "none", "nan", "<na>"})


def normalize_yahoo_roster_provider_identity(rosters: pd.DataFrame) -> pd.DataFrame:
    """Expose the raw Yahoo ``player_id`` under the canonical provider key."""
    normalized = rosters.copy()
    if "player_id" not in normalized.columns:
        return normalized
    if "yahoo_player_id" not in normalized.columns:
        normalized["yahoo_player_id"] = normalized["player_id"]
        return normalized
    blank = _blank_identity_values(normalized["yahoo_player_id"])
    normalized.loc[blank, "yahoo_player_id"] = normalized.loc[blank, "player_id"]
    return normalized


def _patched_yahoo_draft_identities(hydrated: pd.DataFrame, identities: pd.DataFrame) -> pd.DataFrame:
    """Overlay bulk Yahoo draft identities without replacing retained draft facts.

    An active refresh has an authoritative hydrated draft already. Yahoo's
    ``draftresults/players`` endpoint supplies the missing player identities,
    but not manager-level context. Preserve that existing context and reject
    a partial identity payload rather than silently publishing an unresolved
    draft row.
    """
    if hydrated is None or hydrated.empty:
        raise RuntimeError("Yahoo draft identity repair requires a hydrated active-season draft")
    missing_keys = [key for key in _YAHOO_DRAFT_IDENTITY_KEYS if key not in hydrated.columns]
    if missing_keys:
        raise RuntimeError(f"hydrated Yahoo draft is missing identity keys: {missing_keys}")
    if identities is None or identities.empty:
        identity_frame = pd.DataFrame(columns=[*_YAHOO_DRAFT_IDENTITY_KEYS, "yahoo_player_id"])
    else:
        missing_identity_keys = [key for key in _YAHOO_DRAFT_IDENTITY_KEYS if key not in identities.columns]
        if missing_identity_keys:
            raise RuntimeError(f"Yahoo draft identity payload is missing keys: {missing_identity_keys}")
        identity_columns = [
            column
            for column in [*_YAHOO_DRAFT_IDENTITY_KEYS, "yahoo_player_id", "player", "yahoo_position"]
            if column in identities.columns
        ]
        identity_frame = identities[identity_columns].copy()
        identity_frame = identity_frame.drop_duplicates(subset=list(_YAHOO_DRAFT_IDENTITY_KEYS), keep="last")

    renamed = identity_frame.rename(
        columns={
            "yahoo_player_id": "_identity_yahoo_player_id",
            "player": "_identity_player",
            "yahoo_position": "_identity_yahoo_position",
        }
    )
    patched = hydrated.copy().merge(renamed, on=list(_YAHOO_DRAFT_IDENTITY_KEYS), how="left", validate="many_to_one")
    if "yahoo_player_id" not in patched.columns:
        patched["yahoo_player_id"] = pd.NA
    incoming_ids = patched.get("_identity_yahoo_player_id", pd.Series(pd.NA, index=patched.index))
    incoming_id_present = ~_blank_identity_values(incoming_ids)
    patched.loc[incoming_id_present, "yahoo_player_id"] = incoming_ids.loc[incoming_id_present].astype(str)

    for column, identity_column in (("player", "_identity_player"), ("yahoo_position", "_identity_yahoo_position")):
        if identity_column not in patched.columns:
            continue
        if column not in patched.columns:
            patched[column] = pd.NA
        incoming = patched[identity_column]
        fill = _blank_identity_values(patched[column]) & ~_blank_identity_values(incoming)
        patched.loc[fill, column] = incoming.loc[fill]

    named_rows = pd.Series(True, index=patched.index)
    if "player" in patched.columns:
        named_rows = ~_blank_identity_values(patched["player"])
    unresolved = named_rows & _blank_identity_values(patched["yahoo_player_id"])
    if unresolved.any():
        raise RuntimeError(
            "Yahoo draftresults/players did not return IDs for "
            f"{int(unresolved.sum())} named active-season draft pick(s)"
        )
    return patched.drop(columns=[column for column in patched.columns if column.startswith("_identity_")])


def _fetch_yahoo_draft_identities(*, oauth: Any, league_key: str, year: int) -> pd.DataFrame:
    """Read Yahoo's one bulk draft-results endpoint for active identity repair."""
    from multi_league.data_fetchers.yahoo.yahoo_draft import fetch_draft_picks

    picks = fetch_draft_picks(oauth, league_key, year)
    return pd.DataFrame(
        [
            {
                "year": int(pick.year),
                "round": int(pick.round),
                "pick": int(pick.pick),
                "yahoo_player_id": str(pick.yahoo_player_id or "").strip(),
                "player": pick.player,
                "yahoo_position": pick.yahoo_position,
            }
            for pick in picks
        ]
    )


def _active_yahoo_history(
    ctx: Any,
    *,
    oauth: Any,
    active_year: int,
    source_active_key: str | None = None,
    discover: Any,
) -> dict[str, str]:
    """Use a saved or retained active key; discover only as a legacy fallback."""
    saved = getattr(ctx, "league_ids", None) or {}
    history = {
        str(year): str(league_key)
        for year, league_key in saved.items()
        if str(year).strip() and str(league_key).strip()
    }
    if str(active_year) in history:
        return history

    if source_active_key and YAHOO_LEAGUE_KEY_RE.fullmatch(str(source_active_key).strip()):
        return {str(active_year): str(source_active_key).strip()}

    # OAuth credentials authorize a Yahoo account, not a particular renewal
    # chain.  The credential registry's league_id can therefore point at a
    # different league owned by the same user.  Extend the canonical chain
    # from its newest saved key so account-level credentials can never redirect
    # a weekly update into an unrelated league.
    saved_anchors: list[tuple[int, str]] = []
    for season, league_key in history.items():
        try:
            season_number = int(season)
        except (TypeError, ValueError):
            continue
        if YAHOO_LEAGUE_KEY_RE.fullmatch(league_key):
            saved_anchors.append((season_number, league_key))
    anchor = max(saved_anchors)[1] if saved_anchors else str(ctx.league_id)

    discovered = {
        str(year): str(league_key)
        for year, league_key in discover(anchor, oauth=oauth, end_year=active_year).items()
        if str(year).strip() and str(league_key).strip()
    }
    conflicts = {
        season: (history[season], league_key)
        for season, league_key in discovered.items()
        if season in history and history[season] != league_key
    }
    if conflicts:
        details = ", ".join(
            f"{season}: saved={saved_key}, discovered={discovered_key}"
            for season, (saved_key, discovered_key) in sorted(conflicts.items())
        )
        raise RuntimeError(f"Yahoo renewal discovery conflicted with the saved chain: {details}")
    history.update(discovered)
    return history


def _active_yahoo_key_from_source_frames(
    source_frames: dict[str, pd.DataFrame],
    *,
    active_year: int,
) -> str | None:
    """Return the retained active Yahoo key without a provider round trip.

    ``league_settings`` is hydrated from canonical current-season source state
    before provider fetching. It already holds the exact Yahoo game key needed
    for a weekly refresh, so walking every historical renewal link only adds
    latency and does not change this import's scope.
    """
    settings = source_frames.get("league_settings")
    if settings is None or settings.empty or "league_key" not in settings.columns:
        return None

    scoped = settings.copy()
    if "year" in scoped.columns:
        years = pd.to_numeric(scoped["year"], errors="coerce")
        scoped = scoped.loc[years == int(active_year)]
    candidates = {
        str(value).strip()
        for value in scoped["league_key"].dropna().tolist()
        if YAHOO_LEAGUE_KEY_RE.fullmatch(str(value).strip())
    }
    if len(candidates) > 1:
        raise RuntimeError(
            "active Yahoo source settings contain conflicting league keys: "
            + ", ".join(sorted(candidates))
        )
    return next(iter(candidates), None)


def _frontend_settings_from_source_context(source_context: pd.DataFrame, *, db_name: str) -> dict[str, Any]:
    """Reuse the already-hydrated canonical context without another Fly read."""
    from initial_import_v3 import _parse_context_bool, _parse_context_json

    if source_context is None or len(source_context) != 1:
        raise RuntimeError(f"expected exactly one league_context row for {db_name}")
    if "db_name" not in source_context.columns:
        raise RuntimeError("league_context source is missing db_name")
    source_db_names = {str(value) for value in source_context["db_name"].dropna().tolist()}
    if source_db_names != {db_name}:
        raise RuntimeError(f"league_context source did not match {db_name}")

    row = source_context.iloc[0]

    def value(column: str) -> object:
        raw = row.get(column)
        return None if raw is None or pd.isna(raw) else raw

    league_name = value("league_name")
    is_private = value("is_private")
    return {
        "league_name": str(league_name).strip() if league_name is not None else "",
        "league_ids": _parse_context_json(value("league_ids_json"), {}),
        "manager_name_overrides": _parse_context_json(value("manager_name_overrides_json"), {}),
        "franchise_merges": _parse_context_json(value("franchise_merges_json"), []),
        "keeper_rules": _parse_context_json(value("keeper_rules_json"), None),
        "league_rules": _parse_context_json(value("league_rules_json"), None),
        "standings_weights": _parse_context_json(value("standings_weights_json"), None),
        # ``query_df`` may materialize a BOOLEAN as ``numpy.bool_``; convert
        # it to the parser's stable string contract instead of resetting it.
        "is_private": _parse_context_bool(str(is_private) if is_private is not None else None, False),
    }


def _persist_yahoo_renewal_chain(
    local_db: Any,
    *,
    source_context: pd.DataFrame,
    db_name: str,
    history: dict[str, str],
) -> bool:
    """Backfill a discovered Yahoo chain without changing frontend-owned settings.

    Weekly refreshes normally do not publish ``league_context``.  The one
    exception is a legacy Yahoo context that lacks its renewal chain: persisting
    that exact new field prevents Yahoo discovery on every subsequent week.  We
    restore the original Fly row first so manager aliases, keeper rules, and all
    other frontend settings cannot be altered by the quick pipeline.
    """
    if source_context is None or len(source_context) != 1:
        raise RuntimeError(f"expected exactly one league_context row for {db_name}")
    if "db_name" not in source_context.columns:
        raise RuntimeError("league_context source is missing db_name")
    values = {str(value) for value in source_context["db_name"].dropna().tolist()}
    if values != {db_name}:
        raise RuntimeError(f"league_context source did not match {db_name}")

    canonical_history = {
        str(year): str(league_key)
        for year, league_key in history.items()
        if str(year).strip() and str(league_key).strip()
    }
    serialized_history = json.dumps(canonical_history, sort_keys=True, separators=(",", ":"))
    raw_existing = source_context.iloc[0].get("league_ids_json")
    try:
        existing_history = json.loads(raw_existing) if raw_existing else {}
    except (TypeError, json.JSONDecodeError):
        existing_history = {}
    if existing_history == canonical_history:
        return False

    restored = source_context.copy()
    restored["league_ids_json"] = serialized_history
    conn = local_db.connect()
    conn.execute("DELETE FROM public.league_context WHERE db_name = ?", [db_name])
    local_db._insert_into_table("league_context", restored)
    conn.execute(
        "UPDATE public.league_context SET updated_at = CURRENT_TIMESTAMP WHERE db_name = ?",
        [db_name],
    )
    return True


def _empty_source_frame() -> pd.DataFrame:
    return pd.DataFrame({"db_name": pd.Series(dtype="string")})


def _concat_history_parts(parts: list[pd.DataFrame]) -> pd.DataFrame:
    """Concat historical Fly chunks without pandas all-null dtype guessing.

    A table's modern schema often contains feature columns that are entirely
    null in old years.  Those null-only columns carry no values for their
    chunk, so omit them for the concat and add them back afterward.  This
    preserves all values while avoiding pandas' deprecated dtype inference.
    """
    if not parts:
        return _empty_source_frame()

    column_order = list(dict.fromkeys(column for part in parts for column in part.columns))
    normalized_parts: list[pd.DataFrame] = []
    for part in parts:
        null_only_columns = [
            column
            for column in part.columns
            if part[column].isna().all()
        ]
        normalized_parts.append(part.drop(columns=null_only_columns))

    return pd.concat(normalized_parts, ignore_index=True).reindex(columns=column_order)


def _active_source_snapshot_frames(
    reader: Any,
    *,
    registry: dict[str, Any],
    db_name: str,
    active_year: int,
    table_names: tuple[str, ...],
) -> dict[str, pd.DataFrame]:
    """Read scoped league source rows in one Fly round trip, preserving schemas.

    ``FlyReader.query_df`` is intentionally one-table-at-a-time.  The weekly
    path needs the same raw canonical rows but does not need seven sequential
    HTTP requests, so serialize each row on the DuckDB side and tag it with
    its source table.  This changes transport only; the standard quick-import
    hydration and transforms receive the same per-table frames as before.
    """
    safe_db = _sql_literal(db_name)
    query_parts: list[str] = []
    for table_name in table_names:
        table_ref = f"public.{_quoted_identifier(table_name)}"
        where_clause = f"db_name = {safe_db}"
        # keeper_config contains the league-wide year=0 default (and possible
        # per-year overrides), so the active-season source snapshot must retain
        # all of it rather than narrowing to the NFL year.
        if active_year is not None:
            if table_name == "league_settings":
                # The first update of a new season may not have an active-year
                # settings row yet. Hydrate the newest prior row until the
                # authoritative provider response replaces it below.
                where_clause += (
                    " AND year = COALESCE("
                    f"(SELECT MAX(year) FROM {table_ref} "
                    f"WHERE db_name = {safe_db} AND year = {int(active_year)}), "
                    f"(SELECT MAX(year) FROM {table_ref} "
                    f"WHERE db_name = {safe_db} AND year < {int(active_year)})"
                    ")"
                )
            elif table_name != "keeper_config" and "year" in registry[table_name]["columns"]:
                where_clause += f" AND year = {int(active_year)}"
        query_parts.append(
            "(SELECT "
            f"{_sql_literal(table_name)} AS source_table, to_json(t) AS payload "
            f"FROM {table_ref} AS t WHERE {where_clause}"
            ")"
        )

    rows = reader.query(
        "SELECT source_table, payload FROM ("
        + " UNION ALL ".join(query_parts)
        + ") AS active_source_snapshot",
        database=LEAGUES_DATABASE,
    )
    payloads: dict[str, list[dict[str, Any]]] = {table_name: [] for table_name in table_names}
    for row in rows:
        table_name = str(row.get("source_table", ""))
        if table_name not in payloads:
            raise RuntimeError(f"Fly active source snapshot returned unknown table {table_name!r}")
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if not isinstance(payload, dict):
            raise RuntimeError(f"Fly active source snapshot returned invalid {table_name} payload")
        payloads[table_name].append(payload)

    return {
        table_name: pd.DataFrame(records) if records else _empty_source_frame()
        for table_name, records in payloads.items()
    }


def _transaction_matchup_windows(
    local_db: Any,
    *,
    year: int,
    refresh_weeks: list[int],
    current_schedule_frames: list[pd.DataFrame],
) -> pd.DataFrame | None:
    """Return a complete retained schedule calendar for transaction timestamp mapping.

    ``fetch_transactions`` normally builds this calendar by making another
    Yahoo scoreboard request.  A weekly refresh already hydrated its prior
    active-season schedule and fetched the current week, so reuse those exact
    provider rows when they cover every week through the refresh boundary.
    A missing week falls back to the fetcher's established Yahoo calendar
    builder rather than guessing transaction dates.
    """
    frames: list[pd.DataFrame] = []
    try:
        frames.append(local_db.read_table("schedule", year=year))
    except Exception:
        pass
    frames.extend(current_schedule_frames)

    usable: list[pd.DataFrame] = []
    required = {"year", "week", "week_start", "week_end"}
    for frame in frames:
        if frame is None or frame.empty or not required.issubset(frame.columns):
            continue
        candidate = frame.loc[:, ["year", "week", "week_start", "week_end"]].copy()
        candidate["year"] = pd.to_numeric(candidate["year"], errors="coerce")
        candidate["week"] = pd.to_numeric(candidate["week"], errors="coerce")
        candidate = candidate.dropna(subset=["year", "week"])
        candidate = candidate[candidate["year"].astype(int) == int(year)]
        if not candidate.empty:
            usable.append(candidate)

    if not usable:
        return None

    windows = pd.concat(usable, ignore_index=True)
    windows["year"] = windows["year"].astype(int)
    windows["week"] = windows["week"].astype(int)
    windows = windows.drop_duplicates(subset=["year", "week"], keep="last").sort_values("week")
    required_weeks = set(range(1, max(int(week) for week in refresh_weeks) + 1)) if refresh_weeks else set()
    if not required_weeks.issubset(set(windows["week"].tolist())):
        return None

    windows["cumulative_week"] = (
        windows["year"].astype(str) + windows["week"].astype(str).str.zfill(2)
    ).astype(int)
    return windows[["year", "week", "week_start", "week_end", "cumulative_week"]].reset_index(drop=True)


def _source_frames(
    reader: Any,
    *,
    db_name: str,
    active_year: int | None = None,
    tables: tuple[str, ...] | None = None,
) -> dict[str, pd.DataFrame]:
    """Fetch canonical source history in bounded year chunks from Fly.

    A normal historical rebuild reads every available year in bounded chunks.
    The weekly worker requests ``active_year`` and reads only the current
    season before running the normal quick-import transforms.
    """
    from multi_league.core.delta_publish import canonical_table_registry

    registry = canonical_table_registry()
    safe_db = _sql_literal(db_name)
    frames: dict[str, pd.DataFrame] = {}
    requested_tables = tables or SOURCE_TABLES
    unknown = sorted(set(requested_tables) - set(SOURCE_TABLES))
    if unknown:
        raise RuntimeError("unknown refresh source table(s): " + ", ".join(unknown))
    if tables is not None:
        for table_name in requested_tables:
            if table_name not in registry:
                raise RuntimeError(f"{table_name} is not a canonical Fly table")
            scope = f"active {active_year}" if active_year is not None else "history"
            print(f"[hydrate] retaining existing {table_name} {scope} for {db_name}", flush=True)
        return _active_source_snapshot_frames(
            reader,
            registry=registry,
            db_name=db_name,
            active_year=active_year,
            table_names=requested_tables,
        )
    for table_name in requested_tables:
        scope_label = f"active {active_year}" if active_year is not None else "history"
        print(f"[hydrate] retaining existing {table_name} {scope_label} for {db_name}", flush=True)
        if table_name not in registry:
            raise RuntimeError(f"{table_name} is not a canonical Fly table")
        table_ref = f"public.{_quoted_identifier(table_name)}"
        if "year" not in registry[table_name]["columns"]:
            frame = reader.query_df(
                f"SELECT * FROM {table_ref} WHERE db_name = {safe_db}",
                database=LEAGUES_DATABASE,
            )
            frames[table_name] = frame if not frame.empty else _empty_source_frame()
            continue

        if active_year is not None:
            frame = reader.query_df(
                f"SELECT * FROM {table_ref} WHERE db_name = {safe_db} AND year = {int(active_year)}",
                database=LEAGUES_DATABASE,
            )
            frames[table_name] = frame if not frame.empty else _empty_source_frame()
            continue

        years = reader.query(
            f"SELECT DISTINCT year FROM {table_ref} "
            f"WHERE db_name = {safe_db} AND year IS NOT NULL ORDER BY year",
            database=LEAGUES_DATABASE,
        )
        parts: list[pd.DataFrame] = []
        for row in years:
            year_value = row.get("year")
            if year_value is None:
                continue
            part = reader.query_df(
                f"SELECT * FROM {table_ref} WHERE db_name = {safe_db} AND year = {int(year_value)}",
                database=LEAGUES_DATABASE,
            )
            if not part.empty:
                parts.append(part)
        frames[table_name] = _concat_history_parts(parts)
    return frames


def _finalized_ops(reader: Any, *, year: int, through_week: int | None) -> pd.DataFrame:
    week_clause = f" AND week <= {int(through_week)}" if through_week is not None else ""
    return reader.query_df(
        "SELECT week, nfl_team, opponent_nfl_team, NFL_player_id, "
        "CAST(hash(NFL_player_id, nfl_team, opponent_nfl_team, passing_yards, passing_tds, "
        "passing_interceptions, rushing_yards, rushing_tds, receptions, receiving_yards, "
        "receiving_tds, fantasy_points_ppr, def_sacks, def_interceptions, def_tackles_solo, "
        "def_tackles_with_assist, fg_made, pat_made, points_allowed) AS VARCHAR) AS source_revision "
        "FROM nfl_historical.nfl_player_stats_all "
        f"WHERE year = {int(year)} AND COALESCE(season_type, 'REG') = 'REG'{week_clause} "
        "AND NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL AND opponent_nfl_team IS NOT NULL",
        database=OPS_DATABASE,
    )


def _last_materialized_week(reader: Any, *, db_name: str, year: int) -> int | None:
    value = reader.query_scalar(
        "SELECT MAX(week) FROM public.player_fantasy "
        f"WHERE db_name = {_sql_literal(db_name)} AND year = {int(year)}",
        database=LEAGUES_DATABASE,
    )
    return int(value) if value is not None else None


def _load_active_refresh_inputs(
    reader: Any,
    *,
    db_name: str,
    year: int,
    through_week: int | None,
) -> tuple[pd.DataFrame, int | None]:
    """Fetch independent active-season gates concurrently through FlyReader."""
    # FlyReader holds immutable URL/token configuration and each request uses
    # its own ``requests.post`` call, so these independent databases can be
    # read concurrently without sharing a database connection or writing data.
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="active-refresh-gate") as executor:
        finalized_future = executor.submit(_finalized_ops, reader, year=year, through_week=through_week)
        materialized_future = executor.submit(_last_materialized_week, reader, db_name=db_name, year=year)
        return finalized_future.result(), materialized_future.result()


def _patch_research_ops_cache_from_fly(
    reader: Any,
    finalized_ops: pd.DataFrame,
    *,
    base: Path,
    year: int,
    weeks: list[int],
    work_dir: Path,
    in_place: bool = False,
) -> Path:
    """Patch the reduced research cache from Fly's finalized source rows.

    The normal Actions cache intentionally contains only the wide weekly
    super-table, not the complete eight-table artifact required by
    ``refresh_live_nfl_ops.py``.  The canonical Fly ops table is therefore the
    authority here: replace only the completed-game rows in its one joined
    table before SQL enrichment.  The default retains a private copy for a
    reusable local cache; a GitHub Actions weekly worker may patch its
    disposable restored workspace cache in place and avoid copying 739 MB.
    """
    if not base.is_file():
        raise RuntimeError(f"OPS_CACHE_PATH is missing: {base}")
    output = base if in_place else work_dir / "ops_cache_fly_finalized.duckdb"
    if not in_place:
        shutil.copy2(base, output)

    cache = duckdb.connect(str(output))
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
        fly_columns = {
            str(row["column_name"]): str(row["column_type"])
            for row in fly_schema_rows
            if row.get("column_name") and row.get("column_type")
        }
        missing_fly = sorted(required - set(fly_columns))
        if missing_fly:
            raise RuntimeError("Fly ops table is missing required weekly columns: " + ", ".join(missing_fly))
        # Keep the historic cache's legacy aliases, then add new authoritative
        # Fly columns to the copy.  This preserves prior-season enrichment
        # compatibility while making current fields available immediately.
        for column_name, column_type in fly_columns.items():
            if column_name not in cache_columns:
                cache.execute(
                    f"ALTER TABLE {target} ADD COLUMN {_quoted_identifier(column_name)} {column_type}"
                )
        columns = [
            str(row[0])
            for row in cache.execute("DESCRIBE nfl_historical.nfl_player_stats_all").fetchall()
        ]
        select_columns = ", ".join(_quoted_identifier(column) for column in columns)
        source_select = ", ".join(
            _quoted_identifier(column) if column in fly_columns else f"NULL AS {_quoted_identifier(column)}"
            for column in columns
        )
        for week in weeks:
            expected = finalized_ops.loc[finalized_ops["week"].astype(int) == int(week)].copy()
            if expected.empty:
                raise RuntimeError(f"no finalized Fly ops rows for {year} week {week}")
            source = reader.query_df(
                f"SELECT {source_select} FROM {target} "
                f"WHERE year = {int(year)} AND week = {int(week)} "
                "AND COALESCE(season_type, 'REG') = 'REG' "
                "AND NFL_player_id IS NOT NULL AND nfl_team IS NOT NULL AND opponent_nfl_team IS NOT NULL",
                database=OPS_DATABASE,
            )
            if source.empty:
                raise RuntimeError(f"Fly returned no full finalized ops rows for {year} week {week}")
            source = source.loc[:, columns].copy()
            incoming_keys = set(
                zip(
                    source["NFL_player_id"].astype(str),
                    source["nfl_team"].astype(str),
                    source["opponent_nfl_team"].astype(str),
                )
            )
            expected_keys = set(
                zip(
                    expected["NFL_player_id"].astype(str),
                    expected["nfl_team"].astype(str),
                    expected["opponent_nfl_team"].astype(str),
                )
            )
            if incoming_keys != expected_keys:
                raise RuntimeError(
                    f"Fly cache refresh source does not match finalized admission facts for {year} week {week}"
                )
            cache.register("__refresh_facts", source)
            try:
                cache.execute(
                    f"""
                    DELETE FROM {target} AS current
                    WHERE current.year = ?
                      AND current.week = ?
                      AND COALESCE(current.season_type, 'REG') = 'REG'
                      AND EXISTS (
                        SELECT 1
                        FROM __refresh_facts AS incoming
                        WHERE current.nfl_team = incoming.nfl_team
                          AND current.opponent_nfl_team = incoming.opponent_nfl_team
                      )
                    """,
                    [int(year), int(week)],
                )
                cache.execute(
                    f"INSERT INTO {target} ({select_columns}) SELECT {select_columns} FROM __refresh_facts"
                )
            finally:
                cache.unregister("__refresh_facts")
    finally:
        cache.close()

    return output


def _ensure_ops_cache_matches_live(
    reader: Any,
    finalized_ops: pd.DataFrame,
    *,
    year: int,
    weeks: list[int],
    work_dir: Path,
) -> Path:
    """Use the Fly-finalized weekly cache slice for the local SQL pipeline."""
    base = Path(os.environ.get("OPS_CACHE_PATH", "")).resolve()
    current = _patch_research_ops_cache_from_fly(
        reader,
        finalized_ops,
        base=base,
        year=year,
        weeks=weeks,
        work_dir=work_dir,
        in_place=True,
    )
    os.environ["OPS_CACHE_PATH"] = str(current)
    return current


def _merge_refresh_payloads(
    *,
    ctx: Any,
    local_db: Any,
    oauth: Any,
    year: int,
    refresh_weeks: list[int],
    finalized_ops: pd.DataFrame,
) -> dict[str, int]:
    """Fetch provider state and merge only safe active-season rows locally."""
    from multi_league.core.canonical_settings import flatten_settings
    from multi_league.core.league_refresh import (
        assert_provider_roster_merge,
        filter_matchups_to_final_results,
        filter_rosters_to_finalized_games,
        merge_provider_refresh_table,
        missing_provider_draft_keys,
        needs_active_season_draft_fetch,
        provider_draft_manifest_matches,
        replace_active_season_draft,
    )
    from multi_league.core.yahoo_league_settings import fetch_league_settings
    from multi_league.data_fetchers.yahoo.yahoo_draft import fetch_draft_data
    from multi_league.data_fetchers.yahoo.yahoo_matchups import weekly_matchup_data
    from multi_league.data_fetchers.yahoo.yahoo_rosters import fetch_rosters_for_year
    from multi_league.data_fetchers.yahoo.yahoo_schedules import _derive_schedule_df_from_matchup_df
    from multi_league.data_fetchers.yahoo.yahoo_transactions import fetch_transactions

    league_key = ctx.get_league_id_for_year(year)
    raw_settings = fetch_league_settings(year, league_key=league_key, oauth=oauth)
    if not raw_settings:
        raise RuntimeError(f"Yahoo returned no settings for {year} ({league_key})")
    settings_row = pd.DataFrame([flatten_settings(raw_settings, "yahoo", year, league_key)])
    settings_row["db_name"] = local_db.league_name
    merge_provider_refresh_table(
        local_db, "league_settings", settings_row, platform="yahoo", league_id=league_key
    )

    rosters, roster_failures = fetch_rosters_for_year(ctx, year, oauth_session=oauth, weeks=refresh_weeks)
    if roster_failures:
        raise RuntimeError(f"Yahoo roster fetch failed for weeks: {roster_failures}")
    rosters = normalize_yahoo_roster_provider_identity(rosters)
    roster_rows = 0
    for week in refresh_weeks:
        source = rosters[rosters["week"].astype(int) == int(week)].copy()
        ops_slice = finalized_ops[finalized_ops["week"].astype(int) == int(week)]
        safe_rows = filter_rosters_to_finalized_games(source, ops_slice)
        if safe_rows.empty:
            raise RuntimeError(f"No roster rows intersect finalized NFL games for {year} week {week}")
        merge_provider_refresh_table(
            local_db,
            "player_fantasy",
            safe_rows,
            platform="yahoo",
            league_id=league_key,
        )
        assert_provider_roster_merge(
            local_db,
            safe_rows,
            year=year,
            week=week,
            provider_id_column="yahoo_player_id",
        )
        roster_rows += len(safe_rows)

    matchup_rows = 0
    schedule_rows = 0
    current_schedule_frames: list[pd.DataFrame] = []
    for week in refresh_weeks:
        scoreboards, failures = weekly_matchup_data(ctx=ctx, year=year, week=week)
        if failures:
            raise RuntimeError(f"Yahoo matchup fetch failed for {year} week {week}: {failures}")
        final_matchups = filter_matchups_to_final_results(scoreboards)
        if not final_matchups.empty:
            merge_provider_refresh_table(
                local_db,
                "matchup",
                final_matchups,
                platform="yahoo",
                league_id=league_key,
            )
            matchup_rows += len(final_matchups)

        raw_schedule = scoreboards.attrs.get("schedule_df")
        if raw_schedule is None or raw_schedule.empty:
            raise RuntimeError(f"Yahoo returned no schedule graph for {year} week {week}")
        current_schedule_frames.append(raw_schedule)
        schedule = _derive_schedule_df_from_matchup_df(
            raw_schedule,
            year,
            getattr(ctx, "manager_name_overrides", None) or {},
        )
        merge_provider_refresh_table(
            local_db,
            "schedule",
            schedule,
            platform="yahoo",
            league_id=league_key,
        )
        schedule_rows += len(schedule)

    transaction_windows = _transaction_matchup_windows(
        local_db,
        year=year,
        refresh_weeks=refresh_weeks,
        current_schedule_frames=current_schedule_frames,
    )
    if transaction_windows is None:
        print("[Yahoo] Retained schedule has a gap; rebuilding transaction windows through Yahoo", flush=True)
    else:
        print("[Yahoo] Reusing retained schedule windows for transaction timestamp mapping", flush=True)
    transactions = fetch_transactions(
        ctx=ctx,
        year=year,
        matchup_windows=transaction_windows,
        local_db=local_db,
    )
    if transactions is not None and not transactions.empty:
        merge_provider_refresh_table(
            local_db,
            "transactions",
            transactions,
            platform="yahoo",
            league_id=league_key,
        )

    draft_rows = 0
    has_hydrated_draft = local_db.table_exists("draft") and int(local_db.row_count("draft") or 0) > 0
    identity_rows = None
    missing_provider_keys: set[tuple[str, ...]] = set()
    provider_manifest_matches = True
    fetched_authoritative_draft = False
    if has_hydrated_draft:
        # ``draftresults/players`` is a single Yahoo request.  It provides the
        # immutable pick manifest without the 12+ roster calls in the complete
        # draft fetch, so every weekly run can prove its retained draft is whole.
        identity_rows = _fetch_yahoo_draft_identities(oauth=oauth, league_key=league_key, year=year)
        missing_provider_keys = missing_provider_draft_keys(
            local_db.read_table("draft"),
            identity_rows,
            key_columns=_YAHOO_DRAFT_IDENTITY_KEYS,
        )
        provider_manifest_matches = provider_draft_manifest_matches(
            local_db.read_table("draft"),
            identity_rows,
            key_columns=_YAHOO_DRAFT_IDENTITY_KEYS,
        )

    if needs_active_season_draft_fetch(
        local_db,
        platform="yahoo",
        provider_manifest=identity_rows,
        manifest_key_columns=_YAHOO_DRAFT_IDENTITY_KEYS if identity_rows is not None else (),
    ):
        if has_hydrated_draft and (missing_provider_keys or not provider_manifest_matches):
            draft = fetch_draft_data(ctx=ctx, year=year)
            fetched_authoritative_draft = True
            reason = (
                f"{len(missing_provider_keys)} provider pick(s) were absent locally"
                if missing_provider_keys
                else "local draft identities included stale or duplicate provider picks"
            )
            print(f"[Yahoo] Re-fetching {year} draft: {reason}", flush=True)
        elif has_hydrated_draft:
            draft = _patched_yahoo_draft_identities(local_db.read_table("draft"), identity_rows)
            print(
                f"[Yahoo] Repaired {year} draft identities through one bulk draftresults/players request",
                flush=True,
            )
        else:
            draft = fetch_draft_data(ctx=ctx, year=year)
            fetched_authoritative_draft = True
        if draft is not None and not draft.empty:
            if fetched_authoritative_draft:
                draft_rows = replace_active_season_draft(
                    local_db,
                    draft,
                    year=year,
                    platform="yahoo",
                    league_id=league_key,
                )
            else:
                merge_provider_refresh_table(
                    local_db,
                    "draft",
                    draft,
                    platform="yahoo",
                    league_id=league_key,
                )
                draft_rows = int(len(draft))
    else:
        print(f"[Yahoo] Verified hydrated {year} draft against provider manifest", flush=True)

    return {
        "roster_rows": int(roster_rows),
        "final_matchup_rows": int(matchup_rows),
        "schedule_rows": int(schedule_rows),
        "transaction_rows": int(len(transactions) if transactions is not None else 0),
        "draft_rows": draft_rows,
    }


def _aggregate_subprocess_env() -> dict[str, str]:
    """Give the aggregate child process the same source-package import path."""
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    entries = [str(DATA_SCRIPTS)]
    if existing:
        entries.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(entries)
    return env


def _run_refresh_simulations(
    *,
    db_name: str,
    active_year: int | None,
    current_week: int,
    work_dir: Path,
    n_sims: int = 10_000,
) -> None:
    """Rebuild active-season luck, playoff, and clutch values deterministically."""
    common = [
        "--db",
        db_name,
        "--data-dir",
        str(work_dir),
        "--target-year",
        str(int(active_year)),
        "--n-sims",
        str(int(n_sims)),
    ]
    subprocess.run(
        [
            sys.executable,
            "-m",
            "multi_league.transformations.matchup.expected_record_v2",
            *common,
            "--current-year",
            str(int(active_year)),
            "--current-week",
            str(int(current_week)),
            "--seed",
            "42",
        ],
        cwd=ROOT,
        env=_aggregate_subprocess_env(),
        check=True,
    )
    subprocess.run(
        [
            sys.executable,
            "-m",
            "multi_league.transformations.matchup.playoff_odds_import",
            *common,
        ],
        cwd=ROOT,
        env=_aggregate_subprocess_env(),
        check=True,
    )


def _run_refresh_aggregates(
    local_db: Any,
    *,
    db_name: str,
    active_year: int,
    work_dir: Path,
    has_finalized_matchups: bool,
) -> None:
    """Rebuild active-season aggregate outputs without career-only work.

    A scoped weekly Fleet bundle publishes only season aggregates.  Reuse the
    existing aggregate functions directly so the worker does not create
    unpublished career tables or pay two child-process startup costs.  The
    full quick SQL enrichment has already run before this function.

    Matchup/standings aggregation stays on its existing subprocess path while
    finalized-score output equivalence is verified separately.
    """
    from multi_league.transformations.aggregation.aggregate_draft_context import (
        aggregate_draft_manager_career,
        aggregate_draft_manager_season,
        aggregate_draft_player_career,
        create_draft_manager_career_table,
        create_draft_manager_season_table,
        create_draft_player_career_table,
    )
    from multi_league.transformations.aggregation.aggregate_fantasy_context import (
        aggregate_fantasy_career,
        aggregate_fantasy_career_all,
        aggregate_fantasy_season,
        aggregate_fantasy_season_all,
        create_fantasy_career_table,
        create_fantasy_career_table_all,
        create_fantasy_season_table,
        create_fantasy_season_table_all,
    )
    from multi_league.transformations.aggregation.aggregate_transaction_context import (
        aggregate_transaction_manager_career,
        aggregate_transaction_manager_season,
        aggregate_transaction_player_career,
        aggregate_transaction_report_card,
        create_transaction_manager_career_table,
        create_transaction_manager_season_table,
        create_transaction_player_career_table,
        create_transaction_report_card_table,
    )
    from multi_league.transformations.aggregation.aggregation_utils import configure_table_catalog

    conn = local_db.connect()
    configure_table_catalog(conn)
    create_fantasy_season_table(conn, db_name)
    aggregate_fantasy_season(conn, db_name, year=active_year)
    create_fantasy_season_table_all(conn, db_name)
    aggregate_fantasy_season_all(conn, db_name, year=active_year)
    create_fantasy_career_table(conn, db_name)
    aggregate_fantasy_career(conn, db_name)
    create_fantasy_career_table_all(conn, db_name)
    aggregate_fantasy_career_all(conn, db_name)
    create_draft_manager_season_table(conn, db_name)
    aggregate_draft_manager_season(conn, db_name)
    create_draft_manager_career_table(conn, db_name)
    aggregate_draft_manager_career(conn, db_name)
    create_draft_player_career_table(conn, db_name)
    aggregate_draft_player_career(conn, db_name)
    create_transaction_manager_season_table(conn, db_name)
    aggregate_transaction_manager_season(conn, db_name)
    create_transaction_manager_career_table(conn, db_name)
    aggregate_transaction_manager_career(conn, db_name)
    create_transaction_player_career_table(conn, db_name)
    aggregate_transaction_player_career(conn, db_name)
    create_transaction_report_card_table(conn, db_name)
    aggregate_transaction_report_card(conn, db_name)

    if not has_finalized_matchups:
        return

    local_db.close()
    try:
        subprocess.run(
            [
                sys.executable,
                str(ROOT / "scripts" / "refresh_aggregates.py"),
                "--db",
                db_name,
                "--data-dir",
                str(work_dir),
                "--steps",
                "matchup,standings",
            ],
            cwd=ROOT,
            env=_aggregate_subprocess_env(),
            check=True,
        )
    finally:
        local_db.connect()


def _run_local_pipeline(
    *,
    ctx: Any,
    context_path: Path,
    local_db: Any,
    db_name: str,
    active_year: int,
    work_dir: Path,
    platform: str = "yahoo",
    keeper_config_hydrated: bool = False,
) -> None:
    from multi_league.core.import_pipeline import (
        require_sql_enrichment_success,
        run_transformation_pipeline,
    )
    from multi_league.transformations.sql_enrichments import SQLEnrichments

    # Keep the provider graph for an incomplete current week.  The shared
    # matchup transform intentionally rebuilds ``schedule`` from *final*
    # matchup rows, which would otherwise erase a live week's schedule.
    provider_schedule = local_db.read_table("schedule", year=active_year)
    local_db.close()
    transforms = run_transformation_pipeline(
        ctx,
        skip_track_2_upload=True,
        import_mode="quick",
        context_file_path=context_path,
        platform=platform,
        db_name=db_name,
        data_dir=work_dir,
        quick=True,
    )
    failed = [name for name, ok in transforms if not ok]
    if failed:
        raise RuntimeError("shared transformation pipeline failed: " + ", ".join(failed))

    local_db.connect()
    enricher = SQLEnrichments(
        db_name=db_name,
        data_dir=str(work_dir),
        quick=True,
        conn=local_db.connect(),
        keeper_config_hydrated=keeper_config_hydrated,
        manager_name_overrides=getattr(ctx, "manager_name_overrides", None),
        franchise_merges=getattr(ctx, "franchise_merges", None),
    )
    try:
        enricher.load_settings_from_db()
        results = enricher.run_all()
        require_sql_enrichment_success(results)

        # The common matchup enrichment rebuilds schedule from finalized matchup
        # rows. Restore the provider's live schedule graph afterward, then apply
        # the saved identity settings to that final copy as well.
        if not local_db._conn:
            local_db.connect()

        if not provider_schedule.empty:
            local_db.merge_table(
                "schedule",
                provider_schedule,
                ["db_name", "manager_week"],
                platform=platform,
                league_id=ctx.get_league_id_for_year(active_year),
            )
            enricher.reapply_saved_identity_settings()
    finally:
        enricher.close()
    active_matchup_row = local_db.connect().execute(
        "SELECT COUNT(*), MAX(week) FROM public.matchup "
        "WHERE db_name = ? AND year = ? AND team_points IS NOT NULL "
        "AND opponent_points IS NOT NULL AND COALESCE(is_bye_week, FALSE) = FALSE",
        [db_name, int(active_year)],
    ).fetchone()
    active_matchup_count = int(active_matchup_row[0] or 0)
    has_finalized_matchups = active_matchup_count > 0
    if has_finalized_matchups:
        local_db.close()
        _run_refresh_simulations(
            db_name=db_name,
            active_year=active_year,
            current_week=int(active_matchup_row[1]),
            work_dir=work_dir,
        )
        local_db.connect()
    _run_refresh_aggregates(
        local_db,
        db_name=db_name,
        active_year=active_year,
        work_dir=work_dir,
        has_finalized_matchups=has_finalized_matchups,
    )


def _publish_generation(reader: Any, db_name: str) -> int:
    value = reader.query_scalar(
        "SELECT COALESCE(MAX(generation), 0) FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {_sql_literal(db_name)}",
        database=LEAGUES_DATABASE,
    )
    return int(value or 0)


def _scope_counts(reader: Any, *, db_name: str, active_year: int, tables: list[str]) -> dict[str, int]:
    from multi_league.core.delta_publish import CADENCE_ACTIVE_SEASON, canonical_table_registry

    registry = canonical_table_registry()
    safe_db = _sql_literal(db_name)
    parts = []
    for table in tables:
        predicate = f"db_name = {safe_db}"
        if registry[table]["cadence_class"] == CADENCE_ACTIVE_SEASON:
            predicate += f" AND year = {int(active_year)}"
        parts.append(f"SELECT {_sql_literal(table)} AS table_name, COUNT(*) AS rows FROM public.{_quoted_identifier(table)} WHERE {predicate}")
    rows = reader.query(" UNION ALL ".join(parts), database=LEAGUES_DATABASE)
    return {str(row["table_name"]): int(row["rows"] or 0) for row in rows}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="Yahoo league db_name")
    parser.add_argument("--year", type=int, default=None, help="Active NFL season (default: current ops season)")
    parser.add_argument("--through-week", type=int, default=None, help="Optional final-week ceiling")
    parser.add_argument("--observed-manifest-digest")
    parser.add_argument("--execute", action="store_true", help="Commit the scoped Fleet bundle to Fly")
    parser.add_argument("--json-out", type=Path, help="Optional non-secret run receipt path")
    args = parser.parse_args(argv)

    os.environ["DATABASE_BACKEND"] = "fly"
    from initial_import_v3 import _build_context_from_fly
    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.league_refresh import (
        active_refresh_publish_tables,
        completed_weeks_to_refresh,
        finalized_source_boundary,
        hydrate_local_refresh_sources,
        stage_refresh_partitions,
    )
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.league_update_plan import load_persisted_refresh_plan
    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.core.targets.fly_target import FlyTarget
    from multi_league.core.yahoo_league_settings import discover_league_history

    reader = FlyReader()
    active_year = args.year
    if active_year is None:
        active_year = int(
            reader.query_scalar("SELECT MAX(year) FROM nfl_historical.nfl_player_stats_all", database=OPS_DATABASE)
        )
    finalized_ops, last_materialized_week = _load_active_refresh_inputs(
        reader,
        db_name=args.db,
        year=active_year,
        through_week=args.through_week,
    )
    if finalized_ops.empty:
        raise RuntimeError(f"No finalized regular-season ops facts for {active_year}")
    finalized_weeks = sorted({int(value) for value in finalized_ops["week"].dropna().tolist()})
    persisted_plan = load_persisted_refresh_plan(
        reader,
        database_name=args.db,
        active_season=active_year,
        expected_observed_digest=args.observed_manifest_digest,
    )
    refresh_weeks = (
        list(persisted_plan.weeks)
        if persisted_plan is not None
        else completed_weeks_to_refresh(
            finalized_weeks=finalized_weeks,
            last_materialized_week=last_materialized_week,
        )
    )
    receipt: dict[str, Any] = {"db_name": args.db, "year": active_year, "refresh_weeks": refresh_weeks, "executed": bool(args.execute)}
    receipt.update(finalized_source_boundary(finalized_ops, year=active_year))
    if persisted_plan is not None:
        receipt["source_manifest_digest"] = persisted_plan.observed_manifest_digest
        receipt["source_manifest_json"] = persisted_plan.observed_manifest_json
        receipt["published_manifest_digest"] = persisted_plan.published_manifest_digest
        receipt["refresh_reasons"] = list(persisted_plan.reasons)
    if not refresh_weeks:
        receipt["status"] = "NO_FINALIZED_WEEKS"
        print(json.dumps(receipt, sort_keys=True))
        if args.json_out:
            args.json_out.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
        return 0

    with tempfile.TemporaryDirectory(prefix=f"{args.db}_weekly_refresh_") as temp_dir:
        work_dir = Path(temp_dir)
        source_frames = _source_frames(
            reader,
            db_name=args.db,
            tables=UPDATE_REFRESH_SOURCE_TABLES,
        )
        if source_frames["league_context"].empty or source_frames["league_settings"].empty:
            raise RuntimeError(f"Fly has no reusable context/settings for {args.db}")
        from multi_league.core.league_update_ownership import source_preservation_snapshot

        preservation_witnesses = source_preservation_snapshot(source_frames)
        source_active_key = _active_yahoo_key_from_source_frames(
            source_frames,
            active_year=active_year,
        )
        frontend_settings = _frontend_settings_from_source_context(
            source_frames["league_context"],
            db_name=args.db,
        )
        ctx, context_path = _build_context_from_fly(
            args.db,
            data_dir_override=str(work_dir),
            reader=reader,
            frontend_settings=frontend_settings,
        )
        oauth = ctx.get_oauth_session()
        saved_history = getattr(ctx, "league_ids", None) or {}
        had_saved_active_key = str(active_year) in {str(year) for year in saved_history}
        history = _active_yahoo_history(
            ctx,
            oauth=oauth,
            active_year=active_year,
            source_active_key=source_active_key,
            discover=discover_league_history,
        )
        if str(active_year) not in history:
            raise RuntimeError(f"Yahoo renewal chain has no {active_year} league key for {args.db}")
        ctx.league_ids = history
        ctx.start_year = active_year
        ctx.end_year = active_year
        ctx.import_mode = "quick"
        ctx.save(context_path)
        receipt["league_key"] = history[str(active_year)]
        local_db = LocalLeagueDB(work_dir, args.db)
        try:
            receipt["hydrated_rows"] = hydrate_local_refresh_sources(
                local_db,
                source_frames,
                db_name=args.db,
                active_year=active_year,
                expected_platform="yahoo",
            )
            from multi_league.core.league_update_ownership import local_preservation_snapshot

            preservation_before = local_preservation_snapshot(local_db, preservation_witnesses)
            receipt["fetch_rows"] = _merge_refresh_payloads(
                ctx=ctx,
                local_db=local_db,
                oauth=oauth,
                year=active_year,
                refresh_weeks=refresh_weeks,
                finalized_ops=finalized_ops,
            )
            if not args.execute:
                receipt["status"] = "DRY_RUN_READY"
                print(json.dumps(receipt, sort_keys=True))
                if args.json_out:
                    args.json_out.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
                return 0

            from multi_league.core.league_refresh import (
                active_platform_player_ids,
                active_platform_player_names,
                active_platform_player_name_hints,
                sync_player_bio_cache_from_fly,
            )

            active_connection = local_db.connect()
            receipt["player_bio_sync"] = sync_player_bio_cache_from_fly(
                reader,
                ops_cache=Path(os.environ.get("OPS_CACHE_PATH", "")),
                platform="yahoo",
                provider_ids=active_platform_player_ids(active_connection, platform="yahoo"),
                player_names=active_platform_player_names(active_connection, platform="yahoo"),
                provider_name_hints=active_platform_player_name_hints(active_connection, platform="yahoo"),
            )
            receipt["ops_cache"] = str(
                _ensure_ops_cache_matches_live(
                    reader,
                    finalized_ops,
                    year=active_year,
                    weeks=refresh_weeks,
                    work_dir=work_dir,
                )
            )
            _run_local_pipeline(
                ctx=ctx,
                context_path=context_path,
                local_db=local_db,
                db_name=args.db,
                active_year=active_year,
                work_dir=work_dir,
                keeper_config_hydrated="keeper_config" in source_frames,
            )
            # A retained active key is deliberately only a one-year fast path.
            # Persist context only after a real full chain discovery, never as
            # a side effect of a weekly update.
            receipt["renewal_chain_backfilled"] = False
            if source_active_key is None and not had_saved_active_key:
                receipt["renewal_chain_backfilled"] = _persist_yahoo_renewal_chain(
                    local_db,
                    source_context=source_frames["league_context"],
                    db_name=args.db,
                    history=history,
                )
            local_db.connect()
            from multi_league.core.homepage_refresh import prepare_homepage_refresh

            homepage = prepare_homepage_refresh(
                reader=reader,
                local_db=local_db,
                db_name=args.db,
                active_year=active_year,
            )
            from multi_league.core.league_update_ownership import (
                assert_refresh_preservation,
                local_preservation_snapshot,
            )

            receipt["preservation"] = assert_refresh_preservation(
                preservation_before,
                local_preservation_snapshot(local_db, preservation_before),
                active_year=active_year,
            )
            publish_tables = active_refresh_publish_tables(local_db.connect())
            publish_tables.extend(homepage["published_tables"])
            if receipt["renewal_chain_backfilled"]:
                publish_tables.append("league_context")
            publish_tables = sorted(set(publish_tables))
            from multi_league.core.league_update_ownership import assert_publish_table_ownership

            receipt["ownership"] = assert_publish_table_ownership(publish_tables)
            stage = stage_refresh_partitions(
                local_db.connect(),
                db_name=args.db,
                active_year=active_year,
                tables=publish_tables,
            )
            try:
                if not publish_tables:
                    raise RuntimeError("refresh pipeline produced no active-season publish tables")
                generation = _publish_generation(reader, args.db)
                bundle = build_fleet_partition_bundle(
                    stage,
                    active_year=active_year,
                    league_generations={args.db: generation},
                    tables=publish_tables,
                    output_dir=work_dir / "bundle",
                )
            finally:
                stage.close()
            result = FlyTarget().merge_fleet_partition(
                bundle.path,
                bundle_id=bundle.bundle_id,
                bundle_hash=bundle.bundle_hash,
            )
            if str(result.get("status") or "").upper() != "COMMITTED":
                raise RuntimeError(f"scoped weekly refresh did not commit: {result}")
            receipt["status"] = "COMMITTED"
            receipt["data_bundle_id"] = bundle.bundle_id
            receipt["bundle_id"] = bundle.bundle_id
            receipt["homepage_bundle_id"] = bundle.bundle_id
            receipt["homepage_rows"] = homepage["rows"]
            receipt["published_tables"] = publish_tables
            receipt["post_publish_counts"] = _scope_counts(
                reader,
                db_name=args.db,
                active_year=active_year,
                tables=receipt["published_tables"],
            )
        finally:
            local_db.close()

    print(json.dumps(receipt, sort_keys=True))
    if args.json_out:
        args.json_out.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
