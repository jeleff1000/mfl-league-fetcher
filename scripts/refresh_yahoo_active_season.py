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
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_SCRIPTS = ROOT / "fantasy_football_data_scripts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
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
    # These are small identity/configuration witnesses.  They retain the
    # franchise graph and user aliases without moving historical fact tables.
    "franchise_identity_audit",
    "franchise_identity_registry",
    # League-level keeper configuration is an input to the normal quick
    # enrichment, but remains excluded from the weekly publish bundle.
    "keeper_config",
    "league_context",
)
# A weekly refresh is a current-season delta.  Historical source partitions
# remain canonical in Fly and are never copied into this disposable local DB.
# Whole-history homepage rollups use their dedicated skinny Fly snapshot.
UPDATE_REFRESH_SOURCE_TABLES = ACTIVE_REFRESH_SOURCE_TABLES
YAHOO_LEAGUE_KEY_RE = re.compile(r"^\d+\.l\.\d+$")
# Scoring rules are provider-defined and their threshold is deliberately part
# of the key (for example ``scoring_bonus_pass_yd_325``).  The canonical
# registry cannot enumerate every valid threshold, but they are still source
# facts and must survive an active-season transform.
_DYNAMIC_LEAGUE_SETTINGS_COLUMN_RE = re.compile(
    r"^(?:bonus|scoring_bonus)_[a-z0-9_]+$"
)


from multi_league.core.ops_cache import (
    _finalized_ops,
    _sql_literal,
    _quoted_identifier,
    _ops_refresh_source_projection,
    _ops_delta_keys,
    _patch_research_ops_cache_from_fly,
)


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
    active_key = str(source_active_key or "").strip()
    if not YAHOO_LEAGUE_KEY_RE.fullmatch(active_key):
        active_key = ""
    saved_active_key = history.get(str(active_year))
    if saved_active_key and active_key and saved_active_key != active_key:
        raise RuntimeError("Fly context and settings have conflicting active Yahoo league keys")
    if saved_active_key and YAHOO_LEAGUE_KEY_RE.fullmatch(saved_active_key):
        return history

    if active_key:
        # Retained active settings avoid discovery, but the worker context must
        # still carry the entire saved cross-platform lineage and user history.
        return {**history, str(active_year): active_key}

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
    if not YAHOO_LEAGUE_KEY_RE.fullmatch(str(history.get(str(active_year)) or "")):
        raise RuntimeError("Yahoo renewal chain has no valid active Yahoo league key")
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


def _active_update_segment_from_source_frames(
    source_frames: dict[str, pd.DataFrame],
    *,
    db_name: str,
    active_year: int,
    expected_platform: str,
) -> Any:
    """Resolve the single current provider leg from the imported timeline."""
    context = source_frames.get("league_context")
    settings = source_frames.get("league_settings")
    if context is None or len(context) != 1:
        raise RuntimeError(f"Fly has no unique league context for {db_name}")
    if settings is None:
        raise RuntimeError(f"Fly has no league settings timeline for {db_name}")
    context_row = context.iloc[0]
    if str(context_row.get("db_name") or "").strip() != db_name:
        raise RuntimeError(f"league context does not belong to {db_name}")
    from multi_league.core.league_update_lineage import resolve_active_update_segment

    return resolve_active_update_segment(
        active_year=int(active_year),
        context_platform=context_row.get("platform"),
        context_league_id=context_row.get("league_id"),
        settings_rows=settings.to_dict("records"),
        expected_platform=expected_platform,
    )


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
    """Read scoped league source rows as compact column packs.

    Fly's JSON endpoint is row-oriented. Serializing a wide active-season table
    with ``to_json(t)`` repeats every field name for every row. DuckDB's
    ``list(COLUMNS(*))`` returns the same rows column-wise. Fetch independent
    tables concurrently, then restore their ordinary DataFrame shape for the
    unchanged quick-import hydration and transformations.
    """
    safe_db = _sql_literal(db_name)

    def fetch_table(table_name: str) -> tuple[str, pd.DataFrame]:
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
        rows = reader.query(
            f"SELECT list(COLUMNS(*)) FROM {table_ref} WHERE {where_clause}",
            database=LEAGUES_DATABASE,
        )
        if not rows:
            return table_name, _empty_source_frame()
        if len(rows) != 1:
            raise RuntimeError(f"Fly active source snapshot returned multiple packs for {table_name}")

        columns: dict[str, list[Any]] = {}
        lengths: set[int] = set()
        for column, values in rows[0].items():
            if values is None:
                values = []
            if not isinstance(values, list):
                raise RuntimeError(
                    f"Fly active source snapshot returned invalid {table_name}.{column} column pack"
                )
            columns[str(column)] = values
            lengths.add(len(values))
        if len(lengths) > 1:
            raise RuntimeError(f"Fly active source snapshot returned uneven columns for {table_name}")

        frame = pd.DataFrame(columns)
        if table_name == "keeper_config":
            # Older Fly keeper tables retain a server-generated ``created_at``
            # column from the pre-canonical DDL. Weekly updates never publish
            # keeper_config, and the canonical shared schema intentionally owns
            # only ``updated_at`` plus the user's keeper fields. Keep the live
            # row untouched on Fly while excluding this legacy metadata from
            # local transformation and preservation witnesses.
            frame = frame.drop(columns=["created_at"], errors="ignore")
        if frame.empty and "db_name" not in frame.columns:
            return table_name, _empty_source_frame()
        return table_name, frame

    frames: dict[str, pd.DataFrame] = {}
    with ThreadPoolExecutor(max_workers=min(4, len(table_names))) as pool:
        futures = [pool.submit(fetch_table, table_name) for table_name in table_names]
        for future in futures:
            table_name, frame = future.result()
            frames[table_name] = frame
    return {table_name: frames[table_name] for table_name in table_names}


def _historical_source_snapshot_frames(
    reader: Any,
    *,
    registry: dict[str, Any],
    db_name: str,
    table_names: tuple[str, ...],
    year_chunk_size: int = 4,
) -> dict[str, pd.DataFrame]:
    """Read complete league history in bounded table/year JSON payloads.

    A refresh must hydrate the full persisted chain before rebuilding shared
    career and homepage enrichments.  It must *not* serialize every wide
    source row from every table through one tagged ``UNION ALL`` request:
    larger leagues exceed Fly's response budget and turn a retryable read into
    several minutes of 503 retries.  A small year manifest keeps the snapshot
    deterministic; each raw payload stays bounded to one table and a handful
    of seasons.
    """
    if year_chunk_size < 1:
        raise ValueError("year_chunk_size must be positive")

    safe_db = _sql_literal(db_name)
    year_tables = tuple(
        table_name
        for table_name in table_names
        if "year" in registry[table_name]["columns"]
    )
    year_manifest_parts = [
        "(SELECT "
        f"{_sql_literal(table_name)} AS source_table, year "
        f"FROM public.{_quoted_identifier(table_name)} "
        f"WHERE db_name = {safe_db} AND year IS NOT NULL GROUP BY year)"
        for table_name in year_tables
    ]
    year_rows = reader.query(
        "SELECT source_table, year FROM ("
        + " UNION ALL ".join(year_manifest_parts)
        + ") AS history_source_years",
        database=LEAGUES_DATABASE,
    ) if year_manifest_parts else []

    years_by_table: dict[str, list[int]] = {table_name: [] for table_name in year_tables}
    for row in year_rows:
        table_name = str(row.get("source_table", ""))
        if table_name not in years_by_table:
            raise RuntimeError(f"Fly historical source manifest returned unknown table {table_name!r}")
        year = pd.to_numeric(pd.Series([row.get("year")]), errors="coerce").iloc[0]
        if pd.notna(year):
            years_by_table[table_name].append(int(year))

    payloads: dict[str, list[dict[str, Any]]] = {table_name: [] for table_name in table_names}
    for table_name in table_names:
        table_ref = f"public.{_quoted_identifier(table_name)}"
        predicates: list[str]
        if table_name in years_by_table:
            years = sorted(set(years_by_table[table_name]))
            predicates = [
                f"db_name = {safe_db} AND year IN ({','.join(str(year) for year in years[index:index + year_chunk_size])})"
                for index in range(0, len(years), year_chunk_size)
            ]
        else:
            predicates = [f"db_name = {safe_db}"]

        for predicate in predicates:
            rows = reader.query(
                f"SELECT to_json(t) AS payload FROM {table_ref} AS t WHERE {predicate}",
                database=LEAGUES_DATABASE,
            )
            for row in rows:
                payload = row.get("payload")
                if isinstance(payload, str):
                    payload = json.loads(payload)
                if not isinstance(payload, dict):
                    raise RuntimeError(f"Fly historical source snapshot returned invalid {table_name} payload")
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
        if active_year is None:
            return _historical_source_snapshot_frames(
                reader,
                registry=registry,
                db_name=db_name,
                table_names=requested_tables,
            )
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


def _split_active_transform_source_frames(
    source_frames: dict[str, pd.DataFrame],
    *,
    active_year: int,
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """Keep historical source facts for rollups, never for active transforms."""
    transform_input = {name: frame.copy() for name, frame in source_frames.items()}
    historical_rows: dict[str, pd.DataFrame] = {}
    for table_name in ACTIVE_REFRESH_SOURCE_TABLES:
        frame = transform_input.get(table_name)
        if frame is None or frame.empty or "year" not in frame.columns:
            continue
        historical = pd.to_numeric(frame["year"], errors="coerce").lt(int(active_year))
        if not historical.any():
            continue
        historical_rows[table_name] = frame.loc[historical].copy()
        transform_input[table_name] = frame.loc[~historical].copy()
    return transform_input, historical_rows


def _restore_historical_source_rows(
    local_db: Any,
    historical_rows: dict[str, pd.DataFrame],
) -> None:
    """Restore immutable historical facts after current-season enrichment."""
    from multi_league.core.delta_publish import canonical_table_registry

    registry = canonical_table_registry()
    for table_name, frame in historical_rows.items():
        if frame.empty:
            continue
        if not local_db.table_exists(table_name):
            raise RuntimeError(f"active transform removed historical source table {table_name}")
        conn = local_db.connect()
        target_columns = {
            str(column)
            for column, *_ in conn.execute(f"DESCRIBE public.{_quoted_identifier(table_name)}").fetchall()
        }
        missing_columns = sorted(set(frame.columns) - target_columns)
        allowed_columns = set(registry[table_name]["columns"])
        dynamic_settings_columns = {
            column
            for column in missing_columns
            if table_name == "league_settings"
            and _DYNAMIC_LEAGUE_SETTINGS_COLUMN_RE.fullmatch(column)
        }
        unexpected = sorted(
            set(missing_columns) - allowed_columns - dynamic_settings_columns
        )
        if unexpected:
            raise RuntimeError(
                f"historical {table_name} has unregistered columns after active transform: {unexpected}"
            )
        if missing_columns:
            conn.register("_historical_restore_schema", frame)
            try:
                source_types = {
                    str(column): str(column_type)
                    for column, column_type, *_ in conn.execute(
                        "DESCRIBE _historical_restore_schema"
                    ).fetchall()
                }
            finally:
                conn.unregister("_historical_restore_schema")
            for column in missing_columns:
                conn.execute(
                    f"ALTER TABLE public.{_quoted_identifier(table_name)} "
                    f"ADD COLUMN {_quoted_identifier(column)} {source_types[column]}"
                )
        local_db._insert_into_table(table_name, frame)


_FRONTEND_OWNED_REFRESH_TABLES = (
    "keeper_config",
    "league_context",
    "league_rules",
    "manager_overrides",
    "standings_config",
)


def _restore_frontend_configuration_rows(
    local_db: Any,
    source_frames: dict[str, pd.DataFrame],
    *,
    db_name: str,
) -> None:
    """Restore exact saved user configuration after shared enrichment.

    These tables are inputs to a refresh, never provider output.  The normal
    transformation pipeline may materialize an import context while building
    local artifacts; restore the Fly snapshot before preservation validation so
    aliases, keeper rules, and other user choices cannot be rewritten by a
    weekly update.
    """
    for table_name in _FRONTEND_OWNED_REFRESH_TABLES:
        source = source_frames.get(table_name)
        if source is None:
            continue
        if "db_name" not in source.columns:
            raise RuntimeError(f"saved frontend configuration {table_name} has no db_name")
        configured = source.loc[source["db_name"].astype(str).eq(str(db_name))].copy()
        local_db.ensure_table(table_name)
        conn = local_db.connect()
        conn.execute(
            f"DELETE FROM public.{_quoted_identifier(table_name)} WHERE db_name = ?",
            [db_name],
        )
        if not configured.empty:
            local_db._insert_into_table(table_name, configured)


def _last_materialized_week(reader: Any, *, db_name: str, year: int) -> int | None:
    value = reader.query_scalar(
        "SELECT TRY_CAST(week AS INTEGER) FROM public.matchup "
        f"WHERE db_name = {_sql_literal(db_name)} AND year = {int(year)} "
        "AND TRY_CAST(week AS INTEGER) IS NOT NULL "
        "ORDER BY TRY_CAST(week AS INTEGER) DESC LIMIT 1",
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
        finalized_ops = finalized_future.result()
        played_watermark = materialized_future.result()
    # An explicit manual ceiling may deliberately revisit an older played
    # week; never let a later materialized week turn that scope into a no-op.
    if through_week is not None and played_watermark is not None:
        played_watermark = min(played_watermark, int(through_week))
    return finalized_ops, played_watermark


_ACTIVE_YEAR_OPS_IDENTITY_COLUMNS = {
    "NFL_player_id",
    "game_date",
    "headshot_url",
    "nfl_position",
    "nfl_team",
    "opponent_nfl_team",
    "player",
    "player_week",
    "position",
    "season_type",
    "week",
    "year",
}


def _active_year_ops_projection_columns(
    schema_columns: list[str],
    scoring_info: dict[str, Any],
) -> list[str]:
    """Return the bounded super-table projection used by one weekly update."""
    from multi_league.core.league_update_manifest import resolve_nfl_scoring_input_columns
    from multi_league.transformations.player.modules.ppg_precompute import (
        get_ppg_columns_for_scoring,
    )

    available = set(schema_columns)
    # Global freshness includes every rank variant; this cache only needs the
    # active league's ranks, added explicitly below.
    selected = {column for column in resolve_nfl_scoring_input_columns(schema_columns)
                if not column.startswith("rank_")}
    selected.update(_ACTIVE_YEAR_OPS_IDENTITY_COLUMNS)

    fpts_column = str(scoring_info.get("fpts_col") or "")
    if fpts_column in available:
        selected.add(fpts_column)
    elif "fpts_4pt_half" in available:
        selected.add("fpts_4pt_half")

    rolling_total = str(scoring_info.get("rolling_total_col") or "")
    if rolling_total:
        selected.add(rolling_total)

    rank_columns = {
        str(column)
        for column in (scoring_info.get("rank_cols") or {}).values()
        if column
    }
    selected.update(rank_columns)
    selected.update(
        "rank_alltime_" + column.removeprefix("rank_")
        for column in rank_columns
        if column.startswith("rank_")
        and not column.startswith(("rank_alltime_", "rank_season_"))
    )

    td_key = str(scoring_info.get("td_key") or "4pt")
    pass_td_points = int(td_key.removesuffix("pt"))
    ppr = float(scoring_info.get("ppr", 0.5))
    selected.update(get_ppg_columns_for_scoring(ppr, pass_td_points).values())

    # Preserve source schema order so cache construction remains deterministic.
    return [column for column in schema_columns if column in selected]


def _active_year_scoring_info(local_db: Any, *, db_name: str, year: int) -> dict[str, Any]:
    """Resolve the active league's existing scoring variant without NFL reads."""
    from multi_league.transformations.sql_enrichments import SQLEnrichments

    planner = SQLEnrichments(
        db_name=db_name,
        quick=True,
        data_dir=str(local_db.data_dir),
        conn=local_db.connect(),
    )
    planner.load_settings_from_db()
    return planner._get_scoring_for_year(year)


def _build_active_year_ops_cache(
    reader: Any,
    *,
    output: Path,
    year: int,
    scoring_info: dict[str, Any],
) -> Path:
    """Build the weekly SQL cache from one active NFL season only.

    The weekly transform database is active-season scoped.  Pulling the
    all-years Actions cache wastes hundreds of megabytes without adding an
    input that this rebuild can publish.  The wide table still carries its
    precomputed all-time rank columns, so the active rows retain the canonical
    historical comparison values.
    """
    stats_target = "nfl_historical.nfl_player_stats_all"
    bio_target = "nfl_historical.player_bio"
    required = {
        "NFL_player_id", "year", "week", "season_type", "nfl_team", "opponent_nfl_team",
    }

    def schema(table: str, *, omit: set[str] | None = None) -> list[tuple[str, str]]:
        omitted = omit or set()
        rows = reader.query(f"DESCRIBE {table}", database=OPS_DATABASE)
        result = [
            (str(row["column_name"]), str(row["column_type"]))
            for row in rows
            if row.get("column_name") and row.get("column_type")
            and str(row["column_name"]) not in omitted
        ]
        if not result:
            raise RuntimeError(f"Fly returned no schema for {table}")
        return result

    full_stats_schema = schema(stats_target, omit={"recon_correction_log"})
    full_stats_types = dict(full_stats_schema)
    stats_columns = _active_year_ops_projection_columns(
        [name for name, _type in full_stats_schema],
        scoring_info,
    )
    stats_schema = [(name, full_stats_types[name]) for name in stats_columns]
    missing = sorted(required - set(stats_columns))
    if missing:
        raise RuntimeError("active-year ops schema is missing required columns: " + ", ".join(missing))
    stats_projection = ", ".join(_quoted_identifier(name) for name in stats_columns)
    stats = reader.query_df_parquet(
        f"SELECT {stats_projection} FROM {stats_target} WHERE year = {int(year)}",
        database=OPS_DATABASE,
    )
    if stats.empty:
        raise RuntimeError(f"Fly returned no NFL ops rows for active year {year}")
    actual_years = {int(value) for value in stats["year"].dropna().tolist()}
    if actual_years != {int(year)}:
        raise RuntimeError(f"active-year ops cache escaped requested year {year}: {sorted(actual_years)}")

    bio_schema = schema(bio_target)
    bio_columns = [name for name, _type in bio_schema]
    if "NFL_player_id" not in bio_columns:
        raise RuntimeError("Fly player_bio schema is missing NFL_player_id")
    bio_projection = ", ".join(_quoted_identifier(name) for name in bio_columns)
    bios = reader.query_df_parquet(
        f"SELECT {bio_projection} FROM {bio_target} "
        f"WHERE NFL_player_id IN (SELECT DISTINCT NFL_player_id FROM {stats_target} "
        f"WHERE year = {int(year)})",
        database=OPS_DATABASE,
    )
    if bios.empty:
        raise RuntimeError(f"Fly returned no player_bio rows for active year {year}")
    if bios["NFL_player_id"].isna().any() or bios["NFL_player_id"].astype(str).duplicated().any():
        raise RuntimeError("active-year player_bio rows have missing or duplicate NFL_player_id values")

    output.parent.mkdir(parents=True, exist_ok=True)
    building = output.with_suffix(output.suffix + ".building")
    if building.exists():
        building.unlink()
    cache = duckdb.connect(str(building))
    try:
        cache.execute("CREATE SCHEMA nfl_historical")
        for target, table_schema, frame, registration in (
            (stats_target, stats_schema, stats, "__active_stats"),
            (bio_target, bio_schema, bios, "__active_bios"),
        ):
            definitions = ", ".join(
                f"{_quoted_identifier(name)} {column_type}" for name, column_type in table_schema
            )
            columns = ", ".join(_quoted_identifier(name) for name, _type in table_schema)
            cache.execute(f"CREATE TABLE {target} ({definitions})")
            cache.register(registration, frame)
            try:
                cache.execute(
                    f"INSERT INTO {target} ({columns}) SELECT {columns} FROM {registration}"
                )
            finally:
                cache.unregister(registration)
    except Exception:
        cache.close()
        if building.exists():
            building.unlink()
        raise
    else:
        cache.close()
    building.replace(output)
    return output


def _ensure_active_year_ops_cache(
    reader: Any,
    *,
    year: int,
    work_dir: Path,
    scoring_info: dict[str, Any],
) -> Path:
    """Return an existing disposable cache or create the bounded weekly one."""
    configured = str(os.environ.get("OPS_CACHE_PATH", "")).strip()
    output = Path(configured).resolve() if configured else work_dir / "ops_cache.duckdb"
    if not _ops_cache_supports_active_scoring(
        output,
        year=year,
        scoring_info=scoring_info,
    ):
        _build_active_year_ops_cache(
            reader,
            output=output,
            year=year,
            scoring_info=scoring_info,
        )
        # This process just read the complete active-year projection from the
        # same live Fly table used by the delta patch.  Record that exact cache
        # identity so the immediately following enrichment step does not issue
        # another schema read plus one fetch per admitted week.
        os.environ["OPS_CACHE_LIVE_ACTIVE_YEAR"] = f"{int(year)}|{output.resolve()}"
    os.environ["OPS_CACHE_PATH"] = str(output)
    return output


def _ops_cache_supports_active_scoring(
    cache_path: Path,
    *,
    year: int,
    scoring_info: dict[str, Any],
) -> bool:
    """Return whether a reusable weekly cache can serve this league variant."""
    if not cache_path.is_file():
        return False

    from multi_league.transformations.player.modules.ppg_precompute import (
        get_ppg_columns_for_scoring,
    )

    td_key = str(scoring_info.get("td_key") or "4pt")
    required = {
        "NFL_player_id",
        "year",
        "week",
        "season_type",
        "nfl_team",
        "opponent_nfl_team",
        *get_ppg_columns_for_scoring(
            float(scoring_info.get("ppr", 0.5)),
            int(td_key.removesuffix("pt")),
        ).values(),
    }
    required.update(
        str(value)
        for value in (scoring_info.get("rank_cols") or {}).values()
        if value
    )
    required.update(
        str(value)
        for value in (
            scoring_info.get("fpts_col"),
            scoring_info.get("rolling_total_col"),
        )
        if value
    )

    cache = None
    try:
        cache = duckdb.connect(str(cache_path), read_only=True)
        columns = {
            str(row[0])
            for row in cache.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'nfl_historical' "
                "AND table_name = 'nfl_player_stats_all'"
            ).fetchall()
        }
        if not required.issubset(columns):
            return False
        return cache.execute(
            "SELECT EXISTS (SELECT 1 FROM nfl_historical.nfl_player_stats_all "
            "WHERE year = ? LIMIT 1)",
            [int(year)],
        ).fetchone()[0]
    except Exception:
        return False
    finally:
        if cache is not None:
            cache.close()


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
    if os.environ.get("OPS_CACHE_LIVE_ACTIVE_YEAR") == f"{int(year)}|{base}":
        return base
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


def yahoo_source_manifest_complete(
    *, refresh_weeks: list[int], fetch_rows: dict[str, Any], plan=None, year: int | None = None,
) -> bool:
    """Keep the observed Yahoo source pending until all scored games/results are admitted."""
    from multi_league.core.league_update_plan import active_publication_covers_plan

    return (
        active_publication_covers_plan(plan, year=year, weeks=refresh_weeks)
        and fetch_rows.get("draft_validated") is True
        and
        "pending_nfl_teams" in fetch_rows
        and "final_matchup_weeks" in fetch_rows
        and not fetch_rows["pending_nfl_teams"]
        and int(fetch_rows["final_matchup_weeks"] or 0) == len(refresh_weeks)
    )


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
    from multi_league.core.league_update_validation import (
        validate_active_roster_frame,
        validate_provider_team_inventory,
        validate_yahoo_week_matchup_scope,
    )
    from multi_league.core.league_refresh import (
        assert_provider_roster_merge,
        assert_authoritative_draft_refetch,
        filter_matchups_to_final_results,
        filter_rosters_to_finalized_games,
        merge_provider_refresh_table,
        missing_provider_draft_keys,
        needs_active_season_draft_fetch,
        pending_provider_nfl_teams,
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
    expected_team_keys = validate_provider_team_inventory(
        provider="yahoo",
        settings_team_count=settings_row.iloc[0]["num_teams"],
        team_ids=tuple(rosters.attrs.get("expected_team_keys") or ()),
    )
    rosters = normalize_yahoo_roster_provider_identity(rosters)
    provider_roster_team_weeks = validate_active_roster_frame(
        provider="yahoo",
        season=year,
        expected_team_ids=expected_team_keys,
        requested_weeks=tuple(int(week) for week in refresh_weeks),
        player_id_column="yahoo_player_id",
        rosters=rosters,
    )
    roster_rows = 0
    pending_nfl_teams: set[str] = set()
    for week in refresh_weeks:
        source = rosters[rosters["week"].astype(int) == int(week)].copy()
        ops_slice = finalized_ops[finalized_ops["week"].astype(int) == int(week)]
        pending_nfl_teams.update(pending_provider_nfl_teams(source, ops_slice))
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
    final_matchup_weeks = 0
    schedule_rows = 0
    current_schedule_frames: list[pd.DataFrame] = []
    playoff_start_week = settings_row.iloc[0].get("playoff_start_week")
    if pd.isna(playoff_start_week):
        playoff_start_week = None
    for week in refresh_weeks:
        scoreboards, failures = weekly_matchup_data(ctx=ctx, year=year, week=week)
        if failures:
            raise RuntimeError(f"Yahoo matchup fetch failed for {year} week {week}: {failures}")
        raw_schedule = scoreboards.attrs.get("schedule_df")
        final_matchups = filter_matchups_to_final_results(scoreboards)
        week_final = validate_yahoo_week_matchup_scope(
            raw_schedule=raw_schedule,
            final_matchups=final_matchups,
            season=year,
            week=week,
            expected_team_keys=expected_team_keys,
            playoff_start_week=playoff_start_week,
        )
        if week_final:
            final_matchup_weeks += 1
        if not final_matchups.empty:
            merge_provider_refresh_table(
                local_db,
                "matchup",
                final_matchups,
                platform="yahoo",
                league_id=league_key,
            )
            matchup_rows += len(final_matchups)

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
    # One bulk pick manifest is the independent witness for both a hydrated
    # draft and the first weekly publication of a newly renewed league.
    identity_rows = _fetch_yahoo_draft_identities(oauth=oauth, league_key=league_key, year=year)
    missing_provider_keys: set[tuple[str, ...]] = set()
    provider_manifest_matches = True
    fetched_authoritative_draft = False
    if has_hydrated_draft:
        # ``draftresults/players`` is a single Yahoo request.  It provides the
        # immutable pick manifest without the 12+ roster calls in the complete
        # draft fetch, so every weekly run can prove its retained draft is whole.
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
        if fetched_authoritative_draft:
            assert_authoritative_draft_refetch(
                draft,
                identity_rows,
                key_columns=_YAHOO_DRAFT_IDENTITY_KEYS,
                confirmed_no_draft=(
                    str((raw_settings.get("metadata") or {}).get("draft_status") or "").lower()
                    == "predraft"
                ),
            )
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
        "provider_roster_team_weeks": provider_roster_team_weeks,
        "pending_nfl_teams": sorted(pending_nfl_teams),
        "final_matchup_weeks": final_matchup_weeks,
        "roster_rows": int(roster_rows),
        "final_matchup_rows": int(matchup_rows),
        "schedule_rows": int(schedule_rows),
        "transaction_rows": int(len(transactions) if transactions is not None else 0),
        "draft_rows": draft_rows,
        "draft_validated": True,
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


def _attach_ops_cache_for_enrichment(local_db: Any) -> None:
    """Attach the patched local OPS cache before any shared SQL enrichment.

    The active-season worker patches the small restored cache from live Fly
    rows.  Rank enrichment is the first consumer of that table, so delaying
    the attachment until aggregate generation leaves new players with null
    generic ranks even when their live OPS ranks are present.
    """
    from multi_league.core.db_utils import attach_ops_cache

    conn = local_db.connect()
    attached = {
        str(row[1])
        for row in conn.execute("PRAGMA database_list").fetchall()
        if len(row) > 1
    }
    if "___ops" in attached:
        return

    ops_cache = Path(os.environ.get("OPS_CACHE_PATH", ""))
    if not ops_cache.is_file():
        raise RuntimeError("weekly enrichment requires the local OPS cache")
    attach_ops_cache(conn, str(ops_cache))


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
    """Build only the local standings payload not owned by atomic Fly rollups.

    SQL enrichment already builds the active matchup-derived tables.  V3 Fly
    publication rebuilds season, career, and homepage aggregates from the
    complete persisted chain after merging source facts.  Rebuilding those in
    scratch first was duplicate work and those copies were never authoritative.
    """
    if not has_finalized_matchups:
        return
    from multi_league.transformations.aggregation.aggregate_standings import aggregate_standings

    aggregate_standings(local_db.connect(), db_name, [int(active_year)])


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
    historical_source_rows: dict[str, pd.DataFrame] | None = None,
    frontend_configuration_rows: dict[str, pd.DataFrame] | None = None,
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
        preserve_frontend_settings=True,
    )
    failed = [name for name, ok in transforms if not ok]
    if failed:
        raise RuntimeError("shared transformation pipeline failed: " + ", ".join(failed))

    local_db.connect()
    _attach_ops_cache_for_enrichment(local_db)
    from multi_league.core.league_refresh import resolve_active_player_nfl_ids_from_bio

    resolved_active_ids = resolve_active_player_nfl_ids_from_bio(
        local_db.connect(),
        db_name=db_name,
        active_year=active_year,
        platform=platform,
    )
    if resolved_active_ids:
        print(
            f"[player_bio] Resolved {resolved_active_ids} active-season provider IDs after cache sync",
            flush=True,
        )
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
        # rows. Restore only provider rows that are still live: restoring a
        # finalized week would append its provider manager_week alongside the
        # canonical franchise-derived identity on a later retry.
        if not local_db._conn:
            local_db.connect()

        provider_schedule = _unresolved_provider_schedule_rows(
            provider_schedule,
            local_db=local_db,
            db_name=db_name,
            active_year=active_year,
        )
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
    _restore_historical_source_rows(local_db, historical_source_rows or {})
    _restore_frontend_configuration_rows(
        local_db,
        frontend_configuration_rows or {},
        db_name=db_name,
    )
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


def _unresolved_provider_schedule_rows(
    provider_schedule: pd.DataFrame,
    *,
    local_db: Any,
    db_name: str,
    active_year: int,
) -> pd.DataFrame:
    """Return provider schedule rows only for weeks not derived from final matchups."""
    if provider_schedule is None or provider_schedule.empty or "week" not in provider_schedule:
        return provider_schedule.copy() if isinstance(provider_schedule, pd.DataFrame) else pd.DataFrame()
    rows = local_db.connect().execute(
        """
        SELECT DISTINCT week
        FROM public.matchup
        WHERE db_name = ? AND year = ?
          AND team_points IS NOT NULL AND opponent_points IS NOT NULL
          AND COALESCE(is_bye_week, FALSE) = FALSE
        """,
        [db_name, int(active_year)],
    ).fetchall()
    finalized_weeks = {int(row[0]) for row in rows if row and row[0] is not None}
    weeks = pd.to_numeric(provider_schedule["week"], errors="coerce")
    return provider_schedule.loc[~weeks.isin(finalized_weeks)].copy()


def _publish_generation(reader: Any, db_name: str) -> int:
    value = reader.query_scalar(
        "SELECT COALESCE(MAX(generation), 0) FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {_sql_literal(db_name)}",
        database=LEAGUES_DATABASE,
    )
    return int(value or 0)


def _capture_update_source_frames(
    reader: Any,
    *,
    db_name: str,
    active_year: int,
    tables: tuple[str, ...],
) -> tuple[dict[str, pd.DataFrame], int]:
    """Bind the bounded league source frames to one publish generation.

    Fly reads are independent requests. A monotonic generation check on both
    sides of hydration detects any commit that landed between those requests;
    a later commit is rejected by the server when this base generation merges.
    """
    base_generation = _publish_generation(reader, db_name)
    frames = _source_frames(
        reader,
        db_name=db_name,
        active_year=active_year,
        tables=tables,
    )
    if _publish_generation(reader, db_name) != base_generation:
        raise RuntimeError(f"{db_name} changed during source snapshot; retry the update")
    return frames, base_generation


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
    from multi_league.core.fleet_publish import FLEET_HOMEPAGE_SCHEMA_VERSION, build_fleet_partition_bundle
    from multi_league.core.league_refresh import (
        active_refresh_publish_tables,
        background_refresh_call,
        completed_weeks_to_refresh,
        finalized_source_boundary,
        hydrate_local_refresh_sources,
        run_independent_refresh_preflight,
        start_background_refresh_call,
        stage_refresh_partitions,
    )
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.league_update_plan import active_provider_league_id, load_persisted_refresh_plan
    from multi_league.core.league_update_timing import PhaseTimer
    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.core.targets.fly_target import FlyTarget
    from scripts.league_update_workflow_receipt import record_publication_commit, write_refresh_receipt
    from multi_league.core.yahoo_league_settings import discover_league_history

    timer = PhaseTimer()
    reader = FlyReader()
    from multi_league.core.fly_writer import FlyWriter
    from multi_league.core.league_update_status import start_league_update_execution
    active_year = args.year
    if active_year is None:
        active_year = int(
            reader.query_scalar("SELECT MAX(year) FROM nfl_historical.nfl_player_stats_all", database=OPS_DATABASE)
        )
    from multi_league.core.league_update_lineage import assert_canonical_history_complete

    preflight = run_independent_refresh_preflight({
        "entitlement": lambda: start_league_update_execution(
            reader,
            FlyWriter(),
            database_name=args.db,
            platform="yahoo",
            dispatch_token=os.environ.get("LEAGUE_UPDATE_TOKEN"),
            attempt_id=os.environ.get("LEAGUE_UPDATE_ATTEMPT_ID"),
            claim_version=int(os.environ.get("LEAGUE_UPDATE_CLAIM_VERSION") or 1),
            workflow_run_id=os.environ.get("GITHUB_RUN_ID"),
        ) if args.execute else None,
        "canonical_history": lambda: assert_canonical_history_complete(
            reader, database_name=args.db, active_season=active_year
        ),
        "active_inputs": lambda: _load_active_refresh_inputs(
            reader,
            db_name=args.db,
            year=active_year,
            through_week=args.through_week,
        ),
        "persisted_plan": lambda: load_persisted_refresh_plan(
            reader,
            database_name=args.db,
            active_season=active_year,
            expected_observed_digest=args.observed_manifest_digest,
        ),
    })
    canonical_history = preflight["canonical_history"]
    finalized_ops, last_materialized_week = preflight["active_inputs"]
    if finalized_ops.empty:
        raise RuntimeError(f"No finalized regular-season ops facts for {active_year}")
    finalized_weeks = sorted({int(value) for value in finalized_ops["week"].dropna().tolist()})
    persisted_plan = preflight["persisted_plan"]
    if args.execute and persisted_plan is None:
        raise RuntimeError("executing update requires a captured source manifest")
    captured_league_id = active_provider_league_id(persisted_plan, provider="yahoo")
    refresh_weeks = (
        list(persisted_plan.weeks)
        if persisted_plan is not None
        else completed_weeks_to_refresh(
            finalized_weeks=finalized_weeks,
            last_materialized_week=last_materialized_week,
        )
    )
    receipt: dict[str, Any] = {
        "db_name": args.db,
        "year": active_year,
        "refresh_weeks": refresh_weeks,
        "executed": bool(args.execute),
        "canonical_history": canonical_history,
    }
    receipt.update(finalized_source_boundary(finalized_ops, year=active_year))
    timer.mark("source_plan")
    if persisted_plan is not None:
        receipt["source_manifest_digest"] = persisted_plan.observed_manifest_digest
        receipt["source_manifest_json"] = persisted_plan.observed_manifest_json
        receipt["published_manifest_digest"] = persisted_plan.published_manifest_digest
        receipt["refresh_reasons"] = list(persisted_plan.reasons)
    if not refresh_weeks:
        receipt["status"] = "NO_FINALIZED_WEEKS"
        receipt["phase_seconds"] = timer.finish()
        print(json.dumps(receipt, sort_keys=True))
        if args.json_out:
            args.json_out.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
        return 0

    with tempfile.TemporaryDirectory(prefix=f"{args.db}_weekly_refresh_") as temp_dir:
        work_dir = Path(temp_dir)
        source_frames, base_generation = _capture_update_source_frames(
            reader,
            db_name=args.db,
            active_year=active_year,
            tables=UPDATE_REFRESH_SOURCE_TABLES,
        )
        timer.mark("source_snapshot")
        receipt["base_generation"] = base_generation
        if source_frames["league_context"].empty or source_frames["league_settings"].empty:
            raise RuntimeError(f"Fly has no reusable context/settings for {args.db}")
        active_segment = _active_update_segment_from_source_frames(
            source_frames,
            db_name=args.db,
            active_year=active_year,
            expected_platform="yahoo",
        )
        receipt["active_segment"] = {
            "platform": active_segment.platform,
            "current_league_id": active_segment.current_league_id,
            "historical_platforms": list(active_segment.historical_platforms),
        }
        from multi_league.core.league_update_ownership import source_preservation_snapshot

        preservation_witnesses = source_preservation_snapshot(source_frames)
        transform_source_frames, historical_source_rows = _split_active_transform_source_frames(
            source_frames, active_year=active_year,
        )
        retained_active_key = active_segment.current_league_id or _active_yahoo_key_from_source_frames(
            source_frames,
            active_year=active_year,
        )
        if captured_league_id and retained_active_key and captured_league_id != retained_active_key:
            raise RuntimeError("captured manifest and Fly have conflicting active Yahoo league keys")
        source_active_key = captured_league_id or retained_active_key
        frontend_settings = _frontend_settings_from_source_context(
            source_frames["league_context"],
            db_name=args.db,
        )
        from multi_league.core.league_update_lineage import merge_provider_chain_ids

        # league_settings is the canonical imported provider timeline. Carry
        # every persisted Yahoo season into the quick context so refreshes do
        # not collapse a complete renewal chain to the context's active key.
        frontend_settings["league_ids"] = merge_provider_chain_ids(
            frontend_settings.get("league_ids"), active_segment,
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
        timer.mark("renewal_resolution")
        local_db = LocalLeagueDB(work_dir, args.db)
        try:
            receipt["hydrated_rows"] = hydrate_local_refresh_sources(
                local_db,
                transform_source_frames,
                db_name=args.db,
                active_year=active_year,
                expected_platform="yahoo",
            )
            # The local transform input intentionally contains only the active
            # season.  Compare the finished rebuild to the full Fly snapshot,
            # otherwise correctly restored historical source rows appear new.
            preservation_before = preservation_witnesses
            timer.mark("local_hydration")
            active_scoring = _active_year_scoring_info(
                local_db,
                db_name=args.db,
                year=active_year,
            )
            ops_context = background_refresh_call(
                lambda: _ensure_active_year_ops_cache(
                    reader,
                    year=active_year,
                    work_dir=work_dir,
                    scoring_info=active_scoring,
                )
            ) if args.execute else nullcontext(None)
            with ops_context as ops_future:
                receipt["fetch_rows"] = _merge_refresh_payloads(
                    ctx=ctx,
                    local_db=local_db,
                    oauth=oauth,
                    year=active_year,
                    refresh_weeks=refresh_weeks,
                    finalized_ops=finalized_ops,
                )
                receipt["source_manifest_complete"] = yahoo_source_manifest_complete(
                    refresh_weeks=refresh_weeks, fetch_rows=receipt["fetch_rows"],
                    plan=persisted_plan, year=active_year,
                )
                from multi_league.core.league_update_plan import active_publication_covers_plan
                receipt["source_manifest_scope_complete"] = (
                    active_publication_covers_plan(
                        persisted_plan, year=active_year, weeks=refresh_weeks,
                    )
                    and receipt["fetch_rows"].get("draft_validated") is True
                )
                timer.mark("provider_fetch")
                if not args.execute:
                    receipt["status"] = "DRY_RUN_READY"
                    receipt["phase_seconds"] = timer.finish()
                    print(json.dumps(receipt, sort_keys=True))
                    if args.json_out:
                        args.json_out.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")
                    return 0
                ops_future.result()
            timer.mark("player_ops_seed")

            from multi_league.core.league_update_validation import (
                assert_transformed_active_matchup_scope,
                assert_transformed_active_player_scope,
                capture_active_final_matchup_scope,
                capture_active_provider_player_scope,
            )
            expected_player_keys = capture_active_provider_player_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="yahoo_player_id",
            )
            expected_matchup_scores = capture_active_final_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year, weeks=refresh_weeks,
            )

            from multi_league.core.league_refresh import (
                active_nfl_player_ids,
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
                nfl_player_ids=active_nfl_player_ids(active_connection),
                provider_name_hints=active_platform_player_name_hints(active_connection, platform="yahoo"),
            )
            timer.mark("player_bio_sync")
            receipt["ops_cache"] = str(
                _ensure_ops_cache_matches_live(
                    reader,
                    finalized_ops,
                    year=active_year,
                    weeks=refresh_weeks,
                    work_dir=work_dir,
                )
            )
            timer.mark("player_ops_cache")
            _run_local_pipeline(
                ctx=ctx,
                context_path=context_path,
                local_db=local_db,
                db_name=args.db,
                active_year=active_year,
                work_dir=work_dir,
                keeper_config_hydrated="keeper_config" in transform_source_frames,
                historical_source_rows=historical_source_rows,
                frontend_configuration_rows=preservation_witnesses,
            )
            timer.mark("shared_transformations")
            from multi_league.core.league_update_ownership import restore_active_derived_source_values

            receipt["restored_active_derived_values"] = restore_active_derived_source_values(
                local_db,
                transform_source_frames,
                active_year=active_year,
            )
            timer.mark("restore_active_derived_values")
            receipt["transformed_player_scope"] = assert_transformed_active_player_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="yahoo_player_id",
                expected_keys=expected_player_keys,
            )
            receipt["transformed_matchup_scope"] = assert_transformed_active_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, expected_scores=expected_matchup_scores,
            )
            timer.mark("transformed_scope_validation")
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
            from multi_league.core.league_update_publish_claim import renew_claim_for_publication

            claim_future = start_background_refresh_call(
                lambda: renew_claim_for_publication(reader, database_name=args.db, platform="yahoo")
            )
            stage_timer = PhaseTimer()
            from multi_league.core.league_update_ownership import (
                assert_refresh_preservation,
                local_preservation_snapshot,
            )
            from multi_league.core.league_refresh import finalized_ops_player_weeks

            preservation_after = local_preservation_snapshot(local_db, preservation_before)
            stage_timer.mark("preservation_snapshot")
            receipt["preservation"] = assert_refresh_preservation(
                preservation_before,
                preservation_after,
                active_year=active_year,
                finalized_ops_player_weeks=finalized_ops_player_weeks(finalized_ops, year=active_year),
            )
            stage_timer.mark("preservation_validation")
            publish_tables = active_refresh_publish_tables(
                local_db.connect(), publication_schema_version=FLEET_HOMEPAGE_SCHEMA_VERSION,
            )
            if receipt["renewal_chain_backfilled"]:
                publish_tables.append("league_context")
            publish_tables = sorted(set(publish_tables))
            from multi_league.core.league_update_validation import assert_refresh_derived_output_health

            receipt["derived_health"] = assert_refresh_derived_output_health(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="yahoo_player_id",
                published_tables=publish_tables,
                publication_schema_version=FLEET_HOMEPAGE_SCHEMA_VERSION,
            )
            from multi_league.core.league_update_ownership import assert_publish_table_ownership

            receipt["ownership"] = assert_publish_table_ownership(publish_tables)
            stage_timer.mark("derived_output_validation")
            stage = stage_refresh_partitions(
                local_db.connect(),
                db_name=args.db,
                active_year=active_year,
                tables=publish_tables,
            )
            stage_timer.mark("stage_partitions")
            try:
                if not publish_tables:
                    raise RuntimeError("refresh pipeline produced no active-season publish tables")
                bundle = build_fleet_partition_bundle(
                    stage,
                    active_year=active_year,
                    league_generations={args.db: base_generation},
                    tables=publish_tables,
                    output_dir=work_dir / "bundle",
                    rebuild_career_rollups=True,
                    rebuild_homepage_rollups=True,
                )
            finally:
                stage.close()
            stage_timer.mark("bundle_build")
            receipt["homepage_preservation_stage_seconds"] = stage_timer.finish()
            timer.mark("homepage_preservation_stage")
            claim_future.result()
            timer.mark("prepublish_claim")
            result = FlyTarget().merge_fleet_partition(
                bundle.path,
                bundle_id=bundle.bundle_id,
                bundle_hash=bundle.bundle_hash,
                merge_timeout_seconds=40,
            )
            record_publication_commit(
                receipt, result=result, bundle_id=bundle.bundle_id, path=args.json_out,
            )
            receipt["homepage_rows"] = result.get("homepage_rollups", {}).get(args.db, {})
            receipt["homepage_seconds"] = result.get("homepage_seconds", {}).get(args.db)
            receipt["season_rollups"] = result.get("season_rollups", {}).get(args.db, {})
            receipt["season_seconds"] = result.get("season_seconds", {}).get(args.db)
            receipt["career_rollups"] = result.get("career_rollups", {}).get(args.db, {})
            receipt["career_seconds"] = result.get("career_seconds", {}).get(args.db)
            receipt["published_tables"] = sorted(
                set(publish_tables)
                | set(receipt["season_rollups"])
                | set(receipt["career_rollups"])
                | set(receipt["homepage_rows"])
            )
            timer.mark("fly_publication")
            receipt["post_publish_counts"] = _scope_counts(
                reader,
                db_name=args.db,
                active_year=active_year,
                tables=receipt["published_tables"],
            )
            timer.mark("post_publish_verification")
        finally:
            local_db.close()

    receipt["phase_seconds"] = timer.finish()
    write_refresh_receipt(receipt, args.json_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
