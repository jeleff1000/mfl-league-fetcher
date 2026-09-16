"""Small, provider-neutral boundaries for incremental league refreshes.

The refresh worker fetches the current active-season provider payload, but it
must only materialize player rows for NFL games whose statistics are finalized
in ``___ops``.  Yahoo roster calls otherwise expose every player in a live
week with a zero placeholder score.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
import hashlib
from pathlib import Path
import re
from typing import Any

import duckdb
import pandas as pd


class RefreshScopeError(RuntimeError):
    """The refresh input cannot prove a safe finalized-game boundary."""


def finalized_source_boundary(finalized_ops: pd.DataFrame, *, year: int) -> dict[str, Any]:
    """Return a stable finalized-stat receipt, including partial-week changes."""
    required = {"week", "nfl_team", "opponent_nfl_team", "NFL_player_id", "source_revision"}
    missing = sorted(required - set(finalized_ops.columns))
    if missing:
        raise RefreshScopeError("finalized ops payload is missing required columns: " + ", ".join(missing))
    games: set[tuple[int, str, str]] = set()
    revisions: list[str] = []
    for row in finalized_ops.loc[:, sorted(required)].itertuples(index=False):
        try:
            week = int(row.week)
        except (TypeError, ValueError):
            continue
        team = _team_code(row.nfl_team)
        opponent = _team_code(row.opponent_nfl_team)
        if not team or not opponent:
            continue
        low, high = sorted((team, opponent))
        games.add((week, low, high))
        player_id = str(row.NFL_player_id or "").strip()
        revision = str(row.source_revision or "").strip()
        if player_id and revision:
            revisions.append(f"{week}:{low}@{high}:{player_id}:{revision}")
    if not games or not revisions:
        return {
            "source_year": int(year),
            "source_week": None,
            "source_game_count": 0,
            "source_fingerprint": None,
        }
    digest = hashlib.sha256(",".join(sorted(revisions)).encode("utf-8")).hexdigest()
    source_week = max(week for week, _team, _opponent in games)
    return {
        "source_year": int(year),
        "source_week": source_week,
        "source_game_count": len(games),
        "source_fingerprint": f"{int(year)}:{source_week}:{digest}",
    }


def finalized_ops_player_weeks(finalized_ops: pd.DataFrame, *, year: int) -> set[str]:
    """Return player-week identities backed by an authoritative OPS stat row."""
    required = {"NFL_player_id", "week"}
    missing = sorted(required - set(finalized_ops.columns))
    if missing:
        raise RefreshScopeError("finalized ops payload is missing player-week columns: " + ", ".join(missing))
    player_weeks: set[str] = set()
    for player_id, week in finalized_ops.loc[:, ["NFL_player_id", "week"]].itertuples(index=False, name=None):
        raw_player_id = str(player_id or "").strip()
        try:
            week_number = int(week)
        except (TypeError, ValueError):
            continue
        if raw_player_id and week_number > 0:
            player_weeks.add(f"{raw_player_id}_{int(year)}_{week_number}")
    return player_weeks


# ``___ops`` and fantasy providers use different abbreviations for a few
# franchises. Compare them through one canonical team-code map.
_TEAM_CODE_ALIASES = {
    "GNB": "GB",
    "KAN": "KC",
    "LAR": "LA",
    "NOR": "NO",
    "NWE": "NE",
    "SFO": "SF",
    "TAM": "TB",
    "WSH": "WAS",
}

_ACTIVE_PLATFORM_ID_SPECS = {
    "yahoo": (("yahoo_player_id",), "yahoo_player_id", True),
    "sleeper": (("sleeper_player_id", "sleeper_player_id_original"), "sleeper_player_id", True),
    "espn": (("espn_player_id", "espn_player_id_original"), "espn_id", False),
}


def _qident(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def active_platform_player_ids(conn: duckdb.DuckDBPyConnection, *, platform: str) -> set[str]:
    """Return the active local provider IDs that need current bio mappings.

    The active local database has already been hydrated and had its provider
    payload merged. Looking only at its current-season player, draft, and
    transaction rows keeps the subsequent Fly lookup tiny and avoids a global
    ``player_bio`` cache refresh on every weekly button press.
    """
    normalized_platform = str(platform).strip().lower()
    if normalized_platform not in _ACTIVE_PLATFORM_ID_SPECS:
        raise RefreshScopeError(f"unsupported player-bio sync platform: {platform!r}")
    local_columns, _bio_column, numeric_ids = _ACTIVE_PLATFORM_ID_SPECS[normalized_platform]
    ids: set[str] = set()
    for table_name in ("player_fantasy", "draft", "transactions"):
        rows = conn.execute(
            """
            SELECT table_schema, column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchall()
        by_column = {str(column): str(schema) for schema, column in rows}
        for column_name in local_columns:
            schema = by_column.get(column_name)
            if schema is None:
                continue
            values = conn.execute(
                f"SELECT DISTINCT {_qident(column_name)} FROM {_qident(schema)}.{_qident(table_name)} "
                f"WHERE {_qident(column_name)} IS NOT NULL"
            ).fetchall()
            for (value,) in values:
                raw = str(value).strip()
                if not raw:
                    continue
                if numeric_ids:
                    try:
                        parsed = float(raw)
                    except ValueError:
                        continue
                    if not parsed.is_integer() or parsed < 0:
                        continue
                    raw = str(int(parsed))
                ids.add(raw)
    return ids


def active_platform_player_names(conn: duckdb.DuckDBPyConnection, *, platform: str) -> set[str]:
    """Return current provider player names as bounded player-bio lookup hints.

    New-season providers can expose a player before its platform-specific ID
    has been promoted to ``player_bio``.  Exact normalized names let the
    active worker hydrate that already-authoritative bio row without scanning
    the entire player directory or guessing through fuzzy matching.
    """
    normalized_platform = str(platform).strip().lower()
    if normalized_platform not in _ACTIVE_PLATFORM_ID_SPECS:
        raise RefreshScopeError(f"unsupported player-bio sync platform: {platform!r}")

    names: set[str] = set()
    for table_name in ("player_fantasy", "draft", "transactions"):
        rows = conn.execute(
            """
            SELECT table_schema, column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchall()
        schemas = {str(schema) for schema, column in rows if str(column) == "player"}
        for schema in schemas:
            values = conn.execute(
                f"SELECT DISTINCT {_qident('player')} FROM {_qident(schema)}.{_qident(table_name)} "
                f"WHERE {_qident('player')} IS NOT NULL"
            ).fetchall()
            for (value,) in values:
                name = str(value).strip()
                if name and name.lower() not in {"unknown", "n/a"}:
                    names.add(name)
    return names


def active_nfl_player_ids(conn: duckdb.DuckDBPyConnection) -> set[str]:
    """Return bounded, already-resolved active NFL identities from local source rows."""
    ids: set[str] = set()
    for table_name in ("player_fantasy", "draft", "transactions"):
        rows = conn.execute(
            """
            SELECT table_schema, column_name
            FROM information_schema.columns
            WHERE table_name = ? AND column_name = 'NFL_player_id'
            """,
            [table_name],
        ).fetchall()
        for schema, _column in rows:
            values = conn.execute(
                f"SELECT DISTINCT NFL_player_id FROM {_qident(schema)}.{_qident(table_name)} "
                "WHERE NFL_player_id IS NOT NULL"
            ).fetchall()
            ids.update(str(value).strip() for (value,) in values if str(value).strip())
    return ids


def active_platform_player_name_hints(
    conn: duckdb.DuckDBPyConnection,
    *,
    platform: str,
) -> dict[str, set[str]]:
    """Return active provider-ID to player-name pairs for bounded fallback lookups."""
    normalized_platform = str(platform).strip().lower()
    if normalized_platform not in _ACTIVE_PLATFORM_ID_SPECS:
        raise RefreshScopeError(f"unsupported player-bio sync platform: {platform!r}")
    local_columns, _bio_column, _numeric_ids = _ACTIVE_PLATFORM_ID_SPECS[normalized_platform]
    hints: dict[str, set[str]] = {}
    for table_name in ("player_fantasy", "draft", "transactions"):
        rows = conn.execute(
            """
            SELECT table_schema, column_name
            FROM information_schema.columns
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchall()
        columns_by_schema: dict[str, set[str]] = {}
        for schema, column in rows:
            columns_by_schema.setdefault(str(schema), set()).add(str(column))
        for schema, columns in columns_by_schema.items():
            if "player" not in columns:
                continue
            for column_name in local_columns:
                if column_name not in columns:
                    continue
                values = conn.execute(
                    f"SELECT DISTINCT {_qident(column_name)}, {_qident('player')} "
                    f"FROM {_qident(schema)}.{_qident(table_name)} "
                    f"WHERE {_qident(column_name)} IS NOT NULL AND {_qident('player')} IS NOT NULL"
                ).fetchall()
                for provider_id, player_name in values:
                    provider = str(provider_id).strip()
                    player = str(player_name).strip()
                    if provider and player and player.lower() not in {"unknown", "n/a"}:
                        hints.setdefault(provider, set()).add(player)
    return hints


def sync_player_bio_cache_from_fly(
    reader: Any,
    *,
    ops_cache: Path | str,
    platform: str,
    provider_ids: set[str],
    player_names: set[str] | None = None,
    nfl_player_ids: set[str] | None = None,
    provider_name_hints: dict[str, set[str]] | None = None,
) -> dict[str, int]:
    """Merge just fetched platform identities from Fly into the local ops cache.

    The durable Actions cache is intentionally static during an import. This
    bounded merge makes a newly promoted roster identity available immediately
    without replacing the complete 38k-row player-bio table or making direct
    writes to Fly.
    """
    normalized_platform = str(platform).strip().lower()
    if normalized_platform not in _ACTIVE_PLATFORM_ID_SPECS:
        raise RefreshScopeError(f"unsupported player-bio sync platform: {platform!r}")
    _local_columns, bio_column, numeric_ids = _ACTIVE_PLATFORM_ID_SPECS[normalized_platform]
    canonical_ids = sorted({str(value).strip() for value in provider_ids if str(value).strip()})
    if numeric_ids:
        normalized_ids: set[str] = set()
        for value in canonical_ids:
            try:
                parsed = float(value)
            except ValueError:
                continue
            if parsed.is_integer() and parsed >= 0:
                normalized_ids.add(str(int(parsed)))
        canonical_ids = sorted(normalized_ids)
    elif normalized_platform == "espn":
        # ESPN frame assembly can coerce provider IDs to floats (``4685526.0``)
        # while ``player_bio`` stores the canonical string (``4685526``).
        # Normalize whole values, including negative team/DST IDs, but retain
        # any non-numeric future provider ID for the existing exact lookup.
        normalized_ids = set()
        for value in canonical_ids:
            try:
                parsed = float(value)
            except ValueError:
                normalized_ids.add(value)
                continue
            normalized_ids.add(str(int(parsed)) if parsed.is_integer() else value)
        canonical_ids = sorted(normalized_ids)
    canonical_names = sorted(
        {
            str(value).strip().lower()
            for value in (player_names or set())
            if str(value).strip() and str(value).strip().lower() not in {"unknown", "n/a"}
        }
    )
    canonical_nfl_ids = sorted(
        {
            str(value).strip()
            for value in (nfl_player_ids or set())
            if str(value).strip()
        }
    )
    if not canonical_ids and not canonical_names and not canonical_nfl_ids:
        return {"provider_ids": 0, "player_bio_rows": 0}

    def normalize_hint_id(value: object) -> str:
        raw = str(value).strip()
        if not raw:
            return ""
        if numeric_ids:
            try:
                parsed = float(raw)
            except ValueError:
                return ""
            return str(int(parsed)) if parsed.is_integer() and parsed >= 0 else ""
        if normalized_platform == "espn":
            try:
                parsed = float(raw)
            except ValueError:
                return raw
            return str(int(parsed)) if parsed.is_integer() else raw
        return raw

    names_by_provider_id: dict[str, set[str]] = {}
    for raw_id, names in (provider_name_hints or {}).items():
        provider_id = normalize_hint_id(raw_id)
        if not provider_id or provider_id not in canonical_ids:
            continue
        normalized_names = {
            str(name).strip().lower()
            for name in names
            if str(name).strip() and str(name).strip().lower() not in {"unknown", "n/a"}
        }
        if normalized_names:
            names_by_provider_id.setdefault(provider_id, set()).update(normalized_names)

    cache_path = Path(ops_cache)
    if not cache_path.is_file():
        raise RefreshScopeError(f"OPS_CACHE_PATH is missing: {cache_path}")
    target = "nfl_historical.player_bio"

    # A restored release cache already contains almost every established
    # provider identity.  Check IDs and names independently: an active player
    # can lack a provider ID even while every other roster ID is cached.  In
    # that case its name is still required for the local SQL rank join.
    missing_ids = canonical_ids
    missing_names = canonical_names
    missing_nfl_ids = canonical_nfl_ids
    if canonical_ids or canonical_names or canonical_nfl_ids:
        cache = duckdb.connect(str(cache_path), read_only=True)
        try:
            cached_ids: set[str] = set()
            if canonical_ids and numeric_ids:
                numeric_id_sql = ", ".join(canonical_ids)
                cached_rows = cache.execute(
                    f"SELECT DISTINCT CAST(TRY_CAST({_qident(bio_column)} AS BIGINT) AS VARCHAR) "
                    f"FROM {target} WHERE TRY_CAST({_qident(bio_column)} AS BIGINT) "
                    f"IN ({numeric_id_sql})"
                ).fetchall()
                cached_ids = {str(value).strip() for (value,) in cached_rows if value is not None}
            elif canonical_ids:
                provider_id_sql = ", ".join(_sql_literal(value) for value in canonical_ids)
                cached_rows = cache.execute(
                    f"SELECT DISTINCT CAST({_qident(bio_column)} AS VARCHAR) FROM {target} "
                    f"WHERE CAST({_qident(bio_column)} AS VARCHAR) IN ({provider_id_sql})"
                ).fetchall()
                cached_ids = {str(value).strip() for (value,) in cached_rows if value is not None}
            cached_names: set[str] = set()
            if canonical_names:
                name_sql = ", ".join(_sql_literal(value) for value in canonical_names)
                cached_name_rows = cache.execute(
                    f"SELECT DISTINCT LOWER(TRIM(player)) FROM {target} "
                    f"WHERE LOWER(TRIM(player)) IN ({name_sql})"
                ).fetchall()
                cached_names = {str(value).strip().lower() for (value,) in cached_name_rows if value is not None}
            cached_nfl_ids: set[str] = set()
            if canonical_nfl_ids:
                nfl_id_sql = ", ".join(_sql_literal(value) for value in canonical_nfl_ids)
                cached_nfl_rows = cache.execute(
                    f"SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) FROM {target} "
                    f"WHERE CAST(NFL_player_id AS VARCHAR) IN ({nfl_id_sql})"
                ).fetchall()
                cached_nfl_ids = {str(value).strip() for (value,) in cached_nfl_rows if value is not None}
        finally:
            cache.close()
        missing_ids = sorted(set(canonical_ids) - cached_ids)
        missing_names = sorted(set(canonical_names) - cached_names)
        missing_nfl_ids = sorted(set(canonical_nfl_ids) - cached_nfl_ids)
        if not missing_ids and not missing_names and not missing_nfl_ids:
            return {"provider_ids": len(canonical_ids), "player_bio_rows": 0}

        # ESPN sometimes publishes a new numeric ID before the central bio
        # table is updated. When the active payload's exact name identifies one
        # and only one local row whose ESPN ID is still blank, the cache already
        # has the authoritative NFL identity needed by SQL enrichment. Fill
        # only that disposable local mapping; duplicate names or an existing
        # conflicting ESPN ID continue through the Fly fallback below.
        if normalized_platform == "espn" and names_by_provider_id:
            locally_resolved: set[str] = set()
            cache = duckdb.connect(str(cache_path))
            try:
                for provider_id in missing_ids:
                    names = names_by_provider_id.get(provider_id, set())
                    if len(names) != 1:
                        continue
                    (player_name,) = tuple(names)
                    matches = cache.execute(
                        f"SELECT NFL_player_id, {_qident(bio_column)} FROM {target} "
                        "WHERE LOWER(TRIM(player)) = ?",
                        [player_name],
                    ).fetchall()
                    if len(matches) != 1:
                        continue
                    nfl_player_id, existing_id = matches[0]
                    if existing_id is not None and str(existing_id).strip():
                        continue
                    cache.execute(
                        f"UPDATE {target} SET {_qident(bio_column)} = ? "
                        f"WHERE NFL_player_id = ? AND ({_qident(bio_column)} IS NULL "
                        f"OR TRIM(CAST({_qident(bio_column)} AS VARCHAR)) = '')",
                        [provider_id, nfl_player_id],
                    )
                    locally_resolved.add(provider_id)
            finally:
                cache.close()
            missing_ids = [provider_id for provider_id in missing_ids if provider_id not in locally_resolved]
            if not missing_ids:
                return {"provider_ids": len(canonical_ids), "player_bio_rows": 0}

        if names_by_provider_id and all(provider_id in names_by_provider_id for provider_id in missing_ids):
            missing_names = sorted(
                set(missing_names)
                | {
                    name
                    for provider_id in missing_ids
                    for name in names_by_provider_id[provider_id]
                }
            )

    canonical_names = missing_names

    predicates: list[str] = []
    if numeric_ids:
        if missing_ids:
            predicates.append(f"TRY_CAST({bio_column} AS BIGINT) IN ({', '.join(missing_ids)})")
    else:
        if missing_ids:
            predicates.append(f"{bio_column} IN ({', '.join(_sql_literal(value) for value in missing_ids)})")
    if canonical_names:
        predicates.append(f"LOWER(TRIM(player)) IN ({', '.join(_sql_literal(value) for value in canonical_names)})")
    if missing_nfl_ids:
        predicates.append(f"NFL_player_id IN ({', '.join(_sql_literal(value) for value in missing_nfl_ids)})")
    predicate = " OR ".join(f"({value})" for value in predicates)
    source = reader.query_df(
        f"SELECT * FROM nfl_historical.player_bio WHERE {predicate}",
        database="___ops",
    )
    if source.empty:
        return {"provider_ids": len(canonical_ids), "player_bio_rows": 0}
    if "NFL_player_id" not in source.columns or source["NFL_player_id"].isna().any():
        raise RefreshScopeError("Fly player_bio sync returned a missing NFL_player_id")
    if source["NFL_player_id"].astype(str).duplicated().any():
        raise RefreshScopeError("Fly player_bio sync returned duplicate NFL_player_id values")

    cache = duckdb.connect(str(cache_path))
    try:
        cache_columns = [str(row[0]) for row in cache.execute(f"DESCRIBE {target}").fetchall()]
        missing_source_columns = [column for column in cache_columns if column not in source.columns]
        if missing_source_columns:
            source = source.assign(**{column: pd.NA for column in missing_source_columns})
        source = source.reindex(columns=cache_columns)
        cache.register("__active_player_bio", source)
        try:
            assignments = ", ".join(
                f"{_qident(column)} = COALESCE(incoming.{_qident(column)}, current.{_qident(column)})"
                for column in cache_columns
                if column != "NFL_player_id"
            )
            if assignments:
                cache.execute(
                    f"UPDATE {target} AS current SET {assignments} "
                    "FROM __active_player_bio AS incoming "
                    "WHERE current.NFL_player_id = incoming.NFL_player_id"
                )
            columns_sql = ", ".join(_qident(column) for column in cache_columns)
            cache.execute(
                f"INSERT INTO {target} ({columns_sql}) "
                f"SELECT {columns_sql} FROM __active_player_bio AS incoming "
                f"WHERE NOT EXISTS (SELECT 1 FROM {target} AS current "
                "WHERE current.NFL_player_id = incoming.NFL_player_id)"
            )
        finally:
            cache.unregister("__active_player_bio")
    finally:
        cache.close()
    return {"provider_ids": len(canonical_ids), "player_bio_rows": int(len(source))}


def _team_code(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    raw = str(value).strip().upper()
    return _TEAM_CODE_ALIASES.get(raw, raw)


def completed_weeks_to_refresh(
    *,
    finalized_weeks: Iterable[int | str],
    last_materialized_week: int | None,
) -> list[int]:
    """Return every finalized week that can still affect active-season data.

    The newest materialized week is deliberately included again. Provider
    corrections can arrive after an earlier refresh, so a safe idempotent
    worker overlaps that boundary instead of assuming a published week is
    immutable. Earlier seasons are never returned or republished.
    """
    weeks: set[int] = set()
    for value in finalized_weeks:
        try:
            week = int(value)
        except (TypeError, ValueError):
            continue
        if week > 0:
            weeks.add(week)

    if not weeks:
        return []

    try:
        watermark = int(last_materialized_week or 0)
    except (TypeError, ValueError):
        watermark = 0
    first_week = max(1, watermark)
    return [week for week in sorted(weeks) if week >= first_week]


def provider_weeks_to_fetch(*, max_week: int, requested_weeks: Iterable[int | str] | None) -> list[int]:
    """Return a bounded, de-duplicated provider fetch scope.

    An active refresh must ask a provider for the finalized weeks it is about
    to replace, never its normal full-season range.  ``None`` retains the
    legacy full-range behavior for a normal import.
    """
    if requested_weeks is None:
        return list(range(1, int(max_week) + 1))
    scoped: set[int] = set()
    for value in requested_weeks:
        try:
            week = int(value)
        except (TypeError, ValueError):
            continue
        if 1 <= week <= int(max_week):
            scoped.add(week)
    return sorted(scoped)


def filter_rosters_to_finalized_games(
    rosters: pd.DataFrame,
    finalized_ops: pd.DataFrame,
) -> pd.DataFrame:
    """Keep roster rows only when their NFL team played a finalized game.

    Each team plays one game in a given NFL week.  The caller must therefore
    call this once per ``(year, week)`` and supply the exact finalized ops
    slice for that same week.  Missing ops identity is an unsafe source state,
    not an empty completed slate.
    """
    required_ops = {"nfl_team", "opponent_nfl_team"}
    missing_ops = sorted(required_ops - set(finalized_ops.columns))
    if missing_ops:
        raise RefreshScopeError(
            "finalized ops payload is missing required columns: " + ", ".join(missing_ops)
        )
    if "nfl_team" not in rosters.columns:
        raise RefreshScopeError("roster payload is missing required column: nfl_team")

    if rosters.empty or finalized_ops.empty:
        return rosters.iloc[0:0].copy()

    ops_pairs = finalized_ops[["nfl_team", "opponent_nfl_team"]].copy()
    ops_pairs["nfl_team"] = ops_pairs["nfl_team"].map(_team_code)
    ops_pairs["opponent_nfl_team"] = ops_pairs["opponent_nfl_team"].map(_team_code)
    if (ops_pairs["nfl_team"] == "").any() or (ops_pairs["opponent_nfl_team"] == "").any():
        raise RefreshScopeError("finalized ops payload has a blank team identity")

    finalized_teams = set(ops_pairs["nfl_team"]) | set(ops_pairs["opponent_nfl_team"])
    roster_team_codes = rosters["nfl_team"].map(_team_code)
    return rosters.loc[roster_team_codes.isin(finalized_teams)].copy()


def pending_provider_nfl_teams(
    rosters: pd.DataFrame,
    finalized_ops: pd.DataFrame,
) -> tuple[str, ...]:
    """Identify scored provider rows excluded because the NFL game is not in ops.

    Zero-point bye and not-yet-played players are not evidence of a missing
    score. Once a player has nonzero points, however, the observed provider
    manifest cannot be treated as published until that game's NFL inputs have
    entered the canonical ops snapshot and the roster row can be admitted.
    """
    required_roster = {"nfl_team", "fantasy_points"}
    required_ops = {"nfl_team", "opponent_nfl_team"}
    if not required_roster.issubset(rosters):
        raise RefreshScopeError(
            "roster payload is missing required columns: "
            + ", ".join(sorted(required_roster - set(rosters)))
        )
    if not required_ops.issubset(finalized_ops):
        raise RefreshScopeError(
            "finalized ops payload is missing required columns: "
            + ", ".join(sorted(required_ops - set(finalized_ops)))
        )
    if rosters.empty:
        return ()
    ops_teams = set(finalized_ops["nfl_team"].map(_team_code)) | set(
        finalized_ops["opponent_nfl_team"].map(_team_code)
    )
    points = pd.to_numeric(rosters["fantasy_points"], errors="coerce")
    scored = rosters.loc[points.notna() & points.ne(0), "nfl_team"].map(_team_code)
    if scored.eq("").any():
        raise RefreshScopeError("scored provider roster row has no NFL team identity")
    return tuple(sorted(set(scored) - ops_teams))


def assert_provider_roster_merge(
    local_db: Any,
    incoming: pd.DataFrame,
    *,
    year: int,
    week: int,
    provider_id_column: str,
    stored_id_column: str | None = None,
) -> None:
    """Fail before enrichment if a roster merge silently collapses players.

    ``LocalLeagueDB.merge_table`` normalizes provider frames before applying
    its deduplication keys. A stale pre-normalization key can therefore shrink
    an entire league-week to one row without raising. Prove that every unique
    provider player ID admitted by the finalized-game boundary still exists in
    the canonical local table immediately after the merge.
    """

    if provider_id_column not in incoming.columns:
        raise RefreshScopeError(f"roster payload is missing provider ID column: {provider_id_column}")

    def canonical_id(value: object) -> str:
        if value is None or pd.isna(value):
            return ""
        raw = str(value).strip()
        match = re.fullmatch(r"(-?\d+)\.0+", raw)
        return match.group(1) if match else raw

    expected = {canonical_id(value) for value in incoming[provider_id_column].tolist()}
    expected.discard("")
    if not expected:
        raise RefreshScopeError(
            f"roster payload has no usable provider IDs in {provider_id_column} for {year} week {week}"
        )

    target_column = stored_id_column or provider_id_column
    rows = local_db.connect().execute(
        f"SELECT DISTINCT CAST({_qident(target_column)} AS VARCHAR) "
        "FROM public.player_fantasy "
        "WHERE db_name = ? AND year = ? AND week = ? "
        f"AND {_qident(target_column)} IS NOT NULL",
        [str(local_db.league_name), int(year), int(week)],
    ).fetchall()
    actual = {canonical_id(value) for (value,) in rows}
    actual.discard("")
    missing = expected - actual
    if missing:
        raise RefreshScopeError(
            f"roster merge preserved only {len(expected) - len(missing)}/{len(expected)} "
            f"provider player IDs for {year} week {week} ({target_column})"
        )


def filter_matchups_to_final_results(
    matchups: pd.DataFrame,
    *,
    winner_column: str = "winner_team_key",
) -> pd.DataFrame:
    """Retain scoreboards only after the provider has supplied a final outcome.

    Yahoo exposes live partial matchup scores as soon as the first NFL game
    ends.  The shared matchup transforms interpret any pair of scores as a
    completed win/loss, so those rows must wait until Yahoo marks the matchup
    final.  A rare tied matchup with no winner key is conservatively retried
    on the next refresh rather than publishing an invented result.
    """
    if winner_column not in matchups.columns:
        raise RefreshScopeError(f"matchup payload is missing required column: {winner_column}")
    if matchups.empty:
        return matchups.copy()
    outcomes = matchups[winner_column].fillna("").astype(str).str.strip()
    # ESPN exposes a partial matchup scoring period as ``UNDECIDED``.  It
    # cannot be allowed through merely because it is a non-empty string: the
    # shared transforms would turn its partial scores into wins and losses.
    return matchups.loc[outcomes.ne("") & outcomes.str.upper().ne("UNDECIDED")].copy()


def espn_schedule_is_final(
    schedule_rows: Iterable[dict[str, Any]],
    *,
    expected_team_ids: Iterable[str] | None = None,
) -> bool:
    """Whether ESPN has resolved every matchup in one scoring period.

    ESPN leaves every pair's ``winner`` as ``UNDECIDED`` while scores are
    live.  The absence of one row or outcome is equally unsafe: no matchup
    results may be materialized until the full period has final outcomes.
    """
    rows = list(schedule_rows)
    if not rows:
        return False
    final_outcomes = {"HOME", "AWAY", "TIE"}
    observed_teams: set[str] = set()
    for row in rows:
        home = row.get("home") or {}
        away = row.get("away")
        home_id = str(home.get("teamId") or "").strip() if isinstance(home, dict) else ""
        away_id = str(away.get("teamId") or "").strip() if isinstance(away, dict) else ""
        playoff_tier = str(row.get("playoffTierType") or "").strip().upper()
        declared_bye = (
            home_id != ""
            and away is None
            and playoff_tier.endswith(("_BRACKET", "_LADDER"))
        )
        outcome = str(row.get("winner") or "").strip().upper()
        if outcome not in final_outcomes and not (declared_bye and outcome == "UNDECIDED"):
            return False
        if expected_team_ids is not None:
            if not home_id or (not away_id and not declared_bye):
                return False
            if home_id in observed_teams or (away_id and away_id in observed_teams):
                return False
            observed_teams.add(home_id)
            if away_id:
                observed_teams.add(away_id)
    if expected_team_ids is not None:
        expected = {str(team_id).strip() for team_id in expected_team_ids}
        if not expected or "" in expected or observed_teams != expected:
            return False
    return True


def hydrate_local_refresh_sources(
    local_db: Any,
    source_frames: dict[str, pd.DataFrame],
    *,
    db_name: str,
    active_year: int | None = None,
    expected_platform: str | None = None,
) -> dict[str, int]:
    """Load the target league's existing canonical source state into local DuckDB.

    Aggregates such as career player value and homepage summaries are rebuilt
    from raw history.  The source frames therefore come from Fly before the
    active-season rows are replaced.  Rejecting a mixed ``db_name`` frame is
    deliberate defense-in-depth: the scoped Fly query and the local build
    must agree before a bundle can be created.
    """
    from multi_league.core.aggregate_ddl import AGGREGATE_TABLE_SPECS, ensure_aggregate_table

    hydrated: dict[str, int] = {}
    for table_name in sorted(source_frames):
        frame = source_frames[table_name]
        if "db_name" not in frame.columns:
            raise RefreshScopeError(f"source frame {table_name!r} is missing required column: db_name")

        db_values = {
            str(value).strip()
            for value in frame["db_name"].dropna().tolist()
            if str(value).strip()
        }
        unexpected = sorted(db_values - {db_name})
        if unexpected:
            raise RefreshScopeError(
                f"source frame {table_name!r} contains unexpected db_name values: {', '.join(unexpected)}"
            )

        if (
            active_year is not None
            and expected_platform
            and {"year", "platform"}.issubset(frame.columns)
            and not frame.empty
        ):
            years = pd.to_numeric(frame["year"], errors="coerce")
            platforms = frame["platform"].fillna("").astype(str).str.strip().str.lower()
            stale_active_rows = (years == int(active_year)) & platforms.ne("") & platforms.ne(
                str(expected_platform).strip().lower()
            )
            if stale_active_rows.any():
                conflicting = sorted(set(platforms.loc[stale_active_rows]))
                raise RefreshScopeError(
                    f"source frame {table_name!r} has overlapping active platform rows "
                    f"for {active_year}: {', '.join(conflicting)}"
                )

        local_db.ensure_table(table_name)
        if table_name in AGGREGATE_TABLE_SPECS:
            conn = local_db.connect()
            catalog = str(conn.execute("SELECT current_database()").fetchone()[0])
            ensure_aggregate_table(conn, catalog, table_name)
            canonical_columns = {
                str(name) for name, *_ in conn.execute(f"DESCRIBE public.{table_name}").fetchall()
            }
            unsupported = sorted(set(frame.columns) - canonical_columns)
            if unsupported:
                raise RefreshScopeError(
                    f"source aggregate {table_name!r} has unsupported Fly columns: "
                    + ", ".join(unsupported)
                )
        if not frame.empty:
            if local_db.table_exists(table_name):
                local_db._insert_into_table(table_name, frame)
            else:
                # Non-aggregate tables without registered local DDL retain
                # their bounded Fly column shape for downstream readers.
                local_db.save_table(table_name, frame)
        hydrated[table_name] = int(len(frame))
    return hydrated


def _bind_provider_db_name(
    local_db: Any, table_name: str, normalized: pd.DataFrame
) -> pd.DataFrame:
    """Give an unscoped provider payload its validated local league identity."""
    db_name = str(getattr(local_db, "league_name", "")).strip()
    if not db_name:
        raise RefreshScopeError(f"{table_name} local league identity is missing")
    if "db_name" not in normalized.columns:
        return normalized.assign(db_name=db_name)
    if normalized["db_name"].astype(str).ne(db_name).any():
        raise RefreshScopeError(f"{table_name} provider payload belongs to a different league")
    return normalized


def merge_provider_refresh_table(
    local_db: Any,
    table_name: str,
    incoming: pd.DataFrame,
    *,
    platform: str,
    league_id: str,
) -> None:
    """Merge a provider payload without granting it ownership of enrichments."""
    if incoming is None or incoming.empty:
        return
    from multi_league.core.league_update_ownership import (
        overlay_provider_columns,
        table_ownership,
    )
    from dataclasses import replace

    normalized = local_db._normalize_table_frame(
        table_name,
        incoming,
        platform=platform,
        league_id=league_id,
        log_context="provider refresh ownership",
    )
    if normalized is None or normalized.empty:
        raise RefreshScopeError(f"{table_name} provider payload normalized to no rows")
    if table_name == "league_settings" and "_raw" in normalized.columns:
        # flatten_settings retains its nested parse witness, but _raw is not
        # a publishable canonical column. Keep the strict ownership check for
        # every other unexpected field.
        normalized = normalized.drop(columns=["_raw"])
    normalized = _bind_provider_db_name(local_db, table_name, normalized)
    contract = table_ownership(table_name)
    if table_name == "player_fantasy":
        # The roster normalizer carries two fetch-only NFL mapping hints that
        # are not columns in canonical player_fantasy. Every other unknown
        # field still fails the ownership gate below.
        normalized = normalized.drop(
            columns=[column for column in ("eligible_positions", "nfl_team_api") if column in normalized]
        )
        provider_id = {
            "yahoo": "yahoo_player_id",
            "espn": "espn_player_id",
            "sleeper": "sleeper_player_id",
        }.get(str(platform).strip().lower())
        if provider_id is None:
            raise RefreshScopeError(f"unsupported roster provider identity: {platform}")
        provider_keys = ("db_name", "year", "week", "team_key", provider_id)
        missing = sorted(set(provider_keys) - set(normalized))
        if missing:
            raise RefreshScopeError(f"{table_name} provider payload is missing keys: {missing}")
        if normalized[list(provider_keys)].isna().any().any():
            raise RefreshScopeError(f"{table_name} provider payload has null ownership keys")
        contract = replace(contract, key_columns=provider_keys)
    elif table_name == "matchup":
        # ``manager_week`` is rebuilt from franchise identity during shared
        # enrichment.  It is not a stable provider key on a retry: using it
        # makes an identical second update append a duplicate team-week.
        # Every supported provider exposes a per-league ``team_key`` that is
        # stable for the active season, so replace provider rows by that key.
        provider_keys = ("db_name", "year", "week", "team_key")
        missing = sorted(set(provider_keys) - set(normalized))
        if missing:
            raise RefreshScopeError(f"{table_name} provider payload is missing keys: {missing}")
        if normalized[list(provider_keys)].isna().any().any():
            raise RefreshScopeError(f"{table_name} provider payload has null ownership keys")
        contract = replace(contract, key_columns=provider_keys)
    existing = (
        local_db.read_table(table_name)
        if local_db.table_exists(table_name)
        else pd.DataFrame(columns=list(contract.classified_columns))
    )
    if table_name == "player_fantasy" and not existing.empty:
        existing = existing.loc[existing[provider_id].notna()].copy()
    protected = overlay_provider_columns(existing, normalized, contract)
    local_db.merge_table(
        table_name,
        protected,
        list(contract.key_columns),
        platform=platform,
        league_id=league_id,
        already_normalized=True,
    )


def active_refresh_publish_tables(source: duckdb.DuckDBPyConnection) -> list[str]:
    """Return locally-built tables safe to replace in an active-season refresh.

    The normal quick importer builds a current-season DuckDB.  Its tables
    with an active-season cadence are safe to replace at ``(db_name, year)``;
    whole-league rollups (career, homepage, aliases, and settings) must stay
    out of a weekly bundle unless they were rebuilt from full history.
    """
    from multi_league.core.delta_publish import CADENCE_ACTIVE_SEASON, canonical_table_registry

    registry = canonical_table_registry()
    # Frontend-owned configuration is an enrichment input, not active-season
    # provider output.  Weekly workers hydrate keeper_config so a quick rebuild
    # respects the user's rules, but must never publish an empty/current-year
    # partition that could replace those rules.  Yahoo's renewal chain is added
    # explicitly by its worker only when a legacy context needs backfilling.
    excluded_config_tables = {"keeper_config", "league_context", "league_rules", "manager_overrides", "standings_config"}
    rebuilt_rollups = {
        "draft_manager_career",
        "draft_player_career",
        "franchise_identity_audit",
        "franchise_identity_registry",
        "homepage_current_standings",
        "homepage_league_summary",
        "homepage_manager_profiles",
        "homepage_manager_rankings",
        "homepage_top_rivalries",
        "matchup_career",
        "matchup_h2h_career",
        "player_fantasy_career",
        "player_fantasy_career_all",
        "transaction_manager_career",
        "transaction_player_career",
    }
    available = {
        str(row[0])
        for row in source.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """
        ).fetchall()
    }
    return sorted(
        table
        for table, spec in registry.items()
        if (
            table in available
            and table not in excluded_config_tables
            and (
                str(spec["cadence_class"]) == CADENCE_ACTIVE_SEASON
                or table in rebuilt_rollups
            )
        )
    )


def replace_active_season_draft(
    local_db: Any,
    provider_draft: pd.DataFrame | None,
    *,
    year: int,
    platform: str,
    league_id: str,
) -> int:
    """Replace one active draft year from a complete provider payload.

    A provider's complete draft is authoritative for its league/year.  It
    must replace that local partition rather than merge by draft identity:
    older imports may have used a malformed provider ``draft_id`` and would
    otherwise remain beside the corrected rows.  Empty payloads deliberately
    do nothing so an unsuccessful fetch cannot erase retained history.
    """
    if provider_draft is None or provider_draft.empty:
        return 0

    from multi_league.core.league_update_ownership import (
        overlay_provider_columns,
        table_ownership,
    )

    normalized = local_db._normalize_table_frame(
        "draft",
        provider_draft,
        platform=platform,
        league_id=league_id,
        log_context="authoritative provider draft",
    )
    normalized = _bind_provider_db_name(local_db, "draft", normalized)
    existing = (
        local_db.read_table("draft")
        if local_db.table_exists("draft")
        else pd.DataFrame(columns=list(table_ownership("draft").classified_columns))
    )
    protected = overlay_provider_columns(existing, normalized, table_ownership("draft"))
    local_db.save_table(
        "draft",
        protected,
        year=int(year),
        platform=platform,
        league_id=str(league_id),
    )
    return int(len(protected))


def needs_active_season_draft_fetch(
    local_db: Any,
    *,
    platform: str | None = None,
    provider_manifest: pd.DataFrame | None = None,
    manifest_key_columns: tuple[str, ...] = (),
    year: int | None = None,
) -> bool:
    """Whether a weekly worker still needs the one-time active-season draft.

    Weekly workers hydrate only the active season.  Once that scope already
    has draft rows, the provider's completed draft cannot change and fetching
    it again adds latency without changing the quick rebuild input. Yahoo is
    the exception: its historic draft payload may have rows without Yahoo
    player IDs, which cannot be resolved downstream until the one bulk
    ``draftresults/players`` request is made.
    """
    if not local_db.table_exists("draft"):
        return True
    if year is not None:
        try:
            active_draft = local_db.read_table("draft", year=year)
        except (AttributeError, duckdb.Error):
            return True
        if active_draft.empty:
            return True
    elif int(local_db.row_count("draft") or 0) == 0:
        return True

    if provider_manifest is not None:
        if provider_manifest.empty:
            raise RefreshScopeError("provider draft manifest is empty; cannot prove hydrated draft completeness")
        if not manifest_key_columns:
            raise ValueError("provider draft manifest requires identity columns")
        try:
            hydrated = active_draft if year is not None else local_db.read_table("draft")
        except (AttributeError, duckdb.Error):
            return True
        if missing_provider_draft_keys(
            hydrated,
            provider_manifest,
            key_columns=manifest_key_columns,
        ):
            return True
        if not provider_draft_manifest_matches(
            hydrated,
            provider_manifest,
            key_columns=manifest_key_columns,
        ):
            return True

    if str(platform or "").strip().lower() != "yahoo":
        return False

    try:
        draft = local_db.read_table("draft")
    except (AttributeError, duckdb.Error):
        return False
    if draft.empty or "yahoo_player_id" not in draft.columns:
        return True
    relevant = draft
    if "player" in draft.columns:
        names = draft["player"].astype(str).str.strip().str.lower()
        relevant = draft.loc[~names.isin({"", "unknown", "n/a"})].copy()
    if relevant.empty:
        return False
    ids = relevant["yahoo_player_id"]
    return bool((ids.isna() | (ids.astype(str).str.strip() == "")).any())


def _canonical_draft_key_value(value: object) -> str:
    """Normalize provider identity values without treating ``3`` and ``3.0`` differently."""
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if re.fullmatch(r"[+-]?\d+\.0+", text):
        return text.split(".", 1)[0]
    return text


def missing_provider_draft_keys(
    hydrated: pd.DataFrame,
    provider: pd.DataFrame,
    *,
    key_columns: tuple[str, ...],
) -> set[tuple[str, ...]]:
    """Return canonical provider draft identities absent from hydrated source rows.

    A completed draft is immutable, but a prior partial fetch can leave a
    nonempty local table missing one or more picks.  The adapters supply a
    cheap provider manifest and use this exact difference to decide whether a
    full draft refetch is necessary.  Blank local key values are intentionally
    excluded, which makes them a repairable missing identity rather than a
    false match.
    """
    columns = tuple(str(column) for column in key_columns)
    if not columns:
        raise ValueError("draft manifest requires at least one key column")
    missing_provider_columns = [column for column in columns if column not in provider.columns]
    if missing_provider_columns:
        raise ValueError("provider draft manifest is missing key columns: " + ", ".join(missing_provider_columns))
    missing_hydrated_columns = [column for column in columns if column not in hydrated.columns]
    if missing_hydrated_columns:
        return {
            tuple(_canonical_draft_key_value(row[column]) for column in columns)
            for _, row in provider.iterrows()
            if all(_canonical_draft_key_value(row[column]) for column in columns)
        }

    def keys(frame: pd.DataFrame) -> set[tuple[str, ...]]:
        return {
            key
            for key in (
                tuple(_canonical_draft_key_value(row[column]) for column in columns)
                for _, row in frame.iterrows()
            )
            if all(key)
        }

    return keys(provider) - keys(hydrated)


def provider_draft_manifest_matches(
    hydrated: pd.DataFrame,
    provider: pd.DataFrame,
    *,
    key_columns: tuple[str, ...],
) -> bool:
    """Whether local draft identities exactly match a complete provider manifest.

    Provider drafts are immutable.  A local key set that has extra rows or
    duplicate keys is therefore just as invalid as one missing a provider
    pick: it usually means a legacy draft ID or partial recovery remains in
    the active-year partition.  The caller must fetch the authoritative
    draft and replace that partition in each of those cases.
    """
    columns = tuple(str(column) for column in key_columns)
    if not columns:
        raise ValueError("draft manifest requires at least one key column")
    missing_provider_columns = [column for column in columns if column not in provider.columns]
    if missing_provider_columns:
        raise ValueError("provider draft manifest is missing key columns: " + ", ".join(missing_provider_columns))
    if any(column not in hydrated.columns for column in columns):
        return False

    def keys(frame: pd.DataFrame) -> list[tuple[str, ...]]:
        return [tuple(_canonical_draft_key_value(row[column]) for column in columns) for _, row in frame.iterrows()]

    provider_keys = keys(provider)
    if not provider_keys or any(not all(key) for key in provider_keys):
        raise RefreshScopeError("provider draft manifest contains blank identity values")
    if len(provider_keys) != len(set(provider_keys)):
        raise RefreshScopeError("provider draft manifest contains duplicate identities")

    hydrated_keys = keys(hydrated)
    return (
        len(hydrated_keys) == len(provider_keys)
        and len(hydrated_keys) == len(set(hydrated_keys))
        and set(hydrated_keys) == set(provider_keys)
    )


def assert_authoritative_draft_refetch(
    fetched: pd.DataFrame | None,
    provider_manifest: pd.DataFrame,
    *,
    key_columns: tuple[str, ...],
    confirmed_no_draft: bool = False,
) -> bool:
    """Admit a full draft refetch only when its pick keys match the provider witness.

    Returns false solely for a provider-confirmed season without any draft.
    A failed or partial fetch must not leave a stale hydrated partition looking
    publishable.
    """
    if provider_manifest is None:
        raise RefreshScopeError("authoritative draft has no provider pick manifest")
    if provider_manifest.empty:
        if confirmed_no_draft and (fetched is None or fetched.empty):
            return False
        raise RefreshScopeError("authoritative draft no-draft status is unconfirmed")
    if fetched is None or fetched.empty:
        raise RefreshScopeError("authoritative draft refetch returned no completed picks")
    try:
        matches = provider_draft_manifest_matches(
            fetched, provider_manifest, key_columns=key_columns
        )
    except ValueError as exc:
        raise RefreshScopeError("authoritative draft refetch lacks pick identities") from exc
    if not matches:
        raise RefreshScopeError("authoritative draft refetch has incomplete pick identities")
    return True


def refresh_authoritative_draft_partition(
    local_db: Any,
    *,
    provider_manifest: pd.DataFrame,
    key_columns: tuple[str, ...],
    fetch_full: Callable[[], pd.DataFrame | None],
    year: int,
    platform: str,
    league_id: str,
    confirmed_no_draft: bool = False,
) -> int:
    """Reuse the quick draft fetch/replacement only after exact-key admission."""
    if provider_manifest.empty and confirmed_no_draft:
        active_draft = (
            local_db.read_table("draft", year=year)
            if local_db.table_exists("draft") else pd.DataFrame()
        )
        if not active_draft.empty:
            raise RefreshScopeError("provider-confirmed no-draft season has retained active picks")
        return 0
    if not needs_active_season_draft_fetch(
        local_db,
        platform=platform,
        provider_manifest=provider_manifest,
        manifest_key_columns=key_columns,
        year=year,
    ):
        return 0
    fetched = fetch_full()
    if not assert_authoritative_draft_refetch(
        fetched,
        provider_manifest,
        key_columns=key_columns,
        confirmed_no_draft=confirmed_no_draft,
    ):
        return 0
    return replace_active_season_draft(
        local_db,
        fetched,
        year=year,
        platform=platform,
        league_id=league_id,
    )


def stage_refresh_partitions(
    source: Any,
    *,
    db_name: str,
    active_year: int,
    tables: Iterable[str] | None = None,
) -> duckdb.DuckDBPyConnection:
    """Copy exactly one league's publishable refresh scope into a new stage.

    ``active_season`` tables carry the complete recomputed current season;
    ``league_rollup`` tables carry the complete refreshed league rollup.  The
    returned connection is intentionally separate from the local build so the
    fleet-publish manifest cannot accidentally include prior seasons, scratch
    tables, or another league.
    """
    from multi_league.core.delta_publish import CADENCE_ACTIVE_SEASON, canonical_table_registry, qident

    registry = canonical_table_registry()
    requested = list(tables) if tables is not None else sorted(registry)
    unknown = sorted(set(requested) - set(registry))
    if unknown:
        raise RefreshScopeError("Unknown canonical refresh table(s): " + ", ".join(unknown))

    available = {
        str(row[0])
        for row in source.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """
        ).fetchall()
    }
    stage = duckdb.connect(":memory:")
    stage.execute("CREATE SCHEMA public")
    try:
        for table in requested:
            if table not in available:
                continue
            spec = registry[table]
            table_ref = f"public.{qident(table)}"
            if str(spec["cadence_class"]) == CADENCE_ACTIVE_SEASON:
                frame = source.execute(
                    f"SELECT * FROM {table_ref} WHERE db_name = ? AND year = ?",
                    [db_name, int(active_year)],
                ).fetchdf()
            else:
                frame = source.execute(
                    f"SELECT * FROM {table_ref} WHERE db_name = ?",
                    [db_name],
                ).fetchdf()
            stage.register("__refresh_source", frame)
            try:
                stage.execute(f"CREATE TABLE public.{qident(table)} AS SELECT * FROM __refresh_source")
            finally:
                stage.unregister("__refresh_source")
    except Exception:
        stage.close()
        raise
    return stage
