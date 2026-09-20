"""Fast whole-history homepage rebuilding for active-season refreshes.

Only the league-scoped inputs used by the established homepage builder are
pulled from Fly. Weekly player data is reduced to started rows and required
columns, while existing season/career aggregates are reused.
"""

from __future__ import annotations

import json
from typing import Any

import duckdb
import pandas as pd

from multi_league.transformations.aggregation.aggregation_utils import (
    configure_table_catalog,
    replace_scoped_aggregate_table_from_dataframe,
    set_active_catalog,
)


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


_MATCHUP_COLUMNS = (
    "db_name", "year", "week", "manager", "franchise_id", "team_name", "platform",
    "opponent", "opponent_franchise_id", "team_points", "opponent_points", "is_playoffs",
    "is_consolation", "grade", "win", "loss", "tie", "margin", "gpa",
    "above_league_median", "below_league_median", "champion", "sacko", "playoff_round",
    "consolation_round", "is_bye_week", "manager_lamar", "p_playoffs", "p_champ",
    "power_rating", "lineup_efficiency",
)
_DRAFT_COLUMNS = (
    "db_name", "year", "round", "pick", "manager", "franchise_id", "team_name", "platform",
    "player", "position", "yahoo_player_id", "sleeper_player_id", "espn_player_id", "cost",
    "is_keeper", "is_keeper_status", "is_keeper_cost", "draft_category", "NFL_player_id",
    "manager_lamar", "pick_quality_zscore", "pick_quality_score", "draft_value_zscore",
    "draft_grade", "manager_draft_score", "manager_draft_grade", "pick_score",
)
_TRANSACTION_COLUMNS = (
    "db_name", "transaction_id", "year", "week", "transaction_type", "platform", "manager",
    "franchise_id", "team_name", "player", "position", "yahoo_player_id", "sleeper_player_id",
    "espn_player_id", "source_manager", "source_franchise_id", "trade_direction", "NFL_player_id", "trade_asset_lamar",
    "trade_net_lamar", "player_lamar_ros", "player_lamar_ros_managed", "player_lamar_ros_total",
    "manager_lamar_ros_managed", "total_points_ros_total", "fa_lamar_ros", "drop_regret_score",
    "transaction_quality_score", "transaction_grade",
)
_MATCHUP_SEASON_COLUMNS = (
    "db_name", "manager", "year", "franchise_id", "games", "wins", "losses", "ties", "win_pct",
    "total_team_points", "avg_team_points", "avg_margin", "above_league_median",
    "below_league_median", "optimal_ceiling_pts", "made_playoffs", "is_champion", "is_sacko",
    "p_playoffs", "p_champ", "power_rating", "consolation_round",
)
_PLAYER_SEASON_COLUMNS = (
    "db_name", "NFL_player_id", "year", "player", "position", "manager_lamar", "clutch_equity",
    "wins", "losses", "managers", "franchise_id", "team_points", "opponent_points",
    "playoff_wins", "playoff_losses", "championships", "last_updated",
)


def _columns_sql(table_name: str, columns: tuple[str, ...]) -> str:
    from multi_league.core.delta_publish import canonical_table_registry

    available = set(canonical_table_registry()[table_name]["columns"])
    selected = [column for column in columns if column in available]
    return ", ".join(f'"{column}"' for column in selected)


def _homepage_source_queries(db_name: str) -> dict[str, str]:
    db = _sql_literal(db_name)
    return {
        "matchup": f"SELECT {_columns_sql('matchup', _MATCHUP_COLUMNS)} FROM public.matchup WHERE db_name = {db}",
        "draft": f"SELECT {_columns_sql('draft', _DRAFT_COLUMNS)} FROM public.draft WHERE db_name = {db}",
        "transactions": (
            f"SELECT {_columns_sql('transactions', _TRANSACTION_COLUMNS)} "
            f"FROM public.transactions WHERE db_name = {db}"
        ),
        "player_fantasy": (
            "SELECT db_name, player, manager, franchise_id, year, week, player_week, "
            "manager_lamar, clutch_equity, is_started, is_playoffs, is_consolation, NFL_player_id "
            f"FROM public.player_fantasy WHERE db_name = {db} "
            "AND is_started = 1 AND manager_lamar IS NOT NULL"
        ),
        "active_franchises": (
            "SELECT DISTINCT db_name, TRY_CAST(year AS INTEGER) AS year, franchise_id "
            f"FROM public.player_fantasy WHERE db_name = {db} AND franchise_id IS NOT NULL "
            "UNION SELECT DISTINCT db_name, TRY_CAST(year AS INTEGER) AS year, franchise_id "
            f"FROM public.matchup WHERE db_name = {db} AND franchise_id IS NOT NULL"
        ),
        "league_settings": f"SELECT * FROM public.league_settings WHERE db_name = {db}",
        "league_context": f"SELECT * FROM public.league_context WHERE db_name = {db}",
        "matchup_season": (
            f"SELECT {_columns_sql('matchup_season', _MATCHUP_SEASON_COLUMNS)} "
            f"FROM public.matchup_season WHERE db_name = {db}"
        ),
        "player_fantasy_season": (
            f"SELECT {_columns_sql('player_fantasy_season', _PLAYER_SEASON_COLUMNS)} "
            "FROM public.player_fantasy_season "
            f"WHERE db_name = {db} AND managers IS NOT NULL "
            "AND LOWER(TRIM(COALESCE(managers, ''))) NOT IN "
            "('', 'unrostered', 'fa', 'free agent', 'waivers')"
        ),
        "homepage_manager_profiles": (
            f"SELECT * FROM public.homepage_manager_profiles WHERE db_name = {db}"
        ),
        "homepage_league_summary": (
            f"SELECT * FROM public.homepage_league_summary WHERE db_name = {db}"
        ),
        "player_bio": (
            "SELECT b.NFL_player_id, b.player, b.headshot_url, b.yahoo_player_id, "
            "b.sleeper_player_id, b.espn_id FROM ___ops.nfl_historical.player_bio b WHERE "
            f"b.NFL_player_id IN (SELECT DISTINCT NFL_player_id FROM public.player_fantasy WHERE db_name = {db} "
            "AND NFL_player_id IS NOT NULL AND is_started = 1 AND manager_lamar IS NOT NULL) "
            f"OR b.NFL_player_id IN (SELECT DISTINCT NFL_player_id FROM public.draft WHERE db_name = {db} AND NFL_player_id IS NOT NULL) "
            f"OR b.NFL_player_id IN (SELECT DISTINCT NFL_player_id FROM public.transactions WHERE db_name = {db} AND NFL_player_id IS NOT NULL)"
        ),
    }


def _load_homepage_source_frames(reader: Any, db_name: str) -> dict[str, pd.DataFrame]:
    queries = _homepage_source_queries(db_name)
    tagged_parts = [
        f"SELECT {_sql_literal(table_name)} AS source_table, json_group_array(to_json(t)) AS payload "
        f"FROM ({sql}) AS t"
        for table_name, sql in queries.items()
    ]
    rows = reader.query(
        "SELECT source_table, payload FROM (" + " UNION ALL ".join(tagged_parts) + ") AS homepage_source_snapshot",
        database="___leagues",
    )
    payloads: dict[str, list[dict[str, Any]]] = {table_name: [] for table_name in queries}
    for row in rows:
        table_name = str(row.get("source_table") or "")
        if table_name not in payloads:
            raise RuntimeError(f"homepage source snapshot returned unknown table {table_name!r}")
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload)
        if payload is None:
            records = []
        else:
            records = payload if isinstance(payload, list) else [payload]
        if not all(isinstance(record, dict) for record in records):
            raise RuntimeError(f"homepage source snapshot returned invalid {table_name} payload")
        payloads[table_name].extend(records)
    frames = {
        table_name: pd.DataFrame(records)
        for table_name, records in payloads.items()
    }

    # Fly's JSON query surface has no column metadata when a SELECT returns no
    # rows. Preserve canonical empty-table schemas so preseason leagues and
    # leagues without transactions/drafts still rebuild deterministically.
    from multi_league.core.delta_publish import canonical_table_registry

    registry = canonical_table_registry()
    skinny_player_columns = [
        "db_name",
        "player",
        "manager",
        "franchise_id",
        "year",
        "week",
        "player_week",
        "manager_lamar",
        "clutch_equity",
        "is_started",
        "is_playoffs",
        "is_consolation",
        "NFL_player_id",
    ]
    for table_name, frame in list(frames.items()):
        if not frame.empty or len(frame.columns):
            continue
        if table_name == "player_bio":
            columns = ["NFL_player_id", "player", "headshot_url", "yahoo_player_id", "sleeper_player_id", "espn_id"]
        elif table_name == "active_franchises":
            columns = ["db_name", "year", "franchise_id"]
        elif table_name == "player_fantasy":
            columns = skinny_player_columns
        else:
            columns = list(registry[table_name]["columns"])
        frames[table_name] = pd.DataFrame({column: pd.Series(dtype="object") for column in columns})
    return frames


def _active_franchise_ids(
    snapshot: pd.DataFrame,
    *,
    active_year: int,
    active_source: Any | None,
    db_name: str,
) -> set[str]:
    """Return every active franchise, including teams without enriched starters."""
    ids: set[str] = set()
    if {"year", "franchise_id"}.issubset(snapshot.columns):
        years = pd.to_numeric(snapshot["year"], errors="coerce")
        ids.update(
            str(value)
            for value in snapshot.loc[years.eq(int(active_year)), "franchise_id"].dropna()
            if str(value).strip()
        )
    if active_source is None:
        return ids

    available = {
        str(row[0])
        for row in active_source.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ).fetchall()
    }
    for table_name in ("matchup", "player_fantasy"):
        if table_name not in available:
            continue
        columns = {
            str(row[0])
            for row in active_source.execute(f'DESCRIBE public."{table_name}"').fetchall()
        }
        if not {"db_name", "year", "franchise_id"}.issubset(columns):
            continue
        rows = active_source.execute(
            f'SELECT DISTINCT franchise_id FROM public."{table_name}" '
            "WHERE db_name = ? AND TRY_CAST(year AS INTEGER) = ? "
            "AND franchise_id IS NOT NULL",
            [db_name, int(active_year)],
        ).fetchall()
        ids.update(str(row[0]) for row in rows if str(row[0] or "").strip())
    return ids


def _overlay_active_source_frames(
    frames: dict[str, pd.DataFrame],
    source: Any,
    *,
    db_name: str,
    active_year: int,
) -> dict[str, pd.DataFrame]:
    """Replace stale remote active-season inputs with the new local build."""
    available = {
        str(row[0])
        for row in source.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        ).fetchall()
    }
    overlaid = {table_name: frame.copy() for table_name, frame in frames.items()}
    for table_name, remote in list(overlaid.items()):
        if table_name == "player_bio" or table_name.startswith("homepage_") or table_name not in available:
            continue
        columns = {
            str(row[0])
            for row in source.execute(f'DESCRIBE public."{table_name}"').fetchall()
        }
        if "db_name" not in columns:
            continue
        predicate = "db_name = ?"
        params: list[Any] = [db_name]
        if "year" in columns:
            predicate += " AND TRY_CAST(year AS INTEGER) = ?"
            params.append(int(active_year))
        if table_name == "player_fantasy":
            selected = [
                column
                for column in (
                    "db_name", "player", "manager", "franchise_id", "year", "week",
                    "player_week", "manager_lamar", "clutch_equity", "is_started",
                    "is_playoffs", "is_consolation", "NFL_player_id",
                )
                if column in columns
            ]
            select_sql = ", ".join(f'"{column}"' for column in selected)
            predicate += " AND is_started = 1 AND manager_lamar IS NOT NULL"
        else:
            select_sql = "*"
        local = source.execute(
            f'SELECT {select_sql} FROM public."{table_name}" WHERE {predicate}',
            params,
        ).fetchdf()
        if "year" in columns and "year" in remote.columns:
            years = pd.to_numeric(remote["year"], errors="coerce")
            remote = remote.loc[years.ne(int(active_year))].copy()
        elif "year" not in columns:
            remote = remote.iloc[0:0].copy()
        column_order = list(dict.fromkeys([*remote.columns, *local.columns]))
        overlaid[table_name] = pd.concat(
            [remote.reindex(columns=column_order), local.reindex(columns=column_order)],
            ignore_index=True,
        )
    return overlaid


def _merge_active_manager_profiles(
    existing: pd.DataFrame,
    refreshed: pd.DataFrame,
    active_franchise_ids: set[str],
) -> pd.DataFrame:
    """Preserve unchanged historical managers and replace active franchises."""
    retained = existing.drop(columns=["db_name"], errors="ignore").copy()
    if "franchise_id" in retained.columns:
        retained = retained.loc[
            ~retained["franchise_id"].astype(str).isin(active_franchise_ids)
        ].copy()
    fresh = refreshed.drop(columns=["db_name"], errors="ignore").copy()
    columns = list(dict.fromkeys([*retained.columns, *fresh.columns]))
    records = [
        *retained.reindex(columns=columns).to_dict("records"),
        *fresh.reindex(columns=columns).to_dict("records"),
    ]
    return pd.DataFrame.from_records(
        records,
        columns=columns,
    )


def _preserve_existing_summary_values(
    existing: pd.DataFrame,
    refreshed: pd.DataFrame,
) -> pd.DataFrame:
    """Prevent an active-season delta from erasing established history facts."""
    if existing.empty or refreshed.empty:
        return refreshed.drop(columns=["db_name"], errors="ignore").copy()
    old = existing.drop(columns=["db_name"], errors="ignore").iloc[0].to_dict()
    new = refreshed.drop(columns=["db_name"], errors="ignore").iloc[0].to_dict()
    for column, old_value in old.items():
        new_value = new.get(column)
        if (column not in new or pd.isna(new_value)) and not pd.isna(old_value):
            new[column] = old_value
    return pd.DataFrame([new])


def compute_homepage_frames_from_fly(
    reader: Any,
    db_name: str,
    *,
    active_source: Any | None = None,
    active_year: int | None = None,
) -> dict[str, pd.DataFrame]:
    """Compute canonical homepage frames locally from a bounded Fly snapshot."""
    from multi_league.transformations.aggregation.homepage_summary import compute_homepage_frames

    frames = _load_homepage_source_frames(reader, db_name)
    if active_source is not None:
        if active_year is None:
            raise ValueError("active_year is required when overlaying an active source")
        frames = _overlay_active_source_frames(
            frames,
            active_source,
            db_name=db_name,
            active_year=active_year,
        )
    player_bio = frames.pop("player_bio")
    active_franchises = frames.pop("active_franchises")
    existing_profiles = frames.pop("homepage_manager_profiles")
    existing_summary = frames.pop("homepage_league_summary")
    active_franchise_ids: set[str] = set()
    if active_year is not None:
        active_franchise_ids = _active_franchise_ids(
            active_franchises,
            active_year=active_year,
            active_source=active_source,
            db_name=db_name,
        )
    conn = duckdb.connect(":memory:")
    previous_catalog = set_active_catalog("memory")
    try:
        conn.execute("CREATE SCHEMA public")
        for table_name, frame in frames.items():
            conn.register("__homepage_source", frame)
            try:
                conn.execute(f'CREATE TABLE public."{table_name}" AS SELECT * FROM __homepage_source')
            finally:
                conn.unregister("__homepage_source")

        # The first Fleet commit replaces the complete current-season aggregate.
        # Re-derive the small career view from all retained season rows so the
        # homepage never reads the pre-refresh career table.
        conn.execute(
            "CREATE TABLE public.player_fantasy_career AS "
            "SELECT db_name, NFL_player_id, MAX(player) AS player, "
            "STRING_AGG(DISTINCT managers, ', ') FILTER (WHERE managers IS NOT NULL) AS managers, "
            "SUM(manager_lamar) AS manager_lamar, SUM(clutch_equity) AS clutch_equity "
            "FROM public.player_fantasy_season GROUP BY db_name, NFL_player_id"
        )

        conn.execute("ATTACH ':memory:' AS ___ops")
        conn.execute("CREATE SCHEMA ___ops.nfl_historical")
        conn.register("__player_bio", player_bio)
        try:
            conn.execute("CREATE TABLE ___ops.nfl_historical.player_bio AS SELECT * FROM __player_bio")
        finally:
            conn.unregister("__player_bio")
        conn.execute(
            "CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all AS "
            "SELECT DISTINCT p.player_week, p.player, b.headshot_url, p.NFL_player_id "
            "FROM public.player_fantasy p LEFT JOIN ___ops.nfl_historical.player_bio b "
            "ON p.NFL_player_id = b.NFL_player_id"
        )
        profile_scope = active_franchise_ids if not existing_profiles.empty else None
        homepage = compute_homepage_frames(
            conn,
            db_name,
            manager_profile_franchise_ids=profile_scope,
        )
        homepage["homepage_league_summary"] = _preserve_existing_summary_values(
            existing_summary,
            homepage["homepage_league_summary"],
        )
        if profile_scope is not None:
            homepage["homepage_manager_profiles"] = _merge_active_manager_profiles(
                existing_profiles,
                homepage["homepage_manager_profiles"],
                active_franchise_ids,
            )
        return homepage
    finally:
        set_active_catalog(previous_catalog)
        conn.close()


def write_homepage_frames(conn: Any, db_name: str, frames: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Write computed homepage frames into the disposable local league DB."""
    configure_table_catalog(conn)
    counts: dict[str, int] = {}
    for table_name, frame in frames.items():
        if not table_name.startswith("homepage_"):
            raise ValueError(f"refusing non-homepage rollup: {table_name}")
        replace_scoped_aggregate_table_from_dataframe(conn, db_name, table_name, frame)
        counts[table_name] = int(len(frame))
    return counts


def prepare_homepage_refresh(
    *,
    reader: Any,
    local_db: Any,
    db_name: str,
    active_year: int,
) -> dict[str, Any]:
    """Compute the shared homepage slice into the local refresh database.

    The caller includes these league-rollup tables in the same Fleet bundle as
    the active-season partitions, so data and homepage state commit together.
    """
    frames = compute_homepage_frames_from_fly(
        reader,
        db_name,
        active_source=local_db.connect(),
        active_year=active_year,
    )
    rows = write_homepage_frames(local_db.connect(), db_name, frames)
    tables = sorted(frames)
    return {"published_tables": tables, "rows": rows}
