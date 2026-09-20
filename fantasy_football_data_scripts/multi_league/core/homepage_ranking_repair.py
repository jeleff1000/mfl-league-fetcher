"""Narrow automatic repair for a missing homepage manager-ranking partition.

This is intentionally not a general reaggregation lane.  It is used only when
persisted matchup history exists but the one required homepage rollup has no
rows.  The repair reads three bounded league-scoped inputs, calls the canonical
ranking aggregator, and publishes only ``homepage_manager_rankings`` through
the existing generation-fenced Fleet delta path.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


_MATCHUP_COLUMNS = (
    "db_name",
    "year",
    "week",
    "manager",
    "franchise_id",
    "team_points",
    "is_bye_week",
    "is_consolation",
    "is_playoffs",
    "win",
    "loss",
    "tie",
    "above_league_median",
    "below_league_median",
    "champion",
    "power_rating",
)
_MATCHUP_SEASON_COLUMNS = ("db_name", "year", "franchise_id", "power_rating")
_LEAGUE_SETTINGS_COLUMNS = ("db_name", "year", "uses_median")


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def manager_rankings_repair_health(reader: Any, db_name: str) -> dict[str, Any]:
    """Return the one-table repair witness in one bounded Fly read."""
    db = _sql_literal(db_name)
    rows = reader.query(
        "SELECT "
        "COALESCE((SELECT MAX(generation) FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {db}), 0) AS base_generation, "
        f"(SELECT COUNT(*) FROM public.matchup WHERE db_name = {db} "
        "AND team_points IS NOT NULL) AS matchup_rows, "
        f"(SELECT COUNT(*) FROM public.homepage_manager_rankings WHERE db_name = {db}) "
        "AS ranking_rows",
        database="___leagues",
    )
    if len(rows) != 1:
        raise RuntimeError(f"manager-ranking health returned {len(rows)} rows for {db_name}")
    row = rows[0]
    base_generation = int(row.get("base_generation") or 0)
    matchup_rows = int(row.get("matchup_rows") or 0)
    ranking_rows = int(row.get("ranking_rows") or 0)
    return {
        "base_generation": base_generation,
        "matchup_rows": matchup_rows,
        "ranking_rows": ranking_rows,
        "needed": matchup_rows > 0 and ranking_rows == 0,
    }


def _load_manager_rankings_source_frames(reader: Any, db_name: str) -> dict[str, pd.DataFrame]:
    """Load only the three canonical inputs consumed by manager rankings."""
    db = _sql_literal(db_name)
    queries = {
        "matchup": (
            f"SELECT {', '.join(_MATCHUP_COLUMNS)} FROM public.matchup WHERE db_name = {db}"
        ),
        "matchup_season": (
            f"SELECT {', '.join(_MATCHUP_SEASON_COLUMNS)} FROM public.matchup_season "
            f"WHERE db_name = {db}"
        ),
        "league_settings": (
            f"SELECT {', '.join(_LEAGUE_SETTINGS_COLUMNS)} FROM public.league_settings "
            f"WHERE db_name = {db}"
        ),
    }
    union = " UNION ALL ".join(
        f"SELECT {_sql_literal(name)} AS source_table, json_group_array(to_json(t)) AS payload "
        f"FROM ({sql}) AS t"
        for name, sql in queries.items()
    )
    rows = reader.query(
        "SELECT source_table, payload FROM (" + union + ") AS ranking_source_snapshot",
        database="___leagues",
    )
    records: dict[str, list[dict[str, Any]]] = {name: [] for name in queries}
    for row in rows:
        table_name = str(row.get("source_table") or "")
        if table_name not in records:
            raise RuntimeError(f"manager-ranking snapshot returned unknown table {table_name!r}")
        payload = row.get("payload")
        if isinstance(payload, str):
            payload = json.loads(payload)
        values = [] if payload is None else (payload if isinstance(payload, list) else [payload])
        if not all(isinstance(value, dict) for value in values):
            raise RuntimeError(f"manager-ranking snapshot returned invalid {table_name} payload")
        records[table_name].extend(values)

    schemas = {
        "matchup": _MATCHUP_COLUMNS,
        "matchup_season": _MATCHUP_SEASON_COLUMNS,
        "league_settings": _LEAGUE_SETTINGS_COLUMNS,
    }
    return {
        name: (
            pd.DataFrame(values)
            if values
            else pd.DataFrame({column: pd.Series(dtype="object") for column in schemas[name]})
        )
        for name, values in records.items()
    }


def prepare_manager_rankings_repair(
    *,
    local_db: Any,
    db_name: str,
    source_frames: Mapping[str, pd.DataFrame],
) -> dict[str, Any]:
    """Compute and stage only the canonical manager-ranking rollup."""
    from multi_league.transformations.aggregation.aggregation_utils import (
        replace_scoped_aggregate_table_from_dataframe,
        set_active_catalog,
    )
    from multi_league.transformations.aggregation.homepage_summary import (
        compute_manager_rankings,
    )

    required = {"matchup", "matchup_season", "league_settings"}
    missing = sorted(required - set(source_frames))
    if missing:
        raise ValueError("manager-ranking repair is missing source frames: " + ", ".join(missing))

    conn = duckdb.connect(":memory:")
    previous_catalog = set_active_catalog("memory")
    try:
        conn.execute("CREATE SCHEMA public")
        for table_name in sorted(required):
            frame = source_frames[table_name]
            conn.register("__ranking_source", frame)
            try:
                conn.execute(f'CREATE TABLE public."{table_name}" AS SELECT * FROM __ranking_source')
            finally:
                conn.unregister("__ranking_source")
        rankings = compute_manager_rankings(conn, db_name)
    finally:
        set_active_catalog(previous_catalog)
        conn.close()

    if rankings.empty:
        raise RuntimeError(f"manager-ranking repair produced no rows for {db_name}")
    replace_scoped_aggregate_table_from_dataframe(
        local_db.connect(), db_name, "homepage_manager_rankings", rankings,
    )
    return {"published_tables": ["homepage_manager_rankings"], "rows": int(len(rankings))}


def repair_missing_manager_rankings_if_needed(
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    before_publish: Callable[[], object] | None = None,
    merge_timeout_seconds: int = 40,
) -> dict[str, Any]:
    """Repair a missing partition atomically; leave healthy leagues untouched."""
    health = manager_rankings_repair_health(reader, db_name)
    if not health["needed"]:
        return {**health, "published": False}

    source_frames = _load_manager_rankings_source_frames(reader, db_name)
    after_snapshot = manager_rankings_repair_health(reader, db_name)
    if after_snapshot["ranking_rows"]:
        return {**after_snapshot, "published": False}
    if after_snapshot["base_generation"] != health["base_generation"]:
        raise RuntimeError(
            f"{db_name} changed while capturing manager-ranking sources; retry against the new generation"
        )

    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.league_refresh import stage_refresh_partitions
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.targets.fly_target import FlyTarget

    with tempfile.TemporaryDirectory(prefix=f"{db_name}_ranking_repair_") as temp_dir:
        local_db = LocalLeagueDB(Path(temp_dir), db_name)
        try:
            prepared = prepare_manager_rankings_repair(
                local_db=local_db,
                db_name=db_name,
                source_frames=source_frames,
            )
            stage = stage_refresh_partitions(
                local_db.connect(),
                db_name=db_name,
                active_year=int(active_year),
                tables=prepared["published_tables"],
            )
            try:
                bundle = build_fleet_partition_bundle(
                    stage,
                    active_year=int(active_year),
                    league_generations={db_name: int(health["base_generation"])},
                    tables=prepared["published_tables"],
                    output_dir=Path(temp_dir) / "bundle",
                )
            finally:
                stage.close()
            if before_publish is not None:
                before_publish()
            result = FlyTarget().merge_fleet_partition(
                bundle.path,
                bundle_id=bundle.bundle_id,
                bundle_hash=bundle.bundle_hash,
                merge_timeout_seconds=int(merge_timeout_seconds),
            )
        finally:
            local_db.close()

    if str(result.get("status") or "").upper() != "COMMITTED":
        raise RuntimeError("manager-ranking repair did not return a confirmed COMMITTED publication")
    return {
        **health,
        **prepared,
        "published": True,
        "bundle_id": bundle.bundle_id,
        "result": result,
    }
