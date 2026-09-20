"""Repair missing historical season rollups when an active season has no weeks.

The normal active-week bundle asks Fly to repair missing season aggregates in
the same transaction as its week publication.  A league with no renewed or
materialized active season has no week partition to send, so this module sends
one unchanged ``league_context`` row as the generation-fenced transaction
witness.  Fly still rebuilds only missing season partitions and their dependent
career rollups from retained canonical weekly history.
"""

from __future__ import annotations

import json
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _missing_years(reader: Any, db_name: str) -> list[int]:
    # Keep the fingerprint contract identical to the planner and server repair
    # gate; this does not download historical rows.
    from multi_league.core.league_update_plan import _missing_derived_aggregate_years

    safe_db = str(db_name).replace("'", "''")
    return sorted(_missing_derived_aggregate_years(reader, safe_db=safe_db))


def _load_context_witness(reader: Any, db_name: str) -> tuple[int, pd.DataFrame]:
    db = _sql_literal(db_name)
    rows = reader.query(
        "SELECT COALESCE((SELECT MAX(generation) "
        "FROM merge_admin.league_publish_generations "
        f"WHERE db_name = {db}), 0) AS base_generation, "
        "to_json(c) AS context_payload "
        f"FROM public.league_context c WHERE c.db_name = {db} LIMIT 1",
        database="___leagues",
    )
    if len(rows) != 1:
        raise RuntimeError(f"Expected one league_context witness for {db_name}, found {len(rows)}")
    payload = rows[0].get("context_payload")
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, dict) or str(payload.get("db_name") or "") != db_name:
        raise RuntimeError(f"Invalid league_context witness for {db_name}")
    return int(rows[0].get("base_generation") or 0), pd.DataFrame([payload])


def repair_missing_season_rollups_if_needed(
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    before_publish: Callable[[], object] | None = None,
    merge_timeout_seconds: int = 40,
) -> dict[str, Any]:
    """Atomically restore only missing rollups from retained weekly history."""
    missing_years = _missing_years(reader, db_name)
    if not missing_years:
        return {"missing_years": [], "published": False}

    base_generation, context = _load_context_witness(reader, db_name)

    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.league_refresh import stage_refresh_partitions
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.targets.fly_target import FlyTarget

    with tempfile.TemporaryDirectory(prefix=f"{db_name}_season_rollup_repair_") as temp_dir:
        local_db = LocalLeagueDB(Path(temp_dir), db_name)
        try:
            conn = local_db.connect()
            conn.register("__context_witness", context)
            try:
                conn.execute(
                    "CREATE OR REPLACE TABLE public.league_context AS "
                    "SELECT * FROM __context_witness"
                )
            finally:
                conn.unregister("__context_witness")
            stage = stage_refresh_partitions(
                conn,
                db_name=db_name,
                active_year=int(active_year),
                tables=["league_context"],
            )
            try:
                bundle = build_fleet_partition_bundle(
                    stage,
                    active_year=int(active_year),
                    league_generations={db_name: base_generation},
                    tables=["league_context"],
                    output_dir=Path(temp_dir) / "bundle",
                    rebuild_career_rollups=True,
                    repair_missing_season_rollups=True,
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
        raise RuntimeError("season-rollup repair did not return a confirmed COMMITTED publication")
    remaining = _missing_years(reader, db_name)
    if remaining:
        raise RuntimeError(
            f"season-rollup repair left incomplete years for {db_name}: {remaining}"
        )
    season_rollups = dict((result.get("season_rollups") or {}).get(db_name) or {})
    career_rollups = dict((result.get("career_rollups") or {}).get(db_name) or {})
    return {
        "base_generation": base_generation,
        "missing_years": missing_years,
        "remaining_missing_years": remaining,
        "published": True,
        "bundle_id": bundle.bundle_id,
        "published_tables": sorted({"league_context", *season_rollups, *career_rollups}),
        "season_rollups": season_rollups,
        "career_rollups": career_rollups,
        "result": result,
    }
