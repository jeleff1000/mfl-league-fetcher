#!/usr/bin/env python3
"""Incrementally refresh one ESPN league's active season through the normal pipeline.

The worker first hydrates the league's existing Fly source history into a
temporary local DuckDB. It then replaces only rows for NFL games finalized in
``___ops`` and Fleet-publishes the active-season partition plus recomputed
league rollups. It never uses the legacy whole-database uploader.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_SCRIPTS = ROOT / "fantasy_football_data_scripts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(DATA_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DATA_SCRIPTS))


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _finalized_espn_matchup_weeks(client: Any, *, year: int, weeks: list[int]) -> list[int]:
    """Return weeks for which ESPN has finalized every fantasy matchup."""
    from multi_league.core.league_refresh import espn_schedule_is_final

    finalized: list[int] = []
    for week in weeks:
        schedule_rows = client.get_raw_schedule(year, int(week))
        if espn_schedule_is_final(schedule_rows):
            finalized.append(int(week))
        else:
            print(f"[ESPN] {year} week {week}: fantasy outcomes are still live; holding matchup rows", flush=True)
    return finalized


def espn_source_manifest_complete(
    *,
    refresh_weeks: list[int],
    fetch_rows: dict[str, Any],
) -> bool:
    """Do not call a safe partial ESPN publication fully source-current."""
    return (
        not fetch_rows.get("pending_nfl_teams")
        and int(fetch_rows.get("final_matchup_weeks") or 0) == len(refresh_weeks)
    )


def _build_context(
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    work_dir: Path,
    frontend_settings: dict[str, Any] | None = None,
) -> tuple[Any, Path, Any, Any]:
    """Load stored ESPN cookies and construct only the active-season context."""
    from initial_import_v3 import _load_frontend_context_settings
    from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient
    from multi_league.data_fetchers.espn.espn_context import ESPNContext, build_manager_names
    from multi_league.utils.credential_store import retrieve_espn_credentials

    credentials = retrieve_espn_credentials(db_name, reader=reader)
    if not credentials:
        raise RuntimeError(f"Fly has no usable encrypted ESPN cookies for {db_name}")

    frontend = frontend_settings if frontend_settings is not None else _load_frontend_context_settings(
        reader,
        db_name.replace("'", "''"),
    )
    league_name = (frontend.get("league_name") or credentials.get("league_name") or "").strip()
    if not league_name:
        raise RuntimeError(f"Fly has no league name for ESPN league {db_name}")

    ctx = ESPNContext(
        league_id=int(credentials["league_id"]),
        league_name=league_name,
        espn_s2=credentials["espn_s2"],
        swid=credentials["swid"],
        start_year=active_year,
        end_year=active_year,
        data_directory=work_dir,
        database_name=db_name,
        manager_name_overrides=frontend.get("manager_name_overrides") or {},
        franchise_merges=frontend.get("franchise_merges") or [],
        keeper_rules=frontend.get("keeper_rules"),
        league_rules=frontend.get("league_rules"),
        standings_weights=frontend.get("standings_weights"),
        is_private=frontend.get("is_private") is True,
        import_mode="quick",
    )
    client = ESPNAPIClient(ctx.league_id, ctx.espn_s2, ctx.swid)
    league = client.get_league(active_year)
    if not league or not getattr(league, "teams", None):
        raise RuntimeError(f"ESPN returned no active league teams for {db_name} in {active_year}")

    ctx.num_teams = len(league.teams)
    ctx.team_to_manager = build_manager_names(league.teams)
    ctx.team_to_team_name[str(active_year)] = {}
    ctx.team_to_manager_by_year[str(active_year)] = dict(ctx.team_to_manager)
    ctx.team_to_guid_by_year[str(active_year)] = {}
    for team in league.teams:
        team_id = int(team.team_id)
        ctx.team_to_team_name[str(active_year)][team_id] = getattr(team, "team_name", f"Team {team_id}")
        owners = getattr(team, "owners", []) or []
        owner = owners[0] if isinstance(owners, list) and owners else owners
        guid = owner.get("id", "") if isinstance(owner, dict) else getattr(owner, "id", "") if owner else ""
        if guid:
            ctx.team_to_guid[team_id] = guid
            ctx.team_to_guid_by_year[str(active_year)][team_id] = guid

    context_path = work_dir / "espn_context.json"
    ctx.save(context_path)
    return ctx, context_path, client, league


def _espn_draft_manifest(league: Any) -> pd.DataFrame:
    """Return ESPN's complete active-draft identity list from the loaded league."""
    draft = getattr(league, "draft", None) or []
    return pd.DataFrame({"pick": list(range(1, len(draft) + 1))})


def _merge_active_payloads(
    *,
    ctx: Any,
    client: Any,
    league: Any,
    local_db: Any,
    active_year: int,
    refresh_weeks: list[int],
    finalized_ops: pd.DataFrame,
) -> dict[str, Any]:
    """Fetch ESPN state without letting a provider adapter replace old weeks."""
    from multi_league.core.canonical_settings import flatten_settings
    from multi_league.core.league_refresh import (
        assert_provider_roster_merge,
        filter_rosters_to_finalized_games,
        merge_provider_refresh_table,
        needs_active_season_draft_fetch,
        pending_provider_nfl_teams,
        replace_active_season_draft,
    )
    from multi_league.data_fetchers.espn.espn_draft import fetch_espn_draft
    from multi_league.data_fetchers.espn.espn_league_settings import fetch_espn_settings
    from multi_league.data_fetchers.espn.espn_matchups import fetch_espn_matchups
    from multi_league.data_fetchers.espn.espn_rosters import fetch_espn_rosters_modern
    from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions

    league_id = str(ctx.get_league_id_for_year(active_year))
    raw_settings = fetch_espn_settings(ctx, active_year, client=client, league=league)
    if not raw_settings:
        raise RuntimeError(f"ESPN returned no settings for {active_year} ({league_id})")
    settings = pd.DataFrame([flatten_settings(raw_settings, "espn", active_year, league_id)])
    settings["db_name"] = local_db.league_name
    merge_provider_refresh_table(
        local_db, "league_settings", settings, platform="espn", league_id=league_id
    )

    # This lower-level fetcher has an explicit week scope and does not call
    # LocalLeagueDB.save_table(), which would delete all prior active-season
    # rows before the narrow refresh can merge its safe replacement rows.
    rosters = fetch_espn_rosters_modern(
        ctx,
        active_year,
        db=local_db,
        max_weeks=max(refresh_weeks),
        weeks=refresh_weeks,
        client=client,
        league=league,
    )
    roster_rows = 0
    pending_nfl_teams: set[str] = set()
    for week in refresh_weeks:
        source = rosters.loc[rosters["week"].astype(int) == int(week)].copy() if rosters is not None else pd.DataFrame()
        ops_week = finalized_ops.loc[finalized_ops["week"].astype(int) == int(week)]
        pending_nfl_teams.update(pending_provider_nfl_teams(source, ops_week))
        safe_rows = filter_rosters_to_finalized_games(
            source,
            ops_week,
        )
        if safe_rows.empty:
            continue
        merge_provider_refresh_table(
            local_db,
            "player_fantasy",
            safe_rows,
            platform="espn",
            league_id=league_id,
        )
        assert_provider_roster_merge(
            local_db,
            safe_rows,
            year=active_year,
            week=week,
            provider_id_column="espn_player_id",
        )
        roster_rows += len(safe_rows)

    final_matchup_weeks = _finalized_espn_matchup_weeks(client, year=active_year, weeks=refresh_weeks)
    matchup_rows = 0
    if final_matchup_weeks:
        matchups = fetch_espn_matchups(ctx, active_year, weeks=final_matchup_weeks)
        if matchups is not None and not matchups.empty:
            merge_provider_refresh_table(
                local_db,
                "matchup",
                matchups,
                platform="espn",
                league_id=league_id,
            )
            matchup_rows = len(matchups)

    transactions = fetch_espn_transactions(
        ctx,
        active_year,
        max_week=max(refresh_weeks),
        client=client,
        league=league,
    )
    if transactions is not None and not transactions.empty:
        merge_provider_refresh_table(
            local_db,
            "transactions",
            transactions,
            platform="espn",
            league_id=league_id,
        )
    draft_rows = 0
    draft_manifest = _espn_draft_manifest(league)
    if needs_active_season_draft_fetch(
        local_db,
        provider_manifest=draft_manifest,
        manifest_key_columns=("pick",),
    ):
        draft = fetch_espn_draft(ctx, active_year)
        if draft is not None and not draft.empty:
            draft_rows = replace_active_season_draft(
                local_db,
                draft,
                year=active_year,
                platform="espn",
                league_id=league_id,
            )
    else:
        print(f"[ESPN] Verified hydrated {active_year} draft against provider manifest", flush=True)
    return {
        "roster_rows": int(roster_rows),
        "final_matchup_rows": int(matchup_rows),
        "final_matchup_weeks": len(final_matchup_weeks),
        "transaction_rows": int(len(transactions) if transactions is not None else 0),
        "draft_rows": draft_rows,
        "pending_nfl_teams": sorted(pending_nfl_teams),
    }


def _write_receipt(receipt: dict[str, Any], path: Path | None) -> None:
    """Emit the non-secret receipt and retain it for the Actions artifact."""
    print(json.dumps(receipt, sort_keys=True))
    if path:
        path.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="ESPN league db_name")
    parser.add_argument("--year", type=int, default=None, help="Active NFL season (default: latest Fly ops season)")
    parser.add_argument("--through-week", type=int, default=None, help="Optional final-week ceiling")
    parser.add_argument("--observed-manifest-digest")
    parser.add_argument("--execute", action="store_true", help="Commit the scoped Fleet bundle to Fly")
    parser.add_argument("--json-out", type=Path, help="Optional non-secret run receipt path")
    args = parser.parse_args(argv)

    os.environ["DATABASE_BACKEND"] = "fly"
    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.league_refresh import (
        active_platform_player_ids,
        active_platform_player_names,
        active_platform_player_name_hints,
        active_refresh_publish_tables,
        completed_weeks_to_refresh,
        finalized_source_boundary,
        hydrate_local_refresh_sources,
        stage_refresh_partitions,
        sync_player_bio_cache_from_fly,
    )
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.league_update_plan import load_persisted_refresh_plan
    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.core.targets.fly_target import FlyTarget
    from scripts.refresh_yahoo_active_season import (
        UPDATE_REFRESH_SOURCE_TABLES,
        OPS_DATABASE,
        _ensure_ops_cache_matches_live,
        _load_active_refresh_inputs,
        _frontend_settings_from_source_context,
        _publish_generation,
        _run_local_pipeline,
        _scope_counts,
        _source_frames,
    )

    reader = FlyReader()
    active_year = args.year or int(
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
            finalized_weeks=finalized_ops["week"].dropna().tolist(),
            last_materialized_week=last_materialized_week,
        )
    )
    receipt: dict[str, Any] = {
        "db_name": args.db,
        "year": active_year,
        "refresh_weeks": refresh_weeks,
        "executed": bool(args.execute),
    }
    receipt.update(finalized_source_boundary(finalized_ops, year=active_year))
    if persisted_plan is not None:
        receipt["source_manifest_digest"] = persisted_plan.observed_manifest_digest
        receipt["source_manifest_json"] = persisted_plan.observed_manifest_json
        receipt["published_manifest_digest"] = persisted_plan.published_manifest_digest
        receipt["refresh_reasons"] = list(persisted_plan.reasons)
    if not refresh_weeks:
        receipt["status"] = "NO_FINALIZED_WEEKS"
        _write_receipt(receipt, args.json_out)
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
        ctx, context_path, client, league = _build_context(
            reader=reader,
            db_name=args.db,
            active_year=active_year,
            work_dir=work_dir,
            frontend_settings=_frontend_settings_from_source_context(
                source_frames["league_context"],
                db_name=args.db,
            ),
        )
        local_db = LocalLeagueDB(work_dir, args.db)
        try:
            receipt["hydrated_rows"] = hydrate_local_refresh_sources(
                local_db,
                source_frames,
                db_name=args.db,
                active_year=active_year,
                expected_platform="espn",
            )
            from multi_league.core.league_update_ownership import local_preservation_snapshot

            preservation_before = local_preservation_snapshot(local_db, preservation_witnesses)
            receipt["fetch_rows"] = _merge_active_payloads(
                ctx=ctx,
                client=client,
                league=league,
                local_db=local_db,
                active_year=active_year,
                refresh_weeks=refresh_weeks,
                finalized_ops=finalized_ops,
            )
            receipt["source_manifest_complete"] = espn_source_manifest_complete(
                refresh_weeks=refresh_weeks,
                fetch_rows=receipt["fetch_rows"],
            )
            if not args.execute:
                receipt["status"] = "DRY_RUN_READY"
                _write_receipt(receipt, args.json_out)
                return 0

            active_connection = local_db.connect()
            receipt["player_bio_sync"] = sync_player_bio_cache_from_fly(
                reader,
                ops_cache=Path(os.environ.get("OPS_CACHE_PATH", "")),
                platform="espn",
                provider_ids=active_platform_player_ids(active_connection, platform="espn"),
                player_names=active_platform_player_names(active_connection, platform="espn"),
                provider_name_hints=active_platform_player_name_hints(active_connection, platform="espn"),
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
                platform="espn",
                keeper_config_hydrated="keeper_config" in source_frames,
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
            publish_tables = sorted(set([*publish_tables, *homepage["published_tables"]]))
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
                raise RuntimeError(f"scoped ESPN refresh did not commit: {result}")
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

    _write_receipt(receipt, args.json_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
