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
from contextlib import nullcontext
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


def _finalized_espn_matchup_weeks(
    client: Any,
    *,
    year: int,
    weeks: list[int],
    expected_team_ids: tuple[str, ...],
    schedule_out: dict[int, list[dict[str, Any]]] | None = None,
) -> list[int]:
    """Return weeks for which ESPN has finalized every fantasy matchup."""
    from multi_league.core.league_refresh import espn_schedule_is_final

    finalized: list[int] = []
    for week in weeks:
        schedule_rows = client.get_raw_schedule(year, int(week))
        if espn_schedule_is_final(schedule_rows, expected_team_ids=expected_team_ids):
            finalized.append(int(week))
            if schedule_out is not None:
                schedule_out[int(week)] = schedule_rows
        else:
            print(f"[ESPN] {year} week {week}: fantasy outcomes are still live; holding matchup rows", flush=True)
    return finalized


def espn_source_manifest_complete(
    *,
    refresh_weeks: list[int],
    fetch_rows: dict[str, Any],
    plan=None,
    year: int | None = None,
) -> bool:
    """Do not call a safe partial ESPN publication fully source-current."""
    from multi_league.core.league_update_plan import active_publication_covers_plan

    return (
        active_publication_covers_plan(plan, year=year, weeks=refresh_weeks)
        and fetch_rows.get("draft_validated") is True
        and not fetch_rows.get("pending_nfl_teams")
        and int(fetch_rows.get("final_matchup_weeks") or 0) == len(refresh_weeks)
    )


def _build_context(
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    work_dir: Path,
    active_league_id: str | None = None,
    league_ids: dict[str, str] | None = None,
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

    league_id = int(active_league_id or credentials["league_id"])
    context_league_ids = {
        str(year): int(saved_id)
        for year, saved_id in (league_ids or {str(active_year): league_id}).items()
    }
    saved_active_id = context_league_ids.get(str(active_year))
    if saved_active_id is not None and saved_active_id != league_id:
        raise RuntimeError("Fly and caller have conflicting active ESPN league IDs")
    context_league_ids[str(active_year)] = league_id
    ctx = ESPNContext(
        league_id=league_id,
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
        league_ids=context_league_ids,
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


def _espn_draft_manifest(client: Any, league: Any, year: int) -> tuple[pd.DataFrame, bool]:
    """Prove parsed picks against ESPN's raw draft and configured draft size."""
    from multi_league.core.league_refresh import RefreshScopeError

    raw = client.get_raw_league(year, ("mDraftDetail", "mSettings"))
    detail = raw.get("draftDetail") if isinstance(raw, dict) else None
    settings = raw.get("settings") if isinstance(raw, dict) else None
    if not isinstance(detail, dict) or not isinstance(settings, dict):
        raise RefreshScopeError("ESPN draft witness lacks raw draft detail or settings")
    picks = detail.get("picks")
    parsed = getattr(league, "draft", None)
    if not isinstance(picks, list) or parsed is None:
        raise RefreshScopeError("ESPN draft witness lacks an explicit pick list")
    if detail.get("drafted") is False and not picks and not parsed:
        return pd.DataFrame(columns=["pick"]), True
    if detail.get("drafted") is not True or detail.get("inProgress") is True:
        raise RefreshScopeError("ESPN active draft is not confirmed complete")

    roster = settings.get("rosterSettings") or {}
    slots = roster.get("lineupSlotCounts") if isinstance(roster, dict) else None
    try:
        teams = int(settings["size"])
        # ESPN slot 21 is injured reserve and has no draft pick. All other
        # positive lineup and bench slots require one draft pick per team.
        rounds = sum(int(count) for slot, count in slots.items() if str(slot) != "21")
        pick_numbers = [int(pick["overallPickNumber"]) for pick in picks]
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise RefreshScopeError("ESPN draft witness lacks team, slot, or pick identities") from exc
    expected = teams * rounds
    if teams < 1 or rounds < 1 or len(pick_numbers) != expected or len(parsed) != expected:
        raise RefreshScopeError("ESPN draft pick count disagrees with configured draft size")
    if sorted(pick_numbers) != list(range(1, expected + 1)):
        raise RefreshScopeError("ESPN raw draft has missing or duplicate overall picks")
    pick_order = (settings.get("draftSettings") or {}).get("pickOrder")
    if pick_order and len(pick_order) != teams:
        raise RefreshScopeError("ESPN draft order disagrees with league team count")
    return pd.DataFrame({"pick": sorted(pick_numbers)}), False


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
    from multi_league.core.league_update_validation import (
        validate_active_roster_frame,
        validate_espn_final_matchup_frame,
        validate_provider_team_inventory,
    )
    from multi_league.core.league_refresh import (
        assert_provider_roster_merge,
        filter_rosters_to_finalized_games,
        merge_provider_refresh_table,
        refresh_authoritative_draft_partition,
        pending_provider_nfl_teams,
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
    expected_team_ids = validate_provider_team_inventory(
        provider="espn",
        settings_team_count=settings.iloc[0]["num_teams"],
        team_ids=tuple(str(int(team.team_id)) for team in league.teams),
    )
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
    provider_roster_team_weeks = validate_active_roster_frame(
        provider="espn",
        season=active_year,
        expected_team_ids=expected_team_ids,
        requested_weeks=tuple(int(week) for week in refresh_weeks),
        player_id_column="espn_player_id",
        rosters=rosters,
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

    final_schedule_graphs: dict[int, list[dict[str, Any]]] = {}
    final_matchup_weeks = _finalized_espn_matchup_weeks(
        client,
        year=active_year,
        weeks=refresh_weeks,
        expected_team_ids=expected_team_ids,
        schedule_out=final_schedule_graphs,
    )
    matchup_rows = 0
    if final_matchup_weeks:
        matchups = fetch_espn_matchups(ctx, active_year, weeks=final_matchup_weeks)
        for week in final_matchup_weeks:
            weekly_matchups = (
                matchups.loc[matchups["week"].astype(int) == int(week)].copy()
                if isinstance(matchups, pd.DataFrame) and "week" in matchups else pd.DataFrame()
            )
            validate_espn_final_matchup_frame(
                season=active_year,
                week=week,
                expected_team_ids=expected_team_ids,
                raw_schedule=final_schedule_graphs[week],
                matchups=weekly_matchups,
            )
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
    draft_manifest, confirmed_no_draft = _espn_draft_manifest(client, league, active_year)
    draft_rows = refresh_authoritative_draft_partition(
        local_db,
        provider_manifest=draft_manifest,
        key_columns=("pick",),
        fetch_full=lambda: fetch_espn_draft(ctx, active_year),
        year=active_year,
        platform="espn",
        league_id=league_id,
        confirmed_no_draft=confirmed_no_draft,
    )
    return {
        "provider_roster_team_weeks": provider_roster_team_weeks,
        "roster_rows": int(roster_rows),
        "final_matchup_rows": int(matchup_rows),
        "final_matchup_weeks": len(final_matchup_weeks),
        "transaction_rows": int(len(transactions) if transactions is not None else 0),
        "draft_rows": draft_rows,
        "draft_validated": True,
        "pending_nfl_teams": sorted(pending_nfl_teams),
    }


def _write_receipt(receipt: dict[str, Any], path: Path | None) -> None:
    """Emit the non-secret receipt and retain it for the Actions artifact."""
    from scripts.league_update_workflow_receipt import write_refresh_receipt

    write_refresh_receipt(receipt, path)


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
    from multi_league.core.fleet_publish import FLEET_HOMEPAGE_SCHEMA_VERSION, build_fleet_partition_bundle
    from multi_league.core.league_refresh import (
        active_nfl_player_ids,
        active_platform_player_ids,
        active_platform_player_names,
        active_platform_player_name_hints,
        active_refresh_publish_tables,
        background_refresh_call,
        completed_weeks_to_refresh,
        finalized_source_boundary,
        hydrate_local_refresh_sources,
        run_independent_refresh_preflight,
        stage_refresh_partitions,
        sync_player_bio_cache_from_fly,
    )
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.league_update_plan import active_provider_league_id, load_persisted_refresh_plan
    from multi_league.core.league_update_timing import PhaseTimer
    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.core.targets.fly_target import FlyTarget
    from scripts.league_update_workflow_receipt import record_publication_commit
    from scripts.refresh_yahoo_active_season import (
        UPDATE_REFRESH_SOURCE_TABLES,
        OPS_DATABASE,
        _active_year_scoring_info,
        _ensure_active_year_ops_cache,
        _ensure_ops_cache_matches_live,
        _load_active_refresh_inputs,
        _active_update_segment_from_source_frames,
        _frontend_settings_from_source_context,
        _capture_update_source_frames,
        _run_local_pipeline,
        _scope_counts,
        _split_active_transform_source_frames,
    )

    timer = PhaseTimer()
    reader = FlyReader()
    from multi_league.core.league_update_status import assert_league_update_entitled
    active_year = args.year or int(
        reader.query_scalar("SELECT MAX(year) FROM nfl_historical.nfl_player_stats_all", database=OPS_DATABASE)
    )
    from multi_league.core.league_update_lineage import assert_canonical_history_complete

    preflight = run_independent_refresh_preflight({
        "entitlement": lambda: assert_league_update_entitled(reader, database_name=args.db)
        if args.execute else None,
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
    persisted_plan = preflight["persisted_plan"]
    if args.execute and persisted_plan is None:
        raise RuntimeError("executing update requires a captured source manifest")
    captured_league_id = active_provider_league_id(persisted_plan, provider="espn")
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
        _write_receipt(receipt, args.json_out)
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
            expected_platform="espn",
        )
        receipt["active_segment"] = {
            "platform": active_segment.platform,
            "current_league_id": active_segment.current_league_id,
            "historical_platforms": list(active_segment.historical_platforms),
        }
        from multi_league.core.league_update_lineage import merge_provider_chain_ids
        from multi_league.core.league_update_ownership import source_preservation_snapshot

        preservation_witnesses = source_preservation_snapshot(source_frames)
        transform_source_frames, historical_source_rows = _split_active_transform_source_frames(
            source_frames, active_year=active_year,
        )
        frontend_settings = _frontend_settings_from_source_context(
            source_frames["league_context"],
            db_name=args.db,
        )
        imported_chain = merge_provider_chain_ids(
            frontend_settings.get("league_ids"), active_segment,
        )
        ctx, context_path, client, league = _build_context(
            reader=reader,
            db_name=args.db,
            active_year=active_year,
            active_league_id=captured_league_id or active_segment.current_league_id,
            league_ids=imported_chain,
            work_dir=work_dir,
            frontend_settings=frontend_settings,
        )
        local_db = LocalLeagueDB(work_dir, args.db)
        try:
            receipt["hydrated_rows"] = hydrate_local_refresh_sources(
                local_db,
                transform_source_frames,
                db_name=args.db,
                active_year=active_year,
                expected_platform="espn",
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
                    _write_receipt(receipt, args.json_out)
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
                weeks=refresh_weeks, provider_id_column="espn_player_id",
            )
            expected_matchup_scores = capture_active_final_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year, weeks=refresh_weeks,
            )

            active_connection = local_db.connect()
            receipt["player_bio_sync"] = sync_player_bio_cache_from_fly(
                reader,
                ops_cache=Path(os.environ.get("OPS_CACHE_PATH", "")),
                platform="espn",
                provider_ids=active_platform_player_ids(active_connection, platform="espn"),
                player_names=active_platform_player_names(active_connection, platform="espn"),
                nfl_player_ids=active_nfl_player_ids(active_connection),
                provider_name_hints=active_platform_player_name_hints(active_connection, platform="espn"),
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
                platform="espn",
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
                weeks=refresh_weeks, provider_id_column="espn_player_id",
                expected_keys=expected_player_keys,
            )
            receipt["transformed_matchup_scope"] = assert_transformed_active_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, expected_scores=expected_matchup_scores,
            )
            timer.mark("transformed_scope_validation")
            local_db.connect()
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
            from multi_league.core.league_update_validation import assert_refresh_derived_output_health

            receipt["derived_health"] = assert_refresh_derived_output_health(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="espn_player_id",
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
            from multi_league.core.league_update_publish_claim import renew_claim_for_publication

            renew_claim_for_publication(reader, database_name=args.db, platform="espn")
            timer.mark("prepublish_claim")
            result = FlyTarget().merge_fleet_partition(
                bundle.path,
                bundle_id=bundle.bundle_id,
                bundle_hash=bundle.bundle_hash,
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
    _write_receipt(receipt, args.json_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
