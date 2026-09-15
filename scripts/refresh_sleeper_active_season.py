#!/usr/bin/env python3
"""Incrementally refresh one Sleeper league's active season through Fly Fleet.

The saved Sleeper league ID is normally the last completed season. This
entrypoint resolves the current season from the persisted league-ID chain,
validates a predecessor chain when one is stored, hydrates
the existing Fly history, and publishes only the active-year partition plus
recomputed league rollups.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from collections.abc import Callable
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


def _renewal_chain_reaches_seed(
    candidate_league_id: str,
    *,
    seed_league_id: str,
    get_league: Callable[[str], dict[str, Any] | None],
) -> bool:
    """Require a candidate's predecessor chain to include the stored league."""
    seen: set[str] = set()
    current_id = str(candidate_league_id)
    seed = str(seed_league_id)
    while current_id and current_id not in seen:
        if current_id == seed:
            return True
        seen.add(current_id)
        current = get_league(current_id) or {}
        previous = current.get("previous_league_id")
        current_id = str(previous) if previous else ""
    return False


def _resolve_active_renewal(
    client: Any,
    *,
    seed_league_id: str | None,
    active_year: int,
    known_league_ids: dict[str, str],
) -> dict[str, Any] | None:
    """Prove a persisted ID, or discover one through saved league members.

    The member list only supplies candidate IDs. A successor is accepted
    solely when its provider predecessor chain reaches the stored seed.
    """
    active_id = str(known_league_ids.get(str(active_year)) or "")
    if not active_id:
        if not seed_league_id:
            return None
        candidates: set[str] = set()
        members = client.get_league_users(seed_league_id)
        for member in members:
            user_id = str(member.get("user_id") or "").strip()
            if not user_id:
                continue
            for league in client.get_user_leagues(user_id, "nfl", active_year):
                candidate_id = str(league.get("league_id") or "").strip()
                if candidate_id:
                    candidates.add(candidate_id)
        verified = [
            candidate_id for candidate_id in sorted(candidates)
            if str((client.get_league(candidate_id) or {}).get("season") or "") == str(active_year)
            and _renewal_chain_reaches_seed(
                candidate_id, seed_league_id=seed_league_id, get_league=client.get_league
            )
        ]
        if len(verified) != 1:
            return None
        active_id = verified[0]
    candidate = client.get_league(active_id) or {}
    if str(candidate.get("league_id") or "") != active_id:
        return None
    if str(candidate.get("season") or "") != str(active_year):
        return None
    if not seed_league_id:
        if str(candidate.get("league_id") or "") != active_id:
            return None
        if candidate.get("previous_league_id"):
            return None
        return candidate
    if _renewal_chain_reaches_seed(
        active_id,
        seed_league_id=seed_league_id,
        get_league=client.get_league,
    ):
        return candidate
    return None


def _sleeper_week_is_final(league: dict[str, Any], week: int) -> bool:
    """Sleeper sets ``last_scored_leg`` only after it finalizes a scoring leg."""
    try:
        return int((league.get("settings") or {}).get("last_scored_leg") or 0) >= int(week)
    except (TypeError, ValueError):
        return False


def _write_receipt(receipt: dict[str, Any], path: Path | None) -> None:
    print(json.dumps(receipt, sort_keys=True))
    if path:
        path.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")


def _json_map(value: object) -> dict[str, str]:
    """Parse a persisted year -> provider-ID mapping without trusting it yet."""
    if not value:
        return {}
    try:
        raw = json.loads(str(value)) if isinstance(value, str) else value
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {str(year): str(league_id) for year, league_id in raw.items() if league_id}


def _load_persisted_sleeper_chain(reader: Any, *, db_name: str) -> tuple[dict[str, Any], dict[str, str]]:
    """Load the onboarding chain plus canonical historical IDs from Fly."""
    quoted_db = _sql_literal(db_name)
    context_columns = {
        str(row["column_name"])
        for row in reader.query(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'league_context'",
            database="___leagues",
        )
    }
    context_select = [
        "league_id",
        "league_name",
        "manager_name_overrides_json",
        "franchise_merges_json",
        "keeper_rules_json",
        "league_rules_json",
        "standings_weights_json",
        "is_private",
    ]
    # Older rows predate ``league_ids_json``.  Their per-year canonical
    # settings remain enough to reconstruct the already imported chain.
    if "league_ids_json" in context_columns:
        context_select.insert(2, "league_ids_json")
    context_rows = reader.query(
        f"SELECT {', '.join(context_select)} FROM public.league_context WHERE db_name = {quoted_db}",
        database="___leagues",
    )
    if not context_rows:
        raise RuntimeError(f"Fly has no Sleeper league context for {db_name}")
    context = context_rows[0]
    known = _json_map(context.get("league_ids_json"))
    setting_rows = reader.query(
        "SELECT year, league_key FROM public.league_settings "
        f"WHERE db_name = {quoted_db} AND LOWER(COALESCE(platform, '')) = 'sleeper' "
        "AND league_key IS NOT NULL",
        database="___leagues",
    )
    for row in setting_rows:
        try:
            year = str(int(row["year"]))
        except (TypeError, ValueError):
            continue
        league_id = str(row.get("league_key") or "").strip()
        if league_id:
            known.setdefault(year, league_id)
    return context, known


def _build_context(
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    work_dir: Path,
    active_league_id: str | None = None,
) -> tuple[Any, Path, Any, dict[str, Any] | None]:
    """Create the active Sleeper context from Fly registry/context state."""
    from initial_import_v3 import _load_frontend_context_settings
    from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient
    from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext

    frontend, known_league_ids = _load_persisted_sleeper_chain(reader, db_name=db_name)
    if active_league_id:
        known_league_ids[str(active_year)] = str(active_league_id)
    predecessors = [
        (int(year), league_id)
        for year, league_id in known_league_ids.items()
        if str(year).isdigit() and int(year) < int(active_year) and league_id
    ]
    seed_league_id = max(predecessors)[1] if predecessors else None
    if seed_league_id is None and not known_league_ids.get(str(active_year)):
        raise RuntimeError(f"Fly has no active Sleeper league ID for {db_name}")
    client = SleeperAPIClient()
    renewal = _resolve_active_renewal(
        client,
        seed_league_id=seed_league_id,
        active_year=active_year,
        known_league_ids=known_league_ids,
    )
    if not renewal:
        return None, Path(), client, None
    known_league_ids[str(active_year)] = str(renewal["league_id"])

    frontend_settings = _load_frontend_context_settings(reader, db_name.replace("'", "''"))
    league_name = (frontend_settings.get("league_name") or renewal.get("name") or frontend.get("league_name") or "").strip()
    if not league_name:
        raise RuntimeError(f"Fly has no league name for Sleeper league {db_name}")
    renewal_id = str(renewal["league_id"])
    ctx = SleeperContext(
        league_id=renewal_id,
        league_name=league_name,
        username="",
        start_year=active_year,
        end_year=active_year,
        data_directory=work_dir,
        database_name=db_name,
        league_ids=known_league_ids,
        manager_name_overrides=frontend_settings.get("manager_name_overrides") or {},
        franchise_merges=frontend_settings.get("franchise_merges") or [],
        keeper_rules=frontend_settings.get("keeper_rules"),
        league_rules=frontend_settings.get("league_rules"),
        standings_weights=frontend_settings.get("standings_weights"),
        is_private=frontend_settings.get("is_private") is True,
        import_mode="quick",
    )
    users = client.get_league_users(renewal_id)
    rosters = client.get_league_rosters(renewal_id)
    ctx.build_roster_mappings(rosters, users)
    context_path = work_dir / "sleeper_context.json"
    ctx.save(context_path)
    return ctx, context_path, client, renewal


def _merge_active_payloads(
    *,
    ctx: Any,
    client: Any,
    active_league: dict[str, Any],
    local_db: Any,
    active_year: int,
    refresh_weeks: list[int],
    finalized_ops: pd.DataFrame,
) -> dict[str, int]:
    """Fetch narrow Sleeper state while preserving earlier active-season rows."""
    from multi_league.core.canonical_settings import flatten_settings
    from multi_league.core.league_refresh import (
        assert_provider_roster_merge,
        filter_rosters_to_finalized_games,
        merge_provider_refresh_table,
        needs_active_season_draft_fetch,
        replace_active_season_draft,
    )
    from multi_league.data_fetchers.sleeper.sleeper_draft import SleeperDraftFetcher
    from multi_league.data_fetchers.sleeper.sleeper_league_settings import fetch_sleeper_settings
    from multi_league.data_fetchers.sleeper.sleeper_matchups import SleeperMatchupFetcher
    from multi_league.data_fetchers.sleeper.sleeper_player_cache import SleeperPlayerCache
    from multi_league.data_fetchers.sleeper.sleeper_rosters import SleeperRosterFetcher
    from multi_league.data_fetchers.sleeper.sleeper_schedules import SleeperScheduleFetcher
    from multi_league.data_fetchers.sleeper.sleeper_transactions import SleeperTransactionFetcher

    league_id = str(ctx.get_league_id_for_year(active_year))
    raw_settings = fetch_sleeper_settings(client, league_id, active_year)
    if not raw_settings:
        raise RuntimeError(f"Sleeper returned no settings for {active_year} ({league_id})")
    settings = pd.DataFrame([flatten_settings(raw_settings, "sleeper", active_year, league_id)])
    settings["db_name"] = local_db.league_name
    merge_provider_refresh_table(
        local_db, "league_settings", settings, platform="sleeper", league_id=league_id
    )

    player_cache = SleeperPlayerCache(ctx.cache_directory)
    player_cache.refresh_if_stale(client)
    final_matchup_weeks = [week for week in refresh_weeks if _sleeper_week_is_final(active_league, week)]
    if final_matchup_weeks:
        matchups = SleeperMatchupFetcher(ctx, client).fetch_matchups_for_year(active_year, weeks=final_matchup_weeks)
        if not matchups.empty:
            merge_provider_refresh_table(
                local_db, "matchup", matchups, platform="sleeper", league_id=league_id
            )
        schedule = SleeperScheduleFetcher(ctx, client).fetch_schedule_for_year(active_year, weeks=final_matchup_weeks)
        if not schedule.empty:
            merge_provider_refresh_table(
                local_db, "schedule", schedule, platform="sleeper", league_id=league_id
            )
        matchup_rows = int(len(matchups))
        schedule_rows = int(len(schedule))
    else:
        print(f"[Sleeper] {active_year} weeks {refresh_weeks}: scoring leg still live; holding matchup/schedule rows")
        matchup_rows = 0
        schedule_rows = 0

    rosters = SleeperRosterFetcher(ctx, client, player_cache).fetch_season_rosters(
        active_year, weeks=refresh_weeks, db=local_db
    )
    roster_rows = 0
    for week in refresh_weeks:
        source = rosters.loc[rosters["week"].astype(int) == int(week)].copy() if not rosters.empty else pd.DataFrame()
        safe_rows = filter_rosters_to_finalized_games(
            source,
            finalized_ops.loc[finalized_ops["week"].astype(int) == int(week)],
        )
        if safe_rows.empty:
            continue
        merge_provider_refresh_table(
            local_db,
            "player_fantasy",
            safe_rows,
            platform="sleeper",
            league_id=league_id,
        )
        assert_provider_roster_merge(
            local_db,
            safe_rows,
            year=active_year,
            week=week,
            provider_id_column="sleeper_player_id",
        )
        roster_rows += len(safe_rows)

    transactions = SleeperTransactionFetcher(ctx, client, player_cache).fetch_transactions_for_year(
        active_year,
        db=local_db,
        max_week=max(refresh_weeks),
    )
    if not transactions.empty:
        merge_provider_refresh_table(
            local_db,
            "transactions",
            transactions,
            platform="sleeper",
            league_id=league_id,
        )
    draft_rows = 0
    draft_fetcher = SleeperDraftFetcher(ctx, client, player_cache)
    has_hydrated_draft = local_db.table_exists("draft") and int(local_db.row_count("draft") or 0) > 0
    draft_manifest = draft_fetcher.fetch_draft_manifest_for_year(active_year) if has_hydrated_draft else None
    if needs_active_season_draft_fetch(
        local_db,
        provider_manifest=draft_manifest,
        manifest_key_columns=("draft_id", "pick") if draft_manifest is not None else (),
    ):
        draft = draft_fetcher.fetch_draft_for_year(active_year)
        if not draft.empty:
            draft_rows = replace_active_season_draft(
                local_db,
                draft,
                year=active_year,
                platform="sleeper",
                league_id=league_id,
            )
    else:
        print(f"[Sleeper] Verified hydrated {active_year} draft against provider manifest", flush=True)
    return {
        "roster_rows": int(roster_rows),
        "final_matchup_rows": matchup_rows,
        "final_matchup_weeks": len(final_matchup_weeks),
        "schedule_rows": schedule_rows,
        "transaction_rows": int(len(transactions)),
        "draft_rows": draft_rows,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="Sleeper league db_name")
    parser.add_argument("--year", type=int, default=None, help="Active NFL season (default: latest Fly ops season)")
    parser.add_argument("--through-week", type=int, default=None, help="Optional finalized-week ceiling, e.g. 1")
    parser.add_argument("--observed-manifest-digest")
    parser.add_argument(
        "--league-id",
        help="Current Sleeper league ID from the onboarding chain; persisted after a successful refresh",
    )
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
        _capture_update_source_frames,
        _run_local_pipeline,
        _scope_counts,
    )

    reader = FlyReader()
    if args.execute:
        from multi_league.core.league_update_status import assert_league_update_entitled

        assert_league_update_entitled(reader, database_name=args.db)
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
        ctx, context_path, client, active_league = _build_context(
            reader=reader,
            db_name=args.db,
            active_year=active_year,
            work_dir=work_dir,
            active_league_id=args.league_id,
        )
        if ctx is None:
            receipt["status"] = "NO_ACTIVE_RENEWAL"
            _write_receipt(receipt, args.json_out)
            return 0
        receipt["league_id"] = str(active_league["league_id"])
        source_frames, base_generation = _capture_update_source_frames(
            reader,
            db_name=args.db,
            tables=UPDATE_REFRESH_SOURCE_TABLES,
        )
        receipt["base_generation"] = base_generation
        if source_frames["league_context"].empty or source_frames["league_settings"].empty:
            raise RuntimeError(f"Fly has no reusable context/settings for {args.db}")
        from multi_league.core.league_update_ownership import source_preservation_snapshot

        preservation_witnesses = source_preservation_snapshot(source_frames)
        local_db = LocalLeagueDB(work_dir, args.db)
        try:
            receipt["hydrated_rows"] = hydrate_local_refresh_sources(
                local_db,
                source_frames,
                db_name=args.db,
                active_year=active_year,
                expected_platform="sleeper",
            )
            from multi_league.core.league_update_ownership import local_preservation_snapshot

            preservation_before = local_preservation_snapshot(local_db, preservation_witnesses)
            receipt["fetch_rows"] = _merge_active_payloads(
                ctx=ctx,
                client=client,
                active_league=active_league,
                local_db=local_db,
                active_year=active_year,
                refresh_weeks=refresh_weeks,
                finalized_ops=finalized_ops,
            )
            if not args.execute:
                receipt["status"] = "DRY_RUN_READY"
                _write_receipt(receipt, args.json_out)
                return 0

            active_connection = local_db.connect()
            receipt["player_bio_sync"] = sync_player_bio_cache_from_fly(
                reader,
                ops_cache=Path(os.environ.get("OPS_CACHE_PATH", "")),
                platform="sleeper",
                provider_ids=active_platform_player_ids(active_connection, platform="sleeper"),
                player_names=active_platform_player_names(active_connection, platform="sleeper"),
                provider_name_hints=active_platform_player_name_hints(active_connection, platform="sleeper"),
            )
            receipt["ops_cache"] = str(
                _ensure_ops_cache_matches_live(
                    reader, finalized_ops, year=active_year, weeks=refresh_weeks, work_dir=work_dir
                )
            )
            _run_local_pipeline(
                ctx=ctx,
                context_path=context_path,
                local_db=local_db,
                db_name=args.db,
                active_year=active_year,
                work_dir=work_dir,
                platform="sleeper",
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
                bundle = build_fleet_partition_bundle(
                    stage,
                    active_year=active_year,
                    league_generations={args.db: base_generation},
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
                raise RuntimeError(f"scoped Sleeper refresh did not commit: {result}")
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
