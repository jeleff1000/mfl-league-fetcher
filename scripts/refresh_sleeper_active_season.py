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


def sleeper_source_manifest_complete(*, refresh_weeks: list[int], fetch_rows: dict[str, Any]) -> bool:
    """Only a wholly admitted provider/NFL scope may advance persisted freshness."""
    return (
        fetch_rows.get("draft_validated") is True
        and "pending_nfl_teams" in fetch_rows
        and not fetch_rows["pending_nfl_teams"]
        and int(fetch_rows.get("final_matchup_weeks") or 0) == len(refresh_weeks)
    )


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _sleeper_confirmed_no_draft(active_league: dict[str, Any], manifest: pd.DataFrame) -> bool:
    """An empty draft is authoritative only when both provider surfaces say so."""
    if "draft_id" not in active_league:
        raise ValueError("Sleeper active league omitted its draft_id field")
    return manifest.empty and not str(active_league.get("draft_id") or "").strip()


def _active_sleeper_roster_scope(rosters: pd.DataFrame) -> pd.DataFrame:
    """Expose Sleeper's raw ``points`` under the shared refresh score field.

    The normalizer performs this rename later, before writing canonical
    ``player_fantasy``.  The active-refresh completeness gate runs earlier so
    it can hold rows from unfinalized NFL games; it therefore needs the same
    scoring field without changing the fetcher's broad-import contract.
    """
    scoped = rosters.copy()
    if "fantasy_points" not in scoped.columns and "points" in scoped.columns:
        scoped["fantasy_points"] = scoped["points"]
    return scoped


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
        if str(current.get("league_id") or "") != current_id:
            return False
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


def _load_persisted_sleeper_chain(
    reader: Any,
    *,
    db_name: str,
    active_year: int | None = None,
) -> tuple[dict[str, Any], dict[str, str]]:
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
    if "platform" in context_columns:
        context_select.insert(0, "platform")
    if "league_ids_json" in context_columns:
        context_select.insert(2, "league_ids_json")
    context_rows = reader.query(
        f"SELECT {', '.join(context_select)} FROM public.league_context WHERE db_name = {quoted_db}",
        database="___leagues",
    )
    if not context_rows:
        raise RuntimeError(f"Fly has no Sleeper league context for {db_name}")
    context = context_rows[0]
    # A multiplatform context may retain another provider's onboarding IDs.
    # Only matching-platform context IDs can seed Sleeper renewal discovery;
    # per-year Sleeper settings remain the canonical imported witness.
    context_platform = str(context.get("platform") or "").strip().lower()
    known = _json_map(context.get("league_ids_json")) if context_platform in ("", "sleeper") else {}
    setting_rows = reader.query(
        "SELECT year, platform, league_key FROM public.league_settings "
        f"WHERE db_name = {quoted_db} AND platform IS NOT NULL",
        database="___leagues",
    )
    imported_owners: dict[str, set[str]] = {}
    for row in setting_rows:
        try:
            year = str(int(row["year"]))
        except (TypeError, ValueError):
            continue
        imported_owners.setdefault(year, set()).add(str(row.get("platform") or "").strip().lower())
    if any("sleeper" in owners and owners != {"sleeper"} for owners in imported_owners.values()):
        raise RuntimeError("Fly has overlapping provider ownership for a Sleeper season")
    known = {
        year: league_id for year, league_id in known.items()
        if not imported_owners.get(year) or imported_owners[year] == {"sleeper"}
    }
    for row in setting_rows:
        if str(row.get("platform") or "").strip().lower() != "sleeper":
            continue
        try:
            year = str(int(row["year"]))
        except (TypeError, ValueError):
            continue
        league_id = str(row.get("league_key") or "").strip()
        if league_id:
            if year in known and known[year] != league_id:
                raise RuntimeError("Fly has conflicting Sleeper league IDs for one imported season")
            known.setdefault(year, league_id)
    if active_year is not None:
        from multi_league.core.league_update_lineage import resolve_active_update_segment

        # The full import persists every provider leg in league_settings.
        # Resolve that timeline before any Sleeper API call so a Yahoo/ESPN
        # continuation cannot be accidentally fetched by this worker.
        resolve_active_update_segment(
            active_year=int(active_year),
            context_platform=context.get("platform"),
            context_league_id=context.get("league_id"),
            settings_rows=setting_rows,
            expected_platform="sleeper",
        )
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

    frontend, known_league_ids = _load_persisted_sleeper_chain(
        reader,
        db_name=db_name,
        active_year=active_year,
    )
    if active_league_id:
        saved_active_id = str(known_league_ids.get(str(active_year)) or "").strip()
        if saved_active_id and saved_active_id != str(active_league_id).strip():
            raise RuntimeError("Fly and caller have conflicting active Sleeper league IDs")
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
        pending_provider_nfl_teams,
        refresh_authoritative_draft_partition,
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
    matchups = pd.DataFrame()
    schedule = pd.DataFrame()
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
    rosters = _active_sleeper_roster_scope(rosters)
    roster_rows = 0
    pending_nfl_teams: set[str] = set()
    for week in refresh_weeks:
        source = rosters.loc[rosters["week"].astype(int) == int(week)].copy() if not rosters.empty else pd.DataFrame()
        pending_nfl_teams.update(pending_provider_nfl_teams(
            source,
            finalized_ops.loc[finalized_ops["week"].astype(int) == int(week)],
        ))
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
    draft_manifest = draft_fetcher.fetch_draft_manifest_for_year(
        active_year, expected_primary_draft_id=str(active_league.get("draft_id") or "").strip()
    )
    draft_rows = refresh_authoritative_draft_partition(
        local_db,
        provider_manifest=draft_manifest,
        key_columns=("draft_id", "pick"),
        fetch_full=lambda: draft_fetcher.fetch_draft_for_year(active_year),
        year=active_year,
        platform="sleeper",
        league_id=league_id,
        confirmed_no_draft=_sleeper_confirmed_no_draft(active_league, draft_manifest),
    )
    from multi_league.core.league_update_validation import (
        IncompleteSourceError,
        validate_tabular_active_scope,
    )

    provider_rosters = client.get_league_rosters(league_id)
    expected_ids = tuple(
        str(roster.get("roster_id") or "").strip()
        for roster in provider_rosters
    )
    if len(expected_ids) != int(active_league.get("total_rosters") or 0):
        raise IncompleteSourceError(
            "Sleeper roster identity count disagrees with active league settings"
        )
    draft = local_db.read_table("draft", year=active_year) if local_db.table_exists("draft") else pd.DataFrame()
    validation = validate_tabular_active_scope(
        provider="sleeper",
        league_id=league_id,
        season=active_year,
        expected_team_ids=expected_ids,
        requested_weeks=tuple(refresh_weeks),
        finalized_weeks=tuple(final_matchup_weeks),
        player_id_column="sleeper_player_id",
        rosters=rosters,
        matchups=matchups,
        schedule=schedule,
        draft=draft,
    )
    return {
        "roster_rows": int(roster_rows),
        "final_matchup_rows": matchup_rows,
        "final_matchup_weeks": len(final_matchup_weeks),
        "schedule_rows": schedule_rows,
        "transaction_rows": int(len(transactions)),
        "draft_rows": draft_rows,
        "draft_validated": True,
        "pending_nfl_teams": sorted(pending_nfl_teams),
        "provider_validation": validation,
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
        active_nfl_player_ids,
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
    from multi_league.core.league_update_timing import PhaseTimer
    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.core.targets.fly_target import FlyTarget
    from scripts.refresh_yahoo_active_season import (
        UPDATE_REFRESH_SOURCE_TABLES,
        OPS_DATABASE,
        _ensure_ops_cache_matches_live,
        _load_active_refresh_inputs,
        _active_update_segment_from_source_frames,
        _capture_update_source_frames,
        _run_local_pipeline,
        _scope_counts,
        _split_active_transform_source_frames,
    )

    timer = PhaseTimer()
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
        ctx, context_path, client, active_league = _build_context(
            reader=reader,
            db_name=args.db,
            active_year=active_year,
            work_dir=work_dir,
            active_league_id=args.league_id,
        )
        if ctx is None:
            receipt["status"] = "NO_ACTIVE_RENEWAL"
            receipt["phase_seconds"] = timer.finish()
            _write_receipt(receipt, args.json_out)
            return 0
        receipt["league_id"] = str(active_league["league_id"])
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
            expected_platform="sleeper",
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
        local_db = LocalLeagueDB(work_dir, args.db)
        try:
            receipt["hydrated_rows"] = hydrate_local_refresh_sources(
                local_db,
                transform_source_frames,
                db_name=args.db,
                active_year=active_year,
                expected_platform="sleeper",
            )
            # The local transform input intentionally contains only the active
            # season.  Compare the finished rebuild to the full Fly snapshot,
            # otherwise correctly restored historical source rows appear new.
            preservation_before = preservation_witnesses
            timer.mark("local_hydration")
            receipt["fetch_rows"] = _merge_active_payloads(
                ctx=ctx,
                client=client,
                active_league=active_league,
                local_db=local_db,
                active_year=active_year,
                refresh_weeks=refresh_weeks,
                finalized_ops=finalized_ops,
            )
            receipt["source_manifest_complete"] = sleeper_source_manifest_complete(
                refresh_weeks=refresh_weeks,
                fetch_rows=receipt["fetch_rows"],
            )
            timer.mark("provider_fetch")
            if not args.execute:
                receipt["status"] = "DRY_RUN_READY"
                receipt["phase_seconds"] = timer.finish()
                _write_receipt(receipt, args.json_out)
                return 0

            from multi_league.core.league_update_validation import (
                assert_transformed_active_matchup_scope,
                assert_transformed_active_player_scope,
                capture_active_final_matchup_scope,
                capture_active_provider_player_scope,
            )
            expected_player_keys = capture_active_provider_player_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="sleeper_player_id",
            )
            expected_matchup_scores = capture_active_final_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year, weeks=refresh_weeks,
            )

            active_connection = local_db.connect()
            receipt["player_bio_sync"] = sync_player_bio_cache_from_fly(
                reader,
                ops_cache=Path(os.environ.get("OPS_CACHE_PATH", "")),
                platform="sleeper",
                provider_ids=active_platform_player_ids(active_connection, platform="sleeper"),
                player_names=active_platform_player_names(active_connection, platform="sleeper"),
                nfl_player_ids=active_nfl_player_ids(active_connection),
                provider_name_hints=active_platform_player_name_hints(active_connection, platform="sleeper"),
            )
            receipt["ops_cache"] = str(
                _ensure_ops_cache_matches_live(
                    reader, finalized_ops, year=active_year, weeks=refresh_weeks, work_dir=work_dir
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
                platform="sleeper",
                keeper_config_hydrated="keeper_config" in transform_source_frames,
                historical_source_rows=historical_source_rows,
                frontend_configuration_rows=preservation_witnesses,
            )
            timer.mark("shared_transformations")
            receipt["transformed_player_scope"] = assert_transformed_active_player_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="sleeper_player_id",
                expected_keys=expected_player_keys,
            )
            receipt["transformed_matchup_scope"] = assert_transformed_active_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, expected_scores=expected_matchup_scores,
            )
            local_db.connect()
            from multi_league.core.homepage_refresh import prepare_homepage_refresh

            stage_timer = PhaseTimer()
            homepage = prepare_homepage_refresh(
                reader=reader,
                local_db=local_db,
                db_name=args.db,
                active_year=active_year,
            )
            stage_timer.mark("homepage_refresh")
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
            publish_tables = active_refresh_publish_tables(local_db.connect())
            publish_tables = sorted(set([*publish_tables, *homepage["published_tables"]]))
            from multi_league.core.league_update_validation import assert_refresh_derived_output_health

            receipt["derived_health"] = assert_refresh_derived_output_health(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="sleeper_player_id",
                published_tables=publish_tables,
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
                )
            finally:
                stage.close()
            stage_timer.mark("bundle_build")
            receipt["homepage_preservation_stage_seconds"] = stage_timer.finish()
            timer.mark("homepage_preservation_stage")
            from multi_league.core.league_update_publish_claim import renew_claim_for_publication

            renew_claim_for_publication(reader, database_name=args.db, platform="sleeper")
            timer.mark("prepublish_claim")
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
            receipt["career_rollups"] = result.get("career_rollups", {}).get(args.db, {})
            receipt["career_seconds"] = result.get("career_seconds", {}).get(args.db)
            receipt["published_tables"] = sorted(set(publish_tables) | set(receipt["career_rollups"]))
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
