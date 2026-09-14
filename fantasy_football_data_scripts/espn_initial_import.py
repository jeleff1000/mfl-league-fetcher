#!/usr/bin/env python3
"""
ESPN INITIAL IMPORT - Unified Multi-League Pipeline for ESPN Fantasy

Parallel to sleeper_initial_import.py but for ESPN platform.
Uses espn_api library + raw ESPN API for private leagues.

TWO-TRACK ARCHITECTURE:
- Track 1: Updates NFL super table (shared with Yahoo/Sleeper pipeline)
- Track 2: Uploads league tables to Fly via shared helpers

Join Key: player_week = {NFL_player_id}_{year}_{week}

What this does:
1) Track 1: Verify NFL super table has required data (shared)
2) Phase 0: Discovery (connect to ESPN, build manager maps)
3) Phase 1: Fetch ESPN data (matchups, rosters, draft, transactions, schedules)
4) Phase 2: Merge ESPN+NFL data, normalize to canonical schemas
5) Phase 3: Transformations (cumulative stats, LAMAR, keeper economics, etc.)
6) Phase 4: Track 2 upload to Fly
7) Phase 5: Fantasy aggregation

Usage:
  python espn_initial_import.py --context path/to/espn_context.json
  python espn_initial_import.py --context path/to/espn_context.json --dry-run
  python espn_initial_import.py --context path/to/espn_context.json --skip-fetchers
  python espn_initial_import.py --context path/to/espn_context.json --skip-track-1
"""

from __future__ import annotations

import argparse
import sys

_verbose = "--verbose" in sys.argv

from pathlib import Path

import pandas as pd

# Add script directory to path
SCRIPT_DIR = Path(__file__).parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

# Import ESPN modules
from multi_league.data_fetchers.espn import (
    ESPNContext,
    ESPNAPIClient,
    build_manager_names,
    fetch_all_espn_matchups,
    fetch_all_espn_rosters,
    fetch_all_espn_drafts,
    fetch_all_espn_transactions,
    fetch_and_save_all_settings,
    normalize_matchup_data,
    normalize_draft_data,
    normalize_transaction_data,
)

# Import shared utilities
from multi_league.core.script_runner import log, run_script
from multi_league.core.date_utils import get_current_nfl_season_year, get_nfl_state
from multi_league.core.fetch_runtime import runtime_from_source
from multi_league.core.import_utils import (
    run_track_1_verify,
    upload_league_tables as _shared_upload_league_tables,
)
from multi_league.core.import_pipeline import (
    require_sql_enrichment_success,
    run_local_fantasy_aggregation,
    run_transformation_pipeline,
)
from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.canonical_settings import flatten_settings
from multi_league.core.scoring_variant import derive_scoring_variant
from multi_league.core.data_normalization import harmonize_dtypes
from multi_league.core.year_filter_utils import (
    coerce_int,
    current_state_shell_has_scores,
    format_year_filter,
    resolve_history_years,
    resolve_quick_import_years,
    unscored_current_shell_years,
)
from multi_league.data_fetchers.shared.staging_reader import (
    clear_staging_tables,
    pre_fetch_staging_settings,
)
from multi_league.data_fetchers.shared.staging_data_merger import merge_staging_data


def _derive_settings_variant(row: dict) -> str:
    """Derive the precomputed LAMAR variant key from flattened canonical settings."""
    return derive_scoring_variant(
        num_teams=row.get("num_teams", 12),
        has_superflex=row.get("roster_SUPER_FLEX") or row.get("roster_OP") or row.get("roster_Q/W/R/T"),
        has_idp=row.get("roster_DL") or row.get("roster_LB") or row.get("roster_DB"),
        pass_td_pts=row.get("scoring_pass_td", 4),
        ppr=row.get("scoring_rec", 0.5),
        te_premium=row.get("scoring_bonus_rec_te", 0),
    )


def _resolve_espn_quick_import_years(
    ctx: ESPNContext,
    target_year: int,
    nfl_state: dict | None = None,
    available_years: list[int] | set[int] | tuple[int, ...] | None = None,
) -> list[int]:
    """Resolve ESPN quick years using context mappings plus discovered scored years."""
    extra_years = [target_year, *(available_years or [])]
    state = nfl_state or {}
    state_season = coerce_int(state.get("league_season") or state.get("season"))
    previous_season = coerce_int(state.get("previous_season"))
    if state_season == int(target_year) and not current_state_shell_has_scores(state) and previous_season:
        # ESPN league IDs are stable across seasons, so the previous scored
        # season is safe to fetch even when discovery has not populated it yet.
        extra_years.append(previous_season)

    history_years = resolve_history_years(
        ctx.league_ids,
        start_year=ctx.start_year,
        end_year=ctx.end_year,
        cap_year=max(target_year, get_current_nfl_season_year()),
        extra_years=extra_years,
    )
    return resolve_quick_import_years(
        history_years,
        target_year,
        nfl_state,
        include_previous_available=True,
    )


def _apply_espn_quick_years(ctx: ESPNContext, quick_years: list[int]) -> None:
    ctx.quick_import_years = sorted({int(year) for year in quick_years})
    ctx.start_year = min(ctx.quick_import_years)
    ctx.end_year = max(ctx.quick_import_years)


def _discover_espn_import_years(ctx: ESPNContext) -> list[int]:
    """Discover requested ESPN seasons with the league ID assigned to each season."""
    years_by_league_id: dict[int, list[int]] = {}
    for year in ctx.get_year_range():
        parsed_year = int(year)
        league_id = int(ctx.get_league_id_for_year(parsed_year))
        years_by_league_id.setdefault(league_id, []).append(parsed_year)

    available_years: list[int] = []
    for league_id, years in years_by_league_id.items():
        year_client = ESPNAPIClient(league_id, ctx.espn_s2, ctx.swid)
        available_years.extend(year_client.discover_available_years(years=years))
    return sorted(set(available_years))


def main():
    parser = argparse.ArgumentParser(description="Complete data import for an ESPN fantasy league")
    parser.add_argument("--context", required=True, help="Path to espn_context.json")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing")
    parser.add_argument("--skip-fetchers", action="store_true", help="Skip data fetchers (use existing files)")
    parser.add_argument("--skip-transformations", action="store_true", help="Skip transformations")
    parser.add_argument("--start-phase", type=int, choices=[0, 1, 2, 3], help="Start at specific phase (0=discovery)")
    parser.add_argument("--skip-track-1", action="store_true", help="Skip NFL super table verification")
    parser.add_argument("--skip-track-2-upload", action="store_true", help="Skip Fly upload")
    parser.add_argument(
        "--import-mode",
        choices=["quick", "full"],
        default="full",
        help="Import mode: 'quick' = target season refresh, 'full' = all years",
    )
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    # Targeted fetch flags (match Yahoo v3 pattern)
    parser.add_argument(
        "--fetch",
        type=str,
        choices=["matchups", "rosters", "draft", "transactions"],
        help="Targeted fetch: run only this fetcher (requires --year)",
    )
    parser.add_argument("--year", type=int, default=None, help="Targeted fetch: specific year")
    parser.add_argument("--with-transforms", action="store_true", help="Run transformations after targeted fetch")
    args = parser.parse_args()

    context_path = Path(args.context).resolve()
    if not context_path.exists():
        log(f"[FAIL] ESPN context not found: {context_path}")
        sys.exit(1)

    # Load context
    ctx = ESPNContext.load(str(context_path))
    ctx.import_mode = args.import_mode

    # --skip-fetchers skips Phase 1 (data fetchers) but NOT Phase 0 (discovery/settings)
    start_phase = args.start_phase if args.start_phase is not None else 1

    log("=" * 96)
    log("ESPN INITIAL IMPORT - Unified Pipeline")
    log("=" * 96)
    log(f"League: {ctx.league_name} | League ID: {ctx.league_id}")
    log(f"Mode:   {args.import_mode.upper()}")
    log(f"Years:  {ctx.start_year} - {ctx.end_year or 'current'}")
    log(f"Root:   {ctx.data_directory}")
    log(f"Auth:   {'Configured' if ctx.espn_s2 else 'Public'}")
    log(f"DryRun: {'Yes' if args.dry_run else 'No'}")
    log(f"Track 1 (NFL):  {'SKIP' if args.skip_track_1 else 'ENABLED'}")
    log(f"Track 2 Upload: {'SKIP' if args.skip_track_2_upload else 'ENABLED'}")
    log(f"Start Phase: {start_phase}")
    log("=" * 96)

    results: dict[str, list[tuple[str, bool]]] = {
        "track1": [],
        "settings": [],
        "fetchers": [],
        "merges": [],
        "track2": [],
        "transformations": [],
    }

    end_year = ctx.end_year or get_current_nfl_season_year()
    is_quick_import = args.import_mode == "quick"
    nfl_state: dict = {}
    quick_years = [end_year] if is_quick_import else []
    unscored_quick_years: set[int] = set()
    if is_quick_import:
        try:
            nfl_state = get_nfl_state() or {}
        except Exception as e:
            log(f"[QUICK IMPORT] Could not fetch NFL state for year selection: {e}")
            nfl_state = {}
        quick_years = _resolve_espn_quick_import_years(ctx, int(end_year), nfl_state)
        _apply_espn_quick_years(ctx, quick_years)
        end_year = ctx.end_year or end_year
        unscored_quick_years = unscored_current_shell_years(quick_years, nfl_state)
        log(f"[QUICK IMPORT] Target import years: {format_year_filter(quick_years)}")
    year_filter = end_year if is_quick_import else None

    # Initialize API client
    client = ESPNAPIClient(ctx.league_id, ctx.espn_s2, ctx.swid)

    # =========================================================================
    # INITIALIZE LOCAL LEAGUE DB
    # =========================================================================
    runtime = runtime_from_source(ctx, context_path=context_path)
    data_dir = runtime.data_dir
    db_name = runtime.db_name
    db = LocalLeagueDB(data_dir, db_name)
    db.connect()
    for table in ["matchup", "player_fantasy", "draft", "transactions", "schedule", "league_settings"]:
        db.ensure_table(table)
    log(f"[LOCAL DB] Initialized {db.db_path}")

    # =========================================================================
    # TARGETED FETCH MODE (--fetch --year)
    # Runs a single fetcher for one year, saves to local DB, optionally uploads.
    # =========================================================================
    if args.fetch:
        if args.year is None:
            log("[TARGETED FETCH][FAIL] --year is required when using --fetch")
            db.close()
            sys.exit(1)

        log("=" * 96)
        log("TARGETED FETCH MODE")
        log("=" * 96)
        log(f"League:  {ctx.league_name}")
        log(f"Fetcher: {args.fetch}")
        log(f"Year:    {args.year}")
        log(f"Transforms: {'Yes' if args.with_transforms else 'No'}")
        log("=" * 96)

        _FETCH_MAP = {
            "matchups": ("matchup", lambda yr: fetch_all_espn_matchups(ctx)),
            "rosters": ("player_fantasy", lambda yr: fetch_all_espn_rosters(ctx, db=db)),
            "draft": ("draft", lambda yr: fetch_all_espn_drafts(ctx)),
            "transactions": ("transactions", lambda yr: fetch_all_espn_transactions(ctx)),
        }

        table_name, fetch_fn = _FETCH_MAP[args.fetch]
        try:
            # ESPN fetchers don't have year_filter — ctx year range controls what's fetched
            # Set context to single year for targeted fetch
            ctx.start_year = args.year
            ctx.end_year = args.year

            df = fetch_fn(args.year)
            row_count = len(df) if df is not None else 0
            log(f"[TARGETED FETCH] {args.fetch} year={args.year}: {row_count:,} rows")

            # Rosters already saved to db by fetcher; others need explicit save
            if args.fetch != "rosters" and df is not None and not df.empty:
                db.save_table(table_name, df, platform="espn", league_id=str(ctx.league_id))
                log(f"[TARGETED FETCH] Saved to local DB: {table_name}")

            if not args.skip_track_2_upload and not args.dry_run:
                log(f"[TARGETED FETCH] Uploading to Fly ({db_name})...")
                db.upload_to_fly(db_name)
                log("[TARGETED FETCH] Upload complete")

            if args.with_transforms and not args.dry_run:
                log("[TARGETED FETCH] Running SQL enrichments (local)...")
                from multi_league.transformations.sql_enrichments import SQLEnrichments

                eng = SQLEnrichments(db_name=db_name, data_dir=str(data_dir), quick=is_quick_import, conn=db.connect())
                eng.load_settings_from_db()
                eng.run_all()
                eng.close()  # no-op — db owns the connection

            log("[TARGETED FETCH] SUCCESS")
        except Exception as e:
            log(f"[TARGETED FETCH] FAILED: {e}")
            db.close()
            sys.exit(2)

        db.close()
        sys.exit(0)

    # =========================================================================
    # PHASE 0 PRE: Fresh Start (Quick imports only)
    # =========================================================================
    # NOTE: No early FRESH START delete against ___leagues. The final
    # upload_to_fly() handles DELETE WHERE db_name + INSERT
    # atomically per table.

    # =========================================================================
    # PHASE 0: Discovery
    # =========================================================================
    if start_phase <= 1:
        log("\n" + "=" * 96)
        log("PHASE 0: ESPN League Discovery")
        log("=" * 96)

        # Discover available years if not already set
        if not ctx.team_to_manager:
            log("[DISCOVERY] Connecting to ESPN league...")
            try:
                # Discover available years
                explicit_import_years = sorted({int(y) for y in getattr(ctx, "explicit_import_years", []) or []})
                available_years = _discover_espn_import_years(ctx)
                if available_years:
                    log(f"[DISCOVERY] Found data for years: {available_years}")
                    if is_quick_import:
                        target_year = max(quick_years) if quick_years else int(end_year)
                        resolved_quick_years = _resolve_espn_quick_import_years(
                            ctx,
                            target_year,
                            nfl_state,
                            available_years=available_years,
                        )
                        if resolved_quick_years != quick_years:
                            log(
                                f"[DISCOVERY] Quick import target years: "
                                f"{format_year_filter(quick_years)} -> {format_year_filter(resolved_quick_years)}"
                            )
                        quick_years = resolved_quick_years
                        _apply_espn_quick_years(ctx, quick_years)
                        end_year = ctx.end_year or end_year
                        unscored_quick_years = unscored_current_shell_years(quick_years, nfl_state)
                    else:
                        if explicit_import_years:
                            scoped_years = [year for year in available_years if year in set(explicit_import_years)]
                            missing_years = sorted(set(explicit_import_years) - set(scoped_years))
                            if missing_years:
                                log(f"[DISCOVERY] Scoped years unavailable from ESPN, skipping: {missing_years}")
                            if scoped_years:
                                ctx.explicit_import_years = scoped_years
                                ctx.start_year = min(scoped_years)
                                ctx.end_year = max(scoped_years)
                        # Full imports fetch only the verified seasons, but keep the
                        # original requested range intact for user-facing progress and
                        # any cross-platform merge metadata.  A current ESPN ID can
                        # legitimately return 404 for earlier IDs in a renewal chain.
                        if not explicit_import_years:
                            ctx.explicit_import_years = available_years

                # Build manager mappings from latest year
                league = client.get_league(ctx.end_year or end_year)
                if league and league.teams:
                    ctx.num_teams = len(league.teams)
                    log(f"[DISCOVERY] League has {ctx.num_teams} teams")

                    # Build manager names using dedup logic
                    ctx.team_to_manager = build_manager_names(league.teams)

                    # Build GUID mapping
                    for team in league.teams:
                        team_id = team.team_id
                        owners = getattr(team, "owners", []) or []
                        if owners:
                            owner = owners[0] if isinstance(owners, list) else owners
                            if isinstance(owner, dict):
                                guid = owner.get("id", "")
                            else:
                                guid = getattr(owner, "id", "")
                            if guid:
                                ctx.team_to_guid[team_id] = guid

                    # For combined leagues, discover managers from additional league IDs
                    if ctx.league_ids:
                        seen_league_ids = {ctx.league_id}
                        for lid in ctx.league_ids.values():
                            if lid in seen_league_ids:
                                continue
                            seen_league_ids.add(lid)
                            try:
                                alt_client = ESPNAPIClient(lid, ctx.espn_s2, ctx.swid)
                                # Find which year this league ID covers
                                alt_year = next(
                                    (int(y) for y, l in ctx.league_ids.items() if l == lid), ctx.end_year or end_year
                                )
                                alt_league = alt_client.get_league(alt_year)
                                if alt_league and alt_league.teams:
                                    alt_managers = build_manager_names(alt_league.teams)
                                    for tid, name in alt_managers.items():
                                        if tid not in ctx.team_to_manager:
                                            ctx.team_to_manager[tid] = name
                                    for team in alt_league.teams:
                                        tid = team.team_id
                                        if tid not in ctx.team_to_guid:
                                            owners = getattr(team, "owners", []) or []
                                            if owners:
                                                owner = owners[0] if isinstance(owners, list) else owners
                                                guid = (
                                                    owner.get("id", "")
                                                    if isinstance(owner, dict)
                                                    else getattr(owner, "id", "")
                                                )
                                                if guid:
                                                    ctx.team_to_guid[tid] = guid
                                    log(f"[DISCOVERY] Added managers from league {lid}: {alt_managers}")
                            except Exception as e:
                                log(f"[DISCOVERY] Could not discover managers from league {lid}: {e}")

                    # Build per-year team name AND owner mappings (parallel)
                    from concurrent.futures import ThreadPoolExecutor, as_completed

                    def _fetch_year_teams(yr):
                        try:
                            yc = ESPNAPIClient(ctx.get_league_id_for_year(yr), ctx.espn_s2, ctx.swid)
                            yl = yc.get_league(yr)
                            if yl and yl.teams:
                                names = {}
                                guids = {}
                                for t in yl.teams:
                                    names[t.team_id] = getattr(t, "team_name", f"Team {t.team_id}")
                                    owners = getattr(t, "owners", []) or []
                                    if owners:
                                        o = owners[0] if isinstance(owners, list) else owners
                                        g = o.get("id", "") if isinstance(o, dict) else getattr(o, "id", "")
                                        if g:
                                            guids[t.team_id] = g
                                return (yr, names, guids, build_manager_names(yl.teams))
                        except Exception:
                            pass
                        return (yr, None, None, None)

                    team_years = quick_years if is_quick_import and quick_years else list(ctx.get_year_range())
                    with ThreadPoolExecutor(max_workers=min(5, max(1, len(team_years)))) as executor:
                        futures = [executor.submit(_fetch_year_teams, yr) for yr in team_years]
                        for f in as_completed(futures):
                            yr, names, guids, mgrs = f.result()
                            if names:
                                ctx.team_to_team_name[str(yr)] = names
                                ctx.team_to_manager_by_year[str(yr)] = mgrs
                                if guids:
                                    ctx.team_to_guid_by_year[str(yr)] = guids

                    log(f"[DISCOVERY] Managers: {ctx.team_to_manager}")

                ctx.save(str(context_path))
                log(f"[DISCOVERY] Context saved to {context_path}")

            except Exception as e:
                log(f"[DISCOVERY] Error: {e}")
                if not ctx.team_to_manager:
                    log("[DISCOVERY] FATAL: No manager mappings available")
                    sys.exit(1)
        else:
            log(f"[DISCOVERY] Using pre-configured context with {len(ctx.team_to_manager)} teams")

        if is_quick_import:
            _apply_espn_quick_years(ctx, quick_years)
            end_year = ctx.end_year or end_year
            log(f"[QUICK IMPORT] Processing {format_year_filter(quick_years)}")

        # -----------------------------------------------------------------
        # Check for staged settings from league merge
        # -----------------------------------------------------------------
        staging_settings_years: set[int] = set()
        try:
            staging_settings_years = pre_fetch_staging_settings(ctx, log_func=log)
        except Exception as e:
            log(f"[STAGING SETTINGS] WARN: pre-fetch failed: {e}")
            if getattr(ctx, "has_external_data", False):
                raise

        # Fetch settings, flatten to canonical schema, save to local DuckDB
        # Quick imports include the current shell plus prior scored season when needed.
        if is_quick_import:
            settings_years = quick_years
        else:
            settings_years = sorted(ctx.get_year_range())
        log(f"\n[SETTINGS] Fetching and flattening league settings for {len(settings_years)} year(s)...")
        if not args.dry_run:
            raw_settings = fetch_and_save_all_settings(ctx, years=settings_years if is_quick_import else None)
            if raw_settings:
                import json as _json

                settings_rows = []
                settings_dir = ctx.data_directory / "league_settings"
                for year in settings_years:
                    try:
                        # Read the JSON file that fetch_and_save_all_settings wrote
                        settings_files = list(settings_dir.glob(f"league_settings_{year}*.json"))
                        if settings_files:
                            with open(settings_files[0], encoding="utf-8") as f:
                                raw = _json.load(f)
                            league_key = ctx.get_league_id_for_year(year) or str(ctx.league_id)
                            settings_rows.append(
                                flatten_settings(raw, platform="espn", year=year, league_key=league_key)
                            )
                            log(f"  OK: {year}")
                    except Exception as e:
                        log(f"  FAIL: {year} - {e}")

                if settings_rows:
                    import polars as _pl

                    active_years = sorted({int(row["year"]) for row in settings_rows if row.get("year") is not None})
                    first_active_year = active_years[0]
                    last_active_year = active_years[-1]
                    import_mode = args.import_mode
                    scoring_variant_first_year = _derive_settings_variant(
                        next(row for row in settings_rows if int(row["year"]) == first_active_year)
                    )
                    scoring_variant = _derive_settings_variant(
                        next(row for row in settings_rows if int(row["year"]) == last_active_year)
                    )

                    for row in settings_rows:
                        row["first_active_year"] = first_active_year
                        row["last_active_year"] = last_active_year
                        row["import_mode"] = import_mode
                        row["scoring_variant"] = scoring_variant
                        row["scoring_variant_first_year"] = scoring_variant_first_year

                    log(
                        f"[SETTINGS] scoring_variant={scoring_variant} "
                        f"first_year={scoring_variant_first_year} "
                        f"years={first_active_year}-{last_active_year} import_mode={import_mode}"
                    )

                    for row in settings_rows:
                        db.save_table("league_settings", _pl.DataFrame([row]), year=row["year"])
                    log(f"[SETTINGS] Saved {len(settings_rows)} year(s) to local DuckDB (flat canonical schema)")
                results["settings"].append(("League Settings", True))
            else:
                results["settings"].append(("League Settings", False))
        else:
            log("  [DRY-RUN] Would fetch settings")
            results["settings"].append(("League Settings", True))

    # =========================================================================
    # TRACK 1: NFL Super Table Load
    # =========================================================================
    if not args.skip_track_1 and start_phase <= 1:
        log("\n" + "=" * 96)
        log("TRACK 1: NFL Super Table Load")
        log("=" * 96)

        if is_quick_import:
            track_1_years = [year for year in quick_years if year not in unscored_quick_years]
            if not track_1_years:
                track_1_years = [max(quick_years)]
            nfl_start = min(track_1_years)
            track_1_end_year = max(track_1_years)
        else:
            nfl_start = max(1999, ctx.start_year)
            track_1_end_year = end_year
        track1_ok = run_track_1_verify(nfl_start, track_1_end_year, dry_run=args.dry_run)
        results["track1"].append(("NFL Super Table Verify", track1_ok))

        if track1_ok and not args.dry_run:
            ok, err = run_script(
                "multi_league/data_fetchers/load_nfl_from_super_table.py",
                "NFL Super Table Load",
                str(context_path),
                additional_args=["--start-year", str(nfl_start), "--end-year", str(track_1_end_year)],
                timeout=300,
            )
            results["track1"].append(("NFL Super Table Load", ok))
            if ok:
                log("[TRACK 1] NFL stats loaded successfully")
            else:
                log("[TRACK 1] NFL stats load failed")

    # =========================================================================
    # PHASE 1: ESPN Data Fetchers
    # =========================================================================
    # For quick import, narrow year range to the resolved quick years.
    # Settings already fetched for all years in Phase 0.
    _original_start_year = ctx.start_year
    _original_end_year = ctx.end_year
    if is_quick_import:
        _apply_espn_quick_years(ctx, quick_years)
        log(f"[QUICK IMPORT] Narrowing fetcher year range to {format_year_filter(quick_years)}")

    if start_phase <= 1 and not args.skip_fetchers:
        log("\n" + "=" * 96)
        log("PHASE 1: ESPN Data Fetchers (staged)")
        log("=" * 96)

        if args.dry_run:
            for name in ["Matchups", "Rosters", "Draft", "Transactions", "Schedules"]:
                log(f"  [DRY-RUN] Would fetch {name}")
                results["fetchers"].append((name, True))
        else:
            # STAGED PIPELINE: Rosters need draft + transactions in local DuckDB
            # for ghost roster resolution (ownership map). Run in dependency order:
            #   Stage 1: Matchups (independent, no DB dependencies)
            #   Stage 2: Draft → save to DB
            #   Stage 3: Transactions → save to DB
            #   Stage 4: Rosters (reads draft + transactions from DB for ownership)

            fetcher_dfs: dict[str, pd.DataFrame] = {}

            # --- Stage 1: Matchups (independent) ---
            log("\n[FETCHERS] Stage 1: Matchups...")
            try:
                matchups_df = fetch_all_espn_matchups(ctx)
                log(f"  OK: Matchups - {len(matchups_df):,} rows")
                results["fetchers"].append(("Matchups", True))
                fetcher_dfs["Matchups"] = matchups_df
            except Exception as e:
                log(f"  FAIL: Matchups - {e}")
                results["fetchers"].append(("Matchups", False))

            # --- Stage 2: Draft → save to DB ---
            log("\n[FETCHERS] Stage 2: Draft...")
            try:
                draft_df = fetch_all_espn_drafts(ctx)
                log(f"  OK: Draft - {len(draft_df):,} rows")
                results["fetchers"].append(("Draft", True))
                fetcher_dfs["Draft"] = draft_df
                if not draft_df.empty:
                    db.save_table("draft", draft_df, platform="espn", league_id=str(ctx.league_id))
                    log(f"  [LOCAL DB] draft: {len(draft_df):,} rows written")
            except Exception as e:
                log(f"  FAIL: Draft - {e}")
                results["fetchers"].append(("Draft", False))

            # --- Stage 3: Transactions → save to DB ---
            log("\n[FETCHERS] Stage 3: Transactions...")
            try:
                txn_df = fetch_all_espn_transactions(ctx)
                log(f"  OK: Transactions - {len(txn_df):,} rows")
                results["fetchers"].append(("Transactions", True))
                fetcher_dfs["Transactions"] = txn_df
                if not txn_df.empty:
                    db.save_table("transactions", txn_df, platform="espn", league_id=str(ctx.league_id))
                    log(f"  [LOCAL DB] transactions: {len(txn_df):,} rows written")
            except Exception as e:
                log(f"  FAIL: Transactions - {e}")
                results["fetchers"].append(("Transactions", False))

            # --- Stage 4: Rosters (reads draft + transactions from DB) ---
            log("\n[FETCHERS] Stage 4: Rosters (reads draft + transactions from DB for ownership)...")
            try:
                rosters_df = fetch_all_espn_rosters(ctx, db=db)
                log(f"  OK: Rosters - {len(rosters_df):,} rows")
                results["fetchers"].append(("Rosters", True))
                fetcher_dfs["Rosters"] = rosters_df
            except Exception as e:
                log(f"  FAIL: Rosters - {e}")
                results["fetchers"].append(("Rosters", False))

            # Write matchups to local DuckDB (draft + transactions already saved above)
            if "Matchups" in fetcher_dfs and not fetcher_dfs["Matchups"].empty:
                db.save_table("matchup", fetcher_dfs["Matchups"], platform="espn", league_id=str(ctx.league_id))
                log(f"  [LOCAL DB] matchup: {len(fetcher_dfs['Matchups']):,} rows written")

            # Rosters already saved to DuckDB by fetch_all_espn_rosters when db is provided
            if "Rosters" in fetcher_dfs and not fetcher_dfs["Rosters"].empty:
                rosters_df = fetcher_dfs["Rosters"]
                log(f"  [LOCAL DB] player_fantasy: {len(rosters_df):,} rows (saved by fetcher)")

            # Schedule: derived from matchup data by build_schedule_from_matchup SQL enrichment

    # Keep the resolved quick range for transformations/enrichments.
    if is_quick_import:
        ctx.start_year = _original_start_year
        ctx.end_year = _original_end_year

    # =========================================================================
    # PHASE 1.5: Early abort if no data was fetched
    # =========================================================================
    if start_phase <= 1 and not args.skip_fetchers and not args.dry_run:
        # Check if core data exists in local DuckDB (matchup + rosters are required)
        has_matchups = db.table_exists("matchup") and db.row_count("matchup") > 0
        has_rosters = db.table_exists("player_fantasy") and db.row_count("player_fantasy") > 0

        has_startup_data = (
            (db.table_exists("league_settings") and db.row_count("league_settings") > 0)
            or (db.table_exists("draft") and db.row_count("draft") > 0)
            or (db.table_exists("transactions") and db.row_count("transactions") > 0)
        )
        allow_empty_startup_shell = has_startup_data and (
            (
                is_quick_import
                and quick_years
                and set(quick_years).issubset(unscored_quick_years)
            )
            or (
                not is_quick_import
                and ctx.start_year == ctx.end_year == get_current_nfl_season_year()
            )
        )

        if not has_matchups and not has_rosters and not allow_empty_startup_shell:
            log("\n" + "=" * 96)
            log("[ABORT] No matchup or roster data was fetched from ESPN.")
            log("[ABORT] This typically happens with inactive/historical leagues where ESPN")
            log("[ABORT] returns scoringPeriodId=0. Check the diagnostic logging above for details.")
            log("[ABORT] Skipping Phases 2-5 (no data to transform/upload).")
            log("=" * 96)

            # Jump straight to summary
            log("\n" + "=" * 96)
            log("SUMMARY")
            log("=" * 96)
            for phase, phase_results in results.items():
                if phase_results:
                    successes = sum(1 for _, ok in phase_results if ok)
                    total = len(phase_results)
                    status = "OK" if successes == total else f"{successes}/{total}"
                    log(f"  {phase.upper()}: {status}")
            log("\n[FAIL] ESPN import aborted — no data fetched from ESPN API")
            log("=" * 96)
            db.close()
            sys.exit(1)
        if allow_empty_startup_shell:
            log("[STARTUP SHELL] No ESPN matchup/roster rows yet; continuing with draft/settings data")

    # =========================================================================
    # PHASE 2: Normalize & Consolidate in Local DuckDB
    # =========================================================================
    if start_phase <= 2:
        log("\n" + "=" * 96)
        log("PHASE 2: Normalize & Consolidate")
        log("=" * 96)

        # 2.0: Player_fantasy — already normalized in local DB by fetcher
        log("\n[NORMALIZE] Player_fantasy...")
        try:
            _has_db_rosters = db.table_exists("player_fantasy") and db.row_count("player_fantasy") > 0
            if _has_db_rosters:
                _pf_count = db.row_count("player_fantasy")
                log(f"  [OK] player_fantasy: {_pf_count:,} rows in local DB (canonical)")
                log("  Skipping ESPN+NFL merge — SQL enrichments resolve NFL_player_id after upload")
                results["merges"].append(("ESPN+NFL Merge", True))
            else:
                log("  WARNING: No player_fantasy data in local DB")
                results["merges"].append(("ESPN+NFL Merge", False))
        except Exception as e:
            log(f"  FAIL: {e}")
            results["merges"].append(("ESPN+NFL Merge", False))

        # 2.1: Normalize matchups
        log("\n[NORMALIZE] Matchups...")
        try:
            has_db_matchups = db.table_exists("matchup") and db.row_count("matchup") > 0
            if has_db_matchups and not args.dry_run:
                all_matchups = db.read_table("matchup")
                log(f"  Read {len(all_matchups):,} matchup rows from local DB")
                normalized = normalize_matchup_data(all_matchups, str(ctx.league_id))
                db.execute_sql("DELETE FROM public.matchup")
                db.save_table("matchup", normalized, platform="espn", league_id=str(ctx.league_id))
                log(f"  OK: matchup normalized ({len(normalized):,} rows)")
            results["merges"].append(("Matchup normalization", True))
        except Exception as e:
            log(f"  FAIL: Matchup normalization - {e}")
            results["merges"].append(("Matchup normalization", False))

        # 2.2: Normalize draft
        log("\n[NORMALIZE] Draft...")
        try:
            has_db_drafts = db.table_exists("draft") and db.row_count("draft") > 0
            if has_db_drafts and not args.dry_run:
                all_drafts = db.read_table("draft")
                log(f"  Read {len(all_drafts):,} draft rows from local DB")
                normalized = normalize_draft_data(all_drafts, str(ctx.league_id))
                db.execute_sql("DELETE FROM public.draft")
                db.save_table("draft", normalized, platform="espn", league_id=str(ctx.league_id))
                log(f"  OK: draft normalized ({len(normalized):,} rows)")
            results["merges"].append(("Draft normalization", True))
        except Exception as e:
            log(f"  FAIL: Draft normalization - {e}")
            results["merges"].append(("Draft normalization", False))

        # 2.3: Normalize transactions
        log("\n[NORMALIZE] Transactions...")
        try:
            has_db_txns = db.table_exists("transactions") and db.row_count("transactions") > 0
            if has_db_txns and not args.dry_run:
                all_txns = db.read_table("transactions")
                log(f"  Read {len(all_txns):,} transaction rows from local DB")
                normalized = normalize_transaction_data(all_txns, str(ctx.league_id))
                db.execute_sql("DELETE FROM public.transactions")
                db.save_table("transactions", normalized, platform="espn", league_id=str(ctx.league_id))
                log(f"  OK: transactions normalized ({len(normalized):,} rows)")
            elif not has_db_txns and not args.dry_run:
                log("  [WARN] No transaction data in local DB")
            results["merges"].append(("Transaction normalization", True))
        except Exception as e:
            log(f"  FAIL: Transaction normalization - {e}")
            results["merges"].append(("Transaction normalization", False))

        # 2.4: Schedules — derived from matchup by build_schedule_from_matchup enrichment
        log("\n[NORMALIZE] Schedules...")
        log("  Schedule derived from matchup by SQL enrichment (no separate fetch)")

    # =========================================================================
    # PHASE 2.5: Merge Staging Data (league merge flow)
    # =========================================================================
    if start_phase <= 2:
        log("\n" + "=" * 96)
        log("PHASE 2.5: Merge Staging Data")
        log("=" * 96)

        # ---------------------------------------------------------------------
        # PHASE 1.7: Schema-Conform external uploads before staging merge
        # ---------------------------------------------------------------------
        from multi_league.core.import_pipeline import run_phase_1_7
        from multi_league.external_ingest.schema_conform import (
            SchemaConformAbort,
            EXIT_CODE_SCHEMA_CONFORM_ABORT,
        )

        try:
            run_phase_1_7(db.connect(), ctx)
            log("[PHASE 1.7] Schema-conform complete (or no external data)")
        except SchemaConformAbort as _sc_err:
            log(f"[PHASE 1.7] HARD ABORT: {_sc_err}  " f"failures={[vars(f) for f in _sc_err.failures]}")
            raise SystemExit(EXIT_CODE_SCHEMA_CONFORM_ABORT)  # noqa: B904 - intentional: preserve SchemaConformAbort traceback
        # ---------------------------------------------------------------------

        if getattr(ctx, "merge_source", None) or getattr(ctx, "merge_sources", None):
            log("[MERGE_SOURCE] Skipping local staging download; Fly-side copy runs after upload")
            results["merges"].append(("Staging Data Merge", True))
        else:
            try:
                merge_stats = merge_staging_data(
                    ctx=ctx,
                    db=db,
                    harmonize_dtypes_func=harmonize_dtypes,
                    log_func=log,
                )
            except Exception as e:
                if getattr(ctx, "has_external_data", False):
                    log(f"[STAGING] [FAIL] merge failed with has_external_data=True: {e}")
                    raise
                log(f"[STAGING] WARN: merge failed, continuing without external data: {e}")
                results["merges"].append(("Staging Data Merge", False))
            else:
                if merge_stats.get("status") == "no_staging":
                    if getattr(ctx, "has_external_data", False):
                        log("[STAGING] WARN: has_external_data=True but no staging tables on Fly")
                    else:
                        log("[STAGING] No external staging data - using API data only")
                    results["merges"].append(("Staging Data Merge", True))
                else:
                    log("[STAGING] [OK] Staging data merged successfully")
                    for table_type, stat in merge_stats.items():
                        if isinstance(stat, int):
                            log(f"  • {table_type}: {stat:,} rows")
                        else:
                            log(f"  • {table_type}: {stat}")
                    results["merges"].append(("Staging Data Merge", True))
                    if not ctx.is_single_year_import:
                        log("[STAGING] Clearing staging tables on Fly after full import...")
                        clear_staging_tables(db_name=ctx.league_name, log_func=log)
                    else:
                        log("[STAGING] Preserving staging tables for subsequent full import")

    # =========================================================================
    # PHASE 3: Transformations (local-first)
    # =========================================================================
    if start_phase <= 3 and not args.skip_transformations:
        log("\n" + "=" * 96)
        log("PHASE 3: Transformations (local-first)")
        log("=" * 96)

        # Save context for transformation scripts
        ctx.save(str(ctx.data_directory / "espn_context.json"))

        # Close local DB so subprocess scripts can get exclusive access
        db.close()
        log("[LOCAL DB] Closed for Phase 3 (subprocess scripts need exclusive access)")

        md_db_name = runtime.db_name
        transform_results = run_transformation_pipeline(
            ctx,
            dry_run=args.dry_run,
            skip_track_2_upload=args.skip_track_2_upload,
            import_mode=args.import_mode,
            platform="espn",
            db_name=md_db_name,
            data_dir=str(data_dir),
            quick=is_quick_import,
        )
        results["transformations"].extend(transform_results)

        # Reopen local DB for enrichments
        db.connect()
        log("[LOCAL DB] Reopened after Phase 3")

    # =========================================================================
    # PHASE 3.5: SQL Enrichments (local DuckDB — before upload)
    # =========================================================================
    if start_phase <= 3 and not args.skip_transformations and not args.dry_run:
        log("\n" + "=" * 96)
        log("PHASE 3.5: SQL Enrichments (local DuckDB)")
        log("=" * 96)
        eng = None
        try:
            from multi_league.transformations.sql_enrichments import SQLEnrichments

            md_db_name = runtime.db_name
            eng = SQLEnrichments(db_name=md_db_name, data_dir=str(data_dir), quick=is_quick_import, conn=db.connect())
            roster_by_year, scoring_params = eng.load_settings_from_db()
            eng.roster_by_year = roster_by_year
            eng._update_scoring_params(scoring_params)
            enrichment_results = eng.run_all()
            timing_results = getattr(eng, "last_run_timings", {})

            ok_count = sum(1 for v in enrichment_results.values() if not isinstance(v, tuple))
            fail_count = sum(1 for v in enrichment_results.values() if isinstance(v, tuple))
            for name, count in enrichment_results.items():
                timing_suffix = f" in {timing_results[name]:.2f}s" if name in timing_results else ""
                if isinstance(count, tuple) and count[0] == "error":
                    log(f"  [SQL] {name}: FAILED - {count[1]}{timing_suffix}")
                elif isinstance(count, int) and count >= 0:
                    log(f"  [SQL] {name}: {count:,} rows affected{timing_suffix}")
                elif isinstance(count, int) and count < 0:
                    log(f"  [SQL] {name}: completed (no row count){timing_suffix}")
                else:
                    log(f"  [SQL] {name}: skipped{timing_suffix}")
            if timing_results:
                log("[SQL ENRICHMENTS] Slowest enrichments:")
                for name, elapsed in sorted(timing_results.items(), key=lambda item: item[1], reverse=True)[:10]:
                    log(f"  [SQL TIMING] {name}: {elapsed:.2f}s")
            log(f"[SQL ENRICHMENTS] {ok_count} OK, {fail_count} failed")
            results["transformations"].append(("SQL Enrichments (local)", fail_count == 0))
            require_sql_enrichment_success(enrichment_results)
            fantasy_agg_ok = run_local_fantasy_aggregation(
                db_name=md_db_name,
                data_dir=str(data_dir),
                dry_run=args.dry_run,
                conn=eng.conn,
            )
            results["transformations"].append(("Fantasy Aggregation (local)", fantasy_agg_ok))
            require_sql_enrichment_success(
                enrichment_results,
                fantasy_aggregation_ok=fantasy_agg_ok,
            )
        except Exception as e:
            log(f"[SQL ENRICHMENTS] FAIL: {e}")
            import traceback

            traceback.print_exc()
            results["transformations"].append(("SQL Enrichments (local)", False))
            results["transformations"].append(("Fantasy Aggregation (local)", False))
            raise
        finally:
            if eng is not None:
                eng.close()

    # =========================================================================
    # PHASE 4: Upload Local DuckDB to Fly (storage only)
    # =========================================================================
    if not args.skip_track_2_upload:
        log("\n" + "=" * 96)
        log("PHASE 4: Upload to Fly")
        log("=" * 96)

        db_name = runtime.db_name
        track2_results = _shared_upload_league_tables(
            db,
            db_name,
            ctx,
            platform="espn",
            dry_run=args.dry_run,
        )
        results["track2"].extend(track2_results)

    # =========================================================================
    # SUMMARY
    # =========================================================================
    log("\n" + "=" * 96)
    log("SUMMARY")
    log("=" * 96)

    for phase, phase_results in results.items():
        if phase_results:
            successes = sum(1 for _, ok in phase_results if ok)
            total = len(phase_results)
            status = "OK" if successes == total else f"{successes}/{total}"
            log(f"  {phase.upper()}: {status}")

    all_ok = all(ok for phase_results in results.values() for _, ok in phase_results)

    if all_ok:
        log("\n[SUCCESS] ESPN import completed successfully!")
    else:
        log("\n[WARNING] ESPN import completed with some failures")

    log("=" * 96)

    # Close local DuckDB
    db.close()


if __name__ == "__main__":
    main()
