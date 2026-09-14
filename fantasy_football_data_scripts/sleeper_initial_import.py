#!/usr/bin/env python3
"""
SLEEPER INITIAL IMPORT - Local-First DuckDB Pipeline for Sleeper Fantasy

No OAuth required - uses public Sleeper API.

TWO-TRACK ARCHITECTURE:
- Track 1: Updates NFL super table (shared with Yahoo pipeline)
- Track 2: Uploads league fantasy tables from local DuckDB to Fly

Join Key: player_week = {NFL_player_id}_{year}_{week}

What this does:
1) Track 1: Verify NFL super table has required data (shared)
2) Phase 0: Discover league history, fetch & flatten settings → local DuckDB
3) Phase 1: Fetch Sleeper data → local DuckDB (matchups, rosters, draft, transactions, schedules)
4) Phase 2: Normalize & consolidate data in local DuckDB
5) Phase 3: Transformations (expand_to_all_nfl + keeper_economics run as SQL enrichments in Phase 3.5)
6) Phase 4: Single upload from local DuckDB to Fly
7) Phases 4.5-5.6: Post-upload SQL enrichments

Usage:
  python sleeper_initial_import.py --context path/to/sleeper_context.json
  python sleeper_initial_import.py --context path/to/sleeper_context.json --dry-run
  python sleeper_initial_import.py --context path/to/sleeper_context.json --skip-fetchers
  python sleeper_initial_import.py --context path/to/sleeper_context.json --skip-track-1
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

# Import Sleeper modules
from multi_league.data_fetchers.sleeper import (
    SleeperContext,
    SleeperAPIClient,
    SleeperPlayerCache,
    discover_league_history,
    fetch_all_sleeper_rosters,
    fetch_all_sleeper_matchups,
    fetch_sleeper_draft,
    fetch_all_sleeper_drafts,
    fetch_sleeper_transactions,
    fetch_all_sleeper_transactions,
    fetch_sleeper_traded_picks,
    fetch_sleeper_settings,
    load_sleeper_settings,
    normalize_matchup_data,
    normalize_draft_data,
    normalize_transaction_data,
)

# Import shared utilities
from multi_league.core.script_runner import log
from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.core.fetch_runtime import runtime_from_source
from multi_league.core.year_filter_utils import (
    coerce_int,
    format_year_filter,
    resolve_history_years,
    resolve_quick_import_years,
    unscored_current_shell_years,
)

# Import shared pipeline modules
from multi_league.core.import_utils import (
    run_track_1_verify as _shared_track_1_verify,
    upload_league_tables as _shared_upload_league_tables,
)
from multi_league.core.import_pipeline import (
    require_sql_enrichment_success,
    run_local_fantasy_aggregation,
    run_post_upload_pipeline,
    run_transformation_pipeline,
)
from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.canonical_settings import flatten_settings
from multi_league.core.scoring_variant import derive_scoring_variant
from multi_league.core.data_normalization import harmonize_dtypes
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


def fix_league_id_by_year(df: pd.DataFrame, ctx: SleeperContext) -> pd.DataFrame:
    """
    Fix league_id column to use year-specific league IDs instead of root league_id.

    Sleeper leagues have different league_ids for each season. This function
    corrects the league_id column after normalization to use the proper
    year-specific ID.

    Args:
        df: DataFrame with 'year' and 'league_id' columns
        ctx: SleeperContext with league_ids mapping (year -> league_id)

    Returns:
        DataFrame with corrected league_id column
    """
    if df.empty or "year" not in df.columns or "league_id" not in df.columns:
        return df

    if not ctx.league_ids:
        return df

    # Create year -> league_id mapping
    year_to_lid = {int(y): lid for y, lid in ctx.league_ids.items()}

    # Map each row's year to correct league_id, fallback to current value
    df = df.copy()
    df["league_id"] = df.apply(
        lambda row: year_to_lid.get(int(row["year"]), row["league_id"]) if pd.notna(row["year"]) else row["league_id"],
        axis=1,
    )

    return df


def _sync_context_years_from_history(ctx: SleeperContext, import_mode: str) -> list[int]:
    """Align context year bounds to discovered Sleeper seasons."""
    discovered_years = ctx.get_processing_years()
    if not discovered_years:
        return []

    if import_mode == "quick":
        most_recent = max(discovered_years)
        if ctx.start_year != most_recent or ctx.end_year != most_recent:
            log(f"[QUICK IMPORT] Narrowing discovered history to {most_recent} only")
        ctx.start_year = most_recent
        ctx.end_year = most_recent
        return discovered_years

    new_start = min(discovered_years)
    new_end = max(discovered_years)
    if ctx.start_year != new_start or ctx.end_year != new_end:
        log(f"[HISTORY] Expanding context years: {ctx.start_year}-{ctx.end_year} -> {new_start}-{new_end}")
    ctx.start_year = new_start
    ctx.end_year = new_end
    return discovered_years


def _to_int_or_none(value) -> int | None:
    return coerce_int(value)


def _ctx_history_years(ctx: SleeperContext) -> list[int]:
    years = resolve_history_years(ctx.league_ids, start_year=ctx.start_year, end_year=ctx.end_year)
    return years or list(ctx.get_year_range())


def _resolve_quick_import_years(ctx: SleeperContext, target_year: int, nfl_state: dict | None = None) -> list[int]:
    """Return years to fetch for a Sleeper quick import.

    Sleeper can renew leagues into the next season before any NFL games have
    scores. In that state, the new shell contains useful draft/transaction
    activity, while the previous season still contains matchup and roster data.
    """
    return resolve_quick_import_years(_ctx_history_years(ctx), target_year, nfl_state)


def _format_year_filter(year_filter) -> str:
    return format_year_filter(year_filter)


def run_track_1_verify(start_year: int, end_year: int, dry_run: bool = False) -> bool:
    """Verify NFL super table has required data. Delegates to shared implementation."""
    return _shared_track_1_verify(start_year, end_year, dry_run=dry_run)


def main():
    parser = argparse.ArgumentParser(description="Complete data import for a Sleeper fantasy league")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--league-id", help="Sleeper league ID (bootstraps everything from API)")
    group.add_argument("--context", help="Path to sleeper_context.json (legacy)")
    parser.add_argument("--data-dir", help="Data directory (default: temp dir)")
    parser.add_argument("--database-name", help="Override the league database name when bootstrapping from --league-id")
    parser.add_argument("--dry-run", action="store_true", help="Simulate without writing")
    parser.add_argument("--skip-fetchers", action="store_true", help="Skip data fetchers (use existing files)")
    parser.add_argument("--skip-transformations", action="store_true", help="Skip transformations")
    parser.add_argument(
        "--start-phase",
        type=int,
        choices=[0, 1, 2, 3, 4],
        help="Start at specific phase (0=discovery, 1=fetchers, 2=normalize, 3=enrichments, 4=upload)",
    )
    parser.add_argument(
        "--stop-after",
        type=int,
        choices=[0, 1, 2, 3, 4],
        help="Stop after this phase (0=discovery, 1=fetchers, 2=normalize, 3=enrichments, 4=upload)",
    )
    parser.add_argument("--skip-track-1", action="store_true", help="Skip NFL super table verification")
    parser.add_argument("--skip-track-2-upload", action="store_true", help="Skip Fly upload")
    parser.add_argument(
        "--import-mode",
        choices=["quick", "full"],
        default="full",
        help="Import mode: 'quick' = target season refresh, 'full' = all years",
    )
    # Targeted fetch flags (match Yahoo v3 pattern)
    parser.add_argument(
        "--fetch",
        type=str,
        choices=["matchups", "rosters", "draft", "transactions", "traded_picks"],
        help="Targeted fetch: run only this fetcher (requires --year)",
    )
    parser.add_argument("--year", type=int, default=None, help="Targeted fetch: specific year")
    parser.add_argument("--with-transforms", action="store_true", help="Run transformations after targeted fetch")
    args = parser.parse_args()

    if args.league_id:
        # Bootstrap context from just a league ID — no context file needed
        import tempfile

        client = SleeperAPIClient()
        league_info = client.get_league(args.league_id)
        if not league_info:
            log(f"[FAIL] Could not fetch league {args.league_id} from Sleeper API")
            sys.exit(1)

        league_name = league_info.get("name", f"sleeper_{args.league_id}")
        season = league_info.get("season", str(get_current_nfl_season_year()))

        if args.data_dir:
            data_dir = Path(args.data_dir)
            data_dir.mkdir(parents=True, exist_ok=True)
        else:
            data_dir = Path(tempfile.mkdtemp(prefix=f"{league_name.lower().replace(' ', '_')}_"))

        ctx = SleeperContext(
            league_id=args.league_id,
            league_name=league_name,
            username="",  # Not needed — we discover via league_id chain
            start_year=int(season),
            data_directory=data_dir,
        )
        if args.database_name:
            ctx.database_name = args.database_name

        # Discover full league history so get_league_id_for_year() works for all years
        log("[BOOTSTRAP] Discovering league history...")
        history = discover_league_history(client, args.league_id)
        if history:
            ctx.league_ids = history
            discovered_years = sorted(int(y) for y in history.keys())
            ctx.start_year = min(discovered_years)
            ctx.end_year = max(discovered_years)
            log(f"[BOOTSTRAP] Found {len(history)} seasons: {discovered_years}")
        else:
            ctx.league_ids = {season: args.league_id}
            log(f"[BOOTSTRAP] No history chain — using single season {season}")

        context_path = data_dir / "sleeper_context.json"
        ctx.save(str(context_path))
        log(f"[BOOTSTRAP] Created context: {league_name} ({args.league_id})")
        log(f"[BOOTSTRAP] Data dir: {data_dir}")

        # Always fetch + flatten settings during bootstrap — everything downstream needs them
        # Quick imports: only current year. Full imports: all discovered years.
        is_quick = args.import_mode == "quick"
        if is_quick:
            settings_years = [ctx.end_year or max(int(y) for y in ctx.league_ids.keys())]
        else:
            settings_years = sorted(int(y) for y in ctx.league_ids.keys())
        log(f"[BOOTSTRAP] Fetching settings for {len(settings_years)} year(s)...")
        bootstrap_runtime = runtime_from_source(ctx)
        _bootstrap_db = LocalLeagueDB(bootstrap_runtime.data_dir, bootstrap_runtime.db_name)
        _bootstrap_db.connect()
        _bootstrap_db.ensure_table("league_settings")
        settings_rows = []
        for yr in settings_years:
            lid = ctx.get_league_id_for_year(yr)
            if not lid:
                continue
            try:
                raw = fetch_sleeper_settings(client, lid, yr)
                settings_rows.append(flatten_settings(raw, platform="sleeper", year=yr, league_key=str(lid)))
                log(f"  [BOOTSTRAP] {yr}: OK")
            except Exception as e:
                log(f"  [BOOTSTRAP] {yr}: {e}")
        if settings_rows:
            import polars as _pl

            for row in settings_rows:
                _bootstrap_db.save_table("league_settings", _pl.DataFrame([row]), year=row["year"])
            log(f"[BOOTSTRAP] Saved {len(settings_rows)} year(s) of settings to local DuckDB")
        _bootstrap_db.close()
    else:
        context_path = Path(args.context).resolve()
        if not context_path.exists():
            log(f"[FAIL] Sleeper context not found: {context_path}")
            sys.exit(1)
        ctx = SleeperContext.load(str(context_path))

    # CRITICAL: Set import_mode on context so transformations know if it's quick or full
    # This affects SQLEnrichments.expand_to_all_nfl(), which honors quick mode by limiting
    # expansion to imported years only.
    ctx.import_mode = args.import_mode

    # Determine starting/stopping phase
    # --skip-fetchers skips Phase 1 (data fetchers) but NOT Phase 0 (history/settings)
    # Use --start-phase 2 to skip both Phase 0 and Phase 1
    start_phase = args.start_phase if args.start_phase is not None else 0
    stop_after = args.stop_after if args.stop_after is not None else 99  # run all by default

    log("=" * 96)
    log("SLEEPER INITIAL IMPORT - Local-First Pipeline")
    log("=" * 96)
    log(f"League: {ctx.league_name} | League ID: {ctx.league_id}")
    log(
        f"Mode:   {args.import_mode.upper()} "
        f"{'(latest season refresh; includes previous scored season for empty renewed shells)' if args.import_mode == 'quick' else '(preserves existing data)'}"
    )
    log(f"Years:  {ctx.start_year} - {ctx.end_year or 'current'}")
    log(f"Root:   {ctx.data_directory}")
    log(f"DryRun: {'Yes' if args.dry_run else 'No'}")
    log(f"Track 1 (NFL):  {'SKIP' if args.skip_track_1 else 'ENABLED'}")
    log(f"Track 2 Upload: {'SKIP' if args.skip_track_2_upload else 'ENABLED'}")
    log(f"Phases: {start_phase} to {'all' if stop_after >= 99 else stop_after}")
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

    # Initialize local DuckDB for pipeline storage
    runtime = runtime_from_source(ctx, context_path=context_path)
    data_dir = runtime.data_dir
    db_name = runtime.db_name
    db = LocalLeagueDB(data_dir, db_name)
    db.connect()
    for table in ["matchup", "player_fantasy", "draft", "transactions", "schedule", "league_settings"]:
        db.ensure_table(table)
    log(f"[LOCAL DB] Initialized {db.db_path}")

    # Initialize API client and player cache
    client = SleeperAPIClient()
    player_cache = SleeperPlayerCache(ctx.cache_directory)

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
            "matchups": ("matchup", lambda yr: fetch_all_sleeper_matchups(ctx, client=client, year_filter=yr, db=db)),
            "rosters": (
                "player_fantasy",
                lambda yr: fetch_all_sleeper_rosters(
                    ctx, client=client, player_cache=player_cache, year_filter=yr, db=db
                ),
            ),
            "draft": (
                "draft",
                lambda yr: fetch_all_sleeper_drafts(
                    ctx, client=client, player_cache=player_cache, year_filter=yr, db=db
                ),
            ),
            "transactions": (
                "transactions",
                lambda yr: fetch_all_sleeper_transactions(
                    ctx, client=client, player_cache=player_cache, year_filter=yr, db=db
                ),
            ),
            "traded_picks": (
                "traded_picks",
                lambda yr: fetch_sleeper_traded_picks(ctx, client=client, year_filter=yr, db=db),
            ),
        }

        table_name, fetch_fn = _FETCH_MAP[args.fetch]
        try:
            result = fetch_fn(args.year)
            row_count = (
                len(result)
                if hasattr(result, "__len__")
                else sum(len(v) for v in result.values())
                if isinstance(result, dict)
                else 0
            )
            log(f"[TARGETED FETCH] {args.fetch} year={args.year}: {row_count:,} rows -> local DB")

            if not args.skip_track_2_upload and not args.dry_run:
                log(f"[TARGETED FETCH] Uploading to Fly ({db_name})...")
                db.upload_to_fly(db_name)
                log("[TARGETED FETCH] Upload complete")

            if args.with_transforms and not args.dry_run:
                log("[TARGETED FETCH] Running post-upload pipeline...")
                run_post_upload_pipeline(ctx, dry_run=args.dry_run)

            log("[TARGETED FETCH] SUCCESS")
        except Exception as e:
            log(f"[TARGETED FETCH] FAILED: {e}")
            db.close()
            sys.exit(2)

        db.close()
        sys.exit(0)

    # Determine year filter for quick import mode
    is_quick_import = args.import_mode == "quick"
    quick_years = [end_year] if is_quick_import else []
    year_filter = end_year if is_quick_import else None
    nfl_state: dict = {}
    if is_quick_import:
        log(f"[QUICK IMPORT] Only fetching year {year_filter}")
    else:
        log(f"[FULL IMPORT] Fetching all years ({ctx.start_year}-{end_year})")

    staging_settings_years: set[int] = set()

    # =========================================================================
    # PHASE 0: League History & Settings Discovery
    # =========================================================================
    if start_phase <= 0 and stop_after >= 0:
        log("\n" + "=" * 96)
        log("PHASE 0: League History & Settings Discovery")
        log("=" * 96)

        # NOTE: No early FRESH START delete against ___leagues. The final
        # upload_to_fly() handles DELETE WHERE db_name + INSERT
        # atomically per table, so the league's data stays available in
        # ___leagues throughout the import. If the import crashes mid-way
        # the old data is still there — only a successful upload replaces it.

        # Discover league history if not present
        if not ctx.league_ids:
            log("[HISTORY] Discovering league history...")
            try:
                league_ids = discover_league_history(client, ctx.league_id)
                if league_ids:
                    ctx.league_ids = league_ids
                    discovered_years = _sync_context_years_from_history(ctx, args.import_mode)
                    ctx.save(str(context_path))
                    log(f"[HISTORY] Discovered {len(league_ids)} seasons: {list(league_ids.keys())}")
                    if discovered_years:
                        log(f"[HISTORY] Processing years: {discovered_years}")
                else:
                    log("[HISTORY] No history found, using single league ID")
                    ctx.league_ids = {str(ctx.start_year): ctx.league_id}
            except Exception as e:
                log(f"[HISTORY] Error discovering history: {e}")
                ctx.league_ids = {str(ctx.start_year): ctx.league_id}
        else:
            log(f"[HISTORY] Using {len(ctx.league_ids)} pre-configured league IDs")
            discovered_years = _sync_context_years_from_history(ctx, args.import_mode)
            if discovered_years:
                ctx.save(str(context_path))
                log(f"[HISTORY] Processing years from league_ids: {discovered_years}")

        # After discovering league IDs, update year_filter for quick imports.
        # If Sleeper has already renewed an empty next-season shell, fetch both
        # the new shell and the previous scored season.
        if is_quick_import and ctx.league_ids:
            try:
                nfl_state = client.get_nfl_state() or {}
            except Exception as e:
                log(f"[QUICK IMPORT] Could not fetch Sleeper NFL state for year selection: {e}")
                nfl_state = {}

            quick_years = _resolve_quick_import_years(ctx, end_year, nfl_state)
            new_year_filter = quick_years[0] if len(quick_years) == 1 else quick_years
            if new_year_filter != year_filter:
                reason = ""
                if len(quick_years) > 1:
                    reason = " (current Sleeper season has no scores yet)"
                log(
                    f"[QUICK IMPORT] Updating target years: "
                    f"{_format_year_filter(year_filter)} -> {_format_year_filter(new_year_filter)}{reason}"
                )
            year_filter = new_year_filter
            ctx.start_year = min(quick_years)
            ctx.end_year = max(quick_years)
            end_year = ctx.end_year
            ctx.save(str(context_path))

        # History discovery may have widened the context beyond the payload range.
        end_year = ctx.end_year or end_year

        # Refresh player cache
        log("[CACHE] Refreshing player cache...")
        if not args.dry_run:
            player_cache.refresh_if_stale(client)
            log(f"[CACHE] Player cache ready: {len(player_cache._players):,} players")

        # -----------------------------------------------------------------
        # Check for staged settings from league merge
        # -----------------------------------------------------------------
        # When merging another league, copy_league_to_staging() puts the
        # source league's league_settings into staging.staging_settings.
        # We download them to disk BEFORE the API fetch loop so they are
        # included in the settings upload call later.
        staging_settings_years: set[int] = set()
        try:
            staging_settings_years = pre_fetch_staging_settings(ctx, log_func=log)
        except Exception as e:
            log(f"[STAGING SETTINGS] WARN: pre-fetch failed: {e}")
            if getattr(ctx, "has_external_data", False):
                raise

        # Fetch settings, flatten to canonical schema, save to local DuckDB
        log("[SETTINGS] Fetching and flattening league settings...")
        effective_end_year = ctx.end_year or get_current_nfl_season_year()
        if is_quick_import and quick_years:
            all_years = quick_years
        else:
            configured_years = (
                sorted([int(y) for y in ctx.league_ids.keys() if int(y) <= effective_end_year])
                if ctx.league_ids
                else list(ctx.get_year_range())
            )
            all_years = sorted(set(configured_years) | staging_settings_years)
        log(f"[SETTINGS] Years to process (capped at {effective_end_year}): {all_years}")

        settings_rows = []
        for year in all_years:
            try:
                if year in staging_settings_years:
                    # Load staged settings from disk (written by pre_fetch_staging_settings)
                    raw = load_sleeper_settings(ctx, year)
                    if raw:
                        raw_platform = str(raw.get("platform") or "sleeper").lower()
                        raw_league_key = raw.get("league_key") or raw.get("league_id")
                        settings_rows.append(
                            flatten_settings(
                                raw,
                                platform=raw_platform,
                                year=year,
                                league_key=str(raw_league_key or ctx.get_league_id_for_year(year) or ctx.league_id),
                            )
                        )
                        log(f"  [STAGED] {year} - loaded canonical settings from league merge")
                    else:
                        log(f"  [STAGED] {year} - no settings file found after staging write")
                    results["settings"].append((str(year), True))
                    continue

                league_id = ctx.get_league_id_for_year(year)
                if not league_id:
                    log(f"  WARNING: No league ID for {year}, skipping")
                    continue

                if args.dry_run:
                    log(f"  [DRY-RUN] Would fetch settings for {year}")
                else:
                    raw = fetch_sleeper_settings(client, league_id, year)
                    settings_rows.append(
                        flatten_settings(raw, platform="sleeper", year=year, league_key=str(league_id))
                    )
                    log(f"  OK: {year} - {raw.get('league_name', raw.get('name', 'Unknown'))}")
                results["settings"].append((str(year), True))
            except Exception as e:
                log(f"  FAIL: {year} - {e}")
                results["settings"].append((str(year), False))

        # Save flattened settings to local DuckDB (uploaded with everything else in Phase 4)
        # Clear any settings written by bootstrap to avoid PK violations
        if settings_rows and not args.dry_run:
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
            conn = db.connect()
            conn.execute("DELETE FROM public.league_settings")
            db.save_table("league_settings", pd.DataFrame(settings_rows))
            log(f"[SETTINGS] Saved {len(settings_rows)} year(s) to local DuckDB (flat canonical schema)")
            results["settings"].append(("Local DB Save", True))

        # NOTE: create_league_context_parquet removed — all transformation passes
        # are empty; SQL enrichments work directly from local DuckDB, no parquets needed.

    if stop_after <= 0:
        log("\n[STOP] Stopped after Phase 0 (discovery + settings)")
        db.close()
        log(f"[LOCAL DB] Closed {db.db_path}")
        sys.exit(0)

    # =========================================================================
    # TRACK 1: NFL Super Table Load (same as Yahoo)
    # =========================================================================
    if not args.skip_track_1 and start_phase <= 1 and stop_after >= 1:
        log("\n" + "=" * 96)
        log("TRACK 1: NFL Super Table Verify")
        log("=" * 96)

        if is_quick_import and quick_years:
            track_1_years = list(quick_years)
            latest_quick_year = max(quick_years)
            unscored_shells = unscored_current_shell_years(quick_years, nfl_state)
            if len(quick_years) > 1 and latest_quick_year in unscored_shells:
                track_1_years = [year for year in quick_years if year not in unscored_shells]
            if not track_1_years:
                track_1_years = [latest_quick_year]
            nfl_start = min(track_1_years)
            track_1_end_year = max(track_1_years)
        else:
            nfl_start = max(1999, min([ctx.start_year, *staging_settings_years]))
            track_1_end_year = end_year

        # Verify the super table has data (SQL enrichments resolve NFL_player_id directly —
        # no parquet download needed, just confirm the super table covers our year range)
        track1_ok = run_track_1_verify(nfl_start, track_1_end_year, dry_run=args.dry_run)
        results["track1"].append(("NFL Super Table Verify", track1_ok))

        if not track1_ok and not args.dry_run:
            log("[TRACK 1] WARNING: NFL data verification failed, continuing anyway")
    elif is_quick_import and start_phase <= 1:
        log("\n" + "=" * 96)
        log("[QUICK IMPORT] Skipping Track 1 - NFL super table not needed for current-year refresh")
        log("=" * 96)

    # =========================================================================
    # PHASE 1: Sleeper Data Fetchers
    # =========================================================================
    if start_phase <= 1 and stop_after >= 1 and not args.skip_fetchers:
        log("\n" + "=" * 96)
        log("PHASE 1: Sleeper Data Fetchers")
        log("=" * 96)

        # Fetch matchups FIRST - roster fetcher uses matchup data to build roster_map
        # This ensures historical manager names are preserved even if managers left the league
        log("\n[FETCHERS] Matchups...")
        try:
            if args.dry_run:
                log(f"  [DRY-RUN] Would fetch matchups for {_format_year_filter(year_filter)}")
            else:
                matchups_df = fetch_all_sleeper_matchups(ctx, client=client, year_filter=year_filter, db=db)
                log(f"  OK: {len(matchups_df):,} matchup rows")
            results["fetchers"].append(("Matchups", True))
        except Exception as e:
            log(f"  FAIL: {e}")
            results["fetchers"].append(("Matchups", False))

        # Fetch rosters (weekly player ownership) - uses matchup data for roster_map
        log("\n[FETCHERS] Rosters...")
        try:
            if args.dry_run:
                log(f"  [DRY-RUN] Would fetch rosters for {_format_year_filter(year_filter)}")
            else:
                rosters_df = fetch_all_sleeper_rosters(
                    ctx,
                    client=client,
                    player_cache=player_cache,
                    year_filter=year_filter,
                    db=db,
                )
                log(f"  OK: {len(rosters_df):,} roster rows")
            results["fetchers"].append(("Rosters", True))
        except Exception as e:
            log(f"  FAIL: {e}")
            results["fetchers"].append(("Rosters", False))

        # Fetch draft
        log("\n[FETCHERS] Draft...")
        try:
            if args.dry_run:
                log(f"  [DRY-RUN] Would fetch drafts for {_format_year_filter(year_filter)}")
            else:
                draft_dfs = fetch_all_sleeper_drafts(
                    ctx,
                    client=client,
                    player_cache=player_cache,
                    year_filter=year_filter,
                    db=db,
                )
                total_picks = sum(len(df) for df in draft_dfs.values())
                log(f"  OK: {total_picks:,} draft picks across {len(draft_dfs)} years")
            results["fetchers"].append(("Draft", True))
        except Exception as e:
            log(f"  FAIL: {e}")
            results["fetchers"].append(("Draft", False))

        # Fetch transactions
        log("\n[FETCHERS] Transactions...")
        try:
            if args.dry_run:
                log(f"  [DRY-RUN] Would fetch transactions for {_format_year_filter(year_filter)}")
            else:
                transactions_dfs = fetch_all_sleeper_transactions(
                    ctx,
                    client=client,
                    player_cache=player_cache,
                    year_filter=year_filter,
                    db=db,
                )
                total_transactions = sum(len(df) for df in transactions_dfs.values())
                log(f"  OK: {total_transactions:,} transactions across {len(transactions_dfs)} years")
            results["fetchers"].append(("Transactions", True))
        except Exception as e:
            log(f"  FAIL: {e}")
            results["fetchers"].append(("Transactions", False))

        # Fetch offseason data from next year's shell league (dynasty/keeper leagues)
        # Dynasty leagues often have trades and rookie drafts in the offseason, stored
        # under the next year's shell league_id (e.g. 2026 shell has spring 2026 rookie draft
        # and post-championship trades). We tag this data with the shell year so it's
        # captured even though we don't import 2026 matchups/rosters.
        offseason_year = end_year + 1
        offseason_league_id = ctx.get_league_id_for_year(offseason_year)
        if offseason_league_id and not is_quick_import:
            log(f"\n[FETCHERS] Offseason data from {offseason_year} shell league...")

            # Offseason transactions (trades, FA pickups between seasons)
            try:
                if not args.dry_run:
                    offseason_txns = fetch_sleeper_transactions(
                        ctx=ctx,
                        year=offseason_year,
                        client=client,
                        player_cache=player_cache,
                        db=db,
                    )
                    if offseason_txns is not None and not offseason_txns.empty:
                        log(f"  OK: {len(offseason_txns):,} offseason transactions from {offseason_year} shell")
                    else:
                        log(f"  OK: No offseason transactions in {offseason_year} shell")
                else:
                    log(f"  [DRY-RUN] Would fetch offseason transactions from {offseason_year} shell")
            except Exception as e:
                log(f"  WARNING: Could not fetch offseason transactions from {offseason_year}: {e}")

            # Offseason draft (rookie/supplemental drafts between seasons)
            try:
                if not args.dry_run:
                    offseason_draft = fetch_sleeper_draft(
                        ctx=ctx,
                        year=offseason_year,
                        client=client,
                        player_cache=player_cache,
                        db=db,
                    )
                    if offseason_draft is not None and not offseason_draft.empty:
                        log(f"  OK: {len(offseason_draft):,} offseason draft picks from {offseason_year} shell")
                    else:
                        log(f"  OK: No offseason draft in {offseason_year} shell")
                else:
                    log(f"  [DRY-RUN] Would fetch offseason draft from {offseason_year} shell")
            except Exception as e:
                log(f"  WARNING: Could not fetch offseason draft from {offseason_year}: {e}")

        # Schedule: derived from matchup data by build_schedule_from_matchup SQL enrichment
        # No separate API call needed — matchup table has all schedule columns

        # Fetch traded picks (dynasty leagues)
        # Quick imports: only fetch current year's traded picks
        log("\n[FETCHERS] Traded Picks...")
        try:
            if args.dry_run:
                log("  [DRY-RUN] Would fetch traded picks")
            else:
                traded_picks_df = fetch_sleeper_traded_picks(ctx, client=client, year_filter=year_filter, db=db)
                if traded_picks_df is not None and not traded_picks_df.empty:
                    log(f"  OK: {len(traded_picks_df):,} traded picks")
                else:
                    log("  OK: No traded picks found (normal for redraft leagues)")
            results["fetchers"].append(("Traded Picks", True))
        except Exception as e:
            log(f"  FAIL: {e}")
            results["fetchers"].append(("Traded Picks", False))

    if stop_after <= 1:
        log("\n[STOP] Stopped after Phase 1 (fetchers)")
        db.close()
        log(f"[LOCAL DB] Closed {db.db_path}")
        sys.exit(0)

    # =========================================================================
    # PHASE 2: Normalize & Consolidate in Local DuckDB
    # =========================================================================
    if start_phase <= 2 and stop_after >= 2:
        log("\n" + "=" * 96)
        log("PHASE 2: Normalize & Consolidate")
        log("=" * 96)

        # 2.0: Player_fantasy — already normalized in local DB by fetcher
        log("\n[NORMALIZE] Player_fantasy...")
        try:
            _has_db_rosters = (
                db is not None and db.table_exists("player_fantasy") and db.row_count("player_fantasy") > 0
            )
            if _has_db_rosters:
                _pf_count = db.row_count("player_fantasy")
                log(f"  [OK] player_fantasy: {_pf_count:,} rows in local DB (canonical)")
                log("  Skipping Sleeper+NFL merge — SQL enrichments resolve NFL_player_id after upload")
                results["merges"].append(("Sleeper+NFL Merge", True))
            else:
                log("  WARNING: No player_fantasy data in local DB")
                results["merges"].append(("Sleeper+NFL Merge", False))
        except Exception as e:
            log(f"  FAIL: {e}")
            results["merges"].append(("Sleeper+NFL Merge", False))

        # 2.1: Normalize matchups
        log("\n[NORMALIZE] Matchups...")
        try:
            has_db_matchups = db is not None and db.table_exists("matchup") and db.row_count("matchup") > 0
            if has_db_matchups and not args.dry_run:
                all_matchups = db.read_table("matchup")
                log(f"  Read {len(all_matchups):,} matchup rows from local DB")

                normalized = normalize_matchup_data(all_matchups, ctx.league_id)
                normalized = fix_league_id_by_year(normalized, ctx)

                conn = db.connect()
                conn.execute("DELETE FROM public.matchup")
                db.save_table("matchup", normalized, platform="sleeper", league_id=str(ctx.league_id))
                log(f"  OK: matchup normalized ({len(normalized):,} rows)")
                results["merges"].append(("Matchup normalization", True))
            elif not args.dry_run:
                log("  WARNING: No matchup data in local DB")
                results["merges"].append(("Matchup normalization", True))
            else:
                results["merges"].append(("Matchup normalization", True))
        except Exception as e:
            log(f"  FAIL: Matchup normalization - {e}")
            results["merges"].append(("Matchup normalization", False))

        # 2.2: Normalize draft
        log("\n[NORMALIZE] Draft...")
        try:
            has_db_drafts = db is not None and db.table_exists("draft") and db.row_count("draft") > 0
            if has_db_drafts and not args.dry_run:
                all_drafts = db.read_table("draft")
                log(f"  Read {len(all_drafts):,} draft rows from local DB")

                # Deduplicate by (player_id, year)
                if "sleeper_player_id" in all_drafts.columns:
                    dedup_key = ["sleeper_player_id", "year"]
                elif "player_id" in all_drafts.columns:
                    dedup_key = ["player_id", "year"]
                else:
                    dedup_key = ["player", "year"]
                before_dedup = len(all_drafts)
                all_drafts = all_drafts.drop_duplicates(subset=dedup_key, keep="first")
                if before_dedup != len(all_drafts):
                    log(f"  Removed {before_dedup - len(all_drafts):,} duplicate rows")

                normalized = normalize_draft_data(all_drafts, ctx.league_id)
                normalized = fix_league_id_by_year(normalized, ctx)

                conn = db.connect()
                conn.execute("DELETE FROM public.draft")
                db.save_table("draft", normalized, platform="sleeper", league_id=str(ctx.league_id))
                log(f"  OK: draft normalized ({len(normalized):,} rows)")
            results["merges"].append(("Draft normalization", True))
        except Exception as e:
            log(f"  FAIL: Draft normalization - {e}")
            results["merges"].append(("Draft normalization", False))

        # 2.3: Normalize transactions
        log("\n[NORMALIZE] Transactions...")
        try:
            has_db_txns = db is not None and db.table_exists("transactions") and db.row_count("transactions") > 0
            if has_db_txns and not args.dry_run:
                all_transactions = db.read_table("transactions")
                log(f"  Read {len(all_transactions):,} transaction rows from local DB")

                normalized = normalize_transaction_data(all_transactions, ctx.league_id)
                normalized = fix_league_id_by_year(normalized, ctx)

                conn = db.connect()
                conn.execute("DELETE FROM public.transactions")
                db.save_table("transactions", normalized, platform="sleeper", league_id=str(ctx.league_id))
                log(f"  OK: transactions normalized ({len(normalized):,} rows)")
            elif not has_db_txns and not args.dry_run:
                log("  [WARN] No transaction data in local DB")
            results["merges"].append(("Transaction normalization", True))
        except Exception as e:
            log(f"  FAIL: Transaction normalization - {e}")
            results["merges"].append(("Transaction normalization", False))

        # 2.4: Verify schedules (already normalized by fetcher + save_table normalizer)
        log("\n[NORMALIZE] Schedules...")
        try:
            has_db_schedules = db is not None and db.table_exists("schedule") and db.row_count("schedule") > 0
            if has_db_schedules:
                sched_count = db.row_count("schedule")
                log(f"  OK: schedule: {sched_count:,} rows in local DB")
            elif not args.dry_run:
                log("  [WARN] No schedule data in local DB")
            results["merges"].append(("Schedule normalization", True))
        except Exception as e:
            log(f"  FAIL: Schedule normalization - {e}")
            results["merges"].append(("Schedule normalization", False))

    # =========================================================================
    # PHASE 2.5: Merge Staging Data (league merge flow)
    # =========================================================================
    if start_phase <= 2 and stop_after >= 2:
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

    if stop_after <= 2:
        log("\n[STOP] Stopped after Phase 2 (normalize)")
        db.close()
        log(f"[LOCAL DB] Closed {db.db_path}")
        sys.exit(0)

    # =========================================================================
    # PHASE 3: Transformations
    # =========================================================================
    if start_phase <= 3 and stop_after >= 3 and not args.skip_transformations:
        log("\n" + "=" * 96)
        log("PHASE 3: Transformations")
        log("=" * 96)

        # For full imports, expand year range to include ALL years with data
        if args.import_mode == "full":
            nfl_cap = ctx.end_year or get_current_nfl_season_year()
            all_available_years: set[int] = set(int(y) for y in staging_settings_years if int(y) <= nfl_cap)
            if ctx.league_ids:
                all_available_years.update(int(y) for y in ctx.league_ids.keys() if int(y) <= nfl_cap)
            try:
                local_year_rows = (
                    db.connect()
                    .execute("SELECT DISTINCT year FROM public.league_settings WHERE year IS NOT NULL ORDER BY year")
                    .fetchall()
                )
                all_available_years.update(int(row[0]) for row in local_year_rows if row and row[0] is not None)
            except Exception as e:
                log(f"[TRANSFORMATIONS] Could not read local league_settings years: {e}")

            if all_available_years:
                original_range = f"{ctx.start_year}-{ctx.end_year}"
                ctx.start_year = min(all_available_years)
                ctx.end_year = max(all_available_years)
                new_range = f"{ctx.start_year}-{ctx.end_year}"
                if original_range != new_range:
                    log(f"[TRANSFORMATIONS] Expanded year range: {original_range} -> {new_range}")

        # Save context for transformation scripts
        ctx.save(str(ctx.data_directory / "sleeper_context.json"))

        # Close local DB so subprocess scripts can get exclusive access
        db.close()
        log("[LOCAL DB] Closed for Phase 3 (subprocess scripts need exclusive access)")

        transform_results = run_transformation_pipeline(
            ctx,
            dry_run=args.dry_run,
            skip_track_2_upload=args.skip_track_2_upload,
            import_mode=args.import_mode,
            platform="sleeper",
            db_name=db_name,
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
    if start_phase <= 3 and stop_after >= 3 and not args.skip_transformations and not args.dry_run:
        log("\n" + "=" * 96)
        log("PHASE 3.5: SQL Enrichments (local DuckDB)")
        log("=" * 96)
        eng = None
        try:
            from multi_league.transformations.sql_enrichments import SQLEnrichments

            eng = SQLEnrichments(db_name=db_name, data_dir=str(data_dir), quick=is_quick_import, conn=db.connect())
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
                db_name=db_name,
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

    if stop_after <= 3:
        log("\n[STOP] Stopped after Phase 3 (enrichments)")
        db.close()
        log(f"[LOCAL DB] Closed {db.db_path}")
        sys.exit(0)

    # =========================================================================
    # PHASE 4: Upload Local DuckDB to Fly (storage only)
    # =========================================================================
    if not args.skip_track_2_upload and stop_after >= 4:
        log("\n" + "=" * 96)
        log("PHASE 4: Upload to Fly")
        log("=" * 96)

        track2_results = _shared_upload_league_tables(
            db,
            db_name,
            ctx,
            platform="sleeper",
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

    # Check for any failures
    all_ok = all(ok for phase_results in results.values() for _, ok in phase_results)

    if all_ok:
        log("\n[SUCCESS] Sleeper import completed successfully!")
    else:
        log("\n[WARNING] Sleeper import completed with some failures")

    log("=" * 96)

    # Final is_playoffs diagnostic before closing (catches any late-stage clobber)
    try:
        conn = db.connect()
        diag = conn.execute(
            "SELECT year, "
            "SUM(CASE WHEN CAST(is_playoffs AS INTEGER) = 1 THEN 1 ELSE 0 END) as po, "
            "SUM(CASE WHEN CAST(champion AS INTEGER) = 1 THEN 1 ELSE 0 END) as ch "
            "FROM public.matchup GROUP BY year ORDER BY year"
        ).fetchall()
        for row in diag:
            status = "OK" if row[1] > 0 else "*** MISSING ***"
            print(f"[FINAL DIAG] {row[0]}: is_playoffs={row[1]}, champion={row[2]} {status}")
    except Exception as e:
        print(f"[FINAL DIAG] check failed: {e}")

    # Close local DB
    db.close()
    log(f"[LOCAL DB] Closed {db.db_path}")


if __name__ == "__main__":
    main()
