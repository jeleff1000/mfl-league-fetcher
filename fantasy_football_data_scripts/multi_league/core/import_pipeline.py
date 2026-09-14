"""
Shared Import Pipeline

Shared post-upload pipeline functions (Phases 4.1, 4.5, 5.6) and the full
transformation pipeline (Phase 3) used by all three import orchestrators.
Aggregation and homepage are handled by the playoff_odds finalization workflow.

This replaces hundreds of lines of duplicated code across initial_import_v2.py,
sleeper_initial_import.py, and espn_initial_import.py.

Usage:
    from multi_league.core.import_pipeline import (
        run_sql_enrichments,
        run_deprecated_column_cleanup,
        run_post_upload_pipeline,
        run_transformation_pipeline,
    )
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

from multi_league.core.db_utils import get_db_name
from multi_league.core.script_runner import log, run_script
from multi_league.core.import_utils import (
    run_transformations,
    persist_frontend_settings_tables,
    _detect_context_file,
    _detect_platform,
)
from multi_league.core.import_config import (
    TRANSFORMATIONS_PASS_1,
    TRANSFORMATIONS_PASS_2A,
    TRANSFORMATIONS_PASS_2B,
    TRANSFORMATIONS_PASS_3,
    ESPN_PASS_1_EXTRAS,
)


# =============================================================================
# Phase 4.1: Deprecated Column Cleanup
# =============================================================================


def run_deprecated_column_cleanup(
    ctx: Any,
    db_name: str,
    dry_run: bool = False,
) -> bool:
    """Drop deprecated columns from database tables.

    Previously ESPN-only; now runs for all platforms.

    Returns:
        True if cleanup succeeded or was skipped, False on error
    """
    if dry_run:
        log("[CLEANUP][DRY-RUN] Would drop deprecated columns")
        return True

    log("  [SKIP] Deprecated column cleanup not needed on Fly backend")
    return True


# =============================================================================
# Phase 4.5: SQL Enrichments
# =============================================================================

# NOTE: run_fantasy_aggregation, run_matchup_aggregation, run_draft_aggregation,
# run_transaction_aggregation, run_homepage_summary were removed — aggregation
# and homepage are now handled by the playoff_odds finalization workflow.


def require_sql_enrichment_success(
    enrichment_results: dict[str, object],
    *,
    fantasy_aggregation_ok: bool | None = None,
) -> None:
    """Block publication when local SQL output is incomplete.

    ``SQLEnrichments.run_all`` reports step failures as ``("error", message)``
    so callers must inspect the returned mapping instead of treating a normal
    Python return as success. This gate is shared by every platform importer.
    """
    failed = [
        name
        for name, result in enrichment_results.items()
        if isinstance(result, tuple) and result and result[0] == "error"
    ]
    if failed:
        raise RuntimeError("SQL enrichments failed: " + ", ".join(failed))
    if fantasy_aggregation_ok is False:
        raise RuntimeError("Fantasy aggregation failed after SQL enrichments")


def run_sql_enrichments(
    ctx: Any,
    db_name: str,
    dry_run: bool = False,
    quick: bool = False,
) -> bool:
    """Run SQL enrichments directly in the database after upload.

    Runs optimal lineup, LAMAR calculations, player_to_matchup,
    draft cost buckets, transaction LAMAR, etc.

    Returns:
        True if enrichments succeeded, False on error
    """
    try:
        if dry_run:
            log("[SQL ENRICHMENTS][DRY-RUN] Would run SQL enrichments")
            return True

        from multi_league.transformations.sql_enrichments import SQLEnrichments

        log(f"[SQL ENRICHMENTS] Running on database: {db_name}")

        with SQLEnrichments(
            db_name,
            dry_run=False,
            quick=quick,
            manager_name_overrides=getattr(ctx, "manager_name_overrides", None),
            franchise_merges=getattr(ctx, "franchise_merges", None),
        ) as enricher:
            # Load settings from the active DuckDB target.
            roster_by_year, scoring_params = enricher.load_settings_from_db()
            if roster_by_year:
                log(f"[SQL ENRICHMENTS] Loaded settings: {len(roster_by_year)} years")
                log(
                    f"[SQL ENRICHMENTS] Scoring: {scoring_params.get('ppr', 0.0)} PPR, {scoring_params.get('pass_td_pts', 4)}pt TD"
                )
                enricher.roster_by_year = roster_by_year
                enricher._update_scoring_params(scoring_params)
            else:
                log("[SQL ENRICHMENTS] WARNING: No settings found, optimal lineup may be skipped")

            # Run enrichments in optimal order
            enrichment_results = enricher.run_all()
            timing_results = getattr(enricher, "last_run_timings", {})

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

            require_sql_enrichment_success(enrichment_results)

        log("[SQL ENRICHMENTS] OK: All SQL enrichments completed")
        return True

    except Exception as e:
        log(f"[SQL ENRICHMENTS] FAIL: {e}")
        return False


# =============================================================================
# Local Fantasy Aggregation
# =============================================================================


def run_local_fantasy_aggregation(
    db_name: str,
    data_dir: str | Path,
    dry_run: bool = False,
    conn: Any | None = None,
) -> bool:
    """Build local player_fantasy aggregate tables from DuckDB.

    This gives local imports and GH runners the same season/career fantasy
    aggregates the UI expects without waiting for a post-upload remote step.
    """
    if dry_run:
        log("[FANTASY AGG][DRY-RUN] Would aggregate fantasy season/career tables locally")
        return True

    try:
        if conn is not None:
            from multi_league.transformations.aggregation.aggregate_fantasy_context import run_aggregation

            run_aggregation(conn, db_name)
            log("[FANTASY AGG] OK: Local fantasy aggregates updated on existing DuckDB connection")
            return True

        ok, err = run_script(
            "multi_league/transformations/aggregation/aggregate_fantasy_context.py",
            "Fantasy Aggregation (local)",
            "",
            timeout=900,
            db_name=db_name,
            data_dir=str(data_dir),
        )
        if not ok and err:
            log(f"[FANTASY AGG] FAIL: {err}")
        return ok
    except Exception as e:
        log(f"[FANTASY AGG] FAIL: {e}")
        return False


# =============================================================================
# Phase 1.7: Schema-Conform External Uploads
# =============================================================================


def run_phase_1_7(conn, ctx) -> None:
    """PHASE 1.7 — Schema-Conform external uploads to DDL shape.

    Bridges Fly staging <-> local DuckDB <-> Fly conformed:
      1. Pull ``___leagues.staging.staging_*`` rows for ctx.db_name into local DuckDB.
      2. Run schema_conform.run() against local conn — writes ``staging.conformed_*``.
      3. Push local ``staging.conformed_*`` back to Fly so the existing
         staging_reader picks them up in lieu of the raw staging tables.

    Gated on ctx.has_external_data: the wizard's upload-staging route sets
    this to True when the user uploads external files for a league, and the
    flag is plumbed through dispatch -> workflow -> ctx. For 99%+ of imports
    the flag is False and we skip entirely — no Fly probe, no round-trip.
    The IMPORT_USE_EXTERNAL_STAGING=1 env var forces the run as an ops escape
    hatch (e.g. recovering when staging exists but the dispatch flag was lost).
    League-to-league merge_source imports copy canonical rows in Fly after the
    target upload, so they bypass schema conform unless that env override is set.

    No-op when Fly has zero staging rows for this db_name.
    Aborts non-zero on any of the 4 hard-abort gates; callers must catch
    SchemaConformAbort and handle (e.g. raise SystemExit(EXIT_CODE_SCHEMA_CONFORM_ABORT)).

    Args:
        conn: Active DuckDB connection to the local league database.
        ctx: Any context object understood by get_db_name() (league_name / db_name).
    """
    import os

    db_name = get_db_name(ctx)
    run_id = getattr(ctx, "run_id", None) or "unknown"

    has_external = bool(getattr(ctx, "has_external_data", False))
    env_override = os.environ.get("IMPORT_USE_EXTERNAL_STAGING") == "1"
    if not has_external and not env_override:
        log("[PHASE 1.7] skipped (ctx.has_external_data=False, no Fly probe)")
        return
    if (getattr(ctx, "merge_source", None) or getattr(ctx, "merge_sources", None)) and not env_override:
        log("[PHASE 1.7] skipped (merge_source(s) use worker-side Fly copy)")
        return

    from multi_league.external_ingest import schema_conform
    from multi_league.external_ingest.schema_conform import _fly_bridge

    # Step 1: pull Fly staging rows into local DuckDB.
    pulled = _fly_bridge.pull_staging_to_local(conn, db_name)
    if all(n == 0 for n in pulled.values()):
        return  # no external data — no-op

    # Step 2: conform locally (raises SchemaConformAbort on hard-abort gates).
    schema_conform.run(
        conn=conn,
        db_name=db_name,
        run_id=run_id,
        franchise_merges=getattr(ctx, "franchise_merges", None),
    )

    # Step 3: push conformed_* back to Fly so the merger can read them.
    _fly_bridge.push_conformed_to_fly(conn, db_name)


# =============================================================================
# Combined Post-Upload Pipeline
# =============================================================================


def run_post_upload_pipeline(
    ctx: Any,
    dry_run: bool = False,
    skip_transformations: bool = False,
) -> dict[str, bool]:
    """Run the post-upload pipeline (Phases 4.1, 4.5).

    Aggregation and homepage are handled by the playoff_odds finalization workflow.
    Call this after the Track 2 upload completes.

    Args:
        ctx: Any context object with league_name, data_directory
        dry_run: If True, simulate without running
        skip_transformations: If True, skip SQL enrichments

    Returns:
        Dict of step names to success booleans
    """
    results = {}
    db_name = get_db_name(ctx)

    # Phase 4.1: Deprecated column cleanup
    if not dry_run:
        log("\n[CLEANUP] Dropping deprecated columns...")
        results["deprecated_cleanup"] = run_deprecated_column_cleanup(ctx, db_name, dry_run)

    # Phase 4.5: SQL enrichments
    if not skip_transformations:
        log("\n" + "=" * 96)
        log("PHASE 4.5: SQL Enrichments")
        log("=" * 96)
        results["sql_enrichments"] = run_sql_enrichments(
            ctx,
            db_name,
            dry_run,
            quick=getattr(ctx, "is_single_year_import", False),
        )

    return results


# =============================================================================
# Full Transformation Pipeline (Phase 3)
# =============================================================================


def run_transformation_pipeline(
    ctx: Any,
    dry_run: bool = False,
    skip_track_2_upload: bool = False,
    import_mode: str = "full",
    context_file_path: str | Path | None = None,
    platform: str | None = None,
    pre_upload_fn: Any = None,
    intermediate_upload_fn: Any = None,
    db_name: str | None = None,
    data_dir: str | Path | None = None,
    quick: bool = False,
) -> list[tuple[str, bool]]:
    """Run the full transformation pipeline (Pass 1 -> 2A -> pre-upload -> 2B -> 3).

    This consolidates the Phase 3 logic from all three orchestrators.
    Platform-specific steps (Yahoo backfill, ESPN expected_record) are handled
    via the platform parameter.

    Args:
        ctx: Any context object with data_directory, league_name
        dry_run: If True, simulate without running
        skip_track_2_upload: If True, skip remote uploads between passes
        import_mode: "quick" or "full"
        context_file_path: Explicit path to context JSON file
        platform: "yahoo", "sleeper", or "espn" (auto-detected if None)
        pre_upload_fn: Callable(ctx, dry_run) -> bool for uploading player_fantasy
                       between Pass 2A and 2B. If None, uses default.
        intermediate_upload_fn: Callable(ctx, dry_run) -> dict for uploading
                                intermediate tables after Pass 3. If None, skipped.
        db_name: League database name. When provided with data_dir,
                 child scripts receive --db/--data-dir instead of --context.
        data_dir: Local data directory path. Used with db_name.
        quick: If True and using --db/--data-dir mode, also passes --quick.

    Returns:
        List of (description, success) tuples for all transformations
    """
    all_results: list[tuple[str, bool]] = []

    if platform is None:
        platform = _detect_platform(ctx)

    if context_file_path is None:
        context_file_path = _detect_context_file(ctx)

    if not dry_run and db_name and data_dir:
        try:
            from multi_league.core.local_db import LocalLeagueDB

            with LocalLeagueDB(data_dir, db_name) as db:
                persist_frontend_settings_tables(db, db_name, ctx, platform)
            log("[SETTINGS] Persisted import settings to local DuckDB")
        except Exception as e:
            log(f"[SETTINGS] WARNING: Could not persist import settings locally: {e}")

    # Build platform-specific Pass 1
    pass_1 = list(TRANSFORMATIONS_PASS_1)
    if platform == "espn":
        pass_1.extend(ESPN_PASS_1_EXTRAS)

    # Build platform-specific Pass 2A
    # PASS_2A is now empty — all work is done via SQL enrichments.
    # (backfill_yahoo_points, player_stats_v2, manager_optimal all replaced by SQL)
    pass_2a = list(TRANSFORMATIONS_PASS_2A)  # [] for all platforms

    # Common kwargs for --db/--data-dir forwarding
    _db_kwargs = dict(db_name=db_name, data_dir=data_dir, quick=quick)

    # Pass 1: Base calculations
    pass1_results = run_transformations(
        ctx,
        pass_1,
        "Pass 1: Base Calculations",
        dry_run,
        context_file_path=context_file_path,
        **_db_kwargs,
    )
    all_results.extend(pass1_results)

    # Pass 2A: Create player table + manager optimal
    pass2a_results = run_transformations(
        ctx,
        pass_2a,
        "Pass 2A: Create Player Table",
        dry_run,
        context_file_path=context_file_path,
        **_db_kwargs,
    )
    all_results.extend(pass2a_results)

    # Pre-upload: upload rostered player_fantasy data before NFL expansion.
    if not skip_track_2_upload and import_mode == "full" and pre_upload_fn is not None:
        log("\n[PRE-UPLOAD] Uploading player_fantasy rostered data before NFL expansion...")
        pre_upload_ok = pre_upload_fn(ctx, dry_run)
        if pre_upload_ok:
            log("[PRE-UPLOAD] Complete - rostered data now available")
        else:
            log("[PRE-UPLOAD] FAILED - blocking Pass 2B (full import requires rostered data)")
            all_results.append(("Pre-upload player_fantasy", False))
            return all_results

    # Pass 2B: Expand to all NFL + stats + replacement level
    # For quick imports, expand_to_all_nfl detects single-year mode and only loads current year
    pass2b_results = run_transformations(
        ctx,
        list(TRANSFORMATIONS_PASS_2B),
        "Pass 2B: NFL Expansion + Stats",
        dry_run,
        context_file_path=context_file_path,
        **_db_kwargs,
    )
    all_results.extend(pass2b_results)

    # Pass 3: Draft/transaction LAMAR + finalizations
    pass3_results = run_transformations(
        ctx,
        list(TRANSFORMATIONS_PASS_3),
        "Pass 3: Draft/Transaction LAMAR + Finalize",
        dry_run,
        context_file_path=context_file_path,
        **_db_kwargs,
    )
    all_results.extend(pass3_results)

    # Intermediate upload: upload core tables to Fly.
    if not skip_track_2_upload and intermediate_upload_fn is not None:
        log("\n" + "=" * 96)
        log("INTERMEDIATE UPLOAD: Core Tables")
        log("=" * 96)
        intermediate_results = intermediate_upload_fn(ctx, dry_run)
        if isinstance(intermediate_results, dict):
            for table, success in intermediate_results.items():
                all_results.append((f"Intermediate: {table}", success))

    return all_results
