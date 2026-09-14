#!/usr/bin/env python3
# ruff: noqa: E402
"""Initial import pipeline for Fleaflicker leagues.

This intentionally keeps Fleaflicker platform code separate from Yahoo,
Sleeper, and ESPN import files. The fetchers write canonical local DuckDB
tables, then the shared transformation/upload pipeline takes over.
"""

from __future__ import annotations

import argparse
import os
import sys
import tempfile
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from multi_league.core.date_utils import get_current_nfl_season_year
from multi_league.core.db_utils import get_db_name, sanitize_database_name
from multi_league.core.import_pipeline import run_local_fantasy_aggregation, run_transformation_pipeline
from multi_league.core.import_utils import run_track_1_verify, upload_league_tables
from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.script_runner import log, run_script
from multi_league.core.year_filter_utils import coerce_int, current_state_shell_has_scores
from multi_league.data_fetchers.fleaflicker import (
    FleaflickerAPIClient,
    FleaflickerAPIConfig,
    FleaflickerContext,
    FleaflickerAPIError,
    fetch_all_fleaflicker_drafts,
    fetch_all_fleaflicker_matchups,
    fetch_all_fleaflicker_rosters,
    fetch_all_fleaflicker_schedules,
    fetch_all_fleaflicker_settings,
    fetch_all_fleaflicker_transactions,
)
from multi_league.data_fetchers.fleaflicker.fleaflicker_utils import get_any, team_identity


FETCH_TARGETS = {
    "settings": fetch_all_fleaflicker_settings,
    "matchups": fetch_all_fleaflicker_matchups,
    "rosters": fetch_all_fleaflicker_rosters,
    "draft": fetch_all_fleaflicker_drafts,
    "transactions": fetch_all_fleaflicker_transactions,
    "schedule": fetch_all_fleaflicker_schedules,
}


def _env_int(name: str) -> int | None:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError:
        log(f"[CONFIG] Ignoring invalid {name}={raw!r}")
        return None


def _positive_int(value: int | None, default: int) -> int:
    if value is None:
        return default
    return value if value > 0 else default


def _build_client(args) -> FleaflickerAPIClient:
    default_config = FleaflickerAPIConfig()
    rate_limit = _positive_int(args.rate_limit_per_min, default_config.rate_limit_per_min)
    default_config.rate_limit_per_min = rate_limit
    return FleaflickerAPIClient(default_config)


def _default_full_history_end_year() -> int:
    """Prefer the most recent scored season when the current shell is unplayed."""
    try:
        from multi_league.core.date_utils import get_nfl_state

        state = get_nfl_state() or {}
    except Exception:
        state = {}

    state_year = coerce_int(state.get("league_season") or state.get("season"))
    if state_year is not None and not current_state_shell_has_scores(state):
        return state_year - 1
    return get_current_nfl_season_year()


def _should_discover_full_history(args) -> bool:
    if args.context or args.year or args.import_mode != "full":
        return False
    return bool(args.discover_years or args.start_year is None or args.end_year is None)


def _bootstrap_context(args, client: FleaflickerAPIClient) -> FleaflickerContext:
    if _should_discover_full_history(args):
        discovery_start = args.start_year or 2005
        discovery_end = args.end_year or _default_full_history_end_year()
        available_years = client.discover_available_years(
            args.league_id,
            start_year=discovery_start,
            end_year=discovery_end,
        )
        if not available_years:
            raise ValueError(
                "No Fleaflicker seasons were discovered for "
                f"league_id={args.league_id} in {discovery_start}-{discovery_end}"
            )
        start_year = min(available_years)
        end_year = max(available_years)
        log("[DISCOVERY] Fleaflicker seasons: " f"{start_year}-{end_year} ({len(available_years)} year(s))")
    else:
        current_year = get_current_nfl_season_year()
        end_year = args.end_year or args.year or current_year
        start_year = args.start_year or end_year
        available_years = list(range(start_year, end_year + 1))

    standings = client.fetch_standings(args.league_id, season=end_year) or {}
    league = standings.get("league") or {}
    league_name = league.get("name") or f"Fleaflicker {args.league_id}"

    if args.data_dir:
        data_dir = Path(args.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
    else:
        safe_name = sanitize_database_name(league_name)
        data_dir = Path(tempfile.mkdtemp(prefix=f"fleaflicker_{safe_name}_"))

    league_ids = {str(year): str(args.league_id) for year in available_years}
    return FleaflickerContext(
        league_id=str(args.league_id),
        league_name=league_name,
        start_year=start_year,
        end_year=end_year,
        data_directory=data_dir,
        league_ids=league_ids,
        database_name=args.database_name or sanitize_database_name(league_name),
        import_mode=args.import_mode,
        max_workers=_positive_int(args.max_workers, FleaflickerContext.max_workers),
        rate_limit_per_min=client.config.rate_limit_per_min,
    )


def _load_context(args, client: FleaflickerAPIClient) -> FleaflickerContext:
    if args.context:
        ctx = FleaflickerContext.load(args.context)
        if args.data_dir:
            ctx.data_directory = Path(args.data_dir).resolve()
            ctx._create_directories()
        if args.database_name:
            ctx.database_name = args.database_name
    else:
        ctx = _bootstrap_context(args, client)

    ctx.import_mode = args.import_mode
    ctx.max_workers = _positive_int(args.max_workers, ctx.max_workers)
    ctx.rate_limit_per_min = client.config.rate_limit_per_min
    if args.import_mode == "quick":
        target_year = args.year or args.end_year or get_current_nfl_season_year()
        ctx.start_year = target_year
        ctx.end_year = target_year
        ctx.league_ids = {str(target_year): ctx.get_league_id_for_year(target_year) or ctx.league_id}
    elif args.start_year or args.end_year:
        if args.start_year:
            ctx.start_year = args.start_year
        if args.end_year:
            ctx.end_year = args.end_year
        end_year = ctx.end_year or ctx.start_year
        ctx.league_ids = {str(year): ctx.league_id for year in range(ctx.start_year, end_year + 1)}

    return ctx


def _run_fetchers(ctx: FleaflickerContext, client: FleaflickerAPIClient, db: LocalLeagueDB, args) -> None:
    year_filter = args.year if args.year else None
    targets = [args.fetch] if args.fetch else ["settings", "matchups", "draft", "transactions", "schedule", "rosters"]

    for target in targets:
        fetcher = FETCH_TARGETS[target]
        log(f"[FETCH] Fleaflicker {target}...")
        fetcher(ctx, client=client, year_filter=year_filter, db=db)


def _run_local_sql_enrichments(ctx: FleaflickerContext, db_name: str, quick: bool, dry_run: bool) -> bool:
    if dry_run:
        log("[SQL ENRICHMENTS][DRY-RUN] Would run local SQL enrichments")
        return True

    log("\n" + "=" * 96)
    log("PHASE 3.5: SQL Enrichments (local DuckDB)")
    log("=" * 96)

    eng = None
    try:
        from multi_league.transformations.sql_enrichments import SQLEnrichments

        with LocalLeagueDB(ctx.data_directory, db_name) as db:
            eng = SQLEnrichments(
                db_name=db_name,
                data_dir=str(ctx.data_directory),
                quick=quick,
                conn=db.connect(),
            )
            roster_by_year, scoring_params = eng.load_settings_from_db()
            eng.roster_by_year = roster_by_year
            eng._update_scoring_params(scoring_params)
            enrichment_results = eng.run_all()
            timing_results = getattr(eng, "last_run_timings", {})

            ok_count = sum(1 for value in enrichment_results.values() if not isinstance(value, tuple))
            fail_count = sum(1 for value in enrichment_results.values() if isinstance(value, tuple))
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

            fantasy_agg_ok = run_local_fantasy_aggregation(
                db_name=db_name,
                data_dir=str(ctx.data_directory),
                dry_run=dry_run,
                conn=eng.conn,
            )

        return fail_count == 0 and fantasy_agg_ok
    except Exception as exc:
        log(f"[SQL ENRICHMENTS] FAIL: {exc}")
        return False
    finally:
        if eng is not None:
            eng.close()


def _fallback_placement_ranks_from_matchup(conn, year: int, cols: set[str]) -> dict[str, int]:
    seed_parts = []
    if "final_playoff_seed" in cols:
        seed_parts.append("MIN(CASE WHEN final_playoff_seed IS NOT NULL THEN CAST(final_playoff_seed AS INTEGER) END)")
    if "playoff_seed" in cols:
        seed_parts.append("MIN(CASE WHEN playoff_seed IS NOT NULL THEN CAST(playoff_seed AS INTEGER) END)")
    if len(seed_parts) > 1:
        seed_expr = f"COALESCE({', '.join(seed_parts)})"
    elif seed_parts:
        seed_expr = seed_parts[0]
    else:
        seed_expr = "NULL"
    rows = conn.execute(
        f"""
        SELECT
            franchise_id,
            {seed_expr} AS seed,
            MAX(COALESCE(CAST(champion AS INTEGER), 0)) AS is_champion,
            MAX(COALESCE(CAST(sacko AS INTEGER), 0)) AS is_sacko
        FROM public.matchup
        WHERE year = ?
          AND franchise_id IS NOT NULL
          AND manager IS NOT NULL
          AND TRIM(COALESCE(manager, '')) != ''
          AND LOWER(TRIM(manager)) NOT IN ('unrostered', 'fa', 'free agent', 'waivers')
        GROUP BY franchise_id
        """,
        [year],
    ).fetchall()
    if not rows:
        return {}

    teams = [
        {
            "franchise_id": str(row[0]),
            "seed": int(row[1]) if row[1] is not None else None,
            "is_champion": int(row[2] or 0) == 1,
            "is_sacko": int(row[3] or 0) == 1,
        }
        for row in rows
        if row[0] is not None
    ]
    if not teams:
        return {}

    team_count = len(teams)
    champion_ids = [team["franchise_id"] for team in teams if team["is_champion"]]
    sacko_ids = [team["franchise_id"] for team in teams if team["is_sacko"]]
    champion_id = champion_ids[0] if len(champion_ids) == 1 else None
    sacko_id = sacko_ids[0] if len(sacko_ids) == 1 else None

    rank_by_franchise: dict[str, int] = {}
    next_rank = 1
    if champion_id:
        rank_by_franchise[champion_id] = 1
        next_rank = 2

    reserve_sacko_last = bool(sacko_id and sacko_id not in rank_by_franchise)
    for team in sorted(teams, key=lambda item: (item["seed"] is None, item["seed"] or 9999, item["franchise_id"])):
        franchise_id = team["franchise_id"]
        if franchise_id in rank_by_franchise or (reserve_sacko_last and franchise_id == sacko_id):
            continue
        rank_by_franchise[franchise_id] = next_rank
        next_rank += 1

    if reserve_sacko_last and sacko_id:
        rank_by_franchise[sacko_id] = team_count

    return rank_by_franchise if len(rank_by_franchise) == team_count else {}


def _finalize_missing_playoff_results(ctx: FleaflickerContext, db_name: str, dry_run: bool) -> bool:
    if dry_run:
        log("[PLAYOFF FINALIZE][DRY-RUN] Would fill missing champion/sacko flags")
        return True

    try:
        client = FleaflickerAPIClient()
        with LocalLeagueDB(ctx.data_directory, db_name) as db:
            conn = db.connect()
            cols = {row[0] for row in conn.execute("DESCRIBE public.matchup").fetchall()}
            if "champion" not in cols:
                conn.execute("ALTER TABLE public.matchup ADD COLUMN champion INTEGER")
            if "sacko" not in cols:
                conn.execute("ALTER TABLE public.matchup ADD COLUMN sacko INTEGER")
            if "placement_rank" not in cols:
                conn.execute("ALTER TABLE public.matchup ADD COLUMN placement_rank INTEGER")
            cols = {row[0] for row in conn.execute("DESCRIBE public.matchup").fetchall()}

            years = [
                int(row[0])
                for row in conn.execute(
                    """
                    SELECT DISTINCT year
                    FROM public.matchup
                    WHERE year IS NOT NULL
                    ORDER BY year
                    """
                ).fetchall()
            ]

            champions = 0
            sackos = 0
            placements = 0
            for year in years:
                league_id = ctx.get_league_id_for_year(year)
                rank_by_franchise: dict[str, int] = {}
                if league_id:
                    try:
                        standings = client.fetch_standings(league_id, season=year) or {}
                    except FleaflickerAPIError as exc:
                        if exc.status_code != 403:
                            raise
                        log(
                            "[PLAYOFF FINALIZE] WARN: standings forbidden for "
                            f"league={league_id} year={year}; skipping placement fallback"
                        )
                        standings = {}
                    for division in standings.get("divisions") or []:
                        if not isinstance(division, dict):
                            continue
                        for raw_team in division.get("teams") or []:
                            if not isinstance(raw_team, dict):
                                continue
                            team = team_identity(raw_team)
                            rank = get_any(get_any(raw_team, "recordOverall", "record_overall"), "rank")
                            try:
                                if team["franchise_id"] and rank is not None:
                                    rank_by_franchise[str(team["franchise_id"])] = int(rank)
                            except (TypeError, ValueError):
                                continue

                champ_exists = conn.execute(
                    """
                    SELECT COUNT(DISTINCT franchise_id)
                    FROM public.matchup
                    WHERE year = ?
                      AND COALESCE(CAST(champion AS INTEGER), 0) = 1
                    """,
                    [year],
                ).fetchone()[0]
                if not champ_exists and "p_champ" in cols:
                    champ_row = conn.execute(
                        """
                        SELECT franchise_id, MAX(week) AS final_week
                        FROM public.matchup
                        WHERE year = ?
                          AND COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1
                          AND franchise_id IS NOT NULL
                          AND COALESCE(CAST(p_champ AS DOUBLE), 0) >= 99.5
                        GROUP BY franchise_id
                        ORDER BY MAX(CAST(p_champ AS DOUBLE)) DESC, final_week DESC
                        LIMIT 1
                        """,
                        [year],
                    ).fetchone()
                    if champ_row and champ_row[0] is not None and champ_row[1] is not None:
                        conn.execute("UPDATE public.matchup SET champion = 0 WHERE year = ?", [year])
                        conn.execute(
                            """
                            UPDATE public.matchup
                            SET champion = 1
                            WHERE year = ?
                              AND franchise_id = ?
                              AND week = ?
                            """,
                            [year, champ_row[0], int(champ_row[1])],
                        )
                        champions += 1

                sacko_exists = conn.execute(
                    """
                    SELECT COUNT(DISTINCT franchise_id)
                    FROM public.matchup
                    WHERE year = ?
                      AND COALESCE(CAST(sacko AS INTEGER), 0) = 1
                    """,
                    [year],
                ).fetchone()[0]
                if not sacko_exists and "final_playoff_seed" in cols:
                    sacko_row = conn.execute(
                        """
                        SELECT franchise_id, MAX(week) AS final_week
                        FROM public.matchup
                        WHERE year = ?
                          AND COALESCE(CAST(is_consolation AS INTEGER), 0) = 1
                          AND franchise_id IS NOT NULL
                          AND final_playoff_seed IS NOT NULL
                        GROUP BY franchise_id
                        ORDER BY MAX(CAST(final_playoff_seed AS INTEGER)) DESC, final_week DESC
                        LIMIT 1
                        """,
                        [year],
                    ).fetchone()
                    if sacko_row and sacko_row[0] is not None and sacko_row[1] is not None:
                        conn.execute("UPDATE public.matchup SET sacko = 0 WHERE year = ?", [year])
                        conn.execute(
                            """
                            UPDATE public.matchup
                            SET sacko = 1
                            WHERE year = ?
                              AND franchise_id = ?
                              AND week = ?
                            """,
                            [year, sacko_row[0], int(sacko_row[1])],
                        )
                        sackos += 1

                if not rank_by_franchise:
                    rank_by_franchise = _fallback_placement_ranks_from_matchup(conn, year, cols)
                    if rank_by_franchise:
                        log(
                            "[PLAYOFF FINALIZE] Derived seed-based placement fallback for "
                            f"year={year}: {len(rank_by_franchise)} teams"
                        )

                if rank_by_franchise:
                    conn.execute("UPDATE public.matchup SET placement_rank = NULL WHERE year = ?", [year])
                    for franchise_id, rank in rank_by_franchise.items():
                        conn.execute(
                            """
                            UPDATE public.matchup
                            SET placement_rank = ?
                            WHERE year = ?
                              AND franchise_id = ?
                            """,
                            [rank, year, franchise_id],
                        )
                    placements += len(rank_by_franchise)

            log(f"[PLAYOFF FINALIZE] Filled placements={placements}, missing champions={champions}, sackos={sackos}")
        return True
    except Exception as exc:
        log(f"[PLAYOFF FINALIZE] FAIL: {exc}")
        return False


def _run_local_finalization(ctx: FleaflickerContext, db_name: str, quick: bool, dry_run: bool) -> bool:
    if dry_run:
        log("[FINALIZE][DRY-RUN] Would run playoff, luck, aggregate, and homepage finalization")
        return True

    data_dir = str(ctx.data_directory)
    steps = [
        (
            "multi_league/transformations/matchup/playoff_odds_import.py",
            "Playoff Odds (local)",
            ["--n-sims", "1000"],
            2400,
        ),
        (
            "multi_league/transformations/matchup/expected_record_v2.py",
            "Expected Record / Schedule Luck (local)",
            ["--n-sims", "1000", "--seed", "42"],
            2400,
        ),
    ]

    for script, label, extra_args, timeout in steps:
        ok, err = run_script(
            script,
            label,
            "",
            additional_args=extra_args,
            timeout=timeout,
            db_name=db_name,
            data_dir=data_dir,
        )
        if not ok:
            if err:
                log(f"[FINALIZE] FAIL {label}: {err}")
            return False

    if not _finalize_missing_playoff_results(ctx, db_name, dry_run):
        return False

    aggregate_steps = [
        ("multi_league/transformations/aggregation/aggregate_fantasy_context.py", "Fantasy Aggregation (final)"),
        ("multi_league/transformations/aggregation/aggregate_matchup_context.py", "Matchup Aggregation (final)"),
        ("multi_league/transformations/aggregation/aggregate_standings.py", "Standings Aggregation (final)"),
        ("multi_league/transformations/aggregation/aggregate_draft_context.py", "Draft Aggregation (final)"),
        (
            "multi_league/transformations/aggregation/aggregate_transaction_context.py",
            "Transaction Aggregation (final)",
        ),
        ("multi_league/transformations/aggregation/homepage_summary.py", "Homepage Summary (final)"),
    ]
    for script, label in aggregate_steps:
        ok, err = run_script(
            script,
            label,
            "",
            timeout=1800,
            db_name=db_name,
            data_dir=data_dir,
        )
        if not ok:
            if err:
                log(f"[FINALIZE] FAIL {label}: {err}")
            return False

    log("[FINALIZE] OK: Local Fleaflicker finalization complete")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Import a Fleaflicker fantasy football league")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--league-id", help="Fleaflicker league ID")
    source.add_argument("--context", help="Path to fleaflicker_context.json")
    parser.add_argument("--data-dir", help="Local data directory")
    parser.add_argument("--database-name", help="Override target database name")
    parser.add_argument("--start-year", type=int, help="First season to import")
    parser.add_argument("--end-year", type=int, help="Last season to import")
    parser.add_argument("--year", type=int, help="Target a single season")
    parser.add_argument(
        "--discover-years",
        action="store_true",
        help="Discover public Fleaflicker seasons for full-history imports",
    )
    parser.add_argument("--fetch", choices=sorted(FETCH_TARGETS), help="Run only one fetcher")
    parser.add_argument(
        "--rate-limit-per-min",
        type=int,
        default=_env_int("FLEAFLICKER_RATE_LIMIT_PER_MIN"),
        help="Fleaflicker API request budget per minute; 0 disables client-side throttling (default/env: 60)",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=_env_int("FLEAFLICKER_MAX_WORKERS"),
        help="Concurrent Fleaflicker roster requests per week (default/env: 1)",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-fetchers", action="store_true", help="Use existing local tables")
    parser.add_argument("--skip-track-1", action="store_true", help="Skip NFL super table verification")
    parser.add_argument("--skip-transformations", action="store_true", help="Skip shared transformations")
    parser.add_argument("--skip-track-2-upload", action="store_true", help="Skip Fly upload")
    parser.add_argument("--import-mode", choices=["quick", "full"], default="full")
    parser.add_argument(
        "--stop-after",
        type=int,
        choices=[0, 1, 2, 3, 4, 5],
        help="0=settings, 1=fetch, 2=shared transforms, 3=sql enrichments, 4=finalization, 5=upload",
    )
    args = parser.parse_args()

    client = _build_client(args)
    ctx = _load_context(args, client)
    context_path = ctx.save()
    db_name = get_db_name(ctx)
    end_year = ctx.end_year or ctx.start_year

    log("=" * 72)
    log(f"Fleaflicker Import: {ctx.league_name}")
    log(f"League ID: {ctx.league_id}")
    log(f"Database: {db_name}")
    log(f"Years: {ctx.start_year}-{end_year}")
    log(f"Mode: {ctx.import_mode}")
    log(f"API rate limit: {ctx.rate_limit_per_min}/min")
    log(f"Roster workers: {ctx.max_workers}")
    log(f"Data directory: {ctx.data_directory}")
    log("=" * 72)

    if not args.skip_track_1:
        run_track_1_verify(ctx.start_year, end_year, dry_run=args.dry_run)

    with LocalLeagueDB(ctx.data_directory, db_name) as db:
        if not args.skip_fetchers:
            _run_fetchers(ctx, client, db, args)
        else:
            log("[FETCH] Skipped; using existing local DuckDB tables")

    if args.stop_after is not None and args.stop_after <= 1:
        log("[STOP] Stopping after fetch phase")
        return

    if not args.skip_transformations:
        results = run_transformation_pipeline(
            ctx,
            dry_run=args.dry_run,
            skip_track_2_upload=args.skip_track_2_upload,
            import_mode=ctx.import_mode,
            context_file_path=context_path,
            platform="fleaflicker",
            db_name=db_name,
            data_dir=ctx.data_directory,
            quick=ctx.import_mode == "quick",
        )
        if any(not ok for _, ok in results):
            log("[TRANSFORM] One or more transformations failed")
            sys.exit(1)
    else:
        log("[TRANSFORM] Skipped")

    if args.stop_after is not None and args.stop_after <= 2:
        log("[STOP] Stopping after transformation phase")
        return

    if not args.skip_transformations:
        sql_ok = _run_local_sql_enrichments(
            ctx,
            db_name,
            quick=ctx.import_mode == "quick",
            dry_run=args.dry_run,
        )
        if not sql_ok:
            sys.exit(1)
    else:
        log("[SQL ENRICHMENTS] Skipped")

    if args.stop_after is not None and args.stop_after <= 3:
        log("[STOP] Stopping after SQL enrichment phase")
        return

    if not args.skip_transformations:
        final_ok = _run_local_finalization(
            ctx,
            db_name,
            quick=ctx.import_mode == "quick",
            dry_run=args.dry_run,
        )
        if not final_ok:
            sys.exit(1)
    else:
        log("[FINALIZE] Skipped")

    if args.stop_after is not None and args.stop_after <= 4:
        log("[STOP] Stopping after finalization phase")
        return

    if not args.skip_track_2_upload:
        with LocalLeagueDB(ctx.data_directory, db_name) as db:
            upload_results = upload_league_tables(db, db_name, ctx, platform="fleaflicker", dry_run=args.dry_run)
        if any(not ok for _, ok in upload_results):
            sys.exit(1)
    else:
        log("[TRACK 2] Upload skipped")


if __name__ == "__main__":
    main()
