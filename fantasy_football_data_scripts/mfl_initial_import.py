#!/usr/bin/env python3
# ruff: noqa: E402
"""Initial import pipeline for MyFantasyLeague (MFL) leagues.

This intentionally keeps MFL platform code separate from Yahoo, Sleeper,
ESPN, and Fleaflicker import files. The fetchers write canonical local
DuckDB tables, then the shared transformation/upload pipeline takes over.

MFL quirk this orchestrator owns: league IDs are per-SEASON namespaces, so
``--league-id`` accepts ``YEAR:ID`` (e.g. ``2024:63886``) as the seed and
the league's own ``history.league`` links resolve the per-year id map.
"""

from __future__ import annotations

import argparse
import os
import re
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
from multi_league.data_fetchers.mfl import (
    MFLAPIClient,
    MFLAPIConfig,
    MFLAPIError,
    MFLContext,
    fetch_all_mfl_drafts,
    fetch_all_mfl_matchups,
    fetch_all_mfl_rosters,
    fetch_all_mfl_schedules,
    fetch_all_mfl_settings,
    fetch_all_mfl_transactions,
)
from multi_league.data_fetchers.mfl.mfl_utils import as_list, get_any


FETCH_TARGETS = {
    "settings": fetch_all_mfl_settings,
    "matchups": fetch_all_mfl_matchups,
    "rosters": fetch_all_mfl_rosters,
    "draft": fetch_all_mfl_drafts,
    "transactions": fetch_all_mfl_transactions,
    "schedule": fetch_all_mfl_schedules,
}

_HISTORY_URL_RE = re.compile(r"/(\d{4})/home/(\d+)")


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


def _build_client(args) -> MFLAPIClient:
    default_config = MFLAPIConfig()
    rate_limit = _positive_int(args.rate_limit_per_min, default_config.rate_limit_per_min)
    default_config.rate_limit_per_min = rate_limit
    return MFLAPIClient(default_config)


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


def _parse_league_seed(raw: str) -> tuple[int, str]:
    """Parse ``--league-id`` as ``YEAR:ID`` or a bare id (seed year = current)."""
    text = str(raw or "").strip()
    if ":" in text:
        year_part, _, id_part = text.partition(":")
        try:
            seed_year = int(year_part)
        except ValueError as exc:
            raise ValueError(f"Invalid --league-id {raw!r}; expected YEAR:ID (e.g. 2024:63886)") from exc
        seed_id = id_part.strip()
    else:
        seed_year = _default_full_history_end_year()
        seed_id = text
    if not seed_id.isdigit():
        raise ValueError(f"Invalid --league-id {raw!r}; MFL league ids are numeric")
    return seed_year, seed_id


def _history_entries(league_payload: dict) -> dict[str, str]:
    """Extract {year: league_id} from a league payload's history links."""
    entries: dict[str, str] = {}
    history = get_any((league_payload or {}).get("history") or {}, "league")
    for item in as_list(history):
        if not isinstance(item, dict):
            continue
        url = str(get_any(item, "url") or "")
        match = _HISTORY_URL_RE.search(url)
        year = get_any(item, "year") or (match.group(1) if match else None)
        league_id = match.group(2) if match else None
        try:
            year_int = int(year)
        except (TypeError, ValueError):
            continue
        if league_id:
            entries[str(year_int)] = str(league_id)
    return entries


def _resolve_league_history(
    client: MFLAPIClient,
    seed_year: int,
    seed_id: str,
    *,
    max_expansions: int = 32,
) -> tuple[dict[str, str], dict]:
    """Walk history links from the seed season to build {year: league_id}.

    The seed fetch is authoritative; older seasons' own history payloads are
    expanded (bounded) in case they link even earlier seasons.
    Returns (league_ids, seed_league_payload).
    """
    seed_league = client.fetch_league(seed_id, seed_year) or {}
    league_ids: dict[str, str] = {str(seed_year): str(seed_id)}
    league_ids.update(_history_entries(seed_league))

    visited: set[tuple[str, str]] = {(str(seed_year), str(seed_id))}
    invalid_years: set[str] = set()
    for _ in range(max_expansions):
        unvisited = sorted(
            (int(year), league_id)
            for year, league_id in league_ids.items()
            if (year, league_id) not in visited
        )
        if not unvisited:
            break
        year, league_id = unvisited[0]  # expand the oldest known season
        visited.add((str(year), str(league_id)))
        try:
            league = client.fetch_league(league_id, year) or {}
        except MFLAPIError as exc:
            log(f"[DISCOVERY] MFL history probe failed for {year}:{league_id}: {exc}")
            # History links can outlive the underlying MFL league object (and
            # some old links are simply malformed).  Do not retain an ID that
            # has already proved invalid: fetchers later iterate league_ids and
            # one stale link would otherwise abort the entire multi-year import.
            league_ids.pop(str(year), None)
            invalid_years.add(str(year))
            continue
        before = len(league_ids)
        league_ids.update(
            {
                y: lid
                for y, lid in _history_entries(league).items()
                if y not in league_ids and y not in invalid_years
            }
        )
        if len(league_ids) == before:
            # This particular historical season may be a leaf even though
            # other discovered seasons still need probing.  Breaking here
            # leaves later stale IDs (including invalid league links) in the
            # context and the fetch phase then aborts the whole import.
            continue
    return league_ids, seed_league


def _bootstrap_context(args, client: MFLAPIClient) -> MFLContext:
    seed_year, seed_id = _parse_league_seed(args.league_id)
    league_ids, seed_league = _resolve_league_history(client, seed_year, seed_id)
    league_name = seed_league.get("name") or f"MFL {seed_id}"

    cap_year = args.end_year or args.year or _default_full_history_end_year()
    floor_year = args.start_year or 1997
    filtered_ids = {
        year: league_id
        for year, league_id in league_ids.items()
        if floor_year <= int(year) <= cap_year
    }
    if not filtered_ids:
        filtered_ids = {str(seed_year): str(seed_id)}

    years = sorted(int(year) for year in filtered_ids)
    start_year = years[0]
    end_year = years[-1]
    log(f"[DISCOVERY] MFL seasons: {start_year}-{end_year} ({len(years)} year(s)) via history links")

    if args.data_dir:
        data_dir = Path(args.data_dir)
        data_dir.mkdir(parents=True, exist_ok=True)
    else:
        safe_name = sanitize_database_name(league_name)
        data_dir = Path(tempfile.mkdtemp(prefix=f"mfl_{safe_name}_"))

    return MFLContext(
        league_id=str(seed_id),
        league_name=league_name,
        start_year=start_year,
        end_year=end_year,
        data_directory=data_dir,
        league_ids=filtered_ids,
        database_name=args.database_name or sanitize_database_name(league_name),
        import_mode=args.import_mode,
        max_workers=1,
        rate_limit_per_min=client.config.rate_limit_per_min,
    )


def _load_context(args, client: MFLAPIClient) -> MFLContext:
    if args.context:
        ctx = MFLContext.load(args.context)
        if args.data_dir:
            ctx.data_directory = Path(args.data_dir).resolve()
            ctx._create_directories()
        if args.database_name:
            ctx.database_name = args.database_name
    else:
        ctx = _bootstrap_context(args, client)

    ctx.import_mode = args.import_mode
    ctx.max_workers = 1  # MFL throttle: single-threaded only
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
        ctx.league_ids = {
            str(year): ctx.get_league_id_for_year(year) or ctx.league_id
            for year in range(ctx.start_year, end_year + 1)
            if str(year) in ctx.league_ids or ctx.league_ids.get(str(year)) is not None
        } or {str(year): ctx.league_id for year in range(ctx.start_year, end_year + 1)}

    return ctx


def _run_fetchers(ctx: MFLContext, client: MFLAPIClient, db: LocalLeagueDB, args) -> None:
    year_filter = args.year if args.year else None
    targets = [args.fetch] if args.fetch else ["settings", "matchups", "draft", "transactions", "schedule", "rosters"]

    for target in targets:
        fetcher = FETCH_TARGETS[target]
        log(f"[FETCH] MFL {target}...")
        fetcher(ctx, client=client, year_filter=year_filter, db=db)

    if not args.fetch or {"settings", "matchups"} <= set(targets):
        _reconcile_playoff_settings(db)


def _reconcile_playoff_settings(db: LocalLeagueDB) -> None:
    """Backfill playoff_teams / playoff_start_week from real playoff games.

    Small MFL leagues often carry no named championship bracket (playoffBrackets is
    empty or unlabeled), leaving playoff_teams NULL — which hard-fails the playoff
    sim ("Missing required playoff setting 'num_playoff_teams'"). weeklyResults
    flags playoff games directly, so the matchup table is the honest source: the
    field size is the number of non-consolation participants in the first playoff
    week. Years with no playoff games at all stay NULL, correctly.
    """
    con = db.connect()  # shared cached connection -- do NOT close it here
    # A league whose fetch produced no matchup rows has no table to derive from -- skip the
    # backfill instead of crashing the import (observed: empty/private leagues died here in
    # crawl run 29626415574 with "Table with name matchup does not exist").
    have = {r[0] for r in con.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema='public'").fetchall()}
    if "matchup" not in have or "league_settings" not in have:
        log("[SETTINGS] no matchup/league_settings table yet -- skipping playoff reconciliation")
        return
    con.execute("""
            UPDATE public.league_settings ls SET playoff_start_week = (
                SELECT MIN(m.week) FROM public.matchup m
                WHERE m.year = ls.year AND m.is_playoffs
            )
            WHERE ls.playoff_start_week IS NULL
              AND EXISTS (SELECT 1 FROM public.matchup m WHERE m.year = ls.year AND m.is_playoffs)
        """)
    con.execute("""
            UPDATE public.league_settings ls SET regular_season_weeks = ls.playoff_start_week - 1
            WHERE ls.regular_season_weeks IS NULL AND ls.playoff_start_week IS NOT NULL
        """)
    con.execute("""
            UPDATE public.league_settings ls SET playoff_teams = (
                SELECT COUNT(DISTINCT m.franchise_id) FROM public.matchup m
                WHERE m.year = ls.year AND m.is_playoffs
                  AND NOT COALESCE(m.is_consolation, FALSE)
                  AND m.week = (
                      SELECT MIN(m2.week) FROM public.matchup m2
                      WHERE m2.year = ls.year AND m2.is_playoffs
                        AND NOT COALESCE(m2.is_consolation, FALSE)
                  )
            )
            WHERE ls.playoff_teams IS NULL
              AND EXISTS (
                  SELECT 1 FROM public.matchup m WHERE m.year = ls.year AND m.is_playoffs
                    AND NOT COALESCE(m.is_consolation, FALSE)
              )
        """)
    # Unplayed shell seasons (league renewed, zero activity) must not count as league-years.
    # A year with lineups but no H2H games is a TOTAL-POINTS season, not a shell — keep it
    # (usage metrics remain valid; H2H metrics stay NULL). Gate on the matchup table being
    # non-empty so a failed matchup fetch can't nuke settings.
    if con.execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0] > 0:
        shells = con.execute("""
            DELETE FROM public.league_settings ls
            WHERE NOT EXISTS (SELECT 1 FROM public.matchup m WHERE m.year = ls.year)
              AND NOT EXISTS (SELECT 1 FROM public.player_fantasy pf WHERE pf.year = ls.year)
            RETURNING ls.year
        """).fetchall()
        if shells:
            log(f"[FETCH] Dropped {len(shells)} unplayed shell season(s): {sorted(y for (y,) in shells)}")

    # bye_teams: next power of two above the field, minus the field (sim requires it)
    con.execute("""
        UPDATE public.league_settings SET bye_teams = GREATEST(
            0,
            CAST(POWER(2, CEIL(LOG2(playoff_teams))) AS INTEGER) - playoff_teams
        )
        WHERE bye_teams IS NULL AND playoff_teams IS NOT NULL AND playoff_teams > 1
    """)
    filled = con.execute(
        "SELECT COUNT(*) FROM public.league_settings WHERE playoff_teams IS NOT NULL"
    ).fetchone()[0]
    log(f"[FETCH] Playoff settings reconciled from matchup data ({filled} year(s) have playoff_teams)")


def _reconcile_playoff_outcomes(db: LocalLeagueDB) -> None:
    """Re-derive is_championship/champion on the FINAL classified matchup rows.

    Runs AFTER the transformation pipeline: fetch-time regularSeason flags are
    unreliable for some seasons, and a transform re-initializes champion=0 for
    years it touches without re-crowning (observed: 2021 all-zero while
    2022-2025 kept their fetch-time champions). The walk on final flags is the
    single source of truth for both columns.
    """
    from multi_league.core.canonical_matchup import apply_playoff_outcomes

    con = db.connect()  # shared cached connection -- do NOT close it here
    years = [y for (y,) in con.execute("SELECT DISTINCT year FROM public.matchup ORDER BY year").fetchall()]
    total = 0
    for year in years:
        rows = con.execute("""
            SELECT week, franchise_id, opponent_franchise_id, win, loss, team_points,
                   is_playoffs, is_consolation
            FROM public.matchup WHERE year = ?
        """, [year]).fetchdf().to_dict("records")
        apply_playoff_outcomes(rows)
        con.execute("UPDATE public.matchup SET champion = 0, is_championship = FALSE WHERE year = ?", [year])
        for r in rows:
            if r.get("is_championship"):
                total += int(r.get("champion") or 0)
                con.execute("""
                    UPDATE public.matchup SET is_championship = TRUE, champion = ?
                    WHERE year = ? AND week = ? AND franchise_id = ?
                """, [int(r.get("champion") or 0), year, int(r["week"]), r["franchise_id"]])
    # Mirror the fresh flags onto player_fantasy (its champion column is copied from
    # matchup during transforms and would otherwise stay stale for re-crowned years).
    con.execute("UPDATE public.player_fantasy SET champion = 0 WHERE champion IS NOT NULL")
    con.execute("""
        UPDATE public.player_fantasy pf SET champion = m.champion
        FROM public.matchup m
        WHERE m.year = pf.year AND m.week = pf.week AND m.franchise_id = pf.franchise_id
          AND m.champion = 1
    """)
    log(f"[RECONCILE] Champions re-derived on final playoff flags: {total} champion(s) across {len(years)} year(s)")


def _run_local_sql_enrichments(ctx: MFLContext, db_name: str, quick: bool, dry_run: bool) -> bool:
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


def _finalize_missing_playoff_results(ctx: MFLContext, db_name: str, dry_run: bool) -> bool:
    """Fill champion/sacko/placement from local matchup data.

    Unlike Fleaflicker there is no standings-rank API worth trusting here, so
    this uses only the matchup-derived fallback (p_champ / final_playoff_seed
    columns written by the shared sims).
    """
    if dry_run:
        log("[PLAYOFF FINALIZE][DRY-RUN] Would fill missing champion/sacko flags")
        return True

    try:
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

                rank_by_franchise = _fallback_placement_ranks_from_matchup(conn, year, cols)
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


def _run_local_finalization(ctx: MFLContext, db_name: str, quick: bool, dry_run: bool) -> bool:
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

    log("[FINALIZE] OK: Local MFL finalization complete")
    return True


def main() -> None:
    parser = argparse.ArgumentParser(description="Import a MyFantasyLeague (MFL) fantasy football league")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--league-id", help="MFL league seed as YEAR:ID (e.g. 2024:63886) or a bare league ID")
    source.add_argument("--context", help="Path to mfl_context.json")
    parser.add_argument("--data-dir", help="Local data directory")
    parser.add_argument("--database-name", help="Override target database name")
    parser.add_argument("--start-year", type=int, help="First season to import")
    parser.add_argument("--end-year", type=int, help="Last season to import")
    parser.add_argument("--year", type=int, help="Target a single season")
    parser.add_argument("--fetch", choices=sorted(FETCH_TARGETS), help="Run only one fetcher")
    parser.add_argument(
        "--rate-limit-per-min",
        type=int,
        default=_env_int("MFL_RATE_LIMIT_PER_MIN"),
        help="MFL API request budget per minute (default/env: 25; MFL throttles hard past ~70 fast requests)",
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
    log(f"[DISCOVERY] resolved league ids: {ctx.league_ids}")

    log("=" * 72)
    log(f"MFL Import: {ctx.league_name}")
    log(f"League ID (seed): {ctx.league_id}")
    log(f"Database: {db_name}")
    log(f"Years: {ctx.start_year}-{end_year}")
    log(f"Mode: {ctx.import_mode}")
    log(f"API rate limit: {ctx.rate_limit_per_min}/min (single-threaded)")
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
            platform="mfl",
            db_name=db_name,
            data_dir=ctx.data_directory,
            quick=ctx.import_mode == "quick",
        )
        if any(not ok for _, ok in results):
            log("[TRANSFORM] One or more transformations failed")
            sys.exit(1)
        with LocalLeagueDB(ctx.data_directory, db_name) as db:
            _reconcile_playoff_outcomes(db)
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
            upload_results = upload_league_tables(db, db_name, ctx, platform="mfl", dry_run=args.dry_run)
        if any(not ok for _, ok in upload_results):
            sys.exit(1)
    else:
        log("[TRACK 2] Upload skipped")


if __name__ == "__main__":
    main()
