#!/usr/bin/env python3
"""
INITIAL IMPORT V3 - In-Process Yahoo Pipeline

Replaces subprocess-based fetcher invocation (v2) with direct in-process
function calls for lower overhead and better error handling.

TWO-TRACK ARCHITECTURE:
- Track 1: Verifies NFL super table (___ops.nfl_historical.nfl_player_stats_all)
          Run ONCE globally before processing any leagues
- Track 2: Uploads league fantasy rows into centralized Fly DuckDB tables
          (scoped by `db_name` in `___leagues.public.*`)
          Run per-league with fantasy context (manager, position, points)

Join Key: player_week = {NFL_player_id}_{year}_{week}

What this does:
1) Track 1: Verify NFL super table has data (once globally)
2) Fetchers: Yahoo matchups, rosters, schedule, draft, transactions (in-process)
3) Roster data normalized via canonical_roster.normalize_roster_df() -> local DuckDB
4) Local SQL enrichments resolve NFL_player_id via player_bio
5) Transformations: cumulative stats, expected record, playoff odds, etc.
6) Upload to Fly and run post-upload SQL enrichments

Usage:
  python initial_import_v3.py --context path/to/league_context.json
  python initial_import_v3.py --league the_league                     # pulls credentials from Fly ops tables
  python initial_import_v3.py --context path/to/league_context.json --dry-run
  python initial_import_v3.py --context path/to/league_context.json --skip-fetchers
  python initial_import_v3.py --context path/to/league_context.json --skip-track-1
  python initial_import_v3.py --context path/to/league_context.json --fetch matchups --year 2024
"""

from __future__ import annotations

import argparse
import os
import sys

_verbose = "--verbose" in sys.argv

import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import json
import re

# Import new modular utilities
from multi_league.core.date_utils import get_current_nfl_season_year, get_nfl_state
from multi_league.core.fetch_runtime import runtime_from_source
from multi_league.core.script_runner import log
from multi_league.core.scoring_variant import derive_scoring_variant
from multi_league.core.year_filter_utils import (
    coerce_int,
    format_year_filter,
    resolve_history_years,
    resolve_most_recent_available_year,
    resolve_quick_import_years,
    unscored_current_shell_years,
)
from multi_league.core.yahoo_league_settings import fetch_league_settings
from multi_league.data_fetchers.yahoo.fetch_failure_manifest import (
    write_manifest,
    clear_manifest,
    clear_all_manifests,
)

# normalize_dst_player_names, normalize_player_name_suffixes used only in --utility mode (lazy-imported there)
from multi_league.data_fetchers.shared.staging_data_merger import merge_staging_data
from multi_league.data_fetchers.shared.staging_reader import (
    clear_staging_tables,
    pre_fetch_staging_settings,
    read_staging_data,
)
from multi_league.core.league_context import LeagueContext
from multi_league.core.local_db import LocalLeagueDB

# canonical script dir
SCRIPT_DIR = Path(__file__).parent
if str(SCRIPT_DIR.parent / "scripts") not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR.parent / "scripts"))

YAHOO_NFL_GAME_KEY_TO_SEASON = {
    348: 2015,
    359: 2016,
    371: 2017,
    380: 2018,
    390: 2019,
    399: 2020,
    406: 2021,
    414: 2022,
    423: 2023,
    449: 2024,
    461: 2025,
    470: 2026,
}


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


from multi_league.core.import_utils import (
    drop_league_tables as _shared_drop_league_tables,
    run_track_1_verify as _shared_track_1_verify,
    upload_league_tables as _shared_upload_league_tables,
)
from multi_league.core.import_pipeline import (
    require_sql_enrichment_success as _require_sql_enrichment_success,
    run_local_fantasy_aggregation,
    run_post_upload_pipeline,
    run_transformation_pipeline,
)


# harmonize_dtypes/coerce_staging_dtypes used only in staging data paths
from multi_league.core.data_normalization import harmonize_dtypes, coerce_staging_dtypes


# Helper to load context (compatibility wrapper)
def _load_ctx(context_path: str):
    return LeagueContext.load(context_path)


def _create_local_sql_enricher(ctx, db_name: str, data_dir: str, conn, enricher_cls=None):
    """Build the local enricher with persisted manager identity settings."""
    if enricher_cls is None:
        from multi_league.transformations.sql_enrichments import SQLEnrichments

        enricher_cls = SQLEnrichments

    return enricher_cls(
        db_name=db_name,
        data_dir=str(data_dir),
        ppr=0.5,
        pass_td_pts=4,
        quick=ctx.is_single_year_import,
        conn=conn,
        manager_name_overrides=getattr(ctx, "manager_name_overrides", None),
        franchise_merges=getattr(ctx, "franchise_merges", None),
    )


def _get_shared_yahoo_session(ctx, oauth_file: Path | None):
    """Return the context's active Yahoo transport for parallel settings fetches."""
    if getattr(ctx, "yahoo_auth_mode", "oauth") == "cookie":
        return ctx.get_oauth_session()

    if oauth_file and oauth_file.exists():
        from yahoo_oauth import OAuth2

        return OAuth2(None, None, from_file=str(oauth_file))
    if getattr(ctx, "oauth_credentials", None):
        return ctx.get_oauth_session()
    return None


def _parse_context_json(raw, default):
    if not raw:
        return default
    try:
        parsed = json.loads(raw)
    except Exception:
        return default
    return parsed if parsed is not None else default


def _parse_context_bool(raw, default=False):
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, int | float):
        return bool(raw)
    if isinstance(raw, str):
        return raw.strip().lower() in {"1", "true", "yes", "on"}
    return default


def _load_frontend_context_settings(reader, safe_db: str, *, strict_identity: bool = False) -> dict:
    """Read frontend-owned import settings from Fly for --league reimports."""
    if strict_identity:
        # Quick imports must distinguish no saved row from an unavailable or
        # malformed read. Never use the legacy fallback that omits merges.
        fields = {"manager_name_overrides": dict, "franchise_merges": list}
        try:
            rows = reader.query(
                "SELECT manager_name_overrides_json, franchise_merges_json "
                f"FROM public.league_context WHERE db_name = '{safe_db}' LIMIT 2",
                database="___leagues",
            )
        except Exception as exc:
            raise RuntimeError("Could not read saved quick identity settings") from exc
        if not isinstance(rows, list) or len(rows) > 1:
            raise ValueError("Ambiguous saved quick identity settings")
        if not rows:
            return {}
        row = rows[0]
        settings = {}
        for field, kind in fields.items():
            column = f"{field}_json"
            if not isinstance(row, dict) or column not in row:
                raise ValueError(f"Missing saved identity field: {column}")
            raw = row[column]
            try:
                value = kind() if raw is None or raw == "" else json.loads(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"Malformed saved identity field: {column}") from exc
            if not isinstance(value, kind):
                raise ValueError(f"Malformed saved identity field: {column}")
            settings[field] = value
        return settings

    full_cols = (
        "league_name, league_ids_json, manager_name_overrides_json, franchise_merges_json, "
        "keeper_rules_json, league_rules_json, standings_weights_json, is_private"
    )
    fallback_cols = "manager_name_overrides_json, keeper_rules_json, " "league_rules_json, standings_weights_json"
    for cols in (full_cols, fallback_cols):
        try:
            rows = reader.query(
                f"SELECT {cols} FROM public.league_context WHERE db_name = '{safe_db}' LIMIT 1",
                database="___leagues",
            )
        except Exception:
            continue
        if not rows:
            return {}
        row = rows[0]
        return {
            # Canonical: set at first import, changed only in Settings.
            "league_name": (row.get("league_name") or "").strip(),
            "league_ids": _parse_context_json(row.get("league_ids_json"), {}),
            "manager_name_overrides": _parse_context_json(row.get("manager_name_overrides_json"), {}),
            "franchise_merges": _parse_context_json(row.get("franchise_merges_json"), []),
            "keeper_rules": _parse_context_json(row.get("keeper_rules_json"), None),
            "league_rules": _parse_context_json(row.get("league_rules_json"), None),
            "standings_weights": _parse_context_json(row.get("standings_weights_json"), None),
            "is_private": _parse_context_bool(row.get("is_private"), False),
        }
    return {}


def _hydrate_quick_identity_context(ctx, context_path: Path, *, reader=None) -> None:
    """Load only canonical identities before fetch/transform; retain import scope/auth."""
    from multi_league.core.db_reader import get_reader
    from multi_league.core.fetch_runtime import _resolve_db_name

    db_name = _resolve_db_name(ctx, ctx.league_name)
    settings = _load_frontend_context_settings(
        reader if reader is not None else get_reader(),
        str(db_name).replace("'", "''"),
        strict_identity=True,
    )
    if not settings:
        return  # New league: onboarding preferences remain authoritative.
    ctx.manager_name_overrides = settings["manager_name_overrides"]
    ctx.franchise_merges = settings["franchise_merges"]
    ctx.save(context_path)  # Subprocesses and final publication must see the same settings.


def _build_context_from_fly(
    database_name: str,
    data_dir_override: str | None = None,
    *,
    reader=None,
    frontend_settings: dict | None = None,
):
    """Build a LeagueContext by pulling credentials from Fly ops tables.

    Queries ___ops credential tables to determine the platform, then builds
    an appropriate LeagueContext for Yahoo leagues.  Non-Yahoo leagues get an
    error directing the user to the correct importer.

    Args:
        database_name: League database name.
        data_dir_override: If provided, use this directory instead of a temp dir.
            When set, the directory is NOT cleaned up after import.
        reader: Optional already-scoped Fly reader for a known Yahoo caller.
            When supplied, avoids probing the Sleeper and ESPN registries.
        frontend_settings: Optional already-loaded canonical league-context
            settings for the same database.

    Returns:
        (ctx, context_path) — the LeagueContext and the Path where it was saved
    """
    from multi_league.utils.credential_store import decrypt_token, get_encryption_key
    from multi_league.core.db_reader import get_reader

    supplied_reader = reader is not None
    reader = reader or get_reader()
    safe_db = str(database_name).replace("'", "''")

    # Check Yahoo
    yahoo_rows = reader.query(
        f"SELECT league_id, league_name, encrypted_refresh_token "
        f"FROM main.league_credentials WHERE database_name = '{safe_db}'",
        database="___ops",
    )
    yahoo = tuple(yahoo_rows[0].values()) if yahoo_rows else None

    # ---- Yahoo takes priority (this is the Yahoo importer) ----
    if not yahoo:
        # The active Yahoo worker has already selected the platform and passes
        # its shared reader explicitly.  Do not spend two network round trips
        # probing unrelated provider registries on that fast path.
        if supplied_reader:
            log(f"[FAIL] {database_name} not found in Yahoo credentials")
            sys.exit(1)

        # Check Sleeper
        sleeper_rows = reader.query(
            f"SELECT sleeper_league_id, league_name FROM main.sleeper_leagues WHERE database_name = '{safe_db}'",
            database="___ops",
        )
        sleeper = tuple(sleeper_rows[0].values()) if sleeper_rows else None

        # Check ESPN
        espn_rows = reader.query(
            f"SELECT espn_league_id, league_name, encrypted_espn_s2, encrypted_swid "
            f"FROM main.espn_leagues WHERE database_name = '{safe_db}'",
            database="___ops",
        )
        espn = tuple(espn_rows[0].values()) if espn_rows else None
        if sleeper:
            log(f"[ERROR] {database_name} is a Sleeper league. Use sleeper_initial_import.py instead.")
            sys.exit(1)
        if espn:
            log(f"[ERROR] {database_name} is an ESPN league. Use espn_initial_import.py instead.")
            sys.exit(1)
        log(f"[FAIL] {database_name} not found in any credential table (yahoo, sleeper, espn)")
        sys.exit(1)

    league_id, league_name, encrypted_refresh_token = yahoo

    # ---- Decrypt refresh token ----
    encryption_key = get_encryption_key()
    if not encryption_key:
        log("[FAIL] CREDENTIAL_ENCRYPTION_KEY environment variable is required for --league mode")
        sys.exit(1)

    refresh_token = decrypt_token(encrypted_refresh_token, encryption_key)

    # ---- Resolve consumer key / secret from environment ----
    consumer_key = os.environ.get("YAHOO_CLIENT_ID") or os.environ.get("YAHOO_CONSUMER_KEY")
    consumer_secret = os.environ.get("YAHOO_CLIENT_SECRET") or os.environ.get("YAHOO_CONSUMER_SECRET")
    if not consumer_key or not consumer_secret:
        log(
            "[FAIL] YAHOO_CLIENT_ID/YAHOO_CLIENT_SECRET (or YAHOO_CONSUMER_KEY/YAHOO_CONSUMER_SECRET) env vars required"
        )
        sys.exit(1)

    # ---- Data directory ----
    if data_dir_override:
        data_dir = Path(data_dir_override)
        data_dir.mkdir(parents=True, exist_ok=True)
        log(f"[--league] Using explicit data dir: {data_dir}")
    else:
        import tempfile as _tempfile

        data_dir = Path(_tempfile.mkdtemp(prefix=f"yahoo_v3_{database_name}_"))
        log(f"[--league] Using temp data dir: {data_dir}")

    # ---- Write OAuth JSON file for yahoo_oauth library ----
    oauth_file = data_dir / "oauth_credentials.json"
    oauth_data = {
        "access_token": "expired",
        "consumer_key": consumer_key,
        "consumer_secret": consumer_secret,
        "refresh_token": refresh_token,
        "token_time": 0.0,
        "token_type": "bearer",
    }
    with open(oauth_file, "w", encoding="utf-8") as f:
        json.dump(oauth_data, f, indent=2)

    log(f"[--league] Resolved {database_name} -> Yahoo league {league_id} ({league_name})")
    frontend_settings = frontend_settings if frontend_settings is not None else _load_frontend_context_settings(reader, safe_db)

    # A name already stored for this league is canonical: it was either captured
    # at first import or set deliberately in Settings (which also sets the slug).
    # Prefer it over the credential registry so a reimport or weekly update never
    # reverts a rename. (Joe, 2026-07-19.)
    stored_name = (frontend_settings.get("league_name") or "").strip()
    if stored_name and stored_name != league_name:
        log(f"[--league] Using stored league name '{stored_name}' (registry had '{league_name}')")
        league_name = stored_name

    # ---- Build LeagueContext ----
    ctx = LeagueContext(
        league_id=league_id,
        league_name=league_name,
        oauth_file_path=str(oauth_file),
        data_directory=data_dir,
        database_name=database_name,
        league_ids=frontend_settings.get("league_ids") or {},
        manager_name_overrides=frontend_settings.get("manager_name_overrides") or {},
        franchise_merges=frontend_settings.get("franchise_merges") or [],
        keeper_rules=frontend_settings.get("keeper_rules"),
        league_rules=frontend_settings.get("league_rules"),
        standings_weights=frontend_settings.get("standings_weights"),
        is_private=frontend_settings.get("is_private") is True,
    )

    # Save context so downstream code that references context_path works
    context_path = data_dir / "league_context.json"
    ctx.save(context_path)
    log(f"[--league] Context saved to {context_path}")

    return ctx, context_path


def _detect_locally_complete_yahoo_years(db, candidate_years: list[int]) -> list[int]:
    """Return years whose core Yahoo fetcher tables are already populated locally.

    We only skip a year when all core year-granularity Yahoo tables already have
    rows for that year in the local DuckDB. This lets a full import reuse a
    previously completed quick-import year without masking partial/incomplete data.
    """

    required_tables = ("matchup", "player_fantasy", "draft", "transactions", "schedule")
    year_sets: list[set[int]] = []

    for table_name in required_tables:
        try:
            rows = (
                db.connect().execute(f"SELECT DISTINCT year FROM public.{table_name} WHERE year IS NOT NULL").fetchall()
            )
            table_years = {int(row[0]) for row in rows if row[0] is not None}
        except Exception:
            table_years = set()
        year_sets.append(table_years)

    if not year_sets:
        return []

    complete_years = set(candidate_years)
    for table_years in year_sets:
        complete_years &= table_years

    return sorted(complete_years)


def _local_table_has_year_rows(db, table_name: str, year: int) -> bool:
    """Return whether a local public table has any rows for a given year."""
    try:
        row = (
            db.connect()
            .execute(
                f"SELECT 1 FROM public.{table_name} WHERE year = ? LIMIT 1",
                [int(year)],
            )
            .fetchone()
        )
        return row is not None
    except Exception:
        return False


def _local_schedule_year_has_no_played_rows(db, year: int) -> bool:
    """Return True when schedule rows exist but all known scores are zero/empty."""
    try:
        conn = db.connect()
        rows, played_rows = conn.execute(
            """
            SELECT
                COUNT(*) AS rows,
                SUM(
                    CASE
                        WHEN COALESCE(team_points, 0) != 0
                          OR COALESCE(opponent_points, 0) != 0
                        THEN 1 ELSE 0
                    END
                ) AS played_rows
            FROM public.schedule
            WHERE year = ?
            """,
            [int(year)],
        ).fetchone()
        return int(rows or 0) > 0 and int(played_rows or 0) == 0
    except Exception:
        return False


def _quick_import_years_from_ctx(ctx) -> list[int]:
    years = getattr(ctx, "quick_import_years", None)
    if years:
        return sorted({int(year) for year in years if coerce_int(year) is not None})
    start = coerce_int(getattr(ctx, "start_year", None))
    end = coerce_int(getattr(ctx, "end_year", None))
    if start is not None and end is not None and start <= end:
        return list(range(start, end + 1))
    return []


def _infer_yahoo_season_from_league_key(league_key: str | None) -> int | None:
    """Infer NFL season from Yahoo's numeric game key when OAuth discovery is unavailable."""
    if not league_key:
        return None
    match = re.match(r"^\s*(\d+)\.l\.\d+\s*$", str(league_key))
    if not match:
        return None
    return YAHOO_NFL_GAME_KEY_TO_SEASON.get(int(match.group(1)))


def _allow_empty_quick_startup(ctx, db, unplayed_years: set[int]) -> bool:
    """Allow settings-only quick imports for a brand-new unplayed season shell."""
    if getattr(ctx, "import_mode", "") != "quick":
        return False

    quick_years = set(_quick_import_years_from_ctx(ctx))
    if not quick_years or not quick_years.issubset(set(unplayed_years or set())):
        return False

    for table_name in ("matchup", "player_fantasy", "schedule"):
        try:
            if db.table_exists(table_name) and db.row_count(table_name) > 0:
                return False
        except Exception:
            continue

    return True


def _pre_upload_non_empty_tables(
    *,
    allow_empty_quick_startup: bool,
    allow_partial_history_source: bool,
) -> list[str]:
    """Choose the non-empty contract while retaining all schema checks.

    A non-target multi-platform source can still contribute useful historical
    settings, rosters, drafts, and transactions when Yahoo no longer exposes
    that season's matchup page. Final target imports never enable this mode.
    """
    if allow_empty_quick_startup:
        return ["league_settings"]
    if allow_partial_history_source:
        return ["league_settings", "player_fantasy"]
    return ["league_settings", "matchup", "player_fantasy", "draft", "transactions", "schedule"]


def _allow_empty_quick_fallback_target(ctx, db, year: int) -> bool:
    """Allow a quick target year to be empty when a prior fallback year is present."""
    if getattr(ctx, "import_mode", "") != "quick":
        return False

    quick_years = _quick_import_years_from_ctx(ctx)
    if len(quick_years) < 2:
        return False

    year = int(year)
    if year != max(quick_years):
        return False

    prior_years = [quick_year for quick_year in quick_years if quick_year < year]
    return any(_local_table_has_year_rows(db, "matchup", prior_year) for prior_year in prior_years)


def _apply_quick_import(ctx, year: int = None):
    """Configure context for quick (single-year) import.

    Sets start_year = end_year = year.  When no year is provided and the
    context doesn't have start_year/end_year yet, resolves the year from
    the Yahoo league settings (``metadata.season``) via a single API call.
    """
    configured_end_year = coerce_int(getattr(ctx, "end_year", None))
    configured_start_year = coerce_int(getattr(ctx, "start_year", None))
    current_nfl_year = get_current_nfl_season_year()
    inferred_yahoo_year = _infer_yahoo_season_from_league_key(getattr(ctx, "league_id", None))
    cap_year = max(
        year or 0,
        configured_end_year or 0,
        configured_start_year or 0,
        inferred_yahoo_year or 0,
        current_nfl_year,
    )
    mapped_target_year = resolve_most_recent_available_year(
        getattr(ctx, "league_ids", None),
        cap_year=cap_year,
    )
    target_year = year or mapped_target_year or configured_end_year or configured_start_year or inferred_yahoo_year
    if year is None and mapped_target_year is not None and mapped_target_year != (ctx.end_year or ctx.start_year):
        log(f"[QUICK] Using most recent mapped league year: {mapped_target_year}")
    if (
        year is None
        and mapped_target_year is None
        and inferred_yahoo_year is not None
        and target_year == inferred_yahoo_year
    ):
        log(f"[QUICK] Inferred Yahoo season {inferred_yahoo_year} from league key {ctx.league_id}")
    if target_year is None:
        # Resolve year from Yahoo API — one settings call, not full discovery
        try:
            settings = fetch_league_settings(
                year=inferred_yahoo_year or current_nfl_year,
                league_key=ctx.league_id,
                oauth_file=Path(ctx.oauth_file_path) if ctx.oauth_file_path else None,
            )
            if settings:
                target_year = settings.get("metadata", {}).get("season")
                if target_year:
                    target_year = int(target_year)
                    log(f"[QUICK] Resolved year {target_year} from league settings")
        except Exception as e:
            log(f"[QUICK] Could not resolve year from settings: {e}")
        if target_year is None:
            target_year = current_nfl_year
            log(f"[QUICK] Falling back to current NFL season: {target_year}")

    target_year = int(target_year)
    try:
        nfl_state = get_nfl_state() or {}
    except Exception as e:
        log(f"[QUICK] Could not fetch NFL state for year selection: {e}")
        nfl_state = {}

    history_cap_year = max(target_year, configured_end_year or 0, configured_start_year or 0)
    history_years = resolve_history_years(
        getattr(ctx, "league_ids", None),
        start_year=configured_start_year,
        end_year=configured_end_year,
        cap_year=history_cap_year,
        extra_years=[target_year],
    )
    quick_years = resolve_quick_import_years(
        history_years,
        target_year,
        nfl_state,
        include_previous_available=True,
    )
    if len(quick_years) > 1:
        reason = "current season has no scores yet"
        state_season = coerce_int(nfl_state.get("league_season") or nfl_state.get("season"))
        if not (state_season == target_year and nfl_state.get("season_has_scores") is False):
            reason = "including prior mapped season for empty-shell fallback"
        log(
            f"[QUICK] Updating target years: {format_year_filter(target_year)} -> "
            f"{format_year_filter(quick_years)} ({reason})"
        )

    ctx.start_year = min(quick_years)
    ctx.end_year = max(quick_years)
    ctx.import_mode = "quick"
    ctx.quick_import_years = quick_years
    # Pre-set the target year so discovery doesn't try a full history walk.
    # Preserve existing per-year mappings instead of overwriting them.
    if not ctx.league_ids:
        ctx.league_ids = {}
    target_league_id = ctx.league_ids.get(str(max(quick_years)))
    if target_league_id:
        if ctx.league_id != target_league_id:
            log(f"[QUICK] Switching league_id to mapped {max(quick_years)} league: {target_league_id}")
        ctx.league_id = target_league_id
    else:
        ctx.league_ids[str(max(quick_years))] = ctx.league_id
    return ctx


# --------------------------------------------------------------------------------------
# OAuth session management (v3: in-process fetchers share sessions)
# --------------------------------------------------------------------------------------


def create_oauth_session(ctx):
    """Create a fresh OAuth session + Yahoo Game Manager.

    Call this before each fetcher type to ensure fresh tokens.
    Yahoo sessions last ~90 minutes; a full 19-year import takes ~30-40 min
    per fetcher type, so one session per fetcher is safe.

    Returns:
        (oauth, gm) tuple
    """
    import yahoo_fantasy_api as yfa

    oauth = ctx.get_oauth_session()
    gm = yfa.Game(oauth, "nfl")
    log("[OAUTH] Fresh session created")
    return oauth, gm


def ensure_session_healthy(oauth, ctx):
    """Check if OAuth session is still valid, refresh or recreate if needed.

    Call between years within a fetcher to catch expiry early.
    Yahoo tokens expire after 3600s; this proactively refreshes.

    Returns:
        oauth session (same or new)
    """
    try:
        if hasattr(oauth, "token_is_valid") and not oauth.token_is_valid():
            log("[OAUTH] Token expired, refreshing...")
            oauth.refresh_access_token()
            log("[OAUTH] Token refreshed")
        return oauth
    except Exception as e:
        # Token refresh failed — create entirely new session
        log(f"[OAUTH] Refresh failed ({e}), creating fresh session")
        return ctx.get_oauth_session()


# --------------------------------------------------------------------------------------
# Child script configuration
# --------------------------------------------------------------------------------------

# NFL_SUPER_TABLE_LOADER removed — dead for v3 (no downstream consumers)


def run_track_1_initial(start_year: int, end_year: int, dry_run: bool = False) -> bool:
    """
    Track 1: Verify NFL super table has data for the required years.

    NOTE: This function NO LONGER fetches from NFLverse. The super table is updated
    by a separate scheduled workflow (update_nfl_super_table.yml) that runs daily.
    This import just reads from the already-updated super table.

    Args:
        start_year: First year to process
        end_year: Last year to process (typically current year)
        dry_run: If True, just log what would be done

    Returns:
        True if super table has data, False if critical data is missing
    """
    log("\n" + "=" * 96)
    log("[TRACK 1] VERIFYING NFL SUPER TABLE")
    log("=" * 96)
    return _shared_track_1_verify(start_year, end_year, dry_run=dry_run)


# Transformation pass definitions imported from multi_league.core.import_config


# --------------------------------------------------------------------------------------
# Phase functions (extracted from main for auditability)
# --------------------------------------------------------------------------------------


def drop_league_tables(ctx, db_name: str, dry_run: bool = False) -> int:
    """Phase 0: Drop all league tables for quick import fresh start.

    Delegates to shared implementation in import_utils.
    Kept as module-level function for backward compat (ESPN imports this).
    """
    return _shared_drop_league_tables(ctx, db_name, dry_run=dry_run, platform="yahoo")


def upload_yahoo_tables(ctx, db: LocalLeagueDB = None, dry_run: bool = False) -> list[tuple[str, bool]]:
    """Phase 4: Upload all tables to Fly after transformations.

    Delegates to shared implementation in import_utils.
    """
    if db is None:
        log("[TRACK 2] No LocalLeagueDB — cannot upload")
        return [("League Table Upload", False)]
    db_name = runtime_from_source(ctx).db_name
    return _shared_upload_league_tables(db, db_name, ctx, platform="yahoo", dry_run=dry_run)


# --------------------------------------------------------------------------------------
# Fetcher wrapper (DRY replacement for 5 ad-hoc loops)
# --------------------------------------------------------------------------------------


def run_v3_fetcher(
    *,
    ctx,
    db: LocalLeagueDB,
    fetcher_name: str,
    years: list[int],
    fetch_year_fn,
    table_name: str,
    normalize_fn=None,
    sleep_between_years: int = 2,
) -> dict:
    """Run an in-process fetcher across multiple years with manifest tracking.

    Args:
        ctx: LeagueContext
        db: LocalLeagueDB to save data into
        fetcher_name: Manifest key (e.g. "matchups", "rosters", "draft")
        years: List of years to fetch
        fetch_year_fn: Callable(year, oauth) -> DataFrame or (DataFrame, failed_weeks)
        table_name: LocalLeagueDB table to save into
        normalize_fn: Optional Callable(df, year) -> df for post-fetch normalization
        sleep_between_years: Seconds to sleep between years (default 2)

    Returns:
        dict of {year: error_string} for years that had failures
    """
    failures: dict[int, str] = {}
    if not years:
        return failures

    oauth, _gm = create_oauth_session(ctx)

    for i, year in enumerate(years):
        try:
            oauth = ensure_session_healthy(oauth, ctx)

            result = fetch_year_fn(year=year, oauth=oauth)

            # Fetchers return either df or (df, failed_weeks)
            failed_weeks = None
            if isinstance(result, tuple):
                df, failed_weeks = result
            else:
                df = result

            if normalize_fn is not None and df is not None and not df.empty:
                df = normalize_fn(df, year)

            league_id_for_year = (
                ctx.get_league_id_for_year(year) if hasattr(ctx, "get_league_id_for_year") else ctx.league_id
            )
            schedule_df = getattr(df, "attrs", {}).get("schedule_df") if df is not None else None

            if df is not None and not df.empty:
                db.save_table(
                    table_name,
                    df,
                    year=year,
                    platform="yahoo",
                    league_id=league_id_for_year,
                )
                log(f"  [OK] {fetcher_name} {year}: {len(df):,} rows -> local DB")
            else:
                log(f"  [WARN] {fetcher_name} {year}: empty result")

            # If the matchup response includes schedule data, write it even
            # when the played-matchup frame is empty. That gives the import a
            # year-level "known empty shell" signal instead of forcing roster
            # recovery to guess week by week.
            if schedule_df is not None and not schedule_df.empty:
                from multi_league.data_fetchers.yahoo.yahoo_schedules import (
                    _derive_schedule_df_from_matchup_df,
                )

                sched = _derive_schedule_df_from_matchup_df(
                    schedule_df,
                    year,
                    getattr(ctx, "manager_name_overrides", None) or {},
                )
                if not sched.empty:
                    db.save_table(
                        "schedule",
                        sched,
                        year=year,
                        platform="yahoo",
                        league_id=league_id_for_year,
                    )
                    log(f"  [OK] schedule {year}: {len(sched):,} rows -> local DB (from matchup)")

            if failed_weeks:
                write_manifest(ctx.data_directory, fetcher_name, year, failed_weeks=failed_weeks)
                failures[year] = f"failed weeks: {failed_weeks}"
                log(f"  [WARN] {fetcher_name} {year}: failed weeks {failed_weeks}")
            else:
                clear_manifest(ctx.data_directory, fetcher_name, year)

        except Exception as e:
            write_manifest(ctx.data_directory, fetcher_name, year, error=str(e), failed_year=True)
            failures[year] = str(e)
            log(f"  [ERROR] {fetcher_name} {year}: {e}")

        if i < len(years) - 1:
            time.sleep(sleep_between_years)

    return failures


# --------------------------------------------------------------------------------------
# Targeted fetch (reusable from CLI --fetch and internal recovery callers)
# --------------------------------------------------------------------------------------

# Table mapping: fetch_name -> LocalLeagueDB table
_TARGETED_TABLE_MAP = {
    "matchups": "matchup",
    "rosters": "player_fantasy",
    "draft": "draft",
    "transactions": "transactions",
    "schedules": "schedule",
}


def run_targeted_fetch(
    *,
    ctx,
    db: LocalLeagueDB,
    fetch_name: str,
    fetch_year: int,
    fetch_week: int | None = None,
    upload_to_fly: bool = True,
    with_transforms: bool = False,
    dry_run: bool = False,
) -> dict:
    """Run a single fetcher for one year, save to local DB, optionally upload + transform.

    Args:
        ctx: LeagueContext
        db: LocalLeagueDB instance (must be connected)
        fetch_name: One of "matchups", "rosters", "draft", "transactions", "schedules"
        fetch_year: Year to fetch
        fetch_week: Optional week (matchups/rosters only)
        upload_to_fly: Whether to upload to Fly after saving
        with_transforms: Whether to run post-upload pipeline after upload
        dry_run: If True, skip actual upload/transforms

    Returns:
        dict with keys: ok (bool), rows (int), failed_weeks (list|None), error (str|None)
    """
    from multi_league.data_fetchers.yahoo.yahoo_matchups import weekly_matchup_data
    from multi_league.data_fetchers.yahoo.yahoo_rosters import fetch_rosters_for_year
    from multi_league.data_fetchers.yahoo.yahoo_draft import fetch_draft_data
    from multi_league.data_fetchers.yahoo.yahoo_transactions import fetch_transactions
    from multi_league.data_fetchers.yahoo.yahoo_schedules import fetch_schedule_for_year

    table_name = _TARGETED_TABLE_MAP.get(fetch_name)
    if not table_name:
        return {"ok": False, "rows": 0, "failed_weeks": None, "error": f"Unknown fetcher: {fetch_name}"}

    try:
        oauth, _gm = create_oauth_session(ctx)

        log(f"[TARGETED FETCH] {fetch_name} year={fetch_year} week={fetch_week or 'ALL'}")

        new_df = None
        failed_weeks = None

        if fetch_name == "matchups":
            new_df, failed_weeks = weekly_matchup_data(ctx=ctx, year=fetch_year, week=fetch_week)
        elif fetch_name == "rosters":
            new_df, failed_weeks = fetch_rosters_for_year(
                ctx=ctx,
                year=fetch_year,
                oauth_session=oauth,
                local_db=db,
            )
        elif fetch_name == "draft":
            new_df = fetch_draft_data(ctx=ctx, year=fetch_year)
        elif fetch_name == "transactions":
            new_df = fetch_transactions(ctx=ctx, year=fetch_year, local_db=db)
        elif fetch_name == "schedules":
            new_df = fetch_schedule_for_year(
                ctx=ctx,
                year=fetch_year,
                manager_overrides=ctx.manager_name_overrides,
                local_db=db,
            )

        if new_df is None or new_df.empty:
            log(f"[TARGETED FETCH] No data returned for {fetch_name} year={fetch_year}")
            return {"ok": True, "rows": 0, "failed_weeks": failed_weeks, "error": None}

        # Save to local DB
        db.save_table(
            table_name,
            new_df,
            year=fetch_year,
            platform="yahoo",
            league_id=ctx.league_id,
        )
        log(f"[TARGETED FETCH] Saved {len(new_df)} rows to local DB: {table_name}")

        # Write/clear manifests
        if failed_weeks:
            write_manifest(ctx.data_directory, fetch_name, fetch_year, failed_weeks=failed_weeks)
            log(f"[TARGETED FETCH] {len(failed_weeks)} failed weeks recorded in manifest")
        else:
            clear_manifest(ctx.data_directory, fetch_name, fetch_year)

        # Upload to Fly
        if upload_to_fly and not dry_run:
            db_name = runtime_from_source(ctx).db_name
            log(f"[TARGETED FETCH] Uploading to Fly ({db_name})...")
            db.upload_to_fly(db_name)
            log("[TARGETED FETCH] Upload complete")

        # Optionally run post-upload transformations
        if with_transforms and not dry_run:
            log("[TARGETED FETCH] Running post-upload transformation pipeline...")
            run_post_upload_pipeline(ctx=ctx, dry_run=dry_run)
            log("[TARGETED FETCH] Transformations complete")

        return {"ok": True, "rows": len(new_df), "failed_weeks": failed_weeks, "error": None}

    except Exception as e:
        write_manifest(ctx.data_directory, fetch_name, fetch_year, error=str(e), failed_year=True)
        log(f"[TARGETED FETCH][ERROR] {fetch_name} year={fetch_year}: {e}")
        return {"ok": False, "rows": 0, "failed_weeks": None, "error": str(e)}


# --------------------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------------------


def verify_unified_outputs(ctx, db: LocalLeagueDB = None):
    """Verify unified outputs exist and have data.

    Uses the local DuckDB as the single source of truth.
    """
    summary = {}

    if db is not None:
        for table_name in ["player_fantasy", "matchup", "draft", "transactions"]:
            if db.table_exists(table_name):
                conn = db.connect()
                res = conn.execute(f"SELECT count(*), min(year), max(year) FROM public.{table_name}").fetchone()
                summary[table_name] = {"rows": res[0], "min_year": res[1], "max_year": res[2]}
            else:
                summary[table_name] = "missing"
    else:
        log("[SUMMARY] No LocalLeagueDB available — cannot verify outputs")
        return

    log(f"[SUMMARY] Local DB:\n{json.dumps(summary, indent=2)}")


def main():
    parser = argparse.ArgumentParser(description="Complete historical data import for a fantasy league (Unified)")
    # --context and --league are mutually exclusive; one is required unless --utility mode
    source_group = parser.add_mutually_exclusive_group(required=False)
    source_group.add_argument("--context", type=str, help="Path to league_context.json")
    source_group.add_argument(
        "--league", type=str, help="Database name to import (pulls credentials from Fly ops tables)"
    )
    parser.add_argument(
        "--auth-mode",
        choices=["auto", "oauth", "cookie"],
        default="auto",
        help="Yahoo credential track for --league: auto (OAuth default), oauth, or cookie",
    )
    parser.add_argument("--cookie-league-keys-json", help="JSON file/object mapping Yahoo cookie-track years to league keys")
    parser.add_argument("--cookie-start-year", type=int, default=2015)
    parser.add_argument("--cookie-end-year", type=int, default=2025)
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Quick import: target season refresh; may include prior scored season for empty renewed shells",
    )
    parser.add_argument("--dry-run", action="store_true", help="Run all scripts in dry-run mode when supported")
    parser.add_argument("--skip-fetchers", action="store_true", help="Skip data fetchers (use existing files)")
    parser.add_argument("--skip-transformations", action="store_true", help="Skip transformations")
    parser.add_argument(
        "--start-phase",
        type=int,
        choices=[1, 2, 3],
        help="Start at specific phase (1=fetchers, 2=merges, 3=transformations)",
    )
    parser.add_argument(
        "--utility",
        action="store_true",
        help="Run utility functions (matchups, draft, transactions, schedule, fantasy points)",
    )
    parser.add_argument(
        "--util-action",
        choices=[
            "merge_matchups",
            "normalize_draft",
            "normalize_transactions",
            "normalize_schedule",
            "points_alias",
            "all",
        ],
        help="Which utility to run when --utility is set",
    )
    parser.add_argument(
        "--years", nargs="*", type=int, help="Optional years for merge_matchups (e.g. --years 2021 2022)"
    )
    parser.add_argument(
        "--data-dir", type=str, help="Data directory for local DuckDB (default: temp dir, cleaned up after import)"
    )
    parser.add_argument("--skip-track-1", action="store_true", help="Skip NFL super table update (Track 1)")
    parser.add_argument("--skip-track-2-upload", action="store_true", help="Skip league table upload to Fly (Track 2)")
    parser.add_argument(
        "--allow-partial-history-source",
        action="store_true",
        help="Allow a non-target multi-platform source to retain valid partial historical data",
    )

    # v3: Targeted fetch flags
    parser.add_argument(
        "--fetch",
        type=str,
        choices=["matchups", "rosters", "draft", "transactions", "schedules"],
        help="Targeted fetch: run only this fetcher (requires --year)",
    )
    parser.add_argument("--year", type=int, default=None, help="Targeted fetch: specific year (used with --fetch)")
    parser.add_argument(
        "--week", type=int, default=None, help="Targeted fetch: specific week (only with --fetch matchups/rosters)"
    )
    parser.add_argument("--with-transforms", action="store_true", help="Run transformations after targeted fetch")

    args = parser.parse_args()

    effective_auth_mode = args.auth_mode
    if effective_auth_mode == "auto":
        effective_auth_mode = os.environ.get("YAHOO_AUTH_MODE", "oauth").strip().lower()
    if effective_auth_mode not in {"oauth", "cookie"}:
        parser.error("YAHOO_AUTH_MODE must be oauth or cookie")
    if effective_auth_mode == "cookie":
        if not args.league:
            parser.error("--auth-mode cookie requires --league so encrypted Fly credentials can be resolved")
        from yahoo_cookie_worker import run as run_cookie_worker

        cookie_start_year = args.cookie_end_year if args.quick else args.cookie_start_year
        worker_args = argparse.Namespace(
            db_name=args.league,
            league_name=args.league,
            ops_cache=os.environ.get("OPS_CACHE_PATH"),
            draft_global_source=os.environ.get("DRAFT_GLOBAL_SOURCE_PATH"),
            output_dir=args.data_dir or os.path.join(os.environ.get("TEMP", "."), f"yahoo_cookie_{args.league}"),
            context_json=None,
            league_keys_json=args.cookie_league_keys_json,
            import_mode="quick" if args.quick else "full",
            start_year=cookie_start_year,
            end_year=args.cookie_end_year,
            team_count=10,
            roster_weeks=None,
            transaction_pages=200,
            request_delay=0.5,
            throttle_retries=5,
            n_sims=10000,
            skip_playoff=False,
            skip_aggregations=False,
            skip_upload=False,
        )
        if not worker_args.ops_cache or not worker_args.draft_global_source:
            parser.error("cookie mode requires OPS_CACHE_PATH and DRAFT_GLOBAL_SOURCE_PATH")
        return run_cookie_worker(worker_args)

    # Suppress noisy third-party loggers before any OAuth/API calls
    from multi_league.core.logging_config import suppress_noisy_loggers

    suppress_noisy_loggers()

    # Validate that at least one source is provided (unless --utility with --context)
    if not args.context and not args.league:
        parser.error("one of --context or --league is required")

    # =========================================================================
    # RESOLVE CONTEXT: either from --context file or --league (Fly ops tables)
    # =========================================================================
    if args.league:
        ctx, context_path = _build_context_from_fly(args.league, data_dir_override=getattr(args, "data_dir", None))
    else:
        context_path = Path(args.context).resolve()
        if not context_path.exists():
            log(f"[FAIL] League context not found: {context_path}")
            sys.exit(1)
        ctx = _load_ctx(str(context_path))

    if (args.quick or getattr(ctx, "import_mode", None) == "quick") and not args.dry_run:
        _hydrate_quick_identity_context(ctx, context_path)

    # Apply --quick flag: single year import. Resolves year from league settings
    # if start_year/end_year aren't set (one API call, no full discovery needed).
    if args.quick:
        year_override = args.year if hasattr(args, "year") and args.year else None
        ctx = _apply_quick_import(ctx, year=year_override)
        quick_years = _quick_import_years_from_ctx(ctx)
        log(f"[QUICK] Target import years: {format_year_filter(quick_years or ctx.start_year)}")

    # CRITICAL: Discover league history to ensure correct league_id for each year
    existing_years = set(int(y) for y in ctx.league_ids.keys()) if ctx.league_ids else set()
    if ctx.start_year is not None:
        expected_years = set(range(ctx.start_year, (ctx.end_year or get_current_nfl_season_year()) + 1))
        missing_years = expected_years - existing_years
    else:
        # start_year not yet known — discovery will set it
        expected_years = set()
        missing_years = set()
    # Trust a substantial pre-built league_ids mapping (e.g., from registration UI).
    # Missing years are likely intentional gaps (league didn't exist that year),
    # not data we need to discover.  Only re-discover if we have NO mapping or
    # very few years (< 3) relative to the expected range.
    has_substantial_mapping = len(existing_years) >= min(3, len(expected_years)) if expected_years else False
    needs_discovery = not ctx.has_league_ids_mapping() or (
        missing_years and not ctx.is_single_year_import and not has_substantial_mapping
    )
    if missing_years and has_substantial_mapping and not ctx.is_single_year_import:
        log(
            f"[LEAGUE HISTORY] league_ids has {len(existing_years)} years, {len(missing_years)} gap(s): {sorted(missing_years)} — trusting provided mapping"
        )
    elif needs_discovery and missing_years:
        log(f"[LEAGUE HISTORY] league_ids missing for {len(missing_years)} year(s): {sorted(missing_years)}")
    if needs_discovery:
        # For single-year (quick) imports, skip API discovery - just use the provided league_id
        # This saves unnecessary API calls since we only need the current year's league
        if ctx.is_single_year_import:
            requested_year = str(ctx.start_year)
            ctx.league_ids = {requested_year: ctx.league_id}
            ctx.save(context_path)
            log(
                f"[LEAGUE HISTORY] Quick import: using provided league_id for {requested_year} (skipped history discovery)"
            )
        else:
            # Full import: discover all years to get correct league_ids for each season
            log("[LEAGUE HISTORY] No league_ids mapping found - discovering league history...")
            try:
                from multi_league.core.yahoo_league_settings import discover_league_history

                league_ids = discover_league_history(
                    league_key=ctx.league_id,
                    oauth_file=Path(ctx.oauth_file_path) if ctx.oauth_file_path else None,
                    start_year=ctx.start_year,
                    end_year=ctx.end_year,
                )
                if league_ids:
                    ctx.league_ids = league_ids
                    # Update start_year/end_year to match discovered years
                    # This prevents trying to fetch data for years the user can't access
                    discovered_years = sorted([int(y) for y in league_ids.keys()])
                    if discovered_years:
                        old_start, old_end = ctx.start_year, ctx.end_year
                        ctx.start_year = min(discovered_years)
                        ctx.end_year = max(discovered_years)

                        # IMPORTANT: Preserve original end_year if it's current/recent season
                        # Yahoo league renewal happens mid-season, so 2025 won't be discoverable
                        # until the user explicitly renews. But if user specified end_year=2025,
                        # we should honor that and use the current league_id for the current year.
                        current_year = get_current_nfl_season_year()
                        if old_end and old_end >= current_year - 1 and old_end > ctx.end_year:
                            log(
                                f"[LEAGUE HISTORY] Preserving end_year={old_end} (current season not yet renewed in Yahoo)"
                            )
                            ctx.end_year = old_end
                            # Add current year to league_ids using the base league_id
                            if str(old_end) not in ctx.league_ids:
                                ctx.league_ids[str(old_end)] = ctx.league_id
                                log(f"[LEAGUE HISTORY] Added {old_end}: {ctx.league_id} to league_ids")

                        if ctx.start_year != old_start or ctx.end_year != old_end:
                            log(
                                f"[LEAGUE HISTORY] Adjusted year range from {old_start}-{old_end} to {ctx.start_year}-{ctx.end_year} (based on accessible years)"
                            )

                    ctx.save(context_path)  # Save back to ORIGINAL context file (not data_directory)
                    log(f"[LEAGUE HISTORY] Discovered {len(league_ids)} league IDs and saved to context")
                else:
                    log(
                        "[LEAGUE HISTORY] WARNING: Could not discover league history. Data may be mixed if user is in multiple leagues."
                    )
            except Exception as e:
                log(f"[LEAGUE HISTORY] WARNING: Failed to discover league history: {e}")
                log("[LEAGUE HISTORY] WARNING: Data may be mixed if user is in multiple leagues.")
    else:
        log(f"[LEAGUE HISTORY] Using {len(ctx.league_ids)} pre-configured league IDs from context")

    # If utilities requested, run them and exit
    if args.utility:
        action = args.util_action or "all"
        log(f"[UTILITY] Running utility action: {action}")
        # points_alias still needs the legacy file fixer; canonical table rewrites run via DuckDB.
        from multi_league.data_fetchers.shared.aggregators import ensure_fantasy_points_alias

        runtime = runtime_from_source(ctx, context_path=context_path)

        try:
            with LocalLeagueDB(runtime.data_dir, runtime.db_name) as utility_db:
                if action in ("merge_matchups", "all"):
                    row_count = utility_db.recanonicalize_table("matchup", platform="yahoo", league_id=ctx.league_id)
                    log(
                        f"[UTILITY] merge_matchups recanonicalized public.matchup in {utility_db.db_path} ({row_count:,} rows)"
                    )
                if action in ("normalize_draft", "all"):
                    row_count = utility_db.recanonicalize_table("draft", platform="yahoo", league_id=ctx.league_id)
                    log(
                        f"[UTILITY] normalize_draft recanonicalized public.draft in {utility_db.db_path} ({row_count:,} rows)"
                    )
                if action in ("normalize_transactions", "all"):
                    row_count = utility_db.recanonicalize_table(
                        "transactions",
                        platform="yahoo",
                        league_id=ctx.league_id,
                    )
                    log(
                        f"[UTILITY] normalize_transactions recanonicalized public.transactions in {utility_db.db_path} ({row_count:,} rows)"
                    )
                if action in ("normalize_schedule", "all"):
                    row_count = utility_db.recanonicalize_table("schedule", platform="yahoo", league_id=ctx.league_id)
                    log(
                        f"[UTILITY] normalize_schedule recanonicalized public.schedule in {utility_db.db_path} ({row_count:,} rows)"
                    )
            if action in ("points_alias", "all"):
                result = ensure_fantasy_points_alias(db=utility_db, log=log)
                log(f"[UTILITY] points_alias updated: {result}")
        except Exception as e:
            log(f"[UTILITY][ERROR] {e}")
            sys.exit(2)
        log("[UTILITY] Completed.")
        return

    # =========================================================================
    # INITIALIZE LOCAL LEAGUE DB
    # =========================================================================
    runtime = runtime_from_source(ctx, context_path=context_path)
    data_dir = runtime.data_dir
    db_name = runtime.db_name
    db = LocalLeagueDB(data_dir, db_name)
    db.connect()
    for table in ["matchup", "player_fantasy", "draft", "transactions", "schedule"]:
        db.ensure_table(table)
    log(f"[LOCAL DB] Initialized {db.db_path}")

    # =========================================================================
    # TARGETED FETCH MODE (--fetch --year [--week] [--with-transforms])
    # Runs a single fetcher, saves to local DB, uploads, then exits.
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
        log(f"Week:    {args.week or 'ALL'}")
        log(f"Transforms: {'Yes' if args.with_transforms else 'No'}")
        log("=" * 96)

        result = run_targeted_fetch(
            ctx=ctx,
            db=db,
            fetch_name=args.fetch,
            fetch_year=args.year,
            fetch_week=args.week,
            upload_to_fly=not args.skip_track_2_upload,
            with_transforms=args.with_transforms,
            dry_run=args.dry_run,
        )

        db.close()
        if result["ok"]:
            log(f"[TARGETED FETCH] SUCCESS — {args.fetch} for year={args.year}: {result['rows']} rows")
            sys.exit(0)
        else:
            log(f"[TARGETED FETCH] FAILED — {result['error']}")
            sys.exit(2)

    # Determine starting phase (handles both new --start-phase and legacy --skip-fetchers)
    if args.start_phase:
        start_phase = args.start_phase
    elif args.skip_fetchers:
        start_phase = 2  # Legacy: --skip-fetchers meant start at merges
    else:
        start_phase = 1  # Default: start at beginning

    log("=" * 96)
    log("INITIAL IMPORT V3 - In-Process Pipeline")
    log("=" * 96)
    log(f"League: {ctx.league_name} | League ID: {ctx.league_id}")
    log(f"Years:  {ctx.start_year} - {ctx.end_year or 'current'}")
    log(
        f"Mode:   {getattr(ctx, 'import_mode', 'full').upper()} "
        f"{'(quick refresh)' if getattr(ctx, 'import_mode', '') == 'quick' else '(all years)'}"
    )
    log(f"Root:   {ctx.data_directory}")
    log(f"DryRun: {'Yes' if args.dry_run else 'No'}")
    log(f"Track 1 (NFL):  {'SKIP' if args.skip_track_1 else 'ENABLED'}")
    log(f"Track 2 Upload: {'SKIP' if args.skip_track_2_upload else 'ENABLED'}")
    log(
        f"Start Phase: {start_phase} ({'Settings/Fetchers' if start_phase == 1 else 'Merges' if start_phase == 2 else 'Transformations'})"
    )
    log("=" * 96)

    results: dict[str, list[tuple[str, bool]]] = {
        "track1": [],
        "settings": [],
        "fetchers": [],
        "merges": [],
        "track2": [],
        "transformations": [],
    }
    extra_args = ["--dry-run"] if args.dry_run else []

    # =========================================================================
    # PHASE 0: No early FRESH START delete against ___leagues. The final
    # upload_to_fly() handles DELETE WHERE db_name + INSERT
    # atomically per table, so the league's data stays available in
    # ___leagues throughout the import.

    # =========================================================================
    # TRACK 1: Update NFL Super Table (Run ONCE globally - before fetchers)
    # =========================================================================
    end_year = ctx.end_year or get_current_nfl_season_year()
    if not args.skip_fetchers and not args.skip_track_1 and start_phase <= 1:
        # Track 1 verifies the shared NFL super table, preferring OPS_CACHE_PATH
        # Only process NFLverse years (1999+) - historical data already in super table
        if getattr(ctx, "import_mode", "") == "quick":
            quick_years = _quick_import_years_from_ctx(ctx) or [end_year]
            try:
                nfl_state = get_nfl_state() or {}
            except Exception:
                nfl_state = {}
            track_1_years = [
                year for year in quick_years if year not in unscored_current_shell_years(quick_years, nfl_state)
            ]
            if not track_1_years:
                track_1_years = [max(quick_years)]
            nfl_start_year = max(1999, min(track_1_years))
            nfl_end_year = max(track_1_years)
        else:
            nfl_start_year = 1999
            nfl_end_year = end_year
        track1_ok = run_track_1_initial(nfl_start_year, nfl_end_year, dry_run=args.dry_run)
        results["track1"].append(("NFL Super Table", track1_ok))

        if not track1_ok and not args.dry_run:
            log("[WARN] Track 1 failed, continuing with existing NFL data")
    elif args.skip_track_1:
        log("\n[SKIP] Track 1 - NFL super table update skipped by user")
        results["track1"].append(("NFL Super Table", True))

    # -------------------------------------------------------------------------
    # PHASE 0: LEAGUE SETTINGS (fetch all league configuration first)
    # -------------------------------------------------------------------------
    if start_phase <= 1:
        log("\n" + "=" * 96)
        log("PHASE 0: League Settings Discovery")
        log("=" * 96)
        log("[INFO] Fetching league settings for all years from Yahoo API")
        log("[INFO] All other scripts will READ these settings (no additional API calls)")

        if ctx.start_year is None:
            log("[FAIL] start_year is None — league history discovery failed (Yahoo API may be rate limited)")
            log("[FAIL] Try again later or provide a context file with start_year set")
            sys.exit(1)
        years_to_fetch = list(range(ctx.start_year, (ctx.end_year or get_current_nfl_season_year()) + 1))

        # Determine settings directory (league-wide, not player-specific)
        settings_dir = Path(ctx.data_directory) / "league_settings"
        settings_dir.mkdir(parents=True, exist_ok=True)

        # -------------------------------------------------------------------------
        # PHASE 0.1: LOCAL SETTINGS DISCOVERY - Check for local settings files
        # -------------------------------------------------------------------------
        # Look for league_settings_*.json files in multiple locations:
        # 1. Import directory (matchup_data_directory) - primary location for uploaded files
        # 2. Data directory - for manually placed files
        # This allows users to add historical settings files without the web upload
        local_settings_years = set()

        # Collect all candidate directories to search
        search_dirs = []
        if hasattr(ctx, "data_directory") and ctx.data_directory:
            search_dirs.append(Path(ctx.data_directory))

        # Deduplicate and filter to existing directories
        search_dirs = list({d.resolve() for d in search_dirs if d.exists()})
        # Exclude the settings_dir itself to avoid finding files we're about to create
        search_dirs = [d for d in search_dirs if d.resolve() != settings_dir.resolve()]

        # Find all local settings files
        local_settings_files = []
        for search_dir in search_dirs:
            found_files = list(search_dir.glob("league_settings_*.json"))
            if found_files:
                log(f"[LOCAL SETTINGS] Searching {search_dir}: found {len(found_files)} settings file(s)")
                local_settings_files.extend(found_files)

        if local_settings_files:
            log(f"[LOCAL SETTINGS] Processing {len(local_settings_files)} local settings file(s)...")
            local_league_ids = {}  # Track league_keys from local settings
            for local_file in local_settings_files:
                try:
                    with open(local_file, encoding="utf-8") as f:
                        local_data = json.load(f)

                    # Extract year from file or data
                    local_year = local_data.get("year")
                    if not local_year:
                        # Try to extract from filename (league_settings_YYYY_*.json)
                        # Note: use global re import at top of file (Python 3.12+ scoping issue)
                        match = re.search(r"league_settings_(\d{4})_", local_file.name)
                        if match:
                            local_year = int(match.group(1))

                    if local_year:
                        local_year = int(local_year)
                        # Copy to settings directory if not already there
                        dest_file = settings_dir / local_file.name
                        if not dest_file.exists():
                            import shutil

                            shutil.copy2(local_file, dest_file)
                            log(f"[LOCAL SETTINGS] Copied {local_file.name} -> {dest_file}")
                        else:
                            log(f"[LOCAL SETTINGS] {local_file.name} already exists in settings directory")

                        local_settings_years.add(local_year)

                        # Log key info from the settings
                        metadata = local_data.get("metadata", local_data)
                        playoff_start = metadata.get("playoff_start_week", "?")
                        playoff_teams = metadata.get("num_playoff_teams", metadata.get("playoff_teams", "?"))
                        log(
                            f"[LOCAL SETTINGS] {local_year}: playoff_start_week={playoff_start}, playoff_teams={playoff_teams}"
                        )

                        # CRITICAL: Extract league_key for this year to prevent wrong league fetching
                        league_key = local_data.get("league_key") or metadata.get("league_key")
                        if league_key:
                            local_league_ids[str(local_year)] = league_key
                            log(f"[LOCAL SETTINGS] {local_year}: league_key={league_key}")
                except Exception as e:
                    log(f"[LOCAL SETTINGS] Warning: Failed to process {local_file.name}: {e}")

            if local_settings_years:
                log(f"[LOCAL SETTINGS] Years with local settings: {sorted(local_settings_years)}")

            # Merge local league_ids into ctx.league_ids
            if local_league_ids:
                if not ctx.league_ids:
                    ctx.league_ids = {}
                ctx.league_ids.update(local_league_ids)
                ctx.save(context_path)
                log(f"[LOCAL SETTINGS] Updated ctx.league_ids with {len(local_league_ids)} external league key(s)")

        # -------------------------------------------------------------------------
        # PHASE 0.2: STAGING SETTINGS - Check Fly staging for external settings
        # -------------------------------------------------------------------------
        log("\n[STAGING SETTINGS] Checking Fly staging for external settings...")
        staging_settings_years: set[int] = set()
        found_staging_data = False
        try:
            staging_settings_years = pre_fetch_staging_settings(ctx, log_func=log)
            if staging_settings_years:
                found_staging_data = True
        except Exception as e:
            log(f"[STAGING SETTINGS] WARN: pre-fetch failed: {e}")
            if getattr(ctx, "has_external_data", False):
                raise

        # Combine all external settings years (local + staging)
        external_settings_years = local_settings_years | staging_settings_years

        # Pre-populate fetched_settings with local/staging JSON files so they
        # flow through the same flatten → local DuckDB path as Yahoo-fetched settings
        from multi_league.core.settings_loader import load_settings_json_files

        fetched_settings: dict[int, dict] = {}
        if external_settings_years and settings_dir.exists():
            external_raw = load_settings_json_files(settings_dir)
            for yr, raw in external_raw.items():
                if yr in external_settings_years:
                    fetched_settings[yr] = raw
            if fetched_settings:
                log(f"[SETTINGS] Loaded {len(fetched_settings)} external settings into memory")

        # Fetch settings for each year IN PARALLEL (10-12x faster than sequential)
        # Skip years that already have external settings (local or staging)
        years = list(range(ctx.start_year, (ctx.end_year or get_current_nfl_season_year()) + 1))
        years_to_fetch_from_yahoo = [y for y in years if y not in external_settings_years]

        all_settings_ok = True
        successful_years = list(external_settings_years)  # Start with external settings years as successful
        failed_years = []

        if external_settings_years:
            log(
                f"[SETTINGS] Skipping {len(external_settings_years)} year(s) with external settings: {sorted(external_settings_years)}"
            )

        if years_to_fetch_from_yahoo:
            log(
                f"[SETTINGS] Fetching settings for {len(years_to_fetch_from_yahoo)} years from Yahoo API in parallel..."
            )

        # Create a shared Yahoo session before parallel fetching. Cookie mode
        # uses the same fetchers with its browser-session transport.
        shared_oauth = None
        oauth_file = Path(ctx.oauth_file_path) if ctx.oauth_file_path else None
        try:
            mode_label = "cookie" if getattr(ctx, "yahoo_auth_mode", "oauth") == "cookie" else "OAuth"
            log(f"[SETTINGS] Creating shared {mode_label} session for parallel fetch...")
            shared_oauth = _get_shared_yahoo_session(ctx, oauth_file)
            if shared_oauth is not None:
                log("[SETTINGS] Shared Yahoo session created successfully")
        except Exception as e:
            log(f"[SETTINGS] Warning: Could not create shared Yahoo session: {e}")
            log("[SETTINGS] Falling back to per-thread Yahoo session creation")

        # Collect raw settings dicts in memory — no disk I/O
        import polars as _pl
        from multi_league.core.canonical_settings import flatten_settings as _flatten

        # fetched_settings already initialized above (may have external/staging data)

        def fetch_year_settings(year):
            """Fetch settings for a single year (runs in parallel)"""
            try:
                year_league_key = ctx.get_league_id_for_year(year) if hasattr(ctx, "get_league_id_for_year") else None
                if year_league_key:
                    log(f"[SETTINGS] Using league_key from context for {year}: {year_league_key}")

                settings = fetch_league_settings(
                    year=year,
                    league_key=year_league_key,
                    oauth=shared_oauth,
                )
                if settings:
                    return (year, True, settings)
                else:
                    return (year, False, None)
            except Exception as e:
                return (year, False, str(e))

        if years_to_fetch_from_yahoo:
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_year = {
                    executor.submit(fetch_year_settings, year): year for year in years_to_fetch_from_yahoo
                }
                for future in as_completed(future_to_year):
                    year, success, result = future.result()
                    if success:
                        num_teams = result.get("metadata", {}).get("num_teams", "?")
                        playoff_teams = result.get("metadata", {}).get("playoff_teams", "?")
                        bye_teams = result.get("metadata", {}).get("bye_teams", "?")
                        log(
                            f"[SETTINGS] [OK] {year}: {num_teams} teams, {playoff_teams} playoff spots, {bye_teams} byes"
                        )
                        successful_years.append(year)
                        fetched_settings[year] = result
                    else:
                        error_msg = result if isinstance(result, str) else "Failed to fetch settings"
                        log(f"[SETTINGS] [FAIL] {year}: {error_msg}")
                        failed_years.append(year)
                        all_settings_ok = False

        # Yahoo's own name for the league, from the most recent season fetched.
        # Yahoo already pins database_name above, so the retargeting hazard in
        # core.league_name_sync does not apply here; the helper re-checks anyway.
        try:
            from multi_league.core.league_name_sync import adopt_from_yearly_settings

            adopt_from_yearly_settings(
                ctx, fetched_settings, key="name", nested="metadata", log=log
            )
        except Exception as e:  # never fail an import over a display name
            log(f"[SETTINGS] Could not sync league name: {e}")

        yahoo_count = len(successful_years) - len(external_settings_years)
        log(
            f"\n[SETTINGS] Settings complete: {len(successful_years)} total ({len(local_settings_years)} local, {len(staging_settings_years)} staging, {yahoo_count} from Yahoo API)"
        )
        if failed_years:
            log(f"[SETTINGS] Failed years: {sorted(failed_years)}")

        results["settings"].append(("League Settings (all years)", all_settings_ok))

        if not args.dry_run and not fetched_settings:
            # No settings from any source (local, staging, or Yahoo) for any
            # year: every downstream fetcher would be skipped and the import
            # would exit 0 with an empty database. Fail loudly instead.
            raise RuntimeError(
                "League settings unavailable for every requested year -- cannot continue import"
            )

        # Flatten and save to local DuckDB — no disk round-trip
        if not args.dry_run and fetched_settings:
            log("\n[SETTINGS] Flattening and saving to local DuckDB...")
            settings_rows = []
            flatten_failures = []
            for yr, raw in sorted(fetched_settings.items()):
                try:
                    league_key = (ctx.get_league_id_for_year(yr) or ctx.league_id) if ctx else ""
                    settings_rows.append(_flatten(raw, platform="yahoo", year=yr, league_key=league_key))
                except Exception as _e:
                    flatten_failures.append((yr, str(_e)))
                    log(f"  [FAIL] year {yr}: {_e}")
            if flatten_failures:
                failed = ", ".join(f"{yr} ({err})" for yr, err in flatten_failures)
                raise RuntimeError(f"Failed to flatten league_settings for year(s): {failed}")
            if settings_rows:
                active_years = sorted({int(row["year"]) for row in settings_rows if row.get("year") is not None})
                first_active_year = active_years[0]
                last_active_year = active_years[-1]
                import_mode = getattr(ctx, "import_mode", None) or getattr(args, "import_mode", None) or "full"
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
                # Drop existing settings (quick import may have saved a single year)
                # Full import replaces with the complete set
                try:
                    db.connect().execute("DELETE FROM public.league_settings")
                except Exception:
                    pass
                # strict=False: settings_rows can mix int/float for the same
                # numeric column across years (e.g., staged 2013 JSON has int 4,
                # Yahoo API 2025 has float 4.0). Polars is strict by default.
                db.save_table(
                    "league_settings",
                    _pl.DataFrame(settings_rows, strict=False, infer_schema_length=None),
                )
                log(f"[SETTINGS] Saved {len(settings_rows)} year(s) to local DuckDB (flat canonical schema)")
            else:
                raise RuntimeError("Fetched league settings but produced 0 canonical rows")

        # -------------------------------------------------------------------------
        # PHASE 0.5: EXTERNAL DATA - Verify settings (already written in PHASE 0.2)
        # -------------------------------------------------------------------------
        # NOTE: Always log staging status regardless of has_external_data flag
        if staging_settings_years or found_staging_data:
            log("\n" + "-" * 48)
            log(
                f"[EXTERNAL DATA] Settings already written in PHASE 0.2 for {len(staging_settings_years)} year(s): {sorted(staging_settings_years)}"
            )
            log("-" * 48)

        # -------------------------------------------------------------------------
        # PHASE 0.3: EXTERNAL DATA FILES - Download from Fly staging
        # -------------------------------------------------------------------------
        # Download actual data files (matchup, draft, transactions) for external years
        # This was the MISSING PIECE causing bug - settings were downloaded but not data!
        external_data_years = set()
        if staging_settings_years:
            log("\n" + "=" * 96)
            log("PHASE 0.3: External Data Files Download")
            log("=" * 96)

            try:
                for year in sorted(staging_settings_years):
                    log(f"\n[EXTERNAL DATA] Downloading data files for {year}...")

                    # Download matchup data
                    try:
                        matchup_df = read_staging_data(db_name=ctx.league_name, table_name="matchup_data", year=year)
                        if matchup_df is not None and len(matchup_df) > 0:
                            matchup_df = coerce_staging_dtypes(matchup_df)
                            # Save to local DB
                            db.save_table(
                                "matchup",
                                matchup_df,
                                year=year,
                                platform="yahoo",
                                league_id=ctx.league_id,
                            )
                            log(f"  [OK] Matchup data: {len(matchup_df):,} rows -> local DB")
                            external_data_years.add(year)
                    except Exception as e:
                        log(f"  [WARN] Matchup data: {e}")

                    # Download draft data
                    try:
                        draft_df = read_staging_data(db_name=ctx.league_name, table_name="draft_data", year=year)
                        if draft_df is not None and len(draft_df) > 0:
                            db.save_table(
                                "draft",
                                draft_df,
                                year=year,
                                platform="yahoo",
                                league_id=ctx.league_id,
                            )
                            log(f"  [OK] Draft data: {len(draft_df):,} rows -> local DB")
                    except Exception as e:
                        log(f"  [WARN] Draft data: {e}")

                    # Download transaction data
                    try:
                        trans_df = read_staging_data(db_name=ctx.league_name, table_name="transaction_data", year=year)
                        if trans_df is not None and len(trans_df) > 0:
                            trans_df = coerce_staging_dtypes(trans_df)
                            db.save_table(
                                "transactions",
                                trans_df,
                                year=year,
                                platform="yahoo",
                                league_id=ctx.league_id,
                            )
                            log(f"  [OK] Transaction data: {len(trans_df):,} rows -> local DB")
                    except Exception as e:
                        log(f"  [WARN] Transaction data: {e}")

                    # Download player (roster) data to local DB
                    try:
                        player_df = read_staging_data(db_name=ctx.league_name, table_name="player_data", year=year)
                        if player_df is not None and len(player_df) > 0:
                            player_df = coerce_staging_dtypes(player_df)
                            db.save_table(
                                "player_fantasy",
                                player_df,
                                year=year,
                                platform="yahoo",
                                league_id=ctx.league_id,
                            )
                            log(f"  [OK] Player data: {len(player_df):,} rows -> local DB")
                    except Exception as e:
                        log(f"  [WARN] Player data: {e}")

                if external_data_years:
                    log(
                        f"\n[EXTERNAL DATA] Downloaded data files for {len(external_data_years)} year(s): {sorted(external_data_years)}"
                    )
                else:
                    log("\n[EXTERNAL DATA] No data files found in staging (settings-only)")
            except Exception as e:
                log(f"[EXTERNAL DATA] Error downloading external data: {e}")
                import traceback

                log(f"[DEBUG] {traceback.format_exc()}")

        log("=" * 96)

    # -------------------------------------------------------------------------
    # PHASE 0.4: DOWNLOAD QUICK IMPORT DATA FROM CENTRALIZED LEAGUE TABLES
    # -------------------------------------------------------------------------
    # For full imports: Check if league tables have data for years beyond our scope
    # (e.g., 2025 from a previous quick import). Download it so it flows through
    # all transformations (LAMAR, cumulative stats, etc.)
    # NOTE: Skip years with external data - staging data takes priority over remote league rows
    if args.skip_track_2_upload and not ctx.is_single_year_import and start_phase <= 1:
        log("\n[SKIP] Phase 0.4 - Quick import data download skipped (fleet/local mode)")
    elif not ctx.is_single_year_import and start_phase <= 1:
        log("\n" + "=" * 96)
        log("PHASE 0.4: Download Quick Import Data from League Tables")
        log("=" * 96)
        log("[INFO] Checking for quick import data in centralized Fly league tables...")

        try:
            backend = os.environ.get("DATABASE_BACKEND", "fly").lower()
            if backend == "fly":
                from multi_league.core.db_utils import get_pipeline_connection

                # db_name scopes the centralized league rows for this import.
                db_name = runtime.db_name
                conn = get_pipeline_connection(db_name)

                # Check what years exist in the centralized league tables OUTSIDE our import scope
                # This includes years > end_year (e.g., 2025 from quick import)
                # AND years < start_year (e.g., 2014 from external staging data)
                # NOTE: Only sync years with ROSTERED players. Years 2000-2012 may exist
                # but only with 'Unrostered' data from SQLEnrichments.expand_to_all_nfl() - skip those.
                try:
                    result = conn.execute(
                        """
                        SELECT DISTINCT year FROM ___leagues.public.player_fantasy
                        WHERE db_name = ?
                          AND (year > ? OR year < ?)
                          AND manager IS NOT NULL
                          AND manager != 'Unrostered'
                          AND manager != ''
                        ORDER BY year
                    """,
                        [db_name, ctx.end_year, ctx.start_year],
                    ).fetchall()
                    extra_years = [r[0] for r in result if r[0] is not None]

                    # Skip years with external data - staging/local data takes priority
                    # Remote league tables may have stale or wrong-league data for those years
                    if external_settings_years:
                        skipped = [y for y in extra_years if y in external_settings_years]
                        if skipped:
                            log(f"[QUICK-IMPORT-DATA] Skipping {len(skipped)} year(s) with external data: {skipped}")
                            extra_years = [y for y in extra_years if y not in external_settings_years]

                    if extra_years:
                        log(f"[QUICK-IMPORT-DATA] Found years outside scope in league tables: {extra_years}")

                        # Track downloaded years for merging later
                        remote_downloaded_years = []

                        for year in extra_years:
                            # Download player data to local DB
                            # This is ALREADY PROCESSED data - save directly to player_fantasy
                            try:
                                df = conn.execute(
                                    "SELECT * FROM ___leagues.public.player_fantasy WHERE db_name = ? AND year = ?",
                                    [db_name, year],
                                ).fetchdf()
                                if len(df) > 0:
                                    db.save_table(
                                        "player_fantasy",
                                        df,
                                        year=year,
                                        platform="yahoo",
                                        league_id=ctx.league_id,
                                    )
                                    log(f"  [OK] Player data: {len(df):,} rows for {year} -> local DB")
                                    remote_downloaded_years.append(year)
                            except Exception as e:
                                log(f"  [WARN] Player data for {year}: {e}")

                            # Download matchup data to local DB
                            try:
                                df = conn.execute(
                                    "SELECT * FROM ___leagues.public.matchup WHERE db_name = ? AND year = ?",
                                    [db_name, year],
                                ).fetchdf()
                                if len(df) > 0:
                                    db.save_table(
                                        "matchup",
                                        df,
                                        year=year,
                                        platform="yahoo",
                                        league_id=ctx.league_id,
                                    )
                                    log(f"  [OK] Matchup data: {len(df):,} rows for {year} -> local DB")
                            except Exception as e:
                                log(f"  [WARN] Matchup data for {year}: {e}")

                            # Download draft data to local DB
                            try:
                                df = conn.execute(
                                    "SELECT * FROM ___leagues.public.draft WHERE db_name = ? AND year = ?",
                                    [db_name, year],
                                ).fetchdf()
                                if len(df) > 0:
                                    db.save_table(
                                        "draft",
                                        df,
                                        year=year,
                                        platform="yahoo",
                                        league_id=ctx.league_id,
                                    )
                                    log(f"  [OK] Draft data: {len(df):,} rows for {year} -> local DB")
                            except Exception as e:
                                log(f"  [WARN] Draft data for {year}: {e}")

                            # Download transactions data to local DB
                            try:
                                df = conn.execute(
                                    "SELECT * FROM ___leagues.public.transactions WHERE db_name = ? AND year = ?",
                                    [db_name, year],
                                ).fetchdf()
                                if len(df) > 0:
                                    db.save_table(
                                        "transactions",
                                        df,
                                        year=year,
                                        platform="yahoo",
                                        league_id=ctx.league_id,
                                    )
                                    log(f"  [OK] Transactions data: {len(df):,} rows for {year} -> local DB")
                            except Exception as e:
                                log(f"  [WARN] Transactions data for {year}: {e}")

                        # CRITICAL: Extend ctx year range to include all downloaded years
                        # This ensures transformations process these years too
                        min_year = min(extra_years)
                        max_year = max(extra_years)
                        updated = False

                        if max_year > ctx.end_year:
                            log(f"\n[QUICK-IMPORT-DATA] Extending ctx.end_year from {ctx.end_year} to {max_year}")
                            ctx.end_year = max_year
                            # NOTE: Do NOT add to league_ids - that would cause player fetcher to re-fetch from Yahoo
                            # The downloaded data is already processed and should flow through directly
                            updated = True

                        if min_year < ctx.start_year:
                            log(f"[QUICK-IMPORT-DATA] Extending ctx.start_year from {ctx.start_year} to {min_year}")
                            ctx.start_year = min_year
                            updated = True

                        # CRITICAL: Track downloaded years to skip in Yahoo fetcher
                        # These years were already processed and uploaded by quick import - no need to re-fetch
                        # Store in ctx so yahoo_fantasy_data fetcher can skip them
                        if remote_downloaded_years:
                            ctx.remote_downloaded_years = remote_downloaded_years
                            log(
                                f"[QUICK-IMPORT-DATA] Stored {len(remote_downloaded_years)} year(s) to skip in Yahoo fetcher: {remote_downloaded_years}"
                            )
                            # Also remove from league_ids if present (for other fetchers that use league_ids)
                            for year in remote_downloaded_years:
                                year_str = str(year)
                                if year_str in ctx.league_ids:
                                    del ctx.league_ids[year_str]
                                    log(f"[QUICK-IMPORT-DATA] Removed {year} from league_ids")
                            updated = True

                        if updated:
                            ctx.save(context_path)
                            log(
                                f"[QUICK-IMPORT-DATA] Updated league_context.json: start_year={ctx.start_year}, end_year={ctx.end_year}"
                            )
                    else:
                        log("[QUICK-IMPORT-DATA] No additional years found in league tables")

                except Exception as e:
                    # Table might not exist yet (first import)
                    log(f"[QUICK-IMPORT-DATA] Could not query league tables: {e}")

                conn.close()
            else:
                log(f"[QUICK-IMPORT-DATA] Skipping remote lookup: DATABASE_BACKEND={backend!r} is not fly")

        except ImportError:
            log("[QUICK-IMPORT-DATA] DuckDB not available, skipping")
        except Exception as e:
            log(f"[QUICK-IMPORT-DATA] Error: {e}")
            import traceback

            log(f"[DEBUG] {traceback.format_exc()}")

        log("=" * 96)

    # -------------------------------------------------------------------------
    # PHASE 0.5: FETCH SETTINGS FOR NEWLY DISCOVERED YEARS
    # -------------------------------------------------------------------------
    # After Phase 0.4 extends the year range, check if we need settings for new years
    # This happens when quick import ran for a year (e.g., 2025) that wasn't in our range
    if not ctx.is_single_year_import and start_phase <= 1:
        # Phase 0.5: Check if any years are missing settings in the local DuckDB.
        # Phase 0 already fetches and saves all settings to DuckDB, so this only
        # triggers when Phase 0.4 extended the year range with quick-import data.
        current_year_range = list(range(ctx.start_year, (ctx.end_year or get_current_nfl_season_year()) + 1))
        try:
            db_years = {
                int(row[0])
                for row in db.connect().execute("SELECT DISTINCT year FROM public.league_settings").fetchall()
                if row[0] is not None
            }
        except Exception:
            db_years = set()

        missing_settings_years = [y for y in current_year_range if y not in db_years]

        if missing_settings_years:
            log("\n" + "=" * 96)
            log("PHASE 0.5: Fetch Settings for Newly Discovered Years")
            log("=" * 96)
            log(f"[SETTINGS] Missing settings for years: {missing_settings_years}")

            settings_dir = Path(ctx.data_directory) / "league_settings"
            oauth_path = Path(ctx.oauth_file_path) if isinstance(ctx.oauth_file_path, str) else ctx.oauth_file_path
            shared_yahoo_session = _get_shared_yahoo_session(ctx, oauth_path)
            for year in missing_settings_years:
                try:
                    year_league_key = (
                        ctx.get_league_id_for_year(year) if hasattr(ctx, "get_league_id_for_year") else None
                    )
                    if not year_league_key:
                        log(f"[SETTINGS] No league_key for {year}, skipping")
                        continue

                    settings = fetch_league_settings(
                        year=year,
                        league_key=year_league_key,
                        settings_dir=settings_dir,
                        oauth_file=oauth_path,
                        oauth=shared_yahoo_session,
                    )
                    if settings and settings.get("ok"):
                        log(f"[SETTINGS] [OK] {year}: Fetched settings for discovered year")
                    else:
                        log(f"[SETTINGS] [WARN] {year}: Could not fetch settings - optimal lineup may use fallback")
                except Exception as e:
                    log(f"[SETTINGS] [WARN] {year}: {e} - optimal lineup may use fallback")

            log("=" * 96)

    unplayed_yahoo_years: set[int] = set()
    unfetchable_yahoo_years: set[int] = set()

    # -------------------------------------------------------------------------
    # PHASE 1: FETCHERS (v3: in-process, replacing subprocess calls)
    # -------------------------------------------------------------------------
    if not args.skip_fetchers and start_phase <= 1:
        log("\n" + "=" * 96)
        log("PHASE 1: Data Fetchers")
        log("=" * 96)

        # Clear stale failure manifests from any previous run
        if not args.dry_run:
            try:
                clear_all_manifests(ctx.data_directory)
                log("[MANIFEST] Cleared stale failure manifests")
            except Exception as e:
                log(f"[MANIFEST] Warning: could not clear stale manifests: {e}")

        # In-process fetcher imports
        from multi_league.data_fetchers.yahoo.yahoo_matchups import weekly_matchup_data
        from multi_league.data_fetchers.yahoo.yahoo_rosters import fetch_rosters_for_year
        from multi_league.data_fetchers.yahoo.yahoo_draft import fetch_draft_data
        from multi_league.data_fetchers.yahoo.yahoo_transactions import fetch_transactions

        end_year = ctx.end_year or get_current_nfl_season_year()

        # Common skip-year sets
        remote_skip_years = getattr(ctx, "remote_downloaded_years", []) or []

        # -----------------------------------------------------------------
        # NFL Super Table Loader REMOVED — no downstream consumers in v3.
        # SQLEnrichments.expand_to_all_nfl() queries super_table directly via ATTACH.
        # NFL player ID resolution happens via player_bio join in SQL enrichments.
        # -----------------------------------------------------------------
        from multi_league.core.import_utils import compute_fetch_years

        all_years = list(range(ctx.start_year, end_year + 1))
        settings_failed_years = set(failed_years or []) if "failed_years" in locals() else set()
        if settings_failed_years:
            log(f"[FETCHERS] Skipping year(s) without successful settings: {sorted(settings_failed_years)}")
            unfetchable_yahoo_years |= settings_failed_years

        if hasattr(ctx, "has_league_ids_mapping") and ctx.has_league_ids_mapping():
            missing_mapped_years = {
                int(year)
                for year in all_years
                if year not in set(external_settings_years or set())
                and not (ctx.get_league_id_for_year(year) if hasattr(ctx, "get_league_id_for_year") else None)
            }
            if missing_mapped_years:
                log(f"[FETCHERS] Skipping year(s) with no mapped Yahoo league id: {sorted(missing_mapped_years)}")
                unfetchable_yahoo_years |= missing_mapped_years

        if unfetchable_yahoo_years:
            all_years = [year for year in all_years if year not in unfetchable_yahoo_years]

        local_skip_years = _detect_locally_complete_yahoo_years(db, all_years)
        if local_skip_years:
            log(f"[LOCAL CACHE] Reusing complete local year(s): {local_skip_years}")
        current_nfl_year = get_current_nfl_season_year()

        # Transaction normalize: player_name -> player
        def _normalize_transactions(df, year):
            if "player_name" in df.columns and "player" not in df.columns:
                df = df.rename(columns={"player_name": "player"})
            elif "player_name" in df.columns and "player" in df.columns:
                df["player"] = df["player"].fillna(df["player_name"])
            return df

        # Fetcher order: matchups(1 call) first, then draft(~12 calls) and
        # rosters(~17 calls) separated by transactions(1 call). Schedules
        # derived from matchup data (0 API calls). Odd/even years swap
        # which heavy runs first.
        fetcher_specs = [
            {
                "label": "Yahoo Matchups",
                "fetcher_name": "matchups",
                "table_name": "matchup",
                "years": compute_fetch_years(
                    all_years,
                    external_settings_years,
                    remote_skip_years,
                    local_skip_years,
                    "Matchups",
                    log,
                ),
                "fetch_year_fn": lambda year, oauth: weekly_matchup_data(ctx=ctx, year=year),
            },
            {
                "label": "Yahoo Transactions",
                "fetcher_name": "transactions",
                "table_name": "transactions",
                "years": compute_fetch_years(
                    all_years,
                    external_settings_years,
                    remote_skip_years,
                    local_skip_years,
                    "Transactions",
                    log,
                ),
                "fetch_year_fn": lambda year, oauth: fetch_transactions(
                    ctx=ctx,
                    year=year,
                    local_db=db,
                ),
                "normalize_fn": _normalize_transactions,
            },
            {
                "label": "Yahoo Draft",
                "fetcher_name": "draft",
                "table_name": "draft",
                "years": compute_fetch_years(
                    all_years,
                    external_settings_years,
                    remote_skip_years,
                    local_skip_years,
                    "Draft",
                    log,
                ),
                "fetch_year_fn": lambda year, oauth: fetch_draft_data(ctx=ctx, year=year),
            },
            # NOTE: Schedule is populated by the matchup fetcher (same scoreboard call).
            # No separate schedule fetcher needed.
            {
                "label": "Yahoo Rosters",
                "fetcher_name": "rosters",
                "table_name": "player_fantasy",
                "years": compute_fetch_years(
                    all_years,
                    external_settings_years,
                    remote_skip_years,
                    local_skip_years,
                    "Rosters",
                    log,
                ),
                "fetch_year_fn": lambda year, oauth: fetch_rosters_for_year(
                    ctx=ctx,
                    year=year,
                    oauth_session=oauth,
                    local_db=db,
                ),
            },
        ]

        # ---------------------------------------------------------------
        # YEAR-BY-YEAR FETCHING with EVEN/ODD ALTERNATION
        #
        # 4 fetchers: matchups(1), transactions(1), draft(~12), rosters(~17)
        # Schedule is derived from matchup (0 calls, written by matchup fetcher).
        # Draft analysis removed (ops table). ~31 calls/year total.
        #
        # Odd:  matchups(1+sched) → DRAFT(12) → transactions(1) → ROSTERS(17)
        # Even: matchups(1+sched) → ROSTERS(17) → transactions(1) → DRAFT(12)
        # ---------------------------------------------------------------
        # fetcher_specs indices: 0=matchups, 1=transactions, 2=draft, 3=rosters
        fetcher_specs_odd = [
            fetcher_specs[0],  # matchups (1 call, also writes schedule)
            fetcher_specs[2],  # draft (~12 calls)
            fetcher_specs[1],  # transactions (1 call)
            fetcher_specs[3],  # rosters (~17 calls)
        ]
        fetcher_specs_even = [
            fetcher_specs[0],  # matchups (1 call, also writes schedule)
            fetcher_specs[3],  # rosters (~17 calls)
            fetcher_specs[1],  # transactions (1 call)
            fetcher_specs[2],  # draft (~12 calls)
        ]

        all_failures = {}
        fetcher_success = {spec["label"]: True for spec in fetcher_specs}
        for year in all_years:
            league_key = ctx.get_league_id_for_year(year) if hasattr(ctx, "get_league_id_for_year") else ctx.league_id
            order = fetcher_specs_odd if year % 2 == 1 else fetcher_specs_even
            order_label = "odd" if year % 2 == 1 else "even"
            log(f"\n[YEAR {year}] Fetching all data (league {league_key}) [{order_label} order]")

            for spec in order:
                if year not in spec["years"]:
                    continue

                failures = run_v3_fetcher(
                    ctx=ctx,
                    db=db,
                    fetcher_name=spec["fetcher_name"],
                    years=[year],
                    fetch_year_fn=spec["fetch_year_fn"],
                    table_name=spec["table_name"],
                    normalize_fn=spec.get("normalize_fn"),
                    sleep_between_years=0,
                )
                if (
                    spec["fetcher_name"] == "matchups"
                    and not _local_table_has_year_rows(db, "matchup", year)
                    and (
                        year >= current_nfl_year
                        or _allow_empty_quick_fallback_target(ctx, db, year)
                        or _local_schedule_year_has_no_played_rows(db, year)
                    )
                ):
                    # Yahoo can renew a league into the upcoming season before
                    # any games have been played. Quick fallback imports can
                    # also target an older Yahoo shell that has settings but no
                    # played matchups; the prior mapped year carries the usable
                    # league data in that case. Treat those empty matchup
                    # seasons as intentionally unplayed, not recoverable gaps.
                    unplayed_yahoo_years.add(year)
                    for manifest_name in ("matchups", "rosters", "draft", "transactions", "schedule", "schedules"):
                        clear_manifest(ctx.data_directory, manifest_name, year)
                    if year >= current_nfl_year:
                        log(
                            f"[YEAR {year}] No played Yahoo matchups found; "
                            "skipping remaining fetchers and recovery manifests for this unplayed season"
                        )
                    elif _local_schedule_year_has_no_played_rows(db, year):
                        log(
                            f"[YEAR {year}] Yahoo returned a schedule shell with no played matchups; "
                            "skipping remaining fetchers and recovery manifests for this empty season"
                        )
                    else:
                        log(
                            f"[YEAR {year}] No played Yahoo matchups found; using prior quick fallback season "
                            "and skipping remaining fetchers/recovery manifests for this empty shell"
                        )
                    break

                if failures:
                    fetcher_success[spec["label"]] = False
                    all_failures.update(failures)

            # Brief pause between years to let rate limit window slide
            if year != all_years[-1]:
                time.sleep(2)

        for spec in fetcher_specs:
            results["fetchers"].append((spec["label"], fetcher_success[spec["label"]]))

        # -----------------------------------------------------------------
        # Fetcher summary
        # -----------------------------------------------------------------
        log(f"\n[FETCHERS] Complete. {len(all_failures)} year-level failure(s) across all fetchers.")

        # PHASE 1.5: YAHOO RECOVERY
        # Replaces legacy Phase 1.5 (manifest retry) + Phase 1.9 (validate_fetch_completeness).
        # Detects ALL gaps in fetched data (manifests + local DuckDB audit), then fills them
        # with surgical Yahoo API calls using escalating cooldowns.
        log("\n" + "=" * 96)
        log("PHASE 1.5: Yahoo Recovery (rate-limit gap detection + fill)")
        log("=" * 96)

        try:
            from multi_league.data_fetchers.yahoo.yahoo_recovery import run_yahoo_recovery

            # Use settings already fetched in Phase 0 (in-memory, no disk I/O)
            recovery_league_settings = dict(fetched_settings) if fetched_settings else {}

            # Fallback: use ctx year range if no settings files found
            if not recovery_league_settings:
                end_yr = ctx.end_year or get_current_nfl_season_year()
                for y in range(ctx.start_year, end_yr + 1):
                    recovery_league_settings[y] = {}

            # Years whose data came from external staging are NOT fetchable
            # from Yahoo (league_key is usually pre-API-history). Skip gap
            # detection for those years entirely — otherwise recovery loops
            # forever hitting rate limits / 401s that never resolve.
            staging_exclude = (
                set(staging_settings_years or set())
                | set(external_data_years or set())
                | set(unplayed_yahoo_years)
                | set(unfetchable_yahoo_years)
            )

            recovery_result = run_yahoo_recovery(
                ctx=ctx,
                league_settings=recovery_league_settings,
                dead_man_minutes=120,
                local_db=db,
                exclude_years=staging_exclude if staging_exclude else None,
            )

            if recovery_result.success:
                resolved_count = len(recovery_result.resolved)
                hard_missing_count = len(recovery_result.hard_missing)
                if recovery_result.rounds > 0:
                    log(
                        f"[RECOVERY] Complete — {resolved_count} gaps resolved, "
                        f"{hard_missing_count} hard missing, "
                        f"{recovery_result.rounds} rounds ({recovery_result.elapsed_seconds:.0f}s)"
                    )
                else:
                    log("[RECOVERY] No gaps detected — all data complete")
            else:
                unresolved = recovery_result.unresolved
                hard_missing = recovery_result.hard_missing
                log(
                    f"[RECOVERY] WARNING — {len(unresolved)} gaps unresolved, "
                    f"{len(hard_missing)} hard missing after "
                    f"{recovery_result.elapsed_seconds:.0f}s"
                )
                if unresolved:
                    for gap in unresolved[:5]:
                        log(f"[RECOVERY]   {gap.fetcher} {gap.year} {gap.detail} ({gap.attempts} attempts)")
                    if len(unresolved) > 5:
                        log(f"[RECOVERY]   ... and {len(unresolved) - 5} more")
                log("[RECOVERY] Continuing with available data — use --fetch to fill gaps later")

        except ImportError:
            log("[RECOVERY] yahoo_recovery module not available — skipping (legacy mode)")
        except Exception as e:
            log(f"[RECOVERY] Warning: Recovery util failed with {e} — continuing with available data")

        log("\n[OK] Proceeding to Phase 2.")

    # -------------------------------------------------------------------------
    # PHASE 2: MERGES
    # -------------------------------------------------------------------------
    if start_phase <= 2 and not args.skip_transformations:
        log("\n" + "=" * 96)
        log("PHASE 2: Merges")
        log("=" * 96)

        # =========================================================================
        # TRACK 1: NFL Super Table already updated before PHASE 0
        # =========================================================================
        # NOTE: Track 1 runs ONCE before fetchers (line ~710). No need to run again here.
        # The super table was already refreshed with the latest week's data.
        # Removing this duplicate call saves ~5 seconds and avoids re-downloading
        # the same 16k+ records from NFLverse.

        # -------------------------------------------------------------------------
        # PHASE 2.0 SKIPPED: yahoo_nfl_merge replaced by canonical pipeline.
        # -------------------------------------------------------------------------
        # Roster data is already in LocalLeagueDB (saved per-year in Phase 1c
        # via normalize_roster_df). NFL_player_id, player_week, nfl_team, and
        # cumulative_week are resolved by SQL enrichments
        # (resolve_all_nfl_player_ids via player_bio join) during local SQL enrichments.
        log("\n" + "=" * 96)
        log("PHASE 2.0: SKIPPED — roster data already in local DB, NFL IDs resolved by SQL enrichments")
        log("=" * 96)

        # Verify player_fantasy exists in local DB from Phase 1c
        if db.table_exists("player_fantasy") and db.row_count("player_fantasy") > 0:
            pf_count = db.row_count("player_fantasy")
            log(f"  [OK] player_fantasy: {pf_count:,} rows in local DB (from Phase 1c fetchers)")
            results["merges"].append(("Yahoo+NFL Merge (skipped - canonical)", True))
        else:
            log("  [WARN] player_fantasy is empty or missing — fetchers may have failed")
            results["merges"].append(("Yahoo+NFL Merge (skipped - canonical)", False))

        # -------------------------------------------------------------------------
        # PHASE 2.1: VERIFY LOCAL DB TABLES
        # -------------------------------------------------------------------------
        # Phase 2.0.5 (remote cache merge) and Phase 2.1 (parquet consolidation)
        # are no longer needed — DuckDB table preserves all years automatically.
        log("\n" + "=" * 96)
        log("PHASE 2.1: Verifying local DB tables...")
        log("=" * 96)
        for table_name in db.list_tables():
            count = db.row_count(table_name)
            log(f"  {table_name}: {count:,} rows")

        # --- FANTASY POINTS ALIAS (check in local DB) --------------------------------
        try:
            log("\n[VERIFY] Ensuring fantasy_points column in player_fantasy...")
            if db.table_exists("player_fantasy") and db.row_count("player_fantasy") > 0:
                conn = db.connect()
                table_cols = {
                    r[0]
                    for r in conn.execute(
                        "SELECT column_name FROM information_schema.columns "
                        "WHERE table_schema = 'public' AND table_name = 'player_fantasy'"
                    ).fetchall()
                }
                if "fantasy_points" not in table_cols:
                    candidates = [
                        "fantasy_points_total",
                        "fantasy_points_ppr",
                        "fpts",
                        "points",
                        "FPTS",
                        "fantasy_points_std",
                    ]
                    chosen = None
                    for c in candidates:
                        if c in table_cols:
                            chosen = c
                            break
                    if chosen:
                        conn.execute("ALTER TABLE public.player_fantasy ADD COLUMN IF NOT EXISTS fantasy_points DOUBLE")
                        conn.execute(f'UPDATE public.player_fantasy SET fantasy_points = "{chosen}"')
                        log(f"  Added fantasy_points alias from {chosen}")
                    else:
                        log("  WARNING: No suitable points column found for alias")
                else:
                    log("  OK: fantasy_points column already exists")
            else:
                log("  SKIP: player_fantasy not found or empty")
        except Exception as e:
            log(f"  WARNING: Failed to ensure fantasy_points alias: {e}")

        # -------------------------------------------------------------------------
        # PHASE 2.2: MERGE STAGING DATA (Full imports only)
        # -------------------------------------------------------------------------
        # Merge external staging data (pre-league years) with Yahoo data
        # Now runs for ALL imports (both full and quick) to ensure staged data is included
        # Skip in fleet mode (--skip-track-2-upload) — staging data comes from web UI,
        # not relevant for fleet reimports, and avoids unnecessary remote roundtrip.
        if args.skip_track_2_upload:
            log("\n[SKIP] Phase 2.2 - Staging data merge skipped (fleet/local mode)")
            results["merges"].append(("Staging Data Merge", True))
        else:
            log("\n" + "=" * 96)
            log("PHASE 2.2: Merge Staging Data")
            log("=" * 96)
            import_mode_desc = "quick" if ctx.is_single_year_import else "full"
            log(f"[STAGING] Checking for external staging data to merge ({import_mode_desc} import mode)...")

            # -----------------------------------------------------------------
            # PHASE 1.7: Schema-Conform external uploads before staging merge
            # -----------------------------------------------------------------
            from multi_league.core.import_pipeline import run_phase_1_7
            from multi_league.external_ingest.schema_conform import (
                SchemaConformAbort,
                EXIT_CODE_SCHEMA_CONFORM_ABORT,
            )

            try:
                run_phase_1_7(db.connect(), ctx)
                log("[PHASE 1.7] Schema-conform complete (or no external data)")
            except SchemaConformAbort as _sc_err:
                log(f"[PHASE 1.7] HARD ABORT: {_sc_err}  failures={[vars(f) for f in _sc_err.failures]}")
                raise SystemExit(EXIT_CODE_SCHEMA_CONFORM_ABORT)  # noqa: B904 - intentional: preserve SchemaConformAbort traceback
            # -----------------------------------------------------------------

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

        # -------------------------------------------------------------------------
        # PHASE 2.6: VALIDATE LOCAL DB TABLES
        # -------------------------------------------------------------------------
        log("\n" + "=" * 96)
        log("PHASE 2.6: Validate Local DB")
        log("=" * 96)

        required_tables = ["matchup", "player_fantasy", "draft", "transactions", "schedule", "league_settings"]
        missing = []
        for t in required_tables:
            if db.table_exists(t):
                count = db.row_count(t)
                log(f"  [OK] {t}: {count:,} rows")
            else:
                log(f"  [FAIL] {t}: MISSING")
                missing.append(t)

        if missing:
            log(f"[VALIDATE] {len(missing)} required table(s) missing: {missing}")
        else:
            log("[VALIDATE] [OK] All required tables present in local DB")

        # =========================================================================
        # TRACK 2: Create skinny centralized league table
        # =========================================================================
        if not args.skip_track_2_upload and start_phase <= 2:
            log("\n" + "=" * 96)
            log("[TRACK 2] CREATING CENTRALIZED LEAGUE TABLE")
            log("=" * 96)

            # Track 2: Create centralized league table
            log("[TRACK 2] Centralized table creation is handled in transformations")
        elif args.skip_track_2_upload:
            log("\n[SKIP] Track 2 - Centralized table upload skipped by user")
            results["track2"].append(("League Table", True))

        log("\n[MERGES] Merge phase complete. All data is now unified.")
        log(f"[SUMMARY] Unified outputs:\n{json.dumps(results, indent=2)}")

    # -------------------------------------------------------------------------
    # PHASE 2.7: REMOVED — transformations now run on local DuckDB
    # Single upload happens at Phase 4 after all transformations complete.
    # -------------------------------------------------------------------------

    # -------------------------------------------------------------------------
    # PHASE 3: TRANSFORMATIONS (local-first — operates on local DuckDB)
    # -------------------------------------------------------------------------
    if start_phase <= 3 and not args.skip_transformations:
        log("\n" + "=" * 96)
        log("PHASE 3: Transformations (local-first)")
        log("=" * 96)

        # Close local DB so subprocess scripts can get exclusive access
        db.close()
        log("[LOCAL DB] Closed for Phase 3 (subprocess scripts need exclusive access)")

        transform_results = run_transformation_pipeline(
            ctx,
            dry_run=args.dry_run,
            skip_track_2_upload=args.skip_track_2_upload,
            import_mode=getattr(args, "import_mode", "full"),
            context_file_path=str(context_path),
            platform="yahoo",
            db_name=db_name,
            data_dir=str(data_dir),
            quick=ctx.is_single_year_import,
        )
        results["transformations"].extend(transform_results)

        # Reopen local DB for upload
        db.connect()
        log("[LOCAL DB] Reopened after Phase 3")

        log("\n[TRANSFORMATIONS] Transformation phase complete.")

        # Final verification of unified outputs
        verify_unified_outputs(ctx, db=db)

    # -------------------------------------------------------------------------
    # PHASE 3.5: SQL ENRICHMENTS (local DuckDB — before upload)
    # -------------------------------------------------------------------------
    if start_phase <= 3 and not args.skip_transformations and not args.dry_run:
        log("\n" + "=" * 96)
        log("PHASE 3.5: SQL Enrichments (local DuckDB)")
        log("=" * 96)
        eng = None
        try:
            eng = _create_local_sql_enricher(
                ctx=ctx,
                db_name=db_name,
                data_dir=str(data_dir),
                conn=db.connect(),
            )
            eng.load_settings_from_db()
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
            _require_sql_enrichment_success(enrichment_results)
            fantasy_agg_ok = run_local_fantasy_aggregation(
                db_name=db_name,
                data_dir=str(data_dir),
                dry_run=args.dry_run,
                conn=eng.conn,
            )
            results["transformations"].append(("Fantasy Aggregation (local)", fantasy_agg_ok))
            _require_sql_enrichment_success(
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

        # Reopen local DB (enrichments may have closed it)
        if not db._conn:
            db.connect()

        log("\n" + "=" * 96)
        log("PHASE 3.6: Local Pre-Upload Validation")
        log("=" * 96)
        try:
            from multi_league.validation.checks.pipeline_checks import (
                check_pre_upload_sanity,
                check_transform_output_non_empty,
            )

            validation_conn = db.connect()
            validation_errors: list[str] = []
            required_tables = ["league_settings", "matchup", "player_fantasy", "draft", "transactions", "schedule"]
            allow_empty_quick_startup = _allow_empty_quick_startup(ctx, db, unplayed_yahoo_years)
            non_empty_tables = _pre_upload_non_empty_tables(
                allow_empty_quick_startup=allow_empty_quick_startup,
                allow_partial_history_source=args.allow_partial_history_source,
            )
            if allow_empty_quick_startup:
                log(
                    "[PRE-UPLOAD] Quick import is an unplayed startup shell; "
                    "allowing empty fantasy tables and uploading available settings/activity"
                )
            elif args.allow_partial_history_source:
                log(
                    "[PRE-UPLOAD] Historical source mode: requiring settings and player data; "
                    "retaining other available source tables for the final merged validation"
                )

            validation_errors.extend(check_transform_output_non_empty(validation_conn, non_empty_tables))
            validation_errors.extend(check_pre_upload_sanity(validation_conn))
            for table_name in required_tables:
                validation_errors.extend(f"CANONICAL_SCHEMA: {msg}" for msg in db.validate_table_schema(table_name))

            if validation_errors:
                for error in validation_errors[:50]:
                    log(f"[PRE-UPLOAD][FAIL] {error}")
                if len(validation_errors) > 50:
                    log(f"[PRE-UPLOAD][FAIL] ... plus {len(validation_errors) - 50} more issue(s)")
                raise RuntimeError(f"Local DuckDB failed pre-upload validation with {len(validation_errors)} issue(s)")

            log("[PRE-UPLOAD] OK: Local DuckDB passed canonical schema + sanity validation")
            results["transformations"].append(("Local Pre-Upload Validation", True))
        except Exception as e:
            results["transformations"].append(("Local Pre-Upload Validation", False))
            db.close()
            raise
        finally:
            # Upload should reopen the file cleanly for its own connection lifecycle.
            db.close()

    # -------------------------------------------------------------------------
    # PHASE 4: TRACK 2 UPLOAD (Post-Enrichment)
    # -------------------------------------------------------------------------
    if start_phase <= 3 and not args.skip_transformations and not args.skip_track_2_upload:
        log("\n" + "=" * 96)
        log("PHASE 4: Track 2 Upload (Post-Enrichment)")
        log("=" * 96)
        # upload_to_fly closes local conn, stages the local database, then reopens
        track2_results = upload_yahoo_tables(ctx, db=db, dry_run=args.dry_run)
        results["track2"].extend(track2_results)

    # -------------------------------------------------------------------------
    # POST-PROCESSING: Run any final utility functions or cleanups
    # -------------------------------------------------------------------------
    log("\n" + "=" * 96)
    log("POST-PROCESSING")
    log("=" * 96)

    log("[POST-PROCESSING] No temporary files to clean up (parquet eliminated)")

    # Verify local DB tables
    log("\n[POST-PROCESSING] Verifying local DB tables...")

    canonical_tables = {
        "player_fantasy": ["player_lamar", "manager_lamar", "player_week"],
        "matchup": ["wins_to_date", "losses_to_date", "champion", "sacko", "final_playoff_seed"],
        "draft": [],  # manager_lamar, draft_grade added by SQL enrichments
        "transactions": [],  # fa_lamar_ros, transaction_score added by SQL enrichments
    }

    missing_tables = []
    for table_name, expected_cols in canonical_tables.items():
        if db.table_exists(table_name):
            count = db.row_count(table_name)
            if count == 0:
                log(f"  [WARN] {table_name}: EXISTS but EMPTY")
            else:
                conn = db.connect()
                table_cols = {
                    r[0]
                    for r in conn.execute(
                        f"SELECT column_name FROM information_schema.columns "
                        f"WHERE table_schema = 'public' AND table_name = '{table_name}'"
                    ).fetchall()
                }
                log(f"  [OK] {table_name}: {count:,} rows, {len(table_cols)} columns")
                if expected_cols:
                    found = [c for c in expected_cols if c in table_cols]
                    missing = [c for c in expected_cols if c not in table_cols]
                    if missing:
                        log(f"      [WARN] Missing expected: {missing}")
                    if found:
                        log(f"      [OK] Has expected: {found}")
        else:
            missing_tables.append(table_name)
            log(f"  [FAIL] {table_name}: MISSING")

    if missing_tables:
        log(f"\n[POST-PROCESSING] [WARN] WARNING: Missing tables: {missing_tables}")
    else:
        log("\n[POST-PROCESSING] [OK] All canonical tables present")

    log("\n[POST-PROCESSING] Complete.")

    log("\n" + "=" * 96)
    log("INITIAL IMPORT V3 - In-Process Pipeline Complete")
    log("=" * 96)

    # Summary of results
    log(f"\n[SUMMARY] Track 1: {results['track1']}")
    log(f"[SUMMARY] Settings: {results['settings']}")
    log(f"[SUMMARY] Fetchers: {results['fetchers']}")
    log(f"[SUMMARY] Merges:   {results['merges']}")
    log(f"[SUMMARY] Transformations: {results['transformations']}")

    log("\n[FINALIZE] All steps complete. You can now access your league data.")

    # NOTE: Full historical import trigger is handled by the workflow's trigger-downstream action
    # (see .github/actions/trigger-downstream/action.yml)
    # This decouples the Python script from workflow orchestration concerns.

    log("\n[SUCCESS] Import pipeline finished successfully!")

    # Close local DB
    db.close()
    log(f"[LOCAL DB] Closed {db.db_path}")

    # Clean up temp data directory if using --league mode (skip if --data-dir was provided)
    if args.league and not getattr(args, "data_dir", None) and hasattr(ctx, "data_directory"):
        import shutil

        temp_dir = Path(ctx.data_directory)
        if temp_dir.exists() and "yahoo_v3_" in str(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
            log(f"[CLEANUP] Removed temp data dir: {temp_dir}")


# =========================================================================
# ENTRY POINT
# =========================================================================
if __name__ == "__main__":
    main()
