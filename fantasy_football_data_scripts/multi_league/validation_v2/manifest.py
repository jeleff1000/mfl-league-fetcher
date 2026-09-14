"""Manifest builder for validation_v2.

Provides two functions:
- build_manifest(settings_rows, inventory): pure Python, no DB required
- load_manifest(conn, table_prefix): loads from SQL then calls build_manifest
"""

from __future__ import annotations

from collections import defaultdict

import duckdb

from multi_league.validation_v2.models import Manifest
from multi_league.validation_v2.scope_sql import published_league_scope_sql

# Default table prefix for MotherDuck centralized DB
DEFAULT_TABLE_PREFIX = "___leagues.public."


def build_manifest(settings_rows: list[dict], inventory: list[str]) -> Manifest:
    """Build a Manifest from settings rows and validator scope list.

    Pure Python — no DB connection required.

    Args:
        settings_rows: List of dicts, one per league-year from league_settings.
            Expected keys: db_name, platform, uses_median, num_teams,
            playoff_start_week, playoff_teams, waiver_budget, max_keepers,
            is_dynasty, end_week, draft_type, year.
        inventory: List of db_name strings from validator scope discovery.

    Returns:
        Manifest with all feature subsets populated.
        full_import_leagues and sim_leagues are left empty (require data queries).
    """
    scope_leagues = sorted(
        {
            *inventory,
            *(row["db_name"] for row in settings_rows if row.get("db_name")),
        }
    )
    if not scope_leagues:
        return Manifest()

    # Group settings rows by db_name
    rows_by_league: dict[str, list[dict]] = defaultdict(list)
    for row in settings_rows:
        rows_by_league[row["db_name"]].append(row)

    # Feature sets — built from settings
    median_leagues: set[str] = set()
    keeper_leagues: set[str] = set()
    dynasty_leagues: set[str] = set()
    faab_leagues: set[str] = set()
    espn_leagues: set[str] = set()
    yahoo_leagues: set[str] = set()
    sleeper_leagues: set[str] = set()
    multi_year_leagues: set[str] = set()
    consolation_leagues: set[str] = set()

    # Settings dict: most recent year's row per league
    settings: dict[str, dict] = {}

    for db_name, rows in rows_by_league.items():
        # median: True if any year has uses_median == True
        if any(r.get("uses_median") for r in rows):
            median_leagues.add(db_name)

        # keeper: max_keepers >= 2 (any year) — dynasty leagues have their own gate.
        # max_keepers=1 is Sleeper's default for ALL leagues (including redraft).
        # Leagues with actual keeper data are added in load_manifest() via discovery.
        if any((r.get("max_keepers") or 0) >= 2 for r in rows):
            keeper_leagues.add(db_name)

        # dynasty: is_dynasty == True (any year)
        if any(r.get("is_dynasty") for r in rows):
            dynasty_leagues.add(db_name)

        # faab: waiver_budget > 0 (any year)
        if any((r.get("waiver_budget") or 0) > 0 for r in rows):
            faab_leagues.add(db_name)

        # platform buckets — use the most common platform across years
        # (in practice all years should share one platform, just pick first)
        platform = (rows[-1].get("platform") or "").lower()
        if platform == "espn":
            espn_leagues.add(db_name)
        elif platform == "yahoo":
            yahoo_leagues.add(db_name)
        elif platform == "sleeper":
            sleeper_leagues.add(db_name)

        # multi_year: more than 1 distinct year
        distinct_years = {r.get("year") for r in rows if r.get("year") is not None}
        if len(distinct_years) > 1:
            multi_year_leagues.add(db_name)

        # consolation: settings explicitly indicate a consolation bracket/field
        if any(_truthy(r.get("has_consolation_bracket")) for r in rows) or any(
            _positive_int(r.get("num_playoff_consolation_teams")) for r in rows
        ):
            consolation_leagues.add(db_name)

        # most recent year's settings row
        valid_rows = [r for r in rows if r.get("year") is not None]
        if valid_rows:
            settings[db_name] = max(valid_rows, key=lambda r: r["year"])

    def _sorted(s: set[str]) -> list[str]:
        """Return a sorted list for deterministic output."""
        return sorted(s)

    return Manifest(
        all_leagues=list(scope_leagues),
        median_leagues=_sorted(median_leagues),
        consolation_leagues=_sorted(consolation_leagues),
        keeper_leagues=_sorted(keeper_leagues),
        dynasty_leagues=_sorted(dynasty_leagues),
        faab_leagues=_sorted(faab_leagues),
        espn_leagues=_sorted(espn_leagues),
        yahoo_leagues=_sorted(yahoo_leagues),
        sleeper_leagues=_sorted(sleeper_leagues),
        multi_year_leagues=_sorted(multi_year_leagues),
        full_import_leagues=[],  # populated by load_manifest
        sim_leagues=[],  # populated by load_manifest
        settings=settings,
    )


def _truthy(value) -> bool:
    """Return True for bool-ish values from SQL/settings rows."""
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, int | float):
        return value != 0
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _positive_int(value) -> bool:
    try:
        return int(value or 0) > 0
    except (TypeError, ValueError):
        return False


def _settings_select_sql(conn: duckdb.DuckDBPyConnection, table_prefix: str) -> str:
    """Build a league_settings SELECT that tolerates older local test schemas."""
    base_cols = [
        "db_name",
        "platform",
        "num_teams",
        "playoff_start_week",
        "playoff_teams",
        "uses_median",
        "end_week",
        "waiver_budget",
        "max_keepers",
        "is_dynasty",
        "draft_type",
        "year",
    ]
    optional_cols = ["has_consolation_bracket", "num_playoff_consolation_teams"]
    try:
        rows = conn.execute(f"DESCRIBE {table_prefix}league_settings").fetchall()
        available = {row[0] for row in rows}
    except Exception:
        available = set(base_cols)

    select_parts = list(base_cols)
    for col in optional_cols:
        if col in available:
            select_parts.append(col)
        else:
            select_parts.append(f"NULL AS {col}")
    return f"SELECT {', '.join(select_parts)} FROM {table_prefix}league_settings"


def load_manifest(
    conn: duckdb.DuckDBPyConnection,
    table_prefix: str = DEFAULT_TABLE_PREFIX,
) -> Manifest:
    """Load a Manifest from SQL queries against the centralized DB.

    Runs settings + inventory queries, calls build_manifest, then runs
    discovery queries to populate sim_leagues and full_import_leagues.

    Args:
        conn: DuckDB connection (local or MotherDuck).
        table_prefix: SQL prefix for table references (default: ___leagues.public.).

    Returns:
        Fully populated Manifest including sim_leagues and full_import_leagues.
    """
    # Load league_settings
    settings_sql = _settings_select_sql(conn, table_prefix)
    settings_rows_raw = conn.execute(settings_sql).fetchall()
    settings_cols = [desc[0] for desc in conn.description]
    settings_rows = [dict(zip(settings_cols, row)) for row in settings_rows_raw]

    # Also need scoring_rec for reference (not in build_manifest but documented in spec)
    # Load league_inventory
    # league_inventory lives in ___ops.accounts, not ___leagues
    inventory_sql = published_league_scope_sql(table_prefix=table_prefix, league_list=None)
    inventory_rows = conn.execute(inventory_sql).fetchall()
    inventory = [row[0] for row in inventory_rows]
    inventory_set = set(inventory)
    settings_rows = [row for row in settings_rows if row.get("db_name") in inventory_set]

    # Build the core manifest
    manifest = build_manifest(settings_rows, inventory)

    # Discovery: sim_leagues — leagues with x0_win data in matchup
    try:
        sim_sql = f"""
            SELECT DISTINCT db_name
            FROM {table_prefix}matchup
            WHERE x0_win IS NOT NULL
        """
        sim_rows = conn.execute(sim_sql).fetchall()
        manifest.sim_leagues = sorted(row[0] for row in sim_rows)
    except Exception:
        # Column may not exist yet on some local test DBs
        manifest.sim_leagues = []

    # Discovery: keeper_leagues — merge settings-based with data-based (actual is_keeper rows)
    try:
        keeper_sql = f"""
            SELECT DISTINCT db_name
            FROM {table_prefix}draft
            WHERE is_keeper IS NOT NULL AND is_keeper = 1
        """
        keeper_rows = conn.execute(keeper_sql).fetchall()
        data_keeper_leagues = {row[0] for row in keeper_rows}
        # Merge: settings-based (max_keepers >= 2) + data-based (actual keeper picks)
        manifest.keeper_leagues = sorted(set(manifest.keeper_leagues) | data_keeper_leagues)
    except Exception:
        pass  # Keep settings-based list as fallback

    # Discovery: consolation_leagues — merge settings-based with actual data.
    try:
        consolation_sql = f"""
            SELECT DISTINCT db_name
            FROM {table_prefix}matchup
            WHERE is_consolation = 1
        """
        consolation_rows = conn.execute(consolation_sql).fetchall()
        data_consolation_leagues = {row[0] for row in consolation_rows}
        manifest.consolation_leagues = sorted(set(manifest.consolation_leagues) | data_consolation_leagues)
    except Exception:
        pass

    # Discovery: full_import_leagues — leagues with Unrostered players in player_fantasy
    try:
        full_import_sql = f"""
            SELECT DISTINCT db_name
            FROM {table_prefix}player_fantasy
            WHERE manager = 'Unrostered' OR LOWER(TRIM(manager)) = 'unrostered'
        """
        full_import_rows = conn.execute(full_import_sql).fetchall()
        manifest.full_import_leagues = sorted(row[0] for row in full_import_rows)
    except Exception:
        manifest.full_import_leagues = []

    return manifest
