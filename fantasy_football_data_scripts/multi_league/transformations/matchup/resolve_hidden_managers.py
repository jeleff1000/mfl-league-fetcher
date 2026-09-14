"""
Resolve Hidden Manager Names Using GUID

This transformation runs early in the pipeline to unify manager names across
years using the persistent Yahoo manager GUID.

When a manager has their profile set to private (--hidden--), their name
falls back to their team name, which can change from year to year. This
makes it difficult to track the same manager across seasons.

The GUID is a persistent identifier that stays the same across all years
for the same Yahoo user. This transformation:

1. Groups all records by manager_guid
2. For each guid, finds the most recent year's manager name
3. Updates all records for that guid to use that consistent name

This ensures the same person always shows with the same name, even if
their team name changes across seasons.

Usage:
    python resolve_hidden_managers.py --context path/to/league_context.json
    python resolve_hidden_managers.py --context path/to/league_context.json --dry-run
"""

import argparse

import pandas as pd

try:
    from multi_league.shared.import_setup import setup_module_path
except ImportError:
    import sys as _sys
    from pathlib import Path as _Path

    _d = _Path(__file__).resolve().parent
    while not (_d / "shared" / "import_setup.py").exists() and _d != _d.parent:
        _d = _d.parent
    _sys.path.insert(0, str(_d.parent))
    del _d
    from multi_league.shared.import_setup import setup_module_path
setup_module_path()

from multi_league.shared.filters import UNROSTERED_VALUES
from multi_league.core.db_utils import get_pipeline_connection, sanitize_database_name
from multi_league.core.manager_identity import (
    HIDDEN_MANAGER_GUID_TOKENS,
    hidden_manager_guid_mask,
    is_hidden_manager_guid,
)
from multi_league.core.sql_utils import execute_scoped


CENTRAL_DB_NAME = "___leagues"
ACTIVE_TABLE_CATALOG = CENTRAL_DB_NAME


def current_catalog(conn) -> str:
    return conn.execute("SELECT current_database()").fetchone()[0]


def configure_table_catalog(conn) -> None:
    global ACTIVE_TABLE_CATALOG
    catalog = current_catalog(conn)
    if catalog == "memory" and ACTIVE_TABLE_CATALOG not in {CENTRAL_DB_NAME, "memory"}:
        return
    ACTIVE_TABLE_CATALOG = catalog


def _ensure_active_catalog(conn) -> None:
    if ACTIVE_TABLE_CATALOG != current_catalog(conn):
        configure_table_catalog(conn)


def central_table(table_name: str) -> str:
    return f"{ACTIVE_TABLE_CATALOG}.public.{table_name}"


def league_db_filter(db_name: str, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    return f"{prefix}db_name = '{db_name}'"


def _read_table(conn, db_name: str, table_name: str) -> pd.DataFrame:
    """Read a scoped table, returning an empty DataFrame if not found."""
    _ensure_active_catalog(conn)
    table_sql = central_table(table_name)
    try:
        return conn.execute(f"SELECT * FROM {table_sql} WHERE {league_db_filter(db_name)}").fetchdf()
    except Exception:
        if ACTIVE_TABLE_CATALOG == CENTRAL_DB_NAME:
            return pd.DataFrame()
        try:
            return conn.execute(f"SELECT * FROM {table_sql}").fetchdf()
        except Exception:
            return pd.DataFrame()


def _replace_scoped_table_from_dataframe(conn, db_name: str, table_name: str, df: pd.DataFrame) -> None:
    """Replace a table's rows using scoped delete+insert semantics."""
    _ensure_active_catalog(conn)
    table_sql = central_table(table_name)

    payload = df.copy()
    if "db_name" not in payload.columns:
        payload.insert(0, "db_name", db_name)
    else:
        payload["db_name"] = db_name

    upload = payload.drop(columns=["db_name"], errors="ignore")
    conn.register("_upload", upload)
    try:
        conn.execute(f"CREATE TABLE IF NOT EXISTS {table_sql} AS SELECT * FROM _upload WHERE 1 = 0")
        try:
            conn.execute(f"ALTER TABLE {table_sql} ADD COLUMN IF NOT EXISTS db_name VARCHAR")
        except Exception:
            pass

        execute_scoped(
            conn,
            f"DELETE FROM {table_sql} WHERE {league_db_filter(db_name)}",
            db_name,
            label=f"{table_name}:delete",
        )

        insert_cols = list(upload.columns) + ["db_name"]
        quoted_insert_cols = ", ".join(f'"{col}"' for col in insert_cols)
        select_cols = ", ".join(f'"{col}"' for col in upload.columns)
        execute_scoped(
            conn,
            f"INSERT INTO {table_sql} ({quoted_insert_cols}) "
            f"SELECT {select_cols}, '{db_name}' AS db_name FROM _upload",
            db_name,
            label=f"{table_name}:insert",
        )
    finally:
        conn.unregister("_upload")


def is_valid_guid(guid) -> bool:
    """
    Check if a GUID is valid (not a placeholder for hidden managers).

    Invalid GUIDs include:
    - None, NaN, empty string
    - '--' (Yahoo's placeholder for hidden managers)
    - Any other placeholder patterns

    When a GUID is invalid, we should NOT unify records because they may be
    different people who both have hidden profiles.
    """
    return not is_hidden_manager_guid(guid)


def create_missing_manager_stubs(
    matchup_df: pd.DataFrame, player_df: pd.DataFrame, user_overrides: dict = None
) -> tuple:
    """
    Create stub rows in player_fantasy for managers that exist in matchup but not player_fantasy.

    This handles hidden managers whose roster data wasn't captured by the API.
    We create minimal stub rows so they appear consistently across all tables.

    Args:
        matchup_df: Matchup DataFrame with manager, year, week columns
        player_df: Player fantasy DataFrame
        user_overrides: Dict mapping team_name -> real_name (for hidden managers)

    Returns:
        Tuple of (updated_player_df, num_stubs_created)
    """
    if matchup_df is None or matchup_df.empty:
        return player_df, 0

    if player_df is None:
        player_df = pd.DataFrame()

    # Build case-insensitive override lookup
    override_lookup = {}
    if user_overrides:
        override_lookup = {k.lower(): v for k, v in user_overrides.items()}

    # Get managers from matchup (excluding None/empty)
    matchup_managers = set()
    if "manager" in matchup_df.columns:
        matchup_managers = set(
            m
            for m in matchup_df["manager"].dropna().unique()
            if str(m).strip() and str(m).lower() not in UNROSTERED_VALUES
        )

    # Get managers from player_fantasy (excluding None/empty/unrostered)
    player_managers = set()
    if not player_df.empty and "manager" in player_df.columns:
        player_managers = set(
            m
            for m in player_df["manager"].dropna().unique()
            if str(m).strip() and str(m).lower() not in UNROSTERED_VALUES
        )

    # Find missing managers
    missing_managers = matchup_managers - player_managers

    if not missing_managers:
        return player_df, 0

    print(f"\n[STUB CREATION] Found {len(missing_managers)} manager(s) in matchup but not player_fantasy:")
    for m in sorted(missing_managers):
        print(f"    - {m}")

    # Create stub rows for each missing manager
    # Get (year, week) combinations from matchup for each missing manager
    stub_rows = []

    for manager in missing_managers:
        manager_matchups = matchup_df[matchup_df["manager"] == manager]

        for _, matchup_row in manager_matchups.iterrows():
            year = matchup_row.get("year")
            week = matchup_row.get("week")

            if pd.isna(year) or pd.isna(week):
                continue

            # Get additional context from matchup
            team_name = matchup_row.get("team_name", "")
            manager_guid = matchup_row.get("manager_guid", "")
            franchise_id = matchup_row.get("franchise_id", "")
            team_points = matchup_row.get("team_points", 0)
            opponent = matchup_row.get("opponent", "")
            opponent_points = matchup_row.get("opponent_points", 0)

            # Create stub row with available matchup context
            # This ensures the manager appears in player_fantasy
            stub = {
                "year": int(year),
                "week": int(week),
                "manager": manager,
                "team_name": team_name,
                "manager_guid": manager_guid,
                "franchise_id": franchise_id,
                "fantasy_points": 0.0,  # We don't have player-level data
                "points": 0.0,
                "is_rostered": 1,
                "is_started": 0,  # Mark as not started since we don't know lineup
                "player": f"[No roster data - {manager}]",  # Placeholder
                "position": "UNK",
                "fantasy_position": "BN",  # Bench since we don't know
                "team_points": team_points,
                "opponent": opponent,
                "opponent_points": opponent_points,
                "_source": "stub_from_matchup",  # Track origin
            }

            # Add matchup context if available
            for col in ["matchup_name", "win", "loss", "margin", "is_playoffs", "is_consolation"]:
                if col in matchup_row.index and pd.notna(matchup_row[col]):
                    stub[col] = matchup_row[col]

            stub_rows.append(stub)

    if not stub_rows:
        return player_df, 0

    # Create DataFrame from stubs
    stub_df = pd.DataFrame(stub_rows)
    print(f"    Created {len(stub_df)} stub rows for {len(missing_managers)} missing manager(s)")

    # Ensure stub_df has same columns as player_df (fill missing with None)
    if not player_df.empty:
        for col in player_df.columns:
            if col not in stub_df.columns:
                stub_df[col] = None

    # Concat stubs to player_df
    if player_df.empty:
        result_df = stub_df
    else:
        result_df = pd.concat([player_df, stub_df], ignore_index=True, sort=False)

    return result_df, len(stub_rows)


def validate_manager_opponent_consistency(df: pd.DataFrame, table_name: str = "matchup") -> dict:
    """
    Validate that manager and opponent columns are consistent.

    Checks:
    1. Every opponent name should exist as a manager somewhere
    2. Every manager name should exist as an opponent somewhere

    Args:
        df: DataFrame with 'manager' and 'opponent' columns
        table_name: Name of the table for logging

    Returns:
        Dict with validation results:
        - orphan_opponents: opponent names not in manager set
        - orphan_managers: manager names not in opponent set
        - is_valid: True if no orphans found
    """
    result = {"orphan_opponents": set(), "orphan_managers": set(), "is_valid": True, "table_name": table_name}

    if df is None or df.empty:
        return result

    if "manager" not in df.columns or "opponent" not in df.columns:
        return result

    manager_set = set(df["manager"].dropna().unique())
    opponent_set = set(df["opponent"].dropna().unique())

    result["orphan_opponents"] = opponent_set - manager_set
    result["orphan_managers"] = manager_set - opponent_set
    result["is_valid"] = len(result["orphan_opponents"]) == 0 and len(result["orphan_managers"]) == 0

    return result


def print_validation_report(validations: list):
    """Print a validation report for all tables."""
    has_issues = any(not v["is_valid"] for v in validations)

    if not has_issues:
        print("\n[Validation] All manager/opponent names are consistent")
        return

    print("\n" + "=" * 60)
    print("[Validation] Manager/Opponent Consistency Check")
    print("=" * 60)

    for v in validations:
        if not v["is_valid"]:
            print(f"\n  {v['table_name']}:")
            if v["orphan_opponents"]:
                print(f"    Orphan opponents (not in manager column): {v['orphan_opponents']}")
            if v["orphan_managers"]:
                print(f"    Orphan managers (not in opponent column): {v['orphan_managers']}")

    print("\n  These will be automatically resolved by the orphan fix logic.")
    print("=" * 60)


def build_guid_to_name_mapping(dataframes: dict) -> dict:
    """
    Build a mapping from manager_guid to the most recent year's manager name.

    Args:
        dataframes: Dict of table_name -> DataFrame with manager and manager_guid columns

    Returns:
        Dict mapping guid -> most recent manager name

    Note: GUIDs that are '--' or other placeholders are SKIPPED because they
    represent hidden managers who may be different people.
    """
    # Collect all records from all tables using vectorized concat
    all_subsets = []

    for _table_name, df in dataframes.items():
        if df is None or df.empty:
            continue
        if "manager_guid" not in df.columns or "manager" not in df.columns:
            continue
        if "year" not in df.columns:
            continue

        # Get unique (guid, year, manager) combinations - vectorized
        subset = df[["manager_guid", "year", "manager"]].dropna(subset=["manager_guid", "manager"])
        subset = subset.drop_duplicates()
        all_subsets.append(subset)

    if not all_subsets:
        return {}

    # Concat all subsets at once
    records_df = pd.concat(all_subsets, ignore_index=True)

    # Convert guid to string and filter invalid GUIDs vectorized
    records_df["guid"] = records_df["manager_guid"].astype(str).str.strip()
    records_df = records_df[~records_df["guid"].str.lower().isin(HIDDEN_MANAGER_GUID_TOKENS)]
    records_df = records_df[records_df["guid"].notna()]

    if records_df.empty:
        return {}

    # Convert year to int for sorting
    records_df["year"] = records_df["year"].astype(int)
    records_df["manager"] = records_df["manager"].astype(str)

    # Drop duplicates again after concat
    records_df = records_df.drop_duplicates(subset=["guid", "year", "manager"])

    # For each guid, get the manager name from the most recent year - VECTORIZED
    # Sort by year descending, then drop duplicates keeping first (most recent)
    records_df = records_df.sort_values("year", ascending=False)
    most_recent = records_df.drop_duplicates(subset=["guid"], keep="first")

    # Convert to dict
    guid_to_name = dict(zip(most_recent["guid"], most_recent["manager"]))

    return guid_to_name


def update_manager_names(
    df: pd.DataFrame, guid_to_name: dict, table_name: str, user_override_keys: set = None
) -> tuple:
    """
    Update manager names in a DataFrame using the guid mapping.

    Updates:
    - manager column (using guid)
    - opponent column (by name matching, since opponent doesn't have guid)
    - manager_year and manager_week composite keys

    For hidden managers (GUID = '--'), uses team_name as manager name to keep
    them as separate individuals, UNLESS the team_name has a user override.

    Args:
        df: DataFrame with manager and manager_guid columns
        guid_to_name: Dict mapping guid -> unified manager name
        table_name: Name of the table (for logging)
        user_override_keys: Set of team names (lowercase) that have user overrides - skip hidden manager fix for these

    Returns:
        Tuple of (updated_df, num_updates)
    """
    if df is None or df.empty:
        return df, 0

    if "manager_guid" not in df.columns or "manager" not in df.columns:
        return df, 0

    df = df.copy()
    hidden_updates = 0
    user_override_keys = user_override_keys or set()

    # Step 0: Fix hidden managers - use team_name as manager name (VECTORIZED)
    if "team_name" in df.columns:
        hidden_mask = hidden_manager_guid_mask(df["manager_guid"])

        if hidden_mask.any():
            # Get team_names for hidden managers, stripped
            team_names = df.loc[hidden_mask, "team_name"].astype(str).str.strip()
            old_names = df.loc[hidden_mask, "manager"]

            # Build mask for rows that should be updated
            # Skip empty team_names and those with user overrides
            valid_team_name = (team_names != "") & (team_names != "nan") & (team_names != "None")
            team_name_lower = team_names.str.lower()
            has_override = team_name_lower.isin(user_override_keys)
            should_update = valid_team_name & ~has_override & (old_names != team_names)

            # Apply updates vectorized
            update_indices = df.index[hidden_mask][should_update]
            if len(update_indices) > 0:
                df.loc[update_indices, "manager"] = team_names[should_update].values
                hidden_updates = len(update_indices)

                # Recalculate composite keys vectorized
                new_names_no_spaces = team_names[should_update].str.replace(" ", "", regex=False)

                if "manager_year" in df.columns and "year" in df.columns:
                    years = df.loc[update_indices, "year"]
                    valid_years = years.notna()
                    if valid_years.any():
                        df.loc[update_indices[valid_years], "manager_year"] = (
                            new_names_no_spaces[valid_years.values].values
                            + years[valid_years].astype(int).astype(str).values
                        )

                if "manager_week" in df.columns and "cumulative_week" in df.columns:
                    cw = df.loc[update_indices, "cumulative_week"]
                    valid_cw = cw.notna()
                    if valid_cw.any():
                        df.loc[update_indices[valid_cw], "manager_week"] = (
                            new_names_no_spaces[valid_cw.values].values + cw[valid_cw].astype(int).astype(str).values
                        )

                print(f"    Fixed {hidden_updates} hidden manager names using team_name")

            # Build lookup for fixing orphan opponents using merge (VECTORIZED)
            if hidden_updates > 0 and "opponent" in df.columns and "opponent_points" in df.columns:
                has_required_cols = all(c in df.columns for c in ["year", "week", "team_points"])
                if has_required_cols:
                    # Create lookup from hidden managers: (year, week, team_points) -> manager name
                    hidden_df = df.loc[hidden_mask, ["year", "week", "team_points", "manager"]].copy()
                    hidden_df = hidden_df.dropna(subset=["year", "week", "team_points"])
                    hidden_df = hidden_df.drop_duplicates(subset=["year", "week", "team_points"])

                    # Find orphan opponents
                    manager_set = set(df["manager"].dropna().unique())
                    orphan_mask = df["opponent"].notna() & ~df["opponent"].isin(manager_set)

                    if orphan_mask.any():
                        # Merge to find correct names
                        orphan_df = df.loc[orphan_mask, ["year", "week", "opponent_points"]].copy()
                        orphan_df["_idx"] = orphan_df.index

                        merged = orphan_df.merge(
                            hidden_df,
                            left_on=["year", "week", "opponent_points"],
                            right_on=["year", "week", "team_points"],
                            how="inner",
                        )

                        if not merged.empty:
                            # Apply updates
                            df.loc[merged["_idx"].values, "opponent"] = merged["manager"].values
                            print(f"    Fixed {len(merged)} orphan opponent references for hidden managers")

    # Step 0b: Normalize opponent casing to match manager names (VECTORIZED)
    if "opponent" in df.columns and "manager" in df.columns:
        manager_names = df["manager"].dropna().unique()
        manager_case_map = {str(m).lower(): str(m) for m in manager_names}

        # Map opponents using lowercase lookup
        opp_notna = df["opponent"].notna()
        if opp_notna.any():
            opp_lower = df.loc[opp_notna, "opponent"].astype(str).str.lower()
            corrected = opp_lower.map(manager_case_map)

            # Only update where we have a mapping and it's different
            has_correction = corrected.notna()
            original = df.loc[opp_notna, "opponent"].astype(str)
            needs_fix = has_correction & (original != corrected)

            if needs_fix.any():
                update_idx = df.index[opp_notna][needs_fix]
                df.loc[update_idx, "opponent"] = corrected[needs_fix].values
                print(f"    Fixed {needs_fix.sum()} opponent name casing issues")

    # Step 1: Build old_name -> new_name mapping from guid (VECTORIZED)
    # Filter to valid GUIDs
    guid_col = df["manager_guid"].astype(str).str.strip()
    valid_guid_mask = ~guid_col.str.lower().isin(HIDDEN_MANAGER_GUID_TOKENS) & guid_col.notna()

    old_to_new = {}
    if valid_guid_mask.any():
        valid_df = df.loc[valid_guid_mask, ["manager_guid", "manager"]].copy()
        valid_df["guid_str"] = valid_df["manager_guid"].astype(str)

        for guid_str, new_name in guid_to_name.items():
            mask = valid_df["guid_str"] == guid_str
            if mask.any():
                old_names = valid_df.loc[mask, "manager"].unique()
                for old_name in old_names:
                    if old_name != new_name and old_name not in old_to_new:
                        old_to_new[old_name] = new_name

    # Step 2: Update manager column using guid (VECTORIZED)
    updates = hidden_updates
    if guid_to_name and valid_guid_mask.any():
        guid_str_col = guid_col[valid_guid_mask]
        new_manager = guid_str_col.map(guid_to_name)

        # Only update where we have a mapping
        has_mapping = new_manager.notna()
        if has_mapping.any():
            update_indices = df.index[valid_guid_mask][has_mapping]
            old_managers = df.loc[update_indices, "manager"]
            new_managers = new_manager[has_mapping]

            # Only count actual changes
            changed = old_managers.values != new_managers.values
            if changed.any():
                changed_indices = update_indices[changed]
                df.loc[changed_indices, "manager"] = new_managers[changed].values
                updates += changed.sum()

                # Recalculate composite keys vectorized
                new_names_no_spaces = new_managers[changed].str.replace(" ", "", regex=False)

                if "manager_year" in df.columns and "year" in df.columns:
                    years = df.loc[changed_indices, "year"]
                    valid_years = years.notna()
                    if valid_years.any():
                        df.loc[changed_indices[valid_years], "manager_year"] = (
                            new_names_no_spaces[valid_years.values].values
                            + years[valid_years].astype(int).astype(str).values
                        )

                if "manager_week" in df.columns and "cumulative_week" in df.columns:
                    cw = df.loc[changed_indices, "cumulative_week"]
                    valid_cw = cw.notna()
                    if valid_cw.any():
                        df.loc[changed_indices[valid_cw], "manager_week"] = (
                            new_names_no_spaces[valid_cw.values].values + cw[valid_cw].astype(int).astype(str).values
                        )

    # Step 3: Update opponent column using old_to_new mapping (already vectorized)
    if "opponent" in df.columns and old_to_new:
        opponent_updates = 0
        for old_name, new_name in old_to_new.items():
            mask = df["opponent"] == old_name
            count = mask.sum()
            if count > 0:
                opponent_updates += count
                df.loc[mask, "opponent"] = new_name
        if opponent_updates > 0:
            print(f"    Also updated {opponent_updates} opponent references")

    return df, updates


def resolve_hidden_managers(ctx=None, dry_run: bool = False, db_name: str = None, data_dir: str = None) -> dict:
    """
    Resolve hidden manager names across all tables using GUID.

    Args:
        ctx: LeagueContext with paths to data files (legacy)
        dry_run: If True, only show what would be done without making changes
        db_name: MotherDuck database name (preferred over ctx)
        data_dir: Local data directory (used with db_name)

    Returns:
        Dict with statistics about updates made
    """

    stats = {
        "guids_found": 0,
        "tables_updated": {},
        "total_updates": 0,
        "user_overrides_applied": 0,
        "stubs_created": 0,
    }

    # Determine data source — local DuckDB when data_dir provided, MotherDuck fallback
    if db_name:
        conn = get_pipeline_connection(db_name, data_dir, qualified=True)
    elif ctx is not None:
        from multi_league.core.db_utils import get_db_name

        db_name = get_db_name(ctx)
        conn = get_pipeline_connection(db_name, getattr(ctx, "data_directory", None), qualified=True)
    else:
        raise ValueError("Either ctx or db_name must be provided")

    # Define table names to read from MotherDuck
    table_names = ["player_fantasy", "matchup", "draft", "transactions", "schedule"]

    # Load all tables from MotherDuck
    dataframes = {}
    for table_name in table_names:
        # Map internal names for compatibility with existing code
        internal_name = "player" if table_name == "player_fantasy" else table_name
        try:
            df = _read_table(conn, db_name, table_name)
            if df.empty:
                print(f"  {internal_name}: empty or not found (skipping)")
                dataframes[internal_name] = None
                continue
            dataframes[internal_name] = df
            print(f"  Loaded {internal_name}: {len(df):,} rows")
            if "manager_guid" in df.columns:
                non_null = df["manager_guid"].notna().sum()
                print(f"    - manager_guid: {non_null:,} non-null values")
        except Exception as e:
            print(f"  {internal_name}: not found ({e})")
            dataframes[internal_name] = None

    # Run initial validation to report any manager/opponent inconsistencies
    print("\n[Validation] Checking initial manager/opponent consistency...")
    initial_validations = []
    for table_name, df in dataframes.items():
        if df is not None and "opponent" in df.columns:
            v = validate_manager_opponent_consistency(df, table_name)
            initial_validations.append(v)
    print_validation_report(initial_validations)

    # NOTE: Stub creation for hidden managers is done AFTER GUID resolution
    # (see below) so that we check for resolved names like "Shim" instead of
    # original names like "I Like Ceeeereal"

    # STEP 0: Apply user-provided manager_name_overrides from LeagueContext
    # This handles renames configured in the UI (e.g., "Newton's Law" -> "John Smith")
    user_overrides = (
        ctx.manager_name_overrides if hasattr(ctx, "manager_name_overrides") and ctx.manager_name_overrides else {}
    )
    user_override_keys = set()  # Track which team names have user overrides (to prevent later overwrites)
    if user_overrides:
        print(f"\n[User Overrides] Applying {len(user_overrides)} user-configured name mappings...")
        for old_name, new_name in user_overrides.items():
            print(f"  {old_name} -> {new_name}")

        # Build a case-insensitive lookup: lowercase key -> new_name (VECTORIZED)
        override_lookup = {old_name.lower(): new_name for old_name, new_name in user_overrides.items()}
        user_override_keys = set(override_lookup.keys())

        for table_name, df in dataframes.items():
            if df is None:
                continue

            table_override_count = 0

            # Apply to manager column using vectorized map (case-insensitive)
            if "manager" in df.columns:
                mgr_notna = df["manager"].notna()
                if mgr_notna.any():
                    mgr_lower = df.loc[mgr_notna, "manager"].astype(str).str.lower()
                    new_names = mgr_lower.map(override_lookup)
                    has_override = new_names.notna()
                    if has_override.any():
                        original = df.loc[mgr_notna, "manager"].astype(str)
                        changed = has_override & (original != new_names)
                        if changed.any():
                            update_idx = df.index[mgr_notna][changed]
                            df.loc[update_idx, "manager"] = new_names[changed].values
                            table_override_count += changed.sum()

                # Also check team_name column — ESPN overrides are keyed by team_name
                if "team_name" in df.columns:
                    unknown_mask = mgr_notna & df["manager"].astype(str).str.lower().isin(["unknown", ""])
                    if unknown_mask.any():
                        tn_lower = df.loc[unknown_mask, "team_name"].astype(str).str.lower()
                        tn_overrides = tn_lower.map(override_lookup)
                        has_tn = tn_overrides.notna()
                        if has_tn.any():
                            update_idx = df.index[unknown_mask][has_tn]
                            df.loc[update_idx, "manager"] = tn_overrides[has_tn].values
                            table_override_count += has_tn.sum()

            # Apply to opponent column using vectorized map (case-insensitive)
            if "opponent" in df.columns:
                opp_notna = df["opponent"].notna()
                if opp_notna.any():
                    opp_lower = df.loc[opp_notna, "opponent"].astype(str).str.lower()
                    new_names = opp_lower.map(override_lookup)
                    has_override = new_names.notna()
                    if has_override.any():
                        original = df.loc[opp_notna, "opponent"].astype(str)
                        changed = has_override & (original != new_names)
                        if changed.any():
                            update_idx = df.index[opp_notna][changed]
                            df.loc[update_idx, "opponent"] = new_names[changed].values
                            table_override_count += changed.sum()

                # Also check opponent_team_name — ESPN overrides keyed by team_name
                if "opponent_team_name" in df.columns:
                    unknown_opp = opp_notna & df["opponent"].astype(str).str.lower().isin(["unknown", ""])
                    if unknown_opp.any():
                        otn_lower = df.loc[unknown_opp, "opponent_team_name"].astype(str).str.lower()
                        otn_overrides = otn_lower.map(override_lookup)
                        has_otn = otn_overrides.notna()
                        if has_otn.any():
                            update_idx = df.index[unknown_opp][has_otn]
                            df.loc[update_idx, "opponent"] = otn_overrides[has_otn].values
                            table_override_count += has_otn.sum()

            if table_override_count > 0:
                print(f"    {table_name}: applied {table_override_count} user overrides")
                stats["user_overrides_applied"] += table_override_count
                dataframes[table_name] = df

    # Build guid -> name mapping
    print("\n[Building] GUID to name mapping...")
    guid_to_name = build_guid_to_name_mapping(dataframes)
    stats["guids_found"] = len(guid_to_name)
    print(f"  Found {len(guid_to_name)} unique GUIDs with manager names")

    if not guid_to_name:
        print("  No GUIDs found - skipping GUID resolution")
        # Still need to create stubs for missing managers even without GUIDs
        user_overrides_for_stubs = (
            ctx.manager_name_overrides if hasattr(ctx, "manager_name_overrides") and ctx.manager_name_overrides else {}
        )
        if "matchup" in dataframes and dataframes["matchup"] is not None and "player" in dataframes:
            print("\n[Stub Creation] Checking for managers in matchup but not player_fantasy...")
            player_df = dataframes.get("player")
            matchup_df = dataframes["matchup"]

            updated_player_df, stubs_created = create_missing_manager_stubs(
                matchup_df=matchup_df, player_df=player_df, user_overrides=user_overrides_for_stubs
            )

            stats["stubs_created"] = stubs_created
            if stubs_created > 0:
                dataframes["player"] = updated_player_df
                stats["tables_updated"]["player"] = stats["tables_updated"].get("player", 0) + stubs_created
                stats["total_updates"] += stubs_created
                print(f"    Added {stubs_created} stub rows to player_fantasy for hidden managers")
            else:
                print("    No missing managers found - all managers have player data")

        # Save any stub updates or user overrides to MotherDuck
        if stats.get("stubs_created", 0) > 0 or stats.get("user_overrides_applied", 0) > 0:
            print("\n[Saving] Updated tables to MotherDuck (stubs and/or user overrides)...")
            _md_table_map = {
                "player": "player_fantasy",
                "matchup": "matchup",
                "draft": "draft",
                "transactions": "transactions",
                "schedule": "schedule",
            }
            for table_name, df in dataframes.items():
                if df is None:
                    continue
                total_updates = stats["tables_updated"].get(table_name, 0)
                if total_updates > 0:
                    md_name = _md_table_map.get(table_name, table_name)
                    _replace_scoped_table_from_dataframe(conn, db_name, md_name, df)
                    print(f"  {table_name}: Saved {total_updates:,} updates -> {central_table(md_name)}")
        conn.close()
        return stats

    # Show the mapping
    print("\n[Mapping] GUID -> Manager Name (most recent year):")
    for guid, name in sorted(guid_to_name.items(), key=lambda x: x[1]):
        print(f"  {guid[:12]}... -> {name}")

    if dry_run:
        print("\n[DRY RUN] Would update the following tables:")
        for table_name, df in dataframes.items():
            if df is not None and "manager_guid" in df.columns:
                _, potential_updates = update_manager_names(df.copy(), guid_to_name, table_name, user_override_keys)
                print(f"  {table_name}: {potential_updates:,} potential updates")
        return stats

    # Update each table with GUID-based name resolution
    print("\n[Updating] Manager names in each table...")
    for table_name, df in dataframes.items():
        if df is None:
            continue

        updated_df, num_updates = update_manager_names(df, guid_to_name, table_name, user_override_keys)
        stats["tables_updated"][table_name] = num_updates
        stats["total_updates"] += num_updates
        dataframes[table_name] = updated_df  # Keep updated df for canonical name building

    # VALIDATION: Check for multiple team_keys mapping to the same manager in the same year
    # This catches configuration errors where two different teams are accidentally mapped to one name
    if "player" in dataframes and dataframes["player"] is not None:
        player_df = dataframes["player"]
        if "team_key" in player_df.columns and "manager" in player_df.columns and "year" in player_df.columns:
            # Group by (year, manager) and count distinct team_keys
            team_key_counts = player_df.groupby(["year", "manager"])["team_key"].nunique().reset_index()
            team_key_counts.columns = ["year", "manager", "team_key_count"]
            duplicates = team_key_counts[team_key_counts["team_key_count"] > 1]

            if len(duplicates) > 0:
                print("\n" + "=" * 70)
                print("[WARNING] MULTIPLE TEAMS MAPPED TO SAME MANAGER!")
                print("=" * 70)
                print("The following managers have multiple team_keys in the same year.")
                print("This usually means two different teams were mapped to the same name")
                print("via manager_name_overrides. This will cause DOUBLED player data.")
                print()
                for _, row in duplicates.iterrows():
                    # Find the actual team_keys
                    mask = (player_df["year"] == row["year"]) & (player_df["manager"] == row["manager"])
                    team_keys = player_df.loc[mask, "team_key"].dropna().unique()
                    print(f"  {row['manager']} ({int(row['year'])}): {int(row['team_key_count'])} team_keys")
                    for tk in team_keys[:5]:  # Show up to 5
                        print(f"    - {tk}")
                print()
                print("FIX: Review your manager_name_overrides and ensure each team")
                print("     maps to a UNIQUE manager name (or they're truly the same person).")
                print("=" * 70 + "\n")
                stats["duplicate_team_key_warnings"] = len(duplicates)

    # STEP: Create stub rows for hidden managers missing from player_fantasy
    # This runs AFTER GUID resolution so we check for resolved names like "Shim"
    # instead of original names like "I Like Ceeeereal"
    user_overrides_for_stubs = (
        ctx.manager_name_overrides if hasattr(ctx, "manager_name_overrides") and ctx.manager_name_overrides else {}
    )
    if "matchup" in dataframes and dataframes["matchup"] is not None and "player" in dataframes:
        print("\n[Stub Creation] Checking for managers in matchup but not player_fantasy...")
        player_df = dataframes.get("player")
        matchup_df = dataframes["matchup"]

        updated_player_df, stubs_created = create_missing_manager_stubs(
            matchup_df=matchup_df, player_df=player_df, user_overrides=user_overrides_for_stubs
        )

        stats["stubs_created"] = stubs_created
        if stubs_created > 0:
            dataframes["player"] = updated_player_df
            # Mark player table for saving by adding to tables_updated
            stats["tables_updated"]["player"] = stats["tables_updated"].get("player", 0) + stubs_created
            stats["total_updates"] += stubs_created
            print(f"    Added {stubs_created} stub rows to player_fantasy for hidden managers")
        else:
            print("    No missing managers found - all managers have player data")

    # Build canonical manager name list from matchup (has correct casing from team_name)
    # Then apply to ALL tables to ensure consistency
    print("\n[Normalizing] Manager/opponent casing across all tables...")
    canonical_names = {}  # lowercase -> canonical name
    if "matchup" in dataframes and dataframes["matchup"] is not None:
        matchup_df = dataframes["matchup"]
        if "manager" in matchup_df.columns:
            for name in matchup_df["manager"].dropna().unique():
                canonical_names[str(name).lower()] = str(name)
            print(f"  Built canonical name list from matchup: {len(canonical_names)} names")

    # FIX ORPHAN OPPONENT NAMES: Opponent names that don't match any manager name
    # This happens when Yahoo returns different names for the same person:
    # - When they're the manager: team_name fallback gives "Newton's Law"
    # - When they're the opponent: raw nickname gives "Shim"
    # Fix by matching on (year, week, team_points) == (year, week, opponent_points)
    if "matchup" in dataframes and dataframes["matchup"] is not None:
        matchup_df = dataframes["matchup"]
        if "opponent" in matchup_df.columns and "manager" in matchup_df.columns:
            manager_set = set(matchup_df["manager"].dropna().unique())
            opponent_set = set(matchup_df["opponent"].dropna().unique())
            orphan_opponents = opponent_set - manager_set

            if orphan_opponents:
                print(f"\n  Found {len(orphan_opponents)} orphan opponent name(s): {orphan_opponents}")

                # VECTORIZED: Use merge to find orphan mappings instead of nested loops
                # Get rows with orphan opponents
                orphan_mask = matchup_df["opponent"].isin(orphan_opponents)
                orphan_rows = matchup_df.loc[orphan_mask, ["opponent", "year", "week", "opponent_points"]].copy()
                orphan_rows = orphan_rows.dropna(subset=["year", "week", "opponent_points"])

                # Create lookup from all rows: (year, week, team_points) -> manager
                manager_lookup = matchup_df[["year", "week", "team_points", "manager"]].copy()
                manager_lookup = manager_lookup.dropna(subset=["year", "week", "team_points", "manager"])

                # Merge to find correct names
                merged = orphan_rows.merge(
                    manager_lookup,
                    left_on=["year", "week", "opponent_points"],
                    right_on=["year", "week", "team_points"],
                    how="inner",
                )

                # FIX ORPHAN OPPONENTS DIRECTLY (row-by-row based on points/week context)
                # This handles cases like "Michael" which could map to "Michael - Allahu Akbar",
                # "Michael - BONUS", or "Michael - Tig Ole Bitties" depending on the specific matchup.
                # We can't use a single mapping because the same orphan name can refer to different managers.
                orphan_mapping = {}
                if not merged.empty:
                    # Group merged results to check for ambiguous mappings
                    orphan_manager_combos = merged.groupby("opponent")["manager"].unique()

                    for orphan in orphan_opponents:
                        if orphan not in orphan_manager_combos.index:
                            continue

                        possible_managers = orphan_manager_combos[orphan]
                        if len(possible_managers) == 1:
                            # Unambiguous: single mapping works
                            correct_name = possible_managers[0]
                            if correct_name and pd.notna(correct_name) and correct_name != orphan:
                                orphan_mapping[orphan] = correct_name
                                print(f"    Mapping orphan '{orphan}' -> '{correct_name}'")
                        else:
                            # Ambiguous: multiple possible managers share this orphan name
                            # We need to fix row-by-row using (year, week, opponent_points)
                            print(f"    Orphan '{orphan}' is ambiguous ({len(possible_managers)} possible managers)")

                            # IMPORTANT: Filter to only managers that share the orphan prefix
                            # This prevents false matches (e.g., "Michael" shouldn't map to "Kevin"
                            # just because they had the same score in a week)
                            orphan_lower = orphan.lower()
                            valid_managers = [m for m in possible_managers if str(m).lower().startswith(orphan_lower)]

                            if not valid_managers:
                                print(f"      WARNING: No managers match prefix '{orphan}' - skipping")
                                continue

                            print(f"      Valid matches (prefix '{orphan}'): {valid_managers}")
                            print("      Fixing row-by-row using matchup context...")

                            # Fix directly in the dataframe using the merged results
                            orphan_mask = matchup_df["opponent"] == orphan
                            orphan_indices = matchup_df.loc[orphan_mask].index

                            # Create lookup from merged: (year, week, opponent_points) -> manager
                            # ONLY include valid managers (those matching the orphan prefix)
                            orphan_merged = merged[
                                (merged["opponent"] == orphan) & (merged["manager"].isin(valid_managers))
                            ]
                            fix_lookup = {}
                            for _, row in orphan_merged.iterrows():
                                key = (row["year"], row["week"], row["opponent_points"])
                                # Only add if not already in lookup (prefer first match)
                                if key not in fix_lookup:
                                    fix_lookup[key] = row["manager"]

                            fixed_count = 0
                            for idx in orphan_indices:
                                row = matchup_df.loc[idx]
                                key = (row["year"], row["week"], row["opponent_points"])
                                if key in fix_lookup:
                                    correct_name = fix_lookup[key]
                                    if correct_name != orphan:
                                        matchup_df.loc[idx, "opponent"] = correct_name
                                        fixed_count += 1

                            if fixed_count > 0:
                                print(f"      Fixed {fixed_count} rows for '{orphan}'")
                                dataframes["matchup"] = matchup_df  # Update reference

                # Regenerate matchup_key after fixing opponent names
                # This ensures both sides of a matchup have the same key
                try:
                    from modules.matchup_keys import add_matchup_keys

                    matchup_df = add_matchup_keys(matchup_df)
                    dataframes["matchup"] = matchup_df
                    print("  Regenerated matchup_key column after opponent fixes")
                except ImportError:
                    try:
                        from multi_league.transformations.matchup.modules.matchup_keys import add_matchup_keys

                        matchup_df = add_matchup_keys(matchup_df)
                        dataframes["matchup"] = matchup_df
                        print("  Regenerated matchup_key column after opponent fixes")
                    except ImportError:
                        print("  WARNING: Could not regenerate matchup_key - import failed")

                # Apply orphan mappings to canonical_names
                if orphan_mapping:
                    for orphan, correct in orphan_mapping.items():
                        canonical_names[orphan.lower()] = correct
                    print(f"  Added {len(orphan_mapping)} orphan mappings to canonical names")

    # Apply canonical names to all tables (VECTORIZED)
    if canonical_names:
        for table_name, df in dataframes.items():
            if df is None:
                continue

            table_fixes = 0

            # Fix manager column using vectorized map
            if "manager" in df.columns:
                mgr_notna = df["manager"].notna()
                if mgr_notna.any():
                    mgr_lower = df.loc[mgr_notna, "manager"].astype(str).str.lower()
                    canonical = mgr_lower.map(canonical_names)
                    has_canonical = canonical.notna()
                    if has_canonical.any():
                        original = df.loc[mgr_notna, "manager"].astype(str)
                        needs_fix = has_canonical & (original != canonical)
                        if needs_fix.any():
                            update_idx = df.index[mgr_notna][needs_fix]
                            df.loc[update_idx, "manager"] = canonical[needs_fix].values
                            table_fixes += needs_fix.sum()

            # Fix opponent column using vectorized map
            if "opponent" in df.columns:
                opp_notna = df["opponent"].notna()
                if opp_notna.any():
                    opp_lower = df.loc[opp_notna, "opponent"].astype(str).str.lower()
                    canonical = opp_lower.map(canonical_names)
                    has_canonical = canonical.notna()
                    if has_canonical.any():
                        original = df.loc[opp_notna, "opponent"].astype(str)
                        needs_fix = has_canonical & (original != canonical)
                        if needs_fix.any():
                            update_idx = df.index[opp_notna][needs_fix]
                            df.loc[update_idx, "opponent"] = canonical[needs_fix].values
                            table_fixes += needs_fix.sum()

            if table_fixes > 0:
                print(f"    {table_name}: fixed {table_fixes} name casing issues")
                dataframes[table_name] = df
                stats["tables_updated"][table_name] = stats["tables_updated"].get(table_name, 0) + table_fixes
                stats["total_updates"] += table_fixes

    # Run final validation to confirm all issues are resolved
    print("\n[Validation] Checking final manager/opponent consistency...")
    final_validations = []
    for table_name, df in dataframes.items():
        if df is not None and "opponent" in df.columns:
            v = validate_manager_opponent_consistency(df, table_name)
            final_validations.append(v)

    all_valid = all(v["is_valid"] for v in final_validations)
    if all_valid:
        print("  All manager/opponent names are now consistent!")
    else:
        print_validation_report(final_validations)
        print("  WARNING: Some inconsistencies remain - manual review may be needed")

    # Save all updated tables to MotherDuck
    print("\n[Saving] Updated tables to MotherDuck...")
    _md_table_map = {
        "player": "player_fantasy",
        "matchup": "matchup",
        "draft": "draft",
        "transactions": "transactions",
        "schedule": "schedule",
    }
    for table_name, df in dataframes.items():
        if df is None:
            continue

        total_updates = stats["tables_updated"].get(table_name, 0)
        if total_updates > 0:
            md_name = _md_table_map.get(table_name, table_name)
            _replace_scoped_table_from_dataframe(conn, db_name, md_name, df)
            print(f"  {table_name}: Saved {total_updates:,} updates -> {central_table(md_name)}")
        else:
            print(f"  {table_name}: No updates needed")

    conn.close()
    return stats


def main(args):
    """Main entry point."""
    print("\n" + "=" * 80)
    print("RESOLVE HIDDEN MANAGERS BY GUID")
    print("=" * 80 + "\n")

    # Resolve db_name from --db or --context
    _ctx = None
    if args.db:
        _db_name = sanitize_database_name(args.db)
        _data_dir = getattr(args, "data_dir", None)
        print(f"[League] {_db_name} (MotherDuck)")
    elif args.context:
        from core.league_context import LeagueContext

        _ctx = LeagueContext.load(args.context)
        from multi_league.core.db_utils import get_db_name

        _db_name = get_db_name(_ctx)
        _data_dir = str(_ctx.data_directory)
        print(f"[League] {_ctx.league_name} ({_ctx.league_id})")
        print(f"[Dir] Data directory: {_ctx.data_directory}")
    else:
        raise SystemExit("Provide --db or --context")

    if args.dry_run:
        print("\n[DRY RUN] No changes will be made")

    print("\n[Loading] Data from MotherDuck...")
    stats = resolve_hidden_managers(ctx=_ctx, dry_run=args.dry_run, db_name=_db_name, data_dir=_data_dir)

    print("\n" + "=" * 80)
    print("[SUMMARY]")
    print(f"  GUIDs found: {stats['guids_found']}")
    print(f"  Total updates: {stats['total_updates']}")
    if stats.get("stubs_created", 0) > 0:
        print(f"  Hidden manager stubs created: {stats['stubs_created']}")
    for table_name, count in stats.get("tables_updated", {}).items():
        print(f"    - {table_name}: {count:,}")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Resolve hidden manager names using GUID")
    from multi_league.core.import_args import add_import_args

    add_import_args(parser)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()
    main(args)
