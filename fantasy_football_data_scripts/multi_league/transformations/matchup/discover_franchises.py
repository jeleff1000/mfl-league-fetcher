"""
Discover Franchises Transformation

Runs early in the transformation pipeline to establish franchise_id for each team.
Must run BEFORE sql_matchup_enrichments.py so career stats are grouped correctly.

This transformation:
1. Loads existing franchise_config.json (preserves manual edits)
2. Scans matchup + draft data for all team instances
3. Creates/updates franchises with disambiguation when needed
4. Adds franchise_id and franchise_name columns to matchup table
5. Saves updated franchise_config.json

Usage:
    python discover_franchises.py --context path/to/league_context.json
    python discover_franchises.py --context path/to/league_context.json --dry-run
"""

import argparse
from pathlib import Path

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

from core.franchise_registry import FranchiseRegistry
from core.manager_identity import hidden_manager_guid_mask, hidden_manager_owner_id, is_hidden_manager_guid
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


def _write_table(conn, db_name: str, table_name: str, df: pd.DataFrame):
    """Write a DataFrame using a scoped delete + insert."""
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
        insert_sql = ", ".join(f'"{col}"' for col in upload.columns)
        execute_scoped(
            conn,
            f"INSERT INTO {table_sql} ({quoted_insert_cols}) "
            f"SELECT {insert_sql}, '{db_name}' AS db_name FROM _upload",
            db_name,
            label=f"{table_name}:insert",
        )
    finally:
        conn.unregister("_upload")


def _apply_side_franchise_columns(
    df: pd.DataFrame,
    registry: FranchiseRegistry,
    *,
    id_col: str,
    guid_col: str,
    manager_col: str,
    team_col: str,
) -> pd.DataFrame:
    """Resolve non-primary manager identity columns through the franchise registry."""
    if df is None or df.empty or id_col not in df.columns:
        return df
    if not any(col in df.columns for col in (guid_col, manager_col, team_col)):
        return df

    out = df.copy()

    def _lookup(row):
        manager_name = row.get(manager_col)
        if (manager_name is None or pd.isna(manager_name) or str(manager_name).strip() == "") and team_col in row:
            manager_name = row.get(team_col)
        return registry.get_franchise_id(
            manager_guid=row.get(guid_col),
            team_name=row.get(team_col),
            year=row.get("year"),
            manager_name=manager_name,
        )

    resolved = out.apply(_lookup, axis=1)
    has_resolved = resolved.notna() & (resolved.astype(str).str.strip() != "")
    if has_resolved.any():
        out.loc[has_resolved, id_col] = resolved.loc[has_resolved]
        if manager_col in out.columns:
            names = resolved.loc[has_resolved].map(lambda fid: registry.get_franchise_name(fid))
            has_name = names.notna() & (names.astype(str).str.strip() != "")
            if has_name.any():
                out.loc[names.index[has_name], manager_col] = names.loc[has_name]

    return out


def _apply_transaction_party_franchise_columns(df: pd.DataFrame, registry: FranchiseRegistry) -> pd.DataFrame:
    """Resolve transaction source/destination franchise ids through the matchup-built registry."""
    out = _apply_side_franchise_columns(
        df,
        registry,
        id_col="source_franchise_id",
        guid_col="source_manager_guid",
        manager_col="source_manager",
        team_col="source_team_name",
    )
    out = _apply_side_franchise_columns(
        out,
        registry,
        id_col="destination_franchise_id",
        guid_col="destination_manager_guid",
        manager_col="destination_manager",
        team_col="destination_team_name",
    )
    return out


def _apply_schedule_opponent_franchise_columns(df: pd.DataFrame, registry: FranchiseRegistry) -> pd.DataFrame:
    """Resolve schedule opponent franchise ids through the same registry as matchup rows."""
    return _apply_side_franchise_columns(
        df,
        registry,
        id_col="opponent_franchise_id",
        guid_col="opponent_guid",
        manager_col="opponent",
        team_col="opponent_team_name",
    )


def _clean_manager_merge_value(value) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    if text.lower() in {"", "none", "nan", "n/a"}:
        return ""
    return text


def _safe_year_value(value) -> int | None:
    try:
        if value is None or pd.isna(value):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


def _hidden_owner_id_for_manager(manager_name: str) -> str:
    return hidden_manager_owner_id(manager_name)


def _derive_franchise_merges_from_name_overrides(
    matchup_df: pd.DataFrame, manager_name_overrides: dict
) -> list[dict[str, object]]:
    """
    Backfill stable owner-ID merges from older name-only manager overrides.

    Manager overrides saved before franchise_merges_json existed can still tell us
    intent: rows whose raw names collapse to the same override target should be one
    franchise, as long as those owner IDs do not overlap in the same season.
    """
    required_cols = {"manager", "manager_guid", "year"}
    if matchup_df is None or matchup_df.empty or not manager_name_overrides or not required_cols.issubset(matchup_df):
        return []

    override_lookup = {
        str(raw).strip().lower(): str(target).strip()
        for raw, target in manager_name_overrides.items()
        if str(raw).strip() and str(target).strip()
    }
    target_names = {target for target in override_lookup.values() if target}
    if not override_lookup or not target_names:
        return []

    hidden_disambig_names: set[str] = set()
    if "team_name" in matchup_df.columns:
        hidden_rows = matchup_df[hidden_manager_guid_mask(matchup_df["manager_guid"])]
        for manager_name, manager_rows in hidden_rows.groupby("manager"):
            clean_manager = _clean_manager_merge_value(manager_name)
            if not clean_manager:
                continue
            for year in manager_rows["year"].dropna().unique():
                year_rows = manager_rows[manager_rows["year"] == year]
                team_names = {
                    _clean_manager_merge_value(team_name)
                    for team_name in year_rows["team_name"].dropna().unique()
                    if _clean_manager_merge_value(team_name)
                }
                if len(team_names) > 1:
                    hidden_disambig_names.add(clean_manager)
                    break

    grouped: dict[str, dict[str, dict[str, object]]] = {}
    select_cols = ["manager", "manager_guid", "year"]
    if "team_name" in matchup_df.columns:
        select_cols.append("team_name")
    for _, row in matchup_df[select_cols].drop_duplicates().iterrows():
        raw_manager = _clean_manager_merge_value(row.get("manager"))
        owner_id = _clean_manager_merge_value(row.get("manager_guid"))
        year = _safe_year_value(row.get("year"))
        if not raw_manager or year is None:
            continue
        if not owner_id or is_hidden_manager_guid(owner_id):
            if raw_manager in hidden_disambig_names:
                continue
            owner_id = _hidden_owner_id_for_manager(raw_manager)

        resolved_manager = override_lookup.get(raw_manager.lower(), raw_manager)
        if resolved_manager not in target_names:
            continue

        display_bucket = grouped.setdefault(resolved_manager, {})
        owner_bucket = display_bucket.setdefault(owner_id, {"years": set(), "raw_names": set()})
        owner_bucket["years"].add(year)
        owner_bucket["raw_names"].add(raw_manager)

    merges: list[dict[str, object]] = []
    for display_name, owners in sorted(grouped.items()):
        if len(owners) < 2:
            continue

        year_to_owners: dict[int, set[str]] = {}
        for owner_id, info in owners.items():
            for year in info["years"]:
                year_to_owners.setdefault(year, set()).add(owner_id)
        if any(len(owner_ids) > 1 for owner_ids in year_to_owners.values()):
            continue

        canonical_candidates = [
            owner_id
            for owner_id, info in owners.items()
            if any(str(raw).strip().lower() == display_name.lower() for raw in info["raw_names"])
        ]
        owner_latest_year = {owner_id: max(info["years"]) for owner_id, info in owners.items()}
        if canonical_candidates:
            canonical_owner = max(canonical_candidates, key=lambda owner_id: (owner_latest_year[owner_id], owner_id))
        else:
            canonical_owner = max(owner_latest_year, key=lambda owner_id: (owner_latest_year[owner_id], owner_id))

        merged_owner_ids = [canonical_owner]
        merged_owner_ids.extend(
            owner_id
            for owner_id in sorted(
                owners,
                key=lambda candidate: (-owner_latest_year[candidate], candidate),
            )
            if owner_id != canonical_owner
        )
        merges.append({"display_name": display_name, "owner_ids": merged_owner_ids})

    return merges


def _merge_explicit_and_derived_franchise_merges(
    explicit_merges: list[dict], derived_merges: list[dict[str, object]]
) -> list[dict]:
    if not derived_merges:
        return list(explicit_merges or [])

    combined = list(explicit_merges or [])
    explicit_owner_ids = {
        str(owner_id)
        for merge in combined
        for owner_id in (merge.get("owner_ids") or [])
        if _clean_manager_merge_value(owner_id)
    }
    for merge in derived_merges:
        owner_ids = [str(owner_id) for owner_id in (merge.get("owner_ids") or [])]
        if len(owner_ids) < 2:
            continue
        if any(owner_id in explicit_owner_ids for owner_id in owner_ids):
            continue
        combined.append(dict(merge))
        explicit_owner_ids.update(owner_ids)
    return combined


def discover_franchises(
    ctx=None, dry_run: bool = False, quiet: bool = False, db_name: str = None, data_dir: str = None
) -> dict:
    """
    Discover and apply franchise identifiers to league data.

    Args:
        ctx: LeagueContext with paths to data files (legacy)
        dry_run: If True, show what would be done without saving
        quiet: If True, suppress verbose logging
        db_name: MotherDuck database name (preferred over ctx)
        data_dir: Local data directory (used with db_name)

    Returns:
        Dict with statistics about franchises discovered
    """
    stats = {
        "franchises_total": 0,
        "franchises_needing_disambiguation": 0,
        "tables_updated": {},
        "orphan_teams": [],
        "duplicates_removed": 0,
    }

    # Determine data source — local DuckDB when data_dir provided, MotherDuck fallback
    if db_name:
        from multi_league.core.db_utils import get_pipeline_connection

        conn = get_pipeline_connection(db_name, data_dir, qualified=True)
        _data_dir = Path(data_dir) if data_dir else Path(".")
    elif ctx is not None:
        from multi_league.core.db_utils import get_db_name, get_pipeline_connection

        db_name = get_db_name(ctx)
        conn = get_pipeline_connection(db_name, getattr(ctx, "data_directory", None), qualified=True)
        _data_dir = Path(ctx.data_directory)
    else:
        raise ValueError("Either ctx or db_name must be provided")

    config_path = _data_dir / "franchise_config.json"

    # Resolve manager_name_overrides (from ctx if available, else empty)
    _manager_name_overrides = getattr(ctx, "manager_name_overrides", None) or {}
    _franchise_merges = getattr(ctx, "franchise_merges", None) or []

    # Load data from MotherDuck
    if not quiet:
        print("\n[Loading] Data from MotherDuck...")

    matchup_df = _read_table(conn, db_name, "matchup")
    draft_df = _read_table(conn, db_name, "draft")

    if matchup_df.empty:
        print(f"  [WARN] Matchup table empty in {db_name}")
        conn.close()
        return stats

    # Drop stale franchise columns from previous imports so we rebuild them fresh.
    stale_cols = [c for c in ["franchise_id", "franchise_name", "opponent_franchise_id"] if c in matchup_df.columns]
    if stale_cols:
        matchup_df = matchup_df.drop(columns=stale_cols)
    # Normalize year/week columns to integers (handles float years like 2013.0)
    for col in ["year", "week"]:
        if col in matchup_df.columns:
            matchup_df[col] = pd.to_numeric(matchup_df[col], errors="coerce").astype("Int64")
    if not quiet:
        print(f"  Loaded matchup: {len(matchup_df):,} rows")

    if not draft_df.empty:
        # Normalize year column to integer
        if "year" in draft_df.columns:
            draft_df["year"] = pd.to_numeric(draft_df["year"], errors="coerce").astype("Int64")
        if not quiet:
            print(f"  Loaded draft: {len(draft_df):,} rows")
    else:
        draft_df = None
        if not quiet:
            print("  [INFO] Draft table empty (optional)")

    derived_franchise_merges = _derive_franchise_merges_from_name_overrides(matchup_df, _manager_name_overrides)
    _franchise_merges = _merge_explicit_and_derived_franchise_merges(_franchise_merges, derived_franchise_merges)
    if derived_franchise_merges and not quiet:
        print(f"  [INFO] Derived {len(derived_franchise_merges)} owner-ID merge(s) " "from manager_name_overrides")

    # Discover franchises
    if not quiet:
        print("\n[Discovering] Franchises...")

    registry = FranchiseRegistry.from_data(
        matchup_df=matchup_df,
        draft_df=draft_df,
        existing_config=config_path if config_path.exists() else None,
        franchise_merges=_franchise_merges,
    )

    stats["franchises_total"] = len(registry.franchises)

    # Count franchises needing disambiguation
    disambiguated_base_names = set()
    for franchise in registry.franchises.values():
        if " - " in franchise.franchise_name:
            stats["franchises_needing_disambiguation"] += 1
            # Track the base name (before " - ") so we know which names collide
            disambiguated_base_names.add(franchise.owner_name)

    # ── Import-time warnings ──
    # Warn about --hidden-- managers and duplicate display names
    if matchup_df is not None and "manager" in matchup_df.columns:
        # Check for --hidden-- managers (private Yahoo profiles)
        hidden_managers = set()
        if "manager_guid" in matchup_df.columns:
            for _, row in matchup_df[["manager", "manager_guid"]].drop_duplicates().iterrows():
                guid = str(row.get("manager_guid", "") or "")
                if is_hidden_manager_guid(guid) or guid.startswith("hidden_"):
                    hidden_managers.add(row["manager"])
        if hidden_managers:
            print("\n" + "!" * 80)
            print("[WARNING] HIDDEN MANAGERS DETECTED")
            print("!" * 80)
            print("  The following managers have private Yahoo profiles (--hidden--).")
            print("  Their display name defaults to their team name, which may change yearly.")
            for hm in sorted(hidden_managers):
                print(f"    - {hm}")
            overrides = _manager_name_overrides
            if not any(hm.lower() in {k.lower() for k in overrides} for hm in hidden_managers):
                print("  To fix: add 'manager_name_overrides' to your league context, e.g.:")
                print(f'    "manager_name_overrides": {{"{next(iter(hidden_managers))}": "Real Name"}}')
            print("!" * 80)

        # Check for duplicate display names (different people, same name)
        if disambiguated_base_names:
            print("\n" + "!" * 80)
            print("[WARNING] DUPLICATE MANAGER NAMES DETECTED")
            print("!" * 80)
            print("  The following names are shared by multiple managers with different Yahoo accounts:")
            for name in sorted(disambiguated_base_names):
                # List the disambiguated versions
                variants = [
                    f.franchise_name
                    for f in registry.franchises.values()
                    if f.owner_name == name and " - " in f.franchise_name
                ]
                print(f'    - "{name}" → {variants}')
            overrides = _manager_name_overrides
            has_override = any(name.lower() in {k.lower() for k in overrides} for name in disambiguated_base_names)
            if not has_override:
                print("  Since no 'manager_name_overrides' are configured, disambiguated names")
                print("  (with team name suffix) will be used as display names.")
                print("  To customize: add 'manager_name_overrides' to your league context, e.g.:")
                example_name = next(iter(disambiguated_base_names))
                print(
                    f'    "manager_name_overrides": {{"{example_name} - TeamA": "Rob A", "{example_name} - TeamB": "Rob B"}}'
                )
            print("!" * 80)

    # Show summary (consolidated)
    if not quiet:
        summary_df = registry.get_summary_df()
        if not summary_df.empty:
            total_wins = summary_df["career_wins"].sum()
            total_losses = summary_df["career_losses"].sum()
            year_range = f"{summary_df['first_year'].min()}-{summary_df['last_year'].max()}"
            print(f"\n[Summary] {len(summary_df)} franchises ({year_range}), {total_wins}W-{total_losses}L total")

    if dry_run:
        if not quiet:
            print("\n[DRY RUN] Would update the following:")
            print("  - Add franchise_id/franchise_name to matchup table")
            print(f"  - Save franchise_config.json with {len(registry.franchises)} franchises")
        conn.close()
        return stats

    # Apply franchise columns to matchup data
    if not quiet:
        print("\n[Applying] Franchise columns to matchup data...")

    matchup_df = registry.apply_franchise_columns(matchup_df)

    # Safety-net: detect and fix cases where the same (year, week, manager, team_name)
    # got multiple franchise_ids (e.g. from a stale config assigning _1 and _2).
    # IMPORTANT: We group by team_name too, so multi-team owners (same manager name,
    # different team names) correctly keep separate franchise_ids.
    has_team_name = "team_name" in matchup_df.columns
    if "franchise_id" in matchup_df.columns and "manager" in matchup_df.columns and has_team_name:
        group_cols = ["year", "week", "manager", "team_name"]
        multi_fid = matchup_df.groupby(group_cols)["franchise_id"].nunique()
        bad = multi_fid[multi_fid > 1]
        if len(bad) > 0:
            print(f"  [WARN] {len(bad)} (year,week,manager,team_name) combos have multiple franchise_ids - fixing")
            # Keep the franchise_id that appears most often for this (manager, team_name) pair
            fid_counts = matchup_df.groupby(["manager", "team_name", "franchise_id"]).size().reset_index(name="count")
            best_fid = fid_counts.sort_values("count", ascending=False).drop_duplicates(["manager", "team_name"])
            # Build lookup: (manager, team_name) -> best franchise_id
            mgr_team_to_fid = {
                (row["manager"], row["team_name"]): row["franchise_id"] for _, row in best_fid.iterrows()
            }
            # Only overwrite rows where the (manager, team_name) had conflicting franchise_ids
            bad_combos = set(bad.index)  # Set of (year, week, manager, team_name) tuples
            for idx, row in matchup_df.iterrows():
                key = (row["year"], row["week"], row["manager"], row.get("team_name", ""))
                if key in bad_combos:
                    lookup = (row["manager"], row.get("team_name", ""))
                    if lookup in mgr_team_to_fid:
                        matchup_df.at[idx, "franchise_id"] = mgr_team_to_fid[lookup]

    # Verify columns were added
    if "franchise_id" in matchup_df.columns:
        non_null = matchup_df["franchise_id"].notna().sum()
        print(f"  franchise_id: {non_null:,}/{len(matchup_df):,} rows mapped")
        stats["tables_updated"]["matchup"] = non_null

        # Normalize manager column to franchise_name for consistency
        # This handles cases where a user changed their Sleeper display name
        # (e.g., MisterTheSauce -> DavidSausen)
        if "franchise_name" in matchup_df.columns and "manager" in matchup_df.columns:
            # Only update rows where franchise_name is not null
            mask = matchup_df["franchise_name"].notna()
            old_managers = matchup_df.loc[mask, "manager"].nunique()

            # For disambiguated names (e.g., "Rob - When Harry Metcalf Sally"),
            # keep the full franchise_name so two different Robs stay distinct.
            # Only strip the suffix for non-disambiguated names (single owner
            # who just changed their team name across years).
            # If manager_name_overrides exist, apply those instead.
            overrides = _manager_name_overrides
            override_lookup = {k.lower(): v for k, v in overrides.items()}

            def resolve_display_name(franchise_name):
                """Determine the display name for a franchise_name."""
                # Check if there's a user override for this exact franchise_name
                if franchise_name.lower() in override_lookup:
                    return override_lookup[franchise_name.lower()]
                # Check if the base name (without suffix) has an override
                base = franchise_name.split(" - ")[0].strip() if " - " in franchise_name else franchise_name
                if base.lower() in override_lookup:
                    return override_lookup[base.lower()]
                # If this is a disambiguated name (base name collides with another),
                # keep the full franchise_name to prevent conflation
                if base in disambiguated_base_names:
                    return franchise_name
                # Non-disambiguated: strip the team suffix (just a team name change)
                return base

            matchup_df.loc[mask, "manager"] = matchup_df.loc[mask, "franchise_name"].apply(resolve_display_name)

            new_managers = matchup_df.loc[mask, "manager"].nunique()
            if old_managers != new_managers:
                print(
                    f"  [NORMALIZE] Updated manager names to current display names ({old_managers} -> {new_managers} unique)"
                )

            # CRITICAL: Also update opponent column to use franchise_name
            # This ensures playoff flag logic works correctly (it checks if both
            # manager AND opponent are in alive_for_championship set)
            if "opponent" in matchup_df.columns:
                # Find opponent values that don't match any manager (orphan opponents)
                all_managers = set(matchup_df["manager"].unique())
                orphan_opponents = set(matchup_df["opponent"].unique()) - all_managers - {None, "None", ""}

                if orphan_opponents:
                    # Vectorized fix: join with reverse matchups to find correct names
                    # Create lookup table: (year, week, opponent_of_reverse) -> correct_manager_name
                    reverse_lookup = matchup_df[["year", "week", "manager", "opponent"]].copy()
                    reverse_lookup = reverse_lookup.rename(
                        columns={"manager": "correct_opponent_name", "opponent": "our_manager"}
                    )

                    # Merge to find correct opponent names
                    matchup_df = matchup_df.merge(
                        reverse_lookup,
                        left_on=["year", "week", "manager"],
                        right_on=["year", "week", "our_manager"],
                        how="left",
                        suffixes=("", "_lookup"),
                    )

                    # Update opponent where it's an orphan and we found a match
                    orphan_mask = matchup_df["opponent"].isin(orphan_opponents)
                    has_correction = matchup_df["correct_opponent_name"].notna()
                    fix_mask = orphan_mask & has_correction

                    opponent_fixes = fix_mask.sum()
                    if opponent_fixes > 0:
                        matchup_df.loc[fix_mask, "opponent"] = matchup_df.loc[fix_mask, "correct_opponent_name"]
                        print(f"  [NORMALIZE] Fixed {opponent_fixes} opponent names to match franchise names")

                    # Clean up temporary columns
                    matchup_df = matchup_df.drop(columns=["our_manager", "correct_opponent_name"], errors="ignore")

            # Add opponent_franchise_id by score-based self-join.
            # For each row, find the row in the same (year, week) whose
            # team_points == our opponent_points AND opponent_points == our team_points.
            # This is robust against duplicate manager names (e.g. two "Ryan"s).
            if "franchise_id" in matchup_df.columns and "team_points" in matchup_df.columns:
                has_scores = (
                    matchup_df["team_points"].notna()
                    & matchup_df["opponent_points"].notna()
                    & ((matchup_df["team_points"] != 0) | (matchup_df["opponent_points"] != 0))
                )
                lookup = (
                    matchup_df.loc[has_scores, ["year", "week", "team_points", "opponent_points", "franchise_id"]]
                    .copy()
                    .rename(
                        columns={
                            "team_points": "_rev_tp",
                            "opponent_points": "_rev_op",
                            "franchise_id": "_opp_fid",
                        }
                    )
                )
                matchup_df = matchup_df.merge(
                    lookup,
                    left_on=["year", "week", "opponent_points", "team_points"],
                    right_on=["year", "week", "_rev_tp", "_rev_op"],
                    how="left",
                )
                # Only accept matches where the franchise_id differs (not self-match)
                mask = matchup_df["_opp_fid"].notna() & (matchup_df["_opp_fid"] != matchup_df["franchise_id"])
                matchup_df["opponent_franchise_id"] = None
                matchup_df.loc[mask, "opponent_franchise_id"] = matchup_df.loc[mask, "_opp_fid"]
                matchup_df = matchup_df.drop(columns=["_rev_tp", "_rev_op", "_opp_fid"], errors="ignore")

                # Handle rare score collisions: if merge produced duplicates, keep first
                pre = len(matchup_df)
                dedup_key = ["year", "week", "franchise_id"]
                if "opponent" in matchup_df.columns:
                    dedup_key.append("opponent")
                matchup_df = matchup_df.drop_duplicates(subset=dedup_key, keep="first")
                if len(matchup_df) < pre:
                    print(f"  [WARN] Dropped {pre - len(matchup_df)} duplicate rows from score-based opponent join")

                opp_fid_count = matchup_df["opponent_franchise_id"].notna().sum()
                print(f"  opponent_franchise_id: {opp_fid_count:,}/{len(matchup_df):,} rows mapped")
    else:
        print("  [WARN] franchise_id column not added!")

    # Deduplicate matchup data using franchise_id + opponent
    # This handles staging data duplicates that couldn't be deduped earlier (no franchise_id)
    # IMPORTANT: Only deduplicate rows WITH franchise_id - rows with NULL franchise_id
    # should not be treated as duplicates of each other
    # NOTE: We include 'opponent' in the dedup key so multi-team owners (same franchise_id
    # due to edge cases) don't lose rows for different matchups in the same week.
    dedup_cols = ["year", "week", "franchise_id", "opponent"]
    # Fall back to without opponent if column missing
    if "opponent" not in matchup_df.columns:
        dedup_cols = ["year", "week", "franchise_id"]
    if all(col in matchup_df.columns for col in dedup_cols):
        before_dedup = len(matchup_df)

        # Split into rows with and without franchise_id
        has_fid = matchup_df["franchise_id"].notna()
        df_with_fid = matchup_df[has_fid].copy()
        df_without_fid = matchup_df[~has_fid].copy()

        if not df_with_fid.empty:
            # Prefer staging data (data_source='staging') over Yahoo data
            if "data_source" in df_with_fid.columns:
                # Sort so staging comes first, then drop duplicates keeping first
                df_with_fid = df_with_fid.sort_values("data_source", key=lambda x: x != "staging")
            df_with_fid = df_with_fid.drop_duplicates(subset=dedup_cols, keep="first")

        # Recombine - keep all rows without franchise_id (don't treat them as duplicates)
        matchup_df = pd.concat([df_with_fid, df_without_fid], ignore_index=True)

        removed = before_dedup - len(matchup_df)
        if removed > 0:
            print(f"  [DEDUP] Removed {removed:,} duplicate matchup rows (kept staging data)")
            stats["duplicates_removed"] = removed

        # Warn about rows with NULL franchise_id
        null_fid_count = df_without_fid.shape[0]
        if null_fid_count > 0:
            print(f"  [WARN] {null_fid_count:,} rows have NULL franchise_id (not deduplicated)")
    else:
        missing = [c for c in dedup_cols if c not in matchup_df.columns]
        print(f"  [WARN] Cannot deduplicate: missing columns {missing}")

    # Save updated matchup to MotherDuck
    if not quiet:
        print(f"\n[Saving] Updated matchup to {central_table('matchup')}...")
    _write_table(conn, db_name, "matchup", matchup_df)

    # Save franchise config
    if not quiet:
        print(f"\n[Saving] Franchise config to {config_path.name}...")
    registry.save(config_path)

    # Apply to other tables if they exist in MotherDuck
    if not quiet:
        print("\n[Propagating] Franchise columns to other tables...")
    other_table_names = ["draft", "transactions", "schedule", "player_fantasy"]

    for table_name in other_table_names:
        try:
            df = _read_table(conn, db_name, table_name)
            if df.empty:
                if not quiet:
                    print(f"  [SKIP] {table_name}: not found or empty")
                stats["tables_updated"][table_name] = "not_found"
                continue

            total_rows = len(df)

            # Check for columns we can use to join franchise
            has_guid = "manager_guid" in df.columns and df["manager_guid"].notna().any()
            has_manager = "manager" in df.columns and df["manager"].notna().any()

            if not has_guid and not has_manager:
                if not quiet:
                    print(f"  [SKIP] {table_name}: no manager_guid or manager column ({total_rows:,} rows)")
                stats["tables_updated"][table_name] = "no_join_col"
                continue

            # Prefer manager_guid for joining, but fall back to manager name
            if has_guid:
                guid_count = df["manager_guid"].notna().sum()
                if not quiet:
                    print(f"  [APPLY] {table_name}: {total_rows:,} rows, {guid_count:,} with manager_guid")
            else:
                manager_count = df["manager"].notna().sum()
                if not quiet:
                    print(f"  [APPLY] {table_name}: {total_rows:,} rows, {manager_count:,} with manager (no guid)")

            df = registry.apply_franchise_columns(df)
            if table_name == "transactions":
                df = _apply_transaction_party_franchise_columns(df, registry)
            elif table_name == "schedule":
                df = _apply_schedule_opponent_franchise_columns(df, registry)
            _write_table(conn, db_name, table_name, df)

            non_null = df["franchise_id"].notna().sum() if "franchise_id" in df.columns else 0
            stats["tables_updated"][table_name] = non_null
            print(f"  [OK] {table_name}: {non_null:,}/{total_rows:,} rows mapped to franchise_id")

        except Exception as e:
            print(f"  [WARN] Could not update {table_name}: {e}")
            stats["tables_updated"][table_name] = f"error: {e}"

    # Check for orphan teams
    orphans = registry.get_orphan_teams()
    if orphans:
        stats["orphan_teams"] = orphans
        if not quiet:
            print(f"\n[WARN] {len(orphans)} orphan teams need manual linking:")
            for orphan in orphans:
                print(f"  - {orphan}")

    conn.close()
    return stats


def main(args):
    """Main entry point."""
    print("\n" + "=" * 80)
    print("DISCOVER FRANCHISES")
    print("=" * 80)

    # Resolve db_name from --db or --context
    if args.db:
        _db_name = args.db
        _data_dir = getattr(args, "data_dir", None)
        print(f"\n[League] {_db_name} (MotherDuck)")
    elif args.context:
        from core.league_context import LeagueContext

        ctx = LeagueContext.load(args.context)
        from multi_league.core.db_utils import get_db_name

        _db_name = get_db_name(ctx)
        _data_dir = str(ctx.data_directory)
        print(f"\n[League] {ctx.league_name} ({ctx.league_id})")
        print(f"[Dir] {ctx.data_directory}")
    else:
        raise SystemExit("Provide --db or --context")

    if args.dry_run:
        print("\n[DRY RUN] No changes will be made")

    stats = discover_franchises(db_name=_db_name, data_dir=_data_dir, dry_run=args.dry_run)

    print("\n" + "=" * 80)
    print("[SUMMARY]")
    print(f"  Total franchises: {stats['franchises_total']}")
    print(f"  Needing disambiguation: {stats['franchises_needing_disambiguation']}")
    print("\n  Tables with franchise_id:")
    for table, result in stats["tables_updated"].items():
        if isinstance(result, int):
            print(f"    - {table}: {result:,} rows mapped")
        else:
            print(f"    - {table}: {result}")
    if stats["orphan_teams"]:
        print(f"\n  Orphan teams: {len(stats['orphan_teams'])} (need manual linking)")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Discover and apply franchise identifiers")
    from multi_league.core.import_args import add_import_args

    add_import_args(parser)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")

    args = parser.parse_args()
    main(args)
