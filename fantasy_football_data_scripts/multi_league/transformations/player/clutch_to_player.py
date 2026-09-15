"""
Clutch Score Transformation - Add Clutch Equity to Player Table

This transformation calculates championship equity contribution (clutch scores)
by combining player LAMAR data with matchup championship odds.

MUST RUN AFTER:
- sql_enrichments.calculate_lamar_for_all() (adds manager_lamar to player table)
- playoff_odds_import.py (adds p_champ to matchup table)

What this does:
1. Reads player_fantasy table (with manager_lamar from replacement_level)
2. Reads matchup table (with p_champ from playoff_odds)
3. Calculates weekly starter baselines per position
4. Distributes credit/blame for odds changes proportionally
5. Adds clutch_equity and related columns to player_fantasy table

All computation happens on local DuckDB when --data-dir is provided.
MotherDuck is NEVER touched — the upload step handles that.

Usage:
    python clutch_to_player.py --db my_league --data-dir /path/to/data
    python clutch_to_player.py --db my_league --data-dir /path/to/data --dry-run
"""

import argparse
from datetime import datetime
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

from multi_league.core.sql_utils import execute_scoped


def fill_missing_clutch_equity(player_df: pd.DataFrame) -> pd.DataFrame:
    """Complete the clutch contract with a neutral value when LAMAR is absent.

    A rostered player can legitimately have no calculable LAMAR (most often an
    unmapped IDP record).  That is not evidence of positive or negative clutch;
    leaving it NULL makes an otherwise processed league-season fail the lake
    completeness contract.  Preserve calculated values and use neutral zero
    only for rows the metric could not score.
    """
    result = player_df.copy()
    if "clutch_equity" not in result.columns:
        result["clutch_equity"] = 0.0
    result["clutch_equity"] = pd.to_numeric(result["clutch_equity"], errors="coerce").fillna(0.0)
    return result


def build_clutch_row_keys(player_df: pd.DataFrame) -> pd.Series:
    """Create a stable per-row update key, falling back when canonical IDs are blank."""
    result = pd.Series(pd.NA, index=player_df.index, dtype="string")

    def populated(column: str) -> pd.Series:
        if column not in player_df.columns:
            return pd.Series(False, index=player_df.index)
        return player_df[column].notna() & player_df[column].astype(str).str.strip().ne("")

    def set_key(mask: pd.Series, value: pd.Series) -> None:
        available = mask & result.isna()
        result.loc[available] = value.loc[available].astype("string")

    if "player_week" in player_df.columns:
        set_key(populated("player_week"), "pw:" + player_df["player_week"].astype(str))

    for column in ("NFL_player_id", "sleeper_player_id", "fleaflicker_player_id", "espn_player_id", "yahoo_player_id"):
        if column not in player_df.columns:
            continue
        year = player_df["year"].astype(str) if "year" in player_df.columns else ""
        week = player_df["week"].astype(str) if "week" in player_df.columns else ""
        set_key(
            populated(column),
            column + ":" + player_df[column].astype(str) + ":" + year + ":" + week,
        )

    fallback_columns = ("year", "week", "manager", "position", "player")
    fallback = pd.Series("fallback:", index=player_df.index, dtype="string")
    for column in fallback_columns:
        values = player_df[column].fillna("").astype(str) if column in player_df.columns else ""
        fallback = fallback + ":" + values
    return result.fillna(fallback)


def clutch_row_key_sql(table_alias: str, available_columns: set[str]) -> str:
    """Return the DuckDB expression equivalent to :func:`build_clutch_row_keys`."""
    def present(column: str) -> str:
        return f"NULLIF(TRIM(CAST({table_alias}.\"{column}\" AS VARCHAR)), '')"

    def value(column: str) -> str:
        return f"COALESCE(CAST({table_alias}.\"{column}\" AS VARCHAR), '')" if column in available_columns else "''"

    cases = []
    if "player_week" in available_columns:
        cases.append(f"WHEN {present('player_week')} IS NOT NULL THEN 'pw:' || {value('player_week')}")
    for column in ("NFL_player_id", "sleeper_player_id", "fleaflicker_player_id", "espn_player_id", "yahoo_player_id"):
        if column in available_columns:
            cases.append(
                f"WHEN {present(column)} IS NOT NULL THEN '{column}:' || {value(column)} || ':' || {value('year')} || ':' || {value('week')}"
            )
    fallback = " || ':' || ".join(value(column) for column in ("year", "week", "manager", "position", "player"))
    return "CASE " + " ".join(cases) + f" ELSE 'fallback::' || {fallback} END"
from multi_league.transformations.player.modules.clutch_calculator import calculate_all_clutch_metrics


def detect_lamar_column(player_df: pd.DataFrame) -> str:
    """
    Detect which LAMAR column is available in the player DataFrame.

    Returns the best available column name for LAMAR values.
    """
    # Preference order: manager_lamar (from sql_enrichments.calculate_lamar_for_all) > player_lamar
    lamar_cols = ["manager_lamar", "player_lamar"]

    for col in lamar_cols:
        if col in player_df.columns:
            return col

    # Check for any lamar-like column
    available = [c for c in player_df.columns if "lamar" in c.lower()]
    if available:
        return available[0]

    return None


def main(args=None):
    """Main entry point for clutch calculation."""
    if args is None:
        parser = argparse.ArgumentParser(description="Calculate clutch equity scores for players")
        parser.add_argument("--dry-run", action="store_true", help="Show what would be done without making changes")
        parser.add_argument("--backup", action="store_true", help="Create backup before modifying files")
        parser.add_argument("--target-year", type=int, help="Only recalculate and write this league season")

        # Support both new --db/--data-dir and legacy --context
        from multi_league.core.import_args import add_import_args

        add_import_args(parser)

        args = parser.parse_args()

    print("\n" + "=" * 80)
    print("CLUTCH SCORE CALCULATION")
    print("=" * 80 + "\n")

    # Resolve args to (db_name, data_dir, is_quick)
    from multi_league.core.import_args import resolve_import_args

    db_name, data_dir, is_quick = resolve_import_args(args)
    print(f"[League] {db_name}")
    print(f"[Dir] Data directory: {data_dir}")

    # Dry run check
    if args.dry_run:
        print("\n[DRY RUN] - No changes will be made")
        print(f"   Would read player_fantasy and matchup from local DuckDB ({db_name})")
        return

    # Connect to local DuckDB (never MotherDuck — upload step handles that)
    # Use direct connection (not ATTACH) to avoid file-lock conflicts with
    # the parent playoff_odds_import process.
    import duckdb

    if data_dir:
        local_db_path = Path(data_dir) / f"{db_name}.duckdb"
        if not local_db_path.exists():
            print(f"[ERROR] Local DuckDB not found: {local_db_path}")
            return
        print(f"[Connect] Direct connection to {local_db_path}")
        conn = duckdb.connect(str(local_db_path))
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    else:
        from multi_league.core.db_utils import get_pipeline_connection

        conn = get_pipeline_connection(db_name, qualified=True)

    # Detect local vs centralized: if we connected directly (data_dir provided),
    # we're local even if the table happens to have a db_name column.
    _player_cols = {r[0] for r in conn.execute("DESCRIBE public.player_fantasy").fetchall()}
    _is_local = data_dir is not None
    _db_filter = "" if _is_local else f"WHERE db_name = '{db_name}'"
    print(f"[Mode] {'local DuckDB' if _is_local else '___leagues (centralized)'}")
    target_year = getattr(args, "target_year", None)

    # Table references — use unqualified public.table for local
    player_table = "public.player_fantasy"
    matchup_tbl = "public.matchup"

    # Surgical player read — only columns the clutch calculator needs
    _needed_player = [
        "year",
        "week",
        "manager",
        "position",
        "fantasy_position",
        "manager_lamar",
        "player_lamar",
        "franchise_id",
        "player",
        "player_week",
        "NFL_player_id",
        "yahoo_player_id",
        "sleeper_player_id",
        "espn_player_id",
        "fleaflicker_player_id",
        "is_rostered",
    ]
    _sel_p = [c for c in _needed_player if c in _player_cols]

    print(f"\n[Loading] Player data from {player_table} ({len(_sel_p)} columns)...")
    player_df = conn.execute(f'SELECT {", ".join(_sel_p)} FROM {player_table} {_db_filter}').fetchdf()
    if target_year is not None:
        player_df = player_df.loc[
            pd.to_numeric(player_df["year"], errors="coerce").eq(int(target_year))
        ].copy()

    if player_df.empty:
        print("[ERROR] player_fantasy table is empty")
        conn.close()
        return

    # Surgical matchup read — only need p_champ + join keys
    _needed_m = ["year", "week", "manager", "franchise_id", "p_champ", "is_playoffs"]
    _avail_m = {r[0] for r in conn.execute(f"DESCRIBE {matchup_tbl}").fetchall()}
    _sel_m = [c for c in _needed_m if c in _avail_m]

    print(f"\n[Loading] Matchup data from {matchup_tbl} ({len(_sel_m)} columns)...")
    matchup_df = conn.execute(f'SELECT {", ".join(_sel_m)} FROM {matchup_tbl} {_db_filter}').fetchdf()
    if target_year is not None:
        matchup_df = matchup_df.loc[
            pd.to_numeric(matchup_df["year"], errors="coerce").eq(int(target_year))
        ].copy()

    if matchup_df.empty:
        print("[ERROR] matchup table is empty")
        conn.close()
        return

    # Check for required columns
    required_player_cols = ["year", "week", "position", "manager", "fantasy_position"]
    missing_player = [c for c in required_player_cols if c not in player_df.columns]
    if missing_player:
        raise ValueError(f"[ERROR] Player missing required columns: {missing_player}")

    # Check for LAMAR column
    lamar_col = detect_lamar_column(player_df)
    if not lamar_col:
        raise ValueError(
            "[ERROR] Player file missing LAMAR columns. "
            "Make sure sql_enrichments.calculate_lamar_for_all() has run first!"
        )
    print(f"   [OK] Found LAMAR column: {lamar_col}")

    # Check for p_champ in matchup
    if "p_champ" not in matchup_df.columns:
        print("\n[WARN] p_champ column not found in matchup table")
        print("   Clutch calculation requires playoff_odds_import.py to run first.")
        print("   Skipping clutch calculation - will be calculated on next run after playoff odds.")
        return

    # Check if p_champ has any non-null values
    p_champ_valid = matchup_df["p_champ"].notna().sum()
    if p_champ_valid == 0:
        print("\n[WARN] p_champ column exists but has no values")
        print("   Skipping clutch calculation - playoff odds may not have been calculated yet.")
        return

    print(f"   [OK] Found {p_champ_valid:,} rows with p_champ values")

    # No need to drop clutch columns — surgical SELECT didn't include them

    # Calculate clutch metrics
    print("\n[Calculating] Clutch equity scores...")
    print("   (Credit/blame distributed based on above/below starter baseline)")

    try:
        player_with_clutch = calculate_all_clutch_metrics(
            player_df=player_df,
            matchup_df=matchup_df,
            manager_col="manager",
            odds_col="p_champ",
            lamar_col=lamar_col,
            include_playoffs=True,  # Include playoff clutch performances
        )
        player_with_clutch = fill_missing_clutch_equity(player_with_clutch)

        # Count how many players got clutch values
        clutch_count = player_with_clutch["clutch_equity"].notna().sum()
        total_count = len(player_with_clutch)
        print(f"   [OK] Calculated clutch_equity for {clutch_count:,}/{total_count:,} player rows")

        # Show summary statistics
        started_mask = player_with_clutch["fantasy_position"].notna() & ~player_with_clutch["fantasy_position"].isin(
            ["BN", "IR", "IR+", "TAXI"]
        )
        started_clutch = player_with_clutch.loc[started_mask, "clutch_equity"]

        if started_clutch.notna().any():
            print("\n[Stats] Clutch equity for started players:")
            print(f"   Mean:   {started_clutch.mean():.4f}%")
            print(f"   Std:    {started_clutch.std():.4f}%")
            print(f"   Min:    {started_clutch.min():.4f}%")
            print(f"   Max:    {started_clutch.max():.4f}%")

            # Top clutch performances
            print("\n[Leaders] Top 5 single-week clutch performances:")
            top_clutch = player_with_clutch.loc[started_mask].nlargest(5, "clutch_equity")
            for _, row in top_clutch.iterrows():
                player_name = row.get("player", row.get("player_name", "Unknown"))
                print(f"   {player_name}: +{row['clutch_equity']:.2f}% (Week {row['week']}, {row['year']})")

    except Exception as e:
        print(f"\n[ERROR] Clutch calculation failed: {e}")
        import traceback

        traceback.print_exc()
        return

    # Clean up: if we created manager_lamar as a copy of manager_lamar, remove the duplicate
    # But keep manager_lamar if it has different values (e.g., after baseline calculations)
    if "manager_lamar" in player_with_clutch.columns and "manager_lamar" in player_with_clutch.columns:
        # Only drop if they're identical (no transformation applied)
        try:
            if player_with_clutch["manager_lamar"].equals(player_with_clutch["manager_lamar"]):
                player_with_clutch = player_with_clutch.drop(columns=["manager_lamar"])
                print("   [INFO] Removed duplicate manager_lamar column")
        except Exception:
            pass  # Keep both if comparison fails

    # Ensure clutch columns are proper float64 dtype (not object/string)
    clutch_numeric_cols = ["clutch_equity", "starter_baseline_lamar", "above_baseline", "below_baseline"]
    for col in clutch_numeric_cols:
        if col in player_with_clutch.columns:
            player_with_clutch[col] = pd.to_numeric(player_with_clutch[col], errors="coerce").astype("float64")

    # Surgical write — UPDATE only clutch columns (no DROP TABLE)
    _clutch_output = [
        "clutch_equity",
        "starter_baseline_lamar",
        "above_baseline",
        "below_baseline",
        "odds_delta",
        "team_above_baseline",
        "team_below_baseline",
    ]
    _update_cols = [c for c in _clutch_output if c in player_with_clutch.columns]

    # Use a per-row stable key.  A single global key fails mixed historical
    # datasets where mapped rows have an NFL id but a small unmapped subset
    # needs its platform-native id or a human-readable fallback.
    _update_df = player_with_clutch[_update_cols].copy()
    _update_df["__clutch_row_key"] = build_clutch_row_keys(player_with_clutch)
    for col in _update_cols:
        _update_df[col] = pd.to_numeric(_update_df[col], errors="coerce").astype("float64")

    # Ensure columns exist in target table
    for col in _update_cols:
        if col not in _player_cols:
            conn.execute(f'ALTER TABLE {player_table} ADD COLUMN IF NOT EXISTS "{col}" DOUBLE')

    print(f"\n[Saving] Updating {len(_update_cols)} clutch columns on {player_table}...")
    conn.register("_clutch_update", _update_df)
    _set = ", ".join(f'"{c}" = u."{c}"' for c in _update_cols)
    _join = f"{clutch_row_key_sql('t', _player_cols)} = u.\"__clutch_row_key\""
    _db_and = "" if _is_local else f"\n          AND t.db_name = '{db_name}'"
    update_sql = f"""
        UPDATE {player_table} t
        SET {_set}
        FROM _clutch_update u
        WHERE {_join}{_db_and}
    """
    if _is_local:
        conn.execute(update_sql)
    else:
        execute_scoped(conn, update_sql, db_name, label="clutch_to_player:update")
    conn.unregister("_clutch_update")
    conn.close()
    print(f"  [OK] Updated {len(_update_cols)} clutch columns on {len(_update_df)} rows")

    print("\n" + "=" * 80)
    print("[SUCCESS] CLUTCH SCORE CALCULATION COMPLETE")
    print("=" * 80 + "\n")


if __name__ == "__main__":
    main()
