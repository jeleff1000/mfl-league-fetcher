"""
SQL Aggregations Module

Optimized SQL-based helper functions for playoff simulations.
Replaces slow pandas iterrows and cell-by-cell updates with bulk SQL operations.

This module provides:
- get_points_by_manager_week(): Replaces iterrows for building points dicts
- get_wins_points_to_date(): Replaces match_key groupby pattern
- get_standings_ranked(): SQL-based ranking with tiebreakers
- batch_update_simulation_results(): Bulk update instead of df.at[]

Bracket simulation functions have been moved to bracket_simulation.py but are
re-exported here for backwards compatibility.
"""

import numpy as np
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

# Re-export bracket simulation functions for backwards compatibility
# These have been moved to bracket_simulation.py for better modularity
from multi_league.core.identity import get_manager_col as _get_manager_col

try:
    from .bracket_simulation import (
        simulate_playoff_bracket_vectorized,  # noqa: F401
        _get_bracket_side,  # noqa: F401
        calculate_effective_byes,  # noqa: F401
        apply_completed_round_overrides,  # noqa: F401
        build_playoff_week_mask,  # noqa: F401
    )
except ImportError:
    pass


# ============================================================================
# Points Mapping Functions
# ============================================================================


def get_points_by_manager_week(
    df: pd.DataFrame, year: int, max_week: int, points_col: str = "team_points"
) -> dict[tuple[str, int], float]:
    """
    Build (manager, week) -> points dictionary using vectorized operations.

    Replaces:
        points_perf = {(r['manager'], int(r['week'])): float(r['team_points'])
                      for _, r in df_to_week.iterrows()}

    Args:
        df: DataFrame with manager, week, and points columns
        year: Season year to filter
        max_week: Maximum week to include
        points_col: Column name for points (default: 'team_points')

    Returns:
        Dict mapping (manager, week) to points value
    """
    # Determine ID column: prefer franchise_id for stable lookups
    id_col = _get_manager_col(df)

    # Filter data
    mask = (df["year"] == year) & (df["week"] <= max_week) & (df["is_playoffs"] == 0)
    cols = [id_col, "week", points_col]
    if id_col != "manager":
        cols.append("manager")  # Keep manager for fallback
    filtered = df.loc[mask, cols].copy()

    # Drop rows with missing values
    filtered = filtered.dropna(subset=[id_col, "week", points_col])

    # Convert to proper types
    filtered["week"] = filtered["week"].astype(int)
    filtered[points_col] = filtered[points_col].astype(float)

    # Build dict using vectorized zip (much faster than iterrows)
    return dict(zip(zip(filtered[id_col], filtered["week"]), filtered[points_col]))


def get_points_dicts_for_week(
    df: pd.DataFrame, max_week: int
) -> tuple[dict[tuple[str, int], float], dict[tuple[str, int], float]]:
    """
    Build both team_points and opponent_points dicts in one pass.

    More efficient than calling get_points_by_manager_week twice.

    Args:
        df: DataFrame with manager, week, team_points, opponent_points
        max_week: Maximum week to include

    Returns:
        Tuple of (team_points_dict, opponent_points_dict)
    """
    # Determine ID column: prefer franchise_id for stable lookups
    id_col = _get_manager_col(df)

    # Filter data
    mask = df["week"] <= max_week
    cols = [id_col, "week", "team_points", "opponent_points"]
    if id_col != "manager":
        cols.append("manager")  # Keep manager for fallback
    filtered = df.loc[mask, [c for c in cols if c in df.columns]].copy()

    # Drop rows with missing id/week
    filtered = filtered.dropna(subset=[id_col, "week"])
    filtered["week"] = filtered["week"].astype(int)

    # Build team_points dict
    valid_team = filtered[filtered["team_points"].notna()]
    team_points_dict = dict(zip(zip(valid_team[id_col], valid_team["week"]), valid_team["team_points"].astype(float)))

    # Build opponent_points dict
    if "opponent_points" in filtered.columns:
        valid_opp = filtered[filtered["opponent_points"].notna()]
        opponent_points_dict = dict(
            zip(zip(valid_opp[id_col], valid_opp["week"]), valid_opp["opponent_points"].astype(float))
        )
    else:
        opponent_points_dict = {}

    return team_points_dict, opponent_points_dict


# ============================================================================
# Wins/Points Aggregation Functions
# ============================================================================


def get_wins_points_to_date_fast(df: pd.DataFrame, use_median: bool = False) -> tuple[pd.Series, pd.Series]:
    """
    Calculate cumulative wins and points using vectorized operations.

    Replaces the match_key groupby pattern in wins_points_to_date():
        tmp = add_match_key(played_raw)
        tmp["win_val"] = tmp.groupby("match_key")["team_points"].transform(...)

    Args:
        df: DataFrame with matchup data (manager, opponent, team_points, year, week)
        use_median: If True, include above_league_median wins (H2H+Median scoring)

    Returns:
        wins: Series of cumulative wins per manager
        points: Series of cumulative points per manager
    """
    if df.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    # Filter out bye rows — a manager with no opponent isn't an H2H matchup,
    # and the NaN cells they produce in opponent / opponent_franchise_id break
    # both row-wise min/max (str vs float) and downstream mgr2idx lookups
    # that expect the identity column to be consistently non-null. Bye rows
    # are the root source of the partial-franchise_id shape in format-changing
    # ESPN leagues like the_pigskin_platoon; filtering them here keeps the
    # identity column decision downstream stable and unambiguous.
    if "opponent" in df.columns:
        df = df[df["opponent"].notna() & (df["opponent"].astype(str).str.strip() != "")]
    if df.empty:
        return pd.Series(dtype=float), pd.Series(dtype=float)

    # Use franchise_id when available for stable identity tracking. After the
    # bye filter above, franchise_id / opponent_franchise_id are fully
    # populated on every remaining row in practice, so the original loose
    # presence check is safe again. Do NOT reset _id_col to "manager" on
    # fallback — callers (_vectorized_regular_and_bracket) build their own
    # mgr2idx using get_manager_col(df) and silently break with KeyError if
    # the identity column here disagrees with theirs.
    _id_col = _get_manager_col(df)
    _opp_col = (
        "opponent_franchise_id"
        if "opponent_franchise_id" in df.columns and df["opponent_franchise_id"].notna().any()
        else "opponent"
    )

    # Create a clean copy
    copy_cols = [_id_col, _opp_col, "team_points", "year", "week"]
    if _id_col != "manager":
        copy_cols.append("manager")
    if _opp_col != "opponent":
        copy_cols.append("opponent")
    tmp = df[copy_cols].copy()

    # Create canonical match key (sorted team pair). Coerce both id columns
    # to string before min/max as defense in depth — the bye filter above
    # should already have removed the sources of NaN in id columns, but
    # pandas mixed-dtype .min(axis=1) fails loudly on any residual str/float
    # mix so we force uniform string dtype regardless.
    _a_str = tmp[_id_col].fillna("").astype(str)
    _b_str = tmp[_opp_col].fillna("").astype(str)
    _pair = pd.concat([_a_str.rename("a"), _b_str.rename("b")], axis=1)
    tmp["mA"] = _pair.min(axis=1)
    tmp["mB"] = _pair.max(axis=1)
    tmp["match_key"] = list(zip(tmp["year"], tmp["week"], tmp["mA"], tmp["mB"]))

    # Calculate win values by comparing points within each matchup
    # This is vectorized using merge instead of groupby transform
    matchup_points = (
        tmp.groupby("match_key")
        .agg(
            pts_a=("team_points", "first"),
            pts_b=("team_points", "last"),
            mgr_a=(_id_col, "first"),
            mgr_b=(_id_col, "last"),
        )
        .reset_index()
    )

    # Determine wins (1.0 for win, 0.0 for loss, 0.5 for tie)
    matchup_points["win_a"] = np.where(
        matchup_points["pts_a"] > matchup_points["pts_b"],
        1.0,
        np.where(matchup_points["pts_a"] < matchup_points["pts_b"], 0.0, 0.5),
    )
    matchup_points["win_b"] = 1.0 - matchup_points["win_a"]
    matchup_points.loc[matchup_points["pts_a"] == matchup_points["pts_b"], "win_b"] = 0.5

    # Aggregate wins and points per manager
    wins_a = matchup_points.groupby("mgr_a")["win_a"].sum()
    wins_b = matchup_points.groupby("mgr_b")["win_b"].sum()
    pts_a = matchup_points.groupby("mgr_a")["pts_a"].sum()
    pts_b = matchup_points.groupby("mgr_b")["pts_b"].sum()

    # Combine wins from both perspectives
    wins = wins_a.add(wins_b, fill_value=0.0)
    points = pts_a.add(pts_b, fill_value=0.0)

    # Add median wins if H2H+Median scoring is enabled. Reuse the _id_col
    # chosen above so the grouping key matches the `wins` Series index — a
    # re-fetch via _get_manager_col(df) would return "franchise_id" even when
    # the guard above fell back to "manager" for NaN-safety, producing a
    # mismatched-index add that silently drops the median contribution.
    if use_median and "above_league_median" in df.columns:
        median_wins = pd.to_numeric(df["above_league_median"], errors="coerce").fillna(0)
        median_by_mgr = df.groupby(_id_col).apply(lambda g: median_wins.loc[g.index].sum(), include_groups=False)
        wins = wins.add(median_by_mgr, fill_value=0.0)

    return wins.astype(float), points.astype(float)


def get_cumulative_standings(df: pd.DataFrame, year: int, max_week: int, use_median: bool = False) -> pd.DataFrame:
    """
    Get cumulative standings (wins, points) for each manager up to a given week.

    Uses SQL-style aggregation instead of Python loops.

    Args:
        df: Full matchup DataFrame
        year: Season year
        max_week: Maximum week to include
        use_median: If True, include median wins

    Returns:
        DataFrame with columns: manager, wins, points
    """
    # Filter to regular season games up to max_week
    mask = (df["year"] == year) & (df["week"] <= max_week) & (df["is_playoffs"] == 0)
    if "is_consolation" in df.columns:
        mask = mask & (df["is_consolation"] == 0)

    played = df[mask].copy()

    if played.empty:
        return pd.DataFrame(columns=["manager", "wins", "points"])

    wins, points = get_wins_points_to_date_fast(played, use_median=use_median)

    # wins/points are keyed by _id_col (franchise_id or manager)
    _id_col = _get_manager_col(played)
    result = pd.DataFrame({"manager": wins.index, "wins": wins.values, "points": points.reindex(wins.index).values})
    # If franchise_id was used, rename for downstream compatibility
    if _id_col == "franchise_id":
        result = result.rename(columns={"manager": "franchise_id"})
        # Map franchise_id back to manager name for display
        fid_to_mgr = played.drop_duplicates(subset=["franchise_id"])[["franchise_id", "manager"]].set_index(
            "franchise_id"
        )["manager"]
        result["manager"] = result["franchise_id"].map(fid_to_mgr)

    return result


# ============================================================================
# Ranking Functions
# ============================================================================


def get_standings_ranked(
    df: pd.DataFrame, year: int, max_week: int, playoff_slots: int = 6, bye_slots: int = 2, use_median: bool = False
) -> pd.DataFrame:
    """
    Get ranked standings using SQL-style operations.

    Replaces Python sorting with pandas rank functions.

    Args:
        df: Full matchup DataFrame
        year: Season year
        max_week: Maximum week to include
        playoff_slots: Number of playoff spots
        bye_slots: Number of first-round byes
        use_median: If True, include median wins

    Returns:
        DataFrame with columns: seed, manager, W, L, PF, made_playoffs, bye
    """
    standings = get_cumulative_standings(df, year, max_week, use_median=use_median)

    if standings.empty:
        return pd.DataFrame(columns=["seed", "manager", "W", "L", "PF", "made_playoffs", "bye"])

    # Calculate games played
    _id_col = _get_manager_col(df)
    mask = (df["year"] == year) & (df["week"] <= max_week) & (df["is_playoffs"] == 0)
    games_played = df[mask].groupby(_id_col).size()
    map_col = _get_manager_col(standings)
    standings["games"] = standings[map_col].map(games_played).fillna(0).astype(int)
    standings["L"] = standings["games"] - standings["wins"]
    standings["L"] = standings["L"].clip(lower=0)

    # Rank by wins (desc), then points (desc) using pandas
    # This is equivalent to SQL: ORDER BY wins DESC, points DESC
    standings = standings.sort_values(by=["wins", "points"], ascending=[False, False]).reset_index(drop=True)

    standings["seed"] = range(1, len(standings) + 1)
    standings["made_playoffs"] = standings["seed"] <= playoff_slots
    standings["bye"] = standings["seed"] <= bye_slots

    # Rename columns for compatibility
    result = standings.rename(columns={"wins": "W", "points": "PF"})[
        ["seed", "manager", "W", "L", "PF", "made_playoffs", "bye"]
    ]

    return result


# ============================================================================
# Batch Update Functions
# ============================================================================


def build_simulation_results_df(
    managers: list[str],
    year: int,
    week: int,
    win_hists: dict[str, list[int]],
    seed_hists: dict[str, list[int]],
    expected_wins: dict[str, float],
    expected_seed: dict[str, float],
    expected_playoffs: dict[str, float],
    expected_bye: dict[str, float],
    max_wins: int = 30,
    n_teams: int = 10,
    col_prefix: str = "shuffle",
    id_col: str = "manager",
) -> pd.DataFrame:
    """
    Build a DataFrame with all simulation results for batch update.

    Replaces cell-by-cell updates:
        for idx in rows:
            for wv in range(max_wins):
                df.at[idx, f"shuffle_{wv}_win"] = ...

    With batch merge:
        results_df = build_simulation_results_df(...)
        df = df.merge(results_df, on=['manager', 'year', 'week'], how='left')

    Args:
        managers: List of manager names (or franchise_ids when id_col='franchise_id')
        year: Season year
        week: Week number
        win_hists: Dict of manager/franchise_id -> win histogram counts
        seed_hists: Dict of manager/franchise_id -> seed histogram counts
        expected_wins: Dict of manager/franchise_id -> expected wins
        expected_seed: Dict of manager/franchise_id -> expected seed
        expected_playoffs: Dict of manager/franchise_id -> playoff probability
        expected_bye: Dict of manager/franchise_id -> bye probability
        max_wins: Maximum possible wins (for column generation)
        n_teams: Number of teams (for seed columns)
        col_prefix: Column prefix ('shuffle' or 'opp_shuffle')
        id_col: Column name for manager identifier (default: 'manager', can be 'franchise_id')

    Returns:
        DataFrame with simulation result columns
    """
    rows = []

    for mgr in managers:
        row = {
            id_col: mgr,
            "year": year,
            "week": week,
        }

        # Win probabilities
        win_hist = win_hists.get(mgr, [])
        win_den = max(1, sum(win_hist))
        for wv in range(min(len(win_hist), max_wins + 1)):
            row[f"{col_prefix}_{wv}_win"] = round(100.0 * win_hist[wv] / win_den, 2)

        # Seed probabilities
        seed_hist = seed_hists.get(mgr, [])
        seed_den = max(1, sum(seed_hist))
        for s in range(1, min(len(seed_hist) + 1, n_teams + 1)):
            if s - 1 < len(seed_hist):
                row[f"{col_prefix}_{s}_seed"] = round(100.0 * seed_hist[s - 1] / seed_den, 2)

        # Summary stats
        row[f"{col_prefix}_avg_wins"] = round(expected_wins.get(mgr, 0.0), 2)
        row[f"{col_prefix}_avg_seed"] = round(expected_seed.get(mgr, 0.0), 2)
        row[f"{col_prefix}_avg_playoffs"] = round(expected_playoffs.get(mgr, 0.0), 2)
        row[f"{col_prefix}_avg_bye"] = round(expected_bye.get(mgr, 0.0), 2)

        rows.append(row)

    return pd.DataFrame(rows)


def apply_simulation_results(df: pd.DataFrame, results_df: pd.DataFrame, mask: pd.Series = None) -> pd.DataFrame:
    """
    Apply simulation results to main DataFrame using batch update.

    Much faster than cell-by-cell df.at[] updates.

    Args:
        df: Main matchup DataFrame
        results_df: Results from build_simulation_results_df()
        mask: Optional boolean mask for rows to update

    Returns:
        Updated DataFrame
    """
    # Determine ID column from results_df, and refuse to merge manager-keyed
    # results onto franchise-keyed matchup data.
    id_col = next((col for col in ("franchise_id", "manager") if col in results_df.columns), None)
    if id_col is None:
        raise ValueError("results_df must include franchise_id or manager")
    if id_col != "franchise_id" and "franchise_id" in df.columns and df["franchise_id"].notna().any():
        raise ValueError("results_df is manager-keyed but df has franchise_id; refusing identity fallback")

    # Get columns to update (exclude merge keys)
    merge_keys = {"manager", "year", "week", "franchise_id"}
    update_cols = [c for c in results_df.columns if c not in merge_keys]

    # Ensure all columns exist in df
    for col in update_cols:
        if col not in df.columns:
            df[col] = np.nan

    # Create index for matching
    df = df.copy()

    # Build lookup dict from results keyed by the id column present in results_df
    results_dict = {}
    for _, row in results_df.iterrows():
        key = (row[id_col], row["year"], row["week"])
        results_dict[key] = {col: row[col] for col in update_cols if col in row.index}

    # Apply updates using vectorized operations where possible
    if mask is None:
        mask = pd.Series(True, index=df.index)

    # Create composite key for matching using the same identity column as results_df.
    match_col = id_col if id_col in df.columns else None
    if match_col is None:
        raise ValueError(f"df is missing required merge key column: {id_col}")
    df["_key"] = list(zip(df[match_col], df["year"], df["week"]))

    # Update each column
    for col in update_cols:
        col_values = df["_key"].map(lambda k: results_dict.get(k, {}).get(col, np.nan))
        df.loc[mask, col] = col_values.loc[mask]

    # Clean up
    df.drop("_key", axis=1, inplace=True)

    return df


# ============================================================================
# History Snapshots (Window Functions Alternative)
# ============================================================================


def get_history_snapshots_fast(df: pd.DataFrame, playoff_slots: int = 6, use_median: bool = False) -> pd.DataFrame:
    """
    Generate historical snapshots using vectorized cumulative calculations.

    Replaces triple-nested loop in history_snapshots():
        for yr in seasons:
            for w in weeks:
                played = reg[reg["week"] <= w]
                wins_w, pts_w = wins_points_to_date(played)

    Uses cumulative groupby operations instead.

    Args:
        df: All historical matchup data
        playoff_slots: Number of playoff spots
        use_median: If True, include above_league_median wins

    Returns:
        DataFrame with columns: year, week, manager, W, L, PF_pct, made_playoffs, final_seed
    """
    if df.empty:
        return pd.DataFrame(columns=["year", "week", "manager", "W", "L", "PF_pct", "made_playoffs", "final_seed"])

    rows = []

    # Get regular season data
    reg_mask = df["is_playoffs"] == 0
    if "is_consolation" in df.columns:
        reg_mask = reg_mask & (df["is_consolation"] == 0)

    all_reg = df[reg_mask].copy()

    # Determine if franchise_id is available for stable keying
    has_fid = "franchise_id" in df.columns and df["franchise_id"].notna().any()

    for year in sorted(all_reg["year"].dropna().unique().astype(int)):
        year_data = all_reg[all_reg["year"] == year]
        if year_data.empty:
            continue

        weeks = sorted(year_data["week"].dropna().unique().astype(int))
        managers = year_data["franchise_id"].unique()

        # Get final standings for this year
        final_standings = get_standings_ranked(df, year, max(weeks), playoff_slots, 2, use_median)
        if not (has_fid and "franchise_id" in final_standings.columns):
            raise ValueError("franchise_id is required for final seed mapping")
        seed_key_col = "franchise_id"
        final_seed_map = dict(zip(final_standings[seed_key_col], final_standings["seed"]))
        made_playoffs_set = set(final_standings.loc[final_standings["made_playoffs"], seed_key_col])

        # Build franchise_id lookup for this year if available
        fid_seed_map = {}
        mgr_to_fid = {}
        if has_fid:
            yr_fid = year_data[["manager", "franchise_id"]].drop_duplicates()
            mgr_to_fid = dict(zip(yr_fid["manager"], yr_fid["franchise_id"]))
            fid_seed_map = {mgr_to_fid.get(m, m): s for m, s in final_seed_map.items()}

        # Calculate cumulative stats for each week
        for week in weeks:
            standings = get_cumulative_standings(df, year, week, use_median)
            if standings.empty:
                continue

            # Calculate PF percentile
            standings["PF_pct"] = 100.0 * standings["points"].rank(pct=True)

            # Calculate games and losses
            _snap_id_col = _get_manager_col(year_data)
            week_data = year_data[year_data["week"] <= week]
            games = week_data.groupby(_snap_id_col).size()
            snap_map_col = _get_manager_col(standings)
            standings["games"] = standings[snap_map_col].map(games).fillna(0).astype(int)
            standings["L"] = (standings["games"] - standings["wins"]).clip(lower=0)

            for _, row in standings.iterrows():
                mgr = row["manager"]
                snapshot = {
                    "year": year,
                    "week": int(week),
                    "manager": mgr,
                    "W": float(row["wins"]),
                    "L": float(row["L"]),
                    "PF_pct": float(row["PF_pct"]),
                }
                if has_fid:
                    fid = row.get("franchise_id")
                    if pd.isna(fid):
                        fid = mgr_to_fid.get(mgr)
                    snapshot["franchise_id"] = fid
                    snapshot["made_playoffs"] = 1.0 if fid in made_playoffs_set else 0.0
                    snapshot["final_seed"] = fid_seed_map.get(fid, final_seed_map.get(fid, np.nan))
                else:
                    snapshot["made_playoffs"] = 1.0 if mgr in made_playoffs_set else 0.0
                    snapshot["final_seed"] = final_seed_map.get(mgr, np.nan)
                rows.append(snapshot)

    return pd.DataFrame(rows)


# ============================================================================
# Clutch Equity Functions
# ============================================================================


def get_weekly_starter_baseline_fast(player_df: pd.DataFrame, lamar_col: str = "manager_lamar") -> pd.DataFrame:
    """
    Calculate weekly starter baseline LAMAR for each position using vectorized ops.

    Replaces the iterrows-based pattern with efficient groupby aggregation.

    Args:
        player_df: Player DataFrame with year, week, position, fantasy_position, and LAMAR
        lamar_col: Column name for LAMAR values

    Returns:
        DataFrame with columns: year, week, position, starter_baseline_lamar
    """
    df = player_df.copy()

    # Filter to STARTERS ONLY - exclude bench, IR, and unrostered
    if "fantasy_position" in df.columns:
        started_mask = df["fantasy_position"].notna() & ~df["fantasy_position"].isin(["BN", "IR", "IR+", "TAXI"])
        df = df[started_mask]

    if df.empty or "position" not in df.columns:
        return pd.DataFrame(columns=["year", "week", "position", "starter_baseline_lamar"])

    # Ensure LAMAR column exists
    if lamar_col not in df.columns:
        return pd.DataFrame(columns=["year", "week", "position", "starter_baseline_lamar"])

    # Single vectorized groupby - much faster than iterating
    baseline = df.groupby(["year", "week", "position"], as_index=False).agg(starter_baseline_lamar=(lamar_col, "mean"))

    return baseline


def get_weekly_odds_delta_fast(matchup_df: pd.DataFrame, odds_col: str = "p_champ") -> pd.DataFrame:
    """
    Calculate weekly championship odds delta using vectorized window operations.

    IMPORTANT: If p_champ_change column already exists (pre-calculated by playoff_scenarios.py),
    we use it directly. This ensures:
    1. NaN-safe handling for eliminated teams (they get 0 instead of NaN)
    2. Consistency with the odds calculations done in playoff_odds_import.py
    3. Zero-sum property is maintained (all odds changes sum to 0 per week)

    Args:
        matchup_df: Matchup DataFrame with manager, franchise_id, year, week, and odds columns
        odds_col: Column name for championship probability

    Returns:
        DataFrame with columns: manager, franchise_id, year, week, odds_delta, is_win
    """
    # Enforce franchise_id requirement uniformly across both paths.
    manager_col = _get_manager_col(matchup_df)

    output_cols = ["manager", "franchise_id", "year", "week", "odds_delta", "is_win"]

    # Check if pre-calculated change column exists
    change_col = f"{odds_col}_change"
    if change_col in matchup_df.columns:
        # Use the pre-calculated change directly - it's already NaN-safe
        result = matchup_df[["manager", "franchise_id", "year", "week"]].copy()
        # Fill NaN with 0 for eliminated teams (who have NaN p_champ_change)
        result["odds_delta"] = matchup_df[change_col].fillna(0.0)

        # Add win calculation if points available
        if "team_points" in matchup_df.columns and "opponent_points" in matchup_df.columns:
            result["is_win"] = (matchup_df["team_points"] > matchup_df["opponent_points"]).astype(int)
        else:
            result["is_win"] = None

        return result[output_cols]

    # Fallback: Calculate from raw odds (with NaN-safety)
    if odds_col not in matchup_df.columns:
        return pd.DataFrame(columns=output_cols)

    df = matchup_df[["manager", "franchise_id", "year", "week", odds_col]].copy()

    # Add win calculation if points available
    if "team_points" in matchup_df.columns and "opponent_points" in matchup_df.columns:
        df["is_win"] = (matchup_df["team_points"] > matchup_df["opponent_points"]).astype(int)
    else:
        df["is_win"] = None

    # Sort for proper shift
    df = df.sort_values([manager_col, "year", "week"]).reset_index(drop=True)

    # Vectorized shift within groups - pandas handles this efficiently
    df["odds_before"] = df.groupby([manager_col, "year"])[odds_col].shift(1)

    # For first week of each year, use 100/num_teams as baseline
    first_week_mask = df["odds_before"].isna()
    if first_week_mask.any():
        teams_by_year = matchup_df.groupby("year")["franchise_id"].nunique()
        df["_num_teams"] = df["year"].map(teams_by_year)
        df.loc[first_week_mask, "odds_before"] = 100.0 / df.loc[first_week_mask, "_num_teams"]
        df.drop("_num_teams", axis=1, inplace=True)

    # Calculate delta with NaN-safety
    # Fill NaN odds with 0 for eliminated teams
    df["odds_after"] = df[odds_col].fillna(0.0)
    df["odds_delta"] = np.where(
        pd.notna(df["odds_after"]) & pd.notna(df["odds_before"]),
        df["odds_after"] - df["odds_before"],
        0.0,  # Eliminated teams get 0 odds_delta
    )

    return df[output_cols]


def calculate_clutch_equity_fast(
    player_df: pd.DataFrame, matchup_df: pd.DataFrame, lamar_col: str = "manager_lamar", odds_col: str = "p_champ"
) -> pd.DataFrame:
    """
    Calculate clutch equity using optimized SQL-style operations.

    Combines baseline calculation, odds delta, and equity distribution
    in a single efficient pipeline.

    Args:
        player_df: Player DataFrame with LAMAR values
        matchup_df: Matchup DataFrame with championship odds
        lamar_col: Column for LAMAR values
        odds_col: Column for championship odds

    Returns:
        DataFrame with clutch_equity and related columns added
    """
    df = player_df.copy()

    # Step 1: Get baselines in one vectorized operation
    baseline_df = get_weekly_starter_baseline_fast(df, lamar_col=lamar_col)

    # Step 2: Get odds delta in one vectorized operation
    odds_df = get_weekly_odds_delta_fast(matchup_df, odds_col=odds_col)

    # Step 3: Merge baseline to player data (single merge)
    if not baseline_df.empty:
        df = df.merge(baseline_df, on=["year", "week", "position"], how="left")

    if "starter_baseline_lamar" not in df.columns:
        df["starter_baseline_lamar"] = 0.0
    df["starter_baseline_lamar"] = df["starter_baseline_lamar"].fillna(0)

    # Step 4: Calculate above/below baseline (vectorized)
    df["above_baseline"] = (df[lamar_col] - df["starter_baseline_lamar"]).clip(lower=0)
    df["below_baseline"] = (df["starter_baseline_lamar"] - df[lamar_col]).clip(lower=0)

    # Step 5: Merge odds delta (single merge)
    manager_col = _get_manager_col(df)
    if not odds_df.empty:
        merge_cols = [manager_col, "year", "week"]
        odds_merge_cols = merge_cols + ["odds_delta"]
        df = df.merge(odds_df[odds_merge_cols], on=merge_cols, how="left")

    if "odds_delta" not in df.columns:
        df["odds_delta"] = 0.0
    df["odds_delta"] = df["odds_delta"].fillna(0)

    # Step 6: Filter to started players for team totals
    if "fantasy_position" in df.columns:
        started_mask = df["fantasy_position"].notna() & ~df["fantasy_position"].isin(["BN", "IR", "IR+", "TAXI"])
    else:
        started_mask = pd.Series(True, index=df.index)

    # Prefer franchise_id for consistent career tracking
    manager_col = _get_manager_col(df)

    # Step 7: Calculate team totals using single groupby (not per-row)
    team_totals = (
        df[started_mask]
        .groupby([manager_col, "year", "week"], as_index=False)
        .agg(team_above_baseline=("above_baseline", "sum"), team_below_baseline=("below_baseline", "sum"))
    )

    df = df.merge(team_totals, on=[manager_col, "year", "week"], how="left")
    df["team_above_baseline"] = df["team_above_baseline"].fillna(0)
    df["team_below_baseline"] = df["team_below_baseline"].fillna(0)

    # Step 7b: Calculate matchup-level stake for zero-sum clutch equity
    # For clutch to be zero-sum within a matchup, we need to use a SINGLE stake
    # value for both teams (positive for winner, negative for loser).
    #
    # CRITICAL: Use franchise_id as primary key for matching, not manager names.
    # Manager names can change due to disambiguation.
    if "is_win" in odds_df.columns and "opponent" in df.columns:
        # Merge win status to player data
        if "is_win" not in df.columns:
            win_merge_cols = [manager_col, "year", "week"]
            win_merge_src = win_merge_cols + ["is_win"]
            df = df.merge(odds_df[win_merge_src], on=win_merge_cols, how="left")

        # Build matchup stake lookup using franchise_id as primary key
        matchup_stake = {}

        # Build manager_name -> franchise_id lookup for opponent resolution
        name_to_fid = {}
        if "manager" in df.columns:
            for _, row in df[["manager", "franchise_id"]].drop_duplicates().iterrows():
                if pd.notna(row["manager"]) and pd.notna(row["franchise_id"]):
                    name_to_fid[row["manager"]] = row["franchise_id"]

        # Get unique manager-weeks with their odds_delta and win status
        agg_cols = {"odds_delta": "first", "is_win": "first", "opponent": "first"}
        if "manager" in df.columns:
            agg_cols["manager"] = "first"
        # Include opponent_franchise_id if available for more reliable matching
        has_opp_fid = "opponent_franchise_id" in df.columns
        if has_opp_fid:
            agg_cols["opponent_franchise_id"] = "first"
        manager_weeks = df.groupby([manager_col, "year", "week"]).agg(agg_cols).reset_index()

        if manager_weeks is not None and not manager_weeks.empty:
            for _, row in manager_weeks.iterrows():
                mgr_key = row[manager_col]
                yr = row["year"]
                wk = row["week"]
                opp_name = row["opponent"]
                is_win = row["is_win"]
                delta = row["odds_delta"]

                if pd.isna(opp_name) or pd.isna(is_win):
                    continue

                # Resolve opponent to franchise_id - prefer opponent_franchise_id if available
                if has_opp_fid and pd.notna(row.get("opponent_franchise_id")):
                    opp_key = row["opponent_franchise_id"]
                else:
                    opp_key = name_to_fid.get(opp_name, opp_name)

                # For zero-sum clutch, we assign stakes based on matchup outcome
                # Winner gets positive stake, loser gets negative stake
                # The magnitude is based on the larger absolute odds change between the two teams
                if is_win == 1:
                    # Winner - set positive stake if not already set
                    if (yr, wk, mgr_key) not in matchup_stake:
                        # Use abs of delta as stake magnitude (could be negative if other results hurt winner)
                        stake = max(abs(delta), 0.001)  # Ensure non-zero stake
                        matchup_stake[(yr, wk, mgr_key)] = stake
                        matchup_stake[(yr, wk, opp_key)] = -stake
                elif is_win == 0:
                    # Loser - set negative stake if not already set by winner
                    if (yr, wk, mgr_key) not in matchup_stake:
                        stake = max(abs(delta), 0.001)  # Ensure non-zero stake
                        matchup_stake[(yr, wk, mgr_key)] = -stake
                        matchup_stake[(yr, wk, opp_key)] = stake

        def get_matchup_stake(row):
            key = (row["year"], row["week"], row[manager_col])
            # Bye weeks (no opponent) should have 0 stake - they didn't play
            if pd.isna(row.get("opponent")):
                return 0.0
            return matchup_stake.get(key, row["odds_delta"])

        df["matchup_stake"] = df.apply(get_matchup_stake, axis=1)
    else:
        df["matchup_stake"] = df["odds_delta"]

    # Step 8: Calculate clutch equity using matchup_stake (zero-sum per matchup)
    df["clutch_equity"] = 0.0

    # WINNERS (matchup_stake > 0): credit above-baseline players
    stake_positive = started_mask & (df["matchup_stake"] > 0)
    has_above = df["team_above_baseline"] > 0

    df.loc[stake_positive & has_above, "clutch_equity"] = (
        df.loc[stake_positive & has_above, "matchup_stake"]
        * df.loc[stake_positive & has_above, "above_baseline"]
        / df.loc[stake_positive & has_above, "team_above_baseline"]
    )

    # EDGE CASE: Winner with no one above baseline - distribute equally
    no_above = df["team_above_baseline"] == 0
    if (stake_positive & no_above).any():
        team_week_counts = df[started_mask].groupby([manager_col, "year", "week"]).size()
        for idx in df[stake_positive & no_above].index:
            row = df.loc[idx]
            key = (row[manager_col], row["year"], row["week"])
            starter_count = team_week_counts.get(key, 1)
            df.loc[idx, "clutch_equity"] = row["matchup_stake"] / starter_count

    # LOSERS (matchup_stake < 0): distribute negative stake
    # CRITICAL FIX: Losers ALWAYS get negative clutch equity to maintain zero-sum
    stake_negative = started_mask & (df["matchup_stake"] < 0)
    has_below = df["team_below_baseline"] > 0

    # Case 1: Loser has below-baseline players - distribute blame proportionally
    df.loc[stake_negative & has_below, "clutch_equity"] = (
        df.loc[stake_negative & has_below, "matchup_stake"]
        * df.loc[stake_negative & has_below, "below_baseline"]
        / df.loc[stake_negative & has_below, "team_below_baseline"]
    )

    # Case 2: Loser has NO below-baseline players (everyone performed well but lost)
    # CRITICAL: These players STILL get negative clutch equity (team's bad luck)
    no_below = df["team_below_baseline"] == 0
    if (stake_negative & no_below).any():
        if "team_week_counts" not in dir():
            team_week_counts = df[started_mask].groupby([manager_col, "year", "week"]).size()
        for idx in df[stake_negative & no_below].index:
            row = df.loc[idx]
            key = (row[manager_col], row["year"], row["week"])
            starter_count = team_week_counts.get(key, 1)
            df.loc[idx, "clutch_equity"] = row["matchup_stake"] / starter_count

    # Weekly zero-sum correction
    # Managers without started players (eliminated in playoffs, bye weeks) may have
    # p_champ_change that isn't distributed, breaking league-wide zero-sum.
    # Redistribute the residual equally across all starters each week.
    weekly_sums = df.loc[started_mask].groupby(["year", "week"])["clutch_equity"].transform("sum")
    starter_counts = df.loc[started_mask].groupby(["year", "week"])["clutch_equity"].transform("count")
    needs_correction = started_mask & (weekly_sums.abs() > 0.001)
    if needs_correction.any():
        df.loc[needs_correction, "clutch_equity"] -= weekly_sums[needs_correction] / starter_counts[needs_correction]

    # Clean up
    if "matchup_stake" in df.columns:
        df = df.drop(columns=["matchup_stake"])

    return df


def aggregate_season_clutch_fast(player_df: pd.DataFrame, id_col: str = "yahoo_player_id") -> pd.DataFrame:
    """
    Aggregate clutch scores to season level using single groupby.

    Much faster than iterating through players.

    Args:
        player_df: Player DataFrame with clutch_equity calculated
        id_col: Column for player identifier

    Returns:
        DataFrame with season-level clutch metrics
    """
    df = player_df.copy()

    # Filter to started weeks
    if "fantasy_position" in df.columns:
        started_mask = df["fantasy_position"].notna() & ~df["fantasy_position"].isin(["BN", "IR", "IR+", "TAXI"])
        df = df[started_mask]

    if "clutch_equity" not in df.columns:
        return pd.DataFrame()

    # Pre-compute helper columns vectorized
    df["positive_clutch"] = df["clutch_equity"].clip(lower=0)
    df["negative_clutch"] = df["clutch_equity"].clip(upper=0)
    df["was_above"] = (df["above_baseline"] > 0).astype(int) if "above_baseline" in df.columns else 0
    df["was_below"] = (df["below_baseline"] > 0).astype(int) if "below_baseline" in df.columns else 0

    # Single groupby aggregation
    group_cols = [id_col, "year"] if id_col in df.columns else ["year"]

    agg_cols = {
        "clutch_equity": "sum",
        "positive_clutch": "sum",
        "negative_clutch": "sum",
        "was_above": "sum",
        "was_below": "sum",
        "week": "count",
    }

    if "manager_lamar" in df.columns:
        agg_cols["manager_lamar"] = "sum"

    season = df.groupby(group_cols, as_index=False).agg(agg_cols)

    season.rename(
        columns={
            "clutch_equity": "total_clutch_equity",
            "was_above": "weeks_above_baseline",
            "was_below": "weeks_below_baseline",
            "week": "weeks_started",
            "manager_lamar": "total_manager_lamar",
        },
        inplace=True,
    )

    # Rate metrics (vectorized)
    season["avg_clutch_per_week"] = season["total_clutch_equity"] / season["weeks_started"].clip(lower=1)
    season["pct_above_baseline"] = 100.0 * season["weeks_above_baseline"] / season["weeks_started"].clip(lower=1)

    return season


# ============================================================================
# Final Week Detection
# ============================================================================


def get_final_regular_week_by_year(
    df: pd.DataFrame,
    settings_by_year: dict[int, dict] | None = None,
) -> dict[int, int]:
    """
    Find the final regular season week for each year.

    Priority order:
      1) Canonical league_settings (regular_season_weeks or playoff_start_week - 1)
      2) First playoff week detected in matchup (min is_playoffs=1) - 1
      3) Max regular-season week present in matchup

    This avoids false "final week" flags in leagues with regular-season byes,
    where the last *full* week (all managers played) can be earlier than the
    actual final regular-season week.

    Args:
        df: Full matchup DataFrame
        settings_by_year: Optional league_settings keyed by year

    Returns:
        Dict mapping year -> final regular season week
    """
    result = {}

    # Filter to regular season
    reg_mask = df["is_playoffs"] == 0
    if "is_consolation" in df.columns:
        reg_mask = reg_mask & (df["is_consolation"] == 0)

    reg = df[reg_mask].copy()

    def _to_int(value):
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return None
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return None

    for year in sorted(reg["year"].dropna().unique().astype(int)):
        year_data = reg[reg["year"] == year]
        if year_data.empty:
            continue

        max_reg_week = _to_int(pd.to_numeric(year_data["week"], errors="coerce").dropna().max())

        # 1) Canonical settings (regular_season_weeks or playoff_start_week - 1)
        candidate = None
        if settings_by_year:
            settings = settings_by_year.get(year) or settings_by_year.get(str(year)) or {}
            candidate = _to_int(settings.get("regular_season_weeks"))
            if candidate is None:
                playoff_start = _to_int(settings.get("playoff_start_week") or settings.get("playoff_week_start"))
                if playoff_start is not None:
                    candidate = playoff_start - 1
        if candidate is not None and candidate > 0:
            if max_reg_week is not None and candidate > max_reg_week:
                candidate = max_reg_week
            if candidate is not None:
                result[year] = candidate
                continue

        # 2) Infer from matchup playoff flags (first playoff week - 1)
        if "is_playoffs" in df.columns:
            playoff_mask = (df["year"] == year) & (df["is_playoffs"] == 1)
            if "is_consolation" in df.columns:
                playoff_mask = playoff_mask & (df["is_consolation"] == 0)
            playoff_weeks = pd.to_numeric(df.loc[playoff_mask, "week"], errors="coerce").dropna()
            if not playoff_weeks.empty:
                candidate = _to_int(playoff_weeks.min())
                if candidate is not None:
                    candidate = candidate - 1
                    if candidate > 0:
                        result[year] = candidate
                        continue

        # 3) Fallback to max regular-season week in data
        if max_reg_week is not None:
            result[year] = max_reg_week

    return result


# ============================================================================
# Vectorized Simulation Functions
# ============================================================================


def _generate_round_robin_base(n: int) -> np.ndarray:
    """
    Generate a base round-robin schedule for n teams.

    Uses the standard rotation algorithm but returns numpy arrays
    for efficient vectorization.

    Args:
        n: Number of teams (must be even)

    Returns:
        Array of shape (n-1, n//2, 2) representing matchups
        Indices are team positions in the manager list
    """
    # Standard round-robin: n-1 weeks, n/2 matchups per week
    n_weeks_rr = n - 1
    n_matches_per_week = n // 2

    # Initialize with team indices
    teams = list(range(n))
    left = teams[:n_matches_per_week]
    right = teams[n_matches_per_week:][::-1]

    schedule = np.zeros((n_weeks_rr, n_matches_per_week, 2), dtype=np.int32)

    for w in range(n_weeks_rr):
        for m in range(n_matches_per_week):
            schedule[w, m, 0] = left[m]
            schedule[w, m, 1] = right[m]

        # Rotate (keep left[0] fixed, rotate rest)
        if n > 2:
            right.insert(0, left.pop(1))
            left.append(right.pop())

    return schedule


def _generate_all_schedules(n_teams: int, n_weeks: int, n_sims: int, rng: np.random.Generator) -> np.ndarray:
    """
    Pre-generate all random schedules for vectorized simulation.

    Fully vectorized version that generates all schedules without Python loops.

    Args:
        n_teams: Number of teams
        n_weeks: Number of weeks
        n_sims: Number of simulations
        rng: Numpy random generator

    Returns:
        Array of shape (n_sims, n_weeks, n_matches_per_week, 2)
        Values are team indices (0 to n_teams-1)
    """
    n_matches_per_week = n_teams // 2
    max_rr_weeks = n_teams - 1

    # Generate base round-robin schedule once
    base_schedule = _generate_round_robin_base(n_teams)  # (max_rr_weeks, n_matches, 2)

    # Generate all permutations at once using vectorized shuffle
    # Create n_sims copies of [0, 1, 2, ..., n_teams-1]
    all_perms = np.tile(np.arange(n_teams), (n_sims, 1))  # (n_sims, n_teams)
    # Shuffle each row independently
    rng.permuted(all_perms, axis=1, out=all_perms)

    # Select which round-robin weeks to use for each simulation
    if n_weeks <= max_rr_weeks:
        # All simulations use the same week selection (first n_weeks)
        week_selection = np.arange(n_weeks)  # (n_weeks,)
        # Broadcast to all simulations
        week_selection = np.broadcast_to(week_selection, (n_sims, n_weeks))
    else:
        # Need more weeks than round-robin provides
        # First n-1 weeks are fixed, rest are randomly sampled
        fixed_weeks = np.arange(max_rr_weeks)  # (max_rr_weeks,)
        extra = n_weeks - max_rr_weeks
        # Generate random extra weeks for all simulations at once
        extra_weeks = rng.integers(0, max_rr_weeks, size=(n_sims, extra))  # (n_sims, extra)
        # Combine: broadcast fixed weeks and concatenate with random extras
        fixed_broadcast = np.broadcast_to(fixed_weeks, (n_sims, max_rr_weeks))
        week_selection = np.concatenate([fixed_broadcast, extra_weeks], axis=1)  # (n_sims, n_weeks)

    # Get base schedule indices for selected weeks
    # base_schedule shape: (max_rr_weeks, n_matches, 2)
    # week_selection shape: (n_sims, n_weeks)
    # Result shape: (n_sims, n_weeks, n_matches, 2)
    base_indices = base_schedule[week_selection]  # Advanced indexing

    # Apply permutations: map base indices through each simulation's permutation
    # base_indices contains team indices 0 to n_teams-1
    # all_perms[sim, base_idx] gives the shuffled team index
    # Use advanced indexing: for each (sim, week, match, side), look up all_perms[sim, base_indices[sim, week, match, side]]

    # Reshape for broadcasting
    # all_perms: (n_sims, n_teams)
    # base_indices: (n_sims, n_weeks, n_matches, 2)
    # We need: all_perms[sim_idx, base_indices[sim_idx, ...]]

    # Create simulation indices for advanced indexing
    sim_idx = np.arange(n_sims)[:, np.newaxis, np.newaxis, np.newaxis]  # (n_sims, 1, 1, 1)
    sim_idx = np.broadcast_to(sim_idx, base_indices.shape)  # (n_sims, n_weeks, n_matches, 2)

    # Apply permutations using advanced indexing
    all_schedules = all_perms[sim_idx, base_indices]

    return all_schedules.astype(np.int32)


def run_simulations_vectorized(
    points_dict: dict[tuple[str, int], float],
    managers: list[str],
    n_weeks: int,
    n_sims: int,
    rng_seed: int | None = None,
    mode: str = "performance",
    use_median: bool = False,
    standings_weights: dict[str, float] | None = None,
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    """
    Vectorized Monte Carlo simulation using numpy arrays.

    This is a highly optimized version of run_simulations that:
    1. Pre-generates all random schedules as numpy arrays
    2. Uses numpy broadcasting for point comparisons
    3. Uses numpy bincount for histogram building

    Performance: ~10-20x faster than Python loop version for n_sims >= 1000

    Args:
        points_dict: Dict[(manager, week)] -> points value
        managers: List of managers
        n_weeks: Number of weeks
        n_sims: Number of simulations
        rng_seed: Random seed for reproducibility
        mode: "performance" (team_points) or "opponent" (opponent_points)
        use_median: If True, include median wins (H2H+Median scoring)
        standings_weights: Optional dict of weight values for weighted seed ranking.
            Keys: h2h_wins, above_median_wins, all_play_wins, total_points, etc.
            When provided with non-zero all_play/total_points/optimal/efficiency,
            seed ranking uses weighted standing_points instead of H2H wins.

    Returns:
        (win_histograms, seed_histograms) - same format as run_simulations
    """
    n_teams = len(managers)
    if n_teams < 2:
        raise ValueError(f"Team count must be >= 2, got {n_teams}")
    if n_teams % 2 != 0:
        raise ValueError(
            f"Team count must be even, got {n_teams}. " "Caller should pad with a dummy bye team for odd-count leagues."
        )

    # Create manager index mapping
    mgr_to_idx = {m: i for i, m in enumerate(managers)}
    idx_to_mgr = {i: m for i, m in enumerate(managers)}

    # Convert points dict to numpy array: shape (n_teams, n_weeks)
    points_array = np.zeros((n_teams, n_weeks), dtype=np.float64)
    for (mgr, week), pts in points_dict.items():
        if mgr in mgr_to_idx and 1 <= week <= n_weeks:
            points_array[mgr_to_idx[mgr], week - 1] = pts

    # Initialize numpy RNG
    np_rng = np.random.default_rng(rng_seed)

    # Pre-generate all schedules
    all_schedules = _generate_all_schedules(n_teams, n_weeks, n_sims, np_rng)
    # Shape: (n_sims, n_weeks, n_matches_per_week, 2)

    n_matches_per_week = n_teams // 2

    # Extract team indices for each matchup
    team_a = all_schedules[:, :, :, 0]  # (n_sims, n_weeks, n_matches)
    team_b = all_schedules[:, :, :, 1]  # (n_sims, n_weeks, n_matches)

    # Look up points for each team in each matchup
    # Use advanced indexing: for each (sim, week, match), get points[team_idx, week]
    week_indices = np.arange(n_weeks)[np.newaxis, :, np.newaxis]  # (1, n_weeks, 1)
    week_indices = np.broadcast_to(week_indices, (n_sims, n_weeks, n_matches_per_week))

    points_a = points_array[team_a, week_indices]  # (n_sims, n_weeks, n_matches)
    points_b = points_array[team_b, week_indices]  # (n_sims, n_weeks, n_matches)

    # Calculate wins based on mode
    if mode == "performance":
        # Performance: higher points wins
        wins_a = (points_a > points_b).astype(np.float32)
        wins_b = (points_b > points_a).astype(np.float32)
    else:
        # Opponent difficulty: lower opponent_points wins (easier schedule)
        wins_a = (points_a < points_b).astype(np.float32)
        wins_b = (points_b < points_a).astype(np.float32)

    # Handle ties with random coin flip
    ties = points_a == points_b
    coin_flip = np_rng.random((n_sims, n_weeks, n_matches_per_week)) < 0.5
    wins_a[ties] = coin_flip[ties].astype(np.float32)
    wins_b[ties] = (~coin_flip[ties]).astype(np.float32)

    # Add median wins if H2H+Median
    median_wins_array = None
    if use_median and mode == "performance":
        # Calculate league median for each week in each simulation
        # For simplicity, use the same median based on actual points (not shuffled)
        # This is a reasonable approximation since the *order* of opponents changes, not the scores
        all_week_medians = np.median(points_array, axis=0)  # (n_weeks,)

        # For each team, add a win if their score exceeds median
        median_wins_array = np.zeros((n_sims, n_teams), dtype=np.float32)
        for team_idx in range(n_teams):
            team_points = points_array[team_idx, :]  # (n_weeks,)
            above_median = (team_points > all_week_medians).astype(np.float32)
            at_median = team_points == all_week_medians
            # Coin flip for exact ties at median
            coin = np_rng.random((n_sims, n_weeks)) < 0.5
            above_median_expanded = np.broadcast_to(above_median, (n_sims, n_weeks))
            at_median_expanded = np.broadcast_to(at_median, (n_sims, n_weeks))
            median_win_per_week = above_median_expanded.copy()
            median_win_per_week[at_median_expanded] = coin[at_median_expanded].astype(np.float32)
            median_wins_array[:, team_idx] = median_win_per_week.sum(axis=1)

    # Accumulate wins per team per simulation
    # wins_a/wins_b are indexed by team_a/team_b positions
    # We need to sum wins for each actual team index

    team_wins = np.zeros((n_sims, n_teams), dtype=np.float32)
    team_points_cum = np.zeros((n_sims, n_teams), dtype=np.float64)

    # Pre-compute simulation indices once
    sim_indices = np.arange(n_sims)

    # Vectorized accumulation using np.add.at
    for w in range(n_weeks):
        for m in range(n_matches_per_week):
            # For each simulation, add wins to the appropriate team
            np.add.at(team_wins, (sim_indices, team_a[:, w, m]), wins_a[:, w, m])
            np.add.at(team_wins, (sim_indices, team_b[:, w, m]), wins_b[:, w, m])
            # Track cumulative points for seeding
            np.add.at(team_points_cum, (sim_indices, team_a[:, w, m]), points_a[:, w, m])
            np.add.at(team_points_cum, (sim_indices, team_b[:, w, m]), points_b[:, w, m])

    # Add median wins if applicable
    if median_wins_array is not None:
        team_wins += median_wins_array

    # Calculate seeds for each simulation
    # Sort by wins (desc), then points (desc)
    # We need seed rankings for each simulation
    max_wins = int((2 * n_weeks) if use_median else n_weeks)
    # Allow all valid seeds for the league (was capped at 10, causing seed=0 for 11th/12th in 12-team leagues)
    n_seed_buckets = n_teams

    # Initialize histograms
    win_hists = {mgr: np.zeros(max_wins + 1, dtype=np.int32) for mgr in managers}
    seed_hists = {mgr: np.zeros(n_seed_buckets, dtype=np.int32) for mgr in managers}

    # Determine if weighted mode is active
    sw = standings_weights or {}
    is_weighted = any(
        sw.get(k, 0) > 0 for k in ["all_play_wins", "total_points", "optimal_points", "lineup_efficiency"]
    )

    # For seeding: compute standing_points per simulation when weighted
    if is_weighted:
        standing_points_sim = np.zeros((n_sims, n_teams), dtype=np.float64)

        # H2H component (varies per simulation — this is team_wins WITHOUT median)
        h2h_only_wins = team_wins.copy()
        if median_wins_array is not None:
            h2h_only_wins = team_wins - median_wins_array

        standing_points_sim += sw.get("h2h_wins", 1.0) * h2h_only_wins

        # Median component with its own weight (schedule-independent, constant across sims)
        if median_wins_array is not None:
            standing_points_sim += sw.get("above_median_wins", 0.0) * median_wins_array

        # All-play wins (schedule-independent, constant across sims)
        if sw.get("all_play_wins", 0) > 0:
            # For each team, count how many other teams they beat each week
            allplay_totals = np.zeros(n_teams, dtype=np.float64)
            for team_idx in range(n_teams):
                for w in range(n_weeks):
                    allplay_totals[team_idx] += (points_array[team_idx, w] > points_array[:, w]).sum()
            # Broadcast constant across all sims
            standing_points_sim += sw["all_play_wins"] * allplay_totals[np.newaxis, :]

        # Total points (schedule-independent)
        if sw.get("total_points", 0) > 0:
            team_total_pts = points_array.sum(axis=1)  # (n_teams,)
            standing_points_sim += sw["total_points"] * team_total_pts[np.newaxis, :]

        # Use standing_points for seed ranking (higher = better seed)
        sort_key = -standing_points_sim * 1e9 - team_points_cum
    else:
        # Traditional: sort by wins then points
        sort_key = -team_wins * 1e9 - team_points_cum  # (n_sims, n_teams)

    # Vectorized seed ranking using argsort + argsort inversion trick
    # First argsort gives the order (which team is 1st, 2nd, etc.)
    # Second argsort on the order gives the rank for each team position
    order = np.argsort(sort_key, axis=1)  # (n_sims, n_teams)
    seed_ranks = np.argsort(order, axis=1) + 1  # +1 for 1-indexed seeds

    # Build histograms using numpy operations
    for team_idx, mgr in enumerate(managers):
        wins_this_team = team_wins[:, team_idx].astype(np.int32)
        seeds_this_team = seed_ranks[:, team_idx]

        # Clip wins to valid range
        wins_this_team = np.clip(wins_this_team, 0, max_wins)

        # Count wins using bincount
        win_counts = np.bincount(wins_this_team, minlength=max_wins + 1)
        win_hists[mgr] = win_counts[: max_wins + 1].tolist()

        # Count seeds (1-indexed, so subtract 1 for array index)
        valid_seeds = seeds_this_team[(seeds_this_team >= 1) & (seeds_this_team <= n_seed_buckets)]
        if len(valid_seeds) > 0:
            seed_counts = np.bincount(valid_seeds - 1, minlength=n_seed_buckets)
            seed_hists[mgr] = seed_counts[:n_seed_buckets].tolist()
        else:
            seed_hists[mgr] = [0] * n_seed_buckets

    return win_hists, seed_hists


# NOTE: The following functions have been moved to bracket_simulation.py:
# - _get_bracket_side()
# - simulate_playoff_bracket_vectorized()
# - calculate_effective_byes()
# - apply_completed_round_overrides()
# - build_playoff_week_mask()
#
# They are imported at the top of this file for backwards compatibility.
# See bracket_simulation.py for the implementation.
