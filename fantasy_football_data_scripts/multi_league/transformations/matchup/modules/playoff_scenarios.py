#!/usr/bin/env python3
"""
playoff_scenarios.py - Playoff Scenario Calculations

Adds columns for:
- Magic numbers (clinch playoffs, bye, 1st seed)
- Elimination numbers
- Clinch/elimination flags
- Weekly playoff odds changes
- Critical matchup identification

These columns enable:
1. Year-in-review summaries (dramatic swings, critical matchups)
2. Playoff Machine UI (fast scenario calculations)
3. Clinch/elimination alerts

SETTINGS-DRIVEN: Reads playoff configuration from flat league_settings JSON files.
"""

from __future__ import annotations

import json
import numpy as np
import pandas as pd
from pathlib import Path

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

try:
    from core.data_normalization import find_league_settings_directory
except ImportError:
    find_league_settings_directory = None

from multi_league.core.identity import get_manager_col


def _load_year_settings(year: int, data_directory: str | None = None, df: pd.DataFrame | None = None) -> dict:
    """
    Load league settings for a specific year.

    Args:
        year: Season year
        data_directory: Path to league data directory
        df: DataFrame with league_id for auto-detection

    Returns:
        Dict with num_playoff_teams, bye_teams, regular_season_weeks
    """
    # Default bye_teams from bracket structure: next_power_of_2(N) - N
    default_playoff_teams = 6
    _next_p2 = 1 << (default_playoff_teams - 1).bit_length()
    default_bye_teams = _next_p2 - default_playoff_teams
    defaults = {
        "num_playoff_teams": default_playoff_teams,
        "bye_teams": default_bye_teams,
        "regular_season_weeks": 14,
        "playoff_start_week": 15,
    }

    if find_league_settings_directory is None:
        return defaults

    # Find settings directory
    settings_path = None
    if data_directory:
        settings_path = find_league_settings_directory(data_directory=Path(data_directory), df=df)
    elif df is not None:
        settings_path = find_league_settings_directory(df=df)

    if not settings_path or not settings_path.exists():
        return defaults

    # Find settings file for this year. DbContext writes league_settings_YYYY.json
    # while raw import flows may still use league_settings_YYYY_*.json.
    settings_files = list(settings_path.glob(f"league_settings_{year}.json"))
    if not settings_files:
        settings_files = list(settings_path.glob(f"league_settings_{year}_*.json"))
    if not settings_files:
        settings_files = list(settings_path.glob(f"settings_{year}.json"))
    if not settings_files:
        return defaults

    try:
        with open(settings_files[0], encoding="utf-8") as f:
            settings = json.load(f)

        result = defaults.copy()

        # Extract playoff configuration from the flat DDL first.
        if "playoff_teams" in settings and settings["playoff_teams"] is not None:
            result["num_playoff_teams"] = int(settings["playoff_teams"])
        elif "num_playoff_teams" in settings and settings["num_playoff_teams"] is not None:
            result["num_playoff_teams"] = int(settings["num_playoff_teams"])

        if "playoff_start_week" in settings and settings["playoff_start_week"] is not None:
            result["playoff_start_week"] = int(settings["playoff_start_week"])
            # Regular season weeks = playoff_start_week - 1
            result["regular_season_weeks"] = result["playoff_start_week"] - 1

        if "bye_teams" in settings and settings["bye_teams"] is not None:
            result["bye_teams"] = int(settings["bye_teams"])
        elif "bye_week_count" in settings and settings["bye_week_count"] is not None:
            result["bye_teams"] = int(settings["bye_week_count"])
        elif result["num_playoff_teams"] == 6:
            result["bye_teams"] = 2  # Standard 6-team playoff with 2 byes
        elif result["num_playoff_teams"] == 4:
            result["bye_teams"] = 0  # 4-team playoff, no byes
        elif result["num_playoff_teams"] == 8:
            result["bye_teams"] = 0  # 8-team playoff, no byes

        return result

    except Exception as e:
        print(f"[WARN] playoff_scenarios: Failed to load settings for {year}: {e}")
        return defaults


def calculate_magic_numbers(
    df: pd.DataFrame,
    num_playoff_teams: int = 6,
    num_bye_teams: int = 2,
    total_regular_weeks: int = 14,
    data_directory: str | None = None,
) -> pd.DataFrame:
    """
    Calculate magic numbers for playoff clinching and elimination.

    Magic Number = Games remaining + 1 + (Current wins of team on bubble) - (Your wins)
    When magic number reaches 0, you've clinched.

    Elimination Number = When it's mathematically impossible to make playoffs.

    Args:
        df: DataFrame with matchup data
        num_playoff_teams: Number of teams that make playoffs (default 6, overridden by settings)
        num_bye_teams: Number of teams with first-round bye (default 2, overridden by settings)
        total_regular_weeks: Total regular season weeks (default 14, overridden by settings)
        data_directory: Path to league data directory for loading settings

    Returns:
        DataFrame with magic number columns added
    """
    df = df.copy()

    # Ensure required columns
    required = ["year", "week", "manager", "wins_to_date", "losses_to_date", "is_playoffs", "is_consolation"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        print(f"[WARN] playoff_scenarios: Missing columns {missing}, skipping magic numbers")
        return df

    # Initialize columns
    df["playoff_magic_number"] = np.nan
    df["bye_magic_number"] = np.nan
    df["first_seed_magic_number"] = np.nan
    df["elimination_number"] = np.nan
    df["clinched_playoffs"] = 0
    df["clinched_bye"] = 0
    df["clinched_first_seed"] = 0
    df["eliminated_from_playoffs"] = 0
    df["eliminated_from_bye"] = 0

    # Cache settings by year to avoid repeated file reads
    _year_settings_cache = {}

    # Process each year/week
    for (year, _week), group in df.groupby(["year", "week"]):
        # Only calculate for regular season
        if group["is_playoffs"].max() == 1 or group["is_consolation"].max() == 1:
            continue

        # Load year-specific settings (cached)
        year_int = int(year)
        if year_int not in _year_settings_cache:
            _year_settings_cache[year_int] = _load_year_settings(year_int, data_directory, df)
        year_settings = _year_settings_cache[year_int]

        # Use year-specific settings (fall back to function params if not found)
        yr_num_playoff_teams = year_settings.get("num_playoff_teams", num_playoff_teams)
        yr_num_bye_teams = year_settings.get("bye_teams", num_bye_teams)
        yr_total_regular_weeks = year_settings.get("regular_season_weeks", total_regular_weeks)

        # Get standings for this week
        standings = group.groupby("franchise_id").agg({"wins_to_date": "max", "losses_to_date": "max"}).reset_index()

        standings = standings.sort_values("wins_to_date", ascending=False).reset_index(drop=True)
        n_teams = len(standings)

        if n_teams == 0:
            continue

        # Calculate games remaining
        games_played = standings["wins_to_date"].iloc[0] + standings["losses_to_date"].iloc[0]
        games_remaining = max(0, yr_total_regular_weeks - games_played)

        # Get bubble team wins (team in last playoff spot)
        if n_teams >= yr_num_playoff_teams:
            bubble_playoff_wins = standings.iloc[yr_num_playoff_teams - 1]["wins_to_date"]
            first_out_wins = (
                standings.iloc[yr_num_playoff_teams]["wins_to_date"] if n_teams > yr_num_playoff_teams else 0
            )
        else:
            bubble_playoff_wins = 0
            first_out_wins = 0

        if n_teams >= yr_num_bye_teams and yr_num_bye_teams > 0:
            bubble_bye_wins = standings.iloc[yr_num_bye_teams - 1]["wins_to_date"]
        else:
            bubble_bye_wins = 0

        first_place_wins = standings.iloc[0]["wins_to_date"]

        # Calculate magic numbers for each manager
        for idx in group.index:
            mgr = df.at[idx, "manager"]
            mgr_wins = df.at[idx, "wins_to_date"]

            if pd.isna(mgr_wins):
                continue

            # Playoff magic number
            # You clinch when: your_wins > bubble_team_max_possible_wins
            # Magic = games_remaining + bubble_wins - your_wins + 1
            playoff_magic = games_remaining + first_out_wins - mgr_wins + 1
            df.at[idx, "playoff_magic_number"] = max(0, playoff_magic)

            if playoff_magic <= 0:
                df.at[idx, "clinched_playoffs"] = 1

            # Bye magic number
            bye_magic = games_remaining + bubble_bye_wins - mgr_wins + 1
            df.at[idx, "bye_magic_number"] = max(0, bye_magic)

            if bye_magic <= 0:
                df.at[idx, "clinched_bye"] = 1

            # First seed magic number
            first_seed_magic = games_remaining + first_place_wins - mgr_wins + 1
            df.at[idx, "first_seed_magic_number"] = max(0, first_seed_magic)

            if first_seed_magic <= 0 and mgr_wins >= first_place_wins:
                df.at[idx, "clinched_first_seed"] = 1

            # Elimination number
            # You're eliminated when: your_max_possible_wins < bubble_team_current_wins
            max_possible_wins = mgr_wins + games_remaining
            if max_possible_wins < bubble_playoff_wins:
                df.at[idx, "eliminated_from_playoffs"] = 1
                df.at[idx, "elimination_number"] = 0
            else:
                # Elimination number = wins needed by bubble team to eliminate you
                elim_number = max_possible_wins - bubble_playoff_wins + 1
                df.at[idx, "elimination_number"] = max(0, elim_number)

            # Bye elimination
            if max_possible_wins < bubble_bye_wins:
                df.at[idx, "eliminated_from_bye"] = 1

    return df


def calculate_weekly_odds_changes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate week-over-week changes in playoff odds.

    Adds columns:
    - p_playoffs_change: Change in playoff probability from last week
    - p_champ_change: Change in championship probability
    - p_bye_change: Change in bye probability
    - is_critical_matchup: 1 if this matchup caused >10% swing for any team

    Args:
        df: DataFrame with franchise_id (required) and playoff odds
            (p_playoffs, p_champ, p_bye).

    Returns:
        DataFrame with change columns added

    Raises:
        KeyError: if df does not contain a franchise_id column.
    """
    df = df.copy()

    # Check for required columns
    odds_cols = ["p_playoffs", "p_champ", "p_bye"]
    available_cols = [c for c in odds_cols if c in df.columns]

    if not available_cols:
        print("[WARN] playoff_scenarios: No playoff odds columns found, skipping change calculation")
        return df

    # Initialize change columns
    for col in available_cols:
        df[f"{col}_change"] = np.nan
        df[f"{col}_prev"] = np.nan

    df["max_odds_swing"] = np.nan
    df["is_critical_matchup"] = 0

    # franchise_id is required — raises KeyError if missing
    manager_col = get_manager_col(df)

    # Sort by year, manager/franchise, week
    df = df.sort_values(["year", manager_col, "week"]).reset_index(drop=True)

    # Calculate changes within each year/manager
    for (_year, _manager), group in df.groupby(["year", manager_col]):
        if len(group) < 2:
            continue

        group = group.sort_values("week")
        indices = group.index.tolist()

        for i, idx in enumerate(indices[1:], 1):
            prev_idx = indices[i - 1]

            for col in available_cols:
                curr_val = df.at[idx, col]
                prev_val = df.at[prev_idx, col]

                if pd.notna(curr_val) and pd.notna(prev_val):
                    df.at[idx, f"{col}_change"] = curr_val - prev_val
                    df.at[idx, f"{col}_prev"] = prev_val

    # Calculate max swing per matchup
    if "p_playoffs_change" in df.columns:
        # Group by matchup (year, week, manager+opponent pair)
        for (_year, _week), group in df.groupby(["year", "week"]):
            if "opponent" not in df.columns:
                continue

            # Find max absolute change in this week
            max_swing = group["p_playoffs_change"].abs().max()
            if pd.notna(max_swing):
                df.loc[group.index, "max_odds_swing"] = max_swing

                # Mark as critical if >10% swing
                if max_swing > 10:
                    df.loc[group.index, "is_critical_matchup"] = 1

    return df


def identify_dramatic_moments(df: pd.DataFrame, threshold: float = 15.0) -> pd.DataFrame:
    """
    Identify the most dramatic moments in the season.

    Adds columns:
    - is_dramatic_win: 1 if this win caused a large positive swing
    - is_dramatic_loss: 1 if this loss caused a large negative swing
    - drama_score: Absolute value of odds change (for ranking)

    Args:
        df: DataFrame with playoff odds changes
        threshold: Minimum odds change to be considered dramatic (default 15%)

    Returns:
        DataFrame with dramatic moment flags
    """
    df = df.copy()

    df["is_dramatic_win"] = 0
    df["is_dramatic_loss"] = 0
    df["drama_score"] = 0.0

    if "p_playoffs_change" not in df.columns or "win" not in df.columns:
        return df

    # Calculate drama score
    df["drama_score"] = df["p_playoffs_change"].abs().fillna(0)

    # Mark dramatic wins/losses
    dramatic_mask = df["drama_score"] >= threshold

    df.loc[dramatic_mask & (df["win"] == 1), "is_dramatic_win"] = 1
    df.loc[dramatic_mask & (df["win"] == 0), "is_dramatic_loss"] = 1

    return df


def calculate_scenario_factors(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate factors needed for fast scenario simulation in UI.

    Adds columns:
    - team_mu: Team's expected score (power rating)
    - team_sigma: Team's scoring standard deviation
    - win_probability_vs_avg: Probability of beating league average team

    These enable fast "what-if" calculations without full Monte Carlo.

    Args:
        df: DataFrame with team performance data

    Returns:
        DataFrame with scenario factors added
    """
    df = df.copy()

    # Initialize columns
    df["team_mu"] = np.nan
    df["team_sigma"] = np.nan
    df["win_probability_vs_avg"] = np.nan

    if "team_points" not in df.columns:
        print("[WARN] playoff_scenarios: team_points column missing, skipping scenario factors")
        return df

    # franchise_id is required — raises KeyError if missing
    manager_col = get_manager_col(df)

    # Calculate per franchise/year/week (cumulative stats up to that point)
    for (year, week), group in df.groupby(["year", "week"]):
        # Get all games up to this week for this year
        year_data = df[(df["year"] == year) & (df["week"] <= week)]

        if year_data.empty:
            continue

        # League average for this year (up to this week)
        league_mu = year_data["team_points"].mean()
        league_sigma = year_data["team_points"].std()

        if pd.isna(league_sigma) or league_sigma == 0:
            league_sigma = 20.0  # Default floor

        # Calculate per franchise (franchise_id is the stable join key — backend
        # convention. Previous version named the loop var `manager` and wrote back
        # using df["manager"] == manager, which silently matched zero rows since
        # the loop values were franchise_ids.)
        for fid in group["franchise_id"].unique():
            if pd.isna(fid):
                continue
            mgr_data = year_data[year_data["franchise_id"] == fid]["team_points"]

            if len(mgr_data) >= 2:
                team_mu = mgr_data.mean()
                team_sigma = mgr_data.std()
                if pd.isna(team_sigma) or team_sigma < 10:
                    team_sigma = max(10, league_sigma * 0.8)
            else:
                # Not enough data, use league average with shrinkage
                team_mu = league_mu
                team_sigma = league_sigma

            # Win probability vs average team (logistic approximation)
            # P(A beats B) ≈ 1 / (1 + 10^((mu_B - mu_A) / scale))
            scale = 25.0  # Calibrated for fantasy football
            win_prob = 1 / (1 + 10 ** ((league_mu - team_mu) / scale))

            # Update rows for this franchise/week
            mask = (df["year"] == year) & (df["week"] == week) & (df["franchise_id"] == fid)
            df.loc[mask, "team_mu"] = round(team_mu, 2)
            df.loc[mask, "team_sigma"] = round(team_sigma, 2)
            df.loc[mask, "win_probability_vs_avg"] = round(win_prob * 100, 1)

    return df


def add_playoff_scenario_columns(
    df: pd.DataFrame,
    num_playoff_teams: int = 6,
    num_bye_teams: int = 2,
    total_regular_weeks: int = 14,
    data_directory: str | None = None,
) -> pd.DataFrame:
    """
    Main entry point - adds all playoff scenario columns.

    Columns added:
    - playoff_magic_number, bye_magic_number, first_seed_magic_number
    - elimination_number
    - clinched_playoffs, clinched_bye, clinched_first_seed
    - eliminated_from_playoffs, eliminated_from_bye
    - p_playoffs_change, p_champ_change, p_bye_change
    - is_critical_matchup, is_dramatic_win, is_dramatic_loss, drama_score
    - team_mu, team_sigma, win_probability_vs_avg

    Args:
        df: DataFrame with matchup data
        num_playoff_teams: Number of playoff teams (overridden by settings if available)
        num_bye_teams: Number of bye teams (overridden by settings if available)
        total_regular_weeks: Total regular season weeks (overridden by settings if available)
        data_directory: Path to league data directory for loading settings

    Returns:
        DataFrame with all scenario columns added
    """
    print("[playoff_scenarios] Adding playoff scenario columns...")

    # Step 1: Magic numbers and clinch/elimination
    print("  [1/4] Calculating magic numbers...")
    df = calculate_magic_numbers(df, num_playoff_teams, num_bye_teams, total_regular_weeks, data_directory)

    # Step 2: Weekly odds changes
    print("  [2/4] Calculating weekly odds changes...")
    df = calculate_weekly_odds_changes(df)

    # Step 3: Dramatic moments
    print("  [3/4] Identifying dramatic moments...")
    df = identify_dramatic_moments(df)

    # Step 4: Scenario factors for UI
    print("  [4/4] Calculating scenario factors (team_mu, team_sigma)...")
    df = calculate_scenario_factors(df)

    print("[playoff_scenarios] Done - added scenario columns")
    return df
