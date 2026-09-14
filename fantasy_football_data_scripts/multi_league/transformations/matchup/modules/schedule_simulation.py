def ensure_normalized(func):
    """Decorator to ensure data normalization"""

    @wraps(func)
    def wrapper(df: pd.DataFrame, *args, **kwargs):
        # Normalize input
        df = normalize_numeric_columns(df)

        # Run transformation
        result = func(df, *args, **kwargs)

        # Normalize output
        result = normalize_numeric_columns(result)

        # Ensure league_id present
        if "league_id" in df.columns:
            league_id = df["league_id"].iloc[0] if len(df) > 0 else None
            if league_id:
                result = ensure_league_id(result, league_id)

        return result

    return wrapper


"""
Schedule Simulation Module

Core simulation engine for schedule-independent record calculations.

This module handles:
- Round-robin schedule generation
- Random schedule validation
- Expected record simulation (performance-based)
- Schedule strength simulation (opponent-based)
"""

from functools import wraps

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

from core.data_normalization import normalize_numeric_columns, ensure_league_id
import numpy as np
import pandas as pd
from collections import defaultdict
import random
from multi_league.core.identity import get_manager_col


def round_robin_weeks(managers: list[str]) -> dict[int, list[tuple[str, str]]]:
    """
    Generate round-robin schedule using rotation algorithm.
    Args:
        managers: List of manager names (must be even number, >= 4)
    Returns:
        Dict mapping week number to list of matchups
    Raises:
        ValueError: If team count is odd or < 4
    """
    teams = list(managers)
    n = len(teams)
    if n < 4:
        raise ValueError(f"Team count must be >= 4, got {n}")
    if n % 2 != 0:
        raise ValueError(f"Team count must be even, got {n}")
    left = teams[: n // 2]
    right = teams[n // 2 :][::-1]
    weeks = {}
    for w in range(1, n):
        weeks[w] = [(a, b) for a, b in zip(left, right)]
        if n > 2:
            right.insert(0, left.pop(1))
            left.append(right.pop())
    return weeks


def validate_schedule(
    sched: dict[int, list[tuple[str, str]]], teams: list[str], no_repeats_weeks: int = 5, max_meetings: int = 2
) -> tuple[bool, str]:
    """
    Validate schedule constraints.
    Checks:
    - No team plays twice in same week
    - No matchup repeats in first N weeks
    - No pair plays more than max_meetings times
    - Every team plays exactly n_weeks games
    Args:
        sched: Schedule dict
        teams: List of team names
        no_repeats_weeks: No repeat matchups in first N weeks
        max_meetings: Maximum times any pair can meet
    Returns:
        (is_valid, message)
    """
    pair_ct = defaultdict(int)
    earliest = min(sched.keys())
    latest = max(sched.keys())
    early_end = min(earliest + no_repeats_weeks - 1, latest)
    seen_early = set()
    # Check early weeks for repeats
    for w in range(earliest, early_end + 1):
        for a, b in sched[w]:
            p = tuple(sorted((a, b)))
            if p in seen_early:
                return False, f"Repeat within weeks {earliest}-{early_end}: {p} in week {w}"
            seen_early.add(p)
    # Check weekly constraints
    for w, games in sched.items():
        used = set()
        for a, b in games:
            if a in used or b in used:
                return False, f"Team plays twice in week {w}"
            used.add(a)
            used.add(b)
            p = tuple(sorted((a, b)))
            pair_ct[p] += 1
            if pair_ct[p] > max_meetings:
                return False, f"Pair > {max_meetings} meetings: {p}"
    # Check total games per team
    team_games = defaultdict(int)
    n_weeks = len(sched)
    for games in sched.values():
        for a, b in games:
            team_games[a] += 1
            team_games[b] += 1
    for t in teams:
        if team_games[t] != n_weeks:
            return False, f"{t} has {team_games[t]} games (need {n_weeks})"
    return True, "OK"


def schedule_nxN(
    managers: list[str], n_weeks: int, rng: random.Random, validate: bool = False
) -> dict[int, list[tuple[str, str]]]:
    """
    Generate random N-week schedule for N teams.
    Uses round-robin as base, shuffles teams and weeks for randomness.
    Args:
        managers: List of manager names
        n_weeks: Number of weeks to generate
        rng: Random number generator for reproducibility
        validate: Whether to validate constraints
    Returns:
        Dict mapping week to list of matchups
    Raises:
        ValueError: If team count is invalid or n_weeks exceeds maximum
    """
    teams = list(managers)
    n = len(teams)
    if n < 4:
        raise ValueError(f"Team count must be >= 4, got {n}")
    if n % 2 != 0:
        raise ValueError(f"Team count must be even, got {n}")
    max_weeks = 2 * (n - 1)
    if n_weeks > max_weeks:
        raise ValueError(f"n_weeks ({n_weeks}) > max_weeks ({max_weeks}) would force some pair >2 meetings")
    # Shuffle teams for randomness
    M = list(managers)
    rng.shuffle(M)
    # Generate round-robin
    rr = round_robin_weeks(M)
    base_weeks = list(rr.keys())
    last_rr = base_weeks[-1]
    # Take first n_weeks from round-robin
    sched = {w: rr[w][:] for w in range(1, min(last_rr, n_weeks) + 1)}
    # If need more weeks, randomly pick from round-robin
    extra = max(0, n_weeks - last_rr)
    if extra:
        pick = rng.sample(base_weeks, extra)
        rng.shuffle(pick)
        for off, base_w in enumerate(pick, start=last_rr + 1):
            sched[off] = rr[base_w][:]
    # Shuffle weeks 6+ for additional randomness
    if n_weeks > 5:
        tail_old = list(range(6, n_weeks + 1))
        rng.shuffle(tail_old)
        remapped = {}
        for w in range(1, 6):
            remapped[w] = sched[w]
        for new_w, old_w in zip(range(6, n_weeks + 1), tail_old):
            remapped[new_w] = sched[old_w]
        sched = remapped
    if validate:
        ok, msg = validate_schedule(sched, M)
        if not ok:
            raise RuntimeError(f"Schedule invalid: {msg}")
    return sched


def simulate_once_performance(
    points_by_mgr_week: dict[tuple[str, int], float],
    managers: list[str],
    n_weeks: int,
    rng: random.Random,
    use_median: bool = False,
) -> tuple[dict[str, int], dict[str, int]]:
    """
    Simulate one random schedule using actual team_points.
    Compares each manager's team_points against opponent's team_points
    in randomly generated matchups.

    For H2H+Median leagues (use_median=True), also adds a median win/loss
    for each manager based on whether their score is above/below the
    league median for that week.

    Args:
        points_by_mgr_week: Dict[(manager, week)] -> team_points
        managers: List of managers
        n_weeks: Number of weeks
        rng: Random generator
        use_median: If True, include median wins (H2H+Median scoring)
    Returns:
        (wins_dict, seeds_dict)
    """
    sched = schedule_nxN(managers, n_weeks, rng, validate=False)
    wins = defaultdict(int)
    cum_points = defaultdict(float)

    for w in range(1, n_weeks + 1):
        # Collect all points for this week (for median calculation)
        week_points = {}
        for a, b in sched[w]:
            pa = points_by_mgr_week.get((a, w))
            pb = points_by_mgr_week.get((b, w))
            if pa is not None:
                week_points[a] = pa
            if pb is not None:
                week_points[b] = pb

        # Calculate H2H wins
        for a, b in sched[w]:
            pa = points_by_mgr_week.get((a, w))
            pb = points_by_mgr_week.get((b, w))
            if pa is None or pb is None:
                continue
            cum_points[a] += pa
            cum_points[b] += pb
            if pa > pb:
                wins[a] += 1
            elif pb > pa:
                wins[b] += 1
            else:
                # Coin flip for ties
                wins[a if rng.random() < 0.5 else b] += 1

        # Add median wins if H2H+Median scoring is enabled
        if use_median and week_points:
            # Calculate league median for this week
            scores = sorted(week_points.values())
            n_scores = len(scores)
            if n_scores > 0:
                if n_scores % 2 == 0:
                    median = (scores[n_scores // 2 - 1] + scores[n_scores // 2]) / 2
                else:
                    median = scores[n_scores // 2]

                # Award median wins (above median = win, below = loss, at median = coin flip)
                for mgr, pts in week_points.items():
                    if pts > median:
                        wins[mgr] += 1
                    elif pts == median:
                        # Coin flip for exact median ties
                        if rng.random() < 0.5:
                            wins[mgr] += 1
                    # Below median = no additional win (effectively a loss)

    # Seed by wins (descending), then points (descending)
    ladder = [(m, wins[m], cum_points[m]) for m in managers]
    ladder.sort(key=lambda x: (-x[1], -x[2], x[0]))
    seeds = {m: i + 1 for i, (m, _, _) in enumerate(ladder)}
    return wins, seeds


def simulate_once_opponent_difficulty(
    opp_points_by_mgr_week: dict[tuple[str, int], float], managers: list[str], n_weeks: int, rng: random.Random
) -> tuple[dict[str, int], dict[str, int]]:
    """
    Simulate one random schedule using opponent_points (schedule strength).
    Compares opponent difficulty: LOWER opponent_points = easier schedule = more "wins".
    This measures schedule luck rather than performance.
    Args:
        opp_points_by_mgr_week: Dict[(manager, week)] -> opponent_points
        managers: List of managers
        n_weeks: Number of weeks
        rng: Random generator
    Returns:
        (wins_dict, seeds_dict) where wins = # of easy weeks
    """
    sched = schedule_nxN(managers, n_weeks, rng, validate=False)
    wins = defaultdict(int)  # "wins" = easier weeks
    cum_opp = defaultdict(float)  # Lower is better
    for w in range(1, n_weeks + 1):
        for a, b in sched[w]:
            pa = opp_points_by_mgr_week.get((a, w))  # opponent_points manager A faced
            pb = opp_points_by_mgr_week.get((b, w))  # opponent_points manager B faced
            if pa is None or pb is None:
                continue
            cum_opp[a] += pa
            cum_opp[b] += pb
            # FLIPPED: Lower opponent_points = easier = win
            if pa < pb:
                wins[a] += 1
            elif pb < pa:
                wins[b] += 1
            else:
                wins[a if rng.random() < 0.5 else b] += 1
    # Seed by more easy weeks (wins descending), then lower cum_opp (ascending)
    ladder = [(m, wins[m], cum_opp[m]) for m in managers]
    ladder.sort(key=lambda x: (-x[1], x[2], x[0]))  # More wins better, lower opp better
    seeds = {m: i + 1 for i, (m, _, _) in enumerate(ladder)}
    return wins, seeds


def current_regular_week(df_season: pd.DataFrame) -> int:
    """
    Find the latest completed regular season week.
    A week is complete if every non-bye team has a recorded team_points.

    Odd-team leagues (e.g. pigskin 2014 with 9 teams) have one team on a
    natural bye each regular-season week. Those bye rows live in the
    matchup table with is_bye_week=1 and team_points=NULL, which would
    otherwise cause this function to report week 0 for every year — in
    turn skipping the entire shuffle/expected-record simulation for
    odd-team leagues. Filter bye rows out before the completeness check.

    Args:
        df_season: Season matchup data
    Returns:
        Latest complete week number (0 if none)
    """
    if "is_bye_week" in df_season.columns:
        non_bye = df_season[df_season["is_bye_week"].fillna(False).astype(bool) == False]
    else:
        non_bye = df_season
    managers = sorted(non_bye["franchise_id"].dropna().unique())
    weeks = sorted(non_bye["week"].dropna().unique())
    complete = 0
    for w in weeks:
        block = non_bye[non_bye["week"] == w]
        # Every playing team for this week must have a recorded score. In
        # odd-team leagues the bye team is simply absent from `block` after
        # the filter above, so the count reflects only the teams who played.
        week_teams = block["franchise_id"].dropna().unique()
        expected_count = len(managers) - 1 if len(managers) % 2 != 0 else len(managers)
        if len(week_teams) >= expected_count and block["team_points"].notna().all():
            complete = w
        else:
            break
    return int(complete)


def run_simulations(
    points_dict: dict[tuple[str, int], float],
    managers: list[str],
    n_weeks: int,
    n_sims: int,
    rng: random.Random,
    mode: str = "performance",
    use_median: bool = False,
) -> tuple[dict[str, list[int]], dict[str, list[int]]]:
    """
    Run N simulations and collect win/seed distributions.
    Args:
        points_dict: Dict[(manager, week)] -> points value
        managers: List of managers
        n_weeks: Number of weeks
        n_sims: Number of simulations
        rng: Random generator
        mode: "performance" (team_points) or "opponent" (opponent_points)
        use_median: If True, include median wins in simulation (H2H+Median scoring)
    Returns:
        (win_histograms, seed_histograms)
    """
    # For H2H+Median, max possible wins is 2*n_weeks (H2H + median per week)
    max_wins = (2 * n_weeks) if use_median else n_weeks
    win_hists = {mgr: [0] * (max_wins + 1) for mgr in managers}
    seed_hists = {mgr: [0] * len(managers) for mgr in managers}

    for _ in range(n_sims):
        if mode == "performance":
            wins, seeds = simulate_once_performance(points_dict, managers, n_weeks, rng, use_median=use_median)
        else:
            wins, seeds = simulate_once_opponent_difficulty(points_dict, managers, n_weeks, rng)

        for mgr in managers:
            wc = max(0, min(wins.get(mgr, 0), max_wins))
            sd = seeds.get(mgr, 1)
            win_hists[mgr][wc] += 1
            if 1 <= sd <= len(seed_hists[mgr]):
                seed_hists[mgr][sd - 1] += 1
    return win_hists, seed_hists


@ensure_normalized
def calculate_summary_stats(df: pd.DataFrame, regular_mask: pd.Series, col_prefix: str) -> pd.DataFrame:
    """
    Calculate summary statistics from probability distributions.
    Args:
        df: DataFrame with probability columns
        regular_mask: Boolean mask for regular season rows
        col_prefix: "shuffle" or "opp_shuffle"
    Returns:
        DataFrame with summary columns added
    """
    df = df.copy()
    # Expected wins
    win_prob_cols = [f"{col_prefix}_{w}_win" for w in range(0, 15)]
    win_prob_cols = [c for c in win_prob_cols if c in df.columns]
    if win_prob_cols:
        df_reg = df.loc[regular_mask, win_prob_cols]
        if not df_reg.empty:
            weights = np.arange(0, len(win_prob_cols), dtype=float)
            vals = np.round((df_reg.fillna(0.0).to_numpy(dtype=float) @ weights) / 100.0, 2)
            has_data = df_reg.notna().any(axis=1)
            df.loc[regular_mask, f"{col_prefix}_avg_wins"] = vals
            df.loc[regular_mask & ~has_data, f"{col_prefix}_avg_wins"] = np.nan
    # Expected seed - dynamically detect all seed columns instead of hardcoding 10
    import re as _re

    seed_prob_cols = sorted(
        [c for c in df.columns if _re.match(rf"^{_re.escape(col_prefix)}_\d+_seed$", c)],
        key=lambda c: int(c.split("_")[-2]),
    )
    if seed_prob_cols:
        df_reg = df.loc[regular_mask, seed_prob_cols]
        if not df_reg.empty:
            weights = np.arange(1, len(seed_prob_cols) + 1, dtype=float)
            vals = np.round((df_reg.fillna(0.0).to_numpy(dtype=float) @ weights) / 100.0, 2)
            has_data = df_reg.notna().any(axis=1)
            df.loc[regular_mask, f"{col_prefix}_avg_seed"] = vals
            df.loc[regular_mask & ~has_data, f"{col_prefix}_avg_seed"] = np.nan
    # Playoff odds (seeds 1-6)
    playoff_cols = [f"{col_prefix}_{s}_seed" for s in range(1, 7)]
    playoff_cols = [c for c in playoff_cols if c in df.columns]
    if playoff_cols:
        df_reg = df.loc[regular_mask, playoff_cols]
        if not df_reg.empty:
            vals = df_reg.fillna(0.0).sum(axis=1)
            has_data = df_reg.notna().any(axis=1)
            df.loc[regular_mask, f"{col_prefix}_avg_playoffs"] = vals
            df.loc[regular_mask & ~has_data, f"{col_prefix}_avg_playoffs"] = np.nan
    # Bye odds (seeds 1-2)
    bye_cols = [f"{col_prefix}_1_seed", f"{col_prefix}_2_seed"]
    bye_cols = [c for c in bye_cols if c in df.columns]
    if bye_cols:
        df_reg = df.loc[regular_mask, bye_cols]
        if not df_reg.empty:
            vals = df_reg.fillna(0.0).sum(axis=1)
            has_data = df_reg.notna().any(axis=1)
            df.loc[regular_mask, f"{col_prefix}_avg_bye"] = vals
            df.loc[regular_mask & ~has_data, f"{col_prefix}_avg_bye"] = np.nan
    return df


@ensure_normalized
def calculate_opponent_rank_percentile(df: pd.DataFrame, regular_mask: pd.Series) -> pd.DataFrame:
    """
    Calculate simple per-week opponent difficulty rank and percentile.
    Args:
        df: DataFrame with opponent_points
        regular_mask: Boolean mask for regular season rows
    Returns:
        DataFrame with opp_pts_week_rank and opp_pts_week_pct added
    """
    df = df.copy()
    if "opponent_points" in df.columns:
        idx = regular_mask & df["opponent_points"].notna()
        grouped = df.loc[idx].groupby(["year", "week"])["opponent_points"]
        # Rank: 1 = hardest opponent (highest opponent_points)
        df.loc[idx, "opp_pts_week_rank"] = grouped.rank(method="min", ascending=False).astype("Int64")
        # Percentile: 100 = hardest
        df.loc[idx, "opp_pts_week_pct"] = (100.0 * grouped.rank(pct=True, ascending=True)).round(2)
    return df


@ensure_normalized
def lock_postseason_to_final_week(df: pd.DataFrame, regular_mask: pd.Series, col_list: list[str]) -> pd.DataFrame:
    """
    Copy final regular season values to playoff/consolation rows AND bye weeks.

    Ensures that expected record columns freeze at the end of the regular season:
    - Playoff rows get values from final regular season week
    - Consolation rows get values from final regular season week
    - Bye week rows (during postseason) get values from final regular season week

    Args:
        df: Full DataFrame
        regular_mask: Mask for regular season rows
        col_list: List of columns to copy
    Returns:
        DataFrame with postseason rows locked
    """
    df = df.copy()

    # Postseason includes: playoffs, consolation, AND bye weeks during postseason
    post_mask = (df["is_playoffs"] == 1) | (df["is_consolation"] == 1)

    if not post_mask.any():
        return df

    cols_to_copy = [c for c in col_list if c in df.columns]

    for year in sorted(df["year"].dropna().unique()):
        year = int(year)
        df_reg_year = df[(df["year"] == year) & regular_mask]
        if df_reg_year.empty:
            continue

        last_wk = current_regular_week(df_reg_year)
        if last_wk == 0:
            continue

        final_rows = df_reg_year[df_reg_year["week"] == last_wk]
        if final_rows.empty:
            continue

        # Use franchise_id for matching when available (stable across name changes)
        _id_col = get_manager_col(df)

        # Get all managers for this year
        all_managers = set(df[df["year"] == year][_id_col].unique())

        # Find all postseason weeks (any week after regular season)
        all_weeks = sorted(df[df["year"] == year]["week"].dropna().unique())
        postseason_weeks = [w for w in all_weeks if w > last_wk]

        # For each manager, copy final regular season values to ALL postseason weeks
        for mgr in all_managers:
            src = final_rows[final_rows[_id_col] == mgr]
            if src.empty:
                continue
            src_row = src.iloc[0]

            # Update all postseason rows for this manager
            idx_post = df.index[(df["year"] == year) & (df[_id_col] == mgr) & (df["week"] > last_wk)]

            for idx in idx_post:
                for c in cols_to_copy:
                    df.at[idx, c] = src_row[c]

    return df
