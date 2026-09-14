#!/usr/bin/env python3
"""
Expected Record Transformation

Calculates schedule-independent expected records by simulating random schedules.

RECALCULATE WEEKLY: All columns in this module must be recalculated every week.
"""

import sys

_verbose = "--verbose" in sys.argv
import argparse
import random
import re
import numpy as np
from functools import wraps
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

from core.data_normalization import normalize_numeric_columns, ensure_league_id
from core.league_context import LeagueContext
from multi_league.transformations.matchup.modules.schedule_simulation import (
    current_regular_week,
    calculate_opponent_rank_percentile,
    lock_postseason_to_final_week,
)
from multi_league.core.identity import get_manager_col
from multi_league.transformations.matchup.modules.bye_week_filler import fill_bye_weeks, validate_bye_week_coverage
from multi_league.transformations.matchup.modules.sql_aggregations import (
    get_points_dicts_for_week,
    build_simulation_results_df,
    apply_simulation_results,
    get_final_regular_week_by_year,
    run_simulations_vectorized,
)


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
            if league_id is not None and pd.notna(league_id):
                result = ensure_league_id(result, league_id)

        return result

    return wrapper


# =========================================================
# Configuration Constants
# =========================================================
N_SIMS = 100000
RNG_SEED = None  # None for random, int for reproducibility

# Columns to add/update
# Seed columns (shuffle_N_seed, opp_shuffle_N_seed) are created dynamically
# by apply_simulation_results() based on actual team count — no pre-declaration needed.

# Base win columns - extended dynamically for H2H+Median (up to 36 for 18-week season)
SHUFFLE_WIN_COLS = [f"shuffle_{w}_win" for w in range(0, 37)]
SHUFFLE_SUMMARY_COLS = [
    "shuffle_avg_wins",
    "shuffle_avg_seed",
    "shuffle_avg_playoffs",
    "shuffle_avg_bye",
    "wins_vs_shuffle_wins",
    "seed_vs_shuffle_seed",
]

OPP_SHUFFLE_WIN_COLS = [f"opp_shuffle_{w}_win" for w in range(0, 37)]
OPP_SHUFFLE_SUMMARY_COLS = [
    "opp_shuffle_avg_wins",
    "opp_shuffle_avg_seed",
    "opp_shuffle_avg_playoffs",
    "opp_shuffle_avg_bye",
    "wins_vs_opp_shuffle_wins",
    "seed_vs_opp_shuffle_seed",
]

OPP_RANK_COLS = ["opp_pts_week_rank", "opp_pts_week_pct"]


# =========================================================
# H2H+Median Detection
# =========================================================
def detect_median_scoring(
    data_directory: Path | None = None,
    year: int | None = None,
    settings_by_year: dict[int, dict] | None = None,
) -> bool:
    """
    Detect if league uses H2H + Median scoring for a specific year.

    Canonical source of truth is the flat ``public.league_settings`` table.
    Local JSON files are only a legacy fallback when canonical settings rows
    are not directly available.

    Args:
        data_directory: Path to league data directory
        year: Specific year to check. If None, returns True if any year enables median.

    Returns:
        True if league uses H2H + Median scoring, False otherwise
    """
    import json

    if settings_by_year:
        years_to_check = [int(year)] if year is not None else sorted(int(y) for y in settings_by_year)
        for target_year in years_to_check:
            row = settings_by_year.get(target_year) or settings_by_year.get(str(target_year)) or {}
            if bool(row.get("uses_median")):
                if _verbose:
                    print(f"  [INFO] Detected H2H + Median from canonical league_settings row for {target_year}")
                return True

    if not data_directory:
        return False

    data_path = Path(data_directory)
    duckdb_files = list(data_path.glob("*.duckdb"))
    if duckdb_files:
        try:
            import duckdb as _ddb

            with _ddb.connect(str(duckdb_files[0]), read_only=True) as conn:
                exists = conn.execute(
                    "SELECT COUNT(*) FROM information_schema.tables "
                    "WHERE table_schema = 'public' AND table_name = 'league_settings'"
                ).fetchone()[0]
                if exists:
                    if year is not None:
                        row = conn.execute(
                            "SELECT COALESCE(uses_median, FALSE) FROM public.league_settings WHERE year = ? LIMIT 1",
                            [int(year)],
                        ).fetchone()
                        return bool(row and row[0])
                    row = conn.execute(
                        "SELECT COUNT(*) FROM public.league_settings WHERE COALESCE(uses_median, FALSE)"
                    ).fetchone()
                    return bool(row and row[0] > 0)
        except Exception:
            pass

    settings_dir = data_path / "league_settings"
    if not settings_dir.exists():
        return False

    def extract_year(filename: str) -> int | None:
        match = re.search(r"league_settings_(\d{4})(?:_|\.json$)", filename)
        return int(match.group(1)) if match else None

    files_by_year: dict[int, Path] = {}
    for json_file in settings_dir.glob("league_settings_*.json"):
        file_year = extract_year(json_file.name)
        if file_year is not None:
            files_by_year[file_year] = json_file

    years_to_check = [int(year)] if year is not None else sorted(files_by_year)
    for target_year in years_to_check:
        target_file = files_by_year.get(target_year)
        if not target_file:
            continue
        try:
            with open(target_file, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(data, dict) and bool(data.get("uses_median")):
            if _verbose:
                print(f"  [INFO] Detected H2H + Median from {target_file.name} (uses_median=true)")
            return True

    return False


# =========================================================
# Main Transformation Function
# =========================================================


@ensure_normalized
def calculate_expected_records(
    matchup_df: pd.DataFrame,
    current_week: int | None = None,
    current_year: int | None = None,
    n_sims: int = N_SIMS,
    rng_seed: int | None = RNG_SEED,
    data_directory: Path | None = None,
    settings_by_year: dict[int, dict] | None = None,
) -> pd.DataFrame:
    """
    Calculate expected records using schedule simulations.

    Runs two types of simulations:
    1. Performance-based: Shuffle actual scores across random schedules
    2. Opponent difficulty: Shuffle opponent difficulty to measure schedule luck

    For H2H+Median leagues, simulations include both head-to-head matchup wins
    AND median wins (above league median = additional win).

    Args:
        matchup_df: DataFrame with matchup data
        current_week: Current week (optional, for partial season updates)
        current_year: Current year (optional)
        n_sims: Number of Monte Carlo simulations (default: 100,000)
        rng_seed: Random seed for reproducibility (None = random)
        data_directory: Path to league data directory (for detecting H2H+Median)

    Returns:
        DataFrame with expected record columns added
    """
    # Detect if league uses H2H + Median scoring (fallback - per-year detection from matchup data takes precedence)
    use_median = (
        detect_median_scoring(
            data_directory,
            year=current_year,
            settings_by_year=settings_by_year,
        )
        if (data_directory or settings_by_year)
        else False
    )
    scoring_type = "H2H + Median" if use_median else "H2H"
    print(
        f"Calculating expected records ({n_sims:,} simulations, default={scoring_type}, per-year from matchup data)..."
    )

    df = matchup_df.copy()

    # Use numpy RNG seed (vectorized simulation uses numpy, not stdlib random)
    # If no seed provided, generate one for reproducibility during this run
    np_rng_seed = rng_seed if rng_seed is not None else random.randint(0, 2**31)

    # Normalize critical columns to avoid NA comparison issues
    # Year/week must be integers; is_playoffs/is_consolation must be 0/1
    for col in ["year", "week"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    for col in ["is_playoffs", "is_consolation", "is_bye_week"]:
        if col in df.columns:
            # Boolean columns (native or nullable) from MotherDuck can't fillna(0);
            # convert via object to break out of boolean dtype first
            df[col] = df[col].astype(object).fillna(0).astype(int)

    # Filter out rows with NA year or week (invalid data)
    valid_mask = df["year"].notna() & df["week"].notna()
    if not valid_mask.all():
        n_invalid = (~valid_mask).sum()
        print(f"  WARNING: Filtering out {n_invalid} rows with invalid/NA year/week values")
        df = df[valid_mask].copy()
    # Convert to plain int now that NAs are gone (avoids pd.NA issues in sorted())
    for col in ["year", "week"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    # Initialize fixed columns (win/summary/rank) — seed columns are created
    # dynamically by apply_simulation_results() based on actual team count
    init_cols = (
        SHUFFLE_WIN_COLS + SHUFFLE_SUMMARY_COLS + OPP_SHUFFLE_WIN_COLS + OPP_SHUFFLE_SUMMARY_COLS + OPP_RANK_COLS
    )

    # Find columns that need to be added
    new_cols = [col for col in init_cols if col not in df.columns]
    if new_cols:
        new_col_df = pd.DataFrame(np.nan, index=df.index, columns=new_cols)
        df = pd.concat([df, new_col_df], axis=1)

    # Regular season mask
    regular_mask = (df["is_playoffs"] == 0) & (df["is_consolation"] == 0)

    # Clear all shuffle/opp_shuffle fields for non-regular rows (discover from data)
    shuffle_cols = [c for c in df.columns if c.startswith(("shuffle_", "opp_shuffle_", "opp_pts_week_"))]
    df.loc[~regular_mask, shuffle_cols] = np.nan

    # Process each season
    seasons = sorted(df[regular_mask]["year"].dropna().unique().astype(int))

    for year in seasons:
        if _verbose:
            print(f"\nProcessing season {year}...")
        df_season = df[df["year"] == year].copy()
        df_reg_season = df_season[df_season["is_playoffs"] == 0]

        if df_reg_season.empty:
            if _verbose:
                print(f"  Skipping {year} - no regular season data")
            continue

        # Prefer franchise_id for stable identification when available
        _id_col = get_manager_col(df_reg_season)
        managers = tuple(sorted(df_reg_season[_id_col].unique()))
        n_managers = len(managers)

        # Odd team count: add a dummy "BYE" team so round-robin works.
        # The dummy gets each week's league-median score (neutral opponent).
        # After simulation, dummy results are stripped before writing back.
        _odd_team_padded = False
        _dummy_id = "__BYE_DUMMY__"
        if n_managers % 2 != 0:
            _odd_team_padded = True
            managers = tuple(list(managers) + [_dummy_id])
            n_managers = len(managers)
            if _verbose:
                print(f"  Odd team count ({n_managers - 1}) - padding with dummy bye team")

        last_wk = current_regular_week(df_reg_season)
        if last_wk == 0:
            if _verbose:
                print(f"  Skipping {year} - no complete weeks")
            continue

        # PER-YEAR median detection from flat DDL settings (canonical source of truth)
        use_median_this_year = use_median  # Default to global detection
        if settings_by_year:
            yr_settings = settings_by_year.get(year) or settings_by_year.get(str(year)) or {}
            if isinstance(yr_settings, dict):
                use_median_this_year = bool(yr_settings.get("uses_median"))
            if _verbose and use_median_this_year != use_median:
                scoring_type_year = "H2H + Median" if use_median_this_year else "H2H"
                print(f"  [NOTE] Year {year} uses {scoring_type_year} scoring (from league_settings)")

        # Weighted standings not currently used (legacy weight_* columns deprecated)
        year_weights = None

        # PER-YEAR playoff bracket settings: num_playoff_teams and bye_teams
        # These columns are set by cumulative_records from league settings
        year_playoff_slots = 6  # Default
        year_bye_slots = 2  # Default
        if "num_playoff_teams" in df_reg_season.columns:
            npt = df_reg_season["num_playoff_teams"].dropna()
            if not npt.empty:
                year_playoff_slots = int(npt.iloc[0])
        if "num_bye_teams" in df_reg_season.columns:
            nbt = df_reg_season["num_bye_teams"].dropna()
            if not nbt.empty:
                year_bye_slots = int(nbt.iloc[0])
        else:
            # Fallback: calculate byes from bracket structure
            # Standard: next_power_of_2(N) - N
            next_p2 = 1 << (year_playoff_slots - 1).bit_length()
            year_bye_slots = next_p2 - year_playoff_slots

        if _verbose:
            print(
                f"  {n_managers} managers, {last_wk} complete weeks, {'H2H+Median' if use_median_this_year else 'H2H'}, {year_playoff_slots} playoff spots, {year_bye_slots} byes"
            )

        # Extract points data (include franchise_id when available for stable dict keying)
        points_cols = ["manager", "week", "team_points", "opponent_points"]
        if "franchise_id" in df_reg_season.columns:
            points_cols.insert(1, "franchise_id")
        df_points = df_reg_season[[c for c in points_cols if c in df_reg_season.columns]].copy()

        # Process each week
        for wk in range(1, last_wk + 1):
            if _verbose:
                print(f"    Processing week {wk}...", end="", flush=True)

            n_weeks = wk

            df_to_week = df_points[df_points["week"] <= n_weeks]

            # ===========================
            # PERFORMANCE-BASED SIMULATION (team_points)
            # ===========================
            # Optimized: Use vectorized dict building instead of iterrows
            points_perf, points_opp_raw = get_points_dicts_for_week(df_to_week, n_weeks)

            # Inject dummy bye team with zero points so it always finishes last
            # and absorbs the phantom Nth-seed position without displacing real teams
            if _odd_team_padded:
                for w_i in range(1, n_weeks + 1):
                    points_perf[(_dummy_id, w_i)] = 0.0

            # Use vectorized numpy simulation (10-20x faster than Python loop)
            win_hists_perf, seed_hists_perf = run_simulations_vectorized(
                points_perf,
                list(managers),
                n_weeks,
                n_sims,
                rng_seed=np_rng_seed + year * 1000 + wk,  # Unique seed per year/week
                mode="performance",
                use_median=use_median_this_year,
                standings_weights=year_weights,
            )

            # Strip dummy team from results
            if _odd_team_padded:
                win_hists_perf.pop(_dummy_id, None)
                seed_hists_perf.pop(_dummy_id, None)

            # Use real managers (excluding dummy) for result computation
            _real_managers = [m for m in managers if m != _dummy_id] if _odd_team_padded else managers

            # Calculate denominators
            win_den = {m: max(1, sum(win_hists_perf[m])) for m in _real_managers}
            seed_den = {m: max(1, sum(seed_hists_perf[m])) for m in _real_managers}

            # Calculate expected wins/seeds/playoffs/byes DIRECTLY from counts (no rounding)
            expected_wins_perf = {}
            expected_seed_perf = {}
            expected_playoffs_perf = {}
            expected_bye_perf = {}
            for mgr in _real_managers:
                # Expected wins = sum(wins * probability) = sum(wins * count) / total_count
                expected_wins_perf[mgr] = (
                    sum(wv * win_hists_perf[mgr][wv] for wv in range(len(win_hists_perf[mgr]))) / win_den[mgr]
                )
                # Expected seed = sum(seed * probability) = sum(seed * count) / total_count
                expected_seed_perf[mgr] = (
                    sum((s + 1) * seed_hists_perf[mgr][s] for s in range(len(seed_hists_perf[mgr]))) / seed_den[mgr]
                )
                # Playoff odds = probability of making playoffs (seeds 1 through num_playoff_teams)
                expected_playoffs_perf[mgr] = (
                    100.0
                    * sum(seed_hists_perf[mgr][s] for s in range(min(year_playoff_slots, len(seed_hists_perf[mgr]))))
                    / seed_den[mgr]
                )
                # Bye odds = probability of getting a bye (seeds 1 through num_bye_teams)
                if year_bye_slots > 0:
                    expected_bye_perf[mgr] = (
                        100.0
                        * sum(seed_hists_perf[mgr][s] for s in range(min(year_bye_slots, len(seed_hists_perf[mgr]))))
                        / seed_den[mgr]
                    )
                else:
                    expected_bye_perf[mgr] = 0.0

            # Write performance-based results using batch update (optimized)
            # Max wins for H2H+Median is 2*n_weeks (H2H win + median win per week)
            max_wins_this_week = (2 * n_weeks) if use_median_this_year else n_weeks
            _real_n = len(_real_managers)
            perf_results_df = build_simulation_results_df(
                managers=list(_real_managers),
                year=year,
                week=wk,
                win_hists=win_hists_perf,
                seed_hists=seed_hists_perf,
                expected_wins=expected_wins_perf,
                expected_seed=expected_seed_perf,
                expected_playoffs=expected_playoffs_perf,
                expected_bye=expected_bye_perf,
                max_wins=min(max_wins_this_week, 36),
                n_teams=_real_n,
                col_prefix="shuffle",
                id_col=_id_col,
            )

            # Apply results using batch merge
            week_mask = (df["year"] == year) & (df["week"] == wk) & regular_mask
            df = apply_simulation_results(df, perf_results_df, week_mask)

            # ===========================
            # OPPONENT DIFFICULTY SIMULATION (opponent_points)
            # ===========================
            # Optimized: Use pre-computed points_opp_raw from get_points_dicts_for_week
            points_opp = {k: v for k, v in points_opp_raw.items() if k[1] <= n_weeks}

            # Inject dummy bye team with zero opponent points for odd-team padding
            if _odd_team_padded and points_opp:
                for w_i in range(1, n_weeks + 1):
                    points_opp[(_dummy_id, w_i)] = 0.0

            if points_opp:  # Only if opponent_points available
                # Use vectorized numpy simulation (10-20x faster than Python loop)
                win_hists_opp, seed_hists_opp = run_simulations_vectorized(
                    points_opp,
                    list(managers),
                    n_weeks,
                    n_sims,
                    rng_seed=np_rng_seed + year * 1000 + wk + 500,  # Different seed than performance sim
                    mode="opponent",
                )

                # Strip dummy team from results
                if _odd_team_padded:
                    win_hists_opp.pop(_dummy_id, None)
                    seed_hists_opp.pop(_dummy_id, None)

                win_den_opp = {m: max(1, sum(win_hists_opp[m])) for m in _real_managers}
                seed_den_opp = {m: max(1, sum(seed_hists_opp[m])) for m in _real_managers}

                # Calculate expected wins/seeds/playoffs/byes DIRECTLY from counts (no rounding)
                expected_wins_opp = {}
                expected_seed_opp = {}
                expected_playoffs_opp = {}
                expected_bye_opp = {}
                for mgr in _real_managers:
                    # Expected wins = sum(wins * probability) = sum(wins * count) / total_count
                    expected_wins_opp[mgr] = (
                        sum(wv * win_hists_opp[mgr][wv] for wv in range(len(win_hists_opp[mgr]))) / win_den_opp[mgr]
                    )
                    # Expected seed = sum(seed * probability) = sum(seed * count) / total_count
                    expected_seed_opp[mgr] = (
                        sum((s + 1) * seed_hists_opp[mgr][s] for s in range(len(seed_hists_opp[mgr])))
                        / seed_den_opp[mgr]
                    )
                    # Playoff odds = probability of making playoffs (seeds 1 through num_playoff_teams)
                    expected_playoffs_opp[mgr] = (
                        100.0
                        * sum(seed_hists_opp[mgr][s] for s in range(min(year_playoff_slots, len(seed_hists_opp[mgr]))))
                        / seed_den_opp[mgr]
                    )
                    # Bye odds = probability of getting a bye (seeds 1 through num_bye_teams)
                    if year_bye_slots > 0:
                        expected_bye_opp[mgr] = (
                            100.0
                            * sum(seed_hists_opp[mgr][s] for s in range(min(year_bye_slots, len(seed_hists_opp[mgr]))))
                            / seed_den_opp[mgr]
                        )
                    else:
                        expected_bye_opp[mgr] = 0.0

                # Write opponent difficulty results using batch update (optimized)
                opp_results_df = build_simulation_results_df(
                    managers=list(_real_managers),
                    year=year,
                    week=wk,
                    win_hists=win_hists_opp,
                    seed_hists=seed_hists_opp,
                    expected_wins=expected_wins_opp,
                    expected_seed=expected_seed_opp,
                    expected_playoffs=expected_playoffs_opp,
                    expected_bye=expected_bye_opp,
                    max_wins=min(n_weeks, 14),
                    n_teams=_real_n,
                    col_prefix="opp_shuffle",
                    id_col=_id_col,
                )
                df = apply_simulation_results(df, opp_results_df, week_mask)

            if _verbose:
                print(" done")

    # ===========================
    # MARK FINAL REGULAR SEASON WEEK
    # ===========================
    # Mark which week is the last FULL regular season week for each year
    # This is the week where ALL managers played regular season (for end-of-season stats)
    if _verbose:
        print("\nMarking final regular season week for each year...")

    # Initialize column
    df["is_final_regular_week"] = 0

    # Optimized: Use vectorized final week detection instead of Python loop
    final_weeks_by_year = get_final_regular_week_by_year(df, settings_by_year=settings_by_year)

    for year, last_full_week in final_weeks_by_year.items():
        if last_full_week:
            # Mark this week for all managers in this year
            mask = (df["year"] == year) & (df["week"] == last_full_week) & (df["is_playoffs"] == 0)
            df.loc[mask, "is_final_regular_week"] = 1
            if _verbose:
                n_managers = len(df[(df["year"] == year) & (df["is_playoffs"] == 0)]["franchise_id"].unique())
                print(f"  {year}: Final regular season week = {last_full_week} ({n_managers} managers)")

    # ===========================
    # COMPUTE SUMMARY STATISTICS
    # ===========================
    # NOTE: avg_wins, avg_seed, avg_playoffs, avg_bye are now calculated DIRECTLY
    # from histogram counts in the main loop above (no rounding errors).
    # The calculate_summary_stats function is no longer needed for these values.
    if _verbose:
        print("\nSummary statistics already calculated from raw counts (skipping recalculation)...")

    # Actual vs expected deltas (performance-based)
    # wins_to_date already includes median outcomes for H2H+Median leagues.
    # Do not add cumulative above_league_median again here.
    if "wins_to_date" in df.columns and "shuffle_avg_wins" in df.columns:
        actual_wins = pd.to_numeric(df["wins_to_date"], errors="coerce")

        mask = regular_mask & df["wins_to_date"].notna() & df["shuffle_avg_wins"].notna()
        df.loc[mask, "wins_vs_shuffle_wins"] = (
            actual_wins.loc[mask] - pd.to_numeric(df.loc[mask, "shuffle_avg_wins"], errors="coerce")
        ).round(2)

    if "playoff_seed_to_date" in df.columns and "shuffle_avg_seed" in df.columns:
        mask = regular_mask & df["playoff_seed_to_date"].notna() & df["shuffle_avg_seed"].notna()
        df.loc[mask, "seed_vs_shuffle_seed"] = (
            pd.to_numeric(df.loc[mask, "playoff_seed_to_date"], errors="coerce")
            - pd.to_numeric(df.loc[mask, "shuffle_avg_seed"], errors="coerce")
        ).round(2)

    # Actual vs expected deltas (opponent difficulty-based)
    if "wins_to_date" in df.columns and "opp_shuffle_avg_wins" in df.columns:
        mask = regular_mask & df["wins_to_date"].notna() & df["opp_shuffle_avg_wins"].notna()
        df.loc[mask, "wins_vs_opp_shuffle_wins"] = (
            actual_wins.loc[mask] - pd.to_numeric(df.loc[mask, "opp_shuffle_avg_wins"], errors="coerce")
        ).round(2)

    if "playoff_seed_to_date" in df.columns and "opp_shuffle_avg_seed" in df.columns:
        mask = regular_mask & df["playoff_seed_to_date"].notna() & df["opp_shuffle_avg_seed"].notna()
        df.loc[mask, "seed_vs_opp_shuffle_seed"] = (
            pd.to_numeric(df.loc[mask, "playoff_seed_to_date"], errors="coerce")
            - pd.to_numeric(df.loc[mask, "opp_shuffle_avg_seed"], errors="coerce")
        ).round(2)

    # Simple opponent difficulty rank/percentile
    df = calculate_opponent_rank_percentile(df, regular_mask)

    # ===========================
    # LOCK POSTSEASON VALUES
    # ===========================
    if _verbose:
        print("Locking postseason values...")

    # Discover all simulation columns from the data itself (fully dynamic)
    cols_to_lock = [c for c in df.columns if c.startswith(("shuffle_", "opp_shuffle_", "opp_pts_week_"))]

    df = lock_postseason_to_final_week(df, regular_mask, cols_to_lock)

    print("\nExpected records calculation complete!")
    print(f"Updated {len(df)} records with expected record simulations")

    # ===========================
    # FILL BYE WEEKS
    # ===========================
    # Add rows for teams with bye weeks (no opponent)
    # Weekly stats = 0, cumulative stats carried forward from previous week
    df = fill_bye_weeks(df)
    validate_bye_week_coverage(df)

    return df


# =========================================================
# CLI Interface
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="Calculate expected records using Monte Carlo schedule simulations")
    parser.add_argument("--context", type=str, help="Path to league_context.json")
    parser.add_argument(
        "--db", type=str, help="MotherDuck database name (alternative to --context, reads/writes directly)"
    )
    parser.add_argument("--current-week", type=int, help="Current week number (for weekly updates)")
    parser.add_argument("--current-year", type=int, help="Current year (for weekly updates)")
    parser.add_argument("--n-sims", type=int, default=N_SIMS, help=f"Number of simulations (default: {N_SIMS:,})")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility (default: None/random)")
    parser.add_argument(
        "--data-dir", type=str, default=None, help="Path to local DuckDB directory (runs locally instead of MotherDuck)"
    )

    args = parser.parse_args()

    if not args.context and not args.db:
        parser.error("Either --context or --db is required")

    # Auto-convert --context to --db (context is legacy)
    if args.context and not args.db:
        ctx = LeagueContext.load_readonly(args.context)
        print(f"Loaded league context: {ctx.league_name}")
        from multi_league.core.db_utils import get_db_name

        args.db = get_db_name(ctx)
        print(f"Auto-converting --context to --db {args.db}")

    # ── Direct database mode (local artifact or Fly) ──
    if args.db:
        from multi_league.core.db_context import DbContext

        db = DbContext(args.db, data_dir=args.data_dir)
        connection_target = "local DuckDB" if args.data_dir else "Fly DuckDB"
        print(f"Connected to {connection_target}: {args.db} ({db.league_name})")

        # Surgical read — only columns the sim needs
        needed_cols = [
            "year",
            "week",
            "manager",
            "franchise_id",
            "team_points",
            "opponent",
            "opponent_points",
            "is_playoffs",
            "is_consolation",
            "is_bye_week",
            "wins_to_date",
            "playoff_seed_to_date",
            "above_league_median",
            # Per-year bracket settings (median detection uses settings_by_year)
            "num_playoff_teams",
            "num_bye_teams",
        ]
        available = {r[0] for r in db.execute("DESCRIBE public.matchup").fetchall()}
        select_cols = [c for c in needed_cols if c in available]

        matchup_df = db.read_table("matchup", columns=", ".join(select_cols))
        print(
            f"Loaded {len(matchup_df)} matchup records ({len(select_cols)} columns) "
            f"from {connection_target}"
        )

        if matchup_df.empty:
            print("[FAIL] matchup table is empty — nothing to calculate")
            db.close()
            return

        # Settings dir for detect_median_scoring fallback
        settings_by_year = db.get_settings()

        enriched_df = calculate_expected_records(
            matchup_df,
            current_week=args.current_week,
            current_year=args.current_year,
            n_sims=args.n_sims,
            rng_seed=args.seed,
            data_directory=db.data_directory,
            settings_by_year=settings_by_year,
        )

        # Surgical write — only the columns the sim produced (no DROP TABLE)
        input_set = set(select_cols)
        output_cols = [c for c in enriched_df.columns if c not in input_set]
        key_cols = ["year", "week", "manager"]

        db.update_columns("matchup", enriched_df, key_cols=key_cols, update_cols=output_cols)
        print(f"\n[OK] Updated {len(output_cols)} columns on {len(enriched_df)} rows in {args.db}.public.matchup")
        db.close()
        return


if __name__ == "__main__":
    main()
