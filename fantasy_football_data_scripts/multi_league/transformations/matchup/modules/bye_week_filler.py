"""
Bye Week Filler Module

Fills in missing bye week rows in matchup data with appropriate values:
- Weekly stats: NaN (no game played - ensures excluded from aggregations)
- Cumulative stats: Carried forward from previous week
"""

import pandas as pd
from pathlib import Path
import numpy as np
import logging

logger = logging.getLogger(__name__)


def fill_bye_weeks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill in missing bye week rows for teams.

    For bye weeks, creates rows with:
    - opponent = None (no opponent)
    - All game-specific columns = NaN (excluded from aggregations)
    - Cumulative stats: Carried forward from previous week
    - Playoff odds: Carried forward from previous week

    Using NaN (not 0) is critical - 0 would count as a loss in win/loss columns
    and would incorrectly affect averages/counts in pandas aggregations.

    Args:
        df: Matchup DataFrame

    Returns:
        DataFrame with bye week rows added (original if empty or invalid)
    """
    print("\n[BYE WEEKS] Filling in missing bye week rows...")

    # === INPUT VALIDATION ===
    if df is None or df.empty:
        logger.warning("[BYE WEEKS] Empty DataFrame provided, returning as-is")
        print("[BYE WEEKS] Empty DataFrame provided, returning as-is")
        return df if df is not None else pd.DataFrame()

    # Check required columns exist
    required_cols = ["year", "week", "manager"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.error(f"[BYE WEEKS] Missing required columns: {missing_cols}")
        print(f"[BYE WEEKS] ERROR: Missing required columns: {missing_cols}")
        return df

    # DYNAMIC: Detect column categories from actual DataFrame columns
    # This makes the solution generic for any league size, week count, etc.
    all_cols = set(df.columns)

    # Weekly stat columns: Set to NaN for bye weeks (no game played)
    # These are game-specific columns that should NOT count in aggregations
    # Using NaN ensures pandas sum(), mean(), count() skip these rows
    #
    # NOTE: We intentionally KEEP cumulative/season stats (wins_to_date, shuffle_avg_wins, etc.)
    # and playoff odds (p_playoffs, p_champ, etc.) - those should carry forward
    weekly_stat_patterns = [
        # === Core game results ===
        "win",
        "loss",
        "tie",
        "team_points",
        "opponent_points",
        "margin",
        "total_matchup_score",
        "close_margin",
        # === Projections ===
        "team_projected_points",
        "opponent_projected_points",
        "proj_score_error",
        "abs_proj_score_error",
        "above_proj_score",
        "below_proj_score",
        "proj_wins",
        "proj_losses",
        # === Spread/odds ===
        "expected_spread",
        "expected_odds",
        "win_vs_spread",
        "lose_vs_spread",
        "underdog_wins",
        "favorite_losses",
        "win_probability",
        # === Weekly rankings ===
        "weekly_rank",
        "teams_beat_this_week",
        "opponent_teams_beat_this_week",
        "above_league_median",
        "below_league_median",
        "above_opponent_median",
        "below_opponent_median",
        # === Optimal lineup ===
        "optimal_points",
        "optimal_win",
        "optimal_loss",
        "bench_points",
        "lineup_efficiency",
        # === Grades ===
        "gpa",
        "grade",
        # === Drama/critical matchup ===
        "drama_score",
        "is_critical_matchup",
        "is_dramatic_win",
        "is_dramatic_loss",
        "max_odds_swing",
    ]
    weekly_stat_cols = [col for col in weekly_stat_patterns if col in all_cols]

    # Cumulative stat columns: Carry forward from previous week (no change during bye)
    # Pattern: columns with "_to_date", "avg_", "vs_" that accumulate over season
    cumulative_stat_patterns = [
        # === Cumulative records ===
        "wins_to_date",
        "losses_to_date",
        "points_scored_to_date",
        "points_against_to_date",
        # === Shuffle simulation averages ===
        "shuffle_avg_wins",
        "shuffle_avg_seed",
        "shuffle_avg_playoffs",
        "shuffle_avg_bye",
        "wins_vs_shuffle_wins",
        "seed_vs_shuffle_seed",
        "opp_shuffle_avg_wins",
        "opp_shuffle_avg_seed",
        "opp_shuffle_avg_playoffs",
        "opp_shuffle_avg_bye",
        # === Playoff odds (carry forward during playoffs) ===
        "p_playoffs",
        "p_bye",
        "p_semis",
        "p_final",
        "p_champ",
        "power_rating",
        "avg_seed",
        "exp_final_wins",
        "exp_final_pf",
        # === Season averages ===
        "manager_season_mean",
        "manager_season_median",
        "personal_season_mean",
        "personal_season_median",
        "season_mean",
        "season_median",
        # === Streaks ===
        "winning_streak",
        "losing_streak",
    ]
    cumulative_stat_cols = [col for col in cumulative_stat_patterns if col in all_cols]

    # DYNAMIC: Shuffle probability columns (carry forward)
    # Pattern: "shuffle_N_seed", "shuffle_N_win", "opp_shuffle_N_seed", "opp_shuffle_N_win"
    # Detect these dynamically to support any league size (10, 12, 14 teams, etc.)
    shuffle_prob_cols = [
        col
        for col in all_cols
        if (col.startswith("shuffle_") and (col.endswith("_seed") or col.endswith("_win")))
        or (col.startswith("opp_shuffle_") and (col.endswith("_seed") or col.endswith("_win")))
    ]

    # DYNAMIC: Seed probability columns (carry forward)
    # Pattern: "x1_seed", "x2_seed", etc. - probability of finishing at each seed
    seed_prob_cols = [col for col in all_cols if col.startswith("x") and col.endswith("_seed") and col[1:-5].isdigit()]

    # DYNAMIC: Win probability columns (carry forward)
    # Pattern: "x0_win", "x1_win", etc. - probability of finishing with that many wins
    win_prob_cols = [col for col in all_cols if col.startswith("x") and col.endswith("_win") and col[1:-4].isdigit()]

    print(f"  Detected {len(weekly_stat_cols)} weekly stat columns (set to NaN)")
    print(f"  Detected {len(cumulative_stat_cols)} cumulative stat columns (carry forward)")
    print(f"  Detected {len(shuffle_prob_cols)} shuffle probability columns (carry forward)")
    print(f"  Detected {len(seed_prob_cols)} seed probability columns (carry forward)")
    print(f"  Detected {len(win_prob_cols)} win probability columns (carry forward)")

    # Track new rows
    bye_week_rows = []
    initial_count = len(df)

    # Process each season
    seasons = sorted(df["year"].unique())
    for year in seasons:
        df_year = df[df["year"] == year].copy()

        # Get all managers and weeks in this season
        all_managers = sorted(df_year["franchise_id"].unique())
        all_weeks = sorted(df_year["week"].unique())

        if not all_managers or not all_weeks:
            continue

        print(f"  {year}: {len(all_managers)} managers, weeks {min(all_weeks)}-{max(all_weeks)}")

        # Get all weeks in this season (regular + playoffs + consolation)
        all_season_weeks = set(df_year["week"].dropna().unique())
        if not all_season_weeks:
            logger.debug(f"  {year}: No weeks found, skipping")
            continue

        # Identify regular season weeks for special handling
        regular_season_mask = (df_year["is_playoffs"] != 1) & (df_year["is_consolation"] != 1)
        if "is_playoffs" not in df_year.columns:
            regular_season_mask = pd.Series(True, index=df_year.index)

        regular_season_weeks = set(df_year[regular_season_mask]["week"].dropna().unique())
        final_regular_week = max(regular_season_weeks) if regular_season_weeks else max(all_season_weeks)

        # Check each manager for missing weeks
        # IMPORTANT: Only fill bye weeks for regular season. Eliminated teams
        # should NOT get placeholder rows for postseason weeks they don't play in.
        # Postseason bye rows should only exist for teams with a real bye (e.g.,
        # top seeds with first-round byes in the playoff bracket).
        postseason_weeks = all_season_weeks - regular_season_weeks

        for manager in all_managers:
            manager_data = df_year[df_year["franchise_id"] == manager].sort_values("week")

            if manager_data.empty:
                logger.warning(f"  {year}: Manager '{manager}' has no data rows")
                continue

            manager_weeks = set(manager_data["week"].dropna().tolist())

            # Regular season: fill ALL missing weeks (true bye weeks)
            missing_regular = [w for w in regular_season_weeks if w not in manager_weeks]

            # Postseason: only fill BETWEEN postseason games (real bracket byes),
            # not after the team's last game (eliminated teams sitting out).
            # Example: top seed has bye week 15, plays week 16-17 → fill week 15
            # Example: team loses week 15, no game 16-17 → don't fill 16-17
            manager_postseason_weeks = sorted(w for w in manager_weeks if w in postseason_weeks)
            if manager_postseason_weeks:
                last_postseason_game = max(manager_postseason_weeks)
                # Only fill gaps BEFORE their last postseason game (real byes)
                missing_postseason = [
                    w for w in postseason_weeks if w not in manager_weeks and w < last_postseason_game
                ]
            else:
                missing_postseason = []

            missing_weeks = sorted(missing_regular + missing_postseason)

            if not missing_weeks:
                continue

            # Get reference row for this manager (most recent week)
            ref_row = manager_data.iloc[-1].to_dict() if len(manager_data) > 0 else None
            if not ref_row:
                continue

            print(f"    {manager}: Missing weeks {missing_weeks}")

            # Create bye week rows
            for bye_week in sorted(missing_weeks):
                # Start with a copy of the reference row
                bye_row = ref_row.copy()

                # Update week
                bye_row["week"] = bye_week

                # Set opponent to None
                bye_row["opponent"] = None

                # Determine if this is a postseason week
                is_postseason_week = bye_week > final_regular_week

                # Update composite key columns for the new week
                # cumulative_week format: year * 100 + week (e.g., 2024 week 15 = 202415)
                new_cumulative_week = int(year) * 100 + int(bye_week)
                bye_row["cumulative_week"] = new_cumulative_week

                # manager_week / manager_year_week format: manager (no spaces) + cumulative_week
                manager_clean = str(manager).replace(" ", "")
                new_manager_week = f"{manager_clean}{new_cumulative_week}"
                if "manager_week" in bye_row:
                    bye_row["manager_week"] = new_manager_week
                if "manager_year_week" in bye_row:
                    bye_row["manager_year_week"] = new_manager_week

                # matchup_key format: "{team1}__vs__{team2}__{year}__{week}" - but no opponent
                # Set to a unique key for this placeholder row
                bye_row["matchup_key"] = f"{manager}__bye__{year}__{bye_week}"
                if "matchup_id" in bye_row:
                    bye_row["matchup_id"] = np.nan  # Keep numeric dtype; matchup_key already identifies bye rows

                # opponent_week and opponent_team should be None since no opponent
                if "opponent_week" in bye_row:
                    bye_row["opponent_week"] = None
                if "opponent_team" in bye_row:
                    bye_row["opponent_team"] = None

                # Set ALL weekly stats to NaN for bye weeks
                # This ensures bye weeks don't count as games played, wins, or losses
                # Using NaN (not 0) is critical - 0 would count as a loss in win/loss columns
                for col in weekly_stat_cols:
                    if col in bye_row:
                        bye_row[col] = np.nan

                # For cumulative stats, carry forward from appropriate source
                if is_postseason_week:
                    # For postseason weeks, use FINAL REGULAR SEASON values
                    # This ensures expected record stats freeze at end of regular season
                    source_week_data = df_year[
                        (df_year["franchise_id"] == manager) & (df_year["week"] == final_regular_week)
                    ]
                else:
                    # For regular season bye weeks, use previous week
                    source_week_data = df_year[
                        (df_year["franchise_id"] == manager) & (df_year["week"] < bye_week)
                    ].sort_values("week", ascending=False)

                if len(source_week_data) > 0:
                    source_row = source_week_data.iloc[0]

                    # Carry forward cumulative stats
                    for col in cumulative_stat_cols:
                        if col in source_row and pd.notna(source_row[col]):
                            bye_row[col] = source_row[col]

                    # Carry forward shuffle probabilities
                    for col in shuffle_prob_cols:
                        if col in source_row and pd.notna(source_row[col]):
                            bye_row[col] = source_row[col]

                    # Carry forward seed probabilities (x1_seed, x2_seed, etc.)
                    for col in seed_prob_cols:
                        if col in source_row and pd.notna(source_row[col]):
                            bye_row[col] = source_row[col]

                    # Carry forward win probabilities (x0_win, x1_win, etc.)
                    for col in win_prob_cols:
                        if col in source_row and pd.notna(source_row[col]):
                            bye_row[col] = source_row[col]

                # is_bye_week is the authoritative signal for phantom/bye rows.
                bye_row["is_bye_week"] = 1

                # Set postseason flag for postseason bye weeks (e.g., top seed first-round bye)
                if is_postseason_week:
                    bye_row["postseason"] = 1
                    # Postseason byes are playoff byes (top seeds sit out round 1)
                    bye_row["is_playoffs"] = 1
                    bye_row["is_consolation"] = 0
                else:
                    # Regular season byes: NULL flags so they don't match any filter
                    bye_row["is_playoffs"] = np.nan
                    bye_row["is_consolation"] = np.nan

                # Ensure string columns that should be empty for bye weeks
                for str_col in ["season_result", "playoff_round", "consolation_round"]:
                    if str_col in bye_row:
                        bye_row[str_col] = ""

                # Preserve team identifiers and playoff flags
                # (already copied from ref_row)

                bye_week_rows.append(bye_row)

    # Add bye week rows to dataframe
    if bye_week_rows:
        df_byes = pd.DataFrame(bye_week_rows)
        df = pd.concat([df, df_byes], ignore_index=True)

        # Sort by year, week, manager
        df = df.sort_values(["year", "week", "manager"]).reset_index(drop=True)

        added_count = len(bye_week_rows)
        print(f"\n[BYE WEEKS] Added {added_count} bye week rows")
        print(f"[BYE WEEKS] Total rows: {initial_count} -> {len(df)} (+{added_count})")
    else:
        print("\n[BYE WEEKS] No bye weeks found")

    # Add is_bye_week column if it doesn't exist
    if "is_bye_week" not in df.columns:
        df["is_bye_week"] = 0
    else:
        df["is_bye_week"] = df["is_bye_week"].fillna(0).astype(int)

    return df


def validate_bye_week_coverage(df: pd.DataFrame) -> None:
    """
    Validate that all teams have rows for all weeks (including bye weeks).

    Args:
        df: Matchup DataFrame with bye weeks filled
    """
    print("\n[BYE WEEKS] Validating coverage...")

    seasons = sorted(df["year"].unique())
    for year in seasons:
        df_year = df[df["year"] == year]

        all_managers = sorted(df_year["franchise_id"].unique())
        all_weeks = sorted(df_year["week"].unique())

        # Check each manager
        missing_found = False
        for manager in all_managers:
            manager_weeks = set(df_year[df_year["franchise_id"] == manager]["week"].tolist())
            missing_weeks = [w for w in all_weeks if w not in manager_weeks]

            if missing_weeks:
                print(f"  {year} - {manager}: Still missing weeks {missing_weeks}")
                missing_found = True

        if not missing_found:
            print(f"  {year}: OK - All {len(all_managers)} managers have rows for all {len(all_weeks)} weeks")


if __name__ == "__main__":
    # Test the module
    import sys
    import argparse
    from pathlib import Path

    # Add parent directory for imports
    SCRIPT_DIR = Path(__file__).parent.parent.parent.parent
    sys.path.insert(0, str(SCRIPT_DIR))

    parser = argparse.ArgumentParser(description="Test bye week filler")
    parser.add_argument("--db", required=True, help="Database name")
    test_args = parser.parse_args()

    from multi_league.core.db_reader import get_reader

    reader = get_reader()
    import pandas as pd

    rows = reader.query(f"SELECT * FROM public.matchup WHERE db_name = '{test_args.db}'", database="___leagues")
    df = pd.DataFrame(rows)

    print(f"Loaded {len(df)} matchup rows")

    # Fill bye weeks
    df_filled = fill_bye_weeks(df)

    # Validate
    validate_bye_week_coverage(df_filled)

    # Show example
    print("\nExample bye week row:")
    bye_rows = df_filled[df_filled["is_bye_week"] == 1]
    if len(bye_rows) > 0:
        cols = ["manager", "year", "week", "opponent", "team_points", "win", "shuffle_avg_wins", "is_bye_week"]
        print(bye_rows[cols].head(5).to_string(index=False))
