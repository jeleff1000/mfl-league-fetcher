"""
Clutch Score Calculator

Calculates championship equity contribution using weekly starter baselines.
Each player is compared to what started players at their position did THAT WEEK,
then credit/blame for odds changes is distributed proportionally.

Key Concepts:

    Weekly Starter Baseline (B_p,w):
        Average LAMAR of all started players at position p in week w.
        Not replacement level, not season average - this week's starter pool.
        FLEX handled automatically (baseline by position, not slot).

    Above-Baseline (G) and Below-Baseline (D):
        G = max(LAMAR - baseline, 0)  -- exceeded expectations
        D = max(baseline - LAMAR, 0)  -- fell short of expectations

        You can have positive LAMAR but still disappoint (L > 0 but L < B).
        You can have a monster game (L >> B).

    Credit Distribution (odds went UP):
        Only above-baseline players get credit.
        clutch = Δp × (G / team_total_G)

    Blame Distribution (odds went DOWN):
        Only below-baseline players get blamed.
        clutch = Δp × (D / team_total_D)  -- negative because Δp < 0

        Heroes in valiant losses (LAMAR above baseline) get 0 blame.

    Anchored to Reality:
        sum(clutch) = Δp for each team-week
        Every % of championship equity is accounted for.
"""

import pandas as pd
import numpy as np

from multi_league.core.identity import get_manager_col
from multi_league.core.player_identity import select_platform_player_id_column

# Import optimized SQL functions from matchup modules
try:
    from multi_league.transformations.matchup.modules.sql_aggregations import (
        get_weekly_starter_baseline_fast,
        get_weekly_odds_delta_fast,
        calculate_clutch_equity_fast,
        aggregate_season_clutch_fast,
    )

    _SQL_OPTIMIZATIONS_AVAILABLE = True
except ImportError:
    _SQL_OPTIMIZATIONS_AVAILABLE = False


def calculate_weekly_starter_baseline(player_df: pd.DataFrame, manager_col: str = "manager") -> pd.DataFrame:
    """
    Calculate weekly starter baseline LAMAR for each NATURAL position.

    Baseline = average LAMAR of all STARTED players at that natural position league-wide.

    Key design decisions:
    - Filter to STARTERS ONLY (fantasy_position not BN/IR) - bench players excluded
    - Group by NATURAL POSITION (position column: QB, RB, WR, TE, K, DEF)
    - This means FLEX players are compared to their natural position peers
      (RB in FLEX slot compared to all started RBs, not to other FLEX starters)

    Benefits:
    - Apples-to-apples: RBs compared to RBs, WRs to WRs
    - Larger sample size: all started RBs vs just RB1/RB2 slot
    - FLEX handled correctly: a RB in FLEX is still a RB

    Args:
        player_df: Player DataFrame with manager_lamar, position, fantasy_position
        manager_col: Column name for manager/team

    Returns:
        DataFrame with columns: year, week, position, starter_baseline_lamar
    """
    # Use optimized version if available
    if _SQL_OPTIMIZATIONS_AVAILABLE:
        baseline = get_weekly_starter_baseline_fast(player_df, lamar_col="manager_lamar")
        if not baseline.empty:
            # Show sample of baselines
            sample_year = baseline["year"].max()
            sample_week = baseline[baseline["year"] == sample_year]["week"].max()
            sample = baseline[(baseline["year"] == sample_year) & (baseline["week"] == sample_week)]
            print(f"   [Baseline] Week {sample_week}, {sample_year} starter baselines:")
            for _, row in sample.iterrows():
                print(f"      {row['position']}: {row['starter_baseline_lamar']:.2f} LAMAR")
        return baseline

    # Fallback to original implementation
    df = player_df.copy()

    # Validate required columns
    if "position" not in df.columns:
        raise ValueError("'position' column (natural position) not found in player DataFrame")

    # Filter to STARTERS ONLY - exclude bench, IR, and unrostered
    if "fantasy_position" in df.columns:
        started_mask = df["fantasy_position"].notna() & ~df["fantasy_position"].isin(["BN", "IR", "IR+", "TAXI"])
        df = df[started_mask].copy()
        print(f"   [Baseline] Filtered to {len(df):,} started player-weeks")
    else:
        print("   [WARN] No fantasy_position column - using all rows for baseline")

    if df.empty:
        print("[WARN] No started players found for baseline calculation")
        return pd.DataFrame(columns=["year", "week", "position", "starter_baseline_lamar"])

    # Group by year, week, and NATURAL POSITION (not lineup slot)
    # This ensures FLEX players are compared to their position peers
    baseline = df.groupby(["year", "week", "position"]).agg({"manager_lamar": "mean"}).reset_index()

    baseline.rename(columns={"manager_lamar": "starter_baseline_lamar"}, inplace=True)

    # Show sample of baselines
    if not baseline.empty:
        sample_year = baseline["year"].max()
        sample_week = baseline[baseline["year"] == sample_year]["week"].max()
        sample = baseline[(baseline["year"] == sample_year) & (baseline["week"] == sample_week)]
        print(f"   [Baseline] Week {sample_week}, {sample_year} starter baselines:")
        for _, row in sample.iterrows():
            print(f"      {row['position']}: {row['starter_baseline_lamar']:.2f} LAMAR")

    return baseline


def calculate_weekly_odds_delta(
    matchup_df: pd.DataFrame, odds_col: str = "p_champ", include_playoffs: bool = True
) -> pd.DataFrame:
    """
    Calculate weekly championship odds change for each manager.

    IMPORTANT: If p_champ_change column already exists (pre-calculated by playoff_scenarios.py),
    we use it directly. This ensures:
    1. NaN-safe handling for eliminated teams (they get 0 instead of NaN)
    2. Consistency with the odds calculations done in playoff_odds_import.py
    3. Zero-sum property is maintained (all odds changes sum to 0 per week)

    Args:
        matchup_df: Matchup DataFrame with manager, year, week, and odds columns
        odds_col: Column name for championship probability (default: 'p_champ')
        include_playoffs: If True, include playoff weeks in clutch calculation.
                         Playoff clutch is arguably MORE important than regular season.
                         Default: True (include playoffs)

    Returns:
        DataFrame with columns: manager, year, week, odds_before, odds_after,
                               odds_delta, is_win, is_playoffs
    """
    df = matchup_df.copy()

    # Check if pre-calculated change column exists
    change_col = f"{odds_col}_change"
    if change_col in df.columns:
        print(f"   [Clutch] Using pre-calculated {change_col} column (NaN-safe)")
        # Use the pre-calculated change directly - it's already NaN-safe
        result = df[["manager", "year", "week"]].copy()
        # Fill NaN with 0 for eliminated teams (who have NaN p_champ_change)
        result["odds_delta"] = df[change_col].fillna(0.0)

        # Add odds_before/after if they exist (for debugging)
        prev_col = f"{odds_col}_prev"
        if prev_col in df.columns:
            result["odds_before"] = df[prev_col].fillna(0.0)
        else:
            result["odds_before"] = np.nan
        result["odds_after"] = df[odds_col].fillna(0.0)

        # Calculate win/loss if we have the columns
        if "team_points" in df.columns and "opponent_points" in df.columns:
            result["is_win"] = (df["team_points"] > df["opponent_points"]).astype(int)
        else:
            result["is_win"] = None

        # Track playoff status
        if "is_playoffs" in df.columns:
            result["is_playoffs"] = df["is_playoffs"].fillna(0).astype(int)
        else:
            result["is_playoffs"] = 0

        return result[["manager", "year", "week", "odds_before", "odds_after", "odds_delta", "is_win", "is_playoffs"]]

    # Fallback: Use optimized version if available
    if _SQL_OPTIMIZATIONS_AVAILABLE:
        result = get_weekly_odds_delta_fast(matchup_df, odds_col=odds_col)
        # Add is_playoffs column if not present
        if "is_playoffs" not in result.columns:
            if "is_playoffs" in matchup_df.columns:
                playoffs_map = matchup_df.set_index(["franchise_id", "year", "week"])["is_playoffs"].to_dict()
                result["is_playoffs"] = result.apply(
                    lambda r: playoffs_map.get((r["franchise_id"], r["year"], r["week"]), 0), axis=1
                )
            else:
                result["is_playoffs"] = 0
        return result

    # Fallback to original implementation
    # Ensure required columns exist
    required_cols = ["manager", "year", "week", odds_col]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Optionally filter to regular season only
    # NOTE: Default is now to INCLUDE playoffs - clutch performances in playoffs matter!
    if not include_playoffs and "is_playoffs" in df.columns:
        df = df[df["is_playoffs"] == 0].copy()

    # Track playoff status for output
    if "is_playoffs" not in df.columns:
        df["is_playoffs"] = 0

    # Determine manager column: prefer franchise_id for consistent career tracking
    group_col = get_manager_col(df)

    # Sort by manager/franchise, year, week to calculate deltas
    df = df.sort_values([group_col, "year", "week"]).reset_index(drop=True)

    # Calculate win/loss if we have the columns
    if "team_points" in df.columns and "opponent_points" in df.columns:
        df["is_win"] = (df["team_points"] > df["opponent_points"]).astype(int)
    else:
        df["is_win"] = None

    # Get odds before (previous week's ending odds = this week's starting odds)
    df["odds_before"] = df.groupby([group_col, "year"])[odds_col].shift(1)

    # For first week, estimate baseline as 1/num_teams
    first_week_mask = df["odds_before"].isna()
    if first_week_mask.any():
        num_teams_by_year = df.groupby("year")["franchise_id"].nunique()
        for year, num_teams in num_teams_by_year.items():
            year_first_week = first_week_mask & (df["year"] == year)
            df.loc[year_first_week, "odds_before"] = 100.0 / num_teams

    # Current week's odds are the "after"
    df["odds_after"] = df[odds_col].fillna(0.0)  # NaN-safe for eliminated teams

    # Calculate delta with NaN-safety
    # If either value is NaN, treat the change as 0 (for eliminated teams)
    df["odds_delta"] = np.where(
        pd.notna(df["odds_after"]) & pd.notna(df["odds_before"]),
        df["odds_after"] - df["odds_before"],
        0.0,  # Eliminated teams get 0 odds_delta
    )

    return df[["manager", "year", "week", "odds_before", "odds_after", "odds_delta", "is_win", "is_playoffs"]]


def add_clutch_equity(
    player_df: pd.DataFrame, odds_delta_df: pd.DataFrame, manager_col: str = "manager"
) -> pd.DataFrame:
    """
    Add clutch equity to player DataFrame using weekly starter baselines.

    Credit/blame is distributed proportionally based on how much each player
    exceeded or fell short of that week's starter baseline for their position.

    Args:
        player_df: Player DataFrame with manager_lamar calculated
        odds_delta_df: Output from calculate_weekly_odds_delta()
        manager_col: Column name for manager/team

    Returns:
        DataFrame with clutch columns added:
        - starter_baseline_lamar: What started players at this position averaged
        - above_baseline (G): max(LAMAR - baseline, 0)
        - below_baseline (D): max(baseline - LAMAR, 0)
        - clutch_equity: Share of team's odds change attributed to this player
    """
    df = player_df.copy()

    # Ensure LAMAR is calculated
    if "manager_lamar" not in df.columns:
        raise ValueError("player_df must have manager_lamar column (run sql_enrichments.calculate_lamar_for_all first)")

    # Step 1: Calculate weekly starter baselines
    baseline_df = calculate_weekly_starter_baseline(df, manager_col=manager_col)

    # Step 2: Join baseline to player data
    # Debug: check baseline_df shape and columns
    if baseline_df.empty:
        print("   [WARN] baseline_df is empty - no baselines calculated")
    else:
        print(f"   [Baseline] baseline_df shape: {baseline_df.shape}, columns: {list(baseline_df.columns)}")

    df = df.merge(baseline_df, on=["year", "week", "position"], how="left")

    # Defensive: ensure column exists after merge (handles edge cases with empty/mismatched baselines)
    if "starter_baseline_lamar" not in df.columns:
        print("   [WARN] starter_baseline_lamar column missing after merge - creating with zeros")
        print(
            f"   [DEBUG] df columns after merge: {[c for c in df.columns if 'baseline' in c.lower() or 'lamar' in c.lower()]}"
        )
        print(
            f"   [DEBUG] position values in df: {df['position'].dropna().unique()[:10] if 'position' in df.columns else 'no position col'}"
        )
        df["starter_baseline_lamar"] = 0.0

    # Fill missing baselines (edge case: only one starter at position)
    df["starter_baseline_lamar"] = df["starter_baseline_lamar"].fillna(0)

    # Step 3: Calculate above-baseline (G) and below-baseline (D)
    df["above_baseline"] = (df["manager_lamar"] - df["starter_baseline_lamar"]).clip(lower=0)
    df["below_baseline"] = (df["starter_baseline_lamar"] - df["manager_lamar"]).clip(lower=0)

    # Step 4: Join odds delta and win status
    # We need both odds_delta and is_win to make clutch zero-sum per matchup
    odds_cols_needed = ["manager", "year", "week", "odds_delta"]
    if "is_win" in odds_delta_df.columns:
        odds_cols_needed.append("is_win")

    df = df.merge(
        odds_delta_df[odds_cols_needed],
        left_on=[manager_col, "year", "week"],
        right_on=["manager", "year", "week"],
        how="left",
    )

    # Handle duplicate manager column
    if "manager_x" in df.columns:
        df = df.drop(columns=["manager_x"]).rename(columns={"manager_y": "manager"})

    df["odds_delta"] = df["odds_delta"].fillna(0)

    # Step 4b: Calculate matchup-level stake for zero-sum clutch equity
    # The key insight: for clutch to be zero-sum within a matchup, we need to use
    # a SINGLE stake value for both teams (positive for winner, negative for loser).
    # Using each team's individual odds_delta breaks zero-sum because championship
    # odds depend on the entire league, not just these two teams.
    #
    # Matchup stake = winner's odds gain (absolute value)
    # Winner gets: +stake distributed to above-baseline players
    # Loser gets: -stake distributed to below-baseline (or equally if none)
    #
    # CRITICAL: Use franchise_id as primary key for matching, not manager names.
    # Manager names can change due to disambiguation.
    if "is_win" in df.columns and "opponent" in df.columns:
        # Build matchup stake lookup using franchise_id as primary key
        matchup_stake = {}

        # Determine key column: prefer franchise_id over manager
        use_fid = manager_col == "franchise_id"

        # Build manager_name -> franchise_id lookup for opponent resolution
        # This handles cases where opponent is stored as name but we need fid
        name_to_fid = {}
        if use_fid and "manager" in df.columns:
            for _, row in df[["manager", "franchise_id"]].drop_duplicates().iterrows():
                if pd.notna(row["manager"]) and pd.notna(row["franchise_id"]):
                    name_to_fid[row["manager"]] = row["franchise_id"]

        # Get unique manager-weeks with their odds_delta and win status
        agg_cols = {"odds_delta": "first", "is_win": "first", "opponent": "first"}
        if "manager" in df.columns and manager_col != "manager":
            agg_cols["manager"] = "first"  # Keep manager name for opponent lookup
        # Include opponent_franchise_id if available for more reliable matching
        has_opp_fid = "opponent_franchise_id" in df.columns
        if has_opp_fid:
            agg_cols["opponent_franchise_id"] = "first"
        manager_weeks = df.groupby([manager_col, "year", "week"]).agg(agg_cols).reset_index()

        if manager_weeks is not None and not manager_weeks.empty:
            for _, row in manager_weeks.iterrows():
                mgr_key = row[manager_col]  # This is franchise_id if available
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
                elif use_fid:
                    opp_key = name_to_fid.get(opp_name, opp_name)
                else:
                    opp_key = opp_name

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

        # Apply matchup stake to override individual odds_delta
        # This makes clutch zero-sum per matchup
        def get_matchup_stake(row):
            key = (row["year"], row["week"], row[manager_col])
            # Bye weeks (no opponent) should have 0 stake - they didn't play
            if pd.isna(row.get("opponent")):
                return 0.0
            return matchup_stake.get(key, row["odds_delta"])

        df["matchup_stake"] = df.apply(get_matchup_stake, axis=1)
    else:
        # Fallback: use individual odds_delta (not zero-sum per matchup but preserves original behavior)
        df["matchup_stake"] = df["odds_delta"]

    # Step 5: Filter to started players for team totals
    if "fantasy_position" in df.columns:
        started_mask = df["fantasy_position"].notna() & ~df["fantasy_position"].isin(["BN", "IR", "IR+", "TAXI"])
    else:
        started_mask = pd.Series([True] * len(df))

    # Step 6: Calculate team totals (S+ and S-) for started players
    started_df = df[started_mask].copy()

    team_totals = (
        started_df.groupby([manager_col, "year", "week"])
        .agg(
            {
                "above_baseline": "sum",  # S+
                "below_baseline": "sum",  # S-
            }
        )
        .reset_index()
    )
    team_totals.columns = [manager_col, "year", "week", "team_above_baseline", "team_below_baseline"]

    df = df.merge(team_totals, on=[manager_col, "year", "week"], how="left")

    # Step 7: Calculate clutch equity using matchup_stake (zero-sum per matchup)
    df["clutch_equity"] = 0.0

    # For WINNERS (matchup_stake > 0): credit above-baseline players proportionally
    stake_positive = started_mask & (df["matchup_stake"] > 0)
    has_above = df["team_above_baseline"] > 0

    df.loc[stake_positive & has_above, "clutch_equity"] = (
        df.loc[stake_positive & has_above, "matchup_stake"]
        * df.loc[stake_positive & has_above, "above_baseline"]
        / df.loc[stake_positive & has_above, "team_above_baseline"]
    )

    # EDGE CASE: Winner with no one above baseline (rare luck) - distribute equally
    no_above = df["team_above_baseline"] == 0
    if (stake_positive & no_above).any():
        team_week_counts = df[started_mask].groupby([manager_col, "year", "week"]).size()
        for idx in df[stake_positive & no_above].index:
            row = df.loc[idx]
            key = (row[manager_col], row["year"], row["week"])
            starter_count = team_week_counts.get(key, 1)
            df.loc[idx, "clutch_equity"] = row["matchup_stake"] / starter_count

    # For LOSERS (matchup_stake < 0): distribute negative stake
    # CRITICAL FIX: Losers ALWAYS get negative clutch equity to maintain zero-sum.
    # Previously, losers with all above-baseline players got 0, breaking zero-sum.
    stake_negative = started_mask & (df["matchup_stake"] < 0)
    has_below = df["team_below_baseline"] > 0

    # Case 1: Loser has below-baseline players - distribute blame proportionally
    df.loc[stake_negative & has_below, "clutch_equity"] = (
        df.loc[stake_negative & has_below, "matchup_stake"]  # negative
        * df.loc[stake_negative & has_below, "below_baseline"]
        / df.loc[stake_negative & has_below, "team_below_baseline"]
    )

    # Case 2: Loser has NO below-baseline players (everyone performed well but still lost)
    # CRITICAL: These players STILL get negative clutch equity (team's bad luck)
    # This is the key fix - previously this resulted in 0, breaking zero-sum
    no_below = df["team_below_baseline"] == 0
    if (stake_negative & no_below).any():
        if "team_week_counts" not in dir():
            team_week_counts = df[started_mask].groupby([manager_col, "year", "week"]).size()
        for idx in df[stake_negative & no_below].index:
            row = df.loc[idx]
            key = (row[manager_col], row["year"], row["week"])
            starter_count = team_week_counts.get(key, 1)
            # Distribute negative stake equally among all starters
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

    # Clean up intermediate columns
    if "matchup_stake" in df.columns:
        df = df.drop(columns=["matchup_stake"])

    return df


def calculate_season_clutch(player_df: pd.DataFrame, group_cols: list = None) -> pd.DataFrame:
    """
    Aggregate clutch scores to season level.

    Args:
        player_df: Player DataFrame with clutch_equity calculated
        group_cols: Columns to group by (default: [player_id/yahoo_player_id, year])

    Returns:
        DataFrame with season-level clutch metrics:
        - total_clutch_equity: Sum of weekly clutch equity (in % championship odds)
        - positive_clutch: Sum of credit received
        - negative_clutch: Sum of blame received
        - weeks_above_baseline: How often they exceeded the weekly bar
        - weeks_below_baseline: How often they fell short
    """
    if group_cols is None:
        id_col = select_platform_player_id_column(player_df.columns) or "yahoo_player_id"
        group_cols = [id_col, "year"]

    # Use optimized version if available
    if _SQL_OPTIMIZATIONS_AVAILABLE:
        id_col = group_cols[0] if group_cols else "yahoo_player_id"
        return aggregate_season_clutch_fast(player_df, id_col=id_col)

    # Fallback to original implementation
    df = player_df.copy()

    # Filter to started weeks
    if "fantasy_position" in df.columns:
        started_mask = df["fantasy_position"].notna() & ~df["fantasy_position"].isin(["BN", "IR", "IR+", "TAXI"])
        df = df[started_mask].copy()

    # Add helper columns
    df["positive_clutch"] = df["clutch_equity"].clip(lower=0)
    df["negative_clutch"] = df["clutch_equity"].clip(upper=0)
    df["was_above_baseline"] = (df["above_baseline"] > 0).astype(int)
    df["was_below_baseline"] = (df["below_baseline"] > 0).astype(int)

    # Aggregate
    agg_dict = {
        "clutch_equity": "sum",
        "positive_clutch": "sum",
        "negative_clutch": "sum",
        "above_baseline": "sum",
        "below_baseline": "sum",
        "was_above_baseline": "sum",
        "was_below_baseline": "sum",
        "manager_lamar": "sum",
        "week": "count",
    }

    season_stats = df.groupby(group_cols).agg(agg_dict).reset_index()

    season_stats.rename(
        columns={
            "clutch_equity": "total_clutch_equity",
            "above_baseline": "total_above_baseline",
            "below_baseline": "total_below_baseline",
            "was_above_baseline": "weeks_above_baseline",
            "was_below_baseline": "weeks_below_baseline",
            "manager_lamar": "total_manager_lamar",
            "week": "weeks_started",
        },
        inplace=True,
    )

    # Rate metrics
    season_stats["avg_clutch_per_week"] = season_stats["total_clutch_equity"] / season_stats["weeks_started"].clip(
        lower=1
    )

    # Consistency: what % of weeks were above baseline?
    season_stats["pct_weeks_above_baseline"] = (
        season_stats["weeks_above_baseline"] / season_stats["weeks_started"].clip(lower=1) * 100
    )

    return season_stats


def calculate_manager_clutch_summary(player_df: pd.DataFrame, manager_col: str = "manager") -> pd.DataFrame:
    """
    Aggregate clutch scores by manager for season.

    Shows total championship equity gained/lost by each manager's roster.
    Should sum to roughly 0 across the league (zero-sum game).

    Args:
        player_df: Player DataFrame with clutch_equity calculated
        manager_col: Column name for manager (overridden to franchise_id if available)

    Returns:
        DataFrame with manager-level clutch metrics
    """
    df = player_df.copy()

    # Prefer franchise_id for consistent career tracking
    if True:
        manager_col = "franchise_id"

    # Filter to started players
    if "fantasy_position" in df.columns:
        started_mask = df["fantasy_position"].notna() & ~df["fantasy_position"].isin(["BN", "IR", "IR+", "TAXI"])
        df = df[started_mask].copy()

    # Aggregate by manager/franchise and year
    manager_stats = (
        df.groupby([manager_col, "year"])
        .agg({"clutch_equity": "sum", "above_baseline": "sum", "below_baseline": "sum", "manager_lamar": "sum"})
        .reset_index()
    )

    manager_stats.rename(
        columns={
            "clutch_equity": "total_roster_clutch",
            "above_baseline": "total_roster_above_baseline",
            "below_baseline": "total_roster_below_baseline",
            "manager_lamar": "total_roster_lamar",
        },
        inplace=True,
    )

    return manager_stats


def get_clutch_leaders(
    player_df: pd.DataFrame, year: int = None, top_n: int = 20, min_weeks: int = 8
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Get top clutch performers and biggest clutch failures for a season.

    Args:
        player_df: Player DataFrame with clutch_equity calculated
        year: Filter to specific year (None = all years)
        top_n: Number of leaders to return
        min_weeks: Minimum weeks started to qualify

    Returns:
        Tuple of (clutch_heroes, clutch_goats) DataFrames
    """
    season_clutch = calculate_season_clutch(player_df)

    if year is not None:
        season_clutch = season_clutch[season_clutch["year"] == year]

    # Filter by minimum weeks
    qualified = season_clutch[season_clutch["weeks_started"] >= min_weeks].copy()

    # Get player names if available
    id_col = select_platform_player_id_column(player_df.columns) or "yahoo_player_id"
    if "player_name" in player_df.columns:
        name_map = player_df.drop_duplicates(id_col).set_index(id_col)["player_name"]
        qualified["player_name"] = qualified[id_col].map(name_map)

    # Sort for heroes (highest positive clutch)
    heroes = qualified.nlargest(top_n, "total_clutch_equity")

    # Sort for goats (most negative clutch)
    goats = qualified.nsmallest(top_n, "total_clutch_equity")

    return heroes, goats


def calculate_all_clutch_metrics(
    player_df: pd.DataFrame,
    matchup_df: pd.DataFrame,
    manager_col: str = "manager",
    odds_col: str = "p_champ",
    lamar_col: str = "manager_lamar",
    include_playoffs: bool = True,
) -> pd.DataFrame:
    """
    Main entry point - calculate all clutch metrics and add to player DataFrame.

    Uses weekly starter baselines to determine credit/blame:
    - Above-baseline players get credit when odds go up
    - Below-baseline players get blamed when odds go down
    - Sum of clutch equity = actual odds change (anchored to reality)

    Args:
        player_df: Player DataFrame with LAMAR column
        matchup_df: Matchup DataFrame with championship odds
        manager_col: Column name for manager/team
        odds_col: Column name for championship probability
        lamar_col: Column name for weekly LAMAR values. Accepts 'manager_lamar',
                  'manager_lamar', or 'player_lamar'. Will auto-detect if not specified.
        include_playoffs: If True, include playoff weeks (default: True)

    Returns:
        DataFrame with clutch columns added:
        - starter_baseline_lamar: What started players at this position averaged
        - above_baseline: How much player exceeded baseline (0 if below)
        - below_baseline: How much player fell short of baseline (0 if above)
        - clutch_equity: Share of team's odds change (in %)
    """
    df = player_df.copy()

    # Auto-detect LAMAR column if the specified one doesn't exist
    if lamar_col not in df.columns:
        # Try common alternatives
        lamar_alternatives = ["manager_lamar", "manager_lamar", "player_lamar"]
        found_col = None
        for alt in lamar_alternatives:
            if alt in df.columns:
                found_col = alt
                break

        if found_col is None:
            raise ValueError(
                f"LAMAR column '{lamar_col}' not found. "
                f"Available columns: {[c for c in df.columns if 'lamar' in c.lower()]}"
            )

        print(f"[INFO] Using '{found_col}' as LAMAR column ('{lamar_col}' not found)")
        lamar_col = found_col

    # Normalize to expected column name for internal functions
    if lamar_col != "manager_lamar":
        df["manager_lamar"] = df[lamar_col]

    # Validate: check for duplicate player-week-manager rows
    id_col = select_platform_player_id_column(df.columns) or "yahoo_player_id"
    if id_col in df.columns:
        dup_check = df.groupby([id_col, manager_col, "year", "week"]).size()
        dups = dup_check[dup_check > 1]
        if len(dups) > 0:
            print(f"[WARN] Found {len(dups)} duplicate player-week-manager combinations")
            print(f"       First few: {dups.head().to_dict()}")
            # Deduplicate by taking the first occurrence
            df = df.drop_duplicates(subset=[id_col, manager_col, "year", "week"], keep="first")

    # Use optimized combined function if available
    if _SQL_OPTIMIZATIONS_AVAILABLE:
        print("   [Clutch] Using optimized SQL-style calculations")
        df = calculate_clutch_equity_fast(
            player_df=df, matchup_df=matchup_df, lamar_col="manager_lamar", odds_col=odds_col
        )
        # Clean up: remove temporary column if we created it
        if lamar_col != "manager_lamar" and "manager_lamar" in df.columns:
            # Keep both - the original and our working column
            pass
        return df

    # Fallback: Step 1: Calculate weekly odds delta from matchup data
    odds_delta_df = calculate_weekly_odds_delta(matchup_df, odds_col=odds_col, include_playoffs=include_playoffs)

    # Step 2: Add clutch equity to player data
    df = add_clutch_equity(df, odds_delta_df, manager_col=manager_col)

    # Clean up: remove temporary column if we created it
    if lamar_col != "manager_lamar" and "manager_lamar" in df.columns:
        # Keep both - the original and our working column (now has baseline comparisons)
        pass

    return df
