"""
Playoff value holdover functions.

Ensures all managers have rows for playoff weeks and freezes/overrides
playoff odds for eliminated and non-playoff teams during postseason.
"""

import numpy as np
import pandas as pd

from multi_league.core.identity import get_manager_col
from multi_league.core.logging_config import get_logger
from multi_league.transformations.matchup.modules.team_model import safe_col_get

logger = get_logger(__name__)


def ensure_all_managers_have_playoff_rows(df: pd.DataFrame, data_directory: str = None) -> pd.DataFrame:
    """
    Ensure ALL managers (playoff AND non-playoff) have rows for ALL playoff weeks.

    This ensures every cumulative_week has data for every manager who played that year,
    even if they didn't make playoffs, were eliminated, or were on bye.

    For managers missing playoff weeks:
    - Add rows with opponent=None
    - Copy identifying columns from their last valid row
    - Set is_bye_week=1 for placeholder rows

    Args:
        df: Full matchup DataFrame
        data_directory: Path to league data directory (for loading settings)

    Returns:
        DataFrame with placeholder rows added for all managers
    """
    logger.info("Ensuring all managers have rows for all playoff weeks...")

    new_rows = []

    for year in sorted(df["year"].dropna().unique().astype(int)):
        df_year = df[df["year"] == year].copy()

        # Identify playoff weeks (championship bracket only)
        playoff_mask = (df_year["is_playoffs"] == 1) & (df_year["is_consolation"] == 0)
        playoff_weeks = sorted(df_year[playoff_mask]["week"].dropna().unique().astype(int))

        if not playoff_weeks:
            continue

        # Identify the final regular season week
        regular_mask = (df_year["is_playoffs"] == 0) & (df_year["is_consolation"] == 0)
        regular_weeks = df_year[regular_mask]["week"].dropna().unique()
        if len(regular_weeks) == 0:
            continue
        final_regular_week = int(max(regular_weeks))

        # Get ALL managers who played in this year (not just playoff managers)
        # Use franchise_id when available to handle duplicate manager names (e.g. two "Ryan"s)
        _id_col = get_manager_col(df_year)
        all_managers = set(df_year[_id_col].dropna().unique())

        if not all_managers:
            continue

        # Identify which managers actually made the playoffs this year
        # ONLY use is_playoffs == 1 — the enrichment pipeline populates this correctly
        # before the sim runs. Do NOT fall back to final_playoff_seed, which can
        # incorrectly include consolation-bracket managers.
        actual_playoff_managers = set(df_year[df_year["is_playoffs"] == 1][_id_col].dropna().unique())

        logger.debug(
            f"  {year}: {len(all_managers)} total managers ({_id_col}), {len(actual_playoff_managers)} playoff managers, playoff weeks {playoff_weeks}"
        )

        # For each manager, check for missing weeks
        for manager in all_managers:
            manager_rows = df_year[df_year[_id_col] == manager]
            # Check ALL weeks the manager has a row for (not just is_playoffs=1)
            # This prevents creating duplicates when a manager has a consolation game
            manager_all_weeks = set(manager_rows["week"].dropna().astype(int))

            # Find missing playoff weeks (weeks where manager has NO row at all)
            missing_weeks = [w for w in playoff_weeks if w not in manager_all_weeks]

            if not missing_weeks:
                continue

            # Get reference row (last played week - prefer regular season final)
            ref_row = manager_rows[manager_rows["week"] == final_regular_week]
            if ref_row.empty:
                # Fallback to any row for this manager
                ref_row = manager_rows.iloc[-1:] if len(manager_rows) > 0 else None

            if ref_row is None or ref_row.empty:
                continue

            ref_row = ref_row.iloc[0].to_dict()

            logger.debug(f"    {manager}: Adding rows for weeks {missing_weeks}")

            for missing_week in missing_weeks:
                # Create placeholder row
                new_row = ref_row.copy()
                new_row["week"] = missing_week
                new_row["opponent"] = None

                # Set is_playoffs and is_consolation to NULL for placeholder rows
                # This prevents them from matching ANY filter (is_playoffs=1 OR is_playoffs=0)
                # The only way to include these rows is explicitly via is_bye_week=1
                new_row["is_playoffs"] = np.nan
                new_row["is_consolation"] = np.nan
                new_row["is_bye_week"] = 1

                # Update composite key columns for the new week
                # cumulative_week format: year * 100 + week (e.g., 2024 week 15 = 202415)
                new_cumulative_week = int(year) * 100 + int(missing_week)
                new_row["cumulative_week"] = new_cumulative_week

                # manager_week / manager_year_week format: manager (no spaces) + cumulative_week
                manager_clean = str(manager).replace(" ", "")
                new_manager_week = f"{manager_clean}{new_cumulative_week}"
                if "manager_week" in new_row:
                    new_row["manager_week"] = new_manager_week
                if "manager_year_week" in new_row:
                    new_row["manager_year_week"] = new_manager_week

                # matchup_key format: "{team1}__vs__{team2}__{year}__{week}" - but no opponent
                # Set to a unique key for this placeholder row
                new_row["matchup_key"] = f"{manager}__bye__{year}__{missing_week}"
                if "matchup_id" in new_row:
                    new_row["matchup_id"] = np.nan  # Keep numeric dtype; matchup_key identifies bye rows

                # opponent_week and opponent_team should be None since no opponent
                if "opponent_week" in new_row:
                    new_row["opponent_week"] = None
                if "opponent_team" in new_row:
                    new_row["opponent_team"] = None

                # Set all game-specific columns to NaN so they're excluded from all calculations
                # This ensures placeholder rows don't count as games, wins, losses, or affect averages
                #
                # NOTE: We KEEP cumulative/season stats (wins_to_date, shuffle_avg_wins, etc.)
                # Playoff odds (p_playoffs, p_champ, etc.) are copied from reference row but will be
                # OVERWRITTEN when playoff week processing runs the simulation for that week.
                # This function must be called BEFORE the playoff week processing loop.
                game_cols = [
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
                for col in game_cols:
                    if col in new_row:
                        new_row[col] = np.nan

                # Clear round-specific columns
                round_cols = ["playoff_round", "consolation_round", "season_result"]
                for col in round_cols:
                    if col in new_row:
                        new_row[col] = ""

                new_rows.append(new_row)

    if new_rows:
        df_new = pd.DataFrame(new_rows)
        df = pd.concat([df, df_new], ignore_index=True)
        df = df.sort_values(["year", "week", "manager"]).reset_index(drop=True)
        logger.info(f"  Added {len(new_rows)} placeholder rows for eliminated/bye managers")
    else:
        logger.info("  No missing playoff rows found")

    return df


def hold_playoff_values_for_eliminated(
    df: pd.DataFrame,
    data_directory: str = None,
    playoff_round_type: int = 0,
    playoff_start_week: int | None = None,
    end_week: int | None = None,
    num_playoff_teams: int | None = None,
) -> pd.DataFrame:
    """
    Hold playoff odds values for ALL managers during postseason weeks.

    For ALL managers (playoff and non-playoff) during postseason:
    - power_rating, avg_seed, exp_final_wins, x{N}_seed freeze at final regular season values
    - Non-playoff teams: p_playoffs=0%, all other playoff odds=0%

    For playoff managers specifically:
    - p_playoffs stays at 100% (they made playoffs)
    - p_bye reflects their actual bye status (100% if they had bye, 0% otherwise)
    - p_semis, p_final, p_champ reflect what ACTUALLY happened:
      - 100% if they reached that round
      - 0% if they were eliminated before that round

    This ensures eliminated teams show their final status, not simulation probabilities.

    Args:
        df: Full matchup DataFrame

    Returns:
        DataFrame with held values for all teams during postseason
    """
    logger.info("Holding playoff values for eliminated teams...")

    # === INPUT VALIDATION ===
    if df is None or df.empty:
        logger.warning("Empty DataFrame provided to hold_playoff_values_for_eliminated")
        return df if df is not None else pd.DataFrame()

    # Check required columns
    required_cols = ["year", "week", "manager"]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return df

    # Ensure playoff flag columns exist with defaults
    if "is_playoffs" not in df.columns:
        logger.warning("'is_playoffs' column missing, assuming all regular season")
        df["is_playoffs"] = 0
    if "is_consolation" not in df.columns:
        df["is_consolation"] = 0

    # Columns to freeze from final regular season (before playoffs started)
    # These NEVER change during postseason - expected record stats freeze at end of regular season
    cols_freeze_from_reg = [
        "avg_seed",
        "exp_final_wins",
        "exp_final_pf",
    ]
    # Add canonical seed columns (x1_seed through x64_seed) so larger brackets
    # and edge-case leagues keep a stable NULL-padded schema during holdover.
    cols_freeze_from_reg += [f"x{i}_seed" for i in range(1, 65)]
    # Add dynamic win columns (x0_win through x36_win) for H2H and H2H+median leagues.
    cols_freeze_from_reg += [f"x{i}_win" for i in range(0, 37)]

    # Power rating is special - it CAN update during playoff games, but NOT during:
    # - Bye weeks (no game played)
    # - Consolation games (not competitive for championship)

    # Columns that depend on playoff progress
    round_progression_cols = ["p_semis", "p_final", "p_champ"]

    # Build week -> round boundary lookup for 2-week round awareness
    from transformations.matchup.modules.playoff_config import round_weeks as _round_weeks_fn

    _week_to_round: dict = {}
    if (
        playoff_round_type > 0
        and playoff_start_week is not None
        and end_week is not None
        and num_playoff_teams is not None
    ):
        _rounds = _round_weeks_fn(num_playoff_teams, playoff_start_week, end_week, playoff_round_type)
        for _ri, (_ws, _we) in enumerate(_rounds):
            for _w in range(_ws, _we + 1):
                _week_to_round[_w] = (_ri, _ws, _we)

    for year in sorted(df["year"].dropna().unique().astype(int)):
        df_year = df[df["year"] == year].copy()

        # Find playoff start week
        playoff_mask = (df["year"] == year) & (df["is_playoffs"] == 1) & (df["is_consolation"] == 0)
        playoff_weeks = df.loc[playoff_mask, "week"].dropna().unique()
        if len(playoff_weeks) == 0:
            continue
        playoff_start_week = int(min(playoff_weeks))

        # Include ALL weeks from playoff start through end of season
        # This catches post-championship weeks (like week 18) that have is_playoffs=0
        # but still need their values locked for the champion
        max_week_for_year = int(df_year["week"].max())
        all_playoff_weeks = sorted([int(w) for w in range(playoff_start_week, max_week_for_year + 1)])

        # Find final regular season week
        # CRITICAL: Use playoff_start_week - 1, not max of regular season weeks
        # This avoids issues where placeholder rows in postseason weeks have is_playoffs=0
        # The final regular season week is always the week before playoffs start
        final_regular_week = playoff_start_week - 1

        # Verify this week exists in the data
        week_check_mask = (df["year"] == year) & (df["week"] == final_regular_week)
        if not week_check_mask.any():
            # Fallback: find max week that's actually regular season (before playoff_start_week)
            pre_playoff_mask = (df["year"] == year) & (df["week"] < playoff_start_week)
            pre_playoff_weeks = df.loc[pre_playoff_mask, "week"].dropna().unique()
            if len(pre_playoff_weeks) == 0:
                continue
            final_regular_week = int(max(pre_playoff_weeks))

        _id_col = get_manager_col(df_year)
        # Create mask for regular season rows (used when looking up final reg values)
        regular_mask = (df["year"] == year) & (df["is_playoffs"] == 0) & (df["is_consolation"] == 0)
        latest_regular_values = (
            df.loc[regular_mask & (df["week"] < playoff_start_week)]
            .sort_values([_id_col, "week"])
            .groupby(_id_col, as_index=False)
            .tail(1)
            .set_index(_id_col)
        )

        # Get ALL managers in this year — use franchise_id for duplicate name safety
        all_managers = df_year[_id_col].dropna().unique()

        # Determine who made playoffs
        # ONLY use is_playoffs == 1 — the enrichment pipeline populates this correctly
        # before the sim runs. Do NOT fall back to final_playoff_seed, which can
        # incorrectly include consolation-bracket managers.
        playoff_managers = set(df_year[df_year["is_playoffs"] == 1][_id_col].dropna().unique())
        season_has_quarterfinal = False
        if "playoff_round" in df_year.columns:
            season_has_quarterfinal = df_year["playoff_round"].fillna("").eq("quarterfinal").any()

        # Non-playoff managers
        non_playoff_managers = set(all_managers) - playoff_managers

        # Track who lost (and when) in playoffs, and which rounds they played in which weeks
        manager_elimination_week = {}
        manager_round_weeks = {}  # manager -> {round_name: week_played}
        manager_had_bye = {}  # manager -> bool

        for manager in playoff_managers:
            manager_playoff_rows = df_year[
                (df_year[_id_col] == manager)
                & (df_year["is_playoffs"] == 1)
                & (df_year["is_consolation"] == 0)
                & (df_year["opponent"].notna())  # Actual games, not placeholder rows
            ].sort_values("week")

            round_weeks = {}
            elimination_week = None

            for _, row in manager_playoff_rows.iterrows():
                week = int(row["week"])
                playoff_round = row.get("playoff_round", "")

                # Track which round they played in which week (keep earliest
                # week per round so 2-week championships use the start week)
                if playoff_round:
                    if playoff_round not in round_weeks or week < round_weeks[playoff_round]:
                        round_weeks[playoff_round] = week

                # Check if they lost this game
                # Missing win/loss columns should not be interpreted as a playoff loss.
                # Treat absent values as unknown so upstream read-slice mistakes do not
                # zero out championship odds for every playoff team.
                # IMPORTANT: In 2-week championships, a team can lose the individual
                # week but win on combined score — champion=1 is the truth.
                champion_val = pd.to_numeric(row.get("champion"), errors="coerce")
                if pd.notna(champion_val) and champion_val == 1:
                    continue  # champion flag overrides per-week win/loss

                # Check round boundaries for 2-week round awareness
                _round_info = _week_to_round.get(week)
                if _round_info:
                    _, _wk_start, _wk_end = _round_info
                    if _wk_start != _wk_end and week < _wk_end:
                        # Week 1 of 2-week round: skip — round not decided yet
                        continue
                    elif _wk_start != _wk_end and week == _wk_end:
                        # Week 2 of 2-week round: check combined score
                        both_weeks = df[
                            (df["year"] == year)
                            & (df["week"].isin(range(_wk_start, _wk_end + 1)))
                            & (df[_id_col] == manager)
                            & (df["is_playoffs"] == 1)
                            & (df["is_consolation"] == 0)
                        ]
                        if both_weeks.empty:
                            continue
                        combined_pts = both_weeks["team_points"].sum()
                        # Find opponent via opponent_franchise_id on any of these rows
                        opp_fid = (
                            both_weeks["opponent_franchise_id"].dropna().iloc[0]
                            if "opponent_franchise_id" in both_weeks.columns
                            else None
                        )
                        if opp_fid:
                            opp_both = df[
                                (df["year"] == year)
                                & (df["week"].isin(range(_wk_start, _wk_end + 1)))
                                & (df[_id_col] == opp_fid)
                                & (df["is_playoffs"] == 1)
                                & (df["is_consolation"] == 0)
                            ]
                            if not opp_both.empty:
                                opp_combined = opp_both["team_points"].sum()
                                if combined_pts < opp_combined:
                                    elimination_week = week
                                    break
                        continue  # If we can't determine, don't eliminate

                # 1-week round or prt=0: existing logic
                loss_val = pd.to_numeric(row.get("loss"), errors="coerce")
                win_val = pd.to_numeric(row.get("win"), errors="coerce")

                if pd.notna(loss_val) and loss_val == 1:
                    elimination_week = week
                    break
                elif pd.notna(win_val) and win_val == 0:
                    elimination_week = week
                    break

            manager_elimination_week[manager] = elimination_week
            manager_round_weeks[manager] = round_weeks

            # Check if they had a bye (their first playoff game was semifinal, not quarterfinal)
            first_playoff_week = min(manager_playoff_rows["week"].tolist()) if len(manager_playoff_rows) > 0 else None
            if first_playoff_week and first_playoff_week > playoff_start_week:
                manager_had_bye[manager] = True
            elif (
                season_has_quarterfinal
                and round_weeks
                and "quarterfinal" not in round_weeks
                and "semifinal" in round_weeks
            ):
                manager_had_bye[manager] = True
            else:
                manager_had_bye[manager] = False

        # First, handle NON-PLAYOFF managers - they should have rows for all postseason weeks
        # with frozen values and 0% for all playoff odds
        # OPTIMIZED: Use vectorized batch update instead of nested loops
        logger.debug(f"  {year}: Processing {len(non_playoff_managers)} non-playoff managers (batch)")

        if non_playoff_managers:
            # Get the latest pre-playoff regular-season row for each non-playoff
            # manager. Some odd-team seasons legitimately have no row on the
            # literal final regular-season week.
            final_reg_values = latest_regular_values.loc[latest_regular_values.index.intersection(non_playoff_managers)]

            # Mask for all postseason rows of non-playoff managers
            postseason_non_playoff_mask = (
                (df["year"] == year) & (df["week"].isin(all_playoff_weeks)) & (df[_id_col].isin(non_playoff_managers))
            )

            if postseason_non_playoff_mask.any() and not final_reg_values.empty:
                # Create a mapping from manager to their final reg values
                for col in cols_freeze_from_reg:
                    if col in df.columns and col in final_reg_values.columns:
                        # Map final reg values to postseason rows by manager
                        df.loc[postseason_non_playoff_mask, col] = df.loc[postseason_non_playoff_mask, _id_col].map(
                            final_reg_values[col].to_dict()
                        )

                # Freeze power_rating
                if "power_rating" in df.columns and "power_rating" in final_reg_values.columns:
                    df.loc[postseason_non_playoff_mask, "power_rating"] = df.loc[
                        postseason_non_playoff_mask, _id_col
                    ].map(final_reg_values["power_rating"].to_dict())

                # Set all playoff odds to 0% for non-playoff teams (vectorized)
                for odds_col in ["p_playoffs", "p_bye", "p_semis", "p_final", "p_champ"]:
                    if odds_col in df.columns:
                        df.loc[postseason_non_playoff_mask, odds_col] = 0.0

        # Now handle PLAYOFF managers
        logger.debug(f"  {year}: Processing {len(playoff_managers)} playoff managers")
        for manager in playoff_managers:
            elimination_week = manager_elimination_week.get(manager)
            round_weeks = manager_round_weeks.get(manager, {})
            had_bye = manager_had_bye.get(manager, False)

            # Get final regular season row for this manager
            final_reg_row = latest_regular_values.loc[latest_regular_values.index == manager]

            if final_reg_row.empty:
                continue

            final_reg_values = final_reg_row.iloc[0]

            # Determine what rounds this manager reached
            played_quarterfinal = "quarterfinal" in round_weeks
            played_semifinal = "semifinal" in round_weeks
            played_championship = "championship" in round_weeks
            is_champion = elimination_week is None and played_championship

            # Get the week each round was played
            qf_week = round_weeks.get("quarterfinal")
            sf_week = round_weeks.get("semifinal")
            final_week = round_weeks.get("championship")

            # Track the last playoff week this manager played a real game
            # (for freezing power_rating after elimination/bye)
            last_playoff_game_week = max(round_weeks.values()) if round_weeks else None

            # For each playoff week, update values
            for pw in all_playoff_weeks:
                row_mask = (df["year"] == year) & (df["week"] == pw) & (df[_id_col] == manager)

                if not row_mask.any():
                    continue

                for idx in df.index[row_mask]:
                    # 1. Freeze expected record columns from final regular season
                    for col in cols_freeze_from_reg:
                        if col in df.columns and col in final_reg_values.index:
                            val = final_reg_values[col]
                            if pd.notna(val):
                                df.at[idx, col] = val

                    # 2. Handle power_rating specially
                    # Power rating CAN update during actual playoff games
                    # But should FREEZE for: bye weeks, consolation games, weeks after elimination
                    is_bye_row = (
                        (safe_col_get(df, idx, "is_bye_week", default=0) == 1)
                        or pd.isna(safe_col_get(df, idx, "opponent", default=None))
                        or safe_col_get(df, idx, "opponent", default="") == ""
                    )
                    is_consolation_row = safe_col_get(df, idx, "is_consolation", default=0) == 1
                    is_after_elimination = elimination_week is not None and pw > elimination_week

                    if is_bye_row or is_consolation_row or is_after_elimination:
                        # Freeze power_rating from last meaningful game
                        if last_playoff_game_week and last_playoff_game_week <= pw:
                            # Use power_rating from their last playoff game
                            # Must match on the identity column we looped on (franchise_id
                            # when available) — NOT the manager display name, which doesn't
                            # match the franchise_id values held in `manager` here.
                            last_game_row = df.loc[
                                (df["year"] == year)
                                & (df["week"] == last_playoff_game_week)
                                & (df[_id_col] == manager)
                                & (df["is_playoffs"] == 1)
                                & (df["is_consolation"] == 0)
                            ]
                            if not last_game_row.empty and "power_rating" in last_game_row.columns:
                                pr_val = last_game_row.iloc[0]["power_rating"]
                                if pd.notna(pr_val):
                                    df.at[idx, "power_rating"] = pr_val
                        else:
                            # Fallback to final regular season
                            if "power_rating" in df.columns and "power_rating" in final_reg_values.index:
                                val = final_reg_values["power_rating"]
                                if pd.notna(val):
                                    df.at[idx, "power_rating"] = val
                    # else: power_rating can be calculated naturally for actual playoff games

                    # 3. p_playoffs = 100% (they made playoffs - this is fact)
                    if "p_playoffs" in df.columns:
                        df.at[idx, "p_playoffs"] = 100.0

                    # 4. p_bye reflects actual bye status (100% or 0%)
                    if "p_bye" in df.columns:
                        df.at[idx, "p_bye"] = 100.0 if had_bye else 0.0

                    # 5. Handle round progression - reflect what ACTUALLY happened
                    # IMPORTANT: Only lock in results AFTER the round has been played
                    # Before the round, keep the simulated probabilities

                    # p_semis: Did they make semifinals?
                    # Lock in only after quarterfinals are played (or if they had bye)
                    if "p_semis" in df.columns:
                        # If they had a bye, they auto-made semis from the start
                        if had_bye:
                            df.at[idx, "p_semis"] = 100.0
                        # If they played in semis (or later), they made semis
                        elif sf_week and pw >= sf_week:
                            df.at[idx, "p_semis"] = 100.0
                        # If they were eliminated before semis, 0%
                        elif (
                            elimination_week is not None
                            and pw >= elimination_week
                            and (sf_week is None or elimination_week < sf_week)
                        ):
                            df.at[idx, "p_semis"] = 0.0
                        # Otherwise keep simulated probability

                    # p_final: Did they make the championship game?
                    # Lock in ON semifinal week (post-game state: winners have 100%, losers have 0%)
                    if "p_final" in df.columns:
                        if played_championship:
                            # Infer semifinal week if missing (handles bye teams whose
                            # semis rows are mislabeled as consolation in the data)
                            effective_sf_week = sf_week or (final_week - 1 if final_week else None)
                            if effective_sf_week and pw >= effective_sf_week:
                                df.at[idx, "p_final"] = 100.0
                            elif final_week and pw >= final_week:
                                df.at[idx, "p_final"] = 100.0
                        # If eliminated in semis or earlier, 0% for weeks ON AND AFTER elimination
                        elif elimination_week is not None and pw >= elimination_week:
                            df.at[idx, "p_final"] = 0.0
                        # Otherwise keep simulated probability (pre-game weeks)

                    # p_champ: Did they WIN the championship?
                    # Lock in ON championship game (post-game state: champion has 100%, all others 0%)
                    if "p_champ" in df.columns:
                        # Champion: 100% on and after championship week
                        if is_champion and final_week and pw >= final_week:
                            df.at[idx, "p_champ"] = 100.0
                        # Championship loser: They played in finals but lost - 0% on championship week
                        elif played_championship and not is_champion and final_week and pw >= final_week:
                            df.at[idx, "p_champ"] = 0.0
                        # If eliminated before finals (lost any earlier playoff game), 0% from elimination week
                        # Odds represent POST-GAME state: "what are odds from here forward?"
                        elif elimination_week is not None and pw >= elimination_week:
                            df.at[idx, "p_champ"] = 0.0
                        # DEFENSIVE: If p_champ is NaN (team wasn't in simulation), set to 0
                        # This handles edge cases where eliminated teams have no simulation value
                        elif pd.isna(df.at[idx, "p_champ"]):
                            df.at[idx, "p_champ"] = 0.0
                        # Otherwise keep simulated probability (including the week of the game)

        logger.debug(f"  {year}: Updated values for all managers")

    # x_win buckets are a regular-season finish distribution. Once a row is
    # explicitly consolation, carrying those frozen buckets forward becomes
    # misleading because postseason placement wins can increment wins_to_date
    # while the distribution remains anchored to the final regular-season state.
    # Leave the holdover on playoff rows for title-path history, but clear it
    # on consolation rows where it has no meaningful interpretation.
    x_win_cols = [col for col in (f"x{i}_win" for i in range(0, 37)) if col in df.columns]
    if x_win_cols and "is_consolation" in df.columns:
        consolation_mask = pd.to_numeric(df["is_consolation"], errors="coerce").fillna(0).eq(1)
        if consolation_mask.any():
            df.loc[consolation_mask, x_win_cols] = np.nan

    # Re-enforce hierarchy after postseason overwrites. This hierarchy only
    # applies to seasons with an actual championship bracket; no-playoff
    # seasons use p_champ as "chance to finish seed 1" while p_playoffs/p_final
    # stay at 0 by design.
    playoff_year_mask = pd.Series(False, index=df.index)
    if {"year", "is_playoffs", "is_consolation"}.issubset(df.columns):
        _is_playoffs = pd.to_numeric(df["is_playoffs"], errors="coerce").fillna(0).eq(1)
        _is_consolation = pd.to_numeric(df["is_consolation"], errors="coerce").fillna(0).eq(1)
        playoff_years = set(df.loc[_is_playoffs & ~_is_consolation, "year"].dropna().unique())
        playoff_year_mask = df["year"].isin(playoff_years)

    for col_lo, col_hi in [("p_champ", "p_final"), ("p_final", "p_semis"), ("p_semis", "p_playoffs")]:
        if col_lo in df.columns and col_hi in df.columns:
            mask = playoff_year_mask & (df[col_lo] > df[col_hi])
            if mask.any():
                logger.debug(f"  Hierarchy fix: clipping {mask.sum()} rows where {col_lo} > {col_hi}")
                df.loc[mask, col_lo] = df.loc[mask, col_hi]

    logger.info("  Playoff values held for eliminated teams")
    return df
