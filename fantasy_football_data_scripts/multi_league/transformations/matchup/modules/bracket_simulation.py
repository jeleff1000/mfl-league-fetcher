"""
Bracket Simulation Module

Vectorized Monte Carlo playoff bracket simulation and related helper functions.

This module contains:
- simulate_playoff_bracket_vectorized: Main simulation function
- _get_bracket_side: Bracket side assignment based on seeding
- calculate_effective_byes: Determine active bye count based on round state
- apply_completed_round_overrides: Override probabilities for completed games
- build_playoff_week_mask: Create mask for playoff week rows including bye teams
"""

import math
import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# =============================================================================
# BRACKET HELPER FUNCTIONS
# =============================================================================


def _coerce_positive_integer(value, field_name: str) -> int:
    """Normalize integer-like numpy/pandas scalars while rejecting non-integers."""
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a positive integer, got {value}")

    if isinstance(value, (int, np.integer)):
        coerced = int(value)
    elif isinstance(value, (float, np.floating)):
        if not np.isfinite(value) or not float(value).is_integer():
            raise ValueError(f"{field_name} must be a positive integer, got {value}")
        coerced = int(value)
    else:
        raise ValueError(f"{field_name} must be a positive integer, got {value}")

    if coerced < 1:
        raise ValueError(f"{field_name} must be a positive integer, got {value}")

    return coerced


def _get_bracket_side(seed: int, num_playoff_teams: int) -> str:
    """
    Determine which bracket side a seed is on (A or B) for fixed bracket.

    Standard bracket structure assigns seeds to sides so that the #1 seed
    can only meet the #2 seed in the finals:
    - 4-team: Side A = 1,4; Side B = 2,3
    - 6-team: Side A = 1,4,5; Side B = 2,3,6
    - 8-team: Side A = 1,4,5,8; Side B = 2,3,6,7

    Raises:
        ValueError: If seed or num_playoff_teams is invalid
    """
    seed = _coerce_positive_integer(seed, "seed")
    num_playoff_teams = _coerce_positive_integer(num_playoff_teams, "num_playoff_teams")
    if num_playoff_teams < 2:
        raise ValueError(f"num_playoff_teams must be >= 2, got {num_playoff_teams}")

    if num_playoff_teams <= 4:
        return "A" if seed in [1, 4] else "B"
    elif num_playoff_teams <= 6:
        return "A" if seed in [1, 4, 5] else "B"
    elif num_playoff_teams <= 8:
        return "A" if seed in [1, 4, 5, 8] else "B"
    else:
        # For larger brackets, use standard tournament seeding pattern
        bracket_group = ((seed - 1) // 4) % 2
        position_in_group = (seed - 1) % 4
        if bracket_group == 0:
            return "A" if position_in_group in [0, 3] else "B"
        else:
            return "A" if position_in_group in [0, 3] else "B"


def calculate_effective_byes(current_playoff_round: int, bye_teams: int, games_this_round_complete: bool) -> int:
    """
    Calculate the effective bye count for simulation based on round state.

    Byes only apply in round 0 (wild card). Once wild card games are complete,
    bye teams must play in the next round (semis), so effective byes = 0.

    Args:
        current_playoff_round: 0 = wild card, 1 = semis, 2 = finals
        bye_teams: Total number of bye teams in the bracket
        games_this_round_complete: Whether current round's games are finished

    Returns:
        Number of effective byes for simulation

    Raises:
        ValueError: If inputs are invalid
    """
    if not isinstance(current_playoff_round, (int, np.integer)) or current_playoff_round < 0:
        raise ValueError(f"current_playoff_round must be non-negative integer, got {current_playoff_round}")
    if not isinstance(bye_teams, (int, np.integer)) or bye_teams < 0:
        raise ValueError(f"bye_teams must be non-negative integer, got {bye_teams}")

    if games_this_round_complete:
        # Round complete - byes have already been processed
        return 0
    elif current_playoff_round == 0:
        # Wild card round, byes still active
        return bye_teams
    else:
        # Past wild card, no more byes
        return 0


def apply_completed_round_overrides(
    odds: pd.DataFrame,
    teams_alive: list[str],
    playoff_teams: list[str],
    bracket_info: dict | None,
    current_playoff_round: int,
    games_this_round_complete: bool,
) -> pd.DataFrame:
    """
    Override simulation probabilities based on completed games.

    When games are complete, some probabilities become deterministic:
    - Wild card complete: all teams_alive are semifinalists (p_semis=100%)
    - 2 teams remaining: finalists are locked (p_final=100%)
    - Championship complete: winner has p_champ=100%

    Args:
        odds: DataFrame with probability columns (P_Semis, P_Final, P_Champ)
        teams_alive: List of teams still alive in playoffs
        playoff_teams: Full list of teams that made playoffs
        bracket_info: Dict with 'games' key containing list of (mgr, opp, winner)
        current_playoff_round: 0 = wild card, 1 = semis, 2 = finals
        games_this_round_complete: Whether current round's games are finished

    Returns:
        Updated odds DataFrame with deterministic overrides applied

    Raises:
        ValueError: If odds DataFrame is missing required columns
    """
    # Validate inputs
    if odds is None or odds.empty:
        logger.warning("Empty odds DataFrame passed to apply_completed_round_overrides")
        return odds if odds is not None else pd.DataFrame()

    required_cols = {"P_Semis", "P_Final", "P_Champ"}
    missing_cols = required_cols - set(odds.columns)
    if missing_cols:
        raise ValueError(f"odds DataFrame missing required columns: {missing_cols}")

    idx_mgrs = odds.index.tolist()
    teams_alive_set = set(teams_alive) if teams_alive else set()
    playoff_teams_set = set(playoff_teams) if playoff_teams else set()

    # Wild card complete - all remaining teams made semis
    if games_this_round_complete and current_playoff_round == 0:
        for m in idx_mgrs:
            if m in teams_alive_set:
                odds.at[m, "P_Semis"] = 100.0
            elif m in playoff_teams_set:
                # Was in playoffs but eliminated in wild card
                odds.at[m, "P_Semis"] = 0.0
        logger.debug(f"[Wild Card Complete] {len(teams_alive_set)} semifinalists: {sorted(teams_alive_set)}")

    # Championship game locked - 2 finalists determined
    # IMPORTANT: Do NOT modify P_Champ values here - keep simulated pre-game odds.
    # The holdover logic will set correct deterministic values for weeks AFTER.
    # This preserves the "odds at the time" for clutch equity calculation.
    if len(teams_alive_set) == 2:
        finals_set = teams_alive_set
        # Set P_Final for finalists (this is deterministic - they made finals)
        odds.loc[list(finals_set), "P_Final"] = 100.0
        # NOTE: We intentionally do NOT modify P_Champ here - keep simulated values
        # so p_champ sum remains 100% for the championship week

    # Single team remaining - champion determined (post-championship week)
    # NOTE: Don't modify P_Champ here either - holdover handles it
    # Keep simulated values so clutch calculation works correctly

    return odds


def build_playoff_week_mask(df: pd.DataFrame, year: int, week: int, playoff_team_set: set[str]) -> pd.Series:
    """
    Build a mask for playoff week rows that includes both regular playoff rows
    and bye team rows.

    Bye teams have is_playoffs=NaN (not 1) during their bye week since they
    don't have an actual game. This function ensures they're still included
    in playoff processing.

    Args:
        df: DataFrame with columns year, week, is_playoffs, is_consolation, manager
        year: Season year
        week: Playoff week number
        playoff_team_set: Set of manager names who made playoffs

    Returns:
        Boolean Series mask for rows to include

    Raises:
        ValueError: If DataFrame is missing required columns
    """
    # Validate inputs
    if df is None or df.empty:
        logger.warning("Empty DataFrame passed to build_playoff_week_mask")
        return pd.Series(dtype=bool)

    required_cols = {"year", "week", "is_playoffs", "is_consolation", "manager"}
    missing_cols = required_cols - set(df.columns)
    if missing_cols:
        raise ValueError(f"DataFrame missing required columns: {missing_cols}")

    # Normal playoff rows: is_playoffs=1, not consolation
    normal_playoff_mask = (
        (df["year"] == year) & (df["week"] == week) & (df["is_playoffs"] == 1) & (df["is_consolation"] == 0)
    )

    # Bye team rows: `is_bye_week=1` is authoritative, even if the source marks
    # `is_playoffs` as False instead of NULL for the placeholder row. Byes can only
    # be identified when both the flag and the stable identity column are present;
    # if either is missing there are simply no bye rows to add.
    if "is_bye_week" in df.columns and "franchise_id" in df.columns:
        bye_team_mask = (
            (df["year"] == year)
            & (df["week"] == week)
            & (df["is_bye_week"] == 1)
            & (df["is_consolation"].isna() | (df["is_consolation"] == 0))
            & (df["franchise_id"].isin(playoff_team_set))
        )
        return normal_playoff_mask | bye_team_mask

    return normal_playoff_mask


# =============================================================================
# RESEEDING BRACKET SIMULATION (INTERNAL)
# =============================================================================


def _simulate_reseeding_bracket(
    teams_alive: list[str],
    seeds_map: dict[str, int],
    mu_hat: dict[str, float],
    sigma_hat: dict[str, float],
    n_sims: int,
    bye_teams: int,
    rng_seed: int | None,
    actual_results: dict[tuple[str, str], str],
    playoff_round_type: int = 0,
    round_week_ranges: "list[tuple[int, int]] | None" = None,
    round_actuals: "dict[tuple[int, int], dict[str, float]] | None" = None,
) -> dict[str, dict[str, float]]:
    """
    Simulate playoff bracket with reseeding after each round.

    With reseeding, after each round the remaining teams are re-ranked by seed
    and paired: highest vs lowest, 2nd highest vs 2nd lowest, etc.
    This differs from fixed brackets where teams stay on their bracket side.

    Example (6-team with 2 byes, reseeding):
    - Wild card: #3 vs #6, #4 vs #5
    - Semis: #1 vs lowest remaining, #2 vs other remaining
    - Finals: winners play

    This is called internally by simulate_playoff_bracket_vectorized when
    uses_reseeding=True.
    """
    # Sort teams by seed
    sorted_teams = sorted(teams_alive, key=lambda m: seeds_map.get(m, 999))
    managers = sorted_teams
    n_teams = len(managers)
    mgr_to_idx = {m: i for i, m in enumerate(managers)}

    # Build parameter arrays
    default_mu = np.mean([mu_hat.get(m, 100.0) for m in managers])
    default_sigma = np.mean([sigma_hat.get(m, 15.0) for m in managers])
    mu_arr = np.array([mu_hat.get(m, default_mu) for m in managers])
    sigma_arr = np.array([sigma_hat.get(m, default_sigma) for m in managers])

    # Initialize RNG
    rng = np.random.default_rng(rng_seed)

    # Pre-draw all random scores
    max_rounds = max(4, math.ceil(math.log2(n_teams)) + 2)
    _round_ranges = round_week_ranges or [(i, i) for i in range(max_rounds)]
    all_scores, _all_scores_wk2 = generate_round_scores(
        mu_arr,
        sigma_arr,
        n_sims,
        n_teams,
        max_rounds,
        playoff_round_type,
        round_actuals,
        {m: i for i, m in enumerate(managers)},
        rng,
        _round_ranges,
    )

    # Track alive status: alive[sim, team_idx] = True if still in tournament
    alive = np.ones((n_sims, n_teams), dtype=bool)

    # Track milestones
    reached_semis = np.zeros((n_sims, n_teams), dtype=bool)
    reached_final = np.zeros((n_sims, n_teams), dtype=bool)
    won_champ = np.zeros((n_sims, n_teams), dtype=bool)

    # Bye teams don't play in round 0
    bye_indices = set(range(bye_teams))  # First N indices (sorted by seed)

    def simulate_matchup_reseeding(idx_a, idx_b, round_num):
        """Simulate matchup, return (a_won, b_won) masks."""
        team_a = managers[idx_a]
        team_b = managers[idx_b]

        # Check for actual result
        matchup_key = tuple(sorted([team_a, team_b]))
        actual_winner = actual_results.get(matchup_key)
        if actual_winner is None:
            actual_winner = actual_results.get((team_a, team_b))
        if actual_winner is None:
            actual_winner = actual_results.get((team_b, team_a))

        both_alive = alive[:, idx_a] & alive[:, idx_b]

        if actual_winner:
            if actual_winner == team_a:
                return both_alive, np.zeros(n_sims, dtype=bool)
            else:
                return np.zeros(n_sims, dtype=bool), both_alive

        # Simulate
        score_a = all_scores[:, idx_a, round_num]
        score_b = all_scores[:, idx_b, round_num]

        a_wins = (score_a > score_b) & both_alive
        b_wins = (score_b > score_a) & both_alive
        ties = (score_a == score_b) & both_alive

        # Deterministic tie-breaking: higher seed (lower number) wins
        seed_a = seeds_map.get(team_a, 999)
        seed_b = seeds_map.get(team_b, 999)
        if seed_a <= seed_b:
            a_wins_tie = ties
            b_wins_tie = np.zeros(n_sims, dtype=bool)
        else:
            a_wins_tie = np.zeros(n_sims, dtype=bool)
            b_wins_tie = ties

        return a_wins | a_wins_tie, b_wins | b_wins_tie

    current_round = 0

    # Run tournament rounds until champion determined
    while True:
        # Count alive teams per simulation
        alive_counts = alive.sum(axis=1)

        # Mark milestones based on alive count
        # Semifinals: 4 or fewer teams remain
        for idx in range(n_teams):
            semis_mask = (alive_counts <= 4) & alive[:, idx]
            reached_semis[:, idx] |= semis_mask

        # Finals: 2 teams remain
        for idx in range(n_teams):
            finals_mask = (alive_counts <= 2) & alive[:, idx]
            reached_final[:, idx] |= finals_mask

        # Champion: 1 team remains
        if (alive_counts == 1).all():
            for idx in range(n_teams):
                won_champ[:, idx] |= alive[:, idx]
            break

        # Safety check
        if current_round > max_rounds:
            logger.warning(f"Reseeding bracket exceeded max rounds ({max_rounds})")
            break

        # Determine matchups for this round
        # For each simulation, get alive team indices sorted by seed
        # Then pair: 0 vs -1, 1 vs -2, etc.

        # Get list of teams playing this round (exclude byes in round 0)
        if current_round == 0 and bye_teams > 0:
            playing_indices = [i for i in range(n_teams) if i not in bye_indices]
        else:
            playing_indices = list(range(n_teams))

        # For reseeding, we need to handle the fact that different sims
        # may have different alive teams. We iterate through possible matchups.

        # Get the alive teams in seed order for pairing
        # In reseeding, highest seed plays lowest, etc.
        new_alive = alive.copy()

        # Number of matchups = floor(alive_playing / 2)
        # We handle this by checking all possible high/low seed combinations

        for high_seed_rank in range(len(playing_indices) // 2):
            # high_seed_rank=0 means highest seed, =1 means 2nd highest, etc.
            # They play against the corresponding low seed

            for high_idx in playing_indices:
                if high_idx in bye_indices and current_round == 0:
                    continue

                for low_idx in playing_indices:
                    if low_idx in bye_indices and current_round == 0:
                        continue
                    if low_idx <= high_idx:
                        continue  # low must be lower seeded (higher index)

                    # Check if in any simulation, high_idx is the (high_seed_rank)th
                    # highest alive seed and low_idx is the corresponding lowest

                    # Count how many playing teams with index < high_idx are alive
                    higher_alive = np.zeros(n_sims, dtype=int)
                    for check_idx in playing_indices:
                        if check_idx < high_idx:
                            higher_alive += alive[:, check_idx].astype(int)

                    # high_idx is the (high_seed_rank)th highest if exactly
                    # high_seed_rank teams with lower index are alive
                    is_high_seed_at_rank = (higher_alive == high_seed_rank) & alive[:, high_idx]

                    # Count how many playing teams with index > low_idx are alive
                    lower_alive = np.zeros(n_sims, dtype=int)
                    for check_idx in playing_indices:
                        if check_idx > low_idx:
                            lower_alive += alive[:, check_idx].astype(int)

                    # low_idx is the (high_seed_rank)th lowest if exactly
                    # high_seed_rank teams with higher index are alive
                    is_low_seed_at_rank = (lower_alive == high_seed_rank) & alive[:, low_idx]

                    # This matchup happens where both conditions are true
                    is_this_matchup = is_high_seed_at_rank & is_low_seed_at_rank

                    if not is_this_matchup.any():
                        continue

                    # Simulate this matchup
                    a_won, b_won = simulate_matchup_reseeding(high_idx, low_idx, current_round)

                    # Update alive status only for sims where this is the matchup
                    # Winner stays alive, loser is eliminated
                    new_alive[:, high_idx] = np.where(is_this_matchup, a_won, new_alive[:, high_idx])
                    new_alive[:, low_idx] = np.where(is_this_matchup, b_won, new_alive[:, low_idx])

        alive = new_alive
        current_round += 1

    # Calculate probabilities
    p_semis = {}
    p_final = {}
    p_champ = {}

    for idx, mgr in enumerate(managers):
        p_semis[mgr] = 100.0 * reached_semis[:, idx].sum() / n_sims
        p_final[mgr] = 100.0 * reached_final[:, idx].sum() / n_sims
        p_champ[mgr] = 100.0 * won_champ[:, idx].sum() / n_sims

    return {"p_semis": p_semis, "p_final": p_final, "p_champ": p_champ}


# =============================================================================
# SCORE GENERATION HELPER
# =============================================================================


def generate_round_scores(
    mu_arr: np.ndarray,
    sigma_arr: np.ndarray,
    n_sims: int,
    n_teams: int,
    max_rounds: int,
    playoff_round_type: int,
    round_actuals: "dict[tuple[int, int], dict[str, float]] | None",
    mgr_to_idx: "dict[str, int]",
    rng: np.random.Generator,
    round_week_ranges: "list[tuple[int, int]]",
) -> "tuple[np.ndarray, np.ndarray]":
    """Generate combined scores for all rounds across all simulations.

    For 2-week rounds: sum of two independent draws from N(mu, sigma).
    For 1-week rounds: single draw.
    When round_actuals provides week 1 scores: substitute actual for first draw.

    Args:
        mu_arr: (n_teams,) expected scores per team
        sigma_arr: (n_teams,) score volatility per team
        n_sims: number of Monte Carlo simulations
        n_teams: number of teams
        max_rounds: max rounds to generate scores for
        playoff_round_type: 0=single, 1=all 2-week, 2=championship only
            (unused directly — round_week_ranges is the source of truth)
        round_actuals: {(wk_start, wk_end): {franchise_id: week1_points}}
        mgr_to_idx: {franchise_id: array_index}
        rng: numpy random generator
        round_week_ranges: [(wk_start, wk_end), ...] from round_weeks()

    Returns:
        all_scores: (n_sims, n_teams, max_rounds) combined round scores
        all_scores_week2: (n_sims, n_teams, max_rounds) week 2 draws
    """
    all_scores_week1 = rng.normal(
        mu_arr[np.newaxis, :, np.newaxis],
        sigma_arr[np.newaxis, :, np.newaxis],
        size=(n_sims, n_teams, max_rounds),
    )
    all_scores_week2 = rng.normal(
        mu_arr[np.newaxis, :, np.newaxis],
        sigma_arr[np.newaxis, :, np.newaxis],
        size=(n_sims, n_teams, max_rounds),
    )

    # Determine which rounds are 2-week from the week ranges
    is_two_week = np.zeros(max_rounds, dtype=bool)
    for round_idx, (wk_start, wk_end) in enumerate(round_week_ranges):
        if round_idx < max_rounds and wk_start != wk_end:
            is_two_week[round_idx] = True

    # Combined scores: sum both draws for 2-week rounds, single draw for 1-week
    all_scores = np.where(
        is_two_week[np.newaxis, np.newaxis, :],
        all_scores_week1 + all_scores_week2,
        all_scores_week1,
    )

    # Apply actuals: for rounds with week 1 data, replace first draw with actual
    if round_actuals:
        for week_range, team_scores in round_actuals.items():
            # Linear scan — R <= 5, avoids dict alignment assumptions
            round_idx = None
            for i, (wk_s, wk_e) in enumerate(round_week_ranges):
                if i < max_rounds and (wk_s, wk_e) == week_range:
                    round_idx = i
                    break
            if round_idx is not None and is_two_week[round_idx]:
                for team_id, actual_pts in team_scores.items():
                    if team_id in mgr_to_idx:
                        tidx = mgr_to_idx[team_id]
                        all_scores[:, tidx, round_idx] = actual_pts + all_scores_week2[:, tidx, round_idx]

    return all_scores, all_scores_week2


# =============================================================================
# MAIN BRACKET SIMULATION
# =============================================================================


def simulate_playoff_bracket_vectorized(
    teams_alive: list[str],
    seeds_map: dict[str, int],
    mu_hat: dict[str, float],
    sigma_hat: dict[str, float],
    n_sims: int = 10000,
    bye_teams: int = 0,
    uses_reseeding: bool = False,
    rng_seed: int | None = None,
    actual_results: dict[tuple[str, str], str] | None = None,
    original_playoff_teams: int | None = None,
    playoff_round_type: int = 0,
    round_week_ranges: list[tuple[int, int]] | None = None,
    round_actuals: dict[tuple[int, int], dict[str, float]] | None = None,
) -> dict[str, dict[str, float]]:
    """
    Vectorized Monte Carlo simulation of playoff bracket.

    Pre-generates all random scores and uses numpy operations to simulate
    all playoff rounds in parallel across all simulations.

    For standard brackets (no reseeding), matchups are fixed based on seeds
    and bracket sides (A vs B), allowing full vectorization.

    Args:
        teams_alive: List of teams still alive in playoffs
        seeds_map: Dict mapping manager -> seed number
        mu_hat: Dict mapping manager -> expected score (mean)
        sigma_hat: Dict mapping manager -> score volatility (std dev)
        n_sims: Number of simulations
        bye_teams: Number of teams with first-round byes (default 0)
        uses_reseeding: Whether to reseed bracket after each round
        rng_seed: Random seed for reproducibility
        actual_results: Dict of (teamA, teamB) -> winner for games already played
        original_playoff_teams: Original playoff bracket size for determining bracket sides
                                (e.g., 6 for a 6-team bracket even if only 4 remain)

    Returns:
        Dict with probability keys:
        - 'p_semis': Dict[manager] -> probability of making semifinals (0-100)
        - 'p_final': Dict[manager] -> probability of making finals (0-100)
        - 'p_champ': Dict[manager] -> probability of winning championship (0-100)

    Raises:
        ValueError: If inputs are invalid
        TypeError: If inputs are wrong type
    """
    # =================================================================
    # INPUT VALIDATION
    # =================================================================

    # Validate teams_alive
    if teams_alive is None:
        raise TypeError("teams_alive cannot be None")
    if not isinstance(teams_alive, (list, tuple)):
        raise TypeError(f"teams_alive must be a list or tuple, got {type(teams_alive).__name__}")

    # Validate seeds_map
    if seeds_map is None:
        raise TypeError("seeds_map cannot be None")
    if not isinstance(seeds_map, dict):
        raise TypeError(f"seeds_map must be a dict, got {type(seeds_map).__name__}")

    # Validate mu_hat and sigma_hat
    if mu_hat is None:
        raise TypeError("mu_hat cannot be None")
    if not isinstance(mu_hat, dict):
        raise TypeError(f"mu_hat must be a dict, got {type(mu_hat).__name__}")
    if sigma_hat is None:
        raise TypeError("sigma_hat cannot be None")
    if not isinstance(sigma_hat, dict):
        raise TypeError(f"sigma_hat must be a dict, got {type(sigma_hat).__name__}")

    # Validate n_sims
    if not isinstance(n_sims, (int, np.integer)) or n_sims < 1:
        raise ValueError(f"n_sims must be a positive integer, got {n_sims}")

    # Validate bye_teams
    if not isinstance(bye_teams, (int, np.integer)) or bye_teams < 0:
        raise ValueError(f"bye_teams must be a non-negative integer, got {bye_teams}")
    if bye_teams > len(teams_alive):
        raise ValueError(f"bye_teams ({bye_teams}) cannot exceed teams_alive count ({len(teams_alive)})")

    # Validate original_playoff_teams
    if original_playoff_teams is not None:
        if not isinstance(original_playoff_teams, (int, np.integer)) or original_playoff_teams < 2:
            raise ValueError(f"original_playoff_teams must be >= 2 or None, got {original_playoff_teams}")
        if original_playoff_teams < len(teams_alive):
            raise ValueError(
                f"original_playoff_teams ({original_playoff_teams}) cannot be less than "
                f"teams_alive count ({len(teams_alive)})"
            )

    # Validate actual_results
    if actual_results is not None and not isinstance(actual_results, dict):
        raise TypeError(f"actual_results must be a dict or None, got {type(actual_results).__name__}")

    normalized_seeds_map = {}
    for team, seed in seeds_map.items():
        if team is None or pd.isna(team) or seed is None or pd.isna(seed):
            continue
        normalized_seeds_map[team] = _coerce_positive_integer(seed, "seed")
    seeds_map = normalized_seeds_map

    # Warn about missing seeds/stats (not errors - we have defaults)
    missing_seeds = [t for t in teams_alive if t not in seeds_map]
    if missing_seeds:
        logger.warning(f"Teams missing from seeds_map (using default seed 999): {missing_seeds}")

    missing_mu = [t for t in teams_alive if t not in mu_hat]
    if missing_mu:
        logger.warning(f"Teams missing from mu_hat (using average): {missing_mu}")

    missing_sigma = [t for t in teams_alive if t not in sigma_hat]
    if missing_sigma:
        logger.warning(f"Teams missing from sigma_hat (using average): {missing_sigma}")

    # =================================================================
    # EARLY RETURN FOR TRIVIAL CASES
    # =================================================================

    if len(teams_alive) <= 1:
        result = {"p_semis": {}, "p_final": {}, "p_champ": {}}
        if len(teams_alive) == 1:
            winner = teams_alive[0]
            result["p_semis"][winner] = 100.0
            result["p_final"][winner] = 100.0
            result["p_champ"][winner] = 100.0
        return result

    if actual_results is None:
        actual_results = {}

    # =================================================================
    # RESEEDING BRACKET (if enabled)
    # =================================================================
    # With reseeding, after each round teams are re-ranked by seed and
    # highest plays lowest. No fixed bracket sides.
    if uses_reseeding:
        return _simulate_reseeding_bracket(
            teams_alive=teams_alive,
            seeds_map=seeds_map,
            mu_hat=mu_hat,
            sigma_hat=sigma_hat,
            n_sims=n_sims,
            bye_teams=bye_teams,
            rng_seed=rng_seed,
            actual_results=actual_results,
            playoff_round_type=playoff_round_type,
            round_week_ranges=round_week_ranges,
            round_actuals=round_actuals,
        )

    # Create index mappings - sort by seed for consistent bracket positions
    sorted_teams = sorted(teams_alive, key=lambda m: seeds_map.get(m, 999))
    managers = sorted_teams
    n_teams = len(managers)
    mgr_to_idx = {m: i for i, m in enumerate(managers)}

    # Determine bracket size for side assignment
    # Use original_playoff_teams if provided, otherwise infer from max seed in seeds_map
    if original_playoff_teams is not None:
        bracket_size = original_playoff_teams
    else:
        # Infer from seeds - use max seed that's in the alive teams
        max_seed = max(seeds_map.get(m, 0) for m in teams_alive)
        bracket_size = max(n_teams, max_seed)

    # Assign bracket sides based on seeds and ORIGINAL bracket structure
    # This ensures teams stay on the correct side even after eliminations
    side_a_indices = []
    side_b_indices = []
    for idx, mgr in enumerate(managers):
        seed = seeds_map.get(mgr, 999)
        if _get_bracket_side(seed, bracket_size) == "A":
            side_a_indices.append(idx)
        else:
            side_b_indices.append(idx)

    # Build parameter arrays (already sorted by seed)
    default_mu = np.mean([mu_hat.get(m, 100.0) for m in managers])
    default_sigma = np.mean([sigma_hat.get(m, 15.0) for m in managers])
    mu_arr = np.array([mu_hat.get(m, default_mu) for m in managers])
    sigma_arr = np.array([sigma_hat.get(m, default_sigma) for m in managers])

    # Initialize RNG
    rng = np.random.default_rng(rng_seed)

    # Pre-draw ALL random scores upfront
    # Shape: (n_sims, n_teams, max_rounds)
    # Calculate max rounds needed: ceil(log2(n_teams)) + 1 for safety
    max_rounds = max(4, math.ceil(math.log2(n_teams)) + 2)
    _round_ranges = round_week_ranges or [(i, i) for i in range(max_rounds)]
    all_scores, _all_scores_wk2 = generate_round_scores(
        mu_arr,
        sigma_arr,
        n_sims,
        n_teams,
        max_rounds,
        playoff_round_type,
        round_actuals,
        {m: i for i, m in enumerate(managers)},
        rng,
        _round_ranges,
    )

    # Track alive status per side: alive_mask[sim, team_idx] = True if alive
    alive_side_a = np.zeros((n_sims, n_teams), dtype=bool)
    alive_side_b = np.zeros((n_sims, n_teams), dtype=bool)
    for idx in side_a_indices:
        alive_side_a[:, idx] = True
    for idx in side_b_indices:
        alive_side_b[:, idx] = True

    # Track stage reached
    reached_semis = np.zeros((n_sims, n_teams), dtype=bool)
    reached_final = np.zeros((n_sims, n_teams), dtype=bool)
    won_champ = np.zeros((n_sims, n_teams), dtype=bool)

    def simulate_matchup(team_a_idx, team_b_idx, round_num, alive_mask_a, alive_mask_b):
        """Simulate a single matchup across all simulations, returns winner mask."""
        team_a = managers[team_a_idx]
        team_b = managers[team_b_idx]

        # Check for actual result
        matchup_key = tuple(sorted([team_a, team_b]))
        actual_winner = actual_results.get(matchup_key)
        if actual_winner is None:
            actual_winner = actual_results.get((team_a, team_b))
        if actual_winner is None:
            actual_winner = actual_results.get((team_b, team_a))

        both_alive = alive_mask_a[:, team_a_idx] & alive_mask_b[:, team_b_idx]

        a_won = np.zeros(n_sims, dtype=bool)
        b_won = np.zeros(n_sims, dtype=bool)

        if actual_winner:
            # Use actual result
            if actual_winner == team_a:
                a_won = both_alive
            else:
                b_won = both_alive
        else:
            # Simulate
            score_a = all_scores[:, team_a_idx, round_num]
            score_b = all_scores[:, team_b_idx, round_num]

            a_wins = (score_a > score_b) & both_alive
            b_wins = (score_b > score_a) & both_alive
            ties = (score_a == score_b) & both_alive

            # Deterministic tie-breaking: higher seed (lower number) wins
            seed_a = seeds_map.get(team_a, 999)
            seed_b = seeds_map.get(team_b, 999)
            if seed_a <= seed_b:
                a_wins_tie = ties
                b_wins_tie = np.zeros(n_sims, dtype=bool)
            else:
                a_wins_tie = np.zeros(n_sims, dtype=bool)
                b_wins_tie = ties

            a_won = a_wins | a_wins_tie
            b_won = b_wins | b_wins_tie

        return a_won, b_won

    def run_side_bracket(side_indices, alive_mask, round_start, bye_count):
        """Run bracket for one side, returns updated alive mask and round used."""
        if len(side_indices) <= 1:
            return alive_mask, round_start

        # Sort by seed (indices are already in seed order from sorted_teams)
        current_round = round_start

        # Determine bye teams on this side (top seeds)
        bye_indices = side_indices[:bye_count] if bye_count > 0 else []
        playing_indices = side_indices[bye_count:] if bye_count > 0 else side_indices

        # Mark semifinals BEFORE round 0: if this side starts with <= 2 teams,
        # ALL of them are semifinalists (they're playing IN the semis).
        # This handles 4-team brackets where round 1 IS the semifinals.
        side_alive_pre = sum(alive_mask[:, idx] for idx in side_indices)
        for idx in side_indices:
            semis_pre = (side_alive_pre <= 2) & alive_mask[:, idx]
            reached_semis[:, idx] |= semis_pre

        # Round 0: Wild card games (non-bye teams play)
        if len(playing_indices) >= 2:
            n_matchups = len(playing_indices) // 2
            winners = alive_mask.copy()

            for m_idx in range(n_matchups):
                higher_idx = playing_indices[m_idx]
                lower_idx = playing_indices[-(m_idx + 1)]

                a_won, b_won = simulate_matchup(higher_idx, lower_idx, current_round, alive_mask, alive_mask)

                # Update winners: loser is no longer alive
                winners[:, higher_idx] = alive_mask[:, higher_idx] & a_won
                winners[:, lower_idx] = alive_mask[:, lower_idx] & b_won

            alive_mask = winners
            current_round += 1

        # Subsequent rounds until one team remains
        while True:
            # Count alive on this side
            side_alive_counts = sum(alive_mask[:, idx] for idx in side_indices)

            # Mark semifinals (this side has 2 or fewer)
            for idx in side_indices:
                semis_reached = (side_alive_counts <= 2) & alive_mask[:, idx]
                reached_semis[:, idx] |= semis_reached

            # If only 1 alive on this side, done
            if (side_alive_counts <= 1).all():
                break

            # Get alive team indices per simulation for matchups
            winners = alive_mask.copy()

            # For each simulation, pair highest vs lowest seed among alive teams
            # This is tricky to vectorize perfectly, but we can handle common cases

            # Collect alive teams in seed order
            alive_in_order = []
            for idx in side_indices:
                alive_in_order.append((idx, alive_mask[:, idx]))

            # For standard 2-team matchup (most common after wild card)
            alive_count_mode = int(np.median(side_alive_counts[side_alive_counts > 1]))

            if alive_count_mode == 2:
                # Find the two alive teams for each sim
                # Since side_indices is in seed order, first alive is higher seed
                for i, (idx_a, alive_a) in enumerate(alive_in_order):
                    for _j, (idx_b, alive_b) in enumerate(alive_in_order[i + 1 :], i + 1):
                        both = alive_a & alive_b

                        a_won, b_won = simulate_matchup(idx_a, idx_b, current_round, alive_mask, alive_mask)

                        # Only update sims where these two were the matchup
                        # (both alive and exactly 2 alive total on this side)
                        is_this_matchup = both & (side_alive_counts == 2)
                        winners[:, idx_a] = np.where(is_this_matchup, a_won, winners[:, idx_a])
                        winners[:, idx_b] = np.where(is_this_matchup, b_won, winners[:, idx_b])
            else:
                # More than 2 teams: pair highest vs lowest iteratively
                for _ in range(alive_count_mode // 2):
                    # Find first and last alive
                    first_alive_idx = None
                    last_alive_idx = None

                    for idx in side_indices:
                        mask = winners[:, idx]
                        if mask.any():
                            if first_alive_idx is None:
                                first_alive_idx = idx
                            last_alive_idx = idx

                    if first_alive_idx is not None and last_alive_idx is not None and first_alive_idx != last_alive_idx:
                        a_won, b_won = simulate_matchup(
                            first_alive_idx, last_alive_idx, current_round, winners, winners
                        )
                        winners[:, first_alive_idx] &= a_won
                        winners[:, last_alive_idx] &= b_won

            alive_mask = winners
            current_round += 1

            if current_round > round_start + 3:  # Safety limit
                break

        return alive_mask, current_round

    # SPECIAL CASE: Championship game with only 2 teams
    # When simulating from the finals week, we have exactly 2 teams.
    # They might be on the same bracket side (e.g., seeds 1 and 5 both on Side A).
    # Skip the side brackets and go directly to the championship game.
    if len(managers) == 2 and (len(side_a_indices) == 2 or len(side_b_indices) == 2):
        idx_0, idx_1 = 0, 1
        # Use combined alive mask since they're on the same side
        combined_alive = alive_side_a if len(side_a_indices) == 2 else alive_side_b
        both_finalists = combined_alive[:, idx_0] & combined_alive[:, idx_1]

        # Mark both as finalists and semifinalists
        reached_semis[:, idx_0] |= both_finalists
        reached_semis[:, idx_1] |= both_finalists
        reached_final[:, idx_0] |= both_finalists
        reached_final[:, idx_1] |= both_finalists

        if both_finalists.any():
            # Simulate the championship matchup
            team_a = managers[idx_0]
            team_b = managers[idx_1]

            # Check for actual result
            actual_winner = actual_results.get((team_a, team_b)) or actual_results.get((team_b, team_a))

            a_won = np.zeros(n_sims, dtype=bool)
            b_won = np.zeros(n_sims, dtype=bool)

            if actual_winner:
                if actual_winner == team_a:
                    a_won[both_finalists] = True
                else:
                    b_won[both_finalists] = True
            else:
                # Simulate using pre-drawn scores
                scores_a = all_scores[both_finalists, idx_0, 0]  # Round 0 for finals
                scores_b = all_scores[both_finalists, idx_1, 0]
                ties = scores_a == scores_b
                # Deterministic tie-breaking: higher seed (lower number) wins
                seed_0 = seeds_map.get(managers[idx_0], 999)
                seed_1 = seeds_map.get(managers[idx_1], 999)
                a_beats_b = (scores_a > scores_b) | (ties & (seed_0 <= seed_1))
                a_won_mask = np.zeros(n_sims, dtype=bool)
                a_won_mask[both_finalists] = a_beats_b
                a_won = a_won_mask
                b_won = both_finalists & ~a_won

            won_champ[:, idx_0] |= a_won
            won_champ[:, idx_1] |= b_won

        # Calculate probabilities and return early
        p_semis = {}
        p_final = {}
        p_champ = {}
        for idx, mgr in enumerate(managers):
            p_semis[mgr] = 100.0 * reached_semis[:, idx].sum() / n_sims
            p_final[mgr] = 100.0 * reached_final[:, idx].sum() / n_sims
            p_champ[mgr] = 100.0 * won_champ[:, idx].sum() / n_sims

        return {"p_semis": p_semis, "p_final": p_final, "p_champ": p_champ}

    # Normal case: multiple teams, run side brackets
    # Determine byes per side
    # For 6 teams with 2 byes: side A has 1 bye (seed 1), side B has 1 bye (seed 2)
    # Seeds are distributed: Side A = [1, 4, 5], Side B = [2, 3, 6]
    # Byes go to seeds 1 and 2
    bye_seeds = set(range(1, bye_teams + 1))
    side_a_byes = sum(1 for idx in side_a_indices if seeds_map.get(managers[idx], 999) in bye_seeds)
    side_b_byes = sum(1 for idx in side_b_indices if seeds_map.get(managers[idx], 999) in bye_seeds)

    # Run each side's bracket
    alive_side_a, round_used_a = run_side_bracket(side_a_indices, alive_side_a, 0, side_a_byes)
    alive_side_b, round_used_b = run_side_bracket(side_b_indices, alive_side_b, 0, side_b_byes)

    # Finals: winner of side A vs winner of side B
    finals_round = max(round_used_a, round_used_b)

    # Find the finalist from each side
    for idx in side_a_indices:
        finalist_mask = alive_side_a[:, idx]
        reached_final[:, idx] |= finalist_mask

    for idx in side_b_indices:
        finalist_mask = alive_side_b[:, idx]
        reached_final[:, idx] |= finalist_mask

    # Simulate finals
    # Normal case: finalists from opposite sides
    # For each simulation, find the one alive from each side
    for idx_a in side_a_indices:
        for idx_b in side_b_indices:
            both_finalists = alive_side_a[:, idx_a] & alive_side_b[:, idx_b]

            if both_finalists.any():
                a_won, b_won = simulate_matchup(idx_a, idx_b, finals_round, alive_side_a, alive_side_b)

                # Mark champions
                won_champ[:, idx_a] |= both_finalists & a_won
                won_champ[:, idx_b] |= both_finalists & b_won

    # Finalists who made finals but didn't win are still finalists
    reached_final |= won_champ

    # Semifinalists include all finalists
    reached_semis |= reached_final

    # Calculate probabilities
    p_semis = {}
    p_final = {}
    p_champ = {}

    for idx, mgr in enumerate(managers):
        p_semis[mgr] = 100.0 * reached_semis[:, idx].sum() / n_sims
        p_final[mgr] = 100.0 * reached_final[:, idx].sum() / n_sims
        p_champ[mgr] = 100.0 * won_champ[:, idx].sum() / n_sims

    return {"p_semis": p_semis, "p_final": p_final, "p_champ": p_champ}
