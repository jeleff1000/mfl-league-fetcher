"""Probability normalization and hierarchy enforcement for playoff odds."""

import pandas as pd

from multi_league.transformations.matchup.modules.team_model import compress_early_season


def bye_advances_to_semis(num_playoff_teams: int, bye_teams: int) -> bool:
    """Return True when a first-round bye guarantees reaching the top-4 stage."""
    if bye_teams <= 0:
        return False
    teams_after_opening_round = bye_teams + ((max(0, num_playoff_teams - bye_teams) + 1) // 2)
    return teams_after_opening_round <= 4


def enforce_hierarchy(odds: pd.DataFrame, enforce_bye_semis: bool = True) -> pd.DataFrame:
    """Clip from top down: P_Champ <= P_Final <= P_Semis <= P_Playoffs."""
    if "P_Champ" in odds.columns and "P_Final" in odds.columns:
        odds["P_Champ"] = odds[["P_Champ", "P_Final"]].min(axis=1)
    if "P_Final" in odds.columns and "P_Semis" in odds.columns:
        odds["P_Final"] = odds[["P_Final", "P_Semis"]].min(axis=1)
    if "P_Semis" in odds.columns and "P_Playoffs" in odds.columns:
        odds["P_Semis"] = odds[["P_Semis", "P_Playoffs"]].min(axis=1)
    if enforce_bye_semis and "P_Bye" in odds.columns and "P_Semis" in odds.columns:
        odds["P_Bye"] = odds[["P_Bye", "P_Semis"]].min(axis=1)
    return odds


def compress_and_normalize_probabilities(
    odds: pd.DataFrame,
    week: int,
    num_teams: int,
    num_playoff_teams: int,
    bye_teams: int,
) -> pd.DataFrame:
    """Compress early-season extremes, renormalize to sum targets, enforce hierarchy.

    Three steps, each run exactly once:
    1. Compress: shrink each column independently toward its base rate (early weeks)
    2. Renormalize: scale each column so its league-wide sum hits the exact target
    3. Hierarchy: clip so P_Champ <= P_Final <= P_Semis <= P_Playoffs per team
    """
    prob_cols = ["P_Playoffs", "P_Bye", "P_Semis", "P_Final", "P_Champ"]
    for col in prob_cols:
        if col in odds.columns:
            odds[col] = odds[col].clip(lower=0.0, upper=100.0)

    if num_playoff_teams <= 1:
        for col in ["P_Playoffs", "P_Bye", "P_Semis", "P_Final"]:
            if col in odds.columns:
                odds[col] = 0.0

        if "P_Champ" in odds.columns:
            base = (1 / num_teams * 100) if num_teams else 50
            odds["P_Champ"] = odds["P_Champ"].apply(lambda p, b=base: compress_early_season(p, week, base_rate=b))
            champ_sum = odds["P_Champ"].sum()
            if champ_sum > 0:
                odds["P_Champ"] = odds["P_Champ"] * 100.0 / champ_sum
            odds["P_Champ"] = odds["P_Champ"].clip(lower=0.0, upper=100.0)

        return odds

    enforce_bye_semis_flag = bye_advances_to_semis(num_playoff_teams, bye_teams)

    # -- Step 1: Compress early-season extremes --------------------------
    # Each column compressed independently toward its own natural base rate.
    # Base rate = what you'd expect if every team were equally likely.
    bases = {
        "P_Playoffs": (num_playoff_teams / num_teams * 100) if num_teams else 50,
        "P_Bye": (bye_teams / num_teams * 100) if num_teams and bye_teams else 0,
        "P_Semis": (min(num_playoff_teams, 4) / num_teams * 100) if num_teams else 50,
        "P_Final": (2 / num_teams * 100) if num_teams else 50,
        "P_Champ": (1 / num_teams * 100) if num_teams else 50,
    }
    for col, base in bases.items():
        if col in odds.columns:
            odds[col] = odds[col].apply(lambda p, b=base: compress_early_season(p, week, base_rate=b))

    # -- Step 2: Renormalize to exact sum targets ------------------------
    # After compression, sums may have drifted. Rescale each column so the
    # league-wide total matches the mathematical invariant.
    targets = [
        ("P_Playoffs", float(num_playoff_teams) * 100.0),
        ("P_Semis", float(min(num_playoff_teams, 4)) * 100.0),
        ("P_Final", 200.0),
        ("P_Champ", 100.0),
    ]
    if bye_teams > 0:
        targets.append(("P_Bye", float(bye_teams) * 100.0))

    for col, target in targets:
        if col in odds.columns:
            col_sum = odds[col].sum()
            if col_sum > 0:
                odds[col] = odds[col] * target / col_sum

    # -- Step 3: Enforce hierarchy (single pass, final) ------------------
    # This may reduce sums slightly (a few tenths from clipping) but never
    # by 100+ points like the old triple-pass approach.
    odds = enforce_hierarchy(odds, enforce_bye_semis=enforce_bye_semis_flag)

    # Clip to valid range
    for col in prob_cols:
        if col in odds.columns:
            odds[col] = odds[col].clip(lower=0.0, upper=100.0)

    return odds
