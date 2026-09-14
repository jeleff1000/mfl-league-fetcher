"""Shared utilities for championship and consolation bracket tracing.

Common functions used by both bracket_tracer.py and consolation_tracer.py.
No hardcoded values — all bracket geometry comes from settings.
"""

from __future__ import annotations


import pandas as pd


# ---------------------------------------------------------------------------
# ID column resolution
# ---------------------------------------------------------------------------


def resolve_id_col(df: pd.DataFrame, preferred: str) -> str:
    """Resolve which ID column to use for team identity."""
    if preferred in df.columns and df[preferred].notna().any():
        return preferred
    raise ValueError(f"ID column '{preferred}' not found (or all null). Available columns: {list(df.columns)}")


def get_manager_name(year_df: pd.DataFrame, team_id: str, id_col: str) -> str:
    """Map a team_id to its manager name."""
    if id_col == "manager":
        return team_id
    rows = year_df[year_df[id_col] == team_id]
    if not rows.empty and "manager" in rows.columns:
        name = rows.iloc[0]["manager"]
        if pd.notna(name):
            return str(name)
    return team_id


# ---------------------------------------------------------------------------
# Seed extraction
# ---------------------------------------------------------------------------


def extract_seeds(
    year_df: pd.DataFrame,
    id_col: str,
    playoff_teams: int,
) -> dict[str, int]:
    """Return ``{team_id: seed}`` for playoff-qualifying teams only."""
    if "final_playoff_seed" not in year_df.columns:
        return {}
    seeds: dict[str, int] = {}
    for tid in year_df[id_col].dropna().unique():
        vals = year_df.loc[year_df[id_col] == tid, "final_playoff_seed"].dropna()
        if not vals.empty:
            s = int(vals.iloc[0])
            if 1 <= s <= playoff_teams:
                seeds[tid] = s
    return seeds


def extract_all_seeds(
    year_df: pd.DataFrame,
    id_col: str,
) -> dict[str, int]:
    """Return ``{team_id: seed}`` for ALL teams (including non-playoff)."""
    if "final_playoff_seed" not in year_df.columns:
        return {}
    seeds: dict[str, int] = {}
    for tid in year_df[id_col].dropna().unique():
        vals = year_df.loc[year_df[id_col] == tid, "final_playoff_seed"].dropna()
        if not vals.empty:
            seeds[tid] = int(vals.iloc[0])
    return seeds


# ---------------------------------------------------------------------------
# Bracket geometry
# ---------------------------------------------------------------------------


def seed_order(n: int) -> list[int]:
    """Standard bracket seed positions via recursive halving.

    For ``n=8``: ``[1, 8, 4, 5, 2, 7, 3, 6]``
    """
    if n == 1:
        return [1]
    half = seed_order(n // 2)
    return [x for seed in half for x in (seed, n + 1 - seed)]


def build_round1_matchups(
    num_teams: int,
    bye_count: int,
    seed_to_team: dict[int, str],
) -> tuple[list[tuple[int, int]], list[str]]:
    """Build round-1 matchup pairs for a bracket of *num_teams* with *bye_count* byes.

    Returns:
        (matchup_seed_pairs, bye_team_ids)
    """
    bye_seeds = set(range(1, bye_count + 1))
    bye_ids = [seed_to_team[s] for s in sorted(bye_seeds) if s in seed_to_team]

    playing_seeds = sorted(s for s in range(1, num_teams + 1) if s not in bye_seeds)

    # Odd number of playing seeds: highest remaining seed gets an extra bye
    if len(playing_seeds) % 2 != 0:
        extra_bye_seed = playing_seeds.pop(0)
        if extra_bye_seed in seed_to_team:
            bye_ids.append(seed_to_team[extra_bye_seed])

    # Pair highest vs lowest
    n = len(playing_seeds)
    matchups: list[tuple[int, int]] = []
    for i in range(n // 2):
        s_hi = playing_seeds[i]
        s_lo = playing_seeds[n - 1 - i]
        matchups.append((s_hi, s_lo))

    return matchups, bye_ids


# ---------------------------------------------------------------------------
# Winner determination
# ---------------------------------------------------------------------------


def determine_winner(
    year_df: pd.DataFrame,
    wk_start: int,
    wk_end: int,
    team_a: str,
    team_b: str,
    id_col: str,
    opp_id_col: str,
    seeds: dict[str, int],
) -> str | None:
    """Determine the winner between *team_a* and *team_b* across the given
    week range. For multi-week rounds the scores are summed."""
    round_df = year_df[(year_df["week"] >= wk_start) & (year_df["week"] <= wk_end)]

    if opp_id_col not in round_df.columns:
        raise ValueError(f"Opponent ID column '{opp_id_col}' not found in matchup data")

    game = round_df[
        ((round_df[id_col] == team_a) & (round_df[opp_id_col] == team_b))
        | ((round_df[id_col] == team_b) & (round_df[opp_id_col] == team_a))
    ]

    if game.empty:
        return _compare_scores(round_df, team_a, team_b, id_col, seeds)

    # Multi-week: sum points per team
    if wk_end > wk_start:
        pts: dict[str, float] = {}
        for tid in (team_a, team_b):
            team_rows = game[game[id_col] == tid]
            pts[tid] = float(team_rows["team_points"].sum()) if not team_rows.empty else 0.0
        if pts[team_a] > pts[team_b]:
            return team_a
        if pts[team_b] > pts[team_a]:
            return team_b
        return team_a if seeds.get(team_a, 999) < seeds.get(team_b, 999) else team_b

    # Single week: prefer the win flag
    winner_rows = game[game["win"] == 1]
    if not winner_rows.empty:
        return winner_rows.iloc[0][id_col]

    # Fallback: compare points
    row = game.iloc[0]
    tp = float(row.get("team_points", 0) or 0)
    op = float(row.get("opponent_points", 0) or 0)
    row_team = row[id_col]
    other = team_b if row_team == team_a else team_a

    if tp > op:
        return row_team
    if op > tp:
        return other
    return team_a if seeds.get(team_a, 999) < seeds.get(team_b, 999) else team_b


def _compare_scores(
    round_df: pd.DataFrame,
    team_a: str,
    team_b: str,
    id_col: str,
    seeds: dict[str, int],
) -> str | None:
    """Compare individual scores when teams aren't directly matched."""
    pts_a = float(round_df[round_df[id_col] == team_a]["team_points"].sum())
    pts_b = float(round_df[round_df[id_col] == team_b]["team_points"].sum())
    if pts_a == 0 and pts_b == 0:
        return None
    if pts_a > pts_b:
        return team_a
    if pts_b > pts_a:
        return team_b
    return team_a if seeds.get(team_a, 999) < seeds.get(team_b, 999) else team_b
