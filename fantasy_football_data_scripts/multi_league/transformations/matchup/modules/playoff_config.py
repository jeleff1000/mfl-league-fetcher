"""Playoff configuration dataclass — replaces mutable module-level globals."""

import math
from dataclasses import dataclass, field


DEFAULT_TIEBREAKER_ORDER = ["total_points", "head_to_head"]


@dataclass
class PlayoffConfig:
    """Immutable playoff configuration loaded from league settings."""

    playoff_slots: int
    bye_slots: int
    num_teams: int
    regular_season_weeks: int
    use_median: bool = False
    bracket_reseed: bool = False
    tiebreaker_order: list[str] = field(default_factory=lambda: list(DEFAULT_TIEBREAKER_ORDER))

    @property
    def target_cols(self) -> list[str]:
        """Generate canonical target columns for playoff odds outputs."""
        cols = [
            "avg_seed",
            "p_playoffs",
            "p_bye",
            "exp_final_wins",
            "exp_final_pf",
            "p_semis",
            "p_final",
            "p_champ",
            "power_rating",
        ]
        # Keep the canonical x*_seed envelope stable across leagues so smaller
        # brackets simply leave the higher buckets NULL.
        cols.extend([f"x{i}_seed" for i in range(1, 65)])
        # Keep the canonical x*_win envelope stable across leagues so shorter
        # seasons simply leave the higher buckets NULL.
        cols.extend([f"x{i}_win" for i in range(0, 37)])
        return cols


def round_weeks(playoff_teams: int, playoff_start: int, end_week: int, prt: int) -> list[tuple[int, int]]:
    """Compute (wk_start, wk_end) tuples for each playoff round.

    Args:
        playoff_teams: Number of playoff teams.
        playoff_start: First week of the playoffs.
        end_week: Last valid week (clamps round boundaries).
        prt: Playoff round type — 0=all single-week, 1=all two-week,
             2=championship-only two-week.

    Returns:
        List of (wk_start, wk_end) tuples, one per round.
    """
    num_rounds = math.ceil(math.log2(max(playoff_teams, 2)))
    rounds = []
    wk = playoff_start
    for r in range(1, num_rounds + 1):
        span = 2 if (prt == 1 or (prt == 2 and r == num_rounds)) else 1
        wk_end = min(wk + span - 1, end_week)
        rounds.append((wk, wk_end))
        wk = wk_end + 1
    return rounds
