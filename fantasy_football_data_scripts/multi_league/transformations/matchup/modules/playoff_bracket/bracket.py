"""
Bracket — deterministic playoff bracket model.

Given settings and game results, traces the bracket forward round-by-round
to identify the champion, placement games, and round assignments.

Works for any bracket size (2-64 teams), all round types (0/1/2),
fixed brackets and reseeding.

Settings:
    num_playoff_teams (2-64)
    playoff_start_week (int)
    round_type: 0 = single-week rounds, 1 = all 2-week rounds, 2 = 2-week finals only
    uses_reseeding (bool)
    num_teams (total league size, informational)

Derived:
    num_rounds = ceil(log2(num_playoff_teams))
    bracket_size = 2^num_rounds
    byes = bracket_size - num_playoff_teams
"""

from __future__ import annotations

import math
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def _seed_order(n: int) -> list[int]:
    """
    Generate standard bracket seed positions using recursive halving.

    For n=8: [1, 8, 4, 5, 2, 7, 3, 6]
    Paired: (1v8), (4v5), (2v7), (3v6)

    This ensures top seeds are on opposite sides of the bracket and
    produces the standard NCAA/NFL bracket structure.
    """
    if n == 1:
        return [1]
    half = _seed_order(n // 2)
    return [item for seed in half for item in (seed, n + 1 - seed)]


class Bracket:
    """
    A deterministic single-elimination bracket.

    Construct with settings, then feed in game results via record_result().
    Query matchups, week ranges, and champion at any point.
    """

    def __init__(
        self,
        num_playoff_teams: int = 6,
        playoff_start_week: int = 15,
        round_type: int = 0,
        uses_reseeding: bool = False,
        num_teams: int = 12,
    ):
        if num_playoff_teams < 2:
            raise ValueError(f"num_playoff_teams must be >= 2, got {num_playoff_teams}")

        self.num_playoff_teams = num_playoff_teams
        self.playoff_start_week = playoff_start_week
        self.round_type = round_type
        self.uses_reseeding = uses_reseeding
        self.num_teams = num_teams

        # Derived
        self.num_rounds = math.ceil(math.log2(num_playoff_teams))
        self.bracket_size = 2**self.num_rounds
        self.byes = self.bracket_size - num_playoff_teams

        # Build initial bracket slot assignments: list of seeds in bracket-position order.
        # Seeds > num_playoff_teams are byes (empty slots).
        self._initial_slots = _seed_order(self.bracket_size)

        # Results storage: round_num -> set of (winner_seed, loser_seed)
        self._results: dict[int, set[tuple[int, int]]] = {}

    # ------------------------------------------------------------------
    # Public query API
    # ------------------------------------------------------------------

    def get_bye_seeds(self) -> set[int]:
        """Seeds that auto-advance past round 1 (have a bye)."""
        bye_seeds = set()
        for seed in self._initial_slots:
            if seed > self.num_playoff_teams:
                # The opponent of this bye slot gets a bye
                idx = self._initial_slots.index(seed)
                # Slots are paired: (0,1), (2,3), (4,5), ...
                partner_idx = idx ^ 1  # flip last bit to get partner
                partner_seed = self._initial_slots[partner_idx]
                if partner_seed <= self.num_playoff_teams:
                    bye_seeds.add(partner_seed)
        return bye_seeds

    def round_matchups(self, round_num: int) -> list[tuple[int, int]]:
        """
        Return actual games for a round (excludes byes).

        Each element is (seed_a, seed_b) where seed_a < seed_b (higher seed first).

        For round 1: uses initial bracket slots, filtering out bye matchups.
        For later rounds: uses winners from previous rounds.
        """
        if round_num < 1 or round_num > self.num_rounds:
            return []

        if round_num == 1:
            return self._round1_matchups()

        # Get advancing teams for this round
        advancing = self._get_advancing_teams(round_num)
        if len(advancing) < 2:
            return []

        if self.uses_reseeding:
            # Sort by seed, pair best vs worst
            sorted_seeds = sorted(advancing)
            matchups = []
            n = len(sorted_seeds)
            for i in range(n // 2):
                a, b = sorted_seeds[i], sorted_seeds[n - 1 - i]
                matchups.append((min(a, b), max(a, b)))
            return matchups
        else:
            # Fixed bracket: winners advance by bracket position
            return self._fixed_bracket_round(round_num)

    def record_result(self, round_num: int, winner_seed: int, loser_seed: int) -> None:
        """Record the result of a game in the given round."""
        if round_num not in self._results:
            self._results[round_num] = set()
        self._results[round_num].add((winner_seed, loser_seed))

    def weeks_for_round(self, round_num: int) -> int:
        """Number of weeks this round spans (1 or 2)."""
        if self.round_type == 0:
            return 1
        elif self.round_type == 1:
            return 2
        elif self.round_type == 2:
            # Only the final round is 2 weeks
            return 2 if round_num == self.num_rounds else 1
        return 1

    def week_range_for_round(self, round_num: int) -> tuple[int, int]:
        """Return (first_week, last_week) for the given round."""
        # Calculate the start week for this round by summing weeks of prior rounds
        week = self.playoff_start_week
        for r in range(1, round_num):
            week += self.weeks_for_round(r)
        first_week = week
        last_week = week + self.weeks_for_round(round_num) - 1
        return (first_week, last_week)

    @property
    def championship_week(self) -> int:
        """The last week of the finals round."""
        _, last = self.week_range_for_round(self.num_rounds)
        return last

    def get_champion_seed(self) -> int | None:
        """Winner of the final round, or None if finals haven't been played."""
        final_results = self._results.get(self.num_rounds, set())
        if not final_results:
            return None
        # Should be exactly one game in the finals
        for winner, _ in final_results:
            return winner
        return None

    def is_championship_game(
        self,
        manager_a: str,
        manager_b: str,
        week: int,
        seeds: dict[str, int],
    ) -> bool:
        """
        Return True if the game between manager_a and manager_b in the given
        week is the championship game (not a placement game).

        Args:
            manager_a: Name of first manager
            manager_b: Name of second manager
            week: Week number
            seeds: Dict mapping manager name -> playoff seed
        """
        # Check if this week falls within the championship round
        champ_first, champ_last = self.week_range_for_round(self.num_rounds)
        if week < champ_first or week > champ_last:
            return False

        # Get the seeds of the two managers
        seed_a = seeds.get(manager_a)
        seed_b = seeds.get(manager_b)
        if seed_a is None or seed_b is None:
            return False

        # Get the expected finalists by tracing the bracket
        finalists = self._get_advancing_teams(self.num_rounds)
        if len(finalists) != 2:
            # Can't determine finalists yet
            return False

        return {seed_a, seed_b} == set(finalists)

    def fill_from_matchup_data(
        self,
        df: pd.DataFrame,
        year: int,
        seeds: dict[str, int],
    ) -> None:
        """
        Fill bracket results from matchup data.

        For each round, find games in the round's week(s) where both teams
        are expected participants, record the winner.

        Args:
            df: Matchup DataFrame (needs franchise_id / opponent_franchise_id / week /
                win / team_points columns, or manager / opponent as a fallback)
            year: Season year to filter
            seeds: Dict mapping team id -> playoff seed. Caller determines whether
                the keys are `franchise_id` values or `manager` names; the id column
                used here matches that choice.
        """

        year_df = df[df["year"] == year].copy()

        # Match the id column to the seeds key type: prefer franchise_id when the
        # column is populated (this is the caller's default when franchise_id exists).
        if "franchise_id" in year_df.columns and year_df["franchise_id"].notna().any():
            id_col = "franchise_id"
            opp_col = "opponent_franchise_id" if "opponent_franchise_id" in year_df.columns else "opponent"
        else:
            id_col = "manager"
            opp_col = "opponent"

        seed_to_id = {v: k for k, v in seeds.items()}
        id_to_seed = seeds

        for round_num in range(1, self.num_rounds + 1):
            first_week, last_week = self.week_range_for_round(round_num)
            round_df = year_df[(year_df["week"] >= first_week) & (year_df["week"] <= last_week)]

            expected = self.round_matchups(round_num)

            for seed_a, seed_b in expected:
                if seed_a == 0 or seed_b == 0:
                    continue  # bye

                id_a = seed_to_id.get(seed_a)
                id_b = seed_to_id.get(seed_b)
                if not id_a or not id_b:
                    continue

                # Find the game between these two teams
                game = round_df[
                    ((round_df[id_col] == id_a) & (round_df[opp_col] == id_b))
                    | ((round_df[id_col] == id_b) & (round_df[opp_col] == id_a))
                ]

                if game.empty:
                    continue

                # For multiweek rounds, sum points across weeks
                if last_week > first_week:
                    pts = {}
                    for team_id in [id_a, id_b]:
                        team_games = game[game[id_col] == team_id]
                        pts[team_id] = team_games["team_points"].sum() if not team_games.empty else 0
                    winner = max(pts, key=pts.get)
                    loser = id_b if winner == id_a else id_a
                else:
                    # Single week: check win flag first, then points
                    winner_rows = game[game["win"] == 1]
                    if not winner_rows.empty:
                        winner = winner_rows.iloc[0][id_col]
                        loser = id_b if winner == id_a else id_a
                    else:
                        # Fallback to points comparison
                        row = game.iloc[0]
                        tp = row.get("team_points", 0) or 0
                        op = row.get("opponent_points", 0) or 0
                        if tp > op:
                            winner, loser = row[id_col], row[opp_col]
                        elif op > tp:
                            winner, loser = row[opp_col], row[id_col]
                        else:
                            # True tie — higher seed wins
                            if id_to_seed.get(id_a, 999) < id_to_seed.get(id_b, 999):
                                winner, loser = id_a, id_b
                            else:
                                winner, loser = id_b, id_a

                self.record_result(
                    round_num,
                    winner_seed=id_to_seed[winner],
                    loser_seed=id_to_seed[loser],
                )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _round1_matchups(self) -> list[tuple[int, int]]:
        """Get round 1 matchups, filtering out byes."""
        matchups = []
        slots = self._initial_slots
        for i in range(0, len(slots), 2):
            a, b = slots[i], slots[i + 1]
            # Skip if either side is a bye (seed > num_playoff_teams)
            if a > self.num_playoff_teams or b > self.num_playoff_teams:
                continue
            matchups.append((min(a, b), max(a, b)))
        return matchups

    def _get_round_winners(self, round_num: int) -> list[int]:
        """
        Get winners from the given round in bracket-position order (for fixed bracket).

        For round 1, this includes both game winners and bye auto-advances,
        ordered by their bracket slot position.
        """
        if round_num == 1:
            return self._get_round1_winners_ordered()

        # For later rounds, we need to know the advancing teams and their positions
        prev_winners = self._get_advancing_teams_ordered(round_num)
        results = self._results.get(round_num, set())
        winners_set = {w for w, _ in results}

        # Return winners in the same position order as they appeared
        ordered = []
        for i in range(0, len(prev_winners), 2):
            if i + 1 >= len(prev_winners):
                # Odd team out (shouldn't happen in valid bracket)
                ordered.append(prev_winners[i])
                continue
            a, b = prev_winners[i], prev_winners[i + 1]
            if a in winners_set:
                ordered.append(a)
            elif b in winners_set:
                ordered.append(b)
            # If neither won yet, skip (incomplete round)
        return ordered

    def _get_round1_winners_ordered(self) -> list[int]:
        """
        Get round 1 winners + bye advances in bracket-position order.

        Each pair of slots (0,1), (2,3), ... produces one advancing team.
        """
        slots = self._initial_slots
        results = self._results.get(1, set())
        winners_set = {w for w, _ in results}
        ordered = []

        for i in range(0, len(slots), 2):
            a, b = slots[i], slots[i + 1]
            if a > self.num_playoff_teams:
                # b gets a bye
                ordered.append(b)
            elif b > self.num_playoff_teams:
                # a gets a bye
                ordered.append(a)
            elif a in winners_set:
                ordered.append(a)
            elif b in winners_set:
                ordered.append(b)
            # else: game not yet played, skip

        return ordered

    def _get_advancing_teams(self, round_num: int) -> list[int]:
        """
        Get the set of teams that should play in the given round.

        For round 1: all non-bye seeds.
        For round N: winners from round N-1 (includes bye advances for round 2).
        """
        if round_num == 1:
            return [s for s in range(1, self.num_playoff_teams + 1) if s not in self.get_bye_seeds()]

        if self.uses_reseeding:
            return self._get_advancing_reseeded(round_num)
        else:
            return self._get_advancing_fixed(round_num)

    def _get_advancing_fixed(self, round_num: int) -> list[int]:
        """Get advancing teams for a fixed bracket (bracket-position order)."""
        # Chain through all previous rounds
        prev_winners = self._get_round_winners(round_num - 1)
        return prev_winners

    def _get_advancing_teams_ordered(self, round_num: int) -> list[int]:
        """Get teams advancing to this round in bracket-position order (fixed bracket)."""
        return self._get_advancing_fixed(round_num)

    def _get_advancing_reseeded(self, round_num: int) -> list[int]:
        """Get advancing teams for a reseeded bracket."""
        # Start with all playoff seeds
        remaining = set(range(1, self.num_playoff_teams + 1))

        # Remove losers from all previous rounds
        for r in range(1, round_num):
            results = self._results.get(r, set())
            for _, loser in results:
                remaining.discard(loser)

        # Also remove seeds that should have played but haven't been recorded
        # (they're still "in" but we return the full remaining set)
        return sorted(remaining)

    def _fixed_bracket_round(self, round_num: int) -> list[tuple[int, int]]:
        """
        Generate matchups for a fixed-bracket round > 1.

        Winners from previous round are in bracket-position order.
        Adjacent pairs play each other.
        """
        ordered = self._get_advancing_fixed(round_num)
        matchups = []
        for i in range(0, len(ordered), 2):
            if i + 1 >= len(ordered):
                break
            a, b = ordered[i], ordered[i + 1]
            matchups.append((min(a, b), max(a, b)))
        return matchups
