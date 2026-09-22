"""
Sleeper Weekly Matchup Data Fetcher

Fetches head-to-head matchup scores from Sleeper leagues.
Output schema matches weekly_matchup_data_v2.py for downstream compatibility.

Output columns:
- week, year
- manager, manager_guid, team_name
- team_points
- opponent, opponent_points
- margin, win, loss, tie
- matchup_id (for pairing)
- close_margin (within 10 points)
- teams_beat_this_week, league_weekly_mean
- is_playoffs, is_consolation (detected from league settings and bracket APIs)

Note: Sleeper API has a projections endpoint (/projections/nfl/{year}/{week}) but it returns
empty data for all players. Grades and recap URLs are also not available. These columns are null.

Usage:
    from sleeper_matchups import fetch_sleeper_matchups
    from sleeper_context import SleeperContext

    ctx = SleeperContext.load("path/to/sleeper_context.json")
    df = fetch_sleeper_matchups(ctx, year=2024)
"""

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from .sleeper_api_client import SleeperAPIClient
from .sleeper_context import SleeperContext, YearFilter, resolve_years_to_fetch
from .playoff_utils import (
    bracket_team_ids,
    bracket_winner,
    championship_contenders_for_round,
    consolation_rosters_for_round,
    playoff_round_for_week,
    playoff_weeks_for_round,
    resolve_playoff_structure,
)

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function matching Yahoo script pattern."""
    logger.info(msg)
    print(msg)


class SleeperMatchupFetcher:
    """
    Fetch weekly matchup data from Sleeper API.

    Includes head-to-head scores, margins, and derived metrics.
    Output matches Yahoo format for downstream compatibility.
    """

    def __init__(self, ctx: SleeperContext, client: SleeperAPIClient | None = None):
        """
        Initialize the matchup fetcher.

        Args:
            ctx: SleeperContext with league configuration
            client: Optional pre-configured API client
        """
        self.ctx = ctx
        self.client = client or SleeperAPIClient()

        # Cache for playoff structure per league_id
        self._playoff_cache: dict[str, dict[str, Any]] = {}
        self._consolation_rosters_cache: dict[str, set] = {}
        self._winners_bracket_cache: dict[str, list[dict]] = {}
        self._bracket_matchups_cache: dict[str, Any] = {}

    @staticmethod
    def _effective_points(matchup: dict[str, Any]) -> float:
        """Return Sleeper's official fantasy score for a matchup row."""
        score = matchup.get("custom_points")
        if score is None:
            score = matchup.get("points", 0)
        try:
            return float(score or 0)
        except (TypeError, ValueError):
            return 0.0

    def _get_playoff_structure(self, league_id: str) -> dict[str, Any]:
        """
        Get playoff week start and structure from league settings.

        Args:
            league_id: Sleeper league_id

        Returns:
            Dict with playoff_week_start, playoff_teams, etc.
        """
        if league_id in self._playoff_cache:
            return self._playoff_cache[league_id]

        league = self.client.get_league(league_id)
        if not league:
            return {"playoff_week_start": 15, "playoff_week_end": 17, "playoff_teams": 6, "playoff_rounds": 3}

        settings = league.get("settings", {})
        playoff_structure = resolve_playoff_structure(
            settings,
            season_complete=str(league.get("status") or "").strip().lower() == "complete",
        )

        result = {
            "playoff_week_start": playoff_structure["playoff_week_start"],
            "playoff_week_end": playoff_structure["playoff_week_end"],
            "playoff_teams": playoff_structure["playoff_teams"],
            "playoff_round_type": playoff_structure["playoff_round_type"],
            "playoff_rounds": playoff_structure["playoff_rounds"],
            "last_scored_leg": playoff_structure["last_scored_leg"],  # Actual end week from API
        }

        self._playoff_cache[league_id] = result
        return result

    def _get_season_week_bounds(self, league_id: str, year: int) -> tuple[int, int]:
        """
        Get season week bounds from cached league settings.

        Uses data already fetched by _get_playoff_structure:
        - last_scored_leg: The actual end week of the season (most accurate)
        - playoff_week_end: Calculated from playoff structure

        Falls back to 18 weeks if not available. Handles extended playoff seasons.

        Args:
            league_id: Sleeper league_id
            year: Season year (used for capping current season to current week)

        Returns:
            Tuple of (start_week, end_week)
        """
        # Uses cached playoff structure (no extra API call)
        playoff_structure = self._get_playoff_structure(league_id)

        # last_scored_leg is the actual end week (most accurate)
        end_week = playoff_structure.get("last_scored_leg")

        if not end_week or end_week <= 0:
            # Fallback to calculated playoff_week_end
            end_week = playoff_structure.get("playoff_week_end", 17)

        # Safety: Cap at 22 (max for extended playoffs)
        end_week = min(max(end_week, 1), 22)

        # For current season, cap at current NFL week (avoid future weeks)
        nfl_state = self.client.get_nfl_state()
        if nfl_state:
            current_year = nfl_state.get("season", year)
            current_week = nfl_state.get("week", 18)

            if year == current_year and current_week < end_week:
                end_week = current_week

        return 1, end_week

    def _get_consolation_roster_ids(
        self,
        league_id: str,
        week: int,
        active_rosters_this_round: set[int] | None = None,
    ) -> set:
        """
        Get roster_ids participating in consolation bracket for a given week.

        Uses the losers bracket API to identify which teams are in consolation.
        Also includes:
        - Teams playing in placement games (p>=3 like 3rd place, 5th place)
        - Teams who lost in previous championship rounds (QF losers, SF losers, etc.)

        Args:
            league_id: Sleeper league_id
            week: Week number

        Returns:
            Set of roster_ids in consolation bracket for this specific week
        """
        cache_key = f"{league_id}_{week}"
        if cache_key in self._consolation_rosters_cache:
            return self._consolation_rosters_cache[cache_key]

        # Calculate which playoff round this week corresponds to
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_start = playoff_structure.get("playoff_week_start", 15)
        target_round = playoff_round_for_week(
            week,
            playoff_start,
            playoff_structure.get("playoff_round_type", 0),
            playoff_structure.get("playoff_rounds", 1),
        )

        winners_bracket = self._get_winners_bracket(league_id)
        losers_bracket = self.client.get_losers_bracket(league_id) or []
        contenders_cache = self._bracket_matchups_cache.setdefault(f"{league_id}_contenders", {})
        consolation_rosters = consolation_rosters_for_round(
            winners_bracket,
            losers_bracket,
            target_round,
            active_rosters_this_round=active_rosters_this_round,
            contenders_cache=contenders_cache,
        )

        self._consolation_rosters_cache[cache_key] = consolation_rosters
        return consolation_rosters

    def _get_winners_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get cached winners bracket for a league.

        Args:
            league_id: Sleeper league_id

        Returns:
            Winners bracket data from API
        """
        if league_id not in self._winners_bracket_cache:
            bracket = self.client.get_winners_bracket(league_id)
            self._winners_bracket_cache[league_id] = bracket or []
        return self._winners_bracket_cache[league_id]

    def _get_losers_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """
        Get cached losers/consolation bracket for a league.

        Args:
            league_id: Sleeper league_id

        Returns:
            Losers bracket data from API
        """
        cache_key = f"{league_id}_losers"
        if cache_key not in self._winners_bracket_cache:
            bracket = self.client.get_losers_bracket(league_id)
            self._winners_bracket_cache[cache_key] = bracket or []
        return self._winners_bracket_cache[cache_key]

    def _get_championship_contenders(self, league_id: str, target_round: int) -> set:
        """
        Get roster_ids that are STILL IN CHAMPIONSHIP CONTENTION for a given round.

        A team is in championship contention if:
        - Round 1: They appear in the winners bracket (initial playoff qualifiers)
        - Round 2+: They WON in the previous round (tracked via 'w' field)

        This EXCLUDES teams who lost in earlier rounds, even if they're still playing
        games (e.g., 3rd place games, 5th place games, consolation bracket).

        Args:
            league_id: Sleeper league_id
            target_round: The playoff round to check (1 = quarterfinal, 2 = semifinal, etc.)

        Returns:
            Set of roster_ids still competing for 1st place in this round
        """
        contenders_cache = self._bracket_matchups_cache.setdefault(f"{league_id}_contenders", {})
        return championship_contenders_for_round(
            self._get_winners_bracket(league_id),
            target_round,
            contenders_cache,
        )

    def _get_bracket_pairings_by_round(self, league_id: str) -> dict[int, list[tuple[int, int]]]:
        """
        Get playoff matchup pairings from winners bracket, organized by round.

        The winners bracket API returns:
        - r: round number (1, 2, etc.)
        - m: match number within round
        - t1, t2: roster_ids of the teams
        - w: winner roster_id
        - p: placement (1 = championship game, 3 = 3rd place game, etc.)

        Args:
            league_id: Sleeper league_id

        Returns:
            Dict mapping round number to list of (roster_id_1, roster_id_2) tuples
        """
        cache_key = league_id
        if cache_key in self._bracket_matchups_cache:
            return self._bracket_matchups_cache[cache_key]

        bracket = self._get_winners_bracket(league_id)
        pairings_by_round: dict[int, list[tuple[int, int]]] = {}

        for matchup in bracket:
            round_num = matchup.get("r", 0)
            t1, t2 = bracket_team_ids(matchup)

            if t1 is not None and t2 is not None:
                if round_num not in pairings_by_round:
                    pairings_by_round[round_num] = []
                pairings_by_round[round_num].append((t1, t2))

        self._bracket_matchups_cache[cache_key] = pairings_by_round
        return pairings_by_round

    def _get_championship_winner(self, league_id: str) -> int | None:
        """
        Get the championship winner roster_id from the winners bracket.

        The championship game has p=1 (placement 1).

        Args:
            league_id: Sleeper league_id

        Returns:
            Roster_id of the champion, or None if not found
        """
        bracket = self._get_winners_bracket(league_id)

        for matchup in bracket:
            # p=1 indicates the championship game
            if matchup.get("p") == 1:
                return bracket_winner(matchup)

        return None

    def _get_playoff_rosters_from_bracket(self, league_id: str) -> set[int]:
        """
        Get all roster_ids that appear in the winners bracket.

        Args:
            league_id: Sleeper league_id

        Returns:
            Set of roster_ids in the playoff bracket
        """
        bracket = self._get_winners_bracket(league_id)
        roster_ids = set()

        for matchup in bracket:
            t1, t2 = bracket_team_ids(matchup)
            if t1 is not None:
                roster_ids.add(t1)
            if t2 is not None:
                roster_ids.add(t2)

        return roster_ids

    def _get_championship_info(self, league_id: str) -> dict[str, Any]:
        """
        Get championship game info from winners bracket.

        The winners bracket API contains complete bracket structure including
        the championship game (p=1) and its winner.

        Args:
            league_id: Sleeper league_id

        Returns:
            Dict with:
            - championship_week: int (week of championship game)
            - champion_roster_id: int (winner roster_id)
            - finalist_roster_ids: tuple (t1, t2 roster_ids)
            - championship_round: int (round number)
        """
        bracket = self._get_winners_bracket(league_id)
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_start = playoff_structure.get("playoff_week_start", 15)
        playoff_round_type = playoff_structure.get("playoff_round_type", 0)
        playoff_rounds = playoff_structure.get("playoff_rounds", 1)

        # Primary: Look for p=1 (championship game)
        for matchup in bracket:
            if matchup.get("p") == 1:  # Championship game (placement 1)
                round_num = matchup.get("r", 0)
                champ_start, championship_week = playoff_weeks_for_round(
                    playoff_start, round_num, playoff_round_type, playoff_rounds
                )
                t1, t2 = bracket_team_ids(matchup)
                return {
                    "championship_week": championship_week,
                    "championship_weeks": tuple(range(champ_start, championship_week + 1)),
                    "champion_roster_id": bracket_winner(matchup),
                    "finalist_roster_ids": (t1, t2),
                    "championship_round": round_num,
                }

        # Fallback: If no p=1 game, use the last round's game (highest r value)
        # This handles leagues where Sleeper API didn't set p=1 properly
        # GUARD: Only use this fallback if the final round has a winner (season complete)
        if bracket:
            max_round = max((m.get("r", 0) for m in bracket), default=0)
            playoff_rounds = playoff_structure.get("playoff_rounds", 3)

            # Only use fallback if:
            # 1. We've reached the expected final round (max_round >= playoff_rounds)
            # 2. The final round game has a winner (w is set)
            if max_round > 0 and max_round >= playoff_rounds:
                # Find the game in the final round (should be championship)
                final_round_games = [m for m in bracket if m.get("r") == max_round]
                # If multiple games in final round, prefer the one with p=None (championship) over p>=3 (3rd place)
                final_game = None
                for game in final_round_games:
                    if game.get("p") is None or game.get("p") == 1:
                        final_game = game
                        break
                if not final_game and final_round_games:
                    # Still no match, just take the first one
                    final_game = final_round_games[0]

                # Only return if the game has a winner (season complete)
                if final_game and final_game.get("w") is not None:
                    champ_start, championship_week = playoff_weeks_for_round(
                        playoff_start, max_round, playoff_round_type, playoff_rounds
                    )
                    t1, t2 = bracket_team_ids(final_game)
                    logger.debug(f"[CHAMP FALLBACK] Using round {max_round} as championship (no p=1 in bracket)")
                    return {
                        "championship_week": championship_week,
                        "championship_weeks": tuple(range(champ_start, championship_week + 1)),
                        "champion_roster_id": bracket_winner(final_game),
                        "finalist_roster_ids": (t1, t2),
                        "championship_round": max_round,
                    }

        return {}

    def _determine_winner_from_bracket(
        self, league_id: str, week: int, roster_id_1: int, roster_id_2: int
    ) -> tuple[int, int, int, int, int, int]:
        """
        Determine win/loss from bracket when point data is missing (0 points).

        This handles historical Sleeper data where /matchups API returns 0 points
        but the winners_bracket API has the correct winner.

        Args:
            league_id: Sleeper league_id
            week: Week number
            roster_id_1: First team's roster_id
            roster_id_2: Second team's roster_id

        Returns:
            Tuple of (win_1, loss_1, tie_1, win_2, loss_2, tie_2)
        """
        bracket = self._get_winners_bracket(league_id)
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_start = playoff_structure.get("playoff_week_start", 15)
        playoff_round_type = playoff_structure.get("playoff_round_type", 0)
        playoff_rounds = playoff_structure.get("playoff_rounds", 1)

        for matchup in bracket:
            round_num = matchup.get("r", 0)
            matchup_week_start, matchup_week_end = playoff_weeks_for_round(
                playoff_start, round_num, playoff_round_type, playoff_rounds
            )
            t1, t2 = bracket_team_ids(matchup)
            winner = bracket_winner(matchup)

            if matchup_week_start <= week <= matchup_week_end:
                # Check if this bracket matchup matches our teams
                if {t1, t2} == {roster_id_1, roster_id_2}:
                    if winner == roster_id_1:
                        return (1, 0, 0, 0, 1, 0)
                    elif winner == roster_id_2:
                        return (0, 1, 0, 1, 0, 0)

        # Also check losers bracket for consolation games
        losers_bracket = self.client.get_losers_bracket(league_id) or []
        for matchup in losers_bracket:
            round_num = matchup.get("r", 0)
            matchup_week_start, matchup_week_end = playoff_weeks_for_round(
                playoff_start, round_num, playoff_round_type, playoff_rounds
            )
            t1, t2 = bracket_team_ids(matchup)
            winner = bracket_winner(matchup)

            if matchup_week_start <= week <= matchup_week_end:
                if {t1, t2} == {roster_id_1, roster_id_2}:
                    if winner == roster_id_1:
                        return (1, 0, 0, 0, 1, 0)
                    elif winner == roster_id_2:
                        return (0, 1, 0, 1, 0, 0)

        return (0, 0, 0, 0, 0, 0)  # Unknown - keep as-is

    def _get_bracket_round_for_roster(self, league_id: str, week: int, roster_id: int) -> dict[str, Any] | None:
        """
        Get bracket matchup info for a roster in a specific week.

        Args:
            league_id: Sleeper league_id
            week: Week number
            roster_id: Roster ID to find

        Returns:
            Dict with bracket matchup info, or None if not in bracket
        """
        bracket = self._get_winners_bracket(league_id)
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_start = playoff_structure.get("playoff_week_start", 15)
        playoff_round_type = playoff_structure.get("playoff_round_type", 0)
        playoff_rounds = playoff_structure.get("playoff_rounds", 1)

        for matchup in bracket:
            round_num = matchup.get("r", 0)
            matchup_week_start, matchup_week_end = playoff_weeks_for_round(
                playoff_start, round_num, playoff_round_type, playoff_rounds
            )
            t1, t2 = bracket_team_ids(matchup)

            if matchup_week_start <= week <= matchup_week_end and roster_id in (t1, t2):
                return {
                    "round": round_num,
                    "placement": matchup.get("p"),
                    "winner": bracket_winner(matchup),
                    "t1": t1,
                    "t2": t2,
                }

        return None

    def _determine_playoff_flags(
        self,
        week: int,
        roster_id: int,
        league_id: str,
        active_rosters_this_round: set[int] | None = None,
    ) -> tuple[bool, bool]:
        """
        Determine if a matchup is playoffs and/or consolation.

        Args:
            week: Week number
            roster_id: Roster ID of the team
            league_id: Sleeper league_id

        Returns:
            Tuple of (is_playoffs, is_consolation)
        """
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_week_start = playoff_structure.get("playoff_week_start", 15)
        playoff_week_end = playoff_structure.get("playoff_week_end", 17)
        playoff_round_type = playoff_structure.get("playoff_round_type", 0)
        playoff_rounds = playoff_structure.get("playoff_rounds", 1)

        # Before playoffs start, neither flag is true
        if week < playoff_week_start:
            return False, False

        # Calculate which playoff round this week corresponds to
        target_round = playoff_round_for_week(week, playoff_week_start, playoff_round_type, playoff_rounds)

        winners_bracket = self._get_winners_bracket(league_id)

        # CRITICAL FIX: Track which teams are STILL IN CHAMPIONSHIP CONTENTION
        # by tracing winner progression through the bracket.
        #
        # Sleeper's winners_bracket includes ALL playoff games (championship AND
        # consolation), and uses the 'w' field to indicate winners. For rounds > 1,
        # a team is only competing for 1st place if they WON in all previous rounds.
        #
        # Sleeper API placement values:
        #   p=1: Championship (1st/2nd place)
        #   p=3: 3rd place game
        #   p=5: 5th place game
        #   p=None: Advancement round (no final placement determined yet)
        #
        # Build championship contenders by tracking winners through rounds
        championship_contenders = self._get_championship_contenders(league_id, target_round)

        # Get consolation rosters for this round
        consolation_rosters = self._get_consolation_roster_ids(league_id, week, active_rosters_this_round)

        # If Sleeper does not expose bracket metadata, do not flatten guessed
        # playoff tiers into the DDL. Downstream fallback inference can run only
        # because the platform source is absent; once flattened, these flags are
        # treated as facts and must not be broad guesses.
        if not winners_bracket and playoff_week_start <= week <= playoff_week_end:
            return False, False

        # Determine flags:
        # - is_playoffs=True ONLY if roster is in championship contention for this round
        # - is_consolation=True ONLY if roster is in consolation bracket for this round
        # - Both False if roster is not in any bracket (didn't qualify for postseason)
        is_playoffs = roster_id in championship_contenders
        is_consolation = roster_id in consolation_rosters

        return is_playoffs, is_consolation

    def _get_roster_mappings(self, league_id: str) -> dict[int, dict[str, str]]:
        """
        Build roster_id -> manager info mapping.

        Returns:
            Dict mapping roster_id to {manager_name, manager_guid, team_name}
        """
        rosters = self.client.get_league_rosters(league_id)
        users = self.client.get_league_users(league_id)

        # Build user_id -> user info mapping
        user_info = {}
        for user in users:
            # Historical Sleeper leagues can return a null user entry for a
            # deleted account. The roster row is still usable; preserve it as
            # an orphaned roster instead of aborting the entire season fetch.
            if not isinstance(user, dict):
                continue
            user_id = user.get("user_id")
            if user_id:
                display_name = user.get("display_name") or user.get("username", "Unknown")
                metadata = user.get("metadata") or {}
                team_name = metadata.get("team_name", display_name)
                user_info[user_id] = {
                    "display_name": display_name,
                    "team_name": team_name,
                }

        # Build roster mappings
        roster_map = {}
        for roster in rosters:
            roster_id = roster.get("roster_id")
            owner_id = roster.get("owner_id")

            if roster_id is not None:
                info = user_info.get(owner_id, {})
                if owner_id and owner_id in user_info:
                    manager_name = info.get("display_name", f"Team {roster_id}")
                else:
                    # Orphaned roster (owner_id=None or deleted account)
                    # Use synthetic name from roster_id for stable identity
                    manager_name = f"Team {roster_id}"

                # Apply overrides
                if manager_name in self.ctx.manager_name_overrides:
                    manager_name = self.ctx.manager_name_overrides[manager_name]

                # Synthetic guid for orphaned rosters enables stable franchise_id.
                # Use short prefix so first 8 chars are unique per roster_id.
                manager_guid = owner_id or f"orp{roster_id:04d}00"
                roster_map[roster_id] = {
                    "manager_name": manager_name,
                    "manager_guid": manager_guid,
                    "team_name": info.get("team_name", manager_name),
                }

        return roster_map

    def _pair_matchups(
        self, matchups: list[dict[str, Any]], league_id: str | None = None, week: int | None = None
    ) -> list[tuple[dict, dict | None]]:
        """
        Group matchups by matchup_id to pair opponents.

        Sleeper's weekly matchup_id groups are the source of truth for played
        games. Bracket data is only used to recover fallback playoff pairings
        when the weekly rows are null or otherwise unpaired.

        Args:
            matchups: Raw matchups from API
            league_id: Optional league_id for bracket-based pairing
            week: Optional week number for bracket-based pairing

        Returns:
            List of (team1, team2) tuples
        """
        # Sleeper's weekly matchup API is authoritative for games that were
        # actually recorded. Bracket data is only a fallback for unpaired/null
        # historical rows; it must not reshuffle played matchup groups.
        by_matchup: dict[Any, list[dict]] = {}
        null_matchup_entries: list[dict] = []
        unpaired_entries: list[dict] = []

        for m in matchups:
            matchup_id = m.get("matchup_id")
            if matchup_id is None:
                null_matchup_entries.append(m)
            else:
                if matchup_id not in by_matchup:
                    by_matchup[matchup_id] = []
                by_matchup[matchup_id].append(m)

        pairs: list[tuple[dict, dict | None]] = []
        for matchup_id, teams in by_matchup.items():
            if len(teams) == 2:
                pairs.append((teams[0], teams[1]))
            elif len(teams) == 1:
                pairs.append((teams[0], None))
            else:
                unpaired_entries.extend(teams)

        fallback_entries = null_matchup_entries + unpaired_entries
        if fallback_entries and league_id and week:
            playoff_structure = self._get_playoff_structure(league_id)
            playoff_start = playoff_structure.get("playoff_week_start", 15)
            playoff_end = playoff_structure.get("playoff_week_end", 17)

            if playoff_start <= week <= playoff_end:
                pairs.extend(self._pair_from_bracket(fallback_entries, league_id, week))
            elif week > playoff_end:
                logger.debug(f"Skipping week {week}: NFL playoff week (fantasy playoffs ended week {playoff_end})")
                logger.debug(f"{len(fallback_entries)} unpaired entries (not fantasy matchups)")
            else:
                logger.debug(
                    f"Warning: {len(fallback_entries)} unpaired entries in week {week} (pre-playoffs) - skipping"
                )

        if not pairs and league_id and week:
            playoff_structure = self._get_playoff_structure(league_id)
            playoff_start = playoff_structure.get("playoff_week_start", 15)
            playoff_end = playoff_structure.get("playoff_week_end", 17)
            if playoff_start <= week <= playoff_end:
                bracket_pairs = self._pair_from_bracket_all(matchups, league_id, week)
                if bracket_pairs:
                    return bracket_pairs

        return pairs

    def _pair_from_bracket_all(
        self, matchups: list[dict[str, Any]], league_id: str, week: int
    ) -> list[tuple[dict, dict | None]]:
        """
        Pair matchups using bracket data when weekly matchup_id pairing produced
        no usable games.

        Only pairs teams that are in the winners or losers bracket.
        Teams with matchup_id=None (non-playoff teams) are skipped entirely.

        Args:
            matchups: ALL matchup entries for the week
            league_id: Sleeper league_id
            week: Week number

        Returns:
            List of (team1, team2) tuples for bracket games only
        """
        if not matchups:
            return []

        # Get winners bracket pairings for fallback recovery.
        winners_pairings = self._get_bracket_pairings_by_round(league_id)

        playoff_structure = self._get_playoff_structure(league_id)
        playoff_start = playoff_structure.get("playoff_week_start", 15)

        # Calculate which playoff round this week corresponds to
        playoff_round = playoff_round_for_week(
            week,
            playoff_start,
            playoff_structure.get("playoff_round_type", 0),
            playoff_structure.get("playoff_rounds", 1),
        )

        # Build roster_id to matchup entry mapping for all playoff participants
        roster_to_matchup: dict[int, dict] = {}
        for m in matchups:
            roster_id = m.get("roster_id")
            if roster_id is not None:
                roster_to_matchup[roster_id] = m

        pairs: list[tuple[dict, dict | None]] = []
        used_rosters: set[int] = set()

        # Process winners bracket pairings for this fallback round.
        if playoff_round in winners_pairings:
            logger.debug(f"Using bracket data for playoff week {week} (round {playoff_round})")
            for t1_id, t2_id in winners_pairings[playoff_round]:
                m1 = roster_to_matchup.get(t1_id)
                m2 = roster_to_matchup.get(t2_id)

                if m1 and m2:
                    pairs.append((m1, m2))
                    used_rosters.add(t1_id)
                    used_rosters.add(t2_id)

        # For remaining teams (consolation, etc.), use matchup_id-based pairing
        # This handles teams not in the winners bracket correctly
        remaining = [
            m for rid, m in roster_to_matchup.items() if rid not in used_rosters and m.get("matchup_id") is not None
        ]
        if remaining:
            by_mid: dict[Any, list[dict]] = {}
            for m in remaining:
                mid = m.get("matchup_id")
                if mid not in by_mid:
                    by_mid[mid] = []
                by_mid[mid].append(m)

            for mid, teams in by_mid.items():
                if len(teams) == 2:
                    pairs.append((teams[0], teams[1]))
                    used_rosters.add(teams[0].get("roster_id"))
                    used_rosters.add(teams[1].get("roster_id"))

        if pairs:
            logger.debug(f"Bracket-based pairing for playoffs: {len(pairs)} pairs")

        return pairs

    def _get_losers_bracket_pairings_by_round(self, league_id: str) -> dict[int, list[tuple[int, int]]]:
        """
        Get consolation/losers bracket pairings organized by round.

        Returns:
            Dict mapping round number to list of (roster_id_1, roster_id_2) tuples
        """
        bracket = self._get_losers_bracket(league_id)
        pairings: dict[int, list[tuple[int, int]]] = {}

        for matchup in bracket:
            round_num = matchup.get("r", 0)
            t1, t2 = bracket_team_ids(matchup)

            if t1 is not None and t2 is not None:
                if round_num not in pairings:
                    pairings[round_num] = []
                pairings[round_num].append((t1, t2))

        return pairings

    def _pair_from_bracket(
        self, matchups: list[dict[str, Any]], league_id: str, week: int
    ) -> list[tuple[dict, dict | None]]:
        """
        Pair matchups using bracket data when matchup_id is NULL.

        This handles historical Sleeper data where playoff matchups
        don't have proper matchup_id values.

        Args:
            matchups: Matchup entries with NULL matchup_id
            league_id: Sleeper league_id
            week: Week number

        Returns:
            List of (team1, team2) tuples
        """
        if not matchups:
            return []

        # Get bracket pairings by round. Sleeper's weekly matchup API is the
        # source of truth for normal paired games; bracket metadata is only a
        # fallback for null matchup_id rows that explicitly appear in a bracket.
        bracket_pairings = self._get_bracket_pairings_by_round(league_id)
        losers_bracket_pairings = self._get_losers_bracket_pairings_by_round(league_id)
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_start = playoff_structure.get("playoff_week_start", 15)

        # Calculate which playoff round this week corresponds to
        playoff_round = playoff_round_for_week(
            week,
            playoff_start,
            playoff_structure.get("playoff_round_type", 0),
            playoff_structure.get("playoff_rounds", 1),
        )

        # Build roster_id to matchup entry mapping
        roster_to_matchup: dict[int, dict] = {}
        for m in matchups:
            roster_id = m.get("roster_id")
            if roster_id is not None:
                roster_to_matchup[roster_id] = m

        pairs: list[tuple[dict, dict | None]] = []
        used_rosters: set[int] = set()

        # Try to pair using bracket data for this round
        fallback_pairings = [
            *bracket_pairings.get(playoff_round, []),
            *losers_bracket_pairings.get(playoff_round, []),
        ]
        if fallback_pairings:
            logger.debug(f"Using bracket data for playoff round {playoff_round} (week {week})")
            for t1_id, t2_id in fallback_pairings:
                m1 = roster_to_matchup.get(t1_id)
                m2 = roster_to_matchup.get(t2_id)

                if m1 and t1_id not in used_rosters:
                    if m2 and t2_id not in used_rosters:
                        pairs.append((m1, m2))
                        used_rosters.add(t1_id)
                        used_rosters.add(t2_id)
                    else:
                        # Only t1 has data - might be a bye or missing data
                        pairs.append((m1, None))
                        used_rosters.add(t1_id)
                elif m2 and t2_id not in used_rosters:
                    pairs.append((m2, None))
                    used_rosters.add(t2_id)

        skipped = len([roster_id for roster_id in roster_to_matchup if roster_id not in used_rosters])
        if skipped:
            logger.debug(
                "Skipping %s NULL matchup_id roster rows in week %s because neither weekly matchups nor bracket APIs "
                "provide an opponent",
                skipped,
                week,
            )

        if pairs:
            logger.debug(f"Bracket-based pairing: {len(pairs)} pairs from {len(matchups)} NULL matchup_id entries")

        return pairs

    def _calculate_derived_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate derived metrics (teams_beat_this_week, league_weekly_mean, etc.).

        Args:
            df: DataFrame with matchup data

        Returns:
            DataFrame with additional metrics
        """
        if df.empty:
            return df

        # Calculate league weekly mean
        weekly_means = df.groupby(["year", "week"])["team_points"].transform("mean")
        df["league_weekly_mean"] = weekly_means.round(2)

        # Calculate teams beat this week (all-play record)
        def calc_teams_beat(row):
            week_df = df[(df["year"] == row["year"]) & (df["week"] == row["week"])]
            return (week_df["team_points"] < row["team_points"]).sum()

        df["teams_beat_this_week"] = df.apply(calc_teams_beat, axis=1)

        # Above league median
        weekly_medians = df.groupby(["year", "week"])["team_points"].transform("median")
        df["above_league_median"] = (df["team_points"] > weekly_medians).astype(int)

        return df

    def fetch_matchups_for_week(
        self, league_id: str, year: int, week: int, roster_map: dict[int, dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Fetch matchup data for a specific week.

        Args:
            league_id: Sleeper league_id
            year: Season year
            week: Week number (1-indexed)
            roster_map: roster_id -> manager info mapping

        Returns:
            List of matchup rows (2 per matchup - one for each team's perspective)
        """
        matchups = self.client.get_league_matchups(league_id, week)

        if not matchups:
            return []

        # Pair matchups (pass league_id and week for bracket-based fallback)
        pairs = self._pair_matchups(matchups, league_id=league_id, week=week)
        active_rosters_this_week = {
            roster_id
            for team1, team2 in pairs
            if team2 is not None
            for roster_id in (team1.get("roster_id"), team2.get("roster_id"))
            if roster_id is not None
        }

        # Get championship info from bracket (for marking champion and finalists)
        championship_info = self._get_championship_info(league_id)
        championship_week = championship_info.get("championship_week")
        championship_weeks = set(championship_info.get("championship_weeks") or [])
        if championship_week and not championship_weeks:
            championship_weeks = {championship_week}
        champion_roster_id = championship_info.get("champion_roster_id")
        finalist_roster_ids = championship_info.get("finalist_roster_ids", ())

        # Debug logging for championship detection (only at debug level to reduce noise)
        if week >= 15:  # Only log for playoff weeks
            logger.debug(f"[CHAMP DEBUG] week={week}, league_id={league_id}")
            logger.debug(
                f"[CHAMP DEBUG] championship_week={championship_week}, finalists={finalist_roster_ids}, champion={champion_roster_id}"
            )

        # Get playoff structure for bracket-based winner detection
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_start = playoff_structure.get("playoff_week_start", 15)

        all_rows = []

        for team1, team2 in pairs:
            roster_id_1 = team1.get("roster_id")
            points_1 = self._effective_points(team1)
            matchup_id = team1.get("matchup_id")

            info_1 = roster_map.get(roster_id_1, {})
            manager_1 = info_1.get("manager_name", "Unknown")
            guid_1 = info_1.get("manager_guid", "")
            team_name_1 = info_1.get("team_name", manager_1)

            # Determine playoff/consolation flags for team 1
            is_playoffs_1, is_consolation_1 = self._determine_playoff_flags(
                week,
                roster_id_1,
                league_id,
                active_rosters_this_week,
            )

            if team2:
                # Regular matchup
                roster_id_2 = team2.get("roster_id")
                points_2 = self._effective_points(team2)
                championship_pair = {tid for tid in finalist_roster_ids if tid is not None}
                current_pair = {roster_id_1, roster_id_2}
                is_championship_game = (
                    week in championship_weeks and len(championship_pair) == 2 and current_pair == championship_pair
                )

                info_2 = roster_map.get(roster_id_2, {})
                manager_2 = info_2.get("manager_name", "Unknown")
                guid_2 = info_2.get("manager_guid", "")
                team_name_2 = info_2.get("team_name", manager_2)

                # Determine playoff/consolation flags for team 2
                is_playoffs_2, is_consolation_2 = self._determine_playoff_flags(
                    week,
                    roster_id_2,
                    league_id,
                    active_rosters_this_week,
                )

                margin_1 = round(points_1 - points_2, 2)
                margin_2 = round(points_2 - points_1, 2)

                # Determine win/loss/tie
                if points_1 > points_2:
                    win_1, loss_1, tie_1 = 1, 0, 0
                    win_2, loss_2, tie_2 = 0, 1, 0
                elif points_1 < points_2:
                    win_1, loss_1, tie_1 = 0, 1, 0
                    win_2, loss_2, tie_2 = 1, 0, 0
                else:
                    win_1, loss_1, tie_1 = 0, 0, 1
                    win_2, loss_2, tie_2 = 0, 0, 1

                # If both teams have 0 points during playoffs, use bracket data for winner
                if points_1 == 0 and points_2 == 0 and week >= playoff_start:
                    bracket_result = self._determine_winner_from_bracket(league_id, week, roster_id_1, roster_id_2)
                    if bracket_result != (0, 0, 0, 0, 0, 0):
                        win_1, loss_1, tie_1, win_2, loss_2, tie_2 = bracket_result
                        logger.debug(
                            f"Using bracket data for winner in week {week}: roster {roster_id_1 if win_1 else roster_id_2} won"
                        )

                is_champion_1 = is_championship_game and (
                    roster_id_1 == champion_roster_id or (champion_roster_id is None and win_1 == 1)
                )
                is_champion_2 = is_championship_game and (
                    roster_id_2 == champion_roster_id or (champion_roster_id is None and win_2 == 1)
                )

                # Log when championship game is detected (debug level to reduce noise)
                if is_championship_game:
                    logger.debug(
                        "[CHAMP] Setting championship=1 for roster_ids=%s/%s in week %s",
                        roster_id_1,
                        roster_id_2,
                        week,
                    )
                    if is_champion_1 or is_champion_2:
                        winner_id = roster_id_1 if is_champion_1 else roster_id_2
                        logger.debug("[CHAMP] Setting champion=1 for roster_id=%s (WINNER)", winner_id)

                close_margin = abs(margin_1) <= 10

                # Row for team 1
                all_rows.append(
                    {
                        "week": week,
                        "year": year,
                        "manager": manager_1,
                        "manager_guid": guid_1,
                        "team_name": team_name_1,
                        "team_key": str(roster_id_1),
                        "team_points": round(points_1, 2),
                        "opponent": manager_2,
                        "opponent_guid": guid_2,
                        "opponent_points": round(points_2, 2),
                        "margin": margin_1,
                        "total_matchup_score": round(points_1 + points_2, 2),
                        "close_margin": close_margin,
                        "win": win_1,
                        "loss": loss_1,
                        "tie": tie_1,
                        "matchup_id": matchup_id,
                        "is_playoffs": is_playoffs_1,
                        "is_consolation": is_consolation_1,
                        "championship": 1 if is_championship_game else 0,
                        "champion": 1 if is_champion_1 else 0,
                        # Sleeper doesn't provide these
                        "team_projected_points": None,
                        "opponent_projected_points": None,
                        "gpa": None,
                        "grade": None,
                        "matchup_recap_url": None,
                    }
                )

                # Row for team 2
                all_rows.append(
                    {
                        "week": week,
                        "year": year,
                        "manager": manager_2,
                        "manager_guid": guid_2,
                        "team_name": team_name_2,
                        "team_key": str(roster_id_2),
                        "team_points": round(points_2, 2),
                        "opponent": manager_1,
                        "opponent_guid": guid_1,
                        "opponent_points": round(points_1, 2),
                        "margin": margin_2,
                        "total_matchup_score": round(points_1 + points_2, 2),
                        "close_margin": close_margin,
                        "win": win_2,
                        "loss": loss_2,
                        "tie": tie_2,
                        "matchup_id": matchup_id,
                        "is_playoffs": is_playoffs_2,
                        "is_consolation": is_consolation_2,
                        "championship": 1 if is_championship_game else 0,
                        "champion": 1 if is_champion_2 else 0,
                        "team_projected_points": None,
                        "opponent_projected_points": None,
                        "gpa": None,
                        "grade": None,
                        "matchup_recap_url": None,
                    }
                )

            else:
                # Bye week
                is_championship_game = False
                is_champion_1 = False
                all_rows.append(
                    {
                        "week": week,
                        "year": year,
                        "manager": manager_1,
                        "manager_guid": guid_1,
                        "team_name": team_name_1,
                        "team_key": str(roster_id_1),
                        "team_points": round(points_1, 2),
                        "opponent": "BYE",
                        "opponent_guid": None,
                        "opponent_points": 0,
                        "margin": round(points_1, 2),
                        "total_matchup_score": round(points_1, 2),
                        "close_margin": False,
                        "win": 0,
                        "loss": 0,
                        "tie": 0,
                        "matchup_id": matchup_id,
                        "is_playoffs": is_playoffs_1,
                        "is_consolation": is_consolation_1,
                        "championship": 1 if is_championship_game else 0,
                        "champion": 1 if is_champion_1 else 0,
                        "team_projected_points": None,
                        "opponent_projected_points": None,
                        "gpa": None,
                        "grade": None,
                        "matchup_recap_url": None,
                    }
                )

        return all_rows

    def fetch_matchups_for_year(self, year: int, weeks: list[int] | None = None) -> pd.DataFrame:
        """
        Fetch matchup data for an entire season.

        Args:
            year: Season year
            weeks: Optional list of specific weeks to fetch

        Returns:
            DataFrame with matchup data
        """
        log(f"\n{'=' * 60}")
        log(f"Fetching Sleeper matchups for {year}")
        log(f"{'=' * 60}")

        # Get league ID for year
        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            log(f"No league ID found for year {year}")
            return pd.DataFrame()

        log(f"League ID: {league_id}")

        # Get roster mappings
        roster_map = self._get_roster_mappings(league_id)
        log(f"Found {len(roster_map)} teams")

        # Determine weeks to fetch
        explicit_week_scope = weeks is not None
        if weeks is None:
            # Use league settings to determine week bounds (avoids iterating through empty weeks)
            start_week, end_week = self._get_season_week_bounds(league_id, year)
            weeks = list(range(start_week, end_week + 1))
            log(f"Fetching weeks {start_week}-{end_week} (from league settings)")
        else:
            log(f"Fetching weeks: {weeks[0]}-{weeks[-1]}")

        # Fetch all weeks
        all_matchups = []
        consecutive_empty_weeks = 0
        valid_weeks_count = 0
        skipped_weeks = []

        for week in weeks:
            try:
                week_data = self.fetch_matchups_for_week(
                    league_id=league_id, year=year, week=week, roster_map=roster_map
                )

                # Skip phantom weeks (no data or all points are 0)
                if not week_data:
                    consecutive_empty_weeks += 1
                    skipped_weeks.append(week)
                    if not explicit_week_scope and consecutive_empty_weeks >= 3:
                        logger.debug(f"3+ consecutive empty weeks at week {week} - stopping")
                        break
                    continue

                total_points = sum(entry.get("team_points", 0) for entry in week_data)
                if total_points == 0:
                    consecutive_empty_weeks += 1
                    skipped_weeks.append(week)
                    if not explicit_week_scope and consecutive_empty_weeks >= 3:
                        logger.debug(f"3+ consecutive phantom weeks at week {week} - stopping")
                        break
                    continue

                consecutive_empty_weeks = 0  # Reset on valid week
                valid_weeks_count += 1
                all_matchups.extend(week_data)
                logger.debug(f"Week {week}: {len(week_data)} entries, {total_points:.1f} pts")

            except Exception as e:
                if explicit_week_scope:
                    raise
                logger.warning(f"Error fetching week {week}: {e}")
                skipped_weeks.append(week)
                continue

        if not all_matchups:
            log("No matchup data found")
            return pd.DataFrame()

        # Create DataFrame
        df = pd.DataFrame(all_matchups)

        # Calculate derived metrics
        df = self._calculate_derived_metrics(df)

        # Add composite keys
        df["manager_week"] = df["manager"] + "_" + df["year"].astype(str) + "_" + df["week"].astype(str)
        df["manager_year"] = df["manager"] + "_" + df["year"].astype(str)

        # Summary log (reduced from per-week logging)
        min_week = df["week"].min()
        max_week = df["week"].max()
        log(f"  {year}: {len(df)} matchup entries across weeks {min_week}-{max_week} ({valid_weeks_count} valid weeks)")
        if skipped_weeks:
            logger.debug(f"  Skipped weeks: {skipped_weeks}")

        return df


def fetch_sleeper_matchups(
    ctx: SleeperContext,
    year: int,
    weeks: list[int] | None = None,
    client: SleeperAPIClient | None = None,
    db=None,
) -> pd.DataFrame:
    """
    Main entry point for fetching Sleeper matchup data.

    Args:
        ctx: SleeperContext with league configuration
        year: Season year to fetch
        weeks: Optional list of specific weeks
        client: Optional pre-configured API client
        db: LocalLeagueDB instance (required).

    Returns:
        DataFrame with matchup data
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    fetcher = SleeperMatchupFetcher(ctx, client)

    df = fetcher.fetch_matchups_for_year(year, weeks)

    if df.empty:
        return df

    # Save output
    league_id = ctx.get_league_id_for_year(year) or ctx.league_id
    db.save_table("matchup", df, year=year, platform="sleeper", league_id=str(league_id))
    log(f"\n  [LocalDB] matchup year={year}: {len(df):,} rows")

    return df


def _save_manager_manifest(ctx: SleeperContext, df: pd.DataFrame) -> None:
    """
    Save roster_id -> manager mappings for rosters fetcher to use.

    This manifest allows the rosters fetcher to resolve manager names for
    historical years even if the manager has since left the league. The
    matchup data preserves historical manager names from when games were played.

    Args:
        ctx: SleeperContext with league configuration
        df: DataFrame with matchup data containing manager, manager_guid, team_name, team_key, year
    """
    if df.empty or "manager" not in df.columns:
        return

    manifest = {}
    for year in df["year"].unique():
        year_df = df[df["year"] == year]
        # Get unique team_key -> manager mappings (team_key is roster_id as string)
        for team_key in year_df["team_key"].dropna().unique():
            team_rows = year_df[year_df["team_key"] == team_key]
            # Only use non-Unknown manager names
            managers = team_rows["manager"][team_rows["manager"] != "Unknown"].unique()
            if len(managers) > 0:
                # Get additional info from the first valid row
                first_row = team_rows[team_rows["manager"] != "Unknown"].iloc[0]
                manifest_key = f"{int(year)}_{team_key}"
                manifest[manifest_key] = {
                    "manager": managers[0],
                    "manager_guid": str(first_row.get("manager_guid", ""))
                    if pd.notna(first_row.get("manager_guid"))
                    else "",
                    "team_name": str(first_row.get("team_name", "")) if pd.notna(first_row.get("team_name")) else "",
                }

    manifest_path = ctx.data_directory / "sleeper_roster_manager_manifest.json"
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    log(f"  Saved manager manifest: {len(manifest)} team-year mappings to {manifest_path}")


def fetch_all_sleeper_matchups(
    ctx: SleeperContext,
    client: SleeperAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    """
    Fetch matchup data for all years in context range.

    Args:
        ctx: SleeperContext with league configuration
        client: Optional pre-configured API client
        year_filter: If set, only fetch this year or these years (for quick imports)
        db: LocalLeagueDB instance (required).

    Returns:
        Combined DataFrame with all years
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    all_dfs = []

    # If year_filter is set, only fetch that year/those years (quick import mode).
    # Otherwise prefer discovered league_ids over the payload start/end window.
    years_to_fetch = resolve_years_to_fetch(ctx, year_filter)

    for year in years_to_fetch:
        df = fetch_sleeper_matchups(
            ctx=ctx,
            year=year,
            client=client,
            db=db,
        )

        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    combined_df = pd.concat(all_dfs, ignore_index=True)

    # Save manager manifest for rosters fetcher to use
    # This preserves historical manager names for years where managers may have left
    _save_manager_manifest(ctx, combined_df)

    return combined_df


# CLI Support
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sleeper Matchup Data Fetcher")
    parser.add_argument("--context", required=True, help="Path to sleeper_context.json")
    parser.add_argument("--year", type=int, help="Specific year to fetch")
    parser.add_argument("--week", type=int, help="Specific week to fetch")

    args = parser.parse_args()

    ctx = SleeperContext.load(Path(args.context))

    if args.year:
        weeks = [args.week] if args.week else None
        df = fetch_sleeper_matchups(ctx, args.year, weeks)
    else:
        df = fetch_all_sleeper_matchups(ctx)

    print(f"\nFetched {len(df)} matchup entries")
    if not df.empty:
        print(df.head())
