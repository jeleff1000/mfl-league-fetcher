"""
Sleeper Schedule Data Fetcher

Fetches fantasy football schedule data from Sleeper API.
Derives schedule from matchup data with playoff/consolation flags.

Output schema matches season_schedules.py for downstream compatibility.

Required columns:
- is_playoffs, is_consolation
- manager, manager_guid, team_name
- cumulative_week, manager_week, manager_year
- opponent, opponent_week, opponent_year
- week, year
- team_points, opponent_points
- win, loss

Usage:
    from sleeper_schedules import fetch_sleeper_schedule
    from sleeper_context import SleeperContext

    ctx = SleeperContext.load("path/to/sleeper_context.json")
    df = fetch_sleeper_schedule(ctx, year=2024)
"""

import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_verbose = "--verbose" in sys.argv

from .sleeper_api_client import SleeperAPIClient
from .sleeper_context import SleeperContext, YearFilter, resolve_years_to_fetch
from .playoff_utils import (
    calculate_playoff_rounds,
    championship_contenders_for_round,
    consolation_rosters_for_round,
    playoff_round_for_week,
    resolve_playoff_structure,
)

logger = logging.getLogger(__name__)


def log(msg: str):
    """Simple logging function matching Yahoo script pattern."""
    logger.info(msg)
    print(msg)


class SleeperScheduleFetcher:
    """
    Fetch schedule data from Sleeper API.

    Derives schedule from matchup data and adds playoff/consolation flags.
    Output matches Yahoo format for downstream compatibility.
    """

    def __init__(self, ctx: SleeperContext, client: SleeperAPIClient | None = None):
        """
        Initialize the schedule fetcher.

        Args:
            ctx: SleeperContext with league configuration
            client: Optional pre-configured API client
        """
        self.ctx = ctx
        self.client = client or SleeperAPIClient()

        # Cache for playoff structure per league_id
        self._playoff_cache: dict[str, dict[str, Any]] = {}
        self._consolation_rosters_cache: dict[str, set] = {}
        self._winners_bracket_cache: dict[str, list[dict[str, Any]]] = {}
        self._championship_contenders_cache: dict[str, dict[int, set[int]]] = {}

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
            user_id = user.get("user_id")
            if user_id:
                display_name = user.get("display_name") or user.get("username", "Unknown")
                team_name = user.get("metadata", {}).get("team_name", display_name)
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
                manager_name = info.get("display_name", "Unknown")

                # Apply overrides
                if manager_name in self.ctx.manager_name_overrides:
                    manager_name = self.ctx.manager_name_overrides[manager_name]

                roster_map[roster_id] = {
                    "manager_name": manager_name,
                    "manager_guid": owner_id or "",
                    "team_name": info.get("team_name", manager_name),
                }

        return roster_map

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
            return {"playoff_week_start": 15, "playoff_teams": 6}

        settings = league.get("settings", {})
        playoff_structure = resolve_playoff_structure(settings)

        result = {
            "playoff_week_start": playoff_structure["playoff_week_start"],
            "playoff_teams": playoff_structure["playoff_teams"],
            "playoff_round_type": playoff_structure["playoff_round_type"],
            "playoff_rounds": playoff_structure["playoff_rounds"],
            "playoff_week_end": playoff_structure["playoff_week_end"],
        }

        self._playoff_cache[league_id] = result
        return result

    def _get_consolation_roster_ids(self, league_id: str) -> set:
        """
        Get roster_ids participating in consolation bracket.

        Args:
            league_id: Sleeper league_id

        Returns:
            Set of roster_ids in consolation bracket
        """
        if league_id in self._consolation_rosters_cache:
            return self._consolation_rosters_cache[league_id]

        losers_bracket = self.client.get_losers_bracket(league_id)
        if not losers_bracket:
            return set()

        consolation_rosters = set()
        for matchup in losers_bracket:
            t1 = matchup.get("t1")
            t2 = matchup.get("t2")
            if t1:
                consolation_rosters.add(t1)
            if t2:
                consolation_rosters.add(t2)

        self._consolation_rosters_cache[league_id] = consolation_rosters
        return consolation_rosters

    def _get_winners_bracket(self, league_id: str) -> list[dict[str, Any]]:
        """Get cached winners bracket data for a league."""
        if league_id not in self._winners_bracket_cache:
            self._winners_bracket_cache[league_id] = self.client.get_winners_bracket(league_id) or []
        return self._winners_bracket_cache[league_id]

    def _get_playoff_bye_roster_ids(self, league_id: str) -> set:
        """
        Get roster_ids that have first-round playoff byes.

        These are teams that appear in round 2 of the winners bracket
        but NOT in round 1 (they skip the first round).

        Args:
            league_id: Sleeper league_id

        Returns:
            Set of roster_ids with first-round byes
        """
        winners_bracket = self.client.get_winners_bracket(league_id)
        if not winners_bracket:
            return set()

        round_1_teams = set()
        round_2_teams = set()

        for matchup in winners_bracket:
            round_num = matchup.get("r")
            t1 = matchup.get("t1")
            t2 = matchup.get("t2")

            if round_num == 1:
                if t1:
                    round_1_teams.add(t1)
                if t2:
                    round_1_teams.add(t2)
            elif round_num == 2:
                if t1:
                    round_2_teams.add(t1)
                if t2:
                    round_2_teams.add(t2)

        # Bye teams = teams in round 2 that weren't in round 1
        bye_teams = round_2_teams - round_1_teams
        return bye_teams

    def _determine_playoff_flags(
        self,
        week: int,
        roster_id: int,
        league_id: str,
        championship_contenders: set[int] | None = None,
        consolation_rosters: set[int] | None = None,
    ) -> dict[str, Any]:
        """
        Determine playoff status and round details for a matchup.

        Args:
            week: Week number
            roster_id: Roster ID of the team
            league_id: Sleeper league_id

        Returns:
            Dict with playoff flags and round details
        """
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_week_start = playoff_structure.get("playoff_week_start", 15)
        playoff_teams = playoff_structure.get("playoff_teams", 6)
        playoff_round_type = playoff_structure.get("playoff_round_type", 0)
        playoff_rounds = playoff_structure.get("playoff_rounds") or calculate_playoff_rounds(playoff_teams)

        # Default values for regular season
        result = {
            "is_playoffs": False,
            "is_consolation": False,
            "postseason": False,
            "playoff_round": "",
            "playoff_round_num": 0,
            "playoff_week_index": 0,
            "quarterfinal": False,
            "semifinal": False,
            "championship": False,
            "consolation_round": "",
            "consolation_semifinal": False,
            "consolation_final": False,
            "placement_game": False,
        }

        if week < playoff_week_start:
            return result

        # It's postseason
        result["postseason"] = True
        target_round = playoff_round_for_week(week, playoff_week_start, playoff_round_type, playoff_rounds)
        playoff_week_index = max(target_round - 1, 0)
        result["playoff_week_index"] = playoff_week_index

        if championship_contenders is None or consolation_rosters is None:
            winners_bracket = self._get_winners_bracket(league_id)
            losers_bracket = self.client.get_losers_bracket(league_id) or []
            contenders_cache = self._championship_contenders_cache.setdefault(league_id, {})
            championship_contenders = championship_contenders_for_round(
                winners_bracket,
                target_round,
                contenders_cache,
            )
            consolation_rosters = consolation_rosters_for_round(
                winners_bracket,
                losers_bracket,
                target_round,
                contenders_cache=contenders_cache,
            )

        is_playoffs = roster_id in championship_contenders
        is_consolation = roster_id in consolation_rosters

        result["is_playoffs"] = is_playoffs
        result["is_consolation"] = is_consolation

        # Determine round names based on playoff structure
        # Standard labels are based on distance from the final round.
        if is_playoffs:
            result["playoff_round_num"] = target_round
            offset_from_final = playoff_rounds - target_round
            if offset_from_final == 0:
                result["playoff_round"] = "Championship"
                result["championship"] = True
            elif offset_from_final == 1:
                result["playoff_round"] = "Semifinal"
                result["semifinal"] = True
            elif offset_from_final == 2:
                result["playoff_round"] = "Quarterfinal"
                result["quarterfinal"] = True
            else:
                result["playoff_round"] = f"Round {target_round}"
        else:
            # Consolation bracket
            offset_from_final = playoff_rounds - target_round
            if playoff_rounds <= 1 and target_round == 1:
                result["consolation_round"] = "Consolation Semifinal"
                result["consolation_semifinal"] = True
            elif offset_from_final == 1:
                result["consolation_round"] = "Consolation Semifinal"
                result["consolation_semifinal"] = True
            elif offset_from_final == 0:
                result["consolation_round"] = "Consolation Final"
                result["consolation_final"] = True
            elif offset_from_final > 0:
                result["consolation_round"] = f"Consolation Round {target_round}"
            else:
                result["consolation_round"] = "Placement"
                result["placement_game"] = True

        return result

    def _pair_matchups(self, matchups: list[dict[str, Any]]) -> list[tuple]:
        """
        Group matchups by matchup_id to pair opponents.

        Args:
            matchups: Raw matchups from API

        Returns:
            List of (team1, team2) tuples
        """
        by_matchup = {}
        for m in matchups:
            matchup_id = m.get("matchup_id")
            if matchup_id is None:
                continue

            if matchup_id not in by_matchup:
                by_matchup[matchup_id] = []
            by_matchup[matchup_id].append(m)

        pairs = []
        for matchup_id, teams in by_matchup.items():
            if len(teams) == 2:
                pairs.append((teams[0], teams[1]))
            elif len(teams) == 1:
                pairs.append((teams[0], None))

        return pairs

    def fetch_schedule_for_week(
        self, league_id: str, year: int, week: int, roster_map: dict[int, dict[str, str]]
    ) -> list[dict[str, Any]]:
        """
        Fetch schedule data for a specific week.

        Args:
            league_id: Sleeper league_id
            year: Season year
            week: Week number (1-indexed)
            roster_map: roster_id -> manager info mapping

        Returns:
            List of schedule rows (2 per matchup - one for each team's perspective)
        """
        matchups = self.client.get_league_matchups(league_id, week)

        if not matchups:
            return []

        pairs = self._pair_matchups(matchups)
        all_rows = []
        active_rosters_this_week = {
            roster_id
            for pair in pairs
            for team in pair
            if team is not None
            for roster_id in [team.get("roster_id")]
            if roster_id is not None and pair[1] is not None
        }

        playoff_structure = self._get_playoff_structure(league_id)
        playoff_week_start = playoff_structure.get("playoff_week_start", 15)
        playoff_round_type = playoff_structure.get("playoff_round_type", 0)
        playoff_rounds = playoff_structure.get("playoff_rounds") or calculate_playoff_rounds(
            playoff_structure.get("playoff_teams", 6)
        )
        championship_contenders: set[int] = set()
        consolation_rosters: set[int] = set()
        if week >= playoff_week_start:
            target_round = playoff_round_for_week(week, playoff_week_start, playoff_round_type, playoff_rounds)
            winners_bracket = self._get_winners_bracket(league_id)
            losers_bracket = self.client.get_losers_bracket(league_id) or []
            contenders_cache = self._championship_contenders_cache.setdefault(league_id, {})
            championship_contenders = championship_contenders_for_round(
                winners_bracket,
                target_round,
                contenders_cache,
            )
            consolation_rosters = consolation_rosters_for_round(
                winners_bracket,
                losers_bracket,
                target_round,
                active_rosters_this_round=active_rosters_this_week,
                contenders_cache=contenders_cache,
            )

        for team1, team2 in pairs:
            roster_id_1 = team1.get("roster_id")
            points_1 = self._effective_points(team1)

            info_1 = roster_map.get(roster_id_1, {})
            manager_1 = info_1.get("manager_name", "Unknown")
            guid_1 = info_1.get("manager_guid", "")
            team_name_1 = info_1.get("team_name", manager_1)

            playoff_info_1 = self._determine_playoff_flags(
                week,
                roster_id_1,
                league_id,
                championship_contenders,
                consolation_rosters,
            )

            # Calculate cumulative_week (year * 100 + week)
            cumulative_week = year * 100 + week

            if team2:
                roster_id_2 = team2.get("roster_id")
                points_2 = self._effective_points(team2)

                info_2 = roster_map.get(roster_id_2, {})
                manager_2 = info_2.get("manager_name", "Unknown")
                guid_2 = info_2.get("manager_guid", "")
                team_name_2 = info_2.get("team_name", manager_2)

                playoff_info_2 = self._determine_playoff_flags(
                    week,
                    roster_id_2,
                    league_id,
                    championship_contenders,
                    consolation_rosters,
                )

                # Determine win/loss
                if points_1 > points_2:
                    win_1, loss_1 = 1, 0
                    win_2, loss_2 = 0, 1
                elif points_1 < points_2:
                    win_1, loss_1 = 0, 1
                    win_2, loss_2 = 1, 0
                else:
                    win_1, loss_1 = 0, 0
                    win_2, loss_2 = 0, 0

                # Row for team 1
                row_1 = {
                    "year": year,
                    "week": week,
                    "cumulative_week": cumulative_week,
                    "league_id": league_id,
                    "manager": manager_1,
                    "manager_guid": guid_1,
                    "team_name": team_name_1,
                    "manager_week": f"{manager_1}_{year}_{week}",
                    "manager_year": f"{manager_1}_{year}",
                    "opponent": manager_2,
                    "opponent_week": f"{manager_2}_{year}_{week}",
                    "opponent_year": f"{manager_2}_{year}",
                    "team_points": round(points_1, 2),
                    "opponent_points": round(points_2, 2),
                    "win": win_1,
                    "loss": loss_1,
                }
                row_1.update(playoff_info_1)
                all_rows.append(row_1)

                # Row for team 2
                row_2 = {
                    "year": year,
                    "week": week,
                    "cumulative_week": cumulative_week,
                    "league_id": league_id,
                    "manager": manager_2,
                    "manager_guid": guid_2,
                    "team_name": team_name_2,
                    "manager_week": f"{manager_2}_{year}_{week}",
                    "manager_year": f"{manager_2}_{year}",
                    "opponent": manager_1,
                    "opponent_week": f"{manager_1}_{year}_{week}",
                    "opponent_year": f"{manager_1}_{year}",
                    "team_points": round(points_2, 2),
                    "opponent_points": round(points_1, 2),
                    "win": win_2,
                    "loss": loss_2,
                }
                row_2.update(playoff_info_2)
                all_rows.append(row_2)

            else:
                # Bye week
                row_bye = {
                    "year": year,
                    "week": week,
                    "cumulative_week": cumulative_week,
                    "league_id": league_id,
                    "manager": manager_1,
                    "manager_guid": guid_1,
                    "team_name": team_name_1,
                    "manager_week": f"{manager_1}_{year}_{week}",
                    "manager_year": f"{manager_1}_{year}",
                    "opponent": "BYE",
                    "opponent_week": f"BYE_{year}_{week}",
                    "opponent_year": f"BYE_{year}",
                    "team_points": round(points_1, 2),
                    "opponent_points": 0,
                    "win": 0,
                    "loss": 0,
                }
                row_bye.update(playoff_info_1)
                all_rows.append(row_bye)

        # Add playoff bye entries for teams with first-round byes
        # These teams don't appear in matchups for the first playoff week
        playoff_structure = self._get_playoff_structure(league_id)
        playoff_week_start = playoff_structure.get("playoff_week_start", 15)

        if week == playoff_week_start:
            bye_roster_ids = self._get_playoff_bye_roster_ids(league_id)
            if bye_roster_ids:
                # Get roster IDs already in this week's matchups
                matched_roster_ids = set()
                for pair in pairs:
                    if pair[0]:
                        matched_roster_ids.add(pair[0].get("roster_id"))
                    if pair[1]:
                        matched_roster_ids.add(pair[1].get("roster_id"))

                # Add bye entries for teams not in matchups
                for bye_roster_id in bye_roster_ids:
                    if bye_roster_id in matched_roster_ids:
                        continue  # Already has a matchup entry

                    info = roster_map.get(bye_roster_id, {})
                    manager_name = info.get("manager_name", "Unknown")
                    manager_guid = info.get("manager_guid", "")
                    team_name = info.get("team_name", manager_name)

                    bye_row = {
                        "year": year,
                        "week": week,
                        "cumulative_week": cumulative_week,
                        "league_id": league_id,
                        "manager": manager_name,
                        "manager_guid": manager_guid,
                        "team_name": team_name,
                        "manager_week": f"{manager_name}_{year}_{week}",
                        "manager_year": f"{manager_name}_{year}",
                        "opponent": "BYE",
                        "opponent_week": f"BYE_{year}_{week}",
                        "opponent_year": f"BYE_{year}",
                        "team_points": 0,
                        "opponent_points": 0,
                        "win": 0,
                        "loss": 0,
                        "is_playoffs": True,
                        "is_consolation": False,
                        "postseason": True,
                        "playoff_round": "First Round Bye",
                        "playoff_round_num": 0,
                        "playoff_week_index": 0,
                        "quarterfinal": False,
                        "semifinal": False,
                        "championship": False,
                        "consolation_round": "",
                        "consolation_semifinal": False,
                        "consolation_final": False,
                        "placement_game": False,
                    }
                    all_rows.append(bye_row)

        return all_rows

    def fetch_schedule_for_year(self, year: int, weeks: list[int] | None = None) -> pd.DataFrame:
        """
        Fetch schedule data for an entire season.

        Args:
            year: Season year
            weeks: Optional list of specific weeks to fetch

        Returns:
            DataFrame with schedule data
        """
        log(f"\nFetching Sleeper schedule for {year}")

        league_id = self.ctx.get_league_id_for_year(year)
        if not league_id:
            log(f"No league ID found for year {year}")
            return pd.DataFrame()

        if _verbose:
            log(f"League ID: {league_id}")

        roster_map = self._get_roster_mappings(league_id)
        log(f"Found {len(roster_map)} teams")

        explicit_week_scope = weeks is not None
        if weeks is None:
            # Fetch up to 22 weeks to cover regular season (18) + playoffs (4)
            # Sleeper will return empty/phantom data for weeks that haven't happened
            # We filter those out below
            weeks = list(range(1, 23))

        log(f"Fetching weeks: {weeks[0]}-{weeks[-1]}")

        all_schedule = []
        consecutive_empty_weeks = 0
        valid_weeks = 0

        for week in weeks:
            try:
                week_data = self.fetch_schedule_for_week(
                    league_id=league_id, year=year, week=week, roster_map=roster_map
                )

                # Skip phantom weeks (no data)
                if not week_data:
                    consecutive_empty_weeks += 1
                    if _verbose:
                        log(f"  Week {week}: No schedule data - skipping")
                    if not explicit_week_scope and consecutive_empty_weeks >= 3:
                        if _verbose:
                            log("    3+ consecutive empty weeks - stopping")
                        break
                    continue

                consecutive_empty_weeks = 0  # Reset on valid week
                valid_weeks += 1
                all_schedule.extend(week_data)
                if _verbose:
                    log(f"  Week {week}: {len(week_data)} schedule entries")

            except Exception as e:
                if explicit_week_scope:
                    raise
                log(f"  Error fetching week {week}: {e}")
                continue

        if not all_schedule:
            log("No schedule data found")
            return pd.DataFrame()

        df = pd.DataFrame(all_schedule)

        log(f"[OK] {year}: {len(df)} schedule entries across {valid_weeks} weeks")

        return df


def fetch_sleeper_schedule(
    ctx: SleeperContext,
    year: int,
    weeks: list[int] | None = None,
    client: SleeperAPIClient | None = None,
    db=None,
) -> pd.DataFrame:
    """
    Main entry point for fetching Sleeper schedule data.

    Args:
        ctx: SleeperContext with league configuration
        year: Season year to fetch
        weeks: Optional list of specific weeks
        client: Optional pre-configured API client
        db: LocalLeagueDB instance (required).

    Returns:
        DataFrame with schedule data
    """
    if db is None:
        raise ValueError("db: LocalLeagueDB is required")

    fetcher = SleeperScheduleFetcher(ctx, client)

    df = fetcher.fetch_schedule_for_year(year, weeks)

    if df.empty:
        return df

    league_id = ctx.get_league_id_for_year(year) or ctx.league_id
    db.save_table("schedule", df, year=year, platform="sleeper", league_id=str(league_id))
    if _verbose:
        log(f"  [LocalDB] schedule year={year}: {len(df):,} rows")

    return df


def fetch_all_sleeper_schedules(
    ctx: SleeperContext,
    client: SleeperAPIClient | None = None,
    year_filter: YearFilter = None,
    db=None,
) -> pd.DataFrame:
    """
    Fetch schedule data for all years in context range.

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
        df = fetch_sleeper_schedule(
            ctx=ctx,
            year=year,
            client=client,
            db=db,
        )

        if not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)


# CLI Support
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Sleeper Schedule Data Fetcher")
    parser.add_argument("--context", required=True, help="Path to sleeper_context.json")
    parser.add_argument("--year", type=int, help="Specific year to fetch")
    parser.add_argument("--week", type=int, help="Specific week to fetch")

    args = parser.parse_args()

    ctx = SleeperContext.load(Path(args.context))

    if args.year:
        weeks = [args.week] if args.week else None
        df = fetch_sleeper_schedule(ctx, args.year, weeks)
    else:
        df = fetch_all_sleeper_schedules(ctx)

    print(f"\nFetched {len(df)} schedule entries")
    if not df.empty:
        print(df.head())
