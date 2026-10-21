"""
ESPN Schedules Fetcher

Extracts schedule and week-window data from league settings and matchup data.
Pattern matches sleeper_schedules.py.

Outputs per-week schedule metadata:
    year, week, is_playoffs, is_consolation, is_championship, regular_season_end
"""

import logging
from typing import Any

import pandas as pd
from multi_league.core.espn_playoff_settings import espn_matchup_periods_for_weeks

logger = logging.getLogger(__name__)


def log(msg: str):
    logger.info(msg)
    print(msg)


def _get_max_weeks(year: int) -> int:
    if year >= 2021:
        return 18
    return 17


def full_espn_schedule_frame(
    *,
    ctx: Any,
    raw_schedule: list[dict[str, Any]],
    year: int,
    regular_season_weeks: int,
    expected_team_ids: tuple[str, ...],
    current_matchup_period: int | None = None,
    matchup_periods: Any = None,
) -> pd.DataFrame:
    """Expand ESPN's one-call season schedule into canonical team-week rows."""
    from multi_league.core.league_update_validation import IncompleteSourceError

    expected = {str(team_id) for team_id in expected_team_ids}
    rows: list[dict[str, Any]] = []
    teams_by_week: dict[int, list[str]] = {}
    period_by_week = espn_matchup_periods_for_weeks(
        matchup_periods, list(range(1, int(regular_season_weeks) + 1))
    )
    expanded: list[tuple[int, int, dict[str, Any]]] = []
    for matchup in raw_schedule or []:
        if not isinstance(matchup, dict):
            continue
        try:
            native_period = int(matchup.get("matchupPeriodId"))
        except (TypeError, ValueError):
            continue
        expanded.extend(
            (week, native_period, matchup)
            for week, period in period_by_week.items() if period == native_period
        )
    for week, native_period, matchup in expanded:
        home = matchup.get("home")
        away = matchup.get("away")
        unassigned = (
            isinstance(home, dict) and away is None
            and current_matchup_period is not None
            and int(current_matchup_period) > 0 and native_period >= int(current_matchup_period)
            and str(matchup.get("winner") or "").strip().upper() == "UNDECIDED"
        )
        if not isinstance(home, dict) or (not isinstance(away, dict) and not unassigned):
            raise IncompleteSourceError(f"ESPN schedule week {week} has a missing matchup side")
        try:
            home_id = int(home.get("teamId"))
            away_id = None if unassigned else int(away.get("teamId"))
        except (TypeError, ValueError) as exc:
            raise IncompleteSourceError(f"ESPN schedule week {week} has an invalid team identity") from exc
        tier = str(matchup.get("playoffTierType") or "NONE").strip().upper()
        is_playoffs = int(tier == "WINNERS_BRACKET")
        is_consolation = int(tier != "NONE" and not is_playoffs)
        winner = str(matchup.get("winner") or "").strip().upper()
        sides = [("home", home_id, away_id)]
        if away_id is not None:
            sides.append(("away", away_id, home_id))
        for side_name, team_id, opponent_id in sides:
            team_name = ctx.get_team_name(team_id, year)
            opponent_team_name = None if opponent_id is None else ctx.get_team_name(opponent_id, year)
            franchise_id = ctx.get_franchise_id(team_id, year)
            opponent_franchise_id = None if opponent_id is None else ctx.get_franchise_id(opponent_id, year)
            result = None
            if winner in {"HOME", "AWAY"}:
                result = winner.lower() == side_name
            elif winner == "TIE":
                result = None
            rows.append({
                "year": int(year),
                "week": week,
                "manager": ctx.get_manager_name(team_id, team_name, year),
                "manager_guid": ctx.get_manager_guid(team_id, year),
                "franchise_id": franchise_id,
                "team_name": team_name,
                "manager_week": f"{franchise_id}_{int(year)}_{week}",
                "opponent": "TBD" if opponent_id is None else ctx.get_manager_name(opponent_id, opponent_team_name, year),
                "opponent_guid": None if opponent_id is None else ctx.get_manager_guid(opponent_id, year),
                "opponent_franchise_id": opponent_franchise_id,
                "win": int(result is True) if result is not None else None,
                "loss": int(result is False) if result is not None else None,
                "is_playoffs": is_playoffs,
                "is_consolation": is_consolation,
                "platform": "espn",
                "league_id": str(ctx.get_league_id_for_year(year)),
            })
            teams_by_week.setdefault(week, []).append(str(team_id))

    missing_weeks = [
        week for week in range(1, int(regular_season_weeks) + 1)
        if week not in teams_by_week
    ]
    if missing_weeks:
        raise IncompleteSourceError(
            f"ESPN full schedule is missing regular-season weeks {missing_weeks}"
        )
    for week, team_ids in sorted(teams_by_week.items()):
        observed = set(team_ids)
        if observed != expected or len(team_ids) != len(expected):
            raise IncompleteSourceError(
                f"ESPN full schedule week {week} team coverage mismatch: "
                f"missing={sorted(expected - observed)}, extra={sorted(observed - expected)}, "
                f"rows={len(team_ids)}, expected={len(expected)}"
            )
    return pd.DataFrame(rows)


def fetch_espn_schedule(ctx: "ESPNContext", year: int) -> pd.DataFrame | None:
    """
    Build schedule data for a single year.

    Uses matchup data if available (most accurate), otherwise derives from
    league settings.

    Args:
        ctx: ESPNContext
        year: NFL season year

    Returns:
        DataFrame with schedule metadata per week
    """
    from .espn_league_settings import load_espn_settings

    max_weeks = _get_max_weeks(year)

    # Try to get playoff start from settings
    settings = load_espn_settings(ctx, year)
    playoff_start = None
    if settings:
        playoff_start = settings.get("playoff_start_week")

    if not playoff_start:
        playoff_start = ctx.playoff_start_week

    if not playoff_start:
        # Default based on era
        playoff_start = 15 if year >= 2021 else 14

    regular_season_end = playoff_start - 1

    from multi_league.core.date_utils import get_current_nfl_season_year

    if int(year) == get_current_nfl_season_year():
        from .espn_api_client import ESPNAPIClient

        # ESPN returns the complete native matchup graph in one small call.
        # Share the refresh parser rather than projecting future results from
        # the completed-only matchup table or fetching future player lineups.
        client = ESPNAPIClient(ctx.get_league_id_for_year(year), ctx.espn_s2, ctx.swid)
        raw = client.get_raw_league(year, ["mSettings", "mTeam", "mScoreboard", "mMatchupScore"])
        schedule_settings = (raw.get("settings") or {}).get("scheduleSettings") or {}
        regular_season_end = int(schedule_settings.get("matchupPeriodCount") or regular_season_end)
        return full_espn_schedule_frame(
            ctx=ctx, raw_schedule=raw.get("schedule") or [], year=year,
            regular_season_weeks=regular_season_end,
            expected_team_ids=tuple(str(team["id"]) for team in raw.get("teams") or []),
            current_matchup_period=(raw.get("status") or {}).get("currentMatchupPeriod"),
            matchup_periods=schedule_settings.get("matchupPeriods"),
        )

    # Schedule is now derived from matchup data by the build_schedule_from_matchup
    # SQL enrichment. This fetcher builds a minimal per-week skeleton from settings.
    matchup_playoff_weeks = set()
    matchup_consolation_weeks = set()
    matchup_championship_weeks = set()
    matchup_manager_data = None

    rows = []
    for week in range(1, max_weeks + 1):
        is_playoff = week in matchup_playoff_weeks if matchup_playoff_weeks else week >= playoff_start
        is_consolation = week in matchup_consolation_weeks
        is_championship = week in matchup_championship_weeks

        # Championship is typically the last playoff week
        if not matchup_championship_weeks and is_playoff and week == max_weeks:
            is_championship = True

        rows.append(
            {
                "year": year,
                "week": week,
                "is_playoffs": is_playoff,
                "is_consolation": is_consolation,
                "is_championship": is_championship,
                "regular_season_end": regular_season_end,
                "playoff_start_week": playoff_start,
                "platform": "espn",
                "league_id": str(ctx.get_league_id_for_year(year)),
            }
        )

    if not rows:
        return None

    df = pd.DataFrame(rows)

    # Expand to per-manager rows using matchup data (canonical schedule has one row per manager per week)
    if matchup_manager_data is not None and not matchup_manager_data.empty:
        # Remove BYE-opponent entries before merge - we'll re-generate them properly below
        real_matchup_data = matchup_manager_data[matchup_manager_data["opponent"].fillna("").str.strip().ne("BYE")]
        df = df.merge(
            real_matchup_data,
            on=["year", "week"],
            how="left",
        )

        # Generate bye-week entries for teams with first-round playoff byes
        # In 6-team playoffs (common), top 2 seeds get byes in the first playoff week
        num_playoff_teams = None
        if settings:
            num_playoff_teams = settings.get("num_playoff_teams") or settings.get("playoff_teams")
        if not num_playoff_teams:
            num_playoff_teams = getattr(ctx, "num_playoff_teams", None)

        if num_playoff_teams and playoff_start:
            import math

            # Number of byes = num_playoff_teams - 2^(floor(log2(num_playoff_teams)))
            # e.g., 6 teams: 6 - 4 = 2 byes; 8 teams: 8 - 8 = 0 byes; 4 teams: 4 - 4 = 0 byes
            bracket_size = 2 ** math.floor(math.log2(max(num_playoff_teams, 2)))
            num_byes = num_playoff_teams - bracket_size

            if num_byes > 0:
                first_playoff_week = playoff_start
                # Find all managers who played in regular season
                all_managers = set()
                if "manager" in matchup_manager_data.columns:
                    all_managers = set(matchup_manager_data["manager"].dropna().unique())

                # Find managers who already have a REAL matchup in the first playoff week
                # (exclude BYE entries - those ARE the bye teams)
                managers_with_matchup = set()
                first_week_data = matchup_manager_data[matchup_manager_data["week"] == first_playoff_week]
                if not first_week_data.empty and "manager" in first_week_data.columns:
                    real_matchups = first_week_data[first_week_data["opponent"].fillna("").str.strip().ne("BYE")]
                    managers_with_matchup = set(real_matchups["manager"].dropna().unique())

                # Managers without a matchup in first playoff week = bye teams
                bye_managers = all_managers - managers_with_matchup
                # Only generate byes for the expected number of bye teams
                bye_managers = sorted(bye_managers)[:num_byes]

                if bye_managers:
                    bye_rows = []
                    for mgr in bye_managers:
                        bye_rows.append(
                            {
                                "year": year,
                                "week": first_playoff_week,
                                "is_playoffs": True,
                                "is_consolation": False,
                                "is_championship": False,
                                "regular_season_end": regular_season_end,
                                "playoff_start_week": playoff_start,
                                "platform": "espn",
                                "league_id": str(ctx.get_league_id_for_year(year)),
                                "manager": mgr,
                                "opponent": "BYE",
                                "is_bye_week": 1,
                            }
                        )
                    bye_df = pd.DataFrame(bye_rows)
                    df = pd.concat([df, bye_df], ignore_index=True)
                    log(
                        f"  [SCHEDULE] {year}: Added {len(bye_managers)} bye-week entries for first playoff week ({first_playoff_week})"
                    )

        log(
            f"  [SCHEDULE] {year}: {len(df)} manager-week rows (regular: 1-{regular_season_end}, playoffs: {playoff_start}+)"
        )
    else:
        log(f"  [SCHEDULE] {year}: {len(df)} weeks (regular: 1-{regular_season_end}, playoffs: {playoff_start}+)")

    return df


def fetch_all_espn_schedules(ctx: "ESPNContext") -> pd.DataFrame:
    """
    Build schedule data for all years.

    Args:
        ctx: ESPNContext with year range

    Returns:
        Combined DataFrame for all years
    """
    log(f"\n{'='*60}")
    log("Building ESPN schedule data")
    log(f"{'='*60}")

    all_dfs = []

    for year in ctx.get_year_range():
        df = fetch_espn_schedule(ctx, year)
        if df is not None and not df.empty:
            all_dfs.append(df)

    if not all_dfs:
        log("[SCHEDULE] No schedule data built for any year")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)
    log(f"[SCHEDULE] Total: {len(combined)} week entries across {len(all_dfs)} years")

    return combined
