"""
ESPN Schedules Fetcher

Extracts schedule and week-window data from league settings and matchup data.
Pattern matches sleeper_schedules.py.

Outputs per-week schedule metadata:
    year, week, is_playoffs, is_consolation, is_championship, regular_season_end
"""

import logging

import pandas as pd

logger = logging.getLogger(__name__)


def log(msg: str):
    logger.info(msg)
    print(msg)


def _get_max_weeks(year: int) -> int:
    if year >= 2021:
        return 18
    return 17


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
