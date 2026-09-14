"""ESPN playoff schedule normalization helpers.

ESPN exposes both old all-round playoff lengths and newer per-round matchup
period maps. Keep this logic in one place so fetch-time JSON and flat
league_settings rows stay consistent.
"""

from __future__ import annotations

import math
from typing import Any


def _to_int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def normalize_espn_matchup_periods(matchup_periods: Any) -> dict[int, list[int]]:
    """Return ESPN matchupPeriods as {matchup_period: [scoring_weeks]}."""
    if not isinstance(matchup_periods, dict):
        return {}

    normalized: dict[int, list[int]] = {}
    for raw_period, raw_weeks in matchup_periods.items():
        period = _to_int_or_none(raw_period)
        if period is None:
            continue

        if isinstance(raw_weeks, list | tuple | set):
            week_values = raw_weeks
        else:
            week_values = [raw_weeks]

        weeks = sorted(
            {week for week in (_to_int_or_none(raw_week) for raw_week in week_values) if week is not None and week > 0}
        )
        if weeks:
            normalized[period] = weeks

    return normalized


def _playoff_round_type(round_lengths: list[int]) -> int:
    """Canonical round type: 0 single-week, 1 all multi-week, 2 championship-only."""
    if not round_lengths or max(round_lengths) <= 1:
        return 0
    if all(length > 1 for length in round_lengths):
        return 1
    if round_lengths[-1] > 1 and all(length <= 1 for length in round_lengths[:-1]):
        return 2
    return 1


def derive_espn_playoff_metadata(
    playoff_teams: Any,
    regular_season_length: Any,
    playoff_matchup_period_length: Any = None,
    matchup_periods: Any = None,
) -> dict[str, Any]:
    """Derive canonical ESPN playoff metadata from actual schedule settings.

    ``playoffTeamCount=0`` is a real ESPN value for no-playoff seasons and must
    not fall through to the usual 6-team default. Newer ESPN seasons can also
    set ``playoffMatchupPeriodLength=0`` and provide per-round matchup periods,
    such as single-week early rounds plus a two-week championship.
    """
    playoff_teams_int = _to_int_or_none(playoff_teams)
    regular_season_int = _to_int_or_none(regular_season_length)
    raw_pmpl = _to_int_or_none(playoff_matchup_period_length)
    effective_pmpl = raw_pmpl if raw_pmpl and raw_pmpl > 0 else 1
    playoff_start = regular_season_int + 1 if regular_season_int is not None else None

    normalized_periods = normalize_espn_matchup_periods(matchup_periods)
    postseason_periods: list[list[int]] = []
    if regular_season_int is not None:
        postseason_periods = [
            weeks for period, weeks in sorted(normalized_periods.items()) if period > regular_season_int
        ]

    playoffs_disabled = playoff_teams_int is not None and playoff_teams_int <= 1
    if playoffs_disabled:
        round_lengths: list[int] = []
        num_rounds = 0
        bye_teams = 0
        end_week = regular_season_int
        championship_week = None
    else:
        effective_playoff_teams = playoff_teams_int if playoff_teams_int is not None else 6
        num_rounds = math.ceil(math.log2(max(effective_playoff_teams, 2)))
        next_power_of_two = 2 ** math.ceil(math.log2(max(effective_playoff_teams, 2)))
        bye_teams = max(next_power_of_two - effective_playoff_teams, 0) if playoff_teams_int is not None else 0

        round_lengths = [len(weeks) for weeks in postseason_periods]
        if not round_lengths:
            round_lengths = [effective_pmpl] * num_rounds

        if postseason_periods:
            end_week = max(max(weeks) for weeks in postseason_periods)
        elif playoff_start is not None:
            end_week = playoff_start + sum(round_lengths[:num_rounds]) - 1
        else:
            end_week = None
        championship_week = end_week

    round_type = _playoff_round_type(round_lengths)

    return {
        "playoff_teams": playoff_teams_int,
        "num_playoff_teams": playoff_teams_int,
        "playoff_start_week": playoff_start,
        "regular_season_length": regular_season_int,
        "regular_season_weeks": regular_season_int,
        "playoff_matchup_period_length": raw_pmpl if raw_pmpl is not None else effective_pmpl,
        "playoff_round_lengths": round_lengths,
        "playoff_round_type": round_type,
        "bye_teams": bye_teams,
        "num_rounds": num_rounds,
        "end_week": end_week,
        "championship_week": championship_week,
        "has_multiweek_championship": 1 if round_lengths and round_lengths[-1] > 1 else 0,
    }
