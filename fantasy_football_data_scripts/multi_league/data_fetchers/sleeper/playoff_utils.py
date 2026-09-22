"""Shared Sleeper playoff structure helpers.

Sleeper's raw ``playoff_round_type`` uses:
    0 = single-week rounds
    1 = two-week championship only
    2 = two-week all rounds

Our canonical codebase uses:
    0 = single-week rounds
    1 = two-week all rounds
    2 = two-week championship only

Historically different Sleeper fetchers inferred ``playoff_week_start`` in
slightly different ways, and some paths forgot to account for multiweek
championships when deriving the start week from ``last_scored_leg``.
This module keeps that inference in one place.
"""

from __future__ import annotations

import math
from typing import Any


def canonicalize_playoff_round_type(raw_round_type: Any) -> int:
    """Convert Sleeper's raw playoff_round_type to the repo's canonical enum."""
    try:
        value = int(raw_round_type)
    except (TypeError, ValueError):
        value = 0

    if value == 1:
        return 2
    if value == 2:
        return 1
    return 0


def calculate_playoff_rounds(playoff_teams: Any) -> int:
    """Return the number of bracket rounds implied by the playoff field size."""
    try:
        teams = int(playoff_teams)
    except (TypeError, ValueError):
        teams = 0

    if teams <= 1:
        return 0
    return int(math.ceil(math.log2(teams)))


def calculate_weeks_in_playoffs(playoff_rounds: int, canonical_round_type: int) -> int:
    """Return total playoff weeks after accounting for multiweek formats."""
    if playoff_rounds <= 0:
        return 0
    if canonical_round_type == 1:
        return 2 * playoff_rounds
    if canonical_round_type == 2:
        return playoff_rounds + 1
    return playoff_rounds


def playoff_round_for_week(
    week: Any,
    playoff_week_start: Any,
    canonical_round_type: Any,
    playoff_rounds: Any,
) -> int:
    """Map an NFL week to Sleeper's bracket round number.

    Sleeper bracket APIs number matchups by bracket round, not by NFL week.
    Multi-week formats therefore reuse the same bracket round across multiple
    weeks (for example, all-2-week 4-team playoffs map weeks 14-15 to round 1
    and weeks 16-17 to round 2).
    """
    try:
        week_int = int(week)
        start_int = int(playoff_week_start)
    except (TypeError, ValueError):
        return 0

    try:
        round_type = int(canonical_round_type)
    except (TypeError, ValueError):
        round_type = 0

    try:
        total_rounds = max(int(playoff_rounds), 1)
    except (TypeError, ValueError):
        total_rounds = 1

    offset = week_int - start_int
    if offset < 0:
        return 0

    if round_type == 1:
        return (offset // 2) + 1

    if round_type == 2:
        first_final_offset = total_rounds - 1
        if offset < first_final_offset:
            return offset + 1
        if offset < first_final_offset + 2:
            return total_rounds
        return total_rounds + (offset - first_final_offset - 1)

    return offset + 1


def playoff_weeks_for_round(
    playoff_week_start: Any,
    target_round: Any,
    canonical_round_type: Any,
    playoff_rounds: Any,
) -> tuple[int, int]:
    """Return the inclusive NFL week range for a Sleeper bracket round."""
    try:
        start_int = int(playoff_week_start)
        round_int = int(target_round)
    except (TypeError, ValueError):
        return (0, 0)

    try:
        round_type = int(canonical_round_type)
    except (TypeError, ValueError):
        round_type = 0

    try:
        total_rounds = max(int(playoff_rounds), 1)
    except (TypeError, ValueError):
        total_rounds = 1

    if round_int < 1:
        return (0, 0)

    if round_type == 1:
        week_start = start_int + ((round_int - 1) * 2)
        return (week_start, week_start + 1)

    if round_type == 2:
        if round_int < total_rounds:
            week_start = start_int + round_int - 1
            return (week_start, week_start)
        if round_int == total_rounds:
            week_start = start_int + total_rounds - 1
            return (week_start, week_start + 1)
        week_start = start_int + total_rounds + (round_int - total_rounds)
        return (week_start, week_start)

    week_start = start_int + round_int - 1
    return (week_start, week_start)


def resolve_playoff_structure(
    settings: dict[str, Any],
    default_start_week: int = 15,
    *,
    season_complete: bool = True,
) -> dict[str, Any]:
    """Resolve a consistent Sleeper playoff structure from raw league settings."""
    playoff_start_raw = settings.get("playoff_week_start", 0)
    playoff_teams = settings.get("playoff_teams", 6)
    last_scored_leg = settings.get("last_scored_leg")
    raw_round_type = settings.get("playoff_round_type", 0)

    try:
        playoff_teams_int = int(playoff_teams or 0)
    except (TypeError, ValueError):
        playoff_teams_int = 0

    canonical_round_type = canonicalize_playoff_round_type(raw_round_type)
    playoff_rounds = calculate_playoff_rounds(playoff_teams)
    weeks_in_playoffs = calculate_weeks_in_playoffs(playoff_rounds, canonical_round_type)

    try:
        if playoff_teams_int <= 1:
            if season_complete and last_scored_leg and int(last_scored_leg) > 0:
                playoff_week_end = int(last_scored_leg)
                playoff_week_start = playoff_week_end + 1
                source = "disabled"
            elif playoff_start_raw and int(playoff_start_raw) > 0:
                playoff_week_start = int(playoff_start_raw)
                playoff_week_end = playoff_week_start - 1
                source = "disabled_api"
            else:
                playoff_week_start = int(default_start_week)
                playoff_week_end = playoff_week_start - 1
                source = "disabled_default"
        elif playoff_start_raw and int(playoff_start_raw) > 0:
            playoff_week_start = int(playoff_start_raw)
            playoff_week_end = playoff_week_start + weeks_in_playoffs - 1
            source = "api"
        elif season_complete and last_scored_leg and int(last_scored_leg) > 0:
            playoff_week_start = int(last_scored_leg) - weeks_in_playoffs + 1
            playoff_week_end = int(last_scored_leg)
            source = "inferred"
        else:
            playoff_week_start = int(default_start_week)
            playoff_week_end = playoff_week_start + weeks_in_playoffs - 1
            source = "default"
    except (TypeError, ValueError):
        playoff_week_start = int(default_start_week)
        playoff_week_end = playoff_week_start + weeks_in_playoffs - 1
        source = "default"

    return {
        "playoff_week_start": playoff_week_start,
        "playoff_week_end": playoff_week_end,
        "playoff_teams": playoff_teams_int,
        "playoff_round_type": canonical_round_type,
        "raw_playoff_round_type": raw_round_type,
        "playoff_rounds": playoff_rounds,
        "weeks_in_playoffs": weeks_in_playoffs,
        "last_scored_leg": last_scored_leg,
        "playoff_start_source": source,
    }


def normalize_bracket_placement(raw_placement: Any) -> int | None:
    """Return an integer bracket placement value when Sleeper provides one."""
    try:
        return int(raw_placement) if raw_placement is not None else None
    except (TypeError, ValueError):
        return None


def _normalize_roster_id(raw_roster_id: Any) -> int | None:
    """Normalize Sleeper roster ids to integers for stable set membership."""
    try:
        return int(raw_roster_id) if raw_roster_id is not None else None
    except (TypeError, ValueError):
        return None


def bracket_team_ids(matchup: dict[str, Any]) -> tuple[int | None, int | None]:
    """Return the effective team ids for a Sleeper bracket matchup.

    Some completed Sleeper brackets expose both ``t1_original`` and
    ``t2_original`` when the visible ``t1`` / ``t2`` fields no longer match
    the teams that originally advanced into a placement game. For pairing and
    classification, that complete original pair is the only form that lines up
    with weekly NULL-matchup rows. When only one side has an original, keep the
    visible ``t1`` / ``t2`` pair; single-side originals are seed/source hints,
    not a replacement matchup.
    """
    if matchup.get("t1_original") is not None and matchup.get("t2_original") is not None:
        t1 = matchup.get("t1_original")
        t2 = matchup.get("t2_original")
    else:
        t1 = matchup.get("t1")
        t2 = matchup.get("t2")
    return _normalize_roster_id(t1), _normalize_roster_id(t2)


def bracket_winner(matchup: dict[str, Any]) -> int | None:
    """Return the bracket winner only when it belongs to the effective teams."""
    winner = _normalize_roster_id(matchup.get("w"))
    if winner is None:
        return None
    return winner if winner in set(bracket_team_ids(matchup)) else None


def championship_contenders_for_round(
    winners_bracket: list[dict[str, Any]],
    target_round: int,
    cache: dict[int, set[int]] | None = None,
) -> set[int]:
    """Return roster ids still alive for the title in a given bracket round.

    Returns the empty set when the requested round is beyond the bracket's
    actual depth AND the championship has been decided. This is the correct
    answer for post-championship phantom weeks (e.g. dingleberry_derby 2018
    has a 2-round bracket with last_scored_leg=16; W16 is target_round=3,
    one past the championship). Without this gate the round-(N-1) "winners
    advancing" walk would surface the eventual champion as a contender for
    target_round = bracket_depth + 1, which then flows into the Sleeper
    fetcher's _determine_playoff_flags and writes is_playoffs=True onto a
    bye row that represents a week the team never played.

    The gate is conditional on `championship_decided` so in-season behavior
    is preserved: while a p=1 matchup still has w=None, the existing
    fallback logic continues to identify both finalists as contenders.
    """
    if target_round < 1 or not winners_bracket:
        return set()

    if cache is not None and target_round in cache:
        return set(cache[target_round])

    bracket_rounds = [m.get("r") for m in winners_bracket if m.get("r") is not None]
    bracket_depth = max(bracket_rounds) if bracket_rounds else 0
    if target_round > bracket_depth:
        championship_decided = any(
            normalize_bracket_placement(m.get("p")) == 1 and m.get("w") is not None for m in winners_bracket
        )
        if championship_decided:
            if cache is not None:
                cache[target_round] = set()
            return set()

    contenders: set[int] = set()

    if target_round == 1:
        for matchup in winners_bracket:
            if matchup.get("r") != 1:
                continue
            placement = normalize_bracket_placement(matchup.get("p"))
            if placement not in (None, 1):
                continue
            for team_id in bracket_team_ids(matchup):
                if team_id is not None:
                    contenders.add(team_id)
    else:
        prev_round_contenders = championship_contenders_for_round(winners_bracket, target_round - 1, cache)

        for matchup in winners_bracket:
            if matchup.get("r") != target_round - 1:
                continue
            placement = normalize_bracket_placement(matchup.get("p"))
            if placement not in (None, 1):
                continue
            winner = bracket_winner(matchup)
            team_ids = set(bracket_team_ids(matchup))
            if winner is not None and any(
                team_id in prev_round_contenders for team_id in team_ids if team_id is not None
            ):
                contenders.add(winner)

        all_prev_round_rosters: set[int] = set()
        for prev_round in range(1, target_round):
            for matchup in winners_bracket:
                if matchup.get("r") != prev_round:
                    continue
                for team_id in bracket_team_ids(matchup):
                    if team_id is not None:
                        all_prev_round_rosters.add(team_id)

        for matchup in winners_bracket:
            if matchup.get("r") != target_round:
                continue
            placement = normalize_bracket_placement(matchup.get("p"))
            if placement not in (None, 1):
                continue
            for team_id in bracket_team_ids(matchup):
                if team_id is not None and team_id not in all_prev_round_rosters:
                    contenders.add(team_id)

        if not contenders and prev_round_contenders:
            for matchup in winners_bracket:
                if matchup.get("r") != target_round:
                    continue
                placement = normalize_bracket_placement(matchup.get("p"))
                if placement not in (None, 1):
                    continue
                for team_id in bracket_team_ids(matchup):
                    if team_id is not None and team_id in prev_round_contenders:
                        contenders.add(team_id)

    if cache is not None:
        cache[target_round] = set(contenders)

    return contenders


def consolation_rosters_for_round(
    winners_bracket: list[dict[str, Any]],
    losers_bracket: list[dict[str, Any]],
    target_round: int,
    *,
    active_rosters_this_round: set[int] | None = None,
    contenders_cache: dict[int, set[int]] | None = None,
) -> set[int]:
    """Return roster ids that should be treated as consolation for a given round."""
    championship_contenders = championship_contenders_for_round(winners_bracket, target_round, contenders_cache)

    consolation_rosters: set[int] = set()
    placement_game_rosters: set[int] = set()
    all_bracket_rosters_this_round: set[int] = set()

    for matchup in winners_bracket:
        if matchup.get("r") != target_round:
            continue
        placement = normalize_bracket_placement(matchup.get("p"))
        team_ids = list(bracket_team_ids(matchup))
        for team_id in team_ids:
            if team_id is not None:
                all_bracket_rosters_this_round.add(team_id)
        if placement is not None and placement >= 3:
            for team_id in team_ids:
                if team_id is not None:
                    placement_game_rosters.add(team_id)

    for matchup in losers_bracket:
        if matchup.get("r") != target_round:
            continue
        for team_id in (_normalize_roster_id(matchup.get("t1")), _normalize_roster_id(matchup.get("t2"))):
            if team_id is not None:
                consolation_rosters.add(team_id)

    consolation_rosters |= placement_game_rosters
    consolation_rosters |= all_bracket_rosters_this_round - championship_contenders

    if active_rosters_this_round:
        consolation_rosters |= {team_id for team_id in active_rosters_this_round if team_id is not None}
        consolation_rosters -= championship_contenders

    return consolation_rosters
