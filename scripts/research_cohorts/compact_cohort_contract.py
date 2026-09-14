"""Immutable visible cohort contract for compact matchup serving tables."""
from __future__ import annotations

from itertools import product


TEAM_TIERS = ("08tm", "10tm", "12tm", "14tm")
ROSTER_VALUES = ("flx", "sflx", "idp")
SCORING_VALUES = ("std", "half", "ppr")
PASS_TD_VALUES = ("4pt", "6pt")
DYNASTY_VALUES = ("redraft", "dynasty")
BEST_BALL_VALUES = ("managed", "best_ball")
BRACKET_VALUES = ("4po", "6po", "8po")

CORE_REQUESTS = tuple(product(
    TEAM_TIERS,
    ROSTER_VALUES,
    SCORING_VALUES,
    PASS_TD_VALUES,
    DYNASTY_VALUES,
    BEST_BALL_VALUES,
))
GRADE_REQUESTS = tuple(product(
    TEAM_TIERS,
    ROSTER_VALUES,
    SCORING_VALUES,
    PASS_TD_VALUES,
    DYNASTY_VALUES,
    BEST_BALL_VALUES,
    BRACKET_VALUES,
))


def core_request_ordinal(
    teams: str,
    roster: str,
    scoring: str,
    pass_td: str,
    dynasty: str,
    best_ball: str,
) -> int:
    """Return the one-based compact core selector ordinal."""
    return CORE_REQUESTS.index((teams, roster, scoring, pass_td, dynasty, best_ball)) + 1


def grade_request_ordinal(
    teams: str,
    roster: str,
    scoring: str,
    pass_td: str,
    dynasty: str,
    best_ball: str,
    playoff_teams: str,
) -> int:
    """Return the one-based compact playoff/champ selector ordinal."""
    return GRADE_REQUESTS.index((teams, roster, scoring, pass_td, dynasty, best_ball, playoff_teams)) + 1
