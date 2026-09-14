"""Product cohort request contract for compact matchup serving tables."""

from scripts.research_cohorts.compact_cohort_contract import (
    BRACKET_VALUES,
    CORE_REQUESTS,
    GRADE_REQUESTS,
    TEAM_TIERS,
    core_request_ordinal,
    grade_request_ordinal,
)


def test_compact_request_contract_covers_all_visible_team_formats() -> None:
    """The UI grid has four team markets and three playoff brackets."""
    assert TEAM_TIERS == ("08tm", "10tm", "12tm", "14tm")
    assert BRACKET_VALUES == ("4po", "6po", "8po")
    assert len(CORE_REQUESTS) == 4 * 3 * 3 * 2 * 2 * 2
    assert len(GRADE_REQUESTS) == 4 * 3 * 3 * 2 * 2 * 2 * 3
    assert core_request_ordinal("08tm", "flx", "std", "4pt", "redraft", "managed") == 1
    assert grade_request_ordinal("14tm", "idp", "ppr", "6pt", "dynasty", "best_ball", "8po") == 864
