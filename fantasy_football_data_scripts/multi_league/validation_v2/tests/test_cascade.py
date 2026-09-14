"""Tests for cascade suppression."""

from __future__ import annotations

from multi_league.validation_v2.models import Check, CheckResult
from multi_league.validation_v2.cascade import apply_cascade


def _make_check(name: str, page: str, severity: str = "ERROR") -> Check:
    """Helper to create a minimal Check for testing."""
    return Check(
        name=name,
        page=page,
        table="matchup",
        severity=severity,
        description=f"test check {name}",
        sql_expr="SUM(CASE WHEN 1=0 THEN 1 ELSE 0 END)",
    )


def _make_result(
    check: Check,
    db_name: str,
    fail_count: int = 0,
    passed: bool = True,
    skipped: bool = False,
) -> CheckResult:
    """Helper to create a CheckResult for testing."""
    return CheckResult(
        check=check,
        db_name=db_name,
        fail_count=fail_count,
        passed=passed,
        skipped=skipped,
    )


# ---------------------------------------------------------------------------
# 1. test_cascade_suppresses_downstream
# ---------------------------------------------------------------------------


def test_cascade_suppresses_downstream():
    """Root failure in completeness_has_matchup should suppress matchups-page checks."""
    root_check = _make_check("completeness_has_matchup", "completeness", "BLOCKER")
    downstream_check = _make_check("matchups_team_points_not_null", "matchups")

    results = [
        # Root fails for broken_league
        _make_result(root_check, "broken_league", fail_count=1, passed=False),
        # Downstream also fails for broken_league
        _make_result(downstream_check, "broken_league", fail_count=5, passed=False),
    ]

    results = apply_cascade(results)

    # Root stays visible (not suppressed)
    assert results[0].suppressed is False
    assert results[0].suppressed_by is None

    # Downstream gets suppressed
    assert results[1].suppressed is True
    assert results[1].suppressed_by == "completeness_has_matchup"


# ---------------------------------------------------------------------------
# 2. test_cascade_doesnt_suppress_other_leagues
# ---------------------------------------------------------------------------


def test_cascade_doesnt_suppress_other_leagues():
    """Root failure for league_a should NOT suppress downstream for league_b."""
    root_check = _make_check("completeness_has_matchup", "completeness", "BLOCKER")
    downstream_check = _make_check("matchups_team_points_not_null", "matchups")

    results = [
        # Root fails for league_a
        _make_result(root_check, "league_a", fail_count=1, passed=False),
        # Downstream fails for league_b (different league)
        _make_result(downstream_check, "league_b", fail_count=5, passed=False),
    ]

    results = apply_cascade(results)

    # league_b's downstream failure should NOT be suppressed
    assert results[1].suppressed is False
    assert results[1].suppressed_by is None


# ---------------------------------------------------------------------------
# 3. test_cascade_doesnt_suppress_root
# ---------------------------------------------------------------------------


def test_cascade_doesnt_suppress_root():
    """Two root checks both fail — neither should be suppressed."""
    root_matchup = _make_check("completeness_has_matchup", "completeness", "BLOCKER")
    root_player = _make_check("completeness_has_player_fantasy", "completeness", "BLOCKER")

    results = [
        _make_result(root_matchup, "bad_league", fail_count=1, passed=False),
        _make_result(root_player, "bad_league", fail_count=1, passed=False),
    ]

    results = apply_cascade(results)

    # Neither root check should be suppressed
    assert results[0].suppressed is False
    assert results[1].suppressed is False


# ---------------------------------------------------------------------------
# 4. test_cascade_suppresses_passing_downstream_too
# ---------------------------------------------------------------------------


def test_cascade_suppresses_passing_downstream():
    """Even passing downstream checks get suppressed when root fails.

    This is important because a passing downstream check is meaningless
    if the root data is absent — the pass is vacuously true.
    """
    root_check = _make_check("completeness_has_draft", "completeness", "WARNING")
    downstream_check = _make_check("draft_round_not_null", "draft")

    results = [
        _make_result(root_check, "no_draft_league", fail_count=1, passed=False),
        # Downstream passes (vacuously, because no draft data exists)
        _make_result(downstream_check, "no_draft_league", fail_count=0, passed=True),
    ]

    results = apply_cascade(results)
    assert results[1].suppressed is True
    assert results[1].suppressed_by == "completeness_has_draft"


# ---------------------------------------------------------------------------
# 5. test_cascade_no_failures_no_suppression
# ---------------------------------------------------------------------------


def test_cascade_no_failures_no_suppression():
    """When everything passes, nothing gets suppressed."""
    root_check = _make_check("completeness_has_matchup", "completeness", "BLOCKER")
    downstream_check = _make_check("matchups_team_points_not_null", "matchups")

    results = [
        _make_result(root_check, "good_league", fail_count=0, passed=True),
        _make_result(downstream_check, "good_league", fail_count=0, passed=True),
    ]

    results = apply_cascade(results)

    assert results[0].suppressed is False
    assert results[1].suppressed is False


# ---------------------------------------------------------------------------
# 6. test_cascade_skipped_root_doesnt_trigger
# ---------------------------------------------------------------------------


def test_cascade_skipped_root_doesnt_trigger():
    """A skipped root check should not trigger cascade suppression."""
    root_check = _make_check("completeness_has_matchup", "completeness", "BLOCKER")
    downstream_check = _make_check("matchups_team_points_not_null", "matchups")

    results = [
        # Root is skipped, not truly failed
        _make_result(root_check, "skipped_league", fail_count=0, passed=False, skipped=True),
        _make_result(downstream_check, "skipped_league", fail_count=3, passed=False),
    ]

    results = apply_cascade(results)

    # Downstream should NOT be suppressed because root was skipped not failed
    assert results[1].suppressed is False


# ---------------------------------------------------------------------------
# 7. test_cascade_identity_franchise_suppresses_system
# ---------------------------------------------------------------------------


def test_cascade_identity_franchise_suppresses_system():
    """identity_franchise_id_not_null failure suppresses system page checks."""
    root_check = _make_check("identity_franchise_id_not_null", "identity")
    system_check = _make_check("system_team_points_vs_player_sum", "system")

    results = [
        _make_result(root_check, "broken_fid_league", fail_count=10, passed=False),
        _make_result(system_check, "broken_fid_league", fail_count=5, passed=False),
    ]

    results = apply_cascade(results)

    assert results[0].suppressed is False  # Root stays
    assert results[1].suppressed is True  # System check suppressed
    assert results[1].suppressed_by == "identity_franchise_id_not_null"


# ---------------------------------------------------------------------------
# 8. test_cascade_multiple_roots_first_wins
# ---------------------------------------------------------------------------


def test_cascade_multiple_roots_first_match_wins():
    """When multiple root failures could suppress a check, one wins."""
    root_matchup = _make_check("completeness_has_matchup", "completeness", "BLOCKER")
    root_player = _make_check("completeness_has_player_fantasy", "completeness", "BLOCKER")
    # recaps is in both suppression sets
    downstream_check = _make_check("recaps_some_check", "recaps")

    results = [
        _make_result(root_matchup, "broken_league", fail_count=1, passed=False),
        _make_result(root_player, "broken_league", fail_count=1, passed=False),
        _make_result(downstream_check, "broken_league", fail_count=2, passed=False),
    ]

    results = apply_cascade(results)

    # Downstream is suppressed (by one of the two roots)
    assert results[2].suppressed is True
    assert results[2].suppressed_by in (
        "completeness_has_matchup",
        "completeness_has_player_fantasy",
    )
