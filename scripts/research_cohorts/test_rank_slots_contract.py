"""Guard the start-rate measurement contract. These are not unit tests of arithmetic --
they pin down rules that were each got WRONG first and would silently corrupt any
re-measurement if they drifted back.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rank_slots_contract import (  # noqa: E402
    BEST_BALL_FILTER, ELIGIBLE_SQL, FLEX_SHARE, GRAIN, NOT_POOLABLE, POOLABLE,
    QB_ACTIVE_REFINEMENT, slots_expr,
)


def test_eligibility_has_no_snap_or_touch_threshold():
    """Row presence IS the rule. A threshold leaves the rank table identical (97.1% of ranks
    unchanged, max shift 2) while silently truncating the corpus to 2012+, because
    offense_snaps is NULL for every earlier row."""
    sql = ELIGIBLE_SQL.lower()
    for banned in ("offense_snaps", "attempts", "carries", "targets", "receptions"):
        assert banned not in sql, f"eligibility must not gate on {banned}"
    assert "season_type = 'reg'" in sql


def test_qb_refinement_is_not_part_of_eligibility():
    """It exists only for absolute start% of a marginal player (Flacco's 1-snap week),
    never for ranking."""
    assert "offense_snaps" in QB_ACTIVE_REFINEMENT
    assert "offense_snaps" not in ELIGIBLE_SQL


def test_best_ball_is_never_poolable():
    """Best ball ranks by what a player SCORED, not what managers believed -- a different
    ordering (corr 0.445, mean 7.4 ranks apart), not a correction factor."""
    assert "lineup (managed vs best ball)" in NOT_POOLABLE
    corr, diff, top12 = NOT_POOLABLE["lineup (managed vs best ball)"]
    assert corr < 0.6 and diff > 5
    assert not any("best ball" in k for k in POOLABLE), "best ball must never be poolable"
    assert "sleeper_best_ball" in BEST_BALL_FILTER and "false" in BEST_BALL_FILTER


def test_every_poolable_dimension_clears_its_bar():
    """Pooling rank across these is what removes the need for a per-player fit."""
    assert POOLABLE, "poolable set must not be emptied"
    for dim, (corr, mean_diff, top12) in POOLABLE.items():
        assert corr >= 0.94, f"{dim}: rank corr {corr} too low to pool"
        assert mean_diff <= 2.1, f"{dim}: mean rank diff {mean_diff} too large to pool"
        assert top12 >= 92.0, f"{dim}: top-12 agreement {top12}% too low to pool"


def test_slots_is_exact_for_qb_and_guarded_elsewhere():
    """QB slots need no fitted input. Skill positions must not silently use base slots and
    ignore the contested FLEX slot."""
    e = slots_expr("QB")
    assert "num_teams" in e and "roster_QB" in e and "roster_SUPER_FLEX" in e
    for pos in ("RB", "WR", "TE"):
        try:
            slots_expr(pos)
        except NotImplementedError:
            pass
        else:
            raise AssertionError(f"{pos} slots must stay guarded until flex share is re-derived")


def test_flex_share_is_a_distribution_per_scoring():
    """RB+WR+TE must account for essentially the whole FLEX slot at each scoring level."""
    for scoring in ("std", "half", "ppr"):
        total = sum(FLEX_SHARE[p][scoring] for p in ("RB", "WR", "TE"))
        assert 0.97 <= total <= 1.03, f"{scoring}: flex shares sum to {total}"
    # PPR moves the slot from RB to TE -- the measured direction
    assert FLEX_SHARE["TE"]["ppr"] > FLEX_SHARE["TE"]["std"]
    assert FLEX_SHARE["RB"]["ppr"] < FLEX_SHARE["RB"]["std"]


def test_grain_is_week():
    """Season aggregation mixes regimes (a bench stash who becomes an MVP candidate is two
    different players in one number)."""
    assert GRAIN == "week"
