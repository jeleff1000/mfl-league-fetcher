"""The observed-capacity tier contract.

THE DECLARED-SLOT PATH IS GONE (Joe, 2026-08-03). There is one way to compute a team number
now, and these tests cover it. The old tests exercised teams_bucket_sql / team_equiv_sql /
eff_slots_sql / settings_teams_select, which read `roster_*` columns and did arithmetic. That
path failed three ways: league_settings understates real rosters by 3-5 spots, its bench column
swings 20 points between adjacent sizes, and it only ever modelled QB/RB/WR/TE so the other
five positions had no formula at all. It was deleted rather than deprecated, and its tests
with it.
"""
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import position_slots_contract as C


@pytest.fixture()
def con():
    return duckdb.connect()


# ---------------------------------------------------------------------------
# coverage: 9 positions x 2 stats x 4 tiers = 72 rules
# ---------------------------------------------------------------------------

def test_every_position_and_stat_has_measured_cuts():
    for pos in C.TIER_POSITIONS:
        for stat in C.TIER_STATS:
            if pos == "QB":
                for lane in ("flx", "sflx"):
                    assert len(C.tier_cuts(pos, stat, lane)) == 3
            else:
                assert len(C.tier_cuts(pos, stat)) == 3


def test_the_grid_is_seventy_two_rules():
    """9 positions x 2 stats x 4 tiers. QB carries two lanes, so 20 cut sets serve them."""
    assert len(C.TIER_POSITIONS) == 9
    assert len(C.TIER_STATS) == 2
    assert len(C.TIER_LABELS) == 4
    assert len(C.TIER_POSITIONS) * len(C.TIER_STATS) * len(C.TIER_LABELS) == 72


def test_cuts_are_strictly_increasing():
    for key, cuts in C.TIER_CUTS.items():
        assert cuts[0] < cuts[1] < cuts[2], f"{key} cuts must ascend, got {cuts}"


def test_an_unmeasured_position_raises_rather_than_guessing():
    with pytest.raises(KeyError):
        C.tier_cuts("P", "started")


# ---------------------------------------------------------------------------
# the properties the cuts must have
# ---------------------------------------------------------------------------

def test_cuts_clear_the_mass_points_they_sit_on():
    """K and DST start exactly one per team, so 8/10/12 are dense mass points.

    A cut landing exactly on one would sweep that whole league size up a tier -- the v2 bug
    that made a 10-team league read 12tm. Cuts must fall strictly between the counts they
    separate.
    """
    for pos in ("K", "DEF"):
        c1, c2, c3 = C.tier_cuts(pos, "started")
        assert 8 < c1 < 10, f"{pos} c1 must separate 8 from 10, got {c1}"
        assert 10 < c2 < 12, f"{pos} c2 must separate 10 from 12, got {c2}"
        assert 12 < c3 < 14, f"{pos} c3 must separate 12 from 14, got {c3}"


def test_superflex_is_its_own_rule_set():
    """Superflex IS the QB axis (R1): started goes 1.0 -> 1.8 per team, so every cut moves."""
    for stat in C.TIER_STATS:
        flx = C.tier_cuts("QB", stat, "flx")
        sfx = C.tier_cuts("QB", stat, "sflx")
        assert all(a > b for a, b in zip(sfx, flx)), f"{stat}: sflx {sfx} must exceed flx {flx}"


def test_rostered_always_exceeds_started():
    """You cannot start more of a position than you roster."""
    for pos in C.TIER_POSITIONS:
        lanes = ("flx", "sflx") if pos == "QB" else (None,)
        for lane in lanes:
            ros = C.tier_cuts(pos, "rostered", lane)
            st = C.tier_cuts(pos, "started", lane)
            assert all(r >= s for r, s in zip(ros, st)), f"{pos}/{lane}: {ros} vs {st}"


def test_tier_sql_emits_the_four_bucket_alphabet(con):
    cuts = C.tier_cuts("WR", "rostered")
    sql = C.observed_capacity_tier_sql("cap", cuts=cuts)
    probes = (cuts[0] - 1, cuts[0] + 1, cuts[1] + 1, cuts[2] + 1)
    got = [con.execute(f"SELECT {sql} FROM (SELECT {v}::DOUBLE AS cap)").fetchone()[0]
           for v in probes]
    assert got == list(C.TIER_LABELS)


def test_cuts_are_required_so_no_caller_inherits_the_receiver_ladder():
    """`cuts` defaulted to WR's once, silently giving QB, RB and TE the receiver ladder."""
    with pytest.raises(TypeError):
        C.observed_capacity_tier_sql("cap")


# ---------------------------------------------------------------------------
# provenance -- every number says where it came from and whether it passed
# ---------------------------------------------------------------------------

def test_locked_and_provisional_partition_the_grid():
    """Every pair is either locked or has its failing gate named. No silent middle."""
    for pos in C.TIER_POSITIONS:
        for stat in C.TIER_STATS:
            key = (pos, stat)
            assert key in C.FULLY_DERIVED or key in C.PROVISIONAL_REASON, \
                f"{key} is neither locked nor explained"
            assert not (key in C.FULLY_DERIVED and key in C.PROVISIONAL_REASON), \
                f"{key} cannot be both"


def test_idp_is_accepted_as_is_and_says_so():
    assert C.IDP_ACCEPTED_AS_IS is True
    assert set(C.IDP_FLEX_SHARE) == {"DL", "LB", "DB"}
    assert abs(sum(C.IDP_FLEX_SHARE.values()) - 1.0) < 0.01


def test_tiers_record_the_lane_they_were_derived_on():
    """Format is its own cohort axis, so a tier must never mix formats."""
    assert C.TIERS_DERIVED_ON == "managed redraft"
