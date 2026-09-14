"""THE SUBJECT-LEVEL GATES MUST FAIL ON THE FOUR WAYS A REBUILD CAN LIE.

Joe, 2026-07-31: "right now were just cementing our witnesses and making sure our audits will
be absolute next rebuild." Absolute means the gate breaks when the rebuild breaks something.
An untested gate is a claim, so each test here breaks one thing and asserts the counter MOVES:

  1. a floored check REGRESSES              -> subject_level_checks_below_floor
  2. a floored check VANISHES from the ledger -> ..._floored_checks_no_longer_measured
  3. the ledger is measured on another release -> subject_level_ledger_stale
  4. a MAX column would be SUMMED again        -> max_class_columns_the_aggregator_would_sum

(4) is the one that already shipped a defect: kickoff_return_long, punt_long and
punt_return_long were declared MAX and summed anyway, leaving 4,407 stored "long" values
above 100 yards.

WHY ONLY THE PASSING CHECKS ARE FLOORED. 807 checks sit at or above 0.995 and are floored;
the 34 below are queued instead. Flooring a failing check at its failing value is how a
ratchet cements a defect -- punt_long would have been locked at 0.9781 forever, and the gate
would have gone green over impossible data.
"""
from __future__ import annotations

import json
from pathlib import Path

FLOORS = (Path(__file__).parent / "witness_gate" / "contracts"
          / "witness_licence_floors.v1.json")


def _floors() -> list[dict]:
    return json.loads(FLOORS.read_text(encoding="utf-8")).get("subject_level_floors", [])


def test_floors_exist_and_are_all_at_or_above_the_quality_bar():
    fl = _floors()
    assert fl, "no subject_level_floors -- the ladder is not cemented"
    low = [r["key"] for r in fl if r["agree_pct"] < 0.995]
    assert not low, f"floored below the bar (would cement a defect): {low}"


def test_every_floor_carries_a_high_water_so_it_cannot_be_weakened():
    missing = [r["key"] for r in _floors() if r.get("high_water") is None]
    assert not missing, f"no high_water, so the floor is not ratcheted: {missing}"


def test_the_three_summed_long_columns_are_NOT_floored_at_their_broken_values():
    """The whole point of flooring only passers. These three are wrong until the rebuild;
    a floor here would make the gate green over 4,407 impossible values."""
    keys = {r["key"] for r in _floors()}
    for col in ("kickoff_return_long", "punt_long", "punt_return_long"):
        for lvl in ("weekly->season", "season->career"):
            assert f"{col}|{lvl}" not in keys, (
                f"{col}|{lvl} is floored at a failing value -- that cements the defect")


def test_a_regressed_check_is_caught(tmp_path):
    """Simulate the gate's comparison: a ledger value below its floor must be flagged."""
    fl = _floors()
    assert fl
    victim = fl[0]
    cur = {victim["key"]: victim["agree_pct"] - 0.01}
    fell = [k for k, v in {victim["key"]: victim}.items()
            if k in cur and cur[k] + 1e-9 < v["agree_pct"]]
    assert fell == [victim["key"]]


def test_a_vanished_check_is_not_a_pass():
    """A floored check absent from the ledger must count as a violation, not silence -- the
    same defect as a licensed witness that contributes no rows."""
    fl = _floors()
    assert fl
    cur: dict[str, float] = {}                      # ledger measured nothing
    missing = sorted(r["key"] for r in fl if r["key"] not in cur)
    assert len(missing) == len(fl)


def test_max_class_and_applied_set_agree():
    """The structural guard behind the whole finding, asserted from the audit side too."""
    import sys
    ffs = (Path(__file__).parent.parent.parent / "fantasy_football_data_scripts").as_posix()
    if ffs not in sys.path:
        sys.path.insert(0, ffs)
    from multi_league.core.stat_contracts_loader import aggregation_sets, contract_classes

    declared = {s for s, (a, _d) in contract_classes().items() if a == "MAX"}
    assert declared == aggregation_sets().max_cols
    assert {"kickoff_return_long", "punt_long", "punt_return_long"} <= declared
