"""A MAX COLUMN MUST BE MAXED, AND THE LIST OF THEM MUST NOT BE MAINTAINED TWICE.

WHAT THIS PREVENTS, MEASURED 2026-07-31. `stat_contracts.v1.json` held the fact twice:
`aggregation_sets.max_cols` was a hand-written list of FOUR (fg_long, passing_long,
receiving_long, rushing_long) while `stats[].aggregation_class == "MAX"` declared SEVEN --
the same four plus kickoff_return_long, punt_long and punt_return_long. The aggregator reads
the hand list, so those three were SUMMED into player_nfl_season and player_nfl_career.

It shipped, and it produced values that cannot exist. On rows where weekly MAX != weekly SUM
the stored season value matched SUM on 100% and MAX on 0% -- 1,720 rows for
kickoff_return_long, 1,916 for punt_long, 1,086 for punt_return_long, repeated on career at
630 / 342 / 302. 4,407 stored "long" values exceeded 100 yards on a 100-yard field: a
342-yard kickoff return, an 875-yard punt.

The four columns that WERE on the hand list aggregated correctly. That crossed control is
what identified the drift as the cause rather than a MAX bug, and it is why deriving the set
from the declared class is the whole fix.
"""
from __future__ import annotations

from multi_league.core.stat_contracts_loader import (aggregation_sets, contract_classes,
                                                     load_registry)

#: the three that were missing, named so a revert fails loudly on the actual regression
PREVIOUSLY_SUMMED = {"kickoff_return_long", "punt_long", "punt_return_long"}

#: the four that were always right -- the control group
ALWAYS_CORRECT = {"fg_long", "passing_long", "receiving_long", "rushing_long"}


def _declared_max() -> set[str]:
    return {sid for sid, (agg, _der) in contract_classes().items() if agg == "MAX"}


def test_max_cols_is_derived_from_the_declared_class_not_a_second_list():
    """The invariant. If these two ever differ again, a column is being aggregated by the
    wrong function and nothing else in the pipeline will notice."""
    assert aggregation_sets().max_cols == _declared_max()


def test_the_three_columns_that_were_summed_are_now_max_columns():
    assert PREVIOUSLY_SUMMED <= aggregation_sets().max_cols


def test_the_control_group_is_still_max():
    """These four were never broken; a fix that loses them would trade one defect for another."""
    assert ALWAYS_CORRECT <= aggregation_sets().max_cols


def test_the_aggregator_module_emits_MAX_for_every_declared_max_column():
    """Reach through to the consumer. The loader being right is necessary, not sufficient --
    MAX_COLS is bound at import time in the aggregator, which is what actually writes SQL."""
    import multi_league.data_fetchers.aggregate_nfl_stats_fly as AGG

    missing = sorted(_declared_max() - set(AGG.MAX_COLS))
    assert not missing, f"declared MAX but the aggregator would SUM: {missing}"


def test_a_hand_written_max_cols_entry_cannot_smuggle_in_an_undeclared_column():
    """The drift is now impossible in the dangerous direction (declared-but-not-applied). Guard
    the other direction too: a name added only to `aggregation_sets.max_cols` must not silently
    become a MAX column without a contract saying so."""
    hand = set(load_registry()["aggregation_sets"].get("max_cols", []))
    undeclared = sorted(hand - _declared_max())
    assert not undeclared, (
        f"in aggregation_sets.max_cols with no aggregation_class=MAX contract: {undeclared}. "
        "Declare the class; the applied set is derived from it and ignores this list.")
