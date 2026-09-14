"""THE REPAIR GATE MUST FAIL ON THE THREE WAYS A BACKLOG LIES ABOUT ITSELF.

A gate nobody tried to break is a claim, not a gate. Each test here breaks one thing and
asserts the counter MOVES:

  1. mark a remedy applied with no receipt          -> counted
  2. mark it applied with a receipt that is FALSE   -> counted (the predicate is re-run,
                                                       so reverting a fix is caught too)
  3. prove an absence over an EMPTY ledger          -> raises, never passes vacuously

(3) is not hypothetical. The first version of `_no_disposition` read the wrong contract key,
got [], and cheerfully receipted four withdrawals against a ledger it had never seen.
"""
from __future__ import annotations

import json

import pytest

from . import repair_queue as RQ


def _rows():
    return json.loads(RQ.CONTRACT.read_text(encoding="utf-8"))["adjudications"]


def test_every_applied_remedy_names_a_known_receipt_that_passes():
    """The live ledger is clean -- this is the baseline the breakage tests move off."""
    c = RQ.build(schema=set())["counters"]
    assert c["remedy_applied_without_receipt"] == 0
    assert c["adjudications_with_unknown_fault"] == 0
    assert c["adjudications_missing_remedy_status"] == 0


def test_applied_without_a_receipt_is_caught(monkeypatch):
    rows = _rows()
    rows.append({"fault": "SUPERTABLE_GAP", "source": "x", "table_key": "t",
                 "v26_col": "fake_col", "remedy": "do a thing", "remedy_applied": True})
    monkeypatch.setattr(RQ, "load", lambda: rows)
    c = RQ.build(schema=set())["counters"]
    assert c["remedy_applied_without_receipt"] == 1


def test_a_receipt_whose_predicate_is_false_is_caught(monkeypatch):
    """Reverting a landed fix must re-open its repair, not sit behind a stale boolean."""
    rows = _rows()
    rows.append({"fault": "SUPERTABLE_GAP", "source": "x", "table_key": "t",
                 "v26_col": "fake_col", "remedy": "do a thing", "remedy_applied": True,
                 "remedy_receipt": "published_filters_on_cast"})
    monkeypatch.setattr(RQ, "load", lambda: rows)
    monkeypatch.setitem(RQ.RECEIPT_CHECKS, "published_filters_on_cast", lambda: False)
    # TWO, not one, and that is the behaviour worth having: breaking one predicate re-opens
    # EVERY repair resting on it -- the fake row here and the real adjudication that cites
    # the same receipt. A reverted fix cannot leave a landed repair looking landed.
    assert RQ.build(schema=set())["counters"]["remedy_applied_without_receipt"] == 2


def test_an_unknown_receipt_id_is_not_a_receipt(monkeypatch):
    rows = _rows()
    rows.append({"fault": "MAPPING_DEFECT", "source": "x", "table_key": "t",
                 "v26_col": "fake_col", "remedy": "r", "remedy_applied": True,
                 "remedy_receipt": "no_such_receipt_id"})
    monkeypatch.setattr(RQ, "load", lambda: rows)
    assert RQ.build(schema=set())["counters"]["remedy_applied_without_receipt"] == 1


def test_an_unknown_fault_is_counted_not_silently_dropped(monkeypatch):
    rows = _rows()
    rows.append({"fault": "NEWLY_INVENTED_FAULT", "source": "x", "table_key": "t",
                 "v26_col": "c", "remedy": "r", "remedy_applied": False})
    monkeypatch.setattr(RQ, "load", lambda: rows)
    assert RQ.build(schema=set())["counters"]["adjudications_with_unknown_fault"] == 1


def test_absence_over_an_empty_ledger_raises_instead_of_passing(monkeypatch):
    """The bug that shipped: [] made every withdrawal receipt pass without evidence."""
    monkeypatch.setattr(RQ, "_txt", lambda p: json.dumps({"decisions": []}))
    with pytest.raises(ValueError, match="empty ledger"):
        RQ._no_disposition("nflcom_player_career", "solo", "def_tackles_solo")


def test_withdrawal_checks_are_scoped_to_their_source():
    """`solo -> def_tackles_solo` has 19 live rows under OTHER sources; an unscoped check
    would read them and call a real withdrawal a failure."""
    assert RQ._no_disposition("nflcom_player_career", "solo", "def_tackles_solo")
    assert RQ._matches("nflcom_player_logs", "solo", "def_tackles_solo")


def test_a_source_defect_is_not_a_backfill_we_owe():
    rows = RQ.build(schema=set())["rows"]
    assert all(not r["owes_repair"] for r in rows if r["fault"] == "SOURCE_DEFECT")


def test_the_growing_repair_is_singled_out():
    """The 2025 week-9 cutover costs more every week; it must not age like a settled hole."""
    c = RQ.build(schema=set())["counters"]
    assert c["repairs_still_growing"] >= 1
