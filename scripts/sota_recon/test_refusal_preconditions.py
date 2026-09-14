"""RULE C's own tests: the refusal gate has to be unable to pass by construction.

Every test here corresponds to a way the 2026-07-27/28 failures could recur.

    python -m pytest scripts/sota_recon/test_refusal_preconditions.py -q
"""
from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path

import pytest

from . import refusal_preconditions as RP
from . import composite_column_adjudication as CA
from .composite_column_adjudication import apply_decisions
from .composite_witness_lane import InternalResolution, LaneObservation, load_specs


def _document():
    return RP.build()


def test_no_refusal_stands_on_ground_that_has_gone():
    """THE GATE. A SPENT precondition is the splits/situational failure, exactly."""
    document = _document()
    spent = [f"{row['refusal']}: {row['observation']}" for row in document["spent"]]
    assert not spent, ("refusals whose precondition no longer holds -- lift them or "
                       f"re-declare them on ground that still exists: {spent}")


def test_every_discovered_refusal_site_is_checked():
    """Rule B applied to Rule C. A gate over only the refusals someone remembered to
    type in measures the typist, not the program."""
    document = _document()
    assert not document["unenrolled"], (
        "refusal sites with no enrolled precondition: "
        f"{[row['refusal'] for row in document['unenrolled']]}")


def test_no_enrolment_refuses_nothing():
    document = _document()
    assert not document["orphaned"], (
        "enrolled refusals whose site has vanished -- they read as checked and hold "
        f"nothing back: {[row['refusal'] for row in document['orphaned']]}")


def test_the_denominator_is_discovered_from_more_than_one_inventory():
    """A single-inventory denominator is how the .json-only contract gate read zero
    while three correspondence tables landed unenrolled."""
    document = _document()
    kinds = document["denominator"]["discovered_by_kind"]
    assert len(kinds) >= 4, f"only {len(kinds)} refusal-site kinds are being walked: {kinds}"
    assert sum(kinds.values()) == document["counters"]["refusal_sites_discovered"]


def test_every_precondition_names_a_real_predicate():
    for refusal_id, entry in RP.REFUSALS.items():
        kind = entry["precondition"]["kind"]
        assert kind in RP.PREDICATES, f"{refusal_id}: unknown predicate {kind!r}"


def test_every_refusal_says_what_would_settle_it():
    """A refusal with no settling condition is a permanent state wearing a queue's
    clothes. `standing` refusals say NEVER, and say WHY -- that is a settling condition
    too, and it is the only legal way to say never."""
    for refusal_id, entry in RP.REFUSALS.items():
        settles = entry.get("settles_when", "")
        assert len(settles) > 30, f"{refusal_id}: settles_when is too thin: {settles!r}"
        if entry.get("standing"):
            assert "NEVER" in settles, (
                f"{refusal_id}: a standing refusal must say NEVER explicitly, so it can "
                "never be mistaken for backlog")


def test_a_predicate_that_cannot_observe_is_not_a_predicate():
    """Every predicate must report WHAT it looked at. A bare boolean is a claim with no
    receipt, which is the shape of every failure this module exists to catch."""
    for row in _document()["refusals"]:
        if row["state"] in {"HOLDS", "SPENT"}:
            assert len(row["observation"]) > 40, (
                f"{row['refusal']}: observation {row['observation']!r} does not name the "
                "state it read")


def test_superseded_preconditions_stay_spent():
    """The evidence that this mechanism was needed. If one of these ever evaluates TRUE
    again, the repair it records has been reverted and the old refusal is live again --
    which must be loud, not silent."""
    found = 0
    for refusal_id, entry in RP.REFUSALS.items():
        superseded = entry.get("superseded")
        if not superseded:
            continue
        found += 1
        spec = superseded["precondition"]
        holds, observation = RP.PREDICATES[spec["kind"]](spec)
        assert not holds, (
            f"{refusal_id}: the SUPERSEDED precondition {spec} is true again -- the repair "
            f"recorded as landing {superseded['spent_utc']} has been reverted. {observation}")
    assert found >= 2, "the two 2026-07-28 spent preconditions must stay recorded"


def test_signoff_ledger_items_are_well_formed():
    ledger = json.loads(RP.SIGNOFF_LEDGER.read_text(encoding="utf-8"))
    vocabulary = set(ledger["status_vocabulary"])
    ids = set()
    for item in ledger["items"]:
        assert item["status"] in vocabulary, f"{item['id']}: status {item['status']!r}"
        assert len(item["asks"]) > 40, f"{item['id']}: asks nothing legible"
        assert item["id"] not in ids, f"duplicate signoff id {item['id']}"
        ids.add(item["id"])
        if item["status"] != "PENDING":
            assert item["decided_utc"], f"{item['id']}: decided with no timestamp"


def test_every_no_signoff_refusal_points_at_a_real_ledger_item():
    ledger = json.loads(RP.SIGNOFF_LEDGER.read_text(encoding="utf-8"))
    ids = {item["id"] for item in ledger["items"]}
    for refusal_id, entry in RP.REFUSALS.items():
        spec = entry["precondition"]
        if spec["kind"] == "NO_SIGNOFF":
            assert spec["item"] in ids, (
                f"{refusal_id}: waits on {spec['item']!r}, which nobody has been asked")


def test_an_approved_signoff_makes_its_refusal_spend(monkeypatch):
    """The mechanism itself, exercised. Flip an item to APPROVED and the refusals waiting
    on it must go SPENT -- otherwise this gate would sit green through exactly the event
    it exists to catch."""
    waiting = [rid for rid, entry in RP.REFUSALS.items()
               if entry["precondition"]["kind"] == "NO_SIGNOFF"]
    assert waiting, "no refusal waits on a signoff -- the predicate is untested in anger"
    item = RP.REFUSALS[waiting[0]]["precondition"]["item"]
    ledger = RP.load_signoff_ledger()
    for entry in ledger["items"]:
        if entry["id"] == item:
            entry["status"] = "APPROVED"
    monkeypatch.setattr(RP, "load_signoff_ledger", lambda: ledger)
    spent = {row["refusal"] for row in RP.build()["spent"]}
    expected = {rid for rid, entry in RP.REFUSALS.items()
                if entry["precondition"].get("item") == item}
    assert expected <= spent, (
        f"approving {item!r} left {expected - spent} standing -- the gate would not have "
        "noticed the decision it was waiting for")


def test_the_scoreboard_enrols_rule_c():
    from .closure_scoreboard import build as scoreboard_build

    gates = {gate["gate"] for gate in scoreboard_build()["zero_gates"]}
    for required in ("refusals_with_spent_preconditions",
                     "refusal_sites_without_a_checked_precondition",
                     "refusal_enrolments_orphaned"):
        assert required in gates, f"{required} is not a zero-gate"


def test_composite_failures_are_five_groups_not_126_independent_refusals():
    document = _document()
    composite = [
        row for row in document["refusals"]
        if row["site_kind"] == "COMPOSITE_SPEC_BLOCKER"
    ]

    assert len(composite) == 5
    assert sum(row["affected_rows"] for row in composite) == 126
    assert all(row["state"] == "HOLDS" for row in composite)


def test_nflcom_grain_refusal_names_the_resolved_conflict():
    document = _document()
    nfl = next(
        row for row in document["refusals"]
        if row["refusal"] == "composite_spec_blocker:nflcom-l7-fg-buckets-v1"
    )

    assert nfl["precondition"]["kind"] == "NO_LIKE_FOR_LIKE_NFLCOM_L7_GRAIN"
    assert "mike-mercer" in nfl["observation"]
    assert "Memorial Stadium" in nfl["observation"]


def test_all_generic_signoffs_remain_pending():
    ledger = RP.load_signoff_ledger()
    assert sum(item["status"] == "PENDING" for item in ledger["items"]) == 8


def _write_all_pass_receipt(tmp_path: Path) -> Path:
    receipt = json.loads(RP.COMPOSITE_RECEIPT.read_text(encoding="utf-8"))
    for spec in load_specs():
        external = tuple(target for target in spec.targets if target not in spec.internal_targets)
        observations = [asdict(LaneObservation(
            spec.spec_id,
            spec.sources[0],
            spec.kind,
            external,
            1,
            0,
            "PASS",
            1,
            (("fixture", "current licensed result"),),
        ))]
        internal = [] if not spec.internal_targets else [asdict(InternalResolution(
            spec.spec_id,
            "player_bio",
            spec.internal_targets,
            1,
            0,
            "PASS",
            1,
            (("fixture", "internal only"),),
        ))]
        status = receipt["spec_statuses"][spec.spec_id]
        status["resolution_status"] = status["license_status"] = "PASS"
        status["result"] = {
            "observations": observations,
            "internal_resolutions": internal,
            "compared_rows": 1,
            "rejected_rows": 0,
            "errors": [],
        }
    path = tmp_path / "all-pass-receipt.json"
    path.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_current_pass_spends_each_composite_refusal(monkeypatch, tmp_path: Path):
    path = _write_all_pass_receipt(tmp_path)
    monkeypatch.setattr(RP, "COMPOSITE_RECEIPT", path)

    composite = [
        row for row in RP.build()["refusals"]
        if row["site_kind"] == "COMPOSITE_SPEC_BLOCKER"
    ]

    assert len(composite) == 5
    assert all(row["state"] == "SPENT" for row in composite)


def test_pass_apply_resolves_blockers_and_clears_scoreboard(monkeypatch, tmp_path: Path):
    receipt = _write_all_pass_receipt(tmp_path)
    dispositions = tmp_path / "column_dispositions.v1.json"
    dispositions.write_text('{"version": 1, "decisions": []}\n', encoding="utf-8")
    monkeypatch.setattr(RP, "COMPOSITE_RECEIPT", receipt)
    monkeypatch.setattr(RP, "COMPOSITE_DISPOSITIONS", dispositions, raising=False)

    assert len([
        row for row in RP.build()["spent"]
        if row.get("site_kind") == "COMPOSITE_SPEC_BLOCKER"
    ]) == 5
    assert apply_decisions(receipt, dispositions)["written"] == 126

    document = RP.build()
    composite = [
        row for row in document["refusals"]
        if row.get("site_kind") == "COMPOSITE_SPEC_BLOCKER"
        or row["refusal"].startswith("composite_spec_blocker:")
    ]
    assert composite == []
    assert not document["spent"]
    assert not document["orphaned"]

    from .closure_scoreboard import build as scoreboard_build
    scoreboard = scoreboard_build()
    assert scoreboard["gate_pass"], scoreboard["failing_gates"]


def test_non_composite_owned_row_never_counts_as_composite_closure(monkeypatch, tmp_path: Path):
    receipt = _write_all_pass_receipt(tmp_path)
    dispositions = tmp_path / "column_dispositions.v1.json"
    protected = {
        "key": "pfr_adj_passing|*|awards",
        "disposition": "EXCLUDED_WITH_REASON",
        "reason": "hand decision",
        "evidence": "not composite-owned output",
        "generated_by": "scripts.sota_recon.some_other_generator",
    }
    dispositions.write_text(
        json.dumps({"version": 1, "decisions": [protected]}, indent=2) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(RP, "COMPOSITE_RECEIPT", receipt)
    monkeypatch.setattr(RP, "COMPOSITE_DISPOSITIONS", dispositions, raising=False)

    result = apply_decisions(receipt, dispositions)
    awards = next(
        row for row in RP.build()["refusals"]
        if row["refusal"] == "composite_spec_blocker:pfr-awards-v1"
    )

    assert result["hand_decisions_preserved"] == 1
    assert awards["state"] == "SPENT"


def test_composite_refusals_are_generated_from_shared_registries(monkeypatch):
    spec_id = "pfr-awards-v1"
    monkeypatch.setitem(CA.BLOCKER_CODES_BY_SPEC, spec_id, ("MUTATED_SHARED_CODE",))
    monkeypatch.setitem(CA.SETTLING_EVIDENCE_BY_SPEC, spec_id, "mutated shared settlement")

    entry = RP.composite_refusal_entries()[f"composite_spec_blocker:{spec_id}"]

    assert "MUTATED_SHARED_CODE" in entry["refuses"]
    assert "mutated shared settlement" in entry["settles_when"]


@pytest.mark.parametrize("kind", sorted(RP.PREDICATES))
def test_every_predicate_is_actually_used(kind):
    """An unused predicate is a mechanism nobody has exercised, and it will be wrong the
    first time it matters."""
    used = {entry["precondition"]["kind"] for entry in RP.REFUSALS.values()}
    used |= {entry["superseded"]["precondition"]["kind"]
             for entry in RP.REFUSALS.values() if entry.get("superseded")}
    assert kind in used, f"predicate {kind!r} is declared and never used"
