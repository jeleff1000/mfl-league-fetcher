"""The meta-gate over decisions that live in PYTHON, not in .json.

`contracts_unenrolled_in_scoreboard` read 0 through a session that added three
correspondence tables holding hundreds of decisions, because a correspondence table is not
a file in `witness_gate/contracts/`. These tests pin the properties that make the
replacement gate actually catch that -- and, in particular, that it catches the case the
obvious implementation misses.

Run:  python -m pytest scripts/sota_recon/test_adjudication_registry_census.py -q
"""

from __future__ import annotations

from . import adjudication_registry_census as CENSUS
from .closure_scoreboard import (ENROLLED_ADJUDICATION_REGISTRIES,
                                 EXEMPT_ADJUDICATION_REGISTRIES)


def test_every_adjudication_registry_is_enrolled_or_exempt_with_a_reason() -> None:
    document = CENSUS.build()
    unenrolled = sorted(set(document["adjudication_registries"])
                        - set(ENROLLED_ADJUDICATION_REGISTRIES)
                        - set(EXEMPT_ADJUDICATION_REGISTRIES))
    assert unenrolled == [], (
        f"{unenrolled} decides something and nothing says what. Enrol it in "
        "closure_scoreboard.ENROLLED_ADJUDICATION_REGISTRIES with one line saying what it "
        "decides, or declare it a lookup with the reason")
    for name, reason in EXEMPT_ADJUDICATION_REGISTRIES.items():
        assert reason, f"{name}: an exemption without a reason is just a subtraction"
    for name, reason in ENROLLED_ADJUDICATION_REGISTRIES.items():
        assert reason and len(reason) > 20, f"{name}: enrolment must say what it decides"


def test_a_short_valued_correspondence_table_is_still_caught() -> None:
    """THE CASE A PROSE RULE MISSES, and the reason the SINK rule exists.

    `MAPPED` holds the single most consequential decisions in the program -- which
    canonical cell a foreign column becomes -- and its values are bare column names. A
    classifier that looks for reasons would file all 164 of them as a lookup table and the
    gate would read clean while the largest decision registry sat outside it."""
    document = CENSUS.build()
    caught = set(document["adjudication_registries"])
    for module in ("nflcom_column_adjudication", "statscrew_column_adjudication",
                   "nflcom_splits_column_adjudication"):
        identifier = f"{module}.py:MAPPED"
        assert identifier in caught, f"{identifier} classified as a lookup table"
        row = next(r for r in document["registries"] if r["id"] == identifier)
        assert "SINK" in row["matched_rules"], (
            f"{identifier} was caught by PROSE alone -- if its values ever shorten it "
            "would fall out of the gate")


def test_the_denominator_is_every_module_level_registry_not_only_the_flagged_ones() -> None:
    """The counter that failed did so by measuring a container type. This one reports the
    FULL population beside the flagged subset, so a classifier that quietly stops
    flagging things is visible as a shrinking numerator over a stable denominator."""
    document = CENSUS.build()
    counters = document["counters"]
    assert counters["module_level_registries"] > 300
    assert (counters["adjudication_registries"] + counters["lookup_registries"]
            == counters["module_level_registries"]), "a registry fell out of both classes"
    assert len(document["registries"]) == counters["module_level_registries"]
    assert all(row["classification"] in ("ADJUDICATION", "LOOKUP")
               for row in document["registries"])


def test_the_sink_rule_is_what_a_new_generator_cannot_avoid() -> None:
    """Writing decisions means calling `apply_to_ledger`. That is why the SINK rule is the
    load-bearing one: a new adjudication pass lands UNENROLLED and FAILS, regardless of
    what its tables are named or what shape their values take."""
    assert "apply_to_ledger" in CENSUS.DECISION_SINKS
    assert "DISPOSITIONS_PATH" in CENSUS.DECISION_SINKS
    document = CENSUS.build()
    sink_modules = {row["module"] for row in document["registries"]
                    if "SINK" in row["matched_rules"]}
    assert "nflcom_splits_column_adjudication.py" in sink_modules, (
        "the newest generator was not recognised as a decision producer")


def test_every_adjudication_generator_can_report_its_refusals() -> None:
    """An escalation invisible to the scoreboard is indistinguishable from backlog, and
    the next session pays for it twice -- once re-deriving it, once risking deciding it
    the easy way."""
    from .column_escalation_census import build as escalation_build

    document = escalation_build()
    assert document["generators_without_escalation_export"] == [], (
        "an adjudication pass cannot report what it refused")
    assert (document["counters"]["escalated_rows"]
            + document["counters"]["open_not_yet_reached"]
            == document["counters"]["open_rows"])


def test_the_gate_walks_SUBDIRECTORIES_not_just_one_directory() -> None:
    """THE DEFECT THIS MODULE SHIPPED WITH.

    The .json gate it replaced counted one directory. The first version of this module
    also counted one directory -- `os.listdir(HERE)` -- and so never opened
    corrections/, witness_gate/ or witness_audit_v2/, which between them hold 94
    module-level registries including one literally named ADJUDICATION_RULES.

    A denominator that stops at a directory boundary is the whole failure mode here, so
    the boundary is pinned."""
    document = CENSUS.build()
    modules = {row["module"] for row in document["registries"]}
    nested = {m for m in modules if "/" in m}
    assert nested, "the census is walking a single directory again"
    for required in ("witness_gate/", "corrections/"):
        assert any(m.startswith(required) for m in modules), (
            f"{required} is invisible to the registry gate")
    assert "witness_gate/build_stat_contracts.py:ADJUDICATION_RULES" in set(
        document["adjudication_registries"])


def test_a_registry_built_by_a_CALL_is_still_a_registry() -> None:
    """`QUARANTINED_SOURCES = set(...)` is a decision about which sources may NOT be
    adjudicated, inside a sink module, and the literal-only walk could not see it. Nothing
    about `set(...)` versus `{...}` changes what a registry decides."""
    document = CENSUS.build()
    caught = set(document["adjudication_registries"])
    assert "column_dossier.py:QUARANTINED_SOURCES" in caught
    assert document["counters"]["registries_built_by_call_or_comprehension"] > 0
    # ...but a path is not a container, and declining it must stay deliberate
    ids = {row["id"] for row in document["registries"]}
    assert not any(i.endswith(":BIO") or i.endswith(":PFR_ROOT") for i in ids), (
        "Path(...) assignments swept in as registries -- the mirror defect")
