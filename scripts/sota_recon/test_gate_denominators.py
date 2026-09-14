"""A counter reading zero must publish the denominator it read zero against.

Joe, 2026-07-27: "whenever a counter reads clean, check its denominator against the layer
beneath it." Every wrong number in this programme so far was a PASSING counter whose
denominator was authored one layer above where the truth lived -- shards instead of work
items, processes instead of records, sources instead of columns, our own TOC instead of
the site's index.

`primary_lineages_without_capture_contract` was the instance inside the scoreboard itself.
It read zero because six lineages were subtracted from its denominator by a set literal
written inline in the expression. Two of the six had since acquired real contracts, so
subtracting them was redundant; the other four had none. And two of THOSE four were not
missing contracts at all -- `ngs` and `pfa_loc` are the same origins as the
`nextgen_stats` and `profootballarchives` contracts under a different spelling, patched at
both ends so the mismatch cancelled itself out and presented as coverage.
"""

from __future__ import annotations

from .capture_contracts import CONTRACTS
from .closure_scoreboard import (
    CAPTURE_CONTRACT_EXEMPT_LINEAGES,
    LINEAGE_CONTRACT_ALIASES,
    build,
)
from .sources import registry


def _gate(name: str) -> dict:
    found = [g for g in build()["zero_gates"] if g["gate"] == name]
    assert found, f"{name} is not in the scoreboard"
    return found[0]


def test_the_lineage_gate_publishes_its_own_denominator() -> None:
    """A zero is only as good as the set it was measured over, so the set is reported."""
    gate = _gate("primary_lineages_without_capture_contract")
    primary = {s.lineage for s in registry(include_subject=True).values()
               if s.witness_class == "primary"}
    assert set(gate["denominator"]) == primary, (
        "the gate must report the FULL primary-lineage set it measured, not a "
        "pre-subtracted remainder")
    accounted = set(gate["detail"]) | set(gate["exempt_with_reason"]) | {
        lineage for lineage in primary
        if LINEAGE_CONTRACT_ALIASES.get(lineage, lineage) in CONTRACTS
    }
    assert accounted == primary, "every primary lineage must be contracted or exempt"


def test_no_lineage_is_removed_from_the_denominator_without_a_reason() -> None:
    gate = _gate("capture_contract_exemptions_without_reason")
    assert gate["value"] == 0
    for lineage, reason in CAPTURE_CONTRACT_EXEMPT_LINEAGES.items():
        assert reason and len(reason) > 40, (
            f"{lineage}: an exemption without a real reason is just a subtraction")


def test_the_four_formerly_hidden_lineages_are_now_visible() -> None:
    """internal, ngs, pbp_merged and pfa_loc all read as covered while being subtracted
    by hand. Each must now be explicitly one thing or the other."""
    gate = _gate("primary_lineages_without_capture_contract")
    exempt = set(gate["exempt_with_reason"])
    aliased = set(LINEAGE_CONTRACT_ALIASES)
    for lineage in ("internal", "pbp_merged"):
        assert lineage in exempt, f"{lineage} must be an explicit exemption"
    for lineage in ("ngs", "pfa_loc"):
        assert lineage in aliased, (
            f"{lineage} is a spelling of a contracted origin and must be DECLARED as an "
            "alias, not compensated for on both sides of the comparison")
        assert LINEAGE_CONTRACT_ALIASES[lineage] in CONTRACTS


def test_an_alias_must_point_at_a_real_contract() -> None:
    """An alias to a nonexistent contract would recreate the original defect: a lineage
    that looks covered because a name was mapped away."""
    for lineage, contract in LINEAGE_CONTRACT_ALIASES.items():
        assert contract in CONTRACTS, f"{lineage} aliases missing contract {contract}"
        assert lineage not in CONTRACTS, (
            f"{lineage} has its own contract; the alias hides which one is authoritative")
