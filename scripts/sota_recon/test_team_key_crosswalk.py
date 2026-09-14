"""The team key space, and the blocker that was inherited rather than measured.

Two sources were declared BLOCKED on "team-code canonicalization into team_fid franchise
space, the SAME era-alias problem (OTI/HOU, CRD/STL/PHO, CLT/BAL)". For StatsCrew that
claim is measurably false, and for nflcom the real defect is a different one entirely.
These tests keep both findings from decaying back into the assumption.

Run:  python -m pytest scripts/sota_recon/test_team_key_crosswalk.py -q
"""

from __future__ import annotations

import json

import pytest

from . import team_key_crosswalk as TEAM


def _receipt() -> dict:
    if not TEAM.RECEIPT.exists():
        pytest.skip("team key receipt not built in this tree")
    return json.loads(TEAM.RECEIPT.read_text(encoding="utf-8"))


def test_the_catalog_team_key_is_a_function() -> None:
    """Everything below depends on it. A team_code resolving to two franchises in one
    season would make every downstream join silently pick one, and no coverage number
    would show it."""
    catalog = _receipt()["catalog"]
    assert catalog["code_years_resolving_to_two_franchises"] == 0
    assert catalog["is_a_function"] is True


def test_the_statscrew_team_blocker_is_measurably_absent() -> None:
    """StatsCrew publishes the catalog's OWN code space. The era-alias problem was
    inferred from the shape of the codes (CAN/AKR/CLE look archaic) and never measured;
    measured, every team-season resolves to exactly one franchise.

    If this ever fails, the alias machinery genuinely IS needed and the pending note was
    right after all -- which is worth knowing loudly rather than assuming either way."""
    statscrew = _receipt()["statscrew"]
    assert set(statscrew) == set(TEAM.STATSCREW)
    for key, body in statscrew.items():
        assert body["no_catalog_counterpart"] == 0, key
        assert body["resolve_to_exactly_one_team_fid"] == body["team_seasons"], key
        assert body["coverage"] == 1.0, key


def test_the_nflcom_team_column_is_a_doubled_nickname_and_inverts() -> None:
    """A different defect from the declared one: the column holds no code at all. The
    doubling is mechanical on every value, so the repair is deterministic -- but it is a
    REPAIR, and the source may not be joined on this column until it lands."""
    nflcom = _receipt()["nflcom_team_stats"]
    assert nflcom["doubling_is_invertible"] is True
    assert nflcom["exact_halves_doubled"] == nflcom["distinct_values"]
    assert nflcom["distinct_values"] > 40
    # the undoubled values are NICKNAMES, not codes -- if they ever look like 2-3 letter
    # codes, the source changed shape and the correspondence table below is wrong
    nicknames = nflcom["undoubled_nicknames"]
    assert any(len(n) > 4 for n in nicknames), "undoubled values look like codes, not names"
    assert "49ers" in nicknames and "Bears" in nicknames


def test_the_truncation_is_declared_as_unrecoverable() -> None:
    """Inverting the doubling restores the TRUNCATED string, not the true name. Recording
    that stops someone reading 'Combine (AKA Steagle' as the site's real value."""
    nflcom = _receipt()["nflcom_team_stats"]
    assert "truncated" in nflcom["not_recoverable_by_undoubling"].lower()
    assert any("Steagle" in n for n in nflcom["undoubled_nicknames"])
