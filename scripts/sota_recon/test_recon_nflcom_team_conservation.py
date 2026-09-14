from __future__ import annotations

from .recon_nflcom_team_conservation import FAMILIES, _norm


def test_receiving_defense_20_has_an_independent_team_witness_family() -> None:
    """The receiving-page 20+ allowed column must not disappear behind its mirror."""
    specs = {
        (cat, side, column, canonical)
        for family in FAMILIES
        for _, cat, side, column, canonical, *_ in [_norm(family)]
    }
    assert (
        "receiving",
        "defense",
        "20",
        "def_explosive_pass_allowed",
    ) in specs


def test_scoring_page_duplicates_each_have_an_independent_witness_family() -> None:
    """A duplicate page is still audited; it must not vanish because its target is reused."""
    specs = {
        (cat, side, column, canonical)
        for family in FAMILIES
        for _, cat, side, column, canonical, *_ in [_norm(family)]
    }
    assert {
        ("scoring", "offense", "rec_td", "receiving_tds"),
        ("scoring", "offense", "rsh_td", "rushing_tds"),
        ("scoring", "special-teams", "fgm", "fg_made"),
        ("scoring", "special-teams", "kret_td", "kickoff_return_tds"),
        ("scoring", "special-teams", "pret_t", "punt_return_tds"),
    } <= specs
