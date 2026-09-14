"""Tests for the DERIVED-TOC lane (O.8e).

The failure guarded: a hand-authored table-of-contents can only contain known knowns.
This lane reads the site's own link graph out of pages we already hold, so a section
nobody thought to list still gets counted.
"""

from __future__ import annotations

from . import derive_source_toc as D


def test_normalizer_keeps_section_words_literal():
    """Regression on this lane's own bug (2026-07-26): a /[a-z0-9-]{8,}/ rule collapsed
    'football' -- an 8-character SECTION word -- into {slug}, hiding every StatsCrew
    sport section. Section words must survive; only entity slugs collapse."""
    assert D._normalize("/football/roster/t-CHI/y-1924") == "/football/roster/t-{id}/y-{year}"
    assert "/football/" in D._normalize("/football/stats/p-anderhun001")
    assert D._normalize("/players/frank-abruzzino/stats") == "/players/{slug}/stats"


def test_normalizer_rejects_non_navigable_hrefs():
    for bad in ("mailto:a@b.c", "javascript:void(0);", "#top", "tel:555"):
        assert D._normalize(bad) is None


def test_every_source_declares_a_retained_page_cache():
    for src, cfg in D.PAGE_CACHES.items():
        assert cfg.get("root") and cfg.get("glob"), src


def test_chrome_exclusions_all_carry_a_reason():
    for rx, reason in D.CHROME:
        assert len(reason) > 5, rx


def test_accounted_patterns_reference_real_toc_entries():
    """Every ACCOUNTED mapping must point at an entry that actually exists in the
    capture contract -- otherwise a pattern is 'accounted for' by a fiction."""
    from .capture_contracts import CONTRACTS
    NAV_ONLY = {"site index (navigation only)", "site search (navigation only)"}
    for src, rules in D.ACCOUNTED.items():
        entries = {t["toc_entry"] for t in CONTRACTS[src]["toc"]} | NAV_ONLY
        for _rx, entry in rules:
            assert entry in entries, f"{src}: '{entry}' is not a capture-contract TOC entry"
