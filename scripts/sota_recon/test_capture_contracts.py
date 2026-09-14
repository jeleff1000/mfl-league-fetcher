"""Law B (CAPTURE) gate tests -- O.8.

The failure this guards (Joe, 2026-07-26): "we have StatsCrew" and "we have all of
StatsCrew" were indistinguishable to every counter in the program. A thin slice
registered as if complete passed every gate.
"""

from __future__ import annotations

from . import capture_contracts as CC
from .sources import registry


def test_every_toc_entry_has_a_legal_status():
    doc = CC.build()
    assert doc["counters"]["invalid_status_rows"] == 0, doc["invalid_status_rows"]


def test_every_toc_entry_carries_a_note():
    for source, c in CC.CONTRACTS.items():
        for t in c["toc"]:
            assert len(t.get("note", "")) > 15, f"{source}:{t['toc_entry']}: note required"


def test_excluded_entries_state_why_not_just_that():
    """EXCLUDED is a permanent decision -- it must say what makes the family
    redundant or out of domain, so a later session can challenge the reasoning."""
    for source, c in CC.CONTRACTS.items():
        for t in c["toc"]:
            if t["status"] == "EXCLUDED":
                assert len(t["note"]) > 40, f"{source}:{t['toc_entry']}: thin exclusion"


def test_registered_sources_named_in_contracts_exist():
    reg = registry(include_subject=True)
    for source, c in CC.CONTRACTS.items():
        for t in c["toc"]:
            for s in t.get("registered_sources") or []:
                assert s in reg, f"{source}:{t['toc_entry']}: unknown source {s}"


def test_thin_slice_sources_are_not_recorded_as_complete():
    """StatsCrew (rosters only) and PFA (participation only) MUST carry open TOC
    entries. If either ever shows zero open families, someone declared a thin capture
    complete -- exactly the Law B failure mode."""
    doc = {c["source"]: c for c in CC.build()["contracts"]}
    for src in ("statscrew", "profootballarchives"):
        assert doc[src]["counters"]["queued"] > 0, (
            f"{src}: capture contract shows no open families, but the site publishes "
            "far more than we captured -- this counter going to zero requires CAPTURE, "
            "never redefinition")


def test_pfa_expansion_is_recorded_as_a_parse_not_a_crawl():
    """The measured 2026-07-26 finding: PFA raw boxscore HTML is retained on disk, so
    the stat content is already captured at the byte level. Losing this note would
    turn a cheap re-parse back into an expensive re-crawl decision."""
    note = CC.CONTRACTS["profootballarchives"]["law_b_note"]
    assert "PARSE" in note.upper() and "raw HTML" in note


def test_newspaper_open_entries_are_owned_by_the_newspaper_program():
    """Newspaper capture completeness is unbounded by nature; its open entries must
    stay attributed to the newspaper program and never be arbitrated in this lane."""
    c = CC.CONTRACTS["newspaper"]
    assert "NEWSPAPER PROGRAM OWNS" in c["law_b_note"].upper()
    open_rows = [t for t in c["toc"] if t["status"] == "QUEUED"]
    assert open_rows and all("PROGRAM" in t["note"].upper() for t in open_rows)


def test_open_entries_api_is_source_tagged():
    rows = CC.open_entries()
    assert rows and all("source" in r and "toc_entry" in r for r in rows)


# ---- the parallel-league rule (2026-07-28) -------------------------------------------

# The subject's measured boundary: 78 team codes, every one NFL-lineage -- the 1920s
# APFA clubs, the AAFC 1946-49, the AFL 1960-69, and the modern NFL. Pinned because the
# capture rule ("a league the subject has never carried is an expansion, and expansion is
# declined") is DERIVED from this set. If the set moves, the rule must be re-derived
# rather than quietly kept.
NFL_LINEAGE_TEAM_CODES = {
    "AKR", "ARI", "ATL", "BAL", "BCL", "BDA", "BKN", "BOS", "BRL", "BUF", "CAN", "CAR",
    "CHH", "CHI", "CHR", "CHT", "CIN", "CLE", "CLI", "COL", "CRD", "DAL", "DAY", "DEN",
    "DET", "DTX", "DUL", "EVN", "FRN", "GNB", "HAM", "HOU", "HRT", "IND", "JAX", "KAN",
    "KEN", "LAB", "LAC", "LAD", "LAR", "LOU", "LVR", "MIA", "MIL", "MIN", "MUN", "NOR",
    "NWE", "NYB", "NYG", "NYJ", "NYT", "NYY", "OAK", "OOR", "PHI", "PHO", "PIT", "POT",
    "PRT", "PRV", "RAC", "RAI", "RAM", "RCH", "RII", "SDG", "SEA", "SFO", "SIS", "STL",
    "TAM", "TEN", "TOL", "TON", "TOR", "WAS",
}


def test_the_subject_boundary_the_league_rule_is_derived_from_has_not_moved():
    """THE REOPENING CONDITION, made a fact rather than a note.

    Joe declined league expansion on 2026-07-28, and the capture contract records that as
    a standing decision rather than a queue item. The decision is only sound while the
    subject really is NFL-lineage-only -- that is what makes a parallel league an
    EXPANSION rather than a coverage gap. So the set it was measured from is pinned here:
    a team code outside it means the boundary moved and the rule must be re-derived, not
    silently kept.
    """
    import duckdb
    import pytest

    from .sources import latest_v26

    try:
        release = latest_v26()
    except FileNotFoundError:
        pytest.skip("no v26 release in this tree")
    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    try:
        observed = {
            row[0] for row in connection.execute(
                "SELECT DISTINCT nfl_team FROM read_parquet(?)", [release]).fetchall()
            if row[0]
        }
    finally:
        connection.close()
    intruders = sorted(observed - NFL_LINEAGE_TEAM_CODES)
    assert not intruders, (
        f"team codes outside the NFL lineage appeared in the subject: {intruders}. The "
        "parallel-league capture rule is DERIVED from the boundary being NFL-only -- "
        "re-derive it deliberately, do not just widen this set")


def test_no_parallel_league_entry_is_left_as_an_open_question():
    """A declined class must not sit in the queue looking like work. Every parallel
    league names its ruling; none of them is QUEUED."""
    statscrew = CC.CONTRACTS["statscrew"]
    parallel = [e for e in statscrew["toc"]
                if "parallel-league" in e["toc_entry"] or "WFL" in e["toc_entry"]]
    assert parallel, "the parallel-league dispositions vanished"
    for entry in parallel:
        assert entry["status"] == "EXCLUDED", entry
        assert "2026-07-28" in entry["note"], "an exclusion must date its ruling"
