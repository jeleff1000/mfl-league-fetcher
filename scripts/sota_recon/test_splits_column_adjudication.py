"""The splits + situational burn-down: 1,175 decisions whose answers all LOOK right.

That is what makes this block dangerous and why these tests are about where the evidence
CAME FROM rather than whether the mappings are plausible. Reading `COMP`/`RATE` and
concluding "passing" gives the same answer as measuring it -- right up until L4, where the
plausible reading is wrong in a way no amount of staring reveals: NFL.com renders ONE
header set under both <h3>Kick Return</h3> and <h3>Punt Return</h3>.

The 2026-07-28 handoff measured the shortcut and refused it in advance (matching these
column sets against O.9.0's RESOLVED gamelog blocks matched 0 of 8). These tests keep that
refusal enforced.

Run:  python -m pytest scripts/sota_recon/test_splits_column_adjudication.py -q
"""

from __future__ import annotations

import json

import pytest

from . import nflcom_column_adjudication as NFLCOM
from . import nflcom_splits_column_adjudication as SPLITS
from . import nflcom_splits_layout_identity as IDENTITY
from . import statscrew_column_adjudication as STATSCREW
from .column_dossier import load_decisions


def _receipts() -> tuple[dict, dict]:
    if not (SPLITS.BLOCK_CENSUS.exists() and SPLITS.LAYOUT_IDENTITY.exists()):
        pytest.skip("splits block census / identity sweep not built in this tree")
    return (json.loads(SPLITS.BLOCK_CENSUS.read_text(encoding="utf-8")),
            json.loads(SPLITS.LAYOUT_IDENTITY.read_text(encoding="utf-8")))


def test_the_block_names_are_the_SITES_names_not_ours() -> None:
    """Every layout's stat family must be confirmed by NFL.com's own <h3>, in BOTH
    families independently -- splits_L3 and situational_L3 are two separate measurements
    that happen to agree, never one assumption applied twice."""
    census, _ = _receipts()
    for suffix, declared in SPLITS.BLOCKS.items():
        if declared is None:
            continue
        for family in SPLITS.SOURCES.values():
            record = census["layouts"][f"{family}_{suffix}"]
            assert record["status"] == "RESOLVED", (
                f"{family}_{suffix}: {record['sections']}")
            assert record["section"] == declared, (
                f"{family}_{suffix}: we say {declared!r}, the site says "
                f"{record['section']!r}")


def test_the_block_census_checked_its_own_structural_reading() -> None:
    """The section rule knows no vocabulary -- it is 'an <h3> followed by another <h3>
    before any <table> is a section'. What makes that trustworthy is that the captions it
    derives must be exactly the split axes the stored table carries, and the section names
    must be disjoint from them. A mis-segmented page would make those two sets overlap."""
    census, _ = _receipts()
    for family, captions in census["captions_observed"].items():
        sections = set(census["sections_observed"][family])
        overlap = sections & set(captions)
        assert not overlap, (
            f"{family}: {sorted(overlap)} read as both a stat family and a split axis -- "
            "the structural rule mis-segmented the page")
        assert "" not in sections, f"{family}: a table with no section heading above it"


def test_the_census_declares_that_it_sampled_and_how() -> None:
    """100% agreement on an uncharacterised sample proves nothing -- the lesson the
    un-shift holdout was re-measured for on 2026-07-28. Reading every retained page is
    ~30 h wall on this disk, so the run samples; what it may not do is sample quietly.
    Population, size, seed and season skew are all in the receipt, and every layout the
    adjudication keys on must actually have been reached."""
    census, _ = _receipts()
    sampling = census["sampling"]
    assert sampling["sampled"] > 0 and sampling["eligible_cache_pages"] > 0
    assert sampling["seed"] is not None, "an undeclared seed is not reproducible"
    assert sampling["worst_season_share_skew_points"] < 5.0, sampling
    assert census["layouts_declared_but_unobserved"] == [], (
        "a declared layout was never reached by the sample -- its identity is undeclared, "
        "not decided")
    for suffix in SPLITS.BLOCKS:
        for family in SPLITS.SOURCES.values():
            record = census["layouts"][f"{family}_{suffix}"]
            assert record["tables"] >= 10, (
                f"{family}_{suffix}: only {record['tables']} observations -- too few for "
                "'no competing section name' to mean anything")


def test_the_ambiguous_return_block_is_escalated_and_never_mapped() -> None:
    """L4's header set is rendered under BOTH <h3>Kick Return</h3> and <h3>Punt
    Return</h3>, and the O.9.0b un-shift keys a row's layout by its COLUMN SET -- so the
    stored table cannot tell them apart. Every stat column there takes a different
    canonical under each reading. Mapping one would file an unknown share of 811,572 rows
    under the wrong return family, and it would look entirely reasonable in the ledger."""
    census, _ = _receipts()
    for family in SPLITS.SOURCES.values():
        record = census["layouts"][f"{family}_L4"]
        assert record["status"] == "AMBIGUOUS" and len(record["sections"]) > 1, (
            f"{family}_L4 resolved to one section -- the escalation is now WRONG and its "
            "seven columns are decidable; settle them rather than leaving them open")
    for column, (kick, punt) in SPLITS._L4_PAIRS.items():
        assert kick != punt, f"L4.{column}: the two readings must differ to be ambiguous"
        if column == "yds":
            assert SPLITS.MAPPED[("L4", column)] == "total_return_yards"
            continue
        assert ("L4", column) not in SPLITS.MAPPED, f"L4.{column} mapped while ambiguous"
        assert ("L4", column) not in SPLITS.NEW_CANDIDATES

    decisions, _ = SPLITS.build_decisions()
    for entry in decisions:
        _source, table_key, column = entry["key"].split("|", 2)
        if table_key.endswith("_L4") and column in SPLITS._L4_PAIRS and column != "yds":
            raise AssertionError(
                f"an L4 stat column was decided: {entry['key']} -> {entry['disposition']}")


def test_l4_yards_uses_the_existing_combined_return_canonical() -> None:
    assert SPLITS.MAPPED[("L4", "yds")] == "total_return_yards"
    decisions, _ = SPLITS.build_decisions()
    rows = [row for row in decisions if "_L4|yds" in row["key"]]
    assert rows
    assert {row["disposition"] for row in rows} == {"MAPPED_TO_CANONICAL"}
    assert {row["canonical"] for row in rows} == {"total_return_yards"}


def test_the_derived_ratios_are_MEASURED_against_every_rival() -> None:
    """'Its operands are in the same block' is an arithmetic claim. The sweep scores it
    against every other ordered pair in the layout, so the MARGIN is the receipt -- a
    one-sided match proves nothing."""
    _census, identity = _receipts()
    for (suffix, column), (numerator, denominator) in SPLITS.DERIVED_RATIOS.items():
        for family in SPLITS.SOURCES.values():
            record = identity["layouts"][f"{family}_{suffix}"]["identities"][column]
            assert record["status"] == "RESOLVED", f"{family}_{suffix}.{column}: {record}"
            best = record["best"]
            assert (best["numerator"], best["denominator"]) == (numerator, denominator), (
                f"{family}_{suffix}.{column}: declared {numerator}/{denominator}, "
                f"measured {best['numerator']}/{best['denominator']}")
            margin = record["margin_points"]
            assert margin is None or margin >= 20.0, (
                f"{family}_{suffix}.{column}: margin {margin} -- a rival equation is "
                "nearly as good, so the arithmetic does not identify this column")


def test_a_zero_rate_is_not_counted_as_agreement() -> None:
    """The first version of the sweep counted empty split rows, where 0 = 0/x holds for
    EVERY x. That made the defense block's `avg` score 99.95% for yds/int and 96.47% for
    yds/sfty -- a rival reading conjured out of mass agreement on nothing, which reported
    WEAK and looked like a real dispute. Relaxing eligibility back would make every layout
    pass for the wrong reason."""
    assert IDENTITY.NONDEGENERATE, (
        "degenerate rows re-admitted -- a zero rate agrees with every denominator")


def test_the_composite_field_goal_cells_are_escalated_not_forced() -> None:
    """One cell, two facts: '3-3' is made AND attempted, on 100% of 119,994 non-null
    values. No disposition in the vocabulary fits -- MAPPED names one target and drops the
    attempts, EXCLUDED discards the only place NFL.com publishes per-distance field goals,
    NEW proposes storing the string. This is the SECOND instance of the gap StatsCrew's
    `results.game` opened, and two lineages make it a vocabulary question rather than a
    quirk of one source, so neither may be quietly closed without the other."""
    for column in SPLITS._BUCKETS:
        assert ("L7", column) not in SPLITS.MAPPED, (
            f"L7.{column} is a made-att composite and cannot map to one canonical")
        assert ("L7", column) not in SPLITS.NEW_CANDIDATES
    assert ("results", "game") in STATSCREW.ESCALATED, (
        "the sibling escalation disappeared -- these two are one question")


def test_the_tackle_total_is_refused_by_every_lineage_that_raises_it() -> None:
    """Three sources ask the same question independently: nflcom player_career
    (TKL|AST|COMBINED|SOLO), StatsCrew ('Total Tackles', where tackle = solo + ast holds
    on 34.6% of rows), and now the nflcom DEFENSE splits block. That is evidence about the
    CONCEPT, not about any one parse -- so if one pass ever answers it, the other two must
    not still be refusing."""
    assert ("L0", "total") in SPLITS.ESCALATED
    assert "def_tackles" in NFLCOM.ESCALATED
    assert ("defense_and_fumbles", "tackle") in STATSCREW.ESCALATED
    for question in SPLITS.ESCALATED.values():
        assert "SETTLED BY" in question, "an escalation must say what would settle it"


def test_the_punter_side_columns_are_not_credited_to_the_returner() -> None:
    """RET and RETY sit INSIDE the Punting block, so they are returns ALLOWED. O.9.0's
    gamelog pass legitimately maps those same names to punt_returns / punt_return_yards
    from a RETURNS block -- so the hazard here is a correct mapping applied to the wrong
    block, which is the least visible kind."""
    for column in ("ret", "rety"):
        assert ("L6", column) in SPLITS.NEW_CANDIDATES
        assert ("L6", column) not in SPLITS.MAPPED
    assert SPLITS.MAPPED.get(("L6", "punts")) == "punts"


def test_the_pass_reaches_a_decision_on_everything_it_can() -> None:
    """An unhandled column is left OPEN with nobody having looked at it -- indistinguishable
    in the queue from one nobody has reached yet."""
    _decisions, tally = SPLITS.build_decisions()
    assert tally["unhandled"] == [], tally["unhandled"]
    assert tally["escalated_left_open"] > 0


def test_the_correspondence_table_survives_its_own_receipts() -> None:
    """`_validate` is the module's gate: block declarations against the census, every
    derived ratio against the sweep, every canonical against v26, one column one bucket.
    It runs before --apply writes anything."""
    if not (SPLITS.BLOCK_CENSUS.exists() and SPLITS.LAYOUT_IDENTITY.exists()):
        pytest.skip("splits receipts not built in this tree")
    assert SPLITS._validate(NFLCOM._v26_columns()) == []


def test_every_decided_column_cites_the_SITES_own_header_string() -> None:
    """The stored key is OUR slug and the site's label is not the same string: `1st_2` is
    '1st%', `20` is '20+', `in_20` is 'IN 20', `1_19` is '1-19'. Citing our normalisation
    back to ourselves would be the same move as reading a stat off its abbreviation, which
    StatsCrew already proved wrong twice. Anything decided without a published header can
    only be one of our parser's own emissions."""
    headers = SPLITS.published_headers()
    for bucket in (SPLITS.MAPPED, SPLITS.NEW_CANDIDATES, SPLITS.DERIVED_RATIOS):
        for suffix, column in bucket:
            assert (suffix, column) in headers, f"{suffix}.{column} has no site header"
    assert headers[("L2", "1st_2")] == "1st%"
    assert headers[("L2", "20")] == "20+"
    assert headers[("L6", "in_20")] == "IN 20"
    assert headers[("L7", "1_19")] == "1-19"
    # our own emissions are the only things excluded without one
    parser_emitted = {"_view", "_lost_column", "split_value"}
    for suffix, column in SPLITS.EXCLUDED:
        if (suffix, column) not in headers and column not in parser_emitted:
            assert column == "g", f"{suffix}.{column} excluded with no header and no rule"


def test_the_committed_splits_decisions_all_cite_the_block_census() -> None:
    """Evidence must be the SOURCE'S own published name wherever the source publishes one.
    For these families that is the <h3>, so every committed decision must cite it."""
    committed = {key: entry for key, entry in load_decisions().items()
                 if entry.get("generated_by") == SPLITS.GENERATOR}
    if not committed:
        pytest.skip("the splits pass has not been applied in this tree")
    for key, entry in committed.items():
        assert "nflcom-splits-block-census.json" in (entry.get("evidence") or ""), key
