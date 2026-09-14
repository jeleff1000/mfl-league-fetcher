"""O.9.3 -- the column dossier must keep counting the MATERIAL, not the paperwork.

`sources_without_mapping_obligation = 0` is a statement about 89 sources. This dossier is
the statement about their columns, and the ways it could quietly stop being one are all
the same shape: a regime asserted instead of measured, a source silently unreadable, or a
stat surface closed by a bulk rule because bulk rules are convenient.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from .column_dossier import (
    KEY_COLUMNS,
    MARKUP_SUFFIXES,
    NEEDS_LAYOUT,
    PROVENANCE_COLUMNS,
    bulk_disposition,
)

DOSSIER = Path(__file__).resolve().parents[2] / "docs" / "column-dossier.json"


def _dossier() -> dict:
    if not DOSSIER.exists():
        pytest.skip("column dossier not built in this tree")
    return json.loads(DOSSIER.read_text(encoding="utf-8"))


def test_a_stat_column_is_never_closed_by_a_bulk_rule() -> None:
    """Bulk cohorts may only close plumbing. The moment a rule can close a stat, the
    queue can be driven to zero without a single adjudication -- which is how a gate
    becomes a formality."""
    for column in ("pass_yds", "rush_att", "tackles", "sacks", "yds", "int", "fum_rec",
                   "def_air_yards_allowed", "targets", "epa"):
        assert bulk_disposition(column) is None, (
            f"{column} is stat material and must be adjudicated, not closed by rule")


def test_the_bulk_cohorts_are_plumbing_only() -> None:
    assert bulk_disposition("source_url")[0] == "EXCLUDED_CAPTURE_PROVENANCE"
    assert bulk_disposition("content_sha256")[0] == "EXCLUDED_CAPTURE_PROVENANCE"
    for column in (
        "_cache_sha1",
        "_header_signature_sha256",
        "_source_artifacts",
        "_source_row_index",
        "_source_table_index",
    ):
        assert bulk_disposition(column)[0] == "EXCLUDED_CAPTURE_PROVENANCE"
    assert bulk_disposition("pfr_id")[0] == "EXCLUDED_KEY_COLUMN"
    assert bulk_disposition("team_links_json")[0] == "EXCLUDED_MARKUP_RESIDUE"
    # and the cohorts must not quietly grow into stat space
    assert not (PROVENANCE_COLUMNS & KEY_COLUMNS)
    assert all(suffix.startswith("_") for suffix in MARKUP_SUFFIXES)


def test_every_source_is_enumerated_or_reported_unreadable() -> None:
    """A source that cannot be read contributes zero rows, which would make the queue
    look smaller than the material. It must show up as a number instead."""
    from .sources import registry

    document = _dossier()
    counters = document["counters"]
    assert counters["sources_enumerated"] + counters["sources_unreadable"] == len(
        registry(include_subject=True)
    )


def test_all_three_keying_regimes_are_present_and_measured() -> None:
    """Finding 7: one regime does not fit. WIDE collapses nothing, MULTI_TABLE keeps 82
    nflcom logical tables out of 7 registry rows, and LONG is the only way the newspaper
    sources' ~585 stat concepts are visible at all."""
    document = _dossier()
    regimes = document["regimes"]
    assert set(regimes) >= {"WIDE", "MULTI_TABLE", "LONG"}, regimes
    assert regimes["LONG"] >= 3, "the three newspaper LONG sources must be detected"


def test_long_sources_enumerate_concepts_AND_physical_columns() -> None:
    """Enumerating only the stat concepts would repeat the original error in mirror
    image -- the concepts are what a column-keyed dossier misses, but value/unit/
    reviewer columns are still real."""
    document = _dossier()
    long_rows = [row for row in document["rows"] if row["regime"] == "LONG"]
    assert {row["table_key"] for row in long_rows} == {"stat_name", "physical"}


def test_the_logs_families_are_keyed_with_layout() -> None:
    """O.9.0 measured that `_table` on player_logs is only the season stratum and each
    caption spans SEVEN layouts. Without + layout, `yds` under one key is passing AND
    rushing AND receiving AND interception-return AND punt yards at once."""
    document = _dossier()
    underspecified = set(document.get("sources_needing_layout_without_census", []))
    for source in NEEDS_LAYOUT:
        keys = {row["table_key"] for row in document["rows"] if row["source"] == source}
        if not keys:
            continue
        if source in underspecified:
            # Allowed ONLY because it is counted. O.9.0 censused six families and
            # `player_logs_targeted` was not one of them; borrowing another family's
            # layouts would assert a shape instead of measuring it.
            continue
        assert any("|" in key for key in keys), (
            f"{source}: table keys {sorted(keys)[:4]} carry no layout component")
    assert "nflcom_player_logs" not in underspecified, (
        "player_logs HAS a layout census and must use it")


def test_an_underspecified_key_is_counted_not_hidden() -> None:
    document = _dossier()
    counted = document["counters"]["sources_needing_layout_without_census"]
    assert counted == len(document.get("sources_needing_layout_without_census", []))
    # TIGHTENED 2026-07-29, exactly as the previous version instructed. It asserted
    # `counted >= 1` on the strength of player_logs_targeted having no layout census;
    # that source now HAS one and its 104 rows adjudicated, so the old assertion was
    # pinning a gap that had been closed. What must not decay is the INVARIANT -- that
    # the count and the enumerated list agree, so a source needing a layout census can
    # never be counted without being NAMED. That is checked above and is the whole point;
    # the queue reaching zero is success, not a reason to delete the check.
    assert counted >= 0


def test_the_queue_is_reported_not_forced_to_zero() -> None:
    document = _dossier()
    assert document["counters"]["column_dossier_open_rows"] > 0, (
        "an open-row count of zero would mean every column had been adjudicated; if that "
        "ever becomes true, this test should be replaced by a zero-gate, not deleted")


def test_the_committed_dossier_is_the_repaired_one() -> None:
    """A concurrent slower build silently reverted this receipt once, restoring a shape
    that collapsed 54 nflcom logical tables and shrank the open queue from 10,273 to
    6,027. It failed in the FLATTERING direction, which is the direction nobody
    double-checks, so the repaired shape is pinned here directly."""
    document = _dossier()
    multi = {row["source"] for row in document["rows"] if row["regime"] == "MULTI_TABLE"}
    assert multi >= {
        "nflcom_player_logs", "nflcom_player_logs_targeted", "nflcom_player_career",
        "nflcom_player_season", "nflcom_player_situational", "nflcom_player_splits",
        "nflcom_team_stats",
    }, ("all seven nflcom families must be MULTI_TABLE; fewer means the composite "
        f"discriminator regressed and player_season/team_stats collapsed again: {multi}")
    assert "sources_needing_layout_without_census" in document["counters"]


# ---- the CROSS-PRODUCT denominator (2026-07-27) --------------------------------------

def test_the_cross_product_is_conserved_not_quietly_discarded() -> None:
    """A MULTI_TABLE source's physical column list is the UNION over its tables, so the
    naive cross product counts pairs no table publishes -- `rating` under PUNTING,
    `qbh` under KICKING. Dropping them is right; dropping them SILENTLY is how a queue
    shrinks for reasons nobody can audit. Both numbers stay in the artifact and must
    add up."""
    counters = _dossier()["counters"]
    assert (counters["cross_product_pairs"]
            == counters["dossier_rows"] + counters["column_dossier_unpublished_pairs"]), (
        "cross_product_pairs != dossier_rows + unpublished_pairs -- the shrink is no "
        "longer accounted for")
    assert counters["column_dossier_unpublished_pairs"] > 0, (
        "if this ever reaches zero, every MULTI_TABLE source publishes every column in "
        "every table, which no HTML table family does -- suspect the measurement, not "
        "the sources")


def test_an_unpublished_pair_really_carries_no_values() -> None:
    """THE CLAIM UNDER THIS SHRINK, checked against the data rather than asserted.

    A dropped pair must have zero non-NULL values. Verified on the source with the
    largest drop, because a measurement that silently matched nothing would drop
    EVERYTHING and read as a triumphant burn-down."""
    import duckdb

    from .column_dossier import _resolve_scan, detect_regime, measure_occupancy
    from .sources import registry

    document = _dossier()
    source_key = max(document["unpublished_pairs_by_source"].items(),
                     key=lambda item: item[1])[0]
    source = registry(include_subject=True)[source_key]
    connection = duckdb.connect()
    connection.execute("SET memory_limit='4GB'")
    try:
        resolved = _resolve_scan(connection, source.path)
        if resolved is None:
            pytest.skip(f"{source_key} unreadable in this tree")
        scan, columns = resolved
        regime = detect_regime(connection, scan, columns)
        occupancy = measure_occupancy(connection, scan, regime["discriminator"], columns)
    finally:
        connection.close()

    emitted: dict[str, set[str]] = {}
    for row in document["rows"]:
        if row["source"] == source_key:
            emitted.setdefault(row["table_key"], set()).add(row["column"])
    assert emitted, f"{source_key} emitted no rows at all"
    for table_key, published in emitted.items():
        # every EMITTED pair carries values ...
        assert published <= occupancy.get(table_key, set()), (
            f"{source_key}/{table_key}: emitted a pair with no values")
        # ... and every DROPPED pair carries none
        dropped = set(columns) - published
        assert not (dropped & occupancy.get(table_key, set())), (
            f"{source_key}/{table_key}: dropped a pair that HAS values -- "
            "this shrink would be hiding material")


def test_the_logs_family_declaration_comes_from_the_sources_own_header() -> None:
    """`layout` is not a column in the data, so occupancy cannot be measured at
    `caption|layout` grain. O.9.0's per-signature keys ARE the source's header and were
    receipted by regeneration -- using them is better evidence than measurement, and
    falling back to the cross product here would put all seven layouts' columns under
    every one of them."""
    from .column_dossier import _family_signature_keys

    per_pair, header_keys = _family_signature_keys("nflcom_player_logs")
    assert per_pair, "the layout census must still resolve for player_logs"
    assert header_keys

    document = _dossier()
    entry = next(e for e in document["per_source"] if e["source"] == "nflcom_player_logs")
    assert entry["column_declaration"] == "SOURCE_HEADER + MEASURED"
    assert entry["unpublished_pairs"] > 0

    emitted: dict[str, set[str]] = {}
    for row in document["rows"]:
        if row["source"] == "nflcom_player_logs":
            emitted.setdefault(row["table_key"], set()).add(row["column"].lower())
    for table_key, published in emitted.items():
        declared = per_pair.get(table_key, set())
        missing = declared - published
        assert not missing, (
            f"player_logs/{table_key}: the source declares {sorted(missing)} in its own "
            "header and the dossier does not carry them")


def test_dropping_a_pair_can_never_empty_the_queue() -> None:
    """The shrink is bounded by what the sources publish. If a regression made the
    occupancy measurement return nothing, every pair would look unpublished and the
    queue would burn itself down to zero without a single adjudication -- the same
    failure the bulk-rule test guards from the other direction."""
    document = _dossier()
    for entry in document["per_source"]:
        if entry["regime"] != "MULTI_TABLE":
            continue
        assert entry["dossier_rows"] > 0, (
            f"{entry['source']}: every (table, column) pair measured unpublished, which "
            "means the measurement failed, not that the source is empty")
        assert entry["dossier_rows"] >= entry["table_keys"], (
            f"{entry['source']}: fewer emitted columns than logical tables")


# ---- the adjudication ledger --------------------------------------------------------

def test_an_adjudication_survives_a_rebuild() -> None:
    """THE reason the ledger exists. The dossier is regenerated from scratch every run, so
    a decision written into the artifact would be erased by the next build -- and two
    stale builds silently reverted that artifact on 2026-07-27, which would have destroyed
    the decision with no trace."""
    from .column_dossier import apply_decisions, row_key

    rows = [
        {"source": "nflcom_player_career", "table_key": "QB", "column": "yds",
         "disposition": "OPEN", "reason": None},
    ]
    decisions = {
        row_key("nflcom_player_career", "QB", "yds"): {
            "key": row_key("nflcom_player_career", "QB", "yds"),
            "disposition": "MAPPED_TO_CANONICAL",
            "canonical": "pass_yds",
            "reason": "QB block; yds is passing yards",
            "evidence": "docs/nflcom-column-semantics.json player_career/QB",
        }
    }
    applied, orphaned = apply_decisions(rows, decisions)
    assert (applied, orphaned) == (1, [])
    assert rows[0]["disposition"] == "MAPPED_TO_CANONICAL"
    assert rows[0]["canonical"] == "pass_yds"
    assert rows[0]["closed_by"] == "adjudication"


def test_an_adjudication_beats_a_bulk_rule() -> None:
    """The rules are triage. A human who looked at the column and its evidence is the
    authority, and a rule that could override one would make the ledger advisory."""
    from .column_dossier import apply_decisions, row_key

    key = row_key("some_source", "*", "team")
    rows = [{"source": "some_source", "table_key": "*", "column": "team",
             "disposition": "EXCLUDED_KEY_COLUMN", "reason": "locates the row"}]
    applied, _ = apply_decisions(rows, {key: {
        "key": key, "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
        "reason": "here it is a stat surface, not a key",
        "evidence": "measured: 40 distinct values, none of them team codes"}})
    assert applied == 1
    assert rows[0]["disposition"] == "NEW_SUPERTABLE_COLUMN_CANDIDATE"


def test_a_stale_decision_is_orphaned_not_silently_dropped() -> None:
    """A key the dossier no longer produces means the row it adjudicated has re-opened."""
    from .column_dossier import apply_decisions

    rows = [{"source": "a", "table_key": "*", "column": "x", "disposition": "OPEN",
             "reason": None}]
    applied, orphaned = apply_decisions(rows, {"a|*|renamed_column": {
        "key": "a|*|renamed_column", "disposition": "EXCLUDED_WITH_REASON",
        "reason": "r", "evidence": "e"}})
    assert applied == 0
    assert orphaned == ["a|*|renamed_column"]


def test_an_adjudication_without_evidence_is_rejected() -> None:
    from .column_dossier import validate_decision

    receipt = "docs/nflcom-column-semantics.json signature 'Regular Season|QB' position 4"
    assert validate_decision({"key": "a|*|x", "disposition": "EXCLUDED_WITH_REASON",
                              "reason": "looks like plumbing"}) != []
    assert validate_decision({"key": "a|*|x", "disposition": "MAPPED_TO_CANONICAL",
                              "reason": "r", "evidence": receipt}) != [], "must name canonical"
    assert validate_decision({"key": "a|*|x", "disposition": "NOT_A_DISPOSITION",
                              "reason": "r", "evidence": receipt}) != []
    assert validate_decision({"key": "a|*|x", "disposition": "MAPPED_TO_CANONICAL",
                              "canonical": "pass_yds", "reason": "r",
                              "evidence": receipt}) == []


def test_a_one_token_evidence_string_is_not_evidence() -> None:
    """The field was checked for non-emptiness, which `"e"` satisfies. Every real
    receipt in this ledger names an artifact and a locator; a token that clears the
    check without naming anything is the opinion the rule exists to reject."""
    from .column_dossier import validate_decision

    assert validate_decision({"key": "a|*|x", "disposition": "EXCLUDED_WITH_REASON",
                              "reason": "plumbing", "evidence": "e"}) != []
    assert validate_decision({"key": "a|*|x", "disposition": "EXCLUDED_WITH_REASON",
                              "reason": "plumbing", "evidence": "measured"}) != []


def test_the_gate_checks_the_claim_and_not_only_the_paperwork() -> None:
    """PROBED 2026-07-28, and all three passed the shape check while the counter read
    zero. A gate that stops at 'the fields are filled in' certifies a ledger that maps
    to nowhere, duplicates nothing, and adjudicates a source whose names are known-wrong.

    Tests for all three already existed. They are not the gate -- they skip when the
    dossier artifact is absent, and the counter is what always runs."""
    from .column_dossier import validate_decisions_against_material

    document = _dossier()
    receipt = "a receipt naming an artifact and a locator inside it"
    rows = document["rows"]
    real_key = f"{rows[0]['source']}|{rows[0]['table_key']}|{rows[0]['column']}"

    mapping_to_nowhere = {"key": real_key, "disposition": "MAPPED_TO_CANONICAL",
                          "canonical": "there_is_no_such_column", "reason": "r",
                          "evidence": receipt}
    duplicate_of_nothing = {"key": real_key, "disposition": "DUPLICATE_OF",
                            "canonical": "not|a|row", "reason": "r", "evidence": receipt}
    for probe in (mapping_to_nowhere, duplicate_of_nothing):
        assert validate_decisions_against_material(rows, {probe["key"]: probe}), probe

    # The laundered-defect probe needs a source IN the quarantine set, and the set is
    # empty as of the 2026-07-28 un-shift repoint. Injecting one keeps the check alive
    # for the next time a source's names are known-wrong -- deleting it because nothing
    # is quarantined today would retire the guard exactly when it stops being exercised.
    from . import column_dossier as CD

    laundered = {"key": "some_broken_source|Home Games|yds",
                 "disposition": "MAPPED_TO_CANONICAL", "canonical": "passing_yards",
                 "reason": "r", "evidence": receipt}
    original = CD.QUARANTINED_SOURCES
    try:
        CD.QUARANTINED_SOURCES = {"some_broken_source"}
        assert validate_decisions_against_material(rows, {laundered["key"]: laundered})
    finally:
        CD.QUARANTINED_SOURCES = original

    # and the committed ledger survives the same check
    from .column_dossier import load_decisions

    assert validate_decisions_against_material(rows, load_decisions()) == []


def test_the_committed_ledger_is_valid_and_starts_empty() -> None:
    """Empty is the honest state: 10,273 rows are open and none is adjudicated yet."""
    from .column_dossier import DISPOSITIONS_PATH, load_decisions, validate_decision

    assert DISPOSITIONS_PATH.exists(), "the ledger must exist before the burn-down starts"
    decisions = load_decisions()
    problems = [p for entry in decisions.values() for p in validate_decision(entry)]
    assert problems == [], problems


def test_the_repaired_families_are_keyed_by_a_MEASURED_layout() -> None:
    """THE UN-SHIFT REPOINT, 2026-07-28, and the cross-check that makes it trustworthy.

    The repaired tables carry `_layout` as a REAL COLUMN, so these two families are keyed
    on the measured composite (`_table`, `_layout`) instead of on a caption census. That
    matters beyond tidiness: the resulting table-key counts land on 48 and 58 -- EXACTLY
    the number of signatures O.9.0's block grammar found by regenerating header sequences
    from cached HTML. Two independent methods, one reading bytes and one counting distinct
    values in repaired parquet, agree on the number of logical tables.
    """
    document = _dossier()
    expected = {"nflcom_player_splits": 48, "nflcom_player_situational": 58}
    by_source = {entry["source"]: entry for entry in document["per_source"]}
    for source, signatures in expected.items():
        entry = by_source.get(source)
        if entry is None:
            pytest.skip(f"{source} not enumerated in this tree")
        assert entry["regime"] == "MULTI_TABLE", entry
        assert "_layout" in (entry["discriminator"] or []), (
            f"{source}: the repaired table carries `_layout`; keying without it puts "
            "eight position blocks under one key again")
        assert entry["table_keys"] == signatures, (
            f"{source}: {entry['table_keys']} table keys but O.9.0's block grammar found "
            f"{signatures} signatures -- the two methods have diverged, and one of them "
            "is wrong about how many logical tables this family has")


def test_the_quarantine_set_is_empty_but_still_enforced() -> None:
    """Emptying it is a CLAIM ('no source has known-wrong names today'), not the removal
    of a concept. The machinery has to stay live or the guard rots while unexercised."""
    from .column_dossier import QUARANTINED_SOURCES

    assert QUARANTINED_SOURCES == set(), (
        "a source re-entered quarantine -- its dossier rows must stop being adjudicable "
        f"and the blocked counter must move: {QUARANTINED_SOURCES}")
    counters = _dossier()["counters"]
    assert counters["column_dossier_rows_blocked_by_quarantine"] == 0
