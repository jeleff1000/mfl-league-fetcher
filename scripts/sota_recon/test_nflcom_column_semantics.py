"""O.9.0 gates: nflcom column semantics + the team_stats contamination filter.

Two things are pinned here.

1. THE BLOCK GRAMMAR IS SELF-PROVING. A layout may only be trusted because it REGENERATES the observed
   header sequence exactly. These tests assert the regeneration property still holds and, critically,
   that the resolver REFUSES rather than guesses when no layout (or more than one) fits. If someone
   loosens the resolver into a greedy segmenter, the refusal tests fail.

2. THE special-teams/{kicking,punting} CONTAMINATION STAYS EXCLUDED. NFL.com serves no such page; the
   harvester cached the default PASSING table under those names, so those rows would witness passing
   values as kicking/punting totals. The filter is declared on the source; this measures the duplication
   on disk so the filter can never be dropped while the defect is still real -- and so that if a future
   re-harvest fixes it, the test says so instead of silently keeping a stale exclusion.

Run:  python -m pytest scripts/sota_recon/test_nflcom_column_semantics.py -q
"""
from __future__ import annotations

import glob
import os

import pytest

from . import sources as S
from .nflcom_block_grammar import BLOCKS, LAYOUTS, N, resolve_signature, try_layout

TEAM_STATS_DIR = S.registry()["nflcom_team_stats"].path
_DUPLICATE_FAMILIES = ("special-teams_kicking", "special-teams_punting")
_MASTER = "offense_passing"


# --------------------------------------------------------------------------- block grammar
def test_every_block_is_internally_consistent():
    """headers and semantics are parallel arrays -- a drift here silently shifts every label."""
    for name, headers, semantics in BLOCKS:
        assert len(headers) == len(semantics), f"block {name} misaligned"
        assert len(set(semantics)) == len(semantics), f"block {name} repeats a semantic name"


def test_declared_layouts_reference_real_blocks():
    known = {b[0] for b in BLOCKS}
    for family, layouts in LAYOUTS.items():
        for lname, blocks in layouts.items():
            for b in blocks:
                assert b in known, f"{family}/{lname} references unknown block {b!r}"


CAREER_QB = ["SEASON", "TEAM", "G", "GS", "COMP", "ATT", "YDS", "AVG", "TD", "INT", "SCK", "SCKY",
             "RATE", "ATT", "YDS", "AVG", "TD", "FUM", "LOST"]
CAREER_WRTE = ["SEASON", "TEAM", "G", "GS", "REC", "YDS", "AVG", "LNG", "TD", "ATT", "YDS", "AVG",
               "LNG", "TD", "FUM", "LOST"]
LOG_DEF = ["WK", "Game Date", "OPP", "RESULT", "Total", "Solo", "AST", "SCK", "SFTY", "PDEF", "INT",
           "YDS", "AVG", "LNG", "TDS", "FF", "FR"]


@pytest.mark.parametrize("family,headers,layout", [
    ("player_career", CAREER_QB, "id_career:QB"),
    ("player_career", CAREER_WRTE, "id_career:WRTE"),
    ("player_logs", LOG_DEF, "id_gamelog:DEF_log"),
])
def test_known_signatures_resolve_uniquely(family, headers, layout):
    r = resolve_signature(family, headers)
    assert r["status"] == "RESOLVED", f"{layout}: {r['status']}"
    assert r["layout"] == layout
    assert len(r["semantics"]) == len(headers)


def test_the_polysemy_that_blocked_the_naive_diff():
    """`yds` is a DIFFERENT stat per layout -- the reason a bare column diff was never valid."""
    def sem_for(family, headers, stored_position):
        return resolve_signature(family, headers)["semantics"][stored_position]

    assert sem_for("player_career", CAREER_QB, 6) == "pass_yds"
    assert sem_for("player_career", CAREER_WRTE, 5) == "rec_yds"
    assert sem_for("player_logs", LOG_DEF, 11) == "def_int_ret_yds"


def test_resolver_refuses_instead_of_guessing():
    """A header sequence no layout reproduces must come back UNRESOLVED -- never a best-effort guess."""
    r = resolve_signature("player_career", ["SEASON", "TEAM", "G", "GS", "WIDGETS", "SPROCKETS"])
    assert r["status"] == "UNRESOLVED"
    assert r["semantics"] is None


def test_layout_match_is_exact_not_prefix():
    """A truncated signature must NOT match the layout it is a prefix of."""
    assert try_layout(CAREER_QB[:-2], LAYOUTS["player_career"]["id_career:QB"]) is None


def test_header_normalization_is_case_and_space_insensitive():
    assert N("Net Avg") == N("NET  AVG") == "NET AVG"


def test_quarantined_families_never_receive_labels():
    """A shifted family must not be handed confident semantics -- that would launder the defect."""
    from .nflcom_block_grammar import QUARANTINED_FAMILIES
    for fam in QUARANTINED_FAMILIES:
        r = resolve_signature(fam, ["G", "FUM", "LOST"])
        assert r["status"] == "QUARANTINED"
        assert r["semantics"] is None


def test_named_family_status_is_measured_not_assumed():
    """NAMED is only legitimate while no header repeats; a repeat means _dedup minted a `_2`
    artifact and the self-describing claim is false, so it must fall back to UNRESOLVED."""
    ok = resolve_signature("team_stats", ["Team", "Att", "Cmp", "Pass Yds"])
    assert ok["status"] == "NAMED"
    repeated = resolve_signature("team_stats", ["Team", "Yds", "Yds"])
    assert repeated["status"] == "UNRESOLVED"


# --------------------------------------------------------------------------- label validation
def test_every_identity_targets_columns_the_grammar_actually_assigns():
    """An identity over a column no layout produces would validate nothing."""
    from .nflcom_semantic_validation import IDENTITIES
    for i in IDENTITIES:
        assert i.quotient and i.num and i.den
        assert i.proves, f"{i.caption}/{i.quotient}: an identity must say what it discriminates"


def test_validation_tolerance_matches_display_precision():
    """0.06 is the one-decimal rounding boundary. Tightening it below 0.05 would reject correct
    labels wholesale (measured: 57.93% at 0.02); loosening it far past the cliff would stop
    discriminating. Pinned so nobody 'fixes' a failure by widening the tolerance."""
    from .nflcom_semantic_validation import MIN_AGREE, TOL
    assert 0.05 < TOL <= 0.1
    assert MIN_AGREE >= 0.99


@pytest.mark.skipif(not glob.glob(os.path.join(
    S.registry()["nflcom_player_career"].path, "*.parquet")),
    reason="nflcom player_career parquets not present on this machine")
def test_rbfb_wrte_denominator_swap_is_discriminating():
    """The identities are only evidence because the WRONG reading fails. If a swapped denominator
    also agreed, a passing identity would prove nothing about the layout."""
    import duckdb
    from .nflcom_semantic_validation import _counter_check
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    for caption, c in _counter_check(con).items():
        assert c["declared_agreement"] >= 0.99, caption
        assert c["swapped_agreement"] < 0.10, caption
        assert c["discriminating"], caption


# --------------------------------------------------------------------------- parser shift fix
def test_parser_keeps_blank_headers_aligned_with_their_cells():
    """The root-cause regression. A blank header must be NAMED, never dropped -- dropping it while
    its cell survives is what shifted 7.78M rows one column left."""
    from .nflcom_harvest import parse_all_tables
    html = ("<table><thead><tr><th></th><th>G</th><th>FUM</th></tr></thead>"
            "<tbody><tr><td>Sundays</td><td>12</td><td>3</td></tr></tbody></table>")
    t = parse_all_tables(html)[0]
    assert t["keys"][0].startswith("unnamed_")
    assert t["rows"][0][t["keys"][0]] == "Sundays"
    assert t["rows"][0]["g"] == "12", "games must land in `g`, not the split label"
    assert t["rows"][0]["fum"] == "3", "the last column must survive, not be truncated"


def test_parser_unchanged_where_no_blank_header():
    """The fix must be inert for career/logs/season/team_stats, which have no blank header."""
    from .nflcom_harvest import parse_all_tables
    html = ("<table><thead><tr><th>SEASON</th><th>TEAM</th><th>G</th></tr></thead>"
            "<tbody><tr><td>2000</td><td>NE</td><td>1</td></tr></tbody></table>")
    t = parse_all_tables(html)[0]
    assert t["keys"] == ["season", "team", "g"]
    assert t["rows"][0] == {"season": "2000", "team": "NE", "g": "1"}


# --------------------------------------------------------------------------- contamination filter
def _hash_family(con, stem: str) -> str:
    path = os.path.join(TEAM_STATS_DIR, f"{stem}.parquet").replace("\\", "/")
    cols = [c[0] for c in con.execute(f"SELECT * FROM read_parquet('{path}') LIMIT 0").description]
    data = [c for c in cols if not c.startswith("_") and c not in ("season", "season_type")]
    sel = ",".join(f'"{c}"' for c in data)
    return con.execute(
        f"SELECT md5(string_agg(x,'|' ORDER BY x)) FROM "
        f"(SELECT concat_ws('~',{sel}) x FROM read_parquet('{path}'))").fetchone()[0]


@pytest.mark.skipif(not glob.glob(os.path.join(TEAM_STATS_DIR, "*.parquet")),
                    reason="nflcom team_stats parquets not present on this machine")
def test_special_teams_kicking_and_punting_are_still_duplicate_passing():
    """The defect this filter exists for. If a re-harvest ever fixes it, this test fails LOUD --
    that is the signal to drop the exclusion, not a reason to weaken the test."""
    import duckdb
    con = duckdb.connect()
    master = _hash_family(con, _MASTER)
    for fam in _DUPLICATE_FAMILIES:
        assert _hash_family(con, fam) == master, (
            f"{fam} no longer duplicates {_MASTER} -- the harvest may have been repaired; "
            f"re-adjudicate the nflcom_team_stats source-definition filter")


# --------------------------------------------------------------------------- column-shift quarantine
_SHIFTED = ("nflcom_player_splits", "nflcom_player_situational")
_UNSHIFTED = ("nflcom_player_career", "nflcom_player_logs", "nflcom_team_stats")


@pytest.mark.parametrize("key", _SHIFTED)
def test_shifted_families_are_repointed_at_the_repaired_table(key):
    """FLIPPED 2026-07-28, and only because the repair landed and was receipted.

    This guard used to assert the QUARANTINE was declared on the source -- a 7.8M-row
    misattribution with no declaration being the silent-corruption failure mode. The
    un-shift repoint makes that assertion false, so the guard now holds the other end:
    the source must point at the repaired table, must say what the repair cost, and must
    still NOT be licensed. Deleting it instead would have retired the only thing standing
    between these families and a silent repoint."""
    source = S.registry()[key]
    assert source.path.endswith("_unshifted"), (
        f"{key} is not pointing at the repaired table: {source.path}")
    note = source.note
    assert "REPOINTED" in note and "REPAIRED" in note
    assert "UNRECOVERABLE" in note, (
        "the note must carry what the repair could NOT recover -- eight columns per "
        "family are truncated at parse and left NULL, and a note that omits that invites "
        "someone to read NULL as zero")


@pytest.mark.parametrize("key", _SHIFTED)
def test_the_repair_settles_identity_and_not_licensing(key):
    """THE STAGE BOUNDARY: identity, licensing and mapping are three separate gates.

    The repoint settled IDENTITY and touched nothing about voting. A separate act (Joe's
    2026-07-29 sign-off plus the written receipt) settled LICENSING. A third -- MAPPING,
    the MapSpec that lets a validator resolve a witness through a column -- was owed for a
    further day, which is when these two stages visibly came apart on the same source.

    On 2026-07-30 the mapping was authored, so this family legitimately LEFT
    MAPPING_PENDING. The principle is unchanged and the proxy for it had to move: asserting
    membership in a queue only tests the boundary while the queue happens to be occupied.
    So it is asserted directly instead -- leaving the bucket requires SPECS, not a licence.

    The original assertion ("crosswalk" appears in the note) would have PASSED on a stale
    note that still said BLOCKED ON CROSSWALK, which is exactly the failure Rule C exists
    to catch. That check is kept for whatever remains in the bucket."""
    from .test_mapping_obligation import MAPPING_PENDING
    from .witness_map import WITNESS_MAP

    specs = [m for m in WITNESS_MAP if m.source_key == key]
    in_queue = key in MAPPING_PENDING

    # THE BOUNDARY, stated directly: a source leaves the queue only by having specs, and
    # stays in it only by having none. A licence alone can never move it either way.
    assert bool(specs) != in_queue, (
        f"{key}: {len(specs)} MapSpecs and MAPPING_PENDING={in_queue}. Leaving the queue "
        "requires authored specs; being licensed is not authoring them.")

    if not in_queue:
        # having left, it must carry a real table selector -- the thing that made the
        # authoring possible at all on a source where one column name holds many statistics
        assert all(m.source_table and m.table_col for m in specs), (
            f"{key} left MAPPING_PENDING with specs that do not name their table")
        return

    note = MAPPING_PENDING[key]
    from .refusal_preconditions import _p_no_receipt_licenses_source

    unlicensed, observation = _p_no_receipt_licenses_source({"source": key})
    if not unlicensed:
        assert "BLOCKED on crosswalk" not in note, (
            f"{key} is licensed by a PASS receipt but its queue note still says BLOCKED ON "
            f"CROSSWALK -- a refusal outliving its condition, in the file the condition "
            f"is not in. {observation}")


@pytest.mark.parametrize("key", _UNSHIFTED)
def test_unshifted_families_are_not_quarantined(key):
    """The quarantine must stay scoped to the two families that actually carry the defect."""
    assert "QUARANTINED" not in S.registry()[key].note


@pytest.mark.skipif(not os.path.isdir(S.registry()["nflcom_player_splits"].path),
                    reason="nflcom player_splits parquets not present on this machine")
def test_the_split_label_now_lives_in_its_own_column_and_g_is_a_game_count():
    """THE DEFECT ITSELF, re-measured at rest after the repair.

    This test used to assert that `g` still held the split LABEL ('Sundays', 'Wins') --
    the defect made visible -- and said that when a re-parse repaired it the test would
    fail LOUD as the signal to lift the quarantine. It did exactly that on 2026-07-28.
    Inverted rather than deleted: `g` must now be a game count everywhere, and the label
    must have somewhere of its own to live. If either regresses, the shift is back."""
    import duckdb

    path = os.path.join(S.registry()["nflcom_player_splits"].path,
                        "*.parquet").replace("\\", "/")
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    scan = f"read_parquet('{path}', union_by_name=true)"
    non_numeric = con.execute(
        f"SELECT COUNT(*) FROM {scan} "
        f"WHERE g IS NOT NULL AND TRY_CAST(g AS DOUBLE) IS NULL").fetchone()[0]
    assert non_numeric == 0, (
        f"{non_numeric:,} rows still hold a non-numeric `g` -- the column shift has "
        "returned, or the source is pointing at the unrepaired table")
    labels = {row[0] for row in con.execute(
        f"SELECT DISTINCT split_value FROM {scan} WHERE split_value IS NOT NULL "
        "LIMIT 500").fetchall()}
    assert labels, "split_value is empty -- the label the shift displaced went nowhere"
    assert any(not str(v).replace(".", "").isdigit() for v in labels), (
        "split_value holds only numbers; the split LABEL is what belongs there")


def test_quarantine_is_visible_on_the_scoreboard():
    """Scoreboard law: a new lane/counter enrolls when it is created. A 7.78M-row
    misattribution recorded only in a source note is invisible to the program.

    The counter now reads ZERO because the repair landed -- but it stays enrolled and
    stays LIVE. It is derived from the source notes rather than from a hand-maintained
    list, so a future source that declares itself QUARANTINED raises it without anyone
    remembering to. A counter that only ever reads zero because nothing feeds it is the
    failure this whole session kept finding."""
    from .closure_scoreboard import build
    q = {x["queue"]: x for x in build()["declared_queues"]}
    assert "quarantined_sources" in q, "quarantined_sources not enrolled"
    assert q["quarantined_sources"]["count"] == 0, (
        f"a source is quarantined again: {q['quarantined_sources']['detail']}")
    # the lane still WORKS -- proven by feeding it, not by trusting the zero
    import scripts.sota_recon.sources as _live
    probe = [k for k, s in _live.registry().items() if "QUARANTINED" in s.note]
    assert probe == [], probe


_REPAIR_DIR = os.path.join("D:/league-history-data/nfl/derived/validation/sota_recon_master",
                           "relationship_repair")
_MIRROR_CSV = os.path.join(_REPAIR_DIR, "proposals_mirror_clone.csv")


@pytest.mark.skipif(not os.path.exists(_MIRROR_CSV),
                    reason="repair dry-run proposals not generated on this machine")
def test_mirror_proposal_columns_do_not_read_as_a_merge_pair():
    """A deletion review artifact must not imply an identity claim it is not making.

    The pair is formed by an IDENTICAL STAT LINE, not by identity, so the surviving id may
    be a different human (Ndukwe/A.Jones -- two players who each had exactly 1 INT that
    week). The old `witnessed_name` beside `doom_name` was read as a merge pair in review.
    Under deletion discipline that misreading is a real hazard, so the naming is pinned."""
    import csv
    cols = set(next(iter(csv.DictReader(open(_MIRROR_CSV)))).keys())
    assert "witnessed_name" not in cols and "witnessed_player_week" not in cols
    assert {"matched_line_name", "matched_line_id", "pair_identity_class"} <= cols
    # the decision input is stub-ness of the DOOM id, never sameness of the two humans
    assert "doom_is_bio_dup_identity" not in cols
    assert "doom_is_pfr_keyed_stub_identity" in cols


@pytest.mark.skipif(not os.path.exists(_MIRROR_CSV),
                    reason="repair dry-run proposals not generated on this machine")
def test_row_deletions_only_ever_target_stub_identities():
    """The safety property behind the whole lane: a ROW_DELETE may only drop a pfr-keyed
    DOB-less stub. Deleting a row belonging to an ESTABLISHED identity must be impossible --
    those take CELL_ZERO instead, because a real player's row is never a phantom."""
    import csv
    for r in csv.DictReader(open(_MIRROR_CSV)):
        if r["proposal"] == "ROW_DELETE_CLONE":
            assert r["pair_identity_class"] == "DOOM_IS_PFR_KEYED_STUB", r["doom_id"]
        else:
            assert r["proposal"] == "CELL_ZERO_DOUBLE_CREDIT"
            assert r["pair_identity_class"] == "BOTH_ESTABLISHED_IDENTITIES", r["doom_id"]
            assert "REVIEW WITH CARE" in r["witness_basis"]


def test_team_stats_source_declares_the_exclusion_filter():
    """A measured contamination with no declared filter is exactly the silent-drop failure mode."""
    note = S.registry()["nflcom_team_stats"].note
    assert "SOURCE-DEFINITION FILTER" in note
    for fam in ("kicking", "punting"):
        assert fam in note
    assert "special-teams" in note


# --------------------------------------------------------------------------- O.9.0b un-shift
def test_unshift_layouts_are_uniquely_determined():
    """The algebra is only valid because a row's populated key-SET picks exactly one column
    ORDER. If a future census ever maps one key-set to two orders, layouts_for must refuse --
    guessing the order would silently mis-assign every stat in that layout."""
    from .build_nflcom_splits_unshift import layouts_for
    for fam in ("player_splits", "player_situational"):
        lays = layouts_for(fam)
        sets = [frozenset(L) for L in lays]
        assert len(sets) == len(set(sets)), f"{fam}: duplicate key-set survived"
        assert len(lays) == 8, f"{fam}: expected 8 layouts, got {len(lays)}"


def test_unshift_receipt_is_fully_classified_and_holdout_clean():
    """Two properties the repair stands on: every source row was classified into a layout
    (nothing guessed, nothing dropped), and the algebra reproduces an INDEPENDENT re-parse of
    the retained cache bytes exactly on every recoverable column."""
    import json as _json
    p = os.path.join(os.path.dirname(__file__), "..", "..", "docs",
                     "nflcom-splits-unshift-receipt.json")
    if not os.path.exists(p):
        pytest.skip("un-shift receipt not generated on this machine")
    rec = _json.load(open(p))
    for fam, f in rec["families"].items():
        assert f["rows_unclassified"] == 0, f"{fam}: {f['rows_unclassified']} rows unclassified"
        assert f["rows_unshifted"] == f["rows_source"], fam
        h = f.get("holdout")
        if h:
            assert h["worst_recoverable_column_agreement"] >= 0.9999, (fam, h["worst_column"])
            assert h["overlap_rows"] > 1000, fam
