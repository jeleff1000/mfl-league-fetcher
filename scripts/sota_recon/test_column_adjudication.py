"""The O.9.3 burn-down passes: 656 decisions that must not be able to go wrong quietly.

Two generators write into one ledger. Every way they could turn a burn-down into a
laundering operation is a test here:

  - a canonical that does not exist (the ledger's own validator only checks the field is
    non-empty, so a mapping to nowhere would pass it)
  - a decision whose key no dossier row produces (counted as orphaned, but a generator
    should never create one)
  - two signatures disagreeing about one row, last-write-wins
  - a QUARANTINED family adjudicated as if its stored names meant anything
  - the escalated label questions quietly acquiring a mapping
  - a hand decision reverted by re-running a generator

Run:  python -m pytest scripts/sota_recon/test_column_adjudication.py -q
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from . import nflcom_column_adjudication as NFLCOM
from . import nflcom_category_column_adjudication as CATEGORY
from . import internal_column_adjudication as INTERNAL
from . import newspaper_sidecar_column_adjudication as NEWSPAPER_SIDECAR
from . import pbp_column_adjudication as PBP
from . import pfr_datastat_column_adjudication as PFR
from . import statscrew_column_adjudication as STATSCREW
from .column_dossier import DISPOSITIONS_PATH, load_decisions, row_key, validate_decision

DOSSIER = Path(__file__).resolve().parents[2] / "docs" / "column-dossier.json"


def _dossier() -> dict:
    if not DOSSIER.exists():
        pytest.skip("column dossier not built in this tree")
    return json.loads(DOSSIER.read_text(encoding="utf-8"))


def test_every_ledger_decision_is_valid() -> None:
    problems = [p for entry in load_decisions().values() for p in validate_decision(entry)]
    assert problems == [], problems


def test_no_decision_names_a_canonical_that_does_not_exist() -> None:
    """A mapping to nowhere passes the ledger's validator, which only checks that
    `canonical` is present.

    THE CANONICAL SPACE IS NOT ONE FILE. This test first asked only whether the name was
    a column of the weekly v26 release, and 42 `player_bio` mappings failed it -- height,
    college, draft_round, birth_date. They were right and the test was wrong: those are
    BIO-grain canonicals, registered in stat_contracts.v1.json with grain `bio`, and the
    weekly release is not where a bio column lives. The universe is the contract
    registry plus the physical tables that realise it."""
    import duckdb

    from .sources import DATA_LAKE, latest_v26
    from .column_dossier import DISPOSITIONS_PATH

    registry = json.loads(
        (DISPOSITIONS_PATH.parent / "stat_contracts.v1.json").read_text(encoding="utf-8"))
    known = {contract["canonical_name"] for contract in registry["stats"]}
    known |= {contract["stat_id"] for contract in registry["stats"]}

    connection = duckdb.connect()
    try:
        known |= {
            row[0] for row in connection.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{latest_v26()}')").fetchall()}
        games = (Path(DATA_LAKE) / "raw" / "pfr" / "boxscores"
                 / "nfl_team_games_all.parquet")
        if games.is_file():
            known |= {
                row[0] for row in connection.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{games.as_posix()}')"
                ).fetchall()}
    finally:
        connection.close()

    missing = sorted({
        entry["canonical"] for entry in load_decisions().values()
        if entry["disposition"] == "MAPPED_TO_CANONICAL"
        and entry.get("canonical") not in known
    })
    assert missing == [], f"mapped to canonical columns that do not exist: {missing}"


def test_no_decision_is_orphaned() -> None:
    """Orphans are counted rather than dropped, but a GENERATOR producing one means its
    key construction has drifted from `row_key` -- which would silently reopen every row
    it thought it had closed."""
    document = _dossier()
    keys = {row_key(row["source"], row["table_key"], row["column"])
            for row in document["rows"]}
    orphans = sorted(set(load_decisions()) - keys)
    assert orphans == [], orphans


def test_a_new_supertable_candidate_never_names_an_existing_column() -> None:
    """NEW means the supertable does not carry it. If it does, the row is a mapping and
    proposing it as new would add a duplicate column to the schema."""
    import duckdb

    from .sources import latest_v26

    connection = duckdb.connect()
    try:
        v26 = {row[0] for row in connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{latest_v26()}')").fetchall()}
    finally:
        connection.close()
    clashes = sorted({
        semantic for semantic in NFLCOM.NEW_CANDIDATES if semantic in v26})
    assert clashes == [], f"proposed as NEW but v26 already carries: {clashes}"


# ---- the nflcom pass -----------------------------------------------------------------

def test_the_nflcom_correspondence_table_has_no_double_bucketed_semantic() -> None:
    """A semantic in two buckets means the last `elif` silently wins."""
    buckets = [set(NFLCOM.MAPPED), set(NFLCOM.NEW_CANDIDATES), set(NFLCOM.EXCLUDED),
               set(NFLCOM.DERIVED_RATIOS), set(NFLCOM.ESCALATED)]
    for index, first in enumerate(buckets):
        for second in buckets[index + 1:]:
            assert not (first & second), f"semantic in two buckets: {sorted(first & second)}"


def test_player_log_coverage_returns_are_not_player_returner_stats() -> None:
    """K/P log return cells describe returns allowed by the kicker/punter's kicks."""
    # The correspondence table remains reusable by returner-owned surfaces; the
    # ownership ruling is deliberately scoped to player_logs.
    assert NFLCOM.MAPPED["kickoff_ret"] == "kickoff_returns"
    assert NFLCOM.MAPPED["punt_ret"] == "punt_returns"
    decisions, _ = NFLCOM.build_decisions()
    by_key = {entry["key"]: entry for entry in decisions}
    for layout, column in (("K_log", "ret"), ("P", "ret"),
                           ("P", "rety"), ("P", "td")):
        for source in ("nflcom_player_logs", "nflcom_player_logs_targeted"):
            key = row_key(source, f"Post Season|id_gamelog:{layout}", column)
            assert by_key[key]["disposition"] == "EXCLUDED_WITH_REASON"


def test_team_block_components_and_rush_20_are_candidates_not_wrong_canonicals() -> None:
    """Team defensive block components and a 20-yard allowed threshold need new targets.

    The existing ``fg_blocked``, ``punts_blocked`` and ``def_explosive_rush_allowed``
    columns describe different subjects or thresholds.  Keeping these rows mapped would
    make a plausible-looking but semantically false witness.
    """
    decisions, _ = CATEGORY.build_decisions()
    by_key = {entry["key"]: entry for entry in decisions}
    for category, side, column in (
        ("kickoff-returns", "special-teams", "fg_blk"),
        ("punt-returns", "special-teams", "p_blk"),
        ("rushing", "defense", "20"),
    ):
        for season_type in ("reg", "post"):
            key = row_key("nflcom_team_stats", f"{category}|{side}|{season_type}", column)
            assert by_key[key]["disposition"] == "NEW_SUPERTABLE_COLUMN_CANDIDATE"


def test_team_receiving_defense_20_keeps_the_existing_pass_allowed_witness() -> None:
    """The receiving/defense 20+ column is an independently published pass-allowed arm."""
    decisions, _ = CATEGORY.build_decisions()
    by_key = {entry["key"]: entry for entry in decisions}
    for season_type in ("reg", "post"):
        key = row_key("nflcom_team_stats", f"receiving|defense|{season_type}", "20")
        assert by_key[key]["disposition"] == "MAPPED_TO_CANONICAL"
        assert by_key[key]["canonical"] == "def_explosive_pass_allowed"


def test_the_splits_families_are_only_adjudicated_against_the_REPAIRED_tables() -> None:
    """INVERTED 2026-07-29, and the inversion is the point.

    This test used to say `player_splits` and `player_situational` may NEVER be
    adjudicated -- they carried the O.9.0 COLUMN-SHIFT defect, so every stored value sat
    one column left of its name and a decision would have laundered a defect. The O.9.0b
    repair landed and sources.py was REPOINTED at the unshifted tables on 2026-07-28, so
    the ban is spent. What must not be spent is the reason for it: a decision on these
    families is only meaningful against the REPAIRED bytes.

    So the guard is kept and turned around -- the source definitions must still point at
    `*_unshifted`. If anyone ever repoints them back at the raw tables, the hundreds of
    decisions in the ledger silently become claims about mislabelled columns again, and
    this fails instead."""
    from .sources import NFLCOM_PLAYER_SITUATIONAL, NFLCOM_PLAYER_SPLITS

    for source in (NFLCOM_PLAYER_SPLITS, NFLCOM_PLAYER_SITUATIONAL):
        assert source.path.replace("\\", "/").endswith("_unshifted"), (
            f"{source.key} points at {source.path} -- decisions in the ledger were made "
            "against the column-shift REPAIR and are wrong for the raw table")


def test_only_resolved_signatures_are_adjudicated() -> None:
    """A NAMED signature has no layout reproducing it, so its block boundaries are not
    receipted -- the evidence is an uppercased header string and nothing more."""
    document = json.loads(NFLCOM.NFLCOM_SEMANTICS.read_text(encoding="utf-8"))
    for family, source_key in NFLCOM.FAMILIES.items():
        # `player_logs_targeted` is a CAPTURE population, not a header grammar: it shares
        # player_logs' signatures, which is why the module carries `_signature_family`.
        # The test must resolve through it or it looks for a family the semantics artifact
        # has no reason to contain.
        statuses = {
            signature.get("status")
            for signature in document["families"][
                NFLCOM._signature_family(family)]["signatures"]}
        assert statuses == {"RESOLVED"}, (
            f"{source_key}: adjudicated a family carrying {statuses - {'RESOLVED'}}")


def test_the_escalated_label_questions_stay_open() -> None:
    """These are the rows an unchecked bulk pass would have swallowed. Each carries the
    measurement that stopped the guess; none may acquire a disposition until it is
    settled."""
    decisions, tally = NFLCOM.build_decisions()
    assert tally["escalated_left_open"] > 0
    assert tally["unhandled_semantics"] == [], (
        "a semantic reached neither a bucket nor the escalation list -- it would be "
        "left OPEN with nobody having looked at it")
    assert "def_tackles" in NFLCOM.ESCALATED
    assert "def_fum_rec" not in NFLCOM.ESCALATED
    for question in NFLCOM.ESCALATED.values():
        assert "SETTLED BY" in question, "an escalation must say what would settle it"


def test_defense_log_fr_uses_the_measured_opponent_recovery_witness() -> None:
    assert NFLCOM.MAPPED["def_fum_rec"] == "fumble_recovery_opp"
    decisions, _ = NFLCOM.build_decisions()
    fr_rows = [row for row in decisions if "DEF_log|fr" in row["key"]]
    assert fr_rows
    assert {row["disposition"] for row in fr_rows} == {"MAPPED_TO_CANONICAL"}
    assert {row["canonical"] for row in fr_rows} == {"fumble_recovery_opp"}


def test_a_derived_ratio_without_its_operands_becomes_material() -> None:
    """The exclusion argument is 'the operands are on the same page, so this row adds
    nothing'. Where they are NOT, the ratio is the only form the number appears in and
    excluding it would lose it -- so the generator must check rather than assert."""
    decisions, _ = NFLCOM.build_decisions()
    by_key = {entry["key"]: entry for entry in decisions}
    excluded = [entry for entry in by_key.values()
                if entry["disposition"] == "EXCLUDED_WITH_REASON"
                and "derived ratio" in entry["reason"]]
    assert excluded, "no derived ratio was excluded -- the operand check never ran"
    for entry in excluded:
        assert "MEASURED on this signature" in entry["reason"]


# ---- the statscrew pass --------------------------------------------------------------

def test_the_statscrew_evidence_is_the_sites_own_title() -> None:
    """`tds` is PassingTouchdowns while `td` is Touchdown Percentage. Deciding either
    from the abbreviation is how a rate becomes a count."""
    dictionary = STATSCREW.load_dictionary()
    assert dictionary[("team_season_stats", "passing", "tds")]["title"] \
        == "PassingTouchdowns"
    assert dictionary[("team_season_stats", "passing", "td")]["title"] \
        == "Touchdown Percentage"
    assert STATSCREW.MAPPED[("passing", "tds")] == "passing_tds"
    assert STATSCREW.MAPPED[("passing", "td")] == "passing_td_pct"
    assert STATSCREW.MAPPED[("passing", "ints")] == "passing_interceptions"
    assert STATSCREW.MAPPED[("passing", "int")] == "passing_int_pct"


def test_the_statscrew_table_is_keyed_on_the_tag_not_the_column() -> None:
    """`no` means interceptions, kick returns, punt returns, punts, receptions, rush
    attempts and sacks depending on the table. A column-keyed table would collapse all
    seven into whichever was written last."""
    targets = {canonical for (tag, column), canonical in STATSCREW.MAPPED.items()
               if column == "no"}
    assert len(targets) >= 7, targets


def test_the_statscrew_label_question_matches_the_nflcom_one() -> None:
    """Two independent sources raised the same tackle-total question. Neither is mapped,
    and the statscrew escalation carries the identity test that failed."""
    assert ("defense_and_fumbles", "tackle") in STATSCREW.ESCALATED
    assert "9,483" in STATSCREW.ESCALATED[("defense_and_fumbles", "tackle")]
    assert ("defense_and_fumbles", "tackle") not in STATSCREW.MAPPED


def test_the_composite_results_cell_is_escalated_not_forced() -> None:
    """`game` holds opponent, both scores and the venue relation in one string. No
    single-column disposition in the vocabulary fits it, and forcing one would either
    name one of three targets or discard the only place the scores appear."""
    assert ("results", "game") in STATSCREW.ESCALATED
    assert ("results", "game") not in STATSCREW.MAPPED
    for question in STATSCREW.ESCALATED.values():
        assert "SETTLED BY" in question or "blocked on" in question.lower()


def test_the_blank_header_columns_are_kept_as_material() -> None:
    """The pre-fix parser dropped both trailing blank <th> cells along with their
    values. They carry overtime markers and playoff-round / neutral-site notes."""
    assert STATSCREW.MAPPED[("results", "col_6")] == "is_overtime"
    assert ("results", "col_7") in STATSCREW.NEW_CANDIDATES


def test_statscrew_reaches_a_decision_on_everything_it_can() -> None:
    decisions, tally = STATSCREW.build_decisions()
    assert tally["unhandled"] == [], (
        f"columns with no bucket and no escalation: {tally['unhandled']}")


# ---- the own-vocabulary pass ---------------------------------------------------------

def test_the_own_vocabulary_rule_is_scoped_by_authorship_not_by_name_collision() -> None:
    """THE WHOLE ARGUMENT. For a table we author, a column matching a canonical stat name
    matches because we chose the name on both sides. For a table we transcribe, it could
    be a false friend -- StatsCrew's `td` is a PERCENTAGE. The rule is only sound inside
    the authored set, so no foreign-lineage source may enter it."""
    from . import internal_column_adjudication as INTERNAL
    from .sources import registry

    sources = registry(include_subject=True)
    foreign = {"nflcom", "statscrew", "newspaper"}
    intruders = sorted(
        key for key in INTERNAL.OWN_VOCABULARY
        if key in sources and sources[key].lineage in foreign
        and not key.startswith("ancient_"))
    assert intruders == [], (
        f"a transcribed source entered the authored set: {intruders} -- an exact name "
        "match there is a coincidence claim, not an authorship claim")
    for key, why in INTERNAL.OWN_VOCABULARY.items():
        assert key in sources, f"{key}: not a registered source"
        assert len(why) > 30, f"{key}: authorship claim needs a real reason"


def test_the_authored_match_rate_is_what_separates_it_from_luck() -> None:
    """If our own tables resolved at the same rate as transcribed ones, the rule would
    be reading coincidence. Measured, they do not -- and this test fails if that stops
    being true, because then the argument for the rule has evaporated."""
    from . import internal_column_adjudication as INTERNAL

    contracts = INTERNAL._contracts()
    document = _dossier()
    authored = transcribed = authored_hit = transcribed_hit = 0
    for row in document["rows"]:
        matched = row["column"] in contracts
        if row["source"] in INTERNAL.OWN_VOCABULARY:
            authored += 1
            authored_hit += matched
        elif row["lineage"] in {"nflcom", "newspaper", "statscrew"}:
            transcribed += 1
            transcribed_hit += matched
    assert authored and transcribed
    authored_rate = authored_hit / authored
    transcribed_rate = transcribed_hit / transcribed
    assert authored_rate > 0.75, authored_rate
    assert transcribed_rate < 0.35, transcribed_rate
    assert authored_rate > 2 * transcribed_rate, (
        f"authored {authored_rate:.0%} vs transcribed {transcribed_rate:.0%} -- the gap "
        "that justifies reading a name match as authorship has closed")


def test_the_unmatched_columns_are_reported_by_name() -> None:
    """133 legacy columns resolve to no contract. That is the interesting half -- dead
    vocabulary or concepts the registry never picked up -- and it must be enumerable,
    not merely a count."""
    from . import internal_column_adjudication as INTERNAL

    _, tally = INTERNAL.build_decisions()
    assert tally["left_open_no_contract"] == sum(
        len(columns) for columns in tally["unmatched"].values())
    assert "legacy_motherduck_supertable" in tally["unmatched"]


def test_settled_source_position_strings_map_to_nfl_position() -> None:
    """A source's specific position vocabulary must not be collapsed into the ten
    fantasy buckets carried by `position`. The settled taxonomy sends PFR season
    strings and newspaper-listed strings to `nfl_position`, including multi-position
    values such as C/G and DE-LB."""
    pfr_decisions, _ = PFR.build_decisions()
    sidecar_decisions, _ = NEWSPAPER_SIDECAR.build_decisions()
    internal_decisions, _ = INTERNAL.build_decisions()
    expected_ancient_aliases = {
        ("ancient_pbp1978_recovery", "source_positions"): "nfl_position",
        ("ancient_pfa_gamelog", "source_positions"): "nfl_position",
    }
    assert INTERNAL.CANONICAL_ALIASES == expected_ancient_aliases

    # The internal pass owns only these two residual ancient-bundle position strings.
    # PFR and newspaper source_positions are deliberately owned by their correspondence
    # passes; another internal source-position row must not be swept in by an alias rule.
    internal_alias_keys = {
        entry["key"] for entry in internal_decisions
        if entry["key"].rsplit("|", 1)[-1] == "source_positions"
    }
    assert internal_alias_keys == {
        f"{source}|*|{column}"
        for source, column in expected_ancient_aliases
    }

    decisions = pfr_decisions + sidecar_decisions + internal_decisions
    position_rows = [
        entry for entry in decisions
        if entry["key"].rsplit("|", 1)[-1]
        in {"pos", "source_positions", "listed_position_raw"}
    ]

    assert position_rows, "the settled source-position rows were left open"
    assert {entry["canonical"] for entry in position_rows} == {"nfl_position"}
    assert all(entry["disposition"] == "MAPPED_TO_CANONICAL"
               for entry in position_rows)
    keys = {entry["key"] for entry in position_rows}
    assert {
        "ancient_pbp1978_recovery|*|source_positions",
        "ancient_pfa_gamelog|*|source_positions",
        "ancient_pfr_recovery|*|source_positions",
        "ancient_newspaper_ocr|*|source_positions",
    } <= keys
    assert "ancient_pbp1978_recovery|*|source_positions" not in PBP.escalated_row_keys()


# ---- the ledger's own protections ----------------------------------------------------

def test_a_hand_decision_survives_a_generator_rerun(tmp_path) -> None:
    """A generated table is triage at scale; a human who looked at one column and
    disagreed is the authority. Re-running the generator must not revert them."""
    ledger = json.loads(DISPOSITIONS_PATH.read_text(encoding="utf-8"))
    hand = {"key": "some|*|column", "disposition": "EXCLUDED_WITH_REASON",
            "reason": "a human looked at it", "evidence": "measured by hand"}
    generated = {"key": "some|*|column", "disposition": "MAPPED_TO_CANONICAL",
                 "canonical": "pass_yds", "reason": "table says so", "evidence": "table"}
    scratch = tmp_path / "column_dispositions.v1.json"
    scratch.write_text(json.dumps(ledger | {"decisions": [hand]}), encoding="utf-8")

    original = NFLCOM.DISPOSITIONS_PATH
    try:
        NFLCOM.DISPOSITIONS_PATH = scratch
        result = NFLCOM.apply_to_ledger([generated])
    finally:
        NFLCOM.DISPOSITIONS_PATH = original
    assert result["hand_decisions_preserved"] == 1
    assert result["written"] == 0
    kept = json.loads(scratch.read_text(encoding="utf-8"))["decisions"][0]
    assert kept["disposition"] == "EXCLUDED_WITH_REASON"


def test_every_committed_decision_records_which_generator_wrote_it() -> None:
    """Provenance is what makes the hand-decision rule enforceable: without it every
    decision looks hand-made and no generator could ever update its own output.

    DERIVED FROM `column_escalation_census.GENERATORS`, not typed a second time. The
    allowlist used to be a hand-written set literal, so landing a new adjudication pass
    meant remembering to add it in two files -- and the one that fails is this one, AFTER
    the ledger has already been written. One list, two consumers.
    """
    from importlib import import_module

    from .column_escalation_census import GENERATORS

    allowed = {import_module(f"{__package__}.{name}").GENERATOR for name in GENERATORS}
    generators = {entry.get("generated_by") for entry in load_decisions().values()}
    assert generators <= allowed | {None}, generators - allowed


def test_a_generator_can_revise_its_own_earlier_decision(tmp_path) -> None:
    """THE BUG THIS PINS, which reported success twice before it was noticed.

    `apply_to_ledger` protects hand decisions by refusing to overwrite an entry whose
    `generated_by` is not this generator. The statscrew pass called it with `__name__`,
    which under `python -m` is "__main__" -- so it stopped recognising its own output,
    read all 114 of its rows as hand-written, declined to update them, and printed a
    clean result. A correspondence table that can only be extended and never revised is
    not a ledger, and nothing about the run said so.
    """
    from . import statscrew_column_adjudication as SC

    for module in (NFLCOM, SC, __import__(
            "scripts.sota_recon.internal_column_adjudication", fromlist=["x"])):
        assert "." in module.GENERATOR, module.GENERATOR
        assert module.GENERATOR != "__main__"

    with pytest.raises(ValueError, match="stable module path"):
        NFLCOM.apply_to_ledger([], generated_by="__main__")

    ledger = json.loads(DISPOSITIONS_PATH.read_text(encoding="utf-8"))
    key = "s|t|c"
    first = {"key": key, "disposition": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
             "reason": "r", "evidence": "a receipt naming an artifact",
             "generated_by": NFLCOM.GENERATOR}
    scratch = tmp_path / "column_dispositions.v1.json"
    scratch.write_text(json.dumps(ledger | {"decisions": [first]}), encoding="utf-8")
    revised = {"key": key, "disposition": "EXCLUDED_WITH_REASON",
               "reason": "a domain ruling landed",
               "evidence": "measured: zero non-zero values in scope"}
    original = NFLCOM.DISPOSITIONS_PATH
    try:
        NFLCOM.DISPOSITIONS_PATH = scratch
        result = NFLCOM.apply_to_ledger([revised], generated_by=NFLCOM.GENERATOR)
    finally:
        NFLCOM.DISPOSITIONS_PATH = original
    assert result == {"written": 1, "hand_decisions_preserved": 0, "ledger_total": 1}
    assert json.loads(scratch.read_text(encoding="utf-8"))["decisions"][0][
        "disposition"] == "EXCLUDED_WITH_REASON"


def test_generator_revision_preserves_attached_measurement_receipt(tmp_path) -> None:
    """A mapping revision must not erase an audit-capacity receipt on the same key."""
    ledger = {"version": 1, "decisions": []}
    key = "s|t|c"
    previous = {
        "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": "old",
        "reason": "old", "evidence": "old", "generated_by": NFLCOM.GENERATOR,
        "audit_capacity": {"kind": "EQUATION", "status": "RECEIPTED",
                            "expression": "x == y", "audits": ["x"],
                            "receipt": "measured"},
    }
    ledger["decisions"].append(previous)
    scratch = tmp_path / "column_dispositions.v1.json"
    scratch.write_text(json.dumps(ledger), encoding="utf-8")
    original = NFLCOM.DISPOSITIONS_PATH
    try:
        NFLCOM.DISPOSITIONS_PATH = scratch
        NFLCOM.apply_to_ledger([{
            "key": key, "disposition": "EXCLUDED_WITH_REASON",
            "reason": "new ruling", "evidence": "new evidence",
        }], generated_by=NFLCOM.GENERATOR)
    finally:
        NFLCOM.DISPOSITIONS_PATH = original
    revised = json.loads(scratch.read_text(encoding="utf-8"))["decisions"][0]
    assert revised["disposition"] == "EXCLUDED_WITH_REASON"
    assert revised["audit_capacity"]["status"] == "RECEIPTED"


def test_the_cfl_columns_are_excluded_on_a_ruling_AND_a_measurement() -> None:
    """Joe, 2026-07-28: "we don't need the CFL stuff." StatsCrew renders every league
    from one table shape, so an NFL page still carries the rouge columns. The ruling was
    checked against the data before being applied, and the data agrees harder than the
    ruling does: both columns are ZERO on every one of our 168,390 rows, so nothing is
    being declined.

    `x_c` went the OTHER way on the same check. Its published title is a four-league
    union ("Extra Points, Converts, Action Points, XFL PATs") and the first pass filed it
    as a candidate pending a split -- but in NFL scope every value is an NFL extra point,
    3,789 of them non-zero. A ruling about leagues is not a ruling about every column
    whose title mentions one."""
    from . import statscrew_column_adjudication as SC

    for key in (("kicking", "kos"), ("total_scoring", "single")):
        assert key in SC.EXCLUDED, key
        assert key not in SC.NEW_CANDIDATES and key not in SC.MAPPED
        assert "ZERO non-zero" in SC.EXCLUDED[key] or "ZERO non-zero" in SC.EXCLUDED[key]
    assert SC.MAPPED[("total_scoring", "x_c")] == "pat_made"
    assert ("total_scoring", "x_c") not in SC.NEW_CANDIDATES


def test_a_duplicate_of_names_a_row_the_dossier_actually_produces() -> None:
    """DUPLICATE_OF points at ANOTHER DOSSIER ROW, not at a canonical column. A target
    the dossier does not produce would make the duplicate untraceable -- and the ledger's
    validator only checks that `canonical` is non-empty, so it would pass."""
    document = _dossier()
    keys = {row_key(row["source"], row["table_key"], row["column"])
            for row in document["rows"]}
    dangling = sorted({
        entry["canonical"] for entry in load_decisions().values()
        if entry["disposition"] == "DUPLICATE_OF" and entry["canonical"] not in keys})
    assert dangling == [], dangling


def test_the_cached_passing_tables_are_marked_duplicate_not_left_open() -> None:
    """sources.py already excludes these ROWS by source-definition filter, measured by
    content hash. Leaving their COLUMNS open invites someone to adjudicate `att` as a
    kicking attempt -- the exact mislabelling the row filter exists to prevent."""
    decisions = load_decisions()
    for family in ("kicking|special-teams", "punting|special-teams"):
        marked = [key for key, entry in decisions.items()
                  if key.startswith(f"nflcom_team_stats|{family}|")
                  and entry["disposition"] == "DUPLICATE_OF"]
        assert marked, f"{family}: no column marked as a duplicate of the passing table"
