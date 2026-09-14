"""RULE C: every refusal carries a machine-checkable PRECONDITION, re-checked every run.

THE DEFECT THIS CLOSES, three instances of one shape measured 2026-07-27/28:

    the splits/situational refusal   cited "QUARANTINED for the column-shift defect".
                                     The quarantine cleared, `QUARANTINED_SOURCES` went
                                     EMPTY, sources.py was repointed at the unshifted
                                     tables -- and the refusal sat for a day holding out
                                     7,782,148 rows on a condition that no longer held.
    the crosswalk receipt            wrote its verdict to `gates.licenses_sources` while
                                     the consumer read `rec["status"]`. It licensed
                                     NOTHING, and it failed in the SAFE direction, which
                                     is exactly why nobody saw it.
    MAPPING_PENDING notes            statscrew's "era-alias" blocker was measurably absent.

All three are the same shape: **a claim hardcoded in one file whose condition lives in
another, with nothing joining them.** Rule A (no hand-maintained numbers) and Rule B (every
counter publishes its denominator) both exist because a stale FIGURE is dangerous. A stale
REFUSAL is worse: a wrong number gets corrected the next time someone looks, while a refusal
stops anyone from looking at all.

WHAT A REFUSAL IS HERE. Not an escalation -- `column_escalation_census` already covers the
column-grain "somebody measured and refused, here is the question". A refusal in this module
is coarser and more dangerous: a whole SOURCE, FAMILY or TOC ENTRY held out of the program,
where the reason lives in one file and the condition lives in another.

THE DENOMINATOR IS DISCOVERED, NOT DECLARED. `discover_sites()` walks five mechanical
inventories -- MAPPING_PENDING, the capture-contract EXCLUDED rows, the capture-contract
lineage exemptions, QUARANTINED_SOURCES, and every `does_not_license` clause in the enrolled
crosswalk receipts. A refusal site that appears in any of them and is not enrolled here FAILS
the scoreboard, so a new refusal cannot land unchecked. That is Rule B applied to Rule C: the
counter would otherwise be measuring only the refusals someone remembered to type in.

THE PREDICATES ARE SMALL AND LITERAL. Each answers one question against live state -- is
this source in the quarantine set, does any PASS receipt license it, does the contract
registry carry a canonical column that would consume this material, has a human signed this
off. A refusal whose predicate returns FALSE is SPENT: the ground it stood on is gone, and
the scoreboard fails until someone either lifts the refusal or re-declares it on ground that
still holds.

FOUND BY THIS MODULE ON ITS FIRST RUN, which is the argument for it existing:

    pfr | awards / honors        refused because "no canonical stat column consumes awards
                                 or honors" -- while stat_contracts.v1.json registers 42
                                 `honors_bio` canonicals including hof, mvp, dpoy,
                                 pro_bowl, all_pro_first_team, career_mvps.
    nflcom | draft / ...         refused because "no canonical column consumes them" --
                                 while draft_round, draft_overall and age_at_draft are
                                 registered canonicals.

Both reasons were false at the moment they were read, and neither would ever have been
re-read, because nothing joined the reason to the registry it made a claim about.

    python -m scripts.sota_recon.refusal_preconditions
"""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path

HERE = Path(__file__).resolve().parent
CONTRACTS = HERE / "witness_gate" / "contracts"
DOCS = HERE.parents[1] / "docs"
SIGNOFF_LEDGER = CONTRACTS / "signoff_ledger.v1.json"
RECEIPT = DOCS / "refusal-preconditions.json"
COMPOSITE_RECEIPT = DOCS / "composite-witness-lane-receipt.json"
COMPOSITE_DISPOSITIONS = CONTRACTS / "column_dispositions.v1.json"
_ACTIVE_COMPOSITE_GROUPS: tuple[dict, ...] | None = None


# ---------------------------------------------------------------- the predicates
#
# Each takes the precondition spec and returns (holds, observation). `holds=True` means the
# ground the refusal stands on is still there. `holds=False` means the refusal is SPENT.
# An observation is mandatory: a predicate that cannot say WHAT it looked at is a boolean
# with no receipt, which is the thing this module exists to stop.


def _p_source_quarantined(spec: dict) -> tuple[bool, str]:
    from .column_dossier import QUARANTINED_SOURCES

    source = spec["source"]
    inside = source in QUARANTINED_SOURCES
    return inside, (f"column_dossier.QUARANTINED_SOURCES = {sorted(QUARANTINED_SOURCES)!r}; "
                    f"{source!r} {'IS' if inside else 'is NOT'} a member")


def _p_no_receipt_licenses_source(spec: dict) -> tuple[bool, str]:
    """Holds while NO passing crosswalk receipt licenses this source.

    Reads `status`, which is the field the consumer (kc_planes) reads. Reading
    `gates.licenses_sources` instead is the exact bug that made the nflcom receipt license
    nothing while reporting success, so this predicate reads the consumer's field and says
    so in its observation.
    """
    source = spec["source"]
    document = json.loads((CONTRACTS / "crosswalk_receipts.v1.json").read_text(encoding="utf-8"))
    licensing = [r["receipt_id"] for r in document["receipts"]
                 if r.get("status") == "PASS" and source in (r.get("licenses") or [])]
    enrolled_without_status = [r["receipt_id"] for r in document["receipts"]
                              if r.get("status") is None and source in (r.get("licenses") or [])]
    note = ""
    if enrolled_without_status:
        note = (f"; NOTE {enrolled_without_status} name it in `licenses` but carry NO "
                "`status` field, so the consumer's `rec['status'] == 'PASS'` test cannot "
                "see them -- the receipt fails SAFE and licenses nothing")
    return not licensing, (f"crosswalk_receipts.v1.json: receipts with status==PASS "
                           f"licensing {source!r} = {licensing!r}{note}")


def load_signoff_ledger() -> dict:
    return json.loads(SIGNOFF_LEDGER.read_text(encoding="utf-8"))


def _p_no_signoff(spec: dict) -> tuple[bool, str]:
    item_id = spec["item"]
    ledger = load_signoff_ledger()
    matches = [i for i in ledger["items"] if i["id"] == item_id]
    if not matches:
        raise KeyError(f"signoff_ledger.v1.json has no item {item_id!r} -- a refusal cannot "
                       "wait on a decision nobody has been asked to make")
    status = matches[0]["status"]
    return status != "APPROVED", f"signoff_ledger.v1.json:{item_id} status={status!r}"


def _p_lineage_without_capture_contract(spec: dict) -> tuple[bool, str]:
    from .capture_contracts import CONTRACTS as CAPTURE
    from .closure_scoreboard import LINEAGE_CONTRACT_ALIASES

    lineage = spec["lineage"]
    resolved = LINEAGE_CONTRACT_ALIASES.get(lineage, lineage)
    return resolved not in CAPTURE, (
        f"capture_contracts.CONTRACTS keys = {sorted(CAPTURE)!r}; lineage {lineage!r} "
        f"resolves to {resolved!r} and is {'ABSENT' if resolved not in CAPTURE else 'PRESENT'}")


def _p_no_canonical_consumer(spec: dict) -> tuple[bool, str]:
    """Holds while the contract registry carries NO canonical that would consume this.

    This is the predicate that caught the two false capture refusals. 'No canonical column
    consumes X' is a claim ABOUT stat_contracts.v1.json, made in a different file, and until
    now nothing ever joined the two.
    """
    document = json.loads((CONTRACTS / "stat_contracts.v1.json").read_text(encoding="utf-8"))
    universe = {c["canonical_name"] for c in document["stats"]} | {
        c["stat_id"] for c in document["stats"]}
    families = {c.get("family") for c in document["stats"]}
    present = sorted(n for n in spec["names"] if n in universe)
    present += sorted(f"family={f}" for f in spec.get("families", []) if f in families)
    return not present, (f"stat_contracts.v1.json: of the consumers this refusal claims do "
                         f"not exist, PRESENT = {present!r} (checked "
                         f"{len(spec['names'])} names, "
                         f"{len(spec.get('families', []))} families)")


def _p_no_mapspec_for_source(spec: dict) -> tuple[bool, str]:
    """Holds while no MapSpec aliases any column of this source.

    LICENSING AND MAPPING ARE DIFFERENT GATES, and this predicate is where that law stops
    being a slogan. On 2026-07-29 Joe approved the promotion, the receipt was written, and
    six nflcom refusals standing on NO_SIGNOFF / NO_RECEIPT_LICENSES_SOURCE all went SPENT
    at once -- correctly, because those families ARE licensed now. What is still owed is
    the MapSpec that lets a validator resolve a witness through a column, and that is
    engineering work rather than a decision anyone is waiting on. Re-declaring on this
    predicate says exactly that, and it cannot be satisfied by writing prose.
    """
    from . import witness_map as WM

    source = spec["source"]
    specs = [m for m in WM.WITNESS_MAP if m.source_key == source]
    return not specs, (f"witness_map.WITNESS_MAP holds {len(specs)} MapSpec(s) for "
                       f"{source!r} out of {len(WM.WITNESS_MAP)} total")


# _p_source_path_endswith was RETIRED 2026-07-30 with its last enrolment.
#
# It guarded a real hazard: while nflcom_player_situational pointed at
# `tables/player_situational` its stored column names were one column off, and a silent
# revert of the repoint to `player_situational_unshifted` would put every spec on shifted
# data without erroring. Discharging the refusal must not discharge the protection, so the
# check moved to a standing test -- test_mapspec_generator.py::
# test_the_shifted_nflcom_tables_are_never_the_source -- which now runs on every suite
# rather than only while a refusal happened to be enrolled.

def _p_registry_lacks_key(spec: dict) -> tuple[bool, str]:
    from importlib import import_module

    module = import_module(f"{__package__}.{spec['module']}")
    container = getattr(module, spec["registry"])
    absent = spec["key"] not in container
    return absent, (f"{spec['module']}.{spec['registry']} has {len(container)} entries; "
                    f"{spec['key']!r} {'is ABSENT' if absent else 'IS PRESENT'}")


def _composite_group(spec_id: str) -> dict | None:
    from .composite_column_adjudication import build_decisions

    groups = _ACTIVE_COMPOSITE_GROUPS
    if groups is None:
        _, groups = build_decisions(COMPOSITE_RECEIPT)
    return next((group for group in groups if group["spec_id"] == spec_id), None)


@contextmanager
def _shared_composite_evaluation():
    """Share one manifest/receipt evaluation across all five predicates in a build."""
    from .composite_column_adjudication import build_decisions

    global _ACTIVE_COMPOSITE_GROUPS
    previous = _ACTIVE_COMPOSITE_GROUPS
    _, groups = build_decisions(COMPOSITE_RECEIPT)
    _ACTIVE_COMPOSITE_GROUPS = groups
    try:
        yield
    finally:
        _ACTIVE_COMPOSITE_GROUPS = previous


def _p_composite_spec_not_pass(spec: dict) -> tuple[bool, str]:
    """Holds until this exact local specification produces a current licensed PASS."""
    spec_id = spec["spec_id"]
    group = _composite_group(spec_id)
    passing = group is None
    evidence = None if group is None else group["evidence_summary"]
    return not passing, (
        f"composite-witness-lane-receipt.json spec {spec_id!r}: "
        f"current_semantically_valid_licensed_PASS={passing}; evidence={evidence!r}"
    )


def _p_no_like_for_like_nflcom_l7_grain(spec: dict) -> tuple[bool, str]:
    """Hold only while neither a current PASS nor canonical split projection exists."""
    from .composite_column_adjudication import (
        CANONICAL_SPLIT_PROJECTIONS,
    )

    spec_id = spec["spec_id"]
    projection = CANONICAL_SPLIT_PROJECTIONS.get(spec_id)
    group = _composite_group(spec_id)
    passing = group is None
    evidence = None if group is None else group["evidence_summary"]
    holds = not passing and not projection
    return holds, (
        f"composite-witness-lane-receipt.json spec {spec_id!r}: "
        f"current_semantically_valid_licensed_PASS={passing}; executable canonical "
        f"split projection={projection!r}; resolved conflicting grain evidence={evidence!r}"
    )


PREDICATES = {
    "SOURCE_QUARANTINED": _p_source_quarantined,
    "NO_RECEIPT_LICENSES_SOURCE": _p_no_receipt_licenses_source,
    "NO_SIGNOFF": _p_no_signoff,
    "LINEAGE_WITHOUT_CAPTURE_CONTRACT": _p_lineage_without_capture_contract,
    "NO_CANONICAL_CONSUMER": _p_no_canonical_consumer,
    "NO_MAPSPEC_FOR_SOURCE": _p_no_mapspec_for_source,
    "REGISTRY_LACKS_KEY": _p_registry_lacks_key,
    "COMPOSITE_SPEC_NOT_PASS": _p_composite_spec_not_pass,
    "NO_LIKE_FOR_LIKE_NFLCOM_L7_GRAIN": _p_no_like_for_like_nflcom_l7_grain,
}


def composite_refusal_entries() -> dict[str, dict]:
    """Build the five refusal declarations from the adjudicator's shared registries."""
    from .composite_column_adjudication import (
        BLOCKER_CODES_BY_SPEC,
        SETTLING_EVIDENCE_BY_SPEC,
    )

    entries: dict[str, dict] = {}
    for spec_id, codes in BLOCKER_CODES_BY_SPEC.items():
        if spec_id == "nflcom-l7-fg-buckets-v1":
            precondition = {
                "kind": "NO_LIKE_FOR_LIKE_NFLCOM_L7_GRAIN",
                "spec_id": spec_id,
            }
        else:
            precondition = {"kind": "COMPOSITE_SPEC_NOT_PASS", "spec_id": spec_id}
        entries[f"composite_spec_blocker:{spec_id}"] = {
            "refuses": f"closing the exact contract rows owned by {spec_id}; production "
                       f"blocker codes are {codes!r}",
            "precondition": precondition,
            "settles_when": f"{SETTLING_EVIDENCE_BY_SPEC[spec_id]}; the exact "
                            "specification then produces a current semantically valid "
                            "licensed PASS receipt and its PASS-owned decisions are applied",
        }
    return entries


# ---------------------------------------------------------------- the enrolled refusals
#
# `settles_when` is prose for the reader; `precondition` is what the gate evaluates. Where a
# refusal previously stood on ground that has since gone, `superseded` records the dead
# precondition and the date it died -- deleting it would erase the evidence that this
# mechanism was needed, and a test asserts those stay spent.

REFUSALS: dict[str, dict] = {
    # ---- the five exact composite specification blockers -----------------------------
    **composite_refusal_entries(),
    # ---- the six nflcom player families, one shared blocker ---------------------------
    # ---- RE-DECLARED 2026-07-29, and the re-declaration is the mechanism working.
    # These four stood on NO_RECEIPT_LICENSES_SOURCE. The receipt was written that day and
    # licenses all four with status PASS, so that precondition went FALSE and the gate
    # failed -- which is exactly what it is for. They are LICENSED now. What is still owed
    # is the MapSpec, and that is our work, not a decision anyone is waiting on.
    **{
        f"mapping_pending:{source}": {
            "refuses": f"treating {source} as MAPPED. It is licensed to vote and its "
                       "columns are adjudicated in the dossier, but no MapSpec aliases them, "
                       "so no validator can resolve a witness through one yet",
            "precondition": {"kind": "NO_MAPSPEC_FOR_SOURCE", "source": source},
            "settles_when": "MapSpecs are authored for this source's adjudicated columns; "
                            "the identity decisions already exist in "
                            "column_dispositions.v1.json and the crosswalk resolves the key "
                            "space, so this is transcription, not adjudication",
            "superseded": {
                "precondition": {"kind": "NO_SIGNOFF",
                                 "item": "write_nflcom_crosswalk_receipt"},
                "spent_utc": "2026-07-29",
                "note": "Joe approved and the receipt was written the same day, licensing "
                        "2,278,306 rows across these four families",
            },
        }
        # 2026-07-30: career and season LEFT this loop -- their MapSpecs exist now. The
        # two that remain are held by a CAPTURE defect, not a modelling one: their
        # position-group block id is absent from the parquet, so `yds` is physically a
        # union of passing, rushing and interception-return yards and no single canonical
        # can be aliased to it. That is why giving MapSpec a table selector freed the other
        # four and not these.
        for source in ("nflcom_player_logs", "nflcom_player_logs_targeted")
    },
    # ---- RETIRED 2026-07-30: nflcom_player_splits and nflcom_player_situational.
    # Both stood on NO_MAPSPEC_FOR_SOURCE and both now carry 39 table-scoped specs each,
    # so the precondition went FALSE and this gate failed until the enrolment was removed --
    # the mechanism working in the discharge direction rather than the escalation one.
    #
    # Their history is worth keeping even though the entries are gone: each recorded its
    # superseded precondition from BOTH ends of the same repair -- the quarantine set that
    # was emptied, and the source path that was repointed at the unshifted tables. The
    # second was the stronger check, because emptying a set is a decision while a path is
    # where the data actually is.
    # ...and the two whose hold is now COLUMN-level, not source-level. Their superseded
    # preconditions are recorded from BOTH ENDS of the same repair, deliberately: the
    # quarantine set that was emptied, and the source path that was repointed. Either one
    # coming back true means the un-shift has been reverted and the old refusal is live
    # again -- and the second is the stronger check, because emptying a set is a decision
    # while a path is where the data actually is.
    "mapping_pending:nflcom_team_stats": {
        # RE-DECLARED 2026-07-30. The KEY blocker is spent: crosswalk_receipts.v1.json now
        # carries `nflcom_team_fid` at PASS licensing this source, and kc_planes DERIVES
        # ACTIVE from it. The key was established by stat fingerprint rather than by label,
        # because a nickname cannot be matched to a franchise by name -- every franchise
        # spans the nickname's seasons, and renamed franchises (Redskins -> Football Team ->
        # Commanders) defeat similarity. 2,146 of 2,228 team-seasons match to within a yard.
        #
        # What REMAINS is the mapping: no MapSpec targets this source yet, because its rows
        # are TEAM totals and every existing shape resolves a PLAYER to pfr_id. Its audit
        # is a conservation check -- team total against the sum of that team's players --
        # which is a different lane, not a missing spec of the usual kind.
        "refuses": "licensing nflcom_team_stats as a MAPPED value witness. Its team key is "
                   "now receipted and ACTIVE; what is still owed is the team-vs-sum-of-"
                   "players conservation lane its 136 mapped columns actually belong to",
        "precondition": {"kind": "NO_MAPSPEC_FOR_SOURCE", "source": "nflcom_team_stats"},
        "settles_when": "a team-grain conservation lane consumes its columns, or MapSpec "
                        "gains a team-grain shape",
    },
    "mapping_pending:statscrew_team_season_stats": {
        "refuses": "licensing StatsCrew season-grain per-player totals (11 table-tagged "
                   "families, 1921-2025; the pre-1994 IDP surface is its highest-value "
                   "stratum)",
        "precondition": {"kind": "NO_SIGNOFF", "item": "dob_apply"},
        "settles_when": "the DOB apply lands, making statscrew_player_id->pfr_id resolvable "
                        "where (name, season) is ambiguous, and a receipt licenses it",
    },
    "mapping_pending:statscrew_team_season_results": {
        "refuses": "licensing the StatsCrew game-calendar / running-record witness "
                   "(1,521 team-seasons, 1920-2023)",
        "precondition": {"kind": "NO_RECEIPT_LICENSES_SOURCE",
                         "source": "statscrew_team_season_results"},
        "settles_when": "statscrew team codes are canonicalized into team_fid franchise "
                        "space and a receipt licenses it",
    },

    # ---- the capture-contract lineage exemptions --------------------------------------
    "capture_exempt_lineage:internal": {
        "refuses": "holding `internal` to a capture contract",
        "precondition": {"kind": "LINEAGE_WITHOUT_CAPTURE_CONTRACT", "lineage": "internal"},
        "settles_when": "an external publisher of our derived material appears, giving it a "
                        "table of contents to reconcile against",
    },
    "capture_exempt_lineage:pbp_merged": {
        "refuses": "holding `pbp_merged` to a capture contract",
        "precondition": {"kind": "LINEAGE_WITHOUT_CAPTURE_CONTRACT", "lineage": "pbp_merged"},
        "settles_when": "the merge stops being an assembly of registered sources and "
                        "acquires a publisher of its own",
    },

    # ---- the crosswalk receipt's own does_not_license clauses -------------------------
    "receipt_does_not_license:nflcom_slug_pfrid#0": {
        "refuses": "letting an UNADJUDICATED column of a licensed family vote -- the L4 "
                   "return block, the L7 made-att composites, the L0 tackle total and the "
                   "per-layout truncations",
        "precondition": {"kind": "REGISTRY_LACKS_KEY", "module": "column_dossier",
                         "registry": "QUARANTINED_SOURCES", "key": "__never__"},
        "standing": True,
        "settles_when": "NEVER as a source-level act. This is structural rather than a "
                        "wait: an OPEN dossier row has no MAPPED_TO_CANONICAL entry, so "
                        "nothing can resolve a witness through it whatever the source is "
                        "licensed for. Individual columns leave the hold by being "
                        "adjudicated, one escalation at a time",
        "superseded": {
            "precondition": {"kind": "SOURCE_QUARANTINED", "source": "nflcom_player_splits"},
            "spent_utc": "2026-07-28",
            "note": "the enrolled contract copy of this clause STILL carries the dead "
                    "reason verbatim -- see the queue `refusal_texts_citing_a_superseded_"
                    "precondition`. Correcting it means re-running --write-receipt, which "
                    "also licenses 2,278,306 rows, and that is a licensing act",
        },
    },
    "receipt_does_not_license:nflcom_slug_pfrid#1": {
        "refuses": "licensing rows whose slug never resolved -- they stay unlicensed rather "
                   "than being joined on a weaker key",
        "precondition": {"kind": "REGISTRY_LACKS_KEY", "module": "column_dossier",
                         "registry": "QUARANTINED_SOURCES", "key": "__never__"},
        "settles_when": "NEVER by design. This is a standing method refusal (a bare name "
                        "join is the twins hazard, §19.2), not a condition waiting to "
                        "clear. Enrolled with a predicate that is true by construction so "
                        "that it is visibly CHECKED rather than quietly exempt",
        "standing": True,
    },

    # ---- the capture-contract EXCLUDED rows -------------------------------------------
    # Nine refusals to capture. Six rest on a domain argument -- "no canonical column
    # consumes this" -- which is a claim about stat_contracts.v1.json made in another file.
    # Joining them is what caught the two false ones.
    "capture_toc_excluded:nextgen_stats|NGS tracking primitives (raw player tracking)": {
        "refuses": "capturing raw NGS tracking primitives",
        "precondition": {"kind": "NO_CANONICAL_CONSUMER",
                         "names": ["tracking_x", "tracking_y", "player_tracking_frame"],
                         "families": []},
        "settles_when": "the league publishes tracking primitives, or a canonical column "
                        "consumes them",
    },
    "capture_toc_excluded:nflcom|standings": {
        "refuses": "capturing NFL.com standings",
        "precondition": {"kind": "REGISTRY_LACKS_KEY", "module": "sources",
                         "registry": "KNOWN_LINEAGES", "key": "__nflcom_standings__"},
        "settles_when": "NEVER by design: team W/L/T is authoritative from "
                        "nfl_team_games_all, and a second copy in a franchise space we "
                        "would have to invent adds no witness value",
        "standing": True,
    },
    "capture_toc_excluded:nflcom|schedules": {
        "refuses": "capturing NFL.com schedule pages",
        "precondition": {"kind": "REGISTRY_LACKS_KEY", "module": "sources",
                         "registry": "KNOWN_LINEAGES", "key": "__nflcom_schedules__"},
        "settles_when": "NEVER by design: the calendar is anchored by schedule_master + "
                        "pfr_team_games",
        "standing": True,
    },
    "capture_toc_excluded:nflcom|transactions / injuries": {
        "refuses": "capturing NFL.com transaction and injury pages",
        "precondition": {"kind": "NO_CANONICAL_CONSUMER",
                         "names": ["transaction_type", "injury_status", "injury_designation"],
                         "families": []},
        "settles_when": "a canonical column consumes them",
        "note": "RE-DECLARED 2026-07-29. The original TOC entry bundled DRAFT in and "
                "claimed no canonical column consumes any of the three. "
                "stat_contracts.v1.json registers draft_round, draft_overall and "
                "age_at_draft, so it was false for one of the three; `draft` is now its "
                "own QUEUED entry. Transactions and injuries have no consumer and the "
                "refusal stands on them alone",
    },
    "capture_toc_excluded:pfr|franchise / team pages": {
        "refuses": "capturing PFR franchise / team identity pages",
        "precondition": {"kind": "REGISTRY_LACKS_KEY", "module": "sources",
                         "registry": "KNOWN_LINEAGES", "key": "__pfr_franchise_pages__"},
        "settles_when": "NEVER by design: franchise identity is carried by the internal "
                        "franchise registry keyed on team_fid with era-scoped aliases",
        "standing": True,
    },
    "capture_toc_excluded:statscrew|defunct-league records: WFL (1974-75)": {
        "refuses": "capturing WFL records",
        "precondition": {"kind": "REGISTRY_LACKS_KEY", "module": "sources",
                         "registry": "KNOWN_LINEAGES", "key": "wfl"},
        "settles_when": "NEVER: ruled out by Joe 2026-07-28 and pinned by the 78-code "
                        "boundary test -- v26 1974-75 carries 26 team codes, all NFL",
        "standing": True,
    },
    "capture_toc_excluded:statscrew|parallel-league records (USFL, XFL, UFL, CFL, ...)": {
        "refuses": "capturing any parallel-league records",
        "precondition": {"kind": "REGISTRY_LACKS_KEY", "module": "sources",
                         "registry": "KNOWN_LINEAGES", "key": "cfl"},
        "settles_when": "NEVER: SETTLED, pinned by the 78-code boundary test over the whole "
                        "v26 release. APFA/AAFC/AFL are IN; CFL/WFL/USFL/XFL/UFL are OUT",
        "standing": True,
    },
    "capture_toc_excluded:statscrew|coaching records": {
        "refuses": "capturing coaching records",
        "precondition": {"kind": "NO_CANONICAL_CONSUMER",
                         "names": ["coach", "head_coach", "coaching_record"], "families": []},
        "settles_when": "a canonical column consumes coaching tenure",
    },
}


# ---------------------------------------------------------------- the denominator


def discover_sites() -> dict[str, str]:
    """Every refusal site the program can be MECHANICALLY shown to hold, -> its kind.

    Declaring the list by hand would make this module measure only the refusals somebody
    remembered to type in, which is the failure it exists to close one level up.
    """
    sites: dict[str, str] = {}

    # Closed contract, not data discovery: at most five specification-level refusal sites.
    # A failed spec remains a blocker. A current PASS remains a spent blocker until every
    # exact owned row is closed by the composite generator itself. Once fully applied, the
    # site disappears cleanly instead of leaving SPENT/ORPHANED residue.
    from .composite_column_adjudication import GENERATOR, build_decisions
    from .composite_witness_lane import load_specs
    decisions, groups = build_decisions(COMPOSITE_RECEIPT)
    passing = {str(decision["spec_id"]) for decision in decisions}
    blocked = {str(group["spec_id"]) for group in groups}
    try:
        ledger = json.loads(COMPOSITE_DISPOSITIONS.read_text(encoding="utf-8"))
        ledger_entries = ledger.get("decisions", [])
    except (OSError, json.JSONDecodeError, AttributeError):
        ledger_entries = []
    closed_by_spec: dict[str, set[str]] = {spec_id: set() for spec_id in passing}
    for entry in ledger_entries:
        if (
            isinstance(entry, dict)
            and entry.get("generated_by") == GENERATOR
            and entry.get("disposition") == "EXCLUDED_WITH_REASON"
            and entry.get("spec_id") in passing
        ):
            closed_by_spec[str(entry["spec_id"])].add(str(entry.get("key")))
    for spec in load_specs():
        fully_applied = (
            spec.spec_id in passing
            and set(spec.row_keys) <= closed_by_spec.get(spec.spec_id, set())
        )
        if spec.spec_id in blocked or not fully_applied:
            sites[f"composite_spec_blocker:{spec.spec_id}"] = "COMPOSITE_SPEC_BLOCKER"

    from .test_mapping_obligation import MAPPING_PENDING
    for source in MAPPING_PENDING:
        sites[f"mapping_pending:{source}"] = "MAPPING_PENDING"

    from .closure_scoreboard import CAPTURE_CONTRACT_EXEMPT_LINEAGES
    for lineage in CAPTURE_CONTRACT_EXEMPT_LINEAGES:
        sites[f"capture_exempt_lineage:{lineage}"] = "CAPTURE_CONTRACT_EXEMPT_LINEAGE"

    from .column_dossier import NEEDS_LAYOUT, QUARANTINED_SOURCES
    for source in QUARANTINED_SOURCES:
        sites[f"quarantined_source:{source}"] = "QUARANTINED_SOURCE"
    # the dossier's refusal to borrow a sibling family's layouts -- a family in NEEDS_LAYOUT
    # with no census entry is keyed under-specified ON PURPOSE and says so
    from .column_dossier import _family_signature_keys
    for source in sorted(NEEDS_LAYOUT):
        if not _family_signature_keys(source)[0]:
            sites[f"dossier_key_under_specified:{source}"] = "DOSSIER_KEY_UNDER_SPECIFIED"

    from .capture_contracts import build as capture_build
    for contract in capture_build()["contracts"]:
        for entry in contract["toc"]:
            if entry["status"] == "EXCLUDED":
                sites[f"capture_toc_excluded:{contract['source']}|{entry['toc_entry']}"] = \
                    "CAPTURE_TOC_EXCLUDED"

    receipts = json.loads(
        (CONTRACTS / "crosswalk_receipts.v1.json").read_text(encoding="utf-8"))
    for receipt in receipts["receipts"]:
        for index, _clause in enumerate(receipt.get("does_not_license") or []):
            sites[f"receipt_does_not_license:{receipt['receipt_id']}#{index}"] = \
                "RECEIPT_DOES_NOT_LICENSE"

    return dict(sorted(sites.items()))


# markers whose appearance in a refusal's STATED TEXT means the text still cites ground
# that has gone. String-level, deliberately: the enrolled contract copy of a receipt clause
# can go stale independently of the generator that produced it, and did.
SUPERSEDED_TEXT_MARKERS = {
    "QUARANTINED for the column-shift defect":
        "the column-shift quarantine cleared 2026-07-28 (QUARANTINED_SOURCES is empty and "
        "sources.py is repointed at the unshifted tables)",
    "one column left of their names at rest":
        "the un-shift landed 2026-07-28; the values are no longer displaced",
}


def _stated_texts() -> dict[str, str]:
    """The text each refusal site actually PUBLISHES, for the staleness scan."""
    texts: dict[str, str] = {}
    from .test_mapping_obligation import MAPPING_PENDING
    for source, note in MAPPING_PENDING.items():
        texts[f"mapping_pending:{source}"] = note
    from .capture_contracts import build as capture_build
    for contract in capture_build()["contracts"]:
        for entry in contract["toc"]:
            if entry["status"] == "EXCLUDED":
                texts[f"capture_toc_excluded:{contract['source']}|{entry['toc_entry']}"] = \
                    entry["note"]
    receipts = json.loads(
        (CONTRACTS / "crosswalk_receipts.v1.json").read_text(encoding="utf-8"))
    for receipt in receipts["receipts"]:
        for index, clause in enumerate(receipt.get("does_not_license") or []):
            texts[f"receipt_does_not_license:{receipt['receipt_id']}#{index}"] = clause
    return texts


def build() -> dict:
    sites = discover_sites()
    evaluated: list[dict] = []
    with _shared_composite_evaluation():
        for refusal_id in sorted(set(sites) | set(REFUSALS)):
            entry = REFUSALS.get(refusal_id)
            if entry is None:
                evaluated.append({"refusal": refusal_id, "site_kind": sites[refusal_id],
                                  "state": "UNENROLLED",
                                  "observation": "discovered on disk, enrolled nowhere -- no "
                                                 "precondition is being checked for it"})
                continue
            if refusal_id not in sites:
                if refusal_id.startswith("composite_spec_blocker:"):
                    # A current PASS whose exact owned rows were all applied by the
                    # composite generator is resolved, not an orphaned enrolment.
                    continue
                # enrolled but the site is gone: the refusal was lifted at its source and this
                # entry is now a fiction. Reported so it cannot linger as decoration.
                evaluated.append({"refusal": refusal_id, "site_kind": None, "state": "ORPHANED",
                                  "observation": "no mechanical inventory holds this refusal "
                                                 "any more; the site was removed"})
                continue
            spec = entry["precondition"]
            predicate = PREDICATES[spec["kind"]]
            holds, observation = predicate(spec)
            evaluated.append({
                "refusal": refusal_id,
                "site_kind": sites[refusal_id],
                "state": "HOLDS" if holds else "SPENT",
                "precondition": spec,
                "refuses": entry["refuses"],
                "settles_when": entry["settles_when"],
                "standing": bool(entry.get("standing")),
                "observation": observation,
                "superseded": entry.get("superseded"),
            })

    from .composite_column_adjudication import BLOCKER_CODES_BY_SPEC
    from .composite_witness_lane import load_specs
    composite_specs = {spec.spec_id: spec for spec in load_specs()}
    for row in evaluated:
        if row.get("site_kind") != "COMPOSITE_SPEC_BLOCKER":
            continue
        spec_id = row["refusal"].split(":", 1)[1]
        row["affected_rows"] = len(composite_specs[spec_id].row_keys)
        row["blocker_codes"] = BLOCKER_CODES_BY_SPEC[spec_id]

    stale_texts = []
    for site, text in _stated_texts().items():
        for marker, why in SUPERSEDED_TEXT_MARKERS.items():
            if marker in text:
                stale_texts.append({"refusal": site, "marker": marker, "spent_because": why})

    spent = [row for row in evaluated if row["state"] == "SPENT"]
    unenrolled = [row for row in evaluated if row["state"] == "UNENROLLED"]
    orphaned = [row for row in evaluated if row["state"] == "ORPHANED"]
    return {
        "generated": "RULE C: every refusal's precondition, re-evaluated against live state",
        "law": "a refusal whose precondition no longer holds is SPENT and FAILS the "
               "scoreboard; a refusal site with no enrolled precondition FAILS it too, "
               "because an unchecked refusal is indistinguishable from a checked one",
        "predicates": sorted(PREDICATES),
        "counters": {
            "refusal_sites_discovered": len(sites),
            "enrolled": len(REFUSALS),
            "holds": sum(1 for row in evaluated if row["state"] == "HOLDS"),
            "standing_by_design": sum(1 for row in evaluated if row.get("standing")),
            "refusals_with_spent_preconditions": len(spent),
            "refusal_sites_without_a_checked_precondition": len(unenrolled),
            "orphaned_enrolments": len(orphaned),
            "refusal_texts_citing_a_superseded_precondition": len(stale_texts),
        },
        "denominator": {
            "discovered_by_kind": {
                kind: sum(1 for k in sites.values() if k == kind)
                for kind in sorted(set(sites.values()))},
            "inventories_walked": ["test_mapping_obligation.MAPPING_PENDING",
                                   "composite_witnesses.v1.json exact specification blockers",
                                   "closure_scoreboard.CAPTURE_CONTRACT_EXEMPT_LINEAGES",
                                   "column_dossier.QUARANTINED_SOURCES",
                                   "column_dossier.NEEDS_LAYOUT (without a layout census)",
                                   "capture_contracts EXCLUDED toc entries",
                                   "crosswalk_receipts.v1.json does_not_license clauses"],
        },
        "spent": spent,
        "unenrolled": unenrolled,
        "orphaned": orphaned,
        "stale_refusal_texts": stale_texts,
        "refusals": evaluated,
    }


def write() -> dict:
    document = build()
    RECEIPT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def main() -> int:
    document = write()
    for name, value in document["counters"].items():
        print(f"  {name:52s} {value}")
    print("\n  discovered by kind:")
    for kind, count in document["denominator"]["discovered_by_kind"].items():
        print(f"    {kind:36s} {count}")
    if document["spent"]:
        print("\nSPENT -- the ground these stand on is gone:")
        for row in document["spent"]:
            print(f"  !! {row['refusal']}")
            print(f"     {row['observation']}")
    if document["unenrolled"]:
        print("\nUNENROLLED refusal sites (nothing is checking them):")
        for row in document["unenrolled"]:
            print(f"  !! {row['refusal']}")
    if document["stale_refusal_texts"]:
        print("\nSTALE TEXT (the refusal stands, but its published reason does not):")
        for row in document["stale_refusal_texts"]:
            print(f"  ~~ {row['refusal']}\n     cites {row['marker']!r}\n"
                  f"     {row['spent_because']}")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
