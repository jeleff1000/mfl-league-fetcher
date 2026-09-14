"""
sota_recon/lineage_roots.py  --  O.5: family x era LINEAGE-ROOT contract (master plan §16.1/16.2)

Root identity is NOT source-scoped -- it is (source x stat-family x era)-scoped. Two facts
drive this:

  1. The pbp stream is era-split: the merged PBP corpus (and everything derived from it)
     is nflverse-rooted 1999+ but PFR-pbp-rooted 1978-98. In 1978-98 the "pbp witness" is
     the SAME bloodline as the PFR tables and must fold into the pfr root -- those years
     have one independent bloodline, and the verdict must say so honestly.
  2. Roots can be family-scoped: NFL.com and PFR modern box atoms share the Elias/GSIS
     gamebook ancestor for BOX stat families (near-copies) while being genuinely
     independent for charting/derived families. No NFL.com source is registered today, so
     this contract carries that ancestry as a DECLARED_UNRECEIPTED annotation plus an open
     question -- never as a silent assumption when nflcom registers.

This module GENERATES the committed contract
  scripts/sota_recon/witness_gate/contracts/lineage_roots.v1.json
from the sources registry (whose `lineage` field is mandatory-explicit as of O.5 -- the
old lineage="pfr" default is dead). Voting lanes resolve roots ONLY through `root_of()`;
the era split is contract data, not lane code.

Independence doctrine (§16 header, standing): the declared adversary structure is
pfr / nflverse_pbp (1999+) / ngs / pfa_loc / newspaper / internal. Newspaper is its own
root and is NEVER arbitrated by PFR lineage. Platform feeds (Yahoo/Sleeper/ESPN) are
candidate roots only -- no votes until shared-error fingerprints prove independence
(§20.5, §25.5). Genuinely unclear family-scoped calls live in `open_questions`, queued
for adjudication -- escalate, don't improvise.

Regenerate + validate:  python -m scripts.sota_recon.lineage_roots
Regen-diff gate:        test_lineage_roots.py (committed file == generator output)
"""

from __future__ import annotations

import functools
import json
import os

from .sources import registry

CONTRACTS_PATH = os.path.join(
    os.path.dirname(__file__), "witness_gate", "contracts", "lineage_roots.v1.json")

# The legal ROOT vocabulary: the units that cast independent votes. `internal` is a
# bloodline label, never an adversary root (subject/derived tables cannot vote anyway).
ROOTS = ("internal", "newspaper", "nflcom", "nflverse_pbp", "ngs", "pfa_loc", "pfr",
         "statscrew")

# The pbp era split (registry doctrine, formalized here as contract data).
PBP_SPLIT_YEAR = 1999  # >= this year: nflverse-rooted; before: PFR-pbp-rooted

_LINEAGE_TO_ROOT = {
    "pfr": "pfr",
    "ngs": "ngs",
    "pfa_loc": "pfa_loc",
    "newspaper": "newspaper",
    "internal": "internal",
    "nflcom": "nflcom",        # O.8: own root; OQ-LR-1 governs any collapse into pfr
    "statscrew": "statscrew",  # O.8: candidate root; no votes until independence receipted
    # "pbp_merged" is the era-split case, handled explicitly in _assignments()
}

_PBP_PRE_BASIS = (
    "pbp_merged 1978-98 is PFR-pbp-rooted (registry-declared era split): in these years "
    "the pbp stream and the PFR tables are ONE bloodline -- one independent root, "
    "never a 2-1 majority against pfr")
_PBP_POST_BASIS = "pbp_merged 1999+ is nflverse-rooted: independent of the PFR scrape bloodline"

# Non-voting ancestry annotations (§16.2 generalization + §17.2.8 residual risk). These
# change NOTHING about vote collapse today; they exist so the shared-primal-record
# structure is declared before any new descendant source (nflcom) registers.
SHARED_ANCESTORS = [
    {
        "ancestor": "elias_gsis_gamebook",
        "status": "DECLARED_UNRECEIPTED",
        "voting_effect": "none",
        "members": [
            {
                "root": "pfr",
                "scope": "PFR box + player-page season tables, box-count stat families, "
                         "modern era (era boundary unadjudicated -- see open_questions)",
                "families": ["passing", "rushing", "receiving", "kicking", "punting",
                             "returns", "defense", "fumbles", "special_teams", "general"],
            },
            {
                "root": "nflverse_pbp",
                "scope": "nflverse play feed descends from NFL GSIS; box-count rollups "
                         "share the gamebook primal record class with PFR transcriptions",
                "families": ["passing", "rushing", "receiving", "kicking", "punting",
                             "returns", "defense", "fumbles", "special_teams", "general"],
            },
        ],
        "note": "pfr and nflverse_pbp stay SEPARATE roots per standing registry doctrine "
                "(§16 header); this annotation records the coordinated-lineage residual "
                "risk (§17.2.8) -- agreement between them on box counts may be inherited "
                "from one primal gamebook record. Plausibility lane + source-image "
                "arbitration is the only recourse; collapsing them is an adjudication, "
                "never a code change.",
    },
    {
        # O.8 (2026-07-26): nflcom registered. OQ-LR-1 asked which nflcom families
        # share the gamebook ancestor with PFR; it stays QUEUED and this annotation
        # is the honest interim -- nflcom is its OWN root (never a blanket collapse,
        # never a blanket independence claim), with the residual risk declared.
        "ancestor": "elias_gsis_gamebook",
        "status": "DECLARED_UNRECEIPTED",
        "voting_effect": "none",
        "members": [
            {
                "root": "nflcom",
                "scope": "NFL.com box-count stat families in the gamebook era "
                         "(player_logs / player_career / player_season / team_stats); "
                         "era boundary unadjudicated (OQ-LR-2). Ancient-era NFL.com "
                         "rows (pre-Elias) are a DIFFERENT provenance class -- likely "
                         "league-release/newspaper compilation -- and are NOT covered "
                         "by this annotation.",
                "families": ["passing", "rushing", "receiving", "kicking", "punting",
                             "returns", "defense", "fumbles", "special_teams", "general"],
            },
            {
                "root": "pfr",
                "scope": "PFR box + player-page season tables, same box-count families, "
                         "modern era",
                "families": ["passing", "rushing", "receiving", "kicking", "punting",
                             "returns", "defense", "fumbles", "special_teams", "general"],
            },
        ],
        "note": "nflcom stays a SEPARATE root from pfr pending OQ-LR-1. It cannot vote "
                "yet regardless (no licensed mappings -- the slug->pfr_id crosswalk is "
                "unbuilt), so nothing downstream depends on this call today. When the "
                "crosswalk lands, the FIRST use of nflcom must be a shared-error "
                "fingerprint study against pfr on the modern box stratum; a blanket "
                "'independent' or 'collapsed' assumption in either direction is "
                "forbidden.",
    },
]

# Escalation queue (§16.2 / O.5 contract): genuinely unclear family-scoped independence
# calls. QUEUED means no lane may resolve these implicitly.
OPEN_QUESTIONS = [
    {
        "id": "OQ-LR-1",
        "question": "nflcom box-family root assignment: NFL.com harvest exists "
                    "(nflcom_harvest.py) but no nflcom source is registered. On "
                    "registration its BOX families presumptively share the "
                    "elias_gsis_gamebook ancestor with PFR box atoms (near-copies) while "
                    "charting/derived families are independent -- which families exactly, "
                    "and does box-family agreement collapse to one root?",
        "blocker": "requires registration + shared-error fingerprint receipt (§20.5)",
        "status": "QUEUED",
    },
    {
        "id": "OQ-LR-2",
        "question": "Era boundary at which PFR box atoms become gamebook transcriptions "
                    "(Elias founded 1961, GSIS later; pre-war box provenance is "
                    "newspaper/league-release class). Affects ancestor scope only, not "
                    "current vote collapse.",
        "blocker": "needs source-provenance evidence, not code",
        "status": "QUEUED",
    },
    {
        "id": "OQ-LR-3",
        "question": "Platform feeds (Yahoo/Sleeper/ESPN NFL stat feeds) as candidate "
                    "roots: they may share upstream providers with each other and with "
                    "GSIS. No votes until independence is proven empirically.",
        "blocker": "shared-error fingerprint study (§20.5, §25.5 corrected §20.1)",
        "status": "QUEUED",
    },
    {
        "id": "OQ-LR-5",
        "question": "Composite-bundle root fidelity: the ancient ready bundle's rows "
                    "carry row-level lineage (bundle_source/data_source incl. "
                    "newspapers_com_ocr, loc_newspaper_ocr, pfr recovery lanes, "
                    "pbp-1978) but the single registered source resolved ALL rows to "
                    "pfa_loc -- ancient-era root diversity miscounted in both "
                    "directions.",
        "status": "RESOLVED",
        "resolution": "2026-07-26: row-level lineage beats source-level lineage. The "
                      "composite is registered as per-stream sources, each scoped by a "
                      "mandatory bundle_source/data_source filter declared in the "
                      "source note: ancient_pfa_gamelog (pfa_loc root, through_1978 "
                      "bundle, 10,320 rows 1920-54), ancient_pfr_recovery (pfr root, "
                      "19,887 rows 1920-78), ancient_pbp1978_recovery (pbp_merged "
                      "era-split -> pfr root, 17,129 rows 1978-79), "
                      "ancient_newspaper_ocr (newspaper root, non-voting, 133 rows "
                      "1920-39). Mixed-lineage rows (multiple data_source roots, ~6 "
                      "pfa_scoring_summary+loc rows) are multi-root corroborated and "
                      "carry NO single-root credit. EVIDENCE FINDING queued to Joe: "
                      "the upstream ancient_upsert_bundle input dir is EMPTY; the "
                      "2026-07-18 through_1979 regen silently dropped the entire "
                      "pfa_player_gamelog stream (10,320 rows) -- surviving snapshot "
                      "= through_1978 bundle (registered), rebuild-or-promote "
                      "decision is Joe's (deletion discipline).",
    },
    {
        "id": "OQ-LR-4",
        "question": "consensus_reconcile 'idp' witness kind sums the SUBJECT's own rows "
                    "(v26 IDP detail) as a consensus participant -- under LOROO the "
                    "subject may not vote for itself. Re-type as internal-consistency "
                    "check or drop from the >=2-agreement count?",
        "blocker": "apply-lane semantics change; needs adjudication before touching "
                   "shipped reconcile behavior",
        "status": "QUEUED",
    },
]


def _assignments(src) -> list[dict]:
    """Era/family-scoped root assignments for one source. families ['*'] = all families
    the source witnesses; per-family splits appear only when a receipted call exists."""
    if src.lineage == "pbp_merged":
        out = []
        if src.year_min < PBP_SPLIT_YEAR:
            out.append({
                "families": ["*"],
                "year_min": src.year_min,
                "year_max": min(src.year_max, PBP_SPLIT_YEAR - 1),
                "root": "pfr",
                "basis": _PBP_PRE_BASIS,
            })
        if src.year_max >= PBP_SPLIT_YEAR:
            out.append({
                "families": ["*"],
                "year_min": max(src.year_min, PBP_SPLIT_YEAR),
                "year_max": src.year_max,
                "root": "nflverse_pbp",
                "basis": _PBP_POST_BASIS,
            })
        return out
    return [{
        "families": ["*"],
        "year_min": src.year_min,
        "year_max": src.year_max,
        "root": _LINEAGE_TO_ROOT[src.lineage],
        "basis": f"registry lineage '{src.lineage}' (mandatory-explicit, O.5)",
    }]


def generate() -> dict:
    reg = registry(include_subject=True)
    sources = []
    for sid, src in reg.items():
        sources.append({
            "source_id": sid,
            "lineage": src.lineage,
            "witness_class": src.witness_class,
            "assignments": _assignments(src),
        })
    return {
        "version": "v1",
        "n_sources": len(sources),
        "roots": list(ROOTS),
        "pbp_split_year": PBP_SPLIT_YEAR,
        "sources": sources,
        "shared_ancestors": SHARED_ANCESTORS,
        "open_questions": OPEN_QUESTIONS,
    }


def load() -> dict:
    with open(CONTRACTS_PATH, encoding="utf-8") as f:
        return json.load(f)


def validate(doc: dict) -> list[str]:
    problems: list[str] = []
    reg = registry(include_subject=True)
    seen = {e["source_id"] for e in doc["sources"]}
    if seen != set(reg):
        problems.append(
            f"contract/registry mismatch: missing={sorted(set(reg) - seen)} "
            f"extra={sorted(seen - set(reg))}")
    legal_roots = set(doc["roots"])
    for e in doc["sources"]:
        sid = e["source_id"]
        src = reg.get(sid)
        asg = e["assignments"]
        if not asg:
            problems.append(f"{sid}: no assignments")
            continue
        for a in asg:
            if a["root"] not in legal_roots:
                problems.append(f"{sid}: root {a['root']!r} not in vocabulary")
            if not a.get("basis"):
                problems.append(f"{sid}: assignment without basis")
        if src is None:
            continue
        # '*'-family windows must tile [year_min, year_max] exactly: no gap, no overlap.
        stars = sorted((a for a in asg if a["families"] == ["*"]),
                       key=lambda a: a["year_min"])
        if stars:
            if stars[0]["year_min"] != src.year_min or stars[-1]["year_max"] != src.year_max:
                problems.append(f"{sid}: '*' windows do not span source years "
                                f"{src.year_min}-{src.year_max}")
            for prev, nxt in zip(stars, stars[1:]):
                if nxt["year_min"] != prev["year_max"] + 1:
                    problems.append(f"{sid}: '*' windows gap/overlap at "
                                    f"{prev['year_max']}->{nxt['year_min']}")
    return problems


@functools.lru_cache(maxsize=1)
def _index() -> dict[str, list[dict]]:
    return {e["source_id"]: e["assignments"] for e in load()["sources"]}


def root_of(source_id: str, year: int, family: str = "*") -> str:
    """Resolve the independent-root identity of (source, stat-family, year).

    THE resolver for every voting lane -- lanes must not hardcode era splits. Family
    match: an assignment scoped ['*'] covers every family; explicit family lists win
    over '*'. Years outside every window clamp to the nearest window (data rows outside
    a source's declared span are their own K-plane finding, not a root question)."""
    asg = _index()[source_id]
    fam_matches = [a for a in asg if family in a["families"]] or \
                  [a for a in asg if a["families"] == ["*"]]
    for a in fam_matches:
        if a["year_min"] <= year <= a["year_max"]:
            return a["root"]
    fam_matches.sort(key=lambda a: a["year_min"])
    return (fam_matches[0] if year < fam_matches[0]["year_min"] else fam_matches[-1])["root"]


def main() -> int:
    doc = generate()
    problems = validate(doc)
    with open(CONTRACTS_PATH, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2)
    _index.cache_clear()
    from collections import Counter
    roots = Counter(a["root"] for e in doc["sources"] for a in e["assignments"])
    split = [e["source_id"] for e in doc["sources"] if len(e["assignments"]) > 1]
    print(f"lineage_roots.v1.json: {doc['n_sources']} sources -> {CONTRACTS_PATH}")
    print("assignment roots:", dict(roots))
    print("era-split sources:", split)
    print(f"open questions queued: {len(doc['open_questions'])}")
    if problems:
        print("VALIDATION PROBLEMS:")
        for p in problems:
            print("  -", p)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
