"""THE CHAIN, end to end, in one place: source -> column -> witness -> vote.

WHY THIS EXISTS. Every number below already existed, scattered across the scoreboard, the
column dossier, the witness map, the crosswalk receipts and the root-diversity map. What
did not exist was the JOIN between them -- so the honest answer to "where are we?" required
opening five artifacts and doing arithmetic by hand, and the answer that came back was
whichever stage the reader happened to look at. That is the §18 spine complaint applied to
the program's own progress reporting.

This is NOT a new analytical lens and NOT a new queue (Law 2). It reads the existing
counters and reports the chain they form, regenerating from scratch every run.

THE SIX STAGES, and the question each answers:

  1 SOURCES     is everything on disk registered and dispositioned?
  2 LINKAGE     of every physical column of every source, how many are tied to a canonical?
  3 DEPTH       per SUPERTABLE COLUMN, how many independent LINEAGE ROOTS can witness it?
  4 LICENSING   of that depth, how much may actually VOTE today?
  5 ADDITIONS   what material do we hold that the supertable has no column for?
  6 NEWSPAPER   the deepest-era backfill lane, tracked separately because it is the only
                lineage that is primary rather than derived for the ancient era.

THE ONE THAT MATTERS IS 3 vs 4, and reading either alone is misleading in opposite
directions. Stage 3 says 99.8% of supertable columns have SOME source column mapped to
them, which sounds finished. Stage 4 says only a fraction may vote. Neither number is the
answer; the DIFFERENCE between them is, because it is exactly the work that is adjudicated
and not yet licensed.

COUNTED BY ROOT, NOT BY SOURCE. Two tables of the same bloodline are ONE witness -- pfr
season tables and pfr box scores agreeing proves the parser works, not that the number is
right. Every depth figure here is distinct LINEAGE ROOTS, and the source count is reported
beside it only to show how much of the apparent breadth collapses.

    python -m scripts.sota_recon.closure_chain
"""
from __future__ import annotations

import collections
import json
from pathlib import Path

RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "closure-chain.json"
DOSSIER = Path(__file__).resolve().parents[2] / "docs" / "column-dossier.json"
COMPOSITE_RECEIPT = (
    Path(__file__).resolve().parents[2] / "docs" / "composite-witness-lane-receipt.json"
)


def receipt_obligations(receipt_path: Path):
    """Expose the composite lane's strictly validated, type-separated obligations."""
    from .composite_column_adjudication import receipt_obligations as _obligations

    return _obligations(receipt_path)


# ---- THE DENOMINATOR OF STAGE 3, MADE EXPLICIT --------------------------------------
#
# Stage 3 counted against `latest_v26()` alone -- the 1,077-column weekly release -- and
# reported "1,075 of 1,077 have a mapped source column", which reads like the job is
# finished. It is the wrong base in BOTH directions at once:
#
#   TOO SMALL  141 registered stats are absent from the weekly release entirely (56 lamar,
#              42 honors_bio, 21 context, 12 general, ...). A column cannot be counted as
#              covered or uncovered if the base never contains it.
#   TOO LARGE  849 of the 1,218 registered stats are OUR OWN COMPUTATION -- ranks over our
#              own pool, league fantasy-scoring variants, per-game derivations, LAMAR,
#              pipeline provenance, identity locators. No external publisher can ever
#              witness one, so counting them inflates the base against which external
#              coverage is measured, and a percentage over that base cannot answer the
#              SOTA question at all.
#
# Families are the registry's OWN classification, so this split is auditable rather than
# asserted: change one line here and the composition table below moves with it.
#
# The 554 figure quoted in the finish-line plan is NOT carried forward. It could not be
# reproduced from the registry under this rule or any near variant, and an inherited number
# that will not reproduce is exactly what must not be republished.
OUR_OWN_COMPUTATION_FAMILIES = {
    "rank": "our ranking of players against our own pool -- no publisher ranks our universe",
    "fantasy_points": "league scoring variants we apply; the source publishes the inputs, "
                      "never our weighted sum",
    "ppg": "our per-game derivation over our own games-played denominator",
    "lamar": "our own value metric",
    "provenance": "how the row got here -- pipeline bookkeeping, not a measurement",
    "identity": "locators. Their obligation is the crosswalk lane (§19.2), not the "
                "mapping lane",
}


def denominators(v26: set[str], stats: list[dict]) -> dict:
    """The three bases, side by side, with the composition that separates them."""
    by_family = collections.Counter(s["family"] for s in stats)
    core = {s["canonical_name"] for s in stats
            if s["family"] not in OUR_OWN_COMPUTATION_FAMILIES}
    absent = collections.Counter(
        s["family"] for s in stats if s["canonical_name"] not in v26)
    return {
        "weekly_release_columns": len(v26),
        "full_stat_registry": len(stats),
        "externally_witnessable_core": len(core),
        "our_own_computation": len(stats) - len(core),
        "registered_but_absent_from_the_weekly_release": sum(absent.values()),
        "absent_by_family": dict(absent.most_common()),
        "family_composition": {
            family: {"stats": count,
                     "class": ("OUR_OWN_COMPUTATION"
                               if family in OUR_OWN_COMPUTATION_FAMILIES
                               else "EXTERNALLY_WITNESSABLE"),
                     "why": OUR_OWN_COMPUTATION_FAMILIES.get(family, "")}
            for family, count in by_family.most_common()},
        "core_column_names": sorted(core),
    }


def build() -> dict:
    import duckdb

    from .column_dossier import load_decisions
    from .sources import latest_v26, registry
    from . import witness_map as WM
    from .test_mapping_obligation import (LANE_WITNESSED, MAPPING_PENDING,
                                          NON_MAPPING_ROLES)

    dossier = json.loads(DOSSIER.read_text(encoding="utf-8"))
    decisions = load_decisions()
    sources = registry(include_subject=True)
    lineage_of = {key: spec.lineage for key, spec in sources.items()}

    connection = duckdb.connect()
    try:
        v26 = {row[0] for row in connection.execute(
            f"DESCRIBE SELECT * FROM read_parquet('{latest_v26()}')").fetchall()}
    finally:
        connection.close()

    contracts_path = (Path(__file__).resolve().parent / "witness_gate" / "contracts"
                      / "stat_contracts.v1.json")
    stats = json.loads(contracts_path.read_text(encoding="utf-8"))["stats"]
    stage_denominators = denominators(v26, stats)
    core = set(stage_denominators.pop("core_column_names"))

    # ---- 1 sources -------------------------------------------------------------------
    mapspec_sources = {m.source_key for m in WM.WITNESS_MAP}
    pending = set(MAPPING_PENDING)
    stage_sources = {
        "registered": len(sources),
        "enumerated_in_dossier": dossier["counters"]["sources_enumerated"],
        "unreadable": dossier["counters"]["sources_unreadable"],
        "with_a_mapspec": len(mapspec_sources & set(sources)),
        "mapping_pending": len(pending & set(sources)),
        "lane_witnessed_note": len(set(LANE_WITNESSED) & set(sources)),
        "non_mapping_role": sum(1 for s in sources.values()
                                if s.role in NON_MAPPING_ROLES),
    }

    # ---- 2 linkage -------------------------------------------------------------------
    dispositions = collections.Counter(v["disposition"] for v in decisions.values())
    stage_linkage = dict(dispositions) | {
        "closed_by_bulk_rule": dossier["counters"]["closed_by_rule"],
        "open": dossier["counters"]["column_dossier_open_rows"],
        "dossier_rows": dossier["counters"]["dossier_rows"],
    }

    # ---- 3 depth ---------------------------------------------------------------------
    # WIDENED to the whole registry, not just the weekly release. The old filter dropped
    # any mapping whose canonical is registered but absent from the release file -- all 42
    # honors_bio canonicals among them -- so an adjudicated witness for `hof` or
    # `draft_round` counted as nothing at all.
    registry_names = {s["canonical_name"] for s in stats}
    roots_per_column: dict[str, set[str]] = collections.defaultdict(set)
    sources_per_column: dict[str, set[str]] = collections.defaultdict(set)
    for key, entry in decisions.items():
        if entry["disposition"] != "MAPPED_TO_CANONICAL":
            continue
        canonical = entry.get("canonical")
        if canonical not in registry_names and canonical not in v26:
            continue
        source = key.split("|", 1)[0]
        sources_per_column[canonical].add(source)
        roots_per_column[canonical].add(lineage_of.get(source, "UNKNOWN"))

    # Composite receipt outputs enter depth only through current PASS scalar observations.
    # Constraints and internal resolutions are reported beside depth but never become a
    # source/root for a canonical component.
    scalar_observations, constraints, internal_resolutions = receipt_obligations(
        COMPOSITE_RECEIPT
    )
    composite_scalar_sources: dict[str, set[str]] = collections.defaultdict(set)
    for observation in scalar_observations:
        if observation.source not in lineage_of or observation.source in pending:
            continue
        for canonical in observation.targets:
            if canonical not in registry_names and canonical not in v26:
                continue
            sources_per_column[canonical].add(observation.source)
            roots_per_column[canonical].add(lineage_of[observation.source])
            composite_scalar_sources[canonical].add(observation.source)

    # THE ONE THAT ANSWERS THE SOTA QUESTION. Against the weekly release the figure is
    # 1,075 of 1,077 and reads as finished; against the columns an external publisher could
    # actually witness, it is the number that moves when pbp and pfr close, and it moves
    # 0 -> 1 rather than 1 -> 2.
    core_witnessed = {name for name in roots_per_column if name in core}
    core_external = {name for name, roots in roots_per_column.items()
                     if name in core and roots - {"internal"}}

    root_depth = collections.Counter(
        len(v) for k, v in roots_per_column.items() if k in v26)
    stage_depth = {
        "supertable_columns": len(v26),
        "columns_with_a_mapped_source_column": len(set(roots_per_column) & v26),
        "columns_with_no_adjudicated_witness_YET": len(v26 - set(roots_per_column)),
        # ---- the same question asked against the base that can answer it ----
        "core_columns_externally_witnessable": len(core),
        "core_columns_with_any_adjudicated_witness": len(core_witnessed),
        "core_columns_with_a_NON_INTERNAL_witness": len(core_external),
        "core_columns_with_zero_external_witness": len(core - core_external),
        # NOT "columns nothing can witness". A canonical with no witness here means no
        # dossier row has been ADJUDICATED to it yet, not that no source holds it. Source
        # names differ from canonical names ~92% of the time on transcribed sources, so
        # searching for the canonical name among open columns finds almost nothing and
        # proves less: `def_tackles_missed` is open as pfr `tackles_missed`,
        # `defense_snaps` as pfr `defense`, `def_knockdowns` as pfr `qb_knockdown`, and 12
        # def_* EPA/success/plays columns are open in pbp_merged under their exact names.
        "by_distinct_lineage_roots": {str(k): v for k, v in sorted(root_depth.items())},
        "single_root_columns": root_depth.get(1, 0),
        "cross_examinable_adjudicated": sum(v for k, v in root_depth.items() if k >= 2),
        "external_constraint_observations": len(constraints),
        "external_constraint_target_columns": len({
            target for constraint in constraints for target in constraint.targets
        }),
        "internal_resolution_observations": len(internal_resolutions),
        "internal_resolution_target_columns": len({
            target for resolution in internal_resolutions for target in resolution.targets
        }),
        "composite_scalar_sources_per_column": {
            canonical: sorted(sources)
            for canonical, sources in sorted(composite_scalar_sources.items())
        },
    }

    # ---- 4 licensing -----------------------------------------------------------------
    licensed_roots: dict[str, set[str]] = collections.defaultdict(set)
    for canonical, srcs in sources_per_column.items():
        for source in srcs:
            if source in mapspec_sources and source not in pending:
                licensed_roots[canonical].add(lineage_of.get(source, "UNKNOWN"))
    licensed_depth = collections.Counter(len(v) for v in licensed_roots.values())
    cross_today = sum(v for k, v in licensed_depth.items() if k >= 2)
    stage_licensing = {
        "columns_with_a_licensed_root": len(licensed_roots),
        "columns_with_a_mapped_but_unlicensed_root":
            len(roots_per_column) - len(licensed_roots),
        "by_licensed_roots": {str(k): v for k, v in sorted(licensed_depth.items())},
        "cross_examinable_today": cross_today,
        "adjudicated_but_not_licensed":
            stage_depth["cross_examinable_adjudicated"] - cross_today,
    }

    # ---- 5 additions -----------------------------------------------------------------
    candidates = {k: v for k, v in decisions.items()
                  if v["disposition"] == "NEW_SUPERTABLE_COLUMN_CANDIDATE"}
    concepts = {(k.split("|", 1)[0].split("_")[0], k.rsplit("|", 1)[-1])
                for k in candidates}
    stage_additions = {
        "candidate_rows": len(candidates),
        "distinct_concepts": len(concepts),
        "by_source": dict(collections.Counter(
            k.split("|", 1)[0] for k in candidates).most_common()),
    }

    # ---- 6 newspaper -----------------------------------------------------------------
    paper = [r for r in dossier["rows"] if r["lineage"] == "newspaper"]
    paper_open = [r for r in paper if r["disposition"] == "OPEN"]
    stage_newspaper = {
        "dossier_rows": len(paper),
        "open": len(paper_open),
        "distinct_stat_name_concepts_open": len({r["column"] for r in paper_open}),
        "by_source": dict(collections.Counter(
            r["source"] for r in paper_open).most_common()),
    }

    return {
        "generated": "the closure chain: source -> column -> witness -> vote",
        "law": "depth is counted by LINEAGE ROOT, never by source -- two tables of one "
               "bloodline are one witness",
        "stage_0_denominators": stage_denominators,
        "stage_1_sources": stage_sources,
        "stage_2_linkage": stage_linkage,
        "stage_3_depth": stage_depth,
        "stage_4_licensing": stage_licensing,
        "stage_5_additions": stage_additions,
        "stage_6_newspaper": stage_newspaper,
        "binding_constraint": (
            "stage 4. "
            f"{stage_depth['columns_with_a_mapped_source_column']} of "
            f"{stage_depth['supertable_columns']} supertable columns have a source column "
            f"mapped to them, but only {stage_licensing['columns_with_a_licensed_root']} "
            f"have a LICENSED root and only {cross_today} can be cross-examined today. "
            f"{stage_licensing['adjudicated_but_not_licensed']} columns are adjudicated to "
            "two or more independent roots and still cannot vote."),
        "read_this_one_instead": (
            f"'{stage_depth['columns_with_a_mapped_source_column']} of "
            f"{stage_depth['supertable_columns']}' is measured against a base that cannot "
            "answer the SOTA question: 849 of the 1,218 registered stats are our own "
            "computation and no publisher can witness one. Against the "
            f"{stage_depth['core_columns_externally_witnessable']} columns an external "
            "publisher COULD witness, "
            f"{stage_depth['core_columns_with_zero_external_witness']} still have ZERO "
            "external witness adjudicated. That is the number the pbp and pfr passes move, "
            "and they move it 0 -> 1."),
    }


def write() -> dict:
    document = build()
    RECEIPT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def main() -> int:
    d = write()
    for stage in ("stage_0_denominators", "stage_1_sources", "stage_2_linkage",
                  "stage_3_depth", "stage_4_licensing", "stage_5_additions",
                  "stage_6_newspaper"):
        print(f"\n{stage.replace('_', ' ').upper()}")
        for name, value in d[stage].items():
            if isinstance(value, dict):
                print(f"  {name}:")
                for k, v in value.items():
                    print(f"      {k:38s} {v:,}" if isinstance(v, int)
                          else f"      {k}: {v}")
            else:
                print(f"  {name:44s} {value:,}" if isinstance(value, int)
                      else f"  {name}: {value}")
    print(f"\nBINDING CONSTRAINT: {d['binding_constraint']}")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
