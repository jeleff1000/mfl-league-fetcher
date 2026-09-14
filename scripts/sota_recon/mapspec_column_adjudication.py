"""Import the EXISTING MapSpecs into the column ledger. They were already decided.

JOE ASKED "are you sure pfr isn't fully mapped already?" -- and the answer was no, it is
not, but it is a great deal more mapped than the dossier said. 189 of the 943 open pfr rows
already carry a MapSpec in `witness_map.WITNESS_MAP`: a committed, reviewed decision naming
the exact v26 column that source column becomes. The dossier never consulted it.

THIS IS THE §18 SPINE COMPLAINT, INSIDE THE TOOL BUILT TO FIX IT. Two registries hold the
same fact in two key spaces -- `witness_map` on (source_key, source_col) and the dossier on
(source, table, column) -- and nothing joined them. The consequence is not cosmetic: the
next pass over pfr would have hand-written a correspondence table for 189 columns that are
already decided, and any entry that disagreed would have silently contradicted a receipted
mapping rather than failing loudly.

SO THIS PASS ARGUES NOTHING. It is a JOIN, not an adjudication. Every decision it writes is
the MapSpec's own `v26_col`, with the MapSpec cited as evidence. That is why it is safe to
run in bulk where a name-matching pass would not be: nobody is inferring anything from a
name, we are reading a decision someone already made and recording it where the dossier can
see it.

IDENTITY, NOT LICENSING. 201 of the 256 MapSpecs carry no validation referee and 42 carry
only a year range; 13 carry both. That variation is deliberately IGNORED here, because a
MapSpec states what a column IS -- which is exactly MAPPED_TO_CANONICAL -- while validation
depth governs whether the source may VOTE. Importing a thinly-validated MapSpec as an
identity decision overclaims nothing; the licensing gate is elsewhere and unaffected.

ORPHANS ARE REPORTED, NEVER SILENT. A MapSpec whose (source_key, source_col) matches no
dossier row at all is a mapping pointing at a column that does not exist -- either the
source changed shape or the spec has a typo. Those are counted and listed rather than
skipped, because a join that quietly drops its misses is how the two registries diverged in
the first place.

Run:  python -m scripts.sota_recon.mapspec_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger

GENERATOR = "scripts.sota_recon.mapspec_column_adjudication"


def build_decisions() -> tuple[list[dict], dict]:
    from . import witness_map as WM

    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    v26 = _v26_columns()

    # every dossier row keyed the way a MapSpec addresses it: (source, column). One source
    # column can appear under several TABLE keys (pfr `blitzes` sits in adv_defense and in
    # box_defense_advanced), and the MapSpec decides the COLUMN, so it fans out to each.
    rows_by_pair: dict[tuple[str, str], list[dict]] = collections.defaultdict(list)
    for row in dossier["rows"]:
        rows_by_pair[(row["source"], row["column"])].append(row)

    from .column_dossier import load_decisions
    existing = load_decisions()
    mine = {k for k, e in existing.items() if e.get("generated_by") == GENERATOR}

    decisions: list[dict] = []
    tally = {"mapped": 0, "already_decided_elsewhere": 0, "orphan_specs": 0,
             "canonical_not_in_v26": 0}
    orphans: list[str] = []
    bad_target: list[str] = []
    conflicts: list[str] = []
    seen: dict[str, str] = {}

    for spec in WM.WITNESS_MAP:
        if not spec.source_col or not spec.v26_col:
            continue
        matches = rows_by_pair.get((spec.source_key, spec.source_col))
        if not matches:
            tally["orphan_specs"] += 1
            orphans.append(f"{spec.source_key}|{spec.source_col} -> {spec.v26_col}")
            continue
        if spec.v26_col not in v26:
            tally["canonical_not_in_v26"] += 1
            bad_target.append(f"{spec.source_key}|{spec.source_col} -> {spec.v26_col}")
            continue
        for row in matches:
            key = row_key(row["source"], row["table_key"], row["column"])
            # TWO SPECS DISAGREEING about one column is a real contradiction between
            # committed mappings and must surface, not be resolved by last-write-wins.
            if key in seen and seen[key] != spec.v26_col:
                conflicts.append(f"{key}: {seen[key]} vs {spec.v26_col}")
                continue
            seen[key] = spec.v26_col
            if row["disposition"] != "OPEN" and key not in mine:
                tally["already_decided_elsewhere"] += 1
                continue
            grain = f", grain {spec.grain!r}" if spec.grain else ""
            validated = ""
            if spec.validation_referee:
                validated = (f", validated against {spec.validation_referee!r}"
                             f"{' ' + str(spec.validation_years) if spec.validation_years else ''}")
            elif spec.validation_years:
                validated = f", validation years {spec.validation_years}"
            decisions.append({
                "key": key,
                "disposition": "MAPPED_TO_CANONICAL",
                "canonical": spec.v26_col,
                "reason": f"a committed MapSpec already decides this column: "
                          f"{spec.source_key}.{spec.source_col} -> {spec.v26_col}"
                          f"{grain}. Imported, not re-argued -- the decision predates this "
                          f"pass and lives in witness_map",
                "evidence": f"witness_map.WITNESS_MAP MapSpec "
                            f"({spec.source_key}, {spec.source_col}) -> {spec.v26_col}"
                            f"{grain}{validated}",
            })
            tally["mapped"] += 1

    if conflicts:
        raise SystemExit("two MapSpecs disagree about one dossier row:\n  "
                         + "\n  ".join(sorted(set(conflicts))[:20]))
    return decisions, tally | {"orphans": sorted(orphans),
                               "canonical_not_in_v26_detail": sorted(bad_target)}


def escalated_row_keys() -> dict[str, str]:
    """This pass escalates nothing: it imports decisions rather than making them."""
    return {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    for name, value in tally.items():
        if not isinstance(value, list):
            print(f"  {name:28s} {value}")
    if tally["orphans"]:
        print(f"\nORPHAN MapSpecs -- point at a (source, column) no dossier row produces "
              f"({len(tally['orphans'])}):")
        for name in tally["orphans"][:25]:
            print("   -", name)
    if tally["canonical_not_in_v26_detail"]:
        print("\nMapSpecs whose v26 target does not exist:")
        for name in tally["canonical_not_in_v26_detail"]:
            print("   -", name)
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
