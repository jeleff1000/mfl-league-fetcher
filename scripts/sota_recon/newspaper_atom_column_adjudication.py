"""O.9.3 burn-down: the newspaper player-cell atoms that were already decided.

STEP 0 FOUND THIS ONE. `newspaper_witness_common` has carried a canonicalisation table
since the sidecar bundle was built -- `STAT_CELL_ATOM_MAP` (the raw stat_name strings the
reviewers actually typed, folded onto a canonical atom) composed with `ATOM_TO_V26_COL`
(atom -> weekly v26 column). It is the same shape as the 189 pfr MapSpecs the dossier had
never consulted: a committed decision in one key space, and a queue asking the same question
in another, with nothing joining them.

SO THIS PASS ARGUES NOTHING EITHER. It is a JOIN. Every decision it writes is the existing
table's own target, cited as evidence.

WHY IT IS 42 AND NOT 22. The finish-line plan says "the 22 newspaper atoms in
ATOM_TO_V26_COL". Twenty-two is the count of DISTINCT ATOMS, and it undercounts the rows it
closes, because `newspaper_player_cells` is a LONG source: its dossier rows are the
stat_name STRINGS the reviewers wrote, and the synonym layer folds several onto one atom --
`rush_attempts`, `rushing_attempts` and `carries` are three open rows and one atom. Measured
against the dossier rather than against the map: 42 open rows resolve. Counting the map's
keys instead of the rows they close is the same deflation as counting sources instead of
columns, one layer down.

SCOPED TO `newspaper_player_cells`, AND THE SCOPE IS THE DECISION. `newspaper_team_claims`
and `newspaper_team_stats` are LONG in the same way and eight of their open stat_names are
spelled identically to atoms in this map -- `passing_yards`, `fumbles`, `punts`,
`def_interceptions` among them. They are NOT imported. The map is a PLAYER-CELL vocabulary,
and the same string at team grain claims a different fact: a team's passing yards is not a
player's, and v26's `passing_yards` is a player-week column. Sweeping them in because the
strings match would be reading meaning off a name at exactly the moment the grain changed
underneath it.

`newspaper_scoring_events` is not imported either, and for a different reason worth
recording: `EVENT_TYPE_MAP` looks like a second ready-made import, but that source is WIDE
in the dossier -- its rows are columns like `event_type` and `scoring_team`, not the
event_type VALUES the map is keyed on. The two registries do not meet. Checked, measured
zero overlap, left alone.

Run:  python -m scripts.sota_recon.newspaper_atom_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import json

from .column_dossier import row_key
from .newspaper_witness_common import ATOM_TO_V26_COL, STAT_CELL_ATOM_MAP
from .nflcom_column_adjudication import DOSSIER_PATH, _v26_columns, apply_to_ledger

GENERATOR = "scripts.sota_recon.newspaper_atom_column_adjudication"

# The one source whose stat_name vocabulary this map was authored for. A dict rather than a
# bare string because the scope IS a decision -- see the module docstring on the eight
# team-grain rows that spell identically and are deliberately not swept.
SOURCES = {
    "newspaper_player_cells":
        "the sidecar's weekly PLAYER stat-cell table; STAT_CELL_ATOM_MAP was authored "
        "against its stat_name vocabulary and no other",
}


def build_decisions() -> tuple[list[dict], dict]:
    from .column_dossier import load_decisions

    v26 = _v26_columns()
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    existing = load_decisions()
    mine = {key for key, entry in existing.items()
            if entry.get("generated_by") == GENERATOR}

    decisions: list[dict] = []
    unmapped: list[str] = []
    bad_target: list[str] = []
    for row in dossier["rows"]:
        if row["source"] not in SOURCES:
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        atom = STAT_CELL_ATOM_MAP.get(row["column"])
        if atom is None:
            unmapped.append(row["column"])
            continue
        canonical = ATOM_TO_V26_COL.get(atom)
        # An atom the second table does not carry would be a half-decided mapping, and
        # writing it would point a decision at nothing. Reported, never guessed through.
        if canonical is None or canonical not in v26:
            bad_target.append(f"{row['column']} -> atom {atom} -> {canonical}")
            continue
        synonym = "" if row["column"] == atom else (
            f" The stored string {row['column']!r} is a reviewer synonym the map folds "
            f"onto atom {atom!r}.")
        decisions.append({
            "key": key,
            "disposition": "MAPPED_TO_CANONICAL",
            "canonical": canonical,
            "reason": f"the newspaper sidecar's own read-side canonicalisation already "
                      f"decides this stat_name: {row['column']!r} -> atom {atom!r} -> v26 "
                      f"{canonical!r}. Imported, not re-argued -- the decision predates "
                      f"this pass and lives in newspaper_witness_common.{synonym}",
            "evidence": f"newspaper_witness_common.STAT_CELL_ATOM_MAP[{row['column']!r}] "
                        f"= {atom!r}; ATOM_TO_V26_COL[{atom!r}] = {canonical!r}. The "
                        f"bundle stores reviewer vocabulary VERBATIM (that is part of the "
                        f"evidence) and canonicalises read-side only, so this table is the "
                        f"single place the register script and the recon lane agree",
        })
    return decisions, {
        "mapped": len(decisions),
        "left_open_not_in_the_atom_map": len(unmapped),
        "atoms_with_no_v26_target": len(bad_target),
        "atoms_with_no_v26_target_detail": sorted(set(bad_target)),
        "unmapped_sample": sorted(set(unmapped))[:30],
    }


def escalated_row_keys() -> dict[str, str]:
    """Nothing refused here -- but the eight team-grain look-alikes ARE a question, and
    they belong to whoever adjudicates the team-grain newspaper sources, not to an import
    pass. Filing them here would attribute a refusal to a pass that never examined them."""
    return {}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    for name, value in tally.items():
        if not isinstance(value, list):
            print(f"  {name:32s} {value}")
    if tally["atoms_with_no_v26_target_detail"]:
        print("\nATOMS WITH NO v26 TARGET (half-decided -- not written):")
        for name in tally["atoms_with_no_v26_target_detail"]:
            print("   -", name)
    print(f"\nleft OPEN, not in the atom map (sample): {tally['unmapped_sample']}")
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
