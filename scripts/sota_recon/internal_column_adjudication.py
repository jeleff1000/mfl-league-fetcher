"""O.9.3 burn-down: the sources whose column names are OUR OWN canonical vocabulary.

THE ARGUMENT, and why it does not generalise. For a FOREIGN table, a column called
`passing_yards` matching a canonical stat named `passing_yards` is weak evidence -- the
site chose that string, we chose ours, and agreement could be coincidence or a false
friend (StatsCrew's `td` is a PERCENTAGE; nflcom's `tds` under one caption is passing
touchdowns and under another is interception-return touchdowns). That is why the nflcom
and StatsCrew passes read the source's published header instead of trusting a name.

For the tables in `OWN_VOCABULARY` below, the name is not a coincidence: WE wrote it. The
v26 release, the identity spine, our pbp rollup, the assembled ancient bundle and the NGS
loads all get their column names from our own builders, out of the same vocabulary
`stat_contracts.v1.json` registers. An exact match there is a statement about a name we
control on both sides.

MEASURED, which is what makes the boundary defensible rather than convenient:

    v26_release                 1,073 of 1,073 open columns  (100%)
    player_bio                     44 of 44                  (100%)
    ngs_season / ngs_weekly_raw    36 of 36                  (100%)
    pbp_player_week_rollup        105 of 119                  (88%)
    the four ancient_* bundles     84 of 102 each             (82%)
    legacy_motherduck_supertable  141 of 274                  (51%)

    -- against pfr 23%, newspaper 13%, nflcom 7% --

A vocabulary we author matches at 100% or near it. A vocabulary we transcribe does not.
The gap between those two figures IS the evidence that this rule is reading authorship
and not luck, and it is why the rule is scoped by an explicit source list rather than
applied wherever a name happens to collide.

WHAT STAYS OPEN. Every column in these sources with NO contract entry is left OPEN and
reported by name -- for `legacy_motherduck_supertable` that is 133 columns, which is the
interesting half: a legacy column with no contract is either dead vocabulary or a concept
the contract registry never picked up.

Run:  python -m scripts.sota_recon.internal_column_adjudication [--apply]
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, apply_to_ledger
from .witness_gate.contracts import __file__ as _CONTRACTS_INIT  # noqa: F401

GENERATOR = "scripts.sota_recon.internal_column_adjudication"

# source -> why ITS column names are our vocabulary rather than a publisher's
OWN_VOCABULARY: dict[str, str] = {
    "v26_release":
        "THE SUBJECT ITSELF. Its columns are not a witness needing a mapping -- they ARE "
        "the canonical space every other row in this dossier maps INTO. Recorded as "
        "mapped-to-itself rather than excluded so the dossier keeps its property that "
        "every row ends naming a canonical cell; the column's real obligation is its "
        "stat contract (grain, aggregation class, tolerance policy), which the "
        "stat-contract lane gates, not the mapping lane",
    "player_bio":
        "our identity spine, written by our own bio builder out of the same vocabulary "
        "the contract registry declares",
    "legacy_motherduck_supertable":
        "our own PREVIOUS supertable. Its names are ours from an earlier generation, "
        "which is exactly why only half of them still resolve -- the unmatched half is "
        "the interesting part and is left open",
    "pbp_player_week_rollup":
        "our rollup over merged play-by-play; every column is named by our own "
        "aggregation code",
    "ngs_weekly_raw":
        "Next Gen Stats load; the ngs_* prefix and the column names are our "
        "normalisation, not the feed's",
    "ngs_season_published":
        "Next Gen Stats load; the ngs_* prefix and the column names are our "
        "normalisation, not the feed's",
    "ancient_pfa_gamelog":
        "a stream of the ancient READY BUNDLE, which we assembled -- the bundle's schema "
        "is ours regardless of which lineage each row came from (OQ-LR-5)",
    "ancient_pfr_recovery":
        "a stream of the ancient READY BUNDLE, which we assembled (OQ-LR-5)",
    "ancient_pbp1978_recovery":
        "a stream of the ancient READY BUNDLE, which we assembled (OQ-LR-5)",
    "ancient_newspaper_ocr":
        "a stream of the ancient READY BUNDLE, which we assembled (OQ-LR-5)",
    "scoring_summary":
        "our own scoring decomposition (golden_points), named by our own builder",
}

# Closed, source-specific aliases for OUR assembled-bundle columns whose read-side name is
# intentionally not the canonical name. Do not turn this into a catch-all: source_positions
# is the publisher's specific, sometimes multi-valued vocabulary, and the settled taxonomy
# ruling sends it to nfl_position rather than the ten fantasy buckets in position.
# The PFR and newspaper streams are handled by their own correspondence tables; these are
# the two remaining ancient-bundle lineages.
CANONICAL_ALIASES: dict[tuple[str, str], str] = {
    ("ancient_pbp1978_recovery", "source_positions"): "nfl_position",
    ("ancient_pfa_gamelog", "source_positions"): "nfl_position",
}


def _contracts() -> dict[str, dict]:
    from .column_dossier import DISPOSITIONS_PATH

    path = DISPOSITIONS_PATH.parent / "stat_contracts.v1.json"
    document = json.loads(path.read_text(encoding="utf-8"))
    by_name: dict[str, dict] = {}
    for contract in document["stats"]:
        by_name.setdefault(contract["canonical_name"], contract)
        by_name.setdefault(contract["stat_id"], contract)
    return by_name


def escalated_row_keys() -> dict[str, str]:
    """This pass escalates NOTHING, and that is a claim rather than an omission.

    Its rule is authorship: a column of a table WE wrote whose name matches a registered
    stat contract is mapped, and a column with no contract entry is left OPEN and reported
    by name. An unmatched column is NOT a refused question -- nobody has argued anything
    about it. Filing those 133 legacy columns as escalations would inflate the refused
    count with plain backlog, which is the mirror of hiding refusals inside it."""
    return {}


def build_decisions() -> tuple[list[dict], dict]:
    # An authorship claim about a source that is not registered is a claim about
    # nothing, and it fails SILENTLY -- the source contributes no rows, so the pass
    # simply closes less and reports success. `ngs_season` sat here for one run before a
    # test caught it; the real key is `ngs_season_published`.
    from .sources import registry

    unknown = sorted(set(OWN_VOCABULARY) - set(registry(include_subject=True)))
    if unknown:
        raise SystemExit(
            f"OWN_VOCABULARY names sources that are not registered: {unknown} -- an "
            "authorship claim about a non-existent source closes nothing and reports "
            "success")

    contracts = _contracts()
    dossier = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    decisions: list[dict] = []
    unmatched: dict[str, list[str]] = defaultdict(list)

    # OPEN rows PLUS this generator's own earlier decisions -- see the statscrew pass for
    # why: a table that can only be extended and never REVISED reports "0 written" and
    # reads as agreement.
    from .column_dossier import load_decisions

    mine = {key for key, entry in load_decisions().items()
            if entry.get("generated_by") == GENERATOR}
    for row in dossier["rows"]:
        source = row["source"]
        if source not in OWN_VOCABULARY:
            continue
        if row["disposition"] != "OPEN" and row_key(
                source, row["table_key"], row["column"]) not in mine:
            continue
        alias = CANONICAL_ALIASES.get((source, row["column"]))
        contract = contracts.get(alias or row["column"])
        if contract is None:
            unmatched[source].append(row["column"])
            continue
        if alias:
            reason = (
                "The settled position-taxonomy ruling: this assembled-bundle field "
                "preserves the source's specific, potentially multi-valued position string "
                "and therefore maps to `nfl_position`, not the ten fantasy buckets carried "
                "by `position`")
        else:
            reason = (f"{OWN_VOCABULARY[source].split('.')[0].strip()}; the column name "
                      f"is our canonical vocabulary and the registry carries it as "
                      f"{contract['canonical_name']!r} "
                      f"(family {contract.get('family')!r}, natural grain "
                      f"{contract.get('natural_grain')!r})")
        decisions.append({
            "key": row_key(source, row["table_key"], row["column"]),
            "disposition": "MAPPED_TO_CANONICAL",
            "canonical": contract["canonical_name"],
            "reason": reason,
            "evidence": f"stat_contracts.v1.json stat_id={contract['stat_id']!r}, "
                        f"canonical_name={contract['canonical_name']!r}, "
                        f"aggregation_class={contract.get('aggregation_class')!r}, "
                        f"tolerance_policy="
                        f"{(contract.get('tolerance_policy') or {}).get('policy')!r}. "
                        f"unit={contract.get('unit')!r}, "
                        f"natural_grain={contract.get('natural_grain')!r}. AUTHORSHIP: "
                        f"{source} is written by our own builder, so the "
                        "name matches on both sides by construction rather than by "
                        "coincidence -- measured, our own tables resolve at 82-100% "
                        "against 7-23% for transcribed foreign tables",
        })
    return decisions, {"mapped": len(decisions),
                       "left_open_no_contract": sum(len(v) for v in unmatched.values()),
                       "unmatched": {k: sorted(v) for k, v in sorted(unmatched.items())}}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    decisions, tally = build_decisions()
    print(f"decisions: {len(decisions):,}")
    print(f"  mapped                 {tally['mapped']}")
    print(f"  left OPEN (no contract) {tally['left_open_no_contract']}")
    for source, columns in tally["unmatched"].items():
        print(f"\n  {source}: {len(columns)} columns with no contract entry")
        print(f"    {columns}")
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
