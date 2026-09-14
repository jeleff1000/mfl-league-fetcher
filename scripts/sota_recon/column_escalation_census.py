"""What the column-dossier OPEN count is actually made of.

`column_dossier_open_rows` is one number, and it silently mixes two populations that need
opposite treatment:

    NOT YET REACHED   nobody has looked. Burn it down.
    ESCALATED         somebody looked, measured, and REFUSED -- with the question that
                      would settle it. Re-deriving it is waste, and worse, a session that
                      cannot see the refusal is likely to decide it the easy way.

A queue that cannot tell those apart is a queue that lies about its own composition, which
is the same defect as a wrong denominator wearing different clothes. This census splits it.

THE DENOMINATOR IS DECLARED, INCLUDING ITS HOLE. `GENERATORS` lists every adjudication
module, and a module that does not expose `escalated_row_keys()` is reported in
`generators_without_escalation_export` rather than dropped -- so "0 escalations from that
pass" can never be confused with "that pass has no way to report them".

    python -m scripts.sota_recon.column_escalation_census
"""
from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path

from .column_dossier import row_key

RECEIPT = Path(__file__).resolve().parents[2] / "docs" / "column-escalation-census.json"
DOSSIER = Path(__file__).resolve().parents[2] / "docs" / "column-dossier.json"

# every adjudication generator, whether or not it currently has anything to escalate
GENERATORS = (
    "nflcom_column_adjudication",
    "statscrew_column_adjudication",
    "nflcom_splits_column_adjudication",
    "nflcom_category_column_adjudication",
    "mapspec_column_adjudication",
    "newspaper_column_adjudication",
    "pfr_column_adjudication",
    "legacy_column_adjudication",
    "internal_column_adjudication",
    "newspaper_atom_column_adjudication",
    "pbp_column_adjudication",
    "pfr_datastat_column_adjudication",
    "pfr_defense_semantic_overrides",
    "newspaper_sidecar_column_adjudication",
)


def build() -> dict:
    escalations: dict[str, dict] = {}
    missing: list[str] = []
    for name in GENERATORS:
        module = import_module(f"{__package__}.{name}")
        export = getattr(module, "escalated_row_keys", None)
        if export is None:
            missing.append(name)
            continue
        for key, question in export().items():
            escalations[key] = {"generator": name, "question": question}

    open_rows = total = 0
    open_keys: set[str] = set()
    if DOSSIER.exists():
        rows = json.loads(DOSSIER.read_text(encoding="utf-8"))["rows"]
        total = len(rows)
        for row in rows:
            if row["disposition"] == "OPEN":
                open_rows += 1
                open_keys.add(row_key(row["source"], row["table_key"], row["column"]))

    # An escalation whose row is no longer OPEN is SPENT: something closed the row after
    # the generator raised its question. Subtracting the raw escalation count from the
    # open count would then understate what is unexamined -- the same inflated-denominator
    # defect this census exists to expose, committed by the census itself. Measured
    # 2026-07-29: 16 of 202 escalations pointed at rows that had since closed, so
    # "not yet reached" read 71 when it was 87.
    spent = sorted(key for key in escalations if open_keys and key not in open_keys)
    live = {key: value for key, value in escalations.items() if key not in set(spent)}

    by_generator: dict[str, int] = {}
    for entry in live.values():
        by_generator[entry["generator"]] = by_generator.get(entry["generator"], 0) + 1

    return {
        "generated": "the composition of column_dossier_open_rows: refused-with-a-question "
                     "vs not-yet-reached",
        "counters": {
            "dossier_rows": total,
            "open_rows": open_rows,
            "escalated_rows": len(live),
            "open_not_yet_reached": open_rows - len(live),
            "escalations_spent_row_already_closed": len(spent),
            "generators_without_escalation_export": len(missing),
        },
        "generators_without_escalation_export": missing,
        "escalated_by_generator": dict(sorted(by_generator.items())),
        "escalations_spent": spent,
        "escalations": dict(sorted(escalations.items())),
    }


def write() -> dict:
    document = build()
    RECEIPT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def main() -> int:
    document = write()
    for name, value in document["counters"].items():
        print(f"  {name:38s} {value:,}" if isinstance(value, int) else f"  {name} {value}")
    print()
    for name, count in document["escalated_by_generator"].items():
        print(f"  {name:40s} {count:,}")
    if document["generators_without_escalation_export"]:
        print("\nNO ESCALATION EXPORT (their refusals are invisible to this counter):")
        for name in document["generators_without_escalation_export"]:
            print("   -", name)
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
