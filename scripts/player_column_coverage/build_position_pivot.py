#!/usr/bin/env python3
"""Build the canonical position-combo pivot.

Multi-role players carry compound position tokens (`QB,P`, `RB,DB`, `TE,QB`).
Two problems follow:

  1. The same combo can be spelled in more than one order (`TE,QB` vs `QB,TE`),
     so it counts as two distinct values when it is one football fact.
  2. `position` must still resolve to ONE broad category for fantasy grouping,
     and `primary_position` cannot be trusted for it — measured on this release,
     it names a position that is not even a member of the combo on 11.1% of
     multi-position rows (`QB,K` -> `RB`, `LB,P` -> `K`).

Both are solved by ordering compound tokens by FANTASY PRECEDENCE:

    QB > RB > WR > TE > K > IDP (DL > LB > DB) > P > OL > DEF

Ordering makes the spelling canonical (always `QB,K`, never `K,QB`) and makes
resolution deterministic: `position` is simply the FIRST token of the canonical
form, which is always a real member of the combo and always the most
fantasy-relevant role.

    python scripts/player_column_coverage/build_position_pivot.py --parquet <release>
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import duckdb

# Fantasy precedence. IDP expands to DL > LB > DB (front seven before secondary)
# so the ordering is total; the user-facing rule treats them as one IDP tier.
PRECEDENCE = ["QB", "RB", "WR", "TE", "K", "DL", "LB", "DB", "P", "OL", "DEF"]
RANK = {token: index for index, token in enumerate(PRECEDENCE)}


def canonicalize(raw: str, detailed_to_broad: dict[str, str]) -> tuple[str, str | None]:
    """Return (canonical combo, resolved broad position)."""
    tokens = [part.strip().upper() for part in raw.split(",") if part.strip()]
    broad: list[str] = []
    for token in tokens:
        mapped = detailed_to_broad.get(token, token)
        if mapped not in broad:
            broad.append(mapped)
    broad.sort(key=lambda token: RANK.get(token, 99))
    if not broad:
        return "", None
    return ",".join(broad), broad[0]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parquet", type=Path, required=True)
    parser.add_argument(
        "--taxonomy",
        type=Path,
        default=Path("scripts/sota_recon/witness_gate/contracts/position_taxonomy.v1.json"),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("docs/player-column-coverage/position-combo-pivot.json"),
    )
    args = parser.parse_args()

    taxonomy = json.loads(args.taxonomy.read_text(encoding="utf-8"))
    detailed_to_broad = {k.upper(): v for k, v in taxonomy["detailed_to_broad"].items()}

    connection = duckdb.connect()
    source = f"read_parquet('{args.parquet.as_posix()}')"

    entries: dict[str, dict[str, Any]] = {}
    for column in ("position", "primary_position", "nfl_position"):
        rows = connection.execute(
            f"SELECT {column} AS value, COUNT(*) AS n FROM {source} "
            f"WHERE {column} IS NOT NULL AND {column} <> '' GROUP BY 1"
        ).fetchall()
        for value, count in rows:
            canonical, resolved = canonicalize(str(value), detailed_to_broad)
            if not canonical:
                continue
            entry = entries.setdefault(
                canonical,
                {
                    "canonical": canonical,
                    "resolvedPosition": resolved,
                    "isCompound": "," in canonical,
                    "observedSpellings": {},
                    "rowsByColumn": {},
                },
            )
            spellings = entry["observedSpellings"].setdefault(str(value), 0)
            entry["observedSpellings"][str(value)] = spellings + int(count)
            entry["rowsByColumn"][column] = entry["rowsByColumn"].get(column, 0) + int(count)

    for entry in entries.values():
        entry["observedSpellings"] = dict(sorted(entry["observedSpellings"].items()))
        entry["spellingCount"] = len(entry["observedSpellings"])
        entry["totalRows"] = sum(entry["rowsByColumn"].values())

    ordered = dict(
        sorted(
            entries.items(),
            key=lambda item: (item[1]["isCompound"], -item[1]["totalRows"]),
        )
    )
    compound = {k: v for k, v in ordered.items() if v["isCompound"]}
    merged = {k: v for k, v in compound.items() if v["spellingCount"] > 1}

    payload = {
        "precedence": PRECEDENCE,
        "precedenceRule": "QB > RB > WR > TE > K > IDP (DL > LB > DB) > P > OL > DEF",
        "resolutionRule": "position = first token of the canonical combo",
        "summary": {
            "canonicalForms": len(ordered),
            "compoundForms": len(compound),
            "formsWithMultipleSpellings": len(merged),
            "spellingsMerged": sum(v["spellingCount"] - 1 for v in merged.values()),
            "rowsAffectedByMerge": sum(v["totalRows"] for v in merged.values()),
        },
        "mergedSpellings": {k: v["observedSpellings"] for k, v in merged.items()},
        "combos": ordered,
    }

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, indent=2)
        handle.write("\n")

    summary = payload["summary"]
    print(f"canonical forms      : {summary['canonicalForms']}")
    print(f"  compound           : {summary['compoundForms']}")
    print(f"  merged spellings   : {summary['formsWithMultipleSpellings']} forms, "
          f"{summary['spellingsMerged']} spellings collapsed, "
          f"{summary['rowsAffectedByMerge']} rows")
    for canonical, spellings in payload["mergedSpellings"].items():
        print(f"    {canonical:14} <- {spellings}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
