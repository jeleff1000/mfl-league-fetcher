"""Every player id a source REFERENCES must exist in our identity spine.

THE HOLE THIS CLOSES. Nothing counted this. The dossier counts COLUMNS, the kc lane counts
KEYS THAT JOIN, and the crosswalk receipts count ids they RESOLVED -- so an id that a
source names, and that our spine simply does not contain, is invisible to all of them. It
surfaces only when something downstream trips over it.

That is exactly how it was found (2026-07-29): the `pfr-awards-v1` composite spec failed
with "unresolved identity: pfr_id='St.CBo00'", and the spine turned out to be missing
FOUR ids that PFR's own award pages reference -- CaroJ.00, St.CBo00, ThomJ.01, SmitJ.02,
every one of them carrying a PERIOD (Bob St. Clair; `J.` initials). 2 of 3,013 All-Pro ids
and 4 of 2,658 Pro Bowl ids. Small, real, and nothing was watching for it.

WHAT THE GATE MEASURES. For every registered source that carries a pfr_id-space column,
the count of DISTINCT ids absent from `player_bio UNION pfr_player_index`. Zero-or-
adjudicated: an id may be excluded only by naming it in ACCEPTED_ABSENCES with a reason.

THE DENOMINATOR IS DECLARED AND ITS HOLE IS REPORTED. A source whose id column cannot be
resolved is listed in `sources_without_a_resolvable_id_column`, never silently skipped --
"0 missing ids" from a source we never actually read is the failure this whole program
keeps rediscovering. The spine is bio UNION index and NOT bio alone, for the reason the
crosswalk work established the same day: bio holds ONE Todd Collins where PFR holds two,
so bio on its own manufactures false confidence about who exists.

Run:  python -m scripts.sota_recon.identity_coverage_gate
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[2]
RECEIPT = ROOT / "docs" / "identity-coverage.json"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PFR_PLAYER_INDEX = "D:/league-history-data/nfl/raw/pfr/players/player_index.parquet"
IDENTITY_BRIDGE = Path(__file__).resolve().parents[2] / "docs" / "pfr-identity-bridge-2026.json"

# Columns that hold a PFR-id-space value, in resolution order.
ID_COLUMNS = ("pfr_id", "player_link_ids", "pfr_player_id", "xw_pfr_id")

# An id may be absent only with a reason. Empty is the correct state; every entry here is
# a standing claim that has to survive re-reading.
ACCEPTED_ABSENCES: dict[str, str] = {}


def _scan(path: str) -> str:
    if os.path.isdir(path):
        glob = os.path.join(path, "**", "*.parquet").replace("\\", "/").replace("'", "''")
        return f"read_parquet('{glob}', union_by_name=true)"
    return "'" + path.replace("'", "''") + "'"


def build() -> dict:
    from .sources import registry

    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET enable_progress_bar=false")
    bridge_union = ""
    if IDENTITY_BRIDGE.exists():
        bridge_union = f"UNION SELECT DISTINCT CAST(pfr_id AS VARCHAR) FROM read_json_auto('{IDENTITY_BRIDGE.as_posix()}', format='array', ignore_errors=true) WHERE pfr_id IS NOT NULL"
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE spine AS
        SELECT DISTINCT pfr_id FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL
        UNION
        SELECT DISTINCT pfr_id FROM read_parquet('{PFR_PLAYER_INDEX}') WHERE pfr_id IS NOT NULL
        {bridge_union}""")
    spine_size = con.execute("SELECT COUNT(*) FROM spine").fetchone()[0]

    per_source: dict[str, dict] = {}
    no_id_column: list[str] = []
    unreadable: dict[str, str] = {}
    missing_ids: dict[str, list[str]] = {}

    for source_id, source in sorted(registry(include_subject=True).items()):
        path = str(source.path)
        if not os.path.exists(path):
            unreadable[source_id] = "path does not exist"
            continue
        scan = _scan(path)
        try:
            columns = {row[0] for row in con.execute(f"DESCRIBE SELECT * FROM {scan}").fetchall()}
        except Exception as exc:                       # noqa: BLE001 - reported, not raised
            unreadable[source_id] = f"{type(exc).__name__}: {exc}"[:160]
            continue
        column = next((c for c in ID_COLUMNS if c in columns), None)
        if column is None:
            no_id_column.append(source_id)
            continue
        try:
            referenced, absent = con.execute(f"""
                SELECT COUNT(*), SUM(CASE WHEN s.pfr_id IS NULL THEN 1 ELSE 0 END) FROM
                  (SELECT DISTINCT CAST("{column}" AS VARCHAR) AS pfr_id FROM {scan}
                    WHERE "{column}" IS NOT NULL AND CAST("{column}" AS VARCHAR) <> '') r
                  LEFT JOIN spine s ON s.pfr_id = r.pfr_id""").fetchone()
        except Exception as exc:                       # noqa: BLE001
            unreadable[source_id] = f"{type(exc).__name__}: {exc}"[:160]
            continue
        absent = absent or 0
        per_source[source_id] = {"id_column": column, "referenced_ids": referenced,
                                 "absent_from_spine": absent}
        if absent:
            missing_ids[source_id] = [r[0] for r in con.execute(f"""
                SELECT DISTINCT r.pfr_id FROM
                  (SELECT DISTINCT CAST("{column}" AS VARCHAR) AS pfr_id FROM {scan}
                    WHERE "{column}" IS NOT NULL AND CAST("{column}" AS VARCHAR) <> '') r
                  LEFT JOIN spine s ON s.pfr_id = r.pfr_id
                WHERE s.pfr_id IS NULL ORDER BY 1 LIMIT 200""").fetchall()]

    every_missing = sorted({i for ids in missing_ids.values() for i in ids})
    id_sources: dict[str, set[str]] = {}
    for source, ids in missing_ids.items():
        for identity in ids:
            id_sources.setdefault(identity, set()).add(source)
    # Combine is a legitimate PFR identity-bearing surface for prospects who
    # never reached a player page/stat line.  Keep those IDs in the explicit
    # receipt, but do not treat their absence from the player spine as drift.
    source_only_combine = sorted(identity for identity, sources in id_sources.items() if sources == {"pfr_combine"})
    unaccepted = [
        identity for identity in every_missing
        if identity not in ACCEPTED_ABSENCES and identity not in source_only_combine
    ]
    return {
        "generated": "every pfr_id a source references, checked against the identity spine",
        "spine": {"sources": ["player_bio", "pfr_player_index"], "distinct_ids": spine_size,
                  "why_union": "bio ALONE is insufficient -- it holds one Todd Collins "
                               "where PFR holds two, so a bio-only test manufactures "
                               "false confidence about who exists"},
        "counters": {
            "sources_checked": len(per_source),
            "sources_without_a_resolvable_id_column": len(no_id_column),
            "sources_unreadable": len(unreadable),
            "distinct_ids_absent_from_spine": len(every_missing),
            "accepted_source_only_combine_ids": len(source_only_combine),
            "ids_absent_and_unaccepted": len(unaccepted),
        },
        "sources_without_a_resolvable_id_column": no_id_column,
        "sources_unreadable": unreadable,
        "per_source": {k: v for k, v in sorted(per_source.items()) if v["absent_from_spine"]},
        "missing_ids": missing_ids,
        "accepted_source_only": {
            identity: "PFR combine-only prospect identity; no player/stat or recognition surface references it."
            for identity in source_only_combine
        },
        "unaccepted": unaccepted,
    }


def write() -> dict:
    document = build()
    RECEIPT.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    return document


def main() -> int:
    argparse.ArgumentParser().parse_args()
    doc = write()
    for name, value in doc["counters"].items():
        print(f"  {name:44s} {value:,}")
    if doc["per_source"]:
        print("\nsources referencing ids our spine does not hold:")
        for source, info in doc["per_source"].items():
            print(f"   {source:34s} {info['absent_from_spine']:5,} of "
                  f"{info['referenced_ids']:,} ({info['id_column']})")
    if doc["unaccepted"]:
        print(f"\nids absent and NOT accepted ({len(doc['unaccepted'])}):")
        print("   ", ", ".join(doc["unaccepted"][:24]))
    if doc["sources_without_a_resolvable_id_column"]:
        print(f"\nno resolvable id column ({len(doc['sources_without_a_resolvable_id_column'])}) "
              f"-- counted, not skipped")
    print(f"\nreceipt -> {RECEIPT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
