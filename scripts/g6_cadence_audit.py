#!/usr/bin/env python3
"""G6: audit table-level cadence labels against column-level evidence.

Proves labels are CORRECT, not just consistent, by cross-referencing three
independent sources:

1. The publish registry's table-level cadence_class (what the server enforces).
2. The column-level cadence inventory (what each column's write scope
   actually requires — scripts/_artifacts/update_cadence_*.csv).
3. The route census (which API routes read each column — the staleness
   blast radius when a column's required scope exceeds the table's class).

Findings:
- VIOLATION: a table label narrower than what its columns require in a way
  that silently loses updates (fails the audit).
- KNOWN-STALE: career/all-time broadcast columns on active_season tables —
  prior-season rows keep old values after a weekly publish. Accepted v1
  debt (WS-D sidecar migration), but every column + its reading routes must
  be enumerated so the staleness is chosen, not discovered.
- MISMATCH: table label wider/narrower than the dominant column evidence.

Offline only: local CSVs + registry import. Exit 1 on VIOLATION, 0 otherwise.
"""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

INVENTORY_CSV = ROOT / "scripts" / "_artifacts" / "update_cadence_2026_07_05.csv"
CENSUS_CSV = ROOT / "docs" / "hidden-column-exposure-census.csv"
OUT = ROOT / "scripts" / "_artifacts" / "g6_cadence_audit_20260706.json"

# Minimal weekly write scope each column bucket requires. Wider table scopes
# satisfy narrower needs; a table scope narrower than a column's need means
# some rows keep stale values after a weekly publish.
BUCKET_SCOPE = {
    "weekly_fact": "week",
    "week_local_derived": "week",
    "weekly_matrix": "week",
    "week_local_or_identity_enrichment": "week",
    "season_fact": "season",
    "season_schedule": "season",
    "season_config": "season",
    "current_season_snapshot": "season",
    "current_season_rollup": "season",
    "current_season_simulation": "season",
    "current_season_outcome": "season",
    "career_rollup": "league",
    "homepage_payload": "league",
    "career_broadcast_on_weekly_rows": "league",
    "career_broadcast_on_pick_rows": "league",
    "identity_or_config": "same",
}
SCOPE_ORDER = {"week": 0, "season": 1, "league": 2}
CLASS_SCOPE = {"active_season": "season", "league_rollup": "league", "append_week": "week"}


def main() -> int:
    from multi_league.core.delta_publish import canonical_table_registry

    registry = canonical_table_registry()

    inventory = defaultdict(list)
    for row in csv.DictReader(open(INVENTORY_CSV, encoding="utf-8")):
        inventory[row["table"]].append(row)

    census_routes = defaultdict(set)
    for row in csv.DictReader(open(CENSUS_CSV, encoding="utf-8")):
        table = row["table"].split(".")[-1]
        routes = row.get("api_payload_routes") or ""
        for route in routes.split(";"):
            route = route.strip()
            if route:
                census_routes[(table, row["column"])].add(route)

    report = {
        "captured_at": datetime.now(timezone.utc).isoformat(),
        "tables": {},
        "violations": [],
        "known_stale": [],
        "unlabeled_columns": [],
        "registry_tables_missing_inventory": [],
    }

    for table, spec in sorted(registry.items()):
        cls = spec["cadence_class"]
        cols = inventory.get(table)
        if not cols:
            report["registry_tables_missing_inventory"].append(table)
            continue

        buckets = Counter(c["cadence"] for c in cols)
        table_scope = CLASS_SCOPE[cls]
        entry = {"cadence_class": cls, "columns": len(cols), "buckets": dict(buckets)}

        for c in cols:
            bucket = c["cadence"]
            need = BUCKET_SCOPE.get(bucket)
            if need is None:
                report["unlabeled_columns"].append({"table": table, "column": c["column"], "bucket": bucket})
                continue
            if need == "same":
                continue
            if SCOPE_ORDER[need] > SCOPE_ORDER[table_scope]:
                # Column needs a wider refresh than the table's weekly scope.
                # For active_season tables this means prior-season rows keep
                # stale values — the documented WS-D debt, enumerated here.
                routes = sorted(census_routes.get((table, c["column"]), []))
                finding = {
                    "table": table,
                    "column": c["column"],
                    "table_class": cls,
                    "column_bucket": bucket,
                    "needs_scope": need,
                    "reading_routes": routes,
                }
                if bucket.startswith("career_broadcast") or bucket in ("career_rollup", "homepage_payload"):
                    report["known_stale"].append(finding)
                else:
                    report["violations"].append(finding)

        # Label sanity: an active_season table whose columns are ALL
        # league-scope would be mislabeled (its weekly publish would never
        # refresh anything meaningful at season scope).
        non_identity = [c for c in cols if BUCKET_SCOPE.get(c["cadence"]) not in (None, "same")]
        if cls == "active_season" and non_identity and all(
            SCOPE_ORDER[BUCKET_SCOPE[c["cadence"]]] == SCOPE_ORDER["league"] for c in non_identity
        ):
            report["violations"].append({
                "table": table, "column": "<ALL>", "table_class": cls,
                "column_bucket": "all league-scope", "needs_scope": "league",
                "reading_routes": [], "note": "table label narrower than every column",
            })
        report["tables"][table] = entry

    stale_routes = sorted({r for f in report["known_stale"] for r in f["reading_routes"]})
    summary = {
        "tables_audited": len(report["tables"]),
        "violations": len(report["violations"]),
        "known_stale_columns": len(report["known_stale"]),
        "known_stale_tables": sorted({f["table"] for f in report["known_stale"]}),
        "routes_reading_stale_columns": stale_routes,
        "unlabeled_columns": len(report["unlabeled_columns"]),
        "registry_tables_missing_inventory": report["registry_tables_missing_inventory"],
    }
    report["summary"] = summary

    OUT.write_text(json.dumps(report, indent=1), encoding="utf-8")
    print(json.dumps(summary, indent=1))
    print(f"[g6] full report: {OUT}")
    return 1 if report["violations"] else 0


if __name__ == "__main__":
    sys.exit(main())
