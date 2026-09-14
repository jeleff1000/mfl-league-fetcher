"""CLI wrapper for scan_external_staging — called by the Next.js scan route.

Usage:
    python -m multi_league.external_ingest.cli_scan --db <db_name> [--data-dir <path>]

Reads staged DataFrames from Fly staging tables via read_staging_data(), builds a
FranchiseRegistry from any existing franchise_config.json, then calls
scan_external_staging() and prints the result as JSON to stdout.

The --data-dir argument is optional. If provided (or discoverable from the
conventional location), franchise_config.json and external_source_config.json
are loaded from it. If neither file exists the CLI proceeds with empty configs
(correct behaviour for first-time / external-only leagues).

Exit codes:
    0  success — JSON written to stdout
    1  error   — error dict written to stdout as JSON
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd


# ── Staging dict key  →  manifest table key ──────────────────────────────────
# read_staging_data() returns: matchup, player, draft, transactions, schedule, settings
# scan_external_staging() expects manifest keys: matchup, draft, transactions, player_fantasy
_STAGING_KEY_TO_MANIFEST: dict[str, str] = {
    "matchup": "matchup",
    "draft": "draft",
    "transactions": "transactions",
    "player": "player_fantasy",  # staging uses "player"; manifest uses "player_fantasy"
}


def _default_data_dir(db_name: str) -> Path:
    """Return the conventional data directory for a league.

    Mirrors the pattern used by SleeperContext / LeagueContext:
    ~/fantasy_football_data/<db_name>/  (platform-agnostic fallback).
    """
    return Path.home() / "fantasy_football_data" / db_name


def _serialize_resolution(r: object, occ_by_manager: dict) -> dict:
    """Serialise a mixed IdentityResolution | IdentityCluster to a JSON-safe dict.

    Adds a 'kind' discriminator so the frontend can narrow the union type.
    occ_by_manager maps manager string → ExternalManagerOccurrence so that
    'single' resolutions can emit the required years/tables fields.
    """
    from multi_league.external_ingest.identity_match import IdentityCluster, IdentityResolution

    if isinstance(r, IdentityCluster):
        return {
            "kind": "cluster",
            "franchise_id": r.franchise_id,
            "external_managers": list(r.external_managers),
        }

    # IdentityResolution (single)
    assert isinstance(r, IdentityResolution)
    out: dict = {
        "kind": "single",
        "manager": r.manager,
        "state": r.state,
        "franchise_id": r.franchise_id,
    }
    occ = occ_by_manager.get(r.manager)
    if occ is not None:
        out["years"] = sorted(int(y) for y in occ.years)
        out["tables"] = sorted(occ.tables)
    else:
        out["years"] = []
        out["tables"] = []
    if r.suggestions:
        out["suggestions"] = [
            {
                "franchise_id": s.franchise_id,
                "franchise_name": s.franchise_name,
                "score": round(s.score, 4),
            }
            for s in r.suggestions
        ]
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Scan external staging data for a league.")
    parser.add_argument("--db", required=True, help="Database / league name")
    parser.add_argument(
        "--data-dir",
        default=None,
        help="Optional local data directory (for franchise_config.json lookup)",
    )
    args = parser.parse_args()

    db_name: str = args.db
    data_dir: Path = Path(args.data_dir) if args.data_dir else _default_data_dir(db_name)

    try:
        from multi_league.data_fetchers.shared.staging_reader import read_staging_data
        from multi_league.external_ingest.config_io import (
            read_external_alias_mappings,
            read_external_source_config,
        )
        from multi_league.external_ingest.scan import scan_external_staging
        from multi_league.core.franchise_registry import FranchiseRegistry
    except ImportError as exc:
        print(json.dumps({"error": f"Import error: {exc}"}), flush=True)
        sys.exit(1)

    # ── 1. Read staging data from Fly ────────────────────────────────────────
    try:
        staging_raw = read_staging_data(db_name)
    except Exception as exc:
        print(json.dumps({"error": f"Failed to read staging data: {exc}"}), flush=True)
        sys.exit(1)

    if not isinstance(staging_raw, dict):
        staging_raw = {}

    # ── 2. Map staging keys to manifest table names ───────────────────────────
    staged_dfs: dict[str, pd.DataFrame] = {}
    for staging_key, manifest_key in _STAGING_KEY_TO_MANIFEST.items():
        df = staging_raw.get(staging_key)
        if df is not None and isinstance(df, pd.DataFrame) and not df.empty:
            staged_dfs[manifest_key] = df

    # ── 3. Build FranchiseRegistry ───────────────────────────────────────────
    franchise_config_path = data_dir / "franchise_config.json"
    existing_config: Path | None = franchise_config_path if franchise_config_path.exists() else None

    # matchup_df may be None for first-time/external-only leagues — from_data handles it
    matchup_df = staged_dfs.get("matchup")
    draft_df = staged_dfs.get("draft")

    try:
        registry = FranchiseRegistry.from_data(
            matchup_df=matchup_df,
            draft_df=draft_df,
            existing_config=existing_config,
        )
    except Exception as exc:
        print(json.dumps({"error": f"Failed to build FranchiseRegistry: {exc}"}), flush=True)
        sys.exit(1)

    # ── 4. Load saved config files (gracefully degrade if absent) ────────────
    source_config_path = data_dir / "external_source_config.json"
    saved_source_configs = read_external_source_config(source_config_path)
    saved_identity_mappings = read_external_alias_mappings(franchise_config_path) if existing_config else {}

    # ── 5. Run scan ───────────────────────────────────────────────────────────
    try:
        scan_result = scan_external_staging(
            staged_dfs=staged_dfs,
            registry=registry,
            saved_source_configs=saved_source_configs,
            saved_identity_mappings=saved_identity_mappings,
        )
    except Exception as exc:
        print(json.dumps({"error": f"scan_external_staging failed: {exc}"}), flush=True)
        sys.exit(1)

    # ── 6. Serialise to ScanExternalStagingResponse shape ────────────────────
    tables_out = []
    for table_scan in scan_result.tables.values():
        tables_out.append(
            {
                "table": table_scan.table,
                "file_columns": list(table_scan.file_columns),
                "column_map": dict(table_scan.column_map),
                "ambiguous_slots": list(table_scan.ambiguous_slots),
                "unfilled_slots": list(table_scan.unfilled_slots),
                "satisfaction_status": table_scan.satisfaction_status,
            }
        )

    occ_by_manager = {occ.manager: occ for occ in scan_result.identity_occurrences}
    identity_resolutions_out = [_serialize_resolution(r, occ_by_manager) for r in scan_result.resolutions]

    unmapped_count_estimate = sum(1 for r in scan_result.resolutions if getattr(r, "state", None) == "unresolved")

    canonical_franchises = [
        {"franchise_id": f.franchise_id, "franchise_name": f.franchise_name} for f in registry.franchises.values()
    ]

    response = {
        "tables": tables_out,
        "identity_resolutions": identity_resolutions_out,
        "unmapped_count_estimate": unmapped_count_estimate,
        "canonical_franchises": canonical_franchises,
    }

    print(json.dumps(response), flush=True)


if __name__ == "__main__":
    main()
