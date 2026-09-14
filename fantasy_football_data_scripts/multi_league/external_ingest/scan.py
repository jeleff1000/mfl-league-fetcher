"""Orchestrates schema scan + identity scan + auto-resolve passes for the wizard."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from multi_league.external_ingest.auto_map import auto_map, column_set_hash, AutoMapResult
from multi_league.external_ingest.config_io import IdentityMapping, SourceMapping
from multi_league.external_ingest.identity_match import (
    ExternalManagerOccurrence,
    IdentityResolution,
    cluster_resolutions,
    resolve_identity,
)
from multi_league.external_ingest.manifests import get_manifest


@dataclass
class TableScan:
    table: str
    file_columns: list[str]
    column_map: dict[str, str]
    ambiguous_slots: list[str]
    unfilled_slots: list[str]
    satisfaction_status: str  # "satisfied" | "missing_outcome" | "missing_identity"


@dataclass
class ScanResult:
    tables: dict[str, TableScan] = field(default_factory=dict)
    identity_occurrences: list[ExternalManagerOccurrence] = field(default_factory=list)
    resolutions: list = field(default_factory=list)  # mixed IdentityResolution + IdentityCluster


def _enumerate_occurrences(staged_dfs: dict[str, pd.DataFrame]) -> list[ExternalManagerOccurrence]:
    """Build ExternalManagerOccurrence per distinct manager string across all tables."""
    occ: dict[str, ExternalManagerOccurrence] = {}
    for table, df in staged_dfs.items():
        if "manager" not in df.columns:
            continue
        for mgr, group in df.groupby("manager", dropna=True):
            mgr_str = str(mgr)
            o = occ.get(mgr_str) or ExternalManagerOccurrence(
                manager=mgr_str,
                rows_with_guid={},
                years=set(),
                tables=set(),
            )
            if "year" in group.columns:
                o.years.update(int(y) for y in group["year"].dropna().unique())
            o.tables.add(table)
            if "manager_guid" in group.columns:
                guid_counts = group["manager_guid"].dropna().value_counts().to_dict()
                for g, c in guid_counts.items():
                    o.rows_with_guid[str(g)] = o.rows_with_guid.get(str(g), 0) + int(c)
            o.row_count_total += len(group)
            occ[mgr_str] = o
    return list(occ.values())


def scan_external_staging(
    staged_dfs: dict[str, pd.DataFrame],
    registry: Any,
    saved_source_configs: list[SourceMapping],
    saved_identity_mappings: dict[str, IdentityMapping],
) -> ScanResult:
    result = ScanResult()

    # --- Pass 1: schema scan
    for table, df in staged_dfs.items():
        try:
            manifest = get_manifest(table)
        except KeyError:
            continue
        cols = list(df.columns)
        sig = column_set_hash(cols)

        # Apply saved source config if hash matches
        saved = next((s for s in saved_source_configs if s.source_signature == sig and s.table == table), None)
        if saved is not None:
            cm = saved.column_map
            am = AutoMapResult(column_map=dict(cm), satisfied=True)
        else:
            am = auto_map(cols, manifest)

        # Lookup slot.group from the manifest (slot NAMES like "year"/"manager" don't
        # contain "identity"; the group classification is what we need).
        slot_groups = {s.name: s.group for s in manifest.slots}
        unfilled_groups = {slot_groups.get(s, "optional") for s in am.unfilled_slots}
        status = (
            "satisfied"
            if am.satisfied
            else ("missing_identity" if "identity" in unfilled_groups else "missing_outcome")
        )
        result.tables[table] = TableScan(
            table=table,
            file_columns=cols,
            column_map=am.column_map,
            ambiguous_slots=am.ambiguous_slots,
            unfilled_slots=am.unfilled_slots,
            satisfaction_status=status,
        )

    # --- Pass 2: identity scan
    result.identity_occurrences = _enumerate_occurrences(staged_dfs)

    # --- Pass 3: auto-resolve (apply saved identity mappings first)
    resolutions: list[IdentityResolution] = []
    for occ in result.identity_occurrences:
        saved = saved_identity_mappings.get(occ.manager)
        if saved is not None:
            if saved.ignore:
                resolutions.append(
                    IdentityResolution(
                        manager=occ.manager,
                        state="ignored",
                        franchise_id=None,
                    )
                )
            elif saved.franchise_id is not None:
                resolutions.append(
                    IdentityResolution(
                        manager=occ.manager,
                        state="auto_suggested",
                        franchise_id=saved.franchise_id,
                    )
                )
            else:
                # create-new placeholder
                resolutions.append(
                    IdentityResolution(
                        manager=occ.manager,
                        state="create_new",
                        franchise_id=None,
                    )
                )
        else:
            resolutions.append(resolve_identity(occ, registry))

    # --- Cluster strings auto-suggesting to same franchise
    result.resolutions = cluster_resolutions(resolutions)
    return result
