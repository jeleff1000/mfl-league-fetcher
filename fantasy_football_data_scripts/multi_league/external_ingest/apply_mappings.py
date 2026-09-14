"""Column rename + manager_guid synthesis + ignore-row drop, applied to staged DataFrames."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field

import pandas as pd


# Stable platform identity / join-key columns. apply_mappings selects only
# the column_map's mapped columns, which strips these because most are NOT
# in the per-table manifests (PLAYER_FANTASY_MANIFEST has no slot for
# yahoo_player_id). Silently dropping them destroys staging-merge dedup:
# the NULL-fallback dedup key `(year, week, yahoo_player_id, manager)`
# collapses every distinct player-week to one row per (year, week, manager)
# when yahoo_player_id is NULL — losing all BN/IR/FLX lineup info and
# blocking the canonical-franchise merge. These are stable platform IDs,
# not enrichments, so preserving them from source does not risk staleness.
_PRESERVED_IDENTITY_COLS: tuple[str, ...] = (
    "yahoo_player_id",
    "sleeper_player_id",
    "espn_player_id",
    "NFL_player_id",
    "player_week",
    "player_key",
    "position",
    "nfl_team_api",
)


@dataclass
class ColumnMap:
    table: str
    column_map: dict[str, str]  # slot_name -> source_file_column


@dataclass
class IdentityDecision:
    franchise_id: str | None = None  # None if create-new or ignore
    owner_guid: str | None = None  # the canonical franchise's owner_guid (for franchise_id case)
    display_name: str | None = None  # optional canonical manager label to write into staged rows
    create_new: bool = False
    ignore: bool = False


@dataclass
class ApplyResult:
    dfs: dict[str, pd.DataFrame] = field(default_factory=dict)
    ignored_rows_dropped: dict[str, int] = field(default_factory=dict)  # manager -> count
    unmapped_managers: list[str] = field(default_factory=list)
    unmapped_managers_count: int = 0


def synthesize_external_guid(league_key: str, manager_string: str, platform: str = "") -> str:
    """Deterministic guid for create-new external franchises.

    Keyed on (platform, league_key, manager_string) so two leagues that happen
    to share a human-readable league_name cannot collide. league_key should be
    the platform-unique league identifier (Yahoo league_key, Sleeper league_id,
    or ESPN league_id) — NOT the user-facing league_name.
    """
    h = hashlib.sha256(f"{platform}::{league_key}::{manager_string}".encode()).hexdigest()[:16]
    return f"external_{h}"


def apply_mappings(
    staged_dfs: dict[str, pd.DataFrame],
    column_maps: list[ColumnMap],
    identity_decisions: dict[str, IdentityDecision],
    league_db: str,
    platform: str = "",
) -> ApplyResult:
    """Rename columns, synthesize manager_guid, drop ignored rows.

    Operates on already-materialized DataFrames from the existing staging-merge step.
    Returns the transformed DataFrames + reporting counts."""
    result = ApplyResult()
    cm_by_table = {cm.table: cm for cm in column_maps}
    unmapped_set: set[str] = set()

    for table, df in staged_dfs.items():
        cm = cm_by_table.get(table)
        if cm is None:
            # No column map for this table — pass through (may already be in canonical shape)
            out = df.copy()

        else:
            # 1. Build inverse map: source_col -> slot_name
            rename_map = {src: slot for slot, src in cm.column_map.items() if src in df.columns}

            # 2. Keep only mapped columns, then rename
            out = df[list(rename_map.keys())].rename(columns=rename_map).copy()

            # 2b. Preserve stable platform identity columns from the source file
            # even when not in column_map. They aren't slots in most manifests
            # (e.g., yahoo_player_id is not in PLAYER_FANTASY_MANIFEST), so the
            # wizard never maps them — but staging-merge dedup needs them. Skip
            # any name already produced by the rename (mapped target) or already
            # consumed as a rename source, to avoid shadowing explicit mappings.
            consumed_sources = set(rename_map.keys())
            existing_targets = set(out.columns)
            for col in _PRESERVED_IDENTITY_COLS:
                if col in df.columns and col not in consumed_sources and col not in existing_targets:
                    out[col] = df[col].values

        # 3. Apply identity decisions per row
        if "manager" in out.columns:
            # Drop ignored rows
            ignore_managers = {m for m, d in identity_decisions.items() if d.ignore}
            if ignore_managers:
                drop_mask = out["manager"].isin(ignore_managers)
                for m in ignore_managers:
                    cnt = int((out["manager"] == m).sum())
                    if cnt:
                        result.ignored_rows_dropped[m] = result.ignored_rows_dropped.get(m, 0) + cnt
                out = out[~drop_mask].copy()

            # Synthesize manager_guid
            def _resolve_guid(m):
                d = identity_decisions.get(m)
                if d is None:
                    unmapped_set.add(m)
                    return None  # leaves NULL → downstream registry will hidden_ guid it
                if d.franchise_id is not None:
                    # owner_guid is typically equal to franchise_id (Yahoo OAuth
                    # GUID IS the canonical key; same for Sleeper user_id and
                    # ESPN swid). Fall back to franchise_id when the wizard
                    # persists only franchise_id without owner_guid — otherwise
                    # the staged rows land with NULL manager_guid and the
                    # franchise_registry synthesizes a `hidden_<slug>` id that
                    # never merges with the canonical years.
                    return d.owner_guid or d.franchise_id
                if d.create_new:
                    return synthesize_external_guid(league_db, m, platform)
                return None

            existing_guid = out.get("manager_guid")
            new_guids = out["manager"].map(_resolve_guid)
            # User identity decisions WIN over whatever was in the source file.
            # Fall back to the file's manager_guid only for managers with no decision
            # (where new_guids is NaN/None), which lets real Yahoo guids on
            # unmapped rows survive untouched.
            if existing_guid is not None:
                out["manager_guid"] = new_guids.where(new_guids.notna(), existing_guid)
            else:
                out["manager_guid"] = new_guids

            display_names = out["manager"].map(
                lambda m: identity_decisions.get(m).display_name
                if identity_decisions.get(m) and identity_decisions.get(m).display_name
                else None
            )
            out["manager"] = display_names.where(display_names.notna(), out["manager"])

        result.dfs[table] = out

    result.unmapped_managers = sorted(unmapped_set)
    result.unmapped_managers_count = len(unmapped_set)
    return result
