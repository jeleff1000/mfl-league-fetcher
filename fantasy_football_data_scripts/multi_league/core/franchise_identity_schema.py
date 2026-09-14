"""Canonical schema for franchise identity continuity tables."""

from __future__ import annotations


FRANCHISE_IDENTITY_REGISTRY_COLUMN_TYPES: dict[str, str] = {
    "db_name": "VARCHAR",
    "platform": "VARCHAR",
    "base_franchise_id": "VARCHAR",
    "manager_guid": "VARCHAR",
    "identity_key": "VARCHAR",
    "branch_key": "VARCHAR",
    "resolved_franchise_id": "VARCHAR",
    "team_index": "INTEGER",
    "anchor_year": "INTEGER",
    "anchor_week": "INTEGER",
    "anchor_team_name": "VARCHAR",
    "anchor_manager": "VARCHAR",
    "anchor_team_key": "VARCHAR",
    "anchor_team_slot": "VARCHAR",
    "known_team_names": "VARCHAR",
    "known_manager_names": "VARCHAR",
    "known_team_slots": "VARCHAR",
    "active_years": "VARCHAR",
    "created_at": "TIMESTAMP",
    "updated_at": "TIMESTAMP",
}

FRANCHISE_IDENTITY_AUDIT_COLUMN_TYPES: dict[str, str] = {
    "db_name": "VARCHAR",
    "platform": "VARCHAR",
    "year": "INTEGER",
    "week": "INTEGER",
    "manager_guid": "VARCHAR",
    "base_franchise_id": "VARCHAR",
    "resolved_franchise_id": "VARCHAR",
    "assignment_key": "VARCHAR",
    "identity_key": "VARCHAR",
    "branch_key": "VARCHAR",
    "team_index": "INTEGER",
    "team_key": "VARCHAR",
    "team_slot": "VARCHAR",
    "team_name": "VARCHAR",
    "manager": "VARCHAR",
    "reason": "VARCHAR",
    "identity_score": "INTEGER",
    "name_score": "INTEGER",
    "slot_score": "INTEGER",
    "total_score": "INTEGER",
    "created_at": "TIMESTAMP",
}

FRANCHISE_IDENTITY_REGISTRY_COLUMNS: list[str] = list(FRANCHISE_IDENTITY_REGISTRY_COLUMN_TYPES)
FRANCHISE_IDENTITY_AUDIT_COLUMNS: list[str] = list(FRANCHISE_IDENTITY_AUDIT_COLUMN_TYPES)


def _ddl(table_name: str, columns: dict[str, str]) -> str:
    col_defs = ",\n".join(f"  {name} {dtype}" for name, dtype in columns.items())
    return f"CREATE TABLE IF NOT EXISTS {table_name} (\n{col_defs}\n)"


FRANCHISE_IDENTITY_REGISTRY_DDL = _ddl(
    "franchise_identity_registry",
    FRANCHISE_IDENTITY_REGISTRY_COLUMN_TYPES,
)
FRANCHISE_IDENTITY_AUDIT_DDL = _ddl(
    "franchise_identity_audit",
    FRANCHISE_IDENTITY_AUDIT_COLUMN_TYPES,
)
