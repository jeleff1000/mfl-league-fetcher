"""Single source of truth for the keeper_config table schema.

The JSON file is imported by both Python and TypeScript so backend and
frontend cannot drift on column names or types.
"""

from __future__ import annotations

import json
from pathlib import Path

_SCHEMA_PATH = Path(__file__).with_suffix(".json")
_schema = json.loads(_SCHEMA_PATH.read_text())

KEEPER_CONFIG_KEY_COLUMNS: list[str] = [c["name"] for c in _schema["key_columns"]]
KEEPER_CONFIG_FIELD_COLUMNS: list[str] = [c["name"] for c in _schema["field_columns"]]
KEEPER_CONFIG_COLUMNS: list[str] = KEEPER_CONFIG_KEY_COLUMNS + KEEPER_CONFIG_FIELD_COLUMNS

KEEPER_CONFIG_COLUMN_TYPES: dict[str, str] = {
    c["name"]: c["type"] for c in _schema["key_columns"] + _schema["field_columns"]
}


def _ddl() -> str:
    cols = []
    for c in _schema["key_columns"] + _schema["field_columns"]:
        nn = "" if c["nullable"] else " NOT NULL"
        cols.append(f"  {c['name']} {c['type']}{nn}")
    return "CREATE TABLE IF NOT EXISTS keeper_config (\n" + ",\n".join(cols) + ",\n  PRIMARY KEY (db_name, year)\n)"


KEEPER_CONFIG_DDL: str = _ddl()
