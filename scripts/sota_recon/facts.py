"""
sota_recon/facts.py  --  append-only correction FACTS (Phase 0B of the SOTA closeout plan).

Corrections are data, not scripts. From Phase 0C onward every fix emits fact rows here,
even while fixes are still applied through the wave framework; the Phase-6 pure builder
consumes these same facts. See docs/runbooks/v26-sota-closeout-and-drift-plan.md (WS5).

Five fact kinds, one DuckDB file on the D: lake (append-only; no update/delete API exists):
  cell_override               one cell: (table, target_key, column) old_value -> new_value
  row_split                   one merged row replaced by N rows (JSON payload)
  row_delete                  one row removed (old_row_hash pins what was deleted)
  row_tag                     marker on a row (e.g. doubleheader_unsplit)
  source_precedence_decision  witness precedence ruling per stat x era (x grain/league/version)

Application semantics (enforced here, not by convention):
  * target_key is a stable grain key (player_week, or NFL_player_id|year|week|game_date),
    never a physical row id.
  * every cell_override asserts the old_value it corrects; assert_fact_applies() flags
    STALE facts (current value != asserted old) so a fact never silently applies to data
    it wasn't adjudicated against.
  * effective_*() resolves newest-wins per target; two facts on one target with the same
    created_at is a FactConflict (build error), not a coin flip.
  * every fact records the source_snapshot_id it was adjudicated against.

    python -m scripts.sota_recon.facts            # init db + show counts
"""
from __future__ import annotations

import datetime as _dt
import json
import os
import uuid

import duckdb

FACTS_DB = os.environ.get(
    "SOTA_FACTS_DB", r"D:\league-history-data\nfl\facts\corrections.duckdb"
)

COMMON_COLS = (
    "fact_id VARCHAR NOT NULL, created_at TIMESTAMP NOT NULL, wave_id VARCHAR NOT NULL, "
    "reason VARCHAR NOT NULL, witness VARCHAR NOT NULL, source_snapshot_id VARCHAR NOT NULL"
)

SCHEMAS = {
    "cell_override": (
        f"{COMMON_COLS}, table_name VARCHAR NOT NULL, target_key VARCHAR NOT NULL, "
        "column_name VARCHAR NOT NULL, old_value VARCHAR, new_value VARCHAR"
    ),
    "row_split": (
        f"{COMMON_COLS}, table_name VARCHAR NOT NULL, target_key VARCHAR NOT NULL, "
        "replacement_rows_json VARCHAR NOT NULL"
    ),
    "row_delete": (
        f"{COMMON_COLS}, table_name VARCHAR NOT NULL, target_key VARCHAR NOT NULL, "
        "old_row_hash VARCHAR NOT NULL"
    ),
    "row_tag": (
        f"{COMMON_COLS}, table_name VARCHAR NOT NULL, target_key VARCHAR NOT NULL, "
        "tag VARCHAR NOT NULL"
    ),
    "row_add": (
        f"{COMMON_COLS}, table_name VARCHAR NOT NULL, target_key VARCHAR NOT NULL, "
        "row_json VARCHAR NOT NULL"
    ),
    "source_precedence_decision": (
        f"{COMMON_COLS}, stat VARCHAR NOT NULL, era VARCHAR NOT NULL, "
        "grain VARCHAR NOT NULL, league VARCHAR, source_version VARCHAR, "
        "ruling VARCHAR NOT NULL, winner VARCHAR"
    ),
}

RULINGS = {"winner", "disputed", "plausibility_only"}

# per-kind required emit fields beyond the common ones
_REQUIRED = {
    "cell_override": {"table_name", "target_key", "column_name", "new_value"},
    "row_split": {"table_name", "target_key", "replacement_rows_json"},
    "row_delete": {"table_name", "target_key", "old_row_hash"},
    "row_tag": {"table_name", "target_key", "tag"},
    "row_add": {"table_name", "target_key", "row_json"},
    "source_precedence_decision": {"stat", "era", "grain", "ruling"},
}


class FactConflict(Exception):
    """Two facts claim the same target with no deterministic winner."""


class StaleFact(Exception):
    """A fact's asserted old_value no longer matches the data it targets."""


def connect(db: str | None = None) -> duckdb.DuckDBPyConnection:
    path = db or FACTS_DB
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con = duckdb.connect(path)
    for kind, cols in SCHEMAS.items():
        con.execute(f"CREATE TABLE IF NOT EXISTS {kind} ({cols})")
    return con


def emit_fact(kind: str, con: duckdb.DuckDBPyConnection | None = None, **fields) -> str:
    """Append one fact. Returns fact_id. Validates kind + required fields; never updates."""
    if kind not in SCHEMAS:
        raise ValueError(f"unknown fact kind {kind!r}; must be one of {sorted(SCHEMAS)}")
    for req in ("wave_id", "reason", "witness", "source_snapshot_id"):
        if not fields.get(req):
            raise ValueError(f"{kind}: missing required common field {req!r}")
    missing = _REQUIRED[kind] - {k for k, v in fields.items() if v is not None}
    if missing:
        raise ValueError(f"{kind}: missing required fields {sorted(missing)}")
    if kind == "source_precedence_decision":
        if fields["ruling"] not in RULINGS:
            raise ValueError(f"ruling must be one of {sorted(RULINGS)}")
        if fields["ruling"] == "winner" and not fields.get("winner"):
            raise ValueError("ruling='winner' requires a winner")
    if kind == "row_split":
        rows = json.loads(fields["replacement_rows_json"])
        if not isinstance(rows, list) or len(rows) < 2:
            raise ValueError("row_split replacement_rows_json must be a JSON list of >=2 rows")

    fields = dict(fields)
    fields["fact_id"] = str(uuid.uuid4())
    fields["created_at"] = fields.get("created_at") or _dt.datetime.now(_dt.timezone.utc)

    own = con is not None
    c = con or connect()
    try:
        cols = ", ".join(fields)
        c.execute(
            f"INSERT INTO {kind} ({cols}) VALUES ({', '.join('?' for _ in fields)})",
            list(fields.values()),
        )
    finally:
        if not own:
            c.close()
    return fields["fact_id"]


def effective_cell_overrides(
    con: duckdb.DuckDBPyConnection, table_name: str | None = None
):
    """Newest-wins per (table, target_key, column). Same-timestamp collision = FactConflict."""
    where = "WHERE table_name = ?" if table_name else ""
    params = [table_name] if table_name else []
    dup = con.execute(
        f"""SELECT table_name, target_key, column_name, created_at, COUNT(*)
            FROM cell_override {where}
            GROUP BY 1,2,3,4 HAVING COUNT(*) > 1""",
        params,
    ).fetchall()
    if dup:
        raise FactConflict(
            f"{len(dup)} same-timestamp fact collisions, first: {dup[0][:3]}"
        )
    return con.execute(
        f"""SELECT * FROM (
              SELECT *, ROW_NUMBER() OVER (
                PARTITION BY table_name, target_key, column_name
                ORDER BY created_at DESC) AS rn
              FROM cell_override {where}
            ) WHERE rn = 1""",
        params,
    ).fetchall()


def assert_fact_applies(current_value, old_value) -> None:
    """STALE guard: a cell_override only applies to the value it was adjudicated against."""
    cur = None if current_value is None else str(current_value)
    old = None if old_value is None else str(old_value)
    if cur != old:
        raise StaleFact(
            f"current value {cur!r} != asserted old_value {old!r} -> re-adjudicate"
        )


def counts(con: duckdb.DuckDBPyConnection) -> dict:
    return {k: con.execute(f"SELECT COUNT(*) FROM {k}").fetchone()[0] for k in SCHEMAS}


if __name__ == "__main__":
    c = connect()
    print(f"facts db: {FACTS_DB}")
    print(json.dumps(counts(c), indent=2))
    c.close()
