"""Pre-deploy migration: keeper_config.rules_json (JSON blob) → 40 flat columns.

The keeper_config table is centralized at ``___leagues.public.keeper_config``
(one table, keyed by db_name) — NOT per-league. So this is a single ALTER +
single read + single backfill + single DROP, not a per-league loop.

Idempotent:
- Table missing or rules_json column already gone → skipped.
- Otherwise: snapshot existing rows to local JSONL, ALTER TABLE add columns,
  typed UPDATEs from the parsed JSON, ALTER TABLE DROP rules_json.

Per spec: NO backup table inside the league DB; recovery is via the local
snapshot artifact + user re-saves via wizard.
"""

from __future__ import annotations

import argparse
import enum
import json
import logging
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import IO

import pandas as pd

# Make multi_league importable when run directly from scripts/
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "fantasy_football_data_scripts"))

from multi_league.core.keeper_config_flatten import flatten_rules_to_columns  # noqa: E402
from multi_league.core.keeper_config_schema import (  # noqa: E402
    KEEPER_CONFIG_COLUMN_TYPES,
    KEEPER_CONFIG_FIELD_COLUMNS,
)

log = logging.getLogger("migrate_keeper_config")

KEEPER_CONFIG_TABLE = "___leagues.public.keeper_config"


class SkipReason(enum.Enum):
    no_table = "no_table"
    already_migrated = "already_migrated"


@dataclass
class MigrationResult:
    rows_migrated: int
    invalid: int


def migrate_centralized(
    fly_reader,
    fly_writer,
    snapshot_writer: IO[str],
) -> SkipReason | MigrationResult:
    """Migrate the centralized keeper_config table from rules_json → flat DDL.

    Single-shot — keeper_config is one table for all leagues, keyed by db_name.

    Returns SkipReason if the table is missing or already migrated, or
    MigrationResult with row counts.
    """
    # 1. Detect schema
    cols = fly_reader.query_df(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'keeper_config' AND table_catalog = '___leagues'",
        database="___leagues",
    )
    if cols.empty:
        return SkipReason.no_table
    if "rules_json" not in cols["column_name"].values:
        return SkipReason.already_migrated

    # 2. Read all rows before any writes (across all leagues — single table)
    rows = fly_reader.query_df(
        f"SELECT db_name, year, rules_json, updated_at FROM {KEEPER_CONFIG_TABLE}",
        database="___leagues",
    )

    # 3. Snapshot BEFORE any destructive operation
    now_iso = datetime.now(UTC).isoformat()
    for row in rows.to_dict("records"):
        snapshot_writer.write(
            json.dumps(
                {
                    "db_name": row["db_name"],
                    "year": int(row["year"]),
                    "rules_json": row["rules_json"],
                    "updated_at": str(row["updated_at"]),
                    "snapshot_at": now_iso,
                }
            )
            + "\n"
        )
    snapshot_writer.flush()

    # 4. Parse + validate each row
    parsed_rows: list[dict] = []
    invalid_count = 0
    for _, row in rows.iterrows():
        raw = row["rules_json"]
        # NULL/NaN rules_json → no rules configured; treat as empty (not invalid).
        if pd.isna(raw) if not isinstance(raw, str) else not raw:
            blob = {}
        else:
            try:
                blob = json.loads(raw)
            except (json.JSONDecodeError, TypeError) as e:
                log.warning(f"[MIGRATE] {row['db_name']} year={row['year']}: malformed rules_json — {e}")
                invalid_count += 1
                continue
        try:
            flat = flatten_rules_to_columns(blob, strict=False)
        except Exception as e:
            log.warning(f"[MIGRATE] {row['db_name']} year={row['year']}: flatten failed — {e}")
            invalid_count += 1
            continue
        flat["db_name"] = row["db_name"]
        flat["year"] = int(row["year"])
        # `updated_at` is a key column (not in KEEPER_CONFIG_FIELD_COLUMNS); the
        # per-row UPDATE preserves the original timestamp by not setting it.
        parsed_rows.append(flat)

    # 5. Schema migration: ALTER TABLE ADD COLUMN (one statement per column,
    #    IF NOT EXISTS so the script is safely re-runnable mid-flight).
    for col in KEEPER_CONFIG_FIELD_COLUMNS:
        col_type = KEEPER_CONFIG_COLUMN_TYPES[col]
        fly_writer.execute(
            f"ALTER TABLE {KEEPER_CONFIG_TABLE} ADD COLUMN IF NOT EXISTS {col} {col_type}",
            database="___leagues",
        )

    # 6. Typed UPDATEs per row
    for flat in parsed_rows:
        set_clauses = []
        for col in KEEPER_CONFIG_FIELD_COLUMNS:
            v = flat.get(col)
            if v is None:
                set_clauses.append(f"{col} = NULL")
            elif isinstance(v, bool):
                set_clauses.append(f"{col} = {'TRUE' if v else 'FALSE'}")
            elif isinstance(v, (int, float)):
                set_clauses.append(f"{col} = {v}")
            else:
                escaped = str(v).replace("'", "''")
                set_clauses.append(f"{col} = '{escaped}'")
        db_name_esc = flat["db_name"].replace("'", "''")
        sql = (
            f"UPDATE {KEEPER_CONFIG_TABLE} SET {', '.join(set_clauses)} "
            f"WHERE db_name = '{db_name_esc}' AND year = {flat['year']}"
        )
        fly_writer.execute(sql, database="___leagues")

    # 7. Drop the now-migrated rules_json column
    fly_writer.execute(
        f"ALTER TABLE {KEEPER_CONFIG_TABLE} DROP COLUMN IF EXISTS rules_json",
        database="___leagues",
    )

    return MigrationResult(
        rows_migrated=len(parsed_rows),
        invalid=invalid_count,
    )


def main():
    parser = argparse.ArgumentParser(description="Migrate keeper_config.rules_json blob → 40 flat DDL columns.")
    parser.add_argument(
        "--snapshot-path",
        default=None,
        help="Path for JSONL snapshot (default: scripts/keeper_config_snapshot_pre_migration_<date>.jsonl)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Detect only, no writes")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    date_str = datetime.now(UTC).strftime("%Y_%m_%d")
    snapshot_path = args.snapshot_path or f"scripts/keeper_config_snapshot_pre_migration_{date_str}.jsonl"
    Path(snapshot_path).parent.mkdir(parents=True, exist_ok=True)

    from multi_league.core.fly_writer import FlyWriter  # noqa: PLC0415
    from multi_league.core.readers.fly_reader import FlyReader  # noqa: PLC0415

    reader = FlyReader()
    writer = FlyWriter()

    if args.dry_run:
        cols = reader.query_df(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name = 'keeper_config' AND table_catalog = '___leagues'",
            database="___leagues",
        )
        if cols.empty:
            log.info("[DRY-RUN] keeper_config table missing — nothing to migrate")
        elif "rules_json" not in cols["column_name"].values:
            log.info("[DRY-RUN] keeper_config already migrated (no rules_json column)")
        else:
            row_count = reader.query_df(
                f"SELECT COUNT(*) AS n FROM {KEEPER_CONFIG_TABLE}",
                database="___leagues",
            ).iloc[0]["n"]
            log.info(f"[DRY-RUN] would migrate {row_count} rules_json row(s) → flat columns")
        return

    log.info(f"[MIGRATE] snapshot → {snapshot_path}")
    with open(snapshot_path, "a") as snapshot_fh:
        try:
            result = migrate_centralized(reader, writer, snapshot_fh)
        except Exception as e:
            log.error(f"[FAIL] {e}")
            sys.exit(1)

    if isinstance(result, SkipReason):
        log.info(f"[SKIP] {result.value}")
    else:
        log.info(f"[OK] {result.rows_migrated} migrated, {result.invalid} invalid")


if __name__ == "__main__":
    main()
