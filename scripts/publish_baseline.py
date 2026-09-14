#!/usr/bin/env python3
"""Capture and diff publish baselines — the guard rail for every rehearsal.

Nothing may touch live data (in tests, rehearsals, or the first live Tuesday)
until a baseline exists, and every write must be provable against one:

    # 1. Capture (read-only; works against a local file or the Fly API)
    python scripts/publish_baseline.py capture --local path/___leagues.duckdb --out before.json
    python scripts/publish_baseline.py capture --fly --database ___leagues --out before.json

    # 2. ...run the rehearsal / publish...

    # 3. Diff, optionally proving changes stayed inside a bundle's declared scope
    python scripts/publish_baseline.py diff before.json after.json
    python scripts/publish_baseline.py diff before.json after.json --manifest bundle_manifest.json

Capture levels:
- light: per-table row counts + per-scope (db_name[, year]) row counts. Cheap
  single scans; safe against the serving box.
- full: adds an order-independent content checksum per scope (SUM of per-row
  hashes), so unchanged scopes are proven byte-stable, not just count-stable.

Diff exit codes: 0 = no changes / all changes inside declared scope,
1 = changes outside declared scope (or any change when no manifest given),
2 = usage/comparison error.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

BASELINE_VERSION = 1

# Internal tables (leading underscore) are excluded from capture.
_EXCLUDED_TABLE_PREFIXES = ("_",)


def qident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


class LocalSource:
    """Read-only DuckDB file source."""

    def __init__(self, path: str, schema: str = "public"):
        import duckdb

        self.conn = duckdb.connect(path, read_only=True)
        self.schema = schema
        self.label = f"local:{path}"

    def query(self, sql: str) -> list[dict]:
        cur = self.conn.execute(sql)
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]

    def close(self) -> None:
        self.conn.close()


class FlySource:
    """Read-only Fly API source (POST /query with the read token)."""

    def __init__(self, database: str, schema: str = "public"):
        import requests

        self._requests = requests
        self.url = os.environ.get("DATABASE_SERVER_URL", "").rstrip("/")
        self.token = os.environ.get("DATABASE_READ_TOKEN", "")
        if not self.url or not self.token:
            raise RuntimeError("DATABASE_SERVER_URL and DATABASE_READ_TOKEN must be set for --fly")
        self.database = database
        self.schema = schema
        self.label = f"fly:{database}"

    def query(self, sql: str) -> list[dict]:
        for attempt in range(5):
            resp = self._requests.post(
                f"{self.url}/query",
                json={"sql": sql, "database": self.database},
                headers={"Authorization": f"Bearer {self.token}"},
                timeout=60,
            )
            if resp.status_code in {429, 503} and attempt < 4:
                time.sleep(min(2**attempt, 15))
                continue
            if resp.status_code != 200:
                raise RuntimeError(f"Query failed ({resp.status_code}): {resp.text[:300]}")
            return resp.json()
        raise RuntimeError("Query exhausted retries")

    def close(self) -> None:
        pass


def list_tables(source, schema: str) -> dict[str, list[str]]:
    rows = source.query(
        f"""
        SELECT table_name, column_name
        FROM information_schema.columns
        WHERE table_schema = '{schema}'
        ORDER BY table_name, ordinal_position
        """
    )
    tables: dict[str, list[str]] = {}
    for row in rows:
        tables.setdefault(str(row["table_name"]), []).append(str(row["column_name"]))
    return {t: cols for t, cols in tables.items() if not t.startswith(_EXCLUDED_TABLE_PREFIXES)}


def scope_keys_for(columns: list[str]) -> list[str]:
    keys = []
    if "db_name" in columns:
        keys.append("db_name")
    if "year" in columns:
        keys.append("year")
    return keys


def checksum_expr(columns: list[str]) -> str:
    """Order-independent content checksum: sum of per-row hashes (one scan)."""
    row_expr = " || chr(31) || ".join(f"COALESCE(CAST({qident(c)} AS VARCHAR), '<NULL>')" for c in columns)
    return f"CAST(SUM(CAST(hash({row_expr}) AS HUGEINT)) AS VARCHAR)"


def capture_table(source, schema: str, table: str, columns: list[str], level: str) -> dict[str, Any]:
    ref = f"{qident(schema)}.{qident(table)}"
    keys = scope_keys_for(columns)
    entry: dict[str, Any] = {"columns": columns, "scope_keys": keys, "scopes": {}}

    select_metrics = "COUNT(*)::BIGINT AS rows"
    if level == "full":
        select_metrics += f", {checksum_expr(columns)} AS checksum"

    if keys:
        key_select = ", ".join(qident(k) for k in keys)
        rows = source.query(f"SELECT {key_select}, {select_metrics} FROM {ref} GROUP BY {key_select}")
        for row in rows:
            scope_id = "\x1f".join(str(row[k]) for k in keys)
            entry["scopes"][scope_id] = {
                "rows": int(row["rows"]),
                **({"checksum": row["checksum"]} if level == "full" else {}),
            }
    else:
        rows = source.query(f"SELECT {select_metrics} FROM {ref}")
        entry["scopes"]["<table>"] = {
            "rows": int(rows[0]["rows"]),
            **({"checksum": rows[0]["checksum"]} if level == "full" else {}),
        }
    entry["total_rows"] = sum(s["rows"] for s in entry["scopes"].values())
    return entry


def cmd_capture(args) -> int:
    if args.local:
        source = LocalSource(args.local, schema=args.schema)
    elif args.fly:
        source = FlySource(args.database, schema=args.schema)
    else:
        print("capture requires --local <path> or --fly", file=sys.stderr)
        return 2

    started = time.perf_counter()
    try:
        tables = list_tables(source, args.schema)
        if args.tables:
            requested = set(args.tables.split(","))
            missing = requested - set(tables)
            if missing:
                print(f"Tables not found: {', '.join(sorted(missing))}", file=sys.stderr)
                return 2
            tables = {t: cols for t, cols in tables.items() if t in requested}

        baseline: dict[str, Any] = {
            "baseline_version": BASELINE_VERSION,
            "captured_at": datetime.now(timezone.utc).isoformat(),
            "source": source.label,
            "schema": args.schema,
            "level": args.level,
            "tables": {},
        }
        for table, columns in sorted(tables.items()):
            t0 = time.perf_counter()
            baseline["tables"][table] = capture_table(source, args.schema, table, columns, args.level)
            print(
                f"[baseline] {table}: {baseline['tables'][table]['total_rows']:,} rows, "
                f"{len(baseline['tables'][table]['scopes'])} scopes ({time.perf_counter() - t0:.2f}s)",
                flush=True,
            )
        baseline["elapsed_seconds"] = round(time.perf_counter() - started, 2)
    finally:
        source.close()

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(baseline, indent=2, sort_keys=True), encoding="utf-8")
    print(f"[baseline] wrote {out} ({len(baseline['tables'])} tables, {baseline['elapsed_seconds']}s)")
    return 0


def _load_manifest_scopes(manifest_path: str) -> tuple[dict[str, dict[str, Any]], set[str] | None]:
    """Extract per-table allowed scopes + the batched db_name set from a manifest."""
    manifest = json.loads(Path(manifest_path).read_text(encoding="utf-8"))
    allowed: dict[str, dict[str, Any]] = {}
    for entry in manifest.get("tables", []):
        table = str(entry.get("table"))
        allowed[table] = {
            "cadence_class": entry.get("cadence_class"),
            "scope": entry.get("scope") or {},
            "db_names_hash": entry.get("db_names_hash"),
            "db_name_count": entry.get("db_name_count"),
        }
    raw_db_names = manifest.get("db_names")
    db_names = {str(n) for n in raw_db_names} if isinstance(raw_db_names, list) else None
    return allowed, db_names


def _scope_allowed(
    table: str,
    scope_id: str,
    scope_keys: list[str],
    allowed: dict[str, dict[str, Any]],
    db_names: set[str] | None,
) -> bool:
    """May this (table, scope) legitimately change under the manifest?

    Bounds by table, by year for active_season tables, and — when the manifest
    carries the batched ``db_names`` list — by league membership, so a delete
    that widened to an excluded league is flagged as a violation.
    """
    if table not in allowed:
        return False
    entry = allowed[table]
    parts = scope_id.split("\x1f")
    if db_names is not None and "db_name" in scope_keys:
        if parts[scope_keys.index("db_name")] not in db_names:
            return False
    scope_year = entry["scope"].get("year")
    if scope_year is None:
        return True  # league_rollup: any year slice of a batched league may change
    if "year" not in scope_keys:
        return True
    return str(parts[scope_keys.index("year")]) == str(scope_year)


def cmd_diff(args) -> int:
    before = json.loads(Path(args.before).read_text(encoding="utf-8"))
    after = json.loads(Path(args.after).read_text(encoding="utf-8"))
    allowed, manifest_db_names = _load_manifest_scopes(args.manifest) if args.manifest else (None, None)

    if before.get("schema") != after.get("schema"):
        print("Baselines are for different schemas", file=sys.stderr)
        return 2

    changes: list[dict[str, Any]] = []
    violations: list[dict[str, Any]] = []

    all_tables = sorted(set(before["tables"]) | set(after["tables"]))
    for table in all_tables:
        b = before["tables"].get(table)
        a = after["tables"].get(table)
        if b is None or a is None:
            change = {"table": table, "kind": "table_added" if b is None else "table_removed"}
            changes.append(change)
            if allowed is not None and table not in (allowed or {}):
                violations.append(change)
            continue
        scope_keys = a.get("scope_keys") or b.get("scope_keys") or []
        scope_ids = set(b["scopes"]) | set(a["scopes"])
        for scope_id in sorted(scope_ids):
            sb = b["scopes"].get(scope_id)
            sa = a["scopes"].get(scope_id)
            if sb == sa:
                continue
            kind = "scope_added" if sb is None else "scope_removed" if sa is None else "scope_changed"
            change = {
                "table": table,
                "scope": dict(zip(scope_keys, scope_id.split("\x1f"))) if scope_keys else scope_id,
                "kind": kind,
                "rows_before": (sb or {}).get("rows"),
                "rows_after": (sa or {}).get("rows"),
            }
            changes.append(change)
            if allowed is not None and not _scope_allowed(table, scope_id, scope_keys, allowed, manifest_db_names):
                violations.append(change)

    report = {
        "before": {"source": before.get("source"), "captured_at": before.get("captured_at")},
        "after": {"source": after.get("source"), "captured_at": after.get("captured_at")},
        "manifest": args.manifest,
        "change_count": len(changes),
        "violation_count": len(violations) if allowed is not None else None,
        "changes": changes[: args.max_changes],
        "violations": violations[: args.max_changes] if allowed is not None else None,
    }
    if args.out:
        Path(args.out).write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps({k: report[k] for k in ("change_count", "violation_count")}, indent=2))
    for change in (violations if allowed is not None else changes)[:20]:
        print(f"  ! {change}")

    if allowed is not None:
        return 1 if violations else 0
    return 1 if changes else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)

    cap = sub.add_parser("capture", help="Capture a read-only baseline")
    cap.add_argument("--local", help="Path to a local DuckDB file")
    cap.add_argument("--fly", action="store_true", help="Read from the Fly API (env: DATABASE_SERVER_URL/READ_TOKEN)")
    cap.add_argument("--database", default="___leagues", help="Fly database name")
    cap.add_argument("--schema", default="public")
    cap.add_argument("--tables", help="Comma-separated subset (default: all)")
    cap.add_argument("--level", choices=["light", "full"], default="light")
    cap.add_argument("--out", required=True)
    cap.set_defaults(func=cmd_capture)

    diff = sub.add_parser("diff", help="Diff two baselines, optionally against a bundle manifest")
    diff.add_argument("before")
    diff.add_argument("after")
    diff.add_argument("--manifest", help="Fleet bundle manifest declaring allowed scopes")
    diff.add_argument("--out", help="Write full diff report JSON")
    diff.add_argument("--max-changes", type=int, default=500)
    diff.set_defaults(func=cmd_diff)

    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
