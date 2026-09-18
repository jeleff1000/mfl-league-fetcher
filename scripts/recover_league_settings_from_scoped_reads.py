"""Reconstruct a corrupt league_settings table through scoped, readable rows.

This recovery helper never scans league_settings broadly and never replaces the
database.  It discovers league names from intact canonical inventories, reads
each league's settings independently, validates complete row coverage against
DuckDB's table cardinality, and optionally submits one table-only replacement.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path
from typing import Callable

import duckdb
import pandas as pd
import requests


def _load_env(path: Path) -> None:
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _query(server: str, token: str, database: str, sql: str) -> list[dict]:
    response = requests.post(
        f"{server.rstrip('/')}/query",
        json={"database": database, "sql": sql},
        headers={"Authorization": f"Bearer {token}"},
        timeout=90,
    )
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, list):
        raise RuntimeError("Fly query returned a non-list payload")
    return payload


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _read_scoped_batches(
    names: list[str],
    query: Callable[[str], list[dict]],
    *,
    batch_size: int = 100,
    tolerate_unreadable: bool = False,
) -> tuple[list[dict], set[str], set[str]]:
    """Read settings with predicate-pruned batches and isolate only failures."""
    if batch_size < 1:
        raise ValueError("batch_size must be positive")

    rows: list[dict] = []
    populated: set[str] = set()
    unreadable: set[str] = set()

    def read_batch(batch: list[str]) -> None:
        predicate = ",".join(_sql_literal(name) for name in batch)
        try:
            scoped = query(
                "SELECT * FROM public.league_settings "
                f"WHERE db_name IN ({predicate}) ORDER BY db_name, year"
            )
        except Exception as exc:
            if len(batch) == 1:
                if tolerate_unreadable:
                    unreadable.add(batch[0])
                    return
                raise RuntimeError(
                    f"Unable to recover league_settings for db_name={batch[0]!r}"
                ) from exc
            midpoint = len(batch) // 2
            read_batch(batch[:midpoint])
            read_batch(batch[midpoint:])
            return
        rows.extend(scoped)
        populated.update(
            str(row["db_name"])
            for row in scoped
            if row.get("db_name") is not None
        )

    for start in range(0, len(names), batch_size):
        read_batch(names[start : start + batch_size])
    return rows, populated, unreadable


def _recover_yahoo_settings_rows(
    database_name: str,
    *,
    server: str,
    read_token: str,
) -> list[dict]:
    """Regenerate one unreadable Yahoo settings chain through canonical fetchers."""
    safe_name = _sql_literal(database_name)
    context_rows = _query(
        server,
        read_token,
        "___leagues",
        "SELECT platform FROM public.league_context "
        f"WHERE db_name={safe_name} LIMIT 1",
    )
    if not context_rows or str(context_rows[0].get("platform") or "").lower() != "yahoo":
        raise RuntimeError(
            f"Unreadable league_settings identity {database_name!r} is not recoverable as Yahoo OAuth"
        )
    expected_years = {
        int(row["year"])
        for row in _query(
            server,
            read_token,
            "___leagues",
            "SELECT DISTINCT year FROM public.matchup "
            f"WHERE db_name={safe_name} AND year IS NOT NULL ORDER BY year",
        )
    }
    if not expected_years:
        raise RuntimeError(f"No canonical seasons found for {database_name!r}")

    root = Path(__file__).resolve().parents[1]
    fantasy_root = root / "fantasy_football_data_scripts"
    if str(fantasy_root) not in sys.path:
        sys.path.insert(0, str(fantasy_root))
    from initial_import_v3 import _build_context_from_fly, _get_shared_yahoo_session
    from multi_league.core.canonical_settings import flatten_settings
    from multi_league.core.yahoo_league_settings import (
        discover_league_history,
        fetch_league_settings,
    )

    with tempfile.TemporaryDirectory(prefix="settings_recovery_") as temp_dir:
        ctx, _ = _build_context_from_fly(database_name, data_dir_override=temp_dir)
        oauth = _get_shared_yahoo_session(ctx, Path(ctx.oauth_file_path))
        if oauth is None:
            raise RuntimeError(f"No Yahoo OAuth session available for {database_name!r}")
        if not oauth.token_is_valid():
            oauth.refresh_access_token()
        chain = discover_league_history(
            ctx.league_id,
            oauth=oauth,
            start_year=min(expected_years),
            end_year=max(expected_years),
        )
        discovered_years = {int(year) for year in chain}
        if discovered_years != expected_years:
            raise RuntimeError(
                f"Yahoo renewal-chain mismatch for {database_name!r}: "
                f"expected={sorted(expected_years)}, discovered={sorted(discovered_years)}"
            )
        recovered: list[dict] = []
        for year in sorted(expected_years):
            league_key = chain[str(year)]
            raw = fetch_league_settings(year, league_key=league_key, oauth=oauth)
            if not raw:
                raise RuntimeError(
                    f"Yahoo returned no settings for {database_name!r} season {year}"
                )
            row = flatten_settings(raw, "yahoo", year, league_key)
            row["db_name"] = database_name
            recovered.append(row)
    return recovered


def reconstruct_bundle(*, server: str, read_token: str, output: Path) -> dict:
    name_rows = _query(
        server,
        read_token,
        "___leagues",
        "SELECT DISTINCT db_name FROM ("
        "SELECT db_name FROM public.league_context "
        "UNION ALL SELECT db_name FROM public.matchup "
        "UNION ALL SELECT db_name FROM public.player_fantasy "
        "UNION ALL SELECT db_name FROM public.draft "
        "UNION ALL SELECT db_name FROM public.transactions "
        "UNION ALL SELECT db_name FROM public.schedule"
        ") WHERE db_name IS NOT NULL AND TRIM(db_name) <> '' ORDER BY db_name",
    )
    inventory_rows = _query(
        server,
        read_token,
        "___ops",
        "SELECT DISTINCT database_name AS db_name FROM accounts.league_inventory "
        "WHERE database_name IS NOT NULL AND TRIM(database_name) <> ''",
    )
    names = sorted({str(row["db_name"]) for row in [*name_rows, *inventory_rows]})
    if not names:
        raise RuntimeError("No league identities were discovered")

    physical_row_estimate = int(
        _query(
            server,
            read_token,
            "___leagues",
            "SELECT estimated_size AS rows FROM duckdb_tables() "
            "WHERE database_name='___leagues' AND schema_name='public' "
            "AND table_name='league_settings'",
        )[0]["rows"]
    )
    ddl = str(
        _query(
            server,
            read_token,
            "___leagues",
            "SELECT sql FROM duckdb_tables() WHERE database_name='___leagues' "
            "AND schema_name='public' AND table_name='league_settings'",
        )[0]["sql"]
    )
    columns = [
        str(row["column_name"])
        for row in _query(
            server,
            read_token,
            "___leagues",
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_catalog='___leagues' AND table_schema='public' "
            "AND table_name='league_settings' ORDER BY ordinal_position",
        )
    ]
    if not columns:
        raise RuntimeError("league_settings schema is unavailable")

    rows, populated_names, unreadable_names = _read_scoped_batches(
        names,
        lambda sql: _query(server, read_token, "___leagues", sql),
        tolerate_unreadable=True,
    )
    for name in sorted(unreadable_names):
        recovered = _recover_yahoo_settings_rows(
            name,
            server=server,
            read_token=read_token,
        )
        rows.extend(recovered)
        if recovered:
            populated_names.add(name)

    keys = [(str(row.get("db_name")), int(row.get("year"))) for row in rows]
    if not keys:
        raise RuntimeError("Scoped settings reconstruction is empty")
    if len(keys) != len(set(keys)):
        raise RuntimeError("Scoped settings reconstruction contains duplicate (db_name, year) keys")
    expected_rows = len(keys)

    output.parent.mkdir(parents=True, exist_ok=True)
    output.unlink(missing_ok=True)
    conn = duckdb.connect(str(output))
    try:
        conn.execute("CREATE SCHEMA public")
        conn.execute("SET schema='public'")
        normalized_ddl = re.sub(
            r"(?i)^CREATE TABLE\s+(?:\"?public\"?\.)?",
            "CREATE TABLE public.",
            ddl,
            count=1,
        )
        conn.execute(normalized_ddl)
        quoted = ", ".join('"' + column.replace('"', '""') + '"' for column in columns)
        frame = pd.DataFrame.from_records(rows, columns=columns)
        conn.register("_settings_recovery_rows", frame)
        conn.execute(
            f"INSERT INTO public.league_settings ({quoted}) "
            f"SELECT {quoted} FROM _settings_recovery_rows"
        )
        conn.unregister("_settings_recovery_rows")
        verified = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT (db_name, year)) FROM public.league_settings"
        ).fetchone()
        if verified != (expected_rows, expected_rows):
            raise RuntimeError(f"Bundle verification failed: {verified!r}")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    return {
        "discovered_leagues": len(names),
        "populated_leagues": len(populated_names),
        "regenerated_leagues": sorted(unreadable_names),
        "rows": expected_rows,
        "physical_row_estimate": physical_row_estimate,
        "sha256": hashlib.sha256(output.read_bytes()).hexdigest(),
        "bytes": output.stat().st_size,
    }


def apply_bundle(*, server: str, admin_token: str, output: Path, receipt: dict) -> dict:
    with output.open("rb") as handle:
        response = requests.post(
            f"{server.rstrip('/')}/replace-canonical-table",
            headers={
                "Authorization": f"Bearer {admin_token}",
                "X-Db-Name": "___leagues",
                "X-Table-Name": "league_settings",
                "X-Expected-Rows": str(receipt["rows"]),
                "X-Content-Sha256": str(receipt["sha256"]),
            },
            files={"file": (output.name, handle, "application/octet-stream")},
            timeout=180,
        )
    response.raise_for_status()
    result = response.json()
    if result.get("status") != "replaced" or int(result.get("rows", -1)) != receipt["rows"]:
        raise RuntimeError(f"Unexpected replacement response: {result!r}")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.env_file:
        _load_env(args.env_file)
    server = os.environ["DATABASE_SERVER_URL"]
    read_token = os.environ["DATABASE_READ_TOKEN"]
    receipt = reconstruct_bundle(server=server, read_token=read_token, output=args.output)
    result = {"bundle": receipt}
    if args.execute:
        result["replacement"] = apply_bundle(
            server=server,
            admin_token=os.environ["DATABASE_ADMIN_TOKEN"],
            output=args.output,
            receipt=receipt,
        )
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
