"""Build local DuckDB cache of ___ops tables for fleet imports.

Downloads nfl_player_stats_all and player_bio into a single DuckDB file using
explicit column lists. Zero column tracking: if it's in ___ops, it's cached.

Fly-only: the live source of truth is the Fly DuckDB API. No legacy database
fallbacks are allowed for import workers.

Usage:
    python build_ops_cache.py --output ops_cache/ops_cache.duckdb
    python build_ops_cache.py --output ops_cache/ops_cache.duckdb --update-existing
"""

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import json
import logging
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path

import duckdb

TABLES = [
    "nfl_player_stats_all",
    "player_bio",
]

_CACHE_DATA_TABLES = {("nfl_historical", table) for table in TABLES}
_CACHE_ADMIN_TABLES = {("main", "ops_cache_metadata")}
_ALLOWED_CACHE_TABLES = _CACHE_DATA_TABLES | _CACHE_ADMIN_TABLES
_CHECKPOINT_VERSION = "ops-cache-checkpoint-v4"
_STATS_BATCH_SIZE = 1
_STATS_BATCH_COUNT = 110
_YEAR_PAGE_SIZE = 10_000


def _quote_ident(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


def _duckdb_type(data_type: str) -> str:
    """Return a trusted DuckDB type string from information_schema."""
    type_sql = str(data_type or "VARCHAR").strip()
    if not type_sql:
        return "VARCHAR"
    if not re.fullmatch(r"[A-Za-z0-9_(),\[\] ]+", type_sql):
        raise ValueError(f"Unsafe DuckDB data type from schema: {data_type!r}")
    return type_sql


def _schema_columns(schema_rows: list[dict]) -> list[str]:
    return [str(r["column_name"]) for r in schema_rows]


def _remote_schema_sql(table: str) -> str:
    """Return schema SQL scoped to the Fly database selected for the query.

    The Fly service may attach the same ops tables under more than one catalog.
    DuckDB's information_schema spans every attached catalog, so omitting the
    catalog predicate duplicates each column and produces an invalid local DDL.
    """
    if table not in TABLES:
        raise ValueError(f"Unsupported ops cache table: {table}")
    return (
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_catalog = current_database() "
        "AND table_schema = 'nfl_historical' "
        f"AND table_name = '{table}' ORDER BY ordinal_position"
    )


def _create_table_from_schema(conn: duckdb.DuckDBPyConnection, table: str, schema_rows: list[dict]) -> None:
    """Create the local cache table from Fly's schema instead of pandas inference."""
    column_defs = [f'{_quote_ident(r["column_name"])} {_duckdb_type(str(r["data_type"]))}' for r in schema_rows]
    conn.execute(f"CREATE TABLE nfl_historical.{table} ({', '.join(column_defs)})")


def _insert_rows(conn: duckdb.DuckDBPyConnection, table: str, columns: list[str], rows: list[dict]) -> None:
    """Insert rows by explicit column list so chunk schemas cannot drift."""
    if not rows:
        return

    import pandas as pd

    df = pd.DataFrame(rows)
    df = df.reindex(columns=columns)
    select_cols = ", ".join(_quote_ident(c) for c in columns)
    conn.execute(
        f"""
        INSERT INTO nfl_historical.{table} ({select_cols})
        SELECT {select_cols} FROM df
        """
    )


def _table_exists(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> bool:
    return bool(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = ? AND table_name = ?
            """,
            [schema, table],
        ).fetchone()[0]
    )


def _local_columns(conn: duckdb.DuckDBPyConnection, schema: str, table: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = ? AND table_name = ?
        ORDER BY ordinal_position
        """,
        [schema, table],
    ).fetchall()
    return [str(r[0]) for r in rows]


def _prune_non_cache_tables(conn: duckdb.DuckDBPyConnection) -> None:
    """Keep ops_cache.duckdb scoped to the two intended data tables."""
    rows = conn.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('information_schema', 'pg_catalog')
        """
    ).fetchall()
    for schema, table in rows:
        key = (str(schema), str(table))
        if key in _ALLOWED_CACHE_TABLES:
            continue
        conn.execute(f"DROP TABLE IF EXISTS {_quote_ident(schema)}.{_quote_ident(table)}")
        print(f"[ops-cache] Pruned non-cache table: {schema}.{table}")


def _schema_fingerprint(schema_rows: list[dict]) -> str:
    payload = [(str(row["column_name"]), str(row["data_type"])) for row in schema_rows]
    encoded = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode()
    return hashlib.sha256(encoded).hexdigest()


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _checkpoint_manifest_path(path: Path) -> Path:
    return path.with_suffix(".json")


def checkpoint_batch_id(table: str, index: int | str) -> str:
    if table == "player_bio":
        return "player_bio-all"
    return f"{table}-batch-{int(index):02d}"


def write_checkpoint(
    path: Path,
    *,
    table: str,
    batch_id: str,
    schema_rows: list[dict],
    rows: list[dict],
    years: list[int],
) -> dict:
    """Write one independently restorable DuckDB checkpoint and manifest."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.unlink(missing_ok=True)
    _checkpoint_manifest_path(path).unlink(missing_ok=True)

    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE SCHEMA nfl_historical")
        _create_table_from_schema(conn, table, schema_rows)
        _insert_rows(conn, table, _schema_columns(schema_rows), rows)
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    year_counts: dict[str, int] = {}
    if years:
        for row in rows:
            year = row.get("year")
            if year is not None:
                key = str(int(year))
                year_counts[key] = year_counts.get(key, 0) + 1
    manifest = {
        "version": _CHECKPOINT_VERSION,
        "batch_id": batch_id,
        "table": table,
        "schema_rows": schema_rows,
        "schema_fingerprint": _schema_fingerprint(schema_rows),
        "years": sorted(int(year) for year in years),
        "year_counts": year_counts,
        "row_count": len(rows),
        "file_sha256": _file_sha256(path),
    }
    _checkpoint_manifest_path(path).write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    return manifest


def _read_checkpoint_manifest(path: Path) -> dict:
    manifest_path = _checkpoint_manifest_path(path)
    if not manifest_path.exists():
        return {}
    try:
        return json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def validate_checkpoint(path: Path, manifest: dict) -> bool:
    """Validate checkpoint file integrity, schema, years, and row counts."""
    path = Path(path)
    if not path.is_file() or manifest.get("version") != _CHECKPOINT_VERSION:
        return False
    if manifest.get("file_sha256") != _file_sha256(path):
        return False

    table = str(manifest.get("table", ""))
    schema_rows = manifest.get("schema_rows") or []
    if not table or not schema_rows:
        return False
    try:
        conn = duckdb.connect(str(path), read_only=True)
        try:
            actual_schema = [
                {"column_name": row[0], "data_type": row[1]}
                for row in conn.execute(f"DESCRIBE nfl_historical.{_quote_ident(table)}").fetchall()
            ]
            if _schema_fingerprint(actual_schema) != manifest.get("schema_fingerprint"):
                return False
            row_count = conn.execute(f"SELECT COUNT(*) FROM nfl_historical.{_quote_ident(table)}").fetchone()[0]
            if int(row_count) != int(manifest.get("row_count", -1)):
                return False
            expected_years = sorted(int(year) for year in manifest.get("years", []))
            if expected_years:
                actual_years = [
                    int(row[0])
                    for row in conn.execute(
                        f"SELECT DISTINCT year FROM nfl_historical.{_quote_ident(table)} ORDER BY year"
                    ).fetchall()
                ]
                if actual_years != expected_years:
                    return False
        finally:
            conn.close()
    except (duckdb.Error, OSError, ValueError):
        return False
    return True


def _manifest_matches_expected(local: dict, expected: dict) -> bool:
    if not local or local.get("table") != expected.get("table"):
        return False
    schema_rows = expected.get("schema_rows") or []
    if _schema_fingerprint(schema_rows) != local.get("schema_fingerprint"):
        return False
    if sorted(int(year) for year in expected.get("years", [])) != sorted(
        int(year) for year in local.get("years", [])
    ):
        return False
    if "row_count" in expected and int(expected["row_count"]) != int(local.get("row_count", -1)):
        return False
    if expected.get("year_counts") and expected.get("year_counts") != local.get("year_counts"):
        return False
    return True


def expected_checkpoint_ids() -> list[str]:
    return [
        *(checkpoint_batch_id("nfl_player_stats_all", index) for index in range(_STATS_BATCH_COUNT)),
        checkpoint_batch_id("player_bio", 0),
    ]


def build_checkpoint_plan(url: str, token: str) -> dict[str, dict]:
    """Fetch the remote schema/count manifest used to validate checkpoints."""
    plan: dict[str, dict] = {}
    schema_by_table = {}
    for table in TABLES:
        schema_rows = _fly_query_json(url, token, _remote_schema_sql(table), timeout=60)
        if not schema_rows:
            raise RuntimeError(f"No schema found for required ops table: {table}")
        schema_by_table[table] = schema_rows

    year_rows = _fly_query_json(
        url,
        token,
        "SELECT year, COUNT(*) AS rows FROM nfl_historical.nfl_player_stats_all GROUP BY year ORDER BY year",
        timeout=60,
    )
    year_counts = {int(row["year"]): int(row["rows"]) for row in year_rows}
    for index in range(_STATS_BATCH_COUNT):
        start_year = 1920 + index * _STATS_BATCH_SIZE
        years = [year for year in sorted(year_counts) if start_year <= year < start_year + _STATS_BATCH_SIZE]
        plan[checkpoint_batch_id("nfl_player_stats_all", index)] = {
            "version": _CHECKPOINT_VERSION,
            "batch_id": checkpoint_batch_id("nfl_player_stats_all", index),
            "table": "nfl_player_stats_all",
            "schema_rows": schema_by_table["nfl_player_stats_all"],
            "schema_fingerprint": _schema_fingerprint(schema_by_table["nfl_player_stats_all"]),
            "years": years,
            "year_counts": {str(year): year_counts[year] for year in years},
            "row_count": sum(year_counts[year] for year in years),
        }

    bio_count = int(
        _fly_query_json(url, token, "SELECT COUNT(*) AS rows FROM nfl_historical.player_bio", timeout=60)[0]["rows"]
    )
    plan[checkpoint_batch_id("player_bio", 0)] = {
        "version": _CHECKPOINT_VERSION,
        "batch_id": checkpoint_batch_id("player_bio", 0),
        "table": "player_bio",
        "schema_rows": schema_by_table["player_bio"],
        "schema_fingerprint": _schema_fingerprint(schema_by_table["player_bio"]),
        "years": [],
        "year_counts": {},
        "row_count": bio_count,
    }
    return plan


def build_single_checkpoint_plan(url: str, token: str, batch_id: str) -> dict:
    """Build only the manifest needed by one matrix batch.

    Matrix workers must not each run the expensive full-table year-count query.
    That query was multiplied by every checkpoint job and overloaded the single
    Fly DuckDB machine during cold-cache rebuilds.
    """
    if batch_id == checkpoint_batch_id("player_bio", 0):
        table = "player_bio"
        years: list[int] = []
    else:
        prefix = f"{TABLES[0]}-batch-"
        if not batch_id.startswith(prefix):
            raise ValueError(f"Unknown checkpoint batch: {batch_id}")
        try:
            index = int(batch_id[len(prefix) :])
        except ValueError as exc:
            raise ValueError(f"Unknown checkpoint batch: {batch_id}") from exc
        if not 0 <= index < _STATS_BATCH_COUNT:
            raise ValueError(f"Unknown checkpoint batch: {batch_id}")
        table = TABLES[0]
        years = [1920 + index * _STATS_BATCH_SIZE]

    schema_rows = _fly_query_json(url, token, _remote_schema_sql(table), timeout=60)
    if not schema_rows:
        raise RuntimeError(f"No schema found for required ops table: {table}")
    if table == "player_bio":
        row_count = int(
            _fly_query_json(
                url,
                token,
                "SELECT COUNT(*) AS rows FROM nfl_historical.player_bio",
                timeout=60,
            )[0]["rows"]
        )
        year_counts = {}
    else:
        row_count = int(
            _fly_query_json(
                url,
                token,
                f"SELECT COUNT(*) AS rows FROM nfl_historical.{table} "
                f"WHERE year = {years[0]}",
                timeout=60,
                )[0]["rows"]
        )
        # The fixed matrix includes future/empty years.  Do not put an empty
        # year in the manifest: validation compares the manifest years with
        # the years actually present in the checkpoint database.
        if row_count:
            year_counts = {str(years[0]): row_count}
        else:
            years = []
            year_counts = {}
    return {
        "version": _CHECKPOINT_VERSION,
        "batch_id": batch_id,
        "table": table,
        "schema_rows": schema_rows,
        "schema_fingerprint": _schema_fingerprint(schema_rows),
        "years": years,
        "year_counts": year_counts,
        "row_count": row_count,
    }


def download_checkpoint_batch(
    checkpoint_dir: Path,
    batch_id: str,
    url: str,
    token: str,
    *,
    expected_manifest: dict,
    max_workers: int = 1,
    status_path: Path | None = None,
) -> Path:
    """Restore a valid local batch or download exactly that batch from Fly."""
    del max_workers  # Checkpoint downloads intentionally stay serial for Fly stability.
    checkpoint_dir = Path(checkpoint_dir)
    path = checkpoint_dir / f"{batch_id}.duckdb"
    local_manifest = _read_checkpoint_manifest(path)
    if _manifest_matches_expected(local_manifest, expected_manifest) and validate_checkpoint(path, local_manifest):
        print(f"[ops-cache] Reusing checkpoint {batch_id}")
        if status_path:
            Path(status_path).write_text(json.dumps({"reused": True}) + "\n")
        return path

    table = expected_manifest["table"]
    schema_rows = expected_manifest["schema_rows"]
    columns = _schema_columns(schema_rows)
    select_cols = ", ".join(_quote_ident(column) for column in columns)
    rows: list[dict] = []
    years = [int(year) for year in expected_manifest.get("years", [])]
    if table == "player_bio":
        rows = _fly_query_json(url, token, f"SELECT {select_cols} FROM nfl_historical.{table}", timeout=120)
    else:
        expected_year_counts = {
            int(year): int(count)
            for year, count in (expected_manifest.get("year_counts") or {}).items()
        }
        for year in years:
            expected_count = expected_year_counts.get(year)
            if expected_count is None:
                raise RuntimeError(f"Missing expected row count for checkpoint year {year}")
            year_rows: list[dict] = []
            for offset in range(0, expected_count, _YEAR_PAGE_SIZE):
                page = _fly_query_json(
                    url,
                    token,
                    f"SELECT {select_cols} FROM nfl_historical.{table} "
                    f"WHERE year = {year} LIMIT {_YEAR_PAGE_SIZE} OFFSET {offset}",
                    timeout=120,
                )
                if not page:
                    raise RuntimeError(
                        f"Fly returned an empty page for {table} year {year} "
                        f"at offset {offset}; expected {expected_count} rows"
                    )
                year_rows.extend(page)
                if len(page) < _YEAR_PAGE_SIZE and len(year_rows) < expected_count:
                    raise RuntimeError(
                        f"Fly returned only {len(year_rows)} of {expected_count} rows "
                        f"for {table} year {year}"
                    )
            if len(year_rows) != expected_count:
                raise RuntimeError(
                    f"Fetched {len(year_rows)} of {expected_count} rows for {table} year {year}"
                )
            rows.extend(year_rows)
    write_checkpoint(path, table=table, batch_id=batch_id, schema_rows=schema_rows, rows=rows, years=years)
    if status_path:
        Path(status_path).write_text(json.dumps({"reused": False}) + "\n")
    return path


def assemble_checkpoints(output_path: Path, checkpoint_dir: Path, expected_manifests: dict[str, dict]) -> None:
    """Assemble validated chunks and atomically publish the canonical cache."""
    output_path = Path(output_path)
    checkpoint_dir = Path(checkpoint_dir)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_output = output_path.with_name(f"{output_path.name}.tmp")
    temp_output.unlink(missing_ok=True)

    by_table: dict[str, list[tuple[str, dict, Path]]] = {}
    for batch_id, manifest in sorted(expected_manifests.items()):
        path = checkpoint_dir / f"{batch_id}.duckdb"
        local_manifest = _read_checkpoint_manifest(path)
        if not _manifest_matches_expected(local_manifest, manifest) or not validate_checkpoint(path, local_manifest):
            raise RuntimeError(f"Invalid checkpoint: {batch_id}")
        by_table.setdefault(manifest["table"], []).append((batch_id, manifest, path))

    conn = duckdb.connect(str(temp_output))
    try:
        conn.execute("CREATE SCHEMA nfl_historical")
        metadata_rows = []
        for table, chunks in sorted(by_table.items()):
            schema_rows = chunks[0][1]["schema_rows"]
            _create_table_from_schema(conn, table, schema_rows)
            columns = _schema_columns(schema_rows)
            quoted_columns = ", ".join(_quote_ident(column) for column in columns)
            for index, (_batch_id, manifest, path) in enumerate(chunks):
                alias = f"checkpoint_{index}"
                attach_path = str(path).replace("'", "''")
                conn.execute(f"ATTACH '{attach_path}' AS {_quote_ident(alias)} (READ_ONLY)")
                conn.execute(
                    f"INSERT INTO nfl_historical.{_quote_ident(table)} ({quoted_columns}) "
                    f"SELECT {quoted_columns} FROM {_quote_ident(alias)}.nfl_historical.{_quote_ident(table)}"
                )
                conn.execute(f"DETACH {_quote_ident(alias)}")
                metadata_rows.append((table, int(manifest["row_count"]), len(columns)))
        conn.execute(
            """
            CREATE TABLE ops_cache_metadata (
                built_at TIMESTAMP,
                source_backend VARCHAR,
                source_url VARCHAR,
                table_name VARCHAR,
                rows BIGINT,
                cols BIGINT
            )
            """
        )
        conn.executemany(
            "INSERT INTO ops_cache_metadata VALUES (current_timestamp, 'fly', 'checkpointed', ?, ?, ?)",
            metadata_rows,
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    for table, chunks in by_table.items():
        expected_count = sum(int(manifest["row_count"]) for _id, manifest, _path in chunks)
        verify = duckdb.connect(str(temp_output), read_only=True)
        try:
            actual_count = verify.execute(f"SELECT COUNT(*) FROM nfl_historical.{_quote_ident(table)}").fetchone()[0]
        finally:
            verify.close()
        if int(actual_count) != expected_count:
            temp_output.unlink(missing_ok=True)
            raise RuntimeError(f"Assembled row count mismatch for {table}: {actual_count} != {expected_count}")
    os.replace(temp_output, output_path)


def _replace_table_from_fly(
    conn: duckdb.DuckDBPyConnection,
    url: str,
    token: str,
    table: str,
    schema_rows: list[dict],
    *,
    max_workers: int = 4,
    min_year: int | None = None,
    max_year: int | None = None,
) -> tuple[int, int]:
    """Replace one local cache table from Fly using explicit schema and chunks."""
    column_names = _schema_columns(schema_rows)
    select_cols = ", ".join(_quote_ident(c) for c in column_names)

    conn.execute(f"DROP TABLE IF EXISTS nfl_historical.{table}")
    _create_table_from_schema(conn, table, schema_rows)

    has_year = any(r["column_name"] == "year" for r in schema_rows)
    rows_count = 0
    if has_year:
        year_filters = []
        if min_year is not None:
            year_filters.append(f"year >= {int(min_year)}")
        if max_year is not None:
            year_filters.append(f"year <= {int(max_year)}")
        where_sql = f" WHERE {' AND '.join(year_filters)}" if year_filters else ""
        year_sql = f"SELECT DISTINCT year FROM nfl_historical.{table}{where_sql} ORDER BY year"
        year_rows = _fly_query_json(url, token, year_sql, timeout=60)
        years = [r["year"] for r in year_rows]
        if not years:
            raise RuntimeError(f"No year values found for required ops table: {table}")

        def fetch_year(year):
            chunk_sql = f"SELECT {select_cols} FROM nfl_historical.{table} WHERE year = {year}"
            return year, _fly_query_json(url, token, chunk_sql, timeout=120)

        with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
            futures = {executor.submit(fetch_year, year): year for year in years}
            for i, future in enumerate(as_completed(futures), start=1):
                year, rows = future.result()
                if rows:
                    _insert_rows(conn, table, column_names, rows)
                    rows_count += len(rows)
                print(f"[ops-cache]   {table}: fetched year={year} ({i}/{len(years)}, {len(rows):,} rows)")
    else:
        rows = _fly_query_json(url, token, f"SELECT {select_cols} FROM nfl_historical.{table}", timeout=120)
        if rows:
            _insert_rows(conn, table, column_names, rows)
            rows_count = len(rows)

    cols = len(conn.execute(f"DESCRIBE nfl_historical.{table}").fetchall()) if rows_count > 0 else 0
    if rows_count <= 0 or cols <= 0:
        raise RuntimeError(f"Required ops table {table} cached empty ({rows_count} rows, {cols} cols)")
    return rows_count, cols


def _fly_query_json(url: str, token: str, sql: str, *, timeout: int, max_retries: int = 4) -> list[dict]:
    """Run a Fly read query with short retries for transient gateway failures."""
    data = json.dumps({"sql": sql, "database": "___ops"}).encode()
    last_error: Exception | None = None
    for attempt in range(max_retries):
        req = urllib.request.Request(
            url,
            data=data,
            headers={
                "Content-Type": "application/json",
                "Authorization": f"Bearer {token}",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read())
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {429, 500, 502, 503, 504} or attempt == max_retries - 1:
                raise
        except (urllib.error.URLError, TimeoutError) as exc:
            last_error = exc
            if attempt == max_retries - 1:
                raise
        delay = min(2**attempt, 8)
        print(f"[ops-cache] Fly query failed transiently ({last_error}); retrying in {delay}s")
        time.sleep(delay)
    raise RuntimeError(f"Fly query failed after {max_retries} attempts: {last_error}")


def build_cache_fly(
    output_path: str,
    *,
    max_workers: int = 4,
    min_year: int | None = None,
    max_year: int | None = None,
) -> None:
    """Build ops cache by downloading from Fly.io DuckDB server."""
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        output.unlink()

    url = os.environ["DATABASE_SERVER_URL"].rstrip("/") + "/query"
    token = os.environ["DATABASE_READ_TOKEN"]

    conn = duckdb.connect(str(output))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
        _prune_non_cache_tables(conn)
        table_metadata = []

        for table in TABLES:
            print(f"[ops-cache] Downloading {table} from Fly...")
            t0 = time.time()

            # First get the schema.
            schema_sql = _remote_schema_sql(table)
            schema_rows = _fly_query_json(url, token, schema_sql, timeout=60)

            if not schema_rows:
                raise RuntimeError(f"No schema found for required ops table: {table}")

            rows_count, cols = _replace_table_from_fly(
                conn,
                url,
                token,
                table,
                schema_rows,
                max_workers=max_workers,
                min_year=min_year,
                max_year=max_year,
            )
            print(f"[ops-cache]   {table}: {rows_count:,} rows, {cols} cols ({time.time() - t0:.1f}s)")
            table_metadata.append((table, rows_count, cols))

        conn.execute(
            """
            CREATE TABLE ops_cache_metadata (
                built_at TIMESTAMP,
                source_backend VARCHAR,
                source_url VARCHAR,
                table_name VARCHAR,
                rows BIGINT,
                cols BIGINT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO ops_cache_metadata
            VALUES (current_timestamp, 'fly', ?, ?, ?, ?)
            """,
            [(url, table, rows, cols) for table, rows, cols in table_metadata],
        )
        conn.execute("CHECKPOINT")

        file_mb = output.stat().st_size / (1024 * 1024)
        print(f"[ops-cache] Done: {output} ({file_mb:.0f} MB)")
    finally:
        conn.close()


def update_cache_fly(output_path: str, *, max_workers: int = 4) -> None:
    """Update an existing ops cache in place, building it if no cache exists yet."""
    output = Path(output_path)
    if not output.exists():
        print(f"[ops-cache] No existing cache at {output}; building a new cache")
        build_cache_fly(output_path, max_workers=max_workers)
        return

    url = os.environ["DATABASE_SERVER_URL"].rstrip("/") + "/query"
    token = os.environ["DATABASE_READ_TOKEN"]

    conn = duckdb.connect(str(output))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS nfl_historical")
        _prune_non_cache_tables(conn)
        table_metadata = []

        for table in TABLES:
            print(f"[ops-cache] Updating {table} from Fly...")
            t0 = time.time()
            schema_sql = _remote_schema_sql(table)
            schema_rows = _fly_query_json(url, token, schema_sql, timeout=60)
            if not schema_rows:
                raise RuntimeError(f"No schema found for required ops table: {table}")

            remote_columns = _schema_columns(schema_rows)
            if not _table_exists(conn, "nfl_historical", table):
                rows_count, cols = _replace_table_from_fly(
                    conn, url, token, table, schema_rows, max_workers=max_workers
                )
                print(f"[ops-cache]   {table}: created {rows_count:,} rows ({time.time() - t0:.1f}s)")
                table_metadata.append((table, rows_count, cols))
                continue

            local_columns = _local_columns(conn, "nfl_historical", table)
            if local_columns != remote_columns:
                print(f"[ops-cache]   {table}: schema changed; replacing table")
                rows_count, cols = _replace_table_from_fly(
                    conn, url, token, table, schema_rows, max_workers=max_workers
                )
                print(f"[ops-cache]   {table}: replaced {rows_count:,} rows ({time.time() - t0:.1f}s)")
                table_metadata.append((table, rows_count, cols))
                continue

            has_year = any(r["column_name"] == "year" for r in schema_rows)
            if not has_year:
                rows_count, cols = _replace_table_from_fly(
                    conn, url, token, table, schema_rows, max_workers=max_workers
                )
                print(f"[ops-cache]   {table}: replaced {rows_count:,} rows ({time.time() - t0:.1f}s)")
                table_metadata.append((table, rows_count, cols))
                continue

            select_cols = ", ".join(_quote_ident(c) for c in remote_columns)
            remote_counts = {
                r["year"]: r["rows"]
                for r in _fly_query_json(
                    url,
                    token,
                    f"SELECT year, COUNT(*) AS rows FROM nfl_historical.{table} GROUP BY year ORDER BY year",
                    timeout=60,
                )
            }
            local_counts = {
                year: rows
                for year, rows in conn.execute(
                    f"SELECT year, COUNT(*) AS rows FROM nfl_historical.{table} GROUP BY year ORDER BY year"
                ).fetchall()
            }

            remote_years = set(remote_counts)
            local_years = set(local_counts)
            stale_years = {year for year, rows in remote_counts.items() if local_counts.get(year) != rows}
            if remote_years:
                stale_years.add(max(remote_years))

            for year in sorted(local_years - remote_years):
                conn.execute(f"DELETE FROM nfl_historical.{table} WHERE year = ?", [year])

            refreshed_rows = 0

            # Each year is an independent, very large JSON response. Fetching
            # them serially makes a schema refresh take longer than the GitHub
            # Actions job limit. Keep network concurrency bounded so Fly is not
            # overwhelmed, but let the runner overlap the slow HTTP transfers.
            def fetch_year(year):
                return year, _fly_query_json(
                    url,
                    token,
                    f"SELECT {select_cols} FROM nfl_historical.{table} WHERE year = {year}",
                    timeout=120,
                )

            with ThreadPoolExecutor(max_workers=max(1, max_workers)) as executor:
                futures = {executor.submit(fetch_year, year): year for year in sorted(stale_years)}
                for future in as_completed(futures):
                    year, rows = future.result()
                    # DuckDB writes remain on this connection's owner thread.
                    conn.execute(f"DELETE FROM nfl_historical.{table} WHERE year = ?", [year])
                    if rows:
                        _insert_rows(conn, table, remote_columns, rows)
                        refreshed_rows += len(rows)
                    print(f"[ops-cache]   {table}: refreshed year={year} ({len(rows):,} rows)")

            rows_count = conn.execute(f"SELECT COUNT(*) FROM nfl_historical.{table}").fetchone()[0]
            cols = len(conn.execute(f"DESCRIBE nfl_historical.{table}").fetchall()) if rows_count > 0 else 0
            if rows_count <= 0 or cols <= 0:
                raise RuntimeError(f"Required ops table {table} cached empty ({rows_count} rows, {cols} cols)")
            print(
                f"[ops-cache]   {table}: {rows_count:,} total rows, "
                f"{refreshed_rows:,} refreshed ({time.time() - t0:.1f}s)"
            )
            table_metadata.append((table, rows_count, cols))

        conn.execute("DROP TABLE IF EXISTS ops_cache_metadata")
        conn.execute(
            """
            CREATE TABLE ops_cache_metadata (
                built_at TIMESTAMP,
                source_backend VARCHAR,
                source_url VARCHAR,
                table_name VARCHAR,
                rows BIGINT,
                cols BIGINT
            )
            """
        )
        conn.executemany(
            """
            INSERT INTO ops_cache_metadata
            VALUES (current_timestamp, 'fly', ?, ?, ?, ?)
            """,
            [(url, table, rows, cols) for table, rows, cols in table_metadata],
        )
        conn.execute("CHECKPOINT")
        print(f"[ops-cache] Updated: {output} ({output.stat().st_size / (1024 * 1024):.0f} MB)")
    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Build local ___ops cache")
    parser.add_argument("--output", required=True, help="Output DuckDB path")
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help="Update an existing cache file in place. Builds a new file when none exists.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=int(os.environ.get("OPS_CACHE_MAX_WORKERS", "4")),
        help="Maximum concurrent Fly year downloads when updating an existing cache.",
    )
    parser.add_argument(
        "--min-year",
        type=int,
        help="For a fresh build, include only rows from this year onward for year-partitioned tables.",
    )
    parser.add_argument(
        "--max-year",
        type=int,
        help="For a fresh build, include only rows through this year for year-partitioned tables.",
    )
    parser.add_argument(
        "--checkpoint-dir",
        type=Path,
        help="Directory containing resumable ops-cache checkpoints.",
    )
    parser.add_argument(
        "--batch-id",
        help="Download or reuse one checkpoint batch, for example nfl_player_stats_all-batch-03.",
    )
    parser.add_argument(
        "--assemble",
        action="store_true",
        help="Assemble validated checkpoints into the canonical output without Fly downloads.",
    )
    parser.add_argument(
        "--status-file",
        type=Path,
        help="Optional JSON status path for checkpoint reuse/rebuild reporting.",
    )
    args = parser.parse_args()

    backend = os.environ.get("DATABASE_BACKEND", "fly").lower()
    if backend != "fly":
        raise SystemExit("DATABASE_BACKEND must be fly; legacy database backends are deprecated")
    if not os.environ.get("DATABASE_SERVER_URL") or not os.environ.get("DATABASE_READ_TOKEN"):
        if not args.assemble:
            raise SystemExit("DATABASE_SERVER_URL and DATABASE_READ_TOKEN must be set for Fly backend")
    if args.assemble:
        if not args.checkpoint_dir:
            raise SystemExit("--checkpoint-dir is required with --assemble")
        manifests = {}
        for batch_id in expected_checkpoint_ids():
            manifest_path = _checkpoint_manifest_path(args.checkpoint_dir / f"{batch_id}.duckdb")
            if not manifest_path.exists():
                raise SystemExit(f"Missing checkpoint manifest: {manifest_path}")
            manifests[batch_id] = json.loads(manifest_path.read_text())
        assemble_checkpoints(Path(args.output), args.checkpoint_dir, manifests)
    elif args.batch_id:
        if not args.checkpoint_dir:
            raise SystemExit("--checkpoint-dir is required with --batch-id")
        url = os.environ["DATABASE_SERVER_URL"].rstrip("/") + "/query"
        token = os.environ["DATABASE_READ_TOKEN"]
        try:
            manifest = build_single_checkpoint_plan(url, token, args.batch_id)
        except (RuntimeError, ValueError) as exc:
            raise SystemExit(str(exc)) from exc
        download_checkpoint_batch(
            args.checkpoint_dir,
            args.batch_id,
            url,
            token,
            expected_manifest=manifest,
            max_workers=1,
            status_path=args.status_file,
        )
    elif args.update_existing:
        update_cache_fly(args.output, max_workers=args.max_workers)
    else:
        build_cache_fly(
            args.output,
            max_workers=args.max_workers,
            min_year=args.min_year,
            max_year=args.max_year,
        )
