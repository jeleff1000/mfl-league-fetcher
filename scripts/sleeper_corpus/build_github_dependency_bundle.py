#!/usr/bin/env python3
"""Build the corpus dependency bundle entirely from local canonical files.

This script intentionally does not load .env and does not import a Fly client.
The resulting directory can be archived and published as a private GitHub release
asset, then promoted into the workers repository's Actions cache.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_ROOT = REPO_ROOT / "scripts"
if str(SCRIPTS_ROOT) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_ROOT))

from backfill_draft_scores import build_global_source_cache  # noqa: E402


SCHEMA_VERSION = 1
DEFAULT_OPS_CACHE = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/ops_cache.duckdb")
DEFAULT_CORPUS_SNAPSHOT = Path(
    "D:/league-history-data/fantasy_leagues/sampling_corpus/corpus_snapshot.duckdb"
)
DEFAULT_OUTPUT = Path(
    "D:/league-history-data/fantasy_leagues/sampling_corpus/github_dependencies_v1"
)


class LocalSnapshotReader:
    """Minimal reader contract for the shared draft-source materializer."""

    TIMEOUT_SECONDS = 0
    MAX_RETRIES = 0
    RETRY_MAX_DELAY = 0

    def __init__(self, snapshot: Path, ops_cache: Path) -> None:
        self._conn = duckdb.connect()
        snapshot_sql = str(snapshot.resolve()).replace("'", "''")
        ops_sql = str(ops_cache.resolve()).replace("'", "''")
        self._conn.execute(f"ATTACH '{snapshot_sql}' AS corpus (READ_ONLY)")
        self._conn.execute(f"ATTACH '{ops_sql}' AS ops (READ_ONLY)")
        self._conn.execute("CREATE SCHEMA public")
        self._conn.execute(
            """
            CREATE VIEW public.league_settings AS
            WITH draft_years AS (
                SELECT db_name, year, MAX(COALESCE(TRY_CAST(round AS INTEGER), 0)) AS draft_rounds
                FROM corpus.public.draft
                GROUP BY db_name, year
            )
            SELECT s.*,
                   COALESCE(d.draft_rounds, 0) AS draft_rounds,
                   0::INTEGER AS roster_REC_FLEX,
                   COALESCE(s.roster_TAXI, 0)::INTEGER AS sleeper_taxi_slots,
                   FALSE AS sleeper_pick_trading,
                   FALSE AS uses_median
            FROM corpus.public.league_settings s
            LEFT JOIN draft_years d USING (db_name, year)
            """
        )
        self._conn.execute(
            """
            CREATE VIEW public.draft AS
            WITH year_shape AS (
                SELECT db_name, year,
                       MAX(COALESCE(TRY_CAST(round AS INTEGER), 0)) AS max_round,
                       MAX(COALESCE(TRY_CAST(cost AS DOUBLE), 0)) AS max_cost
                FROM corpus.public.draft
                GROUP BY db_name, year
            ),
            positions AS (
                SELECT NFL_player_id, ARG_MAX(position, year) AS position
                FROM ops.nfl_historical.nfl_player_stats_all
                WHERE NFL_player_id IS NOT NULL AND position IS NOT NULL
                GROUP BY NFL_player_id
            )
            SELECT d.db_name, d.year, d.round, d.pick, d.cost, d.manager_lamar,
                   d.is_keeper,
                   COALESCE(p.position, 'UNK') AS position,
                   CAST(COALESCE(
                       TRY_CAST(d.pick_in_round AS INTEGER),
                       1 + ((COALESCE(TRY_CAST(d.pick AS INTEGER), 1) - 1)
                            % GREATEST(COALESCE(s.num_teams, 12), 1))
                   ) AS VARCHAR) AS manager,
                   CASE
                     WHEN COALESCE(TRY_CAST(d.is_keeper AS INTEGER), 0) = 1 THEN 'keeper'
                     WHEN COALESCE(s.is_dynasty, FALSE) AND y.max_round <= 6 THEN 'rookie'
                     WHEN COALESCE(s.is_dynasty, FALSE) THEN 'startup'
                     ELSE 'redraft'
                   END AS draft_category,
                   CASE WHEN y.max_cost > 0 THEN 'auction' ELSE 'snake' END AS draft_type
            FROM corpus.public.draft d
            JOIN year_shape y USING (db_name, year)
            LEFT JOIN corpus.public.league_settings s USING (db_name, year)
            LEFT JOIN positions p USING (NFL_player_id)
            """
        )

    def query_df(self, sql: str, database: str | None = None) -> pd.DataFrame:
        del database
        return self._conn.execute(sql).fetchdf()

    def close(self) -> None:
        self._conn.close()


def sha256_file(path: Path, *, chunk_size: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def link_or_copy(source: Path, target: Path) -> None:
    if target.exists():
        target.unlink()
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)


def duckdb_table_stats(path: Path, qualified_table: str) -> dict[str, int]:
    conn = duckdb.connect(str(path), read_only=True)
    try:
        rows = int(conn.execute(f"SELECT COUNT(*) FROM {qualified_table}").fetchone()[0])
        columns = int(conn.execute(f"SELECT COUNT(*) FROM (DESCRIBE {qualified_table})").fetchone()[0])
        return {"rows": rows, "columns": columns}
    finally:
        conn.close()


def parquet_stats(path: Path) -> dict[str, int]:
    conn = duckdb.connect()
    try:
        rows = int(conn.execute("SELECT COUNT(*) FROM read_parquet(?)", [str(path)]).fetchone()[0])
        columns = len(conn.execute("DESCRIBE SELECT * FROM read_parquet(?)", [str(path)]).fetchall())
        return {"rows": rows, "columns": columns}
    finally:
        conn.close()


def build_bundle(ops_cache: Path, corpus_snapshot: Path, output: Path) -> dict[str, Any]:
    for label, source in (("ops cache", ops_cache), ("corpus snapshot", corpus_snapshot)):
        if not source.is_file():
            raise FileNotFoundError(f"Local {label} is missing: {source}")

    output.mkdir(parents=True, exist_ok=True)
    bundled_ops = output / "ops_cache.duckdb"
    draft_source = output / "draft_global_source.parquet"
    link_or_copy(ops_cache.resolve(), bundled_ops)

    reader = LocalSnapshotReader(corpus_snapshot, bundled_ops)
    try:
        built = build_global_source_cache(reader, draft_source, refresh=True)
    finally:
        reader.close()
    if built is None or not draft_source.is_file():
        raise RuntimeError("The local corpus snapshot produced no draft global source rows")

    assets = {
        "ops_cache.duckdb": {
            "bytes": bundled_ops.stat().st_size,
            "sha256": sha256_file(bundled_ops),
            **duckdb_table_stats(bundled_ops, "nfl_historical.nfl_player_stats_all"),
        },
        "draft_global_source.parquet": {
            "bytes": draft_source.stat().st_size,
            "sha256": sha256_file(draft_source),
            **parquet_stats(draft_source),
        },
    }
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "sources": {
            "ops_cache": str(ops_cache.resolve()),
            "corpus_snapshot": str(corpus_snapshot.resolve()),
        },
        "assets": assets,
    }
    (output / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return manifest


def validate_bundle(output: Path) -> dict[str, Any]:
    manifest_path = output / "manifest.json"
    if not manifest_path.is_file():
        raise FileNotFoundError(f"Dependency manifest is missing: {manifest_path}")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != SCHEMA_VERSION:
        raise RuntimeError(
            f"Unsupported dependency schema version: {manifest.get('schema_version')}"
        )
    for name, expected in manifest["assets"].items():
        asset = output / name
        if not asset.is_file():
            raise FileNotFoundError(f"Dependency asset is missing: {asset}")
        actual_hash = sha256_file(asset)
        if actual_hash != expected["sha256"]:
            raise RuntimeError(f"Checksum mismatch for {name}: {actual_hash}")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ops-cache", type=Path, default=DEFAULT_OPS_CACHE)
    parser.add_argument("--corpus-snapshot", type=Path, default=DEFAULT_CORPUS_SNAPSHOT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    manifest = (
        validate_bundle(args.output)
        if args.validate_only
        else build_bundle(args.ops_cache, args.corpus_snapshot, args.output)
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
