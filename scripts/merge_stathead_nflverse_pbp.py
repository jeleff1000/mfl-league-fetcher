"""Merge the 1978-1998 Stathead PBP twin with 1999-2025 nflverse PBP.

The merged artifact keeps all nflverse columns and appends two lineage columns:

- pbp_source_system: stathead or nflverse
- player_id_namespace: pfr or nflverse
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_STATHEAD_TWIN = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized"
    r"\stathead_generated\pbp_backfill_1978_1998\stathead_pbp_1978_1998_nflverse_twin.parquet"
)
DEFAULT_NFLVERSE_DIR = REPO_ROOT / "fantasy_football_data" / "cache" / "nflverse"
DEFAULT_OUT_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized" r"\stathead_generated\pbp_merged_1978_2025"
)
DEFAULT_OUT = DEFAULT_OUT_DIR / "nfl_pbp_1978_2025_merged.parquet"
DEFAULT_AUDIT_DIR = Path(
    r"C:\Users\joeye\OneDrive\Desktop\fantasy_football_data_organized" r"\_catalog\pbp_merged_1978_2025_20260507"
)


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_path(path: Path) -> str:
    return sql_string(str(path))


def sql_path_list(paths: list[Path]) -> str:
    return "[" + ", ".join(sql_path(path) for path in paths) + "]"


def discover_nflverse_files(nflverse_dir: Path, min_year: int, max_year: int) -> list[Path]:
    pattern = re.compile(r"^nflverse_pbp_(\d{4})\.parquet$")
    files: list[tuple[int, Path]] = []
    for path in nflverse_dir.glob("nflverse_pbp_*.parquet"):
        match = pattern.match(path.name)
        if not match:
            continue
        year = int(match.group(1))
        if min_year <= year <= max_year:
            files.append((year, path))
    files.sort()
    years = [year for year, _ in files]
    expected = list(range(min_year, max_year + 1))
    missing = sorted(set(expected) - set(years))
    if missing:
        raise FileNotFoundError(f"Missing nflverse PBP parquet years: {missing}")
    return [path for _, path in files]


def duckdb_type(pa_type: pa.DataType) -> str:
    if pa.types.is_integer(pa_type):
        return "INTEGER"
    if pa.types.is_floating(pa_type):
        return "DOUBLE"
    if pa.types.is_string(pa_type) or pa.types.is_large_string(pa_type) or pa.types.is_null(pa_type):
        return "VARCHAR"
    raise TypeError(f"Unsupported PBP parquet type: {pa_type}")


def typed_select_exprs(schema: pa.Schema) -> str:
    return ", ".join(
        f"CAST({qident(field.name)} AS {duckdb_type(field.type)}) AS {qident(field.name)}" for field in schema
    )


def assert_same_column_layout(paths: list[Path], audit_dir: Path) -> pa.Schema:
    if not paths:
        raise ValueError("No parquet files supplied for schema validation")
    reference_path = paths[0]
    reference_schema = pq.ParquetFile(reference_path).schema_arrow
    layout_mismatches: list[dict[str, Any]] = []
    type_diffs: list[dict[str, Any]] = []
    for path in paths[1:]:
        schema = pq.ParquetFile(path).schema_arrow
        if schema.names != reference_schema.names:
            layout_mismatches.append(
                {
                    "path": str(path),
                    "reference": str(reference_path),
                    "missing": [name for name in reference_schema.names if name not in schema.names],
                    "extra": [name for name in schema.names if name not in reference_schema.names],
                    "same_order": schema.names == reference_schema.names,
                }
            )
            continue
        for ref_field, field in zip(reference_schema, schema):
            if field.type != ref_field.type:
                type_diffs.append(
                    {
                        "path": str(path),
                        "column": field.name,
                        "reference_type": str(ref_field.type),
                        "file_type": str(field.type),
                    }
                )
    if layout_mismatches:
        raise ValueError(f"Parquet column layouts do not match: {json.dumps(layout_mismatches, indent=2)}")
    if type_diffs:
        audit_dir.mkdir(parents=True, exist_ok=True)
        (audit_dir / "merged_pbp_input_type_casts.json").write_text(
            json.dumps(type_diffs, indent=2),
            encoding="utf-8",
        )
    return reference_schema


def parquet_rows(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def merge_pbp(
    stathead_twin: Path,
    nflverse_dir: Path,
    out_path: Path,
    audit_dir: Path,
    min_nflverse_year: int,
    max_nflverse_year: int,
) -> dict[str, Any]:
    nflverse_files = discover_nflverse_files(nflverse_dir, min_nflverse_year, max_nflverse_year)
    schema = assert_same_column_layout([stathead_twin, *nflverse_files], audit_dir)
    base_cols = typed_select_exprs(schema)

    if "pbp_source_system" in schema.names or "player_id_namespace" in schema.names:
        raise ValueError("Lineage columns already exist in the base PBP schema")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()

    stathead_rows = parquet_rows(stathead_twin)
    nflverse_year_rows = {path.stem.rsplit("_", 1)[-1]: parquet_rows(path) for path in nflverse_files}
    expected_rows = stathead_rows + sum(nflverse_year_rows.values())

    con = duckdb.connect()
    con.execute("SET preserve_insertion_order=false")
    con.execute(
        f"""
        CREATE TEMP VIEW stathead_pbp AS
        SELECT
            {base_cols},
            'stathead'::VARCHAR AS pbp_source_system,
            'pfr'::VARCHAR AS player_id_namespace
        FROM read_parquet({sql_path(stathead_twin)})
        """
    )
    con.execute(
        f"""
        CREATE TEMP VIEW nflverse_pbp AS
        SELECT
            {base_cols},
            'nflverse'::VARCHAR AS pbp_source_system,
            'nflverse'::VARCHAR AS player_id_namespace
        FROM read_parquet({sql_path_list(nflverse_files)})
        """
    )
    con.execute(
        f"""
        COPY (
            SELECT *
            FROM (
                SELECT * FROM stathead_pbp
                UNION ALL
                SELECT * FROM nflverse_pbp
            )
            ORDER BY season, week, game_id, play_id
        )
        TO {sql_path(out_path)}
        (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    out_pf = pq.ParquetFile(out_path)
    out_schema = out_pf.schema_arrow
    out_sql = f"read_parquet({sql_path(out_path)})"
    validation = (
        con.execute(
            f"""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT season) AS seasons,
            MIN(season) AS min_season,
            MAX(season) AS max_season,
            COUNT(DISTINCT game_id) AS games,
            SUM(CASE WHEN game_id IS NULL THEN 1 ELSE 0 END) AS null_game_id_rows,
            SUM(CASE WHEN season IS NULL THEN 1 ELSE 0 END) AS null_season_rows,
            SUM(CASE WHEN week IS NULL THEN 1 ELSE 0 END) AS null_week_rows,
            COUNT(*) - COUNT(DISTINCT game_id || '#' || CAST(play_id AS VARCHAR)) AS duplicate_game_play_rows
        FROM {out_sql}
        """
        )
        .fetchdf()
        .iloc[0]
        .to_dict()
    )
    source_counts = con.execute(
        f"""
        SELECT pbp_source_system, player_id_namespace, COUNT(*) AS rows, COUNT(DISTINCT game_id) AS games
        FROM {out_sql}
        GROUP BY 1, 2
        ORDER BY 1, 2
        """
    ).fetchdf()
    season_counts = con.execute(
        f"""
        SELECT season, pbp_source_system, COUNT(*) AS rows, COUNT(DISTINCT game_id) AS games
        FROM {out_sql}
        GROUP BY 1, 2
        ORDER BY season, pbp_source_system
        """
    ).fetchdf()

    source_counts_path = audit_dir / "merged_pbp_source_counts.csv"
    season_counts_path = audit_dir / "merged_pbp_season_counts.csv"
    source_counts.to_csv(source_counts_path, index=False)
    season_counts.to_csv(season_counts_path, index=False)

    validation = {key: int(value) for key, value in validation.items()}
    summary = {
        "output": str(out_path),
        "stathead_twin": str(stathead_twin),
        "nflverse_dir": str(nflverse_dir),
        "nflverse_files": [str(path) for path in nflverse_files],
        "nflverse_years": [int(path.stem.rsplit("_", 1)[-1]) for path in nflverse_files],
        "lineage_columns": ["pbp_source_system", "player_id_namespace"],
        "base_schema_columns": len(schema.names),
        "output_schema_columns": len(out_schema.names),
        "base_schema_preserved_prefix": out_schema.names[: len(schema.names)] == schema.names,
        "expected_rows": expected_rows,
        "stathead_rows": stathead_rows,
        "nflverse_rows": sum(nflverse_year_rows.values()),
        "nflverse_year_rows": nflverse_year_rows,
        "output_rows": out_pf.metadata.num_rows,
        "output_size_mb": round(out_path.stat().st_size / 1024 / 1024, 3),
        "validation": validation,
        "source_counts_csv": str(source_counts_path),
        "season_counts_csv": str(season_counts_path),
        "input_type_casts_json": str(audit_dir / "merged_pbp_input_type_casts.json"),
        "notes": [
            "1978-1998 rows are Stathead/PFR-derived nflverse-schema twin rows.",
            "1999-2025 rows are original nflverse PBP cache rows.",
            "Merged artifact intentionally keeps PFR player IDs for Stathead-era rows and nflverse IDs for 1999+ rows.",
            "Remaining documented historical gaps are accepted for fantasy/supertable use.",
        ],
    }
    summary_path = audit_dir / "merged_pbp_manifest.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if summary["output_rows"] != expected_rows:
        raise RuntimeError(f"Output row count {summary['output_rows']} != expected {expected_rows}")
    if validation["duplicate_game_play_rows"] != 0:
        raise RuntimeError(f"Duplicate game/play rows detected: {validation['duplicate_game_play_rows']}")
    if validation["null_game_id_rows"] != 0 or validation["null_season_rows"] != 0 or validation["null_week_rows"] != 0:
        raise RuntimeError(f"Null key validation failed: {validation}")

    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stathead-twin", type=Path, default=DEFAULT_STATHEAD_TWIN)
    parser.add_argument("--nflverse-dir", type=Path, default=DEFAULT_NFLVERSE_DIR)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument("--min-nflverse-year", type=int, default=1999)
    parser.add_argument("--max-nflverse-year", type=int, default=2025)
    args = parser.parse_args()

    summary = merge_pbp(
        stathead_twin=args.stathead_twin,
        nflverse_dir=args.nflverse_dir,
        out_path=args.out,
        audit_dir=args.audit_dir,
        min_nflverse_year=args.min_nflverse_year,
        max_nflverse_year=args.max_nflverse_year,
    )
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
