"""Measure NFL.com season return-page mappings and the deferred detail fields.

This is a measurement-only lane.  It deliberately records missing v26 canonicals for
fair catches, return-threshold buckets, and return fumbles instead of assigning them to
generic or defensive fumble columns.  Counts use SUM; longest-return fields use MAX.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import registry, v26_plane


OUT = Path(
    r"D:/league-history-data/nfl/derived/validation/sota_recon_master"
) / "nflcom_player_season_return_detail_witness.json"
SOURCE = Path(registry(include_subject=False)["nflcom_player_season"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)

RETURN_DETAIL_FIELDS = ("fc", "fum", "20", "40")
RETURN_PAGE_CANDIDATES = {
    "kickoff-returns": {
        "ret": ("kickoff_returns", "punt_returns"),
        "yds": ("kickoff_return_yards", "punt_return_yards", "total_return_yards"),
        "lng": ("kickoff_return_long", "punt_return_long"),
        "kret_td": ("kickoff_return_tds", "punt_return_tds"),
        "fc": ("kickoff_returns", "punt_returns", "punts"),
        "fum": ("fumbles", "rushing_fumbles", "receiving_fumbles", "sack_fumbles", "def_fumbles"),
        "20": ("receptions_20_29", "pass_explosive_20", "rec_explosive_20"),
        "40": ("receptions_40plus", "completions_40plus", "rushing_40plus"),
    },
    "punt-returns": {
        "ret": ("punt_returns", "kickoff_returns"),
        "yds": ("punt_return_yards", "kickoff_return_yards", "total_return_yards"),
        "lng": ("punt_return_long", "kickoff_return_long"),
        "pret_t": ("punt_return_tds", "kickoff_return_tds"),
        "fc": ("punt_returns", "punts", "kickoff_returns"),
        "fum": ("fumbles", "rushing_fumbles", "receiving_fumbles", "sack_fumbles", "def_fumbles"),
        "20": ("receptions_20_29", "pass_explosive_20", "rec_explosive_20"),
        "40": ("receptions_40plus", "completions_40plus", "rushing_40plus"),
    },
}
DETAIL_SCHEMA_NAMES = {
    "fc": ("kickoff_return_fair_catches", "punt_return_fair_catches", "fair_catches"),
    "fum": ("kickoff_return_fumbles", "punt_return_fumbles", "return_fumbles"),
    "20": ("kickoff_returns_20plus", "punt_returns_20plus", "return_20plus"),
    "40": ("kickoff_returns_40plus", "punt_returns_40plus", "return_40plus"),
}
SOURCE_COLUMNS = {"kickoff-returns": "kret_td", "punt-returns": "pret_t"}


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def witness_form(source_column: str) -> str:
    return "MAX" if source_column == "lng" else "SUM"


def _agreement_counts(pairs: list[tuple[float | None, float | None]]) -> tuple[int, int, int, int, int]:
    joined = len(pairs)
    informative = agree = source_hi = target_hi = 0
    for source_value, target_value in pairs:
        if source_value is None or target_value is None:
            continue
        if source_value == 0 and target_value == 0:
            continue
        informative += 1
        agree += int(source_value == target_value)
        source_hi += int(source_value > target_value)
        target_hi += int(source_value < target_value)
    return joined, informative, agree, source_hi, target_hi


def return_detail_inventory(
    schema_columns: set[str] | dict[str, set[str]],
) -> dict[str, dict[str, dict]]:
    if isinstance(schema_columns, set):
        schema_columns = {"weekly": schema_columns}
    return {
        category: {
            field: {
                "status": "CURRENT_V26_CANONICAL_FOUND"
                if any(
                    name in columns
                    for columns in schema_columns.values()
                    for name in DETAIL_SCHEMA_NAMES[field]
                )
                else "NO_CURRENT_V26_CANONICAL",
                "schema_search_names": list(DETAIL_SCHEMA_NAMES[field]),
                "schema_search_found": any(
                    name in columns
                    for columns in schema_columns.values()
                    for name in DETAIL_SCHEMA_NAMES[field]
                ),
                "schema_search_found_by_plane": {
                    plane: any(name in columns for name in DETAIL_SCHEMA_NAMES[field])
                    for plane, columns in schema_columns.items()
                },
            }
            for field in RETURN_DETAIL_FIELDS
        }
        for category in RETURN_PAGE_CANDIDATES
    }


def _candidate_measurement(
    con: duckdb.DuckDBPyConnection,
    category: str,
    source_column: str,
    canonical: str,
    v26: str,
) -> dict:
    source_agg = witness_form(source_column)
    # The source is season grain, so read the stored player-season value directly.
    # MAX is safe here because the season plane is unique at (player, year); it also
    # keeps the candidate comparison explicit for both counts and long fields.
    target_agg = "MAX"
    sql = f"""WITH s AS (
        SELECT _player_slug, TRY_CAST(season AS INTEGER) AS yr,
               {source_agg}(TRY_CAST("{source_column}" AS DOUBLE)) AS source_value
        FROM src WHERE _category='{category}'
          AND TRY_CAST("{source_column}" AS DOUBLE) IS NOT NULL
        GROUP BY 1, 2
      ), v AS (
        SELECT i.nflcom_slug, v.year AS yr,
               {target_agg}(TRY_CAST(v."{canonical}" AS DOUBLE)) AS target_value
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        WHERE v.year IS NOT NULL
        GROUP BY 1, 2
      )
      SELECT source_value, target_value
      FROM s JOIN v ON v.nflcom_slug=s._player_slug AND v.yr=s.yr"""
    joined, informative, agree, source_hi, target_hi = _agreement_counts(
        con.execute(sql).fetchall()
    )
    return {
        "canonical": canonical,
        "source_aggregation": source_agg,
        "target_aggregation": target_agg,
        "joined_n": int(joined or 0),
        "informative_n": int(informative or 0),
        "agree_n": int(agree or 0),
        "agree_pct": round(100.0 * agree / informative, 2) if informative else None,
        "source_exceeds_target": int(source_hi or 0),
        "target_exceeds_source": int(target_hi or 0),
    }


def measure() -> dict:
    con = duckdb.connect()
    con.execute(
        f"""CREATE TEMP TABLE src AS
        SELECT * FROM read_parquet('{_q(SOURCE)}/**/*.parquet', union_by_name=true)
        WHERE LOWER(CAST(season_type AS VARCHAR))='reg'
          AND _category IN ('kickoff-returns', 'punt-returns')"""
    )
    con.execute(
        f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id"""
    )
    v26_path = _q(v26_plane("season"))
    schema_columns_by_plane = {}
    for plane in ("weekly", "season", "career", "season_team"):
        path = _q(v26_plane(plane))
        schema_columns_by_plane[plane] = {
            row[0]
            for row in con.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 1"
            ).fetchall()
        }
    schema_columns = schema_columns_by_plane["season"]
    matrix = {}
    for category, candidates in RETURN_PAGE_CANDIDATES.items():
        matrix[category] = {}
        for source_column, candidate_columns in candidates.items():
            matrix[category][source_column] = [
                _candidate_measurement(con, category, source_column, candidate, v26_path)
                for candidate in candidate_columns
                if candidate in schema_columns
            ]
    result = {
        "source": "nflcom_player_season",
        "season_type": "REG",
        "subject_plane": "player_nfl_season",
        "source_rows": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),
        "return_detail_inventory": return_detail_inventory(schema_columns_by_plane),
        "schema_planes": {
            plane: v26_plane(plane)
            for plane in ("weekly", "season", "career", "season_team")
        },
        "candidate_matrix": matrix,
        "denominator": (
            "joined (_player_slug, season) rows; target NULLs remain in joined_n, "
            "and informative_n is the nonzero union"
        ),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> int:
    result = measure()
    print(f"return detail: {result['source_rows']:,} source rows -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
