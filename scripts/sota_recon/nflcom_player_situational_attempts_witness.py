"""Measure the NFL.com player-situational Attempts subtable.

Attempts is a special situational axis, not a single player-season row: its four
attempt buckets must be aggregated within each ``(nflcom_slug, season)``.  This
receipt uses the existing column-audit lane for the primary verdict and adds a
same-family candidate matrix for every mapped field.  It is measurement-only;
it does not alter the disposition ledger, v26, or licenses.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from . import sources as S
from .nflcom_column_audit import SourcePlan, audit_source


OUT_ROOT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT_BY_DIMENSION = {
    "Attempts": OUT_ROOT / "nflcom_player_situational_attempts_witness.json",
    "Field Position": OUT_ROOT / "nflcom_player_situational_field_position_witness.json",
    "Home vs Road": OUT_ROOT / "nflcom_player_situational_home_vs_road_witness.json",
    "Stadium Surfaces": OUT_ROOT / "nflcom_player_situational_stadium_surfaces_witness.json",
    "Quarters": OUT_ROOT / "nflcom_player_situational_quarters_witness.json",
    "Game Halves": OUT_ROOT / "nflcom_player_situational_game_halves_witness.json",
    "Margin of Victory": OUT_ROOT / "nflcom_player_situational_margin_of_victory_witness.json",
    "Point Differential": OUT_ROOT / "nflcom_player_situational_point_differential_witness.json",
}
OUT = (
    Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
    / "nflcom_player_situational_attempts_witness.json"
)

ATTEMPTS_LAYOUTS = {
    "player_situational_L3": "rushing",
    "player_situational_L5": "passing",
}

# Same-family alternatives only.  A higher numerical agreement from a coarser
# total is evidence about the definition, not permission to remap the field.
CANDIDATE_COLUMNS: dict[tuple[str, str], tuple[str, ...]] = {
    ("player_situational_L3", "att"): ("carries", "attempts", "completions"),
    ("player_situational_L3", "lng"): (
        "rushing_long", "passing_long", "receiving_long", "punt_long",
    ),
    ("player_situational_L3", "td"): (
        "rushing_tds", "passing_tds", "receiving_tds",
    ),
    ("player_situational_L3", "yds"): (
        "rushing_yards", "passing_yards", "receiving_yards",
    ),
    ("player_situational_L5", "1st"): (
        "passing_first_downs", "receiving_first_downs", "rushing_first_downs",
    ),
    ("player_situational_L5", "20"): (
        "pass_explosive_20", "rec_explosive_20", "receptions_20_29",
    ),
    ("player_situational_L5", "att"): ("attempts", "carries", "completions"),
    ("player_situational_L5", "comp"): ("completions", "receptions", "attempts"),
    ("player_situational_L5", "int"): (
        "passing_interceptions", "def_interceptions",
    ),
    ("player_situational_L5", "lng"): (
        "passing_long", "rushing_long", "receiving_long", "punt_long",
    ),
    ("player_situational_L5", "sck"): ("sacks_suffered", "def_sacks"),
    ("player_situational_L5", "scky"): ("sack_yards_lost", "def_sack_yards"),
    ("player_situational_L5", "td"): (
        "passing_tds", "rushing_tds", "receiving_tds",
    ),
    ("player_situational_L5", "yds"): (
        "passing_yards", "rushing_yards", "receiving_yards",
    ),
}

FIELD_POSITION_LAYOUTS = {
    "player_situational_L0": "defense",
    "player_situational_L1": "fumbles",
    "player_situational_L2": "receiving",
    "player_situational_L3": "rushing",
    "player_situational_L5": "passing",
    "player_situational_L6": "punts",
    "player_situational_L7": "field-goals",
}

HOME_VS_ROAD_LAYOUTS = {
    "player_situational_L0": "defense",
    "player_situational_L1": "fumbles",
    "player_situational_L2": "receiving",
    "player_situational_L3": "rushing",
    "player_situational_L4": "returns",
    "player_situational_L5": "passing",
    "player_situational_L6": "punts",
    "player_situational_L7": "field-goals",
}

STADIUM_SURFACES_LAYOUTS = {
    "player_situational_L0": "defense",
    "player_situational_L1": "fumbles",
    "player_situational_L2": "receiving",
    "player_situational_L3": "rushing",
    "player_situational_L4": "returns",
    "player_situational_L5": "passing",
    "player_situational_L6": "punts",
    "player_situational_L7": "field-goals",
}

QUARTERS_LAYOUTS = {
    "player_situational_L0": "defense",
    "player_situational_L1": "fumbles",
    "player_situational_L2": "receiving",
    "player_situational_L3": "rushing",
    "player_situational_L4": "returns",
    "player_situational_L5": "passing",
    "player_situational_L6": "punts",
    "player_situational_L7": "field-goals",
}

GAME_HALVES_LAYOUTS = {
    "player_situational_L0": "defense",
    "player_situational_L1": "fumbles",
    "player_situational_L2": "receiving",
    "player_situational_L3": "rushing",
    "player_situational_L4": "returns",
    "player_situational_L5": "passing",
    "player_situational_L6": "punts",
    "player_situational_L7": "field-goals",
}

MARGIN_OF_VICTORY_LAYOUTS = {
    "player_situational_L0": "defense",
    "player_situational_L1": "fumbles",
    "player_situational_L2": "receiving",
    "player_situational_L3": "rushing",
    "player_situational_L4": "returns",
    "player_situational_L5": "passing",
    "player_situational_L6": "punts",
    "player_situational_L7": "field-goals",
}

POINT_DIFFERENTIAL_LAYOUTS = {
    "player_situational_L0": "defense",
    "player_situational_L1": "fumbles",
    "player_situational_L2": "receiving",
    "player_situational_L3": "rushing",
    "player_situational_L4": "returns",
    "player_situational_L5": "passing",
    "player_situational_L6": "punts",
    "player_situational_L7": "field-goals",
}

QUARTERS_EXCLUDED_SPLITS = ("4th Quarter within 7",)
GAME_HALVES_EXCLUDED_SPLITS = ("Last Two Minutes of Half",)
POINT_DIFFERENTIAL_EXCLUDED_SPLITS = (
    "Ahead by 1-8 Points",
    "Ahead by 9-16 Points",
    "Behind by 1-8 Points",
    "Behind by 9-16 Points",
)


def dimension_where(dimension: str) -> str:
    predicate = f"_table='{dimension}'"
    if dimension == "Quarters":
        return predicate + " AND split_value <> '4th Quarter within 7'"
    if dimension == "Game Halves":
        return predicate + " AND split_value <> 'Last Two Minutes of Half'"
    if dimension == "Point Differential":
        values = ", ".join(f"'{value}'" for value in POINT_DIFFERENTIAL_EXCLUDED_SPLITS)
        return predicate + f" AND split_value NOT IN ({values})"
    return predicate


def check_capture_recoverability(
    candidate_matrix: dict[str, list[dict]],
    lost_columns_by_layout: dict[str, dict[str, int]],
) -> list[dict]:
    """Prove that measured situational fields survive the parser boundary."""
    fields_by_layout: dict[str, set[str]] = {}
    for key in candidate_matrix:
        layout, source_column = key.split(".", 1)
        fields_by_layout.setdefault(layout, set()).add(source_column)
    result = []
    for layout, fields in sorted(fields_by_layout.items()):
        lost = set(lost_columns_by_layout.get(layout, {}))
        overlap = sorted(fields & lost)
        result.append(
            {
                "layout": layout,
                "witness_columns": sorted(fields),
                "lost_columns": sorted(lost),
                "lost_witness_columns": overlap,
                "status": (
                    "RECOVERABLE_WITNESS_FIELDS"
                    if lost and not overlap
                    else "PROOF_PENDING_CAPTURE"
                ),
            }
        )
    return result

FIELD_POSITION_CANDIDATES: dict[tuple[str, str], tuple[str, ...]] = {
    ("player_situational_L0", "total"): (
        "def_tackles_combined", "def_tackles_solo", "def_tackle_assists",
        "def_tackles_with_assist",
    ),
    ("player_situational_L0", "solo"): (
        "def_tackles_solo", "def_tackles_combined", "def_tackle_assists",
    ),
    ("player_situational_L0", "ast"): (
        "def_tackle_assists", "def_tackles_solo", "def_tackles_combined",
    ),
    ("player_situational_L0", "sck"): ("def_sacks", "sacks_suffered"),
    ("player_situational_L0", "sfty"): ("def_safeties",),
    ("player_situational_L0", "pdef"): ("def_pass_defended",),
    ("player_situational_L0", "int"): (
        "def_interceptions", "passing_interceptions",
    ),
    ("player_situational_L0", "tds"): (
        "def_int_ret_td", "def_tds", "fum_ret_td",
    ),
    ("player_situational_L0", "yds"): (
        "def_interception_yards", "passing_yards", "fumble_recovery_yards",
    ),
    ("player_situational_L1", "fum"): (
        "fumbles", "rushing_fumbles", "receiving_fumbles", "sack_fumbles",
        "def_fumbles",
    ),
    ("player_situational_L1", "lost"): (
        "fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost",
        "sack_fumbles_lost",
    ),
    ("player_situational_L1", "ff"): ("def_fumbles_forced", "def_fumbles"),
    ("player_situational_L1", "own_fr"): (
        "fumble_recovery_own", "fumble_recovery_opp", "def_fumbles",
    ),
    ("player_situational_L1", "opp_fr"): (
        "fumble_recovery_opp", "fumble_recovery_own", "def_fumbles",
    ),
    ("player_situational_L2", "yds"): (
        "receiving_yards", "passing_yards", "rushing_yards",
    ),
    ("player_situational_L2", "lng"): (
        "receiving_long", "passing_long", "rushing_long",
    ),
    ("player_situational_L2", "td"): (
        "receiving_tds", "passing_tds", "rushing_tds",
    ),
    ("player_situational_L2", "rec"): ("receptions", "carries", "attempts"),
    ("player_situational_L2", "1st"): (
        "receiving_first_downs", "passing_first_downs", "rushing_first_downs",
    ),
    ("player_situational_L2", "20"): (
        "rec_explosive_20", "pass_explosive_20", "receptions_20_29",
    ),
    ("player_situational_L3", "yds"): (
        "rushing_yards", "passing_yards", "receiving_yards",
    ),
    ("player_situational_L3", "lng"): (
        "rushing_long", "passing_long", "receiving_long", "punt_long",
    ),
    ("player_situational_L3", "td"): (
        "rushing_tds", "passing_tds", "receiving_tds",
    ),
    ("player_situational_L3", "att"): ("carries", "attempts", "completions"),
    # The return block is ambiguous between kickoff and punt pages, but its combined
    # YDS cell has a single existing aggregate target. The weekly subject proves that
    # target is the sum of the two return-family yard columns; measure it alongside the
    # other candidates instead of filing the schema search as empty.
    ("player_situational_L4", "yds"): ("total_return_yards",),
    ("player_situational_L5", "sck"): ("sacks_suffered", "def_sacks"),
    ("player_situational_L5", "int"): (
        "passing_interceptions", "def_interceptions",
    ),
    ("player_situational_L5", "yds"): (
        "passing_yards", "rushing_yards", "receiving_yards",
    ),
    ("player_situational_L5", "lng"): (
        "passing_long", "rushing_long", "receiving_long", "punt_long",
    ),
    ("player_situational_L5", "td"): (
        "passing_tds", "rushing_tds", "receiving_tds",
    ),
    ("player_situational_L5", "1st"): (
        "passing_first_downs", "receiving_first_downs", "rushing_first_downs",
    ),
    ("player_situational_L5", "20"): (
        "pass_explosive_20", "rec_explosive_20", "receptions_20_29",
    ),
    ("player_situational_L5", "att"): ("attempts", "carries", "completions"),
    ("player_situational_L5", "comp"): ("completions", "receptions", "attempts"),
    ("player_situational_L5", "scky"): ("sack_yards_lost", "def_sack_yards"),
    ("player_situational_L6", "yds"): (
        "punt_yards", "passing_yards", "rushing_yards",
    ),
    ("player_situational_L6", "lng"): (
        "punt_long", "passing_long", "rushing_long",
    ),
    ("player_situational_L6", "punts"): ("punts", "punt_returns", "attempts"),
    ("player_situational_L6", "blk"): ("punts_blocked", "fg_blocked", "pat_blocked"),
    ("player_situational_L7", "fgm"): ("fg_made", "fg_att", "pat_made"),
    ("player_situational_L7", "fg_att"): ("fg_att", "fg_made", "pat_att"),
}


def witness_form(layout: str, source_column: str) -> str:
    """Return the aggregation form for a published Attempts cell."""
    return "MAX" if source_column == "lng" else "SUM"


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def identity_checks(con: duckdb.DuckDBPyConnection, dimension: str) -> dict:
    """Return source-side equations that constrain a composite material field."""
    if dimension not in {
        "Field Position", "Home vs Road", "Stadium Surfaces", "Quarters",
        "Game Halves", "Margin of Victory", "Point Differential",
    }:
        return {}
    complete, equal, source_hi, target_hi = con.execute(
        f"""SELECT
            COUNT(*) FILTER (
                WHERE _table='{dimension}' AND _layout='player_situational_L0'
                  AND TRY_CAST(total AS DOUBLE) IS NOT NULL
                  AND TRY_CAST(solo AS DOUBLE) IS NOT NULL
                  AND TRY_CAST(ast AS DOUBLE) IS NOT NULL
            ),
            COUNT(*) FILTER (
                WHERE _table='{dimension}' AND _layout='player_situational_L0'
                  AND TRY_CAST(total AS DOUBLE) IS NOT NULL
                  AND TRY_CAST(solo AS DOUBLE) IS NOT NULL
                  AND TRY_CAST(ast AS DOUBLE) IS NOT NULL
                  AND TRY_CAST(total AS DOUBLE) =
                      TRY_CAST(solo AS DOUBLE) + TRY_CAST(ast AS DOUBLE)
            ),
            COUNT(*) FILTER (
                WHERE _table='{dimension}' AND _layout='player_situational_L0'
                  AND TRY_CAST(total AS DOUBLE) >
                      TRY_CAST(solo AS DOUBLE) + TRY_CAST(ast AS DOUBLE)
            ),
            COUNT(*) FILTER (
                WHERE _table='{dimension}' AND _layout='player_situational_L0'
                  AND TRY_CAST(total AS DOUBLE) <
                      TRY_CAST(solo AS DOUBLE) + TRY_CAST(ast AS DOUBLE)
            )
          FROM src"""
    ).fetchone()
    return {
        "equation": "total = solo + ast",
        "complete_n": int(complete or 0),
        "equal_n": int(equal or 0),
        "source_exceeds_parts": int(source_hi or 0),
        "parts_exceed_source": int(target_hi or 0),
        "agree_pct": round(100.0 * equal / complete, 2) if complete else None,
    }


def _dimension_plan(dimension: str, declared_tables: int) -> SourcePlan:
    predicate = dimension_where(dimension)
    return SourcePlan(
        "nflcom_player_situational",
        S.registry(include_subject=False)["nflcom_player_situational"].path,
        "nflcom_slug",
        ["_layout"],
        row_filter=predicate,
        declared_tables=declared_tables,
        table_key_part=1,
        source_read_filter=predicate,
        note=f"{dimension} axis aggregated within player-season.",
    )


def _candidate_measurement(
    con: duckdb.DuckDBPyConnection,
    dimension: str,
    layout: str,
    source_column: str,
    canonical: str,
) -> dict:
    agg = witness_form(layout, source_column)
    source_agg = "MAX" if agg == "MAX" else "SUM"
    sql = f"""WITH s AS (
            SELECT nflcom_slug, TRY_CAST(season AS INTEGER) AS year,
                   {source_agg}(TRY_CAST(\"{source_column}\" AS DOUBLE)) AS source_value
            FROM src
            WHERE {dimension_where(dimension)} AND _layout='{layout}'
              AND TRY_CAST(\"{source_column}\" AS DOUBLE) IS NOT NULL
            GROUP BY 1, 2
          ), v AS (
            SELECT i.nflcom_slug, year,
                   {source_agg}(TRY_CAST(v.\"{canonical}\" AS DOUBLE)) AS target_value
            FROM ids i JOIN v26 v ON v.NFL_player_id=i.NFL_player_id
            WHERE v.season_type='REG'
            GROUP BY 1, 2
          ), j AS (
            SELECT s.source_value, v.target_value
            FROM s JOIN v USING (nflcom_slug, year)
          )
          SELECT COUNT(*) AS joined_n,
                 COUNT(*) FILTER (WHERE source_value > 0 OR target_value > 0) AS informative_n,
                 COUNT(*) FILTER (WHERE (source_value > 0 OR target_value > 0)
                                    AND source_value = target_value) AS agree_n,
                 COUNT(*) FILTER (WHERE source_value > target_value) AS source_exceeds_target,
                 COUNT(*) FILTER (WHERE source_value < target_value) AS target_exceeds_source,
                 COUNT(*) FILTER (WHERE source_value > 0) AS source_nonzero_n,
                 COUNT(*) FILTER (WHERE target_value > 0) AS target_nonzero_n,
                 MEDIAN(source_value) FILTER (WHERE source_value > 0 OR target_value > 0),
                 MEDIAN(target_value) FILTER (WHERE source_value > 0 OR target_value > 0)
          FROM j"""
    (
        joined,
        informative,
        agree,
        source_hi,
        target_hi,
        source_nonzero,
        target_nonzero,
        source_median,
        target_median,
    ) = con.execute(sql).fetchone()
    informative = int(informative or 0)
    agree = int(agree or 0)
    return {
        "canonical": canonical,
        "aggregation": agg,
        "joined_n": int(joined or 0),
        "informative_n": informative,
        "agree_n": agree,
        "agree_pct": round(100.0 * agree / informative, 2) if informative else None,
        "source_exceeds_target": int(source_hi or 0),
        "target_exceeds_source": int(target_hi or 0),
        "source_nonzero_n": int(source_nonzero or 0),
        "target_nonzero_n": int(target_nonzero or 0),
        "median_source": source_median,
        "median_target": target_median,
    }


def measure_dimension(
    dimension: str,
    layouts: dict[str, str],
    candidates: dict[tuple[str, str], tuple[str, ...]],
) -> dict:
    primary = audit_source(_dimension_plan(dimension, len(layouts)))
    con = duckdb.connect()
    source = S.registry(include_subject=False)["nflcom_player_situational"].path
    slug_path = S.registry(include_subject=False)["nflcom_slug_pfrid"].path
    bio_path = S.registry(include_subject=False)["player_bio"].path
    v26_path = S.latest_v26()
    raw_source_rows_all = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_q(source)}/**/*.parquet', union_by_name=true) "
        f"WHERE _table='{dimension}'"
    ).fetchone()[0]
    raw_source_rows_used = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{_q(source)}/**/*.parquet', union_by_name=true) "
        f"WHERE {dimension_where(dimension)}"
    ).fetchone()[0]
    con.execute(
        f"CREATE TEMP TABLE src AS SELECT DISTINCT * FROM read_parquet('{_q(source)}/**/*.parquet', union_by_name=true) "
        f"WHERE {dimension_where(dimension)}"
    )
    con.execute(
        f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(slug_path)}') x
        JOIN read_parquet('{_q(bio_path)}') b ON b.pfr_id=x.pfr_id"""
    )
    candidate_names = sorted({c for cs in candidates.values() for c in cs})
    selected = ", ".join(f'"{c}"' for c in candidate_names)
    con.execute(
        f"""CREATE TEMP TABLE v26 AS
        SELECT NFL_player_id, year, season_type, {selected}
        FROM read_parquet('{_q(v26_path)}')
        WHERE season_type='REG'"""
    )
    candidate_matrix: dict[str, list[dict]] = {}
    for key, candidate_values in candidates.items():
        layout, source_column = key
        candidate_matrix[f"{layout}.{source_column}"] = [
            _candidate_measurement(con, dimension, layout, source_column, candidate)
            for candidate in candidate_values
        ]
    lost = {
        str(column): int(count)
        for column, count in con.execute(
            f"SELECT _lost_column, COUNT(*) FROM src WHERE {dimension_where(dimension)} GROUP BY 1 ORDER BY 1"
        ).fetchall()
    }
    lost_by_layout: dict[str, dict[str, int]] = {}
    for layout, column, count in con.execute(
        f"""SELECT _layout, _lost_column, COUNT(*)
        FROM src WHERE {dimension_where(dimension)}
        GROUP BY 1, 2 ORDER BY 1, 2"""
    ).fetchall():
        lost_by_layout.setdefault(str(layout), {})[str(column)] = int(count)
    capture_recoverability = check_capture_recoverability(
        candidate_matrix, lost_by_layout
    )
    source_rows = con.execute(
        f"SELECT COUNT(*) FROM src WHERE {dimension_where(dimension)}"
    ).fetchone()[0]
    identities = identity_checks(con, dimension)
    con.close()
    result = {
        "source": "nflcom_player_situational",
        "subtable": dimension,
        "source_rows_raw": int(raw_source_rows_all),
        "source_rows_raw_used": int(raw_source_rows_used),
        "partition_excluded_rows_raw": int(raw_source_rows_all - raw_source_rows_used),
        "source_rows": int(source_rows),
        "layouts": layouts,
        "lost_column_counts": lost,
        "lost_column_counts_by_layout": lost_by_layout,
        "identity_checks": identities,
        "primary_audit": primary,
        "candidate_matrix": candidate_matrix,
        "capture_recoverability": capture_recoverability,
        "denominator": (
            "joined (nflcom_slug, season) rows with source and v26 values; informative_n "
            "is the nonzero union used for exact agreement"
        ),
    }
    output = OUT_BY_DIMENSION[dimension]
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, default=str), encoding="utf-8")
    return result


def measure_attempts() -> dict:
    return measure_dimension("Attempts", ATTEMPTS_LAYOUTS, CANDIDATE_COLUMNS)


def measure_field_position() -> dict:
    return measure_dimension(
        "Field Position", FIELD_POSITION_LAYOUTS, FIELD_POSITION_CANDIDATES
    )


def measure_home_vs_road() -> dict:
    return measure_dimension("Home vs Road", HOME_VS_ROAD_LAYOUTS, FIELD_POSITION_CANDIDATES)


def measure_stadium_surfaces() -> dict:
    return measure_dimension("Stadium Surfaces", STADIUM_SURFACES_LAYOUTS, FIELD_POSITION_CANDIDATES)


def measure_quarters() -> dict:
    return measure_dimension("Quarters", QUARTERS_LAYOUTS, FIELD_POSITION_CANDIDATES)


def measure_game_halves() -> dict:
    return measure_dimension("Game Halves", GAME_HALVES_LAYOUTS, FIELD_POSITION_CANDIDATES)


def measure_margin_of_victory() -> dict:
    return measure_dimension(
        "Margin of Victory", MARGIN_OF_VICTORY_LAYOUTS, FIELD_POSITION_CANDIDATES
    )


def measure_point_differential() -> dict:
    return measure_dimension(
        "Point Differential", POINT_DIFFERENTIAL_LAYOUTS, FIELD_POSITION_CANDIDATES
    )


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--dimension", choices=(
            "attempts", "field_position", "home_vs_road", "stadium_surfaces",
            "quarters", "game_halves", "margin_of_victory", "point_differential",
        ), default="attempts"
    )
    args = parser.parse_args()
    if args.dimension == "attempts":
        result = measure_attempts()
    elif args.dimension == "field_position":
        result = measure_field_position()
    elif args.dimension == "home_vs_road":
        result = measure_home_vs_road()
    elif args.dimension == "stadium_surfaces":
        result = measure_stadium_surfaces()
    elif args.dimension == "quarters":
        result = measure_quarters()
    elif args.dimension == "game_halves":
        result = measure_game_halves()
    elif args.dimension == "margin_of_victory":
        result = measure_margin_of_victory()
    else:
        result = measure_point_differential()
    measured = [
        row for row in result["primary_audit"]["rows"] if row.get("audit") == "MEASURED"
    ]
    print(
        f"{result['subtable']}: {result['source_rows']:,} distinct source rows, "
        f"{len(measured)} mapped fields -> {OUT_BY_DIMENSION[result['subtable']]}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
