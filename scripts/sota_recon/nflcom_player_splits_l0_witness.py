"""Measure the L0 Defense and L1 Fumbles witness forms for ``nflcom_player_splits``.

This is a measurement-only lane.  It does not change the disposition ledger,
the v26 release, or the license file.  Counts are measured at the declared
``(nflcom_slug, season)`` grain on the ``Months`` partition.  The L0 AVG cell
is witnessed by recomputing ``YDS / INT``; it is not summed or compared as a
stored rate.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Sequence

import duckdb

from .sources import latest_v26, registry


OUT = (
    Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
    / "nflcom_player_splits_witness.json"
)
SOURCE = Path(r"D:/league-history-data/nfl/raw/nflcom/tables/player_splits_unshifted/unshifted.parquet")
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)
TOLERANCE = 0.05


def _q(path: str | Path) -> str:
    return str(path).replace("'", "''")


def compare_ratio_rows(
    rows: Iterable[Sequence[float | int | None]],
    tolerance: float = TOLERANCE,
    scale: float = 1.0,
) -> dict[str, int]:
    """Compare source and target ratios, refusing zero/unknown denominators."""
    result = {
        "informative_n": 0,
        "agree_n": 0,
        "source_exceeds_target": 0,
        "target_exceeds_source": 0,
    }
    for source_yards, source_attempts, target_yards, target_attempts in rows:
        if any(value is None for value in (source_yards, source_attempts, target_yards, target_attempts)):
            continue
        if source_attempts <= 0 or target_attempts <= 0:
            continue
        source_ratio = scale * source_yards / source_attempts
        target_ratio = scale * target_yards / target_attempts
        delta = source_ratio - target_ratio
        result["informative_n"] += 1
        if abs(delta) <= tolerance:
            result["agree_n"] += 1
        elif delta > 0:
            result["source_exceeds_target"] += 1
        else:
            result["target_exceeds_source"] += 1
    return result


def compare_exact_rows(
    rows: Iterable[Sequence[float | int | None]],
) -> dict[str, int]:
    """Compare count cells without treating an unknown as zero."""
    result = {
        "informative_n": 0,
        "agree_n": 0,
        "source_exceeds_target": 0,
        "target_exceeds_source": 0,
    }
    for source_value, target_value in rows:
        if source_value is None or target_value is None:
            continue
        result["informative_n"] += 1
        if source_value == target_value:
            result["agree_n"] += 1
        elif source_value > target_value:
            result["source_exceeds_target"] += 1
        else:
            result["target_exceeds_source"] += 1
    return result


def summarize_capture_loss(
    rows: Iterable[Sequence[str | int | None]],
) -> list[dict]:
    """Summarize the parser-declared lost field without treating it as unknown data."""
    by_layout: dict[str, dict] = {}
    for layout, table, lost_column, row_count in rows:
        body = by_layout.setdefault(
            str(layout),
            {"layout": str(layout), "rows": 0, "split_tables": [], "lost_columns": []},
        )
        body["rows"] += int(row_count)
        if str(table) not in body["split_tables"]:
            body["split_tables"].append(str(table))
        lost = str(lost_column) if lost_column not in (None, "") else None
        if lost is not None and lost not in body["lost_columns"]:
            body["lost_columns"].append(lost)
    return [
        {
            **body,
            "split_tables": sorted(body["split_tables"]),
            "lost_columns": sorted(body["lost_columns"]),
            "all_rows_declare_loss": len(body["lost_columns"]) == 1,
        }
        for body in sorted(by_layout.values(), key=lambda item: item["layout"])
    ]


def measure_capture_loss(con: duckdb.DuckDBPyConnection, source: Path) -> list[dict]:
    rows = con.execute(
        f"""SELECT _layout, _table, _lost_column, COUNT(*)
        FROM read_parquet('{_q(source)}')
        GROUP BY 1, 2, 3
        ORDER BY 1, 2, 3"""
    ).fetchall()
    return summarize_capture_loss(rows)


WITNESS_SOURCE_COLUMNS = {
    "player_splits_L0": {
        "total", "solo", "ast", "yds", "int", "pdef", "sck", "sfty", "tds",
    },
    "player_splits_L1": {"ff", "fum", "lost", "opp_fr", "own_fr"},
    "player_splits_L2": {"1st", "20", "lng", "rec", "td", "yds"},
    "player_splits_L3": {"att", "lng", "td", "yds"},
    "player_splits_L4": {"ret", "yds", "td", "lng", "fc", "20", "40"},
    "player_splits_L5": {
        "1st", "20", "att", "comp", "int", "lng", "sck", "scky", "td", "yds",
        "avg", "pct", "1st_2",
    },
    "player_splits_L6": {"blk", "lng", "punts", "yds", "avg"},
    "player_splits_L7": {"fg_att", "fgm"},
}

L0_MAPPINGS = {
    "ast": "def_tackle_assists",
    "int": "def_interceptions",
    "pdef": "def_pass_defended",
    "sck": "def_sacks",
    "sfty": "def_safeties",
    "solo": "def_tackles_solo",
    "tds": "def_int_ret_td",
    "total": "def_tackles_combined",
    "yds": "def_interception_yards",
}
L0_TARGETS = tuple(L0_MAPPINGS.values())


def check_capture_recoverability(capture_loss: list[dict]) -> list[dict]:
    """Prove that every selected witness field survives the parser boundary."""
    by_layout = {row["layout"]: row for row in capture_loss}
    result = []
    for layout, columns in WITNESS_SOURCE_COLUMNS.items():
        row = by_layout.get(layout, {})
        lost = set(row.get("lost_columns", []))
        overlap = sorted(columns & lost)
        result.append(
            {
                "layout": layout,
                "witness_columns": sorted(columns),
                "lost_columns": sorted(lost),
                "lost_witness_columns": overlap,
                "status": (
                    "RECOVERABLE_WITNESS_FIELDS"
                    if row.get("all_rows_declare_loss") and not overlap
                    else "PROOF_PENDING_CAPTURE"
                ),
            }
        )
    return result


def rank_candidate_results(results: list[dict]) -> list[dict]:
    """Rank a source column's candidate canonicals by measured agreement."""
    return sorted(
        results,
        key=lambda row: (-row["agree_pct"], row["canonical"]),
    )


def compare_tackle_identity(
    rows: Iterable[Sequence[float | int | None]],
) -> dict[str, int]:
    """Re-derive the source's combined-tackle identity on complete rows."""
    result = {"complete_n": 0, "agree_n": 0}
    for total, solo, assists in rows:
        if any(value is None for value in (total, solo, assists)):
            continue
        result["complete_n"] += 1
        result["agree_n"] += int(total == solo + assists)
    return result


L1_MAPPINGS = {
    "ff": "def_fumbles_forced",
    "fum": "fumbles",
    "lost": "fumbles_lost",
    "opp_fr": "fumble_recovery_opp",
    "own_fr": "fumble_recovery_own",
}
L1_TARGETS = tuple(L1_MAPPINGS.values())
L2_MAPPINGS = {
    "1st": "receiving_first_downs",
    "20": "rec_explosive_20",
    "lng": "receiving_long",
    "rec": "receptions",
    "td": "receiving_tds",
    "yds": "receiving_yards",
}
L2_TARGETS = tuple(L2_MAPPINGS.values())
L3_MAPPINGS = {
    "att": "carries",
    "lng": "rushing_long",
    "td": "rushing_tds",
    "yds": "rushing_yards",
}
L3_TARGETS = tuple(L3_MAPPINGS.values())
L5_MAPPINGS = {
    "1st": "passing_first_downs",
    "20": "pass_explosive_20",
    "att": "attempts",
    "comp": "completions",
    "int": "passing_interceptions",
    "lng": "passing_long",
    "sck": "sacks_suffered",
    "scky": "sack_yards_lost",
    "td": "passing_tds",
    "yds": "passing_yards",
}
L5_TARGETS = tuple(L5_MAPPINGS.values())
L6_MAPPINGS = {
    "blk": "punts_blocked",
    "lng": "punt_long",
    "punts": "punts",
    "yds": "punt_yards",
}
L6_TARGETS = tuple(L6_MAPPINGS.values())
L7_MAPPINGS = {
    "fg_att": "fg_att",
    "fgm": "fg_made",
}
L7_TARGETS = tuple(L7_MAPPINGS.values())


def _exact_measurement(
    con,
    source: str,
    v26: str,
    source_column: str,
    canonical: str,
    layout: str = "player_splits_L1",
    aggregation: str = "SUM",
):
    return con.execute(
        f"""WITH source_totals AS (
          SELECT nflcom_slug, TRY_CAST(season AS INTEGER) AS season_year,
                 {aggregation}(TRY_CAST(\"{source_column}\" AS DOUBLE)) AS source_value
          FROM read_parquet('{source}')
          WHERE _layout = '{layout}' AND _table = 'Months'
            AND TRY_CAST(\"{source_column}\" AS DOUBLE) IS NOT NULL
          GROUP BY 1, 2
        ), target_totals AS (
          SELECT i.nflcom_slug, v.year AS season_year,
                 {aggregation}(TRY_CAST(v.\"{canonical}\" AS DOUBLE)) AS target_value
          FROM ids i
          JOIN read_parquet('{v26}') v ON v.NFL_player_id = i.NFL_player_id
          WHERE v.season_type = 'REG'
            AND v.\"{canonical}\" IS NOT NULL
          GROUP BY 1, 2
        )
        SELECT s.source_value, t.target_value
        FROM source_totals s
        JOIN target_totals t USING (nflcom_slug, season_year)"""
    ).fetchall()


def _ratio_measurement(
    con,
    source: str,
    v26: str,
    layout: str,
    source_numerator: str,
    source_denominator: str,
    target_numerator: str,
    target_denominator: str,
):
    return con.execute(
        f"""WITH source_totals AS (
          SELECT nflcom_slug, TRY_CAST(season AS INTEGER) AS season_year,
                 SUM(TRY_CAST(\"{source_numerator}\" AS DOUBLE)) AS source_numerator,
                 SUM(TRY_CAST(\"{source_denominator}\" AS DOUBLE)) AS source_denominator
          FROM read_parquet('{source}')
          WHERE _layout = '{layout}' AND _table = 'Months'
            AND TRY_CAST(\"{source_numerator}\" AS DOUBLE) IS NOT NULL
            AND TRY_CAST(\"{source_denominator}\" AS DOUBLE) IS NOT NULL
          GROUP BY 1, 2
        ), target_totals AS (
          SELECT i.nflcom_slug, v.year AS season_year,
                 SUM(TRY_CAST(v.\"{target_numerator}\" AS DOUBLE)) AS target_numerator,
                 SUM(TRY_CAST(v.\"{target_denominator}\" AS DOUBLE)) AS target_denominator
          FROM ids i
          JOIN read_parquet('{v26}') v ON v.NFL_player_id = i.NFL_player_id
          WHERE v.season_type = 'REG'
            AND v.\"{target_numerator}\" IS NOT NULL
            AND v.\"{target_denominator}\" IS NOT NULL
          GROUP BY 1, 2
        )
        SELECT s.source_numerator, s.source_denominator,
               t.target_numerator, t.target_denominator
        FROM source_totals s
        JOIN target_totals t USING (nflcom_slug, season_year)"""
    ).fetchall()


def _measure_exact_layout(con, source, v26, layout, mappings, targets):
    exact = []
    candidates_by_source = {}
    for source_column, canonical in mappings.items():
        aggregation = "MAX" if source_column == "lng" else "SUM"
        rows = _exact_measurement(
            con, source, v26, source_column, canonical,
            layout=layout, aggregation=aggregation,
        )
        measured = compare_exact_rows(rows)
        measured.update({
            "source_column": source_column,
            "canonical": canonical,
            "joined_n": len(rows),
            "agree_pct": round(100.0 * measured["agree_n"] / measured["informative_n"], 2)
            if measured["informative_n"] else None,
            "witness": aggregation,
            "partition": f"_layout={layout}, _table=Months",
        })
        exact.append(measured)
        candidates = []
        for candidate in targets:
            candidate_rows = _exact_measurement(
                con, source, v26, source_column, candidate,
                layout=layout, aggregation=aggregation,
            )
            candidate_result = compare_exact_rows(candidate_rows)
            candidate_result.update({
                "canonical": candidate,
                "joined_n": len(candidate_rows),
                "agree_pct": round(
                    100.0 * candidate_result["agree_n"] /
                    candidate_result["informative_n"], 2
                ) if candidate_result["informative_n"] else None,
                "is_natural_mapping": candidate == canonical,
            })
            candidates.append(candidate_result)
        candidates_by_source[source_column] = rank_candidate_results(candidates)
    return exact, candidates_by_source


def _measure_l4_combined(con, source, v26):
    pairs = {
        "ret": ("kickoff_returns", "punt_returns", "SUM"),
        "yds": ("kickoff_return_yards", "punt_return_yards", "SUM"),
        "td": ("kickoff_return_tds", "punt_return_tds", "SUM"),
        "lng": ("kickoff_return_long", "punt_return_long", "MAX"),
    }
    exact = []
    for source_column, (kickoff, punt, aggregation) in pairs.items():
        target_expr = (
            f"GREATEST(MAX(TRY_CAST(v.{kickoff} AS DOUBLE)), "
            f"MAX(TRY_CAST(v.{punt} AS DOUBLE)))"
            if aggregation == "MAX" else
            f"SUM(TRY_CAST(v.{kickoff} AS DOUBLE)) + "
            f"SUM(TRY_CAST(v.{punt} AS DOUBLE))"
        )
        q = f"""WITH source_totals AS (
          SELECT nflcom_slug, TRY_CAST(season AS INTEGER) AS season_year,
                 {aggregation}(TRY_CAST(\"{source_column}\" AS DOUBLE)) AS source_value
          FROM read_parquet('{source}')
          WHERE _layout = 'player_splits_L4' AND _table = 'Months'
            AND TRY_CAST(\"{source_column}\" AS DOUBLE) IS NOT NULL
          GROUP BY 1, 2
        ), target_totals AS (
          SELECT i.nflcom_slug, v.year AS season_year,
                 {target_expr} AS target_value
          FROM ids i
          JOIN read_parquet('{v26}') v ON v.NFL_player_id = i.NFL_player_id
          WHERE v.season_type = 'REG'
            AND v.{kickoff} IS NOT NULL AND v.{punt} IS NOT NULL
          GROUP BY 1, 2
        )
        SELECT s.source_value, t.target_value
        FROM source_totals s JOIN target_totals t USING (nflcom_slug, season_year)"""
        rows = con.execute(q).fetchall()
        measured = compare_exact_rows(rows)
        measured.update({
            "source_column": source_column,
            "source_witness": aggregation,
            "canonical_expression": f"{kickoff} + {punt}" if aggregation == "SUM" else
                f"MAX({kickoff}, {punt})",
            "joined_n": len(rows),
            "agree_pct": round(100.0 * measured["agree_n"] / measured["informative_n"], 2)
            if measured["informative_n"] else None,
            "partition": "_layout=player_splits_L4, _table=Months",
        })
        exact.append(measured)
    # `total_return_yards` is an existing v26 canonical that was omitted from the
    # first L4 pass.  It is not a new guess: the weekly subject itself proves the
    # target identity `total_return_yards = kickoff_return_yards + punt_return_yards`.
    # Measure it against the source's combined YDS cell with the same player-season
    # denominator, retaining the candidate beside the prior two-component form.
    total_return_rows = con.execute(
        f"""WITH source_totals AS (
          SELECT nflcom_slug, TRY_CAST(season AS INTEGER) AS season_year,
                 SUM(TRY_CAST(yds AS DOUBLE)) AS source_value
          FROM read_parquet('{source}')
          WHERE _layout = 'player_splits_L4' AND _table = 'Months'
            AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
          GROUP BY 1, 2
        ), target_totals AS (
          SELECT i.nflcom_slug, v.year AS season_year,
                 SUM(TRY_CAST(v.total_return_yards AS DOUBLE)) AS target_value
          FROM ids i
          JOIN read_parquet('{v26}') v ON v.NFL_player_id = i.NFL_player_id
          WHERE v.season_type = 'REG' AND v.total_return_yards IS NOT NULL
          GROUP BY 1, 2
        )
        SELECT s.source_value, t.target_value
        FROM source_totals s JOIN target_totals t USING (nflcom_slug, season_year)"""
    ).fetchall()
    total_return_measured = compare_exact_rows(total_return_rows)
    total_return_candidate = {
        **total_return_measured,
        "canonical": "total_return_yards",
        "aggregation": "SUM",
        "joined_n": len(total_return_rows),
        "agree_pct": round(
            100.0 * total_return_measured["agree_n"] /
            total_return_measured["informative_n"], 2
        ) if total_return_measured["informative_n"] else None,
        "source_column": "yds",
        "target_identity": "total_return_yards = kickoff_return_yards + punt_return_yards",
        "partition": "_layout=player_splits_L4, _table=Months",
    }
    avg_rows = con.execute(
        f"""WITH source_totals AS (
          SELECT nflcom_slug, TRY_CAST(season AS INTEGER) AS season_year,
                 SUM(TRY_CAST(yds AS DOUBLE)) AS source_yards,
                 SUM(TRY_CAST(ret AS DOUBLE)) AS source_returns
          FROM read_parquet('{source}')
          WHERE _layout = 'player_splits_L4' AND _table = 'Months'
            AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
            AND TRY_CAST(ret AS DOUBLE) IS NOT NULL
          GROUP BY 1, 2
        ), target_totals AS (
          SELECT i.nflcom_slug, v.year AS season_year,
                 SUM(TRY_CAST(v.kickoff_return_yards AS DOUBLE)) +
                   SUM(TRY_CAST(v.punt_return_yards AS DOUBLE)) AS target_yards,
                 SUM(TRY_CAST(v.kickoff_returns AS DOUBLE)) +
                   SUM(TRY_CAST(v.punt_returns AS DOUBLE)) AS target_returns
          FROM ids i
          JOIN read_parquet('{v26}') v ON v.NFL_player_id = i.NFL_player_id
          WHERE v.season_type = 'REG'
            AND v.kickoff_return_yards IS NOT NULL AND v.punt_return_yards IS NOT NULL
            AND v.kickoff_returns IS NOT NULL AND v.punt_returns IS NOT NULL
          GROUP BY 1, 2
        )
        SELECT s.source_yards, s.source_returns, t.target_yards, t.target_returns
        FROM source_totals s JOIN target_totals t USING (nflcom_slug, season_year)"""
    ).fetchall()
    ratio = compare_ratio_rows(avg_rows)
    ratio.update({
        "source_column": "avg",
        "source_expression": "SUM(yds) / SUM(ret)",
        "canonical_expression":
            "(SUM(kickoff_return_yards)+SUM(punt_return_yards)) / "
            "(SUM(kickoff_returns)+SUM(punt_returns))",
        "joined_n": len(avg_rows),
        "agree_pct": round(100.0 * ratio["agree_n"] / ratio["informative_n"], 2)
        if ratio["informative_n"] else None,
        "tolerance": TOLERANCE,
        "partition": "_layout=player_splits_L4, _table=Months",
    })
    detail = []
    for source_column in ("fc", "20", "40"):
        row = con.execute(
            f"""SELECT COUNT(*) AS rows,
                       SUM(CASE WHEN TRY_CAST(\"{source_column}\" AS DOUBLE) IS NOT NULL
                                THEN 1 ELSE 0 END) AS populated
            FROM read_parquet('{source}')
            WHERE _layout = 'player_splits_L4' AND _table = 'Months'"""
        ).fetchone()
        detail.append({
            "source_column": source_column,
            "source_rows": int(row[0]),
            "populated_n": int(row[1] or 0),
            "status": "NEW_SUPERTABLE_COLUMN_CANDIDATE",
            "reason": "L4 return-page detail has no existing v26 canonical and no surviving kick/punt page label",
        })
    return {
        "combined_exact": exact,
        "candidate_matrix": {"yds": [total_return_candidate]},
        "avg": ratio,
        "detail_candidates": detail,
    }


def measure() -> dict:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    con.execute(
        f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id = x.pfr_id"""
    )
    v26 = _q(latest_v26())
    source = _q(SOURCE)
    ratio_rows = con.execute(
        f"""WITH source_totals AS (
          SELECT nflcom_slug, TRY_CAST(season AS INTEGER) AS season_year,
                 SUM(TRY_CAST(yds AS DOUBLE)) AS source_yards,
                 SUM(TRY_CAST(int AS DOUBLE)) AS source_interceptions
          FROM read_parquet('{source}')
          WHERE _layout = 'player_splits_L0' AND _table = 'Months'
            AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
            AND TRY_CAST(int AS DOUBLE) IS NOT NULL
          GROUP BY 1, 2
        ), target_totals AS (
          SELECT i.nflcom_slug, v.year AS season_year,
                 SUM(TRY_CAST(v.def_interception_yards AS DOUBLE)) AS target_yards,
                 SUM(TRY_CAST(v.def_interceptions AS DOUBLE)) AS target_interceptions
          FROM ids i
          JOIN read_parquet('{v26}') v ON v.NFL_player_id = i.NFL_player_id
          WHERE v.season_type = 'REG'
          GROUP BY 1, 2
        )
        SELECT s.source_yards, s.source_interceptions,
               t.target_yards, t.target_interceptions
        FROM source_totals s
        JOIN target_totals t USING (nflcom_slug, season_year)"""
    ).fetchall()
    ratio = compare_ratio_rows(ratio_rows)
    ratio.update({
        "joined_n": len(ratio_rows),
        "agree_pct": round(100.0 * ratio["agree_n"] / ratio["informative_n"], 2)
        if ratio["informative_n"] else None,
        "source_column": "avg",
        "source_expression": "SUM(yds) / SUM(int)",
        "canonical_expression":
            "SUM(def_interception_yards) / SUM(def_interceptions)",
        "partition": "_layout=player_splits_L0, _table=Months",
        "tolerance": TOLERANCE,
    })

    identity_rows = con.execute(
        f"""SELECT TRY_CAST(total AS DOUBLE), TRY_CAST(solo AS DOUBLE),
                      TRY_CAST(ast AS DOUBLE)
        FROM read_parquet('{source}')
        WHERE _layout = 'player_splits_L0' AND _table = 'Months'"""
    ).fetchall()
    identity = compare_tackle_identity(identity_rows)
    identity.update({
        "agree_pct": round(100.0 * identity["agree_n"] / identity["complete_n"], 2)
        if identity["complete_n"] else None,
        "expression": "total = solo + ast",
        "partition": "_layout=player_splits_L0, _table=Months",
    })
    l0_exact, l0_candidates = _measure_exact_layout(
        con, source, v26, "player_splits_L0", L0_MAPPINGS, L0_TARGETS
    )
    l1 = []
    l1_candidates = {}
    for source_column, canonical in L1_MAPPINGS.items():
        rows = _exact_measurement(con, source, v26, source_column, canonical)
        measured = compare_exact_rows(rows)
        measured.update({
            "source_column": source_column,
            "canonical": canonical,
            "joined_n": len(rows),
            "agree_pct": round(100.0 * measured["agree_n"] / measured["informative_n"], 2)
            if measured["informative_n"] else None,
            "witness": "SUM",
            "partition": "_layout=player_splits_L1, _table=Months",
        })
        l1.append(measured)
        candidates = []
        for candidate in L1_TARGETS:
            candidate_rows = _exact_measurement(
                con, source, v26, source_column, candidate
            )
            candidate_result = compare_exact_rows(candidate_rows)
            candidate_result.update({
                "canonical": candidate,
                "joined_n": len(candidate_rows),
                "agree_pct": round(
                    100.0 * candidate_result["agree_n"] /
                    candidate_result["informative_n"], 2
                ) if candidate_result["informative_n"] else None,
                "is_natural_mapping": candidate == canonical,
            })
            candidates.append(candidate_result)
        l1_candidates[source_column] = rank_candidate_results(candidates)
    l2 = []
    l2_candidates = {}
    for source_column, canonical in L2_MAPPINGS.items():
        aggregation = "MAX" if source_column == "lng" else "SUM"
        rows = _exact_measurement(
            con, source, v26, source_column, canonical,
            layout="player_splits_L2", aggregation=aggregation,
        )
        measured = compare_exact_rows(rows)
        measured.update({
            "source_column": source_column,
            "canonical": canonical,
            "joined_n": len(rows),
            "agree_pct": round(100.0 * measured["agree_n"] / measured["informative_n"], 2)
            if measured["informative_n"] else None,
            "witness": aggregation,
            "partition": "_layout=player_splits_L2, _table=Months",
        })
        l2.append(measured)
        candidates = []
        for candidate in L2_TARGETS:
            candidate_rows = _exact_measurement(
                con, source, v26, source_column, candidate,
                layout="player_splits_L2", aggregation=aggregation,
            )
            candidate_result = compare_exact_rows(candidate_rows)
            candidate_result.update({
                "canonical": candidate,
                "joined_n": len(candidate_rows),
                "agree_pct": round(
                    100.0 * candidate_result["agree_n"] /
                    candidate_result["informative_n"], 2
                ) if candidate_result["informative_n"] else None,
                "is_natural_mapping": candidate == canonical,
            })
            candidates.append(candidate_result)
        l2_candidates[source_column] = rank_candidate_results(candidates)
    l2_ratios = []
    for name, numerator, denominator, target_numerator, target_denominator, scale in (
        ("avg", "yds", "rec", "receiving_yards", "receptions", 1.0),
        ("1st_2", "1st", "rec", "receiving_first_downs", "receptions", 100.0),
    ):
        rows = _ratio_measurement(
            con, source, v26, "player_splits_L2", numerator, denominator,
            target_numerator, target_denominator,
        )
        measured = compare_ratio_rows(rows, scale=scale)
        measured.update({
            "source_column": name,
            "source_expression": f"SUM({numerator}) / SUM({denominator})"
                + (" * 100" if scale == 100.0 else ""),
            "canonical_expression":
                f"SUM({target_numerator}) / SUM({target_denominator})"
                + (" * 100" if scale == 100.0 else ""),
            "joined_n": len(rows),
            "agree_pct": round(100.0 * measured["agree_n"] / measured["informative_n"], 2)
            if measured["informative_n"] else None,
            "tolerance": TOLERANCE,
            "scale": scale,
            "partition": "_layout=player_splits_L2, _table=Months",
        })
        l2_ratios.append(measured)
    layout_results = {}
    for layout, mappings, targets in (
        ("player_splits_L3", L3_MAPPINGS, L3_TARGETS),
        ("player_splits_L5", L5_MAPPINGS, L5_TARGETS),
        ("player_splits_L6", L6_MAPPINGS, L6_TARGETS),
    ):
        exact, candidates = _measure_exact_layout(
            con, source, v26, layout, mappings, targets
        )
        layout_results[layout] = {
            "exact": exact,
            "candidate_matrix": candidates,
        }
    ratio_specs = {
        "player_splits_L3": [
            ("avg", "yds", "att", "rushing_yards", "carries", 1.0),
        ],
        "player_splits_L5": [
            ("avg", "yds", "att", "passing_yards", "attempts", 1.0),
            ("pct", "comp", "att", "completions", "attempts", 100.0),
            ("1st_2", "1st", "att", "passing_first_downs", "attempts", 100.0),
        ],
        "player_splits_L6": [
            ("avg", "yds", "punts", "punt_yards", "punts", 1.0),
        ],
    }
    for layout, specs in ratio_specs.items():
        ratios = []
        for name, numerator, denominator, target_numerator, target_denominator, scale in specs:
            rows = _ratio_measurement(
                con, source, v26, layout, numerator, denominator,
                target_numerator, target_denominator,
            )
            measured = compare_ratio_rows(rows, scale=scale)
            measured.update({
                "source_column": name,
                "source_expression": f"SUM({numerator}) / SUM({denominator})"
                    + (" * 100" if scale == 100.0 else ""),
                "canonical_expression":
                    f"SUM({target_numerator}) / SUM({target_denominator})"
                    + (" * 100" if scale == 100.0 else ""),
                "joined_n": len(rows),
                "agree_pct": round(100.0 * measured["agree_n"] / measured["informative_n"], 2)
                if measured["informative_n"] else None,
                "tolerance": TOLERANCE,
                "scale": scale,
                "partition": f"_layout={layout}, _table=Months",
            })
            ratios.append(measured)
        layout_results[layout]["ratios"] = ratios
    layout_results["player_splits_L4"] = _measure_l4_combined(con, source, v26)
    l7_exact, l7_candidates = _measure_exact_layout(
        con, source, v26, "player_splits_L7", L7_MAPPINGS, L7_TARGETS
    )
    layout_results["player_splits_L7"] = {
        "exact": l7_exact,
        "candidate_matrix": l7_candidates,
    }
    capture_loss = measure_capture_loss(con, source)
    capture_recoverability = check_capture_recoverability(capture_loss)
    result = {
        "source": "nflcom_player_splits",
        "layouts": [
            "player_splits_L0", "player_splits_L1", "player_splits_L2",
            "player_splits_L3", "player_splits_L4", "player_splits_L5",
            "player_splits_L6", "player_splits_L7",
        ],
        "witness_partition": "Months",
        "season_type": "REG",
        "capture_loss": capture_loss,
        "capture_recoverability": capture_recoverability,
        "avg_witness": ratio,
        "total_identity": identity,
        "l0_exact": l0_exact,
        "l0_candidate_matrix": l0_candidates,
        "l1_exact": l1,
        "l1_candidate_matrix": l1_candidates,
        "l2_exact": l2,
        "l2_candidate_matrix": l2_candidates,
        "l2_ratios": l2_ratios,
        "additional_layouts": layout_results,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    con.close()
    return result


def main() -> int:
    result = measure()
    ratio = result["avg_witness"]
    identity = result["total_identity"]
    print(
        f"L0 AVG: {ratio['agree_n']}/{ratio['informative_n']} "
        f"({ratio['agree_pct']}%)"
    )
    print(
        f"L0 TOTAL identity: {identity['agree_n']}/{identity['complete_n']} "
        f"({identity['agree_pct']}%)"
    )
    for row in result["l1_exact"]:
        print(
            f"L1 {row['source_column']} -> {row['canonical']}: "
            f"{row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)"
        )
    for row in result["l2_exact"]:
        print(
            f"L2 {row['source_column']} -> {row['canonical']}: "
            f"{row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)"
        )
    for row in result["l2_ratios"]:
        print(
            f"L2 {row['source_column']} recompute: "
            f"{row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)"
        )
    for layout, body in result["additional_layouts"].items():
        for row in body.get("exact", []):
            print(
                f"{layout} {row['source_column']} -> {row['canonical']}: "
                f"{row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)"
            )
        for row in body.get("ratios", []):
            print(
                f"{layout} {row['source_column']} recompute: "
                f"{row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)"
            )
    l4 = result["additional_layouts"]["player_splits_L4"]
    for row in l4["combined_exact"]:
        print(
            f"player_splits_L4 {row['source_column']} combined: "
            f"{row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)"
        )
    print(
        f"player_splits_L4 avg combined recompute: "
        f"{l4['avg']['agree_n']}/{l4['avg']['informative_n']} "
        f"({l4['avg']['agree_pct']}%)"
    )
    for row in result["additional_layouts"]["player_splits_L7"]["exact"]:
        print(
            f"player_splits_L7 {row['source_column']} -> {row['canonical']}: "
            f"{row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)"
        )
    print(f"measurement -> {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
