"""Measure the existing combined return-yards witness across situational axes.

This is a measurement-only companion to the broad situational candidate runner.  It
projects the source and weekly subject once, then checks the one L4 field whose page
axis can be safely collapsed: ``YDS`` against ``total_return_yards``.  The other L4
fields remain page-ambiguous and are not inferred from this receipt.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from . import sources as S
from .nflcom_player_situational_attempts_witness import dimension_where


OUT = (
    Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
    / "nflcom_player_situational_return_yards_witness.json"
)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def measure() -> dict:
    con = duckdb.connect()
    con.execute("SET enable_progress_bar=false")
    source = S.registry(include_subject=False)["nflcom_player_situational"].path
    slugs = S.registry(include_subject=False)["nflcom_slug_pfrid"].path
    bio = S.registry(include_subject=False)["player_bio"].path
    v26 = S.latest_v26()

    con.execute(f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(slugs)}') x
        JOIN read_parquet('{_q(bio)}') b ON b.pfr_id=x.pfr_id""")
    con.execute(f"""CREATE TEMP TABLE source_l4 AS
        SELECT DISTINCT _table AS dimension, nflcom_slug,
               TRY_CAST(season AS INTEGER) AS year,
               TRY_CAST(yds AS DOUBLE) AS source_value, split_value
        FROM (
          SELECT DISTINCT *
          FROM read_parquet('{_q(source)}/**/*.parquet', union_by_name=true)
          WHERE _layout='player_situational_L4'
            AND TRY_CAST(yds AS DOUBLE) IS NOT NULL
        )
        """)
    con.execute(f"""CREATE TEMP TABLE target AS
        SELECT i.nflcom_slug, v.year,
               SUM(TRY_CAST(v.total_return_yards AS DOUBLE)) AS target_value
        FROM ids i
        JOIN read_parquet('{_q(v26)}') v ON v.NFL_player_id=i.NFL_player_id
        WHERE v.season_type='REG' AND v.total_return_yards IS NOT NULL
        GROUP BY 1, 2""")

    dimensions = [
        "Field Position", "Home vs Road", "Stadium Surfaces", "Quarters",
        "Game Halves", "Margin of Victory", "Point Differential",
    ]
    results = []
    for dimension in dimensions:
        predicate = dimension_where(dimension).replace("_table", "dimension")
        # Keep the exact source split law used by the broad runner. This prevents
        # nested detail rows from inflating a clean counter.
        rows = con.execute(f"""WITH s AS (
              SELECT nflcom_slug, year, SUM(source_value) AS source_value
              FROM source_l4
              WHERE {predicate}
              GROUP BY 1, 2
            ), j AS (
              SELECT s.source_value, t.target_value
              FROM s JOIN target t USING (nflcom_slug, year)
            )
            SELECT COUNT(*) AS joined_n,
                   COUNT(*) FILTER (WHERE source_value > 0 OR target_value > 0),
                   COUNT(*) FILTER (WHERE (source_value > 0 OR target_value > 0)
                                      AND source_value = target_value),
                   COUNT(*) FILTER (WHERE source_value > target_value),
                   COUNT(*) FILTER (WHERE target_value > source_value),
                   COUNT(*) FILTER (WHERE source_value > 0),
                   COUNT(*) FILTER (WHERE target_value > 0),
                   MEDIAN(source_value) FILTER (WHERE source_value > 0 OR target_value > 0),
                   MEDIAN(target_value) FILTER (WHERE source_value > 0 OR target_value > 0)
            FROM j""").fetchone()
        joined, informative, agree, source_hi, target_hi, source_nonzero, target_nonzero, med_s, med_t = rows
        results.append({
            "dimension": dimension,
            "source_column": "yds",
            "canonical": "total_return_yards",
            "aggregation": "SUM",
            "joined_n": int(joined or 0),
            "informative_n": int(informative or 0),
            "agree_n": int(agree or 0),
            "agree_pct": round(100.0 * agree / informative, 2) if informative else None,
            "source_exceeds_target": int(source_hi or 0),
            "target_exceeds_source": int(target_hi or 0),
            "source_nonzero_n": int(source_nonzero or 0),
            "target_nonzero_n": int(target_nonzero or 0),
            "median_source": med_s,
            "median_target": med_t,
            "denominator": "joined (nflcom_slug, season) rows; informative_n is the nonzero union",
        })
    result = {
        "source": "nflcom_player_situational",
        "layout": "player_situational_L4",
        "source_column": "yds",
        "canonical": "total_return_yards",
        "target_identity": "total_return_yards = kickoff_return_yards + punt_return_yards",
        "subject_grain": "weekly v26 rolled to (nflcom_slug, season)",
        "results": results,
        "scope_note": "L4's kickoff/punt page axis remains lost; only combined YDS has one existing canonical.",
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, default=str) + "\n", encoding="utf-8")
    con.close()
    return result


if __name__ == "__main__":
    result = measure()
    for row in result["results"]:
        print(f"{row['dimension']}: {row['agree_n']}/{row['informative_n']} ({row['agree_pct']}%)")
    print(f"measurement -> {OUT}")
