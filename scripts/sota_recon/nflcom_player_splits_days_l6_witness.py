"""Measure the recoverable Days/L6 punting block of player splits."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import latest_v26, registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / "nflcom_player_splits_days_l6_witness.json"
SOURCE = Path(registry(include_subject=False)["nflcom_player_splits"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)
MAPPINGS = {
    "blk": "punts_blocked",
    "lng": "punt_long",
    "punts": "punts",
    "yds": "punt_yards",
}


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _compare(pairs: list[tuple[float | None, float | None]]) -> dict:
    info = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a or b)]
    agree = sum(int(a == b) for a, b in info)
    return {
        "joined_n": len(pairs),
        "informative_n": len(info),
        "agree_n": agree,
        "agree_pct": round(100 * agree / len(info), 2) if info else None,
        "source_exceeds_target": sum(int(a > b) for a, b in info),
        "target_exceeds_source": sum(int(a < b) for a, b in info),
    }


def _measure(con, source_field: str, target_expression: str, form: str, v26: str) -> dict:
    aggregation = "MAX" if source_field == "lng" else "SUM"
    sql = f"""
    WITH s AS (
      SELECT nflcom_slug, TRY_CAST(season AS INTEGER) season_year,
             {aggregation}(TRY_CAST(\"{source_field}\" AS DOUBLE)) source_value
      FROM src WHERE TRY_CAST(\"{source_field}\" AS DOUBLE) IS NOT NULL
      GROUP BY 1, 2
    ), v AS (
      SELECT i.nflcom_slug, v.year season_year, {target_expression} target_value
      FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
      WHERE v.season_type='REG'
      GROUP BY 1, 2
    )
    SELECT s.source_value, v.target_value FROM s JOIN v USING(nflcom_slug, season_year)
    """
    return {"source_field": source_field, "canonical": target_expression, "witness_form": form, **_compare(con.execute(sql).fetchall())}


def _ratio(con, v26: str) -> dict:
    sql = f"""
    WITH s AS (
      SELECT nflcom_slug, TRY_CAST(season AS INTEGER) season_year,
             SUM(TRY_CAST(yds AS DOUBLE)) yds, SUM(TRY_CAST(punts AS DOUBLE)) punts
      FROM src WHERE TRY_CAST(yds AS DOUBLE) IS NOT NULL AND TRY_CAST(punts AS DOUBLE) IS NOT NULL
      GROUP BY 1, 2
    ), v AS (
      SELECT i.nflcom_slug, v.year season_year,
             SUM(TRY_CAST(v.punt_yards AS DOUBLE)) yds, SUM(TRY_CAST(v.punts AS DOUBLE)) punts
      FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
      WHERE v.season_type='REG' AND v.punt_yards IS NOT NULL AND v.punts IS NOT NULL
      GROUP BY 1, 2
    ) SELECT s.yds, s.punts, v.yds, v.punts FROM s JOIN v USING(nflcom_slug, season_year)
    """
    rows = con.execute(sql).fetchall()
    pairs = [(a / b, c / d) for a, b, c, d in rows if b and d]
    return {"source_field": "avg", "source_expression": "SUM(yds) / SUM(punts)", "canonical_expression": "SUM(punt_yards) / SUM(punts)", "witness_form": "RECOMPUTE", **_compare(pairs)}


def _denominator(con, v26: str) -> dict:
    source = con.execute("SELECT COUNT(*) n, SUM(CASE WHEN TRY_CAST(blk AS DOUBLE) <= TRY_CAST(punts AS DOUBLE) THEN 1 ELSE 0 END) ok FROM src WHERE TRY_CAST(blk AS DOUBLE) IS NOT NULL AND TRY_CAST(punts AS DOUBLE) IS NOT NULL").fetchone()
    target = con.execute(f"SELECT COUNT(*) n, SUM(CASE WHEN COALESCE(punts_blocked, 0) <= COALESCE(punts, 0) THEN 1 ELSE 0 END) ok FROM read_parquet('{v26}') WHERE season_type='REG' AND punts_blocked IS NOT NULL AND punts IS NOT NULL").fetchone()
    return {"source": {"rows": int(source[0]), "satisfy_blk_le_punts": int(source[1] or 0)}, "target_weekly": {"rows": int(target[0]), "satisfy_punts_blocked_le_punts": int(target[1] or 0)}}


def measure() -> dict:
    con = duckdb.connect()
    source = _q(SOURCE / "**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE src AS SELECT DISTINCT nflcom_slug,season,split_value,_lost_column,g,yds,avg,lng,ret,punts,blk,rety,in_20,net_avg FROM read_parquet('{source}',union_by_name=true) WHERE _table='Days' AND _layout='player_splits_L6'""")
    con.execute(f"""CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26 = _q(latest_v26())
    fields = [_measure(con, s, f"SUM(v.{c})" if s != "lng" else f"MAX(v.{c})", "MAX" if s == "lng" else "SUM", v26) for s, c in MAPPINGS.items()]
    candidates = {
        "blk": [_measure(con, "blk", "SUM(v.punts_blocked)", "SUM", v26), _measure(con, "blk", "SUM(v.fg_blocked)", "SUM", v26)],
        "ret": [_measure(con, "ret", "SUM(v.punt_returns)", "SUM", v26), _measure(con, "ret", "SUM(v.kickoff_returns)", "SUM", v26)],
        "rety": [_measure(con, "rety", "SUM(v.punt_return_yards)", "SUM", v26), _measure(con, "rety", "SUM(v.kickoff_return_yards)", "SUM", v26)],
        "yds": [_measure(con, "yds", "SUM(v.punt_yards)", "SUM", v26), _measure(con, "yds", "SUM(v.passing_yards)", "SUM", v26)],
        "lng": [_measure(con, "lng", "MAX(v.punt_long)", "MAX", v26), _measure(con, "lng", "MAX(v.kickoff_return_long)", "MAX", v26)],
    }
    for source_field, rows in candidates.items():
        for row in rows:
            row["candidate_role"] = "natural" if row["canonical"] in {MAPPINGS.get(source_field)} else "family contrast"
    loss = con.execute("SELECT _lost_column, COUNT(*) n FROM src GROUP BY 1 ORDER BY 1").fetchall()
    net = con.execute("SELECT COUNT(*) row_n, SUM(CASE WHEN TRY_CAST(net_avg AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) populated FROM src").fetchone()
    target_columns = {x[0] for x in con.execute(f"SELECT * FROM read_parquet('{v26}') LIMIT 0").description}
    games_aliases = ["games_played", "games", "g"]
    in20_aliases = ["punts_inside_20", "punts_in_20", "punt_inside_20", "punt_in_20", "in_20"]
    return {"measurement_only": True, "source_table": "Days", "layout": "player_splits_L6", "subject_plane": "weekly summed to player-season", "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]), "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]), "lost_columns": [{"column": str(a) if a is not None else None, "rows": int(b)} for a, b in loss], "fields": fields + [_ratio(con, v26)], "candidate_matrix": candidates, "denominator_checks": _denominator(con, v26), "unmapped_fields": {"g": {"status": "NO_EXISTING_CANONICAL_FOUND", "schema_search": games_aliases, "schema_matches": sorted(set(games_aliases) & target_columns)}, "ret": {"status": "NO_EXISTING_CANONICAL_FOUND", "schema_search": ["punt_returns_allowed", "punt_return_attempts", "returns"], "schema_matches": []}, "rety": {"status": "NO_EXISTING_CANONICAL_FOUND", "schema_search": ["punt_return_yards_allowed", "punt_returns_yards", "return_yards"], "schema_matches": []}, "in_20": {"status": "NO_EXISTING_CANONICAL_FOUND", "schema_search": in20_aliases, "schema_matches": sorted(set(in20_aliases) & target_columns)}, "net_avg": {"status": "CAPTURE_LOST", "rows": int(net[0]), "populated_n": int(net[1] or 0)}}, "target_aggregation": "SUM weekly REG by (NFL_player_id, year); MAX for long; recompute avg", "no_state_change": True}


def main():
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
