"""Measure Opponents-by-Team/L4 combined return witnesses."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import latest_v26, registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / "nflcom_player_splits_opponents_team_l4_witness.json"
SOURCE = Path(registry(include_subject=False)["nflcom_player_splits"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _compare(pairs):
    info = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a or b)]
    agree = sum(int(a == b) for a, b in info)
    return {"joined_n": len(pairs), "informative_n": len(info), "agree_n": agree, "agree_pct": round(100 * agree / len(info), 2) if info else None, "source_exceeds_target": sum(int(a > b) for a, b in info), "target_exceeds_source": sum(int(a < b) for a, b in info)}


def _measure(con, source_field, target_expression, form, label, v26):
    source_aggregation = "MAX" if source_field == "lng" else "SUM"
    sql = f"""WITH s AS (SELECT nflcom_slug,TRY_CAST(season AS INTEGER) season_year,{source_aggregation}(TRY_CAST(\"{source_field}\" AS DOUBLE)) source_value FROM src WHERE TRY_CAST(\"{source_field}\" AS DOUBLE) IS NOT NULL GROUP BY 1,2),v AS (SELECT i.nflcom_slug,v.year season_year,{target_expression} target_value FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id WHERE v.season_type='REG' GROUP BY 1,2) SELECT s.source_value,v.target_value FROM s JOIN v USING(nflcom_slug,season_year)"""
    return {"source_field": source_field, "canonical": label, "target_expression": target_expression, "witness_form": form, **_compare(con.execute(sql).fetchall())}


def _denominator(con, v26):
    source = con.execute("SELECT COUNT(*) n,SUM(CASE WHEN TRY_CAST(td AS DOUBLE)<=TRY_CAST(ret AS DOUBLE) THEN 1 ELSE 0 END) ok FROM src WHERE TRY_CAST(td AS DOUBLE) IS NOT NULL AND TRY_CAST(ret AS DOUBLE) IS NOT NULL").fetchone()
    target = con.execute(f"SELECT COUNT(*) n,SUM(CASE WHEN COALESCE(kickoff_return_tds,0)+COALESCE(punt_return_tds,0)<=COALESCE(kickoff_returns,0)+COALESCE(punt_returns,0) THEN 1 ELSE 0 END) ok FROM read_parquet('{v26}') WHERE season_type='REG' AND kickoff_return_tds IS NOT NULL AND punt_return_tds IS NOT NULL AND kickoff_returns IS NOT NULL AND punt_returns IS NOT NULL").fetchone()
    return {"source": {"rows": int(source[0]), "satisfy_td_le_ret": int(source[1] or 0)}, "target_weekly": {"rows": int(target[0]), "satisfy_combined_return_tds_le_returns": int(target[1] or 0)}}


def measure():
    con = duckdb.connect()
    source = _q(SOURCE / "**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE src AS SELECT DISTINCT nflcom_slug,season,split_value,_lost_column,ret,yds,td,lng,fum,fc,\"20\",\"40\" FROM read_parquet('{source}',union_by_name=true) WHERE _table='Opponents by Team' AND _layout='player_splits_L4'""")
    con.execute(f"""CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26 = _q(latest_v26())
    candidates = {"ret": [_measure(con, "ret", "SUM(v.kickoff_returns)+SUM(v.punt_returns)", "SUM kickoffs + punts", "kickoff_returns + punt_returns", v26), _measure(con, "ret", "SUM(v.kickoff_returns)", "SUM kickoff only", "kickoff_returns", v26), _measure(con, "ret", "SUM(v.punt_returns)", "SUM punt only", "punt_returns", v26)], "yds": [_measure(con, "yds", "SUM(v.total_return_yards)", "SUM total_return_yards", "total_return_yards", v26), _measure(con, "yds", "SUM(v.kickoff_return_yards)+SUM(v.punt_return_yards)", "SUM kickoff + punt yards", "kickoff_return_yards + punt_return_yards", v26)], "td": [_measure(con, "td", "SUM(v.kickoff_return_tds)+SUM(v.punt_return_tds)", "SUM kickoffs + punts", "kickoff_return_tds + punt_return_tds", v26), _measure(con, "td", "SUM(v.kickoff_return_tds)", "SUM kickoff only", "kickoff_return_tds", v26), _measure(con, "td", "SUM(v.punt_return_tds)", "SUM punt only", "punt_return_tds", v26)], "lng": [_measure(con, "lng", "GREATEST(MAX(v.kickoff_return_long),MAX(v.punt_return_long))", "MAX kickoffs/punts", "MAX(kickoff_return_long, punt_return_long)", v26), _measure(con, "lng", "MAX(v.kickoff_return_long)", "MAX kickoff only", "kickoff_return_long", v26), _measure(con, "lng", "MAX(v.punt_return_long)", "MAX punt only", "punt_return_long", v26)]}
    for source_field, rows in candidates.items():
        for index, row in enumerate(rows):
            row["candidate_role"] = "natural combined" if index == 0 else "single-component contrast"
    loss = con.execute("SELECT _lost_column,COUNT(*) n FROM src GROUP BY 1 ORDER BY 1").fetchall()
    detail = {}
    for field in ("fum", "fc", "20", "40"):
        row = con.execute(f"SELECT COUNT(*) row_n,SUM(CASE WHEN TRY_CAST(\"{field}\" AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) populated FROM src").fetchone()
        detail[field] = {"rows": int(row[0]), "populated_n": int(row[1] or 0), "status": "CAPTURE_LOST" if field == "fum" else "FUTURE_DETAIL_CANDIDATE"}
    return {"measurement_only": True, "source_table": "Opponents by Team", "layout": "player_splits_L4", "subject_plane": "sum 48 disjoint opponent-team rows to player-season", "source_rows_after_exact_dedup": int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]), "source_players": int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]), "source_split_values": int(con.execute("SELECT COUNT(DISTINCT split_value) FROM src").fetchone()[0]), "lost_columns": [{"column": str(a) if a is not None else None, "rows": int(b)} for a, b in loss], "candidate_matrix": candidates, "detail_fields": detail, "denominator_checks": _denominator(con, v26), "target_aggregation": "SUM weekly REG by (NFL_player_id, year); MAX for long", "no_state_change": True}


def main():
    result = measure()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
