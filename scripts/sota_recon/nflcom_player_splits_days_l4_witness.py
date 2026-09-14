"""Measure combined kickoff/punt return fields in Days/L4 player splits."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import latest_v26, registry


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / "nflcom_player_splits_days_l4_witness.json"
SOURCE = Path(registry(include_subject=False)["nflcom_player_splits"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def _compare(pairs: list[tuple[float | None, float | None]]) -> dict:
    info=[(float(a),float(b)) for a,b in pairs if a is not None and b is not None and (a or b)];agree=sum(int(a==b) for a,b in info)
    return {"joined_n":len(pairs),"informative_n":len(info),"agree_n":agree,"agree_pct":round(100*agree/len(info),2) if info else None,"source_exceeds_target":sum(int(a>b) for a,b in info),"target_exceeds_source":sum(int(a<b) for a,b in info)}


def _measure(con, source_field: str, target_expr: str, witness_form: str, v26: str) -> dict:
    sql=f"""WITH s AS (SELECT nflcom_slug,TRY_CAST(season AS INTEGER) season_year,{('MAX' if source_field=='lng' else 'SUM')}(TRY_CAST(\"{source_field}\" AS DOUBLE)) source_value FROM src WHERE TRY_CAST(\"{source_field}\" AS DOUBLE) IS NOT NULL GROUP BY 1,2),v AS (SELECT i.nflcom_slug,v.year season_year,{target_expr} target_value FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id WHERE v.season_type='REG' GROUP BY 1,2) SELECT s.source_value,v.target_value FROM s JOIN v USING(nflcom_slug,season_year)"""
    return {"source_field":source_field,"target_expression":target_expr,"witness_form":witness_form,**_compare(con.execute(sql).fetchall())}


def measure() -> dict:
    con=duckdb.connect();source=_q(SOURCE/"**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE src AS SELECT DISTINCT nflcom_slug,season,split_value,_lost_column,ret,yds,td,lng,fum,fc,20,40 FROM read_parquet('{source}',union_by_name=true) WHERE _table='Days' AND _layout='player_splits_L4'""")
    con.execute(f"""CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26=_q(latest_v26())
    rows={
      "ret": [_measure(con,"ret","SUM(v.kickoff_returns)+SUM(v.punt_returns)","SUM kickoffs + punts",v26),_measure(con,"ret","SUM(v.kickoff_returns)","SUM kickoff only",v26),_measure(con,"ret","SUM(v.punt_returns)","SUM punt only",v26)],
      "yds": [_measure(con,"yds","SUM(v.total_return_yards)","SUM total_return_yards",v26),_measure(con,"yds","SUM(v.kickoff_return_yards)+SUM(v.punt_return_yards)","SUM kickoff + punt yards",v26)],
      "td": [_measure(con,"td","SUM(v.kickoff_return_tds)+SUM(v.punt_return_tds)","SUM kickoff + punt TDs",v26),_measure(con,"td","SUM(v.kickoff_return_tds)","SUM kickoff TDs",v26),_measure(con,"td","SUM(v.punt_return_tds)","SUM punt TDs",v26)],
      "lng": [_measure(con,"lng","GREATEST(MAX(v.kickoff_return_long),MAX(v.punt_return_long))","MAX kickoffs/punts",v26),_measure(con,"lng","MAX(v.kickoff_return_long)","MAX kickoff only",v26),_measure(con,"lng","MAX(v.punt_return_long)","MAX punt only",v26)],
    }
    loss=con.execute("SELECT _lost_column,COUNT(*) n FROM src GROUP BY 1 ORDER BY 1").fetchall()
    detail={}
    for field in ('fum','fc','20','40'):
        row=con.execute(f"SELECT COUNT(*) row_n,SUM(CASE WHEN TRY_CAST(\"{field}\" AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END) populated FROM src").fetchone();detail[field]={"rows":int(row[0]),"populated_n":int(row[1] or 0),"status":"CAPTURE_LOST" if field=='fum' else "FUTURE_DETAIL_CANDIDATE"}
    return {"measurement_only":True,"source_table":"Days","layout":"player_splits_L4","subject_plane":"weekly summed to player-season","source_rows_after_exact_dedup":int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),"source_players":int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]),"lost_columns":[{"column":str(a) if a is not None else None,"rows":int(b)} for a,b in loss],"candidate_matrix":rows,"detail_fields":detail,"target_aggregation":"SUM weekly REG; MAX for long","no_state_change":True}


def main():
    result=measure();OUT.parent.mkdir(parents=True,exist_ok=True);OUT.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n',encoding='utf-8');print(json.dumps(result,indent=2,sort_keys=True))


if __name__=='__main__':main()
