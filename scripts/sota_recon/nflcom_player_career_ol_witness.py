"""Measure the two NFL.com Offensive Line Career games fields."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import registry, v26_plane


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / (
    "nflcom_player_career_ol_witness.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_career"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def measure() -> dict:
    con = duckdb.connect()
    con.execute(f"""CREATE TEMP TABLE src AS
        SELECT DISTINCT nflcom_slug, season, team, g, gs
        FROM read_parquet('{_q(SOURCE)}/**/*.parquet', union_by_name=true)
        WHERE _table='Offensive Line Career'""")
    con.execute(f"""CREATE TEMP TABLE ids AS
        SELECT DISTINCT x.nflcom_slug, b.NFL_player_id
        FROM read_parquet('{_q(SLUGS)}') x
        JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26 = _q(v26_plane("career"))
    rows = []
    for source_field, canonical in (("g", "games_played"), ("gs", "games_started")):
        sql = f"""WITH s AS (
            SELECT nflcom_slug, SUM(TRY_CAST("{source_field}" AS DOUBLE)) source_value
            FROM src WHERE TRY_CAST("{source_field}" AS DOUBLE) IS NOT NULL GROUP BY 1
          ), v AS (
            SELECT i.nflcom_slug, MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) target_value
            FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
            WHERE v."{canonical}" IS NOT NULL GROUP BY 1
          ) SELECT s.source_value,v.target_value FROM s JOIN v USING(nflcom_slug)"""
        pairs = con.execute(sql).fetchall()
        info = [(float(a), float(b)) for a,b in pairs if a is not None and b is not None and (a != 0 or b != 0)]
        agree = sum(int(a == b) for a,b in info)
        rows.append({"source_field":source_field,"canonical":canonical,"witness_form":"SUM","paired_n":len(pairs),"informative_n":len(info),"agree_n":agree,"agree_pct":round(100*agree/len(info),2) if info else None,"source_exceeds_target":sum(int(a>b) for a,b in info),"target_exceeds_source":sum(int(a<b) for a,b in info)})
    return {"measurement_only":True,"source_table":"Offensive Line Career","subject_plane":"player_nfl_career","source_deduplication":"SELECT DISTINCT including season/team before SUM","source_rows_after_exact_dedup":int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),"source_players":int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]),"fields":rows,"interpretation":"These are position-table games fields; a low comparison to generic player_nfl_career games is not a column swap proof.","no_state_change":True}


def main():
    result=measure(); OUT.parent.mkdir(parents=True,exist_ok=True); OUT.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n',encoding='utf-8'); print(json.dumps(result,indent=2,sort_keys=True))


if __name__=='__main__': main()
