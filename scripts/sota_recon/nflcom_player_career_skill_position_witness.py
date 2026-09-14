"""Measure NFL.com RBFB and WRTE Career witnesses.

RBFB and WRTE share the capture layout, but the unnumbered and ``*_2`` fields
reverse their meaning.  The table specification below is therefore explicit and
never inferred from the canonical column name.  Rates use player-level operand
ratios; longest fields use MAX; all other mapped fields use SUM.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .sources import registry, v26_plane


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / (
    "nflcom_player_career_skill_position_witness.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_career"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)

TABLES = {
    "RBFB Career": {
        "counts": {
            "g": ("games_played", "SUM"), "gs": ("games_started", "SUM"),
            "att": ("carries", "SUM"), "rec": ("receptions", "SUM"),
            "yds": ("rushing_yards", "SUM"), "yds_2": ("receiving_yards", "SUM"),
            "td": ("rushing_tds", "SUM"), "td_2": ("receiving_tds", "SUM"),
            "lng": ("receiving_long", "MAX"), "fum": ("fumbles", "SUM"),
            "lost": ("fumbles_lost", "SUM"),
        },
        "rates": [("avg", "yds", "att", "rushing_yards_per_carry", "rushing_yards", "carries"),
                   ("avg_2", "yds_2", "rec", "receiving_yards_per_reception", "receiving_yards", "receptions")],
    },
    "WRTE Career": {
        "counts": {
            "g": ("games_played", "SUM"), "gs": ("games_started", "SUM"),
            "att": ("carries", "SUM"), "rec": ("receptions", "SUM"),
            "yds": ("receiving_yards", "SUM"), "yds_2": ("rushing_yards", "SUM"),
            "td": ("receiving_tds", "SUM"), "td_2": ("rushing_tds", "SUM"),
            "lng": ("receiving_long", "MAX"), "lng_2": ("rushing_long", "MAX"),
            "fum": ("fumbles", "SUM"), "lost": ("fumbles_lost", "SUM"),
        },
        "rates": [("avg", "yds", "rec", "receiving_yards_per_reception", "receiving_yards", "receptions"),
                   ("avg_2", "yds_2", "att", "rushing_yards_per_carry", "rushing_yards", "carries")],
    },
}


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def close_value(left: float | None, right: float | None, tolerance: float = 0.11) -> bool:
    return left is not None and right is not None and abs(left - right) <= tolerance


def ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _count(con: duckdb.DuckDBPyConnection, source_field: str, canonical: str, form: str, v26: str) -> dict:
    aggregate = "MAX" if form == "MAX" else "SUM"
    sql = f"""WITH s AS (
        SELECT nflcom_slug,{aggregate}(TRY_CAST("{source_field}" AS DOUBLE)) source_value
        FROM src WHERE TRY_CAST("{source_field}" AS DOUBLE) IS NOT NULL GROUP BY 1
      ),v AS (
        SELECT i.nflcom_slug,MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) target_value
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        WHERE v."{canonical}" IS NOT NULL GROUP BY 1
      ) SELECT s.source_value,v.target_value FROM s JOIN v USING(nflcom_slug)"""
    pairs = con.execute(sql).fetchall()
    info = [(float(a), float(b)) for a,b in pairs if a is not None and b is not None and (a != 0 or b != 0)]
    agree = sum(int(a == b) for a,b in info)
    return {"source_field":source_field,"canonical":canonical,"witness_form":form,"paired_n":len(pairs),"informative_n":len(info),"agree_n":agree,"agree_pct":round(100*agree/len(info),2) if info else None,"source_exceeds_target":sum(int(a>b) for a,b in info),"target_exceeds_source":sum(int(a<b) for a,b in info)}


def _rate(con: duckdb.DuckDBPyConnection, spec: tuple[str,str,str,str,str,str], v26: str) -> dict:
    source_rate, source_num, source_den, canonical, v26_num, v26_den = spec
    sql = f"""WITH s AS (
        SELECT nflcom_slug,SUM(TRY_CAST("{source_num}" AS DOUBLE)) sn,SUM(TRY_CAST("{source_den}" AS DOUBLE)) sd
        FROM src GROUP BY 1
      ),v AS (
        SELECT i.nflcom_slug,MAX(TRY_CAST(v."{v26_num}" AS DOUBLE)) vn,MAX(TRY_CAST(v."{v26_den}" AS DOUBLE)) vd,
          MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) stored_rate
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id GROUP BY 1
      ) SELECT s.sn,s.sd,v.vn,v.vd,v.stored_rate FROM s JOIN v USING(nflcom_slug)"""
    rows = con.execute(sql).fetchall(); info=[]
    for sn,sd,vn,vd,stored in rows:
        source_ratio=ratio(sn,sd); target_ratio=ratio(vn,vd)
        if source_ratio is not None or target_ratio is not None: info.append((source_ratio,target_ratio,stored))
    operand=[(s,t) for s,t,_ in info if s is not None and t is not None]; stored=[(s,t) for s,_,t in info if s is not None and t is not None]
    return {"source_field":source_rate,"source_operands":[source_num,source_den],"canonical":canonical,"v26_operands":[v26_num,v26_den],"witness_form":f"RECOMPUTE(SUM({source_num})/SUM({source_den}))","informative_n":len(info),"source_operands_vs_v26_operands_agree_n":sum(int(close_value(s,t)) for s,t in operand),"source_operands_vs_v26_operands_agree_pct":round(100*sum(int(close_value(s,t)) for s,t in operand)/len(operand),2) if operand else None,"source_operands_vs_v26_stored_rate_agree_n":sum(int(close_value(s,t)) for s,t in stored),"source_operands_vs_v26_stored_rate_agree_pct":round(100*sum(int(close_value(s,t)) for s,t in stored)/len(info),2) if info else None,"stored_target_status":"contrast-only; operand recomputation is the desired witness","rate_denominator":"player-level numerator and denominator sums; zero/zero omitted"}


def _fumble_contrasts(con: duckdb.DuckDBPyConnection, source_field: str, natural: str, alternatives: tuple[str,...], v26: str) -> list[dict]:
    return [{**_count(con,source_field,canonical,"SUM",v26),"candidate_role":"natural" if canonical == natural else "family contrast"} for canonical in (natural,)+alternatives]


def measure_table(table: str, spec: dict) -> dict:
    con=duckdb.connect(); src_glob=_q(SOURCE/"**/*.parquet")
    fields=set(spec["counts"])
    for rate in spec["rates"]: fields.update(rate[:3])
    fields.update(("season","team","nflcom_slug"))
    select=", ".join(fields)
    con.execute(f"""CREATE TEMP TABLE src AS SELECT DISTINCT {select} FROM read_parquet('{src_glob}',union_by_name=true) WHERE _table='{table}'""")
    con.execute(f"""CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26=_q(v26_plane("career"))
    counts=[_count(con,source,canonical,form,v26) for source,(canonical,form) in spec["counts"].items()]
    rates=[_rate(con,rate,v26) for rate in spec["rates"]]
    return {"measurement_only":True,"source_table":table,"subject_plane":"player_nfl_career","source_deduplication":"SELECT DISTINCT including season/team before aggregation","source_rows_after_exact_dedup":int(con.execute("SELECT COUNT(*) FROM src").fetchone()[0]),"source_players":int(con.execute("SELECT COUNT(DISTINCT nflcom_slug) FROM src").fetchone()[0]),"counts":counts,"rates":rates,"fumble_contrasts":{"fum":_fumble_contrasts(con,"fum","fumbles",("rushing_fumbles","receiving_fumbles","sack_fumbles","fumbles_lost"),v26),"lost":_fumble_contrasts(con,"lost","fumbles_lost",("rushing_fumbles_lost","receiving_fumbles_lost","sack_fumbles_lost"),v26)},"no_state_change":True}


def measure() -> dict:
    return {table:measure_table(table,spec) for table,spec in TABLES.items()}


def main():
    result=measure(); OUT.parent.mkdir(parents=True,exist_ok=True); OUT.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n',encoding='utf-8'); print(json.dumps(result,indent=2,sort_keys=True))


if __name__=='__main__': main()
