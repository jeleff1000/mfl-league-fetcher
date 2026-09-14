"""Measure NFL.com QB Career mappings with operand-level rate witnesses."""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .nflcom_player_logs_weekly_witness import _passer_rating
from .sources import registry, v26_plane


OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master") / (
    "nflcom_player_career_qb_witness.json"
)
SOURCE = Path(registry(include_subject=False)["nflcom_player_career"].path)
SLUGS = Path(registry(include_subject=False)["nflcom_slug_pfrid"].path)
BIO = Path(registry(include_subject=False)["player_bio"].path)


def _q(value: str | Path) -> str:
    return str(value).replace("'", "''")


def close_value(left: float | None, right: float | None, tolerance: float = 0.11) -> bool:
    return left is not None and right is not None and abs(left - right) <= tolerance


def ratio(numerator: float | None, denominator: float | None) -> float | None:
    if numerator is None or denominator is None or denominator == 0:
        return None
    return numerator / denominator


def _count(con: duckdb.DuckDBPyConnection, source_field: str, canonical: str, v26: str) -> dict:
    sql = f"""WITH s AS (
        SELECT nflcom_slug, SUM(TRY_CAST("{source_field}" AS DOUBLE)) source_value
        FROM src WHERE TRY_CAST("{source_field}" AS DOUBLE) IS NOT NULL GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug, MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) target_value
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id
        WHERE v."{canonical}" IS NOT NULL GROUP BY 1
      ) SELECT s.source_value,v.target_value FROM s JOIN v USING(nflcom_slug)"""
    pairs = con.execute(sql).fetchall()
    informative = [(float(a), float(b)) for a, b in pairs if a is not None and b is not None and (a != 0 or b != 0)]
    return {
        "source_field": source_field, "canonical": canonical, "witness_form": "SUM",
        "paired_n": len(pairs), "informative_n": len(informative),
        "agree_n": sum(int(a == b) for a, b in informative),
        "agree_pct": round(100.0 * sum(int(a == b) for a, b in informative) / len(informative), 2) if informative else None,
        "source_exceeds_target": sum(int(a > b) for a, b in informative),
        "target_exceeds_source": sum(int(a < b) for a, b in informative),
    }


def _candidate_matrix(con: duckdb.DuckDBPyConnection, source_field: str, candidates: tuple[str, ...], v26: str) -> list[dict]:
    """Retain semantic alternatives when a natural fumble field is weak."""
    out = []
    for canonical in candidates:
        row = _count(con, source_field, canonical, v26)
        row["candidate_role"] = "natural" if canonical in {"fumbles", "fumbles_lost"} else "family contrast"
        out.append(row)
    return out


def _ratio_rate(con: duckdb.DuckDBPyConnection, source_num: str, source_den: str, canonical: str, v26_num: str, v26_den: str, v26: str) -> dict:
    sql = f"""WITH s AS (
        SELECT nflcom_slug,SUM(TRY_CAST("{source_num}" AS DOUBLE)) sn,SUM(TRY_CAST("{source_den}" AS DOUBLE)) sd,
               MAX(TRY_CAST("{source_num}" AS DOUBLE)/NULLIF(TRY_CAST("{source_den}" AS DOUBLE),0)) published
        FROM src GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug,MAX(TRY_CAST(v."{v26_num}" AS DOUBLE)) vn,MAX(TRY_CAST(v."{v26_den}" AS DOUBLE)) vd,
               MAX(TRY_CAST(v."{canonical}" AS DOUBLE)) stored_rate
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id GROUP BY 1
      ) SELECT s.sn,s.sd,s.published,v.vn,v.vd,v.stored_rate FROM s JOIN v USING(nflcom_slug)"""
    rows = con.execute(sql).fetchall()
    info = [(ratio(sn, sd), ratio(vn, vd), stored) for sn, sd, _, vn, vd, stored in rows if ratio(sn, sd) is not None or ratio(vn, vd) is not None]
    operand_pairs = [(s, t) for s, t, _ in info if s is not None and t is not None]
    stored_pairs = [(s, stored) for s, _, stored in info if s is not None and stored is not None]
    return {
        "source_fields": [source_num, source_den], "canonical": canonical,
        "v26_operands": [v26_num, v26_den], "witness_form": f"RECOMPUTE(SUM({source_num})/SUM({source_den}))",
        "paired_n": len(rows), "informative_n": len(info),
        "source_operands_vs_v26_operands_agree_n": sum(int(close_value(s, t)) for s, t in operand_pairs),
        "source_operands_vs_v26_operands_agree_pct": round(100.0 * sum(int(close_value(s, t)) for s, t in operand_pairs) / len(operand_pairs), 2) if operand_pairs else None,
        "source_operands_vs_v26_stored_rate_agree_n": sum(int(close_value(s, t)) for s, t in stored_pairs),
        "source_operands_vs_v26_stored_rate_agree_pct": round(100.0 * sum(int(close_value(s, t)) for s, t in stored_pairs) / len(info), 2) if info else None,
        "v26_stored_rate_status": "contrast_only; operand recomputation is the desired witness",
        "rate_denominator": "player-level numerator and denominator sums; zero/zero omitted",
    }


def _passer_rate(con: duckdb.DuckDBPyConnection, v26: str) -> dict:
    sql = f"""WITH s AS (
        SELECT nflcom_slug,SUM(TRY_CAST(comp AS DOUBLE)) comp,SUM(TRY_CAST(att AS DOUBLE)) att,
          SUM(TRY_CAST(yds AS DOUBLE)) yds,SUM(TRY_CAST(td AS DOUBLE)) td,SUM(TRY_CAST(int AS DOUBLE)) ints,
          MAX(TRY_CAST(rate AS DOUBLE)) published FROM src GROUP BY 1
      ), v AS (
        SELECT i.nflcom_slug,MAX(completions) comp,MAX(attempts) att,MAX(passing_yards) yds,
          MAX(passing_tds) td,MAX(passing_interceptions) ints,MAX(passer_rating) stored_rate
        FROM ids i JOIN read_parquet('{v26}') v ON v.NFL_player_id=i.NFL_player_id GROUP BY 1
      ) SELECT s.comp,s.att,s.yds,s.td,s.ints,s.published,v.comp,v.att,v.yds,v.td,v.ints,v.stored_rate FROM s JOIN v USING(nflcom_slug)"""
    rows = con.execute(sql).fetchall(); info=[]
    for sc,sa,sy,st,si,pub,vc,va,vy,vt,vi,stored in rows:
        sr=_passer_rating(sc,sa,sy,st,si) if None not in (sc,sa,sy,st,si) and sa else None
        vr=_passer_rating(vc,va,vy,vt,vi) if None not in (vc,va,vy,vt,vi) and va else None
        if sr is not None or vr is not None: info.append((sr,vr,stored))
    ops=[(s,t) for s,t,_ in info if s is not None and t is not None]; stored=[(s,t) for s,_,t in info if s is not None and t is not None]
    return {
        "source_field":"rate","canonical":"passer_rating","witness_form":"RECOMPUTE passer_rating from five operands",
        "informative_n":len(info),"source_recomputed_vs_v26_recomputed_agree_n":sum(int(close_value(s,t)) for s,t in ops),
        "source_recomputed_vs_v26_recomputed_agree_pct":round(100*sum(int(close_value(s,t)) for s,t in ops)/len(ops),2) if ops else None,
        "source_recomputed_vs_v26_stored_agree_n":sum(int(close_value(s,t)) for s,t in stored),
        "source_recomputed_vs_v26_stored_agree_pct":round(100*sum(int(close_value(s,t)) for s,t in stored)/len(info),2) if info else None,
        "rate_denominator":"player-level comp, att, yds, td, int sums; att=0 omitted",
    }


def measure() -> dict:
    con=duckdb.connect(); src_glob=_q(SOURCE/"**/*.parquet")
    con.execute(f"""CREATE TEMP TABLE src AS SELECT DISTINCT nflcom_slug,season,team,g,gs,att,comp,yds,avg,td,int,rate,
      att_2,yds_2,avg_2,td_2,fum,lost,sck,scky FROM read_parquet('{src_glob}',union_by_name=true) WHERE _table='QB Career'""")
    con.execute(f"""CREATE TEMP TABLE ids AS SELECT DISTINCT x.nflcom_slug,b.NFL_player_id FROM read_parquet('{_q(SLUGS)}') x JOIN read_parquet('{_q(BIO)}') b ON b.pfr_id=x.pfr_id""")
    v26=_q(v26_plane('career'))
    counts=[
      _count(con,'g','games_played',v26),_count(con,'gs','games_started',v26),_count(con,'att','attempts',v26),
      _count(con,'att_2','carries',v26),_count(con,'comp','completions',v26),_count(con,'fum','fumbles',v26),
      _count(con,'int','passing_interceptions',v26),_count(con,'lost','fumbles_lost',v26),_count(con,'sck','sacks_suffered',v26),
      _count(con,'scky','sack_yards_lost',v26),_count(con,'td','passing_tds',v26),_count(con,'td_2','rushing_tds',v26),
      _count(con,'yds','passing_yards',v26),_count(con,'yds_2','rushing_yards',v26)]
    rates=[_ratio_rate(con,'yds','att','passing_yards_per_attempt','passing_yards','attempts',v26),_ratio_rate(con,'yds_2','att_2','rushing_yards_per_carry','rushing_yards','carries',v26),_passer_rate(con,v26)]
    return {'measurement_only':True,'source_table':'QB Career','subject_plane':'player_nfl_career','source_deduplication':'SELECT DISTINCT including season/team before aggregation','source_rows_after_exact_dedup':int(con.execute('SELECT COUNT(*) FROM src').fetchone()[0]),'source_players':int(con.execute('SELECT COUNT(DISTINCT nflcom_slug) FROM src').fetchone()[0]),'counts':counts,'rates':rates,'fumble_contrasts':{'fum':_candidate_matrix(con,'fum',('fumbles','rushing_fumbles','receiving_fumbles','sack_fumbles','fumbles_lost'),v26),'lost':_candidate_matrix(con,'lost',('fumbles_lost','rushing_fumbles_lost','receiving_fumbles_lost','sack_fumbles_lost'),v26)},'no_state_change':True}


def main():
    result=measure(); OUT.parent.mkdir(parents=True,exist_ok=True); OUT.write_text(json.dumps(result,indent=2,sort_keys=True)+'\n',encoding='utf-8'); print(json.dumps(result,indent=2,sort_keys=True))


if __name__=='__main__': main()
