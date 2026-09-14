"""Read-only 2025 receipts for PFR regular/postseason player punting tables."""
from __future__ import annotations
import argparse, json, math
from datetime import datetime, timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions

ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables")
RELEASE=Path(r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet")
SURFACES={"regular":("punting","pfr_player_punting","REG"),"post":("punting_post","pfr_punting_post","POST")}
DIRECT={"games":"games_played","punt":"punts","punt_yds":"punt_yards","punt_long":"punt_long","punt_blocked":"punts_blocked"}
RATES={"punt_yds_per_punt":("punt_yards","punts",1.0)}
CONTEXT={"pfr_id","player","index_letter","index_position","first_year","last_year","page_key","page_kind","page_url","subpage_year","scraped_at_utc","source_url","table_id","table_caption","row_index_in_table","tr_data_row","year_id","age","team_name_abbr","comp_name_abbr","pos","NFL_player_id","awards","awards_links_json","awards_link_texts","awards_link_ids","awards_urls"}
def num(v):
    try:
        return None if v is None or (isinstance(v,float) and math.isnan(v)) else float(v)
    except (TypeError,ValueError): return None
def eq(a,b): return a is not None and b is not None and abs(a-b)<=0.06
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--surface',choices=sorted(SURFACES),default='regular'); a=ap.parse_args()
    table,source,stype=SURFACES[a.surface]; c=duckdb.connect(); pfr=str(ROOT/table/'_combined.parquet').replace('\\','/'); rel=str(RELEASE).replace('\\','/')
    rows=c.execute("""SELECT * FROM read_parquet(?) WHERE year_id='2025'
      QUALIFY row_number() OVER (PARTITION BY pfr_id,year_id ORDER BY CASE WHEN team_name_abbr='2TM' THEN 0 ELSE 1 END,team_name_abbr)=1""",[pfr]).fetchdf().to_dict('records')
    v=c.execute(f"""SELECT lower(trim(player)) k,COUNT(DISTINCT week) games_played,SUM(punts) punts,SUM(punt_yards) punt_yards,MAX(punt_long) punt_long,SUM(punts_blocked) punts_blocked FROM read_parquet(?) WHERE year=2025 AND season_type='{stype}' GROUP BY 1""",[rel]).fetchdf().to_dict('records'); vm={x['k']:x for x in v}; decisions=load_decisions(); cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[pfr]).fetchdf()['column_name'])
    checks={}; mismatch_examples={}; matrix=[]
    for col in cols:
        if col in CONTEXT: audit='CONTEXT_OR_PROVENANCE'
        elif col in RATES: audit='DERIVED_WITNESS'
        elif col in DIRECT: audit='DIRECT_COUNTER'
        elif col in {'punt_net_yds','punt_net_yds_per_punt','punt_ret_yds_opp','punt_tb','punt_tb_pct','punt_in_20','punt_in_20_pct'}: audit='PROMOTION_CANDIDATE_OR_STRUCTURED_WITNESS'
        elif col=='av': audit='PROMOTION_CANDIDATE'
        else: audit='STRUCTURED_OR_EXCLUDED'
        comp=match=0; examples=[]
        if col in DIRECT or col in RATES:
            for r in rows:
                vv=vm.get(str(r.get('player') or '').strip().lower()); p=num(r.get(col));
                if not vv or p is None: continue
                if col in DIRECT: q=num(vv.get(DIRECT[col]))
                else:
                    n=num(vv.get(RATES[col][0])); d=num(vv.get(RATES[col][1])); q=None if n is None or not d else n/d*RATES[col][2]
                if q is None: continue
                comp+=1
                if eq(p,q): match+=1
                elif len(examples)<5: examples.append({'player':r.get('player'),'pfr':p,'weekly':q})
        checks[col]={'comparable':comp,'matches':match,'mismatches':comp-match}
        if examples: mismatch_examples[col]=examples
        key=f'{source}|*|{col}'; d=decisions.get(key,{})
        matrix.append({'source':source,'table_key':'*','column':col,'disposition':d.get('disposition'),'canonical':d.get('canonical'),'audit_disposition':audit,'checks':checks[col]})
    out=Path(r'D:\yahoo_oauth\docs\audits')/f'pfr-{source}-2025.json'; payload={'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':source,'surface':a.surface,'year':2025,'selected_rows':len(rows),'columns':len(cols),'checks':checks,'mismatch_examples':mismatch_examples,'matrix':matrix,'open_blocked':0,'notes':['All source columns classified; no release backfill performed.','PFR net-yard and opponent-return-yard fields have no equivalent weekly counters and remain explicit candidates/structured witnesses.']}; out.write_text(json.dumps(payload,indent=2,default=str),encoding='utf-8'); print(json.dumps({'written':str(out),'rows':len(rows),'columns':len(cols),'open_blocked':0},indent=2))
if __name__=='__main__': main()
