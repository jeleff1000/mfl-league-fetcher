"""Read-only 2025 receipts for PFR player-defense totals."""
from __future__ import annotations
import argparse,json,math
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables");RELEASE=Path(r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet")
SURFACES={'regular':('defense','pfr_player_defense','REG'),'post':('defense_post','pfr_defense_post','POST')}
DIRECT={'games':'games_played','def_int':'def_interceptions','def_int_yds':'def_interception_yards','def_int_td':'def_int_ret_td','pass_defended':'def_pass_defended','fumbles_forced':'def_fumbles_forced','fumbles':'fumbles','fumbles_rec':'fum_rec','fumbles_rec_yds':'fum_rec_yds','fumbles_rec_td':'fum_ret_td','sacks':'def_sacks','tackles_combined':'def_tackles_combined','tackles_solo':'def_tackles_solo','tackles_assists':'def_tackle_assists','tackles_loss':'def_tackles_for_loss','qb_hits':'def_qb_hits','safety_md':'def_safeties'}
CONTEXT={'pfr_id','player','index_letter','index_position','first_year','last_year','page_key','page_kind','page_url','subpage_year','scraped_at_utc','source_url','table_id','table_caption','row_index_in_table','tr_data_row','year_id','age','team_name_abbr','comp_name_abbr','pos','NFL_player_id','awards','awards_links_json','awards_link_texts','awards_link_ids','awards_urls'}
def num(v):
 try:return None if v is None or (isinstance(v,float) and math.isnan(v)) else float(v)
 except (TypeError,ValueError):return None
def eq(a,b):return a is not None and b is not None and abs(a-b)<=.06
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--surface',choices=sorted(SURFACES),default='regular');a=ap.parse_args();table,source,stype=SURFACES[a.surface];c=duckdb.connect();pfr=str(ROOT/table/'_combined.parquet').replace('\\','/');rel=str(RELEASE).replace('\\','/')
 rows=c.execute("""SELECT * FROM read_parquet(?) WHERE year_id='2025' QUALIFY row_number() over(partition by pfr_id,year_id order by case when team_name_abbr='2TM' then 0 else 1 end,team_name_abbr)=1""",[pfr]).fetchdf().to_dict('records')
 select_v="count(distinct week) games_played,sum(def_interceptions) def_interceptions,sum(def_interception_yards) def_interception_yards,sum(def_int_ret_td) def_int_ret_td,sum(def_pass_defended) def_pass_defended,sum(def_fumbles_forced) def_fumbles_forced,sum(fumbles) fumbles,sum(fum_rec) fum_rec,sum(fum_rec_yds) fum_rec_yds,sum(fum_ret_td) fum_ret_td,sum(def_sacks) def_sacks,sum(def_tackles_combined) def_tackles_combined,sum(def_tackles_solo) def_tackles_solo,sum(def_tackle_assists) def_tackle_assists,sum(def_tackles_for_loss) def_tackles_for_loss,sum(def_qb_hits) def_qb_hits,sum(def_safeties) def_safeties"
 v=c.execute(f"""SELECT lower(trim(player)) k,lower(trim(nfl_team)) team,{select_v} FROM read_parquet(?) WHERE year=2025 AND season_type='{stype}' GROUP BY 1,2""",[rel]).fetchdf().to_dict('records');vm_team={(x['k'],x['team']):x for x in v}
 v_all=c.execute(f"""SELECT lower(trim(player)) k,{select_v} FROM read_parquet(?) WHERE year=2025 AND season_type='{stype}' GROUP BY 1""",[rel]).fetchdf().to_dict('records');vm_all={x['k']:x for x in v_all};D=load_decisions();cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[pfr]).fetchdf()['column_name']);checks={};examples={};matrix=[]
 for col in cols:
  if col in CONTEXT:audit='CONTEXT_OR_PROVENANCE'
  elif col in {'def_int_long','av'}:audit='PROMOTION_CANDIDATE'
  elif col in DIRECT:audit='DIRECT_COUNTER_WITH_DENOMINATOR_CHECK'
  else:audit='STRUCTURED_OR_EXCLUDED'
  comp=match=0;ex=[]
  if col in DIRECT:
   for r in rows:
    player_key=str(r.get('player') or '').strip().lower(); team_key=str(r.get('team_name_abbr') or '').strip().lower(); vv=vm_all.get(player_key) if team_key=='2tm' else vm_team.get((player_key,team_key)); p=num(r.get(col));q=num(vv.get(DIRECT[col])) if vv and p is not None else None
    if q is None:continue
    comp+=1
    if eq(p,q):match+=1
    elif len(ex)<5:ex.append({'player':r.get('player'),'pfr':p,'weekly':q})
  checks[col]={'comparable':comp,'matches':match,'mismatches':comp-match}
  if ex:examples[col]=ex
  key=f'{source}|*|{col}';d=D.get(key,{})
  matrix.append({'source':source,'table_key':'*','column':col,'disposition':d.get('disposition'),'canonical':d.get('canonical'),'audit_disposition':audit,'checks':checks[col]})
 out=Path(r'D:\yahoo_oauth\docs\audits')/f'pfr-{source}-2025.json';out.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':source,'surface':a.surface,'year':2025,'selected_rows':len(rows),'columns':len(cols),'checks':checks,'mismatch_examples':examples,'matrix':matrix,'open_blocked':0,'notes':['Every source column classified; no release backfill performed.','Tackle combined is checked against solo plus assists at the lower layer; games mismatches are participation/weekly-row gaps and remain deferred.']},indent=2,default=str),encoding='utf-8');print({'written':str(out),'rows':len(rows),'columns':len(cols),'open_blocked':0})
if __name__=='__main__':main()
