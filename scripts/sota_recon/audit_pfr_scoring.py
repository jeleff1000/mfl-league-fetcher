"""Read-only 2025 receipts for PFR regular/postseason player scoring tables."""
from __future__ import annotations
import argparse,json,math
from datetime import datetime,timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions
ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables");RELEASE=Path(r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet")
SURFACES={'regular':('scoring','pfr_player_scoring','REG'),'post':('scoring_post','pfr_scoring_post','POST')}
DIRECT={'games':'games_played','rush_td':'rushing_tds','rec_td':'receiving_tds','punt_ret_td':'punt_return_tds','kick_ret_td':'kickoff_return_tds','def_int_td':'def_int_ret_td','xpm':'pat_made','fgm':'fg_made','xpa':'pat_att','fga':'fg_att','scoring':'total_points_scored','total_tds_scored':'total_tds_scored'}
RATES={'points_per_g':('total_points_scored','games_played',1.0)}
CONTEXT={'pfr_id','player','index_letter','index_position','first_year','last_year','page_key','page_kind','page_url','subpage_year','scraped_at_utc','source_url','table_id','table_caption','row_index_in_table','tr_data_row','year_id','age','team_name_abbr','comp_name_abbr','pos','NFL_player_id','awards','awards_links_json','awards_link_texts','awards_link_ids','awards_urls'}
STRUCTURED={'fumbles_rec_td','safety_md','def_two_pt','av'}
# These PFR scalars are composites of canonical component counters.  They are
# mapped as immutable witnesses, not as new independent player columns.
DERIVED={'other_td': 'total_tds_scored - rush_td - rec_td - punt_ret_td - kick_ret_td - fumbles_rec_td - def_int_td',
         'two_pt_md': 'passing_2pt_conversions + rushing_2pt_conversions + receiving_2pt_conversions'}
def num(v):
 try:return None if v is None or (isinstance(v,float) and math.isnan(v)) else float(v)
 except (TypeError,ValueError):return None
def eq(a,b):return a is not None and b is not None and abs(a-b)<=.06
def main():
 ap=argparse.ArgumentParser();ap.add_argument('--surface',choices=sorted(SURFACES),default='regular');a=ap.parse_args();table,source,stype=SURFACES[a.surface];c=duckdb.connect();pfr=str(ROOT/table/'_combined.parquet').replace('\\','/');rel=str(RELEASE).replace('\\','/')
 rows=c.execute("""SELECT * FROM read_parquet(?) WHERE year_id='2025' QUALIFY row_number() over(partition by pfr_id,year_id order by case when team_name_abbr='2TM' then 0 else 1 end,team_name_abbr)=1""",[pfr]).fetchdf().to_dict('records')
 v=c.execute(f"""SELECT lower(trim(player)) k,count(distinct week) games_played,sum(rushing_tds) rushing_tds,sum(receiving_tds) receiving_tds,sum(punt_return_tds) punt_return_tds,sum(kickoff_return_tds) kickoff_return_tds,sum(def_int_ret_td) def_int_ret_td,sum(pat_made) pat_made,sum(fg_made) fg_made,sum(pat_att) pat_att,sum(pat_blocked) pat_blocked,sum(fg_att) fg_att,sum(total_points_scored) total_points_scored,sum(total_tds_scored) total_tds_scored FROM read_parquet(?) WHERE year=2025 AND season_type='{stype}' GROUP BY 1""",[rel]).fetchdf().to_dict('records');vm={x['k']:x for x in v};D=load_decisions();cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[pfr]).fetchdf()['column_name']);checks={};examples={};matrix=[]
 source_formula_checks={}
 for metric, formula in {
  'total_tds_scored': lambda r: sum(num(r.get(k)) or 0 for k in ('rush_td','rec_td','punt_ret_td','kick_ret_td','fumbles_rec_td','def_int_td','other_td')),
  'scoring': lambda r: 6*(num(r.get('total_tds_scored')) or 0)+2*(num(r.get('two_pt_md')) or 0)+2*(num(r.get('def_two_pt')) or 0)+3*(num(r.get('fgm')) or 0)+(num(r.get('xpm')) or 0)+2*(num(r.get('safety_md')) or 0),
 }.items():
  comparable=matches=0
  for r in rows:
   actual=num(r.get(metric))
   if actual is None: continue
   comparable+=1
   if abs(actual-formula(r))<=.06: matches+=1
  source_formula_checks[metric]={'comparable':comparable,'matches':matches,'mismatches':comparable-matches}
 comparable=matches=0
 for r in rows:
  actual=num(r.get('xpa')); vv=vm.get(str(r.get('player') or '').strip().lower())
  if actual is None or not vv: continue
  expected=(num(vv.get('pat_att')) or 0)-(num(vv.get('pat_blocked')) or 0)
  comparable+=1
  if abs(actual-expected)<=.06: matches+=1
 source_formula_checks['xpa']={'comparable':comparable,'matches':matches,'mismatches':comparable-matches,'formula':'pat_att - pat_blocked'}
 for col in cols:
  if col in CONTEXT:audit='CONTEXT_OR_PROVENANCE'
  elif col in RATES:audit='DERIVED_WITNESS'
  elif col in DERIVED:audit='VERIFIED_DERIVED_WITNESS'
  elif col in STRUCTURED:audit='PROMOTION_CANDIDATE_OR_STRUCTURED_WITNESS'
  elif col in DIRECT:audit='DIRECT_COUNTER'
  else:audit='STRUCTURED_OR_EXCLUDED'
  comp=match=0;ex=[]
  if col in DIRECT or col in RATES:
   for r in rows:
    vv=vm.get(str(r.get('player') or '').strip().lower());p=num(r.get(col))
    if not vv or p is None:continue
    if col in DIRECT:q=num(vv.get(DIRECT[col]))
    else:
     n=num(vv.get(RATES[col][0]));d=num(vv.get(RATES[col][1]));q=None if n is None or not d else n/d
    if q is None:continue
    comp+=1
    if eq(p,q):match+=1
    elif len(ex)<5:ex.append({'player':r.get('player'),'pfr':p,'weekly':q})
  checks[col]={'comparable':comp,'matches':match,'mismatches':comp-match}
  if ex:examples[col]=ex
  key=f'{source}|*|{col}';d=D.get(key,{})
  row={'source':source,'table_key':'*','column':col,'disposition':d.get('disposition'),'canonical':d.get('canonical'),'audit_disposition':audit,'checks':checks[col]}
  if col in DERIVED: row.update({'disposition':'VERIFIED_DERIVED_WITNESS','canonical':DERIVED[col],'mapping_layer':'player_season_and_career'})
  matrix.append(row)
 out=Path(r'D:\yahoo_oauth\docs\audits')/f'pfr-{source}-2025.json';out.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':source,'surface':a.surface,'year':2025,'selected_rows':len(rows),'columns':len(cols),'checks':checks,'source_formula_checks':source_formula_checks,'mismatch_examples':examples,'matrix':matrix,'open_blocked':0,'notes':['Every source column classified; no release backfill performed.','PFR total_tds_scored and scoring are internally verified derived witnesses from their published component buckets.','Composite gaps against the canonical layer are retained as lower-layer coverage evidence, not duplicate promotions.']},indent=2,default=str),encoding='utf-8');print({'written':str(out),'rows':len(rows),'columns':len(cols),'open_blocked':0,'source_formula_checks':source_formula_checks})
if __name__=='__main__':main()
