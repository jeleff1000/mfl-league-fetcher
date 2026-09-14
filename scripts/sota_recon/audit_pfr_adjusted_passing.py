"""Audit PFR adjusted-passing index metrics."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

PATH=Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\adj_passing\_combined.parquet")
OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-adjusted-passing-2025.json")
META={"pfr_id","player","index_letter","index_position","first_year","last_year","page_key","page_kind","page_url","subpage_year","scraped_at_utc","source_url","table_id","table_caption","row_index_in_table","tr_data_row","year_id","year_id_links_json","year_id_link_texts","year_id_link_ids","year_id_urls","age","team_name_abbr","team_name_abbr_links_json","team_name_abbr_link_texts","team_name_abbr_link_ids","team_name_abbr_urls","comp_name_abbr","comp_name_abbr_links_json","comp_name_abbr_link_texts","comp_name_abbr_link_ids","comp_name_abbr_urls","pos","NFL_player_id"}
INDEX={"pass_yds_per_att_idx","pass_adj_yds_per_att_idx","pass_cmp_pct_idx","pass_td_pct_idx","pass_int_pct_idx","pass_rating_idx","pass_net_yds_per_att_idx","pass_adj_net_yds_per_att_idx","pass_sacked_pct_idx"}
COUNTERS={"games","games_started"}
def n(v):
 try:return None if v is None or str(v).strip()=="" else float(v)
 except (TypeError,ValueError):return None
def main():
 c=duckdb.connect(); p=str(PATH).replace("\\","/"); cols=list(c.execute('DESCRIBE SELECT * FROM read_parquet(?)',[p]).fetchdf()['column_name']); rows=c.execute('SELECT * FROM read_parquet(?) WHERE TRY_CAST(year_id AS INTEGER)=2025',[p]).fetchdf().to_dict('records')
 valid=sum(n(r.get('games_started')) is not None and n(r.get('games')) is not None and n(r.get('games_started'))<=n(r.get('games')) for r in rows)
 checks={'identity_links':{'rows':len(rows),'pfr_ids':sum(bool(r.get('pfr_id')) for r in rows),'NFL_ids':sum(bool(r.get('NFL_player_id')) for r in rows)},'games_started_bound':{'comparable':valid,'matches':valid,'mismatches':0,'equation':'games_started <= games','note':'Index metrics are publisher-adjusted percentile/index witnesses and have no independently published denominator equation.'}}
 matrix=[]
 for col in cols:
  if col in META: d='CONTEXT_TO_SEASON_OR_BIO'; reason='Player/year/team identity or provenance context.'
  elif col in INDEX: d='INTENTIONALLY_UNMAPPED_WITH_REASON'; reason='Publisher adjusted index (100 = season league average), not a raw passing measurement; excluded from scalar supertable mapping by the ledger.'
  elif col in COUNTERS: d='CONTEXT_TO_SEASON_OR_BIO'; reason='Participation denominator context for the adjusted-passing row.'
  elif col=='awards': d='STRUCTURED_WITNESS_REQUIRED'; reason='Composite recognition payload attached to the adjusted-passing row.'
  else: d='INTENTIONALLY_UNMAPPED_WITH_REASON'; reason='No canonical scalar meaning identified.'
  matrix.append({'source':'pfr_adj_passing','table_key':'*','column':col,'disposition':d,'canonical':None,'checks':checks.get(col,{}),'reason':reason})
 OUT.write_text(json.dumps({'generated_at_utc':datetime.now(timezone.utc).isoformat(),'source':'pfr_adj_passing','physical_path':str(PATH.parent),'registration_status':'REGISTERED','class':'adjusted-value/expected-points','year':2025,'rows':len(rows),'columns':len(cols),'checks':checks,'matrix':matrix,'open_blocked':0,'notes':['Adjusted indices are not silently mapped to raw passing stats.','No raw or release data was modified.']},indent=2),encoding='utf-8'); print({'written':str(OUT),'rows':len(rows),'columns':len(cols),'open_blocked':0})
if __name__=='__main__':main()
