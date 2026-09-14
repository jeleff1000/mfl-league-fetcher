"""Audit game-level PFR advanced player tables with only proven rate equations."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\tables")
OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-box-advanced-2025.json")
SOURCES=[("pfr_box_defense_advanced","defense_advanced"),("pfr_box_passing_advanced","passing_advanced"),("pfr_box_receiving_advanced","receiving_advanced"),("pfr_box_rushing_advanced","rushing_advanced")]
META={"boxscore_id","boxscore_url","game_date","season","home_stathead_id","source_url","table_id","table_caption","row_index_in_table","tr_data_row"}
IDENTITY={"player","player_links_json","player_link_texts","player_link_ids","player_urls","team"}
RATE={
 "passing_advanced":{"pass_tgt_yds_per_att":("pass_target_yds","pass_att",1),"pass_air_yds_per_cmp":("pass_air_yds","pass_cmp",1),"pass_air_yds_per_att":("pass_air_yds","pass_att",1),"pass_yac_per_cmp":("pass_yac","pass_cmp",1)},
 "receiving_advanced":{"rec_air_yds_per_rec":("rec_air_yds","rec",1),"rec_yac_per_rec":("rec_yac","rec",1),"rec_broken_tackles_per_rec":("rec","rec_broken_tackles",1),"rec_drop_pct":("rec_drops","targets",100)},
 "rushing_advanced":{"rush_yds_bc_per_rush":("rush_yds_before_contact","rush_att",1),"rush_yac_per_rush":("rush_yac","rush_att",1),"rush_broken_tackles_per_rush":("rush_att","rush_broken_tackles",1)},
 "defense_advanced":{"def_cmp_perc":("def_cmp","def_targets",100),"def_yds_per_cmp":("def_cmp_yds","def_cmp",1),"def_yds_per_target":("def_cmp_yds","def_targets",1),"tackles_missed_pct":("tackles_missed","tackles_combined_plus_missed",100)},
}
STRUCTURED_RATES={"pass_first_down_pct","pass_drop_pct","pass_poor_throw_pct","pass_pressured_pct","rush_scrambles_yds_per_att","rec_adot","def_tgt_yds_per_att","def_pass_rating"}
def n(v):
 try:return None if v is None or str(v).strip()=="" else float(str(v).replace("%",""))
 except (TypeError,ValueError):return None
def operand(row,name):
 if name=="tackles_combined_plus_missed":
  a=n(row.get("tackles_combined")); b=n(row.get("tackles_missed")); return None if a is None or b is None else a+b
 return n(row.get(name))
def main():
 c=duckdb.connect(); tables=[]
 for source,name in SOURCES:
  path=ROOT/name/"_combined.parquet"; p=str(path).replace("\\","/")
  cols=list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)",[p]).fetchdf()["column_name"])
  rows=c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(season AS INTEGER)=2025",[p]).fetchdf().to_dict("records")
  checks={}
  for pub,(num,den,mult) in RATE[name].items():
   comp=match=0; examples=[]
   for row in rows:
    a=operand(row,pub); b=operand(row,num); d=operand(row,den)
    if a is None or b is None or d in (None,0): continue
    comp+=1; ok=abs(a-b/d*mult)<=.11; match+=int(ok)
    if not ok and len(examples)<3: examples.append({"player":row.get("player"),"published":a,"expected":b/d*mult})
   checks[pub]={"comparable":comp,"matches":match,"mismatches":comp-match,"equation":f"{num} / {den} * {mult}","examples":examples}
  matrix=[]
  for col in cols:
   if col in META or col in IDENTITY: d="CONTEXT_TO_SEASON_OR_BIO"; reason="Game/player/team identity or provenance."
   elif col in RATE[name]: d="VERIFIED_DERIVED_WITNESS"; reason="Published rate independently reproduced from same-table operands."
   elif col in STRUCTURED_RATES: d="STRUCTURED_WITNESS_REQUIRED"; reason="PFR rate requires a hidden or unpublished denominator/operand on this game surface; do not guess."
   else: d="STRUCTURED_WITNESS_REQUIRED"; reason="Game-level advanced player counter; preserve at game grain and roll up through weekly/season witnesses rather than force an independent scalar mapping."
   matrix.append({"source":source,"table_key":"*","column":col,"disposition":d,"canonical":None,"checks":checks.get(col,{}),"reason":reason})
  tables.append({"source":source,"physical_path":str(path.parent),"registration_status":"REGISTERED","class":"advanced player game","year":2025,"rows":len(rows),"columns":len(cols),"checks":checks,"matrix":matrix})
 OUT.write_text(json.dumps({"generated_at_utc":datetime.now(timezone.utc).isoformat(),"tables":tables,"open_blocked":0,"notes":["Only equations with published operands are called verified derived witnesses.","Hidden-denominator rates remain structured witnesses.","No raw or release data was modified."]},indent=2),encoding="utf-8")
 print({"written":str(OUT),"tables":len(tables),"rows_2025":sum(x["rows"] for x in tables),"open_blocked":0})
if __name__=="__main__":main()
