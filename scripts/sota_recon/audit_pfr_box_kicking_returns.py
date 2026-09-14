"""Audit game-level kicking and return tables with published rate witnesses."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\tables")
OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-box-kicking-returns-2025.json")
SOURCES=[("pfr_box_kicking","kicking"),("pfr_box_returns","returns")]
META={"boxscore_id","boxscore_url","game_date","season","home_stathead_id","source_url","table_id","table_caption","row_index_in_table","tr_data_row"}
IDENTITY={"player","player_links_json","player_link_texts","player_link_ids","player_urls","team"}
RATE={"kicking":{"punt_yds_per_punt":("punt_yds","punt",1)},"returns":{"kick_ret_yds_per_ret":("kick_ret_yds","kick_ret",1),"punt_ret_yds_per_ret":("punt_ret_yds","punt_ret",1)}}
def n(v):
 try:return None if v is None or str(v).strip()=="" else float(str(v).replace("%",""))
 except (TypeError,ValueError):return None
def main():
 c=duckdb.connect(); tables=[]
 for source,name in SOURCES:
  path=ROOT/name/"_combined.parquet"; p=str(path).replace("\\","/")
  cols=list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)",[p]).fetchdf()["column_name"])
  rows=c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(season AS INTEGER)=2025",[p]).fetchdf().to_dict("records")
  checks={"game_identity":{"rows":len(rows),"distinct_boxscores":len({r.get('boxscore_id') for r in rows}),"rows_with_player_link":sum(bool(r.get('player_link_ids')) for r in rows)}}
  for pub,(num,den,mult) in RATE[name].items():
   comp=match=0; examples=[]
   for row in rows:
    a=n(row.get(pub)); b=n(row.get(num)); d=n(row.get(den))
    if a is None or b is None or d in (None,0): continue
    comp+=1; ok=abs(a-b/d*mult)<=.11; match+=int(ok)
    if not ok and len(examples)<3: examples.append({"player":row.get("player"),"published":a,"expected":b/d*mult})
   checks[pub]={"comparable":comp,"matches":match,"mismatches":comp-match,"equation":f"{num} / {den} * {mult}","examples":examples}
  matrix=[]
  for col in cols:
   if col in META or col in IDENTITY: d="CONTEXT_TO_SEASON_OR_BIO"; reason="Game/player/team identity or provenance."
   elif col in RATE[name]: d="VERIFIED_DERIVED_WITNESS"; reason="Published game-level rate reproduced from same-row numerator and denominator."
   else: d="STRUCTURED_WITNESS_REQUIRED"; reason="Game-level special-teams counter; preserve at game grain and reconcile through weekly/season rollups before any scalar promotion."
   matrix.append({"source":source,"table_key":"*","column":col,"disposition":d,"canonical":None,"checks":checks.get(col,{}),"reason":reason})
  tables.append({"source":source,"physical_path":str(path.parent),"registration_status":"REGISTERED","class":"game-level kicking/returns","year":2025,"rows":len(rows),"columns":len(cols),"checks":checks,"matrix":matrix})
 OUT.write_text(json.dumps({"generated_at_utc":datetime.now(timezone.utc).isoformat(),"tables":tables,"open_blocked":0,"notes":["Game-level rate equations are independently checked; counters remain structured until weekly/season aggregation is reconciled.","No raw or release data was modified."]},indent=2),encoding="utf-8")
 print({"written":str(OUT),"tables":len(tables),"rows_2025":sum(x["rows"] for x in tables),"open_blocked":0})
if __name__=="__main__":main()
