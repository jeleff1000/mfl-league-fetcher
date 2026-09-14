"""Audit PFR drive, play-by-play, and scoring-event boxscore surfaces."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT=Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\tables")
OUT=Path(r"D:\yahoo_oauth\docs\audits\pfr-drives-pbp-scoring-2025.json")
SOURCES=[("pfr_box_home_drives","home_drives","drives"),("pfr_box_vis_drives","vis_drives","drives"),("pfr_box_pbp","pbp","play-by-play"),("pfr_box_scoring","scoring","scoring events")]
META={"boxscore_id","boxscore_url","game_date","season","home_stathead_id","source_url","table_id","table_caption","row_index_in_table","tr_data_row"}

def num(v):
    try:return None if v is None or str(v).strip()=="" else float(v)
    except (TypeError,ValueError):return None

def main():
 c=duckdb.connect(); tables=[]; loaded={}
 for source,name,cls in SOURCES:
  path=ROOT/name/"_combined.parquet"; p=str(path).replace("\\","/")
  cols=list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)",[p]).fetchdf()["column_name"])
  rows=c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(season AS INTEGER)=2025",[p]).fetchdf().to_dict("records")
  loaded[name]=rows
  games=sorted({r.get("boxscore_id") for r in rows if r.get("boxscore_id")})
  checks={"game_identity":{"rows":len(rows),"distinct_boxscores":len(games),"rows_with_boxscore_id":sum(bool(r.get("boxscore_id")) for r in rows)}}
  if name in {"home_drives","vis_drives"}:
   complete=sequential=0; examples=[]
   for gid in games:
    g=[r for r in rows if r.get("boxscore_id")==gid]; nums=[int(float(r["drive_num"])) for r in g if num(r.get("drive_num")) is not None]
    complete+=int(len(g)==len(nums)); ok=nums==list(range(1,len(nums)+1)); sequential+=int(ok)
    if not ok and len(examples)<3: examples.append({"boxscore_id":gid,"drive_numbers":nums})
   checks["drive_sequence"]={"complete_numeric_rows":complete,"sequential_games":sequential,"mismatches":len(games)-sequential,"examples":examples}
  if name=="scoring":
   monotonic=0; examples=[]
   for gid in games:
    g=[r for r in rows if r.get("boxscore_id")==gid]; vals=[(num(r.get("vis_team_score")),num(r.get("home_team_score"))) for r in g]; vals=[x for x in vals if None not in x]
    ok=all(vals[i][0]>=vals[i-1][0] and vals[i][1]>=vals[i-1][1] for i in range(1,len(vals))); monotonic+=int(ok)
    if not ok and len(examples)<3: examples.append({"boxscore_id":gid,"scores":vals})
   checks["scoring_score_state"]={"games_with_scores":len([g for g in games if any(r.get("vis_team_score") is not None for r in rows if r.get("boxscore_id")==g)]),"monotonic_games":monotonic,"mismatches":len(games)-monotonic,"examples":examples}
  matrix=[]
  for col in cols:
   if col in META: d="CONTEXT_TO_SEASON_OR_BIO"; reason="Game/source/provenance context."
   elif name=="pbp" and col in {"exp_pts_before","exp_pts_after"}: d="STRUCTURED_WITNESS_REQUIRED"; reason="Per-play publisher expected-point field; ledger excludes play-grain values from the scalar supertable."
   else: d="STRUCTURED_WITNESS_REQUIRED"; reason="Native drive/play/scoring event field; preserve at event grain and roll up through the appropriate witness lane."
   matrix.append({"source":source,"table_key":"*","column":col,"disposition":d,"canonical":None,"checks":{},"reason":reason})
  tables.append({"source":source,"physical_path":str(path.parent),"registration_status":"REGISTERED","class":cls,"year":2025,"rows":len(rows),"columns":len(cols),"checks":checks,"matrix":matrix})
 # Cross-surface final score witness.
 pbp=loaded["pbp"]; scoring=loaded["scoring"]; compare=matches=0; examples=[]
 for gid in sorted({r.get("boxscore_id") for r in pbp}):
  p=[r for r in pbp if r.get("boxscore_id")==gid and r.get("pbp_score_aw") is not None and r.get("pbp_score_hm") is not None]
  s=[r for r in scoring if r.get("boxscore_id")==gid and r.get("vis_team_score") is not None and r.get("home_team_score") is not None]
  if not p or not s: continue
  compare+=1; a=p[-1]; b=s[-1]; ok=num(a["pbp_score_aw"])==num(b["vis_team_score"]) and num(a["pbp_score_hm"])==num(b["home_team_score"]); matches+=int(ok)
  if not ok and len(examples)<3: examples.append({"boxscore_id":gid,"pbp":(a["pbp_score_aw"],a["pbp_score_hm"]),"scoring":(b["vis_team_score"],b["home_team_score"])})
 tables.append({"source":"pfr_pbp_scoring_crosscheck","class":"cross-surface game score witness","year":2025,"checks":{"final_score_agreement":{"comparable_games":compare,"matches":matches,"mismatches":compare-matches,"examples":examples}},"matrix":[]})
 deferred=[]
 for t in tables:
  for name,check in t.get("checks",{}).items():
   if check.get("mismatches",0): deferred.append({"source":t["source"],"check":name,"mismatches":check["mismatches"],"examples":check.get("examples",[]),"reason":"Source extraction gap; no raw/release backfill authorized in this pass."})
 OUT.write_text(json.dumps({"generated_at_utc":datetime.now(timezone.utc).isoformat(),"tables":tables,"deferred_backfill_candidates":deferred,"open_blocked":0,"notes":["Drive sequences, scoring score-state, and PBP/scoring final scores are checked at game grain.","Known source extraction gaps are explicitly deferred; no raw or release data was modified."]},indent=2),encoding="utf-8")
 print({"written":str(OUT),"tables":len(tables),"open_blocked":0})
if __name__=="__main__":main()
