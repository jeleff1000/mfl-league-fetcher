"""Audit registered game-info, officials, team-stats, and expected-points surfaces."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr\boxscores\tables")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-boxscore-context-2025.json")
SOURCES = [("pfr_box_game_info", "game_info", "game-information/stadium/weather"), ("pfr_box_officials", "officials", "officials"), ("pfr_box_team_stats", "team_stats", "team boxscore"), ("pfr_box_expected_points", "expected_points", "adjusted-value/expected-points")]
META = {"boxscore_id", "boxscore_url", "game_date", "season", "home_stathead_id", "source_url", "table_id", "table_caption", "row_index_in_table", "tr_data_row"}

def num(v):
    try: return None if v is None or str(v).strip()=="" else float(v)
    except (TypeError, ValueError): return None

def main() -> None:
    c=duckdb.connect(); tables=[]
    for source, name, cls in SOURCES:
        path=ROOT/name/"_combined.parquet"; p=str(path).replace("\\", "/")
        cols=list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)",[p]).fetchdf()["column_name"])
        rows=c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(season AS INTEGER)=2025",[p]).fetchdf().to_dict("records")
        games=sorted({r.get("boxscore_id") for r in rows if r.get("boxscore_id")})
        checks={"game_identity": {"rows":len(rows),"distinct_boxscores":len(games),"rows_with_boxscore_id":sum(bool(r.get("boxscore_id")) for r in rows)}}
        if name=="expected_points":
            complete=zero=0; examples=[]
            for gid in games:
                group=[r for r in rows if r.get("boxscore_id")==gid]
                if len(group)!=2: continue
                complete+=1; ok=True
                a,b=group
                pairs=[("pbp_exp_points_tot", "pbp_exp_points_tot", 1), ("pbp_exp_points_off_tot", "pbp_exp_points_def_tot", 1), ("pbp_exp_points_def_tot", "pbp_exp_points_off_tot", 1)]
                for left,right,_ in pairs:
                    av=num(a.get(left)); bv=num(b.get(right))
                    if av is None or bv is None or abs(av+bv)>0.021: ok=False
                zero+=int(ok)
                if not ok and len(examples)<3: examples.append({"boxscore_id":gid,"rows":group})
            checks["team_total_and_offense_defense_crosscheck"]={"complete_games":complete,"exact_games":zero,"mismatches":complete-zero,"examples":examples,"equations":["team total A + team total B = 0","offense total A + defense total B = 0","defense total A + offense total B = 0"],"note":"Only proven opposing-team invariants are checked; special-teams subcomponents are not guessed."}
        elif name=="team_stats":
            checks["paired_team_values"]={"complete_rows":sum(bool(r.get("stat")) and r.get("vis_stat") is not None and r.get("home_stat") is not None for r in rows),"rows":len(rows),"note":"Long-form stat label with visitor/home operands; composite values remain structured."}
        matrix=[]
        for col in cols:
            if col in META: d="CONTEXT_TO_SEASON_OR_BIO"; reason="Game/source/provenance context."
            elif name=="expected_points" and col.startswith("pbp_exp_"): d="STRUCTURED_WITNESS_REQUIRED"; reason="Publisher expected-points decomposition; ledger excludes the publisher-specific component from the scalar supertable."
            else: d="STRUCTURED_WITNESS_REQUIRED"; reason="Game-level or long-format context/detail field; preserve at native grain rather than forcing a scalar player mapping."
            matrix.append({"source":source,"table_key":"*","column":col,"disposition":d,"canonical":None,"checks":{},"reason":reason})
        tables.append({"source":source,"physical_path":str(path.parent),"registration_status":"REGISTERED","class":cls,"year":2025,"rows":len(rows),"columns":len(cols),"checks":checks,"matrix":matrix})
    OUT.write_text(json.dumps({"generated_at_utc":datetime.now(timezone.utc).isoformat(),"tables":tables,"open_blocked":0,"notes":["All four registered boxscore context surfaces are audited at native game/long-form grain.","No raw or release data was modified."]},indent=2),encoding="utf-8")
    print({"written":str(OUT),"tables":len(tables),"rows_2025":sum(x["rows"] for x in tables),"open_blocked":0})

if __name__=="__main__": main()
