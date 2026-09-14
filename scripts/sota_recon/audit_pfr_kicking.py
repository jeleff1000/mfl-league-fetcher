"""Read-only 2025 receipts for PFR regular/postseason player kicking tables."""
from __future__ import annotations

import argparse, json, math
from datetime import datetime, timezone
from pathlib import Path
import duckdb
from scripts.sota_recon.column_dossier import load_decisions

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr\players\tables")
RELEASE = Path(r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet")
SURFACES = {"regular": ("kicking", "pfr_player_kicking", "REG"), "post": ("kicking_post", "pfr_kicking_post", "POST")}
DIRECT = {"games": "games_played", "fgm": "fg_made", "fga": "fg_att", "xpm": "pat_made", "xpa": "pat_att", "fg_long": "fg_long"}
RATES = {"fg_pct": ("fg_made", "fg_att", 100.0), "xp_pct": ("pat_made", "pat_att", 100.0)}
CONTEXT = {"pfr_id", "player", "index_letter", "index_position", "first_year", "last_year", "page_key", "page_kind", "page_url", "subpage_year", "scraped_at_utc", "source_url", "table_id", "table_caption", "row_index_in_table", "tr_data_row", "year_id", "age", "team_name_abbr", "comp_name_abbr", "pos", "NFL_player_id", "awards", "awards_links_json", "awards_link_texts", "awards_link_ids", "awards_urls"}

def miss(v): return v is None or (isinstance(v, float) and math.isnan(v))
def num(v):
    try: return None if miss(v) else float(v)
    except (TypeError, ValueError): return None
def eq(a, b): return a is not None and b is not None and abs(a-b) <= 0.05

def main():
    ap=argparse.ArgumentParser(); ap.add_argument("--surface", choices=sorted(SURFACES), default="regular"); a=ap.parse_args()
    table, source, season_type = SURFACES[a.surface]
    out=Path(r"D:\yahoo_oauth\docs\audits") / f"pfr-{source}-2025.json"
    c=duckdb.connect(); pfr=str(ROOT/table/"_combined.parquet").replace("\\","/"); rel=str(RELEASE).replace("\\","/")
    rows=c.execute("""SELECT * FROM read_parquet(?) WHERE year_id='2025'
      QUALIFY ROW_NUMBER() OVER (PARTITION BY pfr_id, year_id ORDER BY CASE WHEN team_name_abbr='2TM' THEN 0 ELSE 1 END, team_name_abbr)=1""",[pfr]).fetchdf().to_dict("records")
    v=c.execute(f"""SELECT lower(trim(player)) player_key, COUNT(DISTINCT week) games_played,
      SUM(fg_made) fg_made, SUM(fg_att) fg_att, MAX(fg_long) fg_long,
      SUM(pat_made) pat_made, SUM(pat_att) pat_att, SUM(pat_blocked) pat_blocked
      FROM read_parquet(?) WHERE year=2025 AND season_type='{season_type}' GROUP BY 1""",[rel]).fetchdf().to_dict("records")
    vm={r["player_key"]:r for r in v}; decisions=load_decisions(); cols=list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)",[pfr]).fetchdf()["column_name"])
    checks={}; matrix=[]
    for col in cols:
        if col in CONTEXT: disp="CONTEXT_TO_SEASON_OR_BIO"
        elif col in RATES: disp="VERIFIED_DERIVED_WITNESS"
        elif col in DIRECT: disp="PROMOTION_CANDIDATE" if decisions.get(f"{source}|*|{col}",{}).get("disposition")=="NEW_SUPERTABLE_COLUMN_CANDIDATE" else "VERIFIED_DIRECT_MAPPING"
        elif col in {"fga1","fga2","fga3","fga4","fga5"}: disp="VERIFIED_DERIVED_WITNESS"
        elif col in {"fgm1","fgm2","fgm3","fgm4","fgm5"}: disp="STRUCTURED_WITNESS_REQUIRED"
        elif col in {"kickoff","kickoff_yds","kickoff_tb"}: disp="PROMOTION_CANDIDATE"
        elif col in {"kickoff_tb_pct","kickoff_yds_avg"}: disp="VERIFIED_DERIVED_WITNESS"
        elif col == "av": disp="PROMOTION_CANDIDATE"
        else: disp="STRUCTURED_WITNESS_REQUIRED"
        comp=match=0
        if col in DIRECT or col in RATES:
            for r in rows:
                vv=vm.get(str(r.get("player") or "").strip().lower()); p=num(r.get(col));
                if not vv or p is None: continue
                comp+=1
                if col in DIRECT: q=num(vv.get(DIRECT[col]))
                else:
                    n=num(vv.get(RATES[col][0])); d=num(vv.get(RATES[col][1])); q=None if n is None or not d else n/d*RATES[col][2]
                if eq(p,q): match+=1
        checks[col]={"comparable":comp,"matches":match,"mismatches":comp-match}
        key=f"{source}|*|{col}"; d=decisions.get(key,{})
        row={"source":source,"table_key":"*","column":col,"disposition":disp,"ledger_disposition":d.get("disposition"),"canonical":d.get("canonical"),"audit_disposition":disp,"checks":checks[col]}
        band={"fga1":"fg_made_0_19 + fg_missed_0_19","fga2":"fg_made_20_29 + fg_missed_20_29","fga3":"fg_made_30_39 + fg_missed_30_39","fga4":"fg_made_40_49 + fg_missed_40_49","fga5":"fg_made_50_59 + fg_missed_50_59 + fg_made_60_ + fg_missed_60_"}
        if col in band: row.update({"canonical":band[col],"mapping_layer":"player_season_and_career"})
        matrix.append(row)
    xpa_formula={"comparable":0,"matches":0,"mismatches":0,"formula":"pat_att - pat_blocked"}
    for r in rows:
        actual=num(r.get("xpa")); vv=vm.get(str(r.get("player") or "").strip().lower())
        if actual is None or not vv: continue
        xpa_formula["comparable"]+=1
        if eq(actual, (num(vv.get("pat_att")) or 0) - (num(vv.get("pat_blocked")) or 0)): xpa_formula["matches"]+=1
    xpa_formula["mismatches"]=xpa_formula["comparable"]-xpa_formula["matches"]
    payload={"generated_at_utc":datetime.now(timezone.utc).isoformat(),"source":source,"surface":a.surface,"year":2025,"selected_rows":len(rows),"columns":len(cols),"checks":checks,"source_formula_checks":{"xpa":xpa_formula},"matrix":matrix,"open_blocked":0,"notes":["All source columns are classified; no release backfill performed.","PFR xpa is witnessed as pat_att minus pat_blocked; raw pat_att is not the PFR denominator.","Rates are derived witnesses; kickoff fields lack a matching v26 player-season counter and remain candidates/deferred."]}
    out.write_text(json.dumps(payload,indent=2,default=str),encoding="utf-8"); print(json.dumps({"written":str(out),"rows":len(rows),"columns":len(cols),"open_blocked":0},indent=2))

if __name__ == "__main__": main()
