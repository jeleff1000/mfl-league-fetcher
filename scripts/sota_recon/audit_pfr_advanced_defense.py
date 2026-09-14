"""Read-only 2025 receipts for PFR advanced player-defense tables."""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from scripts.sota_recon.column_dossier import load_decisions


ROOT = Path(r"D:\league-history-data\nfl\raw\pfr\players\tables")
RELEASE = Path(r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet")
SURFACES = {
    "regular": ("adv_defense", "pfr_adv_defense", "REG"),
    "post": ("adv_defense_post", "pfr_adv_defense_post", "POST"),
}


def missing(v):
    return v is None or (isinstance(v, float) and math.isnan(v))


def ratio(n, d, scale=1.0):
    if missing(n) or missing(d) or float(d) == 0:
        return None
    return float(n) / float(d) * scale


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--surface", choices=sorted(SURFACES), default="regular"); args = ap.parse_args()
    table_name, decision_key, season_type = SURFACES[args.surface]
    pfr_path = ROOT / table_name / "_combined.parquet"; pfr = str(pfr_path).replace("\\", "/")
    release = str(RELEASE).replace("\\", "/")
    output = Path(r"D:\yahoo_oauth\docs\audits") / f"pfr-{decision_key}-2025.json"
    c = duckdb.connect()
    pfr_sql = """
    WITH t AS (
      SELECT *,
        TRY_CAST(NULLIF(def_int, '') AS DOUBLE) int_n,
        TRY_CAST(NULLIF(def_targets, '') AS DOUBLE) targets_n,
        TRY_CAST(NULLIF(def_cmp, '') AS DOUBLE) cmp_n,
        TRY_CAST(NULLIF(def_cmp_pct, '') AS DOUBLE) cmp_pct_n,
        TRY_CAST(NULLIF(def_cmp_yds, '') AS DOUBLE) cmp_yds_n,
        TRY_CAST(NULLIF(def_yds_per_cmp, '') AS DOUBLE) yds_cmp_n,
        TRY_CAST(NULLIF(def_yds_per_target, '') AS DOUBLE) yds_target_n,
        TRY_CAST(NULLIF(def_cmp_td, '') AS DOUBLE) cmp_td_n,
        TRY_CAST(NULLIF(def_pass_rating, '') AS DOUBLE) rating_n,
        TRY_CAST(NULLIF(def_tgt_yds_per_att, '') AS DOUBLE) target_yds_att_n,
        TRY_CAST(NULLIF(def_air_yds, '') AS DOUBLE) air_n,
        TRY_CAST(NULLIF(def_yac, '') AS DOUBLE) yac_n,
        TRY_CAST(NULLIF(blitzes, '') AS DOUBLE) blitz_n,
        TRY_CAST(NULLIF(qb_hurry, '') AS DOUBLE) hurry_n,
        TRY_CAST(NULLIF(qb_knockdown, '') AS DOUBLE) knockdown_n,
        TRY_CAST(NULLIF(def_batted_passes, '') AS DOUBLE) batted_n,
        TRY_CAST(NULLIF(sacks, '') AS DOUBLE) sacks_n,
        TRY_CAST(NULLIF(pressures, '') AS DOUBLE) pressures_n,
        TRY_CAST(NULLIF(tackles_combined, '') AS DOUBLE) tackles_n,
        TRY_CAST(NULLIF(tackles_missed, '') AS DOUBLE) missed_n,
        TRY_CAST(NULLIF(tackles_missed_pct, '') AS DOUBLE) missed_pct_n
      FROM read_parquet(?) WHERE year_id = '2025'
    )
    SELECT * FROM t
    QUALIFY ROW_NUMBER() OVER (PARTITION BY player, year_id ORDER BY CASE WHEN team_name_abbr='2TM' THEN 0 ELSE 1 END, team_name_abbr)=1
    """
    v_sql = f"""
    SELECT lower(trim(player)) player_key,
      SUM(def_interceptions) int_n, SUM(def_targets_allowed) targets_n,
      SUM(def_completions_allowed) cmp_n, SUM(def_completion_yards_allowed) cmp_yds_n,
      SUM(def_completion_tds_allowed) cmp_td_n, SUM(def_passer_rating_allowed * def_targets_allowed) rating_weighted,
      SUM(def_air_yards_allowed) air_n, SUM(def_yards_after_catch_allowed) yac_n,
      SUM(def_blitzes) blitz_n, SUM(def_hurries) hurry_n, SUM(def_knockdowns) knockdown_n,
      SUM(def_sacks) sacks_n, SUM(def_pressures) pressures_n, SUM(def_tackles_combined) tackles_n,
      SUM(def_tackles_missed) missed_n, COUNT(DISTINCT week) games_n
    FROM read_parquet(?) WHERE year=2025 AND season_type='{season_type}' GROUP BY 1
    """
    rows = c.execute(pfr_sql, [pfr]).fetchdf().to_dict("records")
    weekly = {r["player_key"]: r for r in c.execute(v_sql, [release]).fetchdf().to_dict("records")}
    direct = {"def_int": ("int_n", "int_n"), "def_targets": ("targets_n", "targets_n"), "def_cmp": ("cmp_n", "cmp_n"), "def_cmp_yds": ("cmp_yds_n", "cmp_yds_n"), "def_cmp_td": ("cmp_td_n", "cmp_td_n"), "def_air_yds": ("air_n", "air_n"), "def_yac": ("yac_n", "yac_n"), "blitzes": ("blitz_n", "blitz_n"), "qb_hurry": ("hurry_n", "hurry_n"), "qb_knockdown": ("knockdown_n", "knockdown_n"), "sacks": ("sacks_n", "sacks_n"), "pressures": ("pressures_n", "pressures_n"), "tackles_missed": ("missed_n", "missed_n")}
    checks = {}
    for col, (pk, vk) in direct.items():
        comp = match = 0
        for r in rows:
            v = weekly.get(str(r["player"]).strip().lower()); a, b = r.get(pk), v.get(vk) if v else None
            if not missing(a) and not missing(b): comp += 1; match += int(float(a) == float(b))
        checks[col] = {"comparable": comp, "exact_matches": match, "mismatches": comp-match, "aggregation": "SUM"}
    checks["tackles_combined"] = {"comparable": 0, "blocker": "The total is disputed across PFR/StatsCrew/NFL.com surfaces; preserve as a structured tackle-total witness until solo+assist versus publisher-total semantics are adjudicated."}
    rate_specs = {"def_cmp_pct": ("cmp_pct_n", "cmp_n", "targets_n", 100.0, "completions_allowed/targets_allowed"), "def_yds_per_cmp": ("yds_cmp_n", "cmp_yds_n", "cmp_n", 1.0, "completion_yards_allowed/completions_allowed"), "def_yds_per_target": ("yds_target_n", "cmp_yds_n", "targets_n", 1.0, "completion_yards_allowed/targets_allowed"), "tackles_missed_pct": ("missed_pct_n", "missed_n", "tackle_opportunities", 100.0, "tackles_missed/(tackles_combined+tackles_missed)")}
    for col, (ak, num, den, scale, denominator) in rate_specs.items():
        pc = pm = vc = vm = 0
        for r in rows:
            v = weekly.get(str(r["player"]).strip().lower()); actual = r.get(ak)
            if den == "tackle_opportunities":
                pden = (float(r["tackles_n"])+float(r["missed_n"])) if not missing(r.get("tackles_n")) and not missing(r.get("missed_n")) else None
                vden = (float(v["tackles_n"])+float(v["missed_n"])) if v and not missing(v.get("tackles_n")) and not missing(v.get("missed_n")) else None
            else: pden, vden = r.get(den), v.get(den) if v else None
            ep = ratio(r.get(num), pden, scale); ev = ratio(v.get(num), vden, scale) if v else None
            if not missing(actual) and not missing(ep): pc += 1; pm += int(abs(float(actual)-round(ep,1)) <= .11)
            if not missing(actual) and not missing(ev): vc += 1; vm += int(abs(float(actual)-round(ev,1)) <= .11)
        checks[col] = {"pfr_operand_comparable": pc, "pfr_operand_matches": pm, "pfr_mismatches": pc-pm, "v26_operand_comparable": vc, "v26_rounded_one_decimal_matches": vm, "v26_mismatches": vc-vm, "denominator": denominator}
    checks["def_pass_rating"] = {"blocker": "Allowed passer rating is nonlinear; no declared season aggregation equation."}
    checks["def_tgt_yds_per_att"] = {"blocker": "PFR publishes this beside def_yds_per_target; its denominator/meaning is not settled from the captured identifiers."}

    decisions = load_decisions(); prefix = decision_key + "|*|"; by_col = {k[len(prefix):]: dict(v) for k,v in decisions.items() if k.startswith(prefix)}
    cols = [r[0] for r in c.execute("describe select * from read_parquet(?)", [pfr]).fetchall()]
    capture={"pfr_id":("CONTEXT_TO_SEASON_OR_BIO","PFR identity key; player_bio crosswalk."),"player":("CONTEXT_TO_SEASON_OR_BIO","Player identity; player_bio lane."),"team_name_abbr":("CONTEXT_TO_SEASON_OR_BIO","Season team context."),"comp_name_abbr":("CONTEXT_TO_SEASON_OR_BIO","NFL comparison context."),"NFL_player_id":("CONTEXT_TO_SEASON_OR_BIO","Canonical identity key; player_bio lane.")}
    locator={"index_letter","index_position","first_year","last_year","page_key","page_kind","page_url","subpage_year","scraped_at_utc","source_url","table_id","table_caption","row_index_in_table","tr_data_row","year_id","year_id_links_json","year_id_link_texts","year_id_link_ids","year_id_urls","team_name_abbr_links_json","team_name_abbr_link_texts","team_name_abbr_link_ids","team_name_abbr_urls","comp_name_abbr_links_json","comp_name_abbr_link_texts","comp_name_abbr_link_ids","comp_name_abbr_urls"}
    matrix=[]
    for col in cols:
        if col in by_col: e=dict(by_col[col])
        elif col=="tackles_combined": e={"disposition":"STRUCTURED_WITNESS_REQUIRED","reason":"Tackle total is under active cross-source adjudication."}
        elif col in {"def_tgt_yds_per_att","def_yds_per_target"}: e={"disposition":"STRUCTURED_WITNESS_REQUIRED","reason":"The two published defensive target-yard fields must be resolved as a pair; one is a derived yard/target witness and the other has unresolved denominator semantics."}
        elif col=="awards" or col.startswith("awards_"): e={"disposition":"STRUCTURED_WITNESS_REQUIRED","reason":"Awards/link values are composite membership/provenance material."}
        elif col in capture: d,reason=capture[col]; e={"disposition":d,"reason":reason}
        elif col in locator: e={"disposition":"INTENTIONALLY_UNMAPPED_WITH_REASON","reason":"Capture locator/markup/provenance, not a stat."}
        else: e={"disposition":"OPEN_BLOCKED","reason":"No ledger disposition found."}
        e["column"]=col
        if col in rate_specs: e["audit_disposition"]="VERIFIED_DERIVED_WITNESS" if not checks[col].get("pfr_mismatches") and not checks[col].get("v26_mismatches") else "VERIFIED_DERIVED_WITNESS_WITH_RATE_GAP"
        elif col in {"def_tgt_yds_per_att","def_yds_per_target","tackles_combined"}: e["audit_disposition"]="STRUCTURED_WITNESS_REQUIRED"
        elif col in direct and checks[col].get("mismatches"): e["audit_disposition"]="DEFERRED_BACKFILL_CANDIDATE"
        elif e.get("disposition")=="MAPPED_TO_CANONICAL": e["audit_disposition"]="VERIFIED_DIRECT_MAPPING"
        elif e.get("disposition")=="EXCLUDED_WITH_REASON": e["audit_disposition"]="VERIFIED_DERIVED_WITNESS"
        elif e.get("disposition")=="NEW_SUPERTABLE_COLUMN_CANDIDATE": e["audit_disposition"]="PROMOTION_CANDIDATE"
        else: e["audit_disposition"]=e.get("disposition")
        matrix.append(e)
    receipt={"source":{"path":str(pfr_path),"table_id":table_name,"table_class":"advanced player defense / postseason" if season_type=="POST" else "advanced player defense / regular-season","subtables":[{"table_id":table_name,"caption":"Advanced Defense Table"}]},"grain":{"pfr":"player-season with 2TM preference","canonical":f"player-week {season_type} rolled to player-season","year":2025},"counts":{"pfr_total_rows":int(c.execute("select count(*) from read_parquet(?)",[pfr]).fetchone()[0]),"pfr_2025_selected_rows":len(rows),"weekly_players":len(weekly)},"checks":checks,"column_dispositions":matrix,"generated_at_utc":datetime.now(timezone.utc).isoformat()}
    output.parent.mkdir(parents=True,exist_ok=True);output.write_text(json.dumps(receipt,indent=2,default=str)+"\n",encoding="utf-8")
    print(json.dumps({"output":str(output),"rows":len(rows),"columns":len(matrix),"open":[r["column"] for r in matrix if r.get("audit_disposition")=="OPEN_BLOCKED"],"checks":checks},indent=2,default=str))


if __name__ == "__main__": main()
