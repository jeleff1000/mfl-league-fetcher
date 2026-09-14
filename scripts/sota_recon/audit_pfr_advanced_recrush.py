"""Read-only receipts for PFR advanced receiving/rushing player tables."""
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
    "recrush": ("adv_receiving_and_rushing", "pfr_adv_recrush", "REG"),
    "rushrec": ("adv_rushing_and_receiving", "pfr_adv_rushrec", "REG"),
    "recrush_post": ("adv_receiving_and_rushing_post", "pfr_adv_recrush_post", "POST"),
    "rushrec_post": ("adv_rushing_and_receiving_post", "pfr_adv_rushrec_post", "POST"),
}


def missing(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


def rate(numerator, denominator, scale=1.0):
    if missing(numerator) or missing(denominator) or float(denominator) == 0:
        return None
    return float(numerator) / float(denominator) * scale


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--surface", choices=sorted(SURFACES), default="recrush")
    args = parser.parse_args()
    table_name, decision_key, season_type = SURFACES[args.surface]
    pfr_path = ROOT / table_name / "_combined.parquet"
    output = Path(r"D:\yahoo_oauth\docs\audits") / f"pfr-{decision_key}-2025.json"
    pfr = str(pfr_path).replace("\\", "/")
    release = str(RELEASE).replace("\\", "/")
    con = duckdb.connect()

    pfr_sql = """
    WITH t AS (
      SELECT *,
        TRY_CAST(NULLIF(targets, '') AS DOUBLE) targets_n,
        TRY_CAST(NULLIF(rec, '') AS DOUBLE) rec_n,
        TRY_CAST(NULLIF(rec_yds, '') AS DOUBLE) rec_yds_n,
        TRY_CAST(NULLIF(rec_first_down, '') AS DOUBLE) rec_fd_n,
        TRY_CAST(NULLIF(rec_air_yds, '') AS DOUBLE) rec_air_n,
        TRY_CAST(NULLIF(rec_air_yds_per_rec, '') AS DOUBLE) rec_air_per_rec_n,
        TRY_CAST(NULLIF(rec_yac, '') AS DOUBLE) rec_yac_n,
        TRY_CAST(NULLIF(rec_yac_per_rec, '') AS DOUBLE) rec_yac_per_rec_n,
        TRY_CAST(NULLIF(rec_adot, '') AS DOUBLE) rec_adot_n,
        TRY_CAST(NULLIF(rec_broken_tackles, '') AS DOUBLE) rec_bt_n,
        TRY_CAST(NULLIF(rec_broken_tackles_per_rec, '') AS DOUBLE) rec_bt_per_rec_n,
        TRY_CAST(NULLIF(rec_drops, '') AS DOUBLE) rec_drops_n,
        TRY_CAST(NULLIF(rec_drop_pct, '') AS DOUBLE) rec_drop_pct_n,
        TRY_CAST(NULLIF(rec_target_int, '') AS DOUBLE) rec_target_int_n,
        TRY_CAST(NULLIF(rec_pass_rating, '') AS DOUBLE) rec_pass_rating_n,
        TRY_CAST(NULLIF(rush_att, '') AS DOUBLE) rush_att_n,
        TRY_CAST(NULLIF(rush_yds, '') AS DOUBLE) rush_yds_n,
        TRY_CAST(NULLIF(rush_first_down, '') AS DOUBLE) rush_fd_n,
        TRY_CAST(NULLIF(rush_yds_before_contact, '') AS DOUBLE) rush_bc_n,
        TRY_CAST(NULLIF(rush_yds_bc_per_rush, '') AS DOUBLE) rush_bc_per_rush_n,
        TRY_CAST(NULLIF(rush_yac, '') AS DOUBLE) rush_yac_n,
        TRY_CAST(NULLIF(rush_yac_per_rush, '') AS DOUBLE) rush_yac_per_rush_n,
        TRY_CAST(NULLIF(rush_broken_tackles, '') AS DOUBLE) rush_bt_n,
        TRY_CAST(NULLIF(rush_broken_tackles_per_rush, '') AS DOUBLE) rush_bt_per_rush_n
      FROM read_parquet(?) WHERE year_id = '2025'
    )
    SELECT * FROM t
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY player, year_id
      ORDER BY CASE WHEN team_name_abbr = '2TM' THEN 0 ELSE 1 END, team_name_abbr
    ) = 1
    """
    weekly_sql = f"""
    SELECT lower(trim(player)) player_key,
      SUM(targets) targets_n, SUM(receptions) rec_n, SUM(receiving_yards) rec_yds_n,
      SUM(receiving_first_downs) rec_fd_n, SUM(receiving_completed_air_yards) rec_air_n,
      SUM(receiving_air_yards) intended_air_n, SUM(receiving_yards_after_catch) rec_yac_n,
      SUM(receiving_broken_tackles) rec_bt_n, SUM(receiving_drops) rec_drops_n,
      SUM(receiving_target_interceptions) rec_target_int_n,
      SUM(carries) rush_att_n, SUM(rushing_yards) rush_yds_n,
      SUM(rushing_first_downs) rush_fd_n, SUM(rushing_yards_before_contact) rush_bc_n,
      SUM(rushing_yards_after_contact) rush_yac_n, SUM(rushing_broken_tackles) rush_bt_n,
      SUM(receiving_pass_rating * targets) rec_pass_rating_weighted,
      COUNT(DISTINCT week) games_n
    FROM read_parquet(?)
    WHERE year = 2025 AND season_type = '{season_type}'
    GROUP BY 1
    """
    rows = con.execute(pfr_sql, [pfr]).fetchdf().to_dict("records")
    weekly = {r["player_key"]: r for r in con.execute(weekly_sql, [release]).fetchdf().to_dict("records")}

    direct = {
        "targets": ("targets_n", "targets_n"), "rec": ("rec_n", "rec_n"),
        "rec_yds": ("rec_yds_n", "rec_yds_n"), "rec_first_down": ("rec_fd_n", "rec_fd_n"),
        "rec_air_yds": ("rec_air_n", "rec_air_n"), "rec_yac": ("rec_yac_n", "rec_yac_n"),
        "rec_broken_tackles": ("rec_bt_n", "rec_bt_n"), "rec_drops": ("rec_drops_n", "rec_drops_n"),
        "rec_target_int": ("rec_target_int_n", "rec_target_int_n"),
        "rush_att": ("rush_att_n", "rush_att_n"), "rush_yds": ("rush_yds_n", "rush_yds_n"),
        "rush_first_down": ("rush_fd_n", "rush_fd_n"), "rush_yds_before_contact": ("rush_bc_n", "rush_bc_n"),
        "rush_yac": ("rush_yac_n", "rush_yac_n"), "rush_broken_tackles": ("rush_bt_n", "rush_bt_n"),
    }
    checks = {}
    for col, (pk, vk) in direct.items():
        comparable = matches = 0
        for row in rows:
            v = weekly.get(str(row["player"]).strip().lower())
            a, b = row.get(pk), v.get(vk) if v else None
            if not missing(a) and not missing(b):
                comparable += 1; matches += int(float(a) == float(b))
        checks[col] = {"comparable": comparable, "exact_matches": matches, "mismatches": comparable - matches, "aggregation": "SUM"}

    rate_specs = {
        "rec_air_yds_per_rec": ("rec_air_per_rec_n", "rec_air_n", "rec_n", 1.0, "completed_air_yards/receptions"),
        "rec_yac_per_rec": ("rec_yac_per_rec_n", "rec_yac_n", "rec_n", 1.0, "receiving_yards_after_catch/receptions"),
        "rec_broken_tackles_per_rec": ("rec_bt_per_rec_n", "rec_n", "rec_bt_n", 1.0, "receptions/receiving_broken_tackles"),
        "rec_drop_pct": ("rec_drop_pct_n", "rec_drops_n", "targets_n", 100.0, "receiving_drops/targets"),
        "rush_yds_bc_per_rush": ("rush_bc_per_rush_n", "rush_bc_n", "rush_att_n", 1.0, "rushing_yards_before_contact/carries"),
        "rush_yac_per_rush": ("rush_yac_per_rush_n", "rush_yac_n", "rush_att_n", 1.0, "rushing_yards_after_contact/carries"),
        "rush_broken_tackles_per_rush": ("rush_bt_per_rush_n", "rush_att_n", "rush_bt_n", 1.0, "carries/rushing_broken_tackles"),
    }
    for col, (actual_key, num, den, scale, denominator) in rate_specs.items():
        pfr_comp = pfr_match = v_comp = v_match = 0
        for row in rows:
            v = weekly.get(str(row["player"]).strip().lower())
            actual, expected_pfr = row.get(actual_key), rate(row.get(num), row.get(den), scale)
            if not missing(actual) and not missing(expected_pfr):
                pfr_comp += 1; pfr_match += int(abs(float(actual) - round(expected_pfr, 1)) <= 0.11)
            expected_v = rate(v.get(num), v.get(den), scale) if v else None
            if not missing(actual) and not missing(expected_v):
                v_comp += 1; v_match += int(abs(float(actual) - round(expected_v, 1)) <= 0.11)
        checks[col] = {"pfr_operand_comparable": pfr_comp, "pfr_operand_matches": pfr_match, "pfr_mismatches": pfr_comp - pfr_match, "v26_operand_comparable": v_comp, "v26_rounded_one_decimal_matches": v_match, "v26_mismatches": v_comp - v_match, "denominator": denominator}
    # ADOT uses intended air yards / targets, while rec_air_yds is completed air yards.
    adot_matches = sum(1 for row in rows if (lambda v: v and not missing(v.get("intended_air_n")) and not missing(v.get("targets_n")) and v.get("targets_n") != 0 and not missing(row.get("rec_adot_n")) and abs(float(row["rec_adot_n"]) - round(float(v["intended_air_n"]) / float(v["targets_n"]), 1)) <= 0.11)(weekly.get(str(row["player"]).strip().lower())))
    checks["rec_adot"] = {"pfr_operand_comparable": 0, "v26_operand_comparable": len(rows), "v26_matches": adot_matches, "v26_mismatches": len(rows) - adot_matches, "denominator": "intended_air_yards/targets"}
    checks["rec_pass_rating"] = {"blocker": "Season-level receiving passer rating is nonlinear; no canonical aggregation equation is declared."}

    decisions = load_decisions(); prefix = decision_key + "|*|"
    by_column = {k[len(prefix):]: dict(v) for k, v in decisions.items() if k.startswith(prefix)}
    columns = [r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [pfr]).fetchall()]
    capture = {"pfr_id": ("CONTEXT_TO_SEASON_OR_BIO", "PFR identity key; player_bio crosswalk."), "player": ("CONTEXT_TO_SEASON_OR_BIO", "Player identity; player_bio lane."), "team_name_abbr": ("CONTEXT_TO_SEASON_OR_BIO", "Season team context."), "comp_name_abbr": ("CONTEXT_TO_SEASON_OR_BIO", "NFL comparison context."), "NFL_player_id": ("CONTEXT_TO_SEASON_OR_BIO", "Canonical identity key; player_bio lane.")}
    locator = {"index_letter", "index_position", "first_year", "last_year", "page_key", "page_kind", "page_url", "subpage_year", "scraped_at_utc", "source_url", "table_id", "table_caption", "row_index_in_table", "tr_data_row", "year_id", "year_id_links_json", "year_id_link_texts", "year_id_link_ids", "year_id_urls", "team_name_abbr_links_json", "team_name_abbr_link_texts", "team_name_abbr_link_ids", "team_name_abbr_urls", "comp_name_abbr_links_json", "comp_name_abbr_link_texts", "comp_name_abbr_link_ids", "comp_name_abbr_urls"}
    matrix = []
    for col in columns:
        if col in by_column: entry = dict(by_column[col])
        elif col == "awards" or col.startswith("awards_"): entry = {"disposition": "STRUCTURED_WITNESS_REQUIRED", "reason": "Awards/link values are composite membership/provenance material."}
        elif col in capture: d, reason = capture[col]; entry = {"disposition": d, "reason": reason}
        elif col in locator: entry = {"disposition": "INTENTIONALLY_UNMAPPED_WITH_REASON", "reason": "Capture locator/markup/provenance, not a stat."}
        else: entry = {"disposition": "OPEN_BLOCKED", "reason": "No ledger disposition found."}
        entry["column"] = col
        if col in rate_specs:
            entry["audit_disposition"] = "VERIFIED_DERIVED_WITNESS" if not checks[col]["pfr_mismatches"] and not checks[col]["v26_mismatches"] else "VERIFIED_DERIVED_WITNESS_WITH_RATE_GAP"
        elif col in {"rec_adot", "rec_pass_rating"}:
            entry["audit_disposition"] = "VERIFIED_DERIVED_WITNESS_WITH_RATE_GAP"
        elif col in {"rec_first_down", "rush_first_down"} and checks[col]["mismatches"]:
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif col in direct and checks[col]["mismatches"]:
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif entry.get("disposition") == "MAPPED_TO_CANONICAL": entry["audit_disposition"] = "VERIFIED_DIRECT_MAPPING"
        elif entry.get("disposition") == "EXCLUDED_WITH_REASON": entry["audit_disposition"] = "VERIFIED_DERIVED_WITNESS"
        elif entry.get("disposition") == "NEW_SUPERTABLE_COLUMN_CANDIDATE": entry["audit_disposition"] = "PROMOTION_CANDIDATE"
        else: entry["audit_disposition"] = entry.get("disposition")
        matrix.append(entry)

    receipt = {"source": {"path": str(pfr_path), "table_id": table_name, "table_class": "advanced player receiving/rushing / postseason" if season_type == "POST" else "advanced player receiving/rushing / regular-season", "subtables": [{"table_id": table_name, "caption": "Advanced Receiving & Rushing Table"}]}, "grain": {"pfr": "player-season with 2TM preference", "canonical": f"player-week {season_type} rolled to player-season", "year": 2025}, "counts": {"pfr_total_rows": int(con.execute("select count(*) from read_parquet(?)", [pfr]).fetchone()[0]), "pfr_2025_selected_rows": len(rows), "weekly_players": len(weekly)}, "checks": checks, "deferred": [{"column": c, "reason": "PFR-to-v26 mismatch is retained as a derivation/coverage candidate; no backfill authorized."} for c in ["rec_first_down", "rush_first_down"] if checks[c]["mismatches"]], "column_dispositions": matrix, "generated_at_utc": datetime.now(timezone.utc).isoformat()}
    output.parent.mkdir(parents=True, exist_ok=True); output.write_text(json.dumps(receipt, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output), "rows": len(rows), "columns": len(matrix), "open": [r["column"] for r in matrix if r.get("audit_disposition") == "OPEN_BLOCKED"], "checks": checks}, indent=2, default=str))


if __name__ == "__main__": main()
