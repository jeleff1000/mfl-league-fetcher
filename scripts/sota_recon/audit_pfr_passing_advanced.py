"""Read-only 2025 receipt for the PFR advanced passing postseason table."""
from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from scripts.sota_recon.column_dossier import load_decisions


PFR = Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\passing_advanced_post\_combined.parquet")
STANDARD = Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\passing_post\_combined.parquet")
RELEASE = Path(r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-player-season-passing-advanced-post-2025.json")
SURFACES = {
    "post": {"pfr": PFR, "standard": STANDARD, "out": OUT, "season_type": "POST", "table_key": "pfr_passing_adv_post", "table_id": "passing_advanced_post"},
    "regular": {
        "pfr": Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\passing_advanced\_combined.parquet"),
        "standard": Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\passing\_combined.parquet"),
        "out": Path(r"D:\yahoo_oauth\docs\audits\pfr-player-season-passing-advanced-2025.json"),
        "season_type": "REG", "table_key": "pfr_passing_adv_season", "table_id": "passing_advanced",
    },
}


def missing(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


def equal(a, b, tolerance=0.0):
    return not missing(a) and not missing(b) and abs(float(a) - float(b)) <= tolerance


def rate(numerator, denominator, scale=1.0):
    if missing(numerator) or missing(denominator) or float(denominator) == 0:
        return None
    return float(numerator) / float(denominator) * scale


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--surface", choices=sorted(SURFACES), default="post")
    args = parser.parse_args()
    surface = SURFACES[args.surface]
    con = duckdb.connect()
    pfr_path, standard_path, output_path = surface["pfr"], surface["standard"], surface["out"]
    season_type = surface["season_type"]
    pfr = str(pfr_path).replace("\\", "/")
    standard = str(standard_path).replace("\\", "/")
    release = str(RELEASE).replace("\\", "/")

    pfr_sql = """
    WITH typed AS (
      SELECT *,
        TRY_CAST(NULLIF(pass_cmp, '') AS DOUBLE) cmp_n,
        TRY_CAST(NULLIF(pass_att, '') AS DOUBLE) att_n,
        TRY_CAST(NULLIF(pass_target_yds, '') AS DOUBLE) target_yds_n,
        TRY_CAST(NULLIF(pass_air_yds, '') AS DOUBLE) air_yds_n,
        TRY_CAST(NULLIF(pass_yac, '') AS DOUBLE) yac_n,
        TRY_CAST(NULLIF(pass_drops, '') AS DOUBLE) drops_n,
        TRY_CAST(NULLIF(pass_poor_throws, '') AS DOUBLE) poor_n,
        TRY_CAST(NULLIF(pass_on_target, '') AS DOUBLE) on_target_n,
        TRY_CAST(NULLIF(pass_pressured, '') AS DOUBLE) pressured_n,
        TRY_CAST(NULLIF(pass_blitzed, '') AS DOUBLE) blitzed_n,
        TRY_CAST(NULLIF(pass_hurried, '') AS DOUBLE) hurried_n,
        TRY_CAST(NULLIF(pass_hits, '') AS DOUBLE) hits_n,
        TRY_CAST(NULLIF(rush_scrambles, '') AS DOUBLE) scrambles_n,
        TRY_CAST(NULLIF(pass_throwaways, '') AS DOUBLE) throwaways_n,
        TRY_CAST(NULLIF(pass_air_yds_per_cmp, '') AS DOUBLE) air_per_cmp_n,
        TRY_CAST(NULLIF(pass_air_yds_per_att, '') AS DOUBLE) air_per_att_n,
        TRY_CAST(NULLIF(pass_yac_per_cmp, '') AS DOUBLE) yac_per_cmp_n,
        TRY_CAST(NULLIF(pass_drop_pct, '') AS DOUBLE) drop_pct_n,
        TRY_CAST(NULLIF(pass_poor_throw_pct, '') AS DOUBLE) poor_pct_n,
        TRY_CAST(NULLIF(pass_on_target_pct, '') AS DOUBLE) on_target_pct_n,
        TRY_CAST(NULLIF(pass_pressured_pct, '') AS DOUBLE) pressured_pct_n,
        TRY_CAST(NULLIF(pass_tgt_yds_per_att, '') AS DOUBLE) target_per_att_n,
        TRY_CAST(NULLIF(rush_scrambles_yds_per_att, '') AS DOUBLE) scramble_yds_per_att_n
      FROM read_parquet(?) WHERE year_id = '2025'
    )
    SELECT * FROM typed
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY player, year_id
      ORDER BY CASE WHEN team_name_abbr = '2TM' THEN 0 ELSE 1 END, team_name_abbr
    ) = 1
    """
    weekly_sql = f"""
    SELECT lower(trim(player)) player_key,
      SUM(completions) cmp_n, SUM(attempts) att_n,
      SUM(passing_air_yards) target_yds_n,
      SUM(passing_completed_air_yards) air_yds_n,
      SUM(passing_yards_after_catch) yac_n,
      SUM(passing_drops) drops_n, SUM(passing_poor_throws) poor_n,
      NULL::DOUBLE AS on_target_n, SUM(passing_pressured) pressured_n,
      SUM(passing_blitzed) blitzed_n, SUM(passing_hurried) hurried_n,
      SUM(passing_hits) hits_n, SUM(rushing_scrambles) scrambles_n,
      SUM(dropbacks) dropbacks_n, COUNT(DISTINCT week) games_n
    FROM read_parquet(?)
    WHERE year = 2025 AND season_type = '{season_type}'
    GROUP BY 1
    """
    # PFR's pressure percentage denominator is dropbacks; standard PFR supplies sacks
    # for the source-side denominator where the advanced table omits them.
    standard_sql = """
    SELECT lower(trim(player)) player_key,
      SUM(TRY_CAST(NULLIF(pass_att, '') AS DOUBLE)) att_n,
      SUM(TRY_CAST(NULLIF(pass_sacked, '') AS DOUBLE)) sacks_n
    FROM read_parquet(?) WHERE year_id = '2025'
    GROUP BY 1
    """
    rows = con.execute(pfr_sql, [pfr]).fetchdf().to_dict("records")
    weekly = {r["player_key"]: r for r in con.execute(weekly_sql, [release]).fetchdf().to_dict("records")}
    std = {r["player_key"]: r for r in con.execute(standard_sql, [standard]).fetchdf().to_dict("records")}

    direct = {
        "pass_cmp": ("cmp_n", "cmp_n"), "pass_att": ("att_n", "att_n"),
        "pass_target_yds": ("target_yds_n", "target_yds_n"),
        "pass_air_yds": ("air_yds_n", "air_yds_n"), "pass_yac": ("yac_n", "yac_n"),
        "pass_drops": ("drops_n", "drops_n"), "pass_poor_throws": ("poor_n", "poor_n"),
        "pass_pressured": ("pressured_n", "pressured_n"), "pass_blitzed": ("blitzed_n", "blitzed_n"),
        "pass_hurried": ("hurried_n", "hurried_n"), "pass_hits": ("hits_n", "hits_n"),
        "rush_scrambles": ("scrambles_n", "scrambles_n"),
    }
    checks = {}
    for column, (pkey, vkey) in direct.items():
        comparable = matches = 0
        for row in rows:
            v = weekly.get(str(row["player"]).strip().lower())
            a, b = row.get(pkey), v.get(vkey) if v else None
            if not missing(a) and not missing(b):
                comparable += 1
                matches += int(equal(a, b))
        checks[column] = {"comparable": comparable, "exact_matches": matches,
                          "mismatches": comparable - matches, "aggregation": "SUM"}

    rate_specs = {
        "pass_air_yds_per_cmp": ("air_per_cmp_n", "air_yds_n", "cmp_n", 1.0, "passing_completed_air_yards/completions"),
        "pass_air_yds_per_att": ("air_per_att_n", "air_yds_n", "att_n", 1.0, "passing_completed_air_yards/attempts"),
        "pass_yac_per_cmp": ("yac_per_cmp_n", "yac_n", "cmp_n", 1.0, "passing_yards_after_catch/completions"),
        "pass_drop_pct": ("drop_pct_n", "drops_n", "att_minus_throwaways_n", 100.0, "passing_drops/(attempts-throwaways)"),
        "pass_poor_throw_pct": ("poor_pct_n", "poor_n", "att_minus_throwaways_n", 100.0, "passing_poor_throws/(attempts-throwaways)"),
        "pass_on_target_pct": ("on_target_pct_n", "on_target_n", "att_minus_throwaways_n", 100.0, "pass_on_target/(attempts-throwaways)"),
        "pass_pressured_pct": ("pressured_pct_n", "pressured_n", "dropbacks_n", 100.0, "passing_pressured/dropbacks"),
        "pass_tgt_yds_per_att": ("target_per_att_n", "target_yds_n", "att_n", 1.0, "passing_air_yards/attempts"),
    }
    for column, (actual_key, num, den, scale, denominator_note) in rate_specs.items():
        pfr_self = pfr_matches = v_comparable = v_matches = 0
        for row in rows:
            v = weekly.get(str(row["player"]).strip().lower())
            actual = row.get(actual_key)
            source_num = row.get({"pass_tgt_yds_per_att": "target_yds_n"}.get(column, num))
            source_den = (float(row.get("att_n")) - float(row.get("throwaways_n"))) if den == "att_minus_throwaways_n" and not missing(row.get("att_n")) and not missing(row.get("throwaways_n")) else row.get(den)
            if column == "pass_pressured_pct":
                source = std.get(str(row["player"]).strip().lower())
                source_den = (float(source.get("att_n")) + float(source.get("sacks_n"))) if source and not missing(source.get("att_n")) and not missing(source.get("sacks_n")) else None
            expected_source = rate(source_num, source_den, scale)
            if not missing(actual) and not missing(expected_source):
                pfr_self += 1
                pfr_matches += int(abs(float(actual) - round(expected_source, 1)) <= 0.11)
            expected_v = rate(v.get(num), v.get(den), scale) if v and den != "att_minus_throwaways_n" else None
            if not missing(actual) and not missing(expected_v):
                v_comparable += 1
                v_matches += int(abs(float(actual) - round(expected_v, 1)) <= 0.11)
        checks[column] = {"pfr_operand_comparable": pfr_self, "pfr_operand_matches": pfr_matches,
                          "v26_operand_comparable": v_comparable, "v26_rounded_one_decimal_matches": v_matches,
                          "pfr_mismatches": pfr_self - pfr_matches, "v26_mismatches": v_comparable - v_matches,
                          "denominator": denominator_note}
    checks["rush_scrambles_yds_per_att"] = {"pfr_operand_comparable": 0, "blocker": "The table publishes only the rate; no registered scramble-yards numerator exists in v26 or this PFR table."}

    decisions = load_decisions()
    prefix = surface["table_key"] + "|*|"
    by_column = {k[len(prefix):]: dict(v) for k, v in decisions.items() if k.startswith(prefix)}
    columns = [r[0] for r in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [pfr]).fetchall()]
    capture = {
        "pfr_id": ("CONTEXT_TO_SEASON_OR_BIO", "PFR identity key; player_bio crosswalk."),
        "player": ("CONTEXT_TO_SEASON_OR_BIO", "Player identity; player_bio lane."),
        "team_name_abbr": ("CONTEXT_TO_SEASON_OR_BIO", "Season team context."),
        "comp_name_abbr": ("CONTEXT_TO_SEASON_OR_BIO", "NFL comparison context."),
        "NFL_player_id": ("CONTEXT_TO_SEASON_OR_BIO", "Canonical identity key; player_bio identity lane."),
    }
    locator = {"index_letter", "index_position", "first_year", "last_year", "page_key", "page_kind", "page_url", "subpage_year", "scraped_at_utc", "source_url", "table_id", "table_caption", "row_index_in_table", "tr_data_row", "year_id", "year_id_links_json", "year_id_link_texts", "year_id_link_ids", "year_id_urls", "team_name_abbr_links_json", "team_name_abbr_link_texts", "team_name_abbr_link_ids", "team_name_abbr_urls", "comp_name_abbr_links_json", "comp_name_abbr_link_texts", "comp_name_abbr_link_ids", "comp_name_abbr_urls"}
    matrix = []
    for column in columns:
        if column in by_column:
            entry = dict(by_column[column])
        elif column == "awards" or column.startswith("awards_"):
            entry = {"disposition": "STRUCTURED_WITNESS_REQUIRED", "reason": "Awards and links are composite membership/provenance material."}
        elif column in capture:
            d, reason = capture[column]; entry = {"disposition": d, "reason": reason}
        elif column in locator:
            entry = {"disposition": "INTENTIONALLY_UNMAPPED_WITH_REASON", "reason": "Capture locator/markup/provenance, not a stat."}
        else:
            entry = {"disposition": "OPEN_BLOCKED", "reason": "No ledger disposition found."}
        entry["column"] = column
        if column in {"pass_batted_passes", "pass_throwaways", "pass_spikes", "pass_on_target", "pass_play_action", "pass_play_action_pass_yds", "pass_rpo", "pass_rpo_yds", "pass_rpo_pass_att", "pass_rpo_pass_yds", "pass_rpo_rush_att", "pass_rpo_rush_yds", "pocket_time"}:
            entry["audit_disposition"] = "PROMOTION_CANDIDATE"
        elif column == "pass_yac" and (checks[column]["mismatches"] or checks[column]["comparable"] == 0):
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif column == "pass_air_yds" and checks[column]["mismatches"]:
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif column == "pass_target_yds" and checks[column]["mismatches"]:
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif column in rate_specs and (checks[column].get("v26_mismatches", 0) or checks[column].get("pfr_mismatches", 0)):
            entry["audit_disposition"] = "VERIFIED_DERIVED_WITNESS_WITH_RATE_GAP"
        elif column == "rush_scrambles_yds_per_att":
            entry["audit_disposition"] = "PROMOTION_CANDIDATE"
        elif entry.get("disposition") == "MAPPED_TO_CANONICAL":
            entry["audit_disposition"] = "VERIFIED_DIRECT_MAPPING"
        elif entry.get("disposition") == "EXCLUDED_WITH_REASON":
            entry["audit_disposition"] = "VERIFIED_DERIVED_WITNESS"
        elif entry.get("disposition") == "NEW_SUPERTABLE_COLUMN_CANDIDATE":
            entry["audit_disposition"] = "PROMOTION_CANDIDATE"
        elif entry.get("disposition") in {"STRUCTURED_WITNESS_REQUIRED", "CONTEXT_TO_SEASON_OR_BIO", "INTENTIONALLY_UNMAPPED_WITH_REASON", "OPEN_BLOCKED"}:
            entry["audit_disposition"] = entry["disposition"]
        matrix.append(entry)

    receipt = {
        "source": {"path": str(pfr_path), "table_id": surface["table_id"], "table_class": "advanced player passing / postseason" if season_type == "POST" else "advanced player passing / regular-season", "subtables": [{"table_id": surface["table_id"], "caption": "Advanced Passing Table"}]},
        "grain": {"pfr": "player-season with 2TM preference", "canonical": f"player-week {season_type} rolled to player-season", "year": 2025},
        "counts": {"pfr_total_rows": int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [pfr]).fetchone()[0]), "pfr_2025_selected_rows": len(rows), "weekly_players": len(weekly), "standard_pfr_denominator_source": str(standard_path)},
        "checks": checks,
        "deferred": [{"column": "pass_yac", "reason": "v26 passing_yards_after_catch is NULL for the compared postseason rows; no release backfill authorized."}, {"column": "pass_target_yds", "reason": "Three 2025 rows differ from v26 passing_air_yards; the field remains semantically plausible as intended-air-yard material, but requires source/derivation adjudication before equality can be claimed."}],
        "column_dispositions": matrix,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(receipt, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output_path), "rows": len(rows), "checks": checks, "columns": len(matrix), "open": [r["column"] for r in matrix if r.get("audit_disposition") == "OPEN_BLOCKED"]}, indent=2, default=str))


if __name__ == "__main__":
    main()
