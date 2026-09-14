"""Produce the evidence receipt for PFR's 2025 receiving/rushing season table.

This is intentionally read-only.  It compares the PFR season grain with REG weekly
rollups from the v26 release, and checks published rates from their operands.  It does
not repair the release; first-down gaps are recorded as deferred derivation work.
"""
from __future__ import annotations

import json
import math
import argparse
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from scripts.sota_recon.column_dossier import load_decisions


PFR = Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\receiving_and_rushing\_combined.parquet")
RELEASE = Path(r"D:\league-history-data\nfl\releases\nfl_local_release_franchise_backfill_20260617T122657Z_v26\tables\nfl_player_stats_all.parquet")
PBP = Path(r"D:\league-history-data\nfl\raw\stathead\generated\pbp_merged_1978_2025\nfl_pbp_1978_2025_merged.parquet")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-player-season-rec-rush-2025.json")
SURFACES = {
    "rec_rush": {
        "pfr": PFR, "out": OUT, "table_key": "pfr_player_season_rec_rush",
        "table_id": "receiving_and_rushing", "caption": "Receiving & Rushing Table",
        "season_type": "REG", "table_class": "regular-season player totals",
        "decision_key": "pfr_player_season_rec_rush",
    },
    "rush_rec": {
        "pfr": Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\rushing_and_receiving\_combined.parquet"),
        "out": Path(r"D:\yahoo_oauth\docs\audits\pfr-player-season-rush-rec-2025.json"),
        "table_key": "pfr_player_season_rush_rec", "table_id": "rushing_and_receiving",
        "caption": "Rushing & Receiving Table", "season_type": "REG",
        "table_class": "regular-season player totals",
        "decision_key": "pfr_player_season_rush_rec",
    },
    "rec_rush_post": {
        "pfr": Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\receiving_and_rushing_post\_combined.parquet"),
        "out": Path(r"D:\yahoo_oauth\docs\audits\pfr-player-season-rec-rush-post-2025.json"),
        "table_key": "pfr_player_season_rec_rush_post", "table_id": "receiving_and_rushing_post",
        "caption": "Receiving & Rushing Table", "season_type": "POST",
        "table_class": "postseason player totals",
        "decision_key": "pfr_player_season_rec_rush",
    },
    "rush_rec_post": {
        "pfr": Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\rushing_and_receiving_post\_combined.parquet"),
        "out": Path(r"D:\yahoo_oauth\docs\audits\pfr-player-season-rush-rec-post-2025.json"),
        "table_key": "pfr_player_season_rush_rec_post", "table_id": "rushing_and_receiving_post",
        "caption": "Rushing & Receiving Table", "season_type": "POST",
        "table_class": "postseason player totals",
        "decision_key": "pfr_player_season_rush_rec",
    },
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--surface", choices=sorted(SURFACES), default="rec_rush")
    args = parser.parse_args()
    surface = SURFACES[args.surface]
    source_path = surface["pfr"]
    output_path = surface["out"]
    con = duckdb.connect()
    pfr = str(source_path).replace("\\", "/")
    release = str(RELEASE).replace("\\", "/")
    pfr_sql = """
    WITH typed AS (
      SELECT *,
        TRY_CAST(NULLIF(rec, '') AS DOUBLE) AS rec_n,
        TRY_CAST(NULLIF(rec_yds, '') AS DOUBLE) AS rec_yds_n,
        TRY_CAST(NULLIF(rec_td, '') AS DOUBLE) AS rec_td_n,
        TRY_CAST(NULLIF(rec_long, '') AS DOUBLE) AS rec_long_n,
        TRY_CAST(NULLIF(rush_att, '') AS DOUBLE) AS rush_att_n,
        TRY_CAST(NULLIF(rush_yds, '') AS DOUBLE) AS rush_yds_n,
        TRY_CAST(NULLIF(rush_td, '') AS DOUBLE) AS rush_td_n,
        TRY_CAST(NULLIF(rush_long, '') AS DOUBLE) AS rush_long_n,
        TRY_CAST(NULLIF(touches, '') AS DOUBLE) AS touches_n,
        TRY_CAST(NULLIF(yds_from_scrimmage, '') AS DOUBLE) AS scrimmage_yds_n,
        TRY_CAST(NULLIF(rush_receive_td, '') AS DOUBLE) AS scrimmage_td_n,
        TRY_CAST(NULLIF(fumbles, '') AS DOUBLE) AS fumbles_n,
        TRY_CAST(NULLIF(targets, '') AS DOUBLE) AS targets_n,
        TRY_CAST(NULLIF(rec_first_down, '') AS DOUBLE) AS rec_fd_n,
        TRY_CAST(NULLIF(rush_first_down, '') AS DOUBLE) AS rush_fd_n,
        TRY_CAST(NULLIF(rec_success, '') AS DOUBLE) AS rec_success_n,
        TRY_CAST(NULLIF(rec_yds_per_rec, '') AS DOUBLE) AS rec_yds_per_rec_n,
        TRY_CAST(NULLIF(rec_per_g, '') AS DOUBLE) AS rec_per_g_n,
        TRY_CAST(NULLIF(rec_yds_per_g, '') AS DOUBLE) AS rec_yds_per_g_n,
        TRY_CAST(NULLIF(rush_yds_per_g, '') AS DOUBLE) AS rush_yds_per_g_n,
        TRY_CAST(NULLIF(rush_att_per_g, '') AS DOUBLE) AS rush_att_per_g_n,
        TRY_CAST(NULLIF(yds_per_touch, '') AS DOUBLE) AS yds_per_touch_n,
        TRY_CAST(NULLIF(catch_pct, '') AS DOUBLE) AS catch_pct_n,
        TRY_CAST(NULLIF(rec_yds_per_tgt, '') AS DOUBLE) AS rec_yds_per_tgt_n,
        TRY_CAST(NULLIF(rush_success, '') AS DOUBLE) AS rush_success_n,
        TRY_CAST(NULLIF(rush_yds_per_att, '') AS DOUBLE) AS rush_yds_per_att_n,
        TRY_CAST(NULLIF(games, '') AS DOUBLE) AS games_n
      FROM read_parquet(?) WHERE year_id = '2025'
    )
    SELECT * FROM typed
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY player, year_id
      ORDER BY CASE WHEN team_name_abbr = '2TM' THEN 0 ELSE 1 END, team_name_abbr
    ) = 1
    """
    weekly_sql = f"""
    SELECT lower(trim(player)) AS player_key,
      SUM(receptions) AS rec_n, SUM(receiving_yards) AS rec_yds_n,
      SUM(receiving_tds) AS rec_td_n, MAX(receiving_long) AS rec_long_n,
      SUM(carries) AS rush_att_n, SUM(rushing_yards) AS rush_yds_n,
      SUM(rushing_tds) AS rush_td_n, MAX(rushing_long) AS rush_long_n,
      SUM(touches) AS touches_n, SUM(scrimmage_yards) AS scrimmage_yds_n,
      SUM(scrimmage_tds) AS scrimmage_td_n, SUM(fumbles) AS fumbles_n,
      SUM(targets) AS targets_n, SUM(receiving_first_downs) AS rec_fd_n,
      SUM(rushing_first_downs) AS rush_fd_n, SUM(rec_success) AS rec_success_count,
      SUM(rec_success_plays) AS rec_success_den, SUM(rush_success) AS rush_success_count,
      SUM(rush_success_plays) AS rush_success_den, COUNT(DISTINCT week) AS games_n
    FROM read_parquet(?)
    WHERE year = 2025 AND season_type = '{surface['season_type']}'
    GROUP BY 1
    """
    pfr_rows = con.execute(pfr_sql, [pfr]).fetchdf().to_dict("records")
    weekly_rows = {
        row["player_key"]: row
        for row in con.execute(weekly_sql, [release]).fetchdf().to_dict("records")
    }

    def eq(a, b, tolerance=0.0):
        if missing(a) or missing(b):
            return None
        return abs(float(a) - float(b)) <= tolerance

    def missing(value):
        return value is None or (isinstance(value, float) and math.isnan(value))

    def rate(numerator, denominator, scale=1.0):
        if missing(numerator) or missing(denominator) or denominator == 0:
            return None
        return float(numerator) / float(denominator) * scale

    direct = {
        "rec": ("rec_n", "rec_n", 0.0), "rec_yds": ("rec_yds_n", "rec_yds_n", 0.0),
        "rec_td": ("rec_td_n", "rec_td_n", 0.0), "rec_long": ("rec_long_n", "rec_long_n", 0.0),
        "rush_att": ("rush_att_n", "rush_att_n", 0.0), "rush_yds": ("rush_yds_n", "rush_yds_n", 0.0),
        "rush_td": ("rush_td_n", "rush_td_n", 0.0), "rush_long": ("rush_long_n", "rush_long_n", 0.0),
        "touches": ("touches_n", "touches_n", 0.0), "yds_from_scrimmage": ("scrimmage_yds_n", "scrimmage_yds_n", 0.0),
        "rush_receive_td": ("scrimmage_td_n", "scrimmage_td_n", 0.0), "targets": ("targets_n", "targets_n", 0.0),
        "fumbles": ("fumbles_n", "fumbles_n", 0.0),
    }
    checks = {}
    for column, (pkey, vkey, tol) in direct.items():
        comparable = matches = 0
        for row in pfr_rows:
            v = weekly_rows.get(str(row["player"]).strip().lower())
            a, b = row.get(pkey), v.get(vkey) if v else None
            if not missing(a) and not missing(b):
                comparable += 1
                matches += int(eq(a, b, tol))
        checks[column] = {"comparable": comparable, "exact_matches": matches,
                          "mismatches": comparable - matches, "aggregation": "SUM" if column not in {"rec_long", "rush_long"} else "MAX"}

    rate_specs = {
        "rec_yds_per_rec": ("rec_yds_n", "rec_n", 1.0),
        "rec_per_g": ("rec_n", "games_n", 1.0), "rec_yds_per_g": ("rec_yds_n", "games_n", 1.0),
        "rush_yds_per_g": ("rush_yds_n", "games_n", 1.0), "rush_att_per_g": ("rush_att_n", "games_n", 1.0),
        "yds_per_touch": ("scrimmage_yds_n", "touches_n", 1.0), "catch_pct": ("rec_n", "targets_n", 100.0),
        "rec_yds_per_tgt": ("rec_yds_n", "targets_n", 1.0), "rush_yds_per_att": ("rush_yds_n", "rush_att_n", 1.0),
        "rec_success": ("rec_success_count", "rec_success_den", 100.0),
        "rush_success": ("rush_success_count", "rush_success_den", 100.0),
    }
    for column, (num, den, scale) in rate_specs.items():
        comparable = matches = 0
        for row in pfr_rows:
            v = weekly_rows.get(str(row["player"]).strip().lower())
            actual = row.get(column + "_n")
            expected = rate(v.get(num), v.get(den), scale) if v else None
            if not missing(actual) and not missing(expected):
                comparable += 1
                matches += int(abs(float(actual) - round(expected, 1)) <= 0.11)
        checks[column] = {"comparable": comparable, "rounded_one_decimal_matches": matches,
                          "mismatches": comparable - matches, "denominator": den,
                          "status": "PROMOTION_RATE_WITNESS" if column in {"rec_success", "rush_success"} else "DERIVED_WITNESS"}

    decisions = load_decisions()
    prefix = surface["decision_key"] + "|*|"
    by_column = {key[len(prefix):]: dict(entry) for key, entry in decisions.items() if key.startswith(prefix)}
    disposition_rows = []
    # Capture-only fields are still columns and must have a disposition.  They do not
    # become player-stat mappings merely because the scraper preserved them.
    capture_only = {
        "pfr_id": ("CONTEXT_TO_SEASON_OR_BIO", "PFR identity key; witnessed through the player_bio crosswalk."),
        "player": ("CONTEXT_TO_SEASON_OR_BIO", "Display identity; checked through player_bio, not a weekly stat."),
        "NFL_player_id": ("CONTEXT_TO_SEASON_OR_BIO", "Canonical identity key; checked through player_bio identity."),
        "team_name_abbr": ("CONTEXT_TO_SEASON_OR_BIO", "Season team context; checked against season/team context."),
        "comp_name_abbr": ("CONTEXT_TO_SEASON_OR_BIO", "NFL comparison/context label, not a player-week stat."),
    }
    locator = {
        "index_letter", "index_position", "first_year", "last_year", "page_key", "page_kind",
        "page_url", "subpage_year", "scraped_at_utc", "source_url", "table_id", "table_caption",
        "row_index_in_table", "tr_data_row", "year_id", "year_id_links_json", "year_id_link_texts",
        "year_id_link_ids", "year_id_urls", "team_name_abbr_links_json", "team_name_abbr_link_texts",
        "team_name_abbr_link_ids", "team_name_abbr_urls", "comp_name_abbr_links_json",
        "comp_name_abbr_link_texts", "comp_name_abbr_link_ids", "comp_name_abbr_urls",
    }
    for column in [row[0] for row in con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [pfr]).fetchall()]:
        if column in by_column:
            entry = dict(by_column[column])
        elif column in {"awards", "awards_links_json", "awards_link_texts", "awards_link_ids", "awards_urls"}:
            entry = {"disposition": "STRUCTURED_WITNESS_REQUIRED", "reason": "Awards value/link fields carry typed membership facts and provenance; split into structured award witnesses."}
        elif column in capture_only:
            disposition, reason = capture_only[column]
            entry = {"disposition": disposition, "reason": reason}
        elif column in locator:
            entry = {"disposition": "INTENTIONALLY_UNMAPPED_WITH_REASON", "reason": "Capture locator, markup, or provenance field; it is not a player statistic."}
        else:
            entry = {"disposition": "OPEN_BLOCKED", "reason": "No column-level disposition found in the reconciliation ledger."}
        entry["column"] = column
        if column in {"rec_success", "rush_success"}:
            entry["audit_disposition"] = "PROMOTION_CANDIDATE"
        elif column in rate_specs:
            entry["audit_disposition"] = "VERIFIED_DERIVED_WITNESS" if checks[column]["mismatches"] == 0 else "VERIFIED_DERIVED_WITNESS_WITH_RATE_GAP"
        elif column in {"rec_first_down", "rush_first_down"}:
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif column == "rush_long" and checks[column]["mismatches"]:
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif column == "fumbles" and checks[column]["mismatches"]:
            entry["audit_disposition"] = "DEFERRED_BACKFILL_CANDIDATE"
        elif entry.get("disposition") == "MAPPED_TO_CANONICAL":
            entry["audit_disposition"] = "VERIFIED_DIRECT_MAPPING"
        elif entry.get("disposition") == "NEW_SUPERTABLE_COLUMN_CANDIDATE":
            entry["audit_disposition"] = "PROMOTION_CANDIDATE"
        elif entry.get("disposition") == "STRUCTURED_WITNESS_REQUIRED":
            entry["audit_disposition"] = "STRUCTURED_WITNESS_REQUIRED"
        elif entry.get("disposition") == "CONTEXT_TO_SEASON_OR_BIO":
            entry["audit_disposition"] = "CONTEXT_TO_SEASON_OR_BIO"
        elif entry.get("disposition") == "INTENTIONALLY_UNMAPPED_WITH_REASON":
            entry["audit_disposition"] = "INTENTIONALLY_UNMAPPED_WITH_REASON"
        elif entry.get("disposition") == "OPEN_BLOCKED":
            entry["audit_disposition"] = "OPEN_BLOCKED"
        disposition_rows.append(entry)

    receipt = {
        "source": {"path": str(source_path), "table_id": surface["table_id"], "table_class": surface["table_class"], "subtables": [{"table_id": surface["table_id"], "caption": surface["caption"]}]},
        "grain": {"pfr": "player-season with 2TM combined row preferred", "canonical": f"player-week {surface['season_type']} rolled to player-season", "year": 2025},
        "counts": {"pfr_2025_selected_rows": len(pfr_rows), "weekly_players": len(weekly_rows), "pfr_total_rows": int(con.execute("SELECT COUNT(*) FROM read_parquet(?)", [pfr]).fetchone()[0]), "pbp_source": str(PBP)},
        "checks": checks,
        "derivation_gap": {"columns": ["rec_first_down", "rush_first_down"], "canonical": ["receiving_first_downs", "rushing_first_downs"], "disposition": "DEFERRED_BACKFILL_CANDIDATE", "reason": "PFR values agree with sampled raw PBP rollups, while v26 has NULL late-season weekly cells; no release backfill was authorized."},
        "fumble_followup": {"column": "fumbles", "disposition": "DEFERRED_BACKFILL_CANDIDATE_OR_ADJUDICATION", "reason": "The remaining v26 mismatches require a player-identity-aware PBP comparison; do not change the scalar mapping until that trace is complete."},
        "column_dispositions": disposition_rows,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(receipt, indent=2, default=str) + "\n", encoding="utf-8")
    print(json.dumps({"output": str(output_path), "pfr_rows": len(pfr_rows), "checks": checks}, indent=2, default=str))


if __name__ == "__main__":
    main()
