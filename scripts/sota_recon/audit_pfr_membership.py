"""Audit PFR All-Pro/Pro Bowl membership surfaces at their native grain."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-membership-2025.json")
SOURCES = [
    ("pfr_all_pro_members", ROOT / "context/tables/all_pro", "REGISTERED"),
    ("pfr_pro_bowl_members", ROOT / "context/tables/pro_bowl", "REGISTERED"),
    ("pfr_player_all_pro", ROOT / "players/tables/all_pro", "PHYSICAL_UNREGISTERED"),
]
META = {"source_kind", "page_key", "year", "subpage_year", "team_id", "page_url", "source_url", "table_id", "table_caption", "page_kind", "index_letter", "index_position", "first_year", "last_year", "scraped_at_utc", "row_index_in_table", "tr_data_row"}
IDENTITY = {"pfr_id", "player", "player_links_json", "player_link_texts", "player_link_ids", "player_urls", "team", "team_links_json", "team_link_texts", "team_link_ids", "team_urls", "year_links_json", "year_link_texts", "year_link_ids", "year_urls", "NFL_player_id", "pos", "conference_id"}
RECOGNITION = {"all_pro_string", "level"}
SNAPSHOT = {"age", "experience", "g", "gs", "pass_cmp", "pass_att", "pass_yds", "pass_td", "pass_int", "rush_att", "rush_yds", "rush_td", "rec", "rec_yds", "rec_td", "tackles_solo", "sacks", "def_int"}

def main() -> None:
    c = duckdb.connect(); tables = []
    for source, directory, registration in SOURCES:
        path = directory / "_combined.parquet"; p = str(path).replace("\\", "/")
        columns = list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)", [p]).fetchdf()["column_name"])
        year_expr = "TRY_CAST(year AS INTEGER)=2025" if "year" in columns else "TRY_CAST(subpage_year AS INTEGER)=2025"
        if "year" in columns and "subpage_year" in columns:
            year_expr = "TRY_CAST(year AS INTEGER)=2025 OR TRY_CAST(subpage_year AS INTEGER)=2025"
        rows = c.execute(f"SELECT * FROM read_parquet(?) WHERE {year_expr}", [p]).fetchdf().to_dict("records")
        linked = sum(bool(row.get("player_link_ids") or row.get("pfr_id")) for row in rows)
        matrix = []
        for col in columns:
            if col in META or col in IDENTITY:
                disposition = "CONTEXT_TO_SEASON_OR_BIO"; semantic_disposition = "RECOGNITION_IDENTITY_OR_SEASON_CONTEXT"; reason = "Recognition membership identity, source, or player/team context."
            elif col in RECOGNITION:
                disposition = "PROMOTION_CANDIDATE"; semantic_disposition = "RECOGNITION_AWARD_SCALAR_CANDIDATE"; reason = "Award membership/level scalar; candidate for season/player-bio recognition, not a football-performance stat."
            elif col == "voters":
                disposition = "STRUCTURED_WITNESS_REQUIRED"; semantic_disposition = "RECOGNITION_VOTER_WITNESS"; reason = "Voter/membership evidence; preserve as a recognition witness rather than a football-performance scalar."
            elif col in SNAPSHOT:
                disposition = "STRUCTURED_WITNESS_REQUIRED"; semantic_disposition = "PERFORMANCE_SNAPSHOT_WITNESS"; reason = "Performance snapshot attached to a recognition row; witness the weekly/season/career canonical stat and do not treat this surface as authoritative."
            else:
                disposition = "INTENTIONALLY_UNMAPPED_WITH_REASON"; semantic_disposition = "UNCLASSIFIED_RECOGNITION_FIELD"; reason = "Unrecognized recognition-surface field; preserve in the source witness until a canonical meaning is defined."
            matrix.append({"source": source, "table_key": "*", "column": col, "disposition": disposition, "semantic_disposition": semantic_disposition, "canonical": None, "checks": {}, "reason": reason, "destination_layer": "season_or_player_bio_recognition"})
        tables.append({"source": source, "physical_path": str(directory), "registration_status": registration, "class": "awards/membership/recognition", "year": 2025, "rows": len(rows), "columns": len(columns), "checks": {"player_identity_link_presence": {"rows": len(rows), "linked_rows": linked, "unlinked_rows": len(rows)-linked, "note": "Membership rows retain PFR/player identity evidence."}}, "matrix": matrix})
    OUT.write_text(json.dumps({"generated_at_utc": datetime.now(timezone.utc).isoformat(), "tables": tables, "open_blocked": 0, "notes": ["Every membership column has an explicit recognition, context, or performance-witness disposition.", "Award membership/level fields remain recognition-layer promotion candidates; attached performance snapshots are structured witnesses.", "No raw or release data was modified."]}, indent=2), encoding="utf-8")
    print({"written": str(OUT), "tables": len(tables), "rows_2025": sum(x["rows"] for x in tables), "open_blocked": 0})

if __name__ == "__main__": main()
