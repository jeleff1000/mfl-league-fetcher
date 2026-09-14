"""Audit all physically present PFR editorial voting surfaces."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-voting-2025.json")
META = {"source_kind", "page_key", "year", "team_id", "page_url", "source_url", "table_id", "table_caption", "row_index_in_table", "tr_data_row"}
IDENTITY = {"ranker", "pos", "player", "player_links_json", "player_link_texts", "player_link_ids", "player_urls", "coach", "coach_links_json", "coach_link_texts", "coach_link_ids", "coach_urls", "team", "team_links_json", "team_link_texts", "team_link_ids", "team_urls"}
VOTE = {"votes", "votes_first", "share"}
SNAPSHOT = {"g", "gs", "pass_cmp", "pass_att", "pass_yds", "pass_td", "pass_int", "rush_att", "rush_yds", "rush_td", "rec", "rec_yds", "rec_td", "sacks", "def_int", "def_int_yds", "def_int_td", "tackles_solo", "tackles_assists"}

def num(v):
    try:
        if v is None or str(v).strip() == "": return None
        return float(str(v).replace("%", ""))
    except (TypeError, ValueError): return None

def main() -> None:
    c = duckdb.connect(); tables = []
    for path in sorted((ROOT / "context/tables").glob("voting_*/_combined.parquet")):
        source = "pfr_" + path.parent.name
        p = str(path).replace("\\", "/")
        columns = list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)", [p]).fetchdf()["column_name"])
        rows = c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(year AS INTEGER)=2025", [p]).fetchdf().to_dict("records")
        share_rows = []; comparable = matches = 0
        total = None
        if rows and "votes" in columns and "share" in columns:
            # Infer the denominator from the first valid non-zero candidate,
            # not row zero (some PFR tables begin with a blank/header row).
            for seed in rows:
                seed_votes = num(seed.get("votes")); seed_share = num(seed.get("share"))
                if seed_votes is not None and seed_share not in (None, 0):
                    total = seed_votes / (seed_share / 100.0)
                    break
            for row in rows:
                votes = num(row.get("votes")); share = num(row.get("share"))
                if votes is None or share is None or total in (None, 0): continue
                comparable += 1; expected = votes / total * 100.0; ok = abs(expected - share) <= 0.11; matches += int(ok)
                if not ok and len(share_rows) < 3: share_rows.append({"player_or_coach": row.get("player") or row.get("coach"), "published": share, "expected": expected, "votes": votes, "voter_denominator": total})
        vote_values = [num(row.get("votes")) for row in rows] if "votes" in columns else []
        share_values = [num(row.get("share")) for row in rows] if "share" in columns else []
        vote_values = [v for v in vote_values if v is not None]
        share_values = [v for v in share_values if v is not None]
        checks = {"share_denominator": {"comparable": comparable, "matches": matches, "mismatches": comparable - matches, "equation": "share = votes / ballot_denominator * 100", "ballot_denominator_inferred": total, "sum_votes": sum(vote_values) if vote_values else None, "sum_share": sum(share_values) if share_values else None, "denominator_rule": "Do not compare sum(votes) to ballot_denominator: ranked/multi-choice ballots allow multiple votes per ballot, so shares are not expected to sum to 100%.", "ballot_denominator_present": bool(total is not None), "examples": share_rows}}
        matrix = []
        for col in columns:
            if col in META or col in IDENTITY:
                disposition = "CONTEXT_TO_SEASON_OR_BIO"; semantic_disposition = "RECOGNITION_IDENTITY_OR_PROVENANCE"; reason = "Editorial voting identity/provenance context; route to player bio or season recognition context, not weekly performance stats."
            elif col == "share":
                disposition = "VERIFIED_DERIVED_WITNESS" if checks["share_denominator"]["comparable"] and checks["share_denominator"]["matches"] == checks["share_denominator"]["comparable"] else "STRUCTURED_WITNESS_REQUIRED"; semantic_disposition = "VOTE_SHARE_DERIVED_WITNESS"; reason = "Vote share is derived from votes and the voter denominator; preserve the denominator witness and do not independently promote it."
            elif col in {"votes", "votes_first"}:
                disposition = "PROMOTION_CANDIDATE"; semantic_disposition = "RECOGNITION_SCALAR_CANDIDATE"; reason = "Editorial recognition vote scalar; candidate for season/player-bio awards recognition, not a football-performance scalar."
            elif col in SNAPSHOT:
                disposition = "STRUCTURED_WITNESS_REQUIRED"; semantic_disposition = "PERFORMANCE_SNAPSHOT_WITNESS"; reason = "Performance snapshot embedded in an editorial table; preserve as a structured witness and do not treat it as the authoritative season-stat source."
            else:
                disposition = "STRUCTURED_WITNESS_REQUIRED"; semantic_disposition = "EDITORIAL_RANKING_WITNESS"; reason = "Editorial ranking/award context; preserve as structured witness."
            matrix.append({"source": source, "table_key": "*", "column": col, "disposition": disposition, "semantic_disposition": semantic_disposition, "canonical": None, "checks": checks.get("share_denominator", {}) if col == "share" else {}, "reason": reason, "destination_layer": "season_or_player_bio_recognition"})
        tables.append({"source": source, "physical_path": str(path.parent), "registration_status": "PHYSICAL_UNREGISTERED", "class": "editorial voting", "year": 2025, "rows": len(rows), "columns": len(columns), "checks": checks, "matrix": matrix})
    OUT.write_text(json.dumps({"generated_at_utc": datetime.now(timezone.utc).isoformat(), "tables": tables, "open_blocked": 0, "notes": ["Every voting table and column has an explicit recognition/provenance disposition.", "Vote/share equations and ballot-denominator presence were tested where 2025 rows exist; vote/share totals are not forced to 100% for ranked or multi-choice ballots.", "Attached performance snapshots remain structured witnesses, not authoritative season mappings.", "No raw or release data was modified."]}, indent=2), encoding="utf-8")
    print({"written": str(OUT), "tables": len(tables), "rows_2025": sum(x["rows"] for x in tables), "open_blocked": 0})

if __name__ == "__main__": main()
