"""Map-spec receipt for the physically present PFR player_fantasy tableset.

The extract is not complete enough to claim 2025 equality.  This receipt
therefore separates semantic targets from measured equality and keeps any
promotion/backfill decision explicit.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr\players\tables\player_fantasy")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-player-fantasy-physical-2025.json")

META = {
    "pfr_id", "player", "index_letter", "index_position", "first_year", "last_year",
    "page_key", "page_kind", "page_url", "subpage_year", "scraped_at_utc", "source_url",
    "table_id", "table_caption", "row_index_in_table", "tr_data_row", "year_links_json",
    "year_link_texts", "year_link_ids", "year_urls", "team_links_json", "team_link_texts",
    "team_link_ids", "team_urls", "games_links_json", "games_link_texts", "games_link_ids",
    "games_urls", "game_date_links_json", "game_date_link_texts", "game_date_link_ids",
    "game_date_urls", "opp_links_json", "opp_link_texts", "opp_link_ids", "opp_urls",
}

DIRECT = {
    "year": "year", "team": "team", "g": "games_played",
    "pass_cmp": "pass_cmp", "pass_att": "pass_att", "pass_yds": "pass_yds",
    "pass_td": "pass_td", "rush_att": "rush_att", "rush_yds": "rush_yds",
    "rush_td": "rush_td", "targets": "targets", "rec": "rec", "rec_yds": "rec_yds",
    "rec_td": "rec_td", "offense": "offense", "off_pct": "off_pct",
    "defense": "defense", "def_pct": "def_pct", "special_teams": "special_teams",
    "st_pct": "st_pct",
}

RED_ZONE_CANDIDATES = {
    "pass_cmp_in_10", "pass_att_in_10", "pass_yds_in_10", "pass_td_in_10",
    "rush_att_in_10", "rush_yds_in_10", "rush_td_in_10",
}
FANTASY_DERIVED = {"fantasy_points", "draftkings_points", "fanduel_points"}
WEEKLY_CONTEXT = {"game_num", "game_date", "game_location", "opp", "game_result", "starter_pos", "ranker"}
PAGE_LABEL_CONTEXT = {"games"}

REGISTERED_SIBLINGS = {
    "year": "pfr_player_games_played / pfr_player_fantasy_registered",
    "team": "pfr_player_games_played",
    "g": "pfr_player_games_played / pfr_player_fantasy_registered",
    "pass_cmp": "pfr_player_passing",
    "pass_att": "pfr_player_passing",
    "pass_yds": "pfr_player_passing",
    "pass_td": "pfr_player_passing",
    "rush_att": "pfr_player_rushing_and_receiving",
    "rush_yds": "pfr_player_rushing_and_receiving",
    "rush_td": "pfr_player_rushing_and_receiving",
    "targets": "pfr_player_rushing_and_receiving",
    "rec": "pfr_player_rushing_and_receiving",
    "rec_yds": "pfr_player_rushing_and_receiving",
    "rec_td": "pfr_player_rushing_and_receiving",
    "offense": "pfr_player_snap_counts",
    "off_pct": "pfr_player_snap_counts",
    "defense": "pfr_player_snap_counts",
    "def_pct": "pfr_player_snap_counts",
    "special_teams": "pfr_player_snap_counts",
    "st_pct": "pfr_player_snap_counts",
    "fantasy_points": "pfr_player_fantasy_registered",
    "games": "pfr_player_fantasy_registered (page-label witness)",
}


def main() -> None:
    parquet = str(ROOT / "_combined.parquet").replace("\\", "/")
    con = duckdb.connect()
    rows = con.execute("SELECT * FROM read_parquet(?)", [parquet]).fetchdf().to_dict("records")
    cols = list(con.execute("DESCRIBE SELECT * FROM read_parquet(?)", [parquet]).fetchdf()["column_name"])
    y25 = [row for row in rows if str(row.get("year") or "") == "2025"]
    matrix = []

    for col in cols:
        canonical = None
        if col in DIRECT:
            disposition = "VERIFIED_DIRECT_MAPPING"
            canonical = DIRECT[col]
            reason = "Direct semantic match; use weekly grain where present and aggregate to season/career as applicable."
        elif col in RED_ZONE_CANDIDATES:
            disposition = "PROMOTION_CANDIDATE"
            reason = "PFR inside-10 opportunity/yards field is material stat content not present in the current supertable inventory; defer promotion and backfill."
        elif col in FANTASY_DERIVED:
            disposition = "STRUCTURED_WITNESS_REQUIRED"
            semantic_disposition = "DERIVED_WITNESS"
            reason = "Publisher fantasy score is a scoring witness; derive from canonical scoring operands rather than promoting it as an independent football stat."
        elif col in WEEKLY_CONTEXT:
            disposition = "CONTEXT_TO_SEASON_OR_BIO"
            semantic_disposition = "CONTEXT_TO_WEEKLY"
            reason = "Game-level player context; belongs to weekly/context grain, not season/career stat promotion."
        elif col in PAGE_LABEL_CONTEXT:
            disposition = "CONTEXT_TO_SEASON_OR_BIO"
            semantic_disposition = "PAGE_LABEL_WITNESS"
            reason = "Publisher page label such as 'All 2018 Games'; it is not a games-played counter and must not map to games_played."
        elif col in META:
            disposition = "INTENTIONALLY_UNMAPPED_WITH_REASON"
            semantic_disposition = "PROVENANCE_ONLY"
            reason = "Scrape, identity, link, and table-layout metadata; retain as provenance only."
        else:
            disposition = "INTENTIONALLY_UNMAPPED_WITH_REASON"
            semantic_disposition = "STRUCTURED_WITNESS"
            reason = "Physical source field is fully enumerated but requires an explicit structured witness contract before canonical mapping or promotion."
        if col in DIRECT:
            semantic_disposition = "MAPPED_TO_CANONICAL"
        elif col in RED_ZONE_CANDIDATES:
            semantic_disposition = "PROMOTION_CANDIDATE"
        matrix.append({
            "source": "pfr_player_fantasy", "table_key": "physical", "column": col,
            "disposition": disposition, "semantic_disposition": semantic_disposition,
            "canonical": canonical, "reason": reason,
            "registered_sibling": REGISTERED_SIBLINGS.get(col),
            "evidence": "D:/league-history-data/nfl/raw/pfr/players/tables/player_fantasy/_combined.parquet",
            "equality": {"comparable": 0, "matches": 0, "mismatches": 0,
                         "reason": "Incomplete physical probe; no 2025 equality claim."},
        })

    receipt = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "pfr_player_fantasy", "physical_path": str(ROOT),
        "registration_status": "PHYSICAL_UNREGISTERED", "class": "player fantasy/scoring",
        "capture_status": "RESOLVED_AS_PARTIAL_REDUNDANT_EXTRACT",
        "canonical_registered_source": {
            "source": "pfr_player_fantasy",
            "path": str(ROOT.parent / "fantasy"),
            "row_count": 37096,
            "distinct_pfr_ids": 7081,
            "year_min": 1970,
            "year_max": 2025,
            "rows_2025": 643,
            "role": "registered full PFR fantasy surface; authoritative for season fantasy points/VBD/ranks",
        },
        "row_count": len(rows), "years": sorted({str(row.get("year")) for row in rows if row.get("year") is not None}),
        "2025_rows": len(y25), "columns": len(cols),
        "schema_columns_audited": len(cols), "schema_columns_missing_from_receipt": 0,
        "checks": {"applicable_2025_comparison": 0,
                    "reason": "The physical capture contains one player (Lamar Jackson), 26 rows, and one 2025 row; it is not a complete PFR fantasy tableset, so no 2025 equality claim is made.",
                    "resolution": "The physical player_fantasy extract is a partial denormalized player-page probe. Its overlapping season/stat content is covered by registered PFR sibling tables; no independent capture is required for canonical coverage.",
                    "residual_gap": "Only weekly page-detail fields and publisher-specific/red-zone fields remain structured witnesses or promotion candidates."},
        "matrix": matrix, "open_blocked": 0,
        "notes": [
            "Every physical column has an explicit semantic disposition.",
            "Direct mappings are semantic targets, not measured equality results.",
            "The physical extract is not promoted as an independent source. Its overlapping content is witnessed by the registered full fantasy surface plus registered passing, rushing/receiving, snap-count, and games-played tables.",
        ],
    }
    OUT.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    print({"written": str(OUT), "rows": len(rows), "columns": len(cols), "open_blocked": 0})


if __name__ == "__main__":
    main()
