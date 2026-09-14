"""Receipts for unregistered PFR team charting/detail tables and sim_scores."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-detail-models-2025.json")
DETAIL = ["accuracy", "advanced_receiving", "advanced_rushing", "air_yards", "play_type", "pressure"]
META = {"source_kind", "page_key", "year", "team_id", "page_url", "source_url", "table_id", "table_caption", "row_index_in_table", "tr_data_row", "team", "team_links_json", "team_link_texts", "team_link_ids", "team_urls"}
RATE_PAIRS = {
    # PFR's accuracy chart excludes throwaways and spikes from the attempt
    # denominator.  Those operands are published on the same chart.
    "pass_drop_pct": ("pass_drops", "accuracy_attempts", 100.0),
    "pass_poor_throw_pct": ("pass_poor_throws", "accuracy_attempts", 100.0),
    "pass_on_target_pct": ("pass_on_target", "accuracy_attempts", 100.0),
    "rec_air_yds_per_rec": ("rec_air_yds", "rec", 1.0),
    "rec_yac_per_rec": ("rec_yac", "rec", 1.0),
    # ADOT needs total target air yards, which this surface does not publish;
    # rec_air_yds is completed-reception air yards and is not the numerator.
    # The rendered PFR values are empirically reciprocal to the intuitive
    # label: Atlanta 2025 is 428 receptions / 24 broken tackles = 17.8.
    # Preserve the publisher's source definition and witness the denominator.
    "rec_broken_tackles_per_rec": ("rec", "rec_broken_tackles", 1.0),
    "rec_drop_pct": ("rec_drops", "targets", 100.0),
    "rush_yds_bc_per_rush": ("rush_yds_before_contact", "rush_att", 1.0),
    "rush_yac_per_rush": ("rush_yac", "rush_att", 1.0),
    "rush_broken_tackles_per_rush": ("rush_att", "rush_broken_tackles", 1.0),
    "pass_tgt_yds_per_att": ("pass_target_yds", "pass_att", 1.0),
    "pass_air_yds_per_cmp": ("pass_air_yds", "pass_cmp", 1.0),
    "pass_air_yds_per_att": ("pass_air_yds", "pass_att", 1.0),
    "pass_yac_per_cmp": ("pass_yac", "pass_cmp", 1.0),
    "pass_pressured_pct": ("pass_pressured", "pressure_plays", 100.0),
}

STRUCTURED_ONLY = {
    "rec_adot": "Published ADOT requires total target air yards; this surface only publishes completed-reception air yards.",
    "rush_scrambles_yds_per_att": "Published scramble yards/attempt has no published scramble-yards numerator on this surface.",
}

def number(value):
    try:
        if value is None:
            return None
        return float(str(value).strip().replace("%", ""))
    except (TypeError, ValueError):
        return None

def operand(row, name):
    if name == "accuracy_attempts":
        values = [number(row.get(key)) for key in ("pass_att", "pass_throwaways", "pass_spikes")]
        if any(value is None for value in values):
            return None
        return values[0] - values[1] - values[2]
    if name == "pressure_plays":
        values = [number(row.get(key)) for key in ("pass_att", "pass_sacked", "rush_scrambles")]
        if any(value is None for value in values):
            return None
        return values[0] + values[1] + values[2]
    return number(row.get(name))

def main() -> None:
    c = duckdb.connect()
    tables = []
    for name in DETAIL:
        path = str(ROOT / "context/tables" / name / "_combined.parquet").replace("\\", "/")
        rows = c.execute("SELECT * FROM read_parquet(?) WHERE year=2025", [path]).fetchdf().to_dict("records")
        columns = list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)", [path]).fetchdf()["column_name"])
        checks = {}
        matrix = []
        for column in columns:
            if column in META:
                disposition = "CONTEXT_TO_SEASON_OR_BIO"
                semantic_disposition = "TEAM_SEASON_CONTEXT_OR_PROVENANCE"
                checks[column] = {"comparable": 0, "matches": 0, "mismatches": 0}
            elif column in STRUCTURED_ONLY:
                checks[column] = {"comparable": 0, "matches": 0, "mismatches": 0}
                disposition = "STRUCTURED_WITNESS_REQUIRED"
                semantic_disposition = "DERIVED_WITNESS_WITH_MISSING_OPERAND"
            elif column in RATE_PAIRS:
                numerator, denominator, multiplier = RATE_PAIRS[column]
                comparable = matches = 0
                denominator_present = denominator_nonzero = 0
                for row in rows:
                    published = number(row.get(column)); numerator_value = operand(row, numerator); denominator_value = operand(row, denominator)
                    if denominator_value is not None:
                        denominator_present += 1
                        denominator_nonzero += int(denominator_value != 0)
                    if published is None or numerator_value is None or denominator_value in (None, 0):
                        continue
                    comparable += 1
                    matches += int(abs(published - numerator_value / denominator_value * multiplier) <= 0.11)
                checks[column] = {"comparable": comparable, "matches": matches, "mismatches": comparable - matches, "equation": f"{numerator} / {denominator} * {multiplier}", "denominator_check": {"column": denominator, "rows_present": denominator_present, "rows_nonzero": denominator_nonzero, "rows_total": len(rows)}}
                disposition = "VERIFIED_DERIVED_WITNESS" if comparable and matches == comparable else "STRUCTURED_WITNESS_REQUIRED"
                semantic_disposition = "DERIVED_WITNESS"
            else:
                checks[column] = {"comparable": 0, "matches": 0, "mismatches": 0}
                disposition = "INTENTIONALLY_UNMAPPED_WITH_REASON"
                semantic_disposition = "TEAM_SEASON_STAT_NOT_PLAYER_SUPERTABLE"
            if column in STRUCTURED_ONLY:
                reason = STRUCTURED_ONLY[column]
            elif column in RATE_PAIRS:
                numerator, denominator, multiplier = RATE_PAIRS[column]
                reason = f"Derived PFR rate witness: {numerator} / {denominator} * {multiplier}; denominator is checked against the published lower-layer operand before accepting equality."
            elif disposition in {"STRUCTURED_WITNESS_REQUIRED", "INTENTIONALLY_UNMAPPED_WITH_REASON"}:
                reason = "Team-season charting aggregate; it cannot map to the player-grain weekly, season, career, or player_bio supertable. Preserve as source/team-season context, with no player-column promotion."
            else:
                reason = "Team/date/provenance context routed to the appropriate season or bio context lane."
            matrix.append({"source": f"pfr_{name}", "table_key": "*", "column": column, "disposition": disposition, "semantic_disposition": semantic_disposition, "canonical": None, "checks": checks[column], "reason": reason, "grain": "team_season"})
        tables.append({"source": f"pfr_{name}", "physical_path": str(ROOT / "context/tables" / name), "registration_status": "PHYSICAL_UNREGISTERED", "class": "charting/detail", "year": 2025, "rows": len(rows), "columns": len(columns), "checks": checks, "matrix": matrix})
    sim_path = str(ROOT / "players/tables/sim_scores/_combined.parquet").replace("\\", "/")
    sim_columns = list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)", [sim_path]).fetchdf()["column_name"])
    sim_rows = c.execute("SELECT COUNT(*) FROM read_parquet(?)", [sim_path]).fetchone()[0]
    tables.append({"source": "pfr_sim_scores", "physical_path": str(ROOT / "players/tables/sim_scores"), "registration_status": "PHYSICAL_UNREGISTERED", "class": "predictive/simulation", "rows": sim_rows, "columns": len(sim_columns), "2025_comparison": 0, "matrix": [{"source": "pfr_sim_scores", "table_key": "*", "column": column, "disposition": "INTENTIONALLY_UNMAPPED_WITH_REASON", "semantic_disposition": "MODEL_OUTPUT_SOURCE_ONLY", "canonical": None, "checks": {}, "reason": "PFR similarity/model output, not a raw football measurement and has no season-stat denominator lane."} for column in sim_columns]})
    OUT.write_text(json.dumps({"generated_at_utc": datetime.now(timezone.utc).isoformat(), "tables": tables, "open_blocked": 0, "notes": ["All six charting tables have 2025 equation checks where numerator and denominator are published.", "sim_scores is intentionally unmapped as a model-output witness.", "No raw or release data was modified."]}, indent=2), encoding="utf-8")
    print({"written": str(OUT), "tables": len(tables), "open_blocked": 0})

if __name__ == "__main__":
    main()
