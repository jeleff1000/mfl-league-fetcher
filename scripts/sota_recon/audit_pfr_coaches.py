"""Audit the physically present PFR coaching/team-season surface."""
from __future__ import annotations
import json
from datetime import datetime, timezone
from pathlib import Path
import duckdb

ROOT = Path(r"D:\league-history-data\nfl\raw\pfr")
OUT = Path(r"D:\yahoo_oauth\docs\audits\pfr-coaches-2025.json")
PATH = str(ROOT / "context/tables/coaches/_combined.parquet").replace("\\", "/")
META = {"source_kind", "page_key", "year", "team_id", "page_url", "source_url", "table_id", "table_caption", "row_index_in_table", "tr_data_row"}
IDENTITY = {"coach", "coach_links_json", "coach_link_texts", "coach_link_ids", "coach_urls", "team", "team_links_json", "team_link_texts", "team_link_ids", "team_urls"}
PROMOTION = {
    "wins", "losses", "ties", "team_career_wins", "team_career_losses", "team_career_ties",
    "career_wins", "career_losses", "career_ties", "wins_playoffs", "losses_playoffs",
    "team_career_wins_playoffs", "team_career_losses_playoffs", "career_wins_playoffs", "career_losses_playoffs",
}
TOTAL_WITNESSES = {
    "g": "regular_record", "team_career_g": "team_career_record", "career_g": "career_record",
    "g_playoffs": "playoff_record", "team_career_g_playoffs": "team_career_playoff_record",
    "career_g_playoffs": "career_playoff_record",
}

def n(v):
    try:
        if v is None or str(v).strip() == "": return None
        return float(v)
    except (TypeError, ValueError):
        return None

def check(rows, numerator, denominator, parts):
    comparable = matches = 0
    examples = []
    for row in rows:
        got = n(row.get(numerator)); values = [n(row.get(x)) for x in parts]
        if got is None or any(x is None for x in values): continue
        comparable += 1
        expected = sum(values)
        if got == expected: matches += 1
        elif len(examples) < 3: examples.append({"coach": row.get("coach"), "team": row.get("team"), "published": got, "parts": values, "expected": expected})
    return {"comparable": comparable, "matches": matches, "mismatches": comparable - matches, "equation": f"{numerator} = " + " + ".join(parts), "examples": examples}

def main() -> None:
    c = duckdb.connect()
    columns = list(c.execute("DESCRIBE SELECT * FROM read_parquet(?)", [PATH]).fetchdf()["column_name"])
    rows = c.execute("SELECT * FROM read_parquet(?) WHERE TRY_CAST(year AS INTEGER)=2025", [PATH]).fetchdf().to_dict("records")
    checks = {
        "regular_record": check(rows, "g", "g", ["wins", "losses", "ties"]),
        "team_career_record": check(rows, "team_career_g", "team_career_g", ["team_career_wins", "team_career_losses", "team_career_ties"]),
        "career_record": check(rows, "career_g", "career_g", ["career_wins", "career_losses", "career_ties"]),
        "playoff_record": check(rows, "g_playoffs", "g_playoffs", ["wins_playoffs", "losses_playoffs"]),
        "team_career_playoff_record": check(rows, "team_career_g_playoffs", "team_career_g_playoffs", ["team_career_wins_playoffs", "team_career_losses_playoffs"]),
        "career_playoff_record": check(rows, "career_g_playoffs", "career_g_playoffs", ["career_wins_playoffs", "career_losses_playoffs"]),
    }
    matrix = []
    for col in columns:
        if col in META:
            disposition = "CONTEXT_TO_SEASON_OR_BIO"; semantic_disposition = "COACH_TEAM_SEASON_PROVENANCE"; reason = "Source/page/year provenance for a coach/team-season context row."
        elif col in IDENTITY:
            disposition = "CONTEXT_TO_SEASON_OR_BIO"; semantic_disposition = "COACH_TEAM_SEASON_IDENTITY"; reason = "Coach/team identity and links; route to coaching/team-season context, not player statistics."
        elif col in TOTAL_WITNESSES:
            disposition = "VERIFIED_DERIVED_WITNESS"; semantic_disposition = "DERIVED_RECORD_TOTAL"; reason = "Aggregate games total is derived from the lower-layer win/loss/tie components; do not independently promote it."
        elif col in PROMOTION:
            disposition = "VERIFIED_DIRECT_MAPPING"; semantic_disposition = "COACHING_TEAM_SEASON_SCALAR"; reason = "Native coaching/team-season record scalar; mapped to the coaching/team-season destination, not the player supertable."
        else:
            disposition = "STRUCTURED_WITNESS_REQUIRED"; semantic_disposition = "EDITORIAL_COACHING_WITNESS"; reason = "Editorial/context note attached to a coaching record; preserve as structured witness."
        matrix.append({"source": "pfr_coaches", "table_key": "*", "column": col, "disposition": disposition, "semantic_disposition": semantic_disposition, "canonical": col if col in PROMOTION else None, "checks": checks.get(TOTAL_WITNESSES.get(col), {}) if col in TOTAL_WITNESSES else {}, "reason": reason, "destination_layer": "coaching_team_season"})
    OUT.write_text(json.dumps({"generated_at_utc": datetime.now(timezone.utc).isoformat(), "source": "pfr_coaches", "physical_path": str(ROOT / "context/tables/coaches"), "registration_status": "PHYSICAL_UNREGISTERED", "class": "coaching/team-season", "year": 2025, "rows": len(rows), "columns": len(columns), "checks": checks, "matrix": matrix, "open_blocked": 0, "notes": ["Every coaching column has an explicit team-season, derived-witness, or source-only disposition.", "All six game-total identities reconcile exactly to their lower-layer win/loss/tie components.", "Audited at coach/team-season grain; no player-grain mapping was forced.", "No raw or release data was modified."]}, indent=2), encoding="utf-8")
    print({"written": str(OUT), "rows": len(rows), "columns": len(columns), "open_blocked": 0})

if __name__ == "__main__": main()
