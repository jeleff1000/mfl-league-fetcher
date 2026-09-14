"""Build the immutable identity-only bridge for PFR IDs absent from bio/index."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
ADJ = ROOT / "docs" / "identity-adjudication-2026.json"
OUT = ROOT / "docs" / "pfr-identity-bridge-2026.json"

# Source link text observed in the PFR box/recognition surfaces.  These are
# identity rows only; no stat values are synthesized.
NAMES = {
    "BaueA.20": "A.C. Bauer", "BoonJ.20": "J.R. Boone", "BracM.20": "M.L. Brackett",
    "CaroJ.00": "J.C. Caroline", "EleyAc00": "Ace Eley", "GainJa00": "Jalan Gaines",
    "HickW.00": "W.K. Hicks", "HoluE.00": "E.J. Holub", "JoynL.20": "L.C. Joyner",
    "KaluN.20": "N.D. Kalu", "KimmJ.20": "J.D. Kimmel", "MoorJo01": "Jordan Moore",
    "SmitJ.02": "J.D. Smith", "St.CBo00": "Bob St. Clair", "St.GTe20": "Ted St. Germaine",
    "St.JHe20": "Herb St. John", "St.JLe00": "Len St. Jean", "StanC.20": "C.B. Stanley",
    "ThomJ.01": "J.T. Thomas", "WhitS.20": "S.J. Whitman", "WillJ.20": "J.R. Williamson",
}
EXACT_RELEASE_IDS = {
    "BoonJ.20", "BracM.20", "CaroJ.00", "HickW.00", "HoluE.00",
    "KaluN.20", "KimmJ.20", "St.CBo00", "ThomJ.01", "WhitS.20", "WillJ.20",
}


def main() -> None:
    adjudication = json.loads(ADJ.read_text(encoding="utf-8"))
    candidates = adjudication.get("identity_bridge_candidates") or adjudication.get("resolved_identity_bridge_ids", {})
    rows = []
    for pfr_id, info in sorted(candidates.items()):
        name = NAMES[pfr_id]
        rows.append({
            "pfr_id": pfr_id,
            "canonical_nfl_player_id": pfr_id if pfr_id in EXACT_RELEASE_IDS else None,
            "player": name,
            "status": "RESOLVED_CANONICAL_ID" if pfr_id in EXACT_RELEASE_IDS else "RESOLVED_PFR_SOURCE_IDENTITY",
            "resolution_method": "EXACT_RELEASE_ID" if pfr_id in EXACT_RELEASE_IDS else "PFR_SOURCE_IDENTITY_ONLY",
            "referencing_sources": info["referencing_sources"],
            "evidence": "PFR player_link_ids + player_link_texts/player URL on the referencing source surfaces",
            "stats_backfilled": False,
        })
    # Keep the bridge as a JSON array so DuckDB can consume it directly in the
    # identity spine gate. Metadata and adjudication rationale live beside it
    # in identity-adjudication-2026.json.
    OUT.write_text(json.dumps(rows, indent=2) + "\n", encoding="utf-8")
    print({"written": str(OUT), "rows": len(rows), "stats_backfilled": False})


if __name__ == "__main__":
    main()
