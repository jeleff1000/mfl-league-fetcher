"""Build the explicit PFR column disposition ledger.

The individual receipts intentionally use compact dispositions.  This ledger
expands every receipt row into a destination or an explicit non-column reason,
so CONTEXT_TO_SEASON_OR_BIO and STRUCTURED_WITNESS_REQUIRED cannot be mistaken
for silently unmapped data.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

AUDITS = Path("docs/audits")
OUT_JSON = AUDITS / "pfr-explicit-disposition-ledger-2025.json"
OUT_MD = AUDITS / "pfr-explicit-disposition-ledger-2025.md"
SKIP = {"pfr-final-adjudication-summary-2025.json", "pfr-audit-coverage-2025.json", "pfr-column-coverage-2025.json", OUT_JSON.name}

DIRECT = {"VERIFIED_DIRECT_MAPPING", "VERIFIED_DERIVED_WITNESS"}
PROV_RE = re.compile(r"(url|link|source|scrape|tr_data|row_index|table_|page_|html|raw|caption|timestamp|seen|manifest|path)", re.I)
IDENTITY_RE = re.compile(r"(pfr_id|nfl_player_id|player|pos|position|age|team|coach|franchise|opponent|year|season|week|game|date|identity)", re.I)


def source_class(source: str) -> str:
    s = source.lower()
    if "coach" in s:
        return "coaching/team-season"
    if any(x in s for x in ("voting", "all_pro", "pro_bowl", "membership")):
        return "recognition/membership"
    if any(x in s for x in ("team_games", "master_schedule", "schedule", "boxscore", "drives", "pbp")):
        return "team-game/boxscore"
    if any(x in s for x in ("combine", "identity", "player_fantasy", "player_index", "players_probe")):
        return "player-context"
    if any(x in s for x in ("air_yards", "accuracy", "pressure", "play_type", "advanced", "sim_scores")):
        return "charting/detail"
    return "player-stat"


def expand(row: dict, artifact: str) -> dict:
    source = str(row.get("source") or "unknown")
    col = str(row.get("column") or "")
    disp = row.get("disposition")
    cls = source_class(source)
    if disp in DIRECT:
        final = "CANONICAL_MAPPING"
        destination = row.get("destination_layer") or ("player_weekly_then_season_career" if cls == "player-stat" else cls)
        reason = "Exact direct or immutable derived mapping recorded by the source receipt."
    elif disp == "PROMOTION_CANDIDATE":
        final = "DEFERRED_PROMOTION_CANDIDATE"
        destination = row.get("destination_layer") or cls
        reason = "Semantic source field has no verified canonical target in the current release; promotion/backfill is explicitly deferred."
    elif PROV_RE.search(col) or cls in {"charting/detail"} and PROV_RE.search(col):
        final = "EXPLICIT_PROVENANCE_OR_RAW_WITNESS"
        destination = "immutable_source_witness_sidecar"
        reason = "Source payload, scrape, link, row, or raw-detail field; retained for reproducibility and not promoted as a player metric."
    elif cls == "coaching/team-season":
        final = "EXPLICIT_CONTEXT_MAPPING"
        destination = "coaching_team_season"
        reason = "Coach/team-season grain; joins through coaching identity and season, not player weekly statistics."
    elif cls == "recognition/membership":
        final = "EXPLICIT_CONTEXT_MAPPING"
        destination = "player_season_career_bio_recognition"
        reason = "Recognition or membership grain; attached to player identity/season/career/bio surfaces."
    elif cls == "team-game/boxscore":
        final = "EXPLICIT_CONTEXT_MAPPING"
        destination = "team_game_and_dst_context"
        reason = "Team-game or boxscore grain; joins to weekly/team-game context and DST reciprocal witnesses."
    elif IDENTITY_RE.search(col) or cls == "player-context":
        final = "EXPLICIT_CONTEXT_MAPPING"
        destination = "player_weekly_season_career_bio_identity"
        reason = "Player identity, team, season, position, or presence field; carried through the player linkage lanes."
    else:
        final = "EXPLICIT_STRUCTURED_WITNESS_NO_CANONICAL_COLUMN"
        destination = "structured_witness_sidecar"
        reason = "Audited source/detail field with no current canonical supertable column; preserved as an explicit witness rather than silently dropped."
    return {"artifact": artifact, "source": source, "table_key": row.get("table_key"), "column": col,
            "original_disposition": disp, "final_disposition": final, "destination": destination,
            "canonical": row.get("canonical"), "reason": reason}


def main() -> None:
    ledger = []
    for path in sorted(AUDITS.glob("pfr-*.json")):
        if path.name in SKIP:
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        matrices = data.get("matrix", [])
        if "tables" in data:
            matrices = [row for table in data["tables"] for row in table.get("matrix", [])]
        for row in matrices:
            if row.get("column"):
                ledger.append(expand(row, path.name))
    counts = Counter(row["final_disposition"] for row in ledger)
    destinations = Counter(row["destination"] for row in ledger)
    payload = {"generated_at_utc": datetime.now(timezone.utc).isoformat(), "rows": len(ledger),
               "disposition_counts": dict(counts), "destination_counts": dict(destinations), "columns": ledger,
               "policy": "Every PFR receipt column has either a canonical destination or an explicit witness/exclusion/deferred reason."}
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = ["# PFR explicit disposition ledger (2025)", "", f"Rows: {len(ledger)}", "", "## Dispositions", "", "| disposition | count |", "|---|---:|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(counts.items())]
    lines += ["", "## Destinations", "", "| destination | count |", "|---|---:|"]
    lines += [f"| {k} | {v} |" for k, v in sorted(destinations.items())]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print({"written": str(OUT_JSON), "rows": len(ledger), "dispositions": dict(counts)})


if __name__ == "__main__":
    main()
