"""O.9.3 burn-down: the newspaper sidecar's WIDE tables -- our own review machinery.

THE SPLIT THAT MAKES THIS TRACTABLE. The newspaper lineage's 859 open rows are two
populations with nothing in common but a name:

    LONG   625 rows   the stat_name VOCABULARY the reviewers typed, one row per string.
                      582 distinct concepts, a genuine long tail, one-offs expected.
    WIDE   234 rows   the COLUMNS of the sidecar tables themselves -- and those are not a
                      vocabulary at all. They are the pilot's gate flags, its row keys, its
                      three generations of identity-resolution columns, and the reviewer
                      ledger. WE WROTE EVERY ONE OF THEM.

This pass takes the WIDE half. It is not the long tail and it does not pretend to be.

WHY THE AUTHORSHIP ARGUMENT APPLIES HERE AND NOT TO THE STAT NAMES. `newspaper_witness_common`
says it outright: the bundle stores reviewer testimony VERBATIM -- "stat_name/event_type
vocabularies are NOT canonicalized at rest (that is part of the evidence)". So the LONG
stat_name strings are the newspapers' words, transcribed, and no name match argues anything
about them. The WIDE column names are the opposite: `controlled_pilot_gate_status`,
`strict_load_ready`, `third_v3_identity_resolution_method` are OUR pipeline's names for OUR
pipeline's states, and nothing outside this program has ever published one.

THREE GENERATIONS OF ONE COLUMN, VISIBLE IN THE NAMES. `identity_task_id`,
`identity_v2_identity_task_id`, `third_v3_identity_task_id` are the same field written by
three successive identity passes, each of which added its own prefix rather than overwriting.
That is 27 of the 234 rows on its own. They are excluded as crosswalk-lane material, not
folded together -- which generation won is the identity lane's question, not the column
dossier's.

WHAT IS NOT SWEPT, and it is the usual proportion. Nine columns in these tables are real
measurements or real canonicals and are decided one at a time: the two team scores, the
starter flags, the participation type, the game locators. Two are refused.

Run:  python -m scripts.sota_recon.newspaper_sidecar_column_adjudication [--apply]
"""
from __future__ import annotations

import argparse
import collections
import json

from .column_dossier import row_key
from .nflcom_column_adjudication import DOSSIER_PATH, apply_to_ledger

GENERATOR = "scripts.sota_recon.newspaper_sidecar_column_adjudication"

SOURCES = {
    "newspaper_game_context": "sidecar per-game context rows",
    "newspaper_lineups": "sidecar lineup/participation rows",
    "newspaper_pbp_events": "sidecar play-by-play event rows",
    "newspaper_player_notes": "sidecar per-player narrative notes",
    "newspaper_scoring_events": "sidecar scoring-event rows",
    "newspaper_review_accepted": "the reviewer's ACCEPTED decisions ledger",
    "newspaper_review_holds": "the reviewer's HOLD decisions ledger",
    "newspaper_source_doc_notes": "reviewer notes about a source document",
}

COHORTS: dict[str, str] = {
    "PILOT_GATE_AND_CONFIDENCE":
        "a gate flag or confidence score OUR controlled pilot writes to decide whether a "
        "row may load. It records what the pipeline concluded about the row, not what the "
        "newspaper said -- and the sidecar is explicitly non-voting until its holds clear, "
        "so these are the machinery of that hold rather than testimony",
    "SIDECAR_ROW_KEY":
        "a key our own bundle mints to address a row, a document or a review packet "
        "(pilot_*_key, target_table, target_entity_key, source_document_id, triage_id, "
        "packet_dir). It locates evidence; its obligation is the crosswalk lane (§19.2)",
    "IDENTITY_RESOLUTION":
        "an identity-resolution input, output or task id. THREE GENERATIONS of these "
        "columns coexist (bare, identity_v2_*, third_v3_*), each pass having prefixed "
        "rather than overwritten. Which generation is authoritative is the identity lane's "
        "question; none of them measures anything about a football game",
    "REVIEW_LEDGER":
        "a column of the REVIEWER'S OWN decision record -- the verdict, the quote it "
        "rested on, the normalisation attempted and whether it parsed. It is evidence "
        "ABOUT a claim, one level up from the claim, and the claim itself is a stat_name "
        "row in the LONG tables",
    "EVENT_GRAIN":
        "a per-EVENT column (scoring event or play): its period, clock, down, distance, "
        "yard line, order, points and narrative text. The v26 subject is a player-week, and "
        "these sources' obligation is already recorded as LANE_WITNESSED -- the newspaper "
        "sidecar scoring and pbp lanes",
    "NARRATIVE":
        "prose the reviewer transcribed. newspaper_witness_common states the standing "
        "decision directly: narrative atoms are historical record, not weekly-cell "
        "witnesses. Imported here rather than re-argued",
}

_BY_COHORT: dict[str, tuple[str, ...]] = {
    "PILOT_GATE_AND_CONFIDENCE": (
        "confidence_bar", "confidence_score", "confidence", "controlled_pilot_gate_status",
        "strict_load_ready", "strict_hold_reason", "review_status", "effective_fields_json",
        "evidence_locator_json", "unmapped_fields_json", "reconciliation_status",
        "materialization_status"),
    "SIDECAR_ROW_KEY": (
        "pilot_source_witness_row_key", "pilot_game_context_key", "pilot_lineup_key",
        "pilot_pbp_event_key", "pilot_player_note_key", "pilot_scoring_event_key",
        "target_table", "target_entity_key", "source_document_id", "source_documents_json",
        "triage_id", "packet_dir", "return_csv", "evidence_file",
        "resolved_evidence_files_json"),
    "IDENTITY_RESOLUTION": (
        "identity_task_id", "identity_resolution_status", "identity_resolution_method",
        "resolved_player", "player_raw", "team_raw", "source_row_text",
        "scoring_player_raw", "scoring_resolved_player", "scoring_team_raw",
        "passer_raw", "passer_resolved_player", "receiver_raw", "receiver_resolved_player",
        "primary_player_raw", "primary_resolved_player", "secondary_player_raw",
        "secondary_resolved_player", "possession_team_raw",
        "team_1_raw", "team_1_resolved", "team_2_raw", "team_2_resolved",
        "scoring_NFL_player_id", "passer_NFL_player_id", "receiver_NFL_player_id",
        "primary_NFL_player_id", "secondary_NFL_player_id"),
    "REVIEW_LEDGER": (
        "decision", "evidence_quote", "reviewer_notes", "winner_source",
        "normalized_atom_type", "normalized_family", "normalized_value",
        "normalized_value_json", "normalized_value_parse_status"),
    "EVENT_GRAIN": (
        "event_order", "event_type", "period_raw", "clock_raw", "down_raw", "distance_raw",
        "yardline_raw", "points", "distance_yards", "yards", "play_type", "scoring_team",
        "possession_team"),
    "NARRATIVE": ("play_text", "note_text", "note_type", "note_fields_json"),
}

# Any column whose name carries an identity-pass generation prefix. A suffix/prefix rule
# rather than an enumeration, because each new identity pass invents its own prefix and an
# enumeration would refill itself -- the same argument PROVENANCE_SUFFIXES rests on.
_IDENTITY_GENERATION_MARKERS = ("identity_v2_", "third_v3_", "_v2_identity_",
                                "_v3_identity_", "_identity_task_id")

MAPPED: dict[str, str] = {
    "game_date": "game_date",
    "season_type": "season_type",
    "nfl_team": "nfl_team",
    "opponent_nfl_team": "opponent_nfl_team",
    "player_week": "player_week",
    "is_starter": "is_starter",
    "starter_position": "starter_position",
    # The settled taxonomy ruling: the newspaper's specific/verbatim position string
    # maps to the deliberately multi-valued NFL vocabulary, never the ten fantasy buckets.
    "listed_position_raw": "nfl_position",
    "source_positions": "nfl_position",
}

ESCALATED: dict[str, str] = {
    "team_1_score":
        "The game-context row is SYMMETRIC -- two teams, neither marked as the subject -- "
        "while the registry's `team_points` and `opponent_points` presuppose one. Assigning "
        "team_1 to `team_points` would fix a subject the source never chose, and would do "
        "it silently for every row. Settles by deciding whether a symmetric game row is "
        "admitted as two subject-oriented rows or as a new symmetric pair of columns",
    "team_2_score": "the other half of the symmetric game-score pair -- see `team_1_score`",
    "participation_type":
        "how the newspaper recorded the player's participation (started, played, "
        "substituted). The registry carries `is_starter` as a BOOLEAN and "
        "`starter_position` as a slot, so a three-or-more-valued participation vocabulary "
        "has no column -- and collapsing it to the boolean would discard the distinction "
        "the newspaper actually drew. Settles as a NEW column decision or a documented "
        "collapse rule",
    "lineup_side_raw":
        "which side of the ball the newspaper listed the player on, verbatim. "
        "`position_side` is a registered canonical, but it is derived from our position "
        "taxonomy rather than from a newspaper's own words, and mapping a raw string onto "
        "it would launder an unnormalised value into a taxonomy-governed column",
}


def escalated_row_keys() -> dict[str, str]:
    document = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    out: dict[str, str] = {}
    for row in document["rows"]:
        if row["source"] not in SOURCES and row["source"] != "ancient_newspaper_ocr":
            continue
        if row["regime"] != "WIDE" or row["column"] not in ESCALATED:
            continue
        out[row_key(row["source"], row["table_key"], row["column"])] = ESCALATED[row["column"]]
    return out


def cohort(column: str) -> str | None:
    for name, members in _BY_COHORT.items():
        if column in members:
            return name
    if any(marker in column for marker in _IDENTITY_GENERATION_MARKERS):
        return "IDENTITY_RESOLUTION"
    return None


def build_decisions() -> tuple[list[dict], dict]:
    from .column_dossier import DISPOSITIONS_PATH, load_decisions

    registry = json.loads(
        (DISPOSITIONS_PATH.parent / "stat_contracts.v1.json").read_text(encoding="utf-8"))
    contracts = {c["canonical_name"]: c for c in registry["stats"]}
    for column, canonical in MAPPED.items():
        if canonical not in contracts:
            raise SystemExit(f"{column} -> {canonical!r} is in no stat contract")

    document = json.loads(DOSSIER_PATH.read_text(encoding="utf-8"))
    mine = {key for key, entry in load_decisions().items()
            if entry.get("generated_by") == GENERATOR}
    decisions: list[dict] = []
    tally: collections.Counter = collections.Counter()
    residual: list[str] = []

    for row in document["rows"]:
        if row["lineage"] != "newspaper" or row["regime"] != "WIDE":
            continue
        key = row_key(row["source"], row["table_key"], row["column"])
        if row["disposition"] != "OPEN" and key not in mine:
            continue
        column = row["column"]
        authorship = (
            "AUTHORSHIP: this is a column of OUR sidecar bundle, named by our own builder. "
            "The bundle stores reviewer testimony VERBATIM and canonicalises read-side only "
            "(newspaper_witness_common), so the LONG stat_name strings are the newspapers' "
            "words and these WIDE column names are ours -- the two halves of this lineage "
            "get opposite treatment for that reason")

        if column in ESCALATED:
            tally["escalated_left_open"] += 1
            continue
        if column in MAPPED:
            canonical = MAPPED[column]
            contract = contracts[canonical]
            decisions.append({
                "key": key, "disposition": "MAPPED_TO_CANONICAL", "canonical": canonical,
                "reason": f"the sidecar carries this as the registered canonical "
                          f"{canonical!r} (family {contract.get('family')!r}, unit "
                          f"{contract.get('unit')!r})",
                "evidence": f"{authorship}. stat_contracts.v1.json carries {canonical!r} "
                            f"with unit {contract.get('unit')!r}, aggregation class "
                            f"{contract.get('aggregation_class')!r}, and natural grain "
                            f"{contract.get('natural_grain')!r}",
            })
            tally["mapped"] += 1
            continue
        name = cohort(column)
        if name is None:
            residual.append(f"{row['source']}|{column}")
            continue
        decisions.append({
            "key": key, "disposition": "EXCLUDED_WITH_REASON",
            "reason": f"{name}: {COHORTS[name]}",
            "evidence": f"{authorship}. Source role: {SOURCES.get(row['source'], row['source'])}",
        })
        tally[f"excluded:{name}"] += 1

    return decisions, {"total": len(decisions), "by_bucket": dict(sorted(tally.items())),
                       "residual": sorted(residual)}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    decisions, tally = build_decisions()
    print(f"decisions: {tally['total']:,}")
    for name, value in tally["by_bucket"].items():
        print(f"  {name:44s} {value}")
    if tally["residual"]:
        print(f"\nRESIDUAL -- no cohort claims these, left OPEN ({len(tally['residual'])}):")
        for name in tally["residual"]:
            print("   -", name)
    if args.apply:
        print("\n", apply_to_ledger(decisions, generated_by=GENERATOR))
    else:
        print("\n(dry run -- pass --apply to write the ledger)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
