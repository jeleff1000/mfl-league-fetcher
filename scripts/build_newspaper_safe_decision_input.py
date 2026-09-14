from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


DEFAULT_DB_PATH = Path(
    r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb"
)
DEFAULT_OUT_ROOT = Path(
    r"D:\league-history-data\nfl\derived\newspaper_atoms\review_decision_inputs"
)

FIELDS = [
    "decision_id",
    "decision_status",
    "decision_value",
    "route_to_lane",
    "resolved_boxscore_id",
    "resolved_target_table",
    "resolved_target_entity_key",
    "notes",
]

PROMOTION_VALUE = "approved_for_local_promotion"

BLOCKING_PROMOTION_TERMS = {
    "needs_boxscore_reconciliation",
    "needs_game_mapping",
    "scoreboard_conflicts",
    "score_conflict",
    "newspaper_score_missing",
    "score_not_present",
    "preview_no_score",
    "needs_resolution",
}

TEAM_ALIASES = {
    "AKR": {"akr", "akron", "pros"},
    "BFF": {"bff", "buffalo", "all-americans", "all americans"},
    "BUF": {"buf", "buffalo", "all-americans", "all americans"},
    "CAN": {"can", "canton", "bulldogs"},
    "CBD": {"cbd", "columbus", "tigers"},
    "CHI": {"chi", "chicago", "bears", "bruins"},
    "CLI": {"cli", "cleveland", "indians"},
    "CRD": {"crd", "cardinals", "chicago cardinals", "cards"},
    "DAY": {"day", "dayton", "triangles", "dayton triangles"},
    "DUL": {"dul", "duluth", "kelleys", "kelleys-duluth", "eskimos"},
    "FRN": {"frn", "frankford", "yellow jackets", "frankford yellow jackets", "philadelphia"},
    "GNB": {"gnb", "green bay", "packers", "green bayians"},
    "HAM": {"ham", "hammond", "pros"},
    "KAN": {"kan", "kansas city", "blues", "cowboys"},
    "KEN": {"ken", "kenosha", "maroons", "kenosha maroons"},
    "MIL": {"mil", "milwaukee", "badgers"},
    "MIN": {"min", "minneapolis", "marines"},
    "OOR": {"oor", "oorang", "indians"},
    "RAC": {"rac", "racine", "legion", "horlick-legion"},
    "RCH": {"rch", "rochester", "jeffersons", "jeffs"},
    "RII": {"rii", "rock island", "independents", "islanders"},
    "SLA": {"sla", "st. louis", "st louis", "all-stars", "all stars"},
    "STL": {"stl", "st. louis", "st louis", "all-stars", "all stars"},
}


def stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def load_proposed_fields(row: dict[str, Any]) -> dict[str, Any]:
    proposed_json = clean(row.get("proposed_fields_json"))
    if not proposed_json:
        return {}
    try:
        payload = json.loads(proposed_json)
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, dict) else {}


def text_has_score_pair(text: str, score_1: str, score_2: str) -> bool:
    if not (score_1 and score_2):
        return False
    pattern = re.compile(
        rf"(?<!\d){re.escape(score_1)}\s*(?:-|to|[\u2013\u2014]|â€“|â€”)\s*{re.escape(score_2)}(?!\d)",
        flags=re.IGNORECASE,
    )
    return bool(pattern.search(text))


def text_has_result_language(text: str) -> bool:
    return bool(
        re.search(
            r"\b(score|final|count|defeat(?:ed|s)?|beat(?:en|s)?|won|victory|triumph|tie|licking|wallop(?:ed|s)?)\b",
            text,
            flags=re.IGNORECASE,
        )
    )


def aliases_for_team(value: str) -> set[str]:
    value = clean(value).strip()
    aliases = set()
    if value:
        aliases.add(value.lower())
    aliases.update(TEAM_ALIASES.get(value.upper(), set()))
    return {alias for alias in aliases if alias}


def text_has_team_alias(text: str, aliases: set[str]) -> bool:
    lowered = text.lower()
    for alias in aliases:
        if re.search(rf"(?<![a-z0-9]){re.escape(alias.lower())}(?![a-z0-9])", lowered):
            return True
    return False


def game_candidate_evidence_is_safe(fields: dict[str, Any]) -> tuple[bool, str]:
    evidence = clean(fields.get("evidence_text"))
    score_1 = clean(fields.get("team_1_score"))
    score_2 = clean(fields.get("team_2_score"))
    team_1 = clean(fields.get("team_1_resolved") or fields.get("team_1_raw"))
    team_2 = clean(fields.get("team_2_resolved") or fields.get("team_2_raw"))

    if not text_has_score_pair(evidence, score_1, score_2):
        return False, "held because evidence quote does not contain the exact proposed score"
    if not text_has_result_language(evidence):
        return False, "held because evidence quote has the score but no final/result language"
    if not text_has_team_alias(evidence, aliases_for_team(team_1)):
        return False, f"held because evidence quote does not name team_1 ({team_1})"
    if not text_has_team_alias(evidence, aliases_for_team(team_2)):
        return False, f"held because evidence quote does not name team_2 ({team_2})"
    return True, "evidence quote contains exact score, result language, and both team labels"


def latest_decision_ledger_run(con: duckdb.DuckDBPyConnection) -> str:
    row = con.execute(
        """
        SELECT decision_ledger_run_id
        FROM newspaper_review.llm_review_decision_ledger_run
        ORDER BY created_at_utc DESC, decision_ledger_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    return clean(row[0]) if row else ""


def query_decisions(con: duckdb.DuckDBPyConnection, run_id: str) -> list[dict[str, Any]]:
    result = con.execute(
        """
        SELECT *
        FROM newspaper_review.llm_review_decision_ledger
        WHERE decision_ledger_run_id = ?
        ORDER BY lane, route_to_lane, target_table, boxscore_id, target_entity_key
        """,
        [run_id],
    )
    fields = [item[0] for item in result.description]
    return [dict(zip(fields, row)) for row in result.fetchall()]


def should_approve(row: dict[str, Any], hold_boxscore_ids: set[str]) -> tuple[bool, str]:
    lane = clean(row.get("lane"))
    route_to_lane = clean(row.get("route_to_lane"))
    target_table = clean(row.get("target_table"))
    confidence_bar = clean(row.get("confidence_bar")).lower()
    boxscore_id = clean(row.get("boxscore_id"))
    proposed_json = clean(row.get("proposed_fields_json"))
    proposed_fields = load_proposed_fields(row)

    if boxscore_id in hold_boxscore_ids:
        return False, f"held because boxscore_id {boxscore_id} is in the explicit hold list"

    if confidence_bar != "high":
        return False, "held because confidence bar is not high"

    blocking_terms = sorted(term for term in BLOCKING_PROMOTION_TERMS if term in proposed_json)
    if blocking_terms:
        return False, f"held because proposed fields contain blocking term(s): {', '.join(blocking_terms)}"

    if lane == "promotion_review" and target_table == "game_candidate" and boxscore_id:
        evidence_ok, evidence_reason = game_candidate_evidence_is_safe(proposed_fields)
        if evidence_ok:
            return True, f"approved high-confidence game candidate: {evidence_reason}"
        return False, evidence_reason

    if lane == "promotion_review" and target_table in {"scoring_event", "play_by_play_event", "player_game_box_score", "team_game_stat_claim"} and boxscore_id:
        return True, "approved high-confidence promotion-review stat/event package with resolved boxscore_id"

    if route_to_lane == "evidence_check_then_promote" and target_table != "game_candidate" and boxscore_id:
        return True, "approved high-confidence evidence-check atom with resolved boxscore_id"

    return False, "held for existing route lane"


def build_overrides(rows: list[dict[str, Any]], hold_boxscore_ids: set[str]) -> tuple[list[dict[str, str]], Counter]:
    overrides: list[dict[str, str]] = []
    counts: Counter = Counter()
    for row in rows:
        approve, reason = should_approve(row, hold_boxscore_ids)
        proposed_fields = load_proposed_fields(row)
        if approve:
            counts["approved"] += 1
            counts[f"approved_{clean(row.get('target_table'))}"] += 1
            overrides.append(
                {
                    "decision_id": clean(row.get("decision_id")),
                    "decision_status": "approved",
                    "decision_value": PROMOTION_VALUE,
                    "route_to_lane": "local_promotion",
                    "resolved_boxscore_id": clean(row.get("boxscore_id")),
                    "resolved_target_table": clean(row.get("target_table")),
                    "resolved_target_entity_key": clean(row.get("target_entity_key")),
                    "notes": reason,
                    "_score_signature": "|".join(
                        clean(proposed_fields.get(field))
                        for field in ["team_1_resolved", "team_1_score", "team_2_resolved", "team_2_score"]
                    ),
                }
            )
        else:
            counts["held"] += 1
            counts[f"held_{clean(row.get('target_table')) or 'no_target'}"] += 1
    score_signatures_by_boxscore: dict[str, set[str]] = {}
    for row in overrides:
        if clean(row.get("resolved_target_table")) != "game_candidate":
            continue
        boxscore_id = clean(row.get("resolved_boxscore_id"))
        signature = clean(row.get("_score_signature"))
        if boxscore_id and signature:
            score_signatures_by_boxscore.setdefault(boxscore_id, set()).add(signature)
    conflict_boxscores = {
        boxscore_id
        for boxscore_id, signatures in score_signatures_by_boxscore.items()
        if len(signatures) > 1
    }
    if conflict_boxscores:
        filtered: list[dict[str, str]] = []
        for row in overrides:
            if clean(row.get("resolved_target_table")) == "game_candidate" and clean(row.get("resolved_boxscore_id")) in conflict_boxscores:
                counts["approved"] -= 1
                counts["approved_game_candidate"] -= 1
                counts["held"] += 1
                counts["held_game_candidate"] += 1
                counts["held_safe_conflicting_game_score"] += 1
                continue
            filtered.append(row)
        overrides = filtered
    for row in overrides:
        row.pop("_score_signature", None)
    return overrides, counts


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument("--decision-ledger-run-id", default="")
    parser.add_argument("--out-root", type=Path, default=DEFAULT_OUT_ROOT)
    parser.add_argument("--label", default="safe_local_promotions")
    parser.add_argument(
        "--hold-boxscore-id",
        action="append",
        default=["192011070rii"],
        help="Boxscore id to keep out of automatic local promotion. Can be repeated.",
    )
    args = parser.parse_args()

    con = duckdb.connect(str(args.db_path), read_only=True)
    run_id = args.decision_ledger_run_id or latest_decision_ledger_run(con)
    if not run_id:
        raise SystemExit("No decision ledger run found.")

    rows = query_decisions(con, run_id)
    overrides, counts = build_overrides(rows, set(args.hold_boxscore_id or []))

    out_dir = args.out_root / f"{stamp()}_{args.label}"
    csv_path = out_dir / "decision_input.csv"
    summary_path = out_dir / "summary.json"
    write_csv(csv_path, overrides)
    summary = {
        "created_at_utc": iso_now(),
        "decision_ledger_run_id": run_id,
        "decision_input_csv": str(csv_path),
        "decision_rows_loaded": len(rows),
        "override_rows": len(overrides),
        "counts": dict(counts),
        "hold_boxscore_ids": sorted(set(args.hold_boxscore_id or [])),
    }
    summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
