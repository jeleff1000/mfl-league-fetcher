"""Re-materialize identity-held newspaper scoring and play-by-play events."""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd


EVENT_SPECS = {
    "scoring_event": [
        ("scoring_player", "eff_scoring_player_raw", "eff_scoring_NFL_player_id"),
        ("passer", "eff_passer_raw", "eff_passer_NFL_player_id"),
        ("receiver", "eff_receiver_raw", "eff_receiver_NFL_player_id"),
    ],
    "play_by_play_event": [
        ("primary_player", "eff_primary_player_raw", "eff_primary_NFL_player_id"),
        ("secondary_player", "eff_secondary_player_raw", "eff_secondary_NFL_player_id"),
    ],
}

ROLE_ALIASES = {
    "scoring": "scoring_player",
    "primary": "primary_player",
    "secondary": "secondary_player",
}

SCORING_COLUMNS = [
    "boxscore_id", "scoring_team", "event_type", "points", "distance_yards",
    "scoring_NFL_player_id", "passer_NFL_player_id", "receiver_NFL_player_id",
    "play_text", "target_entity_key", "source_document_id", "source_documents_json",
    "confidence_bar", "confidence_score", "event_order", "period_raw", "clock_raw",
    "scoring_team_raw", "scoring_player_raw", "passer_raw", "receiver_raw",
    "identity_resolution", "resolution_task_ids_json",
]

PBP_COLUMNS = [
    "boxscore_id", "possession_team", "play_type", "yards", "points",
    "primary_NFL_player_id", "secondary_NFL_player_id", "play_text",
    "target_entity_key", "source_document_id", "source_documents_json",
    "confidence_bar", "confidence_score", "event_order", "period_raw", "clock_raw",
    "possession_team_raw", "yardline_raw", "down_raw", "distance_raw",
    "primary_player_raw", "secondary_player_raw", "identity_resolution",
    "resolution_task_ids_json",
]


def _clean(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    text = str(value).strip()
    return "" if text.lower() in {"nan", "none", "null"} else text


def _norm(value: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", _clean(value).lower())


def _canonical_role(value: object) -> str:
    role = _clean(value).lower()
    return ROLE_ALIASES.get(role, role)


def _field(row: pd.Series, name: str) -> object:
    value = row.get(f"eff_{name}")
    return value if _clean(value) else row.get(name)


def _resolved_lookup(tasks: pd.DataFrame, ledger: pd.DataFrame) -> dict[tuple[str, str, str, str], list[dict]]:
    joined = tasks.merge(ledger, on="identity_task_id", how="inner")
    joined = joined.loc[joined["verdict"].astype(str).str.startswith("resolved_")].copy()
    lookup: dict[tuple[str, str, str, str], list[dict]] = {}
    for _, row in joined.iterrows():
        key = (
            _clean(row.get("source_surface")),
            _clean(row.get("boxscore_id_x") or row.get("boxscore_id_y") or row.get("boxscore_id")),
            _norm(row.get("raw_player_x") or row.get("raw_player_y") or row.get("raw_player")),
            _canonical_role(row.get("role")),
        )
        lookup.setdefault(key, []).append(
            {
                "task_id": _clean(row.get("identity_task_id")),
                "nfl_id": _clean(row.get("resolved_nfl_id")),
                "player": _clean(row.get("resolved_player")),
                "verdict": _clean(row.get("verdict")),
            }
        )
    return lookup


def _base_record(row: pd.Series, target: str) -> dict:
    if target == "scoring_event":
        fields = {
            "boxscore_id": "boxscore_id", "scoring_team": "scoring_team",
            "event_type": "event_type", "points": "points", "distance_yards": "distance_yards",
            "play_text": "play_text", "target_entity_key": "target_entity_key",
            "source_document_id": "source_document_id", "source_documents_json": "source_documents_json",
            "confidence_bar": "confidence_bar", "confidence_score": "confidence_score",
            "event_order": "event_order", "period_raw": "period_raw", "clock_raw": "clock_raw",
            "scoring_team_raw": "scoring_team_raw", "scoring_player_raw": "scoring_player_raw",
            "passer_raw": "passer_raw", "receiver_raw": "receiver_raw",
        }
        columns = SCORING_COLUMNS
    else:
        fields = {
            "boxscore_id": "boxscore_id", "possession_team": "possession_team",
            "play_type": "play_type", "yards": "yards", "points": "points",
            "play_text": "play_text", "target_entity_key": "target_entity_key",
            "source_document_id": "source_document_id", "source_documents_json": "source_documents_json",
            "confidence_bar": "confidence_bar", "confidence_score": "confidence_score",
            "event_order": "event_order", "period_raw": "period_raw", "clock_raw": "clock_raw",
            "possession_team_raw": "possession_team_raw", "yardline_raw": "yardline_raw",
            "down_raw": "down_raw", "distance_raw": "distance_raw",
            "primary_player_raw": "primary_player_raw", "secondary_player_raw": "secondary_player_raw",
        }
        columns = PBP_COLUMNS
    record = {output: _field(row, source) for output, source in fields.items()}
    return {column: record.get(column) for column in columns}


def materialize_resolved_events(
    tasks: pd.DataFrame, ledger: pd.DataFrame, expanded: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict[str, int]]:
    lookup = _resolved_lookup(tasks, ledger)
    relevant = expanded.loc[expanded["target_table"].isin(EVENT_SPECS)].copy()
    fallback_key = (
        relevant["pilot_source_witness_row_key"]
        if "pilot_source_witness_row_key" in relevant
        else pd.Series(relevant.index.astype(str), index=relevant.index)
    )
    relevant["_dedup_key"] = relevant["target_entity_key"].fillna(fallback_key)
    if "eff_created_at_utc" in relevant:
        relevant = relevant.sort_values("eff_created_at_utc")
    relevant = relevant.drop_duplicates("_dedup_key", keep="last")

    scoring_rows: list[dict] = []
    pbp_rows: list[dict] = []
    hold_rows: list[dict] = []
    collision_count = 0
    for _, row in relevant.iterrows():
        target = _clean(row.get("target_table"))
        surface = target
        boxscore_id = _clean(row.get("boxscore_id"))
        patched_ids: dict[str, str] = {}
        unresolved: list[str] = []
        used: list[dict] = []
        collisions: list[str] = []
        role_values: dict[str, tuple[str, str]] = {}

        for role, raw_field, id_field in EVENT_SPECS[target]:
            raw_player = _field(row, raw_field.removeprefix("eff_"))
            current_id = _clean(_field(row, id_field.removeprefix("eff_")))
            matches = lookup.get((surface, boxscore_id, _norm(raw_player), role), [])
            ids = {_clean(match["nfl_id"]) for match in matches if _clean(match["nfl_id"])}
            if not current_id and len(ids) == 1:
                current_id = next(iter(ids))
                used.extend(matches)
            elif not current_id and len(ids) > 1:
                collisions.append(role)
            role_values[role] = (_clean(raw_player), current_id)
            patched_ids[id_field.removeprefix("eff_")] = current_id

        if target == "scoring_event":
            scoring_raw, scoring_id = role_values["scoring_player"]
            receiver_raw, receiver_id = role_values["receiver"]
            if scoring_raw and scoring_raw == receiver_raw:
                shared = scoring_id or receiver_id
                if shared:
                    patched_ids["scoring_NFL_player_id"] = shared
                    patched_ids["receiver_NFL_player_id"] = shared
                    role_values["scoring_player"] = (scoring_raw, shared)
                    role_values["receiver"] = (receiver_raw, shared)

        touched = bool(used or collisions)
        if not touched:
            continue
        for role, (raw_player, resolved_id) in role_values.items():
            if raw_player and not resolved_id:
                unresolved.append(role)
        if collisions:
            collision_count += 1
        if unresolved or collisions:
            hold_rows.append(
                {
                    "target_table": target,
                    "target_entity_key": _clean(row.get("target_entity_key")),
                    "boxscore_id": boxscore_id,
                    "unresolved_roles_json": json.dumps(sorted(unresolved)),
                    "collision_roles_json": json.dumps(sorted(collisions)),
                }
            )
            continue

        record = _base_record(row, target)
        record.update(patched_ids)
        verdicts = {match["verdict"] for match in used}
        record["identity_resolution"] = (
            "ledger_resolved_external_witness_corroborated"
            if "resolved_external_witness_corroborated" in verdicts
            else "ledger_resolved_pfr_corroborated"
        )
        record["resolution_task_ids_json"] = json.dumps(sorted({match["task_id"] for match in used}))
        (scoring_rows if target == "scoring_event" else pbp_rows).append(record)

    scoring = pd.DataFrame(scoring_rows, columns=SCORING_COLUMNS)
    pbp = pd.DataFrame(pbp_rows, columns=PBP_COLUMNS)
    holds = pd.DataFrame(
        hold_rows,
        columns=["target_table", "target_entity_key", "boxscore_id", "unresolved_roles_json", "collision_roles_json"],
    )
    summary = {
        "scoring_events": len(scoring),
        "play_by_play_events": len(pbp),
        "partial_holds": len(holds),
        "collisions": collision_count,
    }
    return scoring, pbp, holds, summary


def _read(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path, low_memory=False)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--expanded", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    scoring, pbp, holds, summary = materialize_resolved_events(
        _read(args.tasks), _read(args.ledger), _read(args.expanded)
    )
    args.out.mkdir(parents=True, exist_ok=True)
    scoring.to_parquet(args.out / "resolved_scoring_events.parquet", index=False)
    pbp.to_parquet(args.out / "resolved_play_by_play_events.parquet", index=False)
    holds.to_parquet(args.out / "resolved_event_holds.parquet", index=False)
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
