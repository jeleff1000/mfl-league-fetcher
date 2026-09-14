from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import duckdb

from scripts.sota_recon.witness_gate.identity_lane import (
    IdentityCandidate,
    IdentityQuery,
    resolve_identity,
)


@dataclass(frozen=True)
class ResolvedNewspaperIdentity:
    identity_task_id: str
    boxscore_id: str
    raw_player: str
    team: str
    year: int
    week: int | None
    nfl_player_id: str
    canonical_player: str
    proof_leaves: tuple[str, ...]


@dataclass(frozen=True)
class CandidateUnlock:
    surface: str
    target_entity_key: str
    boxscore_id: str
    strict_hold_reason: str
    raw_player: str
    nfl_player_id: str
    canonical_player: str
    player_week: str | None
    identity_task_id: str
    proof_leaves: tuple[str, ...]


_ROLE_FIELDS: dict[str, dict[str, str]] = {
    "weekly_player_stat_cell": {"missing_player_week": "player_raw"},
    "lineup_participation": {"missing_player_week": "player_raw"},
    "player_game_note": {"missing_player_week": "player_raw"},
    "scoring_event": {
        "missing_scoring_player_identity": "scoring_player_raw",
        "missing_passer_identity": "passer_player_raw",
        "missing_receiver_identity": "receiver_player_raw",
    },
    "play_by_play_event": {
        "missing_primary_player_identity": "primary_player_raw",
        "missing_secondary_player_identity": "secondary_player_raw",
    },
}


def classify_candidate_unlock(
    surface: str,
    row: Mapping[str, str],
    identities: Mapping[tuple[str, str], ResolvedNewspaperIdentity],
) -> CandidateUnlock | None:
    reason = row.get("strict_hold_reason", "")
    raw_field = _ROLE_FIELDS.get(surface, {}).get(reason)
    if raw_field is None:
        return None
    raw_player = row.get(raw_field, "")
    identity = identities.get((row.get("boxscore_id", ""), raw_player))
    if identity is None:
        return None
    player_week = None
    if identity.week is not None:
        player_week = f"{identity.nfl_player_id}_{identity.year}_{identity.week}"
    return CandidateUnlock(
        surface=surface,
        target_entity_key=row.get("target_entity_key", ""),
        boxscore_id=identity.boxscore_id,
        strict_hold_reason=reason,
        raw_player=raw_player,
        nfl_player_id=identity.nfl_player_id,
        canonical_player=identity.canonical_player,
        player_week=player_week,
        identity_task_id=identity.identity_task_id,
        proof_leaves=identity.proof_leaves,
    )


def _integer(value: str) -> int | None:
    if not value:
        return None
    return int(float(value))


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _candidate_index(subject_path: Path) -> dict[tuple[int, str], tuple[IdentityCandidate, ...]]:
    connection = duckdb.connect()
    safe_path = subject_path.as_posix().replace("'", "''")
    rows = connection.execute(
        f"""
        SELECT DISTINCT CAST(year AS INTEGER), nfl_team, NFL_player_id, player, position
        FROM read_parquet('{safe_path}')
        WHERE year IS NOT NULL
          AND nfl_team IS NOT NULL
          AND NFL_player_id IS NOT NULL
          AND player IS NOT NULL
        """
    ).fetchall()
    grouped: dict[tuple[int, str], list[IdentityCandidate]] = defaultdict(list)
    for year, team, player_id, player, position in rows:
        grouped[(year, team)].append(
            IdentityCandidate(
                player_id=player_id,
                name=player,
                team=team,
                year_start=year,
                year_end=year,
                position=position,
                source_id="pfr-backed-player-team-season",
                proof_leaf=f"leaf:pfr-backed-player-team-season:{player_id}:{year}:{team}",
                precedence=10,
            )
        )
    return {key: tuple(value) for key, value in grouped.items()}


def _resolve_queue(
    queue: Iterable[dict[str, str]],
    candidates: Mapping[tuple[int, str], tuple[IdentityCandidate, ...]],
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], ResolvedNewspaperIdentity]]:
    ledger: list[dict[str, Any]] = []
    resolved: dict[tuple[str, str], ResolvedNewspaperIdentity] = {}
    for row in queue:
        year = _integer(row.get("derived_year", "") or row.get("year", ""))
        if year is None:
            continue
        team = row.get("resolved_team", "") or row.get("nfl_team", "")
        decision = resolve_identity(
            IdentityQuery(row.get("raw_player", ""), team, year),
            candidates.get((year, team), ()),
        )
        selected = decision.selected_candidate
        record: dict[str, Any] = {
            "identity_task_id": row.get("identity_task_id", ""),
            "boxscore_id": row.get("boxscore_id", ""),
            "raw_player": row.get("raw_player", ""),
            "team": team,
            "year": year,
            "week": _integer(row.get("derived_week", "") or row.get("week", "")),
            "status": decision.status,
            "nfl_player_id": decision.player_id or "",
            "canonical_player": selected.name if selected else "",
            "candidate_count": len(decision.candidates),
            "proof_leaves": json.dumps(decision.proof_leaves),
        }
        ledger.append(record)
        if decision.status != "resolved" or selected is None:
            continue
        identity = ResolvedNewspaperIdentity(
            identity_task_id=record["identity_task_id"],
            boxscore_id=record["boxscore_id"],
            raw_player=record["raw_player"],
            team=team,
            year=year,
            week=record["week"],
            nfl_player_id=selected.player_id,
            canonical_player=selected.name,
            proof_leaves=decision.proof_leaves,
        )
        key = (identity.boxscore_id, identity.raw_player)
        prior = resolved.get(key)
        if prior is not None and prior.nfl_player_id != identity.nfl_player_id:
            raise ValueError(f"identity collision after exact context grouping: {key}")
        resolved[key] = identity
    return ledger, resolved


def _write_csv(path: Path, records: list[dict[str, Any]]) -> None:
    if not records:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(records[0]))
        writer.writeheader()
        writer.writerows(records)


def _checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _csv_to_parquet(csv_path: Path, parquet_path: Path) -> None:
    safe_csv = csv_path.as_posix().replace("'", "''")
    safe_parquet = parquet_path.as_posix().replace("'", "''")
    duckdb.connect().execute(
        f"COPY (SELECT * FROM read_csv_auto('{safe_csv}', header = true, all_varchar = true)) "
        f"TO '{safe_parquet}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def run_replay(staging_dir: Path, subject_path: Path, output_dir: Path) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=False)
    queue_path = staging_dir / "manual_identity_resolution_queue.csv"
    queue = _read_csv(queue_path)
    decision_ledger, resolved = _resolve_queue(queue, _candidate_index(subject_path))

    unlocks: list[CandidateUnlock] = []
    for surface in _ROLE_FIELDS:
        path = staging_dir / f"{surface}_candidates_strict_hold.csv"
        for row in _read_csv(path):
            unlock = classify_candidate_unlock(surface, row, resolved)
            if unlock is not None:
                unlocks.append(unlock)

    aliases = sorted(
        {
            (
                item.raw_player,
                item.team,
                item.year,
                item.nfl_player_id,
                item.canonical_player,
                json.dumps(item.proof_leaves),
            )
            for item in resolved.values()
        }
    )
    alias_records = [
        {
            "raw_player": raw,
            "team": team,
            "year": year,
            "NFL_player_id": player_id,
            "canonical_player": canonical,
            "proof_leaves": leaves,
            "application_scope": "team_year_context_required",
        }
        for raw, team, year, player_id, canonical, leaves in aliases
    ]
    unlock_records = []
    for item in unlocks:
        record = asdict(item)
        record["proof_leaves"] = json.dumps(item.proof_leaves)
        unlock_records.append(record)

    files = {
        "identity_decisions": output_dir / "newspaper_identity_decisions.csv",
        "context_aliases": output_dir / "player_bio_context_alias_overlay.csv",
        "candidate_unlocks": output_dir / "newspaper_candidate_unlock_ledger.csv",
    }
    _write_csv(files["identity_decisions"], decision_ledger)
    _write_csv(files["context_aliases"], alias_records)
    _write_csv(files["candidate_unlocks"], unlock_records)
    parquet_files = {
        "identity_decisions_parquet": output_dir / "newspaper_identity_decisions.parquet",
        "context_aliases_parquet": output_dir / "player_bio_context_alias_overlay.parquet",
        "candidate_unlocks_parquet": output_dir / "newspaper_candidate_unlock_ledger.parquet",
    }
    _csv_to_parquet(files["identity_decisions"], parquet_files["identity_decisions_parquet"])
    _csv_to_parquet(files["context_aliases"], parquet_files["context_aliases_parquet"])
    _csv_to_parquet(files["candidate_unlocks"], parquet_files["candidate_unlocks_parquet"])
    files.update(parquet_files)

    unlock_counts = Counter(item.surface for item in unlocks)
    status_counts = Counter(row["status"] for row in decision_ledger)
    summary: dict[str, Any] = {
        "created_at_utc": datetime.now(UTC).isoformat(),
        "algorithm": "team+fuzzy_name+year_exactly_one_candidate.v1",
        "fuzzy_threshold": 85.0,
        "queue_rows": len(queue),
        "identity_status_counts": dict(sorted(status_counts.items())),
        "resolved_contexts": len(resolved),
        "context_alias_rows": len(alias_records),
        "canonical_player_bio_rows_changed": 0,
        "canonical_player_bio_change_reason": (
            "All resolved NFL_player_id rows already exist in player_bio; misspellings and nicknames "
            "are stored as context-scoped aliases to avoid overwriting canonical names."
        ),
        "unlocked_candidate_atoms": len(unlocks),
        "unlocked_by_surface": dict(sorted(unlock_counts.items())),
        "remaining_identity_queue_rows": len(queue) - status_counts["resolved"],
        "inputs": {
            "staging_dir": str(staging_dir),
            "identity_queue": str(queue_path),
            "subject_player_team_history": str(subject_path),
        },
        "outputs": {key: str(value) for key, value in files.items()},
        "checksums_sha256": {key: _checksum(value) for key, value in files.items()},
    }
    summary_path = output_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
    return summary


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay newspaper identity holds against team/name/year evidence.")
    parser.add_argument("--staging-dir", type=Path, required=True)
    parser.add_argument("--subject", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    summary = run_replay(args.staging_dir, args.subject, args.output_dir)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
