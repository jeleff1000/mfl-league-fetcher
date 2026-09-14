"""Resolve newspaper identity tasks with independent roster/participation witnesses."""
from __future__ import annotations

import argparse
import json
import re
import shutil
from datetime import UTC, datetime
from difflib import SequenceMatcher
from pathlib import Path

import duckdb
import pandas as pd

from .recon_common import utc_stamp
from .witness_gate.identity_lane import IdentityCandidate, IdentityQuery
from .witness_gate.roster_collision_resolution import (
    build_collision_ledgers,
    normalize_id_aliases,
    propose_collision_bio_patch,
    resolve_collision_batch,
)

DEFAULT_DB = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\databases\newspaper_atoms.duckdb")
DEFAULT_BIO = Path(r"D:\league-history-data\nfl\ops_data\nfl_historical\player_bio.parquet")

# PFR/newspaper queues use compact historical team codes while roster witnesses
# generally spell out the contemporary team name. Keep this identity crosswalk
# local to the witness resolver: it is evidence matching, not franchise lineage
# normalization for the supertable.
HISTORICAL_TEAM_NAMES: dict[str, tuple[str, ...]] = {
    "AKR": ("Akron Pros", "Akron Indians"),
    "BKN": ("Brooklyn Dodgers", "Brooklyn Tigers"),
    "BOS": ("Boston Braves", "Boston Redskins", "Boston Yanks"),
    "BUF": ("Buffalo All-Americans", "Buffalo All Americans", "Buffalo Bisons", "Buffalo Rangers"),
    "CAN": ("Canton Bulldogs",),
    "CHI": ("Chicago Bears", "Chicago Staleys", "Decatur Staleys"),
    "CIN": ("Cincinnati Celts", "Cincinnati Reds"),
    "COL": ("Columbus Panhandles", "Columbus Tigers"),
    "CRD": ("Chicago Cardinals",),
    "DAY": ("Dayton Triangles",),
    "DET": ("Detroit Tigers", "Detroit Panthers", "Detroit Wolverines", "Detroit Lions"),
    "DUL": ("Duluth Kelleys", "Duluth Eskimos"),
    "EVN": ("Evansville Crimson Giants",),
    "FRN": ("Frankford Yellow Jackets",),
    "GNB": ("Green Bay Packers",),
    "KAN": ("Kansas City Blues", "Kansas City Cowboys"),
    "LOU": ("Louisville Brecks", "Louisville Colonels"),
    "MIL": ("Milwaukee Badgers",),
    "MIN": ("Minneapolis Marines", "Minneapolis Red Jackets"),
    "NYG": ("New York Giants",),
    "OOR": ("Oorang Indians",),
    "PHI": ("Philadelphia Eagles",),
    "PIT": ("Pittsburgh Pirates", "Pittsburgh Steelers"),
    "POT": ("Pottsville Maroons",),
    "PRT": ("Portsmouth Spartans",),
    "PRV": ("Providence Steam Roller",),
    "RAC": ("Racine Legion", "Racine Tornadoes"),
    "RAM": ("Cleveland Rams", "Los Angeles Rams"),
    "RCH": ("Rochester Jeffersons",),
    "RII": ("Rock Island Independents",),
    "STL": ("St Louis Gunners", "St. Louis Gunners", "St Louis Cardinals", "St. Louis Cardinals"),
    "TOL": ("Toledo Maroons",),
    "WAS": ("Washington Redskins",),
}


def _tokens(value: object) -> list[str]:
    text = re.sub(r"[^a-z0-9]+", " ", str(value or "").lower()).strip()
    tokens = [token for token in text.split() if token not in {"jr", "sr", "ii", "iii", "iv"}]
    return tokens


def name_score(left: object, right: object) -> float:
    a, b = _tokens(left), _tokens(right)
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if len(a) == 1 or len(b) == 1:
        short, full = (a, b) if len(a) == 1 else (b, a)
        return 0.96 * SequenceMatcher(None, short[0], full[-1]).ratio()
    last = SequenceMatcher(None, a[-1], b[-1]).ratio()
    first = 1.0 if a[0] == b[0] else (0.94 if a[0][0] == b[0][0] else SequenceMatcher(None, a[0], b[0]).ratio())
    return 0.65 * last + 0.35 * first


def _team_alias_index() -> dict[str, str]:
    aliases: dict[str, str] = {}
    for code, names in HISTORICAL_TEAM_NAMES.items():
        aliases["".join(_tokens(code))] = code
        for name in names:
            aliases["".join(_tokens(name))] = code
    return aliases


TEAM_ALIAS_INDEX = _team_alias_index()


def team_score(left: object, right: object) -> float:
    a, b = "".join(_tokens(left)), "".join(_tokens(right))
    if not a or not b:
        return 0.0
    if a == b:
        return 1.0
    if TEAM_ALIAS_INDEX.get(a) == TEAM_ALIAS_INDEX.get(b) and a in TEAM_ALIAS_INDEX and b in TEAM_ALIAS_INDEX:
        return 1.0
    return SequenceMatcher(None, a, b).ratio()


def _active_in_year(row: object, year: int) -> bool:
    first = pd.to_numeric(row.get("first_year"), errors="coerce")
    last = pd.to_numeric(row.get("last_year"), errors="coerce")
    return (pd.isna(first) or first <= year) and (pd.isna(last) or last >= year)


def reconcile_external_witnesses(
    tasks: pd.DataFrame, witnesses: pd.DataFrame, bio: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame, dict]:
    decisions: list[dict] = []
    additions: dict[str, dict] = {}
    bio_match_cache: dict[tuple[int, str, str], list[tuple[str, object, float]]] = {}
    bio_records = bio.to_dict("records")
    witness_records_by_year: dict[int, list[dict]] = {}
    for witness in witnesses.to_dict("records"):
        season_value = pd.to_numeric(witness.get("season"), errors="coerce")
        if pd.notna(season_value):
            witness_records_by_year.setdefault(int(season_value), []).append(witness)
    bio_name_buckets: dict[str, list[dict]] = {}
    bio_id_index: dict[str, dict] = {}
    for person in bio_records:
        player_tokens = _tokens(person.get("player"))
        if player_tokens:
            bio_name_buckets.setdefault(player_tokens[-1][0], []).append(person)
        for value in (person.get("NFL_player_id"), person.get("pfr_id")):
            if value is not None and str(value):
                bio_id_index[str(value)] = person
    for index, task in tasks.iterrows():
        task_id = str(task.get("task_id") or task.get("identity_task_id") or task.get("readiness_row_id") or index)
        year_value = task.get("year")
        if pd.isna(year_value) or year_value is None:
            match = re.search(r"_(\d{4})_", str(task.get("player_week") or ""))
            if not match:
                raise ValueError(f"task {task_id} has no year or parseable player_week")
            year_value = match.group(1)
        year = int(float(year_value))
        raw_player = task.get("raw_player") or task.get("player_raw")
        raw_team = task.get("raw_team") or task.get("nfl_team") or task.get("np_team") or task.get("resolved_team")
        candidates = pd.DataFrame(witness_records_by_year.get(year, ()), columns=witnesses.columns)
        candidates["task_team_score"] = candidates["team"].map(
            lambda value, task_team=raw_team: team_score(task_team, value)
        )
        candidates["task_name_score"] = candidates["player"].map(
            lambda value, task_player=raw_player: name_score(task_player, value)
        )
        candidates = candidates.loc[(candidates["task_team_score"] >= 0.88) & (candidates["task_name_score"] >= 0.78)]

        bio_ids: dict[str, dict] = {}
        for _, witness in candidates.iterrows():
            source_id = str(witness.get("source_player_id") or "")
            cache_key = (year, str(witness["player"]), source_id)
            if cache_key not in bio_match_cache:
                matches: list[tuple[str, object, float]] = []
                witness_tokens = _tokens(witness["player"])
                pool = bio_name_buckets.get(witness_tokens[-1][0], []) if witness_tokens else []
                direct_person = bio_id_index.get(source_id)
                if direct_person is not None and direct_person not in pool:
                    pool = [direct_person, *pool]
                for person in pool:
                    if not _active_in_year(person, year):
                        continue
                    direct = source_id and source_id in {str(person.get("NFL_player_id") or ""), str(person.get("pfr_id") or "")}
                    score = 1.0 if direct else name_score(witness["player"], person.get("player"))
                    person_id = str(person.get("NFL_player_id") or "")
                    if person_id and score >= 0.88:
                        matches.append((person_id, person.get("player"), score))
                bio_match_cache[cache_key] = matches
            for person_id, player, score in bio_match_cache[cache_key]:
                bio_ids[person_id] = {"player": player, "score": score}

        status = "held_no_candidate"
        resolved_id = None
        resolved_player = None
        if len(bio_ids) == 1:
            resolved_id, detail = next(iter(bio_ids.items()))
            resolved_player = detail["player"]
            status = "resolved_existing_bio"
        elif len(bio_ids) > 1:
            status = "held_multiple_candidates"
        elif len(candidates):
            proposed_id = str(task.get("nid") or task.get("proposed_NFL_player_id") or "").strip()
            witness_names = sorted(set(candidates["player"].dropna().astype(str)))
            if proposed_id and witness_names and all(name_score(witness_names[0], value) >= 0.92 for value in witness_names[1:]):
                resolved_id, resolved_player, status = proposed_id, witness_names[0], "resolved_new_bio"
                additions[proposed_id] = {
                    "NFL_player_id": proposed_id,
                    "player": resolved_player,
                    "first_year": year,
                    "last_year": year,
                    "pfr_id": proposed_id,
                    "external_witness_sources": ",".join(sorted(set(candidates["source"].astype(str)))),
                    "external_witness_urls": json.dumps(sorted(set(candidates["source_url"].astype(str)))),
                }
        decisions.append(
            {
                "task_id": task_id,
                "target_table": task.get("target_table"),
                "target_entity_key": task.get("target_entity_key"),
                "role": task.get("role"),
                "target_id_field": task.get("target_id_field"),
                "boxscore_id": task.get("boxscore_id"),
                "year": year,
                "raw_player": raw_player,
                "raw_team": raw_team,
                "resolution_status": status,
                "resolved_NFL_player_id": resolved_id,
                "resolved_player": resolved_player,
                "witness_candidate_count": len(candidates),
                "bio_candidate_count": len(bio_ids),
                "witness_sources_json": json.dumps(sorted(set(candidates["source"].astype(str)))) if len(candidates) else "[]",
                "witness_urls_json": json.dumps(sorted(set(candidates["source_url"].astype(str)))) if len(candidates) else "[]",
                "proposed_patch_json": json.dumps(
                    {
                        str(task.get("target_id_field") or "NFL_player_id"): resolved_id,
                        "resolved_player": resolved_player,
                        "resolved_player_week": f"{resolved_id}_{year}_{task.get('week')}",
                    },
                    sort_keys=True,
                ) if resolved_id else None,
            }
        )
    decision_frame = pd.DataFrame(decisions)
    addition_frame = pd.DataFrame(additions.values())
    unlocked = int(decision_frame["resolution_status"].isin({"resolved_existing_bio", "resolved_new_bio"}).sum()) if len(decision_frame) else 0
    summary = {
        "task_count": len(tasks),
        "unlocked_tasks": unlocked,
        "bio_additions": len(addition_frame),
        "status_counts": decision_frame["resolution_status"].value_counts().to_dict() if len(decision_frame) else {},
    }
    return decision_frame, addition_frame, summary


def external_resolution_ledger(decisions: pd.DataFrame) -> pd.DataFrame:
    """Project resolved decisions into the newspaper identity-ledger contract."""
    columns = [
        "identity_task_id",
        "boxscore_id",
        "yr",
        "nfl_team",
        "raw_player",
        "verdict",
        "resolved_nfl_id",
        "resolved_player",
        "n_cand",
        "n_appear",
        "witness_candidate_count",
        "witness_sources_json",
        "witness_urls_json",
    ]
    if decisions.empty:
        return pd.DataFrame(columns=columns)
    resolved = decisions.loc[
        decisions["resolution_status"].isin({"resolved_existing_bio", "resolved_new_bio"})
    ].copy()
    if resolved.empty:
        return pd.DataFrame(columns=columns)
    ledger = pd.DataFrame(
        {
            "identity_task_id": resolved["task_id"],
            "boxscore_id": resolved["boxscore_id"],
            "yr": resolved["year"],
            "nfl_team": resolved["raw_team"],
            "raw_player": resolved["raw_player"],
            "verdict": "resolved_external_witness_corroborated",
            "resolved_nfl_id": resolved["resolved_NFL_player_id"],
            "resolved_player": resolved["resolved_player"],
            "n_cand": resolved["bio_candidate_count"],
            "n_appear": pd.NA,
            "witness_candidate_count": resolved["witness_candidate_count"],
            "witness_sources_json": resolved["witness_sources_json"],
            "witness_urls_json": resolved["witness_urls_json"],
        }
    )
    return ledger[columns]


def _clean_id(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def merge_resolution_ledgers(
    base: pd.DataFrame,
    external: pd.DataFrame,
    id_aliases: dict[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, int]]:
    """Merge external resolutions without overriding a conflicting resolved ID."""
    all_columns = list(dict.fromkeys([*base.columns, *external.columns]))
    combined = base.reindex(columns=all_columns).copy()
    incoming = external.reindex(columns=all_columns)
    task_to_index = {
        str(task_id): index for index, task_id in combined["identity_task_id"].items()
    }
    collisions: list[dict] = []
    summary = {"added": 0, "upgraded_holds": 0, "corroborated_existing": 0, "collisions": 0}

    for _, row in incoming.iterrows():
        task_id = str(row["identity_task_id"])
        existing_index = task_to_index.get(task_id)
        if existing_index is None:
            combined = pd.concat([combined, row.to_frame().T], ignore_index=True)
            task_to_index[task_id] = combined.index[-1]
            summary["added"] += 1
            continue

        existing = combined.loc[existing_index]
        existing_is_resolved = str(existing.get("verdict") or "").startswith("resolved_")
        if not existing_is_resolved:
            combined.loc[existing_index, all_columns] = row[all_columns].values
            summary["upgraded_holds"] += 1
            continue

        base_id = _clean_id(existing.get("resolved_nfl_id"))
        external_id = _clean_id(row.get("resolved_nfl_id"))
        canonical_base_id = (id_aliases or {}).get(base_id, base_id)
        canonical_external_id = (id_aliases or {}).get(external_id, external_id)
        if canonical_base_id == canonical_external_id:
            summary["corroborated_existing"] += 1
            continue
        collisions.append(
            {
                "identity_task_id": task_id,
                "base_verdict": existing.get("verdict"),
                "base_resolved_nfl_id": base_id,
                "external_verdict": row.get("verdict"),
                "external_resolved_nfl_id": external_id,
            }
        )
        summary["collisions"] += 1

    collision_columns = [
        "identity_task_id",
        "base_verdict",
        "base_resolved_nfl_id",
        "external_verdict",
        "external_resolved_nfl_id",
    ]
    return combined, pd.DataFrame(collisions, columns=collision_columns), summary


def _read_table(path: Path) -> pd.DataFrame:
    return pd.read_parquet(path) if path.suffix.lower() == ".parquet" else pd.read_csv(path)


def _latest_unresolved_tasks(db_path: Path) -> pd.DataFrame:
    con = duckdb.connect(str(db_path), read_only=True)
    run_id = con.execute(
        "SELECT identity_resolution_run_id FROM newspaper_review.identity_resolution_readiness_run ORDER BY created_at_utc DESC LIMIT 1"
    ).fetchone()[0]
    return con.execute(
        "SELECT * FROM newspaper_review.identity_resolution_readiness_task WHERE identity_resolution_run_id = ? AND resolution_status <> 'auto_resolved'",
        [run_id],
    ).df()


def apply_bio_additions(bio_path: Path, additions: pd.DataFrame, ledger_path: Path) -> int:
    if additions.empty:
        return 0
    bio = pd.read_parquet(bio_path)
    new = additions.loc[~additions["NFL_player_id"].isin(bio["NFL_player_id"])].copy()
    if new.empty:
        return 0
    for column in bio.columns:
        if column not in new.columns:
            new[column] = None
    backup = bio_path.with_suffix(f".pre_external_witness_{utc_stamp()}.parquet")
    shutil.copy2(bio_path, backup)
    pd.concat([bio, new[bio.columns]], ignore_index=True).to_parquet(bio_path, index=False)
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    new.to_parquet(ledger_path, index=False)
    return len(new)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--resolve-roster-collisions", action="store_true")
    mode.add_argument("--apply-proposed-bio-patch", type=Path)
    parser.add_argument("--witness-index", type=Path)
    parser.add_argument("--tasks", type=Path)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--player-bio", type=Path, default=DEFAULT_BIO)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--base-ledger", type=Path)
    parser.add_argument("--apply-bio", action="store_true")
    parser.add_argument("--pfa-materialization", type=Path)
    parser.add_argument("--decision-ledger", type=Path)
    parser.add_argument("--evidence-ledger", type=Path)
    parser.add_argument("--proposed-bio-patch", type=Path)
    return parser


def _tuple_field(value: object) -> tuple[str, ...]:
    if value is None or (not isinstance(value, (list, tuple)) and pd.isna(value)):
        return ()
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()
        if stripped.startswith("["):
            return tuple(str(item) for item in json.loads(stripped))
        return (stripped,)
    return tuple(str(item) for item in value)


def _bio_candidate(row: dict[str, object]) -> IdentityCandidate:
    source_ids: list[str] = []
    for field_name in ("pfr_id", "gsis_id", "source_ids", "id_aliases"):
        source_ids.extend(_tuple_field(row.get(field_name)))
    teams = _tuple_field(row.get("teams"))
    if not teams:
        teams = _tuple_field(row.get("team") or row.get("nfl_team"))
    return IdentityCandidate(
        player_id=_clean_id(row.get("NFL_player_id")),
        name=str(row.get("player") or ""),
        start_year=int(row["first_year"]) if pd.notna(row.get("first_year")) else None,
        end_year=int(row["last_year"]) if pd.notna(row.get("last_year")) else None,
        teams=teams,
        position=_clean_id(row.get("position")) or None,
        nfl_position=_clean_id(row.get("nfl_position")) or None,
        aliases=_tuple_field(row.get("aliases")),
        source_ids=tuple(sorted(set(source_ids))),
        proof_leaf=f"player_bio:{_clean_id(row.get('NFL_player_id'))}",
    )


def _run_collision_resolution(args: argparse.Namespace, parser: argparse.ArgumentParser) -> int:
    required = {
        "--tasks": args.tasks,
        "--pfa-materialization": args.pfa_materialization,
        "--decision-ledger": args.decision_ledger,
        "--evidence-ledger": args.evidence_ledger,
        "--proposed-bio-patch": args.proposed_bio_patch,
    }
    missing = [name for name, value in required.items() if value is None]
    if missing:
        parser.error(f"--resolve-roster-collisions requires {', '.join(missing)}")
    tasks = _read_table(args.tasks)
    bio = pd.read_parquet(args.player_bio)
    bio_records = bio.to_dict("records")
    bio_index = {
        _clean_id(row.get("NFL_player_id")): row
        for row in bio_records
        if _clean_id(row.get("NFL_player_id"))
    }
    candidates_by_id = {player_id: _bio_candidate(row) for player_id, row in bio_index.items()}
    items: list[tuple[str, IdentityQuery, list[IdentityCandidate]]] = []
    for row_number, task in enumerate(tasks.to_dict("records")):
        collision_id = _clean_id(task.get("collision_id") or task.get("task_id")) or str(row_number)
        candidate_ids = _tuple_field(
            task.get("candidate_ids_json") or task.get("candidate_NFL_player_ids_json")
        )
        missing_ids = sorted(set(candidate_ids) - candidates_by_id.keys())
        if missing_ids:
            raise ValueError(f"collision {collision_id} names unknown bio candidates: {missing_ids}")
        items.append(
            (
                collision_id,
                IdentityQuery(
                    name=str(task.get("raw_player") or task.get("player") or ""),
                    year=int(task["year"]),
                    team=_clean_id(task.get("raw_team") or task.get("team")) or None,
                    position=_clean_id(task.get("position")) or None,
                    nfl_position=_clean_id(task.get("nfl_position")) or None,
                    source_id=_clean_id(task.get("source_id")) or None,
                ),
                [candidates_by_id[player_id] for player_id in candidate_ids],
            )
        )
    resolved = resolve_collision_batch(items, pd.read_parquet(args.pfa_materialization))
    decisions, evidence = build_collision_ledgers(resolved)
    patch_rows: list[dict[str, object]] = []
    for collision_id, decision in resolved:
        existing = bio_index.get(decision.canonical_player_id or "", {})
        patch = propose_collision_bio_patch(existing, decision)
        patch_rows.append(
            {
                "collision_id": collision_id,
                "status": patch.status,
                "player_id": patch.player_id,
                "changes_json": json.dumps(patch.changes, sort_keys=True),
                "reason": patch.reason,
            }
        )
    for path in (args.decision_ledger, args.evidence_ledger, args.proposed_bio_patch):
        path.parent.mkdir(parents=True, exist_ok=True)
    decisions.to_parquet(args.decision_ledger, index=False)
    evidence.to_parquet(args.evidence_ledger, index=False)
    pd.DataFrame(patch_rows).to_parquet(args.proposed_bio_patch, index=False)
    return 0


def _blank_bio_value(value: object) -> bool:
    if value is None or value == "":
        return True
    return not isinstance(value, (list, tuple, dict)) and bool(pd.isna(value))


def apply_proposed_bio_patches(bio_path: Path, patch_path: Path) -> int:
    """Atomically apply reviewed proposals without replacing canonical facts."""
    bio = pd.read_parquet(bio_path)
    patches = _read_table(patch_path)
    updated = bio.copy()
    id_to_index = {
        _clean_id(player_id): index
        for index, player_id in updated["NFL_player_id"].items()
        if _clean_id(player_id)
    }
    pending: list[tuple[object, str, object]] = []
    for patch in patches.to_dict("records"):
        if patch.get("status") != "candidate_patch":
            continue
        player_id = _clean_id(patch.get("player_id"))
        if player_id not in id_to_index:
            raise ValueError(f"bio patch names unknown player_id {player_id!r}")
        changes = json.loads(str(patch.get("changes_json") or "{}"))
        if not isinstance(changes, dict):
            raise ValueError(f"bio patch changes must be an object for {player_id}")
        row_index = id_to_index[player_id]
        for field_name, proposed in changes.items():
            if field_name == "id_aliases":
                existing_aliases = (
                    normalize_id_aliases(updated.at[row_index, field_name])
                    if field_name in updated else ()
                )
                merged_aliases = tuple(sorted(set(existing_aliases) | set(normalize_id_aliases(proposed))))
                if merged_aliases != existing_aliases:
                    pending.append((row_index, field_name, merged_aliases))
                continue
            if field_name not in updated.columns:
                updated[field_name] = None
            current = updated.at[row_index, field_name]
            if not _blank_bio_value(current) and str(current) != str(proposed):
                raise ValueError(
                    f"bio patch for {player_id} refuses to overwrite populated field {field_name}"
                )
            if _blank_bio_value(current):
                pending.append((row_index, field_name, proposed))
    for row_index, field_name, proposed in pending:
        if field_name not in updated.columns:
            updated[field_name] = None
        updated.at[row_index, field_name] = proposed
    if not pending:
        return 0
    backup = bio_path.with_suffix(f".pre_collision_patch_{utc_stamp()}.parquet")
    shutil.copy2(bio_path, backup)
    updated.to_parquet(bio_path, index=False)
    return len(pending)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.resolve_roster_collisions:
        return _run_collision_resolution(args, parser)
    if args.apply_proposed_bio_patch:
        applied = apply_proposed_bio_patches(args.player_bio, args.apply_proposed_bio_patch)
        print(json.dumps({"bio_fields_applied": applied}, indent=2))
        return 0
    if not args.witness_index or not args.out:
        parser.error("external witness mode requires --witness-index and --out")
    tasks = _read_table(args.tasks) if args.tasks else _latest_unresolved_tasks(args.db)
    bio = pd.read_parquet(args.player_bio)
    decisions, additions, summary = reconcile_external_witnesses(tasks, pd.read_parquet(args.witness_index), bio)
    args.out.mkdir(parents=True, exist_ok=True)
    decisions.to_parquet(args.out / "identity_decisions.parquet", index=False)
    additions.to_parquet(args.out / "player_bio_additions.parquet", index=False)
    external_ledger = external_resolution_ledger(decisions)
    external_ledger.to_csv(args.out / "RESOLUTION_LEDGER_EXTERNAL.csv", index=False)
    if args.base_ledger:
        id_aliases = {
            _clean_id(row.get("pfr_id")): _clean_id(row.get("NFL_player_id"))
            for _, row in bio.iterrows()
            if _clean_id(row.get("pfr_id")) and _clean_id(row.get("NFL_player_id"))
        }
        combined, collisions, merge_summary = merge_resolution_ledgers(
            _read_table(args.base_ledger), external_ledger, id_aliases=id_aliases
        )
        combined.to_csv(args.out / "RESOLUTION_LEDGER_COMBINED.csv", index=False)
        collisions.to_csv(args.out / "COLLISION_REVIEW.csv", index=False)
        summary["ledger_merge"] = merge_summary
    if args.apply_bio:
        summary["bio_rows_applied"] = apply_bio_additions(args.player_bio, additions, args.out / "applied_player_bio_additions.parquet")
    summary["created_at_utc"] = datetime.now(UTC).isoformat()
    (args.out / "summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    main()
