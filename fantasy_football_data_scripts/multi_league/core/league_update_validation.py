"""Key-level completeness gates for provider snapshots before publication."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

import pandas as pd


class IncompleteSourceError(RuntimeError):
    """A provider response cannot prove the requested snapshot is complete."""


@dataclass(frozen=True, slots=True)
class ProviderSnapshotExpectations:
    provider: str
    league_id: str
    season: int
    weeks: tuple[int, ...]
    expected_team_ids: tuple[str, ...]
    allow_byes: bool = False
    uses_median: bool = False


@dataclass(frozen=True, slots=True)
class ValidationReceipt:
    provider: str
    league_id: str
    season: int
    weeks: tuple[int, ...]
    expected_teams: int
    observed_teams: int
    roster_keys: int
    matchup_keys: int
    player_mappings: int
    valid_empty_resources: tuple[str, ...]
    healthy: bool = True


def _rows(snapshot: Mapping[str, Any], resource: str) -> list[Mapping[str, Any]]:
    value = snapshot.get(resource)
    if not isinstance(value, list) or any(not isinstance(row, Mapping) for row in value):
        raise IncompleteSourceError(f"{resource} response is malformed")
    return list(value)


def _text(value: object) -> str:
    return str(value or "").strip()


def _integer(value: object, *, field: str) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise IncompleteSourceError(f"{field} is missing or invalid") from exc
    return parsed


def _assert_resource_statuses(snapshot: Mapping[str, Any]) -> tuple[str, ...]:
    statuses = snapshot.get("resource_status")
    if not isinstance(statuses, Mapping):
        raise IncompleteSourceError("resource completeness is unknown")
    valid_empty: list[str] = []
    for resource in ("settings", "teams", "rosters", "matchups", "transactions", "draft"):
        status = _text(statuses.get(resource)).lower()
        if status not in {"ok", "ok_empty"}:
            raise IncompleteSourceError(f"{resource} completeness is unknown")
        rows = snapshot.get(resource)
        if rows == []:
            if status != "ok_empty":
                raise IncompleteSourceError(f"{resource} is empty and completeness is unknown")
            if resource not in {"transactions", "draft"}:
                raise IncompleteSourceError(f"{resource} cannot be empty")
            valid_empty.append(resource)
        elif status == "ok_empty":
            raise IncompleteSourceError(f"{resource} is nonempty but marked ok_empty")
    return tuple(sorted(valid_empty))


def _validate_identity(snapshot: Mapping[str, Any], expected: ProviderSnapshotExpectations) -> None:
    if _text(snapshot.get("provider")).lower() != expected.provider.lower():
        raise IncompleteSourceError("provider identity does not match the requested provider")
    if _text(snapshot.get("league_id")) != expected.league_id:
        raise IncompleteSourceError("league identity does not match the requested league")
    if _integer(snapshot.get("season"), field="active season") != expected.season:
        raise IncompleteSourceError("active season does not match the requested season")


def _validate_teams(
    snapshot: Mapping[str, Any],
    expected: ProviderSnapshotExpectations,
) -> tuple[list[Mapping[str, Any]], set[str]]:
    teams = _rows(snapshot, "teams")
    ids = [_text(row.get("team_id")) for row in teams]
    if any(not value for value in ids):
        raise IncompleteSourceError("teams include a blank team identity")
    if len(set(ids)) != len(ids):
        raise IncompleteSourceError("duplicate team identity")
    expected_ids = set(expected.expected_team_ids)
    if set(ids) != expected_ids:
        raise IncompleteSourceError(
            f"teams are incomplete: expected {sorted(expected_ids)}, observed {sorted(set(ids))}"
        )
    franchises = [_text(row.get("franchise_id")) for row in teams]
    if any(not value for value in franchises):
        raise IncompleteSourceError("teams include a missing franchise identity")
    if len(set(franchises)) != len(franchises):
        raise IncompleteSourceError("duplicate franchise identity")
    return teams, expected_ids


def _validate_rosters(
    snapshot: Mapping[str, Any],
    expected: ProviderSnapshotExpectations,
    team_ids: set[str],
) -> tuple[int, int]:
    rosters = _rows(snapshot, "rosters")
    by_key: dict[tuple[int, str], Mapping[str, Any]] = {}
    player_mappings = 0
    for roster in rosters:
        key = (
            _integer(roster.get("week"), field="roster week"),
            _text(roster.get("team_id")),
        )
        if key in by_key:
            raise IncompleteSourceError(f"duplicate roster key: {key}")
        by_key[key] = roster
        players = roster.get("players")
        if not isinstance(players, list) or not players:
            raise IncompleteSourceError(f"roster {key} has no player rows")
        seen_players: set[str] = set()
        for player in players:
            if not isinstance(player, Mapping):
                raise IncompleteSourceError(f"roster {key} has a malformed player row")
            player_id = _text(player.get("player_id"))
            if not player_id:
                raise IncompleteSourceError(f"roster {key} has a missing provider player ID")
            if player_id in seen_players:
                raise IncompleteSourceError(f"roster {key} has duplicate player ID {player_id}")
            seen_players.add(player_id)
            if not _text(player.get("NFL_player_id")):
                raise IncompleteSourceError(f"roster {key} player {player_id} has no NFL mapping")
            player_mappings += 1

    expected_keys = {(week, team_id) for week in expected.weeks for team_id in team_ids}
    observed_keys = set(by_key)
    if observed_keys != expected_keys:
        missing = sorted(expected_keys - observed_keys)
        extra = sorted(observed_keys - expected_keys)
        raise IncompleteSourceError(f"roster coverage mismatch; missing={missing}, extra={extra}")
    return len(observed_keys), player_mappings


def _validate_matchups(
    snapshot: Mapping[str, Any],
    expected: ProviderSnapshotExpectations,
    team_ids: set[str],
) -> int:
    matchups = _rows(snapshot, "matchups")
    by_key: dict[tuple[int, str], Mapping[str, Any]] = {}
    for matchup in matchups:
        key = (
            _integer(matchup.get("week"), field="matchup week"),
            _text(matchup.get("team_id")),
        )
        if key in by_key:
            raise IncompleteSourceError(f"duplicate matchup key: {key}")
        by_key[key] = matchup

    expected_keys = {(week, team_id) for week in expected.weeks for team_id in team_ids}
    if set(by_key) != expected_keys:
        raise IncompleteSourceError(
            f"reciprocal matchup coverage mismatch; missing={sorted(expected_keys - set(by_key))}, "
            f"extra={sorted(set(by_key) - expected_keys)}"
        )

    for (week, team_id), matchup in by_key.items():
        opponent = _text(matchup.get("opponent_team_id"))
        is_bye = bool(matchup.get("is_bye"))
        if is_bye:
            if not expected.allow_byes:
                raise IncompleteSourceError(f"undeclared bye for team {team_id} week {week}")
            if opponent:
                raise IncompleteSourceError(f"bye row for team {team_id} week {week} has an opponent")
            continue
        if not opponent or opponent not in team_ids or opponent == team_id:
            raise IncompleteSourceError(f"invalid opponent for team {team_id} week {week}")
        counterpart = by_key.get((week, opponent))
        if not counterpart or _text(counterpart.get("opponent_team_id")) != team_id:
            raise IncompleteSourceError(f"reciprocal matchup is missing for team {team_id} week {week}")
        matchup_id = _text(matchup.get("matchup_id"))
        other_matchup_id = _text(counterpart.get("matchup_id"))
        if not matchup_id or matchup_id != other_matchup_id:
            raise IncompleteSourceError(f"reciprocal matchup identity differs for team {team_id} week {week}")

    if expected.uses_median:
        median_rows = _rows(snapshot, "median_matchups")
        median_keys: set[tuple[int, str]] = set()
        for row in median_rows:
            key = (_integer(row.get("week"), field="median week"), _text(row.get("team_id")))
            if key in median_keys:
                raise IncompleteSourceError(f"duplicate median matchup key: {key}")
            median_keys.add(key)
        if median_keys != expected_keys:
            raise IncompleteSourceError("median matchup coverage is incomplete")
    return len(by_key)


def validate_provider_snapshot(
    snapshot: Mapping[str, Any],
    expectations: ProviderSnapshotExpectations,
) -> ValidationReceipt:
    """Validate exact provider identities/counts; unknown is never success."""
    if not isinstance(snapshot, Mapping):
        raise IncompleteSourceError("provider snapshot is malformed")
    _validate_identity(snapshot, expectations)
    valid_empty = _assert_resource_statuses(snapshot)
    teams, team_ids = _validate_teams(snapshot, expectations)
    roster_keys, player_mappings = _validate_rosters(snapshot, expectations, team_ids)
    matchup_keys = _validate_matchups(snapshot, expectations, team_ids)
    # Validate nonempty optional resource shapes too; empty safety was proved by
    # resource_status above.
    _rows(snapshot, "transactions")
    _rows(snapshot, "draft")
    return ValidationReceipt(
        provider=expectations.provider,
        league_id=expectations.league_id,
        season=expectations.season,
        weeks=tuple(sorted(set(expectations.weeks))),
        expected_teams=len(expectations.expected_team_ids),
        observed_teams=len(teams),
        roster_keys=roster_keys,
        matchup_keys=matchup_keys,
        player_mappings=player_mappings,
        valid_empty_resources=valid_empty,
    )


def validate_tabular_active_scope(
    *,
    provider: str,
    league_id: str,
    season: int,
    expected_team_ids: tuple[str, ...],
    requested_weeks: tuple[int, ...],
    finalized_weeks: tuple[int, ...],
    player_id_column: str,
    rosters: pd.DataFrame,
    matchups: pd.DataFrame,
    schedule: pd.DataFrame,
    draft: pd.DataFrame,
) -> dict[str, int]:
    """Validate actual weekly fetch frames before partial-week filtering/publication."""
    expected_ids = {str(value).strip() for value in expected_team_ids}
    if not expected_ids or "" in expected_ids or len(expected_ids) != len(expected_team_ids):
        raise IncompleteSourceError(f"{provider} expected team identities are incomplete")
    requested = {int(week) for week in requested_weeks}
    finalized = {int(week) for week in finalized_weeks}
    if not requested or any(week < 1 for week in requested) or not finalized <= requested:
        raise IncompleteSourceError(f"{provider} requested week scope is invalid")

    def coverage(frame: pd.DataFrame, *, name: str, weeks: set[int]) -> int:
        if not isinstance(frame, pd.DataFrame):
            raise IncompleteSourceError(f"{provider} {name} payload is malformed")
        if not weeks:
            if not frame.empty:
                raise IncompleteSourceError(f"{provider} {name} includes an unfinalized week")
            return 0
        required = {"year", "week", "team_key"}
        if not required <= set(frame.columns):
            raise IncompleteSourceError(f"{provider} {name} ownership keys are missing")
        try:
            years = frame["year"].astype(int)
            observed_weeks = frame["week"].astype(int)
        except (TypeError, ValueError) as exc:
            raise IncompleteSourceError(f"{provider} {name} season/week identity is invalid") from exc
        if years.ne(int(season)).any():
            raise IncompleteSourceError(f"{provider} {name} season identity changed")
        team_ids = frame["team_key"].astype(str).str.strip()
        if team_ids.eq("").any() or frame["team_key"].isna().any():
            raise IncompleteSourceError(f"{provider} {name} includes a blank team identity")
        observed = set(zip(observed_weeks.tolist(), team_ids.tolist(), strict=True))
        expected = {(week, team_id) for week in weeks for team_id in expected_ids}
        if observed != expected:
            raise IncompleteSourceError(
                f"{provider} {name} coverage mismatch: missing={sorted(expected - observed)}, "
                f"extra={sorted(observed - expected)}"
            )
        return len(observed)

    roster_keys = coverage(rosters, name="roster", weeks=requested)
    if player_id_column not in rosters.columns:
        raise IncompleteSourceError(f"{provider} roster provider player IDs are missing")
    ids = rosters[player_id_column].astype(str).str.strip()
    if rosters[player_id_column].isna().any() or ids.eq("").any():
        raise IncompleteSourceError(f"{provider} roster has blank provider player IDs")
    duplicate_players = rosters.assign(__provider_player_id=ids).duplicated(
        ["week", "team_key", "__provider_player_id"], keep=False
    )
    if duplicate_players.any():
        raise IncompleteSourceError(f"{provider} roster has duplicate provider player IDs")
    matchup_keys = coverage(matchups, name="matchup", weeks=finalized)
    schedule_for_validation = schedule
    if isinstance(schedule, pd.DataFrame) and not schedule.empty and "team_key" not in schedule:
        # Sleeper's schedule fetcher emits manager/team display fields but not
        # the roster_id. Resolve only through an exact same-week matchup row;
        # never infer team ownership from manager names alone.
        join_keys = ["year", "week", "manager_week", "team_name"]
        if not set(join_keys) <= set(schedule) or not set([*join_keys, "team_key"]) <= set(matchups):
            raise IncompleteSourceError(f"{provider} schedule team identity cannot be resolved")
        mapping = matchups[[*join_keys, "team_key"]].drop_duplicates()
        if mapping.duplicated(join_keys, keep=False).any():
            raise IncompleteSourceError(f"{provider} schedule team identity is ambiguous")
        schedule_for_validation = schedule.merge(
            mapping, on=join_keys, how="left", validate="many_to_one"
        )
        if schedule_for_validation["team_key"].isna().any():
            raise IncompleteSourceError(f"{provider} schedule team identity is missing")
    schedule_keys = coverage(schedule_for_validation, name="schedule", weeks=finalized)

    if not isinstance(draft, pd.DataFrame):
        raise IncompleteSourceError(f"{provider} draft payload is malformed")
    if not draft.empty:
        required = {"year", "draft_id", "round", "pick"}
        if not required <= set(draft.columns):
            raise IncompleteSourceError(f"{provider} draft identity keys are missing")
        if draft[list(required)].isna().any().any():
            raise IncompleteSourceError(f"{provider} draft has null identity keys")
        if draft["year"].astype(int).ne(int(season)).any():
            raise IncompleteSourceError(f"{provider} draft season identity changed")
        if draft.duplicated(["draft_id", "pick"], keep=False).any():
            raise IncompleteSourceError(f"{provider} draft has duplicate picks")
    return {
        "expected_team_weeks": len(expected_ids) * len(requested),
        "observed_team_weeks": roster_keys,
        "observed_final_matchup_weeks": len(finalized),
        "matchup_team_weeks": matchup_keys,
        "schedule_team_weeks": schedule_keys,
        "draft_picks": len(draft),
    }
