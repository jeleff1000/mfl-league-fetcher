"""Key-level completeness gates for provider snapshots before publication."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from math import isfinite
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


def validate_provider_team_inventory(
    *, provider: str, settings_team_count: object, team_ids: tuple[str, ...]
) -> tuple[str, ...]:
    """Cross-check the provider team endpoint with independent league size."""
    identities = tuple(str(team_id).strip() for team_id in team_ids)
    if not identities or any(not team_id for team_id in identities) or len(set(identities)) != len(identities):
        raise IncompleteSourceError(f"{provider} team identities are incomplete")
    try:
        expected_count = int(settings_team_count)
    except (TypeError, ValueError) as exc:
        raise IncompleteSourceError(f"{provider} active league team count is unavailable") from exc
    if expected_count < 1 or len(identities) != expected_count:
        raise IncompleteSourceError(
            f"{provider} active league team count mismatch: settings={expected_count}, "
            f"fetched={len(identities)}"
        )
    return identities


def validate_active_roster_frame(
    *,
    provider: str,
    season: int,
    expected_team_ids: tuple[str, ...],
    requested_weeks: tuple[int, ...],
    player_id_column: str,
    rosters: pd.DataFrame,
) -> int:
    """Check raw provider team/player coverage before NFL finality filtering."""
    expected_ids = {str(value).strip() for value in expected_team_ids}
    if not expected_ids or "" in expected_ids or len(expected_ids) != len(expected_team_ids):
        raise IncompleteSourceError(f"{provider} expected team identities are incomplete")
    requested = {int(week) for week in requested_weeks}
    if not requested or any(week < 1 for week in requested):
        raise IncompleteSourceError(f"{provider} requested week scope is invalid")
    required = {"year", "week", "team_key", player_id_column}
    if not isinstance(rosters, pd.DataFrame) or not required <= set(rosters.columns):
        raise IncompleteSourceError(f"{provider} roster ownership/player keys are missing")
    if rosters[list(required)].isna().any().any():
        raise IncompleteSourceError(f"{provider} roster includes null ownership/player keys")
    try:
        years = rosters["year"].astype(int)
        observed_weeks = rosters["week"].astype(int)
    except (TypeError, ValueError) as exc:
        raise IncompleteSourceError(f"{provider} roster season/week identity is invalid") from exc
    if years.ne(int(season)).any():
        raise IncompleteSourceError(f"{provider} roster season identity changed")
    team_ids = rosters["team_key"].astype(str).str.strip()
    player_ids = rosters[player_id_column].astype(str).str.strip()
    if team_ids.eq("").any() or player_ids.eq("").any():
        raise IncompleteSourceError(f"{provider} roster has blank team/provider player IDs")
    observed = set(zip(observed_weeks.tolist(), team_ids.tolist(), strict=True))
    expected = {(week, team_id) for week in requested for team_id in expected_ids}
    if observed != expected:
        raise IncompleteSourceError(
            f"{provider} roster coverage mismatch: missing={sorted(expected - observed)}, "
            f"extra={sorted(observed - expected)}"
        )
    duplicate_players = rosters.assign(__team_id=team_ids, __player_id=player_ids).duplicated(
        ["week", "__team_id", "__player_id"], keep=False
    )
    if duplicate_players.any():
        raise IncompleteSourceError(f"{provider} roster has duplicate provider player IDs")
    return len(observed)


_ACTIVE_PROVIDER_PLAYER_COLUMNS = frozenset({
    "yahoo_player_id", "espn_player_id", "sleeper_player_id",
})


def _active_player_rows(
    conn: Any, *, db_name: str, year: int, weeks: tuple[int, ...],
    provider_id_column: str, require_mappings: bool,
) -> list[tuple[Any, ...]]:
    """Read only affected league-week provider keys, never the NFL lake."""
    if provider_id_column not in _ACTIVE_PROVIDER_PLAYER_COLUMNS:
        raise IncompleteSourceError("unsupported provider player identity column")
    selected_weeks = tuple(sorted({int(week) for week in weeks}))
    if not selected_weeks or any(week < 1 for week in selected_weeks):
        raise IncompleteSourceError("active player week scope is invalid")
    columns = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'player_fantasy'"
        ).fetchall()
    }
    required = {"db_name", "year", "week", provider_id_column}
    if require_mappings:
        required |= {"NFL_player_id", "fantasy_points"}
    if not required <= columns:
        missing = sorted(required - columns)
        raise IncompleteSourceError(
            "active player provider player identity column or mapping inputs are missing: "
            + ", ".join(missing)
        )
    week_placeholders = ", ".join("?" for _ in selected_weeks)
    identity = '"' + provider_id_column + '"'
    select = f"week, CAST({identity} AS VARCHAR)"
    if require_mappings:
        select += ", CAST(NFL_player_id AS VARCHAR), fantasy_points"
    return conn.execute(
        f"SELECT {select} FROM public.player_fantasy "
        f"WHERE db_name = ? AND year = ? AND week IN ({week_placeholders}) "
        f"AND {identity} IS NOT NULL",
        [str(db_name), int(year), *selected_weeks],
    ).fetchall()


def _provider_player_key(week: Any, player_id: Any) -> tuple[int, str]:
    raw = str(player_id).strip()
    if raw.endswith(".0") and raw[:-2].lstrip("-").isdigit():
        raw = raw[:-2]
    if not raw:
        raise IncompleteSourceError("active provider player ID is blank")
    return int(week), raw


def capture_active_provider_player_scope(
    conn: Any, *, db_name: str, year: int, weeks: tuple[int, ...],
    provider_id_column: str,
) -> set[tuple[int, str]]:
    """Bind merged provider player ownership before shared transformations."""
    rows = _active_player_rows(
        conn, db_name=db_name, year=year, weeks=weeks,
        provider_id_column=provider_id_column, require_mappings=False,
    )
    keys = {_provider_player_key(week, player_id) for week, player_id in rows}
    if not keys:
        raise IncompleteSourceError("active provider player scope has no admitted rows")
    return keys


def assert_transformed_active_player_scope(
    conn: Any, *, db_name: str, year: int, weeks: tuple[int, ...],
    provider_id_column: str, expected_keys: set[tuple[int, str]],
) -> dict[str, int]:
    """Fail before staging if shared transforms lose provider IDs or scored mappings."""
    rows = _active_player_rows(
        conn, db_name=db_name, year=year, weeks=weeks,
        provider_id_column=provider_id_column, require_mappings=True,
    )
    observed = {_provider_player_key(week, player_id) for week, player_id, _, _ in rows}
    missing = expected_keys - observed
    if missing:
        raise IncompleteSourceError(
            f"provider player IDs disappeared after transformations: "
            f"{len(expected_keys) - len(missing)}/{len(expected_keys)} preserved"
        )
    if observed != expected_keys:
        raise IncompleteSourceError("transformed provider player inventory changed")
    mapped_scored = 0
    unmapped_scored = 0
    for week, player_id, nfl_id, points in rows:
        score = pd.to_numeric(pd.Series([points]), errors="coerce").iloc[0]
        if pd.isna(score) or not isfinite(float(score)):
            raise IncompleteSourceError(
                "transformed provider player score is malformed: "
                f"year={int(year)} week={int(week)} {provider_id_column}={player_id}"
            )
        if score == 0:
            continue
        identity = str(nfl_id or "").strip()
        if not identity or identity.upper().startswith(("ESPN-", "YAHOO-", "SLEEPER-")):
            unmapped_scored += 1
        else:
            mapped_scored += 1
    if unmapped_scored:
        raise IncompleteSourceError(
            f"{unmapped_scored} scored provider players lack NFL mappings after transformations"
        )
    return {"provider_player_keys": len(observed), "mapped_scored_players": mapped_scored}


def _active_matchup_scores(
    conn: Any, *, db_name: str, year: int, weeks: tuple[int, ...],
) -> dict[tuple[int, str], tuple[float | None, float | None]]:
    selected_weeks = tuple(sorted({int(week) for week in weeks}))
    if not selected_weeks or any(week < 1 for week in selected_weeks):
        raise IncompleteSourceError("active matchup week scope is invalid")
    columns = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'matchup'"
        ).fetchall()
    }
    if not columns:
        return {}
    required = {"db_name", "year", "week", "team_key", "team_points", "opponent_points"}
    if not required <= columns:
        raise IncompleteSourceError(
            "active matchup identity or score columns are missing: "
            + ", ".join(sorted(required - columns))
        )
    placeholders = ", ".join("?" for _ in selected_weeks)
    rows = conn.execute(
        "SELECT week, CAST(team_key AS VARCHAR), team_points, opponent_points "
        "FROM public.matchup WHERE db_name = ? AND year = ? "
        f"AND week IN ({placeholders}) "
        "AND (team_points IS NOT NULL OR opponent_points IS NOT NULL)",
        [str(db_name), int(year), *selected_weeks],
    ).fetchall()
    result: dict[tuple[int, str], tuple[float | None, float | None]] = {}
    for week, team_key, team_points, opponent_points in rows:
        team = str(team_key or "").strip()
        if not team:
            raise IncompleteSourceError("scored matchup has no provider team identity")
        key = (int(week), team)
        if key in result:
            raise IncompleteSourceError(f"duplicate scored matchup team-week: {key}")

        def score(value: Any) -> float | None:
            if value is None:
                return None
            try:
                number = float(value)
            except (TypeError, ValueError) as exc:
                raise IncompleteSourceError("scored matchup has malformed points") from exc
            if not isfinite(number):
                raise IncompleteSourceError("scored matchup has nonfinite points")
            return number

        result[key] = (score(team_points), score(opponent_points))
    return result


def capture_active_final_matchup_scope(
    conn: Any, *, db_name: str, year: int, weeks: tuple[int, ...],
) -> dict[tuple[int, str], tuple[float | None, float | None]]:
    """Pin exact provider score rows before the shared graph transforms."""
    return _active_matchup_scores(conn, db_name=db_name, year=year, weeks=weeks)


def assert_transformed_active_matchup_scope(
    conn: Any, *, db_name: str, year: int, weeks: tuple[int, ...],
    expected_scores: dict[tuple[int, str], tuple[float | None, float | None]],
) -> dict[str, int]:
    """A late provider correction cannot be dropped by enrichment/staging."""
    observed = _active_matchup_scores(conn, db_name=db_name, year=year, weeks=weeks)
    missing = expected_scores.keys() - observed.keys()
    if missing:
        raise IncompleteSourceError(
            f"scored team-weeks disappeared after transformations: {len(missing)}"
        )
    if observed.keys() != expected_scores.keys():
        raise IncompleteSourceError("transformed scored team-week inventory changed")
    for key, expected_pair in expected_scores.items():
        actual_pair = observed[key]
        if any(
            (old is None) != (new is None)
            or (old is not None and new is not None and abs(old - new) > 1e-6)
            for old, new in zip(expected_pair, actual_pair, strict=True)
        ):
            raise IncompleteSourceError(f"provider score changed after transformations: {key}")
    return {"scored_team_weeks": len(observed)}


def _derived_id_rows(
    conn: Any, *, db_name: str, table_name: str, identity_column: str,
    required_columns: frozenset[str],
) -> dict[str, dict[str, Any]]:
    available = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = ?", [table_name],
        ).fetchall()
    }
    if not required_columns <= available:
        raise IncompleteSourceError(
            f"{table_name} derived output columns are missing: "
            + ", ".join(sorted(required_columns - available))
        )
    select_columns = sorted(required_columns - {"db_name"})
    rows = conn.execute(
        f"SELECT {', '.join(select_columns)} FROM public.{table_name} WHERE db_name = ?",
        [str(db_name)],
    ).fetchall()
    identity_index = select_columns.index(identity_column)
    keyed: dict[str, dict[str, Any]] = {}
    for row in rows:
        identity = str(row[identity_index] or "").strip()
        if not identity or identity in keyed:
            raise IncompleteSourceError(f"{table_name} has blank/duplicate derived identity")
        keyed[identity] = dict(zip(select_columns, row, strict=True))
    return keyed


def assert_active_season_simulation_health(
    conn: Any, *, db_name: str, year: int,
) -> dict[str, int | None]:
    """Require the latest finalized regular week and season rollup to carry fresh sims."""
    matchup_columns = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'matchup'"
        ).fetchall()
    }
    if not matchup_columns:
        return {
            "latest_finalized_week": None,
            "latest_simulation_franchises": 0,
            "simulation_season_franchises": 0,
        }
    required = {
        "db_name", "year", "week", "franchise_id", "team_points",
        "opponent_points", "p_playoffs", "p_champ",
    }
    if not required <= matchup_columns:
        raise IncompleteSourceError(
            "matchup simulation columns are missing: "
            + ", ".join(sorted(required - matchup_columns))
        )
    regular_filters = []
    if "is_bye_week" in matchup_columns:
        regular_filters.append("COALESCE(is_bye_week, FALSE) = FALSE")
    if "is_playoffs" in matchup_columns:
        regular_filters.append("COALESCE(is_playoffs, FALSE) = FALSE")
    if "is_consolation" in matchup_columns:
        regular_filters.append("COALESCE(is_consolation, FALSE) = FALSE")
    regular_sql = "" if not regular_filters else " AND " + " AND ".join(regular_filters)
    latest_row = conn.execute(
        "SELECT MAX(week) FROM public.matchup "
        "WHERE db_name = ? AND year = ? "
        "AND team_points IS NOT NULL AND opponent_points IS NOT NULL"
        + regular_sql,
        [str(db_name), int(year)],
    ).fetchone()
    latest_week = latest_row[0] if latest_row else None

    settings_columns = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'league_settings'"
        ).fetchall()
    }
    if not {"db_name", "year", "num_teams", "playoff_start_week"} <= settings_columns:
        raise IncompleteSourceError("league settings cannot prove the active regular-season schedule")
    settings_row = conn.execute(
        "SELECT CAST(num_teams AS INTEGER), CAST(playoff_start_week AS INTEGER) "
        "FROM public.league_settings WHERE db_name = ? AND year = ? LIMIT 1",
        [str(db_name), int(year)],
    ).fetchone()
    if (
        not settings_row or settings_row[0] is None or settings_row[1] is None
        or int(settings_row[0]) < 1 or int(settings_row[1]) < 2
    ):
        raise IncompleteSourceError("league settings have no valid active-season schedule boundary")
    expected_teams = int(settings_row[0])
    regular_season_end = int(settings_row[1]) - 1

    schedule_columns = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'schedule'"
        ).fetchall()
    }
    required_schedule = {
        "db_name", "year", "week", "franchise_id", "opponent_franchise_id",
    }
    if not required_schedule <= schedule_columns:
        raise IncompleteSourceError(
            "schedule cannot prove future simulation inputs: "
            + ", ".join(sorted(required_schedule - schedule_columns))
        )
    schedule_filter = ""
    if "is_playoffs" in schedule_columns:
        schedule_filter = " AND COALESCE(is_playoffs, FALSE) = FALSE"
    schedule_rows = conn.execute(
        "SELECT CAST(week AS INTEGER), COUNT(*), "
        "COUNT(DISTINCT NULLIF(TRIM(CAST(franchise_id AS VARCHAR)), '')), "
        "COUNT(NULLIF(TRIM(CAST(opponent_franchise_id AS VARCHAR)), '')) "
        "FROM public.schedule WHERE db_name = ? AND year = ? "
        "AND week BETWEEN 1 AND ?" + schedule_filter + " GROUP BY week ORDER BY week",
        [str(db_name), int(year), regular_season_end],
    ).fetchall()
    complete_weeks = {
        int(week) for week, row_count, franchise_count, opponent_count in schedule_rows
        if int(row_count or 0) == expected_teams
        and int(franchise_count or 0) == expected_teams
        and int(opponent_count or 0) == expected_teams
    }
    missing_schedule_weeks = sorted(set(range(1, regular_season_end + 1)) - complete_weeks)
    if missing_schedule_weeks:
        raise IncompleteSourceError(
            f"schedule must cover every team through regular-season week {regular_season_end}; "
            f"missing or incomplete weeks={missing_schedule_weeks}"
        )
    schedule_franchises = {
        str(row[0]).strip() for row in conn.execute(
            "SELECT DISTINCT CAST(franchise_id AS VARCHAR) FROM public.schedule "
            "WHERE db_name = ? AND year = ? AND week BETWEEN 1 AND ? "
            "AND NULLIF(TRIM(CAST(franchise_id AS VARCHAR)), '') IS NOT NULL"
            + schedule_filter,
            [str(db_name), int(year), regular_season_end],
        ).fetchall()
    }
    if len(schedule_franchises) != expected_teams:
        raise IncompleteSourceError(
            "schedule active franchise coverage differs from league settings "
            f"(observed={len(schedule_franchises)}, expected={expected_teams})"
        )
    if latest_week is None:
        return {
            "latest_finalized_week": None,
            "latest_simulation_franchises": 0,
            "simulation_season_franchises": 0,
        }

    latest_rows = conn.execute(
        "SELECT CAST(franchise_id AS VARCHAR), COUNT(*), COUNT(p_playoffs), COUNT(p_champ), "
        "MIN(CAST(p_playoffs AS DOUBLE)), MAX(CAST(p_playoffs AS DOUBLE)), "
        "MIN(CAST(p_champ AS DOUBLE)), MAX(CAST(p_champ AS DOUBLE)) "
        "FROM public.matchup WHERE db_name = ? AND year = ? AND week = ? "
        "AND team_points IS NOT NULL AND opponent_points IS NOT NULL"
        + regular_sql
        + " GROUP BY franchise_id",
        [str(db_name), int(year), int(latest_week)],
    ).fetchall()
    latest: dict[str, tuple[float, float]] = {}
    for franchise_id, row_count, playoff_count, champ_count, p_min, p_max, c_min, c_max in latest_rows:
        key = str(franchise_id or "").strip()
        valid = (
            bool(key)
            and int(row_count or 0) > 0
            and int(playoff_count or 0) == int(row_count or 0)
            and int(champ_count or 0) == int(row_count or 0)
            and all(value is not None and isfinite(float(value)) for value in (p_min, p_max, c_min, c_max))
            and 0 <= float(p_min) <= float(p_max) <= 100
            and 0 <= float(c_min) <= float(c_max) <= 100
            and abs(float(p_max) - float(p_min)) <= 0.011
            and abs(float(c_max) - float(c_min)) <= 0.011
        )
        if not valid:
            raise IncompleteSourceError(
                f"latest finalized week {int(latest_week)} lacks complete simulation output"
            )
        latest[key] = (float(p_max), float(c_max))
    if not latest:
        raise IncompleteSourceError(
            f"latest finalized week {int(latest_week)} lacks simulation franchises"
        )
    if len(latest) != expected_teams or set(latest) != schedule_franchises:
        raise IncompleteSourceError(
            "latest simulation franchise coverage differs from the complete schedule "
            f"(observed={len(latest)}, expected={expected_teams})"
        )

    season_columns = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'matchup_season'"
        ).fetchall()
    }
    required_season = {"db_name", "year", "franchise_id", "p_playoffs", "p_champ"}
    if not required_season <= season_columns:
        raise IncompleteSourceError(
            "matchup_season latest simulation columns are missing: "
            + ", ".join(sorted(required_season - season_columns))
        )
    season_rows = conn.execute(
        "SELECT CAST(franchise_id AS VARCHAR), CAST(p_playoffs AS DOUBLE), CAST(p_champ AS DOUBLE) "
        "FROM public.matchup_season WHERE db_name = ? AND year = ?",
        [str(db_name), int(year)],
    ).fetchall()
    season: dict[str, tuple[float, float]] = {}
    for franchise_id, p_playoffs, p_champ in season_rows:
        key = str(franchise_id or "").strip()
        if (
            not key or key in season or p_playoffs is None or p_champ is None
            or not isfinite(float(p_playoffs)) or not isfinite(float(p_champ))
            or not 0 <= float(p_playoffs) <= 100 or not 0 <= float(p_champ) <= 100
        ):
            raise IncompleteSourceError("matchup_season has invalid latest simulation output")
        season[key] = (float(p_playoffs), float(p_champ))
    missing = set(latest) - set(season)
    extra = set(season) - set(latest)
    stale = {
        key for key in latest.keys() & season.keys()
        if abs(latest[key][0] - season[key][0]) > 0.011
        or abs(latest[key][1] - season[key][1]) > 0.011
    }
    if missing or extra or stale:
        raise IncompleteSourceError(
            "matchup_season does not carry the latest simulation output "
            f"(missing={len(missing)}, extra={len(extra)}, stale={len(stale)})"
        )
    return {
        "latest_finalized_week": int(latest_week),
        "latest_simulation_franchises": len(latest),
        "simulation_season_franchises": len(season),
    }


def assert_refresh_derived_output_health(
    conn: Any, *, db_name: str, year: int, weeks: tuple[int, ...],
    provider_id_column: str, published_tables: tuple[str, ...] | list[str],
    server_rebuilds_career_rollups: bool = False,
    server_rebuilds_homepage_rollups: bool = False,
) -> dict[str, int | str | None]:
    """Check derived coverage and ownership under the actual publication contract.

    The server can rebuild careers on the full Fly connection inside the merge transaction;
    active-season scratch careers are checked here but must not be uploaded.
    Server-owned homepage generation and coverage validation also run in that atomic
    transaction; no historical homepage inputs are hydrated into worker scratch.
    """
    from multi_league.transformations.aggregation.aggregation_utils import (
        CAREER_ROLLUP_TABLES, HOMEPAGE_ROLLUP_TABLES,
    )

    if server_rebuilds_homepage_rollups and not server_rebuilds_career_rollups:
        raise IncompleteSourceError("server homepage rebuilding requires server career rebuilding")
    atomic_homepage = server_rebuilds_homepage_rollups
    if provider_id_column not in _ACTIVE_PROVIDER_PLAYER_COLUMNS:
        raise IncompleteSourceError("unsupported provider player identity column")
    selected_weeks = tuple(sorted({int(week) for week in weeks}))
    if not selected_weeks:
        raise IncompleteSourceError("derived output week scope is invalid")
    placeholders = ", ".join("?" for _ in selected_weeks)
    identity = '"' + provider_id_column + '"'
    active_players = {
        str(row[0]).strip() for row in conn.execute(
            "SELECT DISTINCT CAST(NFL_player_id AS VARCHAR) FROM public.player_fantasy "
            f"WHERE db_name = ? AND year = ? AND week IN ({placeholders}) "
            f"AND {identity} IS NOT NULL AND fantasy_points IS NOT NULL "
            "AND fantasy_points <> 0",
            [str(db_name), int(year), *selected_weeks],
        ).fetchall()
    }
    if "" in active_players or "None" in active_players:
        raise IncompleteSourceError("scored provider player lacks canonical NFL identity")
    matchup_columns = {
        str(row[0]) for row in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'public' AND table_name = 'matchup'"
        ).fetchall()
    }
    active_franchises: set[str] = set()
    if matchup_columns:
        required_matchup = {
            "db_name", "year", "week", "franchise_id", "team_points", "opponent_points",
        }
        if not required_matchup <= matchup_columns:
            raise IncompleteSourceError("scored matchup franchise derivation inputs are missing")
        active_franchises = {
            str(row[0] or "").strip() for row in conn.execute(
                "SELECT DISTINCT CAST(franchise_id AS VARCHAR) FROM public.matchup "
                f"WHERE db_name = ? AND year = ? AND week IN ({placeholders}) "
                "AND team_points IS NOT NULL AND opponent_points IS NOT NULL",
                [str(db_name), int(year), *selected_weeks],
            ).fetchall()
        }
    if "" in active_franchises:
        raise IncompleteSourceError("scored matchup has no stable franchise identity")
    simulation_health = assert_active_season_simulation_health(
        conn, db_name=db_name, year=year,
    )
    required_publish = {"homepage_league_summary"}
    if active_players:
        required_publish |= {"player_fantasy_career", "player_fantasy_career_all"}
    if active_franchises:
        required_publish |= {
            "matchup", "matchup_season", "matchup_career",
            "homepage_manager_rankings", "homepage_current_standings",
        }
    if server_rebuilds_career_rollups:
        if set(published_tables) & set(CAREER_ROLLUP_TABLES):
            raise IncompleteSourceError("server-owned careers must not be uploaded from scratch")
        required_publish -= set(CAREER_ROLLUP_TABLES)
    if atomic_homepage:
        if set(published_tables) & set(HOMEPAGE_ROLLUP_TABLES):
            raise IncompleteSourceError("server-owned homepages must not be uploaded from scratch")
        required_publish -= set(HOMEPAGE_ROLLUP_TABLES)
        if active_players:
            required_publish.add("player_fantasy")
        if active_franchises:
            required_publish.add("matchup")
    missing_publish = required_publish - set(published_tables)
    if missing_publish:
        raise IncompleteSourceError(
            "derived output not in the publication bundle: " + ", ".join(sorted(missing_publish))
        )
    summary = None
    if not atomic_homepage:
        summary = conn.execute(
            "SELECT COUNT(*) FROM public.homepage_league_summary WHERE db_name = ?",
            [str(db_name)],
        ).fetchone()[0]
        if int(summary or 0) != 1:
            raise IncompleteSourceError("homepage_league_summary must contain exactly one league row")
    if not server_rebuilds_career_rollups:
        for table in ("player_fantasy_career", "player_fantasy_career_all"):
            if not active_players:
                continue
            rows = _derived_id_rows(
                conn, db_name=db_name, table_name=table, identity_column="NFL_player_id",
                required_columns=frozenset({"db_name", "NFL_player_id", "games_rostered", "fantasy_points"}),
            )
            missing = active_players - rows.keys()
            if missing:
                raise IncompleteSourceError(
                    f"{table} lacks {len(missing)} scored active provider player(s)"
                )
            for player_id in active_players:
                row = rows[player_id]
                try:
                    games = int(row["games_rostered"])
                except (TypeError, ValueError) as exc:
                    raise IncompleteSourceError(f"{table} has invalid career values for {player_id}") from exc
                if row["fantasy_points"] is None or games < 1:
                    raise IncompleteSourceError(f"{table} has invalid career values for {player_id}")
    for table, metric, positive in (
        ("matchup_career", "games", True),
        ("homepage_manager_rankings", "seasons", True),
        ("homepage_current_standings", "wins", False),
    ):
        if (server_rebuilds_career_rollups and table in CAREER_ROLLUP_TABLES) or (
            atomic_homepage and table in HOMEPAGE_ROLLUP_TABLES
        ):
            continue  # Server-owned complete-chain rollups are checked before COMMIT.
        if not active_franchises:
            continue
        rows = _derived_id_rows(
            conn, db_name=db_name, table_name=table, identity_column="franchise_id",
            required_columns=frozenset({"db_name", "franchise_id", metric}),
        )
        missing = active_franchises - rows.keys()
        if missing:
            raise IncompleteSourceError(f"{table} lacks {len(missing)} active franchise(s)")
        for franchise_id in active_franchises:
            try:
                value = int(rows[franchise_id][metric])
            except (TypeError, ValueError) as exc:
                raise IncompleteSourceError(
                    f"{table} has invalid derived {metric} for {franchise_id}"
                ) from exc
            if value < (1 if positive else 0):
                raise IncompleteSourceError(
                    f"{table} has invalid derived {metric} for {franchise_id}"
                )
    return {
        "active_scored_career_players": len(active_players),
        "active_scored_career_franchises": len(active_franchises),
        "homepage_summary_rows": int(summary) if summary is not None else None,
        "homepage_validation_location": "atomic_fly" if atomic_homepage else "worker",
        **simulation_health,
    }


def validate_yahoo_scoreboard_pair_graph(
    frame: pd.DataFrame,
    *,
    season: int,
    week: int,
    expected_team_keys: tuple[str, ...],
    require_full_teams: bool,
) -> int:
    """Prove each Yahoo team row against its reciprocal fetched opponent row."""
    expected = {str(value).strip() for value in expected_team_keys}
    if not expected or "" in expected or len(expected) != len(expected_team_keys):
        raise IncompleteSourceError("Yahoo expected team identities are incomplete")
    required = {
        "year", "week", "team_key", "opponent_team_key", "team_points", "opponent_points",
    }
    if not isinstance(frame, pd.DataFrame) or not required <= set(frame.columns):
        raise IncompleteSourceError("Yahoo scoreboard pair identities or scores are missing")
    if frame.empty and not require_full_teams:
        return 0
    if frame[list(required)].isna().any().any():
        raise IncompleteSourceError("Yahoo scoreboard includes null pair identities or scores")
    try:
        years = frame["year"].astype(int)
        weeks = frame["week"].astype(int)
        scores = frame[["team_points", "opponent_points"]].apply(pd.to_numeric, errors="coerce")
    except (TypeError, ValueError) as exc:
        raise IncompleteSourceError("Yahoo scoreboard season/week/scores are invalid") from exc
    if years.ne(int(season)).any() or weeks.ne(int(week)).any() or scores.isna().any().any():
        raise IncompleteSourceError("Yahoo scoreboard returned the wrong week or invalid scores")
    rows = frame.assign(
        __team=frame["team_key"].astype(str).str.strip(),
        __opponent=frame["opponent_team_key"].astype(str).str.strip(),
    )
    if rows["__team"].duplicated().any() or rows["__team"].eq("").any():
        raise IncompleteSourceError("Yahoo scoreboard has duplicate or blank teams")
    observed = set(rows["__team"])
    if not observed <= expected or (require_full_teams and observed != expected):
        raise IncompleteSourceError("Yahoo scoreboard team coverage is incomplete or changed")
    indexed = rows.set_index("__team", verify_integrity=True)
    for team_id, row in indexed.iterrows():
        opponent_id = row["__opponent"]
        if opponent_id == team_id or opponent_id not in indexed.index:
            raise IncompleteSourceError("Yahoo scoreboard reciprocal opponent is missing")
        opponent = indexed.loc[opponent_id]
        if opponent["__opponent"] != team_id or abs(float(row["team_points"]) - float(opponent["opponent_points"])) > 1e-6 or abs(float(row["opponent_points"]) - float(opponent["team_points"])) > 1e-6:
            raise IncompleteSourceError("Yahoo scoreboard reciprocal pair scores or IDs disagree")
    return len(indexed)


def validate_yahoo_week_matchup_scope(
    *,
    raw_schedule: pd.DataFrame,
    final_matchups: pd.DataFrame,
    season: int,
    week: int,
    expected_team_keys: tuple[str, ...],
    playoff_start_week: int | None,
) -> bool:
    """Require regular-season inventory or the Yahoo-declared playoff pair graph.

    Yahoo's postseason scoreboard omits teams not scheduled to play, including
    bracket byes. The XML fetcher verifies the provider's declared matchup
    count before this tabular pair check. A final week is complete only when
    every declared pair has a final result.
    """
    try:
        playoff_start = int(playoff_start_week) if playoff_start_week is not None else None
    except (TypeError, ValueError) as exc:
        raise IncompleteSourceError("Yahoo playoff start week is invalid") from exc
    if playoff_start is not None and playoff_start < 1:
        raise IncompleteSourceError("Yahoo playoff start week is invalid")
    is_postseason = playoff_start is not None and int(week) >= playoff_start
    raw_count = validate_yahoo_scoreboard_pair_graph(
        raw_schedule, season=season, week=week,
        expected_team_keys=expected_team_keys, require_full_teams=not is_postseason,
    )
    if raw_count == 0:
        raise IncompleteSourceError("Yahoo scoreboard has no declared matchup pairs")
    final_count = validate_yahoo_scoreboard_pair_graph(
        final_matchups, season=season, week=week,
        expected_team_keys=expected_team_keys, require_full_teams=False,
    )
    if final_count:
        source = raw_schedule.set_index(raw_schedule["team_key"].astype(str).str.strip())
        for _, row in final_matchups.iterrows():
            team_key = str(row["team_key"]).strip()
            if team_key not in source.index:
                raise IncompleteSourceError("Yahoo final matchup is absent from raw scoreboard")
            raw = source.loc[team_key]
            if (
                str(row["opponent_team_key"]).strip() != str(raw["opponent_team_key"]).strip()
                or abs(float(row["team_points"]) - float(raw["team_points"])) > 1e-6
                or abs(float(row["opponent_points"]) - float(raw["opponent_points"])) > 1e-6
            ):
                raise IncompleteSourceError("Yahoo final matchup changed raw pair or score")
    return final_count == raw_count


def validate_espn_final_matchup_frame(
    *,
    season: int,
    week: int,
    expected_team_ids: tuple[str, ...],
    raw_schedule: list[dict[str, Any]],
    matchups: pd.DataFrame,
) -> int:
    """Compare ESPN's fetched final rows with its already-read raw pair graph."""
    from multi_league.core.league_refresh import espn_schedule_is_final

    expected = {str(team_id).strip() for team_id in expected_team_ids}
    if not espn_schedule_is_final(raw_schedule, expected_team_ids=expected_team_ids):
        raise IncompleteSourceError("ESPN raw final matchup graph is incomplete")
    required = {"year", "week", "team_key", "matchup_id", "is_bye_week", "team_points", "opponent_points"}
    if not isinstance(matchups, pd.DataFrame) or not required <= set(matchups):
        columns = sorted(str(column) for column in getattr(matchups, "columns", []))
        row_count = len(matchups) if isinstance(matchups, pd.DataFrame) else None
        raise IncompleteSourceError(
            "ESPN fetched final matchup identity/score keys are missing "
            f"(rows={row_count}, columns={columns})"
        )
    if matchups[["year", "week", "team_key", "matchup_id", "is_bye_week"]].isna().any().any():
        raise IncompleteSourceError("ESPN fetched final matchup has null identity keys")
    try:
        years = matchups["year"].astype(int)
        weeks = matchups["week"].astype(int)
    except (TypeError, ValueError) as exc:
        raise IncompleteSourceError("ESPN fetched final matchup season/week is invalid") from exc
    if years.ne(int(season)).any() or weeks.ne(int(week)).any():
        raise IncompleteSourceError("ESPN fetched final matchup season/week changed")
    team_ids = matchups["team_key"].astype(str).str.strip()
    if team_ids.eq("").any() or team_ids.duplicated().any() or set(team_ids) != expected:
        raise IncompleteSourceError(
            f"ESPN final matchup coverage mismatch: missing={sorted(expected - set(team_ids))}, "
            f"extra={sorted(set(team_ids) - expected)}"
        )
    rows = matchups.assign(__team_id=team_ids).set_index("__team_id", verify_integrity=True)
    bye_flags = rows["is_bye_week"].astype(str).str.strip().str.lower().isin({"true", "1"})
    for raw in raw_schedule:
        home_id = str((raw.get("home") or {}).get("teamId") or "").strip()
        away = raw.get("away")
        away_id = str((away or {}).get("teamId") or "").strip() if isinstance(away, dict) else ""
        if not away_id:
            if not bool(bye_flags.loc[home_id]):
                raise IncompleteSourceError("ESPN declared bye is missing from final matchup frame")
            continue
        home = rows.loc[home_id]
        visitor = rows.loc[away_id]
        if bool(bye_flags.loc[home_id]) or bool(bye_flags.loc[away_id]):
            raise IncompleteSourceError("ESPN scored pair was mislabeled as a bye")
        if home["matchup_id"] != visitor["matchup_id"]:
            raise IncompleteSourceError("ESPN fetched final matchup pair identity changed")
        pair_id = home["matchup_id"]
        pair_teams = set(rows.loc[rows["matchup_id"] == pair_id].index)
        if pair_teams != {home_id, away_id}:
            raise IncompleteSourceError("ESPN fetched final matchup pair coverage changed")
        if pd.to_numeric(pd.Series([home["team_points"], visitor["team_points"]]), errors="coerce").isna().any():
            raise IncompleteSourceError("ESPN fetched final matchup score is missing")
        for current, other in ((home, visitor), (visitor, home)):
            try:
                opponent_score = float(current["opponent_points"])
                other_score = float(other["team_points"])
            except (TypeError, ValueError) as exc:
                raise IncompleteSourceError("ESPN fetched final opponent score is missing") from exc
            if not isfinite(opponent_score) or not isfinite(other_score):
                raise IncompleteSourceError("ESPN fetched final opponent score is missing")
            if abs(opponent_score - other_score) > 1e-6:
                raise IncompleteSourceError("ESPN fetched final opponent score is not reciprocal")
        for side, fetched in ((raw.get("home") or {}, home), (away, visitor)):
            try:
                points_by_period = side.get("pointsByScoringPeriod")
                raw_value = None
                if isinstance(points_by_period, dict):
                    raw_value = points_by_period.get(str(week))
                    if raw_value is None:
                        raw_value = points_by_period.get(int(week))
                if raw_value is None:
                    raw_value = side["totalPoints"]
                raw_score = float(raw_value)
                fetched_score = float(fetched["team_points"])
            except (KeyError, TypeError, ValueError) as exc:
                raise IncompleteSourceError("ESPN raw final matchup score witness is missing") from exc
            if not isfinite(raw_score) or not isfinite(fetched_score):
                raise IncompleteSourceError("ESPN raw final matchup score witness is missing")
            if abs(round(raw_score, 2) - fetched_score) > 1e-6:
                raise IncompleteSourceError("ESPN fetched final matchup changed raw score")
    return len(matchups)


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
        if len(observed) != len(frame):
            raise IncompleteSourceError(f"{provider} {name} has duplicate team-week rows")
        expected = {(week, team_id) for week in weeks for team_id in expected_ids}
        if observed != expected:
            raise IncompleteSourceError(
                f"{provider} {name} coverage mismatch: missing={sorted(expected - observed)}, "
                f"extra={sorted(observed - expected)}"
            )
        return len(observed)

    roster_keys = validate_active_roster_frame(
        provider=provider,
        season=season,
        expected_team_ids=expected_team_ids,
        requested_weeks=requested_weeks,
        player_id_column=player_id_column,
        rosters=rosters,
    )
    matchup_keys = coverage(matchups, name="matchup", weeks=finalized)
    if finalized:
        score_columns = {"matchup_id", "team_points", "opponent_points"}
        if not score_columns <= set(matchups.columns):
            raise IncompleteSourceError(f"{provider} matchup final score inputs are missing")
        paired = matchups.loc[matchups["matchup_id"].notna()]
        for _, pair in paired.groupby(["week", "matchup_id"]):
            if len(pair) > 2:
                raise IncompleteSourceError(f"{provider} matchup pair has more than two teams")
            if len(pair) == 1:
                continue  # Singleton pairing/bye evidence must be checked separately.
            try:
                points = [float(value) for value in (
                    pair.iloc[0]["team_points"], pair.iloc[0]["opponent_points"],
                    pair.iloc[1]["team_points"], pair.iloc[1]["opponent_points"],
                )]
            except (TypeError, ValueError) as exc:
                raise IncompleteSourceError(f"{provider} matchup final score is invalid") from exc
            if not all(isfinite(value) for value in points):
                raise IncompleteSourceError(f"{provider} matchup final score is missing")
            if abs(points[0] - points[3]) > 0.01 or abs(points[1] - points[2]) > 0.01:
                raise IncompleteSourceError(f"{provider} matchup reciprocal score differs")
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
    if finalized and not paired.empty:
        score_columns = {"team_points", "opponent_points"}
        if not score_columns <= set(schedule_for_validation.columns):
            raise IncompleteSourceError(f"{provider} schedule final score inputs are missing")
        key_columns = ["year", "week", "team_key"]
        match_scores = paired[[*key_columns, *sorted(score_columns)]].copy()
        schedule_scores = schedule_for_validation[[*key_columns, *sorted(score_columns)]].copy()
        for frame in (match_scores, schedule_scores):
            frame["year"] = frame["year"].astype(int)
            frame["week"] = frame["week"].astype(int)
            frame["team_key"] = frame["team_key"].astype(str).str.strip()
        compared = match_scores.merge(
            schedule_scores, on=key_columns, how="left", validate="one_to_one",
            suffixes=("_matchup", "_schedule"), indicator=True,
        )
        if len(compared) != len(paired) or compared["_merge"].ne("both").any():
            raise IncompleteSourceError(f"{provider} schedule score team identity differs from matchup")
        for column in score_columns:
            matchup_values = pd.to_numeric(compared[f"{column}_matchup"], errors="coerce")
            schedule_values = pd.to_numeric(compared[f"{column}_schedule"], errors="coerce")
            if not matchup_values.map(isfinite).all() or not schedule_values.map(isfinite).all():
                raise IncompleteSourceError(f"{provider} schedule final score is missing")
            if (matchup_values - schedule_values).abs().gt(0.01).any():
                raise IncompleteSourceError(f"{provider} schedule score differs from matchup")

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
