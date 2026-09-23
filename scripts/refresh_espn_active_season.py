#!/usr/bin/env python3
"""Incrementally refresh one ESPN league's active season through the normal pipeline.

The worker first hydrates the league's existing Fly source history into a
temporary local DuckDB. It then replaces only rows for NFL games finalized in
``___ops`` and Fleet-publishes the active-season partition plus recomputed
league rollups. It never uses the legacy whole-database uploader.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import tempfile
import time
from contextlib import nullcontext
from pathlib import Path
from time import perf_counter
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_SCRIPTS = ROOT / "fantasy_football_data_scripts"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(DATA_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(DATA_SCRIPTS))


def _sql_literal(value: object) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _closed_espn_schedule(
    schedule_rows: list[dict[str, Any]],
    *,
    requested_week: int,
    current_matchup_period: int | None,
    expected_team_ids: tuple[str, ...],
) -> list[dict[str, Any]] | None:
    """Return a validated final graph, deriving stale ESPN winner markers.

    Some active ESPN leagues leave a completed period's ``winner`` at
    ``UNDECIDED`` after advancing ``currentMatchupPeriod``.  The period
    boundary is authoritative for closure, but the full team graph and finite
    scores are still required before deriving outcomes.  The current period
    is never derived, so partial midweek scores remain unpublished.
    """
    from multi_league.core.league_refresh import espn_schedule_is_final

    if espn_schedule_is_final(schedule_rows, expected_team_ids=expected_team_ids):
        return schedule_rows
    if not current_matchup_period or int(current_matchup_period) <= int(requested_week):
        return None

    normalized: list[dict[str, Any]] = []
    for source_row in schedule_rows:
        if not isinstance(source_row, dict):
            return None
        row = dict(source_row)
        raw_period = row.get("matchupPeriodId")
        try:
            matchup_period = int(raw_period)
        except (TypeError, ValueError):
            return None
        if matchup_period != int(requested_week) or matchup_period >= int(current_matchup_period):
            return None

        home = row.get("home")
        away = row.get("away")
        if not isinstance(home, dict) or not isinstance(away, dict):
            return None

        def score(side: dict[str, Any]) -> float | None:
            points_by_period = side.get("pointsByScoringPeriod")
            value = None
            if isinstance(points_by_period, dict):
                value = points_by_period.get(str(requested_week))
                if value is None:
                    value = points_by_period.get(int(requested_week))
            if value is None:
                value = side.get("totalPoints")
            try:
                number = float(value)
            except (TypeError, ValueError):
                return None
            return number if math.isfinite(number) else None

        home_score = score(home)
        away_score = score(away)
        if home_score is None or away_score is None:
            return None
        row["winner"] = (
            "HOME" if home_score > away_score
            else "AWAY" if away_score > home_score
            else "TIE"
        )
        normalized.append(row)

    if not espn_schedule_is_final(normalized, expected_team_ids=expected_team_ids):
        return None
    return normalized


def _finalized_espn_matchup_weeks(
    client: Any,
    *,
    year: int,
    weeks: list[int],
    expected_team_ids: tuple[str, ...],
    current_matchup_period: int | None = None,
    schedule_out: dict[int, list[dict[str, Any]]] | None = None,
) -> list[int]:
    """Return weeks for which ESPN has finalized every fantasy matchup."""
    finalized: list[int] = []
    for week in weeks:
        schedule_rows = client.get_raw_schedule(year, int(week))
        final_schedule = _closed_espn_schedule(
            schedule_rows,
            requested_week=int(week),
            current_matchup_period=current_matchup_period,
            expected_team_ids=expected_team_ids,
        )
        if final_schedule is not None:
            finalized.append(int(week))
            if schedule_out is not None:
                schedule_out[int(week)] = final_schedule
        else:
            outcomes = sorted({str(row.get("winner") or "") for row in schedule_rows})
            matchup_periods = sorted(
                {
                    row.get("matchupPeriodId")
                    for row in schedule_rows
                    if row.get("matchupPeriodId") is not None
                },
                key=str,
            )
            teams = sorted(
                {
                    str(side.get("teamId"))
                    for row in schedule_rows
                    for side in (row.get("home"), row.get("away"))
                    if isinstance(side, dict) and side.get("teamId") is not None
                }
            )
            print(
                f"[ESPN] {year} week {week}: fantasy outcomes are still live; "
                f"holding matchup rows (outcomes={outcomes}, "
                f"matchup_periods={matchup_periods}, teams={teams})",
                flush=True,
            )
    return finalized


def _full_espn_schedule_frame(
    *,
    ctx: Any,
    raw_schedule: list[dict[str, Any]],
    year: int,
    regular_season_weeks: int,
    expected_team_ids: tuple[str, ...],
) -> pd.DataFrame:
    """Expand ESPN's one-call season schedule into canonical team-week rows."""
    from multi_league.core.league_update_validation import IncompleteSourceError

    expected = {str(team_id) for team_id in expected_team_ids}
    rows: list[dict[str, Any]] = []
    teams_by_week: dict[int, list[str]] = {}
    for matchup in raw_schedule or []:
        if not isinstance(matchup, dict):
            continue
        try:
            week = int(matchup.get("matchupPeriodId"))
        except (TypeError, ValueError):
            continue
        if week < 1 or week > int(regular_season_weeks):
            continue
        home = matchup.get("home")
        away = matchup.get("away")
        if not isinstance(home, dict) or not isinstance(away, dict):
            raise IncompleteSourceError(f"ESPN schedule week {week} has a missing matchup side")
        try:
            home_id = int(home.get("teamId"))
            away_id = int(away.get("teamId"))
        except (TypeError, ValueError) as exc:
            raise IncompleteSourceError(f"ESPN schedule week {week} has an invalid team identity") from exc
        tier = str(matchup.get("playoffTierType") or "NONE").strip().upper()
        is_playoffs = int(tier == "WINNERS_BRACKET")
        is_consolation = int(tier != "NONE" and not is_playoffs)
        winner = str(matchup.get("winner") or "").strip().upper()
        for side_name, team_id, opponent_id in (
            ("home", home_id, away_id),
            ("away", away_id, home_id),
        ):
            team_name = ctx.get_team_name(team_id, year)
            opponent_team_name = ctx.get_team_name(opponent_id, year)
            franchise_id = ctx.get_franchise_id(team_id, year)
            opponent_franchise_id = ctx.get_franchise_id(opponent_id, year)
            result = None
            if winner in {"HOME", "AWAY"}:
                result = winner.lower() == side_name
            elif winner == "TIE":
                result = None
            rows.append({
                "year": int(year),
                "week": week,
                "manager": ctx.get_manager_name(team_id, team_name, year),
                "manager_guid": ctx.get_manager_guid(team_id, year),
                "franchise_id": franchise_id,
                "team_name": team_name,
                "manager_week": f"{franchise_id}_{int(year)}_{week}",
                "opponent": ctx.get_manager_name(opponent_id, opponent_team_name, year),
                "opponent_guid": ctx.get_manager_guid(opponent_id, year),
                "opponent_franchise_id": opponent_franchise_id,
                "win": int(result is True) if result is not None else None,
                "loss": int(result is False) if result is not None else None,
                "is_playoffs": is_playoffs,
                "is_consolation": is_consolation,
                "platform": "espn",
                "league_id": str(ctx.get_league_id_for_year(year)),
            })
            teams_by_week.setdefault(week, []).append(str(team_id))

    missing_weeks = [
        week for week in range(1, int(regular_season_weeks) + 1)
        if week not in teams_by_week
    ]
    if missing_weeks:
        raise IncompleteSourceError(
            f"ESPN full schedule is missing regular-season weeks {missing_weeks}"
        )
    for week, team_ids in sorted(teams_by_week.items()):
        observed = set(team_ids)
        if observed != expected or len(team_ids) != len(expected):
            raise IncompleteSourceError(
                f"ESPN full schedule week {week} team coverage mismatch: "
                f"missing={sorted(expected - observed)}, extra={sorted(observed - expected)}, "
                f"rows={len(team_ids)}, expected={len(expected)}"
            )
    return pd.DataFrame(rows)


def _hydrate_zero_espn_schedule_scores(
    schedules: dict[int, list[dict[str, Any]]],
    rosters: pd.DataFrame,
) -> None:
    """Fill ESPN's closed 0-0 schedule shell from its weekly started lineups."""
    from multi_league.core.league_refresh import espn_schedule_is_final
    from multi_league.core.league_update_validation import IncompleteSourceError

    required = {"week", "team_key", "fantasy_points", "is_started"}
    if schedules and (not isinstance(rosters, pd.DataFrame) or not required <= set(rosters)):
        raise IncompleteSourceError("ESPN roster rows cannot witness missing matchup totals")

    for week, schedule_rows in schedules.items():
        sides = [
            side
            for row in schedule_rows
            for side in (row.get("home"), row.get("away"))
            if isinstance(side, dict)
        ]

        def raw_score(side: dict[str, Any], scoring_period: int = week) -> float:
            points = side.get("pointsByScoringPeriod")
            value = points.get(str(scoring_period)) if isinstance(points, dict) else None
            if value is None:
                value = side.get("totalPoints")
            try:
                return float(value)
            except (TypeError, ValueError):
                return 0.0

        if any(abs(raw_score(side)) > 1e-9 for side in sides):
            continue

        weekly = rosters.loc[pd.to_numeric(rosters["week"], errors="coerce").eq(int(week))].copy()
        started = weekly["is_started"].astype(str).str.lower().isin({"true", "1"})
        weekly = weekly.loc[started]
        weekly["__points"] = pd.to_numeric(weekly["fantasy_points"], errors="coerce")
        if weekly["__points"].isna().any():
            raise IncompleteSourceError(f"ESPN week {week} started lineup has missing points")
        totals = weekly.groupby(weekly["team_key"].astype(str))["__points"].sum().to_dict()
        expected = {str(side.get("teamId")) for side in sides}
        if set(totals) != expected:
            raise IncompleteSourceError(
                f"ESPN week {week} started-lineup coverage mismatch: "
                f"missing={sorted(expected - set(totals))}, extra={sorted(set(totals) - expected)}"
            )

        for row in schedule_rows:
            scored: dict[str, float] = {}
            for side_name in ("home", "away"):
                side = row.get(side_name)
                if not isinstance(side, dict):
                    raise IncompleteSourceError(f"ESPN week {week} matchup side is missing")
                team_id = str(side.get("teamId"))
                try:
                    adjustment = float(side.get("adjustment") or 0)
                except (TypeError, ValueError) as exc:
                    raise IncompleteSourceError(f"ESPN week {week} matchup adjustment is invalid") from exc
                score = round(float(totals[team_id]) + adjustment, 2)
                side["totalPoints"] = score
                side.setdefault("pointsByScoringPeriod", {})[str(week)] = score
                scored[side_name] = score
            row["winner"] = (
                "HOME" if scored["home"] > scored["away"]
                else "AWAY" if scored["away"] > scored["home"]
                else "TIE"
            )

        if not espn_schedule_is_final(schedule_rows, expected_team_ids=tuple(expected)):
            raise IncompleteSourceError(f"ESPN week {week} lineup-derived matchup graph is incomplete")


def assert_espn_closed_matchup_weeks(
    *,
    refresh_weeks: list[int],
    finalized_matchup_weeks: list[int],
) -> None:
    """Require completed fantasy outcomes for every requested prior week.

    The newest requested week may still be live so its completed NFL games can
    be published without treating the fantasy matchup as final.  Any earlier
    requested week is already behind that live boundary and must be complete.
    """
    from multi_league.core.league_refresh import RefreshScopeError

    requested = sorted({int(week) for week in refresh_weeks})
    finalized = {int(week) for week in finalized_matchup_weeks}
    missing = [week for week in requested[:-1] if week not in finalized]
    if missing:
        raise RefreshScopeError(
            "ESPN omitted finalized fantasy matchup outcomes for prior requested weeks "
            f"{missing}"
        )


def espn_source_manifest_complete(
    *,
    refresh_weeks: list[int],
    fetch_rows: dict[str, Any],
    plan=None,
    year: int | None = None,
) -> bool:
    """Do not call a safe partial ESPN publication fully source-current."""
    from multi_league.core.league_update_plan import active_publication_covers_plan

    return (
        active_publication_covers_plan(plan, year=year, weeks=refresh_weeks)
        and fetch_rows.get("draft_validated") is True
        and not fetch_rows.get("pending_nfl_teams")
        and int(fetch_rows.get("final_matchup_weeks") or 0) == len(refresh_weeks)
    )


def _espn_finalized_roster_weeks(
    *,
    refresh_weeks: list[int],
    finalized_ops: pd.DataFrame,
) -> list[int]:
    """Fetch roster snapshots only for weeks with finalized NFL games."""
    from multi_league.core.league_refresh import finalized_roster_weeks

    return finalized_roster_weeks(
        refresh_weeks=refresh_weeks,
        finalized_ops=finalized_ops,
    )


def _build_context(
    *,
    reader: Any,
    db_name: str,
    active_year: int,
    work_dir: Path,
    active_league_id: str | None = None,
    league_ids: dict[str, str] | None = None,
    frontend_settings: dict[str, Any] | None = None,
) -> tuple[Any, Path, Any, Any]:
    """Load stored ESPN cookies and construct only the active-season context."""
    from initial_import_v3 import _load_frontend_context_settings
    from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient
    from multi_league.data_fetchers.espn.espn_context import ESPNContext, build_manager_names
    from multi_league.utils.credential_store import retrieve_espn_credentials

    credentials = retrieve_espn_credentials(db_name, reader=reader)
    if not credentials:
        raise RuntimeError(f"Fly has no usable encrypted ESPN cookies for {db_name}")

    frontend = frontend_settings if frontend_settings is not None else _load_frontend_context_settings(
        reader,
        db_name.replace("'", "''"),
    )
    league_name = (frontend.get("league_name") or credentials.get("league_name") or "").strip()
    if not league_name:
        raise RuntimeError(f"Fly has no league name for ESPN league {db_name}")

    league_id = int(active_league_id or credentials["league_id"])
    context_league_ids = {
        str(year): int(saved_id)
        for year, saved_id in (league_ids or {str(active_year): league_id}).items()
    }
    saved_active_id = context_league_ids.get(str(active_year))
    if saved_active_id is not None and saved_active_id != league_id:
        raise RuntimeError("Fly and caller have conflicting active ESPN league IDs")
    context_league_ids[str(active_year)] = league_id
    ctx = ESPNContext(
        league_id=league_id,
        league_name=league_name,
        espn_s2=credentials["espn_s2"],
        swid=credentials["swid"],
        start_year=active_year,
        end_year=active_year,
        data_directory=work_dir,
        database_name=db_name,
        manager_name_overrides=frontend.get("manager_name_overrides") or {},
        franchise_merges=frontend.get("franchise_merges") or [],
        keeper_rules=frontend.get("keeper_rules"),
        league_rules=frontend.get("league_rules"),
        standings_weights=frontend.get("standings_weights"),
        is_private=frontend.get("is_private") is True,
        import_mode="quick",
        league_ids=context_league_ids,
    )
    client = ESPNAPIClient(ctx.league_id, ctx.espn_s2, ctx.swid)
    league = client.get_league(active_year)
    if not league or not getattr(league, "teams", None):
        raise RuntimeError(f"ESPN returned no active league teams for {db_name} in {active_year}")

    ctx.num_teams = len(league.teams)
    ctx.team_to_manager = build_manager_names(league.teams)
    ctx.team_to_team_name[str(active_year)] = {}
    ctx.team_to_manager_by_year[str(active_year)] = dict(ctx.team_to_manager)
    ctx.team_to_guid_by_year[str(active_year)] = {}
    for team in league.teams:
        team_id = int(team.team_id)
        ctx.team_to_team_name[str(active_year)][team_id] = getattr(team, "team_name", f"Team {team_id}")
        owners = getattr(team, "owners", []) or []
        owner = owners[0] if isinstance(owners, list) and owners else owners
        guid = owner.get("id", "") if isinstance(owner, dict) else getattr(owner, "id", "") if owner else ""
        if guid:
            ctx.team_to_guid[team_id] = guid
            ctx.team_to_guid_by_year[str(active_year)][team_id] = guid

    context_path = work_dir / "espn_context.json"
    ctx.save(context_path)
    return ctx, context_path, client, league


def _espn_draft_manifest(client: Any, league: Any, year: int) -> tuple[pd.DataFrame, bool]:
    """Prove parsed picks against ESPN's raw draft and configured draft size."""
    from multi_league.core.league_refresh import RefreshScopeError

    raw = client.get_raw_league(year, ("mDraftDetail", "mSettings"))
    detail = raw.get("draftDetail") if isinstance(raw, dict) else None
    settings = raw.get("settings") if isinstance(raw, dict) else None
    if not isinstance(detail, dict) or not isinstance(settings, dict):
        raise RefreshScopeError("ESPN draft witness lacks raw draft detail or settings")
    picks = detail.get("picks")
    parsed = getattr(league, "draft", None)
    if not isinstance(picks, list) or parsed is None:
        raise RefreshScopeError("ESPN draft witness lacks an explicit pick list")
    if detail.get("drafted") is False and not picks and not parsed:
        return pd.DataFrame(columns=["pick"]), True
    raw_player_ids = [pick.get("playerId") for pick in picks]
    placeholder_slots = bool(picks) and all(
        player_id in (None, "")
        or (str(player_id).lstrip("-").isdigit() and int(player_id) <= 0)
        for player_id in raw_player_ids
    )
    if detail.get("drafted") is False and placeholder_slots and not parsed:
        return pd.DataFrame(columns=["pick"]), True
    if detail.get("inProgress") is True:
        raise RefreshScopeError(
            "ESPN active draft is not confirmed complete "
            f"(drafted={detail.get('drafted')!r}, "
            f"in_progress={detail.get('inProgress')!r}, "
            f"raw_picks={len(picks)}, parsed_picks={len(parsed)})"
        )

    roster = settings.get("rosterSettings") or {}
    slots = roster.get("lineupSlotCounts") if isinstance(roster, dict) else None
    try:
        teams = int(settings["size"])
        # ESPN slot 21 is injured reserve and has no draft pick. All other
        # positive lineup and bench slots require one draft pick per team.
        rounds = sum(int(count) for slot, count in slots.items() if str(slot) != "21")
    except (KeyError, TypeError, ValueError, AttributeError) as exc:
        raise RefreshScopeError("ESPN draft witness lacks team, slot, or pick identities") from exc

    # Some completed ESPN drafts retain whole unused rounds as playerId=0
    # slots (for example optional reserve positions).  Admit only a contiguous
    # suffix of complete rounds; a zero inside a drafted round still fails
    # closed.  Negative IDs remain real D/ST selections.
    from multi_league.data_fetchers.espn.espn_draft import is_unfilled_espn_draft_pick

    trailing_empty_slots = 0
    for pick in reversed(picks):
        if not is_unfilled_espn_draft_pick(pick.get("playerId")):
            break
        trailing_empty_slots += 1
    if trailing_empty_slots:
        if teams < 1 or trailing_empty_slots % teams:
            raise RefreshScopeError("ESPN draft has a partial trailing empty round")
        real_pick_count = len(picks) - trailing_empty_slots
        if len(parsed) == len(picks):
            parsed_suffix = list(parsed)[real_pick_count:]
            if not all(
                is_unfilled_espn_draft_pick(
                    getattr(pick, "playerId", None),
                    getattr(pick, "playerName", None),
                )
                for pick in parsed_suffix
            ):
                raise RefreshScopeError("ESPN parsed draft disagrees with trailing empty slots")
            parsed = list(parsed)[:real_pick_count]
        elif len(parsed) != real_pick_count:
            raise RefreshScopeError("ESPN parsed draft disagrees with trailing empty slot count")
        picks = picks[:real_pick_count]
        rounds -= trailing_empty_slots // teams
        if rounds < 1:
            raise RefreshScopeError("ESPN draft has no completed rounds")

    try:
        pick_numbers = [int(pick["overallPickNumber"]) for pick in picks]
    except (KeyError, TypeError, ValueError) as exc:
        raise RefreshScopeError("ESPN draft witness lacks overall pick identities") from exc
    expected = teams * rounds
    if teams < 1 or rounds < 1 or len(pick_numbers) != expected or len(parsed) != expected:
        raise RefreshScopeError("ESPN draft pick count disagrees with configured draft size")
    if sorted(pick_numbers) != list(range(1, expected + 1)):
        raise RefreshScopeError("ESPN raw draft has missing or duplicate overall picks")
    pick_order = (settings.get("draftSettings") or {}).get("pickOrder")
    if pick_order and len(pick_order) != teams:
        raise RefreshScopeError("ESPN draft order disagrees with league team count")

    parsed_player_ids = [getattr(pick, "playerId", None) for pick in parsed]
    parsed_player_names = [str(getattr(pick, "playerName", None) or "").strip() for pick in parsed]
    raw_player_ids = [pick.get("playerId") for pick in picks]
    unresolved_player_ids = [
        "<missing>" if player_id in (None, "") else str(player_id)
        for player_id, player_name in zip(parsed_player_ids, parsed_player_names)
        if player_id in (None, "") or not player_name or player_name.lower() == "unknown"
    ]
    if unresolved_player_ids:
        raise RefreshScopeError(
            "ESPN draft has unresolved player identities "
            f"(count={len(unresolved_player_ids)}, sample={unresolved_player_ids[:5]})"
        )
    if sorted(map(str, parsed_player_ids)) != sorted(map(str, raw_player_ids)):
        raise RefreshScopeError("ESPN parsed draft player identities disagree with raw picks")
    manifest = pd.DataFrame({
        "pick": pick_numbers,
        "espn_player_id": parsed_player_ids,
        "player": parsed_player_names,
    }).sort_values("pick", ignore_index=True)
    return manifest, False


def _discard_espn_placeholder_draft(
    local_db: Any,
    *,
    db_name: str,
    year: int,
    league_id: str,
) -> int:
    """Remove only the invalid pre-draft slots emitted by ESPN as picks."""
    from multi_league.core.league_refresh import RefreshScopeError

    if not local_db.table_exists("draft"):
        return 0
    active = local_db.read_table("draft", year=year)
    if active is None or active.empty:
        return 0
    required = {"espn_player_id", "player"}
    if not required.issubset(active.columns):
        raise RefreshScopeError("ESPN placeholder draft lacks identity columns")
    player_ids = pd.to_numeric(active["espn_player_id"], errors="coerce")
    names = active["player"].fillna("").astype(str).str.strip().str.lower()
    placeholders = (player_ids.isna() | player_ids.le(0)) & names.isin({"", "unknown"})
    if not placeholders.all():
        raise RefreshScopeError("ESPN draft is undrafted; refusing to remove real picks")
    local_db.connect().execute(
        """
        DELETE FROM public.draft
        WHERE db_name = ? AND year = ? AND platform = 'espn' AND league_id = ?
        """,
        [str(db_name), int(year), str(league_id)],
    )
    return int(len(active))


def _explicit_empty_partitions(fetch_rows: dict[str, Any]) -> set[str]:
    """Carry a verified placeholder cleanup through the atomic fleet publish."""
    return {"draft"} if int(fetch_rows.get("placeholder_draft_rows_removed") or 0) > 0 else set()


def _hydrate_espn_draft_player_names(league: Any, rosters: pd.DataFrame) -> dict[str, str]:
    """Fill stale draft names from the already-fetched active roster payload."""
    if rosters is None or rosters.empty or not {"espn_player_id", "player"}.issubset(rosters.columns):
        return {}
    names: dict[str, str] = {}
    for player_id, group in rosters.dropna(subset=["espn_player_id", "player"]).groupby("espn_player_id"):
        candidates = {
            str(value).strip()
            for value in group["player"].tolist()
            if str(value).strip() and str(value).strip().lower() != "unknown"
        }
        if len(candidates) == 1:
            names[str(player_id)] = next(iter(candidates))
    for pick in getattr(league, "draft", []) or []:
        current = str(getattr(pick, "playerName", None) or "").strip()
        if current and current.lower() != "unknown":
            continue
        resolved = names.get(str(getattr(pick, "playerId", "")))
        if resolved:
            pick.playerName = resolved
    return names


def _merge_active_payloads(
    *,
    ctx: Any,
    client: Any,
    league: Any,
    local_db: Any,
    active_year: int,
    refresh_weeks: list[int],
    finalized_ops: pd.DataFrame,
) -> dict[str, Any]:
    """Fetch ESPN state without letting a provider adapter replace old weeks."""
    from multi_league.core.canonical_settings import flatten_settings
    from multi_league.core.league_update_validation import (
        validate_active_roster_frame,
        validate_espn_final_matchup_frame,
        validate_provider_team_inventory,
    )
    from multi_league.core.league_refresh import (
        assert_provider_roster_merge,
        filter_rosters_to_finalized_games,
        merge_provider_refresh_table,
        prune_unfinalized_provider_matchups,
        refresh_authoritative_draft_partition,
        pending_provider_nfl_teams,
        start_background_refresh_call,
    )
    from multi_league.data_fetchers.espn.espn_api_client import ESPNAPIClient
    from multi_league.data_fetchers.espn.espn_draft import fetch_espn_draft
    from multi_league.data_fetchers.espn.espn_league_settings import fetch_espn_settings
    from multi_league.data_fetchers.espn.espn_matchups import fetch_espn_matchups
    from multi_league.data_fetchers.espn.espn_rosters import fetch_espn_rosters_modern
    from multi_league.data_fetchers.espn.espn_transactions import fetch_espn_transactions

    league_id = str(ctx.get_league_id_for_year(active_year))
    raw_settings = fetch_espn_settings(ctx, active_year, client=client, league=league)
    if not raw_settings:
        raise RuntimeError(f"ESPN returned no settings for {active_year} ({league_id})")
    settings = pd.DataFrame([flatten_settings(raw_settings, "espn", active_year, league_id)])
    expected_team_ids = validate_provider_team_inventory(
        provider="espn",
        settings_team_count=settings.iloc[0]["num_teams"],
        team_ids=tuple(str(int(team.team_id)) for team in league.teams),
    )
    settings["db_name"] = local_db.league_name
    merge_provider_refresh_table(
        local_db, "league_settings", settings, platform="espn", league_id=league_id
    )

    playoff_start_week = int(settings.iloc[0]["playoff_start_week"])
    regular_season_weeks = playoff_start_week - 1
    if regular_season_weeks < 1:
        raise RuntimeError(f"ESPN returned an invalid playoff start week for {active_year}")

    def fetch_secondary_payloads():
        secondary_client = ESPNAPIClient(ctx.league_id, ctx.espn_s2, ctx.swid)
        full_payload = secondary_client.get_raw_league(
            active_year, ("mScoreboard", "mMatchupScore")
        )
        full_schedule = _full_espn_schedule_frame(
            ctx=ctx,
            raw_schedule=full_payload.get("schedule", []) if isinstance(full_payload, dict) else [],
            year=active_year,
            regular_season_weeks=regular_season_weeks,
            expected_team_ids=expected_team_ids,
        )
        schedules: dict[int, list[dict[str, Any]]] = {}
        final_weeks = _finalized_espn_matchup_weeks(
            secondary_client,
            year=active_year,
            weeks=refresh_weeks,
            expected_team_ids=expected_team_ids,
            current_matchup_period=max(refresh_weeks),
            schedule_out=schedules,
        )
        transaction_rows = fetch_espn_transactions(
            ctx,
            active_year,
            max_week=max(refresh_weeks),
            client=secondary_client,
            league=league,
        )
        return final_weeks, schedules, transaction_rows, full_schedule

    secondary_payload_future = start_background_refresh_call(fetch_secondary_payloads)

    # This lower-level fetcher has an explicit week scope and does not call
    # LocalLeagueDB.save_table(), which would delete all prior active-season
    # rows before the narrow refresh can merge its safe replacement rows.
    box_scores_by_week: dict[int, list] = {}
    roster_weeks = _espn_finalized_roster_weeks(
        refresh_weeks=refresh_weeks,
        finalized_ops=finalized_ops,
    )
    if roster_weeks:
        rosters = fetch_espn_rosters_modern(
            ctx,
            active_year,
            db=local_db,
            max_weeks=max(roster_weeks),
            weeks=roster_weeks,
            client=client,
            league=league,
            box_scores_out=box_scores_by_week,
        )
        provider_roster_team_weeks = validate_active_roster_frame(
            provider="espn",
            season=active_year,
            expected_team_ids=expected_team_ids,
            requested_weeks=tuple(roster_weeks),
            player_id_column="espn_player_id",
            rosters=rosters,
        )
    else:
        rosters = pd.DataFrame()
        provider_roster_team_weeks = 0
    draft_player_names = _hydrate_espn_draft_player_names(league, rosters)
    roster_rows = 0
    pending_nfl_teams: set[str] = set()
    for week in roster_weeks:
        source = rosters.loc[rosters["week"].astype(int) == int(week)].copy() if rosters is not None else pd.DataFrame()
        ops_week = finalized_ops.loc[finalized_ops["week"].astype(int) == int(week)]
        pending_nfl_teams.update(pending_provider_nfl_teams(source, ops_week))
        safe_rows = filter_rosters_to_finalized_games(
            source,
            ops_week,
        )
        if safe_rows.empty:
            continue
        merge_provider_refresh_table(
            local_db,
            "player_fantasy",
            safe_rows,
            platform="espn",
            league_id=league_id,
        )
        assert_provider_roster_merge(
            local_db,
            safe_rows,
            year=active_year,
            week=week,
            provider_id_column="espn_player_id",
        )
        roster_rows += len(safe_rows)

    final_matchup_weeks, final_schedule_graphs, transactions, full_schedule = secondary_payload_future.result()
    merge_provider_refresh_table(
        local_db,
        "schedule",
        full_schedule,
        platform="espn",
        league_id=league_id,
    )
    _hydrate_zero_espn_schedule_scores(final_schedule_graphs, rosters)
    assert_espn_closed_matchup_weeks(
        refresh_weeks=refresh_weeks,
        finalized_matchup_weeks=final_matchup_weeks,
    )
    stale_matchup_rows_removed = prune_unfinalized_provider_matchups(
        local_db,
        year=active_year,
        requested_weeks=refresh_weeks,
        finalized_weeks=final_matchup_weeks,
        platform="espn",
        league_id=league_id,
    )
    matchup_rows = 0
    if final_matchup_weeks:
        matchups = fetch_espn_matchups(
            ctx,
            active_year,
            weeks=final_matchup_weeks,
            client=client,
            league=league,
            box_scores_by_week=box_scores_by_week,
            raw_schedules_by_week=final_schedule_graphs,
        )
        for week in final_matchup_weeks:
            weekly_matchups = (
                matchups.loc[matchups["week"].astype(int) == int(week)].copy()
                if isinstance(matchups, pd.DataFrame) and "week" in matchups else pd.DataFrame()
            )
            validate_espn_final_matchup_frame(
                season=active_year,
                week=week,
                expected_team_ids=expected_team_ids,
                raw_schedule=final_schedule_graphs[week],
                matchups=weekly_matchups,
            )
        merge_provider_refresh_table(
            local_db,
            "matchup",
            matchups,
            platform="espn",
            league_id=league_id,
        )
        matchup_rows = len(matchups)

    if transactions is not None and not transactions.empty:
        merge_provider_refresh_table(
            local_db,
            "transactions",
            transactions,
            platform="espn",
            league_id=league_id,
        )
    draft_rows = 0
    draft_manifest, confirmed_no_draft = _espn_draft_manifest(client, league, active_year)
    placeholder_draft_rows_removed = (
        _discard_espn_placeholder_draft(
            local_db,
            db_name=local_db.league_name,
            year=active_year,
            league_id=league_id,
        )
        if confirmed_no_draft else 0
    )
    draft_rows = refresh_authoritative_draft_partition(
        local_db,
        provider_manifest=draft_manifest,
        key_columns=("pick", "espn_player_id", "player"),
        fetch_full=lambda: fetch_espn_draft(
            ctx,
            active_year,
            player_names_by_id=draft_player_names,
            client=client,
            league=league,
        ),
        year=active_year,
        platform="espn",
        league_id=league_id,
        confirmed_no_draft=confirmed_no_draft,
    )
    return {
        "provider_roster_team_weeks": provider_roster_team_weeks,
        "roster_rows": int(roster_rows),
        "final_matchup_rows": int(matchup_rows),
        "stale_matchup_rows_removed": int(stale_matchup_rows_removed),
        "final_matchup_weeks": len(final_matchup_weeks),
        "schedule_rows": int(len(full_schedule)),
        "schedule_weeks": int(regular_season_weeks),
        "transaction_rows": int(len(transactions) if transactions is not None else 0),
        "draft_rows": draft_rows,
        "placeholder_draft_rows_removed": placeholder_draft_rows_removed,
        "draft_validated": True,
        "pending_nfl_teams": sorted(pending_nfl_teams),
    }


def _write_receipt(receipt: dict[str, Any], path: Path | None) -> None:
    """Emit the non-secret receipt and retain it for the Actions artifact."""
    from scripts.league_update_workflow_receipt import write_refresh_receipt

    write_refresh_receipt(receipt, path)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, help="ESPN league db_name")
    parser.add_argument("--year", type=int, default=None, help="Active NFL season (default: latest Fly ops season)")
    parser.add_argument("--through-week", type=int, default=None, help="Optional final-week ceiling")
    parser.add_argument("--observed-manifest-digest")
    parser.add_argument("--execute", action="store_true", help="Commit the scoped Fleet bundle to Fly")
    parser.add_argument("--json-out", type=Path, help="Optional non-secret run receipt path")
    args = parser.parse_args(argv)

    os.environ["DATABASE_BACKEND"] = "fly"
    from multi_league.core.fleet_publish import build_fleet_partition_bundle
    from multi_league.core.league_refresh import (
        RefreshScopeError,
        active_nfl_player_ids,
        active_platform_player_ids,
        active_platform_player_names,
        active_platform_player_name_hints,
        active_refresh_publish_tables,
        background_refresh_call,
        refresh_weeks_for_run,
        finalized_source_boundary,
        hydrate_local_refresh_sources,
        run_independent_refresh_preflight,
        start_background_refresh_call,
        stage_refresh_partitions,
        sync_player_bio_cache_from_fly,
    )
    from multi_league.core.league_update_validation import IncompleteSourceError
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.league_update_plan import active_provider_league_id, load_persisted_refresh_plan
    from multi_league.core.league_update_timing import PhaseTimer
    from multi_league.core.readers.fly_reader import FlyReader
    from multi_league.core.targets.fly_target import FlyTarget
    from scripts.league_update_workflow_receipt import record_publication_commit
    from scripts.refresh_yahoo_active_season import (
        UPDATE_REFRESH_SOURCE_TABLES,
        OPS_DATABASE,
        _active_year_scoring_info,
        _ensure_active_year_ops_cache,
        _ensure_ops_cache_matches_live,
        _load_active_refresh_inputs,
        _active_update_segment_from_source_frames,
        _frontend_settings_from_source_context,
        _capture_update_source_frames,
        _run_local_pipeline,
        _scope_counts,
        _split_active_transform_source_frames,
    )

    timer = PhaseTimer()
    reader = FlyReader()
    from multi_league.core.fly_writer import FlyWriter
    from multi_league.core.league_update_status import start_league_update_execution
    from scripts.claim_manual_league_update import prepare_update_execution
    writer = FlyWriter()
    execution = prepare_update_execution(
        reader,
        writer,
        db_name=args.db,
        platform="espn",
        execute=args.execute,
        observed_manifest_digest=args.observed_manifest_digest,
        dispatch_token=os.environ.get("LEAGUE_UPDATE_TOKEN"),
        attempt_id=os.environ.get("LEAGUE_UPDATE_ATTEMPT_ID"),
        claim_version=int(os.environ.get("LEAGUE_UPDATE_CLAIM_VERSION") or 1),
        run_id=int(os.environ.get("GITHUB_RUN_ID") or 0),
        run_attempt=int(os.environ.get("GITHUB_RUN_ATTEMPT") or 0),
        output_path=Path(os.environ["GITHUB_OUTPUT"]) if os.environ.get("GITHUB_OUTPUT") else None,
    )
    active_year = args.year or int(
        reader.query_scalar("SELECT MAX(year) FROM nfl_historical.nfl_player_stats_all", database=OPS_DATABASE)
    )
    from multi_league.core.league_update_lineage import assert_canonical_history_complete

    # The source-plan and active-input scans share the same Fly DuckDB. Run
    # them serially so two safe league-scoped reads do not amplify each other
    # into a slow no-op preflight.
    preflight = run_independent_refresh_preflight({
        "entitlement": lambda: start_league_update_execution(
            reader,
            writer,
            database_name=args.db,
            platform="espn",
            dispatch_token=execution["dispatch_token"],
            attempt_id=execution["attempt_id"],
            claim_version=execution["claim_version"],
            workflow_run_id=os.environ.get("GITHUB_RUN_ID"),
        ) if args.execute else None,
        "canonical_history": lambda: assert_canonical_history_complete(
            reader, database_name=args.db, active_season=active_year
        ),
    })
    canonical_history = preflight["canonical_history"]
    source_snapshot_future = start_background_refresh_call(
        lambda: _capture_update_source_frames(
            reader,
            db_name=args.db,
            active_year=active_year,
            tables=UPDATE_REFRESH_SOURCE_TABLES,
        )
    )
    source_plan_stage_seconds: dict[str, float] = {}
    source_stage_started = perf_counter()
    persisted_plan = load_persisted_refresh_plan(
        reader,
        database_name=args.db,
        active_season=active_year,
        expected_observed_digest=execution["observed_manifest_digest"],
    )
    source_plan_stage_seconds["persisted_plan"] = round(perf_counter() - source_stage_started, 3)
    source_stage_started = perf_counter()
    finalized_ops, last_materialized_week = _load_active_refresh_inputs(
        reader,
        db_name=args.db,
        year=active_year,
        through_week=args.through_week,
    )
    source_plan_stage_seconds["active_inputs"] = round(perf_counter() - source_stage_started, 3)
    print(f"[weekly-refresh-source] {source_plan_stage_seconds}")
    if finalized_ops.empty:
        raise RuntimeError(f"No finalized regular-season ops facts for {active_year}")
    if args.execute and persisted_plan is None:
        raise RuntimeError("executing update requires a captured source manifest")
    captured_league_id = active_provider_league_id(persisted_plan, provider="espn")
    refresh_weeks = refresh_weeks_for_run(
        planned_weeks=persisted_plan.weeks if persisted_plan is not None else None,
        finalized_weeks=finalized_ops["week"].dropna().tolist(),
        last_materialized_week=last_materialized_week,
        through_week=args.through_week,
    )
    receipt: dict[str, Any] = {
        "db_name": args.db,
        "year": active_year,
        "refresh_weeks": refresh_weeks,
        "executed": bool(args.execute),
        "source_plan_stage_seconds": source_plan_stage_seconds,
        "canonical_history": canonical_history,
    }
    receipt.update(finalized_source_boundary(finalized_ops, year=active_year))
    timer.mark("source_plan")
    if persisted_plan is not None:
        receipt["source_manifest_digest"] = persisted_plan.observed_manifest_digest
        receipt["source_manifest_json"] = persisted_plan.observed_manifest_json
        receipt["published_manifest_digest"] = persisted_plan.published_manifest_digest
        receipt["refresh_reasons"] = list(persisted_plan.reasons)
    if not refresh_weeks:
        if args.execute:
            from scripts.league_update_workflow_receipt import (
                record_missing_season_rollups_repair,
                record_missing_manager_rankings_repair,
            )

            repaired = record_missing_season_rollups_repair(
                receipt,
                reader=reader,
                db_name=args.db,
                active_year=active_year,
                platform="espn",
                path=args.json_out,
            )
            repaired = record_missing_manager_rankings_repair(
                receipt,
                reader=reader,
                db_name=args.db,
                active_year=active_year,
                platform="espn",
                path=args.json_out,
            ) or repaired
            if repaired:
                receipt["phase_seconds"] = timer.finish()
                _write_receipt(receipt, args.json_out)
                return 0
        receipt["status"] = "NO_FINALIZED_WEEKS"
        receipt["phase_seconds"] = timer.finish()
        _write_receipt(receipt, args.json_out)
        return 0

    with tempfile.TemporaryDirectory(prefix=f"{args.db}_weekly_refresh_") as temp_dir:
        work_dir = Path(temp_dir)
        source_frames, base_generation = source_snapshot_future.result()
        timer.mark("source_snapshot")
        from multi_league.core.homepage_refresh import _load_homepage_source_frames

        homepage_source_future = start_background_refresh_call(
            lambda: _load_homepage_source_frames(reader, args.db)
        ) if args.execute else None
        receipt["base_generation"] = base_generation
        if source_frames["league_context"].empty or source_frames["league_settings"].empty:
            raise RuntimeError(f"Fly has no reusable context/settings for {args.db}")
        active_segment = _active_update_segment_from_source_frames(
            source_frames,
            db_name=args.db,
            active_year=active_year,
            expected_platform="espn",
        )
        receipt["active_segment"] = {
            "platform": active_segment.platform,
            "current_league_id": active_segment.current_league_id,
            "historical_platforms": list(active_segment.historical_platforms),
        }
        from multi_league.core.league_update_lineage import merge_provider_chain_ids
        from multi_league.core.league_update_ownership import source_preservation_snapshot

        preservation_witnesses = source_preservation_snapshot(source_frames)
        transform_source_frames, historical_source_rows = _split_active_transform_source_frames(
            source_frames, active_year=active_year,
        )
        frontend_settings = _frontend_settings_from_source_context(
            source_frames["league_context"],
            db_name=args.db,
        )
        imported_chain = merge_provider_chain_ids(
            frontend_settings.get("league_ids"), active_segment,
        )
        context_future = start_background_refresh_call(
            lambda: _build_context(
                reader=reader,
                db_name=args.db,
                active_year=active_year,
                active_league_id=captured_league_id or active_segment.current_league_id,
                league_ids=imported_chain,
                work_dir=work_dir,
                frontend_settings=frontend_settings,
            )
        )
        local_db = LocalLeagueDB(work_dir, args.db)
        try:
            receipt["hydrated_rows"] = hydrate_local_refresh_sources(
                local_db,
                transform_source_frames,
                db_name=args.db,
                active_year=active_year,
                expected_platform="espn",
            )
            # The local transform input intentionally contains only the active
            # season.  Compare the finished rebuild to the full Fly snapshot,
            # otherwise correctly restored historical source rows appear new.
            preservation_before = preservation_witnesses
            ctx, context_path, client, league = context_future.result()
            timer.mark("local_hydration")
            active_scoring = _active_year_scoring_info(
                local_db,
                db_name=args.db,
                year=active_year,
            )
            ops_context = background_refresh_call(
                lambda: _ensure_active_year_ops_cache(
                    reader,
                    year=active_year,
                    work_dir=work_dir,
                    scoring_info=active_scoring,
                )
            ) if args.execute else nullcontext(None)
            with ops_context as ops_future:
                try:
                    receipt["fetch_rows"] = _merge_active_payloads(
                        ctx=ctx,
                        client=client,
                        league=league,
                        local_db=local_db,
                        active_year=active_year,
                        refresh_weeks=refresh_weeks,
                        finalized_ops=finalized_ops,
                    )
                except (RefreshScopeError, IncompleteSourceError) as exc:
                    receipt["status"] = "INCOMPLETE_SOURCE"
                    receipt["error_code"] = "espn_provider_response_incomplete"
                    receipt["error"] = str(exc)
                    receipt["phase_seconds"] = timer.finish()
                    _write_receipt(receipt, args.json_out)
                    raise
                receipt["source_manifest_complete"] = espn_source_manifest_complete(
                    refresh_weeks=refresh_weeks,
                    fetch_rows=receipt["fetch_rows"],
                    plan=persisted_plan, year=active_year,
                )
                from multi_league.core.league_update_plan import active_publication_covers_plan
                receipt["source_manifest_scope_complete"] = (
                    active_publication_covers_plan(
                        persisted_plan, year=active_year, weeks=refresh_weeks,
                    )
                    and receipt["fetch_rows"].get("draft_validated") is True
                )
                timer.mark("provider_fetch")
                if not args.execute:
                    receipt["status"] = "DRY_RUN_READY"
                    receipt["phase_seconds"] = timer.finish()
                    _write_receipt(receipt, args.json_out)
                    return 0
                ops_future.result()
                _ensure_active_year_ops_cache(
                    reader,
                    year=active_year,
                    work_dir=work_dir,
                    scoring_info=_active_year_scoring_info(
                        local_db, db_name=args.db, year=active_year,
                    ),
                )
            timer.mark("player_ops_seed")

            from multi_league.core.league_update_validation import (
                assert_transformed_active_matchup_scope,
                assert_transformed_active_player_scope,
                capture_active_final_matchup_scope,
                capture_active_provider_player_scope,
            )
            expected_player_keys = capture_active_provider_player_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="espn_player_id",
            )
            expected_matchup_scores = capture_active_final_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year, weeks=refresh_weeks,
            )

            active_connection = local_db.connect()
            receipt["player_bio_sync"] = sync_player_bio_cache_from_fly(
                reader,
                ops_cache=Path(os.environ.get("OPS_CACHE_PATH", "")),
                platform="espn",
                provider_ids=active_platform_player_ids(active_connection, platform="espn"),
                player_names=active_platform_player_names(active_connection, platform="espn"),
                nfl_player_ids=active_nfl_player_ids(active_connection),
                provider_name_hints=active_platform_player_name_hints(active_connection, platform="espn"),
            )
            timer.mark("player_bio_sync")
            receipt["ops_cache"] = str(
                _ensure_ops_cache_matches_live(
                    reader,
                    finalized_ops,
                    year=active_year,
                    weeks=refresh_weeks,
                    work_dir=work_dir,
                )
            )
            timer.mark("player_ops_cache")
            _run_local_pipeline(
                ctx=ctx,
                context_path=context_path,
                local_db=local_db,
                db_name=args.db,
                active_year=active_year,
                work_dir=work_dir,
                platform="espn",
                keeper_config_hydrated="keeper_config" in transform_source_frames,
                historical_source_rows=historical_source_rows,
                frontend_configuration_rows=preservation_witnesses,
            )
            timer.mark("shared_transformations")
            from multi_league.core.league_update_ownership import restore_active_derived_source_values

            receipt["restored_active_derived_values"] = restore_active_derived_source_values(
                local_db,
                transform_source_frames,
                active_year=active_year,
            )
            timer.mark("restore_active_derived_values")
            receipt["transformed_player_scope"] = assert_transformed_active_player_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="espn_player_id",
                expected_keys=expected_player_keys,
            )
            receipt["transformed_matchup_scope"] = assert_transformed_active_matchup_scope(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, expected_scores=expected_matchup_scores,
            )
            timer.mark("transformed_scope_validation")
            from multi_league.core.homepage_refresh import prepare_homepage_refresh

            homepage_started = time.monotonic()
            receipt["homepage_refresh"] = prepare_homepage_refresh(
                reader=reader, local_db=local_db, db_name=args.db, active_year=active_year,
                source_frames=homepage_source_future.result(),
            )
            receipt["homepage_seconds"] = round(time.monotonic() - homepage_started, 3)
            timer.mark("homepage_refresh")
            local_db.connect()
            from multi_league.core.league_update_publish_claim import renew_claim_for_publication

            claim_future = start_background_refresh_call(
                lambda: renew_claim_for_publication(reader, database_name=args.db, platform="espn")
            )
            stage_timer = PhaseTimer()
            from multi_league.core.league_update_ownership import (
                assert_refresh_preservation,
                local_preservation_snapshot,
            )
            from multi_league.core.league_refresh import finalized_ops_player_weeks

            preservation_after = local_preservation_snapshot(local_db, preservation_before)
            stage_timer.mark("preservation_snapshot")
            receipt["preservation"] = assert_refresh_preservation(
                preservation_before,
                preservation_after,
                active_year=active_year,
                finalized_ops_player_weeks=finalized_ops_player_weeks(finalized_ops, year=active_year),
            )
            stage_timer.mark("preservation_validation")
            publish_tables = active_refresh_publish_tables(
                local_db.connect(),
                server_rebuilds_career_rollups=True,
                server_rebuilds_homepage_rollups=False,
            )
            from multi_league.core.league_update_validation import assert_refresh_derived_output_health

            receipt["derived_health"] = assert_refresh_derived_output_health(
                local_db.connect(), db_name=args.db, year=active_year,
                weeks=refresh_weeks, provider_id_column="espn_player_id",
                published_tables=publish_tables,
                server_rebuilds_career_rollups=True,
                server_rebuilds_homepage_rollups=False,
            )
            from multi_league.core.league_update_ownership import assert_publish_table_ownership

            receipt["ownership"] = assert_publish_table_ownership(publish_tables)
            stage_timer.mark("derived_output_validation")
            stage = stage_refresh_partitions(
                local_db.connect(),
                db_name=args.db,
                active_year=active_year,
                tables=publish_tables,
            )
            stage_timer.mark("stage_partitions")
            try:
                if not publish_tables:
                    raise RuntimeError("refresh pipeline produced no active-season publish tables")
                bundle = build_fleet_partition_bundle(
                    stage,
                    active_year=active_year,
                    league_generations={args.db: base_generation},
                    tables=publish_tables,
                    output_dir=work_dir / "bundle",
                    rebuild_career_rollups=True,
                    rebuild_homepage_rollups=False,
                    repair_missing_season_rollups=(
                        "missing_derived_aggregate" in persisted_plan.reasons
                    ),
                    empty_active_partitions=_explicit_empty_partitions(receipt["fetch_rows"]),
                )
            finally:
                stage.close()
            stage_timer.mark("bundle_build")
            receipt["homepage_preservation_stage_seconds"] = stage_timer.finish()
            timer.mark("homepage_preservation_stage")
            claim_future.result()
            timer.mark("prepublish_claim")
            result = FlyTarget().merge_fleet_partition(
                bundle.path,
                bundle_id=bundle.bundle_id,
                bundle_hash=bundle.bundle_hash,
                merge_timeout_seconds=40,
            )
            record_publication_commit(
                receipt, result=result, bundle_id=bundle.bundle_id, path=args.json_out,
            )
            receipt["homepage_rows"] = receipt["homepage_refresh"]["rows"]
            receipt["season_rollups"] = result.get("season_rollups", {}).get(args.db, {})
            receipt["season_seconds"] = result.get("season_seconds", {}).get(args.db)
            receipt["career_rollups"] = result.get("career_rollups", {}).get(args.db, {})
            receipt["career_seconds"] = result.get("career_seconds", {}).get(args.db)
            receipt["published_tables"] = sorted(
                set(publish_tables)
                | set(receipt["season_rollups"])
                | set(receipt["career_rollups"])
                | set(receipt["homepage_rows"])
            )
            timer.mark("fly_publication")
            receipt["post_publish_counts"] = _scope_counts(
                reader,
                db_name=args.db,
                active_year=active_year,
                tables=receipt["published_tables"],
            )
            timer.mark("post_publish_verification")
        finally:
            local_db.close()

    receipt["phase_seconds"] = timer.finish()
    _write_receipt(receipt, args.json_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
