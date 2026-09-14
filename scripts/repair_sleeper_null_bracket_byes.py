#!/usr/bin/env python3
"""Repair stale Sleeper null-matchup playoff pairings into bye rows.

Some historical imports paired Sleeper rows with ``matchup_id = NULL`` using
later bracket metadata. Those rows carry weekly scores, but Sleeper did not
record them as head-to-head games for that week. The current fetcher skips those
leftover rows and lets the bye filler create neutral postseason bye rows.

This script performs the same narrow correction in Fly without reimporting the
whole league:

* read affected Sleeper league-seasons from Fly
* fetch Sleeper winners/losers brackets for the league id in league_settings
* find live rows where a NULL matchup_id row is paired against an opponent that
  is not an API bracket pairing for that week's round
* convert only those rows to neutral bye rows
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent.parent
FFS_DIR = ROOT / "fantasy_football_data_scripts"
if str(FFS_DIR) not in sys.path:
    sys.path.insert(0, str(FFS_DIR))

from multi_league.core.db_reader import get_reader
from multi_league.core.fly_writer import FlyWriter
from multi_league.data_fetchers.sleeper.playoff_utils import (
    bracket_team_ids,
    playoff_round_for_week,
)
from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient

DATABASE = "___leagues"
REAL_OPPONENT_SENTINELS = {"", "bye", "none", "null", "nan"}
CHUNK_SIZE = 150


@dataclass(frozen=True)
class SeasonKey:
    db_name: str
    year: int


@dataclass(frozen=True)
class SeasonSettings:
    db_name: str
    year: int
    league_id: str
    playoff_start_week: int
    end_week: int
    playoff_teams: int
    playoff_round_type: int


@dataclass(frozen=True)
class RepairRow:
    db_name: str
    year: int
    week: int
    franchise_id: str
    team_key: str
    old_opponent_franchise_id: str
    old_opponent: str
    old_team_points: float | None
    old_opponent_points: float | None
    is_playoffs: bool
    is_consolation: bool
    reason: str


def _load_env() -> None:
    for env_file in (ROOT / ".env", ROOT / "frontend" / ".env.local"):
        if not env_file.exists():
            continue
        for raw in env_file.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _sql_str(value: Any) -> str:
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def _sql_bool(value: bool) -> str:
    return "TRUE" if value else "FALSE"


def _to_int(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_float(value: Any) -> float | None:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _roster_id(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _read_audit_seasons(path: Path | None) -> set[SeasonKey]:
    if path is None:
        return set()
    seasons: set[SeasonKey] = set()
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if row.get("status") != "MISMATCH":
                continue
            if (row.get("platform") or "").lower() != "sleeper":
                continue
            db_name = row.get("db_name")
            year = _to_int(row.get("year"))
            if db_name and year:
                seasons.add(SeasonKey(db_name, year))
    return seasons


def _season_filter_sql(seasons: Iterable[SeasonKey]) -> str:
    pairs = sorted(seasons, key=lambda s: (s.db_name, s.year))
    if not pairs:
        return ""
    values = ", ".join(f"({_sql_str(s.db_name)}, {s.year})" for s in pairs)
    return f"AND (ls.db_name, ls.year) IN (VALUES {values})"


def _load_candidate_settings(reader, seasons: set[SeasonKey]) -> list[SeasonSettings]:
    rows = reader.query(
        f"""
        SELECT DISTINCT
            ls.db_name,
            ls.year,
            ls.league_key,
            ls.playoff_start_week,
            ls.end_week,
            ls.playoff_teams,
            ls.sleeper_playoff_type
        FROM public.league_settings ls
        JOIN public.matchup m
          ON m.db_name = ls.db_name
         AND m.year = ls.year
        WHERE lower(coalesce(ls.platform, '')) = 'sleeper'
          AND ls.league_key IS NOT NULL
          AND ls.playoff_start_week IS NOT NULL
          AND m.matchup_id IS NULL
          AND m.opponent_franchise_id IS NOT NULL
          AND lower(trim(coalesce(m.opponent, ''))) NOT IN ('', 'bye', 'none', 'null', 'nan')
          {_season_filter_sql(seasons)}
        ORDER BY ls.db_name, ls.year
        """,
        database=DATABASE,
    )
    settings: list[SeasonSettings] = []
    for row in rows:
        start_week = _to_int(row.get("playoff_start_week"), 0)
        end_week = _to_int(row.get("end_week"), start_week)
        if not start_week or not end_week or end_week < start_week:
            continue
        settings.append(
            SeasonSettings(
                db_name=str(row["db_name"]),
                year=int(row["year"]),
                league_id=str(row["league_key"]),
                playoff_start_week=start_week,
                end_week=end_week,
                playoff_teams=_to_int(row.get("playoff_teams"), 0),
                # Fly stores the canonical repo enum in sleeper_playoff_type:
                # 0=single-week, 1=all two-week, 2=championship-only two-week.
                playoff_round_type=_to_int(row.get("sleeper_playoff_type"), 0),
            )
        )
    return settings


def _api_pairings_by_week(client: SleeperAPIClient, settings: SeasonSettings) -> dict[int, set[frozenset[int]]]:
    winners = client.get_winners_bracket(settings.league_id) or []
    losers = client.get_losers_bracket(settings.league_id) or []
    playoff_rounds = max((_to_int(m.get("r"), 0) for m in [*winners, *losers]), default=1)

    pairings: dict[int, set[frozenset[int]]] = {}
    for week in range(settings.playoff_start_week, settings.end_week + 1):
        round_num = playoff_round_for_week(
            week,
            settings.playoff_start_week,
            settings.playoff_round_type,
            playoff_rounds,
        )
        round_pairs: set[frozenset[int]] = set()
        for matchup in winners:
            if _to_int(matchup.get("r"), 0) != round_num:
                continue
            t1, t2 = bracket_team_ids(matchup)
            if t1 is not None and t2 is not None:
                round_pairs.add(frozenset((t1, t2)))
        for matchup in losers:
            if _to_int(matchup.get("r"), 0) != round_num:
                continue
            t1, t2 = bracket_team_ids(matchup)
            if t1 is not None and t2 is not None:
                round_pairs.add(frozenset((t1, t2)))
        pairings[week] = round_pairs
    return pairings


def _load_postseason_rows(reader, settings: SeasonSettings) -> list[dict[str, Any]]:
    return reader.query(
        f"""
        SELECT
            db_name,
            year,
            week,
            manager,
            franchise_id,
            team_key,
            opponent,
            opponent_franchise_id,
            matchup_id,
            team_points,
            opponent_points,
            win,
            loss,
            tie,
            is_playoffs,
            is_consolation,
            is_championship,
            champion,
            sacko,
            final_playoff_seed
        FROM public.matchup
        WHERE db_name = {_sql_str(settings.db_name)}
          AND year = {settings.year}
          AND week BETWEEN {settings.playoff_start_week} AND {settings.end_week}
        ORDER BY week, team_key, manager
        """,
        database=DATABASE,
    )


def _find_repairs(reader, client: SleeperAPIClient, settings: SeasonSettings) -> list[RepairRow]:
    api_pairs = _api_pairings_by_week(client, settings)
    rows = _load_postseason_rows(reader, settings)
    roster_by_week_franchise: dict[tuple[int, str], int] = {}
    for row in rows:
        franchise_id = row.get("franchise_id")
        roster_id = _roster_id(row.get("team_key"))
        week = _to_int(row.get("week"))
        if franchise_id and roster_id is not None and week:
            roster_by_week_franchise[(week, str(franchise_id))] = roster_id

    repairs: list[RepairRow] = []
    seen: set[tuple[int, str]] = set()
    for row in rows:
        week = _to_int(row.get("week"))
        franchise_id = str(row.get("franchise_id") or "")
        if not week or not franchise_id or (week, franchise_id) in seen:
            continue
        seen.add((week, franchise_id))

        if row.get("matchup_id") is not None:
            continue
        opponent = str(row.get("opponent") or "").strip()
        opponent_fid = str(row.get("opponent_franchise_id") or "").strip()
        if not opponent_fid or opponent.lower() in REAL_OPPONENT_SENTINELS:
            continue
        if _to_int(row.get("is_championship"), 0) or _to_int(row.get("champion"), 0) or _to_int(row.get("sacko"), 0):
            continue

        roster_id = _roster_id(row.get("team_key"))
        opponent_roster_id = roster_by_week_franchise.get((week, opponent_fid))
        if roster_id is None or opponent_roster_id is None:
            continue

        pair = frozenset((roster_id, opponent_roster_id))
        if pair in api_pairs.get(week, set()):
            continue

        final_seed = _to_int(row.get("final_playoff_seed"), 0)
        playoff_bye = bool(settings.playoff_teams and final_seed and final_seed <= settings.playoff_teams)
        repairs.append(
            RepairRow(
                db_name=settings.db_name,
                year=settings.year,
                week=week,
                franchise_id=franchise_id,
                team_key=str(row.get("team_key") or ""),
                old_opponent_franchise_id=opponent_fid,
                old_opponent=opponent,
                old_team_points=_to_float(row.get("team_points")),
                old_opponent_points=_to_float(row.get("opponent_points")),
                is_playoffs=playoff_bye,
                is_consolation=not playoff_bye,
                reason=f"null matchup_id pair {roster_id}-{opponent_roster_id} absent from Sleeper bracket round",
            )
        )
    return repairs


def _update_sql(rows: list[RepairRow]) -> str:
    values = ",\n".join(
        "("
        f"{_sql_str(r.db_name)}, {r.year}, {r.week}, {_sql_str(r.franchise_id)}, "
        f"{_sql_str(r.team_key)}, {_sql_str(r.old_opponent_franchise_id)}, "
        f"{_sql_bool(r.is_playoffs)}, {_sql_bool(r.is_consolation)}, "
        f"{_sql_str(f'{r.franchise_id}__bye__{r.year}__{r.week}')}"
        ")"
        for r in rows
    )
    return f"""
    UPDATE public.matchup AS m
       SET opponent = NULL,
           opponent_guid = NULL,
           opponent_franchise_id = NULL,
           opponent_points = NULL,
           opponent_projected_points = NULL,
           team_points = NULL,
           margin = NULL,
           total_matchup_score = NULL,
           close_margin = NULL,
           win = NULL,
           loss = NULL,
           tie = NULL,
           matchup_key = v.matchup_key,
           is_bye_week = TRUE,
           postseason = TRUE,
           is_playoffs = v.is_playoffs,
           is_consolation = v.is_consolation,
           is_championship = FALSE,
           champion = 0,
           playoff_round = NULL,
           playoff_round_num = NULL,
           playoff_week_index = NULL,
           consolation_round = NULL,
           placement_game = 0,
           league_weekly_mean = NULL,
           league_weekly_median = NULL,
           above_league_median = NULL,
           below_league_median = NULL,
           teams_beat_this_week = NULL,
           opponent_teams_beat_this_week = NULL,
           opp_pts_week_rank = NULL,
           opp_pts_week_pct = NULL
      FROM (
        VALUES
        {values}
      ) AS v(db_name, year, week, franchise_id, team_key, old_opponent_franchise_id, is_playoffs, is_consolation, matchup_key)
     WHERE m.db_name = v.db_name
       AND m.year = v.year
       AND m.week = v.week
       AND m.franchise_id = v.franchise_id
       AND m.team_key = v.team_key
       AND m.matchup_id IS NULL
       AND m.opponent_franchise_id = v.old_opponent_franchise_id
    """


def _apply_repairs(rows: list[RepairRow]) -> int:
    writer = FlyWriter()
    updated = 0
    for start in range(0, len(rows), CHUNK_SIZE):
        chunk = rows[start : start + CHUNK_SIZE]
        result = writer.execute(_update_sql(chunk), database=DATABASE)
        if result and isinstance(result, list):
            first = result[0]
            for key in ("Count", "count", "updated", "affected_rows"):
                if key in first:
                    updated += _to_int(first[key])
                    break
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audit-csv", type=Path, help="Optional ddl_bracket_results_audit CSV to limit scope")
    parser.add_argument("--db", action="append", help="Limit to one or more db_name values")
    parser.add_argument("--year", type=int, action="append", help="Limit to one or more seasons")
    parser.add_argument("--apply", action="store_true", help="Write repairs to Fly. Default is dry-run.")
    args = parser.parse_args()

    _load_env()
    reader = get_reader()
    client = SleeperAPIClient()

    seasons = _read_audit_seasons(args.audit_csv)
    if args.audit_csv is not None and not seasons and not args.db and not args.year:
        print("\nCandidate seasons checked: 0")
        print("Rows to repair: 0")
        return 0
    if args.db:
        dbs = set(args.db)
        seasons = {s for s in seasons if s.db_name in dbs} if seasons else set()
    if args.year:
        years = set(args.year)
        seasons = {s for s in seasons if s.year in years} if seasons else set()
    if args.db and args.year and not seasons:
        seasons = {SeasonKey(db, year) for db in args.db for year in args.year}
    elif args.db and not seasons:
        # Let SQL restrict by db through explicit seasons only when year is supplied;
        # otherwise the candidate query scans all years for the db values below.
        pass

    settings = _load_candidate_settings(reader, seasons)
    if args.db:
        dbs = set(args.db)
        settings = [s for s in settings if s.db_name in dbs]
    if args.year:
        years = set(args.year)
        settings = [s for s in settings if s.year in years]

    all_repairs: list[RepairRow] = []
    for season in settings:
        repairs = _find_repairs(reader, client, season)
        all_repairs.extend(repairs)
        if repairs:
            print(f"{season.db_name} {season.year}: {len(repairs)} stale null-pair rows")
            for repair in repairs[:8]:
                print(
                    "  "
                    f"W{repair.week} {repair.franchise_id} vs {repair.old_opponent_franchise_id} "
                    f"({repair.old_team_points}-{repair.old_opponent_points}) -> BYE"
                )
            if len(repairs) > 8:
                print(f"  ... {len(repairs) - 8} more")

    print(f"\nCandidate seasons checked: {len(settings)}")
    print(f"Rows to repair: {len(all_repairs)}")

    if not all_repairs:
        return 0

    if not args.apply:
        print("Dry-run only. Re-run with --apply to write these repairs.")
        return 0

    updated = _apply_repairs(all_repairs)
    print(f"Applied repair UPDATE statements. Reported updated rows: {updated or 'unknown'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
