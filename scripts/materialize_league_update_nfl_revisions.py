"""Persist compact NFL week/game revisions after the live ops refresh."""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import os
import re
import urllib.request
from collections import defaultdict
from collections.abc import Callable
from typing import Any

from multi_league.core.league_update_manifest import (
    MANIFEST_SCHEMA_VERSION,
    NFL_CANONICAL_IDENTITY_COLUMNS,
    NFL_SCORING_INPUT_COLUMNS,
    _canonical_team,
    build_nfl_revision_rows,
    resolve_nfl_scoring_input_columns,
)


OPS_DATABASE = "___ops"
DEFAULT_SOURCE = "nfl_historical.nfl_player_stats_all"
DEFAULT_TARGET = "accounts.league_update_nfl_revisions"
_RELATION_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$")
_PLAYER_RELEASE = "https://github.com/nflverse/nflverse-data/releases/download/stats_player/stats_player_week_{year}.csv.gz"
_SCHEDULE_RELEASE = "https://github.com/nflverse/nflverse-data/releases/download/schedules/games.csv.gz"


def _relation(value: str) -> str:
    if not _RELATION_RE.fullmatch(value):
        raise ValueError(f"invalid DuckDB relation: {value!r}")
    return value


def _literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _fetch_release_bytes(url: str) -> bytes:
    with urllib.request.urlopen(url, timeout=30) as response:
        return response.read()


def _game_key(team: object, opponent: object) -> str:
    left, right = _canonical_team(team), _canonical_team(opponent)
    if not left or not right or left == right:
        raise RuntimeError("NFL game has an invalid reciprocal team identity")
    return "@".join(sorted((left, right)))


def load_nflverse_week_witness(
    year: int,
    week: int,
    *,
    fetch_bytes: Callable[[str], bytes] = _fetch_release_bytes,
) -> dict[str, set[tuple[str, str]]]:
    """Read two small official release assets, not an NFL lake or browser hash."""
    def release_rows(url: str) -> list[dict[str, str]]:
        payload = gzip.decompress(fetch_bytes(url)).decode("utf-8")
        return list(csv.DictReader(io.StringIO(payload)))

    schedules = release_rows(_SCHEDULE_RELEASE)
    players = release_rows(_PLAYER_RELEASE.format(year=int(year)))
    final_ids: dict[str, str] = {}
    for row in schedules:
        if (
            str(row.get("season")) != str(int(year))
            or str(row.get("week")) != str(int(week))
            or row.get("game_type") != "REG"
        ):
            continue
        try:
            float(row.get("away_score") or "")
            float(row.get("home_score") or "")
        except (TypeError, ValueError):
            continue
        game_id = str(row.get("game_id") or "").strip()
        if not game_id:
            raise RuntimeError("NFL game final schedule row has no game ID")
        key = _game_key(row.get("away_team"), row.get("home_team"))
        if key in final_ids:
            raise RuntimeError(f"NFL game {key} has duplicate final schedule rows")
        final_ids[key] = game_id
    if not final_ids:
        raise RuntimeError(f"no finalized NFL games for {year} week {week}")

    witnessed: dict[str, set[tuple[str, str]]] = defaultdict(set)
    for row in players:
        if (
            str(row.get("season")) != str(int(year))
            or str(row.get("week")) != str(int(week))
            or row.get("season_type") != "REG"
        ):
            continue
        key = _game_key(row.get("team"), row.get("opponent_team"))
        if key not in final_ids:
            continue
        if str(row.get("game_id") or "") != final_ids[key]:
            raise RuntimeError(f"NFL game {key} player release disagrees with final schedule")
        player_id = str(row.get("player_id") or "").strip()
        if not player_id:
            if not str(row.get("player_name") or "").strip() and not str(row.get("position") or "").strip():
                # NFLverse can include anonymous team-level penalty rows; no
                # player key exists to map into the canonical scoring table.
                continue
            raise RuntimeError(f"NFL game {key} player release has a blank ID")
        witnessed[key].add((player_id, _canonical_team(row.get("team"))))
    for key in final_ids:
        teams = set(key.split("@"))
        released_teams = {team for _, team in witnessed.get(key, set())}
        if released_teams != teams:
            raise RuntimeError(f"NFL game {key} incomplete: official player identities lack a scored side")
    return dict(witnessed)


def _assert_complete_revision_games(
    rows: list[dict[str, Any]],
    witness: dict[str, set[tuple[str, str]]],
) -> None:
    """A final game needs every official player and both precomputed DST rows."""
    if not witness:
        raise RuntimeError("NFL game witness has no finalized games")
    actual: dict[str, set[tuple[str, str]]] = defaultdict(set)
    defenses: dict[str, set[str]] = defaultdict(set)
    seen: set[tuple[str, str, str, str]] = set()
    for row in rows:
        key = _game_key(row.get("nfl_team"), row.get("opponent_nfl_team"))
        player_id = str(row.get("NFL_player_id") or "").strip()
        team = _canonical_team(row.get("nfl_team"))
        identity = (key, player_id, team, str(row.get("game_date") or ""))
        if not player_id or identity in seen:
            raise RuntimeError(f"NFL game {key} incomplete: blank or duplicate canonical player identity")
        seen.add(identity)
        actual[key].add((player_id, team))
        if str(row.get("position") or "").upper() == "DEF":
            defenses[key].add(team)
    if set(actual) != set(witness):
        raise RuntimeError("NFL game incomplete: canonical and finalized game sets differ")
    for key, official_players in witness.items():
        sides = set(key.split("@"))
        if not official_players.issubset(actual[key]):
            raise RuntimeError(f"NFL game {key} incomplete: official player keys are missing")
        if defenses[key] != sides:
            raise RuntimeError(f"NFL game {key} incomplete: precomputed defense rows are missing")


def materialize_nfl_revisions(
    reader: Any,
    writer: Any,
    *,
    year: int,
    week: int,
    source_relation: str = DEFAULT_SOURCE,
    target_relation: str = DEFAULT_TARGET,
    official_week_witness: Callable[[int, int], dict[str, set[tuple[str, str]]]] = load_nflverse_week_witness,
) -> dict[str, int]:
    """Replace one compact revision scope only after source rows are present."""
    source = _relation(source_relation)
    target = _relation(target_relation)
    source_schema, source_table = source.split(".", 1)
    schema_rows = reader.query(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_schema = {_literal(source_schema)} AND table_name = {_literal(source_table)}",
        database=OPS_DATABASE,
    )
    scoring_columns = resolve_nfl_scoring_input_columns(
        str(row["column_name"]) for row in schema_rows
    )
    if not scoring_columns:
        raise RuntimeError(f"no registered NFL scoring columns exist in {source}")
    if "position" not in {str(row["column_name"]) for row in schema_rows}:
        raise RuntimeError("NFL game completeness requires precomputed defense positions")
    selected = (
        *NFL_CANONICAL_IDENTITY_COLUMNS,
        "year",
        "position",
        *scoring_columns,
    )
    columns = ", ".join(f'"{column}"' for column in dict.fromkeys(selected))
    rows = reader.query(
        f"""
        SELECT {columns}
        FROM {source}
        WHERE TRY_CAST(year AS INTEGER) = {int(year)}
          AND TRY_CAST(week AS INTEGER) = {int(week)}
          AND UPPER(COALESCE(season_type, 'REG')) = 'REG'
          AND NFL_player_id IS NOT NULL
          AND nfl_team IS NOT NULL
          AND opponent_nfl_team IS NOT NULL
        """,
        database=OPS_DATABASE,
    )
    if not rows:
        raise RuntimeError(f"no NFL source rows for {year} week {week}; refusing to replace revisions")
    _assert_complete_revision_games(rows, official_week_witness(int(year), int(week)))

    revisions = build_nfl_revision_rows(
        rows,
        schema_version=MANIFEST_SCHEMA_VERSION,
        scoring_columns=scoring_columns,
    )
    if not revisions:
        raise RuntimeError(f"no complete NFL games for {year} week {week}; refusing to replace revisions")
    values = ",\n".join(
        "(" + ", ".join(
            (
                str(int(row["season"])),
                str(int(row["week"])),
                _literal(str(row["game_key"])),
                _literal(str(row["revision"])),
                str(int(row["schema_version"])),
                "NOW()",
            )
        ) + ")"
        for row in revisions
    )
    writer.execute(
        f"""
        BEGIN TRANSACTION;
        CREATE SCHEMA IF NOT EXISTS accounts;
        CREATE TABLE IF NOT EXISTS {target} (
            season INTEGER NOT NULL,
            week INTEGER NOT NULL,
            game_key VARCHAR NOT NULL,
            revision VARCHAR NOT NULL,
            schema_version INTEGER NOT NULL,
            refreshed_at TIMESTAMP NOT NULL,
            PRIMARY KEY (season, week, game_key)
        );
        DELETE FROM {target} WHERE season = {int(year)} AND week = {int(week)};
        INSERT INTO {target}
            (season, week, game_key, revision, schema_version, refreshed_at)
        VALUES {values};
        COMMIT;
        """,
        database=OPS_DATABASE,
    )
    return {"season": int(year), "week": int(week), "games": len(revisions)}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--week", type=int, required=True)
    args = parser.parse_args(argv)

    os.environ["DATABASE_BACKEND"] = "fly"
    from multi_league.core.fly_writer import FlyWriter
    from multi_league.core.readers.fly_reader import FlyReader

    receipt = materialize_nfl_revisions(FlyReader(), FlyWriter(), year=args.year, week=args.week)
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
