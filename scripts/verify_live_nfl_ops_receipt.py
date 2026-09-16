#!/usr/bin/env python3
"""Verify a promoted live NFL Ops scope through Fly's read path only.

The builder's local artifact receipt proves what was packaged.  This separate
read-only verifier proves that every final game in that scope and each live
aggregate relation can be read from Fly after the atomic promotion.
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


class FlyReceiptError(RuntimeError):
    """The read path does not prove the promoted NFL scope is live."""


YEAR_SCOPED_AGGREGATES = (
    "player_nfl_season",
    "player_nfl_season_all",
    "player_nfl_season_team",
    "player_nfl_season_team_all",
)
ALL_AGGREGATES = (
    *YEAR_SCOPED_AGGREGATES,
    "player_nfl_career",
    "player_nfl_career_all",
    "player_bio",
)


def _normalized_scope(scope: dict[str, Any]) -> dict[str, Any]:
    try:
        year = int(scope["year"])
        week = int(scope["week"])
        season_type = str(scope["season_type"]).strip().upper()
        raw_game_date = scope.get("game_date")
        game_date = None if raw_game_date in (None, "") else date.fromisoformat(str(raw_game_date)).isoformat()
        game_ids = sorted({str(value).strip() for value in scope.get("game_ids", ()) if str(value).strip()})
    except (KeyError, TypeError, ValueError) as error:
        raise FlyReceiptError(f"invalid finalized refresh scope: {error}") from error
    if season_type not in {"REG", "POST"}:
        raise FlyReceiptError(f"invalid finalized refresh season type: {season_type!r}")
    return {
        "year": year,
        "week": week,
        "season_type": season_type,
        "game_date": game_date,
        "game_ids": game_ids,
    }


def _weekly_filter(scope: dict[str, Any], *, alias: str = "") -> str:
    prefix = f"{alias}." if alias else ""
    predicates = [
        f"{prefix}year = {scope['year']}",
        f"{prefix}week = {scope['week']}",
        f"{prefix}season_type = '{scope['season_type']}'",
    ]
    if scope["game_date"] is not None:
        predicates.append(f"{prefix}game_date = '{scope['game_date']}'")
    return " AND ".join(predicates)


def _game_identity_sql(*, alias: str = "") -> str:
    """Return the canonical weekly game identity carried by the live artifact.

    The artifact deliberately keeps team/opponent/date facts, not an upstream
    provider game-id column.  A date plus an unordered team pair is therefore
    the stable game-level identity available on both the candidate and Fly.
    """
    prefix = f"{alias}." if alias else ""
    return (
        "CASE "
        f"WHEN {prefix}game_date IS NULL "
        f"OR NULLIF(TRIM({prefix}nfl_team), '') IS NULL "
        f"OR NULLIF(TRIM({prefix}opponent_nfl_team), '') IS NULL "
        "THEN NULL "
        f"ELSE CAST({prefix}game_date AS VARCHAR) || ':' || "
        f"LEAST(TRIM({prefix}nfl_team), TRIM({prefix}opponent_nfl_team)) || ':' || "
        f"GREATEST(TRIM({prefix}nfl_team), TRIM({prefix}opponent_nfl_team)) END"
    )


def _candidate_game_rows(candidate: Path, scope: dict[str, Any]) -> dict[str, int]:
    """Read the exact promoted game set from the verified candidate artifact."""
    if not candidate.is_file():
        raise FlyReceiptError(f"candidate artifact does not exist: {candidate}")
    import duckdb

    with duckdb.connect(str(candidate), read_only=True) as connection:
        rows = connection.execute(
            f'''\
            SELECT {_game_identity_sql()} AS game_key, COUNT(*) AS rows
            FROM nfl_historical."nfl_player_stats_all"
            WHERE {_weekly_filter(scope)}
            GROUP BY game_key
            ORDER BY game_key
            '''
        ).fetchall()
    if any(not str(game_key or "").strip() for game_key, _row_count in rows):
        raise FlyReceiptError("candidate artifact has rows without a canonical game identity")
    game_rows = {
        str(game_id).strip(): int(row_count or 0)
        for game_id, row_count in rows
        if str(game_id or "").strip()
    }
    if not game_rows or any(rows <= 0 for rows in game_rows.values()):
        raise FlyReceiptError("candidate artifact has no complete game rows for the finalized scope")
    return game_rows


def _read_single(reader: Any, sql: str) -> dict[str, Any]:
    rows = reader.query(sql, database="___ops")
    if len(rows) != 1:
        raise FlyReceiptError(f"receipt query returned {len(rows)} rows instead of one")
    return rows[0]


def _aggregate_coverage_sql(table: str, scope: dict[str, Any]) -> str:
    year_predicate = f"AND aggregate.year = {scope['year']}" if table in YEAR_SCOPED_AGGREGATES else ""
    return f"""
        WITH refreshed AS (
          SELECT DISTINCT NFL_player_id
          FROM nfl_historical."nfl_player_stats_all"
          WHERE {_weekly_filter(scope)}
        )
        SELECT
          COUNT(DISTINCT refreshed.NFL_player_id) AS refreshed_players,
          COUNT(DISTINCT aggregate.NFL_player_id) AS covered_players
        FROM nfl_historical."{table}" AS aggregate
        RIGHT JOIN refreshed
          ON aggregate.NFL_player_id = refreshed.NFL_player_id
          {year_predicate}
    """


def collect_fly_receipt(
    reader: Any,
    scope: dict[str, Any],
    *,
    expected_game_rows: dict[str, int] | None = None,
) -> dict[str, Any]:
    """Return a fail-closed Fly receipt for one finalized NFL date scope."""
    normalized = _normalized_scope(scope)
    weekly_rows = reader.query(
        f"""
        SELECT {_game_identity_sql()} AS game_key, COUNT(*) AS rows
        FROM nfl_historical."nfl_player_stats_all"
        WHERE {_weekly_filter(normalized)}
        GROUP BY game_key
        ORDER BY game_key
        """,
        database="___ops",
    )
    if any(not str(row.get("game_key", "")).strip() for row in weekly_rows):
        raise FlyReceiptError("Fly weekly receipt has rows without a canonical game identity")
    observed_game_rows = {
        str(row.get("game_key", "")).strip(): int(row.get("rows") or 0)
        for row in weekly_rows
        if str(row.get("game_key", "")).strip()
    }
    expected_rows = expected_game_rows or {}
    if not expected_rows:
        raise FlyReceiptError("verification needs exact candidate game identities")
    if set(observed_game_rows) != set(expected_rows):
        raise FlyReceiptError(
            "Fly weekly game identities do not exactly match the verified candidate: "
            f"expected={sorted(expected_rows)}, observed={sorted(observed_game_rows)}"
        )
    if any(rows <= 0 for rows in observed_game_rows.values()):
        raise FlyReceiptError("Fly weekly receipt includes a finalized game with zero player rows")
    if any(
        expected_rows[game_id] is not None and observed_game_rows[game_id] != expected_rows[game_id]
        for game_id in expected_rows
    ):
        raise FlyReceiptError("Fly weekly game row counts do not match the verified candidate artifact")
    aggregates: dict[str, dict[str, int]] = {}
    for table in ALL_AGGREGATES:
        coverage = _read_single(reader, _aggregate_coverage_sql(table, normalized))
        refreshed_players = int(coverage.get("refreshed_players") or 0)
        covered_players = int(coverage.get("covered_players") or 0)
        if refreshed_players <= 0 or covered_players != refreshed_players:
            raise FlyReceiptError(
                f"Fly aggregate {table} is missing refreshed players: "
                f"refreshed={refreshed_players}, covered={covered_players}"
            )
        aggregates[table] = {
            "refreshed_players": refreshed_players,
            "covered_players": covered_players,
        }

    return {
        "scope": normalized,
        "weekly": {
            "game_keys": sorted(observed_game_rows),
            "rows": sum(observed_game_rows.values()),
        },
        "aggregates": aggregates,
        "verified_at": datetime.now(timezone.utc).isoformat(),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scope", type=Path, required=True, help="ready refresh_scope.json emitted by discovery")
    parser.add_argument("--candidate", type=Path, required=True, help="verified complete candidate DuckDB artifact")
    parser.add_argument("--output", type=Path, required=True, help="Fly read-path JSON receipt")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    payload = json.loads(args.scope.read_text(encoding="utf-8"))
    if payload.get("status") != "ready" or not isinstance(payload.get("scope"), dict):
        raise FlyReceiptError("cannot verify Fly promotion without a ready refresh scope")

    from multi_league.core.readers.fly_reader import FlyReader

    normalized = _normalized_scope(payload["scope"])
    expected_game_rows = _candidate_game_rows(args.candidate, normalized)
    receipt = collect_fly_receipt(FlyReader(), normalized, expected_game_rows=expected_game_rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(receipt, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover - exercised by GitHub Actions
    raise SystemExit(main())
