"""Classify exact missing player-week outcome targets against Sleeper source data.

This is deliberately a classifier, not a lake writer.  It keeps the target
ordinal as the primary key and distinguishes a real missing source outcome
from a team that has no fantasy matchup (normally an eliminated team after
the playoff bracket begins).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd
import requests


STATUSES = {
    "recovered",
    "no_opponent",
    "player_map_missing",
    "not_on_week_roster",
    "rostered_not_started",
    "source_week_missing",
    "source_error",
}

# Sleeper represents team defenses by the NFL abbreviation in the roster
# payload (for example ``MIA``), while the research lake uses the stable
# franchise ID (``DEF-18``).  This is intentionally year-aware for the few
# franchises whose abbreviation changed during the target period.
DEF_SOURCE_CODES = {
    1: ("DAL",), 2: ("NYG",), 3: ("PHI",), 4: ("WAS",),
    5: ("CHI",), 6: ("DET",), 7: ("GB",), 8: ("MIN",),
    9: ("ATL",), 10: ("CAR",), 11: ("NO",), 12: ("TB",),
    13: ("ARI",), 14: ("LAR",), 15: ("SF",), 16: ("SEA",), 17: ("BUF",),
    18: ("MIA",), 19: ("NE",), 20: ("NYJ",), 21: ("BAL",),
    22: ("CIN",), 23: ("CLE",), 24: ("PIT",), 25: ("HOU",),
    26: ("IND",), 27: ("JAX",), 28: ("TEN",), 29: ("DEN",),
    30: ("KC",), 31: ("LV",), 32: ("LAC",),
}


def _normalise_source_id(value: object) -> str:
    """Normalise numeric Sleeper IDs read from VARCHAR/DOUBLE parquet fields."""
    if value is None:
        return ""
    text = str(value).strip()
    if text.endswith(".0") and text[:-2].isdigit():
        return text[:-2]
    return text


def _def_source_ids(nfl_id: str, year: int) -> list[str]:
    """Return Sleeper roster IDs that can represent one canonical DEF ID."""
    if not str(nfl_id).startswith("DEF-"):
        return []
    try:
        franchise_id = int(str(nfl_id)[4:])
    except ValueError:
        return []
    codes = list(DEF_SOURCE_CODES.get(franchise_id, ()))
    # Historical aliases are harmless candidates and make this robust to
    # source normalization changes around the rename seasons.
    if franchise_id == 14 and year <= 2019:
        codes.extend(("STL",))
    if franchise_id == 31 and year <= 2019:
        codes.extend(("OAK",))
    if franchise_id == 32 and year <= 2019:
        codes.extend(("SD",))
    return list(dict.fromkeys(codes))


def _selected(db_name: str, year: int, shard: int, shards: int) -> bool:
    key = f"{db_name}|{year}".encode()
    return int(hashlib.sha256(key).hexdigest()[:12], 16) % shards == shard


def _get(session: requests.Session, url: str, attempts: int = 4):
    last = None
    for attempt in range(attempts):
        try:
            response = session.get(url, timeout=45)
            if response.status_code == 200:
                return response.json()
            last = f"http_{response.status_code}"
            if response.status_code not in {408, 429, 500, 502, 503, 504}:
                break
        except Exception as exc:  # pragma: no cover - network-dependent
            last = repr(exc)
        time.sleep(min(2**attempt, 8))
    raise RuntimeError(last or "source_request_failed")


def _source_rows(matchups: list[dict]) -> tuple[dict[str, list[dict]], dict[object, list[dict]]]:
    by_player: dict[str, list[dict]] = defaultdict(list)
    by_matchup: dict[object, list[dict]] = defaultdict(list)
    for row in matchups or []:
        mid = row.get("matchup_id")
        if mid is not None:
            by_matchup[mid].append(row)
        for player_id in row.get("players") or []:
            by_player[str(player_id)].append(row)
    return by_player, by_matchup


def classify(args: argparse.Namespace) -> None:
    con = duckdb.connect()
    target_cols = {r[0] for r in con.execute(
        "DESCRIBE SELECT * FROM read_parquet(?)", [args.targets.resolve().as_posix()]
    ).fetchall()}
    required = {"db_name", "year", "week", "NFL_player_id", "target_ordinal"}
    missing = required - target_cols
    if missing:
        raise SystemExit(f"target artifact missing required columns: {sorted(missing)}")
    all_targets = con.execute(
        """
        SELECT CAST(db_name AS VARCHAR) AS db_name,
               CAST(year AS INTEGER) AS season_year,
               CAST(week AS INTEGER) AS season_week,
               CAST(NFL_player_id AS VARCHAR) NFL_player_id,
               CAST(target_ordinal AS BIGINT) target_ordinal
        FROM read_parquet(?)
        ORDER BY target_ordinal
        """,
        [args.targets.resolve().as_posix()],
    ).fetchall()
    # Keep every week for a league-season on one worker.  Splitting by target
    # ordinal would fetch the same source league/week repeatedly and would make
    # the classification both slow and unnecessarily rate-limit-sensitive.
    targets = [row for row in all_targets if _selected(row[0], row[1], args.shard, args.shards)]
    if not targets:
        raise SystemExit(f"empty target shard {args.shard}/{args.shards}")

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    manifest_rows = manifest.get("targets", manifest) if isinstance(manifest, dict) else manifest
    source_ids = {(str(r.get("db_name")), int(r.get("year", 0))): str(r.get("source_id") or "")
                  for r in manifest_rows}
    map_rows = con.execute(
        """
        SELECT CAST(sleeper_player_id AS VARCHAR), CAST(NFL_player_id AS VARCHAR)
        FROM read_parquet(?)
        WHERE sleeper_player_id IS NOT NULL AND NFL_player_id IS NOT NULL
        """, [args.player_map.resolve().as_posix()]
    ).fetchall()
    nfl_to_sleeper: dict[str, list[str]] = defaultdict(list)
    for sleeper_id, nfl_id in map_rows:
        source_id = _normalise_source_id(sleeper_id)
        if source_id and source_id not in nfl_to_sleeper[str(nfl_id)]:
            nfl_to_sleeper[str(nfl_id)].append(source_id)

    # The historical bio table contains source IDs for some players whose
    # rows are absent from, or collide with an older identity in, the main
    # crosswalk.  Use it only as a fallback for an NFL ID that has no direct
    # crosswalk entry; never replace a confirmed direct mapping.
    bio_rows = con.execute(
        """
        SELECT CAST(NFL_player_id AS VARCHAR), CAST(sleeper_player_id AS VARCHAR)
        FROM read_parquet(?)
        WHERE sleeper_player_id IS NOT NULL AND NFL_player_id IS NOT NULL
        """, [args.player_bio.resolve().as_posix()]
    ).fetchall()
    for nfl_id, sleeper_id in bio_rows:
        source_id = _normalise_source_id(sleeper_id)
        if source_id and not nfl_to_sleeper[str(nfl_id)]:
            nfl_to_sleeper[str(nfl_id)].append(source_id)

    overrides = json.loads(args.identity_overrides.read_text(encoding="utf-8"))
    for nfl_id, sleeper_id in overrides.items():
        source_id = _normalise_source_id(sleeper_id)
        # Explicit research overrides are authoritative: they correct stale
        # or swapped rows in the broad crosswalk, not only missing mappings.
        if source_id:
            nfl_to_sleeper[str(nfl_id)] = [source_id]

    grouped: dict[tuple[str, int], list[tuple]] = defaultdict(list)
    for row in targets:
        grouped[(row[0], row[1])].append(row)
    session = requests.Session()
    output: list[dict] = []
    for (db_name, year), rows in sorted(grouped.items()):
        source_id = source_ids.get((db_name, year), "")
        if not source_id:
            for db, yr, week, nfl, ordinal in rows:
                output.append({"target_ordinal": ordinal, "db_name": db, "year": yr,
                               "week": week, "NFL_player_id": nfl,
                               "status": "source_error", "source_error": "missing_source_id"})
            continue
        try:
            league = _get(session, f"https://api.sleeper.app/v1/league/{source_id}") or {}
            settings = league.get("settings") or {}
            weeks = sorted({week for _, _, week, _, _ in rows})
            week_cache = {}
            for week in weeks:
                week_cache[week] = _get(
                    session, f"https://api.sleeper.app/v1/league/{source_id}/matchups/{week}"
                )
            for db, yr, week, nfl, ordinal in rows:
                matchups = week_cache.get(week) or []
                if not matchups:
                    output.append({"target_ordinal": ordinal, "db_name": db, "year": yr,
                                   "week": week, "NFL_player_id": nfl,
                                   "status": "source_week_missing", "source_id": source_id})
                    continue
                by_player, by_matchup = _source_rows(matchups)
                source_ids_for_player = list(nfl_to_sleeper.get(nfl, []))
                source_ids_for_player.extend(_def_source_ids(nfl, year))
                source_ids_for_player = list(dict.fromkeys(source_ids_for_player))
                candidates = [r for sid in source_ids_for_player for r in by_player.get(sid, [])]
                if not source_ids_for_player:
                    status = "player_map_missing"
                    chosen = None
                elif not candidates:
                    status = "not_on_week_roster"
                    chosen = None
                else:
                    started = [r for r in candidates if str(r.get("roster_id")) and
                               str(next((sid for sid in source_ids_for_player
                                         if sid in (r.get("players") or [])), "")) in
                               {str(x) for x in (r.get("starters") or [])}]
                    chosen = started[0] if started else candidates[0]
                    if not started:
                        status = "rostered_not_started"
                    else:
                        mid = chosen.get("matchup_id")
                        opponents = [r for r in by_matchup.get(mid, [])
                                     if r.get("roster_id") != chosen.get("roster_id")]
                        if not opponents:
                            status = "no_opponent"
                        else:
                            status = "recovered"
                result = {"target_ordinal": ordinal, "db_name": db, "year": yr,
                          "week": week, "NFL_player_id": nfl, "status": status,
                          "source_id": source_id,
                          "total_rosters": league.get("total_rosters"),
                          "playoff_teams": settings.get("playoff_teams"),
                          "playoff_start_week": settings.get("playoff_week_start")}
                if chosen is not None:
                    result.update({
                        "source_roster_id": chosen.get("roster_id"),
                        "source_matchup_id": chosen.get("matchup_id"),
                        "source_team_points": chosen.get("points"),
                    })
                    mid = chosen.get("matchup_id")
                    opponents = [r for r in by_matchup.get(mid, [])
                                 if r.get("roster_id") != chosen.get("roster_id")]
                    if opponents:
                        opp = opponents[0]
                        result.update({
                            "source_opponent_roster_id": opp.get("roster_id"),
                            "source_opponent_points": opp.get("points"),
                            "win": int((chosen.get("points") or 0) > (opp.get("points") or 0)),
                            "loss": int((chosen.get("points") or 0) < (opp.get("points") or 0)),
                            "tie": int((chosen.get("points") or 0) == (opp.get("points") or 0)),
                        })
                output.append(result)
        except Exception as exc:  # retain one explicit result per target
            for db, yr, week, nfl, ordinal in rows:
                output.append({"target_ordinal": ordinal, "db_name": db, "year": yr,
                               "week": week, "NFL_player_id": nfl,
                               "status": "source_error", "source_id": source_id,
                               "source_error": repr(exc)})

    frame = pd.DataFrame(output)
    if len(frame) != len(targets):
        raise SystemExit(f"classification cardinality mismatch: targets={len(targets)} output={len(frame)}")
    if frame.target_ordinal.duplicated().any():
        raise SystemExit("duplicate target ordinals in classification output")
    bad = set(frame.status) - STATUSES
    if bad:
        raise SystemExit(f"unexpected statuses: {sorted(bad)}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    frame.sort_values("target_ordinal").to_parquet(args.out, index=False)
    summary = frame.status.value_counts().to_dict()
    args.out.with_suffix(".json").write_text(json.dumps({
        "shard": args.shard, "shards": args.shards, "target_rows": len(targets),
        "statuses": summary,
    }, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"shard": args.shard, "target_rows": len(targets), "statuses": summary}, sort_keys=True))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--player-map", type=Path, required=True)
    ap.add_argument("--player-bio", type=Path, required=True)
    ap.add_argument("--identity-overrides", type=Path, required=True)
    ap.add_argument("--shard", type=int, required=True)
    ap.add_argument("--shards", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    classify(ap.parse_args())


if __name__ == "__main__":
    main()
