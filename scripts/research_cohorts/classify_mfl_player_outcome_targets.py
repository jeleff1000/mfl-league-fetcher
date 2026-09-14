"""Classify canonical MFL player-week outcome gaps from the MFL API."""
from __future__ import annotations

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd

from multi_league.data_fetchers.mfl.mfl_api_client import MFLAPIClient
from multi_league.data_fetchers.mfl.mfl_matchups import last_regular_season_week, rows_for_week
from multi_league.data_fetchers.mfl.mfl_utils import as_list, clean_id, franchise_lookup_from_league, get_any


def selected(db: str, year: int, week: int, shard: int, shards: int) -> bool:
    return int(hashlib.sha256(f"{db}|{year}|{week}".encode()).hexdigest()[:12], 16) % shards == shard


def bracket_franchises(client: MFLAPIClient, league_id: str, year: int) -> set[str]:
    out: set[str] = set()
    payload = client.fetch_playoff_brackets(league_id, year) or {}
    for item in as_list(payload.get("playoffBracket")):
        if not isinstance(item, dict):
            continue
        bid = clean_id(get_any(item, "id"))
        if not bid:
            continue
        detail = client.fetch_playoff_bracket(league_id, year, bid) or {}
        for rnd in as_list(detail.get("playoffRound")):
            if not isinstance(rnd, dict):
                continue
            for game in as_list(rnd.get("playoffGame")):
                if not isinstance(game, dict):
                    continue
                for side in ("home", "away"):
                    obj = game.get(side) if isinstance(game.get(side), dict) else {}
                    fid = clean_id(get_any(obj, "franchise_id", "franchiseId", "id"))
                    if fid:
                        out.add(fid)
    return out


def norm_manager(value: object) -> str:
    return " ".join(str(value or "").strip().lower().split())


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--shard", type=int, required=True)
    ap.add_argument("--shards", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    groups = [g for g in manifest["groups"] if selected(str(g["db_name"]), int(g["year"]), int(g["week"]), args.shard, args.shards)]
    con = duckdb.connect()
    df = con.execute("SELECT * FROM read_parquet(?)", [str(args.targets)]).fetchdf()
    con.close()
    wanted = {(str(g["db_name"]), int(g["year"]), int(g["week"])) for g in groups}
    df = df[df.apply(lambda r: (str(r.db_name), int(r.year), int(r.week)) in wanted, axis=1)]
    client = MFLAPIClient()
    results: list[dict] = []
    season_cache: dict[tuple[str, int], tuple[dict, dict, set[str]]] = {}
    for group in groups:
        db, year, week = str(group["db_name"]), int(group["year"]), int(group["week"])
        source_id = str(group.get("source_id") or "")
        subset = df[(df.db_name.astype(str) == db) & (df.year.astype(int) == year) & (df.week.astype(int) == week)]
        try:
            if not source_id:
                raise RuntimeError("missing_source_id")
            season_key = (source_id, year)
            if season_key not in season_cache:
                league = client.fetch_league(source_id, year) or {}
                lookup = franchise_lookup_from_league(league)
                season_cache[season_key] = (league, lookup, set())
            league, lookup, playoff_team_ids = season_cache[season_key]
            payload = client.fetch_weekly_results(source_id, year, week)
            source_rows = rows_for_week(payload, year=year, week=week, league_id=source_id, seed_league_id=source_id, team_lookup=lookup, last_reg_week=last_regular_season_week(league), champ_pairs=set())
            by_manager = {norm_manager(r.get("manager")): r for r in source_rows}
            if group.get("playoff_start_week") and week >= int(group["playoff_start_week"]) and not playoff_team_ids:
                playoff_team_ids.update(bracket_franchises(client, source_id, year))
            manager_to_fid = {}
            for fid, info in lookup.items():
                manager_to_fid[norm_manager(info.get("owner_name") or info.get("ownerName") or info.get("username") or info.get("name"))] = str(fid)
            for _, target in subset.iterrows():
                manager = norm_manager(target.get("manager"))
                source = by_manager.get(manager)
                if source is not None and source.get("opponent_franchise_id") and source.get("opponent"):
                    status = "confirmed_outcome"
                    row = {**target.to_dict(), "status": status, "source_id": source_id, "source_win": source.get("win"), "source_loss": source.get("loss"), "source_tie": source.get("tie"), "source_team_points": source.get("team_points"), "source_opponent_points": source.get("opponent_points"), "source_is_playoffs": source.get("is_playoffs")}
                elif source is not None:
                    row = {**target.to_dict(), "status": "source_team_row_no_outcome", "source_id": source_id, "source_team_points": source.get("team_points"), "source_opponent_points": source.get("opponent_points")}
                elif group.get("playoff_start_week") and week >= int(group["playoff_start_week"]):
                    fid = manager_to_fid.get(manager)
                    status = "bye_or_bracket_no_opponent" if fid and fid in playoff_team_ids else "eliminated_no_opponent"
                    row = {**target.to_dict(), "status": status, "source_id": source_id}
                else:
                    row = {**target.to_dict(), "status": "regular_week_unmatched", "source_id": source_id}
                results.append(row)
        except Exception as exc:
            results.extend({**target.to_dict(), "status": "source_error", "source_id": source_id, "error": repr(exc)} for _, target in subset.iterrows())
    out = pd.DataFrame(results)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.out, index=False)
    print(json.dumps({"shard": args.shard, "groups": len(groups), "rows": len(out), "statuses": out.status.value_counts().to_dict() if len(out) else {}}, sort_keys=True))


if __name__ == "__main__":
    main()
