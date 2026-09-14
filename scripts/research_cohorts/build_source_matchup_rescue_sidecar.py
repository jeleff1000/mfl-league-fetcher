"""Fetch canonical source matchup rows for targeted missing-signal league-years.

This is a small, API-only rescue lane.  It deliberately receives an explicit
target manifest containing the per-season native source ID; it never guesses a
MFL season from the folded DB name.  The output is a sidecar, not a direct lake
write, and is safe to inspect/merge only after the per-target status gate passes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

from multi_league.data_fetchers.fleaflicker.fleaflicker_context import FleaflickerContext
from multi_league.data_fetchers.fleaflicker.fleaflicker_matchups import fetch_fleaflicker_matchups
from multi_league.data_fetchers.mfl.mfl_context import MFLContext
from multi_league.data_fetchers.mfl.mfl_matchups import championship_bracket_pairs, fetch_mfl_matchups
from multi_league.data_fetchers.mfl.mfl_utils import clean_id, franchise_identity, franchise_lookup_from_league, get_any, value_of
from multi_league.data_fetchers.mfl.mfl_utils import as_list
from multi_league.data_fetchers.sleeper.sleeper_context import SleeperContext
from multi_league.data_fetchers.sleeper.sleeper_matchups import fetch_sleeper_matchups

LOG = logging.getLogger(__name__)

_INT_COLUMNS = {
    "week", "year", "target_year", "matchup_id", "win", "loss", "tie",
    "is_playoffs", "is_consolation", "is_championship", "champion",
    "playoff_seed", "final_playoff_seed", "waiver_rank", "teams_beat_this_week",
    "above_league_median",
}
_FLOAT_COLUMNS = {
    "team_points", "opponent_points", "margin", "total_matchup_score",
    "team_projected_points", "opponent_projected_points", "league_weekly_mean",
}


def selected(row: dict, shard: int, shards: int) -> bool:
    key = f"{row['db_name']}|{row['year']}".encode()
    return int(hashlib.sha256(key).hexdigest()[:12], 16) % shards == shard


def resolve_mfl_historical_id(client, seed_id: str, seed_year: int, target_year: int) -> str | None:
    """Resolve the target-season MFL ID through the seed league's history.

    MFL IDs are season-scoped.  A folded DB name such as
    ``smpl_mfl_2015_11490`` identifies a seed league, not the 2003 league ID.
    Never send the seed ID to the target season endpoint.
    """
    league = client.fetch_league(seed_id, seed_year) or {}
    history = as_list((league.get("history") or {}).get("league"))
    for entry in history:
        if not isinstance(entry, dict) or str(entry.get("year")) != str(target_year):
            continue
        match = re.search(r"/(\d{4})/home/([^/?]+)", str(entry.get("url") or ""))
        if match and match.group(1) == str(target_year):
            return match.group(2)
    return None


def resolve_mfl_target_id(client, source_id: str, db_name: str, target_year: int) -> str | None:
    """Use a target-year ID when the manifest already has one; otherwise resolve history."""
    try:
        if client.fetch_league(source_id, target_year):
            return source_id
    except Exception:
        pass
    seed_match = re.fullmatch(r"smpl_mfl_(\d{4})_(\d+)", db_name)
    if not seed_match:
        return None
    try:
        return resolve_mfl_historical_id(
            client, source_id, int(seed_match.group(1)), target_year
        )
    except Exception:
        # A stale settings league_key can be invalid even for the seed year.
        # Continue to the folded-ID candidate instead of losing the target.
        return None


def mfl_target_candidates(client, source_id: str, db_name: str, target_year: int) -> list[str]:
    """Return deterministic target-season ID candidates without guessing a result.

    Most rows use the settings league_key as a seed whose history resolves to
    the target-season ID.  A small MFL subset has a valid seed-derived ID that
    returns an empty weeklyResults payload while the folded db-name ID is the
    target-season league.  Both candidates are therefore tested; provenance
    is retained by the caller.
    """
    candidates: list[str] = []
    resolved = resolve_mfl_target_id(client, source_id, db_name, target_year)
    if resolved:
        candidates.append(str(resolved))
    seed_match = re.fullmatch(r"smpl_mfl_(\d{4})_(\d+)", db_name)
    if seed_match:
        folded_id = seed_match.group(2)
        if folded_id not in candidates:
            try:
                if client.fetch_league(folded_id, target_year):
                    candidates.append(folded_id)
            except Exception:
                pass
    return candidates


def mfl_no_opponent_weeks(client, league_id: str, year: int, weeks: set[int]) -> list[int]:
    """Identify source weeks with rosters but no matchup/opponent records.

    MFL survivor/elimination weeks are represented by a ``franchise`` list
    without a ``matchup`` array.  They are valid source data, but cannot yield
    a win/loss outcome.  Treating them as API failures would make a correct
    whole-week rescue fail closed for the wrong reason.
    """
    no_opponent: list[int] = []
    for week in sorted(weeks):
        payload = client.fetch_weekly_results(league_id, year, week) or {}
        if as_list(payload.get("franchise")) and not as_list(payload.get("matchup")):
            no_opponent.append(int(week))
    return no_opponent


def fleaflicker_no_game_weeks(client, league_id: str, year: int, weeks: set[int]) -> list[int]:
    """Identify Fleaflicker periods with no source games at all."""
    no_games: list[int] = []
    for week in sorted(weeks):
        payload = client.fetch_scoreboard(league_id, season=year, scoring_period=week) or {}
        if not as_list(payload.get("games")):
            no_games.append(int(week))
    return no_games


def mfl_bracket_signal_rows(client, league_id: str, seed_league_id: str, year: int, weeks: set[int], league: dict) -> pd.DataFrame:
    """Return team-week playoff/title signals when MFL has no matchup pairs.

    Survivor/elimination and some old MFL seasons expose bracket games through
    playoffBracket but expose only a flat franchise list in weeklyResults.  The
    normal matchup fetcher intentionally emits no rows for that shape.  The
    bracket endpoint is still sufficient for the player-table signals we need.
    """
    brackets = client.fetch_playoff_brackets(league_id, year) or {}
    items = [x for x in as_list(brackets.get("playoffBracket")) if isinstance(x, dict)]
    if not items:
        return pd.DataFrame()
    def num(value):
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return 99
    champ = next((x for x in sorted(items, key=lambda x: num(get_any(x, "id")))
                  if "champ" in f"{get_any(x, 'name') or ''} {get_any(x, 'bracketWinnerTitle') or ''}".lower()), None)
    if champ is None:
        champ = sorted(items, key=lambda x: num(get_any(x, "id")))[0]
    champ_id = clean_id(get_any(champ, "id"))
    lookup = franchise_lookup_from_league(league)
    rows = []
    for bracket in items:
        bid = clean_id(get_any(bracket, "id"))
        if not bid:
            continue
        detail = client.fetch_playoff_bracket(league_id, year, bid) or {}
        for rnd in as_list(detail.get("playoffRound")):
            if not isinstance(rnd, dict):
                continue
            week = int(rnd.get("week") or 0)
            if week not in weeks:
                continue
            for game in as_list(rnd.get("playoffGame")):
                if not isinstance(game, dict):
                    continue
                sides = []
                for side_name in ("home", "away"):
                    side = game.get(side_name) if isinstance(game.get(side_name), dict) else {}
                    fid = clean_id(get_any(side, "franchise_id", "franchiseId", "id"))
                    if fid:
                        sides.append((fid, side))
                if len(sides) != 2:
                    continue
                fids = frozenset(fid for fid, _ in sides)
                is_champ = bid == champ_id and len(fids) == 2
                scores = [value_of(side.get("score")) for _, side in sides]
                winner = None
                if all(score is not None for score in scores) and scores[0] != scores[1]:
                    winner = sides[0][0] if scores[0] > scores[1] else sides[1][0]
                for fid, side in sides:
                    team = franchise_identity({"id": fid}, seed_league_id, lookup)
                    rows.append({
                        "year": year, "week": week, "manager": team["manager"],
                        "manager_guid": team["manager_guid"], "franchise_id": team["franchise_id"],
                        "manager_week": f"{team['franchise_id']}_{year}_{week}" if team["franchise_id"] else None,
                        "team_key": team["team_key"], "team_name": team["team_name"],
                        "league_id": str(league_id), "platform": "mfl",
                        "is_playoffs": 1, "is_championship": int(is_champ),
                        "champion": int(is_champ and winner == fid),
                        "team_points": value_of(side.get("score")),
                    })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).drop_duplicates(subset=["year", "week", "franchise_id"])


def fetch_one(row: dict) -> tuple[dict, pd.DataFrame | None]:
    platform = str(row["platform"]).lower()
    db_name = str(row["db_name"])
    year = int(row["year"])
    source_id = str(row.get("source_id") or "").strip()
    if not source_id:
        return {**row, "status": "missing_source_id", "rows": 0}, None
    try:
        if platform == "mfl":
            # The manifest source_id is the seed ID. Resolve the exact target
            # season before constructing the context; MFL rejects the seed ID
            # when queried in a different season.
            from multi_league.data_fetchers.mfl.mfl_api_client import MFLAPIClient

            resolver_client = MFLAPIClient()
            candidate_ids = mfl_target_candidates(resolver_client, source_id, db_name, year)
            if not candidate_ids:
                return {
                    **row,
                    "status": "missing_historical_source_id",
                    "rows": 0,
                    "source_seed_id": source_id,
                }, None
            target_weeks = {int(w) for w in (row.get("weeks") or [])}
            frame = None
            selected_id = None
            candidate_errors = []
            for candidate_id in candidate_ids:
                try:
                    ctx = MFLContext(
                        league_id=source_id,
                        league_name=db_name,
                        start_year=year,
                        end_year=year,
                        league_ids={str(year): candidate_id},
                        data_directory=Path(".source_context") / db_name,
                    )
                    candidate_frame = fetch_mfl_matchups(
                        ctx,
                        year,
                        client=resolver_client,
                        weeks=sorted(target_weeks) if target_weeks else None,
                    )
                    selected_id = candidate_id
                    if candidate_frame is not None and not candidate_frame.empty:
                        frame = candidate_frame
                        break
                except Exception as exc:
                    candidate_errors.append({"source_id": candidate_id, "error": repr(exc)})
            if frame is None or frame.empty:
                # A missing matchup array is not necessarily a missing playoff
                # signal.  MFL's bracket endpoint can still identify playoff and
                # title-game starters, especially in survivor/elimination formats.
                if platform == "mfl" and selected_id and target_weeks:
                    signal_frame = mfl_bracket_signal_rows(
                        resolver_client, selected_id, source_id, year, target_weeks,
                        resolver_client.fetch_league(selected_id, year) or {},
                    )
                    if signal_frame is not None and not signal_frame.empty:
                        frame = signal_frame
                no_opponent_weeks = (
                    mfl_no_opponent_weeks(resolver_client, selected_id, year, target_weeks)
                    if (frame is None or frame.empty) and selected_id and target_weeks
                    else []
                )
                if (frame is None or frame.empty) and no_opponent_weeks == sorted(target_weeks):
                    return {
                        **row,
                        "status": "source_no_opponent_format",
                        "rows": 0,
                        "source_candidate_ids": candidate_ids,
                        "source_no_opponent_weeks": no_opponent_weeks,
                    }, None
                status = "source_returned_no_matchups"
                return {
                    **row,
                    "status": status,
                    "rows": 0,
                    "source_candidate_ids": candidate_ids,
                    "source_candidate_errors": candidate_errors,
                }, None
            row = {
                **row,
                "source_id_used": selected_id,
                "source_candidate_ids": candidate_ids,
            }
        elif platform == "fleaflicker":
            from multi_league.data_fetchers.fleaflicker.fleaflicker_api_client import FleaflickerAPIClient

            resolver_client = FleaflickerAPIClient()
            ctx = FleaflickerContext(
                league_id=source_id,
                league_name=db_name,
                start_year=year,
                end_year=year,
                league_ids={str(year): source_id},
                data_directory=Path(".source_context") / db_name,
            )
            target_weeks = {int(w) for w in (row.get("weeks") or [])}
            frame = fetch_fleaflicker_matchups(
                ctx,
                year,
                client=resolver_client,
                weeks=sorted(target_weeks) if target_weeks else None,
            )
        elif platform == "sleeper":
            ctx = SleeperContext(
                league_id=source_id,
                league_name=db_name,
                username="",
                start_year=year,
                end_year=year,
                data_directory=Path(".source_context") / db_name,
            )
            # Sleeper's canonical fetcher needs a LocalLeagueDB only as its
            # write target; the resulting DataFrame is still returned and is
            # the only object retained in this sidecar lane.
            from multi_league.core.local_db import LocalLeagueDB

            with LocalLeagueDB(ctx.data_directory, db_name) as local_db:
                target_weeks = {int(w) for w in (row.get("weeks") or [])}
                frame = fetch_sleeper_matchups(
                    ctx, year, weeks=sorted(target_weeks) if target_weeks else None, db=local_db
                )
        else:
            return {**row, "status": "unsupported_platform", "rows": 0}, None
        if frame is None or frame.empty:
            no_games = (
                fleaflicker_no_game_weeks(resolver_client, source_id, year, target_weeks)
                if platform == "fleaflicker" and target_weeks
                else []
            )
            if no_games == sorted(target_weeks):
                return {
                    **row,
                    "status": "source_no_games_weeks",
                    "rows": 0,
                    "source_no_games_weeks": no_games,
                }, None
            return {**row, "status": "source_returned_no_matchups", "rows": 0}, None
        frame = frame.copy()
        target_weeks = {int(w) for w in (row.get("weeks") or [])}
        if target_weeks and "week" in frame.columns:
            frame = frame[pd.to_numeric(frame["week"], errors="coerce").isin(target_weeks)].copy()
        if frame.empty:
            return {**row, "status": "source_returned_no_target_weeks", "rows": 0}, None
        source_weeks = sorted(
            set(pd.to_numeric(frame["week"], errors="coerce").dropna().astype(int).tolist())
        ) if "week" in frame.columns else []
        missing_target_weeks = sorted(target_weeks - set(source_weeks))
        if missing_target_weeks:
            # A targeted recovery is a whole damaged week, never a partial
            # manager-week repair. Keep the evidence for inspection, but make
            # the caller fail closed instead of promoting an incomplete week.
            no_opponent_weeks = (
                mfl_no_opponent_weeks(resolver_client, selected_id, year, set(missing_target_weeks))
                if platform == "mfl" and selected_id
                else []
            )
            no_games = (
                fleaflicker_no_game_weeks(resolver_client, source_id, year, set(missing_target_weeks))
                if platform == "fleaflicker" and resolver_client
                else []
            )
            if no_opponent_weeks == missing_target_weeks:
                status = "done_with_source_no_opponent_weeks"
            elif no_games == missing_target_weeks:
                status = "done_with_source_no_games_weeks"
            elif source_weeks and (
                all(week > max(source_weeks) for week in missing_target_weeks)
                or all(week < min(source_weeks) for week in missing_target_weeks)
            ):
                # Some historical sources stop before, or begin after, the
                # nominal target range. This is a source boundary, not a hole
                # inside a returned week range and not a fetch failure.
                status = "source_outside_target_weeks"
            else:
                status = "source_returned_partial_target_weeks"
        else:
            no_opponent_weeks = []
            no_games = []
            status = "done"
        # Sleeper's normalized fetcher historically names this field
        # ``championship``; the research lake contract is explicit
        # ``is_championship``.  Normalize before any artifact is accepted.
        if "is_championship" not in frame.columns and "championship" in frame.columns:
            frame = frame.rename(columns={"championship": "is_championship"})
        if "is_championship" not in frame.columns:
            frame["is_championship"] = 0
        # Keep the sidecar schema stable across fetchers. Older Sleeper
        # responses do not expose the canonical franchise/platform fields;
        # null franchise_id is intentional and allows manager-based matching.
        if "franchise_id" not in frame.columns:
            frame["franchise_id"] = None
        if "platform" not in frame.columns:
            frame["platform"] = platform
        # Source fetchers expose a few platform-specific nullable fields as
        # mixed Python objects (e.g. Fleaflicker waiver rank is sometimes a
        # string and sometimes numeric).  Stabilize the parquet contract before
        # the artifact leaves the worker.
        for column in _INT_COLUMNS.intersection(frame.columns):
            frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("Int64")
        for column in _FLOAT_COLUMNS.intersection(frame.columns):
            frame[column] = pd.to_numeric(frame[column], errors="coerce")
        for column in frame.columns:
            if frame[column].dtype == "object":
                frame[column] = frame[column].map(lambda value: None if pd.isna(value) else str(value))
        frame.insert(0, "db_name", db_name)
        frame.insert(1, "target_year", year)
        frame.insert(2, "target_reason_codes", json.dumps(row.get("reason_codes", []), sort_keys=True))
        return {
            **row,
            "status": status,
            "rows": len(frame),
            "requested_weeks": sorted(target_weeks),
            "source_weeks": source_weeks,
            "missing_target_weeks": missing_target_weeks,
            "source_no_opponent_weeks": no_opponent_weeks,
            "source_no_games_weeks": no_games,
            "years": sorted(frame.year.dropna().astype(int).unique().tolist()),
        }, frame
    except Exception as exc:  # status is retained for a fail-closed merge gate
        LOG.exception("source rescue failed for %s/%s", db_name, year)
        return {**row, "status": "source_fetch_failed", "rows": 0, "error": repr(exc)}, None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--targets", type=Path, required=True)
    ap.add_argument("--shard", type=int, required=True)
    ap.add_argument("--shards", type=int, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--workers", type=int, default=1)
    ap.add_argument("--platform", choices=["mfl", "fleaflicker", "sleeper"], default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--db-name", default="")
    ap.add_argument("--target-year", type=int, default=0)
    args = ap.parse_args()
    payload = json.loads(args.targets.read_text(encoding="utf-8"))
    targets = payload if isinstance(payload, list) else payload.get("targets", [])
    if args.platform:
        targets = [r for r in targets if str(r.get("platform", "")).lower() == args.platform]
    if args.db_name:
        targets = [r for r in targets if str(r.get("db_name", "")) == args.db_name]
    if args.target_year:
        targets = [r for r in targets if int(r.get("year", 0)) == args.target_year]
    targets = sorted(targets, key=lambda r: (str(r.get("db_name", "")), int(r.get("year", 0))))
    if args.limit > 0:
        targets = targets[:args.limit]
    targets = [r for r in targets if selected(r, args.shard, args.shards)]
    results: list[dict] = []
    frames: list[pd.DataFrame] = []
    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as pool:
        futures = [pool.submit(fetch_one, row) for row in targets]
        for future in as_completed(futures):
            result, frame = future.result()
            results.append(result)
            if frame is not None:
                frames.append(frame)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    result_path = args.out.with_suffix(".jsonl")
    result_path.write_text("\n".join(json.dumps(r, sort_keys=True) for r in sorted(results, key=lambda x: (x["db_name"], x["year"]))) + "\n")
    if frames:
        output = pd.concat(frames, ignore_index=True)
        output.to_parquet(args.out, index=False)
    else:
        pd.DataFrame(columns=["db_name", "target_year"]).to_parquet(args.out, index=False)
    counts = pd.Series([r["status"] for r in results]).value_counts().to_dict()
    print(json.dumps({"shard": args.shard, "targets": len(targets), "counts": counts, "rows": sum(len(f) for f in frames)}, sort_keys=True))


if __name__ == "__main__":
    main()
