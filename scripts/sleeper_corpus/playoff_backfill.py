"""playoff_backfill.py -- narrow Sleeper playoff-outcome backfill (no full ingest).

The corpus fold dropped final_playoff_seed/is_playoffs for slice-delivered leagues
(~90% of the corpus). Full re-ingest is impossible (raw dbs gone) and broken anyway
(SQL-enrichment hard-fail). This pass re-derives "made the playoffs" per player-week
straight from the Sleeper public API -- winners_bracket (authoritative playoff roster
set) + per-week matchups (roster->players) crosswalked to NFL_player_id -- and writes
a SIDECAR parquet keyed (db_name, year, week, NFL_player_id, made_po) that overlays
onto the existing lake by join. Read-only against Sleeper; sidesteps the broken pipeline.

Validated 2026-07-25 vs corpus ground truth on 8 local leagues: population rate exact
(started-week bf 40.6% vs true 40.6%), ~86-90% per-instance (misses = sub-week waiver
churn). Shardable via --shard i/n over a deterministic db_name list.

    py -3 scripts/sleeper_corpus/playoff_backfill.py --db-list <file> --out <dir> [--shard 0/15]
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(os.environ.get("YAHOO_OAUTH_ROOT", "d:/yahoo_oauth"))
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))
from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient
from multi_league.data_fetchers.sleeper.playoff_utils import (
    bracket_team_ids,
    championship_contenders_for_round,
    normalize_bracket_placement,
    playoff_round_for_week,
    resolve_playoff_structure,
)

XW_PATH = Path(os.environ.get(
    "SLEEPER_NFL_MAP",
    "D:/league-history-data/nfl/ops_data/public/sleeper_nfl_player_map.parquet"))


def load_crosswalk() -> dict[str, str]:
    import duckdb
    return {str(r[0]): r[1] for r in duckdb.connect().execute(
        f"SELECT sleeper_player_id, NFL_player_id FROM read_parquet('{XW_PATH.as_posix()}')"
    ).fetchall()}


def lineage_seasons(cli: SleeperAPIClient, head_id: str) -> dict[int, tuple[str, dict]]:
    """Walk previous_league_id from the corpus db's head id back to the lineage origin.

    Returns {season: (league_id, league_obj)} -- the corpus folds a full lineage under
    smpl_{head_id}, so every season the corpus carries is reachable this way with no
    dependence on the discovery ledger.
    """
    seasons: dict[int, tuple[str, dict]] = {}
    cur: str | None = head_id
    seen: set[str] = set()
    while cur and cur not in seen:
        seen.add(cur)
        lg = cli.get_league(cur)
        if not lg:
            break
        try:
            yr = int(lg.get("season"))
        except (TypeError, ValueError):
            break
        seasons[yr] = (cur, lg)
        cur = lg.get("previous_league_id")
    return seasons


def championship_game_rosters_for_week(
    bracket: list[dict], settings: dict, week: int
) -> set[int]:
    """Return rosters actually playing a championship-bracket game in ``week``.

    A roster can qualify for the playoffs and later appear in a consolation or
    placement game. Those rows intentionally do not receive ``is_playoffs``.
    """
    structure = resolve_playoff_structure(settings)
    target_round = playoff_round_for_week(
        week,
        structure["playoff_week_start"],
        structure["playoff_round_type"],
        structure["playoff_rounds"],
    )
    if target_round < 1:
        return set()
    contenders = championship_contenders_for_round(bracket, target_round)
    if not contenders:
        return set()
    result: set[int] = set()
    for matchup in bracket:
        if matchup.get("r") != target_round:
            continue
        placement = normalize_bracket_placement(matchup.get("p"))
        if placement not in (None, 1):
            continue
        for roster_id in bracket_team_ids(matchup):
            if roster_id is not None and roster_id in contenders:
                result.add(roster_id)
    return result


def backfill_league_season(cli: SleeperAPIClient, xw: dict[str, str],
                           db_name: str, year: int, lid: str, lg: dict) -> list[tuple]:
    st = lg.get("settings", {}) or {}
    pw_start = st.get("playoff_week_start") or st.get("playoff_start_week")
    if not pw_start:
        return []
    bracket = cli.get_winners_bracket(lid) or []
    po_rosters: set[int] = set()
    for m in bracket:
        a, b = bracket_team_ids(m)
        for r in (a, b):
            if r is not None:
                po_rosters.add(int(r))
    if not po_rosters:
        return []  # bracket not resolvable (in-progress / absent) -> leave to fallback lane
    out: list[tuple] = []
    for wk in range(1, int(pw_start) + 5):
        for mu in (cli.get_league_matchups(lid, wk) or []):
            rid = mu.get("roster_id")
            if rid is None:
                continue
            made = 1 if int(rid) in po_rosters else 0
            championship_rosters = championship_game_rosters_for_week(bracket, st, wk)
            in_po_wk = 1 if int(rid) in championship_rosters else 0
            for spid in (mu.get("players") or []):
                nfl = xw.get(str(spid))
                if nfl:
                    out.append((db_name, year, wk, nfl, made, in_po_wk))
    return out


SCHEMA = pa.schema([
    ("db_name", pa.string()), ("year", pa.int32()), ("week", pa.int32()),
    ("NFL_player_id", pa.string()), ("made_po", pa.int8()), ("is_playoffs", pa.int8()),
])


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db-list", required=True, help="text file of corpus db_names (smpl_*), one per line")
    ap.add_argument("--out", required=True, help="output dir for the sidecar parquet")
    ap.add_argument("--shard", default="0/1", help="i/n -- process only db_names where idx %% n == i")
    args = ap.parse_args()
    i, n = (int(x) for x in args.shard.split("/"))

    dbs = [l.strip() for l in Path(args.db_list).read_text().splitlines() if l.strip()]
    dbs = [d for k, d in enumerate(dbs) if k % n == i]
    xw = load_crosswalk()
    cli = SleeperAPIClient()
    print(f"[po-backfill] shard {i}/{n}: {len(dbs):,} leagues, crosswalk={len(xw):,}", flush=True)

    rows: list[tuple] = []
    done = 0
    for db in dbs:
        head = db[len("smpl_"):] if db.startswith("smpl_") else db
        try:
            for yr, (lid, lg) in lineage_seasons(cli, head).items():
                rows.extend(backfill_league_season(cli, xw, db, yr, lid, lg))
        except Exception as e:  # a dead/renamed league must not kill the shard
            print(f"  ! {db}: {str(e)[:100]}", flush=True)
        done += 1
        if done % 100 == 0:
            print(f"  {done:,}/{len(dbs):,} leagues, {len(rows):,} rows", flush=True)

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"playoff_backfill_shard{i}-of{n}.parquet"
    cols = list(zip(*rows)) if rows else [[]] * 6
    pq.write_table(pa.table({
        "db_name": pa.array(cols[0], pa.string()), "year": pa.array(cols[1], pa.int32()),
        "week": pa.array(cols[2], pa.int32()), "NFL_player_id": pa.array(cols[3], pa.string()),
        "made_po": pa.array(cols[4], pa.int8()), "is_playoffs": pa.array(cols[5], pa.int8()),
    }, schema=SCHEMA), out_path)
    print(f"[po-backfill] wrote {len(rows):,} rows -> {out_path}", flush=True)


if __name__ == "__main__":
    main()
