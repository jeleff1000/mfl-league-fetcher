"""crawl_sleeper_drain.py -- resumable BFS "drain" of the Sleeper social graph.

The idea (per Joe): seed from supernodes (Scott Fish Bowl entrants -- elite, OG, hyper-connected
users), then breadth-first: user -> all their leagues -> all co-members -> all THEIR leagues,
enqueuing every fresh username until the frontier dries up. Drains the whole connected component
of serious-fantasy leagues, which is exactly where the thin cohorts + deep history live.

Bipartite BFS over (users <-> leagues):
  - pop a user -> get_user_leagues across `seasons` -> classify + ledger each league
  - for each NEW league -> get_league_users -> enqueue fresh users
  - walk previous_league_id back to origin (old leagues + their old-era members)

Resumable + disk-backed: visited_users / frontier / walked / ledger all persisted, so it grinds
across days within Sleeper's 1000/min limit and picks up where it left off. Read-only; NO Fly
writes; NO identities stored (league_id + cohort + best_ball flag only).

    py -3 scripts/sleeper_corpus/crawl_sleeper_drain.py --seed-leagues <id,id> --max-calls 40000
    py -3 scripts/sleeper_corpus/crawl_sleeper_drain.py --seed-users <uid,uid> --max-calls 40000
    py -3 scripts/sleeper_corpus/crawl_sleeper_drain.py            # resume; default seed = our leagues
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from collections import deque
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))
from crawl_sleeper_leagues import classify, decode_created, load_seeds  # shared logic

OUT_DIR = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/discovery")
LEDGER = OUT_DIR / "drain_leagues.parquet"
STATE = OUT_DIR / "drain_state.json"
# User-expansion seasons. 2017 is Sleeper's first NFL season, so this is the FULL reachable
# history -- nothing exists before it.
#
# This was [2025..2021] until 2026-07-17, and the "history via prev-walk" comment was wrong:
# walking previous_league_id only recovers leagues whose lineage SURVIVED into 2021+, so any
# league that ran 2018-2020 and folded was invisible. The drain showed it plainly --
# 2019: 5 leagues, 2020: 13, vs 2025: 7,764 -- and cohort cells where we held 14-28 CUSTOMER
# leagues had in_drain=0. That was never rarity; we simply never asked.
# get_user_leagues(uid, 'nfl', 2018) returns those leagues fine.
SEASONS = list(range(2025, 2016, -1))  # 2025 .. 2017


def _safe(fn, *args, default=None, tries=5):
    """Retry a Sleeper API call through transient 5xx/network blips (client only retries
    network errors, not 502/503) so a long unattended drain self-heals instead of dying."""
    for i in range(tries):
        try:
            return fn(*args)
        except Exception as e:
            if i == tries - 1:
                print(f"  [warn] {getattr(fn, '__name__', fn)}{args}: giving up ({str(e)[:60]})")
                return default
            time.sleep(min(2 ** i, 30))
    return default


def stub(lid: str) -> dict:
    _, cdate, cyear = decode_created(lid)
    return {"league_id": str(lid), "season": None, "prev_league_id": None, "teams": None,
            "roster": None, "ppr": None, "td": None, "is_dynasty": None, "league_type": None,
            "num_teams": None, "bonus_rec_te": None, "best_ball": None,
            "created_date": cdate, "created_year": cyear, "id_season_ok": None}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed-leagues", default="", help="comma league_ids to pull initial users from")
    ap.add_argument("--seed-users", default="", help="comma user_ids to seed the frontier")
    ap.add_argument("--max-calls", type=int, default=40000, help="API call budget for THIS run")
    ap.add_argument("--flush-every", type=int, default=400, help="persist every N loop iterations")
    ap.add_argument("--rewalk-visited", action="store_true",
                    help="re-enqueue every already-visited user so they are re-queried under the "
                         "CURRENT SEASONS list (added for the 2026-07-17 extension to 2017-2020). "
                         "The league ledger is kept, so known leagues dedup via `seen`: the cost "
                         "is len(SEASONS) get_user_leagues calls per user, not a league re-crawl.")
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient
    client = SleeperAPIClient()

    # ---- load resumable state ----
    seen: dict[str, dict] = {}
    if LEDGER.exists():
        for r in pq.read_table(LEDGER).to_pylist():
            seen[r["league_id"]] = r
    visited_users: set[str] = set()
    frontier: deque[str] = deque()
    in_frontier: set[str] = set()
    walked: set[str] = set()          # leagues whose members we've fetched
    league_walk: deque[str] = deque()  # prev-chain leagues to fetch via get_league
    if STATE.exists():
        st = json.loads(STATE.read_text())
        visited_users = set(st.get("visited_users", []))
        frontier = deque(st.get("frontier", []))
        in_frontier = set(frontier)
        walked = set(st.get("walked", []))
        league_walk = deque(st.get("league_walk", []))
        print(f"[resume] {len(seen):,} leagues, {len(visited_users):,} users visited, "
              f"frontier {len(frontier):,}, walk {len(league_walk):,}")
        if args.rewalk_visited and visited_users:
            back = [u for u in visited_users if u not in in_frontier]
            frontier.extend(back); in_frontier.update(back)
            visited_users.clear()
            print(f"[rewalk] re-enqueued {len(back):,} visited users under SEASONS={SEASONS}")

    # ---- seed (only if cold) ----
    if not frontier and not league_walk:
        seed_users: set[str] = set()
        for uid in filter(None, args.seed_users.split(",")):
            seed_users.add(uid.strip())
        seed_leagues = [x.strip() for x in args.seed_leagues.split(",") if x.strip()]
        if not seed_users and not seed_leagues:  # default: our own leagues' members
            seed_leagues = load_seeds(400)
        for lid in seed_leagues:
            for u in _safe(client.get_league_users, lid, default=[]) or []:
                if u.get("user_id"):
                    seed_users.add(str(u["user_id"]))
            if lid not in seen:
                L = _safe(client.get_league, lid)
                seen[lid] = classify(L) or stub(lid)
            walked.add(lid)
        for uid in seed_users:
            if uid not in in_frontier:
                frontier.append(uid); in_frontier.add(uid)
        print(f"[seed] frontier primed with {len(frontier):,} users")

    def enqueue_members(lid: str) -> int:
        n = 0
        for u in _safe(client.get_league_users, lid, default=[]) or []:
            uid = str(u.get("user_id") or "")
            if uid and uid not in visited_users and uid not in in_frontier:
                frontier.append(uid); in_frontier.add(uid); n += 1
        walked.add(lid)
        return n

    def persist():
        pq.write_table(pa.Table.from_pylist(list(seen.values())), LEDGER)
        STATE.write_text(json.dumps({
            "visited_users": list(visited_users), "frontier": list(frontier),
            "walked": list(walked), "league_walk": list(league_walk)}))

    calls = 0
    it = 0
    new_leagues_total = 0
    while (frontier or league_walk) and calls < args.max_calls:
        it += 1
        if frontier:
            uid = frontier.popleft(); in_frontier.discard(uid)
            if uid in visited_users:
                continue
            visited_users.add(uid)
            fresh_leagues = []
            for yr in SEASONS:
                leagues = _safe(client.get_user_leagues, uid, "nfl", yr, default=[]) or []
                calls += 1
                for L in leagues:
                    lid = str(L.get("league_id") or "")
                    if not lid:
                        continue
                    if lid not in seen:
                        seen[lid] = classify(L) or stub(lid)
                        new_leagues_total += 1
                        fresh_leagues.append(lid)
                        p = seen[lid].get("prev_league_id")
                        if p and p not in seen:
                            league_walk.append(p)
                if calls >= args.max_calls:
                    break
            for lid in fresh_leagues:
                if lid not in walked and calls < args.max_calls:
                    calls += 1  # get_league_users = 1 call
                    enqueue_members(lid)
        elif league_walk:
            lid = league_walk.popleft()
            if lid not in seen or seen[lid].get("season") is None:
                L = _safe(client.get_league, lid); calls += 1
                seen[lid] = classify(L) or stub(lid)
                p = seen[lid].get("prev_league_id")
                if p and p not in seen:
                    league_walk.append(p)
            if lid not in walked and calls < args.max_calls:
                calls += 1; enqueue_members(lid)

        if it % args.flush_every == 0:
            persist()
            print(f"  [drain] it {it:,} | calls {calls:,} | leagues {len(seen):,} "
                  f"(+{new_leagues_total:,}) | users {len(visited_users):,} | frontier {len(frontier):,}")

    persist()
    print(f"\n[done] calls {calls:,} | {len(seen):,} leagues ({new_leagues_total:,} new this run) | "
          f"{len(visited_users):,} users visited | frontier {len(frontier):,} remaining -> {LEDGER}")


if __name__ == "__main__":
    main()
