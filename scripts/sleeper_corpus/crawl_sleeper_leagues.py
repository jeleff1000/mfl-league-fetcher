"""crawl_sleeper_leagues.py -- read-only discovery crawl of PUBLIC Sleeper leagues.

Goal: find leagues in the rule-set cohorts our population is thin on, so a later ingest can
drive every cohort/year (2017+, Sleeper's first NFL season) to a confident sample size.

Strategy (all via the keyless, rate-limited SleeperAPIClient -- no ingest, no writes to Fly):
  A. seed = our own Sleeper league_ids (read once from Fly ___leagues).
  B. get_league_users(seed) -> co-member user_ids.
  C. get_user_leagues(user, 'nfl', {recent seasons}) -> full league objects (settings+scoring+
     roster_positions+previous_league_id), classify cohort in-place (no 2nd call).
  D. walk previous_league_id back to each lineage's ORIGIN (captures full 2017->now history).

Classification MIRRORS the cohort builders' _LS SQL exactly (teams/roster/ppr/td/is_dynasty)
so the projection is apples-to-apples. Output: a local ledger parquet (league_id + cohort +
season + lineage), NO league names or usernames stored (league_id alone is not PII).

Resumable: re-run appends; already-seen league_ids are skipped.

    py -3 scripts/sleeper_corpus/crawl_sleeper_leagues.py --seed-cap 500 --user-cap 1200 --classify-cap 8000
"""
from __future__ import annotations

import argparse
import datetime as dt
import random
import sys
import time
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

OUT_DIR = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/discovery")
LEDGER = OUT_DIR / "discovered_leagues.parquet"

# IDP starting slots (Sleeper roster_positions tokens) -> roster='idp'
IDP_SLOTS = {"DL", "LB", "DB", "IDP_FLEX", "DE", "DT", "CB", "S", "SS", "FS", "ILB", "OLB", "EDGE", "IDP"}

# Sleeper league_ids are Snowflake IDs: high bits = ms since a custom epoch (22 low bits are
# machine/sequence). id>>22 + EPOCH_MS = creation timestamp -> season, with ZERO API calls.
# Epoch fit empirically from (league_id, season) pairs (~2016-05-24); good to the season.
EPOCH_MS = 1_464_124_576_608


def decode_created(league_id) -> tuple[int | None, str | None, int | None]:
    """(created_ms, created_date_iso, created_year) decoded from a Sleeper Snowflake id."""
    try:
        ms = (int(league_id) >> 22) + EPOCH_MS
        d = dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)
        return ms, d.date().isoformat(), d.year
    except (TypeError, ValueError, OverflowError, OSError):
        return None, None, None


def classify(L: dict) -> dict | None:
    """Sleeper league object -> cohort row, mirroring the builders' _LS CASE logic."""
    if not L or not L.get("league_id"):
        return None
    s = L.get("settings") or {}
    sc = L.get("scoring_settings") or {}
    rp = L.get("roster_positions") or []
    try:
        season = int(L.get("season"))
    except (TypeError, ValueError):
        season = None
    num = s.get("num_teams") or L.get("total_rosters") or 0
    try:
        num = int(num)
    except (TypeError, ValueError):
        num = 0
    teams = "10t" if num <= 11 else "12t"
    if any(p in IDP_SLOTS for p in rp):
        roster = "idp"
    elif "SUPER_FLEX" in rp:
        roster = "sflx"
    else:
        roster = "flx"
    rec = sc.get("rec", 0) or 0
    ppr = "std" if rec == 0 else ("half" if rec < 0.75 else "ppr")
    ptd = sc.get("pass_td", 4)
    ptd = 4 if ptd is None else ptd
    td = "6pt" if ptd >= 5 else "4pt"
    cms, cdate, cyear = decode_created(L.get("league_id"))
    return {
        "league_id": str(L.get("league_id")),
        "season": season,
        "prev_league_id": str(L["previous_league_id"]) if L.get("previous_league_id") else None,
        "teams": teams,
        "roster": roster,
        "ppr": ppr,
        "td": td,
        "is_dynasty": bool(s.get("type") == 2),   # 0=redraft,1=keeper,2=dynasty
        "league_type": int(s.get("type") or 0),
        "num_teams": num,
        "bonus_rec_te": float(sc.get("bonus_rec_te") or 0),  # TE-premium marker (research LAMAR 'tep')
        "best_ball": bool(s.get("best_ball") == 1),  # own lane: great draft/ADP, no txns, auto lineups
        "created_date": cdate,
        "created_year": cyear,
        # ID-decoded year should match the season field; flag drift for later cleaning.
        "id_season_ok": (cyear is not None and season is not None and abs(cyear - season) <= 1),
    }


def load_seeds(limit: int | None) -> list[str]:
    import os
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("="); os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    from multi_league.core.readers.fly_reader import FlyReader
    rows = FlyReader().query(
        "SELECT DISTINCT league_key FROM public.league_settings "
        "WHERE LOWER(platform) LIKE '%sleep%' AND league_key IS NOT NULL", "___leagues")
    seeds = [str(r["league_key"]) for r in rows if r.get("league_key")]
    random.shuffle(seeds)
    return seeds[:limit] if limit else seeds


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed-cap", type=int, default=500, help="seed leagues to pull members from")
    ap.add_argument("--user-cap", type=int, default=1200, help="unique users to expand")
    ap.add_argument("--recent-seasons", default="2025,2024", help="seasons to query per user for lineage discovery")
    ap.add_argument("--classify-cap", type=int, default=8000, help="max get_league history-walk calls")
    ap.add_argument("--flush-every", type=int, default=500)
    args = ap.parse_args()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    from multi_league.data_fetchers.sleeper.sleeper_api_client import SleeperAPIClient
    client = SleeperAPIClient()

    seen: dict[str, dict] = {}            # league_id -> cohort row
    known_seed: set[str] = set()          # our own current-season league_ids (dedupe "new" vs "known")
    if LEDGER.exists():                   # resume
        for r in pq.read_table(LEDGER).to_pylist():
            seen[r["league_id"]] = r
        print(f"[resume] loaded {len(seen):,} previously-classified leagues")

    seeds = load_seeds(args.seed_cap)
    known_seed = set(seeds) | set(load_seeds(None))  # all our league_keys are "known"
    print(f"[seed] {len(seeds):,} seed leagues (of {len(known_seed):,} known); querying members...")

    def flush():
        pq.write_table(pa.Table.from_pylist(list(seen.values())), LEDGER)

    # ---- Phase B: collect co-member user_ids from seed leagues ----
    users: set[str] = set()
    for i, lid in enumerate(seeds, 1):
        for u in client.get_league_users(lid) or []:
            if u.get("user_id"):
                users.add(str(u["user_id"]))
        if i % 100 == 0:
            print(f"  [members] {i}/{len(seeds)} seeds -> {len(users):,} unique users")
    users = list(users); random.shuffle(users); users = users[: args.user_cap]
    print(f"[users] expanding {len(users):,} users across seasons {args.recent_seasons}")

    # ---- Phase C: per user, full league objects for recent seasons (classify in place) ----
    seasons = [int(x) for x in args.recent_seasons.split(",")]
    prev_frontier: set[str] = set()
    new_ct = 0
    for i, uid in enumerate(users, 1):
        for yr in seasons:
            for L in client.get_user_leagues(uid, "nfl", yr) or []:
                row = classify(L)
                if not row:
                    continue
                if row["league_id"] not in seen:
                    seen[row["league_id"]] = row
                    new_ct += 1
                if row["prev_league_id"]:
                    prev_frontier.add(row["prev_league_id"])
        if i % 200 == 0:
            print(f"  [expand] {i}/{len(users)} users -> {len(seen):,} leagues ({new_ct:,} new)")
            flush()

    # ---- Phase D: walk previous_league_id lineages back to origin (2017-ish) ----
    print(f"[history] walking {len(prev_frontier):,} lineage tails back to origin...")
    calls = 0
    frontier = [p for p in prev_frontier if p and p not in seen]
    while frontier and calls < args.classify_cap:
        lid = frontier.pop()
        if lid in seen:
            continue
        L = client.get_league(lid); calls += 1
        row = classify(L)
        if not row:
            cms, cdate, cyear = decode_created(lid)
            seen[lid] = {"league_id": lid, "season": None, "prev_league_id": None, "teams": None,
                         "roster": None, "ppr": None, "td": None, "is_dynasty": None,
                         "league_type": None, "num_teams": None, "bonus_rec_te": None, "best_ball": None,
                         "created_date": cdate, "created_year": cyear, "id_season_ok": None}
            continue
        seen[row["league_id"]] = row
        if row["prev_league_id"] and row["prev_league_id"] not in seen:
            frontier.append(row["prev_league_id"])
        if calls % args.flush_every == 0:
            print(f"  [history] {calls:,} calls, {len(seen):,} leagues, frontier {len(frontier):,}")
            flush()

    flush()
    # summary
    con = duckdb.connect()
    con.execute(f"CREATE VIEW d AS SELECT * FROM '{LEDGER.as_posix()}'")
    tot, dyn, ss = con.execute(
        "SELECT COUNT(*), COUNT(*) FILTER (WHERE is_dynasty), COUNT(*) FILTER (WHERE is_dynasty=false) FROM d "
        "WHERE season IS NOT NULL").fetchone()
    known_new = con.execute(
        f"SELECT COUNT(*) FILTER (WHERE league_id NOT IN (SELECT UNNEST(?))) FROM d WHERE season IS NOT NULL",
        [list(known_seed)]).fetchone()[0]
    print(f"\n[done] {tot:,} classified league-seasons ({ss:,} single-season, {dyn:,} dynasty); "
          f"~{known_new:,} NOT in our current seed set -> {LEDGER}")
    print(con.execute(
        "SELECT MIN(season) min_yr, MAX(season) max_yr, COUNT(DISTINCT season) yrs FROM d WHERE season IS NOT NULL"
    ).fetchdf().to_string(index=False))


if __name__ == "__main__":
    main()
