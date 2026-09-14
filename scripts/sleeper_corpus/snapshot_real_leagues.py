"""snapshot_real_leagues.py -- one-time local snapshot of the REAL customer leagues.

Pulls the four tables the cohort builders read (only the needed columns; player_fantasy is
rostered-only) from Fly ___leagues into a local DuckDB, so the cohort build -- and the whole
corpus grind -- runs OFFLINE with zero Fly dependency. This is the ONLY Fly touch; refresh it
only when you want fresh real-league data (e.g. after a weekly update).

Chunked per-year (+ hash-buckets on the big tables) to stay under the read timeout and polite
to the box. Writes public.{league_settings,draft,transactions,player_fantasy}.

    py -3 scripts/sleeper_corpus/snapshot_real_leagues.py
"""
from __future__ import annotations
import os, sys
from pathlib import Path
import duckdb
import pyarrow as pa

ROOT = Path("d:/yahoo_oauth")
sys.path.insert(0, str(ROOT / "fantasy_football_data_scripts"))

OUT = Path("D:/league-history-data/fantasy_leagues/sampling_corpus/leagues_snapshot.duckdb")
SNAPSHOT_MIN_YEAR = 1997
SNAPSHOT_MAX_YEAR = 2025


def snapshot_years(start: int = SNAPSHOT_MIN_YEAR, end: int = SNAPSHOT_MAX_YEAR) -> range:
    """Inclusive local-lake range; fail closed instead of silently dropping history."""
    if start > end:
        raise ValueError(f"snapshot start year {start} exceeds end year {end}")
    if start < SNAPSHOT_MIN_YEAR or end > SNAPSHOT_MAX_YEAR:
        raise ValueError(
            f"snapshot years {start}-{end} exceed supported range "
            f"{SNAPSHOT_MIN_YEAR}-{SNAPSHOT_MAX_YEAR}"
        )
    return range(start, end + 1)

# is_keeper/"round" included: keeper% needs them, and is_keeper is the ONLY trustworthy keeper
# gate -- league_settings.max_keepers is a capture gap on Yahoo (26.5% populated vs 100% on
# Sleeper/ESPN) and disagrees with reality on 10 Yahoo league-years that have kept picks while
# the setting reads 0/NULL. is_keeper is 100% populated on every platform.
# "round" is quoted: it collides with DuckDB's round() function.
DRAFT_COLS = ('db_name, year, NFL_player_id, pick, cost, draft_value_zscore, '
              'pick_quality_zscore, manager_lamar, total_fantasy_points, '
              'is_keeper, "round", pick_in_round, '
              # auction budget derivation needs per-team spend (Joe 2026-07-20)
              'franchise_id, manager')
# week included: the weekly-grain research builders (build_weekly_txn/matchup) group by week
TXN_COLS = ("db_name, year, week, NFL_player_id, franchise_id, transaction_type, faab_bid, transaction_score, "
            "manager_lamar_ros_managed, player_lamar_ros_total")
PF_COLS = ("db_name, year, week, NFL_player_id, player, position, fantasy_position, platform, "
           "team_key, team_name, nfl_team_api, yahoo_player_id, sleeper_player_id, "
           "espn_player_id, fleaflicker_player_id, mfl_player_id, "
           "CAST(COALESCE(CAST(is_started AS INTEGER), CASE "
           "WHEN fantasy_position IS NULL THEN NULL "
           "WHEN UPPER(TRIM(CAST(fantasy_position AS VARCHAR))) IN "
           "('BN','IR','TAXI','BENCH','RESERVE','FA','WAIVERS','') THEN 0 ELSE 1 END) AS INTEGER) AS is_started, "
           "is_rostered, "
           "fantasy_points, win, champion, clutch_equity, manager_lamar, manager, team_points")
# playoff ground truth (ledger D15); the standings derivation from pf is only the fallback
MU_COLS = ("db_name, year, week, manager, franchise_id, team_points, is_playoffs, "
           "playoff_seed, final_playoff_seed, champion, is_championship")


def main() -> None:
    import argparse
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", choices=["league_settings", "draft", "transactions", "player_fantasy", "matchup"],
                    action="append",
                    help="refresh only these tables IN PLACE, leaving the rest of the snapshot "
                         "alone. Without it the whole snapshot is rebuilt from scratch -- which "
                         "means re-pulling the 16M-row player_fantasy just to add a draft column.")
    ap.add_argument("--start-year", type=int, default=SNAPSHOT_MIN_YEAR)
    ap.add_argument("--end-year", type=int, default=SNAPSHOT_MAX_YEAR)
    ap.add_argument("--output", type=Path, default=OUT,
                    help="Build at an alternate path for atomic validation/swap")
    args = ap.parse_args()
    years = snapshot_years(args.start_year, args.end_year)
    out = args.output

    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("="); os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))
    from multi_league.core.readers.fly_reader import FlyReader
    fly = FlyReader()

    out.parent.mkdir(parents=True, exist_ok=True)
    if not args.only:
        # Full rebuild. Drop the WAL WITH the database: unlinking only the .duckdb leaves an
        # orphan .wal that DuckDB will try to replay into the NEXT database created at this
        # path -- and if a rerun dies before recreating it, you are left with a lone .wal and
        # no database, which reads as "the snapshot is missing" (a 2026-07-14 run left exactly
        # that, and cost a session to diagnose).
        for p in (out, out.with_suffix(out.suffix + ".wal")):
            if p.exists():
                p.unlink()
    con = duckdb.connect(str(out))
    con.execute("SET memory_limit='3000MB'")
    con.execute("CREATE SCHEMA IF NOT EXISTS public")
    if args.only:
        # Selective refresh: drop just the named tables so they re-CREATE with the current
        # column contract, and leave everything else on disk untouched.
        for t in args.only:
            con.execute(f"DROP TABLE IF EXISTS public.{t}")
        print(f"[snapshot] selective refresh: {', '.join(args.only)}")

    def want(table: str) -> bool:
        return not args.only or table in args.only

    def pull_into(table: str, sql: str) -> int:
        rows = fly.query(sql, "___leagues")
        if not rows:
            return 0
        con.register("t", pa.Table.from_pylist(rows))
        exists = con.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public' AND table_name=?",
            [table]).fetchone()[0]
        con.execute(f"{'INSERT INTO' if exists else 'CREATE TABLE'} public.{table} "
                    f"{'SELECT * FROM t' if exists else 'AS SELECT * FROM t'}")
        con.unregister("t")
        return len(rows)

    # league_settings — small, one pull, all columns
    if want("league_settings"):
        n = pull_into("league_settings", "SELECT * FROM public.league_settings")
        print(f"[snapshot] league_settings: {n:,}")

    # draft — per year (small)
    if want("draft"):
        tot = 0
        for y in years:
            tot += pull_into("draft", f"SELECT {DRAFT_COLS} FROM public.draft WHERE year={y}")
        print(f"[snapshot] draft: {tot:,}")

    # transactions — per year x 4 hash-buckets
    if want("transactions"):
        tot = 0
        for y in years:
            for b in range(4):
                tot += pull_into("transactions",
                    f"SELECT {TXN_COLS} FROM public.transactions WHERE year={y} AND (hash(db_name)%4)={b}")
            print(f"  [txn] {y}: running total {tot:,}")
        print(f"[snapshot] transactions: {tot:,}")

    # matchup — per year x 4 hash-buckets (team-week grain, small)
    if want("matchup"):
        tot = 0
        for y in years:
            for b in range(4):
                tot += pull_into("matchup",
                    f"SELECT {MU_COLS} FROM public.matchup WHERE year={y} AND (hash(db_name)%4)={b}")
        print(f"[snapshot] matchup: {tot:,}")

    # player_fantasy — rostered only, per year x 24 hash-buckets (the 16M-row table)
    if want("player_fantasy"):
        tot = 0
        for y in years:
            for b in range(24):
                tot += pull_into("player_fantasy",
                    f"SELECT {PF_COLS} FROM public.player_fantasy "
                    f"WHERE year={y} AND CAST(is_rostered AS INT)=1 AND (hash(db_name)%24)={b}")
            print(f"  [pf] {y}: running total {tot:,}")
        print(f"[snapshot] player_fantasy (rostered): {tot:,}")

    con.close()
    print(f"\n[done] {out}")


if __name__ == "__main__":
    main()
