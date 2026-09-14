"""How many leagues a cohort needs before ONE player's roster% is trustworthy, per position.

This is the tool that sets the MIN_STABLE roster floors in build_wide_bundle. It lives in the
repo, not the scratchpad, for two reasons: the numbers it produces get committed, and a
scratchpad copy of a repo module silently shadows the real one (see the 2026-08-03 incident
where teams came out 2-tier instead of 4 and a whole measurement had to be redone).

THE QUESTION, in Joe's words: how many leagues before we are 85% sure a 50%-rostered player
reads between 45 and 55.

THE FLOOR IS THE HARDEST PLAYER, NOT THE AVERAGE ONE. Required N is an inverted U in the rate
itself -- a 99%-rostered stud and a 3%-rostered fifth-stringer are both pinned by a handful of
leagues, because their league-to-league spread is nearly zero. The expensive player sits at
50%. Satisfy him and every other player is satisfied by construction, so the floor is the PEAK
of that curve and a per-player gate would be the wrong instrument.

METHOD. For a (year, cohort, player) cell the population is the eligible LIVE leagues in that
cohort, and the player's value in each is his share of the season held there -- 0 for a league
that never rostered him, because absence is the zero (R9). Two answers:

    analytic   n = (z * SD / margin)^2
    empirical  smallest n where >=CONF of random n-league draws land within margin of truth

Trust the empirical one. A share distribution is part point-mass-at-zero and part continuous,
which is exactly where the normal approximation misbehaves. Resampling is WITH replacement on
purpose: the question is "how big must a cohort be", not "how well did I measure these
particular leagues", and sampling without replacement would flatter the floor via the
finite-population correction.

REDRAFT MANAGED ONLY. Dynasty holds the roster by rule and best ball never moves it, so both
are sticky by construction rather than by behaviour. Format is held fixed and drops out of the
cohort key.
"""
from __future__ import annotations

import argparse
from pathlib import Path
from statistics import NormalDist

import duckdb
import numpy as np
import pandas as pd

import position_slots_contract as PS
from cohort_format_sql import cohort_league_settings_sql

# k_slots / def_slots back the K and DEF eligibility gates; not in the default select.
_EXTRAS = ("COALESCE(s.roster_K, 0) AS k_slots", "COALESCE(s.roster_DEF, 0) AS def_slots")

MIN_LEAGUES_COHORT = 40    # a cohort too thin to carry a between-league variance
MIN_LEAGUES_CELL = 25      # a (player, cohort) cell too thin to carry one
N_GRID = [10, 15, 20, 25, 30, 40, 50, 60, 75, 90, 110, 130, 160, 200, 250, 300, 400]
B = 400        # draws per n; 400 puts the +/- on an 85% hit rate at about 1.8 points
B_SEQ = 200    # sequential runs per cell
HORIZON = 400  # leagues to walk before giving up on stabilisation

# DEF is spelled DST in the super table's position column for some sources; both are accepted.
SUPERTABLE_POSITION = {"DEF": ("DEF", "DST")}


def _position_predicate(pos: str) -> str:
    names = SUPERTABLE_POSITION.get(pos, (pos,))
    inlist = ", ".join(f"'{n}'" for n in names)
    # position holds a comma list for multi-eligible players; those are excluded rather than
    # assigned, because a WR/RB is not a clean member of either population.
    return f"UPPER(TRIM(position)) IN ({inlist}) AND position NOT LIKE '%,%'"


def cell_rows(snapshot: Path, ops: Path, pos: str, year: int, tmp: Path) -> pd.DataFrame:
    """Per (cohort, player, league) season share for one position-year."""
    con = duckdb.connect(config={
        "memory_limit": "2000MB", "threads": 3, "temp_directory": str(tmp / f"{pos}{year}")})
    try:
        for s in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                  "PRAGMA max_temp_directory_size='20GB'"):
            con.execute(s)
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS lake (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
        # PASS THE YEAR: unscoped, the capacity CTEs aggregate every year x 9 positions in one
        # query and take the runtime down rather than merely running slowly.
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True, year=year,
                                               extra_select=_EXTRAS)
                    .replace("public.", "lake.public."))
        con.execute(f"""CREATE OR REPLACE TEMP TABLE plyr AS
          SELECT NFL_player_id AS pid, MAX(player) AS pname
          FROM ops.nfl_historical.nfl_player_stats_all
          WHERE "year"={year} AND NFL_player_id IS NOT NULL
            AND {_position_predicate(pos)} GROUP BY 1""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lw AS
          SELECT db_name, COUNT(DISTINCT week) AS wks
          FROM lake.public.player_fantasy WHERE year={year} GROUP BY 1""")
        # teams is POSITION-SPECIFIC derived capacity, so the cohort key must read this
        # position's own tier column -- teams_RB for RB, never the generic 2-level `teams`.
        # A league is eligible only where the position is rosterable (contract, R9).
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lgc AS
          SELECT f.db_name,
                 concat_ws('|', f.teams_{pos}, f.roster, f.ppr, f.td, f.bracket) AS cohort
          FROM fmt f JOIN lw ON lw.db_name=f.db_name
          WHERE f.year={year} AND f.teams_{pos} <> 'ALL' AND f.roster IS NOT NULL
            AND f.ppr IS NOT NULL AND f.td IS NOT NULL AND f.bracket IS NOT NULL
            AND COALESCE(f.lineup_mode,'') <> 'best_ball'
            AND COALESCE(f.league_type,'')  <> 'dynasty'
            AND ({PS.position_eligibility_sql(pos, 'f')})""")
        con.execute("CREATE OR REPLACE TEMP TABLE elig AS "
                    "SELECT cohort, COUNT(*) AS n_elig FROM lgc GROUP BY 1 "
                    f"HAVING COUNT(*) >= {MIN_LEAGUES_COHORT}")
        df = con.execute(f"""
          SELECT g.cohort, pf.NFL_player_id AS pid, MAX(n.pname) AS pname,
                 MAX(e.n_elig) AS n_elig, pf.db_name,
                 COUNT(*)::DOUBLE / MAX(w.wks) AS share
          FROM lake.public.player_fantasy pf
          JOIN plyr n ON n.pid = pf.NFL_player_id
          JOIN lgc g  ON g.db_name = pf.db_name
          JOIN lw w   ON w.db_name = pf.db_name
          JOIN elig e ON e.cohort = g.cohort
          WHERE pf.year={year}
          GROUP BY 1,2,5""").fetchdf()
        df["year"] = year
        return df
    finally:
        con.close()


def week_rows(snapshot: Path, ops: Path, pos: str, year: int, tmp: Path,
              lo: float, hi: float) -> pd.DataFrame:
    """Per (cohort, player, week): how many eligible leagues rostered him THAT week.

    At weekly grain a league's value is 0/1 -- he is on a roster that week or he is not --
    so the cell is a Bernoulli over the cohort's eligible leagues, not a share. That is the
    whole reason the weekly floor is bigger than the season one: a coin flip carries the most
    between-league variance a bounded variable can, sd = sqrt(p(1-p)) = 0.5 at p = 0.5, where
    a season share at the same rate carries about 0.30.
    """
    con = duckdb.connect(config={
        "memory_limit": "2000MB", "threads": 3, "temp_directory": str(tmp / f"w{pos}{year}")})
    try:
        for s_ in ("SET enable_progress_bar=false", "SET preserve_insertion_order=false",
                   "PRAGMA max_temp_directory_size='20GB'"):
            con.execute(s_)
        con.execute(f"ATTACH '{snapshot.as_posix()}' AS lake (READ_ONLY)")
        con.execute(f"ATTACH '{ops.as_posix()}' AS ops (READ_ONLY)")
        con.execute("CREATE OR REPLACE TEMP TABLE fmt AS " +
                    cohort_league_settings_sql(position_slots=True, year=year,
                                               extra_select=_EXTRAS)
                    .replace("public.", "lake.public."))
        con.execute(f"""CREATE OR REPLACE TEMP TABLE plyr AS
          SELECT NFL_player_id AS pid, MAX(player) AS pname
          FROM ops.nfl_historical.nfl_player_stats_all
          WHERE "year"={year} AND NFL_player_id IS NOT NULL
            AND {_position_predicate(pos)} GROUP BY 1""")
        con.execute(f"""CREATE OR REPLACE TEMP TABLE lgc AS
          SELECT f.db_name,
                 concat_ws('|', f.teams_{pos}, f.roster, f.ppr, f.td, f.bracket) AS cohort
          FROM fmt f
          JOIN (SELECT DISTINCT db_name FROM lake.public.player_fantasy WHERE year={year}) lv
            ON lv.db_name=f.db_name
          WHERE f.year={year} AND f.teams_{pos} <> 'ALL' AND f.roster IS NOT NULL
            AND f.ppr IS NOT NULL AND f.td IS NOT NULL AND f.bracket IS NOT NULL
            AND COALESCE(f.lineup_mode,'') <> 'best_ball'
            AND COALESCE(f.league_type,'')  <> 'dynasty'
            AND ({PS.position_eligibility_sql(pos, 'f')})""")
        con.execute("CREATE OR REPLACE TEMP TABLE elig AS "
                    "SELECT cohort, COUNT(*) AS n_elig FROM lgc GROUP BY 1 "
                    f"HAVING COUNT(*) >= {MIN_LEAGUES_COHORT}")
        # Filter the band in SQL. Off-band cells are the overwhelming majority (most players
        # are near 0 or near 1 in most weeks) and pulling them into pandas just to drop them
        # is the difference between a minute and an hour.
        df = con.execute(f"""
          SELECT g.cohort, pf.NFL_player_id AS pid, MAX(n.pname) AS pname, pf.week,
                 MAX(e.n_elig) AS n_elig, COUNT(DISTINCT pf.db_name) AS n_rostered
          FROM lake.public.player_fantasy pf
          JOIN plyr n ON n.pid = pf.NFL_player_id
          JOIN lgc g  ON g.db_name = pf.db_name
          JOIN elig e ON e.cohort = g.cohort
          WHERE pf.year={year} AND pf.week BETWEEN 1 AND 17
          GROUP BY 1,2,4
          HAVING COUNT(DISTINCT pf.db_name)::DOUBLE / MAX(e.n_elig) BETWEEN {lo} AND {hi}
        """).fetchdf()
        df["year"] = year
        return df
    finally:
        con.close()


def required_n(shares: np.ndarray, rng, margin: float, conf: float) -> int:
    """Smallest n on the grid whose draws land within `margin` of truth >=conf of the time."""
    truth = shares.mean()
    for n in N_GRID:
        # No cap at len(shares): drawing with replacement means n may exceed the leagues
        # observed, which is the whole point of asking how big a cohort must be.
        idx = rng.integers(0, len(shares), size=(B, n))
        if (np.abs(shares[idx].mean(axis=1) - truth) <= margin).mean() >= conf:
            return n
    return -1


def stabilise_n(shares: np.ndarray, rng, margin: float, conf: float) -> int:
    """Leagues before the RUNNING estimate settles inside the band and stays there.

    Stricter than required_n and a different question. required_n is a snapshot -- draw n,
    do you land in the band. This walks leagues in one at a time, the way a cohort actually
    fills, and asks when the number stops wandering back out. An estimate can be inside at
    n=60 and outside again at n=70; that has not stabilised at 60 in any useful sense.
    """
    truth = shares.mean()
    draws = shares[rng.integers(0, len(shares), size=(B_SEQ, HORIZON))]
    run = np.cumsum(draws, axis=1) / np.arange(1, HORIZON + 1)
    out = np.abs(run - truth) > margin
    last = np.where(out.any(axis=1), HORIZON - np.argmax(out[:, ::-1], axis=1), 0)
    return int(np.quantile(last, conf))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--snapshot", type=Path, required=True)
    ap.add_argument("--ops", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--position", default="WR", choices=list(PS.TIER_POSITIONS))
    ap.add_argument("--grain", default="season", choices=("season", "weekly"))
    ap.add_argument("--first-year", type=int, default=2021)
    ap.add_argument("--last-year", type=int, default=2025)
    ap.add_argument("--band", type=float, nargs=2, default=(0.40, 0.60))
    ap.add_argument("--margin", type=float, default=0.05)
    ap.add_argument("--conf", type=float, default=0.85)
    ap.add_argument("--thick", type=int, default=300, help="cohort size for the headline cell")
    ap.add_argument("--tmp", type=Path, default=Path("D:/tmp/ddbtmp/reqn"))
    a = ap.parse_args()
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.tmp.mkdir(parents=True, exist_ok=True)
    # Fixed seed: the floor is a committed number, so the same inputs must give it back.
    rng = np.random.default_rng(20260803)
    lo, hi = a.band

    rows = []
    if a.grain == "weekly":
        # A weekly cell is k ones and (n_elig - k) zeros, so required_n depends ONLY on p.
        # Memoise on p: without it this recomputes the same bootstrap for tens of thousands
        # of cells that share a rate, which is the whole runtime.
        cache: dict[int, tuple[int, int]] = {}
        for year in range(a.first_year, a.last_year + 1):
            raw = week_rows(a.snapshot, a.ops, a.position, year, a.tmp, lo, hi)
            print(f"  {year}: {len(raw):,} in-band player-week cells", flush=True)
            for r in raw.itertuples(index=False):
                n_elig, k = int(r.n_elig), int(r.n_rostered)
                rate = k / n_elig
                key = int(round(rate * 1000))
                if key not in cache:
                    v = np.concatenate([np.ones(1000), np.zeros(0)])  # placeholder, replaced
                    pk = key / 1000.0
                    ones = int(round(pk * 2000))
                    v = np.concatenate([np.ones(ones), np.zeros(2000 - ones)])
                    cache[key] = (required_n(v, rng, a.margin, a.conf),
                                  stabilise_n(v, rng, a.margin, a.conf))
                ne, ns = cache[key]
                rows.append({
                    "position": a.position, "year": year, "cohort": r.cohort, "pid": r.pid,
                    "pname": r.pname, "week": int(r.week), "n_elig": n_elig,
                    "lgs_rostered": k, "rate": rate,
                    # a Bernoulli's spread is fixed by its rate; nothing else to estimate
                    "sd": float(np.sqrt(rate * (1 - rate))), "breadth": rate, "depth": 1.0,
                    "n_analytic": (NormalDist().inv_cdf(1 - (1 - a.conf) / 2)
                                   * np.sqrt(rate * (1 - rate)) / a.margin) ** 2,
                    "n_empirical": ne, "n_stabilise": ns})
        out = pd.DataFrame(rows)
        out.to_parquet(a.out, index=False)
        _report(out, a, lo, hi)
        return

    for year in range(a.first_year, a.last_year + 1):
        raw = cell_rows(a.snapshot, a.ops, a.position, year, a.tmp)
        print(f"  {year}: {len(raw):,} player-league rows", flush=True)
        for (cohort, pid), g in raw.groupby(["cohort", "pid"]):
            n_elig = int(g.n_elig.iloc[0])
            if len(g) < MIN_LEAGUES_CELL:
                continue
            # THE ZEROS ARE THE POINT: leagues in the cohort that never rostered him are real
            # zeros in his distribution, not missing rows. Pad to the full eligible set.
            shares = np.concatenate([g.share.to_numpy(), np.zeros(n_elig - len(g))])
            rate = float(shares.mean())
            if not (lo <= rate <= hi):
                continue
            sd = float(shares.std(ddof=1))
            breadth = len(g) / n_elig
            rows.append({
                "position": a.position, "year": year, "cohort": cohort, "pid": pid,
                "pname": g.pname.iloc[0], "n_elig": n_elig, "lgs_rostered": len(g),
                "rate": rate, "sd": sd, "breadth": breadth, "depth": rate / breadth,
                "n_analytic": (NormalDist().inv_cdf(1 - (1 - a.conf) / 2) * sd / a.margin) ** 2,
                "n_empirical": required_n(shares, rng, a.margin, a.conf),
                "n_stabilise": stabilise_n(shares, rng, a.margin, a.conf)})
    out = pd.DataFrame(rows)
    out.to_parquet(a.out, index=False)
    _report(out, a, lo, hi)


def _report(out: pd.DataFrame, a, lo: float, hi: float) -> None:
    pd.set_option("display.width", 210)
    print(f"\n{a.position} season roster% in {lo:.0%}-{hi:.0%}, redraft managed, "
          f"{a.first_year}-{a.last_year}")
    print(f"{len(out)} cells, {out.pid.nunique()} players, {out.year.nunique()} years\n")
    print(f"leagues needed for +/-{a.margin:.0%} @ {a.conf:.0%}:")
    print(out[["n_empirical", "n_stabilise", "n_analytic", "sd", "rate", "breadth", "depth"]]
          .describe(percentiles=[.1, .25, .5, .75, .9])
          .loc[["min", "10%", "25%", "50%", "75%", "90%", "max"]].round(2).to_string())

    mid = (lo + hi) / 2
    t = out[(out.rate.between(mid - .02, mid + .02)) & (out.n_elig >= a.thick)]
    print(f"\nTHE FLOOR -- {mid:.0%} players (+/-2pts) in cohorts of {a.thick}+: "
          f"{len(t)} cells, {t.pid.nunique()} players")
    if len(t):
        print(f"  median {t.n_empirical.median():.0f}   p90 {t.n_empirical.quantile(.9):.0f}"
              f"   worst {t.n_empirical.max():.0f}   sd {t.sd.median():.3f}")
        tt = t.copy()
        tt["tier"] = tt.cohort.str.split("|").str[0]
        print("\n  by teams tier (thin tiers are a warning, not a result):")
        print(tt.groupby("tier").agg(cells=("rate", "size"), need=("n_empirical", "median"),
                                     sd=("sd", "median")).round(2).to_string())
    print("\nby year:")
    print(out.groupby("year").agg(cells=("pid", "size"), players=("pid", "nunique"),
                                  need_med=("n_empirical", "median"),
                                  stab_med=("n_stabilise", "median")).round(1).to_string())


if __name__ == "__main__":
    main()
