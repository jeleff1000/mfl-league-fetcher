"""
sota_recon/recon_season_authority.py  --  v26 vs the published record book (season totals)

The arbiter lane. PFR player-PAGE season tables are the authoritative published season
totals; this checks whether v26's REG weekly rows sum to them, per era. It is the answer
to "is v26 as complete as the record book?" -- distinct from the oracle lane (game-level
PBP corroboration) and the box-score backfill (game-level source).

Findings this encodes (2026-06-19):
  1978-2024  RUSH/PASS/RECV ~100%   -> v26 IS the record book.
  1950-1977  ~99-100%               -> complete within rounding.
  1946-1949  ~92-98%               -> AAFC-era game/season slack.
  1932-1945  RUSH 90 / PASS 89 / RECV 80%  -> the historical record's OWN game-vs-season
             gap: early box scores never captured all production (esp. receiving). NOT a
             v26 defect and NOT closeable at game level -- the data does not exist.

Mechanics that matter (get these wrong and the % is garbage):
  - rush & rec are split across two player-page tables by the page's primary position;
    UNION rushing_and_receiving + receiving_and_rushing for a league-complete total.
  - multi-team seasons carry a 'NTM' (2TM/3TM...) combined row; use it, never sum stints.
  - compare REG to REG (player-page main tables are regular season; v26 must filter REG).

    python -m scripts.sota_recon.recon_season_authority   ->  per-era %, PASS/RESIDUAL
"""

from __future__ import annotations

import re

import pandas as pd
import pyarrow.parquet as pq

from .sources import SEASON_AUTH_PASS, SEASON_AUTH_RUSHREC, SEASON_AUTH_RECRUSH, latest_v26

_TM = re.compile(r"^\dTM$")
# era -> (min completeness % we EXPECT). Below the floor for 1950+ would be a real regression;
# the pre-1950 floors encode the documented historical game-vs-season gap as accepted.
ERA_FLOORS = {
    (1978, 2024): 99.0,
    (1950, 1977): 98.0,
    (1946, 1949): 90.0,
    (1932, 1945): 78.0,
}


def _load_season(path: str, cols: list[str]) -> pd.DataFrame:
    d = pq.read_table(path, columns=["pfr_id", "year_id", "team_name_abbr"] + cols).to_pandas()
    d["yr"] = pd.to_numeric(d.year_id.astype(str).str.extract(r"(\d{4})")[0], errors="coerce")
    for c in cols:
        d[c] = pd.to_numeric(d[c], errors="coerce").fillna(0)
    d = d.dropna(subset=["yr"])
    d["yr"] = d["yr"].astype(int)
    d = d[d.team_name_abbr.str.len() > 0]
    d["is_tot"] = d.team_name_abbr.str.match(_TM).fillna(False)
    return d


def _season_totals(d: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Per (pfr_id, yr): use the multi-team combined row if present, else sum stint rows."""
    tot = d[d.is_tot].copy()
    keys = set(zip(tot.pfr_id, tot.yr))
    rest = d[~d.is_tot].copy()
    rest["k"] = list(zip(rest.pfr_id, rest.yr))
    rest = rest[~rest.k.isin(keys)]
    return (pd.concat([tot, rest], ignore_index=True)
            .groupby(["pfr_id", "yr"], as_index=False)[cols].sum())


def authoritative_by_year() -> pd.DataFrame:
    rr = _load_season(SEASON_AUTH_RUSHREC.path, ["rush_yds", "rec_yds"])
    ar = _load_season(SEASON_AUTH_RECRUSH.path, ["rush_yds", "rec_yds"])
    ru = (pd.concat([_season_totals(rr, ["rush_yds", "rec_yds"]),
                     _season_totals(ar, ["rush_yds", "rec_yds"])])
          .groupby("yr")[["rush_yds", "rec_yds"]].sum())
    pa = _season_totals(_load_season(SEASON_AUTH_PASS.path, ["pass_yds"]), ["pass_yds"]).groupby("yr").pass_yds.sum()
    return pd.DataFrame({"rush": ru.rush_yds, "recv": ru.rec_yds, "pass": pa}).fillna(0)


def run() -> dict:
    auth = authoritative_by_year()
    v = pq.read_table(latest_v26(), columns=["year", "season_type", "rushing_yards",
                                             "passing_yards", "receiving_yards"]).to_pandas()
    v = v[v.season_type == "REG"]
    vg = v.groupby("year")[["rushing_yards", "passing_yards", "receiving_yards"]].sum()

    results, passed = [], 0
    for (lo, hi), floor in ERA_FLOORS.items():
        for label, acol, vcol in [("RUSH", "rush", "rushing_yards"),
                                   ("PASS", "pass", "passing_yards"),
                                   ("RECV", "recv", "receiving_yards")]:
            a = auth[acol][(auth.index >= lo) & (auth.index <= hi)].sum()
            vv = vg[vcol][(vg.index >= lo) & (vg.index <= hi)].sum()
            pct = round(100 * vv / a, 1) if a else 0.0
            ok = pct >= floor
            results.append((f"{lo}-{hi}", label, pct, floor, ok))
            passed += ok
    return {"total": len(results), "passed": passed, "results": results}


if __name__ == "__main__":
    r = run()
    print(f"{'ERA':<11} {'STAT':<5} {'v26/AUTH':>9}  FLOOR  STATUS")
    for era, stat, pct, floor, ok in r["results"]:
        print(f"{era:<11} {stat:<5} {pct:>8.1f}%  {floor:>4.0f}%  {'PASS' if ok else 'BELOW'}")
    print(f"\n{r['passed']}/{r['total']} era-stat checks at/above floor "
          "(pre-1950 floors encode the documented historical game-vs-season gap)")
