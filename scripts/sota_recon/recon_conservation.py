"""
sota_recon/recon_conservation.py  --  LANE: league-year conservation totals (WS7c).

Sum over ALL players of a stat in a year must equal the independent authority total for
that year. Player-grain lanes cannot see a player who is entirely MISSING (no row to
compare), and season_authority's era floors only check >= (undercount) -- so OVERCOUNT
(doubleheader double-counts, duplicated rows) passes today. This lane checks per YEAR in
BOTH directions.

Authority = the PFR player-page season tables already in the lake (reuses
recon_season_authority's loaders). Tolerance is era-aware: modern years must reconcile
tightly; pre-1950 has documented sourcing slack (the asymmetry registry owns the reasons).

    python -m scripts.sota_recon.recon_conservation [--csv out.csv]
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from .recon_season_authority import authoritative_by_year
from .sources import latest_v26

STATS = [  # (label, authority col, v26 weekly col)
    ("PASS", "pass", "passing_yards"),
    ("RUSH", "rush", "rushing_yards"),
    ("RECV", "recv", "receiving_yards"),
]

# |delta| as share of authority total allowed per era, both directions
ERA_TOL = [(1920, 1945, 0.20), (1946, 1949, 0.08), (1950, 1977, 0.02),
           (1978, 2100, 0.005)]


def _tol(year: int) -> float:
    for lo, hi, t in ERA_TOL:
        if lo <= year <= hi:
            return t
    return 0.005


# AUTHORITY-COVERAGE RULE (2026-07-11, the 1950-64 "OVER" postmortem): the pages
# authority only carries players WITH a row in that stat's season table. v26
# legitimately holds yardage for players the table omits (halfback option passes,
# fake-punt throws) -- verified at game grain by recon_vertical_team, and v26
# restricted to covered players matched the authority TO THE YARD (1952/56/62
# exact). So the conservation comparison runs on the COVERED subset; the uncovered
# residual is reported per year (box-witnessed, informational, never a flag).


def _membership_sql(label: str) -> str:
    """(pfr_id, yr) pairs the authority actually carries for a stat family."""
    from . import sources as S
    def q(key):
        return Path(S.registry()[key].path).as_posix()
    yr = "TRY_CAST(regexp_extract(CAST(year_id AS VARCHAR), '(\\d{4})', 1) AS INT)"
    if label == "PASS":
        return (f"SELECT DISTINCT pfr_id, {yr} AS yr "
                f"FROM '{q('pfr_player_season_passing')}' WHERE pass_yds IS NOT NULL")
    col = "rush_yds" if label == "RUSH" else "rec_yds"
    return (f"SELECT DISTINCT pfr_id, {yr} AS yr FROM ("
            f"SELECT pfr_id, year_id, {col} FROM '{q('pfr_player_season_rush_rec')}' "
            f"UNION ALL SELECT pfr_id, year_id, {col} "
            f"FROM '{q('pfr_player_season_rec_rush')}') WHERE {col} IS NOT NULL")


def run(src: str | None = None, csv: str | None = None) -> dict:
    from .sources import PLAYER_BIO
    vq = Path(src or latest_v26()).as_posix()
    bio = Path(PLAYER_BIO.path).as_posix()
    auth = authoritative_by_year()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    per_label: dict[str, dict[int, tuple[float, float]]] = {}
    for label, _, vcol in STATS:
        per_label[label] = {
            int(y): (float(cov or 0), float(unc or 0))
            for y, cov, unc in con.execute(f"""
                WITH mem AS ({_membership_sql(label)})
                SELECT t.year,
                       SUM(t.{vcol}) FILTER (WHERE mem.pfr_id IS NOT NULL) AS covered,
                       SUM(t.{vcol}) FILTER (WHERE mem.pfr_id IS NULL) AS uncovered
                FROM '{vq}' t
                LEFT JOIN '{bio}' b USING (NFL_player_id)
                LEFT JOIN mem ON mem.pfr_id = b.pfr_id AND mem.yr = t.year
                WHERE t.season_type = 'REG' AND t.{vcol} IS NOT NULL
                GROUP BY 1""").fetchall()}
    con.close()

    years = sorted(set(int(y) for y in auth.index)
                   & set().union(*(d.keys() for d in per_label.values())))
    rows, flags = [], 0
    for year in years:
        a_by_label = {"PASS": auth.loc[year, "pass"], "RUSH": auth.loc[year, "rush"],
                      "RECV": auth.loc[year, "recv"]}
        for label, _, _ in STATS:
            a = float(a_by_label[label])
            cov, unc = per_label[label].get(year, (0.0, 0.0))
            if a <= 0:
                continue
            pct = (cov - a) / a
            flagged = abs(pct) > _tol(year)
            flags += flagged
            rows.append(dict(year=year, stat=label, authority=int(a), v26=int(cov),
                             uncovered_v26=int(unc),
                             delta=int(cov - a), pct=round(100 * pct, 2),
                             tol_pct=round(100 * _tol(year), 2),
                             flag="OVER" if (flagged and pct > 0) else
                                  ("UNDER" if flagged else "")))
    if csv:
        import csv as _csv
        with open(csv, "w", newline="") as f:
            w = _csv.DictWriter(f, fieldnames=list(rows[0]))
            w.writeheader(); w.writerows(rows)
    return {"years": len({r['year'] for r in rows}), "checks": len(rows),
            "flagged": flags, "rows": rows}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", default=None)
    ap.add_argument("--src", default=None)
    a = ap.parse_args()
    r = run(a.src, a.csv)
    print(f"CONSERVATION: {r['checks']} year-stat checks over {r['years']} years, "
          f"{r['flagged']} flagged")
    over = [x for x in r["rows"] if x["flag"] == "OVER"]
    under = [x for x in r["rows"] if x["flag"] == "UNDER"]
    print(f"  OVER (duplication suspects): {len(over)}   UNDER (missing suspects): {len(under)}")
    for x in sorted(r["rows"], key=lambda x: -abs(x["pct"]))[:15]:
        if x["flag"]:
            print(f"  {x['year']} {x['stat']}: v26={x['v26']:,} auth={x['authority']:,} "
                  f"delta={x['delta']:+,} ({x['pct']:+.2f}% vs tol ±{x['tol_pct']}%) {x['flag']}")
