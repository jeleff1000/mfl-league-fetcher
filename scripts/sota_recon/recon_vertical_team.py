"""
sota_recon/recon_vertical_team.py  --  WS4a vertical contracts: Σ players == team line.

pfr_box_team_stats holds per-game team stat lines 1920-2025 as key-value rows with
compound values ("Rush-Yds-TDs" = "30-160-2", "Cmp-Att-Yd-TD-INT"). For every game side:

    Σ v26 player atoms (grouped by franchise number)  ==  the team box line

A shortfall = missing players / missing cells (completion signal); an excess = duplicated
or misattributed rows. This is the localization instrument for the 1950-64 PASS-OVER
cluster: per (team, year) deltas name exactly which franchise-years carry the excess.

Families: pass (cmp/att/yds/td/int), rush (att/yds/td). Side -> franchise via
team_games (boxscore_id + is_home); v26 rows group on nfl_franchise_number
(never abbrev). Games where the v26 side has ZERO rows are counted separately
(missing-game class, already queued by other lanes).

READ-ONLY. Outputs under sota_recon_master/vertical_team/.

    python -m scripts.sota_recon.recon_vertical_team
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import sources as S

OUT_DIR = os.path.join(S.DATA_LAKE, "derived", "validation", "sota_recon_master",
                       "vertical_team")

# team-stat row label -> (family, [part names in order])
COMPOUND = {
    "Rush-Yds-TDs": ("rush", ["att", "yds", "td"]),
    "Cmp-Att-Yd-TD-INT": ("pass", ["cmp", "att", "yds", "td", "int"]),
}
# family part -> v26 column
V26 = {
    ("rush", "att"): "carries", ("rush", "yds"): "rushing_yards",
    ("rush", "td"): "rushing_tds",
    ("pass", "cmp"): "completions", ("pass", "att"): "attempts",
    ("pass", "yds"): "passing_yards", ("pass", "td"): "passing_tds",
    ("pass", "int"): "passing_interceptions",
}


def run() -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    os.makedirs(OUT_DIR, exist_ok=True)
    ts = Path(S.registry()["pfr_box_team_stats"].path).as_posix()
    tg = Path(S.TEAM_GAMES.path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()

    labels = ", ".join(f"'{k}'" for k in COMPOUND)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE team_line AS
        WITH kv AS (
          SELECT boxscore_id, stat, vis_stat, home_stat
          FROM '{ts}' WHERE stat IN ({labels})),
        sides AS (
          SELECT boxscore_id, stat, home_stat AS val, TRUE AS is_home FROM kv
          UNION ALL
          SELECT boxscore_id, stat, vis_stat, FALSE FROM kv),
        g AS (
          SELECT boxscore_id, team_fid, year, week, season_type,
                 COALESCE(is_home, FALSE) AS is_home
          FROM '{tg}')
        SELECT s.boxscore_id, g.team_fid, g.year, g.week, g.season_type, s.stat,
               string_split(s.val, '-') AS parts
        FROM sides s JOIN g ON g.boxscore_id = s.boxscore_id
                  AND g.is_home = s.is_home
        WHERE s.val IS NOT NULL AND s.val != ''""")

    # explode compounds into (boxscore, fid, family, part, team_val)
    rows = []
    for label, (fam, parts) in COMPOUND.items():
        for i, part in enumerate(parts):
            rows.append(f"""
            SELECT boxscore_id, team_fid, year, week, season_type,
                   '{fam}' AS family, '{part}' AS part,
                   TRY_CAST(parts[{i + 1}] AS DOUBLE) AS team_val
            FROM team_line WHERE stat = '{label}'""")
    con.execute("CREATE OR REPLACE TEMP TABLE team_atoms AS " + " UNION ALL ".join(rows))

    v26_sums = ", ".join(
        f"SUM({col}) AS v_{fam}_{part}" for (fam, part), col in V26.items())
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE v_team AS
        SELECT nfl_franchise_number AS team_fid, year, CAST(week AS INT) AS week,
               season_type, COUNT(*) AS n_rows, {v26_sums}
        FROM '{v26}'
        GROUP BY 1, 2, 3, 4""")

    con.execute("""
        CREATE OR REPLACE TEMP TABLE verdicts AS
        SELECT t.*, v.n_rows,
               CASE t.family || '_' || t.part
                 """ + "\n                 ".join(
                     f"WHEN '{fam}_{part}' THEN v.v_{fam}_{part}"
                     for (fam, part) in V26) + """
               END AS v26_sum
        FROM team_atoms t
        LEFT JOIN v_team v ON v.team_fid = t.team_fid AND v.year = t.year
          AND v.week = CAST(t.week AS INT) AND v.season_type = t.season_type""")

    summary = con.execute("""
        SELECT family, part,
               COUNT(*) AS sides,
               COUNT(*) FILTER (WHERE n_rows IS NULL) AS no_v26_side,
               COUNT(*) FILTER (WHERE v26_sum IS NOT NULL
                                 AND ABS(v26_sum - team_val) <= 1.5) AS match,
               COUNT(*) FILTER (WHERE v26_sum IS NOT NULL
                                 AND v26_sum - team_val > 1.5) AS players_over,
               COUNT(*) FILTER (WHERE v26_sum IS NOT NULL
                                 AND team_val - v26_sum > 1.5) AS players_under
        FROM verdicts WHERE team_val IS NOT NULL
        GROUP BY 1, 2 ORDER BY 1, 2""").fetchall()

    # the 1950-64 drill: per (year) and per (team_fid, year) pass-yard excess
    year_drill = con.execute("""
        SELECT year,
               COUNT(*) FILTER (WHERE v26_sum - team_val > 1.5) AS over_sides,
               COUNT(*) FILTER (WHERE team_val - v26_sum > 1.5) AS under_sides,
               ROUND(SUM(GREATEST(v26_sum - team_val, 0)), 0) AS excess_yds
        FROM verdicts
        WHERE family = 'pass' AND part = 'yds'
          AND team_val IS NOT NULL AND v26_sum IS NOT NULL
        GROUP BY 1 HAVING over_sides > 0 ORDER BY 1""").fetchall()
    team_drill = con.execute("""
        SELECT team_fid, year,
               COUNT(*) FILTER (WHERE v26_sum - team_val > 1.5) AS over_sides,
               ROUND(SUM(GREATEST(v26_sum - team_val, 0)), 0) AS excess_yds
        FROM verdicts
        WHERE family = 'pass' AND part = 'yds' AND year BETWEEN 1950 AND 1964
          AND team_val IS NOT NULL AND v26_sum IS NOT NULL
        GROUP BY 1, 2 HAVING over_sides > 0
        ORDER BY excess_yds DESC""").fetchall()

    con.execute(f"""
        COPY (SELECT * FROM verdicts
              WHERE team_val IS NOT NULL AND v26_sum IS NOT NULL
                AND ABS(v26_sum - team_val) > 1.5)
        TO '{Path(os.path.join(OUT_DIR, "vertical_diffs.csv")).as_posix()}' (HEADER)""")

    out = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
           "per_part": [dict(family=f, part=p, sides=s, no_v26_side=nv,
                             match=m, players_over=po, players_under=pu)
                        for f, p, s, nv, m, po, pu in summary],
           "pass_yds_over_years": [(int(y), int(o), int(u), int(e))
                                   for y, o, u, e in year_drill],
           "pass_yds_1950_64_team_years": [(int(f), int(y), int(o), int(e))
                                           for f, y, o, e in team_drill][:40]}
    with open(os.path.join(OUT_DIR, "VERTICAL_TEAM_SUMMARY.json"), "w",
              encoding="utf-8") as f:
        json.dump(out, f, indent=1)
    con.close()
    return out


def drill_over(y0: int = 1950, y1: int = 1964) -> dict:
    """0d drill: decompose each (team, year) v26 pass-yard season sum into team-weeks
    COVERED by team-stats lines vs UNCOVERED, and compare covered sums with the team
    lines. The vertical lane proved covered games are clean -- so the league-year OVER
    must live in the uncovered remainder; this names those team-weeks."""
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    ts = Path(S.registry()["pfr_box_team_stats"].path).as_posix()
    tg = Path(S.TEAM_GAMES.path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cov AS
        SELECT DISTINCT g.team_fid, g.year, CAST(g.week AS INT) AS week, g.season_type
        FROM '{tg}' g
        JOIN '{ts}' t ON t.boxscore_id = g.boxscore_id AND t.stat = 'Cmp-Att-Yd-TD-INT'
        """)
    rows = con.execute(f"""
        WITH v AS (
          SELECT nfl_franchise_number AS fid, year, CAST(week AS INT) AS week,
                 season_type, SUM(passing_yards) AS py, COUNT(*) AS n_rows
          FROM '{v26}'
          WHERE year BETWEEN {y0} AND {y1} AND season_type = 'REG'
            AND passing_yards IS NOT NULL
          GROUP BY 1, 2, 3, 4)
        SELECT v.fid, v.year,
               SUM(v.py) FILTER (WHERE c.team_fid IS NOT NULL) AS covered_yds,
               SUM(v.py) FILTER (WHERE c.team_fid IS NULL) AS uncovered_yds,
               COUNT(*) FILTER (WHERE c.team_fid IS NULL) AS uncovered_weeks,
               array_agg(v.week ORDER BY v.week)
                 FILTER (WHERE c.team_fid IS NULL) AS uncovered_week_list
        FROM v LEFT JOIN cov c ON c.team_fid = v.fid AND c.year = v.year
          AND c.week = v.week AND c.season_type = v.season_type
        GROUP BY 1, 2
        HAVING uncovered_yds > 0
        ORDER BY uncovered_yds DESC""").fetchall()
    out_rows = [dict(team_fid=int(f), year=int(y),
                     covered_yds=int(c or 0), uncovered_yds=int(u or 0),
                     uncovered_weeks=int(w), weeks=list(wl or []))
                for f, y, c, u, w, wl in rows]
    import csv
    p = os.path.join(OUT_DIR, f"over_drill_uncovered_{y0}_{y1}.csv")
    with open(p, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["team_fid", "year", "covered_yds",
                                           "uncovered_yds", "uncovered_weeks", "weeks"])
        w.writeheader()
        w.writerows(out_rows)
    con.close()
    return {"team_years_with_uncovered_pass_yds": len(out_rows),
            "total_uncovered_yds": sum(r["uncovered_yds"] for r in out_rows),
            "top": out_rows[:15], "csv": p}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--drill", action="store_true",
                    help="run the 1950-64 OVER decomposition instead of the contracts")
    a = ap.parse_args()
    if a.drill:
        d = drill_over()
        print(f"team-years with UNCOVERED pass yds: {d['team_years_with_uncovered_pass_yds']}"
              f"  total uncovered: {d['total_uncovered_yds']:,} yds")
        for r in d["top"]:
            print(f"  fid={r['team_fid']:>3} {r['year']}  covered={r['covered_yds']:>6,}"
                  f"  UNCOVERED={r['uncovered_yds']:>6,} over {r['uncovered_weeks']} wks {r['weeks']}")
        raise SystemExit(0)
    r = run()
    print(f"{'family':6s} {'part':5s} {'sides':>8s} {'match':>8s} {'over':>6s} {'under':>7s} {'no-v26':>7s}")
    for p in r["per_part"]:
        print(f"{p['family']:6s} {p['part']:5s} {p['sides']:>8,} {p['match']:>8,} "
              f"{p['players_over']:>6,} {p['players_under']:>7,} {p['no_v26_side']:>7,}")
    ys = r["pass_yds_over_years"]
    print(f"\npass-yds OVER years (yr, over_sides, under_sides, excess_yds): {ys[:20]}")
    print(f"top 1950-64 team-year excess: {r['pass_yds_1950_64_team_years'][:15]}")
