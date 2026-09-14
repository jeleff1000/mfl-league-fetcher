"""PLAYERS MUST AGGREGATE TO TEAMS, AND PLAYERS MUST MATCH THEMSELVES.

Joe, 2026-07-31: "players should agg to match teams and players should match themselves.
that's part of the wiring we want."

Two conservation questions the column audit never asks, because its subject is one player's
one column:

  VERTICAL   the team-DEF row == the SUM of that team's IDP rows, per team-week
  INTERNAL   a player's own columns reconcile -- combined == solo + assists

WHY THE COLUMN AUDIT CANNOT SEE THIS. Every nflcom measurement runs through
slug -> pfr_id -> NFL_player_id, and of 34,975 team-DEF rows ZERO carry a pfr_id. The team
plane is structurally invisible to it -- which is correct for a player-subject audit and
means "200 columns gated" says nothing about the 19-81% of defensive material living on team
rows.

WHAT THIS FOUND. Conservation is not uniformly broken; it splits sharply, and the split is
the finding:

    def_fumbles_forced   99.7-100.0%   holds in every era
    def_interceptions     95.4-99.9%   holds
    def_pass_defended          99.3%   holds (2010+, the only era it exists)
    def_tackle_assists         96.4%   holds
    def_sacks            96.5-98.3% modern, 54.1% pre-1994
    def_safeties           2.0-64.7%   IDP always SHORT
    def_tackles_solo           17.2%   IDP OVER on 4,216 of 6,024 team-weeks

A HYPOTHESIS THAT DIED HERE, recorded so it is not retried: the tackles_solo overshoot looked
like the offensive-lineman tail found earlier the same day (we credit tackles to linemen after
turnovers, and they sit on the non-DEF plane). Restricting the IDP side to DB/LB/DL makes it
WORSE, 17.2% -> 11.3%, and the tail is only 7,682 tackles at a median of 1 per team-week. The
team row is somewhere between the two populations and matches neither.

    python -m scripts.sota_recon.conservation_audit
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from . import sources as S

LEDGER = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
          r"\conservation_receipts.json")

#: Every defensive canonical nflcom witnesses on the PLAYER plane. If a column is audited
#: per-player it must also reconcile to its team, or the two planes disagree silently.
VERTICAL = ["def_tackles_solo", "def_tackle_assists", "def_tackles_combined", "def_sacks",
            "def_interceptions", "def_pass_defended", "def_safeties", "def_fumbles_forced"]

#: (label, lhs, rhs...) checked WITHIN a plane -- a player against himself.
INTERNAL = [("combined_eq_solo_plus_assists", "def_tackles_combined",
             ("def_tackles_solo", "def_tackle_assists"))]

ERAS = ("CASE WHEN yr < 1994 THEN '<1994' WHEN yr < 2000 THEN '1994-99' "
        "WHEN yr < 2010 THEN '2000-09' ELSE '2010+' END")


def build() -> dict:
    v26 = Path(S.latest_v26()).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='6GB'")
    con.execute(f"""CREATE TABLE cons AS SELECT year yr, week wk, nfl_team tm,
        {', '.join(f"SUM({c}) FILTER (WHERE position='DEF') AS t_{c}" for c in VERTICAL)},
        {', '.join(f"SUM({c}) FILTER (WHERE position<>'DEF') AS i_{c}" for c in VERTICAL)}
        FROM read_parquet('{v26}') WHERE season_type='REG' AND nfl_team IS NOT NULL
        GROUP BY 1, 2, 3""")
    rows = []
    for c in VERTICAL:
        for era, n, pct, short, over in con.execute(f"""SELECT {ERAS} AS era,
              COUNT(*) FILTER (WHERE t_{c} > 0) AS n_rows,
              ROUND(100.0*COUNT(*) FILTER (WHERE t_{c} = i_{c} AND t_{c} > 0)
                    / NULLIF(COUNT(*) FILTER (WHERE t_{c} > 0), 0), 4) AS pct,
              COUNT(*) FILTER (WHERE t_{c} > COALESCE(i_{c}, 0)) AS idp_short,
              COUNT(*) FILTER (WHERE COALESCE(i_{c}, 0) > t_{c}) AS idp_over
              FROM cons GROUP BY 1 ORDER BY 1""").fetchall():
            if not n:
                continue
            rows.append({"key": f"vertical|{c}|{era}", "kind": "VERTICAL", "canonical": c,
                         "era": era, "n": n, "agree_pct": round((pct or 0) / 100, 4),
                         "idp_short": short, "idp_over": over,
                         # a one-sided residual is a coverage story, a two-sided one a value story
                         "one_sided": (short == 0) != (over == 0)})
    con.execute(f"""CREATE TABLE ply AS SELECT year yr, NFL_player_id pid, position pos,
        {', '.join(f"SUM({c}) AS {c}" for c in
                   ("def_tackles_combined", "def_tackles_solo", "def_tackle_assists"))}
        FROM read_parquet('{v26}') WHERE season_type='REG' GROUP BY 1, 2, 3""")
    for lab, lhs, rhs in INTERNAL:
        for plane, pred in (("idp_rows", "pos <> 'DEF'"), ("team_def_row", "pos = 'DEF'")):
            for era, n, pct in con.execute(f"""SELECT {ERAS} AS era,
                  COUNT(*) FILTER (WHERE {lhs} > 0) AS n_rows,
                  ROUND(100.0*COUNT(*) FILTER (WHERE {lhs} = {' + '.join(rhs)} AND {lhs} > 0)
                        / NULLIF(COUNT(*) FILTER (WHERE {lhs} > 0), 0), 4) AS pct
                  FROM ply WHERE {pred} GROUP BY 1 ORDER BY 1""").fetchall():
                if not n:
                    continue
                rows.append({"key": f"internal|{lab}|{plane}|{era}", "kind": "INTERNAL",
                             "identity": f"{lhs} = {' + '.join(rhs)}", "plane": plane,
                             "era": era, "n": n, "agree_pct": round((pct or 0) / 100, 4)})
    con.close()
    return {"rows": rows}


def main() -> int:
    rep = build()
    print(f"{'check':46s} {'n':>8s} {'agree':>8s}  note")
    for r in rep["rows"]:
        note = ""
        if r["kind"] == "VERTICAL":
            note = ("IDP short" if r["idp_short"] > r["idp_over"] else "IDP over")
            note += " (one-sided)" if r["one_sided"] else ""
        print(f"{r['key']:46s} {r['n']:>8,} {r['agree_pct']:>8.4f}  {note}")
    Path(LEDGER).write_text(json.dumps(rep, indent=1, default=str), encoding="utf-8")
    print(f"\nreceipts -> {LEDGER}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
