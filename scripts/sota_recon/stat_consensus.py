"""
sota_recon/stat_consensus.py  --  N-witness consensus: count the witnesses, let majority settle disputes

For every atom we enumerate ALL independent source records that can produce it at the
team-game grain, not just a primary + one backup. The NUMBER of witnesses is itself the
signal: an atom backed by 3 independent records that agree is settled; a lone outlier is
the error. Disputes are resolved by majority consensus among the INDEPENDENT witnesses,
then v26 is judged against that consensus.

Witness kinds (all keyed to team-game (year, week, franchise) via nfl_team_games_all):
  self_sum(table,col) : SUM of a player boxscore col over the team's own players
  opp_sum(table,col)  : SUM over the OPPONENT's players, attributed to this (defending) team
                        -> opponent's pass_int IS this team's def interceptions, etc.
  ts_self(field)      : this team's parsed team_stats line (1920+)
  ts_opp(field)       : the opponent's parsed team_stats line, attributed to this team
  tg(col)             : nfl_team_games_all team-game field (team_points/opponent_points)
  (scoring TD-counts and pbp reconstruction are further independent witnesses -> next layer)

These witnesses are INDEPENDENT records (player boxscore vs packed team line vs the opponent's
own books vs the game-result table), so agreement is real corroboration against RECORD-level
defects (transcription, attribution, packing). But they are NOT independent lineage roots:
every one of them is the PFR boxscore scrape -- one bloodline (O.5 lineage collapse,
lineage_roots.v1.json). The `roots` column reports COUNT(DISTINCT root) per atom so the lane
says so honestly: consensus here is intra-root copy-consistency, never cross-root
confirmation. Cross-root confirmation lives in witness_votes / lineage_flip_report.

Verdict per atom-era (over team-games with >=1 witness):
  n_wit        : mean independent witnesses available
  dbl%         : share of team-games with >=2 independent witnesses (can be cross-checked)
  consensus%   : share where the witnesses agree (a settled truth exists)
  v26_ok%      : share where v26 matches that settled truth
  disputes     : team-games where witnesses themselves split (no majority) -> need adjudication

    python -m scripts.sota_recon.stat_consensus
    python -m scripts.sota_recon.stat_consensus --atom def_interceptions --show-bad
"""

from __future__ import annotations

import argparse
from collections import Counter

import duckdb
import pandas as pd

from .sources import latest_v26

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
TOL = 0.5
ERAS = [("pre1950", 1920, 1949), ("1950_77", 1950, 1977),
        ("1978_01", 1978, 2001), ("2002_pl", 2002, 2025)]

# atom -> (v26_col, side, [witness specs]); witness spec = (name, kind, *args)
WITNESS_MAP = {
    "passing_yards": ("passing_yards", "off", [
        ("po", "self_sum", "player_offense", "pass_yds"), ("ts", "ts_self", "ws_pass_yds")]),
    "completions": ("completions", "off", [
        ("po", "self_sum", "player_offense", "pass_cmp"), ("ts", "ts_self", "ws_pass_cmp")]),
    "attempts": ("attempts", "off", [
        ("po", "self_sum", "player_offense", "pass_att"), ("ts", "ts_self", "ws_pass_att")]),
    "passing_tds": ("passing_tds", "off", [
        ("po", "self_sum", "player_offense", "pass_td"), ("ts", "ts_self", "ws_pass_td")]),
    "passing_interceptions": ("passing_interceptions", "off", [
        ("po", "self_sum", "player_offense", "pass_int"), ("ts", "ts_self", "ws_pass_int"),
        ("oppdef", "opp_sum", "player_defense", "def_int")]),
    "sacks_suffered": ("sacks_suffered", "off", [
        ("po", "self_sum", "player_offense", "pass_sacked"), ("ts", "ts_self", "ws_sacks"),
        ("oppdef", "opp_sum", "player_defense", "sacks")]),
    "rushing_yards": ("rushing_yards", "off", [
        ("po", "self_sum", "player_offense", "rush_yds"), ("ts", "ts_self", "ws_rush_yds")]),
    "carries": ("carries", "off", [
        ("po", "self_sum", "player_offense", "rush_att"), ("ts", "ts_self", "ws_rush_att")]),
    "rushing_tds": ("rushing_tds", "off", [
        ("po", "self_sum", "player_offense", "rush_td"), ("ts", "ts_self", "ws_rush_td")]),
    "receiving_yards": ("receiving_yards", "off", [
        ("po", "self_sum", "player_offense", "rec_yds"), ("ts", "ts_self", "ws_pass_yds")]),
    "receptions": ("receptions", "off", [
        ("po", "self_sum", "player_offense", "rec"), ("ts", "ts_self", "ws_pass_cmp")]),
    "fumbles": ("fumbles", "off", [
        ("po", "self_sum", "player_offense", "fumbles"), ("ts", "ts_self", "ws_fumbles")]),
    # defense: opponent's offense + opponent's team_stats line corroborate the player_defense primary
    "def_interceptions": ("def_interceptions", "def", [
        ("pd", "self_sum", "player_defense", "def_int"), ("tsopp", "ts_opp", "ws_pass_int"),
        ("oppoff", "opp_sum", "player_offense", "pass_int")]),
    "def_sacks": ("def_sacks", "def", [
        ("pd", "self_sum", "player_defense", "sacks"), ("tsopp", "ts_opp", "ws_sacks"),
        ("oppoff", "opp_sum", "player_offense", "pass_sacked"),
        ("pbpsk", "pbp", "pbp_sacks", "pbpsk")]),
    "def_fumbles": ("def_fumbles", "def", [
        ("pd", "self_sum", "player_defense", "fumbles_rec"), ("tsopp", "ts_opp", "ws_fumbles_lost")]),
    # kicking / returns: boxscore + scoring (2nd independent witness via score-delta)
    "fg_made": ("fg_made", "off", [("kk", "self_sum", "kicking", "fgm"), ("sc", "scoring", "sc_fg")]),
    "fg_att": ("fg_att", "off", [("kk", "self_sum", "kicking", "fga")]),
    "kickoff_returns": ("kickoff_returns", "off", [
        ("rt", "self_sum", "returns", "kick_ret"), ("pbp", "pbp", "pbp_kr")]),
    "punt_returns": ("punt_returns", "off", [
        ("rt", "self_sum", "returns", "punt_ret"), ("pbp", "pbp", "pbp_pr")]),
    # TD families: boxscore + scoring play-description count (2nd witness, 1920+)
    "passing_tds": ("passing_tds", "off", [
        ("po", "self_sum", "player_offense", "pass_td"), ("ts", "ts_self", "ws_pass_td"),
        ("sc", "scoring", "sc_pass_td")]),
    "rushing_tds": ("rushing_tds", "off", [
        ("po", "self_sum", "player_offense", "rush_td"), ("ts", "ts_self", "ws_rush_td"),
        ("sc", "scoring", "sc_rush_td")]),
    "kickoff_return_tds": ("kickoff_return_tds", "off", [
        ("rt", "self_sum", "returns", "kick_ret_td"), ("sc", "scoring", "sc_kr_td")]),
    "punt_return_tds": ("punt_return_tds", "off", [
        ("rt", "self_sum", "returns", "punt_ret_td"), ("sc", "scoring", "sc_pr_td")]),
    "def_int_ret_td": ("def_int_ret_td", "def", [
        ("pd", "self_sum", "player_defense", "def_int_td"), ("sc", "scoring", "sc_int_ret_td")]),
}


# O.5: lineage root per witness KIND. Every kind in this lane reads the PFR boxscore
# scrape (player tables, packed team lines, scoring log, PFR pbp) -- one root.
WITNESS_KIND_ROOT = {"self_sum": "pfr", "opp_sum": "pfr", "ts_self": "pfr",
                     "ts_opp": "pfr", "scoring": "pfr", "pbp": "pfr"}


def independent_root_count(specs) -> int:
    """independent_witness_count = COUNT(DISTINCT lineage root) across an atom's
    witness specs -- copies never add (O.5, lineage_roots.v1.json)."""
    return len({WITNESS_KIND_ROOT[s[1]] for s in specs})


def _ws(con):
    TS = f"read_parquet('{BOX}/team_stats/_combined.parquet')"
    con.execute(f"""CREATE TEMP TABLE tg AS SELECT boxscore_id, team_code,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr, opponent_fid ofr, is_home,
        team_points, opponent_points FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE TEMP TABLE tsw AS
        WITH ts AS (SELECT boxscore_id, stat, vis_stat, home_stat FROM {TS}),
        joined AS (SELECT g.yr, g.wk, g.fr, ts.stat,
                     CASE WHEN g.is_home THEN ts.home_stat ELSE ts.vis_stat END v
                   FROM tg g JOIN ts ON ts.boxscore_id=g.boxscore_id)
        SELECT yr, wk, fr,
          MAX(CASE WHEN stat='Cmp-Att-Yd-TD-INT' AND len(string_split(v,'-'))=5 THEN TRY_CAST(string_split(v,'-')[1] AS INT) END) ws_pass_cmp,
          MAX(CASE WHEN stat='Cmp-Att-Yd-TD-INT' AND len(string_split(v,'-'))=5 THEN TRY_CAST(string_split(v,'-')[2] AS INT) END) ws_pass_att,
          MAX(CASE WHEN stat='Cmp-Att-Yd-TD-INT' AND len(string_split(v,'-'))=5 THEN TRY_CAST(string_split(v,'-')[3] AS INT) END) ws_pass_yds,
          MAX(CASE WHEN stat='Cmp-Att-Yd-TD-INT' AND len(string_split(v,'-'))=5 THEN TRY_CAST(string_split(v,'-')[4] AS INT) END) ws_pass_td,
          MAX(CASE WHEN stat='Cmp-Att-Yd-TD-INT' AND len(string_split(v,'-'))=5 THEN TRY_CAST(string_split(v,'-')[5] AS INT) END) ws_pass_int,
          MAX(CASE WHEN stat='Rush-Yds-TDs' AND len(string_split(v,'-'))=3 THEN TRY_CAST(string_split(v,'-')[1] AS INT) END) ws_rush_att,
          MAX(CASE WHEN stat='Rush-Yds-TDs' AND len(string_split(v,'-'))=3 THEN TRY_CAST(string_split(v,'-')[2] AS INT) END) ws_rush_yds,
          MAX(CASE WHEN stat='Rush-Yds-TDs' AND len(string_split(v,'-'))=3 THEN TRY_CAST(string_split(v,'-')[3] AS INT) END) ws_rush_td,
          MAX(CASE WHEN stat='Sacked-Yards' AND len(string_split(v,'-'))=2 THEN TRY_CAST(string_split(v,'-')[1] AS INT) END) ws_sacks,
          MAX(CASE WHEN stat IN ('Fumbles-Lost','Fumbles Lost') AND len(string_split(v,'-'))=2 THEN TRY_CAST(string_split(v,'-')[1] AS INT) END) ws_fumbles,
          MAX(CASE WHEN stat IN ('Fumbles-Lost','Fumbles Lost') AND len(string_split(v,'-'))=2 THEN TRY_CAST(string_split(v,'-')[2] AS INT) END) ws_fumbles_lost
        FROM joined GROUP BY 1,2,3""")


def _scoring_witness(con):
    """Build scw(yr,wk,fr, sc_*) from the scoring table: attribute each scoring play to a team via
    the running-score delta (which side's total rose) joined to team_games is_home, then classify
    the play from its description -> per-team-game counts of TD types / FG / PAT / safety."""
    SC = f"read_parquet('{BOX}/scoring/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scd AS
        SELECT boxscore_id, lower(COALESCE(description,'')) d, row_index_in_table ri,
          TRY_CAST(vis_team_score AS INT) - LAG(TRY_CAST(vis_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dvis,
          TRY_CAST(home_team_score AS INT) - LAG(TRY_CAST(home_team_score AS INT),1,0)
             OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dhome
        FROM {SC} WHERE description IS NOT NULL AND description<>''""")
    # side: which team scored this play (home if home total rose, else visitor)
    con.execute("""CREATE OR REPLACE TEMP TABLE scs AS
        SELECT boxscore_id, d, (dhome > 0 AND dhome >= dvis) is_home FROM scd
        WHERE dvis > 0 OR dhome > 0""")
    con.execute("""CREATE OR REPLACE TEMP TABLE scw AS
        SELECT g.yr, g.wk, g.fr,
          SUM(CASE WHEN d LIKE '%interception return%' OR d LIKE '%fumble return%'
                    OR (d LIKE '%fumble recovery%' AND d LIKE '%end zone%') THEN 1 ELSE 0 END) sc_def_td,
          SUM(CASE WHEN d LIKE '%kickoff return%' OR d LIKE '%punt return%'
                    OR d LIKE '%blocked%return%' OR d LIKE '%missed field goal return%' THEN 1 ELSE 0 END) sc_st_td,
          SUM(CASE WHEN d LIKE 'safety%' OR d LIKE '%safety,%' THEN 1 ELSE 0 END) sc_safety,
          SUM(CASE WHEN d LIKE '%field goal%' THEN 1 ELSE 0 END) sc_fg,
          SUM(CASE WHEN d LIKE '%kickoff return%' THEN 1 ELSE 0 END) sc_kr_td,
          SUM(CASE WHEN d LIKE '%punt return%' THEN 1 ELSE 0 END) sc_pr_td,
          SUM(CASE WHEN d LIKE '%interception return%' THEN 1 ELSE 0 END) sc_int_ret_td,
          SUM(CASE WHEN d LIKE '%fumble return%'
                    OR (d LIKE '%fumble recovery%' AND d LIKE '%end zone%') THEN 1 ELSE 0 END) sc_fum_ret_td,
          SUM(CASE WHEN d LIKE '%pass from%' THEN 1 ELSE 0 END) sc_pass_td,
          SUM(CASE WHEN d LIKE '%yard rush%' OR d LIKE '% run (%' THEN 1 ELSE 0 END) sc_rush_td
        FROM scs JOIN tg g ON scs.boxscore_id=g.boxscore_id AND scs.is_home=g.is_home
        GROUP BY 1,2,3""")


def _pbp_witness(con):
    """Build pbpw(yr,wk,fr, pbp_kr, pbp_pr) from play-by-play (1966+): count kickoff/punt returns
    per RETURNING team. Team attribution is independent of the returns table: the kicker (1st
    detail link) is in the kicking source -> kicking team -> returning team = its opponent.
    EXCLUDES accepted-penalty-voided plays: lower(detail) LIKE '%no play%' (the official-stat rule)."""
    PBP = f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute("CREATE OR REPLACE TEMP TABLE bx AS SELECT DISTINCT boxscore_id, yr, wk FROM tg")
    # per-boxscore roster: (boxscore_id, pfr_id) -> franchise, from the UNION of all 4 boxscore
    # tables (a player named in a play is attributed to whichever team he appears under here).
    parts = []
    for t in ["player_offense", "player_defense", "kicking", "returns"]:
        parts.append(f"SELECT s.boxscore_id, split_part(s.player_link_ids,';',1) pid, g.fr, g.ofr "
                     f"FROM read_parquet('{BOX}/{t}/_combined.parquet') s JOIN tg g "
                     f"ON s.boxscore_id=g.boxscore_id AND s.team=g.team_code WHERE s.player_link_ids IS NOT NULL")
    con.execute("CREATE OR REPLACE TEMP TABLE pid_team AS SELECT DISTINCT boxscore_id, pid, fr, ofr FROM ("
                + " UNION ALL ".join(parts) + ")")
    # RETURNS: returner = 2nd link -> roster franchise; fallback kicker(1st)->opponent. (no play) excluded.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbpw AS
        WITH p AS (
            SELECT boxscore_id, split_part(detail_link_ids,';',1) kicker,
                   split_part(detail_link_ids,';',2) returner,
                   CASE WHEN detail LIKE '%kicks off%' THEN 1 ELSE 0 END is_ko,
                   CASE WHEN detail LIKE '%punts%' THEN 1 ELSE 0 END is_punt
            FROM {PBP}
            WHERE detail LIKE '%returned by%' AND lower(detail) NOT LIKE '%no play%'
              AND detail_link_ids IS NOT NULL),
        attr AS (
            SELECT p.boxscore_id, p.is_ko, p.is_punt,
                   COALESCE(rt.fr, kk.ofr) fr   -- returner's team, else kicker's opponent
            FROM p LEFT JOIN pid_team rt ON p.boxscore_id=rt.boxscore_id AND p.returner=rt.pid
                   LEFT JOIN pid_team kk ON p.boxscore_id=kk.boxscore_id AND p.kicker=kk.pid)
        SELECT b.yr, b.wk, a.fr, SUM(a.is_ko) pbp_kr, SUM(a.is_punt) pbp_pr
        FROM attr a JOIN bx b ON b.boxscore_id=a.boxscore_id
        WHERE a.fr IS NOT NULL GROUP BY 1,2,3""")
    # SACKS: sacker = 2nd link in "QB sacked by SACKER" -> roster franchise (the DEFENDING team).
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbpsk AS
        WITH p AS (
            SELECT boxscore_id, split_part(detail_link_ids,';',2) sacker
            FROM {PBP}
            WHERE detail LIKE '%sacked by%' AND lower(detail) NOT LIKE '%no play%'
              AND detail_link_ids IS NOT NULL)
        SELECT b.yr, b.wk, pt.fr, COUNT(*) pbp_sacks
        FROM p JOIN pid_team pt ON p.boxscore_id=pt.boxscore_id AND p.sacker=pt.pid
               JOIN bx b ON b.boxscore_id=p.boxscore_id
        GROUP BY 1,2,3""")


def _witness_table(con, spec):
    name, kind = spec[0], spec[1]
    if kind == "pbp":
        field = spec[2]
        tbl = spec[3] if len(spec) > 3 else "pbpw"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE w_{name} AS
            SELECT yr, wk, fr, CAST({field} AS DOUBLE) val FROM {tbl} WHERE {field} IS NOT NULL""")
        return f"w_{name}"
    if kind == "scoring":
        field = spec[2]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE w_{name} AS
            SELECT yr, wk, fr, CAST({field} AS DOUBLE) val FROM scw WHERE {field} IS NOT NULL""")
        return f"w_{name}"
    if kind == "self_sum":
        tbl, col = spec[2], spec[3]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE w_{name} AS
            SELECT tg.yr, tg.wk, tg.fr, SUM(TRY_CAST(s.{col} AS DOUBLE)) val
            FROM read_parquet('{BOX}/{tbl}/_combined.parquet') s JOIN tg
              ON s.boxscore_id=tg.boxscore_id AND s.team=tg.team_code GROUP BY 1,2,3""")
    elif kind == "opp_sum":
        tbl, col = spec[2], spec[3]
        # sum opponent's players, attribute to the DEFENDING team (tg.ofr)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE w_{name} AS
            SELECT tg.yr, tg.wk, tg.ofr fr, SUM(TRY_CAST(s.{col} AS DOUBLE)) val
            FROM read_parquet('{BOX}/{tbl}/_combined.parquet') s JOIN tg
              ON s.boxscore_id=tg.boxscore_id AND s.team=tg.team_code
            WHERE tg.ofr IS NOT NULL GROUP BY 1,2,3""")
    elif kind == "ts_self":
        field = spec[2]
        con.execute(f"""CREATE OR REPLACE TEMP TABLE w_{name} AS
            SELECT yr, wk, fr, CAST({field} AS DOUBLE) val FROM tsw WHERE {field} IS NOT NULL""")
    elif kind == "ts_opp":
        field = spec[2]
        # opponent's parsed line attributed to defending team via tg (fr->ofr)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE w_{name} AS
            SELECT g.yr, g.wk, g.ofr fr, CAST(t.{field} AS DOUBLE) val
            FROM tg g JOIN tsw t ON t.yr=g.yr AND t.wk=g.wk AND t.fr=g.fr
            WHERE g.ofr IS NOT NULL AND t.{field} IS NOT NULL""")
    return f"w_{name}"


def _era_of(y):
    for nm, lo, hi in ERAS:
        if lo <= y <= hi:
            return nm
    return "other"


def run(atom=None, show_bad=False):
    v26 = latest_v26()
    con = duckdb.connect(); con.execute("SET memory_limit='6GB'"); con.execute("PRAGMA threads=2")
    _ws(con)
    _scoring_witness(con)
    _pbp_witness(con)
    V = f"read_parquet('{v26}')"
    items = {atom: WITNESS_MAP[atom]} if atom else WITNESS_MAP
    rows = []
    bad = {}
    for a, (v26col, side, specs) in items.items():
        wnames = [_witness_table(con, s) for s in specs]
        sidef = "position='DEF'" if side == "def" else "position<>'DEF'"
        con.execute(f"""CREATE OR REPLACE TEMP TABLE va AS
            SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, nfl_franchise_number fr,
                   SUM(COALESCE({v26col},0)) v26 FROM {V}
            WHERE {sidef} AND nfl_franchise_number IS NOT NULL GROUP BY 1,2,3""")
        # full outer join all witnesses + v26 on (yr,wk,fr)
        base = "va"
        sel = ["va.yr", "va.wk", "va.fr", "va.v26"]
        joins = ""
        for wn in wnames:
            sel.append(f"{wn}.val AS {wn}")
            joins += f" FULL OUTER JOIN {wn} ON {base}.yr={wn}.yr AND {base}.wk={wn}.wk AND {base}.fr={wn}.fr"
            # note: chained FULL OUTER on va keys; va present for all real team-games
        df = con.execute(f"SELECT {', '.join(sel)} FROM va{joins}").df()
        df["era"] = df.yr.map(_era_of)
        wcols = wnames
        for era, lo, hi in ERAS:
            d = df[df.era == era]
            if d.empty:
                continue
            recs = []
            badrows = []
            for _, r in d.iterrows():
                wvals = [r[w] for w in wcols if pd.notna(r[w])]
                if not wvals:
                    continue
                cnt = Counter(round(x, 1) for x in wvals)
                top, topn = cnt.most_common(1)[0]
                consensus = top if topn >= 2 or len(wvals) == 1 else None
                dispute = (len(cnt) > 1 and topn < 2)  # witnesses split with no majority
                v26v = r["v26"] if pd.notna(r["v26"]) else None
                v26_ok = (consensus is not None and v26v is not None
                          and abs(v26v - consensus) <= TOL)
                recs.append((len(wvals), len(wvals) >= 2, consensus is not None, dispute, v26_ok))
                if show_bad and consensus is not None and not v26_ok:
                    badrows.append((int(r.yr), int(r.wk), int(r.fr), v26v, consensus,
                                    {w: r[w] for w in wcols if pd.notna(r[w])}))
            if not recs:
                continue
            n = len(recs)
            n_wit = sum(x[0] for x in recs) / n
            dbl = 100 * sum(x[1] for x in recs) / n
            cons = 100 * sum(x[2] for x in recs) / n
            disp = sum(x[3] for x in recs)
            cons_rows = [x for x in recs if x[2]]
            v26ok = 100 * sum(x[4] for x in cons_rows) / len(cons_rows) if cons_rows else 0.0
            rows.append((a, era, len(specs), independent_root_count(specs),
                         round(n_wit, 2), round(dbl, 1),
                         round(cons, 1), round(v26ok, 1), disp))
            if show_bad and badrows:
                bad[(a, era)] = badrows[:12]
    con.close()
    return rows, bad


def _print(rows):
    print("%-22s %-8s %5s %6s %6s %7s %9s %8s %8s" %
          ("atom", "era", "#wit", "roots", "meanW", "dbl%", "consen%", "v26_ok%", "disputes"))
    print("-" * 94)
    for r in rows:
        print("%-22s %-8s %5d %6d %6.2f %6.1f%% %8.1f%% %7.1f%% %8d" % r)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--atom", default=None); ap.add_argument("--show-bad", action="store_true")
    a = ap.parse_args()
    rows, bad = run(atom=a.atom, show_bad=a.show_bad)
    _print(rows)
    if bad:
        print("\n--- sample team-games where v26 disagrees with witness consensus ---")
        for (atom, era), brs in bad.items():
            print(f"\n[{atom} {era}]")
            for yr, wk, fr, v26v, cons, wv in brs:
                print(f"  {yr} wk{wk} fr{fr}: v26={v26v} consensus={cons} witnesses={wv}")
