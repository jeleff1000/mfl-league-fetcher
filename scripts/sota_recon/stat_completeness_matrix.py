"""
sota_recon/stat_completeness_matrix.py  --  the master non-derived completeness+accuracy grid

Stops the whack-a-mole. For EVERY atomic non-derived stat (passing/rushing/receiving/
defense/special-teams/kicking), this maps the v26 column to its authoritative boxscore
source column and measures, per era, at the TEAM-GAME grain:

  - coverage% : of the team-games the SOURCE has (in the stat's valid era), how many does
                v26 also have a populated team-game total for?
  - agree%    : of the team-games BOTH have, how many match within tolerance?
  - src_total / v26_total : era sums, for an at-a-glance magnitude check.

Team-game grain (year, week, franchise) is used deliberately: it sidesteps individual
player-id matching noise and directly answers "is every game complete and correct?".
The source boxscore rows are mapped to (year, week, team_fid) via nfl_team_games_all.

SOURCE_MAP is the glossary: source_table -> [(src_col, v26_col, side, era_lo, era_hi)].
  side: 'off' (non-DEF player rows), 'def' (non-DEF), 'ret' (non-DEF), 'kick' (non-DEF),
        'dst' (position='DEF' team row).

    python -m scripts.sota_recon.stat_completeness_matrix            # full grid -> stdout + md
    python -m scripts.sota_recon.stat_completeness_matrix --family defense
    python -m scripts.sota_recon.stat_completeness_matrix --gaps     # only rows below standard
"""

from __future__ import annotations

import argparse
import os

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
TOL = 0.5  # team-game totals are integer counts/yards; <0.5 == exact

# era buckets (lo inclusive, hi inclusive); advanced charted stats use 2018+
ERAS = [("pre1950", 1920, 1949), ("1950_77", 1950, 1977),
        ("1978_01", 1978, 2001), ("2002_pl", 2002, 2025)]

# family -> (source_table, [(src_col, v26_col, side, era_lo, era_hi)])
SOURCE_MAP = {
    "passing": ("player_offense", [
        ("pass_cmp", "completions", "off", 1932, 2025),
        ("pass_att", "attempts", "off", 1932, 2025),
        ("pass_yds", "passing_yards", "off", 1932, 2025),
        ("pass_td", "passing_tds", "off", 1932, 2025),
        ("pass_int", "passing_interceptions", "off", 1932, 2025),
        ("pass_sacked", "sacks_suffered", "off", 1947, 2025),
        ("pass_sacked_yds", "sack_yards_lost", "off", 1947, 2025),
        ("pass_long", "passing_long", "off", 1932, 2025),
    ]),
    "rushing": ("player_offense", [
        ("rush_att", "carries", "off", 1932, 2025),
        ("rush_yds", "rushing_yards", "off", 1932, 2025),
        ("rush_td", "rushing_tds", "off", 1932, 2025),
        ("rush_long", "rushing_long", "off", 1932, 2025),
    ]),
    "receiving": ("player_offense", [
        ("rec", "receptions", "off", 1932, 2025),
        ("rec_yds", "receiving_yards", "off", 1932, 2025),
        ("rec_td", "receiving_tds", "off", 1932, 2025),
        ("rec_long", "receiving_long", "off", 1932, 2025),
        ("targets", "targets", "off", 1992, 2025),
        ("fumbles", "fumbles", "off", 1932, 2025),
        ("fumbles_lost", "fumbles_lost", "off", 1932, 2025),
    ]),
    "defense": ("player_defense", [
        ("def_int", "def_interceptions", "def", 1933, 2025),
        ("def_int_yds", "def_interception_yards", "def", 1933, 2025),
        ("def_int_td", "def_int_ret_td", "def", 1933, 2025),
        ("sacks", "def_sacks", "def", 1982, 2025),
        ("tackles_solo", "def_tackles_solo", "def", 1994, 2025),
        ("tackles_assists", "def_tackle_assists", "def", 1994, 2025),
        ("fumbles_rec", "def_fumbles", "def", 1933, 2025),
        ("fumbles_rec_yds", "fum_rec_yds", "def", 1933, 2025),
        ("fumbles_rec_td", "fum_ret_td", "def", 1933, 2025),
        ("fumbles_forced", "def_fumbles_forced", "def", 1990, 2025),
        ("pass_defended", "def_pass_defended", "def", 1999, 2025),
        ("tackles_loss", "def_tackles_for_loss", "def", 1999, 2025),
        ("qb_hits", "def_qb_hits", "def", 1999, 2025),
    ]),
    "kicking": ("kicking", [
        ("xpm", "pat_made", "kick", 1933, 2025),
        ("xpa", "pat_att", "kick", 1933, 2025),
        ("fgm", "fg_made", "kick", 1933, 2025),
        ("fga", "fg_att", "kick", 1933, 2025),
        ("punt", "punts", "kick", 1941, 2025),
        ("punt_yds", "punt_yards", "kick", 1941, 2025),
        ("punt_long", "punt_long", "kick", 1941, 2025),
    ]),
    "special_teams": ("returns", [
        ("kick_ret", "kickoff_returns", "ret", 1941, 2025),
        ("kick_ret_yds", "kickoff_return_yards", "ret", 1941, 2025),
        ("kick_ret_td", "kickoff_return_tds", "ret", 1941, 2025),
        ("punt_ret", "punt_returns", "ret", 1941, 2025),
        ("punt_ret_yds", "punt_return_yards", "ret", 1941, 2025),
        ("punt_ret_td", "punt_return_tds", "ret", 1941, 2025),
    ]),
    "advanced": ("__multi__", [  # 2018+ charted; source table per-col
        # PFR advanced "air yards" is COMPLETED air yards (matches super's *_completed_air_yards, ~99%);
        # super *_air_yards is INTENDED (all attempts, ~2x). Two distinct columns -- map like-to-like.
        ("pass_air_yds@passing_advanced", "passing_completed_air_yards", "off", 2018, 2025),
        ("pass_yac@passing_advanced", "passing_yards_after_catch", "off", 2018, 2025),
        ("pass_pressured@passing_advanced", "passing_pressured", "off", 2018, 2025),
        ("rec_air_yds@receiving_advanced", "receiving_completed_air_yards", "off", 2018, 2025),
        ("rec_yac@receiving_advanced", "receiving_yards_after_catch", "off", 2018, 2025),
        ("rec_broken_tackles@receiving_advanced", "receiving_broken_tackles", "off", 2018, 2025),
        ("rush_yds_before_contact@rushing_advanced", "rushing_yards_before_contact", "off", 2018, 2025),
        ("rush_broken_tackles@rushing_advanced", "rushing_broken_tackles", "off", 2018, 2025),
    ]),
}

SIDE_FILTER = {
    "off": "position<>'DEF'", "def": "position<>'DEF'",
    "ret": "position<>'DEF'", "kick": "position<>'DEF'", "dst": "position='DEF'",
}


def _era_of(y):
    for nm, lo, hi in ERAS:
        if lo <= y <= hi:
            return nm
    return "other"


def run(family=None, gaps_only=False):
    v26 = latest_v26()
    v26names = set(pq.read_schema(v26).names)
    con = duckdb.connect()
    con.execute("SET memory_limit='5GB'"); con.execute("PRAGMA threads=2")
    con.execute(f"""CREATE TEMP TABLE tg AS SELECT boxscore_id, team_code,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr FROM read_parquet('{TG}')
        WHERE team_fid IS NOT NULL""")
    V = f"read_parquet('{v26}')"
    rows = []
    fams = [family] if family else list(SOURCE_MAP)
    for fam in fams:
        deftbl, cols = SOURCE_MAP[fam]
        for src_col, v26_col, side, elo, ehi in cols:
            if v26_col not in v26names:
                rows.append((fam, v26_col, src_col, "ALL", "MISSING_COL", 0, 0, 0, 0, 0, 0))
                continue
            # resolve source table (advanced uses col@table)
            if "@" in src_col:
                sc, tbl = src_col.split("@")
            else:
                sc, tbl = src_col, deftbl
            srcpath = f"{BOX}/{tbl}/_combined.parquet"
            if not os.path.exists(srcpath):
                rows.append((fam, v26_col, src_col, "ALL", "NO_SRC", 0, 0, 0, 0, 0, 0))
                continue
            # _long columns are MAX-type (team's longest play), not additive -> aggregate by MAX
            agg = "MAX" if v26_col.endswith("_long") else "SUM"
            # source team-game totals (mapped to year/week/franchise via tg)
            con.execute(f"""CREATE OR REPLACE TEMP TABLE s_agg AS
                SELECT tg.yr, tg.wk, tg.fr, {agg}(TRY_CAST(src.{sc} AS DOUBLE)) sval
                FROM read_parquet('{srcpath}') src JOIN tg
                  ON src.boxscore_id=tg.boxscore_id AND src.team=tg.team_code
                WHERE src.season BETWEEN {elo} AND {ehi}
                GROUP BY 1,2,3""")
            # v26 team-game totals
            con.execute(f"""CREATE OR REPLACE TEMP TABLE v_agg AS
                SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, nfl_franchise_number fr,
                       {agg}(COALESCE({v26_col},0)) vval
                FROM {V} WHERE {SIDE_FILTER[side]} AND year BETWEEN {elo} AND {ehi}
                  AND nfl_franchise_number IS NOT NULL
                GROUP BY 1,2,3""")
            df = con.execute("""
                SELECT s.yr, s.sval, v.vval FROM s_agg s
                LEFT JOIN v_agg v ON s.yr=v.yr AND s.wk=v.wk AND s.fr=v.fr
                WHERE s.sval IS NOT NULL""").df()
            if df.empty:
                continue
            df["era"] = df.yr.map(_era_of)
            for era, lo, hi in ERAS:
                d = df[df.era == era]
                if d.empty:
                    continue
                src_games = len(d)
                both = d[d.vval.notna()]
                cov = round(100 * len(both) / src_games, 1) if src_games else 0
                if len(both):
                    agree = round(100 * (abs(both.vval - both.sval) < TOL).sum() / len(both), 1)
                else:
                    agree = 0.0
                rows.append((fam, v26_col, src_col, era, "", src_games, len(both),
                             cov, agree, round(d.sval.sum(), 0), round(both.vval.sum(), 0)))
    con.close()
    # filter to gaps if requested: coverage<99 or agree<99 (within the stat's valid era)
    def is_gap(r):
        return r[4] in ("MISSING_COL", "NO_SRC") or (r[5] > 50 and (r[7] < 99.0 or r[8] < 99.0))
    out = [r for r in rows if (not gaps_only or is_gap(r))]
    return out


def _print(rows):
    hdr = ("family", "v26_col", "src_col", "era", "flag", "src_g", "both_g", "cov%", "agree%", "src_tot", "v26_tot")
    print("%-13s %-26s %-22s %-8s %-11s %7s %7s %6s %7s %12s %12s" % hdr)
    print("-" * 150)
    for r in rows:
        print("%-13s %-26s %-22s %-8s %-11s %7d %7d %6.1f %7.1f %12.0f %12.0f" % r)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--family", default=None)
    ap.add_argument("--gaps", action="store_true")
    a = ap.parse_args()
    rows = run(family=a.family, gaps_only=a.gaps)
    _print(rows)
    ng = sum(1 for r in rows if r[4] in ("MISSING_COL", "NO_SRC") or (r[5] > 50 and (r[7] < 99 or r[8] < 99)))
    print(f"\n{len(rows)} stat-era cells | {ng} below standard (cov<99 or agree<99 in-era)")
