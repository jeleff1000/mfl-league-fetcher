"""
sota_recon/golden_completeness_grid.py -- the FULL completeness grid: position-chain x era x grain x family.

For EVERY nfl_position in the super table (E..WR..LCB..DB..K..P..LS, 85 of them, each with its real
lifespan) this auto-selects a CHAIN of representative players -- one per decade the position existed -- so
the chains overlap and span each position from when it was invented until it was dissolved. Then it grids,
at each GRAIN (weekly / season / career), which STAT FAMILY each chain-cell actually carries, and overlays
the WITNESS era-floor (which witness could provide that family) so each cell reads: our-table-has vs
witness-could-provide. This is the concrete per-position/era gating map. Team/franchise chains are the
same idea at team grain (follow the Bears 1920->now and watch stats switch on).

Scalable: bulk GROUP BY (position, decade) passes, not per-player queries.

    python -m scripts.sota_recon.golden_completeness_grid                 # player position x era x family grid
    python -m scripts.sota_recon.golden_completeness_grid --team          # franchise x era team-stat grid
    python -m scripts.sota_recon.golden_completeness_grid --json OUT.json
"""
from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import duckdb

_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"

# stat family -> atoms (present-if-any-nonzero). Witness era-floor = earliest a witness can provide it.
FAMILIES = {
    "passing":      (["passing_yards", "passing_tds", "attempts", "completions"], 1920),
    "pass_adv":     (["passing_first_downs", "passing_air_yards", "sacks_suffered"], 1978),  # PBP era
    "rushing":      (["rushing_yards", "rushing_tds", "carries"], 1920),
    "rush_adv":     (["rushing_first_downs", "rushing_yards_before_contact", "rushing_broken_tackles"], 1978),
    "receiving":    (["receptions", "receiving_yards", "receiving_tds", "targets"], 1932),
    "rec_adv":      (["receiving_first_downs", "receiving_air_yards", "receiving_yac"], 1978),
    "fumbles":      (["fumbles"], 1978),          # PBP total; NFL.com logs earlier
    "fumbles_lost": (["fumbles_lost"], 1994),     # super today; NFL.com logs fill back to 1960s
    "scrimmage":    (["yds_from_scrimmage", "all_purpose_yards", "total_tds_scored"], 1920),
    "defense":      (["def_interceptions", "def_sacks", "fum_rec", "def_int_ret_td"], 1932),
    "def_adv":      (["def_tackles_solo", "def_tackle_assists", "def_qb_hits", "def_pass_defended", "def_tackles_for_loss"], 1978),
    "kicking":      (["fg_made", "fg_att", "pat_made"], 1932),
    "punting":      (["punts", "punt_net_yards"], 1939),
    "returns":      (["kickoff_return_yards", "punt_return_yards"], 1934),
}


# player_bio witness families (bio / draft / awards / combine / Approximate Value) -- present-if-nonnull.
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
BIO_FAMILIES = {
    "bio":     ["height", "weight", "college"],
    "draft":   ["draft_year", "draft_overall"],
    "awards":  ["hof", "allpro", "probowls"],
    "combine": ["forty", "bench", "vertical"],
    "AV":      ["w_av"],
}


def _latest_v26():
    return sorted(glob.glob("D:/league-history-data/nfl/releases/*_v26/tables/nfl_player_stats_all.parquet"),
                  key=lambda p: Path(p).stat().st_mtime, reverse=True)[0]


def bio_grid(con, src, group, min_players=15):
    """player_bio WITNESS coverage by (position, decade): fraction of the era's cohort that has each bio/
    draft/awards/combine/AV field. Shows where these witnesses light up (combine ~2000+, AV all-era, etc.)."""
    import os
    if not os.path.exists(BIO):
        return []
    bcols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{BIO}')").fetchall()}
    sel = []
    for fam, fields in BIO_FAMILIES.items():
        have = [f for f in fields if f in bcols]
        if not have:
            sel.append(f"NULL AS \"{fam}\""); continue
        anyf = " OR ".join(f"b.{f} IS NOT NULL" for f in have)
        sel.append(f"AVG(CASE WHEN {anyf} THEN 1.0 ELSE 0.0 END) AS \"{fam}\"")
    q = f"""
    WITH ply AS (
      SELECT DISTINCT NFL_player_id, {group} AS grp, (CAST(year AS INT)//10)*10 AS decade
      FROM read_parquet('{src}') WHERE {group} IS NOT NULL AND {group}<>'' AND NFL_player_id IS NOT NULL
    )
    SELECT p.grp, p.decade, COUNT(*) n, {', '.join(sel)}
    FROM ply p LEFT JOIN read_parquet('{BIO}') b USING (NFL_player_id)
    GROUP BY 1,2 HAVING COUNT(*) >= {min_players} ORDER BY 1,2
    """
    return con.execute(q).fetchall()


def _frac_sym(v):
    if v is None:
        return "-"
    return "Y" if v >= 0.6 else ("o" if v >= 0.15 else ".")


# team/franchise families (from super DEF (team-DST) rows) -- keyed on nfl_franchise_number (stable across
# relocations: Bears=5 1920->now). team_games witness adds the game record; nflcom_team adds NFL.com team stats.
TEAM_FAMILIES = {
    "dst_score":   ["pts_def_std"],
    "dst_sacks":   ["def_sacks"],
    "dst_takeaway":["def_interceptions", "fum_rec"],
    "dst_safety":  ["def_safeties"],
    "pts_allowed": ["points_allowed", "dst_points_allowed"],
    "yds_allowed": ["total_yds_allowed"],
    "team_record": ["pts_def_team_win", "pts_def_team_loss"],
    "st_ret_td":   ["special_teams_tds"],
}
TEAM_GAMES = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"


def team_grid(con, src, min_rows=8):
    """Franchise x decade completeness at TEAM grain (super DEF rows), with a representative team_code and
    the game-record witness (nfl_team_games_all) presence. Follow a franchise start->end, watch stats switch on."""
    scols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{src}')").fetchall()}
    fam_sel = []
    for fam, atoms in TEAM_FAMILIES.items():
        e = _fam_expr(atoms, scols)
        fam_sel.append(f"MAX(CASE WHEN ({e})<>0 THEN 1 ELSE 0 END) AS \"{fam}\"" if e else f"NULL AS \"{fam}\"")
    import os
    rec_join = ""
    if os.path.exists(TEAM_GAMES):
        rec_join = f", (SELECT COUNT(*) FROM read_parquet('{TEAM_GAMES}') g WHERE g.team_fid=c.fid AND (CAST(g.year AS INT)//10)*10=c.decade) rec_n"
    q = f"""
    WITH c AS (
      SELECT TRY_CAST(nfl_franchise_number AS INT) fid, (CAST(year AS INT)//10)*10 AS decade,
             COUNT(*) n, ANY_VALUE(nfl_team) code, {', '.join(fam_sel)}
      FROM read_parquet('{src}') WHERE position='DEF' AND nfl_franchise_number IS NOT NULL AND year IS NOT NULL
      GROUP BY 1,2 HAVING COUNT(*)>={min_rows}
    )
    SELECT c.fid, c.decade, c.n, c.code, {', '.join(f'c."{f}"' for f in TEAM_FAMILIES)}{rec_join}
    FROM c ORDER BY c.fid, c.decade
    """
    return con.execute(q).fetchall()


def _fam_expr(atoms, cols):
    have = [a for a in atoms if a in cols]
    return " + ".join(_D(a) for a in have) if have else None


def player_grid(con, src, cols, group, min_rows=150):
    """Bulk: for each (group, decade) with >=min_rows, family presence + a representative player."""
    fam_sel = []
    for fam, (atoms, _) in FAMILIES.items():
        e = _fam_expr(atoms, cols)
        fam_sel.append(f"MAX(CASE WHEN ({e})<>0 THEN 1 ELSE 0 END) AS \"{fam}\"" if e else f"NULL AS \"{fam}\"")
    q = f"""
    WITH cov AS (
      SELECT {group} AS grp, (CAST(year AS INT)//10)*10 AS decade, COUNT(*) n, {', '.join(fam_sel)}
      FROM read_parquet('{src}') WHERE {group} IS NOT NULL AND {group}<>'' AND year IS NOT NULL
      GROUP BY 1, 2 HAVING COUNT(*) >= {min_rows}
    ),
    rep AS (  -- representative player = most rows in that (grp,decade)
      SELECT grp, decade, player, ROW_NUMBER() OVER (PARTITION BY grp,decade ORDER BY cnt DESC, player) rn
      FROM (SELECT {group} AS grp, (CAST(year AS INT)//10)*10 AS decade, player, COUNT(*) cnt
            FROM read_parquet('{src}') WHERE {group} IS NOT NULL AND player IS NOT NULL GROUP BY 1,2,3)
    )
    SELECT c.grp, c.decade, c.n, r.player, {', '.join(f'c."{f}"' for f in FAMILIES)}
    FROM cov c LEFT JOIN rep r ON c.grp=r.grp AND c.decade=r.decade AND r.rn=1
    ORDER BY c.grp, c.decade
    """
    return con.execute(q).fetchall()


def _sym(v):
    return "Y" if v == 1 else ("." if v == 0 else "-")


def run_team(json_out=None):
    v26 = _latest_v26()
    con = duckdb.connect(); con.execute("SET memory_limit='3GB'"); con.execute("PRAGMA threads=3")
    rows = team_grid(con, v26)
    tfams = list(TEAM_FAMILIES)
    print("TEAM/FRANCHISE COMPLETENESS GRID (super DEF team-DST rows + team_games record witness)")
    print("Y=populated  .=empty  -=no column   | fid=nfl_franchise_number (stable across relocations)\n")
    hdr = f"{'fid':>4}{'dec':>6}{'code':>6}{'n':>5}  " + " ".join(f"{f[:10]:>11}" for f in tfams) + "   rec_games"
    print(hdr); print("-" * len(hdr))
    out = []
    cur = None
    for r in rows:
        fid, decade, n, code = r[0], int(r[1]), r[2], r[3] or ""
        cells = r[4:4 + len(tfams)]
        rec = r[-1] if len(r) > 4 + len(tfams) else None
        if fid != cur:
            print(); cur = fid
        print(f"{fid:>4}{decade:>6}{str(code):>6}{n:>5}  " + " ".join(f"{_sym(v):>11}" for v in cells)
              + f"   {rec if rec is not None else '-':>7}")
        out.append({"fid": fid, "decade": decade, "code": code, "rows": n, "rec_games": rec,
                    "families": {f: _sym(v) for f, v in zip(tfams, cells)}})
    con.close()
    if json_out:
        Path(json_out).write_text(json.dumps(out, indent=1, default=str))
        print(f"\nteam grid JSON -> {json_out}")


def run(group="nfl_position", json_out=None):
    if group == "team":
        return run_team(json_out=json_out)
    v26 = _latest_v26()
    con = duckdb.connect(); con.execute("SET memory_limit='3GB'"); con.execute("PRAGMA threads=3")
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{v26}')").fetchall()}
    rows = player_grid(con, v26, cols, group)
    bio = {(r[0], int(r[1])): r[3:] for r in bio_grid(con, v26, group)}  # (grp,decade) -> bio family fracs
    fams = list(FAMILIES); bfams = list(BIO_FAMILIES)
    print("COMPLETENESS GRID (super on-field | player_bio witness)  Y=populated  o=partial  .=empty  -=no column")
    print("witness era-floors: " + "  ".join(f"{f}>={FAMILIES[f][1]}" for f in fams if FAMILIES[f][1] > 1930))
    hdr = (f"{group[:11]:11}{'dec':>5}{'rep player':20}{'n':>6}  "
           + " ".join(f"{f[:7]:>7}" for f in fams) + "  ||BIO " + " ".join(f"{f[:7]:>7}" for f in bfams))
    print("\n" + hdr); print("-" * len(hdr))
    out = []
    cur = None
    for r in rows:
        grp, decade, n, rep = r[0], int(r[1]), r[2], r[3] or ""
        cells = r[4:]
        bcells = bio.get((grp, decade))
        if grp != cur:
            print(); cur = grp
        bio_str = (" ".join(f"{_frac_sym(v):>7}" for v in bcells)) if bcells else " ".join(f"{'?':>7}" for _ in bfams)
        print(f"{str(grp)[:11]:11}{decade:>5}{str(rep)[:20]:20}{n:>6}  "
              + " ".join(f"{_sym(v):>7}" for v in cells) + "  ||    " + bio_str)
        out.append({"pos": grp, "decade": decade, "rows": n, "rep": rep,
                    "onfield": {f: _sym(v) for f, v in zip(fams, cells)},
                    "bio": {f: _frac_sym(v) for f, v in zip(bfams, bcells)} if bcells else None})
    con.close()
    if json_out:
        Path(json_out).write_text(json.dumps(out, indent=1, default=str))
        print(f"\ngrid JSON -> {json_out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--team", action="store_true", help="grid by team (franchise) instead of nfl_position")
    ap.add_argument("--group", default=None)
    ap.add_argument("--json", default=None)
    a = ap.parse_args()
    grp = a.group or ("team" if a.team else "nfl_position")
    run(group=grp, json_out=a.json)
