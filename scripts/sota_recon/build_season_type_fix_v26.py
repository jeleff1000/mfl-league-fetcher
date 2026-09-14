"""
sota_recon/build_season_type_fix_v26.py -- witness-driven repair of season_type (REG/POST) mislabels.

FOUND (2026-07-16, during the pre-1978 fumbles_lost fill): pre-1970 championship/playoff games are
mislabeled season_type='REG' in the weekly super table (1946/48/49/57 conf/63/65/67 Ice Bowl/68 confirmed),
and the game catalog `nfl_team_games_all.parquet` carries the SAME wrong labels for most of them
(franchise_resolution_source='live_exact' -- its season_type was reconciled against the live table, so it
is NOT an independent witness for this column). The 1930s-50s were partially repaired before
(postseason_week_repaired_at_20260504); the 60s NFL side was not.

INDEPENDENT WITNESSES (both local):
  W1 -- NFL.com game-log section captions ('Regular Season' / 'Post Season' / 'Preseason') per game row,
        pinned to the exact catalog game via (season, game_date, opp-nickname -> franchise codes). A game
        with >=1 Post-Season caption and 0 Regular-Season captions is POST (and vice versa); mixed = queue.
        Corpus: the full harvested player_logs shards + the targeted fetch (~105k caption rows).
  W2 -- structural schedule signature: per (season, league-partition) the regular season is the run of
        multi-game weeks (>=3 games in a Mon-Sun bucket for that league); any game in a LATER week is
        postseason (playoff weekends have 1-2 games per league). League partitions are derived from the
        game graph itself (teams only meet in-league before the merger; Jan/Feb games -- the Super Bowl
        edge -- are excluded from partitioning). 1970+ is one league.
TRUTH: W1 where present (Preseason vote = conflict); else W2; where both exist they must AGREE else the
game is queued, never guessed. Every flip carries receipts.

APPLY: patches BOTH the weekly super table (player rows keyed year/week/nfl_team) AND the catalog parquet
(so the witness artifact stops carrying the error), each with backup + exact-count gates.
Season/career aggregates split on season_type (player_nfl_season excludes playoffs) -> re-rollup after.

    python -m scripts.sota_recon.build_season_type_fix_v26            # recon: flip list + receipts
    python -m scripts.sota_recon.build_season_type_fix_v26 --apply    # mutate super + catalog
"""
from __future__ import annotations

import argparse
import os
import shutil
from collections import defaultdict
from pathlib import Path

import duckdb
import pandas as pd

from .sources import latest_v26
from .recon_common import utc_stamp

CATALOG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
NC_LOGS = [
    "D:/league-history-data/nfl/raw/nflcom/tables/player_logs/*.parquet",
    # includes season_type_probe.parquet: targeted per-game player fetches for candidate games the
    # main crawl hadn't reached (the fumbles_pre1978 fetch also carries captions)
    "D:/league-history-data/nfl/raw/nflcom/tables/player_logs_targeted/*.parquet",
]
OUT_DIR = Path("D:/league-history-data/nfl/derived/validation")

from .build_pre1978_fumbles_lost_v26 import NICKNAME_CODES  # same era-aware nickname multimap


def _witness_w1(con) -> pd.DataFrame:
    """NC caption votes per catalog game (boxscore_id): n_post / n_reg / n_pre."""
    reads = " UNION ALL ".join(
        f"SELECT _table, season, game_date, opp FROM read_parquet('{g}', union_by_name=true)" for g in NC_LOGS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nc_rows AS
        SELECT _table caption, TRY_CAST(season AS INT) season,
               TRY_STRPTIME(game_date, '%m/%d/%Y')::DATE gdate,
               TRIM(REPLACE(CAST(opp AS VARCHAR), '@', '')) nick
        FROM ({reads})
        WHERE _table IN ('Regular Season','Post Season','Preseason')
          AND TRY_STRPTIME(game_date, '%m/%d/%Y') IS NOT NULL AND opp IS NOT NULL""")
    nc = con.execute("SELECT * FROM nc_rows").fetchdf()
    cat = con.execute(f"""
        SELECT boxscore_id, CAST(year AS INT) season, TRY_CAST(game_date AS DATE) gdate,
               team_code, opponent_code, team_fid, opponent_fid
        FROM read_parquet('{CATALOG}')""").fetchdf()
    # nickname -> franchise number via the catalog's own code->fid map (franchise number is the
    # drift-proof identity -- codes change era to era, the fid does not)
    code_fid = {}
    for r in cat.itertuples():
        if pd.notna(r.team_fid):
            code_fid.setdefault(r.team_code, set()).add(int(r.team_fid))
        if pd.notna(r.opponent_fid):
            code_fid.setdefault(r.opponent_code, set()).add(int(r.opponent_fid))
    nick_fids = {nick: set().union(*(code_fid.get(c, set()) for c in codes))
                 for nick, codes in NICKNAME_CODES.items()}
    # index catalog games by (gdate, side-fid): a franchise plays once per day -> unique pin
    by_side = {}
    for r in cat.itertuples():
        if pd.notna(r.team_fid):
            by_side[(r.gdate, int(r.team_fid))] = r.boxscore_id
        if pd.notna(r.opponent_fid):
            by_side[(r.gdate, int(r.opponent_fid))] = r.boxscore_id
    votes: dict[str, dict[str, int]] = defaultdict(lambda: {"post": 0, "reg": 0, "pre": 0})
    unmapped_nicks: dict[str, int] = defaultdict(int)
    for r in nc.itertuples():
        fids = nick_fids.get(r.nick)
        if not fids:
            unmapped_nicks[r.nick] += 1
            continue
        bids = {by_side.get((r.gdate, f)) for f in fids} - {None}
        if len(bids) != 1:
            continue  # no catalog game that date for the nickname (preseason mostly) or ambiguous
        b = bids.pop()
        key = "post" if r.caption == "Post Season" else ("reg" if r.caption == "Regular Season" else "pre")
        votes[b][key] += 1
    w1 = pd.DataFrame([{"boxscore_id": b, **v} for b, v in votes.items()])
    w1["w1"] = None
    w1.loc[(w1["post"] > 0) & (w1["reg"] == 0) & (w1["pre"] == 0), "w1"] = "POST"
    w1.loc[(w1["reg"] > 0) & (w1["post"] == 0) & (w1["pre"] == 0), "w1"] = "REG"
    w1.loc[(w1["pre"] > 0) & (w1["post"] == 0) & (w1["reg"] == 0), "w1"] = "PRE"
    return w1, dict(unmapped_nicks)


def _witness_w2(con) -> pd.DataFrame:
    """Structural CANDIDATE GENERATOR (never a flip authority -- pre-merger schedules are too ragged and
    modern playoff weekends too full for a count rule to be truth): a game is a POST-candidate iff its
    DATE is strictly later than the league-partition's last >=3-games-on-one-day date that season.
    Same-day counting avoids the Saturday-playoff-next-to-Sunday-slate bucketing trap (1969 AFL).
    Partitions from the game graph excluding Jan/Feb (the Super Bowl edge)."""
    cat = con.execute(f"""
        SELECT boxscore_id, CAST(year AS INT) season, TRY_CAST(game_date AS DATE) gdate,
               team_code, opponent_code
        FROM read_parquet('{CATALOG}') WHERE game_date IS NOT NULL""").fetchdf()
    out = []
    for season, g in cat.groupby("season"):
        core = g[~g["gdate"].map(lambda d: d.month in (1, 2))]
        adj = defaultdict(set)
        for r in core.itertuples():
            adj[r.team_code].add(r.opponent_code)
            adj[r.opponent_code].add(r.team_code)
        comp_of = {}
        for t in adj:
            if t in comp_of:
                continue
            stack, cid = [t], t
            while stack:
                x = stack.pop()
                if x in comp_of:
                    continue
                comp_of[x] = cid
                stack.extend(adj[x] - comp_of.keys())
        g = g.copy()
        g["comp"] = g["team_code"].map(comp_of)
        missing = g["comp"].isna()
        g.loc[missing, "comp"] = g.loc[missing, "opponent_code"].map(comp_of)
        gg = g.drop_duplicates("boxscore_id")
        per_day = gg.groupby(["comp", "gdate"]).size().rename("n").reset_index()
        last_reg_day = per_day[per_day["n"] >= 3].groupby("comp")["gdate"].max().to_dict()
        for r in gg.itertuples():
            lim = last_reg_day.get(r.comp)
            w2 = None if lim is None else ("POST" if r.gdate > lim else "REG")
            out.append({"boxscore_id": r.boxscore_id, "w2": w2})
    return pd.DataFrame(out)


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("PRAGMA threads=3")
    w1, unmapped = _witness_w1(con)
    w2 = _witness_w2(con)
    cat = con.execute(f"""
        SELECT boxscore_id, CAST(year AS INT) season, CAST(week AS INT) wk,
               TRY_CAST(game_date AS DATE) gdate, team_code, opponent_code,
               CAST(team_fid AS INT) team_fid, UPPER(season_type) cat_st
        FROM read_parquet('{CATALOG}')""").fetchdf()
    games = cat.drop_duplicates("boxscore_id")[["boxscore_id", "season", "wk", "gdate"]] \
        .merge(w1[["boxscore_id", "post", "reg", "pre", "w1"]], on="boxscore_id", how="left") \
        .merge(w2, on="boxscore_id", how="left")

    def truth(r):
        """Flip authorities, in order:
        ERA  -- season < 1932: the NFL had no postseason -> REG (fact, not inference).
        NOV  -- pre-merger game in Nov or earlier: never postseason -> REG (all pre-1970 playoff
                games were mid-December or later).
        W1   -- unambiguous NC caption votes (the only per-game witness).
        1978+ is nflverse territory: report-only, never flip. W2 is a candidate flag, not truth."""
        if pd.isna(r["season"]):
            return ("NO_SEASON", None)
        if r["season"] >= 1978:
            return ("MODERN_REPORT_ONLY", None)
        if r["season"] < 1932:
            return ("ERA_PRE1932", "REG")
        # pre-merger playoff games were never earlier than mid-December: a FALL-month game (Aug-Nov)
        # is regular season by era-fact. (month <= 11 alone would swallow January playoff games!)
        nov = pd.notna(r["gdate"]) and r["season"] < 1970 and r["gdate"].month in (8, 9, 10, 11)
        if pd.notna(r["w1"]):
            if r["w1"] == "PRE":
                return ("CONFLICT_PRESEASON", None)
            if nov and r["w1"] != "REG":
                return ("CONFLICT_NOV_VS_W1", None)
            votes = (r["post"] if r["w1"] == "POST" else r["reg"])
            if r["w2"] == r["w1"]:
                return ("W1+W2ok", r["w1"])
            if votes >= 2:
                return ("W1_MULTI", r["w1"])
            # single caption vote with no structural agreement is not enough to flip
            # (caught live: NFL.com files the 1947 AAFC championship under 'Regular Season')
            return ("CONFLICT_W1_SINGLE_NO_W2", None)
        if nov:
            return ("NOV_RULE", "REG")
        return ("NO_W1", None)

    tr = games.apply(truth, axis=1)
    games["truth_src"] = [t[0] for t in tr]
    games["truth"] = [t[1] for t in tr]

    # super team-weeks -- joined to the catalog on FRANCHISE NUMBER (drift-proof), code as recon
    v26 = Path(latest_v26()).as_posix()
    sup = con.execute(f"""
        SELECT DISTINCT CAST(year AS INT) season, CAST(week AS INT) wk, nfl_team,
               CAST(nfl_franchise_number AS INT) fid, UPPER(season_type) sup_st
        FROM read_parquet('{v26}')
        WHERE nfl_team IS NOT NULL AND year IS NOT NULL AND week IS NOT NULL""").fetchdf()
    m = sup.merge(cat[["season", "wk", "team_fid", "team_code", "boxscore_id", "cat_st"]],
                  left_on=["season", "wk", "fid"], right_on=["season", "wk", "team_fid"],
                  how="left") \
           .merge(games[["boxscore_id", "truth", "truth_src", "post", "reg", "w1", "w2"]],
                  on="boxscore_id", how="left")
    code_drift = m[m["boxscore_id"].notna() & (m["nfl_team"] != m["team_code"])]
    flips = m[m["truth"].notna() & (m["sup_st"] != m["truth"])].copy()
    cat_wrong = m[m["truth"].notna() & (m["cat_st"] != m["truth"])].copy()

    # games still needing a witness: pre-merger candidates (W2-POST or super-POST) with no W1 -> these
    # get a targeted NC log fetch of their players (see --fetch note); until then they are queued.
    mg = m.merge(games[["boxscore_id", "gdate"]], on="boxscore_id", how="left")
    need = mg[(mg["season"].between(1932, 1977)) & (mg["truth"].isna())
              & ((mg["w2"] == "POST") | (mg["sup_st"] == "POST"))]
    need_games = need.drop_duplicates("boxscore_id")

    stamp = utc_stamp()
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv = OUT_DIR / f"season_type_fix_decisions_{stamp}.csv"
    flips.sort_values(["season", "wk", "nfl_team"]).to_csv(csv, index=False)
    fetch_csv = OUT_DIR / f"season_type_fetch_needed_{stamp}.csv"
    need.sort_values(["season", "wk"]).to_csv(fetch_csv, index=False)

    res = {"nc_unmapped_nicknames": {k: v for k, v in sorted(unmapped.items(), key=lambda x: -x[1])[:10]},
           "games_with_truth": int(games["truth"].notna().sum()),
           "games_conflict": int(games["truth_src"].str.startswith("CONFLICT").sum()),
           "super_team_week_flips": int(len(flips)),
           "catalog_team_row_flips": int(len(cat_wrong)),
           "flip_seasons": {int(k): int(v) for k, v in flips.groupby("season").size().items()},
           "fetch_needed_games": int(len(need_games)),
           "fetch_needed_csv": str(fetch_csv),
           "super_catalog_code_drift_rows": int(len(code_drift)),
           "decision_csv": str(csv)}
    if not apply:
        con.close()
        return res

    # sanity gate: pre-merger playoff structure means <=6 flipped games (12 team-rows) per season
    per_season_games = flips.drop_duplicates("boxscore_id").groupby("season").size()
    too_many = per_season_games[per_season_games > 6]
    assert too_many.empty, f"flip volume implausible (>6 games/season): {too_many.to_dict()}"

    # ---- apply to SUPER: single STREAMING pass (SELECT * REPLACE + 30-key hash join -> ParquetWriter),
    #      keyed year/week/FRANCHISE NUMBER (drift-proof). No table materialization.
    import pyarrow.parquet as pq

    # a team-week can appear twice in flips (ancient DOUBLE-GAME weeks: two games share one super
    # "week" -- 1930 NYG, 1933 CRD, 1948 BUF) -- dedupe the key or the LEFT JOIN fans out (row gate
    # catches it). Safe ONLY when both games share one truth; a split-truth doubled week cannot be
    # expressed at (year, week, team) grain at all, so hard-fail if ever seen.
    per_key_truths = flips.groupby(["season", "wk", "fid"])["truth"].nunique()
    assert (per_key_truths == 1).all(), \
        f"doubled team-week with SPLIT truth -- needs game-grain repair: {per_key_truths[per_key_truths > 1]}"
    chg = flips[["season", "wk", "fid", "truth"]].drop_duplicates(subset=["season", "wk", "fid"]).copy()
    v26p = Path(latest_v26())
    sp = os.path.join(os.path.dirname(v26p), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con2 = duckdb.connect()
    con2.execute("PRAGMA threads=4"); con2.execute("PRAGMA disable_progress_bar")
    con2.execute("SET preserve_insertion_order=false"); con2.execute("SET memory_limit='6GB'")
    con2.execute(f"SET temp_directory='{sp}'"); con2.execute("SET max_temp_directory_size='40GB'")
    con2.register("chg", chg)
    src = v26p.as_posix()
    before_rows = con2.execute(f"SELECT COUNT(*) FROM read_parquet('{src}')").fetchone()[0]
    expected = con2.execute(f"""SELECT COUNT(*) FROM read_parquet('{src}') st JOIN chg
        ON CAST(st.year AS INT)=chg.season AND CAST(st.week AS INT)=chg.wk
           AND CAST(st.nfl_franchise_number AS INT)=chg.fid
        WHERE UPPER(st.season_type) != chg.truth""").fetchone()[0]
    tmp = v26p.with_name(v26p.stem + "_sttmp.parquet")
    rb = con2.execute(f"""
        SELECT st.* REPLACE (
            CASE WHEN c.fid IS NOT NULL THEN c.truth ELSE st.season_type END AS season_type)
        FROM read_parquet('{src}') st
        LEFT JOIN chg c
          ON CAST(st.year AS INT)=c.season AND CAST(st.week AS INT)=c.wk
             AND CAST(st.nfl_franchise_number AS INT)=c.fid
    """).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con2.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    misapplied = con2.execute(f"""SELECT COUNT(*) FROM read_parquet('{tq}') st JOIN chg
        ON CAST(st.year AS INT)=chg.season AND CAST(st.week AS INT)=chg.wk
           AND CAST(st.nfl_franchise_number AS INT)=chg.fid
        WHERE UPPER(st.season_type) != chg.truth""").fetchone()[0]
    con2.close()
    res["super_player_rows_flipped"] = int(expected)
    res["rows_unchanged"] = after_rows == before_rows
    res["misapplied_rows"] = int(misapplied)
    if after_rows != before_rows or misapplied != 0:
        res["swapped"] = False
        res["temp"] = str(tmp)
        shutil.rmtree(sp, ignore_errors=True)
        return res
    backup = v26p.with_name(v26p.stem + f"_prest_{stamp}.parquet")
    shutil.copy2(v26p, backup)
    os.replace(tmp, v26p)
    res["super_backup"] = backup.name

    # ---- apply to CATALOG (team rows keyed boxscore_id) so the witness stops carrying the error
    catp = Path(CATALOG)
    con3 = duckdb.connect()
    con3.execute("SET memory_limit='2GB'")
    fix_games = flips.drop_duplicates("boxscore_id")[["boxscore_id", "truth"]]
    con3.register("fix", fix_games)
    con3.execute(f"CREATE TABLE ct AS SELECT * FROM read_parquet('{catp.as_posix()}')")
    n_cat = con3.execute("""SELECT COUNT(*) FROM ct JOIN fix USING (boxscore_id)
                            WHERE UPPER(ct.season_type) != fix.truth""").fetchone()[0]
    con3.execute("""UPDATE ct SET season_type = fix.truth
                    FROM fix WHERE ct.boxscore_id = fix.boxscore_id""")
    ctmp = catp.with_name(catp.stem + "_sttmp.parquet")
    con3.execute(f"COPY ct TO '{ctmp.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    con3.close()
    cbackup = catp.with_name(catp.stem + f"_prest_{stamp}.parquet")
    shutil.copy2(catp, cbackup)
    os.replace(ctmp, catp)
    res["catalog_rows_flipped"] = int(n_cat)
    res["catalog_backup"] = cbackup.name
    res["swapped"] = True
    res["next"] = "season/career rollup re-splits REG/POST -> rerun build_season_career_v26 chain + gates"
    shutil.rmtree(sp, ignore_errors=True)
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    import json
    print(json.dumps(run(apply=a.apply), indent=1, default=str))
