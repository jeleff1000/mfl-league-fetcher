"""
sota_recon/build_pre1978_fumbles_lost_v26.py -- pre-1978 fumbles_lost reconciliation + fill (the deferred
"genuine point mutation" from the 2026-07-16 handoff, widened by audit).

WHY THE WIDER SCOPE: the deferred item was "fill the 54 NULL fumbles_lost rows"; auditing showed the 259
pre-1978 rows that already HAVE fumbles_lost are the same poison -- 19 lost / 307 fumbles (~6% lost-rate vs
the ~50% true base rate), i.e. a default-0 ingest, not data. NFL.com game-log `lost` is likewise default-0
before ~1980 (1940s: 0/98 positive) but carries REAL differentiated values for 1960s+ postseason (verified:
mixed values within games, 100% concordant with PFR team-level Fumbles-Lost; Anderson'65/Starr'67 match the
historical record). So the audit population is ALL pre-1978 player-weeks with fumbles>0 (313 rows).

GAME MATCHING (Joe 2026-07-16): via OUR game catalog `nfl_team_games_all.parquet` -- every NFL game ever,
joined exactly on (year, week, team_code); it supplies boxscore_id + is_home (side) + game_date. No
heuristics. Recon: catalog date vs super game_date asserted where super has one (186/186 agree, 0 drift).

IDENTITY RECON (Joe 2026-07-16): name-based witness joins must reconcile against our unique ids so
name-twins can't misappropriate stats:
  * PFR: player_offense.player_link_ids (PFR id) -> player_bio.pfr_id -> NFL_player_id; per-row receipt
    `pfr_fum_by_id` (individual fumbles matched BY ID, not name) + twin flag when a name-match exists in
    the game whose id is NOT ours.
  * NC (no ids): the log row's `opp` nickname must map to the catalog opponent_code for that exact game
    (era-aware multimap, loud-fail on unmapped) -- a same-name twin's game that date would be against a
    different opponent. Rows failing the check drop their NC value + get flagged.

WITNESSES (all local): PFR team_stats "Fumbles-Lost" [frame] · PFR player_offense individual fumbles
(1932+; by-id) · NC game logs fum/lost (nflcom_targeted_logs.py) · PFR scoring "fumble return" floors.

RULES per (team, game) with team totals F (fumbles) and L (lost):
  R1  L == 0            -> every held fumbler fl := 0 (CONFIRMED), unless a scoring-table fumble-return
                           floor contradicts (then CONFLICT: no fill, queue).
  R2  L == F            -> every fumble was lost -> fl := player's fumbles (forced; no coverage needed).
  R3  NC-trusted game   -> fl := NC per-player value. Trust gate: the game shows ANY NC lost>0 among held
                           rows (differentiated, not default) AND no per-team contradiction with PFR
                           (nc_lost_sum <= L, nc_fum_sum <= F per team) AND NC identity check passed.
  R4  existing fl=0 in a team-game where the losses cannot be covered by unheld fumbles + held-NULL
                           capacity (L - known_HL > (F - S) + null_cap) -> the zeros are group-provably a
                           default -> fl := NULL (unknown) with receipt. Otherwise KEEP (unverified).
  Rows with no matched PFR team stats keep current values (reported); conflicts are queued, never guessed.

DELETION DISCIPLINE: every mutation carries a per-row receipt (rule, PFR F-L, NC value, by-id PFR fum,
prior value) in the decision CSV; apply gates on exact expected-mutation counts; backup parquet.

POINTS: COALESCE(fumbles_lost,0) in the scorer => NULL<->0 moves are points-invariant; fl>0 fills are the
point mutation (-2/lost std). AFTER --apply run the chain: build_rescore_fpts_v26 --apply -> weekly ranks ->
build_season_career_v26 (now auto-preserves live-only cols) -> season/career ranks -> gates.

    python -m scripts.sota_recon.build_pre1978_fumbles_lost_v26            # recon: decision table + summary
    python -m scripts.sota_recon.build_pre1978_fumbles_lost_v26 --apply    # mutate v26 weekly parquet
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pandas as pd

from .sources import latest_v26
from .recon_common import utc_stamp

PFR = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
CATALOG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
NC_TARGETED = "D:/league-history-data/nfl/raw/nflcom/tables/player_logs_targeted/fumbles_pre1978.parquet"
OUT_DIR = Path("D:/league-history-data/nfl/derived/validation")

# NC `opp` nickname -> catalog franchise codes (era-aware multimap; identity gate loud-fails on any
# nickname it meets on a TARGET game that is not mapped here)
NICKNAME_CODES = {
    "49ers": {"SFO"}, "Bears": {"CHI"}, "Bengals": {"CIN"}, "Bills": {"BUF"}, "Broncos": {"DEN"},
    "Browns": {"CLE"}, "Cardinals": {"CRD", "STL", "ARI"}, "Chargers": {"SDG", "LAC"},
    "Chiefs": {"KAN"}, "Colts": {"BAL", "IND", "CLT"}, "Cowboys": {"DAL"}, "Dodgers": {"BKN", "BRK"},
    "Dolphins": {"MIA"}, "Eagles": {"PHI"}, "Falcons": {"ATL"}, "Giants": {"NYG"}, "Jets": {"NYJ"},
    "Lions": {"DET"}, "Oilers": {"HOU", "OTI"}, "Packers": {"GNB"}, "Patriots": {"NWE", "NE", "BOS"},
    "Pirates": {"PIT"}, "Raiders": {"OAK", "RAI", "LV"}, "Rams": {"RAM", "LAR"},
    "Redskins": {"WAS", "BOS"}, "Saints": {"NOR", "NO"}, "Seahawks": {"SEA"}, "Steelers": {"PIT"},
    "Texans": {"KAN", "DTX"}, "Tigers": {"BKN", "BRK"}, "Titans": {"NYJ"}, "Vikings": {"MIN"},
    "Yanks": {"NYY"}, "Yankees": {"NYY"}, "Bulldogs": {"NYY", "NYB"}, "Buccaneers": {"TAM", "TB"},
    # modern-era + odd-franchise nicknames (used by the season_type witness over the full log corpus)
    "Ravens": {"BAL", "RAV"}, "Jaguars": {"JAX", "JAC"}, "Panthers": {"CAR"}, "Niners": {"SFO"},
    "Commanders": {"WAS"}, "Football Team": {"WAS"}, "Spartans": {"POR", "DET"},
    "Steam Roller": {"PRV"}, "Stapletons": {"STP"}, "Bucs": {"TAM", "TB"},
}

_D = lambda c: f"COALESCE(TRY_CAST({c} AS DOUBLE),0)"


def _load(con) -> None:
    v26 = Path(latest_v26()).as_posix()
    # -- targets: ALL pre-1978 fumble-weeks
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tgt AS
        SELECT CAST(NFL_player_id AS VARCHAR) nfl_id, player, CAST(year AS INT) season,
               CAST(week AS INT) wk, season_type, CAST(game_date AS DATE) gdate,
               nfl_team, opponent_nfl_team, fumbles, fumbles_lost
        FROM read_parquet('{v26}')
        WHERE year < 1978 AND fumbles > 0""")
    yrs = [r[0] for r in con.execute("SELECT DISTINCT season FROM tgt ORDER BY 1").fetchall()]
    yl = ",".join(str(y) for y in yrs)
    # -- OUR game catalog: the one true game matcher (boxscore_id + side + date)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cat AS
        SELECT boxscore_id, CAST(year AS INT) AS yr, CAST(week AS INT) AS wk, team_code,
               opponent_code, is_home, TRY_CAST(game_date AS DATE) AS gdate
        FROM read_parquet('{CATALOG}') WHERE year IN ({yl})""")
    # -- bio crosswalk for BY-ID witness joins
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS
        SELECT DISTINCT pfr_id, CAST(NFL_player_id AS VARCHAR) nfl_id
        FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL""")
    # -- PFR player_offense: per-player fumbles WITH the PFR id (identity-safe) + rush yds (side recon)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pfr_po AS
        SELECT boxscore_id, team, player, player_link_ids pfr_pid, {_D('fumbles')} fum,
               TRY_CAST(rush_yds AS DOUBLE) rush_yds
        FROM read_parquet('{PFR}/player_offense/_combined.parquet')
        WHERE season IN ({yl})""")
    # -- PFR team Fumbles-Lost per boxscore (vis/home "F-L" strings)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pfr_fl AS
        SELECT boxscore_id,
               TRY_CAST(string_split(vis_stat,'-')[1] AS INT)  vis_f,
               TRY_CAST(string_split(vis_stat,'-')[2] AS INT)  vis_l,
               TRY_CAST(string_split(home_stat,'-')[1] AS INT) home_f,
               TRY_CAST(string_split(home_stat,'-')[2] AS INT) home_l
        FROM read_parquet('{PFR}/team_stats/_combined.parquet')
        WHERE LOWER(stat) = 'fumbles-lost' AND season IN ({yl})""")
    # -- SIDE RESOLUTION witness: PFR's vis/home column order is INVERTED for some neutral-site games
    #    (verified: SB II/VI/XII), so neither the boxscore-id suffix nor catalog is_home can be trusted
    #    against the column layout. Resolve each team's side by EXACT rushing-yards concordance:
    #    team_stats 'Rush-Yds-TDs' middle number vs SUM(player_offense.rush_yds) per team (team totals
    #    equal individual sums exactly for rushing). Degenerate/missing -> NULL (caller falls back to
    #    catalog is_home and flags).
    con.execute(f"""CREATE OR REPLACE TEMP TABLE side_map AS
        WITH tsr AS (
          SELECT boxscore_id,
                 TRY_CAST(string_split(vis_stat,'-')[2] AS DOUBLE)  vis_rush,
                 TRY_CAST(string_split(home_stat,'-')[2] AS DOUBLE) home_rush
          FROM read_parquet('{PFR}/team_stats/_combined.parquet')
          WHERE stat = 'Rush-Yds-TDs' AND season IN ({yl})
        ), pr AS (
          SELECT boxscore_id, team, SUM(COALESCE(rush_yds,0)) rush
          FROM pfr_po GROUP BY 1,2
        )
        SELECT p.boxscore_id, p.team,
               CASE WHEN p.rush = t.vis_rush AND p.rush != t.home_rush THEN 'vis'
                    WHEN p.rush = t.home_rush AND p.rush != t.vis_rush THEN 'home'
                    ELSE NULL END side
        FROM pr p JOIN tsr t USING (boxscore_id)""")
    # -- scoring-table fumble-return floor (game-level veto on R1)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pfr_fumret AS
        SELECT DISTINCT boxscore_id
        FROM read_parquet('{PFR}/scoring/_combined.parquet')
        WHERE season IN ({yl}) AND LOWER(description) LIKE '%fumble%return%'""")
    # -- NC targeted logs (game grain). COUNT(DISTINCT x) ignores NULLs: >1 distinct = ambiguous page.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nc AS
        SELECT nflcom_slug, TRY_CAST(season AS INT) season,
               TRY_STRPTIME(game_date, '%m/%d/%Y')::DATE gdate,
               ANY_VALUE(opp) opp,
               MAX(TRY_CAST(fumbles AS DOUBLE)) nc_fum,
               MAX(TRY_CAST(fumbles_lost AS DOUBLE)) nc_lost,
               COUNT(DISTINCT TRY_CAST(fumbles AS DOUBLE)) n_fumvals,
               COUNT(DISTINCT TRY_CAST(fumbles_lost AS DOUBLE)) n_lostvals
        FROM read_parquet('{NC_TARGETED}')
        WHERE TRY_CAST(fumbles AS DOUBLE) IS NOT NULL
          AND TRY_STRPTIME(game_date, '%m/%d/%Y') IS NOT NULL
        GROUP BY 1,2,3""")


def _slug_candidates(names: pd.DataFrame) -> pd.DataFrame:
    """player -> era-matching NC slug candidates via the shared player universe."""
    import json
    import re
    import unicodedata
    uni = json.loads(Path("D:/league-history-data/nfl/raw/nflcom/player_universe.json").read_text())

    def slugify(name: str) -> str:
        s = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode()
        s = re.sub(r"[.']", "", s.lower())
        return re.sub(r"[^a-z0-9]+", "-", s).strip("-")

    rows = []
    for _, r in names.iterrows():
        base = slugify(r["player"])
        cands = [s for s in [base] + [f"{base}-{i}" for i in range(2, 6)]
                 if s in uni and uni[s] and min(uni[s]) <= r["season"] <= max(uni[s])]
        for c in cands:
            rows.append({"player": r["player"], "season": r["season"], "nflcom_slug": c})
    return pd.DataFrame(rows, columns=["player", "season", "nflcom_slug"])


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    con.execute("PRAGMA threads=3")
    _load(con)

    # ---- game matching: catalog-exact on (year, week, team); side by rushing concordance,
    #      catalog is_home only as flagged fallback; date recon asserted
    m = con.execute("""
        SELECT t.*, c.boxscore_id, c.gdate AS pfr_gdate, c.is_home, c.opponent_code AS cat_opp,
               COALESCE(s.side, CASE WHEN c.is_home THEN 'home' ELSE 'vis' END) AS side,
               CASE WHEN s.side IS NOT NULL THEN 'rush-exact' ELSE 'catalog-is_home' END AS side_source,
               CASE COALESCE(s.side, CASE WHEN c.is_home THEN 'home' ELSE 'vis' END)
                    WHEN 'home' THEN f.home_f ELSE f.vis_f END AS team_f,
               CASE COALESCE(s.side, CASE WHEN c.is_home THEN 'home' ELSE 'vis' END)
                    WHEN 'home' THEN f.home_l ELSE f.vis_l END AS team_l
        FROM tgt t
        LEFT JOIN cat c ON c.yr=t.season AND c.wk=t.wk AND c.team_code=t.nfl_team
        LEFT JOIN pfr_fl f ON f.boxscore_id=c.boxscore_id
        LEFT JOIN side_map s ON s.boxscore_id=c.boxscore_id AND s.team=t.nfl_team
    """).fetchdf()
    assert len(m) == con.execute("SELECT COUNT(*) FROM tgt").fetchone()[0], "catalog join fanout"
    unmatched_cat = m[m["boxscore_id"].isna()]
    date_drift = m[m["gdate"].notna() & m["pfr_gdate"].notna() & (m["gdate"] != m["pfr_gdate"])]
    opp_drift = m[m["boxscore_id"].notna() & (m["opponent_nfl_team"] != m["cat_opp"])]
    assert len(date_drift) == 0, f"catalog/super game_date drift on {len(date_drift)} rows"
    assert len(opp_drift) == 0, f"catalog/super opponent drift on {len(opp_drift)} rows"
    m["join_date"] = m["gdate"].where(m["gdate"].notna(), m["pfr_gdate"])

    # ---- PFR BY-ID individual receipt: our nfl_id -> bio.pfr_id -> player_offense rows of this game
    pfr_by_id = con.execute("""
        SELECT p.boxscore_id, b.nfl_id, SUM(p.fum) pfr_fum_by_id
        FROM pfr_po p JOIN bio b ON b.pfr_id = p.pfr_pid
        GROUP BY 1,2""").fetchdf()
    m = m.merge(pfr_by_id, left_on=["boxscore_id", "nfl_id"],
                right_on=["boxscore_id", "nfl_id"], how="left")
    # twin risk: a PFR row in this game name-matches our player but its id is NOT ours
    pfr_names = con.execute("""
        SELECT p.boxscore_id, LOWER(TRIM(p.player)) pname, b.nfl_id
        FROM pfr_po p LEFT JOIN bio b ON b.pfr_id = p.pfr_pid""").fetchdf()
    nm = m.assign(pname=m["player"].str.lower().str.strip()) \
          .merge(pfr_names, on=["boxscore_id", "pname"], how="left", suffixes=("", "_pfrrow"))
    twin = nm[nm["nfl_id_pfrrow"].notna() & (nm["nfl_id_pfrrow"] != nm["nfl_id"])]
    m["pfr_name_twin_risk"] = m.set_index(["nfl_id", "season", "wk"]).index.isin(
        twin.set_index(["nfl_id", "season", "wk"]).index)

    # ---- NC values per target row (slug candidates x exact game date)
    cands = _slug_candidates(m[["player", "season"]].drop_duplicates())
    nc = con.execute("SELECT * FROM nc").fetchdf()
    mn = m.merge(cands, on=["player", "season"], how="left") \
          .merge(nc, left_on=["nflcom_slug", "season", "join_date"],
                 right_on=["nflcom_slug", "season", "gdate"], how="left", suffixes=("", "_nc"))
    # twin audit BEFORE collapse: >1 slug candidate with NC rows on the same game date = unresolved twin
    key = ["nfl_id", "season", "wk"]
    ncand = mn[mn["nc_fum"].notna()].groupby(key)["nflcom_slug"].nunique()
    nc_twin_keys = set(ncand[ncand > 1].index)
    # collapse: keep the candidate row that carried NC data (wrong-twin empty pages drop out)
    mn["has_nc"] = mn["nc_fum"].notna()
    mn = mn.sort_values("has_nc", ascending=False).drop_duplicates(subset=key, keep="first")
    assert len(mn) == len(m), f"row fanout after NC join: {len(m)} -> {len(mn)}"
    mn["nc_twin_ambiguous"] = mn.set_index(key).index.isin(nc_twin_keys)

    # multi-valued NC tables for one game = ambiguous page
    bad = (mn["n_fumvals"].fillna(1) > 1) | (mn["n_lostvals"].fillna(1) > 1)
    # NC identity gate: opp nickname must map to the catalog opponent for this exact game
    def _nick_ok(r):
        if pd.isna(r["nc_fum"]) or pd.isna(r["opp"]) or pd.isna(r["cat_opp"]):
            return None
        nick = str(r["opp"]).lstrip("@").strip()
        codes = NICKNAME_CODES.get(nick)
        if codes is None:
            raise RuntimeError(f"unmapped NC opp nickname on a target game: {nick!r} "
                               f"({r['player']} {r['season']} wk{r['wk']})")
        return r["cat_opp"] in codes
    mn["nc_identity_ok"] = mn.apply(_nick_ok, axis=1)
    drop_nc = bad | mn["nc_twin_ambiguous"] | (mn["nc_identity_ok"] == False)  # noqa: E712
    mn.loc[drop_nc, ["nc_fum", "nc_lost"]] = None

    # ---- team-game accounting: S, known_HL, null capacity
    mn["_fl_known"] = mn["fumbles_lost"].fillna(0) * mn["fumbles_lost"].notna()
    mn["_null_cap"] = mn["fumbles"] * mn["fumbles_lost"].isna()
    gsum = mn.groupby(["boxscore_id", "nfl_team"], dropna=True).agg(
        nc_lost_sum=("nc_lost", "sum"), nc_fum_sum=("nc_fum", "sum"),
        team_f=("team_f", "first"), team_l=("team_l", "first"),
        held_fum_sum=("fumbles", "sum"), held_lost_known=("_fl_known", "sum"),
        null_cap=("_null_cap", "sum")).reset_index()
    game_has_signal = mn.groupby("boxscore_id")["nc_lost"].max().rename("game_max_nc_lost")
    gsum = gsum.merge(game_has_signal, on="boxscore_id", how="left")
    gsum["nc_team_ok"] = (
        (gsum["game_max_nc_lost"].fillna(0) > 0)
        & gsum["team_l"].notna()
        & (gsum["nc_lost_sum"].fillna(0) <= gsum["team_l"].fillna(-1))
        & (gsum["nc_fum_sum"].fillna(0) <= gsum["team_f"].fillna(-1))
    )
    mn = mn.merge(gsum[["boxscore_id", "nfl_team", "nc_team_ok", "held_fum_sum",
                        "held_lost_known", "null_cap"]],
                  on=["boxscore_id", "nfl_team"], how="left")
    fr = set(r[0] for r in con.execute("SELECT boxscore_id FROM pfr_fumret").fetchall())
    mn["game_has_fumret_score"] = mn["boxscore_id"].isin(fr)

    # ---- decide
    def decide(r) -> tuple[str, object]:
        cur = r["fumbles_lost"]
        if pd.isna(r["team_l"]):
            return ("NO_PFR_TEAMSTATS", cur)
        if r["held_fum_sum"] > r["team_f"]:
            return ("CONFLICT_FUMSUM_GT_TEAM", cur)
        if r["held_lost_known"] > r["team_l"]:
            return ("CONFLICT_HELDLOST_GT_TEAM", cur)
        if r["team_l"] == 0:
            if r["game_has_fumret_score"]:
                return ("CONFLICT_FUMRET_VS_L0", cur)
            return ("R1_TEAM_ZERO", 0.0)
        if r["team_l"] == r["team_f"]:
            return ("R2_ALL_LOST", float(r["fumbles"]))
        if bool(r["nc_team_ok"]) and pd.notna(r["nc_lost"]):
            return ("R3_NC_TRUSTED", float(min(r["nc_lost"], r["fumbles"])))
        if pd.notna(cur) and cur == 0 and \
                (r["team_l"] - r["held_lost_known"]) > (r["team_f"] - r["held_fum_sum"]) + r["null_cap"]:
            return ("R4_NULL_DEFAULT_ZERO", None)
        if pd.notna(cur):
            return ("KEEP_UNVERIFIED", cur)
        return ("AMBIGUOUS_NULL", None)

    dec = mn.apply(decide, axis=1)
    mn["rule"] = [d[0] for d in dec]
    mn["new_fl"] = [d[1] for d in dec]
    mn["changed"] = ~((mn["new_fl"].isna() & mn["fumbles_lost"].isna())
                      | (mn["new_fl"].notna() & mn["fumbles_lost"].notna()
                         & (mn["new_fl"] == mn["fumbles_lost"])))

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = utc_stamp()
    csv = OUT_DIR / f"pre1978_fumbles_lost_decisions_{stamp}.csv"
    cols = ["nfl_id", "player", "season", "wk", "season_type", "nfl_team", "opponent_nfl_team",
            "boxscore_id", "side", "side_source", "team_f", "team_l", "held_fum_sum", "fumbles",
            "pfr_fum_by_id", "pfr_name_twin_risk", "nflcom_slug", "nc_fum", "nc_lost",
            "nc_identity_ok", "nc_twin_ambiguous", "nc_team_ok", "game_has_fumret_score",
            "fumbles_lost", "rule", "new_fl", "changed"]
    mn[cols].sort_values(["season", "wk", "player"]).to_csv(csv, index=False)

    summary = mn.groupby("rule").agg(rows=("rule", "size"), changed=("changed", "sum"),
                                     lost_added=("new_fl", "sum")).to_dict("index")
    res = {"targets": len(mn), "decision_csv": str(csv),
           "catalog_unmatched": int(len(unmatched_cat)),
           "side_fallback_rows": int((mn["side_source"] != "rush-exact").sum()),
           "pfr_fum_by_id_receipts": int(mn["pfr_fum_by_id"].notna().sum()),
           "pfr_fum_by_id_agree": int((mn["pfr_fum_by_id"] == mn["fumbles"]).sum()),
           "pfr_name_twin_risk_rows": int(mn["pfr_name_twin_risk"].sum()),
           "nc_identity_fail_rows": int((mn["nc_identity_ok"] == False).sum()),  # noqa: E712
           "nc_twin_ambiguous_rows": int(mn["nc_twin_ambiguous"].sum()),
           "changed_total": int(mn["changed"].sum()),
           "by_rule": {k: {kk: (int(vv) if pd.notna(vv) else 0) for kk, vv in v.items()}
                       for k, v in summary.items()}}
    if not apply:
        con.close()
        return res

    con.close()

    # ---- apply: single STREAMING pass (SELECT * REPLACE + 167-key hash join -> ParquetWriter).
    #      Never materializes the 10.6M x 1089 table (a CREATE TABLE + UPDATE + COPY attempt thrashed
    #      for an hour inside a 3GB limit); gates run as cheap aggregates before/after.
    import pyarrow.parquet as pq

    changes = mn[mn["changed"]][["nfl_id", "season", "wk", "new_fl", "fumbles_lost"]].copy()
    n_expected = len(changes)
    v26 = Path(latest_v26())
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con2 = duckdb.connect()
    con2.execute("PRAGMA threads=4"); con2.execute("PRAGMA disable_progress_bar")
    con2.execute("SET preserve_insertion_order=false"); con2.execute("SET memory_limit='6GB'")
    con2.execute(f"SET temp_directory='{sp}'"); con2.execute("SET max_temp_directory_size='40GB'")
    con2.register("chg", changes)
    src = v26.as_posix()
    before_rows = con2.execute(f"SELECT COUNT(*) FROM read_parquet('{src}')").fetchone()[0]
    hit = con2.execute(f"""SELECT COUNT(*) FROM read_parquet('{src}') st JOIN chg
        ON CAST(st.NFL_player_id AS VARCHAR)=chg.nfl_id AND CAST(st.year AS INT)=chg.season
           AND CAST(st.week AS INT)=chg.wk
        WHERE st.year<1978 AND st.fumbles>0""").fetchone()[0]
    assert hit == n_expected, f"key gate: {hit} matched vs {n_expected} expected"
    tmp = v26.with_name(v26.stem + "_fl1978tmp.parquet")
    rb = con2.execute(f"""
        SELECT st.* REPLACE (
            CASE WHEN c.nfl_id IS NOT NULL THEN c.new_fl ELSE st.fumbles_lost END AS fumbles_lost)
        FROM read_parquet('{src}') st
        LEFT JOIN chg c
          ON CAST(st.NFL_player_id AS VARCHAR)=c.nfl_id AND CAST(st.year AS INT)=c.season
             AND CAST(st.week AS INT)=c.wk AND st.year<1978 AND st.fumbles>0
    """).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    after_rows = con2.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    viol = con2.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}') WHERE COALESCE(fumbles_lost,0) > COALESCE(fumbles,0) AND year<1978").fetchone()[0]
    misapplied = con2.execute(f"""SELECT COUNT(*) FROM read_parquet('{tq}') st JOIN chg
        ON CAST(st.NFL_player_id AS VARCHAR)=chg.nfl_id AND CAST(st.year AS INT)=chg.season
           AND CAST(st.week AS INT)=chg.wk
        WHERE st.year<1978 AND st.fumbles>0
          AND st.fumbles_lost IS DISTINCT FROM chg.new_fl""").fetchone()[0]
    con2.close()
    gate = (after_rows == before_rows and viol == 0 and misapplied == 0)
    res["rows_unchanged"] = after_rows == before_rows
    res["fl_gt_fum_violations"] = int(viol)
    res["misapplied_rows"] = int(misapplied)
    if not gate:
        res["swapped"] = False
        res["temp"] = str(tmp)
        shutil.rmtree(sp, ignore_errors=True)
        return res
    backup = v26.with_name(v26.stem + f"_prefl1978_{stamp}.parquet")
    shutil.copy2(v26, backup)
    os.replace(tmp, v26)
    res["backup"] = backup.name
    res["swapped"] = True
    res["next"] = ("build_rescore_fpts_v26 --apply -> build_weekly_ranks_v26 -> build_season_career_v26 "
                   "-> build_season_career_ranks_v26 -> gates")
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    import json
    print(json.dumps(run(apply=a.apply), indent=1, default=str))
