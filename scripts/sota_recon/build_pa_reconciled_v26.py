"""
sota_recon/build_pa_reconciled_v26.py  --  witness-reconciled DST points-allowed (total + eligible).

Replaces the fragile reconstruction `pts_allow = tds*6 + 2pt*2 + fg*3 + pat*1` (defense_stats.py:779)
that inflated `points_allowed` by ~6 per opponent TD for 2014+ (only 41% match the scoreboard in 2024).

Two anchors, one gate:
  1. TOTAL PA   = the scoreboard authority: opponent's points for this team-game (nfl_team_games_all),
                  corroborated by the scoring-table final running score. Exact 1920-2025 by construction.
  2. ELIGIBLE PA = total - 6*(opponent defensive-return TDs: pick-6 + fumble-6 off THIS team's offense)
                  - 2*(opponent safeties). Keeps PATs / 2pt / FG / ST-return-TDs (per fantasy convention:
                  ESPN post-2019 excludes only turnovers-returned-for-TD; ST returns count).
  3. RECONCILIATION GATE: independently reconstruct each team's own score from raw boxscore witnesses
                  (player_offense rush/rec TD, player_defense int/fum-ret TD, returns kr/pr TD, kicking
                  fgm/xpm, scoring safety/2pt) and require it to equal the scoreboard. Non-reconciling
                  games are flagged (not trusted) for pbp resolution.

Join key: the unique game = (franchise, game_date). Where game_date is present it fully disambiguates
doubleheaders; where it is null (older/expanded rows) fall back to (year, week, fr, opponent_fr).

Writes points_allowed(total), dst_points_allowed(eligible), pts_allow(=eligible, drives buckets), and
recomputes the pts_allow_* one-hot buckets from ELIGIBLE PA so DST scoring tiers off eligible.
`build_dst_scoring_v26` then recomputes pts_def_std/fpts_* from the corrected buckets (+ GREATEST TD fix).

    python -m scripts.sota_recon.build_pa_reconciled_v26            # dry-run: report before/after + gate
    python -m scripts.sota_recon.build_pa_reconciled_v26 --apply    # gated write + swap
"""
from __future__ import annotations
import argparse, os, shutil
from pathlib import Path
import duckdb, pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
PROV = "wave60.pa_reconciled"; PROV_COL = "recon_correction_log"
PA_BUCKETS = ["pts_allow_0", "pts_allow_1_6", "pts_allow_7_13", "pts_allow_14_20",
              "pts_allow_21_27", "pts_allow_28_34", "pts_allow_35_plus"]


def _build_witnesses(con, v26):
    # Scoreboard (one row per team-game). total PA for defending fr = opponent_points.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sb AS
        SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, CAST(team_fid AS INT) fr, team_code,
               CAST(opponent_fid AS INT) ofr, CAST(game_date AS DATE) gd, is_home, boxscore_id,
               CAST(opponent_points AS INT) total_pa, CAST(team_points AS INT) team_points
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL AND opponent_points IS NOT NULL""")
    # unique-game (fr, game_date) scoreboard + week-key fallback
    con.execute("""CREATE OR REPLACE TEMP TABLE sb_date AS
        SELECT fr, gd, MAX(total_pa) total_pa FROM sb WHERE gd IS NOT NULL GROUP BY 1,2""")
    con.execute("""CREATE OR REPLACE TEMP TABLE sb_key AS
        SELECT yr, wk, fr, ofr, MAX(total_pa) total_pa FROM sb GROUP BY 1,2,3,4""")

    # Ineligible chunk from the OPPONENT's reconciled DEF-row atoms, game-keyed.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE oi AS
        SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, CAST(nfl_franchise_number AS INT) fr,
               CAST(opponent_nfl_franchise_number AS INT) ofr, CAST(game_date AS DATE) gd,
               6*COALESCE(TRY_CAST(def_int_ret_td AS INT),0) + 6*COALESCE(TRY_CAST(fum_ret_td AS INT),0)
                 + 2*COALESCE(TRY_CAST(def_safeties AS INT),0) AS inelig_pts
        FROM read_parquet('{v26}') WHERE position='DEF' AND nfl_franchise_number IS NOT NULL""")
    con.execute("""CREATE OR REPLACE TEMP TABLE oi_date AS
        SELECT fr, gd, MAX(inelig_pts) inelig FROM oi WHERE gd IS NOT NULL GROUP BY 1,2""")
    con.execute("""CREATE OR REPLACE TEMP TABLE oi_key AS
        SELECT yr, wk, fr, ofr, MAX(inelig_pts) inelig FROM oi GROUP BY 1,2,3,4""")

    # DEF-row identities (the rows we will update), and the hybrid game match.
    con.execute(f"""CREATE OR REPLACE TEMP TABLE d_def AS
        SELECT DISTINCT CAST(year AS INT) yr, CAST(week AS INT) wk, CAST(nfl_franchise_number AS INT) fr,
               CAST(opponent_nfl_franchise_number AS INT) ofr, CAST(game_date AS DATE) gd
        FROM read_parquet('{v26}') WHERE position='DEF' AND nfl_franchise_number IS NOT NULL""")
    # pa_fix: unique per DEF-row identity (yr,wk,fr,ofr,gd). game_date-first, else week-key.
    con.execute("""CREATE OR REPLACE TEMP TABLE pa_fix AS
        SELECT dd.yr, dd.wk, dd.fr, dd.ofr, dd.gd,
          COALESCE(bd.total_pa, bk.total_pa) AS new_total_pa,
          (bd.total_pa IS NOT NULL) AS matched_by_date,
          GREATEST(COALESCE(bd.total_pa, bk.total_pa) - COALESCE(od.inelig, ok.inelig, 0), 0) AS new_eligible_pa
        FROM d_def dd
          LEFT JOIN sb_date bd ON dd.gd IS NOT NULL AND bd.fr=dd.fr AND bd.gd=dd.gd
          LEFT JOIN sb_key  bk ON bk.yr=dd.yr AND bk.wk=dd.wk AND bk.fr=dd.fr AND bk.ofr=dd.ofr
          LEFT JOIN oi_date od ON dd.gd IS NOT NULL AND od.fr=dd.ofr AND od.gd=dd.gd
          LEFT JOIN oi_key  ok ON ok.yr=dd.yr AND ok.wk=dd.wk AND ok.fr=dd.ofr AND ok.ofr=dd.fr""")

    # Raw-boxscore reconstruction of each team's OWN points (the independent gate).
    def agg(name, tbl, cols):
        sel = ", ".join(f"SUM(COALESCE(TRY_CAST({c} AS INT),0)) {a}" for c, a in cols)
        con.execute(f"""CREATE OR REPLACE TEMP TABLE {name} AS SELECT boxscore_id, team, {sel}
            FROM read_parquet('{BOX}/{tbl}/_combined.parquet') GROUP BY 1,2""")
    agg("w_off", "player_offense", [("rush_td", "rush_td"), ("rec_td", "rec_td")])
    agg("w_def", "player_defense", [("def_int_td", "int_td"), ("fumbles_rec_td", "fum_td")])
    agg("w_ret", "returns", [("kick_ret_td", "kr_td"), ("punt_ret_td", "pr_td")])
    agg("w_kik", "kicking", [("fgm", "fgm"), ("xpm", "xpm")])
    con.execute(f"""CREATE OR REPLACE TEMP TABLE scd AS
        SELECT boxscore_id, lower(COALESCE(description,'')) d,
          TRY_CAST(vis_team_score AS INT)-LAG(TRY_CAST(vis_team_score AS INT),1,0)
            OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dvis,
          TRY_CAST(home_team_score AS INT)-LAG(TRY_CAST(home_team_score AS INT),1,0)
            OVER (PARTITION BY boxscore_id ORDER BY row_index_in_table) dhome
        FROM read_parquet('{BOX}/scoring/_combined.parquet') WHERE description IS NOT NULL AND description<>''""")
    con.execute("""CREATE OR REPLACE TEMP TABLE w_sc AS
        SELECT boxscore_id, (dhome>0 AND dhome>=dvis) is_home,
          SUM(CASE WHEN d LIKE '%safety%' THEN 1 ELSE 0 END) safeties,
          SUM(CASE WHEN d LIKE '%two point%' OR d LIKE '%two-point%' OR d LIKE '%2-point%'
                    OR d LIKE '%conversion succ%' THEN 1 ELSE 0 END) two_pt
        FROM scd WHERE dvis>0 OR dhome>0 GROUP BY 1,2""")
    con.execute("""CREATE OR REPLACE TEMP TABLE recon AS
        SELECT sb.yr, sb.wk, sb.fr, sb.team_points,
          6*(COALESCE(o.rush_td,0)+COALESCE(o.rec_td,0)+COALESCE(dd.int_td,0)+COALESCE(dd.fum_td,0)
             +COALESCE(r.kr_td,0)+COALESCE(r.pr_td,0))
            +3*COALESCE(k.fgm,0)+1*COALESCE(k.xpm,0)
            +2*COALESCE(s.safeties,0)+2*COALESCE(s.two_pt,0) AS recon_pts
        FROM sb
          LEFT JOIN w_off o ON o.boxscore_id=sb.boxscore_id AND o.team=sb.team_code
          LEFT JOIN w_def dd ON dd.boxscore_id=sb.boxscore_id AND dd.team=sb.team_code
          LEFT JOIN w_ret r ON r.boxscore_id=sb.boxscore_id AND r.team=sb.team_code
          LEFT JOIN w_kik k ON k.boxscore_id=sb.boxscore_id AND k.team=sb.team_code
          LEFT JOIN w_sc s ON s.boxscore_id=sb.boxscore_id AND s.is_home=sb.is_home""")


def _bucket_case(col):
    return (f"CASE WHEN {col}=0 THEN 'pts_allow_0' WHEN {col}<=6 THEN 'pts_allow_1_6' "
            f"WHEN {col}<=13 THEN 'pts_allow_7_13' WHEN {col}<=20 THEN 'pts_allow_14_20' "
            f"WHEN {col}<=27 THEN 'pts_allow_21_27' WHEN {col}<=34 THEN 'pts_allow_28_34' "
            f"ELSE 'pts_allow_35_plus' END")


def run(apply=False):
    v26 = latest_v26()
    con = duckdb.connect(); con.execute("SET memory_limit='7GB'"); con.execute("PRAGMA threads=3")
    _build_witnesses(con, v26)

    gate = con.execute("""SELECT COUNT(*) n,
        ROUND(100.0*AVG((recon_pts=team_points)::INT),1) exact_pct,
        SUM((recon_pts>team_points)::INT) recon_over,
        SUM((recon_pts<>team_points)::INT) flagged
        FROM recon WHERE yr>=2000""").fetchone()

    coverage = con.execute("""SELECT COUNT(*) def_ids,
        SUM((new_total_pa IS NULL)::INT) unmatched_no_scoreboard,
        SUM(matched_by_date::INT) matched_by_game_date,
        SUM((new_total_pa IS NOT NULL AND NOT matched_by_date)::INT) matched_by_weekkey
        FROM pa_fix""").fetchone()

    ba = con.execute(f"""
        WITH d AS (SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, CAST(nfl_franchise_number AS INT) fr,
                     CAST(opponent_nfl_franchise_number AS INT) ofr, CAST(game_date AS DATE) gd,
                     MAX(TRY_CAST(points_allowed AS INT)) old_pa
                   FROM read_parquet('{v26}') WHERE position='DEF' AND nfl_franchise_number IS NOT NULL
                   GROUP BY 1,2,3,4,5)
        SELECT CASE WHEN d.yr<2014 THEN 'pre-2014' ELSE '2014+' END era, COUNT(*) n,
          ROUND(100.0*AVG((d.old_pa=p.new_total_pa)::INT),1) old_match_pct,
          SUM((d.old_pa IS DISTINCT FROM p.new_total_pa)::INT) rows_changed
        FROM d JOIN pa_fix p ON p.yr=d.yr AND p.wk=d.wk AND p.fr=d.fr AND p.ofr=d.ofr
                             AND p.gd IS NOT DISTINCT FROM d.gd
        WHERE p.new_total_pa IS NOT NULL GROUP BY 1 ORDER BY 1""").df()

    lions = con.execute(f"""
        WITH d AS (SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, CAST(nfl_franchise_number AS INT) fr,
                     CAST(opponent_nfl_franchise_number AS INT) ofr, CAST(game_date AS DATE) gd,
                     ANY_VALUE(player) player, ANY_VALUE(opponent_nfl_team) opp,
                     MAX(TRY_CAST(points_allowed AS INT)) old_total, MAX(TRY_CAST(dst_points_allowed AS INT)) old_elig
                   FROM read_parquet('{v26}') WHERE position='DEF'
                     AND ((year=1950 AND week=1 AND nfl_team='DET')
                          OR (year=2024 AND week=1 AND nfl_team IN ('CAR','ARI','BAL','CHI')))
                   GROUP BY 1,2,3,4,5)
        SELECT d.yr, d.wk, d.player, d.opp, d.old_total, d.old_elig, p.new_total_pa, p.new_eligible_pa
        FROM d JOIN pa_fix p ON p.yr=d.yr AND p.wk=d.wk AND p.fr=d.fr AND p.ofr=d.ofr
                             AND p.gd IS NOT DISTINCT FROM d.gd
        ORDER BY d.yr, d.player""").df()

    res = {"v26": v26, "gate": gate, "coverage": coverage, "before_after": ba, "samples": lions}
    if not apply:
        con.close()
        return res

    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con2 = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con2.execute("PRAGMA threads=1"); con2.execute("PRAGMA disable_progress_bar")
    con2.execute("SET preserve_insertion_order=false"); con2.execute("SET memory_limit='6GB'")
    con2.execute(f"SET temp_directory='{sp}'")
    con2.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    _build_witnesses(con2, v26)
    existing = set(c[0] for c in con2.execute("DESCRIBE st").fetchall())
    for b in PA_BUCKETS:
        if b not in existing:
            con2.execute(f"ALTER TABLE st ADD COLUMN {b} INTEGER")
    if PROV_COL not in existing:
        con2.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con2.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con2.execute(f"""UPDATE st SET points_allowed=p.new_total_pa, dst_points_allowed=p.new_eligible_pa,
          pts_allow=p.new_eligible_pa,
          {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM pa_fix p WHERE st.position='DEF' AND p.new_total_pa IS NOT NULL
          AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk
          AND CAST(st.nfl_franchise_number AS INT)=p.fr
          AND CAST(st.opponent_nfl_franchise_number AS INT)=p.ofr
          AND CAST(st.game_date AS DATE) IS NOT DISTINCT FROM p.gd""")
    for b in PA_BUCKETS:
        con2.execute(f"UPDATE st SET {b}=0 WHERE position='DEF'")
    bc = _bucket_case("dst_points_allowed")
    for b in PA_BUCKETS:
        con2.execute(f"UPDATE st SET {b}=1 WHERE position='DEF' AND dst_points_allowed IS NOT NULL AND {bc}='{b}'")
    con2.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} LIKE '%,%'""")
    after = con2.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    # post-witness: among DEF rows with a scoreboard match, points_allowed must equal it
    post = con2.execute("""
        SELECT ROUND(100.0*AVG((TRY_CAST(st.points_allowed AS INT)=p.new_total_pa)::INT),2)
        FROM st JOIN pa_fix p ON st.position='DEF' AND p.new_total_pa IS NOT NULL
          AND CAST(st.year AS INT)=p.yr AND CAST(st.week AS INT)=p.wk
          AND CAST(st.nfl_franchise_number AS INT)=p.fr AND CAST(st.opponent_nfl_franchise_number AS INT)=p.ofr
          AND CAST(st.game_date AS DATE) IS NOT DISTINCT FROM p.gd""").fetchone()[0]
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_patmp.parquet")
    r = con2.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con2.close(); shutil.rmtree(sp, ignore_errors=True)
    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate_ok = (g["failed"] == 0) and (after == before) and (post >= 99.9)
    res.update({"before": before, "after": after, "post_scoreboard_pct": post,
                "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate_ok), "temp": str(tmp)})
    if gate_ok:
        bk = vp.with_name(vp.stem + f"_prepa_{stamp}.parquet"); shutil.copy2(vp, bk)
        os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true"); a = ap.parse_args()
    r = run(apply=a.apply)
    print(f"v26: {Path(r['v26']).parent.parent.name}\n")
    n, exact, over, flagged = r["gate"]
    print("[reconciliation gate] 2000+ raw-boxscore reconstruction vs scoreboard:")
    print(f"  {n:,} team-games | exact={exact}% | reconstruction_over_scoreboard={over} | flagged(need pbp)={flagged}")
    c_ids, c_un, c_date, c_key = r["coverage"]
    print(f"\n[game match] {c_ids:,} DEF-row identities | by game_date={c_date:,} | by week-key={c_key:,} "
          f"| no-scoreboard(unchanged)={c_un}\n")
    print("[before/after] v26 points_allowed vs scoreboard authority:")
    print(r["before_after"].to_string(index=False)); print()
    print("[samples] before -> after (total_pa, eligible_pa):")
    print(r["samples"].to_string(index=False))
    if a.apply:
        print(f"\nrows {r['before']:,}->{r['after']:,} | post points_allowed==scoreboard: {r['post_scoreboard_pct']}% "
              f"| golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + ("SWAPPED " + r.get("backup", "") if r.get("swapped") else f"NOT swapped; {r.get('temp','')}"))
