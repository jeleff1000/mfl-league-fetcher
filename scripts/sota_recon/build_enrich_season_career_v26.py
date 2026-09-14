"""
sota_recon/build_enrich_season_career_v26.py  --  enrich season/career artifacts from bio + PFR awards.

Adds, to the 4 season/career parquets (tables/season_career_v26/), columns that were hiding in
player_bio + the PFR awards tables:

SEASON (player_nfl_season[_all]):
  age                  = season year - year(bio.birth_date)
  all_pro_first_team   = 1 if a consensus '1st Tm' All-Pro that year (PFR all_pro, via bio.pfr_id)
  all_pro_second_team  = 1 if '2nd Tm' (and not 1st)
  pro_bowl             = 1 if a Pro Bowl that year (PFR context pro_bowl, via player_link_ids=pfr_id)
  mvp                  = 1 if AP-MVP winner that year (voting_apmvp top vote share)

CAREER (player_nfl_career[_all]):
  career-static bio: birth_date, age_at_draft, height, weight, college, conference, draft_year/round/
    overall, nfl_draft_team, is_undrafted, rookie_year, years_active, seasons_started, hof, allpro,
    probowls, w_av, dr_av, forty, bench, vertical, broad_jump, cone, shuttle, ras_score, high_school, birth_place
  position                  = compact career eligibility after build_position_eligibility_v26 runs
  position_candidates        = dual-eligibility from the canonical position overlay (Blanda "QB,K" etc.)
  career_all_pro_first/second, career_pro_bowls, career_mvps = sums of the per-season award flags

Awards key on bio.pfr_id -> NFL_player_id (~75% of all_pro players join; the rest are pre-merge/older).
Local-only, additive. Gate: per-table row counts unchanged; new cols present; spot anchors sane.

    python -m scripts.sota_recon.build_enrich_season_career_v26 [--apply]
"""

from __future__ import annotations
import argparse
import os
import shutil
from pathlib import Path
import duckdb
import pyarrow.parquet as pq
from .sources import latest_v26
from .recon_common import utc_stamp

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
ALLPRO = "D:/league-history-data/nfl/raw/pfr/players/tables/all_pro/**/*.parquet"
PROBOWL = "D:/league-history-data/nfl/raw/pfr/context/tables/pro_bowl/**/*.parquet"
MVP = "D:/league-history-data/nfl/raw/pfr/context/tables/voting_apmvp/**/*.parquet"
# additional AP award winners (same structure: player_link_ids, year, share, votes) -> season flag + career count
VOTING = {
    "opoy": "voting_apopoy",  # Offensive Player of the Year
    "dpoy": "voting_apdpoy",  # Defensive Player of the Year
    "oroy": "voting_aporoy",  # Offensive Rookie of the Year
    "droy": "voting_apdroy",  # Defensive Rookie of the Year
    "cpoy": "voting_apcpoy",  # Comeback Player of the Year
}
_VOTE_DIR = "D:/league-history-data/nfl/raw/pfr/context/tables/{}/**/*.parquet"
OVERLAY_GLOB = "D:/league-history-data/nfl/curated/player_identity/position_overlays/player_bio_canonical_position_overlay_*.parquet"

# career-static bio cols to denormalize (must exist in bio)
STATIC = [
    "birth_date",
    "age_at_draft",
    "height",
    "weight",
    "college",
    "conference",
    "draft_year",
    "draft_round",
    "draft_overall",
    "nfl_draft_team",
    "is_undrafted",
    "rookie_year",
    "years_active",
    "seasons_started",
    "hof",
    "allpro",
    "probowls",
    "w_av",
    "dr_av",
    "forty",
    "bench",
    "vertical",
    "broad_jump",
    "cone",
    "shuttle",
    "ras_score",
    "high_school",
    "birth_place",
]


def _setup(con):
    bio = Path(BIO).as_posix()
    v26 = Path(latest_v26()).as_posix()
    con.execute(f"CREATE OR REPLACE TEMP TABLE bio AS SELECT * FROM '{bio}'")
    # pfr_id -> NFL_player_id (primary award join)
    con.execute("""CREATE OR REPLACE TEMP TABLE pfr2nfl AS
        SELECT pfr_id, ANY_VALUE(NFL_player_id) NFL_player_id FROM bio
        WHERE pfr_id IS NOT NULL AND NFL_player_id IS NOT NULL GROUP BY pfr_id""")
    # name+year -> NFL_player_id (FALLBACK award join: recovers ~25% of awards whose pfr_id isn't in bio).
    # COLLISION-SAFE: 555 name+year keys map to >1 player (three "Chris Jones" 2018 etc.), so ANY_VALUE
    # would misassign. Keep ONLY keys that resolve to a SINGLE id (HAVING COUNT(DISTINCT)=1). Plus
    # side-scoped maps (offense / defense) so same-name-diff-side collisions still resolve for the voting
    # awards whose side is implied by the award (DPOY->defense, OPOY->offense).
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nm2n AS
        SELECT LOWER(TRIM(player)) nm, year yr, ANY_VALUE(NFL_player_id) nid
        FROM '{v26}' WHERE player IS NOT NULL AND NFL_player_id IS NOT NULL
        GROUP BY 1,2 HAVING COUNT(DISTINCT NFL_player_id)=1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nm2n_off AS
        SELECT LOWER(TRIM(player)) nm, year yr, ANY_VALUE(NFL_player_id) nid
        FROM '{v26}' WHERE player IS NOT NULL AND NFL_player_id IS NOT NULL
          AND list_has_any(string_split(COALESCE(position, ''), ','), ['QB','RB','WR','TE','OL'])
        GROUP BY 1,2 HAVING COUNT(DISTINCT NFL_player_id)=1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE nm2n_def AS
        SELECT LOWER(TRIM(player)) nm, year yr, ANY_VALUE(NFL_player_id) nid
        FROM '{v26}' WHERE player IS NOT NULL AND NFL_player_id IS NOT NULL
          AND list_has_any(string_split(COALESCE(position, ''), ','), ['DL','LB','DB','DEF'])
        GROUP BY 1,2 HAVING COUNT(DISTINCT NFL_player_id)=1""")
    # ALL-PRO per (NFL_player_id, year): consensus 1st/2nd  (pfr_id primary, name+year fallback)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE ap AS
        SELECT nid AS NFL_player_id, year, MAX(ap1) ap1, MAX(ap2) ap2 FROM (
          SELECT COALESCE(m.NFL_player_id, nm.nid) AS nid, CAST(a.year AS INTEGER) AS year,
                 CASE WHEN a.team='1st Tm' THEN 1 ELSE 0 END ap1, CASE WHEN a.team='2nd Tm' THEN 1 ELSE 0 END ap2
          FROM read_parquet('{ALLPRO}') a
          LEFT JOIN pfr2nfl m ON a.pfr_id=m.pfr_id
          LEFT JOIN nm2n nm ON LOWER(TRIM(a.player))=nm.nm AND CAST(a.year AS INTEGER)=nm.yr
          WHERE a.year IS NOT NULL
        ) WHERE nid IS NOT NULL GROUP BY 1,2""")
    # PRO BOWL (player_link_ids=pfr_id primary, name+year fallback)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pb AS
        SELECT nid AS NFL_player_id, year, 1 pro_bowl FROM (
          SELECT COALESCE(m.NFL_player_id, nm.nid) AS nid, CAST(p.year AS INTEGER) AS year
          FROM read_parquet('{PROBOWL}') p
          LEFT JOIN pfr2nfl m ON p.player_link_ids=m.pfr_id
          LEFT JOIN nm2n nm ON LOWER(TRIM(p.player))=nm.nm AND CAST(p.year AS INTEGER)=nm.yr
          WHERE p.year IS NOT NULL
        ) WHERE nid IS NOT NULL GROUP BY 1,2""")
    # MVP winner per year = top vote share (pfr_id primary, name+year fallback)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE mvp AS
        WITH base AS (
          SELECT COALESCE(m.NFL_player_id, nm.nid) AS nid, CAST(x.year AS INTEGER) AS year,
                 TRY_CAST(x.share AS DOUBLE) sh, TRY_CAST(x.votes AS DOUBLE) vt
          FROM read_parquet('{MVP}') x
          LEFT JOIN pfr2nfl m ON x.player_link_ids=m.pfr_id
          LEFT JOIN nm2n nm ON LOWER(TRIM(x.player))=nm.nm AND CAST(x.year AS INTEGER)=nm.yr
          WHERE x.year IS NOT NULL),
        v AS (SELECT nid, year, ROW_NUMBER() OVER (PARTITION BY year ORDER BY sh DESC NULLS LAST, vt DESC NULLS LAST) rn
              FROM base WHERE nid IS NOT NULL)
        SELECT nid AS NFL_player_id, year, 1 mvp FROM v WHERE rn=1""")
    # other AP award winners (OPOY/DPOY/OROY/DROY/CPOY): winner per year, pfr_id + side-scoped name+year
    # fallback. side map: defensive awards -> nm2n_def, offensive -> nm2n_off, else plain unique nm2n.
    AWARD_SIDE = {"opoy": "nm2n_off", "oroy": "nm2n_off", "dpoy": "nm2n_def", "droy": "nm2n_def", "cpoy": "nm2n"}
    for key, tbl in VOTING.items():
        path = _VOTE_DIR.format(tbl)
        sidemap = AWARD_SIDE.get(key, "nm2n")
        try:
            con.execute(f"""CREATE OR REPLACE TEMP TABLE award_{key} AS
                WITH base AS (SELECT COALESCE(m.NFL_player_id, sm.nid, nm.nid) AS nid, CAST(x.year AS INTEGER) AS year,
                                TRY_CAST(x.share AS DOUBLE) sh, TRY_CAST(x.votes AS DOUBLE) vt
                              FROM read_parquet('{path}') x
                              LEFT JOIN pfr2nfl m ON x.player_link_ids=m.pfr_id
                              LEFT JOIN {sidemap} sm ON LOWER(TRIM(x.player))=sm.nm AND CAST(x.year AS INTEGER)=sm.yr
                              LEFT JOIN nm2n nm ON LOWER(TRIM(x.player))=nm.nm AND CAST(x.year AS INTEGER)=nm.yr
                              WHERE x.year IS NOT NULL),
                v AS (SELECT nid, year, ROW_NUMBER() OVER (PARTITION BY year ORDER BY sh DESC NULLS LAST, vt DESC NULLS LAST) rn
                      FROM base WHERE nid IS NOT NULL)
                SELECT NFL_player_id, year, 1 AS {key} FROM (SELECT nid AS NFL_player_id, year FROM v WHERE rn=1)""")
        except Exception:
            con.execute(
                f"CREATE OR REPLACE TEMP TABLE award_{key} AS SELECT NULL::VARCHAR NFL_player_id, NULL::INTEGER year, 1 AS {key} WHERE 1=0"
            )
    # overlay dual-eligibility candidates -> NFL_player_id
    import glob

    ov = sorted(glob.glob(OVERLAY_GLOB))
    if ov:
        con.execute(f"""CREATE OR REPLACE TEMP TABLE cand AS
            SELECT NFL_player_id, ANY_VALUE(canonical_position_candidates) position_candidates
            FROM '{Path(ov[-1]).as_posix()}'
            WHERE canonical_position_candidates LIKE '%,%' GROUP BY NFL_player_id""")
    else:
        con.execute(
            "CREATE OR REPLACE TEMP TABLE cand AS SELECT NULL::VARCHAR NFL_player_id, NULL::VARCHAR position_candidates WHERE 1=0"
        )


def _enrich_season(con, src, tmp):
    cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()}
    add = []
    if "birth_date" in {c[0] for c in con.execute("DESCRIBE bio").fetchall()}:
        # guard against bad bio DOBs (87 player-seasons had impossible/negative ages from wrong-century DOBs)
        add.append(
            "CASE WHEN b.birth_date IS NOT NULL AND (CAST(s.year AS INTEGER) - CAST(EXTRACT(year FROM CAST(b.birth_date AS TIMESTAMP)) AS INTEGER)) BETWEEN 16 AND 50 "
            "THEN CAST(s.year AS INTEGER) - CAST(EXTRACT(year FROM CAST(b.birth_date AS TIMESTAMP)) AS INTEGER) END AS age"
        )
    add += [
        "COALESCE(ap.ap1,0) AS all_pro_first_team",
        "COALESCE(ap.ap2,0) AS all_pro_second_team",
        "COALESCE(pb.pro_bowl,0) AS pro_bowl",
        "COALESCE(mvp.mvp,0) AS mvp",
    ]
    add += [f"COALESCE(aw_{k}.{k},0) AS {k}" for k in VOTING]
    # idempotent: drop any of our cols already present so a re-run replaces them
    _mine = ["age", "all_pro_first_team", "all_pro_second_team", "pro_bowl", "mvp"] + list(VOTING)
    _ex = [c for c in _mine if c in cols]
    star = f"s.* EXCLUDE ({', '.join(_ex)})" if _ex else "s.*"
    award_joins = "".join(
        f" LEFT JOIN award_{k} aw_{k} ON s.NFL_player_id=aw_{k}.NFL_player_id AND s.year=aw_{k}.year" for k in VOTING
    )
    sql = (
        f"SELECT {star}, {', '.join(add)} FROM '{src}' s "
        "LEFT JOIN bio b ON s.NFL_player_id=b.NFL_player_id "
        "LEFT JOIN ap ON s.NFL_player_id=ap.NFL_player_id AND s.year=ap.year "
        "LEFT JOIN pb ON s.NFL_player_id=pb.NFL_player_id AND s.year=pb.year "
        "LEFT JOIN mvp ON s.NFL_player_id=mvp.NFL_player_id AND s.year=mvp.year" + award_joins
    )
    r = con.execute(sql).fetch_record_batch(50000)
    w = pq.ParquetWriter(tmp, r.schema)
    for b in r:
        w.write_batch(b)
    w.close()


def _enrich_career(con, src, season_src, tmp):
    biocols = {c[0] for c in con.execute("DESCRIBE bio").fetchall()}
    static = [c for c in STATIC if c in biocols]
    ccols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{src}'").fetchall()}
    # per-season award sums from the (enriched) season table. (career_positions is owned by
    # build_position_eligibility -- not added here.)
    vote_sums = ", ".join(f"SUM(COALESCE({k},0)) career_{k}" for k in VOTING)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE cp AS
        SELECT NFL_player_id,
               SUM(COALESCE(all_pro_first_team,0)) career_all_pro_first,
               SUM(COALESCE(all_pro_second_team,0)) career_all_pro_second,
               SUM(COALESCE(pro_bowl,0)) career_pro_bowls,
               SUM(COALESCE(mvp,0)) career_mvps, {vote_sums}
        FROM '{season_src}' GROUP BY NFL_player_id""")
    static_sel = ", ".join(f"b.{c} AS {c}" for c in static)
    compat_static = []
    compat_static_cols = []
    if "years_active" in static:
        # Fly already carries this duplicate from an earlier enrichment pass. Keep it
        # as a compatibility alias until a deliberate schema cleanup removes it.
        compat_static.append("b.years_active AS years_active_1")
        compat_static_cols.append("years_active_1")
    # idempotent: drop any of our cols already present
    _mine = (
        static
        + compat_static_cols
        + ["position_candidates", "career_all_pro_first", "career_all_pro_second", "career_pro_bowls", "career_mvps"]
        + [f"career_{k}" for k in VOTING]
    )
    _ex = [c for c in _mine if c in ccols]
    star = f"c.* EXCLUDE ({', '.join(_ex)})" if _ex else "c.*"
    vote_sel = ", ".join(f"COALESCE(cp.career_{k},0) AS career_{k}" for k in VOTING)
    static_parts = [p for p in [static_sel, ", ".join(compat_static)] if p]
    sql = (
        f"SELECT {star}, {', '.join(static_parts)}, "
        "cand.position_candidates, "
        "COALESCE(cp.career_all_pro_first,0) AS career_all_pro_first, "
        "COALESCE(cp.career_all_pro_second,0) AS career_all_pro_second, "
        "COALESCE(cp.career_pro_bowls,0) AS career_pro_bowls, "
        f"COALESCE(cp.career_mvps,0) AS career_mvps, {vote_sel} "
        f"FROM '{src}' c "
        "LEFT JOIN bio b ON c.NFL_player_id=b.NFL_player_id "
        "LEFT JOIN cp ON c.NFL_player_id=cp.NFL_player_id "
        "LEFT JOIN cand ON c.NFL_player_id=cand.NFL_player_id"
    )
    r = con.execute(sql).fetch_record_batch(50000)
    w = pq.ParquetWriter(tmp, r.schema)
    for b in r:
        w.write_batch(b)
    w.close()


def run(apply=False):
    art = Path(latest_v26()).parent / "season_career_v26"
    con = duckdb.connect()
    con.execute("PRAGMA threads=3")
    con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    sp = art / ".enrichspill"
    sp.mkdir(exist_ok=True)
    con.execute(f"SET temp_directory='{sp.as_posix()}'")
    _setup(con)
    if not apply:
        n_ap = con.execute("SELECT COUNT(*) FROM ap").fetchone()[0]
        n_pb = con.execute("SELECT COUNT(*) FROM pb").fetchone()[0]
        n_mvp = con.execute("SELECT COUNT(*) FROM mvp").fetchone()[0]
        con.close()
        return {"allpro_player_years": n_ap, "probowl_player_years": n_pb, "mvp_years": n_mvp}

    season_tabs = ["player_nfl_season", "player_nfl_season_all"]
    career_tabs = ["player_nfl_career", "player_nfl_career_all"]
    results = {}
    tmps = {}
    for t in season_tabs:
        src = (art / f"{t}.parquet").as_posix()
        tmp = (art / f"{t}_enr.parquet").as_posix()
        before = con.execute(f"SELECT COUNT(*) FROM '{src}'").fetchone()[0]
        _enrich_season(con, src, tmp)
        after = con.execute(f"SELECT COUNT(*) FROM '{tmp}'").fetchone()[0]
        results[t] = (before, after)
        tmps[t] = (src, tmp)
    # careers read the ENRICHED season (for award sums + positions)
    for t, ssrc in zip(career_tabs, ["player_nfl_season", "player_nfl_season_all"]):
        src = (art / f"{t}.parquet").as_posix()
        tmp = (art / f"{t}_enr.parquet").as_posix()
        season_enriched = tmps[ssrc][1]
        before = con.execute(f"SELECT COUNT(*) FROM '{src}'").fetchone()[0]
        _enrich_career(con, src, season_enriched, tmp)
        after = con.execute(f"SELECT COUNT(*) FROM '{tmp}'").fetchone()[0]
        results[t] = (before, after)
        tmps[t] = (src, tmp)
    # sanity anchors
    se = tmps["player_nfl_season"][1]
    ce = tmps["player_nfl_career"][1]
    rice_ap = con.execute(f"SELECT career_all_pro_first FROM '{ce}' WHERE player='Jerry Rice'").fetchone()
    manning_age = con.execute(f"SELECT age FROM '{se}' WHERE player='Peyton Manning' AND year=2013").fetchone()
    career_cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{ce}'").fetchall()}
    pos_anchor_col = "career_positions" if "career_positions" in career_cols else "position"
    rice_pos = con.execute(f"SELECT {pos_anchor_col} FROM '{ce}' WHERE player='Jerry Rice'").fetchone()
    con.close()
    rows_ok = all(b == a for b, a in results.values())
    anchors_ok = manning_age and manning_age[0] == 37 and rice_ap and rice_ap[0] >= 10
    gate = rows_ok and anchors_ok
    res = {
        "rows": {t: f"{b}->{a}" for t, (b, a) in results.items()},
        "rows_ok": rows_ok,
        "rice_all_pro_first": rice_ap[0] if rice_ap else None,
        "manning_2013_age": manning_age[0] if manning_age else None,
        "rice_positions": rice_pos[0] if rice_pos else None,
        "gate_pass": bool(gate),
    }
    if gate:
        stamp = utc_stamp()
        for t, (src, tmp) in tmps.items():
            bk = Path(src).with_name(Path(src).stem + f"_preenrich_{stamp}.parquet")
            shutil.copy2(src, bk)
            os.replace(tmp, src)
        res["swapped"] = True
    else:
        res["swapped"] = False
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if not a.apply:
        print("DRY:", run())
    else:
        r = run(apply=True)
        print(f"rows: {r['rows']}")
        print(
            f"anchors: Rice all_pro_first={r['rice_all_pro_first']}, Manning 2013 age={r['manning_2013_age']}, Rice positions={r['rice_positions']}"
        )
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> " + ("SWAPPED" if r["swapped"] else "NOT swapped"))
