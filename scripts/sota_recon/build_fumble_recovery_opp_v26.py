"""
sota_recon/build_fumble_recovery_opp_v26.py  --  O.7: the fumble_recovery_opp column
(Joe's 2026-07-26 directive: fum_rec must split takeaways vs own-team recoveries)

v26 semantics (measured 2026-07-26): IDP-plane fum_rec = own + opponent recoveries
combined; team-DEF-plane fum_rec = opponent-only takeaways; fumble_recovery_own
exists 1978+; the OPP COUNT column is missing -- so `turnovers-by-recovery` cannot
be separated from own-ball scrambles anywhere downstream. This builder adds
`fumble_recovery_opp` (IDP rows, 1978-2025):

  1999-2025  structured pbp attribution: fumble_recovery_{1,2}_player_id (gsis =
             NFL_player_id) where the recovery team differs from the slot's
             fumbling team; joined on (player, year, week, season_type)
  1978-1998  the regex lane of build_misc_pbp_pre1998 (the method that built
             fumble_recovery_own there) with the OPPOSITE team condition:
             'recovered by Y' where Y's team differs from the fumbler's team
  zero fill  rows already carrying fumble_recovery_own IS NOT NULL get opp=0 when
             no event matched, so the identity fum_rec = own + opp is testable on
             the same population own vouches for

Team-DEF rows stay NULL ON PURPOSE: populating them needs the pbp-team ->
v26-team join, which is the O.4-queued team_fid-scoped join fix (505 era-ambiguous
codes) -- copying fum_rec would smuggle in the unproven "team fum_rec is exactly
opp-only" claim. The R4 team_idp_vertical lane reports the plane gap honestly.

Gated: rows unchanged, column previously absent-or-null, golden samples pass,
identity agreement rate reported per era. Backup + swap like every v26 builder.

    python -m scripts.sota_recon.build_fumble_recovery_opp_v26 [--apply]
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import PBP_MERGED, latest_v26

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
COL = "fumble_recovery_opp"
PROV = "o7.fumble_recovery_opp"
PROV_COL = "recon_correction_log"
LO, HI = 1978, 2025
NN = "lower(regexp_replace({},'[^A-Za-z]','','g'))"
NAME = r"([A-Z][A-Za-z.''\-]+(?: [A-Z][A-Za-z.''\-]+){0,2})"


def _pbp_path() -> str:
    return Path(getattr(PBP_MERGED, "path", PBP_MERGED)).as_posix()


def _targets(con) -> None:
    pbp = _pbp_path()
    # ---- 1999+: structured slot-paired attribution, gsis ids ----
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE opp99 AS
        SELECT nid, yr, wk, st, COUNT(*) v FROM (
          SELECT fumble_recovery_1_player_id nid, CAST(season AS INT) yr,
                 CAST(week AS INT) wk, season_type st
          FROM '{pbp}'
          WHERE CAST(season AS INT) >= 1999
            AND fumble_recovery_1_player_id IS NOT NULL
            AND fumble_recovery_1_team IS NOT NULL AND fumbled_1_team IS NOT NULL
            AND fumble_recovery_1_team <> fumbled_1_team
          UNION ALL
          SELECT fumble_recovery_2_player_id, CAST(season AS INT),
                 CAST(week AS INT), season_type
          FROM '{pbp}'
          WHERE CAST(season AS INT) >= 1999
            AND fumble_recovery_2_player_id IS NOT NULL
            AND fumble_recovery_2_team IS NOT NULL
            AND COALESCE(fumbled_2_team, fumbled_1_team) IS NOT NULL
            AND fumble_recovery_2_team <> COALESCE(fumbled_2_team, fumbled_1_team)
        ) GROUP BY 1, 2, 3, 4""")
    # ---- 1978-98: regex lane (precedent: build_misc_pbp_pre1998 own-recovery),
    #      opposite condition: recoverer's team differs from fumbler's team ----
    P = f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS
        SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk
        FROM read_parquet('{TG}') WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS
        SELECT pfr_id, ANY_VALUE(NFL_player_id) nid FROM read_parquet('{BIO}')
        WHERE pfr_id IS NOT NULL GROUP BY 1""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE idset AS
        SELECT DISTINCT NFL_player_id nid FROM read_parquet('{BIO}')
        WHERE NFL_player_id IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rnm AS
        SELECT boxscore_id, nn, ANY_VALUE(team) team FROM (
          SELECT boxscore_id, {NN.format('player')} nn, team
          FROM read_parquet('{BOX}/player_offense/_combined.parquet') WHERE player IS NOT NULL
          UNION ALL
          SELECT boxscore_id, {NN.format('player')} nn, team
          FROM read_parquet('{BOX}/player_defense/_combined.parquet') WHERE player IS NOT NULL
        ) GROUP BY 1, 2""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rpfr AS
        SELECT boxscore_id, nn, ANY_VALUE(pid) pid FROM (
          SELECT boxscore_id, {NN.format('player')} nn, split_part(player_link_ids,';',1) pid
          FROM read_parquet('{BOX}/player_offense/_combined.parquet')
          WHERE player IS NOT NULL AND player_link_ids IS NOT NULL
          UNION ALL
          SELECT boxscore_id, {NN.format('player')} nn, split_part(player_link_ids,';',1) pid
          FROM read_parquet('{BOX}/player_defense/_combined.parquet')
          WHERE player IS NOT NULL AND player_link_ids IS NOT NULL
        ) GROUP BY 1, 2 HAVING COUNT(DISTINCT pid) = 1""")
    NID = "COALESCE(b.nid, ds.nid)"
    MAP = ("LEFT JOIN rpfr rp ON rp.boxscore_id=s.boxscore_id AND rp.nn=s.who "
           "LEFT JOIN bio b ON b.pfr_id=rp.pid LEFT JOIN idset ds ON ds.nid=rp.pid")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE opp78 AS
        SELECT {NID} nid, s.yr, s.wk, COUNT(*) v FROM (
          SELECT t.yr, t.wk, p.boxscore_id,
                 {NN.format("regexp_extract(p.detail,'" + NAME + " fumbles',1)")} fnn,
                 {NN.format("regexp_extract(p.detail,'recovered by " + NAME + "',1)")} who
          FROM {P} p JOIN tg t USING(boxscore_id)
          WHERE CAST(p.season AS INT) BETWEEN 1978 AND 1998
            AND lower(p.detail) LIKE '%fumble%' AND lower(p.detail) LIKE '%recovered by%') s
        LEFT JOIN rnm rf ON rf.boxscore_id=s.boxscore_id AND rf.nn=s.fnn
        LEFT JOIN rnm rr ON rr.boxscore_id=s.boxscore_id AND rr.nn=s.who
        {MAP}
        WHERE {NID} IS NOT NULL AND s.fnn <> s.who
          AND rf.team IS NOT NULL AND rr.team IS NOT NULL AND rf.team <> rr.team
        GROUP BY 1, 2, 3""")


def _identity_report(con, table: str) -> dict:
    """fum_rec = fumble_recovery_own + fumble_recovery_opp agreement, per era."""
    rows = con.execute(f"""
        SELECT CASE WHEN CAST(year AS INT) < 1999 THEN '1978_98' ELSE '1999_2025' END era,
               COUNT(*),
               COUNT(*) FILTER (WHERE ABS(TRY_CAST(fum_rec AS DOUBLE)
                 - (TRY_CAST(fumble_recovery_own AS DOUBLE)
                    + TRY_CAST({COL} AS DOUBLE))) <= 1e-6)
        FROM {table}
        WHERE position IS DISTINCT FROM 'DEF' AND CAST(year AS INT) BETWEEN {LO} AND {HI}
          AND fum_rec IS NOT NULL AND fumble_recovery_own IS NOT NULL
          AND {COL} IS NOT NULL
        GROUP BY 1""").fetchall()
    return {era: {"checked": n, "exact": ok, "rate": round(ok / n, 4) if n else None}
            for era, n, ok in rows}


# era-scoped franchise aliases for the pbp<->v26 team-code join (O.7): a static
# canon map cannot express codes whose franchise changes by era (the receipted
# kc_planes pbp_team_defense 505-key class). 1978+ is doubleheader-free, so a
# year-scoped alias pair is provably unambiguous.
ERA_ALIASES = [
    # (code_a, code_b, year_min, year_max) -- treat as the same franchise inside the window
    ("OTI", "HOU", 1978, 1996),   # Houston Oilers
    ("CRD", "STL", 1978, 1987),   # St. Louis (football) Cardinals
    ("CRD", "PHO", 1988, 1993),   # Phoenix Cardinals
    ("CLT", "BAL", 1978, 1983),   # Baltimore Colts
]


def def_rows_report() -> dict:
    """O.7 DRY-RUN: DEF-row fumble_recovery_opp via the team_fid-class join.

    Verifies the queued claim 'team-DEF fum_rec is exactly opp-only' against the
    pbp opp-recovery counts per team-week (structured slot-paired attribution --
    the method certified by the IDP-lane gates). Receipts per era; APPLY IS
    QUEUED FOR JOE (modern era verifies; 1978-98 measures a DIFFERENT population
    and needs an adjudicated accuracy class first)."""
    from .recon_common import canon_team_sql
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    pbp = _pbp_path()
    v26 = Path(latest_v26()).as_posix()
    alias = " ".join(
        f"WHEN team_c = '{a}' AND yr BETWEEN {lo} AND {hi} THEN '{b}' "
        f"WHEN team_c = '{b}' AND yr BETWEEN {lo} AND {hi} THEN '{b}' "
        for a, b, lo, hi in ERA_ALIASES)
    con.execute(f"""
        CREATE TEMP TABLE opp AS
        SELECT (CASE {alias} ELSE team_c END) team_c, yr, wk, stp, SUM(v) opp_ct
        FROM (
          SELECT {canon_team_sql('rteam')} team_c, yr, wk, stp, COUNT(*) v FROM (
            SELECT fumble_recovery_1_team rteam, CAST(season AS INT) yr,
                   CAST(week AS INT) wk, season_type stp
            FROM '{pbp}'
            WHERE fumble_recovery_1_player_id IS NOT NULL
              AND fumble_recovery_1_team IS NOT NULL AND fumbled_1_team IS NOT NULL
              AND fumble_recovery_1_team <> fumbled_1_team
            UNION ALL
            SELECT fumble_recovery_2_team, CAST(season AS INT), CAST(week AS INT),
                   season_type
            FROM '{pbp}'
            WHERE fumble_recovery_2_player_id IS NOT NULL
              AND fumble_recovery_2_team IS NOT NULL
              AND COALESCE(fumbled_2_team, fumbled_1_team) IS NOT NULL
              AND fumble_recovery_2_team <> COALESCE(fumbled_2_team, fumbled_1_team)
          ) GROUP BY 1, 2, 3, 4
        ) GROUP BY 1, 2, 3, 4""")
    con.execute(f"""
        CREATE TEMP TABLE defrows AS
        SELECT (CASE {alias} ELSE team_c END) team_c, yr, wk, stp,
               fum_rec, {COL} AS opp_col, player_week
        FROM (
          SELECT {canon_team_sql('nfl_team')} team_c, CAST(year AS INT) yr,
                 CAST(week AS INT) wk, season_type stp, fum_rec, {COL}, player_week
          FROM '{v26}' WHERE position = 'DEF' AND CAST(year AS INT) >= 1978)""")
    eras = con.execute("""
        SELECT CASE WHEN o.yr < 1999 THEN '1978_98' ELSE '1999_2025' END era,
               COUNT(*) pbp_team_weeks, COUNT(d.team_c) bound,
               COUNT(*) FILTER (WHERE d.fum_rec IS NOT NULL
                                 AND ABS(d.fum_rec - o.opp_ct) <= 1e-6) agree,
               COUNT(*) FILTER (WHERE d.fum_rec IS NOT NULL) fum_rec_nonnull
        FROM opp o LEFT JOIN defrows d USING (team_c, yr, wk, stp)
        GROUP BY 1 ORDER BY 1""").fetchall()
    unbound = con.execute("""
        SELECT o.team_c, MIN(o.yr), MAX(o.yr), COUNT(*)
        FROM opp o LEFT JOIN defrows d USING (team_c, yr, wk, stp)
        WHERE d.team_c IS NULL GROUP BY 1 ORDER BY 4 DESC LIMIT 10""").fetchall()
    disagree = con.execute("""
        SELECT o.yr, COUNT(*) FROM opp o JOIN defrows d USING (team_c, yr, wk, stp)
        WHERE o.yr >= 1999 AND d.fum_rec IS NOT NULL
          AND ABS(d.fum_rec - o.opp_ct) > 1e-6
        GROUP BY 1 ORDER BY 2 DESC LIMIT 8""").fetchall()
    con.close()
    rep = {era: {"pbp_team_weeks": n, "bound": b, "agree": a,
                 "fum_rec_nonnull": f,
                 "agree_rate": round(a / f, 4) if f else None}
           for era, n, b, a, f in eras}
    return {
        "mode": "DRY-RUN (apply queued for Joe: modern claim verified; 1978-98 "
                "team fum_rec measures a different population -> needs an "
                "adjudicated accuracy class before any write)",
        "claim": "team-DEF-plane fum_rec is exactly opponent-only takeaways",
        "per_era": rep,
        "unbound_pbp_team_weeks": [
            {"team": t, "years": f"{y0}-{y1}", "n": n} for t, y0, y1, n in unbound],
        "modern_disagreement_years_top": [(int(y), int(n)) for y, n in disagree],
        "apply_plan_modern_only": "DEF rows 1999-2025: opp = pbp count where it "
            "equals fum_rec; 0 where fum_rec IS NOT NULL and no pbp event and "
            "fum_rec = 0; disagreements stay NULL + queue",
    }


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    if not apply:
        con = duckdb.connect()
        con.execute("PRAGMA threads=4")
        con.execute("SET memory_limit='6GB'")
        _targets(con)
        t99, n99 = con.execute("SELECT SUM(v), COUNT(*) FROM opp99").fetchone()
        t78, n78 = con.execute("SELECT SUM(v), COUNT(*) FROM opp78").fetchone()
        return {"events_1999_2025": int(t99 or 0), "player_weeks_1999_2025": n99,
                "events_1978_98": int(t78 or 0), "player_weeks_1978_98": n78}

    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    coltypes = {c[0]: c[1] for c in con.execute("DESCRIBE st").fetchall()}
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    if COL in coltypes:
        pre_nonnull = con.execute(f"SELECT COUNT({COL}) FROM st").fetchone()[0]
    else:
        con.execute(f"ALTER TABLE st ADD COLUMN {COL} DOUBLE")
        pre_nonnull = 0
    if PROV_COL not in coltypes:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    _targets(con)
    con.execute("""CREATE TEMP TABLE multi AS SELECT NFL_player_id, year, week
        FROM st WHERE position IS DISTINCT FROM 'DEF' GROUP BY 1, 2, 3 HAVING COUNT(*) > 1""")
    prov = (f"{PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' "
            f"ELSE {PROV_COL}||',{PROV}' END")
    # 1999+ events (season_type-keyed)
    con.execute(f"""UPDATE st SET {COL}=t.v, {prov}
        FROM opp99 t WHERE st.NFL_player_id=t.nid AND CAST(st.year AS INT)=t.yr
          AND CAST(st.week AS INT)=t.wk AND st.season_type=t.st
          AND st.position IS DISTINCT FROM 'DEF'
          AND NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id
                          AND m.year=st.year AND m.week=st.week)""")
    # 1978-98 events (single-row player-weeks, precedent join shape)
    con.execute(f"""UPDATE st SET {COL}=t.v, {prov}
        FROM opp78 t WHERE st.NFL_player_id=t.nid AND CAST(st.year AS INT)=t.yr
          AND CAST(st.week AS INT)=t.wk AND st.position IS DISTINCT FROM 'DEF'
          AND CAST(st.year AS INT) BETWEEN 1978 AND 1998
          AND NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id
                          AND m.year=st.year AND m.week=st.week)""")
    # dense-zero on the population fumble_recovery_own vouches for
    con.execute(f"""UPDATE st SET {COL}=0, {prov}
        WHERE {COL} IS NULL AND fumble_recovery_own IS NOT NULL
          AND position IS DISTINCT FROM 'DEF'
          AND CAST(year AS INT) BETWEEN {LO} AND {HI}""")
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(
        list_distinct(string_split({PROV_COL},',')),',') WHERE {PROV_COL} LIKE '%,%'""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    populated = con.execute(f"SELECT COUNT({COL}), SUM({COL}) FROM st").fetchone()
    identity = _identity_report(con, "st")

    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_fropp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close()
    con.close()
    shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    modern_ok = (identity.get("1999_2025", {}).get("rate") or 0) >= 0.95
    gate = (g["failed"] == 0) and (after == before) and (pre_nonnull == 0) \
        and (populated[0] or 0) > 0 and modern_ok
    res = {"before": before, "after": after, "pre_nonnull": pre_nonnull,
           "populated_rows": populated[0], "event_total": populated[1],
           "identity_fum_rec_eq_own_plus_opp": identity,
           "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prefropp_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--def-rows", action="store_true",
                    help="O.7 DEF-row lane report (dry-run only; apply is queued "
                         "for Joe's sign-off)")
    a = ap.parse_args()
    import json
    if a.def_rows:
        if a.apply:
            raise SystemExit("REFUSED: DEF-row apply is queued for Joe's sign-off "
                             "(1978-98 needs an adjudicated accuracy class).")
        print(json.dumps(def_rows_report(), indent=2, default=str))
        raise SystemExit(0)
    r = run(apply=a.apply)
    print(json.dumps(r, indent=2, default=str))
