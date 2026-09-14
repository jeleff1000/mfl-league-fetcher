"""
sota_recon/build_pbp_sacks_1978_1981.py  --  attribute 1978-1981 IDP sacks from pbp

The 4-witness consensus (opp pass_sacked, team_stats Sacked-Yards, pbp, player_defense) agrees on
team sacks 1978+, but v26 def_sacks is ~15% off in 1978-2001 -- the whole gap is 1978-1981, where
player_defense carries almost NO individual sacks (~40/yr) because individual sacks only became an
official stat in 1982 (663+/yr from then). The team total is known but unattributed to players.

pbp (1966+) names the sacker on every sack play, so it supplies the missing IDP attribution.
Credit rule (NFL half-sacks): "QB sacked by SACKER for X" -> SACKER 1.0; "QB sacked by A and B
for X" -> A and B 0.5 each. Link order is [QB, sacker1, (sacker2), ...] so sacker1 = 2nd detail
link, sacker2 = 3rd (only when the 'sacked by ... for' clause contains ' and '). Accepted-penalty
plays (lower(detail) LIKE '%no play%') are excluded -- they are not official sacks.

Scope 1978-1981 ONLY (1982+ player_defense IDP sacks are official and already reconciled). Sets
def_sacks on the sacker's IDP player-week (overwrite, since player_defense was empty); inserts a
DB/DL row when the pass-rusher has no v26 row that game (no other tracked stat in that era).

Gated: golden 24/24, 1978-1981 def_sacks team-game agreement vs opp pass_sacked rises toward
~99%, no def_sacks ceiling weirdness -> backup + swap.

    python -m scripts.sota_recon.build_pbp_sacks_1978_1981            # dry-run
    python -m scripts.sota_recon.build_pbp_sacks_1978_1981 --apply
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

BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables"
TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PROV = "wave23.pbp_idp_sacks_1978_1981"
PROV_COL = "recon_correction_log"
LO, HI = 1978, 1981


def _build(con):
    PBP = f"read_parquet('{BOX}/pbp/_combined.parquet')"
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tg AS SELECT boxscore_id, team_code,
        CAST(year AS INT) yr, CAST(week AS INT) wk, team_fid fr FROM read_parquet('{TG}')
        WHERE team_fid IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE bio AS
        SELECT pfr_id, ANY_VALUE(NFL_player_id) nid, ANY_VALUE(nfl_position) pos
        FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL GROUP BY 1""")
    # per-boxscore roster (any boxscore table) -> franchise, for inserts/team attribution
    parts = []
    for t in ["player_offense", "player_defense", "kicking", "returns"]:
        parts.append(f"SELECT s.boxscore_id, split_part(s.player_link_ids,';',1) pid, g.fr "
                     f"FROM read_parquet('{BOX}/{t}/_combined.parquet') s JOIN tg g "
                     f"ON s.boxscore_id=g.boxscore_id AND s.team=g.team_code WHERE s.player_link_ids IS NOT NULL")
    con.execute("CREATE OR REPLACE TEMP TABLE pid_team AS SELECT DISTINCT boxscore_id, pid, fr FROM ("
                + " UNION ALL ".join(parts) + ")")
    con.execute("CREATE OR REPLACE TEMP TABLE bx AS SELECT DISTINCT boxscore_id, yr, wk FROM tg")
    # one row per sack play with sacker links + co-sack flag
    con.execute(f"""CREATE OR REPLACE TEMP TABLE sk AS
        SELECT boxscore_id,
               split_part(detail_link_ids,';',2) s1,
               split_part(detail_link_ids,';',3) s2,
               regexp_extract(detail,'sacked by (.*?) for',1) LIKE '% and %' is_co
        FROM {PBP}
        WHERE detail LIKE '%sacked by%' AND lower(detail) NOT LIKE '%no play%'
          AND detail_link_ids IS NOT NULL AND season BETWEEN {LO} AND {HI}""")
    # explode to (boxscore, sacker, credit): single -> s1 1.0 ; co -> s1 0.5 + s2 0.5
    con.execute("""CREATE OR REPLACE TEMP TABLE credit AS
        SELECT boxscore_id, s1 sacker, CASE WHEN is_co THEN 0.5 ELSE 1.0 END cr FROM sk WHERE s1<>''
        UNION ALL
        SELECT boxscore_id, s2 sacker, 0.5 cr FROM sk WHERE is_co AND s2<>''""")
    # -> (NFL_player_id, yr, wk, franchise, position, sack credit)
    con.execute("""CREATE OR REPLACE TEMP TABLE tgt AS
        SELECT b.nid AS NFL_player_id, x.yr, x.wk, pt.fr,
               COALESCE(b.pos,'DL') AS "position", SUM(c.cr) def_sacks
        FROM credit c JOIN bx x ON c.boxscore_id=x.boxscore_id
             JOIN bio b ON b.pfr_id=c.sacker
             LEFT JOIN pid_team pt ON pt.boxscore_id=c.boxscore_id AND pt.pid=c.sacker
        WHERE b.nid IS NOT NULL GROUP BY 1,2,3,4,5""")
    return con.execute("SELECT COUNT(*), COALESCE(SUM(def_sacks),0) FROM tgt").fetchone()


def _agree(con, tbl):
    """1978-81 team-game def_sacks: v26 IDP-sum vs opp pass_sacked (independent witness)."""
    con.execute(f"""CREATE OR REPLACE TEMP TABLE wopp AS
        SELECT o.yr, o.wk, o.team_fid_opp fr, SUM(TRY_CAST(s.pass_sacked AS DOUBLE)) v
        FROM read_parquet('{BOX}/player_offense/_combined.parquet') s
        JOIN (SELECT boxscore_id, team_code, CAST(year AS INT) yr, CAST(week AS INT) wk,
                     opponent_fid team_fid_opp FROM read_parquet('{TG}') WHERE opponent_fid IS NOT NULL) o
          ON s.boxscore_id=o.boxscore_id AND s.team=o.team_code
        WHERE o.yr BETWEEN {LO} AND {HI} GROUP BY 1,2,3""")
    r = con.execute(f"""
        WITH v AS (SELECT CAST(year AS INT) yr, CAST(week AS INT) wk, nfl_franchise_number fr,
              SUM(COALESCE(def_sacks,0)) vv FROM {tbl}
            WHERE position<>'DEF' AND year BETWEEN {LO} AND {HI} GROUP BY 1,2,3)
        SELECT ROUND(100.0*SUM(CASE WHEN abs(COALESCE(v.vv,0)-wopp.v)<0.6 THEN 1 ELSE 0 END)/COUNT(*),1)
        FROM wopp LEFT JOIN v USING(yr,wk,fr) WHERE wopp.v>0""").fetchone()
    return r[0]


def apply_build():
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='6GB'")
    con.execute(f"SET temp_directory='{sp}'")
    con.execute(f"CREATE TABLE st AS SELECT * FROM '{v26}'")
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if PROV_COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {PROV_COL} VARCHAR")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    _build(con)
    pre = _agree(con, "st")
    con.execute(f"""CREATE TEMP TABLE multi AS SELECT NFL_player_id,year,week FROM st
        WHERE year BETWEEN {LO} AND {HI} AND position<>'DEF' GROUP BY 1,2,3 HAVING COUNT(*)>1""")
    nm = ("NOT EXISTS (SELECT 1 FROM multi m WHERE m.NFL_player_id=st.NFL_player_id "
          "AND m.year=st.year AND m.week=st.week)")
    updated = con.execute(f"""SELECT COUNT(*) FROM st JOIN tgt t
        ON st.NFL_player_id=t.NFL_player_id AND CAST(st.year AS INT)=t.yr AND CAST(st.week AS INT)=t.wk
        WHERE st.position<>'DEF' AND {nm}""").fetchone()[0]
    con.execute(f"""UPDATE st SET def_sacks=t.def_sacks,
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM tgt t WHERE st.NFL_player_id=t.NFL_player_id AND CAST(st.year AS INT)=t.yr
          AND CAST(st.week AS INT)=t.wk AND st.position<>'DEF' AND {nm}""")
    # insert sacker-games with no v26 row (pure pass-rushers, no other tracked stat in this era)
    inserted = con.execute(f"""SELECT COUNT(*) FROM tgt t WHERE t.fr IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM st s2 WHERE s2.NFL_player_id=t.NFL_player_id AND CAST(s2.year AS INT)=t.yr
          AND CAST(s2.week AS INT)=t.wk)""").fetchone()[0]
    con.execute(f"""INSERT INTO st BY NAME
        SELECT t.NFL_player_id, b.player, CAST(t.yr AS DOUBLE) "year", CAST(t.wk AS DOUBLE) "week",
          tgm.season_type, t.position, tgm.team_code AS nfl_team, t.fr AS nfl_franchise_number,
          t.def_sacks, t.NFL_player_id||'_'||CAST(t.yr AS VARCHAR)||'_'||CAST(t.wk AS VARCHAR) AS player_week,
          'pbp_idp_sacks_1978_1981' AS data_source, '{PROV}' AS {PROV_COL}
        FROM tgt t
          LEFT JOIN (SELECT NFL_player_id nid, ANY_VALUE(player) player FROM read_parquet('{BIO}') GROUP BY 1) b
            ON b.nid=t.NFL_player_id
          LEFT JOIN (SELECT DISTINCT yr,wk,fr,team_code,season_type FROM
                     (SELECT CAST(year AS INT) yr,CAST(week AS INT) wk,team_fid fr,team_code,season_type
                      FROM read_parquet('{TG}'))) tgm ON tgm.yr=t.yr AND tgm.wk=t.wk AND tgm.fr=t.fr
        WHERE t.fr IS NOT NULL AND NOT EXISTS (SELECT 1 FROM st s2
          WHERE s2.NFL_player_id=t.NFL_player_id AND CAST(s2.year AS INT)=t.yr AND CAST(s2.week AS INT)=t.wk)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
        WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")
    post = _agree(con, "st")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_sktmp.parquet")
    r = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0) and (post >= pre + 5) and (updated + inserted > 0)
    res = {"updated": int(updated), "inserted": int(inserted), "before": int(before), "after": int(after),
           "pre_agree": pre, "post_agree": post, "golden": f"{g['passed']}/{g['total']}",
           "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_presk_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser(); ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        r = apply_build()
        print(f"updated={r['updated']} inserted={r['inserted']} rows {r['before']:,}->{r['after']:,} | "
              f"1978-81 def_sacks agree {r['pre_agree']}->{r['post_agree']}% | golden {r['golden']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='5GB'")
        con.execute(f"CREATE TABLE st AS SELECT * FROM '{v}'")
        ng, nsk = _build(con)
        print(f"pbp IDP sack credits 1978-81: {ng} player-weeks, {nsk:.1f} total sacks")
        print(f"current 1978-81 def_sacks team-game agree vs opp pass_sacked: {_agree(con,'st')}%")
