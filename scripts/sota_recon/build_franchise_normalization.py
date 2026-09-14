"""
sota_recon/build_franchise_normalization.py  --  consistent team identity in every column

The franchise NUMBER correctly tracks each NFL team's lineage across relocations (fr14 =
Rams across LA/STL/LAR, fr31 = Raiders OAK/LV, ...). But the ABBREVIATION columns are
inconsistent: the same franchise has 2 abbrevs within a season (SFO/SF, OAK/LV), the
team-view and opponent-view disagree (GNB vs GB for the same franchise-year), and the
DEF team id uses two schemes (DEF-7 vs DEF-PHI).

This normalizes every abbrev to ONE canonical per (year, franchise), anchored on the
franchise number, using nfl_team_games_all as the authority ((year, team_fid)->team_code,
clean except 2 ties resolved by MODE). Cross-season variation (a team's abbrev changing
year to year) is preserved -- only WITHIN-season / cross-column inconsistency is fixed.

  - nfl_team           := canonical[(year, nfl_franchise_number)]
  - opponent_nfl_team  := canonical[(year, opponent_nfl_franchise_number)]
  - DEF-<abbrev> id    := DEF-<franchise_number>  (uniform scheme; player_week rebuilt)

Gated: within-season/team-vs-opp abbrev inconsistencies -> 0, DEF id scheme uniform,
golden holds, row count unchanged -> backup + swap.

    python -m scripts.sota_recon.build_franchise_normalization          # audit
    python -m scripts.sota_recon.build_franchise_normalization --apply  # gated normalize
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

TG = "D:/league-history-data/nfl/raw/pfr/boxscores/nfl_team_games_all.parquet"
PROV = "wave19.franchise_abbrev_normalization"
PROV_COL = "recon_correction_log"


def _audit(con, V):
    c1 = con.execute(f"""SELECT COUNT(*) FROM (SELECT year,nfl_franchise_number FROM {V}
        WHERE nfl_franchise_number IS NOT NULL AND nfl_team IS NOT NULL
        GROUP BY 1,2 HAVING COUNT(DISTINCT nfl_team)>1)""").fetchone()[0]
    c2 = con.execute(f"""SELECT COUNT(*) FROM (SELECT year,opponent_nfl_franchise_number FROM {V}
        WHERE opponent_nfl_franchise_number IS NOT NULL AND opponent_nfl_team IS NOT NULL
        GROUP BY 1,2 HAVING COUNT(DISTINCT opponent_nfl_team)>1)""").fetchone()[0]
    c3 = con.execute(f"""WITH t AS (SELECT DISTINCT year,nfl_franchise_number fr,nfl_team ab FROM {V}
            WHERE nfl_franchise_number IS NOT NULL AND nfl_team IS NOT NULL),
          o AS (SELECT DISTINCT year,opponent_nfl_franchise_number fr,opponent_nfl_team ab FROM {V}
            WHERE opponent_nfl_franchise_number IS NOT NULL AND opponent_nfl_team IS NOT NULL)
        SELECT COUNT(*) FROM t JOIN o ON t.year=o.year AND t.fr=o.fr WHERE t.ab<>o.ab""").fetchone()[0]
    defab = con.execute(f"""SELECT COUNT(DISTINCT NFL_player_id) FROM {V}
        WHERE NFL_player_id LIKE 'DEF-%' AND NOT regexp_matches(NFL_player_id,'^DEF-[0-9]+$')""").fetchone()[0]
    # Every DST row's id MUST equal DEF-{franchise_number} (Jets/Titans 1960-62 were
    # mis-keyed DEF-138 though their franchise is 20; the old check missed numeric ids).
    def_id_ne_fn = con.execute(f"""SELECT COUNT(*) FROM {V}
        WHERE position='DEF' AND nfl_franchise_number IS NOT NULL
          AND NFL_player_id <> 'DEF-'||CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR)""").fetchone()[0]
    # A franchise must have exactly one DST id, and one franchise per (year, team code).
    fn_multi_id = con.execute(f"""SELECT COUNT(*) FROM (
        SELECT nfl_franchise_number FROM {V} WHERE position='DEF' AND nfl_franchise_number IS NOT NULL
        GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id)>1)""").fetchone()[0]
    return {"within_team": c1, "within_opp": c2, "team_vs_opp": c3, "def_abbrev_ids": defab,
            "def_id_ne_fn": def_id_ne_fn, "fn_multi_id": fn_multi_id}


def apply_norm() -> dict:
    v26 = latest_v26(); stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}"); os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='5GB'")
    con.execute(f"SET temp_directory='{sp}'")
    # canonical (year, franchise) -> abbrev from team_games (authority)
    con.execute(f"""CREATE TABLE canon AS
        SELECT year, team_fid fr, MODE(team_code) code FROM read_parquet('{TG}')
        WHERE team_fid IS NOT NULL AND team_code IS NOT NULL GROUP BY 1,2""")
    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{v26}')")
    before = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    pre = _audit(con, "st")

    # 1) nfl_team := canonical[(year, franchise)]
    con.execute(f"""UPDATE st SET nfl_team=c.code,
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        FROM canon c WHERE st.year=c.year AND st.nfl_franchise_number=c.fr AND st.nfl_team IS DISTINCT FROM c.code""")
    # 2) opponent_nfl_team := canonical[(year, opp_franchise)]
    con.execute(f"""UPDATE st SET opponent_nfl_team=c.code
        FROM canon c WHERE st.year=c.year AND st.opponent_nfl_franchise_number=c.fr
          AND st.opponent_nfl_team IS DISTINCT FROM c.code""")
    # 3) DST id := DEF-<franchise_number> for EVERY DST row whose id doesn't already
    #    match (catches DEF-<abbrev> AND DEF-<wrong-number> like the Jets/Titans DEF-138),
    #    and rebuild player_week to the same scheme.
    con.execute(f"""UPDATE st SET
        NFL_player_id='DEF-'||CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR),
        player_week='DEF-'||CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR)||'_'
                    ||CAST(CAST(year AS BIGINT) AS VARCHAR)||'_'||CAST(CAST(week AS BIGINT) AS VARCHAR),
        {PROV_COL}=CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}' ELSE {PROV_COL}||',{PROV}' END
        WHERE NFL_player_id LIKE 'DEF-%' AND nfl_franchise_number IS NOT NULL
          AND NFL_player_id <> 'DEF-'||CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR)""")
    after = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]
    post = _audit(con, "st")
    con.execute(f"""UPDATE st SET {PROV_COL}=array_to_string(list_distinct(string_split({PROV_COL},',')),',')
                    WHERE {PROV_COL} IS NOT NULL AND {PROV_COL} LIKE '%,%'""")

    vp = Path(v26); tmp = vp.with_name(vp.stem + "_fntmp.parquet")
    rdr = con.execute("SELECT * FROM st").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema)
    for b in rdr:
        w.write_batch(b)
    w.close(); con.close(); shutil.rmtree(sp, ignore_errors=True)

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0 and after == before
            and post["within_team"] == 0 and post["within_opp"] == 0
            and post["team_vs_opp"] == 0 and post["def_abbrev_ids"] == 0
            and post["def_id_ne_fn"] == 0 and post["fn_multi_id"] == 0)
    res = {"before": int(before), "after": int(after), "pre": pre, "post": post,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_prefn_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


def apply_def_ids_streaming() -> dict:
    """Streaming enforcement of DST id := DEF-{franchise_number} (step 3 only).

    The materialized apply_norm() path OOMs the 1,010-column table on a laptop; when
    the abbrev normalization is already clean (within_team/opp/team_vs_opp == 0) only
    the DEF-id hardening remains, and that is a per-row rewrite streamable in one pass.
    """
    v26 = latest_v26(); stamp = utc_stamp()
    con = duckdb.connect()
    con.execute("PRAGMA threads=1"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='8GB'")
    src = f"read_parquet('{Path(v26).as_posix()}')"
    before = con.execute(f"SELECT COUNT(*) FROM {src}").fetchone()[0]
    pre = _audit(con, src)
    fn = "CAST(CAST(nfl_franchise_number AS BIGINT) AS VARCHAR)"
    is_def = "NFL_player_id LIKE 'DEF-%' AND nfl_franchise_number IS NOT NULL"
    changed = f"({is_def}) AND NFL_player_id <> 'DEF-'||{fn}"
    transform = f"""
    SELECT * REPLACE (
        CASE WHEN {is_def} THEN 'DEF-'||{fn} ELSE NFL_player_id END AS NFL_player_id,
        CASE WHEN {is_def} THEN 'DEF-'||{fn}||'_'||CAST(CAST(year AS BIGINT) AS VARCHAR)
                                 ||'_'||CAST(CAST(week AS BIGINT) AS VARCHAR)
             ELSE player_week END AS player_week,
        CASE WHEN {changed} THEN
               CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN '{PROV}'
                    WHEN {PROV_COL} LIKE '%{PROV}%' THEN {PROV_COL}
                    ELSE {PROV_COL}||',{PROV}' END
             ELSE {PROV_COL} END AS {PROV_COL}
    ) FROM {src}
    """
    vp = Path(v26); tmp = vp.with_name(vp.stem + "_defidtmp.parquet")
    rdr = con.execute(transform).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rdr.schema)
    for b in rdr:
        w.write_batch(b)
    w.close()
    tq = f"read_parquet('{tmp.as_posix()}')"
    after = con.execute(f"SELECT COUNT(*) FROM {tq}").fetchone()[0]
    post = _audit(con, tq)
    con.close()

    from . import golden_samples
    import scripts.sota_recon.sources as S
    o = S.latest_v26; S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = o
    gate = (g["failed"] == 0 and after == before
            and post["def_id_ne_fn"] == 0 and post["fn_multi_id"] == 0
            and post["def_abbrev_ids"] == 0)
    res = {"before": int(before), "after": int(after), "pre": pre, "post": post,
           "golden": f"{g['passed']}/{g['total']}", "gate_pass": bool(gate), "temp": str(tmp)}
    if gate:
        bk = vp.with_name(vp.stem + f"_predefid_backup_{stamp}.parquet")
        shutil.copy2(vp, bk); os.replace(tmp, vp); res["backup"] = str(bk); res["swapped"] = True
    else:
        res["swapped"] = False
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--def-ids-only", action="store_true",
                    help="streaming DEF-id enforcement only (abbrev already normalized)")
    a = ap.parse_args()
    if a.def_ids_only:
        r = apply_def_ids_streaming()
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        print(f"  before: {r['pre']}")
        print(f"  after:  {r['post']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    elif a.apply:
        r = apply_norm()
        print(f"rows {r['before']:,}->{r['after']:,} | golden {r['golden']}")
        print(f"  before: {r['pre']}")
        print(f"  after:  {r['post']}")
        print(f"GATE {'PASS' if r['gate_pass'] else 'FAIL'} -> "
              + (f"SWAPPED (backup {r['backup']})" if r["swapped"] else f"NOT swapped; temp {r['temp']}"))
    else:
        v = latest_v26(); con = duckdb.connect(); con.execute("SET memory_limit='3GB'")
        print(_audit(con, f"read_parquet('{v}')"))
