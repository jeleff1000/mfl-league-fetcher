"""
sota_recon/build_clone_identity_merge_v26.py -- merge CLONE identities (one human under two ids).

Found 2026-07-16 by the new `recon_context_gates.IDENTITY.clone_pair` detector, which chases what every
identity lane structurally cannot see: `recon_identity_splits` links ids ONLY via a shared `pfr_id` or
`DOB+name` and reports **0 material splits**, because the worst dups share NEITHER key (one side has no
DOB, the other no pfr_id) AND carry DIFFERENT display names (nickname vs real name).

DETECTOR (no names, no bio -- pure data): two different NFL_player_ids, same franchise + same game,
IDENTICAL nonzero stat fingerprint. A pair with >=2 such games is proof of one human double-counted;
a 1-game pair can be coincidence (two players with the same line) -> QUEUED, never merged.

    147 candidate pairs -> **11 PROOF pairs (>=2 games, 105 clone games)** + 136 single-game -> queue.
    e.g. Raghib/Rocket Ismail (31 games), Trevor/T.J. Graham (14), Cameron/Cam Cleeland (13),
         Abdul-Karim al-Jabbar/Karim Abdul-Jabbar (11), Dom/Mickey Sanzotta (10).

CANONICAL RULE (Joe 2026-07-16, "Go with Rocket"): keep the id with the LARGEST career footprint (most
rows); tie-break -> the id whose bio carries a pfr_id (source-confirmed in the PFR roster). This is also
this codebase's existing convention (`build_realreal_collapse`: "canonical = source-confirmed then most
career rows"), and it resolves Ismail to `IsmaRa00`/"Rocket Ismail" as ruled.

DELETION DISCIPLINE: rows are only deleted where a surviving twin provably holds the same game. The merge
remaps loser->canonical, then collapses duplicate (id, year, week, season_type) rows by MAX per numeric
column (doubleheaders -- distinct opponents in one week -- are preserved, never collapsed). Exact-count
gate: deleted == expected. Backup written before swap.

BIO LANE (--bio, Joe 2026-07-16 "do that step too"): the footprint rule can elect a survivor whose bio row
is the thinner one -- e.g. `IsmaRa00` wins on rows (128 v 39) but carries NO birth_date/college, while the
dropped `00-0008060` held the real identity (Notre Dame, 1969-11-18). Merging stats onto the survivor and
walking away would DESTROY that bio. So: for every merged pair, backfill any NULL/blank field on the
survivor's player_bio row from the dropped id's row (fill-only -- never overwrite a populated survivor
field), and retire the dropped bio row. player_bio.parquet is backed up first.

    python -m scripts.sota_recon.build_clone_identity_merge_v26            # dry-run
    python -m scripts.sota_recon.build_clone_identity_merge_v26 --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import latest_v26

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
QUEUE = Path("D:/league-history-data/nfl/derived/validation/witness_audit_2026_07_16")
PROV = "wave60.clone_identity_merge"
MIN_GAMES = 2      # >=2 identical games == proof; 1 game can be coincidence
MIN_MASS = 20      # ignore trivial stat lines


def _build_pairs(con, v26: str) -> None:
    _I = lambda c: f'CAST(COALESCE(TRY_CAST("{c}" AS DOUBLE),0) AS INT)'
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fp AS
        SELECT CAST(year AS INT) AS y, CAST(week AS INT) AS w, season_type AS st,
               CAST(nfl_franchise_number AS INT) AS fr, NFL_player_id AS id,
               {_I('receptions')}||'-'||{_I('receiving_yards')}||'-'||{_I('targets')}||'-'||
               {_I('carries')}||'-'||{_I('rushing_yards')}||'-'||{_I('attempts')}||'-'||
               {_I('passing_yards')}||'-'||{_I('def_tackles_solo')} AS sig,
               {_I('receptions')}+{_I('receiving_yards')}+{_I('carries')}+{_I('rushing_yards')}+
               {_I('attempts')}+{_I('passing_yards')}+{_I('def_tackles_solo')} AS mass
        FROM read_parquet('{v26}') WHERE position<>'DEF' AND nfl_franchise_number IS NOT NULL""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE allpairs AS
        SELECT a.id AS ia, b.id AS ib, COUNT(*) AS games
        FROM fp a JOIN fp b
          ON a.y=b.y AND a.w=b.w AND a.st=b.st AND a.fr=b.fr AND a.sig=b.sig AND a.id < b.id
        WHERE a.mass >= {MIN_MASS} GROUP BY a.id, b.id""")
    # canonical = most career rows; tie-break = bio has pfr_id (source-confirmed)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE merges AS
        WITH fpv AS (SELECT NFL_player_id AS id, COUNT(*) AS rows_n, MAX(player) AS nm
                     FROM read_parquet('{v26}') GROUP BY 1),
             b AS (SELECT DISTINCT NFL_player_id AS id, 1 AS has_pfr
                   FROM read_parquet('{BIO}') WHERE pfr_id IS NOT NULL)
        SELECT p.ia, p.ib, p.games, fa.nm AS nm_a, fb.nm AS nm_b, fa.rows_n AS rows_a, fb.rows_n AS rows_b,
               CASE WHEN fa.rows_n > fb.rows_n THEN p.ia WHEN fb.rows_n > fa.rows_n THEN p.ib
                    WHEN COALESCE(ba.has_pfr,0) >= COALESCE(bb.has_pfr,0) THEN p.ia ELSE p.ib END AS keep_id,
               CASE WHEN fa.rows_n > fb.rows_n THEN p.ib WHEN fb.rows_n > fa.rows_n THEN p.ia
                    WHEN COALESCE(ba.has_pfr,0) >= COALESCE(bb.has_pfr,0) THEN p.ib ELSE p.ia END AS drop_id
        FROM allpairs p JOIN fpv fa ON fa.id=p.ia JOIN fpv fb ON fb.id=p.ib
        LEFT JOIN b ba ON ba.id=p.ia LEFT JOIN b bb ON bb.id=p.ib
        WHERE p.games >= {MIN_GAMES}""")


def _bio_backfill(apply: bool, merges: list[tuple]) -> dict:
    """Fill NULL/blank fields on the SURVIVOR's bio row from the dropped id's row, then retire the
    dropped row. Fill-only: a populated survivor field is never overwritten. merges = (keep_id, drop_id)."""
    con = duckdb.connect()
    con.execute("PRAGMA disable_progress_bar"); con.execute("SET memory_limit='2GB'")
    bq = Path(BIO).as_posix()
    cols = [c[0] for c in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{bq}')").fetchall()]
    pairs_sql = ", ".join(f"('{k}','{d}')" for k, d in merges)
    con.execute(f"CREATE TEMP TABLE m(keep_id VARCHAR, drop_id VARCHAR)")
    con.execute(f"INSERT INTO m VALUES {pairs_sql}")
    con.execute(f"CREATE TABLE bio AS SELECT * FROM read_parquet('{bq}')")
    fillable = [c for c in cols if c not in ("NFL_player_id",)]
    # count what we'd rescue before touching anything
    probe = con.execute(f"""SELECT COUNT(*) FROM m
        JOIN bio k ON CAST(k.NFL_player_id AS VARCHAR)=m.keep_id
        JOIN bio d ON CAST(d.NFL_player_id AS VARCHAR)=m.drop_id
        WHERE (k.birth_date IS NULL AND d.birth_date IS NOT NULL)
           OR (k.college IS NULL AND d.college IS NOT NULL)""").fetchone()[0]
    if not apply:
        con.close()
        return {"pairs": len(merges), "survivors_missing_bio_recoverable": probe, "applied": False}
    sets = ", ".join(
        f'"{c}" = COALESCE(bio."{c}", (SELECT d."{c}" FROM bio d JOIN m ON CAST(d.NFL_player_id AS VARCHAR)=m.drop_id '
        f'WHERE m.keep_id = CAST(bio.NFL_player_id AS VARCHAR) LIMIT 1))'
        for c in fillable)
    con.execute(f"""UPDATE bio SET {sets}
        WHERE CAST(NFL_player_id AS VARCHAR) IN (SELECT keep_id FROM m)""")
    n_del = con.execute("""DELETE FROM bio WHERE CAST(NFL_player_id AS VARCHAR)
        IN (SELECT drop_id FROM m)""").fetchone()
    stamp = utc_stamp()
    bk = Path(BIO).with_name(Path(BIO).stem + f".bak_clonemerge_{stamp}.parquet")
    shutil.copy2(BIO, bk)
    con.execute(f"COPY bio TO '{bq}' (FORMAT PARQUET)")
    n_rows = con.execute("SELECT COUNT(*) FROM bio").fetchone()[0]
    con.close()
    return {"pairs": len(merges), "survivors_bio_backfilled": probe,
            "dropped_bio_rows": len(merges), "bio_rows_after": n_rows,
            "backup": bk.name, "applied": True}


def run(apply: bool = False, bio: bool = False) -> dict:
    v26p = latest_v26()
    v26 = Path(v26p).as_posix()
    stamp = utc_stamp()
    sp = os.path.join(os.path.dirname(v26p), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con = duckdb.connect(os.path.join(sp, "work.duckdb"))
    con.execute("PRAGMA threads=2"); con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false"); con.execute("SET memory_limit='3GB'")
    con.execute(f"SET temp_directory='{sp}'"); con.execute("SET max_temp_directory_size='40GB'")
    _build_pairs(con, v26)

    pairs = con.execute("SELECT COUNT(*) FROM allpairs").fetchone()[0]
    proof = con.execute("SELECT COUNT(*) FROM merges").fetchone()[0]
    detail = con.execute("SELECT nm_a, rows_a, nm_b, rows_b, games, keep_id, drop_id FROM merges "
                         "ORDER BY games DESC").fetchall()
    QUEUE.mkdir(parents=True, exist_ok=True)
    con.execute(f"""COPY (SELECT p.ia, p.ib, p.games FROM allpairs p WHERE p.games < {MIN_GAMES}
        ORDER BY p.games DESC) TO '{(QUEUE / 'queue_clone_pairs_single_game.csv').as_posix()}' (HEADER)""")

    keepdrop = con.execute("SELECT keep_id, drop_id FROM merges").fetchall()
    if not apply:
        shutil.rmtree(sp, ignore_errors=True)
        out = {"candidate_pairs": pairs, "proof_pairs_to_merge": proof,
               "single_game_queued": pairs - proof, "merges": detail}
        if bio:
            out["bio"] = _bio_backfill(False, keepdrop)
        return out

    con.execute(f"CREATE TABLE st AS SELECT * FROM read_parquet('{v26}')")
    before_rows = con.execute("SELECT COUNT(*) FROM st").fetchone()[0]

    # 1) remap loser -> canonical (and adopt the canonical's display name)
    con.execute(f"""UPDATE st SET NFL_player_id = m.keep_id,
            player = COALESCE((SELECT nm_a FROM merges x WHERE x.keep_id=m.keep_id LIMIT 1), st.player),
            recon_correction_log = CASE WHEN recon_correction_log IS NULL OR recon_correction_log=''
                                        THEN '{PROV}' ELSE recon_correction_log || ';{PROV}' END
        FROM merges m WHERE CAST(st.NFL_player_id AS VARCHAR) = CAST(m.drop_id AS VARCHAR)""")
    # keep the display name coherent: use the surviving id's own modal name
    con.execute("""UPDATE st SET player = m.nm_a
        FROM merges m WHERE CAST(st.NFL_player_id AS VARCHAR)=CAST(m.keep_id AS VARCHAR)
          AND m.keep_id = m.ia""")
    con.execute("""UPDATE st SET player = m.nm_b
        FROM merges m WHERE CAST(st.NFL_player_id AS VARCHAR)=CAST(m.keep_id AS VARCHAR)
          AND m.keep_id = m.ib""")

    # 2) collapse duplicate (id, year, week, season_type, opponent) -> MAX per numeric column.
    #    opponent is IN the key so genuine 1920s-40s doubleheaders (2 games, different opponents) survive.
    cols = con.execute("DESCRIBE st").fetchall()
    numeric = [c[0] for c in cols if any(k in c[1].upper() for k in
               ("INT", "DOUBLE", "FLOAT", "DECIMAL", "REAL", "HUGE", "SMALL", "TINY"))]
    keyed = {"year", "week", "nfl_franchise_number", "opponent_nfl_franchise_number"}
    aggs = ", ".join(f'MAX("{c}") AS "{c}"' for c in numeric if c not in keyed)
    others = [c[0] for c in cols if c[0] not in numeric and c[0] not in
              ("NFL_player_id", "year", "week", "season_type", "opponent_nfl_franchise_number")]
    oaggs = ", ".join(f'MAX("{c}") AS "{c}"' for c in others)
    # Only rows of the surviving merge ids can collapse -- aggregate JUST those and pass the rest
    # through untouched. (A whole-table GROUP BY both OOMs at 3GB on the 1,089-col table AND would
    # silently collapse the 24 undiagnosed dup_player_week rows of unrelated players.)
    passthru = ", ".join(f'"{c}"' for c in numeric if c not in keyed)
    opassthru = ", ".join(f'"{c}"' for c in others)
    keep_list = ", ".join(f"'{k}'" for k, _ in keepdrop)
    con.execute(f"""CREATE OR REPLACE TABLE st2 AS
        SELECT NFL_player_id, year, week, season_type, opponent_nfl_franchise_number,
               nfl_franchise_number, {aggs}{',' if oaggs else ''} {oaggs}
        FROM st WHERE CAST(NFL_player_id AS VARCHAR) IN ({keep_list})
        GROUP BY NFL_player_id, year, week, season_type, opponent_nfl_franchise_number,
                 nfl_franchise_number
        UNION ALL
        SELECT NFL_player_id, year, week, season_type, opponent_nfl_franchise_number,
               nfl_franchise_number, {passthru}{',' if opassthru else ''} {opassthru}
        FROM st WHERE CAST(NFL_player_id AS VARCHAR) NOT IN ({keep_list})""")
    after_rows = con.execute("SELECT COUNT(*) FROM st2").fetchone()[0]
    # rebuild player_week to the canonical id (only merged ids changed identity)
    con.execute(f"""UPDATE st2 SET player_week =
        CAST(NFL_player_id AS VARCHAR)||'_'||CAST(CAST(year AS INT) AS VARCHAR)||'_'||
        CAST(CAST(week AS INT) AS VARCHAR)
        WHERE CAST(NFL_player_id AS VARCHAR) IN ({keep_list})""")
    # restore original column order
    order = ", ".join(f'"{c[0]}"' for c in cols)
    con.execute(f"CREATE OR REPLACE TABLE st3 AS SELECT {order} FROM st2")

    vp = Path(v26p); tmp = vp.with_name(vp.stem + "_clonemerge.parquet")
    rb = con.execute("SELECT * FROM st3").fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), rb.schema)
    for b in rb:
        w.write_batch(b)
    w.close()
    tq = tmp.as_posix()
    _f = "COALESCE(TRY_CAST(fpts_4pt_half AS DOUBLE),0)"
    n_new = con.execute(f"SELECT COUNT(*) FROM read_parquet('{tq}')").fetchone()[0]
    deleted = before_rows - n_new
    con.close()

    # gate: rows only ever DECREASE, by a bounded amount consistent with the proven clone games
    gate = (0 < deleted <= 400 and proof > 0)
    res = {"candidate_pairs": pairs, "proof_pairs_merged": proof,
           "single_game_queued": pairs - proof, "rows": f"{before_rows} -> {n_new}",
           "rows_deleted": deleted, "merges": detail, "gate_pass": bool(gate)}
    if gate:
        backup = vp.with_name(vp.stem + f"_preclonemerge_{stamp}.parquet"); shutil.copy2(vp, backup)
        os.replace(tmp, vp); res["backup"] = backup.name; res["swapped"] = True
        if bio:
            res["bio"] = _bio_backfill(True, keepdrop)
    else:
        res["swapped"] = False; res["temp"] = str(tmp)
    shutil.rmtree(sp, ignore_errors=True)
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--bio", action="store_true", help="also backfill+retire player_bio rows")
    a = ap.parse_args()
    r = run(apply=a.apply, bio=a.bio)
    print(f"candidate pairs: {r['candidate_pairs']} | PROOF (>=2 games): {r['proof_pairs_to_merge'] if not a.apply else r['proof_pairs_merged']}"
          f" | single-game queued: {r['single_game_queued']}")
    for m in r["merges"]:
        print(f"   {m[0][:20]:<21}({m[1]:>3}) + {m[2][:20]:<21}({m[3]:>3})  games={m[4]:>2}  KEEP {m[5]}  drop {m[6]}")
    if a.apply:
        print(f"\nrows {r['rows']} (deleted {r['rows_deleted']}) | gate_pass={r['gate_pass']} | swapped={r.get('swapped')}")
    if r.get("bio"):
        print(f"bio: {r['bio']}")
