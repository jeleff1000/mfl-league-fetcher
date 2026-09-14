"""
sota_recon/recon_identity_ids.py  --  cross-ID identity tripwires (beyond same-human NFL_player_id splits).

recon_identity_splits catches "one human wearing two NFL_player_ids". This lane catches the OTHER
identity failure modes that live in the ID columns themselves:

  ORPHAN_STATS    super-table NFL_player_ids with NO player_bio row (stats with no identity anchor)
  DUP_BIO         NFL_player_id appearing in >1 player_bio row (the 1:1 invariant broken)
  REVERSE_SPLIT   one external id (pfr_id / yahoo / sleeper / espn) mapped to >1 NFL_player_id in bio
                  (= two of our ids that are really the same person per the platform's own id)
  BIO_NO_STATS    bio rows with no super-table stats (informational; expected for stubs/rookies)

Local-only (reads the v26 super table + ops_data/nfl_historical/player_bio.parquet). FAIL on
ORPHAN_STATS or DUP_BIO or REVERSE_SPLIT (real identity breakage); BIO_NO_STATS is informational.

    python -m scripts.sota_recon.recon_identity_ids
"""
from __future__ import annotations
from pathlib import Path
import duckdb
from .sources import latest_v26

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
EXTERNAL_IDS = ["pfr_id", "yahoo_player_id", "sleeper_player_id", "espn_id"]


def run(run_dir: str | None = None) -> dict:
    v26 = latest_v26(); wk = Path(v26).as_posix(); bio = Path(BIO).as_posix()
    con = duckdb.connect(); con.execute("SET memory_limit='4GB'")
    biocols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM '{bio}'").fetchall()}

    # ORPHAN_STATS: distinct super-table ids absent from bio
    orphan = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT DISTINCT NFL_player_id FROM '{wk}' WHERE NFL_player_id IS NOT NULL
        ) s LEFT JOIN '{bio}' b USING (NFL_player_id) WHERE b.NFL_player_id IS NULL
    """).fetchone()[0]

    # DUP_BIO: NFL_player_id appearing >1 in bio
    dup_bio = con.execute(f"""
        SELECT COUNT(*) FROM (SELECT NFL_player_id, COUNT(*) n FROM '{bio}'
        WHERE NFL_player_id IS NOT NULL GROUP BY 1 HAVING COUNT(*)>1)
    """).fetchone()[0]

    # REVERSE_SPLIT: one external id -> >1 NFL_player_id
    reverse = {}
    for ext in EXTERNAL_IDS:
        if ext not in biocols:
            continue
        n = con.execute(f"""
            SELECT COUNT(*) FROM (
              SELECT {ext} FROM '{bio}'
              WHERE {ext} IS NOT NULL AND CAST({ext} AS VARCHAR) NOT IN ('','0','0.0')
              GROUP BY {ext} HAVING COUNT(DISTINCT NFL_player_id) > 1
            )
        """).fetchone()[0]
        reverse[ext] = n

    # BIO_NO_STATS (informational)
    bio_no_stats = con.execute(f"""
        SELECT COUNT(*) FROM '{bio}' b
        LEFT JOIN (SELECT DISTINCT NFL_player_id FROM '{wk}') s USING (NFL_player_id)
        WHERE s.NFL_player_id IS NULL AND b.NFL_player_id IS NOT NULL
    """).fetchone()[0]
    con.close()

    reverse_total = sum(reverse.values())
    status = "fail" if (orphan or dup_bio or reverse_total) else "pass"
    return {"status": status, "counts": {
        "orphan_stats": orphan, "dup_bio": dup_bio, "reverse_split_total": reverse_total,
        "reverse_by_id": reverse, "bio_no_stats": bio_no_stats}}


if __name__ == "__main__":
    r = run()
    c = r["counts"]
    print(f"[{r['status']}] cross-ID identity tripwires")
    print(f"  ORPHAN_STATS  (stat ids w/o bio): {c['orphan_stats']:,}")
    print(f"  DUP_BIO       (id in >1 bio row): {c['dup_bio']:,}")
    print(f"  REVERSE_SPLIT (ext id -> >1 nfl id): {c['reverse_split_total']:,}  {c['reverse_by_id']}")
    print(f"  BIO_NO_STATS  (informational): {c['bio_no_stats']:,}")
