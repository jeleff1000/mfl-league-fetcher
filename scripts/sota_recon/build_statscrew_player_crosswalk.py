"""Build the receipted StatsCrew-player -> PFR/NFL identity crosswalk.

The match is deliberately fail-closed:
  * normalize the captured player name;
  * require a unique PFR index name candidate;
  * require the StatsCrew observed year span to overlap the PFR index career span;
  * require that PFR id to exist in player_bio.

No name-only tie is guessed.  The output is a crosswalk receipt used by the StatsCrew
MapSpec value path, not a mutation of any witness or supertable plane.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from . import sources as S

OUT_DIR = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT_PARQUET = OUT_DIR / "statscrew_player_pfr_crosswalk.parquet"
OUT_RECEIPT = OUT_DIR / "statscrew_player_pfr_crosswalk_receipt.json"


def main() -> int:
    stats = Path(S.registry()["statscrew_team_season_stats"].path).as_posix()
    pfr = Path(S.registry()["pfr_player_index"].path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    con.execute(f"""
        COPY (
            WITH sc AS (
                SELECT source_player_id,
                       lower(regexp_replace(trim(player), '[^a-z0-9]', '', 'g')) AS name_key,
                       MIN(TRY_CAST(season AS INT)) AS sc_first_year,
                       MAX(TRY_CAST(season AS INT)) AS sc_last_year
                FROM read_parquet('{stats}', union_by_name=true)
                WHERE source_player_id IS NOT NULL AND player IS NOT NULL
                GROUP BY 1, 2
            ), pi AS (
                SELECT pfr_id,
                       lower(regexp_replace(trim(player), '[^a-z0-9]', '', 'g')) AS name_key,
                       TRY_CAST(first_year AS INT) AS pfr_first_year,
                       TRY_CAST(last_year AS INT) AS pfr_last_year
                FROM read_parquet('{pfr}')
                WHERE pfr_id IS NOT NULL AND player IS NOT NULL
            ), candidates AS (
                SELECT sc.*, pi.pfr_id, pi.pfr_first_year, pi.pfr_last_year,
                       COUNT(*) OVER (PARTITION BY sc.source_player_id) AS candidate_count
                FROM sc JOIN pi USING (name_key)
                WHERE sc.sc_last_year >= COALESCE(pi.pfr_first_year, sc.sc_first_year)
                  AND sc.sc_first_year <= COALESCE(pi.pfr_last_year, sc.sc_last_year)
            ), chosen AS (
                SELECT c.*,
                       ROW_NUMBER() OVER (
                         PARTITION BY source_player_id
                         ORDER BY pfr_first_year NULLS LAST, pfr_id
                       ) AS rn
                FROM candidates c
            )
            SELECT source_player_id, pfr_id, b.NFL_player_id,
                   sc_first_year, sc_last_year, pfr_first_year, pfr_last_year,
                   'unique_normalized_name_plus_year_overlap' AS match_method
            FROM chosen c
            JOIN read_parquet('{bio}') b USING (pfr_id)
            WHERE candidate_count = 1 AND rn = 1
        ) TO '{OUT_PARQUET.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = con.execute(f"SELECT COUNT(*) FROM read_parquet('{OUT_PARQUET.as_posix()}')").fetchone()[0]
    ids = con.execute(f"SELECT COUNT(DISTINCT source_player_id) FROM read_parquet('{OUT_PARQUET.as_posix()}')").fetchone()[0]
    total = con.execute(f"""
        SELECT COUNT(DISTINCT source_player_id)
        FROM read_parquet('{stats}', union_by_name=true)
        WHERE source_player_id IS NOT NULL
    """).fetchone()[0]
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    OUT_RECEIPT.write_text(json.dumps({
        "receipt_id": "statscrew_player_pfr_crosswalk_v1",
        "source": "statscrew_team_season_stats",
        "target": "pfr_id+NFL_player_id",
        "method": "unique normalized name plus observed-year/PFR-career overlap, then player_bio existence",
        "total_source_player_ids": total,
        "matched_source_player_ids": ids,
        "matched_rows": n,
        "unmatched_or_ambiguous_ids": total - ids,
        "fail_closed": True,
        "atoms_modified": False,
    }, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"crosswalk": str(OUT_PARQUET), "receipt": str(OUT_RECEIPT),
                      "total_ids": total, "matched_ids": ids, "matched_rows": n,
                      "unmatched_or_ambiguous": total - ids}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
