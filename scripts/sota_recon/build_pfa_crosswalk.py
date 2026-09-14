"""PFA id crosswalk (Joe, 2026-08-02): profootballarchives keys players in its
own id space ('good04400'). Seed = the participation table's (name, seasons)
profile matched against player_bio's (name, first_year..last_year) span.

TWIN DISCIPLINE: a PFA profile maps to a pfr_id ONLY when exactly one bio row
with that normalized name overlaps its season span. Zero candidates =
UNMATCHED (queue: 2,715-slug class), two or more = AMBIGUOUS_TWIN (queue,
never guess -- PFR encodes twins in the id and fuzzy matches LEAK). The
receipt records every rate; nothing is COALESCE'd.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OUT = LAKE / "pfa_pfrid_crosswalk.parquet"
RECEIPT = LAKE / "pfa_crosswalk_receipt.json"

# strip punctuation and generational suffixes before comparing names
NORM = ("LOWER(regexp_replace(regexp_replace({c}, '[.,'']', '', 'g'), "
        "' (jr|sr|ii|iii|iv|v)$', '', 'g'))")


def build(con: duckdb.DuckDBPyConnection) -> dict:
    part = (Path(S.registry()["pfa_player_game_participation"].path).as_posix()
            + "/**/*.parquet")
    bio = Path(S.PLAYER_BIO.path).as_posix()

    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE pfa AS
    SELECT source_player_id AS pfa_id,
           ANY_VALUE(player) AS name,
           MIN(TRY_CAST(season AS INT)) AS y0,
           MAX(TRY_CAST(season AS INT)) AS y1,
           COUNT(DISTINCT game_id) AS n_games
    FROM read_parquet('{part}', union_by_name=true)
    WHERE source_player_id IS NOT NULL AND player IS NOT NULL
    GROUP BY 1""")

    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE cand AS
    SELECT p.pfa_id, p.name, p.y0, p.y1, p.n_games,
           b.pfr_id, b.NFL_player_id
    FROM pfa p
    JOIN read_parquet('{bio}') b
      ON {NORM.format(c='p.name')} = {NORM.format(c='b.player')}
     AND b.pfr_id IS NOT NULL
     AND p.y1 >= TRY_CAST(b.first_year AS INT) - 2
     AND p.y0 <= TRY_CAST(b.last_year AS INT) + 2""")

    # CORROBORATION (the denominator law, 2026-08-02): the match rate measures
    # RESOLUTION -- one candidate resolved -- not CORRECTNESS. The layer
    # beneath is span containment: a MATCHED profile whose pfa seasons fall
    # outside the matched player's own career (+/-2) is the wrong-person
    # signature (true player absent from bio, name collided with another era).
    # 811 of 17,173 first-pass matches demoted this way.
    con.execute(f"""
    COPY (
      SELECT p.pfa_id, p.name, p.y0, p.y1, p.n_games,
             CASE WHEN c.n = 1 AND NOT viol THEN c.one_pfr END AS pfr_id,
             CASE WHEN c.n = 1 AND NOT viol THEN c.one_gsis END AS NFL_player_id,
             CASE WHEN c.n IS NULL THEN 'UNMATCHED'
                  WHEN c.n = 1 AND viol THEN 'SPAN_VIOLATION'
                  WHEN c.n = 1 THEN 'MATCHED'
                  ELSE 'AMBIGUOUS_TWIN' END AS status,
             COALESCE(c.n, 0) AS n_candidates
      FROM pfa p
      LEFT JOIN (
        SELECT pfa_id, COUNT(DISTINCT pfr_id) AS n,
               ANY_VALUE(pfr_id) AS one_pfr,
               ANY_VALUE(NFL_player_id) AS one_gsis
        FROM cand GROUP BY 1
      ) c USING (pfa_id)
      LEFT JOIN LATERAL (
        SELECT p.y0 < TRY_CAST(b.first_year AS INT) - 2
            OR p.y1 > TRY_CAST(b.last_year AS INT) + 2 AS viol
        FROM read_parquet('{bio}') b WHERE b.pfr_id = c.one_pfr LIMIT 1
      ) v ON TRUE
    ) TO '{OUT.as_posix()}' (FORMAT parquet)""")

    stats = dict(con.execute(f"""
    SELECT status, COUNT(*) FROM read_parquet('{OUT.as_posix()}')
    GROUP BY 1""").fetchall())
    weighted = dict(con.execute(f"""
    SELECT status, SUM(n_games) FROM read_parquet('{OUT.as_posix()}')
    GROUP BY 1""").fetchall())
    total = sum(stats.values())
    receipt = {
        "wave": "pfa_crosswalk", "date": "2026-08-02",
        "seed": "participation (name, season-span) x bio (name, first..last_year +/-2)",
        "profiles": total, "by_status": stats,
        "match_rate": round(stats.get("MATCHED", 0) / total, 4) if total else None,
        "game_weighted": {k: int(v) for k, v in weighted.items()},
        "game_weighted_match_rate": round(
            weighted.get("MATCHED", 0) / max(sum(weighted.values()), 1), 4),
        "law": "exactly-one-or-abstain; ambiguous twins queued, never guessed",
        "out": str(OUT),
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2))
