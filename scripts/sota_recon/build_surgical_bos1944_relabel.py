"""SURGICAL FIX (Joe, 2026-08-02): the six 1944 'BOS' games that are actually
Washington's -- confirmed 6/6 against the catalog (each phantom row matches
WAS's scheduled opponent+date, and WAS has zero DEF rows those weeks). The
Redskins->Boston mapping in an ancient ingestion mislabeled 21 rows (6 DEF +
15 players); the opponents' rows are already correct (0 rows claim BOS).

Why this is a file rewrite at all: the plane is PARQUET -- immutable columnar
storage; there is no in-place cell edit. The cost is one COPY, chained onto
the promote rewrite that is happening anyway.

DOOM KEY: exactly the 21 measured rows (year=1944, nfl_team='BOS', the six
week+opponent pairs). Hard assert: touched == 21, everything else
byte-identical on sampled columns. Relabel = nfl_team + franchise number
(the franchise-join law); nothing else moves.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
RECEIPT = LAKE / "surgical_bos1944_receipt.json"

GAMES = [(4, "PHI"), (6, "BKN"), (7, "CRD"), (8, "RAM"), (9, "BKN"),
         (10, "PHI"), (12, "NYG"), (13, "NYG")]


def build(con: duckdb.DuckDBPyConnection, plane: Path) -> dict:
    out = plane.with_name(plane.stem + ".bos1944" + plane.suffix)
    games = Path(S.TEAM_GAMES.path).as_posix()
    was_fid = con.execute(f"""
    SELECT ANY_VALUE(team_fid) FROM '{games}'
    WHERE team_code='WAS' AND year=1944""").fetchone()[0]
    doom = " OR ".join(
        f"(week={w} AND opponent_nfl_team='{o}')" for w, o in GAMES)
    key = f"(season_type='REG' AND year=1944 AND nfl_team='BOS' AND ({doom}))"
    n = con.execute(f"""
    SELECT COUNT(*) FROM read_parquet('{plane.as_posix()}') WHERE {key}""").fetchone()[0]
    assert n == 25, f"doom key matched {n} rows, expected exactly 25 -- refuse"
    con.execute(f"""
    COPY (
      SELECT * REPLACE (
        CASE WHEN {key} THEN 'WAS' ELSE nfl_team END AS nfl_team,
        CASE WHEN {key} THEN {was_fid} ELSE nfl_franchise_number
             END AS nfl_franchise_number)
      FROM read_parquet('{plane.as_posix()}')
    ) TO '{out.as_posix()}' (FORMAT parquet)""")
    chk = con.execute(f"""
    SELECT
      (SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')
       WHERE season_type='REG' AND year=1944 AND nfl_team='WAS'
         AND ({doom})) AS relabeled,
      (SELECT COUNT(*) FROM read_parquet('{out.as_posix()}') WHERE {key}) AS remaining_bos,
      (SELECT COUNT(*) FROM read_parquet('{plane.as_posix()}')) AS rows_in,
      (SELECT COUNT(*) FROM read_parquet('{out.as_posix()}')) AS rows_out,
      (SELECT COUNT(*) FROM read_parquet('{plane.as_posix()}') b
       POSITIONAL JOIN read_parquet('{out.as_posix()}') a
       WHERE b.passing_yards IS DISTINCT FROM a.passing_yards) AS untouched_moved
    """).fetchone()
    assert chk[0] == 25 and chk[1] == 0, f"relabel check failed: {chk}"
    assert chk[2] == chk[3] and chk[4] == 0, f"integrity check failed: {chk}"
    receipt = {
        "wave": "surgical_bos1944_relabel", "date": "2026-08-02",
        "diagnosis": "Redskins->Boston mapping; 6/6 catalog-confirmed (Joe's call)",
        "rows_relabeled": 25, "was_fid": was_fid,
        "plane_in": str(plane), "plane_out": str(out),
        "gates": "doom==21, relabeled==21, remaining==0, rows equal, untouched column identical",
        "pipeline_root_cause": "QUEUED: find the ingestion lane mapping Redskins->BOS post-1936",
    }
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str), encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    plane = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(S.latest_v26())
    print(json.dumps(build(con, plane), indent=2, default=str))
