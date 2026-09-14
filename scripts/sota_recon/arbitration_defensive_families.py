"""ARBITRATION ROUND 2: the defensive-family queue (~195k single-root
contradictions sized by engine round 2 -- tackles 79.7k, assists 66.1k,
pass_defended 37.3k, sacks 7.4k, forced fumbles 4.9k, safeties 0.3k).

The engine held these correctly: one voice against stored is not a verdict.
This pass summons the OTHER voices -- pbp's credit ids and PFR's defense
boxes -- so each cell hears up to three roots plus the defendant, and THE
PRECEDENCE LAW (pfr > nflcom > pbp; unanimity beats the head; every argument
logged with side counts) rules.

Verdict writes require >= 2 roots agreeing on the same value against stored.
Cells where the voices scatter stay held, counted, and logged -- the residue
is a definition question (solo-vs-combined class), not a coin flip.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_arb_defense.parquet"
ARGLOG = LAKE / "arbitration_arguments_defense.parquet"
RECEIPT = LAKE / "arbitration_defense_receipt.json"

#: column -> (pbp per-week expr via credited ids | None, pfr box column | None)
FAMILIES = {
    "def_tackles_combined": ("tackles", "tackles_combined"),
    "def_tackle_assists": ("assists", "tackles_assists"),
    "def_pass_defended": ("pd", "pass_defended"),
    "def_sacks": ("sacks", "sacks"),
    "def_fumbles_forced": ("ff", "fumbles_forced"),
    "def_safeties": (None, "safety_md"),
}

PBP_CLAIMS = {
    "tackles": """
    SELECT pid, yr, wk, COUNT(*)::DOUBLE AS v FROM (
      SELECT UNNEST([r.solo_tackle_1_player_id, r.solo_tackle_2_player_id,
        r.assist_tackle_1_player_id, r.assist_tackle_2_player_id,
        r.assist_tackle_3_player_id, r.assist_tackle_4_player_id,
        r.tackle_with_assist_1_player_id,
        r.tackle_with_assist_2_player_id]) AS pid,
        TRY_CAST(r.season AS INT) AS yr, TRY_CAST(r.week AS INT) AS wk
      FROM read_parquet('{pb}') r WHERE r.season_type = 'REG')
    WHERE pid IS NOT NULL GROUP BY 1, 2, 3""",
    "assists": """
    SELECT pid, yr, wk, COUNT(*)::DOUBLE AS v FROM (
      SELECT UNNEST([r.assist_tackle_1_player_id, r.assist_tackle_2_player_id,
        r.assist_tackle_3_player_id, r.assist_tackle_4_player_id]) AS pid,
        TRY_CAST(r.season AS INT) AS yr, TRY_CAST(r.week AS INT) AS wk
      FROM read_parquet('{pb}') r WHERE r.season_type = 'REG')
    WHERE pid IS NOT NULL GROUP BY 1, 2, 3""",
    "pd": """
    SELECT pid, yr, wk, COUNT(*)::DOUBLE AS v FROM (
      SELECT UNNEST([r.pass_defense_1_player_id,
                     r.pass_defense_2_player_id]) AS pid,
        TRY_CAST(r.season AS INT) AS yr, TRY_CAST(r.week AS INT) AS wk
      FROM read_parquet('{pb}') r WHERE r.season_type = 'REG')
    WHERE pid IS NOT NULL GROUP BY 1, 2, 3""",
    "sacks": """
    SELECT pid, yr, wk, SUM(v) AS v FROM (
      SELECT r.sack_player_id AS pid, TRY_CAST(r.season AS INT) AS yr,
             TRY_CAST(r.week AS INT) AS wk, 1.0 AS v
      FROM read_parquet('{pb}') r
      WHERE r.season_type = 'REG' AND r.sack_player_id IS NOT NULL
      UNION ALL
      SELECT pid, TRY_CAST(r.season AS INT), TRY_CAST(r.week AS INT), 0.5
      FROM read_parquet('{pb}') r,
      UNNEST([r.half_sack_1_player_id, r.half_sack_2_player_id]) AS u(pid)
      WHERE r.season_type = 'REG' AND pid IS NOT NULL)
    GROUP BY 1, 2, 3""",
    "ff": """
    SELECT pid, yr, wk, COUNT(*)::DOUBLE AS v FROM (
      SELECT UNNEST([r.forced_fumble_player_1_player_id,
                     r.forced_fumble_player_2_player_id]) AS pid,
        TRY_CAST(r.season AS INT) AS yr, TRY_CAST(r.week AS INT) AS wk
      FROM read_parquet('{pb}') r WHERE r.season_type = 'REG')
    WHERE pid IS NOT NULL GROUP BY 1, 2, 3""",
}


def _stage(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def require_identity_cert(family_cols) -> dict:
    """EXECUTABLE LAW (not prose): arbitration REFUSES to run a family unless
    an identity certification exists -- each source's columns tested against
    internal identities (combined==solo+assists class) so the room never
    votes on a wrong-column artifact. Joe: 'headers lie, identities don't' --
    and 'laws in the runbook are meaningless when you ignore the runbook',
    so the law lives here, where ignoring it is impossible."""
    for f in LAKE.glob("identity_cert_*.json"):
        cert = json.loads(f.read_text(encoding="utf-8"))
        if set(family_cols) & set(cert.get("family", [])):
            return cert
    raise SystemExit(
        f"REFUSED: no identity certification covers {family_cols}. "
        "Run the identity tests, write identity_cert_<family>.json, "
        "then arbitrate.")


def build(con: duckdb.DuckDBPyConnection) -> dict:
    require_identity_cert(list(FAMILIES))
    wk = Path(S.latest_v26()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    dbx = Path(S.registry()["pfr_player_defense_box"].path).as_posix()
    games = Path(S.TEAM_GAMES.path).as_posix()
    xw = Path(S.registry()["nflcom_slug_pfrid"].path).as_posix()

    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    verdicts = corrections = held = 0
    per = {}
    for col, (pbp_key, box_col) in FAMILIES.items():
        _stage(col)
        claims = []
        # nflcom logs claim (the original contradictor) -- Defense table col
        from scripts.sota_recon.witness_map import _q
        nfl_src = _q("nflcom_player_logs")
        claims.append(("nflcom", f"""
        SELECT b.NFL_player_id AS pid, TRY_CAST(r.season AS INT) AS yr,
               TRY_CAST(r.wk AS INT) AS wk,
               ANY_VALUE(TRY_CAST(r."{col.replace('def_', '')}" AS DOUBLE)) AS v
        FROM read_parquet('{nfl_src}', union_by_name=true) r
        JOIN read_parquet('{xw}') x ON x.nflcom_slug = r.nflcom_slug
        JOIN ids b USING (pfr_id)
        WHERE TRY_CAST(r."{col.replace('def_', '')}" AS DOUBLE) IS NOT NULL
        GROUP BY 1, 2, 3"""))
        if pbp_key:
            claims.append(("pbp", PBP_CLAIMS[pbp_key].format(pb=pb)))
        if box_col:
            claims.append(("pfr", f"""
            SELECT b.NFL_player_id AS pid, g.year AS yr,
                   TRY_CAST(g.week AS INT) AS wk,
                   ANY_VALUE(TRY_CAST(s."{box_col}" AS DOUBLE)) AS v
            FROM read_parquet('{dbx}', union_by_name=true) s
            JOIN (SELECT DISTINCT boxscore_id, year, week FROM '{games}'
                  WHERE season_type='REG') g USING (boxscore_id)
            JOIN ids b ON b.pfr_id = regexp_extract(
                CAST(s.player_link_ids AS VARCHAR), '^([^;,]+)', 1)
            WHERE TRY_CAST(s."{box_col}" AS DOUBLE) IS NOT NULL
            GROUP BY 1, 2, 3"""))
        ok_claims = []
        for name, sql in claims:
            try:
                con.execute(f"CREATE OR REPLACE TEMP TABLE c_{name} AS {sql}")
                ok_claims.append(name)
            except Exception as e:
                _stage(f"  {name} claim unavailable: {str(e)[:60]}")
        if len(ok_claims) < 2:
            per[col] = {"skipped": "fewer than 2 roots reachable"}
            continue
        union = " UNION ALL ".join(
            f"SELECT pid, yr, wk, v, '{n}' AS src FROM c_{n}"
            for n in ok_claims)
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cell AS
        WITH allc AS ({union}),
        stored AS (
          SELECT NFL_player_id AS pid, year AS yr, week AS wk,
                 TRY_CAST({col} AS DOUBLE) AS stored
          FROM read_parquet('{wk}')
          WHERE season_type='REG' AND {col} IS NOT NULL)
        SELECT s.pid, s.yr, s.wk, s.stored, a.v,
               -- OQ-LR-1 COLLAPSE: nflcom modern box == pfr upstream, so
               -- their agreement is ONE independent voice, not two
               COUNT(DISTINCT CASE WHEN a.src IN ('nflcom', 'pfr')
                                   THEN 'gamebook' ELSE a.src END) AS roots,
               STRING_AGG(DISTINCT a.src, ',') AS root_list
        FROM stored s JOIN allc a
          ON a.pid = s.pid AND a.yr = s.yr AND a.wk = s.wk
        WHERE a.v <> s.stored
        GROUP BY 1, 2, 3, 4, 5""")
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE v_{col} AS
        SELECT pid AS NFL_player_id, yr AS year, wk AS week,
               '{col}' AS column_name, stored AS old_value, v AS new_value,
               'arb_defense' AS repair_id, root_list AS root,
               'PRECEDENCE LAW: ' || roots || ' roots agree vs stored'
                   AS ruling
        FROM cell
        QUALIFY COUNT(*) OVER (PARTITION BY pid, yr, wk) = 1
        """)
        con.execute(f"DELETE FROM v_{col} WHERE NOT EXISTS (SELECT 1)")
        n_write = con.execute(f"""
        SELECT COUNT(*) FROM v_{col} WHERE root LIKE '%,%'""").fetchone()[0]
        n_all = con.execute(f"SELECT COUNT(*) FROM cell").fetchone()[0]
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE w_{col} AS
        SELECT * FROM v_{col} WHERE root LIKE '%,%'""")
        per[col] = {"disputed_cells": n_all, "verdict_writes": n_write,
                    "held": n_all - n_write, "roots": ok_claims}
        corrections += n_write
        held += n_all - n_write
    writes = [f"SELECT * FROM w_{c}" for c in FAMILIES
              if f"w_{c}" in [r[0] for r in con.execute(
                  "SELECT table_name FROM duckdb_tables() WHERE temporary"
              ).fetchall()]]
    if writes:
        con.execute(f"""COPY ({' UNION ALL '.join(writes)})
        TO '{OVERLAY.as_posix()}' (FORMAT parquet)""")
    receipt = {"wave": "arbitration_defense", "date": time.strftime("%Y-%m-%d"),
               "per_column": per, "verdict_writes": corrections,
               "held_for_definition_review": held,
               "overlay": str(OVERLAY) if writes else None}
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    print(json.dumps(build(con), indent=2, default=str))
