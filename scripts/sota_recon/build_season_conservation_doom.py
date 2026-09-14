"""SEASON-CONSERVATION PURGE (born from Joe's Brown/Sayers spot-check,
2026-08-03): a weekly ZERO that contradicts a witnessed season total is a
fabrication, and the witness that "corroborated" it was zero-rendering the
same cells (mutual fabrication -- nflcom career agreed 1.0 on 1960s
fumbles_lost because BOTH sides manufacture zeros).

THE LAW, per (player, season, column) in the pre-pbp era (< 1978):
    W = witnessed season total (licensed season lane, pfr root preferred,
        NONZERO -- a zero witness proves nothing here)
    S = sum of the player's NONZERO weekly cells
    Z = count of zero weekly cells
    S == W  -> every event is placed; the zeros are corroborated; KEEP
    S <  W  -> W-S events exist but cannot be placed; the zero cells are
               unverifiable -> DOOM (NULL them); nonzero cells stay (they
               are recorded events)
    S >  W  -> contradiction -> ESCALATE, touch nothing

Output: season_conservation_doom.parquet (pid, year, column) rows whose
ZERO cells the applier NULLs, + receipt with the escalations.
"""
from __future__ import annotations

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

import scripts.sota_recon.witness_map as W
from scripts.sota_recon import sources as S
from scripts.sota_recon.vouch_2024 import lane_sql

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
DOOM = LAKE / "season_conservation_doom.parquet"
RECEIPT = LAKE / "season_conservation_receipt.json"
COLUMNS = ("fumbles", "fumbles_lost")
ERA_END = 1978   # pbp era onward has weekly witnesses; different jurisdiction


def season_lane(col: str):
    """Best licensed season-grain lane for the column, pfr root first."""
    cands = []
    for sp in W.WITNESS_MAP:
        if sp.v26_col != col:
            continue
        if ((sp.season_type or "REG").upper() != "REG"
                or "post" in (sp.source_table or "").lower()):
            continue
        sql, keys = lane_sql(sp)
        if sql and keys == ("pfr_id", "yr"):
            rank = 2 if sp.source_key.startswith("pfr") else 1
            cands.append((rank, sp.source_key, sql))
    cands.sort(reverse=True)
    return cands[0] if cands else None


def main() -> None:
    con = duckdb.connect()
    con.execute("SET memory_limit='1500MB'")
    con.execute("SET threads=2")
    con.execute(f"SET temp_directory='{(LAKE / 'duck_spill').as_posix()}'")
    wk = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    con.execute(f"""CREATE OR REPLACE TEMP VIEW plane AS
    SELECT * FROM read_parquet('{wk}') WHERE season_type = 'REG'""")
    con.execute(f"""CREATE OR REPLACE TEMP VIEW ids AS
    SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}')
    WHERE pfr_id IS NOT NULL""")

    frames, stats = [], {}
    for col in COLUMNS:
        lane = season_lane(col)
        if lane is None:
            stats[col] = "NO SEASON LANE"
            continue
        _, src, wsql = lane
        con.execute(f"""
        CREATE OR REPLACE TEMP TABLE cons_{col} AS
        WITH w AS ({wsql}),
        weekly AS (
          SELECT b.pfr_id, t.NFL_player_id, CAST(t.year AS INT) AS yr,
                 SUM(CASE WHEN TRY_CAST(t.{col} AS DOUBLE) > 0
                          THEN TRY_CAST(t.{col} AS DOUBLE) ELSE 0 END) AS s,
                 COUNT(*) FILTER (
                   WHERE TRY_CAST(t.{col} AS DOUBLE) = 0) AS z
          FROM plane t JOIN ids b USING (NFL_player_id)
          WHERE t.{col} IS NOT NULL AND CAST(t.year AS INT) < {ERA_END}
          GROUP BY 1, 2, 3)
        SELECT weekly.*, w.val AS witness_total
        FROM weekly JOIN w ON w.pfr_id = weekly.pfr_id AND w.yr = weekly.yr
        WHERE w.val > 0""")
        doomed, kept, esc = con.execute(f"""
        SELECT COUNT(*) FILTER (WHERE s < witness_total AND z > 0),
               COUNT(*) FILTER (WHERE s = witness_total),
               COUNT(*) FILTER (WHERE s > witness_total)
        FROM cons_{col}""").fetchone()
        stats[col] = {"season_lane": src,
                      "player_seasons_doomed": doomed,
                      "corroborated_kept": kept, "escalated": esc}
        frames.append(f"""
        SELECT NFL_player_id, yr AS year, '{col}' AS column_name,
               s AS placed_sum, witness_total, z AS zero_cells
        FROM cons_{col} WHERE s < witness_total AND z > 0""")
        print(f"{col}: doom {doomed} player-seasons "
              f"(kept {kept} corroborated, {esc} escalated)", flush=True)

    assert frames, "no doom frames built"
    con.execute(f"""COPY ({' UNION ALL '.join(frames)})
    TO '{DOOM.as_posix()}' (FORMAT parquet)""")
    n = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{DOOM.as_posix()}')").fetchone()[0]
    RECEIPT.write_text(json.dumps(
        {"date": time.strftime("%Y-%m-%d %H:%M"), "era": f"<{ERA_END}",
         "columns": stats, "doom_rows": n,
         "law": ("weekly zeros contradicting a witnessed nonzero season "
                 "total are unverifiable -> NULL; nonzero weekly cells are "
                 "recorded events and stay; S>W escalates")},
        indent=1), encoding="utf-8")
    print(f"doom list: {n} (player, season, column) rows", flush=True)


if __name__ == "__main__":
    main()
