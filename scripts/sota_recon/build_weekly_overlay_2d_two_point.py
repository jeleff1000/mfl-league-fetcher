"""WEEKLY MUTATION TRAIN, car 2d: 2pt attribution -- TWO independent witnesses
(catalog scoring + pbp) agree against the stored values at ~75%
(equation_scoring2 + catalog_scoring_pbp receipts). Released by repair_signoff.

Derivation: two_point_conv_result = 'success' plays, 1994+ (the rule's birth
year), attributed passer/rusher/receiver. DOOM KEY: player-week cells where
stored differs from the pbp derivation -- the conviction already showed the
witnesses agreeing with each other, so the write follows the witness pair.
NULL-vs-0 convention: a player with no successful 2pt that week and a stored
value gets the derivation (including 0 only when the stored was nonzero --
we never manufacture zeros onto empty cells).
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import duckdb

from scripts.sota_recon import sources as S

LAKE = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
OVERLAY = LAKE / "weekly_overlay_2d.parquet"
RECEIPT = LAKE / "repair_2d_receipt.json"

LANES = [
    ("passing_2pt_conversions", "passer_player_id"),
    ("rushing_2pt_conversions", "rusher_player_id"),
    ("receiving_2pt_conversions", "receiver_player_id"),
]


def build(con: duckdb.DuckDBPyConnection) -> dict:
    wk = Path(S.latest_v26()).as_posix()
    pb = Path(S.registry()["pbp_merged_1978_2025"].path).as_posix()
    parts = []
    for col, actor in LANES:
        parts.append(f"""
        SELECT t.NFL_player_id, t.year, t.week, '{col}' AS column_name,
               TRY_CAST(t.{col} AS DOUBLE) AS old_value,
               COALESCE(w.v, 0) AS new_value, 'repair_2d' AS repair_id,
               'pbp_merged_1978_2025' AS root,
               'two-witness 2pt conviction (catalog+pbp agree ~75% vs stored) '
               || '+ repair_signoff' AS ruling
        FROM read_parquet('{wk}') t
        LEFT JOIN (
          SELECT r.{actor} AS pid, TRY_CAST(r.season AS INT) AS yr,
                 TRY_CAST(r.week AS INT) AS wkn, COUNT(*)::DOUBLE AS v
          FROM read_parquet('{pb}') r
          WHERE r.two_point_conv_result = 'success' AND r.season_type = 'REG'
            AND r.{actor} IS NOT NULL
          GROUP BY 1, 2, 3
        ) w ON w.pid = t.NFL_player_id AND w.yr = t.year AND w.wkn = t.week
        WHERE t.season_type = 'REG' AND t.year >= 1994
          AND ((t.{col} IS NOT NULL
                AND TRY_CAST(t.{col} AS DOUBLE) <> COALESCE(w.v, 0))
               OR (t.{col} IS NULL AND w.v > 0))""")
    union = " UNION ALL ".join(parts)
    con.execute(f"COPY ({union}) TO '{OVERLAY.as_posix()}' (FORMAT parquet)")
    stats = con.execute(f"""
    SELECT column_name, COUNT(*),
           COUNT(*) FILTER (WHERE new_value > old_value) AS raised,
           COUNT(*) FILTER (WHERE new_value < old_value) AS lowered,
           MAX(new_value)
    FROM read_parquet('{OVERLAY.as_posix()}') GROUP BY 1""").fetchall()
    per = {r[0]: {"cells": r[1], "raised": r[2], "lowered": r[3],
                  "max_new": r[4]} for r in stats}
    # sanity: no player-week holds more than 4 successful 2pt of one kind
    mx = max((v["max_new"] or 0) for v in per.values()) if per else 0
    assert mx <= 4, f"sanity gate REFUSED: max {mx} 2pt in one week"
    receipt = {"wave": "repair_2d_two_point", "date": "2026-08-02",
               "per_column": per, "overlay": str(OVERLAY),
               "scope": "1994+ REG; corrections on nonnull cells + POSITIVE credits onto NULL cells (the missing-attribution class -- zeros are never written onto empty cells)"}
    RECEIPT.write_text(json.dumps(receipt, indent=2, default=str),
                       encoding="utf-8")
    return receipt


if __name__ == "__main__":
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    print(json.dumps(build(con), indent=2, default=str))
