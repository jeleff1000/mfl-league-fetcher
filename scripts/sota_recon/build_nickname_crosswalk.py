"""Self-building nickname -> team_code crosswalk from the scoring log's own scores.

pfr boxscore surfaces name teams by NICKNAME ('Chiefs') while the game catalog keys
team_code; expected_points and every team-credited scoring extraction were blocked on
the translation. Nothing is hand-typed here: on each scoring row exactly one of
(vis_team_score, home_team_score) increases, so the scoring row's nickname belongs to
that SIDE, and the catalog names the side's team_code. Majority vote per
(nickname, season) absorbs OCR/parse noise; the receipt keeps the vote counts so a
contested cell is visible instead of silently resolved.

    python -m scripts.sota_recon.build_nickname_crosswalk

Output: nickname_team_code_crosswalk.json beside the other receipts.
"""
from __future__ import annotations

import json
from pathlib import Path

import duckdb

from . import sources as S

OUT = Path(r"D:/league-history-data/nfl/derived/validation/sota_recon_master")
XWALK = OUT / "nickname_team_code_crosswalk.json"


def main() -> int:
    con = duckdb.connect()
    con.execute("SET memory_limit='3GB'")
    sc = Path(S.registry()["pfr_box_scoring"].path).as_posix()
    tg = Path(S.TEAM_GAMES.path).as_posix()
    rows = con.execute(f"""
        WITH scored AS (
          SELECT boxscore_id, team,
                 TRY_CAST(season AS INT) AS season,
                 TRY_CAST(vis_team_score AS INT) AS vs,
                 TRY_CAST(home_team_score AS INT) AS hs,
                 LAG(TRY_CAST(vis_team_score AS INT), 1, 0) OVER w AS pvs,
                 LAG(TRY_CAST(home_team_score AS INT), 1, 0) OVER w AS phs
          FROM read_parquet('{sc}')
          WHERE team IS NOT NULL AND TRIM(team) <> ''
          WINDOW w AS (PARTITION BY boxscore_id ORDER BY row_index_in_table)
        ), sided AS (
          SELECT boxscore_id, team, season,
                 CASE WHEN hs > phs AND vs = pvs THEN 'home'
                      WHEN vs > pvs AND hs = phs THEN 'vis' END AS side
          FROM scored
        ), named AS (
          SELECT s.team AS nickname, s.season,
                 CASE WHEN s.side = 'home' THEN g.team_code
                      ELSE g.opponent_code END AS team_code
          FROM sided s
          JOIN (SELECT boxscore_id, team_code, opponent_code
                FROM read_parquet('{tg}') WHERE is_home) g USING (boxscore_id)
          WHERE s.side IS NOT NULL
        )
        SELECT nickname, season, team_code, COUNT(*) AS votes
        FROM named GROUP BY 1, 2, 3
    """).fetchall()

    by_key: dict[tuple, list] = {}
    for nickname, season, code, votes in rows:
        by_key.setdefault((nickname, int(season)), []).append((code, int(votes)))
    entries, contested = [], []
    for (nickname, season), cands in sorted(by_key.items()):
        cands.sort(key=lambda cv: -cv[1])
        total = sum(v for _, v in cands)
        winner, wv = cands[0]
        entry = {"nickname": nickname, "season": season, "team_code": winner,
                 "votes": wv, "total_votes": total,
                 "confidence": round(wv / total, 4)}
        entries.append(entry)
        if wv / total < 0.95:
            contested.append({**entry, "runners_up": cands[1:4]})

    XWALK.write_text(json.dumps({
        "artifact": "nickname_team_code_crosswalk",
        "built_from": "pfr_box_scoring running scores x game catalog sides",
        "entries": entries,
        "contested": contested,
        "notes": [
            "A cell below 95% majority is CONTESTED and listed rather than silently resolved.",
            "Season-scoped on purpose: nicknames move cities and codes drift (OAK->LV class).",
        ],
    }, indent=1), encoding="utf-8")
    print(json.dumps({"crosswalk": str(XWALK), "cells": len(entries),
                      "contested": len(contested),
                      "distinct_nicknames": len({e['nickname'] for e in entries})}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
