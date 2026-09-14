"""
sota_recon/recon_scoring_events.py  --  WS7b: scoring-event bijection at EVENT grain.

pfr_box_scoring holds EVERY scoring play 1920-2025 (133,466 events / 18,154 games) with
per-participant pfr ids. Each event maps 1:1 onto player-game scoring atoms, so this lane
verifies the scoring family BOTH directions -- deeper than any sum comparison:

  event -> cell : per-player event aggregates vs v26 TD/FG/XP/2pt cells (catches
                  attribution errors: right team total, wrong player)
  cell -> event : v26 rows claiming TDs in a covered game with NO event (overcount class)

It is also the only deep witness reaching 1920: pre-1957 scoring atoms get their first
event-grain verdicts, and event-witnessed scorers with no v26 row feed the ancient
scoring completion queue.

Parse is FAIL-CLOSED: 17 known-exotic descriptions (ancient laterals, "interception in
end zone") + anything unrecognized are enumerated as UNPARSED, never guessed. Suffix
clauses "(X kick)" / "(X kick failed)" / "(X run)" / "(X pass from Y)" credit
XP/2pt participants; participants are resolved positionally by matching each linked
name's offset against the trailing parenthetical.

READ-ONLY. Outputs under sota_recon_master/scoring_bijection/.

    python -m scripts.sota_recon.recon_scoring_events
"""
from __future__ import annotations

import argparse
import json
import os
import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import sources as S

OUT_DIR = os.path.join(S.DATA_LAKE, "derived", "validation", "sota_recon_master",
                       "scoring_bijection")
DIFF_CAP = 30_000

# event stat -> v26 column (columns absent from the release are skipped at run time)
STAT_TO_V26 = {
    "pass_td": "passing_tds", "rec_td": "receiving_tds", "rush_td": "rushing_tds",
    "fg_made": "fg_made", "pat_made": "pat_made", "pat_att": "pat_att",
    "int_ret_td": "def_int_ret_td", "punt_ret_td": "punt_return_tds",
    "kick_ret_td": "kickoff_return_tds",
    "pass_2pt": "passing_2pt_conversions", "rush_2pt": "rushing_2pt_conversions",
    "rec_2pt": "receiving_2pt_conversions",
    # FG distance family: every made FG's distance rides the event text
    # ("35 yard field goal") -> per-game long/buckets/yards. Adjudicates the
    # fg_long=ΣFG-distances defect class (game long = MAX, never SUM).
    "fg_long": "fg_long", "fg_yards": "fg_yards",
    "fg_made_0_19": "fg_made_0_19", "fg_made_20_29": "fg_made_20_29",
    "fg_made_30_39": "fg_made_30_39", "fg_made_40_49": "fg_made_40_49",
    "fg_made_50_59": "fg_made_50_59",
}
_FG_DIST = re.compile(r"(\d+)\s+yard field goal")


def _fg_bucket(d: int) -> str | None:
    if d < 20: return "fg_made_0_19"
    if d < 30: return "fg_made_20_29"
    if d < 40: return "fg_made_30_39"
    if d < 50: return "fg_made_40_49"
    if d < 60: return "fg_made_50_59"
    return None  # 60+: v26 weekly has no 60plus bucket; long/yards still carry it

_MAIN = [
    ("fg_made", re.compile(r"\byard field goal\b")),
    ("pass_td", re.compile(r"\bpass from\b")),
    ("rush_td", re.compile(r"\byard (rush|run)\b")),
    ("punt_ret_td", re.compile(r"\bpunt return\b")),
    ("kick_ret_td", re.compile(r"\bkickoff return\b")),
    ("int_ret_td", re.compile(r"\binterception return\b")),
    ("fum_td", re.compile(r"\bfumble (recovery|return)\b")),
    ("safety", re.compile(r"\bsafety\b", re.I)),
    ("blocked_kick_td", re.compile(r"\bblocked (punt|field goal)\b")),
    ("pat_standalone", re.compile(r"\bextra point\b")),
]


def parse_event(desc: str, ids: list[str], texts: list[str]):
    """-> list[(pfr_id, stat)] credits + parse verdict ('ok'|'unparsed'|'no_ids')."""
    credits: list[tuple[str, str]] = []
    m = re.search(r"\(([^()]*)\)\s*$", desc)
    suffix, main = (m.group(1), desc[: m.start()]) if m else ("", desc)
    # split participants into main-clause vs suffix-clause by name offset
    main_ids, suf_ids = [], []
    for pid, name in zip(ids, texts):
        pos = desc.find(name) if name else -1
        (suf_ids if (m and pos >= m.start()) else main_ids).append(pid)

    kind = next((k for k, rx in _MAIN if rx.search(main)), None)
    if kind is None:
        return credits, "unparsed"
    if kind == "pass_td":
        if len(main_ids) >= 2:
            credits += [(main_ids[0], "rec_td"), (main_ids[1], "pass_td")]
        elif len(main_ids) == 1:  # unlinked passer (ancient) -- credit receiver only
            credits.append((main_ids[0], "rec_td"))
    elif kind in ("fg_made", "rush_td", "punt_ret_td", "kick_ret_td",
                  "int_ret_td", "fum_td", "blocked_kick_td"):
        if main_ids:
            credits.append((main_ids[0], kind))
    elif kind == "safety":
        pass  # team points; player credit optional/absent -- never guessed
    elif kind == "pat_standalone":
        if main_ids:
            credits += [(main_ids[0], "pat_made"), (main_ids[0], "pat_att")]

    if suffix and suf_ids:
        if re.search(r"\bkick failed\b", suffix):
            credits.append((suf_ids[-1], "pat_att"))
        elif re.search(r"\bkick\b", suffix):
            credits += [(suf_ids[-1], "pat_made"), (suf_ids[-1], "pat_att")]
        elif re.search(r"\bpass from\b", suffix) and len(suf_ids) >= 2:
            credits += [(suf_ids[0], "rec_2pt"), (suf_ids[1], "pass_2pt")]
        elif re.search(r"\brun\b", suffix) and "failed" not in suffix:
            credits.append((suf_ids[-1], "rush_2pt"))
    return credits, ("ok" if credits or kind == "safety" else "no_ids")


def build_event_aggregates(con) -> dict:
    p = Path(S.registry()["pfr_box_scoring"].path).as_posix()
    rows = con.execute(f"""
        SELECT boxscore_id, description, description_link_ids, description_link_texts
        FROM '{p}' WHERE description IS NOT NULL""").fetchall()
    agg: dict[tuple[str, str], Counter] = defaultdict(Counter)
    longs: dict[tuple[str, str], int] = {}
    verdicts = Counter()
    unparsed = []
    for bx, desc, ids_s, texts_s in rows:
        ids = [x for x in (ids_s or "").split(";") if x]
        texts = [x for x in (texts_s or "").split(";") if x]
        credits, v = parse_event(desc, ids, texts)
        verdicts[v] += 1
        if v == "unparsed" and len(unparsed) < 200:
            unparsed.append({"boxscore_id": bx, "description": desc[:120]})
        for pid, stat in credits:
            agg[(pid, bx)][stat] += 1
            if stat == "fg_made":
                m = _FG_DIST.search(desc)
                if m:
                    d = int(m.group(1))
                    agg[(pid, bx)]["fg_yards"] += d
                    b = _fg_bucket(d)
                    if b:
                        agg[(pid, bx)][b] += 1
                    longs[(pid, bx)] = max(longs.get((pid, bx), 0), d)
    for key, d in longs.items():
        agg[key]["fg_long"] = d  # MAX, not a count -- set once from the running max
    con.execute("""CREATE OR REPLACE TEMP TABLE ev (
        pid VARCHAR, boxscore_id VARCHAR, stat VARCHAR, val DOUBLE)""")
    con.executemany("INSERT INTO ev VALUES (?, ?, ?, ?)",
                    [(pid, bx, stat, float(n))
                     for (pid, bx), c in agg.items() for stat, n in c.items()])
    return {"events_parsed": sum(verdicts.values()), **verdicts,
            "unparsed_sample": unparsed}


def run() -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    os.makedirs(OUT_DIR, exist_ok=True)
    parse_stats = build_event_aggregates(con)

    tg = Path(S.TEAM_GAMES.path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    have = {r[0] for r in con.execute(
        f"DESCRIBE SELECT * FROM '{v26}'").fetchall()}
    mapping = {s: c for s, c in STAT_TO_V26.items() if c in have}
    stats = list(mapping)

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE evp AS
        SELECT e.pid, e.boxscore_id, g.year, g.week, g.season_type,
               CAST(g.game_date AS DATE) AS gd,
               {", ".join(f"MAX(CASE WHEN stat='{s}' THEN val END) AS e_{s}" for s in stats)}
        FROM ev e
        JOIN (SELECT DISTINCT boxscore_id, year, week, season_type, game_date
              FROM '{tg}') g USING (boxscore_id)
        GROUP BY 1,2,3,4,5,6""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE v AS
        SELECT bio.pfr_id, t.player_week, t.year, t.week, t.season_type,
               CAST(t.game_date AS DATE) AS gd,
               {", ".join(f"t.{c} AS v_{s}" for s, c in mapping.items())}
        FROM '{v26}' t JOIN '{bio}' bio USING (NFL_player_id)
        WHERE bio.pfr_id IS NOT NULL""")
    con.execute("""
        CREATE OR REPLACE TEMP TABLE j AS
        SELECT * FROM (
          SELECT e.*, v.player_week
               , """ + ", ".join(f"v.v_{s}" for s in stats) + """
          FROM (SELECT * FROM evp QUALIFY COUNT(*) OVER (PARTITION BY pid, gd) = 1) e
          JOIN (SELECT * FROM v WHERE gd IS NOT NULL
                QUALIFY COUNT(*) OVER (PARTITION BY pfr_id, gd) = 1) v
            ON v.pfr_id = e.pid AND v.gd = e.gd
          UNION ALL
          SELECT e.*, v.player_week
               , """ + ", ".join(f"v.v_{s}" for s in stats) + """
          FROM (SELECT * FROM evp
                QUALIFY COUNT(*) OVER (PARTITION BY pid, year, week, season_type) = 1) e
          JOIN (SELECT * FROM v WHERE gd IS NULL
                QUALIFY COUNT(*) OVER (PARTITION BY pfr_id, year, week, season_type) = 1) v
            ON v.pfr_id = e.pid AND v.year = e.year AND v.week = e.week
           AND v.season_type = e.season_type)""")

    per_stat, diffs = {}, []
    for s in stats:
        n_eq, n_diff, n_fill = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE ABS(e_{s} - COALESCE(v_{s}, 0)) <= 0.5
                                      AND v_{s} IS NOT NULL),
                   COUNT(*) FILTER (WHERE v_{s} IS NOT NULL
                                      AND ABS(e_{s} - v_{s}) > 0.5),
                   COUNT(*) FILTER (WHERE v_{s} IS NULL AND e_{s} > 0)
            FROM j WHERE e_{s} IS NOT NULL""").fetchone()
        per_stat[s] = dict(witnessed_equal=n_eq, witnessed_diff=n_diff,
                           fill_signal=n_fill)
        if n_diff or n_fill:
            for r in con.execute(f"""
                SELECT pid, player_week, boxscore_id, year, week, season_type, e_{s}, v_{s}
                FROM j WHERE e_{s} IS NOT NULL
                  AND ((v_{s} IS NOT NULL AND ABS(e_{s} - v_{s}) > 0.5)
                       OR (v_{s} IS NULL AND e_{s} > 0))
                LIMIT {max(0, DIFF_CAP - len(diffs))}""").fetchall():
                diffs.append(dict(stat=s, pfr_id=r[0], player_week=r[1],
                                  boxscore_id=r[2], year=r[3], week=r[4],
                                  season_type=r[5], events=r[6], v26=r[7]))

    # the fg_long=ΣFG-distances class, adjudicated: v26 game long exceeds the event
    # max AND equals the event distance SUM on a multi-FG game -> corrected value is
    # the event max, with the boxscore citation (small-arms wave input)
    fg_fix = []
    if "fg_long" in stats and "fg_yards" in mapping:
        fg_fix = con.execute("""
            SELECT pid, player_week, boxscore_id, year, week, season_type,
                   v_fg_long AS v26_fg_long, e_fg_long AS corrected_fg_long,
                   e_fg_yards, e_fg_made
            FROM j
            WHERE e_fg_long IS NOT NULL AND v_fg_long IS NOT NULL
              AND e_fg_made >= 2 AND v_fg_long - e_fg_long > 0.5
              AND ABS(v_fg_long - e_fg_yards) <= 0.5
            ORDER BY year""").fetchall()

    # cell -> event direction: v26 rows claiming a scoring stat the log never recorded
    # for that PLAYER anywhere in that week (a player can only score in the game he
    # played, so no per-game pinning is needed). Guarded to weeks with >= 1 covered
    # game so scrape gaps don't false-positive.
    td_stats = ["pass_td", "rec_td", "rush_td", "fg_made",
                "int_ret_td", "punt_ret_td", "kick_ret_td"]
    over = con.execute(f"""
        WITH cov_wk AS (SELECT DISTINCT year, week, season_type FROM evp),
             cov_gd AS (SELECT DISTINCT gd FROM evp WHERE gd IS NOT NULL)
        SELECT COUNT(*) FROM v
        WHERE ({" + ".join(f"COALESCE(v_{s},0)" for s in td_stats)}) > 0
          AND ((v.gd IS NOT NULL AND v.gd IN (SELECT gd FROM cov_gd)
                AND NOT EXISTS (SELECT 1 FROM evp e WHERE e.pid = v.pfr_id
                                AND e.gd = v.gd))
            OR (v.gd IS NULL
                AND EXISTS (SELECT 1 FROM cov_wk c WHERE c.year = v.year
                            AND c.week = v.week AND c.season_type = v.season_type)
                AND NOT EXISTS (SELECT 1 FROM evp e WHERE e.pid = v.pfr_id
                                AND e.year = v.year AND e.week = v.week
                                AND e.season_type = v.season_type)))""").fetchone()[0]

    # completion: event scorers with NO v26 row at all, by era
    missing = con.execute("""
        SELECT year//10*10, COUNT(*) FROM evp e
        WHERE NOT EXISTS (SELECT 1 FROM j WHERE j.pid = e.pid
                          AND j.boxscore_id = e.boxscore_id)
        GROUP BY 1 ORDER BY 1""").fetchall()
    missing_rows = con.execute("""
        SELECT pid, boxscore_id, year, week, season_type FROM evp e
        WHERE NOT EXISTS (SELECT 1 FROM j WHERE j.pid = e.pid
                          AND j.boxscore_id = e.boxscore_id)""").fetchall()

    import csv
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    if fg_fix:
        with open(os.path.join(OUT_DIR, f"fg_long_sum_class_fix_queue_{stamp}.csv"),
                  "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["pfr_id", "player_week", "boxscore_id", "year", "week",
                        "season_type", "v26_fg_long", "corrected_fg_long",
                        "event_fg_yards", "event_fg_made"])
            w.writerows(fg_fix)
    if diffs:
        with open(os.path.join(OUT_DIR, f"scoring_event_diffs_{stamp}.csv"), "w",
                  newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(diffs[0]))
            w.writeheader(); w.writerows(diffs)
    with open(os.path.join(OUT_DIR, f"event_scorers_missing_rows_{stamp}.csv"), "w",
              newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["pfr_id", "boxscore_id", "year", "week", "season_type"])
        w.writerows(missing_rows)
    summary = {"generated_at_utc": datetime.now(timezone.utc).isoformat(),
               "parse": {k: v for k, v in parse_stats.items() if k != "unparsed_sample"},
               "per_stat": per_stat, "fg_long_sum_class_rows": len(fg_fix),
               "overcount_rows_td_no_event": over,
               "event_scorers_missing_v26_row": len(missing_rows),
               "missing_by_decade": [(int(d), n) for d, n in missing],
               "unparsed_sample": parse_stats["unparsed_sample"]}
    with open(os.path.join(OUT_DIR, "SCORING_BIJECTION_SUMMARY.json"), "w",
              encoding="utf-8") as f:
        json.dump(summary, f, indent=1)
    con.close()
    return summary


if __name__ == "__main__":
    argparse.ArgumentParser().parse_args()
    s = run()
    print("parse:", s["parse"])
    for st, v in s["per_stat"].items():
        tot = v["witnessed_equal"] + v["witnessed_diff"]
        pct = v["witnessed_equal"] / tot if tot else 0
        print(f"  {st:14s} equal={v['witnessed_equal']:>7,} diff={v['witnessed_diff']:>6,} "
              f"({pct:.2%})  fill={v['fill_signal']:>5,}")
    print("overcount (TD cells, no event):", s["overcount_rows_td_no_event"])
    print("event scorers w/o v26 row:", s["event_scorers_missing_v26_row"],
          "by decade:", s["missing_by_decade"])
