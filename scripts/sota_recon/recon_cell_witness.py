"""
sota_recon/recon_cell_witness.py  --  WS4c witness arm: per-CELL verdicts at GAME grain.

The witness map vouches at season-sum grain; this lane drops the same LICENSED mappings
to the subject's own grain (player-game) and gives every reachable cell a verdict.
Matches are counted, mismatches are listed (the oracle-lane pattern). POST rows are
covered for the first time (the season-grain map validates REG only).

Join strategy (week vocabularies drift pre-1978; game_date is NULL for ~all 1999+ rows):
  date arm : (pfr_id, game_date) where the v26 row has a date (~92% pre-1998)
  week arm : (pfr_id, year, week, season_type) where the v26 date is NULL (all modern),
             box lines aggregated per player-week ONLY when the player has exactly one
             line that week -- multi-line weeks are the doubleheader class (excluded +
             counted, already queued; never guessed)

Verdicts per (source, column) on joined rows:
  witnessed_equal     |witness - v26| <= tol (yards 1.5, counts 0.5)
  witnessed_diff      beyond tolerance -> enumerated to CSV (capped)
  fill_signal         witness NONZERO, v26 cell NULL -> completion queue input
  zero_vs_null        witness 0 (incl. blank-zero), v26 NULL -- informational, NOT a
                      defect (appearance doctrine: NULL = not-applicable, never fake 0)
Row-level signals:
  missing_row         witness line with nonzero activity, pid IS in bio, but NO v26 row
                      at that (pfr_id, date/week) -> completion queue (like wave44)
  identity_gap        witness line whose pid is not in player_bio at all -> identity queue
  unwitnessed         v26 rows this source has no line for (coverage, not a defect)

READ-ONLY. Outputs under sota_recon_master/cell_witness/:
  CELL_WITNESS_SUMMARY.json, witness_cell_diffs_<date>.csv, missing_rows_<date>.csv

    python -m scripts.sota_recon.recon_cell_witness [--source KEY]
"""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from . import sources as S
from . import witness_map as WM

OUT_DIR = os.path.join(S.DATA_LAKE, "derived", "validation", "sota_recon_master",
                       "cell_witness")
DIFF_CAP_PER_SOURCE = 20_000

# activity columns per box source used for the missing-row signal (a line must show
# NONZERO activity in at least one of these to claim a v26 row should exist)
_ACTIVITY = {
    "pfr_player_offense_box": ["pass_att", "rush_att", "rec", "targets", "pass_cmp"],
    "pfr_player_defense_box": ["tackles_combined", "tackles_solo", "sacks", "def_int",
                               "pass_defended"],
    "pfr_box_kicking": ["xpa", "fga", "punt"],
    "pfr_box_returns": ["kick_ret", "punt_ret"],
}


def _tol(col: str) -> float:
    return 1.5 if ("yard" in col or col.endswith("_yds")) else 0.5


def _licensed_box_specs() -> dict[str, list[WM.MapSpec]]:
    """source_key -> licensed box-shape specs. sum AND max both compare directly at
    game grain (a game long IS the cell value; only season rollup differs)."""
    lic = WM.licensed()
    out: dict[str, list[WM.MapSpec]] = {}
    for m in WM.WITNESS_MAP:
        if m.shape == "box" and m.agg in ("sum", "max") \
                and (m.source_key, m.v26_col) in lic:
            out.setdefault(m.source_key, []).append(m)
    return out


def _licensed_flat_specs() -> list[WM.MapSpec]:
    lic = WM.licensed()
    return [m for m in WM.WITNESS_MAP
            if m.shape == "flat" and m.agg == "sum"
            and m.source_key == "pbp_player_week_rollup"
            and (m.source_key, m.v26_col) in lic]


def _q(key: str) -> str:
    return Path(S.registry()[key].path).as_posix()


def _tracked_years(con, source_key: str, cols: list[str]) -> dict[str, set[int]]:
    """Per column: years where the column has at least one parseable value (the same
    era guard the season-grain map uses -- blank-everywhere = not tracked = abstain)."""
    tg = Path(S.TEAM_GAMES.path).as_posix()
    exprs = ", ".join(f"COUNT(TRY_CAST(s.{c} AS DOUBLE)) AS {c}" for c in cols)
    rows = con.execute(f"""
        SELECT g.year, {exprs}
        FROM '{_q(source_key)}' s
        JOIN (SELECT DISTINCT boxscore_id, year FROM '{tg}') g USING (boxscore_id)
        GROUP BY 1""").fetchall()
    names = [d[0] for d in con.description][1:]
    out: dict[str, set[int]] = {c: set() for c in cols}
    for row in rows:
        yr = int(row[0])
        for c, v in zip(names, row[1:]):
            if v and v > 0:
                out[c].add(yr)
    return out


def _line_expr(spec: WM.MapSpec, tracked: dict[str, set[int]]) -> str:
    """Per-line value expression honoring blank_zero + era guard (NULL outside era)."""
    base = f"TRY_CAST(s.{spec.source_col} AS DOUBLE)"
    if not spec.blank_zero:
        return base
    yrs = sorted(tracked.get(spec.source_col, set()))
    inlist = ", ".join(str(y) for y in yrs) or "NULL"
    return f"CASE WHEN g.year IN ({inlist}) THEN COALESCE({base}, 0) ELSE {base} END"


def _box_source(con, source_key: str, specs: list[WM.MapSpec], stamp: str) -> dict:
    tg = Path(S.TEAM_GAMES.path).as_posix()
    v26 = Path(S.latest_v26()).as_posix()
    bio = Path(S.PLAYER_BIO.path).as_posix()
    tracked = _tracked_years(con, source_key, [m.source_col for m in specs])
    wcols = ",\n             ".join(
        f"{_line_expr(m, tracked)} AS w_{m.v26_col}" for m in specs)
    vcols = ", ".join(f"t.{m.v26_col} AS v_{m.v26_col}" for m in specs)
    # sources without an activity registration (advanced tables) skip the
    # missing-row signal -- their rows never imply a v26 row should exist
    act_cols = _ACTIVITY.get(source_key)
    act = (" + ".join(f"COALESCE(TRY_CAST(s.{c} AS DOUBLE), 0)" for c in act_cols)
           if act_cols else "0")

    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE w AS
        SELECT regexp_extract(s.player_link_ids, '^([^,]+)', 1) AS pid,
             s.boxscore_id, g.year, g.week, g.season_type,
             CAST(g.game_date AS DATE) AS gd,
             ({act}) > 0 AS has_activity,
             {wcols}
        FROM '{_q(source_key)}' s
        JOIN (SELECT DISTINCT boxscore_id, year, week, season_type, game_date
              FROM '{tg}') g USING (boxscore_id)
        WHERE s.player_link_ids IS NOT NULL""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE v AS
        SELECT bio.pfr_id, t.player_week, t.year, t.week, t.season_type,
               CAST(t.game_date AS DATE) AS gd, {vcols}
        FROM '{v26}' t JOIN '{bio}' bio USING (NFL_player_id)
        WHERE bio.pfr_id IS NOT NULL""")

    # date arm: unambiguous (pid, date) pairs on both sides
    con.execute("""
        CREATE OR REPLACE TEMP TABLE j_date AS
        SELECT w.*, v.player_week, v.pfr_id
             , """ + ", ".join(f"v.v_{m.v26_col}" for m in specs) + """
        FROM (SELECT * FROM w QUALIFY COUNT(*) OVER (PARTITION BY pid, gd) = 1) w
        JOIN (SELECT * FROM v WHERE gd IS NOT NULL
              QUALIFY COUNT(*) OVER (PARTITION BY pfr_id, gd) = 1) v
          ON v.pfr_id = w.pid AND v.gd = w.gd""")
    # week arm: dateless v26 rows; single-line witness weeks only
    con.execute("""
        CREATE OR REPLACE TEMP TABLE j_week AS
        SELECT w.*, v.player_week, v.pfr_id
             , """ + ", ".join(f"v.v_{m.v26_col}" for m in specs) + """
        FROM (SELECT * FROM w
              QUALIFY COUNT(*) OVER (PARTITION BY pid, year, week, season_type) = 1) w
        JOIN (SELECT * FROM v WHERE gd IS NULL
              QUALIFY COUNT(*) OVER (PARTITION BY pfr_id, year, week, season_type) = 1) v
          ON v.pfr_id = w.pid AND v.year = w.year AND v.week = w.week
         AND v.season_type = w.season_type""")
    con.execute("CREATE OR REPLACE TEMP TABLE j AS "
                "SELECT * FROM j_date UNION ALL SELECT * FROM j_week")

    per_col = {}
    diffs = []
    for m in specs:
        c, tol = m.v26_col, _tol(m.v26_col)
        n_eq, n_diff, n_fill, n_zn, n_joined = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE w_{c} IS NOT NULL AND v_{c} IS NOT NULL
                                      AND ABS(w_{c} - v_{c}) <= {tol}),
                   COUNT(*) FILTER (WHERE w_{c} IS NOT NULL AND v_{c} IS NOT NULL
                                      AND ABS(w_{c} - v_{c}) > {tol}),
                   COUNT(*) FILTER (WHERE w_{c} IS NOT NULL AND w_{c} != 0
                                      AND v_{c} IS NULL),
                   COUNT(*) FILTER (WHERE w_{c} = 0 AND v_{c} IS NULL),
                   COUNT(*)
            FROM j""").fetchone()
        per_col[c] = dict(witnessed_equal=n_eq, witnessed_diff=n_diff,
                          fill_signal=n_fill, zero_vs_null=n_zn, joined=n_joined)
        if n_diff or n_fill:
            for r in con.execute(f"""
                SELECT pfr_id, player_week, year, week, season_type, boxscore_id,
                       w_{c}, v_{c}
                FROM j WHERE (w_{c} IS NOT NULL AND v_{c} IS NOT NULL
                              AND ABS(w_{c} - v_{c}) > {tol})
                   OR (w_{c} IS NOT NULL AND w_{c} != 0 AND v_{c} IS NULL)
                ORDER BY ABS(COALESCE(w_{c},0) - COALESCE(v_{c},0)) DESC
                LIMIT {max(0, DIFF_CAP_PER_SOURCE - len(diffs))}""").fetchall():
                diffs.append(dict(source=source_key, column=c, pfr_id=r[0],
                                  player_week=r[1], year=r[2], week=r[3],
                                  season_type=r[4], boxscore_id=r[5],
                                  witness=r[6], v26=r[7],
                                  kind="fill_signal" if r[7] is None else "diff"))

    # row-level signals
    n_missing, n_identity, n_twin_w, n_unwit = con.execute(f"""
        SELECT
          (SELECT COUNT(*) FROM w
           WHERE has_activity AND pid IN (SELECT pfr_id FROM '{bio}' WHERE pfr_id IS NOT NULL)
             AND NOT EXISTS (SELECT 1 FROM j WHERE j.pid = w.pid
                             AND j.boxscore_id = w.boxscore_id)
             AND NOT EXISTS (SELECT 1 FROM v WHERE v.pfr_id = w.pid AND v.year = w.year
                             AND v.week = w.week AND v.season_type = w.season_type)),
          (SELECT COUNT(*) FROM w WHERE has_activity
             AND pid NOT IN (SELECT pfr_id FROM '{bio}' WHERE pfr_id IS NOT NULL)),
          (SELECT COUNT(*) FROM
             (SELECT pid, year, week, season_type FROM w
              GROUP BY 1,2,3,4 HAVING COUNT(*) > 1)),
          (SELECT COUNT(*) FROM v WHERE NOT EXISTS
             (SELECT 1 FROM j WHERE j.player_week = v.player_week))
        """).fetchone()
    missing_rows = con.execute(f"""
        SELECT pid, boxscore_id, year, week, season_type FROM w
        WHERE has_activity AND pid IN (SELECT pfr_id FROM '{bio}' WHERE pfr_id IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM j WHERE j.pid = w.pid
                          AND j.boxscore_id = w.boxscore_id)
          AND NOT EXISTS (SELECT 1 FROM v WHERE v.pfr_id = w.pid AND v.year = w.year
                          AND v.week = w.week AND v.season_type = w.season_type)""").fetchall()
    return dict(per_col=per_col, diffs=diffs,
                missing_rows=[dict(source=source_key, pfr_id=r[0], boxscore_id=r[1],
                                   year=r[2], week=r[3], season_type=r[4])
                              for r in missing_rows],
                row_signals=dict(missing_row=n_missing, identity_gap=n_identity,
                                 multi_line_weeks_excluded=n_twin_w,
                                 unwitnessed_v26_rows=n_unwit))


def _flat_rollup(con, specs: list[WM.MapSpec]) -> dict:
    """pbp rollup is already week grain: join on player_week directly (REG + POST)."""
    v26 = Path(S.latest_v26()).as_posix()
    r = Path(S.registry()["pbp_player_week_rollup"].path).as_posix()
    wcols = ", ".join(f"r.{m.source_col} AS w_{m.v26_col}" for m in specs)
    vcols = ", ".join(f"t.{m.v26_col} AS v_{m.v26_col}" for m in specs)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE j AS
        SELECT r.player_week, r.year, r.week, r.season_type, {wcols}, {vcols}
        FROM '{r}' r JOIN '{v26}' t USING (player_week)""")
    per_col, diffs = {}, []
    for m in specs:
        c, tol = m.v26_col, _tol(m.v26_col)
        n_eq, n_diff, n_fill, n_zn, n_joined = con.execute(f"""
            SELECT COUNT(*) FILTER (WHERE w_{c} IS NOT NULL AND v_{c} IS NOT NULL
                                      AND ABS(w_{c} - v_{c}) <= {tol}),
                   COUNT(*) FILTER (WHERE w_{c} IS NOT NULL AND v_{c} IS NOT NULL
                                      AND ABS(w_{c} - v_{c}) > {tol}),
                   COUNT(*) FILTER (WHERE w_{c} IS NOT NULL AND w_{c} != 0
                                      AND v_{c} IS NULL),
                   COUNT(*) FILTER (WHERE w_{c} = 0 AND v_{c} IS NULL),
                   COUNT(*)
            FROM j""").fetchone()
        per_col[c] = dict(witnessed_equal=n_eq, witnessed_diff=n_diff,
                          fill_signal=n_fill, zero_vs_null=n_zn, joined=n_joined)
        if n_diff:
            for r_ in con.execute(f"""
                SELECT player_week, year, week, season_type, w_{c}, v_{c}
                FROM j WHERE w_{c} IS NOT NULL AND v_{c} IS NOT NULL
                  AND ABS(w_{c} - v_{c}) > {tol}
                ORDER BY ABS(w_{c} - v_{c}) DESC
                LIMIT {max(0, DIFF_CAP_PER_SOURCE - len(diffs))}""").fetchall():
                diffs.append(dict(source="pbp_player_week_rollup", column=c,
                                  pfr_id=None, player_week=r_[0], year=r_[1],
                                  week=r_[2], season_type=r_[3], boxscore_id=None,
                                  witness=r_[4], v26=r_[5], kind="diff"))
    n_missing = con.execute(f"""
        SELECT COUNT(*) FROM '{r}' r WHERE r.season_type IN ('REG','POST')
          AND NOT EXISTS (SELECT 1 FROM '{v26}' t
                          WHERE t.player_week = r.player_week)""").fetchone()[0]
    return dict(per_col=per_col, diffs=diffs, missing_rows=[],
                row_signals=dict(missing_row=n_missing, identity_gap=0,
                                 multi_line_weeks_excluded=0, unwitnessed_v26_rows=None))


def run(only_source: str | None = None) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    os.makedirs(OUT_DIR, exist_ok=True)
    summary, all_diffs, all_missing = {}, [], []
    for source_key, specs in _licensed_box_specs().items():
        if only_source and source_key != only_source:
            continue
        res = _box_source(con, source_key, specs, stamp)
        summary[source_key] = {"per_col": res["per_col"], **res["row_signals"]}
        all_diffs += res["diffs"]
        all_missing += res["missing_rows"]
    if not only_source or only_source == "pbp_player_week_rollup":
        res = _flat_rollup(con, _licensed_flat_specs())
        summary["pbp_player_week_rollup"] = {"per_col": res["per_col"],
                                             **res["row_signals"]}
        all_diffs += res["diffs"]
    con.close()

    import csv
    if all_diffs:
        p = os.path.join(OUT_DIR, f"witness_cell_diffs_{stamp}.csv")
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_diffs[0]))
            w.writeheader()
            w.writerows(all_diffs)
    if all_missing:
        p = os.path.join(OUT_DIR, f"missing_rows_{stamp}.csv")
        with open(p, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(all_missing[0]))
            w.writeheader()
            w.writerows(all_missing)
    with open(os.path.join(OUT_DIR, "CELL_WITNESS_SUMMARY.json"), "w",
              encoding="utf-8") as f:
        json.dump({"generated_at_utc": datetime.now(timezone.utc).isoformat(),
                   "summary": summary, "diffs_enumerated": len(all_diffs),
                   "missing_rows_enumerated": len(all_missing)}, f, indent=1)
    return summary


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default=None)
    a = ap.parse_args()
    s = run(a.source)
    grand_eq = grand_diff = grand_fill = 0
    for src, res in s.items():
        print(f"\n== {src} ==")
        for c, v in res["per_col"].items():
            tot = v["witnessed_equal"] + v["witnessed_diff"]
            pct = v["witnessed_equal"] / tot if tot else 0
            print(f"  {c:26s} equal={v['witnessed_equal']:>9,} diff={v['witnessed_diff']:>7,} "
                  f"({pct:.2%})  fill={v['fill_signal']:>6,}  zero_vs_null={v['zero_vs_null']:>8,}")
            grand_eq += v["witnessed_equal"]; grand_diff += v["witnessed_diff"]
            grand_fill += v["fill_signal"]
        rs = {k: v for k, v in res.items() if k != "per_col"}
        print(f"  rows: {rs}")
    print(f"\nGRAND: witnessed_equal={grand_eq:,} witnessed_diff={grand_diff:,} "
          f"fill_signals={grand_fill:,}")
