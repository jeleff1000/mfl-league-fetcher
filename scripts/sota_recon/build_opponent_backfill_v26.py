"""
sota_recon/build_opponent_backfill_v26.py -- wave57: fill missing opponent fields.

7,398 weekly rows carry no opponent_nfl_team (5,958 no opponent franchise number; all
eras, 1920s and 1960s-70s heaviest). nfl_team_games_all knows the opponent for 6,597 of
them via an unambiguous join; the rest (doubleheader team-weeks / uncovered 1920s games)
are queued, never guessed.

Lanes (closed, exact-count gated):
  DATE   row has game_date; (franchise, date) joins team_games
  WEEK   dateless; (franchise, year, week) joins exactly ONE team_games game
  QUEUE  dateless in a multi-game team-week, or no team_games coverage

Fills only NULL/blank cells: opponent_nfl_team (v26-era abbrev via the year's modal
abbrev for the opponent franchise, falling back to team_games opponent_code) and
opponent_nfl_franchise_number. Never overwrites a non-null value. Facts per filled cell.

    python -m scripts.sota_recon.build_opponent_backfill_v26 [--apply]
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .sources import latest_v26, TEAM_GAMES
from .recon_common import utc_stamp

WAVE = "wave57.opponent_backfill"
QUEUE_TAG = "opponent_unresolved_queue"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"

MISSING = ("(opponent_nfl_team IS NULL OR TRIM(opponent_nfl_team) = '' "
           "OR opponent_nfl_franchise_number IS NULL)")


def _tg_path() -> str:
    return Path(TEAM_GAMES.path).as_posix() if hasattr(TEAM_GAMES, "path") else str(TEAM_GAMES)


def _build_plan(con: duckdb.DuckDBPyConnection, vq: str) -> None:
    tg = _tg_path()
    # unsplit-DH weeks duplicate player_week; key-joined REPLACE would stamp both
    # physical rows on one adjudication -> those keys queue instead
    con.execute(f"""CREATE TEMP TABLE dupkeys AS
        SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*) > 1""")
    con.execute(f"""CREATE TEMP TABLE abbrev AS
        SELECT nfl_franchise_number AS fid, CAST(year AS INT) AS year,
               MODE(nfl_team) AS abbr
        FROM '{vq}' WHERE nfl_team IS NOT NULL GROUP BY 1, 2""")
    con.execute(f"""CREATE TEMP TABLE tgd AS
        SELECT team_fid, CAST(game_date AS DATE) AS gd,
               ANY_VALUE(opponent_fid) AS ofid, ANY_VALUE(opponent_code) AS ocode
        FROM '{tg}' GROUP BY 1, 2""")
    con.execute(f"""CREATE TEMP TABLE tgw AS
        SELECT team_fid, CAST(year AS INT) AS year, CAST(week AS INT) AS week,
               ANY_VALUE(opponent_fid) AS ofid, ANY_VALUE(opponent_code) AS ocode,
               COUNT(*) AS n
        FROM '{tg}' GROUP BY 1, 2, 3""")
    con.execute(f"""CREATE TEMP TABLE plan AS
        SELECT m.player_week,
               m.opponent_nfl_team AS old_team, m.opponent_nfl_franchise_number AS old_fid,
               COALESCE(d.ofid, w1.ofid) AS new_fid,
               COALESCE(a.abbr, COALESCE(d.ocode, w1.ocode)) AS new_team,
               CASE WHEN d.ofid IS NOT NULL THEN 'date' ELSE 'week' END AS lane
        FROM (SELECT player_week, opponent_nfl_team, opponent_nfl_franchise_number,
                     nfl_franchise_number AS fid, CAST(game_date AS DATE) AS gd,
                     CAST(year AS INT) AS year, CAST(week AS INT) AS week
              FROM '{vq}' WHERE {MISSING}
                AND player_week NOT IN (SELECT player_week FROM dupkeys)) m
        LEFT JOIN tgd d ON d.team_fid = m.fid AND d.gd = m.gd
        LEFT JOIN tgw w1 ON w1.team_fid = m.fid AND w1.year = m.year
                        AND w1.week = m.week AND w1.n = 1
        LEFT JOIN abbrev a ON a.fid = COALESCE(d.ofid, w1.ofid) AND a.year = m.year
        WHERE COALESCE(d.ofid, w1.ofid) IS NOT NULL""")
    con.execute(f"""CREATE TEMP TABLE queue AS
        SELECT w.player_week, CAST(w.year AS INT) AS year, CAST(w.week AS INT) AS week,
               w.nfl_team
        FROM '{vq}' w
        WHERE {MISSING}
          AND w.player_week NOT IN (SELECT player_week FROM plan)""")


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    _build_plan(con, vq)
    n_plan = con.execute("SELECT COUNT(*) FROM plan").fetchone()[0]
    n_queue = con.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    n_dup = con.execute(
        "SELECT COUNT(*) FROM (SELECT player_week FROM plan GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    lanes = con.execute("SELECT lane, COUNT(*) FROM plan GROUP BY 1").fetchall()
    res_common = {"planned_fills": n_plan, "queued": n_queue, "lanes": lanes,
                  "dup_keys_in_plan": n_dup}
    if not apply:
        con.close()
        return {"mode": "DRY", **res_common}

    if n_dup:
        con.close()
        return {"mode": "APPLY", "gate_pass": False, **res_common,
                "error": "plan has duplicate player_week targets"}

    stamp = utc_stamp()
    os.makedirs(SWEEPS, exist_ok=True)
    queue_csv = os.path.join(SWEEPS, "wave57_opponent_queue.csv")
    con.execute(f"COPY (SELECT * FROM queue ORDER BY player_week) TO "
                f"'{Path(queue_csv).as_posix()}' (HEADER)")

    sp = os.path.join(os.path.dirname(v26), f".duckspill_{stamp}")
    os.makedirs(sp, exist_ok=True)
    con.execute("PRAGMA threads=3")
    con.execute("PRAGMA disable_progress_bar")
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET temp_directory='{sp}'")
    before_n = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    before_dups = con.execute(
        f"SELECT COUNT(*) FROM (SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    before_missing = con.execute(
        f"SELECT COUNT(*) FROM '{vq}' WHERE {MISSING}").fetchone()[0]
    out_sql = f"""
        SELECT w.* REPLACE (
            CASE WHEN p.player_week IS NOT NULL
                      AND (w.opponent_nfl_team IS NULL OR TRIM(w.opponent_nfl_team) = '')
                 THEN p.new_team ELSE w.opponent_nfl_team END AS opponent_nfl_team,
            CASE WHEN p.player_week IS NOT NULL AND w.opponent_nfl_franchise_number IS NULL
                 THEN p.new_fid ELSE w.opponent_nfl_franchise_number
                 END AS opponent_nfl_franchise_number,
            CASE WHEN p.player_week IS NOT NULL
                 THEN COALESCE(w.recon_correction_log || ',', '') || '{WAVE}'
                 ELSE w.recon_correction_log END AS recon_correction_log)
        FROM '{vq}' w LEFT JOIN plan p ON w.player_week = p.player_week"""
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_oppfill.parquet")
    rb = con.execute(out_sql).fetch_record_batch(50000)
    writer = pq.ParquetWriter(str(tmp), rb.schema)
    for batch in rb:
        writer.write_batch(batch)
    writer.close()

    tq = tmp.as_posix()
    after_n = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    after_dups = con.execute(
        f"SELECT COUNT(*) FROM (SELECT player_week FROM '{tq}' GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    after_missing = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE {MISSING}").fetchone()[0]
    n_stamped = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE recon_correction_log LIKE '%{WAVE}%'"
    ).fetchone()[0]

    from . import golden_samples
    import scripts.sota_recon.sources as S
    old_latest, old_g = S.latest_v26, golden_samples.latest_v26
    S.latest_v26 = golden_samples.latest_v26 = lambda: str(tmp)
    try:
        golden = golden_samples.run()
    finally:
        S.latest_v26, golden_samples.latest_v26 = old_latest, old_g

    gate = (
        golden["failed"] == 0
        and after_n == before_n
        and after_dups == before_dups
        and n_stamped == n_plan
        and after_missing == n_queue
        and before_missing == n_plan + n_queue
    )
    res = {"mode": "APPLY", **res_common, "rows": [before_n, after_n],
           "missing": [before_missing, after_missing], "stamped": n_stamped,
           "dup_keys": [before_dups, after_dups],
           "golden": f"{golden['passed']}/{golden['total']}", "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        for pw, old_team, old_fid, new_fid, new_team, lane in con.execute(
                "SELECT * FROM plan").fetchall():
            if old_team is None or str(old_team).strip() == "":
                facts.emit_fact("cell_override", con=fc,
                                table_name="nfl_player_stats_all", target_key=pw,
                                column_name="opponent_nfl_team",
                                old_value="NULL", new_value=str(new_team), wave_id=WAVE,
                                reason="missing opponent filled from schedule authority",
                                witness=f"nfl_team_games_all via {lane} join",
                                source_snapshot_id=snap)
            if old_fid is None:
                facts.emit_fact("cell_override", con=fc,
                                table_name="nfl_player_stats_all", target_key=pw,
                                column_name="opponent_nfl_franchise_number",
                                old_value="NULL", new_value=str(new_fid), wave_id=WAVE,
                                reason="missing opponent franchise filled from schedule authority",
                                witness=f"nfl_team_games_all via {lane} join",
                                source_snapshot_id=snap)
        for pw, year, week, team in con.execute("SELECT * FROM queue").fetchall():
            facts.emit_fact("row_tag", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw, tag=QUEUE_TAG, wave_id=WAVE,
                            reason=f"no unambiguous team_games opponent for "
                                   f"{team} {year} wk{week}",
                            witness=f"queue csv {os.path.basename(queue_csv)}",
                            source_snapshot_id=snap)
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew57g_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res.update(backup=bk.name, queue_csv=queue_csv, swapped=True)
    else:
        os.remove(tmp)
        res["swapped"] = False
    shutil.rmtree(sp, ignore_errors=True)
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    print(run(apply=ap.parse_args().apply))
