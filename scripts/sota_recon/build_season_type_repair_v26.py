"""
sota_recon/build_season_type_repair_v26.py -- wave57: align season_type to team_games.

The original pipeline mislabeled entire late-season strata POST (AFL 1960s weeks 13-17,
WWII weeks 12-13, 1982 strike week 17, 1993 week 18 -- 4,665 rows) and a handful of true
playoff-tiebreaker games REG (71 rows, 1945-1969). nfl_team_games_all is the season_type
authority (same source the insert waves carried, which is how the disagreement surfaced:
every float-key twin pair conflicted on season_type alone, same game_date).

Repair lanes (closed, exact-count gated):
  DATE     weekly row has game_date; (franchise, date) joins team_games -> take its st
  WEEK     dateless row; (franchise, year, week) joins exactly ONE team_games game -> take st
  QUEUE    dateless row in a multi-game (franchise, year, week) -> parked, untouched

Facts: cell_override season_type per flipped row.

    python -m scripts.sota_recon.build_season_type_repair_v26 [--apply]
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

WAVE = "wave57.season_type_repair"
QUEUE_TAG = "season_type_ambiguous_queue"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"


def _tg_path() -> str:
    return Path(TEAM_GAMES.path).as_posix() if hasattr(TEAM_GAMES, "path") else str(TEAM_GAMES)


def _build_plan(con: duckdb.DuckDBPyConnection, vq: str) -> None:
    tg = _tg_path()
    # unsplit-DH weeks duplicate player_week; a key-joined REPLACE would flip both
    # physical rows on one adjudication -> those keys queue instead
    con.execute(f"""CREATE TEMP TABLE dupkeys AS
        SELECT player_week FROM '{vq}' GROUP BY 1 HAVING COUNT(*) > 1""")
    con.execute(f"""CREATE TEMP TABLE tg_date AS
        SELECT team_fid, CAST(game_date AS DATE) AS gd, ANY_VALUE(season_type) AS st
        FROM '{tg}' GROUP BY 1, 2""")
    con.execute(f"""CREATE TEMP TABLE tg_week AS
        SELECT team_fid, CAST(year AS INT) AS year, CAST(week AS INT) AS week,
               ANY_VALUE(season_type) AS st, COUNT(*) AS n_games
        FROM '{tg}' GROUP BY 1, 2, 3""")
    # DATE lane
    con.execute(f"""CREATE TEMP TABLE plan AS
        SELECT w.player_week, w.season_type AS old_st, a.st AS new_st, 'date' AS lane
        FROM '{vq}' w
        JOIN tg_date a ON a.team_fid = w.nfl_franchise_number
                      AND a.gd = CAST(w.game_date AS DATE)
        WHERE w.season_type IS DISTINCT FROM a.st
          AND w.player_week NOT IN (SELECT player_week FROM dupkeys)""")
    # WEEK lane (dateless, unambiguous week)
    con.execute(f"""INSERT INTO plan
        SELECT w.player_week, w.season_type, a.st, 'week'
        FROM '{vq}' w
        JOIN tg_week a ON a.team_fid = w.nfl_franchise_number
                      AND a.year = CAST(w.year AS INT) AND a.week = CAST(w.week AS INT)
        WHERE w.game_date IS NULL AND a.n_games = 1
          AND w.season_type IS DISTINCT FROM a.st
          AND w.player_week NOT IN (SELECT player_week FROM dupkeys)""")
    # QUEUE lane (dateless multi-game week, or dup-key row that would disagree)
    con.execute(f"""CREATE TEMP TABLE queue AS
        SELECT w.player_week, w.season_type AS old_st, a.st AS tg_st, a.n_games
        FROM '{vq}' w
        JOIN tg_week a ON a.team_fid = w.nfl_franchise_number
                      AND a.year = CAST(w.year AS INT) AND a.week = CAST(w.week AS INT)
        WHERE (w.game_date IS NULL AND a.n_games > 1
               OR w.player_week IN (SELECT player_week FROM dupkeys))
          AND w.season_type IS DISTINCT FROM a.st""")


def run(apply: bool = False) -> dict:
    v26 = latest_v26()
    vq = Path(v26).as_posix()
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    _build_plan(con, vq)
    summary = con.execute("""
        SELECT lane, old_st, new_st, COUNT(*) FROM plan GROUP BY 1,2,3 ORDER BY 4 DESC
    """).fetchall()
    n_plan = con.execute("SELECT COUNT(*) FROM plan").fetchone()[0]
    n_dupkeys_in_plan = con.execute(
        "SELECT COUNT(*) FROM (SELECT player_week FROM plan GROUP BY 1 HAVING COUNT(*)>1)"
    ).fetchone()[0]
    n_queue = con.execute("SELECT COUNT(*) FROM queue").fetchone()[0]
    res_common = {"planned_flips": n_plan, "queued": n_queue,
                  "plan_summary": summary, "dup_keys_in_plan": n_dupkeys_in_plan}

    if not apply:
        con.close()
        return {"mode": "DRY", **res_common}

    if n_dupkeys_in_plan:
        # a player_week matched two authority rows with different verdicts -- unsafe
        con.close()
        return {"mode": "APPLY", "gate_pass": False, **res_common,
                "error": "plan has duplicate player_week targets"}

    stamp = utc_stamp()
    os.makedirs(SWEEPS, exist_ok=True)
    queue_csv = os.path.join(SWEEPS, "wave57_season_type_queue.csv")
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
    out_sql = f"""
        SELECT w.* REPLACE (
            CASE WHEN p.player_week IS NOT NULL THEN p.new_st ELSE w.season_type END AS season_type,
            CASE WHEN p.player_week IS NOT NULL
                 THEN COALESCE(w.recon_correction_log || ',', '') || '{WAVE}'
                 ELSE w.recon_correction_log END AS recon_correction_log)
        FROM '{vq}' w LEFT JOIN plan p ON w.player_week = p.player_week"""
    vp = Path(v26)
    tmp = vp.with_name(vp.stem + "_streprr.parquet")
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
    n_stamped = con.execute(
        f"SELECT COUNT(*) FROM '{tq}' WHERE recon_correction_log LIKE '%{WAVE}%'"
    ).fetchone()[0]
    # authority now satisfied on the date lane (dup-key rows queue, not flip)
    remaining_date = con.execute(f"""
        SELECT COUNT(*) FROM '{tq}' w
        JOIN tg_date a ON a.team_fid = w.nfl_franchise_number
                      AND a.gd = CAST(w.game_date AS DATE)
        WHERE w.season_type IS DISTINCT FROM a.st
          AND w.player_week NOT IN (SELECT player_week FROM dupkeys)""").fetchone()[0]

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
        and remaining_date == 0
    )
    res = {"mode": "APPLY", **res_common, "rows": [before_n, after_n],
           "dup_keys": [before_dups, after_dups], "stamped": n_stamped,
           "remaining_date_disagreements": remaining_date,
           "golden": f"{golden['passed']}/{golden['total']}", "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(vq))
        for pw, old_st, new_st, lane in con.execute(
                "SELECT player_week, old_st, new_st, lane FROM plan").fetchall():
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw, column_name="season_type",
                            old_value=str(old_st), new_value=str(new_st), wave_id=WAVE,
                            reason="pipeline mislabeled late-season strata "
                                   "(AFL 60s / WWII / 1982 wk17 / 1993 wk18)",
                            witness=f"nfl_team_games_all season_type via {lane} join",
                            source_snapshot_id=snap)
        for pw, old_st, tg_st, n_games in con.execute(
                "SELECT player_week, old_st, tg_st, n_games FROM queue").fetchall():
            facts.emit_fact("row_tag", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw, tag=QUEUE_TAG, wave_id=WAVE,
                            reason=f"dateless row in {n_games}-game team-week; "
                                   f"weekly={old_st} vs team_games={tg_st}",
                            witness=f"queue csv {os.path.basename(queue_csv)}",
                            source_snapshot_id=snap)
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew57f_{stamp}.parquet")
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
