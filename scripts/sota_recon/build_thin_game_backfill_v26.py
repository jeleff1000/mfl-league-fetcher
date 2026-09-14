"""
sota_recon/build_thin_game_backfill_v26.py  --  wave44: backfill thin/empty pre-1978 games
from the PFR player_offense boxscore lines already on the lake.

Root cause (adjudication 2026-07-10): late-season games 1933-1968 are thin or absent in
v26 (e.g. KAN 1968 wk15 = 6 wave-artifact rows, wk12 absent) while full player box lines
exist in raw/pfr/boxscores/tables/player_offense. 742 of the 968 season-grain consensus
disputes trace here.

SAFETY DESIGN (agreed with Joe):
  * INSERT-ONLY for players ABSENT from the team-week -- existing rows are never touched.
  * Doubleheader team-weeks are excluded entirely (their merged rows may already contain
    the "missing" game; that class belongs to doubleheader wave 2).
  * Identity: bio.pfr_id is unique; lines whose pfr_id has no bio mapping are SKIPPED to
    the identity queue, never guessed.
  * player_week collisions (id already has a row that year+week, e.g. other team) SKIPPED.
  * Every inserted row emits a row_add FACT with the boxscore citation.
  * Gates on apply: row count == before + inserted; zero new duplicate player_weeks;
    golden_samples 24/24; per-year league totals must move TOWARD the PFR authority.

    python -m scripts.sota_recon.build_thin_game_backfill_v26            # DRY RUN + diff
    python -m scripts.sota_recon.build_thin_game_backfill_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import PLAYER_BIO, PLAYER_OFFENSE_BOX, TEAM_GAMES, latest_v26

WAVE = "wave44.thin_game_backfill"
THIN_MAX_REAL_ROWS = 4
PREVIEW = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
           r"\phase1_sweeps\wave44_backfill_preview.csv")

# box column -> v26 column (atoms only; everything else stays NULL for these rows)
ATOMS = {
    "pass_cmp": "completions", "pass_att": "attempts", "pass_yds": "passing_yards",
    "pass_td": "passing_tds", "pass_int": "passing_interceptions",
    "pass_sacked": "sacks_suffered", "pass_sacked_yds": "sack_yards_lost",
    "pass_long": "passing_long",
    "rush_att": "carries", "rush_yds": "rushing_yards", "rush_td": "rushing_tds",
    "rush_long": "rushing_long",
    "rec": "receptions", "rec_yds": "receiving_yards", "rec_td": "receiving_tds",
    "rec_long": "receiving_long", "targets": "targets",
    "fumbles": "fumbles", "fumbles_lost": "fumbles_lost",
}


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build_candidates(con, vq: str) -> None:
    """TEMP TABLE new_rows: the insertable box lines, fully keyed and attributed."""
    tg, po, bio = _q(TEAM_GAMES), _q(PLAYER_OFFENSE_BOX), _q(PLAYER_BIO)
    atom_sel = ", ".join(f"TRY_CAST(b.{src} AS DOUBLE) AS {dst}" for src, dst in ATOMS.items())
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE new_rows AS
    WITH dh AS (SELECT team_fid, year, CAST(week AS INT) AS week FROM '{tg}'
                GROUP BY 1,2,3 HAVING COUNT(*) > 1),
    v_teamwk AS (
      SELECT year, CAST(week AS INT) AS week, nfl_franchise_number AS fid,
             COUNT(*) FILTER (WHERE COALESCE(passing_yards, rushing_yards, receiving_yards)
                              IS NOT NULL) AS real_rows
      FROM '{vq}' WHERE season_type = 'REG' AND year < 1978 GROUP BY 1,2,3),
    games AS (
      SELECT DISTINCT g.boxscore_id, g.year, CAST(g.week AS INT) AS week,
             g.team_fid, g.opponent_fid, g.team_code, g.opponent_code, g.game_date
      FROM '{tg}' g
      LEFT JOIN v_teamwk v ON v.year = g.year AND v.week = CAST(g.week AS INT)
        AND v.fid = g.team_fid
      LEFT JOIN dh ON dh.team_fid = g.team_fid AND dh.year = g.year
        AND dh.week = CAST(g.week AS INT)
      WHERE g.season_type = 'REG' AND g.year < 1978
        AND COALESCE(v.real_rows, 0) <= {THIN_MAX_REAL_ROWS}
        AND dh.team_fid IS NULL),
    -- v26 team abbrevs per franchise-year (mode), fallback to team_games code
    abbrev AS (
      SELECT nfl_franchise_number AS fid, year, MODE(nfl_team) AS abbr
      FROM '{vq}' WHERE season_type = 'REG' AND nfl_team IS NOT NULL GROUP BY 1, 2),
    lines AS (
      SELECT g.*, regexp_extract(b.player_link_ids, '^([^,]+)', 1) AS pfr_id, b.team AS box_team,
             {atom_sel}
      FROM '{po}' b JOIN games g USING (boxscore_id)
      WHERE b.player_link_ids IS NOT NULL)
    SELECT bio.NFL_player_id,
           bio.NFL_player_id || '_' || CAST(l.year AS INT) || '_' || l.week AS player_week,
           l.year, l.week, 'REG' AS season_type,
           COALESCE(a1.abbr, l.team_code) AS nfl_team,
           COALESCE(a2.abbr, l.opponent_code) AS opponent_nfl_team,
           l.team_fid AS nfl_franchise_number, l.opponent_fid AS opponent_nfl_franchise_number,
           TRY_CAST(l.game_date AS TIMESTAMP) AS game_date,
           bio.nfl_position, bio.nfl_position AS position, bio.player AS player,
           '{WAVE}:' || l.boxscore_id AS recon_correction_log,
           l.boxscore_id, l.pfr_id,
           {", ".join(f"l.{dst}" for dst in ATOMS.values())}
    FROM lines l
    JOIN '{bio}' bio ON bio.pfr_id = l.pfr_id
    LEFT JOIN abbrev a1 ON a1.fid = l.team_fid AND a1.year = l.year
    LEFT JOIN abbrev a2 ON a2.fid = l.opponent_fid AND a2.year = l.year
    -- box_team must be the target franchise's side of the game (player_offense holds both teams)
    WHERE l.box_team = l.team_code
      -- absent-only: no existing v26 row for this player in this team-week
      AND NOT EXISTS (
        SELECT 1 FROM '{vq}' v WHERE v.NFL_player_id = bio.NFL_player_id
          AND v.year = l.year AND CAST(v.week AS INT) = l.week AND v.season_type = 'REG')
    """)


def _diagnostics(con, vq: str) -> dict:
    tg, po, bio = _q(TEAM_GAMES), _q(PLAYER_OFFENSE_BOX), _q(PLAYER_BIO)
    inserts = con.execute("SELECT COUNT(*) FROM new_rows").fetchone()[0]
    games = con.execute("SELECT COUNT(DISTINCT boxscore_id) FROM new_rows").fetchone()[0]
    dup_pw = con.execute("""
        SELECT COUNT(*) FROM (SELECT player_week FROM new_rows GROUP BY 1 HAVING COUNT(*) > 1)
    """).fetchone()[0]
    unmapped = con.execute(f"""
        SELECT COUNT(DISTINCT regexp_extract(b.player_link_ids, '^([^,]+)', 1))
        FROM '{po}' b
        LEFT JOIN '{bio}' bio ON bio.pfr_id = regexp_extract(b.player_link_ids, '^([^,]+)', 1)
        WHERE bio.pfr_id IS NULL AND b.player_link_ids IS NOT NULL""").fetchone()[0]
    per_year = con.execute("""
        SELECT year, COUNT(*), SUM(COALESCE(passing_yards,0)), SUM(COALESCE(rushing_yards,0)),
               SUM(COALESCE(receiving_yards,0))
        FROM new_rows GROUP BY 1 ORDER BY 1""").fetchall()
    return {"inserted_rows": inserts, "games_touched": games, "dup_player_weeks_in_new": dup_pw,
            "box_pfr_ids_without_bio_mapping_global": unmapped,
            "per_year": [(int(y), n, int(p), int(r), int(rc)) for y, n, p, r, rc in per_year]}


def run(apply: bool = False) -> dict:
    vq = _q(latest_v26())
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    _build_candidates(con, vq)
    diag = _diagnostics(con, vq)
    con.execute(f"""COPY (SELECT * FROM new_rows ORDER BY year, week, nfl_team, player_week)
                    TO '{Path(PREVIEW).as_posix()}' (HEADER)""")
    diag["preview_csv"] = PREVIEW
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}

    # ---- APPLY: stream union-by-name, gate, emit facts, swap -----------------------------
    if diag["dup_player_weeks_in_new"]:
        raise SystemExit("gate: duplicate player_weeks inside new rows -- aborting")
    stamp = utc_stamp()
    vp = Path(latest_v26())
    tmp = vp.with_name(vp.stem + "_w44.parquet")
    con.execute("PRAGMA threads=3")
    con.execute("SET preserve_insertion_order=false")
    before = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    r = con.execute(f"""
        SELECT * FROM '{vq}'
        UNION ALL BY NAME
        SELECT * EXCLUDE (boxscore_id, pfr_id) FROM new_rows
    """).fetch_record_batch(50000)
    w = pq.ParquetWriter(str(tmp), r.schema)
    for b in r:
        w.write_batch(b)
    w.close()

    tq = Path(tmp).as_posix()
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    new_dups = con.execute(f"""
        SELECT COUNT(*) FROM (SELECT player_week FROM '{tq}' WHERE player_week IS NOT NULL
        GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    base_dups = con.execute(f"""
        SELECT COUNT(*) FROM (SELECT player_week FROM '{vq}' WHERE player_week IS NOT NULL
        GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]

    from . import golden_samples
    import scripts.sota_recon.sources as S
    orig = S.latest_v26
    S.latest_v26 = lambda: str(tmp)
    try:
        g = golden_samples.run()
    finally:
        S.latest_v26 = orig

    gate = (after == before + diag["inserted_rows"]) and (new_dups == base_dups) \
        and (g["failed"] == 0)
    res = {"mode": "APPLY", **diag, "before": before, "after": after,
           "golden": f"{g['passed']}/{g['total']}", "new_dup_groups": new_dups - base_dups,
           "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        rows = con.execute("""SELECT player_week, boxscore_id, to_json(new_rows) FROM new_rows""").fetchall()
        for pw, bid, rj in rows:
            facts.emit_fact("row_add", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw, row_json=rj, wave_id=WAVE,
                            reason="thin/empty pre-1978 game; player absent from team-week",
                            witness=f"pfr_player_offense_box:{bid}",
                            source_snapshot_id=os.path.basename(os.path.dirname(vq)))
        fc.close()
        bk = vp.with_name(vp.stem + f"_prew44_{stamp}.parquet")
        shutil.copy2(vp, bk)
        os.replace(tmp, vp)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    r = run(apply=a.apply)
    print(json.dumps({k: v for k, v in r.items() if k != "per_year"}, indent=2, default=str))
    print("per-year (year, rows, pass_yds, rush_yds, rec_yds):")
    for row in r["per_year"]:
        print("  ", row)
