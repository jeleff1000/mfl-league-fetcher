"""
sota_recon/build_newspaper_promotion_v26.py  --  wave56: the first newspaper atoms land.

RETIRED FOR WRITES (2026-07-17): newspaper is now a sidecar witness source (see
build_newspaper_witness_bundle.py + recon_newspaper_sidecar.py). --apply fail-closes;
dry-run is kept for historical review of the wave56 fill/insert/disagree ledger.

Source: newspaper_promoted.player_game_box_score rows that the newspaper pipeline
itself resolved (review_status='box_score_resolved') AND that carry full identity keys
(player_week + NFL_player_id). ~291 atoms, 1920-1938 -- Fritz Pollard's 120-yard 1920
game arrives with this wave.

DOCTRINE (Joe 2026-07-12, recorded as precedence facts): newspapers are primary, but
an extraction claim is not newspaper testimony until image-verified; and existing v26
values are never overwritten by extraction. So:

  FILL    existing v26 row, v26 cell IS NULL, newspaper cell has a value -> fill,
          cell_override fact (old NULL) with the source document citation
  DISAGREE existing v26 cell NON-NULL and newspaper differs -> NEVER overwritten;
          enumerated to the image-verification queue (Joe's ruling applies only
          after a human/model reads the page)
  INSERT  no v26 row for the player_week -> new row (wave49-style: keys/teams/fids
          from nfl_team_games_all via boxscore_id), row_add fact
  HELD    doubleheader team-weeks (inserts only -- ambiguous week attribution);
          ids absent from player_bio; boxscores with UNRESOLVED audit conflicts

Stats promoted (v26-identical names, VARCHAR''-safe casts): carries, rushing_yards,
rushing_tds, attempts, completions, passing_yards, passing_tds, passing_interceptions,
receptions, receiving_yards, receiving_tds, pat_made, pat_att, fg_made, fg_att,
fg_long, def_interceptions. ('touchdowns' composite + def_tds excluded v1.)

Gates: rowcount == before + inserts; fills touched ONLY previously-NULL cells
(recount proof); zero new dup player_weeks; invariants after (cumulative).

    python -m scripts.sota_recon.build_newspaper_promotion_v26            # DRY RUN
    python -m scripts.sota_recon.build_newspaper_promotion_v26 --apply
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from .recon_common import utc_stamp
from .sources import PLAYER_BIO, TEAM_GAMES, latest_v26

WAVE = "wave56.newspaper_promotion"
NP_DB = "D:/league-history-data/nfl/derived/newspaper_atoms/databases/newspaper_atoms.duckdb"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"
AUDIT_DIR = Path(r"D:\league-history-data\nfl\derived\newspaper_atoms\witness_conflict_audits")

STATS = ["carries", "rushing_yards", "rushing_tds", "attempts", "completions",
         "passing_yards", "passing_tds", "passing_interceptions", "receptions",
         "receiving_yards", "receiving_tds", "pat_made", "pat_att", "fg_made",
         "fg_att", "fg_long", "def_interceptions"]
BATCH = 131_072


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _unresolved_conflict_boxes() -> set[str]:
    """Boxscores whose audit conflicts are NOT externally/majority resolved."""
    runs = sorted(p for p in AUDIT_DIR.iterdir() if p.is_dir())
    if not runs:
        return set()
    res = runs[-1] / "adjudication_v1" / "conflict_resolutions.csv"
    if not res.exists():
        return set()
    held = set()
    with res.open(encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            # attendance is a context field -- a contested attendance never blocks
            # that game's PLAYER stat atoms
            if row["verdict"] in ("ESCALATED_NO_MATCH", "HELD_TIE") \
                    and row["field_name"] != "attendance":
                held.add(row["boxscore_id"])
    return held


def _build(con) -> None:
    tg, bio = _q(TEAM_GAMES), _q(PLAYER_BIO)
    vq = _q(latest_v26())
    held_boxes = _unresolved_conflict_boxes()
    held_sql = ", ".join(f"'{b}'" for b in sorted(held_boxes)) or "''"
    casts = ", ".join(f"TRY_CAST(NULLIF(TRIM(b.{c}), '') AS DOUBLE) AS {c}"
                      for c in STATS)
    activity = " OR ".join(f"{c} IS NOT NULL" for c in STATS)
    con.execute(f"ATTACH '{NP_DB}' AS np (READ_ONLY)")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE atoms AS
        SELECT TRIM(b.player_week) AS player_week, TRIM(b.NFL_player_id) AS nid,
               b.boxscore_id, TRIM(b.player_raw) AS player_raw,
               NULLIF(TRIM(b.nfl_team), '') AS np_team,
               TRIM(b.source_document_id) AS source_document_id, {casts}
        FROM np.newspaper_promoted.player_game_box_score b
        WHERE NULLIF(TRIM(b.player_week), '') IS NOT NULL
          AND NULLIF(TRIM(b.NFL_player_id), '') IS NOT NULL
          AND b.review_status = 'box_score_resolved'
          AND b.boxscore_id NOT IN ({held_sql})""")
    con.execute("CREATE OR REPLACE TEMP TABLE atoms2 AS "
                f"SELECT * FROM atoms WHERE {activity} "
                "QUALIFY ROW_NUMBER() OVER (PARTITION BY player_week ORDER BY source_document_id) = 1")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE idq AS
        SELECT * FROM atoms2 WHERE nid NOT IN
          (SELECT NFL_player_id FROM '{bio}' WHERE NFL_player_id IS NOT NULL)""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE ok_atoms AS
        SELECT a.* FROM atoms2 a WHERE a.nid IN
          (SELECT NFL_player_id FROM '{bio}' WHERE NFL_player_id IS NOT NULL)""")
    # fills vs inserts vs disagreements against the CURRENT v26
    v_stats = ", ".join(f"v.{c} AS v_{c}" for c in STATS)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE fills AS
        SELECT a.*, {v_stats}
        FROM ok_atoms a JOIN '{vq}' v ON v.player_week = a.player_week""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE inserts_raw AS
        SELECT a.* FROM ok_atoms a
        WHERE NOT EXISTS (SELECT 1 FROM '{vq}' v WHERE v.player_week = a.player_week)""")
    # insert construction: game context from team_games; doubleheader weeks HELD
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE dh AS
        SELECT team_fid, year, CAST(week AS INT) AS week, season_type
        FROM '{tg}' GROUP BY 1,2,3,4 HAVING COUNT(*) > 1""")
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE inserts AS
        WITH g AS (
          SELECT boxscore_id, year, CAST(week AS INT) AS week, season_type, game_date,
                 team_fid, opponent_fid, team_code, opponent_code,
                 COALESCE(is_home, FALSE) AS is_home
          FROM '{tg}'),
        abbrev AS (
          SELECT nfl_franchise_number AS fid, year, MODE(nfl_team) AS abbr
          FROM '{vq}' WHERE nfl_team IS NOT NULL GROUP BY 1, 2),
        -- 1920s bio rows carry no nfl_position; fall back to the player's own
        -- modal position (same-year preferred, career otherwise) or the row
        -- lands position-NULL and misses every eligibility lane (wave57a lesson)
        own_pos_year AS (
          SELECT NFL_player_id, year, ARG_MAX(position, n) AS pos FROM (
            SELECT NFL_player_id, year, position, COUNT(*) AS n FROM '{vq}'
            WHERE position IS NOT NULL GROUP BY 1, 2, 3) GROUP BY 1, 2),
        own_pos_career AS (
          SELECT NFL_player_id, ARG_MAX(position, n) AS pos FROM (
            SELECT NFL_player_id, position, COUNT(*) AS n FROM '{vq}'
            WHERE position IS NOT NULL GROUP BY 1, 2) GROUP BY 1),
        cand AS (
          SELECT i.*, g.year, g.week, g.season_type, g.game_date,
                 g.team_fid, g.opponent_fid, g.team_code, g.opponent_code,
                 COALESCE(bio.nfl_position, opy.pos, opc.pos) AS nfl_position,
                 bio.player AS bio_player,
                 (EXISTS (SELECT 1 FROM dh WHERE dh.team_fid = g.team_fid
                          AND dh.year = g.year AND dh.week = g.week
                          AND dh.season_type = g.season_type)) AS is_dh
          FROM inserts_raw i
          JOIN g ON g.boxscore_id = i.boxscore_id
                -- pin the player's SIDE (team_games has one row per side)
                AND (g.team_code = i.np_team OR i.np_team IS NULL)
          JOIN '{bio}' bio ON bio.NFL_player_id = i.nid
          LEFT JOIN own_pos_year opy ON opy.NFL_player_id = i.nid AND opy.year = g.year
          LEFT JOIN own_pos_career opc ON opc.NFL_player_id = i.nid
          -- CAST: g.year is DOUBLE; bare concat renders '1920.0' and matches nothing
          WHERE i.player_week LIKE i.nid || '_' || CAST(g.year AS INT) || '_' || g.week)
        SELECT c.*, COALESCE(a1.abbr, c.team_code) AS nfl_team_final,
               COALESCE(a2.abbr, c.opponent_code) AS opp_team_final
        FROM cand c
        LEFT JOIN abbrev a1 ON a1.fid = c.team_fid AND a1.year = c.year
        LEFT JOIN abbrev a2 ON a2.fid = c.opponent_fid AND a2.year = c.year
        QUALIFY COUNT(*) OVER (PARTITION BY c.player_week) = 1""")


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("SET preserve_insertion_order = false")
    v26_p = Path(latest_v26())
    vq = _q(v26_p)
    _build(con)

    # fill plan: only NULL v26 cells; disagreements enumerated, never written;
    # AGREEMENTS counted -- each is a new-lineage corroboration of an existing cell
    fill_cells, disagree, agreements = [], [], 0
    for row in con.execute("SELECT * FROM fills").fetchall():
        cols = [d[0] for d in con.description]
        r = dict(zip(cols, row))
        for c in STATS:
            nv, vv = r[c], r[f"v_{c}"]
            if nv is None:
                continue
            if vv is None:
                fill_cells.append((r["player_week"], c, nv, r["source_document_id"],
                                   r["boxscore_id"]))
            elif abs(nv - vv) > 0.5:
                disagree.append(dict(player_week=r["player_week"], column=c,
                                     newspaper=nv, v26=vv,
                                     source_document_id=r["source_document_id"],
                                     boxscore_id=r["boxscore_id"]))
            else:
                agreements += 1
    n_ins, dh_held = con.execute(
        "SELECT COUNT(*) FILTER (WHERE NOT is_dh), COUNT(*) FILTER (WHERE is_dh) "
        "FROM inserts").fetchone()
    idq_n = con.execute("SELECT COUNT(*) FROM idq").fetchone()[0]
    diag = dict(fill_cells=len(fill_cells), disagreements=len(disagree),
                corroborated_cells=agreements,
                insert_rows=n_ins, dh_held_inserts=dh_held, identity_queue=idq_n,
                unresolved_conflict_boxes_excluded=len(_unresolved_conflict_boxes()))
    os.makedirs(SWEEPS, exist_ok=True)
    with open(os.path.join(SWEEPS, "wave56_disagreements.csv"), "w", newline="",
              encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["player_week", "column", "newspaper", "v26",
                                          "source_document_id", "boxscore_id"])
        w.writeheader(); w.writerows(disagree)
    con.execute(f"COPY (SELECT * FROM idq) TO "
                f"'{Path(os.path.join(SWEEPS, 'wave56_identity_queue.csv')).as_posix()}' (HEADER)")
    con.execute(f"COPY (SELECT * FROM inserts WHERE is_dh) TO "
                f"'{Path(os.path.join(SWEEPS, 'wave56_dh_held.csv')).as_posix()}' (HEADER)")
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}

    fill_map: dict[str, dict[str, float]] = {}
    for pw, c, nv, src, bx in fill_cells:
        fill_map.setdefault(pw, {})[c] = nv
    ins_rows = con.execute("""
        SELECT player_week, nid, year, week, season_type, nfl_team_final,
               opp_team_final, team_fid, opponent_fid, game_date, nfl_position,
               bio_player, boxscore_id, source_document_id,
               """ + ", ".join(STATS) + """
        FROM inserts WHERE NOT is_dh""").fetchall()
    ins_cols = [d[0] for d in con.description]

    before_n = con.execute(f"SELECT COUNT(*) FROM '{vq}'").fetchone()[0]
    tmp = v26_p.with_name(v26_p.stem + "_w56.parquet")
    pf = pq.ParquetFile(vq)
    writer = None
    schema_names = None
    filled = 0
    try:
        for batch in pf.iter_batches(batch_size=BATCH):
            tbl = pa.Table.from_batches([batch])
            if schema_names is None:
                schema_names = set(tbl.schema.names)
            pws = tbl.column("player_week").to_pylist()
            hits = [i for i, p in enumerate(pws) if p in fill_map]
            if hits:
                touched: dict[str, list] = {}
                for i in hits:
                    for c, nv in fill_map[pws[i]].items():
                        if c not in touched:
                            touched[c] = tbl.column(c).to_pylist()
                        if touched[c][i] is None:  # fill ONLY NULL (re-assert)
                            touched[c][i] = nv
                            filled += 1
                for c, vals in touched.items():
                    tbl = tbl.set_column(tbl.schema.get_field_index(c), c,
                                         pa.array(vals, pa.float64()))
            if writer is None:
                writer = pq.ParquetWriter(str(tmp), tbl.schema)
            writer.write_table(tbl)
        # append the insert rows as a final batch matching the schema
        if ins_rows:
            base = {n: [None] * len(ins_rows) for n in schema_names}
            for j, row in enumerate(ins_rows):
                r = dict(zip(ins_cols, row))
                base["player_week"][j] = r["player_week"]
                base["NFL_player_id"][j] = r["nid"]
                base["year"][j] = float(r["year"])
                base["week"][j] = float(r["week"])
                base["season_type"][j] = r["season_type"]
                base["nfl_team"][j] = r["nfl_team_final"]
                base["opponent_nfl_team"][j] = r["opp_team_final"]
                base["nfl_franchise_number"][j] = float(r["team_fid"]) if r["team_fid"] is not None else None
                base["opponent_nfl_franchise_number"][j] = float(r["opponent_fid"]) if r["opponent_fid"] is not None else None
                base["nfl_position"][j] = r["nfl_position"]
                base["position"][j] = r["nfl_position"]
                base["player"][j] = r["bio_player"]
                base["recon_correction_log"][j] = f"{WAVE}:{r['boxscore_id']}"
                for c in STATS:
                    if r[c] is not None:
                        base[c][j] = float(r[c])
            arrays, fields = [], []
            ref = pq.ParquetFile(str(tmp)).schema_arrow if writer is None else writer.schema
            for field in ref:
                arrays.append(pa.array(base.get(field.name, [None] * len(ins_rows)),
                                       field.type))
                fields.append(field)
            writer.write_table(pa.Table.from_arrays(arrays, schema=pa.schema(fields)))
    finally:
        if writer is not None:
            writer.close()
        pf.close()

    tq = Path(tmp).as_posix()
    after_n = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    dup_pw = con.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT player_week FROM '{tq}'
          WHERE player_week IN (SELECT player_week FROM inserts WHERE NOT is_dh)
          GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    gate = (after_n == before_n + len(ins_rows) and dup_pw == 0
            and filled == len(fill_cells))
    res = {"mode": "APPLY", **diag, "rows": [before_n, after_n],
           "cells_filled": filled, "dup_player_weeks": dup_pw,
           "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = "newspaper_atoms.duckdb@20260712"
        for pw, c, nv, src, bx in fill_cells:
            facts.emit_fact("cell_override", con=fc, table_name="nfl_player_stats_all",
                            target_key=pw, column_name=c,
                            old_value="NULL", new_value=str(nv),
                            wave_id=WAVE,
                            reason="newspaper box-score fill into NULL cell "
                                   "(box_score_resolved; conflict-clean boxscore)",
                            witness=f"newspaper {src} boxscore {bx}",
                            source_snapshot_id=snap)
        for row in ins_rows:
            r = dict(zip(ins_cols, row))
            facts.emit_fact("row_add", con=fc, table_name="nfl_player_stats_all",
                            target_key=r["player_week"],
                            row_json=json.dumps({"player_week": r["player_week"],
                                                 "NFL_player_id": r["nid"],
                                                 "year": int(r["year"]),
                                                 "week": int(r["week"]),
                                                 "boxscore_id": r["boxscore_id"]}),
                            wave_id=WAVE,
                            reason="newspaper-witnessed player-game, no v26 row",
                            witness=f"newspaper {r['source_document_id']} "
                                    f"boxscore {r['boxscore_id']}",
                            source_snapshot_id=snap)
        fc.close()
        bk = v26_p.with_name(v26_p.stem + f"_prew56_{utc_stamp()}.parquet")
        shutil.copy2(v26_p, bk)
        os.replace(tmp, v26_p)
        res.update(backup=str(bk), swapped=True)
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    if a.apply:
        raise SystemExit(
            "FAIL-CLOSED (2026-07-17): newspaper atoms are finalized as sidecar witness "
            "tables, not supertable writes. Direct v26 mutation from newspaper extraction "
            "is retired. Use the immutable witness bundle under "
            "D:/league-history-data/nfl/curated/witnesses/newspaper/ and "
            "scripts/sota_recon/recon_newspaper_sidecar.py instead. "
            "Dry-run (no flags) remains available for review."
        )
    print(json.dumps(run(apply=False), indent=2, default=str))
