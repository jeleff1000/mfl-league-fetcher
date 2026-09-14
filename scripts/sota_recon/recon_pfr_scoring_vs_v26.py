"""
sota_recon/recon_pfr_scoring_vs_v26.py -- the witness recon that never existed.

PFR's per-play boxscore scoring table (pfr_box_scoring, 1920+) credits the scorer of every
TD/FG via description_link_ids. It was registered as a Source but NEVER wired into
witness_map as a voucher for the v26 scoring-count columns -- the event grain doesn't fit
the box/pages/flat MapSpec shapes, and it needs the pfr_id -> NFL_player_id crosswalk
through player_bio. So PFR's scoring attribution was never reconciled against v26's
rushing_tds / receiving_tds / def_tds / special_teams_tds / fg_made columns. The newspaper
pilot tripped over the gap; this lane makes it a standing, tracked recon.

Per (boxscore, scorer, atom) it compares PFR's count to v26's player-week cell:
  agree | pfr_higher (v26 undercount) | v26_higher (v26 overcount) | no_v26_row | no_crosswalk

Findings are REPORTED, era- and atom-bucketed, not auto-applied. Modern-era pfr_higher is
dominated by def_tds -- v26 does not credit the individual defender for return TDs at
player-week grain (a real systemic gap flagged for its own fix). Corrections stay gated to
independently corroborated cells (see promote_pfr_arbitrated_cells).

Outputs: derived/validation/sota_recon_master/pfr_scoring_vs_v26/
  PFR_SCORING_VS_V26_SUMMARY.json + discrepancies.csv

    python -m scripts.sota_recon.recon_pfr_scoring_vs_v26
"""
from __future__ import annotations

import json
import os
from pathlib import Path

import duckdb

from .newspaper_witness_common import pfr_scoring_atom_case
from .sources import DATA_LAKE, PLAYER_BIO, latest_v26

OUT = Path(DATA_LAKE) / "derived" / "validation" / "sota_recon_master" / "pfr_scoring_vs_v26"
SCORING = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "tables",
                       "scoring", "_combined.parquet").replace("\\", "/")
TG = os.path.join(DATA_LAKE, "raw", "pfr", "boxscores", "nfl_team_games_all.parquet").replace("\\", "/")
# atom names == granular v26 columns the scoring play actually feeds (NOT def_tds rollup)
ATOMS = ("rushing_tds", "receiving_tds", "def_int_ret_td", "fum_ret_td",
         "kickoff_return_tds", "punt_return_tds", "fg_made")


def run() -> dict:
    OUT.mkdir(parents=True, exist_ok=True)
    subject = latest_v26()
    sub = Path(subject).as_posix()
    bio = Path(PLAYER_BIO.path).as_posix()
    con = duckdb.connect()

    atom_case = pfr_scoring_atom_case("description")
    con.execute(f"""CREATE TEMP TABLE pfr AS
        WITH s AS (SELECT boxscore_id, description,
                          split_part(description_link_ids, ';', 1) pid
                   FROM read_parquet('{SCORING}')
                   WHERE description_link_ids IS NOT NULL AND description_link_ids <> '')
        SELECT boxscore_id, pid, {atom_case} atom, COUNT(*) n, ANY_VALUE(description) example
        FROM s GROUP BY 1, 2, {atom_case}""")
    con.execute(f"CREATE TEMP TABLE xw AS SELECT pfr_id, NFL_player_id FROM read_parquet('{bio}') WHERE pfr_id IS NOT NULL")
    con.execute(f"CREATE TEMP TABLE tg AS SELECT DISTINCT boxscore_id, CAST(year AS INT) yr, CAST(week AS INT) wk FROM read_parquet('{TG}')")
    col_case = " ".join(f"WHEN '{a}' THEN v.{a}" for a in ATOMS)
    con.execute(f"""CREATE TEMP TABLE cmp AS
        SELECT p.boxscore_id, p.pid, p.atom, p.n AS pfr_n, p.example, t.yr AS year,
               x.NFL_player_id AS nfl_id,
               TRY_CAST((CASE p.atom {col_case} END) AS DOUBLE) AS v26_n
        FROM pfr p JOIN tg t ON t.boxscore_id = p.boxscore_id
        LEFT JOIN xw x ON x.pfr_id = p.pid
        LEFT JOIN read_parquet('{sub}') v
               ON v.player_week = x.NFL_player_id || '_' || CAST(t.yr AS VARCHAR) || '_' || CAST(t.wk AS VARCHAR)
        WHERE p.atom IS NOT NULL""")

    def _era(y):
        return ("<1940" if y < 1940 else "1940-77" if y < 1978 else "1978+")

    rows = con.execute("""
        SELECT CASE WHEN year<1940 THEN '<1940' WHEN year<1978 THEN '1940-77' ELSE '1978+' END era,
               atom,
               CASE WHEN nfl_id IS NULL THEN 'no_crosswalk'
                    WHEN v26_n IS NULL THEN 'no_v26_row'
                    WHEN v26_n = pfr_n THEN 'agree'
                    WHEN pfr_n > v26_n THEN 'pfr_higher'
                    ELSE 'v26_higher' END verdict,
               COUNT(*) n
        FROM cmp GROUP BY 1,2,3""").fetchall()
    matrix: dict = {}
    for era, atom, verdict, n in rows:
        matrix.setdefault(era, {}).setdefault(verdict, {})[atom] = n

    con.execute(f"""COPY (
        SELECT boxscore_id, pid, nfl_id, year, atom, pfr_n, v26_n, example,
               CASE WHEN nfl_id IS NULL THEN 'no_crosswalk' WHEN v26_n IS NULL THEN 'no_v26_row'
                    WHEN v26_n=pfr_n THEN 'agree' WHEN pfr_n>v26_n THEN 'pfr_higher' ELSE 'v26_higher' END verdict
        FROM cmp WHERE nfl_id IS NOT NULL AND v26_n IS NOT NULL AND v26_n <> pfr_n
        ORDER BY year, atom) TO '{(OUT/'discrepancies.csv').as_posix()}' (HEADER)""")

    totals = {v: con.execute(f"""SELECT COUNT(*) FROM cmp WHERE nfl_id IS NOT NULL AND v26_n IS NOT NULL
              AND {'pfr_n>v26_n' if v=='pfr_higher' else 'pfr_n<v26_n' if v=='v26_higher' else 'v26_n=pfr_n'}""").fetchone()[0]
              for v in ("agree", "pfr_higher", "v26_higher")}
    con.close()

    summary = {
        "lane": "pfr_scoring_vs_v26",
        "subject": subject,
        "witness": "pfr_box_scoring (per-play scorer attribution, previously unmapped)",
        "crosswalk": "player_bio.pfr_id -> NFL_player_id",
        "totals": totals,
        "by_era_atom": matrix,
        "notes": {
            "column_mapping": "return TDs compared against GRANULAR v26 columns "
            "(fum_ret_td / def_int_ret_td / kickoff_return_tds / punt_return_tds), NOT the "
            "def_tds rollup. An earlier version lumped them into def_tds and manufactured a "
            "phantom ~1,500-cell gap (v26 already credits these, e.g. Newman fum_ret_td=1).",
            "correction_path": "REPORT only. Direct cell pokes are unsound -- they leave "
            "derived columns (total_tds_scored, pts_*, fantasy, LAMAR, ranks) stale. Real "
            "corrections must flow through the derivation pipeline, not cell overrides.",
        },
        "status": "review" if (totals["pfr_higher"] + totals["v26_higher"]) else "pass",
        "cloud_write_performed": False,
        "destructive_actions_performed": False,
        "write_guarantee": "Recon artifacts only; no table writes.",
    }
    with open(OUT / "PFR_SCORING_VS_V26_SUMMARY.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


if __name__ == "__main__":
    print(json.dumps(run(), indent=2))
