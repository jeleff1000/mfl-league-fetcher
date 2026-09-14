"""
sota_recon/build_identity_link_pass2_v26.py  --  wave48: identity links, all box sources.

Wave45 linked the wave44 funnel (offense box, pre-1978 REG). The cell-witness lane then
measured the FULL identity-gap class across all four boxscore tables: ~1,583 pids /
~11,800 activity lines whose box pfr_id has no bio mapping -- including Hall of Famers
(Dick LeBeau, Willie Brown) whose bio row's NFL_player_id IS the pfr id string with the
pfr_id field NULL. This pass generalizes wave45's evidence gates over every box source,
all years, REG+POST.

Evidence gate per auto-link (identical to wave45; never name-alone):
  1. name match is UNIQUE among bio rows lacking pfr_id, era-consistent with the PFR index
  2. AND at least one of:
       - bio.NFL_player_id == pfr_id (string identity -- the id scheme already agrees)
       - team-year overlap: the bio id's v26 (team, year)s intersect the pfr box (team, year)s
  3. a pfr_id links only when EXACTLY ONE candidate is decisive; a bio row won by more
     than one pfr_id sends all claimants to review. Anything weaker -> review CSV.

Extra review class (never auto-fixed): gap pids whose id-string bio row already carries a
DIFFERENT pfr_id -> wrong-mapping suspects (the SmitJe25 class), enumerated separately.

Mutates player_bio.parquet (backup + swap). Every link = cell_override fact. Gates:
row count unchanged, pfr_id unique after update, applied count == links.

    python -m scripts.sota_recon.build_identity_link_pass2_v26            # DRY RUN
    python -m scripts.sota_recon.build_identity_link_pass2_v26 --apply
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path

import duckdb

from .recon_common import utc_stamp
from .sources import DATA_LAKE, PLAYER_BIO, TEAM_GAMES, latest_v26, registry

WAVE = "wave48.identity_link_pass2"
SWEEPS = r"D:\league-history-data\nfl\derived\validation\sota_recon_master\phase1_sweeps"
REVIEW = os.path.join(SWEEPS, "wave48_link_review.csv")
WRONG_MAP = os.path.join(SWEEPS, "wave48_wrong_mapping_suspects.csv")

BOX_SOURCES = ["pfr_player_offense_box", "pfr_player_defense_box",
               "pfr_box_kicking", "pfr_box_returns"]


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build_links(con) -> None:
    tg, bio = _q(TEAM_GAMES), _q(PLAYER_BIO)
    idx = _q(os.path.join(DATA_LAKE, "raw", "pfr", "players", "player_index.parquet"))
    vq = _q(latest_v26())
    box_union = " UNION ALL ".join(
        f"SELECT player_link_ids, player, team, boxscore_id FROM '{_q(registry()[k].path)}' "
        f"WHERE player_link_ids IS NOT NULL" for k in BOX_SOURCES)
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE unmapped AS
    SELECT regexp_extract(b.player_link_ids, '^([^,]+)', 1) AS pfr_id,
           ANY_VALUE(b.player) AS box_name, COUNT(*) AS box_lines,
           array_agg(DISTINCT struct_pack(team_code := g.team_code, year := g.year)) AS box_team_years
    FROM ({box_union}) b
    JOIN (SELECT DISTINCT boxscore_id, year, team_code FROM '{tg}') g USING (boxscore_id)
    WHERE b.team = g.team_code
      AND NOT EXISTS (SELECT 1 FROM '{bio}' x
                      WHERE x.pfr_id = regexp_extract(b.player_link_ids, '^([^,]+)', 1))
    GROUP BY 1""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE cand AS
    WITH c AS (
      SELECT u.pfr_id, u.box_name, u.box_lines, u.box_team_years,
             b.NFL_player_id, b.player AS bio_name,
             (b.NFL_player_id = u.pfr_id) AS id_string_match
      FROM unmapped u
      JOIN '{idx}' i USING (pfr_id)
      JOIN '{bio}' b ON LOWER(TRIM(b.player)) = LOWER(TRIM(u.box_name)) AND b.pfr_id IS NULL
        AND (b.first_year IS NULL OR b.first_year <= TRY_CAST(i.last_year AS INT) + 1)
        AND (b.last_year IS NULL OR b.last_year >= TRY_CAST(i.first_year AS INT) - 1)),
    tyo AS (
      SELECT c.pfr_id, c.NFL_player_id, COUNT(*) AS overlap
      FROM c, UNNEST(c.box_team_years) AS t(bt)
      JOIN '{vq}' v ON v.NFL_player_id = c.NFL_player_id
        AND v.nfl_team = bt.team_code AND v.year = bt.year
      GROUP BY 1, 2)
    SELECT c.*, COALESCE(tyo.overlap, 0) AS team_year_overlap
    FROM c LEFT JOIN tyo USING (pfr_id, NFL_player_id)""")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE links AS
    WITH decisive AS (
      SELECT pfr_id, NFL_player_id, id_string_match
      FROM cand WHERE id_string_match OR team_year_overlap > 0),
    one_winner AS (
      SELECT pfr_id FROM decisive GROUP BY 1 HAVING COUNT(DISTINCT NFL_player_id) = 1),
    won_once AS (
      SELECT NFL_player_id FROM decisive WHERE pfr_id IN (SELECT pfr_id FROM one_winner)
      GROUP BY 1 HAVING COUNT(DISTINCT pfr_id) = 1)
    SELECT d.pfr_id, d.NFL_player_id,
           CASE WHEN d.id_string_match THEN 'id_string+name+era'
                ELSE 'team_year+name+era' END AS evidence
    FROM decisive d
    JOIN one_winner USING (pfr_id)
    JOIN won_once USING (NFL_player_id)""")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE review AS
    SELECT * FROM cand WHERE pfr_id NOT IN (SELECT pfr_id FROM links)""")
    # wrong-mapping suspects: id-string bio row exists but carries a DIFFERENT pfr_id
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE wrong_map AS
    SELECT u.pfr_id AS box_pfr_id, u.box_name, u.box_lines,
           b.NFL_player_id, b.player AS bio_name, b.pfr_id AS bio_pfr_id
    FROM unmapped u
    JOIN '{bio}' b ON b.NFL_player_id = u.pfr_id
    WHERE b.pfr_id IS NOT NULL AND b.pfr_id != u.pfr_id""")


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    _build_links(con)
    links = con.execute("SELECT pfr_id, NFL_player_id, evidence FROM links").fetchall()
    n_id = sum(1 for _, _, e in links if e.startswith("id_string"))
    review_n = con.execute("SELECT COUNT(DISTINCT pfr_id) FROM review").fetchone()[0]
    wrong_n = con.execute("SELECT COUNT(*) FROM wrong_map").fetchone()[0]
    unmapped_n, unmapped_lines = con.execute(
        "SELECT COUNT(*), SUM(box_lines) FROM unmapped").fetchone()
    con.execute(f"COPY review TO '{Path(REVIEW).as_posix()}' (HEADER)")
    con.execute(f"COPY wrong_map TO '{Path(WRONG_MAP).as_posix()}' (HEADER)")
    linked_lines = con.execute(
        "SELECT SUM(box_lines) FROM unmapped WHERE pfr_id IN (SELECT pfr_id FROM links)"
    ).fetchone()[0]
    diag = {"gap_pids": unmapped_n, "gap_lines": int(unmapped_lines or 0),
            "auto_links": len(links), "id_string_links": n_id,
            "team_year_links": len(links) - n_id,
            "lines_unblocked": int(linked_lines or 0),
            "sent_to_review": review_n, "wrong_mapping_suspects": wrong_n,
            "review_csv": REVIEW, "wrong_map_csv": WRONG_MAP}
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}

    bio_p = Path(PLAYER_BIO.path)
    bq = _q(PLAYER_BIO)
    stamp = utc_stamp()
    con.execute("CREATE TEMP TABLE lk (pfr_id VARCHAR, nid VARCHAR)")
    con.executemany("INSERT INTO lk VALUES (?, ?)", [(p, n) for p, n, _ in links])
    olds = dict(con.execute(f"""
        SELECT lk.nid, b.pfr_id FROM lk JOIN '{bq}' b ON b.NFL_player_id = lk.nid""").fetchall())

    tmp = bio_p.with_name(bio_p.stem + "_w48.parquet")
    con.execute(f"""
        COPY (
          SELECT b.* REPLACE (COALESCE(lk.pfr_id, b.pfr_id) AS pfr_id)
          FROM '{bq}' b LEFT JOIN lk ON lk.nid = b.NFL_player_id
        ) TO '{Path(tmp).as_posix()}' (FORMAT PARQUET)""")

    tq = Path(tmp).as_posix()
    before = con.execute(f"SELECT COUNT(*) FROM '{bq}'").fetchone()[0]
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    dup_pfr = con.execute(f"""
        SELECT COUNT(*) FROM (SELECT pfr_id FROM '{tq}' WHERE pfr_id IS NOT NULL
        GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    applied = con.execute(f"""
        SELECT COUNT(*) FROM '{tq}' t JOIN lk ON lk.nid = t.NFL_player_id
        WHERE t.pfr_id = lk.pfr_id""").fetchone()[0]
    gate = after == before and dup_pfr == 0 and applied == len(links)
    res = {"mode": "APPLY", **diag, "bio_before": before, "bio_after": after,
           "dup_pfr_after": dup_pfr, "links_applied": applied, "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(_q(latest_v26())))
        for pfr_id, nid, ev in links:
            facts.emit_fact("cell_override", con=fc, table_name="player_bio",
                            target_key=nid, column_name="pfr_id",
                            old_value=olds.get(nid), new_value=pfr_id,
                            wave_id=WAVE, reason=f"identity link ({ev})",
                            witness="pfr_player_index+box tables (4 sources)",
                            source_snapshot_id=snap)
        fc.close()
        bk = bio_p.with_name(bio_p.stem + f"_prew48_{stamp}.parquet")
        shutil.copy2(bio_p, bk)
        os.replace(tmp, bio_p)
        res["backup"] = str(bk)
        res["swapped"] = True
    else:
        os.remove(tmp)
        res["swapped"] = False
    con.close()
    return res


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
