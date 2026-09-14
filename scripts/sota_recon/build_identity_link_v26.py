"""
sota_recon/build_identity_link_v26.py  --  wave45: link PFR identities into player_bio.

The wave44 funnel exposed ~660 box pfr_ids with no bio mapping, blocking 1,778 witnessed
player-game lines. The probe showed 98.6% have exactly one era-consistent bio candidate
(mostly bio rows whose NFL_player_id IS the pfr id string, with the pfr_id field NULL).

Evidence gate per auto-link (never name-alone -- the Reggie White lesson):
  1. name match is UNIQUE among bio rows lacking pfr_id, era-consistent with the PFR index
  2. AND at least one of:
       - bio.NFL_player_id == pfr_id (string identity -- the id scheme already agrees)
       - team-year overlap: the bio id's v26 (team, year)s intersect the pfr box (team, year)s
  anything weaker -> review CSV, no link.

MANUAL_RESOLUTIONS: the 9 hand-adjudicated cases (2026-07-10, evidence in session log):
seven links incl. one CORRECTION of a wrong existing mapping (Jerry Smith TE 1965-77:
bio pfr_id 'SmitJe25' -> 'SmitJe01'), two new bio rows (Fedora 1942, Raimondi 1947).

Mutates player_bio.parquet (backup + swap). Every link = cell_override fact on player_bio;
new rows = row_add facts. Gate: pfr_id unique across bio after update.

    python -m scripts.sota_recon.build_identity_link_v26            # DRY RUN
    python -m scripts.sota_recon.build_identity_link_v26 --apply
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
from .sources import DATA_LAKE, PLAYER_BIO, PLAYER_OFFENSE_BOX, TEAM_GAMES, latest_v26

WAVE = "wave45.identity_link"
REVIEW = (r"D:\league-history-data\nfl\derived\validation\sota_recon_master"
          r"\phase1_sweeps\wave45_link_review.csv")

# pfr_id -> NFL_player_id (hand-adjudicated links)
MANUAL_LINKS = {
    "BeasJo00": "BeasJo00",    # owns all 70 v26 rows; BEA470662 is an empty stub
    "BryaCh00": "BryaCh00",    # RB 1966-69 exact; 00-0028709 (gsis-id DT) is a separate suspect row
    "KennBo20": "KennBo20",    # NYY 1947 fits 1946-50; KennBo21 is the 1949-only Bob Kennedy
    "McAfGe20": "McAfGe20",    # owns all 78 v26 rows; HIST-17713151 is an empty stub
    "PaulDo01": "PaulDo01",    # Cardinals DB; PaulDo00 is the Rams LB (different human)
    "SmitCh01": "SmitCh01",    # OAK RB 1968-75; SmitCh00 is the later Eagles WR
    "SmitJe01": "SmitJe01",    # CORRECTION: row had WRONG pfr_id 'SmitJe25' (Jerry Smith TE 1965-77)
    "ShawBo02": "ShawBo02",    # id match + 28 team-year overlaps (rival claim had 1)
    "WashJo01": "WashJo01",    # id match + 8 overlaps (rival WashJo00 row had 1 stray)
    "SmitBo21": "SmitBo21",    # id match + 11 overlaps (rival row had 3 strays)
    # NOT linked, review as identity-split suspects: ShawBo01 (only claim is a row owned by
    # ShawBo02), SmitBo22 (its id-row has 0 game overlap while SmitBo02's row holds 15 of
    # its games -> bio likely already misattributes those games; forcing a link would
    # compound the error).
}
# brand-new players (no bio row exists): minimal witnessed bio rows
MANUAL_NEW_ROWS = [
    {"NFL_player_id": "FedoWa20", "pfr_id": "FedoWa20", "player": "Walt Fedora",
     "first_year": 1942, "last_year": 1942, "nfl_position": "RB"},
    {"NFL_player_id": "RaimBe20", "pfr_id": "RaimBe20", "player": "Ben Raimondi",
     "first_year": 1947, "last_year": 1947, "nfl_position": "QB"},
]


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _build_links(con) -> None:
    """TEMP TABLE links(pfr_id, NFL_player_id, evidence) + TEMP TABLE review."""
    tg, po, bio = _q(TEAM_GAMES), _q(PLAYER_OFFENSE_BOX), _q(PLAYER_BIO)
    idx = _q(os.path.join(DATA_LAKE, "raw", "pfr", "players", "player_index.parquet"))
    vq = _q(latest_v26())
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE unmapped AS
    SELECT regexp_extract(b.player_link_ids, '^([^,]+)', 1) AS pfr_id,
           ANY_VALUE(b.player) AS box_name,
           array_agg(DISTINCT struct_pack(team_code := g.team_code, year := g.year)) AS box_team_years
    FROM '{po}' b
    JOIN (SELECT DISTINCT boxscore_id, year, team_code FROM '{tg}'
          WHERE season_type = 'REG' AND year < 1978) g USING (boxscore_id)
    WHERE b.player_link_ids IS NOT NULL AND b.team = g.team_code
      AND NOT EXISTS (SELECT 1 FROM '{bio}' x
                      WHERE x.pfr_id = regexp_extract(b.player_link_ids, '^([^,]+)', 1))
    GROUP BY 1""")
    con.execute(f"""
    CREATE OR REPLACE TEMP TABLE cand AS
    WITH c AS (
      SELECT u.pfr_id, u.box_name, u.box_team_years,
             b.NFL_player_id, b.player AS bio_name,
             (b.NFL_player_id = u.pfr_id) AS id_string_match
      FROM unmapped u
      JOIN '{idx}' i USING (pfr_id)
      JOIN '{bio}' b ON LOWER(TRIM(b.player)) = LOWER(TRIM(u.box_name)) AND b.pfr_id IS NULL
        AND b.first_year <= TRY_CAST(i.last_year AS INT) + 1
        AND b.last_year >= TRY_CAST(i.first_year AS INT) - 1),
    tyo AS (  -- team-year overlap between the bio id's v26 rows and the pfr box appearances
      SELECT c.pfr_id, c.NFL_player_id, COUNT(*) AS overlap
      FROM c, UNNEST(c.box_team_years) AS t(bt)
      JOIN '{vq}' v ON v.NFL_player_id = c.NFL_player_id
        AND v.nfl_team = bt.team_code AND v.year = bt.year
      GROUP BY 1, 2)
    SELECT c.*, COALESCE(tyo.overlap, 0) AS team_year_overlap
    FROM c LEFT JOIN tyo USING (pfr_id, NFL_player_id)""")
    con.execute("""
    CREATE OR REPLACE TEMP TABLE links AS
    -- decisive evidence = id-string identity OR team-year overlap. A pfr_id links when
    -- EXACTLY ONE of its candidates is decisive (multi-candidate names tie-break cleanly);
    -- a bio row won by more than one pfr_id sends all its claimants to review.
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


def run(apply: bool = False) -> dict:
    con = duckdb.connect()
    con.execute("SET memory_limit='4GB'")
    _build_links(con)
    auto = con.execute("SELECT pfr_id, NFL_player_id, evidence FROM links").fetchall()
    auto = [(p, n, e) for p, n, e in auto if p not in MANUAL_LINKS]
    review_n = con.execute("SELECT COUNT(DISTINCT pfr_id) FROM review").fetchone()[0]
    con.execute(f"COPY review TO '{Path(REVIEW).as_posix()}' (HEADER)")

    all_links = auto + [(p, n, "manual_adjudication_20260710") for p, n in MANUAL_LINKS.items()]
    diag = {"auto_links": len(auto), "manual_links": len(MANUAL_LINKS),
            "manual_new_rows": len(MANUAL_NEW_ROWS), "sent_to_review": review_n,
            "review_csv": REVIEW}
    if not apply:
        con.close()
        return {"mode": "DRY-RUN", **diag}

    bio_p = Path(PLAYER_BIO.path)
    bq = _q(PLAYER_BIO)
    stamp = utc_stamp()
    con.execute("CREATE TEMP TABLE lk (pfr_id VARCHAR, nid VARCHAR)")
    con.executemany("INSERT INTO lk VALUES (?, ?)", [(p, n) for p, n, _ in all_links])
    # old values for facts (incl. the SmitJe25 correction)
    olds = dict(con.execute(f"""
        SELECT lk.nid, b.pfr_id FROM lk JOIN '{bq}' b ON b.NFL_player_id = lk.nid""").fetchall())

    new_row_sql = ", ".join(
        f"(SELECT '{r['NFL_player_id']}' AS NFL_player_id, '{r['pfr_id']}' AS pfr_id, "
        f"'{r['player']}' AS player, {r['first_year']} AS first_year, "
        f"{r['last_year']} AS last_year, '{r['nfl_position']}' AS nfl_position)"
        for r in MANUAL_NEW_ROWS).replace("), (", ") UNION ALL (")
    tmp = bio_p.with_name(bio_p.stem + "_w45.parquet")
    con.execute(f"""
        COPY (
          SELECT b.* REPLACE (COALESCE(lk.pfr_id, b.pfr_id) AS pfr_id)
          FROM '{bq}' b LEFT JOIN lk ON lk.nid = b.NFL_player_id
          UNION ALL BY NAME
          SELECT * FROM ({new_row_sql})
        ) TO '{Path(tmp).as_posix()}' (FORMAT PARQUET)""")

    tq = Path(tmp).as_posix()
    before = con.execute(f"SELECT COUNT(*) FROM '{bq}'").fetchone()[0]
    after = con.execute(f"SELECT COUNT(*) FROM '{tq}'").fetchone()[0]
    dup_pfr = con.execute(f"""
        SELECT COUNT(*) FROM (SELECT pfr_id FROM '{tq}' WHERE pfr_id IS NOT NULL
        GROUP BY 1 HAVING COUNT(*) > 1)""").fetchone()[0]
    linked = con.execute(f"""
        SELECT COUNT(*) FROM '{tq}' t JOIN lk ON lk.nid = t.NFL_player_id
        WHERE t.pfr_id = lk.pfr_id""").fetchone()[0]
    gate = (after == before + len(MANUAL_NEW_ROWS)) and dup_pfr == 0 \
        and linked == len(all_links)
    res = {"mode": "APPLY", **diag, "bio_before": before, "bio_after": after,
           "dup_pfr_after": dup_pfr, "links_applied": linked, "gate_pass": bool(gate)}
    if gate:
        from . import facts
        fc = facts.connect()
        snap = os.path.basename(os.path.dirname(_q(latest_v26())))
        for pfr_id, nid, ev in all_links:
            facts.emit_fact("cell_override", con=fc, table_name="player_bio",
                            target_key=nid, column_name="pfr_id",
                            old_value=olds.get(nid), new_value=pfr_id,
                            wave_id=WAVE, reason=f"identity link ({ev})",
                            witness="pfr_player_index+player_offense_box",
                            source_snapshot_id=snap)
        for r in MANUAL_NEW_ROWS:
            facts.emit_fact("row_add", con=fc, table_name="player_bio",
                            target_key=r["NFL_player_id"], row_json=json.dumps(r),
                            wave_id=WAVE, reason="player absent from bio; witnessed by pfr index + box",
                            witness="pfr_player_index+player_offense_box",
                            source_snapshot_id=snap)
        fc.close()
        bk = bio_p.with_name(bio_p.stem + f"_prew45_{stamp}.parquet")
        shutil.copy2(bio_p, bk)
        os.replace(tmp, bio_p)
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
    print(json.dumps(run(apply=a.apply), indent=2, default=str))
