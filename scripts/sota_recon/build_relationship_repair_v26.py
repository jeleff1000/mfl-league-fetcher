"""
sota_recon/build_relationship_repair_v26.py  --  O.7 slice 1: BAD-ROW REPAIR from the
relationship-verdict receipts (Joe's directive: "fix the bad rows").

The O.6 edge contract's counterexample receipts are a repair queue. This builder
enumerates the FULL violation sets for the four receipted classes, diagnoses every
violating row against the registered witnesses, and emits TYPED repair proposals with
per-row receipts. It is a DRY-RUN instrument: every proposal class here is either a
deletion or a value correction, so NOTHING applies without Joe's sign-off
(deletion discipline + §24.2 -- irreversible classes are Joe's).

Measured root causes (2026-07-26 probes, this session):

  LANE mirror_clone (R3 mirror holes, modern): the 13-22 violating team-weeks per
    mirror are NOT missing receiver rows -- they are DUPLICATE-CREDIT rows: the same
    game line exists under two NFL_player_ids on the same team-week (e.g. NWE 2005
    wk17 'ChilBr00' + '00-0023701' both 3rec/32yds -- bio carries a pfr-keyed
    DOB-less twin identity, so a box-insert wave minted a second row). pbp arbitrates:
    the rollup witnesses exactly ONE id of the pair. Two typed proposals:
      ROW_DELETE_CLONE        drop id is a bio-dup identity (pfr-keyed bio row,
                              no birth_date) -> the row itself is the clone
      CELL_ZERO_DOUBLE_CREDIT both ids are established identities (name-collision
                              double credit, e.g. J.Stevens/J.Gilmore TAM 2008 wk5)
                              -> zero the double-booked cells, keep the row
    Pairs where pbp witnesses BOTH or NEITHER id -> queue (never guessed).
    Extends build_clone_identity_merge_v26 (wave60): these pairs escaped its
    MIN_GAMES>=2 / MIN_MASS>=20 floors; the pbp witness proves 1-game pairs.

  LANE idp_credit (R4 team_idp_vertical def_interceptions holes): violating
    team-weeks carry an IDP INT credit that neither pbp (rollup) nor the PFR box
    defense table witnesses at that game -> OVER_CREDIT_UNWITNESSED (zero proposal);
    box-witnessed at a DIFFERENT week of the same season -> WRONG_WEEK queue
    (week-assignment adjudication); box witnesses it but pbp does not ->
    WITNESS_CONFLICT queue (e.g. D.Townsend PIT 2004 wk3). Team-row-under holes
    (team < IDP-sum is the modern shape; team > IDP-sum appears 1978-98) get the
    missing interceptor from pbp -> UNDER_CREDIT_FILL proposal (additive).

  LANE fum_rec_identity (R1 fum_rec = own + opp residuals): re-derive own/opp
    event counts from structured pbp (1999+) for exactly the violating rows, with
    the same attribution method the shipped fumble_recovery_opp column certified
    (gates PASS 2026-07-26). Recount closes the identity -> VALUE_CORRECTION
    proposal; recount cannot close it -> PBP_COVERAGE_GAP queue (deficit stated).
    1978-98 rows queue as REGEX_LANE_RECOUNT_PENDING (same method, regex lane).

  LANE receptions_buckets (R7 receptions = SUM(buckets) pbp-coverage rows):
    recompute the six buckets from pbp completions for the violating rows; if pbp
    reception count == stored receptions the buckets are recomputable ->
    VALUE_CORRECTION; else PBP_COVERAGE_GAP queue (pbp is missing plays -- the
    stored receptions root is the box, do not touch it).

Certification (§19.4 -- a fill method must reproduce known cells before touching
unknown ones): every lane re-runs its witness derivation on a deterministic
hash-sample of NON-violating rows and reports the agreement rate in the summary.

Receipts -> D:/league-history-data/nfl/derived/validation/sota_recon_master/
relationship_repair/ (proposals_*.csv, queue_*.csv, REPAIR_DRYRUN_SUMMARY.json).

    python -m scripts.sota_recon.build_relationship_repair_v26            # DRY RUN
    python -m scripts.sota_recon.build_relationship_repair_v26 --apply    # refuses
                       # without --signed-off-by Joe (deletion/correction classes)
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

import duckdb

from .sources import DATA_LAKE, PBP_MERGED, PBP_ROLLUP, TEAM_GAMES, latest_v26

BIO = "D:/league-history-data/nfl/ops_data/nfl_historical/player_bio.parquet"
PFR_DEF_BOX = "D:/league-history-data/nfl/raw/pfr/boxscores/tables/player_defense/_combined.parquet"
OUT_DIR = os.path.join(DATA_LAKE, "derived", "validation", "sota_recon_master",
                       "relationship_repair")
CERT_MOD = 97          # deterministic holdout sample: hash(player_week) % MOD == 0

BUCKETS = ["receptions_0_4", "receptions_5_9", "receptions_10_19",
           "receptions_20_29", "receptions_30_39", "receptions_40plus"]


def _q(s) -> str:
    return Path(getattr(s, "path", s)).as_posix()


def _csv(con, table: str, path: str) -> int:
    n = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    con.execute(f"COPY (SELECT * FROM {table}) TO '{Path(path).as_posix()}' (HEADER)")
    return n


# ---------------------------------------------------------------------------
# lane 1: mirror_clone -- duplicate-credit pairs behind the R3 mirror holes
# ---------------------------------------------------------------------------

# Violation anchors: (anchor_id, duplicated-line stat, opposite mirror side).
# The IDP anchor has no mirror side -- its delta is IDP-sum minus team-DEF row.
ANCHORS = [
    ("yards", "receiving_yards", "passing_yards"),
    ("receptions", "receptions", "completions"),
    ("tds", "receiving_tds", "passing_tds"),
    ("idp_int", "def_interceptions", None),
]


def lane_mirror_clone(con, v26: str) -> dict:
    """Violation-anchored duplicate-credit detector. A proposal exists ONLY where
    (a) a receipted team-week violation exists for an anchor, (b) a same-team-week
    pair carries an identical nonzero duplicated line, (c) removing the doomed line
    CLOSES the anchor's delta exactly, and (d) the pbp rollup witnesses exactly one
    id of the pair (1999+). Everything else queues -- a release-wide fingerprint
    sweep without the violation anchor drowns in mass-1 coincidence pairs
    (measured 2026-07-26: 8.8k pseudo-proposals; rejected)."""
    roll = _q(PBP_ROLLUP)
    # player-plane team-week sums + team-DEF row for every anchor stat
    con.execute(f"""CREATE OR REPLACE TEMP TABLE tw AS
        SELECT nfl_team, CAST(year AS INT) y, CAST(week AS INT) w, season_type st,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN receiving_yards END) s_receiving_yards,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN passing_yards END) s_passing_yards,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN receptions END) s_receptions,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN completions END) s_completions,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN receiving_tds END) s_receiving_tds,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN passing_tds END) s_passing_tds,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN def_interceptions END) s_def_interceptions,
               MAX(CASE WHEN position = 'DEF' THEN def_interceptions END) t_def_interceptions
        FROM '{v26}'
        WHERE nfl_team IS NOT NULL AND week IS NOT NULL
        GROUP BY 1, 2, 3, 4""")
    viol_arms, pair_arms = [], []
    for aid, dup, other in ANCHORS:
        delta = (f"s_{dup} - s_{other}" if other
                 else f"s_{dup} - t_{dup}")
        guard = (f"s_{dup} IS NOT NULL AND s_{other} IS NOT NULL" if other
                 else f"s_{dup} IS NOT NULL AND t_{dup} IS NOT NULL")
        viol_arms.append(f"""
        SELECT '{aid}' anchor, nfl_team, y, w, st, {delta} AS excess
        FROM tw WHERE {guard} AND ABS({delta}) > 1e-6""")
    con.execute("CREATE OR REPLACE TEMP TABLE anchor_viol AS "
                + " UNION ALL ".join(viol_arms))
    # duplicated-line pairs on violating team-weeks; the doomed line must equal the
    # excess exactly (closure) and be identical on both rows of the pair
    for aid, dup, other in ANCHORS:
        line_cols = {
            "yards": ["receptions", "receiving_yards"],
            "receptions": ["receptions", "receiving_yards"],
            "tds": ["receptions", "receiving_yards", "receiving_tds"],
            "idp_int": ["def_interceptions"],
        }[aid]
        eqs = " AND ".join(
            f"a.{c} IS NOT DISTINCT FROM b.{c}" for c in line_cols)
        pair_arms.append(f"""
        SELECT av.anchor, av.nfl_team, av.y, av.w, av.st, av.excess,
               a.player_week pw_a, b.player_week pw_b,
               a.NFL_player_id id_a, b.NFL_player_id id_b,
               a.player name_a, b.player name_b,
               a.{dup} dup_value
        FROM anchor_viol av
        JOIN '{v26}' a ON a.nfl_team = av.nfl_team AND CAST(a.year AS INT) = av.y
          AND CAST(a.week AS INT) = av.w AND a.season_type = av.st
          AND a.position IS DISTINCT FROM 'DEF'
        JOIN '{v26}' b ON b.nfl_team = av.nfl_team AND CAST(b.year AS INT) = av.y
          AND CAST(b.week AS INT) = av.w AND b.season_type = av.st
          AND b.position IS DISTINCT FROM 'DEF'
          AND a.NFL_player_id < b.NFL_player_id AND {eqs}
        WHERE av.anchor = '{aid}' AND a.{dup} IS NOT NULL AND a.{dup} != 0
          AND ABS(a.{dup} - av.excess) <= 1e-6""")
    con.execute("CREATE OR REPLACE TEMP TABLE pairs AS "
                + " UNION ALL ".join(pair_arms))
    # pbp arbitration (rollup row presence per id) + bio-dup identity evidence
    con.execute(f"""CREATE OR REPLACE TEMP TABLE arb AS
        SELECT p.*,
               ra.player_week IS NOT NULL AS wit_a,
               rb.player_week IS NOT NULL AS wit_b,
               COALESCE(ba.pfr_dup, FALSE) AS biodup_a,
               COALESCE(bb.pfr_dup, FALSE) AS biodup_b
        FROM pairs p
        LEFT JOIN '{roll}' ra ON ra.player_week = p.pw_a
        LEFT JOIN '{roll}' rb ON rb.player_week = p.pw_b
        LEFT JOIN (SELECT NFL_player_id id,
                          (NFL_player_id = pfr_id AND birth_date IS NULL) pfr_dup
                   FROM '{BIO}') ba ON ba.id = p.id_a
        LEFT JOIN (SELECT NFL_player_id id,
                          (NFL_player_id = pfr_id AND birth_date IS NULL) pfr_dup
                   FROM '{BIO}') bb ON bb.id = p.id_b""")
    # COLUMN NAMING (O.9, 2026-07-26). The pair is formed by an IDENTICAL STAT LINE on the
    # team-week, NOT by identity, so the surviving id is simply the other member of that
    # line-pair -- it may or may not be the same human. Naming it `witnessed_name` next to
    # `doom_name` read as a merge pair and was misread that way in review (the Ndukwe/
    # A.Jones case: two different players who each had exactly 1 INT that week). Renamed to
    # say what it is. Likewise `doom_is_bio_dup_identity` never meant "these two are the
    # same person" -- it means the DOOM id is a pfr-keyed, DOB-less STUB row, which is the
    # actual decision input, so it is named for that.
    con.execute("""CREATE OR REPLACE TEMP TABLE prop_mirror AS
        SELECT anchor, nfl_team, y, w, st, excess, dup_value,
               CASE WHEN wit_a THEN pw_b ELSE pw_a END AS doom_player_week,
               CASE WHEN wit_a THEN id_b ELSE id_a END AS doom_id,
               CASE WHEN wit_a THEN name_b ELSE name_a END AS doom_name,
               CASE WHEN wit_a THEN pw_a ELSE pw_b END AS matched_line_player_week,
               CASE WHEN wit_a THEN id_a ELSE id_b END AS matched_line_id,
               CASE WHEN wit_a THEN name_a ELSE name_b END AS matched_line_name,
               CASE WHEN wit_a THEN biodup_b ELSE biodup_a END
                    AS doom_is_pfr_keyed_stub_identity,
               CASE WHEN (CASE WHEN wit_a THEN biodup_b ELSE biodup_a END)
                    THEN 'DOOM_IS_PFR_KEYED_STUB' ELSE 'BOTH_ESTABLISHED_IDENTITIES' END
                    AS pair_identity_class,
               CASE WHEN (CASE WHEN wit_a THEN biodup_b ELSE biodup_a END)
                    THEN 'ROW_DELETE_CLONE' ELSE 'CELL_ZERO_DOUBLE_CREDIT' END AS proposal,
               CASE WHEN (CASE WHEN wit_a THEN biodup_b ELSE biodup_a END)
                    THEN 'DOOM row is a pfr-keyed DOB-less STUB identity whose line pbp '
                      || 'does not witness; an identical line on the same team-week IS '
                      || 'witnessed, and dropping the stub row closes the delta exactly. '
                      || 'The matched line need NOT be the same human -- the pair is by '
                      || 'identical stat line, and the stub-ness of the doom id is the '
                      || 'decision input.'
                    ELSE 'BOTH ids are ESTABLISHED identities (real players, DOB present), '
                      || 'so no row may be deleted. pbp witnesses only one of the two '
                      || 'identical lines; the unwitnessed one is double-booked credit and '
                      || 'its cells zero. REVIEW WITH CARE: this zeroes a real player''s '
                      || 'stat line.' END AS witness_basis
        FROM arb WHERE y >= 1999 AND (wit_a != wit_b)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE queue_mirror_pairs AS
        SELECT *, CASE WHEN y < 1999 THEN 'PRE_PBP_ARBITRATION_ERA'
                       WHEN wit_a AND wit_b THEN 'BOTH_IDS_PBP_WITNESSED'
                       ELSE 'NEITHER_ID_PBP_WITNESSED' END AS queue_reason
        FROM arb WHERE NOT (y >= 1999 AND (wit_a != wit_b))""")
    # violating team-weeks not explained by any proposal -> era-typed queue
    con.execute("""CREATE OR REPLACE TEMP TABLE queue_mirror_open AS
        SELECT av.*,
               CASE WHEN av.y < 1950 THEN 'ANCIENT_RECEIVING_COVERAGE_GAP'
                    WHEN av.y < 1999 THEN 'PRE_PBP_ARBITRATION_ERA'
                    ELSE 'NO_CLOSING_PAIR_FOUND' END AS queue_reason
        FROM anchor_viol av
        LEFT JOIN prop_mirror p ON p.anchor = av.anchor AND p.nfl_team = av.nfl_team
          AND p.y = av.y AND p.w = av.w AND p.st = av.st
        WHERE p.anchor IS NULL""")
    per_anchor = {a: dict(zip(("violating_team_weeks", "explained"), r))
                  for a, *r in con.execute("""
        SELECT av.anchor, COUNT(DISTINCT (av.nfl_team, av.y, av.w, av.st)),
               COUNT(DISTINCT (av.nfl_team, av.y, av.w, av.st))
                 FILTER (WHERE p.anchor IS NOT NULL)
        FROM anchor_viol av
        LEFT JOIN prop_mirror p ON p.anchor = av.anchor AND p.nfl_team = av.nfl_team
          AND p.y = av.y AND p.w = av.w AND p.st = av.st
        GROUP BY 1""").fetchall()}
    modern = {a: dict(zip(("violating_team_weeks", "explained"), r))
              for a, *r in con.execute("""
        SELECT av.anchor, COUNT(DISTINCT (av.nfl_team, av.y, av.w, av.st)),
               COUNT(DISTINCT (av.nfl_team, av.y, av.w, av.st))
                 FILTER (WHERE p.anchor IS NOT NULL)
        FROM anchor_viol av
        LEFT JOIN prop_mirror p ON p.anchor = av.anchor AND p.nfl_team = av.nfl_team
          AND p.y = av.y AND p.w = av.w AND p.st = av.st
        WHERE av.y >= 1999 GROUP BY 1""").fetchall()}
    return {
        "proposals": _csv(con, "prop_mirror", os.path.join(OUT_DIR, "proposals_mirror_clone.csv")),
        "queued_pairs": _csv(con, "queue_mirror_pairs",
                             os.path.join(OUT_DIR, "queue_mirror_clone_pairs.csv")),
        "queued_open_team_weeks": _csv(con, "queue_mirror_open",
                                       os.path.join(OUT_DIR, "queue_mirror_open_team_weeks.csv")),
        "per_anchor_all_eras": per_anchor,
        "per_anchor_modern": modern,
    }


# ---------------------------------------------------------------------------
# lane 2: idp_credit -- def_interceptions IDP<->team-DEF attribution holes
# ---------------------------------------------------------------------------

def lane_idp_credit(con, v26: str) -> dict:
    roll, tg, dbox = _q(PBP_ROLLUP), _q(TEAM_GAMES), PFR_DEF_BOX
    con.execute(f"""CREATE OR REPLACE TEMP TABLE idp_tw AS
        SELECT nfl_team, CAST(year AS INT) y, CAST(week AS INT) w, season_type st,
               MAX(CASE WHEN position='DEF' THEN def_interceptions END) t_int,
               SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN def_interceptions END) i_int,
               MAX(CASE WHEN position='DEF' THEN CAST(nfl_franchise_number AS INT) END) fr
        FROM '{v26}'
        WHERE nfl_team IS NOT NULL AND week IS NOT NULL AND year >= 1978
        GROUP BY 1,2,3,4
        HAVING MAX(CASE WHEN position='DEF' THEN def_interceptions END) IS NOT NULL
           AND SUM(CASE WHEN position IS DISTINCT FROM 'DEF' THEN def_interceptions END)
               IS NOT NULL
           AND ABS(t_int - i_int) > 1e-6""")
    # every nonzero IDP INT cell on a violating team-week, with its witness votes
    con.execute(f"""CREATE OR REPLACE TEMP TABLE idp_cells AS
        SELECT v.player_week, v.NFL_player_id, v.player, tw.nfl_team, tw.y, tw.w, tw.st,
               tw.t_int, tw.i_int, v.def_interceptions v26_int,
               r.def_interceptions roll_int
        FROM idp_tw tw
        JOIN '{v26}' v ON v.nfl_team = tw.nfl_team AND CAST(v.year AS INT) = tw.y
          AND CAST(v.week AS INT) = tw.w AND v.season_type = tw.st
          AND v.position IS DISTINCT FROM 'DEF' AND COALESCE(v.def_interceptions, 0) != 0
        LEFT JOIN '{roll}' r ON r.player_week = v.player_week""")
    # PFR box vote: def_int at the SAME game (boxscore matched on team_fid+y+w+st)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE box_vote AS
        SELECT c.player_week,
               MAX(CASE WHEN g.year = c.y AND CAST(g.week AS INT) = c.w
                        AND g.season_type = c.st
                        THEN TRY_CAST(d.def_int AS DOUBLE) END) box_int_same_week,
               MAX(CASE WHEN NOT (g.year = c.y AND CAST(g.week AS INT) = c.w
                        AND g.season_type = c.st) AND TRY_CAST(d.def_int AS DOUBLE) > 0
                        THEN g.week END) box_int_other_week
        FROM idp_cells c
        JOIN '{BIO}' b ON b.NFL_player_id = c.NFL_player_id AND b.pfr_id IS NOT NULL
        JOIN '{dbox}' d ON split_part(d.player_link_ids, ';', 1) = b.pfr_id
          AND CAST(d.season AS INT) = c.y
        JOIN '{tg}' g ON g.boxscore_id = d.boxscore_id AND g.team_code = d.team
        GROUP BY 1""")
    con.execute("""CREATE OR REPLACE TEMP TABLE idp_typed AS
        SELECT c.*, bv.box_int_same_week, bv.box_int_other_week,
          CASE
            WHEN c.i_int > c.t_int AND COALESCE(c.roll_int, 0) = 0
                 AND COALESCE(bv.box_int_same_week, 0) = 0
                 AND bv.box_int_other_week IS NOT NULL THEN 'WRONG_WEEK'
            WHEN c.i_int > c.t_int AND COALESCE(c.roll_int, 0) = 0
                 AND COALESCE(bv.box_int_same_week, 0) = 0
                 THEN 'OVER_CREDIT_UNWITNESSED'
            WHEN c.i_int > c.t_int AND COALESCE(c.roll_int, 0) = 0
                 AND COALESCE(bv.box_int_same_week, 0) > 0 THEN 'WITNESS_CONFLICT'
            ELSE 'CELL_WITNESSED_OK'
          END AS diagnosis
        FROM idp_cells c LEFT JOIN box_vote bv USING (player_week)""")
    con.execute("""CREATE OR REPLACE TEMP TABLE prop_idp AS
        SELECT player_week, NFL_player_id, player, nfl_team, y, w, st,
               v26_int old_value, 0 new_value, 'def_interceptions' AS column_name,
               'OVER_CREDIT_UNWITNESSED' proposal,
               'pbp rollup 0/absent AND pfr box defense 0 at the matched boxscore'
                 AS witness_basis
        FROM idp_typed WHERE diagnosis = 'OVER_CREDIT_UNWITNESSED'""")
    con.execute("""CREATE OR REPLACE TEMP TABLE queue_idp AS
        SELECT * FROM idp_typed WHERE diagnosis IN ('WRONG_WEEK', 'WITNESS_CONFLICT')""")
    # reverse direction (team > IDP sum): missing interceptor witnessed by pbp
    con.execute(f"""CREATE OR REPLACE TEMP TABLE prop_idp_fill AS
        SELECT r.player_week, r.NFL_player_id, r.player, tw.nfl_team, tw.y, tw.w, tw.st,
               v.def_interceptions old_value, r.def_interceptions new_value,
               'def_interceptions' AS column_name, 'UNDER_CREDIT_FILL' proposal,
               'pbp rollup witnesses INT(s) for a player on a team-week where the '
                 || 'team-DEF row exceeds the IDP sum' AS witness_basis
        FROM idp_tw tw
        JOIN '{v26}' v ON v.nfl_team = tw.nfl_team AND CAST(v.year AS INT) = tw.y
          AND CAST(v.week AS INT) = tw.w AND v.season_type = tw.st
          AND v.position IS DISTINCT FROM 'DEF'
        JOIN '{roll}' r ON r.player_week = v.player_week
          AND COALESCE(r.def_interceptions, 0) > COALESCE(v.def_interceptions, 0)
        WHERE tw.t_int > tw.i_int""")
    # team>IDP holes the fill lane cannot close -> open queue with the deficit stated
    con.execute("""CREATE OR REPLACE TEMP TABLE queue_idp_open AS
        SELECT tw.*, 'TEAM_ROW_EXCEEDS_IDP_SUM_NO_PBP_INTERCEPTOR' AS queue_reason
        FROM idp_tw tw
        LEFT JOIN prop_idp_fill f ON f.nfl_team = tw.nfl_team AND f.y = tw.y
          AND f.w = tw.w AND f.st = tw.st
        WHERE tw.t_int > tw.i_int AND f.nfl_team IS NULL""")
    # certification: rollup vs v26 agreement on clean nonzero IDP INT cells (holdout)
    cert = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE ABS(COALESCE(r.def_interceptions,0)
                                                     - v.def_interceptions) <= 1e-6)
        FROM '{v26}' v JOIN '{roll}' r USING (player_week)
        WHERE v.position IS DISTINCT FROM 'DEF' AND COALESCE(v.def_interceptions,0) != 0
          AND CAST(v.year AS INT) >= 1999 AND hash(v.player_week) % {CERT_MOD} = 0
        """).fetchone()
    return {
        "violating_team_weeks": con.execute("SELECT COUNT(*) FROM idp_tw").fetchone()[0],
        "proposals_zero": _csv(con, "prop_idp", os.path.join(OUT_DIR, "proposals_idp_over_credit.csv")),
        "proposals_fill": _csv(con, "prop_idp_fill", os.path.join(OUT_DIR, "proposals_idp_under_credit.csv")),
        "queued": _csv(con, "queue_idp", os.path.join(OUT_DIR, "queue_idp_credit.csv")),
        "queued_open_team_weeks": _csv(con, "queue_idp_open",
                                       os.path.join(OUT_DIR, "queue_idp_open_team_weeks.csv")),
        "certification_rollup_agreement_on_clean_sample":
            {"n": cert[0], "agree": cert[1],
             "rate": round(cert[1] / cert[0], 4) if cert[0] else None},
    }


# ---------------------------------------------------------------------------
# lane 3: fum_rec_identity -- fum_rec = own + opp residual rows
# ---------------------------------------------------------------------------

def lane_fum_rec(con, v26: str) -> dict:
    pbp = _q(PBP_MERGED)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE fr_viol AS
        SELECT player_week, NFL_player_id, CAST(year AS INT) y, CAST(week AS INT) w,
               season_type st, fum_rec, fumble_recovery_own own_v,
               fumble_recovery_opp opp_v
        FROM '{v26}'
        WHERE position IS DISTINCT FROM 'DEF'
          AND fum_rec IS NOT NULL AND fumble_recovery_own IS NOT NULL
          AND fumble_recovery_opp IS NOT NULL
          AND ABS(fum_rec - (fumble_recovery_own + fumble_recovery_opp)) > 1e-6""")
    # structured pbp recount (1999+), both slots, own AND opp, violating weeks only
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbp_rec AS
        SELECT nid, yr, wk, stp,
               SUM(CASE WHEN own THEN 1 ELSE 0 END) own_ct,
               SUM(CASE WHEN NOT own THEN 1 ELSE 0 END) opp_ct
        FROM (
          SELECT fumble_recovery_1_player_id nid, CAST(season AS INT) yr,
                 CAST(week AS INT) wk, season_type stp,
                 fumble_recovery_1_team = fumbled_1_team AS own
          FROM '{pbp}'
          WHERE CAST(season AS INT) >= 1999 AND fumble_recovery_1_player_id IS NOT NULL
            AND fumble_recovery_1_team IS NOT NULL AND fumbled_1_team IS NOT NULL
          UNION ALL
          SELECT fumble_recovery_2_player_id, CAST(season AS INT), CAST(week AS INT),
                 season_type,
                 fumble_recovery_2_team = COALESCE(fumbled_2_team, fumbled_1_team)
          FROM '{pbp}'
          WHERE CAST(season AS INT) >= 1999 AND fumble_recovery_2_player_id IS NOT NULL
            AND fumble_recovery_2_team IS NOT NULL
            AND COALESCE(fumbled_2_team, fumbled_1_team) IS NOT NULL)
        GROUP BY 1, 2, 3, 4""")
    con.execute("""CREATE OR REPLACE TEMP TABLE fr_typed AS
        SELECT v.*, COALESCE(p.own_ct, 0) pbp_own, COALESCE(p.opp_ct, 0) pbp_opp,
          CASE WHEN v.y < 1999 THEN 'REGEX_LANE_RECOUNT_PENDING'
               WHEN ABS(v.fum_rec - (COALESCE(p.own_ct,0) + COALESCE(p.opp_ct,0))) <= 1e-6
                 THEN 'VALUE_CORRECTION'
               ELSE 'PBP_COVERAGE_GAP' END AS diagnosis
        FROM fr_viol v LEFT JOIN pbp_rec p ON p.nid = v.NFL_player_id
          AND p.yr = v.y AND p.wk = v.w AND p.stp = v.st""")
    con.execute("""CREATE OR REPLACE TEMP TABLE prop_fr AS
        SELECT player_week, NFL_player_id, y, w, st, fum_rec,
               own_v old_own, pbp_own new_own, opp_v old_opp, pbp_opp new_opp,
               'VALUE_CORRECTION' proposal,
               'structured pbp slot-paired recovery recount closes fum_rec=own+opp '
                 || '(same method as the certified fumble_recovery_opp build)'
                 AS witness_basis
        FROM fr_typed WHERE diagnosis = 'VALUE_CORRECTION'""")
    con.execute("""CREATE OR REPLACE TEMP TABLE queue_fr AS
        SELECT * FROM fr_typed WHERE diagnosis != 'VALUE_CORRECTION'""")
    # certification: recount reproduces own+opp on clean rows (holdout sample)
    cert = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE
            ABS(COALESCE(p.own_ct,0) - v.fumble_recovery_own) <= 1e-6
            AND ABS(COALESCE(p.opp_ct,0) - v.fumble_recovery_opp) <= 1e-6)
        FROM '{v26}' v LEFT JOIN pbp_rec p ON p.nid = v.NFL_player_id
          AND p.yr = CAST(v.year AS INT) AND p.wk = CAST(v.week AS INT)
          AND p.stp = v.season_type
        WHERE v.position IS DISTINCT FROM 'DEF' AND CAST(v.year AS INT) >= 1999
          AND v.fum_rec IS NOT NULL AND v.fumble_recovery_own IS NOT NULL
          AND v.fumble_recovery_opp IS NOT NULL
          AND ABS(v.fum_rec - (v.fumble_recovery_own + v.fumble_recovery_opp)) <= 1e-6
          AND COALESCE(v.fum_rec, 0) != 0
          AND hash(v.player_week) % {CERT_MOD} = 0""").fetchone()
    by_diag = dict(con.execute(
        "SELECT diagnosis, COUNT(*) FROM fr_typed GROUP BY 1").fetchall())
    return {
        "violating_rows": con.execute("SELECT COUNT(*) FROM fr_viol").fetchone()[0],
        "by_diagnosis": by_diag,
        "proposals": _csv(con, "prop_fr", os.path.join(OUT_DIR, "proposals_fum_rec_identity.csv")),
        "queued": _csv(con, "queue_fr", os.path.join(OUT_DIR, "queue_fum_rec_identity.csv")),
        "certification_recount_on_clean_nonzero_sample":
            {"n": cert[0], "agree": cert[1],
             "rate": round(cert[1] / cert[0], 4) if cert[0] else None},
    }


# ---------------------------------------------------------------------------
# lane 4: receptions_buckets -- receptions = SUM(six buckets) coverage rows
# ---------------------------------------------------------------------------

def lane_receptions_buckets(con, v26: str) -> dict:
    pbp = _q(PBP_MERGED)
    bsum = " + ".join(BUCKETS)
    con.execute(f"""CREATE OR REPLACE TEMP TABLE rb_viol AS
        SELECT player_week, NFL_player_id, CAST(year AS INT) y, CAST(week AS INT) w,
               season_type st, receptions, {', '.join(BUCKETS)}
        FROM '{v26}'
        WHERE receptions IS NOT NULL
          AND {' AND '.join(c + ' IS NOT NULL' for c in BUCKETS)}
          AND ABS(receptions - ({bsum})) > 1e-6""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE pbp_b AS
        SELECT receiver_player_id nid, CAST(season AS INT) yr, CAST(week AS INT) wk,
               season_type stp, COUNT(*) pbp_rec,
               COUNT(*) FILTER (WHERE receiving_yards <= 4) b0,
               COUNT(*) FILTER (WHERE receiving_yards BETWEEN 5 AND 9) b5,
               COUNT(*) FILTER (WHERE receiving_yards BETWEEN 10 AND 19) b10,
               COUNT(*) FILTER (WHERE receiving_yards BETWEEN 20 AND 29) b20,
               COUNT(*) FILTER (WHERE receiving_yards BETWEEN 30 AND 39) b30,
               COUNT(*) FILTER (WHERE receiving_yards >= 40) b40
        FROM '{pbp}'
        WHERE complete_pass = 1 AND receiver_player_id IS NOT NULL
        GROUP BY 1, 2, 3, 4""")
    con.execute("""CREATE OR REPLACE TEMP TABLE rb_typed AS
        SELECT v.*, p.pbp_rec, p.b0, p.b5, p.b10, p.b20, p.b30, p.b40,
          CASE WHEN p.nid IS NULL THEN 'PBP_COVERAGE_GAP'
               WHEN ABS(v.receptions - p.pbp_rec) <= 1e-6 THEN 'VALUE_CORRECTION'
               ELSE 'PBP_COVERAGE_GAP' END AS diagnosis,
          CASE WHEN p.nid IS NOT NULL AND v.receptions < 0 THEN 'NEGATIVE_STORED' END flag
        FROM rb_viol v LEFT JOIN pbp_b p ON p.nid = v.NFL_player_id
          AND p.yr = v.y AND p.wk = v.w AND p.stp = v.st""")
    con.execute(f"""CREATE OR REPLACE TEMP TABLE prop_rb AS
        SELECT player_week, NFL_player_id, y, w, st, receptions,
               {', '.join(BUCKETS)}, b0 new_0_4, b5 new_5_9, b10 new_10_19,
               b20 new_20_29, b30 new_30_39, b40 new_40plus,
               'VALUE_CORRECTION' proposal,
               'pbp reception count equals stored receptions; the six buckets '
                 || 'recompute exactly from the completed-pass yardage' AS witness_basis
        FROM rb_typed WHERE diagnosis = 'VALUE_CORRECTION'""")
    con.execute("""CREATE OR REPLACE TEMP TABLE queue_rb AS
        SELECT * FROM rb_typed WHERE diagnosis != 'VALUE_CORRECTION'""")
    # certification: bucket recompute matches stored buckets on clean rows (holdout)
    cert = con.execute(f"""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE
            p.b0 = v.receptions_0_4 AND p.b5 = v.receptions_5_9
            AND p.b10 = v.receptions_10_19 AND p.b20 = v.receptions_20_29
            AND p.b30 = v.receptions_30_39 AND p.b40 = v.receptions_40plus)
        FROM '{v26}' v JOIN pbp_b p ON p.nid = v.NFL_player_id
          AND p.yr = CAST(v.year AS INT) AND p.wk = CAST(v.week AS INT)
          AND p.stp = v.season_type
        WHERE v.receptions IS NOT NULL AND v.receptions = p.pbp_rec
          AND {' AND '.join('v.' + c + ' IS NOT NULL' for c in BUCKETS)}
          AND ABS(v.receptions - ({' + '.join('v.' + c for c in BUCKETS)})) <= 1e-6
          AND hash(v.player_week) % {CERT_MOD} = 0""").fetchone()
    by_diag = dict(con.execute(
        "SELECT diagnosis, COUNT(*) FROM rb_typed GROUP BY 1").fetchall())
    return {
        "violating_rows": con.execute("SELECT COUNT(*) FROM rb_viol").fetchone()[0],
        "by_diagnosis": by_diag,
        "proposals": _csv(con, "prop_rb", os.path.join(OUT_DIR, "proposals_receptions_buckets.csv")),
        "queued": _csv(con, "queue_rb", os.path.join(OUT_DIR, "queue_receptions_buckets.csv")),
        "certification_bucket_recompute_on_clean_sample":
            {"n": cert[0], "agree": cert[1],
             "rate": round(cert[1] / cert[0], 4) if cert[0] else None},
    }


# ---------------------------------------------------------------------------

def run() -> dict:
    os.makedirs(OUT_DIR, exist_ok=True)
    v26 = _q(latest_v26())
    con = duckdb.connect()
    con.execute("SET memory_limit='6GB'")
    con.execute("PRAGMA threads=4")
    out = {
        "mode": "DRY-RUN (all proposal classes are deletion/value-correction -> "
                "Joe sign-off required before any apply; §24.2 / deletion discipline)",
        "release": v26,
        "out_dir": OUT_DIR,
        "mirror_clone": lane_mirror_clone(con, v26),
        "idp_credit": lane_idp_credit(con, v26),
        "fum_rec_identity": lane_fum_rec(con, v26),
        "receptions_buckets": lane_receptions_buckets(con, v26),
    }
    from .recon_common import utc_stamp
    out["generated_utc"] = utc_stamp()
    with open(os.path.join(OUT_DIR, "REPAIR_DRYRUN_SUMMARY.json"), "w",
              encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)
    con.close()
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--signed-off-by", default="")
    a = ap.parse_args()
    if a.apply:
        print("REFUSED: every proposal class in this builder is a deletion or value "
              "correction. Apply requires Joe's receipted sign-off per class "
              "(deletion discipline; master plan §24.2). Run the dry run, review "
              "the proposal CSVs, then implement the signed apply lane.")
        return 1
    r = run()
    print(json.dumps(r, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
