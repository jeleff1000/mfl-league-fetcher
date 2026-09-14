"""
sota_recon/recon_internal.py  --  LANE: internal consistency

Two tiers, kept separate so real bugs don't drown (the lesson from the old
game_stat_consistency_audit's 70% flag rate):

  HARD  - physically impossible within a single row/game. These are ALWAYS bugs,
          regardless of era. completions>attempts, fg_made>fg_att, receptions>targets,
          made+missed>att, impossible negative yardage. Any HARD hit fails the lane.

  CROSS - cross-position / cross-team identities that must hold where data exists:
          QB INTs thrown == opposing DEF INTs caught (same game),
          team sacks == opposing QB sacks-suffered (1982+),
          team completions ~ team receptions, team passing yds ~ team receiving yds.
          Tolerances + era scoping from asymmetry_registry.

  SOFT  - completeness signals (no QB attempts in a covered game, modern IDP absent,
          etc.) reported for context only, era-scoped via EXPECTED_GAPS, never counted
          as failures.

Outputs:
  internal_hard_violations.csv   - every impossible row (should be ZERO)
  internal_cross_flags.csv       - cross-side identity failures beyond tolerance
  internal_summary.csv           - counts per check, per era
  manifest.json
"""

from __future__ import annotations

import os

from pathlib import Path

from .asymmetry_registry import tol
from . import facts
from .recon_common import canon_team_sql, connect, dump_csv, era_of, lane_dir, source_pin, write_manifest
from .sources import registry

LANE = "internal_consistency"


def _load_documented_attempt_gaps(db_path=None):
    """Return adjudicated NULL-attempt overrides that are source gaps, not defects."""
    path = Path(db_path or facts.FACTS_DB)
    if not path.is_file():
        return []
    c = facts.connect(str(path))
    try:
        return [tuple(row) for row in c.execute(
            """
                SELECT target_key, fact_id, wave_id, reason, witness, source_snapshot_id
                FROM cell_override
                WHERE table_name = 'nfl_player_stats_all'
                  AND column_name = 'attempts'
                  AND new_value = 'NULL'
                ORDER BY target_key, created_at
                """
        ).fetchall()]
    finally:
        c.close()


def run(run_dir: str) -> dict:
    reg = registry()
    v26 = reg["v26_release"].path
    out = lane_dir(run_dir, LANE)
    con = connect()
    ct = canon_team_sql

    # Adjudicated NULL-attempt source gaps (cell_override facts): typed exemptions carrying
    # fact_id/wave_id/reason/witness receipts. Exempted rows leave the HARD lane and land in
    # internal_source_gap_exceptions.csv instead -- reported, never silently dropped.
    # (Fold-in 2026-07-25 of the newer salvaged marathon version, exact port.)
    con.execute("""CREATE TEMP TABLE documented_attempt_gaps (
               player_week VARCHAR,
               fact_id VARCHAR,
               wave_id VARCHAR,
               reason VARCHAR,
               witness VARCHAR,
               source_snapshot_id VARCHAR
           )""")
    for row in _load_documented_attempt_gaps():
        con.execute("INSERT INTO documented_attempt_gaps VALUES (?, ?, ?, ?, ?, ?)", list(row))

    # ---- HARD: physically impossible single-row conditions -------------------------
    # Each UNION arm tags the violation type. Only compares where the relevant cols are
    # non-null (a null is a coverage question, handled elsewhere).
    hard_sql = f"""
        SELECT player_week, NFL_player_id, year, position, 'completions_gt_attempts' AS violation,
               completions AS a, attempts AS b
        FROM '{v26}' WHERE completions IS NOT NULL AND attempts IS NOT NULL AND completions > attempts
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'receptions_gt_targets', receptions, targets
        FROM '{v26}' WHERE receptions IS NOT NULL AND targets IS NOT NULL AND year >= 1978 AND receptions > targets
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'fg_made_gt_att', fg_made, fg_att
        FROM '{v26}' WHERE fg_made IS NOT NULL AND fg_att IS NOT NULL AND fg_made > fg_att
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'pat_made_gt_att', pat_made, pat_att
        FROM '{v26}' WHERE pat_made IS NOT NULL AND pat_att IS NOT NULL AND pat_made > pat_att
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'fg_made_plus_missed_gt_att',
               COALESCE(fg_made,0)+COALESCE(fg_missed,0), fg_att
        FROM '{v26}' WHERE fg_att IS NOT NULL AND (COALESCE(fg_made,0)+COALESCE(fg_missed,0)) > fg_att + COALESCE(fg_blocked,0)
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'completions_no_attempts', completions, attempts
        FROM '{v26}' v
        WHERE completions > 0 AND (attempts IS NULL OR attempts = 0)
          AND NOT EXISTS (
              SELECT 1 FROM documented_attempt_gaps g WHERE g.player_week = v.player_week
          )
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'receptions_no_targets_modern', receptions, targets
        FROM '{v26}' WHERE year >= 1978 AND receptions > 0 AND (targets IS NULL OR targets = 0)
    """
    hard_n = dump_csv(con, hard_sql + " ORDER BY year, violation",
                      os.path.join(out, "internal_hard_violations.csv"))

    # exempted rows: documented source gaps with their adjudication receipts
    dump_csv(con, f"""
        SELECT v.player_week, v.NFL_player_id, v.year, v.position,
               'documented_attempt_source_gap' AS gap_type,
               v.completions, v.attempts,
               g.fact_id, g.wave_id, g.reason, g.witness, g.source_snapshot_id
        FROM '{v26}' v
        JOIN documented_attempt_gaps g ON g.player_week = v.player_week
        WHERE v.completions > 0 AND (v.attempts IS NULL OR v.attempts = 0)
        ORDER BY v.year, v.player_week
    """, os.path.join(out, "internal_source_gap_exceptions.csv"))

    # per-type hard counts
    con.execute(f"CREATE TEMP TABLE hard AS {hard_sql}")
    hard_by_type = dict(con.execute(
        "SELECT violation, COUNT(*) FROM hard GROUP BY violation ORDER BY 2 DESC"
    ).fetchall())

    # ---- STRUCTURAL: composition identities, monotonic bounds, era gates ------------
    # These need NO external source and hold in EVERY era. A violation is a real bug.
    # Guarded so they only fire where the component set is actually populated.
    fgb = "fg_made_0_19,fg_made_20_29,fg_made_30_39,fg_made_40_49,fg_made_50_59,fg_made_60_".split(",")
    fg_sum = "+".join(f"COALESCE({x},0)" for x in fgb)
    fg_present = " OR ".join(f"{x} IS NOT NULL" for x in fgb)
    rng = "receptions_0_4,receptions_5_9,receptions_10_19,receptions_20_29,receptions_30_39,receptions_40plus".split(",")
    rng_sum = "+".join(f"COALESCE({x},0)" for x in rng)
    rng_present = " AND ".join(f"{x} IS NOT NULL" for x in rng)
    # NOTE: bucket sums can legitimately fall SHORT of the total (negative-yardage plays
    # land in no positive-yardage bucket), so we flag only OVERCOUNT (sum > total), which is
    # physically impossible. Likewise YAC can exceed receiving_yards (negative air yards), so
    # there is no YAC<=receiving bound.
    struct_sql = f"""
        SELECT player_week, NFL_player_id, year, position, 'fg_buckets_sum>fg_made' AS violation,
               {fg_sum} AS a, fg_made AS b
        FROM '{v26}' WHERE fg_made IS NOT NULL AND ({fg_present}) AND {fg_sum} > fg_made
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'reception_ranges_sum>receptions',
               {rng_sum}, receptions
        FROM '{v26}' WHERE receptions IS NOT NULL AND ({rng_present}) AND {rng_sum} > receptions
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'pat_components>pat_att',
               COALESCE(pat_made,0)+COALESCE(pat_missed,0)+COALESCE(pat_blocked,0), pat_att
        FROM '{v26}' WHERE pat_att IS NOT NULL AND pat_made IS NOT NULL
               AND COALESCE(pat_made,0)+COALESCE(pat_missed,0)+COALESCE(pat_blocked,0) > pat_att
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'two_point_before_1994',
               COALESCE(passing_2pt_conversions,0)+COALESCE(rushing_2pt_conversions,0)+COALESCE(receiving_2pt_conversions,0), 0
        FROM '{v26}' WHERE year < 1994
               AND COALESCE(passing_2pt_conversions,0)+COALESCE(rushing_2pt_conversions,0)+COALESCE(receiving_2pt_conversions,0) > 0
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'completions_40plus>completions', completions_40plus, completions
        FROM '{v26}' WHERE completions IS NOT NULL AND completions_40plus IS NOT NULL AND completions_40plus > completions
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'receptions_40plus>receptions', receptions_40plus, receptions
        FROM '{v26}' WHERE receptions IS NOT NULL AND receptions_40plus IS NOT NULL AND receptions_40plus > receptions
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'passing_tds_50plus>passing_tds_40plus', passing_tds_50plus, passing_tds_40plus
        FROM '{v26}' WHERE passing_tds_40plus IS NOT NULL AND passing_tds_50plus IS NOT NULL AND passing_tds_50plus > passing_tds_40plus
        UNION ALL
        SELECT player_week, NFL_player_id, year, position, 'punt_long>punt_yards', punt_long, punt_yards
        FROM '{v26}' WHERE punt_yards IS NOT NULL AND punt_long IS NOT NULL AND punts IS NOT NULL AND punts>1 AND punt_long > punt_yards
    """
    struct_n = dump_csv(con, struct_sql + " ORDER BY violation, year",
                        os.path.join(out, "internal_structural_violations.csv"))
    con.execute(f"CREATE TEMP TABLE struct AS {struct_sql}")
    struct_by_type = dict(con.execute(
        "SELECT violation, COUNT(*) FROM struct GROUP BY violation ORDER BY 2 DESC"
    ).fetchall())

    # ---- CROSS: cross-team / cross-position identities ------------------------------
    # build team-game aggregates once
    con.execute(f"""
        CREATE TEMP VIEW tg AS
        SELECT year, CAST(week AS INTEGER) AS week,
               nfl_franchise_number AS team_c, opponent_nfl_franchise_number AS opp_c,
               {era_of('year')} AS era,
               SUM(COALESCE(passing_interceptions,0)) AS qb_int_thrown,
               SUM(COALESCE(passing_yards,0)) AS team_pass_yds,
               SUM(COALESCE(receiving_yards,0)) AS team_recv_yds,
               SUM(COALESCE(completions,0)) AS team_comp,
               SUM(COALESCE(receptions,0)) AS team_rec,
               SUM(COALESCE(sacks_suffered,0)) AS team_sacks_suffered
        FROM '{v26}'
        -- count EVERY individual offensive contributor, not just QB/RB/WR/TE: punter-passers
        -- (Tom Tupa P), holders, and trick-play laterals all contribute real completions/receptions/yards.
        -- Restricting to skill positions falsely flagged comp_rec/pass_recv (team comp==rec is true at
        -- all-position grain 99.5% of modern team-games; the prior gaps were this filter, not the data).
        WHERE COALESCE(position,'') <> 'DEF'
        GROUP BY 1,2,3,4
    """)
    # defense side: INTs caught and sacks by the opposing DEF row.
    # SUM over an all-NULL group returns NULL -> distinguishes "DEF side unpopulated"
    # (coverage gap, a backfill candidate) from "DEF side present but disagrees" (a real bug).
    con.execute(f"""
        CREATE TEMP VIEW dg AS
        SELECT year, CAST(week AS INTEGER) AS week,
               nfl_franchise_number AS team_c, opponent_nfl_franchise_number AS opp_c,
               SUM(def_interceptions) AS def_int_caught,
               SUM(def_sacks) AS def_sacks
        FROM '{v26}'
        WHERE position = 'DEF'
        GROUP BY 1,2,3,4
    """)

    t_int = tol("int_symmetry")
    t_sack = tol("sack_symmetry")
    t_passrecv = tol("pass_recv_yards")

    # INT symmetry: team A QB INTs thrown == team B DEF INTs caught (opp DEF row).
    # ONLY where the DEF side is populated -> a true mismatch, not a coverage gap.
    cross_sql = f"""
        SELECT 'int_symmetry' AS check_name, o.year, o.week, o.team_c, o.opp_c, o.era,
               o.qb_int_thrown AS subject_side, d.def_int_caught AS counter_side,
               ABS(o.qb_int_thrown - d.def_int_caught) AS resid
        FROM tg o
        JOIN dg d ON d.year=o.year AND d.week=o.week AND d.team_c=o.opp_c AND d.opp_c=o.team_c
        WHERE d.def_int_caught IS NOT NULL AND ABS(o.qb_int_thrown - d.def_int_caught) > {t_int}

        UNION ALL
        SELECT 'sack_symmetry', o.year, o.week, o.team_c, o.opp_c, o.era,
               o.team_sacks_suffered, d.def_sacks,
               ABS(o.team_sacks_suffered - d.def_sacks)
        FROM tg o
        JOIN dg d ON d.year=o.year AND d.week=o.week AND d.team_c=o.opp_c AND d.opp_c=o.team_c
        WHERE o.year >= 1982 AND d.def_sacks IS NOT NULL
          AND ABS(o.team_sacks_suffered - d.def_sacks) > {t_sack}

        UNION ALL
        SELECT 'pass_recv_yards', year, week, team_c, opp_c, era,
               team_pass_yds, team_recv_yds, ABS(team_pass_yds - team_recv_yds)
        FROM tg
        WHERE (team_pass_yds > 0 OR team_recv_yds > 0)
          AND ABS(team_pass_yds - team_recv_yds) > {t_passrecv}

        UNION ALL
        SELECT 'comp_rec', year, week, team_c, opp_c, era,
               team_comp, team_rec, ABS(team_comp - team_rec)
        FROM tg
        WHERE (team_comp > 0 OR team_rec > 0) AND ABS(team_comp - team_rec) > 0
    """
    cross_n = dump_csv(con, cross_sql + " ORDER BY check_name, resid DESC",
                       os.path.join(out, "internal_cross_flags.csv"))
    con.execute(f"CREATE TEMP TABLE crossf AS {cross_sql}")
    cross_by_type = dict(con.execute(
        "SELECT check_name, COUNT(*) FROM crossf GROUP BY check_name ORDER BY 2 DESC"
    ).fetchall())

    # cross summary by era
    dump_csv(con, """
        SELECT check_name, era, COUNT(*) AS flags,
               ROUND(AVG(resid),2) AS mean_resid, MAX(resid) AS max_resid
        FROM crossf GROUP BY check_name, era ORDER BY check_name, era
    """, os.path.join(out, "internal_summary.csv"))

    # ---- COVERAGE GAPS: DEF side unpopulated but the counter-stat exists ------------
    # These are not errors; they are BACKFILL CANDIDATES. e.g. opposing QBs threw N INTs
    # this game, so this team's DEF should record N interceptions, but def_interceptions
    # is NULL. Same for sacks (1982+). The offense side is the authoritative counter-source.
    coverage_gap_n = dump_csv(con, f"""
        SELECT 'def_int_backfill' AS gap_type, o.year, o.week, d.team_c AS def_team, d.opp_c AS vs_team,
               o.qb_int_thrown AS should_be
        FROM dg d
        JOIN tg o ON o.year=d.year AND o.week=d.week AND o.team_c=d.opp_c AND o.opp_c=d.team_c
        WHERE d.def_int_caught IS NULL AND o.qb_int_thrown > 0
        UNION ALL
        SELECT 'def_sack_backfill', o.year, o.week, d.team_c, d.opp_c, o.team_sacks_suffered
        FROM dg d
        JOIN tg o ON o.year=d.year AND o.week=d.week AND o.team_c=d.opp_c AND o.opp_c=d.team_c
        WHERE d.year >= 1982 AND d.def_sacks IS NULL AND o.team_sacks_suffered > 0
        ORDER BY year, week
    """, os.path.join(out, "internal_def_backfill_candidates.csv"))

    manifest = {
        "lane": LANE,
        "status": "fail" if (hard_n or struct_n) else ("review" if cross_n else "pass"),
        "counts": {
            "hard_violations": int(hard_n),
            "structural_violations": int(struct_n),
            "cross_flags": int(cross_n),
            "def_backfill_candidates": int(coverage_gap_n),
        },
        "hard_by_type": hard_by_type,
        "structural_by_type": struct_by_type,
        "cross_by_type": cross_by_type,
        "tolerances": {"int_symmetry": t_int, "sack_symmetry": t_sack, "pass_recv_yards": t_passrecv},
        "artifacts": {
            "hard_violations": os.path.join(out, "internal_hard_violations.csv"),
            "structural_violations": os.path.join(out, "internal_structural_violations.csv"),
            "cross_flags": os.path.join(out, "internal_cross_flags.csv"),
            "summary": os.path.join(out, "internal_summary.csv"),
            "def_backfill_candidates": os.path.join(out, "internal_def_backfill_candidates.csv"),
        },
        "notes": [
            "HARD violations are physically impossible -> any nonzero count FAILS the lane.",
            "CROSS checks are cross-team/cross-position identities with documented tolerances.",
            "int_symmetry/sack_symmetry join each team to the OPPOSING DEF row in the same game.",
        ],
    }
    manifest.update(source_pin())
    write_manifest(os.path.join(out, "manifest.json"), manifest)
    con.close()
    return manifest


if __name__ == "__main__":
    from .recon_common import new_run_dir
    import json
    m = run(new_run_dir())
    print(json.dumps({"counts": m["counts"], "hard_by_type": m["hard_by_type"],
                      "cross_by_type": m["cross_by_type"]}, indent=2))
