"""
sota_recon/recon_scoring.py  --  LANE: scoreboard reconciliation

The one fully INDEPENDENT witness: a team's final points (pts_def_team_pts, from the
PFR boxscore/schedule) must equal the sum of its scoring events. Unlike the oracle lane
(which shares our per-player stats), the scoreboard is external ground truth, so this
catches missing/extra scoring atoms nothing else can see.

    team_points  ==  6*(rush_td + rec_td + def_td + fum_ret_td + st_td)
                   + 3*fg_made + pat_made + 2*two_pt + 2*safeties

CRITICAL taxonomy (discovered during the audit):
  * passing_tds == receiving_tds (same play) -> count receiving only.
  * def_tds == def_int_ret_td (def_tds is INT-returns ONLY); fumble-return TDs are the
    SEPARATE fum_ret_td column -> non-offensive TDs = def_tds + fum_ret_td + special_teams_tds.
  * Defensive/ST scores are recorded on BOTH the team DEF row AND individual player rows
    -> summing across all rows DOUBLE-COUNTS. Offensive scoring is taken from non-DEF
    rows; non-offensive scoring from the DEF team row only.

Outputs:
  scoring_recon_by_era.csv     - exact / within-1 reconciliation rate by era
  scoring_recon_residuals.csv  - residual (actual-computed) distribution
  scoring_recon_mismatches.csv - non-reconciling team-games for triage
  manifest.json
"""

from __future__ import annotations

import os

from .recon_common import connect, dump_csv, era_of, lane_dir, source_pin, write_manifest
from .sources import DATA_LAKE, registry

LANE = "scoring_reconciliation"
SCORING_SUMMARY = os.path.join(DATA_LAKE, "derived", "scoring_summary", "scoring_summary.parquet")


def run(run_dir: str) -> dict:
    reg = registry()
    v26 = reg["v26_release"].path
    out = lane_dir(run_dir, LANE)
    con = connect()

    con.execute(f"""
        CREATE TEMP TABLE recon AS
        WITH off AS (
            SELECT year, CAST(week AS INTEGER) AS wk, nfl_franchise_number AS fn,
                   SUM(COALESCE(rushing_tds,0)) + SUM(COALESCE(receiving_tds,0)) AS otd,
                   SUM(COALESCE(fg_made,0)) AS fg, SUM(COALESCE(pat_made,0)) AS pat,
                   SUM(COALESCE(passing_2pt_conversions,0))
                     + SUM(COALESCE(rushing_2pt_conversions,0))
                     + SUM(COALESCE(receiving_2pt_conversions,0)) AS tp
            FROM '{v26}' WHERE position <> 'DEF' AND nfl_franchise_number IS NOT NULL
            GROUP BY 1,2,3
        ),
        d AS (
            SELECT year, CAST(week AS INTEGER) AS wk, nfl_franchise_number AS fn,
                   MAX(COALESCE(def_tds,0)+COALESCE(fum_ret_td,0)+COALESCE(special_teams_tds,0)) AS ntd,
                   MAX(COALESCE(def_safeties,0)) AS saf,
                   MAX(pts_def_team_pts) AS pts
            FROM '{v26}' WHERE position = 'DEF' AND nfl_franchise_number IS NOT NULL
            GROUP BY 1,2,3
        )
        SELECT off.year, off.wk, off.fn, {era_of('off.year')} AS era,
               d.pts AS actual_pts,
               6*(off.otd + d.ntd) + 3*off.fg + off.pat + 2*off.tp + 2*d.saf AS computed_pts,
               d.pts - (6*(off.otd + d.ntd) + 3*off.fg + off.pat + 2*off.tp + 2*d.saf) AS resid
        FROM off JOIN d USING (year, wk, fn)
        WHERE d.pts IS NOT NULL
    """)

    total = con.execute("SELECT COUNT(*) FROM recon").fetchone()[0]
    exact = con.execute("SELECT COUNT(*) FROM recon WHERE resid = 0").fetchone()[0]

    # --- AUTHORITATIVE comparison: v26 vs the PFR scoring-summary (points exact by
    # construction). This is the real reconciliation; the self-sum above is a fallback. ---
    auth = {}
    if os.path.exists(SCORING_SUMMARY):
        con.execute(f"""
            CREATE TEMP TABLE authcmp AS
            WITH a AS (
                SELECT CAST(team_fid AS INTEGER) AS fn, year, week,
                       MAX(points) AS auth_pts, MAX(td) AS auth_td, MAX(fg) AS auth_fg
                FROM '{SCORING_SUMMARY}' WHERE team_fid IS NOT NULL
                GROUP BY 1,2,3
            )
            SELECT r.era, r.actual_pts AS v26_final, r.computed_pts AS v26_atoms,
                   a.auth_pts, {era_of('r.year')} AS era2
            FROM recon r JOIN a ON a.fn=r.fn AND a.year=r.year AND a.week=r.wk
        """)
        rr = con.execute("""
            SELECT COUNT(*) n,
                   COUNT(*) FILTER (WHERE v26_final = auth_pts) final_match,
                   COUNT(*) FILTER (WHERE v26_atoms = auth_pts) atoms_match
            FROM authcmp
        """).fetchone()
        auth = {"compared": int(rr[0]),
                "v26_final_score_matches_authoritative": int(rr[1]),
                "v26_final_match_pct": round(100.0*rr[1]/rr[0], 2) if rr[0] else None,
                "v26_atoms_reconstruct_authoritative": int(rr[2]),
                "v26_atoms_match_pct": round(100.0*rr[2]/rr[0], 2) if rr[0] else None}
        dump_csv(con, """
            SELECT era2 AS era, COUNT(*) team_games,
                   ROUND(100.0*COUNT(*) FILTER (WHERE v26_final=auth_pts)/COUNT(*),1) final_match_pct,
                   ROUND(100.0*COUNT(*) FILTER (WHERE v26_atoms=auth_pts)/COUNT(*),1) atoms_match_pct
            FROM authcmp GROUP BY era2 ORDER BY era2
        """, os.path.join(out, "scoring_vs_authoritative_by_era.csv"))

    dump_csv(con, """
        SELECT era, COUNT(*) AS team_games,
               COUNT(*) FILTER (WHERE resid = 0) AS exact,
               ROUND(100.0*COUNT(*) FILTER (WHERE resid = 0)/COUNT(*), 1) AS exact_pct,
               COUNT(*) FILTER (WHERE ABS(resid) <= 1) AS within1
        FROM recon GROUP BY era ORDER BY era
    """, os.path.join(out, "scoring_recon_by_era.csv"))

    dump_csv(con, """
        SELECT resid, COUNT(*) AS n FROM recon GROUP BY resid ORDER BY n DESC
    """, os.path.join(out, "scoring_recon_residuals.csv"))

    mism_n = dump_csv(con, """
        SELECT year, wk, fn, era, actual_pts, computed_pts, resid
        FROM recon WHERE resid <> 0 ORDER BY ABS(resid) DESC, year DESC
    """, os.path.join(out, "scoring_recon_mismatches.csv"))

    # modern-era headline (where the scoring decomposition should be complete)
    modern = con.execute("""
        SELECT COUNT(*), COUNT(*) FILTER (WHERE resid = 0) FROM recon WHERE year >= 2015
    """).fetchone()
    modern_pct = round(100.0*modern[1]/modern[0], 1) if modern[0] else None

    manifest = {
        "lane": LANE,
        "status": "review",  # scoring decomposition completeness is a known era-dependent gap
        "counts": {
            "team_games": int(total),
            "exact": int(exact),
            "exact_pct": round(100.0*exact/total, 1) if total else None,
            "exact_pct_2015plus": modern_pct,
            "mismatches": int(mism_n),
        },
        "vs_authoritative": auth,
        "artifacts": {
            "by_era": os.path.join(out, "scoring_recon_by_era.csv"),
            "residuals": os.path.join(out, "scoring_recon_residuals.csv"),
            "mismatches": os.path.join(out, "scoring_recon_mismatches.csv"),
        },
        "notes": [
            "Scoreboard is the only witness fully independent of our per-player stats.",
            "Non-reconciliation = incomplete scoring decomposition (which TDs/FGs made up the "
            "points), NOT a wrong final score. Degrades in older eras (sparse play-level data).",
            "Defensive/ST scores are double-counted across team-DEF and player rows; this lane "
            "counts non-offensive scoring from the DEF row only to avoid it.",
        ],
    }
    manifest.update(source_pin())
    write_manifest(os.path.join(out, "manifest.json"), manifest)
    con.close()
    return manifest


if __name__ == "__main__":
    from .recon_common import new_run_dir
    import json
    print(json.dumps(run(new_run_dir())["counts"], indent=2))
