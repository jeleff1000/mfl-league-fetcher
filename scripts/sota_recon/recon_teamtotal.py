"""
sota_recon/recon_teamtotal.py  --  LANE: vertical team-total reconciliation

Orthogonal to offense-vs-defense and to the oracle: checks that the SUM of player
stats in a team-game equals the INDEPENDENT team total from the schedule authority.

This is the only lane that catches a WHOLE PLAYER MISSING (sum falls short of the
team total) or a DUPLICATED player (sum overshoots) — failures that pass every
horizontal and per-player check because the per-row values are individually fine.

Known systematic offset: NFL team total yards are NET of sack yardage, while player
passing_yards are gross. So expect sum(player pass+rush) to run slightly ABOVE
team total by ~the sack yardage (modern era). We measure the residual DISTRIBUTION
by era and flag only the outliers, rather than demanding exact equality.

Outputs:
  teamtotal_residual_summary.csv  - residual (sum_players - team_total) stats by era
  teamtotal_outliers.csv          - team-games with implausible residuals (missing/dup suspects)
  manifest.json
"""

from __future__ import annotations

import os

from .recon_common import canon_team_sql, connect, dump_csv, era_of, lane_dir, source_pin, write_manifest
from .sources import registry

LANE = "team_total_vertical"

# Outlier thresholds on residual = sum(player pass+rush yds) - schedule total yds.
# Negative beyond -MISSING => likely a missing player. Positive beyond +DUP (above the
# expected sack offset) => likely a duplicate / double-count. Era-aware.
NEG_MISSING = -40   # yards short of team total
POS_DUP = 80        # yards over team total (well above any sack-yardage offset)


def run(run_dir: str) -> dict:
    reg = registry()
    v26 = reg["v26_release"].path
    sched = reg["schedule_master"].path
    out = lane_dir(run_dir, LANE)
    con = connect()
    ct = canon_team_sql

    # sum of player offensive yards per team-game (modern offensive positions)
    con.execute(f"""
        CREATE TEMP VIEW player_team_tot AS
        SELECT year, CAST(week AS INTEGER) AS week,
               nfl_franchise_number AS team_c, opponent_nfl_franchise_number AS opp_c,
               SUM(COALESCE(passing_yards,0)) AS sum_pass,
               SUM(COALESCE(rushing_yards,0)) AS sum_rush,
               SUM(COALESCE(passing_yards,0)) + SUM(COALESCE(rushing_yards,0)) AS sum_off_yds,
               COUNT(*) AS n_players
        FROM '{v26}'
        WHERE position IN ('QB','RB','WR','TE')
        GROUP BY 1,2,3,4
    """)
    con.execute(f"""
        CREATE TEMP VIEW sched AS
        SELECT year, week,
               CAST(franchise_id AS INTEGER) AS team_c,
               CAST(opponent_franchise_id AS INTEGER) AS opp_c,
               yds_team, {era_of('year')} AS era
        FROM '{sched}'
        WHERE yds_team IS NOT NULL
    """)
    con.execute("""
        CREATE TEMP VIEW joined AS
        SELECT p.year, p.week, p.team_c, p.opp_c, s.era,
               p.sum_off_yds, s.yds_team,
               (p.sum_off_yds - s.yds_team) AS resid,
               p.n_players
        FROM player_team_tot p
        JOIN sched s USING (year, week, team_c, opp_c)
    """)

    matched = con.execute("SELECT COUNT(*) FROM joined").fetchone()[0]

    dump_csv(con, f"""
        SELECT era,
               COUNT(*) AS team_games,
               ROUND(AVG(resid),2) AS mean_resid,
               ROUND(MEDIAN(resid),2) AS median_resid,
               ROUND(QUANTILE_CONT(resid,0.05),1) AS p05,
               ROUND(QUANTILE_CONT(resid,0.95),1) AS p95,
               MIN(resid) AS min_resid, MAX(resid) AS max_resid,
               COUNT(*) FILTER (WHERE resid < {NEG_MISSING}) AS missing_suspects,
               COUNT(*) FILTER (WHERE resid > {POS_DUP}) AS dup_suspects
        FROM joined GROUP BY era ORDER BY era
    """, os.path.join(out, "teamtotal_residual_summary.csv"))

    outliers_n = dump_csv(con, f"""
        SELECT year, week, team_c, opp_c, era, n_players,
               sum_off_yds, yds_team, resid,
               CASE WHEN resid < {NEG_MISSING} THEN 'missing_suspect'
                    WHEN resid > {POS_DUP} THEN 'dup_suspect' END AS flag
        FROM joined
        WHERE resid < {NEG_MISSING} OR resid > {POS_DUP}
        ORDER BY ABS(resid) DESC
    """, os.path.join(out, "teamtotal_outliers.csv"))

    miss = con.execute(f"SELECT COUNT(*) FROM joined WHERE resid < {NEG_MISSING}").fetchone()[0]
    dup = con.execute(f"SELECT COUNT(*) FROM joined WHERE resid > {POS_DUP}").fetchone()[0]

    manifest = {
        "lane": LANE,
        "status": "review" if outliers_n else "pass",
        "counts": {
            "team_games_compared": int(matched),
            "missing_player_suspects": int(miss),
            "dup_player_suspects": int(dup),
            "outliers_total": int(outliers_n),
            "outlier_pct": round(100.0*outliers_n/matched, 3) if matched else None,
        },
        "thresholds": {"neg_missing": NEG_MISSING, "pos_dup": POS_DUP},
        "artifacts": {
            "residual_summary": os.path.join(out, "teamtotal_residual_summary.csv"),
            "outliers": os.path.join(out, "teamtotal_outliers.csv"),
        },
        "notes": [
            "Vertical check: sum(player pass+rush yds) vs independent schedule team total.",
            "Expected small POSITIVE residual = gross-vs-net sack yardage; large NEGATIVE = "
            "missing player; large POSITIVE = duplicate player.",
            "Only compares team-games where schedule carries a total-yards figure.",
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
