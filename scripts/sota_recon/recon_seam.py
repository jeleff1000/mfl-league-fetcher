"""
sota_recon/recon_seam.py  --  LANE: seam / discontinuity detection

Catches the error class reconciliation is blind to: a SYSTEMATIC transform/units bug
that scales a whole era at once (so offense still ties defense, the oracle for the OTHER
era still agrees, and nothing per-row looks wrong). The fingerprint of such a bug is a
sharp year-over-year jump in a stat's league-wide per-game average — especially at a
known backfill boundary (the 1977/1978 PBP seam, or any year we ran an upsert).

Method: compute per-year league mean of each stat (per player-game, over players who
have the stat) and flag years whose mean jumps more than Z robust-MADs from the local
trend. Real football changes gradually; a data artifact jumps.

Outputs:
  seam_year_means.csv   - per stat, per year: mean, n
  seam_flags.csv        - year-over-year jumps beyond threshold (artifact suspects)
  manifest.json
"""

from __future__ import annotations

import os

from .recon_common import connect, dump_csv, lane_dir, source_pin, write_manifest
from .sources import registry

LANE = "seam_discontinuity"

# Each stat is scoped to the position group that actually produces it, and averaged
# over PRODUCERS ONLY (value > 0). A per-producer mean is stable across coverage
# changes, so a year-over-year jump signals a transform/units artifact, not sparsity.
# (stat, position_filter_sql)
SEAM_STATS = [
    ("passing_yards",     "position IN ('QB','RB','WR','TE')"),
    ("rushing_yards",     "position IN ('QB','RB','WR','TE')"),
    ("receiving_yards",   "position IN ('RB','WR','TE')"),
    ("receptions",        "position IN ('RB','WR','TE')"),
    ("targets",           "position IN ('RB','WR','TE')"),
    ("passing_tds",       "position IN ('QB','RB','WR','TE')"),
    ("rushing_tds",       "position IN ('QB','RB','WR','TE')"),
    ("receiving_tds",     "position IN ('RB','WR','TE')"),
    ("completions",       "position IN ('QB','RB','WR','TE')"),
    ("attempts",          "position IN ('QB','RB','WR','TE')"),
    ("carries",           "position IN ('QB','RB','WR','TE')"),
    ("fg_made",           "position = 'K'"),
    ("fg_att",            "position = 'K'"),
    ("def_sacks",         "position = 'DEF'"),
    ("def_interceptions", "position = 'DEF'"),
]

# a year is flagged if the per-producer mean jumps more than JUMP_FRAC vs the prior
# year. Tuned to catch step-changes, not gradual drift. Producer-scoped means are
# materially large, so we also require a small absolute floor to avoid noise.
JUMP_FRAC = 0.35
MIN_YEAR = 1932       # before this, samples too thin to trend
MIN_PRODUCERS = 40    # need enough producers for a stable mean


def run(run_dir: str) -> dict:
    reg = registry()
    v26 = reg["v26_release"].path
    out = lane_dir(run_dir, LANE)
    con = connect()

    # per-year per-producer mean (value > 0), scoped to the producing position group
    selects = []
    for s, pos_filter in SEAM_STATS:
        selects.append(f"""
            SELECT '{s}' AS stat, year,
                   AVG({s}) AS mean_val, COUNT({s}) AS n
            FROM '{v26}'
            WHERE year >= {MIN_YEAR} AND {s} IS NOT NULL AND {s} > 0
              AND {pos_filter}
            GROUP BY year
        """)
    year_means_sql = " UNION ALL ".join(selects)
    con.execute(f"CREATE TEMP TABLE ym AS {year_means_sql}")
    dump_csv(con, "SELECT * FROM ym ORDER BY stat, year",
             os.path.join(out, "seam_year_means.csv"))

    # year-over-year jump detection
    flags_n = dump_csv(con, f"""
        WITH seq AS (
            SELECT stat, year, mean_val, n,
                   LAG(mean_val) OVER (PARTITION BY stat ORDER BY year) AS prev_mean,
                   LAG(year)     OVER (PARTITION BY stat ORDER BY year) AS prev_year
            FROM ym
        )
        SELECT stat, prev_year, year,
               ROUND(prev_mean,3) AS prev_mean, ROUND(mean_val,3) AS mean_val,
               ROUND(mean_val - prev_mean, 3) AS delta,
               ROUND( (mean_val - prev_mean) / NULLIF(ABS(prev_mean),0), 3) AS frac_change,
               n,
               CASE WHEN year = 1978 OR prev_year = 1977 THEN 'pbp_seam'
                    ELSE 'other' END AS boundary
        FROM seq
        WHERE prev_mean IS NOT NULL AND year = prev_year + 1
          AND n >= {MIN_PRODUCERS}
          AND ABS( (mean_val - prev_mean) / NULLIF(ABS(prev_mean),0) ) > {JUMP_FRAC}
        ORDER BY ABS(frac_change) DESC
    """, os.path.join(out, "seam_flags.csv"))

    manifest = {
        "lane": LANE,
        "status": "review" if flags_n else "pass",
        "counts": {"seam_flags": int(flags_n)},
        "params": {"jump_frac": JUMP_FRAC, "min_year": MIN_YEAR, "stats": len(SEAM_STATS)},
        "artifacts": {
            "year_means": os.path.join(out, "seam_year_means.csv"),
            "flags": os.path.join(out, "seam_flags.csv"),
        },
        "notes": [
            "Flags year-over-year league-mean jumps > 35% (a transform/units artifact "
            "fingerprint that reconciliation cannot see).",
            "boundary='pbp_seam' marks the 1977/1978 PBP coverage boundary where a step "
            "in availability is EXPECTED for advanced stats (targets/sacks) -> review, not bug.",
            "Real rule changes (e.g. 1978 passing liberalization) can also cause real jumps; "
            "these flags are for human triage, not auto-fail.",
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
