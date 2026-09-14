"""
sota_recon/recon_oracle.py  --  LANE: second-source corroboration (the SOTA lane)

Asks the question every other check cannot: does an INDEPENDENT source agree with
our number? Joins v26 player-weeks to the PBP-derived player-week rollup (1978-2025)
and, for each stat family, measures the residual (subject - oracle).

Outputs (per lane dir):
  oracle_stat_summary.csv     - per stat: matched rows, agree %, residual mean/median/p95/max
  oracle_residual_hist.csv    - residual distribution per stat (for spotting systematic bias)
  oracle_mismatch_samples.csv - worst offenders per stat for manual review
  oracle_unmatched.csv        - player-weeks in v26 with no oracle row (coverage gaps)
  manifest.json

Design notes:
  * Residual DISTRIBUTION matters more than a pass/fail count. A clean table has
    residuals spiked at 0; a systematic +N bias reveals a transform bug a threshold hides.
  * We only compare where BOTH sides are non-null. A null on one side is a coverage
    question (reported separately in oracle_unmatched / per-stat null counts), not a mismatch.
  * Counts (TDs, receptions, attempts) must match exactly; yards get a small tolerance.
"""

from __future__ import annotations

import os

from .asymmetry_registry import ORACLE_STAT_PAIRS, tol
from .recon_common import connect, dump_csv, lane_dir, source_pin, write_manifest
from .sources import registry

LANE = "oracle_corroboration"
ORACLE_YEAR_MIN, ORACLE_YEAR_MAX = 1978, 2025


_KEYS = {"player_week", "year", "week", "nfl_franchise_number", "opponent_nfl_franchise_number"}
_DERIVED_PAT = ("pts_", "lamar", "rank", "bonus", "ppg", "_p04", "_p1_", "_p5", "_p25",
                "clutch", "_canonical", "_pct", "share", "_over_30")
_NONSTAT = ("context_count", "mapped_to_player_bio", "bio_lookup", "event_rows",
            "namespaces", "source", "_id", "games")
# same column NAME, different DEFINITION across sources -> not comparable directly.
# def_tackles_with_assist: v26 = combined tackles (= solo+assist, internally 100% consistent);
# the oracle uses the name for a different concept. Validate v26's via internal identity instead.
_NAME_COLLISION = {"def_tackles_with_assist"}
_YARDS_HINT = ("yard", "yds", "long", "distance", "air", "adot")


def _discover_pairs(con, v26: str, pbp: str):
    """Auto-discover every numeric column present in BOTH v26 and the oracle, excluding
    derived columns, keys, and non-stat metadata. Returns (subj_col, orc_col, tol_key).
    Curated ORACLE_STAT_PAIRS take precedence (keep their explicit tolerance class)."""
    def numeric_cols(path):
        return {r[0]: r[1] for r in con.execute(f"DESCRIBE SELECT * FROM '{path}'").fetchall()
                if r[1] in ("DOUBLE", "BIGINT", "INTEGER", "FLOAT", "DECIMAL")}
    vnum, onum = numeric_cols(v26), numeric_cols(pbp)
    curated = {s for s, _, _ in ORACLE_STAT_PAIRS}
    pairs = list(ORACLE_STAT_PAIRS)
    for col in sorted(set(vnum) & set(onum)):
        if col in curated or col in _KEYS:
            continue
        if col in _NAME_COLLISION:
            continue
        low = col.lower()
        if any(d in low for d in _DERIVED_PAT) or any(n in low for n in _NONSTAT):
            continue
        tol = "oracle_yards" if any(h in low for h in _YARDS_HINT) else "oracle_counts"
        pairs.append((col, col, tol))
    return pairs


def run(run_dir: str) -> dict:
    reg = registry()
    v26 = reg["v26_release"].path
    pbp = reg["pbp_player_week_rollup"].path
    out = lane_dir(run_dir, LANE)
    con = connect()

    # Auto-discovered + curated stat pairs (covers IDP, returns, kicking, buckets, etc.)
    pairs = _discover_pairs(con, v26, pbp)

    # Subject side: ALL positions in oracle era (non-null filter handles position scoping,
    # so IDP/returns/punting on defensive/ST players are compared too), keyed by player_week.
    con.execute(f"""
        CREATE TEMP VIEW subj AS
        SELECT player_week, NFL_player_id, year, week, position,
               { ', '.join(s for s, _, _ in pairs) }
        FROM '{v26}'
        WHERE year BETWEEN {ORACLE_YEAR_MIN} AND {ORACLE_YEAR_MAX}
    """)
    con.execute(f"""
        CREATE TEMP VIEW orc AS
        SELECT player_week,
               { ', '.join(f'{o} AS orc_{o}' for _, o, _ in pairs) }
        FROM '{pbp}'
    """)
    # materialize (not a view): the histogram/summary scan it ~100x; a view would re-join each time
    con.execute("""
        CREATE TEMP TABLE joined AS
        SELECT s.*, o.* EXCLUDE (player_week)
        FROM subj s JOIN orc o USING (player_week)
    """)

    subj_n = con.execute("SELECT COUNT(*) FROM subj").fetchone()[0]
    matched_n = con.execute("SELECT COUNT(*) FROM joined").fetchone()[0]

    # --- per-stat agreement summary -------------------------------------------------
    summary_rows = []
    for subj_col, orc_col, tol_key in pairs:
        t = tol(tol_key)
        oc = f"orc_{orc_col}"
        r = con.execute(f"""
            WITH cmp_rows AS (
                SELECT {subj_col} AS s, {oc} AS o,
                       ABS(COALESCE({subj_col},0) - COALESCE({oc},0)) AS resid
                FROM joined
                WHERE {subj_col} IS NOT NULL AND {oc} IS NOT NULL
            )
            SELECT
                COUNT(*)                                   AS compared,
                COUNT(*) FILTER (WHERE resid <= {t})       AS within_tol,
                AVG(s - o)                                 AS mean_signed,
                MEDIAN(resid)                              AS median_abs,
                QUANTILE_CONT(resid, 0.95)                 AS p95_abs,
                MAX(resid)                                 AS max_abs
            FROM cmp_rows
        """).fetchone()
        compared, within, mean_signed, med, p95, mx = r
        agree_pct = (100.0 * within / compared) if compared else None
        summary_rows.append((subj_col, tol_key, t, compared, within,
                             agree_pct, mean_signed, med, p95, mx))

    con.execute("""
        CREATE TEMP TABLE stat_summary (
            stat VARCHAR, tol_class VARCHAR, tolerance DOUBLE,
            compared BIGINT, within_tol BIGINT, agree_pct DOUBLE,
            mean_signed DOUBLE, median_abs DOUBLE, p95_abs DOUBLE, max_abs DOUBLE
        )
    """)
    con.executemany(
        "INSERT INTO stat_summary VALUES (?,?,?,?,?,?,?,?,?,?)", summary_rows
    )
    dump_csv(con, "SELECT * FROM stat_summary ORDER BY agree_pct NULLS FIRST",
             os.path.join(out, "oracle_stat_summary.csv"))

    # --- residual histogram per stat (signed, capped buckets) -----------------------
    hist_selects = []
    for subj_col, orc_col, _ in pairs:
        oc = f"orc_{orc_col}"
        hist_selects.append(f"""
            SELECT '{subj_col}' AS stat,
                   CAST(GREATEST(-10, LEAST(10, ROUND({subj_col} - {oc}))) AS INTEGER) AS signed_bucket,
                   COUNT(*) AS n
            FROM joined
            WHERE {subj_col} IS NOT NULL AND {oc} IS NOT NULL
            GROUP BY 1,2
        """)
    dump_csv(con, " UNION ALL ".join(hist_selects) + " ORDER BY stat, signed_bucket",
             os.path.join(out, "oracle_residual_hist.csv"))

    # --- worst mismatch samples per stat -------------------------------------------
    mm_selects = []
    for subj_col, orc_col, tol_key in pairs:
        t = tol(tol_key)
        oc = f"orc_{orc_col}"
        mm_selects.append(f"""
            SELECT * FROM (
                SELECT '{subj_col}' AS stat, player_week, NFL_player_id, year, week, position,
                       {subj_col} AS subject_val, {oc} AS oracle_val,
                       ABS(COALESCE({subj_col},0)-COALESCE({oc},0)) AS abs_resid
                FROM joined
                WHERE {subj_col} IS NOT NULL AND {oc} IS NOT NULL
                  AND ABS(COALESCE({subj_col},0)-COALESCE({oc},0)) > {t}
                ORDER BY abs_resid DESC
                LIMIT 50
            )
        """)
    mismatch_n = dump_csv(
        con, " UNION ALL ".join(mm_selects) + " ORDER BY stat, abs_resid DESC",
        os.path.join(out, "oracle_mismatch_samples.csv"),
    )

    # --- coverage: subject player-weeks with no oracle row --------------------------
    unmatched_n = dump_csv(con, f"""
        SELECT s.player_week, s.NFL_player_id, s.year, s.week, s.position
        FROM subj s LEFT JOIN orc o USING (player_week)
        WHERE o.player_week IS NULL
        ORDER BY s.year, s.week
    """, os.path.join(out, "oracle_unmatched.csv"))

    # overall headline: weighted agreement across all compared cells
    total_cmp = sum(row[3] for row in summary_rows)
    total_within = sum(row[4] for row in summary_rows)
    overall_agree = (100.0 * total_within / total_cmp) if total_cmp else None

    # flag any stat with a systematic signed bias (|mean_signed| large vs tolerance)
    biased = [row[0] for row in summary_rows
              if row[6] is not None and abs(row[6]) > max(0.25, row[2])]

    manifest = {
        "lane": LANE,
        "status": "review" if (biased or (overall_agree or 100) < 99.0) else "pass",
        "counts": {
            "subject_player_weeks": int(subj_n),
            "matched_to_oracle": int(matched_n),
            "match_pct": round(100.0 * matched_n / subj_n, 2) if subj_n else None,
            "cells_compared": int(total_cmp),
            "cells_within_tolerance": int(total_within),
            "overall_agree_pct": round(overall_agree, 4) if overall_agree is not None else None,
            "unmatched_player_weeks": int(unmatched_n),
            "mismatch_samples": int(mismatch_n),
        },
        "systematic_bias_stats": biased,
        "era": f"{ORACLE_YEAR_MIN}-{ORACLE_YEAR_MAX}",
        "artifacts": {
            "stat_summary": os.path.join(out, "oracle_stat_summary.csv"),
            "residual_hist": os.path.join(out, "oracle_residual_hist.csv"),
            "mismatch_samples": os.path.join(out, "oracle_mismatch_samples.csv"),
            "unmatched": os.path.join(out, "oracle_unmatched.csv"),
        },
        "notes": [
            "Independent PBP-derived oracle. Compares only where both sides non-null.",
            "Counts compared exactly; yards within small tolerance (see asymmetry_registry).",
            "systematic_bias_stats lists stats whose signed residual mean exceeds tolerance "
            "-> indicates a transform/units issue, not random noise.",
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
    print(json.dumps(m["counts"], indent=2))
    if m["systematic_bias_stats"]:
        print("SYSTEMATIC BIAS:", m["systematic_bias_stats"])
