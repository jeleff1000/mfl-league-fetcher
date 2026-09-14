"""
sota_recon/corrections/wave7_efficiency_ratios.py

Wave 7: add the standard efficiency-ratio stats that were missing entirely from v26.
All are FORMULA-DRIVEN (deterministic ratios over audited inputs), so computable in
every era. Each is defined ONLY where its denominator > 0 (a rate over zero attempts
is undefined, not zero) -> NULL elsewhere. Deterministic => idempotent.

    completion_pct                = completions / attempts * 100
    passing_yards_per_attempt     = passing_yards / attempts
    passing_td_pct                = passing_tds / attempts * 100
    passing_int_pct               = passing_interceptions / attempts * 100
    rushing_yards_per_carry       = rushing_yards / carries
    receiving_yards_per_reception = receiving_yards / receptions
    receiving_yards_per_target    = receiving_yards / targets
    catch_pct                     = receptions / targets * 100
    punt_yards_per_punt           = punt_yards / punts
"""

from __future__ import annotations

from .framework import PROV_COL, _count, Correction

# (new_col, numerator_col, denominator_col, scale)
_SPECS = [
    ("completion_pct",                "completions",            "attempts", 100),
    ("passing_yards_per_attempt",     "passing_yards",          "attempts", 1),
    ("passing_td_pct",                "passing_tds",            "attempts", 100),
    ("passing_int_pct",               "passing_interceptions",  "attempts", 100),
    ("rushing_yards_per_carry",       "rushing_yards",          "carries",  1),
    ("receiving_yards_per_reception", "receiving_yards",        "receptions", 1),
    ("receiving_yards_per_target",    "receiving_yards",        "targets",  1),
    ("catch_pct",                     "receptions",             "targets",  100),
    ("punt_yards_per_punt",           "punt_yards",             "punts",    1),
]


def _efficiency_ratios(con) -> int:
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    total_set = 0
    for new_col, num, den, scale in _SPECS:
        if new_col not in cols:
            con.execute(f"ALTER TABLE st ADD COLUMN {new_col} DOUBLE")
        where = f"{den} IS NOT NULL AND {den} > 0"
        total_set += _count(con, where)
        con.execute(f"""
            UPDATE st SET
                {new_col} = ROUND(COALESCE({num},0) / {den} * {scale}, 2),
                {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave7.efficiency_ratios'
                                  ELSE {PROV_COL} || ',wave7.efficiency_ratios' END
            WHERE {where}
        """)
    return total_set


CORRECTIONS = [
    Correction("wave7.efficiency_ratios",
               "add completion_pct, Y/A, YPC, YPR, Y/tgt, catch_pct, TD%, INT%, Y/punt (formula, all eras)",
               _efficiency_ratios, "attempts IS NOT NULL AND attempts > 0"),
]
