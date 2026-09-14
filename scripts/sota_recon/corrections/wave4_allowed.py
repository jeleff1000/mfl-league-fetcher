"""
sota_recon/corrections/wave4_allowed.py

Wave 4a: backfill the DEF "*_allowed" component atoms from the OPPONENT's offense.

Identity: the yards / TDs a defense ALLOWED in a game equal the yards / TDs the
opposing offense GAINED. So for team T's DEF row vs opponent O:
    passing_yds_allowed(T)  = sum O offense passing_yards
    rushing_yds_allowed(T)  = sum O offense rushing_yards
    total_yds_allowed(T)    = sum O offense (passing + rushing) yards
    passing_tds_allowed(T)  = sum O offense passing_tds
    rushing_tds_allowed(T)  = sum O offense rushing_tds
    receiving_tds_allowed(T)= sum O offense receiving_tds

Joined on YEAR-AWARE franchise numbers (nfl_franchise_number), so relocations and
identity changes (BAL Colts vs BAL Ravens) never collide. Only NULL cells are filled,
so this is idempotent. These atoms feed the per-league DST scoring wiring.
"""

from __future__ import annotations

from .framework import PROV_COL, _count, Correction

# (DEF column, offense aggregate expr)
_ALLOWED = [
    ("passing_yds_allowed",  "SUM(passing_yards)"),
    ("rushing_yds_allowed",  "SUM(rushing_yards)"),
    ("total_yds_allowed",    "SUM(COALESCE(passing_yards,0)) + SUM(COALESCE(rushing_yards,0))"),
    ("passing_tds_allowed",  "SUM(passing_tds)"),
    ("rushing_tds_allowed",  "SUM(rushing_tds)"),
    ("receiving_tds_allowed","SUM(receiving_tds)"),
]


def _allowed_from_opp_offense(con) -> int:
    # rows we will touch: DEF rows missing ANY of the allowed atoms
    miss_pred = " OR ".join(f"{c} IS NULL" for c, _ in _ALLOWED)            # unqualified (for _count on st only)
    miss_pred_st = " OR ".join(f"st.{c} IS NULL" for c, _ in _ALLOWED)      # qualified (for the join UPDATE)
    where = f"position='DEF' AND ({miss_pred})"
    n_before = _count(con, where)

    agg_cols = ",\n               ".join(f"{expr} AS {c}" for c, expr in _ALLOWED)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW opp_off AS
        SELECT nfl_franchise_number AS off_fn,
               opponent_nfl_franchise_number AS def_fn,
               year, CAST(week AS INTEGER) AS wk,
               {agg_cols}
        FROM st
        WHERE position IN ('QB','RB','WR','TE')
          AND nfl_franchise_number IS NOT NULL AND opponent_nfl_franchise_number IS NOT NULL
        GROUP BY 1,2,3,4
    """)

    set_clause = ",\n            ".join(
        f"{c} = COALESCE(st.{c}, o.{c})" for c, _ in _ALLOWED
    )
    con.execute(f"""
        UPDATE st
        SET {set_clause},
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}=''
                              THEN 'wave4.allowed_from_opp_offense'
                              ELSE {PROV_COL} || ',wave4.allowed_from_opp_offense' END
        FROM opp_off o
        WHERE st.position='DEF'
          AND st.nfl_franchise_number = o.def_fn
          AND st.opponent_nfl_franchise_number = o.off_fn
          AND st.year = o.year AND CAST(st.week AS INTEGER) = o.wk
          AND ({miss_pred_st})
    """)
    return n_before - _count(con, where)


CORRECTIONS = [
    Correction("wave4.allowed_from_opp_offense",
               "DEF *_allowed atoms from opponent offense (franchise-key identity)",
               _allowed_from_opp_offense,
               "position='DEF' AND (passing_yds_allowed IS NULL OR rushing_yds_allowed IS NULL "
               "OR total_yds_allowed IS NULL OR passing_tds_allowed IS NULL "
               "OR rushing_tds_allowed IS NULL OR receiving_tds_allowed IS NULL)"),
]
