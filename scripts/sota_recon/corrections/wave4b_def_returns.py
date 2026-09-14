"""
sota_recon/corrections/wave4b_def_returns.py

Wave 4b: backfill the DEFENSIVE-return scoring components on DEF rows from the PBP
oracle (1978-2025), aggregated over a team's defensive players per game:
    def_int_ret_td   interceptions returned for TD by the defense
    fum_ret_td       fumbles returned for TD by the DEFENSE (offensive fumble-recovery
                     TDs are excluded by filtering to defensive positions)
    def_safeties     safeties scored by the defense

These are the exact components the dst_points_allowed adjustment (wave 4c) excludes.
Oracle nfl_team -> v26 franchise number is mapped per (code, year) from v26 itself,
so the join is year-aware and code-spelling-proof. Only NULL cells are written.
"""

from __future__ import annotations

from .framework import PROV_COL, _count, Correction
from ..sources import registry

_DEF_POS = "('DB','LB','DL')"
# (DEF column, oracle column)
_RET = [
    ("def_int_ret_td", "def_int_ret_td"),
    ("fum_ret_td",     "fum_ret_td"),
    ("def_safeties",   "def_safeties"),
]


def _def_returns_from_pbp(con) -> int:
    pbp = registry()["pbp_player_week_rollup"].path
    miss_pred = " OR ".join(f"{c} IS NULL" for c, _ in _RET)
    miss_pred_st = " OR ".join(f"st.{c} IS NULL" for c, _ in _RET)
    where = f"position='DEF' AND year >= 1978 AND ({miss_pred})"
    n_before = _count(con, where)

    # (nfl_team, year) -> franchise number, from v26 itself (1:1 within a year)
    con.execute("""
        CREATE OR REPLACE TEMP VIEW code_fn AS
        SELECT nfl_team, year, MODE(nfl_franchise_number) AS fn
        FROM st WHERE nfl_franchise_number IS NOT NULL
        GROUP BY nfl_team, year
    """)
    # team-defense aggregate from the oracle, mapped to franchise
    agg = ",\n               ".join(f"SUM(COALESCE(o.{oc},0)) AS {c}" for c, oc in _RET)
    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW def_ret AS
        SELECT m.fn AS fn, o.year AS year, o.week AS wk,
               {agg}
        FROM '{pbp}' o
        JOIN code_fn m ON o.nfl_team = m.nfl_team AND o.year = m.year
        WHERE o.position IN {_DEF_POS}
        GROUP BY 1,2,3
    """)

    set_clause = ",\n            ".join(f"{c} = COALESCE(st.{c}, d.{c})" for c, _ in _RET)
    con.execute(f"""
        UPDATE st
        SET {set_clause},
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}=''
                              THEN 'wave4b.def_returns_from_pbp'
                              ELSE {PROV_COL} || ',wave4b.def_returns_from_pbp' END
        FROM def_ret d
        WHERE st.position='DEF' AND st.year >= 1978
          AND st.nfl_franchise_number = d.fn
          AND st.year = d.year AND CAST(st.week AS INTEGER) = d.wk
          AND ({miss_pred_st})
    """)
    return n_before - _count(con, where)


CORRECTIONS = [
    Correction("wave4b.def_returns_from_pbp",
               "DEF def_int_ret_td/fum_ret_td/def_safeties from PBP team-defense (1978+)",
               _def_returns_from_pbp,
               "position='DEF' AND year>=1978 AND (def_int_ret_td IS NULL OR fum_ret_td IS NULL OR def_safeties IS NULL)"),
]
