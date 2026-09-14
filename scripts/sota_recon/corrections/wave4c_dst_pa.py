"""
sota_recon/corrections/wave4c_dst_pa.py

Wave 4c: precompute the adjusted fantasy DST points-allowed.

    dst_points_allowed(T) = points_allowed(T)
        - 6 * (opponent INT-return TDs vs T)
        - 6 * (opponent fumble-return TDs vs T)
        - 2 * (opponent safeties vs T)

i.e. subtract the points the OPPONENT'S DEFENSE scored against T's offense (which T's
DST is not responsible for). PAT / 2pt / KR-TD / PR-TD stay counted (per league rules
convention). The exclusion components live on the OPPONENT's DEF row (populated by 4b),
joined by year-aware franchise number.

Scoped to 1978+ (where def-return components exist). Pre-1978 dst_points_allowed stays
equal to raw points_allowed (no data to adjust). Deterministic from points_allowed +
opponent components => idempotent. Overwrites dst_points_allowed (it was just a copy
of points_allowed before this).
"""

from __future__ import annotations

from .framework import PROV_COL, Correction

_EXCL = ("6*COALESCE(O.def_int_ret_td,0) + 6*COALESCE(O.fum_ret_td,0) "
         "+ 2*COALESCE(O.def_safeties,0)")


def _dst_points_allowed(con) -> int:
    # count rows that will actually move (exclusion > 0)
    n = con.execute(f"""
        SELECT COUNT(*)
        FROM st T JOIN st O
          ON T.position='DEF' AND O.position='DEF' AND T.year>=1978
         AND O.nfl_franchise_number = T.opponent_nfl_franchise_number
         AND O.opponent_nfl_franchise_number = T.nfl_franchise_number
         AND O.year=T.year AND CAST(O.week AS INTEGER)=CAST(T.week AS INTEGER)
        WHERE T.points_allowed IS NOT NULL AND ({_EXCL}) > 0
    """).fetchone()[0]

    con.execute(f"""
        UPDATE st AS T
        SET dst_points_allowed = T.points_allowed - ({_EXCL}),
            {PROV_COL} = CASE WHEN T.{PROV_COL} IS NULL OR T.{PROV_COL}=''
                              THEN 'wave4c.dst_points_allowed'
                              ELSE T.{PROV_COL} || ',wave4c.dst_points_allowed' END
        FROM st AS O
        WHERE T.position='DEF' AND O.position='DEF' AND T.year>=1978
          AND O.nfl_franchise_number = T.opponent_nfl_franchise_number
          AND O.opponent_nfl_franchise_number = T.nfl_franchise_number
          AND O.year=T.year AND CAST(O.week AS INTEGER)=CAST(T.week AS INTEGER)
          AND T.points_allowed IS NOT NULL
          AND ({_EXCL}) > 0
    """)
    return n


CORRECTIONS = [
    Correction("wave4c.dst_points_allowed",
               "dst_points_allowed = points_allowed - opp(6*int_ret_td + 6*fum_ret_td + 2*safeties), 1978+",
               _dst_points_allowed,
               None),  # changed count comes from apply (exclusion>0 rows)
]
