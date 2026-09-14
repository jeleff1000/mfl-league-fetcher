"""
sota_recon/corrections/wave5_structural.py

Wave 5: resolve the genuine structural impossibilities the composition/bounds checks
surfaced (overcount-direction only; undercounts are legitimate uncategorized/negative
plays and are left alone).

  pat_att_identity        where pat_made+pat_missed+pat_blocked > pat_att, the total is
                          understated -> set pat_att = components (every PAT is one of the
                          three outcomes). Identity completion, same logic as fg_att.
  null_impossible_recv_buckets  where the reception-range buckets SUM to MORE than total
                          receptions, the bucket data is provably wrong -> NULL the buckets
                          (honest 'unknown' beats a known-impossible value). Tagged.
  null_impossible_punt_long     where punt_long > punt_yards (with >1 punt), punt_long is
                          impossible -> NULL it.
"""

from __future__ import annotations

from .framework import PROV_COL, _count, Correction

_RNG = ["receptions_0_4","receptions_5_9","receptions_10_19","receptions_20_29","receptions_30_39","receptions_40plus"]
_RNG_SUM = "+".join(f"COALESCE({x},0)" for x in _RNG)
_RNG_PRESENT = " AND ".join(f"{x} IS NOT NULL" for x in _RNG)


def _pat_att_identity(con) -> int:
    where = ("pat_att IS NOT NULL AND pat_made IS NOT NULL "
             "AND COALESCE(pat_made,0)+COALESCE(pat_missed,0)+COALESCE(pat_blocked,0) > pat_att")
    n = _count(con, where)
    if n:
        con.execute(f"""
            UPDATE st SET
                pat_att = COALESCE(pat_made,0)+COALESCE(pat_missed,0)+COALESCE(pat_blocked,0),
                {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave5.pat_att_identity'
                                  ELSE {PROV_COL} || ',wave5.pat_att_identity' END
            WHERE {where}
        """)
    return n


def _null_impossible_recv_buckets(con) -> int:
    where = f"receptions IS NOT NULL AND ({_RNG_PRESENT}) AND {_RNG_SUM} > receptions"
    n = _count(con, where)
    if n:
        setnull = ", ".join(f"{x} = NULL" for x in _RNG)
        con.execute(f"""
            UPDATE st SET {setnull},
                {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave5.null_impossible_recv_buckets'
                                  ELSE {PROV_COL} || ',wave5.null_impossible_recv_buckets' END
            WHERE {where}
        """)
    return n


def _null_impossible_punt_long(con) -> int:
    where = ("punt_yards IS NOT NULL AND punt_long IS NOT NULL AND punts IS NOT NULL "
             "AND punts > 1 AND punt_long > punt_yards")
    n = _count(con, where)
    if n:
        con.execute(f"""
            UPDATE st SET punt_long = NULL,
                {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave5.null_impossible_punt_long'
                                  ELSE {PROV_COL} || ',wave5.null_impossible_punt_long' END
            WHERE {where}
        """)
    return n


CORRECTIONS = [
    Correction("wave5.pat_att_identity",
               "pat_att = pat_made+missed+blocked where components exceed att (identity)",
               _pat_att_identity,
               "pat_att IS NOT NULL AND pat_made IS NOT NULL AND COALESCE(pat_made,0)+COALESCE(pat_missed,0)+COALESCE(pat_blocked,0) > pat_att"),
    Correction("wave5.null_impossible_recv_buckets",
               "NULL reception-range buckets where they sum to more than total receptions",
               _null_impossible_recv_buckets,
               f"receptions IS NOT NULL AND ({_RNG_PRESENT}) AND {_RNG_SUM} > receptions"),
    Correction("wave5.null_impossible_punt_long",
               "NULL punt_long where it exceeds total punt_yards (>1 punt)",
               _null_impossible_punt_long,
               "punt_yards IS NOT NULL AND punt_long IS NOT NULL AND punts IS NOT NULL AND punts>1 AND punt_long > punt_yards"),
]
