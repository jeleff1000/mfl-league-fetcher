"""
sota_recon/corrections/wave1_hard.py

Wave 1: eliminate the physical impossibilities flagged by the internal lane.
All three fixes are identity- or oracle-based (no estimation):

  fg_att_identity        fg_att := fg_made + fg_missed + fg_blocked  where understated.
                         Every attempt is made/missed/blocked -> accounting identity.
  attempts_floor         attempts := completions  where completions>0 but attempts null/0.
                         Minimum-possible valid value (ancient box era, true att unknown).
  targets_from_oracle    targets := PBP-oracle targets where receptions>0 but targets
                         null/0 and oracle has a value >= receptions; else floor to receptions.
"""

from __future__ import annotations

from .framework import PROV_COL, Correction, _count, simple_update
from ..sources import registry


def _fg_att_identity(con) -> int:
    where = ("fg_att IS NOT NULL "
             "AND (COALESCE(fg_made,0)+COALESCE(fg_missed,0)) > fg_att + COALESCE(fg_blocked,0)")
    return simple_update(
        con, "wave1.fg_att_identity",
        "fg_att = COALESCE(fg_made,0)+COALESCE(fg_missed,0)+COALESCE(fg_blocked,0)",
        where,
    )


def _attempts_floor(con) -> int:
    where = "completions > 0 AND (attempts IS NULL OR attempts = 0)"
    return simple_update(con, "wave1.attempts_floor", "attempts = completions", where)


def _targets_from_oracle(con) -> int:
    pbp = registry()["pbp_player_week_rollup"].path
    base_where = ("year >= 1978 AND receptions > 0 AND (targets IS NULL OR targets = 0)")

    # pass 1: authoritative oracle target where it exists and is >= receptions
    n1 = _count(con, base_where)
    con.execute(f"""
        UPDATE st
        SET targets = o.targets,
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}=''
                              THEN 'wave1.targets_from_oracle'
                              ELSE {PROV_COL} || ',wave1.targets_from_oracle' END
        FROM (SELECT player_week, targets FROM '{pbp}' WHERE targets IS NOT NULL) o
        WHERE st.player_week = o.player_week
          AND st.year >= 1978 AND st.receptions > 0 AND (st.targets IS NULL OR st.targets = 0)
          AND o.targets >= st.receptions
    """)

    # pass 2: floor remaining to receptions (oracle missing or below receptions)
    simple_update(
        con, "wave1.targets_floor",
        "targets = receptions",
        base_where,
    )
    # rows changed by this correction = those originally matching base_where
    return n1


CORRECTIONS = [
    Correction("wave1.fg_att_identity",
               "fg_att = made+missed+blocked where understated (accounting identity)",
               _fg_att_identity,
               "fg_att IS NOT NULL AND (COALESCE(fg_made,0)+COALESCE(fg_missed,0)) > fg_att + COALESCE(fg_blocked,0)"),
    Correction("wave1.attempts_floor",
               "attempts = completions where completions>0 and attempts null/0 (ancient box)",
               _attempts_floor,
               "completions > 0 AND (attempts IS NULL OR attempts = 0)"),
    Correction("wave1.targets_from_oracle",
               "targets from PBP oracle (>= receptions) else floor to receptions",
               _targets_from_oracle,
               "year >= 1978 AND receptions > 0 AND (targets IS NULL OR targets = 0)"),
]
