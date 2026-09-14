"""
sota_recon/corrections/wave6_passer_rating.py

Wave 6: add the offensive passer rating (it was missing entirely from v26).

Passer rating is FORMULA-DRIVEN: a deterministic closed-form function of completions,
attempts, passing_yards, passing_tds, passing_interceptions -- all of which v26 has and
this session audited (oracle 99.8%+). So computing it for EVERY passer-game with
attempts>0, in every era, is calculation, not fabrication (the formula is era-agnostic;
PFR publishes it retroactively for pre-1973 passers the same way).

    a = clamp((comp/att - 0.3) * 5,        0, 2.375)
    b = clamp((yds/att  - 3) * 0.25,       0, 2.375)
    c = clamp((td/att) * 20,               0, 2.375)
    d = clamp(2.375 - (int/att) * 25,      0, 2.375)
    passer_rating = (a + b + c + d) / 6 * 100

Deterministic -> idempotent (recomputed each run). Computed where attempts>0, NULL
elsewhere (a passer rating with zero attempts is undefined, not zero).
"""

from __future__ import annotations

from .framework import PROV_COL, _count, Correction

_COL = "passer_rating"


def _passer_rating(con) -> int:
    cols = [c[0] for c in con.execute("DESCRIBE st").fetchall()]
    if _COL not in cols:
        con.execute(f"ALTER TABLE st ADD COLUMN {_COL} DOUBLE")

    where = "attempts IS NOT NULL AND attempts > 0"
    n = _count(con, where)
    con.execute(f"""
        UPDATE st SET
            {_COL} = ROUND((
                LEAST(GREATEST((COALESCE(completions,0)/attempts - 0.3) * 5, 0), 2.375) +
                LEAST(GREATEST((COALESCE(passing_yards,0)/attempts - 3) * 0.25, 0), 2.375) +
                LEAST(GREATEST((COALESCE(passing_tds,0)/attempts) * 20, 0), 2.375) +
                LEAST(GREATEST(2.375 - (COALESCE(passing_interceptions,0)/attempts) * 25, 0), 2.375)
            ) / 6 * 100, 1),
            {PROV_COL} = CASE WHEN {PROV_COL} IS NULL OR {PROV_COL}='' THEN 'wave6.passer_rating'
                              ELSE {PROV_COL} || ',wave6.passer_rating' END
        WHERE {where}
    """)
    return n


CORRECTIONS = [
    Correction("wave6.passer_rating",
               "add offensive passer_rating (NFL formula over audited passing inputs, all eras)",
               _passer_rating, "attempts IS NOT NULL AND attempts > 0"),
]
