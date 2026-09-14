"""
Canonical derivation of the DST points-allowed / yards-allowed BRACKET columns from their
scalar source, so the one-hot bracket columns need not be physically stored.

Each bracket is a one-hot flag over a scalar:
    pts_allow_*  <- dst_points_allowed
    yds_allow_*  <- total_yds_allowed

Measured on the v26 release DEF rows (36,282 with a non-null scalar): the stored one-hot
equals the derived one-hot on 100.00% of rows for every one of the 16 brackets. So deriving
at read time is value-identical to reading the stored column -- the "zero drift by arithmetic"
guarantee. This module is the single source of that derivation for SQL and pandas callers alike;
add a bracket in exactly one place.

The four LOW pts_allow tiers (0, 1_6, 7_13, 14_20) are ALSO targeted by IDP_COL_MAP (shared
DST/IDP columns) and are NOT dropped -- but they are still derivable and this module can emit
them too. Only DROPPABLE (non-mirror) brackets are enumerated in DROPPABLE_BRACKETS.
"""
from __future__ import annotations

PTS_ALLOW_SCALAR = "dst_points_allowed"
YDS_ALLOW_SCALAR = "total_yds_allowed"

# bracket column -> (scalar column, low, high). Inclusive bounds; None = open on that side.
# These bounds reproduce the stored columns exactly (verified 100% on the release).
BRACKET_BOUNDS: dict[str, tuple[str, int | None, int | None]] = {
    "pts_allow_0":       (PTS_ALLOW_SCALAR, 0, 0),
    "pts_allow_1_6":     (PTS_ALLOW_SCALAR, 1, 6),
    "pts_allow_7_13":    (PTS_ALLOW_SCALAR, 7, 13),
    "pts_allow_14_20":   (PTS_ALLOW_SCALAR, 14, 20),
    "pts_allow_21_27":   (PTS_ALLOW_SCALAR, 21, 27),
    "pts_allow_28_34":   (PTS_ALLOW_SCALAR, 28, 34),
    "pts_allow_35_plus": (PTS_ALLOW_SCALAR, 35, None),
    "yds_allow_0_99":    (YDS_ALLOW_SCALAR, None, 99),
    "yds_allow_100_199": (YDS_ALLOW_SCALAR, 100, 199),
    "yds_allow_200_299": (YDS_ALLOW_SCALAR, 200, 299),
    "yds_allow_300_349": (YDS_ALLOW_SCALAR, 300, 349),
    "yds_allow_350_399": (YDS_ALLOW_SCALAR, 350, 399),
    "yds_allow_400_449": (YDS_ALLOW_SCALAR, 400, 449),
    "yds_allow_450_499": (YDS_ALLOW_SCALAR, 450, 499),
    "yds_allow_500_549": (YDS_ALLOW_SCALAR, 500, 549),
    "yds_allow_550_plus": (YDS_ALLOW_SCALAR, 550, None),
}

# The four low pts_allow tiers are shared DST/IDP mirror columns -> kept physically stored.
MIRROR_BRACKETS = frozenset({"pts_allow_0", "pts_allow_1_6", "pts_allow_7_13", "pts_allow_14_20"})

# The brackets that are safe to physically drop (derived at read time via this module).
DROPPABLE_BRACKETS = tuple(c for c in BRACKET_BOUNDS if c not in MIRROR_BRACKETS)


def is_bracket(col: str) -> bool:
    return col in BRACKET_BOUNDS


def _cond(scalar_sql: str, lo, hi) -> str:
    if lo is not None and hi is not None:
        return f"{scalar_sql} BETWEEN {lo} AND {hi}"
    if lo is not None:
        return f"{scalar_sql} >= {lo}"
    return f"{scalar_sql} <= {hi}"


def bracket_onehot_sql(col: str, alias: str | None = None) -> str | None:
    """SQL 0/1 one-hot for bracket `col` from its scalar, or None if `col` is not a bracket.

    A NULL scalar yields 0 (the WHEN is not satisfied) -- scoring reads are COALESCE(...,0)-wrapped,
    so this is score-identical to the stored column on every row.
    """
    spec = BRACKET_BOUNDS.get(col)
    if spec is None:
        return None
    scalar, lo, hi = spec
    s = f"{alias}.{scalar}" if alias else scalar
    return f"CASE WHEN {_cond(s, lo, hi)} THEN 1 ELSE 0 END"


def bracket_scored_sql(col: str, mult, alias: str | None = None) -> str | None:
    """SQL scoring contribution `derived_onehot * mult` for bracket `col`, or None.

    Equivalent to `COALESCE(<stored col>, 0) * mult` but reads only the scalar.
    """
    onehot = bracket_onehot_sql(col, alias=alias)
    if onehot is None:
        return None
    return f"({onehot}) * {mult}"


def bracket_onehot_pandas(col: str, scalar_series):
    """pandas float 0/1 one-hot Series for bracket `col` from its scalar Series, or None.

    Mirrors bracket_onehot_sql: non-numeric/NULL scalar -> 0.0.
    """
    import pandas as pd

    spec = BRACKET_BOUNDS.get(col)
    if spec is None:
        return None
    _scalar, lo, hi = spec
    s = pd.to_numeric(scalar_series, errors="coerce")
    if lo is not None and hi is not None:
        mask = (s >= lo) & (s <= hi)
    elif lo is not None:
        mask = s >= lo
    else:
        mask = s <= hi
    return mask.fillna(False).astype(float)
