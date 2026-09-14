"""Canonical super-table values used by rank-only resampling boards."""

from __future__ import annotations


CANONICAL_FLX_SLUGS = frozenset(
    f"{teams}_flx_{ppr}_{td}"
    for teams in ("10t", "12t")
    for ppr in ("std", "half", "ppr")
    for td in ("4pt", "6pt")
)


def _checked_slug(slug: str) -> str:
    value = str(slug)
    if value not in CANONICAL_FLX_SLUGS:
        raise ValueError(f"not a canonical flx slug: {value!r}")
    return value


def canonical_season_sql(year: int, slug: str) -> str:
    """Exact season totals and per-active-game values for one scoring slug."""
    selected = _checked_slug(slug)
    return f"""
SELECT NFL_player_id,
       SUM(COALESCE(fpts,0)) AS canonical_points,
       SUM(COALESCE(fpts,0))/NULLIF(COUNT(*),0) AS canonical_ppg,
       SUM(lamar) AS canonical_lamar,
       AVG(lamar) AS canonical_lamar_ppg
FROM public.player_slug_value
WHERE year={int(year)} AND slug='{selected}'
GROUP BY NFL_player_id
"""


def canonical_weekly_sql(year: int, slug: str) -> str:
    """Exact week values for one scoring slug."""
    selected = _checked_slug(slug)
    return f"""
SELECT NFL_player_id,week,
       ANY_VALUE(fpts) AS canonical_points,
       ANY_VALUE(lamar) AS canonical_lamar
FROM public.player_slug_value
WHERE year={int(year)} AND slug='{selected}'
GROUP BY NFL_player_id,week
"""
