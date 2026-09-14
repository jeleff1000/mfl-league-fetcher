from __future__ import annotations

import pytest

from top10_canonical_values import canonical_season_sql, canonical_weekly_sql


def test_canonical_season_sql_uses_exact_slug_values() -> None:
    sql = " ".join(canonical_season_sql(2025, "12t_flx_ppr_4pt").split())

    assert "public.player_slug_value" in sql
    assert "year=2025" in sql
    assert "slug='12t_flx_ppr_4pt'" in sql
    assert "canonical_points" in sql and "canonical_ppg" in sql
    assert "canonical_lamar" in sql and "canonical_lamar_ppg" in sql
    assert "GROUP BY NFL_player_id" in sql


def test_canonical_weekly_sql_retains_week() -> None:
    sql = " ".join(canonical_weekly_sql(2025, "10t_flx_half_6pt").split())

    assert "week" in sql
    assert "canonical_points" in sql and "canonical_lamar" in sql
    assert "GROUP BY NFL_player_id,week" in sql


@pytest.mark.parametrize(
    "slug",
    ("12t_sflx_ppr_4pt", "8t_flx_ppr_4pt", "12t_flx_ppr_4pt' OR true"),
)
def test_canonical_sql_rejects_noncanonical_or_unsafe_slugs(slug: str) -> None:
    with pytest.raises(ValueError, match="canonical flx slug"):
        canonical_season_sql(2025, slug)
