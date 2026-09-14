"""SQL for the draft research career rollup."""
from __future__ import annotations

from cohort_format_sql import CAREER_GRAIN

GROUP_COLUMNS = ",".join(CAREER_GRAIN)


def _weighted_mean(value: str, weight: str, digits: int = 1) -> str:
    return (
        f"ROUND(SUM({value} * {weight}) / "
        f"NULLIF(SUM(CASE WHEN {value} IS NOT NULL THEN {weight} END), 0), {digits})"
    )


def _relation(source: str) -> str:
    if "/" in source or "\\" in source or source.endswith(".parquet"):
        return "'" + source.replace("'", "''") + "'"
    return source


def draft_career_rollup_sql(source: str) -> str:
    """Build one career row per cohort/player with season extrema and their years."""
    table = _relation(source)
    return f"""
SELECT {GROUP_COLUMNS},
  COUNT(DISTINCT year) AS n_years,
  SUM(n_leagues) AS n_leagues,
  SUM(n_drafted) AS n_drafted,
  SUM(COALESCE(n_auction_leagues, 0)) AS n_auction_leagues,
  SUM(COALESCE(n_auction_eligible_leagues, 0)) AS n_auction_eligible_leagues,
  {_weighted_mean('adp', 'n_leagues')} AS adp,
  ROUND(MIN(adp), 1) AS earliest_adp,
  FIRST(year ORDER BY adp ASC NULLS LAST, year ASC) FILTER (WHERE adp IS NOT NULL) AS earliest_adp_year,
  ROUND(MAX(adp), 1) AS latest_adp,
  FIRST(year ORDER BY adp DESC NULLS LAST, year ASC) FILTER (WHERE adp IS NOT NULL) AS latest_adp_year,
  {_weighted_mean('draft_rate_pct', 'n_leagues')} AS draft_rate_pct,
  ROUND(MAX(draft_rate_pct), 2) AS highest_draft_rate,
  FIRST(year ORDER BY draft_rate_pct DESC NULLS LAST, year ASC) FILTER (WHERE draft_rate_pct IS NOT NULL) AS highest_draft_rate_year,
  ROUND(MIN(draft_rate_pct), 2) AS lowest_draft_rate,
  FIRST(year ORDER BY draft_rate_pct ASC NULLS LAST, year ASC) FILTER (WHERE draft_rate_pct IS NOT NULL) AS lowest_draft_rate_year,
  {_weighted_mean('avg_auction_cost_pct', 'COALESCE(n_auction_leagues, 0)', 2)} AS avg_auction_cost_pct,
  ROUND(100.0 * SUM(COALESCE(n_auction_leagues, 0)) /
        NULLIF(SUM(COALESCE(n_auction_eligible_leagues, 0)), 0), 2) AS auction_draft_rate_pct,
  {_weighted_mean('cost_pct', 'COALESCE(n_auction_eligible_leagues, 0)', 2)} AS cost_pct,
  ROUND(MAX(cost_pct), 2) AS highest_cost_pct,
  FIRST(year ORDER BY cost_pct DESC NULLS LAST, year ASC) FILTER (WHERE cost_pct IS NOT NULL) AS highest_cost_pct_year,
  ROUND(MIN(cost_pct), 2) AS lowest_cost_pct,
  FIRST(year ORDER BY cost_pct ASC NULLS LAST, year ASC) FILTER (WHERE cost_pct IS NOT NULL) AS lowest_cost_pct_year,
  ROUND(SUM(COALESCE(cost_pct, 0)), 2) AS career_auction_spend_pct,
  {_weighted_mean('avg_fantasy_points', 'n_leagues')} AS avg_fantasy_points,
  {_weighted_mean('avg_manager_lamar', 'n_leagues')} AS avg_manager_lamar,
  {_weighted_mean('draft_score', 'n_leagues')} AS draft_score,
  ROUND(MAX(draft_score), 1) AS best_draft_score,
  FIRST(year ORDER BY draft_score DESC NULLS LAST, year ASC) FILTER (WHERE draft_score IS NOT NULL) AS best_draft_score_year,
  ROUND(MIN(draft_score), 1) AS worst_draft_score,
  FIRST(year ORDER BY draft_score ASC NULLS LAST, year ASC) FILTER (WHERE draft_score IS NOT NULL) AS worst_draft_score_year,
  {_weighted_mean('draft_score_healthy', 'n_leagues')} AS draft_score_healthy,
  ROUND(MAX(draft_score_healthy), 1) AS best_healthy_score,
  FIRST(year ORDER BY draft_score_healthy DESC NULLS LAST, year ASC) FILTER (WHERE draft_score_healthy IS NOT NULL) AS best_healthy_score_year,
  ROUND(MIN(draft_score_healthy), 1) AS worst_healthy_score,
  FIRST(year ORDER BY draft_score_healthy ASC NULLS LAST, year ASC) FILTER (WHERE draft_score_healthy IS NOT NULL) AS worst_healthy_score_year,
  CASE WHEN SUM(n_leagues) >= 35 THEN 'confident'
       WHEN SUM(n_leagues) >= 10 THEN 'mushy' ELSE 'insufficient' END AS confidence
FROM {table}
GROUP BY {GROUP_COLUMNS}
"""
