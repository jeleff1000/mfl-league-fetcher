"""Shared DuckDB projections for research matchup weekly-to-season metrics.

Both builders consume the same weekly primitive column contract so the served
weekly values and the season rollup cannot drift apart.
"""


def is_research_week(year: int, week: int) -> bool:
    """Whether an NFL week belongs in the research calendar for ``year``.

    Fantasy playoff weeks stay in scope.  The only exclusion is the final NFL
    week, whose number moved from 17 to 18 when the league expanded in 2021.
    """
    return week <= (16 if year <= 2020 else 17)


def weekly_metric_select(alias: str) -> str:
    """Return unrounded weekly display/rollup expressions for ``alias``."""
    return f"""
      100.0 * {alias}.rostered_leagues
          / NULLIF({alias}.roster_eligible_leagues, 0) AS roster_rate_pct,
      100.0 * {alias}.started_leagues
          / NULLIF({alias}.team_game_eligible_leagues, 0) AS start_rate_pct,
      -- Healthy means the player himself appeared in the NFL game. Start means his NFL team
      -- played. These only coincide for a player who appeared in every team game.
      100.0 * {alias}.healthy_started_leagues
          / NULLIF({alias}.healthy_eligible_leagues, 0) AS healthy_start_rate_pct,
      -- Win% is wins in ALL starts. Unknown/tied starts stay out of W-L credit,
      -- but are not removed from the Win% denominator.
      100.0 * {alias}.wins_started
          / NULLIF({alias}.started_leagues, 0) AS win_rate_pct,
      1.0 * {alias}.started_leagues
          / NULLIF({alias}.team_game_eligible_leagues, 0)
          * {alias}.wins_started / NULLIF({alias}.started_leagues, 0) AS expected_wins,
      1.0 * {alias}.started_leagues
          / NULLIF({alias}.team_game_eligible_leagues, 0)
          * (1.0 - {alias}.wins_started / NULLIF({alias}.started_leagues, 0))
          AS expected_losses,
      1.0 * {alias}.started_leagues
          / NULLIF({alias}.team_game_eligible_leagues, 0) AS expected_starts
    """.strip()


def season_expected_outcome_sql(
    alias: str,
    outcome: str,
    *,
    team_game_denominator: str = "team_game_eligible_leagues",
) -> str:
    """Expected wins/losses on the full eligible-start basis.

    Expected W is the weekly start share multiplied by wins in starts. Expected L
    is its complement at the same basis, so unresolved raw outcomes cannot make
    Expected W + Expected L fall below Expected Starts.
    """
    if outcome not in {"wins_started", "losses_started"}:
        raise ValueError(f"unsupported matchup outcome: {outcome}")
    start_share = (f"(1.0 * {alias}.started_leagues / NULLIF("
                   f"{alias}.{team_game_denominator}, 0))")
    win_share = f"({start_share} * {alias}.wins_started / NULLIF({alias}.started_leagues, 0))"
    if outcome == "wins_started":
        return f"SUM({win_share})"
    return f"SUM({start_share}) - SUM({win_share})"


def season_metric_select(
    alias: str,
    *,
    roster_denominator: str = "roster_eligible_leagues",
    team_game_denominator: str = "team_game_eligible_leagues",
    healthy_denominator: str = "healthy_eligible_leagues",
    healthy_numerator: str = "healthy_started_leagues",
) -> str:
    """Return bottom-up season expressions over active weekly primitives."""
    expected_wins = season_expected_outcome_sql(
        alias, "wins_started", team_game_denominator=team_game_denominator)
    expected_losses = season_expected_outcome_sql(
        alias, "losses_started", team_game_denominator=team_game_denominator)
    return f"""
      100.0 * SUM({alias}.rostered_leagues)
          / NULLIF(SUM({alias}.{roster_denominator}), 0) AS roster_rate_pct,
      100.0 * SUM({alias}.started_leagues)
          / NULLIF(SUM({alias}.{team_game_denominator}), 0) AS start_rate_pct,
      100.0 * SUM({alias}.{healthy_numerator})
          / NULLIF(SUM({alias}.{healthy_denominator}), 0) AS healthy_start_rate_pct,
      SUM({alias}.rostered_leagues) AS rostered_league_weeks,
      {expected_wins} AS expected_wins,
      {expected_losses} AS expected_losses,
      SUM(1.0 * {alias}.started_leagues
          / NULLIF({alias}.{team_game_denominator}, 0)) AS expected_starts,
      100.0 * {expected_wins}
          / NULLIF(SUM(1.0 * {alias}.started_leagues
              / NULLIF({alias}.{team_game_denominator}, 0)), 0) AS win_rate_pct,
      SUM(1.0 * {alias}.{healthy_numerator}
          / NULLIF({alias}.{healthy_denominator}, 0)) AS expected_starts_healthy,
      SUM({alias}.lamar_weighted) AS total_lamar_started,
      SUM({alias}.clutch_weighted) AS avg_clutch_started
    """.strip()
