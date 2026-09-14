"""DuckDB SQL for bottom-up research matchup career rows."""


def matchup_career_sql(source: str) -> str:
    """Return a career SELECT over an expanded season matchup relation."""
    return f"""
      SELECT teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,
        cohort_level,format_level,NFL_player_id,
        COUNT(DISTINCT year) AS n_years,
        SUM(n_leagues) AS n_leagues,
        -- Season active_weeks is the team-game row count used by the weekly
        -- rollup, not the player's NFL-game availability count.  Summing it
        -- here inflated career Active Wks by league support (especially DST).
        SUM(nfl_active_weeks) AS active_weeks,
        -- Internal support for Expected W-L: one row per NFL team game, not
        -- the player's own active-game count exposed above.
        SUM(active_weeks) AS team_game_weeks,
        SUM(started_weeks) AS started_weeks,
        SUM(elig_league_weeks) AS elig_league_weeks,
        SUM(champ_elig_leagues) AS champ_elig_leagues,
        SUM(playoff_eligible_leagues) AS playoff_eligible_leagues,
        SUM(po_n_rostered_leagues) AS po_n_rostered_leagues,
        -- A championship start proves playoff participation; preserve that
        -- closure even when the playoff signal was absent on the source row.
        SUM(GREATEST(n_started_po, n_champ_start_leagues)) AS n_started_po,
        -- A playoff start proves playoff membership; keep the career rollup
        -- monotone even if an older season row lacked the final-week signal.
        SUM(GREATEST(n_final_po, n_started_po, n_champ_start_leagues)) AS n_final_po,
        SUM(n_champ_leagues) AS n_champ_leagues,
        SUM(n_champ_start_leagues) AS n_champ_start_leagues,
        SUM(inactive_weeks) AS inactive_weeks,
        -- Preserve the additive primitives so the assembled career table can be
        -- audited against the displayed rates instead of trusting derived values.
        SUM(rostered_league_weeks) AS rostered_league_weeks,
        SUM(roster_eligible_league_weeks) AS roster_eligible_league_weeks,
        SUM(started_team_game_weeks) AS started_team_game_weeks,
        SUM(started_active_weeks) AS started_active_weeks,
        SUM(healthy_eligible_league_weeks) AS healthy_eligible_league_weeks,
        SUM(po_wkwt_credit) AS po_wkwt_credit,
        SUM(wins_started) AS wins_started,
        SUM(losses_started) AS losses_started,
        SUM(team_game_eligible_league_weeks) AS team_game_eligible_league_weeks,
        -- Career usage is rebuilt from the same additive weekly components as season.
        -- Never average already-derived weekly or seasonal percentages: league support
        -- varies by week, position, injury, and postseason participation.
        100.0 * SUM(rostered_league_weeks)
          / NULLIF(SUM(roster_eligible_league_weeks),0) AS roster_rate_pct,
        100.0 * SUM(started_team_game_weeks)
          / NULLIF(SUM(team_game_eligible_league_weeks),0) AS start_rate_pct,
        100.0 * SUM(started_active_weeks)
          / NULLIF(SUM(healthy_eligible_league_weeks),0) AS healthy_start_rate_pct,
        SUM(expected_wins) AS expected_wins,
        SUM(expected_losses) AS expected_losses,
        SUM(expected_starts) AS expected_starts,
        100.0 * SUM(expected_wins)
          / NULLIF(SUM(expected_starts),0) AS win_rate_pct,
        SUM(total_points_observed) AS total_points_observed,
        SUM(ppg_when_started * started_weeks)
          / NULLIF(SUM(CASE WHEN ppg_when_started IS NOT NULL THEN started_weeks END),0)
          AS ppg_when_started,
        SUM(total_lamar_started) AS total_lamar_started,
         -- Clutch is a weighted average across season rows.  Summing season averages
         -- turns a bounded weekly/season metric into a career-sized number.
        SUM(avg_clutch_started * champ_elig_leagues)
           / NULLIF(SUM(CASE WHEN avg_clutch_started IS NOT NULL THEN champ_elig_leagues END),0)
           AS avg_clutch_started,
        SUM(sum_clutch_started_active) AS sum_clutch_started_active,
        -- This is a week-rate diagnostic, so its denominator must be the summed
        -- champion-eligible league-weeks, not the season league count. Using the
        -- latter made a multi-week championship flag produce values above 100%.
        100.0 * SUM(started_champ_active) / NULLIF(SUM(champ_eligible_league_weeks),0)
          AS champ_week_rate_pct,
        100.0 * SUM(GREATEST(n_started_po, n_champ_start_leagues))
          / NULLIF(SUM(po_n_rostered_leagues),0)
          AS playoff_rate_started,
        100.0 * SUM(po_wkwt_credit) / NULLIF(SUM(po_n_rostered_leagues),0)
          AS playoff_rate_wkwt,
        -- SERVED T7 rates (LOCKED denominators): eligible LEAGUE-YEARS at career grain --
        -- the same SUM(n_leagues) that roster_rate_pct divides by, so a player who was
        -- eligible in more seasons is not rewarded for it.
        100.0 * SUM(n_final_po) / NULLIF(SUM(playoff_eligible_leagues),0) AS playoff_total_pct,
        100.0 * SUM(GREATEST(n_started_po, n_champ_start_leagues))
          / NULLIF(SUM(playoff_eligible_leagues),0) AS playoff_as_starter_pct,
        100.0 * SUM(n_champ_leagues) / NULLIF(SUM(champ_elig_leagues),0) AS champ_total_pct,
        100.0 * SUM(n_champ_start_leagues) / NULLIF(SUM(champ_elig_leagues),0) AS champ_as_starter_pct,
        -- Expected counts are the career SUM of the per-season served rates, so they must
        -- ride the SAME denominator those rates now use (eligible leagues). Left on
        -- champ_elig_leagues / po_n_rostered_leagues they would silently disagree with the
        -- Champ %/Playoff % columns sitting next to them in the same row.
        SUM(1.0 * n_champ_start_leagues / NULLIF(champ_elig_leagues,0)) AS expected_champs,
        SUM(1.0 * GREATEST(n_started_po, n_champ_start_leagues)
          / NULLIF(playoff_eligible_leagues,0)) AS expected_playoffs,
        -- Expected W-L is on the all-start exposure basis. Expected losses are the
        -- complement of expected wins, so the two lanes always sum to expected starts.
        100.0 * SUM(expected_wins)
          / NULLIF(SUM(team_game_eligible_league_weeks),0)
          AS won_pct,
        100.0 * SUM(expected_losses)
          / NULLIF(SUM(team_game_eligible_league_weeks),0)
          AS lost_pct,
        -- diagnostics for how much of the all-start lane had a known result
        -- Keep team-game support separately for the expected-W/L diagnostic.
        SUM(CASE WHEN expected_wins IS NOT NULL THEN active_weeks END) AS decided_active_weeks,
        SUM(CASE WHEN expected_wins IS NOT NULL THEN weekly_start_rate_sum END)
          / NULLIF(SUM(CASE WHEN expected_wins IS NOT NULL THEN active_weeks END),0)
          AS start_rate_decided_pct,
        CASE WHEN SUM(started_weeks) >= 35*15 THEN 'confident'
             WHEN SUM(started_weeks) >= 10*15 THEN 'mushy'
             ELSE 'insufficient' END AS confidence
      FROM {source}
      WHERE COALESCE(elig_league_weeks,0) > 0 OR COALESCE(started_weeks,0) > 0
      GROUP BY teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,
        cohort_level,format_level,NFL_player_id
    """
