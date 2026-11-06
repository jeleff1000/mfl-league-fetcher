"""Regular-season byes retain franchise membership without creating game stats."""

from types import SimpleNamespace

import pandas as pd
import pytest

from multi_league.core.league_update_validation import assert_active_season_simulation_health
from multi_league.core.local_db import LocalLeagueDB
from multi_league.data_fetchers.espn.espn_schedules import full_espn_schedule_frame
from multi_league.transformations.aggregation.aggregate_matchup_context import aggregate_matchup_season
from multi_league.transformations.sql_enrichments import SQLEnrichments


def _save_odd_team_season(db, data_dir, *, finalized_week, rotate_bye=True):
    """Persist native schedule witnesses and canonical completed matchup facts."""
    context = SimpleNamespace(
        get_league_id_for_year=lambda year: 123,
        get_manager_name=lambda team, *args: f"Manager {team}",
        get_manager_guid=lambda team, *args: f"owner-{team}",
        get_team_name=lambda team, *args: f"Team {team}",
        get_franchise_id=lambda team, *args: f"fid-{team}",
    )
    native = []
    scores = []
    for week, pairs, bye_team in [
        (1, [(1, 2), (3, 4), (5, 6)], 7),
        (2, [(2, 3), (4, 5), (6, 7)] if rotate_bye else [(1, 2), (3, 4), (5, 6)], 1 if rotate_bye else 7),
    ]:
        for home, away in pairs:
            scores.extend([
                {"week": week, "franchise_id": f"fid-{home}", "team_points": 110.0 + home,
                 "opponent_points": 90.0 + away},
                {"week": week, "franchise_id": f"fid-{away}", "team_points": 90.0 + away,
                 "opponent_points": 110.0 + home},
            ])
            native.append({
                "matchupPeriodId": week, "playoffTierType": "NONE",
                "winner": "HOME" if week <= finalized_week else "UNDECIDED",
                "home": {"teamId": home, "totalPoints": 110.0 + home},
                "away": {"teamId": away, "totalPoints": 90.0 + away},
            })
        native.append({
            "matchupPeriodId": week, "playoffTierType": "NONE", "winner": "UNDECIDED",
            "home": {"teamId": bye_team, "totalPoints": 121.06}, "away": None,
        })
    schedule = full_espn_schedule_frame(
        ctx=context, raw_schedule=native, year=2026, regular_season_weeks=2,
        expected_team_ids=tuple(str(team) for team in range(1, 8)), current_matchup_period=finalized_week + 1,
    )
    facts = schedule.loc[schedule.week.le(finalized_week)].copy()
    facts = facts.merge(pd.DataFrame(scores), how="left", on=["week", "franchise_id"], validate="one_to_one")
    facts["team_key"] = facts.franchise_id.str.removeprefix("fid-")
    # The completed-matchup fetcher emits null game stats on a declared bye,
    # even when ESPN reports that team's roster points in the native schedule.
    bye = facts.is_bye_week.eq(1)
    facts.loc[bye, ["opponent", "team_points", "opponent_points", "win", "loss"]] = None
    facts.loc[bye, ["opponent_franchise_id", "opponent_guid"]] = ""
    db.save_table("matchup", facts, platform="espn", league_id="123")
    db.save_table("schedule", schedule, platform="espn", league_id="123")
    db.save_table("league_settings", pd.DataFrame([
        {"year": 2026, "platform": "espn", "num_teams": 7, "playoff_start_week": 3},
    ]))
    conn = db.connect()
    enrichments = SQLEnrichments("odd_espn", conn=conn, data_dir=str(data_dir))
    enrichments.enforce_postseason_flags()
    enrichments.normalize_matchup_flags()
    enrichments.build_schedule_from_matchup()
    # Simulations populate SQL-owned columns after canonical raw ingestion.
    conn.execute("UPDATE public.matchup SET p_playoffs=10*week, p_champ=5*week")
    return conn


def test_first_week_bye_has_zero_games_and_passes_real_aggregate_health(tmp_path):
    with LocalLeagueDB(tmp_path, "odd_espn") as db:
        conn = _save_odd_team_season(db, tmp_path, finalized_week=1)

        assert aggregate_matchup_season(conn, "odd_espn", year=2026) == 7

        assert conn.execute(
            "SELECT games,wins,losses,ties,total_team_points,total_opponent_points,"
            "avg_team_points,avg_opponent_points,optimal_games,proj_games,p_playoffs,p_champ,"
            "mean_p_playoffs,mean_p_champ FROM public.matchup_season WHERE franchise_id='fid-7'"
        ).fetchone() == (0, 0, 0, 0, 0.0, 0.0, None, None, 0, 0, 10.0, 5.0, 10.0, 5.0)
        assert assert_active_season_simulation_health(conn, db_name="odd_espn", year=2026) == {
            "latest_finalized_week": 1, "latest_simulation_franchises": 7,
            "simulation_season_franchises": 7,
        }


def test_later_bye_uses_current_odds_without_changing_played_stats(tmp_path):
    with LocalLeagueDB(tmp_path, "odd_espn") as db:
        conn = _save_odd_team_season(db, tmp_path, finalized_week=2)

        aggregate_matchup_season(conn, "odd_espn", year=2026)

        assert conn.execute(
            "SELECT games,wins,losses,ties,total_team_points,total_opponent_points,"
            "avg_team_points,avg_opponent_points,p_playoffs,p_champ "
            "FROM public.matchup_season WHERE franchise_id='fid-1'"
        ).fetchone() == (1, 1, 0, 0, 111.0, 92.0, 111.0, 92.0, 20.0, 10.0)
        # A team playing both weeks keeps the ordinary game and point totals.
        assert conn.execute(
            "SELECT games,wins,losses,total_team_points,total_opponent_points,p_playoffs,p_champ "
            "FROM public.matchup_season WHERE franchise_id='fid-2'"
        ).fetchone() == (2, 1, 1, 204.0, 204.0, 20.0, 10.0)
        assert assert_active_season_simulation_health(conn, db_name="odd_espn", year=2026) == {
            "latest_finalized_week": 2, "latest_simulation_franchises": 7,
            "simulation_season_franchises": 7,
        }


def test_consecutive_byes_keep_current_snapshots_and_mean_without_games(tmp_path):
    with LocalLeagueDB(tmp_path, "odd_espn") as db:
        conn = _save_odd_team_season(db, tmp_path, finalized_week=2, rotate_bye=False)

        aggregate_matchup_season(conn, "odd_espn", year=2026)

        assert conn.execute(
            "SELECT games,wins,losses,ties,total_team_points,avg_team_points,p_playoffs,p_champ,"
            "mean_p_playoffs,mean_p_champ FROM public.matchup_season WHERE franchise_id='fid-7'"
        ).fetchone() == (0, 0, 0, 0, 0.0, None, 20.0, 10.0, 15.0, 7.5)
        assert assert_active_season_simulation_health(conn, db_name="odd_espn", year=2026)[
            "simulation_season_franchises"
        ] == 7


@pytest.mark.parametrize("invalid_bye", [
    "is_placeholder=1", "is_playoffs=1", "is_consolation=1", "is_bye_week=0", "team_points=121.06",
])
def test_only_regular_null_score_byes_create_zero_game_membership(tmp_path, invalid_bye):
    with LocalLeagueDB(tmp_path, "odd_espn") as db:
        conn = _save_odd_team_season(db, tmp_path, finalized_week=1)
        conn.execute("ALTER TABLE public.matchup ADD COLUMN is_placeholder BOOLEAN DEFAULT FALSE")
        conn.execute(f"UPDATE public.matchup SET {invalid_bye} WHERE franchise_id='fid-7'")

        aggregate_matchup_season(conn, "odd_espn", year=2026)

        assert conn.execute(
            "SELECT COUNT(*) FROM public.matchup_season WHERE franchise_id='fid-7'"
        ).fetchone() == (0,)
