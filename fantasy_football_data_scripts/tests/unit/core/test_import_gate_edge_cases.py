"""Valid provider metadata must not become an import/publication blocker."""

from types import SimpleNamespace

import duckdb
import pytest

from multi_league.core.league_update_plan import _derived_refresh_gaps
from multi_league.core.league_update_validation import IncompleteSourceError, assert_active_season_simulation_health
from multi_league.data_fetchers.sleeper.sleeper_draft import SleeperDraftFetcher


@pytest.mark.parametrize("order", [None, {}, {"user-1": 1}, {"user-1": 1, "user-2": 2}])
@pytest.mark.parametrize("picks", [[1, 2, 3, 4], [1, 2, 3], [1, 2, 2, 4]])
def test_completed_sleeper_draft_uses_native_team_count_not_user_order(order, picks):
    # Sleeper's documented league-drafts response permits draft_order=null.
    fetcher = object.__new__(SleeperDraftFetcher)
    fetcher.ctx = SimpleNamespace(get_league_id_for_year=lambda year: "league")
    fetcher.client = SimpleNamespace(
        get_league_drafts=lambda league: [{"draft_id": "primary", "season": "2026", "status": "complete",
            "draft_order": order, "slot_to_roster_id": {"1": 10, "2": 20},
            "settings": {"teams": 2, "rounds": 2}}],
        get_draft_picks=lambda draft: [{"pick_no": pick} for pick in picks],
    )
    if picks != [1, 2, 3, 4]:
        with pytest.raises(ValueError, match="incomplete picks"):
            fetcher.fetch_draft_manifest_for_year(2026, expected_primary_draft_id="primary")
    else:
        assert fetcher.fetch_draft_manifest_for_year(2026, expected_primary_draft_id="primary").values.tolist() == [
            ["primary", 1], ["primary", 2], ["primary", 3], ["primary", 4]]


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("missing_week", [None, 2, 3])
@pytest.mark.parametrize("gate", ["plan", "publish"])
def test_schedule_gates_use_configured_start_without_ignoring_real_gaps(platform, missing_week, gate):
    with duckdb.connect() as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.league_settings AS SELECT 'league' AS db_name, 2026 AS year, "
                     "2 AS num_teams, 2 AS start_week, 4 AS playoff_start_week, ? AS platform", [platform])
        conn.execute("CREATE TABLE public.matchup AS SELECT 'league' AS db_name, 2026 AS year, "
                     "3 AS week, id AS franchise_id, id AS manager, 100 AS team_points, "
                     "90 AS opponent_points, 50.0 AS p_playoffs, 50.0 AS p_champ "
                     "FROM (VALUES ('A'), ('B')) t(id)")
        conn.execute("CREATE TABLE public.matchup_season AS "
                     "SELECT db_name, year, franchise_id, p_playoffs, p_champ FROM public.matchup")
        conn.execute("CREATE TABLE public.schedule AS SELECT 'league' AS db_name, 2026 AS year, "
                     "w AS week, id AS franchise_id, CASE id WHEN 'A' THEN 'B' ELSE 'A' END "
                     "AS opponent_franchise_id, FALSE AS is_playoffs "
                     "FROM (VALUES (2),(3)) weeks(w) CROSS JOIN (VALUES ('A'),('B')) teams(id)")
        for table in ('player_fantasy', 'player_fantasy_season', 'player_fantasy_season_all'):
            conn.execute(f"CREATE TABLE public.{table} (db_name VARCHAR, year INTEGER, "
                         "week INTEGER, NFL_player_id VARCHAR)")
        conn.execute("CREATE TABLE public.standings_by_year AS SELECT db_name, year, franchise_id FROM public.matchup")
        if missing_week:
            conn.execute("DELETE FROM public.schedule WHERE week = ?", [missing_week])
        class Reader:
            def query(self, sql, **kwargs):
                result = conn.execute(sql)
                return [dict(zip([col[0] for col in result.description], row)) for row in result.fetchall()]
        if gate == "plan":
            gaps, missing_schedule = _derived_refresh_gaps(Reader(), safe_db="league", active_season=2026)
            assert gaps == set()
            assert missing_schedule is (missing_week is not None)
        elif missing_week:
            with pytest.raises(IncompleteSourceError, match="missing or incomplete weeks"):
                assert_active_season_simulation_health(conn, db_name="league", year=2026)
        else:
            assert assert_active_season_simulation_health(conn, db_name="league", year=2026) == {
                "latest_finalized_week": 3, "latest_simulation_franchises": 2, "simulation_season_franchises": 2}
