"""A complete odd-team ESPN graph distinguishes a bye from missing data."""

from copy import deepcopy
from types import SimpleNamespace

import pytest

from multi_league.core.league_refresh import espn_schedule_is_final
from multi_league.core.league_update_validation import IncompleteSourceError
from multi_league.data_fetchers.espn import espn_api_client, espn_schedules


TEAM_IDS = ("1", "2", "3", "4", "7", "9", "11")


def native_week(week=1, *, live=False):
    # Sanitized native week 1 from league 1454840497; no league/member data.
    return [
        {"id": 1, "matchupPeriodId": week, "playoffTierType": "NONE", "winner": "UNDECIDED" if live else "AWAY",
         "home": {"teamId": 11, "totalPoints": 93.22}, "away": {"teamId": 1, "totalPoints": 164.06}},
        {"id": 2, "matchupPeriodId": week, "playoffTierType": "NONE", "winner": "UNDECIDED",
         "home": {"teamId": 9, "totalPoints": 121.06}, "away": None},
        {"id": 3, "matchupPeriodId": week, "playoffTierType": "NONE", "winner": "HOME",
         "home": {"teamId": 3, "totalPoints": 156.3}, "away": {"teamId": 7, "totalPoints": 123.4}},
        {"id": 4, "matchupPeriodId": week, "playoffTierType": "NONE", "winner": "HOME",
         "home": {"teamId": 2, "totalPoints": 164.86}, "away": {"teamId": 4, "totalPoints": 106.36}},
    ]


def context():
    return SimpleNamespace(
        espn_s2=None, swid=None, playoff_start_week=3,
        get_league_id_for_year=lambda year: 123,
        get_manager_name=lambda team, *args: f"Manager {team}",
        get_manager_guid=lambda team, *args: f"owner-{team}",
        get_team_name=lambda team, *args: f"Team {team}",
        get_franchise_id=lambda team, *args: f"fid-{team}",
    )


def test_complete_odd_team_bye_allows_final_regular_week():
    assert espn_schedule_is_final(native_week(), expected_team_ids=TEAM_IDS)


@pytest.mark.parametrize("variant", ["live", "missing_pair", "missing_side", "duplicate", "self_pair", "unknown_tier", "bye_winner", "multiple_byes", "mixed_period"])
def test_odd_team_bye_never_hides_an_incomplete_or_live_period(variant):
    rows = native_week(live=variant == "live")
    if variant == "missing_pair": rows.pop()
    if variant == "missing_side": rows[0]["away"] = {}
    if variant == "duplicate": rows[0]["away"]["teamId"] = 9
    if variant == "self_pair": rows[0]["away"]["teamId"] = 11
    if variant == "unknown_tier": rows[1]["playoffTierType"] = "UNKNOWN"
    if variant == "bye_winner": rows[1]["winner"] = "HOME"
    if variant == "multiple_byes":
        rows[0]["away"] = None
        rows[0]["winner"] = "UNDECIDED"
        rows.append({"home": {"teamId": 1}, "away": None, "winner": "UNDECIDED", "playoffTierType": "NONE", "matchupPeriodId": 1})
    if variant == "mixed_period": rows[1]["matchupPeriodId"] = 2
    assert not espn_schedule_is_final(rows, expected_team_ids=TEAM_IDS)


def test_regular_bye_requires_a_real_odd_team_inventory():
    assert not espn_schedule_is_final(native_week())
    assert not espn_schedule_is_final(native_week(), expected_team_ids=(*TEAM_IDS, "12"))


@pytest.mark.parametrize("mode", ["quick", "full", "refresh"])
def test_native_regular_byes_reach_schedule_with_null_results(monkeypatch, mode):
    rows = native_week() + native_week(2, live=True)
    original = deepcopy(rows)
    ctx = context()
    ctx.import_mode = mode
    if mode == "refresh":
        from pathlib import Path
        monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[4] / "scripts"))
        from refresh_espn_active_season import _full_espn_schedule_frame
        frame = _full_espn_schedule_frame(
            ctx=ctx, raw_schedule=rows, year=2026, regular_season_weeks=2,
            expected_team_ids=TEAM_IDS, current_matchup_period=2,
        )
    else:
        class Client:
            def __init__(self, *args): pass
            def get_raw_league(self, year, views):
                return {"status": {"currentMatchupPeriod": 2}, "teams": [{"id": int(t)} for t in TEAM_IDS],
                        "settings": {"scheduleSettings": {"matchupPeriodCount": 2}}, "schedule": rows}
        monkeypatch.setattr(espn_api_client, "ESPNAPIClient", Client)
        monkeypatch.setattr("multi_league.core.date_utils.get_current_nfl_season_year", lambda: 2026)
        monkeypatch.setattr("multi_league.data_fetchers.espn.espn_league_settings.load_espn_settings", lambda *a: {})
        frame = espn_schedules.fetch_espn_schedule(ctx, 2026)

    assert frame.groupby("week").size().to_dict() == {1: 7, 2: 7}
    bye = frame.loc[frame.franchise_id.eq("fid-9")]
    assert bye.opponent.tolist() == ["BYE", "BYE"]
    assert bye.is_bye_week.tolist() == [1, 1]
    assert bye[["opponent_guid", "opponent_franchise_id", "win", "loss"]].isna().all().all()
    assert rows == original


def test_past_one_sided_regular_schedule_still_rejects_truncation():
    rows = native_week()
    rows[0]["away"] = None
    with pytest.raises(IncompleteSourceError):
        espn_schedules.full_espn_schedule_frame(
            ctx=context(), raw_schedule=rows, year=2026, regular_season_weeks=1,
            expected_team_ids=TEAM_IDS, current_matchup_period=2,
        )


@pytest.mark.parametrize("current_period, accepted", [(1, False), (2, True)])
def test_refresh_closure_accepts_verified_bye_only_after_period_ends(monkeypatch, current_period, accepted):
    from pathlib import Path
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[4] / "scripts"))
    from refresh_espn_active_season import _closed_espn_schedule

    rows = native_week()
    closed = _closed_espn_schedule(
        rows, requested_week=1, current_matchup_period=current_period, expected_team_ids=TEAM_IDS,
    )
    assert (closed is not None) is accepted
    if accepted:
        assert closed == rows


@pytest.mark.parametrize("bye_points", [0.0, 121.06])
def test_refresh_lineup_score_recovery_preserves_verified_bye(monkeypatch, bye_points):
    from pathlib import Path
    import pandas as pd
    monkeypatch.syspath_prepend(str(Path(__file__).resolve().parents[4] / "scripts"))
    from refresh_espn_active_season import _hydrate_zero_espn_schedule_scores

    rows = native_week()
    for row in rows:
        for side in (row["home"], row["away"]):
            if side is not None: side["totalPoints"] = 0.0
    rows[1]["home"]["totalPoints"] = bye_points
    rosters = pd.DataFrame([
        {"week": 1, "team_key": team, "fantasy_points": points, "is_started": True}
        for team, points in zip(TEAM_IDS, [164.06, 164.86, 156.3, 106.36, 123.4, 121.06, 93.22])
    ])
    _hydrate_zero_espn_schedule_scores({1: rows}, rosters)
    assert rows[0]["home"]["totalPoints"] == 93.22
    assert rows[0]["away"]["totalPoints"] == 164.06
    assert rows[1]["away"] is None
    assert rows[1]["home"]["totalPoints"] == bye_points
    assert rows[1]["winner"] == "UNDECIDED"
    assert espn_schedule_is_final(rows, expected_team_ids=TEAM_IDS)


@pytest.fixture
def bye_simulation_db():
    import duckdb
    conn = duckdb.connect()
    conn.execute("CREATE SCHEMA public")
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, year INTEGER, week INTEGER, franchise_id VARCHAR, team_points DOUBLE, opponent_points DOUBLE, is_bye_week BOOLEAN, p_playoffs DOUBLE, p_champ DOUBLE)")
    conn.execute("CREATE TABLE public.matchup_season (db_name VARCHAR, year INTEGER, franchise_id VARCHAR, p_playoffs DOUBLE, p_champ DOUBLE)")
    conn.execute("CREATE TABLE public.league_settings (db_name VARCHAR, year INTEGER, num_teams INTEGER, playoff_start_week INTEGER)")
    conn.execute("INSERT INTO public.league_settings VALUES ('odd_espn',2026,7,3)")
    schedule = espn_schedules.full_espn_schedule_frame(
        ctx=context(), raw_schedule=native_week() + native_week(2, live=True), year=2026,
        regular_season_weeks=2, expected_team_ids=TEAM_IDS, current_matchup_period=2,
    )
    schedule["db_name"] = "odd_espn"
    conn.register("native_schedule", schedule)
    conn.execute("CREATE TABLE public.schedule AS SELECT * FROM native_schedule")
    for team in TEAM_IDS:
        conn.execute("INSERT INTO public.matchup VALUES ('odd_espn',2026,1,?,?,?, ?,50,5)",
                     [f"fid-{team}", None if team == "9" else 100, None if team == "9" else 90, team == "9"])
        conn.execute("INSERT INTO public.matchup_season VALUES ('odd_espn',2026,?,50,5)", [f"fid-{team}"])
    yield conn
    conn.close()


def test_simulation_health_includes_verified_null_score_bye(bye_simulation_db):
    from multi_league.core.league_update_validation import assert_active_season_simulation_health
    assert assert_active_season_simulation_health(bye_simulation_db, db_name="odd_espn", year=2026) == {
        "latest_finalized_week": 1, "latest_simulation_franchises": 7, "simulation_season_franchises": 7,
    }


@pytest.mark.parametrize("mutation", ["missing_bye_odds", "missing_bye_row", "missing_played_score", "false_bye", "truncated_future"])
def test_simulation_health_keeps_completeness_checks_for_bye_leagues(bye_simulation_db, mutation):
    from multi_league.core.league_update_validation import assert_active_season_simulation_health
    queries = {
        "missing_bye_odds": "UPDATE public.matchup SET p_playoffs=NULL WHERE franchise_id='fid-9'",
        "missing_bye_row": "DELETE FROM public.matchup WHERE franchise_id='fid-9'",
        "missing_played_score": "UPDATE public.matchup SET opponent_points=NULL WHERE franchise_id='fid-1'",
        "false_bye": "UPDATE public.schedule SET opponent='TBD',is_bye_week=0 WHERE franchise_id='fid-9' AND week=1",
        "truncated_future": "DELETE FROM public.schedule WHERE franchise_id='fid-1' AND week=2",
    }
    bye_simulation_db.execute(queries[mutation])
    with pytest.raises(IncompleteSourceError):
        assert_active_season_simulation_health(bye_simulation_db, db_name="odd_espn", year=2026)


def test_bye_team_roster_points_still_follow_finalized_nfl_games():
    import pandas as pd
    from multi_league.core.league_refresh import filter_rosters_to_finalized_games
    rosters = pd.DataFrame([
        {"team_key": "9", "nfl_team": "ATL", "fantasy_points": 20},
        {"team_key": "9", "nfl_team": "SEA", "fantasy_points": 7},
    ])
    finalized = pd.DataFrame([{"nfl_team": "ATL", "opponent_nfl_team": "TB"}])
    safe = filter_rosters_to_finalized_games(rosters, finalized)
    assert safe[["team_key", "fantasy_points"]].values.tolist() == [["9", 20]]


def test_regular_bye_simulation_retains_seven_teams_without_inventing_a_game(monkeypatch):
    import pandas as pd
    from multi_league.transformations.matchup import playoff_odds_import as poi

    monkeypatch.setattr(poi, "N_SIMS", 128)
    next_week = native_week(2, live=True)
    # The native bye rotates: team 9 plays in week 2, team 11 has the bye.
    for row in next_week:
        for side in (row["home"], row["away"]):
            if side is not None and side["teamId"] in (9, 11):
                side["teamId"] = {9: 11, 11: 9}[side["teamId"]]
    schedule = espn_schedules.full_espn_schedule_frame(
        ctx=context(), raw_schedule=native_week() + next_week, year=2026,
        regular_season_weeks=2, expected_team_ids=TEAM_IDS, current_matchup_period=2,
    )
    played = pd.DataFrame([
        {"year": 2026, "week": 1, "manager": f"Manager {team}", "opponent": f"Manager {opp}",
         "franchise_id": f"fid-{team}", "opponent_franchise_id": f"fid-{opp}",
         "team_points": points, "opponent_points": opposing_points, "win": won, "loss": 1-won}
        for team, opp, points, opposing_points, won in [
            (11, 1, 93.22, 164.06, 0), (1, 11, 164.06, 93.22, 1),
            (3, 7, 156.3, 123.4, 1), (7, 3, 123.4, 156.3, 0),
            (2, 4, 164.86, 106.36, 1), (4, 2, 106.36, 164.86, 0)]
    ])
    future = poi.future_regular_from_schedule(schedule, 2026, 1)
    assert len(future) == 3
    assert "fid-11" not in set(future.franchise_id) | set(future.opponent_franchise_id)
    fids = {f"fid-{team}" for team in TEAM_IDS}
    pack, seeds, _ = poi._vectorized_regular_and_bracket(
        played, future, {fid: 100 for fid in fids}, {fid: 10 for fid in fids},
        2026, 1, playoff_slots=5, bye_slots=3, regular_season_weeks=2, num_teams=7,
    )
    assert set(pack["P_Champ"].index) == fids
    assert pack["P_Champ"].sum() == pytest.approx(100)
    assert pack["Exp_Final_Wins"].sum() == pytest.approx(6)
    assert seeds.shape == (7, 7)


def test_sql_schedule_rebuild_preserves_bye_witness_for_refresh_health(tmp_path, bye_simulation_db):
    import pandas as pd
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.league_update_validation import assert_active_season_simulation_health
    from multi_league.transformations.sql_enrichments import SQLEnrichments

    schedule = espn_schedules.full_espn_schedule_frame(
        ctx=context(), raw_schedule=native_week() + native_week(2, live=True), year=2026,
        regular_season_weeks=2, expected_team_ids=TEAM_IDS, current_matchup_period=2,
    )
    facts = schedule.loc[schedule.week.eq(1)].copy()
    facts["team_key"] = facts.franchise_id.str.removeprefix("fid-")
    # Matchup's canonical bye convention is null opponent/scores, empty IDs.
    bye = facts.is_bye_week.eq(1)
    facts.loc[bye, "opponent"] = None
    facts.loc[bye, "opponent_franchise_id"] = ""
    facts.loc[bye, "opponent_guid"] = ""
    facts["team_points"] = [None if flag else 100.0 for flag in bye]
    facts["opponent_points"] = [None if flag else 90.0 for flag in bye]
    with LocalLeagueDB(tmp_path, "odd_espn") as db:
        db.save_table("matchup", facts)
        db.save_table("schedule", schedule)
        db.save_table("league_settings", pd.DataFrame([{"year": 2026, "num_teams": 7, "playoff_start_week": 3}]))
        enrichments = SQLEnrichments("odd_espn", conn=db.connect(), data_dir=str(tmp_path))
        # The canonical normalizer drops SQL-owned flags; the common pipeline
        # restores them before deriving the played schedule.
        enrichments.enforce_postseason_flags()
        enrichments.normalize_matchup_flags()
        enrichments.build_schedule_from_matchup()
        rebuilt = db.read_table("schedule")
        played_bye = rebuilt.loc[rebuilt.week.eq(1) & rebuilt.franchise_id.eq("fid-9")]
        assert played_bye.opponent.tolist() == ["BYE"]
        assert "is_bye_week" not in rebuilt.columns
        assert db.read_table("matchup").loc[lambda f: f.franchise_id.eq("fid-9"), "is_bye_week"].tolist() == [True]
        assert played_bye.opponent_franchise_id.isna().all()
        assert rebuilt.groupby("week").size().to_dict() == {1: 7, 2: 7}
        # Fleet publication projects to the existing canonical schedule
        # schema, which persists the BYE label but has no is_bye_week column.
        published = rebuilt.drop(columns=["is_bye_week"], errors="ignore")
        bye_simulation_db.register("rebuilt_schedule", published)
        bye_simulation_db.execute("CREATE OR REPLACE TABLE public.schedule AS SELECT * FROM rebuilt_schedule")
        assert assert_active_season_simulation_health(bye_simulation_db, db_name="odd_espn", year=2026)["latest_simulation_franchises"] == 7
        from multi_league.core.league_update_plan import _derived_refresh_gaps
        bye_simulation_db.execute("ALTER TABLE public.matchup ADD COLUMN manager VARCHAR DEFAULT 'Manager'")
        bye_simulation_db.execute("ALTER TABLE public.league_settings ADD COLUMN start_week INTEGER DEFAULT 1")
        for table in ("player_fantasy", "player_fantasy_season", "player_fantasy_season_all"):
            bye_simulation_db.execute(f"CREATE TABLE public.{table} (db_name VARCHAR,year INTEGER,week INTEGER,NFL_player_id VARCHAR)")
        bye_simulation_db.execute("CREATE TABLE public.standings_by_year AS SELECT db_name,year,franchise_id FROM public.matchup")
        class Reader:
            def query(self, sql, *, database):
                return bye_simulation_db.execute(sql).fetchdf().to_dict("records")
        assert _derived_refresh_gaps(Reader(), safe_db="odd_espn", active_season=2026) == (set(), False)
        bye_simulation_db.execute("DELETE FROM public.schedule WHERE week=2 AND franchise_id='fid-1'")
        assert _derived_refresh_gaps(Reader(), safe_db="odd_espn", active_season=2026)[1]
