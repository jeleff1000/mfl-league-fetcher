"""Imports must publish their own mode and keep unplayed provider fixtures."""

import json
from types import SimpleNamespace

import pytest

from multi_league.core import import_utils
from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.targets import fly_target
from multi_league.data_fetchers.yahoo import yahoo_matchups
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("mode", ["quick", "full"])
def test_context_mode_wins_over_enclosing_workflow(monkeypatch, tmp_path, platform, mode):
    calls = []
    class Target:
        def merge_fleet_partition(self, *_args, **_kwargs):
            calls.append("quick")
            return {"status": "COMMITTED"}
        def merge_league_delta(self, *_args, **_kwargs):
            calls.append("full")
            return {"status": "COMMITTED"}
    monkeypatch.setattr(fly_target, "FlyTarget", Target)
    monkeypatch.setenv("GITHUB_WORKFLOW", f"{platform} {'Full' if mode == 'quick' else 'Quick'} Import Worker")
    monkeypatch.setenv("FLY_PUBLISH_FORMAT", "delta")
    monkeypatch.setenv("FLY_FINALIZE_INVENTORY", "0")
    monkeypatch.setenv("LEAGUE_IMPORT_BASE_GENERATION", "2")
    ctx = SimpleNamespace(import_mode=mode, platform=platform, league_id="123", league_name="League",
        league_ids={"2026": "123"}, data_directory=tmp_path, manager_name_overrides={"Old": "Preferred"})
    with LocalLeagueDB(tmp_path, "league") as db:
        db.ensure_table("matchup")
        db.connect().execute("INSERT INTO public.matchup (db_name,year,week,manager_week) "
                             "VALUES ('league',2026,1,'owner_2026_1')")
        result = import_utils.upload_league_tables(db, "league", ctx, platform=platform)
    assert result == [("Fly Upload", True)]
    assert calls == [mode]
    saved = json.loads((tmp_path / "delta_publish_manifest.json").read_text())
    assert saved["publication"]["status"] == "COMMITTED"
    if mode == "quick":
        assert saved["db_name"] == "___fleet"
        assert saved["db_names"] == ["league"]
    else:
        assert saved["import_mode"] == "full"


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("provider_schedule", [False, True])
def test_schedule_rebuild_preserves_future_aliases_and_history(tmp_path, platform, provider_schedule):
    with LocalLeagueDB(tmp_path, "league") as db:
        db.ensure_table("matchup")
        conn = db.connect()
        conn.execute("INSERT INTO public.matchup "
            "(db_name,year,week,cumulative_week,manager,franchise_id,manager_week,opponent,team_points,platform) "
            "VALUES ('league',2025,17,202517,'Historical alias','owner','owner_2025_17','B',100,?),"
            "('league',2026,1,202601,'Preferred alias','owner','owner_2026_1','B',120,?)", [platform, platform])
        if provider_schedule:
            db.ensure_table("schedule")
            conn.execute("INSERT INTO public.schedule "
                "(db_name,year,week,cumulative_week,manager,franchise_id,manager_week,opponent,team_points,platform) "
                "VALUES ('league',2026,1,202601,'Stale alias','owner','owner_2026_1','B',0,?),"
                "('league',2026,2,202602,'Preferred alias','owner','owner_2026_2','C',0,?)", [platform, platform])
        class Runner:
            db_name = "league"
            def _qualified_name(self, table): return "public." + table
            def _db_filter(self, *_args): return "db_name='league'"
            def _table_exists(self, table):
                return conn.execute("SELECT 1 FROM information_schema.tables WHERE table_schema='public' "
                                    "AND table_name=?", [table]).fetchone() is not None
            def _execute(self, sql, *_args): return conn.execute(sql).fetchone()[0]
        for _ in range(2):
            MatchupEnrichmentsMixin.build_schedule_from_matchup(Runner())
        expected = [(2025, 17, "Historical alias", "owner", 100.0), (2026, 1, "Preferred alias", "owner", 120.0)]
        if provider_schedule:
            expected.append((2026, 2, "Preferred alias", "owner", 0.0))
        assert conn.execute("SELECT year,week,manager,franchise_id,team_points FROM public.schedule "
                            "ORDER BY year,week").fetchall() == expected
        assert conn.execute("SELECT COUNT(*) FROM public.matchup").fetchone() == (2,)


@pytest.mark.parametrize("explicit_week", [None, 1])
def test_yahoo_bulk_fetch_keeps_schedule_future_without_publishing_live_scores(monkeypatch, tmp_path, explicit_week):
    urls = []
    def get(url):
        urls.append(url)
        weeks = url.split(";week=")[1].split(",")
        pairs = []
        for week in weeks:
            points = (100, 90) if week == "1" else (10, 9) if week == "2" else (0, 0)
            teams = "".join(f"<team><team_key>470.l.123.t.{i}</team_key><name>Team {i}</name>"
                f"<managers><manager><guid>owner-{i}</guid><nickname>Owner {i}</nickname></manager></managers>"
                f"<team_points><total>{score}</total></team_points></team>" for i, score in enumerate(points, 1))
            pairs.append(f"<matchup><week>{week}</week><teams>{teams}</teams></matchup>")
        return SimpleNamespace(status_code=200, text="<fantasy_content><matchups>"+"".join(pairs)+
                               "</matchups></fantasy_content>", raise_for_status=lambda: None)
    oauth = SimpleNamespace(session=SimpleNamespace(get=get))
    league = SimpleNamespace(settings=lambda: {"start_week": 1, "end_week": 5, "playoff_start_week": 4}, current_week=lambda: 2)
    game = SimpleNamespace(to_league=lambda key: league)
    monkeypatch.setattr(yahoo_matchups, "yfa", SimpleNamespace(Game=lambda *_args, **_kwargs: game))
    monkeypatch.setattr(yahoo_matchups, "YFA_AVAILABLE", True)
    monkeypatch.setattr(yahoo_matchups, "RUN_LOGGER_AVAILABLE", False)
    monkeypatch.setattr(yahoo_matchups, "get_current_nfl_season_year", lambda: 2026)
    ctx = SimpleNamespace(league_id="470.l.123", matchup_data_directory=tmp_path,
        manager_name_overrides={"Owner 1": "Preferred"}, get_oauth_session=lambda: oauth,
        get_league_id_for_year=lambda year: "470.l.123")
    facts, failed = yahoo_matchups.weekly_matchup_data(ctx=ctx, year=2026, week=explicit_week)
    assert failed == []
    assert set(facts["week"]) == {1}
    assert set(facts.attrs["schedule_df"]["week"]) == ({1, 2, 3} if explicit_week is None else {1})
    assert len(urls) == 1
