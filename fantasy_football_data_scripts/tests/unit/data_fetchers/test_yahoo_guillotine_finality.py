"""Native first/final live scores remain schedule inputs, never observed cuts."""

from types import SimpleNamespace

import pandas as pd
import pytest

from multi_league.core.local_db import LocalLeagueDB
from multi_league.data_fetchers.yahoo import yahoo_matchups
from multi_league.data_fetchers.yahoo.yahoo_schedules import _derive_schedule_df_from_matchup_df
from multi_league.transformations.matchup import playoff_odds_import as poi


def fetch_native(monkeypatch, tmp_path, *, stage, witness="xml", native=True):
    first_live = stage in {"first_live", "first_partial"}
    first_scores = {"a": 70, "b": 120, "c": 100}
    final_scores = {"a": 0, "b": 0, "c": 0} if first_live else {"b": 110, "c": 110 if stage == "final_tie" else 90}
    matchups = []
    for week, scores in [(10, first_scores), (11, final_scores)]:
        teams = list(scores)
        for other in teams[1:]:
            a, b = teams[0], other
            complete = ((week == 10 and (not first_live or stage == "first_partial" and other == "b"))
                        or (week == 11 and stage in {"final_done", "final_tie"}))
            winner = max((a, b), key=lambda team: scores[team]) if complete else None
            if stage == "final_tie" and week == 11:
                winner = None
            cards = "".join(
                f"<team><team_key>470.l.1.t.{team}</team_key><name>Team {team}</name>"
                f"<managers><manager><guid>owner-{team}</guid><nickname>Owner {team}</nickname></manager></managers>"
                f"<team_points><total>{scores[team]}</total></team_points></team>" for team in (a, b)
            )
            status = "postevent" if complete else "inevent" if week == (10 if first_live else 11) else "preevent"
            outcome = f"<winner_team_key>470.l.1.t.{winner}</winner_team_key>" if winner else ""
            if stage == "final_tie" and week == 11:
                outcome = "<is_tied>1</is_tied>"
            matchups.append(f"<matchup><week>{week}</week><status>{status}</status>{outcome}<teams>{cards}</teams></matchup>")
    flag = f"<is_guillotine>{int(native)}</is_guillotine>" if witness in {"xml", "conflict", "cached"} else ""
    xml = f"<fantasy_content><league>{flag}<scoreboard><matchups>" + "".join(matchups) + "</matchups></scoreboard></league></fantasy_content>"
    urls = []
    def get(url):
        urls.append(url)
        return SimpleNamespace(text=xml, status_code=200, raise_for_status=lambda: None)
    oauth = SimpleNamespace(session=SimpleNamespace(get=get))
    current = 10 if first_live else 11
    settings = {"start_week": 10, "end_week": 11, "playoff_start_week": 12,
                "num_teams": 3, "num_playoff_teams": 0, "playoff_teams": 0, "bye_teams": 0,
                "is_guillotine": native, "year": 2026, "current_week": current,
                "draft_status": "postdraft"}
    league_settings = {key: value for key, value in settings.items() if key != "is_guillotine"}
    if witness == "conflict":
        league_settings["is_guillotine"] = not native
    league = SimpleNamespace(current_week=current, settings=lambda: league_settings)
    monkeypatch.setattr(yahoo_matchups, "YFA_AVAILABLE", True)
    monkeypatch.setattr(yahoo_matchups, "RUN_LOGGER_AVAILABLE", False)
    monkeypatch.setattr(yahoo_matchups, "get_current_nfl_season_year", lambda: 2026)
    monkeypatch.setattr(yahoo_matchups, "yfa", SimpleNamespace(Game=lambda *_: SimpleNamespace(to_league=lambda _: league)))
    ctx = SimpleNamespace(league_id="470.l.1", matchup_data_directory=tmp_path,
                          data_directory=tmp_path,
                          manager_name_overrides={}, get_oauth_session=lambda: oauth,
                          get_league_id_for_year=lambda _: "470.l.1", import_mode="quick",
                          quick_import_years=[2026], start_year=2026, end_year=2026)
    cached = dict(settings, is_guillotine=not native) if witness == "cached" else settings
    kwargs = {"league_settings": {"metadata": cached}} if witness in {"settings", "conflict", "cached"} else {}
    facts, failures = yahoo_matchups.weekly_matchup_data(ctx=ctx, year=2026, **kwargs)
    assert failures == [] and len(urls) == 1
    return facts, settings, ctx


@pytest.mark.parametrize("witness", ["xml", "settings"])
@pytest.mark.parametrize("stage,expected", [("first_live", []), ("final_live", [10]), ("final_done", [10, 11])])
def test_native_facts_require_provider_final_outcomes_at_first_and_final_week(monkeypatch, tmp_path, witness, stage, expected):
    facts, _, _ = fetch_native(monkeypatch, tmp_path, stage=stage, witness=witness)
    assert sorted(facts.week.unique()) == expected
    schedule = facts.attrs["schedule_df"]
    assert sorted(schedule.week.unique()) == [10, 11]
    assert schedule.loc[schedule.week.eq(10 if stage == "first_live" else 11), "team_points"].gt(0).any()
    assert "provider_is_guillotine" not in facts.columns
    assert "provider_is_guillotine" not in schedule.columns


def test_ordinary_yahoo_live_final_fetch_retains_existing_admission(monkeypatch, tmp_path):
    facts, _, _ = fetch_native(monkeypatch, tmp_path, stage="final_live", native=False)
    assert sorted(facts.week.unique()) == [10, 11]


def test_native_partial_week_waits_for_the_complete_survivor_graph(monkeypatch, tmp_path):
    facts, _, _ = fetch_native(monkeypatch, tmp_path, stage="first_partial")
    assert facts.empty
    assert len(facts.attrs["schedule_df"].loc[lambda frame: frame.week.eq(10)]) == 4


def test_native_completed_tie_without_winner_key_is_final_evidence(monkeypatch, tmp_path):
    facts, _, _ = fetch_native(monkeypatch, tmp_path, stage="final_tie")
    assert sorted(facts.week.unique()) == [10, 11]
    assert facts.loc[facts.week.eq(11), "winner_team_key"].isna().all()
    assert len(facts.attrs["final_tie_team_weeks"]) == 2


def test_native_mode_requires_consistent_explicit_witnesses(monkeypatch, tmp_path):
    with pytest.raises(ValueError, match="mode metadata is inconsistent"):
        fetch_native(monkeypatch, tmp_path, stage="final_live", witness="conflict")


@pytest.mark.parametrize("live_score", [50, 52])
def test_native_duplicate_final_tie_node_cannot_finalize_a_live_copy(monkeypatch, tmp_path, live_score):
    _, _, ctx = fetch_native(monkeypatch, tmp_path, stage="final_live")
    nodes = []
    for points, status, tied in [(50, "postevent", "<is_tied>1</is_tied>"), (live_score, "inevent", "")]:
        cards = "".join(
            f"<team><team_key>470.l.1.t.{team}</team_key><name>{team}</name>"
            f"<managers><manager><guid>owner-{team}</guid><nickname>{team}</nickname></manager></managers>"
            f"<team_points><total>{score}</total></team_points></team>"
            for team, score in [("a", points), ("b", 50)]
        )
        nodes.append(f"<matchup><week>10</week><status>{status}</status>{tied}<teams>{cards}</teams></matchup>")
    xml = '<fantasy_content><league><is_guillotine>1</is_guillotine><scoreboard><matchups count="2">' + "".join(nodes) + '</matchups></scoreboard></league></fantasy_content>'
    ctx.get_oauth_session().session.get = lambda _: SimpleNamespace(text=xml, status_code=200, raise_for_status=lambda: None)
    facts, failures = yahoo_matchups.weekly_matchup_data(ctx=ctx, year=2026, week=10)
    assert failures == [] and facts.empty
    raw = facts.attrs["schedule_df"]
    assert len(raw) == 4
    from multi_league.core.league_refresh import filter_matchups_to_final_results
    from multi_league.core.league_update_validation import validate_yahoo_week_matchup_scope
    assert validate_yahoo_week_matchup_scope(
        raw_schedule=raw, final_matchups=filter_matchups_to_final_results(facts), season=2026, week=10,
        expected_team_keys=("470.l.1.t.a", "470.l.1.t.b"), playoff_start_week=11,
        is_guillotine=True, league_start_week=10,
    ) is False


@pytest.mark.parametrize("native,expected", [(True, [10]), (False, [10, 11])])
def test_fresh_provider_mode_overrides_cached_canonical_migration_default(monkeypatch, tmp_path, native, expected):
    facts, settings, _ = fetch_native(monkeypatch, tmp_path, stage="final_live", witness="cached", native=native)
    assert sorted(facts.week.unique()) == expected
    with LocalLeagueDB(tmp_path, "mode_propagation") as db:
        db.save_table("league_settings", pd.DataFrame([dict(settings, is_guillotine=not native)]))
        yahoo_matchups.apply_yahoo_matchup_settings(db, facts, 2026)
        loaded = poi.load_league_settings(2026, settings_by_year={
            2026: yahoo_matchups.local_yahoo_settings_for_year(db, 2026),
        })
        assert loaded["is_guillotine"] is native


def test_targeted_native_fetch_refreshes_cached_mode_before_simulation(monkeypatch, tmp_path):
    import initial_import_v3

    facts, settings, ctx = fetch_native(monkeypatch, tmp_path, stage="final_live", witness="cached")
    monkeypatch.setattr(initial_import_v3, "create_oauth_session", lambda _: (ctx.get_oauth_session(), None))
    with LocalLeagueDB(tmp_path, "targeted_native") as db:
        db.save_table("league_settings", pd.DataFrame([dict(settings, is_guillotine=False)]))
        db.save_table("schedule", _derive_schedule_df_from_matchup_df(facts.attrs["schedule_df"], 2026, {}),
                      platform="yahoo", league_id="470.l.1")
        result = initial_import_v3.run_targeted_fetch(
            ctx=ctx, db=db, fetch_name="matchups", fetch_year=2026, upload_to_fly=False,
        )
        assert result["ok"] is True
        assert yahoo_matchups.local_yahoo_settings_for_year(db, 2026)["is_guillotine"] is True
        for name in ("PLAYOFF_SLOTS", "BYE_SLOTS", "BRACKET_RESEED", "TIEBREAKER_ORDER", "NUM_TEAMS",
                     "REGULAR_SEASON_WEEKS", "TARGET_COLS", "ACTIVE_TABLE_CATALOG"):
            monkeypatch.setattr(poi, name, getattr(poi, name))
        current_settings = {2026: yahoo_matchups.local_yahoo_settings_for_year(db, 2026)}
        poi.load_playoff_config_from_settings(SimpleNamespace(data_directory=tmp_path), settings_by_year=current_settings)
        poi.process_parquet_files(conn=db.connect(), db_name="targeted_native", data_directory=str(tmp_path),
                                  settings_by_year=current_settings)
        actual = db.connect().execute("SELECT DISTINCT franchise_id,exp_final_wins,p_champ FROM public.matchup WHERE week=10").fetchdf()
        assert len(actual) == 3 and actual.exp_final_wins.eq(0).all()
        assert actual.p_champ.sum() == pytest.approx(100, abs=0.03)


@pytest.mark.parametrize("caller", ["initial", "recovery"])
def test_initial_and_recovery_callers_persist_fresh_mode_in_normal_local_settings(monkeypatch, tmp_path, caller):
    import initial_import_v3
    from multi_league.data_fetchers.yahoo import yahoo_recovery

    stage = "first_live" if caller == "initial" else "final_live"
    _, settings, ctx = fetch_native(monkeypatch, tmp_path, stage=stage, witness="cached")
    ctx.league_ids = {"2026": "470.l.1"}
    monkeypatch.setattr(initial_import_v3, "create_oauth_session", lambda _: (ctx.get_oauth_session(), None))
    monkeypatch.setattr(initial_import_v3, "ensure_session_healthy", lambda oauth, _: oauth)
    with LocalLeagueDB(tmp_path, "caller_native") as db:
        db.save_table("league_settings", pd.DataFrame([dict(settings, is_guillotine=False)]))
        if caller == "initial":
            failures = initial_import_v3.run_v3_fetcher(
                ctx=ctx, db=db, fetcher_name="matchups", years=[2026], table_name="matchup",
                sleep_between_years=0, fetch_year_fn=lambda year, oauth: yahoo_matchups.weekly_matchup_data(
                    ctx=ctx, year=year, league_settings=yahoo_matchups.local_yahoo_settings_for_year(db, year),
                ),
            )
            assert failures == {}
            assert not db.table_exists("matchup") or db.row_count("matchup") == 0
            assert db.row_count("schedule") > 0
        else:
            gap = yahoo_recovery.Gap("matchup", 2026, "full_year", "manifest")
            assert yahoo_recovery._resolve_matchup_gap(gap, ctx, tmp_path, local_db=db) is True
            assert set(db.read_table("matchup").week) == {10}
        assert yahoo_matchups.local_yahoo_settings_for_year(db, 2026)["is_guillotine"] is True


@pytest.mark.parametrize("stage", ["first_live", "final_live"])
def test_native_live_boundary_source_runs_solver_without_deciding_a_live_cut(monkeypatch, tmp_path, stage):
    facts, settings, ctx = fetch_native(monkeypatch, tmp_path, stage=stage)
    schedule = facts.attrs["schedule_df"]
    with LocalLeagueDB(tmp_path, "native_boundary") as db:
        db.ensure_table("matchup")
        if not facts.empty:
            db.save_table("matchup", facts, platform="yahoo", league_id="470.l.1")
        db.save_table("schedule", _derive_schedule_df_from_matchup_df(schedule, 2026, {}),
                      platform="yahoo", league_id="470.l.1")
        db.save_table("league_settings", pd.DataFrame([settings]))
        for name in ("PLAYOFF_SLOTS", "BYE_SLOTS", "BRACKET_RESEED", "TIEBREAKER_ORDER", "NUM_TEAMS",
                     "REGULAR_SEASON_WEEKS", "TARGET_COLS", "ACTIVE_TABLE_CATALOG"):
            monkeypatch.setattr(poi, name, getattr(poi, name))
        poi.load_playoff_config_from_settings(SimpleNamespace(data_directory=tmp_path), settings_by_year={2026: settings})
        poi.process_parquet_files(conn=db.connect(), db_name="native_boundary", data_directory=str(tmp_path),
                                  settings_by_year={2026: settings}, target_year=2026)
        if stage == "first_live":
            import initial_import_v3
            assert db.row_count("matchup") == 0
            assert initial_import_v3._allow_empty_quick_startup(
                ctx, db, {2026}, league_settings={2026: {"metadata": settings}},
            ) is True
        else:
            actual = db.connect().execute("SELECT DISTINCT franchise_id,p_champ FROM public.matchup WHERE week=10").fetchdf()
            assert len(actual) == 3
            assert actual.p_champ.sum() == pytest.approx(100, abs=0.03)
            assert actual.loc[actual.franchise_id.isin(["owner-b", "owner-c"]), "p_champ"].between(0, 100, inclusive="neither").all()
