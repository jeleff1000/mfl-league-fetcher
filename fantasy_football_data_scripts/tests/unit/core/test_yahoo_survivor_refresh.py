"""Refresh admission must accept Yahoo's witnessed survivor graph intact."""

from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd
import pytest

from multi_league.core.league_update_validation import (
    IncompleteSourceError,
    validate_yahoo_week_matchup_scope,
)
from multi_league.core.league_refresh import filter_matchups_to_final_results


def survivor_week(week=7):
    # Exact graph and scores from the existing, anonymized native Yahoo fixture.
    frame = pd.read_csv(Path(__file__).with_name("fixtures") / "yahoo_guillotine_scoreboard.csv")
    frame = frame.loc[frame.week.eq(week)].copy()
    frame["year"] = 2026
    frame["team_key"] = "470.l.1.t." + frame.team_slot.astype(str)
    frame["opponent_team_key"] = "470.l.1.t." + frame.opponent_slot.astype(str)
    return frame


def validate(raw, final=None, playoff_start=14, is_guillotine=True):
    return validate_yahoo_week_matchup_scope(
        raw_schedule=raw, final_matchups=raw.copy() if final is None else final,
        season=2026, week=int(raw.week.iloc[0]),
        expected_team_keys=tuple(f"470.l.1.t.{slot}" for slot in range(1, 15)),
        playoff_start_week=playoff_start,
        is_guillotine=is_guillotine,
    )


@pytest.mark.parametrize("week", [1, 7])
@pytest.mark.parametrize("playoff_start", [None, 1, 14])
def test_survivor_native_graph_accepts_repeated_perspectives_and_eliminated_teams(week, playoff_start):
    assert validate(survivor_week(week), playoff_start=playoff_start) is True


def test_survivor_partial_final_graph_waits_for_remaining_declared_pairs():
    raw = survivor_week()
    final = raw.loc[(raw.team_slot.eq(1) & raw.opponent_slot.eq(7))
                    | (raw.team_slot.eq(7) & raw.opponent_slot.eq(1))].copy()
    assert validate(raw, final) is False


def test_survivor_live_scores_do_not_become_final_matchups():
    raw = survivor_week(8)
    raw["winner_team_key"] = None
    final = filter_matchups_to_final_results(raw)
    assert final.empty
    assert validate(raw, final) is False


@pytest.mark.parametrize("fault", ["changed_score", "nonreciprocal", "extra_duplicate", "foreign_team"])
def test_survivor_graph_rejects_changed_or_incomplete_final_pair_evidence(fault):
    raw = survivor_week()
    final = raw.copy()
    if fault == "changed_score":
        final.loc[final.index[0], "team_points"] += 1
    elif fault == "nonreciprocal":
        final = final.iloc[1:].copy()
    elif fault == "extra_duplicate":
        final = pd.concat([final, final.iloc[:2]], ignore_index=True)
    else:
        final.loc[final.index[0], "opponent_team_key"] = "470.l.1.t.99"
    with pytest.raises(IncompleteSourceError):
        validate(raw, final)


def test_survivor_raw_graph_rejects_conflicting_repeated_scores():
    raw = survivor_week()
    # Change a repeated hub score and its reciprocal witness together. Pair
    # reciprocity alone would pass; the one native team score must still agree.
    first = raw.iloc[0]
    raw.loc[raw.index[0], "opponent_points"] += 1
    reciprocal = raw.team_slot.eq(first.opponent_slot) & raw.opponent_slot.eq(first.team_slot)
    raw.loc[reciprocal, "team_points"] += 1
    with pytest.raises(IncompleteSourceError):
        validate(raw)


def test_ordinary_duplicate_pair_cannot_claim_survivor_semantics():
    pair = survivor_week().iloc[:2].copy()
    with pytest.raises(IncompleteSourceError):
        validate(pd.concat([pair, pair], ignore_index=True), is_guillotine=False)


def test_survivor_star_without_native_mode_still_requires_normal_team_coverage():
    with pytest.raises(IncompleteSourceError):
        validate(survivor_week(), is_guillotine=False)


def test_native_two_survivor_final_can_retain_repeated_reciprocal_pair():
    raw = survivor_week().iloc[:2].copy()
    raw["week"] = 13
    assert validate(pd.concat([raw, raw], ignore_index=True)) is True


def test_native_guillotine_setting_survives_parser_flattening_and_canonical_ddl():
    import duckdb
    from multi_league.core.yahoo_league_settings import _parse_league_metadata
    from multi_league.core.canonical_settings import flatten_settings
    from multi_league.core.local_db import _TABLE_DDL

    root = ET.fromstring('<fantasy_content><league><season>2026</season><num_teams>14</num_teams>'
                         '<is_guillotine>1</is_guillotine><settings><uses_playoff>0</uses_playoff>'
                         '<end_week>13</end_week></settings></league></fantasy_content>')
    metadata = _parse_league_metadata(root)
    assert metadata["is_guillotine"] is True
    flat = flatten_settings({"metadata": metadata}, "yahoo", 2026, "470.l.1")
    assert flat["is_guillotine"] is True
    assert flatten_settings({"metadata": {}}, "yahoo", 2026, "470.l.1")["is_guillotine"] is False
    with duckdb.connect() as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute(_TABLE_DDL["league_settings"]("memory"))
        columns = dict((row[1],row[2]) for row in conn.execute("PRAGMA table_info('public.league_settings')").fetchall())
        assert columns["is_guillotine"] == "BOOLEAN"


def test_validated_survivor_graph_reuses_canonical_candidate_policy_before_ownership_merge(tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.core.league_refresh import canonicalize_provider_matchup_rows, merge_provider_refresh_table
    raw = survivor_week()
    for col, prefix in (("manager_guid", "owner-"), ("manager", "Owner "), ("team_name", "Team ")):
        raw[col] = prefix + raw.team_slot.astype(str)
    raw["opponent_guid"] = "owner-" + raw.opponent_slot.astype(str)
    raw["opponent"] = "Owner " + raw.opponent_slot.astype(str)
    assert validate(raw) is True
    with LocalLeagueDB(tmp_path, "league") as db:
        selected = canonicalize_provider_matchup_rows(db, raw, platform="yahoo", league_id="470.l.1")
        merge_provider_refresh_table(db, "matchup", selected, platform="yahoo", league_id="470.l.1")
        actual = db.read_table("matchup")
        assert len(actual) == 8
        assert actual.team_key.nunique() == 8
        assert actual.manager_week.is_unique
        assert sorted(actual.team_points) == [128.65, 133.85, 139.1, 143.15, 150.45, 153.25, 183.0, 194.8]


def test_guillotine_season_rollup_zeroes_eliminated_odds_without_losing_history(tmp_path):
    from multi_league.core.local_db import LocalLeagueDB
    from multi_league.transformations.aggregation.aggregate_matchup_context import aggregate_matchup_season
    rows = []
    for week, teams in [(1, (1,2,3)), (2, (2,3))]:
        for team in teams:
            rows.append({"year":2026,"week":week,"manager":f"M{team}","manager_guid":f"owner-{team}",
                         "franchise_id":f"owner-{team}","team_key":str(team),"team_name":f"T{team}",
                         "opponent":"Opponent","team_points":90+team,"opponent_points":90,
                         "p_playoffs":0.0,"p_champ":10.0 if team==1 else 50.0})
    with LocalLeagueDB(tmp_path, "league") as db:
        db.save_table("matchup",pd.DataFrame(rows),platform="yahoo",league_id="470.l.1")
        db.save_table("league_settings",pd.DataFrame([{"year":2026,"platform":"yahoo", "league_key":"470.l.1",
            "is_guillotine":True,"playoff_start_week":3}]))
        # Simulations write derived probabilities after raw provider ingestion.
        db.connect().execute("UPDATE public.matchup SET p_playoffs=0,p_champ=CASE WHEN team_key='1' THEN 10 ELSE 50 END")
        aggregate_matchup_season(db.connect(),"league",year=2026)
        actual=db.connect().execute("SELECT franchise_id,games,p_champ FROM public.matchup_season ORDER BY franchise_id").fetchall()
        assert actual == [("owner-1",1,0.0),("owner-2",2,50.0),("owner-3",2,50.0)]


@pytest.mark.parametrize("fault,latest,last_schedule_week", [
    (None, 7, 13), (None, 8, 10), (None, 8, 8),
    ("unmarked", 8, 10), ("missing_survivor", 8, 10), ("missing_future", 8, 10),
    ("foreign_future", 8, 10), ("changed_future", 8, 10), ("extra_season", 8, 10),
])
def test_survivor_schedule_health_and_planner_use_native_elimination_inventory(fault, latest, last_schedule_week):
    import duckdb
    from multi_league.core.league_update_validation import assert_active_season_simulation_health
    from multi_league.core.league_update_plan import _derived_refresh_gaps

    source = pd.read_csv(Path(__file__).with_name("fixtures") / "yahoo_guillotine_scoreboard.csv")
    schedule = source.drop_duplicates(["week", "team_slot"]).copy()
    if latest == 8:
        schedule = schedule.loc[~(schedule.week.ge(9) & schedule.team_slot.eq(2))]
        scores = {1: 170, 2: 80, 3: 100, 4: 130, 11: 160, 13: 180, 14: 140}
        schedule.loc[schedule.week.eq(8), "team_points"] = schedule.loc[schedule.week.eq(8), "team_slot"].map(scores)
    schedule = schedule.loc[schedule.week.le(last_schedule_week)].copy()
    schedule["db_name"] = "league"
    schedule["year"] = 2026
    schedule["franchise_id"] = "owner-" + schedule.team_slot.astype(str)
    schedule["opponent_franchise_id"] = "owner-" + schedule.opponent_slot.astype(str)
    schedule["manager"] = schedule["franchise_id"]
    schedule["platform"] = "yahoo"
    schedule["opponent"] = schedule["opponent_franchise_id"]
    schedule["is_playoffs"] = False
    schedule["p_playoffs"] = 0.0
    schedule["p_champ"] = 100 / (15 - latest)
    with duckdb.connect() as conn:
        conn.execute("CREATE SCHEMA public")
        conn.execute("CREATE TABLE public.schedule AS SELECT * FROM schedule")
        conn.execute("CREATE TABLE public.matchup AS SELECT * FROM schedule WHERE week<=?", [latest])
        conn.execute("CREATE TABLE public.matchup_season AS SELECT DISTINCT db_name,year,franchise_id,p_playoffs,p_champ FROM public.matchup")
        conn.execute("UPDATE public.matchup_season SET p_champ=0 WHERE franchise_id NOT IN "
                     "(SELECT franchise_id FROM public.matchup WHERE week=?)", [latest])
        conn.execute("CREATE TABLE public.standings_by_year AS SELECT DISTINCT db_name,year,franchise_id FROM public.matchup")
        conn.execute("CREATE TABLE public.league_settings AS SELECT 'league' AS db_name,2026 AS year,14 AS num_teams,"
                     "1 AS start_week,14 AS playoff_start_week,'yahoo' AS platform,? AS is_guillotine", [fault != "unmarked"])
        for table in ('player_fantasy', 'player_fantasy_season', 'player_fantasy_season_all'):
            conn.execute(f"CREATE TABLE public.{table} (db_name VARCHAR,year INTEGER,week INTEGER,NFL_player_id VARCHAR)")
        if fault == "missing_survivor":
            conn.execute("DELETE FROM public.schedule WHERE week=? AND team_slot=1", [latest])
        elif fault == "missing_future":
            conn.execute("DELETE FROM public.schedule WHERE week=9 AND team_slot=1")
        elif fault == "foreign_future":
            conn.execute("UPDATE public.schedule SET franchise_id='phantom' WHERE week=9 AND team_slot=1")
        elif fault == "changed_future":
            conn.execute("UPDATE public.schedule SET franchise_id='owner-2' WHERE week=10 AND team_slot=1")
        elif fault == "extra_season":
            conn.execute("INSERT INTO public.matchup_season VALUES ('league',2026,'phantom',50,50)")
        class Reader:
            def query(self,sql,**kwargs):
                result=conn.execute(sql)
                return [dict(zip([col[0] for col in result.description],row)) for row in result.fetchall()]
        if fault:
            with pytest.raises(IncompleteSourceError):
                assert_active_season_simulation_health(conn,db_name="league",year=2026)
        else:
            assert assert_active_season_simulation_health(conn,db_name="league",year=2026) == {
                "latest_finalized_week":latest,"latest_simulation_franchises":15-latest,"simulation_season_franchises":14}
        if fault != "extra_season":
            assert _derived_refresh_gaps(Reader(),safe_db="league",active_season=2026)[1] is (fault is not None)


def test_arbitrary_repeated_multi_opponent_graph_cannot_claim_survivor_semantics():
    raw = survivor_week()
    # A second independent hub/cycle is neither ordinary H2H nor the native
    # survivor graph; reciprocal duplicated payloads must still fail closed.
    extra = raw.iloc[:2].copy()
    extra["team_key"] = ["470.l.1.t.1", "470.l.1.t.2"]
    extra["opponent_team_key"] = ["470.l.1.t.2", "470.l.1.t.1"]
    with pytest.raises(IncompleteSourceError):
        validate(pd.concat([raw, extra], ignore_index=True))

