"""Native future fixtures survive the common import/refresh simulation path."""
import numpy as np
import pandas as pd
import pytest

from multi_league.core.local_db import LocalLeagueDB
from multi_league.core.league_refresh import resolve_active_schedule_franchise_ids
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin
from multi_league.transformations.matchup import playoff_odds_import as poi


@pytest.mark.parametrize("platform", ["yahoo", "espn", "sleeper"])
@pytest.mark.parametrize("flow", ["quick", "full", "refresh"])
def test_remaining_eight_weeks_are_simulated_without_rewriting_results(tmp_path, monkeypatch, platform, flow):
    monkeypatch.setattr(poi, "N_SIMS", 256)
    facts = pd.DataFrame([
        {"db_name": "league", "year": 2026, "week": week, "cumulative_week": 202600+week,
         "manager": f"Alias {i}", "team_name": f"Team {i}", "team_key": str(i),
         "franchise_id": f"saved-{i}", "manager_guid": f"owner-{i}",
         "manager_week": f"saved-{i}_2026_{week}", "opponent": f"Alias {opp}",
         "opponent_franchise_id": f"saved-{opp}", "team_points": 110 if (i+week)%2 else 90,
         "opponent_points": 90 if (i+week)%2 else 110,
         "win": int((i+week)%2 == 1), "loss": int((i+week)%2 == 0),
         "is_playoffs": 0, "is_consolation": 0, "platform": platform}
        for week in range(1, 7) for i in range(1, 13)
        for opp in [i+1 if i%2 else i-1]
    ])
    fixtures = pd.DataFrame([
        {"db_name": "league", "year": 2026, "week": week, "team_key": str(i),
         "opponent_team_key": str(opp), "manager": f"Alias {i}", "opponent": f"Alias {opp}",
         "franchise_id": f"saved-{i}", "opponent_franchise_id": f"saved-{opp}",
         "manager_week": f"saved-{i}_2026_{week}", "is_playoffs": 0, "platform": platform}
        for week in range(7, 15) for i in range(1, 13)
        for opp in [i+1 if i%2 else i-1]
    ])
    with LocalLeagueDB(tmp_path, "league") as db:
        db.save_table("matchup", facts, year=2026, platform=platform, league_id="native")
        if flow == "refresh":
            fixtures = resolve_active_schedule_franchise_ids(
                db, fixtures, active_year=2026, future_weeks=tuple(range(7, 15)),
                expected_team_keys=tuple(str(i) for i in range(1, 13)),
            )
        db.save_table("schedule", fixtures, year=2026, platform=platform, league_id="native")
        conn = db.connect()
        before = db.read_table("matchup").copy()
        class Runner:
            db_name = "league"
            def _qualified_name(self, table): return "public."+table
            def _db_filter(self, *_args): return "db_name='league'"
            def _table_exists(self, table): return db.table_exists(table)
            def _execute(self, sql, *_args): return conn.execute(sql).fetchone()[0]
        for _ in range(2):
            MatchupEnrichmentsMixin.build_schedule_from_matchup(Runner())
        schedule = db.read_table("schedule")
        assert schedule.groupby("week").franchise_id.nunique().to_dict() == {w: 12 for w in range(1, 15)}
        future = poi.future_regular_from_schedule(schedule, 2026, 6)
        a, b, weeks = poi._future_schedule_indices(
            future, manager_to_index={f"saved-{i}": i-1 for i in range(1, 13)},
            rng=np.random.default_rng(42), n_sims=256,
        )
        assert a.ndim == b.ndim == 1  # Exact native pairings, no sampled replacement schedule.
        assert len(weeks) == 48
        pack, seeds, records = poi._vectorized_regular_and_bracket(
            facts, future, {f"saved-{i}": 100 for i in range(1, 13)},
            {f"saved-{i}": 15 for i in range(1, 13)}, 2026, 6,
            playoff_slots=6, bye_slots=2, regular_season_weeks=14, num_teams=12,
        )
        assert pack["Exp_Final_Wins"].sum() == pytest.approx(84)
        assert pack["Exp_Final_PF"].sum() == pytest.approx(16800, abs=100)
        playoff_odds = poi.p_playoffs_from_seeds(seeds, 6)
        assert playoff_odds.sum() == pytest.approx(600)
        assert pack["P_Champ"].sum() == pytest.approx(100)
        assert playoff_odds.between(0, 100, inclusive="neither").all()
        assert np.allclose(records.sum(axis=1), 100)
        pd.testing.assert_frame_equal(db.read_table("matchup"), before)
