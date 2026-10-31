"""Yahoo's guillotine scoreboard repeats teams in its survivor matchup graph."""

from pathlib import Path

import pandas as pd
import pytest

from multi_league.core.fleet_publish import build_fleet_partition_bundle
from multi_league.core.local_db import LocalLeagueDB
from multi_league.data_fetchers.yahoo.yahoo_schedules import _derive_schedule_df_from_matchup_df
from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin


class _ScheduleRunner(MatchupEnrichmentsMixin, SQLEnrichmentsBase):
    pass


@pytest.mark.parametrize("last_week,raw_rows,published_rows", [(10, 196, 98), (13, 238, 119)])
def test_guillotine_schedule_rebuild_publishes_each_team_week_once(tmp_path, last_week, raw_rows, published_rows):
    # Anonymized from the actual Yahoo 470.l.454975 scoreboard, 2026-10-31.
    # Week 7 has eight matchup nodes / sixteen perspective rows but eight
    # teams. Future weeks have fourteen rows for seven remaining teams. The failed
    # quick import's 1..10 slice staged 119 rows, with 21 duplicate identities.
    source = pd.read_csv(Path(__file__).with_name("fixtures") / "yahoo_guillotine_scoreboard.csv")
    source = source.loc[source["week"].le(last_week)].copy()
    assert len(source) == raw_rows
    source["year"] = 2026
    for column, prefix in (("manager_guid", "owner-"), ("manager", "Owner "), ("team_name", "Team ")):
        source[column] = prefix + source["team_slot"].astype(str)
    source["team_key"] = "470.l.1.t." + source["team_slot"].astype(str)
    source["opponent_team_key"] = "470.l.1.t." + source["opponent_slot"].astype(str)
    source["opponent_guid"] = "owner-" + source["opponent_slot"].astype(str)
    source["opponent"] = "Owner " + source["opponent_slot"].astype(str)

    expected_scores = source[["year", "week", "manager_guid", "team_points"]].drop_duplicates()
    assert len(expected_scores) == published_rows  # repeats do not conflict on team scores
    with LocalLeagueDB(tmp_path, "guillotine") as db:
        db.save_table("matchup", source.loc[source["week"].le(7)], platform="yahoo", league_id="470.l.1")
        db.save_table("schedule", _derive_schedule_df_from_matchup_df(source, 2026, {}),
                      platform="yahoo", league_id="470.l.1")
        runner = _ScheduleRunner(db_name="guillotine", conn=db.connect(), data_dir=str(tmp_path))
        assert runner.dedup_matchup_publish_identity() == 77
        for _ in range(2):
            runner.build_schedule_from_matchup()
            schedule = db.read_table("schedule")
            assert len(schedule) == published_rows
            assert schedule["manager_week"].is_unique
            actual_scores = schedule[["year", "week", "manager_guid", "team_points"]]
            pd.testing.assert_frame_equal(
                actual_scores.sort_values(["week", "manager_guid"]).reset_index(drop=True),
                expected_scores.sort_values(["week", "manager_guid"]).reset_index(drop=True),
                check_dtype=False,
            )
            assert len(schedule.loc[schedule["week"].eq(8)]) == 7
            live_candidate = schedule.loc[schedule["week"].eq(8) & schedule["manager_guid"].eq("owner-14")].iloc[0]
            assert (live_candidate["team_points"], live_candidate["opponent_points"],
                    live_candidate["opponent_guid"]) == (9.6, 42.2, "owner-13")

        # Exercise the actual quick-publish identity gate, without uploading.
        build_fleet_partition_bundle(
            db.connect(), active_year=2026, league_generations={"guillotine": 0},
            tables=["matchup", "schedule"], output_dir=tmp_path / "bundle",
            quick_years=[2026], rebuild_career_rollups=True, rebuild_homepage_rollups=True,
        )


def test_schedule_rebuild_dry_run_leaves_provider_rows_unchanged(tmp_path):
    with LocalLeagueDB(tmp_path, "guillotine") as db:
        db.ensure_table("matchup")
        db.ensure_table("schedule")
        conn = db.connect()
        conn.execute("INSERT INTO public.schedule (db_name,year,week,manager_week) "
                     "VALUES ('guillotine',2026,8,'owner_2026_8'),('guillotine',2026,8,'owner_2026_8')")
        runner = _ScheduleRunner(db_name="guillotine", conn=conn, data_dir=str(tmp_path), dry_run=True)

        runner.build_schedule_from_matchup()

        assert conn.execute("SELECT COUNT(*) FROM public.schedule").fetchone() == (2,)
