"""Career matchup rollup tests over explicit season components."""

import duckdb
import pytest

from matchup_career_sql import matchup_career_sql


def test_career_sums_expected_counts_and_weekly_components():
    con = duckdb.connect()
    con.execute("""CREATE TABLE season AS SELECT * FROM (VALUES
      ('12t','flx','half','4pt','6po','ALL','ALL','ALL',4,0,2023,'p1',100,10,800.0,800.0,8,1000.0,
       90.0,20.0,55.0,5.0,3.0,8.0,120.0,4.0,25,100,40,100,200.0),
      -- weekly_start_rate_sum 400 = 100 x expected_starts 4.0, so the season rows are
      -- internally consistent and the career won+lost identity is actually exercised
      ('12t','flx','half','4pt','6po','ALL','ALL','ALL',4,0,2024,'p1',120,5,400.0,400.0,4,500.0,
       80.0,15.0,40.0,2.0,2.0,4.0,80.0,1.0,40,100,20,100,100.0)
    ) t(teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,
        cohort_level,format_level,year,NFL_player_id,n_leagues,
        active_weeks,weekly_start_rate_sum,weekly_healthy_rate_sum,started_weeks,elig_league_weeks,
        roster_rate_pct,ppg_when_started,win_rate_pct,
        expected_wins,expected_losses,expected_starts,total_lamar_started,
        avg_clutch_started,started_champ_active,champ_elig_leagues,n_started_po,
        po_n_rostered_leagues,total_points_observed)""")
    con.execute("ALTER TABLE season ADD COLUMN po_wkwt_credit DOUBLE")
    con.execute("""UPDATE season SET po_wkwt_credit = CASE year
        WHEN 2023 THEN 70.0 WHEN 2024 THEN 50.0 END""")
    con.execute("ALTER TABLE season ADD COLUMN nfl_active_weeks BIGINT")
    con.execute("UPDATE season SET nfl_active_weeks = active_weeks")
    con.execute("ALTER TABLE season ADD COLUMN sum_clutch_started_active DOUBLE")
    con.execute("UPDATE season SET sum_clutch_started_active = avg_clutch_started * champ_elig_leagues")
    con.execute("ALTER TABLE season ADD COLUMN champ_eligible_league_weeks BIGINT")
    con.execute("UPDATE season SET champ_eligible_league_weeks = champ_elig_leagues")
    con.execute("ALTER TABLE season ADD COLUMN wins_started BIGINT")
    con.execute("ALTER TABLE season ADD COLUMN losses_started BIGINT")
    con.execute("""UPDATE season SET
        wins_started = CASE year WHEN 2023 THEN 6 WHEN 2024 THEN 0 END,
        losses_started = CASE year WHEN 2023 THEN 2 WHEN 2024 THEN 1 END""")
    # These are the additive weekly components used by the new availability contract.
    # They intentionally differ by year so a simple average of season percentages fails.
    for col, y2023, y2024 in (
        ("rostered_league_weeks", 720, 300),
        ("roster_eligible_league_weeks", 800, 500),
        ("started_team_game_weeks", 640, 300),
        ("team_game_eligible_league_weeks", 800, 500),
        ("started_active_weeks", 8, 4),
        ("healthy_eligible_league_weeks", 640, 400),
    ):
        con.execute(f"ALTER TABLE season ADD COLUMN {col} BIGINT")
        con.execute(f"UPDATE season SET {col} = CASE year "
                    f"WHEN 2023 THEN {y2023} WHEN 2024 THEN {y2024} END")
    # T7/T8 season components. n_leagues (100/120) is the LOCKED denominator for every
    # champ/playoff rate, so the career numbers are league-YEAR weighted, not year-averaged.
    for col, typ, y2023, y2024 in (
        ("n_final_po", "BIGINT", 60, 30),
        ("n_champ_leagues", "BIGINT", 20, 12),
        ("n_champ_start_leagues", "BIGINT", 10, 6),
        ("inactive_weeks", "BIGINT", 2, 5),
    ):
        con.execute(f"ALTER TABLE season ADD COLUMN {col} {typ}")
        con.execute(f"UPDATE season SET {col} = CASE year "
                    f"WHEN 2023 THEN {y2023} WHEN 2024 THEN {y2024} END")
    con.execute("ALTER TABLE season ADD COLUMN playoff_eligible_leagues BIGINT")
    con.execute("UPDATE season SET playoff_eligible_leagues = CASE year WHEN 2023 THEN 80 WHEN 2024 THEN 60 END")

    row = con.execute(matchup_career_sql("season")).fetchone()
    columns = [item[0] for item in con.description]
    values = dict(zip(columns, row))
    con.close()

    # Expected counts use only the seasons' qualified champion/playoff populations.
    assert values["expected_champs"] == pytest.approx(10 / 100 + 6 / 100)
    # Expected Playoffs is the sum of the same as-starter event displayed in
    # Playoffs, not the broader "rostered on a playoff team" diagnostic.
    assert values["expected_playoffs"] == pytest.approx(40 / 80 + 20 / 60)
    assert values["playoff_rate_wkwt"] == pytest.approx(60.0)
    # T7 career rates: numerator league-years over eligible league-years (100 + 120 = 220)
    assert values["playoff_total_pct"] == pytest.approx(100.0 * 90 / 140)
    assert values["playoff_as_starter_pct"] == pytest.approx(100.0 * 60 / 140)
    assert values["champ_total_pct"] == pytest.approx(100.0 * 32 / 200)
    assert values["champ_as_starter_pct"] == pytest.approx(100.0 * 16 / 200)
    assert values["inactive_weeks"] == 7
    assert values["expected_wins"] == pytest.approx(7.0)
    assert values["expected_losses"] == pytest.approx(5.0)
    assert values["win_rate_pct"] == pytest.approx(100.0 * 7.0 / 12.0)
    assert values["wins_started"] == 6
    assert values["losses_started"] == 3
    assert values["total_lamar_started"] == pytest.approx(200.0)
    # Career clutch is champion-population weighted: (4*100 + 1*100)/(100+100).
    assert values["avg_clutch_started"] == pytest.approx(2.5)
    assert values["roster_rate_pct"] == pytest.approx(100.0 * 1020 / 1300)
    assert values["start_rate_pct"] == pytest.approx(100.0 * 940 / 1300)
    assert values["healthy_start_rate_pct"] == pytest.approx(100.0 * 12 / 1040)
    # roster_rate_pct is a league-weighted mean of an ALREADY-percent column -- no second
    # x100. The stray multiplier put 97% of career rows over 100% (max exactly 10,000).
    assert values["roster_rate_pct"] <= 100.0
    # both seasons carry a decided record, so the lane covers the whole career. The
    # displayed W-L rates use the all-start eligible exposure, not active-week counts.
    assert values["decided_active_weeks"] == 15
    assert values["won_pct"] == pytest.approx(100.0 * 7 / 1300)
    assert values["lost_pct"] == pytest.approx(100.0 * 5 / 1300)
    assert values["total_points_observed"] == pytest.approx(300.0)
    assert values["n_years"] == 2
    assert values["active_weeks"] == 15


def test_career_record_lane_ignores_undecided_seasons():
    """Ledger D13 at the matchup career grain.

    A season whose started weeks produced NO decided result carries NULL expected
    wins/losses on purpose -- unknown outcomes must not become losses. SUM() skips those
    NULLs, so dividing by ALL active weeks charged the player for seasons the numerator
    could never cover. Measured on the 2026-07-26 build: 225,812 undecided season rows,
    breaking won+lost=start_rate on 6,166 career rows by ~9.3 points.

    Here 2023 is decided and 2022 is not. The record lane must speak only for 2023, while
    start_rate_pct still speaks for both.
    """
    con = duckdb.connect()
    con.execute("""CREATE TABLE season AS SELECT * FROM (VALUES
      ('12t','flx','half','4pt','6po','ALL','ALL','ALL',4,0,2023,'p1',100,10,800.0,800.0,8,1000.0,
       90.0,20.0,55.0,5.0,3.0,8.0,120.0,4.0,25,100,60,100,200.0),
      ('12t','flx','half','4pt','6po','ALL','ALL','ALL',4,0,2022,'p1',100,10,600.0,600.0,6,1000.0,
       90.0,20.0,NULL,NULL,NULL,6.0,120.0,4.0,25,100,60,100,200.0)
    ) t(teams,roster,ppr,td,bracket,league_type,lineup_mode,keeper_mode,
        cohort_level,format_level,year,NFL_player_id,n_leagues,
        active_weeks,weekly_start_rate_sum,weekly_healthy_rate_sum,started_weeks,elig_league_weeks,
        roster_rate_pct,ppg_when_started,win_rate_pct,
        expected_wins,expected_losses,expected_starts,total_lamar_started,
        avg_clutch_started,started_champ_active,champ_elig_leagues,n_started_po,
        po_n_rostered_leagues,total_points_observed)""")
    for col, typ, decided, undecided in (
        ("po_wkwt_credit", "DOUBLE", 70.0, 50.0),
        ("wins_started", "BIGINT", 6, 0),
        ("losses_started", "BIGINT", 2, 1),
        ("n_final_po", "BIGINT", 60, 30),
        ("n_champ_leagues", "BIGINT", 20, 12),
        ("n_champ_start_leagues", "BIGINT", 10, 6),
        ("inactive_weeks", "BIGINT", 2, 5),
    ):
        con.execute(f"ALTER TABLE season ADD COLUMN {col} {typ}")
        con.execute(f"UPDATE season SET {col} = CASE year "
                    f"WHEN 2023 THEN {decided} WHEN 2022 THEN {undecided} END")
    con.execute("ALTER TABLE season ADD COLUMN nfl_active_weeks BIGINT")
    con.execute("UPDATE season SET nfl_active_weeks = active_weeks")
    con.execute("ALTER TABLE season ADD COLUMN sum_clutch_started_active DOUBLE")
    con.execute("UPDATE season SET sum_clutch_started_active = avg_clutch_started * champ_elig_leagues")
    con.execute("ALTER TABLE season ADD COLUMN champ_eligible_league_weeks BIGINT")
    con.execute("UPDATE season SET champ_eligible_league_weeks = champ_elig_leagues")
    for col, decided, undecided in (
        ("rostered_league_weeks", 720, 540),
        ("roster_eligible_league_weeks", 800, 600),
        ("started_team_game_weeks", 640, 360),
        ("team_game_eligible_league_weeks", 800, 600),
        ("started_active_weeks", 8, 6),
        ("healthy_eligible_league_weeks", 640, 600),
    ):
        con.execute(f"ALTER TABLE season ADD COLUMN {col} BIGINT")
        con.execute(f"UPDATE season SET {col} = CASE year "
                    f"WHEN 2023 THEN {decided} WHEN 2022 THEN {undecided} END")
    con.execute("ALTER TABLE season ADD COLUMN playoff_eligible_leagues BIGINT")
    con.execute("UPDATE season SET playoff_eligible_leagues = 100")

    row = con.execute(matchup_career_sql("season")).fetchone()
    values = dict(zip([item[0] for item in con.description], row))
    con.close()

    # Start % speaks for both seasons from team-game eligible league-weeks.
    assert values["start_rate_pct"] == pytest.approx(100.0 * 1000 / 1400)
    # the record lane speaks for 2023 only: 10 active weeks, with all-start
    # eligible exposure as its rate denominator.
    assert values["decided_active_weeks"] == 10
    assert values["start_rate_decided_pct"] == pytest.approx(80.0)
    assert values["won_pct"] == pytest.approx(100.0 * 5 / 1400)
    assert values["lost_pct"] == pytest.approx(100.0 * 3 / 1400)
    # the undecided season is visible as the shortfall, not silently folded into losses
    assert values["start_rate_pct"] < values["start_rate_decided_pct"]
