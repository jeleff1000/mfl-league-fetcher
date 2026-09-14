"""EXECUTE every SQL statement the matchup builder runs, against a fixture schema.

Why this exists: `python -m py_compile` does NOT validate SQL. On 2026-07-19 a missing
table alias in po_sql (`FROM tw JOIN ls ON ls.db_name = t.db_name`) bound fine at import,
passed compile checks, and then killed the cycle with a BinderException **16.5 hours in** --
after the matchup and canon passes had already succeeded. Binder errors are cheap to catch
and ruinously expensive to discover late, so every statement gets exercised here in seconds.

The fixture mirrors the views LocalReader exposes. Rows are minimal but non-empty so the
statements bind AND run; this is a plumbing test, not a math test (the math lives in
test_build_research_matchup_canon.py).
"""
from pathlib import Path
import sys

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import build_research_matchup_cohort as M
from top10_contributions import contribution_queries

YEAR = 2024


def test_local_reader_view_gate_reports_unmounted_dependencies():
    c = duckdb.connect()
    c.execute("CREATE SCHEMA public")
    with pytest.raises(RuntimeError, match="player_team_game_week"):
        M.validate_local_reader_views(c)
    c.close()


def test_local_reader_view_gate_accepts_tables_and_views():
    c = duckdb.connect()
    c.execute("CREATE SCHEMA public")
    c.execute("CREATE TABLE public.player_position (id VARCHAR)")
    c.execute("CREATE TABLE public.player_active_week (id VARCHAR)")
    c.execute("CREATE TABLE public.player_team_game_week (id VARCHAR)")
    c.execute("CREATE VIEW public.player_slug_value AS SELECT 'p1' AS id")
    M.validate_local_reader_views(c)
    c.close()


def test_champion_season_flag_never_becomes_championship_start(monkeypatch):
    """A season winner marker is not evidence that this player started the title game."""
    monkeypatch.setattr(M, "_PLAYER_COLUMNS", {"champion"})
    monkeypatch.setattr(M, "_MATCHUP_COLUMNS", set())
    assert M.championship_signal_sql("p", "m") == "0"


@pytest.fixture()
def con():
    c = duckdb.connect()
    c.execute("CREATE SCHEMA public")
    c.execute("""CREATE TABLE public.league_settings (
        db_name VARCHAR, year INTEGER, num_teams INTEGER, is_dynasty BOOLEAN,
        sleeper_best_ball BOOLEAN, roster_IDP INTEGER, roster_DL INTEGER, roster_LB INTEGER,
        roster_DB INTEGER, roster_DB_LB INTEGER, roster_DL_LB INTEGER,
        roster_SUPER_FLEX INTEGER, scoring_rec DOUBLE, scoring_pass_td DOUBLE,
        playoff_start_week INTEGER, playoff_teams INTEGER, roster_K INTEGER,
        roster_DEF INTEGER, scoring_sane BOOLEAN)""")
    c.execute(f"""INSERT INTO public.league_settings VALUES
        ('lg1', {YEAR}, 12, false, false, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, 1.0, 4.0, 15, 6, 1, 1, true)""")
    # Position-slot cohort routing reads these otherwise-optional roster counts.
    # Adding them after the legacy fixture row preserves older INSERT shapes below.
    for col, value in (("roster_QB", 1), ("roster_RB", 2), ("roster_WR", 2),
                       ("roster_TE", 1), ("roster_FLX", 2)):
        c.execute(f"ALTER TABLE public.league_settings ADD COLUMN {col} INTEGER")
        c.execute(f"UPDATE public.league_settings SET {col}={value} WHERE db_name='lg1'")
    # final_playoff_seed / made_po_bf / is_playoffs_bf mirror the LocalReader player_fantasy
    # contract (row-level playoff outcome + the Sleeper backfill overlay). po_sql reads all
    # three; omitting them here let a BinderException hide behind a green-looking suite.
    c.execute("""CREATE TABLE public.player_fantasy (
        db_name VARCHAR, year INTEGER, week INTEGER, NFL_player_id VARCHAR,
        is_started INTEGER, is_rostered INTEGER, fantasy_points DOUBLE, win INTEGER,
        champion INTEGER, clutch_equity DOUBLE, manager_lamar DOUBLE,
        manager VARCHAR, team_points DOUBLE,
        final_playoff_seed INTEGER, made_po_bf TINYINT, is_playoffs_bf TINYINT)""")
    # week 14 = the last regular-season week (playoff_start_week 15), so the "rostered at the
    # end of the regular season" shape has something to find; week 15 is a real playoff game.
    # week 16 is the title game: `champion` flags the WHOLE champion roster, started or not.
    for wk in (1, 2, 14, 15):
        for pid in ('p1', 'punter', 'idp1', 'dual'):
            c.execute(f"""INSERT INTO public.player_fantasy VALUES
                ('lg1', {YEAR}, {wk}, '{pid}', 1, 1, 20.0, 1, 0, 0.5, 3.0, 'Manager A', 110.0,
                 2, NULL, {1 if wk == 15 else 0})""")
    # champion week: p1 is in the title-game STARTING lineup, 'dual' is on the bench.
    c.execute(f"""INSERT INTO public.player_fantasy VALUES
        ('lg1', {YEAR}, 16, 'p1',   1, 1, 20.0, 1, 1, 0.5, 3.0, 'Manager A', 110.0, 2, NULL, 1),
        ('lg1', {YEAR}, 16, 'dual', 0, 1, 0.0,  1, 1, 0.0, 0.0, 'Manager A', 110.0, 2, NULL, 1)""")
    c.execute("""CREATE TABLE public.matchup (
        db_name VARCHAR, year INTEGER, week INTEGER, manager VARCHAR, franchise_id VARCHAR,
        team_points DOUBLE, is_playoffs INTEGER, playoff_seed INTEGER,
        final_playoff_seed INTEGER, champion INTEGER)""")
    c.execute(f"""INSERT INTO public.matchup VALUES
        ('lg1', {YEAR}, 15, 'Manager A', 'f1', 110.0, 1, 2, 2, 0)""")
    c.execute("""CREATE TABLE public.draft (
        db_name VARCHAR, year INTEGER, total_fantasy_points DOUBLE, is_keeper BOOLEAN)""")
    c.execute(f"INSERT INTO public.draft VALUES ('lg1', {YEAR}, 120.0, false)")
    # broad_position mirrors LocalReader's taxonomy-backed view; the eligibility gate reads it
    # broad_positions is a LIST: a player may be dual-eligible (Taysom Hill 'QB,TE'), and
    # eligibility asks whether ANY of his classes can start.
    c.execute("""CREATE TABLE public.player_position (
        NFL_player_id VARCHAR, year INTEGER, position VARCHAR,
        broad_position VARCHAR, broad_positions VARCHAR[])""")
    c.execute(f"""INSERT INTO public.player_position VALUES
        ('p1',     {YEAR}, 'WR',    'WR', ['WR']),
        ('punter', {YEAR}, 'P',     'P',  ['P']),
        ('idp1',   {YEAR}, 'RDE',   'DL', ['DL']),
        ('dual',   {YEAR}, 'QB,TE', 'QB', ['QB','TE'])""")  # Taysom-style dual eligibility
    # The leakage regression below adds a second league-year; give the denominator
    # lattice the same immutable position index for that year even though it has no
    # player-fantasy rows.
    c.execute("""INSERT INTO public.player_position
        SELECT NFL_player_id, 2023, position, broad_position, broad_positions
        FROM public.player_position WHERE year = 2024""")
    c.execute("CREATE TABLE public.player_active_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)")
    # p1 is on an NFL field in weeks 1, 14, 15, 16 -- but NOT week 2. He is rostered that
    # week, so week 2 is exactly one INACTIVE week (T8) and must be excluded from every
    # active-scoped denominator.
    for wk in (1, 14, 15, 16):
        c.execute(f"INSERT INTO public.player_active_week VALUES ('p1', {YEAR}, {wk})")
    # Team-game support includes week 2 even though p1 did not appear. This is the
    # Start % denominator; player_active_week remains the Healthy Start % support.
    c.execute("CREATE TABLE public.player_team_game_week (NFL_player_id VARCHAR, year INTEGER, week INTEGER)")
    for wk in (1, 2, 14, 15, 16):
        c.execute(f"INSERT INTO public.player_team_game_week VALUES ('p1', {YEAR}, {wk})")
    c.execute("""CREATE TABLE public.player_slug_value (
        NFL_player_id VARCHAR, year INTEGER, week INTEGER, slug VARCHAR, lamar DOUBLE)""")
    for wk in (1, 2, 14, 15, 16):
        c.execute(f"""INSERT INTO public.player_slug_value VALUES
            ('p1', {YEAR}, {wk}, '12t_flx_ppr_4pt', 4.0)""")
    return c


def test_every_builder_statement_binds_and_runs(con):
    """Mirrors main()'s execution order. Any BinderException fails here in seconds."""
    # --- the per-year read queries ---
    con.execute(f"CREATE TABLE p AS {M.player_sql(YEAR)}")
    con.execute(f"CREATE TABLE wp AS {M.weekly_player_sql(YEAR)}")
    con.execute(f"CREATE TABLE d AS {M.DENOM_SQL}")
    con.execute(f"CREATE TABLE dw AS {M.DENOM_WEEK_SQL}")
    con.execute(f"CREATE TABLE act AS {M.ACTIVE_SQL}")
    con.execute(f"CREATE TABLE sa AS {M.sa_sql(YEAR)}")
    con.execute(f"CREATE TABLE nst AS {M.nstart_sql(YEAR)}")
    con.execute(f"CREATE TABLE psv AS {M.PSV_SQL}")
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    con.execute(f"CREATE TABLE val AS {M.po_val_sql(YEAR)}")

    # --- the derived tables, in build order ---
    con.execute(M.CANON0_SQL)
    con.execute(M.CANON_LATTICE_SQL)
    con.execute(M.NSTL_SQL)
    con.execute(M.PO_LATTICE_SQL)
    con.execute(M.PDENOM_SQL)
    # iterate exactly as main() does. Handing the whole blob to one execute() is what let a
    # broken statement split ship: DuckDB understands `--` comments, the old text splitter
    # did not, so the test path passed while every shard died at the stage guard.
    for _label, sql in M.WEEKLY_STATEMENTS:
        con.execute(sql)
    con.execute(M.FINAL_SQL)

    # the fixture player must survive to the served table with his lanes populated
    row = con.execute("""SELECT start_rate_pct, win_rate_pct, expected_wins,
        expected_losses, expected_starts, total_lamar_started, avg_lamar_started,
        playoff_rate_wkwt, playoff_rate_final, playoff_rate_started
        FROM final WHERE teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND bracket='6po' AND league_type='redraft' AND lineup_mode='managed'
          AND keeper_mode='ALL'""").fetchone()
    assert row is not None, "fixture player did not reach the final table"
    assert row[5] is not None, "canonical total LAMAR should be populated for a flx cell"
    assert row[2] + row[3] == pytest.approx(row[4])

    weekly = con.execute("""SELECT start_rate_pct, win_rate_pct, expected_wins,
        expected_losses, expected_starts, team_game_eligible_leagues, started_leagues
        FROM weekly_final WHERE teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND bracket='6po' AND league_type='redraft' AND lineup_mode='managed'
          AND keeper_mode='ALL'
        ORDER BY week LIMIT 1""").fetchone()
    assert weekly == pytest.approx((100.0, 100.0, 1.0, 0.0, 1.0, 1, 1))

    # every served rate is bounded by construction -- this is the gate that caught
    # win_rate_started_pct at 1,700% and pct_starts_in_playoffs at 1,200%
    over = con.execute("""SELECT COUNT(*) FROM final WHERE
        GREATEST(COALESCE(roster_rate_pct,0), COALESCE(start_rate_pct,0),
                 COALESCE(won_pct,0), COALESCE(lost_pct,0), COALESCE(win_rate_pct,0),
                 COALESCE(win_rate_started_pct,0), COALESCE(pct_starts_in_playoffs,0),
                 COALESCE(playoff_total_pct,0), COALESCE(playoff_as_starter_pct,0),
                 COALESCE(champ_total_pct,0), COALESCE(champ_as_starter_pct,0)) > 100.001
        """).fetchone()[0]
    assert over == 0, f"{over} rows carry an impossible (>100%) rate"


def test_champ_and_playoff_use_signal_denominators(con):
    """Champ and playoff rates use only leagues with the corresponding signal."""
    con.execute(f"""INSERT INTO public.league_settings VALUES
        ('lg2', {YEAR}, 12, false, false, NULL, NULL, NULL, NULL, NULL, NULL,
         NULL, 1.0, 4.0, 15, 6, 1, 1, true, 1, 2, 2, 1, 2)""")
    for wk in (1, 14, 15, 16):
        con.execute(f"""INSERT INTO public.player_fantasy VALUES
            ('lg2', {YEAR}, {wk}, 'p1', 1, 1, 20.0, 1, 0, 0.5, 3.0,
             'Manager B', 110.0, NULL, NULL, NULL)""")

    con.execute(f"CREATE TABLE p AS {M.player_sql(YEAR)}")
    con.execute(f"CREATE TABLE wp AS {M.weekly_player_sql(YEAR)}")
    con.execute(f"CREATE TABLE d AS {M.DENOM_SQL}")
    con.execute(f"CREATE TABLE dw AS {M.DENOM_WEEK_SQL}")
    con.execute(f"CREATE TABLE act AS {M.ACTIVE_SQL}")
    con.execute(f"CREATE TABLE sa AS {M.sa_sql(YEAR)}")
    con.execute(f"CREATE TABLE nst AS {M.nstart_sql(YEAR)}")
    con.execute(f"CREATE TABLE psv AS {M.PSV_SQL}")
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    con.execute(f"CREATE TABLE val AS {M.po_val_sql(YEAR)}")
    con.execute(M.CANON0_SQL)
    con.execute(M.CANON_LATTICE_SQL)
    con.execute(M.NSTL_SQL)
    con.execute(M.PO_LATTICE_SQL)
    con.execute(M.PDENOM_SQL)
    for _, sql in M.WEEKLY_STATEMENTS:
        con.execute(sql)
    con.execute(M.FINAL_SQL)

    champ, playoff, champ_denom = con.execute("""SELECT champ_as_starter_pct, playoff_total_pct,
        champ_elig_leagues
        FROM final WHERE teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND league_type='ALL' AND lineup_mode='ALL' AND keeper_mode='ALL'
          AND cohort_level=4 AND format_level=0""").fetchone()
    assert champ_denom == 1
    assert champ == pytest.approx(0.0), "a season champion marker is not a title-game start"
    assert playoff == pytest.approx(100.0)
    # Champion eligibility is a season property.  The champion is stamped only in week 16
    # in this fixture, but every weekly denominator must still carry the same one-league
    # champion population; using the current week's champion rows would yield zero before
    # the title week and a one-row denominator only at the end.
    champ_week_denoms = con.execute("""
        SELECT week, n_champ_lg FROM dw
        WHERE teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND pos_grp='QB' ORDER BY week
    """).fetchall()
    assert champ_week_denoms and all(n == 1 for _, n in champ_week_denoms)


def test_postseason_signals_are_scoped_to_the_league_year(con):
    """A title/playoff signal from one season must not leak into another."""
    con.execute(
        "INSERT INTO public.league_settings "
        "SELECT 'lg2023', 2023, * EXCLUDE (db_name, year) "
        "FROM public.league_settings WHERE db_name='lg1' AND year=2024"
    )
    con.execute("""INSERT INTO public.player_fantasy VALUES
        ('lg2023', 2023, 1, 'dual', 1, 1, 20.0, 1, 0, 0.5, 3.0, 'Manager A', 110.0,
         2, NULL, 0)""")
    con.execute(f"CREATE TABLE d AS {M.DENOM_SQL}")
    n_leagues, n_champ = con.execute("""
        SELECT MAX(n_leagues), MAX(n_leagues_champ)
        FROM d
        WHERE year=2023 AND teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND pos_grp='QB'
    """).fetchone()
    assert n_leagues == 1
    assert n_champ == 0


def test_per_league_player_queries_preserve_league_identity(con):
    con.execute(f"CREATE TABLE player_league AS {M.player_sql(YEAR, per_league=True)}")
    con.execute(f"CREATE TABLE weekly_league AS {M.weekly_player_sql(YEAR, per_league=True)}")

    assert con.execute(
        "SELECT DISTINCT db_name FROM player_league ORDER BY 1"
    ).fetchall() == [("lg1",)]
    assert con.execute(
        "SELECT DISTINCT db_name FROM weekly_league ORDER BY 1"
    ).fetchall() == [("lg1",)]
    assert con.execute(
        "SELECT MAX(n_rostered_leagues) FROM player_league"
    ).fetchone()[0] == 1
    assert con.execute(
        "SELECT position FROM player_league WHERE NFL_player_id='p1'"
    ).fetchone()[0] == "WR"
    assert con.execute(
        "SELECT position FROM weekly_league WHERE NFL_player_id='p1' ORDER BY week LIMIT 1"
    ).fetchone()[0] == "WR"


def test_player_cohort_teams_bucket_follows_primary_position_slots(con):
    """One league may route its RB and WR rows to different market-size buckets."""
    # This is a shallow-RB but deep-WR 12-team league: RB is a 10t-equivalent
    # market; WR remains a 12t-equivalent market under the frozen slot model.
    con.execute("UPDATE public.league_settings SET roster_RB=1, roster_WR=3, roster_FLX=1")
    for wk in (1, 14, 15, 16):
        con.execute(f"""INSERT INTO public.player_fantasy VALUES
            ('lg1', {YEAR}, {wk}, 'rb_slot', 1, 1, 12.0, 1, 0, 0.0, 2.0,
             'Manager A', 110.0, 2, NULL, 0)""")
    con.execute(f"""INSERT INTO public.player_position VALUES
        ('rb_slot', {YEAR}, 'RB', 'RB', ['RB'])""")
    for wk in (1, 14, 15, 16):
        con.execute(f"INSERT INTO public.player_active_week VALUES ('rb_slot', {YEAR}, {wk})")
        con.execute(f"INSERT INTO public.player_team_game_week VALUES ('rb_slot', {YEAR}, {wk})")
        # The RB is bucketed as a 10t-equivalent market, but its NFL LAMAR
        # value was computed in the literal 12-team league.  Deliberately do
        # not add a 10t source value: a wrong canonical join must go NULL.
        con.execute(f"""INSERT INTO public.player_slug_value VALUES
            ('rb_slot', {YEAR}, {wk}, '12t_flx_ppr_4pt', 3.0)""")

    # TIER CONTRACT v2 (2026-08-02): teams_* is OBSERVED capacity, so the split across
    # positions has to be observable -- give the league real roster rows. A deep WR market
    # (12 spots/week) and a thin RB market (6) is exactly the asymmetry this test exists to
    # prove. With no rows at all the v2 fallback returns one num_teams bucket for every
    # position and the split is unprovable, which is a fixture gap, not a contract failure.
    for wk in (1, 14, 15, 16):
        for i in range(12):
            con.execute("INSERT INTO public.player_fantasy (db_name, year, week, NFL_player_id) "
                        f"VALUES ('lg', {YEAR}, {wk}, 'wrfill{i}')")
        for i in range(6):
            con.execute("INSERT INTO public.player_fantasy (db_name, year, week, NFL_player_id) "
                        f"VALUES ('lg', {YEAR}, {wk}, 'rbfill{i}')")
    for i in range(12):
        con.execute("INSERT INTO ops.nfl_historical.nfl_player_stats_all "
                    "(NFL_player_id, year, position) "
                    f"VALUES ('wrfill{i}', {YEAR}, 'WR')")
    for i in range(6):
        con.execute("INSERT INTO ops.nfl_historical.nfl_player_stats_all "
                    "(NFL_player_id, year, position) "
                    f"VALUES ('rbfill{i}', {YEAR}, 'RB')")

    con.execute(f"CREATE TABLE player_slots AS {M.player_sql(YEAR)}")
    buckets = con.execute("""SELECT NFL_player_id, teams FROM player_slots
        WHERE NFL_player_id IN ('p1', 'rb_slot')
          AND league_type='ALL' AND lineup_mode='ALL' AND keeper_mode='ALL'""").fetchall()
    # v2 alphabet: four buckets cut from observed capacity, not the old two-level '10t'/'12t'
    assert any(pid == 'p1' and tier in {'08tm', '10tm', '12tm', '14tm'} for pid, tier in buckets)
    assert any(pid == 'rb_slot' and tier in {'08tm', '10tm', '12tm', '14tm'}
               for pid, tier in buckets)

    # Every downstream primitive must use the identical key or the row falls out
    # at a denominator/canonical/playoff join before it reaches the served table.
    con.execute(f"CREATE TABLE p AS {M.player_sql(YEAR)}")
    con.execute(f"CREATE TABLE wp AS {M.weekly_player_sql(YEAR)}")
    con.execute(f"CREATE TABLE d AS {M.DENOM_SQL}")
    con.execute(f"CREATE TABLE dw AS {M.DENOM_WEEK_SQL}")
    con.execute(f"CREATE TABLE act AS {M.ACTIVE_SQL}")
    con.execute(f"CREATE TABLE sa AS {M.sa_sql(YEAR)}")
    con.execute(f"CREATE TABLE nst AS {M.nstart_sql(YEAR)}")
    con.execute(f"CREATE TABLE psv AS {M.PSV_SQL}")
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    con.execute(M.CANON0_SQL)
    con.execute(M.CANON_LATTICE_SQL)
    con.execute(M.NSTL_SQL)
    con.execute(M.PO_LATTICE_SQL)
    con.execute(M.PDENOM_SQL)
    for _, sql in M.WEEKLY_STATEMENTS:
        con.execute(sql)
    con.execute(M.FINAL_SQL)
    row = con.execute("""SELECT total_lamar_started FROM final
        WHERE NFL_player_id='rb_slot' AND teams IN ('08tm','10tm','12tm','14tm')""").fetchone()
    assert row is not None and row[0] is not None


def test_inactive_weeks_exclude_team_byes(con):
    """A rostered player on bye is neither active nor inactive for research usage."""
    con.execute(f"""INSERT INTO public.player_position VALUES
        ('bye_player', {YEAR}, 'RB', 'RB', ['RB'])""")
    for wk in (1, 2):
        con.execute(f"""INSERT INTO public.player_fantasy VALUES
            ('lg1', {YEAR}, {wk}, 'bye_player', 0, 1, 0.0, 0, 0, 0.0, 0.0,
             'Manager A', 110.0, 2, NULL, 0)""")
    # Week 2 has no team-game record: it is a bye, not an injury/scratch.
    con.execute(f"INSERT INTO public.player_team_game_week VALUES ('bye_player', {YEAR}, 1)")
    con.execute(f"INSERT INTO public.player_active_week VALUES ('bye_player', {YEAR}, 1)")

    con.execute(f"CREATE TABLE player_byes AS {M.player_sql(YEAR)}")
    inactive = con.execute("""SELECT inactive_week_mask FROM player_byes
        WHERE NFL_player_id='bye_player' AND teams='12tm' AND roster='flx'
          AND ppr='ppr' AND td='4pt' AND league_type='ALL'
          AND lineup_mode='ALL' AND keeper_mode='ALL'""").fetchone()[0]
    assert inactive == 0


def test_rostered_week_on_team_bye_stays_in_roster_lane(con):
    """Roster% includes a rostered player during his team's bye."""
    con.execute(f"""INSERT INTO public.player_position VALUES
        ('rostered_bye', {YEAR}, 'RB', 'RB', ['RB'])""")
    con.execute(f"""INSERT INTO public.player_fantasy VALUES
        ('lg1', {YEAR}, 2, 'rostered_bye', 0, 1, 0.0, 0, 0, 0.0, 0.0,
         'Manager A', 110.0, 2, NULL, 0)""")
    # No player_team_game_week row for week 2: this is a bye, not an absent row.
    con.execute(f"CREATE TABLE rostered_bye_week AS {M.weekly_player_sql(YEAR)}")
    row = con.execute("""SELECT rostered_leagues, started_leagues,
        healthy_started_leagues
        FROM rostered_bye_week
        WHERE NFL_player_id='rostered_bye' AND week=2
          AND teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND league_type='ALL' AND lineup_mode='ALL' AND keeper_mode='ALL'""").fetchone()
    assert row == (1, 0, 0)


def test_started_bye_does_not_enter_started_fallback_primitives(con):
    """A lineup mistake on a bye remains rostered but is not a real start."""
    con.execute(f"""INSERT INTO public.player_position VALUES
        ('started_bye', {YEAR}, 'RB', 'RB', ['RB'])""")
    con.execute(f"""INSERT INTO public.player_fantasy VALUES
        ('lg1', {YEAR}, 2, 'started_bye', 1, 1, 10.0, 1, 0, 5.0, 7.0,
         'Manager A', 110.0, 2, NULL, 0)""")
    con.execute(f"CREATE TABLE started_bye_player AS {M.player_sql(YEAR)}")
    row = con.execute("""SELECT rostered_weeks, started_weeks, sum_pts_started,
        sum_clutch_started, sum_lamar_started
        FROM started_bye_player
        WHERE NFL_player_id='started_bye' AND teams='12tm' AND roster='flx'
          AND ppr='ppr' AND td='4pt' AND league_type='ALL'
          AND lineup_mode='ALL' AND keeper_mode='ALL'""").fetchone()
    assert row == (1, 0, None, None, None)


def test_def_team_game_is_healthy_even_without_player_active_row(con):
    """DST availability follows the team schedule, not player-active feed sparsity."""
    con.execute(f"""INSERT INTO public.player_position VALUES
        ('def_team', {YEAR}, 'DEF', 'DEF', ['DEF'])""")
    con.execute(f"""INSERT INTO public.player_fantasy VALUES
        ('lg1', {YEAR}, 2, 'def_team', 1, 1, 8.0, 1, 0, 0.0, 0.0,
         'Manager A', 110.0, 2, NULL, 0)""")
    # There is a team game but deliberately no player_active_week row.  A DST is
    # the team itself, so this must still count as healthy and active.
    con.execute(f"INSERT INTO public.player_team_game_week VALUES ('def_team', {YEAR}, 2)")
    con.execute(f"CREATE TABLE def_week AS {M.weekly_player_sql(YEAR)}")
    row = con.execute("""SELECT started_leagues, healthy_started_leagues
        FROM def_week
        WHERE NFL_player_id='def_team' AND week=2
          AND teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND league_type='ALL' AND lineup_mode='ALL' AND keeper_mode='ALL'""").fetchone()
    assert row == (1, 1)


def test_inactive_weeks_include_unrostered_team_game_absences(con):
    """Inactive time follows the NFL team schedule, even after a fantasy drop."""
    # p1's team played in week 2 but he did not.  Remove the fantasy row entirely:
    # availability must still report the missed NFL game, not treat roster history as
    # the schedule source.
    con.execute("DELETE FROM public.player_fantasy WHERE NFL_player_id='p1' AND week=2")
    for stmt in (f"CREATE TABLE p AS {M.player_sql(YEAR)}",
                 f"CREATE TABLE wp AS {M.weekly_player_sql(YEAR)}",
                 f"CREATE TABLE d AS {M.DENOM_SQL}",
                 f"CREATE TABLE dw AS {M.DENOM_WEEK_SQL}",
                 f"CREATE TABLE act AS {M.ACTIVE_SQL}",
                 f"CREATE TABLE sa AS {M.sa_sql(YEAR)}",
                 f"CREATE TABLE nst AS {M.nstart_sql(YEAR)}",
                 f"CREATE TABLE psv AS {M.PSV_SQL}",
                 f"CREATE TABLE po0 AS {M.po_sql(YEAR)}"):
        con.execute(stmt)
    for stmt in (M.CANON0_SQL, M.CANON_LATTICE_SQL, M.NSTL_SQL, M.PO_LATTICE_SQL,
                 M.PDENOM_SQL, M.WEEKLY_FINAL_SQL, M.FINAL_SQL):
        con.execute(stmt)

    inactive = con.execute("""SELECT inactive_weeks FROM final
        WHERE NFL_player_id='p1' AND teams='12tm' AND roster='flx'
          AND ppr='ppr' AND td='4pt' AND league_type='ALL'
          AND lineup_mode='ALL' AND keeper_mode='ALL'""").fetchone()[0]
    assert inactive == 1
    weekly_gap = con.execute("""SELECT started_leagues, rostered_leagues,
        team_game_eligible_leagues
        FROM weekly_final WHERE NFL_player_id='p1' AND week=2
          AND teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'
          AND league_type='ALL' AND lineup_mode='ALL' AND keeper_mode='ALL'""").fetchone()
    assert weekly_gap == (0, 0, 1)


def test_matchup_contribution_queries_execute_at_per_league_grain(con):
    queries = contribution_queries(YEAR, datasets=("matchup",))

    assert set(queries) == {
        f"matchup_weekly_{YEAR}",
        f"matchup_population_weekly_{YEAR}",
        f"matchup_active_{YEAR}",
    }
    for name, sql in queries.items():
        columns = {column[0] for column in con.execute(f"SELECT * FROM ({sql}) LIMIT 0").description}
        if name.startswith("matchup_weekly_"):
            assert {"db_name", "year", "NFL_player_id", "position"} <= columns


def test_locked_denominators_and_availability_split(con, monkeypatch):
    """T6/T7/T8 (Joe 2026-07-26). lg1 is ONE eligible league; p1 started every active week,
    made the playoffs, started a playoff game, and started the title game. 'dual' rode the
    champion's BENCH in the title game and must earn total credit but no as-starter credit.
    p1 is rostered in week 2 without an NFL field -> exactly one inactive week."""
    con.execute("ALTER TABLE public.player_fantasy ADD COLUMN is_championship INTEGER")
    con.execute("UPDATE public.player_fantasy SET is_championship = 1 WHERE week = 16")
    monkeypatch.setattr(M, "_PLAYER_COLUMNS", set(M._PLAYER_COLUMNS) | {"is_championship"})
    for stmt in (f"CREATE TABLE p AS {M.player_sql(YEAR)}",
                 f"CREATE TABLE wp AS {M.weekly_player_sql(YEAR)}",
                 f"CREATE TABLE d AS {M.DENOM_SQL}",
                 f"CREATE TABLE dw AS {M.DENOM_WEEK_SQL}",
                 f"CREATE TABLE act AS {M.ACTIVE_SQL}",
                 f"CREATE TABLE sa AS {M.sa_sql(YEAR)}",
                 f"CREATE TABLE nst AS {M.nstart_sql(YEAR)}",
                 f"CREATE TABLE psv AS {M.PSV_SQL}",
                 f"CREATE TABLE po0 AS {M.po_sql(YEAR)}"):
        con.execute(stmt)
    for stmt in (M.CANON0_SQL, M.CANON_LATTICE_SQL, M.NSTL_SQL, M.PO_LATTICE_SQL,
                 M.PDENOM_SQL, M.WEEKLY_FINAL_SQL, M.FINAL_SQL):
        con.execute(stmt)

    cell = ("teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt' "
            "AND league_type='ALL' AND NFL_player_id=")
    p1 = con.execute(f"""SELECT n_leagues, playoff_total_pct, playoff_as_starter_pct,
        champ_total_pct, champ_as_starter_pct, nfl_active_weeks, inactive_weeks, won_pct,
        active_week_mask
        FROM final WHERE {cell}'p1'""").fetchone()
    assert p1 is not None
    assert p1[0] == 1, "one eligible league in the fixture"
    # numerator 1 league / denominator 1 eligible league
    assert p1[1] == pytest.approx(100.0)   # rostered at the end of the regular season
    assert p1[2] == pytest.approx(100.0)   # started a playoff game
    assert p1[3] == pytest.approx(100.0)   # on the champion's roster
    assert p1[4] == pytest.approx(100.0)   # in the champion's starting lineup
    assert p1[5] == 4, "active weeks = 1, 14, 15, 16"
    assert p1[6] == 1, "week 2 is rostered-but-not-on-an-NFL-field"
    # T6: the served Win % is start% x win-when-started, so it rides the SAME denominator
    assert p1[7] == pytest.approx(100.0)
    assert p1[8] == (1 << 0) | (1 << 13) | (1 << 14) | (1 << 15)

    bench = con.execute(f"""SELECT champ_total_pct, champ_as_starter_pct
        FROM final WHERE {cell}'dual'""").fetchone()
    assert bench[0] == pytest.approx(100.0), "benched on the champion still counts as total"
    assert bench[1] == pytest.approx(0.0), "a benched title-game player earns NO starter credit"


def test_ineligible_positions_never_reach_a_flx_cohort(con):
    """Joe 2026-07-20: a punter turned up in the 2025 flx clutch board, and IDP players
    leaked across cohorts. IDP clutch is legitimate INSIDE an IDP cohort -- the defect is the
    leak. lg1 is a flx league, so only QB/RB/WR/TE/K/DEF may survive the gate."""
    con.execute(f"CREATE TABLE p AS {M.player_sql(YEAR)}")
    served = {r[0] for r in con.execute(
        "SELECT DISTINCT NFL_player_id FROM p WHERE roster = '12t' OR roster IS NOT NULL").fetchall()}
    assert "p1" in served, "an eligible WR must survive the gate"
    assert "punter" not in served, "a punter is startable nowhere and must be gated out"
    assert "idp1" not in served, "a DL must not appear in a flx cohort"
    assert "dual" in served, "a dual-eligible QB,TE (Taysom Hill) must NOT be dropped"


def test_duplicate_player_fantasy_rows_do_not_inflate_start_rate(con):
    """Fanout guard (2026-07-21). 32 corpus leagues (smpl_mfl_2024_*) carry 2-10 BYTE-IDENTICAL
    player_fantasy rows per (db_name, week, NFL_player_id) -- the fold UNION ALL'd the same
    league from overlapping slices. Undeduped, the raw COUNT(*)/SUM() numerator in pw tripled
    (started_active_weeks 42 over a 14-week denominator) and start_rate ran to 300% on 917
    rung-4 rows. The DENOM/canon/playoff lanes use COUNT(DISTINCT db_name) and are dup-safe, so
    only pw needs the pfd dedup. This makes the exact bug reproduce in the fixture and proves the
    fix: with pfd, start_rate stays <= 100 and started_active_weeks counts each league-week once.
    """
    # p1 already has one row per (lg1, week) for weeks 1,2,14,15. Add two identical copies of
    # each so p1 appears 3x per (db, week) -- the worst observed corpus league (smpl_mfl_2024_68961).
    for _ in range(2):
        for wk in (1, 2, 14, 15):
            con.execute(f"""INSERT INTO public.player_fantasy VALUES
                ('lg1', {YEAR}, {wk}, 'p1', 1, 1, 20.0, 1, 0, 0.5, 3.0, 'Manager A', 110.0,
                 2, NULL, {1 if wk == 15 else 0})""")

    con.execute(f"CREATE TABLE p AS {M.player_sql(YEAR)}")
    con.execute(f"CREATE TABLE wp AS {M.weekly_player_sql(YEAR)}")
    con.execute(f"CREATE TABLE d AS {M.DENOM_SQL}")
    con.execute(f"CREATE TABLE dw AS {M.DENOM_WEEK_SQL}")
    con.execute(f"CREATE TABLE act AS {M.ACTIVE_SQL}")
    con.execute(f"CREATE TABLE sa AS {M.sa_sql(YEAR)}")
    con.execute(f"CREATE TABLE nst AS {M.nstart_sql(YEAR)}")
    con.execute(f"CREATE TABLE psv AS {M.PSV_SQL}")
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    con.execute(M.CANON0_SQL); con.execute(M.CANON_LATTICE_SQL); con.execute(M.NSTL_SQL)
    con.execute(M.PO_LATTICE_SQL); con.execute(M.PDENOM_SQL)
    con.execute(M.WEEKLY_FINAL_SQL); con.execute(M.FINAL_SQL)

    # invariant that must hold for EVERY row: a rate cannot exceed its denominator
    bad = con.execute(
        "SELECT COUNT(*) FROM final WHERE start_rate_pct > 100.0").fetchone()[0]
    assert bad == 0, f"{bad} rows have start_rate > 100 -- the dedup did not hold"

    # the rung-4 cell for the tripled player: 4 started+active weeks in 1 league / 4 elig
    # league-weeks = 100%, NOT 300%. started_active_weeks counts each league-week ONCE.
    row = con.execute("""SELECT start_rate_pct, started_active_weeks, elig_league_weeks
        FROM final WHERE NFL_player_id='p1'
          AND teams='12tm' AND roster='flx' AND ppr='ppr' AND td='4pt'""").fetchone()
    assert row is not None, "the tripled player must still reach the final table"
    assert row[0] == pytest.approx(100.0), f"start_rate {row[0]} should be 100, not tripled"
    assert row[1] == 4, f"started_active_weeks {row[1]} should be 4 (distinct grain), not 12"


def test_playoff_shapes_use_ground_truth(con):
    """The three shapes read the folded matchup table, not the standings fallback, when
    matchup rows exist. Manager A holds final_playoff_seed 2 <= playoff_teams 6 -> made it."""
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    n_rost, wkwt, fin, started = con.execute(
        "SELECT n_rost_lg, wkwt_credit, n_final_po, n_started_po FROM po0 "
        "WHERE teams='12tm' AND roster='flx' AND NFL_player_id='p1'").fetchone()
    assert n_rost == 1
    assert wkwt == pytest.approx(1.0)       # every rostered week on a playoff team
    assert fin == 1                          # rostered in week 14 (playoff_start_week-1)...
    assert started == 1                      # started week 15, a real playoff game


def test_later_round_only_start_counts_as_playoff_start(con):
    """A player does not need a first-round start to receive playoff-start credit."""
    con.execute(f"""INSERT INTO public.player_position VALUES
        ('p2', {YEAR}, 'WR', 'WR', ['WR'])""")
    # p2's only start is the later playoff round.  There is no championship flag;
    # the row-level playoff overlay is the evidence that this is a playoff game.
    con.execute(f"""INSERT INTO public.player_fantasy VALUES
        ('lg1', {YEAR}, 16, 'p2', 1, 1, 20.0, 1, 0, 0.5, 3.0, 'Manager A', 110.0,
         2, NULL, 1)""")
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    n_rost, _credit, n_final, n_started = con.execute(
        "SELECT n_rost_lg, wkwt_credit, n_final_po, n_started_po FROM po0 "
        "WHERE teams='12tm' AND roster='flx' AND NFL_player_id='p2'"
    ).fetchone()
    assert n_rost == 1
    assert n_final == 1
    assert n_started == 1


def test_actual_bracket_evidence_overrides_bad_or_missing_seed(con):
    """A real championship-bracket row proves qualification even if its seed is unusable."""
    con.execute("UPDATE public.matchup SET final_playoff_seed = 99 WHERE db_name = 'lg1'")
    con.execute("UPDATE public.player_fantasy SET final_playoff_seed = 99 WHERE db_name = 'lg1'")
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    n_rost, wkwt, fin, started = con.execute(
        "SELECT n_rost_lg, wkwt_credit, n_final_po, n_started_po FROM po0 "
        "WHERE teams='12tm' AND roster='flx' AND NFL_player_id='p1'"
    ).fetchone()
    assert n_rost == 1
    assert wkwt == pytest.approx(1.0)
    assert fin == 1
    assert started == 1


def test_standings_fallback_reconstructs_points_from_started_players(con, monkeypatch):
    """Old MFL/Fleaflicker rows may have no team_points or matchup rows."""
    con.execute("ALTER TABLE public.player_fantasy ADD COLUMN is_championship INTEGER")
    con.execute("UPDATE public.player_fantasy SET is_championship = 1 WHERE week = 16")
    monkeypatch.setattr(M, "_PLAYER_COLUMNS", set(M._PLAYER_COLUMNS) | {"is_championship"})
    con.execute("DELETE FROM public.matchup")
    con.execute("UPDATE public.player_fantasy SET team_points = NULL, final_playoff_seed = NULL, is_playoffs_bf = 0")
    con.execute(f"CREATE TABLE po0 AS {M.po_sql(YEAR)}")
    n_rost, wkwt, fin, started = con.execute(
        "SELECT n_rost_lg, wkwt_credit, n_final_po, n_started_po FROM po0 "
        "WHERE teams='12tm' AND roster='flx' AND NFL_player_id='p1'"
    ).fetchone()
    assert n_rost == 1
    assert wkwt == pytest.approx(1.0)
    assert fin == 1
    # The title-game row is a championship start, and a championship start is
    # necessarily a playoff start even without an explicit playoff-week flag.
    assert started == 1


def test_champion_flag_is_playoff_signal_for_closure(con):
    """A champion-only league must remain in the playoff lane.

    This is the exact closure edge case: no seed, backfill, or manager/standings signal is
    available, but a champion flag exists.  The champion is playoff evidence and must make
    both the denominator and ``n_final_po`` visible to the same lane.
    """
    con.execute("DELETE FROM public.matchup")
    con.execute("""UPDATE public.player_fantasy
                   SET manager = NULL, final_playoff_seed = NULL,
                       made_po_bf = 0, is_playoffs_bf = 0""")

    con.execute(f"CREATE TABLE d AS {M.DENOM_SQL}")
    po = con.execute(f"""SELECT n_rost_lg, n_final_po FROM ({M.po_sql(YEAR)})
                       WHERE teams='12tm' AND roster='flx'""").fetchone()
    assert po is not None
    assert po[1] >= 1, "champion-only evidence must close the playoff numerator"
    n_po = con.execute("""SELECT n_leagues_po FROM d
                        WHERE teams='12tm' AND roster='flx' AND ppr='ppr'
                          AND td='4pt' AND pos_grp='WR'""").fetchone()[0]
    assert n_po == 1, "champion-only evidence must qualify the playoff denominator"


def test_season_champion_marker_does_not_award_championship_start(con, monkeypatch):
    """A champion marker on a playoff row is not a title-game start."""
    con.execute("ALTER TABLE public.player_fantasy ADD COLUMN is_championship INTEGER")
    con.execute("UPDATE public.player_fantasy SET is_championship = 0 WHERE week = 16")
    monkeypatch.setattr(M, "_PLAYER_COLUMNS", set(M._PLAYER_COLUMNS) | {"is_championship"})
    con.execute("UPDATE public.player_fantasy SET champion = 1 WHERE NFL_player_id = 'p1' AND week = 15")
    for stmt in (f"CREATE TABLE p AS {M.player_sql(YEAR)}",
                 f"CREATE TABLE wp AS {M.weekly_player_sql(YEAR)}",
                 f"CREATE TABLE d AS {M.DENOM_SQL}",
                 f"CREATE TABLE dw AS {M.DENOM_WEEK_SQL}",
                 f"CREATE TABLE act AS {M.ACTIVE_SQL}",
                 f"CREATE TABLE sa AS {M.sa_sql(YEAR)}",
                 f"CREATE TABLE nst AS {M.nstart_sql(YEAR)}",
                 f"CREATE TABLE psv AS {M.PSV_SQL}",
                 f"CREATE TABLE po0 AS {M.po_sql(YEAR)}"):
        con.execute(stmt)
    for stmt in (M.CANON0_SQL, M.CANON_LATTICE_SQL, M.NSTL_SQL, M.PO_LATTICE_SQL,
                 M.PDENOM_SQL, M.WEEKLY_FINAL_SQL, M.FINAL_SQL):
        con.execute(stmt)
    champ = con.execute("""SELECT champ_as_starter_pct FROM final
        WHERE NFL_player_id='p1' AND teams='12tm' AND roster='flx'
          AND ppr='ppr' AND td='4pt'""").fetchone()[0]
    assert champ == pytest.approx(0.0)


def test_validation_query_reports_agreement(con):
    """po_val_sql must bind and produce the derivation-vs-truth counts the build prints."""
    row = con.execute(M.po_val_sql(YEAR)).fetchone()
    assert row is not None
    year, n_gt, n_agree = row
    assert year == YEAR
    assert n_gt >= 1 and n_agree <= n_gt


def test_weekly_statements_are_one_statement_each():
    """Each weekly rollup string must be exactly one CREATE TABLE.

    Regression for 2026-07-30: main() built this list by splitting one blob on ";", which is
    not a SQL parser. A semicolon inside a `--` comment cut a statement in half, the stage
    guard raised "expected 3 weekly rollup statements, got 4", and all 28 shards failed
    minutes into the run. The statements are now separate strings; this pins that they stay
    separate AND that no single one smuggles in a second statement.
    """
    assert len(M.WEEKLY_STATEMENTS) == 3
    labels = [label for label, _ in M.WEEKLY_STATEMENTS]
    assert labels == ["weekly base", "weekly final", "weekly season"]
    for label, sql in M.WEEKLY_STATEMENTS:
        # strip comments before counting -- a `;` inside a comment is legal and must not fail
        bare = "\n".join(line.split("--")[0] for line in sql.splitlines())
        assert bare.count(";") == 0, f"{label}: statement text must not contain ';'"
        assert bare.upper().count("CREATE TABLE") == 1, f"{label}: expected one CREATE TABLE"
