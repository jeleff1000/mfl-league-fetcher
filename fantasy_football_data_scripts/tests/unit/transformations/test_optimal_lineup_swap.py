"""Unit tests for the manager_optimal swap pass — specifically dual-eligibility
patterns where the player's NFL position and the lineup slot the manager
actually played them in disagree, AND fixing the assignment requires a
cascade through the FLEX slot.

Concrete fleet bug this catches: the_tfl 2021 W2 manager Elchapo1040.
Cordarrelle Patterson has position='RB' but fantasy_position='WR' (manager
started him at WR). Greedy fills RB before WR (scarcity order), so Patterson
gets consumed for RB1 — leaving WR3 to backfill with a 0-pt bench player.
The stored optimal totals 111.52 < team_points 115.22, breaking the
optimal_gte_team_points invariant. The 3.7 miss is exactly George Kittle's
score, who never makes it into FLEX because Mitchell (RB) was forced there.
"""

import sys
from pathlib import Path

import duckdb
import pytest

SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)

from multi_league.transformations.aggregation.sql_aggregation_enrichments import AggregationEnrichmentsMixin
from multi_league.transformations.aggregation.modules.optimal_lineup import RosterHelpers, position_rank
from multi_league.transformations.common.sql_base import SQLEnrichmentsBase


class _AggregationRunner(AggregationEnrichmentsMixin, SQLEnrichmentsBase):
    pass


@pytest.mark.parametrize("ppr,td_key,season_ppg,career_ppg", [
    (0.5, "4pt", 12.25, 11.5),
    (1.0, "6pt", 15.75, 13.25),
    (0.0, "4pt", 0.0, 0.0),
])
def test_position_alltime_rank_uses_ops_history_not_active_year_subset(ppr, td_key, season_ppg, career_ppg):
    """A quick refresh must never call a single current-week player #1 ever.

    The historical rank is precomputed in the OPS super table.  The local
    refresh database deliberately contains only the active partition, so
    recomputing an all-time window there turns every lone player-week into
    rank 1.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA ___ops.nfl_historical")
    conn.execute(
        "CREATE TABLE ___ops.nfl_historical.player_bio "
        "(NFL_player_id VARCHAR, nfl_position VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all "
        "(player_week VARCHAR, position VARCHAR, rank_qb_4pt INTEGER, rank_alltime_qb_4pt INTEGER)"
    )
    conn.execute("INSERT INTO ___ops.nfl_historical.player_bio VALUES ('caleb', 'QB')")
    conn.execute(
        "INSERT INTO ___ops.nfl_historical.nfl_player_stats_all VALUES ('2026_01_caleb', 'QB', 1, 222)"
    )
    for suffix, season, career in (("4pt_half", 12.25, 11.5), ("6pt_ppr", 15.75, 13.25), ("4pt_0ppr", 0.0, 0.0)):
        for metric, value in (("season", season), ("alltime", career)):
            conn.execute(f"ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all ADD COLUMN ppg_{metric}_{suffix} DOUBLE")
            conn.execute(f"UPDATE ___ops.nfl_historical.nfl_player_stats_all SET ppg_{metric}_{suffix} = ?", [value])
    conn.execute(
        """
        CREATE TABLE player_fantasy (
            db_name VARCHAR, player_week VARCHAR, NFL_player_id VARCHAR,
            year INTEGER, week INTEGER, position VARCHAR, fantasy_points DOUBLE,
            position_rank INTEGER, position_week_rank INTEGER,
            position_season_rank INTEGER, position_alltime_rank INTEGER,
            flex_week_rank INTEGER, flex_season_rank INTEGER, flex_alltime_rank INTEGER,
            sflex_week_rank INTEGER, sflex_season_rank INTEGER, sflex_alltime_rank INTEGER,
            season_ppg DOUBLE, alltime_ppg DOUBLE
        )
        """
    )
    conn.execute(
        "INSERT INTO player_fantasy VALUES "
        "('quick_scope', '2026_01_caleb', 'caleb', 2026, 1, 'QB', 18.76, "
        "NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)"
    )
    conn.execute("INSERT INTO ___ops.nfl_historical.player_bio VALUES ('punter', 'P')")
    conn.execute("INSERT INTO ___ops.nfl_historical.nfl_player_stats_all (player_week,position) VALUES ('2026_01_punter','P')")
    conn.execute("INSERT INTO player_fantasy (db_name,player_week,NFL_player_id,year,week,position,fantasy_points,position_alltime_rank) VALUES ('quick_scope','2026_01_punter','punter',2026,1,'P',0,7)")
    helpers = RosterHelpers(
        available_settings_years=lambda: [2026],
        resolve_settings_year=lambda year: year,
        get_dedicated_slots=lambda _settings: {},
        identify_flex_positions=lambda _settings: {},
        position_eligibility_sql=lambda column, position: f"UPPER({column}) = '{position}'",
        flex_eligibility_sql=lambda _column, _positions: "FALSE",
        preferred_flex_rank_column=lambda *_args: None,
        front7_eligibility_sql=lambda _column: "FALSE",
        primary_position_sql=lambda column: f"UPPER({column})",
        group_years_by_scoring=lambda years, _settings: [(
            {"rank_cols": {"QB": "rank_qb_4pt"}, "ppr": ppr, "td_key": td_key},
            years,
        )],
    )

    position_rank(
        conn,
        "player_fantasy",
        {2026: {}},
        helpers,
        db_name="quick_scope",
    )

    assert conn.execute(
        "SELECT position_alltime_rank FROM player_fantasy WHERE player_week = '2026_01_caleb'"
    ).fetchone()[0] == 222
    assert conn.execute(
        "SELECT season_ppg, alltime_ppg FROM player_fantasy WHERE player_week = '2026_01_caleb'"
    ).fetchone() == (season_ppg, career_ppg)
    assert conn.execute("SELECT position_alltime_rank FROM player_fantasy WHERE NFL_player_id='punter'").fetchone()[0] is None

    # A missing historical source must fail before erasing the previous output,
    # not substitute the single hydrated week's 18.76 points for a career mean.
    conn.execute("ALTER TABLE ___ops.nfl_historical.nfl_player_stats_all DROP COLUMN ppg_alltime_" + td_key + "_" + {0.0: "0ppr", 0.5: "half", 1.0: "ppr"}[ppr])
    with pytest.raises(RuntimeError, match="PPG source columns"):
        position_rank(conn, "player_fantasy", {2026: {}}, helpers, db_name="quick_scope")
    assert conn.execute("SELECT alltime_ppg FROM player_fantasy WHERE NFL_player_id='caleb'").fetchone()[0] == career_ppg


def _create_player_fantasy(conn) -> None:
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            position VARCHAR,
            fantasy_position VARCHAR,
            fantasy_points DOUBLE,
            position_rank INTEGER,
            flex_week_rank INTEGER,
            sflex_week_rank INTEGER,
            optimal_player INTEGER,
            optimal_position VARCHAR
        )
        """
    )


def test_manager_optimal_handles_fantasy_position_mismatch_with_flex_cascade(tmp_path):
    """When a player's NFL position differs from the slot the manager played
    them in (Patterson: NFL=RB, fantasy_position=WR), and resolving the
    mismatch requires cascading through FLEX (Patterson->WR forces an RB
    out of FLEX so a bench TE can take FLEX), manager_optimal must still
    find the lineup that maximizes points.

    Minimal setup mirroring the Elchapo1040 2021 W2 case:
        Slots: QB=1, RB=2, WR=3, FLX=1 (7 starters)

        Started by manager (matches team_points):
          QB1   QB,    QB,  25.0
          HighRB RB,   WR,  21.0  -- the dual-eligible player
          RB1    RB,   RB,   8.0
          RB2    RB,   RB,   6.0
          WR1    WR,   WR,  16.0
          WR2    WR,   WR,   1.0
          BenchTE TE,  FLX,  4.0  -- TE played in FLX

        Bench (rostered but not started):
          BenchWR0 WR, BN,   0.0

    team_points = 25+21+8+6+16+1+4 = 81.0

    Greedy (current bug) yields optimal=77.0:
        QB1, HighRB->RB1, RB1->RB2, WR1, WR2, BenchWR0->WR3, RB2->FLX
        25 + 21 + 8 + 16 + 1 + 0 + 6 = 77.0
        BenchTE (4) and the other 0-pt benches stay benched.

    With swap+cascade (the fix), optimal must reach 81.0:
        QB1, RB1+RB2 at RB, HighRB+WR1+WR2 at WR, BenchTE at FLX
        25 + 8 + 6 + 21 + 16 + 1 + 4 = 81.0
    """
    db_name = "optimal_swap_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    _create_player_fantasy(conn)

    # (db_name, player_week, year, week, manager, franchise_id,
    #  position, fantasy_position, fantasy_points,
    #  position_rank, flex_week_rank, sflex_week_rank, optimal_player, optimal_position)
    rows = [
        (db_name, "qb1", 2021, 2, "Elchapo", "fid_e", "QB", "QB", 25.0, 1, 1, 1, 0, None),
        # The dual-eligible player: NFL=RB but manager started at WR
        (db_name, "high_rb", 2021, 2, "Elchapo", "fid_e", "RB", "WR", 21.0, 1, 2, 2, 0, None),
        (db_name, "rb1", 2021, 2, "Elchapo", "fid_e", "RB", "RB", 8.0, 2, 3, 3, 0, None),
        (db_name, "rb2", 2021, 2, "Elchapo", "fid_e", "RB", "RB", 6.0, 3, 4, 4, 0, None),
        (db_name, "wr1", 2021, 2, "Elchapo", "fid_e", "WR", "WR", 16.0, 1, 5, 5, 0, None),
        (db_name, "wr2", 2021, 2, "Elchapo", "fid_e", "WR", "WR", 1.0, 2, 6, 6, 0, None),
        # TE that the manager played in FLX (this is the slot that gets vacated by cascade)
        (db_name, "bench_te", 2021, 2, "Elchapo", "fid_e", "TE", "FLX", 4.0, 1, 7, 7, 0, None),
        # Bench WR with 0 — what greedy currently picks for WR3
        (db_name, "bench_wr0", 2021, 2, "Elchapo", "fid_e", "WR", "BN", 0.0, 3, 8, 8, 0, None),
    ]
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()

    runner = _AggregationRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.compute_manager_optimal({2021: {"QB": 1, "RB": 2, "WR": 3, "FLX": 1}})
        optimal_total = runner.conn.execute(
            """
            SELECT ROUND(SUM(fantasy_points), 2)
            FROM public.player_fantasy
            WHERE optimal_player = 1
            """
        ).fetchone()[0]
        selected = runner.conn.execute(
            """
            SELECT player_week, optimal_position
            FROM public.player_fantasy
            WHERE optimal_player = 1
            ORDER BY fantasy_points DESC, player_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    # Optimal MUST equal team_points (81.0) — anything less means greedy left
    # points on the bench, breaking the optimal_gte_team_points invariant.
    assert optimal_total == 81.0, (
        f"Expected optimal=81.0 (matches team_points), got {optimal_total}. " f"Selected: {selected}"
    )

    # Sanity: bench_wr0 must not be in optimal (it would replace high_rb at WR);
    # bench_te must be in optimal (FLX slot via cascade).
    selected_pws = {pw for pw, _ in selected}
    assert "bench_wr0" not in selected_pws, "bench_wr0 (0 pts) should not be optimal"
    assert "bench_te" in selected_pws, "bench_te (4 pts) should be in FLX via cascade"


def test_manager_optimal_fills_idp_flex_before_offensive_flex_for_dual_slot_player(tmp_path):
    """A WR started in an IDP slot must not get consumed by offensive FLX first.

    Sleeper can expose a player as playable in an IDP lineup slot even when the
    NFL position we store is still WR. The optimizer combines NFL position and
    the observed fantasy_position to model that eligibility, but it still has to
    fill the defensive flex slot before generic offensive FLX slots. Otherwise
    the player gets picked as FLX, IDP stays empty, and optimal_points can fall
    below the actual starter total.
    """
    db_name = "optimal_idp_flex_order_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    _create_player_fantasy(conn)

    rows = [
        (db_name, "drake_maye", 2025, 3, "deebtp", "fid_d", "QB", "QB", 34.9, 1, 1, 1, 0, None),
        (db_name, "quinshon_judkins", 2025, 3, "deebtp", "fid_d", "RB", "RB", 18.5, 1, 1, 1, 0, None),
        (db_name, "treveyon_henderson", 2025, 3, "deebtp", "fid_d", "RB", "RB", 8.7, 2, 4, 4, 0, None),
        (db_name, "bhayshul_tuten", 2025, 3, "deebtp", "fid_d", "RB", "BN", 8.6, 3, 5, 5, 0, None),
        (db_name, "tucker_kraft", 2025, 3, "deebtp", "fid_d", "TE", "TE", 6.4, 1, 7, 7, 0, None),
        (db_name, "drake_london", 2025, 3, "deebtp", "fid_d", "WR", "WR", 12.0, 1, 2, 2, 0, None),
        (db_name, "ladd_mcconkey", 2025, 3, "deebtp", "fid_d", "WR", "WR", 9.6, 2, 3, 3, 0, None),
        (db_name, "marquise_brown", 2025, 3, "deebtp", "fid_d", "WR", "FLX", 9.2, 3, 6, 6, 0, None),
        (db_name, "travis_hunter", 2025, 3, "deebtp", "fid_d", "WR", "IDP", 8.6, 4, 8, 8, 0, None),
        (db_name, "brian_thomas", 2025, 3, "deebtp", "fid_d", "WR", "WR", 8.5, 5, 9, 9, 0, None),
        (db_name, "stefon_diggs", 2025, 3, "deebtp", "fid_d", "WR", "FLX", 6.3, 6, 10, 10, 0, None),
    ]
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()

    runner = _AggregationRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.compute_manager_optimal({2025: {"QB": 1, "RB": 2, "WR": 3, "TE": 1, "FLX": 2, "IDP": 1}})
        selected = runner.conn.execute(
            """
            SELECT player_week, optimal_position
            FROM public.player_fantasy
            WHERE optimal_player = 1
            ORDER BY fantasy_points DESC, player_week
            """
        ).fetchall()
        optimal_total = runner.conn.execute(
            """
            SELECT ROUND(SUM(fantasy_points), 2)
            FROM public.player_fantasy
            WHERE optimal_player = 1
            """
        ).fetchone()[0]
    finally:
        if runner._conn is not None:
            runner._conn.close()

    pw_to_slot = {pw: slot for pw, slot in selected}
    assert len(selected) == 10
    assert pw_to_slot["travis_hunter"] == "IDP"
    assert "brian_thomas" in pw_to_slot
    assert "stefon_diggs" not in pw_to_slot
    assert optimal_total == 125.0


def test_manager_optimal_backstop_includes_punter_and_head_coach(tmp_path):
    """When a league has roster slots that aren't represented in the standard
    flat `league_settings` DDL (Punter `P`, Head Coach `HC`, custom Yahoo IDP
    variants), `roster_settings` for that league won't include them. The
    dedicated/flex slot-fill passes never enumerate those slots, so starters
    in P/HC end up with optimal_player=0 — making `matchup.team_points` (sum
    of started fantasy_points) > `matchup.optimal_points`, which is
    impossible by definition.

    Regression for `njfl` (ESPN) where every team starts a Punter (P) and a
    Head Coach (HC) every week — 125 P phantom starters + 30 HC phantom
    starters across 4 years.

    The backstop in `_set_manager_optimal_via_temp_tables` must mark any
    started player whose `fantasy_position` slot the standard pass didn't
    enumerate as already-optimal at that slot, so optimal_points >=
    team_points holds.
    """
    db_name = "optimal_backstop_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    _create_player_fantasy(conn)

    # Manager started one player at QB (8.0) plus a Punter (5.5) at slot 'P'
    # and a Head Coach (6.0) at slot 'HC'. The roster_settings dict for this
    # year only knows about QB. Without the backstop, optimal_points = 8.0
    # (just the QB). With it, optimal must include the P and HC starters
    # because the standard pass never tried to fill those slots.
    rows = [
        (db_name, "qb1", 2024, 1, "Tim", "fid_t", "QB", "QB", 8.0, 1, 1, 1, 0, None),
        (db_name, "punter1", 2024, 1, "Tim", "fid_t", "P", "P", 5.5, None, None, None, 0, None),
        (db_name, "hc1", 2024, 1, "Tim", "fid_t", "HC", "HC", 6.0, None, None, None, 0, None),
        (db_name, "bench_qb", 2024, 1, "Tim", "fid_t", "QB", "BN", 4.0, None, None, None, 0, None),
    ]
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    conn.close()

    runner = _AggregationRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.compute_manager_optimal({2024: {"QB": 1}})
        optimal_total = runner.conn.execute(
            """
            SELECT ROUND(SUM(fantasy_points), 2)
            FROM public.player_fantasy
            WHERE optimal_player = 1
            """
        ).fetchone()[0]
        selected = runner.conn.execute(
            """
            SELECT player_week, optimal_position
            FROM public.player_fantasy
            WHERE optimal_player = 1
            ORDER BY player_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    selected_pws = {pw for pw, _ in selected}
    assert "punter1" in selected_pws, "P starter must be in optimal (backstop)"
    assert "hc1" in selected_pws, "HC starter must be in optimal (backstop)"
    assert "qb1" in selected_pws, "QB starter must be in optimal (standard pass)"
    assert "bench_qb" not in selected_pws, "Bench player at BN should not be in optimal"
    # Total: 8.0 (QB) + 5.5 (P) + 6.0 (HC) = 19.5
    assert optimal_total == 19.5, f"Expected optimal=19.5 (QB+P+HC), got {optimal_total}. Selected: {selected}"
    # Verify the slot_group label captured the actual fantasy_position
    pw_to_slot = {pw: slot for pw, slot in selected}
    assert pw_to_slot["punter1"] == "P"
    assert pw_to_slot["hc1"] == "HC"
