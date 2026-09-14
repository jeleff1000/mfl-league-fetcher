import duckdb
import pytest

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.player.sql_player_enrichments import PlayerEnrichmentsMixin


class _PlayerRunner(PlayerEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_expand_to_all_nfl_full_mode_respects_active_year_bounds():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            player VARCHAR,
            year INTEGER,
            week INTEGER,
            cumulative_week BIGINT,
            position VARCHAR,
            manager VARCHAR,
            is_started BOOLEAN
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            db_name VARCHAR,
            year INTEGER,
            first_active_year INTEGER,
            last_active_year INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            ('test_db', 2024, 2024, 2025),
            ('test_db', 2025, 2024, 2025)
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            player VARCHAR,
            year INTEGER,
            week INTEGER,
            nfl_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all VALUES
            ('qb_2023_1', 'qb_2023', 'Old QB', 2023, 1, 'QB'),
            ('qb_2024_1', 'qb_2024', 'League QB 2024', 2024, 1, 'QB'),
            ('qb_2025_1', 'qb_2025', 'League QB 2025', 2025, 1, 'QB'),
            ('qb_2025_1', 'qb_2025', 'League QB 2025 Duplicate', 2025, 1, 'QB')
        """
    )

    runner = _PlayerRunner(
        db_name="test_db",
        data_dir="local",
        roster_by_year={2024: {"QB": 1}, 2025: {"QB": 1}},
    )
    runner._conn = conn

    try:
        inserted = runner.expand_to_all_nfl()
        years = runner.conn.execute(
            """
            SELECT year
            FROM public.player_fantasy
            WHERE manager = 'Unrostered'
            ORDER BY year
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert inserted == 2
    assert years == [(2024,), (2025,)]


def test_dedup_player_fantasy_prefers_started_then_draft_owner():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            NFL_player_id VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            is_started INTEGER,
            fantasy_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            year INTEGER,
            NFL_player_id VARCHAR,
            franchise_id VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("test_db", 2021, 1, "p_started", "DraftOwner", "fid_draft", "1", 0, "BN"),
            ("test_db", 2021, 1, "p_started", "Starter", "fid_starter", "2", 1, "RB"),
            ("test_db", 2021, 1, "p_bench", "DraftOwner", "fid_draft", "1", 0, "BN"),
            ("test_db", 2021, 1, "p_bench", "OtherBench", "fid_other", "2", 0, "BN"),
            ("test_db", 2021, 1, "p_unrostered", "Unrostered", None, None, 0, "BN"),
        ],
    )
    conn.executemany(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?)",
        [
            ("test_db", 2021, "p_started", "fid_draft"),
            ("test_db", 2021, "p_bench", "fid_draft"),
        ],
    )

    runner = _PlayerRunner(db_name="test_db", data_dir="local")
    runner._conn = conn

    try:
        runner.dedup_player_fantasy()
        rows = runner.conn.execute(
            """
            SELECT NFL_player_id, manager, franchise_id
            FROM public.player_fantasy
            ORDER BY NFL_player_id, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("p_bench", "DraftOwner", "fid_draft"),
        ("p_started", "Starter", "fid_starter"),
        ("p_unrostered", "Unrostered", None),
    ]


def test_dedup_player_fantasy_falls_back_to_platform_player_id():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            NFL_player_id VARCHAR,
            yahoo_player_id VARCHAR,
            player VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            is_started INTEGER,
            fantasy_position VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("test_db", 2003, 1, None, "126", "Jerry Rice", "Nabeel", "hidden_nabeel", "79.l.1.t.12", None, 1, "WR"),
            (
                "test_db",
                2003,
                1,
                None,
                "126",
                "Jerry Rice",
                "Nabeel",
                "hidden_nabeel",
                "79.l.1.t.12",
                "Nabeel",
                1,
                "WR",
            ),
            ("test_db", 2003, 1, None, "177", "Tim Brown", "Ace", "hidden_ace", "79.l.1.t.4", "Ace", 0, "BN"),
        ],
    )

    runner = _PlayerRunner(db_name="test_db", data_dir="local")
    runner._conn = conn

    try:
        runner.dedup_player_fantasy()
        rows = runner.conn.execute(
            """
            SELECT yahoo_player_id, player, manager, team_name
            FROM public.player_fantasy
            ORDER BY yahoo_player_id
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("126", "Jerry Rice", "Nabeel", "Nabeel"),
        ("177", "Tim Brown", "Ace", "Ace"),
    ]


def test_dedup_player_fantasy_publish_identity_prefers_rostered_rows():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            is_started INTEGER,
            fantasy_position VARCHAR,
            fantasy_points DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("test_db", "p_rostered_2025_1", "p_rostered", 2025, 1, "Unrostered", None, None, 0, "BN", 9.0),
            ("test_db", "p_rostered_2025_1", "p_rostered", 2025, 1, "Starter", "fid_1", "team_1", 1, "RB", 9.0),
            ("test_db", "p_unrostered_2025_1", "p_unrostered", 2025, 1, "Unrostered", None, None, 0, "BN", 1.0),
            ("test_db", "p_unrostered_2025_1", "p_unrostered", 2025, 1, "Unrostered", None, None, 0, "BN", 1.0),
        ],
    )

    runner = _PlayerRunner(db_name="test_db", data_dir="local")
    runner._conn = conn

    try:
        removed = runner.dedup_player_fantasy_publish_identity()
        rows = runner.conn.execute(
            """
            SELECT player_week, manager, franchise_id
            FROM public.player_fantasy
            ORDER BY player_week, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert removed == 2
    assert rows == [
        ("p_rostered_2025_1", "Starter", "fid_1"),
        ("p_unrostered_2025_1", "Unrostered", None),
    ]


def test_populate_fantasy_points_applies_custom_offense_corrections():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            pts_pass_6pt DOUBLE,
            pts_rush DOUBLE,
            pts_rec_ppr DOUBLE,
            pts_misc DOUBLE,
            pts_pass_cmp DOUBLE,
            pts_def_std DOUBLE,
            pts_idp_std DOUBLE,
            pts_k_std DOUBLE,
            passing_yards DOUBLE,
            completions DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            nfl_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('test_db', '00-0020245_2010_12', '00-0020245', 2010, 12, 'QB', 'J', NULL, NULL, NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all VALUES
            ('00-0020245_2010_12', '00-0020245', 23.32, 4.4, 0.0, 0.0, 7.25, 0.0, 0.0, 0.0, 333.0, 29.0)
        """
    )

    runner = _PlayerRunner(
        db_name="test_db",
        data_dir="local",
        roster_by_year={
            2010: {
                "QB": 1,
                "scoring_settings": {
                    "rec": 1.0,
                    "pass_td": 6.0,
                    "pass_cmp": 0.5,
                    "pass_yd": 0.033333333333333,
                },
            }
        },
    )
    runner._conn = conn
    runner._platform = "sleeper"

    try:
        runner.populate_fantasy_points()
        row = runner.conn.execute(
            """
            SELECT fantasy_points, bonus_points, te_premium_points
            FROM public.player_fantasy
            """
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert row[0] == pytest.approx(40.0, abs=0.01)
    assert row[1] == pytest.approx(0.0, abs=0.01)
    assert row[2] == pytest.approx(0.0, abs=0.01)


def test_populate_fantasy_points_preserves_yahoo_rostered_api_points():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            pts_pass_4pt DOUBLE,
            pts_rush DOUBLE,
            pts_rec_half DOUBLE,
            pts_misc DOUBLE,
            pts_def_std DOUBLE,
            pts_idp_std DOUBLE,
            pts_k_std DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            nfl_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('test_db', 'qb_2021_1', 'qb_2021', 2021, 1, 'QB', 'Rostered Manager', 17.5, NULL, NULL),
            ('test_db', 'qb_2021_2', 'qb_2021', 2021, 2, 'QB', 'Unrostered', NULL, NULL, NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all VALUES
            ('qb_2021_1', 'qb_2021', 24.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0),
            ('qb_2021_2', 'qb_2021', 21.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        """
    )

    runner = _PlayerRunner(
        db_name="test_db",
        data_dir="local",
        roster_by_year={2021: {"QB": 1, "scoring_settings": {"rec": 0.0, "pass_td": 4.0}}},
    )
    runner._conn = conn
    runner._platform = "yahoo"

    try:
        runner.populate_fantasy_points()
        rows = runner.conn.execute(
            """
            SELECT week, manager, fantasy_points
            FROM public.player_fantasy
            ORDER BY week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        (1, "Rostered Manager", 17.5),
        (2, "Unrostered", 21.0),
    ]


def test_populate_fantasy_points_recomputes_yahoo_rostered_def():
    """Yahoo's per-player stats endpoint historically drops DEF special-teams
    TD scoring (pts_def_ret_td * scoring_def_st_td) for older seasons; this
    test pins the fix that always recomputes DEF rows from
    super_table x league_settings, regardless of platform-stored points.

    Mirrors mohoney 2013 wk2 Broncos: stored=9 (1 sack + 4 INT, missing
    1 KOR/PR TD x 6 = 6 pts), recompute should produce 15.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            pts_def_sack DOUBLE,
            pts_def_int DOUBLE,
            pts_def_ret_td DOUBLE,
            pts_def_fr DOUBLE,
            pts_def_td DOUBLE,
            def_tds DOUBLE,
            fum_ret_td DOUBLE,
            pts_def_safety DOUBLE,
            pts_def_block DOUBLE,
            pts_allow_14_20 DOUBLE,
            pts_allow_21_27 DOUBLE,
            pts_def_std DOUBLE,
            pts_idp_std DOUBLE,
            pts_k_std DOUBLE,
            pts_pass_4pt DOUBLE,
            pts_rush DOUBLE,
            pts_rec_half DOUBLE,
            pts_misc DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            nfl_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('DEF-29', 'DEF')
        """
    )
    # Stored fantasy_points=9 mimics the post-seed Yahoo state where
    # pts_def_std (1 sack + 4 INT*2 = 9) was stored but the league's
    # scoring_def_st_td=6 was never applied to pts_def_ret_td=1.
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('test_db', 'DEF-29_2013_2', 'DEF-29', 2013, 2, 'DEF', 'Ross', 9.0, NULL, NULL)
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all
            (player_week, NFL_player_id, pts_def_sack, pts_def_int,
             pts_def_ret_td, pts_def_fr, pts_def_td, def_tds, fum_ret_td,
             pts_def_safety, pts_def_block, pts_allow_14_20, pts_allow_21_27,
             pts_def_std, pts_idp_std, pts_k_std,
             pts_pass_4pt, pts_rush, pts_rec_half, pts_misc)
        VALUES ('DEF-29_2013_2', 'DEF-29', 1, 4, 1, 0, 0, 0, 0,
                0, 0, 0, 1, 9.0, 0, 0, 0, 0, 0, 0)
        """
    )

    # Mohoney 2013 multipliers: sack=1, int=2, ret_td=6, fr=2, td=6,
    # safety=2, block=2; PA bucket 21-27 = 0 pts.
    runner = _PlayerRunner(
        db_name="test_db",
        data_dir="local",
        roster_by_year={
            2013: {
                "DEF": 1,
                "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
                "def_multipliers": {
                    "pts_def_sack": 1.0,
                    "pts_def_int": 2.0,
                    "pts_def_fr": 2.0,
                    "pts_def_td": 6.0,
                    "pts_def_safety": 2.0,
                    "pts_def_block": 2.0,
                    "pts_def_ret_td": 6.0,
                    "pts_allow_14_20": 1.0,
                    "pts_allow_21_27": 0.0,
                },
            },
        },
    )
    runner._conn = conn
    runner._platform = "yahoo"

    try:
        runner.populate_fantasy_points()
        row = runner.conn.execute(
            "SELECT fantasy_points FROM public.player_fantasy WHERE NFL_player_id='DEF-29'"
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    # 1 sack*1 + 4 INT*2 + 1 ret_td*6 + 1 PA bucket = 1 + 8 + 6 + 0 = 15
    assert row[0] == pytest.approx(15.0)


def test_def_recompute_uses_canonical_pts_def_td_without_readding_fum_ret_td():
    """Pin canonical DEF touchdown scoring.

    The canonical pts_def_td component is produced upstream. Reconstructing
    fantasy_points_calculator.py:478 — the precomputed sum of defensive
    it as def_tds + fum_ret_td at league-import time double-counts
    PFR-retabulated team-DST rows where def_tds already includes fumble
    return touchdowns.

    """
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            manager VARCHAR,
            fantasy_points DOUBLE,
            bonus_points DOUBLE,
            te_premium_points DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            player_week VARCHAR,
            NFL_player_id VARCHAR,
            pts_def_sack DOUBLE,
            pts_def_int DOUBLE,
            pts_def_fr DOUBLE,
            pts_def_td DOUBLE,
            def_tds DOUBLE,
            fum_ret_td DOUBLE,
            pts_def_safety DOUBLE,
            pts_def_block DOUBLE,
            pts_def_st_td DOUBLE,
            pts_allow_1_6 DOUBLE,
            pts_allow_35_plus DOUBLE,
            pts_def_std DOUBLE,
            pts_idp_std DOUBLE,
            pts_k_std DOUBLE,
            pts_pass_4pt DOUBLE,
            pts_rush DOUBLE,
            pts_rec_half DOUBLE,
            pts_misc DOUBLE
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.player_bio (
            NFL_player_id VARCHAR,
            nfl_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.player_bio VALUES
            ('DEF-COWBOYS', 'DEF'),
            ('DEF-CHARGERS', 'DEF')
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('test_db', 'DEF-COWBOYS_2021_10', 'DEF-COWBOYS', 2021, 10, 'DEF', 'Matt', 27.0, NULL, NULL),
            ('test_db', 'DEF-CHARGERS_2016_9', 'DEF-CHARGERS', 2016, 9, 'DEF', 'Robert', 2.0, NULL, NULL)
        """
    )
    # Cowboys 2021 wk10: 2 sacks, 3 INT, 0 fum_rec, 0 def TDs, 1 ST TD, 3 pts allowed.
    # super_table CORRUPT: pts_def_td=1 (phantom, contaminated with special_teams_tds).
    # Expected after fix: 2*1 + 3*2 + 0*2 + (0+0)*6 + 0*2 + 0*2 + 1*6 + 1*7 + 0 = 21
    # (Yahoo records 23 — the residual +2 is scoring_def_pass_def which mohoney has NULL,
    # so 21 is the correct mohoney-config-only score; what matters is we no longer
    # over-count by +6 from the phantom pts_def_td.)
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all
            (player_week, NFL_player_id, pts_def_sack, pts_def_int, pts_def_fr,
             pts_def_td, def_tds, fum_ret_td,
             pts_def_safety, pts_def_block, pts_def_st_td,
             pts_allow_1_6, pts_allow_35_plus,
             pts_def_std, pts_idp_std, pts_k_std,
             pts_pass_4pt, pts_rush, pts_rec_half, pts_misc)
        VALUES
            ('DEF-COWBOYS_2021_10', 'DEF-COWBOYS', 2, 3, 0,
             0, 0, 0, 0, 0, 1, 1, 0, 16.0, 0, 0, 0, 0, 0, 0)
        """
    )
    # Chargers 2016 wk9: 0 sacks, 2 INT, 1 fum_rec, 1 INT-return TD, 1 fumble-return TD,
    # 35 pts allowed. super_table CORRUPT: pts_def_td=0 (formula failed to add the
    # def_tds=1 + fum_ret_td=1).
    # Expected after fix: 0*1 + 2*2 + 1*2 + (1+1)*6 + 0*2 + 0*2 + 0*6 + 0 + 1*-4 = 14
    conn.execute(
        """
        INSERT INTO ___ops.nfl_historical.nfl_player_stats_all
            (player_week, NFL_player_id, pts_def_sack, pts_def_int, pts_def_fr,
             pts_def_td, def_tds, fum_ret_td,
             pts_def_safety, pts_def_block, pts_def_st_td,
             pts_allow_1_6, pts_allow_35_plus,
             pts_def_std, pts_idp_std, pts_k_std,
             pts_pass_4pt, pts_rush, pts_rec_half, pts_misc)
        VALUES
            ('DEF-CHARGERS_2016_9', 'DEF-CHARGERS', 0, 2, 1,
             2, 2, 1, 0, 0, 0, 0, 1, 14.0, 0, 0, 0, 0, 0, 0)
        """
    )

    # Mohoney's actual scoring config (same multipliers both years).
    mohoney_def_mults = {
        "pts_def_sack": 1.0,
        "pts_def_int": 2.0,
        "pts_def_fr": 2.0,
        "pts_def_td": 6.0,
        "pts_def_safety": 2.0,
        "pts_def_block": 2.0,
        "pts_def_st_td": 6.0,
        "pts_allow_1_6": 7.0,
        "pts_allow_35_plus": -4.0,
    }
    runner = _PlayerRunner(
        db_name="test_db",
        data_dir="local",
        roster_by_year={
            2016: {
                "DEF": 1,
                "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
                "def_multipliers": mohoney_def_mults,
            },
            2021: {
                "DEF": 1,
                "scoring_settings": {"rec": 1.0, "pass_td": 4.0},
                "def_multipliers": mohoney_def_mults,
            },
        },
    )
    runner._conn = conn
    runner._platform = "yahoo"

    try:
        runner.populate_fantasy_points()
        rows = dict(runner.conn.execute("SELECT NFL_player_id, fantasy_points FROM public.player_fantasy").fetchall())
    finally:
        if runner._conn is not None:
            runner._conn.close()

    # Cowboys: 2 + 6 + 0 + (0+0)*6 + 0 + 0 + 1*6 + 7 + 0 = 21
    # (Old code with pts_def_td=1: 2 + 6 + 0 + 1*6 + 0 + 0 + 1*6 + 7 + 0 = 27 — over by 6)
    assert rows["DEF-COWBOYS"] == pytest.approx(21.0)
    # Chargers: 0 + 4 + 2 + (1+1)*6 + 0 + 0 + 0 + 0 + (-4) = 14
    # (Old code with pts_def_td=0: 0 + 4 + 2 + 0*6 + 0 + 0 + 0 + 0 + (-4) = 2 — under by 12)
    assert rows["DEF-CHARGERS"] == pytest.approx(14.0)


def test_ensure_player_week_regenerates_stale_yahoo_prefix():
    """Regression for KMFFL 2025 Bo Nix: when staging merger's
    _backfill_nfl_player_ids set player_week to 'YAHOO-{yahoo_id}_{year}_{week}'
    (its fallback for rows it couldn't resolve at that moment) and
    resolve_all_nfl_player_ids later succeeded via player_bio,
    ensure_player_week left the stale YAHOO- prefix in place because it only
    updated NULL/empty player_weeks. super_table joins then missed on the
    wrong key, fantasy_points stayed 0 for all 18 weeks, and
    fix_zero_point_starters un-starred Bo Nix → BN with NULL manager_lamar.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            yahoo_player_id VARCHAR,
            "NFL_player_id" VARCHAR,
            player_week VARCHAR
        )
        """
    )
    # Rows that exercise the regeneration logic:
    #  (a) stale YAHOO- prefix (Bo Nix scenario) — must be regenerated
    #  (b) NULL player_week                     — must be populated
    #  (c) already-correct canonical player_week — must be left alone (no-op)
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('test_db', 2025, 1, '40875',  '00-0039732', 'YAHOO-40875_2025_1'),
            ('test_db', 2025, 2, '40875',  '00-0039732',  NULL),
            ('test_db', 2025, 3, '40875',  '00-0039732', '00-0039732_2025_3'),
            -- still-unresolved row (no NFL_player_id) — leave its YAHOO- alone
            ('test_db', 2025, 4, '99999',   NULL,         'YAHOO-99999_2025_4'),
            -- still-unresolved row with no player_week; create stable publish identity
            ('test_db', 2025, 5, '88888',   NULL,          NULL),
            ('test_db', 2025, NULL, '77777', NULL,         NULL)
        """
    )

    runner = _PlayerRunner(db_name="test_db", data_dir="local")
    runner._conn = conn

    try:
        runner.ensure_player_week()
        rows = runner.conn.execute(
            "SELECT week, player_week FROM public.player_fantasy ORDER BY COALESCE(week, 99)"
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows[:4] == [
        (1, "00-0039732_2025_1"),  # stale YAHOO- regenerated to canonical
        (2, "00-0039732_2025_2"),  # NULL filled
        (3, "00-0039732_2025_3"),  # already correct, no-op
        (4, "YAHOO-99999_2025_4"),  # still unresolved (no NFL_player_id) — left alone
    ]
    assert rows[4][0] == 5
    assert rows[4][1].startswith("UNMAPPED_")
    assert rows[4][1].endswith("_2025_5")
    assert rows[5][0] is None
    assert rows[5][1].startswith("UNMAPPED_")
    assert rows[5][1].endswith("_2025_UNKNOWN_WEEK")


def test_apply_super_table_skill_positions_overrides_fb_to_rb_from_super_table():
    """Sleeper rostered rows leak Sleeper API depth-chart position 'FB' into
    player_fantasy.position even though super_table classifies the player as 'RB'.
    Concrete fleet repro: Patrick Ricard (NFL_player_id 00-0033376) shows
    position='FB' on the rostered/started row and 'RB' on every unrostered row.

    super_table is the authoritative source: across the fleet all 6 distinct
    FB-rostered players (Ricard, Juszczyk, Luepke, Ham, Blasingame, Ingold)
    are 'RB' in super_table. This enrichment must overwrite FB->RB whenever
    super_table agrees, while leaving unrelated rows (existing RB, IDP labels
    like LB) untouched.
    """
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("ATTACH ':memory:' AS ___ops")
    conn.execute("CREATE SCHEMA IF NOT EXISTS ___ops.nfl_historical")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            NFL_player_id VARCHAR,
            manager VARCHAR,
            position VARCHAR,
            is_started INTEGER,
            fantasy_position VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE ___ops.nfl_historical.nfl_player_stats_all (
            NFL_player_id VARCHAR,
            year INTEGER,
            week INTEGER,
            position VARCHAR,
            player VARCHAR
        )
        """
    )
    # Ricard: rostered+started FB row (the bug) + unrostered FB row (also wrong)
    # Already-correct RB row: must not be touched.
    # IDP LB row: NOT in the FB->RB whitelist — must not be touched even if
    # super_table disagrees, per "don't blanket-normalize IDP" principle.
    # FB row with no super_table match: leave as-is (defensive).
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        [
            ("test_db", 2024, 12, "00-0033376", "MagicMawlz", "FB", 1, "RB"),
            ("test_db", 2024, 11, "00-0033376", "Unrostered", "FB", 0, "BN"),
            ("test_db", 2024, 12, "00-0036900", "MagicMawlz", "RB", 1, "RB"),
            ("test_db", 2024, 12, "00-0034950", "MagicMawlz", "LB", 1, "LB"),
            ("test_db", 2024, 12, "00-9999999", "Unrostered", "FB", 0, "BN"),
        ],
    )
    # super_table: Ricard is RB at both his weeks, the LB player is LB
    # (matches reality — IDP labels are intentionally raw in super_table),
    # and the orphan FB has no super_table row at all.
    conn.executemany(
        "INSERT INTO ___ops.nfl_historical.nfl_player_stats_all VALUES (?, ?, ?, ?, ?)",
        [
            ("00-0033376", 2024, 12, "RB", "Patrick Ricard"),
            ("00-0033376", 2024, 11, "RB", "Patrick Ricard"),
            ("00-0036900", 2024, 12, "RB", "Some Other RB"),
            ("00-0034950", 2024, 12, "LB", "Some LB"),
        ],
    )

    runner = _PlayerRunner(db_name="test_db", data_dir="local")
    runner._conn = conn

    try:
        runner.apply_super_table_skill_positions()
        rows = runner.conn.execute(
            """
            SELECT week, NFL_player_id, manager, position
            FROM public.player_fantasy
            ORDER BY NFL_player_id, week, manager
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        (11, "00-0033376", "Unrostered", "RB"),  # FB -> RB (super_table agrees)
        (12, "00-0033376", "MagicMawlz", "RB"),  # FB -> RB (the Ricard bug)
        (12, "00-0034950", "MagicMawlz", "LB"),  # IDP LB untouched
        (12, "00-0036900", "MagicMawlz", "RB"),  # already RB, no-op
        (12, "00-9999999", "Unrostered", "FB"),  # no super_table match, untouched
    ]
