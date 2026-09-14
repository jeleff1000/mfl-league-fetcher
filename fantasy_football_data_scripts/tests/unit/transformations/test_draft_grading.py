import duckdb
import pytest

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.transformations.draft.sql_draft_enrichments import DraftEnrichmentsMixin


class _DraftRunner(DraftEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_corpus_draft_baseline_loads_local_parquet_without_fly(tmp_path, monkeypatch):
    db_name = "smpl_offline_baseline"
    db_path = tmp_path / f"{db_name}.duckdb"
    source_path = tmp_path / "draft_global_source.parquet"

    source = duckdb.connect()
    source.execute(
        """
        COPY (
            SELECT ('smpl_source_' || CAST(i AS VARCHAR))::VARCHAR AS db_name, 2024::INTEGER AS year,
                   'redraft'::VARCHAR AS draft_market, 'snake'::VARCHAR AS draft_kind,
                   'draft'::VARCHAR AS cohort, 'RB'::VARCHAR AS position,
                   25.0::DOUBLE AS manager_lamar, 12.0::DOUBLE AS teams,
                   16.0::DOUBLE AS draft_rounds, 1.0::DOUBLE AS scoring_rec,
                   4.0::DOUBLE AS pass_td_pts, 0::INTEGER AS superflex,
                   0::INTEGER AS idp, 5.0::DOUBLE AS bench_count,
                   2.0::DOUBLE AS flex_count, 16.0::DOUBLE AS total_roster_slots,
                   0::INTEGER AS dynasty_like, 0::INTEGER AS keeper_like,
                   0::INTEGER AS uses_median, 's_01'::VARCHAR AS capital_bucket
            FROM range(3) AS source_rows(i)
        ) TO ? (FORMAT PARQUET)
        """,
        [str(source_path)],
    )
    source.close()

    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE _draft_score_rows AS
        SELECT 2024::INTEGER AS year, 'redraft'::VARCHAR AS draft_market,
               'snake'::VARCHAR AS draft_kind, 'draft'::VARCHAR AS cohort,
               12.0::DOUBLE AS teams, 16.0::DOUBLE AS draft_rounds,
               16.0::DOUBLE AS total_roster_slots, 5.0::DOUBLE AS bench_count,
               2.0::DOUBLE AS flex_count, 0::INTEGER AS superflex,
               0::INTEGER AS idp, 1.0::DOUBLE AS scoring_rec,
               4.0::DOUBLE AS pass_td_pts, 0::INTEGER AS dynasty_like,
               0::INTEGER AS keeper_like, 0::INTEGER AS uses_median
        """
    )
    conn.close()

    monkeypatch.setenv("CORPUS_MODE", "1")
    monkeypatch.setenv("DRAFT_GLOBAL_SOURCE_PATH", str(source_path))
    monkeypatch.delenv("DATABASE_SERVER_URL", raising=False)
    monkeypatch.delenv("DATABASE_READ_TOKEN", raising=False)

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner._fetch_global_draft_score_baseline() == 1
    finally:
        runner._conn.close()


def test_corpus_draft_baseline_fails_closed_when_local_source_missing(tmp_path, monkeypatch):
    db_name = "smpl_missing_baseline"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute(
        """
        CREATE TABLE _draft_score_rows AS
        SELECT 2024::INTEGER AS year, 'redraft'::VARCHAR AS draft_market,
               'snake'::VARCHAR AS draft_kind, 'draft'::VARCHAR AS cohort,
               12.0::DOUBLE AS teams, 16.0::DOUBLE AS draft_rounds,
               16.0::DOUBLE AS total_roster_slots, 5.0::DOUBLE AS bench_count,
               2.0::DOUBLE AS flex_count, 0::INTEGER AS superflex,
               0::INTEGER AS idp, 1.0::DOUBLE AS scoring_rec,
               4.0::DOUBLE AS pass_td_pts, 0::INTEGER AS dynasty_like,
               0::INTEGER AS keeper_like, 0::INTEGER AS uses_median
        """
    )
    conn.close()

    monkeypatch.setenv("CORPUS_MODE", "1")
    monkeypatch.setenv("DRAFT_GLOBAL_SOURCE_PATH", str(tmp_path / "missing.parquet"))

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        with pytest.raises(RuntimeError, match="DRAFT_GLOBAL_SOURCE_PATH"):
            runner._fetch_global_draft_score_baseline()
    finally:
        runner._conn.close()


def test_pick_score_normalizes_draft_value_zscore(tmp_path):
    db_name = "draft_score_capital_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            round INTEGER,
            pick INTEGER,
            cost DOUBLE,
            draft_type VARCHAR,
            manager_lamar DOUBLE,
            expected_lamar DOUBLE,
            draft_value_zscore DOUBLE,
            pick_score DOUBLE,
            is_keeper INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (2019, 5, 50, 4.0, "auction", 173.4, 78.3, 1.068, None, 0),
            (2019, 1, 1, 70.0, "auction", 120.0, 80.0, 1.068, None, 0),
            (2019, 12, 120, 1.0, "auction", -10.0, 15.0, -1.25, None, 0),
            (2019, 2, 12, 55.0, "auction", 35.0, 90.0, -1.25, None, 0),
        ],
    )
    conn.close()

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner._compute_pick_score()
        rows = runner.conn.execute(
            """
            SELECT cost, manager_lamar, expected_lamar, draft_value_zscore, pick_score
            FROM public.draft
            ORDER BY cost
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    for _cost, _manager_lamar, _expected_lamar, zscore, score in rows:
        assert score == round(100 + 15 * zscore, 3)


def test_blended_score_keeps_rookie_market_separate(tmp_path, monkeypatch):
    monkeypatch.setenv("DRAFT_SCORE_GLOBAL_BASELINE", "0")
    db_name = "draft_rookie_market_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            round INTEGER,
            pick INTEGER,
            cost DOUBLE,
            draft_type VARCHAR,
            draft_category VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            position VARCHAR,
            manager_lamar DOUBLE,
            expected_lamar DOUBLE,
            draft_value_zscore DOUBLE,
            pick_quality_zscore DOUBLE,
            pick_score DOUBLE,
            draft_grade VARCHAR,
            manager_draft_score DOUBLE,
            manager_draft_grade VARCHAR,
            is_keeper INTEGER
        )
        """
    )
    rows = []
    for i, value in enumerate([10.0, 12.0, 14.0, 16.0, 100.0], start=1):
        rows.append(
            (
                2025,
                1,
                i,
                0.0,
                "snake",
                "rookie",
                f"Mgr{i}",
                f"fid_r{i}",
                "RB",
                value,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
            )
        )
    for i, value in enumerate([180.0, 190.0, 200.0, 210.0, 220.0], start=1):
        rows.append(
            (
                2025,
                1,
                20 + i,
                0.0,
                "snake",
                "",
                f"Redraft{i}",
                f"fid_d{i}",
                "RB",
                value,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                0,
            )
        )
    conn.executemany("INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.draft_value_zscore()
        expected, zscore, pick_score = runner.conn.execute(
            """
            SELECT expected_lamar, draft_value_zscore, pick_score
            FROM public.draft
            WHERE draft_category = 'rookie' AND manager_lamar = 100
            """
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert expected < 30
    assert zscore > 2
    assert pick_score == round(100 + 15 * zscore, 3)


def test_blended_score_updates_duplicate_pick_slots_by_draft_identity(tmp_path, monkeypatch):
    monkeypatch.setenv("DRAFT_SCORE_GLOBAL_BASELINE", "0")
    db_name = "draft_duplicate_pick_identity_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            draft_id VARCHAR,
            year INTEGER,
            round INTEGER,
            pick INTEGER,
            cost DOUBLE,
            draft_type VARCHAR,
            draft_category VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            player VARCHAR,
            position VARCHAR,
            manager_lamar DOUBLE,
            expected_lamar DOUBLE,
            draft_value_zscore DOUBLE,
            pick_score DOUBLE,
            manager_draft_score DOUBLE,
            manager_draft_grade VARCHAR,
            is_keeper INTEGER
        )
        """
    )
    rows = []
    for i, value in enumerate([100.0, 10.0, 12.0, 14.0, 16.0], start=1):
        rows.append(
            (
                "rookie-2025",
                2025,
                1,
                i,
                0.0,
                "snake",
                "rookie",
                f"Mgr{i}",
                f"fid_r{i}",
                f"Rookie {i}",
                "RB",
                value,
                None,
                None,
                None,
                None,
                None,
                0,
            )
        )
    for i, value in enumerate([220.0, 180.0, 190.0, 200.0, 210.0], start=1):
        rows.append(
            (
                "startup-2025",
                2025,
                1,
                i,
                0.0,
                "snake",
                "startup",
                f"Redraft{i}",
                f"fid_s{i}",
                f"Startup {i}",
                "RB",
                value,
                None,
                None,
                None,
                None,
                None,
                0,
            )
        )
    conn.executemany("INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.draft_value_zscore()
        rookie_expected, startup_expected = runner.conn.execute(
            """
            SELECT
                MAX(CASE WHEN draft_id = 'rookie-2025' AND pick = 1 THEN expected_lamar END),
                MAX(CASE WHEN draft_id = 'startup-2025' AND pick = 1 THEN expected_lamar END)
            FROM public.draft
            """
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rookie_expected < 20
    assert startup_expected > 190


def test_draft_score_profile_detects_roster_config(tmp_path, monkeypatch):
    monkeypatch.setenv("DRAFT_SCORE_GLOBAL_BASELINE", "0")
    db_name = "draft_profile_config_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            round INTEGER,
            pick INTEGER,
            cost DOUBLE,
            draft_type VARCHAR,
            manager VARCHAR,
            position VARCHAR,
            manager_lamar DOUBLE,
            draft_value_zscore DOUBLE,
            pick_score DOUBLE,
            is_keeper INTEGER
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            num_teams INTEGER,
            draft_rounds INTEGER,
            scoring_rec DOUBLE,
            scoring_pass_td DOUBLE,
            roster_QB INTEGER,
            roster_RB INTEGER,
            roster_WR INTEGER,
            roster_TE INTEGER,
            roster_FLX INTEGER,
            roster_SUPER_FLEX INTEGER,
            roster_BN INTEGER,
            roster_LB INTEGER,
            roster_DL INTEGER,
            roster_DB INTEGER,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute("INSERT INTO public.draft VALUES (2025, 10, 115, 0, 'snake', 'Joe', 'QB', 45.0, NULL, NULL, 0)")
    conn.execute(
        """
        INSERT INTO public.league_settings
        VALUES (2025, 12, 18, 0.5, 4, 2, 2, 3, 1, 2, 1, 8, 1, 1, 1, true)
        """
    )
    conn.close()

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        draft_table = runner._qualified_name("draft")
        draft_cols = runner._get_table_columns("draft")
        runner._create_draft_score_rows_temp(draft_table, draft_cols, "manager_lamar", "position")
        profile = runner.conn.execute(
            """
            SELECT teams, draft_rounds, bench_count, flex_count, superflex, idp, uses_median, total_roster_slots
            FROM _draft_score_rows
            """
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert profile[0] == 12
    assert profile[1] == 18
    assert profile[2] == 8
    assert profile[3] == 3
    assert profile[4] == 1
    assert profile[5] == 1
    assert profile[6] == 1
    assert profile[7] > 20


def test_draft_score_ignores_all_zero_lamar_years(tmp_path, monkeypatch):
    monkeypatch.setenv("DRAFT_SCORE_GLOBAL_BASELINE", "0")
    db_name = "draft_zero_lamar_year_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            round INTEGER,
            pick INTEGER,
            cost DOUBLE,
            draft_type VARCHAR,
            manager VARCHAR,
            position VARCHAR,
            manager_lamar DOUBLE,
            expected_lamar DOUBLE,
            draft_value_zscore DOUBLE,
            pick_score DOUBLE,
            is_keeper INTEGER
        )
        """
    )
    rows = []
    for pick in range(1, 6):
        rows.append((2024, 1, pick, 0.0, "snake", f"Zero{pick}", "RB", 0.0, None, None, None, 0))
    for pick, value in enumerate([10.0, 12.0, 14.0, 16.0, 100.0], start=1):
        rows.append((2025, 1, pick, 0.0, "snake", f"Mgr{pick}", "RB", value, None, None, None, 0))
    conn.executemany("INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
    conn.close()

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.draft_value_zscore()
        scored_by_year = runner.conn.execute(
            """
            SELECT year, COUNT(draft_value_zscore)
            FROM public.draft
            GROUP BY year
            ORDER BY year
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert scored_by_year == [(2024, 0), (2025, 5)]


def test_pick_grades_partition_keepers_from_non_keepers(tmp_path):
    db_name = "draft_grading_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            round INTEGER,
            pick INTEGER,
            pick_score DOUBLE,
            manager_lamar DOUBLE,
            is_keeper INTEGER,
            draft_grade VARCHAR
        )
        """
    )

    non_keeper_rows = [(2025, 1, pick, float(pick), float(pick), 0, None) for pick in range(1, 21)]
    keeper_rows = [(2025, 2, pick, float(100 + pick), float(100 + pick), 1, None) for pick in range(1, 11)]
    conn.executemany("INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?, ?)", non_keeper_rows + keeper_rows)
    conn.close()

    runner = _DraftRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner._assign_pick_grade_percentiles(
            runner._qualified_name("draft"),
            runner._get_table_columns("draft"),
            "manager_lamar",
            "'standard'",
            "'standard'",
        )
        rows = runner.conn.execute(
            """
            SELECT is_keeper, pick, pick_score, draft_grade
            FROM public.draft
            ORDER BY is_keeper, pick
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    non_keepers = [row for row in rows if row[0] == 0]
    keepers = [row for row in rows if row[0] == 1]

    assert non_keepers[-1][3] == "A+"
    assert non_keepers[0][3] == "F"
    assert keepers[-1][3] == "A+"
    assert keepers[0][3] == "F"
