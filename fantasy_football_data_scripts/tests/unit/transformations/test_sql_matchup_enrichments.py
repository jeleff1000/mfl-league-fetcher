import sys
from pathlib import Path

import duckdb
import pandas as pd

SCRIPTS_DIR = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(SCRIPTS_DIR))

from multi_league.transformations.common.sql_base import SQLEnrichmentsBase
from multi_league.core.franchise_identity_schema import FRANCHISE_IDENTITY_REGISTRY_DDL
from multi_league.transformations.matchup.sql_matchup_enrichments import MatchupEnrichmentsMixin


class _MatchupRunner(MatchupEnrichmentsMixin, SQLEnrichmentsBase):
    pass


def test_compute_league_weekly_stats_replaces_stale_provider_derivations(tmp_path):
    """Shared enrichment owns weekly aggregates even when a provider supplied values."""
    db_name = "weekly_stats_refresh_test"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            weekly_mean DOUBLE,
            weekly_median DOUBLE,
            league_weekly_mean DOUBLE,
            league_weekly_median DOUBLE,
            above_league_median INTEGER,
            below_league_median INTEGER,
            teams_beat_this_week INTEGER,
            opponent_teams_beat_this_week INTEGER
        )
        """
    )
    scores = [155.53, 147.43, 129.58, 122.0, 120.42, 119.08,
              113.98, 106.99, 81.13, 75.39, 70.01, 63.4]
    rows = []
    for index, score in enumerate(scores):
        opponent = index + 1 if index % 2 == 0 else index - 1
        rows.append(
            (
                db_name,
                f"team-{index}",
                f"team-{opponent}",
                2026,
                1,
                score,
                10.98,
                10.98,
                0,
                0,
            )
        )
    conn.executemany(
        """
        INSERT INTO public.matchup (
            db_name, franchise_id, opponent_franchise_id, year, week,
            team_points, league_weekly_mean, league_weekly_median,
            teams_beat_this_week, opponent_teams_beat_this_week
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        runner.compute_league_weekly_stats()
        actual = runner._get_connection().execute(
            """
            SELECT franchise_id, league_weekly_mean, league_weekly_median,
                   teams_beat_this_week, opponent_teams_beat_this_week
            FROM public.matchup
            ORDER BY team_points DESC
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert {row[1] for row in actual} == {108.75}
    assert {row[2] for row in actual} == {116.53}
    assert [row[3] for row in actual] == list(range(11, -1, -1))
    assert actual[0][4] == 10
    assert actual[1][4] == 11


def test_resolve_hidden_managers_reapplies_saved_franchise_merges_by_identity(tmp_path):
    """A full rebuild must preserve user merges without conflating a shared team name."""
    db_name = "saved_franchise_merge_test"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_team_key VARCHAR,
            opponent_franchise_id VARCHAR,
            platform VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (db_name, "jeff-2015", "Jeff - OUT OF MOVES", "jeff-2015", "348.l.1.t.11", "OUT OF MOVES", "Other", "other", "348.l.1.t.1", "other", "yahoo", 2015, 1, 100.0, 90.0, 0, None),
            (db_name, "jeff-2018", "Jeff - Last Place", "jeff-2018", "380.l.1.t.6", "Last Place", "Other", "other", "380.l.1.t.1", "other", "yahoo", 2018, 1, 110.0, 95.0, 0, None),
            (db_name, "matt", "Matt Davis", "matt", "242.l.1.t.3", "Last Place", "Other", "other", "242.l.1.t.1", "other", "yahoo", 2010, 1, 105.0, 92.0, 0, None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(
        db_name=db_name,
        data_dir=str(tmp_path),
        manager_name_overrides={"Jeff - Last Place": "Jeff", "Last Place": "Jeff"},
        franchise_merges=[
            {
                "display_name": "Jeff - Last Place",
                "owner_ids": ["jeff-2018", "jeff-2015"],
            }
        ],
    )
    try:
        runner.resolve_hidden_managers()
        rows = runner._get_connection().execute(
            """
            SELECT year, franchise_id, manager, team_name
            FROM public.matchup
            ORDER BY year
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        (2010, "matt", "Matt Davis", "Last Place"),
        (2015, "jeff-2018", "Jeff", "OUT OF MOVES"),
        (2018, "jeff-2018", "Jeff", "Last Place"),
    ]


def test_resolve_hidden_managers_does_not_issue_updates_for_empty_matchup_table(tmp_path, monkeypatch):
    """An active week without posted matchups has no identity work to resolve."""
    db_name = "empty_matchup_identity_test"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR)")
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        monkeypatch.setattr(
            runner,
            "_execute",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("empty matchup tables must not run identity UPDATE statements")
            ),
        )
        assert runner.resolve_hidden_managers() == 0
    finally:
        if runner._conn is not None:
            runner._conn.close()


def test_resolve_hidden_managers_applies_saved_aliases_without_posted_matchups(tmp_path):
    """Preseason refresh rows must use saved aliases before matchup results exist."""
    db_name = "demo_league"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR)")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            franchise_id VARCHAR,
            franchise_name VARCHAR
        )
        """
    )
    aliases = {
        "Daniel": "Lisa",
        "Eleff": "Joe",
        "Jason": "Erin",
        "Jesse": "Jessica",
        "Marc": "Tom",
        "Yaacov": "Jackie",
    }
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?)",
        [
            (db_name, source, source, f"fid-{source.lower()}", source)
            for source in aliases
        ],
    )
    conn.close()

    runner = _MatchupRunner(
        db_name=db_name,
        data_dir=str(tmp_path),
        manager_name_overrides=aliases,
    )
    try:
        runner.resolve_hidden_managers()
        rows = runner._get_connection().execute(
            """
            SELECT manager, opponent, franchise_name
            FROM public.player_fantasy
            ORDER BY franchise_id
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        (aliases[source], aliases[source], aliases[source])
        for source in sorted(aliases)
    ]


def test_resolve_hidden_managers_applies_saved_merge_without_posted_matchups(tmp_path):
    """A new seasonal owner ID must collapse into the saved franchise before week 1."""
    db_name = "preseason_saved_merge_test"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR)")
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR,
            manager VARCHAR,
            franchise_id VARCHAR,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?)",
        [
            (db_name, "Old Joe", "stable-joe", "Old Joe"),
            (db_name, "Seasonal Joe", "seasonal-joe", "Seasonal Joe"),
        ],
    )
    conn.close()

    runner = _MatchupRunner(
        db_name=db_name,
        data_dir=str(tmp_path),
        manager_name_overrides={"Old Joe": "Joe"},
        franchise_merges=[
            {
                "display_name": "Old Joe",
                "into_franchise_id": "stable-joe",
                "owner_ids": ["stable-joe", "seasonal-joe"],
            }
        ],
    )
    try:
        runner.resolve_hidden_managers()
        rows = runner._get_connection().execute(
            "SELECT manager, franchise_id, franchise_name FROM public.draft ORDER BY manager"
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Joe", "stable-joe", "Joe"),
        ("Joe", "stable-joe", "Joe"),
    ]


def test_populate_franchise_id_uses_roster_identity_before_matchups_exist(tmp_path):
    """Draft and transaction identities must join the roster before week 1 scores post."""
    db_name = "preseason_roster_identity_test"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR, year INTEGER, week INTEGER, manager VARCHAR,
            manager_guid VARCHAR, franchise_id VARCHAR, team_key VARCHAR, team_name VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR, year INTEGER, week INTEGER, manager VARCHAR,
            manager_guid VARCHAR, franchise_id VARCHAR, team_key VARCHAR, team_name VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO public.player_fantasy VALUES (?, 2026, 1, 'Eleff', 'new-joe', 'new-joe', '470.l.1.t.1', NULL)",
        [db_name],
    )
    conn.execute(
        """
        CREATE TABLE public.schedule (
            db_name VARCHAR, year INTEGER, week INTEGER, manager VARCHAR,
            manager_guid VARCHAR, franchise_id VARCHAR, team_name VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO public.schedule VALUES (?, 2026, 1, 'Erin', 'stable-erin', 'stable-erin', 'Team Erin')",
        [db_name],
    )
    conn.execute(
        """
        CREATE TABLE public.draft (
            db_name VARCHAR, year INTEGER, manager VARCHAR, manager_guid VARCHAR,
            franchise_id VARCHAR, team_key VARCHAR, team_name VARCHAR, franchise_name VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO public.draft VALUES (?, 2026, 'Eleff', '--hidden--', NULL, '470.l.1.t.1', 'Team Joe', 'Eleff')",
        [db_name],
    )
    conn.execute(
        "INSERT INTO public.draft VALUES (?, 2026, 'Erin', '--hidden--', NULL, '470.l.1.t.2', 'Team Erin', 'Erin')",
        [db_name],
    )
    conn.execute(
        """
        CREATE TABLE public.transactions (
            db_name VARCHAR, year INTEGER, manager VARCHAR, manager_guid VARCHAR,
            franchise_id VARCHAR, team_name VARCHAR
        )
        """
    )
    conn.execute(
        "INSERT INTO public.transactions VALUES (?, 2026, 'Eleff', '--hidden--', NULL, 'Team Joe')",
        [db_name],
    )
    conn.execute(
        "INSERT INTO public.transactions VALUES (?, 2026, 'Erin', '--hidden--', NULL, 'Team Erin')",
        [db_name],
    )
    conn.close()

    runner = _MatchupRunner(
        db_name=db_name,
        data_dir=str(tmp_path),
        manager_name_overrides={"Eleff": "Joe"},
        franchise_merges=[
            {
                "display_name": "Eleff",
                "into_franchise_id": "stable-joe",
                "owner_ids": ["new-joe", "stable-joe"],
            }
        ],
    )
    try:
        runner.resolve_hidden_managers()
        runner.populate_franchise_id()
        draft = runner._get_connection().execute(
            "SELECT manager, manager_guid, franchise_id, franchise_name FROM public.draft ORDER BY manager"
        ).fetchall()
        transaction = runner._get_connection().execute(
            "SELECT manager, franchise_id FROM public.transactions ORDER BY manager"
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert draft == [
        ("Erin", "--hidden--", "stable-erin", "Erin"),
        ("Joe", "--hidden--", "stable-joe", "Joe"),
    ]
    assert transaction == [("Erin", "stable-erin"), ("Joe", "stable-joe")]


def test_resolve_hidden_managers_links_preseason_schedule_opponents(tmp_path):
    """A complete schedule should carry stable opponent identity before scores post."""
    db_name = "preseason_schedule_opponents"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR)")
    conn.execute(
        """
        CREATE TABLE public.schedule (
            db_name VARCHAR, year INTEGER, week INTEGER,
            manager VARCHAR, manager_guid VARCHAR, franchise_id VARCHAR,
            opponent VARCHAR, opponent_guid VARCHAR, opponent_franchise_id VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.schedule VALUES (?, 2026, 1, ?, ?, ?, ?, NULL, NULL)",
        [
            (db_name, "Eleff", "new-joe", "new-joe", "Erin"),
            (db_name, "Erin", "stable-erin", "stable-erin", "Eleff"),
        ],
    )
    conn.close()

    runner = _MatchupRunner(
        db_name=db_name,
        data_dir=str(tmp_path),
        manager_name_overrides={"Eleff": "Joe"},
    )
    try:
        runner.resolve_hidden_managers()
        rows = runner._get_connection().execute(
            "SELECT manager, opponent, opponent_guid, opponent_franchise_id FROM public.schedule ORDER BY manager"
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Erin", "Joe", "new-joe", "new-joe"),
        ("Joe", "Erin", "stable-erin", "stable-erin"),
    ]


def test_resolve_hidden_managers_canonicalizes_preseason_name_by_franchise_id(tmp_path):
    """A live roster name should replace an older draft display for the same owner ID."""
    db_name = "preseason_display_name"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR)")
    conn.execute(
        "CREATE TABLE public.player_fantasy (db_name VARCHAR, year INTEGER, week INTEGER, manager VARCHAR, franchise_id VARCHAR)"
    )
    conn.execute(
        "CREATE TABLE public.draft (db_name VARCHAR, year INTEGER, manager VARCHAR, franchise_id VARCHAR, franchise_name VARCHAR)"
    )
    conn.execute(
        "INSERT INTO public.player_fantasy VALUES (?, 2026, 1, 'Matthew F.', 'stable-matthew')",
        [db_name],
    )
    conn.execute(
        "INSERT INTO public.draft VALUES (?, 2026, 'Matthew', 'stable-matthew', 'Matthew')",
        [db_name],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        runner.resolve_hidden_managers()
        row = runner._get_connection().execute(
            "SELECT manager, franchise_name FROM public.draft"
        ).fetchone()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert row == ("Matthew F.", "Matthew F.")


def test_dedup_matchup_publish_identity_skips_empty_matchup_table(tmp_path, monkeypatch):
    """No active matchup rows means there cannot be a duplicate to remove."""
    db_name = "empty_matchup_dedup_test"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute("CREATE TABLE public.matchup (db_name VARCHAR, manager_week VARCHAR)")
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        monkeypatch.setattr(
            runner,
            "_execute",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                AssertionError("empty matchup tables must not stage duplicate rows")
            ),
        )
        assert runner.dedup_matchup_publish_identity() == 0
    finally:
        if runner._conn is not None:
            runner._conn.close()


def test_bracket_tracer_preserves_explicit_yahoo_consolation_labels(monkeypatch):
    """Yahoo's literal bracket sections outrank a heuristic tracer classification."""
    from multi_league.transformations.matchup.modules.playoff_bracket import bracket_tracer

    teams = [("A", "B"), ("B", "A"), ("C", "D"), ("D", "C"), ("E", "F"), ("F", "E"), ("G", "H"), ("H", "G")]
    matchup_df = pd.DataFrame(
        [
            {
                "year": 2015,
                "week": 15,
                "franchise_id": team,
                "opponent_franchise_id": opponent,
                "opponent": opponent,
                "is_playoffs": 1,
                "is_consolation": int(team in {"E", "F", "G", "H"}),
            }
            for team, opponent in teams
        ]
    )

    monkeypatch.setattr(
        bracket_tracer,
        "trace_bracket",
        lambda *_args, **_kwargs: {
            "classifications": {(team, 15): "playoff" for team, _ in teams},
            "championship_week": 15,
            "championship_teams": ["A", "B"],
            "champion": "A",
        },
    )

    result = _MatchupRunner._classify_via_bracket_tracer(
        object.__new__(_MatchupRunner),
        matchup_df,
        {
            2015: {
                "playoff_start_week": 15,
                "num_playoff_teams": 4,
                "bye_teams": 0,
                "end_week": 16,
                "num_teams": 10,
            }
        },
    )

    championship = result[result["franchise_id"].isin(["A", "B", "C", "D"])]
    consolation = result[result["franchise_id"].isin(["E", "F", "G", "H"])]
    assert set(championship[["is_playoffs", "is_consolation"]].itertuples(index=False, name=None)) == {(1, 0)}
    assert set(consolation[["is_playoffs", "is_consolation"]].itertuples(index=False, name=None)) == {(0, 1)}


def test_normalize_matchup_flags_keeps_yahoo_consolation_rows_consolation(tmp_path):
    """Yahoo encodes literal consolation-bracket rows with both raw flags set."""
    db_name = "yahoo_raw_bracket_flags"
    conn = duckdb.connect(str(tmp_path / f"{db_name}.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            is_playoffs INTEGER,
            is_consolation INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?)",
        [
            (db_name, 2015, 15, "championship_team", 1, 0),
            (db_name, 2015, 15, "consolation_team", 1, 1),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        runner.normalize_matchup_flags()
        rows = runner._get_connection().execute(
            "SELECT franchise_id, CAST(is_playoffs AS INTEGER), CAST(is_consolation AS INTEGER) "
            "FROM public.matchup ORDER BY franchise_id"
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("championship_team", 1, 0),
        ("consolation_team", 0, 1),
    ]


def test_resolve_playoff_round_type_uses_season_span_for_multiweek_shapes():
    assert (
        _MatchupRunner._resolve_playoff_round_type(
            {
                "playoff_teams": 4,
                "playoff_start_week": 14,
                "end_week": 17,
                "has_multiweek_championship": True,
            }
        )
        == 1
    )
    assert (
        _MatchupRunner._resolve_playoff_round_type(
            {
                "playoff_teams": 6,
                "playoff_start_week": 15,
                "end_week": 18,
                "has_multiweek_championship": True,
                "sleeper_playoff_type": 1,
            }
        )
        == 2
    )


def test_dedup_matchup_publish_identity_prefers_richer_real_row():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            manager_week VARCHAR,
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            matchup_id INTEGER,
            matchup_key VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            win INTEGER,
            loss INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "speed_test",
                "alice_2025_1",
                2025,
                1,
                "Alice",
                None,
                "alice_fid",
                None,
                "Aces",
                None,
                None,
                None,
                None,
                None,
                None,
                1,
                None,
                None,
            ),
            (
                "speed_test",
                "alice_2025_1",
                2025,
                1,
                "Alice",
                "guid_alice",
                "alice_fid",
                "461.l.1.t.1",
                "Aces",
                "Bob",
                "bob_fid",
                10,
                "461.l.1.1",
                111.4,
                98.2,
                0,
                1,
                0,
            ),
            (
                "speed_test",
                "bob_2025_1",
                2025,
                1,
                "Bob",
                "guid_bob",
                "bob_fid",
                "461.l.1.t.2",
                "Bees",
                "Alice",
                "alice_fid",
                10,
                "461.l.1.1",
                98.2,
                111.4,
                0,
                0,
                1,
            ),
        ],
    )

    runner = _MatchupRunner(db_name="speed_test", data_dir="local")
    runner._conn = conn

    try:
        removed = runner.dedup_matchup_publish_identity()
        rows = runner.conn.execute(
            """
            SELECT manager_week, opponent, team_points, is_bye_week, matchup_id
            FROM public.matchup
            ORDER BY manager_week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert removed == 1
    assert rows == [
        ("alice_2025_1", "Bob", 111.4, 0, 10),
        ("bob_2025_1", "Alice", 98.2, 0, 10),
    ]


def test_cumulative_records_freezes_final_playoff_seed_from_last_regular_week(tmp_path):
    db_name = "matchup_seed_freeze_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_start_week INTEGER,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute("INSERT INTO public.league_settings VALUES (2025, 3, FALSE)")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            team_points DOUBLE,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            wins_to_date INTEGER,
            losses_to_date INTEGER,
            ties_to_date INTEGER,
            points_scored_to_date DOUBLE,
            playoff_seed_to_date INTEGER,
            final_playoff_seed INTEGER,
            playoff_seed INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("A", 2025, 1, 1, 0, 0, 100.0, 0, 0, None, None, None, None, None, None, None),
            ("B", 2025, 1, 1, 0, 0, 90.0, 0, 0, None, None, None, None, None, None, None),
            ("C", 2025, 1, 0, 1, 0, 85.0, 0, 0, None, None, None, None, None, None, None),
            ("D", 2025, 1, 0, 1, 0, 80.0, 0, 0, None, None, None, None, None, None, None),
            ("A", 2025, 2, 1, 0, 0, 95.0, 0, 0, None, None, None, None, None, None, None),
            ("B", 2025, 2, 0, 1, 0, 80.0, 0, 0, None, None, None, None, None, None, None),
            ("C", 2025, 2, 0, 1, 0, 70.0, 0, 0, None, None, None, None, None, None, None),
            ("D", 2025, 2, 1, 0, 0, 110.0, 0, 0, None, None, None, None, None, None, None),
            ("A", 2025, 3, 0, 1, 0, 88.0, 1, 0, None, None, None, None, None, None, None),
            ("D", 2025, 3, 1, 0, 0, 102.0, 1, 0, None, None, None, None, None, None, None),
            ("B", 2025, 3, 1, 0, 0, 99.0, 0, 1, None, None, None, None, None, None, None),
            ("C", 2025, 3, 0, 1, 0, 77.0, 0, 1, None, None, None, None, None, None, None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.cumulative_records()
        rows = runner.conn.execute(
            """
            SELECT franchise_id, week, wins_to_date, losses_to_date, playoff_seed_to_date, final_playoff_seed, playoff_seed
            FROM public.matchup
            ORDER BY week, franchise_id
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("A", 1, 1, 0, 1, 1, 1),
        ("B", 1, 1, 0, 2, 3, 3),
        ("C", 1, 0, 1, 3, 4, 4),
        ("D", 1, 0, 1, 4, 2, 2),
        ("A", 2, 2, 0, 1, 1, 1),
        ("B", 2, 1, 1, 3, 3, 3),
        ("C", 2, 0, 2, 4, 4, 4),
        ("D", 2, 1, 1, 2, 2, 2),
        ("A", 3, 2, 0, 1, 1, 1),
        ("B", 3, 1, 1, 3, 3, 3),
        ("C", 3, 0, 2, 4, 4, 4),
        ("D", 3, 1, 1, 2, 2, 2),
    ]


def test_cumulative_records_streaks_ignore_consolation_and_non_games_but_cross_playoff_seasons(tmp_path):
    db_name = "matchup_streak_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            team_points DOUBLE,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            is_placeholder INTEGER,
            win_streak INTEGER,
            loss_streak INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("A", 2024, 14, 0, 1, 0, 90.0, 0, 0, 0, 0, None, None),
            ("A", 2024, 15, 1, 0, 0, 110.0, 1, 0, 0, 0, None, None),
            ("A", 2024, 16, 1, 0, 0, 120.0, 1, 0, 0, 0, None, None),
            ("A", 2024, 17, 0, 1, 0, 80.0, 0, 1, 0, 0, None, None),
            ("A", 2025, 1, 1, 0, 0, 105.0, 0, 0, 0, 0, None, None),
            ("A", 2025, 2, 0, 1, 0, 95.0, 0, 0, 0, 0, None, None),
            ("A", 2025, 3, 0, 0, 1, 100.0, 0, 0, 0, 0, None, None),
            ("A", 2025, 4, 0, 1, 0, 88.0, 0, 0, 0, 0, None, None),
            ("A", 2025, 5, None, None, None, 0.0, 0, 0, 1, 0, None, None),
            ("A", 2025, 6, 0, 1, 0, 92.0, 0, 0, 0, 0, None, None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.cumulative_records()
        rows = runner.conn.execute(
            """
            SELECT year, week, win_streak, loss_streak
            FROM public.matchup
            ORDER BY year, week
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        (2024, 14, 0, 1),
        (2024, 15, 1, 0),
        (2024, 16, 2, 0),
        (2024, 17, 0, 0),
        (2025, 1, 3, 0),
        (2025, 2, 0, 1),
        (2025, 3, 0, 0),
        (2025, 4, 0, 1),
        (2025, 5, 0, 0),
        (2025, 6, 0, 2),
    ]


def test_populate_franchise_id_uses_stable_keys_and_leaves_ambiguous_rows_null(tmp_path):
    db_name = "matchup_identity_backfill_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_name VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            team_key VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("fid_alpha", "Alex", None, "Alpha Squad", "414.l.1.t.1", 2024, 1),
            ("fid_beta", "Alex", None, "Beta Squad", "414.l.1.t.2", 2024, 1),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            team_key VARCHAR,
            franchise_id VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.draft VALUES (?, ?, ?, ?, ?, ?)",
        [
            (2024, "Alex", None, None, "414.l.1.t.2", None),
            (2024, "Alex", None, None, None, None),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.transactions (
            year INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            franchise_id VARCHAR,
            source_manager VARCHAR,
            source_manager_guid VARCHAR,
            source_team_name VARCHAR,
            source_franchise_id VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (2024, "Alex", None, "Alpha Squad", None, "Alex", None, "Beta Squad", None),
            (2024, "Alex", None, None, None, "Alex", None, None, None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.populate_franchise_id()
        db_conn = runner._get_connection()
        draft_rows = db_conn.execute(
            """
            SELECT team_key, franchise_id
            FROM public.draft
            ORDER BY team_key NULLS LAST
            """
        ).fetchall()
        transaction_rows = db_conn.execute(
            """
            SELECT team_name, franchise_id, source_team_name, source_franchise_id
            FROM public.transactions
            ORDER BY team_name NULLS LAST
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert draft_rows == [
        ("414.l.1.t.2", "fid_beta"),
        (None, None),
    ]
    assert transaction_rows == [
        ("Alpha Squad", "fid_alpha", "Beta Squad", "fid_beta"),
        (None, None, None, None),
    ]


def test_resolve_hidden_managers_splits_real_guid_multi_team_owner_by_team_key(tmp_path):
    db_name = "matchup_multi_team_owner_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "owner-guid",
                "Mc Bidmaster",
                "owner-guid",
                "371.l.2116.t.2",
                "OPEN TEAM 1",
                "Bob",
                "bob-guid",
                "bob-guid",
                2017,
                1,
                100.0,
                90.0,
                0,
                None,
            ),
            (
                "bob-guid",
                "Bob",
                "bob-guid",
                "371.l.2116.t.3",
                "Bobcats",
                "Mc Bidmaster",
                "owner-guid",
                "owner-guid",
                2017,
                1,
                90.0,
                100.0,
                0,
                None,
            ),
            (
                "owner-guid",
                "Mc Bidmaster",
                "owner-guid",
                "371.l.2116.t.6",
                "OPEN TEAM 2",
                "Cal",
                "cal-guid",
                "cal-guid",
                2017,
                1,
                110.0,
                80.0,
                0,
                None,
            ),
            (
                "cal-guid",
                "Cal",
                "cal-guid",
                "371.l.2116.t.7",
                "Calzone",
                "Mc Bidmaster",
                "owner-guid",
                "owner-guid",
                2017,
                1,
                80.0,
                110.0,
                0,
                None,
            ),
            (
                "owner-guid",
                "Mc Bidmaster",
                None,
                None,
                "OPEN TEAM 1",
                None,
                None,
                None,
                2017,
                2,
                None,
                None,
                1,
                None,
            ),
            (
                "owner-guid",
                "Mc Bidmaster",
                "owner-guid",
                "9",
                "OPEN TEAM 1",
                "Future Opponent",
                "future-guid",
                "future-guid",
                2025,
                1,
                120.0,
                100.0,
                0,
                None,
            ),
        ],
    )
    for table_name in ("player_fantasy", "draft", "schedule"):
        conn.execute(
            f"""
            CREATE TABLE public.{table_name} (
                franchise_id VARCHAR,
                manager VARCHAR,
                manager_guid VARCHAR,
                team_key VARCHAR,
                team_name VARCHAR,
                year INTEGER,
                week INTEGER
            )
            """
        )
        conn.executemany(
            f"INSERT INTO public.{table_name} VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                ("owner-guid", "Mc Bidmaster", "owner-guid", "371.l.2116.t.2", "OPEN TEAM 1", 2017, 1),
                ("owner-guid", "Mc Bidmaster", "owner-guid", "371.l.2116.t.6", "OPEN TEAM 2", 2017, 1),
            ],
        )
    conn.execute(
        FRANCHISE_IDENTITY_REGISTRY_DDL.replace("franchise_identity_registry", "public.franchise_identity_registry", 1)
    )
    conn.executemany(
        """
        INSERT INTO public.franchise_identity_registry (
            db_name, platform, base_franchise_id, manager_guid, identity_key, branch_key,
            resolved_franchise_id, team_index, anchor_year, anchor_week, anchor_team_name,
            anchor_manager, anchor_team_key, anchor_team_slot, known_team_names,
            known_manager_names, known_team_slots, active_years, created_at, updated_at
        ) VALUES (?, 'yahoo', 'owner-guid', 'owner-guid', ?, ?, ?, ?, 2017, 1, ?,
                  'Mc Bidmaster', ?, ?, ?, 'Mc Bidmaster', ?, '2017', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [
            (
                db_name,
                "team_slot:2",
                "team_slot:2",
                "owner-guid_1",
                1,
                "OPEN TEAM 1",
                "371.l.2116.t.2",
                "2",
                "OPEN TEAM 1",
                "2",
            ),
            (
                db_name,
                "team_slot:6",
                "team_slot:6",
                "owner-guid_2",
                2,
                "OPEN TEAM 2",
                "371.l.2116.t.6",
                "6",
                "OPEN TEAM 2",
                "6",
            ),
            (
                db_name,
                "team_name:open team 1",
                "team_name:open team 1",
                "owner-guid_3",
                3,
                "OPEN TEAM 1",
                None,
                None,
                "OPEN TEAM 1",
                None,
            ),
            (
                db_name,
                "team_slot:9",
                "team_slot:9",
                "owner-guid_4",
                4,
                "OPEN TEAM 1",
                "9",
                "9",
                "OPEN TEAM 1",
                "9",
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        db_conn = runner._get_connection()
        owner_rows = db_conn.execute(
            """
            SELECT team_key, franchise_id, manager
            FROM public.matchup
            WHERE team_name IN ('OPEN TEAM 1', 'OPEN TEAM 2')
            ORDER BY team_key NULLS FIRST
            """
        ).fetchall()
        asymmetry_count = db_conn.execute(
            """
            SELECT COUNT(*)
            FROM public.matchup a
            WHERE a.year = 2017
              AND COALESCE(a.is_bye_week, 0) = 0
              AND a.opponent IS NOT NULL
              AND NOT EXISTS (
                SELECT 1
                FROM public.matchup b
                WHERE b.year = a.year
                  AND b.week = a.week
                  AND b.franchise_id = a.opponent_franchise_id
                  AND b.opponent_franchise_id = a.franchise_id
              )
            """
        ).fetchone()[0]
        related_rows = {
            table_name: db_conn.execute(
                f"SELECT team_key, franchise_id, manager FROM public.{table_name} ORDER BY team_key"
            ).fetchall()
            for table_name in ("player_fantasy", "draft", "schedule")
        }
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert owner_rows == [
        (None, "owner-guid_1", "Mc Bidmaster - OPEN TEAM 1 (id_1)"),
        ("371.l.2116.t.2", "owner-guid_1", "Mc Bidmaster - OPEN TEAM 1 (id_1)"),
        ("371.l.2116.t.6", "owner-guid_2", "Mc Bidmaster - OPEN TEAM 2"),
        ("9", "owner-guid_4", "Mc Bidmaster - OPEN TEAM 1 (id_4)"),
    ]
    expected_related_rows = [
        ("371.l.2116.t.2", "owner-guid_1", "Mc Bidmaster - OPEN TEAM 1 (id_1)"),
        ("371.l.2116.t.6", "owner-guid_2", "Mc Bidmaster - OPEN TEAM 2"),
    ]
    assert related_rows == {
        "player_fantasy": expected_related_rows,
        "draft": expected_related_rows,
        "schedule": expected_related_rows,
    }
    assert asymmetry_count == 0


def test_resolve_hidden_managers_keeps_yahoo_team_slot_continuity_across_years(tmp_path):
    db_name = "matchup_multi_team_owner_slot_continuity_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "273.l.211043.t.1",
                "The Irate Irish",
                "Bob",
                "bob-guid",
                "bob-guid",
                2012,
                1,
                100.0,
                90.0,
                0,
                None,
            ),
            (
                "bob-guid",
                "Bob",
                "bob-guid",
                "273.l.211043.t.2",
                "Bobcats",
                "Jason",
                "owner-guid",
                "owner-guid",
                2012,
                1,
                90.0,
                100.0,
                0,
                None,
            ),
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "273.l.211043.t.10",
                "Fcuk It'ers",
                "Cal",
                "cal-guid",
                "cal-guid",
                2012,
                1,
                110.0,
                80.0,
                0,
                None,
            ),
            (
                "cal-guid",
                "Cal",
                "cal-guid",
                "273.l.211043.t.3",
                "Calzone",
                "Jason",
                "owner-guid",
                "owner-guid",
                2012,
                1,
                80.0,
                110.0,
                0,
                None,
            ),
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "314.l.170659.t.1",
                "Pierre's Pals",
                "Dan",
                "dan-guid",
                "dan-guid",
                2013,
                1,
                101.0,
                91.0,
                0,
                None,
            ),
            (
                "dan-guid",
                "Dan",
                "dan-guid",
                "314.l.170659.t.2",
                "Danimals",
                "Jason",
                "owner-guid",
                "owner-guid",
                2013,
                1,
                91.0,
                101.0,
                0,
                None,
            ),
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "331.l.74270.t.4",
                "Reseated Primary",
                "Eva",
                "eva-guid",
                "eva-guid",
                2014,
                1,
                102.0,
                92.0,
                0,
                None,
            ),
            (
                "eva-guid",
                "Eva",
                "eva-guid",
                "331.l.74270.t.5",
                "Eva Unit",
                "Jason",
                "owner-guid",
                "owner-guid",
                2014,
                1,
                92.0,
                102.0,
                0,
                None,
            ),
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "348.l.8622.t.1",
                "Fcuk It'ers",
                "Fay",
                "fay-guid",
                "fay-guid",
                2015,
                1,
                103.0,
                93.0,
                0,
                None,
            ),
            (
                "fay-guid",
                "Fay",
                "fay-guid",
                "348.l.8622.t.2",
                "Fay Area",
                "Jason",
                "owner-guid",
                "owner-guid",
                2015,
                1,
                93.0,
                103.0,
                0,
                None,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        db_conn = runner._get_connection()
        owner_rows = db_conn.execute(
            """
            SELECT year, team_key, team_name, franchise_id
            FROM public.matchup
            WHERE manager_guid = 'owner-guid'
            ORDER BY year, team_key
            """
        ).fetchall()
        first_snapshot = db_conn.execute(
            """
            SELECT
                year,
                week,
                team_key,
                manager,
                manager_guid,
                team_name,
                franchise_id,
                opponent,
                opponent_guid,
                opponent_franchise_id,
                franchise_name
            FROM public.matchup
            ORDER BY year, week, team_key
            """
        ).fetchall()

        runner.resolve_hidden_managers()
        second_snapshot = db_conn.execute(
            """
            SELECT
                year,
                week,
                team_key,
                manager,
                manager_guid,
                team_name,
                franchise_id,
                opponent,
                opponent_guid,
                opponent_franchise_id,
                franchise_name
            FROM public.matchup
            ORDER BY year, week, team_key
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert owner_rows == [
        (2012, "273.l.211043.t.1", "The Irate Irish", "owner-guid_1"),
        (2012, "273.l.211043.t.10", "Fcuk It'ers", "owner-guid_2"),
        (2013, "314.l.170659.t.1", "Pierre's Pals", "owner-guid_1"),
        (2014, "331.l.74270.t.4", "Reseated Primary", "owner-guid_1"),
        (2015, "348.l.8622.t.1", "Fcuk It'ers", "owner-guid_2"),
    ]
    assert second_snapshot == first_snapshot


def test_resolve_hidden_managers_reuses_existing_franchise_identity_registry(tmp_path):
    db_name = "matchup_multi_team_owner_registry_reuse_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "273.l.211043.t.1",
                "The Irate Irish",
                "Bob",
                "bob-guid",
                "bob-guid",
                2012,
                1,
                100.0,
                90.0,
                0,
                None,
            ),
            (
                "bob-guid",
                "Bob",
                "bob-guid",
                "273.l.211043.t.2",
                "Bobcats",
                "Jason",
                "owner-guid",
                "owner-guid",
                2012,
                1,
                90.0,
                100.0,
                0,
                None,
            ),
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "273.l.211043.t.10",
                "Fcuk It'ers",
                "Cal",
                "cal-guid",
                "cal-guid",
                2012,
                1,
                110.0,
                80.0,
                0,
                None,
            ),
            (
                "cal-guid",
                "Cal",
                "cal-guid",
                "273.l.211043.t.3",
                "Calzone",
                "Jason",
                "owner-guid",
                "owner-guid",
                2012,
                1,
                80.0,
                110.0,
                0,
                None,
            ),
        ],
    )
    conn.execute(
        FRANCHISE_IDENTITY_REGISTRY_DDL.replace("franchise_identity_registry", "public.franchise_identity_registry", 1)
    )
    conn.executemany(
        """
        INSERT INTO public.franchise_identity_registry (
            db_name, platform, base_franchise_id, manager_guid, identity_key, branch_key,
            resolved_franchise_id, team_index, anchor_year, anchor_week, anchor_team_name,
            anchor_manager, anchor_team_key, anchor_team_slot, known_team_names,
            known_manager_names, known_team_slots, active_years, created_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [
            (
                db_name,
                "yahoo",
                "owner-guid",
                "owner-guid",
                "team_slot:1",
                "team_slot:1",
                "owner-guid_5",
                5,
                2012,
                1,
                "The Irate Irish",
                "Jason",
                "273.l.211043.t.1",
                "1",
                "The Irate Irish",
                "Jason",
                "1",
                "2012",
            ),
            (
                db_name,
                "yahoo",
                "owner-guid",
                "owner-guid",
                "team_slot:10",
                "team_slot:10",
                "owner-guid_9",
                9,
                2012,
                1,
                "Fcuk It'ers",
                "Jason",
                "273.l.211043.t.10",
                "10",
                "Fcuk It'ers",
                "Jason",
                "10",
                "2012",
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        db_conn = runner._get_connection()
        owner_rows = db_conn.execute(
            """
            SELECT team_key, franchise_id
            FROM public.matchup
            WHERE manager_guid = 'owner-guid'
            ORDER BY team_key
            """
        ).fetchall()
        audit_rows = db_conn.execute("SELECT COUNT(*) FROM public.franchise_identity_audit").fetchone()[0]
        first_snapshot = db_conn.execute(
            """
            SELECT year, week, team_key, manager, team_name, franchise_id, opponent_franchise_id
            FROM public.matchup
            ORDER BY year, week, team_key
            """
        ).fetchall()

        runner.resolve_hidden_managers()
        second_snapshot = db_conn.execute(
            """
            SELECT year, week, team_key, manager, team_name, franchise_id, opponent_franchise_id
            FROM public.matchup
            ORDER BY year, week, team_key
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert owner_rows == [
        ("273.l.211043.t.1", "owner-guid_5"),
        ("273.l.211043.t.10", "owner-guid_9"),
    ]
    assert audit_rows == 2
    assert second_snapshot == first_snapshot


def test_resolve_hidden_managers_uses_registry_without_local_simultaneous_week(tmp_path):
    db_name = "matchup_multi_team_owner_registry_quick_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_franchise_id VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "owner-guid",
                "Jason",
                "owner-guid",
                "414.l.999.t.10",
                "Fcuk It'ers",
                "Bob",
                "bob-guid",
                "bob-guid",
                2025,
                1,
                110.0,
                90.0,
                0,
                None,
            ),
            (
                "bob-guid",
                "Bob",
                "bob-guid",
                "414.l.999.t.2",
                "Bobcats",
                "Jason",
                "owner-guid",
                "owner-guid",
                2025,
                1,
                90.0,
                110.0,
                0,
                None,
            ),
        ],
    )
    conn.execute(
        FRANCHISE_IDENTITY_REGISTRY_DDL.replace("franchise_identity_registry", "public.franchise_identity_registry", 1)
    )
    conn.executemany(
        """
        INSERT INTO public.franchise_identity_registry (
            db_name, platform, base_franchise_id, manager_guid, identity_key, branch_key,
            resolved_franchise_id, team_index, anchor_year, anchor_week, anchor_team_name,
            anchor_manager, anchor_team_key, anchor_team_slot, known_team_names,
            known_manager_names, known_team_slots, active_years, created_at, updated_at
        )
        VALUES (?, 'yahoo', 'owner-guid', 'owner-guid', ?, ?, ?, ?, 2012, 1, ?, 'Jason', ?, ?, ?, 'Jason', ?, '2012', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [
            (
                db_name,
                "team_slot:1",
                "team_slot:1",
                "owner-guid_5",
                5,
                "The Irate Irish",
                "273.l.211043.t.1",
                "1",
                "The Irate Irish",
                "1",
            ),
            (
                db_name,
                "team_slot:10",
                "team_slot:10",
                "owner-guid_9",
                9,
                "Fcuk It'ers",
                "273.l.211043.t.10",
                "10",
                "Fcuk It'ers",
                "10",
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        db_conn = runner._get_connection()
        row = db_conn.execute(
            """
            SELECT franchise_id, manager
            FROM public.matchup
            WHERE manager_guid = 'owner-guid'
            """
        ).fetchone()
        audit_rows = db_conn.execute("SELECT COUNT(*) FROM public.franchise_identity_audit").fetchone()[0]
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert row == ("owner-guid_9", "Jason")
    assert audit_rows == 1


def test_resolve_hidden_managers_reuses_hidden_yahoo_registry_after_synthetic_team_ids(tmp_path):
    db_name = "matchup_hidden_yahoo_registry_after_team_ids_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_team_key VARCHAR,
            opponent_franchise_id VARCHAR,
            platform VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                db_name,
                "--",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.10",
                "Dynasty",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.2",
                "--",
                "yahoo",
                2008,
                1,
                84.0,
                80.0,
                0,
                None,
            ),
            (
                db_name,
                "--",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.2",
                "x--Zain",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.10",
                "--",
                "yahoo",
                2008,
                1,
                80.0,
                84.0,
                0,
                None,
            ),
        ],
    )
    conn.execute(
        FRANCHISE_IDENTITY_REGISTRY_DDL.replace("franchise_identity_registry", "public.franchise_identity_registry", 1)
    )
    conn.executemany(
        """
        INSERT INTO public.franchise_identity_registry (
            db_name, platform, base_franchise_id, manager_guid, identity_key, branch_key,
            resolved_franchise_id, team_index, anchor_year, anchor_week, anchor_team_name,
            anchor_manager, anchor_team_key, anchor_team_slot, known_team_names,
            known_manager_names, known_team_slots, active_years, created_at, updated_at
        )
        VALUES (?, 'yahoo', '--hidden--', '--hidden--', ?, ?, ?, ?, 2008, 1, ?, ?, ?, ?, ?, ?, ?, '2008', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """,
        [
            (
                db_name,
                "team_slot:10",
                "team_slot:10",
                "--hidden--_10",
                10,
                "Dynasty",
                "Zain Dhanani",
                "199.l.22810.t.10",
                "10",
                "Dynasty",
                "Zain Dhanani",
                "10",
            ),
            (
                db_name,
                "team_slot:2",
                "team_slot:2",
                "--hidden--_2",
                2,
                "x--Zain",
                "Zain Dhanani",
                "199.l.22810.t.2",
                "2",
                "x--Zain",
                "Zain Dhanani",
                "2",
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        db_conn = runner._get_connection()
        rows = db_conn.execute(
            """
            SELECT team_key, franchise_id, manager
            FROM public.matchup
            ORDER BY team_key
            """
        ).fetchall()
        audit_rows = db_conn.execute("SELECT COUNT(*) FROM public.franchise_identity_audit").fetchone()[0]
        first_snapshot = db_conn.execute(
            """
            SELECT team_key, franchise_id, manager, opponent_franchise_id
            FROM public.matchup
            ORDER BY team_key
            """
        ).fetchall()

        runner.resolve_hidden_managers()
        second_snapshot = db_conn.execute(
            """
            SELECT team_key, franchise_id, manager, opponent_franchise_id
            FROM public.matchup
            ORDER BY team_key
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("199.l.22810.t.10", "--hidden--_10", "Zain Dhanani - Dynasty"),
        ("199.l.22810.t.2", "--hidden--_2", "Zain Dhanani - x--Zain"),
    ]
    assert audit_rows == 2
    assert second_snapshot == first_snapshot


def test_resolve_hidden_managers_preserves_visible_yahoo_matchup_names(tmp_path):
    db_name = "matchup_yahoo_visible_names_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_team_key VARCHAR,
            opponent_franchise_id VARCHAR,
            platform VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                db_name,
                "fid_blitz",
                "Shawn",
                "shared-yahoo-guid",
                "423.l.303482.t.3",
                "Blitzkrieg",
                "Ace",
                "ace-guid",
                "423.l.303482.t.9",
                "fid_ace",
                "yahoo",
                2023,
                15,
                109.38,
                123.68,
                0,
                None,
            ),
            (
                db_name,
                "fid_ace",
                "Ace",
                "ace-guid",
                "423.l.303482.t.9",
                "Parks of Carrollton",
                "Shawn",
                "shared-yahoo-guid",
                "423.l.303482.t.3",
                "fid_blitz",
                "yahoo",
                2023,
                15,
                123.68,
                109.38,
                0,
                None,
            ),
            (
                db_name,
                "fid_blitz",
                "Zain Dhanani",
                "shared-yahoo-guid",
                "449.l.5702.t.3",
                "Blitzkrieg",
                "Rahim",
                "rahim-guid",
                "449.l.5702.t.6",
                "fid_rahim",
                "yahoo",
                2024,
                15,
                109.38,
                109.30,
                0,
                None,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT year, franchise_id, manager, opponent
                FROM public.matchup
                WHERE year = 2023
                ORDER BY franchise_id
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        (2023, "fid_ace", "Ace", "Shawn"),
        (2023, "fid_blitz", "Shawn", "Ace"),
    ]


def test_resolve_hidden_managers_still_updates_hidden_yahoo_matchup_names(tmp_path):
    db_name = "matchup_yahoo_hidden_name_still_resolves_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_team_key VARCHAR,
            opponent_franchise_id VARCHAR,
            platform VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                db_name,
                "fid_private",
                "--hidden--",
                "--",
                "423.l.303482.t.8",
                "Private Team",
                "Ali",
                "ali-guid",
                "423.l.303482.t.4",
                "fid_ali",
                "yahoo",
                2023,
                1,
                80.0,
                91.0,
                0,
                None,
            ),
            (
                db_name,
                "fid_private",
                "Ilan",
                "--",
                "449.l.5702.t.8",
                "Private Team",
                "Ali",
                "ali-guid",
                "449.l.5702.t.4",
                "fid_ali",
                "yahoo",
                2024,
                1,
                93.0,
                88.0,
                0,
                None,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT year, manager
                FROM public.matchup
                WHERE franchise_id = 'fid_private'
                ORDER BY year
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [(2023, "Ilan"), (2024, "Ilan")]


def test_resolve_hidden_managers_preserves_hidden_yahoo_nicknames_and_disambiguates_duplicates(tmp_path):
    db_name = "matchup_yahoo_hidden_duplicate_nicknames_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_team_key VARCHAR,
            opponent_franchise_id VARCHAR,
            platform VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                db_name,
                "--",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.10",
                "Dynasty",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.2",
                "--",
                "yahoo",
                2008,
                1,
                84.0,
                80.0,
                0,
                None,
            ),
            (
                db_name,
                "--",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.2",
                "x--Zain",
                "Zain Dhanani",
                "--hidden--",
                "199.l.22810.t.10",
                "--",
                "yahoo",
                2008,
                1,
                80.0,
                84.0,
                0,
                None,
            ),
            (
                db_name,
                "--",
                "--hidden--",
                "--hidden--",
                "199.l.22810.t.12",
                "Private Team",
                "Ali S",
                "--hidden--",
                "199.l.22810.t.11",
                "--",
                "yahoo",
                2008,
                1,
                55.0,
                72.0,
                0,
                None,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT team_name, franchise_id, manager
                FROM public.matchup
                ORDER BY team_name
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Dynasty", "hidden_dynasty", "Zain Dhanani - Dynasty"),
        ("Private Team", "hidden_private_team", "Private Team"),
        ("x--Zain", "hidden_x--zain", "Zain Dhanani - x--Zain"),
    ]


def test_resolve_hidden_managers_uses_yahoo_opponent_team_key_for_duplicate_scores(tmp_path):
    db_name = "matchup_yahoo_opponent_team_key_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            opponent VARCHAR,
            opponent_guid VARCHAR,
            opponent_team_key VARCHAR,
            opponent_franchise_id VARCHAR,
            platform VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_bye_week INTEGER,
            franchise_name VARCHAR
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                db_name,
                "fid_a",
                "A",
                "guid-a",
                "449.l.1.t.1",
                "A Team",
                "B",
                "guid-b",
                "449.l.1.t.2",
                None,
                "yahoo",
                2024,
                15,
                100.0,
                90.0,
                0,
                None,
            ),
            (
                db_name,
                "fid_b",
                "B",
                "guid-b",
                "449.l.1.t.2",
                "B Team",
                "A",
                "guid-a",
                "449.l.1.t.1",
                None,
                "yahoo",
                2024,
                15,
                90.0,
                100.0,
                0,
                None,
            ),
            (
                db_name,
                "fid_c",
                "C",
                "guid-c",
                "449.l.1.t.3",
                "C Team",
                "D",
                "guid-d",
                "449.l.1.t.4",
                None,
                "yahoo",
                2024,
                15,
                100.0,
                90.0,
                0,
                None,
            ),
            (
                db_name,
                "fid_d",
                "D",
                "guid-d",
                "449.l.1.t.4",
                "D Team",
                "C",
                "guid-c",
                "449.l.1.t.3",
                None,
                "yahoo",
                2024,
                15,
                90.0,
                100.0,
                0,
                None,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.resolve_hidden_managers()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT franchise_id, opponent_franchise_id
                FROM public.matchup
                ORDER BY franchise_id
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("fid_a", "fid_b"),
        ("fid_b", "fid_a"),
        ("fid_c", "fid_d"),
        ("fid_d", "fid_c"),
    ]


def test_repair_matchup_symmetry_does_not_synthesize_yahoo_rows(tmp_path):
    db_name = "matchup_yahoo_symmetry_skip_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            platform VARCHAR,
            league_id VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            manager_guid VARCHAR,
            opponent_guid VARCHAR,
            team_key VARCHAR,
            opponent_team_key VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            margin DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_bye_week INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            championship INTEGER,
            total_matchup_score DOUBLE,
            close_margin INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.matchup VALUES (
            ?, 'yahoo', '449.l.5702', 'fid_a', 'fid_b', 'A', 'B',
            'guid-a', 'guid-b', '449.l.5702.t.1', '449.l.5702.t.2',
            2024, 15, 100.0, 90.0, 10.0, 1, 0, 0, 0, 1, 0, 0, 190.0, 0
        )
        """,
        [db_name],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        affected = runner.repair_matchup_symmetry()
        row_count = runner._get_connection().execute("SELECT COUNT(*) FROM public.matchup").fetchone()[0]
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert affected == 0
    assert row_count == 1


def test_repair_matchup_symmetry_still_synthesizes_non_yahoo_rows(tmp_path):
    db_name = "matchup_sleeper_symmetry_repair_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            platform VARCHAR,
            league_id VARCHAR,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            manager_guid VARCHAR,
            opponent_guid VARCHAR,
            team_key VARCHAR,
            opponent_team_key VARCHAR,
            year INTEGER,
            week INTEGER,
            team_points DOUBLE,
            opponent_points DOUBLE,
            margin DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_bye_week INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            championship INTEGER,
            total_matchup_score DOUBLE,
            close_margin INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.matchup VALUES (
            ?, 'sleeper', 'league-1', 'fid_a', 'fid_b', 'A', 'Departed B',
            'guid-a', 'guid-b', '1', NULL,
            2024, 7, 100.0, 90.0, 10.0, 1, 0, 0, 0, 0, 0, 0, 190.0, 0
        )
        """,
        [db_name],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        affected = runner.repair_matchup_symmetry()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT franchise_id, opponent_franchise_id, manager, opponent,
                       team_points, opponent_points, win, loss, team_key, opponent_team_key
                FROM public.matchup
                ORDER BY franchise_id
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert affected == 1
    assert rows == [
        ("fid_a", "fid_b", "A", "Departed B", 100.0, 90.0, 1, 0, "1", None),
        ("fid_b", "fid_a", "Departed B", "A", 90.0, 100.0, 0, 1, None, "1"),
    ]


def test_populate_franchise_id_prefers_team_key_over_raw_guid_for_multi_team_owner(tmp_path):
    db_name = "matchup_multi_team_player_backfill_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("owner-guid_1", "Mc Bidmaster - OPEN TEAM 1", "owner-guid", "371.l.2116.t.2", "OPEN TEAM 1", 2017, 1),
            ("owner-guid_2", "Mc Bidmaster - OPEN TEAM 2", "owner-guid", "371.l.2116.t.6", "OPEN TEAM 2", 2017, 1),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (2017, 1, "Mc Bidmaster", "owner-guid", "owner-guid", "371.l.2116.t.2", None),
            (2017, 1, "Mc Bidmaster", "owner-guid", "owner-guid", "371.l.2116.t.6", None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.populate_franchise_id()
        rows = (
            runner._get_connection()
            .execute(
                """
            SELECT team_key, franchise_id
            FROM public.player_fantasy
            ORDER BY team_key
            """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("371.l.2116.t.2", "owner-guid_1"),
        ("371.l.2116.t.6", "owner-guid_2"),
    ]


def test_populate_franchise_id_does_not_clobber_guid_backfill_when_manager_name_is_wrong(tmp_path):
    db_name = "matchup_manager_name_clobber_guard_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("698770866359586816", "DavidCollier", "698770866359586816", "2", "Bijan Bogdanovic", 2023, 1),
            ("700468029472989184", "JornLawson", "700468029472989184", "8", "", 2023, 1),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (2023, 1, "JornLawson", "698770866359586816", "698770866359586816", "2", None),
            (2023, 1, "JornLawson", "700468029472989184", "700468029472989184", "8", None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.populate_franchise_id()
        rows = (
            runner._get_connection()
            .execute(
                """
            SELECT team_key, manager, manager_guid, franchise_id
            FROM public.player_fantasy
            ORDER BY team_key
            """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("2", "JornLawson", "698770866359586816", "698770866359586816"),
        ("8", "JornLawson", "700468029472989184", "700468029472989184"),
    ]


def test_populate_franchise_id_uses_year_scoped_team_key_when_same_week_matchup_is_missing(tmp_path):
    db_name = "matchup_team_key_year_scope_backfill_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("owner-guid_4", "Mc Bidmaster - OPEN TEAM 4", "owner-guid", "371.l.2116.t.2", "OPEN TEAM 4", 2017, 16),
            ("owner-guid_2", "Mc Bidmaster - OPEN TEAM 2", "owner-guid", "371.l.2116.t.6", "OPEN TEAM 2", 2017, 16),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (2017, 14, "Mc Bidmaster", "owner-guid", "owner-guid", "371.l.2116.t.2", None),
            (2017, 14, "Mc Bidmaster", "owner-guid", "owner-guid", "371.l.2116.t.6", None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.populate_franchise_id()
        rows = (
            runner._get_connection()
            .execute(
                """
            SELECT team_key, week, franchise_id
            FROM public.player_fantasy
            ORDER BY team_key
            """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("371.l.2116.t.2", 14, "owner-guid_4"),
        ("371.l.2116.t.6", 14, "owner-guid_2"),
    ]


def test_populate_franchise_id_falls_back_to_unambiguous_manager_when_old_yahoo_missing_guid(tmp_path):
    db_name = "matchup_old_yahoo_hidden_owner_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            ("hidden_face", "FACE", None, None, "FACE", 2005, 1),
            ("hidden_yup", "the yup yup yupppers", None, None, "the yup yup yupppers", 2005, 1),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?)",
        [
            (2005, 1, "Face", None, None, "124.l.10444.t.4", None),
            (2005, 1, "The Yup Yup Yupppers", None, None, "124.l.10444.t.10", None),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.populate_franchise_id()
        rows = (
            runner._get_connection()
            .execute(
                """
            SELECT manager, franchise_id
            FROM public.player_fantasy
            ORDER BY manager
            """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Face", "hidden_face"),
        ("The Yup Yup Yupppers", "hidden_yup"),
    ]


def test_populate_franchise_id_replaces_invalid_placeholder_franchise_ids(tmp_path):
    db_name = "matchup_placeholder_franchise_backfill_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("hidden_moneyball", "MoneyBall", "--", "MoneyBall", 2012, 1),
            ("hidden_pale_horse", "The Pale Horse", "--", "The Pale Horse", 2012, 1),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_name VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?)",
        [
            (2012, 1, "Moneyball", "--hidden--", "--hidden--", None),
            (2012, 1, "The Pale Horse", None, "<NA>", "The Pale Horse"),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.draft (
            year INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            franchise_id VARCHAR
        )
        """
    )
    conn.execute("INSERT INTO public.draft VALUES (2012, 'Moneyball', '--hidden--', 'MoneyBall', '--hidden--')")
    conn.execute(
        """
        CREATE TABLE public.transactions (
            year INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            franchise_id VARCHAR,
            source_manager VARCHAR,
            source_manager_guid VARCHAR,
            source_team_name VARCHAR,
            source_franchise_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.transactions
        VALUES (
            2012,
            'Moneyball',
            '--hidden--',
            'MoneyBall',
            '--hidden--',
            'The Pale Horse',
            '--hidden--',
            'The Pale Horse',
            '--hidden--'
        )
        """
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.populate_franchise_id()
        db_conn = runner._get_connection()
        player_rows = db_conn.execute(
            """
            SELECT manager, franchise_id
            FROM public.player_fantasy
            ORDER BY manager
            """
        ).fetchall()
        draft_rows = db_conn.execute("SELECT manager, franchise_id FROM public.draft").fetchall()
        txn_rows = db_conn.execute(
            """
            SELECT manager, franchise_id, source_manager, source_franchise_id
            FROM public.transactions
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert player_rows == [
        ("Moneyball", "hidden_moneyball"),
        ("The Pale Horse", "hidden_pale_horse"),
    ]
    assert draft_rows == [("Moneyball", "hidden_moneyball")]
    assert txn_rows == [("Moneyball", "hidden_moneyball", "The Pale Horse", "hidden_pale_horse")]


def test_populate_franchise_id_repairs_stale_espn_trade_perspective_from_guid(tmp_path):
    """A mirrored trade must use the year-scoped owner identity on both legs."""
    db_name = "espn_trade_owner_repair_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            year INTEGER,
            week INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?)",
        [
            ("owner_a_0", "Ryan", "owner-a", "Team A", 2024, 1),
            ("espn_3", "Unknown", "hidden-owner", "CPU Team 1", 2024, 1),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_name VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE public.transactions (
            transaction_id VARCHAR,
            year INTEGER,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            franchise_id VARCHAR,
            source_manager VARCHAR,
            source_manager_guid VARCHAR,
            source_team_name VARCHAR,
            source_franchise_id VARCHAR,
            trade_direction VARCHAR,
            player VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "trade-1", 2024, "Ryan", "owner-a", "Team A", "owner_a_0",
                "Unknown", "hidden-owner", "CPU Team 1", "espn_3", "received", "Player A",
            ),
            (
                "trade-1", 2024, "Ryan", "hidden-owner", "CPU Team 1", "owner_a_0",
                "Ryan", "owner-a", "Team A", "owner_a_0", "sent", "Player A",
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        runner.populate_franchise_id()
        rows = runner._get_connection().execute(
            """
            SELECT trade_direction, franchise_id, source_franchise_id
            FROM public.transactions
            ORDER BY trade_direction
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("received", "owner_a_0", "espn_3"),
        ("sent", "espn_3", "owner_a_0"),
    ]


def test_ensure_missing_player_stubs_prunes_redundant_stubs_after_franchise_backfill(tmp_path):
    db_name = "matchup_redundant_player_stub_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            team_name VARCHAR,
            team_key VARCHAR,
            year INTEGER,
            week INTEGER,
            platform VARCHAR,
            league_id VARCHAR,
            team_points DOUBLE,
            opponent VARCHAR,
            opponent_points DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "fid_nabeel",
                "Nabeel",
                "--hidden--",
                "Nabeel",
                "79.l.182382.t.12",
                2003,
                1,
                "yahoo",
                "79.l.182382",
                60.0,
                "Danish",
                45.0,
            ),
            (
                "fid_missing",
                "Missing Manager",
                "--hidden--",
                "Missing Team",
                "79.l.182382.t.99",
                2003,
                1,
                "yahoo",
                "79.l.182382",
                0.0,
                "Bye",
                0.0,
            ),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            year INTEGER,
            week INTEGER,
            cumulative_week BIGINT,
            manager_week VARCHAR,
            manager VARCHAR,
            manager_guid VARCHAR,
            franchise_id VARCHAR,
            team_key VARCHAR,
            team_name VARCHAR,
            platform VARCHAR,
            league_id VARCHAR,
            fantasy_points DOUBLE,
            points DOUBLE,
            is_started INTEGER,
            is_rostered INTEGER,
            position VARCHAR,
            fantasy_position VARCHAR,
            team_points DOUBLE,
            opponent VARCHAR,
            opponent_points DOUBLE
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                2003,
                1,
                200301,
                "fid_nabeel_2003_1",
                "Nabeel",
                "--hidden--",
                "fid_nabeel",
                None,
                "Nabeel",
                "yahoo",
                "79.l.182382",
                0.0,
                0.0,
                0,
                1,
                "STUB",
                "BN",
                60.0,
                "Danish",
                45.0,
            ),
            (
                2003,
                1,
                None,
                None,
                "Nabeel",
                "--hidden--",
                "--hidden--",
                "79.l.182382.t.12",
                None,
                "yahoo",
                "79.l.182382",
                17.0,
                17.0,
                1,
                1,
                "QB",
                "QB",
                None,
                None,
                None,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.populate_franchise_id()
        runner.ensure_missing_player_stubs()
        runner.ensure_missing_player_stubs()
        db_conn = runner._get_connection()
        rows = db_conn.execute(
            """
            SELECT manager, franchise_id, team_key, position, fantasy_points, team_points
            FROM public.player_fantasy
            ORDER BY franchise_id, position
            """
        ).fetchall()
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Missing Manager", "fid_missing", "79.l.182382.t.99", "STUB", 0.0, 0.0),
        ("Nabeel", "fid_nabeel", "79.l.182382.t.12", "QB", 17.0, None),
    ]


def test_matchup_to_player_propagates_is_bye_week_to_player_rows(tmp_path):
    db_name = "matchup_to_player_bye_flag_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            franchise_id VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            margin DOUBLE,
            is_bye_week INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            final_playoff_seed INTEGER,
            playoff_seed INTEGER,
            champion INTEGER,
            sacko INTEGER,
            is_championship INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "fid_active",
                "Finalist",
                2024,
                17,
                "Rival",
                118.4,
                109.1,
                9.3,
                0,
                1,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "fid_bye",
                "Phantom Bye",
                2024,
                17,
                None,
                None,
                None,
                None,
                1,
                0,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ],
    )
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            franchise_id VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            win INTEGER,
            loss INTEGER,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            margin DOUBLE,
            is_bye_week INTEGER,
            final_playoff_seed INTEGER,
            playoff_seed INTEGER,
            championship INTEGER,
            is_championship INTEGER,
            champion INTEGER,
            sacko INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            is_playoffs INTEGER,
            is_consolation INTEGER
        )
        """
    )
    conn.executemany(
        "INSERT INTO public.player_fantasy VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [
            (
                "fid_active",
                "Finalist",
                2024,
                17,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
            (
                "fid_bye",
                "Phantom Bye",
                2024,
                17,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.matchup_to_player()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT franchise_id, manager, is_bye_week, is_playoffs, is_consolation, opponent
                FROM public.player_fantasy
                ORDER BY franchise_id
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("fid_active", "Finalist", 0, 1, 0, "Rival"),
        ("fid_bye", "Phantom Bye", 1, 0, 0, None),
    ]


def test_shape_playoff_bracket_generic_tracer_respects_points_standings(tmp_path):
    db_name = "points_seeded_bracket_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN,
            playoff_seeding_rule VARCHAR,
            playoff_seeding_rule_by INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2025, 4, 14, 15, 4, 0, FALSE, 0, FALSE, FALSE, 'TOTAL_POINTS_SCORED', 0)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_bye_week INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )

    def row(week, fid, opp_fid, manager, opponent, points, opp_points):
        win = int(points > opp_points)
        return (
            2025,
            week,
            fid,
            opp_fid,
            manager,
            opponent,
            points,
            opp_points,
            win,
            int(not win),
            0,
            0,
            0,
            0,
            0,
            False,
            0,
            0,
            None,
            None,
            None,
            None,
        )

    rows = []
    regular_specs = [
        ("fid_a", "Record One", 12, 90.0),
        ("fid_b", "Record Two", 10, 85.0),
        ("fid_c", "Points One", 6, 150.0),
        ("fid_d", "Points Two", 5, 140.0),
    ]
    for fid, manager, wins, points in regular_specs:
        for week in range(1, 14):
            opp_points = points - 10 if week <= wins else points + 10
            rows.append(row(week, fid, f"opp_{fid}", manager, "Schedule Opponent", points, opp_points))

    rows.extend(
        [
            row(14, "fid_c", "fid_b", "Points One", "Record Two", 160.0, 100.0),
            row(14, "fid_b", "fid_c", "Record Two", "Points One", 100.0, 160.0),
            row(14, "fid_d", "fid_a", "Points Two", "Record One", 155.0, 120.0),
            row(14, "fid_a", "fid_d", "Record One", "Points Two", 120.0, 155.0),
            row(15, "fid_c", "fid_d", "Points One", "Points Two", 170.0, 160.0),
            row(15, "fid_d", "fid_c", "Points Two", "Points One", 160.0, 170.0),
        ]
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 1
        seeds = dict(
            runner._get_connection()
            .execute(
                """
                SELECT franchise_id, MIN(final_playoff_seed)
                FROM public.matchup
                GROUP BY franchise_id
                """
            )
            .fetchall()
        )
        final_rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, playoff_round, is_championship, champion
                FROM public.matchup
                WHERE week = 15
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert seeds == {"fid_c": 1, "fid_d": 2, "fid_a": 3, "fid_b": 4}
    assert final_rows == [
        ("Points One", "championship", True, 1),
        ("Points Two", "championship", True, 0),
    ]


def test_shape_playoff_bracket_preserves_existing_tiers_from_ddl(tmp_path):
    db_name = "existing_tier_bracket_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2014, 2, 15, 15, 12, 0, FALSE, 0, FALSE, FALSE)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                2014,
                15,
                "fid_ross",
                "fid_master",
                "Ross",
                "master",
                206.58,
                146.22,
                1,
                0,
                0,
                1,
                0,
                1,
                None,
                1,
                None,
                None,
                None,
                None,
                3,
            ),
            (
                2014,
                15,
                "fid_master",
                "fid_ross",
                "master",
                "Ross",
                146.22,
                206.58,
                0,
                1,
                0,
                1,
                0,
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                4,
            ),
            (
                2014,
                15,
                "fid_james",
                "fid_jordan",
                "James",
                "Jordan",
                300.0,
                10.0,
                1,
                0,
                0,
                0,
                1,
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                1,
            ),
            (
                2014,
                15,
                "fid_jordan",
                "fid_james",
                "Jordan",
                "James",
                10.0,
                300.0,
                0,
                1,
                0,
                0,
                1,
                1,
                None,
                None,
                None,
                None,
                None,
                None,
                2,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 1
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, is_playoffs, is_consolation, is_championship, champion,
                       playoff_round, consolation_round
                FROM public.matchup
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    by_manager = {row[0]: row[1:] for row in rows}
    assert by_manager["Ross"] == (1, 0, True, 1, "championship", None)
    assert by_manager["master"] == (1, 0, True, 0, "championship", None)
    assert by_manager["James"] == (0, 1, False, 0, None, "consolation_final")
    assert by_manager["Jordan"] == (0, 1, False, 0, None, "consolation_final")


def test_shape_playoff_bracket_traces_when_only_consolation_tiers_exist(tmp_path):
    db_name = "consolation_only_tier_bracket_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2024, 2, 2, 2, 4, 0, FALSE, 0, FALSE, FALSE)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )

    def row(week, fid, opp_fid, manager, opponent, points, opp_points, is_consolation=0):
        win = int(points > opp_points)
        return (
            2024,
            week,
            fid,
            opp_fid,
            manager,
            opponent,
            points,
            opp_points,
            win,
            int(not win),
            0,
            0,
            is_consolation,
            is_consolation,
            False,
            0,
            0,
            None,
            None,
            "consolation_final" if is_consolation else None,
            None,
        )

    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row(1, "fid_a", "fid_c", "Alpha", "Charlie", 120.0, 80.0),
            row(1, "fid_c", "fid_a", "Charlie", "Alpha", 80.0, 120.0),
            row(1, "fid_b", "fid_d", "Bravo", "Delta", 110.0, 70.0),
            row(1, "fid_d", "fid_b", "Delta", "Bravo", 70.0, 110.0),
            row(2, "fid_a", "fid_b", "Alpha", "Bravo", 131.0, 126.0),
            row(2, "fid_b", "fid_a", "Bravo", "Alpha", 126.0, 131.0),
            row(2, "fid_c", "fid_d", "Charlie", "Delta", 90.0, 85.0, is_consolation=1),
            row(2, "fid_d", "fid_c", "Delta", "Charlie", 85.0, 90.0, is_consolation=1),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 1
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, is_playoffs, is_consolation, is_championship, champion,
                       playoff_round, consolation_round
                FROM public.matchup
                WHERE week = 2
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    by_manager = {row[0]: row[1:] for row in rows}
    assert by_manager["Alpha"] == (1, 0, True, 1, "championship", None)
    assert by_manager["Bravo"] == (1, 0, True, 0, "championship", None)
    assert by_manager["Charlie"][0] == 0
    assert by_manager["Delta"][0] == 0


def test_shape_playoff_bracket_preserves_existing_eight_seed_champion_without_platform_source(tmp_path):
    db_name = "existing_champion_bracket_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2025, 8, 15, 17, 8, 0, FALSE, 0, FALSE, FALSE)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )

    def matchup_row(
        week,
        fid,
        opp_fid,
        manager,
        opponent,
        points,
        opp_points,
        win,
        is_playoffs,
        is_consolation,
        is_championship,
        champion,
        final_seed,
    ):
        return (
            2025,
            week,
            fid,
            opp_fid,
            manager,
            opponent,
            points,
            opp_points,
            win,
            0 if win else 1,
            0,
            is_playoffs,
            is_consolation,
            1 if is_playoffs or is_consolation else 0,
            is_championship,
            champion,
            None,
            None,
            None,
            None,
            final_seed,
        )

    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            matchup_row(14, "fid_1", "fid_8", "Seed 1", "Seed 8 Champion", 200.0, 10.0, 1, 0, 0, None, 0, 1),
            matchup_row(14, "fid_8", "fid_1", "Seed 8 Champion", "Seed 1", 10.0, 200.0, 0, 0, 0, None, 0, 8),
            matchup_row(14, "fid_2", "fid_7", "Seed 2", "Seed 7 Finalist", 190.0, 20.0, 1, 0, 0, None, 0, 2),
            matchup_row(14, "fid_7", "fid_2", "Seed 7 Finalist", "Seed 2", 20.0, 190.0, 0, 0, 0, None, 0, 7),
            matchup_row(14, "fid_3", "fid_6", "Seed 3", "Seed 6", 180.0, 30.0, 1, 0, 0, None, 0, 3),
            matchup_row(14, "fid_6", "fid_3", "Seed 6", "Seed 3", 30.0, 180.0, 0, 0, 0, None, 0, 6),
            matchup_row(14, "fid_4", "fid_5", "Seed 4", "Seed 5", 170.0, 40.0, 1, 0, 0, None, 0, 4),
            matchup_row(14, "fid_5", "fid_4", "Seed 5", "Seed 4", 40.0, 170.0, 0, 0, 0, None, 0, 5),
            matchup_row(17, "fid_8", "fid_7", "Seed 8 Champion", "Seed 7 Finalist", 111.0, 90.0, 1, 1, 0, None, 1, 8),
            matchup_row(17, "fid_7", "fid_8", "Seed 7 Finalist", "Seed 8 Champion", 90.0, 111.0, 0, 1, 0, None, 0, 7),
            matchup_row(17, "fid_1", "fid_2", "Seed 1", "Seed 2", 150.0, 140.0, 1, 0, 1, None, 0, 1),
            matchup_row(17, "fid_2", "fid_1", "Seed 2", "Seed 1", 140.0, 150.0, 0, 0, 1, None, 0, 2),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 1
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, final_playoff_seed, is_playoffs, is_consolation, is_championship, champion,
                       playoff_round, consolation_round
                FROM public.matchup
                WHERE week = 17
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    by_manager = {row[0]: row[1:] for row in rows}
    assert by_manager["Seed 8 Champion"] == (8, 1, 0, True, 1, "championship", None)
    assert by_manager["Seed 7 Finalist"] == (7, 1, 0, True, 0, "championship", None)
    assert by_manager["Seed 1"] == (1, 0, 1, False, 0, None, "consolation_final")
    assert by_manager["Seed 2"] == (2, 0, 1, False, 0, None, "consolation_final")


def test_shape_playoff_bracket_fills_missing_champion_from_existing_ddl_final(tmp_path):
    db_name = "existing_tiers_missing_champion_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2025, 8, 15, 17, 8, 0, FALSE, 0, FALSE, FALSE)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )

    def matchup_row(
        fid,
        opp_fid,
        manager,
        opponent,
        points,
        opp_points,
        win,
        is_playoffs,
        is_consolation,
        final_seed,
    ):
        return (
            2025,
            17,
            fid,
            opp_fid,
            manager,
            opponent,
            points,
            opp_points,
            win,
            0 if win else 1,
            0,
            is_playoffs,
            is_consolation,
            1,
            None,
            None,
            None,
            None,
            None,
            None,
            final_seed,
        )

    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            matchup_row("fid_8", "fid_7", "Seed 8 DDL Winner", "Seed 7 Finalist", 111.0, 90.0, 1, 1, 0, 8),
            matchup_row("fid_7", "fid_8", "Seed 7 Finalist", "Seed 8 DDL Winner", 90.0, 111.0, 0, 1, 0, 7),
            matchup_row("fid_1", "fid_2", "Seed 1 Placement", "Seed 2 Placement", 150.0, 140.0, 1, 0, 1, 1),
            matchup_row("fid_2", "fid_1", "Seed 2 Placement", "Seed 1 Placement", 140.0, 150.0, 0, 0, 1, 2),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 1
        first_rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, is_playoffs, is_consolation, is_championship, champion,
                       playoff_round, consolation_round
                FROM public.matchup
                ORDER BY manager
                """
            )
            .fetchall()
        )
        assert runner.shape_playoff_bracket_local() == 1
        second_rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, is_playoffs, is_consolation, is_championship, champion,
                       playoff_round, consolation_round
                FROM public.matchup
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert second_rows == first_rows
    by_manager = {row[0]: row[1:] for row in second_rows}
    assert by_manager["Seed 8 DDL Winner"] == (1, 0, True, 1, "championship", None)
    assert by_manager["Seed 7 Finalist"] == (1, 0, True, 0, "championship", None)
    assert by_manager["Seed 1 Placement"] == (0, 1, False, 0, None, "consolation_final")
    assert by_manager["Seed 2 Placement"] == (0, 1, False, 0, None, "consolation_final")


def test_detect_champions_does_not_seed_dedup_existing_api_champion_flags(tmp_path):
    db_name = "existing_api_champion_dedup_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            manager_week VARCHAR,
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            consolation_round VARCHAR,
            champion INTEGER,
            sacko INTEGER,
            final_playoff_seed INTEGER
        )
        """
    )

    def row(manager, fid, opponent, opp_fid, points, opp_points, win, champion, seed):
        return (
            f"{fid}_2025_17",
            2025,
            17,
            fid,
            opp_fid,
            manager,
            opponent,
            points,
            opp_points,
            win,
            0 if win else 1,
            1,
            0,
            None,
            champion,
            0,
            seed,
        )

    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row("API Low Seed Champ", "fid_8", "Runner Up", "fid_7", 120.0, 110.0, 1, 1, 8),
            row("Runner Up", "fid_7", "API Low Seed Champ", "fid_8", 110.0, 120.0, 0, 0, 7),
            row("Seed Dedup Temptation", "fid_1", "Placement Opp", "fid_2", 90.0, 80.0, 1, 1, 1),
            row("Placement Opp", "fid_2", "Seed Dedup Temptation", "fid_1", 80.0, 90.0, 0, 0, 2),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        runner.detect_champions_and_sackos()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, champion
                FROM public.matchup
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("API Low Seed Champ", 1),
        ("Placement Opp", 0),
        ("Runner Up", 0),
        ("Seed Dedup Temptation", 1),
    ]


def test_detect_champions_and_sackos_preserves_existing_api_sacko(tmp_path):
    db_name = "existing_sacko_bracket_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            manager_week VARCHAR,
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            consolation_round VARCHAR,
            champion INTEGER,
            sacko INTEGER
        )
        """
    )

    def row(manager, fid, opponent, opp_fid, points, opp_points, win, sacko):
        return (
            f"{fid}_2025_17",
            2025,
            17,
            fid,
            opp_fid,
            manager,
            opponent,
            points,
            opp_points,
            win,
            0 if win else 1,
            0,
            1,
            "consolation_final",
            0,
            sacko,
        )

    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            row("DDL Sacko", "fid_api", "Opponent", "fid_opponent", 120.0, 130.0, 0, 1),
            row("Opponent", "fid_opponent", "DDL Sacko", "fid_api", 130.0, 120.0, 1, 0),
            row("Lowest Score Lure", "fid_lure", "Lure Opp", "fid_lure_opp", 40.0, 80.0, 0, 0),
            row("Lure Opp", "fid_lure_opp", "Lowest Score Lure", "fid_lure", 80.0, 40.0, 1, 0),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        runner.detect_champions_and_sackos()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, sacko
                FROM public.matchup
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("DDL Sacko", 1),
        ("Lowest Score Lure", 0),
        ("Lure Opp", 0),
        ("Opponent", 0),
    ]


def test_shape_playoff_bracket_does_not_infer_champion_before_season_complete(tmp_path):
    db_name = "incomplete_no_champion_bracket_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2026, 2, 17, 17, 2, 0, FALSE, 0, FALSE, FALSE)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                2026,
                16,
                "fid_1",
                "fid_2",
                "Seed 1",
                "Seed 2",
                150.0,
                100.0,
                1,
                0,
                0,
                0,
                0,
                0,
                False,
                0,
                0,
                None,
                None,
                None,
                1,
            ),
            (
                2026,
                16,
                "fid_2",
                "fid_1",
                "Seed 2",
                "Seed 1",
                100.0,
                150.0,
                0,
                1,
                0,
                0,
                0,
                0,
                False,
                0,
                0,
                None,
                None,
                None,
                2,
            ),
            (
                2026,
                17,
                "fid_1",
                "fid_2",
                "Seed 1",
                "Seed 2",
                0.0,
                0.0,
                0,
                0,
                0,
                1,
                0,
                1,
                True,
                0,
                0,
                None,
                None,
                None,
                1,
            ),
            (
                2026,
                17,
                "fid_2",
                "fid_1",
                "Seed 2",
                "Seed 1",
                0.0,
                0.0,
                0,
                0,
                0,
                1,
                0,
                1,
                True,
                0,
                0,
                None,
                None,
                None,
                2,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 0
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, champion, is_playoffs, is_championship, playoff_round
                FROM public.matchup
                WHERE week = 17
                ORDER BY manager
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows == [
        ("Seed 1", 0, 1, True, None),
        ("Seed 2", 0, 1, True, None),
    ]


def test_shape_playoff_bracket_no_playoff_season_marks_seed_one_champion(tmp_path):
    db_name = "no_playoff_bracket_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2024, 0, 3, 2, 4, 0, FALSE, 0, FALSE, FALSE)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                2024,
                1,
                "fid_1",
                "fid_4",
                "Seed One",
                "Seed Four",
                110.0,
                90.0,
                1,
                0,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                1,
            ),
            (
                2024,
                1,
                "fid_4",
                "fid_1",
                "Seed Four",
                "Seed One",
                90.0,
                110.0,
                0,
                1,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                4,
            ),
            (
                2024,
                1,
                "fid_2",
                "fid_3",
                "Seed Two",
                "Seed Three",
                105.0,
                95.0,
                1,
                0,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                2,
            ),
            (
                2024,
                1,
                "fid_3",
                "fid_2",
                "Seed Three",
                "Seed Two",
                95.0,
                105.0,
                0,
                1,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                3,
            ),
            (
                2024,
                2,
                "fid_1",
                "fid_2",
                "Seed One",
                "Seed Two",
                100.0,
                98.0,
                1,
                0,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                1,
            ),
            (
                2024,
                2,
                "fid_2",
                "fid_1",
                "Seed Two",
                "Seed One",
                98.0,
                100.0,
                0,
                1,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                2,
            ),
            (
                2024,
                2,
                "fid_3",
                "fid_4",
                "Seed Three",
                "Seed Four",
                120.0,
                80.0,
                1,
                0,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                3,
            ),
            (
                2024,
                2,
                "fid_4",
                "fid_3",
                "Seed Four",
                "Seed Three",
                80.0,
                120.0,
                0,
                1,
                0,
                0,
                0,
                0,
                None,
                0,
                0,
                None,
                None,
                None,
                4,
            ),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 1
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, week, is_playoffs, is_consolation, postseason,
                       is_championship, champion, sacko, placement_rank,
                       playoff_round, consolation_round
                FROM public.matchup
                ORDER BY manager, week
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert {row[8] for row in rows} == {1, 2, 3, 4}
    assert all(row[2] == 0 and row[3] == 0 and row[4] == 0 for row in rows)
    assert all(row[5] is False and row[9] is None and row[10] is None for row in rows)

    champion_rows = [row for row in rows if row[6] == 1]
    sacko_rows = [row for row in rows if row[7] == 1]
    assert champion_rows == [("Seed One", 2, 0, 0, 0, False, 1, 0, 1, None, None)]
    assert sacko_rows == [("Seed Four", 2, 0, 0, 0, False, 0, 1, 4, None, None)]


def test_shape_playoff_bracket_no_playoff_points_standings_marks_points_champion(tmp_path):
    db_name = "no_playoff_points_seed_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_teams INTEGER,
            playoff_start_week INTEGER,
            end_week INTEGER,
            num_teams INTEGER,
            bye_teams INTEGER,
            has_multiweek_championship BOOLEAN,
            playoff_round_type INTEGER,
            uses_playoff_reseeding BOOLEAN,
            uses_median BOOLEAN,
            playoff_seeding_rule VARCHAR,
            playoff_seeding_rule_by INTEGER
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.league_settings VALUES
            (2024, 0, 4, 3, 3, 0, FALSE, 0, FALSE, FALSE, 'TOTAL_POINTS_SCORED', 0)
        """
    )
    conn.execute(
        """
        CREATE TABLE public.matchup (
            year INTEGER,
            week INTEGER,
            franchise_id VARCHAR,
            opponent_franchise_id VARCHAR,
            manager VARCHAR,
            opponent VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            win INTEGER,
            loss INTEGER,
            tie INTEGER,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            postseason INTEGER,
            is_championship BOOLEAN,
            champion INTEGER,
            sacko INTEGER,
            placement_rank INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            final_playoff_seed INTEGER
        )
        """
    )

    def row(week, fid, manager, points, opp_points):
        win = int(points > opp_points)
        return (
            2024,
            week,
            fid,
            f"opp_{fid}",
            manager,
            "Opponent",
            points,
            opp_points,
            win,
            int(not win),
            0,
            0,
            0,
            0,
            False,
            0,
            0,
            None,
            None,
            None,
            None,
        )

    rows = []
    for week in range(1, 4):
        rows.append(row(week, "fid_record", "Record Champ Lure", 90.0, 80.0))
        rows.append(row(week, "fid_points", "Points Champ", 150.0, 160.0))
        rows.append(row(week, "fid_low", "Low Points", 70.0, 100.0))
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))
    try:
        assert runner.shape_playoff_bracket_local() == 1
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT manager, champion, placement_rank
                FROM public.matchup
                WHERE week = 3
                ORDER BY placement_rank
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert rows[0] == ("Points Champ", 1, 1)


def test_enforce_postseason_flags_does_not_set_postseason_flags_on_bye_rows(tmp_path):
    """Per project policy (memory: feedback_phantom_rows_no_flags), a phantom row
    where the team didn't play (no opponent, no points) must never have
    is_playoffs=1, is_consolation=1, or postseason=1 set.

    enforce_postseason_flags Step 0 used to flag postseason no-opponent rows as
    is_bye_week=1, is_playoffs=1, postseason=1. The is_playoffs+postseason part
    is wrong: per policy those flags only belong on rows the team actually played.
    Step 0 must set is_bye_week=1 only and leave the other flags alone.

    Concrete fleet repro shape: dingleberry_derby 2018 W16 phantom rows for
    SellOffForPicks (champion) and HerbertsOlMan (runner-up) — championship was
    decided in W15, W16 has no game, but Sleeper API returned matchup_id=None
    rows that became phantom byes.
    """
    db_name = "phantom_bye_no_flags_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (
            year INTEGER,
            playoff_start_week INTEGER
        )
        """
    )
    conn.execute("INSERT INTO public.league_settings VALUES (2018, 14)")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            postseason INTEGER,
            win INTEGER,
            loss INTEGER,
            tie INTEGER
        )
        """
    )
    # Seed two phantom (no-opponent) rows in postseason weeks AND one normal
    # regular-season row — the function should only touch the phantoms.
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            # Phantom W16 row — should get is_bye_week=1 but NOT is_playoffs/is_consolation/postseason
            (db_name, "fid_champ", "Champion", 2018, 16, None, None, None, None, 0, 0, 0, 0, 0, 0, 0),
            # Another phantom W16 row — same expectation
            (db_name, "fid_runner", "Runner Up", 2018, 16, None, None, None, None, 0, 0, 0, 0, 0, 0, 0),
            # Regular-season row — must remain untouched
            (db_name, "fid_other", "Other", 2018, 5, "Rival", "fid_rival", 100.0, 90.0, 0, 0, 0, 0, 1, 0, 0),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.enforce_postseason_flags()
        rows = (
            runner._get_connection()
            .execute(
                """
                SELECT franchise_id, week,
                       COALESCE(is_bye_week, 0) AS bye,
                       COALESCE(is_playoffs, 0) AS pl,
                       COALESCE(is_consolation, 0) AS con,
                       COALESCE(postseason, 0) AS post
                FROM public.matchup
                ORDER BY franchise_id, week
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    by_fid = {r[0]: r for r in rows}

    # Phantom W16 rows: is_bye_week=1 set by Step 0, but NO postseason flags.
    assert by_fid["fid_champ"][2:] == (
        1,
        0,
        0,
        0,
    ), f"phantom champion row should have is_bye_week=1 only; got {by_fid['fid_champ']}"
    assert by_fid["fid_runner"][2:] == (
        1,
        0,
        0,
        0,
    ), f"phantom runner-up row should have is_bye_week=1 only; got {by_fid['fid_runner']}"
    # Regular-season row untouched.
    assert by_fid["fid_other"][2:] == (0, 0, 0, 0), f"regular-season row should be untouched; got {by_fid['fid_other']}"


def test_invariant_no_phantom_row_has_postseason_flags_after_enforce(tmp_path):
    """Pipeline invariant guardrail (memory: feedback_phantom_rows_no_flags).

    Asserts the cross-cutting policy: after enforce_postseason_flags runs,
    NO row where the team didn't play (opponent IS NULL AND team_points IS
    NULL) has any of: is_playoffs=1, is_consolation=1, postseason=1,
    playoff_round set, consolation_round set.

    This is the canonical regression-lock for the dingleberry_derby 2018-2020
    phantom-postseason-rows bug. If anyone re-introduces a write that flags a
    phantom row, this test fails.

    Test data covers all three phantom flavors:
      - pre-game bye (postseason team that has a future game)
      - post-elimination phantom (eliminated, no future game)
      - post-championship phantom (championship participant, no future game)
    """
    db_name = "phantom_invariant_test"
    db_path = tmp_path / f"{db_name}.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.league_settings (year INTEGER, playoff_start_week INTEGER)
        """
    )
    conn.execute("INSERT INTO public.league_settings VALUES (2024, 15)")
    conn.execute(
        """
        CREATE TABLE public.matchup (
            db_name VARCHAR,
            franchise_id VARCHAR,
            manager VARCHAR,
            year INTEGER,
            week INTEGER,
            opponent VARCHAR,
            opponent_franchise_id VARCHAR,
            team_points DOUBLE,
            opponent_points DOUBLE,
            is_playoffs INTEGER,
            is_consolation INTEGER,
            is_bye_week INTEGER,
            postseason INTEGER,
            playoff_round VARCHAR,
            consolation_round VARCHAR,
            win INTEGER,
            loss INTEGER,
            tie INTEGER
        )
        """
    )
    conn.executemany(
        """
        INSERT INTO public.matchup VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            # Pre-game bye: team A has W15 phantom + W16 real game
            (db_name, "A", "PreGameBye", 2024, 15, None, None, None, None, 0, 0, 0, 0, None, None, 0, 0, 0),
            (db_name, "A", "PreGameBye", 2024, 16, "B", "B", 110.0, 90.0, 0, 0, 0, 0, None, None, 1, 0, 0),
            # Post-elimination: team B has W15 real game then W16 phantom
            (db_name, "B", "PostElim", 2024, 15, "A_unused", None, 80.0, 100.0, 0, 0, 0, 0, None, None, 0, 1, 0),
            (db_name, "B", "PostElim", 2024, 16, None, None, None, None, 0, 0, 0, 0, None, None, 0, 0, 0),
            # Post-championship: team C has W16 real game (championship) then W17 phantom
            (db_name, "C", "PostChamp", 2024, 16, "A", "A", 120.0, 110.0, 0, 0, 0, 0, None, None, 1, 0, 0),
            (db_name, "C", "PostChamp", 2024, 17, None, None, None, None, 0, 0, 0, 0, None, None, 0, 0, 0),
        ],
    )
    conn.close()

    runner = _MatchupRunner(db_name=db_name, data_dir=str(tmp_path))

    try:
        runner.enforce_postseason_flags()
        violations = (
            runner._get_connection()
            .execute(
                """
                SELECT franchise_id, week, is_playoffs, is_consolation, postseason,
                       playoff_round, consolation_round
                FROM public.matchup
                WHERE opponent IS NULL
                  AND team_points IS NULL
                  AND (
                      COALESCE(CAST(is_playoffs AS INTEGER), 0) = 1
                      OR COALESCE(CAST(is_consolation AS INTEGER), 0) = 1
                      OR COALESCE(CAST(postseason AS INTEGER), 0) = 1
                      OR playoff_round IS NOT NULL
                      OR consolation_round IS NOT NULL
                  )
                """
            )
            .fetchall()
        )
    finally:
        if runner._conn is not None:
            runner._conn.close()

    assert violations == [], (
        "phantom-row policy violation: rows where the team did not play must "
        f"never have postseason flags set. Offenders: {violations}"
    )
