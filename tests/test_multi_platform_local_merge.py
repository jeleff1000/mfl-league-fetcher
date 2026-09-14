from __future__ import annotations

import importlib.util
from pathlib import Path

import duckdb


def load_runner():
    script_path = Path(__file__).resolve().parents[1] / ".github" / "scripts" / "run_multi_platform_import.py"
    spec = importlib.util.spec_from_file_location("run_multi_platform_import", script_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def create_segment_db(path: Path, *, db_name: str, rows_year: int, manager: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS public")
        conn.execute(
            """
            CREATE TABLE public.matchup (
                db_name VARCHAR,
                year INTEGER,
                week INTEGER,
                manager VARCHAR,
                opponent VARCHAR,
                franchise_id VARCHAR,
                opponent_franchise_id VARCHAR,
                points DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE public.league_settings (
                db_name VARCHAR,
                year INTEGER,
                league_name VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE public.player_fantasy (
                db_name VARCHAR,
                year INTEGER,
                week INTEGER,
                manager VARCHAR,
                NFL_player_id VARCHAR,
                fantasy_points DOUBLE
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE public.draft (
                db_name VARCHAR,
                year INTEGER,
                round INTEGER,
                pick INTEGER,
                manager VARCHAR,
                franchise_id VARCHAR,
                team_name VARCHAR
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE public.transactions (
                db_name VARCHAR,
                year INTEGER,
                transaction_id VARCHAR,
                manager VARCHAR,
                franchise_id VARCHAR,
                source_manager VARCHAR,
                source_franchise_id VARCHAR,
                destination_manager VARCHAR,
                destination_franchise_id VARCHAR
            )
            """
        )
        conn.execute(
            "INSERT INTO public.matchup VALUES (?, ?, 1, ?, 'Opponent A', ?, 'opp_old', 101.5)",
            [db_name, rows_year, manager, f"{manager}_fid"],
        )
        conn.execute(
            "INSERT INTO public.league_settings VALUES (?, ?, ?)",
            [db_name, rows_year, f"{db_name} settings"],
        )
        conn.execute(
            "INSERT INTO public.player_fantasy VALUES (?, ?, 1, ?, 'nfl_1', 20.5)",
            [db_name, rows_year, manager],
        )
        conn.execute(
            "INSERT INTO public.player_fantasy VALUES (?, ?, 1, 'Unrostered', 'nfl_2', 9.5)",
            [db_name, rows_year],
        )
        conn.execute(
            "INSERT INTO public.draft VALUES (?, ?, 1, 1, ?, ?, ?)",
            [db_name, rows_year, manager, f"{manager}_fid", f"{manager} Team"],
        )
        conn.execute(
            "INSERT INTO public.transactions VALUES (?, ?, 'tx1', ?, ?, NULL, NULL, NULL, NULL)",
            [db_name, rows_year, manager, f"{manager}_fid"],
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()


def test_local_merge_maps_draft_aliases_sharing_mapped_source_franchise_id(tmp_path: Path) -> None:
    runner = load_runner()
    target_segment = {
        "database_name": "target_db",
        "platform": "sleeper",
        "merge_years": [2022],
        "manager_name_overrides": {"Jason - Dead Inside": "maedayjay"},
    }
    source_segment = {
        "database_name": "source_db",
        "platform": "yahoo",
        "merge_years": [2021],
    }
    target_dir = tmp_path / "target"
    source_dir = tmp_path / "source"
    target_path = target_dir / "target_db.duckdb"
    source_path = source_dir / "source_db.duckdb"

    create_segment_db(target_path, db_name="target_db", rows_year=2022, manager="maedayjay")
    create_segment_db(source_path, db_name="source_db", rows_year=2021, manager="Jason - Dead Inside")

    conn = duckdb.connect(str(source_path))
    try:
        conn.execute(
            """
            UPDATE public.draft
            SET manager = 'Jason', team_name = 'Dead Inside'
            WHERE year = 2021
            """
        )
        conn.execute(
            """
            INSERT INTO public.transactions
            VALUES (
                'source_db',
                2021,
                'trade1',
                'Opponent',
                'opponent_fid',
                'Jason - Dead Inside',
                'Jason - Dead Inside_fid',
                NULL,
                NULL
            )
            """
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    stats = runner.merge_local_source_into_target(
        target_segment=target_segment,
        target_data_dir=target_dir,
        source_segment=source_segment,
        source_data_dir=source_dir,
        index=1,
    )

    assert stats["draft"] == 1

    conn = duckdb.connect(str(target_path))
    try:
        assert conn.execute(
            """
            SELECT manager, franchise_id
            FROM public.matchup
            WHERE year = 2021
            """
        ).fetchall() == [("maedayjay", "maedayjay_fid")]
        assert conn.execute(
            """
            SELECT manager, franchise_id
            FROM public.draft
            WHERE year = 2021
            """
        ).fetchall() == [("maedayjay", "maedayjay_fid")]
        assert conn.execute(
            """
            SELECT manager, franchise_id, source_manager, source_franchise_id
            FROM public.transactions
            WHERE year = 2021
            ORDER BY transaction_id
            """
        ).fetchall() == [
            ("Opponent", "opponent_fid", "maedayjay", "maedayjay_fid"),
            ("maedayjay", "maedayjay_fid", None, None),
        ]
    finally:
        conn.close()


def test_local_merge_does_not_treat_an_earlier_source_as_target_identity(tmp_path: Path) -> None:
    """Copied history must retain its own IDs unless it matches the actual target segment."""
    runner = load_runner()
    target_segment = {
        "database_name": "target_db",
        "platform": "sleeper",
        "merge_years": [2022],
    }
    source_segment = {
        "database_name": "source_db",
        "platform": "yahoo",
        "merge_years": [2021],
    }
    target_dir = tmp_path / "target"
    source_dir = tmp_path / "source"
    target_path = target_dir / "target_db.duckdb"
    source_path = source_dir / "source_db.duckdb"

    create_segment_db(target_path, db_name="target_db", rows_year=2022, manager="Target Manager")
    create_segment_db(source_path, db_name="source_db", rows_year=2021, manager="Other")

    conn = duckdb.connect(str(target_path))
    try:
        # This row represents a previously copied Yahoo segment, not the Sleeper target.
        conn.execute(
            """
            INSERT INTO public.matchup
            VALUES ('target_db', 2020, 1, 'Legacy Team', 'Other', 'legacy_2020', 'other_2020', 88.0)
            """
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    conn = duckdb.connect(str(source_path))
    try:
        conn.execute(
            """
            INSERT INTO public.matchup
            VALUES ('source_db', 2021, 2, 'Legacy Team - Display', 'Other', 'legacy_2021', 'other_2021', 91.0),
                   ('source_db', 2021, 2, 'Other', 'Legacy Team', 'other_2021', 'legacy_2021', 93.0)
            """
        )
        conn.execute(
            """
            INSERT INTO public.draft
            VALUES ('source_db', 2021, 2, 2, 'Legacy Team', 'legacy_2021', 'Legacy Team')
            """
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    runner.merge_local_source_into_target(
        target_segment=target_segment,
        target_data_dir=target_dir,
        source_segment=source_segment,
        source_data_dir=source_dir,
        index=1,
    )

    conn = duckdb.connect(str(target_path))
    try:
        assert conn.execute(
            """
            SELECT franchise_id
            FROM public.draft
            WHERE year = 2021 AND manager = 'Legacy Team'
            """
        ).fetchone() == ("legacy_2021",)
        assert conn.execute(
            """
            SELECT franchise_id, opponent_franchise_id
            FROM public.matchup
            WHERE year = 2021 AND week = 2 AND manager = 'Other' AND opponent = 'Legacy Team'
            """
        ).fetchone() == ("other_2021", "legacy_2021")
    finally:
        conn.close()


def test_merge_local_source_into_target_rewrites_selected_years(tmp_path: Path) -> None:
    runner = load_runner()
    target_segment = {"database_name": "target_db", "platform": "sleeper", "merge_years": [2022]}
    source_segment = {
        "database_name": "source_db",
        "platform": "yahoo",
        "merge_years": [2020],
        "manager_mapping": {"Jason": "Jay"},
    }
    target_dir = tmp_path / "target"
    source_dir = tmp_path / "source"
    target_path = target_dir / "target_db.duckdb"
    source_path = source_dir / "source_db.duckdb"

    create_segment_db(target_path, db_name="target_db", rows_year=2022, manager="Jay")
    create_segment_db(source_path, db_name="source_db", rows_year=2020, manager="Jason")

    conn = duckdb.connect(str(target_path))
    try:
        conn.execute(
            "INSERT INTO public.matchup VALUES ('target_db', 2020, 1, 'Stale', 'Old', 'stale_fid', 'old_fid', 1.0)"
        )
        conn.execute(
            "INSERT INTO public.matchup VALUES ('target_db', 2021, 1, 'Ghost', 'Old', 'ghost_fid', 'old_fid', 2.0)"
        )
        conn.execute("INSERT INTO public.league_settings VALUES ('target_db', 2021, 'ghost settings')")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    runner.trim_local_target_to_selected_years(target_segment, target_dir)
    stats = runner.merge_local_source_into_target(
        target_segment=target_segment,
        target_data_dir=target_dir,
        source_segment=source_segment,
        source_data_dir=source_dir,
        index=1,
    )

    assert stats["matchup"] == 1
    assert stats["league_settings"] == 1
    # Full imports retain the entire NFL player pool, including the
    # unrostered rows created by the source platform's NFL expansion.
    # A multi-platform merge must not silently turn a full historical import
    # into a rostered-only one for the source years.
    assert stats["player_fantasy"] == 2

    conn = duckdb.connect(str(target_path))
    try:
        matchup_rows = conn.execute(
            """
            SELECT db_name, year, manager, franchise_id, points
            FROM public.matchup
            WHERE year = 2020
            """
        ).fetchall()
        assert matchup_rows == [("target_db", 2020, "Jay", "Jay_fid", 101.5)]
        assert conn.execute("SELECT COUNT(*) FROM public.matchup WHERE year = 2021").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM public.league_settings WHERE year = 2021").fetchone()[0] == 0

        player_rows = conn.execute(
            """
            SELECT manager, NFL_player_id
            FROM public.player_fantasy
            WHERE year = 2020
            ORDER BY NFL_player_id
            """
        ).fetchall()
        assert player_rows == [("Jay", "nfl_1"), ("Unrostered", "nfl_2")]

        transaction_rows = conn.execute("SELECT db_name, manager FROM public.transactions WHERE year = 2020").fetchall()
        assert transaction_rows == [("target_db", "Jay")]
    finally:
        conn.close()


def test_verify_only_postprocessing_runs_local_work_without_publication(tmp_path, monkeypatch) -> None:
    runner = load_runner()
    commands: list[str] = []

    monkeypatch.setattr(runner, "run_local_sql_enrichments_after_merge", lambda *_: None)
    monkeypatch.setattr(runner, "local_matchup_count", lambda *_: 1)
    monkeypatch.setattr(
        runner,
        "run_logged",
        lambda name, *args, **kwargs: commands.append(name),
    )
    monkeypatch.setattr(
        runner,
        "upload_local_db",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("unexpected publication")),
    )

    runner.postprocess_and_upload_target(
        {"database_name": "verify_only", "platform": "sleeper"},
        tmp_path,
        publish=False,
    )

    assert commands == [
        "CALCULATE EXPECTED RECORDS",
        "CALCULATE PLAYOFF ODDS",
        "RUN AGGREGATIONS",
    ]


def test_local_merge_keeps_partial_historical_source_without_matchups(tmp_path: Path) -> None:
    """A source season with real roster/draft data must not abort the whole paid merge."""
    runner = load_runner()
    target_segment = {
        "database_name": "target_db",
        "platform": "sleeper",
        "merge_years": [2025],
    }
    source_segment = {
        "database_name": "source_db",
        "platform": "yahoo",
        "merge_years": [2005],
        "allow_partial_history_source": True,
        "manager_mapping": {"Todd": "TRV515"},
    }
    target_dir = tmp_path / "target"
    source_dir = tmp_path / "source"
    target_path = target_dir / "target_db.duckdb"
    source_path = source_dir / "source_db.duckdb"

    create_segment_db(target_path, db_name="target_db", rows_year=2025, manager="TRV515")
    # The source importer can apply Todd -> TRV515 before franchise discovery
    # adds its generated team-name suffix.
    create_segment_db(source_path, db_name="source_db", rows_year=2005, manager="TRV515 - Hoodie's")
    conn = duckdb.connect(str(source_path))
    try:
        conn.execute("DELETE FROM public.matchup")
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    stats = runner.merge_local_source_into_target(
        target_segment=target_segment,
        target_data_dir=target_dir,
        source_segment=source_segment,
        source_data_dir=source_dir,
        index=1,
    )

    assert stats["status"] == "copied_partial_local"
    assert "matchup" not in stats
    assert stats["league_settings"] == 1
    assert stats["player_fantasy"] == 2
    conn = duckdb.connect(str(target_path))
    try:
        assert conn.execute(
            "SELECT manager FROM public.player_fantasy WHERE year = 2005 ORDER BY manager"
        ).fetchall() == [("TRV515",), ("Unrostered",)]
        assert conn.execute("SELECT COUNT(*) FROM public.matchup WHERE year = 2025").fetchone()[0] == 1
    finally:
        conn.close()


def test_non_target_yahoo_import_enables_partial_history_contract(monkeypatch, tmp_path: Path) -> None:
    runner = load_runner()
    data_dir = tmp_path / "source"
    context_path = tmp_path / "source-context.json"
    captured: list[str] = []

    monkeypatch.setattr(runner, "FANTASY_DIR", tmp_path)
    monkeypatch.setattr(runner, "LOG_DIR", tmp_path / "logs")
    monkeypatch.setattr(runner, "build_context", lambda *_args, **_kwargs: (context_path, data_dir))
    monkeypatch.setattr(
        runner,
        "run_logged",
        lambda _name, cmd, **_kwargs: captured.extend(cmd),
    )
    monkeypatch.setattr(runner, "persist_yahoo_refresh_token", lambda *_args, **_kwargs: None)

    runner.import_segment(
        {
            "platform": "yahoo",
            "auth_mode": "oauth",
            "database_name": "source_db",
            "league_id": "124.l.161297",
            "merge_years": [2005],
        },
        2,
        is_target=False,
    )

    assert "--allow-partial-history-source" in captured


def test_normalize_segments_applies_manager_overrides_to_all_segments() -> None:
    runner = load_runner()
    segments, target_index, target_db = runner.normalize_segments(
        {
            "target_db": "theta_chi_oe",
            "target_segment_index": 1,
            "manager_name_overrides": {"Kip256": "Daylyn Thomas"},
            "segments": [
                {
                    "platform": "yahoo",
                    "database_name": "theta_chi_oe_yahoo",
                    "league_id": "449.l.9912",
                    "merge_years": [2024],
                },
                {
                    "platform": "sleeper",
                    "database_name": "theta_chi_oe",
                    "league_id": "1316574461163487232",
                    "merge_years": [2025],
                },
            ],
        }
    )

    assert target_index == 1
    assert target_db == "theta_chi_oe"
    assert segments[0]["manager_name_overrides"] == {"Kip256": "Daylyn Thomas"}
    assert segments[1]["manager_name_overrides"] == {"Kip256": "Daylyn Thomas"}


def test_normalize_segments_applies_franchise_merges_to_all_segments() -> None:
    runner = load_runner()
    merges = [{"display_name": "Greg Gamboa", "owner_ids": ["owner_new", "owner_old"]}]
    segments, _, _ = runner.normalize_segments(
        {
            "target_db": "theta_chi_oe",
            "target_segment_index": 1,
            "franchise_merges": merges,
            "segments": [
                {
                    "platform": "yahoo",
                    "database_name": "theta_chi_oe_yahoo",
                    "league_id": "449.l.9912",
                    "merge_years": [2024],
                },
                {
                    "platform": "sleeper",
                    "database_name": "theta_chi_oe",
                    "league_id": "1316574461163487232",
                    "merge_years": [2025],
                },
            ],
        }
    )

    assert segments[0]["franchise_merges"] == merges
    assert segments[1]["franchise_merges"] == merges


def test_local_merge_composes_bridge_mapping_with_final_manager_overrides(tmp_path: Path) -> None:
    runner = load_runner()
    target_segment = {
        "database_name": "target_db",
        "platform": "sleeper",
        "merge_years": [2022],
        "manager_name_overrides": {
            "Moria Balrogs": "Stephen Daugherty",
            "MoriaBalrogs": "Stephen Daugherty",
        },
    }
    source_segment = {
        "database_name": "source_db",
        "platform": "yahoo",
        "merge_years": [2020],
        "manager_mapping": {"Moria Balrogs": "MoriaBalrogs"},
    }
    target_dir = tmp_path / "target"
    source_dir = tmp_path / "source"
    target_path = target_dir / "target_db.duckdb"
    source_path = source_dir / "source_db.duckdb"

    create_segment_db(target_path, db_name="target_db", rows_year=2022, manager="Stephen Daugherty")
    create_segment_db(source_path, db_name="source_db", rows_year=2020, manager="Moria Balrogs")

    stats = runner.merge_local_source_into_target(
        target_segment=target_segment,
        target_data_dir=target_dir,
        source_segment=source_segment,
        source_data_dir=source_dir,
        index=1,
    )

    assert stats["matchup"] == 1

    conn = duckdb.connect(str(target_path))
    try:
        assert conn.execute(
            """
            SELECT manager, franchise_id
            FROM public.matchup
            WHERE year = 2020
            """
        ).fetchall() == [("Stephen Daugherty", "Stephen Daugherty_fid")]
        assert conn.execute(
            """
            SELECT manager
            FROM public.player_fantasy
            WHERE year = 2020
            ORDER BY manager
            """
        ).fetchall() == [("Stephen Daugherty",), ("Unrostered",)]
    finally:
        conn.close()


def test_post_enrichment_reconciles_explicit_bridge_but_preserves_same_year_multi_team(tmp_path: Path) -> None:
    runner = load_runner()
    target_dir = tmp_path / "target"
    target_path = target_dir / "target_db.duckdb"
    create_segment_db(
        target_path,
        db_name="target_db",
        rows_year=2025,
        manager="TRV515 - Your Wife's Flex Ladd",
    )

    conn = duckdb.connect(str(target_path))
    try:
        conn.execute("ALTER TABLE public.player_fantasy ADD COLUMN franchise_id VARCHAR")
        conn.execute(
            """
            INSERT INTO public.matchup VALUES
                ('target_db', 2023, 1, 'TRV515 - Championship Kupp', 'Opponent A', 'old_fid', 'opp_2023', 101.5),
                ('target_db', 2024, 1, 'TRV515 - Your Wife''s Flex Ladd', 'Opponent A', 'TRV515 - Your Wife''s Flex Ladd_fid', 'opp_2024', 101.5),
                ('target_db', 2004, 1, 'TRV515 - Team One', 'Opponent A', 'old_2004_a', 'opp_a', 101.5),
                ('target_db', 2004, 1, 'TRV515 - Team Two', 'Opponent B', 'old_2004_b', 'opp_b', 99.5)
            """
        )
        conn.execute(
            """
            INSERT INTO public.player_fantasy
                (db_name, year, week, manager, NFL_player_id, fantasy_points, franchise_id)
            VALUES
                ('target_db', 2005, 1, 'TRV515', 'nfl_2005', 12.5, 'old_2005'),
                ('target_db', 2023, 1, 'TRV515 - Championship Kupp', 'nfl_2023', 20.5, 'old_fid'),
                ('target_db', 2024, 1, 'TRV515 - Your Wife''s Flex Ladd', 'nfl_2024', 21.5, 'TRV515 - Your Wife''s Flex Ladd_fid'),
                ('target_db', 2004, 1, 'TRV515 - Team One', 'nfl_2004_a', 10.5, 'old_2004_a'),
                ('target_db', 2004, 1, 'TRV515 - Team Two', 'nfl_2004_b', 11.5, 'old_2004_b')
            """
        )
        conn.execute(
            """
            INSERT INTO public.draft VALUES
                ('target_db', 2005, 1, 2, 'TRV515', NULL, 'Old Team')
            """
        )
        conn.execute("CHECKPOINT")
    finally:
        conn.close()

    mapped = runner.reconcile_explicit_manager_bridges_after_enrichment(
        segments=[
            {
                "database_name": "source_db",
                "platform": "yahoo",
                "merge_years": [2004, 2005, 2023, 2024],
                "manager_mapping": {"Todd": "TRV515"},
            },
            {
                "database_name": "target_db",
                "platform": "sleeper",
                "merge_years": [2025],
            },
        ],
        target_index=1,
        target_data_dir=target_dir,
    )

    assert mapped == 3
    conn = duckdb.connect(str(target_path))
    try:
        target_fid = "TRV515 - Your Wife's Flex Ladd_fid"
        assert conn.execute(
            "SELECT manager, franchise_id FROM public.matchup WHERE year = 2023"
        ).fetchall() == [("TRV515", target_fid)]
        assert conn.execute(
            "SELECT manager FROM public.player_fantasy WHERE year = 2023"
        ).fetchall() == [("TRV515",)]
        assert conn.execute(
            "SELECT manager, franchise_id FROM public.matchup WHERE year = 2024"
        ).fetchall() == [("TRV515", target_fid)]
        assert conn.execute(
            "SELECT manager, franchise_id FROM public.player_fantasy WHERE year = 2005"
        ).fetchall() == [("TRV515", target_fid)]
        assert conn.execute(
            "SELECT manager, franchise_id FROM public.draft WHERE year = 2005"
        ).fetchall() == [("TRV515", target_fid)]
        assert conn.execute(
            "SELECT manager, franchise_id FROM public.matchup WHERE year = 2004 ORDER BY franchise_id"
        ).fetchall() == [
            ("TRV515 - Team One", "old_2004_a"),
            ("TRV515 - Team Two", "old_2004_b"),
        ]
        assert conn.execute(
            "SELECT manager FROM public.player_fantasy WHERE year = 2004 ORDER BY manager"
        ).fetchall() == [("TRV515 - Team One",), ("TRV515 - Team Two",)]
    finally:
        conn.close()


def test_post_merge_reenriches_the_combined_league_before_aggregating(monkeypatch, tmp_path: Path) -> None:
    """Cross-segment grades must use the combined historical pool, not each source in isolation."""
    runner = load_runner()
    calls: list[str] = []

    monkeypatch.setattr(runner, "local_matchup_count", lambda *_args, **_kwargs: 1)
    monkeypatch.setattr(
        runner,
        "run_logged",
        lambda name, *_args, **_kwargs: calls.append(name),
    )
    monkeypatch.setattr(
        runner,
        "upload_local_db",
        lambda _segment, _data_dir, *, name, **_kwargs: calls.append(name),
    )
    monkeypatch.delenv("REVALIDATION_SECRET", raising=False)

    runner.postprocess_and_upload_target(
        {"database_name": "target_db", "platform": "sleeper"},
        tmp_path,
    )

    assert calls[0] == "RUN SQL ENRICHMENTS AFTER LOCAL MERGE"
    assert calls.index("RUN SQL ENRICHMENTS AFTER LOCAL MERGE") < calls.index("RUN AGGREGATIONS")
