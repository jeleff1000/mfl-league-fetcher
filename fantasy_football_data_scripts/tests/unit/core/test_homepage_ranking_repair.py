from __future__ import annotations

from pathlib import Path
import json
import tarfile

import pandas as pd


class _Reader:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def query(self, sql: str, *, database: str):
        self.calls.append((sql, database))
        return self.rows


def test_rankings_repair_health_is_one_bounded_fly_read():
    from multi_league.core.homepage_ranking_repair import manager_rankings_repair_health

    reader = _Reader([
        {"base_generation": 17, "matchup_rows": 240, "ranking_rows": 0},
    ])

    health = manager_rankings_repair_health(reader, "suck_a_ditka")

    assert health == {
        "base_generation": 17,
        "matchup_rows": 240,
        "ranking_rows": 0,
        "needed": True,
    }
    assert len(reader.calls) == 1
    sql, database = reader.calls[0]
    assert database == "___leagues"
    assert "public.matchup" in sql
    assert "public.homepage_manager_rankings" in sql
    assert "merge_admin.league_publish_generations" in sql
    assert "suck_a_ditka" in sql


def test_rankings_repair_health_does_not_rewrite_healthy_or_empty_leagues():
    from multi_league.core.homepage_ranking_repair import manager_rankings_repair_health

    healthy = manager_rankings_repair_health(
        _Reader([{"base_generation": 3, "matchup_rows": 100, "ranking_rows": 10}]),
        "healthy",
    )
    empty = manager_rankings_repair_health(
        _Reader([{"base_generation": 0, "matchup_rows": 0, "ranking_rows": 0}]),
        "empty",
    )

    assert healthy["needed"] is False
    assert empty["needed"] is False


def test_prepare_rankings_repair_reuses_complete_history_and_saved_median_settings(tmp_path: Path):
    from multi_league.core.homepage_ranking_repair import prepare_manager_rankings_repair
    from multi_league.core.local_db import LocalLeagueDB

    matchup = pd.DataFrame(
        [
            {
                "db_name": "history",
                "year": 2024,
                "week": 1,
                "manager": "Alpha",
                "franchise_id": "alpha",
                "team_points": 100.0,
                "is_bye_week": 0,
                "is_consolation": 0,
                "is_playoffs": 0,
                "win": 1,
                "loss": 0,
                "tie": 0,
                "above_league_median": 1,
                "below_league_median": 0,
                "champion": 0,
                "power_rating": 101.0,
            },
            {
                "db_name": "history",
                "year": 2025,
                "week": 1,
                "manager": "Alpha Preferred",
                "franchise_id": "alpha",
                "team_points": 90.0,
                "is_bye_week": 0,
                "is_consolation": 0,
                "is_playoffs": 1,
                "win": 1,
                "loss": 0,
                "tie": 0,
                "above_league_median": 0,
                "below_league_median": 0,
                "champion": 1,
                "power_rating": 103.0,
            },
            {
                "db_name": "history",
                "year": 2024,
                "week": 1,
                "manager": "Beta",
                "franchise_id": "beta",
                "team_points": 80.0,
                "is_bye_week": 0,
                "is_consolation": 0,
                "is_playoffs": 0,
                "win": 0,
                "loss": 1,
                "tie": 0,
                "above_league_median": 0,
                "below_league_median": 1,
                "champion": 0,
                "power_rating": 99.0,
            },
        ]
    )
    matchup_season = pd.DataFrame(
        [
            {"db_name": "history", "year": 2024, "franchise_id": "alpha", "power_rating": 101.0},
            {"db_name": "history", "year": 2025, "franchise_id": "alpha", "power_rating": 103.0},
            {"db_name": "history", "year": 2024, "franchise_id": "beta", "power_rating": 99.0},
        ]
    )
    settings = pd.DataFrame(
        [
            {"db_name": "history", "year": 2024, "uses_median": True},
            {"db_name": "history", "year": 2025, "uses_median": False},
        ]
    )
    local_db = LocalLeagueDB(tmp_path, "history")
    try:
        result = prepare_manager_rankings_repair(
            local_db=local_db,
            db_name="history",
            source_frames={
                "matchup": matchup,
                "matchup_season": matchup_season,
                "league_settings": settings,
            },
        )
        rankings = local_db.connect().execute(
            "SELECT manager, franchise_id, wins, losses, championships, seasons "
            "FROM public.homepage_manager_rankings ORDER BY franchise_id"
        ).fetchdf()
        tables = {
            row[0]
            for row in local_db.connect().execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema='public'"
            ).fetchall()
        }
    finally:
        local_db.close()

    assert result == {"published_tables": ["homepage_manager_rankings"], "rows": 2}
    assert tables == {"homepage_manager_rankings"}
    assert rankings.to_dict("records") == [
        {
            "manager": "Alpha Preferred",
            "franchise_id": "alpha",
            "wins": 3.0,
            "losses": 0.0,
            "championships": 1.0,
            "seasons": 2,
        },
        {
            "manager": "Beta",
            "franchise_id": "beta",
            "wins": 0.0,
            "losses": 2.0,
            "championships": 0.0,
            "seasons": 1,
        },
    ]


def test_all_platform_workers_can_repair_a_missing_rankings_rollup_before_noop():
    root = Path(__file__).resolve().parents[4]
    for script_name in (
        "refresh_yahoo_active_season.py",
        "refresh_sleeper_active_season.py",
        "refresh_espn_active_season.py",
    ):
        source = (root / "scripts" / script_name).read_text(encoding="utf-8")
        no_weeks = source.index("if not refresh_weeks:")
        noop_status = source.index('receipt["status"] = "NO_FINALIZED_WEEKS"', no_weeks)
        repair = source.index("record_missing_manager_rankings_repair", no_weeks)
        assert no_weeks < repair < noop_status


def test_missing_rankings_publication_is_one_generation_fenced_rollup(monkeypatch):
    from multi_league.core.homepage_ranking_repair import (
        repair_missing_manager_rankings_if_needed,
    )
    from multi_league.core.targets.fly_target import FlyTarget

    matchup = [
        {
            "db_name": "repair_me",
            "year": 2025,
            "week": 1,
            "manager": "Alpha",
            "franchise_id": "alpha",
            "team_points": 100.0,
            "is_bye_week": 0,
            "is_consolation": 0,
            "is_playoffs": 0,
            "win": 1,
            "loss": 0,
            "tie": 0,
            "above_league_median": 0,
            "below_league_median": 0,
            "champion": 1,
            "power_rating": 101.0,
        }
    ]
    source_rows = [
        {"source_table": "matchup", "payload": json.dumps(matchup)},
        {
            "source_table": "matchup_season",
            "payload": json.dumps([
                {"db_name": "repair_me", "year": 2025, "franchise_id": "alpha", "power_rating": 101.0}
            ]),
        },
        {
            "source_table": "league_settings",
            "payload": json.dumps([
                {"db_name": "repair_me", "year": 2025, "uses_median": False}
            ]),
        },
    ]

    class Reader:
        def query(self, sql: str, *, database: str):
            assert database == "___leagues"
            if "ranking_source_snapshot" in sql:
                return source_rows
            return [{"base_generation": 4, "matchup_rows": 1, "ranking_rows": 0}]

    publication = {}

    def merge(self, path, *, bundle_id, bundle_hash, merge_timeout_seconds):
        with tarfile.open(path, "r:gz") as archive:
            manifest = json.load(archive.extractfile("manifest.json"))
        publication.update(
            manifest=manifest,
            bundle_id=bundle_id,
            bundle_hash=bundle_hash,
            timeout=merge_timeout_seconds,
        )
        return {"status": "COMMITTED", "generation": 5}

    monkeypatch.setattr(FlyTarget, "merge_fleet_partition", merge)
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://example.test")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "test-token")
    lease = []

    result = repair_missing_manager_rankings_if_needed(
        reader=Reader(),
        db_name="repair_me",
        active_year=2026,
        before_publish=lambda: lease.append("renewed"),
    )

    assert lease == ["renewed"]
    assert result["published"] is True
    assert result["published_tables"] == ["homepage_manager_rankings"]
    assert result["base_generation"] == 4
    assert publication["manifest"]["league_generations"] == {"repair_me": 4}
    assert {table["table"] for table in publication["manifest"]["tables"]} == {
        "homepage_manager_rankings"
    }
    assert publication["timeout"] == 40
