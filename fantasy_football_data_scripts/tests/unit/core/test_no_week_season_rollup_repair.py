from __future__ import annotations

import json
import tarfile
from pathlib import Path


def test_no_week_repair_is_one_generation_fenced_context_witness(monkeypatch):
    from multi_league.core.no_week_season_rollup_repair import (
        repair_missing_season_rollups_if_needed,
    )
    from multi_league.core.targets.fly_target import FlyTarget

    class Reader:
        def __init__(self):
            self.gap_checks = 0

        def query(self, sql: str, *, database: str):
            assert database == "___leagues"
            if "missing_derived_years" in sql:
                self.gap_checks += 1
                return [{"missing_derived_years": "2019,2020" if self.gap_checks == 1 else ""}]
            assert "public.league_context" in sql
            return [{
                "base_generation": 7,
                "context_payload": json.dumps({
                    "db_name": "repair_me",
                    "league_name": "Repair Me",
                    "platform": "yahoo",
                }),
            }]

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
        return {
            "status": "COMMITTED",
            "generation": 8,
            "season_rollups": {
                "repair_me": {
                    "player_fantasy_season": 120,
                    "player_fantasy_season_all": 122,
                    "standings_by_year": 20,
                }
            },
            "career_rollups": {"repair_me": {"player_fantasy_career": 80}},
        }

    monkeypatch.setattr(FlyTarget, "merge_fleet_partition", merge)
    monkeypatch.setenv("DATABASE_SERVER_URL", "https://example.test")
    monkeypatch.setenv("DATABASE_ADMIN_TOKEN", "test-token")
    lease = []

    result = repair_missing_season_rollups_if_needed(
        reader=Reader(),
        db_name="repair_me",
        active_year=2026,
        before_publish=lambda: lease.append("renewed"),
    )

    assert lease == ["renewed"]
    assert result["published"] is True
    assert result["base_generation"] == 7
    assert result["missing_years"] == [2019, 2020]
    assert result["remaining_missing_years"] == []
    assert publication["manifest"]["league_generations"] == {"repair_me": 7}
    assert publication["manifest"]["repair_missing_season_rollups"] is True
    assert publication["manifest"]["schema_version"] == "fleet-partition-v2"
    assert {table["table"] for table in publication["manifest"]["tables"]} == {
        "league_context"
    }
    assert publication["timeout"] == 40


def test_no_week_repair_does_not_publish_when_aggregates_are_complete(monkeypatch):
    from multi_league.core.no_week_season_rollup_repair import (
        repair_missing_season_rollups_if_needed,
    )
    from multi_league.core.targets.fly_target import FlyTarget

    class Reader:
        def query(self, sql: str, *, database: str):
            assert database == "___leagues"
            assert "missing_derived_years" in sql
            return [{"missing_derived_years": ""}]

    def forbidden(*args, **kwargs):
        raise AssertionError("healthy no-week refresh must not publish")

    monkeypatch.setattr(FlyTarget, "merge_fleet_partition", forbidden)
    result = repair_missing_season_rollups_if_needed(
        reader=Reader(), db_name="healthy", active_year=2026,
    )

    assert result == {"missing_years": [], "published": False}


def test_all_platform_no_week_paths_repair_season_rollups_before_rankings():
    root = Path(__file__).resolve().parents[4]
    for script_name in (
        "refresh_yahoo_active_season.py",
        "refresh_sleeper_active_season.py",
        "refresh_espn_active_season.py",
    ):
        source = (root / "scripts" / script_name).read_text(encoding="utf-8")
        no_weeks = source.index("if not refresh_weeks:")
        noop_status = source.index('receipt["status"] = "NO_FINALIZED_WEEKS"', no_weeks)
        season_repair = source.index("record_missing_season_rollups_repair", no_weeks)
        ranking_repair = source.index("record_missing_manager_rankings_repair", no_weeks)
        assert no_weeks < season_repair < ranking_repair < noop_status
