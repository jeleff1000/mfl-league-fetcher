from __future__ import annotations

from scripts import merge_league_admin


class _Reader:
    def query_scalar(self, sql: str, *, database: str):
        assert "MAX(TRY_CAST(year AS INTEGER))" in sql
        assert "db_name = 'target_db'" in sql
        assert database == "___leagues"
        return 2026


class _Writer:
    pass


def test_run_merge_repairs_missing_rollups_before_refreshing_homepage(monkeypatch):
    calls: list[object] = []

    monkeypatch.setattr(
        "multi_league.data_fetchers.shared.merge_source_copier.copy_merge_source_to_public",
        lambda ctx, target_db, **kwargs: calls.append(("copy", target_db))
        or {"status": "copied", "matchup": 24},
    )
    monkeypatch.setattr(
        "multi_league.core.no_week_season_rollup_repair.repair_missing_season_rollups_if_needed",
        lambda **kwargs: calls.append(("repair", kwargs["db_name"], kwargs["active_year"]))
        or {
            "published": True,
            "missing_years": [2023, 2024, 2025],
            "remaining_missing_years": [],
            "result": {"status": "COMMITTED"},
        },
    )
    monkeypatch.setattr(
        "multi_league.data_fetchers.shared.merge_source_copier.refresh_merge_source_homepage_aggregates",
        lambda **kwargs: calls.append(("homepage", kwargs["target_db"]))
        or {"homepage_manager_profiles": 12},
        raising=False,
    )

    result = merge_league_admin.run_merge(
        {
            "source_db": "source_db",
            "target_db": "target_db",
            "merge_years": [2023, 2024, 2025],
        },
        reader=_Reader(),
        writer=_Writer(),
    )

    assert calls == [
        ("copy", "target_db"),
        ("repair", "target_db", 2026),
        ("homepage", "target_db"),
    ]
    assert result["season_rollups_repaired"] == "2023, 2024, 2025"
    assert result["homepage_manager_profiles"] == 12


def test_run_merge_skips_homepage_second_pass_without_rollup_repair(monkeypatch):
    calls: list[object] = []

    monkeypatch.setattr(
        "multi_league.data_fetchers.shared.merge_source_copier.copy_merge_source_to_public",
        lambda *args, **kwargs: {"status": "copied"},
    )
    monkeypatch.setattr(
        "multi_league.core.no_week_season_rollup_repair.repair_missing_season_rollups_if_needed",
        lambda **kwargs: {"published": False, "missing_years": []},
    )
    monkeypatch.setattr(
        "multi_league.data_fetchers.shared.merge_source_copier.refresh_merge_source_homepage_aggregates",
        lambda **kwargs: calls.append("homepage") or {},
        raising=False,
    )

    result = merge_league_admin.run_merge(
        {"source_db": "source_db", "target_db": "target_db"},
        reader=_Reader(),
        writer=_Writer(),
    )

    assert calls == []
    assert result["season_rollups_repaired"] == ""
