"""Keep league-data preflight scans from contending with one another."""

from __future__ import annotations

from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[4]


@pytest.mark.parametrize(
    "entrypoint",
    [
        "refresh_yahoo_active_season.py",
        "refresh_sleeper_active_season.py",
        "refresh_espn_active_season.py",
    ],
)
def test_heavy_source_reads_are_outside_parallel_preflight(entrypoint: str) -> None:
    source = (ROOT / "scripts" / entrypoint).read_text(encoding="utf-8")
    parallel_block = source.split(
        "preflight = run_independent_refresh_preflight({", 1
    )[1].split("})", 1)[0]

    assert '"entitlement"' in parallel_block
    assert '"canonical_history"' in parallel_block
    assert '"active_inputs"' not in parallel_block
    assert '"persisted_plan"' not in parallel_block
    assert source.index("load_persisted_refresh_plan(", source.index("preflight =")) < source.index(
        "_load_active_refresh_inputs(", source.index("preflight =")
    )
    assert '"source_plan_stage_seconds": source_plan_stage_seconds' in source


@pytest.mark.parametrize(
    "entrypoint",
    [
        "refresh_yahoo_active_season.py",
        "refresh_sleeper_active_season.py",
        "refresh_espn_active_season.py",
    ],
)
def test_weekly_refresh_publishes_bounded_local_homepage_frames(entrypoint: str) -> None:
    source = (ROOT / "scripts" / entrypoint).read_text(encoding="utf-8")

    assert "prepare_homepage_refresh(" in source
    assert "homepage_source_future = start_background_refresh_call(" in source
    assert "source_frames=homepage_source_future.result()" in source
    assert "publication_schema_version=FLEET_CAREER_SCHEMA_VERSION" in source
    assert "rebuild_homepage_rollups=False" in source
    assert "FLEET_HOMEPAGE_SCHEMA_VERSION" not in source
