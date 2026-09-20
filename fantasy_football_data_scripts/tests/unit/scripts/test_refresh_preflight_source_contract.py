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
