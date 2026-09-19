from __future__ import annotations

from threading import Barrier

from multi_league.core.league_refresh import run_independent_refresh_preflight


def test_refresh_preflight_runs_independent_reads_concurrently() -> None:
    barrier = Barrier(2)

    def read(value: str) -> str:
        barrier.wait(timeout=1)
        return value

    result = run_independent_refresh_preflight(
        {
            "history": lambda: read("history-ok"),
            "manifest": lambda: read("manifest-ok"),
        }
    )

    assert result == {"history": "history-ok", "manifest": "manifest-ok"}
