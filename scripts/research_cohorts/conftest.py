"""Shared test scaffolding for the research-cohort builders.

TIER CONTRACT v2 (2026-08-02). `teams_QB/RB/WR/TE` are now the OBSERVED-CAPACITY tier -- cut
from how many roster spots at that position a league ACTUALLY fills, averaged over its weeks,
rather than from declared `roster_*` columns. So `cohort_league_settings_sql(position_slots=
True)` reads two sources it never touched before:

    public.player_fantasy                        the observed roster rows
    ops.nfl_historical.nfl_player_stats_all      the position index (a separate ATTACHED
                                                 catalog -- LocalReader attaches the cached
                                                 ops_cache.duckdb, and the GitHub workflow
                                                 restores that same file from cache)

Any test that builds a toy DuckDB and runs that SQL must therefore provide both, or the query
fails to bind. `ensure_capacity_sources` does it once here instead of hand-editing every
fixture, so a test added later inherits it automatically.

EMPTY IS THE CORRECT DEFAULT. With no observed rows every `teams_*` comes back NULL, which is
the "observed nothing, invent nothing" branch the contract requires -- not a fabricated bucket.
"""
from __future__ import annotations

import pytest


def ensure_capacity_sources(con) -> None:
    """Give a connection the two sources the v2 tier reads. Idempotent."""
    attached = con.execute(
        "SELECT COUNT(*) FROM duckdb_databases() WHERE database_name = 'ops'"
    ).fetchone()[0]
    if not attached:
        con.execute("ATTACH ':memory:' AS ops")
    # DuckDB's IF NOT EXISTS still raises on `public`, which every connection already owns,
    # so only create what is genuinely absent.
    con.execute("CREATE SCHEMA IF NOT EXISTS ops.nfl_historical")
    con.execute(
        "CREATE TABLE IF NOT EXISTS ops.nfl_historical.nfl_player_stats_all "
        "(NFL_player_id VARCHAR, \"year\" INTEGER, position VARCHAR)"
    )
    # DO NOT pre-create public.player_fantasy. Fixtures that test matchup SQL build their
    # own with real rows, and an IF NOT EXISTS empty table here wins the race and silently
    # swallows their data -- the tests then fail with "fixture player did not reach the final
    # table", which reads like a bug in the builder rather than in this file. Only `ops` is
    # genuinely absent from those fixtures.


@pytest.fixture(autouse=True)
def _capacity_sources_on_every_duckdb_connection(monkeypatch):
    """Attach the capacity sources to every in-test DuckDB connection.

    Autouse so a test written next month cannot forget it and get a bind error that looks
    like a bug in the tier rather than a missing fixture.
    """
    import duckdb

    real_connect = duckdb.connect

    def connect_with_capacity(*args, **kwargs):
        con = real_connect(*args, **kwargs)
        try:
            ensure_capacity_sources(con)
        except Exception:
            # A read-only or already-populated connection is fine; never mask a real failure
            # in the test itself by refusing to hand back the connection.
            pass
        return con

    monkeypatch.setattr(duckdb, "connect", connect_with_capacity)
    yield
