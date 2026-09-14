from __future__ import annotations

import duckdb

from multi_league.validation_v2.executor import run_sql_full
from multi_league.validation_v2.models import Manifest


def _check(name: str):
    from multi_league.validation_v2.checks.pipeline_health import CHECKS

    return next(c for c in CHECKS if c.name == name)


def test_pipeline_nfl_id_zero_pct_ignores_stub_rows():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    conn.execute(
        """
        CREATE TABLE public.player_fantasy (
            db_name VARCHAR,
            manager VARCHAR,
            position VARCHAR,
            NFL_player_id VARCHAR
        )
        """
    )
    conn.execute(
        """
        INSERT INTO public.player_fantasy VALUES
            ('stub_only_league', 'Hidden Manager', 'STUB', NULL),
            ('mapped_league', 'Real Manager', 'RB', '00-0031234'),
            ('broken_league', 'Real Manager', 'RB', NULL)
        """
    )

    manifest = Manifest(all_leagues=["stub_only_league", "mapped_league", "broken_league"])
    results = run_sql_full(conn, _check("pipeline_nfl_id_zero_pct"), manifest, table_prefix="public.")
    result_map = {r.db_name: r for r in results}

    assert result_map["stub_only_league"].passed is True
    assert result_map["mapped_league"].passed is True
    assert result_map["broken_league"].passed is False

    conn.close()
