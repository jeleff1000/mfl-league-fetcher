"""Real profile A/B: shared scratch metadata must not change any output field."""

import json
import re
import time
from collections import Counter
from functools import wraps

import pandas as pd

from fantasy_football_data_scripts.tests.unit.transformations.test_career_rollup_refresh import (
    homepage_chain,  # noqa: F401
    merged_chain,  # noqa: F401
)
from multi_league.transformations.aggregation import homepage_summary as homepage


PROFILE_HELPERS = (
    "_compute_manager_draft_profile", "_compute_manager_txn_profile",
    "_compute_manager_best_trade", "_compute_manager_rivalries",
    "_compute_manager_player_leaders", "_compute_manager_timeline",
)


class CountedConnection:
    def __init__(self, conn, queries):
        self.conn = conn
        self.queries = queries

    def execute(self, sql, params=None):
        self.queries.append(re.sub(r"\s+", " ", sql).strip())
        return self.conn.execute(sql, params) if params is not None else self.conn.execute(sql)

    def __getattr__(self, name):
        return getattr(self.conn, name)


def test_shared_profile_cache_preserves_full_output_and_reduces_metadata(homepage_chain, monkeypatch):
    conn = homepage_chain
    conn.execute("SET threads=8")
    conn.execute("""
        INSERT INTO public.matchup
            (db_name, year, week, manager, franchise_id, opponent, opponent_franchise_id,
             team_name, platform, team_points, opponent_points, win, loss, tie,
             is_playoffs, is_consolation, is_bye_week)
        SELECT db_name, year, week, manager, 'f2', manager, 'f1', 'Different Team',
               platform, opponent_points, team_points, loss, win, tie,
               is_playoffs, is_consolation, is_bye_week
        FROM public.matchup WHERE db_name='test_league'
    """)
    conn.execute("""
        INSERT INTO public.draft
            (db_name, year, manager, franchise_id, player, NFL_player_id,
             draft_value_zscore, manager_lamar, manager_draft_score, manager_draft_grade)
        VALUES ('test_league', 2025, 'Shared Alias', 'f1', 'Player', 'p1', 1.2, 5, 90, 'A')
    """)
    conn.execute("""
        INSERT INTO public.transactions
            (db_name, transaction_id, year, week, manager, franchise_id, player,
             NFL_player_id, transaction_type, manager_lamar_ros_managed, transaction_grade)
        VALUES ('test_league', 'add-1', 2025, 1, 'Shared Alias', 'f1', 'Player', 'p1', 'add', 5, 'A')
    """)
    witness_before = conn.execute("SELECT COUNT(*), bit_xor(hash(m)) FROM public.matchup m").fetchone()
    queries = []
    original_init = homepage.ScopedLocalProfileContext.__init__

    def counted_init(ctx, *args, **kwargs):
        original_init(ctx, *args, **kwargs)
        ctx.local = CountedConnection(ctx.local, queries)
        ctx.cache._conn = ctx.local

    monkeypatch.setattr(homepage.ScopedLocalProfileContext, "__init__", counted_init)

    def without_cache(original):
        @wraps(original)
        def call(*args, **kwargs):
            kwargs.pop("cache", None)
            return original(*args, **kwargs)
        return call

    outputs, measures = {}, {}
    # Both directions reduce simple warm-up/order bias without a larger fixture.
    for label in ("uncached", "cached", "cached", "uncached"):
        queries.clear()
        with monkeypatch.context() as trial:
            if label == "uncached":
                for name in PROFILE_HELPERS:
                    trial.setattr(homepage, name, without_cache(getattr(homepage, name)))
            started = time.perf_counter()
            result = homepage.compute_all_manager_profiles(conn, "test_league", platform="sleeper")
            elapsed = time.perf_counter() - started
        result = result.sort_values("franchise_id").reset_index(drop=True)
        outputs[label] = result
        metadata = [q for q in queries if "information_schema." in q.lower() or q.upper().startswith("DESCRIBE ")]
        measures.setdefault(label, []).append({
            "seconds": round(elapsed, 6), "metadata_queries": len(metadata),
            "sql_queries": len(queries), "metadata_sql": dict(Counter(metadata)),
        })
        if "uncached" in outputs:
            pd.testing.assert_frame_equal(outputs["uncached"], result, check_exact=True)

    print("PROFILE_CACHE_AB " + json.dumps(measures), flush=True)
    assert outputs["cached"]["franchise_id"].tolist() == ["f1", "f2"]
    assert outputs["cached"]["manager"].tolist() == ["Shared Alias", "Shared Alias"]
    assert all("2025" in value and "2026" in value for value in outputs["cached"]["timeline_data"])
    assert max(m["metadata_queries"] for m in measures["cached"]) < min(
        m["metadata_queries"] for m in measures["uncached"])
    # The schema is stable for this context: one DESCRIBE per needed table.
    assert max(measures["cached"][0]["metadata_sql"].values()) == 1
    assert conn.execute("SELECT COUNT(*), bit_xor(hash(m)) FROM public.matchup m").fetchone() == witness_before
