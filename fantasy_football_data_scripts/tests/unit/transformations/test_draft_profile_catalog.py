import sys
from pathlib import Path

import duckdb


SCRIPTS_DIR = Path(__file__).resolve().parent.parent.parent.parent
for path in (SCRIPTS_DIR, SCRIPTS_DIR / "multi_league"):
    path_str = str(path)
    if path_str not in sys.path:
        sys.path.insert(0, path_str)


def _setup_conn():
    conn = duckdb.connect(":memory:")
    conn.execute("ATTACH ':memory:' AS ___leagues")
    conn.execute('USE "___leagues"')
    conn.execute("CREATE SCHEMA IF NOT EXISTS public")
    return conn


def _state(**overrides):
    row = {
        "db_name": "league_a",
        "scope_type": "manager",
        "scope_key": "manager_a",
        "scope_label": "Manager A",
        "state_key": "capital_affinity:player_profile:draft.position_age:rb_age_23_under",
        "state_family": "position_profile",
        "feature_type": "draft.position_age",
        "feature_value": "RB_age_23_under",
        "signal_metric": "capital_affinity",
        "signal_direction": "overweight",
        "promotion_level": "state_ready",
        "validation_score": 88.0,
        "q_value": 0.01,
        "shrunk_z_score": 4.2,
        "shrunk_lift": 2.4,
        "value_delta": 0.18,
        "picks": 12,
        "years_seen": 4,
    }
    row.update(overrides)
    return row


def test_learn_profile_catalog_requires_fleet_support():
    from multi_league.transformations.draft.profile_catalog import learn_profile_catalog

    repeated = [
        _state(db_name=f"league_{idx}", scope_key=f"manager_{idx}", scope_label=f"Manager {idx}") for idx in range(4)
    ]
    one_off = _state(
        scope_key="lonely",
        scope_label="Lonely",
        feature_value="QB_mobile",
        state_key="capital_affinity:player_archetype:qb_mobile",
    )

    catalog = learn_profile_catalog(
        [*repeated, one_off],
        min_manager_scopes=4,
        min_league_scopes=2,
    )

    assert len(catalog) == 1
    profile = catalog[0]
    assert profile["profile_kind"] == "manager"
    assert profile["profile_status"] == "catalog_ready"
    assert profile["scope_count"] == 4
    assert profile["league_count"] == 4
    assert profile["feature_value"] == "RB_age_23_under"


def test_league_inefficiency_profiles_are_separate_from_manager_profiles():
    from multi_league.transformations.draft.profile_catalog import learn_profile_catalog

    rows = [
        _state(
            db_name=f"league_{idx}",
            scope_type="league_inefficiency",
            scope_key=f"league_{idx}",
            scope_label=f"league_{idx}",
            state_family="position_market",
            feature_type="position_market.rb_rookie",
            feature_value="snake_r3_5",
            signal_metric="value_inefficiency",
            signal_direction="underweight",
            state_key="value_inefficiency:position_market:rb_rookie",
        )
        for idx in range(5)
    ]

    catalog = learn_profile_catalog(rows, min_league_scopes=4)

    assert len(catalog) == 1
    assert catalog[0]["profile_kind"] == "league"
    assert catalog[0]["scope_type"] == "league_inefficiency"
    assert catalog[0]["signal_metric"] == "value_inefficiency"


def test_assign_catalog_profiles_caps_and_ranks_per_scope():
    from multi_league.transformations.draft.profile_catalog import (
        assign_catalog_profiles,
        learn_profile_catalog,
    )

    shared_profiles = []
    scope_rows = []
    for idx in range(8):
        feature_value = f"profile_{idx}"
        shared_profiles.extend(
            [
                _state(
                    db_name=f"training_{league_idx}",
                    scope_key=f"training_manager_{idx}_{league_idx}",
                    feature_value=feature_value,
                    state_key=f"capital_affinity:profile:{feature_value}",
                    validation_score=90 - idx,
                )
                for league_idx in range(4)
            ]
        )
        scope_rows.append(
            _state(
                db_name="target_league",
                scope_key="target_manager",
                feature_value=feature_value,
                state_key=f"capital_affinity:profile:{feature_value}",
                validation_score=95 - idx,
            )
        )

    catalog = learn_profile_catalog(shared_profiles, min_manager_scopes=4)
    assignments = assign_catalog_profiles(scope_rows, catalog, max_assignments_per_scope=3)

    assert len(assignments) == 3
    assert [row["assignment_rank"] for row in assignments] == [1, 2, 3]
    assert assignments[0]["assignment_score"] > assignments[-1]["assignment_score"]
    assert {row["scope_key"] for row in assignments} == {"target_manager"}


def test_create_profile_tables_and_replace_from_state_candidates():
    from multi_league.transformations.draft.profile_catalog import (
        DRAFT_PROFILE_ASSIGNMENT_COLUMNS,
        DRAFT_PROFILE_CATALOG_COLUMNS,
        create_draft_profile_tables,
        replace_profile_catalog_and_assignments,
    )
    from multi_league.transformations.draft.tendency_validation import (
        DRAFT_STATE_COLUMNS,
        create_draft_state_table,
    )

    conn = _setup_conn()
    create_draft_state_table(conn)
    create_draft_profile_tables(conn)
    rows = [
        _state(db_name=f"league_{idx}", scope_key=f"manager_{idx}", scope_label=f"Manager {idx}") for idx in range(4)
    ]
    conn.executemany(
        f"INSERT INTO public.draft_intelligence_state_candidate ({', '.join(DRAFT_STATE_COLUMNS)}) "
        f"VALUES ({', '.join(['?'] * len(DRAFT_STATE_COLUMNS))})",
        [[row.get(col) for col in DRAFT_STATE_COLUMNS] for row in rows],
    )

    result = replace_profile_catalog_and_assignments(conn, min_manager_scopes=4)

    assert result == {"catalog": 1, "assignments": 4}
    catalog_count = conn.execute(
        "SELECT COUNT(*) FROM public.draft_profile_catalog WHERE profile_id IS NOT NULL"
    ).fetchone()[0]
    assignment_cols = conn.execute("DESCRIBE public.draft_profile_assignment").fetchall()
    catalog_cols = conn.execute("DESCRIBE public.draft_profile_catalog").fetchall()

    assert catalog_count == 1
    assert {row[0] for row in catalog_cols} >= set(DRAFT_PROFILE_CATALOG_COLUMNS)
    assert {row[0] for row in assignment_cols} >= set(DRAFT_PROFILE_ASSIGNMENT_COLUMNS)
