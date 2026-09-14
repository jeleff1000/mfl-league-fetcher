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


def _assignment(**overrides):
    row = {
        "db_name": "league_a",
        "scope_type": "manager",
        "scope_key": "manager_a",
        "scope_label": "Manager A",
        "profile_id": "manager_position_profile_young_rb",
        "profile_kind": "manager",
        "profile_status": "catalog_ready",
        "assignment_rank": 1,
        "assignment_score": 91.0,
        "evidence_state_key": "position_profile:draft.position_age:rb_age_23_under",
        "state_family": "position_profile",
        "feature_type": "draft.position_age",
        "feature_value": "RB_age_23_under",
        "signal_metric": "capital_affinity",
        "signal_direction": "overweight",
        "promotion_level": "state_ready",
        "validation_score": 88.0,
        "shrunk_z_score": 4.2,
        "value_delta": 0.18,
        "picks": 12,
        "years_seen": 4,
        "model_version": "test",
    }
    row.update(overrides)
    return row


def _combo_rows(count=4):
    rows = []
    for idx in range(count):
        rows.append(
            _assignment(
                db_name=f"league_{idx}",
                scope_key=f"manager_{idx}",
                scope_label=f"Manager {idx}",
                profile_id="profile_young_rb",
                assignment_rank=1,
                assignment_score=93 - idx,
                feature_value="RB_age_23_under",
            )
        )
        rows.append(
            _assignment(
                db_name=f"league_{idx}",
                scope_key=f"manager_{idx}",
                scope_label=f"Manager {idx}",
                profile_id="profile_workhorse_rb",
                assignment_rank=2,
                assignment_score=89 - idx,
                feature_type="archetype",
                feature_value="RB_workhorse",
            )
        )
    return rows


def test_learn_profile_archetypes_builds_composite_from_repeated_signals():
    from multi_league.transformations.draft.profile_archetypes import learn_profile_archetypes

    one_off = _assignment(
        db_name="league_lonely",
        scope_key="manager_lonely",
        profile_id="profile_broncos",
        feature_type="draft.nfl_team",
        feature_value="DEN",
    )

    archetypes = learn_profile_archetypes(
        [*_combo_rows(), one_off],
        min_manager_scopes=4,
        min_league_scopes=2,
    )

    assert archetypes
    top = archetypes[0]
    assert top["archetype_kind"] == "manager"
    assert top["archetype_status"] == "archetype_ready"
    assert top["profile_count"] == 2
    assert top["scope_count"] == 4
    label = top["archetype_label"].lower()
    assert "targets rb age 23 under" in label
    assert "targets rb workhorse" in label
    assert "profile_broncos" not in top["profile_ids_json"]


def test_assign_profile_archetypes_prefers_composite_over_redundant_singles():
    from multi_league.transformations.draft.profile_archetypes import (
        assign_profile_archetypes,
        learn_profile_archetypes,
    )

    training_rows = _combo_rows()
    archetypes = learn_profile_archetypes(training_rows, min_manager_scopes=4)
    target_rows = [
        _assignment(db_name="target", scope_key="target_manager", profile_id="profile_young_rb"),
        _assignment(
            db_name="target",
            scope_key="target_manager",
            profile_id="profile_workhorse_rb",
            assignment_rank=2,
            feature_type="archetype",
            feature_value="RB_workhorse",
        ),
    ]

    assignments = assign_profile_archetypes(target_rows, archetypes)

    assert len(assignments) == 1
    assert assignments[0]["archetype_rank"] == 1
    assert assignments[0]["supporting_profile_count"] == 2
    assert assignments[0]["matched_profile_ids_json"] == '["profile_workhorse_rb","profile_young_rb"]'


def test_league_archetypes_use_market_language():
    from multi_league.transformations.draft.profile_archetypes import learn_profile_archetypes

    rows = [
        _assignment(
            db_name=f"league_{idx}",
            scope_type="league_inefficiency",
            scope_key=f"league_{idx}",
            scope_label=f"league_{idx}",
            profile_kind="league",
            profile_id="league_underweights_rookie_rb",
            feature_type="position_market.rb_rookie",
            feature_value="rookie_RB_round_3_5",
            signal_metric="value_inefficiency",
            signal_direction="underweight",
        )
        for idx in range(4)
    ]

    archetypes = learn_profile_archetypes(rows, min_league_scopes=4)

    assert len(archetypes) == 1
    assert archetypes[0]["archetype_kind"] == "league"
    assert archetypes[0]["archetype_label"].startswith("League: market underweights")


def test_create_profile_archetype_tables_and_replace_from_profile_assignments():
    from multi_league.transformations.draft.profile_archetypes import (
        DRAFT_PROFILE_ARCHETYPE_ASSIGNMENT_COLUMNS,
        DRAFT_PROFILE_ARCHETYPE_COLUMNS,
        create_draft_profile_archetype_tables,
        replace_profile_archetypes_and_assignments,
    )
    from multi_league.transformations.draft.profile_catalog import (
        DRAFT_PROFILE_ASSIGNMENT_COLUMNS,
        create_draft_profile_tables,
    )

    conn = _setup_conn()
    create_draft_profile_tables(conn)
    create_draft_profile_archetype_tables(conn)
    rows = _combo_rows()
    conn.executemany(
        f"INSERT INTO public.draft_profile_assignment ({', '.join(DRAFT_PROFILE_ASSIGNMENT_COLUMNS)}) "
        f"VALUES ({', '.join(['?'] * len(DRAFT_PROFILE_ASSIGNMENT_COLUMNS))})",
        [[row.get(col) for col in DRAFT_PROFILE_ASSIGNMENT_COLUMNS] for row in rows],
    )

    result = replace_profile_archetypes_and_assignments(conn, min_manager_scopes=4)

    assert result == {"archetypes": 3, "assignments": 4}
    archetype_count = conn.execute(
        "SELECT COUNT(*) FROM public.draft_profile_archetype_catalog WHERE archetype_id IS NOT NULL"
    ).fetchone()[0]
    assignment_count = conn.execute(
        "SELECT COUNT(*) FROM public.draft_profile_archetype_assignment WHERE archetype_id IS NOT NULL"
    ).fetchone()[0]
    archetype_cols = conn.execute("DESCRIBE public.draft_profile_archetype_catalog").fetchall()
    assignment_cols = conn.execute("DESCRIBE public.draft_profile_archetype_assignment").fetchall()

    assert archetype_count == 3
    assert assignment_count == 4
    assert {row[0] for row in archetype_cols} >= set(DRAFT_PROFILE_ARCHETYPE_COLUMNS)
    assert {row[0] for row in assignment_cols} >= set(DRAFT_PROFILE_ARCHETYPE_ASSIGNMENT_COLUMNS)
