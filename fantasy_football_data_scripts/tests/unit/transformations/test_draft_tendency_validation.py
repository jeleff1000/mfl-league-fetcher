import duckdb
import sys
from pathlib import Path


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


def _candidate(**overrides):
    row = {
        "db_name": "demo_league",
        "scope_type": "manager",
        "scope_key": "alice",
        "scope_label": "Alice",
        "feature_type": "archetype",
        "feature_value": "QB_mobile",
        "picks": 10,
        "years_seen": 4,
        "positive_years": 3,
        "negative_years": 1,
        "repeatability": 0.75,
        "observed_capital": 140.0,
        "expected_capital": 35.0,
        "excess_capital": 105.0,
        "observed_share": 0.40,
        "expected_share": 0.10,
        "lift": 4.0,
        "capital_z_score": 6.0,
        "confidence": "high",
        "evidence_level": "briefing_candidate",
        "model_version": "draft-tendency-v0.1",
    }
    row.update(overrides)
    return row


def _inefficiency_candidate(**overrides):
    row = {
        "db_name": "demo_league",
        "scope_type": "league_inefficiency",
        "scope_key": "demo_league",
        "scope_label": "demo_league",
        "feature_type": "archetype",
        "feature_value": "RB_rookie",
        "picks": 18,
        "years_seen": 4,
        "positive_years": 3,
        "negative_years": 1,
        "repeatability": 0.75,
        "observed_capital": 240.0,
        "observed_residual": 22.5,
        "expected_residual": 4.0,
        "excess_residual": 18.5,
        "observed_pick_score": 0.85,
        "expected_pick_score": 0.10,
        "pick_score_delta": 0.75,
        "value_z_score": 6.0,
        "confidence": "high",
        "evidence_level": "briefing_candidate",
        "model_version": "draft-league-inefficiency-v0.1",
    }
    row.update(overrides)
    return row


def test_validation_promotes_repeatable_archetype_state():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidate

    result = validate_tendency_candidate(_candidate())

    assert result["promotion_level"] == "state_ready"
    assert result["validation_status"] == "validated"
    assert result["state_family"] == "player_archetype"
    assert result["shrunk_z_score"] < result["capital_z_score"]
    assert result["signal_direction"] == "overweight"
    assert result["signal_metric"] == "capital_affinity"
    assert result["state_key"].startswith("capital_affinity:")


def test_validation_uses_stronger_priors_for_entity_affinities():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidate

    result = validate_tendency_candidate(
        _candidate(
            feature_type="current_nfl_team",
            feature_value="den",
            picks=10,
            years_seen=4,
            observed_capital=140.0,
            expected_capital=35.0,
            capital_z_score=6.0,
        )
    )

    assert result["state_family"] == "entity_affinity"
    assert result["promotion_level"] == "briefing_watch"
    assert result["state_key"].endswith(":current_nfl_team:den")


def test_validation_promotes_prior_award_profiles():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidate

    result = validate_tendency_candidate(
        _candidate(
            feature_type="award_history",
            feature_value="prev_allpro",
            picks=7,
            years_seen=3,
            observed_capital=150.0,
            expected_capital=25.0,
            capital_z_score=7.0,
            repeatability=0.67,
        )
    )

    assert result["state_family"] == "award_profile"
    assert result["promotion_level"] == "state_ready"
    assert ":award_history:prev_allpro" in result["state_key"]


def test_validation_promotes_prior_production_profiles():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidate

    result = validate_tendency_candidate(
        _candidate(
            feature_type="rb_receiving_profile",
            feature_value="pass_catching_rb",
            picks=8,
            years_seen=3,
            observed_capital=135.0,
            expected_capital=30.0,
            capital_z_score=6.5,
            repeatability=0.67,
        )
    )

    assert result["state_family"] == "prior_production_profile"
    assert result["promotion_level"] == "state_ready"
    assert ":rb_receiving_profile:pass_catching_rb" in result["state_key"]


def test_validation_keeps_one_year_spikes_as_explore_only():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidate

    result = validate_tendency_candidate(
        _candidate(
            picks=4,
            years_seen=1,
            positive_years=1,
            negative_years=0,
            repeatability=1.0,
            observed_capital=100.0,
            expected_capital=10.0,
            capital_z_score=8.0,
        )
    )

    assert result["promotion_level"] == "explore_only"
    assert result["sample_reliability"] < 0.25


def test_validation_discounts_stale_evidence_with_recency_weight():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidate

    fresh = validate_tendency_candidate(_candidate(recency_weight=1.0))
    stale = validate_tendency_candidate(_candidate(recency_weight=0.15))

    assert fresh["promotion_level"] == "state_ready"
    assert stale["promotion_level"] == "explore_only"
    assert stale["shrunk_z_score"] < fresh["shrunk_z_score"]
    assert stale["recency_weight"] == 0.15


def test_validation_applies_fdr_across_candidate_set():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidates

    strong = _candidate(scope_key="strong", scope_label="Strong", capital_z_score=8.0)
    marginal = _candidate(scope_key="marginal", scope_label="Marginal", capital_z_score=2.4)
    noise_rows = [
        _candidate(
            scope_key=f"noise_{idx}",
            scope_label=f"Noise {idx}",
            picks=7,
            years_seen=3,
            observed_capital=55.0,
            expected_capital=45.0,
            capital_z_score=0.5,
            repeatability=0.67,
        )
        for idx in range(40)
    ]

    result = validate_tendency_candidates([strong, marginal, *noise_rows])
    by_key = {row["scope_key"]: row for row in result}

    assert by_key["strong"]["promotion_level"] == "state_ready"
    assert by_key["strong"]["q_value"] <= 0.05
    assert by_key["marginal"]["promotion_level"] == "explore_only"
    assert by_key["marginal"]["validation_status"] == "fdr_rejected"


def test_validation_promotes_league_inefficiency_candidate():
    from multi_league.transformations.draft.tendency_validation import validate_inefficiency_candidates

    result = validate_inefficiency_candidates([_inefficiency_candidate()])[0]

    assert result["promotion_level"] == "state_ready"
    assert result["signal_metric"] == "value_inefficiency"
    assert result["state_key"].startswith("value_inefficiency:")
    assert result["observed_value"] == 22.5
    assert result["expected_value"] == 4.0
    assert result["value_delta"] == 18.5
    assert result["pick_score_delta"] == 0.75


def test_validation_caps_state_ready_per_scope():
    from multi_league.transformations.draft.tendency_validation import validate_tendency_candidates

    rows = [
        _candidate(
            scope_key="alice",
            feature_type="archetype",
            feature_value=f"feature_{idx}",
            capital_z_score=8.0 - (idx * 0.1),
            observed_capital=220.0 - idx,
            expected_capital=35.0,
        )
        for idx in range(8)
    ]

    result = validate_tendency_candidates(rows)
    levels = [row["promotion_level"] for row in result]
    ranks = sorted(row["promotion_rank"] for row in result if row["promotion_rank"] is not None)

    assert levels.count("state_ready") == 4
    assert levels.count("briefing_watch") == 4
    assert ranks == list(range(1, 9))


def test_replace_validated_tendency_states_writes_scoped_rows():
    from multi_league.transformations.draft.tendency_miner import (
        DRAFT_TENDENCY_COLUMNS,
        create_draft_tendency_table,
    )
    from multi_league.transformations.draft.tendency_validation import replace_validated_tendency_states

    conn = _setup_conn()
    create_draft_tendency_table(conn)
    rows = [
        _candidate(),
        _candidate(scope_key="bob", scope_label="Bob", feature_value="RB_rookie", capital_z_score=2.5),
    ]
    conn.executemany(
        f"INSERT INTO public.draft_tendency_candidate ({', '.join(DRAFT_TENDENCY_COLUMNS)}) "
        f"VALUES ({', '.join(['?'] * len(DRAFT_TENDENCY_COLUMNS))})",
        [[row.get(col) for col in DRAFT_TENDENCY_COLUMNS] for row in rows],
    )

    count = replace_validated_tendency_states(conn, "demo_league")

    assert count == 2
    ready_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM public.draft_intelligence_state_candidate
        WHERE db_name = 'demo_league' AND promotion_level = 'state_ready'
        """
    ).fetchone()[0]
    assert ready_count == 1


def test_replace_validated_inefficiency_states_writes_scoped_rows():
    from multi_league.transformations.draft.league_inefficiency_miner import (
        DRAFT_INEFFICIENCY_COLUMNS,
        create_draft_inefficiency_table,
    )
    from multi_league.transformations.draft.tendency_validation import replace_validated_inefficiency_states

    conn = _setup_conn()
    create_draft_inefficiency_table(conn)
    rows = [
        _inefficiency_candidate(),
        _inefficiency_candidate(feature_value="QB_mobile", value_z_score=2.4),
    ]
    conn.executemany(
        f"INSERT INTO public.draft_inefficiency_candidate ({', '.join(DRAFT_INEFFICIENCY_COLUMNS)}) "
        f"VALUES ({', '.join(['?'] * len(DRAFT_INEFFICIENCY_COLUMNS))})",
        [[row.get(col) for col in DRAFT_INEFFICIENCY_COLUMNS] for row in rows],
    )

    count = replace_validated_inefficiency_states(conn, "demo_league")

    assert count == 2
    ready_count = conn.execute(
        """
        SELECT COUNT(*)
        FROM public.draft_intelligence_state_candidate
        WHERE db_name = 'demo_league'
          AND signal_metric = 'value_inefficiency'
          AND promotion_level = 'state_ready'
        """
    ).fetchone()[0]
    assert ready_count == 1
