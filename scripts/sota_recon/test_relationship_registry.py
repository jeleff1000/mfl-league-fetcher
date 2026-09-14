"""Registry parity tests (O.7 slice 4): retirement is receipted, never silent."""

from scripts.sota_recon.relationship_registry import (RELATIONSHIPS, RETIRED,
                                                      registry_keys)


def test_registry_keys_are_unique_and_complete():
    keys = {r.key for r in RELATIONSHIPS}
    assert len(keys) == len(RELATIONSHIPS)
    assert registry_keys() == keys


def test_retired_and_kept_are_disjoint():
    assert not registry_keys() & set(RETIRED)


def test_kept_rows_are_outside_the_grid():
    """Everything still in RELATIONSHIPS must be a class the R1-R9 edge contract
    cannot express -- anything else belongs in the contract, not here."""
    allowed = {"source_definition", "identity_limit", "cross_team_bound",
               "scoring_law", "context_gate"}
    for r in RELATIONSHIPS:
        assert r.kind in allowed, r.key


def test_retirement_parity_receipts():
    """Every retired row's superseding edge exists in the committed contract with
    a typed verdict (enforceable gate or do-not-enforce counterexample row)."""
    import os

    import pytest

    from scripts.sota_recon import relationship_edges as RE

    if not os.path.exists(RE.CONTRACT_PATH):
        pytest.skip("edge contract not generated")
    edges = {e["edge_id"]: e for e in RE.load()["edges"]}
    for key, edge_ids in RETIRED.items():
        assert edge_ids, key
        for eid in edge_ids:
            assert eid in edges, f"{key}: superseding edge {eid} missing"
            e = edges[eid]
            assert e["enforceable"] or e.get("do_not_enforce"), \
                f"{key}: superseding edge {eid} is neither a gate nor a " \
                f"receipted rejection ({e['verdict']})"
