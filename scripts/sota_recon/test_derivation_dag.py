"""Contract tests for the O.6 independent-derivation DAG (§16.1).

Run:  python -m pytest scripts/sota_recon/test_derivation_dag.py -q
"""

from __future__ import annotations

import os

import pytest

from . import derivation_dag as DD
from . import relationship_edges as RE

pytestmark = pytest.mark.skipif(
    not os.path.exists(RE.CONTRACT_PATH) or not os.path.exists(DD.DIVERSITY_PATH),
    reason="edge contract or diversity map not yet generated")


def _doc():
    return DD.build()


def test_build_is_deterministic():
    assert DD.build() == DD.build()


def test_counters_partition_single_root_cells():
    doc = _doc()
    c = doc["counters"]
    assert c["single_root_cells"] == (
        c["single_root_cells_with_enforceable_edges"]
        + c["single_root_cells_excluded_model_metric"]
        + c["single_root_cells_excluded_definition_isolated"]
        + c["single_root_cells_excluded_era_out_of_domain"]
        + c["single_root_cells_with_zero_enforceable_edges"])


def test_model_metric_exclusion_only_ngs():
    doc = _doc()
    for cell in doc["cells"]:
        assert cell["excluded_model_metric"] == cell["stat"].startswith("ngs_")
    for row in doc["burn_down_queue_root_poorest_first"]:
        assert not row["stat"].startswith("ngs_")


def test_definition_isolated_exclusions_are_receipt_gated():
    """The exclusion holds ONLY while its do-not-enforce edge is in the contract;
    a missing receipt drops the cell back into the queue."""
    doc = _doc()
    dne = {e["edge_id"] for e in RE.load()["edges"] if e.get("do_not_enforce")}
    for cell in doc["cells"]:
        if cell["excluded_definition_isolated"]:
            assert cell["definition_isolated_receipt"] in dne
        if cell["stat"] in DD.EXCLUDED_DEFINITION_ISOLATED \
                and DD.EXCLUDED_DEFINITION_ISOLATED[cell["stat"]] not in dne:
            assert not cell["excluded_definition_isolated"]


def test_queue_rows_carry_rejection_registry_pointers():
    doc = _doc()
    for row in doc["burn_down_queue_root_poorest_first"]:
        assert "do_not_enforce_edges_touching" in row


def test_burn_down_queue_matches_counter():
    doc = _doc()
    assert len(doc["burn_down_queue_root_poorest_first"]) == \
        doc["counters"]["single_root_cells_with_zero_enforceable_edges"]


def test_identity_paths_only_from_enforceable_edges():
    doc = _doc()
    enforceable = {e["edge_id"] for e in RE.enforceable_edges()}
    for cell in doc["cells"]:
        for p in cell["identity_paths"]:
            assert p["via_edge"] in enforceable, p["via_edge"]
            assert not p["blocked_by_unrooted"]


def test_direct_witness_paths_match_diversity_map():
    doc = _doc()
    for cell in doc["cells"]:
        assert cell["n_direct_roots"] == len(cell["direct_roots"])
        assert cell["single_root"] == (cell["n_direct_roots"] == 1)


def test_derivation_diversity_counts_distinct_root_sets():
    doc = _doc()
    for cell in doc["cells"]:
        assert cell["derivation_diversity"] == len(cell["distinct_root_sets"])
