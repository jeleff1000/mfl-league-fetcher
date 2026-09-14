from __future__ import annotations

import pytest

from scripts.sota_recon.witness_gate.models import ProofNode, ProofNodeKind
from scripts.sota_recon.witness_gate.proof_dag import LineageIdentity, ProofGraph


def node(
    node_id: str,
    kind: ProofNodeKind,
    dependencies: tuple[str, ...] = (),
    lineage_id: str | None = None,
    artifact_fingerprint: str | None = None,
) -> ProofNode:
    return ProofNode(
        contract_version="1",
        node_id=node_id,
        kind=kind,
        atom_id="passing.attempts",
        dependencies=dependencies,
        lineage_id=lineage_id,
        artifact_fingerprint=artifact_fingerprint,
    )


def test_direct_cycle_is_rejected_with_path() -> None:
    graph = ProofGraph(
        nodes=(
            node("a", ProofNodeKind.TRANSFORM, ("b",)),
            node("b", ProofNodeKind.TRANSFORM, ("a",)),
        )
    )

    with pytest.raises(ValueError, match=r"proof cycle.*a.*b.*a"):
        graph.validate()


def test_candidate_cannot_depend_on_itself_through_a_derived_node() -> None:
    graph = ProofGraph(
        nodes=(
            node("candidate", ProofNodeKind.CANDIDATE, ("derived",)),
            node("derived", ProofNodeKind.TRANSFORM, ("candidate",)),
        )
    )

    with pytest.raises(ValueError, match="proof cycle"):
        graph.validate()


def test_unknown_dependency_is_rejected() -> None:
    graph = ProofGraph(nodes=(node("derived", ProofNodeKind.TRANSFORM, ("missing",)),))

    with pytest.raises(ValueError, match="unknown dependency"):
        graph.validate()


def test_leaf_expansion_and_topological_order_are_deterministic() -> None:
    graph = ProofGraph(
        nodes=(
            node("candidate", ProofNodeKind.CANDIDATE, ("sum",)),
            node("leaf-b", ProofNodeKind.LEAF, lineage_id="nfl", artifact_fingerprint="sha256:b"),
            node("sum", ProofNodeKind.TRANSFORM, ("leaf-b", "leaf-a")),
            node("leaf-a", ProofNodeKind.LEAF, lineage_id="pfr", artifact_fingerprint="sha256:a"),
        )
    )

    graph.validate()

    assert graph.leaf_nodes("candidate") == ("leaf-a", "leaf-b")
    order = graph.topological_order()
    assert order.index("leaf-a") < order.index("sum") < order.index("candidate")


def test_mirror_aliases_are_one_independent_leaf_group() -> None:
    pfr_lineage = LineageIdentity(
        acquisition_family="pfr-html",
        upstream_publisher="sports-reference",
        transformation_chain=("raw_html", "boxscore_parser_v2"),
        raw_artifact_fingerprint="sha256:pfr-box",
    )
    nfl_lineage = LineageIdentity(
        acquisition_family="nfl-api",
        upstream_publisher="nfl",
        transformation_chain=("raw_json", "nfl_normalizer_v1"),
        raw_artifact_fingerprint="sha256:nfl-game",
    )
    graph = ProofGraph(
        nodes=(
            node("candidate", ProofNodeKind.CANDIDATE, ("pfr-page", "pfr-mirror", "nfl")),
            node("pfr-page", ProofNodeKind.LEAF, lineage_id="pfr", artifact_fingerprint="sha256:p1"),
            node("pfr-mirror", ProofNodeKind.LEAF, lineage_id="pfr", artifact_fingerprint="sha256:p2"),
            node("nfl", ProofNodeKind.LEAF, lineage_id="nfl", artifact_fingerprint="sha256:n1"),
        ),
        lineages={"pfr": pfr_lineage, "nfl": nfl_lineage},
    )

    groups = graph.independent_leaf_groups("candidate")

    assert len(groups) == 2
    assert ("pfr-mirror", "pfr-page") in groups.values()
    assert ("nfl",) in groups.values()
