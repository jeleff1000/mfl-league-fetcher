from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass

from .models import ProofNode, ProofNodeKind


def _fingerprint(payload: object) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


@dataclass(frozen=True)
class LineageIdentity:
    acquisition_family: str
    upstream_publisher: str
    transformation_chain: tuple[str, ...]
    raw_artifact_fingerprint: str

    @property
    def fingerprint(self) -> str:
        return _fingerprint(
            {
                "acquisition_family": self.acquisition_family,
                "upstream_publisher": self.upstream_publisher,
                "transformation_chain": self.transformation_chain,
                "raw_artifact_fingerprint": self.raw_artifact_fingerprint,
            }
        )


class ProofGraph:
    def __init__(
        self,
        *,
        nodes: Iterable[ProofNode],
        lineages: dict[str, LineageIdentity] | None = None,
    ) -> None:
        materialized = tuple(nodes)
        self._nodes = {item.node_id: item for item in materialized}
        if len(self._nodes) != len(materialized):
            raise ValueError("duplicate proof node_id")
        self._lineages = dict(lineages or {})

    def validate(self) -> None:
        for item in self._nodes.values():
            if item.kind is ProofNodeKind.LEAF:
                if item.dependencies:
                    raise ValueError(f"leaf node {item.node_id} cannot have dependencies")
                if not item.lineage_id or not item.artifact_fingerprint:
                    raise ValueError(f"leaf node {item.node_id} requires lineage and artifact fingerprints")
            for dependency in item.dependencies:
                if dependency not in self._nodes:
                    raise ValueError(f"unknown dependency {dependency!r} from {item.node_id!r}")
        self.topological_order()

    def topological_order(self) -> tuple[str, ...]:
        state: dict[str, int] = {}
        stack: list[str] = []
        ordered: list[str] = []

        def visit(node_id: str) -> None:
            status = state.get(node_id, 0)
            if status == 2:
                return
            if status == 1:
                start = stack.index(node_id)
                cycle = stack[start:] + [node_id]
                raise ValueError(f"proof cycle: {' -> '.join(cycle)}")
            state[node_id] = 1
            stack.append(node_id)
            for dependency in sorted(self._nodes[node_id].dependencies):
                if dependency not in self._nodes:
                    raise ValueError(f"unknown dependency {dependency!r} from {node_id!r}")
                visit(dependency)
            stack.pop()
            state[node_id] = 2
            ordered.append(node_id)

        for node_id in sorted(self._nodes):
            visit(node_id)
        return tuple(ordered)

    def leaf_nodes(self, node_id: str) -> tuple[str, ...]:
        if node_id not in self._nodes:
            raise ValueError(f"unknown proof node: {node_id}")
        self.validate()
        leaves: set[str] = set()

        def collect(current_id: str) -> None:
            current = self._nodes[current_id]
            if current.kind is ProofNodeKind.LEAF:
                leaves.add(current_id)
                return
            for dependency in current.dependencies:
                collect(dependency)

        collect(node_id)
        return tuple(sorted(leaves))

    def independent_leaf_groups(self, node_id: str) -> dict[str, tuple[str, ...]]:
        grouped: dict[str, list[str]] = {}
        for leaf_id in self.leaf_nodes(node_id):
            leaf = self._nodes[leaf_id]
            lineage = self._lineages.get(leaf.lineage_id or "")
            if lineage is None:
                raise ValueError(f"leaf {leaf_id} references unknown lineage {leaf.lineage_id!r}")
            grouped.setdefault(lineage.fingerprint, []).append(leaf_id)
        return {key: tuple(sorted(value)) for key, value in sorted(grouped.items())}

    def node_fingerprint(self, node_id: str) -> str:
        if node_id not in self._nodes:
            raise ValueError(f"unknown proof node: {node_id}")
        self.validate()
        memo: dict[str, str] = {}

        def resolve(current_id: str) -> str:
            if current_id not in memo:
                current = self._nodes[current_id]
                memo[current_id] = _fingerprint(
                    {
                        "node": current.to_dict(),
                        "dependency_fingerprints": [resolve(item) for item in sorted(current.dependencies)],
                    }
                )
            return memo[current_id]

        return resolve(node_id)
