from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from .models import Grain, ProofNode, ProofNodeKind
from .semantic_types import SemanticType, aggregate_type, binary_result_type


def _quote_identifier(identifier: str) -> str:
    if "\x00" in identifier:
        raise ValueError("identifier contains NUL")
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _expression_id(expression: dict[str, Any]) -> str:
    canonical = json.dumps(expression, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]


@dataclass(frozen=True)
class CompiledExpression:
    sql: str
    parameters: tuple[Any, ...]
    semantic_type: SemanticType
    atom_dependencies: tuple[str, ...]
    proof_node: ProofNode


@dataclass(frozen=True)
class _Fragment:
    sql: str
    parameters: tuple[Any, ...]
    semantic_type: SemanticType
    dependencies: frozenset[str]


class TypedExpressionCompiler:
    def __init__(self, *, atom_types: dict[str, SemanticType], column_bindings: dict[str, str]) -> None:
        self._atom_types = dict(atom_types)
        self._column_bindings = dict(column_bindings)
        missing_bindings = set(self._atom_types) - set(self._column_bindings)
        if missing_bindings:
            raise ValueError(f"atoms missing column bindings: {sorted(missing_bindings)}")

    def compile(self, expression: dict[str, Any], *, law_id: str | None = None) -> CompiledExpression:
        fragment = self._compile_fragment(expression)
        expression_id = _expression_id(expression)
        proof_kind = ProofNodeKind.CONSTRAINT if law_id is not None else ProofNodeKind.TRANSFORM
        proof_atom_id = f"law:{law_id}" if law_id is not None else f"expression:{expression_id}"
        return CompiledExpression(
            sql=fragment.sql,
            parameters=fragment.parameters,
            semantic_type=fragment.semantic_type,
            atom_dependencies=tuple(sorted(fragment.dependencies)),
            proof_node=ProofNode(
                contract_version="1",
                node_id=f"{proof_kind.value}:{law_id or expression_id}",
                kind=proof_kind,
                atom_id=proof_atom_id,
                dependencies=tuple(f"atom:{item}" for item in sorted(fragment.dependencies)),
            ),
        )

    def _compile_fragment(self, expression: dict[str, Any]) -> _Fragment:
        if not isinstance(expression, dict):
            raise ValueError("expression node must be an object")
        operator = expression.get("op")
        if operator == "atom":
            allowed = {"op", "atom_id"}
            self._reject_unknown_fields(expression, allowed)
            atom_id = expression.get("atom_id")
            if atom_id not in self._atom_types:
                raise ValueError(f"unknown atom: {atom_id}")
            return _Fragment(
                sql=_quote_identifier(self._column_bindings[atom_id]),
                parameters=(),
                semantic_type=self._atom_types[atom_id],
                dependencies=frozenset({atom_id}),
            )
        if operator in {"add", "subtract", "lte", "gte", "eq"}:
            self._reject_unknown_fields(expression, {"op", "left", "right"})
            left = self._compile_fragment(expression["left"])
            right = self._compile_fragment(expression["right"])
            result_type = binary_result_type(operator, left.semantic_type, right.semantic_type)
            sql_operator = {"add": "+", "subtract": "-", "lte": "<=", "gte": ">=", "eq": "="}[operator]
            return _Fragment(
                sql=f"({left.sql} {sql_operator} {right.sql})",
                parameters=left.parameters + right.parameters,
                semantic_type=result_type,
                dependencies=left.dependencies | right.dependencies,
            )
        if operator == "divide":
            raise ValueError("unsafe division is forbidden; use safe_divide with an explicit zero_policy")
        if operator == "safe_divide":
            self._reject_unknown_fields(expression, {"op", "left", "right", "zero_policy"})
            if expression.get("zero_policy") != "null":
                raise ValueError("safe_divide currently requires zero_policy='null'")
            left = self._compile_fragment(expression["left"])
            right = self._compile_fragment(expression["right"])
            result_type = binary_result_type("divide", left.semantic_type, right.semantic_type)
            return _Fragment(
                sql=f"({left.sql} / NULLIF({right.sql}, 0))",
                parameters=left.parameters + right.parameters,
                semantic_type=result_type,
                dependencies=left.dependencies | right.dependencies,
            )
        if operator == "aggregate":
            self._reject_unknown_fields(
                expression,
                {"op", "arg", "target_grain", "target_partitions"},
            )
            argument = self._compile_fragment(expression["arg"])
            result_type = aggregate_type(
                argument.semantic_type,
                target_grain=Grain(expression["target_grain"]),
                target_partitions=tuple(expression["target_partitions"]),
            )
            return _Fragment(
                sql=f"SUM({argument.sql})",
                parameters=argument.parameters,
                semantic_type=result_type,
                dependencies=argument.dependencies,
            )
        raise ValueError(f"unsupported expression operator: {operator}")

    @staticmethod
    def _reject_unknown_fields(expression: dict[str, Any], allowed: set[str]) -> None:
        unknown = set(expression) - allowed
        if unknown:
            raise ValueError(f"unknown expression fields: {sorted(unknown)}")


def _atom_references(expression: dict[str, Any]) -> set[str]:
    if not isinstance(expression, dict):
        return set()
    if expression.get("op") == "atom":
        return {str(expression.get("atom_id"))}
    references: set[str] = set()
    for value in expression.values():
        if isinstance(value, dict):
            references.update(_atom_references(value))
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    references.update(_atom_references(item))
    return references


def validate_derived_acyclic(contracts: dict[str, dict[str, Any]]) -> None:
    dependencies = {
        atom_id: _atom_references(expression) & contracts.keys()
        for atom_id, expression in contracts.items()
    }
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(atom_id: str) -> None:
        status = state.get(atom_id, 0)
        if status == 2:
            return
        if status == 1:
            start = stack.index(atom_id)
            cycle = stack[start:] + [atom_id]
            raise ValueError(f"derived atom cycle: {' -> '.join(cycle)}")
        state[atom_id] = 1
        stack.append(atom_id)
        for dependency in sorted(dependencies[atom_id]):
            visit(dependency)
        stack.pop()
        state[atom_id] = 2

    for atom_id in sorted(contracts):
        visit(atom_id)
