"""Stable result records for source-to-source and identity reconciliation."""
from __future__ import annotations

from collections import Counter
import json
import math
from typing import Any


def _numeric(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _residual(expected: Any, observed: Any) -> tuple[float | None, float | None, float | None]:
    left, right = _numeric(expected), _numeric(observed)
    if left is None or right is None:
        return (None, None, None)
    absolute = right - left
    percent = absolute / abs(left) if left else None
    return (absolute, abs(absolute), percent)


def _status(expected: Any, observed: Any, tolerance_abs: float, tolerance_pct: float) -> str:
    if expected is None or observed is None:
        return "UNRESOLVED" if expected != observed else "PASS"
    if expected == observed:
        return "PASS"
    absolute, absolute_residual, percent = _residual(expected, observed)
    if absolute_residual is not None:
        if absolute_residual <= tolerance_abs or (
            percent is not None and abs(percent) <= tolerance_pct
        ):
            return "PASS"
    return "RECONSTRUCTION_ERROR"


def _base_result(rule_id: str, scope_key: str, rule_class: str) -> dict:
    return {
        "recon_rule_id": rule_id,
        "recon_rule_version": "1",
        "recon_scope": "explicit",
        "recon_scope_key": scope_key,
        "recon_rule_class": rule_class,
        "recon_filter_predicate": None,
        "recon_expected_value": None,
        "recon_observed_value": None,
        "recon_residual": None,
        "recon_abs_residual": None,
        "recon_pct_residual": None,
        "recon_tolerance_abs": 0.0,
        "recon_tolerance_pct": 0.0,
        "recon_status": "UNRESOLVED",
        "recon_exception_bucket": None,
        "recon_confidence": None,
        "recon_source_count": 0,
        "recon_source_values_json": "{}",
        "recon_majority_value": None,
        "recon_source_spread": None,
        "recon_repair_candidate": False,
        "recon_repair_value": None,
        "recon_repair_source": None,
        "recon_repair_confidence": None,
        "recon_first_bad_year": None,
        "recon_last_bad_year": None,
        "recon_bad_game_count": None,
        "recon_bad_row_count": None,
    }


def compare_observation(
    rule_id: str,
    scope_key: str,
    expected_value: Any,
    observed_value: Any,
    *,
    source_values: dict[str, Any] | None = None,
    recon_rule_class: str = "EXACT_IDENTITY",
    tolerance_abs: float = 0.0,
    tolerance_pct: float = 0.0,
) -> dict:
    result = _base_result(rule_id, scope_key, recon_rule_class)
    residual, absolute, percent = _residual(expected_value, observed_value)
    result.update({
        "recon_expected_value": expected_value,
        "recon_observed_value": observed_value,
        "recon_residual": residual,
        "recon_abs_residual": absolute,
        "recon_pct_residual": percent,
        "recon_tolerance_abs": tolerance_abs,
        "recon_tolerance_pct": tolerance_pct,
        "recon_status": _status(expected_value, observed_value, tolerance_abs, tolerance_pct),
    })
    if expected_value is None or observed_value is None:
        result["recon_exception_bucket"] = "MISSING_WITNESS"
    if source_values:
        result["recon_source_count"] = len(source_values)
        result["recon_source_values_json"] = json.dumps(source_values, sort_keys=True, default=str)
    return result


def reconcile_source_values(
    rule_id: str,
    scope_key: str,
    source_values: dict[str, Any],
    *,
    recon_rule_class: str = "CROSS_SOURCE_EQUALITY",
    tolerance_abs: float = 0.0,
    tolerance_pct: float = 0.0,
) -> dict:
    result = _base_result(rule_id, scope_key, recon_rule_class)
    values = list(source_values.values())
    numeric = [value for value in (_numeric(value) for value in values) if value is not None]
    counts = Counter(values)
    majority_value, majority_count = counts.most_common(1)[0] if counts else (None, 0)
    spread = max(numeric) - min(numeric) if numeric else None
    result.update({
        "recon_source_count": len(source_values),
        "recon_source_values_json": json.dumps(source_values, sort_keys=True, default=str),
        "recon_majority_value": majority_value if majority_count > 1 else None,
        "recon_source_spread": spread,
        "recon_confidence": "high" if len(counts) == 1 and values else None,
    })
    if not values or any(value is None for value in values):
        result["recon_status"] = "UNRESOLVED"
        result["recon_exception_bucket"] = "MISSING_WITNESS"
        return result
    if spread is not None:
        if spread <= tolerance_abs or (
            min(abs(_numeric(value) or 0) for value in values)
            and spread / max(abs(v) for v in numeric) <= tolerance_pct
        ):
            result["recon_status"] = "PASS"
            return result
    result["recon_status"] = "PASS" if len(counts) == 1 else "SOURCE_DISAGREEMENT"
    if result["recon_status"] == "SOURCE_DISAGREEMENT":
        result["recon_exception_bucket"] = "SOURCE_VALUES_DISAGREE"
    return result
