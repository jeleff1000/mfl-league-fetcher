"""Pure helpers and bounded row scanners for value-domain witness audits."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable
from typing import Any

_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
_NULL_STRINGS = {"", "null", "none", "nan", "na", "n/a"}


def parse_numeric(value: object) -> tuple[float | None, str]:
    if value is None:
        return None, "SOURCE_NULL"
    if isinstance(value, bool):
        return float(value), "OBSERVED_VALUE"
    if isinstance(value, (int, float)):
        number = float(value)
        if math.isnan(number):
            return None, "SOURCE_NULL"
        return number, "OBSERVED_ZERO" if number == 0 else "OBSERVED_VALUE"
    text = str(value).strip()
    if text.lower() in _NULL_STRINGS:
        return None, "SOURCE_NULL"
    if not _NUMERIC_RE.fullmatch(text):
        return None, "PARSER_FAILED"
    number = float(text)
    return number, "OBSERVED_ZERO" if number == 0 else "OBSERVED_VALUE"


def extract_json_paths(value: object) -> list[tuple[str, object]]:
    """Flatten JSON-like values into deterministic leaf paths."""

    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return [("$", value)]

    result: list[tuple[str, object]] = []

    def walk(current: object, path: str) -> None:
        if isinstance(current, dict):
            for key in sorted(current, key=str):
                walk(current[key], f"{path}.{key}")
        elif isinstance(current, list):
            for index, item in enumerate(current):
                walk(item, f"{path}[{index}]")
        else:
            result.append((path, current))

    walk(value, "$")
    return result


def stable_examples(values: Iterable[object], limit: int = 5) -> list[str]:
    return sorted({str(value) for value in values if value is not None})[:limit]


def observation_state(value: object) -> str:
    """Classify a raw value without coercing missing values to zero."""

    _, state = parse_numeric(value)
    return state
