from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from numbers import Integral
from typing import Any

import pandas as pd

from .gate_planes import Finding
from .position_taxonomy import BROAD_POSITIONS, UnknownPositionToken, normalize_position


_LINEAGE_COLUMNS = (
    "source",
    "dataset",
    "artifact_run_id",
    "manifest_pin",
    "lineage_id",
    "shard_id",
    "manifest_sha256",
    "season",
    "year",
    "game_id",
    "source_player_id",
    "NFL_player_id",
    "player_id",
    "row_id",
    "lineage_leaves",
)

_SHA256 = re.compile(r"^[0-9a-fA-F]{64}$")


def _json_value(value: object) -> Any:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return {
            str(key): _json_value(value[key])
            for key in sorted(value, key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    to_list = getattr(value, "tolist", None)
    if not isinstance(value, (str, bytes)) and callable(to_list):
        converted = to_list()
        if converted is not value:
            return _json_value(converted)
    missing = pd.isna(value)
    if isinstance(missing, bool) and missing:
        return None
    if hasattr(value, "item"):
        value = value.item()
    return value if isinstance(value, (str, int, float, bool)) else str(value)


def _normalize_lineage_leaves(value: object) -> list[dict[str, object]]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple)):
        if isinstance(value, (str, bytes, Mapping)) or pd.api.types.is_scalar(value):
            if isinstance(pd.isna(value), bool) and pd.isna(value):
                return []
            raise ValueError("lineage_leaves must be a typed list of leaf records")
        to_list = getattr(value, "tolist", None)
        if not callable(to_list):
            raise ValueError("lineage_leaves must be a typed list of leaf records")
        value = to_list()
    if not isinstance(value, (list, tuple)):
        raise ValueError("lineage_leaves array conversion must produce a list")

    leaves: dict[tuple[int, str], dict[str, object]] = {}
    hashes_by_shard: dict[int, str] = {}
    for raw_leaf in value:
        if not isinstance(raw_leaf, Mapping):
            raise ValueError("lineage_leaves must contain typed leaf records")
        if "shard_id" not in raw_leaf or "manifest_sha256" not in raw_leaf:
            raise ValueError("lineage_leaves record requires shard_id and manifest_sha256")
        raw_shard_id = raw_leaf["shard_id"]
        if isinstance(raw_shard_id, bool) or not isinstance(raw_shard_id, Integral):
            raise ValueError("lineage_leaves shard_id must be a nonnegative integer scalar")
        if raw_shard_id < 0:
            raise ValueError("lineage_leaves shard_id must be a nonnegative integer scalar")
        shard_id = int(raw_shard_id)
        manifest_sha256 = str(raw_leaf["manifest_sha256"]).strip().lower()
        if not _SHA256.fullmatch(manifest_sha256):
            raise ValueError("lineage_leaves record has invalid manifest_sha256")
        previous = hashes_by_shard.get(shard_id)
        if previous is not None and previous != manifest_sha256:
            raise ValueError(f"lineage_leaves has conflicting hashes for shard_id={shard_id}")
        hashes_by_shard[shard_id] = manifest_sha256
        leaves[(shard_id, manifest_sha256)] = {
            "shard_id": shard_id,
            "manifest_sha256": manifest_sha256,
        }
    return [leaves[key] for key in sorted(leaves)]


def _clean(value: object) -> str | None:
    normalized = _json_value(value)
    if normalized is None:
        return None
    text = str(normalized).strip()
    return text or None


def _row_ref(row: dict[str, Any]) -> str:
    body = json.dumps(row, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    return f"row:sha256:{hashlib.sha256(body.encode('utf-8')).hexdigest()}"


def _finding(
    *,
    code: str,
    plane: str,
    row: dict[str, Any],
    lineage: dict[str, Any],
    position_col: str,
    nfl_position_col: str,
    columns: tuple[str, ...],
    remediation: str,
) -> Finding:
    scope = {
        "gate_plane": plane,
        "columns": list(columns),
        "position_col": position_col,
        "nfl_position_col": nfl_position_col,
        "position": row[position_col],
        "nfl_position": row[nfl_position_col],
        "lineage": lineage,
    }
    leaf_refs = tuple(
        str(leaf["manifest_sha256"])
        for leaf in lineage.get("lineage_leaves", [])
    )
    return Finding(
        code=code,
        severity="fail",
        scope=scope,
        evidence_refs=(_row_ref(row), *leaf_refs),
        remediation=remediation,
    )


def validate_position_frame(
    frame: pd.DataFrame,
    *,
    position_col: str,
    nfl_position_col: str,
) -> tuple[Finding, ...]:
    """Validate stored position pairs without altering the supplied frame."""
    if not isinstance(frame, pd.DataFrame):
        raise TypeError("position contract input must be a pandas DataFrame")
    if not position_col or not nfl_position_col or position_col == nfl_position_col:
        raise ValueError("position_col and nfl_position_col must be distinct nonempty names")
    duplicate_columns = sorted({str(column) for column in frame.columns[frame.columns.duplicated()]})
    if duplicate_columns:
        raise ValueError(f"duplicate DataFrame columns are forbidden: {duplicate_columns}")
    missing = sorted({position_col, nfl_position_col} - set(frame.columns))
    if missing:
        raise ValueError(f"missing position contract columns: {missing}")

    findings: list[Finding] = []
    lineage_columns = [column for column in _LINEAGE_COLUMNS if column in frame.columns]
    for raw in frame.to_dict("records"):
        if "lineage_leaves" in raw:
            raw["lineage_leaves"] = _normalize_lineage_leaves(raw["lineage_leaves"])
        row = {str(key): _json_value(value) for key, value in raw.items()}
        lineage = {
            column: row[column]
            for column in lineage_columns
            if row.get(column) is not None and _clean(row.get(column)) is not None
        }
        position = _clean(row[position_col])
        nfl_position = _clean(row[nfl_position_col])

        unknown_columns: list[str] = []
        normalized_position = None
        normalized_nfl_position = None
        for column, value in ((position_col, position), (nfl_position_col, nfl_position)):
            try:
                normalized = normalize_position(value)
            except UnknownPositionToken:
                unknown_columns.append(column)
                continue
            if column == position_col:
                normalized_position = normalized
            else:
                normalized_nfl_position = normalized
        if unknown_columns:
            findings.append(
                _finding(
                    code="UNKNOWN_POSITION_TOKEN",
                    plane="source_health",
                    row=row,
                    lineage=lineage,
                    position_col=position_col,
                    nfl_position_col=nfl_position_col,
                    columns=tuple(unknown_columns),
                    remediation="correct or register the source token; validation never normalizes it",
                )
            )
            continue

        if position == "LS":
            findings.append(
                _finding(
                    code="LONG_SNAPPER_IN_BROAD_POSITION",
                    plane="candidate_promotion",
                    row=row,
                    lineage=lineage,
                    position_col=position_col,
                    nfl_position_col=nfl_position_col,
                    columns=(position_col,),
                    remediation="store OL in position and LS in nfl_position",
                )
            )
            continue

        if position is not None and position.upper() not in BROAD_POSITIONS:
            findings.append(
                _finding(
                    code="DETAILED_POSITION_IN_BROAD_COLUMN",
                    plane="candidate_promotion",
                    row=row,
                    lineage=lineage,
                    position_col=position_col,
                    nfl_position_col=nfl_position_col,
                    columns=(position_col,),
                    remediation="store the canonical broad position in position",
                )
            )
            continue

        broad = normalized_position.position if normalized_position is not None else None
        detailed_broad = (
            normalized_nfl_position.position if normalized_nfl_position is not None else None
        )
        if broad != detailed_broad and (broad is not None or detailed_broad is not None):
            findings.append(
                _finding(
                    code="POSITION_PAIR_INCOMPATIBLE",
                    plane="candidate_promotion",
                    row=row,
                    lineage=lineage,
                    position_col=position_col,
                    nfl_position_col=nfl_position_col,
                    columns=(position_col, nfl_position_col),
                    remediation="make position agree with the broad class of nfl_position",
                )
            )

    return tuple(
        sorted(
            findings,
            key=lambda item: json.dumps(item.to_dict(), sort_keys=True, separators=(",", ":")),
        )
    )
