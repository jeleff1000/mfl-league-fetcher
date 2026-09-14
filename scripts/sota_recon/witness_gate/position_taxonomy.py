from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any


POSITION_TAXONOMY_VERSION = "position-taxonomy.v1"
BROAD_POSITIONS = frozenset({"QB", "RB", "WR", "TE", "OL", "DL", "LB", "DB", "K", "P", "DEF"})

_COMPOUND_DELIMITER = re.compile(r"\s*[-/,]\s*")
_VALID_SOURCE_POSITION = re.compile(r"^[A-Z]+(?: [A-Z]+)*(?:\s*[-/,]\s*[A-Z]+(?: [A-Z]+)*)*$")
_CONTRACT_PATH = Path(__file__).with_name("contracts") / "position_taxonomy.v1.json"


class UnknownPositionToken(ValueError):
    pass


class ComboOrderViolation(ValueError):
    """Raised when the fantasy_position combo order is tampered with."""


# ---------------------------------------------------------------------------
# THE COMBO ORDER LAW (Joe 2026-08-04) -- LOCKED, MIRRORED, FINGERPRINTED.
#
# Multi-positional players list their positions in THIS rank in
# fantasy_position. The rank is absolute and is NOT primacy: Deion Sanders'
# WR-eligible season is WR,DB even though DB was his primary; Groza is K,OL;
# Blanda is QB,K; Taysom Hill is QB,TE and never TE,QB; Travis Hunter is
# WR,DB and never DB,WR.
#
# THIS IS NOT BROAD_POSITIONS. That is a different sequence (...OL,DL,LB,DB,
# K,P) and substituting it emits OL,K for Groza. Never sort a combo by it.
#
# IMMUTABILITY / QUORUM: the order lives in TWO places -- this tuple and the
# contract's combo_order block -- pinned by a fingerprint over both. No single
# edit site can change the law quietly: touch one and every program that
# imports this module dies at import with ComboOrderViolation. To change it
# lawfully, edit both sides, recompute the fingerprint, and say why in the
# contract's basis field.
# ---------------------------------------------------------------------------
COMBO_ORDER: tuple[str, ...] = ("QB", "RB", "WR", "TE", "K", "LB", "DL", "DB", "P", "OL")
COMBO_ORDER_SHA256 = "7f01ec3cb0e4fc3219627782af1f5289d99f4d69abcf8cf3028196ef11c360b9"
COMBO_SEPARATOR = ","


@dataclass(frozen=True)
class NormalizedPosition:
    position: str | None
    nfl_position: str | None
    source_position_raw: str | None
    taxonomy_version: str = POSITION_TAXONOMY_VERSION


def _load_contract() -> dict[str, str]:
    payload: dict[str, Any] = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
    if payload.get("schema_version") != "1":
        raise ValueError(f"unsupported position taxonomy schema: {payload.get('schema_version')!r}")
    if payload.get("taxonomy_version") != POSITION_TAXONOMY_VERSION:
        raise ValueError(f"position taxonomy version mismatch: {payload.get('taxonomy_version')!r}")
    if frozenset(payload.get("broad_positions", ())) != BROAD_POSITIONS:
        raise ValueError("position taxonomy broad-position vocabulary mismatch")
    mapping = payload.get("detailed_to_broad")
    if not isinstance(mapping, dict) or not mapping:
        raise ValueError("position taxonomy requires a nonempty detailed_to_broad map")
    invalid = {
        str(detailed): broad
        for detailed, broad in mapping.items()
        if not isinstance(detailed, str)
        or detailed != detailed.strip().upper()
        or broad not in BROAD_POSITIONS
    }
    if invalid:
        raise ValueError(f"invalid detailed_to_broad entries: {invalid!r}")
    return mapping


def _load_combo_order() -> tuple[str, ...]:
    """Verify the combo-order law on BOTH sides and return it.

    Fails loudly and at import: a silently-reordered combo would corrupt
    fantasy_position for every dual-eligible player in history, and would do
    it invisibly (both 'K,OL' and 'OL,K' look like plausible data).
    """
    payload: dict[str, Any] = json.loads(_CONTRACT_PATH.read_text(encoding="utf-8"))
    block = payload.get("combo_order")
    if not isinstance(block, dict):
        raise ComboOrderViolation(
            "position_taxonomy.v1.json has no locked combo_order block. The "
            "fantasy_position ordering law was DELETED. Restore it; do not "
            "improvise an order.")
    if not block.get("LOCKED"):
        raise ComboOrderViolation("combo_order block is no longer marked LOCKED")
    order = tuple(block.get("order", ()))
    declared = block.get("fingerprint_sha256")
    actual = hashlib.sha256(",".join(order).encode()).hexdigest()
    if actual != declared:
        raise ComboOrderViolation(
            f"combo_order FINGERPRINT MISMATCH: the contract's order "
            f"{list(order)} hashes to {actual} but the block declares "
            f"{declared}. Someone edited the order without re-pinning it.")
    if order != COMBO_ORDER:
        raise ComboOrderViolation(
            f"combo_order QUORUM BROKEN: the contract says {list(order)} but "
            f"the code mirror COMBO_ORDER says {list(COMBO_ORDER)}. These must "
            f"agree -- that disagreement is exactly what the two-site lock "
            f"exists to catch. Fix both sides together.")
    if actual != COMBO_ORDER_SHA256:
        raise ComboOrderViolation(
            f"combo_order fingerprint {actual} does not match the pinned "
            f"COMBO_ORDER_SHA256 {COMBO_ORDER_SHA256}")
    missing = set(order) - BROAD_POSITIONS
    if missing:
        raise ComboOrderViolation(f"combo_order contains non-broad tokens: {sorted(missing)}")
    return order


_DETAILED_TO_BROAD = _load_contract()
_DETAILED_ORDER = {token: index for index, token in enumerate(_DETAILED_TO_BROAD)}
_COMBO_ORDER = _load_combo_order()
_COMBO_RANK = {token: index for index, token in enumerate(_COMBO_ORDER)}


def combo_rank(broad: str) -> int:
    """Rank of a broad position in the locked combo order."""
    try:
        return _COMBO_RANK[broad]
    except KeyError:
        raise ComboOrderViolation(
            f"{broad!r} has no rank in the locked combo order {list(_COMBO_ORDER)}"
        ) from None


def order_combo(tokens) -> str:
    """Render broad positions as a fantasy_position combo in the locked order.

    The single canonical way to build a combo. Anything that assembles one
    by hand (string concat, 'base + K + P') is a bug waiting to emit OL,K.
    """
    uniq = {str(t).strip().upper() for t in tokens if str(t).strip()}
    unknown = uniq - BROAD_POSITIONS
    if unknown:
        raise ComboOrderViolation(
            f"combo tokens outside the broad vocabulary: {sorted(unknown)}")
    return COMBO_SEPARATOR.join(sorted(uniq, key=combo_rank))


def is_ordered_combo(value: str) -> bool:
    """True when an existing fantasy_position value obeys the locked order."""
    if not value:
        return False
    toks = [t.strip().upper() for t in str(value).split(COMBO_SEPARATOR) if t.strip()]
    if not toks or len(toks) != len(set(toks)):
        return False
    if set(toks) - BROAD_POSITIONS:
        return False
    return toks == sorted(toks, key=combo_rank)


def _unknown(raw: object, *, strict: bool) -> NormalizedPosition:
    if strict:
        raise UnknownPositionToken(f"unknown position token: {raw!r}")
    return NormalizedPosition(position=None, nfl_position=None, source_position_raw=str(raw))


def normalize_position(raw: object, *, strict: bool = True) -> NormalizedPosition:
    if raw is None:
        return NormalizedPosition(position=None, nfl_position=None, source_position_raw=None)

    source_position_raw = str(raw)
    normalized = " ".join(source_position_raw.upper().split())
    if not normalized:
        return NormalizedPosition(position=None, nfl_position=None, source_position_raw=None)
    if not _VALID_SOURCE_POSITION.fullmatch(normalized):
        return _unknown(raw, strict=strict)

    tokens = _COMPOUND_DELIMITER.split(normalized)
    unknown = [token for token in tokens if token not in _DETAILED_TO_BROAD]
    if unknown:
        return _unknown(raw, strict=strict)

    detailed_tokens = sorted(set(tokens), key=lambda token: (_DETAILED_ORDER[token], token))
    broad_tokens = {_DETAILED_TO_BROAD[token] for token in detailed_tokens}
    if len(broad_tokens) != 1:
        return _unknown(raw, strict=strict)
    return NormalizedPosition(
        position=next(iter(broad_tokens)),
        nfl_position="/".join(detailed_tokens),
        source_position_raw=source_position_raw,
    )


def positions_compatible(left: object, right: object) -> bool:
    left_value = normalize_position(left)
    right_value = normalize_position(right)
    if left_value.position is None or right_value.position is None:
        return False
    return bool(set(left_value.position.split(",")) & set(right_value.position.split(",")))


def validate_position_pair(position: object, nfl_position: object) -> None:
    broad_value = normalize_position(position)
    detailed_value = normalize_position(nfl_position)
    if broad_value.position is None and detailed_value.position is None:
        return
    if broad_value.nfl_position not in BROAD_POSITIONS:
        raise ValueError(f"position must be one broad position, got {position!r}")
    if detailed_value.position != broad_value.nfl_position:
        raise ValueError(
            f"incompatible position pair: position={position!r}, nfl_position={nfl_position!r}"
        )
