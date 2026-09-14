"""Rank-only primitives for Research Mode top-10 stability studies.

Metric values are used to construct a displayed board and are deliberately
excluded from the stability statistic.  The statistic compares player order
and membership only.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
import json
import math
from pathlib import Path
from typing import Any, Iterable, Literal, Mapping, Sequence

from scipy.stats import spearmanr


SortDirection = Literal["asc", "desc"]


@dataclass(frozen=True)
class BoardSpec:
    """One production-facing player board whose ordered top ten is tested."""

    dataset: str
    grain: str
    metric: str
    direction: SortDirection
    support_metric: str
    position_scope: tuple[str, ...]
    common_pool: str
    source_metric: str
    ui_sortable: bool

    @property
    def identity(self) -> tuple[str, str, str]:
        return self.dataset, self.grain, self.metric

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, raw: Mapping[str, Any]) -> "BoardSpec":
        required = {
            "dataset",
            "grain",
            "metric",
            "direction",
            "support_metric",
            "position_scope",
            "common_pool",
            "source_metric",
            "ui_sortable",
        }
        missing = sorted(required - raw.keys())
        if missing:
            raise ValueError(f"board spec missing fields: {', '.join(missing)}")
        direction = str(raw["direction"])
        if direction not in {"asc", "desc"}:
            raise ValueError(f"invalid board direction: {direction!r}")
        positions = tuple(str(value) for value in raw["position_scope"])
        if not positions:
            raise ValueError("board spec position_scope cannot be empty")
        return cls(
            dataset=str(raw["dataset"]),
            grain=str(raw["grain"]),
            metric=str(raw["metric"]),
            direction=direction,
            support_metric=str(raw["support_metric"]),
            position_scope=positions,
            common_pool=str(raw["common_pool"]),
            source_metric=str(raw["source_metric"]),
            ui_sortable=bool(raw["ui_sortable"]),
        )


def load_board_specs(path: Path | str) -> tuple[BoardSpec, ...]:
    """Load and validate a versioned production-board contract."""

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if payload.get("version") != 1:
        raise ValueError(f"unsupported metric-contract version: {payload.get('version')!r}")
    rows = list(payload.get("boards", ()))
    for group in payload.get("groups", ()):
        if not isinstance(group, Mapping):
            raise ValueError("contract group must be an object")
        metrics = group.get("metrics", ())
        defaults = {key: value for key, value in group.items() if key != "metrics"}
        for metric in metrics:
            if not isinstance(metric, Mapping):
                raise ValueError("contract metric must be an object")
            rows.append({**defaults, **metric})
    specs = tuple(BoardSpec.from_dict(raw) for raw in rows)
    identities = [spec.identity for spec in specs]
    if len(identities) != len(set(identities)):
        raise ValueError("duplicate board spec identity")
    return specs


def _finite_number(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def ordered_top10(
    rows: Iterable[Mapping[str, Any]],
    spec: BoardSpec,
    *,
    limit: int = 10,
) -> tuple[str, ...]:
    """Apply the deterministic production ordering contract to player rows."""

    candidates: list[tuple[float, float, str]] = []
    seen: set[str] = set()
    for row in rows:
        player_id = str(row.get("NFL_player_id") or "").strip()
        metric = _finite_number(row.get(spec.metric))
        if not player_id or metric is None:
            continue
        if player_id in seen:
            raise ValueError(f"duplicate player row for {player_id!r}")
        seen.add(player_id)
        support = _finite_number(row.get(spec.support_metric))
        candidates.append((metric, support if support is not None else -math.inf, player_id))

    if spec.direction == "desc":
        candidates.sort(key=lambda item: (-item[0], -item[1], item[2]))
    else:
        candidates.sort(key=lambda item: (item[0], -item[1], item[2]))
    return tuple(player_id for _, _, player_id in candidates[:limit])


def _validate_board(board: Sequence[str], side: str) -> None:
    if any(not str(player).strip() for player in board):
        raise ValueError(f"{side} board contains an empty player ID")
    if len(board) != len(set(board)):
        raise ValueError(f"{side} board contains duplicate player IDs")


def top10_rank_rho(
    left: Sequence[str],
    right: Sequence[str],
    absent_rank: int = 11,
) -> float:
    """Spearman rho over the union of two ordered boards.

    Players missing from one board receive the same absent rank.  That tied
    rank makes membership changes part of the ordering statistic without
    ever correlating the underlying metric values.
    """

    _validate_board(left, "left")
    _validate_board(right, "right")
    if absent_rank <= max(len(left), len(right), 0):
        raise ValueError("absent_rank must be greater than every displayed rank")
    players = sorted(set(left) | set(right))
    if not players:
        raise ValueError("cannot correlate two empty boards")
    if left == right:
        return 1.0
    left_rank = {player: index + 1 for index, player in enumerate(left)}
    right_rank = {player: index + 1 for index, player in enumerate(right)}
    result = spearmanr(
        [left_rank.get(player, absent_rank) for player in players],
        [right_rank.get(player, absent_rank) for player in players],
    ).statistic
    return float(result)
