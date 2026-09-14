"""Runtime collector for silent scoring-key drops at the canonicalization layer.

ONLY used for the canonicalization layer (keys in source dict that don't match
ALL_SCORING_KEYS). The other two drop layers — pre-flight key assertion and
column-existence assertion — are HARD failures via raise, not soft warnings.

Usage:
    from multi_league.transformations.player.modules import silent_drop_logger
    silent_drop_logger.warn("canonicalization", "fg_made_0_39", {"league": "tfl", "year": 2024})
    # ... at end of import ...
    print(silent_drop_logger.emit_summary())
"""

from __future__ import annotations

# Module-level state. Reset between imports.
_drops: dict[tuple[str, str], dict] = {}


def warn(layer: str, key: str, context: dict) -> None:
    """Record a silent drop. Deduplicated by (layer, key)."""
    dedup_key = (layer, key)
    if dedup_key not in _drops:
        _drops[dedup_key] = {"layer": layer, "key": key, "context": context}


def get_drops() -> list[dict]:
    """Return all collected drops as a list of dicts."""
    return list(_drops.values())


def emit_summary() -> str:
    """Return a formatted summary of all drops, then clear state."""
    if not _drops:
        return "[silent_drops] none"
    lines = [f"[silent_drops] {len(_drops)} unique drops detected:"]
    for d in _drops.values():
        ctx = " ".join(f"{k}={v}" for k, v in d["context"].items())
        lines.append(f"  - {d['layer']}: {ctx} key='{d['key']}'")
    summary = "\n".join(lines)
    reset()
    return summary


def reset() -> None:
    """Clear all collected drops. Called between imports and after emit_summary."""
    global _drops
    _drops = {}
