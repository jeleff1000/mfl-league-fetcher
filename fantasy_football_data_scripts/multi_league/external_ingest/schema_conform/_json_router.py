"""JSON shape detection + transformer dispatch for league_settings.

Bypasses cosine entirely. Yahoo settings JSONs go directly to the existing
yahoo settings transformer.
"""

from __future__ import annotations


class UnknownJSONShape(Exception):
    pass


def _yahoo_settings_transformer_lazy(payload, ctx):
    """Lazy import — avoids circular import at module load time."""
    from multi_league.data_fetchers.yahoo.settings_transformer import transform

    return transform(payload, ctx)


JSON_ROUTER_REGISTRY = {
    "yahoo_settings_v1": {
        "required_keys": {
            "year",
            "league_key",
            "metadata",
            "roster_positions",
            "scoring_rules",
            "dst_scoring",
            "stat_categories",
            "stat_modifiers",
        },
        "transformer_fn": _yahoo_settings_transformer_lazy,
    },
}


def detect_router_version(payload: dict) -> str | None:
    """Returns first matching router version key, or None."""
    if not isinstance(payload, dict):
        return None
    keys = set(payload.keys())
    for version, spec in JSON_ROUTER_REGISTRY.items():
        if spec["required_keys"].issubset(keys):
            return version
    return None


def route_json(payload: dict, ctx) -> dict:
    """Dispatch to the matching transformer. Raises UnknownJSONShape if no match."""
    version = detect_router_version(payload)
    if version is None:
        raise UnknownJSONShape(f"no registered JSON router matches payload keys {sorted(payload.keys())}")
    spec = JSON_ROUTER_REGISTRY[version]
    return spec["transformer_fn"](payload, ctx)
