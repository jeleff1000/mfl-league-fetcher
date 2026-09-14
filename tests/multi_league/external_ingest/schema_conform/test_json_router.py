import pytest
from multi_league.external_ingest.schema_conform._json_router import (
    detect_router_version,
    route_json,
    UnknownJSONShape,
)

YAHOO_SETTINGS = {
    "year": 2014,
    "league_key": "331.l.381581",
    "metadata": {},
    "roster_positions": [],
    "scoring_rules": [],
    "dst_scoring": {},
    "stat_categories": {},
    "stat_modifiers": {},
}


def test_detect_yahoo_settings_v1():
    assert detect_router_version(YAHOO_SETTINGS) == "yahoo_settings_v1"


def test_detect_unknown_shape_returns_none():
    assert detect_router_version({"foo": "bar"}) is None


def test_route_json_yahoo_settings_calls_transformer(monkeypatch):
    captured = {}

    def fake_transform(payload, ctx):
        captured["payload"] = payload
        return {"db_name": "test", "year": 2014, "scoring_rec": 0.5}

    import multi_league.external_ingest.schema_conform._json_router as mod

    monkeypatch.setitem(mod.JSON_ROUTER_REGISTRY["yahoo_settings_v1"], "transformer_fn", fake_transform)

    result = route_json(YAHOO_SETTINGS, ctx=None)
    assert result["db_name"] == "test"
    assert captured["payload"] == YAHOO_SETTINGS


def test_route_unknown_raises():
    with pytest.raises(UnknownJSONShape):
        route_json({"random": "json"}, ctx=None)
