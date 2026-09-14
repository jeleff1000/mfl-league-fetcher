from multi_league.transformations.player.modules import silent_drop_logger as sdl


def setup_function():
    sdl.reset()


def test_warn_collects_unique_drops():
    sdl.warn("canonicalization", "fg_made_0_39", {"league": "tfl", "year": 2024})
    sdl.warn("canonicalization", "fg_made_0_39", {"league": "tfl", "year": 2024})
    sdl.warn("canonicalization", "kr_yds", {"league": "demo_league", "year": 2018})

    drops = sdl.get_drops()
    # Same key+layer is deduplicated
    assert len(drops) == 2


def test_warn_dedupes_by_layer_and_key():
    sdl.warn("canonicalization", "x", {})
    sdl.warn("path_b_mapping", "x", {})
    drops = sdl.get_drops()
    # Same key, different layer = different entries
    assert len(drops) == 2


def test_emit_summary_returns_string_and_clears():
    sdl.warn("canonicalization", "x", {"league": "tfl"})
    summary = sdl.emit_summary()
    assert "canonicalization" in summary
    assert "x" in summary
    assert "tfl" in summary
    # After emit, the collector is cleared
    assert sdl.get_drops() == []


def test_reset_clears_state():
    sdl.warn("canonicalization", "x", {})
    sdl.reset()
    assert sdl.get_drops() == []
