from scripts.sota_recon.nflcom_player_logs_neighbor_identity_diagnostic import (
    HISTORICAL_SLUG_ALIASES,
    HISTORICAL_SLUG_PAGE_POSITIONS,
    HISTORICAL_SLUG_IDENTITY_OVERRIDES,
    HISTORICAL_SLUG_PAGE_IDENTITY_CANDIDATES,
    _no_bridge_identity_query,
    _identity_query,
    _neighbor_query,
    _position_calibration_query,
    _position_neighbor_query,
    _position_query,
    _player_query,
    _shared_resolution_query,
)


def test_neighbor_rule_is_adjacent_and_identity_does_not_use_subject_values():
    neighbor = _neighbor_query()
    identity = _identity_query()
    assert "ABS(n.week_num-s.week_num)=1" in neighbor
    assert "COUNT(DISTINCT n.direct_layout)" in neighbor
    assert "NFL_player_id" in identity
    assert "nfl_player_stats_all" not in identity
    assert "v26" not in identity


def test_position_queries_use_unique_identity_and_calibrate_known_layouts():
    position = _position_query()
    calibration = _position_calibration_query()
    assert "COUNT(DISTINCT b.nfl_position)" in position
    assert "UNIQUE_ID_CLEAN_LAYOUT_POSITION" in position
    assert "direct_layout IN ('RBFB5','WRTE')" in calibration
    assert "agree_rows" in calibration
    assert "COUNT(DISTINCT NFL_player_id)" in _player_query()
    assert "POSITION_NEIGHBOR_CONFLICT" in _position_neighbor_query()
    resolution = _shared_resolution_query()
    assert "SEASON_DIRECT_SIGNATURE" in resolution
    assert "POSITION_SELECTS_MIRROR_LAYOUT" in resolution
    assert "PFR_INDEX_POSITION" in resolution
    assert "PFR_ALIAS_POSITION" in resolution
    assert "regexp_matches(index_position" in resolution
    assert "PENDING_NO_UNIQUE_LAYOUT_EVIDENCE" in resolution
    assert len(HISTORICAL_SLUG_ALIASES) == 69
    assert len({slug for slug, _ in HISTORICAL_SLUG_ALIASES}) == len(HISTORICAL_SLUG_ALIASES)
    assert len(HISTORICAL_SLUG_PAGE_POSITIONS) == 74
    assert len({slug for slug, _ in HISTORICAL_SLUG_PAGE_POSITIONS}) == len(HISTORICAL_SLUG_PAGE_POSITIONS)
    assert len(HISTORICAL_SLUG_IDENTITY_OVERRIDES) == 26
    assert len({slug for slug, _ in HISTORICAL_SLUG_IDENTITY_OVERRIDES}) == len(HISTORICAL_SLUG_IDENTITY_OVERRIDES)
    assert len(HISTORICAL_SLUG_PAGE_IDENTITY_CANDIDATES) == 40
    assert len({slug for slug, _ in HISTORICAL_SLUG_PAGE_IDENTITY_CANDIDATES}) == len(HISTORICAL_SLUG_PAGE_IDENTITY_CANDIDATES)


def test_no_bridge_identity_queue_is_source_only_and_season_aware():
    query = _no_bridge_identity_query()
    assert "nfl_player_stats_all" not in query
    assert "player_nfl_season" not in query
    assert "year_compatible" in query
    assert "HISTORICAL_ALIAS_BIO" in query
    assert "PFR_PLAYER_TABLE_EXACT_NAME" in query
    assert "compatible_identity_keys" in query
    assert "compatible_pfr_ids" in query
    assert "observed_rbfb5_rows" in query
    assert "observed_wrte_rows" in query
    assert "uniquely_observed_layout_identity_keys" in query
    assert "HISTORICAL_ALIAS_PFR_ID" in query
    assert "GayxWi20" in query
    assert "AlfoDe00" in query
    assert "unique_identity_key" in query
    assert "compatible_position_values" in query
    assert "observed_layout_identity_key" in query
    assert "unique_neighbor_rbfb5_rows" in query
    assert "neighbor_layout_identity_key" in query
    assert "historical_page_positions" in query
    assert "page_position_identity_key" in query
    assert "historical_identity_overrides" in query
    assert "all_compatible_identity_keys" in query
    assert "PAGE_POSITION_ONLY" in query
    assert "identity_position_families" in query
    assert "source_table_position_families" in query
    assert "source_table_clean_position_family" in query
