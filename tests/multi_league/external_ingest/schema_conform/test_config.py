from multi_league.external_ingest.schema_conform import _config


def test_required_identity_slots_per_table():
    assert _config.REQUIRED_IDENTITY_SLOTS["matchup"] == ["year", "week", "manager_guid"]
    assert "manager_guid" in _config.REQUIRED_IDENTITY_SLOTS["player_fantasy"]
    assert "transaction_id" in _config.REQUIRED_IDENTITY_SLOTS["transactions"]


def test_coverage_thresholds_transactions_looser():
    assert _config.COVERAGE_THRESHOLDS["transactions"] < _config.COVERAGE_THRESHOLDS["matchup"]
    assert _config.COVERAGE_THRESHOLDS["matchup"] == 0.98
    assert _config.COVERAGE_THRESHOLDS["transactions"] == 0.85


def test_min_ref_size_by_slot_manager_guid_thick_at_eight():
    assert _config.MIN_REF_SIZE_BY_SLOT["manager_guid"] == 8


def test_aliases_contain_expected_pairs():
    assert _config.ALIASES["mgr"] == "manager"
    assert _config.ALIASES["pts"] == "points"
    assert _config.ALIASES["txn"] == "transaction"
    # 'id' is intentionally NOT in ALIASES — handled context-sensitively in _normalize
    assert "id" not in _config.ALIASES


def test_identity_tokens_set():
    assert "manager" in _config.IDENTITY_TOKENS
    assert "player" in _config.IDENTITY_TOKENS
    assert "team" in _config.IDENTITY_TOKENS


def test_thresholds():
    assert _config.CONFIDENCE_FLOOR == 0.85
    assert _config.COLLISION_GAP == 0.05
    assert _config.FUZZY_CUTOFF == 90
    assert _config.FUZZY_GAP == 5
    assert _config.REQUIRED_DATA_NULL_FLOOR == 0.5
