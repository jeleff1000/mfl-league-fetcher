import json

import pytest

from scripts.build_player_seo_registry import (
    PlayerSource,
    RegistryValidationError,
    build_registry_entries,
    canonical_suffix,
    fetch_player_sources,
    is_fantasy_position,
    load_legacy_slugs,
    load_repo_environment,
    validate_entries,
    validate_previous_registry,
)


def slug_for(entries: list[dict[str, object]], nfl_player_id: str) -> str:
    return next(
        str(item["canonicalSlug"])
        for item in entries
        if item["nflPlayerId"] == nfl_player_id
    )


def entry(nfl_player_id: str, canonical_slug: str) -> dict[str, object]:
    return {
        "nflPlayerId": nfl_player_id,
        "name": "Player",
        "normalizedName": "player",
        "canonicalSlug": canonical_slug,
        "legacySlug": f"player-{nfl_player_id}",
        "indexable": True,
    }


def test_preserves_short_slug_owner_and_suffixes_new_collision():
    previous = [entry("qb", "lamar-jackson")]
    rows = [
        PlayerSource("qb", "Lamar Jackson", 100, 100, 2025, True),
        PlayerSource("db", "Lamar Jackson", 60, 80, 2024, True),
    ]

    entries = build_registry_entries(rows, previous)

    assert slug_for(entries, "qb") == "lamar-jackson"
    assert slug_for(entries, "db") == "lamar-jackson-db"


def test_reserves_short_slug_for_previous_owner_absent_from_current_source():
    previous = [entry("temporarily-absent", "sam-player")]
    rows = [PlayerSource("new", "Sam Player", 100, 100, 2025, True)]

    entries = build_registry_entries(rows, previous)

    assert slug_for(entries, "new") == "sam-player-new"


def test_ranks_new_short_slug_owner_by_fantasy_coverage():
    rows = [
        PlayerSource("lower", "Sam Player", 20, 100, 2025, True),
        PlayerSource("preferred", "Sam Player", 40, 80, 2024, True),
    ]

    entries = build_registry_entries(rows, [])

    assert slug_for(entries, "preferred") == "sam-player"
    assert slug_for(entries, "lower") == "sam-player-lower"


def test_rejects_duplicate_ids_or_slugs():
    with pytest.raises(RegistryValidationError, match="duplicate NFL player ID"):
        validate_entries([entry("same", "one"), entry("same", "two")])

    with pytest.raises(RegistryValidationError, match="duplicate canonical slug"):
        validate_entries([entry("one", "same"), entry("two", "same")])


def test_rejects_invalid_slugs_missing_reverse_mapping_and_index_limit():
    with pytest.raises(RegistryValidationError, match="invalid canonical slug"):
        validate_entries([entry("one", "Not Valid")])

    missing_legacy = entry("one", "one")
    missing_legacy["legacySlug"] = ""
    with pytest.raises(RegistryValidationError, match="missing legacy slug"):
        validate_entries([missing_legacy])

    too_many = [entry(str(index), f"player-{index}") for index in range(50_001)]
    with pytest.raises(RegistryValidationError, match="50,000"):
        validate_entries(too_many)


def test_validation_retains_exact_legacy_compatibility_slug_characters():
    legacy = entry("HoluE.00", "e-j-holub-holue-00")
    legacy["legacySlug"] = "e-j-holub-HoluE.00"

    validate_entries([legacy])


def test_rejects_cross_owner_canonical_and_legacy_slug_collision():
    player_a = entry("player-a", "alpha")
    player_b = entry("player-b", "beta")
    player_b["legacySlug"] = "alpha"

    with pytest.raises(RegistryValidationError, match="slug ownership collision"):
        validate_entries([player_a, player_b])


def test_allows_same_owner_canonical_and_legacy_slug_equality():
    player = entry("player-a", "alpha")
    player["legacySlug"] = "alpha"

    validate_entries([player])


@pytest.mark.parametrize(
    ("previous", "message"),
    [
        (["not-an-entry"], "entry must be an object"),
        ([{"nflPlayerId": "id", "canonicalSlug": "player"}], "missing fields"),
        ([entry("same", "one"), entry("same", "two")], "duplicate NFL player ID"),
        ([entry("one", "same"), entry("two", "same")], "duplicate canonical slug"),
        (
            [
                {**entry("one", "one"), "legacySlug": "same-legacy"},
                {**entry("two", "two"), "legacySlug": "same-legacy"},
            ],
            "duplicate legacy slug",
        ),
        ([entry("one", "Not Valid")], "invalid canonical slug"),
        (
            [{**entry("one", "one"), "normalizedName": "not player"}],
            "inconsistent normalized name",
        ),
        ({"version": 1}, "entries field"),
        ({"version": 2, "entries": []}, "version"),
        ({"version": True, "entries": []}, "version"),
        ([{**entry("one", "one"), "nflPlayerId": 1}], "malformed fields"),
        ([{**entry("one", "one"), "indexable": 1}], "must be boolean"),
    ],
)
def test_previous_registry_validation_fails_closed(previous, message):
    with pytest.raises(RegistryValidationError, match=message):
        validate_previous_registry(previous)


def test_load_legacy_slugs_keeps_deterministic_exact_compatibility_slug(tmp_path):
    source = tmp_path / "player-slugs.json"
    source.write_text(
        json.dumps(
            {
                "pat-johnson-00-0008598": {
                    "id": "00-0008598",
                    "name": "Pat Johnson",
                },
                "patrick-johnson-00-0008598": {
                    "id": "00-0008598",
                    "name": "Patrick Johnson",
                },
            }
        ),
        encoding="utf-8",
    )

    assert load_legacy_slugs(source, {"00-0008598": "Patrick Johnson"}) == {
        "00-0008598": "patrick-johnson-00-0008598"
    }


def test_canonical_suffix_normalizes_unsafe_player_ids():
    assert canonical_suffix("  id/value_1  ") == "id-value-1"


@pytest.mark.parametrize(
    "position",
    [
        "QB",
        "RB,DB",
        "WR,K",
        "DL/LB",
        "MLB",
        "ED",
        "EDGE",
        "SAF",
        "LB/EDGE",
        "ED,SAF",
    ],
)
def test_fantasy_position_families_accept_supported_composite_slots(position):
    assert is_fantasy_position(position)


def test_fantasy_position_families_exclude_offensive_line():
    assert not is_fantasy_position("OL")


def test_mlb_only_player_is_indexable():
    rows = [
        PlayerSource(
            "mlb-only",
            "Middle Linebacker",
            1,
            1,
            2025,
            is_fantasy_position("MLB"),
        )
    ]

    assert build_registry_entries(rows, [])[0]["indexable"] is True


def test_load_repo_environment_makes_fly_credentials_available(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text("DATABASE_SERVER_URL=https://fly.example\n", encoding="utf-8")
    monkeypatch.delenv("DATABASE_SERVER_URL", raising=False)

    load_repo_environment(env_path)

    assert __import__("os").environ["DATABASE_SERVER_URL"] == "https://fly.example"


def test_fetch_player_sources_uses_fly_list_rows_and_explicit_ops_database():
    class Reader:
        def __init__(self):
            self.call = None

        def query(self, sql, *, database):
            self.call = (sql, database)
            return [
                {
                    "nfl_player_id": "qb",
                    "name": "Q. Back",
                    "fantasy_games": 1,
                    "total_games": 1,
                    "latest_year": 2020,
                    "fantasy_relevant": True,
                },
                {
                    "nfl_player_id": "qb",
                    "name": "Quarter Back",
                    "fantasy_games": 3,
                    "total_games": 4,
                    "latest_year": 2025,
                    "fantasy_relevant": True,
                }
            ]

    reader = Reader()

    assert fetch_player_sources(reader) == [
        PlayerSource("qb", "Quarter Back", 3, 4, 2025, True)
    ]
    assert reader.call is not None
    assert reader.call[1] == "___ops"
    assert "SELECT *" not in reader.call[0].upper()
    assert "ROW_NUMBER()" in reader.call[0].upper()
