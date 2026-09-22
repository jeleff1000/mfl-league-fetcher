import pandas as pd

from scripts.refresh_yahoo_active_season import (
    derive_yahoo_active_franchise_merges,
    normalize_yahoo_roster_provider_identity,
    yahoo_active_identity_repair_count,
    yahoo_identity_repair_week,
)


def test_raw_yahoo_player_id_is_promoted_to_canonical_provider_identity() -> None:
    raw = pd.DataFrame(
        {
            "player_id": ["33477", "33501"],
            "player_name": ["Player One", "Player Two"],
            "year": [2026, 2026],
            "week": [1, 1],
        }
    )

    normalized = normalize_yahoo_roster_provider_identity(raw)

    assert normalized["yahoo_player_id"].tolist() == ["33477", "33501"]
    assert "yahoo_player_id" not in raw.columns


def test_existing_canonical_identity_is_preserved_and_only_blanks_are_filled() -> None:
    raw = pd.DataFrame(
        {
            "player_id": ["raw-1", "raw-2"],
            "yahoo_player_id": ["canonical-1", None],
        }
    )

    normalized = normalize_yahoo_roster_provider_identity(raw)

    assert normalized["yahoo_player_id"].tolist() == ["canonical-1", "raw-2"]


def test_active_synthetic_owner_reuses_unique_historical_franchise() -> None:
    active = pd.DataFrame(
        {
            "manager": ["Abelv", "Alexander"],
            "manager_guid": ["yh-nick-abelv", "yh-nick-alexander"],
            "franchise_id": ["yh-nick-abelv", "yh-nick-alexander"],
            "year": [2026, 2026],
        }
    )
    historical = pd.DataFrame(
        {
            "manager": ["Abel Velasco", "Alex Taylor"],
            "franchise_id": ["GUID-ABEL", "GUID-ALEX"],
            "last_year": [2025, 2025],
        }
    )

    merges = derive_yahoo_active_franchise_merges(
        active,
        historical,
        manager_name_overrides={"Abelv": "Abel Velasco", "Alexander": "Alex Taylor"},
    )

    assert merges == [
        {"display_name": "Abel Velasco", "owner_ids": ["GUID-ABEL", "yh-nick-abelv"]},
        {"display_name": "Alex Taylor", "owner_ids": ["GUID-ALEX", "yh-nick-alexander"]},
    ]


def test_active_identity_reconciliation_fails_closed_for_ambiguous_manager_name() -> None:
    active = pd.DataFrame(
        {
            "manager": ["David"],
            "manager_guid": ["yh-nick-david"],
            "franchise_id": ["yh-nick-david"],
            "year": [2026],
        }
    )
    historical = pd.DataFrame(
        {
            "manager": ["David", "David"],
            "franchise_id": ["GUID-DAVID-1", "GUID-DAVID-2"],
            "last_year": [2025, 2025],
        }
    )

    assert derive_yahoo_active_franchise_merges(active, historical) == []


def test_active_identity_reconciliation_requires_saved_alias_intent() -> None:
    active = pd.DataFrame(
        {
            "manager": ["David"],
            "manager_guid": ["yh-nick-david"],
            "franchise_id": ["yh-nick-david"],
            "year": [2026],
        }
    )
    historical = pd.DataFrame(
        {"manager": ["David"], "franchise_id": ["GUID-DAVID"], "last_year": [2025]}
    )

    assert derive_yahoo_active_franchise_merges(active, historical) == []


def test_active_identity_reconciliation_does_not_rewrite_real_yahoo_guid() -> None:
    active = pd.DataFrame(
        {
            "manager": ["Abel Velasco"],
            "manager_guid": ["REAL-GUID"],
            "franchise_id": ["REAL-GUID"],
            "year": [2026],
        }
    )
    historical = pd.DataFrame(
        {"manager": ["Abel Velasco"], "franchise_id": ["OLD-GUID"], "last_year": [2025]}
    )

    assert derive_yahoo_active_franchise_merges(active, historical) == []


def test_active_identity_reconciliation_accepts_missing_guid_and_uses_synthetic_franchise() -> None:
    active = pd.DataFrame(
        {
            "manager": ["Abel Velasco"],
            "manager_guid": [pd.NA],
            "franchise_id": ["yh-nick-abelv"],
            "year": [2026],
        }
    )
    historical = pd.DataFrame(
        {"manager": ["Abel Velasco"], "franchise_id": ["GUID-ABEL"], "last_year": [2025]}
    )

    assert derive_yahoo_active_franchise_merges(
        active,
        historical,
        manager_name_overrides={"Abel Velasco": "Abel Velasco"},
    ) == [
        {"display_name": "Abel Velasco", "owner_ids": ["GUID-ABEL", "yh-nick-abelv"]}
    ]


def test_active_identity_reconciliation_preserves_explicit_merge_and_avoids_duplicates() -> None:
    active = pd.DataFrame(
        {
            "manager": ["Abelv"],
            "manager_guid": ["yh-nick-abelv"],
            "franchise_id": ["yh-nick-abelv"],
            "year": [2026],
        }
    )
    historical = pd.DataFrame(
        {"manager": ["Abel Velasco"], "franchise_id": ["GUID-ABEL"], "last_year": [2025]}
    )
    existing = [{"display_name": "Abel Velasco", "owner_ids": ["GUID-ABEL", "yh-nick-abelv"]}]

    assert derive_yahoo_active_franchise_merges(
        active,
        historical,
        manager_name_overrides={"Abelv": "Abel Velasco"},
        existing_merges=existing,
    ) == existing


def test_identity_repair_check_is_one_bounded_active_history_query() -> None:
    class Reader:
        def __init__(self) -> None:
            self.sql = ""
            self.database = ""

        def query_scalar(self, sql: str, *, database: str):
            self.sql = sql
            self.database = database
            return 12

    reader = Reader()

    assert yahoo_active_identity_repair_count(
        reader, database_name="pimps_and_ochos", active_year=2026
    ) == 12
    assert reader.database == "___leagues"
    assert "public.homepage_manager_rankings" in reader.sql
    assert "last_year = 2026" in reader.sql
    assert "first_year < 2026" in reader.sql
    assert "LIKE 'yh-%'" in reader.sql


def test_identity_repair_replays_only_latest_finalized_week_within_ceiling() -> None:
    assert yahoo_identity_repair_week(
        repair_count=12,
        finalized_weeks=[1, 2, 3],
        through_week=2,
    ) == 2
    assert yahoo_identity_repair_week(
        repair_count=0,
        finalized_weeks=[1, 2, 3],
        through_week=2,
    ) is None
